use crate::auth::KiteAuth;
use crate::config::{EquitiesConfig, OptionsConfig};
use crate::models::{EquityPair, Instrument, OptionContract, OptionType};
use crate::quote_fetcher;
use std::collections::HashMap;
use tracing::{info, warn};

const INSTRUMENTS_URL: &str = "https://api.kite.trade/instruments";

pub async fn fetch_instruments(
    auth: &KiteAuth,
) -> Result<Vec<Instrument>, Box<dyn std::error::Error + Send + Sync>> {
    info!("Downloading instrument master from {}", INSTRUMENTS_URL);

    let client = reqwest::Client::new();
    let resp = client
        .get(INSTRUMENTS_URL)
        .header("X-Kite-Version", "3")
        .header("Authorization", auth.auth_header())
        .send()
        .await?;

    if !resp.status().is_success() {
        return Err(format!("Instrument fetch failed: HTTP {}", resp.status()).into());
    }

    let body = resp.text().await?;
    parse_instruments_csv(&body)
}

pub fn parse_instruments_csv(
    csv_data: &str,
) -> Result<Vec<Instrument>, Box<dyn std::error::Error + Send + Sync>> {
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .trim(csv::Trim::All)
        .from_reader(csv_data.as_bytes());

    let mut instruments = Vec::with_capacity(100_000);

    for result in rdr.records() {
        let record = result?;
        if record.len() < 12 {
            continue;
        }

        let instrument = Instrument {
            instrument_token: record[0].parse().unwrap_or(0),
            exchange_token: record[1].parse().unwrap_or(0),
            tradingsymbol: record[2].to_string(),
            name: record[3].to_string(),
            last_price: record[4].parse().unwrap_or(0.0),
            expiry: if record[5].is_empty() {
                None
            } else {
                Some(record[5].to_string())
            },
            strike: if record[6].is_empty() {
                None
            } else {
                record[6].parse().ok()
            },
            tick_size: record[7].parse().unwrap_or(0.05),
            lot_size: record[8].parse().unwrap_or(1),
            instrument_type: record[9].to_string(),
            segment: record[10].to_string(),
            exchange: record[11].to_string(),
        };

        instruments.push(instrument);
    }

    info!("Parsed {} instruments from CSV", instruments.len());
    Ok(instruments)
}

pub fn build_equity_pairs(
    instruments: &[Instrument],
    config: &EquitiesConfig,
) -> Vec<EquityPair> {
    let mut nse_map: HashMap<String, &Instrument> = HashMap::new();
    let mut bse_map: HashMap<String, &Instrument> = HashMap::new();

    for inst in instruments {
        if inst.instrument_type != "EQ" {
            continue;
        }

        if inst.lot_size > 1 {
            warn!(
                "Skipping SME/lot-based stock: {} (lot_size: {})",
                inst.tradingsymbol, inst.lot_size
            );
            continue;
        }

        if inst.exchange == "NSE" && inst.segment.contains("SME") {
            warn!(
                "Skipping NSE SME segment: {} (segment: {})",
                inst.tradingsymbol, inst.segment
            );
            continue;
        }

        if inst.exchange == "BSE" && (inst.segment.contains("M") || inst.segment.contains("T")) {
            warn!(
                "Skipping BSE illiquid/T2T segment: {} (segment: {})",
                inst.tradingsymbol, inst.segment
            );
            continue;
        }

        if inst.last_price > 0.0 && inst.last_price < config.min_price {
            warn!(
                "Skipping penny stock: {} (price: ₹{:.2}, min: ₹{:.2})",
                inst.tradingsymbol, inst.last_price, config.min_price
            );
            continue;
        }

        if !config.symbols.is_empty() {
            let in_watchlist = config.symbols.iter().any(|s| {
                s.eq_ignore_ascii_case(&inst.tradingsymbol)
                    || s.eq_ignore_ascii_case(&inst.name)
            });
            if !in_watchlist {
                continue;
            }
        }

        match inst.exchange.as_str() {
            "NSE" => {
                nse_map.insert(inst.tradingsymbol.clone(), inst);
            }
            "BSE" => {
                bse_map.insert(inst.tradingsymbol.clone(), inst);
            }
            _ => {}
        }
    }

    let mut pairs = Vec::new();

    for (symbol, nse_inst) in &nse_map {
        if let Some(bse_inst) = bse_map.get(symbol) {
            pairs.push(EquityPair {
                name: nse_inst.name.clone(),
                symbol: symbol.clone(),
                nse_token: nse_inst.instrument_token,
                bse_token: bse_inst.instrument_token,
                nse_symbol: format!("NSE:{}", nse_inst.tradingsymbol),
                bse_symbol: format!("BSE:{}", bse_inst.tradingsymbol),
            });
            info!(
                "Paired equity: {} — NSE token {} / BSE token {}",
                nse_inst.name, nse_inst.instrument_token, bse_inst.instrument_token
            );
        } else {
            warn!(
                "No BSE match for NSE:{} ({})",
                symbol, nse_inst.name
            );
        }
    }

    info!("Built {} equity pairs for spread tracking", pairs.len());
    pairs
}

fn option_underlying_aliases(underlying: &str) -> Vec<String> {
    let u = underlying.trim().to_ascii_uppercase();
    match u.as_str() {
        "NIFTY" | "NIFTY50" | "NIFTY 50" => {
            vec!["NIFTY".to_string(), "NIFTY50".to_string(), "NIFTY 50".to_string()]
        }
        "NIFTYNXT50" | "NIFTY NEXT 50" | "NIFTYNEXT50" => {
            vec!["NIFTYNXT50".to_string(), "NIFTY NEXT 50".to_string(), "NIFTYNEXT50".to_string()]
        }
        _ => vec![u],
    }
}

fn instrument_matches_underlying(inst: &Instrument, configured_underlying: &str) -> bool {
    let inst_name = inst.name.trim().to_ascii_uppercase();
    let inst_name_compact = inst_name.replace(' ', "");
    let symbol = inst.tradingsymbol.trim().to_ascii_uppercase();

    option_underlying_aliases(configured_underlying)
        .into_iter()
        .any(|alias| {
            let alias_compact = alias.replace(' ', "");
            if inst_name == alias || inst_name_compact == alias_compact {
                return true;
            }
            // Symbol prefix check: require that the character immediately after the
            // alias_compact prefix is a digit (e.g. the expiry year).
            // This prevents "NIFTY" from matching "NIFTYNXT5026MAR..." whose 6th char is 'N'.
            if symbol.starts_with(&alias_compact) {
                let next = symbol.chars().nth(alias_compact.len());
                return next.map(|c| c.is_ascii_digit()).unwrap_or(true);
            }
            false
        })
}

pub async fn build_options_chain(
    auth: &KiteAuth,
    instruments: &[Instrument],
    config: &OptionsConfig,
) -> Vec<OptionContract> {
    let preferred_expiry = config
        .expiry
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());

    let mut available_expiries: HashMap<String, Vec<String>> = HashMap::new();
    for inst in instruments {
        if inst.segment != "NFO-OPT" {
            continue;
        }
        if !matches!(inst.instrument_type.as_str(), "CE" | "PE") {
            continue;
        }
        let Some(expiry) = inst.expiry.as_ref() else {
            continue;
        };
        let Some(underlying) = config
            .underlyings
            .iter()
            .find(|u| instrument_matches_underlying(inst, u))
        else {
            continue;
        };
        available_expiries
            .entry(underlying.clone())
            .or_default()
            .push(expiry.clone());
    }

    let mut selected_expiry_by_underlying: HashMap<String, String> = HashMap::new();
    for underlying in &config.underlyings {
        let mut expiries = available_expiries.remove(underlying).unwrap_or_default();
        expiries.sort();
        expiries.dedup();

        if expiries.is_empty() {
            warn!(
                "No options contracts found for underlying {} (after alias matching).",
                underlying
            );
            continue;
        }

        let selected_expiry = if let Some(pref) = preferred_expiry {
            if expiries.iter().any(|e| e == pref) {
                pref.to_string()
            } else {
                let fallback = expiries[0].clone();
                warn!(
                    "Configured expiry {} not available for {}. Auto-selected nearest available expiry {}",
                    pref, underlying, fallback
                );
                fallback
            }
        } else {
            // Skip expiries with DTE <= 1: on 0/1-DTE days trade next week's expiry instead.
            use chrono::{NaiveDate, Utc, Duration};
            let today_ist = (Utc::now() + Duration::hours(5) + Duration::minutes(30)).date_naive();
            let selected = expiries.iter()
                .find(|e| {
                    NaiveDate::parse_from_str(e, "%Y-%m-%d")
                        .map(|exp| (exp - today_ist).num_days() > 1)
                        .unwrap_or(true)
                })
                .cloned()
                .unwrap_or_else(|| expiries[0].clone());
            if selected != expiries[0] {
                info!(
                    "  {} nearest expiry {} is 0/1-DTE — skipping to next expiry {}",
                    underlying, expiries[0], selected
                );
            }
            selected
        };

        info!(
            "Underlying {} -> selected expiry {} ({} available expiries)",
            underlying,
            selected_expiry,
            expiries.len()
        );
        selected_expiry_by_underlying.insert(underlying.clone(), selected_expiry);
    }

    let mut contracts = Vec::new();
    let mut contract_count_by_underlying: HashMap<String, usize> = HashMap::new();

    for inst in instruments {
        if inst.segment != "NFO-OPT" {
            continue;
        }

        let opt_type = match inst.instrument_type.as_str() {
            "CE" => OptionType::CE,
            "PE" => OptionType::PE,
            _ => continue,
        };

        let underlying_cfg = config
            .underlyings
            .iter()
            .find(|u| instrument_matches_underlying(inst, u));

        let underlying_name = match underlying_cfg {
            Some(u) => u.clone(),
            None => continue,
        };

        let Some(expiry) = inst.expiry.as_ref() else {
            continue;
        };
        let Some(selected_expiry) = selected_expiry_by_underlying.get(&underlying_name) else {
            continue;
        };
        if expiry != selected_expiry {
            continue;
        }

        if let Some(strike) = inst.strike {
            if strike < config.strike_min || strike > config.strike_max {
                continue;
            }

            let underlying = underlying_name;

            contracts.push(OptionContract {
                instrument_token: inst.instrument_token,
                tradingsymbol: inst.tradingsymbol.clone(),
                underlying: underlying.clone(),
                expiry: inst.expiry.clone().unwrap_or_default(),
                strike,
                option_type: opt_type,
                lot_size: inst.lot_size,
            });
            *contract_count_by_underlying.entry(underlying).or_insert(0) += 1;
        }
    }

    // Filter to nearest N strikes per underlying if configured.
    // Each underlying uses config.strikes_for(u), which checks nearest_strikes_override
    // first and falls back to the global nearest_strikes.  The block is entered only
    // if at least one underlying has a non-zero strike count.
    let any_filtering = config.underlyings.iter().any(|u| config.strikes_for(u) > 0);
    if any_filtering {
        // Estimate ATM per underlying from index instrument last_price
        let mut spot_estimate: HashMap<String, f64> = quote_fetcher::fetch_live_quotes(auth, instruments, &config.underlyings).await;
        
        for underlying in &config.underlyings {
            if spot_estimate.contains_key(underlying) {
                continue;
            }
            
            // Try index instrument last_price (use same aliases as main.rs)
            let index_aliases: &[&str] = match underlying.as_str() {
                "NIFTY"      => &["NIFTY 50", "NIFTY50", "NIFTY"],
                "NIFTYNXT50" => &["NIFTY NEXT 50", "NIFTYNXT50"],
                _ => &[underlying.as_str()],
            };
            let index_spot = instruments.iter()
                .find(|inst| {
                    inst.exchange == "NSE"
                        && inst.segment == "INDICES"
                        && index_aliases.iter().any(|a| {
                            inst.tradingsymbol.eq_ignore_ascii_case(a)
                                || inst.name.eq_ignore_ascii_case(a)
                        })
                })
                .map(|inst| inst.last_price)
                .filter(|p| *p > 0.0);

            if let Some(sp) = index_spot {
                spot_estimate.insert(underlying.clone(), sp);
                info!("  {} spot estimate from index master (last_price): {:.0}", underlying, sp);
            } else {
                // Estimate ATM from put-call parity: the strike where |CE_price - PE_price| is smallest.
                // Both CE and PE must have last_price > 0 in the instruments CSV.
                let mut ce_prices: HashMap<u64, f64> = HashMap::new();
                let mut pe_prices: HashMap<u64, f64> = HashMap::new();
                for inst in instruments.iter() {
                    if inst.segment != "NFO-OPT" { continue; }
                    if !instrument_matches_underlying(inst, underlying) { continue; }
                    if let (Some(strike), lp) = (inst.strike, inst.last_price) {
                        if lp <= 0.0 { continue; }
                        let key = (strike * 100.0) as u64;
                        match inst.instrument_type.as_str() {
                            "CE" => { ce_prices.insert(key, lp); }
                            "PE" => { pe_prices.insert(key, lp); }
                            _ => {}
                        }
                    }
                }
                // Find strike with smallest |CE - PE| (ATM indicator via put-call parity)
                let mut best_strike: Option<(f64, f64)> = None; // (strike, abs_diff)
                for (&key, &ce_lp) in &ce_prices {
                    if let Some(&pe_lp) = pe_prices.get(&key) {
                        let strike = key as f64 / 100.0;
                        let diff = (ce_lp - pe_lp).abs();
                        if best_strike.map_or(true, |(_, bd)| diff < bd) {
                            best_strike = Some((strike, diff));
                        }
                    }
                }
                if let Some((atm, _)) = best_strike {
                    spot_estimate.insert(underlying.clone(), atm);
                    info!("  {} spot estimate from put-call parity: {:.0}", underlying, atm);
                } else {
                    // Last resort: median strike
                    let mut strikes: Vec<f64> = contracts.iter()
                        .filter(|c| c.underlying == *underlying)
                        .map(|c| c.strike)
                        .collect();
                    strikes.sort_by(|a, b| a.partial_cmp(b).unwrap());
                    strikes.dedup();
                    if !strikes.is_empty() {
                        let mid = strikes[strikes.len() / 2];
                        spot_estimate.insert(underlying.clone(), mid);
                        info!("  {} spot estimate from median strike: {:.0}", underlying, mid);
                    }
                }
            }
        }

        let mut filtered = Vec::new();
        for underlying in &config.underlyings {
            let per_underlying_n = config.strikes_for(underlying);

            // nearest_strikes = 0 for this underlying → keep all in range
            if per_underlying_n == 0 {
                filtered.extend(contracts.iter().filter(|c| c.underlying == *underlying).cloned());
                continue;
            }

            let spot = match spot_estimate.get(underlying) {
                Some(s) => *s,
                None => {
                    // Keep all contracts if we can't estimate spot
                    filtered.extend(contracts.iter().filter(|c| c.underlying == *underlying).cloned());
                    continue;
                }
            };

            // Strike selection keeps both CE and PE for the ATM strike plus the nearest
            // N strikes above and below ATM. Strategy code later decides direction.
            let mut strikes: Vec<f64> = contracts.iter()
                .filter(|c| c.underlying == *underlying)
                .map(|c| c.strike)
                .collect();
            strikes.sort_by(|a, b| a.partial_cmp(b).unwrap());
            strikes.dedup();

            // ATM = strike with smallest distance to spot
            let atm_strike = strikes.iter()
                .cloned()
                .min_by(|a, b| (a - spot).abs().partial_cmp(&(b - spot).abs()).unwrap())
                .unwrap_or(spot);
            let atm_key = (atm_strike * 100.0) as u64;

            // Keep both CE and PE for the ATM strike plus the nearest N strikes above
            // and below ATM.
            let above_strikes: std::collections::HashSet<u64> = strikes.iter()
                .filter(|&&s| s > atm_strike)
                .take(per_underlying_n as usize)
                .map(|s| (*s * 100.0) as u64)
                .collect();

            let below_strikes: std::collections::HashSet<u64> = strikes.iter()
                .rev()
                .filter(|&&s| s < atm_strike)
                .take(per_underlying_n as usize)
                .map(|s| (*s * 100.0) as u64)
                .collect();

            let before = contracts.iter().filter(|c| c.underlying == *underlying).count();
            filtered.extend(
                contracts.iter()
                    .filter(|c| {
                        if c.underlying != *underlying { return false; }
                        let sk = (c.strike * 100.0) as u64;
                        if sk == atm_key { return true; }
                        above_strikes.contains(&sk) || below_strikes.contains(&sk)
                    })
                    .cloned()
            );
            let after = filtered.iter().filter(|c| c.underlying == *underlying).count();
            info!(
                "  {} nearest={}: {} -> {} contracts (ATM {:.0}, up {}, down {})",
                underlying, per_underlying_n, before, after, atm_strike,
                above_strikes.len(), below_strikes.len()
            );
        }
        contracts = filtered;

        // Rebuild counts
        contract_count_by_underlying.clear();
        for c in &contracts {
            *contract_count_by_underlying.entry(c.underlying.clone()).or_insert(0) += 1;
        }
    }

    let mut breakdown = Vec::new();
    for underlying in &config.underlyings {
        let expiry = selected_expiry_by_underlying
            .get(underlying)
            .map(String::as_str)
            .unwrap_or("N/A");
        let count = contract_count_by_underlying
            .get(underlying)
            .copied()
            .unwrap_or(0);
        breakdown.push(format!("{underlying}: {count} contracts @ {expiry}"));
    }

    info!(
        "Built options chain: {} contracts ({})",
        contracts.len(),
        breakdown.join(", ")
    );

    contracts
}

/// Synchronous version of `build_options_chain` for use in unit tests.
/// Performs the same expiry selection and strike filtering but skips the
/// nearest-strikes ATM lookup (which requires a live API call). Pass
/// `nearest_strikes: 0` in the config when using this function.
pub fn build_options_chain_sync(
    instruments: &[Instrument],
    config: &OptionsConfig,
) -> Vec<OptionContract> {
    let preferred_expiry = config
        .expiry
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());

    let mut available_expiries: HashMap<String, Vec<String>> = HashMap::new();
    for inst in instruments {
        if inst.segment != "NFO-OPT" { continue; }
        if !matches!(inst.instrument_type.as_str(), "CE" | "PE") { continue; }
        let Some(expiry) = inst.expiry.as_ref() else { continue; };
        let Some(underlying) = config.underlyings.iter()
            .find(|u| instrument_matches_underlying(inst, u))
        else { continue; };
        available_expiries.entry(underlying.clone()).or_default().push(expiry.clone());
    }

    let mut selected_expiry_by_underlying: HashMap<String, String> = HashMap::new();
    for underlying in &config.underlyings {
        let mut expiries = available_expiries.remove(underlying).unwrap_or_default();
        expiries.sort();
        expiries.dedup();
        if expiries.is_empty() { continue; }
        let selected = if let Some(pref) = preferred_expiry {
            if expiries.iter().any(|e| e == pref) { pref.to_string() } else { expiries[0].clone() }
        } else {
            use chrono::{NaiveDate, Utc, Duration};
            let today_ist = (Utc::now() + Duration::hours(5) + Duration::minutes(30)).date_naive();
            expiries.iter()
                .find(|e| {
                    NaiveDate::parse_from_str(e, "%Y-%m-%d")
                        .map(|exp| (exp - today_ist).num_days() > 1)
                        .unwrap_or(true)
                })
                .cloned()
                .unwrap_or_else(|| expiries[0].clone())
        };
        selected_expiry_by_underlying.insert(underlying.clone(), selected);
    }

    let mut contracts = Vec::new();
    for inst in instruments {
        if inst.segment != "NFO-OPT" { continue; }
        let opt_type = match inst.instrument_type.as_str() {
            "CE" => OptionType::CE,
            "PE" => OptionType::PE,
            _ => continue,
        };
        let Some(underlying) = config.underlyings.iter()
            .find(|u| instrument_matches_underlying(inst, u))
        else { continue; };
        let Some(expiry) = inst.expiry.as_ref() else { continue; };
        let Some(selected) = selected_expiry_by_underlying.get(underlying) else { continue; };
        if expiry != selected { continue; }
        if let Some(strike) = inst.strike {
            if strike < config.strike_min || strike > config.strike_max { continue; }
            contracts.push(OptionContract {
                instrument_token: inst.instrument_token,
                tradingsymbol: inst.tradingsymbol.clone(),
                underlying: underlying.clone(),
                expiry: expiry.clone(),
                strike,
                option_type: opt_type,
                lot_size: inst.lot_size,
            });
        }
    }
    contracts
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_csv() -> &'static str {
        "instrument_token,exchange_token,tradingsymbol,name,last_price,expiry,strike,tick_size,lot_size,instrument_type,segment,exchange\n\
         408065,1594,INFY,INFOSYS,1400.0,,,0.05,1,EQ,NSE,NSE\n\
         100123,391,INFY,INFOSYS,1399.5,,,0.05,1,EQ,BSE,BSE\n\
         200456,782,TCS,TATA CONSULTANCY,3500.0,,,0.05,1,EQ,NSE,NSE\n\
         5720578,22346,NIFTY26FEB24000CE,NIFTY,23.0,2026-02-26,24000,0.05,65,CE,NFO-OPT,NFO\n\
         5720579,22347,NIFTY26FEB24000PE,NIFTY,45.0,2026-02-26,24000,0.05,65,PE,NFO-OPT,NFO\n\
         5720580,22348,NIFTY26FEB23000CE,NIFTY,120.0,2026-02-26,23000,0.05,65,CE,NFO-OPT,NFO\n\
         5720581,22349,NIFTY26MAR24000CE,NIFTY,50.0,2026-03-26,24000,0.05,65,CE,NFO-OPT,NFO\n\
         6720578,32346,NIFTYNXT5026MAR25000CE,NIFTY NEXT 50,80.0,2026-03-05,25000,0.05,25,CE,NFO-OPT,NFO\n\
         6720579,32347,NIFTYNXT5026MAR25000PE,NIFTY NEXT 50,85.0,2026-03-05,25000,0.05,25,PE,NFO-OPT,NFO\n\
         6720580,32348,NIFTYNXT5026MAR1225000CE,NIFTY NEXT 50,100.0,2026-03-12,25000,0.05,25,CE,NFO-OPT,NFO"
    }

    #[test]
    fn test_parse_instruments_csv() {
        let instruments = parse_instruments_csv(sample_csv()).unwrap();
        assert_eq!(instruments.len(), 10);
        assert_eq!(instruments[0].tradingsymbol, "INFY");
        assert_eq!(instruments[0].exchange, "NSE");
        assert_eq!(instruments[3].instrument_type, "CE");
    }

    #[test]
    fn test_build_equity_pairs() {
        use crate::config::EquitiesConfig;

        let instruments = parse_instruments_csv(sample_csv()).unwrap();
        let config = EquitiesConfig {
            symbols: vec!["INFY".to_string(), "TCS".to_string()],
            min_price: 10.0,
            min_avg_volume: 0,
        };
        let pairs = build_equity_pairs(&instruments, &config);

        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].symbol, "INFY");
        assert_eq!(pairs[0].nse_token, 408065);
        assert_eq!(pairs[0].bse_token, 100123);
    }

    #[test]
    fn test_build_options_chain() {
        let instruments = parse_instruments_csv(sample_csv()).unwrap();
        let config = OptionsConfig {
            underlyings: vec!["NIFTY".to_string(), "NIFTYNXT50".to_string()],
            expiry: None,
            strike_min: 22000.0,
            strike_max: 70000.0,
            strike_step: 50.0,
            nearest_strikes: 0,
            nearest_strikes_override: std::collections::HashMap::new(),
        };
        let contracts = build_options_chain_sync(&instruments, &config);

        let nifty_contracts: Vec<_> = contracts
            .iter()
            .filter(|c| c.underlying == "NIFTY")
            .collect();
        let nxt50_contracts: Vec<_> = contracts
            .iter()
            .filter(|c| c.underlying == "NIFTYNXT50")
            .collect();

        assert_eq!(nifty_contracts.len(), 3);
        assert!(nifty_contracts.iter().all(|c| c.expiry == "2026-02-26"));
        assert_eq!(nxt50_contracts.len(), 2);
        assert!(nxt50_contracts
            .iter()
            .all(|c| c.expiry == "2026-03-05"));
    }
}
