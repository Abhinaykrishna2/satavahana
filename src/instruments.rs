use crate::auth::KiteAuth;
use crate::config::{EquitiesConfig, OptionsConfig};
use crate::models::{EquityPair, Instrument, OptionContract, OptionType};
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

pub fn build_options_chain(
    instruments: &[Instrument],
    config: &OptionsConfig,
) -> Vec<OptionContract> {
    let mut contracts = Vec::new();

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
            .find(|u| inst.name.eq_ignore_ascii_case(u));

        let underlying_name = match underlying_cfg {
            Some(u) => u.clone(),
            None => continue,
        };

        if let Some(ref expiry) = inst.expiry {
            if expiry != &config.expiry {
                continue;
            }
        } else {
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
                underlying,
                expiry: inst.expiry.clone().unwrap_or_default(),
                strike,
                option_type: opt_type,
                lot_size: inst.lot_size,
            });
        }
    }

    info!(
        "Built options chain: {} contracts for {:?} expiry {}",
        contracts.len(),
        config.underlyings,
        config.expiry
    );

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
         5720581,22349,NIFTY26MAR24000CE,NIFTY,50.0,2026-03-26,24000,0.05,65,CE,NFO-OPT,NFO"
    }

    #[test]
    fn test_parse_instruments_csv() {
        let instruments = parse_instruments_csv(sample_csv()).unwrap();
        assert_eq!(instruments.len(), 7);
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
            underlyings: vec!["NIFTY".to_string()],
            expiry: "2026-02-26".to_string(),
            strike_min: 22000.0,
            strike_max: 24000.0,
            strike_step: 50.0,
        };
        let contracts = build_options_chain(&instruments, &config);

        assert_eq!(contracts.len(), 3);
    }
}
