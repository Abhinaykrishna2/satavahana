use satavahana::config::Config;
use tracing_subscriber;
use satavahana::models::{OHLC, OptionContract, OptionType, Tick, TickMode};
use satavahana::options_engine::OptionsEngine;
use satavahana::store::TickStore;

use chrono::{FixedOffset, NaiveDateTime, TimeZone, Utc};
use csv::StringRecord;
use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

const DEFAULT_OPTIONS_FILE: &str = "data/2026-02-22_options_ticks.csv";

#[derive(Default)]
struct TradeSummary {
    trades: usize,
    wins: usize,
    losses: usize,
    breakeven: usize,
    gross_pnl: f64,
    total_costs: f64,
    net_pnl: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CapitalMode {
    Isolated,
    Continuous,
}

impl CapitalMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Isolated => "isolated",
            Self::Continuous => "continuous",
        }
    }
}

#[derive(Debug, Clone)]
struct CliArgs {
    options_file: PathBuf,
    capital_mode: CapitalMode,
    fill_offset_inr: f64,
    max_daily_trades: Option<u32>,
}

fn print_usage(bin: &str) {
    eprintln!(
        "Usage: {bin} [options] [options_csv]\n\
         \n\
         Options:\n\
           --capital-mode <isolated|continuous>   Capital label mode (default: continuous; no persistence)\n\
           --isolated                              Alias for --capital-mode isolated\n\
           --continuous                            Alias for --capital-mode continuous\n\
           --fill-offset-inr <value>              Buy +x / Sell -x execution offset in INR (default: 0.5)\n\
           --max-daily-trades <N>                 Override config max_daily_trades (live=3, backtest=20)\n\
           -h, --help                             Show this help\n\
         \n\
         Examples:\n\
           {bin} data/2026-02-24_options_ticks.csv\n\
           {bin} --max-daily-trades 20 data/2026-02-24_options_ticks.csv\n\
           {bin} --isolated --fill-offset-inr 0.5 data/2026-02-24_options_ticks.csv"
    );
}

fn parse_capital_mode(value: &str) -> Result<CapitalMode, Box<dyn Error>> {
    match value {
        "isolated" => Ok(CapitalMode::Isolated),
        "continuous" => Ok(CapitalMode::Continuous),
        _ => Err(format!("Invalid capital mode '{}': expected isolated|continuous", value).into()),
    }
}

fn parse_args() -> Result<CliArgs, Box<dyn Error>> {
    let mut args = std::env::args();
    let bin = args
        .next()
        .unwrap_or_else(|| "backtest_options".to_string());

    let mut options_file: Option<PathBuf> = None;
    let mut capital_mode = CapitalMode::Continuous;
    let mut fill_offset_inr = 0.5_f64;
    let mut max_daily_trades: Option<u32> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => {
                print_usage(&bin);
                std::process::exit(0);
            }
            "--isolated" => capital_mode = CapitalMode::Isolated,
            "--continuous" => capital_mode = CapitalMode::Continuous,
            "--capital-mode" => {
                let value = args
                    .next()
                    .ok_or_else(|| "Missing value for --capital-mode".to_string())?;
                capital_mode = parse_capital_mode(&value)?;
            }
            "--fill-offset-inr" => {
                let value = args
                    .next()
                    .ok_or_else(|| "Missing value for --fill-offset-inr".to_string())?;
                fill_offset_inr = value.parse::<f64>()
                    .map_err(|e| format!("Invalid --fill-offset-inr value '{}': {}", value, e))?;
            }
            "--max-daily-trades" => {
                let value = args
                    .next()
                    .ok_or_else(|| "Missing value for --max-daily-trades".to_string())?;
                max_daily_trades = Some(value.parse::<u32>()
                    .map_err(|e| format!("Invalid --max-daily-trades value '{}': {}", value, e))?);
            }
            _ if arg.starts_with("--capital-mode=") => {
                let value = arg.split_once('=').map(|(_, v)| v).unwrap_or("");
                capital_mode = parse_capital_mode(value)?;
            }
            _ if arg.starts_with("--fill-offset-inr=") => {
                let value = arg.split_once('=').map(|(_, v)| v).unwrap_or("");
                fill_offset_inr = value.parse::<f64>()
                    .map_err(|e| format!("Invalid --fill-offset-inr value '{}': {}", value, e))?;
            }
            _ if arg.starts_with("--max-daily-trades=") => {
                let value = arg.split_once('=').map(|(_, v)| v).unwrap_or("");
                max_daily_trades = Some(value.parse::<u32>()
                    .map_err(|e| format!("Invalid --max-daily-trades value '{}': {}", value, e))?);
            }
            _ if arg.starts_with('-') => {
                return Err(format!("Unknown flag: {}", arg).into());
            }
            _ => {
                if options_file.is_some() {
                    return Err("Multiple CSV paths provided; pass only one options CSV".into());
                }
                options_file = Some(PathBuf::from(arg));
            }
        }
    }

    if fill_offset_inr < 0.0 {
        return Err("--fill-offset-inr must be >= 0".into());
    }

    Ok(CliArgs {
        options_file: options_file.unwrap_or_else(|| PathBuf::from(DEFAULT_OPTIONS_FILE)),
        capital_mode,
        max_daily_trades,
        fill_offset_inr,
    })
}

fn parse_ts_ms_fallback(s: &str) -> Option<u64> {
    let s = s.trim();
    let dt = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f")
        .ok()
        .or_else(|| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f").ok())
        .or_else(|| NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S").ok())?;

    let ist = FixedOffset::east_opt(5 * 3600 + 30 * 60).unwrap();
    let ist_dt = ist
        .from_local_datetime(&dt)
        .single()
        .or_else(|| ist.from_local_datetime(&dt).earliest())
        .or_else(|| ist.from_local_datetime(&dt).latest());
    if let Some(ist_dt) = ist_dt {
        return Some(ist_dt.with_timezone(&Utc).timestamp_millis() as u64);
    }

    Some(dt.and_utc().timestamp_millis() as u64)
}

fn col_idx(headers: &StringRecord, name: &str) -> Result<usize, Box<dyn Error>> {
    headers
        .iter()
        .position(|h| h == name)
        .ok_or_else(|| format!("Missing column '{}'", name).into())
}

fn parse_u32(rec: &StringRecord, idx: usize) -> u32 {
    rec.get(idx).and_then(|v| v.parse::<u32>().ok()).unwrap_or(0)
}

fn parse_f64(rec: &StringRecord, idx: usize) -> f64 {
    rec.get(idx).and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0)
}

fn parse_option_type(s: &str) -> Option<OptionType> {
    match s.trim() {
        "CE" => Some(OptionType::CE),
        "PE" => Some(OptionType::PE),
        _ => None,
    }
}

/// Canonical lot sizes from SEBI circulars (March 2026). Used as a safe fallback
/// when a token never shows any non-zero last_qty in the captured data.
fn default_lot_size(underlying: &str) -> u32 {
    match underlying {
        "NIFTY"      => 65,
        "NIFTYNXT50" => 65,
        "FINNIFTY"   => 60,
        "MIDCPNIFTY" => 120,
        _ => 25,
    }
}

/// Scan the CSV once and return a map of token → inferred lot size.
///
/// Method: minimum non-zero `last_qty` observed across all ticks for each token.
/// `last_qty` is the quantity of the most-recently executed trade in that tick.
/// The smallest possible trade is 1 lot, so the minimum non-zero value converges
/// to the lot size once a 1-lot trade is seen (which is typical for liquid NSE options).
/// This mirrors what the live system gets from the Kite instruments API — no hardcoding.
fn infer_lot_sizes_from_csv(
    options_csv: &Path,
    i_token: usize,
    i_last_qty: usize,
) -> Result<HashMap<u32, u32>, Box<dyn Error>> {
    let mut min_last_qty: HashMap<u32, u32> = HashMap::new();
    let mut rdr = csv::Reader::from_path(options_csv)?;
    for rec in rdr.records() {
        let rec = rec?;
        let token = parse_u32(&rec, i_token);
        if token == 0 {
            continue;
        }
        let lq = parse_u32(&rec, i_last_qty);
        if lq > 0 {
            let e = min_last_qty.entry(token).or_insert(lq);
            if lq < *e {
                *e = lq;
            }
        }
    }
    Ok(min_last_qty)
}

fn find_single_file_with_suffix(dir: &Path, suffix: &str) -> Result<PathBuf, Box<dyn Error>> {
    let mut matches = Vec::new();
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if name.ends_with(suffix) {
                matches.push(path);
            }
        }
    }
    matches.sort();
    matches
        .into_iter()
        .next()
        .ok_or_else(|| format!("No file ending with '{}' found in {}", suffix, dir.display()).into())
}

#[derive(Default)]
struct TradeDetail {
    trade_id: String,
    underlying: String,
    option_type: String,
    strike: f64,
    spot_at_entry: f64,
    entry_price: f64,
    exit_price: f64,
    lots: u32,
    lot_size: u32,
    exit_reason: String,
    net_pnl: f64,
    strategy: String,
    holding_mins: f64,
}

fn summarize_trades(trades_csv: &Path) -> Result<(TradeSummary, Vec<TradeDetail>), Box<dyn Error>> {
    let mut rdr = csv::Reader::from_path(trades_csv)?;
    let headers = rdr.headers()?.clone();
    let i_trade_id    = col_idx(&headers, "trade_id")?;
    let i_underlying  = col_idx(&headers, "underlying")?;
    let i_opt_type    = col_idx(&headers, "option_type")?;
    let i_strike      = col_idx(&headers, "strike")?;
    let i_spot        = col_idx(&headers, "spot_at_entry")?;
    let i_entry_price = col_idx(&headers, "entry_price")?;
    let i_exit_price  = col_idx(&headers, "exit_price")?;
    let i_lots        = col_idx(&headers, "lots")?;
    let i_lot_size    = col_idx(&headers, "lot_size")?;
    let i_exit_reason = col_idx(&headers, "exit_reason")?;
    let i_gross       = col_idx(&headers, "gross_pnl")?;
    let i_costs       = col_idx(&headers, "transaction_costs")?;
    let i_net         = col_idx(&headers, "net_pnl")?;
    let i_strategy    = col_idx(&headers, "strategy")?;
    let i_holding     = col_idx(&headers, "holding_duration_mins")?;

    let mut out = TradeSummary::default();
    let mut details = Vec::new();

    for rec in rdr.records() {
        let rec = rec?;
        let gross = parse_f64(&rec, i_gross);
        let costs = parse_f64(&rec, i_costs);
        let net = parse_f64(&rec, i_net);
        out.trades += 1;
        out.gross_pnl += gross;
        out.total_costs += costs;
        out.net_pnl += net;
        if net > 0.0 {
            out.wins += 1;
        } else if net < 0.0 {
            out.losses += 1;
        } else {
            out.breakeven += 1;
        }

        details.push(TradeDetail {
            trade_id:     rec.get(i_trade_id).unwrap_or("").to_string(),
            underlying:   rec.get(i_underlying).unwrap_or("").to_string(),
            option_type:  rec.get(i_opt_type).unwrap_or("").to_string(),
            strike:       parse_f64(&rec, i_strike),
            spot_at_entry: parse_f64(&rec, i_spot),
            entry_price:  parse_f64(&rec, i_entry_price),
            exit_price:   parse_f64(&rec, i_exit_price),
            lots:         parse_u32(&rec, i_lots),
            lot_size:     parse_u32(&rec, i_lot_size),
            exit_reason:  rec.get(i_exit_reason).unwrap_or("").to_string(),
            net_pnl:      net,
            strategy:     rec.get(i_strategy).unwrap_or("").to_string(),
            holding_mins: parse_f64(&rec, i_holding),
        });
    }
    Ok((out, details))
}

fn build_contracts(options_csv: &Path) -> Result<Vec<OptionContract>, Box<dyn Error>> {
    // Resolve column indices from CSV headers (needed for both passes).
    let headers = {
        let mut rdr = csv::Reader::from_path(options_csv)?;
        rdr.headers()?.clone()
    };

    let i_token      = col_idx(&headers, "token")?;
    let i_symbol     = col_idx(&headers, "tradingsymbol")?;
    let i_underlying = col_idx(&headers, "underlying")?;
    let i_expiry     = col_idx(&headers, "expiry")?;
    let i_strike     = col_idx(&headers, "strike")?;
    let i_opt_type   = col_idx(&headers, "option_type")?;
    let i_last_qty   = col_idx(&headers, "last_qty")?;

    // Pass 1: infer lot sizes dynamically from the data (min non-zero last_qty per token).
    // This replicates what the live system gets from the Kite instruments API without hardcoding.
    let lot_size_map = infer_lot_sizes_from_csv(options_csv, i_token, i_last_qty)?;

    // Pass 2: build one OptionContract per token.
    let mut contracts: HashMap<u32, OptionContract> = HashMap::new();
    let mut rdr = csv::Reader::from_path(options_csv)?;
    // skip re-reading headers (already cloned above)
    let _ = rdr.headers()?;

    for rec in rdr.records() {
        let rec = rec?;
        let token = parse_u32(&rec, i_token);
        if token == 0 {
            continue;
        }

        let option_type = match parse_option_type(rec.get(i_opt_type).unwrap_or("")) {
            Some(t) => t,
            None => continue,
        };
        let underlying = rec.get(i_underlying).unwrap_or("").trim().to_string();
        if underlying.is_empty() {
            continue;
        }
        let expiry = rec.get(i_expiry).unwrap_or("").trim().to_string();
        if expiry.is_empty() {
            continue;
        }
        let strike = parse_f64(&rec, i_strike);
        if strike <= 0.0 {
            continue;
        }
        let tradingsymbol = rec.get(i_symbol).unwrap_or("").trim().to_string();
        if tradingsymbol.is_empty() {
            continue;
        }
        // Use dynamically inferred lot size; fall back to SEBI canonical size if
        // the token never showed any non-zero last_qty in the captured data.
        let lot_size = lot_size_map.get(&token).copied()
            .unwrap_or_else(|| default_lot_size(&underlying));

        contracts.entry(token).or_insert_with(|| OptionContract {
            instrument_token: token,
            tradingsymbol,
            underlying,
            expiry,
            strike,
            option_type,
            lot_size,
        });
    }

    let mut out: Vec<OptionContract> = contracts.into_values().collect();
    out.sort_by_key(|c| c.instrument_token);
    Ok(out)
}

fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_writer(std::io::stderr)
        .with_target(false)
        .init();

    let cli = parse_args()?;
    let options_file = cli.options_file.clone();
    if !options_file.exists() {
        return Err(format!("Options CSV not found: {}", options_file.display()).into());
    }

    let config = Config::load("config.toml")?;
    let all_contracts = build_contracts(&options_file)?;
    if all_contracts.is_empty() {
        return Err("No option contracts found in CSV".into());
    }

    // Filter to only underlyings in config — ignore MIDCPNIFTY, FINNIFTY, etc.
    let active_underlyings: std::collections::HashSet<&str> =
        config.options.underlyings.iter().map(|s| s.as_str()).collect();
    let contracts: Vec<_> = all_contracts
        .into_iter()
        .filter(|c| active_underlyings.contains(c.underlying.as_str()))
        .collect();

    let mut underlying_names: Vec<String> = contracts.iter().map(|c| c.underlying.clone()).collect();
    underlying_names.sort();
    underlying_names.dedup();

    eprintln!("Options CSV: {}", options_file.display());
    eprintln!("Contracts loaded: {}", contracts.len());
    eprintln!("Underlyings: {:?}", underlying_names);

    // Show inferred lot sizes per underlying (sample one contract per underlying)
    let mut lot_sizes_by_underlying: std::collections::BTreeMap<&str, u32> = std::collections::BTreeMap::new();
    for c in &contracts {
        lot_sizes_by_underlying.entry(c.underlying.as_str()).or_insert(c.lot_size);
    }
    for (u, ls) in &lot_sizes_by_underlying {
        eprintln!("  Lot size [{u}]: {ls} (inferred from data)");
    }
    eprintln!("Capital mode: {}", cli.capital_mode.as_str());
    eprintln!(
        "Execution fill adjustment: buy +₹{:.2}, sell -₹{:.2}",
        cli.fill_offset_inr,
        cli.fill_offset_inr
    );

    let mut engine_cfg = config.options_engine.clone();
    if let Some(mdt) = cli.max_daily_trades {
        eprintln!("max_daily_trades override: {} (config was {})", mdt, engine_cfg.max_daily_trades);
        engine_cfg.max_daily_trades = mdt;
    }
    match cli.capital_mode {
        CapitalMode::Continuous => {
            eprintln!(
                "Continuous mode selected: persistence disabled, starting from configured capital ₹{:.2}",
                engine_cfg.initial_capital
            );
        }
        CapitalMode::Isolated => {
            eprintln!(
                "Isolated mode: starting from configured capital ₹{:.2}",
                engine_cfg.initial_capital
            );
        }
    }

    let run_id = chrono::Local::now().format("%Y%m%d_%H%M%S").to_string();
    let run_dir = PathBuf::from(format!("backtest/options_backtest_{}", run_id));
    fs::create_dir_all(&run_dir)?;

    // Extract replay date from CSV filename (e.g. "2026-02-24_options_ticks.csv" → "2026-02-24")
    // so log files inside the backtest dir are named after the data date, not today.
    let replay_date: Option<String> = options_file
        .file_stem()
        .and_then(|s| s.to_str())
        .and_then(|s| {
            let date_part = s.split('_').next().unwrap_or("");
            if date_part.len() == 10 && date_part.chars().nth(4) == Some('-') {
                Some(date_part.to_string())
            } else {
                None
            }
        });

    // Warmup: 5 min if nearest expiry is 0-1 days from replay date, 15 min otherwise.
    // replay_date from filename may be None (zsh tmp path) — fall back to date prefix
    // of the first recv_ts row, which is always "YYYY-MM-DD..." regardless of path.
    let warmup_secs: u64 = {
        use chrono::NaiveDate;
        let replay_day = replay_date.as_deref()
            .and_then(|s| NaiveDate::parse_from_str(s, "%Y-%m-%d").ok())
            .or_else(|| {
                let mut peek = csv::Reader::from_path(&options_file).ok()?;
                let hdrs = peek.headers().ok()?.clone();
                let i = col_idx(&hdrs, "recv_ts").ok()?;
                let rec = peek.records().next()?.ok()?;
                let ts = rec.get(i)?;
                NaiveDate::parse_from_str(&ts[..10.min(ts.len())], "%Y-%m-%d").ok()
            });
        let nearest_expiry = contracts.iter()
            .filter_map(|c| NaiveDate::parse_from_str(&c.expiry, "%Y-%m-%d").ok())
            .min();
        match (replay_day, nearest_expiry) {
            (Some(day), Some(exp)) if (exp - day).num_days() <= 1 => 5 * 60,
            _ => 15 * 60,
        }
    };

    let store = TickStore::new();
    let underlying_tokens: HashMap<String, u32> = HashMap::new();
    let mut engine = OptionsEngine::new_with_date(
        contracts,
        store.clone(),
        underlying_tokens,
        &engine_cfg,
        config.greeks.risk_free_rate,
        config.greeks.dividend_yield,
        run_dir.to_string_lossy().as_ref(),
        replay_date.as_deref(),
    );
    engine.set_execution_fill_offsets(cli.fill_offset_inr, cli.fill_offset_inr);

    let mut rdr = csv::Reader::from_path(&options_file)?;
    let headers = rdr.headers()?.clone();

    let i_ts = col_idx(&headers, "recv_ts")?;
    let i_token = col_idx(&headers, "token")?;
    let i_ltp = col_idx(&headers, "ltp")?;
    let i_open = col_idx(&headers, "open")?;
    let i_high = col_idx(&headers, "high")?;
    let i_low = col_idx(&headers, "low")?;
    let i_close = col_idx(&headers, "close")?;
    let i_oi = col_idx(&headers, "oi")?;
    let i_oi_day_high = col_idx(&headers, "oi_day_high")?;
    let i_oi_day_low = col_idx(&headers, "oi_day_low")?;
    let i_volume = col_idx(&headers, "volume")?;
    let i_avg_price = col_idx(&headers, "avg_price")?;
    let i_buy_qty = col_idx(&headers, "buy_qty")?;
    let i_sell_qty = col_idx(&headers, "sell_qty")?;
    let i_last_qty = col_idx(&headers, "last_qty")?;
    let i_exchange_ts = col_idx(&headers, "exchange_ts")?;

    let mut warmup_armed = false;

    let mut rows_seen: u64 = 0;
    let mut rows_replayed: u64 = 0;
    let mut last_ts_raw = String::new();
    let mut last_ts_ms: u64 = 0;
    let mut replay_clock_ms: u64 = 0;
    let mut clock_regressions: u64 = 0;
    let started = Instant::now();

    for rec in rdr.records() {
        let rec = rec?;
        rows_seen += 1;

        let token = parse_u32(&rec, i_token);
        if token == 0 {
            continue;
        }

        let ltp = parse_f64(&rec, i_ltp);
        if ltp <= 0.0 {
            continue;
        }

        let exchange_ts = parse_u32(&rec, i_exchange_ts) as u64;
        let ts_ms = if exchange_ts > 0 {
            exchange_ts.saturating_mul(1_000)
        } else {
            let ts_raw = rec.get(i_ts).unwrap_or("").trim();
            if ts_raw == last_ts_raw {
                last_ts_ms
            } else {
                let parsed = match parse_ts_ms_fallback(ts_raw) {
                    Some(v) => v,
                    None => continue,
                };
                last_ts_raw.clear();
                last_ts_raw.push_str(ts_raw);
                last_ts_ms = parsed;
                parsed
            }
        };

        let tick = Tick {
            token,
            ltp,
            last_qty: parse_u32(&rec, i_last_qty),
            avg_price: parse_f64(&rec, i_avg_price),
            volume: parse_u32(&rec, i_volume),
            buy_qty: parse_u32(&rec, i_buy_qty),
            sell_qty: parse_u32(&rec, i_sell_qty),
            ohlc: OHLC {
                open: parse_f64(&rec, i_open),
                high: parse_f64(&rec, i_high),
                low: parse_f64(&rec, i_low),
                close: parse_f64(&rec, i_close),
            },
            oi: parse_u32(&rec, i_oi),
            oi_day_high: parse_u32(&rec, i_oi_day_high),
            oi_day_low: parse_u32(&rec, i_oi_day_low),
            exchange_ts: parse_u32(&rec, i_exchange_ts),
            mode: TickMode::Full,
            ..Tick::default()
        };

        let causal_ts_ms = if ts_ms < replay_clock_ms {
            clock_regressions += 1;
            replay_clock_ms
        } else {
            replay_clock_ms = ts_ms;
            ts_ms
        };

        if !warmup_armed && causal_ts_ms > 0 {
            engine.set_warmup_until_ms(causal_ts_ms + warmup_secs * 1_000);
            eprintln!(
                "Warmup set: suppressing signals for first {}min (until replay clock +{}s)",
                warmup_secs / 60, warmup_secs
            );
            warmup_armed = true;
        }

        store.update(tick);
        engine.process_replay_tick(causal_ts_ms);
        rows_replayed += 1;

        if rows_seen % 1_000_000 == 0 {
            eprintln!(
                "Processed {:>9} rows (replayed {:>9}) in {:.1}s",
                rows_seen,
                rows_replayed,
                started.elapsed().as_secs_f64()
            );
        }
    }

    engine.finalize_replay("Backtest end-of-data exit");

    let trades_csv = find_single_file_with_suffix(&run_dir, "_options_trades.csv")?;
    let (summary, trade_details) = summarize_trades(&trades_csv)?;
    let (signals, entry_attempts, entry_rejections, opened, closed, open_positions) = engine.diagnostics();

    let successful_trades = closed as usize;
    let failed_trades = entry_rejections as usize + open_positions;
    let avg_net = if summary.trades > 0 {
        summary.net_pnl / summary.trades as f64
    } else {
        0.0
    };

    let report = format!(
        "==============================================================\n\
         SATAVAHANA — INDIAN OPTIONS BACKTEST (PATCHED LOGIC)\n\
         ==============================================================\n\
         Data file           : {}\n\
         Run directory       : {}\n\
         Capital mode        : {}\n\
         Starting capital    : ₹{:.2}\n\
         Exec buy/sell adj   : +₹{:.2} / -₹{:.2}\n\
         Rows seen/replayed  : {} / {}\n\
         Clock regressions    : {}\n\
         Elapsed             : {:.2}s\n\
         \n\
         Signals generated   : {}\n\
         Entry attempts      : {}\n\
         Entry rejections    : {}\n\
         Positions opened    : {}\n\
         Successful trades   : {}\n\
         Failed trades       : {}\n\
         Open at end         : {}\n\
         \n\
         Wins/Losses/BE      : {} / {} / {}\n\
         Gross P&L           : ₹{:+.2}\n\
         Total charges       : ₹{:.2}\n\
         Net P&L             : ₹{:+.2}\n\
         Avg net per trade   : ₹{:+.2}\n\
         ==============================================================",
        options_file.display(),
        run_dir.display(),
        cli.capital_mode.as_str(),
        engine_cfg.initial_capital,
        cli.fill_offset_inr,
        cli.fill_offset_inr,
        rows_seen,
        rows_replayed,
        clock_regressions,
        started.elapsed().as_secs_f64(),
        signals,
        entry_attempts,
        entry_rejections,
        opened,
        successful_trades,
        failed_trades,
        open_positions,
        summary.wins,
        summary.losses,
        summary.breakeven,
        summary.gross_pnl,
        summary.total_costs,
        summary.net_pnl,
        avg_net,
    );

    println!("{}", report);

    // Per-trade breakdown: spot, strike, P&L
    if !trade_details.is_empty() {
        println!("\n--------------------------------------------------------------");
        println!("TRADE BREAKDOWN (spot → strike → entry/exit → net P&L)");
        println!("--------------------------------------------------------------");
        println!("{:<4} {:<12} {:<4} {:>8} {:>8} {:>8} {:>8} {:>8} {:>8} {:<20} {:>7}",
            "#", "Underlying", "OT", "Spot", "Strike", "Entry", "Exit",
            "Lots×Sz", "Net P&L", "Exit reason", "Hold(m)");
        println!("{}", "-".repeat(105));
        for (i, t) in trade_details.iter().enumerate() {
            let sign = if t.net_pnl >= 0.0 { "+" } else { "" };
            println!("{:<4} {:<12} {:<4} {:>8.0} {:>8.0} {:>8.2} {:>8.2} {:>4}x{:<3} {:>+8.2} {:<20} {:>7.1}",
                i + 1,
                t.underlying,
                t.option_type,
                t.spot_at_entry,
                t.strike,
                t.entry_price,
                t.exit_price,
                t.lots,
                t.lot_size,
                t.net_pnl,
                &t.exit_reason[..t.exit_reason.len().min(20)],
                t.holding_mins,
            );
            let _ = sign;
        }
        println!("{}", "-".repeat(105));
        println!("Strategy breakdown:");
        let mut strat_map: std::collections::HashMap<String, (usize, usize, f64)> = std::collections::HashMap::new();
        for t in &trade_details {
            let e = strat_map.entry(t.strategy.clone()).or_insert((0, 0, 0.0));
            e.0 += 1;
            if t.net_pnl >= 0.0 { e.1 += 1; }
            e.2 += t.net_pnl;
        }
        let mut strats: Vec<_> = strat_map.iter().collect();
        strats.sort_by_key(|(k, _)| k.as_str());
        for (s, (total, wins, pnl)) in &strats {
            println!("  {:<35}  {}W/{}L  net ₹{:+.2}", s, wins, total - wins, pnl);
        }
    } else {
        println!("\nNo completed trades.");
    }

    fs::write(run_dir.join("backtest_report.txt"), format!("{}\n", report))?;
    println!("\nTrades CSV: {}", trades_csv.display());

    Ok(())
}
