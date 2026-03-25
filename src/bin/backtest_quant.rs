/// Backtest runner for the Quant Engine (src/quant_engine.rs).
/// Reads the same options tick CSV format as backtest_options.
///
/// Usage:
///   cargo run --bin backtest_quant --release -- data/2026-02-24_options_ticks.csv
///   cargo run --bin backtest_quant --release -- --all   (runs all CSVs in data/)

use satavahana::models::{OptionContract, OptionType, Tick, TickMode, OHLC};
use satavahana::quant_engine::QuantEngine;
use satavahana::store::TickStore;

use chrono::{FixedOffset, NaiveDateTime, TimeZone, Utc};
use csv::StringRecord;
use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

const DEFAULT_INITIAL_CAPITAL: f64 = 10_000.0;
const WARMUP_SECS: u64 = 15 * 60;

fn parse_ts_ms(s: &str) -> Option<u64> {
    let s = s.trim();
    let dt = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f")
        .ok()
        .or_else(|| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f").ok())
        .or_else(|| NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S").ok())?;

    let ist = FixedOffset::east_opt(5 * 3600 + 30 * 60).unwrap();
    let ist_dt = ist
        .from_local_datetime(&dt)
        .single()
        .or_else(|| ist.from_local_datetime(&dt).earliest())?;
    Some(ist_dt.with_timezone(&Utc).timestamp_millis() as u64)
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

fn parse_opt_type(s: &str) -> Option<OptionType> {
    match s.trim() {
        "CE" => Some(OptionType::CE),
        "PE" => Some(OptionType::PE),
        _ => None,
    }
}

fn default_lot_size(underlying: &str) -> u32 {
    match underlying {
        "NIFTY"      => 65,
        "NIFTYNXT50" => 25,
        _ => 25,
    }
}

fn build_contracts(csv: &Path) -> Result<Vec<OptionContract>, Box<dyn Error>> {
    let mut rdr = csv::Reader::from_path(csv)?;
    let headers = rdr.headers()?.clone();

    let i_token    = col_idx(&headers, "token")?;
    let i_symbol   = col_idx(&headers, "tradingsymbol")?;
    let i_under    = col_idx(&headers, "underlying")?;
    let i_expiry   = col_idx(&headers, "expiry")?;
    let i_strike   = col_idx(&headers, "strike")?;
    let i_opt_type = col_idx(&headers, "option_type")?;

    let mut seen: HashMap<u32, OptionContract> = HashMap::new();
    for rec in rdr.records() {
        let rec = rec?;
        let token = parse_u32(&rec, i_token);
        if token == 0 || seen.contains_key(&token) { continue; }

        let opt_type = match parse_opt_type(rec.get(i_opt_type).unwrap_or("")) {
            Some(t) => t,
            None => continue,
        };
        let underlying = rec.get(i_under).unwrap_or("").trim().to_string();
        if underlying.is_empty() { continue; }
        let expiry = rec.get(i_expiry).unwrap_or("").trim().to_string();
        let strike = parse_f64(&rec, i_strike);
        if strike <= 0.0 { continue; }
        let tradingsymbol = rec.get(i_symbol).unwrap_or("").trim().to_string();
        if tradingsymbol.is_empty() { continue; }
        let lot_size = default_lot_size(&underlying);

        seen.insert(token, OptionContract {
            instrument_token: token,
            tradingsymbol,
            underlying,
            expiry,
            strike,
            option_type: opt_type,
            lot_size,
        });
    }

    let mut out: Vec<OptionContract> = seen.into_values().collect();
    out.sort_by_key(|c| c.instrument_token);
    Ok(out)
}

#[derive(Default)]
struct RunResult {
    label: String,
    trades: usize,
    wins: usize,
    losses: usize,
    net_pnl: f64,
    rows: u64,
    elapsed_s: f64,
}

fn run_file(csv_path: &Path, run_dir: &Path, capital: f64) -> Result<RunResult, Box<dyn Error>> {
    eprintln!("\n--- Running: {} ---", csv_path.display());
    let started = Instant::now();

    let contracts = build_contracts(csv_path)?;
    if contracts.is_empty() {
        return Err(format!("No contracts in {}", csv_path.display()).into());
    }
    eprintln!("Contracts loaded: {}", contracts.len());

    // Deduce run_label from filename
    let label = csv_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("unknown")
        .trim_end_matches(".csv")
        .to_string();

    let out_dir = run_dir.join(&label);
    fs::create_dir_all(&out_dir)?;

    let store = TickStore::new();
    let mut engine = QuantEngine::new(contracts, store.clone(), capital, out_dir.to_str().unwrap());

    let mut rdr = csv::Reader::from_path(csv_path)?;
    let headers = rdr.headers()?.clone();

    let i_ts         = col_idx(&headers, "recv_ts")?;
    let i_token      = col_idx(&headers, "token")?;
    let i_ltp        = col_idx(&headers, "ltp")?;
    let i_open       = col_idx(&headers, "open")?;
    let i_high       = col_idx(&headers, "high")?;
    let i_low        = col_idx(&headers, "low")?;
    let i_close      = col_idx(&headers, "close")?;
    let i_oi         = col_idx(&headers, "oi")?;
    let i_oi_dh      = col_idx(&headers, "oi_day_high")?;
    let i_oi_dl      = col_idx(&headers, "oi_day_low")?;
    let i_vol        = col_idx(&headers, "volume")?;
    let i_avg        = col_idx(&headers, "avg_price")?;
    let i_bq         = col_idx(&headers, "buy_qty")?;
    let i_sq         = col_idx(&headers, "sell_qty")?;
    let i_lq         = col_idx(&headers, "last_qty")?;
    let i_exch_ts    = col_idx(&headers, "exchange_ts")?;

    let mut warmup_armed = false;
    let mut replay_clock: u64 = 0;
    let mut last_ts_raw = String::new();
    let mut last_ts_ms: u64 = 0;
    let mut rows: u64 = 0;

    for rec in rdr.records() {
        let rec = rec?;
        let token = parse_u32(&rec, i_token);
        if token == 0 { continue; }
        let ltp = parse_f64(&rec, i_ltp);
        if ltp <= 0.0 { continue; }

        let exch_ts = parse_u32(&rec, i_exch_ts) as u64;
        let ts_ms = if exch_ts > 0 {
            exch_ts * 1_000
        } else {
            let raw = rec.get(i_ts).unwrap_or("").trim();
            if raw == last_ts_raw {
                last_ts_ms
            } else {
                match parse_ts_ms(raw) {
                    Some(v) => {
                        last_ts_raw.clear();
                        last_ts_raw.push_str(raw);
                        last_ts_ms = v;
                        v
                    }
                    None => continue,
                }
            }
        };

        let causal_ts = if ts_ms < replay_clock { replay_clock } else { replay_clock = ts_ms; ts_ms };

        if !warmup_armed && causal_ts > 0 {
            engine.set_warmup_until_ms(causal_ts + WARMUP_SECS * 1_000);
            eprintln!("Warmup: suppressing signals for first 15min");
            warmup_armed = true;
        }

        let tick = Tick {
            token,
            ltp,
            last_qty:  parse_u32(&rec, i_lq),
            avg_price: parse_f64(&rec, i_avg),
            volume:    parse_u32(&rec, i_vol),
            buy_qty:   parse_u32(&rec, i_bq),
            sell_qty:  parse_u32(&rec, i_sq),
            ohlc: OHLC {
                open:  parse_f64(&rec, i_open),
                high:  parse_f64(&rec, i_high),
                low:   parse_f64(&rec, i_low),
                close: parse_f64(&rec, i_close),
            },
            oi:          parse_u32(&rec, i_oi),
            oi_day_high: parse_u32(&rec, i_oi_dh),
            oi_day_low:  parse_u32(&rec, i_oi_dl),
            exchange_ts: parse_u32(&rec, i_exch_ts),
            mode: TickMode::Full,
            ..Tick::default()
        };

        store.update(tick);
        engine.on_tick(causal_ts);
        rows += 1;

        if rows % 1_000_000 == 0 {
            eprintln!("  {:>10} rows in {:.1}s", rows, started.elapsed().as_secs_f64());
        }
    }

    engine.finalize("Backtest end-of-data");

    let (trades, wins, losses) = engine.diagnostics();
    let net_pnl = engine.net_pnl();

    Ok(RunResult {
        label,
        trades,
        wins,
        losses,
        net_pnl,
        rows,
        elapsed_s: started.elapsed().as_secs_f64(),
    })
}

fn find_option_csvs(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let p = entry.path();
            if let Some(name) = p.file_name().and_then(|n| n.to_str()) {
                if name.ends_with("_options_ticks.csv") {
                    out.push(p);
                }
            }
        }
    }
    out.sort();
    out
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

    let mut args = std::env::args();
    let _bin = args.next();

    let mut csv_paths: Vec<PathBuf> = Vec::new();
    let mut run_all = false;
    let mut capital = DEFAULT_INITIAL_CAPITAL;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--all" => run_all = true,
            "--capital" => {
                if let Some(v) = args.next() {
                    capital = v.parse::<f64>()
                        .map_err(|e| format!("--capital: {}", e))?;
                }
            }
            "-h" | "--help" => {
                eprintln!(
                    "Usage: backtest_quant [--all] [--capital N] [csv_files...]\n\
                     \n\
                     --all           Run all *_options_ticks.csv in data/ directory\n\
                     --capital N     Starting capital in ₹ (default: {})\n\
                     csv_files       One or more specific CSV paths\n\
                     \n\
                     Examples:\n  backtest_quant data/2026-02-24_options_ticks.csv\n\
                     \n  backtest_quant --all\n",
                    DEFAULT_INITIAL_CAPITAL
                );
                return Ok(());
            }
            _ if arg.starts_with('-') => {
                return Err(format!("Unknown flag: {}", arg).into());
            }
            _ => csv_paths.push(PathBuf::from(arg)),
        }
    }

    if run_all {
        csv_paths = find_option_csvs(Path::new("data"));
        if csv_paths.is_empty() {
            return Err("No *_options_ticks.csv files found in data/".into());
        }
    }

    if csv_paths.is_empty() {
        // Default: try the most recent file
        let mut candidates = find_option_csvs(Path::new("data"));
        if candidates.is_empty() {
            return Err("No CSV provided and no *_options_ticks.csv in data/. Pass a path.".into());
        }
        candidates.sort();
        csv_paths.push(candidates.pop().unwrap());
    }

    let run_id = chrono::Local::now().format("%Y%m%d_%H%M%S").to_string();
    let run_dir = PathBuf::from(format!("backtest/quant_{}", run_id));
    fs::create_dir_all(&run_dir)?;

    eprintln!("Starting capital: ₹{:.2}", capital);
    eprintln!("Run directory:    {}", run_dir.display());

    let mut results: Vec<RunResult> = Vec::new();
    for path in &csv_paths {
        match run_file(path, &run_dir, capital) {
            Ok(r) => results.push(r),
            Err(e) => eprintln!("ERROR on {}: {}", path.display(), e),
        }
    }

    // Combined report
    let total_trades: usize = results.iter().map(|r| r.trades).sum();
    let total_wins:   usize = results.iter().map(|r| r.wins).sum();
    let total_losses: usize = results.iter().map(|r| r.losses).sum();
    let total_net:    f64   = results.iter().map(|r| r.net_pnl).sum();
    let total_rows:   u64   = results.iter().map(|r| r.rows).sum();
    let total_elapsed: f64  = results.iter().map(|r| r.elapsed_s).sum();
    let avg_net = if total_trades > 0 { total_net / total_trades as f64 } else { 0.0 };

    let report = format!(
        "\n==============================================================\n\
         SATAVAHANA — QUANT ENGINE BACKTEST\n\
         ==============================================================\n\
         Starting capital   : ₹{:.2}\n\
         Days run           : {}\n\
         Total rows         : {}\n\
         Total elapsed      : {:.1}s\n\
         \n\
         Per-day breakdown:\n{}\n\
         Totals:\n\
           Trades            : {}\n\
           Wins / Losses     : {} / {}\n\
           Net P&L           : ₹{:+.2}\n\
           Avg net/trade     : ₹{:+.2}\n\
           Return on capital : {:.1}%\n\
         ==============================================================",
        capital,
        results.len(),
        total_rows,
        total_elapsed,
        results.iter().map(|r| format!(
            "  {:40} {:>3} trades  {:>2}W {:>2}L  ₹{:+.2}",
            r.label, r.trades, r.wins, r.losses, r.net_pnl
        )).collect::<Vec<_>>().join("\n"),
        total_trades, total_wins, total_losses,
        total_net, avg_net,
        total_net / capital * 100.0,
    );

    println!("{}", report);
    let report_path = run_dir.join("report.txt");
    fs::write(&report_path, format!("{}\n", report))?;
    eprintln!("Report: {}", report_path.display());

    Ok(())
}
