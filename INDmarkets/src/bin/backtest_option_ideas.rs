//! Backtest-only option strategy lab.
//!
//! This binary is intentionally isolated from the live options engines. It reads recorded
//! `*_option_selling_ticks.csv(.gz)` files, tests simple strategy ideas with bid/ask fills
//! and costs, and prints the best historical candidates. No live order code is imported.

use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use satavahana::models::OptionType;
use std::collections::{BTreeMap, HashMap};
use std::env;
use std::error::Error;
use std::path::{Path, PathBuf};

const LOT_SIZE: u32 = 65;
const MAX_LOTS: u32 = 2;
const DEFAULT_CAPITAL: f64 = 15_000.0;
const DEFAULT_TOP: usize = 10;
const DEFAULT_MIN_TRADES: usize = 2;
const MAX_SPREAD_PCT: f64 = 0.22;
const VERIFIED_CREDIT_SPREAD_MARGIN_INR: f64 = 35_950.0;
const CREDIT_SPREAD_MARGIN_BUFFER: f64 = 0.15;
const DELAY_OPENING_ER: f64 = 0.55;
const DELAY_OPENING_DRIFT_PCT: f64 = 0.0025;
const DELAY_PULLBACK_RETRACE_FRAC: f64 = 0.75;
const DELAY_PULLBACK_GIVEBACK_PTS: f64 = 15.0;
const DELAY_EARLIEST_MIN: u32 = 10 * 60;
const DELAY_LATEST_MIN: u32 = 12 * 60 + 15;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct OptKey {
    strike_key: i64,
    opt: OptionType,
}

impl OptKey {
    fn strike(self) -> f64 {
        self.strike_key as f64 / 100.0
    }
}

#[derive(Debug, Clone, Copy)]
struct QuotePoint {
    ts: NaiveDateTime,
    bid: f64,
    ask: f64,
    delta: f64,
    iv: f64,
}

#[derive(Debug, Clone, Copy)]
struct SpotPoint {
    ts: NaiveDateTime,
    spot: f64,
}

#[derive(Debug)]
struct DayData {
    day: NaiveDate,
    expiry: NaiveDate,
    path: PathBuf,
    spots: Vec<SpotPoint>,
    series: HashMap<OptKey, Vec<QuotePoint>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IdeaKind {
    OrbBreakout,
    OrbFade,
    StraddleMomentum,
    StrangleMomentum,
    CreditEdge,
    CreditEdgeDelayed,
    CreditTrend,
}

#[derive(Debug, Clone)]
struct IdeaSpec {
    name: String,
    kind: IdeaKind,
    entry_min: u32,
    latest_entry_min: u32,
    exit_min: u32,
    buffer_pts: f64,
    min_er: f64,
    max_er: f64,
    min_range_pct: f64,
    max_range_pct: f64,
    edge_threshold: f64,
    delta_target: f64,
    stop_pct: f64,
    target_pct: f64,
    trail_pct: f64,
    alloc_frac: f64,
    enforce_credit_margin: bool,
}

#[derive(Debug, Clone)]
struct LegEntry {
    key: OptKey,
    entry: QuotePoint,
}

#[derive(Debug, Clone)]
struct CreditSpreadEntry {
    short: LegEntry,
    wing: LegEntry,
    width_pts: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CreditDirection {
    BullPut,
    BearCall,
}

#[derive(Debug, Clone, Copy)]
struct OpeningMetrics {
    open: f64,
    close: f64,
    high: f64,
    low: f64,
    er: f64,
    range_pct: f64,
    drift_pct: f64,
    net_pct: f64,
    edge_pos: f64,
}

// Only credit spreads populate this; other idea kinds leave it None.
#[derive(Debug, Clone)]
struct CreditFills {
    short_label: String, // e.g. "PE 24500"
    wing_label: String,
    short_delta: f64,
    // entry: SELL short at its bid, BUY wing at its ask
    short_sell: f64, // short bid at entry
    short_ask_in: f64,
    wing_buy: f64, // wing ask at entry
    wing_bid_in: f64,
    entry_credit: f64,
    // exit: BUY short back at its ask, SELL wing at its bid
    short_buy: f64,  // short ask at exit
    wing_sell: f64,  // wing bid at exit
    close_cost: f64,
    width_pts: f64,
    max_loss_unit: f64,
}

#[derive(Debug, Clone)]
struct TradeResult {
    day: NaiveDate,
    traded: bool,
    direction: String,
    entry_ts: Option<NaiveDateTime>,
    exit_ts: Option<NaiveDateTime>,
    lots: u32,
    net_pnl: f64,
    gross_pnl: f64,
    costs: f64,
    reason: String,
    fills: Option<CreditFills>,
}

#[derive(Debug)]
struct IdeaScore {
    spec: IdeaSpec,
    trades: usize,
    wins: usize,
    losses: usize,
    total_net: f64,
    avg_net: f64,
    worst: f64,
    best: f64,
    results: Vec<TradeResult>,
}

#[derive(Debug)]
struct CliArgs {
    paths: Vec<PathBuf>,
    capital: f64,
    top: usize,
    min_trades: usize,
    cross_verify: bool,
    safe_credit: bool,
    delayed_credit: bool,
    enforce_credit_margin: bool,
}

fn print_usage(bin: &str) {
    eprintln!(
        "Usage: {bin} [--all] [--capital N] [--top N] [--min-trades N] [--cross-verify] [--safe-credit|--delayed-credit] [--enforce-credit-margin] [selling_csv_files...]\n\
         \n\
         Backtest-only option strategy lab. Reads *_option_selling_ticks.csv(.gz),\n\
         uses bid/ask fills and costs, and never touches live engine code.\n\
         --safe-credit isolates CRED-EDGE 09:45 edge65 delta0.25 stop25 target50 er<=0.55.\n\
         --delayed-credit keeps that 09:45 profile, but delays high-ER/high-drift opens until a pullback fails.\n\
         --enforce-credit-margin filters credit spreads unless capital covers the verified Zerodha margin estimate.\n\
         \n\
         Examples:\n\
           {bin} --all\n\
           {bin} --all --safe-credit --capital 42000 --enforce-credit-margin --cross-verify\n\
           {bin} --all --delayed-credit --capital 42000 --enforce-credit-margin --cross-verify\n\
           {bin} --capital 15000 --top 15 --all\n\
           {bin} ../data/2026-07-06_option_selling_ticks.csv.gz"
    );
}

fn parse_args() -> Result<CliArgs, Box<dyn Error>> {
    let mut args = env::args();
    let bin = args.next().unwrap_or_else(|| "backtest_option_ideas".to_string());
    let mut paths = Vec::new();
    let mut all = false;
    let mut capital = DEFAULT_CAPITAL;
    let mut top = DEFAULT_TOP;
    let mut min_trades = DEFAULT_MIN_TRADES;
    let mut cross_verify = false;
    let mut safe_credit = false;
    let mut delayed_credit = false;
    let mut enforce_credit_margin = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => {
                print_usage(&bin);
                std::process::exit(0);
            }
            "--all" => all = true,
            "--cross-verify" => cross_verify = true,
            "--safe-credit" => safe_credit = true,
            "--delayed-credit" => delayed_credit = true,
            "--enforce-credit-margin" => enforce_credit_margin = true,
            "--capital" => {
                let value = args.next().ok_or("missing --capital value")?;
                capital = value.parse::<f64>()?;
            }
            "--top" => {
                let value = args.next().ok_or("missing --top value")?;
                top = value.parse::<usize>()?;
            }
            "--min-trades" => {
                let value = args.next().ok_or("missing --min-trades value")?;
                min_trades = value.parse::<usize>()?;
            }
            s if s.starts_with("--capital=") => {
                capital = s.split_once('=').map(|(_, v)| v).unwrap_or("").parse::<f64>()?;
            }
            s if s.starts_with("--top=") => {
                top = s.split_once('=').map(|(_, v)| v).unwrap_or("").parse::<usize>()?;
            }
            s if s.starts_with("--min-trades=") => {
                min_trades = s.split_once('=').map(|(_, v)| v).unwrap_or("").parse::<usize>()?;
            }
            s if s.starts_with('-') => return Err(format!("unknown flag {s}").into()),
            _ => paths.push(PathBuf::from(arg)),
        }
    }

    if all || paths.is_empty() {
        paths = selling_files_in_data();
    }
    if paths.is_empty() {
        return Err("no *_option_selling_ticks.csv(.gz) files found".into());
    }
    if safe_credit && delayed_credit {
        return Err("--safe-credit and --delayed-credit are mutually exclusive".into());
    }

    Ok(CliArgs {
        paths,
        capital,
        top,
        min_trades,
        cross_verify,
        safe_credit,
        delayed_credit,
        enforce_credit_margin,
    })
}

fn selling_files_in_data() -> Vec<PathBuf> {
    let candidates = [PathBuf::from("../data"), PathBuf::from("data")];
    let mut out = Vec::new();
    for dir in candidates {
        let Ok(read_dir) = std::fs::read_dir(dir) else {
            continue;
        };
        for entry in read_dir.flatten() {
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if (name.ends_with("_option_selling_ticks.csv")
                || name.ends_with("_option_selling_ticks.csv.gz"))
                && !name.contains("_trim")
            {
                out.push(path);
            }
        }
        if !out.is_empty() {
            break;
        }
    }
    out.sort();
    out
}

fn verified_credit_margin_with_buffer() -> f64 {
    VERIFIED_CREDIT_SPREAD_MARGIN_INR * (1.0 + CREDIT_SPREAD_MARGIN_BUFFER)
}

fn credit_margin_filter_reason(capital: f64, enabled: bool) -> Option<String> {
    if !enabled || capital >= verified_credit_margin_with_buffer() {
        return None;
    }
    Some(format!(
        "verified Zerodha margin filter: Rs {:.0} < Rs {:.0} required",
        capital,
        verified_credit_margin_with_buffer()
    ))
}

fn parse_opt(s: &str) -> Option<OptionType> {
    match s.trim() {
        "CE" => Some(OptionType::CE),
        "PE" => Some(OptionType::PE),
        _ => None,
    }
}

fn parse_ts(s: &str) -> Result<NaiveDateTime, Box<dyn Error>> {
    NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
        .map_err(|e| e.into())
}

fn col(headers: &csv::StringRecord, name: &str) -> Result<usize, Box<dyn Error>> {
    headers
        .iter()
        .position(|h| h == name)
        .ok_or_else(|| format!("missing column {name}").into())
}

fn parse_f64(rec: &csv::StringRecord, idx: usize) -> f64 {
    rec.get(idx)
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(0.0)
}

fn load_day(path: &Path) -> Result<DayData, Box<dyn Error>> {
    let mut rdr = satavahana::open_csv(path)?;
    let headers = rdr.headers()?.clone();
    let i_ts = col(&headers, "recv_ts")?;
    let i_expiry = col(&headers, "expiry")?;
    let i_strike = col(&headers, "strike")?;
    let i_opt = col(&headers, "option_type")?;
    let i_spot = col(&headers, "spot")?;
    let i_bid = col(&headers, "bid")?;
    let i_ask = col(&headers, "ask")?;
    let i_delta = col(&headers, "delta_mid")?;
    let i_iv = col(&headers, "iv_mid_pct")?;

    let mut day = None;
    let mut expiry = None;
    let mut spots: Vec<SpotPoint> = Vec::new();
    let mut series: HashMap<OptKey, Vec<QuotePoint>> = HashMap::new();

    for rec in rdr.records() {
        let rec = rec?;
        let ts = parse_ts(rec.get(i_ts).unwrap_or(""))?;
        day.get_or_insert(ts.date());
        let row_expiry = NaiveDate::parse_from_str(rec.get(i_expiry).unwrap_or(""), "%Y-%m-%d")?;
        expiry.get_or_insert(row_expiry);

        let spot = parse_f64(&rec, i_spot);
        if spot.is_finite() && spot > 0.0 {
            match spots.last_mut() {
                Some(last) if last.ts == ts => last.spot = spot,
                _ => spots.push(SpotPoint { ts, spot }),
            }
        }

        let Some(opt) = parse_opt(rec.get(i_opt).unwrap_or("")) else {
            continue;
        };
        let bid = parse_f64(&rec, i_bid);
        let ask = parse_f64(&rec, i_ask);
        if !(bid.is_finite() && ask.is_finite() && bid > 0.0 && ask >= bid) {
            continue;
        }
        let strike = parse_f64(&rec, i_strike);
        let key = OptKey {
            strike_key: (strike * 100.0).round() as i64,
            opt,
        };
        series.entry(key).or_default().push(QuotePoint {
            ts,
            bid,
            ask,
            delta: parse_f64(&rec, i_delta),
            iv: parse_f64(&rec, i_iv),
        });
    }

    for points in series.values_mut() {
        points.sort_by_key(|p| p.ts);
        points.dedup_by_key(|p| p.ts);
    }
    spots.sort_by_key(|p| p.ts);
    spots.dedup_by_key(|p| p.ts);

    Ok(DayData {
        day: day.ok_or("empty day")?,
        expiry: expiry.ok_or("empty expiry")?,
        path: path.to_path_buf(),
        spots,
        series,
    })
}

fn at_min(day: NaiveDate, minute: u32) -> NaiveDateTime {
    let h = minute / 60;
    let m = minute % 60;
    day.and_time(NaiveTime::from_hms_opt(h, m, 0).unwrap())
}

fn ts_ms(ts: NaiveDateTime) -> u64 {
    ts.and_utc().timestamp_millis() as u64
}

fn minute_closes(spots: &[SpotPoint], start: NaiveDateTime, end: NaiveDateTime) -> Vec<f64> {
    let mut out: Vec<(u32, f64)> = Vec::new();
    for p in spots {
        if p.ts < start || p.ts > end {
            continue;
        }
        let key = p.ts.hour() * 60 + p.ts.minute();
        match out.last_mut() {
            Some(last) if last.0 == key => last.1 = p.spot,
            _ => out.push((key, p.spot)),
        }
    }
    out.into_iter().map(|(_, spot)| spot).collect()
}

fn minute_last_spots(spots: &[SpotPoint], start: NaiveDateTime, end: NaiveDateTime) -> Vec<SpotPoint> {
    let mut out: Vec<(u32, SpotPoint)> = Vec::new();
    for p in spots {
        if p.ts < start || p.ts > end {
            continue;
        }
        let key = p.ts.hour() * 60 + p.ts.minute();
        match out.last_mut() {
            Some(last) if last.0 == key => last.1 = *p,
            _ => out.push((key, *p)),
        }
    }
    out.into_iter().map(|(_, spot)| spot).collect()
}

fn efficiency_ratio(values: &[f64]) -> Option<f64> {
    if values.len() < 2 {
        return None;
    }
    let net = (values.last()? - values.first()?).abs();
    let mut path = 0.0;
    for pair in values.windows(2) {
        path += (pair[1] - pair[0]).abs();
    }
    if path <= 0.0 {
        Some(0.0)
    } else {
        Some((net / path).clamp(0.0, 1.0))
    }
}

fn opening_metrics(day: &DayData) -> Option<OpeningMetrics> {
    let start = at_min(day.day, 9 * 60 + 15);
    let end = at_min(day.day, 9 * 60 + 45);
    let closes = minute_closes(&day.spots, start, end);
    let er = efficiency_ratio(&closes)?;
    let open = *closes.first()?;
    let close = *closes.last()?;
    let mut high = f64::NEG_INFINITY;
    let mut low = f64::INFINITY;
    for p in &day.spots {
        if p.ts < start || p.ts > end {
            continue;
        }
        high = high.max(p.spot);
        low = low.min(p.spot);
    }
    if !high.is_finite() || !low.is_finite() || close <= 0.0 {
        return None;
    }
    let range_pct = (high - low) / close;
    let drift_pct = (close - open).abs() / close;
    let net_pct = (close - open) / close;
    let edge_pos = if high > low {
        ((close - low) / (high - low)).clamp(0.0, 1.0)
    } else {
        0.5
    };
    Some(OpeningMetrics {
        open,
        close,
        high,
        low,
        er,
        range_pct,
        drift_pct,
        net_pct,
        edge_pos,
    })
}

fn upper_bound_quote(points: &[QuotePoint], ts: NaiveDateTime) -> Option<usize> {
    let idx = points.partition_point(|p| p.ts <= ts);
    idx.checked_sub(1)
}

fn first_quote_after(points: &[QuotePoint], ts: NaiveDateTime) -> usize {
    points.partition_point(|p| p.ts <= ts)
}

fn spread_ok(q: QuotePoint) -> bool {
    if q.ask <= 0.0 || q.bid <= 0.0 || q.ask < q.bid {
        return false;
    }
    let mid = (q.ask + q.bid) * 0.5;
    mid > 0.0 && (q.ask - q.bid) / mid <= MAX_SPREAD_PCT
}

fn spot_at_or_before(day: &DayData, ts: NaiveDateTime) -> Option<f64> {
    let idx = day.spots.partition_point(|p| p.ts <= ts);
    idx.checked_sub(1).map(|i| day.spots[i].spot)
}

fn select_by_delta(
    day: &DayData,
    ts: NaiveDateTime,
    opt: OptionType,
    target_delta: f64,
) -> Option<LegEntry> {
    let spot = spot_at_or_before(day, ts).unwrap_or(0.0);
    let mut best: Option<(f64, LegEntry)> = None;
    for (&key, points) in &day.series {
        if key.opt != opt {
            continue;
        }
        let Some(q) = upper_bound_quote(points, ts).map(|idx| points[idx]) else {
            continue;
        };
        if !spread_ok(q) {
            continue;
        }
        let delta_score = (q.delta.abs() - target_delta).abs();
        let spot_score = if spot > 0.0 {
            (key.strike() - spot).abs() / 10_000.0
        } else {
            0.0
        };
        let iv_penalty = if q.iv.is_finite() { 0.0 } else { 1.0 };
        let score = delta_score + spot_score + iv_penalty;
        let entry = LegEntry { key, entry: q };
        match &best {
            Some((best_score, _)) if *best_score <= score => {}
            _ => best = Some((score, entry)),
        }
    }
    best.map(|(_, entry)| entry)
}

fn select_atm_pair(day: &DayData, ts: NaiveDateTime) -> Option<(LegEntry, LegEntry)> {
    let spot = spot_at_or_before(day, ts)?;
    let mut best: Option<(f64, LegEntry, LegEntry)> = None;
    for (&key, ce_points) in &day.series {
        if key.opt != OptionType::CE {
            continue;
        }
        let pe_key = OptKey {
            strike_key: key.strike_key,
            opt: OptionType::PE,
        };
        let Some(pe_points) = day.series.get(&pe_key) else {
            continue;
        };
        let Some(ce) = upper_bound_quote(ce_points, ts).map(|idx| ce_points[idx]) else {
            continue;
        };
        let Some(pe) = upper_bound_quote(pe_points, ts).map(|idx| pe_points[idx]) else {
            continue;
        };
        if !spread_ok(ce) || !spread_ok(pe) {
            continue;
        }
        let score = (key.strike() - spot).abs();
        let ce_entry = LegEntry { key, entry: ce };
        let pe_entry = LegEntry {
            key: pe_key,
            entry: pe,
        };
        match &best {
            Some((best_score, _, _)) if *best_score <= score => {}
            _ => best = Some((score, ce_entry, pe_entry)),
        }
    }
    best.map(|(_, ce, pe)| (ce, pe))
}

fn option_order_cost(price: f64, qty: u32, side: Side, exit_ms: u64) -> f64 {
    let premium = price.max(0.0) * qty as f64;
    let brokerage = 20.0;
    let exch = 0.000311 * premium;
    let sebi = 0.000001 * premium;
    let gst = 0.18 * (brokerage + exch + sebi);
    let stt = if side == Side::Sell {
        options_sell_stt_rate(exit_ms) * premium
    } else {
        0.0
    };
    let stamp = if side == Side::Buy {
        0.00003 * premium
    } else {
        0.0
    };
    brokerage + exch + sebi + gst + stt + stamp
}

fn options_sell_stt_rate(exit_ms: u64) -> f64 {
    let dt = chrono::DateTime::from_timestamp_millis(exit_ms as i64)
        .map(|d| d.date_naive())
        .unwrap_or_else(|| NaiveDate::from_ymd_opt(2026, 7, 1).unwrap());
    let hike = NaiveDate::from_ymd_opt(2026, 4, 1).unwrap();
    if dt >= hike {
        0.0015
    } else {
        0.0010
    }
}

fn size_lots(entry_debit_per_unit: f64, capital: f64, alloc_frac: f64) -> u32 {
    let per_lot = entry_debit_per_unit * LOT_SIZE as f64;
    if per_lot <= 0.0 {
        return 0;
    }
    ((capital * alloc_frac / per_lot).floor() as u32).clamp(0, MAX_LOTS)
}

fn simulate_long(
    day: &DayData,
    spec: &IdeaSpec,
    legs: &[LegEntry],
    entry_ts: NaiveDateTime,
    direction: String,
    capital: f64,
) -> TradeResult {
    let entry_debit: f64 = legs.iter().map(|l| l.entry.ask).sum();
    let lots = size_lots(entry_debit, capital, spec.alloc_frac);
    if lots == 0 {
        return TradeResult {
            day: day.day,
            traded: false,
            direction,
            entry_ts: Some(entry_ts),
            exit_ts: None,
            lots: 0,
            net_pnl: 0.0,
            gross_pnl: 0.0,
            costs: 0.0,
            reason: "debit exceeds allocation".to_string(),
            fills: None,
        };
    }

    let qty = lots * LOT_SIZE;
    let exit_cutoff = at_min(day.day, spec.exit_min);
    let mut idxs: Vec<usize> = legs
        .iter()
        .filter_map(|l| day.series.get(&l.key).map(|s| first_quote_after(s, entry_ts)))
        .collect();
    if idxs.len() != legs.len() {
        return TradeResult {
            day: day.day,
            traded: false,
            direction,
            entry_ts: Some(entry_ts),
            exit_ts: None,
            lots,
            net_pnl: 0.0,
            gross_pnl: 0.0,
            costs: 0.0,
            reason: "missing exit series".to_string(),
            fills: None,
        };
    }

    let mut current_bids: Vec<f64> = legs.iter().map(|l| l.entry.bid).collect();
    let mut peak_value = current_bids.iter().sum::<f64>();
    let mut exit_value = peak_value;
    let mut exit_ts = entry_ts;
    let mut reason = "time".to_string();

    loop {
        let mut next_ts: Option<NaiveDateTime> = None;
        for (leg_idx, leg) in legs.iter().enumerate() {
            let Some(series) = day.series.get(&leg.key) else {
                continue;
            };
            if let Some(q) = series.get(idxs[leg_idx]) {
                if q.ts <= exit_cutoff {
                    next_ts = Some(next_ts.map_or(q.ts, |old| old.min(q.ts)));
                }
            }
        }
        let Some(t) = next_ts else {
            break;
        };
        exit_ts = t;
        for (leg_idx, leg) in legs.iter().enumerate() {
            let Some(series) = day.series.get(&leg.key) else {
                continue;
            };
            while let Some(q) = series.get(idxs[leg_idx]) {
                if q.ts != t {
                    break;
                }
                if spread_ok(*q) {
                    current_bids[leg_idx] = q.bid;
                }
                idxs[leg_idx] += 1;
            }
        }

        let value = current_bids.iter().sum::<f64>();
        peak_value = peak_value.max(value);
        exit_value = value;
        let ret = (value - entry_debit) / entry_debit;
        let peak_ret = (peak_value - entry_debit) / entry_debit;
        if ret <= -spec.stop_pct {
            reason = format!("stop -{:.0}%", spec.stop_pct * 100.0);
            break;
        }
        if ret >= spec.target_pct {
            reason = format!("target +{:.0}%", spec.target_pct * 100.0);
            break;
        }
        if peak_ret > 0.10 && peak_value > 0.0 && (peak_value - value) / peak_value >= spec.trail_pct {
            reason = format!("trail {:.0}%", spec.trail_pct * 100.0);
            break;
        }
    }

    let gross = (exit_value - entry_debit) * qty as f64;
    let exit_ms = ts_ms(exit_ts);
    let mut costs = 0.0;
    for leg in legs {
        costs += option_order_cost(leg.entry.ask, qty, Side::Buy, exit_ms);
    }
    for (leg_idx, _leg) in legs.iter().enumerate() {
        costs += option_order_cost(current_bids[leg_idx], qty, Side::Sell, exit_ms);
    }

    TradeResult {
        day: day.day,
        traded: true,
        direction,
        entry_ts: Some(entry_ts),
        exit_ts: Some(exit_ts),
        lots,
        net_pnl: gross - costs,
        gross_pnl: gross,
        costs,
        reason,
        fills: None,
    }
}

fn select_credit_spread(
    day: &DayData,
    ts: NaiveDateTime,
    direction: CreditDirection,
    target_delta: f64,
    width_pts: f64,
) -> Option<CreditSpreadEntry> {
    let opt = match direction {
        CreditDirection::BullPut => OptionType::PE,
        CreditDirection::BearCall => OptionType::CE,
    };
    let width_key = (width_pts * 100.0).round() as i64;
    let spot = spot_at_or_before(day, ts).unwrap_or(0.0);
    let mut best: Option<(f64, CreditSpreadEntry)> = None;

    for (&short_key, short_points) in &day.series {
        if short_key.opt != opt {
            continue;
        }
        let wing_key = OptKey {
            strike_key: match direction {
                CreditDirection::BullPut => short_key.strike_key - width_key,
                CreditDirection::BearCall => short_key.strike_key + width_key,
            },
            opt,
        };
        let Some(wing_points) = day.series.get(&wing_key) else {
            continue;
        };
        let Some(short_q) = upper_bound_quote(short_points, ts).map(|idx| short_points[idx]) else {
            continue;
        };
        let Some(wing_q) = upper_bound_quote(wing_points, ts).map(|idx| wing_points[idx]) else {
            continue;
        };
        if !spread_ok(short_q) || !spread_ok(wing_q) {
            continue;
        }
        let credit = short_q.bid - wing_q.ask;
        let max_loss = width_pts - credit;
        if credit <= 0.0 || max_loss <= 0.0 {
            continue;
        }
        let delta_score = (short_q.delta.abs() - target_delta).abs();
        let spot_score = if spot > 0.0 {
            (short_key.strike() - spot).abs() / 20_000.0
        } else {
            0.0
        };
        let score = delta_score + spot_score;
        let entry = CreditSpreadEntry {
            short: LegEntry {
                key: short_key,
                entry: short_q,
            },
            wing: LegEntry {
                key: wing_key,
                entry: wing_q,
            },
            width_pts,
        };
        match &best {
            Some((best_score, _)) if *best_score <= score => {}
            _ => best = Some((score, entry)),
        }
    }

    best.map(|(_, entry)| entry)
}

fn size_credit_lots(
    credit: f64,
    width_pts: f64,
    capital: f64,
    risk_frac: f64,
    enforce_credit_margin: bool,
) -> u32 {
    let max_loss = (width_pts - credit) * LOT_SIZE as f64;
    if max_loss <= 0.0 {
        return 0;
    }
    let risk_sized = ((capital * risk_frac / max_loss).floor() as u32).clamp(0, MAX_LOTS);
    if !enforce_credit_margin {
        return risk_sized;
    }
    let margin_sized = (capital / verified_credit_margin_with_buffer()).floor() as u32;
    risk_sized.min(margin_sized).clamp(0, MAX_LOTS)
}

fn simulate_credit_spread(
    day: &DayData,
    spec: &IdeaSpec,
    spread: CreditSpreadEntry,
    entry_ts: NaiveDateTime,
    direction: CreditDirection,
    capital: f64,
) -> TradeResult {
    let entry_credit = spread.short.entry.bid - spread.wing.entry.ask;
    let max_loss_unit = spread.width_pts - entry_credit;
    let lots = size_credit_lots(
        entry_credit,
        spread.width_pts,
        capital,
        spec.alloc_frac,
        spec.enforce_credit_margin,
    );
    let label = match direction {
        CreditDirection::BullPut => "bull put credit",
        CreditDirection::BearCall => "bear call credit",
    }
    .to_string();

    if lots == 0 || entry_credit <= 0.0 || max_loss_unit <= 0.0 {
        return TradeResult {
            day: day.day,
            traded: false,
            direction: label,
            entry_ts: Some(entry_ts),
            exit_ts: None,
            lots: 0,
            net_pnl: 0.0,
            gross_pnl: 0.0,
            costs: 0.0,
            reason: "credit spread cannot size".to_string(),
            fills: None,
        };
    }

    let qty = lots * LOT_SIZE;
    let exit_cutoff = at_min(day.day, spec.exit_min);
    let keys = [spread.short.key, spread.wing.key];
    let mut idxs = Vec::new();
    for key in keys {
        let Some(series) = day.series.get(&key) else {
            return no_trade(day.day, "missing credit exit series");
        };
        idxs.push(first_quote_after(series, entry_ts));
    }

    let mut short_ask = spread.short.entry.ask;
    let mut wing_bid = spread.wing.entry.bid;
    let mut exit_ts = entry_ts;
    let mut reason = "time".to_string();

    loop {
        let mut next_ts: Option<NaiveDateTime> = None;
        for (idx, key) in keys.iter().enumerate() {
            let Some(series) = day.series.get(key) else {
                continue;
            };
            if let Some(q) = series.get(idxs[idx]) {
                if q.ts <= exit_cutoff {
                    next_ts = Some(next_ts.map_or(q.ts, |old| old.min(q.ts)));
                }
            }
        }
        let Some(t) = next_ts else {
            break;
        };
        exit_ts = t;
        for (idx, key) in keys.iter().enumerate() {
            let Some(series) = day.series.get(key) else {
                continue;
            };
            while let Some(q) = series.get(idxs[idx]) {
                if q.ts != t {
                    break;
                }
                if spread_ok(*q) {
                    if idx == 0 {
                        short_ask = q.ask;
                    } else {
                        wing_bid = q.bid;
                    }
                }
                idxs[idx] += 1;
            }
        }

        let close_cost = (short_ask - wing_bid).max(0.0);
        let gain_unit = entry_credit - close_cost;
        if gain_unit >= spec.target_pct * entry_credit {
            reason = format!("target {:.0}% credit", spec.target_pct * 100.0);
            break;
        }
        if gain_unit <= -spec.stop_pct * max_loss_unit {
            reason = format!("stop {:.0}% maxloss", spec.stop_pct * 100.0);
            break;
        }
    }

    let close_cost = (short_ask - wing_bid).max(0.0);
    let gain_unit = entry_credit - close_cost;
    let gross = gain_unit * qty as f64;
    let exit_ms = ts_ms(exit_ts);
    let costs = option_order_cost(spread.short.entry.bid, qty, Side::Sell, exit_ms)
        + option_order_cost(spread.wing.entry.ask, qty, Side::Buy, exit_ms)
        + option_order_cost(short_ask, qty, Side::Buy, exit_ms)
        + option_order_cost(wing_bid, qty, Side::Sell, exit_ms);

    let fills = CreditFills {
        short_label: format!("{:?} {:.0}", spread.short.key.opt, spread.short.key.strike()),
        wing_label: format!("{:?} {:.0}", spread.wing.key.opt, spread.wing.key.strike()),
        short_delta: spread.short.entry.delta,
        short_sell: spread.short.entry.bid,
        short_ask_in: spread.short.entry.ask,
        wing_buy: spread.wing.entry.ask,
        wing_bid_in: spread.wing.entry.bid,
        entry_credit,
        short_buy: short_ask,
        wing_sell: wing_bid,
        close_cost,
        width_pts: spread.width_pts,
        max_loss_unit,
    };

    TradeResult {
        day: day.day,
        traded: true,
        direction: label,
        entry_ts: Some(entry_ts),
        exit_ts: Some(exit_ts),
        lots,
        net_pnl: gross - costs,
        gross_pnl: gross,
        costs,
        reason,
        fills: Some(fills),
    }
}

fn find_orb_entry(day: &DayData, spec: &IdeaSpec, high: f64, low: f64) -> Option<(NaiveDateTime, OptionType, String)> {
    let start = at_min(day.day, spec.entry_min);
    let latest = at_min(day.day, spec.latest_entry_min);
    let up_trigger = high + spec.buffer_pts;
    let down_trigger = low - spec.buffer_pts;
    for p in &day.spots {
        if p.ts <= start || p.ts > latest {
            continue;
        }
        if p.spot >= up_trigger {
            return match spec.kind {
                IdeaKind::OrbBreakout => Some((p.ts, OptionType::CE, "up breakout CE".to_string())),
                IdeaKind::OrbFade => Some((p.ts, OptionType::PE, "up fade PE".to_string())),
                _ => None,
            };
        }
        if p.spot <= down_trigger {
            return match spec.kind {
                IdeaKind::OrbBreakout => Some((p.ts, OptionType::PE, "down breakout PE".to_string())),
                IdeaKind::OrbFade => Some((p.ts, OptionType::CE, "down fade CE".to_string())),
                _ => None,
            };
        }
    }
    None
}

fn credit_edge_direction(opening: OpeningMetrics, edge_threshold: f64) -> Option<CreditDirection> {
    if opening.edge_pos >= edge_threshold {
        Some(CreditDirection::BullPut)
    } else if opening.edge_pos <= 1.0 - edge_threshold {
        Some(CreditDirection::BearCall)
    } else {
        None
    }
}

fn needs_delayed_credit_entry(opening: OpeningMetrics) -> bool {
    opening.er > DELAY_OPENING_ER && opening.drift_pct >= DELAY_OPENING_DRIFT_PCT
}

fn find_delayed_credit_entry(
    day: &DayData,
    opening: OpeningMetrics,
    direction: CreditDirection,
) -> Option<NaiveDateTime> {
    let impulse = (opening.close - opening.open).abs();
    if impulse <= 0.0 {
        return None;
    }

    let start = at_min(day.day, DELAY_EARLIEST_MIN);
    let latest = at_min(day.day, DELAY_LATEST_MIN);
    let spots = minute_last_spots(&day.spots, start, latest);

    match direction {
        CreditDirection::BearCall => {
            let retrace_level = opening.close + impulse * DELAY_PULLBACK_RETRACE_FRAC;
            let mut pulled_back = false;
            let mut pullback_peak = f64::NEG_INFINITY;
            for p in spots {
                if p.spot >= retrace_level {
                    pulled_back = true;
                    pullback_peak = pullback_peak.max(p.spot);
                }
                if pulled_back {
                    pullback_peak = pullback_peak.max(p.spot);
                    if pullback_peak - p.spot >= DELAY_PULLBACK_GIVEBACK_PTS {
                        return Some(p.ts);
                    }
                }
            }
        }
        CreditDirection::BullPut => {
            let retrace_level = opening.close - impulse * DELAY_PULLBACK_RETRACE_FRAC;
            let mut pulled_back = false;
            let mut pullback_low = f64::INFINITY;
            for p in spots {
                if p.spot <= retrace_level {
                    pulled_back = true;
                    pullback_low = pullback_low.min(p.spot);
                }
                if pulled_back {
                    pullback_low = pullback_low.min(p.spot);
                    if p.spot - pullback_low >= DELAY_PULLBACK_GIVEBACK_PTS {
                        return Some(p.ts);
                    }
                }
            }
        }
    }

    None
}

fn run_spec_on_day(day: &DayData, spec: &IdeaSpec, capital: f64) -> TradeResult {
    let Some(opening) = opening_metrics(day) else {
        return no_trade(day.day, "insufficient opening metrics");
    };
    if opening.er < spec.min_er || opening.er > spec.max_er {
        return no_trade(day.day, "ER filter");
    }
    if opening.range_pct < spec.min_range_pct || opening.range_pct > spec.max_range_pct {
        return no_trade(day.day, "range filter");
    }

    match spec.kind {
        IdeaKind::OrbBreakout | IdeaKind::OrbFade => {
            let Some((entry_ts, opt, direction)) = find_orb_entry(day, spec, opening.high, opening.low) else {
                return no_trade(day.day, "no OR trigger");
            };
            let Some(leg) = select_by_delta(day, entry_ts, opt, spec.delta_target) else {
                return no_trade(day.day, "no liquid option at trigger");
            };
            simulate_long(day, spec, &[leg], entry_ts, direction, capital)
        }
        IdeaKind::StraddleMomentum => {
            let entry_ts = at_min(day.day, spec.entry_min);
            let Some((ce, pe)) = select_atm_pair(day, entry_ts) else {
                return no_trade(day.day, "no liquid ATM pair");
            };
            simulate_long(day, spec, &[ce, pe], entry_ts, "ATM straddle".to_string(), capital)
        }
        IdeaKind::StrangleMomentum => {
            let entry_ts = at_min(day.day, spec.entry_min);
            let Some(ce) = select_by_delta(day, entry_ts, OptionType::CE, spec.delta_target) else {
                return no_trade(day.day, "no liquid CE");
            };
            let Some(pe) = select_by_delta(day, entry_ts, OptionType::PE, spec.delta_target) else {
                return no_trade(day.day, "no liquid PE");
            };
            simulate_long(day, spec, &[ce, pe], entry_ts, "delta strangle".to_string(), capital)
        }
        IdeaKind::CreditEdge => {
            if let Some(reason) =
                credit_margin_filter_reason(capital, spec.enforce_credit_margin)
            {
                return no_trade(day.day, &reason);
            }
            let entry_ts = at_min(day.day, spec.entry_min);
            let Some(direction) = credit_edge_direction(opening, spec.edge_threshold) else {
                return no_trade(day.day, "opening not near edge");
            };
            let Some(spread) = select_credit_spread(day, entry_ts, direction, spec.delta_target, 100.0) else {
                return no_trade(day.day, "no liquid credit spread");
            };
            simulate_credit_spread(day, spec, spread, entry_ts, direction, capital)
        }
        IdeaKind::CreditEdgeDelayed => {
            if let Some(reason) =
                credit_margin_filter_reason(capital, spec.enforce_credit_margin)
            {
                return no_trade(day.day, &reason);
            }
            let Some(direction) = credit_edge_direction(opening, spec.edge_threshold) else {
                return no_trade(day.day, "opening not near edge");
            };
            let (entry_ts, delayed) = if needs_delayed_credit_entry(opening) {
                let Some(ts) = find_delayed_credit_entry(day, opening, direction) else {
                    return no_trade(day.day, "no delayed pullback trigger");
                };
                (ts, true)
            } else {
                (at_min(day.day, spec.entry_min), false)
            };
            let Some(spread) = select_credit_spread(day, entry_ts, direction, spec.delta_target, 100.0) else {
                return no_trade(day.day, "no liquid credit spread");
            };
            let mut result = simulate_credit_spread(day, spec, spread, entry_ts, direction, capital);
            if delayed && result.traded {
                result.direction = format!("{} delayed", result.direction);
            }
            result
        }
        IdeaKind::CreditTrend => {
            if let Some(reason) =
                credit_margin_filter_reason(capital, spec.enforce_credit_margin)
            {
                return no_trade(day.day, &reason);
            }
            let entry_ts = at_min(day.day, spec.entry_min);
            if opening.drift_pct < 0.0008 {
                return no_trade(day.day, "opening drift too small");
            }
            let direction = if opening.net_pct > 0.0 {
                CreditDirection::BullPut
            } else {
                CreditDirection::BearCall
            };
            let Some(spread) = select_credit_spread(day, entry_ts, direction, spec.delta_target, 100.0) else {
                return no_trade(day.day, "no liquid credit spread");
            };
            simulate_credit_spread(day, spec, spread, entry_ts, direction, capital)
        }
    }
}

fn no_trade(day: NaiveDate, reason: &str) -> TradeResult {
    TradeResult {
        day,
        traded: false,
        direction: String::new(),
        entry_ts: None,
        exit_ts: None,
        lots: 0,
        net_pnl: 0.0,
        gross_pnl: 0.0,
        costs: 0.0,
        reason: reason.to_string(),
        fills: None,
    }
}

fn idea_grid(enforce_credit_margin: bool) -> Vec<IdeaSpec> {
    let mut out = Vec::new();
    let mut push = |kind: IdeaKind,
                    prefix: &str,
                    entry_min: u32,
                    latest_entry_min: u32,
                    buffer_pts: f64,
                    min_er: f64,
                    max_er: f64,
                    min_range_pct: f64,
                    max_range_pct: f64,
                    edge_threshold: f64,
                    delta_target: f64,
                    stop_pct: f64,
                    target_pct: f64,
                    trail_pct: f64,
                    alloc_frac: f64| {
        out.push(IdeaSpec {
            name: format!(
                "{prefix} e{} b{:.0} er{:.2}-{:.2} r{:.2}-{:.2} edge{:.0} d{:.2} s{:.0} t{:.0} tr{:.0}",
                fmt_min(entry_min),
                buffer_pts,
                min_er,
                max_er,
                min_range_pct * 100.0,
                max_range_pct * 100.0,
                edge_threshold * 100.0,
                delta_target,
                stop_pct * 100.0,
                target_pct * 100.0,
                trail_pct * 100.0
            ),
            kind,
            entry_min,
            latest_entry_min,
            exit_min: 14 * 60 + 45,
            buffer_pts,
            min_er,
            max_er,
            min_range_pct,
            max_range_pct,
            edge_threshold,
            delta_target,
            stop_pct,
            target_pct,
            trail_pct,
            alloc_frac,
            enforce_credit_margin,
        });
    };

    for buffer in [0.0, 15.0, 30.0] {
        for min_er in [0.25, 0.45, 0.60] {
            for min_range in [0.0015, 0.0030] {
                for delta in [0.35, 0.45] {
                    for stop in [0.20, 0.30] {
                        for target in [0.30, 0.50] {
                            for trail in [0.15, 0.25] {
                                push(
                                    IdeaKind::OrbBreakout,
                                    "ORB-BRK",
                                    9 * 60 + 45,
                                    11 * 60 + 30,
                                    buffer,
                                    min_er,
                                    1.0,
                                    min_range,
                                    0.0200,
                                    0.70,
                                    delta,
                                    stop,
                                    target,
                                    trail,
                                    0.50,
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    for buffer in [0.0, 15.0] {
        for max_er in [0.25, 0.40] {
            for delta in [0.35, 0.45] {
                for stop in [0.18, 0.25] {
                    for target in [0.20, 0.35] {
                        push(
                            IdeaKind::OrbFade,
                            "ORB-FADE",
                            9 * 60 + 45,
                            11 * 60,
                            buffer,
                            0.0,
                            max_er,
                            0.0,
                            0.0060,
                            0.70,
                            delta,
                            stop,
                            target,
                            0.15,
                            0.45,
                        );
                    }
                }
            }
        }
    }

    for entry in [9 * 60 + 45, 10 * 60, 10 * 60 + 30] {
        for min_er in [0.35, 0.55] {
            for min_range in [0.0020, 0.0040] {
                for stop in [0.18, 0.25] {
                    for target in [0.25, 0.45] {
                        for trail in [0.15, 0.25] {
                            push(
                                IdeaKind::StraddleMomentum,
                                "STRAD-MOM",
                                entry,
                                entry,
                                0.0,
                                min_er,
                                1.0,
                                min_range,
                                0.0200,
                                0.70,
                                0.50,
                                stop,
                                target,
                                trail,
                                0.70,
                            );
                        }
                    }
                }
            }
        }
    }

    for entry in [9 * 60 + 45, 10 * 60] {
        for min_er in [0.35, 0.55] {
            for min_range in [0.0020, 0.0040] {
                for delta in [0.25, 0.30] {
                    for stop in [0.18, 0.25] {
                        for target in [0.25, 0.45] {
                            push(
                                IdeaKind::StrangleMomentum,
                                "STRNG-MOM",
                                entry,
                                entry,
                                0.0,
                                min_er,
                                1.0,
                                min_range,
                                0.0200,
                                0.70,
                                delta,
                                stop,
                                target,
                                0.20,
                                0.60,
                            );
                        }
                    }
                }
            }
        }
    }

    for entry in [9 * 60 + 45, 10 * 60] {
        for max_er in [0.35, 0.55, 1.0] {
            for edge in [0.65, 0.75] {
                for delta in [0.20, 0.25, 0.30] {
                    for stop in [0.25, 0.40] {
                        for target in [0.35, 0.50] {
                            push(
                                IdeaKind::CreditEdge,
                                "CRED-EDGE",
                                entry,
                                entry,
                                0.0,
                                0.0,
                                max_er,
                                0.0,
                                0.0200,
                                edge,
                                delta,
                                stop,
                                target,
                                0.0,
                                0.90,
                            );
                        }
                    }
                }
            }
        }
    }

    for entry in [9 * 60 + 45, 10 * 60] {
        for min_er in [0.30, 0.45, 0.60] {
            for delta in [0.20, 0.25, 0.30] {
                for stop in [0.25, 0.40] {
                    for target in [0.35, 0.50] {
                        push(
                            IdeaKind::CreditTrend,
                            "CRED-TREND",
                            entry,
                            entry,
                            0.0,
                            min_er,
                            1.0,
                            0.0015,
                            0.0200,
                            0.70,
                            delta,
                            stop,
                            target,
                            0.0,
                            0.90,
                        );
                    }
                }
            }
        }
    }

    out
}

fn same_f64(a: f64, b: f64) -> bool {
    (a - b).abs() < 1e-9
}

fn is_safe_credit_spec(sp: &IdeaSpec) -> bool {
    sp.kind == IdeaKind::CreditEdge
        && sp.entry_min == 9 * 60 + 45
        && same_f64(sp.buffer_pts, 0.0)
        && same_f64(sp.min_er, 0.0)
        && same_f64(sp.max_er, 0.55)
        && same_f64(sp.min_range_pct, 0.0)
        && same_f64(sp.max_range_pct, 0.0200)
        && same_f64(sp.edge_threshold, 0.65)
        && same_f64(sp.delta_target, 0.25)
        && same_f64(sp.stop_pct, 0.25)
        && same_f64(sp.target_pct, 0.50)
        && same_f64(sp.trail_pct, 0.0)
}

fn build_specs(args: &CliArgs) -> Result<Vec<IdeaSpec>, Box<dyn Error>> {
    let specs = idea_grid(args.enforce_credit_margin);

    if args.safe_credit {
        let out: Vec<IdeaSpec> = specs
            .into_iter()
            .filter(is_safe_credit_spec)
            .map(|mut sp| {
                sp.name = "CRED-EDGE SAFE e09:45 edge65 d0.25 s25 t50 er<=0.55".to_string();
                sp
            })
            .collect();
        if out.len() != 1 {
            return Err(
                format!("expected exactly one --safe-credit spec, found {}", out.len()).into(),
            );
        }
        return Ok(out);
    }

    if args.delayed_credit {
        let out: Vec<IdeaSpec> = specs
            .into_iter()
            .filter(|sp| {
                sp.kind == IdeaKind::CreditEdge
                    && sp.entry_min == 9 * 60 + 45
                    && same_f64(sp.buffer_pts, 0.0)
                    && same_f64(sp.min_er, 0.0)
                    && same_f64(sp.max_er, 1.0)
                    && same_f64(sp.min_range_pct, 0.0)
                    && same_f64(sp.max_range_pct, 0.0200)
                    && same_f64(sp.edge_threshold, 0.65)
                    && same_f64(sp.delta_target, 0.25)
                    && same_f64(sp.stop_pct, 0.25)
                    && same_f64(sp.target_pct, 0.50)
                    && same_f64(sp.trail_pct, 0.0)
            })
            .map(|mut sp| {
                sp.kind = IdeaKind::CreditEdgeDelayed;
                sp.name = "CRED-EDGE DELAY e09:45/impulse-pullback edge65 d0.25 s25 t50"
                    .to_string();
                sp
            })
            .collect();
        if out.len() != 1 {
            return Err(format!(
                "expected exactly one --delayed-credit spec, found {}",
                out.len()
            )
            .into());
        }
        return Ok(out);
    }

    // Backtest-only what-if: isolate the canonical CRED-EDGE spec and override only its stop,
    // e.g. SATA_STOP_PCT=0.10 tests a tight 10%-of-max-loss stop.
    if let Ok(v) = std::env::var("SATA_STOP_PCT") {
        let s: f64 = v.parse().expect("SATA_STOP_PCT must be a float like 0.10");
        return Ok(specs
            .into_iter()
            .filter(|sp| {
                sp.kind == IdeaKind::CreditEdge
                    && sp.entry_min == 9 * 60 + 45
                    && same_f64(sp.edge_threshold, 0.65)
                    && same_f64(sp.delta_target, 0.25)
                    && same_f64(sp.target_pct, 0.50)
                    && same_f64(sp.max_er, 1.0)
                    && same_f64(sp.stop_pct, 0.40)
            })
            .map(|mut sp| {
                sp.stop_pct = s;
                sp.name = format!("CRED-EDGE stop{:.0}%maxloss", s * 100.0);
                sp
            })
            .collect());
    }

    Ok(specs)
}

fn score_spec_refs(spec: IdeaSpec, days: &[&DayData], start_capital: f64) -> IdeaScore {
    let mut capital = start_capital;
    let mut results = Vec::new();
    for day in days {
        let r = run_spec_on_day(day, &spec, capital);
        if r.traded {
            capital += r.net_pnl;
        }
        results.push(r);
    }
    let traded: Vec<&TradeResult> = results.iter().filter(|r| r.traded).collect();
    let trades = traded.len();
    let wins = traded.iter().filter(|r| r.net_pnl > 0.0).count();
    let losses = traded.iter().filter(|r| r.net_pnl < 0.0).count();
    let total_net = traded.iter().map(|r| r.net_pnl).sum::<f64>();
    let avg_net = if trades > 0 {
        total_net / trades as f64
    } else {
        0.0
    };
    let worst = traded
        .iter()
        .map(|r| r.net_pnl)
        .reduce(f64::min)
        .unwrap_or(0.0);
    let best = traded
        .iter()
        .map(|r| r.net_pnl)
        .reduce(f64::max)
        .unwrap_or(0.0);

    IdeaScore {
        spec,
        trades,
        wins,
        losses,
        total_net,
        avg_net,
        worst,
        best,
        results,
    }
}

fn score_spec(spec: IdeaSpec, days: &[DayData], start_capital: f64) -> IdeaScore {
    let refs: Vec<&DayData> = days.iter().collect();
    score_spec_refs(spec, &refs, start_capital)
}

fn sort_scores(scores: &mut [IdeaScore]) {
    scores.sort_by(|a, b| {
        b.total_net
            .partial_cmp(&a.total_net)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| b.trades.cmp(&a.trades))
            .then_with(|| a.worst.partial_cmp(&b.worst).unwrap_or(std::cmp::Ordering::Equal))
    });
}

fn fmt_min(minute: u32) -> String {
    format!("{:02}:{:02}", minute / 60, minute % 60)
}

fn fmt_ts(ts: Option<NaiveDateTime>) -> String {
    ts.map(|t| format!("{:02}:{:02}:{:02}", t.hour(), t.minute(), t.second()))
        .unwrap_or_else(|| "--".to_string())
}

fn print_score(rank: usize, score: &IdeaScore) {
    let wr = if score.trades > 0 {
        score.wins as f64 / score.trades as f64 * 100.0
    } else {
        0.0
    };
    println!(
        "{rank:>2}. {name:<102} trades {trades:>2}  W/L {wins:>2}/{losses:<2}  WR {wr:>5.1}%  net Rs {net:>8.2}  avg Rs {avg:>7.2}  worst Rs {worst:>8.2}  best Rs {best:>8.2}",
        rank = rank,
        name = score.spec.name,
        trades = score.trades,
        wins = score.wins,
        losses = score.losses,
        wr = wr,
        net = score.total_net,
        avg = score.avg_net,
        worst = score.worst,
        best = score.best
    );
}

fn spread_pct(bid: f64, ask: f64) -> f64 {
    let mid = (bid + ask) * 0.5;
    if mid > 0.0 { (ask - bid) / mid * 100.0 } else { 0.0 }
}

fn print_day_details(score: &IdeaScore) {
    println!("\nBest candidate daily trace: {}", score.spec.name);
    for r in &score.results {
        if !r.traded {
            println!("  {}  no trade  {}", r.day, r.reason);
            continue;
        }
        let Some(f) = &r.fills else {
            // non-credit idea kinds: keep the one-line summary
            println!(
                "  {}  {:<16} entry {} exit {} lots {} net Rs {:+.2} gross Rs {:+.2} costs Rs {:.2} {}",
                r.day, r.direction, fmt_ts(r.entry_ts), fmt_ts(r.exit_ts),
                r.lots, r.net_pnl, r.gross_pnl, r.costs, r.reason
            );
            continue;
        };
        let qty = r.lots * LOT_SIZE;
        println!(
            "\n  {}  {}  x{} lot ({} qty)  {} -> {}  [{}]",
            r.day, r.direction, r.lots, qty,
            fmt_ts(r.entry_ts), fmt_ts(r.exit_ts), r.reason
        );
        println!(
            "     SHORT {:<9} (Δ{:+.2})  SELL@bid {:>7.2}  (bid {:.2}/ask {:.2}, spr {:.1}%)   close BUY@ask {:>7.2}",
            f.short_label, f.short_delta, f.short_sell, f.short_sell, f.short_ask_in,
            spread_pct(f.short_sell, f.short_ask_in), f.short_buy
        );
        println!(
            "     WING  {:<9}          BUY @ask {:>7.2}  (bid {:.2}/ask {:.2}, spr {:.1}%)   close SELL@bid {:>7.2}",
            f.wing_label, f.wing_buy, f.wing_bid_in, f.wing_buy,
            spread_pct(f.wing_bid_in, f.wing_buy), f.wing_sell
        );
        println!(
            "     credit in {:.2}  close cost {:.2}  gain {:+.2}/sh  |  width {:.0}  max-loss {:.2}/sh",
            f.entry_credit, f.close_cost, f.entry_credit - f.close_cost, f.width_pts, f.max_loss_unit
        );
        println!(
            "     gross Rs {:+.2}   costs Rs {:.2}   NET Rs {:+.2}",
            r.gross_pnl, r.costs, r.net_pnl
        );
    }
}

fn print_fixed_leave_one_out(best: &IdeaScore) {
    println!("\nCross-check A: fixed best, omit one day at a time");
    for omitted in &best.results {
        let kept: Vec<&TradeResult> = best
            .results
            .iter()
            .filter(|r| r.day != omitted.day && r.traded)
            .collect();
        let trades = kept.len();
        let wins = kept.iter().filter(|r| r.net_pnl > 0.0).count();
        let losses = kept.iter().filter(|r| r.net_pnl < 0.0).count();
        let total = kept.iter().map(|r| r.net_pnl).sum::<f64>();
        let worst = kept
            .iter()
            .map(|r| r.net_pnl)
            .reduce(f64::min)
            .unwrap_or(0.0);
        println!(
            "  omit {} -> trades {:>2} W/L {:>2}/{:<2} net Rs {:+.2} worst Rs {:+.2}",
            omitted.day, trades, wins, losses, total, worst
        );
    }
}

fn print_leave_one_out_selection(
    days: &[DayData],
    candidate_specs: &[IdeaSpec],
    capital: f64,
    min_trades: usize,
) {
    println!("\nCross-check B: leave-one-day-out selection");
    println!(
        "  Train on {} days, pick best candidate from {} spec(s), then test only the held-out day.",
        days.len().saturating_sub(1),
        candidate_specs.len()
    );

    let mut oos_trades = 0usize;
    let mut oos_wins = 0usize;
    let mut oos_losses = 0usize;
    let mut oos_net = 0.0;
    let mut selected: BTreeMap<String, usize> = BTreeMap::new();

    for hold_idx in 0..days.len() {
        let train: Vec<&DayData> = days
            .iter()
            .enumerate()
            .filter_map(|(idx, day)| (idx != hold_idx).then_some(day))
            .collect();
        let mut train_scores: Vec<IdeaScore> = candidate_specs
            .iter()
            .cloned()
            .map(|spec| score_spec_refs(spec, &train, capital))
            .filter(|s| s.trades >= min_trades)
            .collect();
        sort_scores(&mut train_scores);

        let Some(chosen) = train_scores.first() else {
            println!("  hold {} -> no train candidate met min_trades", days[hold_idx].day);
            continue;
        };
        *selected.entry(chosen.spec.name.clone()).or_default() += 1;
        let test = run_spec_on_day(&days[hold_idx], &chosen.spec, capital);
        if test.traded {
            oos_trades += 1;
            oos_net += test.net_pnl;
            if test.net_pnl > 0.0 {
                oos_wins += 1;
            } else if test.net_pnl < 0.0 {
                oos_losses += 1;
            }
            println!(
                "  hold {} -> train net Rs {:+.2} ({:>2} trades), test {} Rs {:+.2} [{}]",
                days[hold_idx].day,
                chosen.total_net,
                chosen.trades,
                test.direction,
                test.net_pnl,
                test.reason
            );
        } else {
            println!(
                "  hold {} -> train net Rs {:+.2} ({:>2} trades), test no trade ({})",
                days[hold_idx].day,
                chosen.total_net,
                chosen.trades,
                test.reason
            );
        }
    }

    let wr = if oos_trades > 0 {
        oos_wins as f64 / oos_trades as f64 * 100.0
    } else {
        0.0
    };
    println!(
        "  OOS traded days {:>2}/{} | W/L {:>2}/{:<2} | WR {:>5.1}% | net Rs {:+.2}",
        oos_trades,
        days.len(),
        oos_wins,
        oos_losses,
        wr,
        oos_net
    );
    println!("  Selected candidates:");
    for (name, count) in selected {
        println!("    {:>2}x {}", count, name);
    }
}

fn print_neighborhood(scores: &[IdeaScore], best: &IdeaScore) {
    println!("\nCross-check C: nearby parameter robustness");
    let mut neighbors: Vec<&IdeaScore> = scores
        .iter()
        .filter(|s| {
            s.spec.kind == best.spec.kind
                && s.trades >= best.trades.saturating_sub(1)
                && s.spec.entry_min.abs_diff(best.spec.entry_min) <= 15
                && (s.spec.edge_threshold - best.spec.edge_threshold).abs() <= 0.10
                && (s.spec.delta_target - best.spec.delta_target).abs() <= 0.06
                && (s.spec.target_pct - best.spec.target_pct).abs() <= 0.16
        })
        .collect();
    neighbors.sort_by(|a, b| {
        b.total_net
            .partial_cmp(&a.total_net)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let mut nets: Vec<f64> = neighbors.iter().map(|s| s.total_net).collect();
    nets.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let profitable = neighbors.iter().filter(|s| s.total_net > 0.0).count();
    let median = nets.get(nets.len().saturating_sub(1) / 2).copied().unwrap_or(0.0);
    let min = nets.first().copied().unwrap_or(0.0);
    let max = nets.last().copied().unwrap_or(0.0);
    println!(
        "  Neighborhood candidates: {} | profitable: {}/{} | min Rs {:+.2} | median Rs {:+.2} | max Rs {:+.2}",
        neighbors.len(),
        profitable,
        neighbors.len(),
        min,
        median,
        max
    );
    for (idx, score) in neighbors.iter().take(8).enumerate() {
        print_score(idx + 1, score);
    }
}

fn print_cross_verification(
    days: &[DayData],
    scores: &[IdeaScore],
    candidate_specs: &[IdeaSpec],
    capital: f64,
    min_trades: usize,
) {
    let Some(best) = scores.first() else {
        return;
    };
    println!("\n==============================================================");
    println!("CROSS VERIFICATION - RESEARCH ONLY");
    println!("==============================================================");
    print_fixed_leave_one_out(best);
    print_leave_one_out_selection(days, candidate_specs, capital, min_trades);
    print_neighborhood(scores, best);
    println!("==============================================================");
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = parse_args()?;
    let mut days = Vec::new();
    for path in &args.paths {
        eprintln!("loading {}", path.display());
        days.push(load_day(path)?);
    }
    days.sort_by_key(|d| d.day);

    let specs = build_specs(&args)?;
    let candidate_count = specs.len();
    let mut scores: Vec<IdeaScore> = specs
        .iter()
        .cloned()
        .map(|spec| score_spec(spec, &days, args.capital))
        .filter(|s| s.trades >= args.min_trades)
        .collect();
    scores.sort_by(|a, b| {
        b.total_net
            .partial_cmp(&a.total_net)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| b.trades.cmp(&a.trades))
            .then_with(|| a.worst.partial_cmp(&b.worst).unwrap_or(std::cmp::Ordering::Equal))
    });

    println!("==============================================================");
    println!("SATAVAHANA OPTION IDEA LAB - BACKTEST ONLY");
    println!("==============================================================");
    println!("Days loaded       : {}", days.len());
    println!("Starting capital  : Rs {:.2}", args.capital);
    println!("Lot size          : {}", LOT_SIZE);
    println!("Max lots          : {}", MAX_LOTS);
    println!("Spread cap        : {:.0}%", MAX_SPREAD_PCT * 100.0);
    println!(
        "Credit margin     : Zerodha verified approx Rs {:.0} + {:.0}% buffer = Rs {:.0}/lot",
        VERIFIED_CREDIT_SPREAD_MARGIN_INR,
        CREDIT_SPREAD_MARGIN_BUFFER * 100.0,
        verified_credit_margin_with_buffer()
    );
    println!(
        "Credit margin gate: {}",
        if args.enforce_credit_margin {
            "ON"
        } else {
            "OFF (research/theoretical max-loss sizing)"
        }
    );
    println!("Candidate specs   : {}", candidate_count);
    if args.safe_credit {
        println!("Profile filter    : safe credit edge (er<=0.55, stop25)");
    }
    if args.delayed_credit {
        println!(
            "Profile filter    : delayed credit edge (high ER/drift waits for pullback failure)"
        );
    }
    println!("Min trades shown  : {}\n", args.min_trades);
    for day in &days {
        let dte = day.expiry.signed_duration_since(day.day).num_days();
        println!(
            "  {}  {}DTE  {}  series {}  spots {}",
            day.day,
            dte,
            day.path.file_name().and_then(|n| n.to_str()).unwrap_or("-"),
            day.series.len(),
            day.spots.len()
        );
    }
    println!("\nTop candidates:");
    for (idx, score) in scores.iter().take(args.top).enumerate() {
        print_score(idx + 1, score);
    }
    if let Some(best) = scores.first() {
        print_day_details(best);
    } else {
        println!("No candidate met --min-trades {}", args.min_trades);
    }
    if args.cross_verify {
        print_cross_verification(&days, &scores, &specs, args.capital, args.min_trades);
    }
    println!("==============================================================");
    Ok(())
}
