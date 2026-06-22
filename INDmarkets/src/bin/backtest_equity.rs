
use rand::rngs::StdRng;
use rand::SeedableRng;
use rand_distr::{Distribution, Normal};
use satavahana::backtest::{BacktestConfig, BacktestEngine};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;


const CAPITAL: f64 = 5_000.0;
const MIS_LEVERAGE: f64 = 5.0;
const BUYING_POWER_PER_LEG: f64 = CAPITAL * MIS_LEVERAGE / 2.0;

const MIN_EXPECTED_NET_SIGNAL: f64 = 30.0;
const MIN_SIGNAL_SPREAD_RUPEES: f64 = 3.0;

const SPREADS_PER_MIN: usize = 100;
const MAX_SPREAD_PCT: f64 = 0.005;
const MIN_PRICE: f64 = 10.0;
const MAX_PRICE: f64 = 3_000.0;
const HIGH_VOL_THRESHOLD: u64 = 100_000;
const MAX_BUY_ORDERS_PER_DAY: usize = 1_500;
const MAX_SELL_ORDERS_PER_DAY: usize = 1_500;

const SIGNAL_MAX_QUOTE_AGE_MS: u64 = 2_000;
const EXEC_MAX_STALE_MS: u64 = 20_000;
const MAX_INTERP_GAP_MS: u64 = 120_000;
const PER_SYMBOL_COOLDOWN_MS: u64 = 500;

const MIN_HOLD_MS: u64 = 100;
const MAX_HOLD_MS: u64 = 30_000;
const EXIT_POLL_MS: u64 = 100;
const TARGET_REVERSION_PCT: f64 = 0.50;
const STOP_WIDEN_PCT: f64 = 0.35;
const RNG_SEED: u64 = 42;

const ETF_PATTERNS: &[&str] = &[
    "ETF", "BEES", "IETF", "MF", "FUND", "LIQUID", "GOLD", "SILVER", "NIFTY", "SENSEX",
    "MIDCAP", "INFRA", "MOM", "BSE", "NEXT", "LOWVOL", "QUALITY", "VALUE", "ALPHA", "BETA",
    "CPSE", "BHARAT", "INDIA",
];

fn is_etf(s: &str) -> bool {
    let u = s.to_uppercase();
    ETF_PATTERNS.iter().any(|p| u.contains(p))
}


type Timeline = Vec<(u64, f64)>;

#[derive(Debug)]
struct Row {
    ts_us: u64,
    token: u32,
    base: String,
    exchange: String,
    ltp: f64,
    volume: u64,
}

#[derive(Clone, Copy)]
struct Quote {
    ts_us: u64,
    token: u32,
    ltp: f64,
    volume: u64,
}

#[derive(Default)]
struct PairState {
    nse: Option<Quote>,
    bse: Option<Quote>,
    last_signal_ts_us: Option<u64>,
}

#[derive(Clone)]
struct SpreadSignalEvent {
    ts_us: u64,
    nse_token: u32,
    bse_token: u32,
    nse_ltp: f64,
    bse_ltp: f64,
    spread: f64,
    is_high_volume: bool,
}

#[derive(Default, Clone)]
struct SegmentStats {
    signals_seen: usize,
    dropped_edge_filter: usize,
    dropped_rate_limit: usize,
    dropped_insufficient_capital: usize,
    dropped_order_budget: usize,
    attempted_trades: usize,
    successful_trades: usize,
    failed_entry_fills: usize,
    failed_exit_fills: usize,
    profitable_trades: usize,
    losing_trades: usize,
    breakeven_trades: usize,
    total_gross: f64,
    total_costs: f64,
    total_net: f64,
    total_entry_lag_ms: f64,
    total_exit_lag_ms: f64,
    initial_capital: f64,
    final_capital: f64,
    stopped_out: bool,
    buy_orders_used: usize,
    sell_orders_used: usize,
    exit_target_hits: usize,
    exit_stop_hits: usize,
    exit_timeout_hits: usize,
}

struct BacktestStats {
    all: SegmentStats,
    high_volume: SegmentStats,
}


fn parse_ts(s: &str) -> Option<u64> {
    let s = s.trim();
    let dt = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f")
        .ok()
        .or_else(|| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S").ok())
        .or_else(|| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f").ok())?;
    Some(dt.and_utc().timestamp_micros() as u64)
}

fn load_csv(path: &PathBuf) -> Vec<Row> {
    eprintln!(
        "Loading {} ({:.0} MB)...",
        path.display(),
        fs::metadata(path).map(|m| m.len() as f64 / 1e6).unwrap_or(0.0)
    );

    let mut rdr = csv::Reader::from_path(path).expect("cannot open CSV");
    let headers = rdr.headers().expect("no headers").clone();

    let col = |name: &str| {
        headers
            .iter()
            .position(|h| h == name)
            .unwrap_or_else(|| panic!("Column '{}' missing", name))
    };

    let (i_ts, i_tok, i_sym, i_ex, i_ltp, i_vol) = (
        col("recv_ts"),
        col("token"),
        col("symbol"),
        col("exchange"),
        col("ltp"),
        col("volume"),
    );

    let mut rows = Vec::with_capacity(8_000_000);
    for result in rdr.records() {
        let r = match result {
            Ok(r) => r,
            Err(_) => continue,
        };

        let ltp: f64 = r.get(i_ltp).and_then(|v| v.parse().ok()).unwrap_or(0.0);
        if ltp <= 0.0 {
            continue;
        }

        let token: u32 = match r.get(i_tok).and_then(|v| v.parse().ok()) {
            Some(t) => t,
            None => continue,
        };

        let ts_us = match r.get(i_ts).and_then(parse_ts) {
            Some(t) => t,
            None => continue,
        };

        let sym = r.get(i_sym).unwrap_or("").to_string();
        let exch = r.get(i_ex).unwrap_or("").to_string();
        let vol: u64 = r.get(i_vol).and_then(|v| v.parse().ok()).unwrap_or(0);
        let base = sym
            .strip_prefix("NSE:")
            .or_else(|| sym.strip_prefix("BSE:"))
            .unwrap_or(&sym)
            .to_string();

        rows.push(Row {
            ts_us,
            token,
            base,
            exchange: exch,
            ltp,
            volume: vol,
        });
    }

    rows.sort_unstable_by_key(|r| r.ts_us);
    eprintln!("  {} ticks loaded", rows.len());
    rows
}

fn build_timelines(rows: &[Row]) -> HashMap<u32, Timeline> {
    let mut timelines: HashMap<u32, Timeline> = HashMap::new();
    for row in rows {
        timelines.entry(row.token).or_default().push((row.ts_us, row.ltp));
    }
    timelines
}

fn build_signals(rows: &[Row]) -> Vec<SpreadSignalEvent> {
    let mut states: HashMap<String, PairState> = HashMap::new();
    let mut signals = Vec::new();

    let max_quote_age_us = SIGNAL_MAX_QUOTE_AGE_MS * 1_000;
    let cooldown_us = PER_SYMBOL_COOLDOWN_MS * 1_000;

    for row in rows {
        if row.exchange != "NSE" && row.exchange != "BSE" {
            continue;
        }
        if is_etf(&row.base) {
            continue;
        }

        let st = states.entry(row.base.clone()).or_default();
        let q = Quote {
            ts_us: row.ts_us,
            token: row.token,
            ltp: row.ltp,
            volume: row.volume,
        };

        if row.exchange == "NSE" {
            st.nse = Some(q);
        } else {
            st.bse = Some(q);
        }

        let (nse, bse) = match (st.nse, st.bse) {
            (Some(n), Some(b)) => (n, b),
            _ => continue,
        };

        if nse.ts_us.abs_diff(bse.ts_us) > max_quote_age_us {
            continue;
        }

        let signal_ts = nse.ts_us.max(bse.ts_us);
        if let Some(prev_ts) = st.last_signal_ts_us {
            if signal_ts.saturating_sub(prev_ts) < cooldown_us {
                continue;
            }
        }

        let mid = (nse.ltp + bse.ltp) / 2.0;
        if !(MIN_PRICE..=MAX_PRICE).contains(&mid) {
            continue;
        }

        let spread = nse.ltp - bse.ltp;
        let spread_abs = spread.abs();
        if spread_abs < MIN_SIGNAL_SPREAD_RUPEES {
            continue;
        }
        if spread_abs / mid > MAX_SPREAD_PCT {
            continue;
        }
        if nse.volume == 0 || bse.volume == 0 {
            continue;
        }

        st.last_signal_ts_us = Some(signal_ts);
        signals.push(SpreadSignalEvent {
            ts_us: signal_ts,
            nse_token: nse.token,
            bse_token: bse.token,
            nse_ltp: nse.ltp,
            bse_ltp: bse.ltp,
            spread,
            is_high_volume: nse.volume >= HIGH_VOL_THRESHOLD,
        });
    }

    signals
}

fn price_at_guarded(
    timeline: &Timeline,
    ts_us: u64,
    max_stale_us: u64,
    max_interp_gap_us: u64,
) -> Option<f64> {
    if timeline.is_empty() {
        return None;
    }

    let idx = timeline.partition_point(|&(t, _)| t <= ts_us);
    let prev = if idx > 0 { Some(timeline[idx - 1]) } else { None };
    let next = if idx < timeline.len() {
        Some(timeline[idx])
    } else {
        None
    };

    match (prev, next) {
        (Some((t0, p0)), Some((t1, p1))) => {
            let left_age = ts_us.saturating_sub(t0);
            let right_age = t1.saturating_sub(ts_us);
            let gap = t1.saturating_sub(t0);

            if left_age > max_stale_us || right_age > max_stale_us || gap > max_interp_gap_us {
                return None;
            }

            if t1 == t0 {
                return Some(p0);
            }

            let w = (ts_us - t0) as f64 / (t1 - t0) as f64;
            Some(p0 + w * (p1 - p0))
        }
        (Some((t0, p0)), None) => {
            if ts_us.saturating_sub(t0) <= max_stale_us {
                Some(p0)
            } else {
                None
            }
        }
        (None, Some((t1, p1))) => {
            if t1.saturating_sub(ts_us) <= max_stale_us {
                Some(p1)
            } else {
                None
            }
        }
        (None, None) => None,
    }
}

#[derive(Clone, Copy)]
enum ExitReason {
    Target,
    Stop,
    Timeout,
}

fn select_exit_decision_ts(
    entry_exec_ts: u64,
    entry_spread: f64,
    nse_timeline: &Timeline,
    bse_timeline: &Timeline,
    exec_max_stale_us: u64,
    max_interp_gap_us: u64,
) -> (u64, ExitReason) {
    let min_hold_us = MIN_HOLD_MS * 1_000;
    let max_hold_us = MAX_HOLD_MS * 1_000;
    let step_us = EXIT_POLL_MS * 1_000;

    let entry_abs = entry_spread.abs();
    let target_abs = (entry_abs * (1.0 - TARGET_REVERSION_PCT)).max(0.01);
    let stop_abs = entry_abs * (1.0 + STOP_WIDEN_PCT);

    let mut t = entry_exec_ts + min_hold_us;
    let hard_stop = entry_exec_ts + max_hold_us;

    while t <= hard_stop {
        let nse = price_at_guarded(nse_timeline, t, exec_max_stale_us, max_interp_gap_us);
        let bse = price_at_guarded(bse_timeline, t, exec_max_stale_us, max_interp_gap_us);

        if let (Some(n), Some(b)) = (nse, bse) {
            let s = n - b;
            let moved_towards_zero = if entry_spread > 0.0 {
                s < entry_spread
            } else {
                s > entry_spread
            };
            let moved_away = if entry_spread > 0.0 {
                s > entry_spread
            } else {
                s < entry_spread
            };
            let crossed_zero = s.signum() != entry_spread.signum();

            if crossed_zero || (moved_towards_zero && s.abs() <= target_abs) {
                return (t, ExitReason::Target);
            }
            if moved_away && s.abs() >= stop_abs {
                return (t, ExitReason::Stop);
            }
        }
        t += step_us;
    }

    (hard_stop, ExitReason::Timeout)
}

fn simulate(
    signals: &[SpreadSignalEvent],
    timelines: &HashMap<u32, Timeline>,
    cfg: &BacktestConfig,
    engine: &BacktestEngine,
) -> BacktestStats {
    let stats_all = simulate_segment(signals, timelines, cfg, engine, |_| true, RNG_SEED);
    let stats_hv = simulate_segment(
        signals,
        timelines,
        cfg,
        engine,
        |s| s.is_high_volume,
        RNG_SEED.wrapping_add(1),
    );

    BacktestStats {
        all: stats_all,
        high_volume: stats_hv,
    }
}

fn simulate_segment<F>(
    signals: &[SpreadSignalEvent],
    timelines: &HashMap<u32, Timeline>,
    cfg: &BacktestConfig,
    engine: &BacktestEngine,
    mut include: F,
    seed: u64,
) -> SegmentStats
where
    F: FnMut(&SpreadSignalEvent) -> bool,
{
    let mut st = SegmentStats {
        initial_capital: CAPITAL,
        final_capital: CAPITAL,
        ..SegmentStats::default()
    };

    let mut per_minute_count: HashMap<u64, usize> = HashMap::new();
    let mut rng = StdRng::seed_from_u64(seed);
    let lag_dist = Normal::new(cfg.mean_api_lag_ms, cfg.api_lag_std_ms).unwrap();

    let tick = cfg.tick_size;
    let exec_max_stale_us = EXEC_MAX_STALE_MS * 1_000;
    let max_interp_gap_us = MAX_INTERP_GAP_MS * 1_000;
    let mut capital = CAPITAL;

    for s in signals {
        if !include(s) {
            continue;
        }
        st.signals_seen += 1;

        if capital <= 0.0 {
            st.stopped_out = true;
            break;
        }

        let mid = (s.nse_ltp + s.bse_ltp) / 2.0;
        let buying_power_per_leg = capital * MIS_LEVERAGE / 2.0;
        if buying_power_per_leg < mid {
            st.dropped_insufficient_capital += 1;
            continue;
        }

        let qty = ((buying_power_per_leg / mid) as u32).max(1);

        let (signal_buy, signal_sell) = if s.spread > 0.0 {
            (s.bse_ltp + tick, s.nse_ltp - tick)
        } else {
            (s.nse_ltp + tick, s.bse_ltp - tick)
        };
        let expected_roundtrip_cost = 2.0 * engine.calculate_total_costs_pub(signal_buy, signal_sell, qty);
        let expected_gross_flat = ((s.spread.abs() - 4.0 * tick).max(0.0)) * qty as f64;
        let expected_net_signal = expected_gross_flat - expected_roundtrip_cost;

        if expected_net_signal < MIN_EXPECTED_NET_SIGNAL {
            st.dropped_edge_filter += 1;
            continue;
        }

        let minute_bucket = (s.ts_us / 60_000_000) * 60_000_000;
        let used = per_minute_count.entry(minute_bucket).or_insert(0);
        if *used >= SPREADS_PER_MIN {
            st.dropped_rate_limit += 1;
            continue;
        }

        const BUY_ORDERS_PER_SPREAD: usize = 2;
        const SELL_ORDERS_PER_SPREAD: usize = 2;
        if st.buy_orders_used + BUY_ORDERS_PER_SPREAD > MAX_BUY_ORDERS_PER_DAY
            || st.sell_orders_used + SELL_ORDERS_PER_SPREAD > MAX_SELL_ORDERS_PER_DAY
        {
            st.dropped_order_budget += 1;
            continue;
        }
        *used += 1;
        st.buy_orders_used += BUY_ORDERS_PER_SPREAD;
        st.sell_orders_used += SELL_ORDERS_PER_SPREAD;

        st.attempted_trades += 1;

        let entry_lag_ms = lag_dist.sample(&mut rng).max(0.0).min(1_000.0);
        let entry_exec_ts = s.ts_us + (entry_lag_ms * 1_000.0) as u64;

        let nse_entry = timelines
            .get(&s.nse_token)
            .and_then(|t| price_at_guarded(t, entry_exec_ts, exec_max_stale_us, max_interp_gap_us));
        let bse_entry = timelines
            .get(&s.bse_token)
            .and_then(|t| price_at_guarded(t, entry_exec_ts, exec_max_stale_us, max_interp_gap_us));

        let (nse_entry, bse_entry) = match (nse_entry, bse_entry) {
            (Some(n), Some(b)) => (n, b),
            _ => {
                st.failed_entry_fills += 1;
                continue;
            }
        };

        let (long_entry_buy, short_entry_sell) = if s.spread > 0.0 {
            (bse_entry + tick, nse_entry - tick)
        } else {
            (nse_entry + tick, bse_entry - tick)
        };

        let nse_timeline = match timelines.get(&s.nse_token) {
            Some(t) => t,
            None => {
                st.failed_exit_fills += 1;
                continue;
            }
        };
        let bse_timeline = match timelines.get(&s.bse_token) {
            Some(t) => t,
            None => {
                st.failed_exit_fills += 1;
                continue;
            }
        };

        let entry_spread = nse_entry - bse_entry;
        let (exit_decision_ts, exit_reason) = select_exit_decision_ts(
            entry_exec_ts,
            entry_spread,
            nse_timeline,
            bse_timeline,
            exec_max_stale_us,
            max_interp_gap_us,
        );

        let exit_lag_ms = lag_dist.sample(&mut rng).max(0.0).min(1_000.0);
        let exit_exec_ts = exit_decision_ts + (exit_lag_ms * 1_000.0) as u64;

        let nse_exit = price_at_guarded(nse_timeline, exit_exec_ts, exec_max_stale_us, max_interp_gap_us);
        let bse_exit = price_at_guarded(bse_timeline, exit_exec_ts, exec_max_stale_us, max_interp_gap_us);

        let (nse_exit, bse_exit) = match (nse_exit, bse_exit) {
            (Some(n), Some(b)) => (n, b),
            _ => {
                st.failed_exit_fills += 1;
                continue;
            }
        };

        let (long_exit_sell, short_exit_buy) = if s.spread > 0.0 {
            (bse_exit - tick, nse_exit + tick)
        } else {
            (nse_exit - tick, bse_exit + tick)
        };

        let gross = ((long_exit_sell - long_entry_buy) + (short_entry_sell - short_exit_buy)) * qty as f64;
        let entry_cost = engine.calculate_total_costs_pub(long_entry_buy, short_entry_sell, qty);
        let exit_cost = engine.calculate_total_costs_pub(short_exit_buy, long_exit_sell, qty);
        let total_costs = entry_cost + exit_cost;
        let net = gross - total_costs;

        st.successful_trades += 1;
        st.total_gross += gross;
        st.total_costs += total_costs;
        st.total_net += net;
        st.total_entry_lag_ms += entry_lag_ms;
        st.total_exit_lag_ms += exit_lag_ms;
        capital += net;
        st.final_capital = capital;

        match exit_reason {
            ExitReason::Target => st.exit_target_hits += 1,
            ExitReason::Stop => st.exit_stop_hits += 1,
            ExitReason::Timeout => st.exit_timeout_hits += 1,
        }

        if net > 0.0 {
            st.profitable_trades += 1;
        } else if net < 0.0 {
            st.losing_trades += 1;
        } else {
            st.breakeven_trades += 1;
        }
    }

    st.final_capital = capital;
    st
}

fn print_report(title: &str, st: &SegmentStats) {
    let sep = "=".repeat(72);
    println!("\n{sep}");
    println!("  SATAVAHANA — EQUITY SPREAD BACKTEST  [{title}]");
    println!("{sep}");
    println!("  Capital: Rs{CAPITAL:.0}  MIS {MIS_LEVERAGE}x = Rs{BUYING_POWER_PER_LEG:.0} per leg");
    println!(
        "  Signal guardrails: fresh quotes <= {}ms, symbol cooldown {}ms, max spread {:.2}%",
        SIGNAL_MAX_QUOTE_AGE_MS,
        PER_SYMBOL_COOLDOWN_MS,
        MAX_SPREAD_PCT * 100.0
    );
    println!(
        "  Execution model: entry lag N(150,50), hold {}-{}ms, exit lag N(150,50), 1-tick adverse fill each leg",
        MIN_HOLD_MS,
        MAX_HOLD_MS
    );
    println!(
        "  Entry gates: spread >= Rs{MIN_SIGNAL_SPREAD_RUPEES:.2}, expected-net >= Rs{MIN_EXPECTED_NET_SIGNAL:.2}"
    );
    println!(
        "  Exit logic: target reversion {:.0}% | stop widen {:.0}% | poll {}ms",
        TARGET_REVERSION_PCT * 100.0,
        STOP_WIDEN_PCT * 100.0,
        EXIT_POLL_MS
    );
    println!(
        "  Daily order budget: BUY {} + SELL {} (reserved 2+2 per spread)",
        MAX_BUY_ORDERS_PER_DAY,
        MAX_SELL_ORDERS_PER_DAY
    );
    println!();

    println!("  Signals seen            : {:>10}", st.signals_seen);
    println!("  Dropped (edge filter)   : {:>10}", st.dropped_edge_filter);
    println!("  Dropped (rate limit)    : {:>10}", st.dropped_rate_limit);
    println!(
        "  Dropped (capital limit) : {:>10}",
        st.dropped_insufficient_capital
    );
    println!("  Dropped (order budget)  : {:>10}", st.dropped_order_budget);
    println!("  Trades attempted        : {:>10}", st.attempted_trades);
    println!("  Successful trades       : {:>10}", st.successful_trades);
    println!("  Failed fills (entry)    : {:>10}", st.failed_entry_fills);
    println!("  Failed fills (exit)     : {:>10}", st.failed_exit_fills);

    let fill_failures = st.failed_entry_fills + st.failed_exit_fills;
    let fill_success_rate = if st.attempted_trades > 0 {
        (st.successful_trades as f64 / st.attempted_trades as f64) * 100.0
    } else {
        0.0
    };
    println!(
        "  Fill success rate       : {:>9.2}%  ({} / {})",
        fill_success_rate, st.successful_trades, st.attempted_trades
    );
    println!(
        "  Fill failures (total)   : {:>10}",
        fill_failures
    );
    println!();

    println!("  Profitable trades       : {:>10}", st.profitable_trades);
    println!("  Losing trades           : {:>10}", st.losing_trades);
    println!("  Breakeven trades        : {:>10}", st.breakeven_trades);

    let win_rate = if st.successful_trades > 0 {
        (st.profitable_trades as f64 / st.successful_trades as f64) * 100.0
    } else {
        0.0
    };
    println!(
        "  Win rate (filled only)  : {:>9.2}%  ({} / {})",
        win_rate, st.profitable_trades, st.successful_trades
    );
    println!();

    println!("  Initial capital         : Rs{:.2}", st.initial_capital);
    println!("  Final capital           : Rs{:.2}", st.final_capital);
    if st.stopped_out {
        println!("  Capital stop-out        : YES (simulation halted when capital <= 0)");
    } else {
        println!("  Capital stop-out        : NO");
    }
    println!(
        "  Orders used             : BUY {} / {} | SELL {} / {}",
        st.buy_orders_used,
        MAX_BUY_ORDERS_PER_DAY,
        st.sell_orders_used,
        MAX_SELL_ORDERS_PER_DAY
    );
    println!();

    println!("  Gross P&L               : Rs{:+.2}", st.total_gross);
    println!("  Total charges           : Rs{:.2}", st.total_costs);
    println!("  Net P&L                 : Rs{:+.2}", st.total_net);
    println!(
        "  Return on Rs{:.0}         : {:+.2}%",
        st.initial_capital,
        ((st.final_capital - st.initial_capital) / st.initial_capital) * 100.0
    );

    if st.successful_trades > 0 {
        let n = st.successful_trades as f64;
        println!("  Avg net / successful    : Rs{:+.4}", st.total_net / n);
        println!("  Avg gross / successful  : Rs{:+.4}", st.total_gross / n);
        println!("  Avg charges / successful: Rs{:.4}", st.total_costs / n);
        println!("  Avg entry lag           : {:.1} ms", st.total_entry_lag_ms / n);
        println!("  Avg exit lag            : {:.1} ms", st.total_exit_lag_ms / n);
        println!(
            "  Exit reasons            : target {} | stop {} | timeout {}",
            st.exit_target_hits, st.exit_stop_hits, st.exit_timeout_hits
        );
    }
    println!("{sep}");
}

fn main() {
    let mut csvs: Vec<PathBuf> = fs::read_dir("data")
        .expect("data/ not found")
        .filter_map(|e| e.ok().map(|x| x.path()))
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.ends_with("_equity_ticks.csv"))
                .unwrap_or(false)
        })
        .collect();
    csvs.sort();
    let csv = csvs.last().expect("No *_equity_ticks.csv in logs/");

    let rows = load_csv(csv);
    let timelines = build_timelines(&rows);

    eprintln!("Building event-driven spread signals...");
    let signals = build_signals(&rows);
    eprintln!(
        "  {} signal events (ETF-filtered, fresh quotes, no hindsight ranking)",
        signals.len()
    );

    let cfg = BacktestConfig::default();
    let engine = BacktestEngine::new(cfg.clone());
    let stats = simulate(&signals, &timelines, &cfg, &engine);

    print_report("ALL STOCKS", &stats.all);
    print_report(
        &format!("HIGH-VOLUME ONLY (NSE volume >= {})", HIGH_VOL_THRESHOLD),
        &stats.high_volume,
    );
}
