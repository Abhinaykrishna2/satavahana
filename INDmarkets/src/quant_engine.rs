/// Quant Engine — four data-driven strategies orthogonal to options_engine.rs
///
/// Strategies:
///   1. OFI     — Order Flow Imbalance across ATM±5 strikes
///   2. OIV     — OI Velocity: 3-scan OI momentum
///   3. CD      — Controlled Decline: spot drifts without IV spike
///   4. SOA     — Session OI Accumulation: smart-money position loading
///
/// This module is completely standalone. No logic is shared with options_engine.
/// Merge only if backtests consistently show edge.

use crate::models::{OptionContract, OptionType};
use crate::store::TickStore;

use chrono::{FixedOffset, NaiveDate, TimeZone, Timelike, Utc};
use std::collections::{HashMap, VecDeque};
use std::fs;
use std::io::Write as IoWrite;
use std::path::PathBuf;
use tracing::warn;

// ─── Cost model (mirrors options_engine constants) ───────────────────────────

const NSE_OPT_EXCHANGE_RATE: f64 = 0.000311; // 0.0311%

/// STT on options sell-side is date-dependent:
/// 0.10% until Mar 31 2026, 0.15% from Apr 1 2026 (Budget 2025-26 change).
fn stt_sell_rate(exit_ts_ms: u64) -> f64 {
    let ist = FixedOffset::east_opt(5 * 3600 + 30 * 60).expect("valid IST offset");
    let dt_ist = Utc.timestamp_millis_opt(exit_ts_ms as i64)
        .single()
        .unwrap_or_else(Utc::now)
        .with_timezone(&ist);
    let hike_date = NaiveDate::from_ymd_opt(2026, 4, 1).expect("valid date");
    if dt_ist.date_naive() >= hike_date { 0.0015 } else { 0.0010 }
}

/// Returns the current date as YYYY-MM-DD in IST.
fn ist_date_str() -> String {
    let ist = FixedOffset::east_opt(5 * 3600 + 30 * 60).expect("valid IST offset");
    Utc::now().with_timezone(&ist).format("%Y-%m-%d").to_string()
}

fn tx_cost_entry(price: f64, lot_size: u32) -> f64 {
    let prem = price * lot_size as f64;
    let brok = 20.0;
    let exc = NSE_OPT_EXCHANGE_RATE * prem;
    let sebi = 0.000001 * prem;
    let stamp = 0.00003 * prem;
    let gst = 0.18 * (brok + exc + sebi);
    brok + exc + sebi + stamp + gst
}

fn tx_cost_exit(price: f64, lot_size: u32, exit_ts_ms: u64) -> f64 {
    let prem = price * lot_size as f64;
    let brok = 20.0;
    let stt = stt_sell_rate(exit_ts_ms) * prem;
    let exc = NSE_OPT_EXCHANGE_RATE * prem;
    let sebi = 0.000001 * prem;
    let gst = 0.18 * (brok + exc + sebi);
    brok + stt + exc + sebi + gst
}

/// Compute how many lots to buy given current capital and signal confidence.
/// Uses half-Kelly: allocate KELLY_CAPITAL_FRACTION * (confidence/100) of capital to premium.
/// Returns at least 1 lot; capped at MAX_KELLY_LOTS.
fn kelly_lots(capital: f64, confidence: f64, entry_price: f64, lot_size: u32) -> u32 {
    let cost_per_lot = entry_price * lot_size as f64;
    if cost_per_lot <= 0.0 {
        return 1;
    }
    let allocation = capital * KELLY_CAPITAL_FRACTION * (confidence / 100.0).clamp(0.0, 1.0);
    let n = (allocation / cost_per_lot).floor() as u32;
    n.max(1).min(MAX_KELLY_LOTS)
}

// ─── Config ──────────────────────────────────────────────────────────────────

const SCAN_INTERVAL_SECS: u64 = 300;         // 5-minute scans
const CONFIDENCE_THRESHOLD: f64 = 62.0;
const STOP_RATIO: f64 = 0.30;                // 30% stop on entry premium
const TARGET_RATIO: f64 = 0.50;             // 50% target
const BREAKEVEN_TRIGGER_RATIO: f64 = 0.65;  // trail to BE at 65% of target distance
const MAX_HOLD_SECS: u64 = 75 * 60;
const FORCE_CLOSE_HOUR: u32 = 14;
const FORCE_CLOSE_MINUTE: u32 = 50;
const MAX_DAILY_TRADES: u32 = 6;
const DAILY_CIRCUIT_RATIO: f64 = 0.30;
// Dynamic Kelly lot sizing: see kelly_lots() below.
const MAX_KELLY_LOTS: u32 = 5;           // hard cap per trade
const KELLY_CAPITAL_FRACTION: f64 = 0.15; // allocate up to 15% of capital per trade (half-Kelly base)
const FILL_OFFSET: f64 = 0.5; // buy +₹0.5 slippage model

// Strategy score caps
const OFI_MAX: f64 = 35.0;
const OIV_MAX: f64 = 35.0;
const CD_MAX: f64 = 35.0;
const SOA_MAX: f64 = 25.0;
const CROSS_BONUS: f64 = 8.0;

// ─── Internal data types ─────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct PerStrike {
    strike: f64,
    ce_token: Option<u32>,
    ce_ltp: f64,
    ce_oi: u32,
    ce_buy_qty: u32,
    ce_sell_qty: u32,
    ce_volume: u32,
    pe_token: Option<u32>,
    pe_ltp: f64,
    pe_oi: u32,
    pe_buy_qty: u32,
    pe_sell_qty: u32,
    pe_volume: u32,
}

impl PerStrike {
    /// Corrected put-call parity: S = K·e^{-rT} + C - P
    /// The naive K + C - P formula underestimates spot by K·(1 - e^{-rT}),
    /// which is ~17 pts on NIFTY at 5 DTE (r=6.5%) and ~130 pts at 30 DTE.
    fn implied_spot(&self, t_years: f64, risk_free_rate: f64) -> Option<f64> {
        if self.ce_ltp > 0.5 && self.pe_ltp > 0.5 {
            let discount = (-risk_free_rate * t_years).exp();
            Some(self.strike * discount + self.ce_ltp - self.pe_ltp)
        } else {
            None
        }
    }

    fn ce_ofi(&self) -> f64 {
        let total = (self.ce_buy_qty + self.ce_sell_qty) as f64;
        if total < 1.0 {
            return 0.0;
        }
        (self.ce_buy_qty as f64 - self.ce_sell_qty as f64) / total
    }

    fn pe_ofi(&self) -> f64 {
        let total = (self.pe_buy_qty + self.pe_sell_qty) as f64;
        if total < 1.0 {
            return 0.0;
        }
        (self.pe_buy_qty as f64 - self.pe_sell_qty as f64) / total
    }
}

#[derive(Debug, Clone)]
struct ScanSnapshot {
    ts_ms: u64,
    spot: f64,   // estimated via put-call parity
    atm_strike: f64,
    strikes: Vec<PerStrike>, // sorted by |strike - spot|, ATM±5
}

impl ScanSnapshot {
    fn avg_ce_ofi(&self) -> f64 {
        let vals: Vec<f64> = self.strikes.iter().map(|s| s.ce_ofi()).collect();
        if vals.is_empty() { return 0.0; }
        vals.iter().sum::<f64>() / vals.len() as f64
    }

    fn avg_pe_ofi(&self) -> f64 {
        let vals: Vec<f64> = self.strikes.iter().map(|s| s.pe_ofi()).collect();
        if vals.is_empty() { return 0.0; }
        vals.iter().sum::<f64>() / vals.len() as f64
    }

}

#[derive(Debug, Clone, Copy, PartialEq)]
enum SignalDir {
    CE,
    PE,
}

#[derive(Debug, Clone)]
struct QuantSignal {
    ts_ms: u64,
    underlying: String,
    direction: SignalDir,
    confidence: f64,
    token: u32,
    tradingsymbol: String,
    strike: f64,
    entry_price: f64,
    lot_size: u32,
    strategy_tags: String,
}

#[derive(Debug, Clone)]
struct QuantPosition {
    token: u32,
    tradingsymbol: String,
    underlying: String,
    direction: SignalDir,
    strike: f64,
    lot_size: u32,    // contract lot size from instruments API
    n_lots: u32,      // Kelly-sized number of lots bought this trade
    entry_price: f64,
    entry_ts_ms: u64,
    stop_price: f64,
    target_price: f64,
    peak_price: f64,
    breakeven_active: bool,
    breakeven_stop_price: f64,
    confidence: f64,
    strategy_tags: String,
}

#[derive(Debug, Clone)]
struct ClosedTrade {
    underlying: String,
    tradingsymbol: String,
    direction: String,
    strike: f64,
    lot_size: u32,
    entry_price: f64,
    exit_price: f64,
    entry_ts_ms: u64,
    exit_ts_ms: u64,
    gross_pnl: f64,
    tx_costs: f64,
    net_pnl: f64,
    exit_reason: String,
    confidence: f64,
    strategy_tags: String,
}

// ─── Per-underlying state ─────────────────────────────────────────────────────

struct UnderlyingState {
    /// last 3 scan snapshots — oldest first
    history: VecDeque<ScanSnapshot>,
    /// session-start OI per (strike*1000 as u64, OptionType) captured after warmup
    session_oi: HashMap<(u64, OptionType), u32>,
    session_oi_set: bool,
}

impl UnderlyingState {
    fn new() -> Self {
        Self {
            history: VecDeque::with_capacity(4),
            session_oi: HashMap::new(),
            session_oi_set: false,
        }
    }

    fn push_snapshot(&mut self, snap: ScanSnapshot) {
        if self.history.len() >= 3 {
            self.history.pop_front();
        }
        // Capture session baseline on first snapshot after warmup
        if !self.session_oi_set {
            for s in &snap.strikes {
                let k = (s.strike as u64 * 1000, OptionType::PE);
                self.session_oi.insert(k, s.pe_oi);
                let k = (s.strike as u64 * 1000, OptionType::CE);
                self.session_oi.insert(k, s.ce_oi);
            }
            self.session_oi_set = true;
        }
        self.history.push_back(snap);
    }

    fn latest(&self) -> Option<&ScanSnapshot> {
        self.history.back()
    }

    fn prev(&self) -> Option<&ScanSnapshot> {
        if self.history.len() >= 2 {
            self.history.get(self.history.len() - 2)
        } else {
            None
        }
    }

    fn oldest(&self) -> Option<&ScanSnapshot> {
        if self.history.len() >= 3 {
            self.history.front()
        } else {
            None
        }
    }
}

// ─── Strategy scoring ─────────────────────────────────────────────────────────

fn score_ofi(state: &UnderlyingState) -> (f64, f64) {
    // OFI uses RELATIVE dominance: in options, buy_qty > sell_qty on both sides
    // is normal (market maker asymmetry). What matters is whether PE buyers are
    // more aggressive than CE buyers, or vice versa.
    let latest = match state.latest() { Some(s) => s, None => return (0.0, 0.0) };
    let avg_ce = latest.avg_ce_ofi();
    let avg_pe = latest.avg_pe_ofi();

    // Relative imbalance: positive → PE buyers more dominant
    let relative = avg_pe - avg_ce;

    let mut ce_score = 0.0_f64;
    let mut pe_score = 0.0_f64;

    // CE signal: CE buyers clearly more dominant than PE buyers
    if relative < -0.15 {
        ce_score += 25.0;
        if relative < -0.25 { ce_score += 5.0; }
        if relative < -0.35 { ce_score += 5.0; }
        if let Some(prev) = state.prev() {
            let prev_rel = prev.avg_pe_ofi() - prev.avg_ce_ofi();
            if prev_rel < -0.10 { ce_score += 5.0; } // consistent
        }
        let avg_vol: f64 = latest.strikes.iter()
            .map(|s| s.ce_volume as f64)
            .sum::<f64>() / latest.strikes.len().max(1) as f64;
        if avg_vol > 5000.0 { ce_score += 3.0; }
    }

    // PE signal: PE buyers clearly more dominant than CE buyers
    if relative > 0.15 {
        pe_score += 25.0;
        if relative > 0.25 { pe_score += 5.0; }
        if relative > 0.35 { pe_score += 5.0; }
        if let Some(prev) = state.prev() {
            let prev_rel = prev.avg_pe_ofi() - prev.avg_ce_ofi();
            if prev_rel > 0.10 { pe_score += 5.0; } // consistent
        }
        let avg_vol: f64 = latest.strikes.iter()
            .map(|s| s.pe_volume as f64)
            .sum::<f64>() / latest.strikes.len().max(1) as f64;
        if avg_vol > 5000.0 { pe_score += 3.0; }
    }

    (ce_score.min(OFI_MAX), pe_score.min(OFI_MAX))
}

fn score_oiv(state: &UnderlyingState) -> (f64, f64) {
    // OI Velocity: 3-scan OI momentum
    // Need at least 3 snapshots
    let (old, _mid, new) = match (state.oldest(), state.prev(), state.latest()) {
        (Some(o), Some(m), Some(n)) => (o, m, n),
        _ => return (0.0, 0.0),
    };

    // Compute per-strike OI velocity (change over 2 scan intervals)
    let mut pe_velocity: i64 = 0;
    let mut ce_velocity: i64 = 0;

    for s_new in &new.strikes {
        // find matching strike in old snapshot
        if let Some(s_old) = old.strikes.iter().find(|s| (s.strike - s_new.strike).abs() < 1.0) {
            pe_velocity += s_new.pe_oi as i64 - s_old.pe_oi as i64;
            ce_velocity += s_new.ce_oi as i64 - s_old.ce_oi as i64;
        }
    }

    let mut ce_score = 0.0_f64;
    let mut pe_score = 0.0_f64;

    let diff = pe_velocity - ce_velocity;

    // Convention: "writer dominance" — the dominant participant in NSE options is the
    // institutional seller. Rising PE OI therefore signals put writers accumulating
    // (neutral-to-bullish); rising CE OI signals call writers (neutral-to-bearish).
    // This is consistent with the price/OI quadrant when price moves opposite to the
    // building side (PE builds while price rises = writers, not buyers). When price
    // moves with the building side the quadrant interpretation differs, but without
    // per-trade aggressor data the writer assumption is the safer default here.
    if pe_velocity > 20_000 && diff > 15_000 {
        ce_score += 20.0;
        if diff > 50_000 { ce_score += 8.0; }
        if diff > 100_000 { ce_score += 7.0; }
        // Monotonic check: mid-scan PE OI also above oldest
        if let Some(mid) = state.prev() {
            let mid_pe: u32 = mid.strikes.iter().map(|s| s.pe_oi).sum();
            let old_pe: u32 = old.strikes.iter().map(|s| s.pe_oi).sum();
            if mid_pe > old_pe { ce_score += 7.0; }
        }
    }

    // Rising CE OI = call writers accumulating → bearish (maps to BuyPE / downside signal).
    if ce_velocity > 20_000 && -diff > 15_000 {
        pe_score += 20.0;
        if -diff > 50_000 { pe_score += 8.0; }
        if -diff > 100_000 { pe_score += 7.0; }
        if let Some(mid) = state.prev() {
            let mid_ce: u32 = mid.strikes.iter().map(|s| s.ce_oi).sum();
            let old_ce: u32 = old.strikes.iter().map(|s| s.ce_oi).sum();
            if mid_ce > old_ce { pe_score += 7.0; }
        }
    }

    (ce_score.min(OIV_MAX), pe_score.min(OIV_MAX))
}

fn score_cd(state: &UnderlyingState) -> (f64, f64) {
    // Controlled Decline / Controlled Rally
    // Checks that spot has actually moved in the signal direction.
    // "Controlled" = option premium rose proportionally (no IV panic spike):
    //   ATM PE rise should be < 5x the absolute spot drop in points.
    //   E.g., spot falls 40pts → ATM PE can rise up to 200pts before we call it "panic".
    let latest = match state.latest() { Some(s) => s, None => return (0.0, 0.0) };
    let reference = match state.oldest().or_else(|| state.prev()) {
        Some(s) => s,
        None => return (0.0, 0.0),
    };

    if reference.spot <= 0.0 || latest.spot <= 0.0 {
        return (0.0, 0.0);
    }

    let spot_change_pct = (latest.spot - reference.spot) / reference.spot;
    let spot_change_pts = (latest.spot - reference.spot).abs();

    let mut ce_score = 0.0_f64;
    let mut pe_score = 0.0_f64;

    // Bearish: spot fell 0.12%+
    if spot_change_pct < -0.0012 {
        let atm_pe_now = latest.strikes.iter()
            .find(|s| (s.strike - latest.atm_strike).abs() < 1.0)
            .map(|s| s.pe_ltp)
            .unwrap_or(0.0);
        let atm_pe_ref = reference.strikes.iter()
            .find(|s| (s.strike - reference.atm_strike).abs() < 1.0)
            .map(|s| s.pe_ltp)
            .unwrap_or(0.0);

        if atm_pe_now > 0.5 && atm_pe_ref > 0.5 {
            let pe_rise_pts = atm_pe_now - atm_pe_ref;
            // Controlled: PE rose (confirms direction) but didn't panic-spike
            // panic = PE rose more than 5x spot_fall in points
            if pe_rise_pts > 0.0 && pe_rise_pts < 5.0 * spot_change_pts {
                pe_score += 20.0;
                if spot_change_pct < -0.0025 { pe_score += 8.0; } // bigger move
                // Monotonic: both intermediate scans show decline
                if let Some(prev) = state.prev() {
                    if prev.spot < reference.spot && latest.spot < prev.spot {
                        pe_score += 7.0;
                    }
                }
            }
        }
    }

    // Bullish: spot rose 0.12%+
    if spot_change_pct > 0.0012 {
        let atm_ce_now = latest.strikes.iter()
            .find(|s| (s.strike - latest.atm_strike).abs() < 1.0)
            .map(|s| s.ce_ltp)
            .unwrap_or(0.0);
        let atm_ce_ref = reference.strikes.iter()
            .find(|s| (s.strike - reference.atm_strike).abs() < 1.0)
            .map(|s| s.ce_ltp)
            .unwrap_or(0.0);

        if atm_ce_now > 0.5 && atm_ce_ref > 0.5 {
            let ce_rise_pts = atm_ce_now - atm_ce_ref;
            if ce_rise_pts > 0.0 && ce_rise_pts < 5.0 * spot_change_pts {
                ce_score += 20.0;
                if spot_change_pct > 0.0025 { ce_score += 8.0; }
                if let Some(prev) = state.prev() {
                    if prev.spot > reference.spot && latest.spot > prev.spot {
                        ce_score += 7.0;
                    }
                }
            }
        }
    }

    (ce_score.min(CD_MAX), pe_score.min(CD_MAX))
}

fn score_soa(state: &UnderlyingState) -> (f64, f64) {
    // Session OI Accumulation
    if !state.session_oi_set { return (0.0, 0.0); }
    let latest = match state.latest() { Some(s) => s, None => return (0.0, 0.0) };

    let mut pe_accum: i64 = 0;
    let mut ce_accum: i64 = 0;

    for s in &latest.strikes {
        let pe_key = (s.strike as u64 * 1000, OptionType::PE);
        let ce_key = (s.strike as u64 * 1000, OptionType::CE);
        let base_pe = *state.session_oi.get(&pe_key).unwrap_or(&0);
        let base_ce = *state.session_oi.get(&ce_key).unwrap_or(&0);
        pe_accum += s.pe_oi as i64 - base_pe as i64;
        ce_accum += s.ce_oi as i64 - base_ce as i64;
    }

    let net = pe_accum - ce_accum; // positive = more PE built than CE

    let mut ce_score = 0.0_f64;
    let mut pe_score = 0.0_f64;

    // More PE OI accumulated = put writers loaded → bullish → ce_score
    if net > 150_000 {
        ce_score += 15.0;
        if net > 300_000 { ce_score += 10.0; }
    // More CE OI accumulated = call writers loaded → bearish → pe_score
    } else if net < -150_000 {
        pe_score += 15.0;
        if net < -300_000 { pe_score += 10.0; }
    }

    (ce_score.min(SOA_MAX), pe_score.min(SOA_MAX))
}

// ─── Strategy scoring aggregation ────────────────────────────────────────────

struct StrategyScores {
    ofi_ce: f64,
    ofi_pe: f64,
    oiv_ce: f64,
    oiv_pe: f64,
    cd_ce: f64,
    cd_pe: f64,
    soa_ce: f64,
    soa_pe: f64,
}

impl StrategyScores {
    fn total_ce(&self) -> f64 {
        let raw = self.ofi_ce + self.oiv_ce + self.cd_ce + self.soa_ce;
        let strategies_contributing = [self.ofi_ce, self.oiv_ce, self.cd_ce, self.soa_ce]
            .iter()
            .filter(|&&v| v > 0.0)
            .count();
        let bonus = if strategies_contributing >= 2 { CROSS_BONUS } else { 0.0 };
        raw + bonus
    }

    fn total_pe(&self) -> f64 {
        let raw = self.ofi_pe + self.oiv_pe + self.cd_pe + self.soa_pe;
        let strategies_contributing = [self.ofi_pe, self.oiv_pe, self.cd_pe, self.soa_pe]
            .iter()
            .filter(|&&v| v > 0.0)
            .count();
        let bonus = if strategies_contributing >= 2 { CROSS_BONUS } else { 0.0 };
        raw + bonus
    }

    fn tag_ce(&self) -> String {
        let mut parts = Vec::new();
        if self.ofi_ce > 0.0 { parts.push(format!("OFI({:.0})", self.ofi_ce)); }
        if self.oiv_ce > 0.0 { parts.push(format!("OIV({:.0})", self.oiv_ce)); }
        if self.cd_ce > 0.0  { parts.push(format!("CD({:.0})", self.cd_ce)); }
        if self.soa_ce > 0.0 { parts.push(format!("SOA({:.0})", self.soa_ce)); }
        if parts.is_empty() { "none".to_string() } else { parts.join("+") }
    }

    fn tag_pe(&self) -> String {
        let mut parts = Vec::new();
        if self.ofi_pe > 0.0 { parts.push(format!("OFI({:.0})", self.ofi_pe)); }
        if self.oiv_pe > 0.0 { parts.push(format!("OIV({:.0})", self.oiv_pe)); }
        if self.cd_pe > 0.0  { parts.push(format!("CD({:.0})", self.cd_pe)); }
        if self.soa_pe > 0.0 { parts.push(format!("SOA({:.0})", self.soa_pe)); }
        if parts.is_empty() { "none".to_string() } else { parts.join("+") }
    }
}

// ─── Main engine ─────────────────────────────────────────────────────────────

pub struct QuantEngine {
    contracts: Vec<OptionContract>,
    /// token → index into contracts
    token_to_idx: HashMap<u32, usize>,
    /// underlying → sorted list of unique strikes, and per-strike CE/PE tokens
    underlying_strikes: HashMap<String, Vec<f64>>,
    /// (underlying, strike, OptionType) → token
    strike_token: HashMap<(String, u64, OptionType), u32>,

    store: TickStore,

    capital: f64,
    day_start_capital: f64,
    initial_capital: f64,
    daily_realized_pnl: f64, // tracks closed-trade P&L for circuit breaker

    state: HashMap<String, UnderlyingState>,
    open_positions: Vec<QuantPosition>,
    closed_trades: Vec<ClosedTrade>,

    scan_count: u64,
    last_scan_ms: u64,
    warmup_until_ms: u64,
    trades_today: u32,
    current_day_idx: u64,

    log_dir: PathBuf,
    trades_csv_path: PathBuf,
    trades_csv_file: Option<fs::File>,
}

impl QuantEngine {
    pub fn new(
        contracts: Vec<OptionContract>,
        store: TickStore,
        initial_capital: f64,
        log_dir: &str,
    ) -> Self {
        let mut token_to_idx = HashMap::new();
        let mut underlying_strikes: HashMap<String, Vec<f64>> = HashMap::new();
        let mut strike_token: HashMap<(String, u64, OptionType), u32> = HashMap::new();

        let mut min_expiries: HashMap<String, String> = HashMap::new();
        for c in &contracts {
            let entry = min_expiries.entry(c.underlying.clone()).or_insert_with(|| c.expiry.clone());
            if c.expiry < *entry {
                *entry = c.expiry.clone();
            }
        }

        for (i, c) in contracts.iter().enumerate() {
            if let Some(min_exp) = min_expiries.get(&c.underlying) {
                if c.expiry != *min_exp { continue; }
            }
            token_to_idx.insert(c.instrument_token, i);
            underlying_strikes
                .entry(c.underlying.clone())
                .or_default()
                .push(c.strike);
            let key = (c.underlying.clone(), (c.strike * 1000.0) as u64, c.option_type);
            strike_token.insert(key, c.instrument_token);
        }

        for strikes in underlying_strikes.values_mut() {
            strikes.sort_by(|a, b| a.partial_cmp(b).unwrap());
            strikes.dedup();
        }

        let state: HashMap<String, UnderlyingState> = underlying_strikes
            .keys()
            .map(|u| (u.clone(), UnderlyingState::new()))
            .collect();

        let log_path = PathBuf::from(log_dir);
        let _ = fs::create_dir_all(&log_path);
        let date_str = ist_date_str();
        let trades_csv_path = log_path.join(format!("{}_quant_trades.csv", date_str));

        Self {
            contracts,
            token_to_idx,
            underlying_strikes,
            strike_token,
            store,
            capital: initial_capital,
            day_start_capital: initial_capital,
            initial_capital,
            daily_realized_pnl: 0.0,
            state,
            open_positions: Vec::new(),
            closed_trades: Vec::new(),
            scan_count: 0,
            last_scan_ms: 0,
            warmup_until_ms: 0,
            trades_today: 0,
            current_day_idx: 0,
            log_dir: log_path,
            trades_csv_path,
            trades_csv_file: None,
        }
    }

    pub fn set_warmup_until_ms(&mut self, ts: u64) {
        if self.warmup_until_ms == 0 {
            self.warmup_until_ms = ts;
        }
    }

    // Called for every tick from the backtest runner
    pub fn on_tick(&mut self, ts_ms: u64) {
        let ist_ts = ts_ms + 19_800_000;
        let day_idx = ist_ts / 86_400_000;
        if self.current_day_idx != day_idx {
            if self.current_day_idx != 0 {
                self.trades_today = 0;
                self.daily_realized_pnl = 0.0;
                self.day_start_capital = self.capital;
                tracing::info!("Day Rollover in QuantEngine: Resetting daily counters");
            }
            self.current_day_idx = day_idx;
        }

        // Scan every SCAN_INTERVAL_SECS
        if self.last_scan_ms == 0 || ts_ms >= self.last_scan_ms + SCAN_INTERVAL_SECS * 1_000 {
            self.last_scan_ms = ts_ms;
            self.run_scan(ts_ms);
        }
        // Check open positions for exit on every tick
        self.check_positions(ts_ms);
    }

    pub fn finalize(&mut self, reason: &str) {
        let ts_ms = self.last_scan_ms;
        let tokens: Vec<u32> = self.open_positions.iter().map(|p| p.token).collect();
        for token in tokens {
            let ltp = self.store.get(token).map(|t| t.ltp).unwrap_or(0.0);
            if ltp > 0.0 {
                let exit_price = (ltp - FILL_OFFSET).max(0.05);
                self.close_position(token, exit_price, ts_ms, reason);
            }
        }
        self.write_session_summary();
    }

    pub fn diagnostics(&self) -> (usize, usize, usize) {
        let wins = self.closed_trades.iter().filter(|t| t.net_pnl > 0.0).count();
        let losses = self.closed_trades.iter().filter(|t| t.net_pnl < 0.0).count();
        (self.closed_trades.len(), wins, losses)
    }

    pub fn net_pnl(&self) -> f64 {
        self.closed_trades.iter().map(|t| t.net_pnl).sum()
    }

    // ─── Scan logic ──────────────────────────────────────────────────────────

    fn run_scan(&mut self, ts_ms: u64) {
        self.scan_count += 1;

        let underlyings: Vec<String> = self.underlying_strikes.keys().cloned().collect();

        for underlying in &underlyings {
            let snap = self.build_snapshot(underlying, ts_ms);
            let snap = match snap { Some(s) => s, None => continue };

            if snap.spot <= 0.0 || snap.strikes.is_empty() {
                continue;
            }

            let in_warmup = ts_ms < self.warmup_until_ms;

            let state = self.state.get_mut(underlying).unwrap();
            state.push_snapshot(snap.clone());

            if in_warmup {
                warn!(
                    "Quant scan #{} {} [WARMUP] spot={:.0} strikes={}",
                    self.scan_count, underlying, snap.spot, snap.strikes.len()
                );
                continue;
            }

            let (ofi_ce, ofi_pe) = score_ofi(state);
            let (oiv_ce, oiv_pe) = score_oiv(state);
            let (cd_ce,  cd_pe)  = score_cd(state);
            let (soa_ce, soa_pe) = score_soa(state);
            let scores = StrategyScores { ofi_ce, ofi_pe, oiv_ce, oiv_pe, cd_ce, cd_pe, soa_ce, soa_pe };

            let ce_total = scores.total_ce();
            let pe_total = scores.total_pe();

            warn!(
                "Quant scan #{} {} spot={:.0} CE_score={:.0} PE_score={:.0} threshold={:.0} OFI={:.2}/{:.2} OIV_hist={}",
                self.scan_count, underlying, snap.spot,
                ce_total, pe_total, CONFIDENCE_THRESHOLD,
                snap.avg_ce_ofi(), snap.avg_pe_ofi(),
                state.history.len()
            );

            // PE signal takes priority if both fire (bearish market edge)
            let raw_direction = if pe_total >= CONFIDENCE_THRESHOLD && pe_total >= ce_total {
                Some((SignalDir::PE, pe_total, scores.tag_pe()))
            } else if ce_total >= CONFIDENCE_THRESHOLD {
                Some((SignalDir::CE, ce_total, scores.tag_ce()))
            } else {
                None
            };

            // Spot alignment gate: use the 15-minute trend (oldest→latest = 3 scans).
            // This avoids vetoing trades on 5-min bounces inside a larger trend.
            // Block only when the full 15-min window is firmly counter-directional (>0.2%).
            let direction = raw_direction.and_then(|(dir, conf, tags)| {
                // Use oldest scan if available (15-min window), else prev (10-min)
                let reference_spot = state.oldest()
                    .or_else(|| state.prev())
                    .map(|s| s.spot)
                    .unwrap_or(0.0);
                let latest_spot = state.latest().map(|s| s.spot).unwrap_or(0.0);
                let spot_change = if reference_spot > 0.0 {
                    (latest_spot - reference_spot) / reference_spot
                } else {
                    0.0
                };

                // Veto if spot rose 0.20%+ over 15 min when taking PE
                // Veto if spot fell 0.20%+ over 15 min when taking CE
                let blocked = match dir {
                    SignalDir::PE => spot_change > 0.0020,
                    SignalDir::CE => spot_change < -0.0020,
                };
                if blocked {
                    warn!(
                        "Quant: {} signal for {} vetoed by 15-min spot alignment (spot_Δ={:+.2}%)",
                        match dir { SignalDir::PE => "PE", SignalDir::CE => "CE" },
                        underlying, spot_change * 100.0
                    );
                    None
                } else {
                    Some((dir, conf, tags))
                }
            });


            if let Some((dir, confidence, tags)) = direction {
                // Daily circuit breaker — only on REALIZED losses, not open positions
                let daily_realized_loss = -self.daily_realized_pnl.min(0.0);
                if daily_realized_loss > self.day_start_capital * DAILY_CIRCUIT_RATIO {
                    warn!("Quant: daily circuit breaker active (realized loss ₹{:.0}), skipping signal",
                          daily_realized_loss);
                    continue;
                }
                // Max daily trades
                if self.trades_today >= MAX_DAILY_TRADES {
                    warn!("Quant: max daily trades ({}) reached", MAX_DAILY_TRADES);
                    continue;
                }
                // Skip if already have open position for this underlying
                if self.open_positions.iter().any(|p| p.underlying == *underlying) {
                    warn!("Quant: already in position for {}", underlying);
                    continue;
                }

                if let Some(signal) = self.build_signal(
                    &snap, underlying, dir, confidence, &tags, ts_ms,
                ) {
                    self.execute_signal(signal, ts_ms);
                }
            }
        }
    }

    fn build_snapshot(&self, underlying: &str, ts_ms: u64) -> Option<ScanSnapshot> {
        let strikes = self.underlying_strikes.get(underlying)?;

        // Collect all strike data from store
        let mut all_strikes: Vec<PerStrike> = strikes.iter().filter_map(|&strike| {
            let ce_key = (underlying.to_string(), (strike * 1000.0) as u64, OptionType::CE);
            let pe_key = (underlying.to_string(), (strike * 1000.0) as u64, OptionType::PE);

            let ce_tok = self.strike_token.get(&ce_key).copied();
            let pe_tok = self.strike_token.get(&pe_key).copied();

            let ce_tick = ce_tok.and_then(|t| self.store.get(t));
            let pe_tick = pe_tok.and_then(|t| self.store.get(t));

            let ce_ltp = ce_tick.as_ref().map(|t| t.ltp).unwrap_or(0.0);
            let pe_ltp = pe_tick.as_ref().map(|t| t.ltp).unwrap_or(0.0);

            // Skip if neither side has data
            if ce_ltp < 0.05 && pe_ltp < 0.05 { return None; }

            Some(PerStrike {
                strike,
                ce_token: ce_tok,
                ce_ltp,
                ce_oi:       ce_tick.as_ref().map(|t| t.oi).unwrap_or(0),
                ce_buy_qty:  ce_tick.as_ref().map(|t| t.buy_qty).unwrap_or(0),
                ce_sell_qty: ce_tick.as_ref().map(|t| t.sell_qty).unwrap_or(0),
                ce_volume:   ce_tick.as_ref().map(|t| t.volume).unwrap_or(0),
                pe_token: pe_tok,
                pe_ltp,
                pe_oi:       pe_tick.as_ref().map(|t| t.oi).unwrap_or(0),
                pe_buy_qty:  pe_tick.as_ref().map(|t| t.buy_qty).unwrap_or(0),
                pe_sell_qty: pe_tick.as_ref().map(|t| t.sell_qty).unwrap_or(0),
                pe_volume:   pe_tick.as_ref().map(|t| t.volume).unwrap_or(0),
            })
        }).collect();

        if all_strikes.is_empty() { return None; }

        // Compute time-to-expiry for the corrected parity formula.
        const RISK_FREE_RATE: f64 = 0.065; // 6.5% RBI repo rate (matches options_engine default)
        let t_years = {
            let ist = FixedOffset::east_opt(5 * 3600 + 30 * 60).expect("valid IST offset");
            let ts_naive = Utc.timestamp_millis_opt(ts_ms as i64)
                .single()
                .unwrap_or_else(Utc::now)
                .with_timezone(&ist)
                .date_naive();
            let expiry_str = self.contracts.iter()
                .filter(|c| c.underlying == underlying && !c.expiry.is_empty())
                .map(|c| c.expiry.as_str())
                .min()
                .unwrap_or("");
            if let Ok(exp_date) = NaiveDate::parse_from_str(expiry_str, "%Y-%m-%d") {
                let days = (exp_date - ts_naive).num_days().max(0) as f64;
                (days + 0.5) / 365.0
            } else {
                0.0
            }
        };

        // Estimate spot via corrected put-call parity: S = K·e^{-rT} + C - P
        let mut implied_spots: Vec<f64> = all_strikes.iter()
            .filter_map(|s| s.implied_spot(t_years, RISK_FREE_RATE))
            .collect();
        if implied_spots.is_empty() { return None; }

        implied_spots.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let spot = implied_spots[implied_spots.len() / 2]; // median

        // Find ATM strike
        let atm_strike = all_strikes.iter()
            .map(|s| s.strike)
            .min_by(|a, b| {
                (a - spot).abs().partial_cmp(&(b - spot).abs()).unwrap()
            })
            .unwrap_or(spot);

        // Keep only ATM±5 strikes (by proximity)
        all_strikes.sort_by(|a, b| {
            (a.strike - spot).abs().partial_cmp(&(b.strike - spot).abs()).unwrap()
        });
        all_strikes.truncate(11); // top 11 closest = ATM±5 strikes

        Some(ScanSnapshot {
            ts_ms,
            spot,
            atm_strike,
            strikes: all_strikes,
        })
    }

    fn build_signal(
        &self,
        snap: &ScanSnapshot,
        underlying: &str,
        dir: SignalDir,
        confidence: f64,
        tags: &str,
        ts_ms: u64,
    ) -> Option<QuantSignal> {
        let _opt_type = match dir { SignalDir::CE => OptionType::CE, SignalDir::PE => OptionType::PE };

        // Find ATM token of the signaled type
        let atm_strike = snap.atm_strike;
        let token = snap.strikes.iter()
            .filter(|s| {
                let tok = match dir {
                    SignalDir::CE => s.ce_token,
                    SignalDir::PE => s.pe_token,
                };
                let ltp = match dir {
                    SignalDir::CE => s.ce_ltp,
                    SignalDir::PE => s.pe_ltp,
                };
                tok.is_some() && ltp > 0.5
            })
            .min_by(|a, b| {
                (a.strike - atm_strike).abs()
                    .partial_cmp(&(b.strike - atm_strike).abs())
                    .unwrap()
            })
            .and_then(|s| match dir {
                SignalDir::CE => s.ce_token,
                SignalDir::PE => s.pe_token,
            })?;

        let tick = self.store.get(token)?;
        if tick.ltp < 0.5 { return None; }

        let entry_price = tick.ltp + FILL_OFFSET;
        let lot_size = self.token_to_idx.get(&token)
            .map(|&i| self.contracts[i].lot_size)?; // None → skip signal; lot_size must come from API
        let tradingsymbol = self.token_to_idx.get(&token)
            .map(|&i| self.contracts[i].tradingsymbol.clone())
            .unwrap_or_default();
        let strike = self.token_to_idx.get(&token)
            .map(|&i| self.contracts[i].strike)
            .unwrap_or(atm_strike);

        Some(QuantSignal {
            ts_ms,
            underlying: underlying.to_string(),
            direction: dir,
            confidence,
            token,
            tradingsymbol,
            strike,
            entry_price,
            lot_size,
            strategy_tags: tags.to_string(),
        })
    }

    fn execute_signal(&mut self, signal: QuantSignal, ts_ms: u64) {
        let n_lots = kelly_lots(self.capital, signal.confidence, signal.entry_price, signal.lot_size);
        let contracts = signal.lot_size * n_lots;
        let cost = tx_cost_entry(signal.entry_price, contracts);
        let premium_outlay = signal.entry_price * contracts as f64;
        if premium_outlay + cost > self.capital {
            warn!(
                "Quant: insufficient capital ₹{:.0} for {} at ₹{:.1} ({} lots)",
                self.capital, signal.tradingsymbol, signal.entry_price, n_lots
            );
            return;
        }

        self.capital -= premium_outlay + cost;
        self.trades_today += 1;

        let dir_str = match signal.direction { SignalDir::CE => "CE", SignalDir::PE => "PE" };
        warn!(
            "Quant ENTRY  #{} {} {} strike={} entry=₹{:.2} lots={} conf={:.0} tags=[{}]",
            self.trades_today, signal.underlying, dir_str,
            signal.strike, signal.entry_price, n_lots, signal.confidence, signal.strategy_tags
        );

        let stop = signal.entry_price * (1.0 - STOP_RATIO);
        let target = signal.entry_price * (1.0 + TARGET_RATIO);

        self.open_positions.push(QuantPosition {
            token: signal.token,
            tradingsymbol: signal.tradingsymbol,
            underlying: signal.underlying,
            direction: signal.direction,
            strike: signal.strike,
            lot_size: signal.lot_size,
            n_lots,
            entry_price: signal.entry_price,
            entry_ts_ms: ts_ms,
            stop_price: stop,
            target_price: target,
            peak_price: signal.entry_price,
            breakeven_active: false,
            breakeven_stop_price: signal.entry_price * 0.97, // 3% buffer below entry
            confidence: signal.confidence,
            strategy_tags: signal.strategy_tags,
        });
    }

    // ─── Position management ─────────────────────────────────────────────────

    fn check_positions(&mut self, ts_ms: u64) {
        let ist = FixedOffset::east_opt(5 * 3600 + 30 * 60).unwrap();
        let dt = Utc.timestamp_millis_opt(ts_ms as i64).single().unwrap_or_else(Utc::now);
        let dt_ist = dt.with_timezone(&ist);
        let force_close = dt_ist.hour() > FORCE_CLOSE_HOUR
            || (dt_ist.hour() == FORCE_CLOSE_HOUR && dt_ist.minute() >= FORCE_CLOSE_MINUTE);

        let tokens: Vec<u32> = self.open_positions.iter().map(|p| p.token).collect();
        for token in tokens {
            let ltp = match self.store.get(token) { Some(t) => t.ltp, None => continue };
            if ltp <= 0.0 { continue; }

            let exit_price = (ltp - FILL_OFFSET).max(0.05);
            let reason = self.exit_reason(token, exit_price, ts_ms, force_close);
            if let Some(r) = reason {
                self.close_position(token, exit_price, ts_ms, &r);
            } else {
                // Update peak for trailing stop
                if let Some(pos) = self.open_positions.iter_mut().find(|p| p.token == token) {
                    if ltp > pos.peak_price {
                        pos.peak_price = ltp;
                    }
                    // Trail to breakeven at 65% of target distance
                    if !pos.breakeven_active {
                        let target_dist = pos.target_price - pos.entry_price;
                        if ltp >= pos.entry_price + BREAKEVEN_TRIGGER_RATIO * target_dist {
                            pos.breakeven_active = true;
                            pos.breakeven_stop_price = pos.entry_price * 0.97; // 3% buffer below entry
                            warn!(
                                "Quant BE-STOP set {} entry=₹{:.2} current=₹{:.2}",
                                pos.tradingsymbol, pos.entry_price, ltp
                            );
                        }
                    }
                }
            }
        }
    }

    fn exit_reason(&self, token: u32, ltp: f64, ts_ms: u64, force_close: bool) -> Option<String> {
        let pos = self.open_positions.iter().find(|p| p.token == token)?;

        if force_close {
            return Some("ForceClose-EOD".to_string());
        }
        if ts_ms > pos.entry_ts_ms + MAX_HOLD_SECS * 1_000 {
            return Some("TimeStop".to_string());
        }
        if pos.breakeven_active && ltp < pos.breakeven_stop_price {
            return Some("BEStop".to_string());
        }
        if ltp <= pos.stop_price {
            return Some("StopLoss".to_string());
        }
        if ltp >= pos.target_price {
            return Some("TakeProfit".to_string());
        }
        None
    }

    fn close_position(&mut self, token: u32, exit_price: f64, ts_ms: u64, reason: &str) {
        let idx = match self.open_positions.iter().position(|p| p.token == token) {
            Some(i) => i,
            None => return,
        };
        let pos = self.open_positions.remove(idx);

        let lots_total = pos.lot_size * pos.n_lots;
        let entry_cost = tx_cost_entry(pos.entry_price, lots_total);
        let exit_cost = tx_cost_exit(exit_price, lots_total, ts_ms);
        let tx_costs = entry_cost + exit_cost;

        let gross = (exit_price - pos.entry_price) * lots_total as f64;
        let net = gross - tx_costs;

        self.capital += exit_price * lots_total as f64 - exit_cost;
        self.daily_realized_pnl += net;

        let dir_str = match pos.direction { SignalDir::CE => "CE", SignalDir::PE => "PE" };
        warn!(
            "Quant EXIT  {} {} {} exit=₹{:.2} gross=₹{:+.2} net=₹{:+.2} reason={}",
            pos.underlying, dir_str, pos.tradingsymbol,
            exit_price, gross, net, reason
        );

        let trade = ClosedTrade {
            underlying: pos.underlying,
            tradingsymbol: pos.tradingsymbol,
            direction: dir_str.to_string(),
            strike: pos.strike,
            lot_size: lots_total,
            entry_price: pos.entry_price,
            exit_price,
            entry_ts_ms: pos.entry_ts_ms,
            exit_ts_ms: ts_ms,
            gross_pnl: gross,
            tx_costs,
            net_pnl: net,
            exit_reason: reason.to_string(),
            confidence: pos.confidence,
            strategy_tags: pos.strategy_tags,
        };

        self.write_trade_csv(&trade);
        self.closed_trades.push(trade);
    }

    // ─── CSV output ──────────────────────────────────────────────────────────

    fn ensure_csv(&mut self) {
        if self.trades_csv_file.is_none() {
            let is_new = self.trades_csv_path
                .metadata()
                .map(|m| m.len() == 0)
                .unwrap_or(true); // doesn't exist yet → new
            match fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.trades_csv_path)
            {
                Ok(mut f) => {
                    if is_new {
                        let header = "underlying,tradingsymbol,direction,strike,lot_size,\
                                      entry_price,exit_price,entry_ts_ms,exit_ts_ms,\
                                      gross_pnl,tx_costs,net_pnl,exit_reason,confidence,strategy_tags\n";
                        let _ = f.write_all(header.as_bytes());
                    }
                    self.trades_csv_file = Some(f);
                }
                Err(e) => warn!("Quant: cannot open trades CSV: {}", e),
            }
        }
    }

    fn write_trade_csv(&mut self, t: &ClosedTrade) {
        self.ensure_csv();
        if let Some(ref mut f) = self.trades_csv_file {
            let row = format!(
                "{},{},{},{:.1},{},{:.2},{:.2},{},{},{:.2},{:.2},{:.2},{},{:.0},{}\n",
                t.underlying, t.tradingsymbol, t.direction, t.strike, t.lot_size,
                t.entry_price, t.exit_price,
                t.entry_ts_ms, t.exit_ts_ms,
                t.gross_pnl, t.tx_costs, t.net_pnl,
                t.exit_reason, t.confidence, t.strategy_tags
            );
            let _ = f.write_all(row.as_bytes());
        }
    }

    fn write_session_summary(&self) {
        let net: f64 = self.closed_trades.iter().map(|t| t.net_pnl).sum();
        let wins = self.closed_trades.iter().filter(|t| t.net_pnl > 0.0).count();
        let losses = self.closed_trades.iter().filter(|t| t.net_pnl < 0.0).count();
        let gross: f64 = self.closed_trades.iter().map(|t| t.gross_pnl).sum();
        let costs: f64 = self.closed_trades.iter().map(|t| t.tx_costs).sum();

        let summary_path = self.log_dir.join(
            format!("{}_quant_summary.txt", ist_date_str())
        );
        let body = format!(
            "QUANT ENGINE SESSION SUMMARY\n\
             ============================\n\
             Scans run     : {}\n\
             Trades        : {}\n\
             Wins / Losses : {} / {}\n\
             Gross P&L     : ₹{:+.2}\n\
             Tx costs      : ₹{:.2}\n\
             Net P&L       : ₹{:+.2}\n\
             Final capital : ₹{:.2}\n\
             Return        : {:.1}%\n",
            self.scan_count,
            self.closed_trades.len(),
            wins, losses,
            gross, costs, net,
            self.capital,
            (self.capital - self.initial_capital) / self.initial_capital * 100.0,
        );
        let _ = fs::write(&summary_path, &body);
        warn!("{}", body);
    }
}
