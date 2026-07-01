
use crate::config::OptionsEngineConfig;
use crate::execution::{OrderCommand, OrderSide, OrderUpdate, PlaceOrderCmd};
use crate::greeks::{compute_greeks, compute_time_to_expiry_at};
use crate::ledger::{EntryContext, OptionsJournal};
use crate::models::{OptionContract, OptionType};
use crate::store::TickStore;
use crate::websocket::TickEvent;

use chrono::{DateTime, FixedOffset, NaiveDate, TimeZone, Timelike, Utc};
use std::collections::{BTreeMap, HashMap, VecDeque};
use tokio::sync::{broadcast, mpsc};
use tracing::{error, info, warn};

const NSE_OPTIONS_EXCHANGE_TXN_RATE: f64 = 0.000311;
const LIVE_ENTRY_LIMIT_PAYUP_INR: f64 = 0.25;
// Options below ₹30 are typically deep OTM or near-expiry junk — bid-ask spread
// alone can be ₹2-5 (7-16% of price), making fills far worse than the mid-price.
const MIN_TRADEABLE_OPTION_PRICE: f64 = 30.0;

/// Flat Zerodha options brokerage per EXECUTED ORDER (not per lot). Bug 5: this was
/// baked into the per-lot cost and multiplied by lots, over-charging multi-lot orders
/// and (via affordability/Kelly) under-sizing them. It is now added once per order.
const OPT_BROKERAGE_FLAT: f64 = 20.0;

/// Flat brokerage + 18% GST on it, charged ONCE per executed order (₹20 → ₹23.60).
fn options_brokerage_per_order() -> f64 {
    OPT_BROKERAGE_FLAT * 1.18
}

/// Per-LOT entry transaction cost EXCLUDING the flat per-order brokerage (everything
/// here genuinely scales with lots). GST here is only on the per-lot fees.
fn entry_cost_per_lot_excl_brokerage(entry_price: f64, lot_size: u32) -> f64 {
    let premium = entry_price * lot_size as f64;
    let exchange_txn = NSE_OPTIONS_EXCHANGE_TXN_RATE * premium;
    let sebi = 0.000001 * premium;
    let stamp_duty = 0.00003 * premium;
    let gst = 0.18 * (exchange_txn + sebi);
    exchange_txn + sebi + stamp_duty + gst
}

/// Total ENTRY-order cost for `lots` lots: per-lot fees × lots + ONE flat brokerage.
fn entry_order_cost(entry_price: f64, lot_size: u32, lots: u32) -> f64 {
    entry_cost_per_lot_excl_brokerage(entry_price, lot_size) * lots as f64
        + options_brokerage_per_order()
}

fn options_sell_stt_rate(exit_time_ms: u64) -> f64 {
    let ist = FixedOffset::east_opt(5 * 3600 + 30 * 60).expect("valid IST offset");
    let dt_ist = Utc
        .timestamp_millis_opt(exit_time_ms as i64)
        .single()
        .unwrap_or_else(Utc::now)
        .with_timezone(&ist);
    let stt_hike_date = NaiveDate::from_ymd_opt(2026, 4, 1).expect("valid date");
    if dt_ist.date_naive() >= stt_hike_date {
        0.0015
    } else {
        0.0010
    }
}

/// Per-LOT exit transaction cost EXCLUDING the flat per-order brokerage.
fn exit_cost_per_lot_excl_brokerage(exit_price: f64, lot_size: u32, exit_time_ms: u64) -> f64 {
    let premium = exit_price * lot_size as f64;
    let stt_sell = options_sell_stt_rate(exit_time_ms) * premium;
    let exchange_txn = NSE_OPTIONS_EXCHANGE_TXN_RATE * premium;
    let sebi = 0.000001 * premium;
    let gst = 0.18 * (exchange_txn + sebi);
    stt_sell + exchange_txn + sebi + gst
}

/// Total EXIT-order cost for `lots` lots: per-lot fees × lots + ONE flat brokerage.
fn exit_order_cost(exit_price: f64, lot_size: u32, lots: u32, exit_time_ms: u64) -> f64 {
    exit_cost_per_lot_excl_brokerage(exit_price, lot_size, exit_time_ms) * lots as f64
        + options_brokerage_per_order()
}

/// Total round-trip cost for `lots` lots: entry order + exit order (each pays one flat
/// brokerage — two legs, two ₹20 charges, regardless of lot count).
fn round_trip_order_cost(
    entry_price: f64,
    exit_price: f64,
    lot_size: u32,
    lots: u32,
    assumed_exit_time_ms: u64,
) -> f64 {
    entry_order_cost(entry_price, lot_size, lots)
        + exit_order_cost(exit_price, lot_size, lots, assumed_exit_time_ms)
}

fn one_lot_entry_cost(entry_price: f64, lot_size: u32) -> f64 {
    entry_price * lot_size as f64 + entry_order_cost(entry_price, lot_size, 1)
}

fn entry_cutoff_ist_mins_for_dte(days_to_expiry: f64) -> u32 {
    if days_to_expiry > 1.0 {
        14 * 60 + 55
    } else {
        15 * 60
    }
}

fn past_entry_cutoff_ist(ist_mins: u32, days_to_expiry: f64) -> bool {
    ist_mins >= entry_cutoff_ist_mins_for_dte(days_to_expiry)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StrikeScanDirection {
    Up,
    Down,
}

fn nearest_affordable_otm_idx(
    prices: &[f64],
    start_idx: usize,
    direction: StrikeScanDirection,
    lot_size: u32,
    deployable_capital: f64,
    min_price: f64,
) -> (Option<usize>, usize) {
    if prices.is_empty() || start_idx >= prices.len() || lot_size == 0 || deployable_capital <= 0.0 {
        return (None, 0);
    }

    let mut checked = 0usize;
    match direction {
        StrikeScanDirection::Up => {
            for idx in start_idx..prices.len() {
                checked += 1;
                let price = prices[idx];
                if !price.is_finite() || price < min_price {
                    continue;
                }
                if one_lot_entry_cost(price, lot_size) <= deployable_capital {
                    return (Some(idx), checked);
                }
            }
        }
        StrikeScanDirection::Down => {
            for idx in (0..=start_idx).rev() {
                checked += 1;
                let price = prices[idx];
                if !price.is_finite() || price < min_price {
                    continue;
                }
                if one_lot_entry_cost(price, lot_size) <= deployable_capital {
                    return (Some(idx), checked);
                }
            }
        }
    }

    (None, checked)
}


#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MarketRegime {
    StrongBullish,
    Bullish,
    Sideways,
    Neutral,
    Bearish,
    StrongBearish,
    PanicHighVol,
    ComplacencyLowVol,
}

impl std::fmt::Display for MarketRegime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MarketRegime::StrongBullish     => write!(f, "STRONG BULL"),
            MarketRegime::Bullish           => write!(f, "BULLISH"),
            MarketRegime::Sideways          => write!(f, "SIDEWAYS"),
            MarketRegime::Neutral           => write!(f, "NEUTRAL"),
            MarketRegime::Bearish           => write!(f, "BEARISH"),
            MarketRegime::StrongBearish     => write!(f, "STRONG BEAR"),
            MarketRegime::PanicHighVol      => write!(f, "PANIC/HIGH-VOL"),
            MarketRegime::ComplacencyLowVol => write!(f, "LOW-VOL"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignalAction {
    BuyCE,
    BuyPE,
    BuyStraddle,
    BuyStrangle,
    Hold,
}

impl std::fmt::Display for SignalAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SignalAction::BuyCE       => write!(f, "BUY CE  📈"),
            SignalAction::BuyPE       => write!(f, "BUY PE  📉"),
            SignalAction::BuyStraddle => write!(f, "BUY STRADDLE ⚡"),
            SignalAction::BuyStrangle => write!(f, "BUY STRANGLE 🎯"),
            SignalAction::Hold        => write!(f, "HOLD    ⏸"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StrategyType {
    GammaScalp,
    IVExpansion,
    SpotReversal,
    TrendFollow,
    MaxPainConvergence,
    OIDivergence,
    IVSkewReversion,
    GEXPlay,
    Composite,
}

impl std::fmt::Display for StrategyType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StrategyType::GammaScalp        => write!(f, "Gamma Scalp (0-2 DTE)"),
            StrategyType::IVExpansion       => write!(f, "IV Expansion (Event)"),
            StrategyType::SpotReversal      => write!(f, "Spot Reversal Break"),
            StrategyType::TrendFollow       => write!(f, "Trend Follow + OI"),
            StrategyType::MaxPainConvergence => write!(f, "Max Pain Gravity"),
            StrategyType::OIDivergence      => write!(f, "OI Divergence Flow"),
            StrategyType::IVSkewReversion   => write!(f, "IV Skew Reversion"),
            StrategyType::GEXPlay           => write!(f, "GEX Dealer Flow"),
            StrategyType::Composite         => write!(f, "Composite Multi-Signal"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SessionPhase {
    PreOpen,
    OpeningBell,
    Morning,
    Midday,
    Afternoon,
    Closing,
    AfterMarket,
}

impl std::fmt::Display for SessionPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SessionPhase::PreOpen      => write!(f, "Pre-Open"),
            SessionPhase::OpeningBell  => write!(f, "Opening Bell"),
            SessionPhase::Morning      => write!(f, "Morning"),
            SessionPhase::Midday       => write!(f, "Midday"),
            SessionPhase::Afternoon    => write!(f, "Afternoon"),
            SessionPhase::Closing      => write!(f, "CLOSING — EXIT ONLY"),
            SessionPhase::AfterMarket  => write!(f, "After Market"),
        }
    }
}


#[derive(Debug, Clone, Default)]
pub struct StrikeLevel {
    pub strike: f64,

    pub ce_token: Option<u32>,
    pub ce_ltp: f64,
    pub ce_oi: u64,
    pub ce_oi_change: i64,
    pub ce_volume: u64,
    pub ce_iv: f64,
    pub ce_delta: f64,
    pub ce_gamma: f64,
    pub ce_theta: f64,

    pub pe_token: Option<u32>,
    pub pe_ltp: f64,
    pub pe_oi: u64,
    pub pe_oi_change: i64,
    pub pe_volume: u64,
    pub pe_iv: f64,
    pub pe_delta: f64,
    pub pe_gamma: f64,
    pub pe_theta: f64,

    pub iv_skew: f64,
    pub ce_pe_oi_ratio: f64,
    pub straddle_premium: f64,
    pub gex_contribution: f64,
}

#[derive(Debug, Clone)]
pub struct ChainSnapshot {
    pub underlying: String,
    pub spot: f64,
    pub timestamp_ms: u64,
    pub expiry: String,
    pub strikes: Vec<StrikeLevel>,

    pub atm_strike: f64,
    pub atm_iv: f64,
    pub iv_rank: f64,
    pub pcr_oi: f64,
    pub pcr_vol: f64,
    pub max_pain: f64,
    pub total_ce_oi: u64,
    pub total_pe_oi: u64,
    pub total_ce_vol: u64,
    pub total_pe_vol: u64,
    pub net_gex: f64,
    pub days_to_expiry: f64,
    pub regime: MarketRegime,
    /// Overnight gap: (today_open - prev_session_close) / prev_session_close * 100.
    /// Positive = gap up, negative = gap down. Zero when prev_close unavailable.
    pub overnight_gap_pct: f64,
}

#[derive(Debug, Clone)]
pub struct Signal {
    pub id: u64,
    pub underlying: String,
    pub expiry: String,
    pub action: SignalAction,
    pub strike: f64,
    pub option_price: f64,
    pub target_price: f64,
    pub stop_price: f64,
    pub lots: u32,
    pub lot_size: u32,
    pub capital_required: f64,
    pub confidence: f64,
    pub strategy: StrategyType,
    pub reasons: Vec<(String, f64)>,
    pub timestamp_ms: u64,
    pub session: SessionPhase,
    pub breakeven_move_pct: f64,
    pub max_loss: f64,
    pub max_profit_target: f64,
    pub risk_reward: f64,
    pub entry_ctx: EntryContext,
}

#[derive(Debug, Clone)]
pub struct Position {
    pub id: u64,
    pub underlying: String,
    pub tradingsymbol: String,
    pub action: SignalAction,
    pub strike: f64,
    pub expiry: String,
    pub option_token: u32,
    pub lots: u32,
    pub lot_size: u32,
    pub entry_price: f64,
    pub target_price: f64,
    pub stop_price: f64,
    pub entry_time_ms: u64,
    pub entry_datetime: String,
    pub strategy: StrategyType,
    pub is_open: bool,
    pub exit_price: f64,
    pub exit_time_ms: u64,
    pub pnl: f64,
    pub pnl_pct: f64,
    pub exit_reason: String,
    pub capital_deployed: f64,
    pub entry_ctx: EntryContext,
    pub breakeven_stop_set: bool,
    pub exit_pending: bool,
    /// Number of 30-min hold checkpoints already evaluated.
    /// At each new checkpoint the trade must be in profit to continue.
    pub checkpoints_evaluated: u32,
}

#[derive(Debug, Clone)]
struct PendingEntryOrder {
    pos_id: u64,
    signal: Signal,
    token: u32,
    tradingsymbol: String,
    estimated_capital: f64,
    tag: String,
    placed_ms: u64,
    last_status_poll_ms: u64,
    cancel_requested: bool,
    /// Why the engine decided to cancel; None until a cancel is triggered.
    cancel_reason: Option<CancelReasonKind>,
    /// Last wall-clock ms at which a CancelByTag was sent — used for retry backoff.
    last_cancel_attempt_ms: u64,
    /// True once capital has been released back to the pool (set by release_pending_entry).
    /// Guards against double-subtraction if a terminal update arrives after removal.
    released_after_timeout: bool,
    order_id: Option<String>,
    /// Signal option price at queue time — used as reference for price-reversal guard.
    signal_price: f64,
    /// Scan cycle in which this order was placed; used to skip same-cycle scan cancellation.
    placed_at_scan_count: u64,
}

#[derive(Debug, Clone)]
struct PendingExitOrder {
    pos_id: u64,
    tag: String,
    tradingsymbol: String,
    total_quantity: u32,
    placed_ms: u64,
    last_status_poll_ms: u64,
    reason: String,
    cancel_requested: bool,
    last_cancel_attempt_ms: u64,
    market_fallback_sent: bool,
    /// Best available execution reference when the broker omits average_price on COMPLETE.
    fallback_book_price: f64,
    total_filled_quantity: u32,
    total_filled_notional: f64,
    current_order_filled_quantity: u32,
    current_order_filled_notional: f64,
}

impl PendingExitOrder {
    fn remaining_quantity(&self) -> u32 {
        self.total_quantity.saturating_sub(self.total_filled_quantity)
    }

    fn avg_fill_price(&self) -> Option<f64> {
        if self.total_filled_quantity == 0 {
            None
        } else {
            let avg = self.total_filled_notional / self.total_filled_quantity as f64;
            (avg > 0.0).then_some(avg)
        }
    }

    fn best_effort_fill_price(&self, update_average_price: Option<f64>) -> Option<f64> {
        self.avg_fill_price()
            .or_else(|| update_average_price.filter(|price| *price > 0.0))
            .or_else(|| (self.fallback_book_price > 0.0).then_some(self.fallback_book_price))
    }

    fn record_fill_progress(&mut self, update: &OrderUpdate) {
        let Some(filled_quantity) = update.filled_quantity else {
            return;
        };
        if filled_quantity < self.current_order_filled_quantity {
            return;
        }

        let average_price = update.average_price.unwrap_or(0.0).max(0.0);
        let order_notional = average_price * filled_quantity as f64;
        let delta_qty = filled_quantity.saturating_sub(self.current_order_filled_quantity);
        let delta_notional = order_notional - self.current_order_filled_notional;

        if delta_qty > 0 {
            self.total_filled_quantity = self.total_filled_quantity.saturating_add(delta_qty);
        }
        if delta_qty > 0 || delta_notional.abs() > 0.0001 {
            self.total_filled_notional = (self.total_filled_notional + delta_notional).max(0.0);
            self.current_order_filled_quantity = filled_quantity;
            self.current_order_filled_notional = order_notional.max(0.0);
        }
    }

    fn prepare_market_fallback(&mut self, now_ms: u64, fallback_book_price: f64) {
        self.placed_ms = now_ms;
        self.last_status_poll_ms = 0;
        self.cancel_requested = false;
        self.last_cancel_attempt_ms = 0;
        self.market_fallback_sent = true;
        if fallback_book_price > 0.0 {
            self.fallback_book_price = fallback_book_price;
        }
        self.current_order_filled_quantity = 0;
        self.current_order_filled_notional = 0.0;
    }
}

/// Why a pending limit entry order was cancelled by the engine (not the broker).
/// Used for log observability and distinguishes the four guard paths.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CancelReasonKind {
    Timeout,
    PriceReversal,
    ScanDirectionFlip,
    ScanScoreCollapse,
}

impl CancelReasonKind {
    fn as_str(self) -> &'static str {
        match self {
            CancelReasonKind::Timeout           => "timeout",
            CancelReasonKind::PriceReversal     => "price reversal",
            CancelReasonKind::ScanDirectionFlip => "scan direction flip",
            CancelReasonKind::ScanScoreCollapse => "scan score collapse",
        }
    }
}

/// Snapshot of the most recent scan result for one underlying.
/// Used to validate pending limit entry orders between scans.
#[derive(Debug, Clone)]
struct LastScanResult {
    /// Dominant direction the engine scored this underlying (BuyCE / BuyPE / Hold).
    action: SignalAction,
    /// Highest composite confidence seen across all strategies (0 when no signal fired).
    best_score: f64,
    /// Wall-clock ms when this scan ran.
    scanned_at_ms: u64,
}

#[derive(Debug, Clone, Copy)]
struct RollingSpotStats {
    range_pct: f64,
    q25_abs_ret: f64,
    q50_abs_ret: f64,
    q75_abs_ret: f64,
    sample_count: usize,
}


struct RiskEngine {
    capital: f64,
    initial_capital: f64,
    reserved_capital: f64,
    win_count: u32,
    loss_count: u32,
    total_win_amount: f64,
    total_loss_amount: f64,
    daily_start_capital: f64,
    max_daily_loss_fraction: f64,
    max_daily_profit_fraction: f64,
    kelly_scale: f64,
}

impl RiskEngine {
    fn new(initial_capital: f64, max_daily_loss_fraction: f64, max_daily_profit_fraction: f64) -> Self {
        Self {
            capital: initial_capital,
            initial_capital,
            reserved_capital: 0.0,
            win_count: 0,
            loss_count: 0,
            total_win_amount: 0.0,
            total_loss_amount: 0.0,
            daily_start_capital: initial_capital,
            max_daily_loss_fraction: max_daily_loss_fraction.clamp(0.01, 0.95),
            max_daily_profit_fraction: max_daily_profit_fraction.clamp(0.01, 5.0),
            kelly_scale: 0.5,
        }
    }

    fn available_capital(&self) -> f64 {
        (self.capital - self.reserved_capital).max(0.0)
    }

    fn kelly_fraction(&self) -> f64 {
        let total_trades = self.win_count + self.loss_count;
        let p = if total_trades == 0 {
            0.55
        } else {
            self.win_count as f64 / total_trades as f64
        };
        let q = 1.0 - p;
        let b = if self.loss_count == 0 || self.total_loss_amount == 0.0 {
            2.0
        } else {
            let avg_win = self.total_win_amount / self.win_count.max(1) as f64;
            let avg_loss = self.total_loss_amount / self.loss_count as f64;
            avg_win / avg_loss.max(0.01)
        };
        let full_kelly = ((p * b) - q) / b;
        (full_kelly * self.kelly_scale)
            .max(0.08)
            .min(0.38)
    }

    fn capital_tier(&self) -> u8 {
        let capital = self.available_capital();
        if capital < 12_000.0 { 1 }
        else if capital < 30_000.0 { 2 }
        else { 3 }
    }

    fn size_lots(&self, option_price: f64, lot_size: u32, stop_loss_pct: f64) -> u32 {
        let available = self.available_capital();
        let sizing_capital = available.min(self.daily_start_capital);
        // Per-lot cost EXCLUDES the flat per-order brokerage (bug 5) — that is subtracted
        // once below, not multiplied into every lot, so affordability/Kelly aren't biased
        // against larger orders.
        let cost_per_lot = option_price * lot_size as f64
            + entry_cost_per_lot_excl_brokerage(option_price, lot_size);
        if cost_per_lot <= 0.0 { return 0; }
        let deployable = (sizing_capital - options_brokerage_per_order()).max(0.0);
        let affordable = (deployable / cost_per_lot).floor() as u32;
        if affordable == 0 { return 0; }

        let kelly = self.kelly_fraction();
        let max_risk_capital = self.daily_start_capital * kelly;
        let risk_per_lot = (stop_loss_pct * option_price * lot_size as f64).max(1.0);
        let lots_from_risk = (max_risk_capital / risk_per_lot).floor() as u32;

        let lots_from_exposure = (deployable * (kelly * 2.5).min(0.95) / cost_per_lot).floor() as u32;

        // STRICT half-Kelly: NO `.max(1)` floor. If one lot's stop-risk exceeds the
        // half-Kelly budget (`lots_from_risk == 0`), return 0 → the caller SKIPS the
        // trade rather than forcing an oversized bet. Consequence (by design): on
        // wide-stop / low-conviction days the options engine takes NO trade — a no-trade
        // day is correct behavior here, not a bug.
        lots_from_risk.min(lots_from_exposure).min(4).min(affordable)
    }

    fn max_one_lot_entry_cost_by_exposure(&self) -> f64 {
        let available = self.available_capital();
        let sizing_capital = available.min(self.daily_start_capital);
        let deployable = (sizing_capital - options_brokerage_per_order()).max(0.0);
        deployable * (self.kelly_fraction() * 2.5).min(0.95) + options_brokerage_per_order()
    }

    fn reserve_capital(&mut self, amount: f64) -> bool {
        if amount <= 0.0 {
            return false;
        }
        if amount > self.available_capital() {
            return false;
        }
        self.reserved_capital += amount;
        true
    }

    fn release_reserved_capital(&mut self, amount: f64) {
        if amount <= 0.0 {
            return;
        }
        self.reserved_capital = (self.reserved_capital - amount).max(0.0);
    }

    fn circuit_breaker_triggered(&self) -> bool {
        self.circuit_reason().is_some()
    }

    /// Returns the tripped circuit ("LOWER -15% loss" / "UPPER +35% profit") or None.
    /// P&L here is net of fees/costs because `capital` is updated with net trade P&L.
    fn circuit_reason(&self) -> Option<&'static str> {
        let daily_pnl = self.capital - self.daily_start_capital;
        if daily_pnl <= -(self.daily_start_capital * self.max_daily_loss_fraction) {
            Some("LOWER circuit (daily loss limit)")
        } else if daily_pnl >= self.daily_start_capital * self.max_daily_profit_fraction {
            Some("UPPER circuit (daily profit limit — anti-overtrade)")
        } else {
            None
        }
    }

    fn record_trade(&mut self, pnl: f64) {
        if pnl >= 0.0 {
            self.win_count += 1;
            self.total_win_amount += pnl;
        } else {
            self.loss_count += 1;
            self.total_loss_amount += pnl.abs();
        }
        self.capital += pnl;
    }

    #[allow(dead_code)]
    fn reset_daily(&mut self) {
        self.daily_start_capital = self.capital;
        self.reserved_capital = 0.0;
    }
}


struct IVHistory {
    data: HashMap<String, VecDeque<(u64, f64)>>,
    max_samples: usize,
}

impl IVHistory {
    fn new() -> Self {
        Self {
            data: HashMap::new(),
            max_samples: 2000,
        }
    }

    fn push(&mut self, underlying: &str, ts: u64, iv: f64) {
        let buf = self.data.entry(underlying.to_string()).or_default();
        buf.push_back((ts, iv));
        while buf.len() > self.max_samples {
            buf.pop_front();
        }
    }

    fn iv_rank(&self, underlying: &str, current_iv: f64) -> f64 {
        let buf = match self.data.get(underlying) {
            Some(b) if b.len() >= 10 => b,
            _ => {
                let min_iv = 0.10_f64;
                let max_iv = 0.35_f64;
                let rank = (current_iv - min_iv) / (max_iv - min_iv) * 100.0;
                return rank.clamp(0.0, 100.0);
            }
        };
        let ivs: Vec<f64> = buf.iter().map(|(_, iv)| *iv).collect();
        let min_iv = ivs.iter().cloned().fold(f64::INFINITY, f64::min);
        let max_iv = ivs.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        if (max_iv - min_iv).abs() < 1e-6 {
            return 50.0;
        }
        ((current_iv - min_iv) / (max_iv - min_iv) * 100.0).clamp(0.0, 100.0)
    }
}


fn session_phase_ist_from_ms(timestamp_ms: u64) -> SessionPhase {
    let now_utc: DateTime<Utc> = Utc
        .timestamp_millis_opt(timestamp_ms as i64)
        .single()
        .unwrap_or_else(Utc::now);
    let ist_offset = chrono::FixedOffset::east_opt(5 * 3600 + 30 * 60)
        .expect("valid IST offset");
    let now_ist = now_utc.with_timezone(&ist_offset);
    let h = now_ist.hour();
    let m = now_ist.minute();
    let mins = h * 60 + m;

    match mins {
        0..=539          => SessionPhase::PreOpen,
        540..=554        => SessionPhase::PreOpen,
        555..=569        => SessionPhase::OpeningBell,
        570..=719        => SessionPhase::Morning,
        720..=839        => SessionPhase::Midday,
        840..=914        => SessionPhase::Afternoon,
        915..=929        => SessionPhase::Closing,
        _                => SessionPhase::AfterMarket,
    }
}

fn datetime_string_from_ms(timestamp_ms: u64) -> String {
    let ist_offset = chrono::FixedOffset::east_opt(5 * 3600 + 30 * 60)
        .expect("valid IST offset");
    Utc.timestamp_millis_opt(timestamp_ms as i64)
        .single()
        .map(|dt| dt.with_timezone(&ist_offset).format("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_else(|| Utc::now().with_timezone(&ist_offset).format("%Y-%m-%d %H:%M:%S").to_string())
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}


fn compute_max_pain(strikes: &[StrikeLevel]) -> f64 {
    let candidate_strikes: Vec<f64> = strikes.iter().map(|s| s.strike).collect();
    let mut min_pain = f64::INFINITY;
    let mut max_pain_strike = candidate_strikes.first().cloned().unwrap_or(0.0);

    for &k in &candidate_strikes {
        let pain: f64 = strikes.iter().map(|s| {
            let ce_pain = (k - s.strike).max(0.0) * s.ce_oi as f64;
            let pe_pain = (s.strike - k).max(0.0) * s.pe_oi as f64;
            ce_pain + pe_pain
        }).sum();

        if pain < min_pain {
            min_pain = pain;
            max_pain_strike = k;
        }
    }
    max_pain_strike
}

fn compute_gex(strikes: &[StrikeLevel], spot: f64, lot_size: u32) -> f64 {
    strikes.iter().map(|s| {
        let ce_gex = s.ce_gamma * s.ce_oi as f64;
        let pe_gex = s.pe_gamma * s.pe_oi as f64;
        (ce_gex - pe_gex) * spot * lot_size as f64
    }).sum()
}


pub struct OptionsEngine {
    contracts: Vec<OptionContract>,
    store: TickStore,
    underlying_tokens: HashMap<String, u32>,

    iv_history: IVHistory,

    spot_history: HashMap<String, VecDeque<(u64, f64)>>,

    prev_oi: HashMap<u32, u64>,

    risk: RiskEngine,

    positions: Vec<Position>,
    next_pos_id: u64,
    next_sig_id: u64,

    journal: OptionsJournal,

    profit_target_pct: f64,
    stop_loss_pct: f64,
    min_confidence: f64,
    expiry_day_min_confidence: f64,

    risk_free_rate: f64,
    dividend_yield: f64,

    scan_count: u64,

    clock_override_ms: Option<u64>,
    replay_last_scan_ms: Option<u64>,
    warmup_until_ms: Option<u64>,

    scan_interval_secs: u64,
    max_daily_trades: u32,
    execution_buy_offset_inr: f64,
    execution_sell_offset_inr: f64,

    signals_generated: u64,
    entry_attempts: u64,
    entry_rejections: u64,
    positions_opened: u64,
    positions_closed: u64,

    trade_day_ist: Option<String>,
    daily_trades_opened: u32,

    last_signal_ms: HashMap<String, u64>,
    stop_streak_by_underlying: HashMap<String, u32>,

    pending_signals: VecDeque<Signal>,

    order_tx: Option<mpsc::UnboundedSender<OrderCommand>>,
    order_updates_rx: Option<mpsc::UnboundedReceiver<OrderUpdate>>,
    order_tag_prefix: String,
    pending_entry_orders: Vec<PendingEntryOrder>,
    pending_exit_orders: Vec<PendingExitOrder>,
    /// Per-position consecutive zero-fill exit-order failures. Bounds resubmission so a
    /// persistently-rejected exit (e.g. a funds/margin reject, which on a CLOSE proves the
    /// broker holds no inventory) can never spray the broker forever.
    exit_failures: HashMap<u64, u32>,
    pending_capital_reserved: f64,
    max_concurrent_positions: usize,
    entry_order_timeout_ms: u64,
    order_status_poll_interval_ms: u64,
    /// Cancel pending entry if current LTP drops this fraction below the limit price
    /// (signal direction has reversed). Default 0.15 = 15%.
    limit_cancel_reversal_pct: f64,
    /// Most recent scan result per underlying — used to invalidate pending entry orders
    /// when the engine's view of direction or confidence changes between scans.
    last_scan_result: HashMap<String, LastScanResult>,

    // Live capital refresh from Kite margins API
    kite_api_key: String,
    kite_access_token: String,
    capital_sync_needed: bool,

    log_dir: String,

    // Account-level shared circuit (halts entries + receives net P&L). None = solo.
    shared_circuit: Option<crate::portfolio::SharedCircuit>,
    /// Per-underlying spot price-action series → technical directional gate.
    spot_series: std::collections::HashMap<String, crate::technicals::SpotSeries>,
}

impl OptionsEngine {
    pub fn new(
        contracts: Vec<OptionContract>,
        store: TickStore,
        underlying_tokens: HashMap<String, u32>,
        cfg: &OptionsEngineConfig,
        risk_free_rate: f64,
        dividend_yield: f64,
        log_dir: &str,
    ) -> Self {
        Self::new_with_date(
            contracts,
            store,
            underlying_tokens,
            cfg,
            risk_free_rate,
            dividend_yield,
            log_dir,
            None,
        )
    }

    pub fn new_with_date(
        contracts: Vec<OptionContract>,
        store: TickStore,
        underlying_tokens: HashMap<String, u32>,
        cfg: &OptionsEngineConfig,
        risk_free_rate: f64,
        dividend_yield: f64,
        log_dir: &str,
        replay_date: Option<&str>,
    ) -> Self {
        let initial_capital = cfg.initial_capital.max(0.0);
        let max_daily_loss_fraction = (cfg.max_daily_loss_pct / 100.0).clamp(0.01, 0.95);
        let max_daily_profit_fraction = (cfg.max_daily_profit_pct / 100.0).clamp(0.01, 5.0);
        let journal = OptionsJournal::new(log_dir, initial_capital, replay_date)
            .unwrap_or_else(|e| {
                warn!("Failed to open options journal in '{}': {}", log_dir, e);
                OptionsJournal::new(".", initial_capital, None)
                    .expect("Could not open options journal even in current directory")
            });
        Self {
            contracts,
            store,
            underlying_tokens,
            iv_history: IVHistory::new(),
            spot_history: HashMap::new(),
            prev_oi: HashMap::new(),
            risk: RiskEngine::new(initial_capital, max_daily_loss_fraction, max_daily_profit_fraction),
            journal,
            positions: Vec::new(),
            next_pos_id: 1,
            next_sig_id: 1,
            profit_target_pct: cfg.profit_target_pct.clamp(1.0, 200.0),
            stop_loss_pct: cfg.stop_loss_pct.clamp(1.0, 95.0),
            min_confidence: cfg.min_confidence.clamp(0.0, 100.0),
            expiry_day_min_confidence: cfg.expiry_day_min_confidence.clamp(0.0, 100.0),
            risk_free_rate,
            dividend_yield,
            scan_count: 0,
            clock_override_ms: None,
            replay_last_scan_ms: None,
            warmup_until_ms: None,
            scan_interval_secs: cfg.scan_interval_secs.max(1),
            // 0 = unlimited (rely on the daily P&L circuits instead of a count cap).
            max_daily_trades: if cfg.max_daily_trades == 0 { u32::MAX } else { cfg.max_daily_trades.min(50) },
            execution_buy_offset_inr: 0.0,
            execution_sell_offset_inr: 0.0,
            signals_generated: 0,
            entry_attempts: 0,
            entry_rejections: 0,
            positions_opened: 0,
            positions_closed: 0,
            trade_day_ist: None,
            daily_trades_opened: 0,
            last_signal_ms: HashMap::new(),
            stop_streak_by_underlying: HashMap::new(),
            pending_signals: VecDeque::new(),
            order_tx: None,
            order_updates_rx: None,
            order_tag_prefix: "SATA".to_string(),
            pending_entry_orders: Vec::new(),
            pending_exit_orders: Vec::new(),
            exit_failures: HashMap::new(),
            pending_capital_reserved: 0.0,
            max_concurrent_positions: 4,
            entry_order_timeout_ms: 4 * 60 * 1_000,
            order_status_poll_interval_ms: 2_000,
            limit_cancel_reversal_pct: 0.15,
            last_scan_result: HashMap::new(),
            kite_api_key: String::new(),
            kite_access_token: String::new(),
            capital_sync_needed: false,
            log_dir: log_dir.to_string(),
            shared_circuit: None,
            spot_series: std::collections::HashMap::new(),
        }
    }

    /// Attach the account-level shared circuit (halts entries + receives net P&L).
    pub fn set_shared_circuit(&mut self, c: crate::portfolio::SharedCircuit) {
        self.shared_circuit = Some(c);
    }

    fn current_time_ms(&self) -> u64 {
        self.clock_override_ms.unwrap_or_else(now_ms)
    }

    fn current_session(&self) -> SessionPhase {
        session_phase_ist_from_ms(self.current_time_ms())
    }

    fn current_day_ist(&self) -> String {
        let ist = FixedOffset::east_opt(5 * 3600 + 30 * 60)
            .expect("valid IST offset");
        Utc.timestamp_millis_opt(self.current_time_ms() as i64)
            .single()
            .unwrap_or_else(Utc::now)
            .with_timezone(&ist)
            .format("%Y-%m-%d")
            .to_string()
    }

    fn min_confidence_floor_for(
        &self,
        _snap: &ChainSnapshot,
        _primary_strategy: StrategyType,
    ) -> f64 {
        self.min_confidence
    }

    fn reset_daily_state_if_needed(&mut self) {
        let day = self.current_day_ist();
        let is_new_day = self
            .trade_day_ist
            .as_ref()
            .map(|d| d != &day)
            .unwrap_or(true);
        if is_new_day {
            self.trade_day_ist = Some(day);
            self.daily_trades_opened = 0;
            self.risk.reset_daily();
            self.stop_streak_by_underlying.clear();
        }
    }

    fn daily_trade_slots_used(&self) -> u32 {
        let pending = self
            .pending_entry_orders
            .iter()
            .filter(|p| !p.released_after_timeout)
            .count() as u32;
        self.daily_trades_opened.saturating_add(pending)
    }

    fn effective_daily_trade_cap(&self, signal: &Signal) -> u32 {
        let dynamic_cap = self.dynamic_daily_trade_cap(signal);
        if self.max_daily_trades == 0 {
            dynamic_cap
        } else {
            dynamic_cap.min(self.max_daily_trades)
        }
    }

    fn set_clock_override_ms(&mut self, timestamp_ms: u64) {
        self.clock_override_ms = Some(timestamp_ms);
    }

    fn clear_clock_override(&mut self) {
        self.clock_override_ms = None;
    }

    pub fn process_replay_tick(&mut self, timestamp_ms: u64) {
        self.set_clock_override_ms(timestamp_ms);
        self.process_order_updates();
        self.poll_pending_orders();
        self.check_open_positions();

        let session = self.current_session();
        if matches!(session, SessionPhase::PreOpen | SessionPhase::AfterMarket) {
            return;
        }

        if !self.pending_signals.is_empty() {
            self.execute_pending_signals();
        }

        let scan_interval_ms = self.scan_interval_secs.saturating_mul(1_000);
        let should_scan = self.replay_last_scan_ms
            .map(|last| timestamp_ms.saturating_sub(last) >= scan_interval_ms)
            .unwrap_or(true);
        if should_scan {
            self.scan_count += 1;
            self.replay_last_scan_ms = Some(timestamp_ms);
            self.run_full_scan();
        }
    }

    pub fn finalize_replay(&mut self, reason: &str) {
        let mut close_indices = Vec::new();
        for (idx, pos) in self.positions.iter().enumerate() {
            if !pos.is_open {
                continue;
            }
            if let Some(tick) = self.store.get(pos.option_token) {
                if tick.ltp > 0.0 {
                    close_indices.push((idx, tick.ltp));
                }
            }
        }

        for (idx, price) in close_indices {
            self.close_position_at(idx, price, reason.to_string());
        }

        let open_count = self.positions.iter().filter(|p| p.is_open).count();
        self.journal.write_session_summary(open_count);
    }

    pub fn ending_capital(&self) -> f64 {
        self.risk.capital
    }

    pub fn set_execution_fill_offsets(&mut self, buy_offset: f64, sell_offset: f64) {
        self.execution_buy_offset_inr = buy_offset.max(0.0);
        self.execution_sell_offset_inr = sell_offset.max(0.0);
    }

    /// Configure limit-order behaviour for live entry orders.
    /// `timeout_secs`   — cancel the order if unfilled after this many seconds (default: 240 = 4 min).
    /// `reversal_pct`   — cancel if the current LTP falls this fraction below the limit price,
    ///                    indicating the signal direction has reversed (default: 0.15 = 15%).
    pub fn set_entry_order_config(&mut self, timeout_secs: u64, reversal_pct: f64) {
        self.entry_order_timeout_ms = timeout_secs.max(30) * 1_000;
        self.limit_cancel_reversal_pct = reversal_pct.clamp(0.05, 0.50);
    }

    /// Suppress signal generation until `ts_ms`. Use in backtest to build OI/IV
    /// baselines before trading begins (mirrors the live OpeningBell run-in).
    pub fn set_warmup_until_ms(&mut self, ts_ms: u64) {
        self.warmup_until_ms = Some(ts_ms);
    }

    pub fn set_live_order_bridge(
        &mut self,
        order_tx: mpsc::UnboundedSender<OrderCommand>,
        order_updates_rx: Option<mpsc::UnboundedReceiver<OrderUpdate>>,
        tag_prefix: String,
    ) {
        self.order_tx = Some(order_tx);
        self.order_updates_rx = order_updates_rx;
        // Kite enforces [a-zA-Z0-9] on tags — strip anything outside that set.
        let clean: String = tag_prefix
            .chars()
            .filter(|c| c.is_ascii_alphanumeric())
            .collect::<String>()
            .to_ascii_uppercase();
        self.order_tag_prefix = if clean.is_empty() {
            "SATA".to_string()
        } else {
            clean
        };
    }

    /// Store Kite credentials so the engine can re-fetch live margin balance
    /// every hour and after every trade closes.
    pub fn set_capital_refresh_credentials(&mut self, api_key: String, access_token: String) {
        self.kite_api_key = api_key;
        self.kite_access_token = access_token;
    }

    async fn sync_capital_from_kite(&mut self) {
        if !self.live_mode_enabled() {
            self.capital_sync_needed = false;
            return;
        }
        if self.kite_api_key.is_empty() {
            return;
        }
        let open = self.positions.iter().filter(|p| p.is_open).count();
        let pending_entries = self
            .pending_entry_orders
            .iter()
            .filter(|p| !p.released_after_timeout)
            .count();
        let pending_exits = self.pending_exit_orders.len();
        if open > 0 || pending_entries > 0 || pending_exits > 0 {
            info!(
                "  Capital sync deferred: {} open position(s), {} pending entry order(s), {} pending exit order(s); will retry when flat",
                open,
                pending_entries,
                pending_exits
            );
            return;
        }
        match crate::execution::fetch_live_available_funds(
            &self.kite_api_key,
            &self.kite_access_token,
        )
        .await
        {
            Ok(funds) => {
                let old = self.risk.capital;
                self.risk.capital = funds.max(0.0);
                self.capital_sync_needed = false;
                info!(
                    "  Capital synced from Kite margins: ₹{:.2} → ₹{:.2}",
                    old, funds
                );
                // Persist to dated file so restarts during the day pick up latest value.
                let date = self.current_day_ist();
                let capital_file = format!("{}/{}_starting_capital.txt", self.log_dir, date);
                if let Err(e) = std::fs::write(&capital_file, format!("{:.2}", funds)) {
                    warn!("  Could not update capital file {}: {}", capital_file, e);
                }
            }
            Err(e) => {
                warn!("  Capital sync from Kite margins failed: {}", e);
            }
        }
    }

    fn build_order_tag(&self, stage: &str, id: u64) -> String {
        // Kite Connect enforces [a-zA-Z0-9] only — no hyphens allowed.
        let mut tag = format!("{}{}{}", self.order_tag_prefix, stage, id);
        if tag.len() > 20 {
            tag.truncate(20);
        }
        tag
    }

    fn send_order_command(&self, cmd: OrderCommand) {
        if let Some(tx) = &self.order_tx {
            if let Err(e) = tx.send(cmd) {
                warn!("Live order bridge: failed to send command: {}", e);
            }
        }
    }

    fn live_mode_enabled(&self) -> bool {
        self.order_tx.is_some()
    }

    fn active_slot_count(&self) -> usize {
        self.positions.iter().filter(|p| p.is_open).count()
            + self
                .pending_entry_orders
                .iter()
                .filter(|p| !p.released_after_timeout)
                .count()
    }

    fn available_capital_for_new_orders(&self) -> f64 {
        (self.risk.available_capital() - self.pending_capital_reserved).max(0.0)
    }

    fn live_entry_limit_price(&self, token: u32, signal_price: f64) -> f64 {
        let current_price = self
            .store
            .get(token)
            .map(|tick| tick.ltp)
            .filter(|price| price.is_finite() && *price > 0.0)
            .unwrap_or(signal_price);
        (current_price + LIVE_ENTRY_LIMIT_PAYUP_INR).max(0.05)
    }

    fn recent_stop_loss_block(
        &self,
        underlying: &str,
        action: &SignalAction,
        cooldown_ms: u64,
    ) -> bool {
        if cooldown_ms == 0 {
            return false;
        }
        let now = self.current_time_ms();
        for pos in self.positions.iter().rev() {
            if pos.is_open || pos.underlying != underlying || pos.action != *action {
                continue;
            }
            let age = now.saturating_sub(pos.exit_time_ms);
            if age > cooldown_ms {
                break;
            }
            if pos.exit_reason.starts_with("STOP HIT") {
                return true;
            }
        }
        false
    }

    fn process_order_updates(&mut self) {
        let mut disconnected = false;
        let mut updates = Vec::new();
        if let Some(rx) = self.order_updates_rx.as_mut() {
            loop {
                match rx.try_recv() {
                    Ok(update) => updates.push(update),
                    Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                    Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                        disconnected = true;
                        break;
                    }
                }
            }
        }
        if disconnected {
            warn!("Live order update channel disconnected; switching to simulation-only status polling off");
            self.order_updates_rx = None;
            self.order_tx = None;
        }

        for update in updates {
            self.handle_order_update(update);
        }
    }

    fn handle_order_update(&mut self, update: OrderUpdate) {
        if let Some(idx) = self.pending_entry_orders.iter().position(|p| p.tag == update.tag) {
            self.handle_entry_order_update(idx, update);
            return;
        }
        if let Some(idx) = self.pending_exit_orders.iter().position(|p| p.tag == update.tag) {
            self.handle_exit_order_update(idx, update);
            return;
        }
        // No pending order matched this tag. A COMPLETE on an EXIT tag here means the pending
        // record was already removed (e.g. a duplicate CANCELLED for the same per-position tag)
        // before the fallback order's fill arrived — the position is flat at the broker. Close
        // it by pos_id so the engine never resubmits a phantom (naked) exit.
        self.reconcile_orphaned_exit_fill(&update);
    }

    /// Parse the position id out of an EXIT order tag (`{prefix}EXIT{pos_id}`).
    fn pos_id_from_exit_tag(&self, tag: &str) -> Option<u64> {
        let prefix = format!("{}EXIT", self.order_tag_prefix);
        tag.strip_prefix(&prefix)?.parse().ok()
    }

    /// Close a position whose exit order filled but whose pending record was already gone.
    fn reconcile_orphaned_exit_fill(&mut self, update: &OrderUpdate) {
        let status = update.status.as_deref().unwrap_or_default().to_ascii_uppercase();
        if status != "COMPLETE" || update.filled_quantity.unwrap_or(0) == 0 {
            return;
        }
        let Some(pos_id) = self.pos_id_from_exit_tag(&update.tag) else { return; };
        let Some(pos_idx) = self.positions.iter().position(|p| p.id == pos_id && p.is_open) else {
            return;
        };
        let avg_price = update
            .average_price
            .filter(|&p| p > 0.0)
            .unwrap_or_else(|| self.positions[pos_idx].entry_price);
        warn!(
            "Reconciled orphaned exit fill tag={} pos_id={} avg=₹{:.2} — closing position (pending record was gone)",
            update.tag, pos_id, avg_price
        );
        self.finalize_position_close(pos_idx, avg_price, "EXIT (reconciled orphan fill)".to_string());
    }

    fn handle_entry_order_update(&mut self, idx: usize, update: OrderUpdate) {
        if let Some(order_id) = update.order_id.clone() {
            self.pending_entry_orders[idx].order_id = Some(order_id);
        }

        if let Some(msg) = update.message.as_deref() {
            match update.source.as_str() {
                "place_error" => {
                    warn!("Entry order {} failed to place: {}", update.tag, msg);
                    self.release_pending_entry(idx, true, "BUY place failed");
                    return;
                }
                "cancel_error" => {
                    // Cancel API call failed transiently. poll_pending_orders() will
                    // retry via last_cancel_attempt_ms backoff — no action needed here.
                    warn!(
                        "Entry order {} cancel failed (will retry): {}",
                        update.tag, msg
                    );
                    return;
                }
                "status_error" => {
                    warn!("Entry order {} status lookup issue: {}", update.tag, msg);
                }
                _ => {}
            }
        }

        let status = update.status.unwrap_or_default();
        if status.is_empty() {
            return;
        }
        let status_upper = status.to_ascii_uppercase();
        let is_complete = status_upper == "COMPLETE";
        let is_terminal_reject = status_upper == "REJECTED"
            || status_upper == "CANCELLED"
            || status_upper == "CANCELED"
            || status_upper == "EXPIRED";

        if is_complete {
            let avg_price = update.average_price.unwrap_or(0.0);
            // Split policy: whether to keep or immediately exit a fill that arrived
            // while a cancel was in-flight depends on WHY the cancel was triggered.
            //
            // Timeout          → keep. Price finally came to our level; capital was
            //                    reserved the whole time; no double-exposure; engine
            //                    state is valid. Entering is fine.
            //
            // PriceReversal    → flatten. LTP had already dropped >15% — the option
            // ScanDirectionFlip  was losing value when the cancel fired. By the time
            // ScanScoreCollapse  the fill arrives the engine may be looking the other
            //                    way. Immediately exit to avoid trading against the
            //                    engine's current view.
            let should_flatten = matches!(
                self.pending_entry_orders[idx].cancel_reason,
                Some(CancelReasonKind::PriceReversal)
                    | Some(CancelReasonKind::ScanDirectionFlip)
                    | Some(CancelReasonKind::ScanScoreCollapse)
            );
            if should_flatten {
                warn!(
                    "Entry fill arrived after direction-based cancel [{}] tag={} avg=₹{:.2} — flattening immediately",
                    self.pending_entry_orders[idx].cancel_reason.map(|r| r.as_str()).unwrap_or("?"),
                    update.tag, avg_price
                );
                self.flatten_stale_filled_entry(idx, avg_price);
            } else {
                self.promote_filled_entry(idx, avg_price);
            }
        } else if is_terminal_reject {
            let filled_qty = update.filled_quantity.unwrap_or(0);
            let avg_price  = update.average_price.unwrap_or(0.0);

            if filled_qty > 0 {
                // Partial fill before cancel: we own `filled_qty` lots at the broker
                // but the engine has no Position for them.  Place an immediate MARKET
                // SELL for exactly the filled quantity and track it so failures surface.
                let pending = &self.pending_entry_orders[idx];
                let exit_tag = self.build_order_tag("EXIT", pending.pos_id);
                let tradingsymbol = pending.tradingsymbol.clone();
                let pos_id = pending.pos_id;
                error!(
                    "Partial fill on cancelled entry: tag={} filled_qty={} avg=₹{:.2} — placing emergency SELL {}",
                    update.tag, filled_qty, avg_price, exit_tag
                );
                let now_ms = self.current_time_ms();
                self.send_order_command(OrderCommand::Place(PlaceOrderCmd {
                    tag: exit_tag.clone(),
                    tradingsymbol,
                    quantity: filled_qty,
                    side: OrderSide::Sell,
                    limit_price: None,
                }));
                self.pending_exit_orders.push(PendingExitOrder {
                    pos_id,
                    tag: exit_tag.clone(),
                    tradingsymbol: pending.tradingsymbol.clone(),
                    total_quantity: filled_qty,
                    placed_ms: now_ms,
                    last_status_poll_ms: 0,
                    reason: "PARTIAL FILL EXIT".to_string(),
                    cancel_requested: false,
                    last_cancel_attempt_ms: 0,
                    market_fallback_sent: false,
                    fallback_book_price: avg_price.max(pending.signal_price),
                    total_filled_quantity: 0,
                    total_filled_notional: 0.0,
                    current_order_filled_quantity: 0,
                    current_order_filled_notional: 0.0,
                });
                self.send_order_command(OrderCommand::StatusByTag { tag: exit_tag });
            }

            let cancel_reason_str = self.pending_entry_orders[idx]
                .cancel_reason
                .map(|r| r.as_str())
                .unwrap_or("broker-initiated");
            let reason = format!(
                "Entry order {} — cancel reason: {}{}",
                status_upper,
                cancel_reason_str,
                if filled_qty > 0 { " (partial fill — emergency exit placed)" } else { "" }
            );
            self.release_pending_entry(idx, true, &reason);
        }
    }

    fn handle_exit_order_update(&mut self, idx: usize, update: OrderUpdate) {
        if let Some(pending) = self.pending_exit_orders.get_mut(idx) {
            pending.record_fill_progress(&update);
        }

        if let Some(msg) = update.message.as_deref() {
            match update.source.as_str() {
                "place_error" => {
                    let pending = self.pending_exit_orders.remove(idx);
                    let is_untracked = Self::is_untracked_exit_reason(&pending.reason);
                    if is_untracked {
                        error!(
                            "[{}] SELL failed to place tag={} err={} — broker position may still be OPEN. MANUAL INTERVENTION REQUIRED.",
                            pending.reason, pending.tag, msg
                        );
                    } else {
                        self.handle_failed_exit(pending, "PLACE_ERROR", Some(msg));
                    }
                    return;
                }
                "cancel_error" => {
                    warn!("Exit order {} cancel failed (will retry): {}", update.tag, msg);
                    return;
                }
                "status_error" => {
                    warn!("Exit order {} status lookup issue: {}", update.tag, msg);
                }
                _ => {}
            }
        }
        let status = update.status.unwrap_or_default();
        if status.is_empty() { return; }
        let status_upper = status.to_ascii_uppercase();
        let is_complete = status_upper == "COMPLETE";
        let is_terminal_reject = status_upper == "REJECTED" || status_upper == "CANCELLED" || status_upper == "CANCELED" || status_upper == "EXPIRED";

        if is_complete {
            let Some(pending) = self.pending_exit_orders.get(idx).cloned() else {
                return;
            };
            let Some(avg_price) = pending.best_effort_fill_price(update.average_price) else {
                error!(
                    "Exit order COMPLETE but avg_price unavailable for tag={} pos_id={} — keeping exit pending for another status poll",
                    pending.tag, pending.pos_id
                );
                if let Some(pending) = self.pending_exit_orders.get_mut(idx) {
                    pending.last_status_poll_ms = 0;
                }
                return;
            };
            let pending = self.pending_exit_orders.remove(idx);
            self.finish_completed_exit(pending, avg_price);
        } else if is_terminal_reject {
            let stale_cancel_after_market_fallback = self
                .pending_exit_orders
                .get(idx)
                .map(|p| {
                    p.market_fallback_sent
                        && (status_upper == "CANCELLED" || status_upper == "CANCELED")
                })
                .unwrap_or(false);
            if stale_cancel_after_market_fallback {
                if let Some(pending) = self.pending_exit_orders.get_mut(idx) {
                    pending.last_status_poll_ms = 0;
                    warn!(
                        "Exit order {} got {} after MARKET fallback was already sent — treating as stale pre-fallback cancel and keeping exit pending",
                        pending.tag, status_upper
                    );
                }
                return;
            }

            let needs_market_fallback = self
                .pending_exit_orders
                .get(idx)
                .map(|p| p.cancel_requested && !p.market_fallback_sent)
                .unwrap_or(false);
            if needs_market_fallback {
                self.launch_exit_market_fallback(idx, &status_upper);
                return;
            }

            let pending = self.pending_exit_orders.remove(idx);
            self.handle_failed_exit(pending, &status_upper, update.message.as_deref());
        }
    }

    fn poll_pending_orders(&mut self) {
        if !self.live_mode_enabled() || (self.pending_entry_orders.is_empty() && self.pending_exit_orders.is_empty()) {
            return;
        }
        // Give a limit exit a brief chance to cross naturally, then cancel it and
        // fall back to a market sell so positions cannot remain exit_pending forever.
        const EXIT_LIMIT_TIMEOUT_MS: u64 = 15_000;
        const CANCEL_RETRY_INTERVAL_MS: u64 = 15_000;
        let now = self.current_time_ms();
        let mut status_tags: Vec<String> = Vec::new();
        let mut cancel_tags: Vec<String> = Vec::new();
        let reversal_pct = self.limit_cancel_reversal_pct;

        for pending in self.pending_entry_orders.iter_mut() {
            // ── Retry path: cancel was already sent but not yet confirmed ──────────────
            // Capital stays reserved until broker returns CANCELLED/REJECTED (Issue 1 fix).
            // Re-send CancelByTag every CANCEL_RETRY_INTERVAL_MS to handle transient
            // failures (Issue 2 fix).
            if pending.cancel_requested {
                if now.saturating_sub(pending.last_cancel_attempt_ms) >= CANCEL_RETRY_INTERVAL_MS {
                    pending.last_cancel_attempt_ms = now;
                    cancel_tags.push(pending.tag.clone());
                    status_tags.push(pending.tag.clone());
                    warn!(
                        "Cancel retry [{}] tag={} — awaiting broker confirmation",
                        pending.cancel_reason.map(|r| r.as_str()).unwrap_or("unknown"),
                        pending.tag
                    );
                }
                continue; // don't apply new guards to already-cancelled orders
            }

            let age = now.saturating_sub(pending.placed_ms);

            // 1. Hard timeout — cancel after configured window.
            if age >= self.entry_order_timeout_ms {
                pending.cancel_requested = true;
                pending.cancel_reason = Some(CancelReasonKind::Timeout);
                pending.last_cancel_attempt_ms = now;
                cancel_tags.push(pending.tag.clone());
                status_tags.push(pending.tag.clone());
                warn!(
                    "Entry order [timeout {}s] tag={} — cancelling unfilled limit order; capital held until broker confirms",
                    self.entry_order_timeout_ms / 1000, pending.tag
                );
                continue;
            }

            // 2. Price-reversal guard — option LTP dropped >reversal_pct below limit.
            if let Some(tick) = self.store.get(pending.token) {
                if tick.ltp > 0.01 {
                    let threshold = pending.signal_price * (1.0 - reversal_pct);
                    if tick.ltp < threshold {
                        pending.cancel_requested = true;
                        pending.cancel_reason = Some(CancelReasonKind::PriceReversal);
                        pending.last_cancel_attempt_ms = now;
                        cancel_tags.push(pending.tag.clone());
                        status_tags.push(pending.tag.clone());
                        warn!(
                            "Entry order [price reversal] tag={} — LTP ₹{:.2} is {:.1}% below limit ₹{:.2}; capital held until broker confirms",
                            pending.tag,
                            tick.ltp,
                            (1.0 - tick.ltp / pending.signal_price) * 100.0,
                            pending.signal_price
                        );
                        continue;
                    }
                }
            }

            // 3. Regular status poll.
            if now.saturating_sub(pending.last_status_poll_ms) >= self.order_status_poll_interval_ms {
                pending.last_status_poll_ms = now;
                status_tags.push(pending.tag.clone());
            }
        }

        for pending in self.pending_exit_orders.iter_mut() {
            if pending.cancel_requested {
                if now.saturating_sub(pending.last_cancel_attempt_ms) >= CANCEL_RETRY_INTERVAL_MS {
                    pending.last_cancel_attempt_ms = now;
                    cancel_tags.push(pending.tag.clone());
                    status_tags.push(pending.tag.clone());
                    warn!(
                        "Exit cancel retry [{}] tag={} — awaiting broker confirmation",
                        pending.reason,
                        pending.tag
                    );
                }
                continue;
            }

            if !pending.market_fallback_sent
                && now.saturating_sub(pending.placed_ms) >= EXIT_LIMIT_TIMEOUT_MS
            {
                pending.cancel_requested = true;
                pending.last_cancel_attempt_ms = now;
                cancel_tags.push(pending.tag.clone());
                status_tags.push(pending.tag.clone());
                warn!(
                    "Exit order [timeout {}s] tag={} — cancelling stale limit sell before MARKET fallback",
                    EXIT_LIMIT_TIMEOUT_MS / 1000,
                    pending.tag
                );
                continue;
            }

            if now.saturating_sub(pending.last_status_poll_ms) >= self.order_status_poll_interval_ms {
                pending.last_status_poll_ms = now;
                status_tags.push(pending.tag.clone());
            }
        }

        for tag in cancel_tags {
            self.send_order_command(OrderCommand::CancelByTag { tag });
        }
        for tag in status_tags {
            self.send_order_command(OrderCommand::StatusByTag { tag });
        }
    }

    fn release_pending_entry(&mut self, idx: usize, count_as_rejection: bool, reason: &str) {
        if idx >= self.pending_entry_orders.len() {
            return;
        }
        let pending = self.pending_entry_orders.remove(idx);
        if !pending.released_after_timeout {
            self.pending_capital_reserved =
                (self.pending_capital_reserved - pending.estimated_capital).max(0.0);
        }
        if count_as_rejection {
            self.entry_rejections += 1;
        }
        warn!("Pending entry released [{}] tag={}", reason, pending.tag);
        // This live entry never became a position. If options now holds no open
        // position and no other live entry in flight, free the global position slot
        // so the next signal (any engine) can claim it.
        if let Some(c) = &self.shared_circuit {
            if !self.has_live_commitment() {
                crate::portfolio::release(c, "options");
            }
        }
    }

    fn promote_filled_entry(&mut self, idx: usize, broker_avg_price: f64) -> bool {
        if idx >= self.pending_entry_orders.len() {
            return false;
        }
        let pending = self.pending_entry_orders.remove(idx);
        if !pending.released_after_timeout {
            self.pending_capital_reserved =
                (self.pending_capital_reserved - pending.estimated_capital).max(0.0);
        }
        // A live entry just became a real position. Ensure options holds the global
        // slot — normally it claimed at submit (this is a no-op), but if the slot was
        // freed by a timeout-release and the order then filled late, re-grab it so the
        // lock reflects the open live position.
        if let Some(c) = &self.shared_circuit {
            let _ = crate::portfolio::try_claim(c, "options");
        }
        self.open_filled_position_from_pending(pending, broker_avg_price)
    }

    fn is_untracked_exit_reason(reason: &str) -> bool {
        reason == "EMERGENCY FLATTEN" || reason == "PARTIAL FILL EXIT"
    }

    fn finish_completed_exit(&mut self, pending: PendingExitOrder, avg_price: f64) {
        if Self::is_untracked_exit_reason(&pending.reason) {
            warn!(
                "[{}] SELL complete tag={} avg=₹{:.2} — broker position closed",
                pending.reason, pending.tag, avg_price
            );
            if let Some(c) = &self.shared_circuit {
                if !self.has_live_commitment() {
                    crate::portfolio::release(c, "options");
                }
            }
        } else if let Some(pos_idx) = self
            .positions
            .iter()
            .position(|p| p.id == pending.pos_id)
        {
            self.finalize_position_close(pos_idx, avg_price, pending.reason);
        }
    }

    fn handle_failed_exit(&mut self, pending: PendingExitOrder, status_upper: &str, reject_msg: Option<&str>) {
        let is_untracked = Self::is_untracked_exit_reason(&pending.reason);

        if pending.total_filled_quantity >= pending.total_quantity && pending.total_quantity > 0 {
            let avg_price = pending.avg_fill_price().unwrap_or(0.0);
            self.finish_completed_exit(pending, avg_price);
            return;
        }

        if is_untracked {
            error!(
                "[{}] SELL {} tag={} filled {}/{} — broker position may still be OPEN. MANUAL INTERVENTION REQUIRED.",
                pending.reason,
                status_upper,
                pending.tag,
                pending.total_filled_quantity,
                pending.total_quantity
            );
            return;
        }

        if pending.total_filled_quantity > 0 {
            // Partial fill then terminal: leave exit_pending so we never oversell; needs a human.
            if let Some(pos) = self.positions.iter_mut().find(|p| p.id == pending.pos_id) {
                error!(
                    "Exit order {} terminal status {} after partial fill {}/{} — manual intervention required; engine position left exit_pending to avoid oversell.",
                    pending.tag,
                    status_upper,
                    pending.total_filled_quantity,
                    pending.total_quantity
                );
                pos.exit_pending = true;
            }
            return;
        }

        // Zero-fill terminal reject. A funds/margin reject on a CLOSE is definitional proof the
        // broker holds no inventory, so a retry can only ever fire a naked short — never retry it.
        // Any other reject is bounded by MAX_EXIT_ATTEMPTS so a persistent failure can't run away
        // (this is what turned one orphaned fill into 498 rejected resubmits on 2026-06-30).
        const MAX_EXIT_ATTEMPTS: u32 = 3;
        let funds_reject = reject_msg
            .map(|m| {
                let m = m.to_ascii_lowercase();
                m.contains("insufficient funds") || m.contains("margin") || m.contains("rms:")
            })
            .unwrap_or(false);
        let attempts = {
            let c = self.exit_failures.entry(pending.pos_id).or_insert(0);
            *c += 1;
            *c
        };
        if let Some(pos) = self.positions.iter_mut().find(|p| p.id == pending.pos_id) {
            if funds_reject || attempts >= MAX_EXIT_ATTEMPTS {
                error!(
                    "Exit order {} {} — {} on a CLOSE: broker shows no open inventory. Halting exit resubmit after {} attempt(s); position {} left exit_pending. MANUAL INTERVENTION REQUIRED (reconcile on the broker).",
                    pending.tag,
                    status_upper,
                    if funds_reject { "funds/margin reject" } else { "retry cap reached" },
                    attempts,
                    pending.pos_id
                );
                pos.exit_pending = true; // blocks close_position_at (guard: !is_open || exit_pending)
            } else {
                warn!(
                    "Exit order {} terminal status {} (attempt {}/{}). Will re-attempt exit later.",
                    pending.tag, status_upper, attempts, MAX_EXIT_ATTEMPTS
                );
                pos.exit_pending = false;
            }
        }
    }

    fn launch_exit_market_fallback(&mut self, idx: usize, terminal_status: &str) {
        let Some(snapshot) = self.pending_exit_orders.get(idx).cloned() else {
            return;
        };
        let remaining_qty = snapshot.remaining_quantity();
        if remaining_qty == 0 {
            let pending = self.pending_exit_orders.remove(idx);
            let avg_price = pending.avg_fill_price().unwrap_or(0.0);
            self.finish_completed_exit(pending, avg_price);
            return;
        }

        // Use current LTP as a bookkeeping reference for the broker MARKET fallback.
        let fallback_price = self
            .positions
            .iter()
            .find(|p| p.id == snapshot.pos_id)
            .and_then(|pos| self.store.get(pos.option_token))
            .map(|tick| tick.ltp)
            .filter(|&ltp| ltp > 0.0)
            .or_else(|| {
                // Untracked exits (EMERGENCY FLATTEN etc) have no matching Position;
                // use the original entry price as a conservative floor.
                self.positions
                    .iter()
                    .find(|p| p.id == snapshot.pos_id)
                    .map(|p| p.entry_price)
            })
            .unwrap_or(0.05); // absolute floor: tick size minimum

        warn!(
            "Exit order {} timed out as {} after filling {}/{} — sending MARKET fallback (reference ₹{:.2}) for remaining {}",
            snapshot.tag,
            terminal_status,
            snapshot.total_filled_quantity,
            snapshot.total_quantity,
            fallback_price,
            remaining_qty
        );

        let now_ms = self.current_time_ms();
        if let Some(pending) = self.pending_exit_orders.get_mut(idx) {
            pending.prepare_market_fallback(now_ms, fallback_price);
        }

        self.send_order_command(OrderCommand::Place(PlaceOrderCmd {
            tag: snapshot.tag.clone(),
            tradingsymbol: snapshot.tradingsymbol,
            quantity: remaining_qty,
            side: OrderSide::Sell,
            limit_price: None,
        }));
        self.send_order_command(OrderCommand::StatusByTag { tag: snapshot.tag });
    }

    fn flatten_stale_filled_entry(&mut self, idx: usize, broker_avg_price: f64) {
        if idx >= self.pending_entry_orders.len() {
            return;
        }
        warn!("Stale entry filled late | avg={:.2} -> opening Position and initiating tracking exit", broker_avg_price);
        if self.promote_filled_entry(idx, broker_avg_price) {
            let new_idx = self.positions.len().saturating_sub(1);
            self.close_position_at(new_idx, broker_avg_price, "STALE FLATTEN".to_string());
            // A stale flatten is a bookkeeping close, not a real trade execution.
            // Do not consume a daily trade slot for it.
            self.daily_trades_opened = self.daily_trades_opened.saturating_sub(1);
        }
    }

    pub fn diagnostics(&self) -> (u64, u64, u64, u64, u64, usize) {
        (
            self.signals_generated,
            self.entry_attempts,
            self.entry_rejections,
            self.positions_opened,
            self.positions_closed,
            self.positions.iter().filter(|p| p.is_open).count(),
        )
    }

    pub fn spawn(
        mut self,
        mut rx: broadcast::Receiver<TickEvent>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
            info!("  OPTIONS SIGNAL ENGINE — ACTIVE");
            info!("  Capital: ₹{:.0} | Target: ₹50,000", self.risk.capital);
            info!("  Profit target: {:.0}% | Stop: {:.0}% | Min confidence: {:.0} | Expiry gamma floor: {:.0}",
                self.profit_target_pct, self.stop_loss_pct, self.min_confidence, self.expiry_day_min_confidence);
            info!("  Scan interval: {}s", self.scan_interval_secs);
            info!("  Max daily trades: {}", if self.max_daily_trades == u32::MAX { "unlimited (P&L circuits only)".to_string() } else { self.max_daily_trades.to_string() });
            info!("  Strategies: GammaScalp | IVExpansion | TrendFollow | MaxPain | OIDivergence | GEX");
            info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

            let mut scan_timer = tokio::time::interval(
                tokio::time::Duration::from_secs(self.scan_interval_secs)
            );

            // Capital refresh: every hour + after every trade close.
            // First tick fires immediately — skip it so we don't double-fetch at startup.
            let mut capital_timer = {
                let mut t = tokio::time::interval(tokio::time::Duration::from_secs(3600));
                t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                t
            };
            capital_timer.tick().await; // consume the immediate first tick

            loop {
                tokio::select! {
                    tick_result = rx.recv() => {
                        match tick_result {
                            Ok(_event) => {
                                self.clear_clock_override();
                                self.process_order_updates();
                                self.poll_pending_orders();
                                self.check_open_positions();
                            }
                            Err(broadcast::error::RecvError::Lagged(n)) => {
                                warn!("Options engine lagged by {} messages", n);
                            }
                            Err(broadcast::error::RecvError::Closed) => {
                                info!("Options engine: channel closed — writing session summary");
                                let open_count = self.positions.iter().filter(|p| p.is_open).count();
                                self.journal.write_session_summary(open_count);
                                break;
                            }
                        }
                    }
                    _ = scan_timer.tick() => {
                        let session = self.current_session();
                        if matches!(session, SessionPhase::PreOpen | SessionPhase::AfterMarket) {
                            continue;
                        }

                        // Sync capital after a trade closed in a previous tick cycle.
                        if self.capital_sync_needed {
                            self.sync_capital_from_kite().await;
                        }

                        self.scan_count += 1;
                        self.process_order_updates();
                        self.poll_pending_orders();
                        self.run_full_scan();
                    }
                    _ = capital_timer.tick() => {
                        self.sync_capital_from_kite().await;
                    }
                }
            }
        })
    }

    /// Release the global one-position slot iff options holds NO commitment (no open
    /// position, no in-flight entry order, and no untracked/fallback exit order).
    /// State-based safety net mirroring micro's
    /// per-tick `release_lock_if_idle`: if a live entry order's terminal broker update
    /// never arrives, the event-driven releases can't fire, so this frees the slot at
    /// the next scan rather than wedging trading for the day. Safe because it is
    /// STATE-based, never time-based — it can never evict a legitimately open position.
    /// `release` is a no-op unless options is the holder.
    fn release_lock_if_idle(&mut self) {
        if let Some(c) = &self.shared_circuit {
            if !self.has_live_commitment() {
                crate::portfolio::release(c, "options");
            }
        }
    }

    fn has_live_commitment(&self) -> bool {
        self.positions.iter().any(|p| p.is_open)
            || self
                .pending_entry_orders
                .iter()
                .any(|p| !p.released_after_timeout)
            || !self.pending_exit_orders.is_empty()
    }

    fn run_full_scan(&mut self) {
        self.release_lock_if_idle();
        self.execute_pending_signals();

        self.reset_daily_state_if_needed();
        let session = self.current_session();
        let mut underlyings: Vec<String> = self.underlying_tokens.keys().cloned().collect();
        if underlyings.is_empty() {
            let mut seen = std::collections::HashSet::new();
            for c in &self.contracts {
                if seen.insert(c.underlying.clone()) {
                    underlyings.push(c.underlying.clone());
                }
            }
        }

        let mut snapshots: Vec<ChainSnapshot> = Vec::new();
        for underlying in &underlyings {
            let snap = match self.build_chain_snapshot(underlying) {
                Some(s) => s,
                None => {
                    warn!("Options engine: cannot build snapshot for {}", underlying);
                    continue;
                }
            };
            if snap.atm_iv > 0.01 {
                self.iv_history.push(&snap.underlying.clone(), snap.timestamp_ms, snap.atm_iv);
            }
            if self.scan_count % 5 == 1 {
                self.log_chain_analysis(&snap);
            }
            snapshots.push(snap);
        }

        // Halt on EITHER the engine's own circuit OR the account-level shared circuit.
        let account_halt = self
            .shared_circuit
            .as_ref()
            .map_or(false, |c| !crate::portfolio::can_enter_holder(c, "options"));
        let circuit_broken = self.risk.circuit_breaker_triggered() || account_halt;
        if circuit_broken {
            let reason = if account_halt {
                "ACCOUNT circuit (-15%/+25% combined)"
            } else {
                self.risk.circuit_reason().unwrap_or("daily limit reached")
            };
            warn!("⛔ CIRCUIT BREAKER: {}. NO NEW TRADES today.", reason);
        }

        // Hard entry cutoff: no new position entries after 15:00 IST.
        // Afternoon phase runs until 15:14, so we must check IST time explicitly.
        let ist_mins = {
            let now_utc = Utc.timestamp_millis_opt(self.current_time_ms() as i64)
                .single()
                .unwrap_or_else(Utc::now);
            let ist = FixedOffset::east_opt(5 * 3600 + 30 * 60).expect("valid IST offset");
            let now_ist = now_utc.with_timezone(&ist);
            now_ist.hour() * 60 + now_ist.minute()
        };
        let past_entry_cutoff = ist_mins >= 900; // 15:00 IST = 900 mins from midnight
        if past_entry_cutoff && !matches!(session, SessionPhase::Closing | SessionPhase::AfterMarket) {
            warn!("Entry cutoff: no new signals after 15:00 IST (current IST {:02}:{:02})",
                ist_mins / 60, ist_mins % 60);
        }

        let mut pending_batch: Vec<(Signal, usize)> = Vec::new();

        let now_ms = self.current_time_ms();
        if !circuit_broken && !past_entry_cutoff && !matches!(session, SessionPhase::OpeningBell | SessionPhase::Closing) {
            for (snap_idx, snap) in snapshots.iter().enumerate() {
                if past_entry_cutoff_ist(ist_mins, snap.days_to_expiry) {
                    let cutoff = entry_cutoff_ist_mins_for_dte(snap.days_to_expiry);
                    warn!(
                        "Entry cutoff: no new {} signals after {:02}:{:02} IST for {:.1} DTE (current {:02}:{:02})",
                        snap.underlying,
                        cutoff / 60,
                        cutoff % 60,
                        snap.days_to_expiry,
                        ist_mins / 60,
                        ist_mins % 60
                    );
                    self.last_scan_result.insert(snap.underlying.clone(), LastScanResult {
                        action: SignalAction::Hold,
                        best_score: 0.0,
                        scanned_at_ms: now_ms,
                    });
                    continue;
                }

                let signals = self.generate_signals(snap);
                let best_score = signals.iter().map(|s| s.confidence).fold(0.0_f64, f64::max);
                // Dominant action: take from the highest-scoring signal (all signals from one
                // scan share the same dominant direction; Hold means no signal fired).
                let dominant_action = signals.iter()
                    .max_by(|a, b| a.confidence.partial_cmp(&b.confidence).unwrap_or(std::cmp::Ordering::Equal))
                    .map(|s| s.action)
                    .unwrap_or(SignalAction::Hold);
                let regular_floor = self.min_confidence;
                let admission_threshold_for = |signal: &Signal| {
                    let base = regular_floor;
                    if matches!(session, SessionPhase::Midday)
                        && !(signal.strategy == StrategyType::GammaScalp && snap.days_to_expiry < 0.5)
                    {
                        base.max(70.0)
                    } else {
                        base
                    }
                };
                self.last_scan_result.insert(snap.underlying.clone(), LastScanResult {
                    action: dominant_action,
                    best_score,
                    scanned_at_ms: now_ms,
                });
                let best = best_score as i32;
                let signals_above = signals.iter()
                    .filter(|s| s.confidence >= admission_threshold_for(s))
                    .count();
                warn!("Scan #{} {} {:?} spot={:.0} atm_iv={:.1}% iv_rank={:.0} pcr={:.2} best_score={} threshold={:.0} signals_above={}",
                    self.scan_count, snap.underlying, session, snap.spot,
                    snap.atm_iv * 100.0, snap.iv_rank, snap.pcr_oi,
                    best, self.min_confidence,
                    signals_above);
                for signal in signals {
                    if signal.confidence >= admission_threshold_for(&signal) {
                        // Morning session on expiry day: require multi-strategy confluence.
                        // Single-strategy Gamma Scalp alone in the morning chop loses to
                        // whipsaws. This doesn't block afternoon signals where GEX+OI+PCR align.
                        if matches!(session, SessionPhase::Morning)
                            && snap.days_to_expiry < 0.5
                            && signal.strategy == StrategyType::GammaScalp
                            && signal.confidence < 70.0
                        {
                            warn!("  {} Morning 0-DTE single-strategy gate: score {:.0} < 70 required for morning expiry",
                                snap.underlying, signal.confidence);
                            continue;
                        }
                        // 0-DTE Morning CE: block when IV Rank is at session floor.
                        // Belt-and-suspenders guard complementing Gate C in generate_signals.
                        // Even if a high-confidence composite signal bypasses Gate C (e.g. a
                        // future code path change), this admission gate ensures no CE trade
                        // enters on expiry morning when IV has no room to expand.
                        if matches!(session, SessionPhase::Morning)
                            && snap.days_to_expiry < 0.5
                            && signal.action == SignalAction::BuyCE
                            && snap.iv_rank < 5.0
                        {
                            warn!("  {} Morning 0-DTE CE gate: IV Rank {:.1}% at session floor — no IV edge",
                                snap.underlying, snap.iv_rank);
                            continue;
                        }
                        pending_batch.push((signal, snap_idx));
                    }
                }
            }
        }

        // Cross-underlying confirmation: if ≥2 underlyings signal the same direction,
        // boost all matching signals. Works for any combination (NIFTY+NIFTYNXT50).
        let ce_underlyings: std::collections::HashSet<&str> = pending_batch.iter()
            .filter(|(s, _)| s.action == SignalAction::BuyCE)
            .map(|(s, _)| s.underlying.as_str())
            .collect();
        let pe_underlyings: std::collections::HashSet<&str> = pending_batch.iter()
            .filter(|(s, _)| s.action == SignalAction::BuyPE)
            .map(|(s, _)| s.underlying.as_str())
            .collect();
        let cross_ce = ce_underlyings.len() >= 2;
        let cross_pe = pe_underlyings.len() >= 2;

        for (signal, snap_idx) in &mut pending_batch {
            let boosted = (cross_ce && signal.action == SignalAction::BuyCE)
                || (cross_pe && signal.action == SignalAction::BuyPE);
            if boosted {
                let boost = 8.0_f64;
                signal.confidence = (signal.confidence + boost).min(100.0);
                signal.reasons.insert(0, (
                    format!("Cross-underlying confirmation: ≥2 indices signal same direction"),
                    boost,
                ));
                info!("  Cross-confirm: {} {} boosted to {:.0} confidence",
                    signal.underlying, signal.action, signal.confidence);
            }
            if let Some(snap) = snapshots.get(*snap_idx) {
                self.log_signal(signal, snap);
            }
        }

        let in_warmup = self.warmup_until_ms
            .map_or(false, |wu| self.current_time_ms() < wu);
        if in_warmup {
            info!("Warmup active: {} signal(s) suppressed (clock {} ms)", pending_batch.len(), self.current_time_ms());
        } else {
            let mut immediate_expiry_gamma = false;
            for (signal, _) in pending_batch {
                if signal.strategy == StrategyType::GammaScalp
                    && signal.entry_ctx.days_to_expiry < 0.5
                {
                    immediate_expiry_gamma = true;
                }
                self.pending_signals.push_back(signal);
            }
            if immediate_expiry_gamma {
                self.execute_pending_signals();
            }
        }

        // After every scan, re-validate any pending limit entry orders against the
        // freshly computed scan results.
        self.validate_pending_entries_against_scan();

        if self.scan_count % 10 == 0 {
            self.log_portfolio_summary();
        }
    }

    /// After each scan cycle, check every pending (unfilled) limit entry order to see
    /// if the engine's view of the market has changed enough to warrant cancellation.
    ///
    /// Two cancellation triggers:
    ///   1. Direction flip  — new scan's dominant action is opposite to the pending order.
    ///   2. Score collapse  — best composite score fell below 40, meaning all strategy
    ///      signals have gone quiet (market has gone flat/undecided).
    ///
    /// A marginal score drop (e.g. 65→55) is intentionally ignored; only a definitive
    /// reversal or complete signal collapse justifies pulling an already-queued order.
    fn validate_pending_entries_against_scan(&mut self) {
        if !self.live_mode_enabled() || self.pending_entry_orders.is_empty() {
            return;
        }
        const SCORE_COLLAPSE_THRESHOLD: f64 = 40.0;
        let now = self.current_time_ms();
        let mut cancel_tags: Vec<(String, CancelReasonKind, String)> = Vec::new();

        for pending in self.pending_entry_orders.iter_mut() {
            if pending.cancel_requested {
                continue;
            }
            // Once the order has been placed at the broker (order_id is set), scan-based
            // validation is moot. MARKET orders fill in <1ms; LIMIT orders that are already
            // live at the exchange are handled by poll_pending_orders (timeout / price-reversal).
            // Marking a just-placed order here causes legitimate fills to be flattened.
            if pending.order_id.is_some() {
                continue;
            }
            // Don't apply scan-based cancellation to orders placed in the current scan cycle.
            // execute_pending_signals() runs at the START of run_full_scan() — the Place command
            // hasn't even reached the broker yet when validate runs at the END of the same call.
            // Waiting until the next scan (scan_count > placed_at_scan_count) gives the broker
            // time to confirm the fill and set order_id, at which point the guard above applies.
            if pending.placed_at_scan_count >= self.scan_count {
                continue;
            }
            let result = match self.last_scan_result.get(&pending.signal.underlying) {
                Some(r) => r.clone(),
                None => continue,
            };

            let direction_flipped = matches!(
                (pending.signal.action, result.action),
                (SignalAction::BuyCE, SignalAction::BuyPE) | (SignalAction::BuyPE, SignalAction::BuyCE)
            );
            let score_collapsed = result.best_score < SCORE_COLLAPSE_THRESHOLD;

            if direction_flipped {
                pending.cancel_requested = true;
                pending.cancel_reason = Some(CancelReasonKind::ScanDirectionFlip);
                pending.last_cancel_attempt_ms = now;
                cancel_tags.push((
                    pending.tag.clone(),
                    CancelReasonKind::ScanDirectionFlip,
                    format!("pending={} new_scan={:?} score={:.0}",
                        pending.signal.action, result.action, result.best_score),
                ));
            } else if score_collapsed {
                pending.cancel_requested = true;
                pending.cancel_reason = Some(CancelReasonKind::ScanScoreCollapse);
                pending.last_cancel_attempt_ms = now;
                cancel_tags.push((
                    pending.tag.clone(),
                    CancelReasonKind::ScanScoreCollapse,
                    format!("best_score={:.0} (below {SCORE_COLLAPSE_THRESHOLD})", result.best_score),
                ));
            }
        }

        // Capital stays reserved until broker confirms — Issue 1 fix.
        for (tag, kind, detail) in cancel_tags {
            warn!(
                "Entry order [{}] tag={} — {}; capital held until broker confirms",
                kind.as_str(), tag, detail
            );
            self.send_order_command(OrderCommand::CancelByTag { tag: tag.clone() });
            self.send_order_command(OrderCommand::StatusByTag { tag });
        }
    }

    fn execute_pending_signals(&mut self) {
        if self.pending_signals.is_empty() { return; }

        let account_halt = self
            .shared_circuit
            .as_ref()
            .map_or(false, |c| !crate::portfolio::can_enter_holder(c, "options"));
        if self.risk.circuit_breaker_triggered() || account_halt {
            let n = self.pending_signals.len();
            if n > 0 {
                warn!("Circuit breaker active: discarding {} pending signal(s)", n);
                self.pending_signals.clear();
            }
            return;
        }

        // Global one-position lock: if any engine already holds the single open slot,
        // a fresh options signal is CANCELLED (one trade at a time across the account).
        // This is a cheap pre-filter; the authoritative grab is the atomic try_claim at
        // the open-commit in open_simulated_position.
        if let Some(c) = &self.shared_circuit {
            if crate::portfolio::is_locked(c) {
                let n = self.pending_signals.len();
                if n > 0 {
                    warn!("Position open elsewhere: cancelling {} pending options signal(s)", n);
                    self.pending_signals.clear();
                }
                return;
            }
        }

        let session = self.current_session();
        if matches!(session, SessionPhase::Closing | SessionPhase::AfterMarket | SessionPhase::PreOpen) {
            self.pending_signals.clear();
            return;
        }

        // Enforce hard 15:00 entry cutoff: a signal queued at 14:59 would normally
        // execute 30s later at 15:00+. Discard any pending signals once IST >= 15:00.
        let exec_ist_mins = {
            let now_utc = Utc.timestamp_millis_opt(self.current_time_ms() as i64)
                .single()
                .unwrap_or_else(Utc::now);
            let ist = FixedOffset::east_opt(5 * 3600 + 30 * 60).expect("valid IST offset");
            let now_ist = now_utc.with_timezone(&ist);
            now_ist.hour() * 60 + now_ist.minute()
        };
        if exec_ist_mins >= 900 {
            let n = self.pending_signals.len();
            if n > 0 {
                warn!("Entry cutoff: discarding {} queued signal(s) — past 15:00 IST", n);
                self.pending_signals.clear();
            }
            return;
        }

        let max_age_ms = self.scan_interval_secs.saturating_mul(2_000);

        let pending: Vec<Signal> = self.pending_signals.drain(..).collect();
        let mut requeue: Vec<Signal> = Vec::new();
        for mut signal in pending {
            if past_entry_cutoff_ist(exec_ist_mins, signal.entry_ctx.days_to_expiry) {
                let cutoff = entry_cutoff_ist_mins_for_dte(signal.entry_ctx.days_to_expiry);
                warn!(
                    "Pending signal #{} discarded: {:.1} DTE entry cutoff {:02}:{:02} IST reached",
                    signal.id,
                    signal.entry_ctx.days_to_expiry,
                    cutoff / 60,
                    cutoff % 60
                );
                continue;
            }

            let min_delay_ms: u64 = if signal.strategy == StrategyType::GammaScalp
                && signal.entry_ctx.days_to_expiry < 0.5
            {
                5_000
            } else if signal.strategy == StrategyType::SpotReversal || signal.confidence >= 90.0 {
                5_000
            } else {
                30_000
            };
            let age_ms = self.current_time_ms().saturating_sub(signal.timestamp_ms);
            if age_ms < min_delay_ms {
                // Too young — re-queue; enforce per-signal delay, not just oldest.
                requeue.push(signal);
                continue;
            }
            if age_ms > max_age_ms {
                warn!("Pending signal #{}: stale ({:.0}s old), discarding",
                    signal.id, age_ms as f64 / 1000.0);
                continue;
            }

            let token = match signal.action {
                SignalAction::BuyCE => self
                    .contracts
                    .iter()
                    .find(|c| {
                        c.underlying == signal.underlying
                            && c.expiry == signal.expiry
                            && c.strike == signal.strike
                            && c.option_type == OptionType::CE
                    })
                    .map(|c| c.instrument_token)
                    .unwrap_or(0),
                SignalAction::BuyPE => self
                    .contracts
                    .iter()
                    .find(|c| {
                        c.underlying == signal.underlying
                            && c.expiry == signal.expiry
                            && c.strike == signal.strike
                            && c.option_type == OptionType::PE
                    })
                    .map(|c| c.instrument_token)
                    .unwrap_or(0),
                _ => 0,
            };
            if token == 0 {
                warn!("Pending signal #{}: no contract found for {} {:?} strike={:.0}, discarding",
                    signal.id, signal.underlying, signal.action, signal.strike);
                continue;
            }

            let fresh_ltp = match self.store.get(token) {
                Some(t) if t.ltp > 0.01 => t.ltp,
                _ => {
                    warn!("Pending signal #{}: no live price for token {}, discarding", signal.id, token);
                    continue;
                }
            };

            let deviation = ((fresh_ltp - signal.option_price) / signal.option_price.max(0.01)).abs();
            if deviation > 0.25 {
                warn!("Pending signal #{}: price moved {:.1}% (₹{:.2} → ₹{:.2}), discarding stale signal",
                    signal.id, deviation * 100.0, signal.option_price, fresh_ltp);
                continue;
            }

            signal.option_price = fresh_ltp;

            let post_stop_cooldown_ms: u64 = self.progressive_cooldown_ms();
            if self.recent_stop_loss_block(
                &signal.underlying,
                &signal.action,
                post_stop_cooldown_ms,
            ) {
                warn!(
                    "Pending signal #{} blocked: recent STOP HIT cooldown active ({}s) for {} {}",
                    signal.id,
                    post_stop_cooldown_ms / 1000,
                    signal.underlying,
                    signal.action
                );
                continue;
            }

            self.open_simulated_position(&signal);
        }
        for sig in requeue {
            self.pending_signals.push_back(sig);
        }
    }


    fn estimate_synthetic_spot(
        &self,
        strike_map: &BTreeMap<u64, StrikeLevel>,
        t_years: f64,
    ) -> Option<f64> {
        // Correct put-call parity: C - P = S·e^{-qT} - K·e^{-rT}
        // Solving for S (with q≈0 for NSE indices):  S = K·e^{-rT} + C - P
        // The naive K + C - P formula underestimates S by K·(1 - e^{-rT}),
        // which is ~22 pts on NIFTY at 5 DTE and ~130 pts at 30 DTE.
        let discount = (-self.risk_free_rate * t_years).exp();
        let mut implied_spots = Vec::new();
        for level in strike_map.values() {
            if level.ce_ltp > 0.01 && level.pe_ltp > 0.01 {
                let implied = level.strike * discount + level.ce_ltp - level.pe_ltp;
                if implied.is_finite() && implied > 0.0 {
                    implied_spots.push(implied);
                }
            }
        }

        if implied_spots.is_empty() {
            return None;
        }
        implied_spots.sort_by(|a, b| a.partial_cmp(b).unwrap());
        Some(implied_spots[implied_spots.len() / 2])
    }

    fn build_chain_snapshot(&mut self, underlying: &str) -> Option<ChainSnapshot> {
        let ts = self.current_time_ms();

        let mut strike_map: BTreeMap<u64, StrikeLevel> = BTreeMap::new();

        let expiry = self.contracts.iter()
            .filter(|c| c.underlying == underlying)
            .map(|c| c.expiry.clone())
            .min()
            .unwrap_or_default();

        let as_of_utc = Utc
            .timestamp_millis_opt(ts as i64)
            .single()
            .unwrap_or_else(Utc::now);
        let t_years = match compute_time_to_expiry_at(&expiry, as_of_utc) {
            Some(t) if t > 0.0 => t,
            _ => return None,
        };
        let days_to_expiry = t_years * 365.25;

        for contract in &self.contracts {
            if contract.underlying != underlying || contract.expiry != expiry {
                continue;
            }

            let tick = match self.store.get(contract.instrument_token) {
                Some(t) if t.ltp > 0.01 => t,
                _ => continue,
            };

            let strike_key = (contract.strike * 100.0) as u64;
            let level = strike_map.entry(strike_key).or_insert_with(|| {
                let mut l = StrikeLevel::default();
                l.strike = contract.strike;
                l
            });

            let prev_oi = *self.prev_oi.get(&contract.instrument_token).unwrap_or(&0);
            let curr_oi = tick.oi as u64;
            // Simple scan-to-scan delta: if OI didn't change the delta is 0 naturally.
            // The old exchange_ts guard was blocking valid OI changes when the exchange
            // sent a fresh OI packet but exchange_ts happened to equal the previous scan's ts.
            let oi_change = if curr_oi > 0 {
                curr_oi as i64 - prev_oi as i64
            } else {
                0i64
            };
            self.prev_oi.insert(contract.instrument_token, curr_oi);

            match contract.option_type {
                OptionType::CE => {
                    level.ce_token = Some(contract.instrument_token);
                    level.ce_ltp = tick.ltp;
                    level.ce_oi = curr_oi;
                    level.ce_oi_change = oi_change;
                    level.ce_volume = tick.volume as u64;
                }
                OptionType::PE => {
                    level.pe_token = Some(contract.instrument_token);
                    level.pe_ltp = tick.ltp;
                    level.pe_oi = curr_oi;
                    level.pe_oi_change = oi_change;
                    level.pe_volume = tick.volume as u64;
                }
            }
        }

        if strike_map.is_empty() {
            return None;
        }

        let min_strike = strike_map.values().map(|s| s.strike).fold(f64::INFINITY, f64::min);
        let max_strike = strike_map.values().map(|s| s.strike).fold(f64::NEG_INFINITY, f64::max);

        // Compute chain center from liquid near-ATM strikes only (both CE and PE have ltp).
        // Using (min+max)/2 across all strikes is unreliable when a wide strike range is
        // configured — deep ITM PE options with no matching CE pull min_strike far below ATM,
        // biasing strike_mid and causing far_from_chain to falsely reject the index token.
        let liquid_center: Option<f64> = {
            let mut liquid: Vec<f64> = strike_map.values()
                .filter(|l| l.ce_ltp > 0.01 && l.pe_ltp > 0.01)
                .map(|l| l.strike)
                .collect();
            liquid.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            if liquid.is_empty() { None } else { Some(liquid[liquid.len() / 2]) }
        };
        let strike_mid = liquid_center.unwrap_or_else(|| (min_strike + max_strike) / 2.0);

        let underlying_raw = self.underlying_tokens.get(underlying)
            .and_then(|tok| self.store.get(*tok));
        let spot_from_token = underlying_raw.as_ref().map(|t| t.ltp).filter(|v| *v > 0.0);
        // ohlc.close is the previous session's official closing price, broadcast in every
        // exchange tick. Used for overnight gap computation.
        let prev_session_close = underlying_raw.as_ref()
            .map(|t| t.ohlc.close)
            .filter(|v| *v > 10.0);
        let synthetic_spot = self.estimate_synthetic_spot(&strike_map, t_years);

        let spot = match (spot_from_token, synthetic_spot) {
            (Some(token_spot), Some(synth_spot)) => {
                let token_plausible = token_spot >= min_strike * 0.60 && token_spot <= max_strike * 1.40;
                let far_from_chain = strike_mid > 0.0 && ((token_spot - strike_mid).abs() / strike_mid) > 0.35;
                let token_vs_synth_large_gap = ((token_spot - synth_spot).abs() / synth_spot.max(1.0)) > 0.25;

                if !token_plausible || far_from_chain || token_vs_synth_large_gap {
                    synth_spot
                } else {
                    token_spot
                }
            }
            (Some(token_spot), None) => token_spot,
            (None, Some(synth_spot)) => synth_spot,
            (None, None) => return None,
        };

        if spot <= 0.0 {
            return None;
        }

        {
            let now_ms = self.current_time_ms();
            let buf = self.spot_history.entry(underlying.to_string()).or_default();
            buf.push_back((now_ms, spot));
            while buf.front().map(|(t, _)| now_ms.saturating_sub(*t) > 60 * 60 * 1_000).unwrap_or(false) {
                buf.pop_front();
            }

            // Frozen-spot guard: if the spot hasn't moved across the last 6 entries
            // spanning at least 4 minutes, the WebSocket feed is stale (e.g., 403 auth
            // failure or disconnection). Bail out so we don't scan on dead data.
            if buf.len() >= 4 {
                let tail: Vec<(u64, f64)> = buf.iter().rev().take(4).cloned().collect();
                let span_ms = tail[0].0.saturating_sub(tail[3].0);
                if span_ms >= 3 * 60 * 1_000 {
                    let (min_s, max_s) = tail.iter().fold(
                        (f64::MAX, f64::MIN),
                        |(mn, mx), (_, v)| (mn.min(*v), mx.max(*v)),
                    );
                    // A truly dead store returns the exact same stored value every scan.
                    // Use an absolute threshold (< 0.5 points = sub-tick movement) rather
                    // than a percentage: a percentage fires on quiet backtest afternoons where
                    // the spot legitimately moves only 10-15 points over 30 minutes.
                    if max_s > 0.0 && (max_s - min_s) < 0.5 {
                        warn!(
                            "{} spot frozen at {:.0} for {}min — stale WebSocket feed, skipping scan",
                            underlying, spot, span_ms / 60_000
                        );
                        return None;
                    }
                }
            }
        }

        for contract in &self.contracts {
            if contract.underlying != underlying || contract.expiry != expiry {
                continue;
            }
            let tick = match self.store.get(contract.instrument_token) {
                Some(t) if t.ltp > 0.01 => t,
                _ => continue,
            };
            let strike_key = (contract.strike * 100.0) as u64;
            let Some(level) = strike_map.get_mut(&strike_key) else {
                continue;
            };

            let greeks = compute_greeks(
                spot,
                contract.strike,
                t_years,
                self.risk_free_rate,
                self.dividend_yield,
                tick.ltp,
                contract.option_type,
            );

            match contract.option_type {
                OptionType::CE => {
                    if let Some(g) = &greeks {
                        level.ce_iv = g.iv;
                        level.ce_delta = g.delta;
                        level.ce_gamma = g.gamma;
                        level.ce_theta = g.theta;
                    }
                }
                OptionType::PE => {
                    if let Some(g) = &greeks {
                        level.pe_iv = g.iv;
                        level.pe_delta = g.delta;
                        level.pe_gamma = g.gamma;
                        level.pe_theta = g.theta;
                    }
                }
            }
        }

        for level in strike_map.values_mut() {
            level.straddle_premium = level.ce_ltp + level.pe_ltp;
            // Only compute skew when both sides solved; zero-IV from a failed Newton-Raphson
            // solve would otherwise fabricate a skew signal.
            level.iv_skew = if level.ce_iv > 0.001 && level.pe_iv > 0.001 {
                level.ce_iv - level.pe_iv
            } else {
                f64::NAN
            };
            level.ce_pe_oi_ratio = if level.pe_oi > 0 {
                level.ce_oi as f64 / level.pe_oi as f64
            } else {
                f64::NAN
            };
        }

        let strikes: Vec<StrikeLevel> = strike_map.into_values().collect();
        if strikes.is_empty() {
            return None;
        }

        let total_ce_oi: u64 = strikes.iter().map(|s| s.ce_oi).sum();
        let total_pe_oi: u64 = strikes.iter().map(|s| s.pe_oi).sum();
        let total_ce_vol: u64 = strikes.iter().map(|s| s.ce_volume).sum();
        let total_pe_vol: u64 = strikes.iter().map(|s| s.pe_volume).sum();

        let pcr_oi = if total_ce_oi > 0 { total_pe_oi as f64 / total_ce_oi as f64 } else { 1.0 };
        let pcr_vol = if total_ce_vol > 0 { total_pe_vol as f64 / total_ce_vol as f64 } else { 1.0 };

        let max_pain = compute_max_pain(&strikes);

        let atm_level = strikes.iter()
            .min_by(|a, b| (a.strike - spot).abs().partial_cmp(&(b.strike - spot).abs()).unwrap());

        let atm_strike = atm_level.map(|l| l.strike).unwrap_or(spot);
        // Average only valid IV sides; a failed Newton-Raphson solve leaves the field at 0.0
        // which would halve atm_iv and silently corrupt iv_rank, regime, and skew logic.
        let atm_iv = atm_level.map(|l| {
            match (l.ce_iv > 0.001, l.pe_iv > 0.001) {
                (true, true)  => (l.ce_iv + l.pe_iv) / 2.0,
                (true, false) => l.ce_iv,
                (false, true) => l.pe_iv,
                (false, false) => 0.0,
            }
        }).unwrap_or(0.0);

        let lot_size = match self.contracts.iter().find(|c| c.underlying == underlying).map(|c| c.lot_size) {
            Some(ls) => ls,
            None => {
                warn!("{}: lot_size unknown (no contracts loaded from API) — skipping snapshot", underlying);
                return None;
            }
        };

        let net_gex = compute_gex(&strikes, spot, lot_size);
        let iv_rank = self.iv_history.iv_rank(underlying, atm_iv);

        let mut strikes_mut = strikes;
        for level in &mut strikes_mut {
            level.gex_contribution = (level.ce_gamma * level.ce_oi as f64
                - level.pe_gamma * level.pe_oi as f64) * spot * lot_size as f64;
        }

        let regime = self.detect_regime(pcr_oi, atm_iv, spot, &strikes_mut);

        // Overnight gap: how far is today's spot from yesterday's official close?
        // Primary source: underlying index token's ohlc.close (live engine where index token
        // is subscribed). Fallback: put-call parity on ATM options — each option tick carries
        // its own previous day close price, so S_yesterday ≈ K_atm + CE_close - PE_close.
        // The parity error is ~20 pts on a 23000 index (0.09%), negligible vs 2.5% gate.
        let overnight_gap_pct = {
            // Primary: index token close (works in live engine)
            let from_index = prev_session_close.map(|c| (spot - c) / c * 100.0);
            // Fallback: put-call parity from ATM options (works in backtest from CSV close field)
            let from_parity = self.contracts.iter()
                .find(|c| c.underlying == underlying && c.expiry == expiry
                    && c.option_type == OptionType::CE
                    && (c.strike - atm_strike).abs() < 1.0)
                .and_then(|ce_c| {
                    let pe_c = self.contracts.iter().find(|c|
                        c.underlying == underlying && c.expiry == expiry
                        && c.option_type == OptionType::PE
                        && (c.strike - atm_strike).abs() < 1.0)?;
                    let ce_close = self.store.get(ce_c.instrument_token).filter(|t| t.ohlc.close > 0.1)?.ohlc.close;
                    let pe_close = self.store.get(pe_c.instrument_token).filter(|t| t.ohlc.close > 0.1)?.ohlc.close;
                    let synth_prev = atm_strike + ce_close - pe_close;
                    if synth_prev > 5000.0 { Some((spot - synth_prev) / synth_prev * 100.0) } else { None }
                });
            from_index.or(from_parity).unwrap_or(0.0)
        };

        Some(ChainSnapshot {
            underlying: underlying.to_string(),
            spot,
            timestamp_ms: ts,
            expiry: expiry.clone(),
            strikes: strikes_mut,
            atm_strike,
            atm_iv,
            iv_rank,
            pcr_oi,
            pcr_vol,
            max_pain,
            total_ce_oi,
            total_pe_oi,
            total_ce_vol,
            total_pe_vol,
            net_gex,
            days_to_expiry,
            regime,
            overnight_gap_pct,
        })
    }


    fn sideways_window_mins(&self, session: SessionPhase, days_to_expiry: f64) -> u64 {
        let base = match session {
            SessionPhase::Morning => 25,
            SessionPhase::Midday => 45,
            SessionPhase::Afternoon => 30,
            SessionPhase::Closing => 20,
            _ => 30,
        };
        let expiry_adj = if days_to_expiry <= 1.0 { 10 } else { 0 };
        (base + expiry_adj).clamp(15, 60)
    }

    fn quantile_from_sorted(values: &[f64], q: f64) -> Option<f64> {
        if values.is_empty() {
            return None;
        }
        let q = q.clamp(0.0, 1.0);
        let n = values.len();
        if n == 1 {
            return Some(values[0]);
        }
        let pos = q * (n.saturating_sub(1)) as f64;
        let lo = pos.floor() as usize;
        let hi = pos.ceil() as usize;
        if lo == hi {
            return Some(values[lo]);
        }
        let w = pos - lo as f64;
        Some(values[lo] * (1.0 - w) + values[hi] * w)
    }

    fn rolling_spot_stats(&self, underlying: &str, window_ms: u64) -> Option<RollingSpotStats> {
        let now = self.current_time_ms();
        let buf = self.spot_history.get(underlying)?;

        let mut prices: Vec<f64> = Vec::new();
        for (ts, p) in buf.iter().rev() {
            if now.saturating_sub(*ts) > window_ms {
                break;
            }
            if *p > 0.0 && p.is_finite() {
                prices.push(*p);
            }
        }
        prices.reverse();
        if prices.len() < 4 {
            return None;
        }

        let hi = prices.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let lo = prices.iter().cloned().fold(f64::INFINITY, f64::min);
        let mid = (hi + lo) / 2.0;
        if !mid.is_finite() || mid <= 0.0 {
            return None;
        }
        let range_pct = (hi - lo) / mid;

        let mut abs_rets: Vec<f64> = prices
            .windows(2)
            .filter_map(|w| {
                let prev = w[0];
                let curr = w[1];
                if prev <= 0.0 {
                    return None;
                }
                let r = ((curr - prev) / prev).abs();
                if r.is_finite() {
                    Some(r)
                } else {
                    None
                }
            })
            .collect();
        if abs_rets.is_empty() {
            return None;
        }
        abs_rets.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let q25 = Self::quantile_from_sorted(&abs_rets, 0.25)?;
        let q50 = Self::quantile_from_sorted(&abs_rets, 0.50)?;
        let q75 = Self::quantile_from_sorted(&abs_rets, 0.75)?;

        Some(RollingSpotStats {
            range_pct,
            q25_abs_ret: q25,
            q50_abs_ret: q50,
            q75_abs_ret: q75,
            sample_count: prices.len(),
        })
    }

    fn dynamic_sideways_threshold_pct(
        &self,
        underlying: &str,
        atm_iv: f64,
        days_to_expiry: f64,
    ) -> Option<(u64, f64)> {
        let session = self.current_session();
        let window_mins = self.sideways_window_mins(session, days_to_expiry);
        let window_ms = window_mins * 60 * 1_000;
        let stats = self.rolling_spot_stats(underlying, window_ms)?;

        let realized_component = (stats.q25_abs_ret * 2.5)
            .max(stats.q50_abs_ret * 2.0)
            .max(stats.q75_abs_ret * 1.8)
            .clamp(0.0008, 0.0080);
        let iv_component = (atm_iv * 0.015).clamp(0.0010, 0.0060);

        let session_mult = match session {
            SessionPhase::Morning => 0.95,
            SessionPhase::Midday => 1.15,
            SessionPhase::Afternoon => 1.0,
            SessionPhase::Closing => 1.10,
            _ => 1.0,
        };

        let threshold = (realized_component.max(iv_component) * session_mult)
            .clamp(0.0015, 0.0075);
        Some((window_mins, threshold))
    }

    fn is_spot_sideways(
        &self,
        underlying: &str,
        atm_iv: f64,
        days_to_expiry: f64,
    ) -> Option<(bool, u64, f64, f64, usize)> {
        let (window_mins, threshold) =
            self.dynamic_sideways_threshold_pct(underlying, atm_iv, days_to_expiry)?;
        let window_ms = window_mins * 60 * 1_000;
        let stats = self.rolling_spot_stats(underlying, window_ms)?;
        Some((
            stats.range_pct < threshold,
            window_mins,
            stats.range_pct,
            threshold,
            stats.sample_count,
        ))
    }

    fn dynamic_max_pain_min_dev_pct(
        &self,
        underlying: &str,
        atm_iv: f64,
        days_to_expiry: f64,
    ) -> f64 {
        let threshold_pct = self
            .dynamic_sideways_threshold_pct(underlying, atm_iv, days_to_expiry)
            .map(|(_, t)| t)
            .unwrap_or(0.003);
        (threshold_pct * 100.0 * 1.4).clamp(0.20, 1.20)
    }

    /// Progressive cooldown: starts tight at open (2 min) and widens each hour.
    /// Keeps expiry-day gamma scalp opportunities available at open while
    /// preventing late-session overtrading.
    ///
    /// 09:15-10:15 → 2 min | 10:15-11:15 → 4 min | 11:15-12:15 → 6 min
    /// 12:15-13:15 → 8 min | 13:15-14:15 → 10 min | 14:15-15:00 → 12 min
    fn progressive_cooldown_ms(&self) -> u64 {
        let now_utc = Utc.timestamp_millis_opt(self.current_time_ms() as i64)
            .single().unwrap_or_else(Utc::now);
        let ist = FixedOffset::east_opt(5 * 3600 + 30 * 60).expect("valid IST offset");
        let now_ist = now_utc.with_timezone(&ist);
        let ist_mins = now_ist.hour() * 60 + now_ist.minute();
        let mins: u64 = match ist_mins {
            0..=614  => 2,  // 09:15–10:15
            615..=674 => 4,  // 10:15–11:15
            675..=734 => 6,  // 11:15–12:15
            735..=794 => 8,  // 12:15–13:15
            795..=854 => 10, // 13:15–14:15
            _         => 12, // 14:15–15:00
        };
        mins * 60 * 1_000
    }

    fn detect_regime(
        &self,
        pcr_oi: f64,
        atm_iv: f64,
        spot: f64,
        strikes: &[StrikeLevel],
    ) -> MarketRegime {
        // Skip IV-based regime gates when atm_iv is zero — that means both CE and PE
        // IV solves failed for the ATM strike; using 0.0 would wrongly fire ComplacencyLowVol.
        if atm_iv > 0.001 {
            if atm_iv > 0.40 { return MarketRegime::PanicHighVol; }
            if atm_iv < 0.11 { return MarketRegime::ComplacencyLowVol; }
        }

        let avg_skew: f64 = {
            let skews: Vec<f64> = strikes.iter()
                .filter(|s| s.iv_skew.is_finite())
                .map(|s| s.iv_skew)
                .collect();
            if skews.is_empty() { 0.0 }
            else { skews.iter().sum::<f64>() / skews.len() as f64 }
        };

        let _ = spot;
        match (pcr_oi, avg_skew) {
            (p, s) if p > 1.5 && s < -0.02 => MarketRegime::StrongBullish,
            (p, _) if p > 1.2              => MarketRegime::Bullish,
            (p, _) if p < 0.70             => MarketRegime::StrongBearish,
            (p, _) if p < 0.90             => MarketRegime::Bearish,
            _                               => MarketRegime::Neutral,
        }
    }


    fn generate_signals(&mut self, snap: &ChainSnapshot) -> Vec<Signal> {
        let mut signals = Vec::new();

        // Feed this scan's spot into the per-underlying price-action series (drives the
        // technical gate below). No look-ahead: the just-fed tick only opens an in-progress
        // bar; the bias reads CLOSED bars (prior scans) only.
        if snap.spot > 0.0 {
            let now = self.current_time_ms();
            self.spot_series
                .entry(snap.underlying.clone())
                .or_default()
                .ingest(now, snap.spot);
        }

        let lot_size = match self.contracts.iter().find(|c| c.underlying == snap.underlying).map(|c| c.lot_size) {
            Some(ls) => ls,
            None => {
                warn!("{}: lot_size unknown (no contracts loaded from API) — skipping signal generation", snap.underlying);
                return signals;
            }
        };

        if self.positions.iter().any(|p| p.is_open && p.underlying == snap.underlying) {
            return signals;
        }

        let cooldown_ms: u64 = self.progressive_cooldown_ms();
        let now_ms = self.current_time_ms();
        if let Some(&last_ms) = self.last_signal_ms.get(&snap.underlying) {
            if now_ms.saturating_sub(last_ms) < cooldown_ms {
                return signals;
            }
        }

        if self
            .stop_streak_by_underlying
            .get(&snap.underlying)
            .copied()
            .unwrap_or(0)
            >= 3
        {
            return signals;
        }

        let mut scored: Vec<(SignalAction, f64, StrategyType, Vec<(String, f64)>)> = Vec::new();

        let sideways_assessment = self.is_spot_sideways(
            &snap.underlying,
            snap.atm_iv,
            snap.days_to_expiry,
        );
        let sideways = sideways_assessment.map(|(s, _, _, _, _)| s).unwrap_or(false);
        let sideways_window_mins = sideways_assessment.map(|(_, w, _, _, _)| w).unwrap_or(30);
        let sideways_range_pct = sideways_assessment.map(|(_, _, r, _, _)| r).unwrap_or(0.0);
        let sideways_threshold_pct = sideways_assessment.map(|(_, _, _, t, _)| t).unwrap_or(0.003);
        let compressive = sideways_range_pct <= sideways_threshold_pct * 1.35;
        let hard_sideways_block = sideways && sideways_range_pct <= sideways_threshold_pct * 0.55;

        if hard_sideways_block {
            return signals;
        }

        // IV rank gates — protect against buying structurally overpriced options.
        //
        // Gate A: In a low-to-moderate IV environment (atm_iv < 38%), an extremely high
        // iv_rank (>80%) means options are near the session IV ceiling. Buying at the
        // peak of today's IV range risks a double loss: adverse spot move + IV crush
        // back toward the session average. Days with genuinely elevated IV (atm_iv ≥ 38%)
        // are exempt because a high iv_rank there reflects sustained fear, not a spike.
        if snap.iv_rank > 80.0 && snap.iv_rank > 0.0 && snap.atm_iv < 0.38 {
            return signals;
        }
        // Gate B: High atm_iv (>38%) combined with low iv_rank (<25%) means IV spiked
        // earlier in the session and is now compressing — spot has already repriced the
        // shock and IV is deflating. Buying expensive options into falling IV is a
        // structural losing trade: spot often bounces (adverse move) while the option
        // loses value from both theta and IV crush simultaneously.
        if snap.atm_iv > 0.38 && snap.iv_rank < 25.0 && snap.iv_rank > 0.0 {
            return signals;
        }

        // Gate C: 0-DTE + IV at session floor + bullish PCR → CE entry has no IV tailwind.
        // All three conditions must hold simultaneously (this is the Apr 07 failure signature).
        // On 0-DTE expiry mornings, if IV rank is at or near zero (options priced at annual
        // lows) AND PCR is mildly bullish, a CE buy has zero IV expansion tailwind. The
        // premium is already minimal and any adverse tick accelerates the loss via gamma.
        // Feb 24 (WIN) is preserved: iv_rank=57% → fails the iv_rank<2.0 check.
        if snap.days_to_expiry < 0.5 && snap.iv_rank < 2.0 && snap.pcr_oi > 1.0 {
            warn!("  {} 0-DTE CE gated: IV Rank {:.1}% (session floor) + PCR {:.2} — no IV edge for CE on expiry morning",
                snap.underlying, snap.iv_rank, snap.pcr_oi);
            return signals;
        }

        // Gate D: Large overnight gap on multi-day contracts (Apr 08 fingerprint).
        // After a prior session with extreme vol (iv_rank=100%, crash or gap-down), the
        // market may open with a relief-rally gap the following day. Options are repriced
        // overnight to reflect the new spot level, but the directional strategies (TrendFollow,
        // OIDivergence) still read residual crash OI/regime data as bearish — creating a
        // false-confidence signal on the wrong side of the gap. Entering a PE into a gap-up
        // (or CE into a gap-down) is fighting the opening momentum in a regime that has
        // already re-priced. The 2.5% threshold is calibrated to April 2026 data where
        // Apr 08 saw a +4.2% gap from Apr 07's crash. All 13 winning days had gaps < 2.5%.
        // Exemption: 0-DTE/1-DTE (DTE <= 1.5) where gamma rules supersede gap context.
        if snap.overnight_gap_pct.abs() > 2.5 && snap.days_to_expiry > 1.5 {
            warn!("  {} Gate D: {:.1}% overnight gap with {:.2} DTE — post-gap regime unreliable, entry blocked",
                snap.underlying, snap.overnight_gap_pct, snap.days_to_expiry);
            return signals;
        }

        // Gate E: Compressed spot range + StrongBearish PCR extreme + mid-DTE entry
        // (Apr 10 fingerprint). When spot is in a tight compression range (compressive=true)
        // AND pcr_oi is into StrongBearish territory (< 0.69, below the 0.70 regime boundary),
        // it signals MM range-defense positioning: option writers are aggressively writing
        // calls to pin the range, creating artificial OI skew that OIDivergence and
        // TrendFollow misread as directional conviction. When the range breaks, it often
        // resolves against the PE position because the call-writing overhead was absorbing
        // natural demand above. DTE window [1.5, 5.0]: below 1.5 gamma-scalp logic applies;
        // above 5.0 the regime has room to legitimately develop (Apr 01 DTE=6.13, PCR=0.662
        // → won because regime transitioned to STRONG BEAR mid-hold).
        // PCR threshold 0.71: saves Mar 27 (PCR=0.733, 2.2pt margin) while blocking Apr 10
        // where PCR oscillated between 0.667–0.70 through the compressive morning session.
        // Safe for all 13 wins: zero wins have compressive + PCR<0.71 + DTE[1.5,5.0] together.
        if compressive
            && snap.pcr_oi < 0.71
            && snap.days_to_expiry >= 1.5
            && snap.days_to_expiry <= 5.0
        {
            warn!("  {} Gate E: COMPRESSED range + StrongBearish PCR {:.3} + {:.2} DTE — \
                 MM range-defense skew, not directional pressure; entry blocked",
                snap.underlying, snap.pcr_oi, snap.days_to_expiry);
            return signals;
        }

        if let Some(s) = self.strategy_gamma_scalp(snap)   { scored.push(s); }
        // IVExpansion requires IV to expand — blocked in compressed regimes where
        // low volatility is likely to persist, directly contradicting the entry thesis.
        if !compressive {
            if let Some(s) = self.strategy_iv_expansion(snap) { scored.push(s); }
        }
        if let Some(s) = self.strategy_spot_reversal(snap) { scored.push(s); }
        if let Some(s) = self.strategy_trend_follow(snap)  { scored.push(s); }
        if let Some(s) = self.strategy_max_pain(snap)      { scored.push(s); }
        if let Some(s) = self.strategy_oi_divergence(snap) {
            let min_sideways_score = (self.min_confidence + 12.0).min(95.0);
            if !compressive || s.1 >= min_sideways_score {
                scored.push(s);
            }
        }
        if let Some(s) = self.strategy_iv_skew(snap)       { scored.push(s); }
        if let Some(s) = self.strategy_gex_play(snap) {
            let min_sideways_score = (self.min_confidence + 15.0).min(97.0);
            if !compressive || s.1 >= min_sideways_score {
                scored.push(s);
            }
        }

        if scored.is_empty() {
            warn!("  {} gen_signals: all strategies returned None", snap.underlying);
            return signals;
        }

        let ce_score: f64 = scored.iter().filter(|(a, _, _, _)| *a == SignalAction::BuyCE).map(|(_, s, _, _)| *s).sum();
        let pe_score: f64 = scored.iter().filter(|(a, _, _, _)| *a == SignalAction::BuyPE).map(|(_, s, _, _)| *s).sum();
        let ce_count = scored.iter().filter(|(a, _, _, _)| *a == SignalAction::BuyCE).count();
        let pe_count = scored.iter().filter(|(a, _, _, _)| *a == SignalAction::BuyPE).count();
        warn!("  {} gen_signals: strategies_fired={} CE={} (score={:.0}) PE={} (score={:.0})",
            snap.underlying, scored.len(), ce_count, ce_score, pe_count, pe_score);
        for (action, score, strat, _) in &scored {
            warn!("    {:?} {:.0} -> {:?}", action, score, strat);
        }

        // Use total accumulated score to pick direction, not vote count.
        // Prevents 3 weak CE signals from overriding 2 strong PE signals.
        let dominant_action = if ce_score > pe_score { SignalAction::BuyCE }
                              else if pe_score > ce_score { SignalAction::BuyPE }
                              else { SignalAction::Hold };

        if dominant_action == SignalAction::Hold {
            warn!("  {} gen_signals: CE/PE tied => Hold, no signal", snap.underlying);
            return signals;
        }

        let dominant_has_spot_reversal = scored.iter().any(|(a, _, strat, _)| {
            *a == dominant_action && *strat == StrategyType::SpotReversal
        });

        let mut technical_note: Option<String> = None;

        // ── Technical confirmation gate (HARD veto on direction conflict) ──
        // Only trade when the chain's chosen direction AGREES with the spot's price-action
        // bias. A BuyPE (bearish) into a CONFIRMED uptrend — exactly today's losing trade —
        // is vetoed. Neutral/unformed bias passes (no protection until indicators form).
        if let Some(series) = self.spot_series.get(&snap.underlying) {
            let b = series.bias();
            let summary = series
                .technical_summary()
                .unwrap_or_else(|| "technical panel still warming".to_string());
            let desired_direction = match dominant_action {
                SignalAction::BuyCE => crate::technicals::Direction::Bull,
                SignalAction::BuyPE => crate::technicals::Direction::Bear,
                _ => crate::technicals::Direction::Neutral,
            };
            let scalp_assessment = series.scalp_assessment(desired_direction);
            let scalp = scalp_assessment
                .as_ref()
                .map(|s| {
                    format!(
                        " | scalp {:?} alignment {}/{}: {}",
                        s.direction, s.aligned, s.observed, s.summary
                    )
                })
                .unwrap_or_default();
            technical_note = Some(format!(
                "Technicals {:?} strength {:.0}%: {}{}",
                b.direction,
                b.strength * 100.0,
                summary,
                scalp
            ));
            let veto = match dominant_action {
                SignalAction::BuyCE => !series.allows_bullish(),
                SignalAction::BuyPE => !series.allows_bearish(),
                _ => false,
            };
            if veto && !dominant_has_spot_reversal {
                warn!(
                    "  {} gen_signals: {:?} VETOED by technicals — price-action bias {:?} (strength {:.2}) conflicts; {}",
                    snap.underlying,
                    dominant_action,
                    b.direction,
                    b.strength,
                    summary
                );
                return signals;
            } else if veto
                && !scalp_assessment
                    .as_ref()
                    .map(|s| s.aligned >= 2 && s.aligned * 2 >= s.observed)
                    .unwrap_or(false)
            {
                warn!(
                    "  {} gen_signals: {:?} VETOED — spot-reversal is fighting slower bias {:?} (strength {:.2}) without scalp majority; {}{}",
                    snap.underlying,
                    dominant_action,
                    b.direction,
                    b.strength,
                    summary,
                    scalp
                );
                return signals;
            } else if veto {
                warn!(
                    "  {} gen_signals: {:?} allowed by spot-reversal break despite slower bias {:?} (strength {:.2}); {}",
                    snap.underlying,
                    dominant_action,
                    b.direction,
                    b.strength,
                    summary
                );
            }
        }

        // 2026-06-30 fingerprint: expiry-morning compression with very low PCR created a false
        // bearish read from call-writing/strike pinning. In that setup, the first PE cluster
        // repeatedly stopped out before the tape developed real downside breadth. Keep later
        // expiry-day PE continuation available once PCR normalizes, and keep multi-day trades out
        // of this gate entirely.
        if dominant_action == SignalAction::BuyPE
            && snap.days_to_expiry < 0.5
            && matches!(self.current_session(), SessionPhase::Morning)
            && compressive
            && snap.pcr_oi < 0.70
        {
            warn!(
                "  {} gen_signals: PE blocked — 0-DTE morning compression with very low PCR {:.2} (<0.70) is likely strike-pinning/call-writing skew",
                snap.underlying, snap.pcr_oi
            );
            return signals;
        }

        // Symmetric PCR conviction floors.
        //
        // For PE: PCR 0.89+ starts to lose bearish clarity, but 0.83–0.88 can still be
        //         actionable when multiple bearish strategies align. Keep a floor, but
        //         avoid filtering out expiry-week downside continuation setups entirely.
        //
        // For CE: PCR 1.23+ is enough to confirm bullish skew when trend/GEX/IV agree.
        //         The older 1.25 floor filtered out valid March expiry-week winners.
        let pe_pcr_ceiling = if dominant_has_spot_reversal { 1.05 } else { 0.88 };
        let ce_pcr_floor = if dominant_has_spot_reversal { 0.95 } else { 1.23 };
        if dominant_action == SignalAction::BuyPE && snap.pcr_oi > pe_pcr_ceiling {
            warn!("  {} gen_signals: PE blocked — PCR {:.2} > {:.2} (weak bearish conviction)",
                snap.underlying, snap.pcr_oi, pe_pcr_ceiling);
            return signals;
        }
        if dominant_action == SignalAction::BuyCE && snap.pcr_oi < ce_pcr_floor {
            warn!("  {} gen_signals: CE blocked — PCR {:.2} < {:.2} (weak bullish conviction)",
                snap.underlying, snap.pcr_oi, ce_pcr_floor);
            return signals;
        }

        let dominant_signals: Vec<_> = scored.iter()
            .filter(|(a, _, _, _)| *a == dominant_action)
            .collect();
        if dominant_signals.is_empty() {
            return signals;
        }

        let (composite_score, mut merged_reasons, primary_strategy) = {

            let max_score = dominant_signals.iter().map(|(_, s, _, _)| *s).fold(0.0_f64, f64::max);
            let sum_all = dominant_signals.iter().map(|(_, s, _, _)| *s).sum::<f64>();
            let composite = (max_score + (sum_all - max_score) * 0.25).min(100.0);

            let mut reasons: Vec<(String, f64)> = Vec::new();
            let mut strategy = StrategyType::Composite;
            for (_, _, strat, r) in &dominant_signals {
                reasons.extend(r.clone());
                if dominant_signals.len() == 1 { strategy = *strat; }
            }
            reasons.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
            reasons.dedup_by(|a, b| a.0 == b.0);
            (composite, reasons, strategy)
        };
        if let Some(note) = technical_note {
            merged_reasons.push((note, 0.0));
        }

        let n_dominant = dominant_signals.len();
        let confidence_floor = self.min_confidence_floor_for(snap, primary_strategy);
        warn!("  {} gen_signals: dominant={:?} composite={:.1} threshold={:.0} strategies={}",
            snap.underlying, dominant_action, composite_score, confidence_floor, n_dominant);
        if composite_score < confidence_floor { return signals; }

        let session_now = self.current_session();
        if matches!(session_now, SessionPhase::Midday)
            && snap.days_to_expiry > 3.5
            && matches!(dominant_action, SignalAction::BuyPE)
            && snap.iv_rank >= 70.0
            && snap.pcr_oi >= 0.85
        {
            warn!(
                "  {} gen_signals: PE blocked — midday 4+DTE bearish flow with rich IV {:.0}% and mild PCR {:.2} is treated as a no-trade",
                snap.underlying,
                snap.iv_rank,
                snap.pcr_oi
            );
            return signals;
        }

        let target_strike = self.pick_strike(snap, &dominant_action, lot_size);
        let option_price = match dominant_action {
            SignalAction::BuyCE => target_strike.map(|s| s.ce_ltp).unwrap_or(0.0),
            SignalAction::BuyPE => target_strike.map(|s| s.pe_ltp).unwrap_or(0.0),
            _ => 0.0,
        };

        // Below this floor gamma/theta dominates and transaction costs become disproportionate.
        warn!("  {} gen_signals: option_price={:.1} (floor={:.0})", snap.underlying, option_price, MIN_TRADEABLE_OPTION_PRICE);
        if option_price < MIN_TRADEABLE_OPTION_PRICE { return signals; }

        // Regime-adaptive exits: target and stop calibrated to market character,
        // not a single fixed config value. Confidence and DTE apply as scalars.
        let (effective_target_pct, effective_stop_pct) = {
            // Confidence multiplier: 0.90x at threshold → 1.15x at threshold+30.
            let conf_norm = ((composite_score - confidence_floor) / 30.0).clamp(0.0, 1.0);
            let conf_mult = 0.90 + conf_norm * 0.25;

            if compressive {
                // Compressed range: target proportional to the measured range, stop floored hard.
                let comp_target = (sideways_threshold_pct * 100.0 * 55.0).clamp(12.0, 25.0);
                let comp_stop = (comp_target * 0.70).max(12.0);
                (comp_target.min(self.profit_target_pct), comp_stop)
            } else {
                // Base target/stop by regime character.
                let (regime_t, regime_s) = match snap.regime {
                    MarketRegime::StrongBullish | MarketRegime::StrongBearish => (48.0_f64, 22.0_f64),
                    MarketRegime::Bullish | MarketRegime::Bearish           => (38.0,     20.0),
                    MarketRegime::PanicHighVol                              => (25.0,     17.0),
                    MarketRegime::ComplacencyLowVol                         => (22.0,     15.0),
                    MarketRegime::Neutral | MarketRegime::Sideways          => (30.0,     18.0),
                };
                // Expiry day: gamma amplifies moves — allow up to 15% more room on target.
                let dte_mult = if snap.days_to_expiry <= 1.0 { 1.15 } else { 1.0 };
                let target = (regime_t * conf_mult * dte_mult).min(self.profit_target_pct);
                // Stop: wider for high-confidence (more conviction = tolerate more noise).
                // Floor at 12% — never risk less, as bid-ask alone can span 5-8%.
                let stop = (regime_s * (2.0 - conf_mult)).clamp(12.0, self.stop_loss_pct);
                (target, stop)
            }
        };

        let effective_target_pct = {
            if matches!(session_now, SessionPhase::Afternoon) {
                let ts_ms = self.current_time_ms();
                let now_utc = Utc.timestamp_millis_opt(ts_ms as i64).single().unwrap_or_else(Utc::now);
                let ist_offset = chrono::FixedOffset::east_opt(5 * 3600 + 30 * 60)
                    .expect("valid IST offset");
                let now_ist = now_utc.with_timezone(&ist_offset);
                let mins = now_ist.hour() * 60 + now_ist.minute();
                let elapsed = mins.saturating_sub(840) as f64;
                let weight = 1.0 - (elapsed / 74.0) * 0.35;
                effective_target_pct * weight.clamp(0.65, 1.0)
            } else {
                effective_target_pct
            }
        };

        let strike_val = target_strike.map(|s| s.strike).unwrap_or(snap.atm_strike);
        let mut lots = self.risk.size_lots(option_price, lot_size, effective_stop_pct / 100.0);
        if compressive {
            lots = lots.min(1);
        }
        if lots == 0 { return signals; }

        // Flat brokerage is per order, not per lot (bug 5): premium scales with lots,
        // brokerage is added once via entry_order_cost.
        let capital_required = option_price * lot_size as f64 * lots as f64
            + entry_order_cost(option_price, lot_size, lots);
        if capital_required > self.risk.available_capital() {
            return signals;
        }

        let target_price = option_price * (1.0 + effective_target_pct / 100.0);
        let stop_price   = option_price * (1.0 - effective_stop_pct / 100.0);

        let straddle_prem = target_strike.map(|s| s.straddle_premium).unwrap_or(option_price * 2.0);
        let breakeven_move_pct = (straddle_prem / snap.spot) * 100.0;

        let max_loss = (option_price - stop_price) * lot_size as f64 * lots as f64
            + round_trip_order_cost(option_price, stop_price, lot_size, lots, self.current_time_ms());
        let max_profit = (target_price - option_price) * lot_size as f64 * lots as f64
            - round_trip_order_cost(option_price, target_price, lot_size, lots, self.current_time_ms());
        let risk_reward = if max_loss > 0.0 { max_profit / max_loss } else { 0.0 };

        // Reject trades without sufficient risk-reward edge.
        // Short-dated high-confidence trades often model with lower static R:R because
        // gamma compresses both target and stop; keep the standard 1.3 floor elsewhere.
        let min_risk_reward = if snap.days_to_expiry <= 3.5 && composite_score >= 62.0 {
            0.95
        } else {
            1.30
        };
        if risk_reward < min_risk_reward {
            warn!("  {} signal rejected: R:R {:.2}:1 below {:.2} floor", snap.underlying, risk_reward, min_risk_reward);
            return signals;
        }


        let session = self.current_session();
        let option_type_str = match dominant_action {
            SignalAction::BuyCE => "CE",
            SignalAction::BuyPE => "PE",
            _ => "XX",
        };

        let reasons_str = merged_reasons.iter()
            .map(|(r, s)| format!("[{:+.0}] {}", s, r))
            .collect::<Vec<_>>()
            .join(" | ");

        let regime_text = if compressive {
            format!(
                "COMPRESSED ({}m range {:.2}% <= {:.2}%)",
                sideways_window_mins,
                sideways_range_pct * 100.0,
                sideways_threshold_pct * 100.0
            )
        } else {
            format!("{}", snap.regime)
        };

        let entry_ctx = EntryContext {
            signal_id: self.next_sig_id,
            confidence: composite_score,
            strategy: format!("{}", primary_strategy),
            session: format!("{}", session),
            regime: regime_text,
            spot: snap.spot,
            atm_iv_pct: snap.atm_iv * 100.0,
            iv_rank: snap.iv_rank,
            pcr_oi: snap.pcr_oi,
            pcr_vol: snap.pcr_vol,
            max_pain: snap.max_pain,
            net_gex: snap.net_gex,
            days_to_expiry: snap.days_to_expiry,
            risk_reward,
            breakeven_pct: breakeven_move_pct,
            signal_reasons: reasons_str,
        };

        let sig_id = { self.next_sig_id += 1; self.next_sig_id - 1 };
        let mut ctx = entry_ctx.clone();
        ctx.signal_id = sig_id;

        let signal_datetime = datetime_string_from_ms(snap.timestamp_ms);
        self.journal.log_signal(
            sig_id,
            &signal_datetime,
            &snap.underlying,
            option_type_str,
            strike_val,
            &snap.expiry,
            &ctx,
            option_price,
            lots,
            lot_size,
            capital_required,
            target_price,
            stop_price,
        );

        let sig = Signal {
            id: sig_id,
            underlying: snap.underlying.clone(),
            expiry: snap.expiry.clone(),
            action: dominant_action.clone(),
            strike: strike_val,
            option_price,
            target_price,
            stop_price,
            lots,
            lot_size,
            capital_required,
            confidence: composite_score,
            strategy: primary_strategy,
            reasons: merged_reasons,
            timestamp_ms: snap.timestamp_ms,
            session,
            breakeven_move_pct,
            max_loss,
            max_profit_target: max_profit,
            risk_reward,
            entry_ctx: ctx,
        };

        self.signals_generated += 1;
        signals.push(sig);
        signals
    }


    fn pick_strike<'a>(&self, snap: &'a ChainSnapshot, action: &SignalAction, lot_size: u32) -> Option<&'a StrikeLevel> {
        let cash_capital = self.available_capital_for_new_orders();
        let sizing_capital = self.risk.max_one_lot_entry_cost_by_exposure();
        let deployable_capital = cash_capital.min(sizing_capital);

        let atm_idx = snap.strikes.iter()
            .enumerate()
            .min_by(|(_, a), (_, b)| {
                (a.strike - snap.spot).abs().partial_cmp(&(b.strike - snap.spot).abs()).unwrap()
            })
            .map(|(i, _)| i)?;

        // Always start at ATM regardless of capital tier.
        // If ATM is unaffordable, the fallback scan below finds the nearest
        // affordable strike. Forcing OTM early led to unnecessarily far strikes.
        let candidate_idx = match action {
            SignalAction::BuyCE => atm_idx,
            SignalAction::BuyPE => atm_idx,
            _ => atm_idx,
        };

        let candidate = &snap.strikes[candidate_idx];

        let price = match action {
            SignalAction::BuyCE => candidate.ce_ltp,
            SignalAction::BuyPE => candidate.pe_ltp,
            _ => 0.0,
        };
        let candidate_cost = one_lot_entry_cost(price, lot_size);

        warn!(
            "  pick_strike {} {:?} lot={} cap={:.0} sizing_cap={:.0} candidate={:.0} price={:.1} one_lot_cost={:.0}",
            snap.underlying,
            action,
            lot_size,
            cash_capital,
            sizing_capital,
            candidate.strike,
            price,
            candidate_cost,
        );
        if candidate_cost <= deployable_capital && price >= MIN_TRADEABLE_OPTION_PRICE {
            return Some(candidate);
        }

        let (prices, direction) = match action {
            SignalAction::BuyCE => (
                snap.strikes.iter().map(|s| s.ce_ltp).collect::<Vec<_>>(),
                StrikeScanDirection::Up,
            ),
            SignalAction::BuyPE => (
                snap.strikes.iter().map(|s| s.pe_ltp).collect::<Vec<_>>(),
                StrikeScanDirection::Down,
            ),
            _ => return Some(candidate),
        };

        let (idx_opt, checked) = nearest_affordable_otm_idx(
            &prices,
            candidate_idx,
            direction,
            lot_size,
            deployable_capital,
            MIN_TRADEABLE_OPTION_PRICE,
        );

        if let Some(idx) = idx_opt {
            let otm_steps = idx.abs_diff(atm_idx);
            let picked = &snap.strikes[idx];
            let picked_price = prices[idx];
            warn!(
                "  pick_strike {} {:?} fallback: checked {} strikes, selected {:.0} @ ₹{:.1} ({} OTM steps)",
                snap.underlying,
                action,
                checked,
                picked.strike,
                picked_price,
                otm_steps,
            );
            // PE: max 2 OTM — deep OTM puts need large spot drops to profit; every
            // winning PE in backtest used 0-1 OTM. CE: max 3 — markets trend up
            // more smoothly and 3-OTM CEs have delivered (Apr 02: +₹6,134).
            let max_otm = if *action == SignalAction::BuyPE { 2 } else { 3 };
            if otm_steps > max_otm {
                warn!(
                    "  pick_strike {} {:?}: {} OTM steps exceeds max {}, skipping",
                    snap.underlying, action, otm_steps, max_otm
                );
                return None;
            }
            Some(picked)
        } else {
            warn!(
                "  pick_strike {} {:?} fallback: checked {} strikes, found=false",
                snap.underlying,
                action,
                checked,
            );
            None
        }
    }

    fn strategy_gamma_scalp(&self, snap: &ChainSnapshot) -> Option<(SignalAction, f64, StrategyType, Vec<(String, f64)>)> {
        if snap.days_to_expiry > 2.0 { return None; }
        if snap.atm_iv > 0.55 { return None; }
        // On 1-DTE (day before expiry), block if IV is at session minimum.
        // iv_rank=0 means IV was higher earlier in the session (crash peak) and is now
        // compressing — buying options into falling IV on expensive 1-DTE premium is
        // a double-whammy loss (spot bounce + IV crush). Not applicable on expiry day (DTE<0.5).
        if snap.days_to_expiry >= 0.5 && snap.iv_rank < 5.0 { return None; }

        let session = self.current_session();
        if !matches!(session, SessionPhase::Morning | SessionPhase::Midday | SessionPhase::Afternoon) { return None; }

        let mut score = 0.0_f64;
        let mut reasons = Vec::new();

        if snap.days_to_expiry < 0.5 {
            score += 25.0;
            reasons.push((format!("Same-day expiry ({:.1} DTE): gamma at maximum", snap.days_to_expiry), 25.0));
        } else if snap.days_to_expiry < 1.5 {
            score += 18.0;
            reasons.push((format!("{:.1} DTE: high gamma acceleration", snap.days_to_expiry), 18.0));
        } else {
            score += 10.0;
            reasons.push((format!("{:.1} DTE: moderate gamma", snap.days_to_expiry), 10.0));
        }

        // On expiry day (DTE < 0.5), gamma dominates even moderate PCR bias.
        // Relax thresholds so a slight lean contributes to composite scoring.
        // Score 18 (not 22) because conviction is lower than extreme PCR; composite still clears 62.
        let (action, pcr_score, pcr_reason) = if snap.days_to_expiry < 0.5 {
            if snap.pcr_oi > 1.15 {
                (SignalAction::BuyCE, 18.0,
                 format!("PCR {:.2} (mildly bullish): expiry-day put writing supports CE gamma play", snap.pcr_oi))
            } else if snap.pcr_oi < 0.75 {
                (SignalAction::BuyPE, 18.0,
                 format!("PCR {:.2} (bearish): expiry-day call writing supports PE gamma play", snap.pcr_oi))
            } else {
                return None;
            }
        } else if snap.pcr_oi > 1.3 {
            (SignalAction::BuyCE, 22.0,
             format!("PCR {:.2} (bullish): heavy put writing = market floor", snap.pcr_oi))
        } else if snap.pcr_oi < 0.80 {
            (SignalAction::BuyPE, 22.0,
             format!("PCR {:.2} (bearish): heavy call writing = market ceiling", snap.pcr_oi))
        } else {
            return None;
        };
        score += pcr_score;
        reasons.push((pcr_reason, pcr_score));

        if (action == SignalAction::BuyCE && snap.pcr_vol > 1.1)
        || (action == SignalAction::BuyPE && snap.pcr_vol < 0.9) {
            score += 12.0;
            reasons.push((format!("Volume PCR {:.2} confirms OI signal", snap.pcr_vol), 12.0));
        }

        if snap.atm_iv > 0.12 {
            score += 8.0;
            reasons.push((format!("ATM IV {:.1}% — sufficient for gamma moves", snap.atm_iv * 100.0), 8.0));
        }

        // Individual strategies use a low floor; composite in generate_signals applies min_confidence.
        if score < 25.0 { return None; }
        Some((action, score.min(100.0), StrategyType::GammaScalp, reasons))
    }

    fn strategy_iv_expansion(&self, snap: &ChainSnapshot) -> Option<(SignalAction, f64, StrategyType, Vec<(String, f64)>)> {
        if snap.iv_rank > 40.0 { return None; }
        // iv_rank=0 means no IV history yet (session just started or data gap).
        // Scoring 30 pts as "historically cheap" on an empty baseline is a false signal.
        if snap.iv_rank < 1.0 { return None; }
        if snap.days_to_expiry < 1.0 { return None; }

        let mut score = 0.0_f64;
        let mut reasons = Vec::new();

        if snap.iv_rank < 15.0 {
            score += 30.0;
            reasons.push((format!("IV Rank {:.0}%: historically cheap options (buy aggresively)", snap.iv_rank), 30.0));
        } else if snap.iv_rank < 25.0 {
            score += 22.0;
            reasons.push((format!("IV Rank {:.0}%: below average IV (good buy zone)", snap.iv_rank), 22.0));
        } else {
            score += 12.0;
            reasons.push((format!("IV Rank {:.0}%: moderately cheap", snap.iv_rank), 12.0));
        }

        let action = match snap.regime {
            MarketRegime::StrongBullish | MarketRegime::Bullish => {
                score += 20.0;
                reasons.push(("Bullish regime: buy CE for directional IV play".into(), 20.0));
                SignalAction::BuyCE
            }
            MarketRegime::StrongBearish | MarketRegime::Bearish => {
                score += 20.0;
                reasons.push(("Bearish regime: buy PE for directional IV play".into(), 20.0));
                SignalAction::BuyPE
            }
            // PANIC: IV is elevated but if iv_rank is low (IV hasn't expanded from session start),
            // options are still relatively cheap vs recent range. Use PCR for direction.
            MarketRegime::PanicHighVol => {
                if snap.pcr_oi > 1.1 {
                    score += 10.0;
                    reasons.push(("Panic + low IV rank: options cheap vs session range, PCR bullish → CE".into(), 10.0));
                    SignalAction::BuyCE
                } else if snap.pcr_oi < 0.8 {
                    score += 10.0;
                    reasons.push(("Panic + low IV rank: options cheap vs session range, PCR bearish → PE".into(), 10.0));
                    SignalAction::BuyPE
                } else {
                    return None;
                }
            }
            _ => {
                if snap.pcr_oi > 1.1 {
                    score += 10.0;
                    reasons.push(("PCR slightly bullish, CE preferred for IV expansion".into(), 10.0));
                    SignalAction::BuyCE
                } else {
                    return None;
                }
            }
        };

        if snap.atm_iv < 0.13 {
            score += 15.0;
            reasons.push((format!("ATM IV {:.1}%: below 13% — IV mean-reversion expected", snap.atm_iv * 100.0), 15.0));
        }

        if snap.days_to_expiry > 3.0 {
            score += 8.0;
            reasons.push((format!("{:.0} DTE: time for IV to build before expiry", snap.days_to_expiry), 8.0));
        }

        if score < 25.0 { return None; }
        Some((action, score.min(100.0), StrategyType::IVExpansion, reasons))
    }

    fn strategy_spot_reversal(&self, snap: &ChainSnapshot) -> Option<(SignalAction, f64, StrategyType, Vec<(String, f64)>)> {
        let session = self.current_session();
        if matches!(session, SessionPhase::OpeningBell | SessionPhase::Closing | SessionPhase::AfterMarket | SessionPhase::PreOpen) {
            return None;
        }

        let reversal = self.spot_series.get(&snap.underlying)?.reversal_break()?;
        let action = match reversal.direction {
            crate::technicals::Direction::Bear => SignalAction::BuyPE,
            crate::technicals::Direction::Bull => SignalAction::BuyCE,
            crate::technicals::Direction::Neutral => return None,
        };

        // Price action can lead OI/PCR, but do not fight a strongly opposite chain.
        if action == SignalAction::BuyPE && snap.pcr_oi > 1.05 {
            return None;
        }
        if action == SignalAction::BuyCE && snap.pcr_oi < 0.95 {
            return None;
        }

        // Avoid buying options at the top of their own session IV range unless the
        // absolute IV regime is genuinely stressed.
        if snap.iv_rank > 75.0 && snap.atm_iv < 0.38 {
            return None;
        }
        if snap.atm_iv <= 0.0 || snap.atm_iv > 0.45 {
            return None;
        }

        let mut score = 0.0_f64;
        let mut reasons = Vec::new();
        let direction_text = match action {
            SignalAction::BuyPE => "rejected recent high",
            SignalAction::BuyCE => "reclaimed from recent low",
            _ => return None,
        };
        score += 38.0;
        reasons.push((
            format!(
                "Spot {} over {} closed bars: {:.2}% move / {:.1} ATR",
                direction_text,
                reversal.lookback_bars,
                reversal.move_pct * 100.0,
                reversal.atr_multiple
            ),
            38.0,
        ));

        let atr_score = (reversal.atr_multiple * 8.0).clamp(12.0, 24.0);
        score += atr_score;
        reasons.push((
            format!("Price-action break strength {:.1} ATR", reversal.atr_multiple),
            atr_score,
        ));

        if snap.iv_rank >= 1.0 && snap.iv_rank < 65.0 {
            score += 10.0;
            reasons.push((format!("IV Rank {:.0}%: options still affordable for reversal", snap.iv_rank), 10.0));
        }

        if snap.atm_iv < 0.16 {
            score += 8.0;
            reasons.push((format!("ATM IV {:.1}%: premium not structurally rich", snap.atm_iv * 100.0), 8.0));
        }

        match action {
            SignalAction::BuyPE if snap.pcr_oi < 1.0 => {
                score += 8.0;
                reasons.push((format!("PCR {:.2}: not opposing bearish reversal", snap.pcr_oi), 8.0));
            }
            SignalAction::BuyCE if snap.pcr_oi > 1.0 => {
                score += 8.0;
                reasons.push((format!("PCR {:.2}: not opposing bullish reversal", snap.pcr_oi), 8.0));
            }
            _ => {
                score += 4.0;
                reasons.push((format!("PCR {:.2}: neutral enough for price-action reversal", snap.pcr_oi), 4.0));
            }
        }

        if score < self.min_confidence {
            return None;
        }
        Some((action, score.min(100.0), StrategyType::SpotReversal, reasons))
    }

    fn strategy_trend_follow(&self, snap: &ChainSnapshot) -> Option<(SignalAction, f64, StrategyType, Vec<(String, f64)>)> {
        let mut score = 0.0_f64;
        let mut reasons = Vec::new();

        let atm_idx = snap.strikes.iter()
            .enumerate()
            .min_by(|(_, a), (_, b)| {
                (a.strike - snap.spot).abs().partial_cmp(&(b.strike - snap.spot).abs()).unwrap()
            })
            .map(|(i, _)| i)
            .unwrap_or(0);

        // Use ±4 window (same 9-strike window as OI Divergence) so OI changes captured
        // at the chain periphery are visible to TrendFollow as well.
        let lo = atm_idx.saturating_sub(4);
        let hi = (atm_idx + 5).min(snap.strikes.len());
        let nearby = &snap.strikes[lo..hi];

        let ce_oi_change: i64 = nearby.iter().map(|s| s.ce_oi_change).sum();
        let pe_oi_change: i64 = nearby.iter().map(|s| s.pe_oi_change).sum();

        // Price/OI quadrant: regime provides the price-direction axis so we can resolve
        // whether an OI build is speculator-driven or writer-driven.
        //   CE OI UP + price UP   (Bullish)  = call buyers entering      → BuyCE  (long buildup)
        //   PE OI UP + price DOWN (Bearish)  = put buyers entering        → BuyPE  (short buildup)
        //   CE OI DOWN + price DOWN (Bearish) = call longs unwinding      → BuyPE  (confirms trend)
        //   PE OI DOWN + price UP  (Bullish)  = put shorts covering       → BuyCE  (confirms trend)
        // This is consistent with strategy_oi_divergence and the writer-dominance convention
        // in quant_engine (quant lacks regime so defaults to writer assumption for all cases).
        let (action, oi_score, oi_reason) = if ce_oi_change > 0 && matches!(snap.regime, MarketRegime::Bullish | MarketRegime::StrongBullish) {
            (SignalAction::BuyCE, 25.0,
             format!("CE OI +{} with bullish regime: new long buildup", ce_oi_change))
        } else if pe_oi_change > 0 && matches!(snap.regime, MarketRegime::Bearish | MarketRegime::StrongBearish) {
            (SignalAction::BuyPE, 25.0,
             format!("PE OI +{} with bearish regime: new short buildup", pe_oi_change))
        } else if ce_oi_change < 0 && matches!(snap.regime, MarketRegime::Bearish | MarketRegime::StrongBearish) {
            (SignalAction::BuyPE, 18.0,
             format!("CE OI {} (unwinding) + bearish: longs exiting, further down", ce_oi_change))
        } else if pe_oi_change < 0 && matches!(snap.regime, MarketRegime::Bullish | MarketRegime::StrongBullish) {
            (SignalAction::BuyCE, 18.0,
             format!("PE OI {} (unwinding) + bullish: bears exiting, further up", pe_oi_change))
        // PANIC: sustained fear → OI activity is directionally bearish regardless of which side builds.
        // CE OI up = call writers piling on; PE OI up = put buyers hedging; CE OI down = longs fleeing.
        } else if matches!(snap.regime, MarketRegime::PanicHighVol) && (ce_oi_change > 0 || pe_oi_change > 0 || ce_oi_change < 0) {
            let mag = pe_oi_change.abs().max(ce_oi_change.abs());
            let reason = if pe_oi_change > ce_oi_change.abs() as i64 {
                format!("PE OI +{} in panic: institutional hedging confirms bearish", pe_oi_change)
            } else if ce_oi_change > 0 {
                format!("CE OI +{} in panic: call writers aggressively positioning short", ce_oi_change)
            } else {
                format!("CE OI {} in panic: longs unwinding, further downside", ce_oi_change)
            };
            if mag < 5000 { return None; }
            (SignalAction::BuyPE, 22.0, reason)
        } else {
            return None;
        };
        score += oi_score;
        reasons.push((oi_reason, oi_score));

        let ce_vol_surge: u64 = nearby.iter().map(|s| s.ce_volume).sum();
        let pe_vol_surge: u64 = nearby.iter().map(|s| s.pe_volume).sum();
        // Two-tier volume confirmation: strong (>2x) or moderate (>1.3x)
        if action == SignalAction::BuyCE && ce_vol_surge > pe_vol_surge * 2 {
            score += 15.0;
            reasons.push((format!("CE volume ({}) >> PE volume ({}): strong buying pressure", ce_vol_surge, pe_vol_surge), 15.0));
        } else if action == SignalAction::BuyCE && ce_vol_surge * 10 > pe_vol_surge * 13 {
            score += 8.0;
            reasons.push((format!("CE volume ({}) > PE volume ({}): moderate buying pressure", ce_vol_surge, pe_vol_surge), 8.0));
        } else if action == SignalAction::BuyPE && pe_vol_surge > ce_vol_surge * 2 {
            score += 15.0;
            reasons.push((format!("PE volume ({}) >> CE volume ({}): strong selling pressure", pe_vol_surge, ce_vol_surge), 15.0));
        } else if action == SignalAction::BuyPE && pe_vol_surge * 10 > ce_vol_surge * 13 {
            score += 8.0;
            reasons.push((format!("PE volume ({}) > CE volume ({}): moderate selling pressure", pe_vol_surge, ce_vol_surge), 8.0));
        }

        if snap.iv_rank < 60.0 {
            score += 10.0;
            reasons.push((format!("IV Rank {:.0}%: options not overpriced", snap.iv_rank), 10.0));
        }

        match (&action, &snap.regime) {
            (SignalAction::BuyCE, MarketRegime::StrongBullish) => {
                score += 20.0;
                reasons.push(("Strong bullish regime confirms CE buy".into(), 20.0));
            }
            (SignalAction::BuyCE, MarketRegime::Bullish) => {
                score += 12.0;
                reasons.push(("Bullish regime with OI + volume confirmation".into(), 12.0));
            }
            (SignalAction::BuyPE, MarketRegime::StrongBearish) => {
                score += 20.0;
                reasons.push(("Strong bearish regime confirms PE buy".into(), 20.0));
            }
            (SignalAction::BuyPE, MarketRegime::Bearish) => {
                score += 12.0;
                reasons.push(("Bearish regime with OI + volume confirmation".into(), 12.0));
            }
            (SignalAction::BuyPE, MarketRegime::PanicHighVol) => {
                score += 15.0;
                reasons.push(("Panic regime confirms PE trend direction".into(), 15.0));
            }
            _ => {}
        }

        if score < 25.0 { return None; }
        Some((action, score.min(100.0), StrategyType::TrendFollow, reasons))
    }

    fn strategy_max_pain(&self, snap: &ChainSnapshot) -> Option<(SignalAction, f64, StrategyType, Vec<(String, f64)>)> {
        if snap.days_to_expiry > 3.5 { return None; }

        let deviation = snap.spot - snap.max_pain;
        let deviation_pct = deviation.abs() / snap.spot * 100.0;

        let min_dev_pct = self.dynamic_max_pain_min_dev_pct(
            &snap.underlying,
            snap.atm_iv,
            snap.days_to_expiry,
        );
        if deviation_pct < min_dev_pct { return None; }

        let mut score = 0.0_f64;
        let mut reasons = Vec::new();

        let action = if deviation > 0.0 { SignalAction::BuyPE } else { SignalAction::BuyCE };
        let direction_text = if deviation > 0.0 {
            format!(
                "Spot ₹{:.0} is {:.1}% ABOVE max pain ₹{:.0} (> {:.2}% dyn threshold) → downside gravity",
                snap.spot, deviation_pct, snap.max_pain, min_dev_pct
            )
        } else {
            format!(
                "Spot ₹{:.0} is {:.1}% BELOW max pain ₹{:.0} (> {:.2}% dyn threshold) → upside gravity",
                snap.spot, deviation_pct, snap.max_pain, min_dev_pct
            )
        };

        let dev_score = (deviation_pct * 10.0).min(35.0);
        score += dev_score;
        reasons.push((direction_text, dev_score));

        if snap.days_to_expiry < 1.0 {
            score += 20.0;
            reasons.push((format!("{:.1} DTE: max pain effect strongest on expiry day", snap.days_to_expiry), 20.0));
        } else if snap.days_to_expiry < 2.0 {
            score += 12.0;
            reasons.push((format!("{:.1} DTE: max pain pull building", snap.days_to_expiry), 12.0));
        } else {
            score += 5.0;
            reasons.push((format!("{:.1} DTE: early max pain signal", snap.days_to_expiry), 5.0));
        }

        if snap.net_gex < 0.0 {
            score += 15.0;
            reasons.push((format!("GEX {:.2e} negative: dealer short gamma amplifies max pain pull", snap.net_gex), 15.0));
        }

        if (action == SignalAction::BuyPE && snap.pcr_oi < 1.0)
        || (action == SignalAction::BuyCE && snap.pcr_oi > 1.0) {
            score += 10.0;
            reasons.push((format!("PCR {:.2} aligns with max pain direction", snap.pcr_oi), 10.0));
        }

        if score < 25.0 { return None; }
        Some((action, score.min(100.0), StrategyType::MaxPainConvergence, reasons))
    }

    fn strategy_oi_divergence(&self, snap: &ChainSnapshot) -> Option<(SignalAction, f64, StrategyType, Vec<(String, f64)>)> {
        // OI buildup signals play out over hours-to-days; on expiry day the positioning
        // is dominated by MM hedging/unwinds, not genuine directional bets.
        if snap.days_to_expiry < 1.0 { return None; }

        // In neutral regime there is no directional bias — large OI builds are
        // typically market-maker activity (delta hedging) not speculator positioning.
        // Without a directional regime the OI signal has no edge.
        if matches!(snap.regime, MarketRegime::Neutral) { return None; }

        let atm_idx = snap.strikes.iter()
            .enumerate()
            .min_by(|(_, a), (_, b)| {
                (a.strike - snap.spot).abs().partial_cmp(&(b.strike - snap.spot).abs()).unwrap()
            })
            .map(|(i, _)| i)
            .unwrap_or(0);

        let lo = atm_idx.saturating_sub(4);
        let hi = (atm_idx + 5).min(snap.strikes.len());
        let nearby = &snap.strikes[lo..hi];

        let net_ce_oi_change: i64 = nearby.iter().map(|s| s.ce_oi_change).sum();
        let net_pe_oi_change: i64 = nearby.iter().map(|s| s.pe_oi_change).sum();

        let nearby_volume: u64 = nearby.iter().map(|s| s.ce_volume + s.pe_volume).sum();
        if nearby_volume < 10_000u64 { return None; }

        // Price/OI quadrant: raw OI direction alone is ambiguous — a CE OI increase could
        // be call BUYERS (bullish) or call WRITERS (bearish), depending on whether price is
        // rising or falling with it. We resolve the ambiguity using the price regime.
        //   CE dominant + uptrend  → call buyers entering        → BuyCE
        //   CE dominant + downtrend → call writers (hedging down) → BuyPE
        //   PE dominant + downtrend → put buyers entering         → BuyPE
        //   PE dominant + uptrend  → put writers (income)        → BuyCE
        let is_ce_dominant = net_ce_oi_change.abs() > net_pe_oi_change.abs();
        let dominant_magnitude = if is_ce_dominant { net_ce_oi_change.abs() } else { net_pe_oi_change.abs() };

        if dominant_magnitude < 1000i64 { return None; }

        let (action, quadrant_desc) = match (is_ce_dominant, &snap.regime) {
            (true, MarketRegime::Bullish | MarketRegime::StrongBullish) =>
                (SignalAction::BuyCE, "CE OI buildup in uptrend: call buyers positioning long"),
            (true, MarketRegime::Bearish | MarketRegime::StrongBearish) =>
                (SignalAction::BuyPE, "CE OI buildup in downtrend: call writers signaling downside"),
            (false, MarketRegime::Bearish | MarketRegime::StrongBearish) =>
                (SignalAction::BuyPE, "PE OI buildup in downtrend: put buyers positioning short"),
            (false, MarketRegime::Bullish | MarketRegime::StrongBullish) =>
                (SignalAction::BuyCE, "PE OI buildup in uptrend: put writers signaling further upside"),
            // In PANIC, massive PE OI buildup = institutional panic hedging = confirmed bearish.
            // CE OI buildup in PANIC = call writers aggressively piling on the downside.
            // Both unambiguously bearish; DTE constraint is applied at the caller level.
            (false, MarketRegime::PanicHighVol) =>
                (SignalAction::BuyPE, "PE OI buildup in panic: institutional hedgers piling short"),
            (true, MarketRegime::PanicHighVol) =>
                (SignalAction::BuyPE, "CE OI buildup in panic: call writers aggressively hedging downside"),
            _ => return None,
        };

        let sideways = self
            .is_spot_sideways(&snap.underlying, snap.atm_iv, snap.days_to_expiry)
            .map(|(s, _, _, _, _)| s)
            .unwrap_or(false);
        if sideways {
            if dominant_magnitude < 1_000_000i64 {
                return None;
            }
            let pcr_extreme_ok =
                (action == SignalAction::BuyCE && snap.pcr_oi >= 1.05)
                || (action == SignalAction::BuyPE && snap.pcr_oi <= 0.95);
            if !pcr_extreme_ok {
                return None;
            }
        }

        let mut score = 0.0_f64;
        let mut reasons = Vec::new();

        let oi_divisor = 10_000.0;
        let oi_score = (dominant_magnitude as f64 / oi_divisor).min(30.0);
        score += oi_score;
        reasons.push((
            format!("{} (OI Δ {}): {}", if is_ce_dominant { "CE dominant" } else { "PE dominant" },
                dominant_magnitude, quadrant_desc),
            oi_score,
        ));

        // Regime alignment is already baked into the quadrant action above; give a small
        // confirmation bonus when the regime is strong.
        if matches!(snap.regime, MarketRegime::StrongBullish | MarketRegime::StrongBearish) {
            score += 10.0;
            reasons.push((format!("Strong {} regime confirms OI quadrant", snap.regime), 10.0));
        }

        if (action == SignalAction::BuyCE && snap.pcr_oi > 1.1)
        || (action == SignalAction::BuyPE && snap.pcr_oi < 0.9) {
            score += 15.0;
            reasons.push((format!("PCR {:.2} confirms OI divergence direction", snap.pcr_oi), 15.0));
        }

        if snap.iv_rank < 55.0 {
            score += 10.0;
            reasons.push((format!("IV Rank {:.0}%: options still affordable", snap.iv_rank), 10.0));
        }

        let nearby_ce_oi: u64 = nearby.iter().map(|s| s.ce_oi).sum();
        let nearby_pe_oi: u64 = nearby.iter().map(|s| s.pe_oi).sum();
        let total_nearby_oi = (nearby_ce_oi + nearby_pe_oi).max(1) as f64;
        let vol_oi_ratio = nearby_volume as f64 / total_nearby_oi;
        if vol_oi_ratio > 0.20 {
            let vol_score = (vol_oi_ratio * 20.0).min(12.0);
            score += vol_score;
            reasons.push((
                format!("Volume/OI ratio {:.2}: strong real activity behind OI change", vol_oi_ratio),
                vol_score,
            ));
        }

        if score < 25.0 { return None; }
        Some((action, score.min(100.0), StrategyType::OIDivergence, reasons))
    }

    fn strategy_iv_skew(&self, snap: &ChainSnapshot) -> Option<(SignalAction, f64, StrategyType, Vec<(String, f64)>)> {
        let atm_idx = snap.strikes.iter()
            .enumerate()
            .min_by(|(_, a), (_, b)| {
                (a.strike - snap.spot).abs().partial_cmp(&(b.strike - snap.spot).abs()).unwrap()
            })
            .map(|(i, _)| i)
            .unwrap_or(0);

        let lo = atm_idx.saturating_sub(2);
        let hi = (atm_idx + 3).min(snap.strikes.len());
        let atm_skews: Vec<f64> = snap.strikes[lo..hi]
            .iter()
            .filter(|s| s.iv_skew.is_finite() && s.ce_iv > 0.0 && s.pe_iv > 0.0)
            .map(|s| s.iv_skew)
            .collect();

        if atm_skews.is_empty() { return None; }
        let avg_skew = atm_skews.iter().sum::<f64>() / atm_skews.len() as f64;

        let mut score = 0.0_f64;
        let mut reasons = Vec::new();

        let action = if avg_skew < -0.08 {
            let skew_score = (avg_skew.abs() * 200.0).min(35.0);
            score += skew_score;
            reasons.push((
                format!("IV skew {:.1}%: put IV >> call IV by {:.1}% — peak fear, contrarian CE opportunity",
                    avg_skew * 100.0, avg_skew.abs() * 100.0),
                skew_score,
            ));
            SignalAction::BuyCE
        } else if avg_skew > 0.06 {
            let skew_score = (avg_skew * 200.0).min(35.0);
            score += skew_score;
            reasons.push((
                format!("IV skew +{:.1}%: call IV >> put IV — extreme greed/squeeze, PE opportunity",
                    avg_skew * 100.0),
                skew_score,
            ));
            SignalAction::BuyPE
        } else {
            return None;
        };

        if (action == SignalAction::BuyCE && snap.pcr_oi > 1.2)
        || (action == SignalAction::BuyPE && snap.pcr_oi < 0.8) {
            score += 20.0;
            reasons.push((format!("PCR {:.2} confirms skew reversion direction", snap.pcr_oi), 20.0));
        }

        if snap.days_to_expiry > 2.0 {
            score += 8.0;
            reasons.push((format!("{:.0} DTE: sufficient time for skew to revert", snap.days_to_expiry), 8.0));
        }

        if score < 25.0 { return None; }
        Some((action, score.min(100.0), StrategyType::IVSkewReversion, reasons))
    }

    fn strategy_gex_play(&self, snap: &ChainSnapshot) -> Option<(SignalAction, f64, StrategyType, Vec<(String, f64)>)> {
        // GEX sign (positive vs negative) cannot be reliably determined from OI + Black-Scholes
        // gamma alone — it would require knowing whether dealers are long or short each strike,
        // which is not observable from public OI data. Use magnitude only; direction from regime.
        let gex_magnitude = snap.net_gex.abs();
        if gex_magnitude < 1e6 { return None; }

        let mut score = 0.0_f64;
        let mut reasons = Vec::new();

        let gex_score = (gex_magnitude.log10() - 5.0).max(0.0) * 10.0;
        let clamped = gex_score.min(30.0);
        score += clamped;
        reasons.push((
            format!("Net GEX magnitude {:.2e}: high gamma environment, amplified moves likely", gex_magnitude),
            clamped,
        ));

        let action = match snap.regime {
            MarketRegime::StrongBullish | MarketRegime::Bullish => {
                score += 20.0;
                reasons.push(("Bullish regime in high-gamma environment: explosive upside possible".into(), 20.0));
                SignalAction::BuyCE
            }
            MarketRegime::StrongBearish | MarketRegime::Bearish => {
                score += 20.0;
                reasons.push(("Bearish regime in high-gamma environment: explosive downside possible".into(), 20.0));
                SignalAction::BuyPE
            }
            // PANIC: sustained fear + high gamma = explosive downside. Direction is clear (bearish).
            MarketRegime::PanicHighVol => {
                score += 15.0;
                reasons.push(("Panic regime in high-gamma environment: explosive downside continuation".into(), 15.0));
                SignalAction::BuyPE
            }
            _ => {
                if snap.pcr_oi > 1.2 {
                    score += 10.0;
                    reasons.push((format!("PCR {:.2}: CE direction for GEX play", snap.pcr_oi), 10.0));
                    SignalAction::BuyCE
                } else if snap.pcr_oi < 0.8 {
                    score += 10.0;
                    reasons.push((format!("PCR {:.2}: PE direction for GEX play", snap.pcr_oi), 10.0));
                    SignalAction::BuyPE
                } else {
                    return None;
                }
            }
        };

        if snap.iv_rank >= 1.0 && snap.iv_rank < 65.0 {
            score += 10.0;
            reasons.push((format!("IV Rank {:.0}%: options still reasonably priced for GEX play", snap.iv_rank), 10.0));
        }

        // PCR confirmation lifts GEX signal above min_confidence threshold in directional regimes
        if (action == SignalAction::BuyCE && snap.pcr_oi > 1.1)
        || (action == SignalAction::BuyPE && snap.pcr_oi < 0.9) {
            score += 8.0;
            reasons.push((format!("PCR {:.2} confirms GEX play direction", snap.pcr_oi), 8.0));
        }

        if score < 25.0 { return None; }
        Some((action, score.min(100.0), StrategyType::GEXPlay, reasons))
    }

    fn dynamic_daily_trade_cap(&self, signal: &Signal) -> u32 {
        let ctx = &signal.entry_ctx;
        let mut cap: u32 = match self.risk.capital_tier() {
            1 => 6,
            2 => 10,
            _ => 14,
        };

        if ctx.regime.contains("SIDEWAYS") {
            cap = cap.min(4);
        }

        if ctx.iv_rank >= 75.0 || ctx.atm_iv_pct >= 22.0 {
            cap = cap.saturating_sub(2);
        }

        if ctx.confidence >= 90.0 {
            cap += 1;
        }

        if ctx.days_to_expiry <= 1.0 && !ctx.regime.contains("SIDEWAYS") {
            cap += 1;
        }

        if ctx.session.contains("Midday") {
            cap = cap.saturating_sub(1);
        }

        let drawdown_frac = if self.risk.initial_capital > 0.0 {
            (self.risk.initial_capital - self.risk.capital).max(0.0) / self.risk.initial_capital
        } else {
            0.0
        };
        if drawdown_frac >= 0.10 {
            cap = ((cap as f64) * 0.60).floor() as u32;
        }

        cap.clamp(2, 18)
    }


    fn open_simulated_position(&mut self, signal: &Signal) {
        self.entry_attempts += 1;
        self.reset_daily_state_if_needed();
        let used_slots = self.daily_trade_slots_used();
        // dynamic_daily_trade_cap adjusts the per-day limit by tier/session/conditions;
        // max_daily_trades (from config) serves as the hard safety ceiling. A config
        // value of 0 means "no static count cap"; use the dynamic cap only.
        let dynamic_cap = self.dynamic_daily_trade_cap(signal);
        let effective_cap = self.effective_daily_trade_cap(signal);
        if used_slots >= effective_cap {
            self.entry_rejections += 1;
            warn!(
                "Position rejected (daily trade cap): used {} / {} (dynamic={}, config_max={})",
                used_slots, effective_cap,
                dynamic_cap,
                self.max_daily_trades
            );
            return;
        }

        let (token, tradingsymbol) = match signal.action {
            SignalAction::BuyCE => {
                self.contracts
                    .iter()
                    .find(|c| {
                        c.underlying == signal.underlying
                            && c.expiry == signal.expiry
                            && c.strike == signal.strike
                            && c.option_type == OptionType::CE
                    })
                    .map(|c| (c.instrument_token, c.tradingsymbol.clone()))
                    .unwrap_or((0, String::new()))
            }
            SignalAction::BuyPE => {
                self.contracts
                    .iter()
                    .find(|c| {
                        c.underlying == signal.underlying
                            && c.expiry == signal.expiry
                            && c.strike == signal.strike
                            && c.option_type == OptionType::PE
                    })
                    .map(|c| (c.instrument_token, c.tradingsymbol.clone()))
                    .unwrap_or((0, String::new()))
            }
            _ => (0, String::new()),
        };

        if token == 0 {
            self.entry_rejections += 1;
            return;
        }

        if self.active_slot_count() >= self.max_concurrent_positions {
            self.entry_rejections += 1;
            warn!(
                "Position rejected (concurrency cap): active {} / {}",
                self.active_slot_count(),
                self.max_concurrent_positions
            );
            return;
        }

        if self.positions.iter().any(|p| p.is_open && p.underlying == signal.underlying) {
            self.entry_rejections += 1;
            return;
        }
        if self
            .pending_entry_orders
            .iter()
            .any(|p| !p.released_after_timeout && p.signal.underlying == signal.underlying)
        {
            self.entry_rejections += 1;
            return;
        }

        if self.positions.iter().any(|p| p.is_open && p.option_token == token) {
            self.entry_rejections += 1;
            return;
        }
        if self
            .pending_entry_orders
            .iter()
            .any(|p| !p.released_after_timeout && p.token == token)
        {
            self.entry_rejections += 1;
            return;
        }

        let entry_fill_price = if self.live_mode_enabled() {
            self.live_entry_limit_price(token, signal.option_price)
        } else {
            (signal.option_price + self.execution_buy_offset_inr).max(0.05)
        };
        let entry_costs = entry_order_cost(entry_fill_price, signal.lot_size, signal.lots);
        let capital_deployed = entry_fill_price * signal.lot_size as f64 * signal.lots as f64
            + entry_costs;

        if self.live_mode_enabled() {
            if capital_deployed > self.available_capital_for_new_orders() {
                self.entry_rejections += 1;
                warn!(
                    "Entry order skipped (budget): need ₹{:.2}, available-for-new ₹{:.2}",
                    capital_deployed,
                    self.available_capital_for_new_orders()
                );
                return;
            }
            // The atomic global-slot claim happens inside submit_live_entry_order,
            // after its quantity/symbol guard, so a claim ALWAYS pairs with a
            // registered pending entry (no leaked lock on an early return).
            self.submit_live_entry_order(
                signal,
                token,
                tradingsymbol,
                capital_deployed,
                entry_fill_price,
            );
            return;
        }

        let entry_datetime = datetime_string_from_ms(self.current_time_ms());
        if !self.risk.reserve_capital(capital_deployed) {
            self.entry_rejections += 1;
            warn!(
                "Position rejected (capital): required ₹{:.2}, available ₹{:.2}",
                capital_deployed,
                self.risk.available_capital()
            );
            return;
        }

        // Claim the single global position slot (atomic) before committing the paper
        // fill. If another engine holds it, give the capital back and cancel.
        if let Some(c) = &self.shared_circuit {
            if !crate::portfolio::try_claim(c, "options") {
                self.risk.release_reserved_capital(capital_deployed);
                self.entry_rejections += 1;
                warn!("Entry cancelled: global position slot held elsewhere");
                return;
            }
        }

        self.last_signal_ms.insert(signal.underlying.clone(), self.current_time_ms());

        let orig_entry = signal.option_price.max(0.01);
        let target_pct = (signal.target_price / orig_entry - 1.0).max(0.0);
        let stop_pct   = (1.0 - signal.stop_price / orig_entry).clamp(0.01, 0.95);
        let adjusted_target = entry_fill_price * (1.0 + target_pct);
        let adjusted_stop   = entry_fill_price * (1.0 - stop_pct);

        let pos_id = { self.next_pos_id += 1; self.next_pos_id - 1 };
        let pos = Position {
            id: pos_id,
            underlying: signal.underlying.clone(),
            tradingsymbol: tradingsymbol.clone(),
            action: signal.action.clone(),
            strike: signal.strike,
            expiry: signal.expiry.clone(),
            option_token: token,
            lots: signal.lots,
            lot_size: signal.lot_size,
            entry_price: entry_fill_price,
            target_price: adjusted_target,
            stop_price: adjusted_stop,
            entry_time_ms: self.current_time_ms(),
            entry_datetime: entry_datetime.clone(),
            strategy: signal.strategy,
            is_open: true,
            exit_price: 0.0,
            exit_time_ms: 0,
            pnl: 0.0,
            pnl_pct: 0.0,
            exit_reason: String::new(),
            capital_deployed,
            entry_ctx: signal.entry_ctx.clone(),
            breakeven_stop_set: false,
            exit_pending: false,
            checkpoints_evaluated: 0,
        };

        info!("📂 POSITION OPENED #{} | {} {} {:.0} {} | {}×{} lots | Entry: ₹{:.2} | Capital: ₹{:.2}",
            pos.id, pos.underlying, signal.action, pos.strike, pos.expiry,
            pos.lots, pos.lot_size, pos.entry_price, capital_deployed);
        let actual_target_pct = (pos.target_price / pos.entry_price - 1.0) * 100.0;
        info!("   Target: ₹{:.2} (+{:.0}%) | Stop: ₹{:.2} (-{:.0}%)",
            pos.target_price, actual_target_pct,
            pos.stop_price, self.stop_loss_pct);
        info!("   Ledger: signals log updated (signal #{}, strategy: {})",
            signal.id, signal.strategy);

        self.positions.push(pos);
        self.positions_opened += 1;
        self.daily_trades_opened += 1;
    }

    fn submit_live_entry_order(
        &mut self,
        signal: &Signal,
        token: u32,
        tradingsymbol: String,
        estimated_capital: f64,
        limit_price: f64,
    ) {
        let quantity = signal.lots.saturating_mul(signal.lot_size);
        if quantity == 0 || tradingsymbol.is_empty() {
            self.entry_rejections += 1;
            return;
        }

        // Claim the single global position slot (atomic) right before registering the
        // order, so a successful claim ALWAYS pairs with a pending entry that
        // release_pending_entry / promote can later release. If another engine holds
        // it, cancel — one open trade at a time across the account.
        if let Some(c) = &self.shared_circuit {
            if !crate::portfolio::try_claim(c, "options") {
                self.entry_rejections += 1;
                warn!("Entry cancelled: global position slot held elsewhere");
                return;
            }
        }

        let pos_id = { self.next_pos_id += 1; self.next_pos_id - 1 };
        let tag = self.build_order_tag("ENTRY", pos_id);
        let now = self.current_time_ms();
        self.pending_capital_reserved += estimated_capital;
        self.pending_entry_orders.push(PendingEntryOrder {
            pos_id,
            signal: signal.clone(),
            token,
            tradingsymbol: tradingsymbol.clone(),
            estimated_capital,
            tag: tag.clone(),
            placed_ms: now,
            last_status_poll_ms: 0,
            cancel_requested: false,
            cancel_reason: None,
            last_cancel_attempt_ms: 0,
            released_after_timeout: false,
            order_id: None,
            signal_price: limit_price,
            placed_at_scan_count: self.scan_count,
        });
        self.last_signal_ms.insert(signal.underlying.clone(), now);

        info!(
            "📨 ENTRY ORDER SUBMITTED | tag={} | {} {} {:.0} {} | qty={} | limit ₹{:.2} (current+₹{:.2}) | est capital ₹{:.2}",
            tag,
            signal.underlying,
            signal.action,
            signal.strike,
            signal.expiry,
            quantity,
            limit_price,
            LIVE_ENTRY_LIMIT_PAYUP_INR,
            estimated_capital
        );
        self.send_order_command(OrderCommand::Place(PlaceOrderCmd {
            tag: tag.clone(),
            tradingsymbol,
            quantity,
            side: OrderSide::Buy,
            limit_price: Some(limit_price),
        }));
        self.send_order_command(OrderCommand::StatusByTag { tag });
    }

    fn open_filled_position_from_pending(
        &mut self,
        pending: PendingEntryOrder,
        broker_avg_price: f64,
    ) -> bool {
        let signal = pending.signal;
        let fallback_price = self
            .store
            .get(pending.token)
            .map(|t| t.ltp)
            .filter(|p| *p > 0.0)
            .unwrap_or(signal.option_price);
        let entry_fill_price = if broker_avg_price > 0.0 {
            broker_avg_price
        } else {
            (fallback_price + self.execution_buy_offset_inr).max(0.05)
        };
        let entry_datetime = datetime_string_from_ms(self.current_time_ms());
        let entry_costs = entry_order_cost(entry_fill_price, signal.lot_size, signal.lots);
        let capital_deployed = entry_fill_price * signal.lot_size as f64 * signal.lots as f64
            + entry_costs;
        if !self.risk.reserve_capital(capital_deployed) {
            self.entry_rejections += 1;
            // This path should be rare with Issue 1 fixed (capital held until
            // broker confirmation), but can still occur if risk capital was genuinely
            // exhausted by a concurrent position. Place an emergency MARKET SELL and
            // track it in pending_exit_orders so status is polled and failures are
            // visible.  pos_id has no matching Position — handle_exit_order_update
            // will log ERROR if the SELL itself fails.
            error!(
                "EMERGENCY FLATTEN: filled BUY but capital exhausted. tag={} deployed ₹{:.2} — placing MARKET SELL",
                pending.tag, capital_deployed
            );
            let exit_tag = self.build_order_tag("EXIT", pending.pos_id);
            let quantity = signal.lots.saturating_mul(signal.lot_size);
            if quantity > 0 {
                let now_ms = self.current_time_ms();
                let exit_symbol = pending.tradingsymbol.clone();
                self.send_order_command(OrderCommand::Place(PlaceOrderCmd {
                    tag: exit_tag.clone(),
                    tradingsymbol: exit_symbol.clone(),
                    quantity,
                    side: OrderSide::Sell,
                    limit_price: None,
                }));
                // Track via PendingExitOrder so status is polled and rejections surface.
                self.pending_exit_orders.push(PendingExitOrder {
                    pos_id: pending.pos_id, // no matching Position; handled below
                    tag: exit_tag.clone(),
                    tradingsymbol: exit_symbol,
                    total_quantity: quantity,
                    placed_ms: now_ms,
                    last_status_poll_ms: 0,
                    reason: "EMERGENCY FLATTEN".to_string(),
                    cancel_requested: false,
                    last_cancel_attempt_ms: 0,
                    market_fallback_sent: false,
                    fallback_book_price: entry_fill_price,
                    total_filled_quantity: 0,
                    total_filled_notional: 0.0,
                    current_order_filled_quantity: 0,
                    current_order_filled_notional: 0.0,
                });
                self.send_order_command(OrderCommand::StatusByTag { tag: exit_tag });
            } else if let Some(c) = &self.shared_circuit {
                if !self.has_live_commitment() {
                    crate::portfolio::release(c, "options");
                }
            }
            return false;
        }

        let orig_entry = signal.option_price.max(0.01);
        let target_pct = (signal.target_price / orig_entry - 1.0).max(0.0);
        let stop_pct = (1.0 - signal.stop_price / orig_entry).clamp(0.01, 0.95);
        let adjusted_target = entry_fill_price * (1.0 + target_pct);
        let adjusted_stop = entry_fill_price * (1.0 - stop_pct);

        let pos = Position {
            id: pending.pos_id,
            underlying: signal.underlying.clone(),
            tradingsymbol: pending.tradingsymbol,
            action: signal.action.clone(),
            strike: signal.strike,
            expiry: signal.expiry.clone(),
            option_token: pending.token,
            lots: signal.lots,
            lot_size: signal.lot_size,
            entry_price: entry_fill_price,
            target_price: adjusted_target,
            stop_price: adjusted_stop,
            entry_time_ms: self.current_time_ms(),
            entry_datetime,
            strategy: signal.strategy,
            is_open: true,
            exit_price: 0.0,
            exit_time_ms: 0,
            pnl: 0.0,
            pnl_pct: 0.0,
            exit_reason: String::new(),
            capital_deployed,
            entry_ctx: signal.entry_ctx.clone(),
            breakeven_stop_set: false,
            exit_pending: false,
            checkpoints_evaluated: 0,
        };

        self.last_signal_ms.insert(signal.underlying.clone(), self.current_time_ms());
        self.positions.push(pos);
        self.positions_opened += 1;
        self.daily_trades_opened += 1;
        info!(
            "✅ BUY FILLED -> POSITION OPEN #{} | {} {} {:.0} | Entry ₹{:.2}",
            pending.pos_id,
            signal.underlying,
            signal.action,
            signal.strike,
            entry_fill_price
        );
        true
    }

    fn close_position_at(&mut self, idx: usize, current_price: f64, reason: String) {
        let live_mode = self.live_mode_enabled();
        let (quantity, tradingsymbol, pos_id) = {
            let Some(pos) = self.positions.get_mut(idx) else { return; };
            if !pos.is_open || pos.exit_pending { return; }
            if live_mode {
                pos.exit_pending = true;
                pos.exit_reason = reason.clone();
                let q = pos.lots.saturating_mul(pos.lot_size);
                (q, pos.tradingsymbol.clone(), pos.id)
            } else {
                (0, String::new(), pos.id)
            }
        };

        if live_mode {
            if quantity > 0 && !tradingsymbol.is_empty() {
                let exit_tag = self.build_order_tag("EXIT", pos_id);
                self.send_order_command(OrderCommand::Place(PlaceOrderCmd {
                    tag: exit_tag.clone(),
                    tradingsymbol: tradingsymbol.clone(),
                    quantity,
                    side: OrderSide::Sell,
                    limit_price: Some(current_price),
                }));
                self.send_order_command(OrderCommand::StatusByTag { tag: exit_tag.clone() });
                
                self.pending_exit_orders.push(PendingExitOrder {
                    pos_id,
                    tag: exit_tag,
                    tradingsymbol,
                    total_quantity: quantity,
                    placed_ms: self.current_time_ms(),
                    last_status_poll_ms: self.current_time_ms(),
                    reason,
                    cancel_requested: false,
                    last_cancel_attempt_ms: 0,
                    market_fallback_sent: false,
                    fallback_book_price: current_price,
                    total_filled_quantity: 0,
                    total_filled_notional: 0.0,
                    current_order_filled_quantity: 0,
                    current_order_filled_notional: 0.0,
                });
            } else {
                self.finalize_position_close(idx, current_price, reason);
            }
        } else {
            let exit_fill_price = (current_price - self.execution_sell_offset_inr).max(0.05);
            self.finalize_position_close(idx, exit_fill_price, reason);
        }
    }

    fn finalize_position_close(&mut self, idx: usize, exit_fill_price: f64, reason: String) {
        let exit_time_ms = self.current_time_ms();
        let should_sync_capital = self.live_mode_enabled();
        let (
            trade_id,
            entry_ctx,
            underlying,
            _tradingsymbol,
            option_type_str,
            strike,
            expiry,
            entry_datetime,
            exit_datetime,
            entry_price,
            exit_price,
            lots,
            lot_size,
            capital_deployed,
            target_price,
            stop_price,
            gross_pnl,
            costs,
            pnl,
            pnl_pct,
            holding_mins,
        ) = {
            let Some(pos) = self.positions.get_mut(idx) else { return; };
            if !pos.is_open { return; }

            pos.is_open = false;
            pos.exit_pending = false;
            pos.exit_price = exit_fill_price;
            pos.exit_time_ms = exit_time_ms;
            pos.exit_reason = reason.clone();
            self.capital_sync_needed = should_sync_capital;

            let gross_pnl = (exit_fill_price - pos.entry_price) * pos.lot_size as f64 * pos.lots as f64;
            let entry_costs = entry_order_cost(pos.entry_price, pos.lot_size, pos.lots);
            let exit_costs = exit_order_cost(exit_fill_price, pos.lot_size, pos.lots, exit_time_ms);
            let costs = entry_costs + exit_costs;
            pos.pnl = gross_pnl - costs;
            pos.pnl_pct = if pos.capital_deployed > 0.0 {
                (pos.pnl / pos.capital_deployed) * 100.0
            } else {
                0.0
            };

            let holding_mins = (pos.exit_time_ms.saturating_sub(pos.entry_time_ms)) as f64 / 60_000.0;
            let option_type_str = match pos.action {
                SignalAction::BuyCE => "CE".to_string(),
                SignalAction::BuyPE => "PE".to_string(),
                _ => "XX".to_string(),
            };

            (
                pos.id, pos.entry_ctx.clone(), pos.underlying.clone(), pos.tradingsymbol.clone(),
                option_type_str, pos.strike, pos.expiry.clone(), pos.entry_datetime.clone(),
                datetime_string_from_ms(pos.exit_time_ms), pos.entry_price, pos.exit_price,
                pos.lots, pos.lot_size, pos.capital_deployed, pos.target_price, pos.stop_price,
                gross_pnl, costs, pos.pnl, pos.pnl_pct, holding_mins,
            )
        };

        let is_stop = reason.starts_with("STOP HIT");
        let is_target = reason.starts_with("TARGET HIT");

        self.risk.release_reserved_capital(capital_deployed);
        self.risk.record_trade(pnl);
        // Feed the account-level circuit (net, post-cost P&L) and FREE the single
        // global position slot — this trade is done, the next signal (any engine)
        // may now claim it. This is the one close chokepoint for paper AND live.
        if let Some(c) = &self.shared_circuit {
            crate::portfolio::record_for(c, "options", pnl);
            crate::portfolio::release(c, "options");
        }
        self.positions_closed += 1;

        if is_stop {
            let next = self.stop_streak_by_underlying.get(&underlying).copied().unwrap_or(0).saturating_add(1);
            self.stop_streak_by_underlying.insert(underlying.clone(), next);
        } else if is_target {
            self.stop_streak_by_underlying.insert(underlying.clone(), 0);
        }

        self.journal.log_trade(
            trade_id,
            &entry_ctx,
            &underlying,
            &option_type_str,
            strike,
            &expiry,
            &entry_datetime,
            &exit_datetime,
            entry_price,
            exit_price,
            lots,
            lot_size,
            capital_deployed,
            target_price,
            stop_price,
            &reason,
            gross_pnl,
            costs,
            pnl,
            pnl_pct,
            self.risk.capital,
            holding_mins,
        );

        info!(
            "   Portfolio Capital: ₹{:.2} | Available: ₹{:.2} | Progress: {:.1}% to ₹50,000",
            self.risk.capital,
            self.risk.available_capital(),
            (self.risk.capital / 50_000.0) * 100.0
        );
    }

    fn check_open_positions(&mut self) {
        // Compute IST minutes once for dynamic closing window logic.
        let close_ist_mins = {
            let now_utc = Utc.timestamp_millis_opt(self.current_time_ms() as i64)
                .single()
                .unwrap_or_else(Utc::now);
            let ist = FixedOffset::east_opt(5 * 3600 + 30 * 60).expect("valid IST offset");
            let now_ist = now_utc.with_timezone(&ist);
            now_ist.hour() * 60 + now_ist.minute()
        };
        // Hard close-all at 15:20 IST regardless of confidence.
        let hard_close_all = close_ist_mins >= 920;

        // Two-stage profit protection (same code path for live + backtest):
        //   • +12% gain (≥3-min hold): one-shot lock — stop → entry+2%, governs the +12–15% zone.
        //   • +15%+ : half-of-peak trail — stop ratchets to (entry+peak)/2, i.e. give back at most
        //     50% of the peak gain before closing (mirrors the multi-leg trail).
        let now_ms = self.current_time_ms();
        for i in 0..self.positions.len() {
            let (is_open, option_token, entry_price, entry_time_ms) = {
                let pos = &self.positions[i];
                (pos.is_open, pos.option_token, pos.entry_price, pos.entry_time_ms)
            };
            if !is_open { continue; }

            let current_price = match self.store.get(option_token) {
                Some(t) if t.ltp > 0.0 => t.ltp,
                _ => continue,
            };
            if entry_price <= 0.0 { continue; }

            // One-shot profit lock — "free-flow" design (replaces the continuous 2.5% trail
            // that chopped winners out on intraday noise before the move developed):
            //   • below +15% gain: NO stop change — the trade rides on its original loss-stop
            //     and is never knocked out by a shallow wobble (the +7% chop that cost the
            //     +116% runner on 2026-06-24).
            //   • at +12% gain (min 3-min hold): make EXACTLY ONE update — stop → entry+2%.
            //     Downside is now capped at +2%; upside stays uncapped up to the strategy
            //     target (~+25%). No further stop moves after this one ("free flow").
            //   • checkpoints_evaluated → 3 so the hold cap extends to 240 min (room to target).
            let hold_mins = (now_ms.saturating_sub(entry_time_ms)) as f64 / 60_000.0;
            let abs_gain_pct = (current_price - entry_price) / entry_price * 100.0;
            if abs_gain_pct >= 12.0 && hold_mins >= 3.0 {
                let pos = &mut self.positions[i];
                if !pos.breakeven_stop_set {
                    let raw_lock = entry_price * 1.02;
                    let target_guard = if pos.target_price > entry_price {
                        (pos.target_price * 0.995).max(entry_price)
                    } else {
                        raw_lock
                    };
                    let new_stop = raw_lock.min(target_guard);
                    if new_stop > pos.stop_price {
                        pos.stop_price = new_stop;
                    }
                    pos.breakeven_stop_set = true;
                    pos.checkpoints_evaluated = 3;
                    info!("  Profit-lock +{:.1}%: pos #{} {} stop → ₹{:.2} (entry+2%), free-flow to target (ltp ₹{:.2})",
                        abs_gain_pct, pos.id, pos.underlying, pos.stop_price, current_price);
                }
            }

            // +15% half-of-peak trail (mirrors the multi-leg trail exactly: arm at 0.15, give
            // back 0.50 of peak). Once the option reaches +15%, ratchet the stop to halfway
            // between entry and the current price = give back at most 50% of the PEAK gain before
            // closing. The `> stop_price` guard makes it a one-way ratchet: it locks the running
            // peak (when current == peak the midpoint is highest and is stored) and never lowers,
            // so a pullback can only hit it, not reset it. Sits ABOVE the +2% lock; the strategy
            // target still caps the upside.
            // No time gate (matches multileg::trail_exits, which arms purely on the peak
            // threshold) — a sub-3-min spike-then-crash is still protected. The +15% trigger is
            // itself the noise filter.
            if abs_gain_pct >= 15.0 {
                let pos = &mut self.positions[i];
                let raw_trail = (entry_price + current_price) / 2.0;
                // Never park the trail on/above a tight target (the target fires first anyway) —
                // same guard the +2% lock uses.
                let trail_stop = if pos.target_price > entry_price {
                    raw_trail.min((pos.target_price * 0.995).max(entry_price))
                } else {
                    raw_trail
                };
                if trail_stop > pos.stop_price {
                    pos.stop_price = trail_stop;
                    pos.checkpoints_evaluated = pos.checkpoints_evaluated.max(3);
                    info!("  Trail +{:.1}% (50%-of-peak): pos #{} {} stop → ₹{:.2} (ltp ₹{:.2})",
                        abs_gain_pct, pos.id, pos.underlying, pos.stop_price, current_price);
                }
            }
        }

        for i in 0..self.positions.len() {
            let (is_open, option_token, target_price, stop_price, confidence, entry_time_ms) = {
                let pos = &self.positions[i];
                (pos.is_open, pos.option_token, pos.target_price, pos.stop_price,
                 pos.entry_ctx.confidence, pos.entry_time_ms)
            };
            if !is_open {
                continue;
            }

            let current_price = match self.store.get(option_token) {
                Some(t) if t.ltp > 0.0 => t.ltp,
                _ => continue,
            };

            // Checkpoint-scaled hold cap: extend runway once the trade proves itself. With the
            // one-shot profit lock, cp is only ever 0 (pre-lock) or 3 (the +15% lock fired);
            // the 1/2 rungs remain for back-compat but are no longer set on the live path.
            //   cp == 0 (no profit locked)  → 120 min (exit a flat trade promptly)
            //   cp >= 1 (legacy)            → 150 min
            //   cp >= 2 (legacy)            → 180 min
            //   cp >= 3 (+12% locked)       → 240 min (let it run to target)
            let cp_now = self.positions[i].checkpoints_evaluated;
            let holding_ms = self.current_time_ms().saturating_sub(entry_time_ms);
            let max_hold_mins: u64 = match cp_now {
                0 => 120,
                1 => 150,
                2 => 180,
                _ => 240,
            };
            let max_hold_expired = holding_ms >= max_hold_mins * 60 * 1_000;

            // Dynamic closing window: higher confidence gets extra time near close.
            //   confidence >= 70 → close at 15:20 (920 mins) — hard_close_all
            //   confidence 65–70 → close at 15:17 (917 mins)
            //   confidence <  65 → close at 15:15 (915 mins, start of Closing phase)
            let time_force_exit = if hard_close_all {
                true
            } else if close_ist_mins >= 917 {
                confidence < 70.0
            } else if close_ist_mins >= 915 {
                confidence < 65.0
            } else {
                false
            };

            let exit_reason = if max_hold_expired {
                Some(format!("Time stop: {}-min max hold ({:.0} min held)", max_hold_mins, holding_ms as f64 / 60_000.0))
            } else if time_force_exit {
                Some(format!("Time stop: market closing ({:02}:{:02} IST, conf {:.0})",
                    close_ist_mins / 60, close_ist_mins % 60, confidence))
            } else if current_price >= target_price {
                Some(format!("TARGET HIT ₹{:.2}", current_price))
            } else if current_price <= stop_price {
                Some(format!("STOP HIT ₹{:.2}", current_price))
            } else {
                None
            };

            if let Some(reason) = exit_reason {
                self.close_position_at(i, current_price, reason);
            }
        }
    }


    fn log_chain_analysis(&self, snap: &ChainSnapshot) {
        info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        info!("📊 CHAIN ANALYSIS | {} | Spot: ₹{:.2} | {}",
            snap.underlying, snap.spot, snap.regime);
        info!("   Expiry: {} ({:.1} DTE) | Session: {}",
            snap.expiry, snap.days_to_expiry, self.current_session());
        info!("   PCR(OI): {:.2} | PCR(Vol): {:.2} | IV Rank: {:.0}% | ATM IV: {:.1}%",
            snap.pcr_oi, snap.pcr_vol, snap.iv_rank, snap.atm_iv * 100.0);
        info!("   Max Pain: ₹{:.0} | Spot vs MaxPain: {:+.0} pts | GEX: {:.2e}",
            snap.max_pain,
            snap.spot - snap.max_pain,
            snap.net_gex);
        info!("   Total CE OI: {} | Total PE OI: {} | CE Vol: {} | PE Vol: {}",
            snap.total_ce_oi, snap.total_pe_oi, snap.total_ce_vol, snap.total_pe_vol);
        info!("   Kelly fraction: {:.1}% | Available capital: ₹{:.2}",
            self.risk.kelly_fraction() * 100.0, self.risk.available_capital());

        let atm_idx = snap.strikes.iter()
            .enumerate()
            .min_by(|(_, a), (_, b)| {
                (a.strike - snap.spot).abs().partial_cmp(&(b.strike - snap.spot).abs()).unwrap()
            })
            .map(|(i, _)| i)
            .unwrap_or(0);

        let lo = atm_idx.saturating_sub(3);
        let hi = (atm_idx + 4).min(snap.strikes.len());

        info!("   {:<8} {:>8} {:>8} {:>10} {:>10} {:>8} {:>8} {:>8} {:>8}",
            "STRIKE", "CE LTP", "PE LTP", "CE OI", "PE OI", "CE IV%", "PE IV%", "IV SKEW", "STRADDLE");
        for s in &snap.strikes[lo..hi] {
            let atm_marker = if (s.strike - snap.atm_strike).abs() < 0.1 { " ← ATM" } else { "" };
            info!("   {:<8.0} {:>8.2} {:>8.2} {:>10} {:>10} {:>8.1} {:>8.1} {:>8.1}% {:>8.2}{}",
                s.strike, s.ce_ltp, s.pe_ltp,
                s.ce_oi, s.pe_oi,
                s.ce_iv * 100.0, s.pe_iv * 100.0,
                s.iv_skew * 100.0,
                s.straddle_premium,
                atm_marker);
        }
        info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    }

    fn log_signal(&self, sig: &Signal, snap: &ChainSnapshot) {
        info!("╔═══════════════════════════════════════════════════════════╗");
        info!("║  🎯 TRADE SIGNAL #{} — CONFIDENCE: {:.0}/100             ║", sig.id, sig.confidence);
        info!("╚═══════════════════════════════════════════════════════════╝");
        info!("  Action    : {} {} {:.0} {} ({})",
            sig.action, sig.underlying, sig.strike, sig.expiry, sig.strategy);
        info!("  Session   : {} | Regime: {}", sig.session, snap.regime);
        info!("  ─────────────────────────────────────────────────────────");
        info!("  Entry     : ₹{:.2} × {} lots × {} units = ₹{:.2} total outlay",
            sig.option_price, sig.lots, sig.lot_size, sig.capital_required);
        info!("  Target    : ₹{:.2} (+{:.0}%) → MAX PROFIT ₹{:.2}",
            sig.target_price, self.profit_target_pct, sig.max_profit_target);
        info!("  Stop Loss : ₹{:.2} (-{:.0}%) → MAX LOSS   ₹{:.2}",
            sig.stop_price, self.stop_loss_pct, sig.max_loss);
        info!("  R:R Ratio : {:.2}:1 | Breakeven move: {:.2}% of spot",
            sig.risk_reward, sig.breakeven_move_pct);
        info!("  ─────────────────────────────────────────────────────────");
        info!("  RATIONALE (why this trade):");
        for (reason, contribution) in &sig.reasons {
            info!("    [{:+.0} pts] {}", contribution, reason);
        }
        info!("  ─────────────────────────────────────────────────────────");
        info!("  Kelly fraction: {:.1}% | Capital after entry: ₹{:.2}",
            self.risk.kelly_fraction() * 100.0,
            (self.risk.available_capital() - sig.capital_required).max(0.0));
        info!("  Progress to ₹50,000: {:.1}%", (self.risk.capital / 50_000.0) * 100.0);
        info!("  ─────────────────────────────────────────────────────────");
        info!("  ⚡ EXECUTE ON ZERODHA: {} NSE {} {:.0} {}",
            if sig.action == SignalAction::BuyCE { "BUY" } else { "BUY" },
            sig.underlying, sig.strike,
            if sig.action == SignalAction::BuyCE { "CE" } else { "PE" });
        info!("  ⚠️  SET SL AT: ₹{:.2} | SET TARGET AT: ₹{:.2}",
            sig.stop_price, sig.target_price);
        info!("═════════════════════════════════════════════════════════════");
    }

    fn log_portfolio_summary(&self) {
        let open_positions: Vec<_> = self.positions.iter().filter(|p| p.is_open).collect();
        let closed_positions: Vec<_> = self.positions.iter().filter(|p| !p.is_open).collect();
        let total_pnl: f64 = closed_positions.iter().map(|p| p.pnl).sum();
        let wins = closed_positions.iter().filter(|p| p.pnl >= 0.0).count();
        let losses = closed_positions.iter().filter(|p| p.pnl < 0.0).count();

        info!("┌─────────────────────────────────────────────────────────┐");
        info!("│  PORTFOLIO SUMMARY — SATAVAHANA OPTIONS ENGINE          │");
        info!("├─────────────────────────────────────────────────────────┤");
        info!("│  Starting Capital : ₹{:<10.2}                          │", self.risk.initial_capital);
        info!("│  Current Capital  : ₹{:<10.2}                          │", self.risk.capital);
        info!("│  Available Cap.   : ₹{:<10.2}                          │", self.risk.available_capital());
        info!("│  Progress         : {:.1}% → ₹50,000                   │", (self.risk.capital / 50_000.0) * 100.0);
        info!("│  Total PnL        : ₹{:<+10.2}  ({:+.1}%)              │",
            total_pnl, (total_pnl / self.risk.initial_capital) * 100.0);
        info!("│  Open Positions   : {}                                   │", open_positions.len());
        info!("│  Closed Trades    : {} wins / {} losses                 │", wins, losses);
        info!("│  Trades Opened Day: {}                                   │", self.daily_trades_opened);
        if wins + losses > 0 {
            info!("│  Win Rate         : {:.1}%                              │",
                wins as f64 / (wins + losses) as f64 * 100.0);
        }
        info!("│  Kelly Fraction   : {:.1}% (half-Kelly adaptive)        │", self.risk.kelly_fraction() * 100.0);
        info!("│  Circuit Breaker  : {}                                │",
            if self.risk.circuit_breaker_triggered() { "TRIGGERED ⛔" } else { "OK ✅           " });
        info!("└─────────────────────────────────────────────────────────┘");
    }
}

#[cfg(test)]
mod tests {
    use super::{
        entry_cost_per_lot_excl_brokerage,
        entry_cutoff_ist_mins_for_dte,
        entry_order_cost,
        nearest_affordable_otm_idx,
        options_brokerage_per_order,
        past_entry_cutoff_ist,
        OptionsEngine,
        PendingEntryOrder,
        PendingExitOrder,
        Position,
        RiskEngine,
        SessionPhase,
        Signal,
        SignalAction,
        StrikeScanDirection,
        StrategyType,
        MIN_TRADEABLE_OPTION_PRICE,
    };
    use crate::config::OptionsEngineConfig;
    use crate::execution::{OrderCommand, OrderSide, OrderUpdate};
    use crate::ledger::EntryContext;
    use crate::models::{Tick, TickMode};
    use crate::store::TickStore;
    use std::collections::HashMap;

    fn test_config() -> OptionsEngineConfig {
        OptionsEngineConfig {
            enabled: true,
            initial_capital: 10_000.0,
            max_daily_loss_pct: 30.0,
            max_daily_profit_pct: 35.0,
            profit_target_pct: 55.0,
            stop_loss_pct: 35.0,
            min_confidence: 60.0,
            expiry_day_min_confidence: 60.0,
            scan_interval_secs: 45,
            max_daily_trades: 3,
        }
    }

    fn test_entry_context() -> EntryContext {
        EntryContext {
            confidence: 75.0,
            session: SessionPhase::Morning.to_string(),
            ..EntryContext::default()
        }
    }

    #[test]
    fn risk_engine_dual_circuit_is_fee_inclusive() {
        // record_trade(pnl) feeds NET (post-cost) P&L into `capital`, and the
        // circuits compare `capital - daily_start_capital`, so they are
        // fee-inclusive by construction.
        let mut lower = RiskEngine::new(5_000.0, 0.15, 0.35); // ₹5k start
        assert!(!lower.circuit_breaker_triggered());
        lower.record_trade(-740.0);
        assert!(!lower.circuit_breaker_triggered(), "-14.8% still open");
        lower.record_trade(-20.0); // total -760 (-15.2%) => lower circuit
        assert!(lower.circuit_breaker_triggered());
        assert_eq!(lower.circuit_reason(), Some("LOWER circuit (daily loss limit)"));

        let mut upper = RiskEngine::new(5_000.0, 0.15, 0.35);
        upper.record_trade(1_700.0); // +34% still open
        assert!(!upper.circuit_breaker_triggered());
        upper.record_trade(60.0); // total +1760 (+35.2%) => upper circuit
        assert!(upper.circuit_breaker_triggered());
        assert_eq!(
            upper.circuit_reason(),
            Some("UPPER circuit (daily profit limit — anti-overtrade)")
        );
    }

    #[test]
    fn zero_config_trade_cap_means_dynamic_cap_only() {
        let mut cfg = test_config();
        cfg.max_daily_trades = 0;
        let store = TickStore::new();
        let engine = OptionsEngine::new(
            Vec::new(),
            store,
            HashMap::new(),
            &cfg,
            0.065,
            0.0,
            "/tmp",
        );
        let signal = Signal {
            id: 1,
            underlying: "NIFTY".to_string(),
            expiry: "2026-06-23".to_string(),
            action: SignalAction::BuyPE,
            strike: 23_900.0,
            option_price: 90.0,
            target_price: 110.0,
            stop_price: 75.0,
            lots: 1,
            lot_size: 65,
            capital_required: 5_900.0,
            confidence: 75.0,
            strategy: StrategyType::Composite,
            reasons: Vec::new(),
            timestamp_ms: 0,
            session: SessionPhase::Afternoon,
            breakeven_move_pct: 1.0,
            max_loss: 1_000.0,
            max_profit_target: 1_000.0,
            risk_reward: 1.5,
            entry_ctx: EntryContext {
                days_to_expiry: 4.0,
                confidence: 75.0,
                session: SessionPhase::Afternoon.to_string(),
                ..EntryContext::default()
            },
        };

        assert!(engine.dynamic_daily_trade_cap(&signal) > 0);
        assert_eq!(
            engine.effective_daily_trade_cap(&signal),
            engine.dynamic_daily_trade_cap(&signal)
        );
    }

    #[test]
    fn multi_day_entry_cutoff_is_earlier_than_expiry_cutoff() {
        assert_eq!(entry_cutoff_ist_mins_for_dte(4.0), 14 * 60 + 55);
        assert_eq!(entry_cutoff_ist_mins_for_dte(0.4), 15 * 60);
        assert!(!past_entry_cutoff_ist(14 * 60 + 54, 4.0));
        assert!(past_entry_cutoff_ist(14 * 60 + 55, 4.0));
        assert!(!past_entry_cutoff_ist(14 * 60 + 55, 0.4));
    }

    fn test_position(exit_pending: bool) -> Position {
        Position {
            id: 1,
            underlying: "NIFTY".to_string(),
            tradingsymbol: "NIFTY26APR22000CE".to_string(),
            action: SignalAction::BuyCE,
            strike: 22_000.0,
            expiry: "2026-04-30".to_string(),
            option_token: 101,
            lots: 1,
            lot_size: 50,
            entry_price: 100.0,
            target_price: 120.0,
            stop_price: 85.0,
            entry_time_ms: 0,
            entry_datetime: "2026-04-06 09:30:00".to_string(),
            strategy: StrategyType::Composite,
            is_open: true,
            exit_price: 0.0,
            exit_time_ms: 0,
            pnl: 0.0,
            pnl_pct: 0.0,
            exit_reason: String::new(),
            capital_deployed: 5_000.0,
            entry_ctx: test_entry_context(),
            breakeven_stop_set: false,
            exit_pending,
            checkpoints_evaluated: 0,
        }
    }

    fn test_engine_with_store(store: TickStore) -> OptionsEngine {
        let cfg = test_config();
        let log_dir = std::env::temp_dir().join(format!(
            "satavahana_options_engine_test_{}",
            std::process::id()
        ));
        let _ = std::fs::create_dir_all(&log_dir);
        OptionsEngine::new(
            Vec::new(),
            store,
            HashMap::new(),
            &cfg,
            0.065,
            0.0,
            log_dir.to_string_lossy().as_ref(),
        )
    }

    #[test]
    fn trail_arms_without_time_gate_and_keeps_half_of_peak() {
        let store = TickStore::new();
        let mut engine = test_engine_with_store(store.clone());
        let mut pos = test_position(false);   // entry 100, initial stop 85 (-15%)
        pos.target_price = 140.0;             // far target so the trail (not target) governs
        engine.positions.push(pos);

        // +16% at only 1 min held: the trail arms immediately — NO time gate (uniform with
        // multileg::trail_exits). stop = (100+116)/2 = 108 = give back 50% of the +16% peak.
        // The +12% lock stays 3-min-gated, so breakeven_stop_set is still false here.
        store.update(Tick { token: 101, ltp: 116.0, mode: TickMode::Ltp, ..Tick::default() });
        engine.set_clock_override_ms(60_000);
        engine.check_open_positions();
        assert!((engine.positions[0].stop_price - 108.0).abs() < 1e-9, "trail arms with no time gate");
        assert_eq!(engine.positions[0].checkpoints_evaluated, 3);
        assert!(!engine.positions[0].breakeven_stop_set, "lock is still time-gated; only the trail fired");
        assert!(engine.positions[0].is_open, "trail must not close a live runner");

        // +26%: trail ratchets up → (100+126)/2 = 113.
        store.update(Tick { token: 101, ltp: 126.0, mode: TickMode::Ltp, ..Tick::default() });
        engine.set_clock_override_ms(4 * 60_000);
        engine.check_open_positions();
        assert!((engine.positions[0].stop_price - 113.0).abs() < 1e-9, "trail ratchets up with the peak");
        assert!(engine.positions[0].is_open);

        // pullback to +18%: one-way ratchet holds the stop at 113; still open (118 > 113).
        store.update(Tick { token: 101, ltp: 118.0, mode: TickMode::Ltp, ..Tick::default() });
        engine.set_clock_override_ms(5 * 60_000);
        engine.check_open_positions();
        assert!((engine.positions[0].stop_price - 113.0).abs() < 1e-9, "trail never lowers");
        assert!(engine.positions[0].is_open);
    }

    #[test]
    fn lock_arms_in_12_to_15_zone() {
        let store = TickStore::new();
        let mut engine = test_engine_with_store(store.clone());
        let mut pos = test_position(false);   // entry 100, initial stop 85
        pos.target_price = 140.0;
        engine.positions.push(pos);

        // +13% (≥3 min), never reached +15%: one-shot lock to entry+2% (102); trail NOT armed.
        store.update(Tick { token: 101, ltp: 113.0, mode: TickMode::Ltp, ..Tick::default() });
        engine.set_clock_override_ms(4 * 60_000);
        engine.check_open_positions();
        assert!((engine.positions[0].stop_price - 102.0).abs() < 1e-9, "+13% arms the entry+2% lock");
        assert!(engine.positions[0].breakeven_stop_set);
        assert!(engine.positions[0].is_open);
    }

    #[test]
    fn profit_lock_stays_below_tight_target() {
        let store = TickStore::new();
        let mut engine = test_engine_with_store(store.clone());
        let mut pos = test_position(false);
        pos.target_price = 101.0;
        engine.positions.push(pos);

        store.update(Tick {
            token: 101,
            ltp: 116.0,
            mode: TickMode::Ltp,
            ..Tick::default()
        });
        engine.set_clock_override_ms(4 * 60_000);
        engine.check_open_positions();

        assert!(
            engine.positions[0].stop_price < engine.positions[0].target_price,
            "profit lock should not be placed on/above an unusually tight target"
        );
        assert!(!engine.positions[0].is_open, "tight target should still close as target hit");
    }

    #[test]
    fn finds_pe_strike_beyond_old_five_strike_limit() {
        let prices = vec![
            44.7, 49.7, 55.6, 61.85, 69.05, 77.15, 86.2, 96.25, 108.4, 120.0, 134.55, 150.25,
            167.55, 186.95, 208.2,
        ];
        let start_idx = prices.len() - 1; // ATM PE
        let deployable_capital = 7_907.0;
        let lot_size = 75;

        let (idx, checked) = nearest_affordable_otm_idx(
            &prices,
            start_idx,
            StrikeScanDirection::Down,
            lot_size,
            deployable_capital,
            MIN_TRADEABLE_OPTION_PRICE,
        );

        assert_eq!(idx, Some(7));
        assert_eq!(checked, 8);
    }

    #[test]
    fn finds_ce_strike_beyond_old_five_strike_limit() {
        let prices = vec![
            560.0, 520.0, 480.0, 440.0, 400.0, 360.0, 320.0, 280.0, 240.0, 210.0,
        ];
        let deployable_capital = 12_000.0;
        let lot_size = 25;

        let (idx, checked) = nearest_affordable_otm_idx(
            &prices,
            0,
            StrikeScanDirection::Up,
            lot_size,
            deployable_capital,
            MIN_TRADEABLE_OPTION_PRICE,
        );

        assert_eq!(idx, Some(3));
        assert_eq!(checked, 4);

        let (idx_far, checked_far) = nearest_affordable_otm_idx(
            &prices,
            0,
            StrikeScanDirection::Up,
            lot_size,
            7_000.0,
            MIN_TRADEABLE_OPTION_PRICE,
        );

        assert_eq!(idx_far, Some(8));
        assert_eq!(checked_far, 9);
    }

    #[test]
    fn prefers_near_otm_contract_when_full_capital_can_fund_one_lot() {
        let prices = vec![210.85, 183.8, 159.0, 136.75, 116.6];
        let deployable_capital = 13_000.0;
        let lot_size = 75;

        let (idx, checked) = nearest_affordable_otm_idx(
            &prices,
            0,
            StrikeScanDirection::Up,
            lot_size,
            deployable_capital,
            MIN_TRADEABLE_OPTION_PRICE,
        );

        assert_eq!(idx, Some(2));
        assert_eq!(checked, 3);
    }

    #[test]
    fn timed_out_limit_exit_falls_back_to_market_and_closes_position() {
        let cfg = test_config();
        let log_dir = std::env::temp_dir().join(format!(
            "satavahana_exit_timeout_test_{}",
            std::process::id()
        ));
        let _ = std::fs::create_dir_all(&log_dir);

        let mut engine = OptionsEngine::new(
            Vec::new(),
            TickStore::new(),
            HashMap::new(),
            &cfg,
            0.065,
            0.0,
            log_dir.to_string_lossy().as_ref(),
        );

        let (order_tx, mut order_rx) = tokio::sync::mpsc::unbounded_channel();
        engine.set_live_order_bridge(order_tx, None, "SATA".to_string());
        engine.set_clock_override_ms(20_000);

        engine.positions.push(test_position(true));
        engine.pending_exit_orders.push(PendingExitOrder {
            pos_id: 1,
            tag: "SATAEXIT1".to_string(),
            tradingsymbol: "NIFTY26APR22000CE".to_string(),
            total_quantity: 50,
            placed_ms: 0,
            last_status_poll_ms: 0,
            reason: "TARGET HIT".to_string(),
            cancel_requested: false,
            last_cancel_attempt_ms: 0,
            market_fallback_sent: false,
            fallback_book_price: 100.0,
            total_filled_quantity: 0,
            total_filled_notional: 0.0,
            current_order_filled_quantity: 0,
            current_order_filled_notional: 0.0,
        });

        engine.poll_pending_orders();
        assert!(engine.pending_exit_orders[0].cancel_requested);

        match order_rx.try_recv().expect("cancel command") {
            OrderCommand::CancelByTag { tag } => assert_eq!(tag, "SATAEXIT1"),
            other => panic!("expected cancel command, got {:?}", other),
        }
        match order_rx.try_recv().expect("status command") {
            OrderCommand::StatusByTag { tag } => assert_eq!(tag, "SATAEXIT1"),
            other => panic!("expected status command, got {:?}", other),
        }

        engine.handle_order_update(OrderUpdate {
            tag: "SATAEXIT1".to_string(),
            order_id: None,
            status: Some("CANCELLED".to_string()),
            average_price: Some(0.0),
            filled_quantity: Some(0),
            pending_quantity: Some(50),
            source: "status_poll".to_string(),
            message: None,
        });

        assert!(engine.pending_exit_orders[0].market_fallback_sent);
        assert!(!engine.pending_exit_orders[0].cancel_requested);

        match order_rx.try_recv().expect("market fallback order") {
            OrderCommand::Place(cmd) => {
                assert_eq!(cmd.tag, "SATAEXIT1");
                assert_eq!(cmd.quantity, 50);
                assert_eq!(cmd.side, OrderSide::Sell);
                assert!(cmd.limit_price.is_none(), "fallback should use MARKET");
            }
            other => panic!("expected place command, got {:?}", other),
        }
        match order_rx.try_recv().expect("fallback status command") {
            OrderCommand::StatusByTag { tag } => assert_eq!(tag, "SATAEXIT1"),
            other => panic!("expected status command, got {:?}", other),
        }

        engine.handle_order_update(OrderUpdate {
            tag: "SATAEXIT1".to_string(),
            order_id: None,
            status: Some("COMPLETE".to_string()),
            average_price: Some(101.5),
            filled_quantity: Some(50),
            pending_quantity: Some(0),
            source: "status_poll".to_string(),
            message: None,
        });

        assert!(engine.pending_exit_orders.is_empty());
        assert!(!engine.positions[0].is_open);
        assert!(!engine.positions[0].exit_pending);
        assert!((engine.positions[0].exit_price - 101.5).abs() < 0.01);
        assert_eq!(engine.positions[0].exit_reason, "TARGET HIT");
    }

    #[test]
    fn complete_exit_without_average_price_uses_reference_price() {
        let cfg = test_config();
        let log_dir = std::env::temp_dir().join(format!(
            "satavahana_exit_no_avg_test_{}",
            std::process::id()
        ));
        let _ = std::fs::create_dir_all(&log_dir);

        let mut engine = OptionsEngine::new(
            Vec::new(),
            TickStore::new(),
            HashMap::new(),
            &cfg,
            0.065,
            0.0,
            log_dir.to_string_lossy().as_ref(),
        );

        let (order_tx, _order_rx) = tokio::sync::mpsc::unbounded_channel();
        engine.set_live_order_bridge(order_tx, None, "SATA".to_string());
        engine.positions.push(test_position(true));
        engine.pending_exit_orders.push(PendingExitOrder {
            pos_id: 1,
            tag: "SATAEXIT1".to_string(),
            tradingsymbol: "NIFTY26APR22000CE".to_string(),
            total_quantity: 50,
            placed_ms: 0,
            last_status_poll_ms: 0,
            reason: "TARGET HIT".to_string(),
            cancel_requested: false,
            last_cancel_attempt_ms: 0,
            market_fallback_sent: true,
            fallback_book_price: 102.25,
            total_filled_quantity: 0,
            total_filled_notional: 0.0,
            current_order_filled_quantity: 0,
            current_order_filled_notional: 0.0,
        });

        engine.handle_order_update(OrderUpdate {
            tag: "SATAEXIT1".to_string(),
            order_id: None,
            status: Some("COMPLETE".to_string()),
            average_price: Some(0.0),
            filled_quantity: Some(50),
            pending_quantity: Some(0),
            source: "status_poll".to_string(),
            message: None,
        });

        assert!(engine.pending_exit_orders.is_empty());
        assert!(!engine.positions[0].is_open);
        assert!(!engine.positions[0].exit_pending);
        assert!((engine.positions[0].exit_price - 102.25).abs() < 0.01);
    }

    #[test]
    fn margin_rejected_exit_does_not_resubmit() {
        // 2026-06-30 incident: an exit was rejected for "Insufficient funds / Margin exceeds".
        // On a CLOSE that proves the broker holds no inventory, so the engine must NOT keep
        // resubmitting (each resubmit would be a naked short) — it must halt for manual reconcile.
        let cfg = test_config();
        let log_dir = std::env::temp_dir().join(format!(
            "satavahana_margin_reject_test_{}",
            std::process::id()
        ));
        let _ = std::fs::create_dir_all(&log_dir);
        let mut engine = OptionsEngine::new(
            Vec::new(),
            TickStore::new(),
            HashMap::new(),
            &cfg,
            0.065,
            0.0,
            log_dir.to_string_lossy().as_ref(),
        );
        let (order_tx, _order_rx) = tokio::sync::mpsc::unbounded_channel();
        engine.set_live_order_bridge(order_tx, None, "SATA".to_string());
        engine.positions.push(test_position(true));
        engine.pending_exit_orders.push(PendingExitOrder {
            pos_id: 1,
            tag: "SATAEXIT1".to_string(),
            tradingsymbol: "NIFTY26APR22000CE".to_string(),
            total_quantity: 50,
            placed_ms: 0,
            last_status_poll_ms: 0,
            reason: "STOP HIT".to_string(),
            cancel_requested: false,
            last_cancel_attempt_ms: 0,
            market_fallback_sent: false,
            fallback_book_price: 85.0,
            total_filled_quantity: 0,
            total_filled_notional: 0.0,
            current_order_filled_quantity: 0,
            current_order_filled_notional: 0.0,
        });

        engine.handle_order_update(OrderUpdate {
            tag: "SATAEXIT1".to_string(),
            order_id: Some("111".to_string()),
            status: Some("REJECTED".to_string()),
            average_price: None,
            filled_quantity: Some(0),
            pending_quantity: Some(0),
            source: "status_poll".to_string(),
            message: Some(
                "Insufficient funds. Margin required: 204193.50. Margin available: 15386.60.".to_string(),
            ),
        });

        assert!(engine.pending_exit_orders.is_empty());
        assert!(engine.positions[0].is_open, "a reject must not falsely close the position");
        assert!(
            engine.positions[0].exit_pending,
            "exit_pending must stay true so close_position_at's guard blocks resubmit"
        );
    }

    #[test]
    fn margin_place_error_exit_does_not_resubmit() {
        // Same safety rule as a broker REJECTED status, but for brokers/API layers that return
        // the margin failure directly from placement instead of creating an order_id first.
        let cfg = test_config();
        let log_dir = std::env::temp_dir().join(format!(
            "satavahana_margin_place_error_test_{}",
            std::process::id()
        ));
        let _ = std::fs::create_dir_all(&log_dir);
        let mut engine = OptionsEngine::new(
            Vec::new(),
            TickStore::new(),
            HashMap::new(),
            &cfg,
            0.065,
            0.0,
            log_dir.to_string_lossy().as_ref(),
        );
        let (order_tx, _order_rx) = tokio::sync::mpsc::unbounded_channel();
        engine.set_live_order_bridge(order_tx, None, "SATA".to_string());
        engine.positions.push(test_position(true));
        engine.pending_exit_orders.push(PendingExitOrder {
            pos_id: 1,
            tag: "SATAEXIT1".to_string(),
            tradingsymbol: "NIFTY26APR22000CE".to_string(),
            total_quantity: 50,
            placed_ms: 0,
            last_status_poll_ms: 0,
            reason: "STOP HIT".to_string(),
            cancel_requested: false,
            last_cancel_attempt_ms: 0,
            market_fallback_sent: false,
            fallback_book_price: 85.0,
            total_filled_quantity: 0,
            total_filled_notional: 0.0,
            current_order_filled_quantity: 0,
            current_order_filled_notional: 0.0,
        });

        engine.handle_order_update(OrderUpdate {
            tag: "SATAEXIT1".to_string(),
            order_id: None,
            status: None,
            average_price: None,
            filled_quantity: None,
            pending_quantity: None,
            source: "place_error".to_string(),
            message: Some("RMS: Margin Exceeds, required: 204193, available: 15386".to_string()),
        });

        assert!(engine.pending_exit_orders.is_empty());
        assert!(engine.positions[0].is_open, "a place error must not falsely close the position");
        assert!(
            engine.positions[0].exit_pending,
            "margin place_error must halt resubmits just like a REJECTED status"
        );
    }

    #[test]
    fn orphaned_exit_fill_closes_position() {
        // 2026-06-30 root cause: a duplicate CANCELLED removed the pending exit before the
        // market-fallback's COMPLETE arrived, so the fill was dropped at tag dispatch and the
        // position stayed "open". The reconcile path must close it by pos_id from the tag.
        let cfg = test_config();
        let log_dir = std::env::temp_dir().join(format!(
            "satavahana_orphan_fill_test_{}",
            std::process::id()
        ));
        let _ = std::fs::create_dir_all(&log_dir);
        let mut engine = OptionsEngine::new(
            Vec::new(),
            TickStore::new(),
            HashMap::new(),
            &cfg,
            0.065,
            0.0,
            log_dir.to_string_lossy().as_ref(),
        );
        let (order_tx, _order_rx) = tokio::sync::mpsc::unbounded_channel();
        engine.set_live_order_bridge(order_tx, None, "SATA".to_string());
        engine.positions.push(test_position(true));
        // No pending_exit_orders: the record was already removed (duplicate CANCELLED).

        engine.handle_order_update(OrderUpdate {
            tag: "SATAEXIT1".to_string(),
            order_id: Some("222".to_string()),
            status: Some("COMPLETE".to_string()),
            average_price: Some(55.10),
            filled_quantity: Some(50),
            pending_quantity: Some(0),
            source: "postback".to_string(),
            message: None,
        });

        assert!(!engine.positions[0].is_open, "orphaned exit fill must close the position");
        assert!(!engine.positions[0].exit_pending);
        assert!((engine.positions[0].exit_price - 55.10).abs() < 0.01);
    }

    #[test]
    fn duplicate_cancel_after_market_fallback_keeps_exit_pending() {
        // The cancel request and the queued status poll can both report the original limit order
        // as CANCELLED. Once the market fallback has been sent, that stale terminal status must not
        // remove the pending exit or clear exit_pending before the fallback fill arrives.
        let cfg = test_config();
        let log_dir = std::env::temp_dir().join(format!(
            "satavahana_duplicate_cancel_after_fallback_test_{}",
            std::process::id()
        ));
        let _ = std::fs::create_dir_all(&log_dir);
        let mut engine = OptionsEngine::new(
            Vec::new(),
            TickStore::new(),
            HashMap::new(),
            &cfg,
            0.065,
            0.0,
            log_dir.to_string_lossy().as_ref(),
        );
        let (order_tx, _order_rx) = tokio::sync::mpsc::unbounded_channel();
        engine.set_live_order_bridge(order_tx, None, "SATA".to_string());
        engine.positions.push(test_position(true));
        engine.pending_exit_orders.push(PendingExitOrder {
            pos_id: 1,
            tag: "SATAEXIT1".to_string(),
            tradingsymbol: "NIFTY26APR22000CE".to_string(),
            total_quantity: 50,
            placed_ms: 20_000,
            last_status_poll_ms: 0,
            reason: "STOP HIT".to_string(),
            cancel_requested: false,
            last_cancel_attempt_ms: 0,
            market_fallback_sent: true,
            fallback_book_price: 57.50,
            total_filled_quantity: 0,
            total_filled_notional: 0.0,
            current_order_filled_quantity: 0,
            current_order_filled_notional: 0.0,
        });

        engine.handle_order_update(OrderUpdate {
            tag: "SATAEXIT1".to_string(),
            order_id: Some("old-limit".to_string()),
            status: Some("CANCELLED".to_string()),
            average_price: Some(0.0),
            filled_quantity: Some(0),
            pending_quantity: Some(50),
            source: "status_poll".to_string(),
            message: None,
        });

        assert_eq!(engine.pending_exit_orders.len(), 1);
        assert!(engine.positions[0].is_open);
        assert!(
            engine.positions[0].exit_pending,
            "duplicate stale cancel must not unblock another close_position_at call"
        );

        engine.handle_order_update(OrderUpdate {
            tag: "SATAEXIT1".to_string(),
            order_id: Some("market-fallback".to_string()),
            status: Some("COMPLETE".to_string()),
            average_price: Some(55.10),
            filled_quantity: Some(50),
            pending_quantity: Some(0),
            source: "status_poll".to_string(),
            message: None,
        });

        assert!(engine.pending_exit_orders.is_empty());
        assert!(!engine.positions[0].is_open);
        assert!(!engine.positions[0].exit_pending);
        assert!((engine.positions[0].exit_price - 55.10).abs() < 0.01);
    }

    #[test]
    fn simulated_close_does_not_request_capital_sync() {
        let cfg = test_config();
        let log_dir = std::env::temp_dir().join(format!(
            "satavahana_sim_close_test_{}",
            std::process::id()
        ));
        let _ = std::fs::create_dir_all(&log_dir);

        let mut engine = OptionsEngine::new(
            Vec::new(),
            TickStore::new(),
            HashMap::new(),
            &cfg,
            0.065,
            0.0,
            log_dir.to_string_lossy().as_ref(),
        );

        engine.positions.push(test_position(false));
        engine.close_position_at(0, 101.0, "TARGET HIT ₹101.00".to_string());

        assert!(!engine.capital_sync_needed);
        assert!(!engine.positions[0].is_open);
    }

    #[test]
    fn global_position_lock_blocks_options_and_frees_on_close() {
        let cfg = test_config();
        let log_dir = std::env::temp_dir().join(format!(
            "satavahana_lock_close_test_{}",
            std::process::id()
        ));
        let _ = std::fs::create_dir_all(&log_dir);

        let mut engine = OptionsEngine::new(
            Vec::new(),
            TickStore::new(),
            HashMap::new(),
            &cfg,
            0.065,
            0.0,
            log_dir.to_string_lossy().as_ref(),
        );
        let circuit = crate::portfolio::new_shared(100_000.0, 15.0, 25.0, u32::MAX);
        engine.set_shared_circuit(circuit.clone());

        // Another engine (micro) holds the single global slot.
        assert!(crate::portfolio::try_claim(&circuit, "micro"));

        // Options' own idle-release must NOT evict another holder's slot.
        engine.release_lock_if_idle();
        assert_eq!(circuit.lock().unwrap().position_holder(), Some("micro"));
        // ...and options cannot claim while micro holds it (entry would be cancelled).
        assert!(!crate::portfolio::try_claim(&circuit, "options"));

        // Micro's trade closes → slot frees → options claims, opens, and the REAL close
        // path (close_position_at -> finalize_position_close) frees the slot again.
        crate::portfolio::release(&circuit, "micro");
        assert!(crate::portfolio::try_claim(&circuit, "options"));
        assert!(crate::portfolio::is_locked(&circuit));
        engine.positions.push(test_position(false));
        engine.close_position_at(0, 101.0, "TARGET HIT ₹101.00".to_string());
        assert!(!engine.positions[0].is_open);
        assert!(
            !crate::portfolio::is_locked(&circuit),
            "the close chokepoint must free the global position slot"
        );
    }

    // Bug 5: Zerodha charges ₹20 flat brokerage per ORDER, not per lot.
    #[test]
    fn flat_brokerage_is_charged_once_per_order_not_per_lot() {
        let price = 159.0;
        let lot = 65u32;
        let per_lot_excl = entry_cost_per_lot_excl_brokerage(price, lot);
        let flat = options_brokerage_per_order();
        assert!((flat - 20.0 * 1.18).abs() < 1e-9, "flat brokerage = ₹20 + 18% GST = ₹23.60");

        let one = entry_order_cost(price, lot, 1);
        let four = entry_order_cost(price, lot, 4);
        assert!((one - (per_lot_excl + flat)).abs() < 1e-6, "1 lot = per-lot + ONE brokerage");
        assert!((four - (4.0 * per_lot_excl + flat)).abs() < 1e-6, "4 lots = 4×per-lot + ONE brokerage");
        // The old per-lot bug would have added 4× brokerage; the real delta from 1→4 lots
        // must be exactly 3× the per-lot ex-brokerage cost, with NO extra brokerage.
        assert!(((four - one) - 3.0 * per_lot_excl).abs() < 1e-6, "added lots must not each pay brokerage");
        // Sanity: the bug over-charged a 4-lot round trip by 3 extra brokerages PER LEG.
        let overcharge_per_leg = 3.0 * flat;
        assert!(overcharge_per_leg > 70.0, "the fixed over-charge was material: ~₹{:.1}/leg", overcharge_per_leg);
    }

    #[test]
    fn untracked_pending_exit_holds_and_releases_global_lock() {
        let cfg = test_config();
        let log_dir = std::env::temp_dir().join(format!(
            "satavahana_untracked_exit_lock_test_{}",
            std::process::id()
        ));
        let _ = std::fs::create_dir_all(&log_dir);

        let mut engine = OptionsEngine::new(
            Vec::new(),
            TickStore::new(),
            HashMap::new(),
            &cfg,
            0.065,
            0.0,
            log_dir.to_string_lossy().as_ref(),
        );
        let circuit = crate::portfolio::new_shared(100_000.0, 15.0, 25.0, u32::MAX);
        engine.set_shared_circuit(circuit.clone());

        assert!(crate::portfolio::try_claim(&circuit, "options"));
        engine.pending_exit_orders.push(PendingExitOrder {
            pos_id: 99,
            tag: "SATAEXIT99".to_string(),
            tradingsymbol: "NIFTY26APR22000CE".to_string(),
            total_quantity: 50,
            placed_ms: 0,
            last_status_poll_ms: 0,
            reason: "EMERGENCY FLATTEN".to_string(),
            cancel_requested: false,
            last_cancel_attempt_ms: 0,
            market_fallback_sent: false,
            fallback_book_price: 100.0,
            total_filled_quantity: 0,
            total_filled_notional: 0.0,
            current_order_filled_quantity: 0,
            current_order_filled_notional: 0.0,
        });

        engine.release_lock_if_idle();
        assert_eq!(
            circuit.lock().unwrap().position_holder(),
            Some("options"),
            "pending emergency exit must keep the global slot held"
        );

        engine.handle_order_update(OrderUpdate {
            tag: "SATAEXIT99".to_string(),
            order_id: None,
            status: Some("COMPLETE".to_string()),
            average_price: Some(101.0),
            filled_quantity: Some(50),
            pending_quantity: Some(0),
            source: "status_poll".to_string(),
            message: None,
        });

        assert!(engine.pending_exit_orders.is_empty());
        assert!(
            !crate::portfolio::is_locked(&circuit),
            "completed emergency exit must free the global slot"
        );
    }

    #[test]
    fn live_entry_order_pays_up_quarter_rupee_from_current_ltp() {
        let cfg = test_config();
        let log_dir = std::env::temp_dir().join(format!(
            "satavahana_live_entry_payup_test_{}",
            std::process::id()
        ));
        let _ = std::fs::create_dir_all(&log_dir);

        let store = TickStore::new();
        store.update(Tick {
            token: 101,
            ltp: 100.0,
            mode: TickMode::Ltp,
            ..Tick::default()
        });

        let mut engine = OptionsEngine::new(
            Vec::new(),
            store,
            HashMap::new(),
            &cfg,
            0.065,
            0.0,
            log_dir.to_string_lossy().as_ref(),
        );

        let (order_tx, mut order_rx) = tokio::sync::mpsc::unbounded_channel();
        engine.set_live_order_bridge(order_tx, None, "SATA".to_string());

        let signal = Signal {
            id: 1,
            underlying: "NIFTY".to_string(),
            expiry: "2026-04-30".to_string(),
            action: SignalAction::BuyCE,
            strike: 22_000.0,
            option_price: 99.0,
            target_price: 120.0,
            stop_price: 85.0,
            lots: 1,
            lot_size: 50,
            capital_required: 5_000.0,
            confidence: 75.0,
            strategy: StrategyType::Composite,
            reasons: Vec::new(),
            timestamp_ms: 0,
            session: SessionPhase::Morning,
            breakeven_move_pct: 0.0,
            max_loss: 0.0,
            max_profit_target: 0.0,
            risk_reward: 1.0,
            entry_ctx: test_entry_context(),
        };

        let limit_price = engine.live_entry_limit_price(101, signal.option_price);
        engine.submit_live_entry_order(
            &signal,
            101,
            "NIFTY26APR22000CE".to_string(),
            5_100.0,
            limit_price,
        );

        match order_rx.try_recv().expect("entry order") {
            OrderCommand::Place(cmd) => {
                assert_eq!(cmd.quantity, 50);
                assert_eq!(cmd.side, OrderSide::Buy);
                assert_eq!(cmd.limit_price, Some(100.25));
            }
            other => panic!("expected place command, got {:?}", other),
        }
        assert_eq!(engine.pending_entry_orders[0].signal_price, 100.25);
    }

    #[test]
    fn emergency_flatten_uses_market_sell_when_capital_is_exhausted() {
        let cfg = test_config();
        let log_dir = std::env::temp_dir().join(format!(
            "satavahana_emergency_flatten_test_{}",
            std::process::id()
        ));
        let _ = std::fs::create_dir_all(&log_dir);

        let store = TickStore::new();
        store.update(Tick {
            token: 101,
            ltp: 101.0,
            mode: TickMode::Ltp,
            ..Tick::default()
        });

        let mut engine = OptionsEngine::new(
            Vec::new(),
            store,
            HashMap::new(),
            &cfg,
            0.065,
            0.0,
            log_dir.to_string_lossy().as_ref(),
        );

        let (order_tx, mut order_rx) = tokio::sync::mpsc::unbounded_channel();
        engine.set_live_order_bridge(order_tx, None, "SATA".to_string());
        engine.risk.capital = 100.0;

        let signal = Signal {
            id: 1,
            underlying: "NIFTY".to_string(),
            expiry: "2026-04-30".to_string(),
            action: SignalAction::BuyCE,
            strike: 22_000.0,
            option_price: 100.0,
            target_price: 120.0,
            stop_price: 85.0,
            lots: 1,
            lot_size: 50,
            capital_required: 5_000.0,
            confidence: 75.0,
            strategy: StrategyType::Composite,
            reasons: Vec::new(),
            timestamp_ms: 0,
            session: SessionPhase::Morning,
            breakeven_move_pct: 0.0,
            max_loss: 0.0,
            max_profit_target: 0.0,
            risk_reward: 1.0,
            entry_ctx: test_entry_context(),
        };

        let pending = PendingEntryOrder {
            pos_id: 1,
            signal,
            token: 101,
            tradingsymbol: "NIFTY26APR22000CE".to_string(),
            estimated_capital: 5_000.0,
            tag: "SATAENTRY1".to_string(),
            placed_ms: 0,
            last_status_poll_ms: 0,
            cancel_requested: false,
            cancel_reason: None,
            last_cancel_attempt_ms: 0,
            released_after_timeout: false,
            order_id: Some("order1".to_string()),
            signal_price: 100.0,
            placed_at_scan_count: 0,
        };

        assert!(!engine.open_filled_position_from_pending(pending, 0.0));
        assert_eq!(engine.pending_exit_orders.len(), 1);
        assert_eq!(engine.pending_exit_orders[0].reason, "EMERGENCY FLATTEN");

        match order_rx.try_recv().expect("emergency sell order") {
            OrderCommand::Place(cmd) => {
                assert_eq!(cmd.tag, "SATAEXIT1");
                assert_eq!(cmd.quantity, 50);
                assert_eq!(cmd.side, OrderSide::Sell);
                assert!(cmd.limit_price.is_none(), "emergency flatten should use MARKET");
            }
            other => panic!("expected place command, got {:?}", other),
        }
        match order_rx.try_recv().expect("emergency sell status") {
            OrderCommand::StatusByTag { tag } => assert_eq!(tag, "SATAEXIT1"),
            other => panic!("expected status command, got {:?}", other),
        }
    }
}
