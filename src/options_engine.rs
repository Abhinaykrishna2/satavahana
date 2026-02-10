
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
use tracing::{info, warn};

const NIFTY_LOT_SIZE: u32 = 65;
#[allow(dead_code)]
const BANKNIFTY_LOT_SIZE: u32 = 30;
const NSE_OPTIONS_EXCHANGE_TXN_RATE: f64 = 0.000311;

fn entry_transaction_cost_per_lot(entry_price: f64, lot_size: u32) -> f64 {
    let premium = entry_price * lot_size as f64;
    let brokerage = 20.0_f64;
    let exchange_txn = NSE_OPTIONS_EXCHANGE_TXN_RATE * premium;
    let sebi = 0.000001 * premium;
    let stamp_duty = 0.00003 * premium;
    let gst = 0.18 * (brokerage + exchange_txn + sebi);
    brokerage + exchange_txn + sebi + stamp_duty + gst
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

fn exit_transaction_cost_per_lot(exit_price: f64, lot_size: u32, exit_time_ms: u64) -> f64 {
    let premium = exit_price * lot_size as f64;
    let brokerage = 20.0_f64;
    let stt_sell = options_sell_stt_rate(exit_time_ms) * premium;
    let exchange_txn = NSE_OPTIONS_EXCHANGE_TXN_RATE * premium;
    let sebi = 0.000001 * premium;
    let gst = 0.18 * (brokerage + exchange_txn + sebi);
    brokerage + stt_sell + exchange_txn + sebi + gst
}

fn round_trip_transaction_cost_per_lot(
    entry_price: f64,
    exit_price: f64,
    lot_size: u32,
    assumed_exit_time_ms: u64,
) -> f64 {
    entry_transaction_cost_per_lot(entry_price, lot_size)
        + exit_transaction_cost_per_lot(exit_price, lot_size, assumed_exit_time_ms)
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

#[derive(Debug, Clone, PartialEq)]
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
    stale_timeout_hit: bool,
    released_after_timeout: bool,
    order_id: Option<String>,
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
    kelly_scale: f64,
}

impl RiskEngine {
    fn new(initial_capital: f64, max_daily_loss_fraction: f64) -> Self {
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
        let cost_per_lot = option_price * lot_size as f64
            + entry_transaction_cost_per_lot(option_price, lot_size);
        if cost_per_lot <= 0.0 { return 0; }
        let affordable = (sizing_capital / cost_per_lot).floor() as u32;
        if affordable == 0 { return 0; }

        let max_risk_capital = self.daily_start_capital * 0.20;
        let risk_per_lot = (stop_loss_pct * option_price * lot_size as f64).max(1.0);
        let lots_from_risk = (max_risk_capital / risk_per_lot).floor() as u32;

        let lots_from_exposure = (sizing_capital * 0.60 / cost_per_lot).floor() as u32;

        lots_from_risk.min(lots_from_exposure).min(4).max(1).min(affordable)
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
        let daily_loss = self.daily_start_capital - self.capital;
        daily_loss > self.daily_start_capital * self.max_daily_loss_fraction
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
    prev_exch_ts: HashMap<u32, u32>,

    risk: RiskEngine,

    positions: Vec<Position>,
    next_pos_id: u64,
    next_sig_id: u64,

    journal: OptionsJournal,

    profit_target_pct: f64,
    stop_loss_pct: f64,
    min_confidence: f64,

    risk_free_rate: f64,
    dividend_yield: f64,

    scan_count: u64,

    clock_override_ms: Option<u64>,
    replay_last_scan_ms: Option<u64>,

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
    pending_capital_reserved: f64,
    max_concurrent_positions: usize,
    entry_order_timeout_ms: u64,
    order_status_poll_interval_ms: u64,
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
        let initial_capital = cfg.initial_capital.max(0.0);
        let max_daily_loss_fraction = (cfg.max_daily_loss_pct / 100.0).clamp(0.01, 0.95);
        let journal = OptionsJournal::new(log_dir, initial_capital)
            .unwrap_or_else(|e| {
                warn!("Failed to open options journal in '{}': {}", log_dir, e);
                OptionsJournal::new(".", initial_capital)
                    .expect("Could not open options journal even in current directory")
            });
        Self {
            contracts,
            store,
            underlying_tokens,
            iv_history: IVHistory::new(),
            spot_history: HashMap::new(),
            prev_oi: HashMap::new(),
            prev_exch_ts: HashMap::new(),
            risk: RiskEngine::new(initial_capital, max_daily_loss_fraction),
            journal,
            positions: Vec::new(),
            next_pos_id: 1,
            next_sig_id: 1,
            profit_target_pct: cfg.profit_target_pct.clamp(1.0, 200.0),
            stop_loss_pct: cfg.stop_loss_pct.clamp(1.0, 95.0),
            min_confidence: cfg.min_confidence.clamp(0.0, 100.0),
            risk_free_rate,
            dividend_yield,
            scan_count: 0,
            clock_override_ms: None,
            replay_last_scan_ms: None,
            scan_interval_secs: cfg.scan_interval_secs.max(1),
            max_daily_trades: cfg.max_daily_trades.clamp(1, 50),
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
            pending_capital_reserved: 0.0,
            max_concurrent_positions: 2,
            entry_order_timeout_ms: 5 * 60 * 1_000,
            order_status_poll_interval_ms: 2_000,
        }
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

    fn set_clock_override_ms(&mut self, timestamp_ms: u64) {
        self.clock_override_ms = Some(timestamp_ms);
    }

    fn clear_clock_override(&mut self) {
        self.clock_override_ms = None;
    }

    pub fn process_replay_tick(&mut self, timestamp_ms: u64) {
        self.set_clock_override_ms(timestamp_ms);
        self.process_order_updates();
        self.poll_pending_entry_orders();
        self.check_open_positions();

        let session = self.current_session();
        if matches!(session, SessionPhase::PreOpen | SessionPhase::AfterMarket) {
            return;
        }

        if !self.pending_signals.is_empty() {
            let min_delay_ms: u64 = 30_000;
            let oldest_age = self.pending_signals.front()
                .map(|s| timestamp_ms.saturating_sub(s.timestamp_ms))
                .unwrap_or(0);
            if oldest_age >= min_delay_ms {
                self.execute_pending_signals();
            }
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

    pub fn set_live_order_bridge(
        &mut self,
        order_tx: mpsc::UnboundedSender<OrderCommand>,
        order_updates_rx: Option<mpsc::UnboundedReceiver<OrderUpdate>>,
        tag_prefix: String,
    ) {
        self.order_tx = Some(order_tx);
        self.order_updates_rx = order_updates_rx;
        let clean = tag_prefix.trim();
        self.order_tag_prefix = if clean.is_empty() {
            "SATA".to_string()
        } else {
            clean.to_ascii_uppercase()
        };
    }

    fn build_order_tag(&self, stage: &str, id: u64) -> String {
        let mut tag = format!("{}-{}-{}", self.order_tag_prefix, stage, id);
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
        }

        for update in updates {
            self.handle_order_update(update);
        }
    }

    fn handle_order_update(&mut self, update: OrderUpdate) {
        let idx_opt = self.pending_entry_orders
            .iter()
            .position(|p| p.tag == update.tag);
        let Some(idx) = idx_opt else {
            return;
        };

        if let Some(order_id) = update.order_id.clone() {
            self.pending_entry_orders[idx].order_id = Some(order_id);
        }

        if let Some(msg) = update.message.as_deref() {
            if update.source == "place_error" {
                warn!("Entry order {} failed to place: {}", update.tag, msg);
                self.release_pending_entry(idx, true, "BUY place failed");
                return;
            }
            if update.source == "status_error" {
                warn!("Entry order {} status lookup issue: {}", update.tag, msg);
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
            if self
                .pending_entry_orders
                .get(idx)
                .map(|p| p.released_after_timeout)
                .unwrap_or(false)
            {
                self.flatten_stale_filled_entry(idx, avg_price);
            } else {
                self.promote_filled_entry(idx, avg_price);
            }
        } else if is_terminal_reject {
            let released = self.pending_entry_orders[idx].released_after_timeout;
            let reason = if self.pending_entry_orders[idx].stale_timeout_hit {
                format!("Stale entry order terminal status {}", status_upper)
            } else {
                format!("Entry order terminal status {}", status_upper)
            };
            self.release_pending_entry(idx, !released, &reason);
        }
    }

    fn poll_pending_entry_orders(&mut self) {
        if !self.live_mode_enabled() || self.pending_entry_orders.is_empty() {
            return;
        }
        let now = self.current_time_ms();
        let mut status_tags: Vec<String> = Vec::new();
        let mut cancel_tags: Vec<String> = Vec::new();
        let mut stale_indices: Vec<usize> = Vec::new();

        for (idx, pending) in self.pending_entry_orders.iter_mut().enumerate() {
            let age = now.saturating_sub(pending.placed_ms);
            if age >= self.entry_order_timeout_ms && !pending.cancel_requested {
                pending.cancel_requested = true;
                pending.stale_timeout_hit = true;
                cancel_tags.push(pending.tag.clone());
                status_tags.push(pending.tag.clone());
                stale_indices.push(idx);
                warn!(
                    "Entry order stale (> {}s): {} — cancel requested",
                    self.entry_order_timeout_ms / 1000,
                    pending.tag
                );
                continue;
            }

            if now.saturating_sub(pending.last_status_poll_ms) >= self.order_status_poll_interval_ms {
                pending.last_status_poll_ms = now;
                status_tags.push(pending.tag.clone());
            }
        }

        for idx in stale_indices {
            self.mark_pending_entry_stale(idx, "Stale order timeout");
        }
        for tag in cancel_tags {
            self.send_order_command(OrderCommand::CancelByTag { tag });
        }
        for tag in status_tags {
            self.send_order_command(OrderCommand::StatusByTag { tag });
        }
    }

    fn mark_pending_entry_stale(&mut self, idx: usize, reason: &str) {
        if idx >= self.pending_entry_orders.len() {
            return;
        }
        let pending = &mut self.pending_entry_orders[idx];
        if pending.released_after_timeout {
            return;
        }
        pending.released_after_timeout = true;
        self.pending_capital_reserved = (self.pending_capital_reserved - pending.estimated_capital).max(0.0);
        self.entry_rejections += 1;
        warn!("Pending entry marked stale [{}] tag={}", reason, pending.tag);
    }

    fn release_pending_entry(&mut self, idx: usize, count_as_rejection: bool, reason: &str) {
        if idx >= self.pending_entry_orders.len() {
            return;
        }
        let pending = self.pending_entry_orders.remove(idx);
        if !pending.released_after_timeout {
            self.pending_capital_reserved = (self.pending_capital_reserved - pending.estimated_capital).max(0.0);
        }
        if count_as_rejection {
            self.entry_rejections += 1;
        }
        warn!("Pending entry released [{}] tag={}", reason, pending.tag);
    }

    fn promote_filled_entry(&mut self, idx: usize, broker_avg_price: f64) {
        if idx >= self.pending_entry_orders.len() {
            return;
        }
        let pending = self.pending_entry_orders.remove(idx);
        if !pending.released_after_timeout {
            self.pending_capital_reserved = (self.pending_capital_reserved - pending.estimated_capital).max(0.0);
        }
        self.open_filled_position_from_pending(pending, broker_avg_price);
    }

    fn flatten_stale_filled_entry(&mut self, idx: usize, broker_avg_price: f64) {
        if idx >= self.pending_entry_orders.len() {
            return;
        }
        let pending = self.pending_entry_orders.remove(idx);
        if !pending.released_after_timeout {
            self.pending_capital_reserved = (self.pending_capital_reserved - pending.estimated_capital).max(0.0);
        }

        let quantity = pending.signal.lots.saturating_mul(pending.signal.lot_size);
        if quantity == 0 || pending.tradingsymbol.is_empty() {
            return;
        }

        warn!(
            "Stale entry filled late | tag={} avg={:.2} -> sending immediate SELL flatten order",
            pending.tag,
            broker_avg_price
        );
        let exit_tag = self.build_order_tag("EXIT", pending.pos_id);
        self.send_order_command(OrderCommand::Place(PlaceOrderCmd {
            tag: exit_tag.clone(),
            tradingsymbol: pending.tradingsymbol,
            quantity,
            side: OrderSide::Sell,
        }));
        self.send_order_command(OrderCommand::StatusByTag { tag: exit_tag });
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
            info!("  Profit target: {:.0}% | Stop: {:.0}% | Min confidence: {:.0}",
                self.profit_target_pct, self.stop_loss_pct, self.min_confidence);
            info!("  Scan interval: {}s", self.scan_interval_secs);
            info!("  Max daily trades: {}", self.max_daily_trades);
            info!("  Strategies: GammaScalp | IVExpansion | TrendFollow | MaxPain | OIDivergence | GEX");
            info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

            let mut scan_timer = tokio::time::interval(
                tokio::time::Duration::from_secs(self.scan_interval_secs)
            );

            loop {
                tokio::select! {
                    tick_result = rx.recv() => {
                        match tick_result {
                            Ok(_event) => {
                                self.clear_clock_override();
                                self.process_order_updates();
                                self.poll_pending_entry_orders();
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

                        self.scan_count += 1;
                        self.process_order_updates();
                        self.poll_pending_entry_orders();
                        self.run_full_scan();
                    }
                }
            }
        })
    }

    fn run_full_scan(&mut self) {
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

        let circuit_broken = self.risk.circuit_breaker_triggered();
        if circuit_broken {
            warn!("⛔ CIRCUIT BREAKER: Daily loss exceeded {:.0}% of starting capital. NO NEW TRADES.",
                self.risk.max_daily_loss_fraction * 100.0);
        }

        let midday_threshold = if matches!(session, SessionPhase::Midday) { 78.0 } else { self.min_confidence };
        let mut pending_batch: Vec<(Signal, usize)> = Vec::new();

        if !circuit_broken && !matches!(session, SessionPhase::OpeningBell | SessionPhase::Closing) {
            for (snap_idx, snap) in snapshots.iter().enumerate() {
                let signals = self.generate_signals(snap);
                for signal in signals {
                    if signal.confidence >= midday_threshold {
                        pending_batch.push((signal, snap_idx));
                    }
                }
            }
        }

        let cross_ce = pending_batch.iter().any(|(s, _)| s.underlying == "NIFTY" && s.action == SignalAction::BuyCE)
            && pending_batch.iter().any(|(s, _)| s.underlying == "BANKNIFTY" && s.action == SignalAction::BuyCE);
        let cross_pe = pending_batch.iter().any(|(s, _)| s.underlying == "NIFTY" && s.action == SignalAction::BuyPE)
            && pending_batch.iter().any(|(s, _)| s.underlying == "BANKNIFTY" && s.action == SignalAction::BuyPE);

        for (signal, snap_idx) in &mut pending_batch {
            let boosted = (cross_ce && signal.action == SignalAction::BuyCE)
                || (cross_pe && signal.action == SignalAction::BuyPE);
            if boosted {
                let boost = 8.0_f64;
                signal.confidence = (signal.confidence + boost).min(100.0);
                signal.reasons.insert(0, (
                    format!("Cross-underlying NIFTY+BANKNIFTY confirmation: both show same direction"),
                    boost,
                ));
                info!("  Cross-confirm: {} {} boosted to {:.0} confidence",
                    signal.underlying, signal.action, signal.confidence);
            }
            if let Some(snap) = snapshots.get(*snap_idx) {
                self.log_signal(signal, snap);
            }
        }

        for (signal, _) in pending_batch {
            self.pending_signals.push_back(signal);
        }

        if self.scan_count % 10 == 0 {
            self.log_portfolio_summary();
        }
    }

    fn execute_pending_signals(&mut self) {
        if self.pending_signals.is_empty() { return; }

        if self.risk.circuit_breaker_triggered() {
            let n = self.pending_signals.len();
            if n > 0 {
                warn!("Circuit breaker active: discarding {} pending signal(s)", n);
                self.pending_signals.clear();
            }
            return;
        }

        let session = self.current_session();
        if matches!(session, SessionPhase::Closing | SessionPhase::AfterMarket | SessionPhase::PreOpen) {
            self.pending_signals.clear();
            return;
        }

        let max_age_ms = self.scan_interval_secs.saturating_mul(2_000);

        let pending: Vec<Signal> = self.pending_signals.drain(..).collect();
        for mut signal in pending {
            let age_ms = self.current_time_ms().saturating_sub(signal.timestamp_ms);
            if age_ms > max_age_ms {
                warn!("Pending signal #{}: stale ({:.0}s old), discarding",
                    signal.id, age_ms as f64 / 1000.0);
                continue;
            }

            let token = match signal.action {
                SignalAction::BuyCE => self.contracts.iter()
                    .find(|c| c.underlying == signal.underlying
                        && c.strike == signal.strike
                        && c.option_type == OptionType::CE)
                    .map(|c| c.instrument_token)
                    .unwrap_or(0),
                SignalAction::BuyPE => self.contracts.iter()
                    .find(|c| c.underlying == signal.underlying
                        && c.strike == signal.strike
                        && c.option_type == OptionType::PE)
                    .map(|c| c.instrument_token)
                    .unwrap_or(0),
                _ => 0,
            };
            if token == 0 { continue; }

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

            let post_stop_cooldown_ms: u64 = 10 * 60 * 1_000;
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
    }


    fn estimate_synthetic_spot(&self, strike_map: &BTreeMap<u64, StrikeLevel>) -> Option<f64> {
        let mut implied_spots = Vec::new();
        for level in strike_map.values() {
            if level.ce_ltp > 0.01 && level.pe_ltp > 0.01 {
                let implied = level.strike + level.ce_ltp - level.pe_ltp;
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
            let prev_ts = *self.prev_exch_ts.get(&contract.instrument_token).unwrap_or(&0);
            let curr_ts = tick.exchange_ts;
            let oi_change = if curr_ts > 0 && curr_ts != prev_ts && curr_oi > 0 {
                curr_oi as i64 - prev_oi as i64
            } else {
                0i64
            };
            self.prev_oi.insert(contract.instrument_token, curr_oi);
            self.prev_exch_ts.insert(contract.instrument_token, curr_ts);

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
        let strike_mid = (min_strike + max_strike) / 2.0;

        let spot_from_token = self.underlying_tokens.get(underlying)
            .and_then(|tok| self.store.get(*tok))
            .map(|t| t.ltp)
            .filter(|v| *v > 0.0);
        let synthetic_spot = self.estimate_synthetic_spot(&strike_map);

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
            level.iv_skew = level.ce_iv - level.pe_iv;
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
        let atm_iv = atm_level.map(|l| (l.ce_iv + l.pe_iv) / 2.0).unwrap_or(0.0);

        let lot_size = self.contracts.iter()
            .find(|c| c.underlying == underlying)
            .map(|c| c.lot_size)
            .unwrap_or(NIFTY_LOT_SIZE);

        let net_gex = compute_gex(&strikes, spot, lot_size);
        let iv_rank = self.iv_history.iv_rank(underlying, atm_iv);

        let mut strikes_mut = strikes;
        for level in &mut strikes_mut {
            level.gex_contribution = (level.ce_gamma * level.ce_oi as f64
                - level.pe_gamma * level.pe_oi as f64) * spot * lot_size as f64;
        }

        let regime = self.detect_regime(pcr_oi, atm_iv, spot, &strikes_mut);

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
        if prices.len() < 6 {
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

    fn detect_regime(
        &self,
        pcr_oi: f64,
        atm_iv: f64,
        spot: f64,
        strikes: &[StrikeLevel],
    ) -> MarketRegime {
        if atm_iv > 0.25 { return MarketRegime::PanicHighVol; }
        if atm_iv < 0.11 { return MarketRegime::ComplacencyLowVol; }

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

        let lot_size = self.contracts.iter()
            .find(|c| c.underlying == snap.underlying)
            .map(|c| c.lot_size)
            .unwrap_or(NIFTY_LOT_SIZE);

        if self.positions.iter().any(|p| p.is_open && p.underlying == snap.underlying) {
            return signals;
        }

        let cooldown_ms: u64 = 5 * 60 * 1_000;
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
            >= 2
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

        if let Some(s) = self.strategy_gamma_scalp(snap)   { scored.push(s); }
        if let Some(s) = self.strategy_iv_expansion(snap)  { scored.push(s); }
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

        if scored.is_empty() { return signals; }

        let ce_count = scored.iter().filter(|(a, _, _, _)| *a == SignalAction::BuyCE).count();
        let pe_count = scored.iter().filter(|(a, _, _, _)| *a == SignalAction::BuyPE).count();

        let dominant_action = if ce_count > pe_count { SignalAction::BuyCE }
                              else if pe_count > ce_count { SignalAction::BuyPE }
                              else { SignalAction::Hold };

        if dominant_action == SignalAction::Hold { return signals; }

        let dominant_signals: Vec<_> = scored.iter()
            .filter(|(a, _, _, _)| *a == dominant_action)
            .collect();
        if dominant_signals.is_empty() {
            return signals;
        }

        let (composite_score, merged_reasons, primary_strategy) = {

            let max_score = dominant_signals.iter().map(|(_, s, _, _)| *s).fold(0.0_f64, f64::max);
            let composite = (max_score + dominant_signals.iter().map(|(_, s, _, _)| *s * 0.25).sum::<f64>())
                .min(100.0);

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

        if composite_score < self.min_confidence { return signals; }

        let target_strike = self.pick_strike(snap, &dominant_action, lot_size);
        let option_price = match dominant_action {
            SignalAction::BuyCE => target_strike.map(|s| s.ce_ltp).unwrap_or(0.0),
            SignalAction::BuyPE => target_strike.map(|s| s.pe_ltp).unwrap_or(0.0),
            _ => 0.0,
        };

        if option_price < 15.0 { return signals; }

        let effective_target_pct = if compressive {
            (sideways_threshold_pct * 100.0 * 55.0)
                .clamp(12.0, 28.0)
                .min(self.profit_target_pct)
        } else {
            self.profit_target_pct
        };
        let effective_stop_pct = if compressive {
            (self.stop_loss_pct * effective_target_pct / self.profit_target_pct)
                .max(self.stop_loss_pct / 2.0)
        } else {
            self.stop_loss_pct
        };

        let effective_target_pct = {
            let session_now = self.current_session();
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

        let entry_cost_per_lot = entry_transaction_cost_per_lot(option_price, lot_size);
        let cost_per_lot = option_price * lot_size as f64 + entry_cost_per_lot;
        let capital_required = cost_per_lot * lots as f64;
        if capital_required > self.risk.available_capital() {
            return signals;
        }

        let target_price = option_price * (1.0 + effective_target_pct / 100.0);
        let stop_price   = option_price * (1.0 - effective_stop_pct / 100.0);

        let straddle_prem = target_strike.map(|s| s.straddle_premium).unwrap_or(option_price * 2.0);
        let breakeven_move_pct = (straddle_prem / snap.spot) * 100.0;

        let max_loss = (option_price - stop_price) * lot_size as f64 * lots as f64
            + round_trip_transaction_cost_per_lot(
                option_price,
                stop_price,
                lot_size,
                self.current_time_ms(),
            ) * lots as f64;
        let max_profit = (target_price - option_price) * lot_size as f64 * lots as f64
            - round_trip_transaction_cost_per_lot(
                option_price,
                target_price,
                lot_size,
                self.current_time_ms(),
            ) * lots as f64;
        let risk_reward = if max_loss > 0.0 { max_profit / max_loss } else { 0.0 };

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
        let max_affordable = self.risk.available_capital() * 0.60;

        let atm_idx = snap.strikes.iter()
            .enumerate()
            .min_by(|(_, a), (_, b)| {
                (a.strike - snap.spot).abs().partial_cmp(&(b.strike - snap.spot).abs()).unwrap()
            })
            .map(|(i, _)| i)?;

        let tier = self.risk.capital_tier();
        let otm_offset: usize = if tier == 1 { 1 } else { 0 };

        let candidate_idx = match action {
            SignalAction::BuyCE => (atm_idx + otm_offset).min(snap.strikes.len() - 1),
            SignalAction::BuyPE => atm_idx.saturating_sub(otm_offset),
            _ => atm_idx,
        };

        let candidate = &snap.strikes[candidate_idx];

        let price = match action {
            SignalAction::BuyCE => candidate.ce_ltp,
            SignalAction::BuyPE => candidate.pe_ltp,
            _ => 0.0,
        };

        if price * lot_size as f64 <= max_affordable && price >= 15.0 { Some(candidate) }
        else {
            let max_otm = 2usize;
            match action {
                SignalAction::BuyCE => {
                    let end = (atm_idx + max_otm + 1).min(snap.strikes.len());
                    snap.strikes[atm_idx..end]
                        .iter()
                        .find(|s| s.ce_ltp * lot_size as f64 <= max_affordable && s.ce_ltp >= 15.0)
                }
                SignalAction::BuyPE => {
                    let start = atm_idx.saturating_sub(max_otm);
                    snap.strikes[start..=atm_idx]
                        .iter()
                        .rev()
                        .find(|s| s.pe_ltp * lot_size as f64 <= max_affordable && s.pe_ltp >= 15.0)
                }
                _ => Some(candidate),
            }
        }
    }

    fn strategy_gamma_scalp(&self, snap: &ChainSnapshot) -> Option<(SignalAction, f64, StrategyType, Vec<(String, f64)>)> {
        if snap.days_to_expiry > 2.5 { return None; }
        if snap.atm_iv > 0.40 { return None; }

        let session = self.current_session();
        if !matches!(session, SessionPhase::Morning | SessionPhase::Afternoon) { return None; }

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

        let (action, pcr_score, pcr_reason) = if snap.pcr_oi > 1.3 {
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

        if score < self.min_confidence { return None; }
        Some((action, score.min(100.0), StrategyType::GammaScalp, reasons))
    }

    fn strategy_iv_expansion(&self, snap: &ChainSnapshot) -> Option<(SignalAction, f64, StrategyType, Vec<(String, f64)>)> {
        if snap.iv_rank > 40.0 { return None; }
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

        if score < self.min_confidence { return None; }
        Some((action, score.min(100.0), StrategyType::IVExpansion, reasons))
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

        let lo = atm_idx.saturating_sub(3);
        let hi = (atm_idx + 4).min(snap.strikes.len());
        let nearby = &snap.strikes[lo..hi];

        let ce_oi_change: i64 = nearby.iter().map(|s| s.ce_oi_change).sum();
        let pe_oi_change: i64 = nearby.iter().map(|s| s.pe_oi_change).sum();

        let (action, oi_score, oi_reason) = if ce_oi_change > 0 && snap.regime == MarketRegime::Bullish {
            (SignalAction::BuyCE, 25.0,
             format!("CE OI +{} with bullish regime: new long buildup", ce_oi_change))
        } else if pe_oi_change > 0 && snap.regime == MarketRegime::Bearish {
            (SignalAction::BuyPE, 25.0,
             format!("PE OI +{} with bearish regime: new short buildup", pe_oi_change))
        } else if ce_oi_change < 0 && snap.regime == MarketRegime::Bearish {
            (SignalAction::BuyPE, 18.0,
             format!("CE OI {} (unwinding) + bearish: longs exiting, further down", ce_oi_change))
        } else if pe_oi_change < 0 && snap.regime == MarketRegime::Bullish {
            (SignalAction::BuyCE, 18.0,
             format!("PE OI {} (unwinding) + bullish: bears exiting, further up", pe_oi_change))
        } else {
            return None;
        };
        score += oi_score;
        reasons.push((oi_reason, oi_score));

        let ce_vol_surge: u64 = nearby.iter().map(|s| s.ce_volume).sum();
        let pe_vol_surge: u64 = nearby.iter().map(|s| s.pe_volume).sum();
        if action == SignalAction::BuyCE && ce_vol_surge > pe_vol_surge * 2 {
            score += 15.0;
            reasons.push((format!("CE volume ({}) >> PE volume ({}): buying pressure", ce_vol_surge, pe_vol_surge), 15.0));
        } else if action == SignalAction::BuyPE && pe_vol_surge > ce_vol_surge * 2 {
            score += 15.0;
            reasons.push((format!("PE volume ({}) >> CE volume ({}): selling pressure", pe_vol_surge, ce_vol_surge), 15.0));
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
            (SignalAction::BuyPE, MarketRegime::StrongBearish) => {
                score += 20.0;
                reasons.push(("Strong bearish regime confirms PE buy".into(), 20.0));
            }
            _ => {}
        }

        if score < self.min_confidence { return None; }
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

        if score < self.min_confidence { return None; }
        Some((action, score.min(100.0), StrategyType::MaxPainConvergence, reasons))
    }

    fn strategy_oi_divergence(&self, snap: &ChainSnapshot) -> Option<(SignalAction, f64, StrategyType, Vec<(String, f64)>)> {
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
        if nearby_volume < 10_000 { return None; }

        let (dominant_change, action) = if net_ce_oi_change.abs() > net_pe_oi_change.abs() {
            (net_ce_oi_change, if net_ce_oi_change > 0 { SignalAction::BuyCE } else { SignalAction::BuyPE })
        } else {
            (net_pe_oi_change, if net_pe_oi_change > 0 { SignalAction::BuyPE } else { SignalAction::BuyCE })
        };

        if dominant_change.abs() < 1000 { return None; }

        let sideways = self
            .is_spot_sideways(&snap.underlying, snap.atm_iv, snap.days_to_expiry)
            .map(|(s, _, _, _, _)| s)
            .unwrap_or(false);
        if sideways {
            if dominant_change.abs() < 1_000_000 {
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

        let oi_score = (dominant_change.abs() as f64 / 10_000.0).min(30.0);
        score += oi_score;
        reasons.push((
            format!("{} OI change {}: strong institutional positioning",
                if action == SignalAction::BuyCE { "CE" } else { "PE" },
                dominant_change),
            oi_score,
        ));

        if (action == SignalAction::BuyCE && matches!(snap.regime, MarketRegime::Bullish | MarketRegime::StrongBullish))
        || (action == SignalAction::BuyPE && matches!(snap.regime, MarketRegime::Bearish | MarketRegime::StrongBearish)) {
            score += 20.0;
            reasons.push((format!("OI divergence aligned with {} regime", snap.regime), 20.0));
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

        if score < self.min_confidence { return None; }
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
            let skew_score = (avg_skew * 200.0).min(30.0);
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
            score += 15.0;
            reasons.push((format!("PCR {:.2} confirms skew reversion direction", snap.pcr_oi), 15.0));
        }

        if snap.days_to_expiry > 2.0 {
            score += 8.0;
            reasons.push((format!("{:.0} DTE: sufficient time for skew to revert", snap.days_to_expiry), 8.0));
        }

        if score < self.min_confidence { return None; }
        Some((action, score.min(100.0), StrategyType::IVSkewReversion, reasons))
    }

    fn strategy_gex_play(&self, snap: &ChainSnapshot) -> Option<(SignalAction, f64, StrategyType, Vec<(String, f64)>)> {
        if snap.net_gex >= 0.0 { return None; }

        let gex_magnitude = snap.net_gex.abs();
        if gex_magnitude < 1e6 { return None; }

        let mut score = 0.0_f64;
        let mut reasons = Vec::new();

        let gex_score = (gex_magnitude.log10() - 5.0).max(0.0) * 10.0;
        let clamped = gex_score.min(30.0);
        score += clamped;
        reasons.push((
            format!("Net GEX {:.2e} (negative): dealers short gamma, moves will be amplified", snap.net_gex),
            clamped,
        ));

        let action = match snap.regime {
            MarketRegime::StrongBullish | MarketRegime::Bullish => {
                score += 20.0;
                reasons.push(("Bullish regime + negative GEX = explosive upside possible".into(), 20.0));
                SignalAction::BuyCE
            }
            MarketRegime::StrongBearish | MarketRegime::Bearish => {
                score += 20.0;
                reasons.push(("Bearish regime + negative GEX = explosive downside possible".into(), 20.0));
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

        if snap.iv_rank < 65.0 {
            score += 10.0;
            reasons.push((format!("IV Rank {:.0}%: options still reasonably priced for GEX play", snap.iv_rank), 10.0));
        }

        if score < self.min_confidence { return None; }
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
        if used_slots >= self.max_daily_trades {
            self.entry_rejections += 1;
            warn!(
                "Position rejected (daily trade cap): used {} / {}",
                used_slots,
                self.max_daily_trades
            );
            return;
        }

        let (token, tradingsymbol) = match signal.action {
            SignalAction::BuyCE => {
                self.contracts.iter()
                    .find(|c| c.underlying == signal.underlying
                        && c.strike == signal.strike
                        && c.option_type == OptionType::CE)
                    .map(|c| (c.instrument_token, c.tradingsymbol.clone()))
                    .unwrap_or((0, String::new()))
            }
            SignalAction::BuyPE => {
                self.contracts.iter()
                    .find(|c| c.underlying == signal.underlying
                        && c.strike == signal.strike
                        && c.option_type == OptionType::PE)
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

        let entry_fill_price = (signal.option_price + self.execution_buy_offset_inr).max(0.05);
        let entry_costs = entry_transaction_cost_per_lot(entry_fill_price, signal.lot_size)
            * signal.lots as f64;
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
            self.submit_live_entry_order(signal, token, tradingsymbol, capital_deployed);
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
    ) {
        let quantity = signal.lots.saturating_mul(signal.lot_size);
        if quantity == 0 || tradingsymbol.is_empty() {
            self.entry_rejections += 1;
            return;
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
            stale_timeout_hit: false,
            released_after_timeout: false,
            order_id: None,
        });
        self.last_signal_ms.insert(signal.underlying.clone(), now);

        info!(
            "📨 ENTRY ORDER SUBMITTED | tag={} | {} {} {:.0} {} | qty={} | est capital ₹{:.2}",
            tag,
            signal.underlying,
            signal.action,
            signal.strike,
            signal.expiry,
            quantity,
            estimated_capital
        );
        self.send_order_command(OrderCommand::Place(PlaceOrderCmd {
            tag: tag.clone(),
            tradingsymbol,
            quantity,
            side: OrderSide::Buy,
        }));
        self.send_order_command(OrderCommand::StatusByTag { tag });
    }

    fn open_filled_position_from_pending(
        &mut self,
        pending: PendingEntryOrder,
        broker_avg_price: f64,
    ) {
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
        let entry_costs = entry_transaction_cost_per_lot(entry_fill_price, signal.lot_size)
            * signal.lots as f64;
        let capital_deployed = entry_fill_price * signal.lot_size as f64 * signal.lots as f64
            + entry_costs;
        if !self.risk.reserve_capital(capital_deployed) {
            self.entry_rejections += 1;
            warn!(
                "Filled BUY could not reserve capital (late-state mismatch). tag={} deployed ₹{:.2}",
                pending.tag,
                capital_deployed
            );
            let exit_tag = self.build_order_tag("EXIT", pending.pos_id);
            let quantity = signal.lots.saturating_mul(signal.lot_size);
            if quantity > 0 {
                self.send_order_command(OrderCommand::Place(PlaceOrderCmd {
                    tag: exit_tag.clone(),
                    tradingsymbol: pending.tradingsymbol,
                    quantity,
                    side: OrderSide::Sell,
                }));
                self.send_order_command(OrderCommand::StatusByTag { tag: exit_tag });
            }
            return;
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
    }

    fn close_position_at(&mut self, idx: usize, current_price: f64, reason: String) {
        let exit_time_ms = self.current_time_ms();
        let (
            trade_id,
            entry_ctx,
            underlying,
            tradingsymbol,
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
            let Some(pos) = self.positions.get_mut(idx) else {
                return;
            };
            if !pos.is_open {
                return;
            }

            let exit_fill_price = (current_price - self.execution_sell_offset_inr).max(0.05);
            pos.is_open = false;
            pos.exit_price = exit_fill_price;
            pos.exit_time_ms = exit_time_ms;
            pos.exit_reason = reason.clone();

            let gross_pnl = (exit_fill_price - pos.entry_price) * pos.lot_size as f64 * pos.lots as f64;
            let entry_costs = entry_transaction_cost_per_lot(pos.entry_price, pos.lot_size) * pos.lots as f64;
            let exit_costs = exit_transaction_cost_per_lot(exit_fill_price, pos.lot_size, exit_time_ms) * pos.lots as f64;
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
                pos.id,
                pos.entry_ctx.clone(),
                pos.underlying.clone(),
                pos.tradingsymbol.clone(),
                option_type_str,
                pos.strike,
                pos.expiry.clone(),
                pos.entry_datetime.clone(),
                datetime_string_from_ms(pos.exit_time_ms),
                pos.entry_price,
                pos.exit_price,
                pos.lots,
                pos.lot_size,
                pos.capital_deployed,
                pos.target_price,
                pos.stop_price,
                gross_pnl,
                costs,
                pos.pnl,
                pos.pnl_pct,
                holding_mins,
            )
        };

        let is_stop = reason.starts_with("STOP HIT");
        let is_target = reason.starts_with("TARGET HIT");

        self.risk.release_reserved_capital(capital_deployed);
        self.risk.record_trade(pnl);
        self.positions_closed += 1;

        if is_stop {
            let next = self
                .stop_streak_by_underlying
                .get(&underlying)
                .copied()
                .unwrap_or(0)
                .saturating_add(1);
            self.stop_streak_by_underlying.insert(underlying.clone(), next);
        } else if is_target {
            self.stop_streak_by_underlying.insert(underlying.clone(), 0);
        }

        let quantity = lots.saturating_mul(lot_size);
        if quantity > 0 && !tradingsymbol.is_empty() {
            let entry_tag = self.build_order_tag("ENTRY", trade_id);
            self.send_order_command(OrderCommand::StatusByTag {
                tag: entry_tag.clone(),
            });

            let exit_tag = self.build_order_tag("EXIT", trade_id);
            self.send_order_command(OrderCommand::Place(PlaceOrderCmd {
                tag: exit_tag.clone(),
                tradingsymbol,
                quantity,
                side: OrderSide::Sell,
            }));
            self.send_order_command(OrderCommand::StatusByTag { tag: exit_tag });
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
        let session = self.current_session();
        let force_exit = matches!(session, SessionPhase::Closing);

        for i in 0..self.positions.len() {
            let (is_open, option_token, entry_price, target_price, breakeven_done) = {
                let pos = &self.positions[i];
                (pos.is_open, pos.option_token, pos.entry_price, pos.target_price, pos.breakeven_stop_set)
            };
            if !is_open || breakeven_done { continue; }

            let current_price = match self.store.get(option_token) {
                Some(t) if t.ltp > 0.0 => t.ltp,
                _ => continue,
            };

            let trigger_level = entry_price + (target_price - entry_price) * 0.65;
            if current_price >= trigger_level && entry_price > 0.0 {
                let pos = &mut self.positions[i];
                let be_stop = pos.entry_price * 0.98;
                if pos.stop_price < be_stop {
                    pos.stop_price = be_stop;
                    pos.breakeven_stop_set = true;
                    info!("  Breakeven stop: position #{} {} stop raised to ₹{:.2} (2% below entry; current ₹{:.2}, target ₹{:.2})",
                        pos.id, pos.underlying, pos.stop_price, current_price, pos.target_price);
                } else {
                    pos.breakeven_stop_set = true;
                }
            }
        }

        for i in 0..self.positions.len() {
            let (is_open, option_token, target_price, stop_price) = {
                let pos = &self.positions[i];
                (pos.is_open, pos.option_token, pos.target_price, pos.stop_price)
            };
            if !is_open {
                continue;
            }

            let current_price = match self.store.get(option_token) {
                Some(t) if t.ltp > 0.0 => t.ltp,
                _ => continue,
            };

            let exit_reason = if force_exit {
                Some("Time stop: market closing".to_string())
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
