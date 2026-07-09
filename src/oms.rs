//! Order-management system: the single owner of risk, positions, and fills.
//!
//! One [`PositionManager`] is driven by one task. On every depth tick it, in order:
//!   1. checks exits (stop / target / time) for open positions,
//!   2. fills resting orders placed on a **strictly earlier** tick,
//!   3. (Phase 4) re-pegs unfilled orders — the limit chase,
//!   4. evaluates strategies and places new resting orders (born this tick, so
//!      they cannot fill until the next one).
//!
//! Because strategy evaluation and order management live in the same single tick
//! pass, signal→fill latency is deterministically ≥1 tick — no same-tick fills and
//! no look-ahead. The paper backend fills a resting limit only when a later tick
//! *touches* the limit price, and records P&L **net of transaction costs**.

use crate::cli::RunMode;
use crate::costs;
use crate::execution::{OrderCommand, OrderSide, PlaceOrderCmd};
use crate::microbook::BookTracker;
use crate::models::MarketDepth;
use crate::risk::{PositionSide, RiskManager};
use crate::strategy::gamma::{self, GammaMeta, GammaParams};
use crate::strategy::imbalance::{self, EquityMeta, ImbalanceParams};
use crate::strategy::{InstrumentKind, Signal, StrategyKind};

use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc::UnboundedSender;
use tracing::{info, warn};

/// Routes live order commands to the right pre-configured executor: equities go to
/// an NSE/MIS executor, options to an NFO/NRML executor (each carries its own
/// exchange/product). Set only in `--live` mode; `None` => paper.
pub struct LiveBridge {
    pub equity_tx: UnboundedSender<OrderCommand>,
    pub options_tx: UnboundedSender<OrderCommand>,
}

impl LiveBridge {
    fn tx_for(&self, kind: InstrumentKind) -> &UnboundedSender<OrderCommand> {
        match kind {
            InstrumentKind::Equity => &self.equity_tx,
            InstrumentKind::Option => &self.options_tx,
        }
    }
}

/// Top of book for a token at the current tick.
#[derive(Debug, Clone, Copy, Default)]
pub struct BookTop {
    pub bid: f64,
    pub ask: f64,
}

impl BookTop {
    pub fn two_sided(&self) -> bool {
        self.bid > 0.0 && self.ask > 0.0 && self.ask >= self.bid
    }
}

/// Ticks an unfilled live exit crosses THROUGH the touch once its chase window
/// elapses, to sweep the book and guarantee the square-off (certainty over price —
/// an exit is a stop/target/flatten that MUST fill). 10 ticks = ₹0.50 on NSE.
const EXIT_CROSS_TICKS: f64 = 10.0;

/// Trailing stop (mirrors the options engine): once the trade has run this fraction of
/// the way from entry to target in our favour, ratchet the stop up to lock the gain.
const TRAIL_TRIGGER_FRAC: f64 = 0.65;
/// After triggering, trail the stop behind the favourable high-water mark by this
/// fraction of the original stop distance (the remainder is locked-in profit).
const TRAIL_GIVEBACK_FRAC: f64 = 0.5;

/// A BUY limit at `limit` fills when a later tick's best ask trades down to it.
pub fn buy_limit_fills(limit: f64, ask: f64) -> bool {
    ask > 0.0 && ask <= limit
}

/// A SELL limit at `limit` fills when a later tick's best bid trades up to it.
pub fn sell_limit_fills(limit: f64, bid: f64) -> bool {
    bid > 0.0 && bid >= limit
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitReason {
    Stop,
    Target,
    Time,
    Flatten,
}

impl ExitReason {
    pub fn label(self) -> &'static str {
        match self {
            ExitReason::Stop => "STOP",
            ExitReason::Target => "TARGET",
            ExitReason::Time => "TIME",
            ExitReason::Flatten => "FLATTEN",
        }
    }
}

#[derive(Debug, Clone)]
struct PendingOrder {
    signal: Signal,
    limit_price: f64,
    qty: u32,
    /// Per-token tick index when this order was placed / last re-pegged. Fills are
    /// only attempted on ticks with a strictly greater index (no look-ahead).
    born_tick: u64,
    repegs: u32,
    placed_ms: u64,
    last_repeg_ms: u64,
    /// True once the order has crossed the spread to take liquidity (final attempt).
    crossed: bool,
    /// A cancel has been sent; keep the order tracked until the broker confirms
    /// cancelled/rejected or reports a late fill.
    cancel_requested: bool,
    /// Live order tag (empty in paper mode).
    tag: String,
}

#[derive(Debug, Clone)]
struct OpenPosition {
    signal: Signal,
    entry_price: f64,
    qty: u32,
    stop_price: f64,
    target_price: f64,
    opened_ms: u64,
    /// Live exit bookkeeping: once an exit order is sent we wait for its fill.
    exiting: bool,
    exit_tag: String,
    exit_reason: ExitReason,
    pending_exit_px: f64,
    /// When the live exit order was first dispatched — drives the time-bounded chase.
    exit_placed_ms: u64,
    /// Last time the exit order's price was modified — enforces the re-peg rate cap.
    exit_last_action_ms: u64,
    /// True once the exit has crossed the spread (marketable fallback sent) — a live
    /// exit limit at the touch is then re-priced THROUGH the book to guarantee a fill,
    /// so a stop/target/flatten can never rest unfilled while the market runs away.
    exit_crossed: bool,
    /// Best favourable price seen since entry (bid for a long, ask for a short) — drives
    /// the trailing stop.
    high_water: f64,
    /// True once the trailing stop has ratcheted (profit-lock armed).
    breakeven_set: bool,
}

/// A completed trade, with post-cost P&L, for the ledger.
#[derive(Debug, Clone)]
pub struct ClosedTrade {
    pub symbol: String,
    pub kind: StrategyKind,
    pub side: PositionSide,
    pub entry_price: f64,
    pub exit_price: f64,
    pub qty: u32,
    pub gross_pnl: f64,
    pub cost: f64,
    pub net_pnl: f64,
    pub reason: ExitReason,
    pub opened_ms: u64,
    pub closed_ms: u64,
    pub rationale: String,
}

/// Tunables for the manager (sourced from config at wiring time).
#[derive(Debug, Clone)]
pub struct ManagerConfig {
    pub max_concurrent_positions: u32,
    pub cooldown_ms: u64,
    pub mis_leverage: f64,
    pub max_hold_ms: u64,
    // ---- Limit-chase ----
    /// Max re-peg actions before the order crosses the spread to secure a fill.
    pub max_repegs: u32,
    /// Re-peg only when the resting price drifts at least this many ticks from the
    /// market — avoids needless modifies (and respects the 10 orders/sec cap).
    pub repeg_threshold_ticks: u32,
    /// Total time an order may chase before crossing to take liquidity.
    pub chase_timeout_ms: u64,
    /// Minimum gap between two re-pegs on the same order.
    pub min_repeg_interval_ms: u64,
    /// Price tick size (NSE = ₹0.05).
    pub tick_size: f64,
    /// A resting entry order is abandoned (cancelled) if it has not filled within
    /// this many ms — prevents stale orders from wedging the concurrency slot.
    pub order_ttl_ms: u64,
}

impl Default for ManagerConfig {
    fn default() -> Self {
        ManagerConfig {
            max_concurrent_positions: 1,
            cooldown_ms: 30_000,
            mis_leverage: 5.0,
            max_hold_ms: 30 * 60 * 1_000, // 30 min intraday hold cap
            max_repegs: 5,
            repeg_threshold_ticks: 1,
            chase_timeout_ms: 3_000,
            min_repeg_interval_ms: 250,
            tick_size: 0.05,
            order_ttl_ms: 60_000,
        }
    }
}

pub struct PositionManager {
    mode: RunMode,
    risk: RiskManager,
    book: BookTracker,
    cfg: ManagerConfig,

    // Strategy 1
    eq_enabled: bool,
    eq_params: ImbalanceParams,
    eq_meta: HashMap<u32, EquityMeta>,

    // Strategy 2
    gamma_enabled: bool,
    gamma_params: GammaParams,
    gamma_meta: HashMap<u32, GammaMeta>,
    is_tuesday: bool,

    // Per-token state
    tops: HashMap<u32, BookTop>,
    tick_no: HashMap<u32, u64>,

    pending: Vec<PendingOrder>,
    open: Vec<OpenPosition>,
    closed: Vec<ClosedTrade>,

    last_open_ms: u64,
    flattened: bool,

    // Live order routing (None in paper mode).
    live: Option<LiveBridge>,
    tag_prefix: String,
    order_seq: u64,

    // Account-level shared circuit (halts entries + receives net P&L). None = solo.
    shared_circuit: Option<crate::portfolio::SharedCircuit>,
}

impl PositionManager {
    pub fn new(
        mode: RunMode,
        risk: RiskManager,
        lookback: usize,
        cfg: ManagerConfig,
    ) -> Self {
        PositionManager {
            mode,
            risk,
            book: BookTracker::new(lookback),
            cfg,
            eq_enabled: false,
            eq_params: ImbalanceParams::default(),
            eq_meta: HashMap::new(),
            gamma_enabled: false,
            gamma_params: GammaParams::default(),
            gamma_meta: HashMap::new(),
            is_tuesday: false,
            tops: HashMap::new(),
            tick_no: HashMap::new(),
            pending: Vec::new(),
            open: Vec::new(),
            closed: Vec::new(),
            last_open_ms: 0,
            flattened: false,
            live: None,
            tag_prefix: "SATA".to_string(),
            order_seq: 0,
            shared_circuit: None,
        }
    }

    /// Attach the account-level shared circuit (halts entries + receives net P&L).
    pub fn set_shared_circuit(&mut self, c: crate::portfolio::SharedCircuit) {
        self.shared_circuit = Some(c);
    }

    /// Arm live order routing. Only call in `--live` mode.
    pub fn arm_live(&mut self, bridge: LiveBridge, tag_prefix: String) {
        self.live = Some(bridge);
        self.tag_prefix = sanitize_tag_prefix(&tag_prefix);
    }

    fn dispatch_place(&self, sig: &Signal, tag: &str, qty: u32, price: f64) {
        if let Some(b) = &self.live {
            let cmd = OrderCommand::Place(PlaceOrderCmd {
                tag: tag.to_string(),
                tradingsymbol: sig.symbol.clone(),
                quantity: qty,
                side: match sig.side {
                    PositionSide::Long => OrderSide::Buy,
                    PositionSide::Short => OrderSide::Sell,
                },
                limit_price: Some(price),
            });
            if let Err(e) = b.tx_for(sig.instrument).send(cmd) {
                warn!("[OMS] live Place dispatch failed for {}: {}", tag, e);
            }
        }
    }

    /// Dispatch a true MARKET order (`limit_price: None`). Used for the EOD flatten of
    /// a non-exiting position, where certainty of square-off beats a stale-top limit.
    fn dispatch_place_market(&self, sig: &Signal, tag: &str, qty: u32) {
        if let Some(b) = &self.live {
            let cmd = OrderCommand::Place(PlaceOrderCmd {
                tag: tag.to_string(),
                tradingsymbol: sig.symbol.clone(),
                quantity: qty,
                side: match sig.side {
                    PositionSide::Long => OrderSide::Buy,
                    PositionSide::Short => OrderSide::Sell,
                },
                limit_price: None, // None => MARKET order in the execution layer
            });
            if let Err(e) = b.tx_for(sig.instrument).send(cmd) {
                warn!("[OMS] live MARKET dispatch failed for {}: {}", tag, e);
            }
        }
    }

    fn dispatch_modify(&self, tag: &str, price: f64, kind: InstrumentKind) {
        if let Some(b) = &self.live {
            let cmd = OrderCommand::ModifyByTag { tag: tag.to_string(), new_price: price };
            if let Err(e) = b.tx_for(kind).send(cmd) {
                warn!("[OMS] live Modify dispatch failed for {}: {}", tag, e);
            }
        }
    }

    fn dispatch_cancel(&self, tag: &str, kind: InstrumentKind) {
        if let Some(b) = &self.live {
            let cmd = OrderCommand::CancelByTag { tag: tag.to_string() };
            if let Err(e) = b.tx_for(kind).send(cmd) {
                warn!("[OMS] live Cancel dispatch failed for {}: {}", tag, e);
            }
        }
    }

    /// Enable Strategy 1 with the given parameters.
    pub fn enable_equity_imbalance(&mut self, params: ImbalanceParams) {
        self.eq_enabled = true;
        self.eq_params = params;
    }

    /// Register an equity token for Strategy 1.
    pub fn register_equity(&mut self, meta: EquityMeta) {
        self.eq_meta.insert(meta.token, meta);
    }

    /// Enable Strategy 2 with the given parameters.
    pub fn enable_gamma(&mut self, params: GammaParams) {
        self.gamma_enabled = true;
        self.gamma_params = params;
    }

    /// Register an ATM option leg for Strategy 2.
    pub fn register_gamma(&mut self, meta: GammaMeta) {
        self.gamma_meta.insert(meta.token, meta);
    }

    /// Tell the manager whether today is a Tuesday (Nifty weekly expiry). Set by
    /// the engine from IST at startup; tests set it directly.
    pub fn set_is_tuesday(&mut self, b: bool) {
        self.is_tuesday = b;
    }

    pub fn realized_pnl(&self) -> f64 {
        self.risk.realized_pnl()
    }
    pub fn capital(&self) -> f64 {
        self.risk.capital()
    }
    pub fn open_count(&self) -> usize {
        self.open.len()
    }
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }
    pub fn closed_trades(&self) -> &[ClosedTrade] {
        &self.closed
    }

    pub fn shutdown_ready(&self) -> bool {
        !self.mode.is_live() || (self.pending.is_empty() && self.open.is_empty())
    }

    /// Hourly capital sync.
    pub fn on_capital(&mut self, funds: f64) {
        let prev = self.risk.capital();
        self.risk.update_capital(funds);
        info!(
            "[OMS] capital sync ₹{:.2} -> ₹{:.2}; per-trade budget ₹{:.2}",
            prev,
            funds,
            self.risk.risk_budget()
        );
    }

    /// Process one depth snapshot for `token`.
    pub fn on_tick(&mut self, token: u32, depth: &MarketDepth, now_ms: u64) {
        let tn = self.tick_no.entry(token).or_insert(0);
        *tn += 1;
        let tick_no = *tn;

        let feat = self.book.update(token, depth);
        let top = BookTop {
            bid: feat.best_bid,
            ask: feat.best_ask,
        };
        self.tops.insert(token, top);
        if !top.two_sided() {
            return;
        }

        // 0. Chase any resting live exit orders from PRIOR ticks (track the touch, then
        //    cross through) so a stop/target/flatten can't rest unfilled. Runs before
        //    process_exits so a freshly-triggered exit rests one tick before re-pegging.
        self.process_exit_chase(token, top, now_ms);
        // 1. Exits for existing positions.
        self.process_exits(token, top, now_ms);
        // 2. Fills for orders placed on a strictly earlier tick.
        self.process_fills(token, top, tick_no, now_ms);
        // 3. Limit chase: re-peg unfilled orders toward the market.
        self.process_repegs(token, top, tick_no, now_ms);
        // 3b. Abandon stale orders that never filled (free the concurrency slot).
        self.prune_stale(token, now_ms);
        // 4. New strategy signals -> resting orders (cannot fill until next tick).
        if !self.flattened {
            self.maybe_signal(token, feat, top, now_ms, tick_no);
        }
        // 5. If this tick left micro with no open position and no live order, free the
        //    global position slot (covers paper fills, exits, and abandoned chases).
        self.release_lock_if_idle();
    }

    /// Release the global one-position slot iff micro currently holds NO commitment
    /// (no open position and no working order). `release` is a no-op unless micro is
    /// the holder, so this can never free another engine's position. Idempotent.
    fn release_lock_if_idle(&mut self) {
        if let Some(c) = &self.shared_circuit {
            if self.open.is_empty() && self.pending.is_empty() {
                crate::portfolio::release(c, "micro");
            }
        }
    }

    fn prune_stale(&mut self, token: u32, now_ms: u64) {
        let ttl = self.cfg.order_ttl_ms;
        if self.mode.is_live() {
            let mut cancels: Vec<(String, InstrumentKind)> = Vec::new();
            for ord in self.pending.iter_mut() {
                if ord.signal.token == token
                    && !ord.cancel_requested
                    && now_ms.saturating_sub(ord.placed_ms) > ttl
                {
                    ord.cancel_requested = true;
                    cancels.push((ord.tag.clone(), ord.signal.instrument));
                }
            }
            for (tag, kind) in cancels {
                warn!(
                    "[OMS:{}] order aged out after {}ms unfilled; cancelling {} and awaiting broker terminal status",
                    self.mode.label(),
                    ttl,
                    tag
                );
                if !tag.is_empty() {
                    self.dispatch_cancel(&tag, kind);
                }
            }
            return;
        }

        let stale: Vec<String> = self
            .pending
            .iter()
            .filter(|o| o.signal.token == token && now_ms.saturating_sub(o.placed_ms) > ttl)
            .map(|o| o.tag.clone())
            .collect();
        if stale.is_empty() {
            return;
        }
        for tag in &stale {
            warn!("[OMS:{}] order aged out after {}ms unfilled; cancelling {}", self.mode.label(), ttl, tag);
        }
        let drop: HashSet<&String> = stale.iter().collect();
        self.pending.retain(|o| o.signal.token != token || !drop.contains(&o.tag));
    }

    /// True once the engine has flattened (3:15 or shutdown) — no more entries.
    pub fn is_flattened(&self) -> bool {
        self.flattened
    }

    /// Flatten if not already done; returns true if it acted. Idempotent.
    pub fn flatten_once(&mut self, now_ms: u64) -> bool {
        if self.flattened {
            return false;
        }
        self.flatten_all(now_ms);
        true
    }

    /// Ratchet a position's stop toward profit as it runs our way. The stop only ever
    /// TIGHTENS — for a long `stop_price.max(trail)`, for a short `.min(trail)` — so a
    /// favourable spike followed by a pullback can never LOOSEN the stop. Once the trade
    /// has run `TRAIL_TRIGGER_FRAC` of the way to target, the stop locks at breakeven and
    /// trails the high-water mark by `TRAIL_GIVEBACK_FRAC` of the original stop distance.
    fn apply_trailing_stop(pos: &mut OpenPosition, top: BookTop) {
        match pos.signal.side {
            PositionSide::Long => {
                pos.high_water = pos.high_water.max(top.bid); // long exits into the bid
                let stop_dist = (pos.entry_price - pos.stop_price).abs();
                let target_dist = (pos.target_price - pos.entry_price).abs();
                let progress = pos.high_water - pos.entry_price;
                if target_dist > 0.0 && progress >= TRAIL_TRIGGER_FRAC * target_dist {
                    let trail = (pos.high_water - TRAIL_GIVEBACK_FRAC * stop_dist).max(pos.entry_price);
                    if trail > pos.stop_price {
                        pos.stop_price = trail; // ratchet UP only
                        pos.breakeven_set = true;
                    }
                }
            }
            PositionSide::Short => {
                pos.high_water = pos.high_water.min(top.ask); // short exits at the ask
                let stop_dist = (pos.stop_price - pos.entry_price).abs();
                let target_dist = (pos.entry_price - pos.target_price).abs();
                let progress = pos.entry_price - pos.high_water;
                if target_dist > 0.0 && progress >= TRAIL_TRIGGER_FRAC * target_dist {
                    let trail = (pos.high_water + TRAIL_GIVEBACK_FRAC * stop_dist).min(pos.entry_price);
                    if trail < pos.stop_price {
                        pos.stop_price = trail; // ratchet DOWN only
                        pos.breakeven_set = true;
                    }
                }
            }
        }
    }

    fn process_exits(&mut self, token: u32, top: BookTop, now_ms: u64) {
        // 0. Trailing stop: ratchet the stop toward profit as the trade runs our way.
        //    Done BEFORE trigger detection so a just-tightened stop can fire this tick.
        //    The stop only ever TIGHTENS (`.max` long / `.min` short) — a favourable
        //    spike then pullback must never LOOSEN it. Skip positions already exiting.
        for pos in self.open.iter_mut() {
            if pos.signal.token != token || pos.exiting {
                continue;
            }
            Self::apply_trailing_stop(pos, top);
        }

        // Decide which positions should exit and at what touch price.
        let mut triggers: Vec<(usize, f64, ExitReason)> = Vec::new();
        for (i, pos) in self.open.iter().enumerate() {
            if pos.signal.token != token || pos.exiting {
                continue;
            }
            let trig = match pos.signal.side {
                PositionSide::Long => {
                    // We exit by selling into the bid.
                    if top.bid <= pos.stop_price {
                        Some((top.bid, ExitReason::Stop))
                    } else if top.bid >= pos.target_price {
                        Some((top.bid, ExitReason::Target))
                    } else if now_ms.saturating_sub(pos.opened_ms) >= self.cfg.max_hold_ms {
                        Some((top.bid, ExitReason::Time))
                    } else {
                        None
                    }
                }
                PositionSide::Short => {
                    if top.ask >= pos.stop_price {
                        Some((top.ask, ExitReason::Stop))
                    } else if top.ask <= pos.target_price {
                        Some((top.ask, ExitReason::Target))
                    } else if now_ms.saturating_sub(pos.opened_ms) >= self.cfg.max_hold_ms {
                        Some((top.ask, ExitReason::Time))
                    } else {
                        None
                    }
                }
            };
            if let Some((px, reason)) = trig {
                triggers.push((i, px, reason));
            }
        }

        if self.mode.is_live() {
            // Live: dispatch a real square-off order; close only on its confirmed
            // fill (on_order_update). Mark the position so we don't re-send.
            let mut dispatches: Vec<(String, Signal, u32, f64)> = Vec::new();
            for (i, px, reason) in triggers {
                if let Some(dispatch) = self.prepare_live_exit(i, px, reason, now_ms) {
                    dispatches.push(dispatch);
                }
            }
            for (tag, sig, qty, px) in dispatches {
                // Exit is the opposite side of the entry.
                let exit_sig = flip_side(&sig);
                info!("[OMS:LIVE] EXIT order {} {} x{} @ ₹{:.2} [{}]", exit_sig.symbol, side_word(exit_sig.side), qty, px, "square-off");
                self.dispatch_place(&exit_sig, &tag, qty, px);
            }
        } else {
            // Paper: close immediately at the touch.
            for (i, exit_px, reason) in triggers.into_iter().rev() {
                let pos = self.open.remove(i);
                self.close_position(pos, exit_px, reason, now_ms);
            }
        }
    }

    /// Live exit chase (bugs 1 & 3). An exit limit resting at the touch will NOT fill
    /// if the market ticks away — leaving a stop-loss/target/flatten as a dead resting
    /// order. Each eligible tick we re-price the resting exit to track the marketable
    /// touch (bid for a long's sell, ask for a short's buy); after `chase_timeout_ms`
    /// we cross THROUGH the book by `EXIT_CROSS_TICKS` and keep tracking the touch-minus-
    /// cushion so it stays marketable even through a gap. **Modify-only** — never
    /// cancel+place-market — so a late fill on the original order can never double-exit
    /// into a naked reversed position. Operates on the `exiting==true` set, the exact
    /// complement of `process_exits`'s filter, so the two never collide.
    fn process_exit_chase(&mut self, token: u32, top: BookTop, now_ms: u64) {
        if !self.mode.is_live() {
            return;
        }
        let threshold = self.cfg.repeg_threshold_ticks as f64 * self.cfg.tick_size;
        let cushion = (self.cfg.tick_size * EXIT_CROSS_TICKS).max(self.cfg.tick_size);
        let mut modifies: Vec<(String, f64, InstrumentKind)> = Vec::new();

        for pos in self.open.iter_mut() {
            if pos.signal.token != token || !pos.exiting || pos.exit_tag.is_empty() {
                continue;
            }
            // Re-peg rate cap — stay well under the 10 orders/sec exchange limit.
            if now_ms.saturating_sub(pos.exit_last_action_ms) < self.cfg.min_repeg_interval_ms {
                continue;
            }
            let touch = match pos.signal.side {
                PositionSide::Long => top.bid,  // long exit = SELL into the bid
                PositionSide::Short => top.ask, // short exit = BUY at the ask
            };
            if touch <= 0.0 {
                continue;
            }
            let elapsed = now_ms.saturating_sub(pos.exit_placed_ms);
            let crossing = elapsed >= self.cfg.chase_timeout_ms;
            // During the window: track the touch. After it: cross THROUGH the touch.
            let target = if crossing {
                match pos.signal.side {
                    PositionSide::Long => (top.bid - cushion).max(self.cfg.tick_size),
                    PositionSide::Short => top.ask + cushion,
                }
            } else {
                touch
            };
            let just_crossed = crossing && !pos.exit_crossed;
            if just_crossed {
                pos.exit_crossed = true;
                warn!(
                    "[OMS:LIVE] exit {} unfilled after {}ms — crossing through @ ₹{:.2} to guarantee square-off",
                    pos.exit_tag, elapsed, target
                );
            }
            if just_crossed || (target - pos.pending_exit_px).abs() >= threshold {
                pos.pending_exit_px = target;
                pos.exit_last_action_ms = now_ms;
                modifies.push((pos.exit_tag.clone(), target, pos.signal.instrument));
            }
        }

        for (tag, px, kind) in modifies {
            self.dispatch_modify(&tag, px, kind);
        }
    }

    fn prepare_live_exit(
        &mut self,
        idx: usize,
        px: f64,
        reason: ExitReason,
        now_ms: u64,
    ) -> Option<(String, Signal, u32, f64)> {
        let existing = self.open.get(idx)?;
        if existing.exiting {
            return None;
        }
        let kind = existing.signal.kind;
        self.order_seq += 1;
        let tag = make_order_tag(&self.tag_prefix, kind, "X", self.order_seq);
        let pos = self.open.get_mut(idx)?;
        pos.exiting = true;
        pos.exit_reason = reason;
        pos.pending_exit_px = px;
        pos.exit_tag = tag.clone();
        // Arm the exit chase (bugs 1 & 3): both the stop/target path (process_exits)
        // and the late-fill reversal path (on_order_update) route through here, so the
        // exit is chased no matter how it was created.
        pos.exit_placed_ms = now_ms;
        pos.exit_last_action_ms = now_ms;
        pos.exit_crossed = false;
        Some((tag, pos.signal.clone(), pos.qty, px))
    }

    /// Reconcile a broker order update (live mode). Confirmed entry fills open
    /// positions at the real average price; confirmed exit fills close them.
    pub fn on_order_update(&mut self, tag: &str, status: &str, avg_price: Option<f64>, now_ms: u64) {
        // Terminal failure: drop a dead entry order, or let a failed exit retry.
        if status.eq_ignore_ascii_case("REJECTED")
            || status.eq_ignore_ascii_case("CANCELLED")
            || status.eq_ignore_ascii_case("CANCELED")
            || status.eq_ignore_ascii_case("EXPIRED")
        {
            if let Some(idx) = self.pending.iter().position(|o| o.tag == tag) {
                warn!("[OMS] entry order {} {} — removing from book", tag, status);
                self.pending.remove(idx);
            } else if let Some(p) = self.open.iter_mut().find(|p| p.exit_tag == tag) {
                warn!("[OMS] exit order {} {} — will retry square-off on next trigger", tag, status);
                p.exiting = false;
                p.exit_tag.clear();
            }
            // A dropped entry order frees the global slot (no position resulted). The
            // exit-retry branch keeps the slot (position still open). Idempotent.
            self.release_lock_if_idle();
            return;
        }
        if !status.eq_ignore_ascii_case("COMPLETE") {
            return; // OPEN / TRIGGER PENDING etc. — nothing to reconcile yet
        }
        // Entry fill?
        if let Some(idx) = self.pending.iter().position(|o| o.tag == tag) {
            let ord = self.pending.remove(idx);
            let should_exit_immediately = self.flattened || ord.cancel_requested;
            let exit_reason = if self.flattened {
                ExitReason::Flatten
            } else {
                ExitReason::Time
            };
            let entry = avg_price.filter(|p| *p > 0.0);
            self.open_from_fill(ord, entry, now_ms);
            if self.mode.is_live() && should_exit_immediately {
                let pos_idx = self.open.len().saturating_sub(1);
                let token = self.open[pos_idx].signal.token;
                let top = self.tops.get(&token).copied().unwrap_or(BookTop {
                    bid: self.open[pos_idx].entry_price,
                    ask: self.open[pos_idx].entry_price,
                });
                let exit_px = match self.open[pos_idx].signal.side {
                    PositionSide::Long => top.bid.max(0.0),
                    PositionSide::Short => top.ask,
                };
                if let Some((exit_tag, sig, qty, px)) = self.prepare_live_exit(pos_idx, exit_px, exit_reason, now_ms) {
                    let exit_sig = flip_side(&sig);
                    warn!(
                        "[OMS:LIVE] late entry fill {} after cancel/flatten — dispatching EXIT {} x{} @ ₹{:.2}",
                        tag, exit_tag, qty, px
                    );
                    self.dispatch_place(&exit_sig, &exit_tag, qty, px);
                }
            }
            return;
        }
        // Exit fill?
        if let Some(idx) = self.open.iter().position(|p| p.exit_tag == tag) {
            let pos = self.open.remove(idx);
            let exit_px = avg_price.filter(|p| *p > 0.0).unwrap_or(pos.pending_exit_px);
            let reason = pos.exit_reason;
            self.close_position(pos, exit_px, reason, now_ms);
        }
    }

    fn process_fills(&mut self, token: u32, top: BookTop, tick_no: u64, now_ms: u64) {
        // In live mode, positions open only on a confirmed broker fill
        // (see `on_order_update`) — never on a simulated book touch.
        if self.mode.is_live() {
            return;
        }
        let mut filled: Vec<usize> = Vec::new();
        for (i, ord) in self.pending.iter().enumerate() {
            if ord.signal.token != token || ord.born_tick >= tick_no {
                continue; // not this token, or born this/later tick (no same-tick fill)
            }
            let fills = match ord.signal.side {
                PositionSide::Long => buy_limit_fills(ord.limit_price, top.ask),
                PositionSide::Short => sell_limit_fills(ord.limit_price, top.bid),
            };
            if fills {
                filled.push(i);
            }
        }
        for i in filled.into_iter().rev() {
            let ord = self.pending.remove(i);
            self.open_from_fill(ord, None, now_ms);
        }
    }

    /// Open a position from a filled order. `entry_override` carries the broker's
    /// real average fill price in live mode; in paper we fill at our limit price.
    fn open_from_fill(&mut self, ord: PendingOrder, entry_override: Option<f64>, now_ms: u64) {
        let entry = entry_override.unwrap_or(ord.limit_price);
        let sig = ord.signal;
        let stop_dist = entry * sig.stop_frac;
        let (stop_price, target_price) = match sig.side {
            PositionSide::Long => ((entry - stop_dist).max(0.0), entry + stop_dist * sig.target_r),
            PositionSide::Short => (entry + stop_dist, (entry - stop_dist * sig.target_r).max(0.0)),
        };
        info!(
            "[OMS:{}] OPEN {} {} x{} @ ₹{:.2} (stop ₹{:.2}, tgt ₹{:.2}) | {}",
            self.mode.label(),
            sig.kind.label(),
            sig.symbol,
            ord.qty,
            entry,
            stop_price,
            target_price,
            sig.rationale
        );
        self.last_open_ms = now_ms;
        self.open.push(OpenPosition {
            signal: sig,
            entry_price: entry,
            qty: ord.qty,
            stop_price,
            target_price,
            opened_ms: now_ms,
            exiting: false,
            exit_tag: String::new(),
            exit_reason: ExitReason::Time,
            pending_exit_px: 0.0,
            exit_placed_ms: 0,
            exit_last_action_ms: 0,
            exit_crossed: false,
            high_water: entry,
            breakeven_set: false,
        });
    }

    fn close_position(&mut self, pos: OpenPosition, exit_px: f64, reason: ExitReason, now_ms: u64) {
        let qty = pos.qty;
        let gross = match pos.signal.side {
            PositionSide::Long => (exit_px - pos.entry_price) * qty as f64,
            PositionSide::Short => (pos.entry_price - exit_px) * qty as f64,
        };
        let cost = match pos.signal.instrument {
            InstrumentKind::Equity => {
                costs::equity_intraday_roundtrip(pos.entry_price, exit_px, qty)
            }
            InstrumentKind::Option => {
                let lots = (qty / pos.signal.lot_size.max(1)).max(1);
                costs::options_roundtrip_at(
                    pos.entry_price,
                    exit_px,
                    pos.signal.lot_size,
                    lots,
                    now_ms,
                )
            }
        };
        let net = gross - cost;
        self.risk.record_realized(net);
        // Feed the account-level circuit (real, net-of-cost P&L only) and FREE the
        // single global position slot — this trade is done. Single close chokepoint
        // for paper AND live (live exit fills route here too).
        if let Some(c) = &self.shared_circuit {
            crate::portfolio::record_for(c, "micro", net);
            crate::portfolio::release(c, "micro");
        }
        info!(
            "[OMS:{}] CLOSE {} {} x{} @ ₹{:.2} [{}] gross ₹{:.2} − cost ₹{:.2} = net ₹{:.2} | realized ₹{:.2}",
            self.mode.label(),
            pos.signal.kind.label(),
            pos.signal.symbol,
            qty,
            exit_px,
            reason.label(),
            gross,
            cost,
            net,
            self.risk.realized_pnl()
        );
        match self.risk.circuit_state() {
            crate::risk::CircuitState::LowerLoss => warn!(
                "[OMS] ⛔ LOWER CIRCUIT (loss) tripped: realized ₹{:.2} <= ₹{:.2}. No more trades today.",
                self.risk.realized_pnl(), self.risk.lower_circuit_threshold()
            ),
            crate::risk::CircuitState::UpperProfit => warn!(
                "[OMS] ⛔ UPPER CIRCUIT (profit) tripped: realized ₹{:.2} >= ₹{:.2}. No more trades today (anti-overtrade).",
                self.risk.realized_pnl(), self.risk.upper_circuit_threshold()
            ),
            crate::risk::CircuitState::Open => {}
        }
        self.closed.push(ClosedTrade {
            symbol: pos.signal.symbol.clone(),
            kind: pos.signal.kind,
            side: pos.signal.side,
            entry_price: pos.entry_price,
            exit_price: exit_px,
            qty,
            gross_pnl: gross,
            cost,
            net_pnl: net,
            reason,
            opened_ms: pos.opened_ms,
            closed_ms: now_ms,
            rationale: pos.signal.rationale.clone(),
        });
    }

    fn maybe_signal(
        &mut self,
        token: u32,
        feat: crate::microbook::MicroFeatures,
        top: BookTop,
        now_ms: u64,
        tick_no: u64,
    ) {
        if !self.risk.can_enter() {
            return; // local daily circuit
        }
        // Account-level circuit + global position lock: halt if the whole book is
        // past -15% / +25%, OR if any engine already holds the single open-position
        // slot (one trade at a time across the account — cancel this signal). The
        // authoritative grab is the atomic try_claim in try_open.
        if let Some(c) = &self.shared_circuit {
            if !crate::portfolio::can_enter_holder(c, "micro") || crate::portfolio::is_locked(c) {
                return;
            }
        }
        if (self.open.len() + self.pending.len()) as u32 >= self.cfg.max_concurrent_positions {
            return;
        }
        if now_ms.saturating_sub(self.last_open_ms) < self.cfg.cooldown_ms && self.last_open_ms > 0 {
            return;
        }
        // Don't stack multiple orders/positions on the same token.
        if self.pending.iter().any(|o| o.signal.token == token)
            || self.open.iter().any(|p| p.signal.token == token)
        {
            return;
        }

        let mut signal: Option<Signal> = None;
        if self.eq_enabled {
            if let Some(meta) = self.eq_meta.get(&token) {
                signal = imbalance::evaluate(&feat, &self.eq_params, meta, now_ms);
            }
        }
        if signal.is_none() && self.gamma_enabled {
            if let Some(meta) = self.gamma_meta.get(&token) {
                signal = gamma::evaluate(&feat, &self.gamma_params, meta, self.is_tuesday, now_ms);
            }
        }

        if let Some(sig) = signal {
            self.try_open(sig, top, now_ms, tick_no);
        }
    }

    fn try_open(&mut self, sig: Signal, top: BookTop, now_ms: u64, tick_no: u64) {
        let qty = self.size(&sig, top);
        if qty == 0 {
            info!(
                "[OMS] {} signal on {} skipped: risk budget ₹{:.2} sizes 0 (costs/stop too large for capital).",
                sig.kind.label(),
                sig.symbol,
                self.risk.risk_budget()
            );
            return;
        }
        // Claim the single global position slot (atomic) before committing the order.
        // If another engine (options) holds it, cancel — one open trade at a time.
        if let Some(c) = &self.shared_circuit {
            if !crate::portfolio::try_claim(c, "micro") {
                return;
            }
        }
        // Start passive: join the near touch (best bid for a buy). The chase
        // walks it toward the market and finally crosses if it won't fill.
        let limit_price = match sig.side {
            PositionSide::Long => top.bid,
            PositionSide::Short => top.ask,
        };
        self.order_seq += 1;
        let tag = make_order_tag(&self.tag_prefix, sig.kind, "E", self.order_seq);
        info!(
            "[OMS:{}] SIGNAL {} {} -> resting {} limit x{} @ ₹{:.2} (passive join)",
            self.mode.label(),
            sig.kind.label(),
            sig.symbol,
            side_word(sig.side),
            qty,
            limit_price
        );
        if self.mode.is_live() {
            self.dispatch_place(&sig, &tag, qty, limit_price);
        }
        self.pending.push(PendingOrder {
            signal: sig,
            limit_price,
            qty,
            born_tick: tick_no,
            repegs: 0,
            placed_ms: now_ms,
            last_repeg_ms: now_ms,
            crossed: false,
            cancel_requested: false,
            tag,
        });
    }

    /// Limit chase: walk each unfilled order toward the market, then cross to take
    /// liquidity once the chase budget (re-pegs or time) is spent. Re-pegging resets
    /// `born_tick`, so a re-pegged price can only fill on a *later* tick — the
    /// simulator never grants a fill from the tick that triggered the move.
    fn process_repegs(&mut self, token: u32, top: BookTop, tick_no: u64, now_ms: u64) {
        let threshold = self.cfg.repeg_threshold_ticks as f64 * self.cfg.tick_size;
        let mut modifies: Vec<(String, f64, InstrumentKind)> = Vec::new();

        for ord in self.pending.iter_mut() {
            if ord.signal.token != token || ord.born_tick >= tick_no || ord.crossed || ord.cancel_requested {
                continue;
            }
            if now_ms.saturating_sub(ord.last_repeg_ms) < self.cfg.min_repeg_interval_ms {
                continue;
            }
            let elapsed = now_ms.saturating_sub(ord.placed_ms);
            let budget_left = ord.repegs < self.cfg.max_repegs && elapsed < self.cfg.chase_timeout_ms;

            // The market price we want to track (passive near touch).
            let market = match ord.signal.side {
                PositionSide::Long => top.bid,
                PositionSide::Short => top.ask,
            };

            if budget_left {
                if (market - ord.limit_price).abs() >= threshold {
                    // Bug 4: only ADVERSE moves (chasing the market away from us) consume
                    // the chase budget. A favorable move — bid falling for a buy, ask
                    // rising for a sell — still re-pegs to capture the better price and
                    // still respects the rate cap, but must NOT burn budget or it would
                    // punish beneficial price action by eventually forcing a cross.
                    let adverse = match ord.signal.side {
                        PositionSide::Long => market > ord.limit_price,  // price rising — paying up
                        PositionSide::Short => market < ord.limit_price, // price falling — selling lower
                    };
                    ord.limit_price = market; // track the touch (chase up OR harvest)
                    if adverse {
                        ord.repegs += 1;
                    }
                    ord.born_tick = tick_no;
                    ord.last_repeg_ms = now_ms;
                    if !ord.tag.is_empty() {
                        modifies.push((ord.tag.clone(), market, ord.signal.instrument));
                    }
                }
            } else {
                // Budget spent: cross the spread to secure the fill.
                let cross = match ord.signal.side {
                    PositionSide::Long => top.ask,
                    PositionSide::Short => top.bid,
                };
                if (cross - ord.limit_price).abs() > f64::EPSILON {
                    ord.limit_price = cross;
                    ord.born_tick = tick_no;
                    ord.last_repeg_ms = now_ms;
                    if !ord.tag.is_empty() {
                        modifies.push((ord.tag.clone(), cross, ord.signal.instrument));
                    }
                }
                ord.crossed = true;
            }
        }

        if self.mode.is_live() {
            for (tag, px, kind) in modifies {
                self.dispatch_modify(&tag, px, kind);
            }
        }
    }

    /// Size a position so worst-case loss to the stop, plus estimated round-trip
    /// costs, stays within the per-trade risk budget; also cap by MIS margin
    /// (equity) or affordability (options).
    fn size(&self, sig: &Signal, _top: BookTop) -> u32 {
        let budget = self.risk.risk_budget();
        let stop_dist = sig.ref_price * sig.stop_frac;
        if stop_dist <= 0.0 || budget <= 0.0 {
            return 0;
        }
        match sig.instrument {
            InstrumentKind::Equity => {
                let qty0 = (budget / stop_dist).floor().max(1.0) as u32;
                let est = costs::equity_est_roundtrip(sig.ref_price, qty0);
                let qty = (((budget - est).max(0.0)) / stop_dist).floor();
                let margin_cap = (self.risk.capital() * self.cfg.mis_leverage / sig.ref_price).floor();
                qty.min(margin_cap).min(sig.max_lots as f64).max(0.0) as u32
            }
            InstrumentKind::Option => {
                let per_lot_risk = stop_dist * sig.lot_size as f64;
                if per_lot_risk <= 0.0 {
                    return 0;
                }
                let lots0 = (budget / per_lot_risk).floor().max(1.0) as u32;
                let est = costs::options_est_roundtrip(sig.ref_price, sig.lot_size, lots0);
                let lots = (((budget - est).max(0.0)) / per_lot_risk).floor();
                let afford = (self.risk.capital() / (sig.ref_price * sig.lot_size as f64)).floor();
                let lots = lots.min(afford).min(sig.max_lots as f64).max(0.0) as u32;
                lots * sig.lot_size
            }
        }
    }

    /// Cancel all pending orders and square off open positions at the current
    /// touch. Called at the 3:15 flatten or on shutdown.
    pub fn flatten_all(&mut self, now_ms: u64) {
        self.flattened = true;
        let n_pending = self.pending.len();
        if self.mode.is_live() {
            let mut cancels: Vec<(String, InstrumentKind)> = Vec::new();
            for ord in self.pending.iter_mut() {
                if !ord.tag.is_empty() && !ord.cancel_requested {
                    ord.cancel_requested = true;
                    cancels.push((ord.tag.clone(), ord.signal.instrument));
                }
            }
            for (tag, kind) in cancels {
                self.dispatch_cancel(&tag, kind);
            }

            // Square off EVERY open position — including ones already mid-exit (bug 2:
            // the old code SKIPPED `exiting` positions, so a stop whose limit missed was
            // carried overnight). Non-exiting -> a true MARKET order. Already-exiting ->
            // re-price the resting exit THROUGH the book (modify-only, no double-fill).
            let cushion = (self.cfg.tick_size * EXIT_CROSS_TICKS).max(self.cfg.tick_size);
            let mut market_exits: Vec<(Signal, String, u32)> = Vec::new();
            let mut cross_modifies: Vec<(String, f64, InstrumentKind)> = Vec::new();
            for idx in 0..self.open.len() {
                let token = self.open[idx].signal.token;
                let top = self.tops.get(&token).copied();
                let side = self.open[idx].signal.side;
                if self.open[idx].exiting {
                    // Force the already-resting exit marketable now (don't wait for the
                    // chase timeout at EOD). Needs a book to price the cross.
                    if let Some(top) = top {
                        let tag = self.open[idx].exit_tag.clone();
                        if !tag.is_empty() {
                            let cross_px = match side {
                                PositionSide::Long => (top.bid - cushion).max(self.cfg.tick_size),
                                PositionSide::Short => top.ask + cushion,
                            };
                            self.open[idx].pending_exit_px = cross_px;
                            self.open[idx].exit_crossed = true;
                            self.open[idx].exit_last_action_ms = now_ms;
                            cross_modifies.push((tag, cross_px, self.open[idx].signal.instrument));
                        }
                    }
                    continue;
                }
                // Non-exiting: fresh MARKET square-off. Set the exit bookkeeping so the
                // confirmed fill closes the position via on_order_update.
                self.order_seq += 1;
                let kind = self.open[idx].signal.kind;
                let tag = make_order_tag(&self.tag_prefix, kind, "X", self.order_seq);
                let ref_px = top
                    .map(|t| match side {
                        PositionSide::Long => t.bid.max(0.0),
                        PositionSide::Short => t.ask,
                    })
                    .unwrap_or(self.open[idx].entry_price);
                self.open[idx].exiting = true;
                self.open[idx].exit_reason = ExitReason::Flatten;
                self.open[idx].exit_tag = tag.clone();
                self.open[idx].pending_exit_px = ref_px; // fallback if broker avg missing
                self.open[idx].exit_placed_ms = now_ms;
                self.open[idx].exit_last_action_ms = now_ms;
                self.open[idx].exit_crossed = true; // a market order is already "marketable"
                market_exits.push((self.open[idx].signal.clone(), tag, self.open[idx].qty));
            }
            let n_requested = market_exits.len() + cross_modifies.len();
            for (sig, tag, qty) in &market_exits {
                let exit_sig = flip_side(sig);
                warn!(
                    "[OMS:LIVE] FLATTEN market square-off {} {} x{} tag={}",
                    exit_sig.symbol, side_word(exit_sig.side), qty, tag
                );
                self.dispatch_place_market(&exit_sig, tag, *qty);
            }
            for (tag, px, kind) in cross_modifies {
                warn!("[OMS:LIVE] FLATTEN cross resting exit tag={} @ ₹{:.2}", tag, px);
                self.dispatch_modify(&tag, px, kind);
            }
            info!(
                "[OMS:{}] FLATTEN requested: cancelled {} pending, requested {} square-offs. Waiting for broker fills; realized ₹{:.2}",
                self.mode.label(),
                n_pending,
                n_requested,
                self.risk.realized_pnl()
            );
            return;
        }

        self.pending.clear();
        let open: Vec<OpenPosition> = std::mem::take(&mut self.open);
        let n_open = open.len();
        for pos in open {
            let token = pos.signal.token;
            let top = self.tops.get(&token).copied().unwrap_or(BookTop {
                bid: pos.entry_price,
                ask: pos.entry_price,
            });
            let exit_px = match pos.signal.side {
                PositionSide::Long => top.bid.max(0.0),
                PositionSide::Short => top.ask,
            };
            self.close_position(pos, exit_px, ExitReason::Flatten, now_ms);
        }
        info!(
            "[OMS:{}] FLATTEN complete: cancelled {} pending, squared {} open. Realized ₹{:.2}",
            self.mode.label(),
            n_pending,
            n_open,
            self.risk.realized_pnl()
        );
    }
}

fn side_word(s: PositionSide) -> &'static str {
    match s {
        PositionSide::Long => "BUY",
        PositionSide::Short => "SELL",
    }
}

fn sanitize_tag_prefix(prefix: &str) -> String {
    let clean: String = prefix
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .map(|c| c.to_ascii_uppercase())
        .collect();
    if clean.is_empty() {
        "SATA".to_string()
    } else {
        clean
    }
}

fn compact_kind(kind: StrategyKind) -> &'static str {
    match kind {
        StrategyKind::EquityImbalance => "EQI",
        StrategyKind::GammaSqueeze => "GAM",
    }
}

fn make_order_tag(prefix: &str, kind: StrategyKind, stage: &str, seq: u64) -> String {
    let prefix = sanitize_tag_prefix(prefix);
    let stage: String = stage
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .map(|c| c.to_ascii_uppercase())
        .collect();
    let suffix = format!("{}{}{}", compact_kind(kind), stage, seq);
    if suffix.len() >= 20 {
        return suffix.chars().take(20).collect();
    }
    let max_prefix = 20 - suffix.len();
    let trimmed_prefix: String = prefix.chars().take(max_prefix).collect();
    format!("{}{}", trimmed_prefix, suffix)
}

/// A copy of the signal with the side reversed — used to build square-off orders.
fn flip_side(sig: &Signal) -> Signal {
    let mut s = sig.clone();
    s.side = match sig.side {
        PositionSide::Long => PositionSide::Short,
        PositionSide::Short => PositionSide::Long,
    };
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::DepthEntry;

    fn depth(bid_px: f64, bid_qty: u32, ask_px: f64, ask_qty: u32) -> MarketDepth {
        let mut d = MarketDepth::default();
        d.bids[0] = DepthEntry { price: bid_px, quantity: bid_qty, orders: 1 };
        d.asks[0] = DepthEntry { price: ask_px, quantity: ask_qty, orders: 1 };
        d
    }

    /// A static sell wall: thin bid @ `mid-0.05`, heavy ask @ `mid+0.05`
    /// (OBI = (10-90)/100 = -0.8). Stable across ticks so it produces no flow.
    fn wall(mid: f64) -> MarketDepth {
        depth(mid - 0.05, 10, mid + 0.05, 90)
    }

    /// Wall still present, but the bid lifts to `mid` (absorption) — OBI stays
    /// strongly negative while OFI turns positive (buyers stepping up).
    fn absorb(mid: f64) -> MarketDepth {
        depth(mid, 12, mid + 0.05, 90)
    }

    /// Base config: cross immediately (no passive wait) for deterministic fills.
    /// The dedicated chase test overrides these.
    fn base_cfg() -> ManagerConfig {
        ManagerConfig {
            max_concurrent_positions: 1,
            cooldown_ms: 0,
            mis_leverage: 5.0,
            max_hold_ms: 60_000,
            max_repegs: 0,
            repeg_threshold_ticks: 1,
            chase_timeout_ms: 0, // budget spent immediately => cross on first post-place tick
            min_repeg_interval_ms: 0,
            tick_size: 0.05,
            order_ttl_ms: 600_000,
        }
    }

    fn manager_with(cfg: ManagerConfig) -> PositionManager {
        let risk = RiskManager::new(100_000.0, 1.0, 15.0, 35.0); // ₹1000 budget => sizes > 0
        let mut m = PositionManager::new(RunMode::Paper, risk, 5, cfg);
        let mut p = ImbalanceParams::default();
        p.min_samples = 3;
        m.enable_equity_imbalance(p);
        m.register_equity(EquityMeta {
            token: 1,
            symbol: "INFY".into(),
            exchange: "NSE".into(),
            product: "MIS".into(),
        });
        m
    }

    fn manager() -> PositionManager {
        manager_with(base_cfg())
    }

    /// Walls then absorption: places (but cannot fill) a long order at t=3000.
    fn run_to_signal(m: &mut PositionManager) {
        m.on_tick(1, &wall(100.0), 0);
        m.on_tick(1, &wall(100.0), 1000);
        m.on_tick(1, &wall(100.0), 2000);
        m.on_tick(1, &absorb(100.0), 3000);
    }

    /// Continue feeding `book` until a position opens (the chase crosses + fills).
    fn drive_until_open(m: &mut PositionManager, book: &MarketDepth, start_ms: u64) -> u64 {
        let mut t = start_ms;
        for _ in 0..10 {
            m.on_tick(1, book, t);
            if m.open_count() > 0 {
                return t;
            }
            t += 1000;
        }
        panic!("position never opened");
    }

    fn test_long_position(entry: f64, stop: f64, target: f64) -> OpenPosition {
        OpenPosition {
            signal: Signal {
                token: 1,
                symbol: "X".into(),
                exchange: "NSE".into(),
                product: "MIS".into(),
                kind: StrategyKind::EquityImbalance,
                instrument: InstrumentKind::Equity,
                side: PositionSide::Long,
                lot_size: 1,
                max_lots: 1,
                ref_price: entry,
                stop_frac: 0.02,
                target_r: 2.0,
                rationale: String::new(),
                ts_ms: 0,
            },
            entry_price: entry,
            qty: 10,
            stop_price: stop,
            target_price: target,
            opened_ms: 0,
            exiting: false,
            exit_tag: String::new(),
            exit_reason: ExitReason::Time,
            pending_exit_px: 0.0,
            exit_placed_ms: 0,
            exit_last_action_ms: 0,
            exit_crossed: false,
            high_water: entry,
            breakeven_set: false,
        }
    }

    // Trailing stop must ratchet toward profit and NEVER loosen on a pullback.
    #[test]
    fn trailing_stop_ratchets_up_and_never_loosens() {
        // entry 100, stop 98 (dist 2), target 104 (dist 4) → trail triggers at high-water 102.6.
        let mut p = test_long_position(100.0, 98.0, 104.0);
        // Below the 65% trigger: stop unchanged.
        PositionManager::apply_trailing_stop(&mut p, BookTop { bid: 101.0, ask: 101.05 });
        assert!((p.stop_price - 98.0).abs() < 1e-9, "no trail before 65% of target");
        assert!(!p.breakeven_set);
        // Past the trigger (high-water 103): stop ratchets to 103 − 0.5×2 = 102 (locks profit).
        PositionManager::apply_trailing_stop(&mut p, BookTop { bid: 103.0, ask: 103.05 });
        assert!((p.stop_price - 102.0).abs() < 1e-9, "stop ratchets up to 102");
        assert!(p.breakeven_set && p.stop_price > p.entry_price, "stop now locks a profit");
        // Pullback: high-water holds at 103, stop must NOT loosen below 102.
        PositionManager::apply_trailing_stop(&mut p, BookTop { bid: 101.5, ask: 101.55 });
        assert!((p.stop_price - 102.0).abs() < 1e-9, "a pullback must never loosen the stop");
        // Further favorable move: stop ratchets up again.
        PositionManager::apply_trailing_stop(&mut p, BookTop { bid: 103.8, ask: 103.85 });
        assert!((p.stop_price - 102.8).abs() < 1e-9, "stop ratchets up to 103.8 − 1.0");
    }

    // THE headline: with max 1 trade/day, once trade 1 closes, no 2nd trade — even
    // though the position lock is free.
    #[test]
    fn daily_cap_blocks_second_micro_trade_with_lock_free() {
        let mut m = manager();
        let circuit = crate::portfolio::new_shared(100_000.0, 15.0, 25.0, 1); // max 1 trade/day
        m.set_shared_circuit(circuit.clone());

        // Trade 1: open, then close at target.
        run_to_signal(&mut m);
        let t = drive_until_open(&mut m, &absorb(100.0), 4000);
        assert_eq!(m.open_count(), 1);
        m.on_tick(1, &depth(101.0, 80, 101.05, 50), t + 1000);
        assert_eq!(m.open_count(), 0, "trade 1 closed");
        assert_eq!(m.closed_trades().len(), 1);

        // The close recorded into the shared circuit → daily cap (1) reached; lock FREE.
        assert!(!crate::portfolio::is_locked(&circuit), "lock is free after the trade closed");
        assert_eq!(
            crate::portfolio::halt_reason_for(&circuit, "micro"),
            Some("daily account trade cap reached"),
            "halt reason must be the daily cap, not a P&L circuit"
        );

        // Trade 2 must NOT open despite a fresh signal AND a free lock.
        m.on_tick(1, &wall(100.0), 10_000);
        m.on_tick(1, &wall(100.0), 11_000);
        m.on_tick(1, &wall(100.0), 12_000);
        m.on_tick(1, &absorb(100.0), 13_000);
        assert_eq!(m.pending_count(), 0, "no 2nd trade today even with the lock free");
        assert_eq!(m.open_count(), 0);
    }

    #[test]
    fn no_same_tick_fill() {
        let mut m = manager();
        run_to_signal(&mut m);
        // The order is born on the signal tick and must NOT fill on it.
        assert_eq!(m.open_count(), 0, "must not fill on the signal tick");
        assert!(m.pending_count() >= 1, "an order should be resting");
        // It does fill on a later tick once the chase crosses.
        drive_until_open(&mut m, &absorb(100.0), 4000);
        assert_eq!(m.open_count(), 1);
        assert_eq!(m.pending_count(), 0);
    }

    #[test]
    fn target_exit_books_post_cost_profit() {
        let mut m = manager();
        run_to_signal(&mut m);
        let t = drive_until_open(&mut m, &absorb(100.0), 4000);
        assert_eq!(m.open_count(), 1);
        // Price rallies past target; bid rises so the long exits into the bid.
        m.on_tick(1, &depth(101.0, 80, 101.05, 50), t + 1000);
        assert_eq!(m.open_count(), 0, "should have hit target");
        let ct = m.closed_trades().last().expect("a closed trade");
        assert_eq!(ct.reason, ExitReason::Target);
        assert!(ct.gross_pnl > 0.0);
        assert!(ct.cost > 0.0, "transaction costs must be charged");
        assert!((ct.net_pnl - (ct.gross_pnl - ct.cost)).abs() < 1e-6);
        assert!(m.realized_pnl() > 0.0);
    }

    #[test]
    fn lower_circuit_blocks_new_entries() {
        let mut m = manager();
        m.risk.record_realized(-20_000.0); // > 15% of 100k => lower circuit
        run_to_signal(&mut m);
        assert_eq!(m.pending_count(), 0, "no orders once the lower circuit trips");
        assert_eq!(m.open_count(), 0);
    }

    #[test]
    fn upper_circuit_blocks_new_entries() {
        let mut m = manager();
        m.risk.record_realized(40_000.0); // > 35% of 100k => upper circuit
        run_to_signal(&mut m);
        assert_eq!(m.pending_count(), 0, "no overtrading once the upper circuit trips");
        assert_eq!(m.open_count(), 0);
    }

    #[test]
    fn global_position_lock_blocks_micro_then_frees() {
        let mut m = manager();
        let circuit = crate::portfolio::new_shared(100_000.0, 15.0, 25.0, u32::MAX);
        m.set_shared_circuit(circuit.clone());

        // Another engine (options) holds the single global open-position slot.
        assert!(crate::portfolio::try_claim(&circuit, "options"));

        // Micro produces a valid signal but must NOT rest an order while a trade is
        // open elsewhere — one trade at a time across the whole account.
        run_to_signal(&mut m);
        assert_eq!(m.pending_count(), 0, "micro entry cancelled while options holds the slot");
        assert_eq!(m.open_count(), 0);

        // The other engine's trade closes → the global slot frees.
        crate::portfolio::release(&circuit, "options");

        // Micro's next signal now rests an order AND grabs the slot itself.
        m.on_tick(1, &wall(100.0), 10_000);
        m.on_tick(1, &wall(100.0), 11_000);
        m.on_tick(1, &wall(100.0), 12_000);
        m.on_tick(1, &absorb(100.0), 13_000);
        assert!(m.pending_count() >= 1, "micro opens once the slot is free");
        assert!(crate::portfolio::is_locked(&circuit), "micro now holds the global slot");
    }

    #[test]
    fn flatten_squares_off_open_positions() {
        let mut m = manager();
        run_to_signal(&mut m);
        let t = drive_until_open(&mut m, &absorb(100.0), 4000);
        assert_eq!(m.open_count(), 1);
        m.flatten_all(t + 1000);
        assert_eq!(m.open_count(), 0);
        assert_eq!(m.pending_count(), 0);
        assert_eq!(m.closed_trades().len(), 1);
        assert_eq!(m.closed_trades()[0].reason, ExitReason::Flatten);
    }

    #[test]
    fn flatten_once_is_idempotent() {
        let mut m = manager();
        run_to_signal(&mut m);
        let t = drive_until_open(&mut m, &absorb(100.0), 4000);
        assert_eq!(m.open_count(), 1);
        assert!(m.flatten_once(t + 1000), "first flatten acts");
        assert!(!m.flatten_once(t + 2000), "second flatten is a no-op");
        assert_eq!(m.open_count(), 0);
    }

    #[test]
    fn stale_unfilled_order_is_pruned() {
        // Never cross (huge timeout/repegs); short TTL so the resting order ages out.
        let cfg = ManagerConfig {
            max_repegs: 1_000,
            chase_timeout_ms: u64::MAX / 2,
            order_ttl_ms: 5_000,
            ..base_cfg()
        };
        let mut m = manager_with(cfg);
        run_to_signal(&mut m); // passive order @ bid 100.00, placed at t=3000
        assert!(m.pending_count() >= 1);
        // A later tick past the TTL with a no-signal book: the order is abandoned.
        m.on_tick(1, &depth(100.0, 50, 100.10, 50), 9_000);
        assert_eq!(m.pending_count(), 0, "stale order should be pruned");
        assert_eq!(m.open_count(), 0);
    }

    #[test]
    fn live_rejected_entry_is_removed() {
        use tokio::sync::mpsc;
        let (eq_tx, mut eq_rx) = mpsc::unbounded_channel();
        let (opt_tx, _o) = mpsc::unbounded_channel();
        let risk = RiskManager::new(100_000.0, 1.0, 15.0, 35.0);
        let mut m = PositionManager::new(RunMode::Live, risk, 5, base_cfg());
        let mut p = ImbalanceParams::default();
        p.min_samples = 3;
        m.enable_equity_imbalance(p);
        m.register_equity(EquityMeta { token: 1, symbol: "INFY".into(), exchange: "NSE".into(), product: "MIS".into() });
        m.arm_live(LiveBridge { equity_tx: eq_tx, options_tx: opt_tx }, "TST".into());
        run_to_signal(&mut m);
        assert!(m.pending_count() >= 1);
        let mut tag = None;
        while let Ok(cmd) = eq_rx.try_recv() {
            if let OrderCommand::Place(pl) = cmd {
                tag = Some(pl.tag);
            }
        }
        let tag = tag.expect("a Place command should have been sent");
        m.on_order_update(&tag, "REJECTED", None, 5_000);
        assert_eq!(m.pending_count(), 0, "rejected entry must be removed, not leaked");
    }

    #[test]
    fn gamma_option_signal_routes_and_sizes() {
        use crate::strategy::gamma::{GammaMeta, GammaParams};
        // Larger capital so a NIFTY lot's stop risk fits the per-trade budget.
        let risk = RiskManager::new(500_000.0, 1.0, 15.0, 35.0); // budget ₹5000
        let mut m = PositionManager::new(RunMode::Paper, risk, 5, base_cfg());
        m.enable_gamma(GammaParams { min_samples: 8, ..GammaParams::default() });
        m.set_is_tuesday(true);
        m.register_gamma(GammaMeta { token: 2, symbol: "NIFTY2561724500CE".into(), lot_size: 65 });

        // Build a slow ask-depletion baseline (≈2/tick) on the option leg...
        let mut ask = 100u32;
        let mut t = 0u64;
        for _ in 0..9 {
            m.on_tick(2, &depth(99.5, 50, 100.5, ask), t);
            ask -= 2;
            t += 1000;
        }
        // ...then a sudden queue collapse => depletion ratio >> 5x => fires.
        m.on_tick(2, &depth(99.5, 50, 100.5, 30), t); // depletion ~52 vs MA ~2
        assert!(m.pending_count() >= 1 || m.open_count() >= 1, "gamma should place an order");
        drive_option(&mut m, &depth(99.5, 50, 100.5, 30), t + 1000);
        let opened = m.open_count() >= 1 || !m.closed_trades().is_empty();
        assert!(opened, "gamma option order should fill");
    }

    fn drive_option(m: &mut PositionManager, book: &MarketDepth, start_ms: u64) {
        let mut t = start_ms;
        for _ in 0..6 {
            m.on_tick(2, book, t);
            if m.open_count() > 0 {
                return;
            }
            t += 1000;
        }
    }

    #[test]
    fn live_opens_only_on_confirmed_fill_and_exits_via_order_update() {
        use tokio::sync::mpsc;
        let (eq_tx, mut eq_rx) = mpsc::unbounded_channel();
        let (opt_tx, _opt_rx) = mpsc::unbounded_channel();
        let risk = RiskManager::new(100_000.0, 1.0, 15.0, 35.0);
        let mut m = PositionManager::new(RunMode::Live, risk, 5, base_cfg());
        let mut p = ImbalanceParams::default();
        p.min_samples = 3;
        m.enable_equity_imbalance(p);
        m.register_equity(EquityMeta { token: 1, symbol: "INFY".into(), exchange: "NSE".into(), product: "MIS".into() });
        m.arm_live(LiveBridge { equity_tx: eq_tx, options_tx: opt_tx }, "TST".into());

        // Drive to a signal: a real Place command is emitted, but NO position opens
        // until the broker confirms the fill.
        run_to_signal(&mut m);
        m.on_tick(1, &absorb(100.0), 4000);
        m.on_tick(1, &absorb(100.0), 5000);
        assert_eq!(m.open_count(), 0, "live must not open on a simulated touch");

        // Capture the entry order tag from the dispatched Place command.
        let mut entry_tag = None;
        while let Ok(cmd) = eq_rx.try_recv() {
            if let OrderCommand::Place(p) = cmd {
                entry_tag = Some(p.tag);
            }
        }
        let entry_tag = entry_tag.expect("a live Place command should have been sent");

        // Broker confirms the entry fill -> position opens at the real avg price.
        m.on_order_update(&entry_tag, "COMPLETE", Some(100.05), 6000);
        assert_eq!(m.open_count(), 1);

        // Price hits target: a real EXIT order is dispatched, position not yet closed.
        m.on_tick(1, &depth(101.0, 80, 101.05, 50), 7000);
        assert_eq!(m.open_count(), 1, "live closes only on confirmed exit fill");
        let mut exit_tag = None;
        while let Ok(cmd) = eq_rx.try_recv() {
            if let OrderCommand::Place(p) = cmd {
                exit_tag = Some(p.tag);
            }
        }
        let exit_tag = exit_tag.expect("a live EXIT Place command should have been sent");
        assert!(exit_tag.chars().all(|c| c.is_ascii_alphanumeric()));
        assert!(exit_tag.len() <= 20);
        assert_ne!(exit_tag, entry_tag);

        // Broker confirms the exit fill -> position closes, P&L booked.
        m.on_order_update(&exit_tag, "COMPLETE", Some(101.0), 8000);
        assert_eq!(m.open_count(), 0);
        assert_eq!(m.closed_trades().len(), 1);
        assert!(m.realized_pnl() > 0.0);
    }

    #[test]
    fn live_tags_are_alphanumeric_and_capped_for_kite() {
        let tag = make_order_tag(
            "micro-prefix_with-symbols-that-is-too-long",
            StrategyKind::EquityImbalance,
            "EXIT",
            123456789,
        );
        assert!(tag.len() <= 20, "tag too long: {tag}");
        assert!(tag.chars().all(|c| c.is_ascii_alphanumeric()), "bad tag: {tag}");
    }

    #[test]
    fn live_flatten_waits_for_broker_exit_fill() {
        use tokio::sync::mpsc;
        let (eq_tx, mut eq_rx) = mpsc::unbounded_channel();
        let (opt_tx, _opt_rx) = mpsc::unbounded_channel();
        let risk = RiskManager::new(100_000.0, 1.0, 15.0, 35.0);
        let mut m = PositionManager::new(RunMode::Live, risk, 5, base_cfg());
        let mut p = ImbalanceParams::default();
        p.min_samples = 3;
        m.enable_equity_imbalance(p);
        m.register_equity(EquityMeta { token: 1, symbol: "INFY".into(), exchange: "NSE".into(), product: "MIS".into() });
        m.arm_live(LiveBridge { equity_tx: eq_tx, options_tx: opt_tx }, "TST-".into());

        run_to_signal(&mut m);
        let mut entry_tag = None;
        while let Ok(cmd) = eq_rx.try_recv() {
            if let OrderCommand::Place(p) = cmd {
                entry_tag = Some(p.tag);
            }
        }
        let entry_tag = entry_tag.expect("entry place command");
        assert!(entry_tag.chars().all(|c| c.is_ascii_alphanumeric()));
        assert!(entry_tag.len() <= 20);

        m.on_order_update(&entry_tag, "COMPLETE", Some(100.05), 6_000);
        assert_eq!(m.open_count(), 1);
        assert_eq!(m.closed_trades().len(), 0);

        m.flatten_all(7_000);
        assert_eq!(m.open_count(), 1, "live flatten must not close locally before broker fill");
        assert_eq!(m.closed_trades().len(), 0);

        let mut exit_tag = None;
        while let Ok(cmd) = eq_rx.try_recv() {
            if let OrderCommand::Place(p) = cmd {
                assert_eq!(p.side, OrderSide::Sell);
                assert!(p.tag.chars().all(|c| c.is_ascii_alphanumeric()));
                assert!(p.tag.len() <= 20);
                exit_tag = Some(p.tag);
            }
        }
        let exit_tag = exit_tag.expect("flatten exit order");
        m.on_order_update(&exit_tag, "COMPLETE", Some(100.0), 8_000);
        assert_eq!(m.open_count(), 0);
        assert_eq!(m.closed_trades().len(), 1);
        assert_eq!(m.closed_trades()[0].reason, ExitReason::Flatten);
    }

    fn drain_all(rx: &mut tokio::sync::mpsc::UnboundedReceiver<OrderCommand>) -> Vec<OrderCommand> {
        let mut v = Vec::new();
        while let Ok(c) = rx.try_recv() {
            v.push(c);
        }
        v
    }
    fn place_tags(cmds: &[OrderCommand]) -> Vec<String> {
        cmds.iter()
            .filter_map(|c| if let OrderCommand::Place(p) = c { Some(p.tag.clone()) } else { None })
            .collect()
    }
    fn modify_prices(cmds: &[OrderCommand]) -> Vec<(String, f64)> {
        cmds.iter()
            .filter_map(|c| match c {
                OrderCommand::ModifyByTag { tag, new_price } => Some((tag.clone(), *new_price)),
                _ => None,
            })
            .collect()
    }

    // Bug 1 & 3: a live exit limit at the touch must follow the market (re-peg) and then
    // cross THROUGH the book — it can never rest unfilled while the price runs away.
    #[test]
    fn live_exit_chases_touch_then_crosses_to_guarantee_fill() {
        use tokio::sync::mpsc;
        let (eq_tx, mut eq_rx) = mpsc::unbounded_channel();
        let (opt_tx, _opt_rx) = mpsc::unbounded_channel();
        let risk = RiskManager::new(100_000.0, 1.0, 15.0, 35.0);
        let cfg = ManagerConfig { chase_timeout_ms: 2_000, ..base_cfg() };
        let mut m = PositionManager::new(RunMode::Live, risk, 5, cfg);
        let mut p = ImbalanceParams::default();
        p.min_samples = 3;
        m.enable_equity_imbalance(p);
        m.register_equity(EquityMeta { token: 1, symbol: "INFY".into(), exchange: "NSE".into(), product: "MIS".into() });
        m.arm_live(LiveBridge { equity_tx: eq_tx, options_tx: opt_tx }, "TST".into());

        run_to_signal(&mut m);
        let entry_tag = place_tags(&drain_all(&mut eq_rx)).pop().expect("entry place");
        m.on_order_update(&entry_tag, "COMPLETE", Some(100.05), 6_000);
        assert_eq!(m.open_count(), 1);

        // Target hit -> exit dispatched at the bid (101.0); withhold the broker fill.
        m.on_tick(1, &depth(101.0, 80, 101.05, 50), 7_000);
        let exit_tag = place_tags(&drain_all(&mut eq_rx)).pop().expect("exit place");
        assert_eq!(m.open_count(), 1, "no local close — awaits broker exit fill");

        // Bid falls to 100.5: the sell @101.0 can't fill; chase re-pegs DOWN to track the
        // touch (elapsed 500 < 2000 => track window, not yet crossing).
        m.on_tick(1, &depth(100.5, 80, 100.55, 50), 7_500);
        let mods = modify_prices(&drain_all(&mut eq_rx));
        assert!(
            mods.iter().any(|(t, px)| t == &exit_tag && (*px - 100.5).abs() < 1e-6),
            "exit must re-peg to the new bid 100.5, got {:?}", mods
        );
        assert_eq!(m.open_count(), 1);

        // Past the chase window: cross THROUGH the bid (100.5 - 10*0.05 = 100.0).
        m.on_tick(1, &depth(100.5, 80, 100.55, 50), 9_500);
        let mods = modify_prices(&drain_all(&mut eq_rx));
        assert!(
            mods.iter().any(|(t, px)| t == &exit_tag && (*px - 100.0).abs() < 1e-6),
            "exit must cross through to 100.0 after the window, got {:?}", mods
        );

        // Broker fills the crossed exit -> position closes.
        m.on_order_update(&exit_tag, "COMPLETE", Some(100.0), 10_000);
        assert_eq!(m.open_count(), 0);
        assert_eq!(m.closed_trades().len(), 1);
    }

    // Bug 2: the EOD flatten must NOT skip an already-exiting position (old code carried
    // it overnight). It must re-price the resting exit THROUGH the book.
    #[test]
    fn live_flatten_crosses_an_already_exiting_position() {
        use tokio::sync::mpsc;
        let (eq_tx, mut eq_rx) = mpsc::unbounded_channel();
        let (opt_tx, _opt_rx) = mpsc::unbounded_channel();
        let risk = RiskManager::new(100_000.0, 1.0, 15.0, 35.0);
        // Long chase window so the exit stays "tracking" (not auto-crossed) before flatten.
        let cfg = ManagerConfig { chase_timeout_ms: 60_000, ..base_cfg() };
        let mut m = PositionManager::new(RunMode::Live, risk, 5, cfg);
        let mut p = ImbalanceParams::default();
        p.min_samples = 3;
        m.enable_equity_imbalance(p);
        m.register_equity(EquityMeta { token: 1, symbol: "INFY".into(), exchange: "NSE".into(), product: "MIS".into() });
        m.arm_live(LiveBridge { equity_tx: eq_tx, options_tx: opt_tx }, "TST".into());

        run_to_signal(&mut m);
        let entry_tag = place_tags(&drain_all(&mut eq_rx)).pop().expect("entry place");
        m.on_order_update(&entry_tag, "COMPLETE", Some(100.05), 6_000);

        // Target hit -> position now mid-exit with a resting (unfilled) limit.
        m.on_tick(1, &depth(101.0, 80, 101.05, 50), 7_000);
        let exit_tag = place_tags(&drain_all(&mut eq_rx)).pop().expect("exit place");
        assert_eq!(m.open_count(), 1);

        // EOD flatten: must cross the resting exit (101.0 bid - 10*0.05 = 100.5), not skip.
        m.flatten_all(8_000);
        let mods = modify_prices(&drain_all(&mut eq_rx));
        assert!(
            mods.iter().any(|(t, px)| t == &exit_tag && (*px - 100.5).abs() < 1e-6),
            "flatten must cross the already-exiting position's resting order, got {:?}", mods
        );
        assert_eq!(m.open_count(), 1, "still awaiting the broker fill");

        m.on_order_update(&exit_tag, "COMPLETE", Some(100.5), 9_000);
        assert_eq!(m.open_count(), 0);
        assert_eq!(m.closed_trades().len(), 1);
    }

    // Bug 4: a favorable move (bid falling for a buy) re-pegs to harvest the better price
    // but must NOT consume the chase budget, so it never forces a cross to the ask.
    #[test]
    fn favorable_repegs_do_not_burn_the_chase_budget() {
        use tokio::sync::mpsc;
        let (eq_tx, mut eq_rx) = mpsc::unbounded_channel();
        let (opt_tx, _opt_rx) = mpsc::unbounded_channel();
        let risk = RiskManager::new(100_000.0, 1.0, 15.0, 35.0);
        let cfg = ManagerConfig {
            max_repegs: 2,
            chase_timeout_ms: 1_000_000, // only re-peg COUNT could force a cross here
            min_repeg_interval_ms: 0,
            repeg_threshold_ticks: 1,
            tick_size: 0.05,
            ..base_cfg()
        };
        let mut m = PositionManager::new(RunMode::Live, risk, 5, cfg);
        let mut p = ImbalanceParams::default();
        p.min_samples = 3;
        m.enable_equity_imbalance(p);
        m.register_equity(EquityMeta { token: 1, symbol: "INFY".into(), exchange: "NSE".into(), product: "MIS".into() });
        m.arm_live(LiveBridge { equity_tx: eq_tx, options_tx: opt_tx }, "TST".into());

        run_to_signal(&mut m); // live Long entry resting at the bid 100.0
        let _ = drain_all(&mut eq_rx);

        // Six favorable moves (bid dropping — cheaper to buy). The order harvests DOWN.
        let mut t = 4_000u64;
        for step in 1..=6 {
            let bid = 100.0 - 0.05 * step as f64;
            m.on_tick(1, &depth(bid, 12, bid + 0.05, 90), t);
            t += 1_000;
        }
        let mods = modify_prices(&drain_all(&mut eq_rx));
        assert!(!mods.is_empty(), "favorable moves should still re-peg to harvest the better price");
        assert!(
            mods.iter().all(|(_, px)| *px <= 100.0 + 1e-9),
            "a favorable harvest must never cross UP to the ask, got {:?}", mods
        );
        // Budget intact: the order is still resting, never crossed/filled.
        assert_eq!(m.open_count(), 0, "no cross-to-ask fill happened");
        assert!(m.pending_count() >= 1, "the entry order is still working");
    }

    #[test]
    fn live_late_entry_fill_after_flatten_is_exited() {
        use tokio::sync::mpsc;
        let (eq_tx, mut eq_rx) = mpsc::unbounded_channel();
        let (opt_tx, _opt_rx) = mpsc::unbounded_channel();
        let risk = RiskManager::new(100_000.0, 1.0, 15.0, 35.0);
        let mut m = PositionManager::new(RunMode::Live, risk, 5, base_cfg());
        let mut p = ImbalanceParams::default();
        p.min_samples = 3;
        m.enable_equity_imbalance(p);
        m.register_equity(EquityMeta { token: 1, symbol: "INFY".into(), exchange: "NSE".into(), product: "MIS".into() });
        m.arm_live(LiveBridge { equity_tx: eq_tx, options_tx: opt_tx }, "TST".into());

        run_to_signal(&mut m);
        let mut entry_tag = None;
        while let Ok(cmd) = eq_rx.try_recv() {
            if let OrderCommand::Place(p) = cmd {
                entry_tag = Some(p.tag);
            }
        }
        let entry_tag = entry_tag.expect("entry place command");

        m.flatten_all(4_500);
        assert_eq!(m.pending_count(), 1, "pending live entry stays tracked until broker status");

        // Drain cancel command, then simulate a broker fill racing the cancel.
        while eq_rx.try_recv().is_ok() {}
        m.on_order_update(&entry_tag, "COMPLETE", Some(100.05), 5_000);
        assert_eq!(m.pending_count(), 0);
        assert_eq!(m.open_count(), 1, "late fill remains open locally until exit fill confirms");

        let mut exit_tag = None;
        while let Ok(cmd) = eq_rx.try_recv() {
            if let OrderCommand::Place(p) = cmd {
                assert_eq!(p.side, OrderSide::Sell);
                exit_tag = Some(p.tag);
            }
        }
        let exit_tag = exit_tag.expect("late-fill exit order");
        m.on_order_update(&exit_tag, "COMPLETE", Some(100.0), 6_000);
        assert_eq!(m.open_count(), 0);
        assert_eq!(m.closed_trades().len(), 1);
        assert_eq!(m.closed_trades()[0].reason, ExitReason::Flatten);
    }

    #[test]
    fn chase_secures_entry_and_never_fills_on_repeg_tick() {
        // Realistic chase: passive start, re-peg as the market runs, then cross.
        let cfg = ManagerConfig {
            max_repegs: 5,
            repeg_threshold_ticks: 1,
            chase_timeout_ms: 10_000,
            min_repeg_interval_ms: 0,
            ..base_cfg()
        };
        let mut m = manager_with(cfg);
        run_to_signal(&mut m); // passive long resting @ best bid 100.00
        assert_eq!(m.open_count(), 0);

        // Market drifts up tick by tick; the order should chase (re-peg) and only
        // ever fill on a tick AFTER a re-peg, never the same one.
        let books = [
            depth(100.05, 12, 100.10, 90),
            depth(100.10, 12, 100.15, 90),
            depth(100.10, 12, 100.15, 90),
            depth(100.10, 12, 100.15, 90),
        ];
        let mut t = 4000;
        for b in &books {
            let before = m.open_count();
            m.on_tick(1, b, t);
            // A fill may happen, but never produce a same-tick fill after a re-peg:
            // structurally enforced by born_tick. Sanity: opens are monotonic.
            assert!(m.open_count() >= before);
            t += 1000;
        }
        // Give it room to cross and fill if it hasn't yet.
        drive_until_open(&mut m, &depth(100.10, 12, 100.15, 90), t);
        assert_eq!(m.open_count(), 1, "chase should eventually secure the entry");
    }
}
