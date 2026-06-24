//! Multi-leg defined-risk option SELLING — slice 1: pure entry logic (no I/O, no live orders).
//!
//! Ports the satakarni sandbox's entry path into the Rust engine: a Mon/Tue day-gate,
//! defined-risk leg selection (iron condor / iron fly), and **wings-first, marketable-limit**
//! placement sequencing. Kept pure and heavily unit-tested. The live order-routing, combo
//! management (TP/stop/15:15), and paper-fill simulation are later slices. Paper-first.
//!
//! Why this shape (from the Kite Connect docs): the API has **no atomic multi-leg / basket
//! order** — each leg is a separate `POST /orders/regular`. So we manage legging risk in
//! code: place the LONG protective wings (BUY) FIRST so we're never momentarily naked, and
//! price every leg as a marketable limit (cross the touch by `ENTRY_SLIP`) so none is missed.

use crate::execution::{
    fetch_basket_final_margin, fetch_live_available_funds, BasketMarginOrder, OrderCommand,
    OrderSide, OrderUpdate, PlaceOrderCmd,
};
use crate::greeks::{compute_greeks, compute_time_to_expiry_at};
use crate::models::{OptionContract, OptionType};
use crate::portfolio::SharedCircuit;
use crate::store::TickStore;
use crate::websocket::TickEvent;
use chrono::{Datelike, FixedOffset, NaiveDate, TimeZone, Timelike, Utc, Weekday};
use std::cmp::Ordering;
use std::collections::{HashMap, VecDeque};
use tokio::sync::{broadcast, mpsc};
use tracing::{info, warn};

/// Wing width in points (NIFTY strikes are 50 apart; 100 = 2 strikes — the only width the
/// recorded ±5-strike chain can seat).
pub const WING: f64 = 100.0;
/// Marketable-limit cushion (₹). Each leg crosses the touch by this so it fills immediately
/// and we never get stuck holding 3 of 4 legs.
pub const ENTRY_SLIP: f64 = 0.50;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SellStructure {
    Condor,
    Fly,
}

impl SellStructure {
    /// (short |delta| target — None = ATM ; wing width in points).
    fn params(self) -> (Option<f64>, f64) {
        match self {
            SellStructure::Condor => (Some(0.25), WING),
            SellStructure::Fly => (None, WING),
        }
    }
}

/// Per-strike quote the selector needs: greeks + top of book for both sides.
#[derive(Debug, Clone, Copy)]
pub struct StrikeQuote {
    pub strike: f64,
    pub ce_delta: f64,
    pub pe_delta: f64,
    pub ce_bid: f64,
    pub ce_ask: f64,
    pub pe_bid: f64,
    pub pe_ask: f64,
}

/// One planned leg of the spread.
#[derive(Debug, Clone, PartialEq)]
pub struct PlannedLeg {
    pub strike: f64,
    pub opt: OptionType,
    pub side: OrderSide,
    /// true = long protective wing (the hedge that defines the risk and unlocks margin).
    pub wing: bool,
}

/// Mon/Tue only — weekly NIFTY theta-selling is for 1-DTE (Mon) and expiry day (Tue),
/// the only days intraday decay is fast enough to be worth the gamma.
pub fn weekday_allows(wd: Weekday) -> bool {
    matches!(wd, Weekday::Mon | Weekday::Tue)
}

fn has_strike(q: &[StrikeQuote], k: f64) -> bool {
    q.iter().any(|s| (s.strike - k).abs() < 1e-6)
}

/// Select the 4 defined-risk legs for `structure`. Returns None if the chain can't seat the
/// wings (e.g. the short would sit at the chain edge with no strike beyond it for the wing).
pub fn select_legs(quotes: &[StrikeQuote], spot: f64, structure: SellStructure) -> Option<Vec<PlannedLeg>> {
    if quotes.is_empty() || !spot.is_finite() {
        return None;
    }
    let (tgt, wing) = structure.params();
    let usable: Vec<&StrikeQuote> = quotes.iter().filter(|q| q.strike.is_finite()).collect();
    if usable.is_empty() {
        return None;
    }
    let kmin = usable.iter().map(|q| q.strike).fold(f64::INFINITY, f64::min);
    let kmax = usable.iter().map(|q| q.strike).fold(f64::NEG_INFINITY, f64::max);

    // Short-strike picker for one side. `is_ce` avoids needing OptionType: Copy.
    let pick = |is_ce: bool| -> Option<f64> {
        let delta_of = |q: &StrikeQuote| if is_ce { q.ce_delta } else { q.pe_delta };
        let room = |k: f64| if is_ce { k + wing <= kmax } else { k - wing >= kmin };
        match tgt {
            None => quotes
                .iter()
                // ATM fly uses ONE strike for both shorts, so it needs BOTH wings to fit the
                // chain. Filter to strikes with room, THEN take the nearest to spot — so a
                // valid near-ATM fly isn't dropped just because the literal-closest strike sits
                // at the chain edge.
                .filter(|q| q.strike + wing <= kmax && q.strike - wing >= kmin)
                .min_by(|a, b| {
                    (a.strike - spot)
                        .abs()
                        .partial_cmp(&(b.strike - spot).abs())
                        .unwrap_or(Ordering::Equal)
                })
                .map(|q| q.strike),
            Some(t) => quotes
                .iter()
                .filter(|q| {
                    q.strike.is_finite()
                        && delta_of(q).is_finite()
                        && (delta_of(q).abs() - t).abs() <= 0.12
                        && room(q.strike)
                })
                .min_by(|a, b| {
                    (delta_of(a).abs() - t).abs()
                        .partial_cmp(&(delta_of(b).abs() - t).abs())
                        .unwrap_or(Ordering::Equal)
                })
                .map(|q| q.strike),
        }
    };

    let ce_s = pick(true)?;
    let pe_s = pick(false)?;
    let ce_w = ce_s + wing;
    let pe_w = pe_s - wing;
    if ce_w > kmax || pe_w < kmin || !has_strike(quotes, ce_w) || !has_strike(quotes, pe_w) {
        return None;
    }
    Some(vec![
        PlannedLeg { strike: ce_s, opt: OptionType::CE, side: OrderSide::Sell, wing: false },
        PlannedLeg { strike: ce_w, opt: OptionType::CE, side: OrderSide::Buy, wing: true },
        PlannedLeg { strike: pe_s, opt: OptionType::PE, side: OrderSide::Sell, wing: false },
        PlannedLeg { strike: pe_w, opt: OptionType::PE, side: OrderSide::Buy, wing: true },
    ])
}

/// Placement order: all LONG wings (BUY) first, then the SHORTS (SELL). If a sell fails
/// after this we're still hedged; selling first and then missing a leg leaves a naked short
/// at full margin — the exact failure this ordering prevents.
///
/// IMPORTANT (slice 3b): this only fixes the *in-memory* order. A Kite `order_id` is NOT a
/// fill — final status can still be REJECTED. The live layer MUST place the wings, wait for
/// each to reach COMPLETE with full `filled_quantity`, and only THEN place the shorts; if any
/// short rejects, immediately flatten the filled wings. See [`basket_margin_ok`] for the
/// pre-trade margin gate that must also pass first.
pub fn placement_sequence(legs: &[PlannedLeg]) -> Vec<PlannedLeg> {
    let mut out: Vec<PlannedLeg> = legs.iter().filter(|l| l.side == OrderSide::Buy).cloned().collect();
    out.extend(legs.iter().filter(|l| l.side == OrderSide::Sell).cloned());
    out
}

/// Marketable limit so a leg fills immediately and is never missed: BUY pays up `ENTRY_SLIP`
/// over the ask; SELL gives up `ENTRY_SLIP` under the bid. Floored at one tick.
pub fn marketable_limit(side: OrderSide, bid: f64, ask: f64) -> f64 {
    match side {
        OrderSide::Buy => ask + ENTRY_SLIP,
        OrderSide::Sell => (bid - ENTRY_SLIP).max(0.05),
    }
}

// ── Slice 2: combo economics + exit/sizing (pure decision logic) ──────────────────────

/// Why a live combo was closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitReason {
    TakeProfit,
    Stop,
    Time,
}

fn quote_at(quotes: &[StrikeQuote], strike: f64) -> Option<&StrikeQuote> {
    quotes.iter().find(|q| (q.strike - strike).abs() < 1e-6)
}

/// Entry fill: a SELL hits the bid, a BUY lifts the ask (the side a marketable limit trades).
fn entry_fill(leg: &PlannedLeg, q: &StrikeQuote) -> f64 {
    match (leg.opt, leg.side) {
        (OptionType::CE, OrderSide::Sell) => q.ce_bid,
        (OptionType::CE, OrderSide::Buy) => q.ce_ask,
        (OptionType::PE, OrderSide::Sell) => q.pe_bid,
        (OptionType::PE, OrderSide::Buy) => q.pe_ask,
    }
}

/// Close fill: a short is bought back at the ask, a long wing is sold at the bid.
fn close_fill(leg: &PlannedLeg, q: &StrikeQuote) -> f64 {
    match (leg.opt, leg.side) {
        (OptionType::CE, OrderSide::Sell) => q.ce_ask,
        (OptionType::CE, OrderSide::Buy) => q.ce_bid,
        (OptionType::PE, OrderSide::Sell) => q.pe_ask,
        (OptionType::PE, OrderSide::Buy) => q.pe_bid,
    }
}

/// Net credit per unit received at entry (shorts at bid − wings at ask). Positive = credit.
/// None if a leg's strike is missing from the chain.
pub fn combo_credit(legs: &[PlannedLeg], quotes: &[StrikeQuote]) -> Option<f64> {
    let mut credit = 0.0;
    for leg in legs {
        let px = entry_fill(leg, quote_at(quotes, leg.strike)?);
        credit += if leg.side == OrderSide::Sell { px } else { -px };
    }
    Some(credit)
}

/// Cost per unit to flatten now (buy back shorts at ask − sell wings at bid). As the shorts
/// decay this falls; running gain = credit − this.
pub fn combo_close_cost(legs: &[PlannedLeg], quotes: &[StrikeQuote]) -> Option<f64> {
    let mut cost = 0.0;
    for leg in legs {
        let px = close_fill(leg, quote_at(quotes, leg.strike)?);
        cost += if leg.side == OrderSide::Sell { px } else { -px };
    }
    Some(cost)
}

/// Exit rule: 15:15 hard close, else take-profit at 50% of credit, else stop at 50%
/// of defined max loss. `ist_mins` = minutes since IST midnight (15:15 = 915).
pub fn exit_reason(credit: f64, close_cost: f64, max_loss_unit: f64, ist_mins: u32) -> Option<ExitReason> {
    if ist_mins >= 915 {
        return Some(ExitReason::Time);
    }
    if credit <= 0.0 || max_loss_unit <= 0.0 {
        return None;
    }
    let gain = credit - close_cost;
    if gain >= 0.5 * credit {
        Some(ExitReason::TakeProfit)
    } else if gain <= -0.5 * max_loss_unit {
        Some(ExitReason::Stop)
    } else {
        None
    }
}

/// Defined max loss for one lot = (wing width − credit) × lot_size.
pub fn max_loss_per_lot(credit: f64, lot_size: u32) -> f64 {
    (WING - credit) * lot_size as f64
}

/// Lots sized so the defined max loss stays within `risk_frac` of capital, capped at
/// `max_lots`. Returns 0 when capital can't fund one lot's risk budget — the caller skips,
/// so a small account correctly takes NO trade rather than over-risking.
pub fn size_lots(capital: f64, max_loss_per_lot: f64, risk_frac: f64, max_lots: u32) -> u32 {
    if max_loss_per_lot <= 0.0 || capital <= 0.0 {
        return 0;
    }
    ((capital * risk_frac / max_loss_per_lot).floor() as u32).min(max_lots)
}

// ── Slice 3a: account-wide trade-admission policy (multi-leg priority) ─────────────────

/// Account state the admission policy reads (shared across the buy + sell engines).
#[derive(Debug, Clone, Copy)]
pub struct AdmissionState {
    /// Any position currently open, account-wide. A multi-leg spread counts as ONE.
    pub position_open: bool,
    /// A single-leg trade has completed today.
    pub single_traded_today: bool,
    /// A multi-leg trade has *completed* today.
    pub multileg_traded_today: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Strategy {
    SingleLeg,
    MultiLeg,
}

/// Trade-admission policy (user's rule):
///   • exactly one position open at a time, account-wide (a multi-leg = ONE position, all legs);
///   • single-leg gets at most one completed trade/day;
///   • multi-leg gets at most one completed trade/day;
///   • a completed trade in one family does not bench the other family.
pub fn may_enter(strategy: Strategy, st: AdmissionState) -> bool {
    if st.position_open {
        return false; // one trade at a time, whoever holds the slot
    }
    match strategy {
        Strategy::MultiLeg => !st.multileg_traded_today,
        Strategy::SingleLeg => !st.single_traded_today,
    }
}

/// Pre-trade margin gate (slice 3b). A Kite order_id is NOT execution — RMS can REJECT for
/// insufficient margin after placement. So before sending ANY leg, slice 3b must call
/// `POST /margins/basket?consider_positions=true` for the 4-leg basket and pass the returned
/// **final** margin (the spread-benefit number, not the naked sum) here; we require it plus a
/// buffer to fit available funds. Conservative: any non-positive margin or funds fails.
pub fn basket_margin_ok(final_margin: f64, available_funds: f64, buffer_frac: f64) -> bool {
    // A negative buffer would *weaken* the check (shrink the required margin) — treat it as a
    // caller error and refuse the trade.
    buffer_frac >= 0.0
        && final_margin > 0.0
        && available_funds > 0.0
        && final_margin * (1.0 + buffer_frac) <= available_funds
}

// ── Drift-zone gating (ported from the satakarni sandbox) ─────────────────────────────
// A short-premium structure profits only while spot stays inside its credit-adjusted zone.
// So gate ENTRY on how much of that zone the morning range already ate, and TRIM in-trade on
// a fast move measured against the zone — both relative to the structure's OWN geometry.

/// Skip the entry if the opening range has eaten more than this fraction of the profit zone.
pub const ENTRY_DRIFT_ZONE_CAP: f64 = 0.50;
/// In-trade trim when a MOVE_WINDOW_MIN move consumes more than this fraction of the zone.
pub const MOVE_ZONE_CAP: f64 = 0.35;
/// In-trade trim when the MOVE_WINDOW_MIN move exceeds this fraction of spot (short gamma can't
/// take a fast realized move).
pub const MOVE_PCT: f64 = 0.0025;
pub const MOVE_WINDOW_MIN: u32 = 5;

/// Credit-adjusted expiry profit zone for the short strikes: `[min_short − credit, max_short +
/// credit]`. Condor → put/call shorts; fly → ATM ± credit. Returns `(lower, upper, width)`, or
/// None if there are no shorts or credit isn't positive.
pub fn profit_zone(legs: &[PlannedLeg], credit: f64) -> Option<(f64, f64, f64)> {
    let shorts: Vec<f64> = legs.iter().filter(|l| l.side == OrderSide::Sell).map(|l| l.strike).collect();
    if shorts.is_empty() || credit <= 0.0 {
        return None;
    }
    let lower = shorts.iter().cloned().fold(f64::INFINITY, f64::min) - credit;
    let upper = shorts.iter().cloned().fold(f64::NEG_INFINITY, f64::max) + credit;
    let width = upper - lower;
    (width > 0.0).then_some((lower, upper, width))
}

/// Entry drift gate: admit only if the opening range is at most `ENTRY_DRIFT_ZONE_CAP` of the
/// profit-zone width (the day hasn't already consumed the cushion).
pub fn entry_drift_admits(opening_range_pts: f64, zone_width: f64) -> bool {
    zone_width > 0.0 && opening_range_pts / zone_width <= ENTRY_DRIFT_ZONE_CAP
}

/// In-trade move trim: exit if the `MOVE_WINDOW_MIN`-minute move (`move_pts = |spot(t) −
/// spot(t−window)|`) exceeds `MOVE_PCT` of spot OR `MOVE_ZONE_CAP` of the profit zone.
pub fn move_trims(move_pts: f64, spot: f64, zone_width: f64) -> bool {
    let by_spot = spot > 0.0 && move_pts / spot > MOVE_PCT;
    let by_zone = zone_width > 0.0 && move_pts / zone_width > MOVE_ZONE_CAP;
    by_spot || by_zone
}

// ── Slice 3b: live/paper runtime harness ─────────────────────────────────────────────

const HOLDER: &str = "multileg";
const ENTRY_MIN: u32 = 9 * 60 + 45;
const CUTOFF_MIN: u32 = 14 * 60 + 30;
const EXIT_MIN: u32 = 15 * 60 + 15;
const OPEN_RANGE_START_SEC: u32 = 9 * 3600 + 15 * 60 + 5;
const OPEN_RANGE_END_SEC: u32 = ENTRY_MIN * 60;
const RISK_FRAC: f64 = 0.10;
const MAX_LOTS: u32 = 5;
const SCAN_INTERVAL_SECS: u64 = 45;
const MARGIN_BUFFER_FRAC: f64 = 0.15;
const LIVE_ORDER_TIMEOUT_SECS: u64 = 25;
const CANCEL_RECONCILE_TIMEOUT_SECS: u64 = 20;
const ORDER_STATUS_POLL_MS: u64 = 750;

#[derive(Debug, Clone)]
struct LegMarket {
    token: u32,
    tradingsymbol: String,
    bid: f64,
    ask: f64,
}

#[derive(Debug, Clone)]
struct LiveLeg {
    plan: PlannedLeg,
    market: LegMarket,
    entry_px: f64,
}

#[derive(Debug, Clone)]
struct FilledLiveLeg {
    leg: LiveLeg,
    qty: u32,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct LiveOrderFill {
    qty: u32,
    avg_price: f64,
}

#[derive(Debug, Clone, PartialEq)]
struct LiveOrderFailure {
    message: String,
    fill: Option<LiveOrderFill>,
    reconciled: bool,
}

#[derive(Debug, Clone)]
struct LiveEntryFailure {
    message: String,
    safe_to_release: bool,
}

#[derive(Debug, Clone)]
struct ChainSnapshot {
    spot: f64,
    expiry: String,
    quotes: Vec<StrikeQuote>,
    markets: HashMap<(u64, OptionType), LegMarket>,
    lot_size: u32,
}

#[derive(Debug, Clone)]
struct ActiveCombo {
    underlying: String,
    structure: SellStructure,
    legs: Vec<LiveLeg>,
    lots: u32,
    lot_size: u32,
    credit: f64,
    max_loss_unit: f64,
    zone_width: f64,
}

#[derive(Default)]
struct UnderlyingState {
    day: Option<NaiveDate>,
    spot_history: VecDeque<(u64, f64)>,
    active: Option<ActiveCombo>,
    traded_today: bool,
    last_scan_ms: u64,
    last_skip_log_ms: u64,
}

#[derive(Debug, Clone)]
struct LiveBridge {
    order_tx: mpsc::UnboundedSender<OrderCommand>,
    api_key: String,
    access_token: String,
    exchange: String,
    product: String,
    variety: String,
    tag_prefix: String,
}

pub struct MultiLegEngine {
    contracts: Vec<OptionContract>,
    store: TickStore,
    underlying_tokens: HashMap<String, u32>,
    risk_free_rate: f64,
    dividend_yield: f64,
    capital: f64,
    shared_circuit: SharedCircuit,
    live: Option<LiveBridge>,
    updates_rx: Option<mpsc::UnboundedReceiver<OrderUpdate>>,
    state: HashMap<String, UnderlyingState>,
    order_seq: u64,
}

impl MultiLegEngine {
    pub fn new(
        contracts: Vec<OptionContract>,
        store: TickStore,
        underlying_tokens: HashMap<String, u32>,
        risk_free_rate: f64,
        dividend_yield: f64,
        capital: f64,
        shared_circuit: SharedCircuit,
    ) -> Self {
        Self {
            contracts,
            store,
            underlying_tokens,
            risk_free_rate,
            dividend_yield,
            capital: capital.max(0.0),
            shared_circuit,
            live: None,
            updates_rx: None,
            state: HashMap::new(),
            order_seq: 0,
        }
    }

    pub fn set_live_order_bridge(
        &mut self,
        order_tx: mpsc::UnboundedSender<OrderCommand>,
        updates_rx: Option<mpsc::UnboundedReceiver<OrderUpdate>>,
        api_key: String,
        access_token: String,
        exchange: String,
        product: String,
        variety: String,
        tag_prefix: String,
    ) {
        self.live = Some(LiveBridge {
            order_tx,
            api_key,
            access_token,
            exchange,
            product,
            variety,
            tag_prefix,
        });
        self.updates_rx = updates_rx;
    }

    pub fn spawn(mut self, mut rx: broadcast::Receiver<TickEvent>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            info!(
                "Multi-leg engine started | structure=CONDOR | holder={} | {} mode",
                HOLDER,
                if self.live.is_some() { "LIVE" } else { "paper" }
            );
            loop {
                match rx.recv().await {
                    Ok(event) => self.on_event(&event).await,
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!("Multi-leg engine lagged by {} messages", n);
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        })
    }

    async fn on_event(&mut self, _event: &TickEvent) {
        self.on_event_at(now_ms()).await;
    }

    async fn on_event_at(&mut self, now_ms: u64) {
        let (day, wd, mins, secs) = ist_parts(now_ms);
        let underlyings = self.underlyings();
        for underlying in underlyings {
            self.reset_day(&underlying, day);
            if let Some(spot) = self.current_spot(&underlying) {
                self.record_spot(&underlying, now_ms, secs, spot);
            }
            if self.state.get(&underlying).and_then(|s| s.active.as_ref()).is_some() {
                self.manage_active(&underlying, now_ms, mins).await;
                continue;
            }
            let should_scan = {
                let st = self.state.entry(underlying.clone()).or_default();
                now_ms.saturating_sub(st.last_scan_ms) >= SCAN_INTERVAL_SECS * 1_000
            };
            if !should_scan {
                continue;
            }
            if let Some(st) = self.state.get_mut(&underlying) {
                st.last_scan_ms = now_ms;
            }
            self.process_underlying(&underlying, now_ms, day, wd, mins).await;
        }
    }

    async fn process_underlying(
        &mut self,
        underlying: &str,
        now_ms: u64,
        day: NaiveDate,
        wd: Weekday,
        mins: u32,
    ) {
        if self.state.get(underlying).and_then(|s| s.active.as_ref()).is_some() {
            self.manage_active(underlying, now_ms, mins).await;
            return;
        }

        if !weekday_allows(wd) || mins < ENTRY_MIN || mins > CUTOFF_MIN {
            return;
        }
        if self
            .state
            .get(underlying)
            .map(|s| s.traded_today)
            .unwrap_or(false)
            || !crate::portfolio::can_enter_holder(&self.shared_circuit, HOLDER)
            || crate::portfolio::is_locked(&self.shared_circuit)
        {
            return;
        }

        let Some(snapshot) = self.build_snapshot(underlying, now_ms) else {
            return;
        };
        let Some((active, drift_frac)) = self.plan_entry(underlying, now_ms, day, &snapshot) else {
            return;
        };
        if !entry_drift_admits(
            drift_frac * active.zone_width,
            active.zone_width,
        ) {
            return;
        }
        self.open_combo(active, snapshot).await;
    }

    fn underlyings(&self) -> Vec<String> {
        let mut out: Vec<String> = self.contracts.iter().map(|c| c.underlying.clone()).collect();
        out.sort();
        out.dedup();
        out
    }

    fn reset_day(&mut self, underlying: &str, day: NaiveDate) {
        let st = self.state.entry(underlying.to_string()).or_default();
        if st.day != Some(day) {
            *st = UnderlyingState { day: Some(day), ..UnderlyingState::default() };
        }
    }

    fn current_spot(&self, underlying: &str) -> Option<f64> {
        self.underlying_tokens
            .get(underlying)
            .and_then(|token| self.store.get(*token))
            .map(|t| t.ltp)
            .filter(|v| v.is_finite() && *v > 0.0)
    }

    fn record_spot(&mut self, underlying: &str, now_ms: u64, secs: u32, spot: f64) {
        let st = self.state.entry(underlying.to_string()).or_default();
        if secs >= OPEN_RANGE_START_SEC {
            st.spot_history.push_back((now_ms, spot));
            while st
                .spot_history
                .front()
                .map(|(t, _)| now_ms.saturating_sub(*t) > 6 * 60 * 60 * 1_000)
                .unwrap_or(false)
            {
                st.spot_history.pop_front();
            }
        }
    }

    fn build_snapshot(&self, underlying: &str, now_ms: u64) -> Option<ChainSnapshot> {
        let expiry = self
            .contracts
            .iter()
            .filter(|c| c.underlying == underlying)
            .map(|c| c.expiry.clone())
            .min()?;
        let spot = self.current_spot(underlying)?;
        let as_of_utc = Utc
            .timestamp_millis_opt(now_ms as i64)
            .single()
            .unwrap_or_else(Utc::now);
        let t_years = compute_time_to_expiry_at(&expiry, as_of_utc)?;
        let mut partial: HashMap<u64, (Option<(f64, f64, f64)>, Option<(f64, f64, f64)>)> = HashMap::new();
        let mut markets = HashMap::new();
        let mut lot_size = None;

        for c in self.contracts.iter().filter(|c| c.underlying == underlying && c.expiry == expiry) {
            let Some(tick) = self.store.get(c.instrument_token) else {
                continue;
            };
            let Some((bid, ask)) = best_bid_ask(&tick) else {
                continue;
            };
            let mid = (bid + ask) / 2.0;
            let Some(greeks) = compute_greeks(
                spot,
                c.strike,
                t_years,
                self.risk_free_rate,
                self.dividend_yield,
                mid,
                c.option_type,
            ) else {
                continue;
            };
            let key = strike_key(c.strike);
            let entry = partial.entry(key).or_insert((None, None));
            match c.option_type {
                OptionType::CE => entry.0 = Some((greeks.delta, bid, ask)),
                OptionType::PE => entry.1 = Some((greeks.delta, bid, ask)),
            }
            markets.insert(
                (key, c.option_type),
                LegMarket {
                    token: c.instrument_token,
                    tradingsymbol: c.tradingsymbol.clone(),
                    bid,
                    ask,
                },
            );
            lot_size = Some(c.lot_size);
        }

        let mut quotes = Vec::new();
        for (key, (ce, pe)) in partial {
            if let (Some((ce_delta, ce_bid, ce_ask)), Some((pe_delta, pe_bid, pe_ask))) = (ce, pe) {
                quotes.push(StrikeQuote {
                    strike: key as f64 / 100.0,
                    ce_delta,
                    pe_delta,
                    ce_bid,
                    ce_ask,
                    pe_bid,
                    pe_ask,
                });
            }
        }
        quotes.sort_by(|a, b| a.strike.partial_cmp(&b.strike).unwrap_or(Ordering::Equal));
        (!quotes.is_empty()).then_some(ChainSnapshot {
            spot,
            expiry,
            quotes,
            markets,
            lot_size: lot_size?,
        })
    }

    fn plan_entry(
        &mut self,
        underlying: &str,
        now_ms: u64,
        day: NaiveDate,
        snapshot: &ChainSnapshot,
    ) -> Option<(ActiveCombo, f64)> {
        let legs = select_legs(&snapshot.quotes, snapshot.spot, SellStructure::Condor)?;
        let credit = combo_credit(&legs, &snapshot.quotes)?;
        let (_zone_lo, _zone_hi, zone_width) = profit_zone(&legs, credit)?;
        let max_loss_unit = WING - credit;
        if credit <= 0.0 || max_loss_unit <= 0.0 {
            return None;
        }
        let opening_range = self.opening_range(underlying, day)?;
        let drift_frac = opening_range / zone_width;
        if drift_frac > ENTRY_DRIFT_ZONE_CAP {
            self.log_skip(
                underlying,
                now_ms,
                format!(
                    "multi-leg skipped: DRIFT-ZONE {:.0}% > {:.0}% (range {:.0} / zone {:.0})",
                    drift_frac * 100.0,
                    ENTRY_DRIFT_ZONE_CAP * 100.0,
                    opening_range,
                    zone_width
                ),
            );
            return None;
        }
        let max_loss_lot = max_loss_unit * snapshot.lot_size as f64;
        let lots = size_lots(self.capital, max_loss_lot, RISK_FRAC, MAX_LOTS);
        if lots == 0 {
            self.log_skip(underlying, now_ms, "multi-leg skipped: risk budget cannot fund one lot".to_string());
            return None;
        }

        let mut live_legs = Vec::with_capacity(legs.len());
        for leg in legs {
            let market = snapshot.markets.get(&(strike_key(leg.strike), leg.opt))?.clone();
            let entry_px = entry_fill(&leg, quote_at(&snapshot.quotes, leg.strike)?);
            live_legs.push(LiveLeg { plan: leg, market, entry_px });
        }

        Some((
            ActiveCombo {
                underlying: underlying.to_string(),
                structure: SellStructure::Condor,
                legs: live_legs,
                lots,
                lot_size: snapshot.lot_size,
                credit,
                max_loss_unit,
                zone_width,
            },
            drift_frac,
        ))
    }

    fn opening_range(&self, underlying: &str, day: NaiveDate) -> Option<f64> {
        let st = self.state.get(underlying)?;
        if st.day != Some(day) {
            return None;
        }
        let vals: Vec<f64> = st
            .spot_history
            .iter()
            .filter_map(|(t, s)| {
                if !s.is_finite() {
                    return None;
                }
                let (sample_day, _wd, _mins, secs) = ist_parts(*t);
                (sample_day == day
                    && secs >= OPEN_RANGE_START_SEC
                    && secs <= OPEN_RANGE_END_SEC)
                    .then_some(*s)
            })
            .collect();
        if vals.len() < 2 {
            return None;
        }
        let min = vals.iter().cloned().fold(f64::INFINITY, f64::min);
        let max = vals.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        Some(max - min)
    }

    async fn open_combo(&mut self, mut active: ActiveCombo, snapshot: ChainSnapshot) {
        if self.live.is_some() && !self.preflight_live_margin(&active, &snapshot).await {
            return;
        }
        if !crate::portfolio::try_claim(&self.shared_circuit, HOLDER) {
            return;
        }

        if self.live.is_some() {
            match self.place_live_entry(&mut active).await {
                Ok(()) => {
                    let key = active.underlying.clone();
                    info!(
                        "MULTILEG LIVE OPEN {} {:?} x{}lot credit {:.2}/u expiry {}",
                        active.underlying, active.structure, active.lots, active.credit, snapshot.expiry
                    );
                    self.state.entry(key).or_default().active = Some(active);
                }
                Err(e) => {
                    if e.safe_to_release {
                        warn!("MULTILEG live entry failed: {}; releasing lock", e.message);
                        crate::portfolio::release(&self.shared_circuit, HOLDER);
                    } else {
                        warn!(
                            "MULTILEG live entry uncertain: {}; keeping global lock held for manual reconciliation",
                            e.message
                        );
                    }
                }
            }
        } else {
            let key = active.underlying.clone();
            info!(
                "MULTILEG PAPER OPEN {} {:?} x{}lot credit {:.2}/u expiry {}",
                active.underlying, active.structure, active.lots, active.credit, snapshot.expiry
            );
            self.state.entry(key).or_default().active = Some(active);
        }
    }

    async fn preflight_live_margin(&self, active: &ActiveCombo, snapshot: &ChainSnapshot) -> bool {
        let Some(live) = &self.live else { return true; };
        let qty = active.lots.saturating_mul(active.lot_size);
        let orders: Vec<BasketMarginOrder> = active
            .legs
            .iter()
            .map(|l| BasketMarginOrder {
                exchange: live.exchange.clone(),
                tradingsymbol: l.market.tradingsymbol.clone(),
                transaction_type: side_word(l.plan.side).to_string(),
                variety: live.variety.clone(),
                product: live.product.clone(),
                order_type: "LIMIT".to_string(),
                quantity: qty,
                price: marketable_limit(l.plan.side, l.market.bid, l.market.ask),
            })
            .collect();
        let funds = match fetch_live_available_funds(&live.api_key, &live.access_token).await {
            Ok(v) => v,
            Err(e) => {
                warn!("MULTILEG margin preflight skipped entry: live funds unavailable: {}", e);
                return false;
            }
        };
        let final_margin = match fetch_basket_final_margin(&live.api_key, &live.access_token, &orders).await {
            Ok(v) => v,
            Err(e) => {
                warn!("MULTILEG margin preflight skipped entry: basket margin unavailable: {}", e);
                return false;
            }
        };
        if !basket_margin_ok(final_margin, funds, MARGIN_BUFFER_FRAC) {
            warn!(
                "MULTILEG margin preflight rejected: final margin ₹{:.0} + {:.0}% buffer > funds ₹{:.0}",
                final_margin,
                MARGIN_BUFFER_FRAC * 100.0,
                funds
            );
            return false;
        }
        info!(
            "MULTILEG margin preflight OK {} {:?}: final ₹{:.0}, funds ₹{:.0}, spot {:.0}",
            active.underlying, active.structure, final_margin, funds, snapshot.spot
        );
        true
    }

    async fn place_live_entry(&mut self, active: &mut ActiveCombo) -> Result<(), LiveEntryFailure> {
        let qty = active.lots.saturating_mul(active.lot_size);
        let mut completed: Vec<FilledLiveLeg> = Vec::new();
        for plan in placement_sequence(&active.legs.iter().map(|l| l.plan.clone()).collect::<Vec<_>>()) {
            let idx = active
                .legs
                .iter()
                .position(|l| l.plan.strike == plan.strike && l.plan.opt == plan.opt && l.plan.side == plan.side)
                .ok_or_else(|| LiveEntryFailure {
                    message: "entry sequence leg missing".to_string(),
                    safe_to_release: true,
                })?;
            let mut leg = active.legs[idx].clone();
            let tag = self.next_tag("MLE");
            let limit = marketable_limit(leg.plan.side, leg.market.bid, leg.market.ask);
            if let Err(e) = self.send_order(&tag, &leg, leg.plan.side, qty, Some(limit)) {
                self.refresh_filled_leg_markets(&active.underlying, &mut completed);
                let flatten = self.flatten_live_fills(&completed, "entry-abort").await;
                let flatten_ok = flatten.is_ok();
                let mut message = format!("leg {} failed before broker placement: {}", tag, e);
                if let Err(e) = flatten {
                    message.push_str(&format!("; abort flatten failed: {}", e));
                }
                return Err(LiveEntryFailure {
                    message,
                    safe_to_release: flatten_ok,
                });
            }
            match self.wait_complete(&tag, qty, LIVE_ORDER_TIMEOUT_SECS).await {
                Ok(fill) => {
                    if fill.avg_price > 0.0 {
                        leg.entry_px = fill.avg_price;
                        active.legs[idx].entry_px = fill.avg_price;
                    }
                    completed.push(FilledLiveLeg { leg, qty: fill.qty });
                }
                Err(failure) => {
                    let failed_side = leg.plan.side;
                    let failed_opt = leg.plan.opt;
                    if let Some(fill) = failure.fill {
                        if fill.qty > 0 {
                            if fill.avg_price > 0.0 {
                                leg.entry_px = fill.avg_price;
                                active.legs[idx].entry_px = fill.avg_price;
                            }
                            completed.push(FilledLiveLeg { leg, qty: fill.qty });
                        }
                    }
                    self.refresh_filled_leg_markets(&active.underlying, &mut completed);
                    let to_flatten =
                        abort_flatten_set(&completed, failed_side, failed_opt, failure.reconciled);
                    let wing_preserved = to_flatten.len() != completed.len();
                    let flatten = self.flatten_live_fills(&to_flatten, "entry-abort").await;
                    let flatten_ok = flatten.is_ok();
                    let mut message = format!("leg {} failed: {}", tag, failure.message);
                    if wing_preserved {
                        message.push_str(&format!(
                            "; short status UNCONFIRMED — preserved long {:?} wing as hedge, MANUAL reconciliation required",
                            failed_opt
                        ));
                    }
                    if let Err(e) = flatten {
                        message.push_str(&format!("; abort flatten failed: {}", e));
                    }
                    return Err(LiveEntryFailure {
                        message,
                        safe_to_release: failure.reconciled && flatten_ok,
                    });
                }
            }
        }
        Ok(())
    }

    async fn manage_active(&mut self, underlying: &str, now_ms: u64, mins: u32) {
        let Some(active) = self.state.get(underlying).and_then(|s| s.active.clone()) else {
            return;
        };
        let Some(snapshot) = self.build_snapshot(underlying, now_ms) else {
            return;
        };
        let plans: Vec<PlannedLeg> = active.legs.iter().map(|l| l.plan.clone()).collect();
        let close_cost = combo_close_cost(&plans, &snapshot.quotes).unwrap_or(active.credit);
        let mut reason = None;
        if let Some(move_pts) = self.recent_move_pts(underlying, now_ms) {
            if move_trims(move_pts, snapshot.spot, active.zone_width) {
                reason = Some(ExitReason::Stop);
            }
        }
        if reason.is_none() {
            reason = exit_reason(active.credit, close_cost, active.max_loss_unit, mins);
        }
        if mins >= EXIT_MIN {
            reason = Some(ExitReason::Time);
        }
        if let Some(exit) = reason {
            self.close_active(active, snapshot, exit).await;
        }
    }

    fn recent_move_pts(&self, underlying: &str, now_ms: u64) -> Option<f64> {
        let st = self.state.get(underlying)?;
        let now_spot = st.spot_history.back()?.1;
        let cutoff = now_ms.saturating_sub(MOVE_WINDOW_MIN as u64 * 60_000);
        let prev = st
            .spot_history
            .iter()
            .rev()
            .find(|(t, _)| *t <= cutoff)
            .map(|(_, s)| *s)?;
        Some((now_spot - prev).abs())
    }

    async fn close_active(&mut self, active: ActiveCombo, snapshot: ChainSnapshot, reason: ExitReason) {
        let qty = active.lots.saturating_mul(active.lot_size);
        let exit_prices = self.exit_prices(&active, &snapshot);
        if self.live.is_some() {
            let mut live_active = active.clone();
            for leg in &mut live_active.legs {
                if let Some(m) = snapshot.markets.get(&(strike_key(leg.plan.strike), leg.plan.opt)) {
                    leg.market = m.clone();
                }
            }
            if let Err(e) = self.flatten_live_legs(&live_active.legs, qty, "exit").await {
                warn!("MULTILEG live exit failed; keeping lock/active position: {}", e);
                return;
            }
        }
        let pnl = realized_pnl(&active.legs, &exit_prices, qty, now_ms());
        self.capital += pnl;
        crate::portfolio::record_for(&self.shared_circuit, HOLDER, pnl);
        crate::portfolio::release(&self.shared_circuit, HOLDER);
        if let Some(st) = self.state.get_mut(&active.underlying) {
            st.active = None;
            st.traded_today = true;
        }
        info!(
            "MULTILEG {:?} CLOSE {} {:?} x{}lot pnl ₹{:+.0} cap ₹{:.0}",
            reason, active.underlying, active.structure, active.lots, pnl, self.capital
        );
    }

    fn exit_prices(&self, active: &ActiveCombo, snapshot: &ChainSnapshot) -> HashMap<(u64, OptionType), f64> {
        let mut out = HashMap::new();
        for leg in &active.legs {
            if let Some(m) = snapshot.markets.get(&(strike_key(leg.plan.strike), leg.plan.opt)) {
                let px = match leg.plan.side {
                    OrderSide::Sell => m.ask,
                    OrderSide::Buy => m.bid,
                };
                out.insert((strike_key(leg.plan.strike), leg.plan.opt), px);
            }
        }
        out
    }

    fn refresh_filled_leg_markets(&self, underlying: &str, fills: &mut [FilledLiveLeg]) {
        let Some(snapshot) = self.build_snapshot(underlying, now_ms()) else {
            for fill in fills {
                if let Some(tick) = self.store.get(fill.leg.market.token) {
                    if let Some((bid, ask)) = best_bid_ask(&tick) {
                        fill.leg.market.bid = bid;
                        fill.leg.market.ask = ask;
                    }
                }
            }
            return;
        };
        for fill in fills {
            if let Some(m) = snapshot
                .markets
                .get(&(strike_key(fill.leg.plan.strike), fill.leg.plan.opt))
            {
                fill.leg.market = m.clone();
            }
        }
    }

    async fn flatten_live_legs(&mut self, legs: &[LiveLeg], qty: u32, label: &str) -> Result<(), String> {
        let fills: Vec<FilledLiveLeg> = legs
            .iter()
            .cloned()
            .map(|leg| FilledLiveLeg { leg, qty })
            .collect();
        self.flatten_live_fills(&fills, label).await
    }

    async fn flatten_live_fills(&mut self, fills: &[FilledLiveLeg], label: &str) -> Result<(), String> {
        let mut seq: Vec<FilledLiveLeg> = fills
            .iter()
            .filter(|f| f.qty > 0 && f.leg.plan.side == OrderSide::Sell)
            .cloned()
            .collect();
        seq.extend(
            fills
                .iter()
                .filter(|f| f.qty > 0 && f.leg.plan.side == OrderSide::Buy)
                .cloned(),
        );
        for fill in seq {
            let side = close_side(fill.leg.plan.side);
            let tag = self.next_tag(if label == "exit" { "MLX" } else { "MLF" });
            let limit = marketable_limit(side, fill.leg.market.bid, fill.leg.market.ask);
            self.send_order(&tag, &fill.leg, side, fill.qty, Some(limit))?;
            match self.wait_complete(&tag, fill.qty, LIVE_ORDER_TIMEOUT_SECS).await {
                Ok(_) => {}
                Err(e) => {
                    if let Some(done) = e.fill {
                        if done.qty >= fill.qty {
                            warn!(
                                "MULTILEG flatten {} completed after cancel/reconcile: tag={} qty={}",
                                label, tag, done.qty
                            );
                            continue;
                        }
                        return Err(format!(
                            "{} flatten {} partially filled {}/{}: {}",
                            label, tag, done.qty, fill.qty, e.message
                        ));
                    }
                    return Err(format!("{} flatten {} failed: {}", label, tag, e.message));
                }
            }
        }
        Ok(())
    }

    fn send_order(
        &self,
        tag: &str,
        leg: &LiveLeg,
        side: OrderSide,
        qty: u32,
        limit_price: Option<f64>,
    ) -> Result<(), String> {
        let live = self.live.as_ref().ok_or_else(|| "live bridge not configured".to_string())?;
        live.order_tx
            .send(OrderCommand::Place(PlaceOrderCmd {
                tag: tag.to_string(),
                tradingsymbol: leg.market.tradingsymbol.clone(),
                quantity: qty,
                side,
                limit_price,
            }))
            .map_err(|e| format!("order channel closed: {}", e))
    }

    async fn wait_complete(
        &mut self,
        tag: &str,
        qty: u32,
        timeout_secs: u64,
    ) -> Result<LiveOrderFill, LiveOrderFailure> {
        let rx = self
            .updates_rx
            .as_mut()
            .ok_or_else(|| LiveOrderFailure {
                message: "live order updates receiver not configured".to_string(),
                fill: None,
                reconciled: false,
            })?;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);
        let mut poll = tokio::time::interval(std::time::Duration::from_millis(ORDER_STATUS_POLL_MS));
        let mut fill = None;
        loop {
            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => {
                    break;
                }
                _ = poll.tick() => {
                    if let Some(live) = &self.live {
                        let _ = live.order_tx.send(OrderCommand::StatusByTag { tag: tag.to_string() });
                    }
                }
                msg = rx.recv() => {
                    let Some(upd) = msg else {
                        return Err(LiveOrderFailure {
                            message: "order update channel closed".to_string(),
                            fill,
                            reconciled: false,
                        });
                    };
                    if upd.tag != tag {
                        continue;
                    }
                    update_fill_progress(&mut fill, &upd);
                    if upd.source == "place_error" {
                        return Err(LiveOrderFailure {
                            message: upd.message.unwrap_or_else(|| "place error".to_string()),
                            fill,
                            reconciled: true,
                        });
                    }
                    if let Some(status) = upd.status.as_deref() {
                        let status_upper = status.to_ascii_uppercase();
                        if status_upper == "COMPLETE" && fill.map(|f| f.qty).unwrap_or(0) >= qty {
                            return Ok(fill.unwrap_or(LiveOrderFill {
                                qty,
                                avg_price: upd.average_price.unwrap_or(0.0),
                            }));
                        }
                        if is_terminal_status(&status_upper) {
                            return Err(LiveOrderFailure {
                                message: upd.message.unwrap_or(status_upper),
                                fill,
                                reconciled: true,
                            });
                        }
                    }
                }
            }
        }

        self.cancel_and_reconcile(tag, qty, fill).await
    }

    async fn cancel_and_reconcile(
        &mut self,
        tag: &str,
        qty: u32,
        mut fill: Option<LiveOrderFill>,
    ) -> Result<LiveOrderFill, LiveOrderFailure> {
        if let Some(live) = &self.live {
            let _ = live.order_tx.send(OrderCommand::CancelByTag { tag: tag.to_string() });
            let _ = live.order_tx.send(OrderCommand::StatusByTag { tag: tag.to_string() });
        }

        let rx = self.updates_rx.as_mut().ok_or_else(|| LiveOrderFailure {
            message: "live order updates receiver not configured after cancel".to_string(),
            fill,
            reconciled: false,
        })?;
        let deadline =
            tokio::time::Instant::now() + std::time::Duration::from_secs(CANCEL_RECONCILE_TIMEOUT_SECS);
        let mut poll = tokio::time::interval(std::time::Duration::from_millis(ORDER_STATUS_POLL_MS));
        loop {
            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => {
                    return Err(LiveOrderFailure {
                        message: format!("timeout waiting for {} broker cancel/reconcile", tag),
                        fill,
                        reconciled: false,
                    });
                }
                _ = poll.tick() => {
                    if let Some(live) = &self.live {
                        let _ = live.order_tx.send(OrderCommand::CancelByTag { tag: tag.to_string() });
                        let _ = live.order_tx.send(OrderCommand::StatusByTag { tag: tag.to_string() });
                    }
                }
                msg = rx.recv() => {
                    let Some(upd) = msg else {
                        return Err(LiveOrderFailure {
                            message: "order update channel closed during cancel/reconcile".to_string(),
                            fill,
                            reconciled: false,
                        });
                    };
                    if upd.tag != tag {
                        continue;
                    }
                    update_fill_progress(&mut fill, &upd);
                    if upd.source == "cancel_error" || upd.source == "status_error" {
                        if let Some(msg) = upd.message.as_deref() {
                            warn!("MULTILEG order {} reconcile issue: {}", tag, msg);
                        }
                        continue;
                    }
                    if let Some(status) = upd.status.as_deref() {
                        let status_upper = status.to_ascii_uppercase();
                        if status_upper == "COMPLETE" && fill.map(|f| f.qty).unwrap_or(0) >= qty {
                            return Err(LiveOrderFailure {
                                message: format!("{} filled after timeout/cancel request", tag),
                                fill,
                                reconciled: true,
                            });
                        }
                        if is_terminal_status(&status_upper) {
                            return Err(LiveOrderFailure {
                                message: upd.message.unwrap_or(status_upper),
                                fill,
                                reconciled: true,
                            });
                        }
                    }
                }
            }
        }
    }

    fn next_tag(&mut self, kind: &str) -> String {
        self.order_seq = self.order_seq.saturating_add(1);
        let prefix = self
            .live
            .as_ref()
            .map(|l| l.tag_prefix.as_str())
            .unwrap_or("SATA");
        sanitize_tag(&format!("{}{}{}", prefix, kind, self.order_seq))
    }

    fn log_skip(&mut self, underlying: &str, now_ms: u64, msg: String) {
        let st = self.state.entry(underlying.to_string()).or_default();
        if now_ms.saturating_sub(st.last_skip_log_ms) >= 5 * 60_000 {
            warn!("{}", msg);
            st.last_skip_log_ms = now_ms;
        }
    }
}

fn best_bid_ask(tick: &crate::models::Tick) -> Option<(f64, f64)> {
    let d = tick.depth.as_ref()?;
    let bid = d.bids[0].price;
    let ask = d.asks[0].price;
    (bid.is_finite() && ask.is_finite() && bid > 0.0 && ask > 0.0 && ask >= bid).then_some((bid, ask))
}

fn strike_key(strike: f64) -> u64 {
    (strike * 100.0).round() as u64
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn ist_parts(ms: u64) -> (NaiveDate, Weekday, u32, u32) {
    let ist = FixedOffset::east_opt(5 * 3600 + 30 * 60).expect("valid IST offset");
    let dt = Utc
        .timestamp_millis_opt(ms as i64)
        .single()
        .unwrap_or_else(Utc::now)
        .with_timezone(&ist);
    let mins = dt.hour() * 60 + dt.minute();
    let secs = mins * 60 + dt.second();
    (dt.date_naive(), dt.weekday(), mins, secs)
}

fn side_word(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "BUY",
        OrderSide::Sell => "SELL",
    }
}

fn close_side(entry_side: OrderSide) -> OrderSide {
    match entry_side {
        OrderSide::Buy => OrderSide::Sell,
        OrderSide::Sell => OrderSide::Buy,
    }
}

/// Pick which already-filled legs to flatten when an entry leg fails mid-sequence.
///
/// Normally everything filled so far is flattened. But if the leg that failed is a SHORT
/// whose broker state we could NOT confirm (`reconciled == false`), that short may still be
/// resting live at the exchange — and selling its same-side long wing here is exactly what
/// would leave us **naked short** on that side. So in that one case we keep the same-side wing
/// ON as a hedge (the caller holds the lock for manual reconciliation). The short itself, if it
/// partially filled, is still included so its known fill gets bought back.
fn abort_flatten_set(
    completed: &[FilledLiveLeg],
    failed_side: OrderSide,
    failed_opt: OptionType,
    reconciled: bool,
) -> Vec<FilledLiveLeg> {
    let preserve_wing = !reconciled && failed_side == OrderSide::Sell;
    completed
        .iter()
        .filter(|f| {
            !(preserve_wing
                && f.leg.plan.opt == failed_opt
                && f.leg.plan.side == OrderSide::Buy
                && f.leg.plan.wing)
        })
        .cloned()
        .collect()
}

fn is_terminal_status(status_upper: &str) -> bool {
    matches!(status_upper, "REJECTED" | "CANCELLED" | "CANCELED" | "EXPIRED")
}

fn update_fill_progress(fill: &mut Option<LiveOrderFill>, upd: &OrderUpdate) {
    let Some(qty) = upd.filled_quantity else {
        return;
    };
    if qty == 0 {
        return;
    }
    let prev = fill.unwrap_or(LiveOrderFill {
        qty: 0,
        avg_price: 0.0,
    });
    if qty < prev.qty {
        return;
    }
    let avg_price = upd
        .average_price
        .filter(|p| *p > 0.0)
        .unwrap_or(prev.avg_price);
    *fill = Some(LiveOrderFill { qty, avg_price });
}

fn sanitize_tag(s: &str) -> String {
    s.chars().filter(|c| c.is_ascii_alphanumeric()).take(20).collect()
}

fn realized_pnl(
    legs: &[LiveLeg],
    exit_prices: &HashMap<(u64, OptionType), f64>,
    qty: u32,
    exit_ms: u64,
) -> f64 {
    let qty_f = qty as f64;
    let mut gross_unit = 0.0;
    let mut costs = 0.0;
    for leg in legs {
        let key = (strike_key(leg.plan.strike), leg.plan.opt);
        let exit_px = exit_prices.get(&key).copied().unwrap_or(leg.entry_px);
        gross_unit += match leg.plan.side {
            OrderSide::Buy => exit_px - leg.entry_px,
            OrderSide::Sell => leg.entry_px - exit_px,
        };
        costs += option_order_cost(leg.entry_px, qty, leg.plan.side, exit_ms);
        costs += option_order_cost(exit_px, qty, close_side(leg.plan.side), exit_ms);
    }
    gross_unit * qty_f - costs
}

fn option_order_cost(price: f64, qty: u32, side: OrderSide, ts_ms: u64) -> f64 {
    let prem = price.max(0.0) * qty as f64;
    let brokerage = 20.0;
    let exch = 0.000311 * prem;
    let sebi = 0.000001 * prem;
    let gst = 0.18 * (brokerage + exch + sebi);
    let stt = if side == OrderSide::Sell { options_sell_stt_rate(ts_ms) * prem } else { 0.0 };
    let stamp = if side == OrderSide::Buy { 0.00003 * prem } else { 0.0 };
    brokerage + exch + sebi + gst + stt + stamp
}

fn options_sell_stt_rate(ts_ms: u64) -> f64 {
    let ist = FixedOffset::east_opt(5 * 3600 + 30 * 60).expect("valid IST offset");
    let dt = Utc
        .timestamp_millis_opt(ts_ms as i64)
        .single()
        .unwrap_or_else(Utc::now)
        .with_timezone(&ist);
    let hike = NaiveDate::from_ymd_opt(2026, 4, 1).expect("valid STT hike date");
    if dt.date_naive() >= hike { 0.0015 } else { 0.0010 }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sq(strike: f64, cd: f64, pd: f64, cb: f64, ca: f64, pb: f64, pa: f64) -> StrikeQuote {
        StrikeQuote { strike, ce_delta: cd, pe_delta: pd, ce_bid: cb, ce_ask: ca, pe_bid: pb, pe_ask: pa }
    }

    /// 06-22-like NIFTY chain (spot ~24109), deltas/quotes from the real recorded data.
    fn chain() -> Vec<StrikeQuote> {
        vec![
            sq(23850.0, 0.86, -0.12, 280.0, 281.0, 12.0, 12.1),
            sq(23900.0, 0.82, -0.16, 250.0, 251.0, 18.15, 18.2),
            sq(23950.0, 0.78, -0.22, 220.0, 221.0, 26.4, 26.5),
            sq(24000.0, 0.72, -0.29, 150.3, 150.75, 38.2, 38.3),
            sq(24050.0, 0.63, -0.38, 116.35, 116.6, 54.1, 54.2),
            sq(24100.0, 0.53, -0.47, 87.1, 87.35, 75.05, 75.25), // ATM
            sq(24150.0, 0.43, -0.57, 62.7, 62.8, 100.7, 101.0),
            sq(24200.0, 0.33, -0.66, 43.25, 43.35, 131.75, 132.0),
            sq(24250.0, 0.25, -0.74, 29.0, 29.1, 175.0, 176.0),
            sq(24300.0, 0.18, -0.80, 18.85, 18.9, 220.0, 221.0),
            sq(24350.0, 0.12, -0.85, 12.05, 12.1, 270.0, 271.0),
        ]
    }

    #[test]
    fn day_gate_mon_tue_only() {
        assert!(weekday_allows(Weekday::Mon));
        assert!(weekday_allows(Weekday::Tue));
        for d in [Weekday::Wed, Weekday::Thu, Weekday::Fri, Weekday::Sat, Weekday::Sun] {
            assert!(!weekday_allows(d));
        }
    }

    #[test]
    fn condor_picks_quarter_delta_shorts_with_wings() {
        let legs = select_legs(&chain(), 24109.0, SellStructure::Condor).unwrap();
        assert_eq!(legs.len(), 4);
        let ce_s = legs.iter().find(|l| l.opt == OptionType::CE && l.side == OrderSide::Sell).unwrap();
        let pe_s = legs.iter().find(|l| l.opt == OptionType::PE && l.side == OrderSide::Sell).unwrap();
        assert_eq!(ce_s.strike, 24250.0); // ~0.25Δ call
        assert_eq!(pe_s.strike, 23950.0); // ~|0.25|Δ put (23900 has no wing room → excluded)
        assert!(legs.iter().any(|l| l.opt == OptionType::CE && l.side == OrderSide::Buy && (l.strike - 24350.0).abs() < 1e-6 && l.wing));
        assert!(legs.iter().any(|l| l.opt == OptionType::PE && l.side == OrderSide::Buy && (l.strike - 23850.0).abs() < 1e-6 && l.wing));
    }

    #[test]
    fn fly_shorts_at_the_money() {
        let legs = select_legs(&chain(), 24109.0, SellStructure::Fly).unwrap();
        let ce_s = legs.iter().find(|l| l.opt == OptionType::CE && l.side == OrderSide::Sell).unwrap();
        let pe_s = legs.iter().find(|l| l.opt == OptionType::PE && l.side == OrderSide::Sell).unwrap();
        assert_eq!(ce_s.strike, 24100.0); // nearest spot 24109
        assert_eq!(pe_s.strike, 24100.0);
    }

    #[test]
    fn wings_bought_before_shorts_sold() {
        let legs = select_legs(&chain(), 24109.0, SellStructure::Condor).unwrap();
        let seq = placement_sequence(&legs);
        let first_sell = seq.iter().position(|l| l.side == OrderSide::Sell).unwrap();
        let last_buy = seq.iter().rposition(|l| l.side == OrderSide::Buy).unwrap();
        assert!(last_buy < first_sell, "every wing (BUY) must precede every short (SELL)");
        assert!(seq[..first_sell].iter().all(|l| l.wing), "the legs placed first are the protective wings");
    }

    #[test]
    fn marketable_limit_crosses_so_no_leg_is_missed() {
        // BUY pays up over the ask; SELL gives up under the bid → both immediately marketable.
        assert!((marketable_limit(OrderSide::Buy, 12.0, 12.1) - 12.6).abs() < 1e-9);
        assert!((marketable_limit(OrderSide::Sell, 29.0, 29.1) - 28.5).abs() < 1e-9);
        assert_eq!(marketable_limit(OrderSide::Sell, 0.3, 0.4), 0.05); // tick floor
    }

    #[test]
    fn narrow_chain_without_wing_room_returns_none() {
        // Only 3 strikes: a 0.25Δ condor short can't seat a 100pt wing → no trade.
        let tiny = vec![
            sq(24050.0, 0.63, -0.38, 116.0, 116.5, 54.0, 54.2),
            sq(24100.0, 0.53, -0.47, 87.0, 87.4, 75.0, 75.3),
            sq(24150.0, 0.43, -0.57, 62.0, 62.8, 100.0, 101.0),
        ];
        assert!(select_legs(&tiny, 24100.0, SellStructure::Condor).is_none());
    }

    #[test]
    fn condor_collects_a_positive_credit() {
        let legs = select_legs(&chain(), 24109.0, SellStructure::Condor).unwrap();
        let credit = combo_credit(&legs, &chain()).unwrap();
        // 29.0(24250C bid) + 26.4(23950P bid) − 12.1(24350C ask) − 12.1(23850P ask) = 31.2
        assert!((credit - 31.2).abs() < 1e-6, "credit = {credit}");
        // closing immediately costs slightly MORE than the credit (you pay the spread)
        let close = combo_close_cost(&legs, &chain()).unwrap();
        assert!(close > credit, "entry close-cost {close} should exceed credit {credit}");
    }

    #[test]
    fn exit_rule_fires_on_tp_stop_and_time() {
        let credit = 31.2;
        let maxloss = WING - credit;
        assert_eq!(exit_reason(credit, 31.55, maxloss, 600), None);            // just opened, mid-session
        assert_eq!(exit_reason(credit, 15.0, maxloss, 600), Some(ExitReason::TakeProfit)); // gain 16.2 ≥ 15.6
        assert_eq!(exit_reason(credit, 66.0, maxloss, 600), Some(ExitReason::Stop));       // loss exceeds 50% ML
        assert_eq!(exit_reason(credit, 5.0, maxloss, 915), Some(ExitReason::Time));        // 15:15 overrides
    }

    #[test]
    fn sizing_respects_risk_budget_and_skips_when_unaffordable() {
        let mll = max_loss_per_lot(31.2, 65); // (100−31.2)×65 ≈ 4472
        assert!((mll - 4472.0).abs() < 1.0);
        assert_eq!(size_lots(150_000.0, mll, 0.10, 5), 3); // ₹15k budget / ₹4472 → 3 lots
        assert_eq!(size_lots(15_000.0, mll, 0.10, 5), 0);  // ₹1.5k budget < one lot → SKIP
        assert_eq!(size_lots(500_000.0, mll, 0.10, 5), 5); // capped at MAX_LOTS
    }

    #[test]
    fn fly_at_chain_edge_picks_nearest_strike_with_wing_room() {
        // strikes 24000..24300; spot 24290. Literal-nearest 24300 has no upper wing (24400
        // absent) → old code returned None. Now it must fall back to 24200 (wings 24300/24100).
        let edge: Vec<StrikeQuote> = [24000.0, 24050.0, 24100.0, 24150.0, 24200.0, 24250.0, 24300.0]
            .iter()
            .map(|&k| sq(k, 0.5, -0.5, 10.0, 10.2, 10.0, 10.2))
            .collect();
        let legs = select_legs(&edge, 24290.0, SellStructure::Fly).expect("a near-ATM fly must exist");
        let ce_s = legs.iter().find(|l| l.opt == OptionType::CE && l.side == OrderSide::Sell).unwrap();
        let pe_s = legs.iter().find(|l| l.opt == OptionType::PE && l.side == OrderSide::Sell).unwrap();
        assert_eq!(ce_s.strike, 24200.0);
        assert_eq!(pe_s.strike, 24200.0);
    }

    #[test]
    fn margin_preflight_requires_final_plus_buffer_to_fit() {
        // condor final margin ₹13,455 + 15% buffer = ₹15,473
        assert!(basket_margin_ok(13455.0, 16000.0, 0.15));
        assert!(!basket_margin_ok(13455.0, 15000.0, 0.15)); // funds short of margin+buffer
        assert!(!basket_margin_ok(0.0, 100000.0, 0.15));    // no/invalid margin → reject
        assert!(!basket_margin_ok(5000.0, 0.0, 0.15));      // no funds → reject
        assert!(!basket_margin_ok(5000.0, 100000.0, -0.5)); // negative buffer weakens check → reject
    }

    #[test]
    fn profit_zone_is_the_credit_adjusted_short_span() {
        let legs = select_legs(&chain(), 24109.0, SellStructure::Condor).unwrap();
        let credit = combo_credit(&legs, &chain()).unwrap(); // 31.2
        let (lo, hi, w) = profit_zone(&legs, credit).unwrap();
        assert!((lo - (23950.0 - credit)).abs() < 1e-6, "lower = short put − credit");
        assert!((hi - (24250.0 + credit)).abs() < 1e-6, "upper = short call + credit");
        assert!((w - (hi - lo)).abs() < 1e-6 && w > 0.0);
    }

    #[test]
    fn entry_drift_gate_rejects_when_open_range_eats_the_zone() {
        let zone = 362.0;
        assert!(entry_drift_admits(90.0, zone));   // 25% of zone → ok
        assert!(!entry_drift_admits(200.0, zone)); // 55% of zone → rejected (>50%)
        assert!(!entry_drift_admits(50.0, 0.0));   // no zone → reject
    }

    #[test]
    fn move_trim_fires_on_spot_pct_or_zone_fraction() {
        assert!(!move_trims(55.0, 24100.0, 362.0));   // 0.23% spot, 15% zone → hold
        assert!(move_trims(130.0, 24100.0, 362.0));   // 0.54% of spot → trim
        assert!(move_trims(130.0, 100_000.0, 300.0)); // 0.13% spot but 43% of zone → trim
    }

    #[test]
    fn admission_policy_gives_multileg_priority() {
        let flat = AdmissionState {
            position_open: false,
            single_traded_today: false,
            multileg_traded_today: false,
        };
        // flat morning, nothing traded yet → both may fire (equal edge at 09:45)
        assert!(may_enter(Strategy::MultiLeg, flat));
        assert!(may_enter(Strategy::SingleLeg, flat));

        // any position open → one-at-a-time, nothing else fires
        let busy = AdmissionState {
            position_open: true,
            single_traded_today: false,
            multileg_traded_today: false,
        };
        assert!(!may_enter(Strategy::MultiLeg, busy));
        assert!(!may_enter(Strategy::SingleLeg, busy));

        // multi-leg already traded today, now flat → single may still fire later.
        let after_multi = AdmissionState {
            position_open: false,
            single_traded_today: false,
            multileg_traded_today: true,
        };
        assert!(!may_enter(Strategy::MultiLeg, after_multi));
        assert!(may_enter(Strategy::SingleLeg, after_multi));

        let after_both = AdmissionState {
            position_open: false,
            single_traded_today: true,
            multileg_traded_today: true,
        };
        assert!(!may_enter(Strategy::MultiLeg, after_both));
        assert!(!may_enter(Strategy::SingleLeg, after_both));
    }

    fn test_engine_with_live_bridge(
        order_tx: mpsc::UnboundedSender<OrderCommand>,
        updates_rx: mpsc::UnboundedReceiver<OrderUpdate>,
    ) -> MultiLegEngine {
        let mut engine = MultiLegEngine::new(
            Vec::new(),
            TickStore::new(),
            HashMap::new(),
            0.0,
            0.0,
            100_000.0,
            crate::portfolio::new_shared(100_000.0, 15.0, 25.0, u32::MAX),
        );
        engine.set_live_order_bridge(
            order_tx,
            Some(updates_rx),
            "key".to_string(),
            "token".to_string(),
            "NFO".to_string(),
            "NRML".to_string(),
            "regular".to_string(),
            "T".to_string(),
        );
        engine
    }

    async fn wait_for_cancel(mut order_rx: mpsc::UnboundedReceiver<OrderCommand>, tag: &str) {
        for _ in 0..12 {
            let cmd = tokio::time::timeout(
                std::time::Duration::from_millis(250),
                order_rx.recv(),
            )
            .await
            .expect("wait_complete should send cancel/status commands")
            .expect("order command channel should stay open");
            if let OrderCommand::CancelByTag { tag: got } = cmd {
                assert_eq!(got, tag);
                return;
            }
        }
        panic!("wait_complete did not send CancelByTag for {}", tag);
    }

    fn order_update(tag: &str, status: &str, avg: f64, filled: u32) -> OrderUpdate {
        OrderUpdate {
            tag: tag.to_string(),
            order_id: Some("OID".to_string()),
            status: Some(status.to_string()),
            average_price: Some(avg),
            filled_quantity: Some(filled),
            pending_quantity: Some(0),
            source: "status_poll".to_string(),
            message: None,
        }
    }

    #[tokio::test]
    async fn wait_complete_timeout_cancels_and_confirms_no_fill() {
        let (order_tx, order_rx) = mpsc::unbounded_channel();
        let (updates_tx, updates_rx) = mpsc::unbounded_channel();
        let mut engine = test_engine_with_live_bridge(order_tx, updates_rx);

        let handle = tokio::spawn(async move { engine.wait_complete("TAGCXL", 50, 0).await });
        wait_for_cancel(order_rx, "TAGCXL").await;
        updates_tx
            .send(order_update("TAGCXL", "CANCELLED", 0.0, 0))
            .expect("test update receiver should be alive");

        let err = handle.await.expect("wait task should not panic").unwrap_err();
        assert!(err.reconciled, "broker terminal status makes this safe to release");
        assert_eq!(err.fill, None, "cancelled-without-fill must not be flattened");
    }

    #[tokio::test]
    async fn wait_complete_timeout_reports_fill_after_cancel_for_flattening() {
        let (order_tx, order_rx) = mpsc::unbounded_channel();
        let (updates_tx, updates_rx) = mpsc::unbounded_channel();
        let mut engine = test_engine_with_live_bridge(order_tx, updates_rx);

        let handle = tokio::spawn(async move { engine.wait_complete("TAGFILL", 50, 0).await });
        wait_for_cancel(order_rx, "TAGFILL").await;
        updates_tx
            .send(order_update("TAGFILL", "COMPLETE", 12.35, 50))
            .expect("test update receiver should be alive");

        let err = handle.await.expect("wait task should not panic").unwrap_err();
        assert!(err.reconciled);
        assert_eq!(
            err.fill,
            Some(LiveOrderFill {
                qty: 50,
                avg_price: 12.35
            })
        );
    }

    fn filled_leg(opt: OptionType, side: OrderSide, wing: bool) -> FilledLiveLeg {
        FilledLiveLeg {
            leg: LiveLeg {
                plan: PlannedLeg { strike: 100.0, opt, side, wing },
                market: LegMarket { token: 1, tradingsymbol: "X".to_string(), bid: 1.0, ask: 1.1 },
                entry_px: 1.0,
            },
            qty: 50,
        }
    }

    #[test]
    fn abort_preserves_same_side_wing_when_short_unconfirmed() {
        use OptionType::{CE, PE};
        use OrderSide::{Buy, Sell};
        // Wings + CE short already filled; the PE short is the leg that failed.
        let completed = vec![
            filled_leg(CE, Buy, true),   // CE wing
            filled_leg(PE, Buy, true),   // PE wing
            filled_leg(CE, Sell, false), // CE short (covered by CE wing)
        ];
        // PE short status UNCONFIRMED -> keep the PE wing on so that side stays defined-risk.
        let set = abort_flatten_set(&completed, Sell, PE, false);
        assert!(
            !set.iter().any(|f| f.leg.plan.opt == PE && f.leg.plan.wing),
            "the same-side PE wing must be preserved as a hedge"
        );
        assert!(set.iter().any(|f| f.leg.plan.opt == CE && f.leg.plan.wing), "CE wing still flattened");
        assert!(set.iter().any(|f| f.leg.plan.side == Sell), "the filled CE short still flattened");
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn abort_flattens_everything_when_short_status_confirmed() {
        use OptionType::PE;
        use OrderSide::{Buy, Sell};
        let completed = vec![filled_leg(crate::models::OptionType::CE, Buy, true), filled_leg(PE, Buy, true)];
        // reconciled=true (broker confirmed terminal) -> no naked risk, flatten all.
        let set = abort_flatten_set(&completed, Sell, PE, true);
        assert_eq!(set.len(), 2);
    }

    fn ist_ms(day: NaiveDate, hour: u32, minute: u32, second: u32) -> u64 {
        let ist = FixedOffset::east_opt(5 * 3600 + 30 * 60).unwrap();
        ist.with_ymd_and_hms(day.year(), day.month(), day.day(), hour, minute, second)
            .single()
            .unwrap()
            .with_timezone(&Utc)
            .timestamp_millis() as u64
    }

    #[test]
    fn opening_range_uses_091505_to_0945_window_only() {
        let day = NaiveDate::from_ymd_opt(2026, 6, 23).unwrap();
        let mut engine = MultiLegEngine::new(
            Vec::new(),
            TickStore::new(),
            HashMap::new(),
            0.0,
            0.0,
            100_000.0,
            crate::portfolio::new_shared(100_000.0, 15.0, 25.0, u32::MAX),
        );
        let st = engine.state.entry("NIFTY".to_string()).or_default();
        st.day = Some(day);
        st.spot_history.push_back((ist_ms(day, 9, 15, 4), 24_000.0));
        st.spot_history.push_back((ist_ms(day, 9, 15, 5), 24_100.0));
        st.spot_history.push_back((ist_ms(day, 9, 30, 0), 24_150.0));
        st.spot_history.push_back((ist_ms(day, 9, 45, 0), 24_120.0));
        st.spot_history.push_back((ist_ms(day, 9, 45, 1), 24_400.0));

        let range = engine.opening_range("NIFTY", day).unwrap();
        assert!(
            (range - 50.0).abs() < 1e-9,
            "range should exclude 09:15:04 and post-09:45 drift, got {range}"
        );
    }

    fn depth(bid: f64, ask: f64) -> crate::models::MarketDepth {
        let mut d = crate::models::MarketDepth::default();
        d.bids[0] = crate::models::DepthEntry {
            quantity: 1_000,
            price: bid,
            orders: 1,
        };
        d.asks[0] = crate::models::DepthEntry {
            quantity: 1_000,
            price: ask,
            orders: 1,
        };
        d
    }

    fn full_tick(token: u32, ltp: f64, bid: f64, ask: f64) -> crate::models::Tick {
        crate::models::Tick {
            token,
            ltp,
            depth: Some(depth(bid, ask)),
            mode: crate::models::TickMode::Full,
            ..crate::models::Tick::default()
        }
    }

    fn runtime_contracts_and_store() -> (Vec<OptionContract>, TickStore, HashMap<String, u32>) {
        let store = TickStore::new();
        let mut underlyings = HashMap::new();
        underlyings.insert("NIFTY".to_string(), 1);
        store.update(crate::models::Tick {
            token: 1,
            ltp: 24_109.0,
            mode: crate::models::TickMode::Ltp,
            ..crate::models::Tick::default()
        });

        let mut contracts = Vec::new();
        let mut token = 10_000;
        for q in chain() {
            for (opt, bid, ask) in [
                (OptionType::CE, q.ce_bid, q.ce_ask),
                (OptionType::PE, q.pe_bid, q.pe_ask),
            ] {
                token += 1;
                let tradingsymbol = format!(
                    "NIFTY26JUN{:.0}{}",
                    q.strike,
                    match opt {
                        OptionType::CE => "CE",
                        OptionType::PE => "PE",
                    }
                );
                contracts.push(OptionContract {
                    instrument_token: token,
                    tradingsymbol,
                    underlying: "NIFTY".to_string(),
                    expiry: "2026-06-23".to_string(),
                    strike: q.strike,
                    option_type: opt,
                    lot_size: 65,
                });
                store.update(full_tick(token, (bid + ask) / 2.0, bid, ask));
            }
        }
        (contracts, store, underlyings)
    }

    #[tokio::test]
    async fn runtime_opens_paper_condor_from_tickstore_snapshot_at_0945() {
        let day = NaiveDate::from_ymd_opt(2026, 6, 23).unwrap();
        let (contracts, store, underlyings) = runtime_contracts_and_store();
        let mut engine = MultiLegEngine::new(
            contracts,
            store.clone(),
            underlyings,
            0.065,
            0.0,
            150_000.0,
            crate::portfolio::new_shared(150_000.0, 15.0, 25.0, 1),
        );

        for (h, m, s, spot) in [
            (9, 15, 5, 24_090.0),
            (9, 30, 0, 24_125.0),
            (9, 45, 0, 24_109.0),
        ] {
            store.update(crate::models::Tick {
                token: 1,
                ltp: spot,
                mode: crate::models::TickMode::Ltp,
                ..crate::models::Tick::default()
            });
            engine.on_event_at(ist_ms(day, h, m, s)).await;
        }

        let active = engine
            .state
            .get("NIFTY")
            .and_then(|s| s.active.as_ref())
            .expect("paper runtime should open the multi-leg condor");
        assert_eq!(active.structure, SellStructure::Condor);
        assert_eq!(active.legs.len(), 4);
        assert!(crate::portfolio::is_locked(&engine.shared_circuit));
    }
}
