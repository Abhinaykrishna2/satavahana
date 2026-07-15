//! Multi-leg defined-risk option SELLING — slice 1: pure entry logic (no I/O, no live orders).
//!
//! Ports the satakarni sandbox's entry path into the Rust engine: near-expiry and far-DTE sideways gates,
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
use std::collections::{BTreeMap, HashMap, VecDeque};
use tokio::sync::{broadcast, mpsc, watch};
use tracing::{info, warn};

/// Wing width in points (NIFTY strikes are 50 apart; 100 = 2 strikes — the only width the
/// recorded ±5-strike chain can seat).
pub const WING: f64 = 100.0;
/// Marketable-limit cushion (₹). Each leg crosses the touch by this so it fills immediately
/// and we never get stuck holding 3 of 4 legs.
pub const ENTRY_SLIP: f64 = 0.50;

pub const CREDIT_EDGE_THRESHOLD: f64 = 0.65;
pub const CREDIT_EDGE_DELTA: f64 = 0.30;
pub const CREDIT_EDGE_MAX_ER: f64 = 0.50;
pub const CREDIT_EDGE_MAX_RANGE_PCT: f64 = 0.0200;
pub const CREDIT_EDGE_MAX_DTE_DAYS: i64 = 6;
pub const CREDIT_EDGE_TARGET_FRAC: f64 = 0.50;
pub const CREDIT_EDGE_STOP_FRAC_ML: f64 = 0.25;
pub const CREDIT_EDGE_HARD_STOP_FRAC_CAP: f64 = 0.15;
pub const CREDIT_EDGE_ALLOC_FRAC: f64 = 0.90;
pub const CREDIT_EDGE_LATE_EXIT_MIN: u32 = 14 * 60 + 30;
pub const CREDIT_EDGE_LATE_NET_PROFIT_PER_LOT: f64 = 112.50;
/// Live cap for CRED. The research lab used up to 2 lots, but the live engine also enforces a
/// 10%-of-capital hard stop; on the 13k account, 2 lots can hit that stop before the trained
/// credit-spread exit recovers. Keep live CRED to 1 lot unless account capital/margin is larger.
pub const CREDIT_EDGE_MAX_LOTS: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SellStructure {
    /// One-sided 100pt vertical credit spread selected from the 09:15-09:45 opening edge:
    /// close near high -> bull put credit, close near low -> bear call credit.
    CreditEdge,
    /// OTM strangle + 100pt wings (~0.25Δ shorts) — widest zone, low credit, high win-prob.
    Condor,
    /// Shorts nearer ATM (~0.33Δ) + 100pt wings — narrower zone, more credit (aggressive condor).
    Tight,
    /// ATM straddle + 100pt wings — narrow zone, big credit, high gamma.
    Fly,
    /// ATM straddle + 200pt wings — keeps more credit, larger max-loss (wider defined risk).
    WideFly,
}

impl SellStructure {
    /// (short |delta| target — None = ATM ; wing width in points). Mirrors satakarni `STRUCTURES`.
    fn params(self) -> (Option<f64>, f64) {
        match self {
            SellStructure::CreditEdge => (Some(CREDIT_EDGE_DELTA), WING),
            SellStructure::Condor => (Some(0.25), WING),
            SellStructure::Tight => (Some(0.33), WING),
            SellStructure::Fly => (None, WING),
            SellStructure::WideFly => (None, 2.0 * WING),
        }
    }

    /// Wing width (points) for this structure's defined-risk geometry. `max loss = wing − credit`.
    pub fn wing(self) -> f64 {
        self.params().1
    }

    /// The live structure ladder. CreditEdge is a separate 2-leg edge-open seller; it only seats
    /// when the 09:45 close is near an opening-range edge. The remaining structures keep the
    /// premium-selling ladder: Condor → Tight → Fly → WideFly.
    pub const LADDER: [SellStructure; 5] = [
        SellStructure::CreditEdge,
        SellStructure::Condor,
        SellStructure::Tight,
        SellStructure::Fly,
        SellStructure::WideFly,
    ];
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreditDirection {
    BullPut,
    BearCall,
}

pub fn credit_edge_direction(edge_pos: f64, threshold: f64) -> Option<CreditDirection> {
    if edge_pos >= threshold {
        Some(CreditDirection::BullPut)
    } else if edge_pos <= 1.0 - threshold {
        Some(CreditDirection::BearCall)
    } else {
        None
    }
}

pub fn credit_edge_er_admits(er: f64) -> bool {
    er.is_finite() && er <= CREDIT_EDGE_MAX_ER
}

/// Near-expiry selling can use the normal 09:45 open because intraday theta is fast enough to
/// justify the gamma. Farther-DTE selling is allowed only after a stronger sideways confirmation.
pub const MAX_SELL_DTE_DAYS: i64 = 1;
pub const FAR_DTE_ENTRY_MIN: u32 = 12 * 60;
pub const FAR_DTE_RECENT_MIN: u32 = 30;
pub const FAR_DTE_MAX_RECENT_ER: f64 = 0.45;
pub const FAR_DTE_MAX_SESSION_NET_PCT: f64 = 0.0035;
pub const FAR_DTE_MAX_SESSION_RANGE_PCT: f64 = 0.0060;
pub const FAR_DTE_MAX_EDGE_FRAC: f64 = 0.65;

pub fn dte_allows(days_to_expiry: i64) -> bool {
    (0..=MAX_SELL_DTE_DAYS).contains(&days_to_expiry)
}

pub fn expiry_dte_days(day: NaiveDate, expiry: &str) -> Option<i64> {
    let expiry = NaiveDate::parse_from_str(expiry, "%Y-%m-%d").ok()?;
    Some(expiry.signed_duration_since(day).num_days())
}

#[derive(Debug, Clone, Copy)]
pub struct FarDteSideways {
    pub session_er: f64,
    pub recent_er: f64,
    pub session_net_pct: f64,
    pub session_range_pct: f64,
    pub edge_frac: f64,
}

pub fn far_dte_sideways_metrics(
    session_closes: &[f64],
    recent_closes: &[f64],
    spot: f64,
) -> Option<FarDteSideways> {
    if spot <= 0.0 || !spot.is_finite() || session_closes.len() < 5 {
        return None;
    }
    let session_er = efficiency_ratio(session_closes)?;
    let recent_er = efficiency_ratio(recent_closes)?;
    let first = *session_closes.first()?;
    let last = *session_closes.last()?;
    let min = session_closes.iter().cloned().fold(f64::INFINITY, f64::min);
    let max = session_closes.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    let range = max - min;
    let range_pos = if range <= 0.0 { 0.5 } else { (last - min) / range };
    Some(FarDteSideways {
        session_er,
        recent_er,
        session_net_pct: (last - first).abs() / spot,
        session_range_pct: range / spot,
        edge_frac: (range_pos - 0.5).abs() * 2.0,
    })
}

pub fn far_dte_sideways_reject(m: FarDteSideways) -> Option<&'static str> {
    if m.recent_er > FAR_DTE_MAX_RECENT_ER {
        return Some("RECENT-TREND");
    }
    if m.session_net_pct > FAR_DTE_MAX_SESSION_NET_PCT {
        return Some("SESSION-DRIFT");
    }
    if m.session_range_pct > FAR_DTE_MAX_SESSION_RANGE_PCT {
        return Some("SESSION-RANGE");
    }
    if m.edge_frac > FAR_DTE_MAX_EDGE_FRAC {
        return Some("RANGE-EDGE");
    }
    None
}

fn has_strike(q: &[StrikeQuote], k: f64) -> bool {
    q.iter().any(|s| (s.strike - k).abs() < 1e-6)
}

/// Select the 4 defined-risk legs for `structure`. Returns None if the chain can't seat the
/// wings (e.g. the short would sit at the chain edge with no strike beyond it for the wing).
pub fn select_legs(quotes: &[StrikeQuote], spot: f64, structure: SellStructure) -> Option<Vec<PlannedLeg>> {
    if structure == SellStructure::CreditEdge {
        return None;
    }
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

pub fn select_credit_spread_legs(
    quotes: &[StrikeQuote],
    spot: f64,
    direction: CreditDirection,
    target_delta: f64,
    width: f64,
) -> Option<Vec<PlannedLeg>> {
    if quotes.is_empty() || !spot.is_finite() || width <= 0.0 {
        return None;
    }

    let opt = match direction {
        CreditDirection::BullPut => OptionType::PE,
        CreditDirection::BearCall => OptionType::CE,
    };
    let mut best: Option<(f64, f64)> = None;
    for q in quotes {
        let wing = match direction {
            CreditDirection::BullPut => q.strike - width,
            CreditDirection::BearCall => q.strike + width,
        };
        if !has_strike(quotes, wing) {
            continue;
        }
        let delta = match direction {
            CreditDirection::BullPut => q.pe_delta.abs(),
            CreditDirection::BearCall => q.ce_delta.abs(),
        };
        if !delta.is_finite() {
            continue;
        }
        let delta_score = (delta - target_delta).abs();
        let spot_score = (q.strike - spot).abs() / 20_000.0;
        let score = delta_score + spot_score;
        match best {
            Some((best_score, _)) if best_score <= score => {}
            _ => best = Some((score, q.strike)),
        }
    }

    let (_, short) = best?;
    let wing = match direction {
        CreditDirection::BullPut => short - width,
        CreditDirection::BearCall => short + width,
    };
    Some(vec![
        PlannedLeg {
            strike: wing,
            opt,
            side: OrderSide::Buy,
            wing: true,
        },
        PlannedLeg {
            strike: short,
            opt,
            side: OrderSide::Sell,
            wing: false,
        },
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
    /// Half-gain profit trail: gave back to 50% of the peak gain (a protective win-keeper, not a loss).
    Trail,
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

/// Defined max loss for one lot = (wing width − credit) × lot_size. `wing` is per-structure
/// (100pt for condor/tight/fly, 200pt for widefly).
pub fn max_loss_per_lot(credit: f64, wing: f64, lot_size: u32) -> f64 {
    (wing - credit) * lot_size as f64
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

/// Wider/lower-gamma structures tolerate more opening-range consumption (mirrors satakarni
/// `ENTRY_DRIFT_ZONE_CAP`).
pub fn entry_drift_zone_cap(structure: SellStructure) -> f64 {
    match structure {
        SellStructure::CreditEdge => 1.00,
        SellStructure::Condor => 0.50,
        SellStructure::Tight => 0.45,
        SellStructure::Fly => 0.45,
        SellStructure::WideFly => 0.42,
    }
}

/// At entry, spot should not sit too close to one edge of the opening range. Edge opens are
/// directional pressure, not chop. Narrow structures need a cleaner center (mirrors satakarni
/// `ENTRY_BALANCE_EDGE_CAP`).
pub fn entry_balance_edge_cap(structure: SellStructure) -> f64 {
    match structure {
        SellStructure::CreditEdge => 1.00,
        SellStructure::Condor => 0.70,
        SellStructure::Tight => 0.40,
        SellStructure::Fly => 0.35,
        SellStructure::WideFly => 0.40,
    }
}

/// In-trade trim when a MOVE_WINDOW_MIN move consumes more than this fraction of the zone (mirrors
/// satakarni `MOVE_ZONE_CAP`).
pub fn move_zone_cap(structure: SellStructure) -> f64 {
    match structure {
        SellStructure::CreditEdge => 1.00,
        SellStructure::Condor => 0.35,
        SellStructure::Tight => 0.30,
        SellStructure::Fly => 0.25,
        SellStructure::WideFly => 0.28,
    }
}
/// In-trade trim when the MOVE_WINDOW_MIN move exceeds this fraction of spot (short gamma can't
/// take a fast realized move).
pub const MOVE_PCT: f64 = 0.0025;
pub const MOVE_WINDOW_MIN: u32 = 2;
pub const TREND_ER: f64 = 0.50;
/// In-trade trend-exit threshold. DISABLED (99) — the A/B showed a rolling-ER trend-exit false-fires
/// on calm days and chops winners; the half-gain trail below is the validated profit protection.
/// Half-gain profit trail: arms once the running gain peaks past +15% of credit, then the stop locks
/// at 50% of the PEAK gain (ratchets up, never down) — a winner gives back at most half its best.
pub const TRAIL_TRIGGER: f64 = 0.15;
pub const TRAIL_FRAC: f64 = 0.50;

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
pub fn entry_drift_admits(opening_range_pts: f64, zone_width: f64, structure: SellStructure) -> bool {
    zone_width > 0.0 && opening_range_pts / zone_width <= entry_drift_zone_cap(structure)
}

pub fn entry_balance_admits(edge_frac: f64, structure: SellStructure) -> bool {
    edge_frac <= entry_balance_edge_cap(structure)
}

/// In-trade move trim: exit if the `MOVE_WINDOW_MIN`-minute move (`move_pts = |spot(t) −
/// spot(t−window)|`) exceeds `MOVE_PCT` of spot OR `MOVE_ZONE_CAP` of the profit zone.
pub fn move_trims(move_pts: f64, spot: f64, zone_width: f64, structure: SellStructure) -> bool {
    let by_spot = spot > 0.0 && move_pts / spot > MOVE_PCT;
    let by_zone = zone_width > 0.0 && move_pts / zone_width > move_zone_cap(structure);
    by_spot || by_zone
}

pub fn target_frac(structure: SellStructure) -> f64 {
    match structure {
        SellStructure::CreditEdge => CREDIT_EDGE_TARGET_FRAC,
        SellStructure::Condor | SellStructure::Tight | SellStructure::Fly | SellStructure::WideFly => 0.50,
    }
}

pub fn stop_frac_ml(structure: SellStructure) -> f64 {
    match structure {
        SellStructure::CreditEdge => CREDIT_EDGE_STOP_FRAC_ML,
        SellStructure::Condor | SellStructure::Tight | SellStructure::Fly | SellStructure::WideFly => STOP_FRAC_ML,
    }
}

pub fn hard_stop_frac_cap(structure: SellStructure) -> f64 {
    match structure {
        SellStructure::CreditEdge => CREDIT_EDGE_HARD_STOP_FRAC_CAP,
        SellStructure::Condor | SellStructure::Tight | SellStructure::Fly | SellStructure::WideFly => STOP_FRAC_CAP,
    }
}

pub fn exit_min_for(structure: SellStructure) -> u32 {
    match structure {
        SellStructure::CreditEdge => CREDIT_EDGE_EXIT_MIN,
        SellStructure::Condor | SellStructure::Tight | SellStructure::Fly | SellStructure::WideFly => EXIT_MIN,
    }
}

pub fn credit_edge_late_net_exit(
    structure: SellStructure,
    mins: u32,
    net_pnl: f64,
    lots: u32,
) -> bool {
    structure == SellStructure::CreditEdge
        && mins >= CREDIT_EDGE_LATE_EXIT_MIN
        && lots > 0
        && net_pnl >= CREDIT_EDGE_LATE_NET_PROFIT_PER_LOT * lots as f64
}

pub fn move_stop_enabled(structure: SellStructure) -> bool {
    structure != SellStructure::CreditEdge
}

pub fn trail_enabled(structure: SellStructure) -> bool {
    structure != SellStructure::CreditEdge
}

// ── Elite seller's regime gate (ported from satakarni; a-priori thresholds, NOT tuned) ──────
// The live runtime must match the sandbox: the open gate only rejects a clearly trending open.
// Range/straddle is logged as context, but no longer blocks entry; protection sits in the
// structure-specific drift/balance gates and the in-trade move/trend exits.
// NOTE: ER is sampling-frequency sensitive, so callers MUST feed 1-min closes (parity with the
// backtest's resample) — otherwise the same threshold would behave differently.

/// Kaufman Efficiency Ratio over the opening window's 1-min closes: `|net| / Σ|step|`. ~0 = chop
/// (oscillated, went nowhere → sell), ~1 = trend (marched one way → stand aside). None if too few
/// bars. Distinguishes "swung 50pt, ended flat" from "trended 50pt straight" — a range cannot.
pub fn efficiency_ratio(closes: &[f64]) -> Option<f64> {
    if closes.len() < 5 {
        return None;
    }
    let net = (closes[closes.len() - 1] - closes[0]).abs();
    let path: f64 = closes.windows(2).map(|w| (w[1] - w[0]).abs()).sum();
    Some(if path > 0.0 { net / path } else { 0.0 })
}

/// ATM straddle premium (CE mid + PE mid at the strike nearest spot) — the options' own priced
/// move, used as the vol-adaptive yardstick for "how big a range is too big".
pub fn atm_straddle(quotes: &[StrikeQuote], spot: f64) -> Option<f64> {
    let q = quotes.iter().min_by(|a, b| {
        (a.strike - spot)
            .abs()
            .partial_cmp(&(b.strike - spot).abs())
            .unwrap_or(Ordering::Equal)
    })?;
    let ce_mid = (q.ce_bid + q.ce_ask) / 2.0;
    let pe_mid = (q.pe_bid + q.pe_ask) / 2.0;
    (ce_mid > 0.0 && pe_mid > 0.0).then_some(ce_mid + pe_mid)
}

/// Regime decision: `Some(reason)` to stand aside, `None` to admit.
pub fn sell_regime_skip(er: f64, range_pts: f64, straddle: f64) -> Option<&'static str> {
    let _ = (range_pts, straddle);
    if er > TREND_ER {
        return Some("TRENDING (efficiency ratio)");
    }
    None
}

/// Opening-session indicators from the 09:15–09:45 window — the only past data available at the
/// 09:45 entry (no look-ahead). Used to pick the best multi-leg structure for *this* day.
#[derive(Debug, Clone, Copy)]
pub struct OpeningRegime {
    pub er: f64,
    pub range_pts: f64,
    pub straddle: f64,
    /// Where 09:45 spot sits in the opening range: 0 = low, 1 = high, 0.5 = centered.
    pub edge_frac: f64,
}

impl OpeningRegime {
    /// Morning range as a fraction of the ATM straddle — realized vs implied ("is vol running hot?").
    pub fn range_straddle(&self) -> f64 {
        if self.straddle > 0.0 {
            self.range_pts / self.straddle
        } else {
            f64::INFINITY
        }
    }

    /// Chop strength: 1 = pure oscillation, 0 = at the trend threshold.
    pub fn chop(&self) -> f64 {
        ((TREND_ER - self.er) / TREND_ER).clamp(0.0, 1.0)
    }

    /// How centered the open is: 1 = middle of range, 0 = at an edge.
    pub fn centered(&self) -> f64 {
        (1.0 - (self.edge_frac - 0.5).abs() * 2.0).clamp(0.0, 1.0)
    }
}

/// Score how well `structure` fits today's opening regime among structures that already cleared
/// their hard gates (drift, balance, margin). Higher = better match. Pure — unit-tested.
///
/// Weights are a-priori (textbook seller logic), not tuned to captured days:
///   • drift/balance *headroom* — how much cushion remains in the profit zone / range center;
///   • structure-specific regime affinity — narrow/high-credit (Fly/Tight) on calm+centered chop,
///     wide/low-gamma (Condor/WideFly) when realized is hot or spot sits off-center.
pub fn structure_regime_score(
    structure: SellStructure,
    regime: OpeningRegime,
    drift_frac: f64,
) -> f64 {
    let drift_cap = entry_drift_zone_cap(structure);
    let edge_cap = entry_balance_edge_cap(structure);
    let drift_headroom = ((drift_cap - drift_frac) / drift_cap).clamp(0.0, 1.0);
    let edge_headroom = ((edge_cap - regime.edge_frac) / edge_cap).clamp(0.0, 1.0);

    let chop = regime.chop();
    let centered = regime.centered();
    let rvs = regime.range_straddle();
    let calm = (1.0 - (rvs / 0.80).min(1.0)).clamp(0.0, 1.0);
    let hot = (rvs / 0.50).min(1.0);

    let affinity = match structure {
        // Directional edge-open credit spread: it wants spot near one side of the opening range,
        // unlike the centered neutral structures below. This score is only considered after the
        // explicit `CREDIT_EDGE_THRESHOLD` direction gate has passed.
        SellStructure::CreditEdge => {
            let edge = regime.edge_frac.clamp(0.0, 1.0);
            0.55 + edge * 0.30 + chop * 0.10 + calm * 0.05
        }
        // Calm, centered chop → ATM/near-ATM premium harvest.
        SellStructure::Fly => chop * 0.35 + centered * 0.35 + calm * 0.30,
        SellStructure::Tight => chop * 0.30 + centered * 0.30 + calm * 0.20 + drift_headroom * 0.20,
        // Wide, forgiving geometry → off-center opens and moderate drift consumption.
        SellStructure::Condor => {
            chop * 0.25 + drift_headroom * 0.30 + (1.0 - centered) * 0.20 + edge_headroom * 0.25
        }
        // Realized running hot vs implied → need the 200pt wings.
        SellStructure::WideFly => chop * 0.20 + hot * 0.35 + drift_headroom * 0.25 + edge_headroom * 0.20,
    };

    0.20 * drift_headroom + 0.15 * edge_headroom + 0.65 * affinity
}

/// Pick the structure with the highest regime score. Ties → safer structure wins (earlier in
/// `SellStructure::LADDER`: Condor first).
pub fn pick_best_structure(candidates: &[(SellStructure, f64)]) -> Option<SellStructure> {
    if candidates.is_empty() {
        return None;
    }
    let ladder_rank = |s: SellStructure| {
        SellStructure::LADDER
            .iter()
            .position(|&x| x == s)
            .unwrap_or(SellStructure::LADDER.len())
    };
    candidates
        .iter()
        .max_by(|a, b| {
            a.1.partial_cmp(&b.1)
                .unwrap_or(Ordering::Equal)
                .then_with(|| ladder_rank(b.0).cmp(&ladder_rank(a.0)))
        })
        .map(|(s, _)| *s)
}

/// Half-gain profit trail: once the peak running gain cleared `TRAIL_TRIGGER` of credit, exit when
/// the current gain has given back to `TRAIL_FRAC` of that peak (a winner keeps at least half its best).
pub fn trail_exits(gain: f64, peak_gain: f64, credit: f64) -> bool {
    credit > 0.0 && peak_gain >= TRAIL_TRIGGER * credit && gain <= TRAIL_FRAC * peak_gain
}

// ── Slice 3b: live/paper runtime harness ─────────────────────────────────────────────

const HOLDER: &str = "multileg";
const ENTRY_MIN: u32 = 9 * 60 + 45;
const CREDIT_EDGE_LATEST_ENTRY_MIN: u32 = ENTRY_MIN + 5;
pub const CREDIT_EDGE_EXIT_MIN: u32 = 14 * 60 + 45;
const CUTOFF_MIN: u32 = 14 * 60 + 30;
const EXIT_MIN: u32 = 14 * 60 + 55;
const OPEN_RANGE_START_SEC: u32 = 9 * 3600 + 15 * 60 + 5;
const OPEN_RANGE_END_SEC: u32 = ENTRY_MIN * 60;
/// Position sizing uses FULL margin affordability — the old 10% risk-fraction SIZE cap is dropped.
/// Downside is bounded by the active `STOP_FRAC_CAP` rupee stop instead of by refusing/shrinking the
/// trade. Mirrors the satakarni sandbox.
const MARGIN_SIZING_FRAC: f64 = 1.0;
/// Hard per-cycle rupee stop = 10% of CURRENT account capital, enforced regardless of lots. The
/// active-stop alternative to limiting risk via position size (mirrors satakarni `STOP_FRAC_CAP`).
const STOP_FRAC_CAP: f64 = 0.10;
/// Defined-risk stop: cut once running loss reaches this fraction of one-unit max loss.
pub const STOP_FRAC_ML: f64 = 0.15;
const MAX_LOTS: u32 = 5;
const SCAN_INTERVAL_SECS: u64 = 5;
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
struct MarginBlock {
    structure: SellStructure,
    lots: u32,
    final_margin: f64,
    funds: f64,
    buffer_frac: f64,
}

#[derive(Debug, Clone)]
enum MarginPreflightFailure {
    Transient(String),
    InsufficientFunds(MarginBlock),
}

impl MarginPreflightFailure {
    fn message(&self) -> String {
        match self {
            MarginPreflightFailure::Transient(msg) => msg.clone(),
            MarginPreflightFailure::InsufficientFunds(block) => format!(
                "{:?} x{}lot final margin ₹{:.0} + {:.0}% buffer > funds ₹{:.0}",
                block.structure,
                block.lots,
                block.final_margin,
                block.buffer_frac * 100.0,
                block.funds
            ),
        }
    }
}

#[derive(Debug, Clone)]
struct ChainSnapshot {
    spot: f64,
    expiry: String,
    quotes: Vec<StrikeQuote>,
    markets: HashMap<(u64, OptionType), LegMarket>,
    lot_size: u32,
}

#[derive(Debug, Clone, Copy)]
struct OpeningLatent {
    range_pts: f64,
    /// Position of the latest opening-window spot inside [low, high]: 0 = low, 1 = high.
    edge_pos: f64,
    edge_frac: f64,
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
    /// Best per-unit running gain seen so far — the ratchet for the half-gain trail.
    peak_gain: f64,
}

#[derive(Default)]
struct UnderlyingState {
    day: Option<NaiveDate>,
    spot_history: VecDeque<(u64, f64)>,
    active: Option<ActiveCombo>,
    traded_today: bool,
    margin_block: Option<MarginBlock>,
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

    pub fn spawn(self, rx: broadcast::Receiver<TickEvent>) -> tokio::task::JoinHandle<()> {
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        self.spawn_with_shutdown(rx, shutdown_rx)
    }

    pub fn spawn_with_shutdown(
        mut self,
        mut rx: broadcast::Receiver<TickEvent>,
        mut shutdown_rx: watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            info!(
                "Multi-leg engine started | structures=CRED-EDGE,CONDOR,TIGHT,FLY,WIDEFLY | holder={} | {} mode",
                HOLDER,
                if self.live.is_some() { "LIVE" } else { "paper" }
            );
            let mut flatten_clock = tokio::time::interval(std::time::Duration::from_secs(2));
            let mut shutdown_requested = false;
            let mut shutdown_deadline_ms = 0_u64;
            loop {
                tokio::select! {
                    event = rx.recv() => match event {
                        Ok(event) => self.on_event(&event).await,
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            warn!("Multi-leg engine lagged by {} messages", n);
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            if self.has_active_positions() {
                                warn!("Multi-leg tick stream closed with active position; attempting final flatten");
                                self.close_all_active(ExitReason::Time).await;
                            }
                            break;
                        }
                    },
                    _ = flatten_clock.tick() => {
                        let now = now_ms();
                        let (_day, _wd, mins, _secs) = ist_parts(now);
                        if self.has_time_exit_due(mins) {
                            self.close_all_active(ExitReason::Time).await;
                        }
                        if shutdown_requested {
                            if !self.has_active_positions() {
                                break;
                            }
                            if shutdown_deadline_ms > 0 && now >= shutdown_deadline_ms {
                                warn!("Multi-leg shutdown flatten window elapsed; active position still tracked");
                                break;
                            }
                            self.close_all_active(ExitReason::Time).await;
                        }
                    },
                    res = shutdown_rx.changed() => {
                        if res.is_err() || *shutdown_rx.borrow() {
                            shutdown_requested = true;
                            shutdown_deadline_ms = now_ms().saturating_add(30_000);
                            warn!("Multi-leg shutdown requested; flattening active combos before exit");
                            self.close_all_active(ExitReason::Time).await;
                            if !self.has_active_positions() {
                                break;
                            }
                        }
                    }
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
        _wd: Weekday,
        mins: u32,
    ) {
        if self.state.get(underlying).and_then(|s| s.active.as_ref()).is_some() {
            self.manage_active(underlying, now_ms, mins).await;
            return;
        }

        if mins < ENTRY_MIN || mins > CUTOFF_MIN {
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
        if let Some(block) = self
            .state
            .get(underlying)
            .and_then(|s| s.margin_block.clone())
        {
            self.log_skip(
                underlying,
                now_ms,
                format!(
                    "multi-leg margin blocked for today: {:?} x{}lot final ₹{:.0} + {:.0}% buffer > funds ₹{:.0}",
                    block.structure,
                    block.lots,
                    block.final_margin,
                    block.buffer_frac * 100.0,
                    block.funds
                ),
            );
            return;
        }

        let Some(snapshot) = self.build_snapshot(underlying, now_ms) else {
            return;
        };
        let dte = match expiry_dte_days(day, &snapshot.expiry) {
            Some(dte) if dte >= 0 => dte,
            Some(dte) => {
                self.log_skip(
                    underlying,
                    now_ms,
                    format!("multi-leg stand aside: expiry {} is expired ({}DTE)", snapshot.expiry, dte),
                );
                return;
            }
            None => {
                self.log_skip(
                    underlying,
                    now_ms,
                    format!("multi-leg stand aside: invalid expiry {}", snapshot.expiry),
                );
                return;
            }
        };
        let near_expiry = dte_allows(dte);
        let credit_edge_window = mins <= CREDIT_EDGE_LATEST_ENTRY_MIN && dte <= CREDIT_EDGE_MAX_DTE_DAYS;
        let mut standard_window = near_expiry || mins >= FAR_DTE_ENTRY_MIN;
        if !credit_edge_window && !standard_window {
            return;
        }
        let standard_latent_end_ms = if near_expiry { None } else { Some(now_ms) };
        // Elite seller's regime gate (ported from satakarni): block only a clearly trending open.
        // Range/straddle is logged for context, not used as an entry blocker in the current sandbox.
        // Insufficient opening data → stand aside (can't confirm a calm open, so don't sell into it).
        if !near_expiry && standard_window {
            let closes = self.entry_minute_closes(underlying, day, standard_latent_end_ms);
            let recent = self.recent_minute_closes(underlying, day, now_ms, FAR_DTE_RECENT_MIN);
            match far_dte_sideways_metrics(&closes, &recent, snapshot.spot) {
                Some(metrics) => {
                    if let Some(why) = far_dte_sideways_reject(metrics) {
                        self.log_skip(
                            underlying,
                            now_ms,
                            format!(
                                "multi-leg far-DTE stand aside: {} (session ER {:.2}, recent ER {:.2}, drift {:.2}%, range {:.2}%, edge {:.0}%)",
                                why,
                                metrics.session_er,
                                metrics.recent_er,
                                metrics.session_net_pct * 100.0,
                                metrics.session_range_pct * 100.0,
                                metrics.edge_frac * 100.0
                            ),
                        );
                        standard_window = false;
                    }
                }
                None => {
                    standard_window = false;
                    if !credit_edge_window {
                        return;
                    }
                }
            }
        }
        let regime_latent_end_ms = if standard_window {
            standard_latent_end_ms
        } else {
            None
        };
        let closes = self.entry_minute_closes(underlying, day, regime_latent_end_ms);
        let regime = match (
            efficiency_ratio(&closes),
            atm_straddle(&snapshot.quotes, snapshot.spot),
        ) {
            (Some(er), Some(straddle)) => {
                let max = closes.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
                let min = closes.iter().cloned().fold(f64::INFINITY, f64::min);
                let range_pts = max - min;
                let standard_regime_skip = sell_regime_skip(er, range_pts, straddle);
                if let Some(why) = standard_regime_skip {
                    self.log_skip(
                        underlying,
                        now_ms,
                        format!(
                            "multi-leg neutral stand aside: {} (ER {:.2}, range {:.0}pt = {:.0}% of straddle {:.0}); CRED edge may still qualify",
                            why,
                            er,
                            range_pts,
                            range_pts / straddle * 100.0,
                            straddle
                        ),
                    );
                }
                let edge_frac = self
                    .opening_latent(underlying, day, regime_latent_end_ms)
                    .map(|l| l.edge_frac)
                    .unwrap_or(0.5);
                OpeningRegime {
                    er,
                    range_pts,
                    straddle,
                    edge_frac,
                }
            }
            _ => return,
        };
        let standard_regime_skip = sell_regime_skip(regime.er, regime.range_pts, regime.straddle);
        let Some((active, _drift_frac)) = self.plan_entry(
            underlying,
            now_ms,
            day,
            mins,
            &snapshot,
            regime,
            standard_latent_end_ms,
            credit_edge_window,
            standard_window,
            standard_regime_skip,
        )
        else {
            return;
        };
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

    /// Indicator-driven structure pick: evaluate every structure against the opening regime,
    /// keep those that clear hard gates (drift, balance, margin), then open the highest
    /// `structure_regime_score`. One multi-leg trade/day is enforced by `traded_today`;
    /// the shared circuit also enforces the account-wide completed-trade cap.
    fn plan_entry(
        &mut self,
        underlying: &str,
        now_ms: u64,
        day: NaiveDate,
        mins: u32,
        snapshot: &ChainSnapshot,
        regime: OpeningRegime,
        standard_latent_end_ms: Option<u64>,
        credit_edge_window: bool,
        standard_window: bool,
        standard_regime_skip: Option<&str>,
    ) -> Option<(ActiveCombo, f64)> {
        let credit_latent = self.opening_latent(underlying, day, None);
        let standard_latent = self.opening_latent(underlying, day, standard_latent_end_ms);
        let mut last_err = String::new();
        let mut scored: Vec<(SellStructure, f64, ActiveCombo)> = Vec::new();
        let mut scoreboard: Vec<(SellStructure, f64)> = Vec::new();

        for structure in SellStructure::LADDER {
            let opening_latent = if structure == SellStructure::CreditEdge {
                if !credit_edge_window {
                    continue;
                }
                if !credit_edge_er_admits(regime.er) {
                    last_err = format!(
                        "CreditEdge CRED ER {:.2} > {:.2}",
                        regime.er, CREDIT_EDGE_MAX_ER
                    );
                    continue;
                }
                match credit_latent {
                    Some(v) => v,
                    None => {
                        last_err = "CreditEdge insufficient 09:45 edge data".to_string();
                        continue;
                    }
                }
            } else {
                if !standard_window {
                    continue;
                }
                if let Some(why) = standard_regime_skip {
                    last_err = format!("{:?} neutral-regime {}", structure, why);
                    continue;
                }
                match standard_latent {
                    Some(v) => v,
                    None => {
                        last_err = format!("{:?} insufficient opening latent", structure);
                        continue;
                    }
                }
            };
            match self.plan_structure(structure, underlying, snapshot, opening_latent, mins) {
                Ok((combo, drift_frac)) => {
                    let score = structure_regime_score(structure, regime, drift_frac);
                    scoreboard.push((structure, score));
                    scored.push((structure, drift_frac, combo));
                }
                Err(e) => last_err = format!("{:?} {}", structure, e),
            }
        }

        if scoreboard.is_empty() {
            self.log_skip(
                underlying,
                now_ms,
                format!("multi-leg skipped: no structure passed gates ({})", last_err),
            );
            return None;
        }

        let best = pick_best_structure(&scoreboard)?;
        let (drift_frac, active) = scored
            .into_iter()
            .find(|(s, _, _)| *s == best)
            .map(|(_, d, c)| (d, c))?;

        let fmt = |s: SellStructure| -> String {
            scoreboard
                .iter()
                .find(|(st, _)| *st == s)
                .map(|(_, sc)| format!("{:.2}", sc))
                .unwrap_or_else(|| "-".to_string())
        };
        info!(
            "MULTILEG regime pick {:?} score {:.2} | ER {:.2} RVS {:.0}% centered {:.0}% | \
             CRED={} Condor={} Tight={} Fly={} WideFly={}",
            best,
            scoreboard.iter().find(|(s, _)| *s == best).map(|(_, sc)| *sc).unwrap_or(0.0),
            regime.er,
            regime.range_straddle() * 100.0,
            regime.centered() * 100.0,
            fmt(SellStructure::CreditEdge),
            fmt(SellStructure::Condor),
            fmt(SellStructure::Tight),
            fmt(SellStructure::Fly),
            fmt(SellStructure::WideFly),
        );

        Some((active, drift_frac))
    }

    /// Plan one structure (no logging, no mutation): `Ok` if it clears the drift gate and funds a
    /// lot, else `Err(reason)`.
    fn plan_structure(
        &self,
        structure: SellStructure,
        underlying: &str,
        snapshot: &ChainSnapshot,
        opening_latent: OpeningLatent,
        mins: u32,
    ) -> Result<(ActiveCombo, f64), String> {
        let legs = if structure == SellStructure::CreditEdge {
            if mins > CREDIT_EDGE_LATEST_ENTRY_MIN {
                return Err("CRED stale entry window".to_string());
            }
            let range_pct = opening_latent.range_pts / snapshot.spot;
            if range_pct > CREDIT_EDGE_MAX_RANGE_PCT {
                return Err(format!(
                    "CRED range {:.2}% > {:.2}%",
                    range_pct * 100.0,
                    CREDIT_EDGE_MAX_RANGE_PCT * 100.0
                ));
            }
            let direction = credit_edge_direction(opening_latent.edge_pos, CREDIT_EDGE_THRESHOLD)
                .ok_or("CRED opening not near edge")?;
            select_credit_spread_legs(
                &snapshot.quotes,
                snapshot.spot,
                direction,
                CREDIT_EDGE_DELTA,
                structure.wing(),
            )
            .ok_or("CRED legs not seatable")?
        } else {
            select_legs(&snapshot.quotes, snapshot.spot, structure)
                .ok_or("legs not seatable")?
        };
        let credit = combo_credit(&legs, &snapshot.quotes).ok_or("no credit")?;
        let max_loss_unit = structure.wing() - credit;
        if credit <= 0.0 || max_loss_unit <= 0.0 {
            return Err("non-positive credit/max-loss".to_string());
        }
        let zone_width = if structure == SellStructure::CreditEdge {
            structure.wing()
        } else {
            let (_zone_lo, _zone_hi, zone_width) = profit_zone(&legs, credit).ok_or("no zone")?;
            zone_width
        };
        let drift_frac = opening_latent.range_pts / zone_width;
        if structure != SellStructure::CreditEdge {
            let drift_cap = entry_drift_zone_cap(structure);
            if !entry_drift_admits(opening_latent.range_pts, zone_width, structure) {
                return Err(format!(
                    "DRIFT-ZONE {:.0}% > {:.0}%",
                    drift_frac * 100.0,
                    drift_cap * 100.0
                ));
            }
            let edge_cap = entry_balance_edge_cap(structure);
            if !entry_balance_admits(opening_latent.edge_frac, structure) {
                return Err(format!(
                    "RANGE-BALANCE edge {:.0}% > {:.0}%",
                    opening_latent.edge_frac * 100.0,
                    edge_cap * 100.0
                ));
            }
        }
        let max_loss_lot = max_loss_unit * snapshot.lot_size as f64;
        let sizing_frac = if structure == SellStructure::CreditEdge {
            CREDIT_EDGE_ALLOC_FRAC
        } else {
            MARGIN_SIZING_FRAC
        };
        let max_lots = if structure == SellStructure::CreditEdge {
            CREDIT_EDGE_MAX_LOTS
        } else {
            MAX_LOTS
        };
        let lots = size_lots(self.capital, max_loss_lot, sizing_frac, max_lots);
        if lots == 0 {
            return Err("margin cannot fund one lot".to_string());
        }

        let mut live_legs = Vec::with_capacity(legs.len());
        for leg in legs {
            let market = snapshot
                .markets
                .get(&(strike_key(leg.strike), leg.opt))
                .ok_or("missing leg market")?
                .clone();
            let entry_px = entry_fill(&leg, quote_at(&snapshot.quotes, leg.strike).ok_or("missing quote")?);
            live_legs.push(LiveLeg { plan: leg, market, entry_px });
        }

        Ok((
            ActiveCombo {
                underlying: underlying.to_string(),
                structure,
                legs: live_legs,
                lots,
                lot_size: snapshot.lot_size,
                credit,
                max_loss_unit,
                zone_width,
                peak_gain: 0.0,
            },
            drift_frac,
        ))
    }

    #[cfg(test)]
    fn opening_range(&self, underlying: &str, day: NaiveDate) -> Option<f64> {
        self.opening_latent(underlying, day, None).map(|l| l.range_pts)
    }

    fn opening_latent(&self, underlying: &str, day: NaiveDate, end_ms: Option<u64>) -> Option<OpeningLatent> {
        let st = self.state.get(underlying)?;
        if st.day != Some(day) {
            return None;
        }
        let end_secs = end_ms
            .map(|end| ist_parts(end).3)
            .unwrap_or(OPEN_RANGE_END_SEC);
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
                    && secs <= end_secs
                    && end_ms.map(|end| *t <= end).unwrap_or(true))
                .then_some(*s)
            })
            .collect();
        if vals.len() < 2 {
            return None;
        }
        let min = vals.iter().cloned().fold(f64::INFINITY, f64::min);
        let max = vals.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let range_pts = max - min;
        let last = *vals.last()?;
        let range_pos = if range_pts <= 0.0 {
            0.5
        } else {
            (last - min) / range_pts
        };
        let edge_pos = range_pos.clamp(0.0, 1.0);
        let edge_frac = (edge_pos - 0.5).abs() * 2.0;
        Some(OpeningLatent {
            range_pts,
            edge_pos,
            edge_frac,
        })
    }

    /// 1-min spot closes over the entry window [09:15:05, end], chronological. Buckets the
    /// per-tick spot history by minute (last sample wins) to match the backtest's resample.
    fn entry_minute_closes(&self, underlying: &str, day: NaiveDate, end_ms: Option<u64>) -> Vec<f64> {
        let Some(st) = self.state.get(underlying) else {
            return Vec::new();
        };
        if st.day != Some(day) {
            return Vec::new();
        }
        let end_secs = end_ms
            .map(|end| ist_parts(end).3)
            .unwrap_or(OPEN_RANGE_END_SEC);
        let mut by_min: BTreeMap<u64, f64> = BTreeMap::new();
        for (t, s) in &st.spot_history {
            if !s.is_finite() || *s <= 0.0 {
                continue;
            }
            let (sample_day, _wd, _mins, secs) = ist_parts(*t);
            if sample_day == day
                && secs >= OPEN_RANGE_START_SEC
                && secs <= end_secs
                && end_ms.map(|end| *t <= end).unwrap_or(true)
            {
                by_min.insert(*t / 60_000, *s);
            }
        }
        by_min.into_values().collect()
    }

    fn recent_minute_closes(&self, underlying: &str, day: NaiveDate, now_ms: u64, minutes: u32) -> Vec<f64> {
        let Some(st) = self.state.get(underlying) else {
            return Vec::new();
        };
        if st.day != Some(day) {
            return Vec::new();
        }
        let start_ms = now_ms.saturating_sub(minutes as u64 * 60_000);
        let mut by_min: BTreeMap<u64, f64> = BTreeMap::new();
        for (t, s) in &st.spot_history {
            if !s.is_finite() || *s <= 0.0 {
                continue;
            }
            let (sample_day, _wd, _mins, secs) = ist_parts(*t);
            if sample_day == day && secs >= OPEN_RANGE_START_SEC && *t >= start_ms && *t <= now_ms {
                by_min.insert(*t / 60_000, *s);
            }
        }
        by_min.into_values().collect()
    }

    async fn open_combo(&mut self, mut active: ActiveCombo, snapshot: ChainSnapshot) {
        if !crate::portfolio::try_claim(&self.shared_circuit, HOLDER) {
            return;
        }
        if self.live.is_some() {
            if let Err(failure) = self.preflight_live_margin(&mut active, &snapshot).await {
                if let MarginPreflightFailure::InsufficientFunds(block) = &failure {
                    let key = active.underlying.clone();
                    self.state.entry(key).or_default().margin_block = Some(block.clone());
                    warn!(
                        "MULTILEG margin preflight latched for today: {}",
                        failure.message()
                    );
                } else {
                    warn!("MULTILEG margin preflight skipped entry: {}", failure.message());
                }
                crate::portfolio::release(&self.shared_circuit, HOLDER);
                return;
            }
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

    async fn preflight_live_margin(
        &self,
        active: &mut ActiveCombo,
        snapshot: &ChainSnapshot,
    ) -> Result<(), MarginPreflightFailure> {
        let Some(live) = &self.live else { return Ok(()); };
        let funds = match fetch_live_available_funds(&live.api_key, &live.access_token).await {
            Ok(v) => v,
            Err(e) => {
                return Err(MarginPreflightFailure::Transient(format!(
                    "live funds unavailable: {}",
                    e
                )));
            }
        };

        let planned_lots = active.lots.max(1);
        let mut last_block: Option<MarginBlock> = None;
        for lots in (1..=planned_lots).rev() {
            let qty = lots.saturating_mul(active.lot_size);
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
            let final_margin = match fetch_basket_final_margin(&live.api_key, &live.access_token, &orders).await {
                Ok(v) => v,
                Err(e) => {
                    return Err(MarginPreflightFailure::Transient(format!(
                        "basket margin unavailable: {}",
                        e
                    )));
                }
            };
            if basket_margin_ok(final_margin, funds, MARGIN_BUFFER_FRAC) {
                if lots != active.lots {
                    warn!(
                        "MULTILEG margin preflight resized {} {:?}: {}lot -> {}lot to fit funds ₹{:.0}",
                        active.underlying, active.structure, active.lots, lots, funds
                    );
                    active.lots = lots;
                }
                info!(
                    "MULTILEG margin preflight OK {} {:?}: final ₹{:.0}, funds ₹{:.0}, spot {:.0}, lots {}",
                    active.underlying, active.structure, final_margin, funds, snapshot.spot, active.lots
                );
                return Ok(());
            }
            last_block = Some(MarginBlock {
                structure: active.structure,
                lots,
                final_margin,
                funds,
                buffer_frac: MARGIN_BUFFER_FRAC,
            });
            warn!(
                "MULTILEG margin preflight rejected {} {:?} x{}lot: final margin ₹{:.0} + {:.0}% buffer > funds ₹{:.0}",
                active.underlying,
                active.structure,
                lots,
                final_margin,
                MARGIN_BUFFER_FRAC * 100.0,
                funds
            );
        }
        Err(MarginPreflightFailure::InsufficientFunds(
            last_block.unwrap_or(MarginBlock {
                structure: active.structure,
                lots: active.lots.max(1),
                final_margin: 0.0,
                funds,
                buffer_frac: MARGIN_BUFFER_FRAC,
            }),
        ))
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
        if move_stop_enabled(active.structure) {
            if let Some(move_pts) = self.recent_move_pts(underlying, now_ms) {
                if move_trims(move_pts, snapshot.spot, active.zone_width, active.structure) {
                    reason = Some(ExitReason::Stop);
                }
            }
        }
        if reason.is_none() {
            let gain = active.credit - close_cost;
            // Ratchet the peak gain and persist it on the active position (the trail floor rises,
            // never falls) so the half-gain trail survives across manage cycles.
            let peak_gain = active.peak_gain.max(gain);
            if let Some(st) = self.state.get_mut(underlying) {
                if let Some(a) = st.active.as_mut() {
                    a.peak_gain = peak_gain;
                }
            }
            // NET (post-cost) P&L the exit would realize right now — same formula as the real close
            // (realized_pnl), so the hard stop books a loss at 10% of capital, not 10%+costs+slippage.
            // Backtest manage() mirrors this via pnl().
            let qty = active.lots.saturating_mul(active.lot_size);
            let cur_exit_prices = self.exit_prices(&active, &snapshot);
            let net_now = realized_pnl(&active.legs, &cur_exit_prices, qty, now_ms);
            if mins >= exit_min_for(active.structure) {
                reason = Some(ExitReason::Time);
            } else if active.credit > 0.0 && gain >= target_frac(active.structure) * active.credit {
                reason = Some(ExitReason::TakeProfit);
            } else if credit_edge_late_net_exit(active.structure, mins, net_now, active.lots) {
                reason = Some(ExitReason::TakeProfit);
            } else if trail_enabled(active.structure) && trail_exits(gain, peak_gain, active.credit) {
                // Half-gain profit trail: a winner that peaked past +15% gives back at most half.
                reason = Some(ExitReason::Trail);
            } else if net_now <= -hard_stop_frac_cap(active.structure) * self.capital {
                // Hard rupee stop on realized net: cut once the post-cost loss hits the
                // structure-specific account cap, regardless of lots.
                reason = Some(ExitReason::Stop);
            } else if active.max_loss_unit > 0.0
                && gain <= -stop_frac_ml(active.structure) * active.max_loss_unit
            {
                reason = Some(ExitReason::Stop);
            }
        }
        if mins >= exit_min_for(active.structure) {
            reason = Some(ExitReason::Time);
        }
        if let Some(exit) = reason {
            self.close_active(active, snapshot, exit).await;
        }
    }

    fn has_active_positions(&self) -> bool {
        self.state.values().any(|st| st.active.is_some())
    }

    fn has_time_exit_due(&self, mins: u32) -> bool {
        self.state
            .values()
            .filter_map(|st| st.active.as_ref())
            .any(|active| mins >= exit_min_for(active.structure))
    }

    async fn close_all_active(&mut self, reason: ExitReason) {
        let active: Vec<ActiveCombo> = self
            .state
            .values()
            .filter_map(|st| st.active.clone())
            .collect();
        for combo in active {
            let Some(snapshot) = self.build_snapshot(&combo.underlying, now_ms()) else {
                warn!(
                    "MULTILEG {:?} CLOSE skipped for {} {:?}: no current chain snapshot",
                    reason, combo.underlying, combo.structure
                );
                continue;
            };
            self.close_active(combo, snapshot, reason).await;
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
        // Close order is the mirror of entry: buy back every SHORT first, THEN sell the wings, so a
        // short is never momentarily left naked. Stable sort preserves intra-group order.
        let mut seq: Vec<FilledLiveLeg> = fills.iter().filter(|f| f.qty > 0).cloned().collect();
        seq.sort_by_key(|f| close_order_rank(f.leg.plan.side));
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

/// Close-ordering rank: a SHORT (entry SELL) is bought back FIRST (rank 0); a LONG wing (entry BUY)
/// is sold AFTER (rank 1). Selling a wing before its short is closed would momentarily un-hedge the
/// short into naked, full-margin/unlimited risk — the exact mirror of `placement_sequence` (wings
/// bought first on entry). A *stable* sort on this rank preserves intra-group order.
fn close_order_rank(entry_side: OrderSide) -> u8 {
    match entry_side {
        OrderSide::Sell => 0,
        OrderSide::Buy => 1,
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

pub(crate) fn option_order_cost(price: f64, qty: u32, side: OrderSide, ts_ms: u64) -> f64 {
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
    fn expiry_dte_gate_allows_only_zero_or_one_dte() {
        let mon = NaiveDate::from_ymd_opt(2026, 6, 22).unwrap();
        let tue = NaiveDate::from_ymd_opt(2026, 6, 23).unwrap();

        assert_eq!(expiry_dte_days(mon, "2026-06-23"), Some(1));
        assert_eq!(expiry_dte_days(tue, "2026-06-23"), Some(0));
        assert_eq!(expiry_dte_days(mon, "not-a-date"), None);

        assert!(dte_allows(0));
        assert!(dte_allows(1));
        assert!(!dte_allows(2));
        assert!(!dte_allows(-1));
    }

    #[test]
    fn far_dte_sideways_gate_admits_clean_range_and_rejects_recent_trend() {
        let sideways = [24000.0, 24020.0, 24005.0, 24025.0, 24010.0, 24030.0, 24015.0, 24020.0];
        let recent_chop = [24015.0, 24025.0, 24018.0, 24028.0, 24020.0];
        let m = far_dte_sideways_metrics(&sideways, &recent_chop, 24020.0).unwrap();
        assert!(far_dte_sideways_reject(m).is_none(), "clean far-DTE range should admit: {m:?}");

        let recent_trend = [24020.0, 24030.0, 24040.0, 24050.0, 24060.0];
        let m = far_dte_sideways_metrics(&sideways, &recent_trend, 24060.0).unwrap();
        assert_eq!(far_dte_sideways_reject(m), Some("RECENT-TREND"));
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
    fn credit_edge_selects_one_sided_vertical_from_opening_edge() {
        assert_eq!(
            credit_edge_direction(0.70, CREDIT_EDGE_THRESHOLD),
            Some(CreditDirection::BullPut)
        );
        assert_eq!(
            credit_edge_direction(0.30, CREDIT_EDGE_THRESHOLD),
            Some(CreditDirection::BearCall)
        );
        assert_eq!(credit_edge_direction(0.50, CREDIT_EDGE_THRESHOLD), None);

        let bull_put = select_credit_spread_legs(
            &chain(),
            24109.0,
            CreditDirection::BullPut,
            CREDIT_EDGE_DELTA,
            WING,
        )
        .expect("bull-put credit spread must seat");
        assert_eq!(bull_put.len(), 2);
        assert!(bull_put.iter().any(|l| {
            l.opt == OptionType::PE && l.side == OrderSide::Sell && (l.strike - 24000.0).abs() < 1e-6
        }));
        assert!(bull_put.iter().any(|l| {
            l.opt == OptionType::PE && l.side == OrderSide::Buy && l.wing && (l.strike - 23900.0).abs() < 1e-6
        }));

        let bear_call = select_credit_spread_legs(
            &chain(),
            24109.0,
            CreditDirection::BearCall,
            CREDIT_EDGE_DELTA,
            WING,
        )
        .expect("bear-call credit spread must seat");
        assert_eq!(bear_call.len(), 2);
        assert!(bear_call.iter().any(|l| {
            l.opt == OptionType::CE && l.side == OrderSide::Sell && (l.strike - 24200.0).abs() < 1e-6
        }));
        assert!(bear_call.iter().any(|l| {
            l.opt == OptionType::CE && l.side == OrderSide::Buy && l.wing && (l.strike - 24300.0).abs() < 1e-6
        }));

        let seq = placement_sequence(&bear_call);
        assert_eq!(seq[0].side, OrderSide::Buy, "CRED must buy the hedge before selling the short");
        assert_eq!(seq[1].side, OrderSide::Sell);
    }

    #[test]
    fn credit_edge_er_gate_rejects_trending_open() {
        assert!(credit_edge_er_admits(0.50), "trained boundary should remain tradable");
        assert!(!credit_edge_er_admits(0.53), "2026-07-15 trending open should be vetoed");
        assert!(!credit_edge_er_admits(f64::NAN), "invalid ER must not admit a credit spread");
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
    fn shorts_bought_back_before_wings_sold_on_exit() {
        // Exit must mirror entry: close the SHORTS (buy-to-close) before selling the protective
        // wings, so a short is never momentarily un-hedged into naked risk.
        let mut legs = select_legs(&chain(), 24109.0, SellStructure::Condor).unwrap();
        legs.sort_by_key(|l| close_order_rank(l.side));
        let first_buy = legs.iter().position(|l| l.side == OrderSide::Buy).unwrap();
        let last_sell = legs.iter().rposition(|l| l.side == OrderSide::Sell).unwrap();
        assert!(
            last_sell < first_buy,
            "every short (SELL, bought back) must close before any wing (BUY, sold)"
        );
        assert!(
            legs[first_buy..].iter().all(|l| l.wing),
            "the legs closed last are the protective wings"
        );
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
    fn tight_picks_third_delta_shorts_with_100pt_wings() {
        // TIGHT shorts sit nearer ATM than the condor (~0.33Δ), wings still 100pt.
        let legs = select_legs(&chain(), 24109.0, SellStructure::Tight).expect("tight must seat");
        let ce_s = legs.iter().find(|l| l.opt == OptionType::CE && l.side == OrderSide::Sell).unwrap();
        let pe_s = legs.iter().find(|l| l.opt == OptionType::PE && l.side == OrderSide::Sell).unwrap();
        let ce_w = legs.iter().find(|l| l.opt == OptionType::CE && l.side == OrderSide::Buy).unwrap();
        let pe_w = legs.iter().find(|l| l.opt == OptionType::PE && l.side == OrderSide::Buy).unwrap();
        assert_eq!(ce_s.strike, 24200.0, "CE short at ~0.33Δ");
        assert_eq!(pe_s.strike, 24000.0, "PE short at ~0.33Δ");
        assert_eq!(ce_w.strike, 24300.0, "CE wing 100pt out");
        assert_eq!(pe_w.strike, 23900.0, "PE wing 100pt out");
        assert_eq!(SellStructure::Tight.wing(), 100.0);
    }

    #[test]
    fn widefly_is_atm_with_200pt_wings_and_larger_maxloss() {
        let legs = select_legs(&chain(), 24109.0, SellStructure::WideFly).expect("widefly must seat");
        let ce_s = legs.iter().find(|l| l.opt == OptionType::CE && l.side == OrderSide::Sell).unwrap();
        let pe_s = legs.iter().find(|l| l.opt == OptionType::PE && l.side == OrderSide::Sell).unwrap();
        let ce_w = legs.iter().find(|l| l.opt == OptionType::CE && l.side == OrderSide::Buy).unwrap();
        let pe_w = legs.iter().find(|l| l.opt == OptionType::PE && l.side == OrderSide::Buy).unwrap();
        assert_eq!(ce_s.strike, 24100.0, "ATM short");
        assert_eq!(pe_s.strike, 24100.0, "ATM short (same strike)");
        assert_eq!(ce_w.strike, 24300.0, "CE wing 200pt out");
        assert_eq!(pe_w.strike, 23900.0, "PE wing 200pt out");
        assert_eq!(SellStructure::WideFly.wing(), 200.0);
        // Wider wing ⇒ larger defined max loss per lot than the 100pt fly at the same credit.
        let credit = 125.0;
        assert!(
            max_loss_per_lot(credit, SellStructure::WideFly.wing(), 65)
                > max_loss_per_lot(credit, SellStructure::Fly.wing(), 65)
        );
    }

    #[test]
    fn per_structure_caps_match_satakarni() {
        assert_eq!(stop_frac_ml(SellStructure::CreditEdge), 0.25, "CRED uses the optimized 25%ML stop");
        assert_eq!(target_frac(SellStructure::CreditEdge), 0.50, "CRED target remains 50% credit capture");
        for (s, drift, edge, mv) in [
            (SellStructure::CreditEdge, 1.00, 1.00, 1.00),
            (SellStructure::Condor, 0.50, 0.70, 0.35),
            (SellStructure::Tight, 0.45, 0.40, 0.30),
            (SellStructure::Fly, 0.45, 0.35, 0.25),
            (SellStructure::WideFly, 0.42, 0.40, 0.28),
        ] {
            assert_eq!(entry_drift_zone_cap(s), drift, "drift cap {:?}", s);
            assert_eq!(entry_balance_edge_cap(s), edge, "balance cap {:?}", s);
            assert_eq!(move_zone_cap(s), mv, "move cap {:?}", s);
        }
    }

    #[test]
    fn credit_edge_late_net_exit_only_locks_real_late_profit() {
        let lots = 2;
        let floor = CREDIT_EDGE_LATE_NET_PROFIT_PER_LOT * lots as f64;
        assert!(!credit_edge_late_net_exit(
            SellStructure::CreditEdge,
            CREDIT_EDGE_LATE_EXIT_MIN - 1,
            floor + 1.0,
            lots
        ));
        assert!(!credit_edge_late_net_exit(
            SellStructure::Condor,
            CREDIT_EDGE_LATE_EXIT_MIN,
            floor + 1.0,
            lots
        ));
        assert!(!credit_edge_late_net_exit(
            SellStructure::CreditEdge,
            CREDIT_EDGE_LATE_EXIT_MIN,
            floor - 1.0,
            lots
        ));
        assert!(credit_edge_late_net_exit(
            SellStructure::CreditEdge,
            CREDIT_EDGE_LATE_EXIT_MIN,
            floor,
            lots
        ));
    }

    #[test]
    fn sizing_respects_risk_budget_and_skips_when_unaffordable() {
        let condor_mll = max_loss_per_lot(31.2, WING, 65); // (100−31.2)×65 ≈ 4472 — wide, expensive
        let fly_mll = max_loss_per_lot(80.0, WING, 65); //    (100−80)×65 = 1300 — cheap, ATM
        assert!((condor_mll - 4472.0).abs() < 1.0);
        // On the real ₹15k account: the cheap fly funds a lot, the expensive condor does not
        // (which is exactly why the ladder falls condor→fly at 15k).
        assert_eq!(size_lots(15_000.0, fly_mll, 0.10, 5), 1); // ₹1.5k budget / ₹1300 → 1 fly lot
        assert_eq!(size_lots(15_000.0, condor_mll, 0.10, 5), 0); // ₹1.5k < one condor lot → SKIP
        // Only once the account compounds up does the condor become fundable.
        assert_eq!(size_lots(50_000.0, condor_mll, 0.10, 5), 1); // ₹5k budget / ₹4472 → 1 condor lot
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
        assert!(entry_drift_admits(90.0, zone, SellStructure::Condor));   // 25% of zone → ok
        assert!(!entry_drift_admits(200.0, zone, SellStructure::Condor)); // 55% → rejected (>50%)
        assert!(!entry_drift_admits(165.0, zone, SellStructure::Fly));    // 46% → rejected for fly (>45%)
        assert!(!entry_drift_admits(50.0, 0.0, SellStructure::Condor));   // no zone → reject
        assert!(entry_balance_admits(0.60, SellStructure::Condor));
        assert!(!entry_balance_admits(0.60, SellStructure::Fly));
    }

    #[test]
    fn move_trim_fires_on_spot_pct_or_zone_fraction() {
        assert!(!move_trims(55.0, 24100.0, 362.0, SellStructure::Condor));   // 0.23% spot, 15% zone → hold
        assert!(move_trims(130.0, 24100.0, 362.0, SellStructure::Condor));   // 0.54% of spot → trim
        assert!(move_trims(130.0, 100_000.0, 300.0, SellStructure::Condor)); // 0.13% spot but 43% zone
        assert!(move_trims(80.0, 100_000.0, 300.0, SellStructure::Fly));     // fly cap is tighter: 27% zone
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

    #[test]
    fn efficiency_ratio_separates_chop_from_trend() {
        // Pure trend (monotone) → ER = 1.0; pure chop (round trip) → ER = 0.
        assert!((efficiency_ratio(&[1.0, 2.0, 3.0, 4.0, 5.0]).unwrap() - 1.0).abs() < 1e-9);
        assert!(efficiency_ratio(&[1.0, 2.0, 1.0, 2.0, 1.0]).unwrap().abs() < 1e-9);
        // Half-efficient: net 2 over path 4 → 0.5.
        assert!((efficiency_ratio(&[0.0, 1.0, 0.0, 1.0, 2.0]).unwrap() - 0.5).abs() < 1e-9);
        // Too few bars → None (can't judge).
        assert!(efficiency_ratio(&[1.0, 2.0]).is_none());
    }

    #[test]
    fn sell_regime_skips_trend_only_and_logs_realized_hot() {
        let straddle = 200.0;
        assert!(sell_regime_skip(0.10, 40.0, straddle).is_none());
        assert!(sell_regime_skip(0.45, 40.0, straddle).is_none());
        assert!(sell_regime_skip(0.51, 40.0, straddle).is_some());
        // Range/straddle is no longer an entry blocker in the sandbox, so live must admit it too.
        assert!(sell_regime_skip(0.10, 180.0, straddle).is_none());
    }

    #[test]
    fn regime_score_prefers_fly_on_calm_centered_chop() {
        let regime = OpeningRegime {
            er: 0.07,
            range_pts: 42.0,
            straddle: 162.0,
            edge_frac: 0.30,
        };
        let s_fly = structure_regime_score(SellStructure::Fly, regime, 0.20);
        let s_condor = structure_regime_score(SellStructure::Condor, regime, 0.35);
        assert!(
            s_fly > s_condor,
            "calm centered chop should favour the ATM fly (fly={s_fly:.3} condor={s_condor:.3})"
        );
    }

    #[test]
    fn regime_score_prefers_widefly_when_realized_is_hot() {
        let regime = OpeningRegime {
            er: 0.15,
            range_pts: 100.0,
            straddle: 140.0,
            edge_frac: 0.30,
        };
        assert!(regime.range_straddle() > 0.5);
        let s_wide = structure_regime_score(SellStructure::WideFly, regime, 0.35);
        let s_fly = structure_regime_score(SellStructure::Fly, regime, 0.40);
        assert!(s_wide > s_fly, "hot realized day should favour widefly");
    }

    #[test]
    fn pick_best_structure_breaks_ties_to_safer_ladder_rank() {
        let chosen = pick_best_structure(&[(SellStructure::Fly, 0.75), (SellStructure::Condor, 0.75)]).unwrap();
        assert_eq!(chosen, SellStructure::Condor);
    }

    #[test]
    fn half_gain_trail_keeps_half_of_peak() {
        let credit = 80.0;
        // Peak below the +15% arm (12) -> trail not active, never exits.
        assert!(!trail_exits(5.0, 10.0, credit));
        // Armed (peak +30% = 24) but gain still at peak -> hold.
        assert!(!trail_exits(24.0, 24.0, credit));
        // Armed, gain gave back to 50% of peak (12) -> exit.
        assert!(trail_exits(12.0, 24.0, credit));
        // Armed exactly at the +15% peak (12), back to half (6) -> exit.
        assert!(trail_exits(6.0, 12.0, credit));
    }

    #[test]
    fn atm_straddle_picks_nearest_strike_mid_sum() {
        let quotes = vec![
            sq(23900.0, 0.6, 0.4, 120.0, 122.0, 60.0, 62.0),
            sq(24000.0, 0.5, 0.5, 95.0, 97.0, 90.0, 92.0), // nearest to spot 24010
            sq(24100.0, 0.4, 0.6, 60.0, 62.0, 130.0, 132.0),
        ];
        // ATM mids: CE (95+97)/2=96, PE (90+92)/2=91 → 187.
        assert!((atm_straddle(&quotes, 24010.0).unwrap() - 187.0).abs() < 1e-9);
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
            // ₹50k = a compounded-up account that can fund the wide condor lot. At the real ₹15k the
            // ladder correctly falls to the fly instead — see runtime_falls_to_fly_when_capital...
            50_000.0,
            crate::portfolio::new_shared(50_000.0, 15.0, 25.0, 1),
        );

        // Calm chop open: low Efficiency Ratio (oscillates, ends flat) and a ~16pt range that is
        // small vs the ~162pt ATM straddle — so the elite regime gate admits and the condor opens.
        for (h, m, s, spot) in [
            (9, 15, 5, 24_100.0),
            (9, 20, 0, 24_112.0),
            (9, 25, 0, 24_096.0),
            (9, 30, 0, 24_108.0),
            (9, 35, 0, 24_099.0),
            (9, 40, 0, 24_106.0),
            // End near the center of the opening range so the fly's stricter balance latent admits.
            (9, 45, 0, 24_104.0),
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

    #[tokio::test]
    async fn runtime_opens_condor_on_15k_via_margin_sizing() {
        let day = NaiveDate::from_ymd_opt(2026, 6, 23).unwrap();
        let (contracts, store, underlyings) = runtime_contracts_and_store();
        // 15k: the 10% risk-fraction SIZE cap is dropped — the condor is now sized by margin
        // affordability, so a calm open opens the wide condor even on the real account. Downside is
        // bounded by the 10%-of-capital active stop, not by refusing the trade.
        let mut engine = MultiLegEngine::new(
            contracts,
            store.clone(),
            underlyings,
            0.065,
            0.0,
            15_000.0,
            crate::portfolio::new_shared(15_000.0, 15.0, 25.0, 1),
        );
        for (h, m, s, spot) in [
            (9, 15, 5, 24_100.0),
            (9, 20, 0, 24_112.0),
            (9, 25, 0, 24_096.0),
            (9, 30, 0, 24_108.0),
            (9, 35, 0, 24_099.0),
            (9, 40, 0, 24_106.0),
            // Center of the opening range so even the fly's stricter balance latent would admit;
            // the condor (tried first) opens here purely because margin sizing now funds it.
            (9, 45, 0, 24_104.0),
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
            .expect("margin-affordability sizing should open a condor on the 15k account");
        assert_eq!(active.structure, SellStructure::Condor);
        assert!(active.lots >= 1);
        assert!(crate::portfolio::is_locked(&engine.shared_circuit));
    }

    #[tokio::test]
    async fn runtime_opens_credit_edge_on_high_opening_edge_below_er_cap() {
        let day = NaiveDate::from_ymd_opt(2026, 6, 23).unwrap();
        let (contracts, store, underlyings) = runtime_contracts_and_store();
        let mut engine = MultiLegEngine::new(
            contracts,
            store.clone(),
            underlyings,
            0.065,
            0.0,
            50_000.0,
            crate::portfolio::new_shared(50_000.0, 15.0, 25.0, 1),
        );

        for (h, m, s, spot) in [
            (9, 15, 5, 24_000.0),
            (9, 20, 0, 24_025.0),
            (9, 25, 0, 24_005.0),
            (9, 30, 0, 24_035.0),
            (9, 35, 0, 24_010.0),
            (9, 40, 0, 24_045.0),
            (9, 45, 0, 24_048.0),
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
            .expect("paper runtime should open the CRED edge spread");
        assert_eq!(active.structure, SellStructure::CreditEdge);
        assert_eq!(active.legs.len(), 2);
        assert!(active
            .legs
            .iter()
            .any(|l| l.plan.opt == OptionType::PE && l.plan.side == OrderSide::Sell));
        assert!(active
            .legs
            .iter()
            .any(|l| l.plan.opt == OptionType::PE && l.plan.side == OrderSide::Buy && l.plan.wing));
    }

    #[tokio::test]
    async fn margin_latch_blocks_repeated_multileg_attempts_for_the_day() {
        let day = NaiveDate::from_ymd_opt(2026, 6, 23).unwrap();
        let (contracts, store, underlyings) = runtime_contracts_and_store();
        let circuit = crate::portfolio::new_shared(13_000.0, 15.0, 25.0, 1);
        let mut engine = MultiLegEngine::new(
            contracts,
            store.clone(),
            underlyings,
            0.065,
            0.0,
            13_000.0,
            circuit.clone(),
        );

        for (h, m, s, spot) in [
            (9, 15, 5, 24_000.0),
            (9, 20, 0, 24_020.0),
            (9, 25, 0, 24_040.0),
            (9, 30, 0, 24_060.0),
            (9, 35, 0, 24_080.0),
            (9, 40, 0, 24_100.0),
        ] {
            store.update(crate::models::Tick {
                token: 1,
                ltp: spot,
                mode: crate::models::TickMode::Ltp,
                ..crate::models::Tick::default()
            });
            engine.on_event_at(ist_ms(day, h, m, s)).await;
        }

        engine.state.entry("NIFTY".to_string()).or_default().margin_block = Some(MarginBlock {
            structure: SellStructure::CreditEdge,
            lots: 1,
            final_margin: 36_000.0,
            funds: 13_000.0,
            buffer_frac: MARGIN_BUFFER_FRAC,
        });

        store.update(crate::models::Tick {
            token: 1,
            ltp: 24_120.0,
            mode: crate::models::TickMode::Ltp,
            ..crate::models::Tick::default()
        });
        engine.on_event_at(ist_ms(day, 9, 45, 0)).await;

        assert!(
            engine
                .state
                .get("NIFTY")
                .and_then(|s| s.active.as_ref())
                .is_none(),
            "margin latch must block a CRED entry that would otherwise qualify"
        );
        assert!(
            !crate::portfolio::is_locked(&circuit),
            "a margin-latched skip must not hold the global position lock"
        );
    }
}
