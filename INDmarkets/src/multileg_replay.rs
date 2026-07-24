//! Replay multi-leg premium selling against recorded `*_option_selling_ticks.csv`.
//! Mirrors live `multileg` logic: regime indicators → best structure → one trade/day → manage exits.

use crate::execution::OrderSide;
use crate::models::OptionType;
use crate::multileg::{
    atm_straddle, combo_close_cost, combo_credit, credit_edge_direction,
    credit_edge_directional_efficiency_admits, credit_edge_er_admits, credit_edge_late_net_exit,
    dte_allows, efficiency_ratio, entry_balance_admits,
    entry_balance_edge_cap, entry_drift_admits, entry_drift_zone_cap, exit_min_for,
    hard_stop_frac_cap, max_loss_per_lot, move_stop_enabled, move_trims, option_order_cost,
    opening_directional_efficiency, pick_best_structure, profit_zone, select_credit_spread_legs,
    select_legs, sell_regime_skip, size_lots,
    stop_frac_ml, structure_regime_score, target_frac, trail_enabled, trail_exits,
    OpeningRegime, PlannedLeg, SellStructure, StrikeQuote, CREDIT_EDGE_ALLOC_FRAC,
    CREDIT_EDGE_DELTA, CREDIT_EDGE_FAR_DTE_MIN_DIRECTIONAL_EFFICIENCY,
    CREDIT_EDGE_MAX_DTE_DAYS, CREDIT_EDGE_MAX_ER, CREDIT_EDGE_MAX_LOTS,
    CREDIT_EDGE_MAX_RANGE_PCT, CREDIT_EDGE_THRESHOLD, MOVE_WINDOW_MIN,
};
use crate::websocket::FEED_SOFT_STALE_MS;

use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use std::collections::BTreeMap;
use std::path::Path;

const REPLAY_FEED_HARD_STALE_MS: u64 = 120_000;
const LOT_SIZE: u32 = 65;
const MARGIN_SIZING_FRAC: f64 = 1.0;
const MAX_LOTS: u32 = 5;

#[derive(Debug, Clone)]
pub(crate) struct TickRow {
    ts: NaiveDateTime,
    strike: f64,
    opt: OptionType,
    spot: f64,
    bid: f64,
    ask: f64,
    delta: f64,
}

#[derive(Debug, Clone)]
pub struct MultilegDayResult {
    pub day: NaiveDate,
    pub traded: bool,
    pub structure: Option<SellStructure>,
    pub lots: u32,
    pub net_pnl: f64,
    pub gross_pnl: f64,
    pub costs: f64,
    pub exit_reason: String,
    pub er: Option<f64>,
    pub skip_reason: Option<String>,
    // ponytail: reporting only — populated on a traded day, ignored by the P&L path.
    pub entry_ts: Option<NaiveDateTime>,
    pub exit_ts: Option<NaiveDateTime>,
    pub credit: f64,
    pub max_loss_unit: f64,
    /// "SELL 24000PE / BUY 23900PE" — short legs first, wings after.
    pub legs_desc: String,
}

#[derive(Debug)]
pub struct MultilegReplaySummary {
    pub start_capital: f64,
    pub end_capital: f64,
    pub days: Vec<MultilegDayResult>,
}

fn parse_opt(s: &str) -> OptionType {
    match s.trim() {
        "CE" => OptionType::CE,
        _ => OptionType::PE,
    }
}

fn ts(day: NaiveDate, hhmm: &str) -> NaiveDateTime {
    let parts: Vec<u32> = hhmm.split(':').map(|p| p.parse().unwrap_or(0)).collect();
    day.and_time(NaiveTime::from_hms_opt(parts[0], parts[1], 0).unwrap())
}

fn ts_min(day: NaiveDate, minute: u32) -> NaiveDateTime {
    day.and_time(NaiveTime::from_hms_opt(minute / 60, minute % 60, 0).unwrap())
}

fn feed_age_ms_at(rows: &[TickRow], t: NaiveDateTime) -> Option<u64> {
    let idx = rows.partition_point(|row| row.ts <= t);
    let last = rows.get(idx.checked_sub(1)?)?;
    Some(t.signed_duration_since(last.ts).num_milliseconds().max(0) as u64)
}

fn entry_px(leg: &PlannedLeg, q: &StrikeQuote) -> f64 {
    match (leg.opt, leg.side) {
        (OptionType::CE, OrderSide::Sell) => q.ce_bid,
        (OptionType::CE, OrderSide::Buy) => q.ce_ask,
        (OptionType::PE, OrderSide::Sell) => q.pe_bid,
        (OptionType::PE, OrderSide::Buy) => q.pe_ask,
    }
}

fn exit_px(leg: &PlannedLeg, q: &StrikeQuote) -> f64 {
    match (leg.opt, leg.side) {
        (OptionType::CE, OrderSide::Sell) => q.ce_ask,
        (OptionType::CE, OrderSide::Buy) => q.ce_bid,
        (OptionType::PE, OrderSide::Sell) => q.pe_ask,
        (OptionType::PE, OrderSide::Buy) => q.pe_bid,
    }
}

fn snapshot_at(rows: &[TickRow], t: NaiveDateTime) -> Option<(f64, Vec<StrikeQuote>)> {
    let mut by_strike: BTreeMap<i64, (Option<&TickRow>, Option<&TickRow>)> = BTreeMap::new();
    let mut last_spot = 0.0_f64;
    for r in rows {
        if r.ts > t {
            break;
        }
        if r.spot.is_finite() && r.spot > 0.0 {
            last_spot = r.spot;
        }
        let key = (r.strike * 1000.0).round() as i64;
        let e = by_strike.entry(key).or_insert((None, None));
        match r.opt {
            OptionType::CE => e.0 = Some(r),
            OptionType::PE => e.1 = Some(r),
        }
    }
    if by_strike.is_empty() {
        return None;
    }
    let mut quotes = Vec::new();
    for (_k, (ce, pe)) in by_strike {
        let (ce, pe) = (ce?, pe?);
        quotes.push(StrikeQuote {
            strike: ce.strike,
            ce_delta: ce.delta,
            pe_delta: pe.delta,
            ce_bid: ce.bid,
            ce_ask: ce.ask,
            pe_bid: pe.bid,
            pe_ask: pe.ask,
        });
    }
    let spot = if last_spot > 0.0 { last_spot } else { return None; };
    Some((spot, quotes))
}

fn minute_closes(rows: &[TickRow], t0: NaiveDateTime, t1: NaiveDateTime) -> Vec<f64> {
    let mut buckets: BTreeMap<(u32, u32), f64> = BTreeMap::new();
    for r in rows {
        if r.ts < t0 || r.ts > t1 {
            continue;
        }
        if r.spot.is_finite() && r.spot > 0.0 {
            buckets.insert((r.ts.hour(), r.ts.minute()), r.spot);
        }
    }
    buckets.into_values().collect()
}

#[derive(Debug, Clone, Copy)]
struct ReplayOpening {
    range_pts: f64,
    edge_pos: f64,
    edge_frac: f64,
}

fn opening_latent(rows: &[TickRow], day: NaiveDate, t_entry: NaiveDateTime) -> Option<ReplayOpening> {
    let t_open = ts(day, "09:15:05");
    let spots: Vec<f64> = rows
        .iter()
        .filter(|r| r.ts >= t_open && r.ts <= t_entry && r.spot.is_finite() && r.spot > 0.0)
        .map(|r| r.spot)
        .collect();
    if spots.len() < 2 {
        return None;
    }
    let min = spots.iter().cloned().fold(f64::INFINITY, f64::min);
    let max = spots.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    let range_pts = max - min;
    let last = *spots.last()?;
    let range_pos = if range_pts <= 0.0 {
        0.5
    } else {
        (last - min) / range_pts
    };
    let edge_pos = range_pos.clamp(0.0, 1.0);
    let edge_frac = (edge_pos - 0.5).abs() * 2.0;
    Some(ReplayOpening {
        range_pts,
        edge_pos,
        edge_frac,
    })
}

fn pnl(
    legs: &[PlannedLeg],
    quotes_e: &[StrikeQuote],
    quotes_x: &[StrikeQuote],
    lots: u32,
    exit_ts_ms: u64,
) -> Option<(f64, f64, f64)> {
    let qty = LOT_SIZE * lots;
    let mut gross_unit = 0.0;
    let mut costs = 0.0;
    for leg in legs {
        let qe = quotes_e.iter().find(|q| (q.strike - leg.strike).abs() < 1e-6)?;
        let qx = quotes_x.iter().find(|q| (q.strike - leg.strike).abs() < 1e-6)?;
        let ef = entry_px(leg, qe);
        let xf = exit_px(leg, qx);
        let pos = if leg.side == OrderSide::Buy { 1.0 } else { -1.0 };
        gross_unit += pos * (xf - ef);
        let close_side = if leg.side == OrderSide::Sell {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        };
        costs += option_order_cost(ef, qty, leg.side, exit_ts_ms);
        costs += option_order_cost(xf, qty, close_side, exit_ts_ms);
    }
    let gross = gross_unit * qty as f64;
    Some((gross - costs, gross, costs))
}

#[derive(Debug, Clone)]
struct Plan {
    structure: SellStructure,
    entry_ts: NaiveDateTime,
    legs: Vec<PlannedLeg>,
    credit: f64,
    max_loss_unit: f64,
    zone_width: f64,
    lots: u32,
    score: f64,
    er: f64,
}

fn try_plan(
    structure: SellStructure,
    entry_ts: NaiveDateTime,
    quotes: &[StrikeQuote],
    spot: f64,
    opening: ReplayOpening,
    regime: OpeningRegime,
    dte: i64,
    capital: f64,
    no_gate: bool,
) -> Result<Plan, String> {
    let legs = if structure == SellStructure::CreditEdge {
        if !no_gate && !credit_edge_er_admits(regime.er) {
            return Err(format!(
                "CRED ER {:.2} > {:.2}",
                regime.er, CREDIT_EDGE_MAX_ER
            ));
        }
        if !no_gate && opening.range_pts / spot > CREDIT_EDGE_MAX_RANGE_PCT {
            return Err(format!(
                "CRED range {:.2}% > {:.2}%",
                opening.range_pts / spot * 100.0,
                CREDIT_EDGE_MAX_RANGE_PCT * 100.0
            ));
        }
        if !no_gate
            && !credit_edge_directional_efficiency_admits(
                dte,
                regime.directional_efficiency,
            )
        {
            return Err(format!(
                "CRED directional efficiency {:.2} < {:.2}",
                regime.directional_efficiency,
                CREDIT_EDGE_FAR_DTE_MIN_DIRECTIONAL_EFFICIENCY
            ));
        }
        let direction = credit_edge_direction(opening.edge_pos, CREDIT_EDGE_THRESHOLD)
            .ok_or("CRED opening not near edge")?;
        select_credit_spread_legs(quotes, spot, direction, CREDIT_EDGE_DELTA, structure.wing())
            .ok_or("CRED legs not seatable")?
    } else {
        select_legs(quotes, spot, structure).ok_or("legs not seatable")?
    };
    let credit = combo_credit(&legs, quotes).ok_or("no credit")?;
    let zone_width = if structure == SellStructure::CreditEdge {
        structure.wing()
    } else {
        let (_lo, _hi, zone_width) = profit_zone(&legs, credit).ok_or("no zone")?;
        zone_width
    };
    let max_loss_unit = structure.wing() - credit;
    if credit <= 0.0 || max_loss_unit <= 0.0 {
        return Err("non-positive credit/max-loss".into());
    }
    let drift_frac = opening.range_pts / zone_width;
    if structure != SellStructure::CreditEdge && !no_gate && !entry_drift_admits(opening.range_pts, zone_width, structure) {
        return Err(format!(
            "DRIFT-ZONE {:.0}% > {:.0}%",
            drift_frac * 100.0,
            entry_drift_zone_cap(structure) * 100.0
        ));
    }
    if structure != SellStructure::CreditEdge && !no_gate && !entry_balance_admits(opening.edge_frac, structure) {
        return Err(format!(
            "RANGE-BALANCE edge {:.0}% > {:.0}%",
            opening.edge_frac * 100.0,
            entry_balance_edge_cap(structure) * 100.0
        ));
    }
    let mll = max_loss_per_lot(credit, structure.wing(), LOT_SIZE);
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
    let lots = size_lots(capital, mll, sizing_frac, max_lots);
    if lots == 0 {
        return Err("margin cannot fund one lot".into());
    }
    let score = structure_regime_score(structure, regime, drift_frac);
    Ok(Plan {
        structure,
        entry_ts,
        legs,
        credit,
        max_loss_unit,
        zone_width,
        lots,
        score,
        er: regime.er,
    })
}

fn manage(
    rows: &[TickRow],
    day: NaiveDate,
    plan: &Plan,
    capital: f64,
    entry_quotes: &[StrikeQuote],
) -> (NaiveDateTime, String) {
    let t_entry = plan.entry_ts;
    let t_exit = ts_min(day, exit_min_for(plan.structure));
    let hard_stale = chrono::Duration::milliseconds(REPLAY_FEED_HARD_STALE_MS as i64);
    let mut peak_gain = 0.0_f64;
    let stop_rupees = hard_stop_frac_cap(plan.structure) * capital;

    // Single O(n) forward pass: evaluate stop/target/trail/move on EVERY recorded tick (one eval
    // per distinct timestamp) — exact parity with live `manage_active`, which runs on every market
    // event with no throttle. (The old minute-sampled loop booked a fast stop up to a minute late,
    // overstating the realized loss; rebuilding the chain per tick via snapshot_at was O(n²).)
    let plan_keys: std::collections::HashSet<i64> = plan
        .legs
        .iter()
        .map(|l| (l.strike * 1000.0).round() as i64)
        .collect();
    let mut by_strike: std::collections::BTreeMap<i64, (Option<(f64, f64, f64)>, Option<(f64, f64, f64)>)> =
        std::collections::BTreeMap::new();
    let mut spot = 0.0_f64;
    let mut spot_hist: Vec<(NaiveDateTime, f64)> = Vec::new();
    let mut mv = 0usize; // pointer to the latest spot_hist entry ≤ t − MOVE_WINDOW_MIN
    let mut last_tick = None;

    let mut idx = 0usize;
    while idx < rows.len() {
        let t = rows[idx].ts;
        if t > t_exit {
            break;
        }
        if t > t_entry {
            if let Some(last) = last_tick {
                let deadline = last + hard_stale;
                if deadline <= t && deadline <= t_exit {
                    return (
                        deadline,
                        format!("FEED-STALE {}s", REPLAY_FEED_HARD_STALE_MS / 1000),
                    );
                }
            }
        }
        // Fold every row stamped at this tick into the running leg-quote state.
        while idx < rows.len() && rows[idx].ts == t {
            let r = &rows[idx];
            if r.spot.is_finite() && r.spot > 0.0 {
                spot = r.spot;
            }
            let key = (r.strike * 1000.0).round() as i64;
            if plan_keys.contains(&key) {
                let e = by_strike.entry(key).or_insert((None, None));
                let q = (r.bid, r.ask, r.delta);
                match r.opt {
                    OptionType::CE => e.0 = Some(q),
                    OptionType::PE => e.1 = Some(q),
                }
            }
            idx += 1;
        }
        last_tick = Some(t);
        if t <= t_entry || spot <= 0.0 {
            continue;
        }

        let quotes: Vec<StrikeQuote> = by_strike
            .iter()
            .filter_map(|(k, (ce, pe))| {
                let (cb, ca, cd) = (*ce)?;
                let (pb, pa, pd) = (*pe)?;
                Some(StrikeQuote {
                    strike: *k as f64 / 1000.0,
                    ce_delta: cd,
                    pe_delta: pd,
                    ce_bid: cb,
                    ce_ask: ca,
                    pe_bid: pb,
                    pe_ask: pa,
                })
            })
            .collect();
        let close_cost = combo_close_cost(&plan.legs, &quotes).unwrap_or(plan.credit);
        let gain = plan.credit - close_cost;

        // Rolling MOVE_WINDOW_MIN move on underlying spot (matches live), via a monotonic pointer.
        spot_hist.push((t, spot));
        let cutoff = t - chrono::Duration::minutes(MOVE_WINDOW_MIN as i64);
        while mv + 1 < spot_hist.len() && spot_hist[mv + 1].0 <= cutoff {
            mv += 1;
        }
        if spot_hist[mv].0 <= cutoff {
            let move_pts = (spot - spot_hist[mv].1).abs();
            if move_stop_enabled(plan.structure) && move_trims(move_pts, spot, plan.zone_width, plan.structure) {
                return (t, format!("MOVE {:.2}%/{MOVE_WINDOW_MIN}m", move_pts / spot * 100.0));
            }
        }

        if gain >= target_frac(plan.structure) * plan.credit {
            return (t, format!("TARGET {:.0}%", target_frac(plan.structure) * 100.0));
        }
        peak_gain = peak_gain.max(gain);
        // NET (post-cost) P&L the exit would realize right now. Live checks the late CRED
        // profit lock before the trail, so preserve that reason precedence in replay.
        let t_ms = t.and_utc().timestamp_millis() as u64;
        let net_now = pnl(&plan.legs, entry_quotes, &quotes, plan.lots, t_ms)
            .map(|(net, _gross, _costs)| net);
        if let Some(net) = net_now {
            let mins = t.hour() * 60 + t.minute();
            if credit_edge_late_net_exit(plan.structure, mins, net, plan.lots) {
                return (t, format!("LATE-NET ₹{net:.0}"));
            }
        }
        if trail_enabled(plan.structure) && trail_exits(gain, peak_gain, plan.credit) {
            return (
                t,
                format!("TRAIL +{:.0}%", crate::multileg::TRAIL_FRAC * peak_gain / plan.credit * 100.0),
            );
        }
        // Hard rupee stop on the NET (post-cost) P&L the exit would actually realize — so the
        // booked loss lands at the structure-specific capital cap, not cap+costs+slippage.
        // Same realized formula (pnl) used at the real exit; live mirrors this via realized_pnl
        // in manage_active.
        if let Some(net) = net_now {
            if net <= -stop_rupees {
                return (
                    t,
                    format!(
                        "STOP ₹{stop_rupees:.0} ({:.0}% cap, net)",
                        hard_stop_frac_cap(plan.structure) * 100.0
                    ),
                );
            }
        }
        if gain <= -stop_frac_ml(plan.structure) * plan.max_loss_unit {
            return (t, format!("STOP {:.0}%ML", stop_frac_ml(plan.structure) * 100.0));
        }
    }

    if let Some(last) = last_tick.filter(|last| *last >= t_entry) {
        let deadline = last + hard_stale;
        if deadline <= t_exit {
            return (
                deadline,
                format!("FEED-STALE {}s", REPLAY_FEED_HARD_STALE_MS / 1000),
            );
        }
    }

    // Label the actual time-exit for this structure (CreditEdge exits 14:45, neutrals 14:55).
    let m = exit_min_for(plan.structure);
    (t_exit, format!("{:02}:{:02}", m / 60, m % 60))
}

pub(crate) fn load_selling_ticks(path: &Path) -> Result<(NaiveDate, NaiveDate, Vec<TickRow>), String> {
    let mut rdr = crate::open_csv(path).map_err(|e| e.to_string())?;
    let headers = rdr.headers().map_err(|e| e.to_string())?.clone();
    let idx = |name: &str| headers.iter().position(|h| h == name).ok_or_else(|| format!("missing col {name}"));
    let i_ts = idx("recv_ts")?;
    let i_expiry = idx("expiry")?;
    let i_strike = idx("strike")?;
    let i_ot = idx("option_type")?;
    let i_spot = idx("spot")?;
    let i_bid = idx("bid")?;
    let i_ask = idx("ask")?;
    let i_delta = idx("delta_mid")?;

    let mut rows = Vec::new();
    let mut day = None;
    let mut expiry = None;
    for rec in rdr.records() {
        let rec = rec.map_err(|e| e.to_string())?;
        let ts = NaiveDateTime::parse_from_str(rec.get(i_ts).unwrap_or(""), "%Y-%m-%dT%H:%M:%S%.f")
            .or_else(|_| NaiveDateTime::parse_from_str(rec.get(i_ts).unwrap_or(""), "%Y-%m-%dT%H:%M:%S"))
            .map_err(|e| format!("ts parse: {e}"))?;
        day.get_or_insert(ts.date());
        let bid: f64 = rec.get(i_bid).unwrap_or("0").parse().unwrap_or(0.0);
        let ask: f64 = rec.get(i_ask).unwrap_or("0").parse().unwrap_or(0.0);
        if bid <= 0.0 && ask <= 0.0 {
            continue;
        }
        let row_expiry = NaiveDate::parse_from_str(rec.get(i_expiry).unwrap_or(""), "%Y-%m-%d")
            .map_err(|e| format!("expiry parse: {e}"))?;
        expiry.get_or_insert(row_expiry);
        rows.push(TickRow {
            ts,
            strike: rec.get(i_strike).unwrap_or("0").parse().unwrap_or(0.0),
            opt: parse_opt(rec.get(i_ot).unwrap_or("CE")),
            spot: rec.get(i_spot).unwrap_or("0").parse().unwrap_or(0.0),
            bid,
            ask,
            delta: rec.get(i_delta).unwrap_or("0").parse().unwrap_or(0.0),
        });
    }
    // manage()'s single-pass per-tick walk (and snapshot_at's early break) assume time order.
    // The recorder writes ascending recv_ts, but make the invariant explicit and cheap to hold.
    rows.sort_by(|a, b| a.ts.cmp(&b.ts));
    Ok((day.ok_or("empty file")?, expiry.ok_or("empty file")?, rows))
}

pub(crate) fn replay_day(
    rows: &[TickRow],
    day: NaiveDate,
    expiry: NaiveDate,
    mut capital: f64,
) -> (MultilegDayResult, f64) {
    let base = MultilegDayResult {
        day,
        traded: false,
        structure: None,
        lots: 0,
        net_pnl: 0.0,
        gross_pnl: 0.0,
        costs: 0.0,
        exit_reason: String::new(),
        er: None,
        skip_reason: None,
        entry_ts: None,
        exit_ts: None,
        credit: 0.0,
        max_loss_unit: 0.0,
        legs_desc: String::new(),
    };

    let dte = expiry.signed_duration_since(day).num_days();
    if dte < 0 {
        return (
            MultilegDayResult {
                skip_reason: Some(format!("expiry {expiry} is expired ({dte}DTE)")),
                ..base
            },
            capital,
        );
    }
    let near_expiry = dte_allows(dte);
    // Backtest-only research bypass for threshold tuning: lift the far-DTE sideways
    // day-admission gate so every day attempts a trade. Live/default is unaffected
    // because this env is only read here in the replay. Entry time and structure gates still apply.
    let no_gate = std::env::var("SATA_ML_NO_GATE").is_ok();
    let credit_edge_allowed = dte <= CREDIT_EDGE_MAX_DTE_DAYS;
    let standard_allowed = near_expiry || no_gate;
    // Far-DTE neutral multi-leg structures remain off by default, but CRED-EDGE is a separate
    // 09:45 edge-open credit spread validated in the option-idea lab through the weekly cycle.
    if !standard_allowed && !credit_edge_allowed {
        return (
            MultilegDayResult {
                skip_reason: Some(format!("far-DTE ({dte}DTE) outside CRED/near-expiry gates")),
                ..base
            },
            capital,
        );
    }

    let t_open = ts(day, "09:15:05");

    let mut candidates: Vec<Plan> = Vec::new();
    let mut last_err = String::new();
    let mut first_er = None;
    for structure in SellStructure::LADDER {
        if structure == SellStructure::CreditEdge && !credit_edge_allowed {
            continue;
        }
        if structure != SellStructure::CreditEdge && !standard_allowed {
            continue;
        }

        let t_entry = if structure == SellStructure::CreditEdge || near_expiry {
            ts(day, "09:45")
        } else {
            ts(day, "12:00")
        };
        if !matches!(feed_age_ms_at(rows, t_entry), Some(age) if age < FEED_SOFT_STALE_MS) {
            last_err = format!("{structure:?} feed stale at entry");
            continue;
        }
        let closes = minute_closes(rows, t_open, t_entry);
        let Some(er) = efficiency_ratio(&closes) else {
            last_err = format!("{structure:?} insufficient morning ER data");
            continue;
        };
        first_er.get_or_insert(er);
        let Some((spot, quotes)) = snapshot_at(rows, t_entry) else {
            last_err = format!("{structure:?} no entry snapshot");
            continue;
        };
        let Some(straddle) = atm_straddle(&quotes, spot) else {
            last_err = format!("{structure:?} no ATM straddle");
            continue;
        };
        let range_pts = closes.iter().cloned().fold(f64::NEG_INFINITY, f64::max)
            - closes.iter().cloned().fold(f64::INFINITY, f64::min);
        let Some(opening) = opening_latent(rows, day, t_entry) else {
            last_err = format!("{structure:?} insufficient drift-latent data");
            continue;
        };
        let regime = OpeningRegime {
            er,
            range_pts,
            straddle,
            directional_efficiency: opening_directional_efficiency(&closes).unwrap_or(0.0),
            edge_frac: opening.edge_frac,
        };
        if structure != SellStructure::CreditEdge {
            if let Some(why) = sell_regime_skip(er, range_pts, straddle).filter(|_| !no_gate) {
                last_err = format!("{structure:?} {why} (ER {er:.2})");
                continue;
            }
        }

        if std::env::var("SATA_ML_LATENTS").is_ok() {
            let drift = regime.directional_efficiency * range_pts / spot * 100.0;
            eprintln!(
                "LATENT {} | {:?} {}DTE {} | ER {:.2} | edge {:.0}% | range {:.4}% | drift {:.4}% | drift/range {:.4} | straddle {:.0} | range/straddle {:.4}",
                day, structure, dte, if near_expiry { "near" } else { "far" }, er, opening.edge_frac * 100.0,
                range_pts / spot * 100.0, drift, drift / (range_pts / spot * 100.0), straddle, range_pts / straddle,
            );
        }

        match try_plan(
            structure,
            t_entry,
            &quotes,
            spot,
            opening,
            regime,
            dte,
            capital,
            no_gate,
        ) {
            Ok(p) => candidates.push(p),
            Err(e) => last_err = format!("{structure:?} {e}"),
        }
    }

    let scoreboard: Vec<(SellStructure, f64)> = candidates.iter().map(|p| (p.structure, p.score)).collect();
    let Some(best_s) = pick_best_structure(&scoreboard) else {
        return (
            MultilegDayResult {
                er: first_er,
                skip_reason: Some(format!("no structure passed gates ({last_err})")),
                ..base
            },
            capital,
        );
    };
    let plan = candidates.into_iter().find(|p| p.structure == best_s).unwrap();

    let Some((_s, q_e)) = snapshot_at(rows, plan.entry_ts) else {
        return (
            MultilegDayResult {
                er: Some(plan.er),
                skip_reason: Some("entry marks missing".into()),
                ..base
            },
            capital,
        );
    };
    let (t_out, why) = manage(rows, day, &plan, capital, &q_e);
    let Some((_s, q_x)) = snapshot_at(rows, t_out) else {
        return (
            MultilegDayResult {
                er: Some(plan.er),
                skip_reason: Some("exit marks missing".into()),
                ..base
            },
            capital,
        );
    };
    let exit_ms = t_out.and_utc().timestamp_millis() as u64;
    let Some((net, gross, costs)) = pnl(&plan.legs, &q_e, &q_x, plan.lots, exit_ms) else {
        return (
            MultilegDayResult {
                er: Some(plan.er),
                skip_reason: Some("pnl calc failed".into()),
                ..base
            },
            capital,
        );
    };

    capital += net;
    (
        MultilegDayResult {
            traded: true,
            structure: Some(plan.structure),
            lots: plan.lots,
            net_pnl: net,
            gross_pnl: gross,
            costs,
            exit_reason: why,
            er: Some(plan.er),
            skip_reason: None,
            entry_ts: Some(plan.entry_ts),
            exit_ts: Some(t_out),
            credit: plan.credit,
            max_loss_unit: plan.max_loss_unit,
            legs_desc: describe_legs(&plan.legs),
            ..base
        },
        capital,
    )
}

/// "SELL 24000PE / BUY 23900PE" — shorts first so the structure reads at a glance.
fn describe_legs(legs: &[crate::multileg::PlannedLeg]) -> String {
    let mut v: Vec<&crate::multileg::PlannedLeg> = legs.iter().collect();
    v.sort_by_key(|l| l.wing); // shorts (wing=false) first
    v.iter()
        .map(|l| {
            let side = if matches!(l.side, OrderSide::Sell) { "SELL" } else { "BUY" };
            format!("{side} {:.0}{:?}", l.strike, l.opt)
        })
        .collect::<Vec<_>>()
        .join(" / ")
}

pub fn replay_file(path: &Path, start_capital: f64) -> Result<MultilegReplaySummary, String> {
    let (day, expiry, rows) = load_selling_ticks(path)?;
    let (result, end) = replay_day(&rows, day, expiry, start_capital);
    Ok(MultilegReplaySummary {
        start_capital,
        end_capital: end,
        days: vec![result],
    })
}

pub fn replay_many(paths: &[&Path], start_capital: f64) -> Result<MultilegReplaySummary, String> {
    let mut cap = start_capital;
    let mut days = Vec::new();
    for path in paths {
        let (day, expiry, rows) = load_selling_ticks(path)?;
        let (r, new_cap) = replay_day(&rows, day, expiry, cap);
        cap = new_cap;
        days.push(r);
    }
    days.sort_by_key(|d| d.day);
    Ok(MultilegReplaySummary {
        start_capital,
        end_capital: cap,
        days,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replay_module_compiles_and_expired_expiry_blocks() {
        let day = NaiveDate::from_ymd_opt(2026, 6, 24).unwrap();
        let expiry = NaiveDate::from_ymd_opt(2026, 6, 23).unwrap();
        let (r, cap) = replay_day(&[], day, expiry, 15_000.0);
        assert!(!r.traded);
        assert!(r.skip_reason.as_ref().unwrap().contains("expired"));
        assert_eq!(cap, 15_000.0);
    }

    #[test]
    fn active_replay_halts_after_two_minute_feed_timeout() {
        let day = NaiveDate::from_ymd_opt(2026, 7, 20).unwrap();
        let t_entry = ts(day, "09:45");
        let legs = vec![
            PlannedLeg { strike: 24300.0, opt: OptionType::CE, side: OrderSide::Sell, wing: false },
            PlannedLeg { strike: 24400.0, opt: OptionType::CE, side: OrderSide::Buy, wing: true },
        ];
        let plan = Plan {
            structure: SellStructure::CreditEdge,
            entry_ts: t_entry,
            legs,
            credit: 20.0,
            max_loss_unit: 80.0,
            zone_width: 100.0,
            lots: 1,
            score: 1.0,
            er: 0.30,
        };
        let quote = |strike: f64| StrikeQuote {
            strike,
            ce_delta: 0.3,
            pe_delta: -0.3,
            ce_bid: if strike == 24300.0 { 30.0 } else { 10.0 },
            ce_ask: if strike == 24300.0 { 30.0 } else { 10.0 },
            pe_bid: 10.0,
            pe_ask: 10.0,
        };
        let entry_quotes = vec![quote(24300.0), quote(24400.0)];
        let mut rows = Vec::new();
        for secs in [0_i64, 10] {
            let t = t_entry + chrono::Duration::seconds(secs);
            for strike in [24300.0, 24400.0] {
                for opt in [OptionType::CE, OptionType::PE] {
                    let px = if opt == OptionType::CE && strike == 24300.0 { 30.0 } else { 10.0 };
                    rows.push(TickRow {
                        ts: t,
                        strike,
                        opt,
                        spot: 24200.0,
                        bid: px,
                        ask: px,
                        delta: 0.3,
                    });
                }
            }
        }

        let (t_out, why) = manage(&rows, day, &plan, 15_000.0, &entry_quotes);
        assert_eq!(why, "FEED-STALE 120s");
        assert_eq!(t_out, t_entry + chrono::Duration::seconds(130));
    }

    // Exercises the per-tick manage() loop's MOVE-exit path (untested by the 3 sample days, none of
    // which trip MOVE): the monotonic spot pointer + plan-key quote accumulation must detect a
    // >MOVE_PCT underlying move across the 2-min window and exit on the right tick.
    #[test]
    fn manage_move_exit_fires_on_per_tick_pass() {
        let day = NaiveDate::from_ymd_opt(2026, 6, 22).unwrap();
        let t_entry = ts(day, "09:45");

        // Condor legs; quotes fixed at bid 10 / ask 30 both sides so close_cost = credit (gain 0)
        // every tick — only MOVE can fire (target/stop stay dormant).
        let legs = vec![
            PlannedLeg { strike: 23900.0, opt: OptionType::PE, side: OrderSide::Sell, wing: false },
            PlannedLeg { strike: 23800.0, opt: OptionType::PE, side: OrderSide::Buy, wing: true },
            PlannedLeg { strike: 24100.0, opt: OptionType::CE, side: OrderSide::Sell, wing: false },
            PlannedLeg { strike: 24200.0, opt: OptionType::CE, side: OrderSide::Buy, wing: true },
        ];
        let plan = Plan {
            structure: SellStructure::Condor,
            entry_ts: t_entry,
            legs,
            credit: 40.0, // shorts 30 each − wings 10 each = +40 collected (matches the quotes below)
            max_loss_unit: 60.0,
            zone_width: 200.0,
            lots: 3,
            score: 1.0,
            er: 0.10,
        };

        // Zero-spread quotes priced so credit/gain AND net all stay flat (only MOVE can fire): shorts
        // 30, wings 10 → combo credit = 40 = plan.credit, gain 0, net ≈ −costs (well above −1500 stop).
        let price = |k: f64| if k == 23900.0 || k == 24100.0 { 30.0 } else { 10.0 };
        let strikes = [23900.0, 23800.0, 24100.0, 24200.0];
        let entry_quotes: Vec<StrikeQuote> = strikes
            .iter()
            .map(|&k| StrikeQuote {
                strike: k,
                ce_delta: 0.2,
                pe_delta: -0.2,
                ce_bid: price(k),
                ce_ask: price(k),
                pe_bid: price(k),
                pe_ask: price(k),
            })
            .collect();

        // Spot flat at 24000 through 09:46:30, then a +100pt (0.42% > MOVE_PCT 0.25%) thrust by
        // 09:47:30 — the 2-min window (cutoff 09:45:30) sees 24000 → 24100.
        let bar = |secs: i64, spot: f64| -> Vec<TickRow> {
            let t = t_entry + chrono::Duration::seconds(secs);
            let mut out = Vec::new();
            for &k in &strikes {
                for opt in [OptionType::CE, OptionType::PE] {
                    out.push(TickRow { ts: t, strike: k, opt, spot, bid: price(k), ask: price(k), delta: 0.2 });
                }
            }
            out
        };
        let mut rows = Vec::new();
        for (secs, spot) in [(0i64, 24000.0), (30, 24000.0), (60, 24000.0), (90, 24030.0), (120, 24060.0), (150, 24100.0)] {
            rows.extend(bar(secs, spot));
        }

        let (t_out, why) = manage(&rows, day, &plan, 15_000.0, &entry_quotes);
        assert!(why.starts_with("MOVE"), "expected MOVE exit, got {why:?}");
        assert_eq!(t_out, t_entry + chrono::Duration::seconds(150), "MOVE must fire on the +100pt tick");
    }

    // The hard stop must trigger on NET (post-cost) P&L, not gross, so the booked loss lands at the
    // 10%-of-capital cap. Shorts blow out 30→60 (wings flat); net loss clears −1500 → "10% cap, net".
    #[test]
    fn rupee_stop_triggers_on_net_pnl() {
        let day = NaiveDate::from_ymd_opt(2026, 6, 22).unwrap();
        let t_entry = ts(day, "09:45");
        let legs = vec![
            PlannedLeg { strike: 23900.0, opt: OptionType::PE, side: OrderSide::Sell, wing: false },
            PlannedLeg { strike: 23800.0, opt: OptionType::PE, side: OrderSide::Buy, wing: true },
            PlannedLeg { strike: 24100.0, opt: OptionType::CE, side: OrderSide::Sell, wing: false },
            PlannedLeg { strike: 24200.0, opt: OptionType::CE, side: OrderSide::Buy, wing: true },
        ];
        let plan = Plan {
            structure: SellStructure::Condor,
            entry_ts: t_entry,
            legs,
            credit: 40.0,
            max_loss_unit: 60.0,
            zone_width: 200.0,
            lots: 3,
            score: 1.0,
            er: 0.10,
        };
        let strikes = [23900.0, 23800.0, 24100.0, 24200.0];
        let mk = |k: f64, short_px: f64| StrikeQuote {
            strike: k,
            ce_delta: 0.2,
            pe_delta: -0.2,
            ce_bid: if k == 24100.0 { short_px } else { 10.0 },
            ce_ask: if k == 24100.0 { short_px } else { 10.0 },
            pe_bid: if k == 23900.0 { short_px } else { 10.0 },
            pe_ask: if k == 23900.0 { short_px } else { 10.0 },
        };
        let entry_quotes: Vec<StrikeQuote> = strikes.iter().map(|&k| mk(k, 30.0)).collect();

        // spot flat (no MOVE); shorts marked at 60 → per-unit gain −40, net far past −1500.
        let mut rows = Vec::new();
        for secs in [0i64, 30] {
            let t = t_entry + chrono::Duration::seconds(secs);
            let short_px = if secs == 0 { 30.0 } else { 60.0 };
            for &k in &strikes {
                let p_ce = if k == 24100.0 { short_px } else { 10.0 };
                let p_pe = if k == 23900.0 { short_px } else { 10.0 };
                rows.push(TickRow { ts: t, strike: k, opt: OptionType::CE, spot: 24000.0, bid: p_ce, ask: p_ce, delta: 0.2 });
                rows.push(TickRow { ts: t, strike: k, opt: OptionType::PE, spot: 24000.0, bid: p_pe, ask: p_pe, delta: 0.2 });
            }
        }

        let (t_out, why) = manage(&rows, day, &plan, 15_000.0, &entry_quotes);
        assert!(why.contains("net"), "stop must be the net-based 10% cap, got {why:?}");
        assert_eq!(t_out, t_entry + chrono::Duration::seconds(30));
    }
}
