//! Replay multi-leg premium selling against recorded `*_option_selling_ticks.csv`.
//! Mirrors live `multileg` logic: regime indicators → best structure → one trade/day → manage exits.

use crate::execution::OrderSide;
use crate::models::OptionType;
use crate::multileg::{
    atm_straddle, combo_close_cost, combo_credit, efficiency_ratio, entry_balance_admits,
    entry_drift_admits, entry_drift_zone_cap, entry_balance_edge_cap, max_loss_per_lot,
    move_trims, option_order_cost, pick_best_structure, profit_zone, select_legs, sell_regime_skip,
    size_lots, structure_regime_score, trail_exits, OpeningRegime, PlannedLeg, SellStructure,
    StrikeQuote, MOVE_WINDOW_MIN, STOP_FRAC_ML, weekday_allows,
};

use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use std::collections::BTreeMap;
use std::path::Path;

const LOT_SIZE: u32 = 65;
const MARGIN_SIZING_FRAC: f64 = 1.0;
const STOP_FRAC_CAP: f64 = 0.10;
const MAX_LOTS: u32 = 5;
const TARGET_FRAC: f64 = 0.50;

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

fn opening_latent(
    rows: &[TickRow],
    day: NaiveDate,
    t_entry: NaiveDateTime,
    legs: &[PlannedLeg],
    credit: f64,
) -> Option<(f64, f64, f64)> {
    let (_lo, _hi, zone_width) = profit_zone(legs, credit)?;
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
    let edge_frac = (range_pos - 0.5).abs() * 2.0;
    let zone_frac = range_pts / zone_width;
    Some((range_pts, edge_frac, zone_frac))
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
    legs: Vec<PlannedLeg>,
    credit: f64,
    max_loss_unit: f64,
    zone_width: f64,
    lots: u32,
    score: f64,
}

fn try_plan(
    structure: SellStructure,
    quotes: &[StrikeQuote],
    spot: f64,
    opening: (f64, f64, f64),
    regime: OpeningRegime,
    capital: f64,
) -> Result<Plan, String> {
    let legs = select_legs(quotes, spot, structure).ok_or("legs not seatable")?;
    let credit = combo_credit(&legs, quotes).ok_or("no credit")?;
    let (_lo, _hi, zone_width) = profit_zone(&legs, credit).ok_or("no zone")?;
    let max_loss_unit = structure.wing() - credit;
    if credit <= 0.0 || max_loss_unit <= 0.0 {
        return Err("non-positive credit/max-loss".into());
    }
    let (range_pts, edge_frac, drift_frac) = opening;
    let _ = range_pts;
    if !entry_drift_admits(opening.0, zone_width, structure) {
        return Err(format!(
            "DRIFT-ZONE {:.0}% > {:.0}%",
            drift_frac * 100.0,
            entry_drift_zone_cap(structure) * 100.0
        ));
    }
    if !entry_balance_admits(edge_frac, structure) {
        return Err(format!(
            "RANGE-BALANCE edge {:.0}% > {:.0}%",
            edge_frac * 100.0,
            entry_balance_edge_cap(structure) * 100.0
        ));
    }
    let mll = max_loss_per_lot(credit, structure.wing(), LOT_SIZE);
    let lots = size_lots(capital, mll, MARGIN_SIZING_FRAC, MAX_LOTS);
    if lots == 0 {
        return Err("margin cannot fund one lot".into());
    }
    let score = structure_regime_score(structure, regime, drift_frac);
    Ok(Plan {
        structure,
        legs,
        credit,
        max_loss_unit,
        zone_width,
        lots,
        score,
    })
}

fn manage(
    rows: &[TickRow],
    day: NaiveDate,
    t_entry: NaiveDateTime,
    plan: &Plan,
    capital: f64,
    entry_quotes: &[StrikeQuote],
) -> (NaiveDateTime, String) {
    let t_exit = ts(day, "15:15");
    let mut peak_gain = 0.0_f64;
    let stop_rupees = STOP_FRAC_CAP * capital;

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

    let mut idx = 0usize;
    while idx < rows.len() {
        let t = rows[idx].ts;
        if t > t_exit {
            break;
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
            if move_trims(move_pts, spot, plan.zone_width, plan.structure) {
                return (t, format!("MOVE {:.2}%/{MOVE_WINDOW_MIN}m", move_pts / spot * 100.0));
            }
        }

        if gain >= TARGET_FRAC * plan.credit {
            return (t, format!("TARGET {:.0}%", TARGET_FRAC * 100.0));
        }
        peak_gain = peak_gain.max(gain);
        if trail_exits(gain, peak_gain, plan.credit) {
            return (
                t,
                format!("TRAIL +{:.0}%", crate::multileg::TRAIL_FRAC * peak_gain / plan.credit * 100.0),
            );
        }
        // Hard rupee stop on the NET (post-cost) P&L the exit would actually realize — so the
        // booked loss lands at 10% of capital, not 10%+costs+slippage. Same realized formula
        // (pnl) used at the real exit; live mirrors this via realized_pnl in manage_active.
        let t_ms = t.and_utc().timestamp_millis() as u64;
        if let Some((net, _g, _c)) = pnl(&plan.legs, entry_quotes, &quotes, plan.lots, t_ms) {
            if net <= -stop_rupees {
                return (t, format!("STOP ₹{stop_rupees:.0} (10% cap, net)"));
            }
        }
        if gain <= -STOP_FRAC_ML * plan.max_loss_unit {
            return (t, format!("STOP {:.0}%ML", STOP_FRAC_ML * 100.0));
        }
    }

    (t_exit, "15:15".to_string())
}

pub(crate) fn load_selling_ticks(path: &Path) -> Result<(NaiveDate, Vec<TickRow>), String> {
    let mut rdr = crate::open_csv(path).map_err(|e| e.to_string())?;
    let headers = rdr.headers().map_err(|e| e.to_string())?.clone();
    let idx = |name: &str| headers.iter().position(|h| h == name).ok_or_else(|| format!("missing col {name}"));
    let i_ts = idx("recv_ts")?;
    let i_strike = idx("strike")?;
    let i_ot = idx("option_type")?;
    let i_spot = idx("spot")?;
    let i_bid = idx("bid")?;
    let i_ask = idx("ask")?;
    let i_delta = idx("delta_mid")?;

    let mut rows = Vec::new();
    let mut day = None;
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
    Ok((day.ok_or("empty file")?, rows))
}

pub(crate) fn replay_day(rows: &[TickRow], day: NaiveDate, mut capital: f64) -> (MultilegDayResult, f64) {
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
    };

    // ponytail: env bypass for "what-if any weekday" backtests; live/default stays Mon/Tue.
    let any_weekday = std::env::var("SATA_ML_ANY_WEEKDAY").is_ok();
    if !any_weekday && !weekday_allows(day.weekday()) {
        return (
            MultilegDayResult {
                skip_reason: Some(format!("weekday {:?} not Mon/Tue", day.weekday())),
                ..base
            },
            capital,
        );
    }

    let t_entry = ts(day, "09:45");
    let t_open = ts(day, "09:15:05");

    let closes = minute_closes(rows, t_open, t_entry);
    let Some(er) = efficiency_ratio(&closes) else {
        return (
            MultilegDayResult {
                skip_reason: Some("insufficient morning ER data".into()),
                ..base
            },
            capital,
        );
    };

    let Some((spot, quotes)) = snapshot_at(rows, t_entry) else {
        return (
            MultilegDayResult {
                er: Some(er),
                skip_reason: Some("no entry snapshot".into()),
                ..base
            },
            capital,
        );
    };

    let Some(straddle) = atm_straddle(&quotes, spot) else {
        return (
            MultilegDayResult {
                er: Some(er),
                skip_reason: Some("no ATM straddle".into()),
                ..base
            },
            capital,
        );
    };

    let range_pts = closes.iter().cloned().fold(f64::NEG_INFINITY, f64::max)
        - closes.iter().cloned().fold(f64::INFINITY, f64::min);
    if let Some(why) = sell_regime_skip(er, range_pts, straddle) {
        return (
            MultilegDayResult {
                er: Some(er),
                skip_reason: Some(format!("{why} (ER {er:.2})")),
                ..base
            },
            capital,
        );
    }

    let edge_frac = {
        let spots: Vec<f64> = rows
            .iter()
            .filter(|r| r.ts >= t_open && r.ts <= t_entry && r.spot > 0.0)
            .map(|r| r.spot)
            .collect();
        if spots.len() < 2 {
            0.5
        } else {
            let min = spots.iter().cloned().fold(f64::INFINITY, f64::min);
            let max = spots.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
            let range = max - min;
            let pos = if range <= 0.0 {
                0.5
            } else {
                (spots.last().copied().unwrap_or(min) - min) / range
            };
            (pos - 0.5).abs() * 2.0
        }
    };

    let regime = OpeningRegime {
        er,
        range_pts,
        straddle,
        edge_frac,
    };

    let mut candidates: Vec<Plan> = Vec::new();
    let mut last_err = String::new();
    for structure in SellStructure::LADDER {
        let Some(legs) = select_legs(&quotes, spot, structure) else {
            last_err = format!("{structure:?} legs not seatable");
            continue;
        };
        let Some(credit) = combo_credit(&legs, &quotes) else {
            last_err = format!("{structure:?} no credit");
            continue;
        };
        let Some((range_pts, edge_frac, drift_frac)) = opening_latent(rows, day, t_entry, &legs, credit)
        else {
            last_err = format!("{structure:?} insufficient drift-latent data");
            continue;
        };
        match try_plan(
            structure,
            &quotes,
            spot,
            (range_pts, edge_frac, drift_frac),
            regime,
            capital,
        ) {
            Ok(p) => candidates.push(p),
            Err(e) => last_err = format!("{structure:?} {e}"),
        }
    }

    let scoreboard: Vec<(SellStructure, f64)> = candidates.iter().map(|p| (p.structure, p.score)).collect();
    let Some(best_s) = pick_best_structure(&scoreboard) else {
        return (
            MultilegDayResult {
                er: Some(er),
                skip_reason: Some(format!("no structure passed gates ({last_err})")),
                ..base
            },
            capital,
        );
    };
    let plan = candidates.into_iter().find(|p| p.structure == best_s).unwrap();

    let Some((_s, q_e)) = snapshot_at(rows, t_entry) else {
        return (
            MultilegDayResult {
                er: Some(er),
                skip_reason: Some("entry marks missing".into()),
                ..base
            },
            capital,
        );
    };
    let (t_out, why) = manage(rows, day, t_entry, &plan, capital, &q_e);
    let Some((_s, q_x)) = snapshot_at(rows, t_out) else {
        return (
            MultilegDayResult {
                er: Some(er),
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
                er: Some(er),
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
            er: Some(er),
            skip_reason: None,
            ..base
        },
        capital,
    )
}

pub fn replay_file(path: &Path, start_capital: f64) -> Result<MultilegReplaySummary, String> {
    let (day, rows) = load_selling_ticks(path)?;
    let (result, end) = replay_day(&rows, day, start_capital);
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
        let (day, rows) = load_selling_ticks(path)?;
        let (r, new_cap) = replay_day(&rows, day, cap);
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
    use chrono::Weekday;

    #[test]
    fn replay_module_compiles_and_weekday_gate_blocks_wed() {
        let day = NaiveDate::from_ymd_opt(2026, 6, 24).unwrap();
        assert_eq!(day.weekday(), Weekday::Wed);
        let (r, cap) = replay_day(&[], day, 15_000.0);
        assert!(!r.traded);
        assert!(r.skip_reason.as_ref().unwrap().contains("Wed"));
        assert_eq!(cap, 15_000.0);
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
            legs,
            credit: 40.0, // shorts 30 each − wings 10 each = +40 collected (matches the quotes below)
            max_loss_unit: 60.0,
            zone_width: 200.0,
            lots: 3,
            score: 1.0,
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

        let (t_out, why) = manage(&rows, day, t_entry, &plan, 15_000.0, &entry_quotes);
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
            legs,
            credit: 40.0,
            max_loss_unit: 60.0,
            zone_width: 200.0,
            lots: 3,
            score: 1.0,
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

        let (t_out, why) = manage(&rows, day, t_entry, &plan, 15_000.0, &entry_quotes);
        assert!(why.contains("net"), "stop must be the net-based 10% cap, got {why:?}");
        assert_eq!(t_out, t_entry + chrono::Duration::seconds(30));
    }
}
