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

fn last_underlying_spot(rows: &[TickRow], t: NaiveDateTime) -> Option<f64> {
    rows.iter()
        .rev()
        .find(|r| r.ts <= t && r.spot.is_finite() && r.spot > 0.0)
        .map(|r| r.spot)
}

fn minute_spot_series(rows: &[TickRow], t0: NaiveDateTime, t1: NaiveDateTime) -> Vec<(NaiveDateTime, f64)> {
    let mut out = Vec::new();
    let mut h = t0.hour();
    let mut m = t0.minute();
    let mut last = last_underlying_spot(rows, t0).unwrap_or(0.0);
    loop {
        let t = t0
            .date()
            .and_time(NaiveTime::from_hms_opt(h, m, 0).unwrap());
        if t > t1 {
            break;
        }
        if t >= t0 {
            if let Some(s) = last_underlying_spot(rows, t) {
                last = s;
            }
            if last > 0.0 {
                out.push((t, last));
            }
        }
        if h == t1.hour() && m == t1.minute() {
            break;
        }
        m += 1;
        if m >= 60 {
            m = 0;
            h += 1;
        }
    }
    out
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
) -> (NaiveDateTime, String) {
    let t_exit = ts(day, "15:15");
    let mut peak_gain = 0.0_f64;
    let stop_rupees = STOP_FRAC_CAP * capital;
    let minute_spots = minute_spot_series(rows, t_entry, t_exit);

    for (i, (t, spot_now)) in minute_spots.iter().enumerate() {
        if *t <= t_entry {
            continue;
        }
        let Some((_spot, quotes)) = snapshot_at(rows, *t) else {
            continue;
        };
        let close_cost = combo_close_cost(&plan.legs, &quotes).unwrap_or(plan.credit);
        let gain = plan.credit - close_cost;

        // Rolling calendar-minute move on underlying spot (matches live `MOVE_WINDOW_MIN`).
        if let Some((t_prev, spot_prev)) = minute_spots
            .iter()
            .rev()
            .find(|(ts, _)| *ts <= *t - chrono::Duration::minutes(MOVE_WINDOW_MIN as i64))
        {
            let move_pts = (spot_now - spot_prev).abs();
            if move_trims(move_pts, *spot_now, plan.zone_width, plan.structure) {
                return (
                    *t,
                    format!(
                        "MOVE {:.2}%/{MOVE_WINDOW_MIN}m",
                        move_pts / spot_now * 100.0
                    ),
                );
            }
            let _ = (i, t_prev);
        }

        if gain >= TARGET_FRAC * plan.credit {
            return (*t, format!("TARGET {:.0}%", TARGET_FRAC * 100.0));
        }

        peak_gain = peak_gain.max(gain);
        if trail_exits(gain, peak_gain, plan.credit) {
            return (
                *t,
                format!(
                    "TRAIL +{:.0}%",
                    crate::multileg::TRAIL_FRAC * peak_gain / plan.credit * 100.0
                ),
            );
        }

        if gain * LOT_SIZE as f64 * plan.lots as f64 <= -stop_rupees {
            return (*t, format!("STOP ₹{stop_rupees:.0} (10% cap)"));
        }
        if gain <= -STOP_FRAC_ML * plan.max_loss_unit {
            return (*t, format!("STOP {:.0}%ML", STOP_FRAC_ML * 100.0));
        }
    }

    (t_exit, "15:15".to_string())
}

pub(crate) fn load_selling_ticks(path: &Path) -> Result<(NaiveDate, Vec<TickRow>), String> {
    let mut rdr = csv::Reader::from_path(path).map_err(|e| e.to_string())?;
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

    if !weekday_allows(day.weekday()) {
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

    let (t_out, why) = manage(rows, day, t_entry, &plan, capital);
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
}
