//! Risk rails for the live microstructure engine.
//!
//! Three always-on guardrails, mandated by the spec and SEBI intraday rules:
//!   1. **3:15 PM IST flatten** — cancel all pending orders and square off open
//!      positions ahead of Zerodha's 3:20 (equity) / 3:26 (F&O) auto-square,
//!      which levies a ₹50 + GST penalty per order.
//!   2. **Per-trade hard stop** — at most `per_trade_stop_pct`% of the *currently
//!      polled* capital may be lost on any single trade.
//!   3. **Daily kill switch** — if realized P&L draws down `daily_kill_pct`% of the
//!      day-start capital, no new entries are allowed for the rest of the session.
//!
//! This module is pure (no I/O) so it is unit-testable; the orchestration layer
//! polls the clock and feeds realized P&L / capital updates in.

use chrono::{DateTime, FixedOffset, Timelike, Utc};

/// India Standard Time offset (UTC+5:30).
pub fn ist_offset() -> FixedOffset {
    FixedOffset::east_opt(5 * 3600 + 30 * 60).expect("valid IST offset")
}

/// Current wall-clock time in IST, independent of the machine's local timezone.
pub fn ist_now() -> DateTime<FixedOffset> {
    Utc::now().with_timezone(&ist_offset())
}

/// Parse an "HH:MM" string into (hour, minute). Returns None on malformed input.
pub fn parse_hhmm(s: &str) -> Option<(u32, u32)> {
    let (h, m) = s.trim().split_once(':')?;
    let h: u32 = h.trim().parse().ok()?;
    let m: u32 = m.trim().parse().ok()?;
    if h < 24 && m < 60 {
        Some((h, m))
    } else {
        None
    }
}

/// Minutes since IST midnight for a timestamp.
pub fn ist_minutes(dt: DateTime<FixedOffset>) -> u32 {
    dt.hour() * 60 + dt.minute()
}

/// True when `now` is at or past the given "HH:MM" IST wall-clock time.
/// A malformed `hhmm` is treated as "never" (returns false) so a typo cannot
/// accidentally trip the flatten early.
pub fn is_at_or_past(now: DateTime<FixedOffset>, hhmm: &str) -> bool {
    match parse_hhmm(hhmm) {
        Some((h, m)) => ist_minutes(now) >= h * 60 + m,
        None => false,
    }
}

/// Long (bought to open) or short (sold to open).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PositionSide {
    Long,
    Short,
}

/// Why new entries are currently halted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    Open,
    /// Lower circuit — day's net loss breached the limit.
    LowerLoss,
    /// Upper circuit — day's net gain breached the limit (anti-overtrading).
    UpperProfit,
}

/// Tracks capital, realized P&L, and the daily lower/upper circuit breakers.
/// All P&L fed in is NET of broker fees and transaction costs, so the circuits
/// are cost-inclusive by construction.
#[derive(Debug, Clone)]
pub struct RiskManager {
    /// Capital at session start — baseline for the daily circuits.
    day_start_capital: f64,
    /// Most recent hourly-polled capital — basis for per-trade sizing/stop.
    capital: f64,
    /// Cumulative realized P&L (net of costs) of closed trades this session.
    realized_pnl: f64,
    per_trade_stop_pct: f64,
    /// Lower circuit: halt when realized loss reaches this % of day-start capital.
    daily_loss_circuit_pct: f64,
    /// Upper circuit: halt when realized gain reaches this % of day-start capital.
    daily_profit_circuit_pct: f64,
}

impl RiskManager {
    pub fn new(
        starting_capital: f64,
        per_trade_stop_pct: f64,
        daily_loss_circuit_pct: f64,
        daily_profit_circuit_pct: f64,
    ) -> Self {
        let cap = starting_capital.max(0.0);
        RiskManager {
            day_start_capital: cap,
            capital: cap,
            realized_pnl: 0.0,
            per_trade_stop_pct,
            daily_loss_circuit_pct,
            daily_profit_circuit_pct,
        }
    }

    pub fn capital(&self) -> f64 {
        self.capital
    }

    pub fn day_start_capital(&self) -> f64 {
        self.day_start_capital
    }

    pub fn realized_pnl(&self) -> f64 {
        self.realized_pnl
    }

    /// Update the live-polled capital (hourly sync). Does not move the day-start
    /// baseline used by the daily kill switch.
    pub fn update_capital(&mut self, funds: f64) {
        if funds.is_finite() && funds >= 0.0 {
            self.capital = funds;
        }
    }

    /// Record the realized P&L of a closed trade (signed: gains positive).
    pub fn record_realized(&mut self, pnl: f64) {
        if pnl.is_finite() {
            self.realized_pnl += pnl;
        }
    }

    /// Maximum rupee loss permitted on a single trade = per_trade_stop_pct% of
    /// the currently-polled capital.
    pub fn risk_budget(&self) -> f64 {
        self.capital * self.per_trade_stop_pct / 100.0
    }

    /// Lower-circuit loss threshold (a negative number).
    pub fn lower_circuit_threshold(&self) -> f64 {
        -(self.day_start_capital * self.daily_loss_circuit_pct / 100.0)
    }

    /// Upper-circuit profit threshold (a positive number).
    pub fn upper_circuit_threshold(&self) -> f64 {
        self.day_start_capital * self.daily_profit_circuit_pct / 100.0
    }

    /// Current circuit state based on realized (net-of-cost) P&L.
    pub fn circuit_state(&self) -> CircuitState {
        if self.realized_pnl <= self.lower_circuit_threshold() {
            CircuitState::LowerLoss
        } else if self.realized_pnl >= self.upper_circuit_threshold() {
            CircuitState::UpperProfit
        } else {
            CircuitState::Open
        }
    }

    /// True once either daily circuit has tripped.
    pub fn circuit_tripped(&self) -> bool {
        self.circuit_state() != CircuitState::Open
    }

    /// Whether a new entry is permitted (no circuit tripped).
    pub fn can_enter(&self) -> bool {
        !self.circuit_tripped()
    }

    /// Largest position quantity whose worst-case loss to `stop_price` stays within
    /// the per-trade risk budget. `entry` and `stop_price` are per-unit prices.
    pub fn max_qty_for_stop(&self, entry: f64, stop_price: f64) -> u32 {
        let per_unit_risk = (entry - stop_price).abs();
        if per_unit_risk <= f64::EPSILON {
            return 0;
        }
        (self.risk_budget() / per_unit_risk).floor().max(0.0) as u32
    }

    /// Hard stop price for a position of `qty` units opened at `entry`, sized so the
    /// loss at the stop equals the per-trade risk budget. Used when quantity is fixed
    /// (e.g. one option lot) and we derive the stop instead of the size.
    pub fn stop_price(&self, entry: f64, qty: u32, side: PositionSide) -> f64 {
        if qty == 0 {
            return entry;
        }
        let dist = self.risk_budget() / qty as f64;
        match side {
            PositionSide::Long => (entry - dist).max(0.0),
            PositionSide::Short => entry + dist,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn ist(h: u32, m: u32) -> DateTime<FixedOffset> {
        ist_offset()
            .with_ymd_and_hms(2026, 6, 16, h, m, 0)
            .single()
            .unwrap()
    }

    #[test]
    fn ist_offset_is_utc_plus_530_regardless_of_machine_tz() {
        // +5:30 == 19800 seconds east. This is fixed and does not read the
        // machine's local timezone, so it is correct on an EST host.
        assert_eq!(ist_offset().local_minus_utc(), 5 * 3600 + 30 * 60);
        // ist_now() must equal the same absolute instant as Utc::now() shifted +5:30.
        let utc = Utc::now();
        let ist = utc.with_timezone(&ist_offset());
        assert_eq!(utc.timestamp(), ist.timestamp());
    }

    #[test]
    fn parse_hhmm_valid_and_invalid() {
        assert_eq!(parse_hhmm("15:15"), Some((15, 15)));
        assert_eq!(parse_hhmm("09:14"), Some((9, 14)));
        assert_eq!(parse_hhmm("24:00"), None);
        assert_eq!(parse_hhmm("9:60"), None);
        assert_eq!(parse_hhmm("nonsense"), None);
    }

    #[test]
    fn flatten_clock_triggers_at_or_after_time() {
        assert!(!is_at_or_past(ist(15, 14), "15:15"));
        assert!(is_at_or_past(ist(15, 15), "15:15"));
        assert!(is_at_or_past(ist(15, 20), "15:15"));
        // Malformed time never trips.
        assert!(!is_at_or_past(ist(23, 59), "bad"));
    }

    #[test]
    fn risk_budget_is_one_percent_of_polled_capital() {
        let rm = RiskManager::new(5000.0, 1.0, 15.0, 35.0);
        assert!((rm.risk_budget() - 50.0).abs() < 1e-9);
    }

    #[test]
    fn hourly_capital_update_changes_sizing_not_daily_baseline() {
        let mut rm = RiskManager::new(5000.0, 1.0, 15.0, 35.0);
        rm.update_capital(8000.0);
        // Per-trade budget tracks live capital...
        assert!((rm.risk_budget() - 80.0).abs() < 1e-9);
        // ...but the daily circuit baseline stays at day-start (15% of 5000).
        assert!((rm.lower_circuit_threshold() - (-750.0)).abs() < 1e-9);
    }

    #[test]
    fn lower_circuit_trips_on_15pct_net_loss() {
        let mut rm = RiskManager::new(5000.0, 1.0, 15.0, 35.0); // lower -750
        assert!(rm.can_enter());
        rm.record_realized(-700.0);
        assert!(rm.can_enter()); // -700 > -750
        rm.record_realized(-60.0); // total -760 <= -750
        assert_eq!(rm.circuit_state(), CircuitState::LowerLoss);
        assert!(!rm.can_enter());
    }

    #[test]
    fn upper_circuit_trips_on_35pct_net_gain() {
        let mut rm = RiskManager::new(5000.0, 1.0, 15.0, 35.0); // upper +1750
        rm.record_realized(1700.0);
        assert!(rm.can_enter()); // +1700 < +1750
        rm.record_realized(60.0); // total +1760 >= +1750
        assert_eq!(rm.circuit_state(), CircuitState::UpperProfit);
        assert!(!rm.can_enter(), "no overtrading a great day");
    }

    #[test]
    fn stop_price_caps_loss_at_budget() {
        let rm = RiskManager::new(5000.0, 1.0, 15.0, 35.0); // budget ₹50
        // One NIFTY lot (65) long at ₹100: stop distance = 50/65 ≈ 0.769
        let stop = rm.stop_price(100.0, 65, PositionSide::Long);
        let loss = (100.0 - stop) * 65.0;
        assert!((loss - 50.0).abs() < 1e-6);
        assert!(stop < 100.0);
    }

    #[test]
    fn max_qty_for_stop_respects_budget() {
        let rm = RiskManager::new(5000.0, 1.0, 15.0, 35.0); // budget ₹50
        // 0.50 per-unit risk => 100 units max (50 / 0.5).
        assert_eq!(rm.max_qty_for_stop(200.0, 199.5), 100);
        assert_eq!(rm.max_qty_for_stop(200.0, 200.0), 0); // zero distance => no size
    }
}
