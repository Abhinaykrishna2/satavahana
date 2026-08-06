//! Account-level risk shared by every engine.
//!
//! Each engine (options, microstructure, quant) keeps its own per-trade sizing and
//! its own local circuit, but they ALL consult one [`PortfolioCircuit`] before
//! opening a position and report realized (net-of-cost) P&L to it on close. This
//! makes the P&L circuit an **account-level** guarantee: "−15% / +25% of day-start
//! capital, then no more trades anywhere" — not a per-engine limit that the
//! combined book could breach N times over.
//!
//! ## Full capital + one global position lock (June 2026)
//!
//! Capital is **no longer split** into slices — each engine is sized off the FULL
//! pool so it can fund a proper ATM lot instead of being starved into deep-OTM
//! contracts. To stop the engines from each deploying the full pool at once (which
//! would double/triple-commit the same rupees onto the same correlated NIFTY bet),
//! the account holds **one global position lock**: at most ONE open position across
//! all real-order engines at a time. An engine must `try_claim` the slot before it
//! opens; if another engine already holds it, the new signal is cancelled. The
//! claim is **race-free** — the circuit check and the slot grab happen in a single
//! mutex critical section, so two engines evaluating concurrently can never both win.
//! The daily trade cap is account-wide as well: with `max_trades_per_day = 1`, a
//! completed multileg trade blocks same-day single-leg/micro re-entry.
//! Each engine is handed the full capital; co-deployment is prevented by the lock,
//! not by a static split.

use crate::risk::CircuitState;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// One account-wide circuit breaker + single-position lock shared by every engine.
#[derive(Debug)]
pub struct PortfolioCircuit {
    day_start_capital: f64,
    realized_pnl: f64,
    loss_pct: f64,
    profit_pct: f64,
    /// `Some(engine)` while a position is open anywhere; `None` when the slot is free.
    /// This is the global one-position lock — only the holder may release it.
    position_holder: Option<&'static str>,
    /// When the slot was claimed — used by the stale-claim watchdog ([`reap_stale`]).
    claimed_at: Option<Instant>,
    /// Completed trades today by engine holder (`options`, `multileg`, `micro`, ...).
    /// Summed for the account-wide daily trade cap.
    trades_by_holder: HashMap<&'static str, u32>,
    /// Hard account-wide cap on completed trades per day.
    /// `u32::MAX` = uncapped. Resets only on process restart (which happens daily via
    /// the stale-token re-login — confirm the engine restarts each trading day).
    max_trades_per_day: u32,
}

impl PortfolioCircuit {
    pub fn new(
        day_start_capital: f64,
        loss_pct: f64,
        profit_pct: f64,
        max_trades_per_day: u32,
    ) -> Self {
        PortfolioCircuit {
            day_start_capital: day_start_capital.max(0.0),
            realized_pnl: 0.0,
            loss_pct: loss_pct.max(0.0),
            profit_pct: profit_pct.max(0.0),
            position_holder: None,
            claimed_at: None,
            trades_by_holder: HashMap::new(),
            max_trades_per_day: max_trades_per_day.max(1),
        }
    }

    /// Add a closed trade's NET (post-cost) P&L from any engine.
    pub fn record(&mut self, net_pnl: f64) {
        self.record_for("account", net_pnl);
    }

    /// Add a closed trade's NET (post-cost) P&L from a specific holder, and count the
    /// trade toward that holder's daily cap. Called once per CLOSE — so a rejected/never-filled order
    /// (e.g. an IP-403'd entry) does NOT burn the day's trade, and a late-fill
    /// `promote` re-grab still sees this holder's count unchanged and can reacquire the lock.
    pub fn record_for(&mut self, holder: &'static str, net_pnl: f64) {
        if net_pnl.is_finite() {
            self.realized_pnl += net_pnl;
            let e = self.trades_by_holder.entry(holder).or_insert(0);
            *e = e.saturating_add(1);
        }
    }

    pub fn trades_today(&self) -> u32 {
        self.trades_by_holder.values().copied().sum()
    }
    pub fn trades_today_for(&self, holder: &'static str) -> u32 {
        self.trades_by_holder.get(holder).copied().unwrap_or(0)
    }
    pub fn daily_cap_reached(&self) -> bool {
        self.trades_today() >= self.max_trades_per_day
    }
    pub fn daily_cap_reached_for(&self, holder: &'static str) -> bool {
        self.trades_today_for(holder) >= self.max_trades_per_day
    }

    pub fn realized_pnl(&self) -> f64 {
        self.realized_pnl
    }
    pub fn day_start_capital(&self) -> f64 {
        self.day_start_capital
    }
    pub fn lower_threshold(&self) -> f64 {
        -(self.day_start_capital * self.loss_pct / 100.0)
    }
    pub fn upper_threshold(&self) -> f64 {
        self.day_start_capital * self.profit_pct / 100.0
    }

    pub fn state(&self) -> CircuitState {
        if self.realized_pnl <= self.lower_threshold() {
            CircuitState::LowerLoss
        } else if self.realized_pnl >= self.upper_threshold() {
            CircuitState::UpperProfit
        } else {
            CircuitState::Open
        }
    }

    pub fn can_enter(&self) -> bool {
        self.state() == CircuitState::Open
    }

    pub fn can_enter_holder(&self, _holder: &'static str) -> bool {
        self.can_enter() && !self.daily_cap_reached()
    }

    /// A human-legible reason new entries are blocked account-wide, or `None` if open.
    pub fn halt_reason(&self) -> Option<&'static str> {
        match self.state() {
            CircuitState::LowerLoss => Some("LOWER circuit — daily loss limit hit"),
            CircuitState::UpperProfit => Some("UPPER circuit — daily profit limit hit"),
            CircuitState::Open => None,
        }
    }

    /// A human-legible reason entries are blocked for a holder.
    pub fn halt_reason_for(&self, _holder: &'static str) -> Option<&'static str> {
        self.halt_reason().or_else(|| {
            self.daily_cap_reached()
                .then_some("daily account trade cap reached")
        })
    }

    // ─── Global single-position lock ────────────────────────────────────────
    //
    // RACE-FREE by construction: every method here runs while the caller holds the
    // one `Mutex<PortfolioCircuit>`, so the circuit check and the slot grab are a
    // single critical section. Two engines calling `try_claim` concurrently are
    // serialized by the mutex; the first sets `position_holder`, the second sees it
    // already set and is denied. There is no check-then-act window.

    /// Atomically claim the single global position slot. Succeeds only if the
    /// circuit is Open **and** no position is currently held. Returns `false` if
    /// another engine holds the slot or the circuit has tripped.
    pub fn try_claim(&mut self, holder: &'static str) -> bool {
        if self.position_holder.is_some() {
            return false; // someone already holds the one open-position slot
        }
        if !self.can_enter_holder(holder) {
            return false; // circuit tripped — no new entries anywhere
        }
        self.position_holder = Some(holder);
        self.claimed_at = Some(Instant::now());
        true
    }

    /// Release the slot. Only the current holder may release it (a stray release
    /// from a non-holder is a no-op, so engines can't free each other's positions).
    pub fn release(&mut self, holder: &'static str) {
        if self.position_holder == Some(holder) {
            self.position_holder = None;
            self.claimed_at = None;
        }
    }

    pub fn is_locked(&self) -> bool {
        self.position_holder.is_some()
    }

    pub fn position_holder(&self) -> Option<&'static str> {
        self.position_holder
    }

    /// Watchdog: force-release the slot if it has been held longer than `max_secs`
    /// (a safety net in case an engine misses a release on some abort path — a
    /// missing order-update would otherwise wedge the lock for the whole day).
    /// Returns the evicted holder, if any.
    pub fn reap_stale(&mut self, max_secs: u64) -> Option<&'static str> {
        if let (Some(h), Some(t)) = (self.position_holder, self.claimed_at) {
            if t.elapsed().as_secs() >= max_secs {
                self.position_holder = None;
                self.claimed_at = None;
                return Some(h);
            }
        }
        None
    }
}

/// Thread-safe handle every engine holds.
pub type SharedCircuit = Arc<Mutex<PortfolioCircuit>>;

pub fn new_shared(
    day_start_capital: f64,
    loss_pct: f64,
    profit_pct: f64,
    max_trades_per_day: u32,
) -> SharedCircuit {
    Arc::new(Mutex::new(PortfolioCircuit::new(
        day_start_capital,
        loss_pct,
        profit_pct,
        max_trades_per_day,
    )))
}

/// Legible reason new entries are halted account-wide (P&L circuit), or None.
pub fn halt_reason(c: &SharedCircuit) -> Option<&'static str> {
    c.lock().ok().and_then(|g| g.halt_reason())
}

/// Legible reason new entries are halted for one holder (P&L circuit or holder cap), or None.
pub fn halt_reason_for(c: &SharedCircuit, holder: &'static str) -> Option<&'static str> {
    c.lock().ok().and_then(|g| g.halt_reason_for(holder))
}

/// Whether a new entry is allowed account-wide by P&L circuit. **Fail-closed**: if the lock is
/// poisoned (a panicked engine), deny new entries rather than risk trading blind.
pub fn can_enter(c: &SharedCircuit) -> bool {
    c.lock().map(|g| g.can_enter()).unwrap_or(false)
}

/// Whether a new entry is allowed for one holder by the account P&L circuit and daily account cap.
pub fn can_enter_holder(c: &SharedCircuit, holder: &'static str) -> bool {
    c.lock()
        .map(|g| g.can_enter_holder(holder))
        .unwrap_or(false)
}

/// Record a closed trade's net P&L into the shared circuit.
pub fn record(c: &SharedCircuit, net_pnl: f64) {
    if let Ok(mut g) = c.lock() {
        g.record(net_pnl);
    }
}

/// Record a closed trade's net P&L for a holder into the shared circuit.
pub fn record_for(c: &SharedCircuit, holder: &'static str, net_pnl: f64) {
    if let Ok(mut g) = c.lock() {
        g.record_for(holder, net_pnl);
    }
}

pub fn state(c: &SharedCircuit) -> CircuitState {
    // Fail-closed: treat an unreadable circuit as tripped.
    c.lock()
        .map(|g| g.state())
        .unwrap_or(CircuitState::LowerLoss)
}

/// Atomically claim the one global position slot for `holder`. Race-free: the
/// circuit check + slot grab happen under a single mutex acquisition. **Fail-closed**:
/// a poisoned lock denies the claim (don't open a position we can't account for).
pub fn try_claim(c: &SharedCircuit, holder: &'static str) -> bool {
    c.lock().map(|mut g| g.try_claim(holder)).unwrap_or(false)
}

/// Release the global position slot held by `holder` (no-op if not the holder).
pub fn release(c: &SharedCircuit, holder: &'static str) {
    if let Ok(mut g) = c.lock() {
        g.release(holder);
    }
}

/// Whether the single global position slot is currently held. **Fail-closed**: an
/// unreadable lock reads as locked (refuse new entries rather than double-open).
pub fn is_locked(c: &SharedCircuit) -> bool {
    c.lock().map(|g| g.is_locked()).unwrap_or(true)
}

/// Watchdog sweep: force-release the slot if held longer than `max_secs`.
pub fn reap_stale(c: &SharedCircuit, max_secs: u64) -> Option<&'static str> {
    c.lock().ok().and_then(|mut g| g.reap_stale(max_secs))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn account_circuit_trips_at_minus_15_and_plus_25() {
        let mut c = PortfolioCircuit::new(100_000.0, 15.0, 25.0, u32::MAX);
        assert!(c.can_enter());
        c.record(-14_000.0);
        assert!(c.can_enter(), "-14% still open");
        c.record(-2_000.0); // -16k <= -15k
        assert_eq!(c.state(), CircuitState::LowerLoss);
        assert!(!c.can_enter());

        let mut up = PortfolioCircuit::new(100_000.0, 15.0, 25.0, u32::MAX);
        up.record(24_000.0);
        assert!(up.can_enter());
        up.record(2_000.0); // +26k >= +25k
        assert_eq!(up.state(), CircuitState::UpperProfit);
        assert!(!up.can_enter());
    }

    #[test]
    fn combined_pnl_across_engines_trips_one_account_circuit() {
        // Two engines each losing 10% would NOT trip a per-engine 15% limit, but
        // together they breach the shared account circuit.
        let c = new_shared(100_000.0, 15.0, 25.0, u32::MAX);
        record_for(&c, "options", -10_000.0); // engine A
        assert!(can_enter(&c));
        record_for(&c, "multileg", -6_000.0); // engine B -> combined -16% trips
        assert!(!can_enter(&c), "account circuit must trip on COMBINED loss");
    }

    #[test]
    fn daily_cap_is_account_wide() {
        let c = new_shared(100_000.0, 15.0, 25.0, 1);
        record_for(&c, "multileg", 1_000.0);
        assert!(
            !can_enter_holder(&c, "multileg"),
            "multi-leg used its one slot"
        );
        assert!(
            !can_enter_holder(&c, "options"),
            "single-leg must not trade after account daily cap"
        );
        assert_eq!(
            halt_reason_for(&c, "multileg"),
            Some("daily account trade cap reached")
        );
    }

    #[test]
    fn only_one_engine_can_hold_the_position_lock() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Barrier;
        use std::thread;

        let c = new_shared(100_000.0, 15.0, 25.0, u32::MAX);
        let n = 8usize;
        let barrier = Arc::new(Barrier::new(n));
        let wins = Arc::new(AtomicUsize::new(0));

        let handles: Vec<_> = (0..n)
            .map(|i| {
                let c = c.clone();
                let b = barrier.clone();
                let w = wins.clone();
                thread::spawn(move || {
                    let holder: &'static str = if i % 2 == 0 { "options" } else { "micro" };
                    b.wait(); // line everyone up so the claims truly race
                    if try_claim(&c, holder) {
                        w.fetch_add(1, Ordering::SeqCst);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        // Exactly one of eight racing threads may hold the single slot.
        assert_eq!(
            wins.load(Ordering::SeqCst),
            1,
            "exactly one engine wins the race"
        );
        assert!(is_locked(&c));

        // A held slot blocks every further claim, including from another engine.
        assert!(!try_claim(&c, "options"));
        assert!(!try_claim(&c, "micro"));

        // Only the holder's release frees it; then a fresh claim succeeds.
        let winner = c.lock().unwrap().position_holder().unwrap();
        release(
            &c,
            if winner == "options" {
                "micro"
            } else {
                "options"
            },
        ); // wrong holder: no-op
        assert!(is_locked(&c), "a non-holder release must not free the slot");
        release(&c, winner);
        assert!(!is_locked(&c));
        assert!(
            try_claim(&c, "options"),
            "after release the slot is claimable again"
        );
    }

    #[test]
    fn claim_is_denied_while_circuit_is_tripped() {
        let c = new_shared(100_000.0, 15.0, 25.0, u32::MAX);
        record_for(&c, "options", -16_000.0); // -16% trips the lower circuit
        assert!(
            !try_claim(&c, "options"),
            "no new position may open once the circuit trips"
        );
        assert!(!is_locked(&c));
    }

    #[test]
    fn stale_claim_is_reaped_by_the_watchdog() {
        let c = new_shared(100_000.0, 15.0, 25.0, u32::MAX);
        assert!(try_claim(&c, "options"));
        assert_eq!(reap_stale(&c, 3600), None, "fresh claim is not reaped");
        assert_eq!(
            reap_stale(&c, 0),
            Some("options"),
            "held >= 0s is force-released"
        );
        assert!(
            !is_locked(&c),
            "watchdog frees the slot for the next signal"
        );
    }
}
