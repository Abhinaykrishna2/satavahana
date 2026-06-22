//! Strategy 2 — Tuesday Nifty ATM gamma squeeze (options).
//!
//! Trades NIFTY weekly options on Tuesday expiries (revised lot size 65). Rather
//! than read the index chart, it watches the *velocity of ATM ask-queue depletion*
//! on the top-5 depth: when the resting sellers at the ATM strike are consumed far
//! faster than their recent average, a gamma move is often underway before it
//! shows on spot — so we buy the option to ride the explosive delta/gamma.
//!
//! Kite gives ~1 Hz top-5 snapshots, so "5x faster" is measured per-snapshot
//! against a rolling average (see `microbook::MicroFeatures::ask_depletion_ratio`).
//! Pure function of features; rolling state lives in the `BookTracker`.

use super::{InstrumentKind, PositionSide, Signal, StrategyKind};
use crate::microbook::MicroFeatures;

#[derive(Debug, Clone)]
pub struct GammaParams {
    /// ATM ask-depletion must exceed this multiple of its rolling MA. Default 5.0.
    pub depletion_multiple: f64,
    /// Minimum snapshots before the token is considered warm.
    pub min_samples: usize,
    /// Protective stop as a fraction of the option premium (default 25%).
    pub stop_frac: f64,
    /// Target as an R-multiple of the stop distance (default 1.2).
    pub target_r: f64,
    /// Max lots the strategy permits (risk budget / affordability may size lower).
    pub max_lots: u32,
    /// Restrict entries to Tuesdays (Nifty weekly expiry day). Default true.
    pub tuesday_only: bool,
}

impl Default for GammaParams {
    fn default() -> Self {
        GammaParams {
            depletion_multiple: 5.0,
            min_samples: 8,
            stop_frac: 0.25,
            target_r: 1.2,
            max_lots: 10,
            tuesday_only: true,
        }
    }
}

/// Per-token metadata for an ATM option leg.
#[derive(Debug, Clone)]
pub struct GammaMeta {
    pub token: u32,
    /// Full option tradingsymbol, e.g. "NIFTY2561724500CE".
    pub symbol: String,
    pub lot_size: u32,
}

/// Evaluate the gamma trigger on one ATM option leg. Returns a long (buy) signal
/// when ATM ask-queue depletion spikes past the configured multiple; else `None`.
pub fn evaluate(
    f: &MicroFeatures,
    p: &GammaParams,
    meta: &GammaMeta,
    is_tuesday: bool,
    ts_ms: u64,
) -> Option<Signal> {
    if p.tuesday_only && !is_tuesday {
        return None;
    }
    if f.samples < p.min_samples {
        return None;
    }
    if f.best_bid <= 0.0 || f.best_ask <= 0.0 || f.mid <= 0.0 {
        return None;
    }
    // Queue consumed far faster than its rolling average => gamma move building.
    if f.ask_depletion_ratio < p.depletion_multiple {
        return None;
    }

    Some(Signal {
        token: meta.token,
        symbol: meta.symbol.clone(),
        exchange: "NFO".to_string(),
        product: "NRML".to_string(),
        kind: StrategyKind::GammaSqueeze,
        instrument: InstrumentKind::Option,
        side: PositionSide::Long,
        lot_size: meta.lot_size,
        max_lots: p.max_lots,
        ref_price: f.mid,
        stop_frac: p.stop_frac,
        target_r: p.target_r,
        rationale: format!(
            "ATM ask-depletion {:.1}x MA (>= {:.1}x); premium {:.2}",
            f.ask_depletion_ratio, p.depletion_multiple, f.mid
        ),
        ts_ms,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta() -> GammaMeta {
        GammaMeta {
            token: 500,
            symbol: "NIFTY2561724500CE".into(),
            lot_size: 65,
        }
    }

    fn feat(depletion_ratio: f64, samples: usize) -> MicroFeatures {
        MicroFeatures {
            ask_depletion_ratio: depletion_ratio,
            best_bid: 99.5,
            best_ask: 100.5,
            mid: 100.0,
            samples,
            ..MicroFeatures::default()
        }
    }

    #[test]
    fn fires_on_tuesday_with_queue_spike() {
        let p = GammaParams::default();
        let s = evaluate(&feat(6.0, 10), &p, &meta(), true, 1_000).expect("should fire");
        assert_eq!(s.kind, StrategyKind::GammaSqueeze);
        assert_eq!(s.side, PositionSide::Long);
        assert_eq!(s.lot_size, 65);
        assert_eq!(s.exchange, "NFO");
    }

    #[test]
    fn no_fire_on_non_tuesday() {
        let p = GammaParams::default();
        assert!(evaluate(&feat(9.0, 10), &p, &meta(), false, 1).is_none());
    }

    #[test]
    fn no_fire_below_depletion_multiple() {
        let p = GammaParams::default();
        assert!(evaluate(&feat(3.0, 10), &p, &meta(), true, 1).is_none());
    }

    #[test]
    fn no_fire_during_warmup() {
        let p = GammaParams::default();
        assert!(evaluate(&feat(9.0, 3), &p, &meta(), true, 1).is_none());
    }

    #[test]
    fn tuesday_gate_can_be_disabled() {
        let mut p = GammaParams::default();
        p.tuesday_only = false;
        assert!(evaluate(&feat(6.0, 10), &p, &meta(), false, 1).is_some());
    }
}
