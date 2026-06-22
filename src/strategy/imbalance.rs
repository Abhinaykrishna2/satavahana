//! Strategy 1 — Micro-Price Transient Imbalance (equity MIS).
//!
//! Logic (Kite-adapted): on the top-5 depth, a heavy resting ask relative to bids
//! drives OBI strongly negative ("sell wall"). If, at the same time, order-flow
//! imbalance (OFI) turns positive — aggressive buyers lifting / absorbing the
//! offer — we enter long, anticipating the wall thins and price reverts up.
//!
//! We cannot see order IDs on Kite's aggregated feed, so this is an imbalance-
//! reversion entry, not literal spoof detection. Pure function of the current
//! [`MicroFeatures`]; all rolling state lives in the `BookTracker`.

use super::{InstrumentKind, PositionSide, Signal, StrategyKind};
use crate::microbook::MicroFeatures;

#[derive(Debug, Clone)]
pub struct ImbalanceParams {
    /// Raw OBI (in [-1,1]) at/below which sell pressure is "extreme". Default -0.4.
    pub obi_threshold: f64,
    /// Require OFI to be positive (buyers absorbing) to confirm the entry.
    pub require_ofi_positive: bool,
    /// Minimum snapshots before the token is considered warm.
    pub min_samples: usize,
    /// Protective stop as a fraction of price (default 0.4%).
    pub stop_frac: f64,
    /// Target as an R-multiple of the stop distance (default 1.5).
    pub target_r: f64,
    /// Max shares/lots the strategy permits (risk budget may size lower).
    pub max_lots: u32,
}

impl Default for ImbalanceParams {
    fn default() -> Self {
        ImbalanceParams {
            obi_threshold: -0.4,
            require_ofi_positive: true,
            min_samples: 5,
            stop_frac: 0.004,
            target_r: 1.5,
            max_lots: 100_000,
        }
    }
}

/// Per-token metadata the strategy needs to build a routable signal.
#[derive(Debug, Clone)]
pub struct EquityMeta {
    pub token: u32,
    pub symbol: String,
    pub exchange: String, // "NSE"
    pub product: String,  // "MIS"
}

/// Evaluate the imbalance trigger. Returns a long entry signal when an extreme
/// sell wall is being absorbed by positive flow; otherwise `None`.
pub fn evaluate(
    f: &MicroFeatures,
    p: &ImbalanceParams,
    meta: &EquityMeta,
    ts_ms: u64,
) -> Option<Signal> {
    if f.samples < p.min_samples {
        return None;
    }
    // Need a usable two-sided book.
    if f.best_bid <= 0.0 || f.best_ask <= 0.0 || f.mid <= 0.0 {
        return None;
    }
    // Extreme negative OBI = heavy resting ask ("sell wall").
    if f.obi > p.obi_threshold {
        return None;
    }
    // Confirmation: aggressive buyers absorbing the offer (positive flow).
    if p.require_ofi_positive && !(f.ofi > 0.0 || f.ofi_sum > 0.0) {
        return None;
    }

    Some(Signal {
        token: meta.token,
        symbol: meta.symbol.clone(),
        exchange: meta.exchange.clone(),
        product: meta.product.clone(),
        kind: StrategyKind::EquityImbalance,
        instrument: InstrumentKind::Equity,
        side: PositionSide::Long,
        lot_size: 1,
        max_lots: p.max_lots,
        ref_price: f.mid,
        stop_frac: p.stop_frac,
        target_r: p.target_r,
        rationale: format!(
            "OBI {:.2} (z {:.2}) <= {:.2} sell-wall; OFI {:+.0} (Σ {:+.0}) absorbing; mid {:.2}",
            f.obi, f.obi_z, p.obi_threshold, f.ofi, f.ofi_sum, f.mid
        ),
        ts_ms,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta() -> EquityMeta {
        EquityMeta {
            token: 100,
            symbol: "INFY".into(),
            exchange: "NSE".into(),
            product: "MIS".into(),
        }
    }

    fn feat(obi: f64, ofi: f64, samples: usize) -> MicroFeatures {
        MicroFeatures {
            obi,
            ofi,
            ofi_sum: ofi,
            best_bid: 99.95,
            best_ask: 100.05,
            mid: 100.0,
            samples,
            ..MicroFeatures::default()
        }
    }

    #[test]
    fn fires_on_sell_wall_with_positive_flow() {
        let p = ImbalanceParams::default();
        let s = evaluate(&feat(-0.5, 30.0, 10), &p, &meta(), 1_000).expect("should fire");
        assert_eq!(s.side, PositionSide::Long);
        assert_eq!(s.kind, StrategyKind::EquityImbalance);
        assert!(s.stop_price() < s.ref_price);
        assert!(s.target_price() > s.ref_price);
    }

    #[test]
    fn no_fire_without_extreme_obi() {
        let p = ImbalanceParams::default();
        assert!(evaluate(&feat(-0.1, 30.0, 10), &p, &meta(), 1).is_none());
    }

    #[test]
    fn no_fire_without_flow_confirmation() {
        let p = ImbalanceParams::default();
        assert!(evaluate(&feat(-0.6, -20.0, 10), &p, &meta(), 1).is_none());
    }

    #[test]
    fn no_fire_during_warmup() {
        let p = ImbalanceParams::default();
        assert!(evaluate(&feat(-0.6, 30.0, 2), &p, &meta(), 1).is_none());
    }
}
