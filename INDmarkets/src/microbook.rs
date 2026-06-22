//! Microstructure features from Kite top-5 depth snapshots.
//!
//! Kite Connect's "full" mode delivers a **top-5 aggregated depth snapshot** per
//! tick (~1 Hz), not an order-by-order stream — so there are no order IDs or
//! sequence numbers to reconstruct a true L2 book. This module therefore keeps a
//! per-token rolling view of the top-5 snapshot and derives:
//!
//!   * **OBI** — Order Book Imbalance: `(ΣbidQty − ΣaskQty) / (ΣbidQty + ΣaskQty)`
//!     over the top 5 levels, in `[-1, 1]`. Negative = sell-heavy (potential wall).
//!   * **OBI z-score** — OBI standardized over a rolling window, to flag *extreme*
//!     deviations rather than absolute levels.
//!   * **OFI** — Order Flow Imbalance (Cont–Kukanov–Stoikov), computed from
//!     consecutive top-of-book price/qty changes. Positive = aggressive buyers
//!     absorbing supply.
//!   * **Ask-queue depletion** — drop in total ask quantity per snapshot and its
//!     ratio to a rolling moving average (Strategy 2's queue-velocity trigger).
//!
//! All math is snapshot-relative and timezone-independent.

use crate::models::MarketDepth;
use std::collections::{HashMap, VecDeque};

const PRICE_EPS: f64 = 1e-4;

/// Features emitted for a single token on each depth update.
#[derive(Debug, Clone, Copy, Default)]
pub struct MicroFeatures {
    /// Raw top-5 order-book imbalance in `[-1, 1]`; negative = sell-heavy.
    pub obi: f64,
    /// Rolling z-score of `obi` (0.0 until the window has ≥2 samples).
    pub obi_z: f64,
    /// Cont–Stoikov order-flow imbalance for this snapshot (0.0 on the first one).
    pub ofi: f64,
    /// Rolling sum of recent `ofi` over the lookback window.
    pub ofi_sum: f64,
    /// Total ask quantity consumed since the previous snapshot (>= 0).
    pub ask_depletion: f64,
    /// `ask_depletion` divided by its rolling MA (0.0 until the MA is positive).
    pub ask_depletion_ratio: f64,
    pub best_bid: f64,
    pub best_ask: f64,
    pub mid: f64,
    pub spread: f64,
    /// Number of snapshots seen for this token (warmup gauge).
    pub samples: usize,
}

/// Sum of quantities across the top-5 bid levels.
pub fn sum_bid_qty(depth: &MarketDepth) -> u64 {
    depth.bids.iter().map(|d| d.quantity as u64).sum()
}

/// Sum of quantities across the top-5 ask levels.
pub fn sum_ask_qty(depth: &MarketDepth) -> u64 {
    depth.asks.iter().map(|d| d.quantity as u64).sum()
}

/// Raw top-5 order-book imbalance in `[-1, 1]`. 0.0 when both sides are empty.
pub fn raw_obi(depth: &MarketDepth) -> f64 {
    let bid = sum_bid_qty(depth) as f64;
    let ask = sum_ask_qty(depth) as f64;
    let tot = bid + ask;
    if tot <= 0.0 {
        0.0
    } else {
        (bid - ask) / tot
    }
}

/// Cont–Kukanov–Stoikov order-flow imbalance between two consecutive top-of-book
/// observations. Positive = net buy pressure.
///
/// ΔW (bid side):  +q_b        if bid price rose
///                  q_b − q_b' if bid price unchanged
///                 −q_b'       if bid price fell
/// ΔV (ask side):  −q_a'       if ask price rose
///                  q_a − q_a' if ask price unchanged
///                 +q_a        if ask price fell
/// OFI = ΔW − ΔV
pub fn ofi(
    prev_bid_px: f64,
    prev_bid_qty: f64,
    prev_ask_px: f64,
    prev_ask_qty: f64,
    bid_px: f64,
    bid_qty: f64,
    ask_px: f64,
    ask_qty: f64,
) -> f64 {
    let dw = if bid_px > prev_bid_px + PRICE_EPS {
        bid_qty
    } else if (bid_px - prev_bid_px).abs() <= PRICE_EPS {
        bid_qty - prev_bid_qty
    } else {
        -prev_bid_qty
    };
    let dv = if ask_px > prev_ask_px + PRICE_EPS {
        -prev_ask_qty
    } else if (ask_px - prev_ask_px).abs() <= PRICE_EPS {
        ask_qty - prev_ask_qty
    } else {
        ask_qty
    };
    dw - dv
}

#[derive(Debug, Clone)]
struct TokenState {
    has_prev: bool,
    prev_bid_px: f64,
    prev_bid_qty: f64,
    prev_ask_px: f64,
    prev_ask_qty: f64,
    prev_ask_total: f64,
    obi_window: VecDeque<f64>,
    ofi_window: VecDeque<f64>,
    depletion_window: VecDeque<f64>,
    samples: usize,
}

impl TokenState {
    fn new() -> Self {
        TokenState {
            has_prev: false,
            prev_bid_px: 0.0,
            prev_bid_qty: 0.0,
            prev_ask_px: 0.0,
            prev_ask_qty: 0.0,
            prev_ask_total: 0.0,
            obi_window: VecDeque::new(),
            ofi_window: VecDeque::new(),
            depletion_window: VecDeque::new(),
            samples: 0,
        }
    }
}

/// Maintains per-token microstructure state and emits [`MicroFeatures`] on each
/// depth update. Not internally synchronized — own it from a single task.
#[derive(Debug, Default)]
pub struct BookTracker {
    states: HashMap<u32, TokenState>,
    lookback: usize,
}

impl BookTracker {
    pub fn new(lookback: usize) -> Self {
        BookTracker {
            states: HashMap::new(),
            lookback: lookback.max(2),
        }
    }

    /// Feed a fresh top-5 depth snapshot for `token`; returns its current features.
    pub fn update(&mut self, token: u32, depth: &MarketDepth) -> MicroFeatures {
        let lookback = self.lookback;
        let st = self.states.entry(token).or_insert_with(TokenState::new);

        let best_bid = depth.bids[0].price;
        let best_ask = depth.asks[0].price;
        let bid_qty0 = depth.bids[0].quantity as f64;
        let ask_qty0 = depth.asks[0].quantity as f64;
        let ask_total = sum_ask_qty(depth) as f64;
        let obi = raw_obi(depth);

        let mut feat = MicroFeatures {
            obi,
            best_bid,
            best_ask,
            ..MicroFeatures::default()
        };
        feat.mid = if best_bid > 0.0 && best_ask > 0.0 {
            (best_bid + best_ask) / 2.0
        } else {
            best_bid.max(best_ask)
        };
        feat.spread = (best_ask - best_bid).max(0.0);

        if st.has_prev {
            feat.ofi = ofi(
                st.prev_bid_px,
                st.prev_bid_qty,
                st.prev_ask_px,
                st.prev_ask_qty,
                best_bid,
                bid_qty0,
                best_ask,
                ask_qty0,
            );
            feat.ask_depletion = (st.prev_ask_total - ask_total).max(0.0);
        }

        // Depletion ratio is measured against the PRIOR rolling average (excluding
        // this snapshot), so a sudden spike reads as "Nx the recent baseline".
        let dep_ma = mean(&st.depletion_window);
        feat.ask_depletion_ratio = if dep_ma > 0.0 {
            feat.ask_depletion / dep_ma
        } else {
            0.0
        };

        // Roll the windows.
        push_capped(&mut st.obi_window, obi, lookback);
        if st.has_prev {
            push_capped(&mut st.ofi_window, feat.ofi, lookback);
            push_capped(&mut st.depletion_window, feat.ask_depletion, lookback);
        }

        feat.obi_z = zscore(&st.obi_window, obi);
        feat.ofi_sum = st.ofi_window.iter().sum();

        // Advance state.
        st.prev_bid_px = best_bid;
        st.prev_bid_qty = bid_qty0;
        st.prev_ask_px = best_ask;
        st.prev_ask_qty = ask_qty0;
        st.prev_ask_total = ask_total;
        st.has_prev = true;
        st.samples += 1;
        feat.samples = st.samples;

        feat
    }

    /// Whether `token` has accumulated at least `n` snapshots (warmup check).
    pub fn is_warm(&self, token: u32, n: usize) -> bool {
        self.states.get(&token).map(|s| s.samples >= n).unwrap_or(false)
    }
}

fn push_capped(window: &mut VecDeque<f64>, v: f64, cap: usize) {
    window.push_back(v);
    while window.len() > cap {
        window.pop_front();
    }
}

fn mean(window: &VecDeque<f64>) -> f64 {
    if window.is_empty() {
        0.0
    } else {
        window.iter().sum::<f64>() / window.len() as f64
    }
}

fn zscore(window: &VecDeque<f64>, value: f64) -> f64 {
    let n = window.len();
    if n < 2 {
        return 0.0;
    }
    let m = mean(window);
    let var = window.iter().map(|x| (x - m) * (x - m)).sum::<f64>() / n as f64;
    let sd = var.sqrt();
    if sd <= 1e-9 {
        0.0
    } else {
        (value - m) / sd
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::DepthEntry;

    fn depth(bids: &[(f64, u32)], asks: &[(f64, u32)]) -> MarketDepth {
        let mut d = MarketDepth::default();
        for (i, (p, q)) in bids.iter().enumerate().take(5) {
            d.bids[i] = DepthEntry { price: *p, quantity: *q, orders: 1 };
        }
        for (i, (p, q)) in asks.iter().enumerate().take(5) {
            d.asks[i] = DepthEntry { price: *p, quantity: *q, orders: 1 };
        }
        d
    }

    #[test]
    fn obi_zero_when_balanced_and_signed_correctly() {
        let balanced = depth(&[(100.0, 50)], &[(101.0, 50)]);
        assert!((raw_obi(&balanced)).abs() < 1e-9);

        // Heavy ask wall => strongly negative OBI.
        let sell_wall = depth(&[(100.0, 30)], &[(101.0, 70)]);
        assert!((raw_obi(&sell_wall) - (-0.4)).abs() < 1e-9);

        // Heavy bid => positive OBI.
        let buy_heavy = depth(&[(100.0, 80)], &[(101.0, 20)]);
        assert!(raw_obi(&buy_heavy) > 0.5);
    }

    #[test]
    fn ofi_positive_when_buyers_lift_and_negative_when_sellers_hit() {
        // Ask price falls and bid qty grows => buyers aggressive => positive OFI.
        let up = ofi(100.0, 10.0, 101.0, 10.0, 100.0, 25.0, 100.5, 8.0);
        assert!(up > 0.0, "expected positive OFI, got {up}");

        // Bid falls away, ask grows => sell pressure => negative OFI.
        let down = ofi(100.0, 30.0, 101.0, 10.0, 99.5, 5.0, 101.0, 40.0);
        assert!(down < 0.0, "expected negative OFI, got {down}");
    }

    #[test]
    fn tracker_emits_extreme_negative_obi_for_sell_wall() {
        let mut t = BookTracker::new(10);
        let f = t.update(1, &depth(&[(100.0, 20)], &[(101.0, 80)]));
        assert!(f.obi <= -0.4, "sell wall OBI should be <= -0.4, got {}", f.obi);
        assert_eq!(f.samples, 1);
        // First snapshot has no flow yet.
        assert_eq!(f.ofi, 0.0);
    }

    #[test]
    fn tracker_detects_ask_depletion_velocity() {
        let mut t = BookTracker::new(10);
        // Build a steady small-depletion baseline.
        t.update(7, &depth(&[(100.0, 50)], &[(101.0, 100)]));
        t.update(7, &depth(&[(100.0, 50)], &[(101.0, 98)])); // -2
        t.update(7, &depth(&[(100.0, 50)], &[(101.0, 96)])); // -2
        // Sudden large consumption.
        let f = t.update(7, &depth(&[(100.0, 50)], &[(101.0, 60)])); // -36
        assert!(f.ask_depletion >= 36.0);
        assert!(
            f.ask_depletion_ratio > 5.0,
            "expected depletion ratio > 5x MA, got {}",
            f.ask_depletion_ratio
        );
    }

    #[test]
    fn obi_zscore_flags_deviation_from_recent_regime() {
        let mut t = BookTracker::new(20);
        // Establish a balanced regime.
        for _ in 0..10 {
            t.update(3, &depth(&[(100.0, 50)], &[(101.0, 50)]));
        }
        // Inject a sell wall: OBI drops well below the recent mean => negative z.
        let f = t.update(3, &depth(&[(100.0, 20)], &[(101.0, 80)]));
        assert!(f.obi_z < -1.0, "expected strongly negative z-score, got {}", f.obi_z);
    }
}
