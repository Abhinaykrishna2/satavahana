//! Microstructure strategies and their shared signal vocabulary.
//!
//! These two strategies are NEW and run only in the live microstructure engine.
//! They sit alongside — and never replace — the existing `options_engine` and
//! `quant_engine` strategies, which continue to run on the default pipeline path.
//!
//!   * [`imbalance`] — Strategy 1: Micro-Price Transient Imbalance (equity MIS).
//!   * [`gamma`]     — Strategy 2: Tuesday Nifty ATM gamma squeeze.

pub mod gamma;
pub mod imbalance;

pub use crate::risk::PositionSide;

/// Which microstructure strategy produced a signal (for cost model + logging).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StrategyKind {
    /// Strategy 1 — equity order-book imbalance.
    EquityImbalance,
    /// Strategy 2 — Tuesday Nifty ATM gamma squeeze.
    GammaSqueeze,
}

impl StrategyKind {
    pub fn label(self) -> &'static str {
        match self {
            StrategyKind::EquityImbalance => "EQ_IMBALANCE",
            StrategyKind::GammaSqueeze => "GAMMA_SQUEEZE",
        }
    }
}

/// Instrument class — selects the transaction-cost model and sizing unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstrumentKind {
    /// Sized in shares; equity intraday (MIS) cost model.
    Equity,
    /// Sized in lots of `lot_size`; index-options cost model.
    Option,
}

/// A trade intent emitted by a strategy. The OMS turns it into a (chased) limit
/// order, sizes it within the risk budget, and manages its lifecycle.
#[derive(Debug, Clone)]
pub struct Signal {
    pub token: u32,
    pub symbol: String,
    /// Order-routing exchange: "NSE" (equity) or "NFO" (options).
    pub exchange: String,
    /// Order product: "MIS" (equity intraday) or "NRML" (options).
    pub product: String,
    pub kind: StrategyKind,
    pub instrument: InstrumentKind,
    /// Entry direction. Both current strategies are buy-to-open (Long).
    pub side: PositionSide,
    /// Shares per lot (1 for equity; e.g. 65 for NIFTY weekly).
    pub lot_size: u32,
    /// Upper bound on lots/units the strategy permits (risk budget may size lower).
    pub max_lots: u32,
    /// Price observed when the signal fired (used for staleness checks).
    pub ref_price: f64,
    /// Protective-stop distance as a fraction of `ref_price` (e.g. 0.004 = 0.4%).
    /// The OMS derives both the size cap and the stop price from this.
    pub stop_frac: f64,
    /// Profit target as a multiple of the stop distance (R-multiple).
    pub target_r: f64,
    pub rationale: String,
    pub ts_ms: u64,
}

impl Signal {
    /// Stop-loss price implied by `stop_frac` for this signal's side.
    pub fn stop_price(&self) -> f64 {
        let dist = self.ref_price * self.stop_frac;
        match self.side {
            PositionSide::Long => (self.ref_price - dist).max(0.0),
            PositionSide::Short => self.ref_price + dist,
        }
    }

    /// Profit-target price implied by `stop_frac * target_r`.
    pub fn target_price(&self) -> f64 {
        let dist = self.ref_price * self.stop_frac * self.target_r;
        match self.side {
            PositionSide::Long => self.ref_price + dist,
            PositionSide::Short => (self.ref_price - dist).max(0.0),
        }
    }
}
