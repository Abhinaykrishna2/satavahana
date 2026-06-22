//! Transaction-cost models for the microstructure engine.
//!
//! Two regimes, deliberately kept separate (they are NOT interchangeable):
//!   * **Equity intraday (MIS)** — Strategy 1. Brokerage min(0.03%, ₹20)/order,
//!     STT 0.025% sell-side, exchange txn, GST, SEBI, stamp duty buy-side.
//!   * **Index options** — Strategy 2. Flat ₹20/order, date-dependent STT on sell premium,
//!     exchange txn on premium, GST, SEBI, stamp duty.
//!
//! On ₹5k capital a 1% (₹50) stop is roughly one round trip of costs, so sizing
//! and realized P&L MUST be computed net of these. Rates mirror the existing
//! `backtest.rs` (equity) and `quant_engine.rs` (options) models.

use chrono::{FixedOffset, NaiveDate, TimeZone, Utc};

// ---- Equity intraday (Zerodha MIS) ----
const EQ_BROKERAGE_RATE: f64 = 0.0003; // 0.03% per executed order...
const EQ_BROKERAGE_CAP: f64 = 20.0; // ...capped at ₹20/order
const EQ_STT_SELL: f64 = 0.00025; // 0.025% on sell value (intraday)
const EQ_EXCHANGE_TXN: f64 = 0.0000297; // NSE ~0.00297% of turnover
const EQ_SEBI_PER_RUPEE: f64 = 0.000001; // ₹10 per crore
const EQ_STAMP_BUY: f64 = 0.00003; // 0.003% on buy value
const GST_RATE: f64 = 0.18; // 18% on (brokerage + txn + sebi)

// ---- Index options (Zerodha) ----
const OPT_BROKERAGE_FLAT: f64 = 20.0; // ₹20 per executed order
const OPT_EXCHANGE_TXN: f64 = 0.000311; // NSE options exchange txn on premium
const OPT_SEBI_PER_RUPEE: f64 = 0.000001; // ₹10 per crore
const OPT_STAMP_BUY: f64 = 0.00003; // 0.003% on buy-side premium

/// Total round-trip cost (both legs) for an equity intraday position of `qty`
/// shares bought at `buy_price` and sold at `sell_price`.
pub fn equity_intraday_roundtrip(buy_price: f64, sell_price: f64, qty: u32) -> f64 {
    let qty = qty as f64;
    let buy_value = buy_price * qty;
    let sell_value = sell_price * qty;
    let turnover = buy_value + sell_value;

    let brokerage = (buy_value * EQ_BROKERAGE_RATE).min(EQ_BROKERAGE_CAP)
        + (sell_value * EQ_BROKERAGE_RATE).min(EQ_BROKERAGE_CAP);
    let stt = sell_value * EQ_STT_SELL;
    let txn = turnover * EQ_EXCHANGE_TXN;
    let sebi = turnover * EQ_SEBI_PER_RUPEE;
    let gst = (brokerage + txn + sebi) * GST_RATE;
    let stamp = buy_value * EQ_STAMP_BUY;

    brokerage + stt + txn + sebi + gst + stamp
}

/// Pre-trade cost estimate for sizing an equity position: assume entry ≈ exit.
pub fn equity_est_roundtrip(price: f64, qty: u32) -> f64 {
    equity_intraday_roundtrip(price, price, qty)
}

/// Total round-trip cost for an options position of `lots` lots (`lot_size` each)
/// entered at premium `entry` and exited at premium `exit`.
pub fn options_roundtrip(entry: f64, exit: f64, lot_size: u32, lots: u32) -> f64 {
    options_roundtrip_at(entry, exit, lot_size, lots, now_ms())
}

/// Total options round-trip cost using the tax regime active at `exit_ts_ms`.
pub fn options_roundtrip_at(
    entry: f64,
    exit: f64,
    lot_size: u32,
    lots: u32,
    exit_ts_ms: u64,
) -> f64 {
    let qty = (lot_size * lots) as f64;
    let buy_prem = entry * qty;
    let sell_prem = exit * qty;
    let turnover = buy_prem + sell_prem;

    let brokerage = 2.0 * OPT_BROKERAGE_FLAT; // both legs
    let stt = sell_prem * options_sell_stt_rate(exit_ts_ms);
    let txn = turnover * OPT_EXCHANGE_TXN;
    let sebi = turnover * OPT_SEBI_PER_RUPEE;
    let gst = (brokerage + txn + sebi) * GST_RATE;
    let stamp = buy_prem * OPT_STAMP_BUY;

    brokerage + stt + txn + sebi + gst + stamp
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn options_sell_stt_rate(exit_ts_ms: u64) -> f64 {
    let ist = FixedOffset::east_opt(5 * 3600 + 30 * 60).expect("valid IST offset");
    let dt_ist = Utc
        .timestamp_millis_opt(exit_ts_ms as i64)
        .single()
        .unwrap_or_else(Utc::now)
        .with_timezone(&ist);
    let stt_hike_date = NaiveDate::from_ymd_opt(2026, 4, 1).expect("valid STT hike date");
    if dt_ist.date_naive() >= stt_hike_date {
        0.0015
    } else {
        0.0010
    }
}

/// Pre-trade options cost estimate for sizing: assume entry ≈ exit.
pub fn options_est_roundtrip(price: f64, lot_size: u32, lots: u32) -> f64 {
    options_roundtrip(price, price, lot_size, lots)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn equity_roundtrip_is_material_on_small_turnover() {
        // ₹100 × 100 shares each side = ₹10k turnover.
        let c = equity_intraday_roundtrip(100.0, 100.0, 100);
        // Brokerage 3+3, STT 2.5, txn ~0.59, sebi 0.02, gst ~1.19, stamp 0.3 ≈ ₹10.6
        assert!(c > 9.0 && c < 13.0, "unexpected equity round-trip cost {c}");
        // Costs eat a large fraction of a ₹50 (1% of ₹5k) risk budget.
        assert!(c > 0.15 * 50.0);
    }

    #[test]
    fn equity_brokerage_caps_at_20_per_side() {
        // ₹1000 × 1000 = ₹1,000,000 per side; 0.03% = ₹300 -> capped to ₹20.
        let c = equity_intraday_roundtrip(1000.0, 1000.0, 1000);
        // Brokerage component alone would be 600 uncapped; capped => 40.
        // Sanity: total must be far below the uncapped-brokerage figure.
        assert!(c < 600.0);
    }

    #[test]
    fn options_roundtrip_one_nifty_lot() {
        // 1 NIFTY lot (65) at ₹100 premium.
        let c = options_roundtrip_at(100.0, 100.0, 65, 1, 1_774_981_800_000);
        // ≈ ₹40 brokerage + STT(0.15% of 6500≈9.75) + txn + gst.
        assert!(c > 55.0 && c < 70.0, "unexpected options round-trip cost {c}");
    }

    #[test]
    fn options_stt_rate_changes_on_2026_april_first_ist() {
        let before = options_roundtrip_at(100.0, 100.0, 65, 1, 1_774_981_799_000);
        let after = options_roundtrip_at(100.0, 100.0, 65, 1, 1_774_981_800_000);
        assert!(after > before);
        assert!((after - before - 3.25).abs() < 0.01);
    }

    #[test]
    fn estimates_match_equal_price_roundtrip() {
        assert_eq!(
            equity_est_roundtrip(250.0, 40),
            equity_intraday_roundtrip(250.0, 250.0, 40)
        );
        assert_eq!(
            options_est_roundtrip(120.0, 65, 2),
            options_roundtrip(120.0, 120.0, 65, 2)
        );
    }
}
