//! Spot-based technical confirmation layer (June 2026).
//!
//! Aggregates an underlying's spot ticks into rolling **1-minute OHLC bars** and
//! derives a small, ORTHOGONAL indicator set used as a **directional gate** on the
//! option-chain / order-flow signals: a trade is only taken when the chain/flow
//! direction and the price-action direction AGREE.
//!
//! ## Honest scope
//! - Fixed textbook periods (EMA 9/21, Bollinger 20/2σ, RSI 14, ATR 14, ADX/DMI 14,
//!   Fibonacci 30, opening range 15 bars). They are NOT tuned to any captured day —
//!   tuning them to 1-2 days of data would be curve-fitting.
//! - **No look-ahead.** Indicators read only bars that have CLOSED before the decision
//!   tick; the in-progress bar never leaks its future into a signal.
//! - The bias is `Neutral` until enough bars have formed (warmup). On `Neutral` the gate
//!   PASSES (does not veto) — so early-session trades, before indicators form, fall back
//!   to the existing logic and are NOT protected by this layer. Deliberate choice.

use std::collections::VecDeque;

/// One completed 1-minute OHLC bar.
#[derive(Debug, Clone, Copy)]
pub struct Bar {
    pub minute: i64, // unix minute (ms / 60_000)
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Bull,
    Bear,
    Neutral,
}

/// The directional read used to gate a trade.
#[derive(Debug, Clone, Copy)]
pub struct TechnicalBias {
    pub direction: Direction,
    /// 0.0–1.0 — fraction of the indicator panel that agrees on `direction`.
    pub strength: f64,
    /// True at RSI extremes (>= 75 or <= 25) — a contrarian caution flag.
    pub rsi_extreme: bool,
}

impl TechnicalBias {
    pub fn neutral() -> Self {
        TechnicalBias { direction: Direction::Neutral, strength: 0.0, rsi_extreme: false }
    }
}

/// Fibonacci retracement map over the configured swing window.
#[derive(Debug, Clone, Copy)]
pub struct FibonacciLevels {
    pub low: f64,
    pub retr_382: f64,
    pub retr_500: f64,
    pub retr_618: f64,
    pub high: f64,
}

#[derive(Debug, Clone, Copy)]
pub struct DmiAdx {
    pub plus_di: f64,
    pub minus_di: f64,
    pub adx: f64,
    pub direction: Direction,
}

#[derive(Debug, Clone, Copy)]
pub struct BollingerWidth {
    pub width_pct: f64,
    pub expanding: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct OpeningRangeRead {
    pub high: f64,
    pub low: f64,
    pub direction: Direction,
}

#[derive(Debug, Clone)]
pub struct ScalpAssessment {
    pub direction: Direction,
    pub aligned: u32,
    pub observed: u32,
    pub summary: String,
}

/// Short-term rejection of a recent swing high/low.
#[derive(Debug, Clone, Copy)]
pub struct SpotReversal {
    pub direction: Direction,
    pub move_pct: f64,
    pub atr_multiple: f64,
    pub lookback_bars: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct DirectionalExtension {
    pub extreme: f64,
    pub current: f64,
    pub points: f64,
    pub pct: f64,
    pub lookback_bars: usize,
}

// ─── Fixed periods (textbook; never tuned to captured data) ──────────────────
const EMA_FAST: usize = 9;
const EMA_SLOW: usize = 21;
const BB_PERIOD: usize = 20;
const BB_K: f64 = 2.0;
const RSI_PERIOD: usize = 14;
const ATR_PERIOD: usize = 14;
const ADX_PERIOD: usize = 14;
const FIB_WINDOW: usize = 30; // swing hi/lo lookback for Fibonacci levels
const OPENING_RANGE_BARS: usize = 15;
const REVERSAL_LOOKBACK: usize = 30;
const REVERSAL_CONFIRM_BARS: usize = 3;
const REVERSAL_SWING_MAX_AGE: usize = 20;
const REVERSAL_MIN_MOVE_PCT: f64 = 0.0010; // 0.10% on the underlying
const REVERSAL_MIN_ATR_MULT: f64 = 1.5;
/// Bars needed before a bias is emitted (longest period that drives direction).
const MIN_BARS: usize = EMA_SLOW + 1;
const MAX_BARS: usize = 420; // one full NSE cash session plus buffer

/// Rolling 1-minute bar series for one underlying, with no-look-ahead indicators.
#[derive(Debug, Clone)]
pub struct SpotSeries {
    bars: VecDeque<Bar>, // CLOSED bars only
    cur: Option<Bar>,    // in-progress bar (NEVER read by indicators)
    cur_minute: i64,
}

impl SpotSeries {
    pub fn new() -> Self {
        SpotSeries { bars: VecDeque::with_capacity(MAX_BARS + 1), cur: None, cur_minute: -1 }
    }

    /// Ingest a spot tick. When the wall-clock minute rolls over, the in-progress bar
    /// is finalized and pushed to the CLOSED history; the new tick starts a fresh bar.
    pub fn ingest(&mut self, ts_ms: u64, price: f64) {
        if price <= 0.0 {
            return;
        }
        let minute = (ts_ms / 60_000) as i64;
        match self.cur {
            Some(ref mut b) if minute == self.cur_minute => {
                b.high = b.high.max(price);
                b.low = b.low.min(price);
                b.close = price;
            }
            _ => {
                // New minute: close the previous in-progress bar into history.
                if let Some(done) = self.cur.take() {
                    self.bars.push_back(done);
                    while self.bars.len() > MAX_BARS {
                        self.bars.pop_front();
                    }
                }
                self.cur = Some(Bar {
                    minute,
                    open: price,
                    high: price,
                    low: price,
                    close: price,
                });
                self.cur_minute = minute;
            }
        }
    }

    /// CLOSED-bar count (the only bars indicators may read).
    pub fn closed_len(&self) -> usize {
        self.bars.len()
    }

    fn closes(&self) -> Vec<f64> {
        self.bars.iter().map(|b| b.close).collect()
    }

    fn ema(values: &[f64], period: usize) -> Option<f64> {
        if values.len() < period {
            return None;
        }
        let k = 2.0 / (period as f64 + 1.0);
        // Seed with the SMA of the first `period`, then roll forward.
        let mut ema = values[..period].iter().sum::<f64>() / period as f64;
        for &v in &values[period..] {
            ema = v * k + ema * (1.0 - k);
        }
        Some(ema)
    }

    fn rsi(values: &[f64], period: usize) -> Option<f64> {
        if values.len() < period + 1 {
            return None;
        }

        let mut gain = 0.0;
        let mut loss = 0.0;
        for i in 1..=period {
            let d = values[i] - values[i - 1];
            if d >= 0.0 {
                gain += d;
            } else {
                loss -= d;
            }
        }

        let mut avg_gain = gain / period as f64;
        let mut avg_loss = loss / period as f64;
        for i in (period + 1)..values.len() {
            let d = values[i] - values[i - 1];
            let g = if d > 0.0 { d } else { 0.0 };
            let l = if d < 0.0 { -d } else { 0.0 };
            avg_gain = (avg_gain * (period as f64 - 1.0) + g) / period as f64;
            avg_loss = (avg_loss * (period as f64 - 1.0) + l) / period as f64;
        }

        if avg_loss <= f64::EPSILON {
            return Some(100.0);
        }
        if avg_gain <= f64::EPSILON {
            return Some(0.0);
        }
        let rs = avg_gain / avg_loss;
        Some(100.0 - 100.0 / (1.0 + rs))
    }

    fn bollinger(values: &[f64], period: usize, k: f64) -> Option<(f64, f64, f64)> {
        if values.len() < period {
            return None;
        }
        let window = &values[values.len() - period..];
        let mean = window.iter().sum::<f64>() / period as f64;
        let var = window.iter().map(|v| (v - mean).powi(2)).sum::<f64>()
            / period as f64;
        let sd = var.sqrt();
        Some((mean - k * sd, mean, mean + k * sd))
    }

    pub fn bollinger_width(&self) -> Option<BollingerWidth> {
        let closes = self.closes();
        let (_lo_prev, mid_prev, hi_prev) =
            Self::bollinger(&closes[..closes.len().saturating_sub(1)], BB_PERIOD, BB_K)?;
        let (lo, mid, hi) = Self::bollinger(&closes, BB_PERIOD, BB_K)?;
        if mid.abs() <= f64::EPSILON || mid_prev.abs() <= f64::EPSILON {
            return None;
        }

        let width = (hi - lo) / mid.abs();
        let prev_width = (hi_prev - _lo_prev) / mid_prev.abs();
        Some(BollingerWidth {
            width_pct: width * 100.0,
            expanding: width > prev_width,
        })
    }

    pub fn dmi_adx(&self) -> Option<DmiAdx> {
        if self.bars.len() < ADX_PERIOD * 2 + 1 {
            return None;
        }

        let bars: Vec<&Bar> = self.bars.iter().collect();
        let mut tr = Vec::with_capacity(bars.len().saturating_sub(1));
        let mut plus_dm = Vec::with_capacity(bars.len().saturating_sub(1));
        let mut minus_dm = Vec::with_capacity(bars.len().saturating_sub(1));

        for i in 1..bars.len() {
            let cur = bars[i];
            let prev = bars[i - 1];
            let up_move = cur.high - prev.high;
            let down_move = prev.low - cur.low;
            plus_dm.push(if up_move > down_move && up_move > 0.0 {
                up_move
            } else {
                0.0
            });
            minus_dm.push(if down_move > up_move && down_move > 0.0 {
                down_move
            } else {
                0.0
            });
            tr.push(
                (cur.high - cur.low)
                    .max((cur.high - prev.close).abs())
                    .max((cur.low - prev.close).abs()),
            );
        }

        let mut smooth_tr: f64 = tr.iter().take(ADX_PERIOD).sum();
        let mut smooth_plus: f64 = plus_dm.iter().take(ADX_PERIOD).sum();
        let mut smooth_minus: f64 = minus_dm.iter().take(ADX_PERIOD).sum();
        let mut dx_values = Vec::new();
        let mut last_plus_di = 0.0;
        let mut last_minus_di = 0.0;

        for i in (ADX_PERIOD - 1)..tr.len() {
            if i >= ADX_PERIOD {
                smooth_tr = smooth_tr - smooth_tr / ADX_PERIOD as f64 + tr[i];
                smooth_plus = smooth_plus - smooth_plus / ADX_PERIOD as f64 + plus_dm[i];
                smooth_minus = smooth_minus - smooth_minus / ADX_PERIOD as f64 + minus_dm[i];
            }
            if smooth_tr <= f64::EPSILON {
                continue;
            }

            last_plus_di = 100.0 * smooth_plus / smooth_tr;
            last_minus_di = 100.0 * smooth_minus / smooth_tr;
            let denom = last_plus_di + last_minus_di;
            if denom > f64::EPSILON {
                dx_values.push(100.0 * (last_plus_di - last_minus_di).abs() / denom);
            }
        }

        if dx_values.len() < ADX_PERIOD {
            return None;
        }
        let mut adx = dx_values.iter().take(ADX_PERIOD).sum::<f64>() / ADX_PERIOD as f64;
        for dx in dx_values.iter().skip(ADX_PERIOD) {
            adx = (adx * (ADX_PERIOD as f64 - 1.0) + dx) / ADX_PERIOD as f64;
        }

        let direction = if last_plus_di > last_minus_di {
            Direction::Bull
        } else if last_minus_di > last_plus_di {
            Direction::Bear
        } else {
            Direction::Neutral
        };

        Some(DmiAdx {
            plus_di: last_plus_di,
            minus_di: last_minus_di,
            adx,
            direction,
        })
    }

    pub fn opening_range(&self) -> Option<OpeningRangeRead> {
        if self.bars.len() < OPENING_RANGE_BARS + 1 {
            return None;
        }
        let mut high = f64::MIN;
        let mut low = f64::MAX;
        for b in self.bars.iter().take(OPENING_RANGE_BARS) {
            high = high.max(b.high);
            low = low.min(b.low);
        }
        if high <= low || !high.is_finite() || !low.is_finite() {
            return None;
        }
        let last = self.bars.back()?.close;
        let direction = if last > high {
            Direction::Bull
        } else if last < low {
            Direction::Bear
        } else {
            Direction::Neutral
        };
        Some(OpeningRangeRead {
            high,
            low,
            direction,
        })
    }

    /// Average True Range over CLOSED bars (volatility magnitude; for context/logging).
    pub fn atr(&self) -> Option<f64> {
        if self.bars.len() < ATR_PERIOD + 1 {
            return None;
        }
        let bars: Vec<&Bar> = self.bars.iter().collect();
        let mut trs = Vec::with_capacity(ATR_PERIOD);
        for i in (bars.len() - ATR_PERIOD)..bars.len() {
            let b = bars[i];
            let prev_close = bars[i - 1].close;
            let tr = (b.high - b.low)
                .max((b.high - prev_close).abs())
                .max((b.low - prev_close).abs());
            trs.push(tr);
        }
        Some(trs.iter().sum::<f64>() / ATR_PERIOD as f64)
    }

    /// Fibonacci retracement levels from the swing high/low over the last `FIB_WINDOW`
    /// CLOSED bars: (low, 0.382, 0.5, 0.618, high).
    pub fn fib_levels(&self) -> Option<FibonacciLevels> {
        if self.bars.len() < FIB_WINDOW {
            return None;
        }
        let window: Vec<&Bar> = self.bars.iter().rev().take(FIB_WINDOW).collect();
        let hi = window.iter().map(|b| b.high).fold(f64::MIN, f64::max);
        let lo = window.iter().map(|b| b.low).fold(f64::MAX, f64::min);
        let range = hi - lo;
        if range <= f64::EPSILON || !range.is_finite() {
            return None;
        }
        Some(FibonacciLevels {
            low: lo,
            retr_382: lo + 0.382 * range,
            retr_500: lo + 0.5 * range,
            retr_618: lo + 0.618 * range,
            high: hi,
        })
    }

    fn fibonacci_bias(&self, last_close: f64) -> Option<Direction> {
        if self.bars.len() < FIB_WINDOW {
            return None;
        }

        let start = self.bars.len() - FIB_WINDOW;
        let window: Vec<&Bar> = self.bars.iter().skip(start).collect();
        let (hi_idx, hi) = window
            .iter()
            .enumerate()
            .max_by(|(_, a), (_, b)| {
                a.high
                    .partial_cmp(&b.high)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|(idx, b)| (idx, b.high))?;
        let (lo_idx, lo) = window
            .iter()
            .enumerate()
            .min_by(|(_, a), (_, b)| {
                a.low
                    .partial_cmp(&b.low)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|(idx, b)| (idx, b.low))?;
        let range = hi - lo;
        if range <= f64::EPSILON || !range.is_finite() {
            return None;
        }

        let fib_382 = lo + 0.382 * range;
        let fib_500 = lo + 0.500 * range;
        let fib_618 = lo + 0.618 * range;

        if lo_idx < hi_idx {
            // Upswing: holding above 50% keeps bullish structure intact; losing 38.2%
            // shows the retracement is deep enough to stop treating the move as bullish.
            if last_close >= fib_500 {
                Some(Direction::Bull)
            } else if last_close < fib_382 {
                Some(Direction::Bear)
            } else {
                Some(Direction::Neutral)
            }
        } else if hi_idx < lo_idx {
            // Downswing: staying below 50% keeps bearish structure intact; reclaiming
            // 61.8% warns that the downside swing has likely failed.
            if last_close <= fib_500 {
                Some(Direction::Bear)
            } else if last_close > fib_618 {
                Some(Direction::Bull)
            } else {
                Some(Direction::Neutral)
            }
        } else {
            Some(Direction::Neutral)
        }
    }

    /// Detects a short-term break away from a recent swing high/low using only
    /// CLOSED bars. This is intentionally not a broad trend detector; it catches
    /// price-action rejection after the slower EMA/RSI panel can still read as the
    /// old trend.
    pub fn reversal_break(&self) -> Option<SpotReversal> {
        if self.bars.len() < REVERSAL_LOOKBACK + 1 || self.bars.len() < ATR_PERIOD + 1 {
            return None;
        }
        let last = self.bars.back()?;
        let prior: Vec<&Bar> = self
            .bars
            .iter()
            .rev()
            .skip(1)
            .take(REVERSAL_LOOKBACK)
            .collect();
        if prior.len() < REVERSAL_LOOKBACK {
            return None;
        }

        let (recent_high_idx, recent_high) = prior
            .iter()
            .enumerate()
            .max_by(|(_, a), (_, b)| {
                a.high
                    .partial_cmp(&b.high)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|(idx, b)| (idx, b.high))?;
        let (recent_low_idx, recent_low) = prior
            .iter()
            .enumerate()
            .min_by(|(_, a), (_, b)| {
                a.low
                    .partial_cmp(&b.low)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|(idx, b)| (idx, b.low))?;
        if recent_high <= recent_low || recent_low <= 0.0 {
            return None;
        }

        let atr = self.atr()?;
        if atr <= f64::EPSILON {
            return None;
        }

        let recent_low_confirm = prior
            .iter()
            .take(REVERSAL_CONFIRM_BARS)
            .map(|b| b.low)
            .fold(f64::MAX, f64::min);
        let recent_high_confirm = prior
            .iter()
            .take(REVERSAL_CONFIRM_BARS)
            .map(|b| b.high)
            .fold(f64::MIN, f64::max);

        let down_points = recent_high - last.close;
        let down_pct = down_points / recent_high.max(1.0);
        let down_atr = down_points / atr;
        let down_ok = down_pct >= REVERSAL_MIN_MOVE_PCT
            && down_atr >= REVERSAL_MIN_ATR_MULT
            && recent_high_idx <= REVERSAL_SWING_MAX_AGE
            && last.close <= recent_low_confirm;

        let up_points = last.close - recent_low;
        let up_pct = up_points / recent_low.max(1.0);
        let up_atr = up_points / atr;
        let up_ok = up_pct >= REVERSAL_MIN_MOVE_PCT
            && up_atr >= REVERSAL_MIN_ATR_MULT
            && recent_low_idx <= REVERSAL_SWING_MAX_AGE
            && last.close >= recent_high_confirm;

        match (down_ok, up_ok) {
            (true, true) if down_atr >= up_atr => Some(SpotReversal {
                direction: Direction::Bear,
                move_pct: down_pct,
                atr_multiple: down_atr,
                lookback_bars: REVERSAL_LOOKBACK,
            }),
            (true, true) => Some(SpotReversal {
                direction: Direction::Bull,
                move_pct: up_pct,
                atr_multiple: up_atr,
                lookback_bars: REVERSAL_LOOKBACK,
            }),
            (true, false) => Some(SpotReversal {
                direction: Direction::Bear,
                move_pct: down_pct,
                atr_multiple: down_atr,
                lookback_bars: REVERSAL_LOOKBACK,
            }),
            (false, true) => Some(SpotReversal {
                direction: Direction::Bull,
                move_pct: up_pct,
                atr_multiple: up_atr,
                lookback_bars: REVERSAL_LOOKBACK,
            }),
            (false, false) => None,
        }
    }

    /// Distance already travelled from the recent closed-bar extreme to `current_price`.
    /// The extreme is taken only from CLOSED bars; the current price is the live decision tick.
    pub fn directional_extension_from_extreme(
        &self,
        direction: Direction,
        lookback_bars: usize,
        current_price: f64,
    ) -> Option<DirectionalExtension> {
        if direction == Direction::Neutral
            || lookback_bars == 0
            || current_price <= 0.0
            || self.bars.is_empty()
        {
            return None;
        }

        let observed = self.bars.len().min(lookback_bars);
        let bars = self.bars.iter().rev().take(observed);
        let extreme = match direction {
            Direction::Bear => bars.map(|b| b.high).fold(f64::NEG_INFINITY, f64::max),
            Direction::Bull => bars.map(|b| b.low).fold(f64::INFINITY, f64::min),
            Direction::Neutral => return None,
        };
        if !extreme.is_finite() || extreme <= 0.0 {
            return None;
        }

        let points = match direction {
            Direction::Bear => extreme - current_price,
            Direction::Bull => current_price - extreme,
            Direction::Neutral => return None,
        };
        if points <= 0.0 {
            return None;
        }

        Some(DirectionalExtension {
            extreme,
            current: current_price,
            points,
            pct: points / extreme,
            lookback_bars: observed,
        })
    }

    pub fn technical_summary(&self) -> Option<String> {
        if self.bars.is_empty() {
            return None;
        }
        let closes = self.closes();
        let last = *closes.last()?;

        let mut parts = vec![format!("close {:.2}", last)];
        if let (Some(fast), Some(slow)) =
            (Self::ema(&closes, EMA_FAST), Self::ema(&closes, EMA_SLOW))
        {
            parts.push(format!(
                "EMA{}/{} {:.2}/{:.2}",
                EMA_FAST, EMA_SLOW, fast, slow
            ));
        }
        if let Some((lo, mid, hi)) = Self::bollinger(&closes, BB_PERIOD, BB_K) {
            parts.push(format!("BB{} {:.2}/{:.2}/{:.2}", BB_PERIOD, lo, mid, hi));
        }
        if let Some(rsi) = Self::rsi(&closes, RSI_PERIOD) {
            parts.push(format!("RSI{} {:.1}", RSI_PERIOD, rsi));
        }
        if let Some(dmi) = self.dmi_adx() {
            parts.push(format!(
                "ADX{} {:.1} +DI/-DI {:.1}/{:.1}",
                ADX_PERIOD, dmi.adx, dmi.plus_di, dmi.minus_di
            ));
        }
        if let Some(f) = self.fib_levels() {
            parts.push(format!(
                "Fib{} {:.2}/{:.2}/{:.2}",
                FIB_WINDOW, f.retr_382, f.retr_500, f.retr_618
            ));
        }
        if let Some(width) = self.bollinger_width() {
            parts.push(format!(
                "BBW {:.2}% {}",
                width.width_pct,
                if width.expanding { "expanding" } else { "contracting" }
            ));
        }
        if let Some(or) = self.opening_range() {
            parts.push(format!(
                "OR{} {:.2}-{:.2} {:?}",
                OPENING_RANGE_BARS, or.low, or.high, or.direction
            ));
        }
        if let Some(atr) = self.atr() {
            parts.push(format!("ATR{} {:.2}", ATR_PERIOD, atr));
        }

        Some(parts.join(", "))
    }

    /// Latest closed-bar RSI used by strategy gates that need the scalar value, not just the
    /// directional vote inside `bias()`.
    pub fn rsi14(&self) -> Option<f64> {
        Self::rsi(&self.closes(), RSI_PERIOD)
    }

    pub fn scalp_assessment(&self, direction: Direction) -> Option<ScalpAssessment> {
        if direction == Direction::Neutral || self.bars.len() < MIN_BARS {
            return None;
        }
        let closes = self.closes();
        let last = *closes.last()?;
        let mut aligned = 0_u32;
        let mut observed = 0_u32;
        let mut parts = Vec::new();

        let bias = self.bias();
        if bias.direction != Direction::Neutral {
            observed += 1;
            if bias.direction == direction {
                aligned += 1;
            }
            parts.push(format!("bias {:?}/{:.0}%", bias.direction, bias.strength * 100.0));
        }

        if let Some(dmi) = self.dmi_adx() {
            observed += 1;
            if dmi.direction == direction {
                aligned += 1;
            }
            parts.push(format!(
                "DMI {:?} ADX {:.1}",
                dmi.direction, dmi.adx
            ));
        }

        if let Some((_, mid, _)) = Self::bollinger(&closes, BB_PERIOD, BB_K) {
            observed += 1;
            let bb_dir = if last > mid {
                Direction::Bull
            } else if last < mid {
                Direction::Bear
            } else {
                Direction::Neutral
            };
            if bb_dir == direction {
                aligned += 1;
            }
            parts.push(format!("BB mid {:?}", bb_dir));
        }

        if let Some(width) = self.bollinger_width() {
            parts.push(format!(
                "BBW {:.2}% {}",
                width.width_pct,
                if width.expanding { "expanding" } else { "contracting" }
            ));
        }

        if let Some(or) = self.opening_range() {
            observed += 1;
            if or.direction == direction {
                aligned += 1;
            }
            parts.push(format!("OR {:?}", or.direction));
        }

        if observed == 0 {
            return None;
        }
        Some(ScalpAssessment {
            direction,
            aligned,
            observed,
            summary: parts.join(", "),
        })
    }

    /// The directional bias from a confluence of trend (EMA9 vs EMA21 + price), Bollinger
    /// position (price vs mid band), RSI (>55 / <45), and Fibonacci swing structure.
    /// Direction is emitted only when the panel has a clear majority; otherwise `Neutral`.
    /// Reads CLOSED bars only.
    pub fn bias(&self) -> TechnicalBias {
        if self.bars.len() < MIN_BARS {
            return TechnicalBias::neutral();
        }
        let closes = self.closes();
        let last = *closes.last().unwrap();

        let mut votes: i32 = 0;
        let mut panel = 0;

        // 1. Trend: fast vs slow EMA, plus price relative to the slow EMA.
        if let (Some(fast), Some(slow)) =
            (Self::ema(&closes, EMA_FAST), Self::ema(&closes, EMA_SLOW))
        {
            panel += 1;
            if fast > slow && last >= slow {
                votes += 1;
            } else if fast < slow && last <= slow {
                votes -= 1;
            }
        }
        // 2. Bollinger position: above the mid band is bullish, below is bearish.
        if let Some((_lo, mid, _hi)) = Self::bollinger(&closes, BB_PERIOD, BB_K) {
            panel += 1;
            if last > mid {
                votes += 1;
            } else if last < mid {
                votes -= 1;
            }
        }
        // 3. RSI momentum (and extreme flag).
        let mut rsi_extreme = false;
        if let Some(rsi) = Self::rsi(&closes, RSI_PERIOD) {
            panel += 1;
            if rsi >= 55.0 {
                votes += 1;
            } else if rsi <= 45.0 {
                votes -= 1;
            }
            rsi_extreme = rsi >= 75.0 || rsi <= 25.0;
        }

        // 4. Fibonacci swing structure over the recent closed-bar range.
        if let Some(dir) = self.fibonacci_bias(last) {
            panel += 1;
            match dir {
                Direction::Bull => votes += 1,
                Direction::Bear => votes -= 1,
                Direction::Neutral => {}
            }
        }

        if panel == 0 {
            return TechnicalBias::neutral();
        }
        let strength = votes.unsigned_abs() as f64 / panel as f64;
        let direction = if votes >= 2 {
            Direction::Bull
        } else if votes <= -2 {
            Direction::Bear
        } else {
            Direction::Neutral
        };
        TechnicalBias { direction, strength, rsi_extreme }
    }

    /// **The hard gate.** Returns true if a BUY-CALL (bullish) trade is permitted: blocked
    /// only when the technicals have a CONFIRMED bearish bias. Neutral/unformed → allowed.
    pub fn allows_bullish(&self) -> bool {
        self.bias().direction != Direction::Bear
    }

    /// Returns true if a BUY-PUT (bearish) trade is permitted: blocked only when the
    /// technicals have a CONFIRMED bullish bias. Neutral/unformed → allowed.
    pub fn allows_bearish(&self) -> bool {
        self.bias().direction != Direction::Bull
    }
}

impl Default for SpotSeries {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Feed `n` minutes of bars climbing by `step`/min (one tick per minute boundary).
    fn feed_trend(s: &mut SpotSeries, start: f64, step: f64, n: usize) {
        let mut px = start;
        for m in 0..n {
            // two ticks inside the minute so high/low form, then the minute rolls
            s.ingest((m as u64) * 60_000 + 1_000, px);
            s.ingest((m as u64) * 60_000 + 30_000, px + step.abs() * 0.3);
            px += step;
        }
        // one extra tick in the next minute to CLOSE the last trend bar
        s.ingest((n as u64) * 60_000 + 1_000, px);
    }

    #[test]
    fn no_bias_before_warmup() {
        let mut s = SpotSeries::new();
        feed_trend(&mut s, 100.0, 1.0, 5); // only 5 closed bars < MIN_BARS
        assert_eq!(s.bias().direction, Direction::Neutral);
        assert!(
            s.allows_bullish() && s.allows_bearish(),
            "unformed → gate passes both"
        );
    }

    #[test]
    fn rsi_uses_wilder_smoothing() {
        // Classic Wilder RSI sample. The first RSI after 14 periods is about 70.46.
        let closes = [
            44.34, 44.09, 44.15, 43.61, 44.33,
            44.83, 45.10, 45.42, 45.84, 46.08,
            45.89, 46.03, 45.61, 46.28, 46.28,
        ];
        let rsi = SpotSeries::rsi(&closes, 14).expect("RSI should form");
        assert!((rsi - 70.46).abs() < 0.05, "unexpected Wilder RSI {rsi}");
    }

    #[test]
    fn rsi14_accessor_matches_wilder_rsi() {
        let closes = [
            44.34, 44.09, 44.15, 43.61, 44.33,
            44.83, 45.10, 45.42, 45.84, 46.08,
            45.89, 46.03, 45.61, 46.28, 46.28,
        ];
        let mut s = SpotSeries::new();
        for (idx, close) in closes.iter().enumerate() {
            s.ingest(idx as u64 * 60_000, *close);
        }
        s.ingest(closes.len() as u64 * 60_000, *closes.last().unwrap());

        let rsi = s.rsi14().expect("RSI should form from closed bars");
        assert!((rsi - 70.46).abs() < 0.05, "unexpected accessor RSI {rsi}");
    }

    #[test]
    fn fibonacci_levels_and_vote_use_recent_closed_swing() {
        let mut up = SpotSeries::new();
        feed_trend(&mut up, 100.0, 1.0, 30);
        let levels = up.fib_levels().expect("fib levels should form");
        assert!(levels.low < levels.retr_382);
        assert!(levels.retr_382 < levels.retr_500);
        assert!(levels.retr_500 < levels.retr_618);
        assert!(levels.retr_618 < levels.high);
        assert_eq!(
            up.fibonacci_bias(*up.closes().last().unwrap()),
            Some(Direction::Bull)
        );

        let mut down = SpotSeries::new();
        feed_trend(&mut down, 200.0, -1.0, 30);
        assert_eq!(
            down.fibonacci_bias(*down.closes().last().unwrap()),
            Some(Direction::Bear)
        );
    }

    #[test]
    fn technical_summary_reports_the_full_panel() {
        let mut s = SpotSeries::new();
        feed_trend(&mut s, 100.0, 1.0, 35);
        let summary = s.technical_summary().expect("summary should form");
        assert!(summary.contains("EMA9/21"));
        assert!(summary.contains("BB20"));
        assert!(summary.contains("RSI14"));
        assert!(summary.contains("ADX14"));
        assert!(summary.contains("Fib30"));
        assert!(summary.contains("BBW"));
        assert!(summary.contains("OR15"));
        assert!(summary.contains("ATR14"));
    }

    #[test]
    fn dmi_and_opening_range_confirm_a_clean_scalp_trend() {
        let mut s = SpotSeries::new();
        feed_trend(&mut s, 100.0, 1.0, 40);

        let dmi = s.dmi_adx().expect("DMI/ADX should form");
        assert_eq!(dmi.direction, Direction::Bull);
        assert!(dmi.plus_di > dmi.minus_di);

        let or = s.opening_range().expect("opening range should form");
        assert_eq!(or.direction, Direction::Bull);

        let scalp = s
            .scalp_assessment(Direction::Bull)
            .expect("scalp read should form");
        assert!(scalp.aligned >= 3, "unexpected scalp alignment {:?}", scalp);
        assert!(scalp.observed >= 3);
    }

    #[test]
    fn uptrend_reads_bullish_and_vetoes_puts() {
        let mut s = SpotSeries::new();
        feed_trend(&mut s, 100.0, 1.0, 30); // steady climb
        let b = s.bias();
        assert_eq!(b.direction, Direction::Bull, "a steady climb must read Bull");
        assert!(s.allows_bullish(), "calls allowed in an uptrend");
        assert!(!s.allows_bearish(), "PUTS VETOED in a confirmed uptrend (today's mistake)");
    }

    #[test]
    fn downtrend_reads_bearish_and_vetoes_calls() {
        let mut s = SpotSeries::new();
        feed_trend(&mut s, 200.0, -1.0, 30); // steady decline
        assert_eq!(s.bias().direction, Direction::Bear);
        assert!(!s.allows_bullish(), "calls vetoed in a confirmed downtrend");
        assert!(s.allows_bearish());
    }

    #[test]
    fn in_progress_bar_is_not_read_by_indicators() {
        let mut s = SpotSeries::new();
        feed_trend(&mut s, 100.0, 1.0, 25);
        let closed_before = s.closed_len();
        // A flurry of ticks within the SAME current minute (25) must not change the closed
        // count (and therefore must not change the bias) — no look-ahead from the live bar.
        let bias_before = s.bias().direction;
        for k in 0..50u64 {
            s.ingest(25 * 60_000 + 2_000 + k * 100, 999.0); // wild spikes, still minute 25
        }
        assert_eq!(s.closed_len(), closed_before, "in-progress ticks must not add closed bars");
        assert_eq!(s.bias().direction, bias_before, "in-progress bar must not move the bias");
    }

    #[test]
    fn reversal_break_reads_closed_bars_only() {
        let mut s = SpotSeries::new();
        feed_trend(&mut s, 100.0, 1.0, 35);
        assert!(s.reversal_break().is_none(), "steady trend is not a reversal");

        for m in 36..41 {
            let px = 135.0 - (m - 35) as f64 * 1.2;
            s.ingest((m as u64) * 60_000 + 1_000, px);
            s.ingest((m as u64) * 60_000 + 30_000, px - 0.4);
        }
        s.ingest(41 * 60_000 + 1_000, 128.0);

        let rev = s.reversal_break().expect("break below recent lows should register");
        assert_eq!(rev.direction, Direction::Bear);
        assert!(rev.move_pct >= 0.0010);
        assert!(rev.atr_multiple >= 1.5);

        let before = rev.move_pct;
        for k in 0..20u64 {
            s.ingest(41 * 60_000 + 2_000 + k * 100, 90.0);
        }
        assert_eq!(
            s.reversal_break().unwrap().move_pct,
            before,
            "in-progress collapse must not alter the closed-bar reversal"
        );
    }

    #[test]
    fn directional_extension_uses_closed_extreme_and_live_price() {
        let mut s = SpotSeries::new();
        feed_trend(&mut s, 100.0, 1.0, 70);

        let ext = s
            .directional_extension_from_extreme(Direction::Bear, 60, 130.0)
            .expect("drop from recent high should be measured");
        assert_eq!(ext.lookback_bars, 60);
        assert!(ext.extreme > 160.0, "recent closed high should come from closed bars");
        assert!(ext.points > 30.0);

        let before = ext.points;
        for k in 0..20u64 {
            s.ingest(70 * 60_000 + 2_000 + k * 100, 999.0);
        }
        let after = s
            .directional_extension_from_extreme(Direction::Bear, 60, 130.0)
            .unwrap();
        assert_eq!(
            after.points, before,
            "same-minute in-progress spike must not change the recent extreme"
        );
    }
}
