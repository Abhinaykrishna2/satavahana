use crate::models::Tick;
use rand::Rng;
use std::collections::VecDeque;
use std::time::Duration;
use tracing::{info, warn};

#[derive(Debug, Clone)]
pub struct BacktestConfig {
    pub mean_api_lag_ms: f64,
    pub api_lag_std_ms: f64,
    pub order_placement_lag_ms: f64,
    pub exchange_processing_lag_ms: f64,
    pub slippage_fraction: f64,
    pub tick_size: f64,
    pub brokerage_per_trade: f64,
    pub stt_rate: f64,
    pub transaction_charges_rate: f64,
    pub gst_rate: f64,
    pub sebi_charges_per_crore: f64,
    pub stamp_duty_rate: f64,
}

impl Default for BacktestConfig {
    fn default() -> Self {
        Self {
            mean_api_lag_ms: 150.0,
            api_lag_std_ms: 50.0,
            order_placement_lag_ms: 5.0,
            exchange_processing_lag_ms: 10.0,

            slippage_fraction: 0.0,
            tick_size: 0.05,

            brokerage_per_trade: 20.0,
            stt_rate: 0.00025,
            transaction_charges_rate: 0.0000720,
            gst_rate: 0.18,
            sebi_charges_per_crore: 10.0,
            stamp_duty_rate: 0.00003,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SpreadSignal {
    pub timestamp_us: u64,
    pub symbol: String,
    pub nse_token: u32,
    pub bse_token: u32,
    pub nse_ltp_at_signal: f64,
    pub bse_ltp_at_signal: f64,
    pub spread_at_signal: f64,
    pub quantity: u32,
}

#[derive(Debug, Clone)]
pub struct BacktestTrade {
    pub signal: SpreadSignal,
    pub total_lag_ms: f64,
    pub nse_execution_price: f64,
    pub bse_execution_price: f64,
    pub spread_at_execution: f64,
    pub gross_pnl: f64,
    pub total_costs: f64,
    pub net_pnl: f64,
    pub slippage_loss: f64,
    pub success: bool,
}

#[derive(Debug, Clone)]
struct PriceSnapshot {
    timestamp_us: u64,
    ltp: f64,
}

pub struct BacktestEngine {
    config: BacktestConfig,
    price_history: std::collections::HashMap<u32, VecDeque<PriceSnapshot>>,
}

impl BacktestEngine {
    pub fn new(config: BacktestConfig) -> Self {
        Self {
            config,
            price_history: std::collections::HashMap::new(),
        }
    }

    pub fn record_tick(&mut self, tick: &Tick, timestamp_us: u64) {
        let entry = self.price_history.entry(tick.token).or_insert_with(VecDeque::new);

        entry.push_back(PriceSnapshot {
            timestamp_us,
            ltp: tick.ltp,
        });

        const MAX_HISTORY_US: u64 = 10_000_000;
        while let Some(front) = entry.front() {
            if timestamp_us - front.timestamp_us > MAX_HISTORY_US {
                entry.pop_front();
            } else {
                break;
            }
        }
    }

    pub fn simulate_trade(&self, signal: SpreadSignal) -> BacktestTrade {
        let api_lag = self.sample_api_lag();
        let total_lag_ms = api_lag
            + self.config.order_placement_lag_ms
            + self.config.exchange_processing_lag_ms;

        let execution_time_us = signal.timestamp_us + (total_lag_ms * 1000.0) as u64;

        let nse_price_at_exec = self.get_price_at_time(signal.nse_token, execution_time_us);
        let bse_price_at_exec = self.get_price_at_time(signal.bse_token, execution_time_us);

        let (nse_exec, bse_exec, success) = match (nse_price_at_exec, bse_price_at_exec) {
            (Some(nse), Some(bse)) => (nse, bse, true),
            _ => {
                warn!(
                    "Backtest: Price data unavailable for {} at execution time",
                    signal.symbol
                );
                return BacktestTrade {
                    signal,
                    total_lag_ms,
                    nse_execution_price: 0.0,
                    bse_execution_price: 0.0,
                    spread_at_execution: 0.0,
                    gross_pnl: 0.0,
                    total_costs: 0.0,
                    net_pnl: 0.0,
                    slippage_loss: 0.0,
                    success: false,
                };
            }
        };

        let tick = self.config.tick_size;
        let (nse_final, bse_final) = if signal.spread_at_signal > 0.0 {
            (nse_exec - tick, bse_exec + tick)
        } else {
            (nse_exec + tick, bse_exec - tick)
        };

        let spread_at_execution = nse_final - bse_final;

        let (buy_price, sell_price) = if signal.spread_at_signal > 0.0 {
            (bse_final, nse_final)
        } else {
            (nse_final, bse_final)
        };

        let gross_pnl = (sell_price - buy_price) * signal.quantity as f64;

        let total_costs = self.calculate_total_costs(
            buy_price,
            sell_price,
            signal.quantity,
        );

        let net_pnl = gross_pnl - total_costs;

        let slippage_loss = signal.spread_at_signal.abs() - spread_at_execution.abs();

        info!(
            "Backtest: {} | Signal: ₹{:.2} @ {}us | Exec: ₹{:.2} @ +{:.1}ms | Gross: ₹{:.2} | Costs: ₹{:.2} | Net: ₹{:.2} | Slippage: ₹{:.2}",
            signal.symbol,
            signal.spread_at_signal.abs(),
            signal.timestamp_us,
            spread_at_execution.abs(),
            total_lag_ms,
            gross_pnl,
            total_costs,
            net_pnl,
            slippage_loss
        );

        BacktestTrade {
            signal,
            total_lag_ms,
            nse_execution_price: nse_final,
            bse_execution_price: bse_final,
            spread_at_execution,
            gross_pnl,
            total_costs,
            net_pnl,
            slippage_loss,
            success,
        }
    }

    fn sample_api_lag(&self) -> f64 {
        use rand_distr::Distribution;
        let normal = rand_distr::Normal::new(
            self.config.mean_api_lag_ms,
            self.config.api_lag_std_ms,
        ).unwrap();

        let mut rng = rand::thread_rng();
        let lag = normal.sample(&mut rng);

        lag.max(0.0).min(1000.0)
    }

    fn get_price_at_time(&self, token: u32, timestamp_us: u64) -> Option<f64> {
        let history = self.price_history.get(&token)?;

        if history.is_empty() {
            return None;
        }

        let mut prev: Option<&PriceSnapshot> = None;
        let mut next: Option<&PriceSnapshot> = None;

        for snapshot in history.iter() {
            if snapshot.timestamp_us <= timestamp_us {
                prev = Some(snapshot);
            } else {
                next = Some(snapshot);
                break;
            }
        }

        match (prev, next) {
            (Some(p), Some(n)) => {
                let weight = (timestamp_us - p.timestamp_us) as f64
                    / (n.timestamp_us - p.timestamp_us) as f64;
                let price = p.ltp + weight * (n.ltp - p.ltp);
                Some(price)
            }
            (Some(p), None) => {
                Some(p.ltp)
            }
            (None, Some(n)) => {
                Some(n.ltp)
            }
            (None, None) => None,
        }
    }

    fn apply_slippage(
        &self,
        nse_signal: f64,
        bse_signal: f64,
        nse_exec: f64,
        bse_exec: f64,
        original_spread: f64,
    ) -> (f64, f64) {
        let spread_at_exec = nse_exec - bse_exec;
        let spread_change = (spread_at_exec - original_spread).abs();

        let slippage = spread_change * self.config.slippage_fraction;

        if original_spread > 0.0 {
            let bse_final = bse_exec + slippage / 2.0;
            let nse_final = nse_exec - slippage / 2.0;
            (nse_final.max(0.0), bse_final.max(0.0))
        } else {
            let nse_final = nse_exec + slippage / 2.0;
            let bse_final = bse_exec - slippage / 2.0;
            (nse_final.max(0.0), bse_final.max(0.0))
        }
    }

    pub fn calculate_total_costs_pub(&self, buy_price: f64, sell_price: f64, quantity: u32) -> f64 {
        self.calculate_total_costs(buy_price, sell_price, quantity)
    }

    fn calculate_total_costs(&self, buy_price: f64, sell_price: f64, quantity: u32) -> f64 {
        let qty = quantity as f64;
        let buy_value = buy_price * qty;
        let sell_value = sell_price * qty;
        let total_turnover = buy_value + sell_value;

        let brokerage_buy = (buy_value * 0.0003).min(self.config.brokerage_per_trade);
        let brokerage_sell = (sell_value * 0.0003).min(self.config.brokerage_per_trade);
        let total_brokerage = brokerage_buy + brokerage_sell;

        let stt = sell_value * self.config.stt_rate;

        let transaction_charges = total_turnover * self.config.transaction_charges_rate;

        let gst = (total_brokerage + transaction_charges) * self.config.gst_rate;

        let sebi_charges = (total_turnover / 10_000_000.0) * self.config.sebi_charges_per_crore;

        let stamp_duty = buy_value * self.config.stamp_duty_rate;

        total_brokerage + stt + transaction_charges + gst + sebi_charges + stamp_duty
    }

    pub fn generate_report(&self, trades: &[BacktestTrade]) {
        if trades.is_empty() {
            info!("No trades executed in backtest.");
            return;
        }

        let successful_trades: Vec<&BacktestTrade> = trades.iter().filter(|t| t.success).collect();
        let total_trades = successful_trades.len();

        if total_trades == 0 {
            warn!("All trades failed (no price data at execution time).");
            return;
        }

        let total_gross_pnl: f64 = successful_trades.iter().map(|t| t.gross_pnl).sum();
        let total_costs: f64 = successful_trades.iter().map(|t| t.total_costs).sum();
        let total_net_pnl: f64 = successful_trades.iter().map(|t| t.net_pnl).sum();
        let total_slippage: f64 = successful_trades.iter().map(|t| t.slippage_loss).sum();

        let profitable_trades = successful_trades.iter().filter(|t| t.net_pnl > 0.0).count();
        let win_rate = (profitable_trades as f64 / total_trades as f64) * 100.0;

        let avg_lag: f64 = successful_trades.iter().map(|t| t.total_lag_ms).sum::<f64>() / total_trades as f64;
        let avg_gross_pnl = total_gross_pnl / total_trades as f64;
        let avg_net_pnl = total_net_pnl / total_trades as f64;

        info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        info!("  BACKTEST REPORT");
        info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        info!("Total Trades: {}", total_trades);
        info!("Profitable: {} ({:.2}% win rate)", profitable_trades, win_rate);
        info!("");
        info!("P&L Summary:");
        info!("  Gross P&L: ₹{:.2}", total_gross_pnl);
        info!("  Total Costs: ₹{:.2}", total_costs);
        info!("  Net P&L: ₹{:.2}", total_net_pnl);
        info!("  Total Slippage Loss: ₹{:.2}", total_slippage);
        info!("");
        info!("Averages:");
        info!("  Avg Lag: {:.2}ms", avg_lag);
        info!("  Avg Gross P&L: ₹{:.2}", avg_gross_pnl);
        info!("  Avg Net P&L: ₹{:.2}", avg_net_pnl);
        info!("  Avg Costs per Trade: ₹{:.2}", total_costs / total_trades as f64);
        info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cost_calculation() {
        let config = BacktestConfig::default();
        let engine = BacktestEngine::new(config);

        let costs = engine.calculate_total_costs(1410.0, 1413.0, 100);

        assert!(costs > 100.0 && costs < 200.0, "Costs: ₹{:.2}", costs);
    }

    #[test]
    fn test_lag_sampling() {
        let config = BacktestConfig::default();
        let engine = BacktestEngine::new(config);

        let mut sum = 0.0;
        for _ in 0..1000 {
            sum += engine.sample_api_lag();
        }
        let avg = sum / 1000.0;

        assert!(avg > 100.0 && avg < 200.0, "Avg lag: {:.2}ms", avg);
    }
}
