use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub kite: KiteConfig,
    pub equities: EquitiesConfig,
    pub options: OptionsConfig,
    pub greeks: GreeksConfig,
    pub output: OutputConfig,
    #[serde(default)]
    pub backtest: Option<BacktestConfig>,
    #[serde(default = "default_execution_config")]
    pub execution: ExecutionConfig,

    #[serde(default = "default_options_engine_config")]
    pub options_engine: OptionsEngineConfig,
}

fn default_options_engine_config() -> OptionsEngineConfig {
    OptionsEngineConfig {
        enabled: true,
        initial_capital: 10_000.0,
        max_daily_loss_pct: 30.0,
        profit_target_pct: 55.0,
        stop_loss_pct: 35.0,
        min_confidence: 60.0,
        scan_interval_secs: 45,
        max_daily_trades: 5,
    }
}

fn default_execution_config() -> ExecutionConfig {
    ExecutionConfig {
        enable_live_orders: false,
        variety: "regular".to_string(),
        exchange: "NFO".to_string(),
        product: "NRML".to_string(),
        order_type: "MARKET".to_string(),
        validity: "DAY".to_string(),
        order_tag_prefix: "SATA".to_string(),
        entry_order_timeout_secs: 240,
        limit_cancel_reversal_pct: 0.15,
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct KiteConfig {
    pub api_key: String,
    pub api_secret: String,
    #[serde(default)]
    pub access_token: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EquitiesConfig {
    pub symbols: Vec<String>,
    #[serde(default = "default_min_price")]
    pub min_price: f64,
    #[serde(default = "default_min_volume")]
    pub min_avg_volume: u32,
}

fn default_min_price() -> f64 {
    10.0
}

fn default_min_volume() -> u32 {
    10_000
}

#[derive(Debug, Clone, Deserialize)]
pub struct OptionsConfig {
    pub underlyings: Vec<String>,
    #[serde(default)]
    pub expiry: Option<String>,
    pub strike_min: f64,
    pub strike_max: f64,
    pub strike_step: f64,
    /// Keep only the N nearest strikes per underlying on each side of ATM (0 = keep all in range).
    /// Acts as the global default; `nearest_strikes_override` can set per-underlying values.
    #[serde(default)]
    pub nearest_strikes: u32,
    /// Per-underlying overrides for nearest_strikes.
    /// Example: { NIFTYNXT50 = 15, FINNIFTY = 15 }
    /// Falls back to `nearest_strikes` for any underlying not listed here.
    #[serde(default)]
    pub nearest_strikes_override: std::collections::HashMap<String, u32>,
}

impl OptionsConfig {
    /// Returns the nearest_strikes value for the given underlying,
    /// using the per-underlying override if set, otherwise the global default.
    pub fn strikes_for(&self, underlying: &str) -> u32 {
        self.nearest_strikes_override
            .get(underlying)
            .copied()
            .unwrap_or(self.nearest_strikes)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct GreeksConfig {
    pub risk_free_rate: f64,
    pub dividend_yield: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OutputConfig {
    pub min_spread_alert: f64,
    pub greeks_log_interval: u32,
    #[serde(default)]
    pub enable_backtest: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BacktestConfig {
    #[serde(default = "default_mean_lag")]
    pub mean_api_lag_ms: f64,
    #[serde(default = "default_lag_std")]
    pub api_lag_std_ms: f64,
    #[serde(default = "default_slippage")]
    pub slippage_fraction: f64,
    #[serde(default = "default_quantity")]
    pub trade_quantity: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExecutionConfig {
    #[serde(default)]
    pub enable_live_orders: bool,
    #[serde(default = "default_order_variety")]
    pub variety: String,
    #[serde(default = "default_order_exchange")]
    pub exchange: String,
    #[serde(default = "default_order_product")]
    pub product: String,
    #[serde(default = "default_order_type")]
    pub order_type: String,
    #[serde(default = "default_order_validity")]
    pub validity: String,
    #[serde(default = "default_order_tag_prefix")]
    pub order_tag_prefix: String,
    /// Seconds to wait for a limit entry order to fill before cancelling (default: 240 = 4 min).
    #[serde(default = "default_entry_order_timeout_secs")]
    pub entry_order_timeout_secs: u64,
    /// Cancel pending entry if LTP drops this fraction below the limit price, signalling
    /// a directional reversal (default: 0.15 = 15%).
    #[serde(default = "default_limit_cancel_reversal_pct")]
    pub limit_cancel_reversal_pct: f64,
}

fn default_mean_lag() -> f64 {
    150.0
}

fn default_lag_std() -> f64 {
    50.0
}

fn default_slippage() -> f64 {
    0.3
}

fn default_quantity() -> u32 {
    100
}

fn default_order_variety() -> String {
    "regular".to_string()
}

fn default_order_exchange() -> String {
    "NFO".to_string()
}

fn default_order_product() -> String {
    "NRML".to_string()
}

fn default_order_type() -> String {
    "MARKET".to_string()
}

fn default_entry_order_timeout_secs() -> u64 {
    240
}

fn default_limit_cancel_reversal_pct() -> f64 {
    0.15
}

fn default_order_validity() -> String {
    "DAY".to_string()
}

fn default_order_tag_prefix() -> String {
    "SATA".to_string()
}

#[derive(Debug, Clone, Deserialize)]
pub struct OptionsEngineConfig {
    /// Set to false to disable signal generation and trading while keeping
    /// the WebSocket and tick recorder running for data collection.
    #[serde(default = "default_engine_enabled")]
    pub enabled: bool,

    #[serde(default = "default_initial_capital")]
    pub initial_capital: f64,

    #[serde(default = "default_max_daily_loss")]
    pub max_daily_loss_pct: f64,

    #[serde(default = "default_profit_target")]
    pub profit_target_pct: f64,

    #[serde(default = "default_stop_loss")]
    pub stop_loss_pct: f64,

    #[serde(default = "default_min_confidence")]
    pub min_confidence: f64,

    #[serde(default = "default_scan_interval")]
    pub scan_interval_secs: u64,

    #[serde(default = "default_max_daily_trades")]
    pub max_daily_trades: u32,
}

fn default_engine_enabled()   -> bool  { true }
fn default_initial_capital() -> f64 { 10_000.0 }
fn default_max_daily_loss()   -> f64 { 30.0 }
fn default_profit_target()    -> f64 { 55.0 }
fn default_stop_loss()        -> f64 { 35.0 }
fn default_min_confidence()   -> f64 { 60.0 }
fn default_scan_interval()    -> u64 { 45 }
fn default_max_daily_trades() -> u32 { 3 }

impl Config {
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        let config: Config = toml::from_str(&content)?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_deserialization() {
        let toml_str = r#"
[kite]
api_key = "test_key"
api_secret = "test_secret"
access_token = "test_token"

[equities]
symbols = ["INFY", "TCS"]

[options]
underlyings = ["NIFTY"]
expiry = "2026-02-26"
strike_min = 22000.0
strike_max = 24000.0
strike_step = 50.0

[greeks]
risk_free_rate = 0.065
dividend_yield = 0.0

[output]
min_spread_alert = 0.50
greeks_log_interval = 1
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(config.kite.api_key, "test_key");
        assert_eq!(config.equities.symbols.len(), 2);
        assert_eq!(config.options.underlyings[0], "NIFTY");
        assert_eq!(config.options.expiry.as_deref(), Some("2026-02-26"));
        assert!((config.greeks.risk_free_rate - 0.065).abs() < 1e-9);
    }
}
