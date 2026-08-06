use serde::Deserialize;
use std::path::{Path, PathBuf};

pub const CONFIG_PATH: &str = "config/config.toml";
pub const CONFIG_EXAMPLE_PATH: &str = "config/config.toml.example";

pub fn default_config_path() -> PathBuf {
    for candidate in [CONFIG_PATH, "../config/config.toml", "config.toml"] {
        let path = Path::new(candidate);
        if path.exists() {
            return path.to_path_buf();
        }
    }

    PathBuf::from(CONFIG_PATH)
}

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

    /// Live microstructure trading engine (Strategy 1 equity imbalance +
    /// Strategy 2 Tuesday Nifty gamma). Disabled by default — opt-in.
    #[serde(default)]
    pub microstructure: MicrostructureConfig,

    /// Risk rails shared by the microstructure engine (per-trade stop,
    /// daily kill switch, 3:15 flatten, hourly capital sync schedule).
    #[serde(default)]
    pub risk: RiskConfig,
}

fn default_options_engine_config() -> OptionsEngineConfig {
    OptionsEngineConfig {
        enabled: true,
        initial_capital: 10_000.0,
        max_daily_loss_pct: 15.0,
        max_daily_profit_pct: 25.0,
        profit_target_pct: 55.0,
        stop_loss_pct: 35.0,
        profit_lock_arm_pct: default_profit_lock_arm_pct(),
        profit_lock_gain_pct: default_profit_lock_gain_pct(),
        trail_arm_pct: default_trail_arm_pct(),
        trail_giveback_pct: default_trail_giveback_pct(),
        min_confidence: 60.0,
        expiry_day_min_confidence: 60.0,
        scan_interval_secs: 30,
        max_daily_trades: default_max_daily_trades(),
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
        equity_exchange: default_equity_exchange(),
        equity_product: default_equity_product(),
        max_repegs: default_max_repegs(),
        repeg_tick_threshold: default_repeg_tick_threshold(),
        chase_timeout_ms: default_chase_timeout_ms(),
        min_repeg_interval_ms: default_min_repeg_interval_ms(),
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
    /// Record every raw tick to `data/*_ticks.csv`. OFF by default — this is the
    /// 25 GB-in-2-days firehose. Only signal/trade ledgers are written otherwise.
    /// Turn on solely for offline data collection.
    #[serde(default)]
    pub record_ticks: bool,
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

    // ---- Equity (Strategy 1) execution leg ----
    /// Exchange for equity MIS orders (Strategy 1). Default: NSE.
    #[serde(default = "default_equity_exchange")]
    pub equity_exchange: String,
    /// Product for equity intraday orders. Default: MIS (5x intraday leverage).
    #[serde(default = "default_equity_product")]
    pub equity_product: String,

    // ---- Limit-chase OMS parameters ----
    /// Max number of re-peg (modify/replace) actions per chased order. Default: 5.
    #[serde(default = "default_max_repegs")]
    pub max_repegs: u32,
    /// Re-peg when the live best price moves this many ticks away from our resting
    /// order. Default: 1 tick.
    #[serde(default = "default_repeg_tick_threshold")]
    pub repeg_tick_threshold: u32,
    /// Total time budget for chasing a single entry before giving up. Default: 3000ms.
    #[serde(default = "default_chase_timeout_ms")]
    pub chase_timeout_ms: u64,
    /// Minimum gap between two re-peg actions on the same order. Throttles us well
    /// under the hard SEBI 10 orders/sec limit (250ms => <=4/sec). Default: 250ms.
    #[serde(default = "default_min_repeg_interval_ms")]
    pub min_repeg_interval_ms: u64,
}

fn default_equity_exchange() -> String {
    "NSE".to_string()
}
fn default_equity_product() -> String {
    "MIS".to_string()
}
fn default_max_repegs() -> u32 {
    5
}
fn default_repeg_tick_threshold() -> u32 {
    1
}
fn default_chase_timeout_ms() -> u64 {
    3000
}
fn default_min_repeg_interval_ms() -> u64 {
    250
}

/// Live microstructure engine configuration. All fields default so the section
/// is optional in `config/config.toml`; the engine is OFF unless `enabled = true`.
#[derive(Debug, Clone, Deserialize)]
pub struct MicrostructureConfig {
    /// Master switch for the live microstructure engine.
    #[serde(default)]
    pub enabled: bool,
    /// Strategy 1: equity order-book imbalance (MIS). Default OFF while the live
    /// system is operated in options-only mode.
    #[serde(default)]
    pub equity_imbalance_enabled: bool,
    /// Strategy 2: Tuesday Nifty weekly ATM gamma squeeze.
    #[serde(default = "default_true")]
    pub gamma_squeeze_enabled: bool,
    /// NSE equity symbols to track for Strategy 1 (subscribed in full/depth mode).
    #[serde(default)]
    pub equity_watchlist: Vec<String>,
    /// Standardized OBI z-score below which "extreme sell pressure" is flagged.
    /// Default: -0.4 (spec).
    #[serde(default = "default_obi_z_threshold")]
    pub obi_z_threshold: f64,
    /// Rolling window (in snapshots) used to standardize OBI and baseline OFI.
    #[serde(default = "default_obi_lookback")]
    pub obi_lookback: usize,
    /// Require OFI to flip positive (aggressive buyers absorbing) before entering long.
    #[serde(default = "default_true")]
    pub ofi_confirm: bool,
    /// Strategy 2: ATM ask-queue depletion must exceed this multiple of its rolling MA.
    /// Default: 5.0 (spec).
    #[serde(default = "default_gamma_depletion_multiple")]
    pub gamma_depletion_multiple: f64,
    /// Rolling window (in snapshots) for the ATM depletion-rate moving average.
    #[serde(default = "default_gamma_ma_window")]
    pub gamma_ma_window: usize,
    /// Underlying for Strategy 2. Default: NIFTY.
    #[serde(default = "default_nifty_symbol")]
    pub nifty_symbol: String,
    /// Assumed intraday leverage for MIS sizing (informational cap). Default: 5.0.
    #[serde(default = "default_mis_leverage")]
    pub mis_leverage: f64,
    /// Maximum simultaneously open microstructure positions. Default: 1.
    #[serde(default = "default_max_concurrent_positions")]
    pub max_concurrent_positions: u32,
    /// Cooldown after opening a position before another entry is allowed. Default: 30s.
    #[serde(default = "default_signal_cooldown_secs")]
    pub signal_cooldown_secs: u64,
}

impl Default for MicrostructureConfig {
    fn default() -> Self {
        MicrostructureConfig {
            enabled: false,
            equity_imbalance_enabled: false,
            gamma_squeeze_enabled: true,
            equity_watchlist: Vec::new(),
            obi_z_threshold: default_obi_z_threshold(),
            obi_lookback: default_obi_lookback(),
            ofi_confirm: true,
            gamma_depletion_multiple: default_gamma_depletion_multiple(),
            gamma_ma_window: default_gamma_ma_window(),
            nifty_symbol: default_nifty_symbol(),
            mis_leverage: default_mis_leverage(),
            max_concurrent_positions: default_max_concurrent_positions(),
            signal_cooldown_secs: default_signal_cooldown_secs(),
        }
    }
}

fn default_true() -> bool {
    true
}
fn default_obi_z_threshold() -> f64 {
    -0.4
}
fn default_obi_lookback() -> usize {
    60
}
fn default_gamma_depletion_multiple() -> f64 {
    5.0
}
fn default_gamma_ma_window() -> usize {
    30
}
fn default_nifty_symbol() -> String {
    "NIFTY".to_string()
}
fn default_mis_leverage() -> f64 {
    5.0
}
fn default_max_concurrent_positions() -> u32 {
    1
}
fn default_signal_cooldown_secs() -> u64 {
    30
}

/// Risk rails for the live engine. Always-on whenever the microstructure engine runs.
#[derive(Debug, Clone, Deserialize)]
pub struct RiskConfig {
    /// Hard per-trade stop as a percentage of dynamically-polled capital. Default: 1.0%.
    #[serde(default = "default_per_trade_stop_pct")]
    pub per_trade_stop_pct: f64,
    /// LOWER circuit: halt all new trades once the day's realized P&L (net of all
    /// broker fees and transaction costs) draws down this % of day-start capital.
    /// Default: 15.0%. (Accepts the legacy key `daily_kill_pct` as an alias.)
    #[serde(default = "default_daily_loss_circuit_pct", alias = "daily_kill_pct")]
    pub daily_loss_circuit_pct: f64,
    /// UPPER circuit: halt all new trades once the day's realized P&L (net of costs)
    /// gains this % of day-start capital — stops overtrading a good day. Default: 35.0%.
    #[serde(default = "default_daily_profit_circuit_pct")]
    pub daily_profit_circuit_pct: f64,
    /// Wall-clock IST time (HH:MM) at which all pending orders are cancelled and open
    /// positions are flattened — ahead of Zerodha's 3:20/3:26 auto-square. Default: 15:15.
    #[serde(default = "default_flatten_time")]
    pub flatten_time: String,
    /// IST time (HH:MM) of the first hourly capital sync. Default: 09:14.
    #[serde(default = "default_capital_sync_start")]
    pub capital_sync_start: String,
    /// Minute past each hour to poll live margins for capital sync. Default: 14.
    #[serde(default = "default_capital_poll_minute")]
    pub capital_poll_minute: u32,
    /// Hard account-wide cap on completed trades per day across all real-order engines.
    /// The shared portfolio also allows only one open position at a time.
    /// 1 = one real trade/day for the account. Default: 1.
    #[serde(default = "default_max_trades_per_day")]
    pub max_trades_per_day: u32,
}

impl Default for RiskConfig {
    fn default() -> Self {
        RiskConfig {
            per_trade_stop_pct: default_per_trade_stop_pct(),
            daily_loss_circuit_pct: default_daily_loss_circuit_pct(),
            daily_profit_circuit_pct: default_daily_profit_circuit_pct(),
            flatten_time: default_flatten_time(),
            capital_sync_start: default_capital_sync_start(),
            capital_poll_minute: default_capital_poll_minute(),
            max_trades_per_day: default_max_trades_per_day(),
        }
    }
}

fn default_max_trades_per_day() -> u32 {
    1
}

fn default_per_trade_stop_pct() -> f64 {
    1.0
}
fn default_daily_loss_circuit_pct() -> f64 {
    15.0
}
fn default_daily_profit_circuit_pct() -> f64 {
    25.0
}
fn default_flatten_time() -> String {
    "15:15".to_string()
}
fn default_capital_sync_start() -> String {
    "09:14".to_string()
}
fn default_capital_poll_minute() -> u32 {
    14
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

    /// LOWER circuit: halt new trades at this % daily loss (net of costs).
    #[serde(default = "default_max_daily_loss")]
    pub max_daily_loss_pct: f64,

    /// UPPER circuit: halt new trades at this % daily profit (anti-overtrade).
    #[serde(default = "default_max_daily_profit")]
    pub max_daily_profit_pct: f64,

    #[serde(default = "default_profit_target")]
    pub profit_target_pct: f64,

    #[serde(default = "default_stop_loss")]
    pub stop_loss_pct: f64,

    /// Runner-profile option premium gain required before moving stop above entry.
    /// The engine applies this only to explicitly eligible >1-DTE runner trades;
    /// expiry-day and 1-DTE exits stay on the rigid built-in safety profile.
    #[serde(default = "default_profit_lock_arm_pct")]
    pub profit_lock_arm_pct: f64,

    /// Runner-profile locked gain after profit lock arms, as % of entry premium.
    #[serde(default = "default_profit_lock_gain_pct")]
    pub profit_lock_gain_pct: f64,

    /// Runner-profile option premium gain required before the peak-giveback trail starts.
    #[serde(default = "default_trail_arm_pct")]
    pub trail_arm_pct: f64,

    /// Runner-profile percent of peak gain allowed to be given back once trailing is armed.
    #[serde(default = "default_trail_giveback_pct")]
    pub trail_giveback_pct: f64,

    #[serde(default = "default_min_confidence")]
    pub min_confidence: f64,

    #[serde(default = "default_expiry_day_min_confidence")]
    pub expiry_day_min_confidence: f64,

    #[serde(default = "default_scan_interval")]
    pub scan_interval_secs: u64,

    #[serde(default = "default_max_daily_trades")]
    pub max_daily_trades: u32,
}

fn default_engine_enabled() -> bool {
    true
}
fn default_initial_capital() -> f64 {
    10_000.0
}
fn default_max_daily_loss() -> f64 {
    15.0
}
fn default_max_daily_profit() -> f64 {
    25.0
}
fn default_profit_target() -> f64 {
    55.0
}
fn default_stop_loss() -> f64 {
    35.0
}
fn default_profit_lock_arm_pct() -> f64 {
    12.0
}
fn default_profit_lock_gain_pct() -> f64 {
    2.0
}
fn default_trail_arm_pct() -> f64 {
    20.0
}
fn default_trail_giveback_pct() -> f64 {
    60.0
}
fn default_min_confidence() -> f64 {
    60.0
}
fn default_expiry_day_min_confidence() -> f64 {
    60.0
}
fn default_scan_interval() -> u64 {
    30
}
fn default_max_daily_trades() -> u32 {
    3
}

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
        // Optional sections default when absent.
        assert!(!config.microstructure.enabled);
        assert!((config.risk.per_trade_stop_pct - 1.0).abs() < 1e-9);
        assert!((config.risk.daily_loss_circuit_pct - 15.0).abs() < 1e-9);
        assert!((config.risk.daily_profit_circuit_pct - 25.0).abs() < 1e-9);
        assert_eq!(config.risk.flatten_time, "15:15");
        assert_eq!(config.execution.equity_exchange, "NSE");
        assert_eq!(
            config.options_engine.max_daily_trades,
            default_max_daily_trades()
        );
    }

    #[test]
    fn test_microstructure_and_risk_sections_parse() {
        let toml_str = r#"
[kite]
api_key = "k"
api_secret = "s"

[equities]
symbols = ["INFY"]

[options]
underlyings = ["NIFTY"]
strike_min = 22000.0
strike_max = 26000.0
strike_step = 50.0

[greeks]
risk_free_rate = 0.065
dividend_yield = 0.0

[output]
min_spread_alert = 0.5
greeks_log_interval = 1

[microstructure]
enabled = true
equity_watchlist = ["RELIANCE", "HDFCBANK"]
obi_z_threshold = -0.45
gamma_depletion_multiple = 6.0

[risk]
per_trade_stop_pct = 0.8
daily_kill_pct = 2.5
daily_profit_circuit_pct = 40.0
flatten_time = "15:10"
capital_sync_start = "09:14"
capital_poll_minute = 14
"#;
        let c: Config = toml::from_str(toml_str).unwrap();
        assert!(c.microstructure.enabled);
        assert_eq!(c.microstructure.equity_watchlist.len(), 2);
        assert!((c.microstructure.obi_z_threshold - (-0.45)).abs() < 1e-9);
        assert!((c.microstructure.gamma_depletion_multiple - 6.0).abs() < 1e-9);
        // Defaulted microstructure fields still present.
        assert!(!c.microstructure.equity_imbalance_enabled);
        assert!((c.risk.per_trade_stop_pct - 0.8).abs() < 1e-9);
        // Legacy `daily_kill_pct` aliases the lower circuit; upper circuit explicit.
        assert!((c.risk.daily_loss_circuit_pct - 2.5).abs() < 1e-9);
        assert!((c.risk.daily_profit_circuit_pct - 40.0).abs() < 1e-9);
        assert_eq!(c.risk.flatten_time, "15:10");
    }
}
