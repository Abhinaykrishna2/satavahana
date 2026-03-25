use std::fmt;

#[derive(Debug, Clone)]
pub struct Instrument {
    pub instrument_token: u32,
    pub exchange_token: u32,
    pub tradingsymbol: String,
    pub name: String,
    pub last_price: f64,
    pub expiry: Option<String>,
    pub strike: Option<f64>,
    pub tick_size: f64,
    pub lot_size: u32,
    pub instrument_type: String,
    pub segment: String,
    pub exchange: String,
}

#[derive(Debug, Clone)]
pub struct EquityPair {
    pub name: String,
    pub symbol: String,
    pub nse_token: u32,
    pub bse_token: u32,
    pub nse_symbol: String,
    pub bse_symbol: String,
}

impl fmt::Display for EquityPair {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} [NSE:{} BSE:{}]",
            self.name, self.nse_token, self.bse_token
        )
    }
}

#[derive(Debug, Clone)]
pub struct OptionContract {
    pub instrument_token: u32,
    pub tradingsymbol: String,
    pub underlying: String,
    pub expiry: String,
    pub strike: f64,
    pub option_type: OptionType,
    pub lot_size: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OptionType {
    CE,
    PE,
}

impl fmt::Display for OptionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OptionType::CE => write!(f, "CE"),
            OptionType::PE => write!(f, "PE"),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct OHLC {
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct DepthEntry {
    pub quantity: u32,
    pub price: f64,
    pub orders: u16,
}

#[derive(Debug, Clone, Default)]
pub struct MarketDepth {
    pub bids: [DepthEntry; 5],
    pub asks: [DepthEntry; 5],
}

#[derive(Debug, Clone)]
pub struct Tick {
    pub token: u32,
    pub ltp: f64,
    pub last_qty: u32,
    pub avg_price: f64,
    pub volume: u32,
    pub buy_qty: u32,
    pub sell_qty: u32,
    pub ohlc: OHLC,
    pub oi: u32,
    pub oi_day_high: u32,
    pub oi_day_low: u32,
    pub exchange_ts: u32,
    pub last_trade_ts: u32,
    pub depth: Option<MarketDepth>,
    pub mode: TickMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TickMode {
    Ltp,
    Quote,
    Full,
}

impl Default for Tick {
    fn default() -> Self {
        Self {
            token: 0,
            ltp: 0.0,
            last_qty: 0,
            avg_price: 0.0,
            volume: 0,
            buy_qty: 0,
            sell_qty: 0,
            ohlc: OHLC::default(),
            oi: 0,
            oi_day_high: 0,
            oi_day_low: 0,
            exchange_ts: 0,
            last_trade_ts: 0,
            depth: None,
            mode: TickMode::Ltp,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Greeks {
    pub iv: f64,
    pub delta: f64,
    pub gamma: f64,
    pub theta: f64,
    pub vega: f64,
    pub rho: f64,
}

impl fmt::Display for Greeks {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "IV:{:.2}% Δ:{:.4} Γ:{:.6} Θ:{:.2} V:{:.2} ρ:{:.4}",
            self.iv * 100.0,
            self.delta,
            self.gamma,
            self.theta,
            self.vega,
            self.rho
        )
    }
}
