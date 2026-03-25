use crate::models::{Greeks, OptionContract, OptionType};
use crate::store::TickStore;
use crate::websocket::TickEvent;
use chrono::{DateTime, NaiveDate, Utc};
use std::collections::HashMap;
use std::collections::HashSet;
use statrs::distribution::{Continuous, ContinuousCDF, Normal};
use tokio::sync::broadcast;
use tracing::{info, warn};

pub struct GreeksEngine {
    pub contracts: Vec<OptionContract>,
    pub store: TickStore,
    pub risk_free_rate: f64,
    pub dividend_yield: f64,
    pub log_interval: u32,
    pub underlying_tokens: HashMap<String, u32>,
    relevant_tokens: HashSet<u32>,
    tick_count: std::sync::atomic::AtomicU32,
}

impl GreeksEngine {
    pub fn new(
        contracts: Vec<OptionContract>,
        store: TickStore,
        risk_free_rate: f64,
        dividend_yield: f64,
        log_interval: u32,
        underlying_tokens: HashMap<String, u32>,
    ) -> Self {
        let mut relevant_tokens = HashSet::new();
        for contract in &contracts {
            relevant_tokens.insert(contract.instrument_token);
        }
        for token in underlying_tokens.values() {
            relevant_tokens.insert(*token);
        }

        Self {
            contracts,
            store,
            risk_free_rate,
            dividend_yield,
            log_interval,
            underlying_tokens,
            relevant_tokens,
            tick_count: std::sync::atomic::AtomicU32::new(0),
        }
    }

    pub fn spawn(
        self,
        mut rx: broadcast::Receiver<TickEvent>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            info!(
                "Greeks engine started — monitoring {} option contracts",
                self.contracts.len()
            );

            loop {
                match rx.recv().await {
                    Ok(event) => {
                        let relevant = event
                            .ticks
                            .iter()
                            .any(|t| self.relevant_tokens.contains(&t.token));

                        if relevant {
                            let count = self
                                .tick_count
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                            if self.log_interval > 0 && count % self.log_interval == 0 {
                                self.compute_and_log_greeks();
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!("Greeks engine lagged by {} messages", n);
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        info!("Greeks engine channel closed, shutting down");
                        break;
                    }
                }
            }
        })
    }

    fn compute_and_log_greeks(&self) {
        for contract in &self.contracts {
            let opt_tick = match self.store.get(contract.instrument_token) {
                Some(t) if t.ltp > 0.0 => t,
                _ => continue,
            };

            let spot = match self.underlying_tokens.get(&contract.underlying) {
                Some(token) => match self.store.get(*token) {
                    Some(t) if t.ltp > 0.0 => t.ltp,
                    _ => continue,
                },
                None => continue,
            };

            let t_years = match compute_time_to_expiry(&contract.expiry) {
                Some(t) if t > 0.0 => t,
                _ => continue,
            };

            match compute_greeks(
                spot,
                contract.strike,
                t_years,
                self.risk_free_rate,
                self.dividend_yield,
                opt_tick.ltp,
                contract.option_type,
            ) {
                Some(greeks) => {
                    info!(
                        "📈 GREEKS | {} {} {:.0} {} | LTP: ₹{:.2} | Spot: ₹{:.2} | {} | OI: {} | Vol: {}",
                        contract.underlying,
                        contract.expiry,
                        contract.strike,
                        contract.option_type,
                        opt_tick.ltp,
                        spot,
                        greeks,
                        opt_tick.oi,
                        opt_tick.volume,
                    );
                }
                None => {
                }
            }
        }
    }
}

pub fn compute_time_to_expiry(expiry_str: &str) -> Option<f64> {
    compute_time_to_expiry_at(expiry_str, Utc::now())
}

pub fn compute_time_to_expiry_at(expiry_str: &str, as_of_utc: DateTime<Utc>) -> Option<f64> {
    let expiry_date = NaiveDate::parse_from_str(expiry_str, "%Y-%m-%d").ok()?;
    let expiry_dt = expiry_date
        .and_hms_opt(10, 0, 0)?;
    let now = as_of_utc.naive_utc();

    let duration = expiry_dt.signed_duration_since(now);
    let seconds = duration.num_seconds() as f64;

    if seconds <= 0.0 {
        return None;
    }

    Some(seconds / (365.25 * 24.0 * 3600.0))
}

fn bs_price(s: f64, k: f64, t: f64, r: f64, q: f64, sigma: f64, opt_type: OptionType) -> f64 {
    let normal = Normal::new(0.0, 1.0).unwrap();

    let d1 = ((s / k).ln() + (r - q + 0.5 * sigma * sigma) * t) / (sigma * t.sqrt());
    let d2 = d1 - sigma * t.sqrt();

    match opt_type {
        OptionType::CE => {
            s * (-q * t).exp() * normal.cdf(d1) - k * (-r * t).exp() * normal.cdf(d2)
        }
        OptionType::PE => {
            k * (-r * t).exp() * normal.cdf(-d2) - s * (-q * t).exp() * normal.cdf(-d1)
        }
    }
}

fn bs_vega(s: f64, k: f64, t: f64, r: f64, q: f64, sigma: f64) -> f64 {
    let normal = Normal::new(0.0, 1.0).unwrap();
    let d1 = ((s / k).ln() + (r - q + 0.5 * sigma * sigma) * t) / (sigma * t.sqrt());

    let nd1 = normal.pdf(d1);
    s * (-q * t).exp() * nd1 * t.sqrt()
}

fn implied_volatility(
    s: f64,
    k: f64,
    t: f64,
    r: f64,
    q: f64,
    market_price: f64,
    opt_type: OptionType,
) -> Option<f64> {
    let mut sigma = 0.3;
    let max_iter = 100;
    let tol = 1e-6;

    for _ in 0..max_iter {
        let price = bs_price(s, k, t, r, q, sigma, opt_type);
        let vega = bs_vega(s, k, t, r, q, sigma);

        if vega.abs() < 1e-12 {
            return None;
        }

        let diff = price - market_price;
        let new_sigma = sigma - diff / vega;

        if new_sigma <= 0.001 {
            sigma = 0.001;
        } else {
            sigma = new_sigma;
        }

        if diff.abs() < tol {
            return Some(sigma);
        }
    }

    if sigma > 0.001 && sigma < 10.0 {
        Some(sigma)
    } else {
        None
    }
}

pub fn compute_greeks(
    spot: f64,
    strike: f64,
    t: f64,
    r: f64,
    q: f64,
    market_price: f64,
    opt_type: OptionType,
) -> Option<Greeks> {
    let sigma = implied_volatility(spot, strike, t, r, q, market_price, opt_type)?;

    let normal = Normal::new(0.0, 1.0).unwrap();
    let sqrt_t = t.sqrt();

    let d1 = ((spot / strike).ln() + (r - q + 0.5 * sigma * sigma) * t) / (sigma * sqrt_t);
    let d2 = d1 - sigma * sqrt_t;

    let nd1 = normal.pdf(d1);

    let delta = match opt_type {
        OptionType::CE => (-q * t).exp() * normal.cdf(d1),
        OptionType::PE => (-q * t).exp() * (normal.cdf(d1) - 1.0),
    };

    let gamma = (-q * t).exp() * nd1 / (spot * sigma * sqrt_t);

    let theta = match opt_type {
        OptionType::CE => {
            let term1 = -(spot * (-q * t).exp() * nd1 * sigma) / (2.0 * sqrt_t);
            let term2 = -r * strike * (-r * t).exp() * normal.cdf(d2);
            let term3 = q * spot * (-q * t).exp() * normal.cdf(d1);
            (term1 + term2 + term3) / 365.25
        }
        OptionType::PE => {
            let term1 = -(spot * (-q * t).exp() * nd1 * sigma) / (2.0 * sqrt_t);
            let term2 = r * strike * (-r * t).exp() * normal.cdf(-d2);
            let term3 = -q * spot * (-q * t).exp() * normal.cdf(-d1);
            (term1 + term2 + term3) / 365.25
        }
    };

    let vega = spot * (-q * t).exp() * nd1 * sqrt_t / 100.0;

    let rho = match opt_type {
        OptionType::CE => strike * t * (-r * t).exp() * normal.cdf(d2) / 100.0,
        OptionType::PE => -strike * t * (-r * t).exp() * normal.cdf(-d2) / 100.0,
    };

    Some(Greeks {
        iv: sigma,
        delta,
        gamma,
        theta,
        vega,
        rho,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bs_price_atm_call() {
        let price = bs_price(100.0, 100.0, 1.0, 0.05, 0.0, 0.2, OptionType::CE);
        assert!(
            (price - 10.45).abs() < 0.1,
            "BS Call price {} not close to 10.45",
            price
        );
    }

    #[test]
    fn test_bs_price_atm_put() {
        let price = bs_price(100.0, 100.0, 1.0, 0.05, 0.0, 0.2, OptionType::PE);
        assert!(
            (price - 5.57).abs() < 0.1,
            "BS Put price {} not close to 5.57",
            price
        );
    }

    #[test]
    fn test_implied_volatility_roundtrip() {
        let known_sigma = 0.25;
        let price = bs_price(100.0, 100.0, 1.0, 0.05, 0.0, known_sigma, OptionType::CE);
        let recovered_sigma =
            implied_volatility(100.0, 100.0, 1.0, 0.05, 0.0, price, OptionType::CE);

        assert!(recovered_sigma.is_some());
        assert!(
            (recovered_sigma.unwrap() - known_sigma).abs() < 1e-4,
            "Recovered σ={} differs from known σ={}",
            recovered_sigma.unwrap(),
            known_sigma
        );
    }

    #[test]
    fn test_greeks_atm_call() {
        let price = bs_price(100.0, 100.0, 1.0, 0.05, 0.0, 0.2, OptionType::CE);
        let greeks =
            compute_greeks(100.0, 100.0, 1.0, 0.05, 0.0, price, OptionType::CE).unwrap();

        assert!(
            (greeks.delta - 0.64).abs() < 0.05,
            "Delta {} not close to 0.64",
            greeks.delta
        );

        assert!(
            (greeks.iv - 0.20).abs() < 0.01,
            "IV {} not close to 0.20",
            greeks.iv
        );

        assert!(greeks.gamma > 0.0, "Gamma should be positive");

        assert!(greeks.theta < 0.0, "Theta should be negative");

        assert!(greeks.vega > 0.0, "Vega should be positive");
    }

    #[test]
    fn test_greeks_atm_put() {
        let price = bs_price(100.0, 100.0, 1.0, 0.05, 0.0, 0.2, OptionType::PE);
        let greeks =
            compute_greeks(100.0, 100.0, 1.0, 0.05, 0.0, price, OptionType::PE).unwrap();

        assert!(
            (greeks.delta - (-0.36)).abs() < 0.05,
            "Put Delta {} not close to -0.36",
            greeks.delta
        );
    }

    #[test]
    fn test_time_to_expiry() {
        let t = compute_time_to_expiry("2030-12-31");
        assert!(t.is_some());
        assert!(t.unwrap() > 0.0);
    }

    #[test]
    fn test_time_to_expiry_past() {
        let t = compute_time_to_expiry("2020-01-01");
        assert!(t.is_none());
    }
}
