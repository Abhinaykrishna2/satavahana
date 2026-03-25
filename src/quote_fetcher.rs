use crate::auth::KiteAuth;
use crate::models::Instrument;
use chrono::{DateTime, FixedOffset, TimeZone, Timelike, Utc};
use reqwest::Client;
use serde_json::Value;
use std::collections::HashMap;
use tracing::{info, warn};
use std::time::Duration;

pub async fn fetch_live_quotes(
    auth: &KiteAuth,
    instruments: &[Instrument],
    underlyings: &[String],
) -> HashMap<String, f64> {
    let mut spot_estimates: HashMap<String, f64> = HashMap::new();
    let ist_offset = FixedOffset::east_opt(5 * 3600 + 30 * 60).expect("valid IST offset");
    
    // Wait until 09:15:05 IST
    loop {
        let now_utc = Utc::now();
        let now_ist = now_utc.with_timezone(&ist_offset);
        let time_val = now_ist.hour() * 10000 + now_ist.minute() * 100 + now_ist.second();
        
        // if time is before 9:15:05, sleep
        if time_val < 91505 {
            let target_time = now_ist.date_naive().and_hms_opt(9, 15, 6).unwrap();
            let target_ist = ist_offset.from_local_datetime(&target_time).unwrap();
            let wait_duration = target_ist.timestamp() - now_utc.timestamp();
            
            if wait_duration > 0 {
                info!("Waiting {} seconds until 09:15:05 IST to fetch live spot...", wait_duration);
                tokio::time::sleep(Duration::from_secs(wait_duration as u64)).await;
            }
        } else {
            break;
        }
    }

    let mut query_params = Vec::new();
    let mut underlying_to_symbol: HashMap<String, String> = HashMap::new();

    for underlying in underlyings {
        let aliases: &[&str] = match underlying.as_str() {
            "NIFTY" => &["NIFTY 50", "NIFTY50", "NIFTY"],
            "NIFTYNXT50" => &["NIFTY NEXT 50", "NIFTYNXT50"],
            _ => &[underlying.as_str()],
        };

        let index_match = instruments.iter().find(|inst| {
            inst.exchange == "NSE"
                && inst.segment == "INDICES"
                && aliases.iter().any(|a| {
                    inst.tradingsymbol.eq_ignore_ascii_case(a) || inst.name.eq_ignore_ascii_case(a)
                })
        });

        if let Some(inst) = index_match {
            let symbol = format!("{}:{}", inst.exchange, inst.tradingsymbol);
            query_params.push(("i", symbol.clone()));
            underlying_to_symbol.insert(symbol, underlying.clone());
        }
    }

    if query_params.is_empty() {
        return spot_estimates;
    }

    info!("Fetching live quotes for {} indices...", query_params.len());
    
    let client = Client::new();
    let res = client
        .get("https://api.kite.trade/quote")
        .header("X-Kite-Version", "3")
        .header("Authorization", format!("token {}:{}", auth.api_key, auth.access_token))
        .query(&query_params)
        .send()
        .await;

    match res {
        Ok(response) => {
            if response.status().is_success() {
                if let Ok(json) = response.json::<Value>().await {
                    if let Some(data) = json.get("data").and_then(|d| d.as_object()) {
                        for (symbol, quote_data) in data {
                            if let Some(last_price) = quote_data.get("last_price").and_then(|lp| lp.as_f64()) {
                                if let Some(underlying) = underlying_to_symbol.get(symbol) {
                                    spot_estimates.insert(underlying.clone(), last_price);
                                    info!("  {} live spot: {:.2}", underlying, last_price);
                                }
                            }
                        }
                    }
                }
            } else {
                warn!("Failed to fetch live quotes: HTTP {}", response.status());
            }
        }
        Err(e) => {
            warn!("Network error fetching live quotes: {}", e);
        }
    }

    spot_estimates
}
