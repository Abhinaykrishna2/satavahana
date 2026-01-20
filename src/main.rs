mod auth;
mod backtest;
mod config;
mod execution;
mod greeks;
mod instruments;
mod ledger;
mod models;
mod options_engine;
mod recorder;
mod store;
mod websocket;

use auth::KiteAuth;
use config::Config;
use execution::{fetch_live_available_funds, spawn_order_executor};
use greeks::GreeksEngine;
use instruments::{build_equity_pairs, build_options_chain, fetch_instruments};
use options_engine::OptionsEngine;
use recorder::TickRecorder;
use store::TickStore;
use websocket::{TickEvent, WsConnection};

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{error, info, warn};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_target(false)
        .init();

    info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    info!("  SATAVAHANA — Real-Time Market Data Pipeline");
    info!("  Zerodha Kite Connect v3");
    info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let config = Config::load("config.toml").map_err(|e| {
        error!("Failed to load config.toml: {}", e);
        error!("Copy config.toml.example to config.toml and fill in your Kite credentials");
        let boxed: Box<dyn std::error::Error + Send + Sync> = e.to_string().into();
        boxed
    })?;

    info!("Configuration loaded successfully");
    info!(
        "  Equity watchlist: {:?}",
        config.equities.symbols
    );
    info!(
        "  Options: {:?} expiry {} strikes {:.0}-{:.0}",
        config.options.underlyings,
        config.options.expiry,
        config.options.strike_min,
        config.options.strike_max,
    );

    let access_token = if config.kite.access_token.is_empty() {
        info!("No access token found — starting automated Zerodha OAuth login...");
        KiteAuth::run_login(&config.kite.api_key, &config.kite.api_secret)
            .await
            .map_err(|e| {
                error!("OAuth login failed: {}", e);
                let boxed: Box<dyn std::error::Error + Send + Sync> = e;
                boxed
            })?
    } else {
        info!("Using access token from config.toml");
        config.kite.access_token.clone()
    };

    let auth = KiteAuth::new(config.kite.api_key.clone(), access_token);

    info!("Fetching instrument master list...");
    let instruments = fetch_instruments(&auth).await.map_err(|e| {
        let boxed: Box<dyn std::error::Error + Send + Sync> = e;
        boxed
    })?;
    info!("  Total instruments: {}", instruments.len());

    let equity_pairs = build_equity_pairs(&instruments, &config.equities);
    info!("  Equity pairs (NSE↔BSE): {}", equity_pairs.len());
    for pair in &equity_pairs {
        info!("    → {}", pair);
    }

    let options_chain = build_options_chain(&instruments, &config.options);
    info!("  Options contracts: {}", options_chain.len());

    if equity_pairs.is_empty() && options_chain.is_empty() {
        error!("No instruments to subscribe to! Check your config and instrument list.");
        return Ok(());
    }

    let mut equity_tokens: Vec<u32> = Vec::new();
    for pair in &equity_pairs {
        equity_tokens.push(pair.nse_token);
        equity_tokens.push(pair.bse_token);
    }

    let option_tokens: Vec<u32> = options_chain.iter().map(|c| c.instrument_token).collect();

    let mut underlying_tokens: HashMap<String, u32> = HashMap::new();
    for underlying in &config.options.underlyings {
        let aliases: &[&str] = match underlying.as_str() {
            "NIFTY" => &["NIFTY 50", "NIFTY50", "NIFTY"],
            "BANKNIFTY" => &["NIFTY BANK", "BANKNIFTY"],
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
            underlying_tokens.insert(underlying.clone(), inst.instrument_token);
            info!(
                "    Underlying {} → token {} ({})",
                underlying, inst.instrument_token, inst.tradingsymbol
            );
            continue;
        }

        let spot_eq_match = instruments.iter().find(|inst| {
            inst.exchange == "NSE"
                && inst.instrument_type == "EQ"
                && aliases.iter().any(|a| inst.tradingsymbol.eq_ignore_ascii_case(a))
        });

        if let Some(inst) = spot_eq_match {
            underlying_tokens.insert(underlying.clone(), inst.instrument_token);
            info!(
                "    Underlying {} → token {} ({}) [EQ fallback]",
                underlying, inst.instrument_token, inst.tradingsymbol
            );
        } else {
            info!("    Underlying {} → no direct spot/index token found; will use synthetic spot fallback", underlying);
        }
    }

    let mut all_option_tokens = option_tokens.clone();
    for token in underlying_tokens.values() {
        if !all_option_tokens.contains(token) {
            all_option_tokens.push(*token);
        }
    }

    let mut all_hot_tokens = option_tokens.clone();
    for t in underlying_tokens.values() {
        if !all_hot_tokens.contains(t) {
            all_hot_tokens.push(*t);
        }
    }

    let store = if !all_hot_tokens.is_empty() {
        info!(
            "Initializing hot-path TickStore with {} pre-allocated slots...",
            all_hot_tokens.len()
        );
        TickStore::new_with_hot_tokens(&all_hot_tokens)
    } else {
        TickStore::new()
    };

    let (tx, _) = broadcast::channel::<TickEvent>(4096);

    let rec_underlying_tokens: Vec<u32> = underlying_tokens.values().copied().collect();
    let rec_option_tokens = option_tokens.clone();

    info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    info!("Starting WebSocket connections...");

    let ws_url = auth.ws_url();
    let mut handles = Vec::new();
    let mut live_order_tx = None;
    let mut live_order_updates_rx = None;
    let mut live_starting_capital_override = None;

    if config.execution.enable_live_orders {
        match fetch_live_available_funds(&auth.api_key, &auth.access_token).await {
            Ok(funds) => {
                live_starting_capital_override = Some(funds);
                info!("  Live funds detected from Kite margins: ₹{:.2}", funds);
            }
            Err(e) => {
                warn!(
                    "  Could not fetch live funds from Kite margins ({}); falling back to config capital",
                    e
                );
            }
        }

        let (tx, updates_rx, handle) = spawn_order_executor(
            auth.api_key.clone(),
            auth.access_token.clone(),
            config.execution.clone(),
        );
        handles.push(handle);
        live_order_tx = Some(tx);
        live_order_updates_rx = Some(updates_rx);
        info!(
            "  Live Order Executor: ACTIVE ({} {} {} {})",
            config.execution.exchange,
            config.execution.product,
            config.execution.order_type,
            config.execution.validity
        );
    } else {
        info!("  Live Order Executor: DISABLED (signals + simulated position tracking only)");
    }


    if !all_option_tokens.is_empty() {
        info!(
            "  Connection 1 (Options): {} tokens in 'full' mode",
            all_option_tokens.len()
        );
        let conn = Arc::new(WsConnection {
            name: "Options".to_string(),
            ws_url: ws_url.clone(),
            tokens: all_option_tokens,
            mode: "full".to_string(),
            tx: tx.clone(),
        });
        handles.push(conn.spawn());
    }

    let store_clone = store.clone();
    let mut store_rx = tx.subscribe();
    let _store_handle = std::thread::spawn(move || {
        let core_ids = core_affinity::get_core_ids().unwrap_or_default();
        if let Some(core_id) = core_ids.first() {
            if core_affinity::set_for_current(*core_id) {
                info!("📌 Store updater pinned to CPU core {:?}", core_id);
            }
        }

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async move {
            loop {
                match store_rx.recv().await {
                    Ok(event) => {
                        store_clone.update_batch(&event.ticks);
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!("Store updater lagged by {} messages", n);
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        })
    });

    info!("  Spread Engine: DISABLED (options-only mode)");

    if !options_chain.is_empty() {
        let greeks_engine = GreeksEngine::new(
            options_chain.clone(),
            store.clone(),
            config.greeks.risk_free_rate,
            config.greeks.dividend_yield,
            config.output.greeks_log_interval,
            underlying_tokens.clone(),
        );
        let greeks_rx = tx.subscribe();
        handles.push(greeks_engine.spawn(greeks_rx));
        info!("  Greeks Engine: ACTIVE");

        let mut oecfg = config.options_engine.clone();
        if let Some(funds) = live_starting_capital_override {
            oecfg.initial_capital = funds.max(0.0);
        }
        let mut options_signal_engine = OptionsEngine::new(
            options_chain,
            store.clone(),
            underlying_tokens,
            &oecfg,
            config.greeks.risk_free_rate,
            config.greeks.dividend_yield,
            "backtest",
        );
        if let Some(tx) = live_order_tx.clone() {
            options_signal_engine.set_live_order_bridge(
                tx,
                live_order_updates_rx.take(),
                config.execution.order_tag_prefix.clone(),
            );
            info!(
                "  Options execution bridge: ACTIVE (tag prefix '{}')",
                config.execution.order_tag_prefix
            );
        }
        let oe_rx = tx.subscribe();
        handles.push(options_signal_engine.spawn(oe_rx));
        info!("  Options Signal Engine: ACTIVE (₹{:.0} capital, scan every {}s)",
            oecfg.initial_capital, oecfg.scan_interval_secs);
    }

    {
        let rec_rx = tx.subscribe();
        let recorder = TickRecorder::new(
            rec_rx,
            &instruments,
            &rec_underlying_tokens,
            &rec_option_tokens,
            "data",
        );
        handles.push(recorder.spawn());
        info!("  Tick Recorder: ACTIVE (equity_ticks + options_ticks CSV)");
    }

    info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    info!("Pipeline running. Press Ctrl+C to stop.");
    info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    info!("Performance optimizations enabled:");
    info!("  ✓ Lock-free atomic tick store (hot path)");
    info!("  ✓ O(1) token-to-pair routing");
    info!("  ✓ CPU core pinning for critical threads");
    info!("  ✓ Cache-aligned memory layout");
    info!("  ✓ Zero-allocation spread calculations");

    tokio::signal::ctrl_c().await?;
    info!("Shutting down...");

    drop(tx);

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    info!("Pipeline stopped. Goodbye!");
    Ok(())
}
