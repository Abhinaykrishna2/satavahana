//! Orchestration shell for the live microstructure trading engine (`--live`).
//!
//! Reuses the existing pipeline building blocks (auth, instrument master,
//! WebSocket ingestion, order executor) and adds the microstructure layer:
//! hourly capital sync, OBI/OFI + ask-depletion features, the two strategies, the
//! limit-chase OMS, and the 3:15 flatten + 1%/3% kill switches.
//!
//! A single position-manager task owns all risk/position state (fed by channels),
//! so strategies, fills, capital syncs, and the flatten clock can never race. In
//! paper mode fills are simulated against the live tape; in `--live` real orders
//! are routed to two executors (NSE/MIS for equities, NFO/NRML for options) and
//! positions open/close only on confirmed broker fills.

use crate::auth::KiteAuth;
use crate::capital::spawn_capital_sync;
use crate::cli::RunMode;
use crate::config::{Config, ExecutionConfig};
use crate::execution::spawn_order_executor;
use crate::models::{Instrument, OptionContract};
use crate::oms::{LiveBridge, ManagerConfig, PositionManager};
use crate::risk::{ist_now, is_at_or_past, RiskManager};
use crate::strategy::gamma::{GammaMeta, GammaParams};
use crate::strategy::imbalance::{EquityMeta, ImbalanceParams};
use crate::websocket::TickEvent;

use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::Datelike;
use tokio::sync::{broadcast, mpsc, watch};
use tracing::{info, warn};

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

/// Find NSE cash-equity tokens for the configured watchlist.
pub fn resolve_equity_metas(instruments: &[Instrument], watchlist: &[String], product: &str) -> Vec<EquityMeta> {
    let mut out = Vec::new();
    for sym in watchlist {
        let want = sym.trim().to_uppercase();
        let found = instruments.iter().find(|i| {
            i.exchange == "NSE" && i.instrument_type == "EQ" && i.tradingsymbol.eq_ignore_ascii_case(&want)
        });
        match found {
            Some(i) => {
                info!("  Strategy 1 leg: {} (NSE token {})", i.tradingsymbol, i.instrument_token);
                out.push(EquityMeta {
                    token: i.instrument_token,
                    symbol: i.tradingsymbol.clone(),
                    exchange: "NSE".to_string(),
                    product: product.to_string(),
                });
            }
            None => warn!("  Strategy 1: no NSE EQ instrument found for '{}'", sym),
        }
    }
    out
}

/// Estimate NIFTY spot from the index instrument, falling back to the chain median.
fn nifty_spot(instruments: &[Instrument], chain: &[OptionContract]) -> f64 {
    let idx = instruments.iter().find(|i| {
        i.exchange == "NSE"
            && i.segment == "INDICES"
            && (i.tradingsymbol.eq_ignore_ascii_case("NIFTY 50") || i.name.eq_ignore_ascii_case("NIFTY 50"))
    });
    if let Some(i) = idx {
        if i.last_price > 0.0 {
            return i.last_price;
        }
    }
    // Median strike as a last resort.
    let mut strikes: Vec<f64> = chain.iter().map(|c| c.strike).collect();
    strikes.sort_by(|a, b| a.partial_cmp(b).unwrap());
    strikes.get(strikes.len() / 2).copied().unwrap_or(0.0)
}

/// Pick the ATM (±1 strike) NIFTY CE/PE legs from an already-built options chain.
/// Used both by the standalone micro engine and the unified `main.rs` (which has
/// the chain in hand and need not re-fetch it).
pub fn gamma_metas_from_chain(
    instruments: &[Instrument],
    chain: &[OptionContract],
    nifty_symbol: &str,
) -> Vec<GammaMeta> {
    let nifty_legs: Vec<&OptionContract> = chain
        .iter()
        .filter(|c| c.underlying.eq_ignore_ascii_case(nifty_symbol))
        .collect();
    if nifty_legs.is_empty() {
        warn!("  Strategy 2: no NIFTY options in chain; gamma squeeze will have no legs");
        return Vec::new();
    }
    let spot = nifty_spot(instruments, chain);

    let mut strikes: Vec<f64> = nifty_legs.iter().map(|c| c.strike).collect();
    strikes.sort_by(|a, b| a.partial_cmp(b).unwrap());
    strikes.dedup_by(|a, b| (*a - *b).abs() < 1e-6);
    strikes.sort_by(|a, b| (a - spot).abs().partial_cmp(&(b - spot).abs()).unwrap());
    let atm_strikes: Vec<f64> = strikes.into_iter().take(3).collect();

    info!("  Strategy 2: NIFTY spot ≈ {:.1}, ATM strikes {:?}", spot, atm_strikes);

    let mut out = Vec::new();
    for c in nifty_legs {
        if atm_strikes.iter().any(|s| (s - c.strike).abs() < 1e-6) {
            out.push(GammaMeta {
                token: c.instrument_token,
                symbol: c.tradingsymbol.clone(),
                lot_size: c.lot_size,
            });
        }
    }
    info!("  Strategy 2: {} ATM option legs", out.len());
    out
}

/// Build the position manager from config and the resolved instrument legs.
fn build_manager(
    mode: RunMode,
    starting_capital: f64,
    config: &Config,
    eq_metas: &[EquityMeta],
    gamma_metas: &[GammaMeta],
) -> PositionManager {
    let mc = &config.microstructure;
    let risk = RiskManager::new(
        starting_capital,
        config.risk.per_trade_stop_pct,
        config.risk.daily_loss_circuit_pct,
        config.risk.daily_profit_circuit_pct,
    );
    let lookback = mc.obi_lookback.max(mc.gamma_ma_window).max(8);

    let mgr_cfg = ManagerConfig {
        max_concurrent_positions: mc.max_concurrent_positions,
        cooldown_ms: mc.signal_cooldown_secs * 1_000,
        mis_leverage: mc.mis_leverage,
        max_hold_ms: 30 * 60 * 1_000,
        max_repegs: config.execution.max_repegs,
        repeg_threshold_ticks: config.execution.repeg_tick_threshold,
        chase_timeout_ms: config.execution.chase_timeout_ms,
        min_repeg_interval_ms: config.execution.min_repeg_interval_ms,
        tick_size: 0.05,
        order_ttl_ms: config.execution.entry_order_timeout_secs * 1_000,
    };

    let mut m = PositionManager::new(mode, risk, lookback, mgr_cfg);

    if mc.equity_imbalance_enabled && !eq_metas.is_empty() {
        let params = ImbalanceParams {
            obi_threshold: mc.obi_z_threshold,
            require_ofi_positive: mc.ofi_confirm,
            ..ImbalanceParams::default()
        };
        m.enable_equity_imbalance(params);
        for meta in eq_metas {
            m.register_equity(meta.clone());
        }
    }

    if mc.gamma_squeeze_enabled && !gamma_metas.is_empty() {
        let params = GammaParams {
            depletion_multiple: mc.gamma_depletion_multiple,
            ..GammaParams::default()
        };
        m.enable_gamma(params);
        for meta in gamma_metas {
            m.register_gamma(meta.clone());
        }
    }

    let is_tuesday = ist_now().weekday() == chrono::Weekday::Tue;
    m.set_is_tuesday(is_tuesday);
    info!("  Today is {} (Tuesday gamma gate: {})", ist_now().weekday(), if is_tuesday { "OPEN" } else { "closed" });

    m
}

/// The single position-manager task. Owns the manager; consumes ticks, capital
/// updates, and broker order updates. It checks the flatten clock itself on a
/// periodic tick (so a one-shot cross-task signal can't be missed while it is
/// mid-`on_tick`), and shuts down on a latched `watch` change.
#[allow(clippy::too_many_arguments)]
async fn run_manager(
    mut manager: PositionManager,
    tracked: HashSet<u32>,
    flatten_time: String,
    mut tick_rx: broadcast::Receiver<TickEvent>,
    mut capital_rx: mpsc::UnboundedReceiver<f64>,
    mut order_rx: mpsc::UnboundedReceiver<(String, String, Option<f64>)>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    info!("  Position manager task started ({} tokens tracked)", tracked.len());
    let mut flatten_clock = tokio::time::interval(std::time::Duration::from_secs(2));
    let mut shutdown_requested = false;
    let mut shutdown_deadline_ms = 0_u64;
    loop {
        tokio::select! {
            ev = tick_rx.recv() => match ev {
                Ok(ev) => {
                    let now = now_ms();
                    for t in &ev.ticks {
                        if tracked.contains(&t.token) {
                            if let Some(depth) = &t.depth {
                                manager.on_tick(t.token, depth, now);
                            }
                        }
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => warn!("  Manager lagged {} tick batches", n),
                Err(broadcast::error::RecvError::Closed) => break,
            },
            Some(funds) = capital_rx.recv() => manager.on_capital(funds),
            Some((tag, status, avg)) = order_rx.recv() => {
                manager.on_order_update(&tag, &status, avg, now_ms());
                if shutdown_requested && manager.shutdown_ready() {
                    break;
                }
            },
            _ = flatten_clock.tick() => {
                // Self-checked, latched flatten: reliably fires even if a tick was
                // being processed when 15:15 rolled over.
                if is_at_or_past(ist_now(), &flatten_time) && manager.flatten_once(now_ms()) {
                    warn!("  ⏰ {} IST flatten executed by manager.", flatten_time);
                }
                if shutdown_requested {
                    let now = now_ms();
                    if manager.shutdown_ready() {
                        break;
                    }
                    if shutdown_deadline_ms > 0 && now >= shutdown_deadline_ms {
                        warn!("  Shutdown flatten confirmation window elapsed; writing summary with any broker-pending state still tracked.");
                        break;
                    }
                }
            }
            res = shutdown_rx.changed() => {
                if res.is_err() || *shutdown_rx.borrow() {
                    shutdown_requested = true;
                    let now = now_ms();
                    manager.flatten_once(now_ms());
                    shutdown_deadline_ms = now.saturating_add(30_000);
                    if manager.shutdown_ready() {
                        break;
                    }
                }
            }
        }
    }
    write_session_summary(&manager);
}

/// Forward an executor's `OrderUpdate` stream into the manager's order channel.
fn spawn_update_forwarder(
    mut rx: mpsc::UnboundedReceiver<crate::execution::OrderUpdate>,
    tx: mpsc::UnboundedSender<(String, String, Option<f64>)>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(u) = rx.recv().await {
            if let Some(status) = u.status {
                let _ = tx.send((u.tag, status, u.average_price));
            }
        }
    })
}

fn write_session_summary(manager: &PositionManager) {
    let trades = manager.closed_trades();
    let _ = std::fs::create_dir_all("logs");
    let date = ist_now().format("%Y-%m-%d");
    let path = format!("logs/{}_microstructure_trades.csv", date);
    let mut csv = String::from(
        "opened_ms,closed_ms,strategy,symbol,side,qty,entry,exit,gross_pnl,cost,net_pnl,reason,rationale\n",
    );
    let (mut wins, mut losses, mut net) = (0, 0, 0.0);
    for t in trades {
        if t.net_pnl >= 0.0 { wins += 1; } else { losses += 1; }
        net += t.net_pnl;
        csv.push_str(&format!(
            "{},{},{},{},{:?},{},{:.2},{:.2},{:.2},{:.2},{:.2},{},\"{}\"\n",
            t.opened_ms, t.closed_ms, t.kind.label(), t.symbol, t.side, t.qty,
            t.entry_price, t.exit_price, t.gross_pnl, t.cost, t.net_pnl, t.reason.label(), t.rationale,
        ));
    }
    if let Err(e) = std::fs::write(&path, csv) {
        warn!("  Could not write trades CSV {}: {}", path, e);
    }
    info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    info!("  SESSION SUMMARY ({} trades)", trades.len());
    info!("    Wins/Losses: {}/{}", wins, losses);
    info!("    Realized P&L (net of costs): ₹{:.2}", net);
    info!("    Final capital basis: ₹{:.2}", manager.capital());
    info!("    Trades CSV: {}", path);
    info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
}

/// Spawn the microstructure manager + its two order executors + capital sync onto
/// an existing shared tick broadcast. Returns the spawned task handles and a
/// shutdown sender (`send(true)` to flatten, square off, and write the summary).
///
/// Used standalone by [`run`] and by the unified `main.rs` orchestration, where it
/// shares the same WebSocket/tick stream as the options + quant engines.
pub fn spawn_micro_engine(
    config: &Config,
    mode: RunMode,
    auth: &KiteAuth,
    eq_metas: Vec<EquityMeta>,
    gamma_metas: Vec<GammaMeta>,
    starting_capital: f64,
    tx: &broadcast::Sender<TickEvent>,
    shared_circuit: Option<crate::portfolio::SharedCircuit>,
) -> (Vec<tokio::task::JoinHandle<()>>, watch::Sender<bool>) {
    let mut tracked: HashSet<u32> = HashSet::new();
    for m in &eq_metas {
        tracked.insert(m.token);
    }
    for m in &gamma_metas {
        tracked.insert(m.token);
    }

    let mut manager = build_manager(mode, starting_capital, config, &eq_metas, &gamma_metas);
    if let Some(c) = shared_circuit {
        manager.set_shared_circuit(c);
    }
    info!(
        "  Microstructure per-trade budget ₹{:.2}",
        manager.capital() * config.risk.per_trade_stop_pct / 100.0
    );

    let (order_tx, order_rx) = mpsc::unbounded_channel::<(String, String, Option<f64>)>();
    let mut handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    if mode.is_live() {
        let mut opt_cfg: ExecutionConfig = config.execution.clone();
        opt_cfg.enable_live_orders = true;
        opt_cfg.exchange = "NFO".to_string();
        opt_cfg.product = "NRML".to_string();
        opt_cfg.order_type = "LIMIT".to_string();

        let eq_tx = if !eq_metas.is_empty() {
            let mut eq_cfg: ExecutionConfig = config.execution.clone();
            eq_cfg.enable_live_orders = true;
            eq_cfg.exchange = config.execution.equity_exchange.clone();
            eq_cfg.product = config.execution.equity_product.clone();
            eq_cfg.order_type = "LIMIT".to_string();
            let (eq_tx, eq_upd, eq_h) =
                spawn_order_executor(auth.api_key.clone(), auth.access_token.clone(), eq_cfg);
            handles.push(eq_h);
            handles.push(spawn_update_forwarder(eq_upd, order_tx.clone()));
            info!(
                "  Live micro equity executor: ACTIVE ({}/{})",
                config.execution.equity_exchange, config.execution.equity_product
            );
            eq_tx
        } else {
            let (eq_tx, _closed_rx) = mpsc::unbounded_channel();
            info!("  Live micro equity executor: disabled (options-only mode)");
            eq_tx
        };
        let (opt_tx, opt_upd, opt_h) =
            spawn_order_executor(auth.api_key.clone(), auth.access_token.clone(), opt_cfg);
        handles.push(opt_h);
        handles.push(spawn_update_forwarder(opt_upd, order_tx.clone()));
        manager.arm_live(
            LiveBridge {
                equity_tx: eq_tx,
                options_tx: opt_tx,
            },
            config.execution.order_tag_prefix.clone(),
        );
        info!("  Live micro executors armed: options NFO/NRML");
    }

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (cap_tx, cap_rx) = mpsc::unbounded_channel::<f64>();
    handles.push(spawn_capital_sync(
        auth.api_key.clone(),
        auth.access_token.clone(),
        cap_tx,
        config.risk.capital_sync_start.clone(),
        config.risk.capital_poll_minute,
    ));
    handles.push(tokio::spawn(run_manager(
        manager,
        tracked,
        config.risk.flatten_time.clone(),
        tx.subscribe(),
        cap_rx,
        order_rx,
        shutdown_rx,
    )));

    (handles, shutdown_tx)
}

/// Spawn the quant engine (4 strategies, simulated execution) driven off the
/// shared tick broadcast. It reads the shared `TickStore` and scans on its own
/// internal cadence. Live order routing for quant is a later phase.
pub fn spawn_quant_engine(
    options_chain: Vec<OptionContract>,
    store: crate::store::TickStore,
    capital: f64,
    tx: &broadcast::Sender<TickEvent>,
) -> tokio::task::JoinHandle<()> {
    let mut engine = crate::quant_engine::QuantEngine::new(options_chain, store, capital, "logs");
    engine.set_warmup_until_ms(now_ms() + 3 * 60 * 1_000);
    let mut rx = tx.subscribe();
    tokio::spawn(async move {
        info!("  Quant engine: ACTIVE (OFI/OIV/CD/SOA, simulated execution, ₹{:.0})", capital);
        loop {
            match rx.recv().await {
                Ok(_ev) => engine.on_tick(now_ms()),
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => {
                    engine.finalize("shutdown");
                    break;
                }
            }
        }
    })
}
