use crate::backtest::{BacktestEngine, SpreadSignal};
use crate::ledger::EquityJournal;
use crate::models::EquityPair;
use crate::store::TickStore;
use crate::websocket::TickEvent;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;
use tracing::{info, warn};

#[derive(Clone)]
struct PairIndex {
    pair_idx: usize,
    is_nse: bool,
}

pub struct SpreadEngine {
    pub pairs: Vec<EquityPair>,
    pub store: TickStore,
    pub min_spread_alert: f64,
    token_to_pair: HashMap<u32, PairIndex>,
    backtest_engine: Option<Arc<Mutex<BacktestEngine>>>,
    equity_journal: std::sync::Mutex<Option<EquityJournal>>,
}

impl SpreadEngine {
    pub fn new(pairs: Vec<EquityPair>, store: TickStore, min_spread_alert: f64) -> Self {
        Self::new_with_backtest(pairs, store, min_spread_alert, None)
    }

    pub fn new_with_backtest(
        pairs: Vec<EquityPair>,
        store: TickStore,
        min_spread_alert: f64,
        backtest_engine: Option<Arc<Mutex<BacktestEngine>>>,
    ) -> Self {
        let mut token_to_pair = HashMap::with_capacity(pairs.len() * 2);

        for (idx, pair) in pairs.iter().enumerate() {
            token_to_pair.insert(
                pair.nse_token,
                PairIndex {
                    pair_idx: idx,
                    is_nse: true,
                },
            );
            token_to_pair.insert(
                pair.bse_token,
                PairIndex {
                    pair_idx: idx,
                    is_nse: false,
                },
            );
        }

        let equity_journal_inner = EquityJournal::new("backtest")
            .map_err(|e| warn!("Could not open equity journal: {}", e))
            .ok();

        Self {
            pairs,
            store,
            min_spread_alert,
            token_to_pair,
            backtest_engine,
            equity_journal: std::sync::Mutex::new(equity_journal_inner),
        }
    }

    pub fn spawn(
        self,
        mut rx: broadcast::Receiver<TickEvent>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            info!(
                "Spread engine started — monitoring {} equity pairs (alert threshold: ₹{:.2})",
                self.pairs.len(),
                self.min_spread_alert
            );

            loop {
                match rx.recv().await {
                    Ok(event) => {
                        if let Some(ref bt_engine) = self.backtest_engine {
                            let now = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap()
                                .as_micros() as u64;

                            let mut bt = bt_engine.lock().unwrap();
                            for tick in &event.ticks {
                                bt.record_tick(tick, now);
                            }
                        }

                        for tick in &event.ticks {
                            if let Some(pair_idx) = self.token_to_pair.get(&tick.token) {
                                self.compute_spread_for_pair(pair_idx.pair_idx);
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!("Spread engine lagged by {} messages", n);
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        info!("Spread engine channel closed, writing equity summary");
                        if let Ok(mut guard) = self.equity_journal.lock() {
                            if let Some(ref mut j) = *guard {
                                j.flush();
                                j.write_equity_summary();
                            }
                        }
                        break;
                    }
                }
            }
        })
    }

    #[inline(always)]
    fn compute_spread_for_pair(&self, pair_idx: usize) {
        let pair = &self.pairs[pair_idx];

        let nse_ltp = match self.store.get_ltp(pair.nse_token) {
            Some(ltp) if ltp > 0.0 => ltp,
            _ => return,
        };

        let bse_ltp = match self.store.get_ltp(pair.bse_token) {
            Some(ltp) if ltp > 0.0 => ltp,
            _ => return,
        };

        let spread = nse_ltp - bse_ltp;
        let spread_pct = (spread / nse_ltp) * 100.0;
        let abs_spread = spread.abs();

        let nse_brokerage = (0.0003 * nse_ltp).min(20.0);
        let bse_brokerage = (0.0003 * bse_ltp).min(20.0);
        let total_brokerage = nse_brokerage + bse_brokerage;
        
        let sell_price: f64 = if nse_ltp > bse_ltp { nse_ltp } else { bse_ltp };
        let stt = 0.00025 * sell_price;
        
        let nse_txn = 0.0000297 * nse_ltp;
        let bse_txn = 0.0000375 * bse_ltp;
        let txn_charges = nse_txn + bse_txn;
        
        let sebi = 0.000001 * (nse_ltp + bse_ltp);
        
        let buy_price: f64 = if nse_ltp < bse_ltp { nse_ltp } else { bse_ltp };
        let stamp_duty = 0.00003 * buy_price;
        
        let gst = 0.18 * (total_brokerage + txn_charges + sebi);
        
        let estimated_trade_cost = total_brokerage + stt + txn_charges + sebi + stamp_duty + gst;
        let net_spread = abs_spread - estimated_trade_cost;

        if net_spread >= self.min_spread_alert {
            let nse_tick = self.store.get(pair.nse_token);
            let bse_tick = self.store.get(pair.bse_token);

            match (nse_tick, bse_tick) {
                (Some(nse), Some(bse)) => {
                    if !self.validate_execution_depth(&nse, &bse, abs_spread) {
                        return;
                    }

                    let direction = if spread > 0.0 {
                        "NSE > BSE (Buy BSE, Sell NSE)"
                    } else {
                        "BSE > NSE (Buy NSE, Sell BSE)"
                    };

                    let depth_info = match (&nse.depth, &bse.depth) {
                        (Some(nd), Some(bd)) => {
                            format!(
                                " | NSE Bid:{:.2}@{} Ask:{:.2}@{} | BSE Bid:{:.2}@{} Ask:{:.2}@{}",
                                nd.bids[0].price,
                                nd.bids[0].quantity,
                                nd.asks[0].price,
                                nd.asks[0].quantity,
                                bd.bids[0].price,
                                bd.bids[0].quantity,
                                bd.asks[0].price,
                                bd.asks[0].quantity
                            )
                        }
                        _ => String::new(),
                    };

                    info!(
                        "🔥 ARBIT | {} | NSE: ₹{:.2} | BSE: ₹{:.2} | Gross: ₹{:.2} | Net: ₹{:.2} ({:.4}%) | {} | Vol NSE:{} BSE:{}{}",
                        pair.symbol,
                        nse_ltp,
                        bse_ltp,
                        abs_spread,
                        net_spread,
                        spread_pct,
                        direction,
                        nse.volume,
                        bse.volume,
                        depth_info
                    );

                    if let Ok(mut journal_guard) = self.equity_journal.lock() {
                        if let Some(ref mut journal) = *journal_guard {
                            let (nse_bid, nse_ask, bse_bid, bse_ask) = match (&nse.depth, &bse.depth) {
                                (Some(nd), Some(bd)) => (nd.bids[0].price, nd.asks[0].price, bd.bids[0].price, bd.asks[0].price),
                                _ => (nse_ltp, nse_ltp, bse_ltp, bse_ltp),
                            };
                            let est_pnl_per_share = net_spread;
                            journal.log_alert(
                                &pair.symbol,
                                direction,
                                nse_ltp,
                                bse_ltp,
                                abs_spread,
                                net_spread,
                                spread_pct,
                                nse.volume,
                                bse.volume,
                                est_pnl_per_share,
                                nse_bid,
                                nse_ask,
                                bse_bid,
                                bse_ask,
                            );
                        }
                    }

                    if let Some(ref bt_engine) = self.backtest_engine {
                        let signal = SpreadSignal {
                            timestamp_us: std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap()
                                .as_micros() as u64,
                            symbol: pair.symbol.clone(),
                            nse_token: pair.nse_token,
                            bse_token: pair.bse_token,
                            nse_ltp_at_signal: nse_ltp,
                            bse_ltp_at_signal: bse_ltp,
                            spread_at_signal: spread,
                            quantity: 100,
                        };

                        let mut bt = bt_engine.lock().unwrap();
                        let _trade = bt.simulate_trade(signal);
                    }
                }
                _ => {
                }
            }
        }
    }

    #[inline(always)]
    fn validate_execution_depth(
        &self,
        nse: &crate::models::Tick,
        bse: &crate::models::Tick,
        _spread: f64,
    ) -> bool {
        match (&nse.depth, &bse.depth) {
            (Some(nse_depth), Some(bse_depth)) => {

                let nse_bid_qty = nse_depth.bids[0].quantity;
                let nse_ask_qty = nse_depth.asks[0].quantity;

                let bse_bid_qty = bse_depth.bids[0].quantity;
                let bse_ask_qty = bse_depth.asks[0].quantity;

                const MIN_QTY: u32 = 10;

                if nse_bid_qty < MIN_QTY || nse_ask_qty < MIN_QTY {
                    return false;
                }

                if bse_bid_qty < MIN_QTY || bse_ask_qty < MIN_QTY {
                    return false;
                }

                true
            }
            _ => {
                false
            }
        }
    }
}
