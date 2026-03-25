use crate::models::Instrument;
use crate::websocket::TickEvent;

use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use chrono::{FixedOffset, TimeZone, Utc};
use tokio::sync::broadcast;
use tracing::{error, info, warn};


#[derive(Clone)]
enum TokenMeta {
    Equity {
        symbol: String,
        exchange: String,
    },
    Option {
        tradingsymbol: String,
        underlying: String,
        expiry: String,
        strike: f64,
        option_type: String,
    },
}


pub struct TickRecorder {
    rx: broadcast::Receiver<TickEvent>,
    token_map: HashMap<u32, TokenMeta>,
    equity_tokens: HashSet<u32>,
    option_tokens: HashSet<u32>,
    logs_dir: PathBuf,
}

impl TickRecorder {
    pub fn new(
        rx: broadcast::Receiver<TickEvent>,
        instruments: &[Instrument],
        equity_tokens: &[u32],
        option_tokens: &[u32],
        logs_dir: &str,
    ) -> Self {
        let eq_set: HashSet<u32> = equity_tokens.iter().copied().collect();
        let opt_set: HashSet<u32> = option_tokens.iter().copied().collect();

        let mut token_map: HashMap<u32, TokenMeta> = HashMap::new();

        for inst in instruments {
            let tok = inst.instrument_token;
            if eq_set.contains(&tok) {
                token_map.insert(
                    tok,
                    TokenMeta::Equity {
                        symbol: format!("{}:{}", inst.exchange, inst.tradingsymbol),
                        exchange: inst.exchange.clone(),
                    },
                );
            } else if opt_set.contains(&tok) {
                let otype = match inst.instrument_type.as_str() {
                    "CE" => "CE",
                    "PE" => "PE",
                    _ => "XX",
                };
                token_map.insert(
                    tok,
                    TokenMeta::Option {
                        tradingsymbol: inst.tradingsymbol.clone(),
                        underlying: inst.name.clone(),
                        expiry: inst.expiry.clone().unwrap_or_default(),
                        strike: inst.strike.unwrap_or(0.0),
                        option_type: otype.to_string(),
                    },
                );
            }
        }

        info!(
            "TickRecorder: mapped {} equity + {} option tokens",
            eq_set.len(),
            opt_set.len()
        );

        let dir = PathBuf::from(logs_dir);
        fs::create_dir_all(&dir).ok();

        Self {
            rx,
            token_map,
            equity_tokens: eq_set,
            option_tokens: opt_set,
            logs_dir: dir,
        }
    }

    pub fn spawn(mut self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            if let Err(e) = self.run().await {
                error!("TickRecorder crashed: {}", e);
            }
        })
    }

    async fn run(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let ist = FixedOffset::east_opt(5 * 3600 + 30 * 60).unwrap();
        let date_str = ist
            .from_utc_datetime(&Utc::now().naive_utc())
            .format("%Y-%m-%d")
            .to_string();

        let opt_path = self.logs_dir.join(format!("{}_options_ticks.csv", date_str));

        let opt_file = File::options()
            .create(true)
            .append(true)
            .open(&opt_path)?;

        let mut opt_writer = BufWriter::with_capacity(8 * 1024 * 1024, opt_file);

        // Equity file is only opened when equity tokens are actually configured.
        let eq_path = self.logs_dir.join(format!("{}_equity_ticks.csv", date_str));
        let mut eq_writer: Option<BufWriter<File>> = if !self.equity_tokens.is_empty() {
            let eq_file = File::options()
                .create(true)
                .append(true)
                .open(&eq_path)?;
            let mut w = BufWriter::with_capacity(8 * 1024 * 1024, eq_file);
            let is_new = eq_path.metadata().map(|m| m.len() == 0).unwrap_or(true);
            if is_new {
                writeln!(
                    w,
                    "recv_ts,token,symbol,exchange,ltp,open,high,low,close,volume,buy_qty,sell_qty,avg_price,last_qty,last_trade_ts,exchange_ts"
                )?;
            }
            Some(w)
        } else {
            None
        };

        let opt_is_new = opt_path.metadata().map(|m| m.len() == 0).unwrap_or(true);
        if opt_is_new {
            writeln!(
                opt_writer,
                "recv_ts,token,tradingsymbol,underlying,expiry,strike,option_type,ltp,open,high,low,close,oi,oi_day_high,oi_day_low,volume,avg_price,buy_qty,sell_qty,last_qty,exchange_ts"
            )?;
        }

        if eq_writer.is_some() {
            info!(
                "TickRecorder: writing to\n  {}\n  {}",
                eq_path.display(),
                opt_path.display()
            );
        } else {
            info!("TickRecorder: writing to {}", opt_path.display());
        }

        let mut tick_count: u64 = 0;
        let mut last_flush = tokio::time::Instant::now();
        let flush_interval = tokio::time::Duration::from_millis(500);

        loop {
            let timeout = tokio::time::sleep(flush_interval);
            tokio::pin!(timeout);

            tokio::select! {
                result = self.rx.recv() => {
                    match result {
                        Ok(event) => {
                            let recv_ts = ist
                                .from_utc_datetime(&Utc::now().naive_utc())
                                .format("%Y-%m-%dT%H:%M:%S%.6f")
                                .to_string();

                            for tick in &event.ticks {
                                let tok = tick.token;

                                if self.equity_tokens.contains(&tok) {
                                    if let (Some(w), Some(TokenMeta::Equity { symbol, exchange })) =
                                        (eq_writer.as_mut(), self.token_map.get(&tok))
                                    {
                                        writeln!(
                                            w,
                                            "{},{},{},{},{:.2},{:.2},{:.2},{:.2},{:.2},{},{},{},{:.2},{},{},{}",
                                            recv_ts,
                                            tok,
                                            symbol,
                                            exchange,
                                            tick.ltp,
                                            tick.ohlc.open,
                                            tick.ohlc.high,
                                            tick.ohlc.low,
                                            tick.ohlc.close,
                                            tick.volume,
                                            tick.buy_qty,
                                            tick.sell_qty,
                                            tick.avg_price,
                                            tick.last_qty,
                                            tick.last_trade_ts,
                                            tick.exchange_ts,
                                        )?;
                                        tick_count += 1;
                                    }
                                } else if self.option_tokens.contains(&tok) {
                                    if let Some(TokenMeta::Option {
                                        tradingsymbol,
                                        underlying,
                                        expiry,
                                        strike,
                                        option_type,
                                    }) = self.token_map.get(&tok)
                                    {
                                        writeln!(
                                            opt_writer,
                                            "{},{},{},{},{},{:.2},{},{:.2},{:.2},{:.2},{:.2},{:.2},{},{},{},{},{:.2},{},{},{},{}",
                                            recv_ts,
                                            tok,
                                            tradingsymbol,
                                            underlying,
                                            expiry,
                                            strike,
                                            option_type,
                                            tick.ltp,
                                            tick.ohlc.open,
                                            tick.ohlc.high,
                                            tick.ohlc.low,
                                            tick.ohlc.close,
                                            tick.oi,
                                            tick.oi_day_high,
                                            tick.oi_day_low,
                                            tick.volume,
                                            tick.avg_price,
                                            tick.buy_qty,
                                            tick.sell_qty,
                                            tick.last_qty,
                                            tick.exchange_ts,
                                        )?;
                                        tick_count += 1;
                                    }
                                }
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            warn!("TickRecorder lagged by {} messages — disk write too slow?", n);
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            break;
                        }
                    }

                    if last_flush.elapsed() >= flush_interval {
                        if let Some(w) = eq_writer.as_mut() { w.flush()?; }
                        opt_writer.flush()?;
                        last_flush = tokio::time::Instant::now();
                    }
                }
                _ = &mut timeout => {
                    if let Some(w) = eq_writer.as_mut() { w.flush()?; }
                    opt_writer.flush()?;
                    last_flush = tokio::time::Instant::now();
                }
            }
        }

        if let Some(w) = eq_writer.as_mut() { w.flush()?; }
        opt_writer.flush()?;

        info!(
            "TickRecorder: shutdown. Total ticks recorded: {}",
            tick_count
        );
        Ok(())
    }
}
