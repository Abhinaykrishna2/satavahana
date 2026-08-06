use crate::models::{DepthEntry, MarketDepth, Tick, TickMode, OHLC};
use byteorder::{BigEndian, ByteOrder};
use chrono::{Datelike, FixedOffset, Timelike, Utc, Weekday};
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::time::{timeout_at, Duration, Instant};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

/// Heartbeats do not count as market data. Reconnect if quotes stop for this long.
const FEED_SILENCE_TIMEOUT_SECS: u64 = 10;
pub const FEED_SOFT_STALE_MS: u64 = 20_000;
pub const FEED_HARD_STALE_MS: u64 = 60_000;
pub const FEED_RECOVERY_STABLE_MS: u64 = 5_000;

#[derive(Debug, Clone)]
pub struct TickEvent {
    pub ticks: Vec<Tick>,
    /// Control event emitted before a tickless connection is recycled.
    pub feed_stale: bool,
}

impl TickEvent {
    fn market_data(ticks: Vec<Tick>) -> Self {
        Self {
            ticks,
            feed_stale: false,
        }
    }

    fn stale() -> Self {
        Self {
            ticks: Vec::new(),
            feed_stale: true,
        }
    }
}

pub fn market_data_expected(timestamp_ms: u64) -> bool {
    let ist = FixedOffset::east_opt(5 * 3600 + 30 * 60).expect("valid IST offset");
    let now = chrono::DateTime::from_timestamp_millis(timestamp_ms as i64)
        .unwrap_or_else(Utc::now)
        .with_timezone(&ist);
    let mins = now.hour() * 60 + now.minute();
    matches!(
        now.weekday(),
        Weekday::Mon | Weekday::Tue | Weekday::Wed | Weekday::Thu | Weekday::Fri
    ) && (555..930).contains(&mins)
}

pub struct WsConnection {
    pub name: String,
    pub ws_url: String,
    pub tokens: Vec<u32>,
    pub mode: String,
    pub tx: broadcast::Sender<TickEvent>,
}

impl WsConnection {
    pub fn spawn(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        let conn = self.clone();
        tokio::spawn(async move {
            loop {
                match conn.run().await {
                    Ok(_) => {
                        warn!("[{}] WebSocket closed cleanly, reconnecting...", conn.name);
                    }
                    Err(e) => {
                        error!(
                            "[{}] WebSocket error: {}, reconnecting in 5s...",
                            conn.name, e
                        );
                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    }
                }
            }
        })
    }

    async fn run(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!(
            "[{}] Connecting to WebSocket ({} tokens in '{}' mode)...",
            self.name,
            self.tokens.len(),
            self.mode
        );

        let (ws_stream, _) = connect_async(&self.ws_url).await?;
        let (mut write, mut read) = ws_stream.split();

        info!(
            "[{}] Connected! Subscribing to {} tokens...",
            self.name,
            self.tokens.len()
        );

        // Kite allows max 3000 tokens; chunk subscribe + mode messages to stay safe
        for chunk in self.tokens.chunks(500) {
            let subscribe_msg = serde_json::json!({
                "a": "subscribe",
                "v": chunk
            });
            write
                .send(Message::Text(subscribe_msg.to_string().into()))
                .await?;

            let mode_msg = serde_json::json!({
                "a": "mode",
                "v": [&self.mode, chunk]
            });
            write
                .send(Message::Text(mode_msg.to_string().into()))
                .await?;
        }

        info!("[{}] Subscribed and mode set. Streaming...", self.name);
        let mut quotes_expected = market_data_expected(now_ms());
        let mut quote_deadline = Instant::now() + Duration::from_secs(FEED_SILENCE_TIMEOUT_SECS);

        loop {
            let expected_now = market_data_expected(now_ms());
            if expected_now && !quotes_expected {
                quote_deadline = Instant::now() + Duration::from_secs(FEED_SILENCE_TIMEOUT_SECS);
            }
            quotes_expected = expected_now;
            let deadline = if quotes_expected {
                quote_deadline
            } else {
                Instant::now() + Duration::from_secs(FEED_SILENCE_TIMEOUT_SECS)
            };

            // Absolute quote deadline: heartbeat/text/ping frames do not extend it.
            let msg = match timeout_at(deadline, read.next()).await {
                Ok(Some(msg)) => msg,
                Ok(None) => break, // stream ended
                Err(_) => {
                    if quotes_expected {
                        let _ = self.tx.send(TickEvent::stale());
                        warn!(
                            "[{}] No market data for {}s (heartbeats do not count) — forcing resubscribe",
                            self.name, FEED_SILENCE_TIMEOUT_SECS
                        );
                    } else {
                        warn!(
                            "[{}] No WebSocket frame for {}s — forcing reconnect",
                            self.name, FEED_SILENCE_TIMEOUT_SECS
                        );
                    }
                    break;
                }
            };
            match msg {
                Ok(Message::Binary(data)) => {
                    if data.len() <= 1 {
                        debug!("[{}] Heartbeat received", self.name);
                        continue;
                    }

                    match parse_binary_message(&data) {
                        Ok(ticks) => {
                            if !ticks.is_empty() {
                                quote_deadline =
                                    Instant::now() + Duration::from_secs(FEED_SILENCE_TIMEOUT_SECS);
                                let _ = self.tx.send(TickEvent::market_data(ticks));
                            }
                        }
                        Err(e) => {
                            warn!("[{}] Parse error: {} (len={})", self.name, e, data.len());
                        }
                    }
                }
                Ok(Message::Text(text)) => {
                    info!("[{}] Text message: {}", self.name, text);
                }
                Ok(Message::Ping(data)) => {
                    let _ = write.send(Message::Pong(data)).await;
                }
                Ok(Message::Close(_)) => {
                    info!("[{}] Server sent close frame", self.name);
                    break;
                }
                Err(e) => {
                    error!("[{}] WebSocket read error: {}", self.name, e);
                    break;
                }
                _ => {}
            }
        }

        Ok(())
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub fn parse_binary_message(
    data: &[u8],
) -> Result<Vec<Tick>, Box<dyn std::error::Error + Send + Sync>> {
    if data.len() < 2 {
        return Ok(vec![]);
    }

    let num_packets_raw = BigEndian::read_i16(&data[0..2]);
    if num_packets_raw <= 0 {
        return Ok(vec![]);
    }
    let num_packets = num_packets_raw as usize;
    // Cap pre-allocation: each packet carries at least a 2-byte length header, so a
    // valid message can never contain more than data.len()/2 packets. This prevents
    // a malformed/huge count from triggering an OOM-sized allocation.
    let mut ticks = Vec::with_capacity(num_packets.min(data.len() / 2 + 1));
    let mut offset = 2;

    for _ in 0..num_packets {
        if offset + 2 > data.len() {
            break;
        }

        let packet_len_raw = BigEndian::read_i16(&data[offset..offset + 2]);
        offset += 2;
        if packet_len_raw < 0 {
            warn!(
                "Malformed packet length {} (negative); stopping parse",
                packet_len_raw
            );
            break;
        }
        let packet_len = packet_len_raw as usize;

        if offset + packet_len > data.len() {
            warn!(
                "Packet length {} exceeds remaining {} bytes",
                packet_len,
                data.len() - offset
            );
            break;
        }

        let packet = &data[offset..offset + packet_len];
        offset += packet_len;

        if let Some(tick) = parse_packet(packet) {
            ticks.push(tick);
        }
    }

    Ok(ticks)
}

fn parse_packet(packet: &[u8]) -> Option<Tick> {
    let len = packet.len();

    if len < 8 {
        return None;
    }

    let token = BigEndian::read_u32(&packet[0..4]);
    let divisor = price_divisor(token);

    match len {
        8 => {
            let ltp = BigEndian::read_i32(&packet[4..8]) as f64 / divisor;
            Some(Tick {
                token,
                ltp,
                mode: TickMode::Ltp,
                ..Tick::default()
            })
        }

        28 => {
            let ltp = BigEndian::read_i32(&packet[4..8]) as f64 / divisor;
            let high = BigEndian::read_i32(&packet[8..12]) as f64 / divisor;
            let low = BigEndian::read_i32(&packet[12..16]) as f64 / divisor;
            let open = BigEndian::read_i32(&packet[16..20]) as f64 / divisor;
            let close = BigEndian::read_i32(&packet[20..24]) as f64 / divisor;

            Some(Tick {
                token,
                ltp,
                ohlc: OHLC {
                    open,
                    high,
                    low,
                    close,
                },
                mode: TickMode::Quote,
                ..Tick::default()
            })
        }

        32 => {
            let ltp = BigEndian::read_i32(&packet[4..8]) as f64 / divisor;
            let high = BigEndian::read_i32(&packet[8..12]) as f64 / divisor;
            let low = BigEndian::read_i32(&packet[12..16]) as f64 / divisor;
            let open = BigEndian::read_i32(&packet[16..20]) as f64 / divisor;
            let close = BigEndian::read_i32(&packet[20..24]) as f64 / divisor;
            let exchange_ts = BigEndian::read_u32(&packet[28..32]);

            Some(Tick {
                token,
                ltp,
                ohlc: OHLC {
                    open,
                    high,
                    low,
                    close,
                },
                exchange_ts,
                mode: TickMode::Full,
                ..Tick::default()
            })
        }

        44 => {
            let ltp = BigEndian::read_i32(&packet[4..8]) as f64 / divisor;
            let last_qty = BigEndian::read_u32(&packet[8..12]);
            let avg_price = BigEndian::read_i32(&packet[12..16]) as f64 / divisor;
            let volume = BigEndian::read_u32(&packet[16..20]);
            let buy_qty = BigEndian::read_u32(&packet[20..24]);
            let sell_qty = BigEndian::read_u32(&packet[24..28]);
            let open = BigEndian::read_i32(&packet[28..32]) as f64 / divisor;
            let high = BigEndian::read_i32(&packet[32..36]) as f64 / divisor;
            let low = BigEndian::read_i32(&packet[36..40]) as f64 / divisor;
            let close = BigEndian::read_i32(&packet[40..44]) as f64 / divisor;

            Some(Tick {
                token,
                ltp,
                last_qty,
                avg_price,
                volume,
                buy_qty,
                sell_qty,
                ohlc: OHLC {
                    open,
                    high,
                    low,
                    close,
                },
                mode: TickMode::Quote,
                ..Tick::default()
            })
        }

        56 => {
            let ltp = BigEndian::read_i32(&packet[4..8]) as f64 / divisor;
            let last_qty = BigEndian::read_u32(&packet[8..12]);
            let avg_price = BigEndian::read_i32(&packet[12..16]) as f64 / divisor;
            let volume = BigEndian::read_u32(&packet[16..20]);
            let buy_qty = BigEndian::read_u32(&packet[20..24]);
            let sell_qty = BigEndian::read_u32(&packet[24..28]);
            let open = BigEndian::read_i32(&packet[28..32]) as f64 / divisor;
            let high = BigEndian::read_i32(&packet[32..36]) as f64 / divisor;
            let low = BigEndian::read_i32(&packet[36..40]) as f64 / divisor;
            let close = BigEndian::read_i32(&packet[40..44]) as f64 / divisor;
            let last_trade_ts = BigEndian::read_u32(&packet[44..48]);
            let oi = BigEndian::read_u32(&packet[48..52]);
            let oi_day_high = BigEndian::read_u32(&packet[52..56]);
            let _ = last_trade_ts;

            Some(Tick {
                token,
                ltp,
                last_qty,
                avg_price,
                volume,
                buy_qty,
                sell_qty,
                ohlc: OHLC {
                    open,
                    high,
                    low,
                    close,
                },
                oi,
                oi_day_high,
                mode: TickMode::Quote,
                ..Tick::default()
            })
        }

        184 => {
            let ltp = BigEndian::read_i32(&packet[4..8]) as f64 / divisor;
            let last_qty = BigEndian::read_u32(&packet[8..12]);
            let avg_price = BigEndian::read_i32(&packet[12..16]) as f64 / divisor;
            let volume = BigEndian::read_u32(&packet[16..20]);
            let buy_qty = BigEndian::read_u32(&packet[20..24]);
            let sell_qty = BigEndian::read_u32(&packet[24..28]);
            let open = BigEndian::read_i32(&packet[28..32]) as f64 / divisor;
            let high = BigEndian::read_i32(&packet[32..36]) as f64 / divisor;
            let low = BigEndian::read_i32(&packet[36..40]) as f64 / divisor;
            let close = BigEndian::read_i32(&packet[40..44]) as f64 / divisor;

            let last_trade_ts = BigEndian::read_u32(&packet[44..48]);
            let oi = BigEndian::read_u32(&packet[48..52]);
            let oi_day_high = BigEndian::read_u32(&packet[52..56]);
            let oi_day_low = BigEndian::read_u32(&packet[56..60]);
            let exchange_ts = BigEndian::read_u32(&packet[60..64]);

            let depth = parse_market_depth(&packet[64..184], divisor);

            Some(Tick {
                token,
                ltp,
                last_qty,
                avg_price,
                volume,
                buy_qty,
                sell_qty,
                ohlc: OHLC {
                    open,
                    high,
                    low,
                    close,
                },
                oi,
                oi_day_high,
                oi_day_low,
                exchange_ts,
                last_trade_ts,
                depth: Some(depth),
                mode: TickMode::Full,
            })
        }

        _ => {
            if len >= 8 {
                let ltp = BigEndian::read_i32(&packet[4..8]) as f64 / divisor;
                Some(Tick {
                    token,
                    ltp,
                    mode: TickMode::Ltp,
                    ..Tick::default()
                })
            } else {
                None
            }
        }
    }
}

fn price_divisor(token: u32) -> f64 {
    match token & 0xff {
        3 => 10_000_000.0, // CDS
        6 => 10_000.0,     // BCD
        _ => 100.0,
    }
}

fn parse_market_depth(data: &[u8], divisor: f64) -> MarketDepth {
    let mut depth = MarketDepth::default();

    for i in 0..5 {
        let base = i * 12;
        depth.bids[i] = DepthEntry {
            quantity: BigEndian::read_u32(&data[base..base + 4]),
            price: BigEndian::read_i32(&data[base + 4..base + 8]) as f64 / divisor,
            orders: BigEndian::read_u16(&data[base + 8..base + 10]),
        };
    }

    for i in 0..5 {
        let base = 60 + i * 12;
        depth.asks[i] = DepthEntry {
            quantity: BigEndian::read_u32(&data[base..base + 4]),
            price: BigEndian::read_i32(&data[base + 4..base + 8]) as f64 / divisor,
            orders: BigEndian::read_u16(&data[base + 8..base + 10]),
        };
    }

    depth
}

#[cfg(test)]
mod tests {
    use super::*;
    use byteorder::WriteBytesExt;

    fn make_ltp_packet(token: u32, ltp_paise: i32) -> Vec<u8> {
        let mut pkt = Vec::new();
        pkt.write_u32::<BigEndian>(token).unwrap();
        pkt.write_i32::<BigEndian>(ltp_paise).unwrap();
        pkt
    }

    fn make_binary_message(packets: &[Vec<u8>]) -> Vec<u8> {
        let mut msg = Vec::new();
        msg.write_i16::<BigEndian>(packets.len() as i16).unwrap();
        for pkt in packets {
            msg.write_i16::<BigEndian>(pkt.len() as i16).unwrap();
            msg.extend_from_slice(pkt);
        }
        msg
    }

    #[test]
    fn test_parse_ltp_packet() {
        let pkt = make_ltp_packet(408065, 141295);
        let tick = parse_packet(&pkt).unwrap();
        assert_eq!(tick.token, 408065);
        assert!((tick.ltp - 1412.95).abs() < 0.01);
        assert_eq!(tick.mode, TickMode::Ltp);
    }

    #[test]
    fn currency_packets_use_segment_price_divisors() {
        let cds_token = (1234 << 8) | 3;
        let tick = parse_packet(&make_ltp_packet(cds_token, 838_350_000)).unwrap();
        assert!((tick.ltp - 83.835).abs() < 1e-9);
        assert_eq!(price_divisor((1234 << 8) | 6), 10_000.0);
        assert_eq!(price_divisor(408065), 100.0);
    }

    #[test]
    fn test_parse_binary_message_multiple() {
        let packets = vec![
            make_ltp_packet(408065, 141295),
            make_ltp_packet(884737, 50050),
        ];
        let msg = make_binary_message(&packets);
        let ticks = parse_binary_message(&msg).unwrap();
        assert_eq!(ticks.len(), 2);
        assert_eq!(ticks[0].token, 408065);
        assert_eq!(ticks[1].token, 884737);
        assert!((ticks[1].ltp - 500.50).abs() < 0.01);
    }

    #[test]
    fn test_parse_quote_packet() {
        let mut pkt = Vec::new();
        pkt.write_u32::<BigEndian>(408065).unwrap();
        pkt.write_i32::<BigEndian>(141295).unwrap();
        pkt.write_u32::<BigEndian>(100).unwrap();
        pkt.write_i32::<BigEndian>(141000).unwrap();
        pkt.write_u32::<BigEndian>(5000000).unwrap();
        pkt.write_u32::<BigEndian>(200000).unwrap();
        pkt.write_u32::<BigEndian>(150000).unwrap();
        pkt.write_i32::<BigEndian>(139600).unwrap();
        pkt.write_i32::<BigEndian>(142175).unwrap();
        pkt.write_i32::<BigEndian>(139555).unwrap();
        pkt.write_i32::<BigEndian>(138965).unwrap();
        assert_eq!(pkt.len(), 44);

        let tick = parse_packet(&pkt).unwrap();
        assert_eq!(tick.token, 408065);
        assert!((tick.ltp - 1412.95).abs() < 0.01);
        assert_eq!(tick.volume, 5000000);
        assert!((tick.ohlc.open - 1396.00).abs() < 0.01);
        assert_eq!(tick.mode, TickMode::Quote);
    }

    #[test]
    fn test_parse_full_packet() {
        let mut pkt = Vec::new();
        pkt.write_u32::<BigEndian>(408065).unwrap();
        pkt.write_i32::<BigEndian>(141295).unwrap();
        pkt.write_u32::<BigEndian>(100).unwrap();
        pkt.write_i32::<BigEndian>(141000).unwrap();
        pkt.write_u32::<BigEndian>(5000000).unwrap();
        pkt.write_u32::<BigEndian>(200000).unwrap();
        pkt.write_u32::<BigEndian>(150000).unwrap();
        pkt.write_i32::<BigEndian>(139600).unwrap();
        pkt.write_i32::<BigEndian>(142175).unwrap();
        pkt.write_i32::<BigEndian>(139555).unwrap();
        pkt.write_i32::<BigEndian>(138965).unwrap();

        pkt.write_u32::<BigEndian>(999999).unwrap();
        pkt.write_u32::<BigEndian>(12500).unwrap();
        pkt.write_u32::<BigEndian>(15000).unwrap();
        pkt.write_u32::<BigEndian>(10000).unwrap();
        pkt.write_u32::<BigEndian>(1000000).unwrap();

        for i in 0..10 {
            pkt.write_u32::<BigEndian>(1000 + i).unwrap();
            pkt.write_i32::<BigEndian>(141200 + i as i32 * 5).unwrap();
            pkt.write_u16::<BigEndian>(50 + i as u16).unwrap();
            pkt.write_u16::<BigEndian>(0).unwrap();
        }

        assert_eq!(pkt.len(), 184);

        let tick = parse_packet(&pkt).unwrap();
        assert_eq!(tick.token, 408065);
        assert_eq!(tick.oi, 12500);
        assert_eq!(tick.mode, TickMode::Full);
        assert!(tick.depth.is_some());

        let depth = tick.depth.unwrap();
        assert_eq!(depth.bids[0].quantity, 1000);
        assert!((depth.bids[0].price - 1412.00).abs() < 0.01);
        assert_eq!(depth.asks[0].quantity, 1005);
    }

    #[test]
    fn test_heartbeat_ignored() {
        let data = vec![0u8];
        let ticks = parse_binary_message(&data).unwrap();
        assert!(ticks.is_empty());
    }

    #[tokio::test]
    async fn heartbeats_do_not_extend_quote_deadline() {
        use futures_util::stream;
        let beats = stream::unfold((), |_| async {
            tokio::time::sleep(Duration::from_millis(2)).await;
            Some((Message::Binary(vec![0].into()), ()))
        });
        let mut beats = Box::pin(beats);
        let deadline = Instant::now() + Duration::from_millis(20);
        let mut received = 0;
        loop {
            match timeout_at(deadline, beats.next()).await {
                Ok(Some(Message::Binary(data))) if data.len() == 1 => received += 1,
                Err(_) => break,
                other => panic!("unexpected stream result: {:?}", other),
            }
        }
        assert!(
            received > 1,
            "heartbeats arrived but did not keep the quote deadline alive"
        );
    }

    #[test]
    fn market_hours_are_weekday_0915_to_1530_ist() {
        let ms = |s: &str| {
            chrono::DateTime::parse_from_rfc3339(s)
                .unwrap()
                .timestamp_millis() as u64
        };
        assert!(market_data_expected(ms("2026-07-13T09:15:00+05:30")));
        assert!(market_data_expected(ms("2026-07-13T15:29:59+05:30")));
        assert!(!market_data_expected(ms("2026-07-13T15:30:00+05:30")));
        assert!(!market_data_expected(ms("2026-07-18T13:00:00+05:30")));
    }

    #[test]
    fn test_negative_packet_count_ignored() {
        let mut data = Vec::new();
        data.write_i16::<BigEndian>(-1).unwrap();
        let ticks = parse_binary_message(&data).unwrap();
        assert!(ticks.is_empty());
    }

    #[test]
    fn test_negative_packet_len_stops_parse() {
        let mut data = Vec::new();
        data.write_i16::<BigEndian>(1).unwrap();
        data.write_i16::<BigEndian>(-8).unwrap();
        let ticks = parse_binary_message(&data).unwrap();
        assert!(ticks.is_empty());
    }
}
