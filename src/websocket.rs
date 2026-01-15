use crate::models::{DepthEntry, MarketDepth, Tick, TickMode, OHLC};
use byteorder::{BigEndian, ByteOrder};
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

#[derive(Debug, Clone)]
pub struct TickEvent {
    pub ticks: Vec<Tick>,
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
                        error!("[{}] WebSocket error: {}, reconnecting in 5s...", conn.name, e);
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

        info!("[{}] Connected! Subscribing to {} tokens...", self.name, self.tokens.len());

        let subscribe_msg = serde_json::json!({
            "a": "subscribe",
            "v": self.tokens
        });
        write
            .send(Message::Text(subscribe_msg.to_string().into()))
            .await?;

        let mode_msg = serde_json::json!({
            "a": "mode",
            "v": [&self.mode, self.tokens]
        });
        write.send(Message::Text(mode_msg.to_string().into())).await?;

        info!("[{}] Subscribed and mode set. Streaming...", self.name);

        while let Some(msg) = read.next().await {
            match msg {
                Ok(Message::Binary(data)) => {
                    if data.len() <= 1 {
                        debug!("[{}] Heartbeat received", self.name);
                        continue;
                    }

                    match parse_binary_message(&data) {
                        Ok(ticks) => {
                            if !ticks.is_empty() {
                                let _ = self.tx.send(TickEvent { ticks });
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

pub fn parse_binary_message(
    data: &[u8],
) -> Result<Vec<Tick>, Box<dyn std::error::Error + Send + Sync>> {
    if data.len() < 2 {
        return Ok(vec![]);
    }

    let num_packets = BigEndian::read_i16(&data[0..2]) as usize;
    let mut ticks = Vec::with_capacity(num_packets);
    let mut offset = 2;

    for _ in 0..num_packets {
        if offset + 2 > data.len() {
            break;
        }

        let packet_len = BigEndian::read_i16(&data[offset..offset + 2]) as usize;
        offset += 2;

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


    match len {
        8 => {
            let ltp = BigEndian::read_i32(&packet[4..8]) as f64 / 100.0;
            Some(Tick {
                token,
                ltp,
                mode: TickMode::Ltp,
                ..Tick::default()
            })
        }

        28 => {
            let ltp = BigEndian::read_i32(&packet[4..8]) as f64 / 100.0;
            let high = BigEndian::read_i32(&packet[8..12]) as f64 / 100.0;
            let low = BigEndian::read_i32(&packet[12..16]) as f64 / 100.0;
            let open = BigEndian::read_i32(&packet[16..20]) as f64 / 100.0;
            let close = BigEndian::read_i32(&packet[20..24]) as f64 / 100.0;

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
            let ltp = BigEndian::read_i32(&packet[4..8]) as f64 / 100.0;
            let high = BigEndian::read_i32(&packet[8..12]) as f64 / 100.0;
            let low = BigEndian::read_i32(&packet[12..16]) as f64 / 100.0;
            let open = BigEndian::read_i32(&packet[16..20]) as f64 / 100.0;
            let close = BigEndian::read_i32(&packet[20..24]) as f64 / 100.0;
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
            let ltp = BigEndian::read_i32(&packet[4..8]) as f64 / 100.0;
            let last_qty = BigEndian::read_u32(&packet[8..12]);
            let avg_price = BigEndian::read_i32(&packet[12..16]) as f64 / 100.0;
            let volume = BigEndian::read_u32(&packet[16..20]);
            let buy_qty = BigEndian::read_u32(&packet[20..24]);
            let sell_qty = BigEndian::read_u32(&packet[24..28]);
            let open = BigEndian::read_i32(&packet[28..32]) as f64 / 100.0;
            let high = BigEndian::read_i32(&packet[32..36]) as f64 / 100.0;
            let low = BigEndian::read_i32(&packet[36..40]) as f64 / 100.0;
            let close = BigEndian::read_i32(&packet[40..44]) as f64 / 100.0;

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
            let ltp = BigEndian::read_i32(&packet[4..8]) as f64 / 100.0;
            let last_qty = BigEndian::read_u32(&packet[8..12]);
            let avg_price = BigEndian::read_i32(&packet[12..16]) as f64 / 100.0;
            let volume = BigEndian::read_u32(&packet[16..20]);
            let buy_qty = BigEndian::read_u32(&packet[20..24]);
            let sell_qty = BigEndian::read_u32(&packet[24..28]);
            let open = BigEndian::read_i32(&packet[28..32]) as f64 / 100.0;
            let high = BigEndian::read_i32(&packet[32..36]) as f64 / 100.0;
            let low = BigEndian::read_i32(&packet[36..40]) as f64 / 100.0;
            let close = BigEndian::read_i32(&packet[40..44]) as f64 / 100.0;
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
            let ltp = BigEndian::read_i32(&packet[4..8]) as f64 / 100.0;
            let last_qty = BigEndian::read_u32(&packet[8..12]);
            let avg_price = BigEndian::read_i32(&packet[12..16]) as f64 / 100.0;
            let volume = BigEndian::read_u32(&packet[16..20]);
            let buy_qty = BigEndian::read_u32(&packet[20..24]);
            let sell_qty = BigEndian::read_u32(&packet[24..28]);
            let open = BigEndian::read_i32(&packet[28..32]) as f64 / 100.0;
            let high = BigEndian::read_i32(&packet[32..36]) as f64 / 100.0;
            let low = BigEndian::read_i32(&packet[36..40]) as f64 / 100.0;
            let close = BigEndian::read_i32(&packet[40..44]) as f64 / 100.0;

            let last_trade_ts = BigEndian::read_u32(&packet[44..48]);
            let oi = BigEndian::read_u32(&packet[48..52]);
            let oi_day_high = BigEndian::read_u32(&packet[52..56]);
            let oi_day_low = BigEndian::read_u32(&packet[56..60]);
            let exchange_ts = BigEndian::read_u32(&packet[60..64]);

            let depth = parse_market_depth(&packet[64..184]);

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
                let ltp = BigEndian::read_i32(&packet[4..8]) as f64 / 100.0;
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

fn parse_market_depth(data: &[u8]) -> MarketDepth {
    let mut depth = MarketDepth::default();

    for i in 0..5 {
        let base = i * 12;
        depth.bids[i] = DepthEntry {
            quantity: BigEndian::read_u32(&data[base..base + 4]),
            price: BigEndian::read_i32(&data[base + 4..base + 8]) as f64 / 100.0,
            orders: BigEndian::read_u16(&data[base + 8..base + 10]),
        };
    }

    for i in 0..5 {
        let base = 60 + i * 12;
        depth.asks[i] = DepthEntry {
            quantity: BigEndian::read_u32(&data[base..base + 4]),
            price: BigEndian::read_i32(&data[base + 4..base + 8]) as f64 / 100.0,
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
}
