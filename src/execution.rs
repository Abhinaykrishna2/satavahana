use crate::config::ExecutionConfig;

use reqwest::Client;
use serde_json::Value;
use std::collections::HashMap;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tracing::{error, info, warn};

const KITE_ORDERS_URL: &str = "https://api.kite.trade/orders";
const KITE_MARGINS_URL: &str = "https://api.kite.trade/user/margins";

pub async fn fetch_live_available_funds(
    api_key: &str,
    access_token: &str,
) -> Result<f64, String> {
    let auth_header = format!("token {}:{}", api_key, access_token);
    let client = Client::new();
    let resp = client
        .get(KITE_MARGINS_URL)
        .header("X-Kite-Version", "3")
        .header("Authorization", auth_header)
        .send()
        .await
        .map_err(|e| format!("network error while fetching margins: {}", e))?;

    let http = resp.status();
    let body: Value = resp
        .json()
        .await
        .map_err(|e| format!("invalid JSON from margins response: {}", e))?;

    if !http.is_success() {
        return Err(format!(
            "HTTP {} from margins endpoint: {}",
            http,
            kite_message(&body)
        ));
    }

    let candidates = [
        "/data/equity/available/live_balance",
        "/data/equity/available/cash",
        "/data/equity/net",
    ];
    for path in candidates {
        if let Some(v) = body.pointer(path).and_then(|v| v.as_f64()) {
            if v.is_finite() && v > 0.0 {
                return Ok(v);
            }
        }
    }

    Err(format!("margins response missing usable equity funds fields: {}", body))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderSide {
    Buy,
    Sell,
}

impl OrderSide {
    fn as_str(self) -> &'static str {
        match self {
            Self::Buy => "BUY",
            Self::Sell => "SELL",
        }
    }
}

#[derive(Debug, Clone)]
pub struct PlaceOrderCmd {
    pub tag: String,
    pub tradingsymbol: String,
    pub quantity: u32,
    pub side: OrderSide,
    /// Set for LIMIT orders; None sends a MARKET order regardless of order_type config.
    pub limit_price: Option<f64>,
}

#[derive(Debug, Clone)]
pub enum OrderCommand {
    Place(PlaceOrderCmd),
    CancelByTag { tag: String },
    StatusByTag { tag: String },
}

#[derive(Debug, Clone)]
pub struct OrderUpdate {
    pub tag: String,
    pub order_id: Option<String>,
    pub status: Option<String>,
    pub average_price: Option<f64>,
    pub filled_quantity: Option<u32>,
    pub pending_quantity: Option<u32>,
    pub source: String,
    pub message: Option<String>,
}

#[derive(Debug, Clone)]
struct KnownOrder {
    order_id: String,
    variety: String,
}

pub fn spawn_order_executor(
    api_key: String,
    access_token: String,
    cfg: ExecutionConfig,
) -> (
    UnboundedSender<OrderCommand>,
    UnboundedReceiver<OrderUpdate>,
    tokio::task::JoinHandle<()>,
) {
    let (tx, rx) = mpsc::unbounded_channel();
    let (update_tx, update_rx) = mpsc::unbounded_channel();
    let auth_header = format!("token {}:{}", api_key, access_token);
    let mut executor = OrderExecutor::new(auth_header, cfg, rx, update_tx);
    let handle = tokio::spawn(async move {
        executor.run().await;
    });
    (tx, update_rx, handle)
}

struct OrderExecutor {
    client: Client,
    auth_header: String,
    cfg: ExecutionConfig,
    rx: UnboundedReceiver<OrderCommand>,
    update_tx: UnboundedSender<OrderUpdate>,
    known_orders: HashMap<String, KnownOrder>,
}

impl OrderExecutor {
    fn new(
        auth_header: String,
        cfg: ExecutionConfig,
        rx: UnboundedReceiver<OrderCommand>,
        update_tx: UnboundedSender<OrderUpdate>,
    ) -> Self {
        Self {
            client: Client::new(),
            auth_header,
            cfg,
            rx,
            update_tx,
            known_orders: HashMap::new(),
        }
    }

    fn publish(&self, update: OrderUpdate) {
        if let Err(e) = self.update_tx.send(update) {
            warn!("Live order executor: failed to publish update: {}", e);
        }
    }

    async fn run(&mut self) {
        info!(
            "Live order executor started | exchange={} product={} variety={} type={} validity={}",
            self.cfg.exchange,
            self.cfg.product,
            self.cfg.variety,
            self.cfg.order_type,
            self.cfg.validity
        );

        while let Some(cmd) = self.rx.recv().await {
            match cmd {
                OrderCommand::Place(place) => {
                    if place.tradingsymbol.is_empty() || place.quantity == 0 {
                        warn!(
                            "LIVE ORDER skipped: invalid payload tag={} symbol='{}' qty={}",
                            place.tag, place.tradingsymbol, place.quantity
                        );
                        continue;
                    }
                    match self.place_order(&place).await {
                        Ok(order_id) => {
                            info!(
                                "LIVE ORDER placed | tag={} side={} symbol={} qty={} order_id={}",
                                place.tag,
                                place.side.as_str(),
                                place.tradingsymbol,
                                place.quantity,
                                order_id
                            );
                            self.known_orders.insert(
                                place.tag.clone(),
                                KnownOrder {
                                    order_id,
                                    variety: self.cfg.variety.clone(),
                                },
                            );
                            self.publish(OrderUpdate {
                                tag: place.tag.clone(),
                                order_id: self.known_orders.get(&place.tag).map(|o| o.order_id.clone()),
                                status: None,
                                average_price: None,
                                filled_quantity: None,
                                pending_quantity: None,
                                source: "place_ack".to_string(),
                                message: None,
                            });
                            self.log_status_by_tag(&place.tag).await;
                        }
                        Err(e) => {
                            error!(
                                "LIVE ORDER place failed | tag={} side={} symbol={} qty={} err={}",
                                place.tag,
                                place.side.as_str(),
                                place.tradingsymbol,
                                place.quantity,
                                e
                            );
                            self.publish(OrderUpdate {
                                tag: place.tag.clone(),
                                order_id: None,
                                status: None,
                                average_price: None,
                                filled_quantity: None,
                                pending_quantity: None,
                                source: "place_error".to_string(),
                                message: Some(e),
                            });
                        }
                    }
                }
                OrderCommand::CancelByTag { tag } => {
                    self.cancel_by_tag(&tag).await;
                }
                OrderCommand::StatusByTag { tag } => {
                    self.log_status_by_tag(&tag).await;
                }
            }
        }

        info!("Live order executor stopped");
    }

    async fn place_order(&self, cmd: &PlaceOrderCmd) -> Result<String, String> {
        let url = format!("{}/{}", KITE_ORDERS_URL, self.cfg.variety);
        // Use LIMIT when a price is provided; fall back to configured order_type otherwise.
        let effective_order_type = if cmd.limit_price.is_some() {
            "LIMIT".to_string()
        } else {
            self.cfg.order_type.clone()
        };
        let mut params = vec![
            ("exchange", self.cfg.exchange.clone()),
            ("tradingsymbol", cmd.tradingsymbol.clone()),
            ("transaction_type", cmd.side.as_str().to_string()),
            ("quantity", cmd.quantity.to_string()),
            ("product", self.cfg.product.clone()),
            ("order_type", effective_order_type),
            ("validity", self.cfg.validity.clone()),
            ("tag", cmd.tag.clone()),
        ];
        if let Some(price) = cmd.limit_price {
            // Round to 2 decimal places — Kite requires ₹0.05 tick on options but
            // accepts 2-dp values; the exchange itself enforces tick rounding.
            params.push(("price", format!("{:.2}", price)));
        }

        let resp = self
            .client
            .post(&url)
            .header("X-Kite-Version", "3")
            .header("Authorization", &self.auth_header)
            .form(&params)
            .send()
            .await
            .map_err(|e| format!("network error while placing order: {}", e))?;

        let http = resp.status();
        let body: Value = resp
            .json()
            .await
            .map_err(|e| format!("invalid JSON from place-order response: {}", e))?;

        if !http.is_success() {
            return Err(format!(
                "HTTP {} from place-order: {}",
                http,
                kite_message(&body)
            ));
        }

        body.pointer("/data/order_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| format!("place-order missing order_id: {}", body))
    }

    async fn cancel_by_tag(&mut self, tag: &str) {
        let Some(known) = self.resolve_order_by_tag(tag, true).await else {
            warn!("LIVE ORDER cancel skipped: no order found for tag={}", tag);
            return;
        };

        match self.cancel_order(&known.variety, &known.order_id).await {
            Ok(()) => {
                info!("LIVE ORDER cancel requested | tag={} order_id={}", tag, known.order_id);
                self.publish(OrderUpdate {
                    tag: tag.to_string(),
                    order_id: Some(known.order_id.clone()),
                    status: None,
                    average_price: None,
                    filled_quantity: None,
                    pending_quantity: None,
                    source: "cancel_ack".to_string(),
                    message: None,
                });
                self.log_status_by_tag(tag).await;
            }
            Err(e) => {
                warn!(
                    "LIVE ORDER cancel failed | tag={} order_id={} err={}",
                    tag, known.order_id, e
                );
                self.publish(OrderUpdate {
                    tag: tag.to_string(),
                    order_id: Some(known.order_id.clone()),
                    status: None,
                    average_price: None,
                    filled_quantity: None,
                    pending_quantity: None,
                    source: "cancel_error".to_string(),
                    message: Some(e),
                });
            }
        }
    }

    async fn cancel_order(&self, variety: &str, order_id: &str) -> Result<(), String> {
        let url = format!("{}/{}/{}", KITE_ORDERS_URL, variety, order_id);
        let resp = self
            .client
            .delete(&url)
            .header("X-Kite-Version", "3")
            .header("Authorization", &self.auth_header)
            .send()
            .await
            .map_err(|e| format!("network error while canceling order: {}", e))?;

        let http = resp.status();
        let body: Value = resp
            .json()
            .await
            .map_err(|e| format!("invalid JSON from cancel-order response: {}", e))?;

        if !http.is_success() {
            return Err(format!(
                "HTTP {} from cancel-order: {}",
                http,
                kite_message(&body)
            ));
        }
        Ok(())
    }

    async fn log_status_by_tag(&mut self, tag: &str) {
        let Some(known) = self.resolve_order_by_tag(tag, true).await else {
            warn!("LIVE ORDER status unavailable: no order found for tag={}", tag);
            self.publish(OrderUpdate {
                tag: tag.to_string(),
                order_id: None,
                status: None,
                average_price: None,
                filled_quantity: None,
                pending_quantity: None,
                source: "status_error".to_string(),
                message: Some("no order found for tag".to_string()),
            });
            return;
        };

        match self.fetch_order_status(&known.order_id).await {
            Ok(status) => {
                info!(
                    "LIVE ORDER status | tag={} order_id={} status={}",
                    tag, known.order_id, status.status
                );
                self.publish(OrderUpdate {
                    tag: tag.to_string(),
                    order_id: Some(known.order_id.clone()),
                    status: Some(status.status),
                    average_price: status.average_price,
                    filled_quantity: status.filled_quantity,
                    pending_quantity: status.pending_quantity,
                    source: "status_poll".to_string(),
                    message: status.message,
                });
            }
            Err(e) => {
                warn!(
                    "LIVE ORDER status lookup failed | tag={} order_id={} err={}",
                    tag, known.order_id, e
                );
                self.publish(OrderUpdate {
                    tag: tag.to_string(),
                    order_id: Some(known.order_id.clone()),
                    status: None,
                    average_price: None,
                    filled_quantity: None,
                    pending_quantity: None,
                    source: "status_error".to_string(),
                    message: Some(e),
                });
            }
        }
    }

    async fn fetch_order_status(&self, order_id: &str) -> Result<OrderSnapshot, String> {
        let url = format!("{}/{}", KITE_ORDERS_URL, order_id);
        let resp = self
            .client
            .get(&url)
            .header("X-Kite-Version", "3")
            .header("Authorization", &self.auth_header)
            .send()
            .await
            .map_err(|e| format!("network error while fetching order status: {}", e))?;

        let http = resp.status();
        let body: Value = resp
            .json()
            .await
            .map_err(|e| format!("invalid JSON from order-status response: {}", e))?;

        if !http.is_success() {
            return Err(format!(
                "HTTP {} from order-status: {}",
                http,
                kite_message(&body)
            ));
        }

        let history = body
            .get("data")
            .and_then(|v| v.as_array())
            .ok_or_else(|| format!("order-status missing data array: {}", body))?;
        let latest = history
            .last()
            .ok_or_else(|| "order-status returned empty history".to_string())?;
        let status = latest
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("UNKNOWN")
            .to_string();
        let message = latest
            .get("status_message")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let average_price = latest
            .get("average_price")
            .and_then(|v| v.as_f64());
        let filled_quantity = latest
            .get("filled_quantity")
            .and_then(|v| v.as_u64())
            .map(|v| v as u32);
        let pending_quantity = latest
            .get("pending_quantity")
            .and_then(|v| v.as_u64())
            .map(|v| v as u32);

        Ok(OrderSnapshot {
            status,
            message,
            average_price,
            filled_quantity,
            pending_quantity,
        })
    }

    async fn resolve_order_by_tag(&mut self, tag: &str, refresh: bool) -> Option<KnownOrder> {
        if !refresh {
            if let Some(existing) = self.known_orders.get(tag) {
                return Some(existing.clone());
            }
        }

        if let Some(found) = self.lookup_tag_from_order_book(tag).await {
            self.known_orders.insert(tag.to_string(), found.clone());
            return Some(found);
        }

        self.known_orders.get(tag).cloned()
    }

    async fn lookup_tag_from_order_book(&self, tag: &str) -> Option<KnownOrder> {
        let resp = self
            .client
            .get(KITE_ORDERS_URL)
            .header("X-Kite-Version", "3")
            .header("Authorization", &self.auth_header)
            .send()
            .await
            .ok()?;

        let http = resp.status();
        let body: Value = resp.json().await.ok()?;
        if !http.is_success() {
            warn!(
                "LIVE ORDER orderbook lookup failed (HTTP {}): {}",
                http,
                kite_message(&body)
            );
            return None;
        }

        let data = body.get("data")?.as_array()?;
        let latest = data.iter().rev().find(|row| {
            row.get("tag")
                .and_then(|v| v.as_str())
                .map(|v| v == tag)
                .unwrap_or(false)
        })?;

        let order_id = latest.get("order_id")?.as_str()?.to_string();
        let variety = latest
            .get("variety")
            .and_then(|v| v.as_str())
            .unwrap_or(self.cfg.variety.as_str())
            .to_string();
        Some(KnownOrder { order_id, variety })
    }
}

fn kite_message(body: &Value) -> String {
    body.get("message")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown Kite error")
        .to_string()
}

#[derive(Debug, Clone)]
struct OrderSnapshot {
    status: String,
    message: Option<String>,
    average_price: Option<f64>,
    filled_quantity: Option<u32>,
    pending_quantity: Option<u32>,
}
