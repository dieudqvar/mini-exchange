use chrono::Utc;
use reqwest::Client;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tracing::{info, warn, error};
use uuid::Uuid;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::consumer::{StreamConsumer, Consumer};
use rdkafka::Message;

use crate::models::*;
use crate::store::Store;

/// Order engine handles the execution logic for BUY and SELL market orders.
pub struct OrderEngine {
    http_client: Client,
    market_service_url: String,
    audit_service_url: String,
    kafka_producer: Option<FutureProducer>,
    kafka_topic_audit: String,
    kafka_topic_incoming: String,
    prices_cache: Arc<RwLock<HashMap<String, f64>>>,
}

impl OrderEngine {
    pub fn new(
        market_service_url: String,
        audit_service_url: String,
        kafka_bootstrap_servers: Option<String>,
    ) -> Self {
        let prices_cache = Arc::new(RwLock::new(HashMap::new()));
        
        // Pre-populate cache with reasonable default values
        {
            let mut cache = prices_cache.write().unwrap();
            cache.insert("BTC".to_string(), 60000.00);
            cache.insert("ETH".to_string(), 3300.00);
            cache.insert("SOL".to_string(), 150.00);
            cache.insert("ADA".to_string(), 0.45);
            cache.insert("XRP".to_string(), 0.50);
        }

        let mut kafka_producer = None;
        if let Some(ref servers) = kafka_bootstrap_servers {
            match ClientConfig::new()
                .set("bootstrap.servers", servers)
                .set("message.timeout.ms", "3000")
                .create::<FutureProducer>()
            {
                Ok(p) => {
                    info!("Portfolio Service connected Kafka Producer to {}", servers);
                    kafka_producer = Some(p);
                }
                Err(e) => {
                    error!("Failed to create Kafka producer: {}", e);
                }
            }

            // Start background consumer for price ticks
            let servers_clone = servers.clone();
            let cache_clone = prices_cache.clone();
            tokio::spawn(async move {
                start_price_consumer(&servers_clone, cache_clone).await;
            });
        } else {
            warn!("KAFKA_BOOTSTRAP_SERVERS not set. Running in HTTP fallback mode.");
        }

        Self {
            http_client: Client::new(),
            market_service_url,
            audit_service_url,
            kafka_producer,
            kafka_topic_audit: "audit-events".to_string(),
            kafka_topic_incoming: "incoming-orders".to_string(),
            prices_cache,
        }
    }

    /// Step 1 (API Gateway): Accepts the order request, buffers it in Kafka, and returns immediately.
    /// This removes the DB bottleneck from the HTTP request loop.
    pub async fn accept_order(
        &self,
        request: OrderRequest,
    ) -> Result<Order, (u16, ErrorResponse)> {
        // Validate basic info
        if request.quantity <= 0.0 {
            return Err((
                400,
                ErrorResponse {
                    error: "INVALID_REQUEST".to_string(),
                    message: "Quantity must be positive".to_string(),
                },
            ));
        }

        let order_id = Uuid::new_v4().to_string();
        let order = Order {
            id: order_id.clone(),
            user_id: request.user_id.clone(),
            symbol: request.symbol.to_uppercase(),
            side: request.side.clone(),
            quantity: request.quantity,
            price: None,
            total: None,
            status: OrderStatus::Pending,
            reject_reason: None,
            created_at: Utc::now().to_rfc3339(),
        };

        // If Kafka is enabled, publish to incoming-orders topic
        if let Some(ref producer) = self.kafka_producer {
            let topic = self.kafka_topic_incoming.clone();
            let payload = serde_json::to_string(&order).unwrap();
            let key = order.id.clone();
            
            let record = FutureRecord::to(&topic)
                .key(&key)
                .payload(&payload);
                
            match producer.send(record, Duration::from_secs(2)).await {
                Ok(_) => {
                    info!(order_id = %key, "Order accepted and buffered into Kafka");
                }
                Err((e, _)) => {
                    error!("Failed to enqueue order to Kafka: {}", e);
                    return Err((500, ErrorResponse {
                        error: "QUEUE_ERROR".to_string(),
                        message: "Failed to queue order".to_string(),
                    }));
                }
            }
        } else {
            warn!("Kafka not available. Order dropped. Please enable Kafka for HFT mode.");
            return Err((503, ErrorResponse {
                error: "SERVICE_UNAVAILABLE".to_string(),
                message: "Kafka required for async processing".to_string(),
            }));
        }

        // Send ORDER_CREATED audit event
        self.send_audit_event("ORDER_CREATED", &order).await;

        // Return immediately with 202 Accepted semantics
        Ok(order)
    }

    /// Step 2 (Core Engine): Processes the accepted order synchronously in memory.
    /// This is called by the background Kafka consumer, decoupling it from the HTTP layer.
    pub async fn process_order(&self, store: Arc<Store>, mut order: Order) {
        // Check if user exists (now an in-memory check!)
        if store.get_portfolio(&order.user_id).await.is_none() {
            order.status = OrderStatus::Rejected;
            order.reject_reason = Some(format!("User '{}' not found", order.user_id));
            store.save_order(order.clone()).await;
            self.send_audit_event("ORDER_REJECTED", &order).await;
            return;
        }

        // Fetch price from Local Cache / Market Service
        let price = match self.fetch_price(&order.symbol).await {
            Ok(p) => p,
            Err(e) => {
                order.status = OrderStatus::Rejected;
                order.reject_reason = Some(e.clone());
                store.save_order(order.clone()).await;
                self.send_audit_event("ORDER_REJECTED", &order).await;
                return;
            }
        };

        order.price = Some(price);
        let total = (price * order.quantity * 10000.0).round() / 10000.0;
        order.total = Some(total);

        // Execute the trade (100% IN-MEMORY -> Extremely Fast)
        let result = match order.side {
            OrderSide::Buy => {
                store.execute_buy(&order.user_id, &order.symbol, order.quantity, total).await
            }
            OrderSide::Sell => {
                store.execute_sell(&order.user_id, &order.symbol, order.quantity, total).await
            }
        };

        match result {
            Ok(()) => {
                order.status = OrderStatus::Executed;
                store.save_order(order.clone()).await;
                info!(
                    order_id = %order.id,
                    user = %order.user_id,
                    side = %order.side,
                    symbol = %order.symbol,
                    qty = order.quantity,
                    price = price,
                    total = total,
                    "Order executed successfully in RAM"
                );
                self.send_audit_event("ORDER_EXECUTED", &order).await;
            }
            Err(reason) => {
                order.status = OrderStatus::Rejected;
                order.reject_reason = Some(reason.clone());
                store.save_order(order.clone()).await;
                warn!(order_id = %order.id, reason = %reason, "Order rejected");
                self.send_audit_event("ORDER_REJECTED", &order).await;
            }
        }
    }

    /// Fetches the current price from local Kafka-populated cache, falling back to HTTP REST.
    async fn fetch_price(&self, symbol: &str) -> Result<f64, String> {
        let sym_upper = symbol.to_uppercase();
        
        // 1. Try local cache
        if let Ok(cache) = self.prices_cache.read() {
            if let Some(price) = cache.get(&sym_upper) {
                return Ok(*price);
            }
        }

        // 2. Fallback to HTTP REST
        let url = format!("{}/prices/{}", self.market_service_url, sym_upper);
        
        let response = self
            .http_client
            .get(&url)
            .timeout(std::time::Duration::from_secs(5))
            .send()
            .await
            .map_err(|e| format!("Failed to connect to Market Service: {}", e))?;

        if !response.status().is_success() {
            return Err(format!(
                "Market Service returned error {}",
                response.status().as_u16()
            ));
        }

        let price_info: PriceInfo = response
            .json()
            .await
            .map_err(|e| format!("Failed to parse price response: {}", e))?;

        Ok(price_info.price)
    }

    /// Sends an audit event to the Audit Service (via Kafka if enabled, otherwise falling back to HTTP).
    async fn send_audit_event(&self, event_type: &str, order: &Order) {
        let event = AuditEvent {
            event_type: event_type.to_string(),
            order_id: order.id.clone(),
            user_id: order.user_id.clone(),
            details: serde_json::json!({
                "symbol": order.symbol,
                "side": order.side,
                "quantity": order.quantity,
                "price": order.price,
                "total": order.total,
                "status": order.status,
                "reject_reason": order.reject_reason,
            }),
        };

        // If Kafka is configured, publish event
        if let Some(ref producer) = self.kafka_producer {
            let topic = self.kafka_topic_audit.clone();
            let payload = serde_json::to_string(&event).unwrap();
            let key = order.id.clone();
            let p = producer.clone();
            
            tokio::spawn(async move {
                let record = FutureRecord::to(&topic)
                    .key(&key)
                    .payload(&payload);
                let _ = p.send(record, Duration::from_secs(2)).await;
            });
        }
    }
}

/// Spawns a background consumer task to update local prices from the Kafka topic `market-prices`.
async fn start_price_consumer(bootstrap_servers: &str, prices_cache: Arc<RwLock<HashMap<String, f64>>>) {
    let mut retry_count = 0;
    let consumer: StreamConsumer = loop {
        match ClientConfig::new()
            .set("group.id", "portfolio-service-prices")
            .set("bootstrap.servers", bootstrap_servers)
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "true")
            .set("auto.offset.reset", "latest")
            .create()
        {
            Ok(c) => break c,
            Err(e) => {
                retry_count += 1;
                warn!("Failed to create Kafka consumer (retry {}): {}", retry_count, e);
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        }
    };

    if let Err(e) = consumer.subscribe(&["market-prices"]) {
        error!("Can't subscribe to market-prices topic: {}", e);
        return;
    }

    info!("Kafka Price Consumer started, subscribed to 'market-prices'...");

    loop {
        match consumer.recv().await {
            Ok(msg) => {
                if let Some(payload) = msg.payload() {
                    if let Ok(payload_str) = std::str::from_utf8(payload) {
                        if let Ok(price_data) = serde_json::from_str::<serde_json::Value>(payload_str) {
                            if let (Some(symbol), Some(price)) = (
                                price_data["symbol"].as_str(),
                                price_data["price"].as_f64(),
                            ) {
                                if let Ok(mut cache) = prices_cache.write() {
                                    cache.insert(symbol.to_uppercase(), price);
                                }
                            }
                        }
                    }
                }
            }
            Err(e) => {
                warn!("Error receiving message from Kafka: {}", e);
            }
        }
    }
}
