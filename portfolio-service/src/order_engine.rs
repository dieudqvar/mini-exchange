use chrono::Utc;
use reqwest::Client;
use tracing::{info, warn};
use uuid::Uuid;

use crate::models::*;
use crate::store::Store;

/// Order engine handles the execution logic for BUY and SELL market orders.
///
/// It coordinates between:
/// - Market Service (to fetch current prices)
/// - Store (to update portfolios)
/// - Audit Service (to log events, fire-and-forget)
pub struct OrderEngine {
    http_client: Client,
    market_service_url: String,
    audit_service_url: String,
}

impl OrderEngine {
    pub fn new(market_service_url: String, audit_service_url: String) -> Self {
        Self {
            http_client: Client::new(),
            market_service_url,
            audit_service_url,
        }
    }

    /// Processes an order request end-to-end:
    /// 1. Validates the request
    /// 2. Fetches current price from Market Service
    /// 3. Executes the trade against the portfolio
    /// 4. Sends audit events
    pub async fn process_order(
        &self,
        store: &Store,
        request: OrderRequest,
    ) -> Result<Order, (u16, ErrorResponse)> {
        let order_id = Uuid::new_v4().to_string();
        let now = Utc::now().to_rfc3339();

        // Validate quantity
        if request.quantity <= 0.0 {
            return Err((
                400,
                ErrorResponse {
                    error: "INVALID_REQUEST".to_string(),
                    message: "Quantity must be positive".to_string(),
                },
            ));
        }

        // Check if user exists
        if store.get_portfolio(&request.user_id).is_none() {
            return Err((
                404,
                ErrorResponse {
                    error: "USER_NOT_FOUND".to_string(),
                    message: format!("User '{}' not found", request.user_id),
                },
            ));
        }

        // Create initial order
        let mut order = Order {
            id: order_id.clone(),
            user_id: request.user_id.clone(),
            symbol: request.symbol.to_uppercase(),
            side: request.side.clone(),
            quantity: request.quantity,
            price: None,
            total: None,
            status: OrderStatus::Pending,
            reject_reason: None,
            created_at: now,
        };

        // Send ORDER_CREATED audit event
        self.send_audit_event("ORDER_CREATED", &order).await;

        // Fetch price from Market Service
        let price = match self.fetch_price(&order.symbol).await {
            Ok(p) => p,
            Err(e) => {
                order.status = OrderStatus::Rejected;
                order.reject_reason = Some(e.clone());
                store.save_order(order.clone());
                self.send_audit_event("ORDER_REJECTED", &order).await;
                return Err((
                    503,
                    ErrorResponse {
                        error: "MARKET_SERVICE_UNAVAILABLE".to_string(),
                        message: e,
                    },
                ));
            }
        };

        order.price = Some(price);
        let total = (price * request.quantity * 100.0).round() / 100.0;
        order.total = Some(total);

        // Execute the trade
        let result = match request.side {
            OrderSide::Buy => {
                store.execute_buy(&request.user_id, &order.symbol, request.quantity, total)
            }
            OrderSide::Sell => {
                store.execute_sell(&request.user_id, &order.symbol, request.quantity, total)
            }
        };

        match result {
            Ok(()) => {
                order.status = OrderStatus::Executed;
                store.save_order(order.clone());
                info!(
                    order_id = %order.id,
                    user = %order.user_id,
                    side = %order.side,
                    symbol = %order.symbol,
                    qty = order.quantity,
                    price = price,
                    total = total,
                    "Order executed successfully"
                );
                self.send_audit_event("ORDER_EXECUTED", &order).await;
                Ok(order)
            }
            Err(reason) => {
                order.status = OrderStatus::Rejected;
                order.reject_reason = Some(reason.clone());
                store.save_order(order.clone());
                warn!(order_id = %order.id, reason = %reason, "Order rejected");
                self.send_audit_event("ORDER_REJECTED", &order).await;
                Err((
                    400,
                    ErrorResponse {
                        error: "ORDER_REJECTED".to_string(),
                        message: reason,
                    },
                ))
            }
        }
    }

    /// Fetches the current price for a symbol from the Market Service.
    async fn fetch_price(&self, symbol: &str) -> Result<f64, String> {
        let url = format!("{}/prices/{}", self.market_service_url, symbol);
        info!(url = %url, "Fetching price from market service");

        let response = self
            .http_client
            .get(&url)
            .timeout(std::time::Duration::from_secs(5))
            .send()
            .await
            .map_err(|e| format!("Failed to connect to Market Service: {}", e))?;

        if !response.status().is_success() {
            let status = response.status().as_u16();
            let body = response.text().await.unwrap_or_default();
            return Err(format!(
                "Market Service returned error {}: {}",
                status, body
            ));
        }

        let price_info: PriceInfo = response
            .json()
            .await
            .map_err(|e| format!("Failed to parse price response: {}", e))?;

        Ok(price_info.price)
    }

    /// Sends an audit event to the Audit Service (fire-and-forget).
    ///
    /// Errors are logged but do not block order processing.
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

        let url = format!("{}/events", self.audit_service_url);
        let client = self.http_client.clone();

        // Fire-and-forget: spawn a task so we don't block the order flow
        tokio::spawn(async move {
            match client
                .post(&url)
                .json(&event)
                .timeout(std::time::Duration::from_secs(3))
                .send()
                .await
            {
                Ok(resp) => {
                    if resp.status().is_success() {
                        info!(event_type = %event.event_type, order_id = %event.order_id, "Audit event sent");
                    } else {
                        warn!(
                            event_type = %event.event_type,
                            status = %resp.status(),
                            "Audit service returned non-success status"
                        );
                    }
                }
                Err(e) => {
                    warn!(
                        event_type = %event.event_type,
                        error = %e,
                        "Failed to send audit event (non-critical)"
                    );
                }
            }
        });
    }
}
