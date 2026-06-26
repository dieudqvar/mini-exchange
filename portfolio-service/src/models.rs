use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Side of a trade order.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, sqlx::Type)]
#[serde(rename_all = "UPPERCASE")]
#[sqlx(type_name = "text", rename_all = "UPPERCASE")]
pub enum OrderSide {
    Buy,
    Sell,
}

impl std::fmt::Display for OrderSide {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OrderSide::Buy => write!(f, "BUY"),
            OrderSide::Sell => write!(f, "SELL"),
        }
    }
}

/// Current status of an order.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, sqlx::Type)]
#[serde(rename_all = "UPPERCASE")]
#[sqlx(type_name = "text", rename_all = "UPPERCASE")]
pub enum OrderStatus {
    Pending,
    Executed,
    Rejected,
}

impl std::fmt::Display for OrderStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OrderStatus::Pending => write!(f, "PENDING"),
            OrderStatus::Executed => write!(f, "EXECUTED"),
            OrderStatus::Rejected => write!(f, "REJECTED"),
        }
    }
}

/// A user's portfolio containing cash balance and asset holdings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Portfolio {
    pub user_id: String,
    pub cash_balance: f64,
    pub assets: HashMap<String, f64>,
}

/// Represents a trade order in the system.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct Order {
    pub id: String,
    pub user_id: String,
    pub symbol: String,
    pub side: OrderSide,
    pub quantity: f64,
    pub price: Option<f64>,
    pub total: Option<f64>,
    pub status: OrderStatus,
    pub reject_reason: Option<String>,
    pub created_at: String,
}

/// Incoming order request from the client.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderRequest {
    pub user_id: String,
    pub symbol: String,
    pub side: OrderSide,
    pub quantity: f64,
}

/// Price info received from the Market Service.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceInfo {
    pub symbol: String,
    pub price: f64,
    pub timestamp: String,
}

/// An audit event sent to the Audit Service.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEvent {
    pub event_type: String,
    pub order_id: String,
    pub user_id: String,
    pub details: serde_json::Value,
}

/// Standard API error response.
#[derive(Debug, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error: String,
    pub message: String,
}

/// Order response returned to the client.
#[derive(Debug, Serialize, Deserialize)]
pub struct OrderResponse {
    pub order: Order,
    pub message: String,
}
