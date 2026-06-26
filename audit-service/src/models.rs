use serde::{Deserialize, Serialize};

/// Types of audit events captured by the system.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub enum EventType {
    #[serde(rename = "ORDER_CREATED")]
    OrderCreated,
    #[serde(rename = "ORDER_EXECUTED")]
    OrderExecuted,
    #[serde(rename = "ORDER_REJECTED")]
    OrderRejected,
}

impl std::fmt::Display for EventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventType::OrderCreated => write!(f, "ORDER_CREATED"),
            EventType::OrderExecuted => write!(f, "ORDER_EXECUTED"),
            EventType::OrderRejected => write!(f, "ORDER_REJECTED"),
        }
    }
}

/// An incoming audit event from the Portfolio Service.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEventRequest {
    pub event_type: String,
    pub order_id: String,
    pub user_id: String,
    pub details: serde_json::Value,
}

/// A stored audit event with additional metadata.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct AuditEvent {
    pub id: String,
    pub event_type: String,
    pub order_id: String,
    pub user_id: String,
    pub details: serde_json::Value,
    pub timestamp: String,
}

/// Standard API error response.
#[derive(Debug, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct ErrorResponse {
    pub error: String,
    pub message: String,
}

/// Query parameters for filtering events.
#[derive(Debug, Deserialize)]
pub struct EventQuery {
    pub user_id: Option<String>,
    pub event_type: Option<String>,
}
