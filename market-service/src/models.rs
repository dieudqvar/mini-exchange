use serde::{Deserialize, Serialize};

/// Represents a tradeable symbol on the exchange.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Symbol {
    pub symbol: String,
    pub name: String,
}

/// Price information for a specific symbol.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceInfo {
    pub symbol: String,
    pub price: f64,
    pub timestamp: String,
}

/// Standard API error response.
#[derive(Debug, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error: String,
    pub message: String,
}
