use actix_web::{web, HttpResponse};
use chrono::Utc;
use std::sync::Arc;

use crate::models::{ErrorResponse, PriceInfo, Symbol};
use crate::price_engine::PriceEngine;

/// Application state shared across all request handlers.
pub struct AppState {
    pub price_engine: Arc<PriceEngine>,
}

/// GET /symbols - Returns all available trading symbols.
pub async fn get_symbols(data: web::Data<AppState>) -> HttpResponse {
    let symbols: Vec<Symbol> = data
        .price_engine
        .get_symbols()
        .into_iter()
        .map(|(symbol, name)| Symbol { symbol, name })
        .collect();

    HttpResponse::Ok().json(symbols)
}

/// GET /prices - Returns current prices for all symbols.
pub async fn get_prices(data: web::Data<AppState>) -> HttpResponse {
    let all_prices = data.price_engine.get_all_prices();
    let timestamp = Utc::now().to_rfc3339();

    let prices: Vec<PriceInfo> = all_prices
        .into_iter()
        .map(|(symbol, price)| PriceInfo {
            symbol,
            price,
            timestamp: timestamp.clone(),
        })
        .collect();

    HttpResponse::Ok().json(prices)
}

/// GET /prices/{symbol} - Returns the current price for a specific symbol.
pub async fn get_price_by_symbol(
    data: web::Data<AppState>,
    path: web::Path<String>,
) -> HttpResponse {
    let symbol = path.into_inner().to_uppercase();

    match data.price_engine.get_price(&symbol) {
        Some(price) => {
            let price_info = PriceInfo {
                symbol,
                price,
                timestamp: Utc::now().to_rfc3339(),
            };
            HttpResponse::Ok().json(price_info)
        }
        None => {
            let error = ErrorResponse {
                error: "NOT_FOUND".to_string(),
                message: format!("Symbol '{}' not found", symbol),
            };
            HttpResponse::NotFound().json(error)
        }
    }
}

/// GET /health - Health check endpoint.
pub async fn health_check() -> HttpResponse {
    HttpResponse::Ok().json(serde_json::json!({
        "status": "healthy",
        "service": "market-service"
    }))
}
