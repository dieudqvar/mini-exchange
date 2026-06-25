use actix_web::{web, HttpResponse};
use std::sync::Arc;

use crate::models::*;
use crate::order_engine::OrderEngine;
use crate::store::Store;

/// Shared application state for the Portfolio Service.
pub struct AppState {
    pub store: Arc<Store>,
    pub order_engine: Arc<OrderEngine>,
}

/// GET /portfolio/{userId} - Returns the portfolio for a given user.
pub async fn get_portfolio(
    data: web::Data<AppState>,
    path: web::Path<String>,
) -> HttpResponse {
    let user_id = path.into_inner();

    match data.store.get_portfolio(&user_id) {
        Some(portfolio) => HttpResponse::Ok().json(portfolio),
        None => {
            let error = ErrorResponse {
                error: "USER_NOT_FOUND".to_string(),
                message: format!("User '{}' not found", user_id),
            };
            HttpResponse::NotFound().json(error)
        }
    }
}

/// POST /orders - Submit a new market order.
///
/// Accepts an `OrderRequest` and processes it through the order engine.
/// Returns the created order with its execution status.
pub async fn create_order(
    data: web::Data<AppState>,
    body: web::Json<OrderRequest>,
) -> HttpResponse {
    let request = body.into_inner();

    match data.order_engine.process_order(&data.store, request).await {
        Ok(order) => {
            let response = OrderResponse {
                message: format!("Order {} executed successfully", order.id),
                order,
            };
            HttpResponse::Created().json(response)
        }
        Err((status_code, error)) => {
            match status_code {
                400 => HttpResponse::BadRequest().json(error),
                404 => HttpResponse::NotFound().json(error),
                503 => HttpResponse::ServiceUnavailable().json(error),
                _ => HttpResponse::InternalServerError().json(error),
            }
        }
    }
}

/// GET /orders/{orderId} - Get the status of an order by its ID.
pub async fn get_order(
    data: web::Data<AppState>,
    path: web::Path<String>,
) -> HttpResponse {
    let order_id = path.into_inner();

    match data.store.get_order(&order_id) {
        Some(order) => HttpResponse::Ok().json(order),
        None => {
            let error = ErrorResponse {
                error: "ORDER_NOT_FOUND".to_string(),
                message: format!("Order '{}' not found", order_id),
            };
            HttpResponse::NotFound().json(error)
        }
    }
}

/// GET /health - Health check endpoint.
pub async fn health_check() -> HttpResponse {
    HttpResponse::Ok().json(serde_json::json!({
        "status": "healthy",
        "service": "portfolio-service"
    }))
}
