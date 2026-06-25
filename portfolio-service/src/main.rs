mod handlers;
mod models;
mod order_engine;
mod store;

use actix_web::{web, App, HttpServer};
use std::sync::Arc;
use tracing::info;

use handlers::AppState;
use order_engine::OrderEngine;
use store::Store;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Load environment variables from .env file
    dotenvy::dotenv().ok();

    // Initialize tracing subscriber for structured logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let host = std::env::var("HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
    let port: u16 = std::env::var("PORTFOLIO_PORT")
        .or_else(|_| std::env::var("PORT"))
        .unwrap_or_else(|_| "8082".to_string())
        .parse()
        .expect("PORTFOLIO_PORT/PORT must be a valid number");

    let market_service_url =
        std::env::var("MARKET_SERVICE_URL").unwrap_or_else(|_| "http://localhost:8081".to_string());
    let audit_service_url =
        std::env::var("AUDIT_SERVICE_URL").unwrap_or_else(|_| "http://localhost:8083".to_string());
    let kafka_bootstrap_servers = std::env::var("KAFKA_BOOTSTRAP_SERVERS").ok();

    info!("Market Service URL: {}", market_service_url);
    info!("Audit Service URL: {}", audit_service_url);

    let store = Arc::new(Store::new());
    let order_engine = Arc::new(OrderEngine::new(market_service_url, audit_service_url, kafka_bootstrap_servers));

    info!("Portfolio Service starting on {}:{}", host, port);

    HttpServer::new(move || {
        let app_state = AppState {
            store: store.clone(),
            order_engine: order_engine.clone(),
        };

        App::new()
            .app_data(web::Data::new(app_state))
            .route("/health", web::get().to(handlers::health_check))
            .route("/portfolio/{userId}", web::get().to(handlers::get_portfolio))
            .route("/orders", web::post().to(handlers::create_order))
            .route("/orders/{orderId}", web::get().to(handlers::get_order))
    })
    .bind((host.as_str(), port))?
    .run()
    .await
}
