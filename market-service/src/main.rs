mod handlers;
mod models;
mod price_engine;

use actix_web::{web, App, HttpServer};
use std::sync::Arc;
use tracing::info;

use handlers::AppState;
use price_engine::PriceEngine;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Initialize tracing subscriber for structured logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let host = std::env::var("HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
    let port: u16 = std::env::var("PORT")
        .unwrap_or_else(|_| "8081".to_string())
        .parse()
        .expect("PORT must be a valid number");

    let price_engine = Arc::new(PriceEngine::new());

    info!("Market Service starting on {}:{}", host, port);

    HttpServer::new(move || {
        let app_state = AppState {
            price_engine: price_engine.clone(),
        };

        App::new()
            .app_data(web::Data::new(app_state))
            .route("/health", web::get().to(handlers::health_check))
            .route("/symbols", web::get().to(handlers::get_symbols))
            .route("/prices", web::get().to(handlers::get_prices))
            .route("/prices/{symbol}", web::get().to(handlers::get_price_by_symbol))
    })
    .bind((host.as_str(), port))?
    .run()
    .await
}
