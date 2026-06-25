mod handlers;
mod models;

use actix_web::{web, App, HttpServer};
use std::sync::RwLock;
use tracing::info;

use handlers::AppState;

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
        .unwrap_or_else(|_| "8083".to_string())
        .parse()
        .expect("PORT must be a valid number");

    info!("Audit Service starting on {}:{}", host, port);

    let app_state = web::Data::new(AppState {
        events: RwLock::new(Vec::new()),
    });

    HttpServer::new(move || {
        App::new()
            .app_data(app_state.clone())
            .route("/health", web::get().to(handlers::health_check))
            .route("/events", web::post().to(handlers::create_event))
            .route("/events", web::get().to(handlers::get_events))
            .route("/events/{orderId}", web::get().to(handlers::get_events_by_order))
    })
    .bind((host.as_str(), port))?
    .run()
    .await
}
