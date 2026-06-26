mod handlers;
mod models;
mod order_engine;
mod store;

use actix_web::{web, App, HttpServer};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::Message;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};

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

    let database_url = std::env::var("PORTFOLIO_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .ok();

    let store = if let Some(url) = database_url {
        info!("Connecting to PostgreSQL database for Portfolio Service...");
        let pool = sqlx::PgPool::connect(&url)
            .await
            .expect("Failed to connect to PostgreSQL");
        info!("PostgreSQL connection established. Initializing schema...");
        let s = Store::new_postgres(pool).await;
        info!("Portfolio database schema initialized and seeded.");
        Arc::new(s)
    } else {
        warn!("DATABASE_URL not set. Running Portfolio Service in-memory mode.");
        Arc::new(Store::new_in_memory())
    };

    let order_engine = Arc::new(OrderEngine::new(
        market_service_url,
        audit_service_url,
        kafka_bootstrap_servers.clone(),
    ));

    // Start background DB flusher
    let store_for_flusher = store.clone();
    tokio::spawn(async move {
        start_db_flusher(store_for_flusher).await;
    });

    // Start background Order Consumer (HFT Core)
    if let Some(servers) = kafka_bootstrap_servers {
        let store_for_consumer = store.clone();
        let engine_for_consumer = order_engine.clone();
        tokio::spawn(async move {
            start_order_consumer(&servers, store_for_consumer, engine_for_consumer).await;
        });
    }

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

async fn start_db_flusher(store: Arc<Store>) {
    info!("Background DB Flusher started (5s interval)...");
    let mut interval = tokio::time::interval(Duration::from_secs(5));
    loop {
        interval.tick().await;
        store.flush_to_db().await;
    }
}

async fn start_order_consumer(
    bootstrap_servers: &str,
    store: Arc<Store>,
    order_engine: Arc<OrderEngine>,
) {
    let mut retry_count = 0;
    let consumer: StreamConsumer = loop {
        match ClientConfig::new()
            .set("group.id", "portfolio-service-orders")
            .set("bootstrap.servers", bootstrap_servers)
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "true")
            .set("auto.offset.reset", "earliest") // Process all queued orders
            .create()
        {
            Ok(c) => break c,
            Err(e) => {
                retry_count += 1;
                warn!("Failed to create Kafka consumer for orders (retry {}): {}", retry_count, e);
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        }
    };

    if let Err(e) = consumer.subscribe(&["incoming-orders"]) {
        error!("Can't subscribe to incoming-orders topic: {}", e);
        return;
    }

    info!("Kafka Order Consumer started, processing HFT queue...");

    loop {
        match consumer.recv().await {
            Ok(msg) => {
                if let Some(payload) = msg.payload() {
                    if let Ok(payload_str) = std::str::from_utf8(payload) {
                        if let Ok(order) = serde_json::from_str::<crate::models::Order>(payload_str) {
                            // Process order asynchronously in memory
                            order_engine.process_order(store.clone(), order).await;
                        }
                    }
                }
            }
            Err(e) => {
                warn!("Error receiving order from Kafka: {}", e);
            }
        }
    }
}
