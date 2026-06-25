mod handlers;
mod models;
mod price_engine;

use actix_web::{web, App, HttpServer};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn, error};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};

use handlers::AppState;
use price_engine::PriceEngine;

use futures_util::stream::StreamExt;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message as WsMessage};

async fn run_binance_websocket(
    price_engine: Arc<PriceEngine>,
    bootstrap_servers: Option<String>,
) {
    let mut producer: Option<FutureProducer> = None;
    let topic = "market-prices";

    // Setup Kafka producer
    if let Some(ref servers) = bootstrap_servers {
        match ClientConfig::new()
            .set("bootstrap.servers", servers)
            .set("message.timeout.ms", "3000")
            .create::<FutureProducer>()
        {
            Ok(p) => {
                info!("Connected to Kafka at {}", servers);
                producer = Some(p);
            }
            Err(e) => {
                error!("Failed to create Kafka producer: {}", e);
            }
        }
    }

    let default_url = "wss://stream.binance.com:9443/stream?streams=btcusdt@miniTicker/ethusdt@miniTicker/solusdt@miniTicker/adausdt@miniTicker/xrpusdt@miniTicker".to_string();
    let url_str = std::env::var("BINANCE_WS_URL").unwrap_or(default_url);

    loop {
        info!("Connecting to Binance WebSocket API: {}", url_str);
        match connect_async(url_str.clone()).await {
            Ok((ws_stream, _)) => {
                info!("Successfully connected to Binance WebSocket!");
                let (_, mut read) = ws_stream.split();

                while let Some(message_result) = read.next().await {
                    match message_result {
                        Ok(msg) => {
                            if let WsMessage::Text(text) = msg {
                                if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&text) {
                                    if let Some(data) = parsed.get("data") {
                                        if let (Some(symbol_raw), Some(price_raw)) = (
                                            data.get("s").and_then(|v| v.as_str()),
                                            data.get("c").and_then(|v| v.as_str()),
                                        ) {
                                            // Symbol from Binance is e.g. "BTCUSDT"
                                            // Map BTCUSDT -> BTC
                                            if symbol_raw.ends_with("USDT") {
                                                let base_symbol = &symbol_raw[..symbol_raw.len() - 4];
                                                if ["BTC", "ETH", "SOL", "ADA", "XRP"].contains(&base_symbol) {
                                                    if let Ok(price) = price_raw.parse::<f64>() {
                                                        let rounded_price = (price * 10000.0).round() / 10000.0;
                                                        price_engine.update_price(base_symbol, rounded_price);

                                                        // Publish to Kafka
                                                        if let Some(ref p) = producer {
                                                            let payload = serde_json::json!({
                                                                "symbol": base_symbol.to_string(),
                                                                "price": rounded_price,
                                                                "timestamp": chrono::Utc::now().to_rfc3339(),
                                                            }).to_string();

                                                            let record = FutureRecord::to(topic)
                                                                .key(base_symbol)
                                                                .payload(&payload);

                                                            if let Err((e, _)) = p.send(record, Duration::from_secs(1)).await {
                                                                warn!("Failed to send price to Kafka for {}: {}", base_symbol, e);
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            warn!("Error receiving message from WebSocket: {}", e);
                            break; // Break the inner loop to trigger reconnect
                        }
                    }
                }
            }
            Err(e) => {
                error!("Failed to connect to Binance WebSocket: {}. Retrying in 5 seconds...", e);
            }
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

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
    let port: u16 = std::env::var("MARKET_PORT")
        .or_else(|_| std::env::var("PORT"))
        .unwrap_or_else(|_| "8081".to_string())
        .parse()
        .expect("MARKET_PORT/PORT must be a valid number");

    let kafka_bootstrap_servers = std::env::var("KAFKA_BOOTSTRAP_SERVERS").ok();
    if let Some(ref servers) = kafka_bootstrap_servers {
        info!("Kafka configuration found: {}", servers);
    } else {
        warn!("KAFKA_BOOTSTRAP_SERVERS not set. Running in offline mode for Kafka.");
    }

    let price_engine = Arc::new(PriceEngine::new());

    // Spawn Binance WebSocket & Kafka publishing task in background
    let engine_clone = price_engine.clone();
    tokio::spawn(async move {
        run_binance_websocket(engine_clone, kafka_bootstrap_servers).await;
    });

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
