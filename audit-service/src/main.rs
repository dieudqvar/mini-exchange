mod handlers;
mod models;

use actix_web::{web, App, HttpServer};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tracing::{info, warn, error};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{StreamConsumer, Consumer};
use rdkafka::Message;

use handlers::AppState;
use models::{AuditEvent, AuditEventRequest};

async fn start_audit_consumer(bootstrap_servers: &str, events_store: Arc<RwLock<Vec<AuditEvent>>>) {
    let mut retry_count = 0;
    let consumer: StreamConsumer = loop {
        match ClientConfig::new()
            .set("group.id", "audit-service-group")
            .set("bootstrap.servers", bootstrap_servers)
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "true")
            .set("auto.offset.reset", "earliest")
            .create()
        {
            Ok(c) => break c,
            Err(e) => {
                retry_count += 1;
                warn!("Failed to create Kafka consumer for Audit Service (retry {}): {}", retry_count, e);
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        }
    };

    if let Err(e) = consumer.subscribe(&["audit-events"]) {
        error!("Can't subscribe to audit-events topic: {}", e);
        return;
    }

    info!("Kafka Audit Event Consumer started, subscribed to 'audit-events'...");

    loop {
        match consumer.recv().await {
            Ok(msg) => {
                info!("Audit Service received raw message from Kafka of length {}", msg.payload_len());
                if let Some(payload) = msg.payload() {
                    match std::str::from_utf8(payload) {
                        Ok(payload_str) => {
                            info!("Raw message payload: {}", payload_str);
                            match serde_json::from_str::<AuditEventRequest>(payload_str) {
                                Ok(req) => {
                                    let event = AuditEvent {
                                        id: uuid::Uuid::new_v4().to_string(),
                                        event_type: req.event_type.clone(),
                                        order_id: req.order_id.clone(),
                                        user_id: req.user_id.clone(),
                                        details: req.details,
                                        timestamp: chrono::Utc::now().to_rfc3339(),
                                    };

                                    if let Ok(mut events) = events_store.write() {
                                        info!(
                                            event_type = %event.event_type,
                                            order_id = %event.order_id,
                                            user_id = %event.user_id,
                                            "Audit Service processed and saved event"
                                        );
                                        events.push(event);
                                    }
                                }
                                Err(err) => {
                                    error!("Failed to deserialize AuditEventRequest: {} (payload: {})", err, payload_str);
                                }
                            }
                        }
                        Err(err) => {
                            error!("Payload is not valid UTF-8: {}", err);
                        }
                    }
                }
            }
            Err(e) => {
                warn!("Error receiving event from Kafka: {}", e);
            }
        }
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
    let port: u16 = std::env::var("AUDIT_PORT")
        .or_else(|_| std::env::var("PORT"))
        .unwrap_or_else(|_| "8083".to_string())
        .parse()
        .expect("AUDIT_PORT/PORT must be a valid number");

    let kafka_bootstrap_servers = std::env::var("KAFKA_BOOTSTRAP_SERVERS").ok();
    if let Some(ref servers) = kafka_bootstrap_servers {
        info!("Kafka configuration found: {}", servers);
    } else {
        warn!("KAFKA_BOOTSTRAP_SERVERS not set. Running in offline REST-only mode.");
    }

    info!("Audit Service starting on {}:{}", host, port);

    let events_store = Arc::new(RwLock::new(Vec::new()));

    // Spawn Kafka audit event consumer in background if config is available
    if let Some(servers) = kafka_bootstrap_servers {
        let store_clone = events_store.clone();
        tokio::spawn(async move {
            start_audit_consumer(&servers, store_clone).await;
        });
    }

    let app_state = web::Data::new(AppState {
        events: events_store,
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
