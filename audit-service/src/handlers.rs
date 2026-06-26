use actix_web::{web, HttpResponse};
use chrono::Utc;
use std::sync::{Arc, RwLock};
use tracing::info;
use uuid::Uuid;

use crate::models::*;

#[derive(Clone)]
pub enum EventStore {
    InMemory(Arc<RwLock<Vec<AuditEvent>>>),
    Postgres(sqlx::PgPool),
}

impl EventStore {
    pub async fn add_event(&self, event: &AuditEvent) -> Result<(), String> {
        match self {
            EventStore::InMemory(ref events) => {
                if let Ok(mut lock) = events.write() {
                    lock.push(event.clone());
                    Ok(())
                } else {
                    Err("Failed to acquire write lock for in-memory events store".to_string())
                }
            }
            EventStore::Postgres(ref pool) => {
                sqlx::query(
                    "INSERT INTO audit_events (id, event_type, order_id, user_id, details, timestamp) VALUES ($1, $2, $3, $4, $5, $6)"
                )
                .bind(&event.id)
                .bind(&event.event_type)
                .bind(&event.order_id)
                .bind(&event.user_id)
                .bind(&event.details)
                .bind(&event.timestamp)
                .execute(pool)
                .await
                .map_err(|e| format!("Failed to insert audit event into database: {}", e))?;
                Ok(())
            }
        }
    }

    pub async fn query_events(
        &self,
        user_id: Option<String>,
        event_type: Option<String>,
    ) -> Result<Vec<AuditEvent>, String> {
        match self {
            EventStore::InMemory(ref events) => {
                let lock = events.read().map_err(|_| "Failed to acquire read lock".to_string())?;
                let filtered = lock.iter()
                    .filter(|e| {
                        if let Some(ref uid) = user_id {
                            if &e.user_id != uid {
                                return false;
                            }
                        }
                        if let Some(ref et) = event_type {
                            if &e.event_type != et {
                                return false;
                            }
                        }
                        true
                    })
                    .cloned()
                    .collect();
                Ok(filtered)
            }
            EventStore::Postgres(ref pool) => {
                let rows = sqlx::query_as::<_, AuditEvent>(
                    "SELECT id, event_type, order_id, user_id, details, timestamp FROM audit_events WHERE ($1 IS NULL OR user_id = $1) AND ($2 IS NULL OR event_type = $2) ORDER BY timestamp DESC"
                )
                .bind(user_id)
                .bind(event_type)
                .fetch_all(pool)
                .await
                .map_err(|e| format!("Failed to query database: {}", e))?;
                Ok(rows)
            }
        }
    }

    pub async fn query_events_by_order(&self, order_id: &str) -> Result<Vec<AuditEvent>, String> {
        match self {
            EventStore::InMemory(ref events) => {
                let lock = events.read().map_err(|_| "Failed to acquire read lock".to_string())?;
                let filtered = lock.iter()
                    .filter(|e| e.order_id == order_id)
                    .cloned()
                    .collect();
                Ok(filtered)
            }
            EventStore::Postgres(ref pool) => {
                let rows = sqlx::query_as::<_, AuditEvent>(
                    "SELECT id, event_type, order_id, user_id, details, timestamp FROM audit_events WHERE order_id = $1 ORDER BY timestamp DESC"
                )
                .bind(order_id)
                .fetch_all(pool)
                .await
                .map_err(|e| format!("Failed to query database: {}", e))?;
                Ok(rows)
            }
        }
    }
}

/// Shared application state.
pub struct AppState {
    pub store: EventStore,
}

/// POST /events - Receive and store an audit event.
pub async fn create_event(
    data: web::Data<AppState>,
    body: web::Json<AuditEventRequest>,
) -> HttpResponse {
    let request = body.into_inner();

    let event = AuditEvent {
        id: Uuid::new_v4().to_string(),
        event_type: request.event_type.clone(),
        order_id: request.order_id.clone(),
        user_id: request.user_id.clone(),
        details: request.details,
        timestamp: Utc::now().to_rfc3339(),
    };

    info!(
        event_type = %event.event_type,
        order_id = %event.order_id,
        user_id = %event.user_id,
        "Audit event received"
    );

    match data.store.add_event(&event).await {
        Ok(_) => HttpResponse::Created().json(event),
        Err(err) => HttpResponse::InternalServerError().json(ErrorResponse {
            error: "DatabaseError".to_string(),
            message: err,
        }),
    }
}

/// GET /events - List all audit events, with optional query filters.
pub async fn get_events(
    data: web::Data<AppState>,
    query: web::Query<EventQuery>,
) -> HttpResponse {
    match data.store.query_events(query.user_id.clone(), query.event_type.clone()).await {
        Ok(events) => HttpResponse::Ok().json(events),
        Err(err) => HttpResponse::InternalServerError().json(ErrorResponse {
            error: "DatabaseError".to_string(),
            message: err,
        }),
    }
}

/// GET /events/{orderId} - Get all audit events for a specific order.
pub async fn get_events_by_order(
    data: web::Data<AppState>,
    path: web::Path<String>,
) -> HttpResponse {
    let order_id = path.into_inner();
    match data.store.query_events_by_order(&order_id).await {
        Ok(events) => HttpResponse::Ok().json(events),
        Err(err) => HttpResponse::InternalServerError().json(ErrorResponse {
            error: "DatabaseError".to_string(),
            message: err,
        }),
    }
}

/// GET /health - Health check endpoint.
pub async fn health_check() -> HttpResponse {
    HttpResponse::Ok().json(serde_json::json!({
        "status": "healthy",
        "service": "audit-service"
    }))
}
