use actix_web::{web, HttpResponse};
use chrono::Utc;
use std::sync::RwLock;
use tracing::info;
use uuid::Uuid;

use crate::models::*;

/// In-memory event store shared across request handlers.
pub struct AppState {
    pub events: RwLock<Vec<AuditEvent>>,
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

    let mut events = data.events.write().unwrap();
    events.push(event.clone());

    HttpResponse::Created().json(event)
}

/// GET /events - List all audit events, with optional query filters.
///
/// Query parameters:
/// - `user_id`: Filter events by user ID
/// - `event_type`: Filter events by type (e.g., ORDER_CREATED)
pub async fn get_events(
    data: web::Data<AppState>,
    query: web::Query<EventQuery>,
) -> HttpResponse {
    let events = data.events.read().unwrap();

    let filtered: Vec<&AuditEvent> = events
        .iter()
        .filter(|e| {
            if let Some(ref user_id) = query.user_id {
                if &e.user_id != user_id {
                    return false;
                }
            }
            if let Some(ref event_type) = query.event_type {
                if &e.event_type != event_type {
                    return false;
                }
            }
            true
        })
        .collect();

    HttpResponse::Ok().json(filtered)
}

/// GET /events/{orderId} - Get all audit events for a specific order.
pub async fn get_events_by_order(
    data: web::Data<AppState>,
    path: web::Path<String>,
) -> HttpResponse {
    let order_id = path.into_inner();
    let events = data.events.read().unwrap();

    let order_events: Vec<&AuditEvent> = events
        .iter()
        .filter(|e| e.order_id == order_id)
        .collect();

    HttpResponse::Ok().json(order_events)
}

/// GET /health - Health check endpoint.
pub async fn health_check() -> HttpResponse {
    HttpResponse::Ok().json(serde_json::json!({
        "status": "healthy",
        "service": "audit-service"
    }))
}
