//! The control socket: a loopback HTTP server. This is the consumer's entire
//! inbound surface — `POST /messages` and `GET /status`.

use crate::config::Config;
use crate::model::{SendJob, SendRequest, SendTarget};
use crate::permissions;
use crate::store::{self, JournaledEvent, Store};
use axum::{
    body::Body,
    extract::{Query, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use bytes::Bytes;
use serde::Deserialize;
use serde_json::json;
use std::convert::Infallible;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{broadcast, mpsc};

#[derive(Clone)]
pub struct AppState {
    pub send_tx: mpsc::UnboundedSender<SendJob>,
    pub start: Instant,
    pub chatdb: PathBuf,
    pub config: Arc<Config>,
    pub store: Store,
    pub events: broadcast::Sender<JournaledEvent>,
}

#[derive(Debug, Deserialize)]
struct EventsQuery {
    #[serde(default)]
    since: i64,
    limit: Option<u64>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/messages", post(post_messages))
        .route("/events", get(get_events))
        .route("/events/stream", get(stream_events))
        .route("/status", get(get_status))
        .route("/debug/chatdb", get(get_debug_chatdb))
        .with_state(state)
}

async fn get_debug_chatdb(State(state): State<AppState>) -> impl IntoResponse {
    match crate::debug::inspect(&state.chatdb) {
        Ok(v) => Json(v),
        Err(e) => Json(json!({ "error": e.to_string() })),
    }
}

async fn post_messages(
    State(state): State<AppState>,
    Json(req): Json<SendRequest>,
) -> impl IntoResponse {
    let target = match (req.to, req.chat_id) {
        (Some(to), None) => SendTarget::Handle { to },
        (None, Some(chat_id)) => SendTarget::Chat { chat_id },
        (Some(_), Some(_)) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "provide either to or chat_id, not both" })),
            );
        }
        (None, None) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "provide to or chat_id" })),
            );
        }
    };

    let message_id = uuid::Uuid::new_v4().to_string();
    let job = SendJob {
        message_id: message_id.clone(),
        target,
        text: req.text,
        protocol: req.protocol,
        client_ref: req.client_ref,
    };

    if state.send_tx.send(job).is_err() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": "send worker unavailable" })),
        );
    }

    (
        StatusCode::ACCEPTED,
        Json(json!({ "message_id": message_id, "status": "queued" })),
    )
}

async fn get_events(
    State(state): State<AppState>,
    Query(q): Query<EventsQuery>,
) -> impl IntoResponse {
    let conn = match state.store.conn() {
        Ok(conn) => conn,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": e.to_string() })),
            )
                .into_response()
        }
    };

    match store::list_events_since(&conn, q.since, q.limit).await {
        Ok(events) => Json(events).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

async fn stream_events(State(state): State<AppState>, Query(q): Query<EventsQuery>) -> Response {
    let store = state.store.clone();
    let mut rx = state.events.subscribe();
    let mut last_sent = q.since;

    let stream = async_stream::stream! {
        match store.conn() {
            Ok(conn) => match store::list_events_since(&conn, last_sent, None).await {
                Ok(events) => {
                    for ev in events {
                        last_sent = ev.id;
                        match serde_json::to_string(&ev) {
                            Ok(line) => yield Ok::<Bytes, Infallible>(Bytes::from(format!("{line}\n"))),
                            Err(e) => tracing::warn!(error = %e, "serialize journaled event"),
                        }
                    }
                }
                Err(e) => tracing::warn!(error = %e, "stream backlog query failed"),
            },
            Err(e) => tracing::warn!(error = %e, "stream cannot connect store"),
        }

        loop {
            match rx.recv().await {
                Ok(ev) => {
                    if ev.id <= last_sent {
                        continue;
                    }
                    last_sent = ev.id;
                    match serde_json::to_string(&ev) {
                        Ok(line) => yield Ok::<Bytes, Infallible>(Bytes::from(format!("{line}\n"))),
                        Err(e) => tracing::warn!(error = %e, "serialize journaled event"),
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    match store.conn() {
                        Ok(conn) => match store::list_events_since(&conn, last_sent, None).await {
                            Ok(events) => {
                                for ev in events {
                                    last_sent = ev.id;
                                    match serde_json::to_string(&ev) {
                                        Ok(line) => yield Ok::<Bytes, Infallible>(Bytes::from(format!("{line}\n"))),
                                        Err(e) => tracing::warn!(error = %e, "serialize journaled event"),
                                    }
                                }
                            }
                            Err(e) => tracing::warn!(error = %e, "stream lag replay failed"),
                        },
                        Err(e) => tracing::warn!(error = %e, "stream lag cannot connect store"),
                    }
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    };

    Response::builder()
        .header(header::CONTENT_TYPE, "application/x-ndjson")
        .body(Body::from_stream(stream))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

async fn get_status(State(state): State<AppState>) -> impl IntoResponse {
    Json(json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION"),
        "transport": "applescript",
        "uptime_secs": state.start.elapsed().as_secs(),
        "port": state.config.port,
        "webhook_configured": state.config.webhook_url.is_some(),
        "permissions": {
            "full_disk_access": permissions::has_full_disk_access(&state.chatdb),
            "automation": permissions::has_automation(),
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Event;
    use axum::body::to_bytes;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    fn temp_db_path(name: &str) -> String {
        std::env::temp_dir()
            .join(format!("blueski-{name}-{}.db", uuid::Uuid::new_v4()))
            .to_string_lossy()
            .into_owned()
    }

    async fn test_state(store: Store) -> AppState {
        let (send_tx, _send_rx) = mpsc::unbounded_channel();
        let (events, _) = broadcast::channel(16);
        AppState {
            send_tx,
            start: Instant::now(),
            chatdb: PathBuf::from("/tmp/nonexistent-chat.db"),
            config: Arc::new(Config::default()),
            store,
            events,
        }
    }

    #[tokio::test]
    async fn events_endpoint_returns_journaled_events() {
        let store = Store::open(&temp_db_path("events-route")).await.unwrap();
        let conn = store.conn().unwrap();
        let mut ev = Event::new("message.received", "msg-1".to_string());
        ev.text = Some("hello".to_string());
        let saved = store::record_event(&conn, &ev).await.unwrap();

        let app = router(test_state(store).await);
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/events?since={}&limit=5", saved.id - 1))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let events: Vec<JournaledEvent> = serde_json::from_slice(&body).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].id, saved.id);
        assert_eq!(events[0].text.as_deref(), Some("hello"));
    }

    #[tokio::test]
    async fn messages_endpoint_rejects_missing_target() {
        let store = Store::open(&temp_db_path("messages-missing-target"))
            .await
            .unwrap();
        let app = router(test_state(store).await);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/messages")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"text":"hello"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn messages_endpoint_rejects_ambiguous_target() {
        let store = Store::open(&temp_db_path("messages-ambiguous-target"))
            .await
            .unwrap();
        let app = router(test_state(store).await);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/messages")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"to":"+15550000001","chat_id":"chat-1","text":"hello"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn status_reports_applescript_as_the_only_transport() {
        let store = Store::open(&temp_db_path("status-transport"))
            .await
            .unwrap();
        let app = router(test_state(store).await);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let status: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(status["transport"], "applescript");
    }
}
