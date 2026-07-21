//! The control socket: a loopback HTTP server. This is the consumer's entire
//! inbound surface, including its machine-readable OpenAPI contract.

use crate::config::Config;
use crate::model::{SendJob, SendRequest, SendTarget};
use crate::permissions;
use crate::store::{self, JournaledEvent, Store};
use crate::webhook::EventSink;
use axum::{
    body::Body,
    extract::{Path, Query, Request, State},
    http::{header, HeaderValue, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use bytes::Bytes;
use serde::Deserialize;
use serde_json::json;
use std::convert::Infallible;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use subtle::ConstantTimeEq;
use tokio::sync::{broadcast, mpsc};

const OPENAPI_DOCUMENT: &str = include_str!("../openapi.json");

#[derive(Clone)]
pub struct AppState {
    pub send_tx: mpsc::UnboundedSender<()>,
    pub start: Instant,
    pub chatdb: PathBuf,
    pub config: Arc<Config>,
    pub store: Store,
    pub events: broadcast::Sender<JournaledEvent>,
    pub event_sink: EventSink,
    pub permissions: permissions::PermissionState,
}

#[derive(Debug, Deserialize)]
struct EventsQuery {
    #[serde(default)]
    since: i64,
    limit: Option<u64>,
}

pub fn router(state: AppState) -> Router {
    let auth_config = state.config.clone();
    Router::new()
        .route("/openapi.json", get(get_openapi))
        .route("/messages", get(get_messages).post(post_messages))
        .route("/messages/:id", get(get_message))
        .route("/events", get(get_events))
        .route("/events/stream", get(stream_events))
        .route("/healthz", get(get_health))
        .route("/status", get(get_status))
        .route("/debug/chatdb", get(get_debug_chatdb))
        .layer(middleware::from_fn_with_state(auth_config, require_auth))
        .with_state(state)
}

async fn get_openapi() -> Response {
    Response::builder()
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(OPENAPI_DOCUMENT))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

async fn require_auth(State(config): State<Arc<Config>>, request: Request, next: Next) -> Response {
    let Some(token) = config.api_token.as_deref() else {
        return next.run(request).await;
    };
    if bearer_matches(request.headers().get(header::AUTHORIZATION), token) {
        next.run(request).await
    } else {
        (
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "missing or invalid bearer token" })),
        )
            .into_response()
    }
}

fn bearer_matches(header: Option<&HeaderValue>, expected: &str) -> bool {
    let Some(provided) = header
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
    else {
        return false;
    };
    bool::from(provided.as_bytes().ct_eq(expected.as_bytes()))
}

async fn get_health(State(state): State<AppState>) -> impl IntoResponse {
    Json(json!({
        "status": "ok",
        "product": "blueski",
        "version": env!("CARGO_PKG_VERSION"),
        "installation_id": state.config.installation_id,
        "port": state.config.port,
        "authentication_required": state.config.api_token.is_some(),
    }))
}

async fn get_messages(State(state): State<AppState>, Query(q): Query<EventsQuery>) -> Response {
    let conn = match state.store.conn() {
        Ok(conn) => conn,
        Err(error) => return internal_error(error),
    };
    match store::list_messages(&conn, q.since, q.limit).await {
        Ok(messages) => Json(messages).into_response(),
        Err(error) => internal_error(error),
    }
}

async fn get_message(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    let conn = match state.store.conn() {
        Ok(conn) => conn,
        Err(error) => return internal_error(error),
    };
    match store::get_message_events(&conn, &id).await {
        Ok(events) if events.is_empty() => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "message not found" })),
        )
            .into_response(),
        Ok(events) => Json(json!({ "message_id": id, "events": events })).into_response(),
        Err(error) => internal_error(error),
    }
}

fn internal_error(error: impl std::fmt::Display) -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "error": error.to_string() })),
    )
        .into_response()
}

async fn get_debug_chatdb(State(state): State<AppState>) -> impl IntoResponse {
    match crate::debug::inspect(&state.chatdb) {
        Ok(v) => Json(v),
        Err(e) => Json(json!({ "error": e.to_string() })),
    }
}

async fn post_messages(State(state): State<AppState>, Json(req): Json<SendRequest>) -> Response {
    let target = match (req.to, req.chat_id) {
        (Some(to), None) => SendTarget::Handle { to },
        (None, Some(chat_id)) => SendTarget::Chat { chat_id },
        (Some(_), Some(_)) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "provide either to or chat_id, not both" })),
            )
                .into_response();
        }
        (None, None) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "provide to or chat_id" })),
            )
                .into_response();
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

    let conn = match state.store.conn() {
        Ok(conn) => conn,
        Err(error) => return internal_error(error),
    };
    let accepted = match store::accept_and_record_queued(&conn, &job).await {
        Ok(accepted) => accepted,
        Err(error) if error.downcast_ref::<store::IdempotencyConflict>().is_some() => {
            return (
                StatusCode::CONFLICT,
                Json(json!({
                    "error": error.to_string(),
                    "client_ref": job.client_ref,
                })),
            )
                .into_response();
        }
        Err(error) => return internal_error(error),
    };
    if let Some(journaled) = accepted.journaled {
        state.event_sink.publish_committed(journaled);
    }

    if accepted.is_new && state.send_tx.send(()).is_err() {
        // The command is already durable. A restarted worker will recover it.
        tracing::warn!(message_id = %accepted.message_id, "send worker wake-up channel is closed");
    }

    (
        StatusCode::ACCEPTED,
        Json(json!({
            "message_id": accepted.message_id,
            "status": accepted.status,
            "idempotent": accepted.idempotent,
        })),
    )
        .into_response()
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
    let webhooks = state.event_sink.webhook_statuses();
    Json(json!({
        "status": "ok",
        "product": "blueski",
        "version": env!("CARGO_PKG_VERSION"),
        "installation_id": state.config.installation_id,
        "transport": "applescript",
        "uptime_secs": state.start.elapsed().as_secs(),
        "port": state.config.port,
        "webhook_configured": !webhooks.is_empty(),
        "webhook_count": webhooks.len(),
        "webhooks": webhooks,
        "permissions": {
            "checked": state.permissions.checked(),
            "full_disk_access": state.permissions.full_disk_access(),
            "automation": state.permissions.automation(),
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
        let event_sink = crate::webhook::spawn(
            reqwest::Client::new(),
            Vec::new(),
            store.clone(),
            events.clone(),
        );
        AppState {
            send_tx,
            start: Instant::now(),
            chatdb: PathBuf::from("/tmp/nonexistent-chat.db"),
            config: Arc::new(Config::default()),
            store,
            events,
            event_sink,
            permissions: permissions::PermissionState::default(),
        }
    }

    #[tokio::test]
    async fn events_endpoint_returns_journaled_events() {
        let store = Store::open(&temp_db_path("events-route"), "bsinst_test")
            .await
            .unwrap();
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
        let store = Store::open(&temp_db_path("messages-missing-target"), "bsinst_test")
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
        let store = Store::open(&temp_db_path("messages-ambiguous-target"), "bsinst_test")
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
    async fn messages_endpoint_commits_before_202_and_replays_client_ref() {
        let store = Store::open(&temp_db_path("messages-idempotent"), "bsinst_test")
            .await
            .unwrap();
        let app = router(test_state(store.clone()).await);
        let request_body = r#"{"to":"+15550000001","text":"hello","client_ref":"outbox-42"}"#;

        let first = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/messages")
                    .header("content-type", "application/json")
                    .body(Body::from(request_body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(first.status(), StatusCode::ACCEPTED);
        let first_body = to_bytes(first.into_body(), 1024 * 1024).await.unwrap();
        let first_json: serde_json::Value = serde_json::from_slice(&first_body).unwrap();
        assert_eq!(first_json["idempotent"], true);

        let replay = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/messages")
                    .header("content-type", "application/json")
                    .body(Body::from(request_body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(replay.status(), StatusCode::ACCEPTED);
        let replay_body = to_bytes(replay.into_body(), 1024 * 1024).await.unwrap();
        let replay_json: serde_json::Value = serde_json::from_slice(&replay_body).unwrap();
        assert_eq!(replay_json["message_id"], first_json["message_id"]);
        assert_eq!(replay_json["idempotent"], true);

        let conflict = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/messages")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"to":"+15550000001","text":"changed","client_ref":"outbox-42"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(conflict.status(), StatusCode::CONFLICT);

        let conn = store.conn().unwrap();
        let queued = store::next_queued(&conn).await.unwrap().unwrap();
        assert_eq!(queued.job.message_id, first_json["message_id"]);
        let events = store::list_events_since(&conn, 0, None).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event, "message.queued");
    }

    #[tokio::test]
    async fn message_collection_and_detail_are_public_routes() {
        let store = Store::open(&temp_db_path("message-routes"), "bsinst_test")
            .await
            .unwrap();
        let conn = store.conn().unwrap();
        let event = Event::new("message.sent", "message-1".to_string());
        store::record_event(&conn, &event).await.unwrap();
        let app = router(test_state(store).await);

        let collection = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/messages?since=0&limit=10")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(collection.status(), StatusCode::OK);

        let detail = app
            .oneshot(
                Request::builder()
                    .uri("/messages/message-1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(detail.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn status_reports_applescript_as_the_only_transport() {
        let store = Store::open(&temp_db_path("status-transport"), "bsinst_test")
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
        assert_eq!(status["product"], "blueski");
        assert!(status["installation_id"]
            .as_str()
            .unwrap()
            .starts_with("bsinst_"));
        assert_eq!(status["webhook_count"], 0);
        assert!(status["webhooks"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn status_exposes_only_safe_webhook_metadata() {
        let store = Store::open(&temp_db_path("status-webhooks"), "bsinst_test")
            .await
            .unwrap();
        let mut state = test_state(store.clone()).await;
        let webhook = crate::config::WebhookConfig {
            id: "audit".to_string(),
            url: "https://events.example.com/private".to_string(),
            secret: "never-return-this".to_string(),
            enabled: true,
        };
        state.config = Arc::new(Config {
            webhooks: vec![webhook.clone()],
            ..Config::default()
        });
        state.event_sink = crate::webhook::spawn(
            reqwest::Client::new(),
            vec![webhook],
            store,
            state.events.clone(),
        );

        let response = router(state)
            .oneshot(
                Request::builder()
                    .uri("/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let raw = String::from_utf8(body.to_vec()).unwrap();
        let status: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(status["webhook_count"], 1);
        assert_eq!(status["webhooks"][0]["id"], "audit");
        assert!(!raw.contains("events.example.com"));
        assert!(!raw.contains("never-return-this"));
    }

    #[tokio::test]
    async fn health_is_fast_and_side_effect_free() {
        let store = Store::open(&temp_db_path("health-route"), "bsinst_test")
            .await
            .unwrap();
        let app = router(test_state(store).await);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let health: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(health["product"], "blueski");
        assert!(health["installation_id"]
            .as_str()
            .unwrap()
            .starts_with("bsinst_"));
        assert!(health.get("permissions").is_none());
    }

    #[tokio::test]
    async fn openapi_document_matches_the_server_contract() {
        let store = Store::open(&temp_db_path("openapi-route"), "bsinst_test")
            .await
            .unwrap();
        let response = router(test_state(store).await)
            .oneshot(
                Request::builder()
                    .uri("/openapi.json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/json"
        );
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let document: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(document["openapi"], "3.1.0");
        assert_eq!(document["info"]["version"], env!("CARGO_PKG_VERSION"));

        let paths = document["paths"].as_object().unwrap();
        for path in [
            "/openapi.json",
            "/healthz",
            "/status",
            "/messages",
            "/messages/{id}",
            "/events",
            "/events/stream",
            "/debug/chatdb",
        ] {
            assert!(paths.contains_key(path), "OpenAPI is missing {path}");
        }
        assert!(document["webhooks"].get("messageEvent").is_some());
    }

    #[tokio::test]
    async fn configured_bearer_token_protects_every_route() {
        let store = Store::open(&temp_db_path("auth-route"), "bsinst_test")
            .await
            .unwrap();
        let mut state = test_state(store).await;
        let config = Config {
            api_token: Some("test-secret".to_string()),
            ..Config::default()
        };
        state.config = Arc::new(config);
        let app = router(state);

        let denied = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);

        let allowed = app
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .header("authorization", "Bearer test-secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(allowed.status(), StatusCode::OK);
    }

    #[test]
    fn bearer_auth_requires_an_exact_token() {
        let valid = HeaderValue::from_static("Bearer test-secret");
        let wrong = HeaderValue::from_static("Bearer test-secreu");
        let wrong_scheme = HeaderValue::from_static("Basic test-secret");

        assert!(bearer_matches(Some(&valid), "test-secret"));
        assert!(!bearer_matches(Some(&wrong), "test-secret"));
        assert!(!bearer_matches(Some(&wrong_scheme), "test-secret"));
        assert!(!bearer_matches(None, "test-secret"));
    }
}
