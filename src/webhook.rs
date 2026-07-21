//! Event fan-out. Workers push `Event`s into an in-process channel; a single
//! background task journals each event, publishes it to local subscribers, and
//! fans the serialized envelope out to isolated destination workers.

use crate::config::WebhookConfig;
use crate::model::Event;
use crate::store::{self, JournaledEvent, Store};
use anyhow::{anyhow, Result};
use hmac::{Hmac, Mac};
use serde::Serialize;
use sha2::Sha256;
use std::sync::{mpsc as std_mpsc, Arc, RwLock};
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};

const MAX_ATTEMPTS: u32 = 3;
const EVENT_QUEUE_CAPACITY: usize = 1024;
const DESTINATION_QUEUE_CAPACITY: usize = 256;
const SIGNATURE_HEADER: &str = "X-Blueski-Signature";

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct WebhookStatus {
    pub id: String,
    pub enabled: bool,
    pub last_success_at: Option<String>,
    pub last_error: Option<String>,
}

/// Clonable handle for journal acknowledgments and post-commit notifications.
#[derive(Clone)]
pub struct EventSink {
    tx: mpsc::Sender<SinkItem>,
    webhook_statuses: Arc<RwLock<Vec<WebhookStatus>>>,
}

impl EventSink {
    /// Journal an event from the dedicated receive thread and synchronously
    /// return only after the commit. Webhook delivery happens after the ack.
    pub fn emit_blocking(&self, event: Event) -> Result<JournaledEvent> {
        let (ack_tx, ack_rx) = std_mpsc::sync_channel(1);
        self.tx
            .blocking_send(SinkItem::New {
                event,
                ack: Some(ack_tx),
            })
            .map_err(|_| anyhow!("event journal task is closed"))?;
        ack_rx
            .recv_timeout(Duration::from_secs(30))
            .map_err(|error| anyhow!("event journal acknowledgment failed: {error}"))?
            .map_err(anyhow::Error::msg)
    }

    /// Publish an event that was already committed transactionally with its
    /// associated state change. Dropping this notification is recoverable by
    /// replaying the journal.
    pub fn publish_committed(&self, event: JournaledEvent) {
        match self.tx.try_send(SinkItem::Committed(event)) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(SinkItem::Committed(event))) => tracing::warn!(
                event_id = event.id,
                "committed event notification dropped: emitter queue full"
            ),
            Err(mpsc::error::TrySendError::Full(SinkItem::New { .. })) => unreachable!(),
            Err(mpsc::error::TrySendError::Closed(_)) => {
                tracing::warn!("committed event notification dropped: emitter task is gone");
            }
        }
    }

    pub fn webhook_statuses(&self) -> Vec<WebhookStatus> {
        self.webhook_statuses
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

enum SinkItem {
    New {
        event: Event,
        ack: Option<std_mpsc::SyncSender<std::result::Result<JournaledEvent, String>>>,
    },
    Committed(JournaledEvent),
}

#[derive(Clone)]
struct Delivery {
    cursor: i64,
    body: Arc<Vec<u8>>,
}

struct Destination {
    id: String,
    tx: mpsc::Sender<Delivery>,
    status_index: usize,
}

/// Spawn the emitter task and return the sink workers write to.
pub fn spawn(
    client: reqwest::Client,
    webhooks: Vec<WebhookConfig>,
    store: Store,
    events: broadcast::Sender<JournaledEvent>,
) -> EventSink {
    let (tx, mut rx) = mpsc::channel::<SinkItem>(EVENT_QUEUE_CAPACITY);
    let webhook_statuses = Arc::new(RwLock::new(
        webhooks
            .iter()
            .map(|webhook| WebhookStatus {
                id: webhook.id.clone(),
                enabled: webhook.enabled,
                last_success_at: None,
                last_error: None,
            })
            .collect::<Vec<_>>(),
    ));
    let mut destinations = Vec::new();
    for (status_index, config) in webhooks.into_iter().enumerate() {
        if !config.enabled {
            continue;
        }
        let (destination_tx, destination_rx) =
            mpsc::channel::<Delivery>(DESTINATION_QUEUE_CAPACITY);
        destinations.push(Destination {
            id: config.id.clone(),
            tx: destination_tx,
            status_index,
        });
        tokio::spawn(destination_worker(
            client.clone(),
            config,
            destination_rx,
            webhook_statuses.clone(),
            status_index,
        ));
    }

    let emitter_statuses = webhook_statuses.clone();
    tokio::spawn(async move {
        let conn = match store.conn() {
            Ok(c) => c,
            Err(e) => {
                tracing::error!(error = %e, "event journal unavailable");
                return;
            }
        };

        while let Some(item) = rx.recv().await {
            let journaled = match item {
                SinkItem::Committed(event) => event,
                SinkItem::New { event, ack } => match store::record_event(&conn, &event).await {
                    Ok(journaled) => {
                        if let Some(ack) = ack {
                            let _ = ack.send(Ok(journaled.clone()));
                        }
                        journaled
                    }
                    Err(error) => {
                        if let Some(ack) = ack {
                            let _ = ack.send(Err(error.to_string()));
                        }
                        tracing::error!(%error, event = %event.event, message_id = %event.message_id, "journal event");
                        continue;
                    }
                },
            };

            tracing::info!(
                event_id = journaled.id,
                event = %journaled.event,
                message_id = %journaled.message_id,
                "event"
            );
            let _ = events.send(journaled.clone());

            let body = match serde_json::to_vec(&journaled) {
                Ok(body) => body,
                Err(error) => {
                    tracing::error!(%error, "serialize journaled event");
                    continue;
                }
            };
            let delivery = Delivery {
                cursor: journaled.id,
                body: Arc::new(body),
            };
            for destination in &destinations {
                match destination.tx.try_send(delivery.clone()) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        set_error(&emitter_statuses, destination.status_index, "queue_full");
                        tracing::warn!(
                            webhook_id = %destination.id,
                            event_cursor = journaled.id,
                            attempt = 0,
                            error_category = "queue_full",
                            "webhook delivery dropped"
                        );
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        set_error(&emitter_statuses, destination.status_index, "worker_closed");
                        tracing::warn!(
                            webhook_id = %destination.id,
                            event_cursor = journaled.id,
                            attempt = 0,
                            error_category = "worker_closed",
                            "webhook delivery dropped"
                        );
                    }
                }
            }
        }
    });

    EventSink {
        tx,
        webhook_statuses,
    }
}

async fn destination_worker(
    client: reqwest::Client,
    config: WebhookConfig,
    mut rx: mpsc::Receiver<Delivery>,
    statuses: Arc<RwLock<Vec<WebhookStatus>>>,
    status_index: usize,
) {
    while let Some(delivery) = rx.recv().await {
        deliver(
            &client,
            &config,
            delivery.cursor,
            delivery.body.as_slice(),
            &statuses,
            status_index,
        )
        .await;
    }
}

async fn deliver(
    client: &reqwest::Client,
    config: &WebhookConfig,
    cursor: i64,
    body: &[u8],
    statuses: &Arc<RwLock<Vec<WebhookStatus>>>,
    status_index: usize,
) {
    let signature = sign(&config.secret, body);
    for attempt in 1..=MAX_ATTEMPTS {
        let res = client
            .post(&config.url)
            .header(SIGNATURE_HEADER, &signature)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body.to_vec())
            .send()
            .await;

        match res {
            Ok(response) if response.status().is_success() => {
                let mut locked = statuses
                    .write()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                locked[status_index].last_success_at = Some(crate::model::now_iso());
                locked[status_index].last_error = None;
                tracing::info!(
                    webhook_id = %config.id,
                    event_cursor = cursor,
                    attempt,
                    http_status = %response.status(),
                    error_category = "none",
                    "webhook delivered"
                );
                return;
            }
            Ok(response) => {
                let category = format!("http_{}", response.status().as_u16());
                set_error(statuses, status_index, &category);
                tracing::warn!(
                    webhook_id = %config.id,
                    event_cursor = cursor,
                    attempt,
                    http_status = %response.status(),
                    error_category = %category,
                    "webhook non-2xx"
                );
            }
            Err(error) => {
                let category = request_error_category(&error);
                set_error(statuses, status_index, category);
                tracing::warn!(
                    webhook_id = %config.id,
                    event_cursor = cursor,
                    attempt,
                    error_category = category,
                    "webhook send failed"
                );
            }
        }

        if attempt < MAX_ATTEMPTS {
            tokio::time::sleep(Duration::from_millis(200 * attempt as u64)).await;
        }
    }
    tracing::warn!(
        webhook_id = %config.id,
        event_cursor = cursor,
        attempt = MAX_ATTEMPTS,
        error_category = "retries_exhausted",
        "webhook dropped after retries"
    );
}

fn set_error(statuses: &Arc<RwLock<Vec<WebhookStatus>>>, index: usize, category: &str) {
    statuses
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())[index]
        .last_error = Some(category.to_string());
}

fn request_error_category(error: &reqwest::Error) -> &'static str {
    if error.is_timeout() {
        "timeout"
    } else if error.is_connect() {
        "connect"
    } else if error.is_request() {
        "request"
    } else {
        "transport"
    }
}

fn sign(secret: &str, body: &[u8]) -> String {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(body);
    hex::encode(mac.finalize().into_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Bytes,
        extract::State,
        http::{HeaderMap, StatusCode},
        routing::post,
        Router,
    };

    fn temp_db_path() -> String {
        std::env::temp_dir()
            .join(format!("blueski-webhook-{}.db", uuid::Uuid::new_v4()))
            .to_string_lossy()
            .into_owned()
    }

    async fn capture(
        State(tx): State<mpsc::Sender<(HeaderMap, Bytes)>>,
        headers: HeaderMap,
        body: Bytes,
    ) -> StatusCode {
        let _ = tx.send((headers, body)).await;
        StatusCode::NO_CONTENT
    }

    async fn slow_capture(
        State(tx): State<mpsc::Sender<(HeaderMap, Bytes)>>,
        headers: HeaderMap,
        body: Bytes,
    ) -> StatusCode {
        tokio::time::sleep(Duration::from_millis(600)).await;
        let _ = tx.send((headers, body)).await;
        StatusCode::NO_CONTENT
    }

    async fn emit(sink: EventSink, event: Event) -> JournaledEvent {
        tokio::task::spawn_blocking(move || sink.emit_blocking(event))
            .await
            .unwrap()
            .unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn webhook_signs_the_exact_journaled_envelope() {
        let (capture_tx, mut capture_rx) = mpsc::channel(1);
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new()
            .route("/", post(capture))
            .with_state(capture_tx);
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let store = Store::open(&temp_db_path(), "bsinst_test").await.unwrap();
        let (events, _) = broadcast::channel(16);
        let sink = spawn(
            reqwest::Client::new(),
            vec![WebhookConfig {
                id: "primary".to_string(),
                url: format!("http://{address}/"),
                secret: "webhook-secret".to_string(),
                enabled: true,
            }],
            store,
            events,
        );
        let mut event = Event::new("message.received", "apple-guid".to_string());
        event.provider_message_id = Some("apple-guid".to_string());
        event.chat_id = Some("iMessage;-;chat-1".to_string());
        event.status = Some("received".to_string());

        let saved = tokio::task::spawn_blocking(move || sink.emit_blocking(event))
            .await
            .unwrap()
            .unwrap();
        let (headers, body) = tokio::time::timeout(Duration::from_secs(5), capture_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let delivered: JournaledEvent = serde_json::from_slice(&body).unwrap();

        assert_eq!(delivered.id, saved.id);
        assert_eq!(delivered.installation_id, "bsinst_test");
        assert_eq!(delivered.created_at, saved.created_at);
        assert_eq!(delivered.chat_id, saved.chat_id);
        assert_eq!(delivered.provider_message_id, saved.provider_message_id);
        assert_eq!(
            headers.get(SIGNATURE_HEADER).unwrap().to_str().unwrap(),
            sign("webhook-secret", &body)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn destinations_are_isolated_signed_and_ordered() {
        let (fast_tx, mut fast_rx) = mpsc::channel(4);
        let fast_listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let fast_address = fast_listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                fast_listener,
                Router::new().route("/", post(capture)).with_state(fast_tx),
            )
            .await
            .unwrap();
        });

        let (slow_tx, mut slow_rx) = mpsc::channel(4);
        let slow_listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let slow_address = slow_listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                slow_listener,
                Router::new()
                    .route("/", post(slow_capture))
                    .with_state(slow_tx),
            )
            .await
            .unwrap();
        });

        let store = Store::open(&temp_db_path(), "bsinst_fanout").await.unwrap();
        let (events, _) = broadcast::channel(16);
        let sink = spawn(
            reqwest::Client::builder()
                .timeout(Duration::from_secs(2))
                .build()
                .unwrap(),
            vec![
                WebhookConfig {
                    id: "fast".to_string(),
                    url: format!("http://{fast_address}/"),
                    secret: "fast-secret".to_string(),
                    enabled: true,
                },
                WebhookConfig {
                    id: "slow".to_string(),
                    url: format!("http://{slow_address}/"),
                    secret: "slow-secret".to_string(),
                    enabled: true,
                },
            ],
            store,
            events,
        );

        let started = tokio::time::Instant::now();
        let first = emit(
            sink.clone(),
            Event::new("message.received", "first".to_string()),
        )
        .await;
        let second = emit(
            sink.clone(),
            Event::new("message.received", "second".to_string()),
        )
        .await;

        let fast_first = tokio::time::timeout(Duration::from_millis(400), fast_rx.recv())
            .await
            .expect("slow destination delayed fast destination")
            .unwrap();
        assert!(started.elapsed() < Duration::from_millis(500));
        let fast_second = tokio::time::timeout(Duration::from_secs(2), fast_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let slow_first = tokio::time::timeout(Duration::from_secs(3), slow_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let slow_second = tokio::time::timeout(Duration::from_secs(3), slow_rx.recv())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(fast_first.1, slow_first.1);
        assert_eq!(fast_second.1, slow_second.1);
        let first_body: JournaledEvent = serde_json::from_slice(&fast_first.1).unwrap();
        let second_body: JournaledEvent = serde_json::from_slice(&fast_second.1).unwrap();
        assert_eq!(first_body.id, first.id);
        assert_eq!(second_body.id, second.id);
        assert!(first_body.id < second_body.id);

        let fast_signature = fast_first.0[SIGNATURE_HEADER].to_str().unwrap();
        let slow_signature = slow_first.0[SIGNATURE_HEADER].to_str().unwrap();
        assert_eq!(fast_signature, sign("fast-secret", &fast_first.1));
        assert_eq!(slow_signature, sign("slow-secret", &slow_first.1));
        assert_ne!(fast_signature, sign("slow-secret", &fast_first.1));
        assert_ne!(slow_signature, sign("fast-secret", &slow_first.1));

        let statuses = sink.webhook_statuses();
        assert_eq!(statuses.len(), 2);
        assert!(statuses
            .iter()
            .all(|status| status.last_success_at.is_some()));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn timing_out_destination_does_not_delay_another() {
        let (fast_tx, mut fast_rx) = mpsc::channel(1);
        let fast_listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let fast_address = fast_listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                fast_listener,
                Router::new().route("/", post(capture)).with_state(fast_tx),
            )
            .await
            .unwrap();
        });

        let (timeout_tx, _timeout_rx) = mpsc::channel(1);
        let timeout_listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let timeout_address = timeout_listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                timeout_listener,
                Router::new()
                    .route("/", post(slow_capture))
                    .with_state(timeout_tx),
            )
            .await
            .unwrap();
        });

        let store = Store::open(&temp_db_path(), "bsinst_timeout")
            .await
            .unwrap();
        let (events, _) = broadcast::channel(16);
        let sink = spawn(
            reqwest::Client::builder()
                .timeout(Duration::from_millis(100))
                .build()
                .unwrap(),
            vec![
                WebhookConfig {
                    id: "fast".to_string(),
                    url: format!("http://{fast_address}/"),
                    secret: "fast-secret".to_string(),
                    enabled: true,
                },
                WebhookConfig {
                    id: "timeout".to_string(),
                    url: format!("http://{timeout_address}/"),
                    secret: "timeout-secret".to_string(),
                    enabled: true,
                },
            ],
            store,
            events,
        );

        let started = tokio::time::Instant::now();
        emit(
            sink,
            Event::new("message.received", "timeout-isolation".to_string()),
        )
        .await;
        tokio::time::timeout(Duration::from_millis(300), fast_rx.recv())
            .await
            .expect("timing out destination delayed fast destination")
            .unwrap();
        assert!(started.elapsed() < Duration::from_millis(400));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn disabled_destination_receives_nothing() {
        let (capture_tx, mut capture_rx) = mpsc::channel(1);
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/", post(capture))
                    .with_state(capture_tx),
            )
            .await
            .unwrap();
        });

        let store = Store::open(&temp_db_path(), "bsinst_disabled")
            .await
            .unwrap();
        let (events, _) = broadcast::channel(16);
        let sink = spawn(
            reqwest::Client::new(),
            vec![WebhookConfig {
                id: "disabled".to_string(),
                url: format!("http://{address}/"),
                secret: "disabled-secret".to_string(),
                enabled: false,
            }],
            store,
            events,
        );
        emit(
            sink.clone(),
            Event::new("message.received", "ignored".to_string()),
        )
        .await;

        assert!(
            tokio::time::timeout(Duration::from_millis(250), capture_rx.recv())
                .await
                .is_err()
        );
        assert_eq!(
            sink.webhook_statuses(),
            vec![WebhookStatus {
                id: "disabled".to_string(),
                enabled: false,
                last_success_at: None,
                last_error: None,
            }]
        );
    }
}
