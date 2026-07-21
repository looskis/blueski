//! Event fan-out. Workers push `Event`s into an in-process channel; a single
//! background task journals each event, publishes it to local subscribers, and
//! sends the existing signed webhook when configured.

use crate::model::Event;
use crate::store::{self, JournaledEvent, Store};
use anyhow::{anyhow, Result};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::sync::mpsc as std_mpsc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};

const MAX_ATTEMPTS: u32 = 3;
const EVENT_QUEUE_CAPACITY: usize = 1024;
const SIGNATURE_HEADER: &str = "X-Blueski-Signature";

/// Clonable handle for journal acknowledgments and post-commit notifications.
#[derive(Clone)]
pub struct EventSink {
    tx: mpsc::Sender<SinkItem>,
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
}

enum SinkItem {
    New {
        event: Event,
        ack: Option<std_mpsc::SyncSender<std::result::Result<JournaledEvent, String>>>,
    },
    Committed(JournaledEvent),
}

/// Spawn the emitter task and return the sink workers write to.
pub fn spawn(
    client: reqwest::Client,
    webhook_url: Option<String>,
    secret: String,
    store: Store,
    events: broadcast::Sender<JournaledEvent>,
) -> EventSink {
    let (tx, mut rx) = mpsc::channel::<SinkItem>(EVENT_QUEUE_CAPACITY);

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

            let Some(url) = webhook_url.as_deref() else {
                continue; // no sink configured: log-only
            };

            let body = match serde_json::to_vec(&journaled) {
                Ok(body) => body,
                Err(error) => {
                    tracing::error!(%error, "serialize journaled event");
                    continue;
                }
            };
            let signature = sign(&secret, &body);
            let client = client.clone();
            let url = url.to_string();
            tokio::spawn(async move {
                deliver(&client, &url, &signature, body).await;
            });
        }
    });

    EventSink { tx }
}

async fn deliver(client: &reqwest::Client, url: &str, signature: &str, body: Vec<u8>) {
    for attempt in 1..=MAX_ATTEMPTS {
        let res = client
            .post(url)
            .header(SIGNATURE_HEADER, signature)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body.clone())
            .send()
            .await;

        match res {
            Ok(r) if r.status().is_success() => return,
            Ok(r) => tracing::warn!(status = %r.status(), attempt, "webhook non-2xx"),
            Err(e) => tracing::warn!(error = %e, attempt, "webhook send failed"),
        }

        if attempt < MAX_ATTEMPTS {
            tokio::time::sleep(Duration::from_millis(200 * attempt as u64)).await;
        }
    }
    tracing::warn!(url, "webhook dropped after retries");
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

        let store = Store::open(&temp_db_path()).await.unwrap();
        let (events, _) = broadcast::channel(16);
        let sink = spawn(
            reqwest::Client::new(),
            Some(format!("http://{address}/")),
            "webhook-secret".to_string(),
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
        assert_eq!(delivered.created_at, saved.created_at);
        assert_eq!(delivered.chat_id, saved.chat_id);
        assert_eq!(delivered.provider_message_id, saved.provider_message_id);
        assert_eq!(
            headers.get(SIGNATURE_HEADER).unwrap().to_str().unwrap(),
            sign("webhook-secret", &body)
        );
    }
}
