//! Event fan-out. Workers push `Event`s into an in-process channel; a single
//! background task journals each event, publishes it to local subscribers, and
//! sends the existing signed webhook when configured.

use crate::model::Event;
use crate::store::{self, JournaledEvent, Store};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};

const MAX_ATTEMPTS: u32 = 3;
const EVENT_QUEUE_CAPACITY: usize = 1024;
const SIGNATURE_HEADER: &str = "X-Blueski-Signature";

/// Clonable handle the workers use to emit events. `emit` is sync and works
/// from any thread (including the non-async receive worker).
#[derive(Clone)]
pub struct EventSink {
    tx: mpsc::Sender<Event>,
}

impl EventSink {
    pub fn emit(&self, event: Event) {
        match self.tx.try_send(event) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(event)) => {
                tracing::warn!(event = %event.event, message_id = %event.message_id, "event dropped: emitter queue full");
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                tracing::warn!("event dropped: emitter task is gone");
            }
        }
    }
}

/// Spawn the emitter task and return the sink workers write to.
pub fn spawn(
    client: reqwest::Client,
    webhook_url: Option<String>,
    secret: String,
    store: Store,
    events: broadcast::Sender<JournaledEvent>,
) -> EventSink {
    let (tx, mut rx) = mpsc::channel::<Event>(EVENT_QUEUE_CAPACITY);

    tokio::spawn(async move {
        let conn = match store.conn() {
            Ok(c) => c,
            Err(e) => {
                tracing::error!(error = %e, "event journal unavailable");
                return;
            }
        };

        while let Some(event) = rx.recv().await {
            let body = match serde_json::to_vec(&event) {
                Ok(b) => b,
                Err(e) => {
                    tracing::error!(error = %e, "serialize event");
                    continue;
                }
            };
            let journaled = match store::record_event(&conn, &event).await {
                Ok(ev) => ev,
                Err(e) => {
                    tracing::error!(error = %e, event = %event.event, message_id = %event.message_id, "journal event");
                    continue;
                }
            };

            tracing::info!(
                event_id = journaled.id,
                event = %event.event,
                message_id = %event.message_id,
                "event"
            );
            let _ = events.send(journaled);

            let Some(url) = webhook_url.as_deref() else {
                continue; // no sink configured: log-only
            };

            let signature = sign(&secret, &body);
            deliver(&client, url, &signature, body).await;
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
