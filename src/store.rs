//! Correlation state, backed by Turso (libSQL). This is the binding between our
//! `message_id` (the UUID we return at 202) and the `chat.db` `guid` that
//! Messages.app mints when it actually sends — the two never meet otherwise.
//!
//! Local file today (`~/.config/blueski/state.db`); the same libSQL
//! handle can become an embedded replica that syncs to a Turso cloud db per
//! node, which is the natural substrate for fleet mode.
//!
//! Lifecycle of a row here:
//!   record_pending  → (message_id, handle, pre_rowid, status=queued, guid=NULL)
//!   bind_resolved   → bind the chat.db guid/rowid after post-send resolution
//!   advance_status  → queued → sent → delivered → read  (monotonic, deduped)

use crate::model::{now_iso, Event};
use crate::webhook::EventSink;
use anyhow::Result;
use libsql::{params, Builder, Connection, Database};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::mpsc::{self, UnboundedSender};

/// A binding resolved from a `chat.db` guid back to our identifiers. `handle`
/// is the recipient we sent to (from the pending record) — authoritative, since
/// the chat.db row's own handle is unreliable for outbound messages.
#[derive(Debug, Clone)]
pub struct Binding {
    pub message_id: String,
    pub client_ref: Option<String>,
    pub handle: String,
    pub last_status: String,
}

/// A persisted event with its local durable cursor.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct JournaledEvent {
    pub id: i64,
    pub event: String,
    pub message_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub handle: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub timestamp: String,
    pub created_at: String,
}

#[derive(Clone)]
pub struct Store {
    db: Arc<Database>,
}

impl Store {
    /// Open (creating if needed) the local libSQL db and run migrations.
    pub async fn open(path: &str) -> Result<Store> {
        let db = Builder::new_local(path).build().await?;
        let conn = db.connect()?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS outbound (
                 message_id  TEXT PRIMARY KEY,
                 client_ref  TEXT,
                 handle      TEXT NOT NULL,
                 protocol    TEXT,
                 pre_rowid   INTEGER NOT NULL,
                 guid        TEXT,
                 rowid       INTEGER,
                 last_status TEXT NOT NULL DEFAULT 'queued',
                 created_at  TEXT
             );
             CREATE INDEX IF NOT EXISTS idx_outbound_guid ON outbound(guid);
             CREATE INDEX IF NOT EXISTS idx_outbound_claim
                 ON outbound(handle, guid, pre_rowid);
             CREATE TABLE IF NOT EXISTS events (
                 id           INTEGER PRIMARY KEY AUTOINCREMENT,
                 event        TEXT NOT NULL,
                 message_id   TEXT NOT NULL,
                 client_ref   TEXT,
                 handle       TEXT,
                 text         TEXT,
                 protocol     TEXT,
                 status       TEXT,
                 reason       TEXT,
                 timestamp    TEXT NOT NULL,
                 payload_json TEXT NOT NULL,
                 created_at   TEXT NOT NULL
             );
             CREATE INDEX IF NOT EXISTS idx_events_id ON events(id);",
        )
        .await?;
        Ok(Store { db: Arc::new(db) })
    }

    /// A fresh connection for one owner (one per thread/task — don't share).
    pub fn conn(&self) -> Result<Connection> {
        Ok(self.db.connect()?)
    }
}

/// Persist an emitted event and return the journaled shape consumers read.
pub async fn record_event(conn: &Connection, event: &Event) -> Result<JournaledEvent> {
    let payload_json = serde_json::to_string(event)?;
    let created_at = now_iso();
    conn.execute(
        "INSERT INTO events
           (event, message_id, client_ref, handle, text, protocol, status, reason,
            timestamp, payload_json, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        params![
            event.event.clone(),
            event.message_id.clone(),
            event.client_ref.clone(),
            event.handle.clone(),
            event.text.clone(),
            event.protocol.clone(),
            event.status.clone(),
            event.reason.clone(),
            event.timestamp.clone(),
            payload_json,
            created_at.clone(),
        ],
    )
    .await?;

    let id = last_insert_rowid(conn).await?;
    Ok(JournaledEvent {
        id,
        event: event.event.clone(),
        message_id: event.message_id.clone(),
        client_ref: event.client_ref.clone(),
        handle: event.handle.clone(),
        text: event.text.clone(),
        protocol: event.protocol.clone(),
        status: event.status.clone(),
        reason: event.reason.clone(),
        timestamp: event.timestamp.clone(),
        created_at,
    })
}

/// Return persisted events after `since`, ordered by cursor ascending.
pub async fn list_events_since(
    conn: &Connection,
    since: i64,
    limit: Option<u64>,
) -> Result<Vec<JournaledEvent>> {
    let sql = match limit {
        Some(_) => {
            "SELECT id, event, message_id, client_ref, handle, text, protocol, status,
                    reason, timestamp, created_at
             FROM events WHERE id > ?1 ORDER BY id ASC LIMIT ?2"
        }
        None => {
            "SELECT id, event, message_id, client_ref, handle, text, protocol, status,
                    reason, timestamp, created_at
             FROM events WHERE id > ?1 ORDER BY id ASC"
        }
    };

    let mut rows = match limit {
        Some(limit) => conn.query(sql, params![since, limit as i64]).await?,
        None => conn.query(sql, params![since]).await?,
    };
    let mut events = Vec::new();
    while let Some(row) = rows.next().await? {
        events.push(JournaledEvent {
            id: row.get(0)?,
            event: row.get(1)?,
            message_id: row.get(2)?,
            client_ref: row.get(3)?,
            handle: row.get(4)?,
            text: row.get(5)?,
            protocol: row.get(6)?,
            status: row.get(7)?,
            reason: row.get(8)?,
            timestamp: row.get(9)?,
            created_at: row.get(10)?,
        });
    }
    Ok(events)
}

async fn last_insert_rowid(conn: &Connection) -> Result<i64> {
    let mut rows = conn.query("SELECT last_insert_rowid()", ()).await?;
    let Some(row) = rows.next().await? else {
        anyhow::bail!("last_insert_rowid returned no row");
    };
    Ok(row.get(0)?)
}

/// Record a send we just accepted, with the chat.db ROWID watermark captured
/// *before* the AppleScript send. The matching outbound row will have
/// `ROWID > pre_rowid`.
pub async fn record_pending(
    conn: &Connection,
    message_id: &str,
    client_ref: Option<&str>,
    handle: &str,
    protocol: &str,
    pre_rowid: i64,
    created_at: &str,
) -> Result<()> {
    conn.execute(
        "INSERT OR REPLACE INTO outbound
           (message_id, client_ref, handle, protocol, pre_rowid, last_status, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, 'queued', ?6)",
        params![message_id, client_ref, handle, protocol, pre_rowid, created_at],
    )
    .await?;
    Ok(())
}

/// Bind a post-send-resolved `chat.db` row to the exact pending message.
/// Returns `true` when this call performed the binding. It refuses to overwrite
/// an existing guid, so ambiguous or duplicate resolution cannot move a row.
pub async fn bind_resolved(
    conn: &Connection,
    message_id: &str,
    rowid: i64,
    guid: &str,
) -> Result<bool> {
    let changed = conn
        .execute(
            "UPDATE outbound SET guid = ?1, rowid = ?2
             WHERE message_id = ?3 AND guid IS NULL",
            params![guid, rowid, message_id],
        )
        .await?;
    Ok(changed > 0)
}

/// Resolve a `chat.db` guid back to our identifiers (if we sent it).
pub async fn lookup_by_guid(conn: &Connection, guid: &str) -> Result<Option<Binding>> {
    // Scope `rows` so it drops before the caller issues its next statement.
    let mut rows = conn
        .query(
            "SELECT message_id, client_ref, handle, last_status FROM outbound WHERE guid = ?1",
            params![guid],
        )
        .await?;
    let binding = match rows.next().await? {
        Some(row) => Some(Binding {
            message_id: row.get(0)?,
            client_ref: row.get::<Option<String>>(1)?,
            handle: row.get(2)?,
            last_status: row.get(3)?,
        }),
        None => None,
    };
    drop(rows);
    Ok(binding)
}

/// Advance a binding's status if the new status is "further along". Returns the
/// updated binding when it actually advanced (caller should emit a webhook), or
/// `None` for a duplicate/regression. Keeps `message.status` idempotent.
pub async fn advance_status(
    conn: &Connection,
    guid: &str,
    status: &str,
) -> Result<Option<Binding>> {
    let Some(b) = lookup_by_guid(conn, guid).await? else {
        return Ok(None);
    };
    if rank(status) <= rank(&b.last_status) {
        return Ok(None);
    }
    conn.execute(
        "UPDATE outbound SET last_status = ?1 WHERE guid = ?2",
        params![status, guid],
    )
    .await?;
    Ok(Some(Binding {
        last_status: status.to_string(),
        ..b
    }))
}

/// Messages the (sync) receive thread sends to the async correlator task. This
/// keeps *all* libSQL work on the runtime — the receive thread never touches it,
/// avoiding the block_on-from-a-foreign-thread deadlocks libSQL is prone to.
#[derive(Debug)]
pub enum CorrEvent {
    /// An outbound row's observed status — advance + emit if it moved forward.
    /// The recipient handle comes from the binding, not the (unreliable) row.
    Status {
        guid: String,
        status: String,
        ts: String,
    },
}

/// Spawn the correlator task (on the current runtime) and return the sender the
/// receive thread uses. The task owns the store connection and does all binding
/// and status work with async `.await`.
pub fn spawn_correlator(store: Store, sink: EventSink) -> UnboundedSender<CorrEvent> {
    let (tx, mut rx) = mpsc::unbounded_channel::<CorrEvent>();

    tokio::spawn(async move {
        let conn = match store.conn() {
            Ok(c) => c,
            Err(e) => {
                tracing::error!(error = %e, "correlator: cannot connect store — correlation disabled");
                return;
            }
        };

        while let Some(ev) = rx.recv().await {
            match ev {
                CorrEvent::Status { guid, status, ts } => {
                    match advance_status(&conn, &guid, &status).await {
                        Ok(Some(binding)) => {
                            // `sent` acceptance is already reported by the send
                            // worker; only surface delivery lifecycle here.
                            if status == "delivered" || status == "read" {
                                let mut ev = Event::new("message.status", binding.message_id);
                                ev.client_ref = binding.client_ref;
                                ev.handle = event_handle_from_binding(&binding.handle);
                                ev.protocol = Some("imessage".to_string());
                                ev.status = Some(status);
                                ev.timestamp = ts;
                                sink.emit(ev);
                            }
                        }
                        Ok(None) => {} // unbound or no advance
                        Err(e) => tracing::warn!(error = %e, "advance_status failed"),
                    }
                }
            }
        }
    });

    tx
}

/// Monotonic ordering of the outbound status lifecycle.
fn rank(status: &str) -> i32 {
    match status {
        "queued" => 0,
        "sent" => 1,
        "delivered" => 2,
        "read" => 3,
        "failed" => 4,
        _ => -1,
    }
}

fn event_handle_from_binding(handle: &str) -> Option<String> {
    if handle.starts_with("any;") {
        None
    } else {
        Some(handle.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_db_path(name: &str) -> String {
        std::env::temp_dir()
            .join(format!("blueski-{name}-{}.db", uuid::Uuid::new_v4()))
            .to_string_lossy()
            .into_owned()
    }

    fn event(message_id: &str, text: &str) -> Event {
        let mut ev = Event::new("message.received", message_id.to_string());
        ev.client_ref = Some("client-1".to_string());
        ev.handle = Some("+13055550123".to_string());
        ev.text = Some(text.to_string());
        ev.protocol = Some("imessage".to_string());
        ev
    }

    #[test]
    fn chat_binding_key_is_not_reported_as_handle() {
        assert_eq!(event_handle_from_binding("any;-;+15550000001"), None);
        assert_eq!(
            event_handle_from_binding("+15550000001").as_deref(),
            Some("+15550000001")
        );
    }

    #[tokio::test]
    async fn records_and_lists_events_by_cursor() {
        let path = temp_db_path("events-cursor");
        let store = Store::open(&path).await.unwrap();
        let conn = store.conn().unwrap();

        let first = record_event(&conn, &event("m1", "one")).await.unwrap();
        let second = record_event(&conn, &event("m2", "two")).await.unwrap();

        assert!(second.id > first.id);
        let events = list_events_since(&conn, first.id, None).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].id, second.id);
        assert_eq!(events[0].text.as_deref(), Some("two"));
        assert_eq!(events[0].client_ref.as_deref(), Some("client-1"));
    }

    #[tokio::test]
    async fn applies_limit_in_ascending_order() {
        let path = temp_db_path("events-limit");
        let store = Store::open(&path).await.unwrap();
        let conn = store.conn().unwrap();

        let first = record_event(&conn, &event("m1", "one")).await.unwrap();
        let second = record_event(&conn, &event("m2", "two")).await.unwrap();
        let _third = record_event(&conn, &event("m3", "three")).await.unwrap();

        let events = list_events_since(&conn, 0, Some(2)).await.unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].id, first.id);
        assert_eq!(events[1].id, second.id);
    }
}
