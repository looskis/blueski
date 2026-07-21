//! Correlation state, backed by Turso (libSQL). This is the binding between our
//! `message_id` (the UUID we return at 202) and the `chat.db` `guid` that
//! Messages.app mints when it actually sends — the two never meet otherwise.
//!
//! Local file today (`~/.config/blueski/state.db`); the same libSQL
//! handle can become an embedded replica that syncs to a Turso cloud db per
//! node, which is the natural substrate for fleet mode.
//!
//! The database is also the durable outbound queue. HTTP acceptance and the
//! `message.queued` journal entry commit together before a 202 is returned.
//! Provider binding/status changes and their journal entries use the same rule.

use crate::model::{now_iso, Event, SendJob, SendTarget};
use crate::webhook::EventSink;
use anyhow::Result;
use libsql::{params, Builder, Connection, Database};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;
use std::sync::Arc;
use tokio::sync::mpsc::{self, UnboundedSender};

/// A binding resolved from a `chat.db` guid back to our identifiers. `handle`
/// is the recipient we sent to (from the pending record) — authoritative, since
/// the chat.db row's own handle is unreliable for outbound messages.
#[derive(Debug, Clone)]
pub struct Binding {
    pub message_id: String,
    pub provider_message_id: String,
    pub client_ref: Option<String>,
    pub handle: String,
    pub target_kind: String,
    pub chat_id: Option<String>,
    pub protocol: String,
    pub last_status: String,
}

#[derive(Debug)]
pub struct AcceptedSend {
    pub message_id: String,
    pub status: String,
    pub is_new: bool,
    pub journaled: Option<JournaledEvent>,
    pub idempotent: bool,
}

#[derive(Debug)]
pub struct IdempotencyConflict {
    pub client_ref: String,
}

impl fmt::Display for IdempotencyConflict {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "client_ref {} was already used for a different request",
            self.client_ref
        )
    }
}

impl std::error::Error for IdempotencyConflict {}

#[derive(Debug, Clone)]
pub struct DurableSend {
    pub job: SendJob,
    pub pre_rowid: i64,
    pub dispatch_state: String,
}

/// A persisted event with its local durable cursor.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct JournaledEvent {
    pub installation_id: String,
    pub id: i64,
    pub event: String,
    pub message_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_message_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub handle: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chat_id: Option<String>,
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
    pub async fn open(path: &str, installation_id: &str) -> Result<Store> {
        let db = Builder::new_local(path).build().await?;
        let conn = db.connect()?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS outbound (
                 message_id  TEXT PRIMARY KEY,
                 client_ref  TEXT,
                 handle      TEXT NOT NULL,
                 target_kind TEXT,
                 target_value TEXT,
                 text        TEXT,
                 protocol    TEXT,
                 pre_rowid   INTEGER NOT NULL,
                 guid        TEXT,
                 chat_id     TEXT,
                 rowid       INTEGER,
                 last_status TEXT NOT NULL DEFAULT 'queued',
                 dispatch_state TEXT NOT NULL DEFAULT 'legacy',
                 created_at  TEXT
             );
             CREATE INDEX IF NOT EXISTS idx_outbound_guid ON outbound(guid);
             CREATE INDEX IF NOT EXISTS idx_outbound_claim
                 ON outbound(handle, guid, pre_rowid);
             CREATE TABLE IF NOT EXISTS events (
                 id           INTEGER PRIMARY KEY AUTOINCREMENT,
                 installation_id TEXT,
                 event        TEXT NOT NULL,
                 message_id   TEXT NOT NULL,
                 provider_message_id TEXT,
                 client_ref   TEXT,
                 handle       TEXT,
                 chat_id      TEXT,
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
        ensure_column(&conn, "outbound", "target_kind", "TEXT").await?;
        ensure_column(&conn, "outbound", "target_value", "TEXT").await?;
        ensure_column(&conn, "outbound", "text", "TEXT").await?;
        ensure_column(&conn, "outbound", "chat_id", "TEXT").await?;
        ensure_column(
            &conn,
            "outbound",
            "dispatch_state",
            "TEXT NOT NULL DEFAULT 'legacy'",
        )
        .await?;
        ensure_column(&conn, "events", "provider_message_id", "TEXT").await?;
        ensure_column(&conn, "events", "chat_id", "TEXT").await?;
        ensure_column(&conn, "events", "installation_id", "TEXT").await?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS outbound_idempotency (
                 client_ref TEXT PRIMARY KEY,
                 message_id TEXT NOT NULL UNIQUE,
                 request_fingerprint TEXT
             );
             INSERT OR IGNORE INTO outbound_idempotency (client_ref, message_id)
             SELECT client_ref, MIN(message_id)
             FROM outbound
             WHERE client_ref IS NOT NULL
             GROUP BY client_ref;",
        )
        .await?;
        ensure_column(&conn, "outbound_idempotency", "request_fingerprint", "TEXT").await?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS metadata (
                 key TEXT PRIMARY KEY,
                 value TEXT NOT NULL
             );",
        )
        .await?;
        let mut rows = conn
            .query(
                "SELECT value FROM metadata WHERE key = 'installation_id'",
                (),
            )
            .await?;
        if let Some(row) = rows.next().await? {
            let stored: String = row.get(0)?;
            if stored != installation_id {
                anyhow::bail!(
                    "state database installation_id {stored} does not match config {installation_id}"
                );
            }
        } else {
            drop(rows);
            conn.execute(
                "INSERT INTO metadata (key, value) VALUES ('installation_id', ?1)",
                params![installation_id],
            )
            .await?;
        }
        conn.execute(
            "UPDATE events SET installation_id = ?1 WHERE installation_id IS NULL",
            params![installation_id],
        )
        .await?;
        Ok(Store { db: Arc::new(db) })
    }

    /// A fresh connection for one owner (one per thread/task — don't share).
    pub fn conn(&self) -> Result<Connection> {
        Ok(self.db.connect()?)
    }
}

async fn ensure_column(
    conn: &Connection,
    table: &str,
    column: &str,
    definition: &str,
) -> Result<()> {
    let mut rows = conn
        .query(&format!("PRAGMA table_info({table})"), ())
        .await?;
    while let Some(row) = rows.next().await? {
        if row.get::<String>(1)? == column {
            return Ok(());
        }
    }
    drop(rows);
    conn.execute(
        &format!("ALTER TABLE {table} ADD COLUMN {column} {definition}"),
        (),
    )
    .await?;
    Ok(())
}

/// Persist an emitted event and return the journaled shape consumers read.
pub async fn record_event(conn: &Connection, event: &Event) -> Result<JournaledEvent> {
    let installation_id = installation_id(conn).await?;
    let payload_json = serde_json::to_string(event)?;
    let created_at = now_iso();
    conn.execute(
        "INSERT INTO events
           (installation_id, event, message_id, provider_message_id, client_ref, handle, chat_id,
            text, protocol, status, reason, timestamp, payload_json, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
        params![
            installation_id.clone(),
            event.event.clone(),
            event.message_id.clone(),
            event.provider_message_id.clone(),
            event.client_ref.clone(),
            event.handle.clone(),
            event.chat_id.clone(),
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
        installation_id,
        id,
        event: event.event.clone(),
        message_id: event.message_id.clone(),
        provider_message_id: event.provider_message_id.clone(),
        client_ref: event.client_ref.clone(),
        handle: event.handle.clone(),
        chat_id: event.chat_id.clone(),
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
            "SELECT installation_id, id, event, message_id, provider_message_id, client_ref, handle,
                    chat_id, text, protocol, status, reason, timestamp, created_at
             FROM events WHERE id > ?1 ORDER BY id ASC LIMIT ?2"
        }
        None => {
            "SELECT installation_id, id, event, message_id, provider_message_id, client_ref, handle,
                    chat_id, text, protocol, status, reason, timestamp, created_at
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
            installation_id: row.get(0)?,
            id: row.get(1)?,
            event: row.get(2)?,
            message_id: row.get(3)?,
            provider_message_id: row.get(4)?,
            client_ref: row.get(5)?,
            handle: row.get(6)?,
            chat_id: row.get(7)?,
            text: row.get(8)?,
            protocol: row.get(9)?,
            status: row.get(10)?,
            reason: row.get(11)?,
            timestamp: row.get(12)?,
            created_at: row.get(13)?,
        });
    }
    Ok(events)
}

/// Return the latest journal entry for each message. This gives agents a
/// compact message collection while `GET /messages/:id` retains the complete
/// lifecycle for a single message.
pub async fn list_messages(
    conn: &Connection,
    since: i64,
    limit: Option<u64>,
) -> Result<Vec<JournaledEvent>> {
    let limit = limit.unwrap_or(100).min(1_000) as i64;
    let mut rows = conn
        .query(
            "SELECT e.installation_id, e.id, e.event, e.message_id, e.provider_message_id, e.client_ref,
                    e.handle, e.chat_id, e.text, e.protocol, e.status, e.reason,
                    e.timestamp, e.created_at
             FROM events e
             JOIN (SELECT message_id, MAX(id) AS id FROM events GROUP BY message_id) latest
               ON latest.id = e.id
             WHERE e.id > ?1
             ORDER BY e.id ASC LIMIT ?2",
            params![since, limit],
        )
        .await?;
    read_journaled_events(&mut rows).await
}

pub async fn get_message_events(
    conn: &Connection,
    message_id: &str,
) -> Result<Vec<JournaledEvent>> {
    let mut rows = conn
        .query(
            "SELECT installation_id, id, event, message_id, provider_message_id, client_ref, handle,
                    chat_id, text, protocol, status, reason, timestamp, created_at
             FROM events WHERE message_id = ?1 ORDER BY id ASC",
            params![message_id],
        )
        .await?;
    read_journaled_events(&mut rows).await
}

async fn read_journaled_events(rows: &mut libsql::Rows) -> Result<Vec<JournaledEvent>> {
    let mut events = Vec::new();
    while let Some(row) = rows.next().await? {
        events.push(JournaledEvent {
            installation_id: row.get(0)?,
            id: row.get(1)?,
            event: row.get(2)?,
            message_id: row.get(3)?,
            provider_message_id: row.get(4)?,
            client_ref: row.get(5)?,
            handle: row.get(6)?,
            chat_id: row.get(7)?,
            text: row.get(8)?,
            protocol: row.get(9)?,
            status: row.get(10)?,
            reason: row.get(11)?,
            timestamp: row.get(12)?,
            created_at: row.get(13)?,
        });
    }
    Ok(events)
}

async fn installation_id(conn: &Connection) -> Result<String> {
    let mut rows = conn
        .query(
            "SELECT value FROM metadata WHERE key = 'installation_id'",
            (),
        )
        .await?;
    let Some(row) = rows.next().await? else {
        anyhow::bail!("installation_id metadata is missing");
    };
    Ok(row.get(0)?)
}

async fn last_insert_rowid(conn: &Connection) -> Result<i64> {
    let mut rows = conn.query("SELECT last_insert_rowid()", ()).await?;
    let Some(row) = rows.next().await? else {
        anyhow::bail!("last_insert_rowid returned no row");
    };
    Ok(row.get(0)?)
}

/// Idempotently persist a complete send command and its queued event before the
/// HTTP handler returns 202. The idempotency mapping is separate so legacy rows
/// with duplicate correlation-only `client_ref`s do not make migration fail.
pub async fn accept_and_record_queued(conn: &Connection, job: &SendJob) -> Result<AcceptedSend> {
    let tx = conn.transaction().await?;

    if let Some(client_ref) = job.client_ref.as_deref() {
        let fingerprint = request_fingerprint(job);
        let mut rows = tx
            .query(
                "SELECT i.message_id, i.request_fingerprint,
                        COALESCE(o.last_status, 'queued')
                 FROM outbound_idempotency i
                 LEFT JOIN outbound o ON o.message_id = i.message_id
                 WHERE i.client_ref = ?1",
                params![client_ref],
            )
            .await?;
        let existing = match rows.next().await? {
            Some(row) => Some((
                row.get::<String>(0)?,
                row.get::<Option<String>>(1)?,
                row.get::<String>(2)?,
            )),
            None => None,
        };
        drop(rows);
        match existing {
            Some((existing_id, Some(existing_fingerprint), existing_status)) => {
                if existing_fingerprint != fingerprint {
                    return Err(IdempotencyConflict {
                        client_ref: client_ref.to_string(),
                    }
                    .into());
                }
                tx.commit().await?;
                return Ok(AcceptedSend {
                    message_id: existing_id,
                    status: existing_status,
                    is_new: false,
                    journaled: None,
                    idempotent: true,
                });
            }
            // A null fingerprint is a pre-upgrade correlation binding. The
            // first request after this upgrade starts the idempotency namespace.
            Some((_, None, _)) => {
                tx.execute(
                    "UPDATE outbound_idempotency
                     SET message_id = ?1, request_fingerprint = ?2
                     WHERE client_ref = ?3 AND request_fingerprint IS NULL",
                    params![job.message_id.clone(), fingerprint, client_ref],
                )
                .await?;
            }
            None => {
                tx.execute(
                    "INSERT INTO outbound_idempotency
                       (client_ref, message_id, request_fingerprint)
                     VALUES (?1, ?2, ?3)",
                    params![client_ref, job.message_id.clone(), fingerprint],
                )
                .await?;
            }
        }
    }

    let created_at = now_iso();
    tx.execute(
        "INSERT INTO outbound
           (message_id, client_ref, handle, target_kind, target_value, text,
            protocol, pre_rowid, last_status, dispatch_state, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 0, 'queued', 'queued', ?8)",
        params![
            job.message_id.clone(),
            job.client_ref.clone(),
            job.target.store_key().to_string(),
            job.target.kind(),
            job.target.store_key().to_string(),
            job.text.clone(),
            job.protocol.clone(),
            created_at,
        ],
    )
    .await?;

    let journaled = insert_event_tx(&tx, &queued_event(job)).await?;
    tx.commit().await?;
    Ok(AcceptedSend {
        message_id: job.message_id.clone(),
        status: "queued".to_string(),
        is_new: true,
        journaled: Some(journaled),
        idempotent: job.client_ref.is_some(),
    })
}

fn request_fingerprint(job: &SendJob) -> String {
    let mut hasher = Sha256::new();
    for value in [
        job.target.kind().as_bytes(),
        job.target.store_key().as_bytes(),
        job.protocol.as_bytes(),
        job.text.as_bytes(),
    ] {
        hasher.update((value.len() as u64).to_be_bytes());
        hasher.update(value);
    }
    hex::encode(hasher.finalize())
}

pub async fn next_queued(conn: &Connection) -> Result<Option<DurableSend>> {
    load_one_send(
        conn,
        "SELECT message_id, target_kind, target_value, text, protocol, client_ref,
                pre_rowid, dispatch_state
         FROM outbound WHERE dispatch_state = 'queued'
         ORDER BY created_at ASC, message_id ASC LIMIT 1",
    )
    .await
}

pub async fn recoverable_sends(conn: &Connection) -> Result<Vec<DurableSend>> {
    let mut rows = conn
        .query(
            "SELECT message_id, target_kind, target_value, text, protocol, client_ref,
                    pre_rowid, dispatch_state
             FROM outbound
             WHERE dispatch_state IN ('dispatching', 'sent_unbound')
             ORDER BY created_at ASC, message_id ASC",
            (),
        )
        .await?;
    let mut sends = Vec::new();
    while let Some(row) = rows.next().await? {
        if let Some(send) = durable_send_from_row(&row)? {
            sends.push(send);
        }
    }
    Ok(sends)
}

/// Provider GUIDs whose delivery/read lifecycle still needs polling after a
/// daemon restart. The receive worker keeps the live copy in memory.
pub async fn active_provider_guids(conn: &Connection) -> Result<Vec<String>> {
    let mut rows = conn
        .query(
            "SELECT guid FROM outbound
             WHERE guid IS NOT NULL AND last_status IN ('sent', 'delivered')",
            (),
        )
        .await?;
    let mut guids = Vec::new();
    while let Some(row) = rows.next().await? {
        guids.push(row.get(0)?);
    }
    Ok(guids)
}

async fn load_one_send(conn: &Connection, sql: &str) -> Result<Option<DurableSend>> {
    let mut rows = conn.query(sql, ()).await?;
    let send = match rows.next().await? {
        Some(row) => durable_send_from_row(&row)?,
        None => None,
    };
    Ok(send)
}

fn durable_send_from_row(row: &libsql::Row) -> Result<Option<DurableSend>> {
    let kind: Option<String> = row.get(1)?;
    let value: Option<String> = row.get(2)?;
    let text: Option<String> = row.get(3)?;
    let (Some(kind), Some(value), Some(text)) = (kind, value, text) else {
        return Ok(None);
    };
    let Some(target) = SendTarget::from_parts(&kind, value) else {
        return Ok(None);
    };
    Ok(Some(DurableSend {
        job: SendJob {
            message_id: row.get(0)?,
            target,
            text,
            protocol: row
                .get::<Option<String>>(4)?
                .unwrap_or_else(|| "imessage".to_string()),
            client_ref: row.get(5)?,
        },
        pre_rowid: row.get(6)?,
        dispatch_state: row.get(7)?,
    }))
}

pub async fn mark_dispatching(conn: &Connection, message_id: &str, pre_rowid: i64) -> Result<bool> {
    Ok(conn
        .execute(
            "UPDATE outbound SET dispatch_state = 'dispatching', pre_rowid = ?1
             WHERE message_id = ?2 AND dispatch_state = 'queued'",
            params![pre_rowid, message_id],
        )
        .await?
        > 0)
}

pub async fn record_sent_acceptance(conn: &Connection, job: &SendJob) -> Result<JournaledEvent> {
    let tx = conn.transaction().await?;
    tx.execute(
        "UPDATE outbound SET dispatch_state = 'sent_unbound' WHERE message_id = ?1",
        params![job.message_id.clone()],
    )
    .await?;
    let journaled = insert_event_tx(&tx, &terminal_event("message.sent", job, None)).await?;
    tx.commit().await?;
    Ok(journaled)
}

pub async fn record_failed(
    conn: &Connection,
    job: &SendJob,
    reason: String,
) -> Result<JournaledEvent> {
    let tx = conn.transaction().await?;
    tx.execute(
        "UPDATE outbound
         SET dispatch_state = 'failed', last_status = 'failed'
         WHERE message_id = ?1",
        params![job.message_id.clone()],
    )
    .await?;
    let journaled =
        insert_event_tx(&tx, &terminal_event("message.failed", job, Some(reason))).await?;
    tx.commit().await?;
    Ok(journaled)
}

pub async fn record_unknown(
    conn: &Connection,
    job: &SendJob,
    reason: &str,
) -> Result<JournaledEvent> {
    let tx = conn.transaction().await?;
    tx.execute(
        "UPDATE outbound
         SET dispatch_state = 'unknown', last_status = 'unknown'
         WHERE message_id = ?1",
        params![job.message_id.clone()],
    )
    .await?;
    let mut event = terminal_event("message.status", job, Some(reason.to_string()));
    event.status = Some("unknown".to_string());
    let journaled = insert_event_tx(&tx, &event).await?;
    tx.commit().await?;
    Ok(journaled)
}

/// Bind the provider row and journal the correlation event atomically. Once
/// this commits, notification loss is recoverable through `/events`.
pub async fn bind_and_record_sent(
    conn: &Connection,
    message_id: &str,
    rowid: i64,
    provider_message_id: &str,
    chat_id: Option<&str>,
) -> Result<Option<JournaledEvent>> {
    let tx = conn.transaction().await?;
    let changed = tx
        .execute(
            "UPDATE outbound
             SET guid = ?1, rowid = ?2, chat_id = ?3, last_status = 'sent',
                 dispatch_state = 'bound'
             WHERE message_id = ?4 AND guid IS NULL",
            params![provider_message_id, rowid, chat_id, message_id],
        )
        .await?;
    if changed == 0 {
        tx.rollback().await?;
        return Ok(None);
    }
    let binding = lookup_binding_tx(&tx, provider_message_id)
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!("outbound binding missing immediately after successful update")
        })?;
    let event = status_event(&binding, "sent", now_iso());
    let journaled = insert_event_tx(&tx, &event).await?;
    tx.commit().await?;
    Ok(Some(journaled))
}

/// Advance a provider lifecycle state and journal the observable transition in
/// the same transaction.
pub async fn advance_status_and_record(
    conn: &Connection,
    guid: &str,
    status: &str,
    timestamp: String,
) -> Result<Option<JournaledEvent>> {
    let tx = conn.transaction().await?;
    let Some(binding) = lookup_binding_tx(&tx, guid).await? else {
        tx.rollback().await?;
        return Ok(None);
    };
    if rank(status) <= rank(&binding.last_status) {
        tx.rollback().await?;
        return Ok(None);
    }
    tx.execute(
        "UPDATE outbound SET last_status = ?1 WHERE guid = ?2",
        params![status, guid],
    )
    .await?;
    let event = status_event(&binding, status, timestamp);
    let journaled = insert_event_tx(&tx, &event).await?;
    tx.commit().await?;
    Ok(Some(journaled))
}

async fn lookup_binding_tx(tx: &libsql::Transaction, guid: &str) -> Result<Option<Binding>> {
    let mut rows = tx
        .query(
            "SELECT message_id, guid, client_ref, handle,
                    COALESCE(target_kind, 'legacy'), chat_id,
                    COALESCE(protocol, 'imessage'), last_status
             FROM outbound WHERE guid = ?1",
            params![guid],
        )
        .await?;
    let binding = match rows.next().await? {
        Some(row) => Some(Binding {
            message_id: row.get(0)?,
            provider_message_id: row.get(1)?,
            client_ref: row.get(2)?,
            handle: row.get(3)?,
            target_kind: row.get(4)?,
            chat_id: row.get(5)?,
            protocol: row.get(6)?,
            last_status: row.get(7)?,
        }),
        None => None,
    };
    Ok(binding)
}

fn queued_event(job: &SendJob) -> Event {
    let mut event = Event::new("message.queued", job.message_id.clone());
    event.client_ref = job.client_ref.clone();
    event.handle = job.target.event_handle();
    event.chat_id = job.target.event_chat_id();
    event.text = Some(job.text.clone());
    event.protocol = Some(job.protocol.clone());
    event.status = Some("queued".to_string());
    event
}

fn terminal_event(name: &str, job: &SendJob, reason: Option<String>) -> Event {
    let mut event = Event::new(name, job.message_id.clone());
    event.client_ref = job.client_ref.clone();
    event.handle = job.target.event_handle();
    event.chat_id = job.target.event_chat_id();
    event.protocol = Some(job.protocol.clone());
    event.reason = reason;
    event
}

fn status_event(binding: &Binding, status: &str, timestamp: String) -> Event {
    let mut event = Event::new("message.status", binding.message_id.clone());
    event.provider_message_id = Some(binding.provider_message_id.clone());
    event.client_ref = binding.client_ref.clone();
    event.handle = event_handle_from_binding(binding);
    event.chat_id = binding.chat_id.clone();
    event.protocol = Some(binding.protocol.clone());
    event.status = Some(status.to_string());
    event.timestamp = timestamp;
    event
}

async fn insert_event_tx(tx: &libsql::Transaction, event: &Event) -> Result<JournaledEvent> {
    let installation_id = installation_id_tx(tx).await?;
    let payload_json = serde_json::to_string(event)?;
    let created_at = now_iso();
    tx.execute(
        "INSERT INTO events
           (installation_id, event, message_id, provider_message_id, client_ref, handle, chat_id,
            text, protocol, status, reason, timestamp, payload_json, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
        params![
            installation_id.clone(),
            event.event.clone(),
            event.message_id.clone(),
            event.provider_message_id.clone(),
            event.client_ref.clone(),
            event.handle.clone(),
            event.chat_id.clone(),
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
    Ok(journaled_event(
        installation_id,
        tx.last_insert_rowid(),
        event,
        created_at,
    ))
}

async fn installation_id_tx(tx: &libsql::Transaction) -> Result<String> {
    let mut rows = tx
        .query(
            "SELECT value FROM metadata WHERE key = 'installation_id'",
            (),
        )
        .await?;
    let Some(row) = rows.next().await? else {
        anyhow::bail!("installation_id metadata is missing");
    };
    Ok(row.get(0)?)
}

fn journaled_event(
    installation_id: String,
    id: i64,
    event: &Event,
    created_at: String,
) -> JournaledEvent {
    JournaledEvent {
        installation_id,
        id,
        event: event.event.clone(),
        message_id: event.message_id.clone(),
        provider_message_id: event.provider_message_id.clone(),
        client_ref: event.client_ref.clone(),
        handle: event.handle.clone(),
        chat_id: event.chat_id.clone(),
        text: event.text.clone(),
        protocol: event.protocol.clone(),
        status: event.status.clone(),
        reason: event.reason.clone(),
        timestamp: event.timestamp.clone(),
        created_at,
    }
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
                    match advance_status_and_record(&conn, &guid, &status, ts).await {
                        Ok(Some(journaled)) => sink.publish_committed(journaled),
                        Ok(None) => {} // unbound or no advance
                        Err(e) => tracing::warn!(error = %e, "advance status failed"),
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
        "unknown" => 4,
        _ => -1,
    }
}

fn event_handle_from_binding(binding: &Binding) -> Option<String> {
    if binding.target_kind == "chat" || binding.handle.starts_with("any;") {
        None
    } else {
        Some(binding.handle.clone())
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

    fn send_job(message_id: &str, client_ref: Option<&str>, target: SendTarget) -> SendJob {
        SendJob {
            message_id: message_id.to_string(),
            target,
            text: "hello from durable queue".to_string(),
            protocol: "sms".to_string(),
            client_ref: client_ref.map(str::to_string),
        }
    }

    #[test]
    fn chat_binding_key_is_not_reported_as_handle() {
        let binding = |handle: &str, target_kind: &str| Binding {
            message_id: "local".to_string(),
            provider_message_id: "provider".to_string(),
            client_ref: None,
            handle: handle.to_string(),
            target_kind: target_kind.to_string(),
            chat_id: None,
            protocol: "imessage".to_string(),
            last_status: "sent".to_string(),
        };
        assert_eq!(
            event_handle_from_binding(&binding("any;-;+15550000001", "legacy")),
            None
        );
        assert_eq!(
            event_handle_from_binding(&binding("iMessage;-;chat-1", "chat")),
            None
        );
        assert_eq!(
            event_handle_from_binding(&binding("+15550000001", "handle")).as_deref(),
            Some("+15550000001")
        );
    }

    #[tokio::test]
    async fn records_and_lists_events_by_cursor() {
        let path = temp_db_path("events-cursor");
        let store = Store::open(&path, "bsinst_test").await.unwrap();
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
        let store = Store::open(&path, "bsinst_test").await.unwrap();
        let conn = store.conn().unwrap();

        let first = record_event(&conn, &event("m1", "one")).await.unwrap();
        let second = record_event(&conn, &event("m2", "two")).await.unwrap();
        let _third = record_event(&conn, &event("m3", "three")).await.unwrap();

        let events = list_events_since(&conn, 0, Some(2)).await.unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].id, first.id);
        assert_eq!(events[1].id, second.id);
    }

    #[tokio::test]
    async fn acceptance_is_durable_and_idempotent_by_client_ref() {
        let path = temp_db_path("durable-acceptance");
        let store = Store::open(&path, "bsinst_test").await.unwrap();
        let conn = store.conn().unwrap();
        let first_job = send_job(
            "local-1",
            Some("caller-key"),
            SendTarget::Handle {
                to: "+15550000001".to_string(),
            },
        );
        let replay_job = send_job(
            "local-2",
            Some("caller-key"),
            SendTarget::Handle {
                to: "+15550000001".to_string(),
            },
        );

        let first = accept_and_record_queued(&conn, &first_job).await.unwrap();
        let replay = accept_and_record_queued(&conn, &replay_job).await.unwrap();

        assert!(first.is_new);
        assert!(first.idempotent);
        assert_eq!(first.message_id, "local-1");
        assert!(!replay.is_new);
        assert!(replay.idempotent);
        assert_eq!(replay.message_id, "local-1");
        let queued = next_queued(&conn).await.unwrap().unwrap();
        assert_eq!(queued.job, first_job);
        let events = list_events_since(&conn, 0, None).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event, "message.queued");
        assert_eq!(events[0].status.as_deref(), Some("queued"));
        assert_eq!(events[0].installation_id, "bsinst_test");
    }

    #[tokio::test]
    async fn reused_client_ref_with_a_different_fingerprint_conflicts() {
        let path = temp_db_path("idempotency-conflict");
        let store = Store::open(&path, "bsinst_test").await.unwrap();
        let conn = store.conn().unwrap();
        let first = send_job(
            "local-1",
            Some("caller-key"),
            SendTarget::Handle {
                to: "+15550000001".to_string(),
            },
        );
        let mut changed = send_job(
            "local-2",
            Some("caller-key"),
            SendTarget::Handle {
                to: "+15550000001".to_string(),
            },
        );
        changed.text = "different body".to_string();

        accept_and_record_queued(&conn, &first).await.unwrap();
        let error = accept_and_record_queued(&conn, &changed).await.unwrap_err();
        assert!(error.downcast_ref::<IdempotencyConflict>().is_some());
        assert_eq!(list_events_since(&conn, 0, None).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn missing_client_ref_is_explicitly_non_idempotent() {
        let path = temp_db_path("non-idempotent");
        let store = Store::open(&path, "bsinst_test").await.unwrap();
        let conn = store.conn().unwrap();
        let first = send_job(
            "local-1",
            None,
            SendTarget::Handle {
                to: "+15550000001".to_string(),
            },
        );
        let second = send_job(
            "local-2",
            None,
            SendTarget::Handle {
                to: "+15550000001".to_string(),
            },
        );

        let first = accept_and_record_queued(&conn, &first).await.unwrap();
        let second = accept_and_record_queued(&conn, &second).await.unwrap();
        assert!(!first.idempotent);
        assert!(!second.idempotent);
        assert_ne!(first.message_id, second.message_id);
    }

    #[tokio::test]
    async fn provider_binding_and_status_are_journaled_atomically() {
        let path = temp_db_path("atomic-binding");
        let store = Store::open(&path, "bsinst_test").await.unwrap();
        let conn = store.conn().unwrap();
        let job = send_job(
            "local-1",
            Some("outbox-1"),
            SendTarget::Chat {
                chat_id: "SMS;-;chat-1".to_string(),
            },
        );
        accept_and_record_queued(&conn, &job).await.unwrap();
        assert!(mark_dispatching(&conn, &job.message_id, 40).await.unwrap());
        record_sent_acceptance(&conn, &job).await.unwrap();

        let sent = bind_and_record_sent(
            &conn,
            &job.message_id,
            41,
            "apple-guid-1",
            Some("SMS;-;chat-1"),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(sent.event, "message.status");
        assert_eq!(sent.status.as_deref(), Some("sent"));
        assert_eq!(sent.provider_message_id.as_deref(), Some("apple-guid-1"));
        assert_eq!(sent.chat_id.as_deref(), Some("SMS;-;chat-1"));
        assert_eq!(sent.protocol.as_deref(), Some("sms"));
        assert_eq!(sent.client_ref.as_deref(), Some("outbox-1"));
        assert_eq!(
            active_provider_guids(&conn).await.unwrap(),
            vec!["apple-guid-1"]
        );

        let delivered = advance_status_and_record(
            &conn,
            "apple-guid-1",
            "delivered",
            "2026-07-20T12:00:00Z".to_string(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(delivered.status.as_deref(), Some("delivered"));
        assert_eq!(delivered.provider_message_id, sent.provider_message_id);
        assert_eq!(delivered.chat_id, sent.chat_id);
        assert_eq!(delivered.protocol, sent.protocol);
        assert!(advance_status_and_record(
            &conn,
            "apple-guid-1",
            "sent",
            "2026-07-20T12:00:01Z".to_string(),
        )
        .await
        .unwrap()
        .is_none());
        advance_status_and_record(
            &conn,
            "apple-guid-1",
            "read",
            "2026-07-20T12:00:02Z".to_string(),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(active_provider_guids(&conn).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn restart_queries_restore_each_durable_dispatch_state() {
        let path = temp_db_path("restart-states");
        let store = Store::open(&path, "bsinst_test").await.unwrap();
        let conn = store.conn().unwrap();
        let queued = send_job("queued", None, SendTarget::Handle { to: "q".into() });
        let dispatching = send_job("dispatching", None, SendTarget::Handle { to: "d".into() });
        let sent_unbound = send_job("sent-unbound", None, SendTarget::Handle { to: "s".into() });
        let bound = send_job("bound", None, SendTarget::Handle { to: "b".into() });
        let unknown = send_job("unknown", None, SendTarget::Handle { to: "u".into() });

        for job in [&queued, &dispatching, &sent_unbound, &bound, &unknown] {
            accept_and_record_queued(&conn, job).await.unwrap();
        }
        mark_dispatching(&conn, &dispatching.message_id, 10)
            .await
            .unwrap();
        mark_dispatching(&conn, &sent_unbound.message_id, 20)
            .await
            .unwrap();
        record_sent_acceptance(&conn, &sent_unbound).await.unwrap();
        mark_dispatching(&conn, &bound.message_id, 30)
            .await
            .unwrap();
        record_sent_acceptance(&conn, &bound).await.unwrap();
        bind_and_record_sent(
            &conn,
            &bound.message_id,
            31,
            "provider-bound",
            Some("chat-bound"),
        )
        .await
        .unwrap();
        mark_dispatching(&conn, &unknown.message_id, 40)
            .await
            .unwrap();
        record_unknown(&conn, &unknown, "restart uncertainty")
            .await
            .unwrap();

        assert_eq!(
            next_queued(&conn).await.unwrap().unwrap().job.message_id,
            "queued"
        );
        let recoverable = recoverable_sends(&conn).await.unwrap();
        assert_eq!(
            recoverable
                .iter()
                .map(|send| (send.job.message_id.as_str(), send.dispatch_state.as_str()))
                .collect::<Vec<_>>(),
            vec![
                ("dispatching", "dispatching"),
                ("sent-unbound", "sent_unbound")
            ]
        );
        assert_eq!(
            active_provider_guids(&conn).await.unwrap(),
            vec!["provider-bound"]
        );
        assert!(!recoverable
            .iter()
            .any(|send| matches!(send.job.message_id.as_str(), "bound" | "unknown")));
    }

    #[tokio::test]
    async fn migrates_legacy_tables_without_losing_events() {
        let path = temp_db_path("legacy-migration");
        let db = Builder::new_local(&path).build().await.unwrap();
        let conn = db.connect().unwrap();
        conn.execute_batch(
            "CREATE TABLE outbound (
                 message_id TEXT PRIMARY KEY, client_ref TEXT, handle TEXT NOT NULL,
                 protocol TEXT, pre_rowid INTEGER NOT NULL, guid TEXT, rowid INTEGER,
                 last_status TEXT NOT NULL DEFAULT 'queued', created_at TEXT
             );
             CREATE TABLE events (
                 id INTEGER PRIMARY KEY AUTOINCREMENT, event TEXT NOT NULL,
                 message_id TEXT NOT NULL, client_ref TEXT, handle TEXT, text TEXT,
                 protocol TEXT, status TEXT, reason TEXT, timestamp TEXT NOT NULL,
                 payload_json TEXT NOT NULL, created_at TEXT NOT NULL
             );
             INSERT INTO events
               (event, message_id, timestamp, payload_json, created_at)
             VALUES ('message.received', 'legacy-guid', '2026-01-01T00:00:00Z',
                     '{\"event\":\"message.received\",\"message_id\":\"legacy-guid\",\"timestamp\":\"2026-01-01T00:00:00Z\"}',
                     '2026-01-01T00:00:00Z');
             INSERT INTO outbound
               (message_id, client_ref, handle, protocol, pre_rowid, last_status, created_at)
             VALUES ('legacy-local', 'correlation-only', '+15550000001', 'imessage', 0,
                     'sent', '2026-01-01T00:00:00Z');",
        )
        .await
        .unwrap();
        drop(conn);
        drop(db);

        let store = Store::open(&path, "bsinst_test").await.unwrap();
        let conn = store.conn().unwrap();
        let events = list_events_since(&conn, 0, None).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].message_id, "legacy-guid");
        assert!(events[0].provider_message_id.is_none());
        assert!(events[0].chat_id.is_none());
        assert_eq!(events[0].installation_id, "bsinst_test");

        let post_upgrade = send_job(
            "post-upgrade",
            Some("correlation-only"),
            SendTarget::Handle {
                to: "+15550000002".to_string(),
            },
        );
        let accepted = accept_and_record_queued(&conn, &post_upgrade)
            .await
            .unwrap();
        assert!(accepted.is_new);
        assert_eq!(accepted.message_id, "post-upgrade");

        let mut rows = conn.query("PRAGMA table_info(outbound)", ()).await.unwrap();
        let mut columns = Vec::new();
        while let Some(row) = rows.next().await.unwrap() {
            columns.push(row.get::<String>(1).unwrap());
        }
        assert!(columns.contains(&"target_kind".to_string()));
        assert!(columns.contains(&"dispatch_state".to_string()));
        assert!(columns.contains(&"chat_id".to_string()));
    }
}
