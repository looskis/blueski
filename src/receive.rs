//! Receive worker. Watches `chat.db-wal` via kqueue and, on each write, reads
//! new rows from `chat.db` (rusqlite, sync) and emits webhooks.
//! Runs on a dedicated thread; it never touches libSQL directly — correlation
//! work is forwarded to the async correlator task over a channel, which keeps
//! all libSQL on the runtime (block_on from this thread deadlocks libSQL).
//!
//! Two passes per WAL event:
//!   1. `scan` — new rows (`ROWID > last_seen`): inbound → `message.received`;
//!      outbound → forward Status to the correlator, and track the guid.
//!   2. `recheck_active` — re-read the tracked outbound guids' status straight
//!      from chat.db and forward Status. Delivered/read are in-place UPDATEs
//!      that don't move ROWID, so the watermark scan alone can't catch them.

use crate::config::{PendingInbound, State};
use crate::model::{apple_time_to_iso, now_iso, Event};
use crate::store::CorrEvent;
use crate::walwatch::{WalEvent, WalWatcher};
use crate::webhook::EventSink;
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;

/// Backstop scan cadence — kqueue should catch every WAL write, so this is a
/// rare safety net (and re-arms if the WAL didn't exist at startup).
const BACKSTOP_INTERVAL: Duration = Duration::from_secs(10);
const QUARANTINE_AFTER_ATTEMPTS: u32 = 90;

// For inbound rows `m.handle_id` reliably gives the sender's handle. Outbound
// rows have an unreliable handle (often 0), so send correlation is resolved by
// the send worker after AppleScript returns; this scanner only observes status.
const QUERY: &str = "\
SELECT m.ROWID, m.guid, h.id, m.text, m.attributedBody, m.date, \
       m.is_from_me, m.is_delivered, m.is_read, m.date_delivered, m.date_read, \
       CASE WHEN COUNT(DISTINCT c.guid) = 1 THEN MIN(c.guid) ELSE NULL END, \
       COUNT(DISTINCT c.guid), m.service, \
       COUNT(DISTINCT CASE \
         WHEN participant.id IS NOT NULL AND TRIM(participant.id) <> '' \
         THEN LOWER(TRIM(participant.id)) END), \
       MAX(CASE WHEN TRIM(COALESCE(c.room_name, '')) <> '' THEN 1 ELSE 0 END), m.destination_caller_id, m.cache_has_attachments \
FROM message m \
LEFT JOIN handle h ON m.handle_id = h.ROWID \
LEFT JOIN chat_message_join cmj ON cmj.message_id = m.ROWID \
LEFT JOIN chat c ON c.ROWID = cmj.chat_id \
LEFT JOIN chat_handle_join chj ON chj.chat_id = c.ROWID \
LEFT JOIN handle participant ON participant.ROWID = chj.handle_id \
WHERE m.ROWID > ?1 \
GROUP BY m.ROWID \
ORDER BY m.ROWID ASC";

const ROW_QUERY: &str = "\
SELECT m.ROWID, m.guid, h.id, m.text, m.attributedBody, m.date, \
       m.is_from_me, m.is_delivered, m.is_read, m.date_delivered, m.date_read, \
       CASE WHEN COUNT(DISTINCT c.guid) = 1 THEN MIN(c.guid) ELSE NULL END, \
       COUNT(DISTINCT c.guid), m.service, \
       COUNT(DISTINCT CASE \
         WHEN participant.id IS NOT NULL AND TRIM(participant.id) <> '' \
         THEN LOWER(TRIM(participant.id)) END), \
       MAX(CASE WHEN TRIM(COALESCE(c.room_name, '')) <> '' THEN 1 ELSE 0 END), m.destination_caller_id, m.cache_has_attachments \
FROM message m \
LEFT JOIN handle h ON m.handle_id = h.ROWID \
LEFT JOIN chat_message_join cmj ON cmj.message_id = m.ROWID \
LEFT JOIN chat c ON c.ROWID = cmj.chat_id \
LEFT JOIN chat_handle_join chj ON chj.chat_id = c.ROWID \
LEFT JOIN handle participant ON participant.ROWID = chj.handle_id \
WHERE m.ROWID = ?1 \
GROUP BY m.ROWID";

#[derive(Clone)]
struct Row {
    rowid: i64,
    guid: String,
    handle: Option<String>,
    destination_caller_id: Option<String>,
    text: Option<String>,
    body: Option<Vec<u8>>,
    date: i64,
    is_from_me: i64,
    is_delivered: i64,
    is_read: i64,
    date_delivered: i64,
    date_read: i64,
    chat_id: Option<String>,
    chat_guid_count: i64,
    service: Option<String>,
    participant_count: i64,
    has_room_name: bool,
    has_attachments: bool,
}

/// Everything the receive worker carries across a scan.
struct Ctx {
    conn: Connection,
    corr_tx: UnboundedSender<CorrEvent>,
    sink: EventSink,
    max_rowid: Arc<AtomicI64>,
    /// Outbound guids still awaiting a terminal status, mapped to how many times
    /// we've re-polled them. Tracked in memory so we can catch delivered/read
    /// transitions; capped so a message that never gets a read receipt is
    /// eventually dropped instead of polled forever.
    active: HashMap<String, u32>,
}

/// Stop re-polling a guid after this many rechecks (~5 min at the backstop
/// cadence) even if it never reaches `read`.
const MAX_RECHECKS: u32 = 150;

/// Start the receive worker on its own thread.
pub fn spawn(
    chatdb: PathBuf,
    wal: PathBuf,
    sink: EventSink,
    corr_tx: UnboundedSender<CorrEvent>,
    max_rowid: Arc<AtomicI64>,
    active_guids: Vec<String>,
) {
    std::thread::Builder::new()
        .name("receive".into())
        .spawn(move || worker(chatdb, wal, sink, corr_tx, max_rowid, active_guids))
        .expect("spawn receive thread");
}

fn worker(
    chatdb: PathBuf,
    wal: PathBuf,
    sink: EventSink,
    corr_tx: UnboundedSender<CorrEvent>,
    max_rowid: Arc<AtomicI64>,
    active_guids: Vec<String>,
) {
    let conn = match open_reader(&chatdb) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "cannot open chat.db (Full Disk Access? run `setup`) — receive disabled");
            return;
        }
    };

    let mut state = State::load();
    // First run: start from the current tail so we don't replay all history.
    if state.last_seen == 0 {
        state.last_seen = max_rowid_db(&conn).unwrap_or(0);
        if let Err(error) = state.save() {
            tracing::warn!(%error, "failed to persist initial receive watermark");
        }
        tracing::info!(last_seen = state.last_seen, "initialized receive watermark");
    }
    max_rowid.store(state.last_seen, Ordering::Relaxed);

    let mut ctx = Ctx {
        conn,
        corr_tx,
        sink,
        max_rowid,
        active: active_guids.into_iter().map(|guid| (guid, 0)).collect(),
    };

    // kqueue subscription on chat.db-wal — wakes us on every WAL commit.
    let mut watcher = match WalWatcher::new(wal) {
        Ok(w) => w,
        Err(e) => {
            tracing::error!(error = %e, "cannot create WAL watcher — receive disabled");
            return;
        }
    };
    tracing::info!("watching for messages (kqueue on WAL)");

    // Catch anything written between watermark init and now.
    retry_pending(&mut ctx, &mut state);
    scan(&mut ctx, &mut state);
    recheck_active(&mut ctx);

    loop {
        // kqueue wakes us within ~ms on any WAL write; the timeout is a rare
        // backstop. The range scan (ROWID > last_seen) makes every trigger
        // idempotent, so coalesced writes are harmless.
        let ev = watcher.wait(BACKSTOP_INTERVAL);
        match ev {
            WalEvent::Written => tracing::debug!("scan trigger=kqueue"),
            WalEvent::Recreated => tracing::info!("scan trigger=kqueue (WAL re-armed)"),
            WalEvent::Timeout => tracing::debug!("scan trigger=backstop"),
        }
        retry_pending(&mut ctx, &mut state);
        scan(&mut ctx, &mut state);
        recheck_active(&mut ctx);
    }
}

/// Open a connection that can see live WAL commits. A strict READ_ONLY handle
/// only sees the checkpointed main db, so inbound messages sitting in the WAL
/// are invisible. We open READ_WRITE (we have FDA) but immediately set
/// `query_only` — so we participate in WAL as a reader without ever writing,
/// honoring the "never contend, never copy" principle. Falls back to READ_ONLY.
pub fn open_reader(path: &std::path::Path) -> rusqlite::Result<Connection> {
    let conn = match Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    ) {
        Ok(c) => c,
        Err(_) => Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?,
    };
    conn.busy_timeout(Duration::from_millis(3000))?;
    let _ = conn.pragma_update(None, "query_only", true);
    Ok(conn)
}

fn max_rowid_db(conn: &Connection) -> rusqlite::Result<i64> {
    conn.query_row("SELECT COALESCE(MAX(ROWID), 0) FROM message", [], |r| {
        r.get(0)
    })
}

fn scan(ctx: &mut Ctx, state: &mut State) {
    let rows = match read_rows(&ctx.conn, state.last_seen) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, "chat.db query failed");
            return;
        }
    };

    if !rows.is_empty() {
        tracing::info!(
            count = rows.len(),
            last_seen = state.last_seen,
            "scan: new rows"
        );
    }
    for row in rows {
        if row.rowid <= state.last_seen {
            continue;
        }
        let rowid = row.rowid;
        let mut next = state.clone();
        match handle_row(ctx, &row) {
            HandleOutcome::Complete => {}
            HandleOutcome::Pending(reason) => {
                if !next
                    .pending_inbound
                    .iter()
                    .any(|pending| pending.guid == row.guid)
                {
                    next.pending_inbound.push(PendingInbound {
                        rowid,
                        guid: row.guid.clone(),
                        first_seen_at: now_iso(),
                        attempts: 0,
                        last_error: Some(reason.clone()),
                        quarantined_at: None,
                    });
                }
                tracing::warn!(
                    rowid,
                    message_guid = %row.guid,
                    %reason,
                    "captured incomplete inbound message for enrichment"
                );
            }
            HandleOutcome::Failed => {
                tracing::warn!(rowid, "scan stopped before receive watermark advanced");
                break;
            }
        }
        next.last_seen = rowid;
        if let Err(error) = next.save() {
            // The event may already be journaled. Retrying this row is an
            // intentional at-least-once duplicate, preferable to skipping it.
            tracing::warn!(%error, rowid, "failed to persist receive watermark");
            break;
        }
        *state = next;
        ctx.max_rowid.store(state.last_seen, Ordering::Relaxed);
    }
}

fn read_rows(conn: &Connection, last_seen: i64) -> rusqlite::Result<Vec<Row>> {
    let mut stmt = conn.prepare(QUERY)?;
    let rows = stmt
        .query_map([last_seen], map_row)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

fn map_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<Row> {
    Ok(Row {
        rowid: r.get(0)?,
        guid: r.get(1)?,
        handle: r.get(2)?,
        destination_caller_id: r.get(16)?,
        text: r.get(3)?,
        body: r.get(4)?,
        date: r.get(5)?,
        is_from_me: r.get(6)?,
        is_delivered: r.get(7)?,
        is_read: r.get(8)?,
        date_delivered: r.get(9)?,
        date_read: r.get(10)?,
        chat_id: r.get(11)?,
        chat_guid_count: r.get(12)?,
        service: r.get(13)?,
        participant_count: r.get(14)?,
        has_room_name: r.get::<_, i64>(15)? != 0,
        has_attachments: r.get::<_, Option<i64>>(17)?.unwrap_or(0) != 0,
    })
}

fn read_row_by_rowid(conn: &Connection, rowid: i64) -> rusqlite::Result<Option<Row>> {
    conn.query_row(ROW_QUERY, [rowid], map_row).optional()
}

enum HandleOutcome {
    Complete,
    Pending(String),
    Failed,
}

fn handle_row(ctx: &mut Ctx, row: &Row) -> HandleOutcome {
    if row.is_from_me == 0 {
        let event = match inbound_event(row) {
            Ok(event) => event,
            Err(reason) => return HandleOutcome::Pending(reason),
        };
        return match ctx.sink.emit_blocking(event) {
            Ok(_) => HandleOutcome::Complete,
            Err(error) => {
                tracing::warn!(%error, "failed to durably journal inbound message");
                HandleOutcome::Failed
            }
        };
    }

    // Outbound: forward its current status. Track the guid for delivered/read
    // re-polling; if the send worker has not bound it yet, Status is ignored.
    let status = status_of(row);
    let ts = status_time(row);
    let _ = ctx.corr_tx.send(CorrEvent::Status {
        guid: row.guid.clone(),
        status: status.to_string(),
        ts,
    });
    if status != "read" {
        ctx.active.entry(row.guid.clone()).or_insert(0);
    }
    HandleOutcome::Complete
}

fn inbound_event(row: &Row) -> Result<Event, String> {
    let text = row
        .text
        .as_ref()
        .filter(|text| !text.trim().is_empty())
        .cloned()
        .or_else(|| row.body.as_deref().and_then(decode_attributed_body))
        .filter(|text| !text.trim().is_empty())
        .or_else(|| row.has_attachments.then(|| "[Attachment]".to_string()));
    let mut missing = Vec::new();
    if row.handle.as_deref().is_none_or(str::is_empty) {
        missing.push("handle");
    }
    if row.chat_guid_count != 1 || row.chat_id.as_deref().is_none_or(str::is_empty) {
        missing.push("chat_id");
    }
    if text.is_none() {
        missing.push("text");
    }
    if !missing.is_empty() {
        return Err(format!("missing {}", missing.join(", ")));
    }

    let mut event = Event::new("message.received", row.guid.clone());
    event.provider_message_id = Some(row.guid.clone());
    event.handle = row.handle.clone();
    event.destination_caller_id = row
        .destination_caller_id
        .clone()
        .filter(|value| !value.trim().is_empty());
    event.chat_id = row.chat_id.clone();
    let classification = classify_thread(row);
    event.thread_kind = Some(classification.kind.to_string());
    event.participant_count = classification.participant_count;
    event.membership_complete = Some(classification.membership_complete);
    event.classification_basis = Some(classification.basis);
    event.classification_conflict = Some(classification.conflict);
    event.text = text;
    event.protocol = Some(normalize_service(row.service.as_deref()));
    event.status = Some("received".to_string());
    event.timestamp = apple_time_to_iso(row.date).unwrap_or_else(now_iso);
    Ok(event)
}

struct ThreadClassification {
    kind: &'static str,
    participant_count: Option<u32>,
    membership_complete: bool,
    basis: Vec<String>,
    conflict: bool,
}

fn classify_thread(row: &Row) -> ThreadClassification {
    let participant_count = u32::try_from(row.participant_count)
        .ok()
        .filter(|count| *count > 0);
    let membership_complete = row.chat_guid_count == 1 && participant_count.is_some();
    let chat_id = row.chat_id.as_deref().unwrap_or_default();
    let guid_direct = chat_id.contains(";-;");
    let guid_group = chat_id.contains(";+;");
    let participant_direct = membership_complete && participant_count == Some(1);
    let participant_group = participant_count.is_some_and(|count| count > 1);
    let conflict = guid_direct && (participant_group || row.has_room_name);

    let mut basis = Vec::new();
    if let Some(count) = participant_count {
        basis.push(format!("participant_count:{count}"));
    } else {
        basis.push("membership_unavailable".to_string());
    }
    if row.has_room_name {
        basis.push("room_name".to_string());
    }
    if guid_direct {
        basis.push("chat_guid:direct".to_string());
    } else if guid_group {
        basis.push("chat_guid:group".to_string());
    } else {
        basis.push("chat_guid:unknown".to_string());
    }
    if conflict {
        basis.push("contradictory_evidence".to_string());
    }

    let kind = if conflict {
        "unclassified"
    } else if participant_group || row.has_room_name || guid_group {
        "group"
    } else if participant_direct && guid_direct {
        "direct"
    } else {
        "unclassified"
    };

    ThreadClassification {
        kind,
        participant_count,
        membership_complete,
        basis,
        conflict,
    }
}

fn retry_pending(ctx: &mut Ctx, state: &mut State) {
    if state.pending_inbound.is_empty() {
        return;
    }
    let mut next = state.clone();
    for pending in state.pending_inbound.clone() {
        let outcome = match read_row_by_rowid(&ctx.conn, pending.rowid) {
            Ok(Some(row)) => match inbound_event(&row) {
                Ok(event) => match ctx.sink.emit_blocking(event) {
                    Ok(_) => Ok(()),
                    Err(error) => Err(format!("journal failed: {error}")),
                },
                Err(reason) => Err(reason),
            },
            Ok(None) => Err("message row is absent from chat.db".to_string()),
            Err(error) => Err(format!("chat.db query failed: {error}")),
        };

        match outcome {
            Ok(()) => {
                next.pending_inbound
                    .retain(|candidate| candidate.guid != pending.guid);
                tracing::info!(
                    message_guid = %pending.guid,
                    attempts = pending.attempts,
                    "published enriched inbound message"
                );
            }
            Err(reason) => {
                if let Some(candidate) = next
                    .pending_inbound
                    .iter_mut()
                    .find(|candidate| candidate.guid == pending.guid)
                {
                    candidate.attempts = candidate.attempts.saturating_add(1);
                    candidate.last_error = Some(reason.clone());
                    if candidate.attempts >= QUARANTINE_AFTER_ATTEMPTS
                        && candidate.quarantined_at.is_none()
                    {
                        candidate.quarantined_at = Some(now_iso());
                        tracing::error!(
                            message_guid = %candidate.guid,
                            rowid = candidate.rowid,
                            %reason,
                            "inbound message quarantined after enrichment deadline"
                        );
                    }
                }
            }
        }
    }
    if let Err(error) = next.save() {
        tracing::warn!(%error, "failed to persist inbound enrichment state");
        return;
    }
    *state = next;
}

fn normalize_service(service: Option<&str>) -> String {
    match service.map(str::trim).filter(|value| !value.is_empty()) {
        Some(value) if value.eq_ignore_ascii_case("iMessage") => "imessage".to_string(),
        Some(value) if value.eq_ignore_ascii_case("SMS") => "sms".to_string(),
        Some(value) if value.eq_ignore_ascii_case("RCS") => "rcs".to_string(),
        Some(value) => value.to_lowercase(),
        None => "imessage".to_string(),
    }
}

/// Re-read the tracked outbound guids' status from chat.db and forward any
/// transition. Catches the in-place delivered/read UPDATEs the ROWID scan
/// misses. Prune guids once they reach a terminal status.
fn recheck_active(ctx: &mut Ctx) {
    if ctx.active.is_empty() {
        return;
    }
    let guids: Vec<String> = ctx.active.keys().cloned().collect();
    let placeholders = std::iter::repeat_n("?", guids.len())
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        "SELECT m.guid, m.is_delivered, m.is_read, m.date, m.date_delivered, m.date_read \
         FROM message m WHERE m.guid IN ({placeholders})"
    );
    let mut stmt = match ctx.conn.prepare(&sql) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(error = %e, "prepare active query failed");
            return;
        }
    };
    let rows = stmt.query_map(rusqlite::params_from_iter(guids.iter()), |r| {
        Ok((
            r.get::<_, String>(0)?, // guid
            r.get::<_, i64>(1)?,    // is_delivered
            r.get::<_, i64>(2)?,    // is_read
            r.get::<_, i64>(3)?,    // date
            r.get::<_, i64>(4)?,    // date_delivered
            r.get::<_, i64>(5)?,    // date_read
        ))
    });
    let rows = match rows {
        Ok(r) => r.flatten().collect::<Vec<_>>(),
        Err(e) => {
            tracing::warn!(error = %e, "active query failed");
            return;
        }
    };

    for (guid, is_delivered, is_read, date, date_delivered, date_read) in rows {
        let status = status_flags(is_delivered, is_read, date_delivered, date_read);
        let ts = apple_time_to_iso(pick_time(status, date, date_delivered, date_read))
            .unwrap_or_else(now_iso);
        let _ = ctx.corr_tx.send(CorrEvent::Status {
            guid: guid.clone(),
            status: status.to_string(),
            ts,
        });
        // Drop on terminal status, or after the recheck cap so we don't poll a
        // never-read message forever.
        let count = ctx.active.entry(guid.clone()).or_insert(0);
        *count += 1;
        if status == "read" || *count >= MAX_RECHECKS {
            ctx.active.remove(&guid);
        }
    }
}

fn status_of(row: &Row) -> &'static str {
    status_flags(
        row.is_delivered,
        row.is_read,
        row.date_delivered,
        row.date_read,
    )
}

fn status_flags(
    is_delivered: i64,
    is_read: i64,
    date_delivered: i64,
    date_read: i64,
) -> &'static str {
    if is_read == 1 || date_read > 0 {
        "read"
    } else if is_delivered == 1 || date_delivered > 0 {
        "delivered"
    } else {
        "sent"
    }
}

fn status_time(row: &Row) -> String {
    apple_time_to_iso(pick_time(
        status_of(row),
        row.date,
        row.date_delivered,
        row.date_read,
    ))
    .unwrap_or_else(now_iso)
}

fn pick_time(status: &str, date: i64, date_delivered: i64, date_read: i64) -> i64 {
    match status {
        "read" => date_read,
        "delivered" => date_delivered,
        _ => date,
    }
}

/// Best-effort extraction of message text from a typedstream `NSAttributedString`
/// blob. The format is undocumented and has appeared with both byte orders for
/// two-byte lengths, so accept only an in-bounds UTF-8 candidate.
pub(crate) fn decode_attributed_body(blob: &[u8]) -> Option<String> {
    let marker = b"NSString";
    let start = blob.windows(marker.len()).position(|w| w == marker)? + marker.len();
    let rest = &blob[start..];

    // Shortly after the class marker is a length-prefixed UTF-8 run. The length
    // token is 0x2B (1-byte length) or 0x81 (2-byte length).
    for i in 0..rest.len().min(16) {
        match rest[i] {
            0x2B => {
                let len = *rest.get(i + 1)? as usize;
                let s = rest.get(i + 2..i + 2 + len)?;
                return String::from_utf8(s.to_vec()).ok();
            }
            0x81 => {
                let first = *rest.get(i + 1)?;
                let second = *rest.get(i + 2)?;
                let mut lengths = [
                    u16::from_be_bytes([first, second]) as usize,
                    u16::from_le_bytes([first, second]) as usize,
                ];
                lengths.sort_unstable_by(|left, right| right.cmp(left));
                for len in lengths {
                    let Some(s) = rest.get(i + 3..i + 3 + len) else {
                        continue;
                    };
                    if let Ok(text) = String::from_utf8(s.to_vec()) {
                        return Some(text);
                    }
                }
            }
            _ => {}
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_messages_services_without_mislabeling_unknown_values() {
        assert_eq!(normalize_service(Some("iMessage")), "imessage");
        assert_eq!(normalize_service(Some("SMS")), "sms");
        assert_eq!(normalize_service(Some("rcs")), "rcs");
        assert_eq!(normalize_service(Some("Satellite")), "satellite");
        assert_eq!(normalize_service(None), "imessage");
    }

    fn inbound_row(handle: Option<&str>, chat_id: Option<&str>, chat_guid_count: i64) -> Row {
        Row {
            rowid: 42,
            guid: "provider-guid".to_string(),
            handle: handle.map(ToString::to_string),
            destination_caller_id: Some("+16469921008".to_string()),
            text: Some("Yes, let's do that".to_string()),
            body: None,
            date: 1,
            is_from_me: 0,
            is_delivered: 0,
            is_read: 0,
            date_delivered: 0,
            date_read: 0,
            chat_id: chat_id.map(ToString::to_string),
            chat_guid_count,
            service: Some("iMessage".to_string()),
            participant_count: 1,
            has_room_name: false,
            has_attachments: false,
        }
    }

    #[test]
    fn missing_destination_remains_optional_without_guessing_from_chat() {
        for destination in [None, Some(""), Some(" ")] {
            let mut row = inbound_row(Some("+15550000001"), Some("any;-;+15550000001"), 1);
            row.destination_caller_id = destination.map(str::to_string);
            let event = inbound_event(&row).unwrap();
            assert!(event.destination_caller_id.is_none());
            assert!(serde_json::to_value(event)
                .unwrap()
                .get("destination_caller_id")
                .is_none());
        }
    }

    #[test]
    fn attachment_only_inbound_is_journaled() {
        let mut row = inbound_row(Some("+15550000001"), Some("iMessage;-;direct"), 1);
        row.text = None;
        row.body = None;
        row.has_attachments = true;
        assert_eq!(
            inbound_event(&row).unwrap().text.as_deref(),
            Some("[Attachment]")
        );
    }

    #[test]
    fn incomplete_inbound_rows_are_not_public_events() {
        let incomplete = inbound_row(None, None, 0);
        assert_eq!(
            inbound_event(&incomplete).unwrap_err(),
            "missing handle, chat_id"
        );

        let enriched = inbound_row(Some("+15550000001"), Some("any;-;+15550000001"), 1);
        let event = inbound_event(&enriched).unwrap();
        assert_eq!(event.handle.as_deref(), Some("+15550000001"));
        assert_eq!(event.chat_id.as_deref(), Some("any;-;+15550000001"));
        assert_eq!(event.thread_kind.as_deref(), Some("direct"));
        assert_eq!(event.participant_count, Some(1));
        assert_eq!(event.membership_complete, Some(true));
        assert_eq!(event.classification_conflict, Some(false));
    }

    #[test]
    fn classifies_group_and_conflicting_chat_evidence_safely() {
        let mut group = inbound_row(Some("+15550000001"), Some("iMessage;+;chat-group"), 1);
        group.participant_count = 2;
        assert_eq!(classify_thread(&group).kind, "group");

        let mut conflicting = inbound_row(Some("+15550000001"), Some("iMessage;-;+15550000001"), 1);
        conflicting.participant_count = 2;
        let classification = classify_thread(&conflicting);
        assert_eq!(classification.kind, "unclassified");
        assert!(classification.conflict);
        assert!(classification
            .basis
            .contains(&"contradictory_evidence".to_string()));
    }

    #[test]
    fn decodes_long_typedstream_lengths_in_both_byte_orders() {
        for len in [128_usize, 255, 256, 570, 1_102] {
            let text = "x".repeat(len);
            for encoded_len in [(len as u16).to_be_bytes(), (len as u16).to_le_bytes()] {
                let mut blob = b"prefixNSString".to_vec();
                blob.extend_from_slice(&[0x81, encoded_len[0], encoded_len[1]]);
                blob.extend_from_slice(text.as_bytes());
                assert_eq!(
                    decode_attributed_body(&blob).as_deref(),
                    Some(text.as_str())
                );
            }
        }
    }

    #[test]
    fn typedstream_lengths_count_utf8_bytes() {
        let text = "🙂".repeat(70);
        let bytes = text.as_bytes();
        let encoded_len = (bytes.len() as u16).to_be_bytes();
        let mut blob = b"NSString".to_vec();
        blob.extend_from_slice(&[0x81, encoded_len[0], encoded_len[1]]);
        blob.extend_from_slice(bytes);
        assert_eq!(
            decode_attributed_body(&blob).as_deref(),
            Some(text.as_str())
        );
    }

    #[tokio::test]
    async fn inbound_query_exposes_only_unambiguous_chat_guids() {
        // libSQL and rusqlite share the bundled SQLite process state. The
        // daemon initializes libSQL first, so mirror that order here before
        // opening the chat.db-shaped rusqlite fixture.
        let state_path = std::env::temp_dir()
            .join(format!("blueski-receive-query-{}.db", uuid::Uuid::new_v4()))
            .to_string_lossy()
            .into_owned();
        let _store = crate::store::Store::open(&state_path, "bsinst_test")
            .await
            .unwrap();
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE message (
                 ROWID INTEGER PRIMARY KEY, guid TEXT NOT NULL, handle_id INTEGER,
                 text TEXT, attributedBody BLOB, date INTEGER NOT NULL,
                 is_from_me INTEGER NOT NULL, is_delivered INTEGER NOT NULL,
                 is_read INTEGER NOT NULL, date_delivered INTEGER NOT NULL,
                 date_read INTEGER NOT NULL, service TEXT, destination_caller_id TEXT, cache_has_attachments INTEGER DEFAULT 0
             );
             CREATE TABLE handle (ROWID INTEGER PRIMARY KEY, id TEXT);
             CREATE TABLE chat (ROWID INTEGER PRIMARY KEY, guid TEXT, room_name TEXT);
             CREATE TABLE chat_message_join (chat_id INTEGER, message_id INTEGER);
             CREATE TABLE chat_handle_join (chat_id INTEGER, handle_id INTEGER);
             INSERT INTO handle VALUES (1, '+15550000001');
             INSERT INTO chat VALUES (10, 'iMessage;-;direct', NULL);
             INSERT INTO chat VALUES (11, 'iMessage;+;group-a', 'Group A');
             INSERT INTO chat VALUES (12, 'iMessage;+;group-b', 'Group B');
             INSERT INTO message VALUES
               (1, 'provider-1', 1, 'one', NULL, 1, 0, 0, 0, 0, 0, 'iMessage', '+16469921008', 0),
               (2, 'provider-2', 1, 'two', NULL, 2, 0, 0, 0, 0, 0, 'SMS', NULL, 0);
             INSERT INTO chat_message_join VALUES (10, 1), (11, 2), (12, 2);
             INSERT INTO chat_handle_join VALUES (10, 1), (11, 1), (12, 1);",
        )
        .unwrap();

        let rows = read_rows(&conn, 0).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0].destination_caller_id.as_deref(),
            Some("+16469921008")
        );
        assert!(rows[1].destination_caller_id.is_none());
        let reread = read_row_by_rowid(&conn, 1).unwrap().unwrap();
        assert_eq!(
            inbound_event(&reread)
                .unwrap()
                .destination_caller_id
                .as_deref(),
            Some("+16469921008")
        );
        assert_eq!(rows[0].chat_id.as_deref(), Some("iMessage;-;direct"));
        assert_eq!(rows[0].chat_guid_count, 1);
        assert_eq!(rows[0].service.as_deref(), Some("iMessage"));
        assert_eq!(rows[0].participant_count, 1);
        assert!(rows[1].chat_id.is_none());
        assert_eq!(rows[1].chat_guid_count, 2);
        assert_eq!(rows[1].service.as_deref(), Some("SMS"));
    }
}
