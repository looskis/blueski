//! Drains the in-memory send queue and runs each job through the `Sender`,
//! emitting `message.queued` -> `message.sent` | `message.failed`.
//!
//! It also records each send into the correlation store *before* dispatching to
//! AppleScript, capturing the chat.db ROWID watermark. After AppleScript returns,
//! it resolves the created row from chat.db and binds that exact guid back to
//! our `message_id`.

use crate::model::{now_iso, Event, SendJob, SendTarget};
use crate::receive::{decode_attributed_body, open_reader};
use crate::sender::Sender;
use crate::store::{self, Store};
use crate::webhook::EventSink;
use rusqlite::Connection;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::sleep;

const RESOLVE_ATTEMPTS: u32 = 32;
const RESOLVE_INTERVAL: Duration = Duration::from_millis(250);

pub async fn run<S: Sender>(
    mut rx: mpsc::UnboundedReceiver<SendJob>,
    sender: S,
    sink: EventSink,
    store: Store,
    max_rowid: Arc<AtomicI64>,
    chatdb: PathBuf,
) {
    let store_conn = match store.conn() {
        Ok(c) => Some(c),
        Err(e) => {
            tracing::warn!(error = %e, "correlation store unavailable — sends won't be bound");
            None
        }
    };

    while let Some(job) = rx.recv().await {
        // Capture the freshest watermark BEFORE the send. The receive worker's
        // atomic is a useful fallback, but it can lag behind chat.db during a
        // burst; querying here narrows the resolver window.
        let pre = match current_max_rowid(&chatdb).await {
            Ok(rowid) => {
                max_rowid.fetch_max(rowid, Ordering::Relaxed);
                rowid
            }
            Err(e) => {
                tracing::warn!(error = %e, "failed to read chat.db watermark; falling back to receive watermark");
                max_rowid.load(Ordering::Relaxed)
            }
        };

        if let Some(conn) = &store_conn {
            if let Err(e) = store::record_pending(
                conn,
                &job.message_id,
                job.client_ref.as_deref(),
                job.target.store_key(),
                &job.protocol,
                pre,
                &now_iso(),
            )
            .await
            {
                tracing::warn!(error = %e, "failed to record pending send");
            }
        }

        sink.emit(queued_event(&job));

        match sender.send(&job).await {
            Ok(()) => {
                sink.emit(terminal_event("message.sent", &job, None));
                if let Some(conn) = &store_conn {
                    match resolve_sent_row(chatdb.clone(), job.clone(), pre).await {
                        Ok(Some(row)) => {
                            match store::bind_resolved(conn, &job.message_id, row.rowid, &row.guid)
                                .await
                            {
                                Ok(true) => {
                                    max_rowid.fetch_max(row.rowid, Ordering::Relaxed);
                                    tracing::info!(
                                        message_id = %job.message_id,
                                        guid = %row.guid,
                                        rowid = row.rowid,
                                        "bound outbound send"
                                    );
                                }
                                Ok(false) => tracing::warn!(
                                    message_id = %job.message_id,
                                    guid = %row.guid,
                                    "resolved send row but pending row was already bound"
                                ),
                                Err(e) => tracing::warn!(
                                    message_id = %job.message_id,
                                    guid = %row.guid,
                                    error = %e,
                                    "failed to persist send binding"
                                ),
                            }
                        }
                        Ok(None) => tracing::warn!(
                            message_id = %job.message_id,
                            "sent message accepted but no unique chat.db row could be resolved"
                        ),
                        Err(e) => tracing::warn!(
                            message_id = %job.message_id,
                            error = %e,
                            "sent message accepted but chat.db row resolution failed"
                        ),
                    }
                }
            }
            Err(reason) => {
                tracing::warn!(message_id = %job.message_id, error = %reason, "send failed");
                sink.emit(terminal_event("message.failed", &job, Some(reason)));
            }
        }
    }
}

fn queued_event(job: &SendJob) -> Event {
    let mut ev = Event::new("message.queued", job.message_id.clone());
    ev.client_ref = job.client_ref.clone();
    ev.handle = job.target.event_handle();
    ev.text = Some(job.text.clone());
    ev.protocol = Some(job.protocol.clone());
    ev
}

fn terminal_event(name: &str, job: &SendJob, reason: Option<String>) -> Event {
    let mut ev = Event::new(name, job.message_id.clone());
    ev.client_ref = job.client_ref.clone();
    ev.handle = job.target.event_handle();
    ev.protocol = Some(job.protocol.clone());
    ev.reason = reason;
    ev
}

#[derive(Debug)]
struct ResolvedRow {
    rowid: i64,
    guid: String,
}

#[derive(Debug)]
enum ResolveAttempt {
    Unique(ResolvedRow),
    Ambiguous { matches: usize },
    Pending,
}

async fn current_max_rowid(chatdb: &Path) -> Result<i64, String> {
    let path = chatdb.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let conn = open_reader(&path).map_err(|e| e.to_string())?;
        max_rowid_db(&conn).map_err(|e| e.to_string())
    })
    .await
    .map_err(|e| e.to_string())?
}

async fn resolve_sent_row(
    chatdb: PathBuf,
    job: SendJob,
    pre_rowid: i64,
) -> Result<Option<ResolvedRow>, String> {
    for attempt in 0..RESOLVE_ATTEMPTS {
        let path = chatdb.clone();
        let text = job.text.clone();
        let target = job.target.clone();
        let allow_single_row_fallback = attempt + 1 == RESOLVE_ATTEMPTS;
        let outcome = tokio::task::spawn_blocking(move || {
            resolve_once(&path, pre_rowid, &text, &target, allow_single_row_fallback)
        })
        .await
        .map_err(|e| e.to_string())??;

        match outcome {
            ResolveAttempt::Unique(row) => return Ok(Some(row)),
            ResolveAttempt::Ambiguous { matches } => {
                tracing::warn!(matches, "ambiguous post-send chat.db candidates");
                return Ok(None);
            }
            ResolveAttempt::Pending => sleep(RESOLVE_INTERVAL).await,
        }
    }
    Ok(None)
}

fn max_rowid_db(conn: &Connection) -> rusqlite::Result<i64> {
    conn.query_row("SELECT COALESCE(MAX(ROWID), 0) FROM message", [], |r| {
        r.get(0)
    })
}

fn resolve_once(
    chatdb: &Path,
    pre_rowid: i64,
    text: &str,
    target: &SendTarget,
    allow_single_row_fallback: bool,
) -> Result<ResolveAttempt, String> {
    let conn = open_reader(chatdb).map_err(|e| e.to_string())?;
    let mut stmt = conn
        .prepare(
            "SELECT m.ROWID, m.guid, m.text, m.attributedBody,
                    group_concat(DISTINCT h.id), group_concat(DISTINCT c.guid)
             FROM message m
             LEFT JOIN chat_message_join cmj ON cmj.message_id = m.ROWID
             LEFT JOIN chat c ON c.ROWID = cmj.chat_id
             LEFT JOIN chat_handle_join chj ON chj.chat_id = cmj.chat_id
             LEFT JOIN handle h ON h.ROWID = chj.handle_id
             WHERE m.ROWID > ?1 AND m.is_from_me = 1
             GROUP BY m.ROWID, m.guid, m.text, m.attributedBody
             ORDER BY m.ROWID ASC
             LIMIT 25",
        )
        .map_err(|e| e.to_string())?;

    let rows = stmt
        .query_map([pre_rowid], |r| {
            Ok(CandidateRow {
                rowid: r.get(0)?,
                guid: r.get(1)?,
                text: r.get(2)?,
                body: r.get(3)?,
                handles_csv: r.get(4)?,
                chat_guids_csv: r.get(5)?,
            })
        })
        .map_err(|e| e.to_string())?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(|e| e.to_string())?;

    select_candidate(&rows, text, target, allow_single_row_fallback)
}

fn select_candidate(
    rows: &[CandidateRow],
    text: &str,
    target: &SendTarget,
    allow_single_row_fallback: bool,
) -> Result<ResolveAttempt, String> {
    if rows.is_empty() {
        return Ok(ResolveAttempt::Pending);
    }

    let text_matches = rows
        .iter()
        .filter(|row| row.message_text().as_deref() == Some(text))
        .collect::<Vec<_>>();
    let strong = text_matches
        .iter()
        .filter(|row| row.matches_target(target))
        .collect::<Vec<_>>();

    if strong.len() == 1 {
        let row = strong[0];
        return Ok(ResolveAttempt::Unique(ResolvedRow {
            rowid: row.rowid,
            guid: row.guid.clone(),
        }));
    }
    if strong.len() > 1 {
        return Ok(ResolveAttempt::Ambiguous {
            matches: strong.len(),
        });
    }

    // Some macOS builds populate chat joins after the message row. If no handle
    // evidence arrives, only fall back when the entire post-watermark window has
    // exactly one outbound row and its decoded body matches.
    if allow_single_row_fallback && rows.len() == 1 && text_matches.len() == 1 {
        let row = text_matches[0];
        return Ok(ResolveAttempt::Unique(ResolvedRow {
            rowid: row.rowid,
            guid: row.guid.clone(),
        }));
    }

    if allow_single_row_fallback && text_matches.len() > 1 {
        return Ok(ResolveAttempt::Ambiguous {
            matches: text_matches.len(),
        });
    }

    Ok(ResolveAttempt::Pending)
}

#[derive(Debug)]
struct CandidateRow {
    rowid: i64,
    guid: String,
    text: Option<String>,
    body: Option<Vec<u8>>,
    handles_csv: Option<String>,
    chat_guids_csv: Option<String>,
}

impl CandidateRow {
    fn message_text(&self) -> Option<String> {
        self.text
            .as_ref()
            .filter(|s| !s.is_empty())
            .cloned()
            .or_else(|| self.body.as_deref().and_then(decode_attributed_body))
    }

    fn has_handle(&self, handle: &str) -> bool {
        self.handles_csv
            .as_deref()
            .map(|handles| handles.split(',').any(|candidate| candidate == handle))
            .unwrap_or(false)
    }

    fn has_chat_guid(&self, chat_id: &str) -> bool {
        self.chat_guids_csv
            .as_deref()
            .map(|chat_guids| chat_guids.split(',').any(|candidate| candidate == chat_id))
            .unwrap_or(false)
    }

    fn matches_target(&self, target: &SendTarget) -> bool {
        match target {
            SendTarget::Handle { to } => self.has_handle(to),
            SendTarget::Chat { chat_id } => self.has_chat_guid(chat_id),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(
        rowid: i64,
        guid: &str,
        text: &str,
        handles_csv: &str,
        chat_guids_csv: &str,
    ) -> CandidateRow {
        CandidateRow {
            rowid,
            guid: guid.to_string(),
            text: Some(text.to_string()),
            body: None,
            handles_csv: Some(handles_csv.to_string()),
            chat_guids_csv: Some(chat_guids_csv.to_string()),
        }
    }

    #[test]
    fn resolver_matches_text_and_recipient() {
        let rows = vec![
            candidate(101, "manual-guid", "same body", "+15550000002", "chat-2"),
            candidate(102, "api-guid", "same body", "+15550000001", "chat-1"),
        ];

        let outcome = select_candidate(
            &rows,
            "same body",
            &SendTarget::Handle {
                to: "+15550000001".to_string(),
            },
            false,
        )
        .unwrap();

        match outcome {
            ResolveAttempt::Unique(row) => {
                assert_eq!(row.rowid, 102);
                assert_eq!(row.guid, "api-guid");
            }
            other => panic!("expected unique resolver match, got {other:?}"),
        }
    }

    #[test]
    fn resolver_refuses_duplicate_recipient_matches() {
        let rows = vec![
            candidate(101, "first-guid", "same body", "+15550000001", "chat-1"),
            candidate(102, "second-guid", "same body", "+15550000001", "chat-1"),
        ];

        let outcome = select_candidate(
            &rows,
            "same body",
            &SendTarget::Handle {
                to: "+15550000001".to_string(),
            },
            false,
        )
        .unwrap();

        match outcome {
            ResolveAttempt::Ambiguous { matches } => assert_eq!(matches, 2),
            other => panic!("expected ambiguous resolver result, got {other:?}"),
        }
    }

    #[test]
    fn resolver_matches_text_and_chat() {
        let rows = vec![
            candidate(101, "other-guid", "same body", "+15550000001", "chat-other"),
            candidate(102, "chat-guid", "same body", "+15550000002", "chat-target"),
        ];

        let outcome = select_candidate(
            &rows,
            "same body",
            &SendTarget::Chat {
                chat_id: "chat-target".to_string(),
            },
            false,
        )
        .unwrap();

        match outcome {
            ResolveAttempt::Unique(row) => {
                assert_eq!(row.rowid, 102);
                assert_eq!(row.guid, "chat-guid");
            }
            other => panic!("expected unique resolver match, got {other:?}"),
        }
    }
}
