//! Drains the in-memory send queue and runs each job through the `Sender`,
//! emitting `message.queued` -> `message.sent` | `message.failed`.
//!
//! It also records each send into the correlation store *before* dispatching to
//! AppleScript, capturing the chat.db ROWID watermark. After AppleScript returns,
//! it resolves the created row from chat.db and binds that exact guid back to
//! our `message_id`.

use crate::model::{SendJob, SendTarget};
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
    mut wake_rx: mpsc::UnboundedReceiver<()>,
    sender: S,
    sink: EventSink,
    store: Store,
    max_rowid: Arc<AtomicI64>,
    chatdb: PathBuf,
) {
    let store_conn = match store.conn() {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "durable send store unavailable — send worker stopped");
            return;
        }
    };

    if let Err(error) = recover_inflight(&store_conn, &sink, &chatdb, &max_rowid).await {
        tracing::warn!(%error, "failed to reconcile in-flight sends at startup");
    }

    loop {
        loop {
            match store::next_queued(&store_conn).await {
                Ok(Some(send)) => {
                    process_queued(&store_conn, &sender, &sink, &chatdb, &max_rowid, send.job)
                        .await;
                }
                Ok(None) => break,
                Err(error) => {
                    tracing::warn!(%error, "failed to load durable send queue");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }

        if wake_rx.recv().await.is_none() {
            break;
        }
    }
}

async fn process_queued<S: Sender>(
    conn: &libsql::Connection,
    sender: &S,
    sink: &EventSink,
    chatdb: &Path,
    max_rowid: &Arc<AtomicI64>,
    job: SendJob,
) {
    // Capture the freshest watermark BEFORE dispatch. Persisting the
    // `dispatching` claim before AppleScript creates an explicit crash window
    // that startup reconciliation handles conservatively.
    let pre = match current_max_rowid(chatdb).await {
        Ok(rowid) => {
            max_rowid.fetch_max(rowid, Ordering::Relaxed);
            rowid
        }
        Err(error) => {
            tracing::warn!(%error, "failed to read chat.db watermark; falling back to receive watermark");
            max_rowid.load(Ordering::Relaxed)
        }
    };
    match store::mark_dispatching(conn, &job.message_id, pre).await {
        Ok(true) => {}
        Ok(false) => return,
        Err(error) => {
            tracing::warn!(%error, message_id = %job.message_id, "failed to claim queued send");
            return;
        }
    }

    match sender.send(&job).await {
        Ok(()) => {
            match store::record_sent_acceptance(conn, &job).await {
                Ok(event) => sink.publish_committed(event),
                Err(error) => tracing::warn!(
                    %error,
                    message_id = %job.message_id,
                    "failed to journal Messages.app acceptance"
                ),
            }
            resolve_and_finish(conn, sink, chatdb, max_rowid, job, pre).await;
        }
        Err(reason) => {
            tracing::warn!(message_id = %job.message_id, error = %reason, "send failed");
            match store::record_failed(conn, &job, reason).await {
                Ok(event) => sink.publish_committed(event),
                Err(error) => tracing::warn!(
                    %error,
                    message_id = %job.message_id,
                    "failed to record send failure"
                ),
            }
        }
    }
}

async fn recover_inflight(
    conn: &libsql::Connection,
    sink: &EventSink,
    chatdb: &Path,
    max_rowid: &Arc<AtomicI64>,
) -> anyhow::Result<()> {
    for send in store::recoverable_sends(conn).await? {
        tracing::info!(
            message_id = %send.job.message_id,
            state = %send.dispatch_state,
            "reconciling interrupted send"
        );
        resolve_and_finish(conn, sink, chatdb, max_rowid, send.job, send.pre_rowid).await;
    }
    Ok(())
}

async fn resolve_and_finish(
    conn: &libsql::Connection,
    sink: &EventSink,
    chatdb: &Path,
    max_rowid: &Arc<AtomicI64>,
    job: SendJob,
    pre_rowid: i64,
) {
    match resolve_sent_row(chatdb.to_path_buf(), job.clone(), pre_rowid).await {
        Ok(Some(row)) => {
            match store::bind_and_record_sent(
                conn,
                &job.message_id,
                row.rowid,
                &row.guid,
                row.chat_id.as_deref(),
            )
            .await
            {
                Ok(Some(event)) => {
                    max_rowid.fetch_max(row.rowid, Ordering::Relaxed);
                    tracing::info!(
                        message_id = %job.message_id,
                        guid = %row.guid,
                        chat_id = ?row.chat_id,
                        rowid = row.rowid,
                        "bound outbound send"
                    );
                    sink.publish_committed(event);
                }
                Ok(None) => tracing::warn!(
                    message_id = %job.message_id,
                    guid = %row.guid,
                    "resolved send row but outbound row was already bound"
                ),
                Err(error) => tracing::warn!(
                    %error,
                    message_id = %job.message_id,
                    guid = %row.guid,
                    "failed to bind and journal resolved send"
                ),
            }
        }
        Ok(None) => {
            let reason = "send outcome could not be reconciled without risking a duplicate";
            tracing::warn!(message_id = %job.message_id, "{reason}");
            match store::record_unknown(conn, &job, reason).await {
                Ok(event) => sink.publish_committed(event),
                Err(error) => tracing::warn!(%error, "failed to record unknown send outcome"),
            }
        }
        Err(error) => {
            let reason = format!("chat.db reconciliation failed: {error}");
            tracing::warn!(message_id = %job.message_id, %reason);
            match store::record_unknown(conn, &job, &reason).await {
                Ok(event) => sink.publish_committed(event),
                Err(error) => tracing::warn!(%error, "failed to record unknown send outcome"),
            }
        }
    }
}

#[derive(Debug)]
struct ResolvedRow {
    rowid: i64,
    guid: String,
    chat_id: Option<String>,
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
            chat_id: row.resolved_chat_id(target),
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
            chat_id: row.resolved_chat_id(target),
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

    fn distinct_chat_guids(&self) -> Vec<&str> {
        let mut guids = self
            .chat_guids_csv
            .as_deref()
            .into_iter()
            .flat_map(|value| value.split(','))
            .filter(|value| !value.is_empty())
            .collect::<Vec<_>>();
        guids.sort_unstable();
        guids.dedup();
        guids
    }

    fn resolved_chat_id(&self, target: &SendTarget) -> Option<String> {
        match target {
            SendTarget::Chat { chat_id } if self.has_chat_guid(chat_id) => Some(chat_id.clone()),
            SendTarget::Chat { .. } => None,
            SendTarget::Handle { .. } => {
                let guids = self.distinct_chat_guids();
                (guids.len() == 1).then(|| guids[0].to_string())
            }
        }
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
                assert_eq!(row.chat_id.as_deref(), Some("chat-1"));
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
                assert_eq!(row.chat_id.as_deref(), Some("chat-target"));
            }
            other => panic!("expected unique resolver match, got {other:?}"),
        }
    }

    #[test]
    fn resolver_leaves_ambiguous_handle_chat_unset() {
        let rows = vec![candidate(
            101,
            "provider-guid",
            "body",
            "+15550000001",
            "chat-1,chat-2",
        )];
        let outcome = select_candidate(
            &rows,
            "body",
            &SendTarget::Handle {
                to: "+15550000001".to_string(),
            },
            false,
        )
        .unwrap();

        match outcome {
            ResolveAttempt::Unique(row) => assert!(row.chat_id.is_none()),
            other => panic!("expected a provider binding with no chat, got {other:?}"),
        }
    }
}
