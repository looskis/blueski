//! Read-only, message-scoped attachments. Never accept a filesystem path from HTTP.
use crate::server::AppState;
use axum::{
    body::Body,
    extract::{Path, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use serde::Serialize;
use std::{io::Read, path::PathBuf};
const MAX_BYTES: u64 = 20 * 1024 * 1024;
#[derive(Serialize)]
struct Attachment {
    id: i64,
    mime_type: Option<String>,
    bytes: Option<i64>,
}
fn conn(path: &std::path::Path) -> rusqlite::Result<Connection> {
    Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
}
fn unavailable() -> Response {
    (StatusCode::SERVICE_UNAVAILABLE, "Attachments unavailable").into_response()
}
pub async fn list(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    tokio::task::spawn_blocking(move || {
        let Ok(db)=conn(&state.chatdb) else {return unavailable()};
        let scope=db.query_row("SELECT m.service,m.destination_caller_id,h.id,(SELECT c.guid FROM chat c JOIN chat_message_join j ON j.chat_id=c.ROWID WHERE j.message_id=m.ROWID LIMIT 1),(SELECT count(*) FROM chat_message_join j WHERE j.message_id=m.ROWID) FROM message m LEFT JOIN handle h ON h.ROWID=m.handle_id WHERE m.guid=?1 AND m.is_from_me=0",[&id],|r|Ok((r.get::<_,Option<String>>(0)?,r.get::<_,Option<String>>(1)?,r.get::<_,Option<String>>(2)?,r.get::<_,Option<String>>(3)?,r.get::<_,i64>(4)?))).optional();
        let (protocol,destination,peer,chat_id,count)=match scope {Ok(Some(s))=>s,Ok(None)=>return StatusCode::NOT_FOUND.into_response(),Err(_)=>return unavailable()};
        if count!=1 {return StatusCode::CONFLICT.into_response()}
        let Ok(mut stmt)=db.prepare("SELECT a.ROWID,a.mime_type,a.total_bytes FROM attachment a JOIN message_attachment_join j ON j.attachment_id=a.ROWID JOIN message m ON m.ROWID=j.message_id WHERE m.guid=?1 AND m.is_from_me=0 ORDER BY a.ROWID LIMIT 9") else {return unavailable()};
        let rows=stmt.query_map([&id],|r|Ok(Attachment{id:r.get(0)?,mime_type:r.get(1)?,bytes:r.get(2)?}));
        let Ok(rows)=rows else{return unavailable()};let Ok(rows)=rows.collect::<Result<Vec<_>,_>>() else{return unavailable()};
        let mut response=Json(serde_json::json!({"message_id":id,"chat_id":chat_id,"destination":destination,"peer":peer,"protocol":protocol,"attachments":rows})).into_response();
        response.headers_mut().insert(header::CACHE_CONTROL,"no-store".parse().unwrap());response
    }).await.unwrap_or_else(|_|unavailable())
}
fn read_scoped(root: &std::path::Path, filename: &str) -> std::io::Result<Vec<u8>> {
    let path = if let Some(rest) = filename.strip_prefix("~/") {
        PathBuf::from(std::env::var_os("HOME").unwrap_or_default()).join(rest)
    } else {
        PathBuf::from(filename)
    };
    let root = root.canonicalize()?;
    let path = path.canonicalize()?;
    if !path.starts_with(&root) {
        return Err(std::io::Error::other("outside attachment directory"));
    }
    let file = std::fs::File::open(&path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() > MAX_BYTES {
        return Err(std::io::Error::other("invalid attachment size"));
    }
    let mut bytes = Vec::new();
    file.take(MAX_BYTES + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_BYTES {
        return Err(std::io::Error::other("attachment too large"));
    }
    Ok(bytes)
}
pub async fn download(
    State(state): State<AppState>,
    Path((id, attachment)): Path<(String, i64)>,
) -> Response {
    tokio::task::spawn_blocking(move || {
        let Ok(db)=conn(&state.chatdb) else {return unavailable()};
        let filename=db.query_row("SELECT a.filename FROM attachment a JOIN message_attachment_join j ON j.attachment_id=a.ROWID JOIN message m ON m.ROWID=j.message_id WHERE m.guid=?1 AND a.ROWID=?2 AND m.is_from_me=0",rusqlite::params![id,attachment],|r|r.get::<_,Option<String>>(0)).optional();
        let filename=match filename {Ok(Some(Some(p)))=>p,Ok(_)=>return StatusCode::NOT_FOUND.into_response(),Err(_)=>return unavailable()};
        let Some(parent)=state.chatdb.parent() else{return unavailable()};
        match read_scoped(&parent.join("Attachments"),&filename) {
            Ok(bytes)=>Response::builder().header(header::CONTENT_TYPE,"application/octet-stream").header(header::CACHE_CONTROL,"no-store").header("X-Content-Type-Options","nosniff").body(Body::from(bytes)).unwrap(),
            Err(_)=>unavailable(),
        }
    }).await.unwrap_or_else(|_|unavailable())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn refuses_paths_outside_attachments_and_symlink_escapes() {
        let base = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        let root = base.join("Attachments");
        std::fs::create_dir_all(&root).unwrap();
        let outside = base.join("secret");
        std::fs::write(&outside, b"private").unwrap();
        assert!(read_scoped(&root, outside.to_str().unwrap()).is_err());
        let link = root.join("escape");
        std::os::unix::fs::symlink(&outside, &link).unwrap();
        assert!(read_scoped(&root, link.to_str().unwrap()).is_err());
        let image = root.join("photo");
        std::fs::write(&image, b"photo").unwrap();
        assert_eq!(
            read_scoped(&root, image.to_str().unwrap()).unwrap(),
            b"photo"
        );
        std::fs::remove_dir_all(base).unwrap();
    }
}
