//! Read-only introspection of chat.db, exposed at `GET /debug/chatdb`. Used to
//! understand the schema + message lifecycle while designing send↔status
//! correlation. Text/body contents are redacted to lengths only.

use rusqlite::{Connection, OptionalExtension};
use serde_json::{json, Map, Value};
use std::path::Path;

pub fn inspect(chatdb: &Path) -> anyhow::Result<Value> {
    let conn = crate::receive::open_reader(chatdb)?;

    // CREATE statements for the tables that make up a message's identity/graph.
    let tables = [
        "message",
        "handle",
        "chat",
        "chat_message_join",
        "chat_handle_join",
        "attachment",
        "message_attachment_join",
    ];
    let mut schemas = Map::new();
    for t in tables {
        let sql: Option<String> = conn
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name=?1",
                [t],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(s) = sql {
            schemas.insert(t.to_string(), json!(s));
        }
    }

    let mut table_columns = Map::new();
    for t in tables {
        table_columns.insert(t.to_string(), json!(columns_for(&conn, t)?));
    }

    // Recent rows showing the lifecycle fields (contents redacted to lengths).
    let mut recent = Vec::new();
    {
        let mut stmt = conn.prepare(
            "SELECT m.ROWID, m.guid, h.id, m.is_from_me, m.is_sent, m.is_delivered, \
                    m.is_read, m.date, m.date_delivered, m.date_read, \
                    length(m.text), length(m.attributedBody) \
             FROM message m LEFT JOIN handle h ON m.handle_id = h.ROWID \
             ORDER BY m.ROWID DESC LIMIT 10",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok(json!({
                "rowid": r.get::<_, i64>(0)?,
                "guid": r.get::<_, String>(1)?,
                "handle": handle_summary(r.get::<_, Option<String>>(2)?),
                "is_from_me": r.get::<_, i64>(3)?,
                "is_sent": r.get::<_, Option<i64>>(4)?,
                "is_delivered": r.get::<_, i64>(5)?,
                "is_read": r.get::<_, i64>(6)?,
                "date": r.get::<_, i64>(7)?,
                "date_delivered": r.get::<_, i64>(8)?,
                "date_read": r.get::<_, i64>(9)?,
                "text_len": r.get::<_, Option<i64>>(10)?,
                "body_len": r.get::<_, Option<i64>>(11)?,
            }))
        })?;
        for row in rows {
            recent.push(row?);
        }
    }

    Ok(json!({
        "schemas": schemas,
        "columns": table_columns,
        "recent": recent,
        "recent_messages": recent_messages(&conn)?,
        "recent_chats": recent_chats(&conn)?,
        "recent_attachments": recent_attachments(&conn)?,
    }))
}

fn columns_for(conn: &Connection, table: &str) -> anyhow::Result<Vec<Value>> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let rows = stmt.query_map([], |r| {
        Ok(json!({
            "name": r.get::<_, String>(1)?,
            "type": r.get::<_, String>(2)?,
        }))
    })?;

    let mut columns = Vec::new();
    for row in rows {
        columns.push(row?);
    }
    Ok(columns)
}

fn recent_messages(conn: &Connection) -> anyhow::Result<Vec<Value>> {
    let mut stmt = conn.prepare(
        "SELECT m.ROWID, m.guid, m.is_from_me, m.service,
                m.cache_has_attachments, length(m.text), length(m.attributedBody),
                group_concat(DISTINCT c.ROWID),
                group_concat(DISTINCT c.guid),
                count(DISTINCT a.ROWID)
         FROM message m
         LEFT JOIN chat_message_join cmj ON cmj.message_id = m.ROWID
         LEFT JOIN chat c ON c.ROWID = cmj.chat_id
         LEFT JOIN message_attachment_join maj ON maj.message_id = m.ROWID
         LEFT JOIN attachment a ON a.ROWID = maj.attachment_id
         GROUP BY m.ROWID, m.guid, m.is_from_me, m.service, m.cache_has_attachments,
                  m.text, m.attributedBody
         ORDER BY m.ROWID DESC
         LIMIT 20",
    )?;
    let rows = stmt.query_map([], |r| {
        Ok(json!({
            "rowid": r.get::<_, i64>(0)?,
            "guid": r.get::<_, String>(1)?,
            "is_from_me": r.get::<_, i64>(2)?,
            "service": r.get::<_, Option<String>>(3)?,
            "cache_has_attachments": r.get::<_, Option<i64>>(4)?,
            "text_len": r.get::<_, Option<i64>>(5)?,
            "body_len": r.get::<_, Option<i64>>(6)?,
            "chat_rowids": csv_i64(r.get::<_, Option<String>>(7)?),
            "chat_guids": chat_guid_summaries(csv_string(r.get::<_, Option<String>>(8)?)),
            "attachment_count": r.get::<_, i64>(9)?,
        }))
    })?;

    collect_json(rows)
}

fn recent_chats(conn: &Connection) -> anyhow::Result<Vec<Value>> {
    let mut stmt = conn.prepare(
        "WITH recent_edges AS (
             SELECT chat_id, message_id
             FROM chat_message_join
             ORDER BY message_id DESC
             LIMIT 500
         ),
         recent_chat AS (
             SELECT chat_id, max(message_id) AS last_message_rowid
             FROM recent_edges
             GROUP BY chat_id
             ORDER BY last_message_rowid DESC
             LIMIT 20
         )
         SELECT c.ROWID AS chat_rowid,
                    c.guid,
                    c.chat_identifier,
                    c.service_name,
                    c.display_name,
                    c.room_name,
                    (SELECT count(DISTINCT handle_id)
                     FROM chat_handle_join
                     WHERE chat_id = c.ROWID) AS participant_count,
                    rc.last_message_rowid,
                    (SELECT count(DISTINCT maj.attachment_id)
                     FROM recent_edges re
                     JOIN message_attachment_join maj ON maj.message_id = re.message_id
                     WHERE re.chat_id = c.ROWID) AS recent_attachment_count
             FROM chat c
             JOIN recent_chat rc ON rc.chat_id = c.ROWID
             ORDER BY rc.last_message_rowid DESC",
    )?;
    let rows = stmt.query_map([], |r| {
        Ok(json!({
            "chat_rowid": r.get::<_, i64>(0)?,
            "guid": chat_guid_summary(r.get::<_, Option<String>>(1)?),
            "chat_identifier_len": opt_len(r.get::<_, Option<String>>(2)?),
            "service_name": r.get::<_, Option<String>>(3)?,
            "display_name_len": opt_len(r.get::<_, Option<String>>(4)?),
            "room_name_len": opt_len(r.get::<_, Option<String>>(5)?),
            "participant_count": r.get::<_, i64>(6)?,
            "last_message_rowid": r.get::<_, Option<i64>>(7)?,
            "attachment_count": r.get::<_, i64>(8)?,
        }))
    })?;

    collect_json(rows)
}

fn recent_attachments(conn: &Connection) -> anyhow::Result<Vec<Value>> {
    let mut stmt = conn.prepare(
        "SELECT a.ROWID, a.guid, a.mime_type, a.transfer_name,
                a.total_bytes, a.is_outgoing, a.hide_attachment,
                group_concat(DISTINCT m.ROWID),
                group_concat(DISTINCT c.ROWID)
         FROM attachment a
         LEFT JOIN message_attachment_join maj ON maj.attachment_id = a.ROWID
         LEFT JOIN message m ON m.ROWID = maj.message_id
         LEFT JOIN chat_message_join cmj ON cmj.message_id = m.ROWID
         LEFT JOIN chat c ON c.ROWID = cmj.chat_id
         GROUP BY a.ROWID, a.guid, a.mime_type, a.transfer_name, a.total_bytes,
                  a.is_outgoing, a.hide_attachment
         ORDER BY a.ROWID DESC
         LIMIT 20",
    )?;
    let rows = stmt.query_map([], |r| {
        Ok(json!({
            "rowid": r.get::<_, i64>(0)?,
            "guid": r.get::<_, Option<String>>(1)?,
            "mime_type": r.get::<_, Option<String>>(2)?,
            "transfer_name_len": opt_len(r.get::<_, Option<String>>(3)?),
            "total_bytes": r.get::<_, Option<i64>>(4)?,
            "is_outgoing": r.get::<_, Option<i64>>(5)?,
            "hide_attachment": r.get::<_, Option<i64>>(6)?,
            "message_rowids": csv_i64(r.get::<_, Option<String>>(7)?),
            "chat_rowids": csv_i64(r.get::<_, Option<String>>(8)?),
        }))
    })?;

    collect_json(rows)
}

fn collect_json(
    rows: rusqlite::MappedRows<'_, impl FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<Value>>,
) -> anyhow::Result<Vec<Value>> {
    let mut values = Vec::new();
    for row in rows {
        values.push(row?);
    }
    Ok(values)
}

fn csv_string(csv: Option<String>) -> Vec<String> {
    csv.unwrap_or_default()
        .split(',')
        .filter(|s| !s.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn csv_i64(csv: Option<String>) -> Vec<i64> {
    csv_string(csv)
        .into_iter()
        .filter_map(|s| s.parse::<i64>().ok())
        .collect()
}

fn opt_len(value: Option<String>) -> Option<usize> {
    value.map(|s| s.chars().count())
}

fn handle_summary(handle: Option<String>) -> Option<Value> {
    handle.map(|h| {
        let kind = if h.starts_with('+') {
            "phone"
        } else if h.contains('@') {
            "email"
        } else {
            "other"
        };
        json!({
            "kind": kind,
            "len": h.chars().count(),
            "suffix": h.chars().rev().take(4).collect::<String>().chars().rev().collect::<String>(),
        })
    })
}

fn chat_guid_summaries(guids: Vec<String>) -> Vec<Value> {
    guids
        .into_iter()
        .map(|guid| chat_guid_summary(Some(guid)).unwrap_or(Value::Null))
        .collect()
}

fn chat_guid_summary(guid: Option<String>) -> Option<Value> {
    guid.map(|g| {
        let kind = if g.starts_with("any;+;") {
            "group"
        } else if g.starts_with("any;-;") {
            "direct"
        } else {
            "other"
        };
        let suffix_len = if kind == "group" { 8 } else { 4 };
        json!({
            "kind": kind,
            "len": g.chars().count(),
            "suffix": g.chars().rev().take(suffix_len).collect::<String>().chars().rev().collect::<String>(),
        })
    })
}
