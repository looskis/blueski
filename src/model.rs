//! Shared types: the send job that flows through the in-memory queue, the
//! inbound HTTP request shape, and the common webhook event envelope.

use serde::{Deserialize, Serialize};

fn default_protocol() -> String {
    "imessage".to_string()
}

/// Body of `POST /messages`. `protocol` is the messaging transport (today
/// `imessage` | `sms`; more to come). `service` is accepted as a legacy alias.
#[derive(Debug, Deserialize)]
pub struct SendRequest {
    pub to: Option<String>,
    pub chat_id: Option<String>,
    pub text: String,
    #[serde(alias = "service", default = "default_protocol")]
    pub protocol: String,
    pub client_ref: Option<String>,
}

#[derive(Debug, Clone)]
pub enum SendTarget {
    Handle { to: String },
    Chat { chat_id: String },
}

impl SendTarget {
    pub fn store_key(&self) -> &str {
        match self {
            SendTarget::Handle { to } => to,
            SendTarget::Chat { chat_id } => chat_id,
        }
    }

    pub fn event_handle(&self) -> Option<String> {
        match self {
            SendTarget::Handle { to } => Some(to.clone()),
            SendTarget::Chat { .. } => None,
        }
    }
}

/// A unit of work handed from the control socket to the send worker over the
/// in-process channel. Not an IPC boundary — same process.
#[derive(Debug, Clone)]
pub struct SendJob {
    pub message_id: String,
    pub target: SendTarget,
    pub text: String,
    pub protocol: String,
    pub client_ref: Option<String>,
}

/// The common webhook envelope. Optional fields are omitted when empty so each
/// event type only carries what's relevant to it.
#[derive(Debug, Clone, Serialize)]
pub struct Event {
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
}

impl Event {
    pub fn new(event: &str, message_id: String) -> Self {
        Event {
            event: event.to_string(),
            message_id,
            client_ref: None,
            handle: None,
            text: None,
            protocol: None,
            status: None,
            reason: None,
            timestamp: now_iso(),
        }
    }
}

/// Current time as an RFC3339 string (second precision, `Z`).
pub fn now_iso() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

/// Convert an Apple-epoch timestamp from `chat.db` to RFC3339.
///
/// **(to verify)** — modern macOS appears to store these as nanoseconds since
/// 2001-01-01 UTC. We guard the obvious failure modes; exact unit per macOS
/// version is still to be confirmed empirically (see README).
pub fn apple_time_to_iso(raw: i64) -> Option<String> {
    if raw == 0 {
        return None;
    }
    // Heuristic: nanoseconds if the value is large, else seconds.
    let secs = if raw > 1_000_000_000_000 {
        raw / 1_000_000_000
    } else {
        raw
    } + 978_307_200; // 2001-01-01 -> 1970-01-01 offset
    chrono::DateTime::from_timestamp(secs, 0)
        .map(|dt| dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
}
