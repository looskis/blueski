//! Config, state, and the fixed filesystem paths the daemon uses.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// launchd label / bundle id.
pub const LABEL: &str = "com.razteam.blueski";

fn home() -> PathBuf {
    PathBuf::from(std::env::var("HOME").expect("HOME not set"))
}

pub fn config_dir() -> PathBuf {
    home().join(".config/blueski")
}

pub fn config_path() -> PathBuf {
    config_dir().join("config.toml")
}

pub fn state_path() -> PathBuf {
    config_dir().join("state.json")
}

pub fn pid_path() -> PathBuf {
    config_dir().join("daemon.pid")
}

/// Correlation store (libSQL/Turso) — message_id <-> chat.db guid bindings.
pub fn store_path() -> PathBuf {
    config_dir().join("state.db")
}

pub fn chatdb_path() -> PathBuf {
    home().join("Library/Messages/chat.db")
}

pub fn chatdb_wal_path() -> PathBuf {
    home().join("Library/Messages/chat.db-wal")
}

pub fn plist_path() -> PathBuf {
    home().join(format!("Library/LaunchAgents/{LABEL}.plist"))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Loopback port for the control socket.
    pub port: u16,
    /// Customer webhook sink. When absent, events are logged only.
    pub webhook_url: Option<String>,
    /// Shared secret used to sign webhook payloads (HMAC-SHA256).
    pub hmac_secret: String,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            port: 8787,
            webhook_url: None,
            hmac_secret: uuid::Uuid::new_v4().to_string(),
        }
    }
}

impl Config {
    /// Load config, creating a default (with a fresh hmac secret) on first run.
    pub fn load_or_init() -> Result<Config> {
        let path = config_path();
        if path.exists() {
            let raw = std::fs::read_to_string(&path)
                .with_context(|| format!("reading {}", path.display()))?;
            Ok(toml::from_str(&raw)?)
        } else {
            let cfg = Config::default();
            std::fs::create_dir_all(config_dir())?;
            std::fs::write(&path, toml::to_string_pretty(&cfg)?)?;
            tracing::info!(path = %path.display(), "wrote default config");
            Ok(cfg)
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct State {
    /// Highest `message.ROWID` we've already processed.
    pub last_seen: i64,
}

impl State {
    pub fn load() -> State {
        std::fs::read_to_string(state_path())
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default()
    }

    pub fn save(&self) {
        if let Err(e) = std::fs::create_dir_all(config_dir()).and_then(|_| {
            std::fs::write(state_path(), serde_json::to_vec(self).unwrap_or_default())
        }) {
            tracing::warn!(error = %e, "failed to persist state");
        }
    }
}
