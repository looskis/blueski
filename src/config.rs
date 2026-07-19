//! Config, state, and the fixed filesystem paths the daemon uses.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// launchd label / bundle id.
pub const LABEL: &str = "com.looskis.blueski";

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

pub fn stdout_log_path() -> PathBuf {
    config_dir().join("blueski.log")
}

pub fn stderr_log_path() -> PathBuf {
    config_dir().join("blueski.err.log")
}

pub fn tunnel_stdout_log_path() -> PathBuf {
    config_dir().join("ngrok.log")
}

pub fn tunnel_stderr_log_path() -> PathBuf {
    config_dir().join("ngrok.err.log")
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

pub fn tunnel_plist_path() -> PathBuf {
    home().join(format!("Library/LaunchAgents/{LABEL}.ngrok.plist"))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Loopback port for the control socket.
    pub port: u16,
    /// Customer webhook sink. When absent, events are logged only.
    pub webhook_url: Option<String>,
    /// Shared secret used to sign webhook payloads (HMAC-SHA256).
    pub hmac_secret: String,
    /// Bearer token required by the HTTP API when public publishing is enabled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_token: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            port: 8788,
            webhook_url: None,
            hmac_secret: uuid::Uuid::new_v4().to_string(),
            api_token: None,
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
            let config = toml::from_str(&raw)?;
            secure_config_file(&path)?;
            Ok(config)
        } else {
            let cfg = Config::default();
            std::fs::create_dir_all(config_dir())?;
            cfg.save()?;
            tracing::info!(path = %path.display(), "wrote default config");
            Ok(cfg)
        }
    }

    pub fn save(&self) -> Result<()> {
        let path = config_path();
        std::fs::create_dir_all(config_dir())?;
        std::fs::write(&path, toml::to_string_pretty(self)?)?;
        secure_config_file(&path)?;
        Ok(())
    }
}

fn secure_config_file(path: &std::path::Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
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
