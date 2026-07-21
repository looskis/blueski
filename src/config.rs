//! Config, state, and the fixed filesystem paths the daemon uses.

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::net::IpAddr;
use std::path::PathBuf;

pub const MAX_WEBHOOKS: usize = 16;

fn default_true() -> bool {
    true
}

fn new_installation_id() -> String {
    format!("bsinst_{}", uuid::Uuid::new_v4().simple())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WebhookConfig {
    pub id: String,
    pub url: String,
    pub secret: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

/// launchd label / bundle id.
pub const LABEL: &str = "com.looskis.blueski";

fn home() -> PathBuf {
    PathBuf::from(std::env::var("HOME").expect("HOME not set"))
}

pub fn config_dir() -> PathBuf {
    if let Some(path) = std::env::var_os("BLUESKI_CONFIG_DIR").filter(|path| !path.is_empty()) {
        return PathBuf::from(path);
    }
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
    /// Stable identity for cursors and idempotency keys created by this node.
    #[serde(default = "new_installation_id")]
    pub installation_id: String,
    /// Loopback port for the control socket.
    pub port: u16,
    /// Independently signed webhook destinations.
    #[serde(default)]
    pub webhooks: Vec<WebhookConfig>,
    /// Legacy single webhook sink. Retained when loading old installations.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub webhook_url: Option<String>,
    /// Legacy single-destination HMAC secret.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hmac_secret: Option<String>,
    /// Bearer token required by the HTTP API when public publishing is enabled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_token: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            installation_id: new_installation_id(),
            port: 8788,
            webhooks: Vec::new(),
            webhook_url: None,
            hmac_secret: None,
            api_token: None,
        }
    }
}

impl Config {
    /// Load config, creating a default with a stable installation ID on first run.
    pub fn load_or_init() -> Result<Config> {
        let path = config_path();
        let (mut config, needs_save) = if path.exists() {
            let raw = std::fs::read_to_string(&path)
                .with_context(|| format!("reading {}", path.display()))?;
            let needs_save = !raw
                .parse::<toml::Value>()?
                .as_table()
                .is_some_and(|table| table.contains_key("installation_id"));
            let config: Config = toml::from_str(&raw)?;
            secure_config_file(&path)?;
            (config, needs_save)
        } else {
            let cfg = Config::default();
            std::fs::create_dir_all(config_dir())?;
            cfg.save()?;
            tracing::info!(path = %path.display(), "wrote default config");
            (cfg, false)
        };
        config.validate()?;
        if needs_save {
            config.save()?;
            tracing::info!(path = %path.display(), "persisted stable installation id");
        }
        if let Some(port) = std::env::var_os("BLUESKI_PORT") {
            config.port = port
                .to_string_lossy()
                .parse()
                .context("BLUESKI_PORT must be a valid TCP port")?;
        }
        Ok(config)
    }

    /// Resolve the legacy single-destination form into the runtime model.
    pub fn effective_webhooks(&self) -> Result<Vec<WebhookConfig>> {
        if self.webhook_url.is_some() && !self.webhooks.is_empty() {
            bail!("configure either legacy webhook_url or [[webhooks]], not both");
        }
        if !self.webhooks.is_empty() {
            return Ok(self.webhooks.clone());
        }
        match self.webhook_url.as_ref() {
            Some(url) => {
                let secret = self
                    .hmac_secret
                    .as_ref()
                    .filter(|secret| !secret.is_empty())
                    .context("legacy webhook_url requires a nonempty hmac_secret")?;
                Ok(vec![WebhookConfig {
                    id: "legacy".to_string(),
                    url: url.clone(),
                    secret: secret.clone(),
                    enabled: true,
                }])
            }
            None => Ok(Vec::new()),
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.installation_id.trim().is_empty() {
            bail!("installation_id must not be empty");
        }
        if self.webhooks.len() > MAX_WEBHOOKS {
            bail!("at most {MAX_WEBHOOKS} webhook destinations may be configured");
        }

        let webhooks = self.effective_webhooks()?;
        let mut ids = HashSet::with_capacity(webhooks.len());
        let mut secrets = HashSet::with_capacity(webhooks.len());
        for webhook in &webhooks {
            if webhook.id.trim().is_empty() {
                bail!("webhook id must not be empty");
            }
            if !ids.insert(webhook.id.as_str()) {
                bail!("duplicate webhook id: {}", webhook.id);
            }
            if webhook.secret.trim().is_empty() {
                bail!("webhook {} has an empty secret", webhook.id);
            }
            if !secrets.insert(webhook.secret.as_str()) {
                bail!("webhook destinations must use different secrets");
            }
            validate_webhook_url(&webhook.id, &webhook.url)?;
        }
        Ok(())
    }

    pub fn save(&self) -> Result<()> {
        let path = config_path();
        std::fs::create_dir_all(config_dir())?;
        std::fs::write(&path, toml::to_string_pretty(self)?)?;
        secure_config_file(&path)?;
        Ok(())
    }
}

fn validate_webhook_url(id: &str, raw: &str) -> Result<()> {
    let url =
        reqwest::Url::parse(raw).with_context(|| format!("webhook {id} has an invalid URL"))?;
    if !url.username().is_empty() || url.password().is_some() {
        bail!("webhook {id} URL must not contain credentials");
    }
    let host = url
        .host_str()
        .with_context(|| format!("webhook {id} URL must include a host"))?;
    match url.scheme() {
        "https" => Ok(()),
        "http" if is_loopback_host(host) => Ok(()),
        "http" => bail!("webhook {id} must use HTTPS unless its host is loopback"),
        _ => bail!("webhook {id} URL must use HTTP or HTTPS"),
    }
}

fn is_loopback_host(host: &str) -> bool {
    let host = host.trim_start_matches('[').trim_end_matches(']');
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn webhook(id: &str, url: &str, secret: &str) -> WebhookConfig {
        WebhookConfig {
            id: id.to_string(),
            url: url.to_string(),
            secret: secret.to_string(),
            enabled: true,
        }
    }

    #[test]
    fn legacy_configuration_produces_one_effective_destination() {
        let config = Config {
            webhook_url: Some("http://127.0.0.1:3001/events".to_string()),
            hmac_secret: Some("legacy-secret".to_string()),
            ..Config::default()
        };
        let effective = config.effective_webhooks().unwrap();
        assert_eq!(effective.len(), 1);
        assert_eq!(effective[0].id, "legacy");
        assert_eq!(effective[0].secret, "legacy-secret");
        config.validate().unwrap();
    }

    #[test]
    fn legacy_fields_survive_a_configuration_round_trip() {
        let legacy = r#"
port = 8788
webhook_url = "http://127.0.0.1:3001/events"
hmac_secret = "legacy-secret"
"#;
        let config: Config = toml::from_str(legacy).unwrap();
        assert!(config.installation_id.starts_with("bsinst_"));
        let serialized = toml::to_string_pretty(&config).unwrap();
        assert!(serialized.contains("webhook_url"));
        assert!(serialized.contains("hmac_secret"));
        assert!(serialized.contains("installation_id"));
    }

    #[test]
    fn new_configuration_uses_only_the_multi_webhook_schema() {
        let serialized = toml::to_string_pretty(&Config::default()).unwrap();
        assert!(serialized.contains("webhooks = []"));
        assert!(!serialized.contains("webhook_url"));
        assert!(!serialized.contains("hmac_secret"));
    }

    #[test]
    fn mixed_legacy_and_new_configuration_is_rejected() {
        let config = Config {
            webhook_url: Some("http://127.0.0.1:3001/events".to_string()),
            hmac_secret: Some("legacy-secret".to_string()),
            webhooks: vec![webhook(
                "new",
                "https://events.example.com/blueski",
                "new-secret",
            )],
            ..Config::default()
        };
        assert!(config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("either"));
    }

    #[test]
    fn duplicate_ids_secrets_and_insecure_remote_urls_are_rejected() {
        let duplicate_ids = Config {
            webhooks: vec![
                webhook("same", "https://one.example/events", "one"),
                webhook("same", "https://two.example/events", "two"),
            ],
            ..Config::default()
        };
        assert!(duplicate_ids.validate().is_err());

        let duplicate_secrets = Config {
            webhooks: vec![
                webhook("one", "https://one.example/events", "shared"),
                webhook("two", "https://two.example/events", "shared"),
            ],
            ..Config::default()
        };
        assert!(duplicate_secrets.validate().is_err());

        let insecure_remote = Config {
            webhooks: vec![webhook(
                "remote",
                "http://events.example.com/blueski",
                "secret",
            )],
            ..Config::default()
        };
        assert!(insecure_remote.validate().is_err());

        let invalid_url = Config {
            webhooks: vec![webhook("invalid", "not a URL", "secret")],
            ..Config::default()
        };
        assert!(invalid_url.validate().is_err());
    }

    #[test]
    fn loopback_http_and_remote_https_are_valid() {
        let config = Config {
            webhooks: vec![
                webhook("local", "http://[::1]:3001/events", "local-secret"),
                webhook(
                    "remote",
                    "https://events.example.com/blueski",
                    "remote-secret",
                ),
            ],
            ..Config::default()
        };
        config.validate().unwrap();
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

    pub fn save(&self) -> Result<()> {
        use std::io::Write;

        let dir = config_dir();
        std::fs::create_dir_all(&dir)?;
        let path = state_path();
        let temp = dir.join(format!("state.json.{}.tmp", std::process::id()));
        let mut file = std::fs::File::create(&temp)?;
        file.write_all(&serde_json::to_vec(self)?)?;
        file.sync_all()?;
        std::fs::rename(&temp, &path)?;
        if let Ok(dir_file) = std::fs::File::open(&dir) {
            let _ = dir_file.sync_all();
        }
        Ok(())
    }
}
