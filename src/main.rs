//! Blueski — a single-process macOS LaunchAgent that turns a Mac into
//! an iMessage send/receive node. See README.md for the spec.

mod config;
mod debug;
mod launchd;
mod model;
mod permissions;
mod receive;
mod send_worker;
mod sender;
mod server;
mod store;
mod walwatch;
mod webhook;

use anyhow::Result;
use clap::{Parser, Subcommand};
use config::Config;
use server::AppState;
use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Parser)]
#[command(name = "blueski", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Send a message via the local daemon (auto-starts it if needed).
    Send {
        /// Recipient handle — E.164 phone or iMessage email.
        #[arg(long)]
        to: Option<String>,
        /// Message body.
        #[arg(long)]
        text: Option<String>,
        /// Messaging protocol: imessage | sms (more to come).
        #[arg(long, default_value = "imessage")]
        protocol: String,
        /// Correlation id echoed back in webhooks.
        #[arg(long)]
        client_ref: Option<String>,
        /// Positional fallback: <TO> <TEXT> (used if --to/--text are omitted).
        #[arg(value_names = ["TO", "TEXT"])]
        positional: Vec<String>,
    },
    /// Read the local durable event journal as newline-delimited JSON.
    Events {
        /// Return events with durable cursor id greater than this value.
        #[arg(long, default_value_t = 0)]
        since: i64,
        /// Maximum events to return for non-follow queries.
        #[arg(long)]
        limit: Option<u64>,
        /// Keep streaming events after the initial backlog.
        #[arg(long)]
        follow: bool,
    },
    /// Ask the OS supervisor to bring the daemon online and report status.
    Up,
    /// Stop and unload the active OS-supervised daemon.
    Down,
    /// Report cached daemon, service, and permission state.
    Status,
    /// Run expensive live permission and service diagnostics.
    Doctor,
    /// Publish through a supervised ngrok tunnel with bearer authentication.
    Publish {
        /// Reserved ngrok hostname (for example blueski.example.com).
        #[arg(long)]
        domain: String,
    },
    /// Stop and remove the public tunnel and its API bearer token.
    Unpublish,
    /// Interactively grant macOS permissions (Full Disk Access, Automation).
    Setup {
        /// Internal marker set when setup is relaunched through LaunchServices.
        #[arg(long, hide = true)]
        app_launched: bool,
    },
    /// Install + load the LaunchAgent (persistent, starts at login).
    Install,
    /// Unload + remove the LaunchAgent.
    Uninstall,
    /// Run the daemon in the foreground (launchd / `up` invoke this).
    Run,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Send {
            to,
            text,
            protocol,
            client_ref,
            positional,
        } => send(to, text, protocol, client_ref, positional),
        Command::Events {
            since,
            limit,
            follow,
        } => events(since, limit, follow),
        Command::Up => up(),
        Command::Down => down(),
        Command::Status => status(),
        Command::Doctor => doctor(),
        Command::Publish { domain } => publish(domain),
        Command::Unpublish => unpublish(),
        Command::Setup { app_launched } => setup(app_launched),
        Command::Install => launchd::install(),
        Command::Uninstall => launchd::uninstall(),
        Command::Run => run(),
    }
}

/// `events`: agent-friendly JSONL reader over the local durable event journal.
fn events(since: i64, limit: Option<u64>, follow: bool) -> Result<()> {
    let config = Config::load_or_init()?;
    ensure_running(&config)?;

    if follow {
        let url = format!(
            "http://127.0.0.1:{}/events/stream?since={since}",
            config.port
        );
        stream_jsonl(&url, config.api_token.as_deref())?;
        return Ok(());
    }

    let mut url = format!("http://127.0.0.1:{}/events?since={since}", config.port);
    if let Some(limit) = limit {
        url.push_str(&format!("&limit={limit}"));
    }
    let body = get_text(&url, None, config.api_token.as_deref())?;
    let events: Vec<serde_json::Value> = serde_json::from_str(&body)?;
    for event in events {
        println!("{}", serde_json::to_string(&event)?);
    }
    Ok(())
}

/// `send`: thin client over the control socket. Ensures the daemon is up, then
/// POSTs the message. The actual send (queue, AppleScript, webhooks) all happens
/// in the daemon — this is just a convenient front door for agents.
fn send(
    to: Option<String>,
    text: Option<String>,
    protocol: String,
    client_ref: Option<String>,
    positional: Vec<String>,
) -> Result<()> {
    let to = to.or_else(|| positional.first().cloned());
    let text = text.or_else(|| positional.get(1).cloned());
    let (to, text) = match (to, text) {
        (Some(to), Some(text)) => (to, text),
        _ => anyhow::bail!(
            "usage: blueski send --to <handle> --text <message> [--protocol imessage]"
        ),
    };

    let config = Config::load_or_init()?;
    ensure_running(&config)?;

    let body = serde_json::json!({
        "to": to,
        "text": text,
        "protocol": protocol,
        "client_ref": client_ref,
    });
    let resp = post_json(
        &format!("http://127.0.0.1:{}/messages", config.port),
        body,
        config.api_token.as_deref(),
    )?;
    print_status(&resp);
    Ok(())
}

/// One-command onboarding: install/repair launchd, start the daemon, complete
/// the unavoidable TCC grants once, then print a machine-readable readiness
/// document. The app-bundle hop gives macOS a stable authorization identity.
fn setup(app_launched: bool) -> Result<()> {
    if app_launched {
        permissions::run_setup(&config::chatdb_path());
        return Ok(());
    }

    let config = Config::load_or_init()?;
    launchd::install()?;
    ensure_running(&config)?;
    if !permissions::relaunch_setup_in_app()? {
        permissions::run_setup(&config::chatdb_path());
    }

    // The receive worker exits when chat.db is unreadable. Restart only after
    // the TCC grants land so inbound monitoring starts with Full Disk Access.
    launchd::start_supervised()?;

    let status_url = format!("http://127.0.0.1:{}/status", config.port);
    for _ in 0..20 {
        if let Ok(body) = get_text(
            &status_url,
            Some(Duration::from_secs(2)),
            config.api_token.as_deref(),
        ) {
            let value: serde_json::Value = serde_json::from_str(&body)?;
            let ready = value["permissions"]["full_disk_access"] == true
                && value["permissions"]["automation"] == true;
            if ready {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "product": "blueski",
                        "ready": true,
                        "port": config.port,
                        "supervisor": launchd::active_label()?,
                        "permissions": value["permissions"],
                    }))?
                );
                return Ok(());
            }
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    anyhow::bail!("macOS grants completed, but the daemon did not report readiness");
}

/// `up`: ensure the listener is online, then print status. Idempotent.
fn up() -> Result<()> {
    let config = Config::load_or_init()?;
    let was_up = probe(&config)?.is_some();

    launchd::install()?;
    ensure_running(&config)?;

    let body = get_text(
        &format!("http://127.0.0.1:{}/status", config.port),
        Some(Duration::from_secs(2)),
        config.api_token.as_deref(),
    )?;
    println!(
        "blueski {} on 127.0.0.1:{}",
        if was_up { "already up" } else { "up" },
        config.port
    );
    print_status(&body);
    Ok(())
}

/// Ensure launchd owns a healthy daemon. The CLI never forks or daemonizes.
fn ensure_running(config: &Config) -> Result<()> {
    if probe(config)?.is_some() {
        return Ok(());
    }
    launchd::start_supervised()?;

    for _ in 0..40 {
        std::thread::sleep(Duration::from_millis(250));
        if probe(config)?.is_some() {
            return Ok(());
        }
    }
    anyhow::bail!(
        "supervised daemon did not become healthy — see {}",
        config::stderr_log_path().display()
    );
}

/// `down`: unload whichever launchd supervisor currently owns Blueski.
fn down() -> Result<()> {
    if launchd::stop_tunnel()? {
        println!("stopped supervised Blueski tunnel");
    }
    let stopped = launchd::stop_active()?;
    if stopped.is_empty() {
        println!("Blueski has no loaded supervisor");
    } else {
        for label in stopped {
            println!("stopped supervised Blueski ({label})");
        }
    }
    Ok(())
}

fn publish(domain: String) -> Result<()> {
    let mut config = Config::load_or_init()?;
    let token = config.api_token.clone().unwrap_or_else(|| {
        format!(
            "bs_{}{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        )
    });
    config.api_token = Some(token.clone());
    config.save()?;

    launchd::install()?;
    launchd::start_supervised()?;
    let mut authenticated = false;
    for _ in 0..40 {
        std::thread::sleep(Duration::from_millis(250));
        if let Some(body) = probe(&config)? {
            if serde_json::from_str::<serde_json::Value>(&body)?["authentication_required"] == true
            {
                authenticated = true;
                break;
            }
        }
    }
    if !authenticated {
        anyhow::bail!("daemon restarted, but did not enable API authentication");
    }
    launchd::install_tunnel(&domain, config.port)?;
    let hostname = domain
        .strip_prefix("https://")
        .or_else(|| domain.strip_prefix("http://"))
        .unwrap_or(&domain)
        .trim_end_matches('/');
    let public_url = format!("https://{hostname}");
    let mut tunnel_ready = false;
    for _ in 0..40 {
        if let Ok(body) = get_text(
            "http://127.0.0.1:4040/api/tunnels",
            Some(Duration::from_millis(500)),
            None,
        ) {
            let state: serde_json::Value = serde_json::from_str(&body)?;
            tunnel_ready = state["tunnels"]
                .as_array()
                .is_some_and(|tunnels| tunnels.iter().any(|t| t["public_url"] == public_url));
            if tunnel_ready {
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    if !tunnel_ready {
        anyhow::bail!(
            "ngrok did not publish {public_url}; see {}",
            config::tunnel_stderr_log_path().display()
        );
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "product": "blueski",
            "published": true,
            "url": public_url,
            "authorization": format!("Bearer {token}"),
            "supervisor": "com.looskis.blueski.ngrok",
        }))?
    );
    Ok(())
}

fn unpublish() -> Result<()> {
    let mut config = Config::load_or_init()?;
    launchd::uninstall_tunnel()?;
    config.api_token = None;
    config.save()?;
    if launchd::active_label()?.is_some() {
        launchd::start_supervised()?;
    }
    println!(r#"{{"product":"blueski","published":false}}"#);
    Ok(())
}

/// `run` is the only async entrypoint; the rest are synchronous CLI actions.
fn run() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    // Multi-threaded (small pool): libSQL needs its background work driven
    // continuously, which a current-thread runtime can't guarantee from the
    // sync receive thread.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    runtime.block_on(async_run())
}

async fn async_run() -> Result<()> {
    let config = Arc::new(Config::load_or_init()?);
    let chatdb = config::chatdb_path();

    // Record our pid so `down` can find us; clear it on exit.
    let _ = std::fs::write(config::pid_path(), std::process::id().to_string());

    // Correlation store (Turso/libSQL) binding our message_id <-> chat.db guid.
    let store = store::Store::open(&config::store_path().to_string_lossy()).await?;
    let (event_tx, _) = tokio::sync::broadcast::channel(1024);

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    let sink = webhook::spawn(
        client,
        config.webhook_url.clone(),
        config.hmac_secret.clone(),
        store.clone(),
        event_tx.clone(),
    );

    // Shared chat.db ROWID watermark: receive worker keeps it current, send
    // worker reads it as the pre-send lower bound for claim-by-watermark.
    let max_rowid = std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0));

    // Send path: control socket -> in-memory queue -> AppleScript -> Messages.
    let (send_tx, send_rx) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(send_worker::run(
        send_rx,
        sender::AppleScriptSender,
        sink.clone(),
        store.clone(),
        max_rowid.clone(),
        config::chatdb_path(),
    ));

    // Correlator task (on this runtime) owns the store; the receive thread talks
    // to it over a channel so libSQL is never touched from that sync thread.
    let corr_tx = store::spawn_correlator(store.clone(), sink.clone());

    // Receive path: kqueue on chat.db-wal -> chat.db reader -> webhooks.
    receive::spawn(
        chatdb.clone(),
        config::chatdb_wal_path(),
        sink.clone(),
        corr_tx,
        max_rowid.clone(),
    );

    let state = AppState {
        send_tx,
        start: Instant::now(),
        chatdb,
        config: config.clone(),
        store: store.clone(),
        events: event_tx,
        permissions: permissions::PermissionState::default(),
    };

    permissions::spawn_refresh(state.permissions.clone(), state.chatdb.clone());

    let listener = tokio::net::TcpListener::bind(("127.0.0.1", config.port)).await?;
    tracing::info!(port = config.port, "control socket listening on loopback");

    axum::serve(listener, server::router(state))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("shutting down");
        })
        .await?;

    let _ = std::fs::remove_file(config::pid_path());
    Ok(())
}

fn get_text(url: &str, timeout: Option<Duration>, token: Option<&str>) -> Result<String> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    rt.block_on(async {
        let mut builder = reqwest::Client::builder();
        if let Some(timeout) = timeout {
            builder = builder.timeout(timeout);
        }
        let client = builder.build()?;
        let mut request = client.get(url);
        if let Some(token) = token {
            request = request.bearer_auth(token);
        }
        let resp = request.send().await?;
        let status = resp.status();
        let text = resp.text().await?;
        if !status.is_success() {
            anyhow::bail!("GET {url} returned {status}: {text}");
        }
        Ok(text)
    })
}

fn stream_jsonl(url: &str, token: Option<&str>) -> Result<()> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    rt.block_on(async {
        let client = reqwest::Client::builder().build()?;
        let mut request = client.get(url);
        if let Some(token) = token {
            request = request.bearer_auth(token);
        }
        let mut resp = request.send().await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("GET {url} returned {status}: {text}");
        }
        while let Some(chunk) = resp.chunk().await? {
            std::io::stdout().write_all(&chunk)?;
            std::io::stdout().flush()?;
        }
        Ok(())
    })
}

/// `status`: self-heal the supervised service, then read its cached state.
fn status() -> Result<()> {
    let config = Config::load_or_init()?;
    ensure_running(&config)?;
    let body = get_text(
        &format!("http://127.0.0.1:{}/status", config.port),
        Some(Duration::from_secs(2)),
        config.api_token.as_deref(),
    )?;
    print_status(&body);
    Ok(())
}

/// Expensive diagnostics are deliberately separate from request-time status.
fn doctor() -> Result<()> {
    let config = Config::load_or_init()?;
    ensure_running(&config)?;
    let report = serde_json::json!({
        "product": "blueski",
        "healthy": probe(&config)?.is_some(),
        "supervisor": launchd::active_label()?,
        "port": config.port,
        "config_path": config::config_path().display().to_string(),
        "logs": {
            "stdout": config::stdout_log_path().display().to_string(),
            "stderr": config::stderr_log_path().display().to_string(),
        },
        "permissions": {
            "full_disk_access": permissions::has_full_disk_access(&config::chatdb_path()),
            "automation": permissions::has_automation(),
        }
    });
    print_status(&serde_json::to_string(&report)?);
    Ok(())
}

/// Fast identity probe. `/status` is used only for compatibility with a daemon
/// from before `/healthz` existed during an in-place upgrade.
fn probe(config: &Config) -> Result<Option<String>> {
    let health_url = format!("http://127.0.0.1:{}/healthz", config.port);
    match probe_url(&health_url, config.api_token.as_deref()) {
        Some((200, body)) => {
            if is_blueski_health(&body) {
                return Ok(Some(body));
            }
            anyhow::bail!("port {} is occupied by another service", config.port);
        }
        Some((404, _)) => {}
        Some(_) => return Ok(None),
        None => return Ok(None),
    }

    let status_url = format!("http://127.0.0.1:{}/status", config.port);
    match probe_url(&status_url, config.api_token.as_deref()) {
        Some((200, body)) if is_blueski_status(&body) => Ok(Some(body)),
        Some((200, _)) => anyhow::bail!("port {} is occupied by another service", config.port),
        _ => Ok(None),
    }
}

fn probe_url(url: &str, token: Option<&str>) -> Option<(u16, String)> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .ok()?;
    rt.block_on(async {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(800))
            .build()
            .ok()?;
        let mut request = client.get(url);
        if let Some(token) = token {
            request = request.bearer_auth(token);
        }
        let resp = request.send().await.ok()?;
        let status = resp.status().as_u16();
        Some((status, resp.text().await.ok()?))
    })
}

fn is_blueski_health(body: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(body)
        .ok()
        .and_then(|value| value.get("product")?.as_str().map(str::to_owned))
        .as_deref()
        == Some("blueski")
}

fn is_blueski_status(body: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(body).is_ok_and(|status| {
        status.get("product").and_then(|value| value.as_str()) == Some("blueski")
            || status.get("transport").and_then(|value| value.as_str()) == Some("applescript")
    })
}

/// Blocking POST of a JSON body to the control socket; returns the response.
fn post_json(url: &str, body: serde_json::Value, token: Option<&str>) -> Result<String> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    rt.block_on(async {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()?;
        let mut request = client.post(url).json(&body);
        if let Some(token) = token {
            request = request.bearer_auth(token);
        }
        let resp = request.send().await?;
        let status = resp.status();
        Ok(resp.text().await?).and_then(|text| http_success_or_error("POST", url, status, text))
    })
}

fn http_success_or_error(
    method: &str,
    url: &str,
    status: reqwest::StatusCode,
    text: String,
) -> Result<String> {
    if status.is_success() {
        Ok(text)
    } else {
        anyhow::bail!("{method} {url} returned {status}: {text}");
    }
}

fn print_status(body: &str) {
    match serde_json::from_str::<serde_json::Value>(body) {
        Ok(v) => println!(
            "{}",
            serde_json::to_string_pretty(&v).unwrap_or_else(|_| body.into())
        ),
        Err(_) => println!("{body}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::StatusCode;

    #[test]
    fn http_success_or_error_rejects_non_2xx() {
        let err = http_success_or_error(
            "POST",
            "http://127.0.0.1:8788/messages",
            StatusCode::SERVICE_UNAVAILABLE,
            r#"{"error":"send worker unavailable"}"#.to_string(),
        )
        .unwrap_err()
        .to_string();

        assert!(err.contains("503 Service Unavailable"));
        assert!(err.contains("send worker unavailable"));
    }

    #[test]
    fn http_success_or_error_returns_success_body() {
        let body = http_success_or_error(
            "POST",
            "http://127.0.0.1:8788/messages",
            StatusCode::ACCEPTED,
            r#"{"status":"queued"}"#.to_string(),
        )
        .unwrap();

        assert_eq!(body, r#"{"status":"queued"}"#);
    }

    #[test]
    fn status_probe_rejects_another_daemon_on_the_same_port() {
        assert!(is_blueski_status(
            r#"{"status":"ok","transport":"applescript"}"#
        ));
        assert!(!is_blueski_status(r#"{"status":"ok","version":"0.1.0"}"#));
        assert!(is_blueski_health(r#"{"status":"ok","product":"blueski"}"#));
        assert!(!is_blueski_health(
            r#"{"status":"ok","product":"greenski"}"#
        ));
    }
}
