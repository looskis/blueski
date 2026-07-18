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
use std::process::Stdio;
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
    /// Bring the daemon online (detached) and report status. Idempotent — if
    /// already running, just reports status. This is the agent entrypoint.
    Up,
    /// Stop a daemon started with `up`.
    Down,
    /// Report health + live state + TCC permission state.
    Status,
    /// Interactively grant macOS permissions (Full Disk Access, Automation).
    Setup,
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
        Command::Setup => {
            permissions::run_setup(&config::chatdb_path());
            Ok(())
        }
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
        stream_jsonl(&url)?;
        return Ok(());
    }

    let mut url = format!("http://127.0.0.1:{}/events?since={since}", config.port);
    if let Some(limit) = limit {
        url.push_str(&format!("&limit={limit}"));
    }
    let body = get_text(&url, None)?;
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
    let resp = post_json(&format!("http://127.0.0.1:{}/messages", config.port), body)?;
    print_status(&resp);
    Ok(())
}

/// `up`: ensure the listener is online, then print status. Idempotent.
fn up() -> Result<()> {
    let config = Config::load_or_init()?;
    let url = format!("http://127.0.0.1:{}/status", config.port);
    let was_up = probe(&url).is_some();

    ensure_running(&config)?;

    let body = probe(&url).unwrap_or_default();
    println!(
        "blueski {} on 127.0.0.1:{}",
        if was_up { "already up" } else { "up" },
        config.port
    );
    print_status(&body);
    Ok(())
}

/// Ensure a daemon is listening: return immediately if `/status` answers, else
/// spawn the `run` process detached and health-gate until it does (~10s).
fn ensure_running(config: &Config) -> Result<()> {
    let url = format!("http://127.0.0.1:{}/status", config.port);
    if probe(&url).is_some() {
        return Ok(());
    }

    let exe = std::env::current_exe()?;
    let out = open_log("/tmp/blueski.log")?;
    let err = open_log("/tmp/blueski.err")?;
    std::process::Command::new(exe)
        .arg("run")
        .stdin(Stdio::null())
        .stdout(out)
        .stderr(err)
        .spawn()?;

    for _ in 0..40 {
        std::thread::sleep(Duration::from_millis(250));
        if probe(&url).is_some() {
            return Ok(());
        }
    }
    anyhow::bail!("started, but control socket never became healthy — see /tmp/blueski.err");
}

/// `down`: stop a daemon previously started with `up` (via its pidfile).
fn down() -> Result<()> {
    let pid_path = config::pid_path();
    let Ok(pid) = std::fs::read_to_string(&pid_path).map(|s| s.trim().to_string()) else {
        println!("no pidfile — daemon not running via `up`");
        return Ok(());
    };
    let status = std::process::Command::new("kill").arg(&pid).status()?;
    let _ = std::fs::remove_file(&pid_path);
    if status.success() {
        println!("stopped blueski (pid {pid})");
    } else {
        println!("no live process for pid {pid} (cleaned up pidfile)");
    }
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
    };

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

fn get_text(url: &str, timeout: Option<Duration>) -> Result<String> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    rt.block_on(async {
        let mut builder = reqwest::Client::builder();
        if let Some(timeout) = timeout {
            builder = builder.timeout(timeout);
        }
        let client = builder.build()?;
        let resp = client.get(url).send().await?;
        let status = resp.status();
        let text = resp.text().await?;
        if !status.is_success() {
            anyhow::bail!("GET {url} returned {status}: {text}");
        }
        Ok(text)
    })
}

fn stream_jsonl(url: &str) -> Result<()> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    rt.block_on(async {
        let client = reqwest::Client::builder().build()?;
        let mut resp = client.get(url).send().await?;
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

/// `status`: local permission checks plus a live probe of the control socket.
fn status() -> Result<()> {
    let config = Config::load_or_init()?;
    let chatdb = config::chatdb_path();
    let url = format!("http://127.0.0.1:{}/status", config.port);

    let report = serde_json::json!({
        "version": env!("CARGO_PKG_VERSION"),
        "transport": "applescript",
        "port": config.port,
        "running": probe(&url).is_some(),
        "webhook_configured": config.webhook_url.is_some(),
        "config_path": config::config_path().display().to_string(),
        "permissions": {
            "full_disk_access": permissions::has_full_disk_access(&chatdb),
            "automation": permissions::has_automation(),
        }
    });
    print_status(&serde_json::to_string(&report)?);
    Ok(())
}

/// Blocking GET of the control socket's `/status`. Returns the body on 2xx.
fn probe(url: &str) -> Option<String> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .ok()?;
    rt.block_on(async {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(800))
            .build()
            .ok()?;
        let resp = client.get(url).send().await.ok()?;
        if resp.status().is_success() {
            resp.text().await.ok()
        } else {
            None
        }
    })
}

/// Blocking POST of a JSON body to the control socket; returns the response.
fn post_json(url: &str, body: serde_json::Value) -> Result<String> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    rt.block_on(async {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()?;
        let resp = client.post(url).json(&body).send().await?;
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

fn open_log(path: &str) -> Result<std::fs::File> {
    Ok(std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::StatusCode;

    #[test]
    fn http_success_or_error_rejects_non_2xx() {
        let err = http_success_or_error(
            "POST",
            "http://127.0.0.1:8787/messages",
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
            "http://127.0.0.1:8787/messages",
            StatusCode::ACCEPTED,
            r#"{"status":"queued"}"#.to_string(),
        )
        .unwrap();

        assert_eq!(body, r#"{"status":"queued"}"#);
    }
}
