//! Per-user LaunchAgent lifecycle. The CLI never daemonizes Blueski itself:
//! launchd (including Homebrew Services) is the single process owner.

use crate::config::{self, LABEL};
use anyhow::{bail, Context, Result};
use std::path::PathBuf;
use std::process::Command;

const HOMEBREW_LABEL: &str = "homebrew.mxcl.blueski";
const TUNNEL_LABEL: &str = "com.looskis.blueski.ngrok";

fn uid() -> u32 {
    unsafe { libc::getuid() }
}

fn domain() -> String {
    format!("gui/{}", uid())
}

fn service(label: &str) -> String {
    format!("{}/{}", domain(), label)
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn service_executable() -> Result<PathBuf> {
    let current = std::env::current_exe()?;
    let resolved = current.canonicalize().unwrap_or_else(|_| current.clone());
    for candidate in [
        PathBuf::from("/opt/homebrew/bin/blueski"),
        PathBuf::from("/usr/local/bin/blueski"),
    ] {
        if candidate.exists() && candidate.canonicalize().is_ok_and(|path| path == resolved) {
            // A stable symlink prevents upgrades from leaving launchd pointed
            // at a removed Homebrew Cellar version.
            return Ok(candidate);
        }
    }
    Ok(current)
}

fn plist_contents(exe: &str) -> String {
    let stdout = xml_escape(&config::stdout_log_path().to_string_lossy());
    let stderr = xml_escape(&config::stderr_log_path().to_string_lossy());
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
<key>Label</key><string>{LABEL}</string>
<key>ProgramArguments</key><array><string>{exe}</string><string>run</string></array>
<key>RunAtLoad</key><true/>
<key>KeepAlive</key><true/>
<key>StandardOutPath</key><string>{stdout}</string>
<key>StandardErrorPath</key><string>{stderr}</string>
</dict></plist>
"#
    )
}

fn write_launch_agent() -> Result<bool> {
    let path = config::plist_path();
    std::fs::create_dir_all(config::config_dir())?;
    std::fs::create_dir_all(path.parent().context("LaunchAgents path has no parent")?)?;
    let executable = xml_escape(&service_executable()?.to_string_lossy());
    let desired = plist_contents(&executable);
    if std::fs::read_to_string(&path).is_ok_and(|current| current == desired) {
        return Ok(false);
    }
    std::fs::write(&path, desired).with_context(|| format!("writing {}", path.display()))?;
    Ok(true)
}

fn loaded(label: &str) -> Result<bool> {
    Ok(Command::new("launchctl")
        .args(["print", &service(label)])
        .output()
        .with_context(|| format!("inspect Blueski LaunchAgent {label}"))?
        .status
        .success())
}

pub fn active_label() -> Result<Option<&'static str>> {
    for label in [LABEL, HOMEBREW_LABEL] {
        if loaded(label)? {
            return Ok(Some(label));
        }
    }
    Ok(None)
}

fn bootstrap(replace: bool) -> Result<()> {
    if replace && loaded(LABEL)? {
        let _ = Command::new("launchctl")
            .args(["bootout", &service(LABEL)])
            .output();
    }
    let path = config::plist_path();
    let result = Command::new("launchctl")
        .args(["bootstrap", &domain(), &path.to_string_lossy()])
        .status()
        .context("load Blueski LaunchAgent")?;
    if !result.success() {
        bail!("launchctl bootstrap failed with {result}");
    }
    Ok(())
}

/// Install or repair the built-in service. An already-loaded Homebrew service
/// remains authoritative so there can never be two supervisors.
pub fn install() -> Result<()> {
    if loaded(HOMEBREW_LABEL)? {
        if loaded(LABEL)? {
            let _ = Command::new("launchctl")
                .args(["bootout", &service(LABEL)])
                .status();
        }
        println!("using existing Homebrew supervisor {HOMEBREW_LABEL}");
        return Ok(());
    }
    let changed = write_launch_agent()?;
    if changed || !loaded(LABEL)? {
        bootstrap(changed)?;
        println!("installed and loaded {}", config::plist_path().display());
    } else {
        println!("LaunchAgent is current ({LABEL})");
    }
    Ok(())
}

/// Ask the active OS supervisor to start/restart Blueski. If none exists,
/// install the built-in LaunchAgent first.
pub fn start_supervised() -> Result<()> {
    let label = match active_label()? {
        Some(label) => label,
        None => {
            write_launch_agent()?;
            bootstrap(false)?;
            return Ok(());
        }
    };
    let result = Command::new("launchctl")
        .args(["kickstart", "-k", &service(label)])
        .status()
        .context("start Blueski LaunchAgent")?;
    if !result.success() {
        bail!("launchctl kickstart failed with {result}");
    }
    Ok(())
}

pub fn stop_active() -> Result<Vec<&'static str>> {
    let mut stopped = Vec::new();
    for label in [LABEL, HOMEBREW_LABEL] {
        if !loaded(label)? {
            continue;
        }
        let result = Command::new("launchctl")
            .args(["bootout", &service(label)])
            .status()
            .with_context(|| format!("stop Blueski LaunchAgent {label}"))?;
        if !result.success() {
            bail!("launchctl bootout for {label} failed with {result}");
        }
        stopped.push(label);
    }
    Ok(stopped)
}

pub fn uninstall() -> Result<()> {
    uninstall_tunnel()?;
    for label in stop_active()? {
        println!("stopped {label}");
    }
    let path = config::plist_path();
    if path.exists() {
        std::fs::remove_file(&path)?;
        println!("removed {}", path.display());
    } else {
        println!("no built-in LaunchAgent at {}", path.display());
    }
    Ok(())
}

fn ngrok_executable() -> Result<PathBuf> {
    for candidate in [
        PathBuf::from("/opt/homebrew/bin/ngrok"),
        PathBuf::from("/usr/local/bin/ngrok"),
    ] {
        if candidate.exists() {
            return Ok(candidate);
        }
    }
    let output = Command::new("which")
        .arg("ngrok")
        .output()
        .context("find ngrok executable")?;
    if output.status.success() {
        let path = PathBuf::from(String::from_utf8(output.stdout)?.trim());
        if path.exists() {
            return Ok(path);
        }
    }
    bail!("ngrok is not installed; install it before running `blueski publish`")
}

pub fn install_tunnel(domain_name: &str, port: u16) -> Result<()> {
    let domain_name = domain_name
        .strip_prefix("https://")
        .or_else(|| domain_name.strip_prefix("http://"))
        .unwrap_or(domain_name)
        .trim_end_matches('/');
    if domain_name.is_empty()
        || domain_name.contains('/')
        || !domain_name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-'))
    {
        bail!("domain must be a hostname such as blueski.example.com");
    }

    std::fs::create_dir_all(config::config_dir())?;
    let executable = xml_escape(&ngrok_executable()?.to_string_lossy());
    let domain_name = xml_escape(domain_name);
    let upstream = format!("127.0.0.1:{port}");
    let stdout = xml_escape(&config::tunnel_stdout_log_path().to_string_lossy());
    let stderr = xml_escape(&config::tunnel_stderr_log_path().to_string_lossy());
    let plist = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
<key>Label</key><string>{TUNNEL_LABEL}</string>
<key>ProgramArguments</key><array>
<string>{executable}</string><string>http</string><string>--domain</string><string>{domain_name}</string><string>{upstream}</string>
</array>
<key>RunAtLoad</key><true/>
<key>KeepAlive</key><true/>
<key>StandardOutPath</key><string>{stdout}</string>
<key>StandardErrorPath</key><string>{stderr}</string>
</dict></plist>
"#
    );
    let path = config::tunnel_plist_path();
    std::fs::create_dir_all(path.parent().context("LaunchAgents path has no parent")?)?;
    std::fs::write(&path, plist)?;
    if loaded(TUNNEL_LABEL)? {
        let _ = Command::new("launchctl")
            .args(["bootout", &service(TUNNEL_LABEL)])
            .status();
    }
    let result = Command::new("launchctl")
        .args(["bootstrap", &domain(), &path.to_string_lossy()])
        .status()
        .context("load Blueski ngrok LaunchAgent")?;
    if !result.success() {
        bail!("launchctl bootstrap for ngrok failed with {result}");
    }
    Ok(())
}

pub fn stop_tunnel() -> Result<bool> {
    if !loaded(TUNNEL_LABEL)? {
        return Ok(false);
    }
    let result = Command::new("launchctl")
        .args(["bootout", &service(TUNNEL_LABEL)])
        .status()?;
    if !result.success() {
        bail!("launchctl bootout for ngrok failed with {result}");
    }
    Ok(true)
}

pub fn uninstall_tunnel() -> Result<()> {
    let _ = stop_tunnel()?;
    let path = config::tunnel_plist_path();
    if path.exists() {
        std::fs::remove_file(path)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escapes_launch_agent_values() {
        assert_eq!(xml_escape("a&<\"'>"), "a&amp;&lt;&quot;&apos;&gt;");
    }
}
