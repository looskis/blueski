//! System-wide TCP keep-alive for Apple's push connection.
//!
//! iMessage reaches a Mac through apsd's long-lived TCP session to
//! push.apple.com. apsd only sends its own keep-alive every 20–30 minutes
//! once it has grown that interval, so a NAT that drops the idle flow leaves
//! the Mac deaf until the next keep-alive fails; messages sent in that window
//! arrive late. Blueski cannot own that socket, but the kernel can probe every
//! idle TCP connection: probes refresh the NAT mapping and, when the session is
//! already dead, surface the error within minutes so apsd reconnects at once.
//!
//! The settings are kernel sysctls, so they need root to apply and a
//! LaunchDaemon to survive reboots. They are machine-wide, not Blueski-only.

use crate::launchd::xml_escape;
use anyhow::{bail, Context, Result};
use std::path::PathBuf;
use std::process::Command;

pub const LABEL: &str = "com.looskis.blueski.keepalive";

/// Idle time before the first probe, in milliseconds.
pub const KEEPIDLE_MS: u64 = 120_000;
/// Interval between unanswered probes, in milliseconds.
pub const KEEPINTVL_MS: u64 = 15_000;
/// Unanswered probes before the connection is declared dead.
pub const KEEPCNT: u64 = 4;

const DEFAULT_KEEPIDLE_MS: u64 = 7_200_000;
const DEFAULT_KEEPINTVL_MS: u64 = 75_000;
const DEFAULT_KEEPCNT: u64 = 8;

const NAMES: [&str; 4] = [
    "net.inet.tcp.always_keepalive",
    "net.inet.tcp.keepidle",
    "net.inet.tcp.keepintvl",
    "net.inet.tcp.keepcnt",
];

pub fn plist_path() -> PathBuf {
    PathBuf::from(format!("/Library/LaunchDaemons/{LABEL}.plist"))
}

fn assignments(always: u64, idle: u64, intvl: u64, cnt: u64) -> [String; 4] {
    [
        format!("{}={always}", NAMES[0]),
        format!("{}={idle}", NAMES[1]),
        format!("{}={intvl}", NAMES[2]),
        format!("{}={cnt}", NAMES[3]),
    ]
}

fn desired() -> [String; 4] {
    assignments(1, KEEPIDLE_MS, KEEPINTVL_MS, KEEPCNT)
}

fn defaults() -> [String; 4] {
    assignments(
        0,
        DEFAULT_KEEPIDLE_MS,
        DEFAULT_KEEPINTVL_MS,
        DEFAULT_KEEPCNT,
    )
}

pub fn plist_contents() -> String {
    let args = desired()
        .iter()
        .map(|value| format!("<string>{}</string>", xml_escape(value)))
        .collect::<Vec<_>>()
        .join("");
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
<key>Label</key><string>{LABEL}</string>
<key>ProgramArguments</key><array><string>/usr/sbin/sysctl</string><string>-w</string>{args}</array>
<key>RunAtLoad</key><true/>
</dict></plist>
"#
    )
}

/// Effective kernel values, read without privileges.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Settings {
    pub always_keepalive: bool,
    pub keepidle_ms: u64,
    pub keepintvl_ms: u64,
    pub keepcnt: u64,
}

impl Settings {
    pub fn parse(output: &str) -> Result<Self> {
        let mut values = output
            .lines()
            .map(|line| line.trim().parse::<u64>().context("sysctl value"));
        let mut next = || values.next().context("sysctl output is short")?;
        Ok(Self {
            always_keepalive: next()? != 0,
            keepidle_ms: next()?,
            keepintvl_ms: next()?,
            keepcnt: next()?,
        })
    }

    /// Whether these values provide the protection Blueski installs.
    pub fn effective(&self) -> bool {
        self.always_keepalive
            && self.keepidle_ms <= KEEPIDLE_MS
            && self.keepintvl_ms <= KEEPINTVL_MS
            && self.keepcnt <= KEEPCNT
    }

    /// Longest time a silently dead connection can go unnoticed.
    pub fn max_detection_secs(&self) -> u64 {
        (self.keepidle_ms + self.keepintvl_ms * self.keepcnt) / 1000
    }
}

pub fn current() -> Result<Settings> {
    let output = Command::new("/usr/sbin/sysctl")
        .arg("-n")
        .args(NAMES)
        .output()
        .context("read tcp keep-alive sysctls")?;
    if !output.status.success() {
        bail!("sysctl failed with {}", output.status);
    }
    Settings::parse(&String::from_utf8_lossy(&output.stdout))
}

fn loaded() -> bool {
    Command::new("launchctl")
        .args(["print", &format!("system/{LABEL}")])
        .output()
        .is_ok_and(|output| output.status.success())
}

pub fn status_json() -> serde_json::Value {
    match current() {
        Ok(settings) => serde_json::json!({
            "installed": plist_path().exists() && loaded(),
            "effective": settings.effective(),
            "always_keepalive": settings.always_keepalive,
            "keepidle_ms": settings.keepidle_ms,
            "keepintvl_ms": settings.keepintvl_ms,
            "keepcnt": settings.keepcnt,
            "max_detection_secs": settings.max_detection_secs(),
            "daemon": plist_path().display().to_string(),
        }),
        Err(error) => serde_json::json!({ "error": error.to_string() }),
    }
}

fn require_root(action: &str) -> Result<()> {
    if unsafe { libc::geteuid() } != 0 {
        bail!("kernel settings need root: sudo blueski keepalive {action}");
    }
    Ok(())
}

fn apply(values: &[String; 4]) -> Result<()> {
    let result = Command::new("/usr/sbin/sysctl")
        .arg("-w")
        .args(values)
        .status()
        .context("apply tcp keep-alive sysctls")?;
    if !result.success() {
        bail!("sysctl -w failed with {result}");
    }
    Ok(())
}

fn bootout() {
    if loaded() {
        let _ = Command::new("launchctl")
            .args(["bootout", &format!("system/{LABEL}")])
            .output();
    }
}

/// Apply the keep-alive sysctls now and install a LaunchDaemon that reapplies
/// them at every boot.
pub fn install() -> Result<()> {
    require_root("install")?;
    let path = plist_path();
    std::fs::write(&path, plist_contents())
        .with_context(|| format!("writing {}", path.display()))?;
    bootout();
    let result = Command::new("launchctl")
        .args(["bootstrap", "system", &path.to_string_lossy()])
        .status()
        .context("load keep-alive LaunchDaemon")?;
    if !result.success() {
        bail!("launchctl bootstrap failed with {result}");
    }
    apply(&desired())?;
    println!("{}", status_json());
    Ok(())
}

/// Remove the LaunchDaemon and restore macOS defaults.
pub fn uninstall() -> Result<()> {
    require_root("uninstall")?;
    bootout();
    let path = plist_path();
    if path.exists() {
        std::fs::remove_file(&path).with_context(|| format!("removing {}", path.display()))?;
    }
    apply(&defaults())?;
    println!("{}", status_json());
    Ok(())
}

pub fn status() -> Result<()> {
    println!("{}", status_json());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plist_applies_every_sysctl_at_boot() {
        let plist = plist_contents();
        assert!(plist.contains(&format!("<string>{LABEL}</string>")));
        assert!(plist.contains("<string>/usr/sbin/sysctl</string><string>-w</string>"));
        for value in desired() {
            assert!(
                plist.contains(&format!("<string>{value}</string>")),
                "{value}"
            );
        }
        assert!(plist.contains("<key>RunAtLoad</key><true/>"));
    }

    #[test]
    fn plist_is_valid_property_list() {
        let path = std::env::temp_dir().join(format!("{LABEL}.{}.plist", std::process::id()));
        std::fs::write(&path, plist_contents()).unwrap();
        let lint = Command::new("/usr/bin/plutil")
            .args(["-lint", "-s"])
            .arg(&path)
            .status();
        let _ = std::fs::remove_file(&path);
        assert!(
            lint.unwrap().success(),
            "plutil rejected the LaunchDaemon plist"
        );
    }

    #[test]
    fn parses_sysctl_output_and_judges_it() {
        let stock = Settings::parse("0\n7200000\n75000\n8\n").unwrap();
        assert!(!stock.effective());
        assert_eq!(stock.max_detection_secs(), 7800);

        let installed = Settings::parse("1\n120000\n15000\n4\n").unwrap();
        assert!(installed.effective());
        assert_eq!(installed.max_detection_secs(), 180);

        // Probing is useless unless every socket is forced to use it.
        let partial = Settings::parse("0\n120000\n15000\n4\n").unwrap();
        assert!(!partial.effective());

        assert!(Settings::parse("1\n120000\n").is_err());
    }

    #[test]
    fn uninstall_restores_macos_defaults() {
        assert_eq!(
            defaults(),
            [
                "net.inet.tcp.always_keepalive=0".to_string(),
                "net.inet.tcp.keepidle=7200000".to_string(),
                "net.inet.tcp.keepintvl=75000".to_string(),
                "net.inet.tcp.keepcnt=8".to_string(),
            ]
        );
    }
}
