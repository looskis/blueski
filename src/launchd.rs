//! LaunchAgent install/uninstall. Per-user GUI agent (not a daemon) — required
//! for AppleScript automation and the per-user Messages DB.

use crate::config::{plist_path, LABEL};
use anyhow::{Context, Result};

fn uid() -> Result<String> {
    let out = std::process::Command::new("id").arg("-u").output()?;
    Ok(String::from_utf8(out.stdout)?.trim().to_string())
}

fn plist_contents(exe: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{LABEL}</string>
    <key>ProgramArguments</key>
    <array>
        <string>{exe}</string>
        <string>run</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>/tmp/blueski.log</string>
    <key>StandardErrorPath</key>
    <string>/tmp/blueski.err</string>
</dict>
</plist>
"#
    )
}

pub fn install() -> Result<()> {
    let exe = std::env::current_exe()?.to_string_lossy().into_owned();
    let path = plist_path();
    std::fs::create_dir_all(path.parent().unwrap())?;
    std::fs::write(&path, plist_contents(&exe))
        .with_context(|| format!("writing {}", path.display()))?;
    println!("wrote {}", path.display());

    let target = format!("gui/{}", uid()?);
    // Replace any prior instance, then load.
    let _ = std::process::Command::new("launchctl")
        .args(["bootout", &target, &path.to_string_lossy()])
        .status();
    let status = std::process::Command::new("launchctl")
        .args(["bootstrap", &target, &path.to_string_lossy()])
        .status()?;

    if status.success() {
        println!("loaded {LABEL} (gui/{})", uid()?);
    } else {
        println!("launchctl bootstrap returned {status} — check `launchctl print {target}`");
    }
    Ok(())
}

pub fn uninstall() -> Result<()> {
    let path = plist_path();
    let target = format!("gui/{}", uid()?);
    let _ = std::process::Command::new("launchctl")
        .args(["bootout", &target, &path.to_string_lossy()])
        .status();
    if path.exists() {
        std::fs::remove_file(&path)?;
        println!("removed {}", path.display());
    } else {
        println!("no plist at {}", path.display());
    }
    Ok(())
}
