//! TCC permission checks and the user-driven `setup` flow. None of these can be
//! granted programmatically — we detect state and deep-link the user to the
//! right System Settings pane.

use std::io::Read;
use std::path::Path;
use std::time::Duration;

const PANE_FULL_DISK: &str =
    "x-apple.systempreferences:com.apple.preference.security?Privacy_AllFiles";
const PANE_AUTOMATION: &str =
    "x-apple.systempreferences:com.apple.preference.security?Privacy_Automation";

/// Full Disk Access — inferred by trying to read a byte of `chat.db`. Without
/// the grant this fails with permission denied.
pub fn has_full_disk_access(chatdb: &Path) -> bool {
    match std::fs::File::open(chatdb) {
        Ok(mut f) => {
            let mut buf = [0u8; 16];
            f.read(&mut buf).is_ok()
        }
        Err(_) => false,
    }
}

/// Automation (control Messages.app). Probing it also surfaces the one-time
/// consent dialog on first run — which is exactly what we want during setup.
pub fn has_automation() -> bool {
    std::process::Command::new("osascript")
        .arg("-e")
        .arg("tell application \"Messages\" to get name")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn open_pane(url: &str) {
    let _ = std::process::Command::new("open").arg(url).status();
}

/// Interactive setup: open the relevant panes and block until both grants land.
pub fn run_setup(chatdb: &Path) {
    println!("blueski setup — granting macOS permissions\n");

    if has_full_disk_access(chatdb) {
        println!("  [ok] Full Disk Access");
    } else {
        println!("  [..] Full Disk Access — opening System Settings");
        println!("       Add 'blueski' under Privacy & Security > Full Disk Access.");
        open_pane(PANE_FULL_DISK);
    }

    // Probe automation to trigger the first-run consent dialog now.
    if has_automation() {
        println!("  [ok] Automation (Messages)");
    } else {
        println!("  [..] Automation — approve the prompt, or enable it in System Settings");
        open_pane(PANE_AUTOMATION);
    }

    println!("\nWaiting for grants (Ctrl-C to quit)…");
    let mut fda_done = false;
    let mut auto_done = false;
    loop {
        if !fda_done && has_full_disk_access(chatdb) {
            println!("  [ok] Full Disk Access granted");
            fda_done = true;
        }
        if !auto_done && has_automation() {
            println!("  [ok] Automation granted");
            auto_done = true;
        }
        if fda_done && auto_done {
            println!("\nAll set. Run `blueski install` to start the agent.");
            return;
        }
        std::thread::sleep(Duration::from_secs(2));
    }
}
