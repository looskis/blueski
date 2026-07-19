//! TCC permission checks and the user-driven `setup` flow. None of these can be
//! granted programmatically — we detect state and deep-link the user to the
//! right System Settings pane.

use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
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

/// Automation (control Messages.app). Querying the services collection sends a
/// real AppleEvent; reading the app name can succeed without exercising TCC.
pub fn has_automation() -> bool {
    std::process::Command::new("osascript")
        .arg("-e")
        .arg("with timeout of 5 seconds")
        .arg("-e")
        .arg("tell application \"Messages\" to count services")
        .arg("-e")
        .arg("end timeout")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Permission state cached by the daemon. HTTP status handlers only read
/// atomics; AppleScript and protected-file probes happen off the request path.
#[derive(Clone, Default)]
pub struct PermissionState {
    full_disk_access: Arc<AtomicBool>,
    automation: Arc<AtomicBool>,
    checked: Arc<AtomicBool>,
}

impl PermissionState {
    pub fn full_disk_access(&self) -> bool {
        self.full_disk_access.load(Ordering::Relaxed)
    }

    pub fn automation(&self) -> bool {
        self.automation.load(Ordering::Relaxed)
    }

    pub fn checked(&self) -> bool {
        self.checked.load(Ordering::Acquire)
    }

    pub fn refresh(&self, chatdb: &Path) {
        self.full_disk_access
            .store(has_full_disk_access(chatdb), Ordering::Relaxed);
        self.automation.store(has_automation(), Ordering::Relaxed);
        self.checked.store(true, Ordering::Release);
    }
}

pub fn spawn_refresh(state: PermissionState, chatdb: PathBuf) {
    tokio::spawn(async move {
        loop {
            let current = state.clone();
            let path = chatdb.clone();
            if let Err(error) = tokio::task::spawn_blocking(move || current.refresh(&path)).await {
                tracing::warn!(%error, "permission refresh task failed");
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    });
}

fn open_pane(url: &str) {
    let _ = std::process::Command::new("open").arg(url).status();
}

fn enclosing_app(path: &Path) -> Option<&Path> {
    path.ancestors()
        .find(|ancestor| ancestor.extension().is_some_and(|ext| ext == "app"))
}

fn full_disk_access_target() -> PathBuf {
    let executable = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("blueski"));
    let resolved = executable.canonicalize().unwrap_or(executable);
    enclosing_app(&resolved).unwrap_or(&resolved).to_path_buf()
}

/// Relaunch setup through LaunchServices so TCC attributes protected-file
/// access to the app bundle instead of the invoking terminal or agent.
pub fn relaunch_setup_in_app() -> std::io::Result<bool> {
    let target = full_disk_access_target();
    if !target.extension().is_some_and(|ext| ext == "app") {
        return Ok(false);
    }

    let status = std::process::Command::new("open")
        .arg("-W")
        .arg("-n")
        .arg(&target)
        .arg("--args")
        .arg("setup")
        .arg("--app-launched")
        .status()?;

    if !status.success() {
        return Err(std::io::Error::other(format!(
            "failed to launch {}",
            target.display()
        )));
    }

    println!("Completed permission setup in {}.", target.display());
    Ok(true)
}

fn reveal_in_finder(path: &Path) {
    let _ = std::process::Command::new("open")
        .arg("-R")
        .arg(path)
        .status();
}

/// Interactive setup: open the relevant panes and block until both grants land.
pub fn run_setup(chatdb: &Path) {
    println!("blueski setup — granting macOS permissions\n");

    if has_full_disk_access(chatdb) {
        println!("  [ok] Full Disk Access");
    } else {
        let target = full_disk_access_target();
        println!("  [..] Full Disk Access — opening System Settings");
        if target.extension().is_some_and(|ext| ext == "app") {
            println!("       Add 'Blueski.app' under Privacy & Security > Full Disk Access.");
            println!("       App: {}", target.display());
            reveal_in_finder(&target);
        } else {
            println!("       Add this executable under Privacy & Security > Full Disk Access.");
            println!("       Executable: {}", target.display());
        }
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
            println!("\nmacOS authorization complete.");
            return;
        }
        std::thread::sleep(Duration::from_secs(2));
    }
}

#[cfg(test)]
mod tests {
    use super::enclosing_app;
    use std::path::Path;

    #[test]
    fn finds_enclosing_app_bundle() {
        let executable =
            Path::new("/opt/homebrew/Cellar/blueski/0.1.1/Blueski.app/Contents/MacOS/blueski");
        assert_eq!(
            enclosing_app(executable),
            Some(Path::new("/opt/homebrew/Cellar/blueski/0.1.1/Blueski.app"))
        );
    }

    #[test]
    fn leaves_source_builds_without_an_app_bundle() {
        assert_eq!(
            enclosing_app(Path::new("/tmp/blueski/target/debug/blueski")),
            None
        );
    }
}
