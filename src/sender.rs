//! AppleScript transport for Messages.app.

use crate::model::{SendJob, SendTarget};
use std::process::Stdio;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

pub trait Sender {
    /// Deliver `job`. `Ok` means Messages.app accepted the AppleScript send.
    fn send(&self, job: &SendJob) -> impl std::future::Future<Output = Result<(), String>> + Send;
}

#[derive(Clone, Copy)]
pub struct AppleScriptSender;

impl Sender for AppleScriptSender {
    async fn send(&self, job: &SendJob) -> Result<(), String> {
        let (script, target_arg) = match &job.target {
            SendTarget::Handle { to } => {
                let service = if job.protocol.eq_ignore_ascii_case("sms") {
                    "SMS"
                } else if job.protocol.eq_ignore_ascii_case("rcs") {
                    "RCS"
                } else {
                    "iMessage"
                };
                (
                    format!(
                        r#"on run argv
  set targetPhone to item 1 of argv
  set targetMessage to item 2 of argv
  tell application "Messages"
    set targetService to 1st service whose service type = {service}
    set targetBuddy to buddy targetPhone of targetService
    send targetMessage to targetBuddy
  end tell
end run"#
                    ),
                    to.clone(),
                )
            }
            SendTarget::Chat { chat_id } => (
                r#"on run argv
  set targetChatId to item 1 of argv
  set targetMessage to item 2 of argv
  tell application "Messages"
    set targetChat to 1st chat whose id = targetChatId
    send targetMessage to targetChat
  end tell
end run"#
                    .to_string(),
                chat_id.clone(),
            ),
        };

        let mut child = Command::new("osascript")
            .arg("-")
            .arg(target_arg)
            .arg(&job.text)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| format!("spawn osascript: {e}"))?;

        {
            let mut stdin = child.stdin.take().ok_or("no stdin")?;
            stdin
                .write_all(script.as_bytes())
                .await
                .map_err(|e| format!("write script: {e}"))?;
        }

        let out = child
            .wait_with_output()
            .await
            .map_err(|e| format!("wait osascript: {e}"))?;

        if out.status.success() {
            Ok(())
        } else {
            let err = String::from_utf8_lossy(&out.stderr).trim().to_string();
            Err(if err.is_empty() {
                format!("osascript exited with {}", out.status)
            } else {
                err
            })
        }
    }
}
