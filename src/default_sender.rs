//! Explicit control of Messages' global default for new conversations.
use serde::Serialize;
use std::process::Stdio;
use tokio::{
    io::AsyncWriteExt,
    process::Command,
    sync::Mutex,
    time::{timeout, Duration},
};

const SCRIPT: &str = include_str!("../scripts/default-sender.applescript");
static UI_LOCK: Mutex<()> = Mutex::const_new(());

#[derive(Debug)]
pub struct SettingsError {
    pub code: &'static str,
    pub message: String,
}

#[derive(Debug, Serialize)]
pub struct DefaultSender {
    pub selected: String,
    pub available: Vec<String>,
    pub scope: &'static str,
}

pub fn normalize_address(value: &str) -> Result<String, SettingsError> {
    let value = value.trim();
    if value.len() > 320 || value.chars().any(char::is_control) {
        return Err(invalid_address());
    }
    if value.contains('@') {
        let parts: Vec<_> = value.split('@').collect();
        if parts.len() == 2
            && !parts[0].is_empty()
            && parts[1].contains('.')
            && !value.chars().any(char::is_whitespace)
        {
            return Ok(value.to_lowercase());
        }
    } else {
        let phone: String = value.chars().filter(|c| !" ().-".contains(*c)).collect();
        if let Some(digits) = phone.strip_prefix('+') {
            if (8..=15).contains(&digits.len())
                && !digits.starts_with('0')
                && digits.bytes().all(|c| c.is_ascii_digit())
            {
                return Ok(phone);
            }
        }
    }
    Err(invalid_address())
}

fn invalid_address() -> SettingsError {
    SettingsError {
        code: "invalid_address",
        message: "Use an international phone number beginning with + or an email address".into(),
    }
}

fn script_error(stderr: &str) -> SettingsError {
    let code = if stderr.contains("19002") {
        "sender_unavailable"
    } else if stderr.contains("19001")
        || stderr.contains("-1743")
        || stderr.contains("-25211")
        || stderr.contains("not allowed assistive access")
    {
        "accessibility_required"
    } else {
        "settings_unavailable"
    };
    SettingsError {
        code,
        message: stderr.trim().to_string(),
    }
}

fn parse_output(output: &str) -> Result<DefaultSender, SettingsError> {
    let mut lines = output.lines();
    let selected = normalize_address(lines.next().unwrap_or(""))?;
    let available = lines
        .map(normalize_address)
        .collect::<Result<Vec<_>, _>>()?;
    if !available.contains(&selected) {
        return Err(SettingsError {
            code: "settings_unavailable",
            message: "Messages returned an inconsistent sender selection".into(),
        });
    }
    Ok(DefaultSender {
        selected,
        available,
        scope: "new_conversations",
    })
}

pub async fn configure(address: Option<&str>) -> Result<DefaultSender, SettingsError> {
    let address = address.map(normalize_address).transpose()?;
    let _guard = UI_LOCK.try_lock().map_err(|_| SettingsError {
        code: "settings_busy",
        message: "Another Messages settings operation is running".into(),
    })?;
    let mut child = Command::new("osascript")
        .arg("-")
        .arg(address.as_deref().unwrap_or(""))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| SettingsError {
            code: "settings_unavailable",
            message: e.to_string(),
        })?;
    let mut stdin = child.stdin.take().expect("piped stdin");
    stdin
        .write_all(SCRIPT.as_bytes())
        .await
        .map_err(|e| SettingsError {
            code: "settings_unavailable",
            message: e.to_string(),
        })?;
    drop(stdin);
    let output = timeout(Duration::from_secs(20), child.wait_with_output())
        .await
        .map_err(|_| SettingsError {
            code: "settings_timeout",
            message: "Messages settings timed out; read the current selection before retrying"
                .into(),
        })?
        .map_err(|e| SettingsError {
            code: "settings_unavailable",
            message: e.to_string(),
        })?;
    if !output.status.success() {
        return Err(script_error(&String::from_utf8_lossy(&output.stderr)));
    }
    let result = parse_output(&String::from_utf8_lossy(&output.stdout))?;
    if address.is_some_and(|expected| expected != result.selected) {
        return Err(SettingsError {
            code: "settings_unavailable",
            message: "Requested default sender was not confirmed".into(),
        });
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn normalizes_without_guessing_phone_country() {
        assert_eq!(
            normalize_address("+1 (646) 992-1008").unwrap(),
            "+16469921008"
        );
        assert_eq!(normalize_address("Kevin@loo.ski").unwrap(), "kevin@loo.ski");
        for bad in [
            "",
            "6469921008",
            "+16469921008\nignore",
            "a@@b.com",
            "+0123456789",
        ] {
            assert!(normalize_address(bad).is_err(), "{bad}");
        }
    }
    #[test]
    fn requires_selected_address_in_available_choices() {
        let result = parse_output("+1 (646) 992-1008\n+16469921008\nkevin@loo.ski\n").unwrap();
        assert_eq!(result.selected, "+16469921008");
        assert_eq!(result.scope, "new_conversations");
        assert!(parse_output("+16469921008\n+17185104786").is_err());
        assert!(parse_output("").is_err());
    }
    #[test]
    fn reports_actionable_ui_errors() {
        assert_eq!(
            script_error("not available (19002)").code,
            "sender_unavailable"
        );
        assert_eq!(
            script_error("not authorized (-1743)").code,
            "accessibility_required"
        );
        assert_eq!(
            script_error("window missing (19003)").code,
            "settings_unavailable"
        );
    }
}
