//! Cross-platform desktop notification helper.
//!
//! Uses platform-native tools via `std::process::Command` so no extra
//! Rust crate is required:
//!   • macOS  → `osascript -e 'display notification …'`
//!   • Linux  → `notify-send` (libnotify)
//!   • Windows → PowerShell `New-BurntToastNotification` (or silent fallback)
//!
//! Failures are always silent — notifications are best-effort.

/// Send a desktop notification. Title and body are plain strings.
/// Never panics; any error is silently discarded.
pub fn send(title: &str, body: &str) {
    #[cfg(target_os = "macos")]
    {
        let script = format!(
            "display notification \"{}\" with title \"{}\"",
            body.replace('"', "\\\""),
            title.replace('"', "\\\"")
        );
        let _ = std::process::Command::new("osascript")
            .args(["-e", &script])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn();
    }

    #[cfg(target_os = "linux")]
    {
        let _ = std::process::Command::new("notify-send")
            .args([title, body])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn();
    }

    #[cfg(target_os = "windows")]
    {
        // Requires BurntToast PowerShell module; silently fails if absent.
        let script = format!(
            "New-BurntToastNotification -Text '{}','{}'",
            title.replace('\'', "''"),
            body.replace('\'', "''")
        );
        let _ = std::process::Command::new("powershell")
            .args(["-NoProfile", "-Command", &script])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn();
    }

    // On unsupported platforms: no-op.
    #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
    let _ = (title, body);
}
