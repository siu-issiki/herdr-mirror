// Foreground-process detection for the mirror streamer.
//
// herdr strips the mouse-mode DECSET from the frames the plugin observes, so the
// streamer can't tell whether the remote pane's app wants the mouse. As a proxy,
// query the remote pane's foreground process (`herdr pane process-info`) and
// classify it: a plain shell at a prompt, or a known agent CLI that renders
// inline without mouse tracking, never enables mouse reporting, so mouse events
// should stay local (no garbage in the prompt / native selection keeps
// working); anything else is treated as a possible mouse-aware TUI and clicks
// are forwarded. This is a heuristic stand-in until herdr exposes the pane's
// mouse-reporting state through the API.

use std::process::Stdio;

use tokio::process::Command;

use crate::pane::sh_quote;
use crate::remote::SSH_COMMON_OPTS;

/// Interactive shells: at a prompt these don't enable mouse reporting, so mouse
/// events over them should stay local rather than being forwarded to the pty.
const SHELLS: &[&str] = &[
    "bash", "zsh", "fish", "sh", "dash", "ksh", "ksh93", "mksh", "ash", "tcsh",
    "csh", "nu", "elvish", "xonsh", "osh", "ysh", "oil", "ion", "murex", "ngs",
    "pwsh", "powershell", "cmd",
];

/// Agent CLIs that render inline without mouse tracking: like shells, mouse
/// events over them should stay local so native selection/copy keeps working.
const AGENTS: &[&str] = &["claude", "codex", "gemini", "aider", "opencode", "goose", "amp"];

/// Classification of a remote pane's foreground process, driving mouse policy:
/// - `Shell`: interactive shell at a prompt — mouse stays local (native
///   selection/scroll), the grab is released.
/// - `Agent`: a known agent CLI (Claude Code, Codex, …) — grab stays on; a click
///   forwards to the remote, a left-drag selects/copies locally.
/// - `Tui`: anything else, assumed mouse-aware — all mouse events forwarded raw.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Foreground {
    Shell,
    Agent,
    Tui,
}

/// Normalizes a login-shell dash (`-name`), a leading path, and a Windows
/// `.exe` suffix, and lowercases the result, before matching against a list.
fn normalize(name: &str) -> String {
    let base = name.trim_start_matches('-').rsplit(['/', '\\']).next().unwrap_or(name);
    base.trim_end_matches(".exe").to_ascii_lowercase()
}

/// Is `name` one of the known interactive shells? Normalizes a login-shell dash
/// (`-bash`), a leading path, and a Windows `.exe` suffix before matching.
pub fn is_shell(name: &str) -> bool {
    SHELLS.contains(&normalize(name).as_str())
}

/// Is `name` one of the known agent CLIs (Claude Code, Codex, etc.)? Same
/// normalization as `is_shell`.
pub fn is_agent(name: &str) -> bool {
    AGENTS.contains(&normalize(name).as_str())
}

/// Classify a `pane process-info` JSON response into a [`Foreground`]. `None` =
/// indeterminate (empty/unparseable), so the caller keeps its last known value.
///
/// Checks both `name` and `argv0` of the leaf process: some agent CLIs (e.g.
/// Claude Code) report a version string like "2.1.207" as `name` but the
/// actual program as `argv0` ("claude"), so `name` alone isn't reliable.
pub fn classify(json: &str) -> Option<Foreground> {
    let v: serde_json::Value = serde_json::from_str(json).ok()?;
    let fg = v.get("result")?.get("process_info")?.get("foreground_processes")?.as_array()?;
    // the last foreground process is the actually-running leaf, so `sudo vim`
    // classifies on `vim`, not `sudo`
    let leaf = fg.last()?;
    let name = leaf.get("name")?.as_str()?;
    let argv0 = leaf.get("argv0").and_then(|v| v.as_str());
    let is_sh = is_shell(name) || argv0.is_some_and(is_shell);
    let is_ag = is_agent(name) || argv0.is_some_and(is_agent);
    Some(if is_sh {
        Foreground::Shell
    } else if is_ag {
        Foreground::Agent
    } else {
        Foreground::Tui
    })
}

/// Query the remote pane's foreground process over ssh and classify it. `None`
/// on any failure (ssh/network/parse) so the caller keeps its last known value.
pub async fn poll(
    ssh_target: &str,
    remote_bin: &str,
    pane: &str,
    ctl_path: Option<&str>,
) -> Option<Foreground> {
    // remote_bin stays unquoted for remote-shell ~ expansion, matching the
    // observe session's command construction
    let cmd = format!("exec {} pane process-info --pane {}", remote_bin, sh_quote(pane));
    let mut sc = Command::new("ssh");
    // reuse the daemon's ControlMaster when given so the poll skips the
    // handshake; `-S` without `-M` uses an existing master or, if the socket
    // isn't there, connects directly — so this degrades gracefully
    if let Some(path) = ctl_path {
        sc.arg("-S").arg(path);
    }
    let out = sc
        .args(SSH_COMMON_OPTS)
        .arg(ssh_target)
        .arg(cmd)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
        .await
        .ok()?;
    if !out.status.success() {
        return None;
    }
    classify(&String::from_utf8_lossy(&out.stdout))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shells_recognized_including_login_and_path() {
        assert!(is_shell("zsh"));
        assert!(is_shell("bash"));
        assert!(is_shell("-bash")); // login shell
        assert!(is_shell("/usr/bin/fish")); // full path
        assert!(is_shell("pwsh.exe")); // windows
        assert!(!is_shell("vim"));
        assert!(!is_shell("htop"));
        assert!(!is_shell("nvim"));
        assert!(!is_shell("lazygit"));
    }

    #[test]
    fn classify_reads_leaf_foreground() {
        let shell = r#"{"result":{"process_info":{"foreground_processes":[{"name":"zsh"}]}}}"#;
        assert_eq!(classify(shell), Some(Foreground::Shell));
        let tui = r#"{"result":{"process_info":{"foreground_processes":[{"name":"vim"}]}}}"#;
        assert_eq!(classify(tui), Some(Foreground::Tui));
        // sudo wrapper: the leaf is the real program
        let sudo =
            r#"{"result":{"process_info":{"foreground_processes":[{"name":"sudo"},{"name":"vim"}]}}}"#;
        assert_eq!(classify(sudo), Some(Foreground::Tui));
    }

    #[test]
    fn classify_indeterminate_on_empty_or_garbage() {
        assert_eq!(classify(r#"{"result":{"process_info":{"foreground_processes":[]}}}"#), None);
        assert_eq!(classify("not json"), None);
        assert_eq!(classify(r#"{"result":{}}"#), None);
    }

    #[test]
    fn classify_recognizes_agent_cli_via_argv0() {
        // real-world shape: Claude Code reports its version string as `name`
        // ("2.1.207") but `argv0` is "claude", so classification must fall
        // back to argv0 when name doesn't match a known shell/agent.
        let claude = r#"{"result":{"process_info":{"foreground_processes":[
            {"argv0":"node","name":"node","pid":23166},
            {"argv":["claude","--resume","..."],"argv0":"claude","name":"2.1.207","pid":22661}
        ]}}}"#;
        assert_eq!(classify(claude), Some(Foreground::Agent));

        let vim = r#"{"result":{"process_info":{"foreground_processes":[
            {"argv0":"vim","name":"vim","pid":1}
        ]}}}"#;
        assert_eq!(classify(vim), Some(Foreground::Tui));

        // shell classification still works when argv0 is present
        let zsh = r#"{"result":{"process_info":{"foreground_processes":[
            {"argv0":"zsh","name":"zsh","pid":1}
        ]}}}"#;
        assert_eq!(classify(zsh), Some(Foreground::Shell));
    }
}
