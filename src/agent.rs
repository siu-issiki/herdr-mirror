// Remote mux agent: `herdr-mirror agent [--herdr-bin PATH]`.
//
// One long-lived process per host. It owns `herdr terminal session` children as
// direct local children (pipes, no ssh channels), talks to the local herdr
// socket for api/events, and speaks the NDJSON mux protocol on stdio.
//
// Hard requirement (regression guard for 2026-07-14 ghost leaks): stdin EOF or
// error kills every child and exits immediately. A dead link must never leave
// orphaned terminal sessions holding pane attach slots.
//
// Only stdio and the mux protocol are agent-specific here; this file does not
// touch the daemon / wrapper / mirror data plane (see docs/mux-design.md
// phase 1).

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde_json::json;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::api::ApiClient;
use crate::protocol::{parse_line, Msg};
use crate::util::{err, home_dir, Result};

const PING_INTERVAL_SECS: u64 = 5;
const EXEC_OUT_CAP: usize = 1024 * 1024; // 1 MiB

pub struct AgentArgs {
    /// Explicit remote herdr binary; None means auto-resolve.
    pub herdr_bin: Option<String>,
}

/// `agent [--herdr-bin PATH]`
pub fn parse_args(rest: &[String]) -> Result<AgentArgs> {
    let mut herdr_bin = None;
    let mut it = rest.iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--herdr-bin" => {
                herdr_bin = Some(
                    it.next().ok_or_else(|| err("--herdr-bin needs a value"))?.clone(),
                );
            }
            other => return Err(err(format!("agent: unexpected argument {other}"))),
        }
    }
    Ok(AgentArgs { herdr_bin })
}

/// Resolve the remote herdr binary: explicit flag wins, else prefer
/// `$HOME/.local/bin/herdr` when present, else bare `herdr` (PATH lookup).
fn resolve_herdr_bin(explicit: Option<String>) -> String {
    if let Some(bin) = explicit {
        return bin;
    }
    let local = home_dir().join(".local").join("bin").join("herdr");
    if local.exists() {
        return local.display().to_string();
    }
    "herdr".into()
}

/// Resolve the herdr socket: `HERDR_SOCKET_PATH`, else `~/.config/herdr/herdr.sock`.
fn resolve_herdr_socket() -> PathBuf {
    match std::env::var("HERDR_SOCKET_PATH") {
        Ok(s) if !s.is_empty() => PathBuf::from(s),
        _ => home_dir().join(".config").join("herdr").join("herdr.sock"),
    }
}

/// Build the argv for a terminal-session child (pure — spawned directly, so no
/// shell quoting). Mirrors `herdr [--session s] terminal session <mode> <pane>
/// --cols C --rows R [--takeover]`.
fn terminal_session_argv(
    herdr_bin: &str,
    session: Option<&str>,
    mode: &str,
    pane: &str,
    cols: u32,
    rows: u32,
    takeover: bool,
) -> Vec<String> {
    let mut argv = vec![herdr_bin.to_string()];
    if let Some(s) = session {
        argv.push("--session".into());
        argv.push(s.to_string());
    }
    argv.extend(["terminal".into(), "session".into(), mode.to_string(), pane.to_string()]);
    argv.extend(["--cols".into(), cols.to_string(), "--rows".into(), rows.to_string()]);
    if takeover {
        argv.push("--takeover".into());
    }
    argv
}

/// Control messages to a running terminal-session supervisor.
enum SessionCtrl {
    Input(String),
    Close,
}

/// A live terminal-session child owned by the agent.
struct Session {
    ctrl: mpsc::UnboundedSender<SessionCtrl>,
    /// pid for a direct SIGTERM on shutdown (the supervisor may already be gone).
    pid: Option<u32>,
    task: JoinHandle<()>,
}

/// Single stdout writer. All output funnels through one mpsc so lines from
/// concurrent tasks never interleave.
#[derive(Clone)]
struct Out(mpsc::UnboundedSender<String>);

impl Out {
    fn send(&self, msg: &Msg) {
        let _ = self.0.send(msg.to_line());
    }
}

pub async fn run(args: AgentArgs) -> Result<()> {
    let herdr_bin = resolve_herdr_bin(args.herdr_bin);
    let herdr_socket = resolve_herdr_socket();

    // Single writer task.
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<String>();
    let writer = tokio::spawn(async move {
        let mut stdout = tokio::io::stdout();
        while let Some(line) = out_rx.recv().await {
            if stdout.write_all(line.as_bytes()).await.is_err() {
                break;
            }
            if stdout.flush().await.is_err() {
                break;
            }
        }
    });
    let out = Out(out_tx);

    // hello first.
    out.send(&Msg::Hello {
        version: env!("CARGO_PKG_VERSION").into(),
        herdr_socket: herdr_socket.display().to_string(),
    });

    // Unconditional 5s ping.
    let ping_out = out.clone();
    let ping = tokio::spawn(async move {
        let mut interval =
            tokio::time::interval(std::time::Duration::from_secs(PING_INTERVAL_SECS));
        interval.tick().await; // consume the immediate first tick -> real 5s cadence
        let mut seq: u64 = 0;
        loop {
            interval.tick().await;
            seq += 1;
            ping_out.send(&Msg::Ping { seq });
        }
    });

    let mut sessions: HashMap<u64, Session> = HashMap::new();
    let mut sub_task: Option<JoinHandle<()>> = None;

    // Main stdin loop. EOF or read error => shut everything down.
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    loop {
        let line = match lines.next_line().await {
            Ok(Some(line)) => line,
            Ok(None) | Err(_) => break,
        };
        let Some(msg) = parse_line(&line) else { continue };
        handle(msg, &out, &herdr_bin, &herdr_socket, &mut sessions, &mut sub_task);
    }

    // Shutdown: kill every child (SIGTERM survives our own exit), stop tasks.
    for (_, session) in sessions.drain() {
        let _ = session.ctrl.send(SessionCtrl::Close);
        if let Some(pid) = session.pid {
            sigterm(pid);
        }
        session.task.abort();
    }
    if let Some(t) = sub_task.take() {
        t.abort();
    }
    ping.abort();
    writer.abort();
    Ok(())
}

fn handle(
    msg: Msg,
    out: &Out,
    herdr_bin: &str,
    herdr_socket: &Path,
    sessions: &mut HashMap<u64, Session>,
    sub_task: &mut Option<JoinHandle<()>>,
) {
    match msg {
        Msg::Open { sid, pane, mode, cols, rows, takeover, session } => {
            // Replace any existing child on this sid.
            if let Some(old) = sessions.remove(&sid) {
                let _ = old.ctrl.send(SessionCtrl::Close);
                if let Some(pid) = old.pid {
                    sigterm(pid);
                }
                old.task.abort();
            }
            let argv = terminal_session_argv(
                herdr_bin,
                session.as_deref(),
                &mode,
                &pane,
                cols,
                rows,
                takeover,
            );
            match spawn_session(sid, argv, out.clone()) {
                Ok(session) => {
                    sessions.insert(sid, session);
                }
                Err(e) => {
                    out.send(&Msg::Exit { sid, code: -1 });
                    let _ = e; // spawn failure reported as an immediate exit
                }
            }
        }
        Msg::Input { sid, d } => {
            if let Some(s) = sessions.get(&sid) {
                let _ = s.ctrl.send(SessionCtrl::Input(d));
            }
        }
        Msg::Close { sid } => {
            if let Some(s) = sessions.remove(&sid) {
                let _ = s.ctrl.send(SessionCtrl::Close);
                // supervisor SIGTERMs and reaps; no direct kill needed here.
            }
        }
        Msg::Api { sid, method, params } => {
            let client = ApiClient::at(herdr_socket);
            let out = out.clone();
            tokio::spawn(async move {
                match client.request(&method, params).await {
                    Ok(result) => out.send(&Msg::ApiRes { sid, result: Some(result), error: None }),
                    Err(e) => out.send(&Msg::ApiRes { sid, result: None, error: Some(e.to_string()) }),
                }
            });
        }
        Msg::Sub { sid, subs } => {
            // A new sub replaces the previous one.
            if let Some(t) = sub_task.take() {
                t.abort();
            }
            let client = ApiClient::at(herdr_socket);
            let out = out.clone();
            *sub_task = Some(tokio::spawn(async move {
                match client.subscribe(subs).await {
                    Ok(mut stream) => {
                        while let Some(ev) = stream.next().await {
                            let raw = json!({ "event": ev.event, "data": ev.data }).to_string();
                            out.send(&Msg::Ev { sid, d: raw });
                        }
                    }
                    Err(e) => {
                        // surface subscribe failure on the sub's sid
                        out.send(&Msg::ApiRes { sid, result: None, error: Some(e.to_string()) });
                    }
                }
            }));
        }
        Msg::Exec { sid, cmd } => {
            let out = out.clone();
            tokio::spawn(async move {
                let (code, mut o) = match Command::new("sh").arg("-c").arg(&cmd).output().await {
                    Ok(output) => (
                        output.status.code().unwrap_or(-1),
                        String::from_utf8_lossy(&output.stdout).into_owned(),
                    ),
                    Err(e) => (-1, format!("exec error: {e}")),
                };
                if o.len() > EXEC_OUT_CAP {
                    o.truncate(EXEC_OUT_CAP);
                }
                out.send(&Msg::ExecRes { sid, code, out: o });
            });
        }
        // agent → client ops are never received; ignore.
        Msg::Hello { .. }
        | Msg::Line { .. }
        | Msg::Exit { .. }
        | Msg::ApiRes { .. }
        | Msg::Ev { .. }
        | Msg::ExecRes { .. }
        | Msg::Ping { .. } => {}
    }
}

/// Spawn a terminal-session child and its supervisor task.
fn spawn_session(sid: u64, argv: Vec<String>, out: Out) -> Result<Session> {
    let mut cmd = Command::new(&argv[0]);
    cmd.args(&argv[1..])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null());
    let mut child = cmd.spawn().map_err(|e| err(format!("spawn {}: {e}", argv[0])))?;
    let pid = child.id();
    let mut stdin = child.stdin.take().ok_or_else(|| err("child stdin missing"))?;
    let stdout = child.stdout.take().ok_or_else(|| err("child stdout missing"))?;
    let (ctrl_tx, mut ctrl_rx) = mpsc::unbounded_channel::<SessionCtrl>();

    let task = tokio::spawn(async move {
        let mut child_lines = BufReader::new(stdout).lines();
        loop {
            tokio::select! {
                line = child_lines.next_line() => {
                    match line {
                        Ok(Some(l)) => out.send(&Msg::Line { sid, d: l }),
                        // stdout closed: child is exiting on its own
                        Ok(None) | Err(_) => break,
                    }
                }
                ctrl = ctrl_rx.recv() => {
                    match ctrl {
                        Some(SessionCtrl::Input(d)) => {
                            let mut buf = d;
                            buf.push('\n');
                            if stdin.write_all(buf.as_bytes()).await.is_err() {
                                // child stdin gone; keep waiting on stdout/exit
                            } else {
                                let _ = stdin.flush().await;
                            }
                        }
                        // close, or the map dropped the sender
                        Some(SessionCtrl::Close) | None => {
                            if let Some(pid) = pid {
                                sigterm(pid);
                            }
                            break;
                        }
                    }
                }
            }
        }
        let code = match child.wait().await {
            Ok(status) => status.code().unwrap_or(-1),
            Err(_) => -1,
        };
        out.send(&Msg::Exit { sid, code });
    });

    Ok(Session { ctrl: ctrl_tx, pid, task })
}

/// Best-effort SIGTERM by pid. Signal delivery is queued by the kernel, so it
/// still lands even if this process exits immediately after.
fn sigterm(pid: u32) {
    unsafe {
        libc::kill(pid as libc::pid_t, libc::SIGTERM);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn argv_full() {
        let argv = terminal_session_argv(
            "/home/u/.local/bin/herdr",
            Some("work"),
            "control",
            "w9:p1",
            176,
            66,
            true,
        );
        assert_eq!(
            argv,
            vec![
                "/home/u/.local/bin/herdr",
                "--session",
                "work",
                "terminal",
                "session",
                "control",
                "w9:p1",
                "--cols",
                "176",
                "--rows",
                "66",
                "--takeover",
            ]
        );
    }

    #[test]
    fn argv_minimal() {
        let argv = terminal_session_argv("herdr", None, "observe", "p0", 80, 24, false);
        assert_eq!(
            argv,
            vec!["herdr", "terminal", "session", "observe", "p0", "--cols", "80", "--rows", "24"]
        );
        assert!(!argv.iter().any(|a| a == "--session"));
        assert!(!argv.iter().any(|a| a == "--takeover"));
    }

    #[test]
    fn argv_pane_not_shell_quoted() {
        // spawned directly: the pane id is one argv element, verbatim.
        let argv = terminal_session_argv("herdr", None, "observe", "a b:c", 80, 24, false);
        assert!(argv.contains(&"a b:c".to_string()));
    }

    #[test]
    fn parse_args_default_and_flag() {
        assert!(parse_args(&[]).unwrap().herdr_bin.is_none());
        let a = parse_args(&["--herdr-bin".into(), "/opt/herdr".into()]).unwrap();
        assert_eq!(a.herdr_bin.as_deref(), Some("/opt/herdr"));
        assert!(parse_args(&["--herdr-bin".into()]).is_err());
        assert!(parse_args(&["--bogus".into()]).is_err());
    }

    #[test]
    fn resolve_herdr_bin_explicit_wins() {
        assert_eq!(resolve_herdr_bin(Some("/opt/x".into())), "/opt/x");
    }

    #[test]
    fn resolve_herdr_socket_env_override() {
        std::env::set_var("HERDR_SOCKET_PATH", "/tmp/custom-herdr.sock");
        assert_eq!(resolve_herdr_socket(), PathBuf::from("/tmp/custom-herdr.sock"));
        std::env::remove_var("HERDR_SOCKET_PATH");
        // fallback ends with the well-known relative path
        assert!(resolve_herdr_socket().ends_with(".config/herdr/herdr.sock"));
    }
}
