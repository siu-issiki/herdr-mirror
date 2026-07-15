// Single-connection mux (local side). One long-lived ssh connection per host
// runs one remote `herdr-mirror agent` that multiplexes every terminal stream,
// api call, event subscription, and exec over the mux NDJSON protocol.
//
// This module owns:
//   * the single ssh child (spawn, liveness, backoff reconnect, self-deploy),
//   * a local unix socket (`<state_dir>/<host>-mux.sock`) where in-process
//     clients (wrappers/actions/daemon, added in later phases) speak the same
//     `protocol::Msg`,
//   * the routing table that maps (client, client-sid) <-> global sid and
//     re-opens / re-subscribes automatically across reconnects.
//
// Phase 2 scope: the mux runs *alongside* the existing per-pane/ControlMaster
// data plane without replacing it — nothing connects to the socket yet. See
// docs/mux-design.md.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::process::{Child, Command};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{timeout, Instant};

use crate::config::HostConfig;
use crate::protocol::{parse_line, Msg};
use crate::remote::SSH_COMMON_OPTS;
use crate::util::{err, Result};

/// Remote path of the agent binary when a host doesn't override `agent_bin`.
pub const DEFAULT_AGENT_BIN: &str = "~/.local/libexec/herdr-mirror";

/// No agent line (pings included) for this long ⇒ the link is dead.
const AGENT_SILENCE: Duration = Duration::from_secs(20);
/// How long to wait for the first `hello` after spawning the child.
const HELLO_TIMEOUT: Duration = Duration::from_secs(15);
/// Reconnect backoff schedule (seconds), clamped at the last entry.
const BACKOFF_SECS: [u64; 4] = [1, 2, 5, 10];
/// A session that stayed up at least this long resets the backoff.
const STABLE_UPTIME: Duration = Duration::from_secs(30);

/// Configuration for one host's mux task.
pub struct MuxConfig {
    pub host_name: String,
    pub target: String,
    /// Remote agent binary path (may contain `~`, expanded by the remote shell).
    pub agent_bin: String,
    pub state_dir: PathBuf,
    /// Test hook: when set, this exact argv is spawned instead of `ssh …`. Lets
    /// integration tests run the agent binary directly (no ssh, no deploy).
    pub spawn_cmd: Option<Vec<String>>,
}

impl MuxConfig {
    /// Build a host's mux config, applying the default remote agent path.
    pub fn from_host(host: &HostConfig, state_dir: &Path) -> MuxConfig {
        MuxConfig {
            host_name: host.name.clone(),
            target: host.target.clone(),
            agent_bin: host.agent_bin.clone().unwrap_or_else(|| DEFAULT_AGENT_BIN.into()),
            state_dir: state_dir.to_path_buf(),
            spawn_cmd: None,
        }
    }
}

/// The local mux socket for a host: `<state_dir>/<host>-mux.sock`.
pub fn sock_path(state_dir: &Path, host_name: &str) -> PathBuf {
    state_dir.join(format!("{host_name}-mux.sock"))
}

// ---------------------------------------------------------------------------
// Pure routing core
// ---------------------------------------------------------------------------

type ClientId = u64;
type ClientSid = u64;
type GlobalSid = u64;

/// Terminal `open` parameters retained for automatic re-open after a reconnect.
#[derive(Debug, Clone, PartialEq)]
struct OpenParams {
    pane: String,
    mode: String,
    cols: u32,
    rows: u32,
    takeover: bool,
    session: Option<String>,
}

/// What a global sid represents, which decides reconnect/teardown behaviour.
#[derive(Debug, Clone, PartialEq)]
enum SidKind {
    /// Long-lived terminal stream — re-opened on reconnect, closed on teardown.
    Terminal(OpenParams),
    /// Held events.subscribe — re-subscribed on reconnect.
    Sub(Vec<Value>),
    /// One-shot api request — errored back if the link drops first.
    Api,
    /// One-shot exec request — errored back if the link drops first.
    Exec,
}

#[derive(Debug, Clone)]
struct SidEntry {
    client: ClientId,
    client_sid: ClientSid,
    kind: SidKind,
}

/// A routing decision produced by the pure core; the async task performs the IO.
#[derive(Debug, Clone, PartialEq)]
enum Action {
    /// Forward a message to the agent (dropped if currently disconnected).
    ToAgent(Msg),
    /// Deliver a raw NDJSON line to a specific local client.
    ToClient { client: ClientId, line: String },
}

/// The mux routing table. Deliberately IO-free so it can be unit-tested: every
/// input returns the list of side effects to perform.
struct MuxState {
    next_global: GlobalSid,
    /// (client, client-sid) → global sid.
    fwd: HashMap<(ClientId, ClientSid), GlobalSid>,
    /// global sid → owner + kind.
    sids: HashMap<GlobalSid, SidEntry>,
    connected: bool,
}

impl MuxState {
    fn new() -> MuxState {
        MuxState { next_global: 1, fwd: HashMap::new(), sids: HashMap::new(), connected: false }
    }

    /// Map an incoming client sid to a global sid, allocating on first use.
    /// Re-using the same client sid (e.g. re-open) keeps its global sid so the
    /// agent replaces the child in place.
    fn map_or_alloc(&mut self, client: ClientId, client_sid: ClientSid) -> GlobalSid {
        if let Some(&g) = self.fwd.get(&(client, client_sid)) {
            return g;
        }
        let g = self.next_global;
        self.next_global += 1;
        self.fwd.insert((client, client_sid), g);
        g
    }

    fn drop_sid(&mut self, g: GlobalSid) {
        if let Some(e) = self.sids.remove(&g) {
            self.fwd.remove(&(e.client, e.client_sid));
        }
    }

    /// A client message → agent-bound traffic (with sid rewritten to global).
    fn on_client_msg(&mut self, client: ClientId, msg: Msg) -> Vec<Action> {
        match msg {
            Msg::Open { sid, pane, mode, cols, rows, takeover, session } => {
                let g = self.map_or_alloc(client, sid);
                self.sids.insert(
                    g,
                    SidEntry {
                        client,
                        client_sid: sid,
                        kind: SidKind::Terminal(OpenParams {
                            pane: pane.clone(),
                            mode: mode.clone(),
                            cols,
                            rows,
                            takeover,
                            session: session.clone(),
                        }),
                    },
                );
                vec![Action::ToAgent(Msg::Open { sid: g, pane, mode, cols, rows, takeover, session })]
            }
            Msg::Input { sid, d } => match self.fwd.get(&(client, sid)) {
                Some(&g) => vec![Action::ToAgent(Msg::Input { sid: g, d })],
                None => vec![],
            },
            Msg::Close { sid } => match self.fwd.get(&(client, sid)).copied() {
                Some(g) => {
                    self.drop_sid(g);
                    vec![Action::ToAgent(Msg::Close { sid: g })]
                }
                None => vec![],
            },
            Msg::Api { sid, method, params } => {
                if !self.connected {
                    return vec![Action::ToClient {
                        client,
                        line: Msg::ApiRes { sid, result: None, error: Some("mux disconnected".into()) }
                            .to_line(),
                    }];
                }
                let g = self.map_or_alloc(client, sid);
                self.sids.insert(g, SidEntry { client, client_sid: sid, kind: SidKind::Api });
                vec![Action::ToAgent(Msg::Api { sid: g, method, params })]
            }
            Msg::Sub { sid, subs } => {
                let g = self.map_or_alloc(client, sid);
                self.sids.insert(
                    g,
                    SidEntry { client, client_sid: sid, kind: SidKind::Sub(subs.clone()) },
                );
                vec![Action::ToAgent(Msg::Sub { sid: g, subs })]
            }
            Msg::Exec { sid, cmd } => {
                if !self.connected {
                    return vec![Action::ToClient {
                        client,
                        line: Msg::ExecRes { sid, code: -1, out: "mux disconnected".into() }.to_line(),
                    }];
                }
                let g = self.map_or_alloc(client, sid);
                self.sids.insert(g, SidEntry { client, client_sid: sid, kind: SidKind::Exec });
                vec![Action::ToAgent(Msg::Exec { sid: g, cmd })]
            }
            // agent→client ops are never legitimately sent by a client.
            Msg::Hello { .. }
            | Msg::Line { .. }
            | Msg::Exit { .. }
            | Msg::ApiRes { .. }
            | Msg::Ev { .. }
            | Msg::ExecRes { .. }
            | Msg::Ping { .. } => vec![],
        }
    }

    /// A client vanished: close every sid it owned on the agent and forget them.
    fn on_client_disconnect(&mut self, client: ClientId) -> Vec<Action> {
        let owned: Vec<GlobalSid> = self
            .sids
            .iter()
            .filter(|(_, e)| e.client == client)
            .map(|(g, _)| *g)
            .collect();
        let mut actions = Vec::new();
        for g in owned {
            self.drop_sid(g);
            actions.push(Action::ToAgent(Msg::Close { sid: g }));
        }
        actions
    }

    /// Rewrite an agent message's global sid back to the owning client's sid and
    /// address it to that client. `None` when the sid is unknown (stale echo).
    fn route_back(&self, g: GlobalSid, build: impl FnOnce(ClientSid) -> Msg) -> Option<Action> {
        let e = self.sids.get(&g)?;
        Some(Action::ToClient { client: e.client, line: build(e.client_sid).to_line() })
    }

    /// An agent message → the owning client (sid rewritten back to the client's).
    fn on_agent_msg(&mut self, msg: Msg) -> Vec<Action> {
        match msg {
            Msg::Line { sid, d } => self
                .route_back(sid, |cs| Msg::Line { sid: cs, d })
                .into_iter()
                .collect(),
            Msg::Ev { sid, d } => {
                self.route_back(sid, |cs| Msg::Ev { sid: cs, d }).into_iter().collect()
            }
            Msg::Exit { sid, code } => {
                let out = self.route_back(sid, |cs| Msg::Exit { sid: cs, code }).into_iter().collect();
                // terminal is gone; forget it so a reconnect won't re-open a corpse
                self.drop_sid(sid);
                out
            }
            Msg::ApiRes { sid, result, error } => {
                let kind = self.sids.get(&sid).map(|e| e.kind.clone());
                let out =
                    self.route_back(sid, |cs| Msg::ApiRes { sid: cs, result, error }).into_iter().collect();
                // api is one-shot; a Sub sid only sees ApiRes on subscribe failure,
                // and a failed sub shouldn't be auto-re-subscribed — drop both.
                if matches!(kind, Some(SidKind::Api) | Some(SidKind::Sub(_))) {
                    self.drop_sid(sid);
                }
                out
            }
            Msg::ExecRes { sid, code, out } => {
                let acts = self.route_back(sid, |cs| Msg::ExecRes { sid: cs, code, out }).into_iter().collect();
                self.drop_sid(sid);
                acts
            }
            // liveness/handshake handled by the connection task; ignore here.
            Msg::Hello { .. } | Msg::Ping { .. } => vec![],
            // client→agent ops never arrive from the agent.
            Msg::Open { .. }
            | Msg::Input { .. }
            | Msg::Close { .. }
            | Msg::Api { .. }
            | Msg::Sub { .. }
            | Msg::Exec { .. } => vec![],
        }
    }

    /// The agent link came up: re-open every terminal (notifying its client of
    /// the blip first) and re-subscribe every held sub, all on their existing
    /// global sids so echoes still route.
    fn on_agent_connected(&mut self) -> Vec<Action> {
        self.connected = true;
        let mut actions = Vec::new();
        // deterministic order keeps tests and logs stable
        let mut entries: Vec<(GlobalSid, SidEntry)> =
            self.sids.iter().map(|(g, e)| (*g, e.clone())).collect();
        entries.sort_by_key(|(g, _)| *g);
        for (g, e) in entries {
            match e.kind {
                SidKind::Terminal(p) => {
                    actions.push(Action::ToClient { client: e.client, line: closed_line(e.client_sid) });
                    actions.push(Action::ToAgent(Msg::Open {
                        sid: g,
                        pane: p.pane,
                        mode: p.mode,
                        cols: p.cols,
                        rows: p.rows,
                        takeover: p.takeover,
                        session: p.session,
                    }));
                }
                SidKind::Sub(subs) => {
                    actions.push(Action::ToAgent(Msg::Sub { sid: g, subs }));
                }
                SidKind::Api | SidKind::Exec => {}
            }
        }
        actions
    }

    /// The agent link dropped: fail every in-flight one-shot back to its client.
    fn on_agent_disconnected(&mut self) -> Vec<Action> {
        self.connected = false;
        let mut actions = Vec::new();
        let mut doomed = Vec::new();
        let mut entries: Vec<(GlobalSid, SidEntry)> =
            self.sids.iter().map(|(g, e)| (*g, e.clone())).collect();
        entries.sort_by_key(|(g, _)| *g);
        for (g, e) in entries {
            match e.kind {
                SidKind::Api => {
                    actions.push(Action::ToClient {
                        client: e.client,
                        line: Msg::ApiRes {
                            sid: e.client_sid,
                            result: None,
                            error: Some("mux disconnected".into()),
                        }
                        .to_line(),
                    });
                    doomed.push(g);
                }
                SidKind::Exec => {
                    actions.push(Action::ToClient {
                        client: e.client,
                        line: Msg::ExecRes { sid: e.client_sid, code: -1, out: "mux disconnected".into() }
                            .to_line(),
                    });
                    doomed.push(g);
                }
                SidKind::Terminal(_) | SidKind::Sub(_) => {}
            }
        }
        for g in doomed {
            self.drop_sid(g);
        }
        actions
    }
}

/// The reconnect notice pushed to a client whose terminal is being re-opened.
/// Not a `protocol::Msg` variant (there is no `closed` op); emitted verbatim so
/// a future wrapper can show a "reconnecting" state until the next full frame.
fn closed_line(client_sid: ClientSid) -> String {
    format!("{{\"op\":\"closed\",\"sid\":{client_sid},\"reason\":\"mux reconnecting\"}}\n")
}

// ---------------------------------------------------------------------------
// Async runtime: core task, connection task, socket server
// ---------------------------------------------------------------------------

/// Everything the core task reacts to, serialized through one channel so the
/// routing table never needs a lock.
enum CoreEvent {
    ClientConnected { id: ClientId, tx: mpsc::UnboundedSender<String> },
    ClientMsg { id: ClientId, msg: Msg },
    ClientDisconnected { id: ClientId },
    AgentConnected { tx: mpsc::UnboundedSender<String> },
    AgentDisconnected,
    AgentMsg { msg: Msg },
}

/// Owns the routing table and every live sender; single consumer, no locks.
async fn core_task(mut rx: mpsc::UnboundedReceiver<CoreEvent>) {
    let mut state = MuxState::new();
    let mut clients: HashMap<ClientId, mpsc::UnboundedSender<String>> = HashMap::new();
    let mut agent_tx: Option<mpsc::UnboundedSender<String>> = None;

    while let Some(ev) = rx.recv().await {
        let actions = match ev {
            CoreEvent::ClientConnected { id, tx } => {
                clients.insert(id, tx);
                Vec::new()
            }
            CoreEvent::ClientMsg { id, msg } => state.on_client_msg(id, msg),
            CoreEvent::ClientDisconnected { id } => {
                let a = state.on_client_disconnect(id);
                clients.remove(&id);
                a
            }
            CoreEvent::AgentConnected { tx } => {
                agent_tx = Some(tx);
                state.on_agent_connected()
            }
            CoreEvent::AgentDisconnected => {
                agent_tx = None;
                state.on_agent_disconnected()
            }
            CoreEvent::AgentMsg { msg } => state.on_agent_msg(msg),
        };
        for action in actions {
            match action {
                Action::ToAgent(m) => {
                    if let Some(tx) = &agent_tx {
                        let _ = tx.send(m.to_line());
                    }
                }
                Action::ToClient { client, line } => {
                    if let Some(tx) = clients.get(&client) {
                        let _ = tx.send(line);
                    }
                }
            }
        }
    }
}

/// Build the child command: the test override wins, else `ssh <opts> <target>
/// exec <agent_bin> agent`.
fn build_command(cfg: &MuxConfig) -> Command {
    if let Some(argv) = &cfg.spawn_cmd {
        let mut cmd = Command::new(&argv[0]);
        cmd.args(&argv[1..]);
        cmd
    } else {
        let mut cmd = Command::new("ssh");
        cmd.args(SSH_COMMON_OPTS);
        cmd.arg(&cfg.target);
        cmd.arg(format!("exec {} agent", cfg.agent_bin));
        cmd
    }
}

/// Outcome of one connection attempt.
enum Attempt {
    /// Handshook and ran until the link died; carries the session uptime.
    Ran(Duration),
    /// No/mismatched `hello` (or immediate ssh failure) — try a self-deploy.
    NeedsDeploy,
    /// Failed to even spawn.
    SpawnFailed(crate::util::Error),
}

/// Spawn the child, validate `hello`, then pump agent lines to the core until
/// the link goes silent (>20s) or closes. Kills the child on the way out.
async fn connect_once(
    cfg: &MuxConfig,
    core_tx: &mpsc::UnboundedSender<CoreEvent>,
    ssh_pid: &AtomicI32,
) -> Attempt {
    let mut cmd = build_command(cfg);
    cmd.stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true);
    let mut child: Child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => return Attempt::SpawnFailed(err(format!("spawn mux transport: {e}"))),
    };
    if let Some(pid) = child.id() {
        ssh_pid.store(pid as i32, Ordering::SeqCst);
    }
    let stdout = match child.stdout.take() {
        Some(s) => s,
        None => return Attempt::SpawnFailed(err("mux transport stdout missing")),
    };
    let mut lines = BufReader::new(stdout).lines();

    // First line must be a matching hello, or we redeploy.
    let hello = match timeout(HELLO_TIMEOUT, lines.next_line()).await {
        Ok(Ok(Some(line))) => parse_line(&line),
        _ => None,
    };
    match hello {
        Some(Msg::Hello { version, .. }) if version == env!("CARGO_PKG_VERSION") => {}
        _ => {
            let _ = child.start_kill();
            let _ = child.wait().await;
            return Attempt::NeedsDeploy;
        }
    }

    // Handshake ok: wire stdin and announce the live link to the core.
    let stdin = match child.stdin.take() {
        Some(s) => s,
        None => return Attempt::SpawnFailed(err("mux transport stdin missing")),
    };
    let (atx, arx) = mpsc::unbounded_channel::<String>();
    tokio::spawn(agent_stdin_writer(arx, stdin));
    let _ = core_tx.send(CoreEvent::AgentConnected { tx: atx });

    let start = Instant::now();
    // Any non-line result (EOF, read error, or >20s silence) means a dead link.
    while let Ok(Ok(Some(line))) = timeout(AGENT_SILENCE, lines.next_line()).await {
        if let Some(msg) = parse_line(&line) {
            let _ = core_tx.send(CoreEvent::AgentMsg { msg });
        }
    }
    let _ = child.start_kill();
    let _ = child.wait().await;
    Attempt::Ran(start.elapsed())
}

/// Drain queued lines to the agent's stdin; ends when the channel closes (link
/// torn down) or the pipe breaks.
async fn agent_stdin_writer(
    mut rx: mpsc::UnboundedReceiver<String>,
    mut stdin: tokio::process::ChildStdin,
) {
    while let Some(line) = rx.recv().await {
        if stdin.write_all(line.as_bytes()).await.is_err() {
            break;
        }
        let _ = stdin.flush().await;
    }
}

/// Reconnect forever: spawn, run, then back off — self-deploying the binary
/// once if the remote agent is absent or stale.
async fn connection_task(cfg: Arc<MuxConfig>, core_tx: mpsc::UnboundedSender<CoreEvent>, ssh_pid: Arc<AtomicI32>) {
    let mut deployed = false;
    let mut backoff_idx = 0usize;
    loop {
        let outcome = connect_once(&cfg, &core_tx, &ssh_pid).await;
        ssh_pid.store(-1, Ordering::SeqCst);
        // Whether or not we were ever "connected", make the core drop stale
        // one-shots and mark itself offline (idempotent when never connected).
        let _ = core_tx.send(CoreEvent::AgentDisconnected);
        match outcome {
            Attempt::Ran(uptime) => {
                if uptime >= STABLE_UPTIME {
                    backoff_idx = 0;
                }
            }
            Attempt::NeedsDeploy => {
                if !deployed {
                    deployed = true;
                    match deploy(&cfg).await {
                        Ok(()) => continue, // retry immediately with the fresh binary
                        Err(_e) => { /* fall through to backoff */ }
                    }
                }
            }
            Attempt::SpawnFailed(_e) => { /* fall through to backoff */ }
        }
        let delay = BACKOFF_SECS[backoff_idx.min(BACKOFF_SECS.len() - 1)];
        backoff_idx = (backoff_idx + 1).min(BACKOFF_SECS.len() - 1);
        tokio::time::sleep(Duration::from_secs(delay)).await;
    }
}

/// Push the local binary to the remote agent path (same-arch assumption).
async fn deploy(cfg: &MuxConfig) -> Result<()> {
    // Never deploy in test mode — spawn_cmd runs a local agent, no ssh involved.
    if cfg.spawn_cmd.is_some() {
        return Err(err("deploy skipped: spawn_cmd override active"));
    }
    let exe = std::env::current_exe()?;
    let bytes = tokio::fs::read(&exe).await?;
    let bin = &cfg.agent_bin;
    let dir = bin.rsplit_once('/').map(|(d, _)| d).unwrap_or(".");
    let script = format!(
        "mkdir -p {dir} && cat > {bin}.new && chmod +x {bin}.new && mv {bin}.new {bin}"
    );
    let mut cmd = Command::new("ssh");
    cmd.args(SSH_COMMON_OPTS)
        .arg(&cfg.target)
        .arg(script)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    let mut child = cmd.spawn()?;
    let mut stdin = child.stdin.take().ok_or_else(|| err("deploy stdin missing"))?;
    stdin.write_all(&bytes).await?;
    drop(stdin); // EOF so the remote `cat` completes
    let status = child.wait().await?;
    if !status.success() {
        return Err(err(format!("deploy failed: exit {:?}", status.code())));
    }
    Ok(())
}

/// One local client connection: reader → core, writer ← core.
async fn client_conn(id: ClientId, stream: UnixStream, core_tx: mpsc::UnboundedSender<CoreEvent>) {
    let (read, mut write) = stream.into_split();
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<String>();
    if core_tx.send(CoreEvent::ClientConnected { id, tx: out_tx }).is_err() {
        return;
    }
    // Writer: drain core-produced lines to the client socket.
    let writer = tokio::spawn(async move {
        while let Some(line) = out_rx.recv().await {
            if write.write_all(line.as_bytes()).await.is_err() {
                break;
            }
        }
    });
    // Reader: parse client NDJSON into core events (ends on EOF/error).
    let mut lines = BufReader::new(read).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        if let Some(msg) = parse_line(&line) {
            if core_tx.send(CoreEvent::ClientMsg { id, msg }).is_err() {
                break;
            }
        }
    }
    let _ = core_tx.send(CoreEvent::ClientDisconnected { id });
    writer.abort();
}

/// Bind the local socket and accept clients forever.
async fn accept_loop(listener: UnixListener, core_tx: mpsc::UnboundedSender<CoreEvent>) -> Result<()> {
    let next_id = AtomicU64::new(1);
    loop {
        let (stream, _) = listener.accept().await?;
        let id = next_id.fetch_add(1, Ordering::Relaxed);
        tokio::spawn(client_conn(id, stream, core_tx.clone()));
    }
}

/// Run the mux for one host until cancelled. Returns early (Ok) if another mux
/// already owns the socket. All concurrent loops live in this one task so
/// cancelling it tears the whole mux down.
pub async fn serve(cfg: MuxConfig, ssh_pid: Arc<AtomicI32>) -> Result<()> {
    let sock = sock_path(&cfg.state_dir, &cfg.host_name);
    // Multi-start guard: a live socket means another mux owns this host.
    if UnixStream::connect(&sock).await.is_ok() {
        return Ok(());
    }
    let _ = std::fs::remove_file(&sock); // clear a stale socket file
    let listener = UnixListener::bind(&sock)
        .map_err(|e| err(format!("bind {}: {e}", sock.display())))?;

    let (core_tx, core_rx) = mpsc::unbounded_channel::<CoreEvent>();
    let cfg = Arc::new(cfg);

    // Three concurrent loops in one task: cancelling `serve` cancels all of
    // them (and drops the ssh Child via kill_on_drop).
    tokio::select! {
        _ = core_task(core_rx) => {}
        _ = connection_task(cfg.clone(), core_tx.clone(), ssh_pid) => {}
        r = accept_loop(listener, core_tx.clone()) => { r?; }
    }
    let _ = std::fs::remove_file(&sock);
    Ok(())
}

/// Handle a running host mux; the daemon keeps one per host.
pub struct MuxHandle {
    task: JoinHandle<()>,
    ssh_pid: Arc<AtomicI32>,
}

impl MuxHandle {
    /// Stop the mux and kill its ssh child. Aborting first prevents the
    /// connection loop from re-spawning after we signal the transport; the
    /// killed ssh then drops the remote agent, which reaps its terminal
    /// children (the 2026-07-14 leak guard).
    pub fn shutdown(&self) {
        self.task.abort();
        let pid = self.ssh_pid.swap(-1, Ordering::SeqCst);
        if pid > 0 {
            unsafe {
                libc::kill(pid, libc::SIGTERM);
            }
        }
    }
}

/// Start a host's mux as a background task (daemon entry point).
pub fn spawn(host: &HostConfig, state_dir: &Path) -> MuxHandle {
    let cfg = MuxConfig::from_host(host, state_dir);
    spawn_with(cfg)
}

/// Hidden `herdr-mirror mux …` entry: run a single mux to completion. Not used
/// in production (the daemon calls `spawn`); it exists so integration tests can
/// drive the mux as a subprocess with a `--spawn-cmd` transport override. The
/// socket is `<state-dir>/<host>-mux.sock`.
///
///   mux --state-dir <dir> --host <name> [--target <t>] [--agent-bin <p>]
///       [--spawn-cmd '<json argv array>']
pub async fn run_cli(rest: &[String]) -> Result<()> {
    let mut host = String::from("mux");
    let mut state_dir = std::env::temp_dir();
    let mut target = String::new();
    let mut agent_bin = DEFAULT_AGENT_BIN.to_string();
    let mut spawn_cmd: Option<Vec<String>> = None;
    let mut it = rest.iter();
    while let Some(arg) = it.next() {
        let mut next = || it.next().cloned().ok_or_else(|| err(format!("mux: {arg} needs a value")));
        match arg.as_str() {
            "--host" => host = next()?,
            "--state-dir" => state_dir = PathBuf::from(next()?),
            "--target" => target = next()?,
            "--agent-bin" => agent_bin = next()?,
            "--spawn-cmd" => {
                let json = next()?;
                spawn_cmd = Some(
                    serde_json::from_str(&json)
                        .map_err(|e| err(format!("mux: --spawn-cmd must be a JSON string array: {e}")))?,
                );
            }
            other => return Err(err(format!("mux: unexpected argument {other}"))),
        }
    }
    let cfg = MuxConfig { host_name: host, target, agent_bin, state_dir, spawn_cmd };
    serve(cfg, Arc::new(AtomicI32::new(-1))).await
}

/// Start a mux from an explicit config (used by `spawn` and the test harness).
pub fn spawn_with(cfg: MuxConfig) -> MuxHandle {
    let ssh_pid = Arc::new(AtomicI32::new(-1));
    let pid = ssh_pid.clone();
    let task = tokio::spawn(async move {
        let _ = serve(cfg, pid).await;
    });
    MuxHandle { task, ssh_pid }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn open(sid: u64, pane: &str) -> Msg {
        Msg::Open {
            sid,
            pane: pane.into(),
            mode: "control".into(),
            cols: 80,
            rows: 24,
            takeover: false,
            session: None,
        }
    }

    fn to_agent(actions: &[Action]) -> Vec<Msg> {
        actions
            .iter()
            .filter_map(|a| match a {
                Action::ToAgent(m) => Some(m.clone()),
                _ => None,
            })
            .collect()
    }

    fn to_client(actions: &[Action]) -> Vec<(ClientId, String)> {
        actions
            .iter()
            .filter_map(|a| match a {
                Action::ToClient { client, line } => Some((*client, line.clone())),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn open_allocates_distinct_global_sids_per_client() {
        let mut s = MuxState::new();
        s.connected = true;
        // two clients both use client-sid 1 → distinct globals
        let a1 = s.on_client_msg(10, open(1, "pA"));
        let a2 = s.on_client_msg(20, open(1, "pB"));
        let g1 = match &to_agent(&a1)[0] {
            Msg::Open { sid, .. } => *sid,
            _ => panic!(),
        };
        let g2 = match &to_agent(&a2)[0] {
            Msg::Open { sid, .. } => *sid,
            _ => panic!(),
        };
        assert_ne!(g1, g2, "distinct clients get distinct global sids");
    }

    #[test]
    fn agent_line_routes_back_with_client_sid() {
        let mut s = MuxState::new();
        s.connected = true;
        let opened = s.on_client_msg(7, open(42, "p"));
        let g = match &to_agent(&opened)[0] {
            Msg::Open { sid, .. } => *sid,
            _ => panic!(),
        };
        // agent emits a line on the global sid; it must reach client 7 as sid 42
        let acts = s.on_agent_msg(Msg::Line { sid: g, d: "frame".into() });
        let delivered = to_client(&acts);
        assert_eq!(delivered.len(), 1);
        assert_eq!(delivered[0].0, 7);
        let v: Value = serde_json::from_str(delivered[0].1.trim()).unwrap();
        assert_eq!(v["op"], "l");
        assert_eq!(v["sid"], 42);
        assert_eq!(v["d"], "frame");
    }

    #[test]
    fn client_disconnect_closes_all_its_sids() {
        let mut s = MuxState::new();
        s.connected = true;
        s.on_client_msg(1, open(1, "p1"));
        s.on_client_msg(1, open(2, "p2"));
        s.on_client_msg(1, Msg::Sub { sid: 3, subs: vec![json!({"type": "x"})] });
        let acts = s.on_client_disconnect(1);
        let closes = to_agent(&acts);
        assert_eq!(closes.len(), 3, "every owned sid is closed on the agent");
        assert!(closes.iter().all(|m| matches!(m, Msg::Close { .. })));
        // routing table is now empty
        assert!(s.sids.is_empty());
        assert!(s.fwd.is_empty());
    }

    #[test]
    fn close_frees_the_mapping() {
        let mut s = MuxState::new();
        s.connected = true;
        s.on_client_msg(1, open(5, "p"));
        assert_eq!(s.sids.len(), 1);
        let acts = s.on_client_msg(1, Msg::Close { sid: 5 });
        assert!(matches!(to_agent(&acts)[0], Msg::Close { .. }));
        assert!(s.sids.is_empty());
        // input after close is dropped (no mapping)
        assert!(s.on_client_msg(1, Msg::Input { sid: 5, d: "x".into() }).is_empty());
    }

    #[test]
    fn exit_forgets_terminal_so_reconnect_wont_reopen_it() {
        let mut s = MuxState::new();
        s.connected = true;
        let opened = s.on_client_msg(1, open(1, "p"));
        let g = match &to_agent(&opened)[0] {
            Msg::Open { sid, .. } => *sid,
            _ => panic!(),
        };
        let acts = s.on_agent_msg(Msg::Exit { sid: g, code: 0 });
        // client sees the exit …
        let v: Value = serde_json::from_str(to_client(&acts)[0].1.trim()).unwrap();
        assert_eq!(v["op"], "exit");
        assert_eq!(v["sid"], 1);
        // … and the terminal is gone, so a reconnect re-opens nothing
        assert!(s.on_agent_connected().is_empty());
    }

    #[test]
    fn reconnect_reopens_terminals_and_resubs() {
        let mut s = MuxState::new();
        s.connected = true;
        s.on_client_msg(1, open(1, "pane-a"));
        s.on_client_msg(1, Msg::Sub { sid: 2, subs: vec![json!({"type": "workspace"})] });
        // link drops, then comes back
        s.on_agent_disconnected();
        let acts = s.on_agent_connected();
        // client gets a "closed / reconnecting" notice for its terminal sid
        let notices = to_client(&acts);
        assert_eq!(notices.len(), 1);
        let v: Value = serde_json::from_str(notices[0].1.trim()).unwrap();
        assert_eq!(v["op"], "closed");
        assert_eq!(v["sid"], 1);
        // agent gets a re-open (same global sid) and a re-sub
        let agent = to_agent(&acts);
        assert!(agent.iter().any(|m| matches!(m, Msg::Open { pane, .. } if pane == "pane-a")));
        assert!(agent.iter().any(|m| matches!(m, Msg::Sub { .. })));
    }

    #[test]
    fn disconnect_errors_pending_oneshots() {
        let mut s = MuxState::new();
        s.connected = true;
        s.on_client_msg(1, Msg::Api { sid: 1, method: "m".into(), params: json!({}) });
        s.on_client_msg(1, Msg::Exec { sid: 2, cmd: "sleep 1".into() });
        let acts = s.on_agent_disconnected();
        let delivered = to_client(&acts);
        assert_eq!(delivered.len(), 2);
        let ops: Vec<String> = delivered
            .iter()
            .map(|(_, l)| serde_json::from_str::<Value>(l.trim()).unwrap()["op"].as_str().unwrap().into())
            .collect();
        assert!(ops.contains(&"api_res".to_string()));
        assert!(ops.contains(&"exec_res".to_string()));
        // one-shots are cleared; a reconnect re-opens nothing
        assert!(s.sids.is_empty());
    }

    #[test]
    fn api_and_exec_while_disconnected_error_immediately() {
        let mut s = MuxState::new(); // connected == false
        let a = s.on_client_msg(1, Msg::Api { sid: 1, method: "m".into(), params: json!({}) });
        let ca = to_client(&a);
        assert_eq!(ca.len(), 1);
        assert_eq!(serde_json::from_str::<Value>(ca[0].1.trim()).unwrap()["op"], "api_res");
        assert!(to_agent(&a).is_empty(), "nothing goes to a dead agent");

        let e = s.on_client_msg(1, Msg::Exec { sid: 2, cmd: "x".into() });
        let ce = to_client(&e);
        assert_eq!(serde_json::from_str::<Value>(ce[0].1.trim()).unwrap()["op"], "exec_res");
    }

    #[test]
    fn api_res_routes_and_clears_the_oneshot() {
        let mut s = MuxState::new();
        s.connected = true;
        let sent = s.on_client_msg(3, Msg::Api { sid: 9, method: "m".into(), params: json!({}) });
        let g = match &to_agent(&sent)[0] {
            Msg::Api { sid, .. } => *sid,
            _ => panic!(),
        };
        let acts = s.on_agent_msg(Msg::ApiRes { sid: g, result: Some(json!({"ok": true})), error: None });
        let d = to_client(&acts);
        assert_eq!(d[0].0, 3);
        let v: Value = serde_json::from_str(d[0].1.trim()).unwrap();
        assert_eq!(v["sid"], 9);
        assert_eq!(v["op"], "api_res");
        assert!(s.sids.is_empty(), "api sid cleared after its response");
    }

    #[test]
    fn reopen_uses_same_global_sid_on_repeat_open() {
        let mut s = MuxState::new();
        s.connected = true;
        let a1 = s.on_client_msg(1, open(1, "p"));
        let a2 = s.on_client_msg(1, open(1, "p")); // same client-sid re-open
        let g1 = match &to_agent(&a1)[0] {
            Msg::Open { sid, .. } => *sid,
            _ => panic!(),
        };
        let g2 = match &to_agent(&a2)[0] {
            Msg::Open { sid, .. } => *sid,
            _ => panic!(),
        };
        assert_eq!(g1, g2, "re-open on the same client-sid keeps its global sid");
        assert_eq!(s.sids.len(), 1);
    }

    #[test]
    fn sock_path_shape() {
        let p = sock_path(Path::new("/var/state"), "work");
        assert_eq!(p, PathBuf::from("/var/state/work-mux.sock"));
    }

    #[test]
    fn closed_line_is_valid_ndjson() {
        let l = closed_line(7);
        assert!(l.ends_with('\n'));
        let v: Value = serde_json::from_str(l.trim()).unwrap();
        assert_eq!(v["op"], "closed");
        assert_eq!(v["sid"], 7);
        assert_eq!(v["reason"], "mux reconnecting");
    }
}
