// herdr-mirror pane wrapper (data plane).
//
// Runs inside a local herdr pane and shows a remote herdr pane's terminal,
// live, over ssh. Read-only observe by default; escalates to a writable
// control session when the user types and releases back to observe.
//
//   herdr-mirror pane <ssh-target> <pane-target> [options]
//
// options:
//   --remote-bin PATH   remote herdr binary (default ~/.local/bin/herdr)
//   --cols N --rows N   observe request size (default 240x72; must be >= the
//                       remote PTY size or the server clips bottom rows away)
//   --dump              headless mode: print plain-text screen per frame
//   --session NAME      remote named session (passed as --session to herdr)
//   --control-idle N    auto-release control after N seconds idle (default 3600)
//   --always-control    start and stay in control: writable, no idle release,
//                       and sized to the local pane so it fills
//
// Every stream gets its own direct ssh connection (no shared ControlMaster):
// isolated, and nothing persists to go stale on a flaky network.
//
// One owner of all state, message-driven: frames, keystrokes, timers, and
// ssh-child exits arrive on one channel; a session generation number tags
// every message so stale ones are dropped.

use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use serde::Deserialize;
use serde_json::json;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::UnixStream;
use tokio::process::ChildStdin;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::foreground::Foreground;
use crate::grid::{normalize_selection, window_offset, Grid, Renderer};
use crate::predict::Predictor;
use crate::protocol;
use crate::util::{err, Result};

// ---------------------------------------------------------------------------
// args

#[derive(Debug, Clone)]
pub struct Args {
    pub ssh_target: String,
    pub pane_target: String,
    pub remote_bin: String,
    pub cols: usize,
    pub rows: usize,
    pub dump: bool,
    pub session: Option<String>,
    /// auto-release control after this much input idle; 0 disables
    pub control_idle_secs: u64,
    /// start and stay in control: writable, no idle release, and sized to the
    /// local pane so it fills. Set by the daemon from per-host config.
    pub always_control: bool,
    /// daemon's ssh ControlMaster socket for this host; foreground polls reuse it
    /// (`ssh -S <path>`) to skip a handshake. None → polls connect directly.
    pub ctl_path: Option<String>,
    /// host's single mux socket (`<state_dir>/<host>-mux.sock`). When set, the
    /// wrapper opens sessions / sends input / polls the foreground over this one
    /// unix connection instead of spawning an ssh child per stream. If the socket
    /// can't be reached (mux not up yet) it falls back to the ssh path below, and
    /// retries the mux on the next (re)connect. None → ssh transport only.
    pub mux_sock: Option<String>,
    /// optimistic-split pending mode: started before the remote pane exists, with
    /// no pane-target positional. The wrapper shows "connecting…" and polls its
    /// claim file (`claim-<HERDR_PANE_ID>.json`) for the remote pane id (or an
    /// error) before entering the normal connect flow.
    pub pending: bool,
}

pub fn parse_args(argv: &[String]) -> Result<Args> {
    let mut args = Args {
        ssh_target: String::new(),
        pane_target: String::new(),
        remote_bin: "~/.local/bin/herdr".into(),
        cols: 240,
        rows: 72,
        dump: false,
        session: None,
        control_idle_secs: 3600,
        always_control: false,
        ctl_path: None,
        mux_sock: None,
        pending: false,
    };
    let mut positional: Vec<String> = Vec::new();
    let mut it = argv.iter();
    while let Some(a) = it.next() {
        let mut next = |flag: &str| -> Result<String> {
            it.next().cloned().ok_or_else(|| err(format!("{flag} needs a value")))
        };
        match a.as_str() {
            "--remote-bin" => args.remote_bin = next("--remote-bin")?,
            "--cols" => {
                args.cols = next("--cols")?.parse().map_err(|_| err("--cols must be a number"))?;
            }
            "--rows" => {
                args.rows = next("--rows")?.parse().map_err(|_| err("--rows must be a number"))?;
            }
            "--session" => args.session = Some(next("--session")?),
            "--control-idle" => {
                args.control_idle_secs =
                    next("--control-idle")?.parse().map_err(|_| err("--control-idle must be a number"))?
            }
            "--always-control" => args.always_control = true,
            "--ctl-path" => args.ctl_path = Some(next("--ctl-path")?),
            "--mux-sock" => args.mux_sock = Some(next("--mux-sock")?),
            "--pending" => args.pending = true,
            "--dump" => args.dump = true,
            other if other.starts_with('-') => return Err(err(format!("unknown option: {other}"))),
            other => positional.push(other.to_string()),
        }
    }
    // --pending starts before the remote pane exists, so it carries only the
    // ssh-target; the pane-target is filled in later from the claim file.
    if args.pending {
        if positional.len() != 1 {
            return Err(err(
                "usage: herdr-mirror pane <ssh-target> --pending [options] (no pane-target)",
            ));
        }
        args.ssh_target = positional.remove(0);
    } else {
        if positional.len() != 2 {
            return Err(err(
                "usage: herdr-mirror pane <ssh-target> <pane-target> [--remote-bin PATH] [--cols N --rows N] [--dump]",
            ));
        }
        args.ssh_target = positional.remove(0);
        args.pane_target = positional.remove(0);
    }
    Ok(args)
}

/// Poll this pending pane's claim file for the remote pane id the action is
/// splitting for us, or a failure. Returns `Ok(remote_pane_id)` on a `pane`
/// claim, `Err(msg)` on an `error` claim or timeout. Pure-ish: the state dir
/// and self pane id are passed in so it's testable without process env.
async fn await_claim(
    state_dir: &std::path::Path,
    self_pane_id: &str,
    poll_interval: Duration,
    timeout: Duration,
) -> std::result::Result<String, String> {
    if self_pane_id.is_empty() {
        return Err("HERDR_PANE_ID not set — cannot resolve the pending split".into());
    }
    let path = crate::util::claim_path(state_dir, self_pane_id);
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(body) = std::fs::read_to_string(&path) {
            match crate::util::parse_claim(&body) {
                crate::util::Claim::Pane(rid) => {
                    let _ = std::fs::remove_file(&path);
                    return Ok(rid);
                }
                crate::util::Claim::Error(msg) => {
                    let _ = std::fs::remove_file(&path);
                    return Err(msg);
                }
                // half-written / not-yet-meaningful: keep polling, leave the file
                crate::util::Claim::Invalid => {}
            }
        }
        if Instant::now() >= deadline {
            return Err("timed out waiting for the remote split".into());
        }
        tokio::time::sleep(poll_interval).await;
    }
}

// ---------------------------------------------------------------------------
// remote session: one ssh child running observe or control

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Observe,
    Control,
}

impl Mode {
    fn as_str(self) -> &'static str {
        match self {
            Mode::Observe => "observe",
            Mode::Control => "control",
        }
    }
}

#[derive(Debug, Deserialize)]
struct Frame {
    #[serde(rename = "type")]
    kind: String,
    seq: Option<u64>,
    full: Option<bool>,
    width: Option<usize>,
    height: Option<usize>,
    bytes: Option<String>,
    reason: Option<String>,
}

enum Msg {
    Frame { gen: u64, frame: Frame },
    SessionExit { gen: u64, mode: Mode, reason: String, uptime: Duration },
    Stdin(Vec<u8>),
    /// result of a background foreground-process poll: `Some(_)` = classified
    /// (shell/agent/TUI), `None` = poll failed (keep last value)
    Foreground(Option<Foreground>),
    /// mux transport: an agent terminal child exited (`exit` op). sid == gen.
    MuxExit { sid: u64, code: i32 },
    /// mux transport: the mux is re-opening the agent link (`{"op":"closed",…}`).
    /// Status-only blip — the session is kept; a full frame follows on recovery.
    MuxReconnecting,
    /// mux transport: the wrapper→mux socket hit EOF (mux/daemon gone). Treated
    /// as the current session exiting so the normal reconnect flow re-establishes.
    MuxClosed,
}

/// How a session reaches the remote terminal child: either a dedicated ssh child
/// (legacy, per-stream) or the shared single mux connection.
enum Transport {
    /// legacy: an ssh child; input goes to its stdin, teardown SIGTERMs its pid.
    Ssh { pid: i32, stdin: ChildStdin },
    /// mux: input is wrapped as an `input` op and teardown sends `close`, both on
    /// the shared write channel. `sid` (== the session's gen) tags every line.
    Mux { sid: u64, tx: mpsc::UnboundedSender<String> },
}

struct Session {
    gen: u64,
    mode: Mode,
    /// when this session was opened — used to compute the exit uptime for mux
    /// sessions (the ssh reader computes its own). Drives control-failure backoff.
    started: Instant,
    transport: Transport,
}

impl Session {
    /// Send a raw child-stdin JSON line (a trailing newline is optional; the mux
    /// path strips it since the agent re-appends one).
    async fn send_input(&mut self, line: &str) {
        match &mut self.transport {
            Transport::Ssh { stdin, .. } => {
                let _ = stdin.write_all(line.as_bytes()).await;
            }
            Transport::Mux { sid, tx } => {
                let d = line.strip_suffix('\n').unwrap_or(line).to_string();
                let _ = tx.send(protocol::Msg::Input { sid: *sid, d }.to_line());
            }
        }
    }

    /// Tear the session down without a graceful release (caller does that first
    /// when needed): SIGTERM the ssh child, or send `close` so the agent reaps
    /// its child.
    fn terminate(&self) {
        match &self.transport {
            Transport::Ssh { pid, .. } => unsafe {
                libc::kill(*pid, libc::SIGTERM);
            },
            Transport::Mux { sid, tx } => {
                let _ = tx.send(protocol::Msg::Close { sid: *sid }.to_line());
            }
        }
    }
}

/// POSIX single-quote: an embedded ' can't break the remote shell parse.
pub(crate) fn sh_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

fn spawn_session(args: &Args, mode: Mode, cols: usize, rows: usize, gen: u64, tx: mpsc::Sender<Msg>) -> Result<Session> {
    let session_flag = args
        .session
        .as_ref()
        .map(|s| format!(" --session {}", sh_quote(s)))
        .unwrap_or_default();
    // always-control: this mirror is the pane's one intended client, so force
    // the takeover to evict any ghost attach a network drop left holding the
    // lock (otherwise every control reconnect is refused with "already has an
    // attached client" until someone manually intervenes). Never on observe —
    // a read-only session has no business stealing the attach.
    let takeover = if mode == Mode::Control && args.always_control { " --takeover" } else { "" };
    // remote_bin stays unquoted on purpose: the default ~/.local/bin/herdr
    // relies on remote-shell tilde expansion
    let cmd = format!(
        "exec {}{} terminal session {} {} --cols {} --rows {}{}",
        args.remote_bin,
        session_flag,
        mode.as_str(),
        sh_quote(&args.pane_target),
        cols,
        rows,
        takeover
    );
    let mut sc = tokio::process::Command::new(crate::remote::ssh_bin());
    // reuse the daemon's ControlMaster when it's alive; a missing socket
    // falls back to a direct connection (same pattern as foreground polls)
    if let Some(path) = &args.ctl_path {
        sc.arg("-S").arg(path);
    }
    let mut child = sc
        .args(crate::remote::SSH_COMMON_OPTS)
        .arg(&args.ssh_target)
        .arg(cmd)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let pid = child.id().map(|p| p as i32).unwrap_or(0);
    let stdin = child.stdin.take().ok_or_else(|| err("no child stdin"))?;
    let stdout = child.stdout.take().ok_or_else(|| err("no child stdout"))?;
    let stderr = child.stderr.take().ok_or_else(|| err("no child stderr"))?;
    let started = Instant::now();

    tokio::spawn(async move {
        // ssh errors arrive on stderr; the server's failure reason arrives as
        // a terminal.closed frame on STDOUT — capture both
        let err_tail: Arc<Mutex<String>> = Arc::new(Mutex::new(String::new()));
        let err_tail2 = err_tail.clone();
        let stderr_task = tokio::spawn(async move {
            let mut lines = BufReader::new(stderr).lines();
            while let Ok(Some(l)) = lines.next_line().await {
                let mut buf = err_tail2.lock().unwrap();
                buf.push_str(&l);
                buf.push('\n');
                if buf.len() > 400 {
                    let tail: String = buf.chars().rev().take(400).collect::<Vec<_>>().into_iter().rev().collect();
                    *buf = tail;
                }
            }
        });
        let mut close_reason = String::new();
        let mut lines = BufReader::new(stdout).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            let Ok(frame) = serde_json::from_str::<Frame>(&line) else { continue };
            if frame.kind == "terminal.closed" {
                if let Some(r) = &frame.reason {
                    close_reason = r.clone();
                }
            }
            if tx.send(Msg::Frame { gen, frame }).await.is_err() {
                break;
            }
        }
        let _ = child.wait().await;
        stderr_task.abort();
        let tail = err_tail.lock().unwrap().trim().to_string();
        let reason = if close_reason.is_empty() { tail } else { close_reason };
        let _ = tx.send(Msg::SessionExit { gen, mode, reason, uptime: started.elapsed() }).await;
    });

    Ok(Session { gen, mode, started, transport: Transport::Ssh { pid, stdin } })
}

// ---------------------------------------------------------------------------
// mux transport: one shared unix connection to the host's mux, replacing the
// per-stream ssh child. `open`/`input`/`close`/`api` go out on the write channel;
// `l`/`exit`/`api_res`/reconnect blips come back through the reader below.

/// api sids for foreground polls live above every plausible session gen so they
/// never collide (routing tolerates a collision anyway — terminals never emit
/// `api_res`, api polls never emit `l`/`exit` — but a disjoint range is tidier).
const MUX_API_SID_BASE: u64 = 1 << 40;

/// A parsed mux line as a wrapper-internal event. Pure (no I/O) so the L→Frame,
/// exit, and reconnect-blip mapping can be unit-tested; the reader task turns
/// these into channel messages.
#[derive(Debug)]
enum MuxEvent {
    /// terminal frame for this sid (== the session gen).
    Frame(u64, Frame),
    /// terminal child exited with this code.
    Exit(u64, i32),
    /// foreground `api_res`, already classified (`None` = indeterminate).
    Foreground(Option<Foreground>),
    /// `{"op":"closed",…}` — the mux is re-opening the agent link (status blip).
    Reconnecting,
    /// nothing actionable (ping / hello / unknown / malformed).
    Ignore,
}

/// Classify a foreground `api_res` result. The agent hands back the bare
/// `result` value, but `foreground::classify` expects the `{"result":{…}}`
/// envelope, so re-wrap before handing it over.
fn classify_result(result: Option<serde_json::Value>) -> Option<Foreground> {
    let result = result?;
    crate::foreground::classify(&json!({ "result": result }).to_string())
}

/// True for the mux's reconnect notice, the one raw line `parse_line` rejects
/// (there is no `closed` op) that the wrapper still acts on.
fn is_mux_reconnecting(line: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(line.trim())
        .ok()
        .and_then(|v| v.get("op").and_then(|o| o.as_str()).map(|s| s == "closed"))
        .unwrap_or(false)
}

/// Pure mux-line → event mapping (see `MuxEvent`).
fn mux_line_to_event(line: &str) -> MuxEvent {
    if let Some(msg) = protocol::parse_line(line) {
        match msg {
            protocol::Msg::Line { sid, d } => match serde_json::from_str::<Frame>(&d) {
                Ok(frame) => MuxEvent::Frame(sid, frame),
                Err(_) => MuxEvent::Ignore,
            },
            protocol::Msg::Exit { sid, code } => MuxEvent::Exit(sid, code),
            protocol::Msg::ApiRes { result, .. } => MuxEvent::Foreground(classify_result(result)),
            _ => MuxEvent::Ignore,
        }
    } else if is_mux_reconnecting(line) {
        MuxEvent::Reconnecting
    } else {
        MuxEvent::Ignore
    }
}

/// Reader task for the shared mux connection: demuxes agent lines onto the App
/// channel. Terminal `l`/`exit` carry the sid (== gen), so `handle_frame`/
/// `handle_exit`'s existing gen check drops any stale line from a replaced
/// session. On EOF it emits `MuxClosed` so the App re-establishes the link.
async fn mux_reader_task(rd: OwnedReadHalf, tx: mpsc::Sender<Msg>) {
    let mut lines = BufReader::new(rd).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let msg = match mux_line_to_event(&line) {
            MuxEvent::Frame(sid, frame) => Msg::Frame { gen: sid, frame },
            MuxEvent::Exit(sid, code) => Msg::MuxExit { sid, code },
            MuxEvent::Foreground(fg) => Msg::Foreground(fg),
            MuxEvent::Reconnecting => Msg::MuxReconnecting,
            MuxEvent::Ignore => continue,
        };
        if tx.send(msg).await.is_err() {
            return;
        }
    }
    let _ = tx.send(Msg::MuxClosed).await;
}

/// Writer task for the shared mux connection: drains the write channel to the
/// socket. Exits on write error, dropping the channel so senders see it closed.
async fn mux_writer_task(mut wr: OwnedWriteHalf, mut rx: mpsc::UnboundedReceiver<String>) {
    while let Some(line) = rx.recv().await {
        if wr.write_all(line.as_bytes()).await.is_err() {
            break;
        }
        let _ = wr.flush().await;
    }
}

// ---------------------------------------------------------------------------
// terminal plumbing

fn term_size() -> (usize, usize) {
    unsafe {
        let mut ws: libc::winsize = std::mem::zeroed();
        if libc::ioctl(libc::STDOUT_FILENO, libc::TIOCGWINSZ, &mut ws) == 0 && ws.ws_col > 0 && ws.ws_row > 0 {
            return (ws.ws_col as usize, ws.ws_row as usize);
        }
    }
    (80, 24)
}

struct RawMode {
    orig: libc::termios,
}

impl RawMode {
    fn enable() -> Option<RawMode> {
        unsafe {
            let mut orig: libc::termios = std::mem::zeroed();
            if libc::tcgetattr(libc::STDIN_FILENO, &mut orig) != 0 {
                return None;
            }
            let mut raw = orig;
            libc::cfmakeraw(&mut raw);
            if libc::tcsetattr(libc::STDIN_FILENO, libc::TCSANOW, &raw) != 0 {
                return None;
            }
            Some(RawMode { orig })
        }
    }

    fn restore(&self) {
        unsafe {
            libc::tcsetattr(libc::STDIN_FILENO, libc::TCSANOW, &self.orig);
        }
    }
}

fn write_stdout(s: &str) {
    use std::io::Write;
    let mut out = std::io::stdout().lock();
    let _ = out.write_all(s.as_bytes());
    let _ = out.flush();
}

/// One SGR mouse event: ESC [ < btn ; col ; row (M|m). Returns (btn, col, row,
/// press, total len) for a sequence starting at `bytes[at]`.
fn parse_mouse(bytes: &[u8], at: usize) -> Option<(u32, u32, u32, bool, usize)> {
    let rest = &bytes[at..];
    if rest.len() < 6 || rest[0] != 0x1b || rest[1] != b'[' || rest[2] != b'<' {
        return None;
    }
    let mut nums = [0u32; 3];
    let mut n = 0usize;
    let mut i = 3usize;
    let mut have_digit = false;
    while i < rest.len() && n < 3 {
        match rest[i] {
            b'0'..=b'9' => {
                // saturate: garbage digit runs on stdin must not overflow-panic
                nums[n] = nums[n].saturating_mul(10).saturating_add((rest[i] - b'0') as u32);
                have_digit = true;
                i += 1;
            }
            b';' if n < 2 && have_digit => {
                n += 1;
                have_digit = false;
                i += 1;
            }
            b'M' | b'm' if n == 2 && have_digit => {
                return Some((nums[0], nums[1], nums[2], rest[i] == b'M', i + 1));
            }
            _ => return None,
        }
    }
    None
}

fn contains_wheel_press(bytes: &[u8]) -> bool {
    let mut i = 0;
    while i < bytes.len() {
        if let Some((btn, _, _, press, len)) = parse_mouse(bytes, i) {
            if press && (btn == 64 || btn == 65) {
                return true;
            }
            i += len;
        } else {
            i += 1;
        }
    }
    false
}

fn has_mouse_seq(bytes: &[u8]) -> bool {
    bytes.windows(3).any(|w| w == [0x1b, b'[', b'<'])
}


// ---------------------------------------------------------------------------
// the wrapper state machine

const BACKOFF: [u64; 4] = [1000, 2000, 5000, 10000];
const SWITCH_GAP: Duration = Duration::from_millis(200);
const QUICK_CONTROL_FAILURE: Duration = Duration::from_secs(4);
/// always-control: how often to retry control while stuck observing after a
/// fallback. Slow enough that a repeatedly-refused control session can't spin
/// (each retry still needs 2 quick failures to fall back again).
const CONTROL_RETRY_INTERVAL: Duration = Duration::from_secs(20);

/// optimistic-split pending mode: how often, and how long, to poll the claim
/// file for the remote pane id the action is creating for us.
const PENDING_POLL_INTERVAL: Duration = Duration::from_millis(30);
const PENDING_TIMEOUT: Duration = Duration::from_secs(8);

struct App {
    args: Args,
    tty: bool,
    grid: Grid,
    renderer: Renderer,
    tx: mpsc::Sender<Msg>,

    mode: Mode,
    /// in-flight mode switch (guards fast re-entry)
    switching_to: Option<Mode>,
    switch_at: Option<Instant>,
    session: Option<Session>,
    next_gen: u64,

    backoff_idx: usize,
    reconnect_at: Option<(Instant, Mode)>,
    /// consecutive quick control failures → fall back to observe
    control_failures: u32,
    control_sticky: bool,
    /// always-control only: fell back to observe and waiting to retry control
    /// without needing a keystroke. Cleared as soon as a control switch is
    /// (re-)attempted; re-armed by `handle_exit`'s fallback if that attempt
    /// also fails quickly.
    control_retry_at: Option<Instant>,
    pending_input: Vec<Vec<u8>>,
    last_input: Instant,
    hint_clear_at: Option<Instant>,
    /// predictive local echo — draws keystrokes optimistically, frame-verified
    predict: Predictor,
    /// remote pane foreground classification (shell/agent/TUI), driving mouse
    /// policy. `None` = unknown (fail safe: grab on, clicks dropped). Refreshed
    /// lazily on mouse/keyboard activity via `herdr pane process-info`.
    foreground: Option<Foreground>,
    /// agent pane: a left-button press held until we learn it's a click (release
    /// with no motion) or a drag (motion). Holds the raw press SGR bytes (to
    /// forward on a click) and the anchor grid cell (for a drag selection).
    mouse_pending: Option<(Vec<u8>, (usize, usize))>,
    /// agent pane: an in-progress local drag selection as (anchor, current) grid
    /// cells, unnormalized (drag direction preserved).
    selecting: Option<((usize, usize), (usize, usize))>,
    /// last time a foreground poll was kicked off (throttles the ssh handshakes)
    fg_poll_at: Option<Instant>,
    /// scheduled delayed re-poll to catch a foreground change the last input just
    /// caused (e.g. quitting a TUI back to a shell); bypasses the throttle
    settle_at: Option<Instant>,
    /// whether the local mouse grab (?1002h) is currently on. Released at a shell
    /// so herdr does native selection/scroll; re-grabbed for a TUI so clicks can
    /// be forwarded.
    mouse_grabbed: bool,
    /// mux transport: write channel to the shared mux connection. `Some` once the
    /// connection is up; `None` before the first connect or after the socket
    /// dropped (re-established lazily by `ensure_mux`). Ignored in ssh mode.
    mux_tx: Option<mpsc::UnboundedSender<String>>,
    /// mux transport: monotonic counter for foreground-poll api sids (offset by
    /// `MUX_API_SID_BASE` so they stay clear of session gens).
    mux_api_sid: u64,
    /// set by `handle_frame` when a `terminal.closed` reason indicates the
    /// remote pane's process itself exited (see `closed_reason_is_pane_exit`).
    /// Checked by the main loop right after dispatching a frame so it can
    /// `break` into the existing clean-shutdown path instead of reconnecting.
    exit_requested: bool,
}

/// A terminal.closed reason of the form "terminal <id> exited" means the
/// remote pane's process ended (ctrl+d etc.) — the pane is gone for good,
/// not a transient attach/server condition (attach conflicts, takeover
/// refusals, and server restarts all use different reason text). Requires a
/// non-empty id between the fixed prefix/suffix rather than a plain
/// starts_with/ends_with check: for a short reason like "terminal exited"
/// those two patterns overlap (9-char prefix + 7-char suffix > 15-char
/// string), which would misclassify an id-less reason as a pane exit. A real
/// reason always carries a non-empty `term_xxx` id, so treating the
/// id-less form conservatively as "not a pane exit" costs nothing in
/// practice while avoiding that false positive.
fn closed_reason_is_pane_exit(reason: Option<&str>) -> bool {
    let Some(rest) = reason.and_then(|r| r.strip_prefix("terminal ")) else {
        return false;
    };
    match rest.strip_suffix(" exited") {
        Some(id) => !id.is_empty(),
        None => false,
    }
}

/// minimum spacing between foreground polls — each is an ssh handshake, so we
/// poll lazily (only around mouse activity) and no faster than this
const FG_POLL_INTERVAL: Duration = Duration::from_millis(1500);

/// after input settles, re-poll once this much later to catch a foreground
/// change the input caused (e.g. a TUI just exited); bypasses FG_POLL_INTERVAL
const SETTLE_DELAY: Duration = Duration::from_millis(350);

impl App {
    fn paint(&mut self) {
        if !self.tty {
            return;
        }
        if self.predict.take_dirty() {
            // cleared predictions may have left ghost chars — full repaint
            self.renderer.invalidate();
        }
        let (cols, rows) = term_size();
        let mut out = self.renderer.paint(&self.grid, cols, rows);
        // inject the prediction overlay inside the synchronized-update block
        let overlay = self.predict.overlay(&self.grid, cols, rows);
        if !overlay.is_empty() {
            const SYNC_END: &str = "\x1b[?2026l";
            if let Some(pos) = out.rfind(SYNC_END) {
                out.insert_str(pos, &overlay);
            } else {
                out.push_str(&overlay);
            }
        }
        write_stdout(&out);
    }

    fn hint(&mut self, text: &str) {
        self.renderer.status(text);
        self.paint();
        self.hint_clear_at = Some(Instant::now() + Duration::from_millis(1500));
    }

    /// Kick a background poll of the remote pane's foreground process, throttled
    /// so a mouse burst doesn't spawn an ssh per event. The result arrives as
    /// Msg::Foreground and updates `remote_is_shell`.
    fn spawn_foreground_poll(&mut self, force: bool) {
        let now = Instant::now();
        if !force && self.fg_poll_at.is_some_and(|t| now.duration_since(t) < FG_POLL_INTERVAL) {
            return;
        }
        self.fg_poll_at = Some(now);
        // mux transport: request over the shared connection; the reader delivers
        // the `api_res` as Msg::Foreground. No per-poll ssh handshake.
        if let Some(tx) = self.mux_tx.clone() {
            self.mux_api_sid += 1;
            let msg = protocol::Msg::Api {
                sid: MUX_API_SID_BASE + self.mux_api_sid,
                method: "pane.process_info".into(),
                params: json!({ "pane_id": self.args.pane_target }),
            };
            let _ = tx.send(msg.to_line());
            return;
        }
        let tx = self.tx.clone();
        let ssh = self.args.ssh_target.clone();
        let bin = self.args.remote_bin.clone();
        let pane = self.args.pane_target.clone();
        let ctl = self.args.ctl_path.clone();
        tokio::spawn(async move {
            let v = crate::foreground::poll(&ssh, &bin, &pane, ctl.as_deref()).await;
            let _ = tx.send(Msg::Foreground(v)).await;
        });
    }

    /// Match the local mouse grab to the classification: release it at a shell so
    /// herdr does native selection/scroll; keep it grabbed for an agent CLI, a
    /// TUI, or while unknown, so clicks/drags reach us. Only writes on a change.
    fn sync_mouse_grab(&mut self) {
        if !self.tty {
            return;
        }
        // grab unless we've confirmed the foreground is a plain shell
        let want = self.foreground != Some(Foreground::Shell);
        if want == self.mouse_grabbed {
            return;
        }
        self.mouse_grabbed = want;
        write_stdout(if want { "\x1b[?1002h\x1b[?1006h" } else { "\x1b[?1002l" });
    }

    /// Map an SGR mouse position (1-based local terminal cell) to a grid cell,
    /// using the same bottom-anchored window offset the renderer paints with.
    fn mouse_to_grid(&self, x: u32, y: u32) -> (usize, usize) {
        let (_cols, rows) = term_size();
        let offset = window_offset(&self.grid, rows);
        let row = (y as usize).saturating_sub(1) + offset;
        let col = (x as usize).saturating_sub(1);
        (row, col)
    }

    /// Push the current drag selection (if any) to the renderer and repaint. The
    /// renderer diffs painted rows, so setting/clearing the highlight repaints
    /// only the affected rows.
    fn refresh_selection(&mut self) {
        let sel = self.selecting.map(|(a, b)| normalize_selection(a, b));
        self.renderer.set_selection(sel);
        self.paint();
    }

    fn observe_size(&self) -> (usize, usize) {
        // must request >= the remote PTY size or the server clips its bottom
        // rows; take the larger of the daemon-passed size (remote size + margin
        // at spawn time — it can grow stale) and the live local terminal size
        let (c, r) = if self.tty { term_size() } else { (0, 0) };
        (self.args.cols.max(c), self.args.rows.max(r))
    }

    /// Stop the child (clean release first for control) — never leave an
    /// orphan holding the remote attach lock.
    fn stop_session(&mut self) {
        if let Some(mut s) = self.session.take() {
            tokio::spawn(async move {
                if s.mode == Mode::Control {
                    s.send_input("{\"type\":\"terminal.release\"}\n").await;
                }
                tokio::time::sleep(Duration::from_millis(150)).await;
                s.terminate();
            });
        }
    }

    /// Ensure the shared mux connection is up, spawning its reader/writer tasks
    /// on a fresh connect. Returns false if the socket can't be reached (mux not
    /// up yet) so the caller falls back to the ssh transport. Only meaningful
    /// when `--mux-sock` was given.
    async fn ensure_mux(&mut self) -> bool {
        if let Some(tx) = &self.mux_tx {
            if !tx.is_closed() {
                return true;
            }
        }
        self.mux_tx = None;
        let Some(sock) = self.args.mux_sock.clone() else { return false };
        match UnixStream::connect(&sock).await {
            Ok(stream) => {
                let (rd, wr) = stream.into_split();
                let (wtx, wrx) = mpsc::unbounded_channel::<String>();
                tokio::spawn(mux_writer_task(wr, wrx));
                tokio::spawn(mux_reader_task(rd, self.tx.clone()));
                self.mux_tx = Some(wtx);
                true
            }
            Err(_) => false,
        }
    }

    /// Open a terminal session over the shared mux connection (the mux analog of
    /// `spawn_session`). The client sid is the session gen, so replies route by
    /// the existing gen check and no stale sid can bleed across generations.
    fn open_mux_session(&self, m: Mode, cols: usize, rows: usize, gen: u64) -> Result<Session> {
        let tx = self.mux_tx.clone().ok_or_else(|| err("mux not connected"))?;
        // same takeover policy as the ssh path: only always-control evicts a
        // ghost attach, and never on observe.
        let takeover = m == Mode::Control && self.args.always_control;
        let open = protocol::Msg::Open {
            sid: gen,
            pane: self.args.pane_target.clone(),
            mode: m.as_str().to_string(),
            cols: cols as u32,
            rows: rows as u32,
            takeover,
            session: self.args.session.clone(),
        };
        tx.send(open.to_line()).map_err(|_| err("mux write channel closed"))?;
        Ok(Session { gen, mode: m, started: Instant::now(), transport: Transport::Mux { sid: gen, tx } })
    }

    async fn connect(&mut self, m: Mode) {
        self.mode = m;
        // re-earn prediction confidence against the new session's frames
        self.predict = Predictor::new();
        let (cols, rows) = match m {
            Mode::Observe => self.observe_size(),
            Mode::Control => term_size(),
        };
        if let Some(s) = self.session.take() {
            s.terminate();
        }
        self.next_gen += 1;
        let gen = self.next_gen;
        // prefer the mux transport; fall back to the ssh child if the socket
        // isn't reachable yet (mux still starting) — retried on the next connect.
        let spawned = if self.args.mux_sock.is_some() && self.ensure_mux().await {
            self.open_mux_session(m, cols, rows, gen)
        } else {
            spawn_session(&self.args, m, cols, rows, gen, self.tx.clone())
        };
        match spawned {
            Ok(mut s) => {
                if m == Mode::Control {
                    self.last_input = Instant::now();
                    // keystrokes typed while the control session was spinning up
                    for buf in std::mem::take(&mut self.pending_input) {
                        let line = json!({ "type": "terminal.input", "bytes": B64.encode(&buf) }).to_string() + "\n";
                        s.send_input(&line).await;
                    }
                } else {
                    self.pending_input.clear();
                }
                self.session = Some(s);
                // warm the foreground classification before the user mouses
                self.spawn_foreground_poll(false);
                // always-control has no release, so no "ctrl+\ to release" hint
                self.renderer.status(
                    if m == Mode::Control && !self.args.always_control {
                        "CONTROL — ctrl+\\ to release"
                    } else {
                        ""
                    },
                );
            }
            Err(e) => self.schedule_reconnect(m, &e.to_string()),
        }
    }

    fn schedule_reconnect(&mut self, m: Mode, reason: &str) {
        let delay = BACKOFF[self.backoff_idx.min(BACKOFF.len() - 1)];
        self.backoff_idx += 1;
        let suffix = if reason.is_empty() { String::new() } else { format!(" — {reason}") };
        self.renderer
            .status(&format!("reconnecting in {}s ({}){suffix}", delay / 1000, m.as_str()));
        self.paint();
        self.reconnect_at = Some((Instant::now() + Duration::from_millis(delay), m));
    }

    fn switch_mode(&mut self, m: Mode) {
        // already settled or scheduled — don't restart. Without this guard,
        // fast typing during the 200ms connect gap would spawn one control
        // ssh per keystroke, all racing to attach the same terminal.
        if self.switching_to == Some(m) || (self.switching_to.is_none() && self.mode == m) {
            return;
        }
        self.reconnect_at = None;
        if m == Mode::Control {
            // an attempt is in flight either way; a renewed failure re-arms
            // this from handle_exit's fallback
            self.control_retry_at = None;
        }
        self.switching_to = Some(m);
        self.stop_session();
        self.renderer.invalidate();
        // immediate feedback for the mode-switch gap (stop + 200ms + reconnect)
        self.renderer.status(if m == Mode::Control { "taking control…" } else { "releasing…" });
        self.paint();
        self.switch_at = Some(Instant::now() + SWITCH_GAP);
    }

    fn handle_frame(&mut self, gen: u64, frame: Frame) {
        if self.session.as_ref().map(|s| s.gen) != Some(gen) {
            return; // stale frame from a replaced session
        }
        if frame.kind == "terminal.closed" {
            let suffix = frame.reason.as_deref().map(|r| format!(": {r}")).unwrap_or_default();
            self.renderer.status(&format!("remote terminal closed{suffix}"));
            self.paint();
            if closed_reason_is_pane_exit(frame.reason.as_deref()) {
                // The remote pane's process itself ended — this pane is gone
                // for good, not a transient attach/server condition. Skip the
                // reconnect/backoff dance entirely: flag the main loop to
                // break so the existing clean-shutdown path (release control
                // if held, terminate the session through the same Ssh-kill /
                // Mux-close machinery, restore the tty) runs immediately and
                // herdr closes the local pane right away instead of waiting
                // out a backoff-driven reconnect.
                self.exit_requested = true;
                return;
            }
            // Observed in the wild: the remote CLI emits terminal.closed (e.g.
            // an attach failure — "already has an attached client; retry with
            // --takeover") but does not exit on its own, so retries pile up
            // ssh/remote-CLI processes on both ends. Kill our side directly by
            // pid — don't touch `self.session`/mode here, so the child's real
            // exit still arrives as a normal SessionExit with a matching gen
            // and handle_exit's backoff / control-failure bookkeeping runs
            // exactly as it would for any other exit (no double state change).
            if let Some(s) = self.session.as_ref() {
                match &s.transport {
                    Transport::Ssh { pid, .. } => unsafe {
                        libc::kill(*pid, libc::SIGTERM);
                    },
                    Transport::Mux { .. } => {
                        // mux owns the child: `close` makes the agent reap it.
                        // But the mux drops the sid on close, so the agent's
                        // `exit` won't route back — synthesize the SessionExit
                        // here so reconnect / control-failure bookkeeping runs
                        // exactly as the ssh kill→exit path would.
                        s.terminate();
                        let (gen, mode, uptime) = (s.gen, s.mode, s.started.elapsed());
                        let reason = frame.reason.clone().unwrap_or_default();
                        let tx = self.tx.clone();
                        tokio::spawn(async move {
                            let _ = tx.send(Msg::SessionExit { gen, mode, reason, uptime }).await;
                        });
                    }
                }
            }
            return;
        }
        if frame.kind != "terminal.frame" {
            return;
        }
        let Some(bytes) = &frame.bytes else { return };
        self.backoff_idx = 0;
        self.renderer.status("");
        self.grid
            .resize(frame.width.unwrap_or(self.grid.width), frame.height.unwrap_or(self.grid.height));
        if frame.full == Some(true) {
            self.grid.clear();
        }
        if let Ok(decoded) = B64.decode(bytes) {
            self.grid.apply(&String::from_utf8_lossy(&decoded));
            // reconcile predictive echo against the authoritative frame
            self.predict.on_frame(&self.grid);
        }
        if self.args.dump {
            let lines: Vec<String> = self.grid.text_lines().into_iter().filter(|l| !l.is_empty()).collect();
            println!(
                "--- frame seq={:?} full={:?} {}x{} ---\n{}",
                frame.seq,
                frame.full,
                frame.width.unwrap_or(0),
                frame.height.unwrap_or(0),
                lines.join("\n")
            );
        } else {
            self.paint();
        }
    }

    fn handle_exit(&mut self, gen: u64, exited_mode: Mode, reason: String, uptime: Duration) {
        if self.session.as_ref().map(|s| s.gen) != Some(gen) {
            return; // an old child we already replaced/killed
        }
        self.session = None;
        let reason_line =
            reason.lines().map(str::trim).rfind(|l| !l.is_empty()).unwrap_or("").to_string();
        // control that dies quickly twice is failing (refused/dropped): fall
        // back to observe so the pane stays viewable; a keystroke retries
        if exited_mode == Mode::Control {
            self.control_failures = if uptime < QUICK_CONTROL_FAILURE { self.control_failures + 1 } else { 0 };
            if self.control_failures >= 2 {
                self.control_failures = 0;
                self.control_sticky = true;
                self.switch_mode(Mode::Observe);
                // always-control has no one to type and retry, so retry itself
                // on a timer instead of waiting indefinitely in observe
                if self.args.always_control {
                    self.control_retry_at = Some(Instant::now() + CONTROL_RETRY_INTERVAL);
                }
                let suffix = if reason_line.is_empty() { String::new() } else { format!(" ({reason_line})") };
                self.hint(&format!("control unavailable — viewing only{suffix}; type to retry"));
                return;
            }
        }
        self.schedule_reconnect(exited_mode, &reason_line);
    }

    /// Escalate observe → control: the path a keystroke takes, also reused by
    /// the always-control timed retry (`control_retry_at`) so a pane stuck
    /// observing after a network blip recovers without input.
    fn retry_control(&mut self) {
        self.control_sticky = false;
        self.switch_mode(Mode::Control);
    }

    async fn send(&mut self, msg: serde_json::Value) {
        if let Some(s) = self.session.as_mut() {
            let line = msg.to_string() + "\n";
            s.send_input(&line).await;
        }
    }

    async fn handle_stdin(&mut self, buf: Vec<u8>) {
        if self.mode == Mode::Observe || self.switching_to == Some(Mode::Observe) {
            // no quit key: the wrapper's lifecycle belongs to the hosting pane
            if has_mouse_seq(&buf) {
                // wheel escalates only after a soft release; a stray wheel
                // while glancing shouldn't grab the remote's lock
                if contains_wheel_press(&buf) {
                    if self.control_sticky {
                        self.control_sticky = false;
                        self.switch_mode(Mode::Control);
                    } else {
                        self.hint("read-only — type to take control");
                    }
                }
                return;
            }
            // any keystroke takes control and is delivered once the session is up
            self.pending_input.push(buf);
            self.retry_control();
            return;
        }

        // control mode
        self.last_input = Instant::now();
        if buf.len() == 1 && buf[0] == 0x1c {
            // ctrl+\ — manual release. In always-control there's nothing to
            // release to, so swallow it (never forward it: ctrl+\ is SIGQUIT).
            if !self.args.always_control {
                self.control_sticky = false;
                self.switch_mode(Mode::Observe);
            }
            return;
        }
        if self.switching_to == Some(Mode::Control) || self.session.is_none() {
            // spinning up or awaiting reconnect: queue the keystroke (flushed
            // on connect) and, if in backoff, reconnect now
            self.pending_input.push(buf);
            if let Some((_, m)) = self.reconnect_at {
                self.reconnect_at = Some((Instant::now(), m));
            }
            return;
        }
        // refresh the foreground classification on any input while active.
        // keyboard reaches us even when the grab is released at a shell, so this
        // is what catches a shell→TUI switch — a released grab means mouse events
        // stop arriving here, so mouse alone can never trigger the re-poll.
        self.spawn_foreground_poll(false);
        // and re-check shortly after input settles, to catch a change the input
        // just caused (e.g. `:q` quitting vim — the poll above still sees vim)
        self.settle_at = Some(Instant::now() + SETTLE_DELAY);
        // per-classification mouse policy:
        //  - Tui: forward every mouse event raw (the remote app owns the mouse)
        //  - Agent: click → forward, left-drag → local select/copy (see below)
        //  - Shell / unknown: wheel → semantic scroll, clicks dropped (kept local
        //    so they don't garbage the prompt)
        let mut rest: Vec<u8> = Vec::with_capacity(buf.len());
        let mut i = 0usize;
        let mut scrolls: Vec<serde_json::Value> = Vec::new();
        // set when a drag selection finalizes: text to copy locally (never sent)
        let mut clip: Option<String> = None;
        // set when the selection highlight changed and needs a repaint
        let mut sel_dirty = false;
        while i < buf.len() {
            if let Some((btn, x, y, press, len)) = parse_mouse(&buf, i) {
                let seq = &buf[i..i + len];
                match self.foreground {
                    Some(Foreground::Tui) => rest.extend_from_slice(seq),
                    Some(Foreground::Agent) => {
                        if btn == 64 || btn == 65 {
                            // wheel: cancel any pending press / in-progress select,
                            // then forward raw so the remote agent scrolls
                            if self.selecting.take().is_some() {
                                sel_dirty = true;
                            }
                            self.mouse_pending = None;
                            rest.extend_from_slice(seq);
                        } else if btn == 0 && press {
                            // left press: hold — could be a click or a drag start
                            let anchor = self.mouse_to_grid(x, y);
                            self.mouse_pending = Some((seq.to_vec(), anchor));
                        } else if btn == 32 {
                            // left-drag motion: enter/continue local selection
                            let cur = self.mouse_to_grid(x, y);
                            if let Some((_, anchor)) = self.mouse_pending.take() {
                                self.selecting = Some((anchor, cur));
                                sel_dirty = true;
                            } else if let Some((anchor, _)) = self.selecting {
                                self.selecting = Some((anchor, cur));
                                sel_dirty = true;
                            }
                            // no pending/selecting → stray motion, ignore
                        } else if btn == 0 && !press {
                            // left release
                            if let Some((anchor, cur)) = self.selecting.take() {
                                // selection confirmed → copy locally, send nothing
                                let (s, e) = normalize_selection(anchor, cur);
                                let text = self.grid.selection_text(s, e);
                                if !text.is_empty() {
                                    clip = Some(text);
                                }
                                sel_dirty = true;
                            } else if let Some((press_bytes, _)) = self.mouse_pending.take() {
                                // click confirmed → forward press + release together
                                rest.extend_from_slice(&press_bytes);
                                rest.extend_from_slice(seq);
                            } else {
                                rest.extend_from_slice(seq);
                            }
                        } else {
                            // right / middle / other buttons: forward raw
                            rest.extend_from_slice(seq);
                        }
                    }
                    _ => {
                        // shell / not-yet-classified: wheel → semantic scroll
                        if press && (btn == 64 || btn == 65) {
                            scrolls.push(json!({
                                "type": "terminal.scroll",
                                "direction": if btn == 64 { "up" } else { "down" },
                                "lines": 3,
                                "source": "wheel",
                                "column": x.saturating_sub(1),
                                "row": y.saturating_sub(1),
                                "modifiers": 0,
                            }));
                        }
                        // else: click → drop (keep mouse local)
                    }
                }
                i += len;
            } else {
                rest.push(buf[i]);
                i += 1;
            }
        }
        if sel_dirty {
            self.refresh_selection();
        }
        if let Some(text) = clip {
            copy_to_clipboard(&text);
            self.hint("copied");
        }
        for s in scrolls {
            self.send(s).await;
        }
        if !rest.is_empty() {
            let msg = json!({ "type": "terminal.input", "bytes": B64.encode(&rest) });
            self.send(msg).await;
            // optimistic local echo: draw the keystroke now, verify on frame
            if self.predict.on_input(&rest, &self.grid) {
                self.paint();
            }
        }
    }
}

/// Copy `text` to the local clipboard. macOS uses `pbcopy`; elsewhere fall back
/// to OSC 52 written to our own stdout. Best-effort — failures are ignored.
fn copy_to_clipboard(text: &str) {
    #[cfg(target_os = "macos")]
    {
        use std::io::Write;
        use std::process::{Command, Stdio};
        if let Ok(mut child) = Command::new("pbcopy").stdin(Stdio::piped()).spawn() {
            if let Some(mut si) = child.stdin.take() {
                let _ = si.write_all(text.as_bytes());
                // drop `si` here so pbcopy sees EOF before we wait
            }
            let _ = child.wait();
        }
    }
    #[cfg(not(target_os = "macos"))]
    {
        let b64 = B64.encode(text.as_bytes());
        write_stdout(&format!("\x1b]52;c;{b64}\x07"));
    }
}

// ---------------------------------------------------------------------------
// main

pub async fn run(args: Args) -> Result<()> {
    let tty = !args.dump && unsafe { libc::isatty(libc::STDOUT_FILENO) } == 1;
    let raw = if tty {
        // 1002/1006: button-event mouse tracking with SGR encoding, so wheel and
        // clicks reach us instead of scrolling the hosting pane's scrollback
        write_stdout("\x1b[?1049h\x1b[2J\x1b[H\x1b[?1002h\x1b[?1006h");
        RawMode::enable()
    } else {
        None
    };

    let (tx, mut rx) = mpsc::channel::<Msg>(256);

    // stdin reader
    {
        let tx = tx.clone();
        tokio::spawn(async move {
            let mut stdin = tokio::io::stdin();
            let mut buf = [0u8; 1024];
            loop {
                match stdin.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        if tx.send(Msg::Stdin(buf[..n].to_vec())).await.is_err() {
                            break;
                        }
                    }
                }
            }
        });
    }

    let mut app = App {
        args,
        tty,
        grid: Grid::new(),
        renderer: Renderer::new(),
        tx,
        mode: Mode::Observe,
        switching_to: None,
        switch_at: None,
        session: None,
        next_gen: 0,
        backoff_idx: 0,
        reconnect_at: None,
        control_failures: 0,
        control_sticky: false,
        control_retry_at: None,
        pending_input: Vec::new(),
        last_input: Instant::now(),
        hint_clear_at: None,
        predict: Predictor::new(),
        foreground: None,
        mouse_pending: None,
        selecting: None,
        fg_poll_at: None,
        settle_at: None,
        mouse_grabbed: tty, // startup wrote ?1002h when we're a tty
        mux_tx: None,
        mux_api_sid: 0,
        exit_requested: false,
    };

    // optimistic-split pending mode: the remote pane doesn't exist yet. Show a
    // "connecting…" placeholder while the remote-split action resolves the
    // remote pane id into our claim file, then adopt it as the pane target. A
    // failure or timeout paints the reason briefly, then we exit so the pane
    // closes on its own.
    if app.args.pending {
        app.renderer.status("connecting…");
        app.paint();
        let self_pane_id = std::env::var("HERDR_PANE_ID").unwrap_or_default();
        let state_dir = crate::util::default_state_dir();
        match await_claim(&state_dir, &self_pane_id, PENDING_POLL_INTERVAL, PENDING_TIMEOUT).await {
            Ok(remote_pane) => {
                app.args.pane_target = remote_pane;
                app.args.pending = false;
            }
            Err(msg) => {
                app.renderer.status(&format!("split failed: {msg}"));
                app.paint();
                tokio::time::sleep(Duration::from_secs(2)).await;
                if tty {
                    write_stdout("\x1b[?1002l\x1b[?1006l\x1b[?25h\x1b[?1049l");
                }
                if let Some(raw) = raw {
                    raw.restore();
                }
                return Ok(());
            }
        }
    }

    app.connect(if app.args.always_control { Mode::Control } else { Mode::Observe }).await;

    let mut sigterm = signal(SignalKind::terminate())?;
    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sighup = signal(SignalKind::hangup())?; // pane closed — don't orphan the ssh child
    let mut sigwinch = signal(SignalKind::window_change())?;

    loop {
        // earliest pending deadline: mode-switch gap, reconnect, hint clear, idle release
        let idle_at = (app.mode == Mode::Control
            && app.switching_to.is_none()
            && app.session.is_some()
            && !app.args.always_control
            && app.args.control_idle_secs > 0)
            .then(|| app.last_input + Duration::from_secs(app.args.control_idle_secs));
        let sleep = crate::util::sleep_until_earliest([
            app.switch_at,
            app.reconnect_at.map(|(t, _)| t),
            app.hint_clear_at,
            idle_at,
            app.predict.deadline(),
            app.settle_at,
            app.control_retry_at,
        ]);

        tokio::select! {
            msg = rx.recv() => {
                match msg {
                    None => break,
                    Some(Msg::Frame { gen, frame }) => {
                        app.handle_frame(gen, frame);
                        if app.exit_requested {
                            break;
                        }
                    }
                    Some(Msg::SessionExit { gen, mode, reason, uptime }) => app.handle_exit(gen, mode, reason, uptime),
                    Some(Msg::MuxExit { sid, code }) => {
                        // rebuild a SessionExit from the current session state; the
                        // reader can't know the exited mode / uptime for a sid.
                        let info = app.session.as_ref().filter(|s| s.gen == sid).map(|s| (s.mode, s.started.elapsed()));
                        if let Some((mode, uptime)) = info {
                            app.handle_exit(sid, mode, format!("agent child exit code {code}"), uptime);
                        }
                    }
                    Some(Msg::MuxReconnecting) => {
                        // status-only: the mux is re-opening the agent link; the
                        // session lives and a full frame clears this on recovery.
                        app.renderer.status("mux reconnecting…");
                        app.paint();
                    }
                    Some(Msg::MuxClosed) => {
                        // the wrapper→mux socket dropped; force a fresh connect and
                        // route the current session through the reconnect flow.
                        app.mux_tx = None;
                        let info = app.session.as_ref().map(|s| (s.gen, s.mode, s.started.elapsed()));
                        if let Some((gen, mode, uptime)) = info {
                            app.handle_exit(gen, mode, "mux socket closed".into(), uptime);
                        }
                    }
                    Some(Msg::Stdin(buf)) => app.handle_stdin(buf).await,
                    // keep the last good classification if a poll failed (None)
                    Some(Msg::Foreground(v)) => if v.is_some() {
                        app.foreground = v;
                        // left agent mode: drop any half-done click/drag and clear
                        // a lingering selection highlight
                        if v != Some(Foreground::Agent) {
                            app.mouse_pending = None;
                            if app.selecting.take().is_some() {
                                app.refresh_selection();
                            }
                        }
                        app.sync_mouse_grab();
                    },
                }
            }
            _ = sigwinch.recv() => {
                app.renderer.invalidate();
                if app.mode == Mode::Control {
                    let (cols, rows) = term_size();
                    app.send(json!({ "type": "terminal.resize", "cols": cols, "rows": rows })).await;
                }
                app.paint();
            }
            _ = sigterm.recv() => break,
            _ = sigint.recv() => break,
            _ = sighup.recv() => break,
            _ = sleep => {
                let now = Instant::now();
                if app.switch_at.is_some_and(|t| t <= now) {
                    app.switch_at = None;
                    if let Some(m) = app.switching_to.take() {
                        app.connect(m).await; // pending input from the gap flushes here
                    }
                }
                if let Some((t, m)) = app.reconnect_at {
                    if t <= now {
                        app.reconnect_at = None;
                        app.connect(m).await;
                    }
                }
                if app.hint_clear_at.is_some_and(|t| t <= now) {
                    app.hint_clear_at = None;
                    app.renderer.status("");
                    app.paint();
                }
                if idle_at.is_some_and(|t| t <= now) && app.mode == Mode::Control && app.switching_to.is_none() {
                    app.control_sticky = true;
                    app.switch_mode(Mode::Observe);
                    app.hint("control released (idle) — type to retake");
                }
                if app.settle_at.is_some_and(|t| t <= now) {
                    app.settle_at = None;
                    app.spawn_foreground_poll(true); // forced: bypass the throttle
                }
                if app.predict.deadline().is_some_and(|t| t <= now) {
                    app.predict.on_tick(); // wipe timed-out ghosts (no-echo prompts)
                    app.paint();
                }
                // always-control stuck in observe after a fallback: retry control
                // on our own rather than waiting forever for a keystroke
                if app.control_retry_at.is_some_and(|t| t <= now) {
                    app.control_retry_at = None;
                    if app.mode == Mode::Observe && app.switching_to.is_none() {
                        app.retry_control();
                    }
                }
            }
        }
    }

    // clean shutdown: release control if held, tear the session down, restore tty
    if let Some(mut s) = app.session.take() {
        if s.mode == Mode::Control {
            s.send_input("{\"type\":\"terminal.release\"}\n").await;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        s.terminate();
    }
    if tty {
        write_stdout("\x1b[?1002l\x1b[?1006l\x1b[?25h\x1b[?1049l");
    }
    if let Some(raw) = raw {
        raw.restore();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn closed_reason_pane_exit_classification() {
        // real shape: "terminal <id> exited" — the remote process ended
        assert!(closed_reason_is_pane_exit(Some("terminal term_abc exited")));
        // attach conflicts / takeover refusals are a different reason shape
        assert!(!closed_reason_is_pane_exit(Some(
            "terminal attach failed: already has an attached client; retry with --takeover"
        )));
        // no reason at all (e.g. a malformed/absent field) is never a pane exit
        assert!(!closed_reason_is_pane_exit(None));
        // boundary: prefix "terminal " (9 chars) and suffix " exited" (7 chars)
        // overlap on a 15-char string with no id in between. A naive
        // starts_with/ends_with check would misclassify this as a pane exit;
        // we require a non-empty id, so it's treated as *not* a pane exit —
        // real reasons always carry a non-empty term_xxx id.
        assert!(!closed_reason_is_pane_exit(Some("terminal exited")));
    }

    #[test]
    fn mouse_parsing() {
        let seq = b"\x1b[<64;10;5M";
        let (btn, x, y, press, len) = parse_mouse(seq, 0).unwrap();
        assert_eq!((btn, x, y, press, len), (64, 10, 5, true, seq.len()));
        assert!(contains_wheel_press(seq));
        assert!(!contains_wheel_press(b"\x1b[<0;3;4M")); // click, not wheel
        assert!(!contains_wheel_press(b"\x1b[<64;10;5m")); // release, not press
        assert!(has_mouse_seq(b"xx\x1b[<0;1;1Myy"));
        assert!(!has_mouse_seq(b"plain text"));
    }


    #[test]
    fn sh_quote_escapes_single_quotes() {
        assert_eq!(sh_quote("w9:p1"), "'w9:p1'");
        assert_eq!(sh_quote("a'b"), "'a'\\''b'");
        // overflow-proof mouse params: 11 digits saturate instead of panicking
        let (_, x, _, _, _) = parse_mouse(b"\x1b[<64;99999999999;1M", 0).unwrap();
        assert_eq!(x, u32::MAX);
    }

    #[test]
    fn arg_parsing() {
        let argv: Vec<String> =
            ["work", "w9:p1", "--remote-bin", "/opt/herdr", "--cols", "176", "--rows", "66"]
                .iter()
                .map(|s| s.to_string())
                .collect();
        let a = parse_args(&argv).unwrap();
        assert_eq!(a.ssh_target, "work");
        assert_eq!(a.pane_target, "w9:p1");
        assert_eq!(a.remote_bin, "/opt/herdr");
        assert_eq!((a.cols, a.rows), (176, 66));
        assert!(a.mux_sock.is_none());
        assert!(parse_args(&["onlyone".to_string()]).is_err());
        assert!(parse_args(&["a".into(), "b".into(), "--visibility-file".into(), "x".into()]).is_err());
    }

    #[test]
    fn arg_parsing_pending_allows_no_pane_target() {
        // --pending: only the ssh-target positional, pane-target resolved later
        let argv: Vec<String> = ["work", "--pending", "--mux-sock", "/tmp/m.sock", "--always-control"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let a = parse_args(&argv).unwrap();
        assert!(a.pending);
        assert_eq!(a.ssh_target, "work");
        assert_eq!(a.pane_target, "");
        assert!(a.always_control);
        assert_eq!(a.mux_sock.as_deref(), Some("/tmp/m.sock"));
        // a pane-target alongside --pending is rejected (2 positionals)
        assert!(parse_args(&["work".into(), "w9:p1".into(), "--pending".into()]).is_err());
        // and --pending still needs the ssh-target (0 positionals is an error)
        assert!(parse_args(&["--pending".into()]).is_err());
    }

    #[tokio::test]
    async fn await_claim_resolves_pane_then_error_then_timeout() {
        let dir = std::env::temp_dir().join(format!(
            "herdr-await-claim-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();

        // pane claim → Ok(remote id), and the claim file is consumed
        let claim = crate::util::claim_path(&dir, "w9:p1");
        crate::util::write_atomic(&claim, &crate::util::claim_json_pane("wR:pR")).unwrap();
        let got = await_claim(&dir, "w9:p1", Duration::from_millis(5), Duration::from_secs(1)).await;
        assert_eq!(got.as_deref(), Ok("wR:pR"));
        assert!(!claim.exists());

        // error claim → Err(msg)
        crate::util::write_atomic(&claim, &crate::util::claim_json_error("remote split failed")).unwrap();
        let got = await_claim(&dir, "w9:p1", Duration::from_millis(5), Duration::from_secs(1)).await;
        assert_eq!(got, Err("remote split failed".to_string()));

        // no claim → times out
        let got = await_claim(&dir, "w9:p2", Duration::from_millis(5), Duration::from_millis(40)).await;
        assert!(got.is_err());

        // no self pane id → immediate error
        assert!(await_claim(&dir, "", Duration::from_millis(5), Duration::from_secs(1)).await.is_err());

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn arg_parsing_mux_sock() {
        let argv: Vec<String> =
            ["work", "w9:p1", "--mux-sock", "/tmp/work-mux.sock", "--ctl-path", "/tmp/work.ctl"]
                .iter()
                .map(|s| s.to_string())
                .collect();
        let a = parse_args(&argv).unwrap();
        assert_eq!(a.mux_sock.as_deref(), Some("/tmp/work-mux.sock"));
        // --ctl-path is still parsed alongside (kept for the ssh fallback)
        assert_eq!(a.ctl_path.as_deref(), Some("/tmp/work.ctl"));
    }

    // ---- pure mux-line → event mapping ----

    #[test]
    fn mux_line_frame_carries_sid_as_gen() {
        // an `l` op wrapping a terminal.frame → Frame(sid, parsed frame)
        let inner = r#"{"type":"terminal.frame","seq":7,"full":true,"width":4,"height":1,"bytes":"aGk="}"#;
        let line = protocol::Msg::Line { sid: 12, d: inner.to_string() }.to_line();
        match mux_line_to_event(&line) {
            MuxEvent::Frame(sid, frame) => {
                assert_eq!(sid, 12, "sid is used as the session gen");
                assert_eq!(frame.kind, "terminal.frame");
                assert_eq!(frame.seq, Some(7));
                assert_eq!(frame.full, Some(true));
                assert_eq!(frame.bytes.as_deref(), Some("aGk="));
            }
            other => panic!("expected Frame, got {other:?}"),
        }
    }

    #[test]
    fn mux_line_terminal_closed_is_a_frame_not_reconnecting() {
        // terminal.closed arrives as an `l` op too; it maps to a Frame so
        // handle_frame runs its existing terminal.closed path.
        let inner = r#"{"type":"terminal.closed","reason":"already has an attached client"}"#;
        let line = protocol::Msg::Line { sid: 3, d: inner.to_string() }.to_line();
        match mux_line_to_event(&line) {
            MuxEvent::Frame(3, frame) => {
                assert_eq!(frame.kind, "terminal.closed");
                assert_eq!(frame.reason.as_deref(), Some("already has an attached client"));
            }
            other => panic!("expected Frame, got {other:?}"),
        }
    }

    #[test]
    fn mux_line_exit_maps_to_exit() {
        let line = protocol::Msg::Exit { sid: 5, code: -1 }.to_line();
        match mux_line_to_event(&line) {
            MuxEvent::Exit(5, -1) => {}
            other => panic!("expected Exit(5,-1), got {other:?}"),
        }
    }

    #[test]
    fn mux_line_api_res_classifies_foreground() {
        // the agent hands back the bare `result`; classify_result re-wraps it
        let result = json!({ "process_info": { "foreground_processes": [{ "name": "zsh" }] } });
        let line = protocol::Msg::ApiRes { sid: MUX_API_SID_BASE + 1, result: Some(result), error: None }.to_line();
        match mux_line_to_event(&line) {
            MuxEvent::Foreground(Some(Foreground::Shell)) => {}
            other => panic!("expected Foreground(Shell), got {other:?}"),
        }
        // an error api_res (no result) → indeterminate, keep last value
        let errline = protocol::Msg::ApiRes { sid: 1, result: None, error: Some("boom".into()) }.to_line();
        assert!(matches!(mux_line_to_event(&errline), MuxEvent::Foreground(None)));
    }

    #[test]
    fn mux_line_reconnecting_blip() {
        // the mux's raw notice (no `closed` op in protocol::Msg) → Reconnecting
        let line = "{\"op\":\"closed\",\"sid\":7,\"reason\":\"mux reconnecting\"}\n";
        assert!(matches!(mux_line_to_event(line), MuxEvent::Reconnecting));
        assert!(is_mux_reconnecting(line));
    }

    #[test]
    fn mux_line_ignores_noise() {
        // ping / hello / blank / garbage carry nothing actionable for the wrapper
        assert!(matches!(mux_line_to_event(&protocol::Msg::Ping { seq: 1 }.to_line()), MuxEvent::Ignore));
        let hello = protocol::Msg::Hello { version: "1".into(), herdr_socket: "/s".into() }.to_line();
        assert!(matches!(mux_line_to_event(&hello), MuxEvent::Ignore));
        assert!(matches!(mux_line_to_event(""), MuxEvent::Ignore));
        assert!(matches!(mux_line_to_event("not json"), MuxEvent::Ignore));
        // an `l` op whose payload isn't a valid frame is dropped, not crashed
        let badframe = protocol::Msg::Line { sid: 1, d: "not a frame".into() }.to_line();
        assert!(matches!(mux_line_to_event(&badframe), MuxEvent::Ignore));
    }
}
