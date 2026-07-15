// Daemon-side client for the local mux socket (`<state_dir>/<host>-mux.sock`).
//
// The daemon's control plane no longer opens its own ControlMaster + api-socket
// forward + status exec: it rides the single mux ssh connection like every other
// client. This module is the thin request/response + subscription + exec layer
// over the mux NDJSON protocol (`protocol::Msg`).
//
// One `MuxApi` owns one connection to the mux socket. A background reader demuxes
// `api_res` / `exec_res` / `ev` lines back to the waiter registered under each
// sid; a background writer drains queued lines to the socket. Both tasks are
// aborted when the last `MuxApi` clone drops (they are tied to the handle, not
// the process), so reconnect cycles don't leak tasks.
//
// See docs/mux-design.md "Local socket protocol".

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::timeout;

use crate::api::EventEnvelope;
use crate::protocol::{parse_line, Msg};
use crate::util::{err, Result};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

/// The mux replaces a client's previous `sub` when the same client-sid is
/// re-used, so every subscribe rides this fixed sid: a resubscribe swaps the
/// held subscription in place (matching the daemon's single-subscription model).
const SUB_SID: u64 = 1;
/// One-shot api/exec sids start here so they never collide with `SUB_SID`.
const FIRST_ONESHOT_SID: u64 = 2;

/// A pending reply the reader must route back to its caller.
enum Waiter {
    /// One-shot `api` — carries `Ok(result)` or `Err(message)`.
    Api(oneshot::Sender<std::result::Result<Value, String>>),
    /// One-shot `exec` — carries `(code, out)`.
    Exec(oneshot::Sender<(i32, String)>),
    /// Held subscription — every `ev` line for this sid is pushed here.
    Sub(mpsc::UnboundedSender<EventEnvelope>),
}

/// Reader-owned routing state, shared with the handle via `Arc`. Kept separate
/// from `ApiInner` so the reader task holds no strong reference to the handle
/// (which would keep the abort-on-drop guard from ever firing).
struct Routing {
    pending: Mutex<HashMap<u64, Waiter>>,
    /// Cleared when the reader task exits (link gone). Requests that register
    /// after that would never be answered, so they check this and fail fast
    /// instead of blocking to the request timeout.
    alive: AtomicBool,
}

impl Routing {
    fn take(&self, sid: u64) -> Option<Waiter> {
        self.pending.lock().unwrap().remove(&sid)
    }

    fn is_alive(&self) -> bool {
        self.alive.load(Ordering::Acquire)
    }
}

/// The owned innards of a `MuxApi`. Dropping the last handle aborts the reader
/// and writer tasks; the local socket then closes and the remote sees this
/// client vanish.
struct ApiInner {
    tx: mpsc::UnboundedSender<String>,
    routing: Arc<Routing>,
    next_sid: AtomicU64,
    reader: JoinHandle<()>,
    writer: JoinHandle<()>,
}

impl Drop for ApiInner {
    fn drop(&mut self) {
        self.reader.abort();
        self.writer.abort();
    }
}

/// A cheap-to-clone handle to one mux-socket connection, exposing the same
/// `request` / `subscribe` surface as `ApiClient` plus a buffered `exec`.
#[derive(Clone)]
pub struct MuxApi {
    inner: Arc<ApiInner>,
}

impl MuxApi {
    /// Connect to the mux socket and start the reader/writer tasks. Fails if the
    /// socket is not yet bound (the daemon waits for mux readiness before calling).
    pub async fn connect(sock: &Path) -> Result<MuxApi> {
        let stream = UnixStream::connect(sock)
            .await
            .map_err(|e| err(format!("mux client connect {}: {e}", sock.display())))?;
        let (read, write) = stream.into_split();
        let routing = Arc::new(Routing {
            pending: Mutex::new(HashMap::new()),
            alive: AtomicBool::new(true),
        });
        let (tx, rx) = mpsc::unbounded_channel::<String>();
        let writer = tokio::spawn(writer_task(write, rx));
        let reader = tokio::spawn(reader_task(read, routing.clone()));
        Ok(MuxApi {
            inner: Arc::new(ApiInner {
                tx,
                routing,
                next_sid: AtomicU64::new(FIRST_ONESHOT_SID),
                reader,
                writer,
            }),
        })
    }

    fn next_sid(&self) -> u64 {
        self.inner.next_sid.fetch_add(1, Ordering::Relaxed)
    }

    /// One-shot request on the remote herdr socket via the agent. Mirrors
    /// `ApiClient::request`: method + params → result Value.
    pub async fn request(&self, method: &str, params: Value) -> Result<Value> {
        let sid = self.next_sid();
        let (rtx, rrx) = oneshot::channel();
        self.inner.routing.pending.lock().unwrap().insert(sid, Waiter::Api(rtx));
        let line = Msg::Api { sid, method: method.to_string(), params }.to_line();
        if !self.inner.routing.is_alive() || self.inner.tx.send(line).is_err() {
            self.inner.routing.take(sid);
            return Err(err(format!("mux client closed: {method}")));
        }
        match timeout(REQUEST_TIMEOUT, rrx).await {
            Ok(Ok(Ok(v))) => Ok(v),
            Ok(Ok(Err(e))) => Err(err(format!("{method}: {e}"))),
            Ok(Err(_)) => Err(err(format!("mux client dropped: {method}"))),
            Err(_) => {
                self.inner.routing.take(sid);
                Err(err(format!("api timeout: {method}")))
            }
        }
    }

    /// `sh -c cmd` on the remote host via the agent; buffered `(code, stdout)`.
    /// Used for the sidebar git-branch probe (replaces the daemon's ssh exec).
    pub async fn exec(&self, cmd: &str) -> Result<(i32, String)> {
        let sid = self.next_sid();
        let (rtx, rrx) = oneshot::channel();
        self.inner.routing.pending.lock().unwrap().insert(sid, Waiter::Exec(rtx));
        let line = Msg::Exec { sid, cmd: cmd.to_string() }.to_line();
        if !self.inner.routing.is_alive() || self.inner.tx.send(line).is_err() {
            self.inner.routing.take(sid);
            return Err(err("mux client closed: exec"));
        }
        match timeout(REQUEST_TIMEOUT, rrx).await {
            Ok(Ok(res)) => Ok(res),
            Ok(Err(_)) => Err(err("mux client dropped: exec")),
            Err(_) => {
                self.inner.routing.take(sid);
                Err(err("exec timeout"))
            }
        }
    }

    /// Held events.subscribe. Re-uses `SUB_SID`, so calling it again replaces the
    /// previous subscription (both here and on the mux/agent). The agent forwards
    /// no positive ack, so this returns as soon as the request is queued; a later
    /// `api_res` error (subscribe rejected) surfaces as the stream ending.
    pub async fn subscribe(&self, subs: Vec<Value>) -> Result<MuxEventStream> {
        let (etx, erx) = mpsc::unbounded_channel::<EventEnvelope>();
        self.inner.routing.pending.lock().unwrap().insert(SUB_SID, Waiter::Sub(etx));
        let line = Msg::Sub { sid: SUB_SID, subs }.to_line();
        if !self.inner.routing.is_alive() || self.inner.tx.send(line).is_err() {
            self.inner.routing.take(SUB_SID);
            return Err(err("mux client closed: subscribe"));
        }
        Ok(MuxEventStream { rx: erx })
    }
}

/// Held event stream over the mux; `None` once the subscription drops (link gone
/// or subscribe rejected), matching `api::EventStream::next`.
pub struct MuxEventStream {
    rx: mpsc::UnboundedReceiver<EventEnvelope>,
}

impl MuxEventStream {
    pub async fn next(&mut self) -> Option<EventEnvelope> {
        self.rx.recv().await
    }
}

/// Drain queued lines to the mux socket; ends on write error or when the last
/// handle drops (channel closes).
async fn writer_task(mut write: tokio::net::unix::OwnedWriteHalf, mut rx: mpsc::UnboundedReceiver<String>) {
    while let Some(line) = rx.recv().await {
        if write.write_all(line.as_bytes()).await.is_err() {
            break;
        }
        let _ = write.flush().await;
    }
}

/// Demux mux replies back to their waiters until EOF, then fail every pending
/// one-shot so callers don't hang waiting on a dead link.
async fn reader_task(read: tokio::net::unix::OwnedReadHalf, routing: Arc<Routing>) {
    let mut lines = BufReader::new(read).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let Some(msg) = parse_line(&line) else { continue };
        match msg {
            Msg::ApiRes { sid, result, error } => match routing.take(sid) {
                Some(Waiter::Api(tx)) => {
                    let _ = tx.send(match error {
                        Some(e) => Err(e),
                        None => Ok(result.unwrap_or(Value::Null)),
                    });
                }
                // an api_res on a sub sid is a subscribe rejection: dropping the
                // sender ends the stream so the caller resubscribes.
                Some(Waiter::Sub(_)) => {}
                // stale exec/echo — ignore.
                Some(Waiter::Exec(_)) | None => {}
            },
            Msg::ExecRes { sid, code, out } => {
                if let Some(Waiter::Exec(tx)) = routing.take(sid) {
                    let _ = tx.send((code, out));
                }
            }
            Msg::Ev { sid, d } => {
                // keep the sub registered: peek instead of take.
                let guard = routing.pending.lock().unwrap();
                if let Some(Waiter::Sub(tx)) = guard.get(&sid) {
                    if let Some(env) = parse_event(&d) {
                        let _ = tx.send(env);
                    }
                }
            }
            // terminals/liveness/handshake never target the daemon client.
            _ => {}
        }
    }
    // link gone: mark dead (so new requests fail fast instead of blocking to the
    // timeout), then fail every pending one-shot; sub senders drop and end streams.
    routing.alive.store(false, Ordering::Release);
    let mut guard = routing.pending.lock().unwrap();
    for (_, w) in guard.drain() {
        match w {
            Waiter::Api(tx) => {
                let _ = tx.send(Err("mux client link closed".into()));
            }
            Waiter::Exec(tx) => {
                let _ = tx.send((-1, "mux client link closed".into()));
            }
            Waiter::Sub(_) => {}
        }
    }
}

/// Parse an agent `ev` payload (`{"event":..,"data":..}`) into an EventEnvelope.
fn parse_event(d: &str) -> Option<EventEnvelope> {
    let v: Value = serde_json::from_str(d).ok()?;
    let event = v.get("event").and_then(|e| e.as_str())?.to_string();
    let data = v.get("data").cloned().unwrap_or(Value::Null);
    Some(EventEnvelope { event, data })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_event_extracts_event_and_data() {
        let env = parse_event("{\"event\":\"pane_created\",\"data\":{\"pane_id\":\"p1\"}}").unwrap();
        assert_eq!(env.event, "pane_created");
        assert_eq!(env.data["pane_id"], "p1");
    }

    #[test]
    fn parse_event_tolerates_missing_data() {
        let env = parse_event("{\"event\":\"x\"}").unwrap();
        assert_eq!(env.event, "x");
        assert_eq!(env.data, Value::Null);
        // no event field → None (not an event line)
        assert!(parse_event("{\"data\":{}}").is_none());
        assert!(parse_event("not json").is_none());
    }

    // one-shot sids must never collide with the fixed subscription sid, or an
    // api_res could be misrouted to the held sub (or vice versa).
    const _: () = assert!(SUB_SID < FIRST_ONESHOT_SID);
}

// Round-trip tests over an in-process stand-in for the mux socket. A full
// mux+agent+fake-herdr converge lap would need three subprocesses and a fake
// herdr JSON server; the transport swap's real risk is sid correlation and
// reply routing in this wrapper, which these exercise directly and cheaply.
#[cfg(test)]
mod round_trip_tests {
    use super::*;
    use serde_json::json;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::UnixListener;

    fn tmp_sock(tag: &str) -> std::path::PathBuf {
        // short path under /tmp: a unix socket path must fit ~104 bytes on macOS.
        std::path::PathBuf::from("/tmp").join(format!(
            "hmuxc-{tag}-{}-{}.sock",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() % 1_000_000
        ))
    }

    /// Minimal mux stand-in: echoes a canned reply per op so the wrapper's sid
    /// rewrite / routing is exercised without ssh, an agent, or a herdr socket.
    async fn fake_mux(listener: UnixListener) {
        let (stream, _) = listener.accept().await.unwrap();
        let (read, mut write) = stream.into_split();
        let mut lines = BufReader::new(read).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            let Some(msg) = parse_line(&line) else { continue };
            let reply = match msg {
                Msg::Api { sid, method, .. } => {
                    Some(Msg::ApiRes { sid, result: Some(json!({ "method": method })), error: None })
                }
                Msg::Exec { sid, cmd } => Some(Msg::ExecRes { sid, code: 0, out: format!("ran:{cmd}") }),
                Msg::Sub { sid, .. } => {
                    let d = json!({ "event": "pane_created", "data": { "pane_id": "p1" } }).to_string();
                    Some(Msg::Ev { sid, d })
                }
                _ => None,
            };
            if let Some(r) = reply {
                if write.write_all(r.to_line().as_bytes()).await.is_err() {
                    break;
                }
            }
        }
    }

    #[tokio::test]
    async fn request_exec_and_subscribe_round_trip() {
        let sock = tmp_sock("rt");
        let listener = UnixListener::bind(&sock).unwrap();
        tokio::spawn(fake_mux(listener));
        // exercise through the ApiClient enum too, to cover its Mux dispatch.
        let mux = MuxApi::connect(&sock).await.unwrap();
        let api = crate::api::ApiClient::mux(mux.clone());

        // api request: result routes back on the caller's sid
        let r = api.request("tab.focus", json!({ "id": "x" })).await.unwrap();
        assert_eq!(r["method"], "tab.focus");

        // exec: code + stdout round-trip (the branch-probe path)
        let (code, out) = mux.exec("echo hi").await.unwrap();
        assert_eq!(code, 0);
        assert_eq!(out, "ran:echo hi");

        // subscribe: the pushed event arrives through EventStream::Mux
        let mut stream = api.subscribe(vec![json!({ "type": "pane.created" })]).await.unwrap();
        let ev = stream.next().await.unwrap();
        assert_eq!(ev.event, "pane_created");
        assert_eq!(ev.data["pane_id"], "p1");

        std::fs::remove_file(&sock).ok();
    }

    #[tokio::test]
    async fn request_errors_fast_when_link_drops() {
        let sock = tmp_sock("drop");
        let listener = UnixListener::bind(&sock).unwrap();
        // accept, then immediately drop the connection (EOF to the client).
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            drop(stream);
        });
        let mux = MuxApi::connect(&sock).await.unwrap();
        // the reader hits EOF and marks the link dead; the request must not hang
        // to the 15s timeout.
        let res = tokio::time::timeout(Duration::from_secs(3), mux.request("ping", json!({}))).await;
        assert!(res.expect("request must resolve well under the request timeout").is_err());

        std::fs::remove_file(&sock).ok();
    }
}
