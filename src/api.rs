// Client for the herdr JSON socket API (HERDR_SOCKET_PATH protocol).
// Works against any unix socket path — the local server directly, or a remote
// server's socket forwarded over ssh.
//
// Connection semantics (verified against preview 2026-06-30): the server
// serves ONE request per connection and closes after the response. The only
// held connection is events.subscribe, which acks with subscription_started
// and then pushes {event, data} envelopes until either side closes.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::time::timeout;

use crate::util::{err, Result};

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
pub struct EventEnvelope {
    pub event: String,
    pub data: Value,
}

/// A herdr API client. `Socket` speaks the raw herdr JSON protocol on a unix
/// socket (one connection per request); `Mux` rides the daemon's single mux ssh
/// connection through the local mux socket. Both expose the same
/// method+params → result surface, so converge/mirror call sites are transport-
/// agnostic — swapping the daemon's remote transport is just a variant swap.
#[derive(Clone)]
pub enum ApiClient {
    Socket(PathBuf),
    Mux(crate::muxclient::MuxApi),
}

impl ApiClient {
    /// Bind a socket client to a path without probing it. Each request/subscribe
    /// opens its own connection, so no liveness is implied here.
    pub fn at(socket_path: &Path) -> ApiClient {
        ApiClient::Socket(socket_path.to_path_buf())
    }

    /// Connect-check the socket (one ping round-trip), then hand back a client.
    pub async fn connect(socket_path: &Path) -> Result<ApiClient> {
        let client = ApiClient::Socket(socket_path.to_path_buf());
        client.request("ping", json!({})).await?;
        Ok(client)
    }

    /// Wrap a mux-socket client as an `ApiClient` (the daemon's remote transport).
    pub fn mux(mux: crate::muxclient::MuxApi) -> ApiClient {
        ApiClient::Mux(mux)
    }

    /// One request; the socket backend closes after responding, the mux backend
    /// rides the shared connection.
    pub async fn request(&self, method: &str, params: Value) -> Result<Value> {
        match self {
            ApiClient::Socket(path) => timeout(REQUEST_TIMEOUT, request_inner(path, method, params))
                .await
                .map_err(|_| err(format!("api timeout: {method}")))?,
            ApiClient::Mux(m) => m.request(method, params).await,
        }
    }

    pub async fn request_t<T: DeserializeOwned>(&self, method: &str, params: Value) -> Result<T> {
        let v = self.request(method, params).await?;
        serde_json::from_value(v).map_err(|e| err(format!("{method}: bad response shape: {e}")))
    }

    /// Held connection pushing events. Pull with `EventStream::next()`; a
    /// `None` means the stream dropped (resubscribe from the caller).
    pub async fn subscribe(&self, subscriptions: Vec<Value>) -> Result<EventStream> {
        match self {
            ApiClient::Socket(path) => Ok(EventStream::Socket(subscribe_inner(path, subscriptions).await?)),
            ApiClient::Mux(m) => Ok(EventStream::Mux(m.subscribe(subscriptions).await?)),
        }
    }
}

async fn request_inner(socket_path: &Path, method: &str, params: Value) -> Result<Value> {
        let stream = timeout(CONNECT_TIMEOUT, UnixStream::connect(socket_path))
            .await
            .map_err(|_| err(format!("api connect timeout: {}", socket_path.display())))??;
        let (read, mut write) = stream.into_split();
        let id = format!("mirror_{}", NEXT_ID.fetch_add(1, Ordering::Relaxed));
        let line = serde_json::to_string(&json!({ "id": id, "method": method, "params": params }))? + "\n";
        write.write_all(line.as_bytes()).await?;
        let mut lines = BufReader::new(read).lines();
        while let Some(line) = lines.next_line().await? {
            let Ok(msg) = serde_json::from_str::<Value>(&line) else { continue };
            if msg.get("id").and_then(|v| v.as_str()) != Some(id.as_str()) {
                continue;
            }
            if let Some(e) = msg.get("error") {
                let text = e
                    .get("message")
                    .or_else(|| e.get("code"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown error");
                return Err(err(format!("{method}: {text}")));
            }
            return Ok(msg.get("result").cloned().unwrap_or(Value::Null));
        }
        Err(err(format!("api closed before response: {method}")))
    }

/// Open a held events.subscribe connection on a raw herdr socket.
async fn subscribe_inner(socket_path: &Path, subscriptions: Vec<Value>) -> Result<SocketEventStream> {
    let stream = timeout(CONNECT_TIMEOUT, UnixStream::connect(socket_path))
        .await
        .map_err(|_| err(format!("api connect timeout: {}", socket_path.display())))??;
    let (read, mut write) = stream.into_split();
    let id = format!("mirror_{}", NEXT_ID.fetch_add(1, Ordering::Relaxed));
    let line = serde_json::to_string(
        &json!({ "id": id, "method": "events.subscribe", "params": { "subscriptions": subscriptions } }),
    )? + "\n";
    write.write_all(line.as_bytes()).await?;
    let mut lines = BufReader::new(read).lines();
    // first line is the ack (or an error)
    let ack = timeout(Duration::from_secs(10), lines.next_line())
        .await
        .map_err(|_| err("subscribe ack timeout"))??
        .ok_or_else(|| err("subscribe: stream closed before ack"))?;
    let msg: Value = serde_json::from_str(&ack)?;
    if let Some(e) = msg.get("error") {
        let text = e.get("message").and_then(|v| v.as_str()).unwrap_or("subscribe failed");
        return Err(err(text.to_string()));
    }
    Ok(SocketEventStream { lines, _write: write })
}

/// A held event stream, either a raw herdr socket connection or a mux subscription.
pub enum EventStream {
    Socket(SocketEventStream),
    Mux(crate::muxclient::MuxEventStream),
}

impl EventStream {
    /// Next event; `None` when the stream has dropped.
    pub async fn next(&mut self) -> Option<EventEnvelope> {
        match self {
            EventStream::Socket(s) => s.next().await,
            EventStream::Mux(m) => m.next().await,
        }
    }
}

pub struct SocketEventStream {
    lines: tokio::io::Lines<BufReader<tokio::net::unix::OwnedReadHalf>>,
    _write: tokio::net::unix::OwnedWriteHalf, // keeps the connection open
}

impl SocketEventStream {
    /// Next event; `None` when the stream has dropped.
    async fn next(&mut self) -> Option<EventEnvelope> {
        loop {
            match self.lines.next_line().await {
                Ok(Some(line)) => {
                    let Ok(msg) = serde_json::from_str::<Value>(&line) else { continue };
                    if let Some(event) = msg.get("event").and_then(|v| v.as_str()) {
                        return Some(EventEnvelope {
                            event: event.to_string(),
                            data: msg.get("data").cloned().unwrap_or(Value::Null),
                        });
                    }
                }
                Ok(None) | Err(_) => return None,
            }
        }
    }
}
