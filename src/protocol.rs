// Mux NDJSON protocol shared by the local mux task and the remote agent.
//
// One JSON object per line ("op" is the discriminator). sids are assigned by
// the local side; the agent echoes the same sid on replies. `ping`/`hello`
// carry no sid. Raw child stdout / event payloads are embedded as JSON strings
// in `d` (their base64-dominated content needs no meaningful escaping).
//
// See docs/mux-design.md "Mux protocol".

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A single mux envelope. Tagged on `op`; both directions share the type.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op")]
pub enum Msg {
    // ---- client → agent ----
    /// Spawn a `herdr terminal session` child for this sid.
    #[serde(rename = "open")]
    Open {
        sid: u64,
        pane: String,
        /// "observe" | "control"
        mode: String,
        cols: u32,
        rows: u32,
        #[serde(default, skip_serializing_if = "is_false")]
        takeover: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        session: Option<String>,
    },
    /// Raw stdin line for the sid's child (agent appends a newline).
    #[serde(rename = "input")]
    Input { sid: u64, d: String },
    /// SIGTERM the sid's child.
    #[serde(rename = "close")]
    Close { sid: u64 },
    /// One-shot request on the remote herdr socket.
    #[serde(rename = "api")]
    Api { sid: u64, method: String, params: Value },
    /// Held events.subscribe; a new `sub` replaces the previous one.
    #[serde(rename = "sub")]
    Sub { sid: u64, subs: Vec<Value> },
    /// `sh -c cmd`, buffered result.
    #[serde(rename = "exec")]
    Exec { sid: u64, cmd: String },

    // ---- agent → client ----
    /// First line after the agent starts.
    #[serde(rename = "hello")]
    Hello { version: String, herdr_socket: String },
    /// Raw child stdout line, verbatim.
    #[serde(rename = "l")]
    Line { sid: u64, d: String },
    /// Terminal child exited.
    #[serde(rename = "exit")]
    Exit { sid: u64, code: i32 },
    /// Result of an `api` request (exactly one of result / error is set).
    #[serde(rename = "api_res")]
    ApiRes {
        sid: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        result: Option<Value>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
    /// A subscribed event line (raw event JSON as a string).
    #[serde(rename = "ev")]
    Ev { sid: u64, d: String },
    /// Result of an `exec` request.
    #[serde(rename = "exec_res")]
    ExecRes { sid: u64, code: i32, out: String },
    /// Emitted every 5s unconditionally (liveness).
    #[serde(rename = "ping")]
    Ping { seq: u64 },
}

fn is_false(b: &bool) -> bool {
    !*b
}

impl Msg {
    /// The sid this envelope carries, if any (`hello`/`ping` have none).
    // Consumed by the phase-2 mux task when routing replies back to clients.
    #[allow(dead_code)]
    pub fn sid(&self) -> Option<u64> {
        match self {
            Msg::Open { sid, .. }
            | Msg::Input { sid, .. }
            | Msg::Close { sid, .. }
            | Msg::Api { sid, .. }
            | Msg::Sub { sid, .. }
            | Msg::Exec { sid, .. }
            | Msg::Line { sid, .. }
            | Msg::Exit { sid, .. }
            | Msg::ApiRes { sid, .. }
            | Msg::Ev { sid, .. }
            | Msg::ExecRes { sid, .. } => Some(*sid),
            Msg::Hello { .. } | Msg::Ping { .. } => None,
        }
    }

    /// Serialize to a single NDJSON line (trailing newline included).
    pub fn to_line(&self) -> String {
        // Struct variants over plain scalars/Value never fail to serialize.
        let mut s = serde_json::to_string(self).expect("Msg serializes");
        s.push('\n');
        s
    }
}

/// Parse one NDJSON line. Returns `None` for blank or malformed lines so a
/// reader can skip them and keep going (the read side must never abort on a
/// single bad line).
pub fn parse_line(line: &str) -> Option<Msg> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return None;
    }
    serde_json::from_str(trimmed).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn roundtrip(msg: Msg) {
        let line = msg.to_line();
        assert!(line.ends_with('\n'), "line must be newline-terminated");
        assert!(!line[..line.len() - 1].contains('\n'), "exactly one line");
        let back = parse_line(&line).expect("parses back");
        assert_eq!(msg, back);
    }

    #[test]
    fn roundtrip_all_ops() {
        roundtrip(Msg::Open {
            sid: 1,
            pane: "w9:p1".into(),
            mode: "control".into(),
            cols: 176,
            rows: 66,
            takeover: true,
            session: Some("work".into()),
        });
        // open without optional fields
        roundtrip(Msg::Open {
            sid: 2,
            pane: "p0".into(),
            mode: "observe".into(),
            cols: 80,
            rows: 24,
            takeover: false,
            session: None,
        });
        roundtrip(Msg::Input { sid: 3, d: "{\"k\":\"a\"}".into() });
        roundtrip(Msg::Close { sid: 4 });
        roundtrip(Msg::Api { sid: 5, method: "tab.focus".into(), params: json!({"id": "x"}) });
        roundtrip(Msg::Sub { sid: 6, subs: vec![json!({"type": "workspace"}), json!("agents")] });
        roundtrip(Msg::Exec { sid: 7, cmd: "echo hi".into() });
        roundtrip(Msg::Hello { version: "0.1.8".into(), herdr_socket: "/tmp/herdr.sock".into() });
        roundtrip(Msg::Line { sid: 8, d: "AAAA==".into() });
        roundtrip(Msg::Exit { sid: 9, code: 0 });
        roundtrip(Msg::ApiRes { sid: 10, result: Some(json!({"ok": true})), error: None });
        roundtrip(Msg::ApiRes { sid: 11, result: None, error: Some("boom".into()) });
        roundtrip(Msg::Ev { sid: 12, d: "{\"event\":\"e\"}".into() });
        roundtrip(Msg::ExecRes { sid: 13, code: 2, out: "partial".into() });
        roundtrip(Msg::Ping { seq: 42 });
    }

    #[test]
    fn sid_accessor() {
        assert_eq!(Msg::Close { sid: 5 }.sid(), Some(5));
        assert_eq!(Msg::Ping { seq: 1 }.sid(), None);
        assert_eq!(
            Msg::Hello { version: "v".into(), herdr_socket: "s".into() }.sid(),
            None
        );
    }

    #[test]
    fn skips_malformed_lines() {
        assert!(parse_line("").is_none());
        assert!(parse_line("   ").is_none());
        assert!(parse_line("not json at all").is_none());
        assert!(parse_line("{ broken json").is_none());
        assert!(parse_line("{\"op\":\"nope\"}").is_none()); // unknown op
        assert!(parse_line("{\"sid\":1}").is_none()); // no op tag
        // a valid line still parses
        assert_eq!(parse_line("{\"op\":\"ping\",\"seq\":3}"), Some(Msg::Ping { seq: 3 }));
    }

    #[test]
    fn hello_and_ping_have_no_sid_on_wire() {
        let hello = Msg::Hello { version: "1".into(), herdr_socket: "/s".into() }.to_line();
        assert!(!hello.contains("sid"), "hello must not serialize a sid: {hello}");
        let ping = Msg::Ping { seq: 0 }.to_line();
        assert!(!ping.contains("sid"), "ping must not serialize a sid: {ping}");
    }

    #[test]
    fn open_omits_default_takeover_and_none_session() {
        let line = Msg::Open {
            sid: 1,
            pane: "p".into(),
            mode: "observe".into(),
            cols: 80,
            rows: 24,
            takeover: false,
            session: None,
        }
        .to_line();
        assert!(!line.contains("takeover"), "default takeover omitted: {line}");
        assert!(!line.contains("session"), "none session omitted: {line}");
    }
}
