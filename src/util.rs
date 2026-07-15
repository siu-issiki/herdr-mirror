// Shared plumbing: error alias, environment/path resolution, logging.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

pub fn err(msg: impl Into<String>) -> Error {
    msg.into().into()
}

pub fn home_dir() -> PathBuf {
    PathBuf::from(std::env::var("HOME").unwrap_or_else(|_| "/".into()))
}

/// The fixed state dir shared by daemon, actions, and pane wrappers. Kept a
/// single canonical path (never the plugin dir) so every invocation agrees on
/// the id map, pidfile, and the optimistic split claim/adopt files.
pub fn default_state_dir() -> PathBuf {
    home_dir().join(".local").join("state").join("herdr-mirror")
}

// ---------------------------------------------------------------------------
// optimistic local split: claim + adopt handoff files
//
// The remote-split action creates the local pane FIRST (optimistic), execs a
// pane wrapper in `--pending` mode, then splits the remote pane in the
// background. Two tiny files carry the handoff:
//
//   claim-<local_pane_id>.json   action → pending wrapper: which remote pane to
//                                stream ({"pane":"<rid>"}), or a failure
//                                ({"error":"…"}) so the wrapper self-closes.
//   adopt/<host>/<remote_pane_id> action → daemon: the local pane id that
//                                already mirrors this remote pane, so converge
//                                maps onto it instead of creating a new one.

/// `<state_dir>/claim-<local_pane_id>.json`
pub fn claim_path(state_dir: &Path, local_pane_id: &str) -> PathBuf {
    state_dir.join(format!("claim-{local_pane_id}.json"))
}

/// `<state_dir>/adopt/<host>/<remote_pane_id>`
pub fn adopt_path(state_dir: &Path, host: &str, remote_pane_id: &str) -> PathBuf {
    state_dir.join("adopt").join(host).join(remote_pane_id)
}

/// Parsed contents of a claim file.
#[derive(Debug, PartialEq, Eq)]
pub enum Claim {
    /// stream this remote pane
    Pane(String),
    /// the remote split failed; the wrapper should surface it and exit
    Error(String),
    /// unreadable / half-written / unrecognized — caller keeps polling
    Invalid,
}

/// Classify a claim file's JSON body. Unknown/garbage is `Invalid` (not an
/// error) so a reader that raced a half-written file just polls again.
pub fn parse_claim(s: &str) -> Claim {
    match serde_json::from_str::<serde_json::Value>(s) {
        Ok(v) => {
            if let Some(p) = v.get("pane").and_then(|x| x.as_str()).filter(|p| !p.is_empty()) {
                Claim::Pane(p.to_string())
            } else if let Some(e) = v.get("error").and_then(|x| x.as_str()) {
                Claim::Error(e.to_string())
            } else {
                Claim::Invalid
            }
        }
        Err(_) => Claim::Invalid,
    }
}

/// `{"pane":"<remote_pane_id>"}`
pub fn claim_json_pane(remote_pane_id: &str) -> String {
    serde_json::json!({ "pane": remote_pane_id }).to_string()
}

/// `{"error":"<msg>"}`
pub fn claim_json_error(msg: &str) -> String {
    serde_json::json!({ "error": msg }).to_string()
}

/// Write via a sibling `.tmp` + rename so a reader never observes a partial
/// file. Creates parent dirs.
pub fn write_atomic(path: &Path, contents: &str) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut tmp = path.as_os_str().to_os_string();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    fs::write(&tmp, contents)?;
    fs::rename(&tmp, path)
}

/// Is `path` older than `max_age`? False if it can't be stat'd. Used to sweep
/// stale claim/adopt files a converge left behind (self-healing).
pub fn older_than(path: &Path, max_age: std::time::Duration) -> bool {
    fs::metadata(path)
        .and_then(|m| m.modified())
        .ok()
        .and_then(|t| std::time::SystemTime::now().duration_since(t).ok())
        .is_some_and(|age| age > max_age)
}

/// Resolved runtime environment. Config prefers the plugin dir if hosts.toml
/// lives there; state is ALWAYS the fixed path so shell and plugin-action
/// invocations share one id map and pidfile.
pub struct Env {
    pub config_dir: PathBuf,
    pub state_dir: PathBuf,
    pub local_socket: PathBuf,
    pub plugin_root: PathBuf,
}

/// HERDR_PLUGIN_ROOT, else walk up from the binary to the manifest. Only used
/// as the mirror panes' cwd.
fn resolve_plugin_root() -> PathBuf {
    if let Ok(root) = std::env::var("HERDR_PLUGIN_ROOT") {
        if !root.is_empty() {
            return PathBuf::from(root);
        }
    }
    if let Ok(exe) = std::env::current_exe() {
        for dir in exe.ancestors().skip(1) {
            if dir.join("herdr-plugin.toml").exists() {
                return dir.to_path_buf();
            }
        }
    }
    home_dir()
}

impl Env {
    pub fn resolve() -> Result<Env> {
        let fallback_config = home_dir().join(".config").join("herdr-mirror");
        let config_dir = match std::env::var("HERDR_PLUGIN_CONFIG_DIR") {
            Ok(dir) if Path::new(&dir).join("hosts.toml").exists() => PathBuf::from(dir),
            _ => fallback_config,
        };
        let state_dir = default_state_dir();
        fs::create_dir_all(&config_dir)?;
        fs::create_dir_all(&state_dir)?;
        let local_socket = match std::env::var("HERDR_SOCKET_PATH") {
            Ok(s) if !s.is_empty() => PathBuf::from(s),
            _ => {
                let out = std::process::Command::new("herdr")
                    .args(["status", "--json"])
                    .output()
                    .map_err(|e| err(format!("cannot run herdr status: {e}")))?;
                let parsed: serde_json::Value = serde_json::from_slice(&out.stdout)?;
                let sock = parsed
                    .pointer("/server/socket")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if sock.is_empty() {
                    return Err(err(
                        "cannot resolve local herdr socket (HERDR_SOCKET_PATH unset, herdr status gave none)",
                    ));
                }
                PathBuf::from(sock)
            }
        };
        Ok(Env { config_dir, state_dir, local_socket, plugin_root: resolve_plugin_root() })
    }
}

/// Append-to-file logger (best-effort), optionally echoing to stdout.
#[derive(Clone)]
pub struct Logger {
    file: PathBuf,
    also_stdout: bool,
}

impl Logger {
    pub fn new(state_dir: &Path, also_stdout: bool) -> Logger {
        Logger { file: state_dir.join("daemon.log"), also_stdout }
    }

    pub fn log(&self, msg: &str) {
        let line = format!("{} {}\n", now_iso(), msg);
        if let Ok(mut f) = fs::OpenOptions::new().create(true).append(true).open(&self.file) {
            let _ = f.write_all(line.as_bytes());
        }
        if self.also_stdout {
            print!("{line}");
            let _ = std::io::stdout().flush();
        }
    }
}

/// ISO-8601 UTC timestamp without pulling in chrono.
pub fn now_iso() -> String {
    let d = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = d.as_secs();
    let millis = d.subsec_millis();
    let days = secs / 86400;
    let (y, mo, dy) = civil_from_days(days as i64);
    let rem = secs % 86400;
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
        y,
        mo,
        dy,
        rem / 3600,
        (rem % 3600) / 60,
        rem % 60,
        millis
    )
}

// Howard Hinnant's civil-from-days algorithm.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Is a pid alive? (signal 0)
pub fn pid_alive(pid: i32) -> bool {
    pid > 0 && unsafe { libc::kill(pid, 0) } == 0
}

/// Sleep until the earliest deadline; pend forever when none.
pub async fn sleep_until_earliest<I>(deadlines: I)
where
    I: IntoIterator<Item = Option<tokio::time::Instant>>,
{
    match deadlines.into_iter().flatten().min() {
        Some(d) => tokio::time::sleep_until(d).await,
        None => std::future::pending::<()>().await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn claim_and_adopt_paths() {
        let sd = Path::new("/state");
        assert_eq!(claim_path(sd, "w1:p2"), PathBuf::from("/state/claim-w1:p2.json"));
        assert_eq!(adopt_path(sd, "work", "wR:pR"), PathBuf::from("/state/adopt/work/wR:pR"));
    }

    #[test]
    fn claim_serialize_roundtrips_through_parse() {
        assert_eq!(parse_claim(&claim_json_pane("wR:pR")), Claim::Pane("wR:pR".into()));
        assert_eq!(parse_claim(&claim_json_error("boom")), Claim::Error("boom".into()));
    }

    #[test]
    fn parse_claim_classifies_bodies() {
        assert_eq!(parse_claim(r#"{"pane":"wR:pR"}"#), Claim::Pane("wR:pR".into()));
        assert_eq!(parse_claim(r#"{"error":"remote split failed"}"#), Claim::Error("remote split failed".into()));
        // an empty pane string is not a usable claim
        assert_eq!(parse_claim(r#"{"pane":""}"#), Claim::Invalid);
        // unknown keys / empty object / half-written garbage → keep polling
        assert_eq!(parse_claim("{}"), Claim::Invalid);
        assert_eq!(parse_claim(r#"{"other":1}"#), Claim::Invalid);
        assert_eq!(parse_claim(r#"{"pane":"#), Claim::Invalid);
        assert_eq!(parse_claim(""), Claim::Invalid);
    }

    #[test]
    fn write_atomic_creates_parents_and_leaves_no_tmp() {
        let dir = std::env::temp_dir().join(format!(
            "herdr-util-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        let target = dir.join("adopt").join("work").join("wR:pR");
        write_atomic(&target, "w9:p1").unwrap();
        assert_eq!(fs::read_to_string(&target).unwrap(), "w9:p1");
        // the sibling temp file must be gone after the rename
        let mut tmp = target.as_os_str().to_os_string();
        tmp.push(".tmp");
        assert!(!PathBuf::from(tmp).exists());
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn older_than_reads_mtime() {
        let dir = std::env::temp_dir().join(format!("herdr-util-age-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let f = dir.join("fresh");
        fs::write(&f, "x").unwrap();
        assert!(!older_than(&f, std::time::Duration::from_secs(60)));
        // a nonexistent file is never "old" (can't be stat'd)
        assert!(!older_than(&dir.join("missing"), std::time::Duration::from_secs(0)));
        fs::remove_dir_all(&dir).ok();
    }
}
