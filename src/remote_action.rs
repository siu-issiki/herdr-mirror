// Remote-create plugin actions: create workspaces/tabs/panes on the REMOTE
// herdr from the local one, inheriting target host and cwd from where the
// action was invoked — the same inheritance rule as native prefix+shift+n,
// extended across the machine boundary.
//
//   herdr-mirror remote-workspace           # new workspace on the context's host
//   herdr-mirror remote-tab                 # new tab in the mirrored remote workspace
//   herdr-mirror remote-split right|down    # split the mirrored remote pane
//
// Resolution: the invocation context's local workspace/tab/pane ids are
// reverse-looked-up in the per-host id maps. Inside a mirror, that pins both
// the host and the remote object (and the remote pane's own cwd). Outside a
// mirror, only `remote-workspace` works, targeting hosts.toml `default_host`
// (else the first host declared).
//
// These create REMOTE objects only; the daemon mirrors them back within a
// couple of seconds. Local mirror objects stay daemon-owned.

use std::time::Duration;

use serde::Deserialize;
use serde_json::{json, Value};

use crate::api::ApiClient;
use crate::config::{load_config, HostConfig};
use crate::remote::RemoteHost;
use crate::state::{load_state, HostState};
use crate::util::{err, Env, Logger, Result};

/// how often (and how long) to poll the host state file for the daemon's
/// mirror of a just-created remote object, before giving up on focusing it.
/// Kept short: this is on the hot path between keypress and split appearing.
const MIRROR_POLL_INTERVAL: Duration = Duration::from_millis(15);
const MIRROR_POLL_TIMEOUT: Duration = Duration::from_millis(4000);

#[derive(Debug, Default, Deserialize, PartialEq, Eq)]
struct InvocationContext {
    workspace_id: Option<String>,
    focused_pane_id: Option<String>,
}

/// Build the invocation context, preferring the plugin-invoke JSON blob
/// (`HERDR_PLUGIN_CONTEXT_JSON`, set by `herdr plugin action invoke`) and
/// falling back to the `HERDR_ACTIVE_WORKSPACE_ID` / `HERDR_ACTIVE_PANE_ID`
/// env vars herdr injects into a keybinding's custom-command shell. The
/// fallback is what lets a keybinding exec this binary directly (skipping
/// the socket round-trip + plugin-process spawn of `action invoke`) while
/// still resolving the same workspace/pane context.
///
/// Takes its inputs as plain arguments rather than reading `std::env`
/// itself so tests can exercise the fallback without mutating real process
/// env vars (which would race across parallel tests).
fn context_from_env(
    plugin_context_json: Option<String>,
    active_workspace_id: Option<String>,
    active_pane_id: Option<String>,
) -> InvocationContext {
    let mut ctx: InvocationContext = plugin_context_json
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default();
    if ctx.workspace_id.is_none() {
        ctx.workspace_id = active_workspace_id.filter(|s| !s.is_empty());
    }
    if ctx.focused_pane_id.is_none() {
        ctx.focused_pane_id = active_pane_id.filter(|s| !s.is_empty());
    }
    ctx
}

struct Resolved {
    host: HostConfig,
    remote_ws_id: Option<String>,
    remote_pane_id: Option<String>,
    /// the focused remote pane's cwd as cached by the daemon's last converge,
    /// if known — lets `run` skip a live `pane.get` round-trip
    remote_pane_cwd: Option<String>,
}

/// find which host (if any) mirrors the workspace the action was invoked from
fn resolve_context(env: &Env, hosts: &[HostConfig], ctx: &InvocationContext) -> Option<Resolved> {
    for host in hosts {
        let state = load_state(&env.state_dir, &host.name);
        let ws_hit = state.workspaces.iter().find(|(_, e)| {
            Some(&e.local_id) == ctx.workspace_id.as_ref() && !e.is_tombstoned()
        });
        let Some((ws_rid, _)) = ws_hit else { continue };
        let pane_hit = state.panes.iter().find(|(_, e)| {
            Some(&e.local_id) == ctx.focused_pane_id.as_ref() && !e.is_tombstoned()
        });
        return Some(Resolved {
            host: host.clone(),
            remote_ws_id: Some(ws_rid.clone()),
            remote_pane_id: pane_hit.map(|(rid, _)| rid.clone()),
            remote_pane_cwd: pane_hit.and_then(|(_, e)| e.cwd.clone()),
        });
    }
    None
}

pub async fn run(env: Env, kind: &str, direction: Option<&str>) -> Result<()> {
    if kind == "split" && !matches!(direction, Some("right") | Some("down")) {
        return Err(err("remote-split needs a direction: right|down"));
    }

    let ctx = context_from_env(
        std::env::var("HERDR_PLUGIN_CONTEXT_JSON").ok(),
        std::env::var("HERDR_ACTIVE_WORKSPACE_ID").ok(),
        std::env::var("HERDR_ACTIVE_PANE_ID").ok(),
    );
    let config = load_config(&env.config_dir)?;
    let resolved = resolve_context(&env, &config.hosts, &ctx);

    if resolved.is_none() && kind != "workspace" {
        return Err(err(format!(
            "remote {kind}: invoke this from inside a mirror workspace so the target host and {} are known",
            if kind == "tab" { "workspace" } else { "pane" }
        )));
    }
    let host = resolved
        .as_ref()
        .map(|r| r.host.clone())
        .or_else(|| config.default_host().cloned())
        .ok_or_else(|| err("no hosts configured"))?;

    // Prefer the daemon's single mux ssh connection: if its socket accepts us,
    // run the create over an `api` op like every other client. Only when the mux
    // isn't reachable (no daemon, or it's down) fall back to the legacy per-action
    // ControlMaster + api-socket forward.
    let mux_sock = crate::mux::sock_path(&env.state_dir, &host.name);
    let api = match crate::muxclient::MuxApi::connect(&mux_sock).await {
        Ok(m) => ApiClient::mux(m),
        Err(_) => {
            let mut remote = RemoteHost::new(&host, &env.state_dir);
            remote.connect_api_fast().await?
        }
    };

    // cwd inheritance comes from the REMOTE side: the remote pane behind the
    // focused mirror pane knows its real cwd; local cwds are meaningless there.
    // The daemon caches that cwd into the state file on every converge, so the
    // common path reads it locally and skips a round-trip. Only when the cache
    // is cold (pane mapped but no cwd recorded yet) do we fall back to pane.get.
    let mut cwd: Option<String> = resolved.as_ref().and_then(|r| r.remote_pane_cwd.clone());
    if cwd.is_none() {
        if let Some(pane_id) = resolved.as_ref().and_then(|r| r.remote_pane_id.clone()) {
            // one pane.get instead of a full snapshot — cache-miss fallback only
            if let Ok(res) = api.request("pane.get", json!({ "pane_id": pane_id })).await {
                cwd = res
                    .pointer("/pane/foreground_cwd")
                    .and_then(|v| v.as_str())
                    .map(String::from)
                    .or_else(|| res.pointer("/pane/cwd").and_then(|v| v.as_str()).map(String::from));
            }
        }
    }

    // Optimistic local split (the cmd+d hot path): create and focus the LOCAL
    // mirror pane FIRST (~50ms), exec a pending wrapper into it, then split the
    // remote pane in the background and hand the new remote pane id to both the
    // wrapper (claim file → start streaming) and the daemon (adopt file → map
    // onto the existing pane instead of creating another). Only `split` is
    // optimistic; remote-tab / remote-workspace keep the legacy path below. If
    // the local side can't be set up, we fall through to the legacy remote-first
    // split so the feature never makes cmd+d worse than before.
    if kind == "split" {
        let dir = direction.unwrap();
        let remote_pane_id = resolved.as_ref().and_then(|r| r.remote_pane_id.clone());
        let remote_ws_id = resolved.as_ref().and_then(|r| r.remote_ws_id.clone());
        let local_target = ctx.focused_pane_id.clone();
        if let (Some(remote_pane_id), Some(remote_ws_id), Some(local_target)) =
            (remote_pane_id, remote_ws_id, local_target)
        {
            if let Some(new_local_id) =
                optimistic_local_split(&env, &host, &local_target, &remote_ws_id, dir).await
            {
                return finish_optimistic_split(
                    &env, &host, &api, &remote_pane_id, &new_local_id, dir, cwd,
                )
                .await;
            }
            // local setup failed → fall through to the legacy remote-first path
        }
    }

    let new_remote_id: Option<String> = match kind {
        "workspace" => {
            let res: Value = api.request("workspace.create", json!({ "cwd": cwd, "focus": false })).await?;
            let remote_id =
                res.pointer("/workspace/workspace_id").and_then(|v| v.as_str()).map(String::from);
            println!(
                "created workspace {} ({}) on {}; mirror follows shortly",
                res.pointer("/workspace/label").and_then(|v| v.as_str()).unwrap_or("?"),
                remote_id.as_deref().unwrap_or("?"),
                host.name
            );
            remote_id
        }
        "tab" => {
            let ws = resolved.as_ref().and_then(|r| r.remote_ws_id.clone()).unwrap();
            let res: Value = api
                .request("tab.create", json!({ "workspace_id": ws, "cwd": cwd, "focus": false }))
                .await?;
            let remote_id = res.pointer("/tab/tab_id").and_then(|v| v.as_str()).map(String::from);
            println!(
                "created tab {} in {}: {ws}; mirror follows shortly",
                remote_id.as_deref().unwrap_or("?"),
                host.name
            );
            remote_id
        }
        "split" => {
            let Some(pane_id) = resolved.as_ref().and_then(|r| r.remote_pane_id.clone()) else {
                return Err(err("remote split: the focused pane is not a mirrored pane"));
            };
            let dir = direction.unwrap();
            let res: Value = api
                .request(
                    "pane.split",
                    json!({ "target_pane_id": pane_id, "direction": dir, "cwd": cwd, "focus": false }),
                )
                .await?;
            let remote_id = res.pointer("/pane/pane_id").and_then(|v| v.as_str()).map(String::from);
            println!(
                "split {pane_id} {dir} on {} → {}; mirror follows shortly",
                host.name,
                remote_id.as_deref().unwrap_or("ok")
            );
            remote_id
        }
        _ => return Err(err(format!("unknown remote action: {kind}"))),
    };

    // Native split/new-tab/new-workspace all move focus to the freshly
    // created object; match that here even though the object we just made is
    // remote and its local mirror doesn't exist yet — the daemon creates it
    // within a couple hundred ms. Best-effort only: any failure or timeout is
    // swallowed so it never turns a successful remote-create into an error.
    if let Some(remote_id) = new_remote_id {
        focus_new_mirror(&env, &host.name, kind, &remote_id).await;
    }
    Ok(())
}

/// Look up the local mirror id for a freshly-created remote object, if the
/// daemon's converge has already mapped it (and, for workspaces/panes, the
/// mapping isn't a tombstone). Kept as a plain sync function over `HostState`
/// so the polling logic can be unit-tested without an async runtime.
fn lookup_mirror_local_id(state: &HostState, kind: &str, remote_id: &str) -> Option<String> {
    match kind {
        "workspace" => state.workspaces.get(remote_id).filter(|e| !e.is_tombstoned()).map(|e| e.local_id.clone()),
        "tab" => state.tabs.get(remote_id).map(|e| e.local_id.clone()),
        "split" => state.panes.get(remote_id).filter(|e| !e.is_tombstoned()).map(|e| e.local_id.clone()),
        _ => None,
    }
}

/// Poll the host state file until the daemon has mirrored `remote_id`
/// locally, or give up after MIRROR_POLL_TIMEOUT.
async fn wait_for_mirror_local_id(env: &Env, host: &str, kind: &str, remote_id: &str) -> Option<String> {
    wait_for_mirror_local_id_with_timeout(env, host, kind, remote_id, MIRROR_POLL_TIMEOUT).await
}

/// Same as `wait_for_mirror_local_id`, but with an explicit timeout — split
/// out so tests can exercise the give-up path without waiting 4 real seconds.
async fn wait_for_mirror_local_id_with_timeout(
    env: &Env,
    host: &str,
    kind: &str,
    remote_id: &str,
    poll_timeout: Duration,
) -> Option<String> {
    let deadline = tokio::time::Instant::now() + poll_timeout;
    loop {
        let state = load_state(&env.state_dir, host);
        if let Some(local_id) = lookup_mirror_local_id(&state, kind, remote_id) {
            return Some(local_id);
        }
        if tokio::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(MIRROR_POLL_INTERVAL).await;
    }
}

/// Best-effort: once the daemon mirrors a just-created remote object
/// locally, move local focus onto it — mirroring native new-workspace/new-tab
/// /split behavior. Never propagates an error: a timeout or API failure is
/// logged and swallowed, since the remote object was already created
/// successfully regardless of whether we can focus its mirror.
async fn focus_new_mirror(env: &Env, host: &str, kind: &str, remote_id: &str) {
    let Some(local_id) = wait_for_mirror_local_id(env, host, kind, remote_id).await else {
        println!("mirror focus timed out waiting for the {kind} mirror of {remote_id}");
        return;
    };
    let local = match ApiClient::connect(&env.local_socket).await {
        Ok(c) => c,
        Err(e) => {
            println!("mirror focus: could not connect to local herdr: {e}");
            return;
        }
    };
    // tab.focus verified against a live herdr socket (preview 2026-06-30):
    // {"method":"tab.focus","params":{"tab_id":...}} focuses the tab (and its
    // workspace). No fallback needed.
    let result = match kind {
        "workspace" => local.request("workspace.focus", json!({ "workspace_id": local_id })).await,
        "tab" => local.request("tab.focus", json!({ "tab_id": local_id })).await,
        "split" => local.request("pane.focus", json!({ "pane_id": local_id })).await,
        _ => return,
    };
    if let Err(e) = result {
        println!("mirror focus failed: {e}");
    }
}

/// Optimistic split, local half: create the mirror pane next to the focused
/// one and exec a pending wrapper into it. Returns the new local pane id, or
/// None if the local herdr couldn't be reached or the split failed (caller
/// falls back to the legacy remote-first path). Best-effort by design.
async fn optimistic_local_split(
    env: &Env,
    host: &HostConfig,
    local_target: &str,
    remote_ws_id: &str,
    dir: &str,
) -> Option<String> {
    let local = ApiClient::connect(&env.local_socket).await.ok()?;
    // the per-workspace mirror proxy cwd tags the new pane as a mirror, so the
    // daemon's loop guard won't try to push it back to the remote
    let cwd = crate::mirror::ensure_ws_cwd(&env.state_dir, &host.name, remote_ws_id);
    let res = local
        .request(
            "pane.split",
            json!({ "target_pane_id": local_target, "direction": dir, "cwd": cwd, "focus": true }),
        )
        .await
        .ok()?;
    let new_local_id = res.pointer("/pane/pane_id").and_then(|v| v.as_str())?.to_string();
    // same wrapper the daemon would spawn, but in --pending mode with no pane
    // target yet — it waits on the claim file we write once the remote lands
    let argv = crate::mirror::pending_streamer_argv(host, &env.state_dir);
    crate::mirror::spawn_streamer_pane(&local, &new_local_id, &argv, &Logger::new(&env.state_dir, false)).await;
    Some(new_local_id)
}

/// Optimistic split, remote half + handoff: split the remote pane, then write
/// the adopt file (daemon: map onto the existing local pane) BEFORE the claim
/// file (wrapper: start streaming this remote pane). On remote failure, write
/// an error claim so the pending wrapper surfaces it and self-closes.
async fn finish_optimistic_split(
    env: &Env,
    host: &HostConfig,
    api: &ApiClient,
    remote_pane_id: &str,
    new_local_id: &str,
    dir: &str,
    cwd: Option<String>,
) -> Result<()> {
    match api
        .request(
            "pane.split",
            json!({ "target_pane_id": remote_pane_id, "direction": dir, "cwd": cwd, "focus": false }),
        )
        .await
    {
        Ok(res) => {
            let new_remote_id =
                res.pointer("/pane/pane_id").and_then(|v| v.as_str()).map(String::from).filter(|s| !s.is_empty());
            let Some(new_remote_id) = new_remote_id else {
                write_claim(env, new_local_id, &crate::util::claim_json_error("remote split returned no pane id"));
                return Err(err("remote split returned no pane id"));
            };
            // adopt BEFORE claim: the daemon's converge (driven by the remote
            // pane_created event) must see the adopt marker to map rather than
            // create; the wrapper's claim only makes it start streaming.
            let adopt = crate::util::adopt_path(&env.state_dir, &host.name, &new_remote_id);
            if let Err(e) = crate::util::write_atomic(&adopt, new_local_id) {
                // non-fatal: without adopt the daemon may create a second pane
                // and the wrapper takes it over via --takeover; still works.
                eprintln!("optimistic split: adopt write failed: {e}");
            }
            write_claim(env, new_local_id, &crate::util::claim_json_pane(&new_remote_id));
            println!(
                "split {remote_pane_id} {dir} on {} → {new_remote_id} (optimistic; local {new_local_id})",
                host.name
            );
            Ok(())
        }
        Err(e) => {
            write_claim(env, new_local_id, &crate::util::claim_json_error(&e.to_string()));
            Err(e)
        }
    }
}

/// Write a pending pane's claim file (best-effort; a failure just means the
/// wrapper eventually times out and self-closes).
fn write_claim(env: &Env, local_pane_id: &str, body: &str) {
    let path = crate::util::claim_path(&env.state_dir, local_pane_id);
    if let Err(e) = crate::util::write_atomic(&path, body) {
        eprintln!("optimistic split: claim write failed: {e}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{save_state, HostState, PaneEntry, WsEntry};

    fn host(name: &str) -> HostConfig {
        HostConfig {
            name: name.into(),
            target: "user@host".into(),
            prefix: "h".into(),
            remote_bin: "herdr".into(),
            agent_bin: None,
            always_control: true,
        }
    }

    fn tmp_env() -> Env {
        let dir = std::env::temp_dir().join(format!(
            "herdr-mirror-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        Env {
            config_dir: dir.clone(),
            state_dir: dir.clone(),
            local_socket: dir.join("sock"),
            plugin_root: dir,
        }
    }

    /// resolve_context surfaces the daemon-cached remote cwd so `run` can skip
    /// the live pane.get; a pane with no cached cwd yields None (cache miss →
    /// pane.get fallback).
    #[test]
    fn resolve_context_reads_cached_cwd() {
        let env = tmp_env();
        let mut state = HostState::default();
        state.workspaces.insert(
            "wR".into(),
            WsEntry { local_id: "wL".into(), tombstone: None, root_tab_local_id: None, last_remote_label: None },
        );
        state.panes.insert(
            "wR:pR".into(),
            PaneEntry {
                local_id: "wL:pL".into(),
                tombstone: None,
                seq: 0,
                reported: None,
                cwd: Some("/remote/work/dir".into()),
            },
        );
        // a second pane with no cached cwd yet
        state.panes.insert(
            "wR:pR2".into(),
            PaneEntry { local_id: "wL:pL2".into(), tombstone: None, seq: 0, reported: None, cwd: None },
        );
        save_state(&env.state_dir, "h1", &state).unwrap();

        let hosts = vec![host("h1")];

        // cached cwd is returned directly
        let ctx = InvocationContext {
            workspace_id: Some("wL".into()),
            focused_pane_id: Some("wL:pL".into()),
        };
        let r = resolve_context(&env, &hosts, &ctx).expect("resolves");
        assert_eq!(r.remote_pane_id.as_deref(), Some("wR:pR"));
        assert_eq!(r.remote_pane_cwd.as_deref(), Some("/remote/work/dir"));

        // pane without a cached cwd → None (falls back to pane.get in run)
        let ctx2 = InvocationContext {
            workspace_id: Some("wL".into()),
            focused_pane_id: Some("wL:pL2".into()),
        };
        let r2 = resolve_context(&env, &hosts, &ctx2).expect("resolves");
        assert_eq!(r2.remote_pane_id.as_deref(), Some("wR:pR2"));
        assert_eq!(r2.remote_pane_cwd, None);

        std::fs::remove_dir_all(&env.state_dir).ok();
    }

    /// context_from_env prefers a complete plugin-invoke JSON blob; when it's
    /// absent, unparsable, or present-but-empty, it falls back to the
    /// HERDR_ACTIVE_WORKSPACE_ID / HERDR_ACTIVE_PANE_ID values herdr injects
    /// into a keybinding's custom-command shell — the mechanism that lets a
    /// keybinding exec this binary directly instead of going through
    /// `herdr plugin action invoke`.
    #[test]
    fn context_from_env_falls_back_to_active_env_vars() {
        // complete plugin JSON: used as-is, env fallback never consulted
        let ctx = context_from_env(
            Some(r#"{"workspace_id":"wJ","focused_pane_id":"pJ"}"#.into()),
            Some("wEnv".into()),
            Some("pEnv".into()),
        );
        assert_eq!(ctx, InvocationContext {
            workspace_id: Some("wJ".into()),
            focused_pane_id: Some("pJ".into()),
        });

        // no plugin JSON at all (direct binary exec): fall back fully to env
        let ctx = context_from_env(None, Some("wEnv".into()), Some("pEnv".into()));
        assert_eq!(ctx, InvocationContext {
            workspace_id: Some("wEnv".into()),
            focused_pane_id: Some("pEnv".into()),
        });

        // plugin JSON present but an empty object: still falls back
        let ctx = context_from_env(Some("{}".into()), Some("wEnv".into()), Some("pEnv".into()));
        assert_eq!(ctx, InvocationContext {
            workspace_id: Some("wEnv".into()),
            focused_pane_id: Some("pEnv".into()),
        });

        // plugin JSON unparsable garbage: treated like absent, falls back
        let ctx = context_from_env(Some("not json".into()), Some("wEnv".into()), Some("pEnv".into()));
        assert_eq!(ctx, InvocationContext {
            workspace_id: Some("wEnv".into()),
            focused_pane_id: Some("pEnv".into()),
        });

        // neither source present: stays empty, exactly like today's
        // outside-a-mirror-workspace behavior
        let ctx = context_from_env(None, None, None);
        assert_eq!(ctx, InvocationContext::default());

        // env vars present but empty strings count as absent, not as ids
        let ctx = context_from_env(None, Some(String::new()), Some(String::new()));
        assert_eq!(ctx, InvocationContext::default());
    }

    /// End-to-end: a context built purely from the HERDR_ACTIVE_* env-var
    /// fallback (as a keybinding execing the plugin binary directly would
    /// produce, with no HERDR_PLUGIN_CONTEXT_JSON at all) still resolves to
    /// the right host/pane through resolve_context — the direct-exec path
    /// reaches the same resolution logic as the plugin action invoke path.
    #[test]
    fn resolve_context_works_with_env_fallback_context() {
        let env = tmp_env();
        let mut state = HostState::default();
        state.workspaces.insert(
            "wR".into(),
            WsEntry { local_id: "wL".into(), tombstone: None, root_tab_local_id: None, last_remote_label: None },
        );
        state.panes.insert(
            "wR:pR".into(),
            PaneEntry {
                local_id: "wL:pL".into(),
                tombstone: None,
                seq: 0,
                reported: None,
                cwd: Some("/remote/work/dir".into()),
            },
        );
        save_state(&env.state_dir, "h1", &state).unwrap();

        let hosts = vec![host("h1")];
        let ctx = context_from_env(None, Some("wL".into()), Some("wL:pL".into()));
        let r = resolve_context(&env, &hosts, &ctx).expect("resolves");
        assert_eq!(r.remote_pane_id.as_deref(), Some("wR:pR"));
        assert_eq!(r.remote_pane_cwd.as_deref(), Some("/remote/work/dir"));

        std::fs::remove_dir_all(&env.state_dir).ok();
    }

    fn ws_entry(local_id: &str, tombstoned: bool) -> WsEntry {
        WsEntry {
            local_id: local_id.into(),
            tombstone: tombstoned.then_some(true),
            root_tab_local_id: None,
            last_remote_label: None,
        }
    }

    fn pane_entry(local_id: &str, tombstoned: bool) -> PaneEntry {
        PaneEntry {
            local_id: local_id.into(),
            tombstone: tombstoned.then_some(true),
            seq: 0,
            reported: None,
            cwd: None,
        }
    }

    /// lookup_mirror_local_id surfaces the local id for each object kind once
    /// the daemon has written it into the host state map.
    #[test]
    fn lookup_mirror_local_id_finds_mapped_objects() {
        let mut state = HostState::default();
        state.workspaces.insert("wR".into(), ws_entry("wL", false));
        state.tabs.insert("wR:tR".into(), crate::state::TabEntry { local_id: "wL:tL".into() });
        state.panes.insert("wR:pR".into(), pane_entry("wL:pL", false));

        assert_eq!(lookup_mirror_local_id(&state, "workspace", "wR").as_deref(), Some("wL"));
        assert_eq!(lookup_mirror_local_id(&state, "tab", "wR:tR").as_deref(), Some("wL:tL"));
        assert_eq!(lookup_mirror_local_id(&state, "split", "wR:pR").as_deref(), Some("wL:pL"));
    }

    /// Not-yet-mirrored objects (absent from the map) yield None, so the
    /// caller keeps polling instead of focusing a stale/nonexistent id.
    #[test]
    fn lookup_mirror_local_id_absent_yields_none() {
        let state = HostState::default();
        assert_eq!(lookup_mirror_local_id(&state, "workspace", "wR"), None);
        assert_eq!(lookup_mirror_local_id(&state, "tab", "wR:tR"), None);
        assert_eq!(lookup_mirror_local_id(&state, "split", "wR:pR"), None);
        assert_eq!(lookup_mirror_local_id(&state, "bogus-kind", "wR"), None);
    }

    /// A tombstoned workspace/pane mapping means "the user closed this
    /// mirror" — never treat it as the freshly-created object's mirror.
    #[test]
    fn lookup_mirror_local_id_ignores_tombstones() {
        let mut state = HostState::default();
        state.workspaces.insert("wR".into(), ws_entry("wL", true));
        state.panes.insert("wR:pR".into(), pane_entry("wL:pL", true));

        assert_eq!(lookup_mirror_local_id(&state, "workspace", "wR"), None);
        assert_eq!(lookup_mirror_local_id(&state, "split", "wR:pR"), None);
    }

    /// wait_for_mirror_local_id polls the state file and returns as soon as
    /// the daemon writes the mapping, without waiting for the full timeout.
    #[tokio::test]
    async fn wait_for_mirror_local_id_picks_up_a_delayed_write() {
        let env = tmp_env();
        let host_name = "h1";

        let state_dir = env.state_dir.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(120));
            let mut state = HostState::default();
            state.workspaces.insert("wR".into(), ws_entry("wL", false));
            save_state(&state_dir, host_name, &state).unwrap();
        });

        let started = std::time::Instant::now();
        let found = wait_for_mirror_local_id(&env, host_name, "workspace", "wR").await;
        assert_eq!(found.as_deref(), Some("wL"));
        // well under MIRROR_POLL_TIMEOUT: proves it returned on the poll that
        // observed the write, not by exhausting the deadline
        assert!(started.elapsed() < Duration::from_secs(2));

        std::fs::remove_dir_all(&env.state_dir).ok();
    }

    /// An object that never gets mirrored (e.g. the daemon is down) times out
    /// instead of hanging forever; the short timeout here is a local override
    /// on top of the same polling logic `run` uses with MIRROR_POLL_TIMEOUT.
    #[tokio::test]
    async fn wait_for_mirror_local_id_times_out_when_never_mapped() {
        let env = tmp_env();
        let found = tokio::time::timeout(
            Duration::from_millis(500),
            wait_for_mirror_local_id_with_timeout(&env, "h1", "workspace", "wR-never", Duration::from_millis(150)),
        )
        .await
        .expect("inner poll loop itself must respect its own timeout");
        assert_eq!(found, None);

        std::fs::remove_dir_all(&env.state_dir).ok();
    }
}
