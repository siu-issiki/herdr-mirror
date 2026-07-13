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

use serde::Deserialize;
use serde_json::{json, Value};

use crate::config::{load_config, HostConfig};
use crate::remote::RemoteHost;
use crate::state::load_state;
use crate::util::{err, Env, Result};

#[derive(Debug, Default, Deserialize)]
struct InvocationContext {
    workspace_id: Option<String>,
    focused_pane_id: Option<String>,
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

    let ctx: InvocationContext = std::env::var("HERDR_PLUGIN_CONTEXT_JSON")
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default();
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

    let mut remote = RemoteHost::new(&host, &env.state_dir);
    let api = remote.connect_api_fast().await?;

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

    match kind {
        "workspace" => {
            let res: Value = api.request("workspace.create", json!({ "cwd": cwd, "focus": false })).await?;
            println!(
                "created workspace {} ({}) on {}; mirror follows shortly",
                res.pointer("/workspace/label").and_then(|v| v.as_str()).unwrap_or("?"),
                res.pointer("/workspace/workspace_id").and_then(|v| v.as_str()).unwrap_or("?"),
                host.name
            );
        }
        "tab" => {
            let ws = resolved.as_ref().and_then(|r| r.remote_ws_id.clone()).unwrap();
            let res: Value = api
                .request("tab.create", json!({ "workspace_id": ws, "cwd": cwd, "focus": false }))
                .await?;
            println!(
                "created tab {} in {}: {ws}; mirror follows shortly",
                res.pointer("/tab/tab_id").and_then(|v| v.as_str()).unwrap_or("?"),
                host.name
            );
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
            println!(
                "split {pane_id} {dir} on {} → {}; mirror follows shortly",
                host.name,
                res.pointer("/pane/pane_id").and_then(|v| v.as_str()).unwrap_or("ok")
            );
        }
        _ => return Err(err(format!("unknown remote action: {kind}"))),
    }
    Ok(())
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
}
