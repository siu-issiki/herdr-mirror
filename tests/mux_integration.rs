// Integration tests for the local-side mux (`herdr-mirror mux`).
//
// The `mux` subcommand exists only for these tests: it runs one host's mux with
// a `--spawn-cmd` override so the transport is a local child (the real agent
// binary, or a stub script) instead of ssh — no network, no self-deploy.
//
// Covered:
//   1. exec round-trips through the mux to a real agent and back,
//   2. a client disconnect closes the terminal it opened (agent child dies),
//   3. the mux re-spawns the transport after it dies (reconnect loop).

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::process::{Child, Command};
use tokio::time::{sleep, timeout};

const BIN: &str = env!("CARGO_BIN_EXE_herdr-mirror");

fn unique_dir(tag: &str) -> PathBuf {
    // Deliberately under /tmp with a short name: a unix socket path must fit in
    // ~104 bytes on macOS, and the default temp_dir there is already long.
    let dir = PathBuf::from("/tmp").join(format!(
        "hmux-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() % 1_000_000
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Spawn `herdr-mirror mux` with a JSON `--spawn-cmd` transport override.
fn spawn_mux(state_dir: &Path, host: &str, spawn_cmd: &[&str], extra: &[(&str, &str)]) -> Child {
    let json = serde_json::to_string(spawn_cmd).unwrap();
    let mut cmd = Command::new(BIN);
    cmd.args(["mux", "--state-dir"])
        .arg(state_dir)
        .args(["--host", host, "--spawn-cmd", &json])
        // exec/api never reach a real socket in these tests
        .env("HERDR_SOCKET_PATH", "/nonexistent/herdr.sock")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    for (k, v) in extra {
        cmd.env(k, v);
    }
    cmd.spawn().expect("spawn mux")
}

/// Connect to the mux socket, retrying until it is bound (or a deadline passes).
async fn connect_mux(sock: &Path) -> UnixStream {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(s) = UnixStream::connect(sock).await {
            return s;
        }
        assert!(std::time::Instant::now() < deadline, "mux socket never came up: {}", sock.display());
        sleep(Duration::from_millis(50)).await;
    }
}

fn write_script(dir: &Path, name: &str, body: &str) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let path = dir.join(name);
    std::fs::write(&path, body).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    path
}

#[tokio::test]
async fn exec_round_trips_through_the_mux() {
    let dir = unique_dir("exec");
    let sock = dir.join("h-mux.sock");
    let mut mux = spawn_mux(&dir, "h", &[BIN, "agent"], &[]);

    let stream = connect_mux(&sock).await;
    let (read, mut write) = stream.into_split();
    let mut lines = BufReader::new(read).lines();

    write
        .write_all(b"{\"op\":\"exec\",\"sid\":7,\"cmd\":\"echo hello-mux\"}\n")
        .await
        .unwrap();
    write.flush().await.unwrap();

    // read until the exec_res for our sid arrives
    let res = timeout(Duration::from_secs(10), async {
        loop {
            let line = lines.next_line().await.unwrap().expect("stream closed");
            let v: serde_json::Value = serde_json::from_str(&line).unwrap();
            if v["op"] == "exec_res" {
                return v;
            }
        }
    })
    .await
    .expect("exec_res timeout");

    assert_eq!(res["sid"], 7, "sid is rewritten back to the client's sid");
    assert_eq!(res["code"], 0);
    assert_eq!(res["out"].as_str().unwrap().trim(), "hello-mux");

    let _ = mux.kill().await;
}

#[tokio::test]
async fn client_disconnect_closes_its_terminal() {
    let dir = unique_dir("close");
    let sock = dir.join("h-mux.sock");
    let marker = dir.join("marker");

    // fake `herdr` whose `terminal session …` child stays alive until SIGTERM,
    // recording its lifecycle to the marker file so the test can observe it.
    let fake = write_script(
        &dir,
        "fake-herdr",
        &format!(
            "#!/bin/sh\n\
             echo started >> {m}\n\
             trap 'echo terminated >> {m}; exit 0' TERM\n\
             while true; do sleep 0.1; done\n",
            m = marker.display()
        ),
    );

    let mut mux = spawn_mux(&dir, "h", &[BIN, "agent", "--herdr-bin", fake.to_str().unwrap()], &[]);
    let stream = connect_mux(&sock).await;
    let (_read, mut write) = stream.into_split();

    // open a terminal; the agent spawns the fake child
    write
        .write_all(b"{\"op\":\"open\",\"sid\":1,\"pane\":\"p0\",\"mode\":\"observe\",\"cols\":80,\"rows\":24}\n")
        .await
        .unwrap();
    write.flush().await.unwrap();

    // wait for the child to come up
    wait_for_marker(&marker, "started").await;

    // disconnect: the mux must close the sid on the agent, which SIGTERMs the child
    drop(write);
    drop(_read);

    wait_for_marker(&marker, "terminated").await;

    let _ = mux.kill().await;
}

#[tokio::test]
async fn mux_respawns_the_transport_after_it_dies() {
    let dir = unique_dir("respawn");
    let counter = dir.join("spawns");

    // A transport that greets correctly (matching version → no deploy path) then
    // exits, so the mux must reconnect. Each spawn appends to the counter file.
    let stub = write_script(
        &dir,
        "stub-agent",
        &format!(
            "#!/bin/sh\n\
             echo run >> {c}\n\
             printf '{{\"op\":\"hello\",\"version\":\"{v}\",\"herdr_socket\":\"/x\"}}\\n'\n\
             sleep 0.2\n",
            c = counter.display(),
            v = env!("CARGO_PKG_VERSION"),
        ),
    );

    let mut mux = spawn_mux(&dir, "h", &[stub.to_str().unwrap()], &[]);

    // First reconnect backoff is 1s; each cycle is ~1.3s. Allow room for >=2.
    let ok = timeout(Duration::from_secs(6), async {
        loop {
            if let Ok(text) = std::fs::read_to_string(&counter) {
                if text.lines().count() >= 2 {
                    return;
                }
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    assert!(ok.is_ok(), "mux did not re-spawn the transport after it died");

    let _ = mux.kill().await;
}

/// Poll `marker` until it contains `needle`, failing after a deadline.
async fn wait_for_marker(marker: &Path, needle: &str) {
    let ok = timeout(Duration::from_secs(8), async {
        loop {
            if let Ok(text) = std::fs::read_to_string(marker) {
                if text.contains(needle) {
                    return;
                }
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(ok.is_ok(), "marker never contained {needle:?} ({})", marker.display());
}
