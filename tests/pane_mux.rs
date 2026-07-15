// Integration test for the pane wrapper's mux transport (`--mux-sock`).
//
// Wires the real pieces end to end with no network: a `herdr-mirror mux` with a
// `--spawn-cmd` override runs a local `herdr-mirror agent`, and the agent spawns
// a fake `herdr` that emits one terminal.frame. The wrapper is launched in
// `--dump` mode against the mux socket; the test asserts the frame travels
//
//   wrapper --open--> mux --> agent --> fake herdr --frame--> agent --l--> mux
//           --l--> wrapper --dump-->
//
// i.e. the wrapper opened over mux (not ssh) and rendered a frame it received
// through the mux `l` op. Also covers the immediate-exit path (no fake herdr →
// the agent child fails to spawn → `exit` reaches the wrapper) by checking the
// wrapper stays alive and retries rather than crashing.

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::time::{sleep, timeout};

const BIN: &str = env!("CARGO_BIN_EXE_herdr-mirror");

fn unique_dir(tag: &str) -> PathBuf {
    // short /tmp path: unix socket paths must fit in ~104 bytes on macOS.
    let dir = PathBuf::from("/tmp").join(format!(
        "hpane-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() % 1_000_000
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn write_script(dir: &Path, name: &str, body: &str) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, body).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    path
}

fn spawn_mux(state_dir: &Path, host: &str, spawn_cmd: &[&str]) -> Child {
    let json = serde_json::to_string(spawn_cmd).unwrap();
    Command::new(BIN)
        .args(["mux", "--state-dir"])
        .arg(state_dir)
        .args(["--host", host, "--spawn-cmd", &json])
        // the agent's api/exec never reach a real herdr socket here
        .env("HERDR_SOCKET_PATH", "/nonexistent/herdr.sock")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn mux")
}

async fn wait_for_socket(sock: &Path) {
    let ok = timeout(Duration::from_secs(10), async {
        while tokio::net::UnixStream::connect(sock).await.is_err() {
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(ok.is_ok(), "mux socket never came up: {}", sock.display());
}

#[tokio::test]
async fn wrapper_opens_over_mux_and_renders_a_frame() {
    let dir = unique_dir("frame");
    let sock = dir.join("h-mux.sock");

    // fake herdr: ignore the `terminal session …` argv, emit one terminal.frame
    // (base64 "hi"), then stay alive so no exit races the read.
    let fake = write_script(
        &dir,
        "fake-herdr",
        "#!/bin/sh\n\
         printf '{\"type\":\"terminal.frame\",\"seq\":1,\"full\":true,\"width\":4,\"height\":1,\"bytes\":\"aGk=\"}\\n'\n\
         while true; do sleep 0.2; done\n",
    );

    let mut mux = spawn_mux(&dir, "h", &[BIN, "agent", "--herdr-bin", fake.to_str().unwrap()]);
    wait_for_socket(&sock).await;

    // launch the wrapper over the mux socket in headless dump mode
    let mut wrapper = Command::new(BIN)
        .args(["pane", "dummy-target", "p0", "--dump", "--mux-sock"])
        .arg(&sock)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn wrapper");

    let stdout = wrapper.stdout.take().unwrap();
    let mut lines = BufReader::new(stdout).lines();

    // the decoded frame text ("hi") must appear in the dump output
    let saw_frame = timeout(Duration::from_secs(10), async {
        while let Ok(Some(line)) = lines.next_line().await {
            if line.contains("hi") || line.contains("terminal.frame") {
                return true;
            }
        }
        false
    })
    .await
    .expect("timed out waiting for a frame in the wrapper's dump output");
    assert!(saw_frame, "wrapper never rendered a frame received over the mux");

    let _ = wrapper.kill().await;
    let _ = mux.kill().await;
}

#[tokio::test]
async fn wrapper_survives_agent_child_exit_over_mux() {
    let dir = unique_dir("exit");
    let sock = dir.join("h-mux.sock");

    // no fake herdr: the agent tries to spawn a missing binary, which fails to
    // spawn → the agent emits `exit` immediately for the sid.
    let mut mux =
        spawn_mux(&dir, "h", &[BIN, "agent", "--herdr-bin", "/nonexistent/herdr-binary"]);
    wait_for_socket(&sock).await;

    let mut wrapper = Command::new(BIN)
        .args(["pane", "dummy-target", "p0", "--dump", "--mux-sock"])
        .arg(&sock)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn wrapper");

    // the open→exit round trip drives handle_exit → schedule_reconnect (backoff);
    // the wrapper must keep running rather than crash on the exit it receives.
    sleep(Duration::from_secs(2)).await;
    assert!(wrapper.try_wait().unwrap().is_none(), "wrapper exited instead of scheduling a reconnect");

    let _ = wrapper.kill().await;
    let _ = mux.kill().await;
}
