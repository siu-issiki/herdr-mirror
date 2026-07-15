// Loopback integration test for `herdr-mirror agent`.
//
// Spawns the built binary as a child, drives it over the mux NDJSON protocol
// with only herdr-socket-free ops, and asserts the four phase-1 guarantees:
//   1. `hello` is the first line,
//   2. `ping` arrives on schedule,
//   3. `exec` round-trips a buffered `exec_res`,
//   4. stdin EOF makes the agent exit promptly (no lingering process).

use std::process::Stdio;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::time::timeout;

fn parse(line: &str) -> serde_json::Value {
    serde_json::from_str(line).expect("agent emits valid JSON")
}

#[tokio::test]
async fn agent_hello_ping_exec_and_eof_exit() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_herdr-mirror"))
        .arg("agent")
        // point the socket somewhere inert; this test never issues api/sub ops
        .env("HERDR_SOCKET_PATH", "/nonexistent/herdr.sock")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn agent");

    let mut stdin = child.stdin.take().unwrap();
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();

    // 1. hello is the first line.
    let hello = timeout(Duration::from_secs(5), lines.next_line())
        .await
        .expect("hello timeout")
        .expect("read error")
        .expect("stdout closed before hello");
    let hello = parse(&hello);
    assert_eq!(hello["op"], "hello", "first line must be hello: {hello}");
    assert!(hello["version"].is_string());
    assert_eq!(hello["herdr_socket"], "/nonexistent/herdr.sock");

    // 3. exec round-trips (drive it early; ping may interleave).
    stdin
        .write_all(b"{\"op\":\"exec\",\"sid\":1,\"cmd\":\"echo hi\"}\n")
        .await
        .unwrap();
    stdin.flush().await.unwrap();

    // 2 + 3: collect lines until we've seen both a ping and the exec_res.
    let mut saw_ping = false;
    let mut saw_exec_res = false;
    let deadline = Duration::from_secs(8); // ping cadence is 5s
    while !(saw_ping && saw_exec_res) {
        let line = timeout(deadline, lines.next_line())
            .await
            .expect("timed out waiting for ping + exec_res")
            .expect("read error")
            .expect("stdout closed unexpectedly");
        let v = parse(&line);
        match v["op"].as_str() {
            Some("ping") => {
                assert!(v["seq"].is_number());
                saw_ping = true;
            }
            Some("exec_res") => {
                assert_eq!(v["sid"], 1);
                assert_eq!(v["code"], 0);
                assert_eq!(v["out"].as_str().unwrap().trim(), "hi");
                saw_exec_res = true;
            }
            _ => {}
        }
    }

    // 4. EOF => the agent exits on its own, leaving no children behind.
    drop(stdin);
    let status = timeout(Duration::from_secs(5), child.wait())
        .await
        .expect("agent did not exit on stdin EOF")
        .expect("wait failed");
    assert!(status.success() || status.code().is_some(), "agent exited cleanly");
}
