# Single-connection mux architecture

## Problem

The current data plane opens one ssh channel + one remote `herdr terminal
session` CLI process **per mirrored pane**, plus a ControlMaster, an API
socket forward, and transient exec channels. Failure modes observed in
production (2026-07-14): sshd MaxSessions exhaustion, reconnect storms
tripping MaxStartups, orphaned remote CLIs holding pane attach slots,
process leaks on both ends (141 remote / 57 local).

## Design

Exactly one long-lived ssh connection per host, running one remote agent
process that multiplexes everything:

```
local                                      remote
─────                                      ──────
daemon ── mux task ══ ssh (single) ══ herdr-mirror agent
             │                             ├─ local herdr socket (api / events)
 wrappers ───┤ <state_dir>/<host>-mux.sock └─ N child `herdr terminal session ...`
 actions  ───┘                                 (local processes, agent-owned)
```

- The **agent** (new subcommand, runs on the remote host) owns terminal
  session CLIs as direct children (local pipes — no ssh channels), talks to
  the remote herdr socket for api/events, and speaks an NDJSON mux protocol
  on stdio.
- The **mux task** (inside the daemon) owns the single ssh child, exposes a
  local unix socket; pane wrappers and remote-action CLIs connect there.
- Terminal children are agent-owned: agent exit or `close` reaps them.
  Ghost attaches and per-pane channel limits disappear structurally.

## Mux protocol (NDJSON, both directions)

Envelope: `{"sid": <u64>, "op": "...", ...}`. sids are assigned by the
local side; agent replies use the same sid.

client → agent:
- `{"sid",op:"open", pane, mode:"observe"|"control", cols, rows, takeover, session?}`
  → agent spawns `herdr [--session s] terminal session <mode> '<pane>' --cols C --rows R [--takeover]`
- `{"sid",op:"input", d:"<raw json line for the child stdin>"}`
- `{"sid",op:"close"}` → SIGTERM the child
- `{"sid",op:"api", method, params}` → one-shot request on the remote herdr socket
- `{"sid",op:"sub", subs:[...]}` → held events.subscribe; previous sub sid is replaced
- `{"sid",op:"exec", cmd}` → `sh -c cmd`, buffered result

agent → client:
- `{op:"hello", version, herdr_socket}` — first line after start
- `{"sid",op:"l", d:"<raw child stdout line>"}` — terminal frames, verbatim
- `{"sid",op:"exit", code}` — terminal child exited
- `{"sid",op:"api_res", result?|error?}`
- `{"sid",op:"ev", d:"<raw event line>"}`
- `{"sid",op:"exec_res", code, out}`
- `{op:"ping", seq}` every 5s unconditionally

Raw child/event lines are embedded as JSON strings (`d`); their payloads
are dominated by base64, which needs no escaping, so overhead is minimal.

## Liveness

The local mux treats >20s without any agent line (pings included) as a dead
link: kill ssh, reconnect with backoff, re-`open` every registered sid, and
notify local clients (`{"op":"closed",reason:"mux reconnecting"}`) so
wrappers show status and wait. One reconnect path for everything.

## Local socket protocol

Same envelopes, minus ssh concerns. Each local client connection owns the
sids it created; disconnect of the client closes its sids (agent-side
children included). The mux maps (client, client-sid) → global sid.

## Binary deployment

On `hello` version mismatch or "command not found", the daemon redeploys
itself: `cat <local binary> | ssh <host> 'mkdir -p ~/.local/libexec && cat > ~/.local/libexec/herdr-mirror.new && chmod +x ... && mv ...'`
then restarts the agent. Same-arch hosts only for now (config
`agent_bin` overrides the remote path).

## Fallbacks

- remote-action CLIs try `<host>-mux.sock` first, then the legacy
  ControlMaster+forward path (kept in remote.rs).
- The legacy per-pane ssh transport remains in git history for rollback.

## Migration phases

1. protocol module + agent subcommand (+ unit tests, loopback test)
2. mux task in the daemon + local socket server + liveness + deploy
3. wrapper transport swap (ssh child → mux.sock client); UI logic untouched
4. daemon data plane (api/events/exec via mux); drop forward for daemon use
5. e2e verification, latency re-measurement, failover tests
