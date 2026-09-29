# pi-ntfy-listener

A [Pi](https://pi.dev/) extension that turns ntfy into an interactive remote
control for your running sessions. Each **project** (session working
directory) is bound to its own ntfy topic; messages you post from your phone
arrive in the session as real user turns, the agent executes them, and its
replies are published back to the same topic on your phone.

```text
phone (ntfy app)  ──▶  ntfy server  ──▶  pi extension  ──▶  agent turn
phone  ◀────────────  ntfy server  ◀────── agent reply  ◀─── pi session
```

## Features

- **Per-project topics** — topic resolution order:
  1. `<project>/.pi-ntfy-topic` file (explicit override)
  2. `NTFY_TOPIC` environment variable
  3. Derived: `pi-<dirname>-<sha1(cwd)[0..10]>` — unique and unguessable per project
- **Loop protection** — self posts ("Pi (…)" / "Claude (…)") are ignored
- **Single-owner arbitration** — when multiple pi sessions run in the same
  project (all bound to the same topic), only ONE session streams the topic
  and delivers messages. Ownership runs through a per-topic lock file with
  pid + 15s heartbeat: dead or stale owners are taken over automatically,
  losing sessions fall back to standby. Without this, every open session in
  the project wakes on every message — multiplying provider requests (and
  burning through rate limits, since long sessions re-send their whole
  context every turn).
- **Burst coalescing** — messages arriving within a quiet window (default
  5s, capped at 45s per batch) are joined into one user message, so a burst
  of pushes starts one agent turn, not one per message.
- **Context guard + hold** — deliveries are checked against the session's
  context usage. Above a threshold (default 60% of the context window),
  messages are held in `~/.pi/agent/ntfy/<hash>.hold.json` (the cursor has
  already advanced, so nothing is ever replayed) and the user is notified;
  held messages flush on the next `agent_end` once usage is back under the
  threshold, or via `/ntfy-flush`. A bloated session is never silently
  pushed into a new, expensive turn.
- **Crash/disconnect self-healing** — the stream restarts with exponential backoff and is re-bound on session start, reload, or project switch
- **Timestamp cursors** — resume where you left off per project, stored under `~/.pi/agent/ntfy/<hash>.since` (unix seconds, never message ids — load-balanced ntfy servers reject ids from other instances). Cursors are stored one second past the last delivered message because ntfy's `since=` is inclusive, so a restart or new session never replays it.
- **Commands**: `/ntfy-start`, `/ntfy-stop`, `/ntfy-status`, `/ntfy-topic`, `/ntfy-flush`

## Install

```bash
pi install npm:pi-ntfy-listener
```

Or from the git mirror:

```bash
pi install git:github.com/MoemenMostafa/pi-ntfy-listener
```

The listener starts automatically with every session (and `pi -e` ad-hoc use
behaves the same). Use `/ntfy-topic` to see the topic for the current project,
then subscribe to it in the ntfy app on your phone.

## Configuration

Defaults work against `https://ntfy.sh` with a direct connection. Two config
layers exist:

### Environment variables

| Variable | Purpose |
|---|---|
| `NTFY_TOPIC` | Pin a topic explicitly (highest priority after the project file) |
| `NTFY_BASE_URL` | Use a self-hosted ntfy server instead of ntfy.sh |
| `NTFY_AUTH_TOKEN` | Bearer token, required if the server demands auth |
| `NTFY_CONFIG_FILE` | Alternative path for the config file (default `~/.pi/agent/ntfy/config.json`) |
| `NTFY_STATE_DIR` | Alternative state directory |
| `NTFY_TUNNEL_*` | Opt-in tunnel settings (see below) |
| `NTFY_COALESCE_MS` | Quiet window for joining burst messages (default `5000`; `0` disables) |
| `NTFY_COALESCE_MAX_MS` | Hard cap on how long a queued message waits (default `45000`) |
| `NTFY_CONTEXT_GUARD_PERCENT` | Hold deliveries when context usage exceeds this percent (default `60`; `0` disables) |

### Config file — `~/.pi/agent/ntfy/config.json`

```json
{
  "baseUrl": "https://ntfy.sh",
  "authToken": "tk_yourtoken",
  "coalesceMs": 5000,
  "coalesceMaxMs": 45000,
  "contextGuardPercent": 60
}
```

### Self-hosted or restrictive networks — optional SSH tunnel

If your LAN blocks the ntfy server over TCP, the extension can route the
stream through an SSH tunnel it manages itself (TLS still ends at the ntfy
server, so SNI and certificate verification stay intact):

```json
{
  "tunnel": {
    "listenPort": 2583,
    "remote": "user@yourserver",
    "target": "ntfy.sh:443",
    "sshKey": "~/.ssh/id_ed25519",
    "sshArgs": ["-o", "ExitOnForwardFailure=yes"]
  }
}
```

- `remote` — ssh destination the local forward binds against
- `target` — `host:port` the remote end forwards to
- `sshKey` — optional private key; `sshArgs` — extra flags passed through

Equivalent env vars: `NTFY_TUNNEL_REMOTE`, `NTFY_TUNNEL_TARGET`,
`NTFY_TUNNEL_LISTEN_PORT`, `NTFY_TUNNEL_SSH_KEY`. Without these, the listener
connects directly to the ntfy server.

## Commands

- `/ntfy-start` — start (or restart) the listener for this project
- `/ntfy-stop` — stop the listener (releases ownership so a standby takes over)
- `/ntfy-status` — binding, topic, cursor, connection mode, owner, held count, tunables
- `/ntfy-topic [topic]` — show (or override with an argument) the topic for this project; writing the override restarts the listener on the new topic
- `/ntfy-flush [force]` — deliver messages held by the context guard; `force` bypasses the guard

## Messages

Anything you publish to the topic becomes a user turn for that project, e.g.
"run the tests and report failures". The agent executes and publishes its
reply with the topic title `Pi (<project slug>)` — those titles are ignored by
the listener, so replies never loop.

## State layout

```
~/.pi/agent/ntfy/
├── config.json              optional server/tunnel/auth/coalescing/guard config
├── <sha1(cwd)[0..10]>.since    per-project cursor (unix seconds)
├── <sha1(cwd)[0..10]>.log      per-project delivery log
├── <sha1(cwd)[0..10]>.owner    per-topic owner lock { pid, cwd, ts heartbeat }
└── <sha1(cwd)[0..10]>.hold.json  messages held by the context guard
```

Per-project files are also re-bindable mid-session: switching the session to a
different project directory automatically switches the stream to that
project's topic.

## License

MIT