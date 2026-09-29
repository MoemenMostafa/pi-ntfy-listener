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
- **Crash/disconnect self-healing** — the stream restarts with exponential backoff and is re-bound on session start, reload, or project switch
- **Timestamp cursors** — resume where you left off per project, stored under `~/.pi/agent/ntfy/<hash>.since` (unix seconds, never message ids — load-balanced ntfy servers reject ids from other instances). Cursors are stored one second past the last delivered message because ntfy's `since=` is inclusive, so a restart or new session never replays it.
- **Commands**: `/ntfy-start`, `/ntfy-stop`, `/ntfy-status`, `/ntfy-topic`

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

### Config file — `~/.pi/agent/ntfy/config.json`

```json
{
  "baseUrl": "https://ntfy.sh",
  "authToken": "tk_yourtoken"
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
- `/ntfy-stop` — stop the listener
- `/ntfy-status` — binding, topic, cursor, connection mode
- `/ntfy-topic [topic]` — show (or override with an argument) the topic for this project; writing the override restarts the listener on the new topic

## Messages

Anything you publish to the topic becomes a user turn for that project, e.g.
"run the tests and report failures". The agent executes and publishes its
reply with the topic title `Pi (<project slug>)` — those titles are ignored by
the listener, so replies never loop.

## State layout

```
~/.pi/agent/ntfy/
├── config.json            optional server/tunnel/auth config
├── <sha1(cwd)[0..10]>.since  per-project cursor (unix seconds)
└── <sha1(cwd)[0..10]>.log    per-project delivery log
```

Per-project files are also re-bindable mid-session: switching the session to a
different project directory automatically switches the stream to that
project's topic.

## License

MIT