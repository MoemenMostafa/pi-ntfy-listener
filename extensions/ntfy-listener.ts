/**
 * pi-ntfy-listener — Pi extension (per-project bound)
 *
 * Streams the ntfy topic bound to the CURRENT PROJECT (the session working
 * directory) and delivers every message that is not from ourselves into the
 * session as a real user message (pi.sendUserMessage), so ntfy is interactive
 * with the running session: a sent message starts (or queues as followUp) a
 * turn, the agent executes it, and replies on the channel.
 *
 * Project binding (resolution order):
 *   1. <project>/.pi-ntfy-topic      — explicit per-project override file
 *   2. NTFY_TOPIC env                — explicit per-process override
 *   3. pi-<slug>-<hash10>            — derived from the project path
 *      (slug = dirname lowercased/normalized, hash10 = first 10 hex of
 *      sha1(cwd)). Every project gets its own private topic; a session in
 *      project A never receives traffic for project B.
 *
 * Server + connection (resolution order):
 *   1. NTFY_BASE_URL env             — base URL of the ntfy server
 *   2. ~/.pi/agent/ntfy/config.json  — { baseUrl?, authToken?, tunnel? }
 *   3. https://ntfy.sh (default)
 *
 * Direct connection is used unless a tunnel is configured. On restrictive
 * networks where the ntfy server is TCP-blocked, you can opt into routing
 * through a local SSH tunnel that the extension spawns itself:
 *
 *   ~/.pi/agent/ntfy/config.json
 *   {
 *     "baseUrl": "https://ntfy.sh",
 *     "tunnel": {
 *       "listenPort": 2583,        // local port to route through
 *       "remote": "user@host",     // SSH target the tunnel binds on
 *       "target": "ntfy.sh:443",   // host:port the remote side forwards to
 *       "sshKey": "~/.ssh/id_ed25519",
 *       "sshArgs": ["-o", "ExitOnForwardFailure=yes"]  // optional extras
 *     }
 *   }
 *
 * Equivalent env vars: NTFY_TUNNEL_LISTEN_PORT, NTFY_TUNNEL_REMOTE,
 * NTFY_TUNNEL_TARGET, NTFY_TUNNEL_SSH_KEY, NTFY_AUTH_TOKEN,
 * NTFY_CONFIG_FILE. With a tunnel configured, TLS still terminates at the
 * ntfy server (SNI/hostname verification keep the real server name).
 *
 * Self posts (titled "Pi (…)" or "Claude (…)") are ignored so agent replies
 * never loop back into the session.
 *
 * State is per project:
 *   - Cursor: ~/.pi/agent/ntfy/<hash10>.since (UNIX timestamp seconds, NEVER
 *     a ntfy message id — ntfy load balancers route requests to different
 *     server instances and since=<id> only resolves on the instance holding
 *     that message, intermittently answering 400 "invalid since parameter").
 *     ntfy's since=<ts> is INCLUSIVE, so the cursor is stored as
 *     <message ts> + 1 — otherwise the last delivered message replays into
 *     every restart / new session.
 *   - Log:    ~/.pi/agent/ntfy/<hash10>.log
 *
 * Delivery arbitration (single owner):
 *   Multiple pi sessions in the SAME project all bind to the same topic —
 *   without arbitration each of them independently wakes on every message,
 *   multiplying provider requests (and burning through rate limits on long
 *   sessions whose context is re-sent every turn). A per-topic owner lock
 *   (~/.pi/agent/ntfy/<hash10>.owner, JSON { pid, cwd, ts }) elects exactly
 *   one streaming session per topic:
 *     - Claimed with O_EXCL; an existing lock owned by a dead pid or with a
 *       heartbeat older than ~2 intervals is taken over.
 *     - The owner heartbeats every 15s (interval + agent_end) and releases
 *       the lock on stop/shutdown.
 *     - Non-owner sessions stand by (no stream, no deliveries) and poll for
 *       ownership — they pick up seamlessly when the owner dies.
 *
 * Burst coalescing:
 *   Messages arriving within a quiet window (~5s, capped at ~45s per batch)
 *   are joined into ONE user message instead of one turn each.
 *
 * Context guard + hold:
 *   Before delivering, the session's context usage is checked. When above a
 *   threshold (default 60% of the context window), messages are HELD in
 *   ~/.pi/agent/ntfy/<hash10>.hold.json (cursor already advanced, so they
 *   survive crashes) and the user is notified; the hold flushes on the next
 *   agent_end once usage is back under the threshold, or via /ntfy-flush.
 *   No surprises: a bloated session is never pushed a new turn silently.
 *
 * Commands: /ntfy-start, /ntfy-stop, /ntfy-status, /ntfy-topic, /ntfy-flush.
 *
 * Tunables (env override > config.json > default):
 *   NTFY_COALESCE_MS            / coalesceMs            (5000)
 *   NTFY_COALESCE_MAX_MS        / coalesceMaxMs         (45000)
 *   NTFY_CONTEXT_GUARD_PERCENT  / contextGuardPercent   (60)
 */

import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import * as https from "node:https";
import * as net from "node:net";
import { spawn } from "node:child_process";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import * as crypto from "node:crypto";

const HOME = os.homedir();
const STATE_DIR = process.env.NTFY_STATE_DIR
	?? path.join(HOME, ".pi", "agent", "ntfy");
const CONFIG_FILE =
	process.env.NTFY_CONFIG_FILE ?? path.join(STATE_DIR, "config.json");
const DEFAULT_TUNNEL_LISTEN_PORT = 2583;
const STREAM_TIMEOUT_MS = 86_400_000;

interface NtfyEvent {
	event?: string;
	title?: string;
	message?: string;
	id?: string;
	time?: number;
}

interface TunnelConfig {
	listenPort?: number;
	remote?: string;
	target?: string;
	sshKey?: string;
	sshArgs?: string[];
}

interface NtfyConfig {
	baseUrl?: string;
	authToken?: string;
	tunnel?: TunnelConfig;
	/** Quiet window for joining burst messages into one delivery. */
	coalesceMs?: number;
	/** Hard cap on how long the first queued message may wait. */
	coalesceMaxMs?: number;
	/** Hold deliveries when context usage percent exceeds this (0 disables). */
	contextGuardPercent?: number;
}

interface OwnerLock {
	pid: number;
	cwd: string;
	ts: number;
}

interface ResolvedConnection {
	/** Normalized base URL, e.g. "https://ntfy.sh" (no trailing slash). */
	baseUrl: string;
	/** Host the TLS stream talks to (server name / SNI). */
	serverName: string;
	/** Where the TCP connection goes. */
	host: string;
	port: number;
	/** Optional bearer token for Authorization. */
	authToken?: string;
	/** True when routed through a locally spawned SSH tunnel. */
	viaTunnel: boolean;
}

interface ProjectBinding {
	/** Resolved working directory (absolute, symlinks resolved). */
	cwd: string;
	/** Short display slug from the directory basename. */
	slug: string;
	/** Stable per-project key: first 10 hex chars of sha1(cwd). */
	key: string;
	/** Resolved ntfy topic for this project. */
	topic: string;
	/** Where the topic came from: project file, env, or derived. */
	source: "project-file" | "env" | "derived";
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

function readConfigFile(): NtfyConfig {
	try {
		const raw: unknown = JSON.parse(fs.readFileSync(CONFIG_FILE, "utf8"));
		return raw && typeof raw === "object" ? (raw as NtfyConfig) : {};
	} catch {
		return {};
	}
}

function expandHome(p: string): string {
	return p.startsWith("~") ? path.join(HOME, p.slice(1)) : p;
}

function resolveConnection(): ResolvedConnection {
	const cfg = readConfigFile();
	const baseUrl = (
		process.env.NTFY_BASE_URL ??
		cfg.baseUrl ??
		"https://ntfy.sh"
	).replace(/\/+$/, "");
	const authToken = process.env.NTFY_AUTH_TOKEN ?? cfg.authToken;
	const url = new URL(baseUrl);
	const serverName = url.hostname;
	const serverPort = url.port ? Number(url.port) : url.protocol === "http:" ? 80 : 443;

	// Tunnel is opt-in: it needs at least an SSH remote or target.
	const envRemote = process.env.NTFY_TUNNEL_REMOTE;
	const envTarget = process.env.NTFY_TUNNEL_TARGET;
	const listenPortEnv = process.env.NTFY_TUNNEL_LISTEN_PORT;
	const listenPort = Number(listenPortEnv ?? cfg.tunnel?.listenPort) || DEFAULT_TUNNEL_LISTEN_PORT;
	const tunnel: TunnelConfig | undefined = {
		listenPort,
		remote: envRemote ?? cfg.tunnel?.remote,
		target: envTarget ?? cfg.tunnel?.target,
		sshKey: process.env.NTFY_TUNNEL_SSH_KEY
			? expandHome(process.env.NTFY_TUNNEL_SSH_KEY)
			: cfg.tunnel?.sshKey
				? expandHome(cfg.tunnel.sshKey)
				: undefined,
		sshArgs: cfg.tunnel?.sshArgs,
	};
	if (tunnel.remote && tunnel.target) {
		return {
			baseUrl,
			serverName,
			host: "127.0.0.1",
			port: tunnel.listenPort ?? DEFAULT_TUNNEL_LISTEN_PORT,
			authToken,
			viaTunnel: true,
		};
	}
	return {
		baseUrl,
		serverName,
		host: serverName,
		port: serverPort,
		authToken,
		viaTunnel: false,
	};
}

// ---------------------------------------------------------------------------
// Project binding
// ---------------------------------------------------------------------------

function log(binding: ProjectBinding | undefined, line: string): void {
	try {
		const file = binding
			? path.join(STATE_DIR, `${binding.key}.log`)
			: path.join(STATE_DIR, "unbound.log");
		fs.mkdirSync(path.dirname(file), { recursive: true });
		const tag = binding ? `${binding.slug} (${binding.topic})` : "unbound";
		fs.appendFileSync(file, `[${new Date().toISOString()}] [${tag}] ${line}\n`);
	} catch {
		/* logging must never break the listener */
	}
}

function normalizeCwd(cwd: string): string {
	let p = path.resolve(cwd);
	try {
		p = fs.realpathSync(p);
	} catch {
		/* keep resolve() result if realpath fails */
	}
	// strip trailing separator so /repo/ and /repo hash identically
	return p.replace(/[\\/]+$/, "");
}

function projectSlug(cwd: string): string {
	const base = path.basename(cwd)
		.toLowerCase()
		.replace(/[^a-z0-9]+/g, "-")
		.replace(/^-+|-+$/g, "");
	return base || "project";
}

function projectKey(cwd: string): string {
	return crypto.createHash("sha1").update(cwd).digest("hex").slice(0, 10);
}

/** Explicit per-project override: <project>/.pi-ntfy-topic */
function projectTopicFile(cwd: string): string {
	return path.join(cwd, ".pi-ntfy-topic");
}

function readTopicFile(file: string): string | undefined {
	try {
		const t = fs.readFileSync(file, "utf8").trim();
		return t || undefined;
	} catch {
		return undefined;
	}
}

/**
 * Bind a project (cwd) to its ntfy topic:
 *   <cwd>/.pi-ntfy-topic  >  NTFY_TOPIC env  >  pi-<slug>-<key> (derived)
 */
function bindProject(cwdRaw: string): ProjectBinding {
	const cwd = normalizeCwd(cwdRaw);
	const slug = projectSlug(cwd);
	const key = projectKey(cwd);

	const fileTopic = readTopicFile(projectTopicFile(cwd));
	if (fileTopic) {
		return { cwd, slug, key, topic: fileTopic, source: "project-file" };
	}
	if (process.env.NTFY_TOPIC) {
		return { cwd, slug, key, topic: process.env.NTFY_TOPIC, source: "env" };
	}
	return { cwd, slug, key, topic: `pi-${slug}-${key}`, source: "derived" };
}

/** Per-project cursor: <state dir>/<key>.since */
function cursorFile(binding: ProjectBinding): string {
	return path.join(STATE_DIR, `${binding.key}.since`);
}

function readCursor(binding: ProjectBinding): string {
	try {
		return fs.readFileSync(cursorFile(binding), "utf8").trim() || "";
	} catch {
		return "";
	}
}

function writeCursor(binding: ProjectBinding, value: string): void {
	try {
		fs.mkdirSync(STATE_DIR, { recursive: true });
		fs.writeFileSync(cursorFile(binding), value);
	} catch {
		/* non-fatal */
	}
}

/** Per-topic owner lock: ~/.pi/agent/ntfy/<key>.owner */
function ownerFile(binding: ProjectBinding): string {
	return path.join(STATE_DIR, `${binding.key}.owner`);
}

/** Per-topic hold queue: ~/.pi/agent/ntfy/<key>.hold.json (array of messages). */
function holdFile(binding: ProjectBinding): string {
	return path.join(STATE_DIR, `${binding.key}.hold.json`);
}

// --- Runtime tunables (env > config.json > default) ------------------------

function runtimeTunables() {
	const cfg = readConfigFile();
	const coalesceMs = Number(
		process.env.NTFY_COALESCE_MS ?? cfg.coalesceMs,
	);
	const coalesceMaxMs = Number(
		process.env.NTFY_COALESCE_MAX_MS ?? cfg.coalesceMaxMs,
	);
	const guardRaw = Number(
		process.env.NTFY_CONTEXT_GUARD_PERCENT ?? cfg.contextGuardPercent,
	);
	const quiet = Number.isFinite(coalesceMs) && coalesceMs >= 0 ? coalesceMs : 5_000;
	const cap = Number.isFinite(coalesceMaxMs) && coalesceMaxMs >= quiet
		? coalesceMaxMs
		: Math.max(45_000, quiet);
	const guardPercent =
		Number.isFinite(guardRaw) && guardRaw >= 0 && guardRaw <= 100 ? guardRaw : 60;
	return { coalesceMs: quiet, coalesceMaxMs: cap, contextGuardPercent: guardPercent };
}

// --- Owner lock -------------------------------------------------------------

function isProcessAlive(pid: number): boolean {
	try {
		process.kill(pid, 0);
		return true;
	} catch (err) {
		return (err as NodeJS.ErrnoException).code === "EPERM";
	}
}

const OWNER_STALE_MS = 45_000; // ~2 missed 15s heartbeats
const HEARTBEAT_MS = 15_000;
const STANDBY_POLL_MS = 5_000;

function readOwnerLock(binding: ProjectBinding): OwnerLock | undefined {
	try {
		const raw = JSON.parse(fs.readFileSync(ownerFile(binding), "utf8"));
		if (raw && typeof raw.pid === "number" && typeof raw.ts === "number") {
			return { pid: raw.pid, cwd: String(raw.cwd ?? ""), ts: raw.ts };
		}
	} catch {
		/* missing/unreadable */
	}
	return undefined;
}

function writeOwnerLock(binding: ProjectBinding): void {
	const lock: OwnerLock = { pid: process.pid, cwd: binding.cwd, ts: Date.now() };
	fs.writeFileSync(ownerFile(binding), JSON.stringify(lock));
}

/**
 * Try to become the streaming owner for this topic. Returns true when this
 * process owns (or already owns) the lock. Dead or stale owners are taken
 * over; a live owner means we go on standby.
 */
function tryAcquireOwner(binding: ProjectBinding): boolean {
	const lock: OwnerLock = { pid: process.pid, cwd: binding.cwd, ts: Date.now() };
	try {
		fs.writeFileSync(ownerFile(binding), JSON.stringify(lock), { flag: "wx" });
		return true;
	} catch {
		/* exists — evaluate below */
	}
	const current = readOwnerLock(binding);
	if (current?.pid === process.pid) return true; // same process (e.g. /reload)
	if (!current) { // unreadable garbage — replace it
		try { fs.unlinkSync(ownerFile(binding)); } catch { return false; }
		try { fs.writeFileSync(ownerFile(binding), JSON.stringify(lock), { flag: "wx" }); return true; } catch { return false; }
	}
	if (current) {
		const dead = !isProcessAlive(current.pid);
		const stale = Date.now() - current.ts > OWNER_STALE_MS;
		if (dead || stale) {
			// Atomic-ish takeover: brief race window is acceptable — worst case a
			// message is delivered by two sessions once during a takeover.
			try { fs.unlinkSync(ownerFile(binding)); } catch { return false; }
			try { fs.writeFileSync(ownerFile(binding), JSON.stringify(lock), { flag: "wx" }); return true; } catch { return false; }
		}
	}
	return false;
}

function heartbeatOwner(binding: ProjectBinding): void {
	const current = readOwnerLock(binding);
	if (current?.pid === process.pid) writeOwnerLock(binding);
}

function releaseOwnerIfMine(binding: ProjectBinding): void {
	const current = readOwnerLock(binding);
	if (current?.pid === process.pid) {
		try { fs.unlinkSync(ownerFile(binding)); } catch { /* gone already */ }
	}
}

function isStillOwner(binding: ProjectBinding): boolean {
	return readOwnerLock(binding)?.pid === process.pid;
}

// --- Hold queue -------------------------------------------------------------

function readHoldQueue(binding: ProjectBinding): string[] {
	try {
		const raw = JSON.parse(fs.readFileSync(holdFile(binding), "utf8"));
		return Array.isArray(raw) ? (raw as unknown[]).filter((m): m is string => typeof m === "string") : [];
	} catch {
		return [];
	}
}

function appendHoldQueue(binding: ProjectBinding, message: string): void {
	const held = readHoldQueue(binding);
	held.push(message);
	try {
		fs.mkdirSync(STATE_DIR, { recursive: true });
		fs.writeFileSync(holdFile(binding), JSON.stringify(held, null, 2));
	} catch {
		/* non-fatal */
	}
}

function writeHoldQueue(binding: ProjectBinding, messages: string[]): void {
	try {
		fs.mkdirSync(STATE_DIR, { recursive: true });
		if (messages.length === 0) fs.unlinkSync(holdFile(binding));
		else fs.writeFileSync(holdFile(binding), JSON.stringify(messages, null, 2));
	} catch {
		/* non-fatal */
	}
}

/**
 * Valid cursor = unix timestamp. Anything else is dropped: ntfy load
 * balancers route requests to different instances, so `since=<message id>`
 * only resolves on the instance holding that message.
 */
function normalizedSince(binding: ProjectBinding): string {
	const raw = readCursor(binding);
	if (/^\d{10,13}$/.test(raw)) {
		return raw.length === 13 ? raw.slice(0, 10) : raw; // ms -> s
	}
	// No cursor yet for this project. A literal "now" is rejected by ntfy
	// (HTTP 400 "invalid since parameter" — silently ends the stream loop), so
	// fall back to the current time minus a small grace window for clock skew.
	return String(Math.floor(Date.now() / 1000) - 5);
}

/**
 * Self posts: anything the agent's side publishes, e.g. "Pi (workspace)".
 * Generalized to any "Pi (…)" / "Claude (…)" so every project's replies are
 * ignored, even after the project is renamed.
 */
function isSelfTitle(title: string, binding: ProjectBinding | undefined): boolean {
	if (/^(Pi|Claude)\s*\(/.test(title)) return true;
	if (!binding) return false;
	return (
		title === `Pi (${binding.slug})` || title === `Claude (${binding.slug})`
	);
}

// ---------------------------------------------------------------------------
// Networking
// ---------------------------------------------------------------------------

function portOpen(host: string, port: number, timeoutMs = 1500): Promise<boolean> {
	return new Promise((resolve) => {
		const s = net.connect({ host, port });
		const done = (ok: boolean) => {
			s.destroy();
			resolve(ok);
		};
		s.setTimeout(timeoutMs, () => done(false));
		s.on("connect", () => done(true));
		s.on("error", () => done(false));
	});
}

/** Spawn the configured SSH tunnel if its local port is not listening. */
async function ensureTunnel(connection: ResolvedConnection): Promise<void> {
	if (!connection.viaTunnel) return;
	if (await portOpen(connection.host, connection.port)) return;
	const cfg = readConfigFile();
	const tunnel = cfg.tunnel ?? {};
	const remote = process.env.NTFY_TUNNEL_REMOTE ?? tunnel.remote;
	const target = process.env.NTFY_TUNNEL_TARGET ?? tunnel.target;
	if (!remote || !target) return;
	const listenPort =
		Number(process.env.NTFY_TUNNEL_LISTEN_PORT ?? tunnel.listenPort ?? 2583) ||
		2583;
	const [targetHost, targetPort] = target.split(":");
	const remoteHost = remote.includes("@") ? remote.slice(remote.indexOf("@") + 1) : remote;
	const args: string[] = [
		"-N",
		"-o",
		"ExitOnForwardFailure=yes",
		"-o",
		"ServerAliveInterval=30",
		"-o",
		"ServerAliveCountMax=3",
		...(tunnel.sshArgs ?? []),
		...(tunnel.sshKey
			? ["-i", tunnel.sshKey, "-o", "IdentitiesOnly=yes"]
			: []),
		"-L",
		`${connection.host}:${listenPort}:${targetHost}:${targetPort ?? 443}`,
		remoteHost,
	];
	const child = spawn("ssh", args, { detached: true, stdio: "ignore" });
	child.unref();
	log(undefined, `[extension] ssh tunnel spawned (ssh ${args.join(" ")})`);
	// give the listener a moment to come up
	await new Promise((r) => setTimeout(r, 1500));
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
	return new Promise((resolve) => {
		const t = setTimeout(resolve, ms);
		signal?.addEventListener(
			"abort",
			() => {
				clearTimeout(t);
				resolve();
			},
			{ once: true },
		);
	});
}

/** Opens the topic stream; resolves when the connection ends (idle/network). */
function connectOnce(
	connection: ResolvedConnection,
	topic: string,
	since: string,
	onMessage: (title: string, body: string, timestampSeconds: number) => void,
): Promise<void> {
	return new Promise((resolve) => {
		let settled = false;
		const done = () => {
			if (settled) return;
			settled = true;
			resolve();
		};
		const url = new URL(connection.baseUrl);
		const req = https.request(
			{
				host: connection.host,
				port: connection.port,
				servername: connection.serverName,
				method: "GET",
				path: `${url.pathname === "/" ? "" : url.pathname}/${topic}/json?since=${encodeURIComponent(since)}`,
				headers: {
					host: connection.serverName,
					accept: "application/x-ndjson",
					...(connection.authToken
						? { authorization: `Bearer ${connection.authToken}` }
						: {}),
				},
				timeout: STREAM_TIMEOUT_MS,
			},
			(res) => {
				if (res.statusCode !== 200) {
					// Surface server rejections (e.g. a bad `since`) — ntfy answers 400
					// with a JSON error and just ends the stream otherwise.
					let body = "";
					res.setEncoding("utf8");
					res.on("data", (c: string) => { body = (body + c).slice(0, 200); });
					res.on("end", () => {
						log(currentBinding, `[extension] stream rejected: HTTP ${res.statusCode} ${body.trim()}`);
						done();
					});
					res.on("error", () => done());
					return;
				}
				res.setEncoding("utf8");
				let buf = "";
				res.on("data", (chunk: string) => {
					buf += chunk;
					let idx = buf.indexOf("\n");
					while (idx >= 0) {
						const line = buf.slice(0, idx).trim();
						buf = buf.slice(idx + 1);
						if (line) {
							try {
								const evt = JSON.parse(line) as NtfyEvent;
								if (
									evt.event === "message" &&
									evt.id &&
									evt.time &&
									!isSelfTitle(evt.title ?? "", currentBinding)
								) {
									onMessage(evt.title ?? "-", evt.message ?? "", evt.time);
								}
							} catch (err) {
								log(currentBinding, `[extension] handle error: ${String(err)}`);
							}
						}
						idx = buf.indexOf("\n");
					}
				});
				res.on("end", () => done());
				res.on("error", () => done());
			},
		);
		req.on("timeout", () => {
			try {
				req.destroy(new Error("idle timeout"));
			} catch {
				/* already torn down */
			}
		});
		req.on("error", (err) => {
			log(currentBinding, `[extension] stream error: ${String(err)}`);
			done();
		});
		req.end();
	});
}

/** Bindings live for the lifetime of the session — one listener per project. */
let currentBinding: ProjectBinding | undefined;
/**
 * Most recent ExtensionContext, captured from lifecycle handlers so network
 * callbacks can consult context usage without owning a live ctx reference.
 * Invalidated on session replacement (a replacement supplies a fresh ctx).
 */
let currentCtx: any;

// ---------------------------------------------------------------------------
// Extension
// ---------------------------------------------------------------------------

export default function (pi: ExtensionAPI) {
	let abort: AbortController | undefined;
	/** Coalescing queue: prefixed messages waiting to be delivered. */
	let pending: string[] = [];
	let firstPendingAt: number | undefined;
	let coalesceTimer: ReturnType<typeof setTimeout> | undefined;
	let heartbeatTimer: ReturnType<typeof setInterval> | undefined;
	let isOwner = false;
	let wasOwner = false;
	/** One log line per distinct owner state, instead of a line per 5s poll. */
	const lastStandbyLog: Record<string, string> = {};

	const running = (): boolean => abort !== undefined && !abort.signal.aborted;

	const stopTimers = (): void => {
		if (coalesceTimer) { clearTimeout(coalesceTimer); coalesceTimer = undefined; }
		if (heartbeatTimer) { clearInterval(heartbeatTimer); heartbeatTimer = undefined; }
	};

	const stop = (): void => {
		abort?.abort();
	};

	/** Context guard: true when the captured ctx says the session is too full. */
	const beyondContextGuard = (): boolean => {
		const threshold = runtimeTunables().contextGuardPercent;
		if (threshold <= 0) return false; // disabled
		try {
			const usage = currentCtx?.getContextUsage?.();
			if (!usage || typeof usage.percent !== "number") return false; // unknown — deliver
			return usage.percent >= threshold;
		} catch {
			return false;
		}
	};

	const sendUserMessageSafe = (text: string, binding: ProjectBinding): void => {
		try {
			pi.sendUserMessage(text, { deliverAs: "followUp" });
		} catch {
			try {
				pi.sendUserMessage(text);
			} catch (err) {
				log(binding, `[extension] sendUserMessage failed: ${String(err)}`);
			}
		}
	};

	/** Deliver one batch (or a single held message), respecting the guard. */
	const deliverJoined = (text: string, binding: ProjectBinding): void => {
		if (beyondContextGuard()) {
			appendHoldQueue(binding, text);
			const threshold = runtimeTunables().contextGuardPercent;
			log(binding, `[extension] context guard — held message (n=${readHoldQueue(binding).length}, threshold=${threshold}%)`);
			try {
				currentCtx?.ui?.notify?.(
					`ntfy: held message — session context >${threshold}% of window. It will be delivered after compaction or via /ntfy-flush.`,
					"warning",
				);
			} catch {
				/* no UI — the log already captured it */
			}
			return;
		}
		sendUserMessageSafe(text, binding);
	};

	/** Push every pending message into a single joined delivery. */
	const flushPending = (binding: ProjectBinding): void => {
		if (coalesceTimer) { clearTimeout(coalesceTimer); coalesceTimer = undefined; }
		const batch = pending;
		pending = [];
		firstPendingAt = undefined;
		if (!batch.length) return;
		deliverJoined(batch.join("\n\n"), binding);
	};

	/** Debounce window so a burst of pushes costs one turn, not N. */
	const enqueueMessage = (text: string, binding: ProjectBinding): void => {
		pending.push(text);
		const { coalesceMs, coalesceMaxMs } = runtimeTunables();
		if (coalesceMs <= 0) {
			flushPending(binding);
			return;
		}
		const now = Date.now();
		firstPendingAt ??= now;
		if (coalesceTimer) clearTimeout(coalesceTimer);
		const elapsed = now - (firstPendingAt ?? now);
		const delay = Math.min(coalesceMs, Math.max(0, coalesceMaxMs - elapsed));
		coalesceTimer = setTimeout(() => {
			coalesceTimer = undefined;
			flushPending(binding);
		}, delay);
	};

	/** Deliver everything held back by the context guard (joined, no surprise turns). */
	const flushHeld = (binding: ProjectBinding, force = false): void => {
		const held = readHoldQueue(binding);
		if (!held.length) return;
		if (!force && beyondContextGuard()) return;
		writeHoldQueue(binding, []);
		deliverJoined(held.join("\n\n"), binding);
	};

	/**
	 * Poll for ownership: acquire, log only on transitions, and flush any
	 * hold queue left behind by a previous life of this topic.
	 */
	const acquireAndMaybeFlushHold = (binding: ProjectBinding): void => {
		wasOwner = isOwner;
		isOwner = tryAcquireOwner(binding);
		if (!isOwner) {
			if (!wasOwner) {
				const current = readOwnerLock(binding);
				const stamp = current
					? `pid ${current.pid} · cwd ${current.cwd || "?"} · heartbeat ${new Date(current.ts).toISOString()}`
					: "unclaimed";
				if (lastStandbyLog[binding.key] !== stamp) {
					lastStandbyLog[binding.key] = stamp;
					log(binding, `[extension] standing by — topic owned by ${stamp}`);
				}
			}
			return;
		}
		if (!wasOwner) log(binding, `[extension] became topic owner (pid ${process.pid}), ${readHoldQueue(binding).length} held`);
		if (readHoldQueue(binding).length) flushHeld(binding);
	};

	const start = (cwd: string): void => {
		if (running()) return;
		const binding = bindProject(cwd);
		currentBinding = binding;
		abort = new AbortController();
		const signal = abort.signal;
		const connection = resolveConnection();
		try {
			releaseOwnerIfMine(binding); // stale lock from a previous life of this pid
		} catch { /* non-fatal */ }
		log(
			binding,
			`[extension] listener started (project=${binding.cwd}, topic=${binding.topic} via ${binding.source}, since=${normalizedSince(binding)}, server=${connection.serverName}${connection.viaTunnel ? ` via tunnel ${connection.host}:${connection.port}` : ""})`,
		);
		acquireAndMaybeFlushHold(binding);
		if (isOwner) heartbeatOwner(binding); // fresh ts on (re)claim
		heartbeatTimer = setInterval(() => {
			try {
				if (isOwner) heartbeatOwner(binding);
			} catch { /* non-fatal */ }
		}, HEARTBEAT_MS);
		void (async () => {
			let backoff = 3000;
			while (!signal.aborted) {
				if (!isOwner) {
					// Standby: no stream, no deliveries — just polling for ownership.
					await sleep(STANDBY_POLL_MS, signal);
					if (signal.aborted) break;
					acquireAndMaybeFlushHold(binding);
					continue;
				}
				try {
					await ensureTunnel(connection);
				} catch {
					log(binding, "[extension] tunnel spawn failed — will retry");
				}
				let delivered = false;
				await connectOnce(
					resolveConnection(),
					binding.topic,
					normalizedSince(binding),
					(title, body, ts) => {
						delivered = true;
						backoff = 3000;
						// Advance the cursor immediately — a crash must never replay
						// a message (ntfy since is inclusive).
						writeCursor(binding, String(ts + 1));
						log(binding, `[${title}] ${body}`);
						enqueueMessage(`[ntfy ${title}] ${body}`, binding);
					},
				);
				// Only the owner streams; a lost lock drops back to standby.
				if (!isStillOwner(binding)) {
					isOwner = false;
					log(binding, "[extension] lost topic ownership — standing by");
					continue;
				}
				if (signal.aborted) break;
				if (delivered) backoff = 3000;
				await sleep(backoff, signal);
				backoff = Math.min(backoff * 2, 30_000);
			}
			// Keep held messages + release ownership so a standby can take over.
			// Any unsent pending messages are parked in the hold queue (cursor is
			// already advanced) — never delivered mid-teardown.
			if (pending.length) {
				const unsent = pending;
				pending = [];
				for (const text of unsent) appendHoldQueue(binding, text);
				log(binding, `[extension] parked ${unsent.length} undelivered message(s) in the hold queue`);
			}
			releaseOwnerIfMine(binding);
			stopTimers();
			isOwner = false;
			log(binding, "[extension] listener stopped");
		})();
	};

	/** Restart the listener against a (possibly different) project. */
	const restartFor = (cwd: string): void => {
		const binding = bindProject(cwd);
		if (currentBinding && currentBinding.key === binding.key && running()) {
			// Same project, still running — nothing to rebind.
			return;
		}
		stop();
		start(cwd);
	};

	pi.registerCommand("ntfy-start", {
		description:
			"Start (or restart) the ntfy listener for the current project",
		handler: async (_args, ctx) => {
			stop();
			start(ctx.cwd);
			ctx.ui.notify(
				`ntfy listener started — project ${projectSlug(ctx.cwd)}, topic ${bindProject(ctx.cwd).topic}`,
				"info",
			);
		},
	});

	pi.registerCommand("ntfy-stop", {
		description: "Stop the ntfy listener",
		handler: async (_args, ctx) => {
			stop();
			ctx.ui.notify("ntfy listener stopped", "info");
		},
	});

	pi.registerCommand("ntfy-flush", {
		description:
			"Deliver ntfy messages held back by the context guard (arg: force = ignore guard)",
		handler: async (args, ctx) => {
			const binding = currentBinding ?? bindProject(ctx.cwd);
			const held = readHoldQueue(binding);
			if (!held.length) {
				ctx.ui.notify("ntfy: nothing held", "info");
				return;
			}
			const force = /^(force|-f)/.test(args?.trim() ?? "");
			if (!force && beyondContextGuard()) {
				ctx.ui.notify(
					`ntfy: context still >${runtimeTunables().contextGuardPercent}% of window — compact first or use "force"`,
					"warning",
				);
				return;
			}
			writeHoldQueue(binding, []);
			deliverJoined(held.join("\n\n"), binding);
			ctx.ui.notify(`ntfy: delivered ${held.length} held message(s)`, "info");
		},
	});

	pi.registerCommand("ntfy-status", {
		description: "Show ntfy listener state (project binding)",
		handler: async (_args, ctx) => {
			const binding = bindProject(ctx.cwd);
			const connection = resolveConnection();
			const owner = readOwnerLock(binding);
			const mine = isStillOwner(binding);
			const held = readHoldQueue(binding);
			const tunables = runtimeTunables();
			ctx.ui.notify(
				`ntfy listener: ${running() ? "running" : "stopped"} — project ${binding.slug} (${binding.cwd}), topic ${binding.topic} (via ${binding.source}), cursor ${currentBinding?.key === binding.key ? normalizedSince(binding) : "n/a (not started)"}, server ${connection.serverName}${connection.viaTunnel ? ` via tunnel ${connection.host}:${connection.port}` : " (direct)"}; owner: ${mine ? `this session (pid ${process.pid})` : owner ? `pid ${owner.pid} (${owner.cwd})` : "unclaimed"}; role: ${mine ? "owner (streaming)" : running() ? "standby" : "off"}; coalesce: ${tunables.coalesceMs}ms (cap ${tunables.coalesceMaxMs}ms); context guard: ${tunables.contextGuardPercent}%; held: ${held.length}`,
				"info",
			);
		},
	});

	pi.registerCommand("ntfy-topic", {
		description:
			"Show or override the ntfy topic bound to this project",
		handler: async (args, ctx) => {
			const binding = bindProject(ctx.cwd);
			const arg = args?.trim() ?? "";
			if (arg) {
				fs.writeFileSync(projectTopicFile(ctx.cwd), `${arg.trim()}\n`);
				ctx.ui.notify(
					`Project topic override written to ${projectTopicFile(ctx.cwd)}`,
					"info",
				);
				restartFor(ctx.cwd);
				ctx.ui.notify(`ntfy listener rebound to topic ${arg.trim()}`, "info");
				return;
			}
			ctx.ui.notify(
				`Project ${binding.slug} (${binding.cwd}) → topic ${binding.topic} (via ${binding.source}). Pass an argument to override via ${projectTopicFile(ctx.cwd)}`,
				"info",
			);
		},
	});

	pi.on("session_start", (event, ctx) => {
		// Session start / /reload / new / resume / fork always rebinds to the
		// session's project; a switch between projects restarts the listener.
		currentCtx = ctx;
		const previousBinding = currentBinding;
		const binding = bindProject(ctx.cwd);
		if (previousBinding && previousBinding.key !== binding.key) {
			// Project switch: hand over cleanly, then rebind under the new topic.
			releaseOwnerIfMine(previousBinding);
		}
		if (!running() || (previousBinding && previousBinding.key !== binding.key)) {
			stop();
		}
		start(ctx.cwd);
	});

	pi.on("agent_start", () => {
		try { if (currentBinding && isOwner) heartbeatOwner(currentBinding); } catch { /* non-fatal */ }
	});

	// Self-heal + ownership hygiene at every turn boundary: restart a dead
	// listener, refresh the owner heartbeat, and release any held messages
	// once the session context is back under the guard threshold.
	pi.on("agent_end", (_event, ctx) => {
		currentCtx = ctx;
		try {
			if (!currentBinding) return;
			if (running()) {
				if (isOwner) heartbeatOwner(currentBinding);
				flushHeld(currentBinding);
			} else {
				log(currentBinding, "[extension] agent_end — listener was down, restarting");
				start(currentBinding.cwd);
			}
		} catch (err) {
			log(currentBinding, `[extension] agent_end handler error: ${String(err)}`);
		}
	});

	pi.on("session_shutdown", () => {
		stop();
		if (currentBinding) releaseOwnerIfMine(currentBinding);
	});
}