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
 *   - Log:    ~/.pi/agent/ntfy/<hash10>.log
 *
 * Commands: /ntfy-start, /ntfy-stop, /ntfy-status, /ntfy-topic.
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
	return "now";
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

// ---------------------------------------------------------------------------
// Extension
// ---------------------------------------------------------------------------

export default function (pi: ExtensionAPI) {
	let abort: AbortController | undefined;

	const running = (): boolean => abort !== undefined && !abort.signal.aborted;

	const stop = (): void => {
		abort?.abort();
	};

	const start = (cwd: string): void => {
		if (running()) return;
		const binding = bindProject(cwd);
		currentBinding = binding;
		abort = new AbortController();
		const signal = abort.signal;
		const connection = resolveConnection();
		log(
			binding,
			`[extension] listener started (project=${binding.cwd}, topic=${binding.topic} via ${binding.source}, since=${normalizedSince(binding)}, server=${connection.serverName}${connection.viaTunnel ? ` via tunnel ${connection.host}:${connection.port}` : ""})`,
		);
		void (async () => {
			let backoff = 3000;
			while (!signal.aborted) {
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
						writeCursor(binding, String(ts));
						log(binding, `[${title}] ${body}`);
						const text = `[ntfy ${title}] ${body}`;
						try {
							pi.sendUserMessage(text, { deliverAs: "followUp" });
						} catch {
							try {
								pi.sendUserMessage(text);
							} catch (err) {
								log(binding, `[extension] sendUserMessage failed: ${String(err)}`);
							}
						}
					},
				);
				if (signal.aborted) break;
				if (delivered) backoff = 3000;
				await sleep(backoff, signal);
				backoff = Math.min(backoff * 2, 30_000);
			}
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

	pi.registerCommand("ntfy-status", {
		description: "Show ntfy listener state (project binding)",
		handler: async (_args, ctx) => {
			const binding = bindProject(ctx.cwd);
			const connection = resolveConnection();
			ctx.ui.notify(
				`ntfy listener: ${running() ? "running" : "stopped"} — project ${binding.slug} (${binding.cwd}), topic ${binding.topic} (via ${binding.source}), cursor ${currentBinding?.key === binding.key ? normalizedSince(binding) : "n/a (not started)"}, server ${connection.serverName}${connection.viaTunnel ? ` via tunnel ${connection.host}:${connection.port}` : " (direct)"}`,
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
		const previousBinding = currentBinding;
		const binding = bindProject(ctx.cwd);
		if (!running() || (previousBinding && previousBinding.key !== binding.key)) {
			stop();
		}
		start(ctx.cwd);
	});

	// Self-heal: after /reload the runtime is replaced and session_start does
	// not always re-fire mid-session — re-check on every turn boundary.
	pi.on("agent_end", () => {
		if (!running() && currentBinding) {
			log(currentBinding, "[extension] agent_end — listener was down, restarting");
			start(currentBinding.cwd);
		}
	});

	pi.on("session_shutdown", () => {
		stop();
	});
}