#!/usr/bin/env node

/** Long-lived Pi SDK host; the installed pi-telegram extension owns Telegram. */
import { createServer } from "node:http";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { homedir } from "node:os";
import { fileURLToPath } from "node:url";
import { startHealthAutomation } from "./health-automation.mjs";
import { startMediaAutomation } from "./media-automation.mjs";
import telegram from "../dist/index.js";
import {
  createAgentSessionFromServices,
  createAgentSessionRuntime,
  createAgentSessionServices,
  getAgentDir,
  SessionManager,
} from "@earendil-works/pi-coding-agent";

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const agentDir = getAgentDir();
const role = process.argv[2] ?? "ai";
const sessions = {
  ai: { cwd: process.cwd(), topic: 16, port: 8186, instructions: "ai/AGENTS.md", notes: ["watchlist.md"] },
  cinema: { cwd: join(homedir(), "projects/home/cli-botlix"), topic: 3, port: 8187, instructions: "cinema/AGENTS.md", notes: ["watchlist.md"], startAutomation: startMediaAutomation },
  health: { cwd: join(homedir(), ".local/share/family/health"), topic: 359, port: 8188, instructions: "health/AGENTS.md", notes: ["health/alex.md", "health/maru.md", "health/notes.md"], startAutomation: startHealthAutomation },
};
const sessionConfig = Object.hasOwn(sessions, role) ? sessions[role] : undefined;
if (!sessionConfig || process.argv[3]) throw new Error("Unknown session role: use ai, cinema or health");
const cwd = sessionConfig.cwd;
const forumTarget = { chatId: -1003985826484, threadId: sessionConfig.topic };
const sessionDir = join(agentDir, "sessions", "pi-telegram");
function log(event, detail = {}) {
  let line = JSON.stringify({ at: new Date().toISOString(), event, ...detail });
  for (const name of ["TELEGRAM_BOT_TOKEN", "DEEPSEEK_API_KEY"]) {
    if (process.env[name]) line = line.replaceAll(process.env[name], "[redacted]");
  }
  process.stdout.write(`${line}\n`);
}

const runtime = await createAgentSessionRuntime(async ({ cwd, agentDir, sessionManager, sessionStartEvent }) => {
  const services = await createAgentSessionServices({
    cwd, agentDir,
    resourceLoaderOptions: {
      noExtensions: true, noContextFiles: true,
      appendSystemPrompt: [join(root, "skills/telegram-bridge/SKILL.md"), join(root, "agent/TELEGRAM.md"), join(root, "agent", sessionConfig.instructions)],
      extensionFactories: [pi => telegram(pi, { forumTarget }), pi => {
        pi.on("before_agent_start", event => ({ systemPrompt: event.systemPrompt + "\n<family_preferences>\n" +
          ["alex.md", "maru.md", ...sessionConfig.notes].map(name => `${name}:\n${readFileSync(join(homedir(), ".local/share/family", name), "utf8")}`).join("\n\n") +
          "\n</family_preferences>" }));
      }],
    },
  });
  const result = await createAgentSessionFromServices({ services, sessionManager, sessionStartEvent });
  if (services.diagnostics.some(item => item.type === "error") || result.extensionsResult.errors.length ||
      !result.extensionsResult.extensions.some(extension => extension.commands.has("telegram-connect"))) {
    throw new Error("Pi extension startup failed; check the installed pi-telegram package.");
  }
  return { ...result, services, diagnostics: services.diagnostics };
}, { cwd, agentDir, sessionManager: SessionManager.continueRecent(cwd, sessionDir) });

let unsubscribe;
async function bindSession(session) {
  const manager = session.sessionManager;
  const file = manager.getSessionFile();
  if (file && !existsSync(file)) {
    writeFileSync(file, [manager.getHeader(), ...manager.getEntries()].map(entry => JSON.stringify(entry)).join("\n") + "\n", { flag: "wx", mode: 0o600 });
    manager.setSessionFile(file);
  }
  unsubscribe?.();
  await session.bindExtensions({
    mode: "rpc",
    commandContextActions: {
      waitForIdle: () => runtime.session.waitForIdle(),
      newSession: options => runtime.newSession(options),
      switchSession: (path, options) => runtime.switchSession(path, options),
      fork: (id, options) => runtime.fork(id, options),
      navigateTree: (id, options) => runtime.session.navigateTree(id, options),
      reload: () => runtime.session.reload(),
    },
    onError: error => log("extension_error", { error: error.error, source: error.extensionPath }),
    shutdownHandler: () => stop(),
  });
  unsubscribe = session.subscribe(event => {
    if (event.type === "agent_settled" || event.type === "compaction_end") {
      log(event.type, { sessionId: session.sessionId, model: session.model?.id });
    }
    if (event.type === "message_end" && event.message.role === "assistant" && event.message.errorMessage) {
      log("model_error", { error: event.message.errorMessage });
    }
  });
  log("session_started", { sessionId: session.sessionId, provider: session.model?.provider, model: session.model?.id, tools: session.getActiveToolNames(), skills: runtime.services.resourceLoader.getSkills().skills.map(skill => skill.name) });
}
runtime.setRebindSession(bindSession);
await bindSession(runtime.session);
await runtime.session.prompt("/telegram-connect", { source: "extension" });

const stopAutomation = sessionConfig.startAutomation?.(log);

const server = createServer((request, response) => {
  if (request.url !== "/healthz") { response.writeHead(404).end(); return; }
  let polling = false;
  try {
    const state = JSON.parse(readFileSync(join(agentDir, "tmp", "pi-telegram", "state.json"), "utf8"));
    const profile = state.profiles?.default;
    polling = profile?.transport?.pid === process.pid;
  } catch { /* Absent diagnostics mean not ready. */ }
  const connected = polling || runtime.session.getActiveToolNames().includes("telegram_attach");
  response.writeHead(connected ? 200 : 503, { "content-type": "application/json" });
  response.end(JSON.stringify({ polling, connected, topic: forumTarget.threadId, role, instructions: sessionConfig.instructions, tools: runtime.session.getActiveToolNames(), pid: process.pid, sessionId: runtime.session.sessionId,
    provider: runtime.session.model?.provider, model: runtime.session.model?.id, busy: runtime.session.isStreaming }));
});
server.listen(sessionConfig.port, "127.0.0.1");

let stopping = false;
async function stop() {
  if (stopping) return;
  stopping = true;
  server.close();
  unsubscribe?.();
  try { await stopAutomation?.(); await runtime.dispose(); log("stopped"); process.exit(0); }
  catch (error) { log("shutdown_error", { error: String(error) }); process.exit(1); }
}
process.once("SIGINT", stop);
process.once("SIGTERM", stop);
