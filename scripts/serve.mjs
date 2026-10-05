#!/usr/bin/env node

/** Long-lived Pi SDK host; the installed pi-telegram extension owns Telegram. */
import { createServer } from "node:http";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  createAgentSessionFromServices,
  createAgentSessionRuntime,
  createAgentSessionServices,
  getAgentDir,
  SessionManager,
} from "@earendil-works/pi-coding-agent";

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const agentDir = getAgentDir();
const cwd = process.cwd();
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
    resourceLoaderOptions: { noContextFiles: true, appendSystemPrompt: [join(root, "agent", "AGENTS.md")] },
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
  unsubscribe?.();
  await session.bindExtensions({
    mode: "rpc",
    commandContextActions: {
      waitForIdle: () => runtime.session.waitForIdle(),
      newSession: options => runtime.newSession(options),
      switchSession: (path, options) => runtime.switchSession(path, options),
      fork: (id, options) => runtime.fork(id, options),
      navigateTree: (id, options) => runtime.session.navigateTree(id, options),
      reload: () => runtime.reload(),
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
  await session.prompt("/telegram-connect", { source: "extension" });
  log("session_started", { sessionId: session.sessionId, provider: session.model?.provider, model: session.model?.id });
}
runtime.setRebindSession(bindSession);
await bindSession(runtime.session);

const server = createServer((request, response) => {
  if (request.url !== "/healthz") { response.writeHead(404).end(); return; }
  let polling = false;
  try {
    const state = JSON.parse(readFileSync(join(agentDir, "tmp", "pi-telegram", "state.json"), "utf8"));
    const profile = state.profiles?.default;
    polling = profile?.runtime?.runtime?.pollingActive === true &&
      profile?.transport?.pid === process.pid;
  } catch { /* Absent diagnostics mean not ready. */ }
  response.writeHead(polling ? 200 : 503, { "content-type": "application/json" });
  response.end(JSON.stringify({ polling, pid: process.pid, sessionId: runtime.session.sessionId,
    provider: runtime.session.model?.provider, model: runtime.session.model?.id, busy: runtime.session.isStreaming }));
});
server.listen(8186, "127.0.0.1");

let stopping = false;
async function stop() {
  if (stopping) return;
  stopping = true;
  server.close();
  unsubscribe?.();
  try { await runtime.dispose(); log("stopped"); process.exit(0); }
  catch (error) { log("shutdown_error", { error: String(error) }); process.exit(1); }
}
process.once("SIGINT", stop);
process.once("SIGTERM", stop);
