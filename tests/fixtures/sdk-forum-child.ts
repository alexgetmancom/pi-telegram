/** Separate Pi process for the real SDK forum lifecycle regression. No external network. */
import { createAgentSessionRuntime, createAgentSessionServices, createAgentSessionFromServices, SessionManager } from "@earendil-works/pi-coding-agent";
import telegram from "../../lib/extension.ts";
const [cwd, agentDir] = process.argv.slice(2) as [string, string];
globalThis.fetch = async () => { throw new Error("Follower must use the leader's Telegram transport"); };
const runtime = await createAgentSessionRuntime(async ({ sessionManager, sessionStartEvent }) => {
  const services = await createAgentSessionServices({ cwd, agentDir, resourceLoaderOptions: {
    noExtensions: true, noSkills: true, noContextFiles: true, noPromptTemplates: true, noThemes: true,
    extensionFactories: [pi => telegram(pi, { forumTarget: { chatId: -1007, threadId: 3 } })],
  } });
  return { ...await createAgentSessionFromServices({ services, sessionManager, sessionStartEvent, tools: [] }), services, diagnostics: services.diagnostics };
}, { cwd, agentDir, sessionManager: SessionManager.create(cwd) });
const bind = async (session: typeof runtime.session) => {
  await session.bindExtensions({ mode: "rpc", commandContextActions: {
    waitForIdle: () => runtime.session.waitForIdle(), newSession: options => runtime.newSession(options),
    switchSession: (path, options) => runtime.switchSession(path, options), fork: (id, options) => runtime.fork(id, options),
    navigateTree: (id, options) => runtime.session.navigateTree(id, options), reload: () => runtime.session.reload(),
  } });
  process.send?.({ sessionId: session.sessionId });
};
runtime.setRebindSession(bind); await bind(runtime.session);
await runtime.session.prompt("/telegram-connect", { source: "extension" });
process.on("message", async () => { await runtime.dispose(); process.exit(0); });
