/** Media notification rendering regression. */
import assert from "node:assert/strict";
import test from "node:test";
import { readFileSync, writeFileSync, existsSync, mkdtempSync, rmSync } from "node:fs";
import { homedir, tmpdir } from "node:os";
import { join } from "node:path";
import { Script, createContext } from "node:vm";
// @ts-expect-error The deployed SDK host runs native JavaScript.
import { mediaEventView } from "../scripts/media-automation.mjs";

test("Media notification escapes titles and preserves Jellyfin action", () => {
  const view = mediaEventView({ text: "Downloaded <movie>", item: { Name: "Movie & name", Overview: "<bad>", watch_url: "https://jf.i/watch" } });
  assert.equal(view.parseMode, "html"); assert.match(view.text, /&lt;movie&gt;/); assert.match(view.text, /Movie &amp; name/);
  assert.equal(view.replyMarkup.inline_keyboard[0][0].url, "https://jf.i/watch");
});

test("Media automation sends a poster, acknowledges only delivery and cleans temporary files", async () => {
  const scheduled: Array<() => void> = [];
  const calls: string[] = [];
  const errors: string[] = [];
  let posterPath = "";
  let photos = 0;
  const event = { id: 1, text: "Downloaded", kind: "completed", item: { Id: "a".repeat(32), Name: "Movie", ImageTags: { Primary: "tag" } } };
  const source = readFileSync(new URL("../scripts/media-automation.mjs", import.meta.url), "utf8")
    .replace(/^import \{([^}]+)\} from "([^"]+)";/gm, (_all, names: string, module: string) => `const {${names}} = imports[${JSON.stringify(module)}];`)
    .replace(/^export function /gm, "function ");
  const context = createContext({
    AbortController,
    setTimeout: (fn: () => void) => { scheduled.push(fn); return { unref() {} }; },
    clearTimeout: () => {},
    imports: {
      "node:child_process": { execFile: async (_binary: string, args: string[]) => {
        calls.push(args[0]);
        if (args[0] === "poster") { posterPath = args[2]; writeFileSync(posterPath, "poster"); }
        return { stdout: JSON.stringify({ ok: true, data: args[0] === "events" ? [event] : {} }) };
      } },
      "node:util": { promisify: (fn: unknown) => fn },
      "node:fs": { existsSync, mkdtempSync, rmSync },
      "node:path": { join },
      "node:os": { homedir, tmpdir },
      "../dist/api/delivery.js": {
        sendTelegramView: async () => { throw new Error("Expected photo delivery"); },
        sendTelegramPhoto: async (path: string) => { assert.equal(existsSync(path), true); photos++; return { ok: true }; },
      },
    },
  });
  new Script(source + "\nglobalThis.startAudit = startMediaAutomation;").runInContext(context);
  const stop = context.startAudit((kind: string, detail: { error?: string }) => { if (kind.includes("error")) errors.push(detail.error ?? kind); });
  scheduled.shift()?.();
  await new Promise(resolve => setImmediate(resolve));
  await stop();
  assert.deepEqual(errors, []);
  assert.equal(photos, 1);
  assert.deepEqual(calls, ["tick", "events", "poster", "claim", "ack"]);
  assert.equal(existsSync(posterPath), false);
});

test("family SDK host keeps Health isolated with full tools and role-specific automation", async () => {
  const source = readFileSync(new URL("../scripts/serve.mjs", import.meta.url), "utf8")
    .replace(/^#!.*\n/u, "")
    .replace(/^import ([\s\S]*?) from "([^"]+)";/gm, (_all, names: string, module: string) => names.trim().startsWith("{")
      ? `const ${names} = imports[${JSON.stringify(module)}];`
      : `const ${names} = imports[${JSON.stringify(module)}].default;`);
  const expected = [
    { role: undefined, topic: 16, port: 8186, cwd: "/home/alex", agent: undefined, automation: false },
    { role: "cinema", topic: 3, port: 8187, cwd: "/home/alex/projects/home/cli-botlix", agent: "CINEMA.md", automation: true },
    { role: "health", topic: 359, port: 8188, cwd: "/home/alex/.local/share/family/health", agent: "HEALTH.md", automation: true },
  ];
  for (const target of expected) {
    const calls: { cwd?: string; topic?: number; port?: number; instructions?: string[]; automation?: boolean; prompt?: string; healthFiles?: string[] } = { healthFiles: [] };
    const sessionManager = { getSessionFile: () => "/session.jsonl", getHeader: () => ({}), getEntries: () => [], setSessionFile: () => {} };
    const before: Array<(event: { systemPrompt: string }) => unknown> = [];
    const tools = ["read", "bash", "edit", "write", "telegram_attach", "telegram_bind", "telegram_channel_post", "telegram_channel_posts", "telegram_message"];
    const session = { sessionManager, sessionId: target.role ?? "ai", model: { provider: "test", id: "test" }, isStreaming: false,
      getActiveToolNames: () => tools, bindExtensions: async () => {}, subscribe: () => () => {}, prompt: async (prompt: string) => { calls.prompt = prompt; } };
    const context = createContext({ console, URL, Object, JSON, AbortController,
      process: { argv: ["node", "serve.mjs", ...(target.role ? [target.role] : [])], cwd: () => "/home/alex", env: {}, stdout: { write: () => {} }, once: () => {} },
      imports: {
        "node:http": { createServer: () => ({ listen: (port: number) => { calls.port = port; }, close: () => {} }) },
        "node:fs": { existsSync: () => true, writeFileSync: () => {}, readFileSync: (path: string) => { calls.healthFiles!.push(path); return "memory"; } },
        "node:path": { dirname: () => "/repo", join: (...parts: string[]) => parts.join("/") },
        "node:os": { homedir: () => "/home/alex" }, "node:url": { fileURLToPath: () => "/repo/scripts/serve.mjs" },
        "./health-automation.mjs": { startHealthAutomation: () => { calls.automation = true; return () => {}; } },
        "./media-automation.mjs": { startMediaAutomation: () => { calls.automation = true; return () => {}; } },
        "../dist/index.js": { default: (_pi: unknown, config: { forumTarget: { threadId: number } }) => { calls.topic = config.forumTarget.threadId; } },
        "@earendil-works/pi-coding-agent": {
          getAgentDir: () => "/agent", SessionManager: { continueRecent: (cwd: string) => { calls.cwd = cwd; return sessionManager; } },
          createAgentSessionRuntime: async (factory: (input: unknown) => Promise<{ services: unknown }>, config: { cwd: string }) => {
            const result = await factory({ cwd: config.cwd, agentDir: "/agent", sessionManager, sessionStartEvent: {} });
            return { session, services: result.services, setRebindSession: () => {}, dispose: async () => {} };
          },
          createAgentSessionServices: async (config: { resourceLoaderOptions: { appendSystemPrompt: string[]; extensionFactories: Array<(pi: unknown) => void> } }) => {
            calls.instructions = config.resourceLoaderOptions.appendSystemPrompt;
            for (const factory of config.resourceLoaderOptions.extensionFactories) factory({ on: (_name: string, handler: (event: { systemPrompt: string }) => unknown) => { before.push(handler); } });
            return { diagnostics: [], resourceLoader: { getSkills: () => ({ skills: [] }) } };
          },
          createAgentSessionFromServices: async () => ({ extensionsResult: { errors: [], extensions: [{ commands: new Map([["telegram-connect", {}]]) }] } }),
        },
      },
    });
    // import.meta is the only source syntax unavailable to a vm Script.
    await new Script(`(async () => { ${source.replaceAll("import.meta.url", '"file:///repo/scripts/serve.mjs"')} })()`).runInContext(context);
    before[0]?.({ systemPrompt: "test" });
    assert.equal(calls.cwd, target.cwd); assert.equal(calls.topic, target.topic); assert.equal(calls.port, target.port);
    assert.equal(Boolean(calls.automation), target.automation); assert.equal(calls.prompt, "/telegram-connect");
    assert.equal(calls.instructions?.some(path => path.endsWith(target.agent ?? "NO_EXTRA_AGENT")), Boolean(target.agent));
    assert.equal(calls.healthFiles?.some(path => path.endsWith("health/alex.md")), target.role === "health");
    assert.equal(calls.healthFiles?.some(path => path.endsWith("watchlist.md")), target.role !== "health");
  }
});

// @ts-expect-error The deployed SDK host runs native JavaScript.
import { healthWeeklyKey, healthReportView } from "../scripts/health-automation.mjs";
test("Health report keeps people separate and missing data unknown; weekly cadence is Moscow time", () => {
  assert.equal(healthWeeklyKey(new Date("2026-10-11T17:59:00Z")), null);
  assert.equal(healthWeeklyKey(new Date("2026-10-11T18:00:00Z")), "2026-10-11");
  assert.equal(healthWeeklyKey(new Date("2026-10-12T18:00:00Z")), null);
  const view = healthReportView({ alex: { metrics: { steps: { data: [{ average: 1234, observed_days: 2 }] } }, sync: [] }, maru: { metrics: {}, sync: [{ metric: "sleep_details", error: "source failure" }] } });
  assert.match(view.text, /Алекс[\s\S]*1234/); assert.match(view.text, /Маша[\s\S]*нет данных/); assert.match(view.text, /sleep_details/);
});

test("Health automation preserves partial CLI results and never repeats uncertain weekly sends", async () => {
  const scheduled: Array<() => void> = [];
  let receipt: string | undefined;
  let sends = 0;
  const commands: string[] = [];
  const source = readFileSync(new URL("../scripts/health-automation.mjs", import.meta.url), "utf8")
    .replace(/^import \{([^}]+)\} from "([^"]+)";/gm, (_all, names: string, module: string) => `const {${names}} = imports[${JSON.stringify(module)}];`)
    .replace(/^export function /gm, "function ");
  class Sunday extends Date { constructor() { super("2026-10-11T18:00:00Z"); } }
  const context = createContext({ AbortController, Intl, Date: Sunday, setTimeout: (fn: () => void) => { scheduled.push(fn); return { unref() {} }; }, clearTimeout() {}, imports: {
    "node:child_process": { execFile: async (_binary: string, args: string[]) => {
      commands.push(args[0]);
      if (args[0] === "tick") throw Object.assign(new Error("partial failure"), { stdout: JSON.stringify({ ok: false, data: {}, error: "source failed" }) });
      return { stdout: JSON.stringify({ ok: true, data: { alex: { metrics: {}, sync: [] } } }) };
    } },
    "node:util": { promisify: (fn: unknown) => fn }, "node:path": { join }, "node:os": { homedir: () => "/private" },
    "node:fs": { readFileSync: () => { if (!receipt) throw Object.assign(new Error("missing"), { code: "ENOENT" }); return receipt; }, writeFileSync: (_path: string, body: string) => { receipt = body; }, renameSync() {} },
    "../dist/api/delivery.js": { sendTelegramView: async (_view: unknown, options: { scope: { target: { threadId: number } } }) => { sends++; assert.equal(options.scope.target.threadId, 359); assert.equal(JSON.parse(receipt!).state, "claimed"); return { ok: false, reason: "commit-unknown" }; } },
  } });
  new Script(source + "\nglobalThis.startAudit = startHealthAutomation;").runInContext(context);
  const stop = context.startAudit(() => {});
  scheduled.shift()?.(); await new Promise(resolve => setImmediate(resolve));
  assert.equal(JSON.parse(receipt!).state, "uncertain");
  scheduled.shift()?.(); await new Promise(resolve => setImmediate(resolve));
  await stop(); assert.equal(sends, 1); assert.deepEqual(commands, ["tick", "compare", "tick"]);
});
