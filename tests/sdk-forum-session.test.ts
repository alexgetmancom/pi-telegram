/** Real Pi SDK session replacement through the native leader/follower bridge; Telegram is local and no model calls occur. */
import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, rmSync, existsSync, realpathSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createAgentSessionRuntime, createAgentSessionServices, createAgentSessionFromServices, SessionManager } from "@earendil-works/pi-coding-agent";
import { fork } from "node:child_process";
import telegram from "../lib/extension.ts";

test("Real SDK replaces AI then Cinema sessions and clears both handoffs", { timeout: 40000 }, async () => {
  const dir = realpathSync(mkdtempSync(join(tmpdir(), "pi-forum-sdk-")));
  const methods: string[] = [];
  const previousDir = process.env.PI_CODING_AGENT_DIR, previousFetch = globalThis.fetch;
  process.env.PI_CODING_AGENT_DIR = dir;
  let child: ReturnType<typeof fork> | undefined, cinemaId: string | undefined;
  const chatId = -1007, updates: object[] = [], sent: Record<string, unknown>[] = [];
  const runtimes: Awaited<ReturnType<typeof createAgentSessionRuntime>>[] = [];
  writeFileSync(join(dir, "telegram.json"), JSON.stringify({ profiles: { default: { botToken: "123:fixture", allowedUserId: 7, forumTarget: { chatId, threadId: 16 } } } }));
  globalThis.fetch = async (input, init) => {
    const url = String(input), method = url.split("/").at(-1)!;
    assert.ok(url.startsWith("https://api.telegram.org/"), "No external/model requests are allowed");
    methods.push(method);
    const body = init?.body ? JSON.parse(String(init.body)) : {};
    let result: unknown = true;
    if (method === "getMe") result = { id: 123, is_bot: true, username: "fixture_bot", first_name: "Fixture", has_topics_enabled: false };
    else if (method === "getUpdates") {
      await new Promise(resolve => setTimeout(resolve, 40));
      if (init?.signal?.aborted) throw new DOMException("Aborted", "AbortError");
      result = updates.splice(0);
    } else if (method === "sendMessage" || method === "editMessageText" || method === "sendRichMessage") {
      sent.push(body); result = { message_id: sent.length + 100, chat: { id: body.chat_id }, message_thread_id: body.message_thread_id, date: Math.floor(Date.now()/1000), text: body.text };
    }
    return new Response(JSON.stringify({ ok: true, result }), { status: 200, headers: { "content-type": "application/json" } });
  };
  const wait = async (predicate: () => boolean) => {
    const deadline = Date.now() + 15000;
    while (!predicate()) {
      if (Date.now() > deadline) {
        const log = join(dir, "tmp/pi-telegram/logs.jsonl");
        throw new Error(`SDK forum lifecycle timed out: ${JSON.stringify({methods: methods.slice(-12), sent, ids: runtimes.map(r => r.session.sessionId)})} ${existsSync(log) ? readFileSync(log, "utf8").slice(-6000) : "no log"}`);
      }
      await new Promise(resolve => setTimeout(resolve, 50));
    }
  };
  try {
    for (const threadId of [16]) {
      const cwd = join(dir, String(threadId)); mkdirSync(cwd);
      const runtime = await createAgentSessionRuntime(async ({ sessionManager, sessionStartEvent }) => {
        const services = await createAgentSessionServices({ cwd, agentDir: dir, resourceLoaderOptions: {
          noExtensions: true, noSkills: true, noContextFiles: true, noPromptTemplates: true, noThemes: true,
          extensionFactories: [pi => telegram(pi, { forumTarget: { chatId, threadId } })],
        } });
        return { ...await createAgentSessionFromServices({ services, sessionManager, sessionStartEvent, tools: [] }), services, diagnostics: services.diagnostics };
      }, { cwd, agentDir: dir, sessionManager: SessionManager.create(cwd, join(dir, "sessions")) });
      runtimes.push(runtime);
      const bind = async (session: typeof runtime.session) => {
        await session.bindExtensions({ mode: "rpc", commandContextActions: {
          waitForIdle: () => runtime.session.waitForIdle(), newSession: options => runtime.newSession(options),
          switchSession: (path, options) => runtime.switchSession(path, options), fork: (id, options) => runtime.fork(id, options),
          navigateTree: (id, options) => runtime.session.navigateTree(id, options), reload: () => runtime.session.reload(),
        } });
      };
      runtime.setRebindSession(bind); await bind(runtime.session);
      await runtime.session.prompt("/telegram-connect", { source: "extension" });
    }
    const cinemaCwd = join(dir, "3"); mkdirSync(cinemaCwd);
    child = fork(new URL("./fixtures/sdk-forum-child.ts", import.meta.url), [cinemaCwd, dir], { stdio: ["ignore", "pipe", "pipe", "ipc"], env: { ...process.env, PI_CODING_AGENT_DIR: dir } });
    let childOutput = "";
    child.stderr?.on("data", data => { childOutput += data; });
    child.on("message", message => { cinemaId = (message as {sessionId: string}).sessionId; });
    await wait(() => cinemaId !== undefined || child!.exitCode !== null);
    assert.equal(child.exitCode, null, childOutput);
    await wait(() => {
      const path = join(dir, "tmp/pi-telegram/state.json");
      return existsSync(path) && JSON.parse(readFileSync(path, "utf8")).profiles?.default?.runtime?.liveRoster?.busFollowers?.length === 1;
    });
    for (const [index, threadId] of [16, 3].entries()) {
      if (index === 1) await wait(() => {
        const state = JSON.parse(readFileSync(join(dir, "tmp/pi-telegram/state.json"), "utf8"));
        return state.profiles.default.runtime.liveRoster.busFollowers.length === 1;
      });
      const currentId = () => index === 0 ? runtimes[0]!.session.sessionId : cinemaId;
      const old = currentId();
      updates.push({ update_id: 1000 + index * 2, message: { message_id: 20 + index, message_thread_id: threadId,
        date: Math.floor(Date.now()/1000), chat: { id: chatId, type: "supergroup" }, from: { id: 7, is_bot: false, first_name: "Alex" }, text: "/new@fixture_bot" } });
      await wait(() => sent.some(body => body.message_thread_id === threadId && String(body.text).includes("Start a new session?")));
      const questionIndex = sent.findIndex(body => body.message_thread_id === threadId && String(body.text).includes("Start a new session?"));
      updates.push({ update_id: 1001 + index * 2, callback_query: { id: `confirm-${index}`, from: { id: 7, is_bot: false, first_name: "Alex" }, data: "new:confirm",
        message: { message_id: questionIndex + 101, message_thread_id: threadId, chat: { id: chatId, type: "supergroup" }, text: "Start a new session?" } } });
      await wait(() => currentId() !== old && sent.some(body => body.message_thread_id === threadId && String(body.text).includes("New session started")));
      const state = JSON.parse(readFileSync(join(dir, "tmp/pi-telegram/state.json"), "utf8"));
      assert.equal(state.profiles.default.workspace.sessionReplacement, undefined);
    }
    assert.equal(sent.some(body => String(body.text).includes("New session failed")), false);
  } finally {
    if (child && child.exitCode === null) {
      const exited = new Promise(resolve => child!.once("exit", resolve));
      child.send("stop"); await exited;
    }
    for (const runtime of runtimes.reverse()) await runtime.dispose();
    globalThis.fetch = previousFetch;
    if (previousDir === undefined) delete process.env.PI_CODING_AGENT_DIR; else process.env.PI_CODING_AGENT_DIR = previousDir;
    rmSync(dir, { recursive: true, force: true });
  }
});
