/** Cinema tool capability and exact-topic admission regressions. Zones: host, telegram. */
import assert from "node:assert/strict";
import test from "node:test";
import fs from "node:fs";
import { syncBuiltinESMExports } from "node:module";
import { join } from "node:path";
import { homedir } from "node:os";
// @ts-expect-error The deployed SDK host runs native JavaScript.
import { cinemaArguments, cinemaMessage, cinemaTarget, connectCinema, familyMemoryTool } from "../scripts/cinema.mjs";
import { registerTelegramDeliveryTarget, createTelegramDeliveryTargetPolicyRuntime, isTelegramDeliveryExplicitTargetAuthorized } from "../lib/delivery.ts";

test("Cinema accepts both humans only in its exact forum topic", () => {
  const message = { chat: { id: cinemaTarget.chatId, type: "supergroup" }, message_thread_id: 3,
    from: { id: 7, is_bot: false }, text: "Найди Сёгун" };
  assert.equal(cinemaMessage({ message }), message);
  assert.ok(cinemaMessage({ message: { ...message, from: { id: 8, is_bot: false } } }));
  for (const rejected of [
    { ...message, message_thread_id: 16 }, { ...message, message_thread_id: 1 },
    { ...message, chat: { id: -1007, type: "supergroup" } },
    { ...message, from: { id: 7, is_bot: true } },
    { ...message, text: "/start@other_bot" },
  ]) assert.equal(cinemaMessage({ message: rejected }), undefined);
});

test("Cinema cannot invoke shell, local file IO, browser auth or delete files", () => {
  for (const p of [
    { action: "bash", query: "id" }, { action: "auth" }, { action: "poster", itemId: "a".repeat(32) },
    { action: "download", url: "/etc/passwd" }, { action: "download", url: "file:///etc/passwd" },
    { action: "download", url: "https://127.0.0.1/private" },
    { action: "download", url: "https://rutracker.org.evil.test/file" },
    { action: "episodes", url: "/series/Shogun", season: "1;id" },
    { action: "remove", hash: "all" }, { action: "series", url: "https://evil.test/series/Test" },
  ]) assert.throws(() => cinemaArguments(p));
  assert.deepEqual(cinemaArguments({ action: "search", source: "lostfilm", query: "test; id" }),
    ["search", "--source", "lostfilm", "test; id"]);
  assert.deepEqual(cinemaArguments({ action: "remove", hash: "A".repeat(40), deleteFiles: true }), ["remove", "a".repeat(40)]);
  assert.deepEqual(cinemaArguments({ action: "episodes", season: 1, url: "/series/Shogun" }),
    ["episodes", "--season", "1", "https://www.lostfilm.tv/series/Shogun"]);
});

test("Companion delivery registration authorizes only the exact topic while registered", () => {
  const policy = createTelegramDeliveryTargetPolicyRuntime({ ownsDirect: () => true, isFollowerRegistered: () => false,
    getAllowedChatId: () => cinemaTarget.chatId, getFollowerTarget: () => undefined,
    getLeaderTarget: () => ({ chatId: cinemaTarget.chatId, threadId: 16 }), listThreadRecords: () => [],
    getActiveTurnTarget: () => undefined, getActiveGuestQueryId: () => undefined });
  assert.equal(isTelegramDeliveryExplicitTargetAuthorized(cinemaTarget, policy.getTargetPolicyView()), false);
  const dispose = registerTelegramDeliveryTarget(cinemaTarget);
  try {
    assert.equal(isTelegramDeliveryExplicitTargetAuthorized(cinemaTarget, policy.getTargetPolicyView()), true);
    assert.equal(isTelegramDeliveryExplicitTargetAuthorized({ ...cinemaTarget, threadId: 4 }, policy.getTargetPolicyView()), false);
    assert.equal(isTelegramDeliveryExplicitTargetAuthorized({ ...cinemaTarget, chatId: -1007 }, policy.getTargetPolicyView()), false);
  } finally { dispose(); }
  assert.equal(isTelegramDeliveryExplicitTargetAuthorized(cinemaTarget, policy.getTargetPolicyView()), false);
});

test("Cinema queues two participants independently of the consumed update's fence", async () => {
  const delivery = await import(new URL("../dist/lib/delivery.js", import.meta.url).href);
  const updates = await import(new URL("../dist/lib/updates.js", import.meta.url).href);
  const prompts: string[] = [], replies: string[] = [];
  let release!: () => void;
  const first = new Promise<void>(resolve => { release = resolve; });
  const runtime = { session: { sessionId: "cinema", messages: [{ role: "assistant", stopReason: "stop" }],
    async prompt(text: string) { prompts.push(text); if (prompts.length === 1) await first; },
    getLastAssistantText: () => "ответ", clearQueue() {}, async abort() {} }, async dispose() {} };
  delivery.bindTelegramDeliveryRuntime({ generation: "test", shutdown() {},
    async sendView(view: { text: string }, options: { scope: { target: unknown } }) {
      assert.deepEqual(options.scope.target, cinemaTarget); replies.push(view.text);
      return { ok: true, value: { target: cinemaTarget, messageIds: [1], generation: "test" } };
    }, async sendChatAction() { return { ok: true }; } });
  const dispose = connectCinema(runtime, () => {});
  try {
    for (const id of [7, 8]) {
      await updates.getTelegramUpdateHandlerRegistry().dispatch({ update_id: id, message: {
        chat: { id: cinemaTarget.chatId, type: "supergroup" }, message_thread_id: 3,
        from: { id, is_bot: false, first_name: `person${id}` }, text: `request${id}` } });
    }
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(prompts.length, 1);
    release();
    for (let i = 0; i < 20 && replies.length < 2; i++) await new Promise(resolve => setImmediate(resolve));
    assert.equal(prompts.length, 2);
    assert.match(prompts[0], /user=7/); assert.match(prompts[1], /user=8/);
    assert.deepEqual(replies, ["ответ", "ответ"]);
  } finally { release(); await dispose(); delivery.clearTelegramDeliveryRuntime(); }
});

test("Family memory confines writes to three files, refuses stale reads and verifies atomic publication", async t => {
  const path = join(homedir(), ".local/share/family/alex.md");
  const files = new Map([[path, "Existing preferences\n"]]);
  let mode = 0;
  t.mock.method(fs, "readFileSync", (p: string) => { if (!files.has(p)) throw new Error("Missing file"); return files.get(p); });
  t.mock.method(fs, "existsSync", (p: string) => files.has(p));
  t.mock.method(fs, "writeFileSync", (p: string, text: string, options: { mode: number; flag: string }) => {
    assert.equal(options.flag, "wx"); assert.equal(files.has(p), false); mode = options.mode; files.set(p, text);
  });
  t.mock.method(fs, "renameSync", (from: string, to: string) => { files.set(to, files.get(from)!); files.delete(from); });
  t.mock.method(fs, "unlinkSync", (p: string) => files.delete(p));
  syncBuiltinESMExports();
  try {
    for (const file of ["../auth.json", "/etc/passwd", "network-issues.md"]) {
      await assert.rejects(familyMemoryTool.execute("id", { action: "read", file }));
    }
    const read = JSON.parse((await familyMemoryTool.execute("id", { action: "read", file: "alex.md" })).content[0].text);
    await assert.rejects(familyMemoryTool.execute("id", { action: "write", file: "alex.md", content: "new", expectedSha256: "stale" }));
    const result = await familyMemoryTool.execute("id", { action: "write", file: "alex.md", content: "Updated preferences\n", expectedSha256: read.sha256 });
    assert.equal(JSON.parse(result.content[0].text).saved, true);
    assert.equal(files.get(path), "Updated preferences\n"); assert.equal(mode, 0o600);
    assert.equal(files.size, 1);
    await assert.rejects(familyMemoryTool.execute("id", { action: "write", file: "alex.md", content: "overwrite", expectedSha256: read.sha256 }));
  } finally { t.mock.restoreAll(); syncBuiltinESMExports(); }
});
