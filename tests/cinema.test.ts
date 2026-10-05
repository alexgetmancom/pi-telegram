/** Cinema tool capability and exact-topic admission regressions. Zones: host, telegram. */
import assert from "node:assert/strict";
import test from "node:test";
// @ts-expect-error The deployed SDK host runs native JavaScript.
import { cinemaArguments, cinemaMessage, cinemaTarget, connectCinema } from "../scripts/cinema.mjs";
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
