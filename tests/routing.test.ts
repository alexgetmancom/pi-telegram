/**
 * Regression tests for inbound Telegram route composition
 * Covers route-level wiring from paired updates into prompt queueing
 */

import { mkdir, mkdtemp, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { constants, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { createHash } from "node:crypto";
import { dirname, join } from "node:path";
import assert from "node:assert/strict";
import { withTelegramFileTransaction } from "../lib/locks.ts";
import test, { after } from "node:test";

// Production cleanup/expiry timers are unref'd so they never hold Pi open. A live Pi process keeps the loop alive;
// mirror that here so Node 22's runner does not abort tests that await those timers.
const eventLoopKeepAlive = setInterval(() => {}, 60_000);
after(() => { clearInterval(eventLoopKeepAlive); });

import * as Commands from "../lib/commands.ts";
import * as Bus from "../lib/bus.ts";
import * as Journal from "../lib/journal.ts";
import { createTelegramConfigStore } from "../lib/config.ts";
import * as Media from "../lib/media.ts";
import * as Menu from "../lib/menu.ts";
import * as Model from "../lib/model.ts";
import * as Ownership from "../lib/ownership.ts";
import * as Paths from "../lib/paths.ts";
import * as Queue from "../lib/queue.ts";
import * as Routing from "../lib/routing.ts";
import { advanceTelegramWorkspaceRestore } from "../lib/routing.ts";
import { createTelegramBusFollowerDurableAdmissionRuntime, createTelegramBusFollowerRegistrationState, createTelegramBusFollowerWorkspaceRestoreHandler,
  createTelegramBusForwardedUpdateReceiverRuntime } from "../lib/bus-follower.ts";
import { createTelegramBusProtocolIdentity, createTelegramBusWorkspaceRestoreController, getTelegramBusFollowerSocketPath,
  TELEGRAM_BUS_CAPABILITY_WORKSPACE_RESTORE } from "../lib/bus.ts";
import { withWorkspaceRestoreFixture as fixture, restoreFixtureRecipient as recipient } from "./fixtures/workspace.ts";
import * as Runtime from "../lib/runtime.ts";
import * as TextGroups from "../lib/text-groups.ts";
import * as Threads from "../lib/threads.ts";
import * as ThreadReconciler from "../lib/thread-reconciler.ts";
import * as Updates from "../lib/updates.ts";
import { createDefaultTelegramBridgeApiRuntime } from "../lib/telegram-api.ts";
import { TELEGRAM_GUEST_TURN_NOTE } from "../lib/turns.ts";
import {
  createTelegramWorkspaceAdmissionLedger,
  TelegramWorkspaceAdmissionError,
} from "../lib/workspace-admission.ts";
import { createTelegramWorkspaceOperationRuntime, createTelegramWorkspaceExternalProtectionCapture } from "../lib/workspace-retirement.ts";
import { createTelegramBusLeaderRuntime } from "../lib/bus-leader.ts";

import { createRouteHarness, type RouteHarnessOptions, type TestContext, type TestModel,
  type TestMessage, type TestCallbackQuery, type TestUpdate, type TestUser } from "./fixtures/routing.ts";

for (const mode of ["waiting", "positive", "foreign-user", "foreign-chat", "dispatched", "started", "receipt-failure"] as const) {
  test(`Standalone continue reaction keeps exact queue and durable source ownership (${mode})`, async () => {
    const dir = await mkdtemp(join(tmpdir(), "telegram-continue-reaction-"));
    let now = Date.now(), refuseSettlement = mode === "receipt-failure", discarded = 0;
    const options = { path: join(dir, "inbox.json"), botIdentity: Journal.createTelegramUpdateJournalBotIdentity({ botToken: "fixture:continue" }), getNowMs: () => now };
    const journal = Journal.createTelegramUpdateJournalStore(options), key = Journal.createTelegramUpdateJournalBindingKey(options);
    const ctx = { cwd: "/repo" };
    let worker: Updates.TelegramUpdateWorkerRuntime<TestContext>;
    const harness = createRouteHarness({ getAdmissionJournalBinding: () => key, isContextActive: () => true,
      onItemsDiscarded(items, current) {
        if (refuseSettlement) throw new Error("fixture continue settlement refused");
        assert.equal(worker.completeQueueReceipts({ receipts: items.flatMap(item => item.admissionReceipts ?? []), ctx: current, reason: "discard" }), true);
        discarded++;
      } });
    worker = Updates.createTelegramUpdateAdmissionWorkerRuntime<TestUpdate & Journal.TelegramJournaledUpdate, TestContext>({
      journal, getJournalBindingKey: () => key, hasAuthority: () => true, getNowMs: () => now,
      getQueueOwnerIdentity: () => ({ instanceId: "leader-a", processId: process.pid, processBirthId: "fixture-continue", sessionGeneration: 1 }),
      scheduleRetry() { return 0; }, cancelRetry() {},
      defaultHandle: (update, current, execution) => harness.routeRuntime.handleUpdate(update, current, execution),
    });
    const feed = async (update: TestUpdate & Journal.TelegramJournaledUpdate) => { journal.appendBatch([update]); worker.signal(); await worker.waitForDrain(); };
    const reaction = (id: number): TestUpdate & Journal.TelegramJournaledUpdate => ({ update_id: id, message_reaction: { message_id: 12,
      chat: { id: mode === "foreign-chat" ? 200 : 100, type: "private" }, user: { id: mode === "foreign-user" ? 8 : 7, is_bot: false },
      old_reaction: [], new_reaction: [{ type: "emoji", emoji: mode === "positive" ? "👍" : "👎" }] } });
    try {
      worker.start(ctx); await worker.waitForDrain();
      await feed({ update_id: 1, message: { message_id: 11, chat: { id: 100, type: "private" }, from: { id: 7, is_bot: false }, text: "unrelated prompt" } });
      await feed({ update_id: 2, message: { message_id: 12, chat: { id: 100, type: "private" }, from: { id: 7, is_bot: false }, text: "/continue" } });
      const [continuation, ordinary] = harness.telegramQueueStore.getQueuedItems();
      assert.equal(continuation?.kind, "prompt"); assert.equal(continuation?.queueLane, "control");
      assert.equal(journal.read().entries.find(entry => entry.updateId === 2)?.state, "queued", "The continuation owns an actual durable receipt");
      if (mode === "dispatched") harness.bridgeRuntime.lifecycle.setDispatchPending(true);
      else if (mode === "started") {
        assert.equal(worker.completeQueueReceipts({ receipts: continuation!.admissionReceipts!, ctx, reason: "prompt-handoff" }), true);
        harness.activeTurnRuntime.set(continuation as Queue.PendingTelegramTurn);
        harness.telegramQueueStore.setQueuedItems([ordinary!]);
      } else harness.activeTurnRuntime.set({ ...(ordinary as Queue.PendingTelegramTurn), sourceMessageIds: [99], admissionReceipts: [] });
      await feed(reaction(3));
      if (mode === "receipt-failure") {
        assert.equal(harness.telegramQueueStore.getQueuedItems()[0], continuation, "Failed settlement keeps the exact continuation waiting");
        assert.equal(journal.read().entries.find(entry => entry.updateId === 2)?.state, "queued");
        refuseSettlement = false; now += 2000; worker.signal(); await worker.waitForDrain();
      }
      const cancelled = mode === "waiting" || mode === "receipt-failure";
      assert.deepEqual(harness.telegramQueueStore.getQueuedItems(), cancelled || mode === "started" ? [ordinary] : [continuation, ordinary]);
      assert.equal(discarded, cancelled ? 1 : 0);
      if (cancelled) {
        assert.equal(journal.read().entries.some(entry => entry.updateId === 2), false);
        await feed(reaction(4)); assert.equal(discarded, 1, "Repeated dislike cannot settle the continuation twice");
      }
      assert.equal(harness.activeTurnRuntime.has(), mode !== "dispatched", "Dislike does not abort any started work");
      assert.equal(continuation?.queueLane, "control", "Reactions never demote the continuation to the normal lane");
    } finally { await worker.stop(); await rm(dir, { recursive: true, force: true }); }
  });
}

async function openRerouteSubmenu(routeRuntime: ReturnType<typeof createRouteHarness>["routeRuntime"], events: string[], rerouteId = "1"): Promise<string> {
  await routeRuntime.handleUpdate({ callback_query: { id: `open-${rerouteId}`, from: { id: 7, is_bot: false },
    message: { message_id: 99, chat: { id: 100, type: "private" } }, data: `reroutemenu:${rerouteId}` } }, { cwd: "/repo" });
  return events.filter(event => event.startsWith("markup:")).at(-1) ?? "";
}

function rerouteFollowerOwnership(instanceId: string) {
  return { instanceId, ownerGeneration: "registered", recipientBindingKey: `manual:${instanceId}`,
    protocolIdentity: Bus.createTelegramBusProtocolIdentity({ runtimeBuild: "fixture",
      capabilities: [Bus.TELEGRAM_BUS_CAPABILITY_DURABLE_FOLLOWER_ADMISSION] }) };
}

function acceptedForeignUpdateSettlement(sourceUpdateId = 1) {
  return {
    status: "accepted" as const,
    delivery: {
      deliveryId: `test-delivery-${sourceUpdateId}`,
      sourceUpdateId,
      recipientBindingKey: "test-recipient",
    },
  };
}

function retryableForeignUpdateSettlement() {
  return {
    status: "retryable" as const,
    failureClass: "acknowledgement-rejected" as const,
    message: "retry",
  };
}

test("Inbound bus projection owns target authority and local labels", () => {
  const follower = {
    instanceId: "follower-a",
    profileKey: "manual:follower-a",
    connectedAtMs: 1,
    lastHeartbeatMs: 2,
    registrationGeneration: "registration-a",
    protocol: {
      protocolVersion: 1 as const,
      runtimeBuild: "0.28.0",
      capabilities: ["durable-follower-admission-v1"],
    },
    target: { chatId: 7, threadId: 11 },
  };
  const runtime = Routing.createTelegramInboundBusProjectionRuntime({
    instanceId: "leader",
    listFollowers: () => [follower],
    listThreadRecords: () => [],
    getLeaderTarget: () => ({ chatId: 7, threadId: 10 }),
    isFollowerRegistered: () => true,
    getFollowerTarget: () => ({ chatId: 7, threadId: 11 }),
    getCurrentIdentity: (target) => ({
      target,
      slot: "C",
      threadName: "Cedar",
    }),
  });

  assert.deepEqual(
    runtime.getTargetOwnership({ chatId: 7, threadId: 11 }),
    {
      instanceId: "follower-a",
      ownerGeneration: "registration-a",
      recipientBindingKey: "manual:follower-a",
      protocolIdentity: follower.protocol,
    },
  );
  assert.deepEqual(runtime.getLiveThreadTargets(), [
    { chatId: 7, threadId: 10 },
    { chatId: 7, threadId: 11 },
  ]);
  assert.equal(
    runtime.getLocalThreadLabelForTarget({ chatId: 7, threadId: 11 }),
    "Cedar",
  );
  assert.equal(
    runtime.getLocalThreadLabelForTarget({ chatId: 7, threadId: 99 }),
    undefined,
  );
});

test("Routing runtime forwards authorized text and preserves slash-menu targets", async () => {
  const events: string[] = [];
  const menuTargets: unknown[] = [];
  const model: TestModel = { provider: "test", id: "model" };
  const bridgeRuntime = Runtime.createTelegramBridgeRuntime();
  const activeTurnRuntime = Queue.createTelegramActiveTurnStore();
  const telegramQueueStore = Queue.createTelegramQueueStore<TestContext>();
  const queueMutationRuntime = Queue.createTelegramQueueMutationController({
    ...telegramQueueStore,
    updateStatus: () => events.push("status"),
  });
  const pendingModelSwitchStore =
    Model.createPendingModelSwitchStore<Model.ScopedTelegramModel<TestModel>>();
  const currentModelRuntime = Model.createCurrentModelRuntime<
    TestContext,
    TestModel
  >({
    getContextModel: () => model,
    updateStatus: () => events.push("status"),
  });
  const modelSwitchController =
    Model.createTelegramModelSwitchControllerRuntime<
      TestContext,
      Model.ScopedTelegramModel<TestModel>
    >({
      isIdle: () => true,
      getPendingModelSwitch: pendingModelSwitchStore.get,
      setPendingModelSwitch: pendingModelSwitchStore.set,
      getActiveTurn: activeTurnRuntime.get,
      getAbortHandler: bridgeRuntime.abort.getHandler,
      hasAbortHandler: bridgeRuntime.abort.hasHandler,
      getActiveToolExecutions: bridgeRuntime.lifecycle.getActiveToolExecutions,
      allocateItemOrder: bridgeRuntime.queue.allocateItemOrder,
      allocateControlOrder: bridgeRuntime.queue.allocateControlOrder,
      appendQueuedItem: queueMutationRuntime.append,
      updateStatus: () => events.push("status"),
    });
  const menuActions: Menu.TelegramMenuActionRuntime<TestContext, TestModel> = {
    updateModelMenuMessage: async () => undefined,
    updateThinkingMenuMessage: async () => undefined,
    updateStatusMessage: async () => undefined,
    sendStatusMessage: async () => {
      events.push("status-menu");
    },
    openModelMenu: async () => {
      events.push("model-menu");
    },
    openThinkingMenu: async (chatId, messageId, _ctx, threadId) => {
      menuTargets.push({ command: "thinking", chatId, messageId, threadId });
      events.push("thinking-menu");
    },
  };
  const routeRuntime = Routing.createTelegramInboundRouteRuntime<
    TestUpdate,
    TestMessage,
    TestCallbackQuery,
    TestContext,
    TestModel
  >({
    configStore: {
      get: () => ({}),
      getAllowedUserId: () => 7,
      persistAllowedUserId: async () => true,
      persist: async () => undefined,
    },
    bridgeRuntime,
    activeTurnRuntime,
    mediaGroupRuntime: Media.createTelegramMediaGroupController<
      TestMessage,
      TestContext
    >(),
    textGroupRuntime: TextGroups.createTelegramTextGroupController<
      TestMessage,
      TestContext
    >({ forwardCommentWaitMs: false }),
    telegramQueueStore,
    queueMutationRuntime,
    modelMenuRuntime: Menu.createTelegramModelMenuRuntime<TestModel>(),
    currentModelRuntime,
    modelSwitchController,
    menuActions,
    openQueueMenu: async (chatId, messageId, _ctx, threadId) => {
      menuTargets.push({ command: "queue", chatId, messageId, threadId });
    },
    openSettingsMenu: async (chatId, messageId, _ctx, threadId) => {
      menuTargets.push({ command: "settings", chatId, messageId, threadId });
    },
    queueMenuCallbackHandler: async () => false,
    inboundHandlerRuntime: {
      process: async (files, rawText) => ({
        rawText,
        promptFiles: files,
        handlerOutputs: [],
        handledFiles: [],
      }),
    },
    updateStatus: () => events.push("status"),
    dispatchNextQueuedTelegramTurn: () => events.push("dispatch"),
    requestDeferredDispatchNextQueuedTelegramTurn: (dispatch) => {
      events.push("deferred-dispatch");
      dispatch({ cwd: "/deferred" });
    },
    answerCallbackQuery: async (callbackQueryId) => {
      events.push(`answer:${callbackQueryId}`);
    },
    answerGuestQuery: async () => {},
    sendTextReply: async (_chatId, _replyToMessageId, text) => {
      events.push(`reply:${text}`);
      return undefined;
    },
    setMyCommands: async () => undefined,
    getCommands: () => [],
    downloadFile: async (_fileId, fileName) => `/tmp/${fileName}`,
    getThinkingLevel: () => "high",
    setThinkingLevel: () => undefined,
    setModel: async () => true,
    sendUserMessage: (message, options) => {
      events.push(`user:${message}:${options?.deliverAs ?? "default"}`);
    },
    isIdle: () => true,
    hasPendingMessages: () => false,
    compact: () => undefined,
    recordRuntimeEvent: (category, error) => {
      events.push(
        `event:${category}:${error instanceof Error ? error.message : String(error)}`,
      );
    },
  });
  await routeRuntime.handleUpdate(
    {
      message: {
        message_id: 11,
        chat: { id: 100, type: "private" },
        from: { id: 7, is_bot: false },
        text: "hello from telegram",
      },
    },
    { cwd: "/repo" },
  );
  const [queued] = telegramQueueStore.getQueuedItems();
  assert.equal(queued?.kind, "prompt");
  assert.equal(queued?.statusSummary, "hello from telegram");
  assert.equal(
    queued?.content[0]?.type === "text" ? queued.content[0].text : "",
    "[telegram] hello from telegram",
  );
  assert.deepEqual(events, [
    "status",
    "dispatch",
    "deferred-dispatch",
    "dispatch",
  ]);
  bridgeRuntime.lifecycle.setFoldQueuedPromptsIntoHistory(true);
  await routeRuntime.handleUpdate(
    {
      message: {
        message_id: 12,
        chat: { id: 100, type: "private" },
        from: { id: 7, is_bot: false },
        text: "/continue",
      },
    },
    { cwd: "/repo" },
  );
  const queuedAfterContinue = telegramQueueStore.getQueuedItems();
  const [continueTurn, originalTurn] = queuedAfterContinue;
  assert.equal(queuedAfterContinue.length, 2);
  assert.equal(continueTurn?.kind, "prompt");
  assert.equal(continueTurn?.queueLane, "control");
  assert.equal(continueTurn?.statusSummary, "continue");
  assert.equal(
    continueTurn?.content[0]?.type === "text"
      ? continueTurn.content[0].text
      : "",
    "[telegram] continue",
  );
  assert.equal(continueTurn?.historyText, "continue");
  assert.equal(originalTurn?.kind, "prompt");
  assert.equal(originalTurn?.statusSummary, "hello from telegram");
  assert.equal(
    originalTurn?.kind === "prompt" && originalTurn.content[0]?.type === "text"
      ? originalTurn.content[0].text
      : "",
    "[telegram] hello from telegram",
  );
  assert.equal(
    bridgeRuntime.lifecycle.shouldFoldQueuedPromptsIntoHistory(),
    false,
  );
  const disposeFailingCommand = Commands.registerTelegramCommand({
    name: "fail",
    handler: () => {
      throw new Error("boom");
    },
  });
  await routeRuntime.handleUpdate(
    {
      message: {
        message_id: 13,
        chat: { id: 100, type: "private" },
        from: { id: 7, is_bot: false },
        text: "/fail now",
      },
    },
    { cwd: "/repo" },
  );
  disposeFailingCommand();
  assert.equal(events.includes("event:telegram-command:boom"), true);
  assert.equal(events.includes("reply:Command failed."), true);
  assert.equal(telegramQueueStore.getQueuedItems().length, 2);
  await routeRuntime.handleUpdate(
    {
      callback_query: {
        id: "cb-custom",
        from: { id: 7, is_bot: false },
        data: "vividfish:approve:123",
        message: {
          message_id: 13,
          chat: { id: 100, type: "private" },
          from: { id: 7, is_bot: false },
        },
      },
    },
    { cwd: "/repo" },
  );
  const ownedCallbackData = [
    "tgbtn:expired",
    "menu:model",
    "model:pick:0",
    "thinking:set:high",
    "status:model",
    "queue:list",
    "allmenu:start:7",
    "reroute:missing:7",
    "reroutecancel:missing",
    "reroutecancel:malformed:extra",
  ];
  for (const [index, data] of ownedCallbackData.entries()) {
    await routeRuntime.handleUpdate(
      {
        callback_query: {
          id: `cb-owned-${index}`,
          from: { id: 7, is_bot: false },
          data,
          message: {
            message_id: 14 + index,
            chat: { id: 100, type: "private" },
            from: { id: 7, is_bot: false },
          },
        },
      },
      { cwd: "/repo" },
    );
  }
  const callbackTurn = telegramQueueStore
    .getQueuedItems()
    .find((item) => item.statusSummary === "vividfish:approve:123");
  assert.equal(callbackTurn?.kind, "prompt");
  assert.equal(callbackTurn?.queueLane, "priority");
  assert.deepEqual(
    callbackTurn?.kind === "prompt" ? callbackTurn.content : undefined,
    [{ type: "text", text: "[callback] vividfish:approve:123" }],
  );
  assert.equal(events.includes("answer:cb-custom"), true);
  for (const data of ownedCallbackData) {
    assert.equal(
      events.some((event) => event.startsWith(`user:[callback] ${data}:`)),
      false,
    );
  }
  menuTargets.length = 0;
  for (const command of ["settings", "thinking", "queue"]) {
    await routeRuntime.handleUpdate({ message: {
      message_id: 81, chat: { id: -1007, type: "supergroup" },
      from: { id: 7, is_bot: false }, message_thread_id: 16, text: `/${command}`,
    } }, { cwd: "/repo" });
  }
  await new Promise<void>((resolve) => setImmediate(resolve));
  assert.deepEqual(menuTargets, ["settings", "thinking", "queue"].map(command => ({
    command, chatId: -1007, messageId: 81, threadId: 16,
  })));
});

test("Routing executes bound generated-button actions before queue admission", async () => {
  const invoked: string[] = [];
  const { buttonActionStore, events, routeRuntime, telegramQueueStore } =
    createRouteHarness({
      invokeBoundButtonAction: async (action) => {
        invoked.push(action.prompt);
        return action.prompt.includes("::") ? "new" : false;
      },
    });
  const callbackData = buttonActionStore.register({
    text: "Next",
    prompt: "music::next",
  });
  await routeRuntime.handleUpdate(
    {
      callback_query: {
        id: "cb-bound",
        from: { id: 7, is_bot: false },
        data: callbackData,
        message: {
          message_id: 42,
          chat: { id: 100, type: "private" },
          from: { id: 7, is_bot: false },
        },
      },
    },
    { cwd: "/repo" },
  );
  assert.deepEqual(invoked, ["music::next"]);
  assert.equal(telegramQueueStore.getQueuedItems().length, 0);
  assert.equal(events.includes("answer:Done."), true);
  assert.equal(events.includes("dispatch"), false);
});

test("Private Thread first contact cannot reach fallback dispatch without pairing publication", async () => {
  for (const failure of ["throw", "deny"] as const) {
    let publications = 0;
    const { routeRuntime, telegramQueueStore, events } = createRouteHarness({
      configStore: {
        get: () => ({}), getAllowedUserId: () => undefined,
        persist: async () => undefined,
        async persistAllowedUserId() {
          publications++;
          if (failure === "throw") throw new Error("pairing publication failed");
          return false;
        },
      },
    });
    const handle = () => routeRuntime.handleUpdate({ message: {
      message_id: 13, message_thread_id: 42,
      chat: { id: 7, type: "private" }, from: { id: 7, is_bot: false }, text: "unpaired prompt",
    } }, { cwd: "/repo" });
    if (failure === "throw") await assert.rejects(handle(), /pairing publication failed/);
    else await handle();
    assert.equal(publications, 1);
    assert.deepEqual(telegramQueueStore.getQueuedItems(), []);
    assert.equal(events.some((event) => event === "dispatch" || event.startsWith("user:")), false);
  }
});

test("Routing admission returns exact outcomes and places priority callbacks first", async () => {
  const { routeRuntime, telegramQueueStore } = createRouteHarness();
  const handle = Updates.createTelegramUpdateAdmissionHandle<
    TestUpdate & { update_id: number },
    TestContext
  >({
    registry: {
      version: 1,
      add: () => () => {},
      dispatch: async () => "pass",
    },
    defaultHandle: routeRuntime.handleUpdate,
  });
  const signal = new AbortController().signal;
  const messageOutcome = await handle(
    {
      update_id: 71,
      message: {
        message_id: 11,
        chat: { id: 100, type: "private" },
        from: { id: 7, is_bot: false },
        text: "journaled prompt",
      },
    },
    { cwd: "/repo" },
    signal,
  );
  const callbackOutcome = await handle(
    {
      update_id: 72,
      callback_query: {
        id: "callback-72",
        from: { id: 7, is_bot: false },
        data: "companion:approve",
        message: {
          message_id: 12,
          chat: { id: 100, type: "private" },
          from: { id: 7, is_bot: false },
        },
      },
    },
    { cwd: "/repo" },
    signal,
  );

  assert.equal(messageOutcome.kind, "queued");
  assert.deepEqual(
    messageOutcome.kind === "queued"
      ? messageOutcome.sourceUpdateIds
      : undefined,
    [71],
  );
  assert.equal(callbackOutcome.kind, "queued");
  assert.deepEqual(
    callbackOutcome.kind === "queued"
      ? callbackOutcome.sourceUpdateIds
      : undefined,
    [72],
  );
  assert.deepEqual(
    telegramQueueStore
      .getQueuedItems()
      .flatMap((item) => item.admissionReceipts ?? [])
      .map((receipt) => receipt.sourceUpdateIds),
    [[72], [71]],
  );
});

test("Routing admission rejects stale queue commit after asynchronous file download", async () => {
  const controller = new AbortController();
  let inboundHandlerCalls = 0;
  const { routeRuntime, telegramQueueStore } = createRouteHarness({
    downloadFile: async (_fileId, fileName) => {
      controller.abort();
      return `/tmp/${fileName}`;
    },
    processInbound: async (files, rawText) => {
      inboundHandlerCalls += 1;
      return {
        rawText,
        promptFiles: files,
        handlerOutputs: [],
        handledFiles: [],
      };
    },
  });
  const handle = Updates.createTelegramUpdateAdmissionHandle<
    TestUpdate & { update_id: number },
    TestContext
  >({
    registry: {
      version: 1,
      add: () => () => {},
      dispatch: async () => "pass",
    },
    defaultHandle: routeRuntime.handleUpdate,
  });

  await assert.rejects(
    handle(
      {
        update_id: 73,
        message: {
          message_id: 13,
          chat: { id: 100, type: "private" },
          from: { id: 7, is_bot: false },
          document: { file_id: "doc-73", file_name: "stale.txt" },
        },
      },
      { cwd: "/repo" },
      controller.signal,
    ),
    /Abort/u,
  );
  assert.equal(inboundHandlerCalls, 0);
  assert.deepEqual(telegramQueueStore.getQueuedItems(), []);
});

test("Routing admission rejects stale queue commit after asynchronous inbound handler", async () => {
  const controller = new AbortController();
  const { routeRuntime, telegramQueueStore } = createRouteHarness({
    processInbound: async (files, rawText) => {
      controller.abort();
      return {
        rawText,
        promptFiles: files,
        handlerOutputs: [],
        handledFiles: [],
      };
    },
  });
  const handle = Updates.createTelegramUpdateAdmissionHandle<
    TestUpdate & { update_id: number },
    TestContext
  >({
    registry: {
      version: 1,
      add: () => () => {},
      dispatch: async () => "pass",
    },
    defaultHandle: routeRuntime.handleUpdate,
  });

  await assert.rejects(
    handle(
      {
        update_id: 74,
        message: {
          message_id: 14,
          chat: { id: 100, type: "private" },
          from: { id: 7, is_bot: false },
          text: "stale handler",
        },
      },
      { cwd: "/repo" },
      controller.signal,
    ),
    /Abort/u,
  );
  assert.deepEqual(telegramQueueStore.getQueuedItems(), []);
});

test("Routing admission defers media groups then reports one exact late receipt", async () => {
  const timers: Array<{
    callback: () => void;
    cleared: boolean;
  }> = [];
  const mediaGroupRuntime = Media.createTelegramMediaGroupController<
    TestMessage,
    TestContext
  >({
    setTimer: (callback) => {
      const timer = { callback, cleared: false };
      timers.push(timer);
      return timer as unknown as ReturnType<typeof setTimeout>;
    },
    clearTimer: (timer) => {
      (timer as unknown as { cleared: boolean }).cleared = true;
    },
  });
  const { routeRuntime, telegramQueueStore } = createRouteHarness({
    mediaGroupRuntime,
  });
  const lateOutcomes: Array<{
    outcome: Updates.TelegramUpdateAdmissionOutcome;
    updateId: number;
  }> = [];
  const lateErrors: unknown[] = [];
  const handle = Updates.createTelegramUpdateAdmissionHandle<
    TestUpdate & { update_id: number },
    TestContext
  >({
    registry: {
      version: 1,
      add: () => () => {},
      dispatch: async () => "pass",
    },
    defaultHandle: routeRuntime.handleUpdate,
    onLateOutcome: (outcome, details) => {
      lateOutcomes.push({ outcome, updateId: details.updateId });
    },
    onLateOutcomeError: (error) => lateErrors.push(error),
  });
  const signal = new AbortController().signal;
  const first = await handle(
    {
      update_id: 81,
      message: {
        message_id: 21,
        media_group_id: "album-a",
        chat: { id: 100, type: "private" },
        from: { id: 7, is_bot: false },
        caption: "first",
      },
    },
    { cwd: "/repo" },
    signal,
  );
  const second = await handle(
    {
      update_id: 82,
      message: {
        message_id: 22,
        media_group_id: "album-a",
        chat: { id: 100, type: "private" },
        from: { id: 7, is_bot: false },
        caption: "second",
      },
    },
    { cwd: "/repo" },
    signal,
  );
  assert.deepEqual(first, { kind: "deferred" });
  assert.deepEqual(second, { kind: "deferred" });

  timers.findLast((timer) => !timer.cleared)?.callback();
  await new Promise<void>((resolve) => setImmediate(resolve));
  await new Promise<void>((resolve) => setImmediate(resolve));

  assert.equal(telegramQueueStore.getQueuedItems().length, 1);
  assert.deepEqual(
    telegramQueueStore.getQueuedItems()[0]?.admissionReceipts?.map(
      (receipt) => receipt.sourceUpdateIds,
    ),
    [[81, 82]],
  );
  assert.deepEqual(
    lateOutcomes.map(({ outcome, updateId }) => ({
      updateId,
      kind: outcome.kind,
      sourceUpdateIds:
        outcome.kind === "queued" ? outcome.sourceUpdateIds : undefined,
    })),
    [
      { updateId: 81, kind: "queued", sourceUpdateIds: [81, 82] },
      { updateId: 82, kind: "queued", sourceUpdateIds: [81, 82] },
    ],
  );
  assert.deepEqual(lateErrors, []);
});

async function withTopicStore<T>(
  run: (
    store: Threads.TelegramTopicTargetStore,
    path: string,
  ) => Promise<T>,
): Promise<T> {
  const dir = await mkdtemp(join(tmpdir(), "pi-telegram-routing-"));
  try {
    const path = join(dir, "telegram-targets.json");
    const store = Threads.createTelegramTopicTargetStore({
      path,
      getNowMs: () => 2000,
    });
    return await run(store, path);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

function unboundTopicUpdate(text = "hello"): TestUpdate {
  return {
    message: {
      message_id: 11,
      message_thread_id: 42,
      chat: { id: 100, type: "private" },
      from: { id: 7, is_bot: false },
      text,
    },
  };
}

test("Routing runtime silently completes journal replay from a confirmed deleted thread", async () => {
  await withTopicStore(async (threadStore, path) => {
    threadStore.upsert({
      profileKey: "old",
      target: { chatId: 100, threadId: 42 },
      status: "active",
      createdAtMs: 1000,
      updatedAtMs: 1000,
      instanceId: "old-leader",
    });
    threadStore.markStaleByTarget(
      { chatId: 100, threadId: 42 },
      "deleted",
    );
    await threadStore.persist();
    const snapshot = JSON.parse(await readFile(path, "utf8")) as {
      threads?: unknown[];
    };
    snapshot.threads = [];
    await writeFile(path, `${JSON.stringify(snapshot)}\n`, "utf8");
    await threadStore.load();
    const apiCalls: unknown[] = [];
    const { routeRuntime, telegramQueueStore } = createRouteHarness({
      threadStore,
      callApi: async (method, body) => {
        apiCalls.push({ method, body });
        return {} as never;
      },
    });

    await routeRuntime.handleUpdate(unboundTopicUpdate("replayed"), {
      cwd: "/repo",
    });

    assert.deepEqual(apiCalls, []);
    assert.deepEqual(telegramQueueStore.getQueuedItems(), []);
  });
});

test("Routing runtime binds the first unbound thread to the leader without visible rename when leader has no active thread", async () => {
  await withTopicStore(async (threadStore) => {
    const apiCalls: unknown[] = [];
    const nowMs = Date.now();
    threadStore.upsert({
      profileKey: "cwd:/repo",
      owner: { kind: "leader", cwd: "/repo", instanceId: "old-leader" },
      target: { chatId: 100, threadId: 9 },
      status: "starting",
      createdAtMs: nowMs,
      updatedAtMs: nowMs,
      slot: "A",
      threadName: "Axial",
    });
    await threadStore.persist();
    const { routeRuntime, telegramQueueStore } = createRouteHarness({
      threadStore,
      callApi: async (method, body) => {
        apiCalls.push({ method, body });
        return {} as never;
      },
    });

    await routeRuntime.handleUpdate(unboundTopicUpdate("test"), {
      cwd: "/repo",
    });

    const record = threadStore.getByProfileKey("cwd:/repo");
    assert.equal(record?.status, "active");
    assert.equal(record?.instanceId, "leader-a");
    assert.equal(record?.slot, "A");
    assert.equal(record?.threadName, "Axial");
    assert.deepEqual(record?.target, { chatId: 100, threadId: 42 });
    assert.deepEqual(apiCalls, []);
    assert.equal(telegramQueueStore.getQueuedItems().length, 1);
  });
});

test("Routing runtime fails closed when retained Workspaces occupy every global slot", async () => {
  await withTopicStore(async (threadStore) => {
    for (const [index, slot] of Array.from("ABCDEFGHIJKLMNOPQRSTUVWXYZ").entries()) {
      const identity = Threads.createTelegramWorkspaceBindingIdentity(`/retained/${index}`);
      assert.ok(identity);
      threadStore.upsertWorkspaceBinding({
        ...identity,
        target: { chatId: 100, threadId: 100 + index },
        slot,
        updatedAtMs: index + 1,
      });
    }
    await threadStore.persist();
    const apiCalls: unknown[] = [];
    const { events, routeRuntime, telegramQueueStore } = createRouteHarness({
      threadStore,
      callApi: async (method, body) => {
        apiCalls.push({ method, body });
        return {} as never;
      },
    });

    await routeRuntime.handleUpdate(unboundTopicUpdate("blocked"), {
      cwd: "/repo",
    });

    assert.deepEqual(threadStore.list(), []);
    assert.equal(threadStore.listWorkspaceBindings().length, 26);
    assert.deepEqual(telegramQueueStore.getQueuedItems(), []);
    assert.deepEqual(apiCalls, []);
    assert.equal(events.includes(
      "reply:No Telegram instance slot is available. Automatic reclamation is disabled for safety.",
    ), true);
  });
});

test("Routing runtime invalidates a proven-stale slotless leader at full capacity", async () => {
  await withTopicStore(async (threadStore) => {
    threadStore.upsert({
      profileKey: "cwd:/repo",
      owner: { kind: "leader", cwd: "/repo", instanceId: "leader-a" },
      target: { chatId: 100, threadId: 7 },
      status: "active",
      createdAtMs: 1,
      updatedAtMs: 2,
      instanceId: "leader-a",
    });
    for (const [index, slot] of Array.from("ABCDEFGHIJKLMNOPQRSTUVWXYZ").entries()) {
      const identity = Threads.createTelegramWorkspaceBindingIdentity(`/retained/${index}`);
      assert.ok(identity);
      threadStore.upsertWorkspaceBinding({
        ...identity,
        target: { chatId: 100, threadId: 100 + index },
        slot,
        updatedAtMs: index + 1,
      });
    }
    await threadStore.persist();
    const apiCalls: string[] = [];
    const { events, routeRuntime, telegramQueueStore } = createRouteHarness({
      threadStore,
      callApi: async (method) => {
        apiCalls.push(method);
        throw new Error(
          "Telegram API sendChatAction failed: HTTP 400: Bad Request: message thread not found",
        );
      },
    });

    await routeRuntime.handleUpdate(unboundTopicUpdate("blocked"), { cwd: "/repo" });

    assert.equal(threadStore.getByProfileKey("cwd:/repo"), undefined);
    assert.equal(threadStore.listSyncObservations().some((observation) =>
      observation.target.chatId === 100 && observation.target.threadId === 7 &&
      observation.syncStatus === "deleted",
    ), true);
    assert.deepEqual(apiCalls, ["sendChatAction"]);
    assert.deepEqual(telegramQueueStore.getQueuedItems(), []);
    assert.equal(events.includes(
      "reply:No Telegram instance slot is available. Automatic reclamation is disabled for safety.",
    ), true);
  });
});

test("Routing runtime assigns internal baked name without visibly renaming unnamed leader startup topic", async () => {
  await withTopicStore(async (threadStore) => {
    const apiCalls: unknown[] = [];
    threadStore.upsert({
      profileKey: "cwd:/repo",
      owner: { kind: "leader", cwd: "/repo", instanceId: "old-leader" },
      target: { chatId: 100, threadId: 9 },
      status: "starting",
      createdAtMs: 1000,
      updatedAtMs: 1000,
      slot: "A",
    });
    await threadStore.persist();
    const { routeRuntime, telegramQueueStore } = createRouteHarness({
      threadStore,
      callApi: async (method, body) => {
        apiCalls.push({ method, body });
        return {} as never;
      },
    });

    await routeRuntime.handleUpdate(unboundTopicUpdate("test"), {
      cwd: "/repo",
    });

    const record = threadStore.getByProfileKey("cwd:/repo");
    assert.equal(record?.status, "active");
    assert.equal(record?.slot, "A");
    assert.equal(record?.threadName, "Anchor");
    assert.deepEqual(apiCalls, []);
    const queued = telegramQueueStore.getQueuedItems()[0];
    assert.equal(
      queued?.kind === "prompt" && queued.content[0]?.type === "text"
        ? queued.content[0].text
        : "",
      "[telegram|thread:Anchor] test",
    );
  });
});

test("Routing runtime restores stale leader thread identity internally without visible rename", async () => {
  await withTopicStore(async (threadStore) => {
    const apiCalls: unknown[] = [];
    threadStore.upsert({
      profileKey: "cwd:/repo",
      owner: { kind: "leader", cwd: "/repo", instanceId: "old-leader" },
      target: { chatId: 100, threadId: 9 },
      status: "active",
      createdAtMs: 1000,
      updatedAtMs: 1000,
      instanceId: "old-leader",
      slot: "A",
      threadName: "Axial",
    });
    threadStore.markStaleByTarget(
      { chatId: 100, threadId: 9 },
      "deleted",
      "manual close",
    );
    await threadStore.persist();
    const { routeRuntime } = createRouteHarness({
      threadStore,
      callApi: async (method, body) => {
        apiCalls.push({ method, body });
        return {} as never;
      },
    });

    await routeRuntime.handleUpdate(unboundTopicUpdate("test"), {
      cwd: "/repo",
    });

    const record = threadStore.getByProfileKey("cwd:/repo");
    assert.equal(record?.slot, "A");
    assert.equal(record?.threadName, "Axial");
    assert.deepEqual(record?.target, { chatId: 100, threadId: 42 });
    assert.deepEqual(apiCalls, []);
  });
});

test("Routing runtime assigns internal baked name when restoring unnamed stale prior leader", async () => {
  await withTopicStore(async (threadStore) => {
    const apiCalls: unknown[] = [];
    threadStore.upsert({
      profileKey: "cwd:/repo",
      owner: { kind: "leader", cwd: "/repo", instanceId: "old-leader" },
      target: { chatId: 100, threadId: 9 },
      status: "active",
      createdAtMs: 1000,
      updatedAtMs: 1000,
      instanceId: "old-leader",
      slot: "A",
    });
    threadStore.markStaleByTarget(
      { chatId: 100, threadId: 9 },
      "deleted",
      "manual close",
    );
    await threadStore.persist();
    const { routeRuntime, telegramQueueStore } = createRouteHarness({
      threadStore,
      callApi: async (method, body) => {
        apiCalls.push({ method, body });
        return {} as never;
      },
    });

    await routeRuntime.handleUpdate(unboundTopicUpdate("test"), {
      cwd: "/repo",
    });

    const record = threadStore.getByProfileKey("cwd:/repo");
    assert.equal(record?.slot, "A");
    assert.equal(record?.threadName, "Anchor");
    assert.deepEqual(record?.target, { chatId: 100, threadId: 42 });
    assert.deepEqual(apiCalls, []);
    const queued = telegramQueueStore.getQueuedItems()[0];
    assert.equal(
      queued?.kind === "prompt" && queued.content[0]?.type === "text"
        ? queued.content[0].text
        : "",
      "[telegram|thread:Anchor] test",
    );
  });
});

test("Routing runtime serves an active leader topic locally", async () => {
  await withTopicStore(async (threadStore) => {
    const apiCalls: unknown[] = [];
    threadStore.upsert({
      profileKey: "cwd:/repo",
      target: { chatId: 100, threadId: 42 },
      status: "active",
      createdAtMs: 1000,
      updatedAtMs: 1000,
      instanceId: "leader-a",
      slot: "A",
      threadName: "Axial",
    });
    await threadStore.persist();
    const { routeRuntime, telegramQueueStore } = createRouteHarness({
      threadStore,
      callApi: async (method, body) => {
        apiCalls.push({ method, body });
        return {} as never;
      },
    });

    await routeRuntime.handleUpdate(unboundTopicUpdate("second"), {
      cwd: "/repo",
    });

    assert.deepEqual(apiCalls, []);
    const record = threadStore.getByProfileKey("cwd:/repo");
    assert.equal(typeof record?.rerouteConfirmedAtMs, "number");
    const queued = telegramQueueStore.getQueuedItems()[0];
    assert.equal(queued?.kind, "prompt");
    assert.equal(
      queued?.kind === "prompt" && queued.content[0]?.type === "text"
        ? queued.content[0].text
        : "",
      "[telegram|thread:Axial] second",
    );
  });
});

test("Routing consumes the exact-target name dialog without agent dispatch", async () => {
  await withTopicStore(async (threadStore) => {
    threadStore.upsert({
      profileKey: "cwd:/repo",
      target: { chatId: 100, threadId: 42 },
      status: "active",
      createdAtMs: 1,
      updatedAtMs: 1,
      instanceId: "leader-a",
      slot: "A",
      threadName: "Anchor",
    });
    threadStore.upsertWorkspaceBinding({
      ...Threads.createTelegramWorkspaceBindingIdentity("/repo")!,
      target: { chatId: 100, threadId: 42 },
      slot: "A",
      threadName: "Anchor",
      updatedAtMs: 1,
    });
    await threadStore.persist();
    const renamed: string[] = [];
    const editedDialogs: string[] = [];
    let resets = 0;
    const { routeRuntime, events, telegramQueueStore } = createRouteHarness({
      editInteractiveMessage: async (_chatId, _messageId, text) => {
        editedDialogs.push(text);
      },
      threadStore,
      validateThreadName: (name) => name === "bad" ? "Invalid name." : undefined,
      renameCurrentThread: async (target, name) => {
        renamed.push(name);
        threadStore.renameByTarget(target, name);
        return { ok: true, threadName: name };
      },
      resetCurrentThreadName: async () => {
        resets++;
        return { ok: true, threadName: "A" };
      },
    });
    await routeRuntime.handleUpdate(unboundTopicUpdate("/name"), { cwd: "/repo" });
    assert.equal(events.some((event) => event.includes("Send a Thread name using printable ASCII")), true);
    assert.equal(events.some((event) => event.includes("Enter name")), false);
    assert.equal(events.some((event) => event.includes("Reset to automatic")), false);
    await routeRuntime.handleUpdate(unboundTopicUpdate("Navigator"), { cwd: "/repo" });
    assert.deepEqual(renamed, ["Navigator"]);
    assert.equal(telegramQueueStore.getQueuedItems().length, 0);
    const beforeSecondDialog = events.length;
    await routeRuntime.handleUpdate(unboundTopicUpdate("/name"), { cwd: "/repo" });
    const secondDialogEvents = events.slice(beforeSecondDialog);
    assert.equal(
      secondDialogEvents.some((event) => event.includes("Reset to automatic")),
      true,
    );
    assert.equal(
      secondDialogEvents.some((event) => event.includes(
        '"inline_keyboard":[[{"text":"↩️ Reset to automatic","callback_data":"thread-name:reset"}],[{"text":"✖ Cancel rename","callback_data":"thread-name:cancel"}]]',
      )),
      true,
    );
    await routeRuntime.handleUpdate(unboundTopicUpdate("A"), { cwd: "/repo" });
    assert.equal(resets, 1);
    threadStore.renameByTarget({ chatId: 100, threadId: 42 }, "Navigator");
    await routeRuntime.handleUpdate(unboundTopicUpdate("/name"), { cwd: "/repo" });
    await routeRuntime.handleUpdate({ callback_query: {
      id: "name-reset", from: { id: 7, is_bot: false }, data: "thread-name:reset",
      message: { message_id: 99, message_thread_id: 42,
        chat: { id: 100, type: "private" } },
    } }, { cwd: "/repo" });
    assert.equal(resets, 2);
    await routeRuntime.handleUpdate({ callback_query: {
      id: "name-reset-duplicate", from: { id: 7, is_bot: false },
      data: "thread-name:reset",
      message: { message_id: 99, message_thread_id: 42,
        chat: { id: 100, type: "private" } },
    } }, { cwd: "/repo" });
    assert.equal(resets, 2);
    await routeRuntime.handleUpdate(unboundTopicUpdate("/name"), { cwd: "/repo" });
    await routeRuntime.handleUpdate({ callback_query: {
      id: "name-cancel", from: { id: 7, is_bot: false },
      data: "thread-name:cancel",
      message: { message_id: 99, message_thread_id: 42,
        chat: { id: 100, type: "private" } },
    } }, { cwd: "/repo" });
    assert.equal(editedDialogs.includes("<b>✖ Rename cancelled.</b>"), true);
    assert.equal(
      editedDialogs.some((text) => /Returning|Starting/u.test(text)),
      false,
    );
    await routeRuntime.handleUpdate(unboundTopicUpdate("ordinary prompt"), { cwd: "/repo" });
    assert.equal(renamed.includes("ordinary prompt"), false);
  });
});

test("Routing runtime falls back to baked name for non-identity topic thread names", async () => {
  await withTopicStore(async (threadStore) => {
    threadStore.upsert({
      profileKey: "cwd:/repo",
      target: { chatId: 100, threadId: 42 },
      status: "active",
      createdAtMs: 1000,
      updatedAtMs: 1000,
      instanceId: "leader-a",
      slot: "O",
      threadName: "Follower",
    });
    await threadStore.persist();
    const { routeRuntime, telegramQueueStore } = createRouteHarness({
      threadStore,
    });

    await routeRuntime.handleUpdate(unboundTopicUpdate("fallback"), {
      cwd: "/repo",
    });

    const queued = telegramQueueStore.getQueuedItems()[0];
    assert.equal(queued?.kind, "prompt");
    assert.equal(
      queued?.kind === "prompt" && queued.content[0]?.type === "text"
        ? queued.content[0].text
        : "",
      "[telegram|thread:Orbit] fallback",
    );
  });
});

test("Routing runtime preserves active follower topics when the follower is not connected", async () => {
  await withTopicStore(async (threadStore) => {
    const apiCalls: unknown[] = [];
    const replyModes: Array<"HTML" | undefined> = [];
    threadStore.upsert({
      profileKey: "cwd:/repo",
      target: { chatId: 100, threadId: 7 },
      status: "active",
      createdAtMs: 1000,
      updatedAtMs: 1000,
      instanceId: "leader-a",
      slot: "A",
    });
    threadStore.upsert({
      profileKey: "manual:follower-b",
      owner: { kind: "manual-follower", instanceId: "follower-b" },
      target: { chatId: 100, threadId: 42 },
      status: "active",
      createdAtMs: 1100,
      updatedAtMs: 1100,
      instanceId: "follower-b",
      slot: "B",
      threadName: "Beacon",
    });
    const { events, routeRuntime, telegramQueueStore } = createRouteHarness({
      threadStore,
      callApi: async (method, body) => {
        apiCalls.push({ method, body });
        return {} as never;
      },
      observeTextReply: (_text, options) => {
        replyModes.push(options?.parseMode);
      },
    });

    await routeRuntime.handleUpdate(unboundTopicUpdate("for follower"), {
      cwd: "/repo",
    });

    assert.equal(
      threadStore.getByProfileKey("manual:follower-b")?.status,
      "active",
    );
    assert.deepEqual(apiCalls, []);
    assert.deepEqual(telegramQueueStore.getQueuedItems(), []);
    assert.equal(
      events.includes(
        "reply:Instance Beacon is not currently registered with the Telegram bus. This thread is preserved; retry shortly. If it does not recover, run <code>/telegram-connect</code> in that Pi instance.",
      ),
      true,
    );
    assert.deepEqual(replyModes, ["HTML"]);
  });
});

test("Routing runtime does not claim an unknown unbound thread while another thread is live", async () => {
  await withTopicStore(async (threadStore) => {
    threadStore.upsert({
      profileKey: "cwd:/repo",
      target: { chatId: 100, threadId: 7 },
      status: "active",
      createdAtMs: 1000,
      updatedAtMs: 1000,
      instanceId: "leader-a",
      slot: "D",
      threadName: "Dune",
    });
    await threadStore.persist();
    const { events, routeRuntime, telegramQueueStore } = createRouteHarness({
      threadStore,
      getLiveThreadTargets: () => [{ chatId: 100, threadId: 7 }],
    });

    await routeRuntime.handleUpdate(unboundTopicUpdate("stray follower text"), {
      cwd: "/repo",
    });

    assert.deepEqual(
      threadStore.getByProfileKey("cwd:/repo")?.target,
      { chatId: 100, threadId: 7 },
    );
    assert.deepEqual(telegramQueueStore.getQueuedItems(), []);
    assert.equal(events.some((event) => event.startsWith("interactive:")), true);
  });
});

test("Routing runtime prefers local thread label over stale shared store binding", async () => {
  await withTopicStore(async (threadStore) => {
    threadStore.upsert({
      profileKey: "cwd:/repo",
      target: { chatId: 100, threadId: 42 },
      status: "active",
      createdAtMs: 1000,
      updatedAtMs: 1000,
      instanceId: "leader-a",
      slot: "D",
      threadName: "Dune",
    });
    await threadStore.persist();
    const { routeRuntime, telegramQueueStore } = createRouteHarness({
      threadStore,
      instanceId: "follower-b",
      getLocalThreadLabelForTarget: (target) =>
        target.chatId === 100 && target.threadId === 42 ? "Juno" : undefined,
    });

    await routeRuntime.handleUpdate(unboundTopicUpdate("for follower"), {
      cwd: "/repo",
    });

    const queued = telegramQueueStore.getQueuedItems()[0];
    assert.equal(
      queued?.kind === "prompt" && queued.content[0]?.type === "text"
        ? queued.content[0].text
        : "",
      "[telegram|thread:Juno] for follower",
    );
  });
});

test("Routing runtime refuses threadless prompts in multi-instance thread mode", async () => {
  await withTopicStore(async (threadStore) => {
    threadStore.upsert({
      profileKey: "cwd:/repo",
      target: { chatId: 100, threadId: 42 },
      status: "active",
      createdAtMs: 1000,
      updatedAtMs: 1000,
      instanceId: "leader-a",
      slot: "A",
    });
    await threadStore.persist();
    const { events, routeRuntime, telegramQueueStore } = createRouteHarness({
      threadStore,
    });

    await routeRuntime.handleUpdate(
      {
        message: {
          message_id: 12,
          chat: { id: 100, type: "private" },
          from: { id: 7, is_bot: false },
          text: "threadless prompt",
        },
      },
      { cwd: "/repo" },
    );

    assert.deepEqual(telegramQueueStore.getQueuedItems(), []);
    assert.equal(
      events.includes(
        "reply:This bot is in threaded multi-instance mode. Send prompts in a bound Pi thread tab so they route to the right instance.",
      ),
      true,
    );
  });
});

test("Routing runtime degrades threadless prompts to classic when topic targets are stale", async () => {
  await withTopicStore(async (threadStore) => {
    const apiCalls: unknown[] = [];
    threadStore.upsert({
      profileKey: "cwd:/repo",
      target: { chatId: 100, threadId: 42 },
      status: "active",
      createdAtMs: 1000,
      updatedAtMs: 1000,
      instanceId: "leader-a",
      slot: "A",
    });
    await threadStore.persist();
    const { routeRuntime, telegramQueueStore } = createRouteHarness({
      threadStore,
      callApi: async (method, body) => {
        apiCalls.push({ method, body });
        throw new Error(
          "Telegram API sendChatAction failed: Bad Request: message thread not found",
        );
      },
    });

    await routeRuntime.handleUpdate(
      {
        message: {
          message_id: 12,
          chat: { id: 100, type: "private" },
          from: { id: 7, is_bot: false },
          text: "classic prompt",
        },
      },
      { cwd: "/repo" },
    );

    assert.deepEqual(apiCalls, [
      {
        method: "sendChatAction",
        body: { chat_id: 100, message_thread_id: 42, action: "typing" },
      },
    ]);
    assert.equal(threadStore.getBotState().threadMode, "disabled");
    const queued = telegramQueueStore.getQueuedItems()[0];
    assert.equal(queued?.kind, "prompt");
    assert.deepEqual(queued?.target, { chatId: 100 });
  });
});

test("Routing runtime assigns guest-mode prompts to the current transport leader", async () => {
  const { routeRuntime, telegramQueueStore } = createRouteHarness({
  });

  await routeRuntime.handleUpdate(
    {
      guest_message: {
        guest_query_id: "guest-1",
        chat: { type: "supergroup", title: "Guest Room" } as never,
        from: { id: 7, is_bot: false, username: "guest" } as TestUser & {
          username: string;
        },
        text: "guest question",
      },
    },
    { cwd: "/repo" },
  );

  const queued = telegramQueueStore.getQueuedItems()[0];
  assert.equal(queued?.kind, "prompt");
  assert.equal(
    queued?.kind === "prompt" ? queued.guestQueryId : undefined,
    "guest-1",
  );
  assert.equal(queued?.kind === "prompt" ? queued.chatId : undefined, 0);
  assert.equal(
    queued?.kind === "prompt" ? queued.target : undefined,
    undefined,
  );
  assert.equal(
    queued?.kind === "prompt" && queued.content[0]?.type === "text"
      ? queued.content[0].text
      : "",
    `[telegram|guest:Guest Room] guest question\n\n${TELEGRAM_GUEST_TURN_NOTE}`,
  );
});

test("Guest-mode media keeps its caption as the prompt text", async () => {
  const downloads: string[] = [];
  const { routeRuntime, telegramQueueStore } = createRouteHarness({
    async downloadFile(_fileId, fileName, source) { downloads.push(`${fileName}:${source?.scope}`); return `/tmp/${fileName}`; } });
  await routeRuntime.handleUpdate({ guest_message: { guest_query_id: "guest-photo", message_id: 5,
    chat: { type: "supergroup", title: "Guest Room" } as never,
    from: { id: 7, is_bot: false, username: "guest" } as TestUser & { username: string },
    caption: "what is this meme about?",
    photo: [{ file_id: "photo", file_unique_id: "photo", width: 1, height: 1 }] } as never }, { cwd: "/repo" });
  const queued = telegramQueueStore.getQueuedItems()[0];
  const prompt = queued?.kind === "prompt" && queued.content[0]?.type === "text" ? queued.content[0].text : "";
  assert.deepEqual(downloads, ["photo-5.jpg:guest"], "a guest source never inherits the bot-chat scope");
  assert.match(prompt, /^\[telegram\|guest:Guest Room\] what is this meme about\?/);
  const owner = { id: 7, username: "owner" }, bot = { id: 9, is_bot: true, username: "k1awbot" };
  assert.equal(Routing.resolveTelegramGuestFileScope({ chatType: "private", chat: { id: 42, username: "peer" }, from: owner, ownerUserId: 7 }), "peer");
  assert.equal(Routing.resolveTelegramGuestFileScope({ chatType: "private", chat: { id: 42 }, from: bot, ownerUserId: 7 }), "42");
  assert.equal(Routing.resolveTelegramGuestFileScope({ chatType: "supergroup", chat: { id: -1001234, title: "Room" }, from: owner, ownerUserId: 7 }), "1001234");
});

test("Routing runtime answers guest-mode queries early with the globe ACK and starts the placeholder", async () => {
  const acks: Array<{
    guestQueryId: string;
    text: string;
    options?: { parseMode?: "HTML" };
  }> = [];
  const starts: string[] = [];
  const { routeRuntime, telegramQueueStore } = createRouteHarness({
    answerGuestQueryForInlineMessage: async (guestQueryId, text, options) => {
      acks.push({ guestQueryId, text, options });
      return "inline-1";
    },
    startGuestPlaceholder: (inlineMessageId) => {
      starts.push(inlineMessageId);
    },
  });

  await routeRuntime.handleUpdate(
    {
      guest_message: {
        guest_query_id: "guest-1",
        chat: { type: "supergroup", title: "Guest Room" } as never,
        from: { id: 7, is_bot: false, username: "guest" } as TestUser & {
          username: string;
        },
        text: "guest question",
      },
    },
    { cwd: "/repo" },
  );

  assert.deepEqual(acks, [
    {
      guestQueryId: "guest-1",
      text: "<b>🌎 Working on it.</b>",
      options: { parseMode: "HTML" },
    },
  ]);
  assert.deepEqual(starts, ["inline-1"]);
  const queued = telegramQueueStore.getQueuedItems()[0];
  assert.equal(queued?.kind, "prompt");
  assert.equal(
    queued?.kind === "prompt" ? queued.guestInlineMessageId : undefined,
    "inline-1",
  );
});

test("Routing runtime keeps the guest-mode turn when the guest ACK fails", async () => {
  const { events, routeRuntime, telegramQueueStore } = createRouteHarness({
    answerGuestQueryForInlineMessage: async () => {
      throw new Error("query is too old");
    },
  });

  await routeRuntime.handleUpdate(
    {
      guest_message: {
        guest_query_id: "guest-1",
        chat: { type: "supergroup", title: "Guest Room" } as never,
        from: { id: 7, is_bot: false, username: "guest" } as TestUser & {
          username: string;
        },
        text: "guest question",
      },
    },
    { cwd: "/repo" },
  );

  assert.equal(
    events.includes("event:guest:Error: query is too old"),
    true,
  );
  const queued = telegramQueueStore.getQueuedItems()[0];
  assert.equal(queued?.kind, "prompt");
  assert.equal(
    queued?.kind === "prompt" ? queued.guestInlineMessageId : undefined,
    undefined,
  );
});

test("Routing runtime labels private guest-mode prompts with dm metadata", async () => {
  const { routeRuntime, telegramQueueStore } = createRouteHarness({});

  await routeRuntime.handleUpdate(
    {
      guest_message: {
        guest_query_id: "guest-dm-1",
        chat: { id: 99, type: "private", username: "guest" } as never,
        from: { id: 7, is_bot: false, username: "llblab" } as TestUser & {
          username: string;
        },
        text: "private guest question",
      },
    },
    { cwd: "/repo" },
  );

  const queued = telegramQueueStore.getQueuedItems()[0];
  assert.equal(
    queued?.kind === "prompt" && queued.content[0]?.type === "text"
      ? queued.content[0].text
      : "",
    `[telegram|guest:guest] private guest question\n\n${TELEGRAM_GUEST_TURN_NOTE}`,
  );
});

test("Routing runtime labels owner-authored private guest turns with the remote chat peer", async () => {
  const { routeRuntime, telegramQueueStore } = createRouteHarness({});

  await routeRuntime.handleUpdate(
    {
      guest_message: {
        guest_query_id: "guest-dm-owner-1",
        chat: {
          id: 99,
          type: "private",
          username: "counterparty",
          first_name: "Remote",
        } as never,
        from: { id: 7, is_bot: false, username: "llblab" } as TestUser & {
          username: string;
        },
        text: "@k1awbot attach something",
      },
    },
    { cwd: "/repo" },
  );

  const queued = telegramQueueStore.getQueuedItems()[0];
  assert.equal(
    queued?.kind === "prompt" && queued.content[0]?.type === "text"
      ? queued.content[0].text
      : "",
    `[telegram|guest:counterparty] @k1awbot attach something\n\n${TELEGRAM_GUEST_TURN_NOTE}`,
  );
});

test("Guest peer resolver never labels the paired owner and uses stable fallbacks", () => {
  assert.equal(
    Routing.resolveTelegramGuestPromptPeer({
      chatType: "private",
      from: { id: 99, username: "remote" },
      ownerUserId: 7,
    }),
    "remote",
  );
  assert.equal(
    Routing.resolveTelegramGuestPromptPeer({
      chatType: "private",
      chat: { id: 99, username: "renamedremote" },
      from: { id: 840585, username: "profileowner" },
      ownerUserId: 840585,
    }),
    "renamedremote",
  );
  assert.equal(
    Routing.resolveTelegramGuestPromptPeer({
      chatType: "private",
      chat: { id: 99, first_name: "Maria", last_name: "Example" },
      from: { id: 7, username: "llblab" },
      ownerUserId: 7,
    }),
    "Maria Example",
  );
  assert.equal(
    Routing.resolveTelegramGuestPromptPeer({
      chatType: "private",
      chat: { id: 99 },
      from: { id: 7, username: "llblab" },
      ownerUserId: 7,
    }),
    "99",
  );
  assert.equal(
    Routing.resolveTelegramGuestPromptPeer({
      chatType: "private",
      chat: { id: 99, username: "remote" },
      from: { id: 7, username: "llblab" },
      ownerUserId: 7,
    }),
    "remote",
  );
  assert.equal(
    Routing.resolveTelegramGuestPromptPeer({
      chatType: "private",
      from: { id: 7, username: "llblab" },
      replyFrom: { id: 99, is_bot: false, username: "remote" },
      ownerUserId: 7,
    }),
    "remote",
  );
  assert.equal(
    Routing.resolveTelegramGuestPromptPeer({
      chatType: "private",
      chat: { id: 7, username: "llblab" },
      from: { id: 7, username: "llblab" },
      replyFrom: { id: 123, is_bot: true, username: "k1awbot" },
      ownerUserId: 7,
    }),
    undefined,
  );
});

test("Routing runtime separates private guest identity from replied peer metadata", async () => {
  const { routeRuntime, telegramQueueStore } = createRouteHarness({});

  await routeRuntime.handleUpdate(
    {
      guest_message: {
        guest_query_id: "guest-dm-reply-1",
        chat: {
          id: 98,
          type: "private",
          username: "counterparty",
        } as never,
        from: { id: 7, is_bot: false, username: "llblab" } as TestUser & {
          username: string;
        },
        text: "@k1awbot test",
        reply_to_message: {
          message_id: 22,
          chat: { id: 7, type: "private" },
          from: {
            id: 99,
            is_bot: false,
            username: "quotedparty",
          } as TestUser & {
            username: string;
          },
          photo: [{ file_id: "photo", file_unique_id: "photo-u", width: 1, height: 1 }],
        } as TestMessage,
      },
    },
    { cwd: "/repo" },
  );

  const queued = telegramQueueStore.getQueuedItems()[0];
  assert.equal(
    queued?.kind === "prompt" && queued.content[0]?.type === "text"
      ? queued.content[0].text
      : "",
    [
      "[telegram|guest:counterparty] @k1awbot test",
      "",
      "[reply|from:quotedparty]",
      "",
      "[attachments|from:quotedparty] /tmp",
      "- /photo-22.jpg",
      "",
      TELEGRAM_GUEST_TURN_NOTE,
    ].join("\n"),
  );
});

test("Routing runtime keeps replied voice transcription inside Guest Mode reply context", async () => {
  const { routeRuntime, telegramQueueStore } = createRouteHarness({
    processInbound: async (files, rawText) => ({
      rawText,
      promptFiles: files,
      handlerOutputs: files.some((file) => file.kind === "voice")
        ? ["guest replied voice transcript"]
        : [],
      handledFiles: [],
    }),
  });

  await routeRuntime.handleUpdate(
    {
      guest_message: {
        guest_query_id: "guest-dm-voice-reply-1",
        chat: { id: 98, type: "private", username: "counterparty" } as never,
        from: { id: 7, is_bot: false, username: "llblab" } as TestUser & {
          username: string;
        },
        text: "@k1awbot respond",
        reply_to_message: {
          message_id: 22,
          chat: { id: 7, type: "private" },
          from: {
            id: 99,
            is_bot: false,
            username: "quotedparty",
          } as TestUser & { username: string },
          voice: { file_id: "voice", mime_type: "audio/ogg" },
        } as never,
      },
    },
    { cwd: "/repo" },
  );

  const queued = telegramQueueStore.getQueuedItems()[0];
  assert.equal(
    queued?.kind === "prompt" && queued.content[0]?.type === "text"
      ? queued.content[0].text
      : "",
    [
      "[telegram|guest:counterparty] @k1awbot respond",
      "",
      "[reply|from:quotedparty]",
      "",
      "[attachments|from:quotedparty] /tmp",
      "- /voice-22.ogg",
      "",
      "[outputs|from:quotedparty]",
      "- guest replied voice transcript",
      "",
      TELEGRAM_GUEST_TURN_NOTE,
    ].join("\n"),
  );
});

test("Routing runtime keeps private guest identity when replying to the bot", async () => {
  const { routeRuntime, telegramQueueStore } = createRouteHarness({});

  await routeRuntime.handleUpdate(
    {
      guest_message: {
        guest_query_id: "guest-dm-bot-reply-1",
        chat: {
          id: 99,
          type: "private",
          username: "counterparty",
        } as never,
        from: { id: 7, is_bot: false, username: "llblab" } as TestUser & {
          username: string;
        },
        text: "@k1awbot follow up",
        reply_to_message: {
          message_id: 23,
          chat: { id: 99, type: "private" },
          from: { id: 123, is_bot: true, username: "k1awbot" } as TestUser & {
            username: string;
          },
          text: "Bot answer",
        } as TestMessage,
      },
    },
    { cwd: "/repo" },
  );

  const queued = telegramQueueStore.getQueuedItems()[0];
  assert.equal(
    queued?.kind === "prompt" && queued.content[0]?.type === "text"
      ? queued.content[0].text
      : "",
    [
      "[telegram|guest:counterparty] @k1awbot follow up",
      "",
      "[reply|from:k1awbot] Bot answer",
      "",
      TELEGRAM_GUEST_TURN_NOTE,
    ].join("\n"),
  );
});

test("Routing runtime preserves follower target and marks generated prompt buttons selected", async () => {
  const selectedMarkups: unknown[] = [];
  const { buttonActionStore, events, routeRuntime, telegramQueueStore } =
    createRouteHarness({
      editMessageReplyMarkup: async (chatId, messageId, replyMarkup) => {
        selectedMarkups.push({ chatId, messageId, replyMarkup });
      },
      getLocalThreadLabelForTarget: ({ threadId }) =>
        threadId === 55 ? "Nimbus" : undefined,
    });
  const callbackData = buttonActionStore.register({
    text: "Continue",
    prompt: "Continue from button",
  });

  await routeRuntime.handleUpdate(
    {
      callback_query: {
        id: "callback-1",
        from: { id: 7, is_bot: false },
        data: callbackData,
        message: {
          message_id: 44,
          message_thread_id: 55,
          chat: { id: 100, type: "private" },
          from: { id: 7, is_bot: false },
          reply_markup: {
            inline_keyboard: [
              [{ text: "Approve", callback_data: callbackData }],
            ],
          },
        },
      },
    },
    { cwd: "/repo" },
  );

  const queued = telegramQueueStore.getQueuedItems()[0];
  assert.equal(queued?.kind, "prompt");
  assert.deepEqual(queued?.kind === "prompt" ? queued.target : undefined, {
    chatId: 100,
    threadId: 55,
  });
  assert.equal(
    queued?.kind === "prompt" ? queued.replyToMessageId : undefined,
    44,
  );
  assert.equal(
    queued?.kind === "prompt" && queued.content[0]?.type === "text"
      ? queued.content[0].text
      : "",
    "[telegram|thread:Nimbus] Continue from button",
  );
  assert.equal(events.includes("dispatch"), true);
  assert.deepEqual(selectedMarkups, [
    {
      chatId: 100,
      messageId: 44,
      replyMarkup: {
        inline_keyboard: [
          [
            {
              text: "Approve",
              callback_data: callbackData,
              style: "primary",
            },
          ],
        ],
      },
    },
  ]);
});

test("All command age policy requires a valid source timestamp and excludes bound threads", () => {
  const now = 10_000_000;
  const date = (now - Routing.TELEGRAM_ALL_TAB_COMMAND_MAX_AGE_MS) / 1000;
  assert.equal(Routing.isTelegramAllTabCommandExpired({ date }, now), true);
  assert.equal(Routing.isTelegramAllTabCommandExpired({ date }, now - 1), false);
  assert.equal(Routing.isTelegramAllTabCommandExpired({ date, message_thread_id: 42 }, now), false);
  for (const invalid of [undefined, 0, -1, NaN, Infinity, now / 1000 + 1]) {
    assert.equal(Routing.isTelegramAllTabCommandExpired({ date: invalid }, now), false);
  }
});

for (const failSettlement of [false, true]) {
test(`All command chooser publication completes every source: ${failSettlement ? "journal failure and recovery" : "normal"}`, async () => {
  await withTopicStore(async (threadStore, path) => {
    threadStore.upsert({
      profileKey: "cwd:/repo", target: { chatId: 100, threadId: 42 },
      status: "active", createdAtMs: Date.now(), updatedAtMs: Date.now(),
      instanceId: "leader-a", slot: "A", threadName: "Axial",
    });
    await threadStore.persist();
    const { routeRuntime, events } = createRouteHarness({ threadStore });
    const updates = ["/start", "/start payload", "/status", "/start"].map((text, index) => ({
      update_id: index + 1,
      message: { message_id: index + 10, date: Math.floor(Date.now() / 1000),
        chat: { id: 100, type: "private" as const }, from: { id: 7, is_bot: false }, text },
    }));
    const openJournal = () => Journal.createTelegramUpdateJournalStore({
      path: `${path}.inbox`,
      botIdentity: Journal.createTelegramUpdateJournalBotIdentity({ botToken: "123:coalesce-command" }),
    });
    const journal = openJournal();
    journal.appendBatch(updates);
    const removeCompleted = journal.removeCompleted;
    let writeFails = failSettlement;
    journal.removeCompleted = (ids) => {
      if (writeFails) throw new Error("fixture journal settlement write failed");
      return removeCompleted(ids);
    };
    const worker = Updates.createTelegramUpdateAdmissionWorkerRuntime<typeof updates[number], TestContext>({
      journal, hasAuthority: () => true,
      defaultHandle: (input, ctx) => routeRuntime.handleUpdate(input, ctx),
    });
    try {
      worker.start({ cwd: "/repo" });
      await worker.waitForDrain();
      await new Promise<void>((resolve) => setImmediate(resolve));
      if (failSettlement) {
        assert.deepEqual(openJournal().read().entries.map((entry) => entry.updateId), [1, 2, 3, 4]);
        assert.equal(worker.getState().phase, "blocked");
        assert.equal(events.includes("status-menu"), false);
        await worker.stop();
        writeFails = false;
        worker.start({ cwd: "/repo" });
        await worker.waitForDrain();
        await new Promise<void>((resolve) => setImmediate(resolve));
      }
      assert.deepEqual(openJournal().read().entries, []);
      assert.equal(worker.getState().deferredClaimCount, 0);
      await routeRuntime.handleUpdate({ callback_query: {
        id: "superseded", from: { id: 7, is_bot: false },
        message: { message_id: 99, chat: { id: 100, type: "private" } }, data: "reroute:1:42",
      } }, { cwd: "/repo" });
      assert.ok(events.includes("answer:⌛ Routing choice expired."));
      assert.equal(events.includes("status-menu"), false);
      await routeRuntime.handleUpdate({ callback_query: {
        id: "current", from: { id: 7, is_bot: false },
        message: { message_id: 99, chat: { id: 100, type: "private" } }, data: `reroute:${failSettlement ? "8" : "4"}:42`,
      } }, { cwd: "/repo" });
      await new Promise<void>((resolve) => setImmediate(resolve));
      assert.deepEqual(openJournal().read().entries, []);
      assert.equal(events.includes("status-menu"), true);
    } finally {
      await worker.stop();
    }
  });
});
}

test("Failed All chooser sends do not exhaust pending route capacity", async () => {
  await withTopicStore(async (threadStore) => {
    threadStore.upsert({
      profileKey: "cwd:/repo", target: { chatId: 100, threadId: 42 },
      status: "active", createdAtMs: Date.now(), updatedAtMs: Date.now(),
      instanceId: "leader-a", slot: "A", threadName: "Axial",
    });
    await threadStore.persist();
    let fail = true;
    let sends = 0;
    const { routeRuntime } = createRouteHarness({
      threadStore, sendInteractiveMessage: async () => {
        sends += 1;
        if (fail) throw new Error("fixture chooser send failure");
        return 99;
      },
    });
    const update = { message: {
      message_id: 12, date: Math.floor(Date.now() / 1000),
      chat: { id: 100, type: "private" as const }, from: { id: 7, is_bot: false }, text: "/start",
    } };
    for (let attempt = 0; attempt < 101; attempt += 1) {
      await assert.rejects(routeRuntime.handleUpdate(update, { cwd: "/repo" }), /fixture chooser send failure/);
    }
    fail = false;
    await routeRuntime.handleUpdate(update, { cwd: "/repo" });
    assert.equal(sends, 102);
  });
});

for (const replacementFails of [false, true]) {
  test(`All start replacement completes each source after chooser publication: ${replacementFails ? "replacement failure" : "delayed replacement success"}`, async () => {
    await withTopicStore(async (threadStore, path) => {
      threadStore.upsert({
        profileKey: "cwd:/repo", target: { chatId: 100, threadId: 42 },
        status: "active", createdAtMs: Date.now(), updatedAtMs: Date.now(),
        instanceId: "leader-a", slot: "A", threadName: "Axial",
      });
      await threadStore.persist();
      let release!: () => void;
      let markStarted!: () => void;
      const gate = new Promise<void>((resolve) => { release = resolve; });
      const started = new Promise<void>((resolve) => { markStarted = resolve; });
      let chooserCalls = 0;
      const { routeRuntime, events } = createRouteHarness({
        threadStore,
        sendInteractiveMessage: async () => {
          chooserCalls += 1;
          if (chooserCalls === 2) {
            markStarted();
            await gate;
            if (replacementFails) throw new Error("fixture chooser delivery failed");
          }
          return 98 + chooserCalls;
        },
      });
      const updates = [1, 2].map((id) => ({
        update_id: id, message: { message_id: id + 10, date: Math.floor(Date.now() / 1000),
          chat: { id: 100, type: "private" as const }, from: { id: 7, is_bot: false }, text: "/start" },
      }));
      const journal = Journal.createTelegramUpdateJournalStore({
        path: `${path}.inbox`,
        botIdentity: Journal.createTelegramUpdateJournalBotIdentity({ botToken: "123:chooser-replacement" }),
      });
      journal.appendBatch([updates[0]!]);
      const worker = Updates.createTelegramUpdateAdmissionWorkerRuntime<typeof updates[number], TestContext>({
        journal, hasAuthority: () => true,
        defaultHandle: (input, ctx) => routeRuntime.handleUpdate(input, ctx),
      });
      try {
        worker.start({ cwd: "/repo" });
        await worker.waitForDrain();
        journal.appendBatch([updates[1]!]);
        worker.signal();
        const drain = worker.waitForDrain();
        await started;
        assert.deepEqual(journal.read().entries.map((entry) => entry.updateId), [2]);
        assert.equal(worker.getState().deferredClaimCount, 0);
        release();
        await drain;
        await new Promise<void>((resolve) => setImmediate(resolve));
        assert.deepEqual(
          journal.read().entries.map((entry) => entry.updateId),
          replacementFails ? [2] : [],
        );
        await routeRuntime.handleUpdate({ callback_query: {
          id: "old", from: { id: 7, is_bot: false },
          message: { message_id: 99, chat: { id: 100, type: "private" } }, data: "reroute:1:42",
        } }, { cwd: "/repo" });
        await new Promise<void>((resolve) => setImmediate(resolve));
        assert.equal(events.includes("status-menu"), replacementFails);
        assert.equal(events.includes("answer:⌛ Routing choice expired."), !replacementFails);
        assert.deepEqual(
          journal.read().entries.map((entry) => entry.updateId),
          replacementFails ? [2] : [],
        );
      } finally {
        release();
        await worker.stop();
      }
    });
  });
}

for (const scenario of ["expired-empty", "expired-restored", "fresh-restored", "classic", "bound"] as const) {
  test(`All command restart routing respects target and mode: ${scenario}`, async (t) => {
    const now = 10_000_000;
    t.mock.method(Date, "now", () => now);
    await withTopicStore(async (threadStore, path) => {
      threadStore.setBotState({ threadMode: scenario === "classic" ? "disabled" : "enabled", updatedAtMs: now });
      const record = {
        profileKey: "cwd:/repo", target: { chatId: 100, threadId: 42 },
        status: "active" as const, createdAtMs: now, updatedAtMs: now,
        instanceId: "leader-a", slot: "A", threadName: "Axial",
      };
      if (scenario !== "expired-empty") threadStore.upsert(record);
      await threadStore.persist();
      const update = { update_id: 123, message: {
        message_id: 12,
        date: (now - (scenario === "fresh-restored" ? 1000 : Routing.TELEGRAM_ALL_TAB_COMMAND_MAX_AGE_MS)) / 1000,
        ...(scenario === "bound" ? { message_thread_id: 42 } : {}),
        chat: { id: 100, type: "private" as const }, from: { id: 7, is_bot: false }, text: "/start",
      } };
      const journal = Journal.createTelegramUpdateJournalStore({
        path: `${path}.inbox`,
        botIdentity: Journal.createTelegramUpdateJournalBotIdentity({ botToken: "123:target-recovery" }),
      });
      journal.appendBatch([update]);
      const run = async () => {
        const harness = createRouteHarness({ threadStore });
        const worker = Updates.createTelegramUpdateAdmissionWorkerRuntime<typeof update, TestContext>({
          journal, hasAuthority: () => true,
          defaultHandle: (input, ctx) => harness.routeRuntime.handleUpdate(input, ctx),
        });
        try {
          worker.start({ cwd: "/repo" });
          await worker.waitForDrain();
          await new Promise<void>((resolve) => setImmediate(resolve));
          return harness.events;
        } finally {
          await worker.stop();
        }
      };
      const events = await run();
      assert.equal(events.includes("dispatch"), false);
      assert.equal(events.includes("status-menu"), scenario === "classic" || scenario === "bound");
      const chooserCount = events.filter((event) => event.startsWith("interactive:html:")).length;
      assert.equal(chooserCount, scenario === "fresh-restored" ? 1 : 0);
      assert.deepEqual(journal.read().entries.map((entry) => entry.updateId), []);
      if (scenario === "fresh-restored") {
        threadStore.upsert({ ...record, target: { chatId: 100, threadId: 44 } });
        await threadStore.persist();
        const replayEvents = await run();
        assert.equal(replayEvents.filter((event) => event.startsWith("interactive:html:")).length, 0);
        assert.equal(replayEvents.includes("dispatch"), false);
        assert.equal(replayEvents.includes("status-menu"), false);
        assert.deepEqual(journal.read().entries.map((entry) => entry.updateId), []);
      }
    });
  });
}

for (const scenario of ["clock-rollback", "unknown-age"] as const) {
  test(`All chooser lifetime remains fenced under ${scenario}`, async (t) => {
    let now = 10_000_000;
    t.mock.timers.enable({ apis: ["setTimeout"] });
    t.mock.method(Date, "now", () => now);
    const advance = (ms: number) => { now += ms; t.mock.timers.tick(ms); };
    await withTopicStore(async (threadStore, path) => {
      threadStore.upsert({
        profileKey: "cwd:/repo", target: { chatId: 100, threadId: 42 },
        status: "active", createdAtMs: now, updatedAtMs: now,
        instanceId: "leader-a", slot: "A", threadName: "Axial",
      });
      await threadStore.persist();
      const { routeRuntime, events } = createRouteHarness({ threadStore });
      const update = { update_id: 123, message: {
        message_id: 12, ...(scenario === "clock-rollback" ? { date: now / 1000 } : {}),
        chat: { id: 100, type: "private" as const }, from: { id: 7, is_bot: false }, text: "/start",
      } };
      const journal = Journal.createTelegramUpdateJournalStore({
        path: `${path}.inbox`,
        botIdentity: Journal.createTelegramUpdateJournalBotIdentity({ botToken: "123:clock-lifecycle" }),
      });
      journal.appendBatch([update]);
      const worker = Updates.createTelegramUpdateAdmissionWorkerRuntime<typeof update, TestContext>({
        journal, hasAuthority: () => true,
        defaultHandle: (input, ctx) => routeRuntime.handleUpdate(input, ctx),
      });
      try {
        worker.start({ cwd: "/repo" });
        await worker.waitForDrain();
        if (scenario === "clock-rollback") now -= 30_000;
        advance(Routing.TELEGRAM_ALL_TAB_COMMAND_MAX_AGE_MS);
        await new Promise<void>((resolve) => setImmediate(resolve));
        assert.equal(journal.read().entries.length, 0);
        if (scenario === "clock-rollback") {
          advance(29_999);
          await new Promise<void>((resolve) => setImmediate(resolve));
          assert.equal(journal.read().entries.length, 0);
          advance(1);
          await new Promise<void>((resolve) => setImmediate(resolve));
          assert.deepEqual(journal.read().entries, []);
        } else {
          await worker.stop();
        }
        await routeRuntime.handleUpdate({ callback_query: {
          id: "old", from: { id: 7, is_bot: false },
          message: { message_id: 99, chat: { id: 100, type: "private" } }, data: "reroute:1:42",
        } }, { cwd: "/repo" });
        assert.ok(events.includes("answer:⌛ Routing choice expired."));
        assert.equal(events.includes("status-menu"), false);
        assert.equal(journal.read().entries.length, 0);
      } finally {
        await worker.stop();
      }
    });
  });
}

test("Unsettled unbound prompt keeps its route after later chooser pruning", async (t) => {
  let now = 10_000_000;
  t.mock.method(Date, "now", () => now);
  await withTopicStore(async (threadStore, path) => {
    threadStore.upsert({
      profileKey: "cwd:/repo", target: { chatId: 100, threadId: 42 },
      status: "active", createdAtMs: now, updatedAtMs: now,
      instanceId: "leader-a", slot: "A", threadName: "Axial",
    });
    await threadStore.persist();
    const { events, routeRuntime, telegramQueueStore } = createRouteHarness({ threadStore });
    const update = { update_id: 123, message: {
      message_id: 12, message_thread_id: 99, date: now / 1000,
      chat: { id: 100, type: "private" as const }, from: { id: 7, is_bot: false },
      text: "retained original prompt",
    } };
    const journal = Journal.createTelegramUpdateJournalStore({
      path: `${path}.inbox`,
      botIdentity: Journal.createTelegramUpdateJournalBotIdentity({ botToken: "123:unbound-retention" }),
    });
    journal.appendBatch([update]);
    const worker = Updates.createTelegramUpdateAdmissionWorkerRuntime<typeof update, TestContext>({
      journal, hasAuthority: () => true,
      defaultHandle: (input, ctx) => routeRuntime.handleUpdate(input, ctx),
    });
    try {
      worker.start({ cwd: "/repo" });
      await worker.waitForDrain();
      assert.equal(journal.read().entries[0]?.state, "pending");
      assert.equal(worker.getState().deferredClaimCount, 1);
      now += 30 * 60_000 + 1;
      journal.appendBatch([{ ...update, update_id: 124,
        message: { ...update.message, message_id: 13, date: now / 1000, text: "later prompt" } }]);
      worker.signal();
      await worker.waitForDrain();
      assert.equal(worker.getState().deferredClaimCount, 2);
      assert.deepEqual(telegramQueueStore.getQueuedItems(), [], "Age is not permission to route or discard");
      const callback = { callback_query: {
        id: "original-route", from: { id: 7, is_bot: false },
        message: { message_id: 99, message_thread_id: 99, chat: { id: 100, type: "private" as const } },
        data: "reroute:1:42",
      } };
      await routeRuntime.handleUpdate(callback, { cwd: "/repo" });
      assert.equal(events.includes("answer:⌛ Routing choice expired."), false,
        "Pruning must not orphan a still-deferred durable source");
      assert.equal(telegramQueueStore.getQueuedItems().length, 1);
      assert.equal(journal.read().entries.find(entry => entry.updateId === 123)?.state, "queued");
      assert.equal(journal.read().entries.find(entry => entry.updateId === 124)?.state, "pending");
      await routeRuntime.handleUpdate(callback, { cwd: "/repo" });
      assert.equal(telegramQueueStore.getQueuedItems().length, 1, "Old button must not queue the source twice");
    } finally {
      await worker.stop();
    }
  });
});

for (const winner of ["cancellation", "selection"] as const) test(`Unbound deferred source allows only ${winner} to win the routing race`, async () => {
  await withTopicStore(async (threadStore, path) => {
    threadStore.upsert({ profileKey: "cwd:/repo", target: { chatId: 100, threadId: 42 },
      status: "active", createdAtMs: 1, updatedAtMs: 1, instanceId: "leader-a", slot: "A", threadName: "Axial" });
    await threadStore.persist();
    let holdSelection = false;
    let entered!: () => void;
    let release!: () => void;
    const enteredSelection = new Promise<void>(resolve => { entered = resolve; });
    const pendingSelection = new Promise<void>(resolve => { release = resolve; });
    const { routeRuntime, telegramQueueStore, events } = createRouteHarness({ threadStore,
      async runWorkspaceOperation(input, operation) {
        if (holdSelection && input.operationId.startsWith("workspace-reroute:")) { entered(); await pendingSelection; }
        return operation();
      },
    });
    const update = { update_id: 123, message: { message_id: 12, message_thread_id: 99,
      chat: { id: 100, type: "private" as const }, from: { id: 7, is_bot: false }, text: "preserve this original" } };
    const options = { path: `${path}.inbox`,
      botIdentity: Journal.createTelegramUpdateJournalBotIdentity({ botToken: "123:race-fixture" }) };
    const journal = Journal.createTelegramUpdateJournalStore(options);
    let source: unknown;
    const worker = Updates.createTelegramUpdateAdmissionWorkerRuntime<typeof update, TestContext>({
      journal, getJournalBindingKey: () => Journal.createTelegramUpdateJournalBindingKey(options), hasAuthority: () => true,
      defaultHandle(input, ctx) { source = input.message; return routeRuntime.handleUpdate(input, ctx); },
    });
    const callback = { callback_query: { id: "choose-route", from: { id: 7, is_bot: false },
      message: { message_id: 99, message_thread_id: 99, chat: { id: 100, type: "private" as const } }, data: "reroute:1:42" } };
    const authority = { operatorAuthorityId: "owner:7", isCurrent: () => true };
    let selection: Promise<void> | undefined;
    try {
      worker.start({ cwd: "/repo" });
      await worker.waitForDrain();
      journal.appendBatch([update]);
      worker.signal();
      await worker.waitForDrain();
      assert.equal(Updates.supportsTelegramDeferredAbandonment(source, Journal.createTelegramUpdateJournalBindingKey(options)), true);
      if (winner === "cancellation") {
        const result = Updates.abandonTelegramDeferredUpdate(source, authority);
        assert.ok(result);
        await worker.waitForDrain();
        const records = threadStore.list();
        await routeRuntime.handleUpdate(callback, { cwd: "/repo" });
        assert.ok(events.includes("answer:⌛ Routing choice expired."));
        assert.deepEqual(threadStore.list(), records);
        assert.deepEqual(telegramQueueStore.getQueuedItems(), []);
        assert.deepEqual(journal.read().entries, []);
        assert.deepEqual(JSON.parse(await readFile(result.retainedPath, "utf8")).entry.update, update);
      } else {
        holdSelection = true;
        selection = routeRuntime.handleUpdate(callback, { cwd: "/repo" });
        await enteredSelection;
        assert.equal(Updates.abandonTelegramDeferredUpdate(source, authority), undefined,
          "Selection must reserve the source before awaited Workspace admission");
        assert.equal(Updates.getTelegramUpdateExecutionFence(source)?.isCurrent(), true);
        assert.deepEqual(telegramQueueStore.getQueuedItems(), []);
        release();
        await selection;
        await new Promise<void>(resolve => setImmediate(resolve));
        assert.equal(telegramQueueStore.getQueuedItems().length, 1);
        assert.equal(journal.read().entries[0]?.state, "queued");
        assert.equal(Updates.abandonTelegramDeferredUpdate(source, authority), undefined);
      }
    } finally { release(); await selection?.catch(() => undefined); await worker.stop(); }
  });
});

for (const scenario of ["cancel", "restore-menu", "owner", "chat", "thread", "chooser", "profile", "source-profile", "session", "epoch",
  "command", "unsupported", "historical", "selected", "forward-unknown", "storage-failure", "storage-recovery", "edit-failure", "workspace-denied"] as const) {
  test(`Owner cancellation travels from chooser through the native worker and journal (${scenario})`, async () => {
    await withTopicStore(async (threadStore, path) => {
      threadStore.upsert({ profileKey: "cwd:/repo", target: { chatId: 100, threadId: 42 }, status: "active",
        createdAtMs: 1, updatedAtMs: 1, instanceId: "leader-a", slot: "A", threadName: "Axial" });
      if (scenario === "forward-unknown") threadStore.upsert({ profileKey: "cwd:/other", target: { chatId: 100, threadId: 43 },
        status: "active", createdAtMs: 1, updatedAtMs: 1, instanceId: "follower-a", slot: "B", threadName: "Beacon" });
      await threadStore.persist();
      let owner = 7;
      let active = true;
      let epoch = 1;
      let failStorage = false;
      let failEdit = scenario === "edit-failure";
      let denyWorkspace = scenario === "workspace-denied";
      let abandonCalls = 0;
      const options = { path: `${path}.inbox`, botIdentity: Journal.createTelegramUpdateJournalBotIdentity({ botToken: "123:owner-cancel" }) };
      const bindingKey = Journal.createTelegramUpdateJournalBindingKey(options);
      let activeBinding = scenario === "source-profile" ? "another-journal" : bindingKey;
      const journal = Journal.createTelegramUpdateJournalStore({ ...options, onPublicationBoundary(boundary, target) {
        if (failStorage && boundary === "before-write" && !target.startsWith(`${options.path}.retained`)) {
          failStorage = false;
          throw new Error("fixture interrupted cancellation");
        }
      } });
      const surfaces: Array<{ text: string; mode: unknown; keyboard: unknown }> = [];
      const editedIds: number[] = [];
      const apiCalls: string[] = [];
      const { routeRuntime, telegramQueueStore, events } = createRouteHarness({ threadStore,
        configStore: { get: () => ({} as never), getAllowedUserId: () => owner,
          persistAllowedUserId: async () => true, persist: async () => undefined },
        getAdmissionJournalBinding: () => activeBinding, isContextActive: () => active, getCurrentLeaderEpoch: () => epoch,
        foreignOwnedUpdateForwarder: { forwardMessage: async () => retryableForeignUpdateSettlement() },
        async runWorkspaceOperation(input, operation) {
          if (denyWorkspace && input.operationKind === "workspace.cancel-unbound-routing") throw new Error("fixture Workspace fence");
          return operation();
        },
        async sendInteractiveMessage(_chat, text, mode, keyboard) { surfaces.push({ text, mode, keyboard }); return 99; },
        async editInteractiveMessage(_chat, messageId, text, mode, keyboard) {
          editedIds.push(messageId);
          if (failEdit && text.includes("Routing cancelled")) { failEdit = false; throw new Error("fixture chooser edit failure"); }
          surfaces.push({ text, mode, keyboard });
        },
        async callApi(method) { apiCalls.push(method); return true as never; },
        async deleteMessage() { apiCalls.push("deleteMessage"); },
      });
      const update = { update_id: 123, message: { message_id: 12, message_thread_id: 99,
        chat: { id: 100, type: "private" as const }, from: { id: 7, is_bot: false },
        text: scenario === "command" ? "/start" : "retain this private original" } };
      if (scenario === "historical") journal.appendBatch([update]);
      const worker = Updates.createTelegramUpdateAdmissionWorkerRuntime<TestUpdate & Journal.TelegramJournaledUpdate, TestContext>({
        journal: { ...journal, abandonPending: scenario === "unsupported" ? undefined : input => { abandonCalls++; return journal.abandonPending(input); } },
        getJournalBindingKey: () => bindingKey, hasAuthority: () => true,
        defaultHandle: (input, ctx) => routeRuntime.handleUpdate(input, ctx),
      });
      let nextUpdateId = 124;
      const click = async (data: string, fault = false, review = false) => {
        journal.appendBatch([{ update_id: nextUpdateId++, callback_query: {
          id: `click-${nextUpdateId}`, from: { id: owner, is_bot: false }, data,
          message: { message_id: review ? 500 : scenario === "chooser" ? 98 : 99,
            message_thread_id: review ? 42 : scenario === "thread" ? 98 : 99,
            chat: { id: scenario === "chat" ? 101 : 100, type: "private" } },
        } }]);
        failStorage = fault;
        worker.signal();
        await worker.waitForDrain();
      };
      try {
        worker.start({ cwd: "/repo" });
        await worker.waitForDrain();
        if (scenario !== "historical") journal.appendBatch([update]);
        const sourceEntry = journal.read().entries[0]!;
        worker.signal();
        await worker.waitForDrain();
        const hasCancel = JSON.stringify(surfaces[0]?.keyboard ?? {}).includes("reroutecancel:1");
        if (scenario === "historical") {
          assert.equal(surfaces.length, 0, "The last-boundary hold must not recreate a route chooser");
          assert.equal(worker.getState().historicalClaimCount, 1);
        }
        if (hasCancel) {
          assert.doesNotMatch(surfaces[0]!.text, /Cancel routing keeps/);
          assert.match(JSON.stringify(surfaces[0]!.keyboard), /"text":"⛔️ Cancel routing"/);
          assert.doesNotMatch(JSON.stringify(surfaces[0]!.keyboard), /❌ Cancel routing/);
        }
        if (scenario === "owner") owner = 8;
        if (scenario === "profile") activeBinding = "another-journal";
        if (scenario === "session") active = false;
        if (scenario === "epoch") epoch = 2;
        if (scenario === "restore-menu") {
          await click("rerouterestore:1");
          assert.doesNotMatch(JSON.stringify(surfaces.at(-1)?.keyboard), /reroutecancel:/);
          assert.match(JSON.stringify(surfaces.at(-1)?.keyboard), /"text":"⬆️ Back"/);
          await click("rerouteroot:1");
          assert.match(JSON.stringify(surfaces.at(-1)?.keyboard), /reroutecancel:1/);
          assert.match(JSON.stringify(surfaces.at(-1)?.keyboard), /"text":"⛔️ Cancel routing"/);
        }
        if (scenario === "selected") await click("reroute:1:42");
        if (scenario === "forward-unknown") await click("reroute:1:43");
        await click("reroutecancel:1", scenario.startsWith("storage-"));
        if (scenario.startsWith("storage-") || scenario === "edit-failure" || scenario === "workspace-denied") {
          assert.ok(events.some(event => event.startsWith("answer:⚠️")));
          if (scenario !== "workspace-denied") {
            await click("reroute:1:42");
            assert.equal(telegramQueueStore.getQueuedItems().length, 0, "Uncertain or committed cancellation must revoke old destination buttons");
          }
          denyWorkspace = false;
          if (scenario === "storage-recovery") {
            await click("reroutecancel:review:open", false, true);
            const retry = JSON.stringify(surfaces.at(-1)!.keyboard).match(/reroutecancel:review:[a-z0-9]+:retry:0/)?.[0];
            assert.ok(retry);
            await click(retry, false, true);
          } else await click("reroutecancel:1");
        }
        const succeeds = ["cancel", "command", "restore-menu", "storage-failure", "storage-recovery", "edit-failure", "workspace-denied"].includes(scenario);
        if (succeeds) {
          assert.equal(journal.read().entries.some(entry => entry.updateId === 123), false);
          assert.equal(journal.read().operatorDispositions?.length, 1);
          assert.equal(abandonCalls, scenario.startsWith("storage-") ? 2 : 1);
          if (scenario === "storage-recovery") {
            assert.deepEqual(editedIds, [500, 99, 500], "Recovery retires the original chooser only after the durable acknowledgement");
            assert.match(surfaces.at(-1)!.text, /⛔️ Routing cancelled\./);
          } else assert.deepEqual(surfaces.at(-1), { text: "<b>⛔️ Routing cancelled.</b>",
            mode: "html", keyboard: { inline_keyboard: [] } });
          const retained = journal.inspectPendingRetention(sourceEntry);
          assert.ok(retained);
          assert.deepEqual(JSON.parse(await readFile(retained.retainedPath, "utf8")).entry.update, update);
          assert.equal(apiCalls.includes("deleteMessage") || apiCalls.includes("deleteForumTopic"), false);
          await click("reroute:1:42");
          await click("reroutecancel:1");
          assert.equal(telegramQueueStore.getQueuedItems().length, 0);
          assert.equal(abandonCalls, scenario.startsWith("storage-") ? 2 : 1);
        } else {
          assert.equal(abandonCalls, 0);
          assert.equal(journal.read().operatorDispositions, undefined);
          assert.equal(journal.read().entries.find(entry => entry.updateId === 123)?.state,
            scenario === "selected" ? "queued" : scenario === "command" ? undefined : "pending");
        }
        assert.equal(hasCancel, !["unsupported", "source-profile", "historical"].includes(scenario));
      } finally { await worker.stop(); }
    });
  });
}

for (const scenario of ["success", "page", "unsupported", "corrupt", "owner", "profile", "epoch", "session",
  "chooser", "thread", "stale", "navigation", "restart-control", "edit-failure", "lease-failure", "lease-drift", "bad-index"] as const) {
  test(`Pending cancellation review uses fresh owner controls and durable originals (${scenario})`, async () => {
    await withTopicStore(async (threadStore, path) => {
      const options = { path: `${path}.recovery`, botIdentity: Journal.createTelegramUpdateJournalBotIdentity({ botToken: "fixture:recovery-ui" }) };
      const bindingKey = Journal.createTelegramUpdateJournalBindingKey(options);
      const initial = Journal.createTelegramUpdateJournalStore(options);
      const originals = Array.from({ length: 6 }, (_, index) => ({ update_id: 123 + index,
        message: { message_id: 12 + index, message_thread_id: 99, chat: { id: 100, type: "private" },
          from: { id: 7, is_bot: false }, text: `original <${index}>` } }));
      initial.appendBatch([...originals, ...(scenario === "unsupported" ? [
        { update_id: 130, message: { ...originals[0]!.message, from: { id: 8, is_bot: false }, text: "PRIVATE OTHER OWNER" } },
        { update_id: 131, message: { ...originals[0]!.message, text: "/private-command" } },
        { update_id: 132, message: { ...originals[0]!.message, media_group_id: "album", text: "PRIVATE GROUP" } },
        { update_id: 133, message: { ...originals[0]!.message, text: "PRIVATE FORWARDED" }, pi_telegram_forwarded: true },
      ] : [])]);
      const entries = initial.read().entries;
      const interrupted = Journal.createTelegramUpdateJournalStore({ ...options, onPublicationBoundary(boundary, target) {
        if (boundary === "before-write" && !target.startsWith(`${options.path}.retained`)) throw new Error("fixture interrupted commit");
      } });
      for (const entry of entries) assert.throws(() => interrupted.abandonPending({ entry, journalBindingKey: bindingKey,
        operatorAuthorityId: "owner:7", isCurrent: () => true }));
      const retainedPath = initial.inspectPendingRetention(entries[0]!)!.retainedPath;
      if (scenario === "corrupt") await writeFile(retainedPath, "{corrupt");
      const retainedBytes = await readFile(retainedPath, "utf8");
      const journal = Journal.createTelegramUpdateJournalStore(options);
      let owner = 7;
      let binding = bindingKey;
      let epoch = 1;
      let active = true;
      let failEdit = scenario === "edit-failure";
      let denyLease = scenario === "lease-failure";
      let abandonCalls = 0;
      const surfaces: Array<{ chat: number; id: number; text: string; mode: unknown; markup: unknown }> = [];
      const routeOptions: RouteHarnessOptions = { threadStore,
        configStore: { get: () => ({} as never), getAllowedUserId: () => owner,
          persistAllowedUserId: async () => true, persist: async () => undefined },
        getAdmissionJournalBinding: () => binding, getCurrentLeaderEpoch: () => epoch, isContextActive: () => active,
        async runWorkspaceOperation(input, operation) {
          if (input.operationKind === "workspace.recover-unbound-cancellation") {
            if (denyLease) throw new Error("fixture admission fence");
            if (scenario === "lease-drift") binding = "foreign";
          }
          return operation();
        },
        async editInteractiveMessage(chat, id, text, mode, markup) {
          if (failEdit && text.includes("⛔️ Routing cancelled.")) { failEdit = false; throw new Error("fixture edit failure"); }
          surfaces.push({ chat, id, text, mode, markup });
        },
        async callApi() { throw new Error("Recovery must not create/delete/probe a Thread"); },
        async deleteMessage() { throw new Error("Recovery must not delete original messages"); },
      };
      let harness = createRouteHarness(routeOptions);
      const readViewId = () => {
        const markup = JSON.stringify(surfaces.at(-1)!.markup);
        for (const button of markup.matchAll(/"callback_data":"([^"]+)"/g)) assert.ok(Buffer.byteLength(button[1]!) <= 64);
        const id = markup.match(/reroutecancel:review:([a-z0-9]+):refresh/)?.[1];
        assert.ok(id);
        return id;
      };
      const executed: number[] = [];
      const worker = Updates.createTelegramUpdateAdmissionWorkerRuntime<TestUpdate & Journal.TelegramJournaledUpdate, TestContext>({
        journal: { ...journal, abandonPending(input) { abandonCalls++; return journal.abandonPending(input); } },
        getJournalBindingKey: () => bindingKey, hasAuthority: () => true,
        async defaultHandle(update, ctx) { executed.push(update.update_id); await harness.routeRuntime.handleUpdate(update, ctx); },
      });
      let updateId = 200;
      const click = async (data: string, wrongTarget = false) => {
        journal.appendBatch([{ update_id: updateId++, callback_query: { id: `query-${updateId}`, from: { id: owner, is_bot: false }, data,
          message: { message_id: wrongTarget && scenario === "chooser" ? 501 : 500,
            message_thread_id: wrongTarget && scenario === "thread" ? 43 : 42, chat: { id: 100, type: "private" } } } }]);
        worker.signal();
        await worker.waitForDrain();
      };
      try {
        worker.start({ cwd: "/repo" });
        await worker.waitForDrain();
        assert.deepEqual(executed, []);
        await click("reroutecancel:review:open");
        assert.match(surfaces.at(-1)!.text, /original &lt;0&gt;/);
        assert.equal(surfaces.at(-1)!.mode, "html");
        const firstViewId = readViewId();
        assert.match(JSON.stringify(surfaces.at(-1)!.markup), new RegExp(`review:${firstViewId}:more`));
        assert.equal(abandonCalls, 0, "Opening review cannot commit cancellation");
        let viewId = firstViewId;
        let expectedSource = 123;
        if (scenario === "page" || scenario === "unsupported") {
          await click(`reroutecancel:review:${viewId}:more`); viewId = readViewId(); expectedSource = 128;
          assert.match(surfaces.at(-1)!.text, /original &lt;5&gt;/);
          assert.doesNotMatch(JSON.stringify(surfaces.at(-1)), /PRIVATE|private-command/);
          assert.doesNotMatch(JSON.stringify(surfaces.at(-1)!.markup), /:more/);
        }
        if (scenario === "stale" || scenario === "navigation" || scenario === "restart-control") {
          if (scenario === "stale") { await click(`reroutecancel:review:${viewId}:refresh`); viewId = readViewId(); }
          if (scenario === "navigation") await click("menu:queue");
          if (scenario === "restart-control") {
            await worker.stop();
            harness = createRouteHarness(routeOptions);
            worker.start({ cwd: "/repo" }); await worker.waitForDrain();
            await click("reroutecancel:review:open"); viewId = readViewId();
          }
          await click(`reroutecancel:review:${firstViewId}:retry:0`);
          assert.equal(abandonCalls, 0, "Obsolete controls cannot cancel an original, even after router recreation in the same message");
          if (scenario === "navigation") { await click("reroutecancel:review:open"); viewId = readViewId(); }
        }
        if (scenario === "owner") owner = 8;
        if (scenario === "profile") binding = "foreign";
        if (scenario === "epoch") epoch = 2;
        if (scenario === "session") active = false;
        await click(`reroutecancel:review:${viewId}:retry:${scenario === "bad-index" ? 999 : 0}`, true);
        if (scenario === "edit-failure" || scenario === "lease-failure") {
          assert.ok(harness.events.some(event => event.startsWith("answer:⚠️")));
          assert.equal(abandonCalls, scenario === "edit-failure" ? 1 : 0);
          denyLease = false;
          await click(`reroutecancel:review:${viewId}:retry:0`);
        }
        const rejects = ["corrupt", "owner", "profile", "epoch", "session", "chooser", "thread", "lease-drift", "bad-index"].includes(scenario);
        assert.equal(abandonCalls, rejects ? (scenario === "corrupt" ? 1 : 0) : 1);
        assert.equal(journal.read().entries.some(entry => entry.updateId === expectedSource), rejects);
        assert.equal(harness.telegramQueueStore.getQueuedItems().length, 0);
        assert.ok(executed.every(id => id >= 200), "Protected originals never enter routing or the agent");
        if (!rejects) {
          assert.match(surfaces.at(-1)!.text, /⛔️ Routing cancelled\./);
          assert.doesNotMatch(JSON.stringify(surfaces.at(-1)!.markup), new RegExp(`review:${viewId}:retry:0`));
          assert.deepEqual(journal.appendBatch([originals[expectedSource - 123]!]).duplicateUpdateIds, [expectedSource]);
        } else assert.equal(journal.read().operatorDispositions, undefined);
        assert.equal(await readFile(retainedPath, "utf8"), retainedBytes);
      } finally { await worker.stop(); }
    });
  });
}

for (const sourceKind of ["command", "voice", "unknown"] as const) {
  for (const [targetKind, forceReview] of [["unbound", false], ["unbound", true], ["bound", false]] as const) {
    test(`Historical unsupported retain-only (${sourceKind}, ${targetKind}, ${forceReview ? "unsafe boolean override" : "production classifier"})`, async () => {
      await withTopicStore(async (threadStore, path) => {
        if (targetKind === "bound") {
          threadStore.upsert({ profileKey: "cwd:/repo", target: { chatId: 100, threadId: 99 }, status: "active",
            createdAtMs: 1, updatedAtMs: 1, instanceId: "leader-a", slot: "A" });
          await threadStore.persist();
        }
        const options = { path: `${path}.unsupported-history`,
          botIdentity: Journal.createTelegramUpdateJournalBotIdentity({ botToken: "fixture:unsupported-history" }) };
        const journal = Journal.createTelegramUpdateJournalStore(options);
        const binding = Journal.createTelegramUpdateJournalBindingKey(options);
        journal.appendBatch([{ update_id: 123, message: { message_id: 12, message_thread_id: 99,
          chat: { id: 100, type: "private" }, from: { id: 7, is_bot: false },
          ...(sourceKind === "command" ? { text: "/next" } : sourceKind === "voice" ?
            { voice: { file_id: "fixture-voice", duration: 1 } } : { future_input: { value: "original" } }) } }]);
        const original = structuredClone(journal.read().entries[0]!);
        const harness = createRouteHarness({ threadStore, getCurrentLeaderEpoch: () => 1,
          getAdmissionJournalBinding: () => binding, isContextActive: () => true });
        const handled: number[] = [], completed: number[] = [], classifications: number[] = [];
        const worker = Updates.createTelegramUpdateAdmissionWorkerRuntime<Updates.TelegramUpdateFlow & Journal.TelegramJournaledUpdate, TestContext>({
          journal, getJournalBindingKey: () => binding, hasAuthority: () => true,
          spendHistoricalInput: true,
          async shouldReviewHistoricalInput(entry, ctx, signal) {
            classifications.push(entry.updateId);
            assert.deepEqual(entry, original, "Classification sees the exact original, not a handler projection");
            assert.equal(await harness.routeRuntime.shouldReviewHistoricalInput(entry, ctx, signal), "retain");
            return forceReview ? true : "retain";
          },
          registry: { version: 1, add: () => () => {}, async dispatch(update) {
            handled.push((update as Journal.TelegramJournaledUpdate).update_id);
            return "consume" as const;
          } },
          defaultHandle() { assert.fail("The companion handler owns this fixture outcome; no Pi execution is supplied"); },
          onUpdateCompleted: id => { completed.push(id); },
        });
        try {
          worker.start({ cwd: "/repo" }); await worker.waitForDrain();
          assert.deepEqual(classifications, [123]);
          assert.deepEqual(handled, [], "Neither retained originals nor spent sources reach a handler");
          assert.deepEqual(completed, []);
          assert.deepEqual(journal.read().entries, forceReview ? [] : [original], "Only the retain verdict preserves exact original evidence");
          assert.equal(worker.getState().historicalClaimCount ?? 0, forceReview ? 0 : 1);
          assert.equal(journal.read().operatorDispositions, undefined);
          journal.appendBatch([{ update_id: 124, message: { ...(original.update.message as object), message_id: 13 } }]);
          worker.signal(); await worker.waitForDrain();
          assert.deepEqual(handled, [124]);
          assert.deepEqual(classifications, [123], "Same-kind arrivals on the same target remain live, not historical");
          for (let generation = 0; generation < 2 && !forceReview; generation++) {
            await worker.stop(); worker.start({ cwd: "/repo" }); await worker.waitForDrain();
            assert.deepEqual(journal.read().entries, [original]);
            assert.deepEqual(handled, [124]);
            assert.equal(worker.getState().historicalClaimCount, 1);
          }
        } finally { await worker.stop(); }
      });
    });
  }
}

test("Historical routing classification is narrow and fails closed before stale context access", async () => {
  await withTopicStore(async (threadStore, path) => {
    threadStore.upsert({ profileKey: "cwd:/repo", target: { chatId: 100, threadId: 42 }, status: "active",
      createdAtMs: 1, updatedAtMs: 1, instanceId: "leader-a", slot: "A" });
    await threadStore.persist();
    const journal = Journal.createTelegramUpdateJournalStore({ path: `${path}.classification`,
      botIdentity: Journal.createTelegramUpdateJournalBotIdentity({ botToken: "fixture:classification" }) });
    journal.appendBatch([{ update_id: 123, message: { message_id: 12, message_thread_id: 99,
      chat: { id: 100, type: "private" }, from: { id: 7, is_bot: false }, text: "original" } }]);
    const original = journal.read().entries[0]!;
    let forbidContext = false;
    const { routeRuntime } = createRouteHarness({ threadStore, getCurrentLeaderEpoch: () => 1,
      getAdmissionJournalBinding: () => "source", isContextActive() { assert.equal(forbidContext, false); return true; } });
    const signal = new AbortController().signal;
    assert.equal(await routeRuntime.shouldReviewHistoricalInput(original, { cwd: "/repo" }, signal), true);
    const cases: Array<[string, Partial<Journal.TelegramUpdateJournalEntry>, Record<string, unknown>]> = [
      ["bound", {}, { message_thread_id: 42 }], ["threadless", {}, { message_thread_id: undefined }],
      ["command", {}, { text: "/next" }], ["media", {}, { voice: {} }], ["album", {}, { media_group_id: "group" }],
      ["business", {}, { business_connection_id: "business" }], ["bot", {}, { from: { id: 7, is_bot: true } }],
      ["group", {}, { chat: { id: 100, type: "supergroup" } }], ["carrier", {}, { pi_telegram_source_update_id: 123 }],
      ["receipt", { queueReceiptId: "accepted" }, {}], ["queued", { state: "queued" }, {}],
      ["claim", { inputClaim: {} as never }, {}], ["provenance", { inputProvenance: {} as never }, {}],
      ["owner", { queueOwner: {} as never }, {}], ["handoff", { queueHandoff: {} as never }, {}],
      ["failure", { failure: {} as never }, {}], ["retry", { state: "retry-wait" }, {}],
      ["excluded", { preApprovalExcluded: true }, {}],
      ["forwarded", { update: { ...original.update, pi_telegram_forwarded: true } }, {}],
    ];
    for (const [name, patch, messagePatch] of cases) {
      const entry = structuredClone(original);
      entry.update.message = { ...(entry.update.message as object), ...messagePatch };
      Object.assign(entry, patch);
      const expected = name === "command" || name === "media" || name === "album" ? "retain" : false;
      assert.equal(await routeRuntime.shouldReviewHistoricalInput(entry, { cwd: "/repo" }, signal), expected, name);
      if (name !== "bound" && name !== "threadless") {
        entry.update.message = { ...(entry.update.message as object), message_thread_id: 42,
          ...(expected === false ? { text: "/next" } : {}) };
        assert.equal(await routeRuntime.shouldReviewHistoricalInput(entry, { cwd: "/repo" }, signal), expected,
          `bound unsupported ${name} preserves its eligibility or owner/custody exclusion`);
      }
    }
    forbidContext = true;
    const ended = new AbortController(); ended.abort();
    await assert.rejects(async () => routeRuntime.shouldReviewHistoricalInput(original, { cwd: "/repo" }, ended.signal), /generation ended/);
    forbidContext = false;
    const missing = createRouteHarness({ getCurrentLeaderEpoch: () => 1, getAdmissionJournalBinding: () => "source", isContextActive: () => true });
    await assert.rejects(async () => missing.routeRuntime.shouldReviewHistoricalInput(original, { cwd: "/repo" }, signal), /unavailable/);
  });
});

for (const scenario of ["bound", "drift", "forwarded", "projected-media"] as const)
  test(`Historical live bindings retain routing and late drift holds before dispatch (${scenario})`, async () => {
  const drift = scenario !== "bound";
  const held = scenario === "drift";
  await withTopicStore(async (threadStore, path) => {
    const target = { chatId: 100, threadId: 99 };
    threadStore.upsert({ profileKey: "cwd:/repo", target, status: "active", createdAtMs: 1, updatedAtMs: 1,
      instanceId: "leader-a", slot: "A", threadName: "Axial" });
    await threadStore.persist();
    const options = { path: `${path}.late-history`, botIdentity: Journal.createTelegramUpdateJournalBotIdentity({ botToken: "fixture:late-history" }) };
    const binding = Journal.createTelegramUpdateJournalBindingKey(options);
    const journal = Journal.createTelegramUpdateJournalStore(options);
    journal.appendBatch([{ update_id: 123, ...(scenario === "forwarded" || scenario === "projected-media" ? { pi_telegram_forwarded: true } : {}),
      message: { message_id: 12, message_thread_id: 99, chat: { id: 100, type: "private" },
        from: { id: 7, is_bot: false }, text: "bound original", ...(scenario === "projected-media" ? { voice: {} } : {}) } }]);
    const harness = createRouteHarness({ threadStore, getAdmissionJournalBinding: () => binding,
      getCurrentLeaderEpoch: () => 1, isContextActive: () => true,
      async sendInteractiveMessage() { assert.fail("Late ownership drift cannot recreate a chooser"); } });
    let classifications = 0;
    const errors: string[] = [];
    const worker = Updates.createTelegramUpdateAdmissionWorkerRuntime<TestUpdate & Journal.TelegramJournaledUpdate, TestContext>({
      journal, getJournalBindingKey: () => binding, hasAuthority: () => true,
      async shouldReviewHistoricalInput(entry, ctx, signal) {
        classifications++; const verdict = await harness.routeRuntime.shouldReviewHistoricalInput(entry, ctx, signal);
        assert.equal(verdict, false); return verdict;
      },
      registry: { version: 1, add: () => () => {}, async dispatch(input) {
        assert.equal(classifications, 1);
        // Handler projections cannot turn accepted envelopes or media into reviewable originals.
        const update = input as Record<string, unknown>;
        delete update.pi_telegram_forwarded;
        delete (update.message as Record<string, unknown>).voice;
        if (drift) { assert.equal(threadStore.markStaleByTarget(target, "unknown", "fixture ownership drift"), true); await threadStore.persist(); }
        return "pass" as const;
      } },
      async defaultHandle(update, ctx, execution) {
        try { await harness.routeRuntime.handleUpdate(update, ctx, execution); }
        catch (error) { errors.push(error instanceof Error ? error.stack! : String(error)); throw error; }
      },
    });
    try {
      worker.start({ cwd: "/repo" }); await worker.waitForDrain();
      assert.equal(classifications, 1);
      assert.equal(worker.getState().historicalClaimCount ?? 0, held ? 1 : 0,
        JSON.stringify({ errors, state: worker.getState(), entries: journal.read().entries, events: harness.events, records: threadStore.list() }));
      assert.equal(harness.telegramQueueStore.getQueuedItems().length, held ? 0 : 1);
      assert.equal(journal.read().entries[0]?.state, held ? "pending" : "queued");
      assert.equal(journal.read().operatorDispositions, undefined);
    } finally { await worker.stop(); }
  });
});

for (const scenario of ["expiry", "half-hour", "restore-menu", "cancel", "restart", "stale-timer", "missing-view", "expiry-fault", "lost-ack", "group", "group-half-hour", "unconfirmed", "clock-before", "clock-after", "refused-restore", "wrong-target", "selection-fault", "operator-loss", "epoch-loss", "authority-loss", "binding-loss", "selection-boundary"] as const) {
  test(`Native routing TTL discards chooser sources without archiving or touching unowned tabs (${scenario})`, async () => {
    await withTopicStore(async (threadStore, path) => {
      threadStore.upsert({ profileKey: "cwd:/repo", target: { chatId: 100, threadId: 42 }, status: "active",
        createdAtMs: 1, updatedAtMs: 1, instanceId: "leader-a", slot: "A", threadName: "Axial" });
      await threadStore.persist(); await threadStore.load();
      let now = 1000, active = true, currentBinding = "", fault = false, copies = 0, operator = 7, epoch = 1;
      const grouped = scenario === "group" || scenario === "group-half-hour";
      const options = { path: `${path}.ttl`, botIdentity: Journal.createTelegramUpdateJournalBotIdentity({ botToken: "fixture:ttl" }), getNowMs: () => now,
        onPublicationBoundary(point: string, target: string) {
          const retention = target.startsWith(`${path}.ttl.retained`);
          if (point === "before-write" && retention) copies++;
          if (fault && scenario === "expiry-fault" && point === "before-write" && !retention) throw new Error("fixture TTL expiry failed");
        } };
      const binding = Journal.createTelegramUpdateJournalBindingKey(options); currentBinding = binding;
      const journal = Journal.createTelegramUpdateJournalStore(options);
      const lifetimePort = journal.routingInputs!;
      journal.routingInputs = { ...lifetimePort,
        arm(input) {
          if (scenario === "clock-before") throw new Error("fixture clock publication refused");
          const result = lifetimePort.arm(input);
          if (scenario === "clock-after") throw new Error("fixture clock ACK lost");
          return result;
        }, select(input) {
          const result = lifetimePort.select(input);
          if (scenario === "selection-fault") throw new Error("fixture selection ACK lost");
          return result;
        }, expire(input) {
        const result = lifetimePort.expire(input);
        if (fault && scenario === "lost-ack") { fault = false; throw new Error("fixture TTL removal ACK lost"); }
        return result;
      } };
      let carrier: TestMessage | undefined;
      const edits: string[] = [], completed: number[] = [], timers: Array<{ callback: () => void; delay: number; cancelled: boolean }> = [];
      let apiCalls = 0;
      const createHarness = () => createRouteHarness({ threadStore, isContextActive: () => active,
        configStore: { get: () => ({} as never), getAllowedUserId: () => operator, persistAllowedUserId: async () => true, persist: async () => undefined },
        getAdmissionJournalBinding: () => currentBinding, getCurrentLeaderEpoch: () => epoch,
        ...(scenario === "unconfirmed" ? { sendInteractiveMessage: async () => undefined } : {}),
        async downloadFile(id) {
          const image = `${path}.${id}.png`;
          await writeFile(image, Buffer.from("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+a7XcAAAAASUVORK5CYII=", "base64"));
          return image;
        },
        async runWorkspaceOperation(input, operation) {
          if (scenario === "epoch-loss" && input.operationKind === "workspace.expire-unbound-routing") epoch++;
          return operation();
        },
        async editInteractiveMessage(_chat, _id, text) { edits.push(text); if (scenario === "missing-view") throw new Error("fixture chooser no longer available"); },
        async callApi() { apiCalls++; return true as never; } });
      let harness = createHarness();
      const worker = Updates.createTelegramUpdateAdmissionWorkerRuntime<TestUpdate & Journal.TelegramJournaledUpdate, TestContext>({
        journal, getJournalBindingKey: () => currentBinding, hasAuthority: () => true, isContextCurrent: () => active,
        getQueueOwnerIdentity: () => ({ instanceId: "leader-a", processId: process.pid, processBirthId: "fixture-ttl", sessionGeneration: 1 }),
        getNowMs: () => now, scheduleRetry(callback, delay) { const timer = { callback, delay, cancelled: false }; timers.push(timer); return timer; },
        cancelRetry(handle) { (handle as typeof timers[number]).cancelled = true; },
        expireRoutingInput: (source, ctx, signal) => harness.routeRuntime.expireRoutingInput(source, ctx, signal),
        shouldReviewHistoricalInput: (entry, ctx, signal) => harness.routeRuntime.shouldReviewHistoricalInput(entry, ctx, signal),
        defaultHandle: (update, ctx, execution) => {
          if (update.update_id === 123) carrier = update.message;
          return harness.routeRuntime.handleUpdate(update, ctx, execution);
        },
        onUpdateCompleted: id => completed.push(id),
        recordRuntimeEvent: (category, error, details) => harness.events.push(`worker:${category}:${String(error)}:${JSON.stringify(details)}`),
      });
      const click = async (data: string, id = 200) => {
        journal.appendBatch([{ update_id: id, callback_query: { id: `ttl-${id}`, from: { id: 7, is_bot: false }, data,
          message: { message_id: 99, message_thread_id: 99, chat: { id: 100, type: "private" } } } }]);
        worker.signal(); await worker.waitForDrain();
        await new Promise(resolve => setImmediate(resolve)); await worker.waitForDrain();
      };
      try {
        worker.start({ cwd: "/repo" }); await worker.waitForDrain();
        journal.appendBatch([{ update_id: 123, message: { message_id: 12, message_thread_id: 99, chat: { id: 100, type: "private" },
          from: { id: 7, is_bot: false }, text: "unselected original",
          ...(grouped ? { media_group_id: "ttl-group", photo: [{ file_id: "photo-a", file_unique_id: "photo-a", width: 1, height: 1 }] } : {}) } },
          ...(grouped ? [{ update_id: 124, message: { message_id: 13, message_thread_id: 99, chat: { id: 100, type: "private" as const },
            from: { id: 7, is_bot: false }, media_group_id: "ttl-group", photo: [{ file_id: "photo-b", file_unique_id: "photo-b", width: 1, height: 1 }] } }] : [])]);
        worker.signal(); await worker.waitForDrain();
        if (grouped) { await new Promise(resolve => setTimeout(resolve, 1250)); await worker.waitForDrain(); }
        const original = journal.read().entries.find(value => value.updateId === 123)!;
        const originals = journal.read().entries;
        if (scenario === "unconfirmed") {
          assert.equal(original.routingInput, undefined);
          now = 4_000_000; worker.signal(); await worker.waitForDrain();
          assert.deepEqual(journal.read().entries, originals, "Unconfirmed publication supplies no deadline"); return;
        }
        if (scenario === "clock-before" || scenario === "clock-after") {
          assert.equal(original.routingInput?.expiresAtMs, scenario === "clock-after" ? 3_601_000 : undefined);
          await click("reroute:1:42");
          assert.equal(harness.telegramQueueStore.getQueuedItems().length, 0, "Unknown clock publication cannot skip selection authority");
          now = 4_000_000; worker.signal(); await worker.waitForDrain();
          assert.equal(journal.read().entries.some(entry => entry.updateId === 123), scenario === "clock-before", "A lost clock ACK reconciles only exact persisted lifetime metadata");
          if (scenario === "clock-after") assert.equal(Updates.getTelegramUpdateExecutionFence(carrier)?.isCurrent(), false);
          await worker.stop(); harness = createHarness(); worker.start({ cwd: "/repo" }); await worker.waitForDrain();
          assert.equal(journal.read().entries.some(entry => entry.updateId === 123), scenario === "clock-before", "Reconstruction uses only a positively retained clock");
          assert.equal(harness.telegramQueueStore.getQueuedItems().length, 0);
          assert.equal(completed.includes(123), false); return;
        }
        if (grouped) assert.equal(originals[1]?.routingInput?.expiresAtMs, 3_601_000);
        assert.equal(original.routingInput?.expiresAtMs, 3_601_000, JSON.stringify({ events: harness.events, state: worker.getState(), original }));
        const initialApiCalls = apiCalls;
        const firstTimer = timers.find(value => !value.cancelled)!;
        assert.ok(firstTimer); assert.equal(firstTimer.delay, 3_600_000);
        now += 30 * 60_000;
        if (scenario === "half-hour" || scenario === "group-half-hour") {
          await click("reroute:1:42");
          assert.equal(harness.telegramQueueStore.getQueuedItems().length, 1, JSON.stringify({ events: harness.events, snapshot: journal.read(), state: worker.getState() }));
          assert.equal(journal.read().entries.find(value => value.updateId === 123)?.routingInput?.phase, "selected");
          if (grouped) assert.equal(journal.read().entries.find(value => value.updateId === 124)?.routingInput?.phase, "selected");
          now = 4_000_000; worker.signal(); await worker.waitForDrain();
          assert.equal(journal.read().entries.find(value => value.updateId === 123)?.routingInput?.phase, "selected", JSON.stringify({ events: harness.events, state: worker.getState(), entries: journal.read().entries }));
          assert.equal(harness.telegramQueueStore.getQueuedItems().length, 1);
          assert.equal(journal.read().operatorDispositions, undefined); return;
        }
        if (scenario === "selection-fault") {
          await click("reroute:1:42");
          assert.equal(journal.read().entries.find(entry => entry.updateId === 123)?.routingInput?.phase, "selected");
          now = 4_000_000; worker.signal(); await worker.waitForDrain();
          await click("reroute:1:42", 201);
          assert.equal(harness.telegramQueueStore.getQueuedItems().length, 0);
          assert.equal(journal.routingInputs!.inspectExpiry(123)?.operatorAuthorityId, "telegram-owner:7");
          assert.equal(journal.read().entries.some(entry => entry.updateId === 123), false); return;
        }
        if (scenario === "refused-restore" || scenario === "wrong-target") {
          await click(scenario === "refused-restore" ? "reroutenew:1:42" : "reroute:1:43");
          assert.equal(journal.read().entries.find(value => value.updateId === 123)?.routingInput?.phase, "waiting");
        }
        if (scenario === "restore-menu") {
          await click("rerouterestore:1");
          assert.equal(journal.read().entries.find(value => value.updateId === 123)?.routingInput?.phase, "waiting", "Browsing Restore is not a target choice");
        }
        if (scenario === "cancel") {
          await click("reroutecancel:1"); assert.equal(journal.read().entries.length, 0); assert.equal(completed.includes(123), false); return;
        }
        if (scenario === "restart" || scenario === "stale-timer") {
          await worker.stop(); harness = createHarness(); worker.start({ cwd: "/repo" }); await worker.waitForDrain();
          assert.equal(journal.read().entries[0]?.routingInput?.expiresAtMs, original.routingInput!.expiresAtMs);
          if (scenario === "stale-timer") { firstTimer.callback(); await worker.waitForDrain(); assert.equal(journal.read().entries.length, 1); }
        }
        if (scenario === "operator-loss") operator = 8;
        if (scenario === "authority-loss") active = false;
        if (scenario === "binding-loss") currentBinding = "foreign";
        now = original.routingInput!.expiresAtMs;
        if (scenario === "selection-boundary") await click("reroute:1:42");
        fault = scenario === "expiry-fault" || scenario === "lost-ack";
        const timer = timers.findLast(value => !value.cancelled);
        timer?.callback(); worker.signal(); await worker.waitForDrain();
        if (scenario === "authority-loss" || scenario === "binding-loss" || scenario === "operator-loss" || scenario === "epoch-loss") {
          assert.deepEqual(journal.read().entries, [original]); assert.equal(apiCalls, initialApiCalls); return;
        }
        if (scenario === "lost-ack") {
          assert.equal(journal.read().entries.some(value => value.updateId === 123), false);
          worker.signal(); await worker.waitForDrain();
          assert.equal(copies, 0, "Lost expiry ACK reconciles a body-free discard tombstone");
        }
        if (fault) {
          assert.equal(journal.read().entries.some(value => value.updateId === 123), true);
          assert.equal(harness.telegramQueueStore.getQueuedItems().length, 0);
          fault = false; worker.signal(); await worker.waitForDrain();
        }
        assert.equal(journal.read().entries.some(value => value.updateId === 123), false, JSON.stringify({ events: harness.events, state: worker.getState() }));
        assert.equal(journal.inspectAbandonedPending(123), undefined);
        assert.equal(journal.routingInputs!.inspectExpiry(123)?.operatorAuthorityId, "telegram-owner:7");
        assert.equal(copies, 0, "TTL never archives prompt bodies");
        if (grouped) {
          assert.equal(journal.read().entries.some(value => value.updateId === 124), false);
          assert.ok(journal.routingInputs!.inspectExpiry(124));
        }
        assert.equal(harness.telegramQueueStore.getQueuedItems().length, 0);
        assert.equal(completed.includes(123), false, "Expiry is not task completion");
        if (scenario === "expiry") assert.deepEqual(edits.filter(text => text.includes("\u231b")), ["<b>\u231b Routing choice expired.</b>"]);
        assert.equal(apiCalls, initialApiCalls, "TTL never probes or deletes an unowned tab");
        if (scenario === "expiry") {
          assert.equal(Updates.getTelegramUpdateExecutionFence(carrier)?.isCurrent(), false);
          Updates.reportTelegramUpdateCompleted(carrier);
          Updates.reportTelegramQueueAdmission([carrier], [{ receiptId: "too-late", queueKind: "prompt", sourceUpdateIds: [123], journalBindingKey: binding }]);
          await new Promise<void>(resolve => setImmediate(resolve)); await worker.waitForDrain();
          assert.equal(Updates.getTelegramUpdateExecutionFence(carrier)?.isCurrent(), false, "A late report cannot revive an expired carrier");
          assert.equal(worker.getState().phase, "idle", "A late report does not block unrelated work");
          assert.equal(journal.read().entries.length, 0);
        }
        assert.equal(journal.read().operatorDispositions?.length, grouped ? 2 : 1);
        await click("reroute:1:42", 201);
        assert.equal(harness.telegramQueueStore.getQueuedItems().length, 0, "Stale chooser cannot revive archived input");
        assert.deepEqual(journal.appendBatch([original.update]).duplicateUpdateIds, [123]);
      } finally { active = true; currentBinding = binding; await worker.stop(); }
    });
  });
}

for (const restart of [false, true]) {
  test(`Retired Historical inputs controls neither reopen a submenu nor settle held input (restart=${restart})`, async () => {
    await withTopicStore(async (threadStore, path) => {
      threadStore.upsert({ profileKey: "cwd:/repo", target: { chatId: 100, threadId: 42 },
        status: "active", createdAtMs: 1, updatedAtMs: 1, instanceId: "leader-a", slot: "A", threadName: "Axial" });
      await threadStore.persist(); await threadStore.load();
      const records = structuredClone(threadStore.list());
      const options = { path: `${path}.history`, botIdentity: Journal.createTelegramUpdateJournalBotIdentity({ botToken: "fixture:history" }) };
      const binding = Journal.createTelegramUpdateJournalBindingKey(options);
      const journal = Journal.createTelegramUpdateJournalStore(options);
      journal.appendBatch([{ update_id: 123, message: { message_id: 12, message_thread_id: 99, chat: { id: 100, type: "private" },
        from: { id: 7, is_bot: false }, text: "held original" } }]);
      const original = journal.read().entries[0]!;
      const createHarness = () => createRouteHarness({ threadStore,
        configStore: { get: () => ({} as never), getAllowedUserId: () => 7, persistAllowedUserId: async () => true, persist: async () => undefined },
        getAdmissionJournalBinding: () => binding, getCurrentLeaderEpoch: () => 1, isContextActive: () => true,
        async sendInteractiveMessage() { assert.fail("Retired UI cannot publish a chooser"); },
        async editInteractiveMessage() { assert.fail("Retired UI cannot reopen its submenu"); },
        async runWorkspaceOperation() { assert.fail("Retired UI cannot acquire mutation admission"); },
        async callApi() { assert.fail("Retired UI cannot mutate Threads"); },
        async deleteMessage() { assert.fail("Retired UI cannot delete messages"); } });
      let harness = createHarness();
      const executed: number[] = [];
      const worker = Updates.createTelegramUpdateAdmissionWorkerRuntime<TestUpdate & Journal.TelegramJournaledUpdate, TestContext>({
        journal: { ...journal, abandonPending() { assert.fail("Retired UI cannot abandon input"); } },
        getJournalBindingKey: () => binding, hasAuthority: () => true,
        shouldReviewHistoricalInput: (entry, ctx, signal) => harness.routeRuntime.shouldReviewHistoricalInput(entry, ctx, signal),
        async defaultHandle(update, ctx, execution) { executed.push(update.update_id); await harness.routeRuntime.handleUpdate(update, ctx, execution); },
      });
      try {
        worker.start({ cwd: "/repo" }); await worker.waitForDrain();
        if (restart) { await worker.stop(); harness = createHarness(); worker.start({ cwd: "/repo" }); await worker.waitForDrain(); }
        assert.deepEqual(executed, []);
        for (const [index, suffix] of ["open", "nonce:choose:0", "nonce:confirm:0", "nonce:retry:0", "nonce:text", "nonce:list", "nonce:refresh", "nonce:more"].entries()) {
          journal.appendBatch([{ update_id: 200 + index, callback_query: { id: `history-${index}`, from: { id: 7, is_bot: false },
            data: `reroutecancel:history:${suffix}`, message: { message_id: 500, message_thread_id: 42, chat: { id: 100, type: "private" } } } }]);
          worker.signal(); await worker.waitForDrain();
          assert.match(harness.events.filter(value => value.startsWith("answer:")).at(-1)!, /no longer available/);
          assert.deepEqual(journal.read().entries, [original], "Old callbacks leave the full original intact");
          assert.equal(worker.getState().historicalClaimCount, 1);
          assert.equal(journal.read().operatorDispositions, undefined);
        }
        assert.deepEqual(threadStore.list(), records);
        assert.equal(harness.telegramQueueStore.getQueuedItems().length, 0);
        assert.ok(executed.every(id => id >= 200));
      } finally { await worker.stop(); }
    });
  });
}

for (const scenario of ["ready", "adopted", "routed", "issued", "multi-partial", "multi-all", "gated", "publication-before", "tampered", "no-port", "foreign"] as const) {
  test(`Cold retained abandonment ends only an unsent ready Restore without historical UI (${scenario})`, async () => {
    await fixture(async ({ store, threads, auth, path }) => {
      const options = { path: `${path}.restore-history`, botIdentity: Journal.createTelegramUpdateJournalBotIdentity({ botToken: "fixture:restore-history" }) };
      const binding = Journal.createTelegramUpdateJournalBindingKey(options);
      const journal = Journal.createTelegramUpdateJournalStore(options);
      const sourceIds = scenario.startsWith("multi") ? [123, 124] : [123];
      journal.appendBatch(sourceIds.map(id => ({ update_id: id, message: { message_id: id, message_thread_id: 42,
        chat: { id: 7, type: "private" }, from: { id: 7, is_bot: false }, text: `restore original ${id}` } })));
      const request = { operationId: "history-restore", binding: threads.listWorkspaceBindings()[0]!, owner: threads.list()[0]!,
        target: { chatId: 7, threadId: 42 }, source: { journalBindingKey: binding, updateIds: sourceIds } };
      let retained = store.issueRecipient((await store.commit(request, auth))!, recipient("leader"), auth)!.intent;
      if (scenario !== "issued") retained = store.confirmReady(retained, recipient("leader"), auth)!;
      if (scenario === "routed") retained = store.issueRouting(retained, auth)!.intent;
      const records = structuredClone(threads.list()), bindings = structuredClone(threads.listWorkspaceBindings());
      // Supplied precondition: exact abandonment was committed before startup. This is not a live UI grant or actual interrupted startup.
      for (const entry of journal.read().entries) {
        if (scenario === "multi-partial" && entry.updateId === 124) continue;
        journal.abandonPending({ journalBindingKey: binding, entry, operatorAuthorityId: "telegram-owner:7", isCurrent: () => true });
      }
      if (scenario === "tampered") {
        const retention = journal.inspectAbandonedPending(123)!.retainedPath;
        const copy = JSON.parse(await readFile(retention, "utf8"));
        copy.entry.update.message.text = "changed"; await writeFile(retention, JSON.stringify(copy));
      }
      let failRetire = scenario === "publication-before";
      const restoreStore: Threads.TelegramWorkspaceRestore = { ...store, retireAbandoned(expected, ids, authority) {
        if (failRetire) { failRetire = false; throw new Error("fixture Restore publication interrupted"); }
        return store.retireAbandoned(expected, ids, authority);
      } };
      const harness = createRouteHarness({ threadStore: threads, instanceId: "leader", getSessionGeneration: () => 1,
        ...(scenario === "no-port" ? {} : { inspectRestoreSourceAbandonment(id: number) {
          const proof = journal.inspectAbandonedPending(id);
          return scenario === "foreign" && proof ? { ...proof, operatorAuthorityId: "telegram-owner:8" } : proof;
        } }),
        configStore: { get: () => ({} as never), getAllowedUserId: () => 7, persistAllowedUserId: async () => true, persist: async () => undefined },
        getAdmissionJournalBinding: () => binding, getCurrentLeaderEpoch: () => scenario === "adopted" ? "next-epoch" : "epoch",
        isContextActive: () => true, getWorkspaceRestoreStore: () => restoreStore, hasWorkspaceRestoreAuthority: () => scenario !== "gated",
        async sendInteractiveMessage() { assert.fail("A held Restore original cannot publish a chooser"); },
        async editInteractiveMessage() { assert.fail("Cold abandonment cannot reopen historical UI"); },
        async runWorkspaceOperation(_input, operation) { return operation(); },
        async callApi() { assert.fail("Cold abandonment cannot mutate Threads"); },
        async deleteMessage() { assert.fail("Cold abandonment cannot delete messages"); } });
      const executed: number[] = [];
      const worker = Updates.createTelegramUpdateAdmissionWorkerRuntime<TestUpdate & Journal.TelegramJournaledUpdate, TestContext>({
        journal, getJournalBindingKey: () => binding, hasAuthority: () => true,
        onUpdateCompleted: (id, ctx, key) => harness.routeRuntime.onUpdateCompleted(id, ctx, key),
        async shouldReviewHistoricalInput(entry, ctx, signal) {
          if (store.list().some(intent => intent.request.source.updateIds.includes(entry.updateId))) return true;
          return harness.routeRuntime.shouldReviewHistoricalInput(entry, ctx, signal);
        },
        async defaultHandle(update, ctx, execution) { executed.push(update.update_id); await harness.routeRuntime.handleUpdate(update, ctx, execution); },
      });
      const wake = async (id: number) => {
        journal.appendBatch([{ update_id: id, callback_query: { id: `restore-history-${id}`, from: { id: 7, is_bot: false },
          data: "reroutecancel:history:open", message: { message_id: 500, message_thread_id: 10, chat: { id: 7, type: "private" } } } }]);
        worker.signal(); await worker.waitForDrain(); await harness.routeRuntime.waitForRestoreSettlement();
      };
      try {
        worker.start({ cwd: "/repo" }); await worker.waitForDrain();
        assert.deepEqual(store.list(), [retained], "Startup and source absence alone never retire Restore");
        assert.equal(worker.getState().historicalClaimCount ?? 0, scenario === "multi-partial" ? 1 : 0);
        await wake(200);
        if (scenario === "publication-before") { assert.deepEqual(store.list(), [retained]); await wake(201); }
        const recovered = ["ready", "adopted", "multi-all", "publication-before"].includes(scenario);
        assert.deepEqual(store.list(), recovered ? [] : [retained]);
        assert.deepEqual(threads.list(), records);
        assert.deepEqual(threads.listWorkspaceBindings(), bindings);
        assert.equal(journal.read().operatorDispositions?.filter(item => item.updateId === 123).length, 1);
        assert.equal(harness.telegramQueueStore.getQueuedItems().length, 0);
        assert.ok(executed.every(id => id >= 200));
      } finally { await worker.stop(); }
    });
  });
}

/** All-command temporary-Thread scenarios with shared native setup and behavior-specific assertions. */
const ALL_COMMAND_SCENARIOS = {
  // Creation, presentation and authority
  creation: [
    "created", "create-error", "no-thread-id", "publication-failure", "authority-off", "foreign-sender", "expired-new",
    "expired-presented", "prompt-in-tab",
  ],
  // Forward from the temporary tab, including follower IPC and lost replies
  forward: [
    "forward", "forward-double", "forward-authority-lost", "forward-with-prompt", "forward-other-tab",
    "forward-then-cancel", "forward-sibling-follower", "forward-sibling-follower-ipc",
    "forward-sibling-follower-ipc-worker", "forward-sibling-follower-ipc-lost-reply",
    "forward-sibling-follower-ipc-worker-lost-reply", "forward-sibling-follower-ipc-lost-reply-retry",
    "forward-sibling-follower-ipc-lost-reply-restart", "forward-sibling-follower-ipc-issue-before",
    "forward-sibling-follower-ipc-issue-after", "forward-sibling-follower-ipc-worker-lost-reply-retry",
    "forward-sibling-follower-ipc-worker-lost-reply-restore-other", "forward-prompt-then-cancel",
  ],
  // Cancel routing, proofs and markers
  cancel: [
    "cancel", "cancel-double", "cancel-delete-fails", "cancel-after-restart", "cancel-with-prompt",
    "cancel-two-prompts", "cancel-two-prompts-reverse", "cancel-proof-missing", "cancel-proof-foreign",
    "cancel-proof-authority-loss", "cancel-marker-before", "cancel-marker-after", "cancel-marker-no-proof",
    "cancel-then-forward",
  ],
  // Delayed cleanup and membership
  cleanup: [
    "cleanup-new-input", "cleanup-census-blocks", "cleanup-census-unavailable", "cleanup-authority-loss",
    "cleanup-proof-lost", "cleanup-delete-fails", "membership-cold", "membership-before", "membership-after",
    "membership-media-group", "membership-text-real-gate",
  ],
  // Restore, siblings, local/queue proofs and full slots
  restore: [
    "restore-sibling-follower", "restore-sibling-follower-ipc", "restore-sibling-follower-ipc-down",
    "restore-sibling-follower-ipc-worker", "restore-sibling-follower-ipc-lost-reply",
    "restore-sibling-follower-ipc-worker-lost-reply", "restore-sibling-follower-ipc-apply-before-reply",
    "restore-sibling-follower-ipc-apply-after-reply", "restore-sibling-follower-ipc-inspect-after-reply",
    "restore", "restore-sibling", "restore-sibling-authority-lost",
    "restore-sibling-photo", "restore-sibling-before", "restore-sibling-restart", "restore-sibling-cold-successor", "restore-sibling-from-prompt",
    "restore-sibling-from-prompt-completed", "restore-sibling-from-prompt-forward-first",
    "restore-sibling-from-prompt-restore-first", "restore-sibling-from-prompt-forward-first-authority-lost",
    "restore-full-slots", "restore-sibling-after-forward", "restore-local-before", "restore-local-after",
    "restore-local-no-proof", "restore-queue-continue", "restore-local-compact", "restore-queue-before",
    "restore-queue-after", "restore-queue-no-proof", "restore-queue-cold",
  ],
} as const;
type AllCommandScenario = (typeof ALL_COMMAND_SCENARIOS)[keyof typeof ALL_COMMAND_SCENARIOS][number];

function createAllCommandFixture(scenario: AllCommandScenario,
  { threads, open, path }: Parameters<Parameters<typeof fixture>[0]>[0], journalPath = `${path}.all`, expiryClock?: () => number) {
  const asFollower = scenario.includes("-follower"), realIpc = scenario.includes("-ipc"), drainRecipient = scenario.includes("-worker"), loseDeliveryReply = scenario.includes("-lost-reply");
  const restoreReplyBoundary = scenario === "restore-sibling-follower-ipc-apply-before-reply" ? "before" :
    scenario === "restore-sibling-follower-ipc-apply-after-reply" || scenario === "restore-sibling-follower-ipc-inspect-after-reply" ? "after" : undefined;
  const restoreModes: Array<{ mode: "apply" | "inspect"; generation: string }> = [];
  const restoreReplyGates = new Map<"apply" | "inspect", { wait: Promise<void>; release: () => void; finished: Promise<void>; finish: () => void }>();
  const heldRestoreModes: Array<"apply" | "inspect"> = restoreReplyBoundary ? ["apply"] : [];
  if (scenario.endsWith("-inspect-after-reply")) heldRestoreModes.push("inspect");
  for (const mode of heldRestoreModes) {
    let release!: () => void, finish!: () => void;
    const wait = new Promise<void>(resolve => { release = resolve; });
    const finished = new Promise<void>(resolve => { finish = resolve; });
    restoreReplyGates.set(mode, { wait, release, finished, finish });
  }
  const options = { path: journalPath, botIdentity: Journal.createTelegramUpdateJournalBotIdentity({ botToken: "fixture:all" }),
    ...(expiryClock ? { getNowMs: expiryClock } : {}) };
  const bindingKey = Journal.createTelegramUpdateJournalBindingKey(options);
  const resolve = Journal.createTelegramUpdateJournalRuntimeBindingResolver({ getProfileName: () => undefined,
    getBotToken: () => "fixture:all", getBotId: () => undefined, getJournalPath: () => options.path,
    withSourceSerialization: createTelegramConfigStore({ agentDir: dirname(path) }).withSourceSerialization });
  const journal = resolve()!.journal;
  if (expiryClock) journal.routingInputs = Journal.createTelegramUpdateJournalStore(options).routingInputs;
  const queuedCommand = scenario.startsWith("restore-queue-") || scenario.startsWith("restore-sibling-from-prompt") ||
    scenario === "membership-media-group" || scenario === "membership-text-real-gate";
  const queueOwnedRestore = queuedCommand || scenario === "restore-sibling-after-forward";
  const selectedRestoreUpdateId = scenario.startsWith("restore-sibling-from-prompt") || scenario === "restore-sibling-after-forward" ? 150 : 123;
  const expired = scenario.startsWith("expired");
  // Cancel needs fresh-cancellation evidence: the command must arrive after the worker's first snapshot.
  const arrivesLive = !!expiryClock || scenario.startsWith("cancel") || scenario.startsWith("cleanup-") || scenario.endsWith("-retry") || scenario.endsWith("-restart") || scenario.startsWith("restore-sibling-from-prompt") ||
    scenario === "membership-media-group" || scenario === "membership-text-real-gate" || scenario === "forward-prompt-then-cancel";
  const appendOriginal = () => journal.appendBatch([{ update_id: 123, message: { message_id: 12,
    date: Math.floor(Date.now() / 1000) - (expired ? 7200 : 0), chat: { id: 7, type: "private" },
    from: { id: scenario === "foreign-sender" ? 8 : 7, is_bot: false }, text: queuedCommand ? "/continue" : scenario === "restore-local-compact" ? "/compact" : "/start" } }]);
  if (!arrivesLive) appendOriginal();
  const store = open();
  let acceptancePublications = 0, queuePublicationFault = scenario === "restore-queue-cold";
  let membershipFault = scenario === "membership-before" || scenario === "membership-after";
  let cancellationFault = scenario === "cancel-marker-before" || scenario === "cancel-marker-after" || scenario === "cancel-marker-no-proof";
  let issueFault = scenario.endsWith("-issue-before") || scenario.endsWith("-issue-after");
  const restoreStore = { ...store, recordTemporaryThreadForwardIssued(expected: Threads.TelegramTemporaryThreadEntry, input: Threads.TelegramTemporaryThreadInput,
    authority: Threads.TelegramWorkspaceRestoreAuthority) {
    if (!issueFault) return store.recordTemporaryThreadForwardIssued(expected, input, authority);
    issueFault = false;
    return open({ onPublicationBoundary(point) {
      if (point === (scenario.endsWith("-issue-after") ? "after-rename" : "after-write-before-rename")) throw new Error("Fixture Forward issuance publication interrupted");
    } }).recordTemporaryThreadForwardIssued(expected, input, authority);
  }, recordSourceAcceptance(expected: Threads.TelegramWorkspaceRestoreIntent,
    evidence: Threads.TelegramWorkspaceRestoreSourceAcceptance, authority: Threads.TelegramWorkspaceRestoreAuthority) {
    acceptancePublications++;
    assert.equal(evidence.updateId, selectedRestoreUpdateId);
    assert.equal(evidence.kind, asFollower ? "forwarded" : queueOwnedRestore ? "queued" : "completed");
    assert.equal(journal.read().entries.find(value => value.updateId === selectedRestoreUpdateId)?.state, queueOwnedRestore ? "queued" : "pending",
      "local execution reports acceptance before source disposal");
    assert.equal(evidence.sourceSha256, Journal.createTelegramUpdateJournalEntryDigest(journal.read().entries.find(value => value.updateId === selectedRestoreUpdateId)!).sourceSha256);
    if (scenario === "restore-local-no-proof" || scenario === "restore-queue-no-proof") return expected;
    if (scenario === "restore-queue-before" || scenario === "restore-queue-after" || queuePublicationFault) return open({ onPublicationBoundary(point) {
      if (point === (scenario === "restore-queue-after" ? "after-rename" : "after-write-before-rename")) {
        throw new Error("Fixture command queued acceptance interrupted");
      }
    } }).recordSourceAcceptance(expected, evidence, authority);
    if (scenario === "restore-local-before" || scenario === "restore-local-after" || scenario === "restore-sibling-before") return open({ onPublicationBoundary(point) {
      if (point === (scenario === "restore-local-after" ? "after-rename" : "after-write-before-rename")) {
        throw new Error("Fixture local acceptance publication interrupted");
      }
    } }).recordSourceAcceptance(expected, evidence, authority);
    return store.recordSourceAcceptance(expected, evidence, authority);
  }, recordTemporaryThreadInput(expected: Threads.TelegramTemporaryThreadEntry, input: Threads.TelegramTemporaryThreadInput,
    authority: Threads.TelegramWorkspaceRestoreAuthority) {
    if (membershipFault) return open({ onPublicationBoundary(point) {
      if (point === (scenario === "membership-after" ? "after-rename" : "after-write-before-rename")) {
        throw new Error("Fixture temporary membership publication interrupted");
      }
    } }).recordTemporaryThreadInput(expected, input, authority);
    return store.recordTemporaryThreadInput(expected, input, authority);
  }, recordTemporaryThreadInputCancellation(expected: Threads.TelegramTemporaryThreadEntry, input: Threads.TelegramTemporaryThreadInput,
    authority: Threads.TelegramWorkspaceRestoreAuthority, inspect: (id: number) => Threads.TelegramTemporaryThreadCancellationEvidence | undefined) {
    if (cancellationFault) {
      if (scenario === "cancel-marker-no-proof") return { ...expected, cancelledInputs: [structuredClone(input)] };
      return open({ onPublicationBoundary(point) {
        if (point === (scenario === "cancel-marker-after" ? "after-rename" : "after-write-before-rename")) {
          throw new Error("Fixture temporary cancellation publication interrupted");
        }
      } }).recordTemporaryThreadInputCancellation(expected, input, authority, inspect);
    }
    return store.recordTemporaryThreadInputCancellation(expected, input, authority, inspect);
  } };
  if (scenario === "expired-presented") {
    const auth = { executor: { instanceId: "leader", leaderEpoch: "epoch" }, operatorUserId: 7, isCurrent: () => true };
    store.acknowledgeTemporaryThread(store.reserveTemporaryThread({ journalBindingKey: bindingKey, updateId: 123 }, "c".repeat(32), auth)!.entry,
      { chatId: 7, threadId: 55 }, auth);
  }
  const creations: unknown[] = [];
  const apiCalls: Array<{ method: string; body: Record<string, unknown> }> = [];
  let temporaryAuthority = scenario !== "authority-off";
  const edits: string[] = [];
  let leaderIdentity: { target: Queue.TelegramQueueTarget; slot?: string; threadName?: string } =
    { slot: "A", target: { chatId: 7, threadId: 10 }, threadName: "Atlas" };
  const followerProtocol = Bus.createTelegramBusProtocolIdentity({ runtimeBuild: "fixture",
    capabilities: [Bus.TELEGRAM_BUS_CAPABILITY_WORKSPACE_RESTORE, Bus.TELEGRAM_BUS_CAPABILITY_DURABLE_FOLLOWER_ADMISSION] });
  const followerRegistry = Bus.createTelegramBusFollowerRegistry();
  const chooserOwnership = Ownership.createTelegramBusMessageOwnershipRuntime({ instanceId: asFollower ? "leader-a" : "old",
    getProfileKey: () => "profile-a:bot-a", listFollowers: followerRegistry.list });
  const forwardedSources: number[] = [];
  // Real IPC: a leader-side controller/forwarder over a Unix socket to a real follower receiver and durable recipient journal.
  const followerSocket = Bus.getTelegramBusFollowerSocketPath("old", dirname(path));
  const recipientJournal = Journal.createTelegramUpdateJournalStore({ ...options, path: `${path}.recipient` });
  const followerRegistration = createTelegramBusFollowerRegistrationState();
  let followerAuthorityCurrent = true, followerContextGeneration = 1;
  const ipcCtx: TestContext = { cwd: "/repo" };
  const ipcStore = Threads.createTelegramTopicTargetStore({ path, canPersist: () => false });
  const ipcRestore = ipcStore.workspaceRestore({ profileName: "default", tokenSha256: "a".repeat(64) });
  const followerExecutions: number[] = [], admittedIds: number[] = [];
  let releaseDeliveryReply: (() => void) | undefined;
  const deliveryReplyGate = new Promise<void>(resolve => { releaseDeliveryReply = resolve; });
  const followerHarness = drainRecipient ? createRouteHarness({ threadStore: ipcStore, instanceId: "old",
    isContextActive: () => followerAuthorityCurrent,
    configStore: { get: () => ({} as never), getAllowedUserId: () => 7, persistAllowedUserId: async () => true, persist: async () => undefined } }) : undefined;
  const followerWorker = followerHarness ? Updates.createTelegramUpdateAdmissionWorkerRuntime<TestUpdate & Journal.TelegramJournaledUpdate, TestContext>({
    journal: recipientJournal, hasAuthority: () => followerAuthorityCurrent,
    isContextCurrent: ctx => ctx === ipcCtx && followerAuthorityCurrent,
    getJournalBindingKey: () => Journal.createTelegramUpdateJournalBindingKey({ ...options, path: `${path}.recipient` }),
    async defaultHandle(update, ctx, execution) {
      if (update.message) followerExecutions.push(update.update_id);
      await followerHarness.routeRuntime.handleUpdate(update, ctx, execution);
    } }) : undefined;
  const ipcHandler = realIpc ? createTelegramBusFollowerWorkspaceRestoreHandler({ instanceId: "old", registrationState: followerRegistration,
    topicTargetStore: ipcStore, getWorkspaceAdmission: () => queueAdmission,
    readRestoreIntent: id => ipcRestore.list().find(value => value.request.operationId === id),
    getContextAuthority: () => followerAuthorityCurrent ? { profileBindingKey: "profile-a:bot-a", operatorUserId: 7,
      executor: { instanceId: "leader-a", leaderEpoch: "epoch" }, sessionId: "session", cwd: "/repo", generation: followerContextGeneration,
      leaderProtocol: followerProtocol } : undefined }) : undefined;
  const forwardedIds = (): number[] => drainRecipient ? admittedIds : realIpc ? recipientJournal.read().entries.map(entry => entry.updateId).sort((left, right) => left - right) : forwardedSources;
  const ipcAdmission = createTelegramBusFollowerDurableAdmissionRuntime({ journal: recipientJournal, signalWorker() { followerWorker?.signal(); } });
  const ipcReceiver = realIpc ? createTelegramBusForwardedUpdateReceiverRuntime({ socketPath: followerSocket, instanceId: "old",
    getAuthSecret: () => "ipc-secret", getRegistrationGeneration: followerRegistration.getGeneration,
    getRecipientBindingKey: () => "manual:old", getContext: () => ipcCtx, isWorkspaceRestoreEnabled: () => true,
    async handleWorkspaceRestore(input) {
      restoreModes.push({ mode: input.mode, generation: input.registrationGeneration });
      const gate = restoreReplyGates.get(input.mode);
      try {
        if (gate && restoreReplyBoundary === "before") await gate.wait;
        const result = await ipcHandler!(input, ipcCtx);
        if (gate && restoreReplyBoundary === "after") await gate.wait;
        return result;
      } finally { gate?.finish(); }
    },
    durableAdmission: { async admit(envelope, context) {
      const accepted = await ipcAdmission.admit(envelope, context);
      if (envelope.kind === "leader.forwardMessage") {
        admittedIds.push(accepted.sourceUpdateId);
        if (loseDeliveryReply) await deliveryReplyGate;
      }
      return accepted;
    } } }) : undefined;
  const ipcRunFollower = realIpc ? Bus.createTelegramBusWorkspaceRestoreController({ getFollower: followerRegistry.get,
    localProtocolIdentity: followerProtocol, getAuthSecret: () => "ipc-secret", timeoutMs: restoreReplyBoundary ? 500 : undefined,
    createRequestId: (() => { let n = 0; return () => `ipc-${++n}`; })() }) : undefined;
  const ipcForwarder = realIpc ? Bus.createTelegramBusForeignOwnedUpdateForwarder<TestContext, Updates.TelegramMessageReactionUpdated,
    TestCallbackQuery, TestMessage>({ socketPath: followerSocket, createRequestId: (() => { let n = 0; return () => `ipc-forward-${++n}`; })(),
    getAuthSecret: () => "ipc-secret", timeoutMs: loseDeliveryReply ? 500 : undefined }) : undefined;
  const followerDeps = asFollower ? {
    hasWorkspaceRestoreAuthority: () => true,
    workspaceRestoreRecipient: { getSessionId: () => "leader-session", getCwd: () => "/leader", getLeaderIdentity: () => undefined,
      followerRegistry, async runFollower(input: Parameters<NonNullable<typeof ipcRunFollower>>[0]) {
        if (ipcRunFollower) return ipcRunFollower(input);
        const live = followerRegistry.get(input.instanceId)!;
        return { operationId: input.operationId, recipient: { kind: "follower" as const, instanceId: live.instanceId, sessionId: input.sessionId,
          generation: live.registrationGeneration! }, target: input.target, slot: input.slot, ready: true };
      } },
    getTargetOwnership: (candidate: Queue.TelegramQueueTarget) => Bus.getTelegramFollowerTargetOwnership({ target: candidate,
      currentInstanceId: "leader-a", followers: followerRegistry.list(), activeThreadRecords: threads.list() }),
    foreignOwnedUpdateForwarder: ipcForwarder ?? { async forwardMessage(input: { message: TestMessage; ownership: { recipientBindingKey?: string } }) {
      const source = Updates.inspectTelegramDeferredSource(input.message);
      if (!source || !input.ownership.recipientBindingKey) throw new Error("fixture follower forward needs a journal-bound source and recipient");
      forwardedSources.push(source.updateId);
      const recipientBindingKey = input.ownership.recipientBindingKey;
      return { status: "accepted" as const, delivery: { sourceUpdateId: source.updateId, recipientBindingKey,
        deliveryId: Bus.createTelegramBusFollowerDeliveryIdentity({ kind: "leader.forwardMessage",
          recipientBindingKey, sourceUpdateId: source.updateId }).deliveryId } };
    } },
  } : {};
  if (asFollower) followerRegistry.register({ instanceId: "old", sessionId: "session", cwd: "/repo", slot: "A", target: { chatId: 7, threadId: 10 },
    registrationGeneration: "fresh", protocol: followerProtocol, busSocketPath: followerSocket, profileKey: "manual:old", connectedAtMs: 1 });
  followerRegistration.setRegistered(true, { chatId: 7, threadId: 10 }, { slot: "A", generation: "fresh", leaderProtocol: followerProtocol });
  const restoreDeps = asFollower ? followerDeps : scenario.startsWith("restore") ? {
    hasWorkspaceRestoreAuthority: () => true,
    setCurrentLeaderIdentity(identity: typeof leaderIdentity) { leaderIdentity = structuredClone(identity); },
    workspaceRestoreRecipient: { getSessionId: () => "session", getCwd: () => "/repo", getLeaderIdentity: () => leaderIdentity,
      followerRegistry: Bus.createTelegramBusFollowerRegistry(), async runFollower() { assert.fail("A leader Restore uses no follower IPC"); } },
  } : {};
  const queueAdmission = createTelegramWorkspaceAdmissionLedger({ path: `${path}.queue-admission`, profileKey: "profile-a:bot-a",
    owner: { processId: process.pid, processBirthId: `${process.pid}:temp-queue` }, getProcessLiveness: () => "alive" });
  const queueOperations = createTelegramWorkspaceOperationRuntime({ getWorkspaceAdmission: () => queueAdmission });
  const choosers: Array<{ target: unknown; text: string; markup: string }> = [];
  let failPublication = scenario === "publication-failure";
  let proofLost = false;
  const liveTargets: Queue.TelegramQueueTarget[] = [{ chatId: 7, threadId: 10 }];
  const createHarness = (overrides: RouteHarnessOptions = {}) => createRouteHarness({ threadStore: threads, instanceId: asFollower ? "leader-a" : "old", getSessionGeneration: () => 1,
    configStore: { get: () => ({} as never), getAllowedUserId: () => 7, persistAllowedUserId: async () => true, persist: async () => undefined },
    getAdmissionJournalBinding: () => bindingKey, getCurrentLeaderEpoch: () => temporaryAuthority ? "epoch" : undefined, isContextActive: () => true,
    getLiveThreadTargets: () => liveTargets, getWorkspaceRestoreStore: () => restoreStore,
    getMessageOwnership: chooserOwnership.getForwardOwnership,
    ...restoreDeps,
    temporaryThreadCleanupDelayMs: scenario === "cleanup-new-input" ? 400 :
      ["cleanup-census-blocks", "cleanup-authority-loss", "cleanup-proof-lost"].includes(scenario) ? 150 : 50,
    inspectTemporaryThreadSources(target) {
      if (scenario === "cleanup-census-unavailable") return undefined;
      return Updates.collectTelegramJournalThreadUpdateIds(resolve()!.journal.read().entries, target as { chatId: number; threadId: number });
    },
    runWorkspaceOperation: queueOperations.run,
    inspectRestoreSourceAbandonment(updateId) {
      const observed = resolve()!.journal.inspectAbandonedPending(updateId);
      if (scenario === "cancel-proof-missing") return undefined;
      if (scenario === "cancel-proof-foreign") return observed ? { ...observed, journalBindingKey: "foreign" } : undefined;
      if (scenario === "cancel-proof-authority-loss") temporaryAuthority = false;
      if (scenario === "cleanup-proof-lost" && proofLost) return undefined;
      return observed;
    },
    inspectRoutingInputGroupExpiry(input) { return journal.routingInputs?.inspectGroupExpiry(input.updateIds); },
    inspectRestoreSourceCompletion(expected) {
      assert.equal(expected.journalBindingKey, bindingKey);
      const proof = resolve()!.journal.inspectSourceCompletion({ updateId: expected.updateId,
        sourceSha256: expected.sourceSha256, completionSha256: expected.completionSha256 });
      return proof ? { ...proof, journalBindingKey: bindingKey } : undefined;
    },
    inspectRestoreQueuedReceipt(expected) {
      assert.ok(queueAdmission.read().leases.length > 0);
      assert.equal(expected.journalBindingKey, bindingKey);
      return resolve()!.journal.inspectQueuedReceipt({ queueKind: expected.queueKind, receiptId: expected.receiptId,
        sourceUpdateIds: [...expected.sourceUpdateIds], queueOwner: { ...expected.queueOwner } });
    },
    async callApi(method, body, options) {
      if (["closeForumTopic", "deleteForumTopic"].includes(method) && body.message_thread_id === 55) {
        assert.equal(restoreStore.listTemporaryThreads()[0]?.cleanupIssued, true, "durable issuance precedes every temporary cleanup API call");
        assert.deepEqual(options, { maxAttempts: 1, retrySafety: "non-idempotent" }, "temporary cleanup disables retries and fallback");
      }
      apiCalls.push({ method, body: body as Record<string, unknown> });
      if (method === "deleteForumTopic" && (scenario === "cancel-delete-fails" || scenario === "cleanup-delete-fails")) throw new Error("fixture deletion failed");
      if (method !== "createForumTopic") return true as never;
      creations.push(body);
      if (scenario === "create-error") throw new Error("fixture creation reply lost");
      return (scenario === "no-thread-id" ? {} : { message_thread_id: 55 }) as never;
    },
    async editInteractiveMessage(_chat, _id, text) { edits.push(text); },
    async sendInteractiveMessage(_chat, text, _mode, markup, sendOptions) {
      if (failPublication) { failPublication = false; throw new Error("fixture chooser publication failed"); }
      choosers.push({ target: sendOptions?.target, text, markup: JSON.stringify(markup) });
      chooserOwnership.recordLocal({ chatId: _chat, messageId: 500 + choosers.length, target: sendOptions?.target });
      return 500 + choosers.length;
    }, ...overrides });
  let harness = createHarness(), workerContext: TestContext | undefined;
  const completedHints: number[] = [];
  const createWorker = (reviewHistorical = false, sourceJournal: Updates.TelegramUpdateWorkerJournalPort = journal,
    onHeldSourcesPrepared?: Updates.TelegramUpdateWorkerRuntimeDeps<TestContext>["onHeldSourcesPrepared"]) => Updates.createTelegramUpdateAdmissionWorkerRuntime<TestUpdate & Journal.TelegramJournaledUpdate, TestContext>({
    onHeldSourcesPrepared, journal: sourceJournal,
    ...(expiryClock ? { getNowMs: expiryClock, spendHistoricalInput: true,
      expireRoutingInput: (source, ctx, signal) => harness.routeRuntime.expireRoutingInput(source, ctx, signal) } : {}),
    ...(reviewHistorical ? { shouldReviewHistoricalInput: (entry, ctx, signal) => harness.routeRuntime.shouldReviewHistoricalInput(entry, ctx, signal) } : {}), getJournalBindingKey: () => bindingKey, hasAuthority: () => true,
    getQueueOwnerIdentity: queueOwnedRestore ? () => ({ instanceId: "old", processId: process.pid,
      processBirthId: `${process.pid}:temp-queue`, sessionGeneration: 1 }) : undefined,
    beforeQueueReceiptPublished: queueOwnedRestore ? harness.routeRuntime.beforeQueueReceiptPublished : undefined,
    onQueueReceiptCommitted: queueOwnedRestore ? (receipt, ctx) => harness.routeRuntime.onQueueReceiptCommitted(receipt, ctx) : undefined,
    onQueueReceiptCompleted: harness.routeRuntime.onQueueReceiptCompleted,
    onUpdateCompleted: (id, ctx, key) => { completedHints.push(id); harness.routeRuntime.onUpdateCompleted(id, ctx, key); },
    recordRuntimeEvent: (category, error, details) => harness.events.push(`worker:${category}:${String(error)}:${JSON.stringify(details)}`),
    async defaultHandle(update, ctx, execution) { workerContext = ctx; await harness.routeRuntime.handleUpdate(update, ctx, execution); } });
  let worker = createWorker();
  let callbackId = 200;
  const click = async (data: string, messageId = 501) => {
    journal.appendBatch([{ update_id: callbackId++, callback_query: { id: `tab-${callbackId}`, from: { id: 7, is_bot: false }, data,
      message: { message_id: messageId, message_thread_id: 55, chat: { id: 7, type: "private" } } } }]);
    worker.signal(); await worker.waitForDrain(); await followerWorker?.waitForDrain(); await harness.routeRuntime.waitForRestoreSettlement();
  };
  const assertFollowerExecution = () => {
    if (!drainRecipient) return;
    assert.deepEqual(followerExecutions, [123], "the real recipient worker executes only the selected message once; stale callbacks are not command replay");
    assert.equal(followerHarness!.events.filter(event => event === "status-menu").length, 1, "the follower router actually runs the selected command");
    assert.deepEqual(recipientJournal.read().entries, [], "the recipient worker disposes its completed source, not just admission");
    assert.equal(followerHarness!.telegramQueueStore.getQueuedItems().length, 0, "no sibling is implicitly queued in the follower Pi");
  };
  const deletions = () => apiCalls.filter(call => call.method === "deleteForumTopic");
  const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
  const routeOf = (index: number) => `reroute:${choosers[index]!.markup.match(/reroutemenu:([a-z0-9]+)/)![1]}:10`;
  const cancelOf = (index: number) => choosers[index]!.markup.match(/reroutecancel:[a-z0-9]+/)![0];
  // An owner text typed inside the routing tab, admitted through the real worker.
  const typeInTab = async (updateId: number, text: string, messageId = updateId - 120) => {
    journal.appendBatch([{ update_id: updateId, message: { message_id: messageId, message_thread_id: 55, date: Math.floor(Date.now() / 1000),
      chat: { id: 7, type: "private" }, from: { id: 7, is_bot: false }, text } }]);
    worker.signal(); await worker.waitForDrain();
  };
  const handOffQueuedInput = async (updateId = selectedRestoreUpdateId) => {
    assert.ok(workerContext);
    const items = harness.telegramQueueStore.getQueuedItems();
    const item = items.find(value => value.admissionReceipts?.some(receipt => receipt.sourceUpdateIds.includes(updateId)));
    assert.ok(item, "the exact selected input owns one live prompt receipt");
    const receipt = item.admissionReceipts!.find(value => value.sourceUpdateIds.includes(updateId))!;
    assert.equal(worker.isQueueReceiptCommitted(receipt), true, "the selected input has exact admitted receipt authority");
    assert.equal(worker.completeQueueReceipts({ receipts: [receipt], ctx: workerContext, reason: "prompt-handoff" }), true,
      "source disposition uses the real worker and exact receipt owner, not fabricated completion");
    // The fixture supplies the subsequent Pi handoff; it does not assert model/task completion.
    harness.telegramQueueStore.setQueuedItems(items.filter(value => value !== item));
    await worker.waitForDrain(); await harness.routeRuntime.waitForRestoreSettlement();
  };
  const assertLostDeliveryProtection = async () => {
    assert.deepEqual(admittedIds, [123], "the real receiver accepted exactly the selected message before the IPC reply was lost");
    if (drainRecipient) assertFollowerExecution();
    else assert.deepEqual(recipientJournal.read().entries.map(entry => entry.updateId), [123], "accepted recipient custody survives the lost reply");
    assert.equal(acceptancePublications, 0, "missing ACK is not leader acceptance proof");
    assert.deepEqual([123, 150].map(id => journal.read().entries.find(entry => entry.updateId === id)?.state), ["pending", "pending"]);
    assert.equal(journal.inspectAbandonedPending(150), undefined, "unknown Restore never cancels a sibling");
    assert.equal(harness.events.includes("status-menu"), false, "the leader never executes the follower's command");
    assert.equal(harness.telegramQueueStore.getQueuedItems().length, 0);
    assert.equal(store.listTemporaryThreads().length, 1);
    assert.deepEqual(deletions(), []);
    await click(cancelOf(1), 502);
    await sleep(150); await harness.routeRuntime.waitForRestoreSettlement();
    assert.ok(journal.inspectAbandonedPending(150), "an independent sibling stays cancellable at its chooser's leader publisher even after the target becomes follower-owned");
    assert.equal(journal.read().entries.find(entry => entry.updateId === 123)?.state, "pending", "the issued but unacknowledged source stays protected");
    assert.equal(journal.inspectAbandonedPending(123), undefined);
    assert.equal(store.listTemporaryThreads().length, 1);
    assert.deepEqual(deletions(), [], "a cancelled sibling cannot dispose of an unresolved selected group or its tab");
    assert.deepEqual(admittedIds, [123], "no automatic replay or sibling delivery");
    if (drainRecipient) assertFollowerExecution();
    else assert.deepEqual(recipientJournal.read().entries.map(entry => entry.updateId), [123]);
    releaseDeliveryReply?.(); await sleep(20);
    assert.equal(journal.read().entries.find(entry => entry.updateId === 123)?.state, "pending", "a reply released after client timeout cannot settle the leader source");
    assert.deepEqual(deletions(), []);
  };
  const fullSlots = scenario === "restore-full-slots" || scenario === "restore-sibling-cold-successor" || restoreReplyBoundary !== undefined;
  if (fullSlots) for (let index = 1; index < 26; index++) threads.upsertWorkspaceBinding({
    ...Threads.createTelegramWorkspaceBindingIdentity(`/repo/${index}`, index, `session-${index}`)!, target: { chatId: 7, threadId: 100 + index },
    slot: String.fromCharCode(65 + index), threadName: `Thread ${index}`, journalBindingKeys: [`manual:${index}`], journalBindingsComplete: true, updatedAtMs: 1 });

  // Accessors keep restart and fault witnesses on the live closure, never a copied worker or harness.
  return {
    admittedIds, apiCalls, assertFollowerExecution, assertLostDeliveryProtection, bindingKey, cancelOf, choosers,
    click, completedHints, createHarness, createWorker, creations, deletions, drainRecipient, edits,
    followerRegistration, followerRegistry, forwardedIds, fullSlots, handOffQueuedInput, journal, liveTargets, loseDeliveryReply, open,
    options, path, queueAdmission, queuedCommand, recipientJournal, resolve, restoreModes, restoreReplyBoundary,
    routeOf, sleep, store, threads, typeInTab,
    renewFollowerRegistration() {
      const live = followerRegistry.get("old")!;
      followerContextGeneration++;
      // Supplied same-session startup publication, not proof of actual Pi host ordering.
      followerRegistry.register({ ...live, target: { chatId: 7, threadId: 55 }, registrationGeneration: "successor" });
      followerRegistration.setRegistered(true, { chatId: 7, threadId: 55 }, { slot: "A", generation: "successor", leaderProtocol: followerProtocol });
    },
    async releaseRestoreReply(mode: "apply" | "inspect") {
      const gate = restoreReplyGates.get(mode);
      assert.ok(gate, "the exact Restore RPC boundary was held");
      gate.release(); await gate.finished; restoreReplyGates.delete(mode);
    },
    get acceptancePublications() { return acceptancePublications; },
    get callbackId() { return callbackId; },
    set callbackId(value: typeof callbackId) { callbackId = value; },
    get cancellationFault() { return cancellationFault; },
    set cancellationFault(value: typeof cancellationFault) { cancellationFault = value; },
    get harness() { return harness; },
    set harness(value: typeof harness) { harness = value; },
    get leaderIdentity() { return leaderIdentity; },
    get membershipFault() { return membershipFault; },
    set membershipFault(value: typeof membershipFault) { membershipFault = value; },
    get proofLost() { return proofLost; },
    set proofLost(value: typeof proofLost) { proofLost = value; },
    get queuePublicationFault() { return queuePublicationFault; },
    set queuePublicationFault(value: typeof queuePublicationFault) { queuePublicationFault = value; },
    get temporaryAuthority() { return temporaryAuthority; },
    set temporaryAuthority(value: typeof temporaryAuthority) { temporaryAuthority = value; },
    get worker() { return worker; },
    set worker(value: typeof worker) { worker = value; },
    async start() {
      followerWorker?.start(ipcCtx); await followerWorker?.waitForDrain();
      if (!scenario.endsWith("-down")) await ipcReceiver?.start();
      worker.start({ cwd: "/repo" }); await worker.waitForDrain();
      if (arrivesLive) { appendOriginal(); worker.signal(); await worker.waitForDrain(); }
    },
    async stop() {
      releaseDeliveryReply?.(); for (const gate of restoreReplyGates.values()) gate.release();
      await worker.stop(); await ipcReceiver?.stop(); await followerWorker?.stop();
    },
  };
}
type AllCommandFixture = ReturnType<typeof createAllCommandFixture>;

for (const mode of ["unselected", "restart", "cold-leader", "multiple-choosers", "partial-cohort", "metadata-before", "metadata-after", "metadata-permanent", "expiry-delete-unknown", "lost-forward", "lost-restore", "restore-queued", "independent-work"] as const) {
  test(`Chooser expiry ends donor custody without archiving or cancelling recipient work (${mode})`,
    { skip: !constants.O_NOFOLLOW || !constants.O_NONBLOCK }, async testContext => {
    await fixture(async frame => {
      let now = Date.now();
      const scenario = mode === "lost-forward" ? "forward-sibling-follower-ipc-lost-reply" :
        mode === "lost-restore" ? "restore-sibling-follower-ipc-lost-reply" : mode === "restore-queued" ? "restore-queue-continue" : mode === "expiry-delete-unknown" ? "cleanup-delete-fails" : "cancel";
      const f = createAllCommandFixture(scenario, frame, `${frame.path}.expiry`, () => Math.max(now, Date.now()));
      let metadataBlocked = mode === "metadata-permanent", metadataAttempts = 0, generation = 1;
      let expiryStore: Threads.TelegramWorkspaceRestore | undefined;
      const mockedCleanup = mode === "metadata-before" || mode === "metadata-permanent" || mode === "expiry-delete-unknown";
      if (mode === "metadata-before" || mode === "metadata-after" || mode === "metadata-permanent") {
        let fail = true;
        const store = { ...f.store, recordTemporaryThreadInputExpiry(...args: Parameters<Threads.TelegramWorkspaceRestore["recordTemporaryThreadInputExpiry"]>) {
          metadataAttempts++;
          if (metadataBlocked) throw new Error("fixture persistent expiry metadata failure");
          if (fail && mode === "metadata-before") { fail = false; throw new Error("fixture expiry metadata refused"); }
          const result = f.store.recordTemporaryThreadInputExpiry(...args);
          if (fail) { fail = false; throw new Error("fixture expiry metadata ACK lost"); }
          return result;
        } };
        expiryStore = store;
        f.harness = f.createHarness({ getWorkspaceRestoreStore: () => store, getSessionGeneration: () => generation }); f.worker = f.createWorker();
      }
      try {
        await f.start();
        if (mode === "multiple-choosers") await f.typeInTab(150, "unselected sibling");
        if (mode === "partial-cohort") {
          // Supplied grouped chooser publication and one earlier donor ACK; all mutations use native journal/Workspace ports.
          f.journal.appendBatch([150, 151].map(id => ({ update_id: id, message: { message_id: id, message_thread_id: 55,
            chat: { id: 7, type: "private" }, from: { id: 7, is_bot: false }, text: `grouped member ${id}` } })));
          const armed = f.journal.routingInputs!.arm({ entries: f.journal.read().entries.filter(entry => entry.updateId >= 150),
            journalBindingKey: f.bindingKey, operatorUserId: 7, publishedAtMs: Math.max(now, Date.now()), isCurrent: () => true });
          f.journal.routingInputs!.select({ entries: armed, journalBindingKey: f.bindingKey, operatorUserId: 7, isCurrent: () => true });
          const entry = f.store.listTemporaryThreads()[0]!, group = { journalBindingKey: f.bindingKey, updateIds: [150, 151] };
          const authority = { executor: entry.executor, operatorUserId: 7, isCurrent: () => true };
          const member = f.store.recordTemporaryThreadInput(entry, group, authority)!;
          assert.ok(f.store.recordTemporaryThreadForwardIssued(member, group, authority));
          f.recipientJournal.appendBatch([{ update_id: 150, message: { message_id: 150, message_thread_id: 10,
            chat: { id: 7, type: "private" }, from: { id: 7, is_bot: false }, text: "accepted grouped member" } }]);
          f.journal.removeCompleted([150]);
          f.worker.signal(); await f.worker.waitForDrain();
        }
        const original = f.journal.read().entries.find(entry => entry.updateId === 123)!;
        assert.equal(original.routingInput?.phase, "waiting", JSON.stringify(f.harness.events));
        const deadline = Math.max(...f.journal.read().entries.filter(entry => entry.routingInput).map(entry => entry.routingInput!.expiresAtMs));
        const route = f.routeOf(0), cancel = f.cancelOf(0);
        if (mode === "lost-forward") await f.click(route);
        if (mode === "lost-restore" || mode === "restore-queued") {
          const restore = f.choosers[0]!.markup.match(/reroutemenu:([a-z0-9]+)/)![1];
          await f.click(`reroutenew:${restore}:10`);
        }
        const recipientBefore = structuredClone(f.recipientJournal.read().entries);
        if (mode === "lost-forward" || mode === "lost-restore") assert.equal(recipientBefore.length, 1, JSON.stringify({ events: f.harness.events, entries: f.journal.read().entries }));
        const queueBefore = f.harness.telegramQueueStore.getQueuedItems();
        if (mode === "restore-queued") {
          assert.equal(f.journal.read().entries.find(entry => entry.updateId === 123)?.state, "queued", JSON.stringify({ events: f.harness.events, entries: f.journal.read().entries }));
          assert.equal(queueBefore.length, 1);
        }
        if (mode === "independent-work") {
          f.journal.appendBatch([{ update_id: 150, message: { message_id: 30, message_thread_id: 55,
            chat: { id: 7, type: "private" }, from: { id: 7, is_bot: false }, text: "independent accepted work" } }]);
          f.journal.markQueued({ queueKind: "prompt", receiptId: "independent", sourceUpdateIds: [150],
            owner: { instanceId: "independent", processId: process.pid, processBirthId: "fixture-independent", sessionGeneration: 1 } });
        }
        if (mode === "restart" || mode === "cold-leader") {
          await f.worker.stop();
          const forgotten = Promise.withResolvers<void>();
          f.harness = f.createHarness(mode === "cold-leader" ? { instanceId: "successor", hasWorkspaceRestoreAuthority: () => true } : {});
          f.worker = f.createWorker(true, f.journal, mode === "cold-leader" ? async input => {
            await f.harness.routeRuntime.forgetPreviousWorld(input, () => () => true); forgotten.resolve();
          } : undefined);
          f.worker.start({ cwd: "/repo" }); await f.worker.waitForDrain();
          if (mode === "cold-leader") {
            await forgotten.promise;
            assert.equal(f.store.listTemporaryThreads().length, 1, "Cold forgetting preserves the acknowledged disposable tab until expiry");
          }
          assert.equal(f.journal.read().entries.find(entry => entry.updateId === 123)?.routingInput?.expiresAtMs, deadline);
        }
        now = deadline;
        if (mockedCleanup) testContext.mock.timers.enable({ apis: ["setTimeout"] });
        f.worker.signal(); await f.worker.waitForDrain();
        if (mockedCleanup) testContext.mock.timers.tick(1000);
        await f.harness.routeRuntime.waitForRestoreSettlement();
        if (mode === "metadata-before" || mode === "metadata-permanent") {
          assert.equal(f.worker.getState().deferredClaimCount, 0, "Confirmed expiry retains no worker claim despite metadata failure");
          assert.equal(f.worker.getState().abandoningClaimCount ?? 0, 0);
          assert.equal(f.worker.getState().journalEntryCount, 0);
          assert.deepEqual(f.deletions(), [], "Unpublished cleanup metadata licenses no deletion");
          const attempts = metadataAttempts;
          f.worker.signal(); await f.worker.waitForDrain();
          assert.equal(metadataAttempts, attempts, "Ordinary worker drains do not retry terminal prompt bodies");
          testContext.mock.timers.tick(60_000); await f.harness.routeRuntime.waitForRestoreSettlement();
          assert.equal(metadataAttempts, attempts + 1, "Only a bounded body-free tab timer retries metadata");
          if (mode === "metadata-permanent") {
            assert.equal(f.worker.getState().deferredClaimCount, 0);
            assert.deepEqual(f.deletions(), []);
            metadataBlocked = false; generation++;
            f.harness = f.createHarness({ getWorkspaceRestoreStore: () => expiryStore,
              getSessionGeneration: () => generation, hasWorkspaceRestoreAuthority: () => true });
            await f.harness.routeRuntime.forgetPreviousWorld({ ctx: { cwd: "/repo" }, journalBindingKey: f.bindingKey,
              signal: new AbortController().signal, isCurrent: () => true, routingSourceIds: [] });
            testContext.mock.timers.tick(1000); await f.harness.routeRuntime.waitForRestoreSettlement();
            assert.equal(f.deletions().length, 1, "Fresh same-instance authority recovers body-free metadata after session replacement");
            const recovered = metadataAttempts;
            testContext.mock.timers.tick(60_000); await f.harness.routeRuntime.waitForRestoreSettlement();
            assert.equal(metadataAttempts, recovered, "The stale-generation retry is inert");
          }
        }
        if (mode === "expiry-delete-unknown") {
          assert.equal(f.store.listTemporaryThreads()[0]?.cleanupIssued, true);
          const attempts = f.deletions().length;
          testContext.mock.timers.tick(120_000); await f.harness.routeRuntime.waitForRestoreSettlement();
          assert.equal(f.deletions().length, attempts, "An unknown issued delete is never retried by metadata recovery");
          assert.equal(f.worker.getState().deferredClaimCount, 0);
        }
        if (mode === "restore-queued") {
          assert.equal(f.journal.read().entries.find(entry => entry.updateId === 123)?.state, "queued");
          assert.deepEqual(f.harness.telegramQueueStore.getQueuedItems(), queueBefore);
          assert.deepEqual(f.deletions(), []);
        } else {
          assert.equal(f.journal.read().entries.some(entry => entry.updateId === 123), false,
            JSON.stringify({ mode, events: f.harness.events, state: f.worker.getState() }));
          assert.ok(f.journal.routingInputs!.inspectExpiry(123));
          assert.equal(f.journal.inspectAbandonedPending(123), undefined, "Expiry writes no private retention copy");
          assert.equal(f.completedHints.includes(123), false, "Expiry is not successful execution");
          if (mode === "multiple-choosers") {
            assert.ok(f.journal.routingInputs!.inspectExpiry(150));
            assert.equal(f.completedHints.includes(150), false);
          }
          if (mode === "partial-cohort") {
            assert.ok(f.journal.routingInputs!.inspectExpiry(151));
            assert.equal(f.journal.read().entries.length, 0, "Partial prior ACK cannot leave the group's final donor pending forever");
          }
          assert.equal(f.deletions().length, mode === "lost-restore" || mode === "independent-work" ? 0 : 1);
          if (mode === "lost-restore") {
            assert.equal(f.threads.listWorkspaceBindings()[0]?.target.threadId, 55, "The restored binding is never rolled back");
            assert.equal(f.store.list().length, 0, JSON.stringify({ events: f.harness.events, temporary: f.store.listTemporaryThreads(), operations: f.store.list() }));
          }
          if (mode === "independent-work") assert.equal(f.journal.read().entries.find(entry => entry.updateId === 150)?.state, "queued");
          await f.click(route); await f.click(cancel);
          assert.equal(f.journal.read().entries.some(entry => entry.updateId === 123), false, "Old buttons never revive the expired input");
          assert.deepEqual(f.recipientJournal.read().entries, recipientBefore, "Recipient acceptance survives donor expiry");
        }
      } finally { await f.stop(); }
    }, mode === "lost-forward" || mode === "lost-restore" ? "follower" : "leader");
  });
}

for (const mode of ["new-world", "last-cancel"] as const) for (const fault of ["empty", "missing-predecessor", "historical-command", "shared-unknown", "shared-other-target", "queued-group", "shared-offered-group", "shared-control", "corrupt", "unclassified", "reference-refusal"] as const) {
  if (mode === "last-cancel" && fault === "missing-predecessor") continue;
  test(`Native temporary cleanup census protects ${mode} (${fault})`, { skip: !constants.O_NOFOLLOW || !constants.O_NONBLOCK }, async () => {
    await fixture(async f => {
      const agentDir = join(await realpath(dirname(f.path)), "agent"), directory = Paths.resolveTelegramTempDir(agentDir);
      const pollingPath = Paths.resolveTelegramSessionPollingJournalPath("leader", agentDir);
      const historical = Paths.resolveTelegramSessionJournalPath("predecessor", "manual:recipient", agentDir);
      const shared = Paths.resolveTelegramSessionJournalPath("other", "manual:shared", agentDir);
      const botIdentity = Journal.createTelegramUpdateJournalBotIdentity({ botToken: "fixture:all" });
      const key = (path: string) => Journal.createTelegramUpdateJournalBindingKey({ path, botIdentity });
      const empty = JSON.stringify({ version: 1, revision: 1, profile: "default", botIdentity, entries: [] });
      for (const path of [pollingPath, historical, shared]) {
        await mkdir(dirname(path), { recursive: true }); await writeFile(path, empty);
      }
      if (fault === "missing-predecessor") await rm(historical);
      if (["historical-command", "shared-unknown", "shared-other-target"].includes(fault)) {
        const update = fault === "shared-unknown" ? { update_id: 300 } : { update_id: 300, message: {
          message_id: 30, message_thread_id: fault === "shared-other-target" ? 999 : 55,
          chat: { id: 7, type: "private" }, from: { id: 7, is_bot: false }, text: "/status" } };
        await writeFile(fault === "historical-command" ? historical : shared, JSON.stringify({ version: 1, revision: 1,
          profile: "default", botIdentity, entries: [{ updateId: 300, update, admittedAtMs: 1, state: "pending" }] }));
      }
      const config = createTelegramConfigStore({ agentDir });
      const ownerBearing = ["queued-group", "shared-offered-group", "shared-control"].includes(fault);
      let ownerJournal: Journal.TelegramUpdateJournalStore | undefined;
      let ownerOriginals: Journal.TelegramUpdateJournalEntry[] | undefined;
      if (ownerBearing) {
        ownerJournal = Journal.createTelegramUpdateJournalStore({ path: fault === "queued-group" ? historical : shared, botIdentity });
        const sourceUpdateIds = fault === "shared-control" ? [300] : [300, 301];
        ownerJournal.appendBatch(sourceUpdateIds.map(update_id => ({ update_id, message: {
          message_id: update_id, message_thread_id: 999, chat: { id: 7, type: "private" }, from: { id: 7, is_bot: false },
          text: fault === "shared-control" ? "/continue" : `Independent grouped work ${update_id}` } })));
        ownerJournal.markQueued({ queueKind: fault === "shared-control" ? "control" : "prompt", receiptId: "independent",
          sourceUpdateIds, owner: { instanceId: "independent", processId: process.pid, processBirthId: `${process.pid}:independent`, sessionGeneration: 1 } });
        const entry = ownerJournal.read().entries[0]!;
        const receipt = { queueKind: entry.queueKind!, receiptId: entry.queueReceiptId!, sourceUpdateIds, queueOwner: entry.queueOwner! };
        const ownerReader = Journal.createTelegramUpdateJournalRuntimeBindingResolver({ getProfileName: () => "default",
          getBotToken: () => "fixture:all", getBotId: () => undefined, getJournalPath: () => fault === "queued-group" ? historical : shared,
          withSourceSerialization: config.withSourceSerialization })()!;
        assert.equal(ownerReader.journal.inspectQueuedReceipt!(receipt)?.sources.length, sourceUpdateIds.length);
        if (fault === "shared-offered-group") {
          ownerJournal.offerQueuedHandoff({ ...receipt, expectedOwner: receipt.queueOwner,
            recipientOwner: { instanceId: "recipient", processId: process.pid + 1, processBirthId: "recipient-birth", sessionGeneration: 2 },
            handoffToken: Journal.createTelegramUpdateQueueHandoffToken() });
          assert.equal(ownerReader.journal.inspectQueuedReceipt!(receipt), undefined, "Offered custody is not ordinary readiness or emptiness");
        }
        ownerOriginals = ownerJournal.read().entries;
        assert.ok(ownerOriginals.every(entry => entry.state === "queued" && (entry.update.message as TestMessage).message_thread_id === 999),
          "Original source targets differ from the cleanup target; receipt ownership, not message address, protects work");
      }
      if (fault === "corrupt") await writeFile(historical, "malformed predecessor");
      const unknown = join(dirname(historical), "journal.unknown.json");
      if (fault === "unclassified") await writeFile(unknown, "unclassified source");
      const originals = await Promise.all([historical, shared].map(path => readFile(path, "utf8").catch(() => undefined)));
      const references = Journal.createTelegramUpdateJournalReferenceRegistry();
      let inspections = 0;
      const inspect: NonNullable<RouteHarnessOptions["inspectTemporaryThreadSources"]> = (_target, required) => {
        inspections++;
        assert.ok(required.includes(key(pollingPath)), "Routing supplies the current source reference");
        if (mode === "new-world") assert.ok(required.includes(key(historical)), "Forgotten membership keeps its captured predecessor reference");
        return config.withSourceSerialization(() => Journal.isTelegramThreadCleanupJournalNamespaceClear({
          directory, profile: "default", botIdentity, pollingPath, requiredJournalBindingKeys: required,
          limits: { maxDirectoryEntries: 10_000, maxFiles: 4096, maxBytes: 64 * 1024 * 1024, maxEntries: 10_000, maxWork: 100_000 },
          withSourceReference(path, operation) {
            if (fault === "reference-refusal") throw new Error("fixture observation reference unavailable");
            return references.withReference({ referenceClass: "operator-disposition", recoveryKey: key(path) }, operation);
          },
        }) ? [] : undefined);
      };
      const bindings = structuredClone(f.threads.listWorkspaceBindings());
      if (mode === "new-world") {
        const reserved = f.store.reserveTemporaryThread({ journalBindingKey: key(historical), updateId: 100 }, "c".repeat(32), f.auth)!.entry;
        f.store.acknowledgeTemporaryThread(reserved, { chatId: 7, threadId: 55 }, f.auth);
        const ctx: TestContext = { cwd: "/leader" }, calls: string[] = [];
        const harness = createRouteHarness({ threadStore: f.threads, instanceId: "successor", getCurrentLeaderEpoch: () => "new",
          getSessionGeneration: () => 1, getAdmissionJournalBinding: () => key(pollingPath), getWorkspaceRestoreStore: () => f.store,
          hasWorkspaceRestoreAuthority: () => true, isContextActive: value => value === ctx, inspectTemporaryThreadSources: inspect,
          async runWorkspaceOperation(_input, operation) { return operation(); },
          async callApi<TResponse>(method: string) { calls.push(method); return true as TResponse; } });
        const input = { ctx, journalBindingKey: key(pollingPath), signal: new AbortController().signal, isCurrent: () => true };
        assert.deepEqual(await harness.routeRuntime.forgetPreviousWorld(input, () => () => true), { forgotten: 1, deleted: fault === "empty" ? 1 : 0 });
        assert.deepEqual(f.store.listTemporaryThreads(), [], "Blocked cleanup does not reconstruct forgotten intent");
        assert.deepEqual(await harness.routeRuntime.forgetPreviousWorld(input, () => () => true), { forgotten: 0, deleted: 0 });
        assert.deepEqual(calls, fault === "empty" ? ["deleteForumTopic"] : [], "No cleanup exemption for shared or unknown sources");
      } else {
        const tab = createAllCommandFixture("cancel", f, pollingPath);
        tab.harness = tab.createHarness({ inspectTemporaryThreadSources: inspect }); tab.worker = tab.createWorker();
        try {
          await tab.start();
          const original = structuredClone(tab.journal.read().entries.find(entry => entry.updateId === 123)!);
          await tab.click(tab.cancelOf(0));
          assert.equal(tab.deletions().length, fault === "empty" ? 1 : 0);
          assert.equal(tab.store.listTemporaryThreads().length, fault === "empty" ? 0 : 1);
          assert.equal(tab.journal.inspectAbandonedPending(123)?.updateId, 123);
          const retention = Journal.inspectTelegramUpdateJournalRetention({ directory, path: pollingPath, profile: "default", botIdentity,
            limits: { maxFiles: 100, maxBytes: 1_000_000, maxEntries: 100, maxWork: 10_000 } });
          assert.equal(retention.retainedInputs.length, 1);
          const retained = JSON.parse(await readFile(retention.retainedInputs[0]!.path, "utf8"));
          assert.deepEqual(retained.entry, original, "The cancelled original stays byte-for-byte private");
          await tab.sleep(100); await tab.harness.routeRuntime.waitForRestoreSettlement();
          assert.equal(tab.deletions().length, fault === "empty" ? 1 : 0, "Refused or issued cleanup never repeats on a timer");
        } finally { await tab.stop(); }
      }
      assert.ok(inspections > 0); assert.deepEqual(references.list(), []);
      assert.deepEqual(await Promise.all([historical, shared].map(path => readFile(path, "utf8").catch(() => undefined))), originals,
        "Strict protection leaves shared, missing and corrupt evidence untouched");
      if (ownerJournal) assert.deepEqual(ownerJournal.read().entries, ownerOriginals, "Whole receipt, owner/acquisition and handoff stay exact without disposition or replay");
      if (fault === "unclassified") assert.equal(await readFile(unknown, "utf8"), "unclassified source");
      await f.threads.load(); assert.deepEqual(f.threads.listWorkspaceBindings(), bindings, "Cleanup changes no Workspace binding");
    });
  });
}

test("Previous-process routing controls all answer one expired choice without effects", async () => {
  const calls: string[] = [];
  const harness = createRouteHarness({ async callApi<TResponse>(method: string) { calls.push(method); return true as TResponse; } });
  for (const [index, data] of ["reroute:1:42", "reroutenew:1:42", "rerouterestore:1", "reroutecancel:1"].entries()) {
    await harness.routeRuntime.handleUpdate({ callback_query: { id: `old-${index}`, from: { id: 7, is_bot: false },
      message: { message_id: 99, message_thread_id: 55, chat: { id: 7, type: "private" } }, data } }, { cwd: "/repo" });
    assert.equal(harness.events.filter(event => event.startsWith("answer:")).at(-1), `answer:${Routing.TELEGRAM_ROUTING_CHOICE_EXPIRED}`, data);
  }
  assert.equal(Routing.TELEGRAM_ROUTING_CHOICE_EXPIRED, "\u231b Routing choice expired.");
  assert.deepEqual(calls, []); assert.deepEqual(harness.telegramQueueStore.getQueuedItems(), []);
});

for (const fault of ["none", "transport-lost", "delete-false", "delete-throws", "retained-source", "census-unavailable", "census-throws", "census-authority-loss"] as const) {
  test(`New-world restart forgets previous Restore/temporary state and deletes only disposable tabs once (${fault})`, async () => {
    await fixture(async ({ store, open, threads, path, request, auth }) => {
      const successor = { executor: { instanceId: "successor", leaderEpoch: "new" }, operatorUserId: 7, isCurrent: () => true };
      const temporary = (updateId: number, threadId: number | undefined, owner = auth) => {
        const reserved = store.reserveTemporaryThread({ journalBindingKey: "cold", updateId }, String(updateId).repeat(32).slice(0, 32), owner)!.entry;
        return threadId === undefined ? reserved : store.acknowledgeTemporaryThread(reserved, { chatId: 7, threadId }, owner)!;
      };
      temporary(300, 55);
      temporary(301, request.target.threadId);
      await store.commit({ ...request, source: { journalBindingKey: "cold", updateIds: [301] } }, auth);
      temporary(302, undefined);
      const current = temporary(303, 57, successor);
      const coldThreads = Threads.createTelegramTopicTargetStore({ path }); await coldThreads.load();
      const cold = open({ threadStore: coldThreads }), ctx: TestContext = { cwd: "/leader" };
      const calls: unknown[][] = [];
      let active = true;
      const harness = createRouteHarness({ threadStore: coldThreads, instanceId: "successor", getCurrentLeaderEpoch: () => "new",
        getSessionGeneration: () => 1, getAdmissionJournalBinding: () => "cold", getWorkspaceRestoreStore: () => cold,
        hasWorkspaceRestoreAuthority: () => true, isContextActive: value => active && value === ctx,
        inspectTemporaryThreadSources() {
          if (fault === "census-throws") throw new Error("source read refused");
          if (fault === "census-authority-loss") active = false;
          return fault === "retained-source" ? [300] : fault === "census-unavailable" ? undefined : [];
        },
        async runWorkspaceOperation(_input, operation) { return operation(); },
        async callApi<TResponse>(method: string, body: Record<string, unknown>, options?: unknown) {
          calls.push([method, body, options]);
          if (fault === "delete-throws") throw new Error("lost delete reply");
          return (fault !== "delete-false") as TResponse;
        } });
      const bindingsBefore = (await (async () => { await threads.load(); return threads.listWorkspaceBindings(); })());
      const input = { ctx, journalBindingKey: "cold", signal: new AbortController().signal, isCurrent: () => true };
      const result = await harness.routeRuntime.forgetPreviousWorld(input, () => fault === "transport-lost" ? undefined : () => true);
      if (fault === "transport-lost") {
        assert.deepEqual(result, { forgotten: 0, deleted: 0 }); assert.deepEqual(calls, []);
        assert.equal(cold.list().length, 1); assert.equal(cold.listTemporaryThreads().length, 4);
        return;
      }
      assert.deepEqual(result, { forgotten: 4, deleted: fault === "none" ? 1 : 0 }, JSON.stringify(harness.events));
      const protectedSource = fault === "retained-source" || fault.startsWith("census-");
      assert.deepEqual(calls, protectedSource ? [] : [["deleteForumTopic", { chat_id: 7, message_thread_id: 55 },
        { maxAttempts: 1, retrySafety: "non-idempotent" }]], "retained or uncertain sources block the one disposable-tab attempt");
      assert.deepEqual(cold.list(), [], "unfinished Restore is forgotten");
      assert.deepEqual(cold.listTemporaryThreads(), [current], "current-instance temporary state is untouched");
      await threads.load();
      assert.deepEqual(threads.listWorkspaceBindings(), bindingsBefore, "committed bindings are never rolled back");
      assert.equal(threads.listWorkspaceBindings().some(binding => binding.target.threadId === request.target.threadId), true);
      assert.deepEqual(await harness.routeRuntime.forgetPreviousWorld(input, () => () => true), { forgotten: 0, deleted: 0 });
      assert.equal(calls.length, protectedSource ? 0 : 1, "blocked, failed or unknown deletion is never retried");
    });
  });
}

for (const membership of ["restore", "temporary"] as const) {
  for (const sourceKind of ["command", "voice", "unknown"] as const) {
    test(`Historical membership forgetting retains bound originals (${membership}/${sourceKind})`, async () => {
      await fixture(async ({ store, open, path, request, auth }) => {
        const options = { path: `${path}.membership-history`,
          botIdentity: Journal.createTelegramUpdateJournalBotIdentity({ botToken: "fixture:membership-history" }) };
        const journal = Journal.createTelegramUpdateJournalStore(options);
        const bindingKey = Journal.createTelegramUpdateJournalBindingKey(options);
        journal.appendBatch([{ update_id: 100, message: { message_id: 12, message_thread_id: 42,
          chat: { id: 7, type: "private" }, from: { id: 7, is_bot: false },
          ...(sourceKind === "command" ? { text: "/next" } : sourceKind === "voice" ?
            { voice: { file_id: "fixture-voice", duration: 1 } } : { future_input: { value: "original" } }) } }]);
        const original = structuredClone(journal.read().entries[0]!);
        if (membership === "temporary") {
          const reserved = store.reserveTemporaryThread({ journalBindingKey: bindingKey, updateId: 101 }, "c".repeat(32), auth)!.entry;
          const temporary = store.acknowledgeTemporaryThread(reserved, request.target, auth)!;
          assert.ok(store.recordTemporaryThreadInput(temporary, { journalBindingKey: bindingKey, updateIds: [100] }, auth));
        }
        // Relocation makes the formerly unbound source target routable. Temporary membership belongs to a sibling of the selected Restore source.
        const relocated = await store.commit({ ...request, source: { journalBindingKey: bindingKey,
          updateIds: membership === "restore" ? [100] : [101] } }, auth);
        assert.equal(relocated?.phase, "relocated");
        const coldThreads = Threads.createTelegramTopicTargetStore({ path }); await coldThreads.load();
        const cold = open({ threadStore: coldThreads }), ctx: TestContext = { cwd: "/repo" };
        const bindings = structuredClone(coldThreads.listWorkspaceBindings());
        const calls: string[] = [], handled: number[] = [], classifications: Array<boolean | "retain"> = [];
        const harness = createRouteHarness({ threadStore: coldThreads, instanceId: "successor", getSessionGeneration: () => 2,
          getCurrentLeaderEpoch: () => "new", getAdmissionJournalBinding: () => bindingKey,
          getWorkspaceRestoreStore: () => cold, hasWorkspaceRestoreAuthority: () => true,
          isContextActive: value => value === ctx,
          inspectTemporaryThreadSources: target => Updates.collectTelegramJournalThreadUpdateIds(journal.read().entries,
            target as { chatId: number; threadId: number }),
          async runWorkspaceOperation(_input, operation) { return operation(); },
          async callApi<TResponse>(method: string) { calls.push(method); return true as TResponse; } });
        const worker = Updates.createTelegramUpdateAdmissionWorkerRuntime<Updates.TelegramUpdateFlow & Journal.TelegramJournaledUpdate, TestContext>({
          journal, getJournalBindingKey: () => bindingKey, hasAuthority: () => true, spendHistoricalInput: true,
          async shouldReviewHistoricalInput(entry, context, signal) {
            const verdict = await harness.routeRuntime.shouldReviewHistoricalInput(entry, context, signal);
            classifications.push(verdict); return verdict;
          },
          registry: { version: 1, add: () => () => {}, async dispatch(update) {
            handled.push((update as Journal.TelegramJournaledUpdate).update_id); return "consume" as const;
          } },
          defaultHandle() { assert.fail("This witness records companion dispatch, not actual Pi/model replay"); },
        });
        try {
          for (let generation = 0; generation < 2; generation++) {
            worker.start(ctx); await worker.waitForDrain();
            assert.deepEqual(journal.read().entries, [original]);
            assert.equal(worker.getState().historicalClaimCount, 1);
            assert.deepEqual(handled, []);
            if (generation === 0) await worker.stop();
          }
          assert.deepEqual(classifications, ["retain", "retain"], "Both owner-membership paths protect a now-bound target");
          const forgotten = await harness.routeRuntime.forgetPreviousWorld({ ctx, journalBindingKey: bindingKey,
            signal: new AbortController().signal, isCurrent: () => true }, () => () => true);
          assert.deepEqual(forgotten, { forgotten: membership === "restore" ? 1 : 2, deleted: 0 });
          assert.deepEqual(cold.list(), []); assert.deepEqual(cold.listTemporaryThreads(), []);
          assert.deepEqual(journal.read().entries, [original], "Forgetting itself does not dispose of an original");
          await coldThreads.load(); assert.deepEqual(coldThreads.listWorkspaceBindings(), bindings);
          assert.deepEqual(calls, [], "Committed relocation is not rolled back or deleted");
          worker.signal(); await worker.waitForDrain();
          assert.deepEqual(journal.read().entries, [original], "The current worker claim survives new-world forgetting");
          assert.deepEqual(handled, []);
          await worker.stop(); worker.start(ctx); await worker.waitForDrain();
          assert.deepEqual(classifications, ["retain", "retain", "retain"]);
          assert.deepEqual(handled, [], "Forgetting plus a bound target never dispatches the unsupported original");
          assert.deepEqual(journal.read().entries, [original], "Retention no longer depends on forgotten membership");
          assert.equal(worker.getState().historicalClaimCount, 1);
          assert.equal(journal.inspectAbandonedPending(100), undefined);
        } finally { await worker.stop(); }
      }, membership === "restore" ? "leader" : "follower");
    });
  }
}

test("Cold temporary command and sibling stay held when the new-world spend is not enabled", async () => {
  await fixture(async frame => {
    const { path, open } = frame;
    const f = createAllCommandFixture("created", frame);
    try {
      await f.start(); await f.typeInTab(150, "still-unassigned sibling");
      assert.equal(f.choosers.length, 2);
      const sourceBodies = structuredClone(f.journal.read().entries.map(value => value.update));
      const temporary = structuredClone(f.store.listTemporaryThreads()[0]);
      await f.worker.stop();
      const coldThreads = Threads.createTelegramTopicTargetStore({ path }); await coldThreads.load();
      const coldStore = open({ threadStore: coldThreads }), context: TestContext = { cwd: "/repo" };
      f.harness = f.createHarness({ threadStore: coldThreads, instanceId: "successor", getSessionGeneration: () => 2,
        getCurrentLeaderEpoch: () => "successor-epoch", isContextActive: ctx => ctx === context,
        getWorkspaceRestoreStore: () => coldStore });
      f.worker = f.createWorker(true, f.resolve()!.journal);
      const bytes = readFileSync(path, "utf8");
      f.worker.start(context); await f.worker.waitForDrain(); await f.harness.routeRuntime.waitForRestoreSettlement();
      assert.deepEqual(f.journal.read().entries.map(value => value.update), sourceBodies);
      assert.deepEqual(f.journal.read().entries.map(value => value.state), ["pending", "pending"]);
      assert.deepEqual(coldStore.listTemporaryThreads(), [temporary]);
      assert.equal(readFileSync(path, "utf8"), bytes, "cold classification never adopts or consumes sources");
      assert.equal(f.creations.length, 1);
      assert.equal(f.choosers.length, 2, "a cold source never republishes ordinary controls as a side effect of dispatch");
      assert.equal(f.harness.events.includes("status-menu"), false);
      assert.equal(f.harness.telegramQueueStore.getQueuedItems().length, 0);
      assert.deepEqual(f.deletions(), [], "classification is not the future cold-disposal grant");
      assert.deepEqual(f.journal.read().sourceCompletions ?? [], []);
      assert.equal(f.journal.inspectAbandonedPending(123), undefined);
      assert.equal(f.journal.inspectAbandonedPending(150), undefined);
    } finally { await f.stop(); }
  });
});

async function assertCreationScenario(scenario: AllCommandScenario, f: AllCommandFixture): Promise<void> {
  if (scenario === "prompt-in-tab") {
    await f.typeInTab(150, "typed in the routing tab", 30);
    assert.equal(f.choosers.length, 2, "a prompt typed in the tab gets its own chooser");
    await f.click(f.routeOf(1), 502);
    assert.equal(f.harness.telegramQueueStore.getQueuedItems().length, 1, "the prompt is delivered");
    assert.deepEqual(f.deletions(), [],
      "another source's Forward cannot remove a retained temporary tab");
    assert.equal(f.harness.events.some(event => event.includes("thread cleanup is still pending")), false,
      "a Forward completes its own input; the tab's removal is a separate, delayed lifecycle");
    assert.equal(f.store.listTemporaryThreads().length, 1);
    await f.click(f.routeOf(0));
    assert.equal(f.deletions().length, 0,
      "the command's own Forward must not bypass another input's unresolved cleanup and receipt protection");
    assert.equal(f.store.listTemporaryThreads().length, 1);
    return;
  }
  if (scenario === "expired-new") {
    assert.deepEqual(f.creations, [], "an expired, never-presented replay keeps the legacy terminal settlement");
    assert.deepEqual(f.store.listTemporaryThreads(), []);
    assert.equal(f.journal.read().entries.some(value => value.updateId === 123), false);
    return;
  }
  if (scenario === "expired-presented") {
    assert.deepEqual(f.creations, [], "a presented tab is reused, never recreated");
    assert.equal(f.choosers.length, 1, "age does not expire an owner-visible tab");
    assert.deepEqual(f.choosers[0]!.target, { chatId: 7, threadId: 55 });
    assert.equal(f.journal.read().entries.some(value => value.updateId === 123), true, "the original stays in custody");
    return;
  }
  if (scenario === "authority-off" || scenario === "foreign-sender") {
    assert.deepEqual(f.creations, [], "without exact leader and owner authority no tab is created");
    assert.deepEqual(f.store.listTemporaryThreads(), []);
    return;
  }
  assert.equal(f.creations.length, 1);
  assert.match(JSON.stringify(f.creations[0]), /"chat_id":7,"name":"\/start"/);
  const [entry] = f.store.listTemporaryThreads();
  assert.deepEqual(entry?.source, { journalBindingKey: f.bindingKey, updateId: 123 });
  assert.equal(f.journal.read().entries.find(value => value.updateId === 123)?.state === "pending" ||
    f.journal.read().entries.find(value => value.updateId === 123)?.state === "retry-wait", true, "the original stays in custody");
  assert.equal(f.harness.telegramQueueStore.getQueuedItems().length, 0, "nothing is sent to Pi before explicit routing");
  assert.equal(f.harness.events.some(event => event.startsWith("interactive:") ), false, "no legacy chooser is published in All");
  if (scenario === "create-error" || scenario === "no-thread-id") {
    assert.equal(entry?.phase, "creating", "an unconfirmed creation stays an unknown outcome");
    assert.deepEqual(f.choosers, []);
    assert.ok(f.harness.events.some(event => event.includes("routing tab for this command could not be confirmed")));
  } else {
    assert.equal(entry?.phase, "created");
    assert.deepEqual(entry?.target, { chatId: 7, threadId: 55 });
    if (scenario === "publication-failure") {
      assert.deepEqual(f.choosers, [], "a failed publication leaves the source retryable");
      f.worker.signal(); await f.sleep(1100); await f.worker.waitForDrain();
    }
    assert.equal(f.choosers.length, 1);
    assert.deepEqual(f.choosers[0]!.target, { chatId: 7, threadId: 55 });
    assert.match(f.choosers[0]!.text, /from the <b>All<\/b> tab\./);
    assert.doesNotMatch(f.choosers[0]!.text, /This tab was opened|nothing has been sent|Cancel routing keeps/);
    assert.match(f.choosers[0]!.markup, /reroutemenu:[a-z0-9]+/);
    assert.match(f.choosers[0]!.markup, /🔁 Restore…/);
    assert.doesNotMatch(f.choosers[0]!.markup, /"callback_data":"reroute:/);
  }
  // Restart: the same pending source reuses its entry and never creates another tab.
  const published = f.choosers.length;
  await f.worker.stop();
  f.harness = f.createHarness();
  f.worker = f.createWorker();
  f.worker.start({ cwd: "/repo" }); await f.worker.waitForDrain();
  assert.equal(f.creations.length, 1, "restart never repeats createForumTopic");
  assert.equal(f.store.listTemporaryThreads().length, 1);
  if (entry?.phase === "created") {
    assert.equal(f.choosers.length, published + 1, "restart republishes the chooser in the same tab");
    assert.deepEqual(f.choosers.at(-1)!.target, { chatId: 7, threadId: 55 });
  } else assert.equal(f.choosers.length, 0);
  assert.equal(f.harness.telegramQueueStore.getQueuedItems().length, 0);
}

for (const kind of ["command", "prompt"] as const) {
test(`A known temporary chooser never falls back to unguarded dispatch when its store becomes unavailable (${kind})`, async () => {
  await fixture(async frame => {
    const f = createAllCommandFixture("cancel", frame);
    let available = true, attempts = 0;
    f.harness = f.createHarness({ getWorkspaceRestoreStore: () => available ? f.store : undefined,
      async sendStatusMessage() { attempts++; }, async processInbound(files, rawText) {
        attempts++; return { rawText, promptFiles: files, handlerOutputs: [], handledFiles: [] };
      } });
    try {
      await f.start();
      if (kind === "prompt") await f.typeInTab(150, "fresh known member");
      const route = f.routeOf(kind === "prompt" ? 1 : 0), before = readFileSync(frame.path, "utf8");
      available = false;
      await f.click(route, kind === "prompt" ? 502 : 501); await f.click(route, kind === "prompt" ? 502 : 501);
      assert.equal(attempts, 0);
      assert.equal(f.journal.read().entries.find(entry => entry.updateId === 123)?.state, "pending");
      assert.equal(readFileSync(frame.path, "utf8"), before, "unavailable evidence never grants issuance or legacy fallback");
      assert.equal(f.journal.inspectAbandonedPending(123), undefined);
      assert.deepEqual(f.deletions(), []);
    } finally { await f.stop(); }
  });
});

}

for (const fault of ["command-before-source-write", "command-after-source-write", "prompt-preparation",
  "issue-before", "issue-after", "issue-authority-loss"] as const) {
  test(`Local temporary Forward keeps its one-shot group across local interruption (${fault})`, async () => {
    await fixture(async frame => {
      const f = createAllCommandFixture("cancel", frame), selected = fault === "prompt-preparation" ? 150 : 123;
      const group = { journalBindingKey: f.bindingKey, updateIds: [selected] };
      let attempts = 0, issueFault = fault.startsWith("issue-");
      const seen: Threads.TelegramTemporaryThreadEntry[] = [];
      const sourceJournal = { ...f.journal, removeCompleted(ids: readonly number[]) {
        if (!fault.startsWith("command-") || !ids.includes(123)) return f.journal.removeCompleted(ids);
        if (fault === "command-after-source-write") f.journal.removeCompleted(ids);
        throw new Error("source completion acknowledgement unavailable");
      } };
      const store = { ...f.store, recordTemporaryThreadForwardIssued(expected: Threads.TelegramTemporaryThreadEntry,
        input: Threads.TelegramTemporaryThreadInput, authority: Threads.TelegramWorkspaceRestoreAuthority) {
        if (!issueFault) return f.store.recordTemporaryThreadForwardIssued(expected, input, authority);
        issueFault = false;
        return f.open({ onPublicationBoundary(boundary) {
          if (fault === "issue-before" && boundary === "after-write-before-rename") throw new Error("local issuance fault");
          if (boundary === "after-rename") {
            if (fault === "issue-after") throw new Error("local issuance reply lost");
            if (fault === "issue-authority-loss") f.temporaryAuthority = false;
          }
        } }).recordTemporaryThreadForwardIssued(expected, input, authority);
      } };
      f.harness = f.createHarness({ getWorkspaceRestoreStore: () => store,
        async sendStatusMessage() { attempts++; seen.push(f.store.listTemporaryThreads()[0]!); },
        async processInbound() {
          attempts++; seen.push(f.store.listTemporaryThreads()[0]!);
          throw new Error("local prompt preparation failed");
        } });
      f.worker = f.createWorker(false, sourceJournal);
      try {
        await f.start(); await f.typeInTab(150, "independent prompt sibling");
        const route = f.routeOf(selected === 123 ? 0 : 1);
        await f.click(route, selected === 123 ? 501 : 502);
        assert.equal(attempts, fault.startsWith("issue-") ? 0 : 1);
        const issued = f.store.listTemporaryThreads()[0]!;
        assert.equal(issued.forwardProtocol, "one-shot-v1", "creation records coverage of local and follower Forward paths");
        assert.deepEqual(issued.forwardedInputs, fault === "issue-before" ? undefined : [group]);
        if (attempts) assert.deepEqual(seen[0]?.forwardedInputs, [group], "issuance precedes command handling and prompt preparation");
        const original = f.journal.read().entries.find(entry => entry.updateId === selected);
        assert.equal(original?.state, fault === "command-after-source-write" ? undefined : "pending",
          "missing originals after commit never substitute for a source ACK");
        assert.equal(issued.completedInputs, undefined, "an unavailable source ACK cannot fabricate completion");
        assert.equal(f.journal.inspectAbandonedPending(selected), undefined);
        assert.deepEqual(f.deletions(), []);
        f.temporaryAuthority = true;
        await f.click(route, selected === 123 ? 501 : 502);
        assert.equal(attempts, fault === "issue-before" ? 1 : fault.startsWith("issue-") ? 0 : 1,
          "only an unpublished grant may receive its first attempt; an issued group never re-enters local code");
        const retained = f.store.listTemporaryThreads()[0]!;
        assert.deepEqual(retained.forwardedInputs, [group]);
        if (fault === "issue-before") {
          assert.deepEqual(retained.completedInputs, [group], "a positively completed retry closes only its own group");
          assert.equal(f.journal.read().entries.find(entry => entry.updateId === 150)?.state, "pending");
          return;
        }
        const other = selected === 123 ? 150 : 123;
        await f.click(f.cancelOf(other === 123 ? 0 : 1), other === 123 ? 501 : 502);
        assert.ok(f.journal.inspectAbandonedPending(other), "the unissued sibling remains independently cancellable");
        assert.equal(f.journal.inspectAbandonedPending(selected), undefined, "unknown issued input is never cancelled");
        await f.worker.stop();
        const coldThreads = Threads.createTelegramTopicTargetStore({ path: frame.path }); await coldThreads.load();
        const coldStore = f.open({ threadStore: coldThreads }), context: TestContext = { cwd: "/repo" };
        f.harness = f.createHarness({ threadStore: coldThreads, getWorkspaceRestoreStore: () => coldStore,
          instanceId: "successor", getSessionGeneration: () => 2, getCurrentLeaderEpoch: () => "successor-epoch",
          isContextActive: ctx => ctx === context, async sendStatusMessage() { assert.fail("cold source cannot replay a local command"); } });
        f.worker = f.createWorker(true, f.resolve()!.journal);
        const bytes = readFileSync(frame.path, "utf8");
        f.worker.start(context); await f.worker.waitForDrain();
        const cold = coldStore.listTemporaryThreads()[0]!;
        assert.deepEqual(cold.forwardedInputs, [group]);
        assert.equal(cold.completedInputs, undefined, "cold absence and held status remain nonterminal");
        assert.equal(readFileSync(frame.path, "utf8"), bytes, "startup and veto never reset/adopt the issued grant");
        assert.equal(f.journal.inspectAbandonedPending(selected), undefined);
        assert.deepEqual(f.deletions(), []);
        assert.equal(f.creations.length, 1, "no replacement tab is created");
      } finally { await f.stop(); }
    });
  });
}

async function assertForwardScenario(scenario: AllCommandScenario, f: AllCommandFixture): Promise<void> {
  if (scenario === "forward-prompt-then-cancel") {
    await f.typeInTab(150, "independent Forward prompt");
    await f.click(f.routeOf(1), 502);
    assert.equal(f.journal.read().entries.find(entry => entry.updateId === 150)?.state, "queued");
    assert.equal(f.store.listTemporaryThreads()[0]?.completedInputs, undefined, "admission is not Forward completion");
    await f.click(f.cancelOf(0), 501);
    await f.sleep(150); await f.harness.routeRuntime.waitForRestoreSettlement();
    assert.ok(f.journal.inspectAbandonedPending(123));
    assert.equal(f.store.listTemporaryThreads().length, 1);
    assert.deepEqual(f.deletions(), [], "an independently queued prompt protects the tab despite sibling cancellation");
    await f.handOffQueuedInput(150);
    assert.ok(f.completedHints.includes(150));
    await f.sleep(150); await f.harness.routeRuntime.waitForRestoreSettlement();
    assert.equal(f.deletions().length, 1, "positive ordinary receipt disposition permits delayed all-resolved cleanup");
    assert.deepEqual(f.store.listTemporaryThreads(), []);
    assert.equal(f.journal.inspectAbandonedPending(150), undefined);
    assert.deepEqual(f.journal.read().entries, []);
    assert.equal(f.harness.events.includes("status-menu"), false, "the cancelled All command never executes");
    return;
  }
  if (scenario.startsWith("forward-sibling-follower")) {
    await f.typeInTab(150, "unforwarded sibling for a follower tab", 30);
    assert.equal(f.choosers.length, 2);
    await f.click(f.routeOf(0), 501);
    if (scenario.includes("-issue-")) {
      const after = scenario.endsWith("-issue-after");
      assert.deepEqual(f.admittedIds, [], "a failed issuance publication sends nothing");
      assert.equal(f.journal.read().entries.find(entry => entry.updateId === 123)?.state, "pending");
      assert.equal(f.store.listTemporaryThreads()[0]?.forwardedInputs?.length ?? 0, after ? 1 : 0, "only a published fact is durable");
      await f.click(f.routeOf(0), 501);
      assert.deepEqual(f.admittedIds, after ? [] : [123], after ? "an ambiguous published issuance is never retried" : "an unpublished issuance may be retried safely");
      assert.equal(f.journal.read().entries.some(entry => entry.updateId === 123), after, "only a positive accepted Forward disposes the source");
      return;
    }
    if (f.loseDeliveryReply) {
      if (scenario.endsWith("-restore-other")) {
        const siblingId = f.choosers[1]!.markup.match(/reroutemenu:([a-z0-9]+)/)![1];
        await f.click(`rerouterestore:${siblingId}`, 502);
        await f.click(`reroutenew:${siblingId}:10`, 502);
        assert.deepEqual(f.store.list(), [], "Restore cannot select another group while its sibling Forward is issued and unknown");
        assert.deepEqual(f.threads.listWorkspaceBindings()[0]?.target, { chatId: 7, threadId: 10 });
        assert.deepEqual(f.admittedIds, [123]);
        f.assertFollowerExecution();
      }
      await f.assertLostDeliveryProtection();
      if (scenario.endsWith("-retry")) {
        await f.click(f.routeOf(0), 501);
        f.assertFollowerExecution();
        assert.deepEqual(f.admittedIds, [123], "an uncertain Forward never issues a second delivery in the same runtime");
        assert.equal(f.journal.read().entries.find(entry => entry.updateId === 123)?.state, "pending");
        assert.ok(f.journal.inspectAbandonedPending(150));
        assert.ok(f.harness.events.includes("answer:🚫 Forward is pending; this button will not resend uncertain input."));
        await f.click(f.cancelOf(0), 501);
        await f.click(`rerouterestore:${f.choosers[0]!.markup.match(/reroutemenu:([a-z0-9]+)/)![1]}`, 501);
        assert.equal(f.journal.inspectAbandonedPending(123), undefined, "uncertain issued work cannot be cancelled or rerouted as an unassigned source");
        assert.deepEqual(f.store.list(), [], "another Restore cannot borrow the uncertain Forward source");
        assert.deepEqual(f.admittedIds, [123]);
        assert.equal(f.deletions().length, 0);
        assert.equal(f.store.listTemporaryThreads().length, 1);
        if (f.drainRecipient) f.assertFollowerExecution();
      }
      if (scenario.endsWith("-restart")) {
        assert.deepEqual(f.store.listTemporaryThreads()[0]?.forwardedInputs?.flatMap(input => input.updateIds), [123], "issuance is durable before the lost reply");
        await f.worker.stop(); f.harness = f.createHarness(); f.worker = f.createWorker();
        f.worker.start({ cwd: "/repo" }); await f.worker.waitForDrain(); await f.harness.routeRuntime.waitForRestoreSettlement();
        const republished = f.choosers.length;
        assert.ok(republished >= 3, "restart republishes the held source's chooser");
        await f.click(f.routeOf(republished - 1), 503);
        assert.deepEqual(f.admittedIds, [123], "a restarted leader never repeats an issued Forward");
        assert.ok(f.harness.events.includes("answer:🚫 Forward is pending; this button will not resend uncertain input."), f.harness.events.join("\n"));
        await f.click(`rerouterestore:${f.choosers[republished - 1]!.markup.match(/reroutemenu:([a-z0-9]+)/)![1]}`, 503);
        await f.click(`reroutenew:${f.choosers[republished - 1]!.markup.match(/reroutemenu:([a-z0-9]+)/)![1]}:10`, 503);
        assert.deepEqual(f.store.list(), [], "Restore cannot borrow the issued Forward source after restart");
        assert.equal(f.journal.read().entries.find(entry => entry.updateId === 123)?.state, "pending");
        assert.equal(f.journal.inspectAbandonedPending(123), undefined);
        assert.deepEqual(f.admittedIds, [123]);
        assert.deepEqual(f.deletions(), []);
        assert.equal(f.store.listTemporaryThreads().length, 1);
        if (f.drainRecipient) f.assertFollowerExecution();
      }
      return;
    }
    assert.deepEqual(f.forwardedIds(), [123], "only the selected input is forwarded to the follower");
    f.assertFollowerExecution();
    if (f.drainRecipient) {
      await f.click(f.routeOf(0), 501);
      f.assertFollowerExecution();
      assert.deepEqual(f.forwardedIds(), [123], "a stale Forward click does not admit another delivery");
    }
    assert.equal(f.harness.events.includes("status-menu"), false, "the leader does not run a follower's command");
    assert.equal(f.journal.read().entries.some(entry => entry.updateId === 123), false);
    assert.equal(f.journal.read().entries.find(entry => entry.updateId === 150)?.state, "pending");
    assert.equal(f.store.listTemporaryThreads()[0]?.completedInputs?.length, 1);
    await f.sleep(150); await f.harness.routeRuntime.waitForRestoreSettlement();
    assert.deepEqual(f.deletions(), [], "an unforwarded sibling keeps the tab");
    await f.click(f.cancelOf(1), 502);
    await f.sleep(150); await f.harness.routeRuntime.waitForRestoreSettlement();
    assert.deepEqual(f.forwardedIds(), [123], "cancellation never forwards");
    f.assertFollowerExecution();
    assert.equal(f.deletions().length, 1, "the last resolution removes the tab once");
    assert.equal(f.deletions()[0]!.body.message_thread_id, 55);
    assert.deepEqual(f.store.listTemporaryThreads(), []);
    assert.ok(f.journal.inspectAbandonedPending(150));
    return;
  }
  if (scenario === "forward-then-cancel" || scenario === "cancel-then-forward") {
    await f.typeInTab(150, "unforwarded sibling", 30);
    assert.equal(f.choosers.length, 2);
    const forward = f.routeOf(0);
    const cancel = f.cancelOf(1);
    if (scenario === "forward-then-cancel") {
      await f.click(forward, 501);
      assert.equal(f.harness.events.filter(event => event === "status-menu").length, 1, "the command runs once");
      assert.equal(f.journal.read().entries.some(entry => entry.updateId === 123), false, "only the forwarded input is gone");
      assert.equal(f.journal.read().entries.find(entry => entry.updateId === 150)?.state, "pending", "the unforwarded input remains");
      assert.equal(f.store.listTemporaryThreads()[0]?.completedInputs?.length, 1);
      await f.sleep(150); await f.harness.routeRuntime.waitForRestoreSettlement();
      assert.deepEqual(f.deletions(), [], "an unresolved sibling keeps the tab");
      await f.click(cancel, 502);
    } else {
      await f.click(cancel, 502);
      assert.deepEqual(f.deletions(), [], "the Forward-able input still holds the tab");
      await f.click(forward, 501);
      assert.equal(f.harness.events.filter(event => event === "status-menu").length, 1);
    }
    await f.sleep(150); await f.harness.routeRuntime.waitForRestoreSettlement();
    assert.equal(f.deletions().length, 1, "the last resolution removes the tab once");
    assert.equal(f.deletions()[0]!.body.message_thread_id, 55);
    assert.deepEqual(f.store.listTemporaryThreads(), []);
    assert.ok(f.journal.inspectAbandonedPending(150), "the cancelled input stays privately retained");
    assert.equal(f.harness.telegramQueueStore.getQueuedItems().length, 0);
    return;
  }
  if (scenario === "forward-with-prompt" || scenario === "forward-other-tab") return assertSiblingProtection(scenario, f);
  const route = f.routeOf(0);
  if (scenario === "forward-authority-lost") f.temporaryAuthority = false;
  await f.click(route);
  if (scenario === "forward-authority-lost") {
    assert.equal(f.apiCalls.some(call => call.method === "deleteForumTopic"), false, "a lost leader epoch never deletes the tab");
    assert.equal(f.store.listTemporaryThreads().length, 1, "only a current leader retires the entry");
    return;
  }
  if (scenario === "forward-double") await f.click(route);
  assert.equal(f.harness.events.filter(event => event === "status-menu").length, 1, "the command runs once in the selected Pi");
  assert.equal(f.journal.read().entries.some(value => value.updateId === 123), false, "routing completes the All original");
  assert.ok(f.apiCalls.some(call => call.method === "deleteForumTopic" && call.body.message_thread_id === 55),
    "Forward removes the freshly created tab");
  assert.equal(f.creations.length, 1);
  assert.deepEqual(f.store.listTemporaryThreads().length, 0,
    "only current authority retires the entry after durable completion");
  return;
}

async function assertCancelScenario(scenario: AllCommandScenario, f: AllCommandFixture): Promise<void> {
  if (scenario === "cancel-two-prompts" || scenario === "cancel-two-prompts-reverse") {
    await f.typeInTab(150, "second cancellable input", 30);
    const primary = f.cancelOf(0);
    const sibling = f.cancelOf(1);
    const order = scenario === "cancel-two-prompts" ? [[primary, 501, 123], [sibling, 502, 150]] as const : [[sibling, 502, 150], [primary, 501, 123]] as const;
    for (const [index, [action, chooserId, updateId]] of order.entries()) {
      await f.click(action, chooserId);
      assert.ok(f.journal.inspectAbandonedPending(updateId));
      const removed = f.deletions();
      if (index === 0) {
        const mid = f.store.listTemporaryThreads()[0]!;
        assert.ok(mid.cancelledInputs?.some(input => input.updateIds.includes(updateId)));
        assert.deepEqual(removed, [], "one cancelled input never grants whole-tab deletion while another remains");
        assert.equal(Threads.isTelegramTemporaryThreadFullyResolved(mid), false);
        const cold = Threads.createTelegramTopicTargetStore({ path: f.path }); await cold.load();
        assert.deepEqual(f.open({ threadStore: cold }).listTemporaryThreads()[0]?.cancelledInputs, mid.cancelledInputs);
      } else {
        assert.equal(removed.length, 1, "the last cancelled input removes the tab once after the quiet period");
        assert.equal(removed[0]!.body.message_thread_id, 55);
        assert.deepEqual(f.store.listTemporaryThreads(), [], "confirmed removal retires the whole entry");
      }
    }
    assert.equal(f.harness.telegramQueueStore.getQueuedItems().length, 0);
    assert.equal(f.journal.read().operatorDispositions?.filter(item => item.updateId === 123 || item.updateId === 150).length, 2);
    return;
  }
  if (scenario === "cancel-proof-missing" || scenario === "cancel-proof-foreign" || scenario === "cancel-proof-authority-loss" ||
      scenario === "cancel-marker-before" || scenario === "cancel-marker-after" || scenario === "cancel-marker-no-proof") {
    const cancel = f.cancelOf(0);
    await f.click(cancel);
    assert.ok(f.journal.inspectAbandonedPending(123), "source cancellation remains committed regardless of marker faults");
    assert.equal(f.store.listTemporaryThreads()[0]?.cancelledInputs?.length ?? 0, scenario === "cancel-marker-after" ? 1 : 0);
    assert.equal(f.store.listTemporaryThreads().length, 1);
    assert.deepEqual(f.deletions(), []);
    assert.equal(f.harness.telegramQueueStore.getQueuedItems().length, 0);
    const retained = readFileSync(f.journal.inspectAbandonedPending(123)!.retainedPath, "utf8");
    if (f.cancellationFault) {
      f.cancellationFault = false;
      await f.click(cancel);
      assert.equal(f.deletions().length, 1, "a fresh exact proof retry may reconcile the marker without recancelling the source");
    } else {
      await f.click(cancel);
      assert.deepEqual(f.deletions(), []);
    }
    assert.equal(f.journal.read().operatorDispositions?.filter(item => item.updateId === 123).length, 1);
    assert.equal(readFileSync(f.journal.inspectAbandonedPending(123)!.retainedPath, "utf8"), retained);
    return;
  }
  if (scenario === "cancel-with-prompt") return assertSiblingProtection(scenario, f);
  if (scenario === "cancel-after-restart") {
    await f.worker.stop(); f.harness = f.createHarness(); f.worker = f.createWorker();
    f.worker.start({ cwd: "/repo" }); await f.worker.waitForDrain();
    assert.equal(f.choosers.length, 2);
    assert.doesNotMatch(f.choosers[1]!.markup, /reroutecancel:/,
      "a source from before restart has no fresh-cancellation evidence, so Cancel is not offered");
    assert.deepEqual(f.deletions(), []);
    assert.equal(f.store.listTemporaryThreads().length, 1);
    return;
  }
  assert.doesNotMatch(f.choosers[0]!.text, /Cancel routing keeps|If no other input remains/);
  const cancel = f.cancelOf(0);
  await f.click(cancel);
  if (scenario === "cancel-double") await f.click(cancel);
  const disposition = f.journal.read().operatorDispositions?.filter(item => item.updateId === 123) ?? [];
  assert.equal(disposition.length, 1, "the original is abandoned exactly once");
  assert.ok(f.journal.inspectAbandonedPending(123), "the original is retained privately");
  assert.equal(f.harness.events.includes("status-menu"), false, "a cancelled command never runs");
  assert.equal(f.harness.telegramQueueStore.getQueuedItems().length, 0);
  const deletes = f.deletions();
  assert.equal(deletes.length, 1, "one removal attempt, never repeated automatically");
  assert.equal(deletes[0]!.body.message_thread_id, 55, "only the source's own tab is removed");
  if (scenario === "cancel-delete-fails") {
    assert.equal(f.store.listTemporaryThreads().length, 1, "an unconfirmed removal keeps the tab protected");
    assert.match(f.edits.at(-1)!, /Routing cancelled\./);
  } else {
    assert.deepEqual(f.store.listTemporaryThreads(), [], "confirmed removal retires the entry");
    assert.ok(f.harness.events.includes("answer:⛔️ Routing cancelled."), f.harness.events.join("\n"));
  }
  return;
}

for (const input of [
  { text: "/start", title: "/start" },
  { text: "/status", title: "/status" },
  { text: "/hidden_parity", title: "/hidden_parity" },
  { text: "/parity_template", title: "/parity_template" },
  { text: "a plain All prompt", title: "New chat" },
] as const) for (const nativeTab of [false, true]) {
  test(`Commands and prompts share the owner routing menu (${input.title}; native=${nativeTab})`, async () => {
    const unregister = Commands.registerTelegramCommand({ name: "hidden_parity", showInMenu: false,
      handler() { assert.fail("No registered command executes before routing selection"); } });
    try {
      await fixture(async frame => {
        const f = await createAllCommandFixture("cancel", frame);
        f.harness = f.createHarness({ getCommands: () => [{ name: "parity_template", source: "prompt", sourceInfo: { path: "/unused/parity.md" } }] });
        f.worker = Updates.createTelegramUpdateAdmissionWorkerRuntime<TestUpdate & Journal.TelegramJournaledUpdate, TestContext>({
          journal: f.journal, getJournalBindingKey: () => f.bindingKey, hasAuthority: () => true,
          expireRoutingInput: f.harness.routeRuntime.expireRoutingInput,
          defaultHandle: (update, ctx, execution) => f.harness.routeRuntime.handleUpdate(update, ctx, execution),
        });
        f.worker.start({ cwd: "/repo" }); await f.worker.waitForDrain();
        try {
          f.journal.appendBatch([{ update_id: 123, message: { message_id: 12,
            ...(nativeTab ? { message_thread_id: 55 } : {}), chat: { id: 7, type: "private" },
            from: { id: 7, is_bot: false }, text: input.text } }]);
          f.worker.signal(); await f.worker.waitForDrain();
          assert.equal(f.choosers.length, 1);
          const root = JSON.parse(f.choosers[0]!.markup).inline_keyboard;
          assert.deepEqual(root.map((row: Array<{ text: string }>) => row[0]!.text),
            ["↪️ Reroute…", "🔁 Restore…", "⛔️ Cancel routing"]);
          if (!nativeTab) assert.deepEqual(f.creations, [{ chat_id: 7, name: input.title }]);
          else {
            assert.deepEqual(f.creations, [], "A Telegram-provided tab is reused, never duplicated");
            const renames = f.apiCalls.filter(call => call.method === "editForumTopic");
            assert.deepEqual(renames, input.title === "New chat" ? [] : [{ method: "editForumTopic",
              body: { chat_id: 7, message_thread_id: 55, name: input.title } }]);
          }
          assert.equal(f.harness.events.includes("status-menu"), false);
          assert.equal(f.harness.telegramQueueStore.getQueuedItems().length, 0);
          const before = f.journal.read().entries.find(entry => entry.updateId === 123);
          assert.ok(before, "The exact source stays pending until the operator selects a route");
          assert.equal(before.routingInput?.phase, "waiting", "All supported chooser sources share the fixed routing lifetime");
          await f.click(root[2][0].callback_data);
          const retained = f.journal.inspectAbandonedPending(123);
          assert.ok(retained, "The same Cancel privately retains commands and prompts");
          assert.equal(f.journal.read().entries.some(entry => entry.updateId === 123), false);
          if (nativeTab) assert.deepEqual(f.deletions(), [], "A received target is not fabricated temporary deletion authority");
          else assert.equal(f.deletions().length, 1, "A positively created temporary tab keeps its last-Cancel cleanup");
        } finally { await f.stop(); }
      });
    } finally { unregister(); }
  });
}

for (const text of ["/status", "a plain All prompt"]) {
  test(`Uniform All chooser dispatches only its selected input (${text})`, async () => {
    await fixture(async frame => {
      const f = await createAllCommandFixture("cancel", frame);
      f.worker.start({ cwd: "/repo" }); await f.worker.waitForDrain();
      try {
        f.journal.appendBatch([{ update_id: 123, message: { message_id: 12, chat: { id: 7, type: "private" },
          from: { id: 7, is_bot: false }, text } }]);
        f.worker.signal(); await f.worker.waitForDrain();
        const root = JSON.parse(f.choosers[0]!.markup).inline_keyboard;
        await f.click(root[0][0].callback_data);
        assert.equal(f.harness.events.includes("status-menu"), false);
        assert.equal(f.harness.telegramQueueStore.getQueuedItems().length, 0);
        await f.click(f.routeOf(0));
        if (text === "/status") {
          assert.equal(f.harness.events.filter(event => event === "status-menu").length, 1);
          assert.equal(f.journal.read().entries.some(entry => entry.updateId === 123), false);
        } else {
          assert.equal(f.harness.telegramQueueStore.getQueuedItems().length, 1);
          assert.equal(f.journal.read().entries.find(entry => entry.updateId === 123)?.state, "queued");
        }
        await f.click(f.routeOf(0));
        assert.equal(f.harness.events.filter(event => event === "status-menu").length, text === "/status" ? 1 : 0);
        assert.equal(f.harness.telegramQueueStore.getQueuedItems().length, text === "/status" ? 0 : 1);
      } finally { await f.stop(); }
    });
  });
}

for (const proof of ["implicit", "manual", "string-flag", "foreign-creator", "historical", "context-changed", "generation-changed"] as const) {
  test(`Native New Chat cleanup requires fresh exact implicit creation (${proof})`, async () => {
    await fixture(async frame => {
      const f = await createAllCommandFixture("cancel", frame);
      let generation = 1;
      f.harness = f.createHarness({ getSessionGeneration: () => generation });
      const creation = { update_id: 120, message: { message_id: 9, message_thread_id: 55,
        chat: { id: 7, type: "private" as const }, from: { id: proof === "foreign-creator" ? 8 : 7, is_bot: false },
        forum_topic_created: { name: "New Chat", icon_color: 9367192,
          is_name_implicit: proof === "manual" ? false : proof === "string-flag" ? "true" : true } } };
      try {
        if (proof === "historical") f.journal.appendBatch([creation]);
        f.worker.start({ cwd: "/repo" }); await f.worker.waitForDrain();
        if (proof !== "historical") { f.journal.appendBatch([creation]); f.worker.signal(); await f.worker.waitForDrain(); }
        if (proof === "context-changed") {
          await f.worker.stop(); f.worker = f.createWorker(); f.worker.start({ cwd: "/repo" }); await f.worker.waitForDrain();
        }
        if (proof === "generation-changed") generation++;
        f.journal.appendBatch([{ update_id: 123, message: { message_id: 12, message_thread_id: 55,
          chat: { id: 7, type: "private" }, from: { id: 7, is_bot: false }, text: "gibberish native All prompt" } }]);
        f.worker.signal(); await f.worker.waitForDrain();
        assert.equal(f.choosers.length, 1);
        assert.deepEqual(f.creations, [], "Observed native creation never issues another createForumTopic");
        assert.equal(f.store.listTemporaryThreads().length, proof === "implicit" ? 1 : 0);
        await f.click(f.cancelOf(0));
        assert.ok(f.journal.inspectAbandonedPending(123));
        assert.equal(f.deletions().length, proof === "implicit" ? 1 : 0, "Only a fresh literal implicit creation may enter normal temporary cleanup");
        assert.deepEqual(f.store.listTemporaryThreads(), []);
      } finally { await f.stop(); }
    });
  });
}

for (const text of ["/status", "native prompt Forward"]) {
  test(`Implicit native tab retains normal one-shot Forward settlement (${text})`, async () => {
    await fixture(async frame => {
      const f = await createAllCommandFixture("cancel", frame);
      f.worker.start({ cwd: "/repo" }); await f.worker.waitForDrain();
      try {
        f.journal.appendBatch([{ update_id: 120, message: { message_id: 9, message_thread_id: 55,
          chat: { id: 7, type: "private" }, from: { id: 7, is_bot: false }, forum_topic_created: { name: "New Chat", is_name_implicit: true } } }]);
        f.worker.signal(); await f.worker.waitForDrain();
        await f.typeInTab(123, text, 12);
        await f.click(f.routeOf(0));
        if (text === "/status") {
          assert.equal(f.harness.events.filter(event => event === "status-menu").length, 1);
          assert.equal(f.journal.read().entries.some(entry => entry.updateId === 123), false, "Command completion settles the observed native source");
        } else {
          assert.equal(f.harness.telegramQueueStore.getQueuedItems().length, 1);
          assert.equal(f.journal.read().entries.find(entry => entry.updateId === 123)?.state, "queued");
          assert.deepEqual(f.deletions(), [], "Queue admission is not completion or tab deletion");
        }
        await f.click(f.routeOf(0));
        assert.equal(f.harness.events.filter(event => event === "status-menu").length, text === "/status" ? 1 : 0);
        assert.equal(f.harness.telegramQueueStore.getQueuedItems().length, text === "/status" ? 0 : 1);
      } finally { await f.stop(); }
    });
  });
}

test("Implicit native tab cancels only after its final input and the same quiet period", async () => {
  await fixture(async frame => {
    const f = await createAllCommandFixture("cancel", frame);
    f.harness = f.createHarness({ temporaryThreadCleanupDelayMs: 200 });
    f.worker.start({ cwd: "/repo" }); await f.worker.waitForDrain();
    try {
      f.journal.appendBatch([{ update_id: 120, message: { message_id: 9, message_thread_id: 55,
        chat: { id: 7, type: "private" }, from: { id: 7, is_bot: false }, forum_topic_created: { name: "New Chat", is_name_implicit: true } } }]);
      f.worker.signal(); await f.worker.waitForDrain();
      await f.typeInTab(123, "first native prompt", 12);
      await f.typeInTab(150, "second native prompt", 30);
      assert.equal(f.store.listTemporaryThreads()[0]?.inputs?.length, 2);
      await f.click(f.cancelOf(0), 501);
      assert.deepEqual(f.deletions(), [], "The first cancellation keeps the tab and sibling");
      assert.ok(f.journal.read().entries.some(entry => entry.updateId === 150));
      const finalCancellation = f.click(f.cancelOf(1), 502);
      await f.sleep(50);
      assert.deepEqual(f.deletions(), [], "Last Cancel does not bypass the quiet period");
      await f.typeInTab(160, "new input during the grace period", 40);
      await finalCancellation;
      assert.deepEqual(f.deletions(), [], "A new input cancels the old cleanup timer");
      await f.click(f.cancelOf(2), 503);
      assert.equal(f.deletions().length, 1);
      assert.deepEqual(f.store.listTemporaryThreads(), []);
    } finally { await f.stop(); }
  });
});

test("Temporary routing offers separate Reroute and Restore submenus without executing the input", async () => {
  await fixture(async frame => {
    const f = await createAllCommandFixture("cancel", frame);
    const menus: Array<{ text: string; markup: string }> = [];
    f.harness = f.createHarness({ async editInteractiveMessage(_chat, _id, text, _mode, markup) {
      menus.push({ text, markup: JSON.stringify(markup) });
    } });
    try {
      await f.start();
      const root = JSON.parse(f.choosers[0]!.markup).inline_keyboard;
      assert.deepEqual(root.map((row: Array<{ text: string }>) => row[0]!.text), ["↪️ Reroute…", "🔁 Restore…", "⛔️ Cancel routing"]);
      await f.click(root[0][0].callback_data);
      assert.match(menus[0]!.text, /Reroute:/);
      assert.match(menus[0]!.markup, /reroute:[a-z0-9]+:10/);
      assert.doesNotMatch(menus[0]!.markup, /reroutenew:/);
      const rerouteMenu = JSON.parse(menus[0]!.markup).inline_keyboard;
      assert.equal(rerouteMenu[0][0].text, "⬆️ Back");
      assert.doesNotMatch(menus[0]!.markup, /reroutecancel:/);
      await f.click(rerouteMenu[0][0].callback_data);
      assert.deepEqual(JSON.parse(menus[1]!.markup).inline_keyboard, root, "Back restores the mode chooser including Cancel");
      assert.equal(menus[1]!.text, f.choosers[0]!.text, "Reroute Back restores the exact original description");
      await f.click(root[1][0].callback_data);
      assert.match(menus[2]!.markup, /reroutenew:[a-z0-9]+:10/);
      assert.doesNotMatch(menus[2]!.markup, /"callback_data":"reroute:|reroutecancel:/);
      const restoreMenu = JSON.parse(menus[2]!.markup).inline_keyboard;
      assert.equal(restoreMenu[0][0].text, "⬆️ Back");
      await f.click(restoreMenu[0][0].callback_data);
      assert.deepEqual(JSON.parse(menus[3]!.markup).inline_keyboard, root);
      assert.equal(menus[3]!.text, f.choosers[0]!.text, "Restore Back restores the exact original description");
      await f.typeInTab(150, "another input in this temporary tab");
      const promptRoot = JSON.parse(f.choosers[1]!.markup).inline_keyboard;
      await f.click(promptRoot[0][0].callback_data, 502);
      const promptMenu = JSON.parse(menus.at(-1)!.markup).inline_keyboard;
      await f.click(promptMenu[0][0].callback_data, 502);
      assert.equal(menus.at(-1)!.text, f.choosers[1]!.text, "Prompt Back restores its own description, not the command description");
      assert.equal(f.harness.events.includes("status-menu"), false);
      assert.equal(f.journal.read().entries.find(entry => entry.updateId === 123)?.state, "pending");
    } finally { await f.stop(); }
  });
});

for (const fault of ["lost-delete-reply", "negative-delete-reply", "input-during-delete", "authority-during-delete", "unrelated-provision"] as const) {
  test(`Temporary cleanup spends its durable grant before transport and retains uncertainty (${fault})`, async () => {
    await fixture(async frame => {
      const f = await createAllCommandFixture("cancel", frame);
      const calls: string[] = [];
      f.harness = f.createHarness({ async callApi(method, body, options) {
        calls.push(method);
        assert.notEqual(body.message_thread_id, 77, "a temporary grant never cleans an unrelated planner target");
        if (method === "createForumTopic") return { message_thread_id: 55 } as never;
        if (method === "closeForumTopic" || method === "deleteForumTopic") {
          assert.equal(f.store.listTemporaryThreads()[0]?.cleanupIssued, true);
          assert.deepEqual(options, { maxAttempts: 1, retrySafety: "non-idempotent" });
        }
        if (method === "deleteForumTopic" && fault === "input-during-delete") {
          f.journal.appendBatch([{ update_id: 151, message: { message_id: 31, message_thread_id: 55, date: 1,
            chat: { id: 7, type: "private" }, from: { id: 7, is_bot: false }, text: "new input after issuance" } }]);
        }
        if (method === "deleteForumTopic" && fault === "authority-during-delete") f.temporaryAuthority = false;
        if (method === "deleteForumTopic" && (fault === "lost-delete-reply" || fault === "unrelated-provision")) throw new Error("delete reply lost");
        if (method === "deleteForumTopic" && fault === "negative-delete-reply") return false as never;
        return true as never;
      } });
      try {
        await f.start();
        if (fault === "unrelated-provision") {
          f.threads.upsertPendingProvision({ id: "unrelated", instanceId: "other", owner: "manual-follower",
            target: { chatId: 7, threadId: 77 }, startedAtMs: 1, expiresAtMs: Date.now() - 1 });
          await f.threads.persist();
          assert.ok(ThreadReconciler.planThreadReconciliation({ nowMs: Date.now(), records: f.threads.list(),
            pendingProvisions: f.threads.listPendingProvisions() }).actions.some(action =>
              action.kind === "close-delete-expired-pending-provision-topic" && action.target.threadId === 77),
            "the general planner really contains an unrelated destructive action");
        }
        await f.click(f.cancelOf(0));
        if (fault === "unrelated-provision") assert.ok(f.threads.listPendingProvisions().some(value => value.id === "unrelated"));
        assert.equal(calls.filter(method => method === "createForumTopic").length, 1);
        assert.equal(calls.filter(method => method === "closeForumTopic").length, 0, "private tabs never call the unsupported close method");
        assert.equal(calls.filter(method => method === "deleteForumTopic").length, 1);
        const retained = f.store.listTemporaryThreads()[0]!;
        assert.equal(retained.cleanupIssued, true, "skip/negative/lost result never withdraws an issued grant");
        assert.ok(f.journal.inspectAbandonedPending(123), "private source disposition survives cleanup uncertainty");
        assert.equal(f.harness.events.includes("status-menu"), false, "abandonment does not execute the command");
        if (fault === "input-during-delete") assert.equal(f.journal.read().entries.find(entry => entry.updateId === 151)?.state, "pending");
        const cold = Threads.createTelegramTopicTargetStore({ path: frame.path }); await cold.load();
        const successor = cold.workspaceRestore({ profileName: "default", tokenSha256: "a".repeat(64), getNowMs: () => 1000 });
        assert.equal(successor.listTemporaryThreads()[0]?.cleanupIssued, true);
        assert.equal(successor.issueTemporaryThreadCleanup(successor.listTemporaryThreads()[0]!, frame.auth), undefined);
        f.temporaryAuthority = true;
        await f.sleep(150); await f.harness.routeRuntime.waitForRestoreSettlement();
        assert.equal(calls.filter(method => method === "closeForumTopic").length, 0, "restored authority/time cannot repeat physical cleanup");
        assert.equal(calls.filter(method => method === "deleteForumTopic").length, 1);
      } finally { await f.stop(); }
    });
  });
}

async function assertCleanupAndMembershipScenario(scenario: AllCommandScenario, f: AllCommandFixture): Promise<void> {
  if (scenario.startsWith("cleanup-")) {
    const quickClick = async (data: string, messageId: number) => {
      f.journal.appendBatch([{ update_id: f.callbackId++, callback_query: { id: `tab-${f.callbackId}`, from: { id: 7, is_bot: false }, data,
        message: { message_id: messageId, message_thread_id: 55, chat: { id: 7, type: "private" } } } }]);
      f.worker.signal(); await f.worker.waitForDrain();
    };
    const appendSibling = () => f.journal.appendBatch([{ update_id: 150, message: { message_id: 30, message_thread_id: 55,
      date: Math.floor(Date.now() / 1000), chat: { id: 7, type: "private" }, from: { id: 7, is_bot: false }, text: "late sibling input" } }]);
    const primary = f.cancelOf(0);
    if (scenario === "cleanup-new-input" || scenario === "cleanup-delete-fails") {
      appendSibling(); f.worker.signal(); await f.worker.waitForDrain();
      assert.equal(f.choosers.length, 2);
      const sibling = f.cancelOf(1);
      if (scenario === "cleanup-delete-fails") {
        await f.click(primary, 501); await f.click(sibling, 502);
        assert.equal(f.deletions().length, 1, "the final cancellation issues one deletion attempt");
        assert.equal(f.store.listTemporaryThreads().length, 1, "an unconfirmed deletion keeps the whole tab protected");
        const retained = f.store.listTemporaryThreads()[0]!;
        assert.deepEqual(retained.cancelledInputs, Threads.getTelegramTemporaryThreadInputs(retained));
        await f.sleep(150); await f.harness.routeRuntime.waitForRestoreSettlement();
        assert.equal(f.deletions().length, 1, "an unknown or failed deletion is never retried automatically");
        return;
      }
      // Both are cancelled but the first quiet period is pending; a third input must cancel it.
      await quickClick(sibling, 502); await quickClick(primary, 501);
      assert.deepEqual(f.deletions(), [], "the quiet period has not elapsed");
      await f.typeInTab(151, "input during grace", 31);
      assert.equal(f.choosers.length, 3, "new input receives its own routing chooser");
      await f.sleep(550); await f.harness.routeRuntime.waitForRestoreSettlement();
      assert.deepEqual(f.deletions(), [], "a fresh input cancels the pending cleanup and protects the tab");
      assert.equal(f.store.listTemporaryThreads().length, 1);
      assert.equal(f.journal.read().entries.find(entry => entry.updateId === 151)?.state, "pending");
      await f.click(f.cancelOf(2), 503);
      assert.equal(f.deletions().length, 1, "the new last cancellation starts a new quiet period");
      assert.deepEqual(f.store.listTemporaryThreads(), []);
      return;
    }
    await quickClick(primary, 501);
    const cancelled = f.store.listTemporaryThreads()[0]!;
    assert.deepEqual(cancelled.cancelledInputs, Threads.getTelegramTemporaryThreadInputs(cancelled));
    if (scenario === "cleanup-census-blocks") appendSibling();
    if (scenario === "cleanup-authority-loss") f.temporaryAuthority = false;
    if (scenario === "cleanup-proof-lost") f.proofLost = true;
    await f.sleep(scenario === "cleanup-census-unavailable" ? 100 : 300); await f.harness.routeRuntime.waitForRestoreSettlement();
    assert.deepEqual(f.deletions(), [], "stale census, authority or proof is never deletion authority");
    assert.equal(f.store.listTemporaryThreads().length, 1);
    assert.ok(f.journal.inspectAbandonedPending(123), "the retained original survives every refused cleanup");
    if (scenario === "cleanup-census-blocks") assert.equal(f.journal.read().entries.find(entry => entry.updateId === 150)?.state, "pending");
    f.temporaryAuthority = true; f.proofLost = false;
    await f.sleep(300); await f.harness.routeRuntime.waitForRestoreSettlement();
    assert.deepEqual(f.deletions(), [], "a spent quiet period is not rescheduled by time or restored authority");
    return;
  }
  if (scenario === "membership-media-group" || scenario === "membership-text-real-gate") {
    // These run through the real, non-reentrant Workspace gate: membership publication must never wait on itself.
    const group = scenario === "membership-media-group";
    const arrivals = group
      ? [150, 151].map(id => ({ update_id: id, message: { message_id: id - 120, message_thread_id: 55, media_group_id: "album",
        date: Math.floor(Date.now() / 1000), chat: { id: 7, type: "private" }, from: { id: 7, is_bot: false },
        photo: [{ file_id: `p${id}`, file_unique_id: `p${id}`, width: 1, height: 1 }] } }))
      : [{ update_id: 150, message: { message_id: 30, message_thread_id: 55, date: Math.floor(Date.now() / 1000),
        chat: { id: 7, type: "private" }, from: { id: 7, is_bot: false }, text: "sibling under the real gate" } }];
    f.journal.appendBatch(arrivals as never);
    f.worker.signal(); await f.worker.waitForDrain();
    if (group) { await f.sleep(1500); await f.harness.routeRuntime.waitForRestoreSettlement(); }
    assert.equal(f.choosers.length, 2, "the sibling input receives its own chooser");
    assert.deepEqual(f.store.listTemporaryThreads()[0]?.inputs, [{ journalBindingKey: f.bindingKey, updateIds: [123] },
      { journalBindingKey: f.bindingKey, updateIds: group ? [150, 151] : [150] }], "one durable group per presented input set");
    assert.deepEqual(f.deletions(), []);
    assert.equal(f.harness.telegramQueueStore.getQueuedItems().length, 0);
    return;
  }
  await f.typeInTab(150, "durable sibling", 30);
  if (f.membershipFault) {
    assert.equal(f.choosers.length, 1, "unacknowledged membership cannot publish a routing chooser");
    assert.equal(f.store.listTemporaryThreads()[0]?.inputs?.length, scenario === "membership-after" ? 2 : 1);
    assert.equal(f.journal.read().entries.find(entry => entry.updateId === 150)?.state, "retry-wait");
    assert.deepEqual(f.deletions(), []);
    f.membershipFault = false;
    f.worker.signal(); await f.sleep(1100); await f.worker.waitForDrain();
  }
  assert.equal(f.choosers.length, 2);
  const membership = [{ journalBindingKey: f.bindingKey, updateIds: [123] }, { journalBindingKey: f.bindingKey, updateIds: [150] }];
  assert.deepEqual(f.store.listTemporaryThreads()[0]?.inputs, membership);
  const cold = Threads.createTelegramTopicTargetStore({ path: f.path }); await cold.load();
  assert.deepEqual(f.open({ threadStore: cold }).listTemporaryThreads()[0]?.inputs, membership);
  assert.equal(f.harness.telegramQueueStore.getQueuedItems().length, 0);
  if (scenario === "membership-cold") {
    await f.click(f.routeOf(0));
    assert.equal(f.journal.read().entries.some(entry => entry.updateId === 123), false);
    await f.worker.stop(); f.harness = f.createHarness();
    const before = readFileSync(f.path, "utf8");
    f.harness.routeRuntime.onUpdateCompleted(123, { cwd: "/repo" }, f.bindingKey);
    await f.harness.routeRuntime.waitForRestoreSettlement();
    assert.equal(readFileSync(f.path, "utf8"), before, "no local chooser memory is not permission to retire durable siblings");
    assert.deepEqual(f.store.listTemporaryThreads()[0]?.inputs, membership);
    assert.deepEqual(f.deletions(), []);
  }
  return;
}

async function assertTemporaryRestoreRpcInterruption(scenario: AllCommandScenario, f: AllCommandFixture, rerouteId: string): Promise<void> {
  f.recipientJournal.appendBatch([{ update_id: 900, message: { message_id: 90, message_thread_id: 55,
    chat: { id: 7, type: "private" }, from: { id: 7, is_bot: false }, text: "Independent accepted recipient work" } }]);
  f.recipientJournal.markQueued({ queueKind: "prompt", receiptId: "independent", sourceUpdateIds: [900],
    owner: { instanceId: "old", processId: process.pid, processBirthId: `${process.pid}:independent`, sessionGeneration: 1 } });
  const independent = structuredClone(f.recipientJournal.read().entries[0]);
  const otherBindings = structuredClone(f.threads.listWorkspaceBindings().filter(value => value.slot !== "A"));
  const assertHeld = () => {
    assert.deepEqual([123, 150].map(id => f.journal.read().entries.find(entry => entry.updateId === id)?.state), ["pending", "pending"]);
    assert.equal(f.journal.inspectAbandonedPending(150), undefined, "uncertain readiness is not sibling cancellation");
    assert.deepEqual(f.admittedIds, [], "no command dispatch before a current positive Restore observation");
    assert.equal(f.acceptancePublications, 0);
    assert.deepEqual(f.deletions(), [], "a lost apply/inspect response grants no cleanup");
    assert.equal(f.store.listTemporaryThreads().length, 1);
    assert.deepEqual(f.recipientJournal.read().entries.find(entry => entry.updateId === 900), independent);
    assert.deepEqual(f.threads.listWorkspaceBindings().filter(value => value.slot !== "A"), otherBindings);
    assert.equal(f.threads.listWorkspaceBindings().length, 26);
    assert.equal(f.threads.listWorkspaceBindings().find(value => value.slot === "A")?.target.threadId, 55);
    assert.equal(f.creations.length, 1, "full slots never allocate another tab or binding");
  };
  await f.click(`reroutenew:${rerouteId}:10`);
  assert.deepEqual(f.restoreModes, [{ mode: "apply", generation: "fresh" }]);
  assert.deepEqual(f.followerRegistration.getTarget(), { chatId: 7, threadId: f.restoreReplyBoundary === "before" ? 10 : 55 },
    "the timed-out original apply either has not run or has changed the real receiver's registration before reply");
  assert.equal(f.store.list()[0]?.phase, "recipient-issued");
  assertHeld();
  if (f.restoreReplyBoundary === "before") f.renewFollowerRegistration();
  await f.releaseRestoreReply("apply");
  assertHeld();
  if (scenario.endsWith("-inspect-after-reply")) {
    const before = readFileSync(f.path, "utf8");
    await f.click(`reroutenew:${rerouteId}:10`);
    assert.deepEqual(f.restoreModes, [{ mode: "apply", generation: "fresh" }, { mode: "inspect", generation: "fresh" }]);
    assert.equal(readFileSync(f.path, "utf8"), before, "timed-out inspect cannot publish readiness or mutate the canonical frame");
    assertHeld();
    f.renewFollowerRegistration();
    await f.releaseRestoreReply("inspect");
    assertHeld();
  } else if (f.restoreReplyBoundary === "after") f.renewFollowerRegistration();
  const selectedEntry = structuredClone(f.journal.read().entries.find(entry => entry.updateId === 123)!);
  await f.click(`reroutenew:${rerouteId}:10`);
  assert.deepEqual(f.restoreModes.at(-1), { mode: "inspect", generation: "successor" });
  assert.equal(f.restoreModes.filter(value => value.mode === "apply").length, 1, "successor inspection never borrows another apply grant");
  assert.deepEqual(f.admittedIds, [123], "only the selected command is durably accepted once");
  assert.equal(f.journal.read().entries.find(entry => entry.updateId === 123), undefined);
  const sourceDigest = Journal.createTelegramUpdateJournalEntryDigest(selectedEntry).sourceSha256;
  const terminal = f.journal.read().sourceCompletions?.find(value => value.updateId === 123);
  assert.ok(terminal, "successful continuation retains scoped disposition proof, never source absence alone");
  assert.equal(terminal.sourceSha256, sourceDigest);
  assert.deepEqual(f.journal.inspectSourceCompletion({ updateId: 123, sourceSha256: sourceDigest,
    completionSha256: terminal.completionSha256 }), terminal);
  assert.ok(f.journal.inspectAbandonedPending(150), "a positive current Restore allows exact private sibling disposition");
  assert.deepEqual(f.recipientJournal.read().entries.find(entry => entry.updateId === 900), independent, "independent accepted work is never rolled back");
  assert.deepEqual(f.recipientJournal.read().entries.map(entry => entry.updateId).sort((a, b) => a - b), [123, 900]);
  assert.deepEqual(f.threads.listWorkspaceBindings().filter(value => value.slot !== "A"), otherBindings);
  assert.equal(f.deletions().some(call => call.body.message_thread_id === 55), false, "the restored bound tab stays intact");
  await f.click(`reroutenew:${rerouteId}:10`);
  assert.deepEqual(f.admittedIds, [123], "stale selection cannot re-admit the completed source");
  assert.equal(f.restoreModes.filter(value => value.mode === "apply").length, 1);
}

async function assertColdTemporaryRestoreHold(f: AllCommandFixture, rerouteId: string): Promise<void> {
  assert.equal(f.harness.events.filter(value => value === "status-menu").length, 1, "the predecessor issued the selected command once");
  assert.equal(f.acceptancePublications, 0, "lost epoch prevents acceptance proof after semantic execution");
  assert.deepEqual([123, 150].map(id => f.journal.read().entries.find(value => value.updateId === id)?.state), ["pending", "pending"]);
  const sourceBodies = structuredClone(f.journal.read().entries.filter(value => [123, 150].includes(value.updateId)).map(value => value.update));
  const intent = structuredClone(f.store.list()[0]);
  assert.ok(intent?.routing, "unknown dispatch remains a durable spent grant");
  assert.deepEqual(intent.routing.acceptances ?? [], []);
  await f.worker.stop(); await f.harness.routeRuntime.waitForRestoreSettlement();
  const coldThreads = Threads.createTelegramTopicTargetStore({ path: f.path }); await coldThreads.load();
  const owner = coldThreads.list().find(value => value.slot === "A")!;
  assert.ok(owner);
  // Supplied current same-session startup owner; this is not actual Pi process/host startup evidence.
  coldThreads.upsert({ ...owner, instanceId: "successor", owner: { kind: "leader", instanceId: "successor", cwd: "/repo" } });
  await coldThreads.persist();
  const coldStore = f.open({ threadStore: coldThreads });
  const freshContext: TestContext = { cwd: "/repo" };
  f.harness = f.createHarness({ threadStore: coldThreads, instanceId: "successor", getSessionGeneration: () => 2,
    getCurrentLeaderEpoch: () => "successor-epoch", isContextActive: ctx => ctx === freshContext,
    getWorkspaceRestoreStore: () => coldStore, getLiveThreadTargets: () => [{ chatId: 7, threadId: 55 }],
    getMessageOwnership: () => undefined });
  f.worker = f.createWorker(true, f.resolve()!.journal);
  const canonical = readFileSync(f.path, "utf8"), publishedChoosers = f.choosers.length;
  f.worker.start(freshContext); await f.worker.waitForDrain(); await f.harness.routeRuntime.waitForRestoreSettlement();
  const assertProtected = () => {
    assert.deepEqual(f.journal.read().entries.filter(value => [123, 150].includes(value.updateId)).map(value => value.update), sourceBodies);
    assert.deepEqual([123, 150].map(id => f.journal.read().entries.find(value => value.updateId === id)?.state), ["pending", "pending"]);
    assert.deepEqual(coldStore.list()[0], intent, "startup and stale controls never adopt or reset an uncertain dispatch");
    assert.equal(readFileSync(f.path, "utf8"), canonical);
    assert.equal(f.harness.events.includes("status-menu"), false, "a fresh worker never repeats predecessor execution");
    assert.equal(f.harness.telegramQueueStore.getQueuedItems().length, 0, "the sibling cannot fall through into bound-tab execution");
    assert.equal(f.choosers.length, publishedChoosers, "protected cold originals currently get no reconstructed chooser");
    assert.equal(f.acceptancePublications, 0);
    assert.deepEqual(f.journal.read().sourceCompletions ?? [], []);
    assert.equal(f.journal.inspectAbandonedPending(123), undefined);
    assert.equal(f.journal.inspectAbandonedPending(150), undefined);
    assert.equal(coldStore.listTemporaryThreads().length, 1);
    assert.equal(coldThreads.listWorkspaceBindings().length, 26);
    assert.deepEqual(coldThreads.listWorkspaceBindings(), f.threads.listWorkspaceBindings());
    assert.equal(f.creations.length, 1);
    assert.deepEqual(f.deletions(), []);
  };
  assertProtected();
  await f.click(`reroutenew:${rerouteId}:10`); assertProtected();
  await f.click(f.routeOf(1), 502); assertProtected();
  await f.click(f.cancelOf(1), 502); assertProtected();
  await f.click(`reroutecancel:${rerouteId}`); assertProtected();
}

async function assertRestoreScenario(scenario: AllCommandScenario, f: AllCommandFixture): Promise<void> {
  if (scenario.startsWith("restore-sibling")) {
    f.journal.appendBatch([{ update_id: 150, message: { message_id: 30, message_thread_id: 55,
      date: Math.floor(Date.now() / 1000), chat: { id: 7, type: "private" }, from: { id: 7, is_bot: false },
      ...(scenario === "restore-sibling-photo" ? { photo: [{ file_id: "p", file_unique_id: "p", width: 1, height: 1 }] } : { text: "sibling in the restored tab" }) } }]);
    f.worker.signal(); await f.worker.waitForDrain();
    assert.equal(f.choosers.length, 2);
    if (scenario === "restore-sibling-after-forward") {
      await f.click(f.routeOf(0), 501);
      assert.equal(f.harness.events.filter(event => event === "status-menu").length, 1);
      assert.equal(f.journal.read().entries.some(entry => entry.updateId === 123), false);
      assert.deepEqual(f.store.listTemporaryThreads()[0]?.completedInputs?.flatMap(input => input.updateIds), [123]);
      const promptId = f.choosers[1]!.markup.match(/reroutemenu:([a-z0-9]+)/)![1];
      await f.click(`rerouterestore:${promptId}`, 502);
      await f.click(`reroutenew:${promptId}:10`, 502);
      assert.equal(f.harness.telegramQueueStore.getQueuedItems().length, 1, "a proven completed Forward does not block Restore of the unassigned sibling");
      assert.equal(f.harness.events.filter(event => event === "status-menu").length, 1, "the earlier Forward is never repeated");
      assert.equal(f.journal.read().entries.find(entry => entry.updateId === 150)?.state, "queued");
      assert.equal(f.journal.inspectAbandonedPending(123), undefined, "completed input is not reclassified as cancelled");
      assert.deepEqual(f.threads.listWorkspaceBindings()[0]?.target, { chatId: 7, threadId: 55 });
      assert.equal(f.store.listTemporaryThreads().length, 1, "queue admission cannot retire the mixed lifecycle");
      await f.handOffQueuedInput();
      assert.equal(f.journal.read().entries.some(entry => entry.updateId === 150), false);
      assert.deepEqual(f.store.listTemporaryThreads(), [], "positive Restore disposition plus the earlier Forward releases membership without deletion");
      assert.deepEqual(f.deletions(), [], "Restore keeps the rebound tab");
      return;
    }
    if (scenario.startsWith("restore-sibling-from-prompt")) {
      // The text input, not the All command, selects Restore; the command is the unassigned sibling.
      const promptId = f.choosers[1]!.markup.match(/reroutemenu:([a-z0-9]+)/)![1];
      await f.click(`rerouterestore:${promptId}`, 502);
      await f.click(`reroutenew:${promptId}:10`, 502);
      assert.equal(f.harness.events.includes("status-menu"), false, "the command sibling is never delivered by the prompt's Restore");
      assert.equal(f.harness.telegramQueueStore.getQueuedItems().length, 1, "only the selected prompt is queued for the restored Pi");
      assert.equal(f.harness.telegramQueueStore.getQueuedItems()[0]!.admissionReceipts?.[0]?.sourceUpdateIds.join(), "150");
      assert.deepEqual(f.threads.listWorkspaceBindings()[0]?.target, { chatId: 7, threadId: 55 });
      assert.equal(f.journal.read().entries.find(entry => entry.updateId === 123)?.state, "pending", "queue admission alone settles nothing, so the sibling stays retained");
      assert.equal(f.journal.inspectAbandonedPending(123), undefined);
      assert.equal(f.store.listTemporaryThreads().length, 1);
      assert.equal(f.apiCalls.some(call => call.method === "deleteForumTopic"), false);
      if (scenario.endsWith("-forward-first") || scenario.endsWith("-restore-first") || scenario.endsWith("-authority-lost")) {
        const commandId = f.choosers[0]!.markup.match(/reroutemenu:([a-z0-9]+)/)![1];
        // Refresh the fixture's live view from the acknowledged leader identity; keep old-target protection intact.
        f.liveTargets.push(f.leaderIdentity.target);
        await f.click(`reroute:${commandId}:55`, 501);
        assert.deepEqual(f.harness.telegramQueueStore.getQueuedItems().flatMap(item => item.admissionReceipts!.flatMap(receipt => receipt.sourceUpdateIds)).sort(), [123, 150]);
        assert.deepEqual([123, 150].map(id => f.journal.read().entries.find(entry => entry.updateId === id)?.state), ["queued", "queued"]);
        assert.equal(f.journal.inspectAbandonedPending(123), undefined, "explicitly Forwarded work is not an unassigned sibling");
        const order = scenario.endsWith("-restore-first") ? [150, 123] : [123, 150];
        if (scenario.endsWith("-authority-lost")) {
          f.temporaryAuthority = false;
          await f.handOffQueuedInput(123);
          await f.handOffQueuedInput(150);
          assert.equal(f.store.listTemporaryThreads().length, 1, "a lost leader epoch cannot publish completion or release membership");
          assert.deepEqual(f.deletions(), []);
          assert.deepEqual(f.threads.listWorkspaceBindings()[0]?.target, { chatId: 7, threadId: 55 });
          return;
        }
        await f.handOffQueuedInput(order[0]);
        assert.equal(f.journal.read().entries.find(entry => entry.updateId === order[1])?.state, "queued", "the independent receipt stays owned and ready");
        assert.equal(f.harness.telegramQueueStore.getQueuedItems().length, 1);
        assert.equal(f.store.listTemporaryThreads().length, 1, "one unsettled input keeps membership protected");
        assert.equal(f.journal.inspectAbandonedPending(123), undefined, "Restore cannot cancel queued/running Forward work");
        assert.deepEqual(f.deletions(), []);
        await f.handOffQueuedInput(order[1]);
        assert.ok(f.completedHints.includes(123), "ordinary queued Forward publishes a source-disposition hint");
        assert.deepEqual([123, 150].filter(id => f.journal.read().entries.some(entry => entry.updateId === id)), []);
        assert.equal(f.journal.inspectAbandonedPending(123), undefined);
        assert.equal(f.journal.inspectAbandonedPending(150), undefined);
        assert.deepEqual(f.store.listTemporaryThreads(), [], "both positive dispositions release temporary membership in either order");
        assert.deepEqual(f.deletions(), [], "completion cannot delete the bound Restore tab");
        assert.deepEqual(f.threads.listWorkspaceBindings()[0]?.target, { chatId: 7, threadId: 55 });
        assert.equal(f.threads.listWorkspaceBindings()[0]?.slot, "A");
        return;
      }
      if (scenario === "restore-sibling-from-prompt-completed") {
        await f.handOffQueuedInput();
        assert.ok(f.journal.inspectAbandonedPending(123), "only positive selected-source disposition permits private sibling cancellation");
        assert.equal(f.journal.read().entries.some(entry => entry.updateId === 150), false);
        assert.equal(f.harness.events.includes("status-menu"), false);
        assert.deepEqual(f.store.listTemporaryThreads(), []);
        assert.deepEqual(f.deletions(), []);
        assert.deepEqual(f.threads.listWorkspaceBindings()[0]?.target, { chatId: 7, threadId: 55 });
        assert.equal(f.threads.listWorkspaceBindings()[0]?.slot, "A");
      }
      return;
    }
    const rerouteId = f.choosers[0]!.markup.match(/reroutemenu:([a-z0-9]+)/)![1];
    await f.click(`rerouterestore:${rerouteId}`);
    if (scenario.startsWith("restore-sibling-follower")) {
      if (f.restoreReplyBoundary) {
        await assertTemporaryRestoreRpcInterruption(scenario, f, rerouteId);
        return;
      }
      await f.click(`reroutenew:${rerouteId}:10`);
      if (f.loseDeliveryReply) {
        await f.click(`reroutenew:${rerouteId}:10`);
        await f.assertLostDeliveryProtection();
        assert.deepEqual(f.threads.listWorkspaceBindings()[0]?.target, { chatId: 7, threadId: 55 }, "the acknowledged relocation remains, but is not source completion");
        assert.equal(f.threads.listWorkspaceBindings()[0]?.slot, "A");
        return;
      }
      if (scenario.endsWith("-down")) {
        // The recipient never answered: nothing was forwarded, so nothing may be cancelled, settled or deleted.
        assert.deepEqual(f.forwardedIds(), []);
        assert.deepEqual([123, 150].map(id => f.journal.read().entries.find(entry => entry.updateId === id)?.state), ["pending", "pending"]);
        assert.equal(f.journal.inspectAbandonedPending(150), undefined);
        assert.equal(f.journal.inspectAbandonedPending(123), undefined);
        assert.equal(f.store.listTemporaryThreads().length, 1);
        assert.equal(f.apiCalls.some(call => call.method === "deleteForumTopic"), false);
        assert.equal(f.harness.telegramQueueStore.getQueuedItems().length, 0, "retained inputs never fall through to bound execution");
        assert.equal(f.harness.events.includes("status-menu"), false);
        return;
      }
      assert.deepEqual(f.forwardedIds(), [123], "only the selected input reaches the restored follower");
      f.assertFollowerExecution();
      if (f.drainRecipient) {
        await f.click(`reroutenew:${rerouteId}:10`);
        f.assertFollowerExecution();
        assert.deepEqual(f.forwardedIds(), [123], "the local chooser publisher refuses stale Restore without admitting another message");
      }
      assert.equal(f.harness.events.includes("status-menu"), false);
      assert.deepEqual(f.threads.listWorkspaceBindings()[0]?.target, { chatId: 7, threadId: 55 }, "the follower's Workspace is rebound to the tab");
      assert.equal(f.threads.listWorkspaceBindings()[0]?.slot, "A");
      assert.deepEqual(f.followerRegistry.get("old")?.target, { chatId: 7, threadId: 55 });
      assert.equal(f.journal.read().entries.find(entry => entry.updateId === 150)?.state, undefined, "the sibling is privately abandoned");
      assert.ok(f.journal.inspectAbandonedPending(150));
      assert.ok(f.edits.includes("<b>⛔️ Routing cancelled.</b>"));
      assert.equal(f.apiCalls.some(call => call.method === "deleteForumTopic" && call.body.message_thread_id === 55), false, "Restore keeps the tab");
      assert.deepEqual(f.store.listTemporaryThreads(), [], "the single completed group releases the entry");
      assert.equal(f.harness.telegramQueueStore.getQueuedItems().length, 0, "a sibling is never delivered by Restore");
      return;
    }
    if (scenario === "restore-sibling-authority-lost") f.temporaryAuthority = false;
    if (scenario === "restore-sibling-restart" || scenario === "restore-sibling-cold-successor") {
      // The leader epoch ends right after the selected command ran: the tab is bound, nothing is settled.
      const push = f.harness.events.push.bind(f.harness.events);
      f.harness.events.push = (...items: string[]) => { if (items.includes("status-menu")) f.temporaryAuthority = false; return push(...items); };
    }
    await f.click(`reroutenew:${rerouteId}:10`);
    const states = (id: number) => f.journal.read().entries.find(entry => entry.updateId === id)?.state;
    if (scenario === "restore-sibling-photo") {
      assert.equal(f.harness.events.filter(event => event === "status-menu").length, 0, "an unretainable sibling blocks Restore before selection");
      assert.deepEqual([states(123), states(150)], ["pending", "pending"]);
      assert.deepEqual(f.store.list(), []);
      assert.equal(f.journal.inspectAbandonedPending(150), undefined);
      assert.equal(f.store.listTemporaryThreads().length, 1);
      assert.equal(f.creations.length, 1);
      return;
    }
    if (scenario === "restore-sibling-cold-successor") {
      await assertColdTemporaryRestoreHold(f, rerouteId);
      return;
    }
    if (scenario === "restore-sibling-restart") {
      assert.deepEqual(f.threads.listWorkspaceBindings()[0]?.target, { chatId: 7, threadId: 55 }, "the tab is bound");
      assert.equal(states(150), "pending");
      f.temporaryAuthority = true;
      await f.worker.stop(); f.harness = f.createHarness(); f.worker = f.createWorker(true);
      f.worker.start({ cwd: "/repo" }); await f.worker.waitForDrain(); await f.harness.routeRuntime.waitForRestoreSettlement();
      assert.equal(states(150), "pending", "the recorded sibling stays retained after restart");
      assert.equal(f.harness.telegramQueueStore.getQueuedItems().length, 0, "it never falls through to bound-tab execution");
      assert.equal(f.harness.events.includes("status-menu"), false, "the restarted runtime does not re-execute the command");
      assert.equal(f.journal.inspectAbandonedPending(150), undefined, "holding never cancels or discards it");
      assert.equal(f.store.listTemporaryThreads().length, 1);
      return;
    }
    if (scenario === "restore-sibling-authority-lost") {
      assert.equal(f.harness.events.includes("status-menu"), false, "a lost leader epoch performs no Restore delivery");
      assert.deepEqual([states(123), states(150)], ["pending", "pending"]);
      assert.equal(f.journal.inspectAbandonedPending(150), undefined);
      assert.equal(f.store.listTemporaryThreads().length, 1);
      assert.equal(f.harness.telegramQueueStore.getQueuedItems().length, 0);
      return;
    }
    assert.equal(f.harness.events.filter(event => event === "status-menu").length, 1, "only the selected input reaches Pi");
    if (scenario !== "restore-sibling-before") assert.equal(f.journal.read().entries.some(entry => entry.updateId === 123), false);
    assert.equal(f.harness.telegramQueueStore.getQueuedItems().length, 0, "a sibling is never delivered by Restore");
    assert.deepEqual(f.threads.listWorkspaceBindings()[0]?.target, { chatId: 7, threadId: 55 });
    assert.equal(f.threads.listWorkspaceBindings()[0]?.slot, "A");
    assert.equal(f.apiCalls.some(call => call.method === "deleteForumTopic" && call.body.message_thread_id === 55), false, "Restore keeps the tab");
    if (scenario === "restore-sibling-before") {
      assert.equal(states(150), "pending", "an unproven Restore never cancels a sibling");
      assert.equal(f.journal.inspectAbandonedPending(150), undefined);
      assert.equal(f.store.listTemporaryThreads().length, 1);
      assert.equal(f.harness.telegramQueueStore.getQueuedItems().length, 0, "the retained sibling cannot fall through to bound execution");
      return;
    }
    assert.equal(states(150), undefined, "the sibling's journal source is retired by private abandonment");
    assert.ok(f.journal.inspectAbandonedPending(150), "the sibling original is retained privately");
    assert.ok(f.edits.includes("<b>⛔️ Routing cancelled.</b>"), "the sibling control is invalidated");
    assert.deepEqual(f.store.listTemporaryThreads(), [], "the single uncancelled completed group releases the entry");
    await f.click(f.routeOf(1), 502);
    assert.equal(f.harness.events.filter(event => event === "status-menu").length, 1, "a stale sibling control cannot route it");
    assert.equal(f.harness.telegramQueueStore.getQueuedItems().length, 0);
    return;
  }
  const rerouteId = f.choosers[0]!.markup.match(/reroutemenu:([a-z0-9]+)/)![1];
  await f.click(`rerouterestore:${rerouteId}`);
  await f.click(`reroutenew:${rerouteId}:10`);
  if (f.queuedCommand) {
    assert.equal(f.harness.telegramQueueStore.getQueuedItems().length, 1);
    const source = f.journal.read().entries.find(value => value.updateId === 123)!;
    assert.equal(source.state, "queued", "accepted command retains its exact source until queue-owned settlement");
    const item = f.harness.telegramQueueStore.getQueuedItems()[0]!;
    const receipt = item.admissionReceipts![0]!;
    assert.equal(receipt.queueKind, "prompt", "a continuation stays a control-lane prompt, not a control receipt");
    const ready = scenario === "restore-queue-continue" || scenario === "restore-queue-after";
    assert.equal(f.worker.isQueueReceiptCommitted(receipt), ready);
    assert.equal(f.acceptancePublications, 1);
    assert.equal(f.store.list()[0]?.routing?.acceptances?.[0]?.kind, ready ? "queued" : undefined);
    assert.deepEqual(f.store.list()[0]?.routing?.settlements.flatMap(value => value.updateIds), ready ? [123] : []);
    assert.equal(f.harness.events.some(event => event.includes("Workspace Restore acceptance source or delivery changed")), false,
      "redundant command completion must not attempt a completed acceptance for already queued work");
    assert.deepEqual(f.threads.listWorkspaceBindings()[0]?.target, { chatId: 7, threadId: 55 });
    assert.equal(f.threads.listWorkspaceBindings()[0]?.slot, "A");
    assert.equal(f.store.listTemporaryThreads().length, 1, "queue readiness alone cannot retire temporary source custody");
    assert.equal(f.apiCalls.some(call => call.method === "deleteForumTopic"), false);
    const queued = JSON.stringify(f.harness.telegramQueueStore.getQueuedItems());
    if (scenario === "restore-queue-cold") {
      const bytes = readFileSync(f.options.path, "utf8");
      await f.worker.stop(); f.queuePublicationFault = false;
      f.worker.start({ cwd: "/repo" }); await f.worker.waitForDrain(); await f.harness.routeRuntime.waitForRestoreSettlement();
      assert.equal(f.worker.isQueueReceiptCommitted(receipt), true);
      assert.equal(f.store.list()[0]?.routing?.acceptances?.[0]?.kind, "queued");
      assert.deepEqual(f.store.list()[0]?.routing?.settlements.flatMap(value => value.updateIds), [123]);
      assert.equal(readFileSync(f.options.path, "utf8"), bytes, "same-process reconstruction publishes proof without disposition");
    }
    await f.click(`reroutenew:${rerouteId}:10`);
    assert.equal(JSON.stringify(f.harness.telegramQueueStore.getQueuedItems()), queued);
    if (ready) assert.equal(f.acceptancePublications, 1);
    assert.equal(f.harness.events.some(event => event.includes("Workspace Restore acceptance source or delivery changed")), false);
    assert.equal(f.creations.length, 1);
    assert.deepEqual(f.queueAdmission.read().leases, []);
    return;
  }
  assert.equal(f.harness.events.filter(event => event === "status-menu").length, scenario === "restore-local-compact" ? 0 : 1,
    `the command retains its semantics in the restored Pi: ${f.harness.events.join(" | ")}`);
  if (scenario === "restore-local-compact") assert.equal(f.choosers.length, 2, "compact opens its existing confirmation; it does not enqueue or execute immediately");
  assert.deepEqual(f.leaderIdentity.target, { chatId: 7, threadId: 55 }, "the leader now answers in the tab");
  await f.threads.load();
  assert.deepEqual(f.threads.listWorkspaceBindings()[0]?.target, { chatId: 7, threadId: 55 }, "the existing Workspace is rebound to the tab");
  assert.equal(f.threads.listWorkspaceBindings()[0]?.slot, "A", "its slot is preserved");
  if (f.fullSlots) {
    assert.equal(f.threads.listWorkspaceBindings().length, 26, "full slots: no allocation or eviction");
    assert.deepEqual(f.threads.listWorkspaceBindings().filter(value => value.slot !== "A").map(value => value.target.threadId), Array.from({ length: 25 }, (_, i) => 101 + i));
    assert.equal(f.creations.length, 1);
  }
  const removed = scenario === "restore" || f.fullSlots || scenario === "restore-local-after" || scenario === "restore-local-compact";
  assert.equal(f.acceptancePublications, 1, "duplicate completion reports do not publish another acceptance");
  assert.equal(f.journal.read().entries.some(value => value.updateId === 123), !removed);
  assert.equal(f.store.list()[0]?.routing?.acceptances?.[0]?.kind, removed ? "completed" : undefined);
  if (removed) {
    const intent = f.store.list()[0]!, acceptance = intent.routing!.acceptances![0]!;
    const completion = { updateId: acceptance.updateId, sourceSha256: acceptance.sourceSha256,
      completionSha256: Threads.getTelegramWorkspaceRestoreSourceCompletionSha256(intent, acceptance) };
    assert.deepEqual(f.resolve()!.journal.inspectSourceCompletion(completion), completion);
  }
  assert.equal(f.store.listTemporaryThreads().length, removed ? 0 : 1, "failed proof publication retains temporary source protection");
  assert.equal(f.apiCalls.some(call => call.method === "deleteForumTopic" && call.body.message_thread_id === 55), false,
    "Restore keeps the tab");
  assert.equal(f.creations.length, 1);
  assert.equal(f.store.list()[0]?.phase, "ready");
  assert.deepEqual(f.store.list()[0]?.routing?.settlements.flatMap(value => value.updateIds), removed ? [123] : []);
  await f.click(`reroutenew:${rerouteId}:10`);
  assert.equal(f.harness.events.filter(event => event === "status-menu").length, scenario === "restore-local-compact" ? 0 : 1, "issued local work never re-executes after publication failure");
  if (scenario === "restore-local-compact") assert.equal(f.choosers.length, 2, "reclick cannot publish another confirmation");
  assert.equal(f.acceptancePublications, 1);
  return;
}

async function assertSiblingProtection(scenario: AllCommandScenario, f: AllCommandFixture): Promise<void> {
  const siblingThread = scenario === "forward-other-tab" ? 56 : 55;
  f.journal.appendBatch([{ update_id: 150, message: { message_id: 30, message_thread_id: siblingThread,
    date: Math.floor(Date.now() / 1000), chat: { id: 7, type: "private" },
    from: { id: 7, is_bot: false }, text: "independent pending sibling" } }]);
  f.worker.signal(); await f.worker.waitForDrain();
  assert.equal(f.choosers.length, 2);
  const action = scenario === "cancel-with-prompt" ? f.cancelOf(0) : f.routeOf(0);
  await f.click(action);
  const removed = f.deletions();
  assert.equal(removed.length, scenario === "forward-other-tab" ? 1 : 0,
    "settling a temporary tab's first source cannot erase another input in that same tab");
  assert.equal(f.journal.read().entries.find(entry => entry.updateId === 150)?.state, "pending",
    "the sibling stays under independent durable routing custody");
  assert.equal(f.harness.telegramQueueStore.getQueuedItems().length, 0, "the sibling is never implicitly dispatched");
  assert.equal(f.store.listTemporaryThreads().length, scenario === "forward-other-tab" ? 0 : 1,
    "blocked deletion keeps canonical temporary-tab protection");
  assert.equal(f.journal.read().entries.some(entry => entry.updateId === 123), false,
    "tab protection does not roll back acknowledged source completion or cancellation");
  if (scenario === "cancel-with-prompt") assert.ok(f.journal.inspectAbandonedPending(123));
  else assert.equal(f.harness.events.filter(event => event === "status-menu").length, 1, "the selected command still runs once");
  await f.click(action);
  assert.equal(f.deletions().length, removed.length,
    "a stale or cleanup-only reclick cannot bypass sibling protection");
  return;
}

async function runAllCommandScenario(scenario: AllCommandScenario): Promise<void> {
  await fixture(async input => {
    const f = createAllCommandFixture(scenario, input);
    try {
      await f.start();
      if (scenario.startsWith("restore")) await assertRestoreScenario(scenario, f);
      else if (scenario.startsWith("cleanup-") || scenario.startsWith("membership-")) await assertCleanupAndMembershipScenario(scenario, f);
      else if (scenario.startsWith("forward") || scenario === "cancel-then-forward") await assertForwardScenario(scenario, f);
      else if (scenario.startsWith("cancel")) await assertCancelScenario(scenario, f);
      else await assertCreationScenario(scenario, f);
    } finally { await f.stop(); }
  }, scenario.includes("-follower") ? "follower" : "leader");
}

// Restore and queued-receipt proofs require strict no-follow journal handles; without them (Windows) Restore fails closed.
const strictJournalUnsupported = !constants.O_NOFOLLOW || !constants.O_NONBLOCK;
const strictAllCommandScenario = (scenario: string): boolean =>
  scenario.startsWith("restore") || scenario === "forward-sibling-follower-ipc-worker" || scenario === "forward-prompt-then-cancel";

for (const scenarios of Object.values(ALL_COMMAND_SCENARIOS)) {
  for (const scenario of scenarios) {
    test(`An All command opens one source-bound temporary Thread and keeps its original (${scenario})`,
      { skip: strictJournalUnsupported && strictAllCommandScenario(scenario) }, () => runAllCommandScenario(scenario));
  }
}

for (const scenario of ["successor", "original", "not-ready", "lost-reply", "old-target", "foreign-session", "relocated",
  "relocated-old-target", "stale-hint", "leader-owner", "foreign-journal"] as const) {
  test(`Recipient heartbeat lets a same-session successor inspect an issued Restore after leader succession (${scenario})`, async () => {
    await fixture(async ({ store, threads, auth, path }) => {
      const request = { operationId: "issued-restore", binding: threads.listWorkspaceBindings()[0]!, owner: threads.list()[0]!,
        target: { chatId: 7, threadId: 42 }, source: { journalBindingKey: "source-journal", updateIds: [123] } };
      const relocated = (await store.commit(request, auth))!;
      const unissued = scenario.startsWith("relocated");
      const retained = unissued ? relocated : store.issueRecipient(relocated, recipient("follower"), auth)!.intent;
      const protocol = Bus.createTelegramBusProtocolIdentity({ runtimeBuild: "fixture", capabilities: [Bus.TELEGRAM_BUS_CAPABILITY_WORKSPACE_RESTORE] });
      const successor: Bus.TelegramBusFollowerView = { instanceId: scenario === "original" ? "old" : "successor",
        registrationGeneration: scenario === "original" ? "registration" : "next", cwd: "/repo/",
        sessionId: scenario === "foreign-session" ? "foreign" : "session", slot: "A",
        target: scenario === "old-target" || scenario === "relocated-old-target" ? request.binding.target : request.target,
        protocol, connectedAtMs: 1, lastHeartbeatMs: 1 };
      const registry = Bus.createTelegramBusFollowerRegistry();
      registry.register(successor);
      const modes: string[] = [];
      let loseReply = scenario === "lost-reply";
      let hintCurrent = scenario !== "stale-hint";
      const observed = { kind: "follower" as const, instanceId: successor.instanceId, sessionId: "session", generation: successor.registrationGeneration! };
      const harness = createRouteHarness({ threadStore: threads, instanceId: "leader", getSessionGeneration: () => 1,
        configStore: { get: () => ({} as never), getAllowedUserId: () => 7, persistAllowedUserId: async () => true, persist: async () => undefined },
        getAdmissionJournalBinding: () => scenario === "foreign-journal" ? "other-journal" : "source-journal",
        getCurrentLeaderEpoch: () => "successor-epoch", isContextActive: () => true,
        getWorkspaceRestoreStore: () => store, hasWorkspaceRestoreAuthority: () => true,
        async runWorkspaceOperation(_input, operation) { return operation(); },
        async callApi() { assert.fail("Readiness inspection must not call Telegram APIs"); },
        workspaceRestoreRecipient: { getSessionId: () => "session", getCwd: () => "/repo", getLeaderIdentity: () => undefined,
          followerRegistry: registry, async runFollower(input) {
            modes.push(input.mode);
            assert.equal(input.isCurrent(), true);
            assert.deepEqual(store.list()[0]?.executor, { instanceId: "leader", leaderEpoch: "successor-epoch" }, "inspection follows durable adoption");
            if (loseReply) { loseReply = false; throw new Error("fixture readiness reply lost"); }
            return { operationId: input.operationId, recipient: observed, target: request.target, slot: "A", ready: scenario !== "not-ready" };
          } } });
      const heartbeat = async () => {
        await harness.routeRuntime.onWorkspaceRestoreRecipientObserved(structuredClone(successor), () => hintCurrent, {} as TestContext)?.catch(() => undefined);
        await harness.routeRuntime.waitForRestoreSettlement();
      };
      const before = await readFile(path, "utf8");
      await heartbeat();
      const inspects = ["successor", "original", "not-ready", "lost-reply", "relocated"].includes(scenario);
      if (!inspects) {
        assert.deepEqual(modes, [], "ineligible hints never reach the recipient");
        assert.equal(await readFile(path, "utf8"), before, "ineligible hints never adopt or rewrite evidence");
        assert.deepEqual(store.list(), [retained]);
        return;
      }
      if (scenario === "lost-reply") {
        assert.equal(store.list()[0]?.phase, "recipient-issued", "a lost inspection reply keeps the issued grant");
        await heartbeat();
      }
      const intent = store.list()[0]!;
      assert.equal(modes.every(mode => mode === "inspect"), true, "a hint never consumes the original apply grant");
      assert.equal(modes.length, scenario === "lost-reply" ? 2 : 1);
      assert.deepEqual(intent.executor, { instanceId: "leader", leaderEpoch: "successor-epoch" });
      // An unissued Restore gives its first grant to the successor already on the tab; an issued one keeps its original.
      assert.deepEqual(intent.recipient, unissued ? observed : recipient("follower"), "issuance identity never changes once recorded");
      assert.deepEqual(intent.request, request);
      assert.equal(intent.routing, undefined, "readiness never issues dispatch or cleanup");
      assert.equal(harness.telegramQueueStore.getQueuedItems().length, 0);
      if (scenario === "not-ready") assert.equal(intent.phase, "recipient-issued");
      else {
        assert.equal(intent.phase, "ready");
        assert.deepEqual(intent.readyRecipient, observed);
        const settled = await readFile(path, "utf8");
        await heartbeat();
        assert.equal(modes.length, scenario === "lost-reply" ? 2 : 1, "ready operations leave the inspection path");
        assert.equal(await readFile(path, "utf8"), settled);
      }
      hintCurrent = false;
    }, scenario === "leader-owner" ? "leader" : "follower");
  });
}

for (const scenario of ["successor", "lost-reply", "record-not-adopted", "same-instance", "old-local-target", "foreign-session", "other-cwd",
  "relocated", "relocated-same-instance", "follower-owner", "foreign-journal", "inactive-context"] as const) {
  test(`Source completion lets the same-session successor leader inspect its issued Restore (${scenario})`, async () => {
    await fixture(async ({ store, threads, auth, path }) => {
      const request = { operationId: "leader-restore", binding: threads.listWorkspaceBindings()[0]!, owner: threads.list()[0]!,
        target: { chatId: 7, threadId: 42 }, source: { journalBindingKey: "source-journal", updateIds: [123] } };
      const relocated = (await store.commit(request, auth))!;
      const unissued = scenario.startsWith("relocated");
      const sameInstance = scenario === "same-instance" || scenario === "relocated-same-instance";
      const retained = unissued ? relocated
        : store.issueRecipient(relocated, recipient(scenario === "follower-owner" ? "follower" : "leader"), auth)!.intent;
      // Precondition: startup provisioning of the successor process owns the relocated leader record.
      if (scenario !== "record-not-adopted" && scenario !== "follower-owner" && !sameInstance) {
        threads.upsert({ ...threads.list()[0]!, instanceId: "next-leader", owner: { kind: "leader", cwd: "/repo", instanceId: "next-leader" } });
        await threads.persist();
      }
      let loseReply = scenario === "lost-reply";
      const failingStore: Threads.TelegramWorkspaceRestore = { ...store, confirmInspectedReady(expected, observed, authority) {
        const result = store.confirmInspectedReady(expected, observed, authority);
        if (loseReply) { loseReply = false; throw new Error("fixture readiness publication reply lost"); }
        return result;
      } };
      const harness = createRouteHarness({ threadStore: threads, instanceId: sameInstance ? "old" : "next-leader", getSessionGeneration: () => 3,
        configStore: { get: () => ({} as never), getAllowedUserId: () => 7, persistAllowedUserId: async () => true, persist: async () => undefined },
        getAdmissionJournalBinding: () => "source-journal", getCurrentLeaderEpoch: () => "next-epoch",
        isContextActive: () => scenario !== "inactive-context",
        getWorkspaceRestoreStore: () => failingStore, hasWorkspaceRestoreAuthority: () => true,
        async runWorkspaceOperation(_input, operation) { return operation(); },
        async callApi() { assert.fail("Readiness inspection must not call Telegram APIs"); },
        setCurrentLeaderIdentity() { assert.fail("Inspection must not apply a leader target"); },
        workspaceRestoreRecipient: { getSessionId: () => scenario === "foreign-session" ? "other" : "session",
          getCwd: () => scenario === "other-cwd" ? "/other" : "/repo/",
          getLeaderIdentity: () => ({ slot: "A", target: scenario === "old-local-target" ? request.binding.target : request.target }),
          followerRegistry: Bus.createTelegramBusFollowerRegistry(),
          async runFollower() { assert.fail("A leader recipient never uses follower IPC"); } } });
      const complete = async () => {
        harness.routeRuntime.onUpdateCompleted(999, {} as TestContext, scenario === "foreign-journal" ? "other-journal" : "source-journal");
        await harness.routeRuntime.waitForRestoreSettlement();
      };
      const before = await readFile(path, "utf8");
      await complete();
      const inspects = ["successor", "lost-reply", "record-not-adopted", "relocated"].includes(scenario);
      if (!inspects) {
        assert.equal(await readFile(path, "utf8"), before, "ineligible completions never adopt or rewrite evidence");
        assert.deepEqual(store.list(), [retained]);
        return;
      }
      if (scenario === "lost-reply") await complete();
      const intent = store.list()[0]!;
      assert.deepEqual(intent.executor, { instanceId: "next-leader", leaderEpoch: "next-epoch" });
      assert.deepEqual(intent.recipient, unissued ? { kind: "leader", instanceId: "next-leader", sessionId: "session", generation: "3" }
        : recipient("leader"), "issuance identity never changes once recorded");
      assert.deepEqual(intent.request, request);
      assert.equal(intent.routing, undefined, "readiness never issues dispatch or cleanup");
      if (scenario === "record-not-adopted") {
        assert.equal(intent.phase, "recipient-issued", "an unowned canonical record is not readiness");
        assert.equal(intent.readyRecipient, undefined);
        return;
      }
      assert.equal(intent.phase, "ready");
      assert.deepEqual(intent.readyRecipient, { kind: "leader", instanceId: "next-leader", sessionId: "session", generation: "3" });
      const settled = await readFile(path, "utf8");
      await complete();
      assert.equal(await readFile(path, "utf8"), settled, "ready operations leave the inspection path");
    }, scenario === "follower-owner" ? "follower" : "leader");
  });
}

test("Published All chooser expiry rejects the old button without retaining its source", async (t) => {
  t.mock.timers.enable({ apis: ["Date", "setTimeout"], now: 10_000_000 });
  await withTopicStore(async (threadStore, path) => {
    threadStore.upsert({
      profileKey: "cwd:/repo", target: { chatId: 100, threadId: 42 },
      status: "active", createdAtMs: Date.now(), updatedAtMs: Date.now(),
      instanceId: "leader-a", slot: "A", threadName: "Axial",
    });
    await threadStore.persist();
    const { events, routeRuntime, telegramQueueStore } = createRouteHarness({ threadStore });
    const update = { update_id: 123, message: {
      message_id: 12, date: Date.now() / 1000, chat: { id: 100, type: "private" as const },
      from: { id: 7, is_bot: false }, text: "/start",
    } };
    const journal = Journal.createTelegramUpdateJournalStore({
      path: `${path}.inbox`,
      botIdentity: Journal.createTelegramUpdateJournalBotIdentity({ botToken: "123:expiry-fixture" }),
    });
    journal.appendBatch([update]);
    const worker = Updates.createTelegramUpdateAdmissionWorkerRuntime<typeof update, TestContext>({
      journal, hasAuthority: () => true,
      defaultHandle: (input, ctx) => routeRuntime.handleUpdate(input, ctx),
    });
    try {
      worker.start({ cwd: "/repo" });
      await worker.waitForDrain();
      t.mock.timers.tick(Routing.TELEGRAM_ALL_TAB_COMMAND_MAX_AGE_MS - 1);
      await new Promise<void>((resolve) => setImmediate(resolve));
      assert.equal(journal.read().entries.length, 0);
      t.mock.timers.tick(1);
      await new Promise<void>((resolve) => setImmediate(resolve));
      assert.deepEqual(journal.read().entries, []);
      assert.equal(worker.getState().deferredClaimCount, 0);
      await routeRuntime.handleUpdate({ callback_query: {
        id: "expired", from: { id: 7, is_bot: false },
        message: { message_id: 99, chat: { id: 100, type: "private" } },
        data: "reroute:1:42",
      } }, { cwd: "/repo" });
      assert.ok(events.includes("answer:⌛ Routing choice expired."));
      assert.deepEqual(telegramQueueStore.getQueuedItems(), []);
      assert.equal(events.filter((event) => event.startsWith("interactive:html:")).length, 1);
    } finally {
      await worker.stop();
    }
  });
});

test("Selected All command settles after dispatch without awaiting background menu delivery", async (t) => {
  t.mock.timers.enable({ apis: ["Date", "setTimeout"], now: 10_000_000 });
  await withTopicStore(async (threadStore, path) => {
    threadStore.upsert({
      profileKey: "cwd:/repo", target: { chatId: 100, threadId: 42 },
      status: "active", createdAtMs: Date.now(), updatedAtMs: Date.now(),
      instanceId: "leader-a", slot: "A", threadName: "Axial",
    });
    await threadStore.persist();
    let release!: () => void;
    const gate = new Promise<void>((resolve) => { release = resolve; });
    let markStarted!: () => void;
    const started = new Promise<void>((resolve) => { markStarted = resolve; });
    const { events, routeRuntime } = createRouteHarness({ threadStore, sendStatusMessage: () => { markStarted(); return gate; } });
    const update = { update_id: 123, message: {
      message_id: 12, date: Date.now() / 1000, chat: { id: 100, type: "private" as const },
      from: { id: 7, is_bot: false }, text: "/start",
    } };
    const journal = Journal.createTelegramUpdateJournalStore({
      path: `${path}.inbox`,
      botIdentity: Journal.createTelegramUpdateJournalBotIdentity({ botToken: "123:selected-command" }),
    });
    journal.appendBatch([update]);
    const worker = Updates.createTelegramUpdateAdmissionWorkerRuntime<typeof update, TestContext>({
      journal, hasAuthority: () => true,
      defaultHandle: (input, ctx) => routeRuntime.handleUpdate(input, ctx),
    });
    let callback: Promise<void> | undefined;
    try {
      worker.start({ cwd: "/repo" });
      await worker.waitForDrain();
      t.mock.timers.tick(Routing.TELEGRAM_ALL_TAB_COMMAND_MAX_AGE_MS - 1);
      callback = routeRuntime.handleUpdate({ callback_query: {
        id: "selected", from: { id: 7, is_bot: false },
        message: { message_id: 99, chat: { id: 100, type: "private" } },
        data: "reroute:1:42",
      } }, { cwd: "/repo" });
      await Promise.race([started, callback.then(() => { throw new Error(`Command returned before execution: ${JSON.stringify(events)}`); })]);
      assert.ok(events.includes("status-menu"));
      await callback;
      await new Promise<void>((resolve) => setImmediate(resolve));
      assert.deepEqual(journal.read().entries, []);
      t.mock.timers.tick(2);
      await new Promise<void>((resolve) => setImmediate(resolve));
      assert.deepEqual(journal.read().entries, []);
      release();
      assert.equal(events.filter((event) => event === "status-menu").length, 1);
    } finally {
      release();
      await callback;
      await worker.stop();
    }
  });
});

for (const finish of ["accepted", "expiry", "restart"] as const) {
test(`Failed All command callback preserves the chooser deadline after source completion: ${finish}`, async (t) => {
  t.mock.timers.enable({ apis: ["Date", "setTimeout"], now: 10_000_000 });
  await withTopicStore(async (threadStore, path) => {
    threadStore.upsert({
      profileKey: "manual:follower-b", owner: { kind: "manual-follower", instanceId: "follower-b" },
      target: { chatId: 100, threadId: 43 }, status: "active",
      createdAtMs: Date.now(), updatedAtMs: Date.now(), instanceId: "follower-b", slot: "B",
    });
    await threadStore.persist();
    let accepted = false;
    let forwarded = 0;
    let releaseFirst!: () => void;
    let markFirstStarted!: () => void;
    const firstGate = new Promise<void>((resolve) => { releaseFirst = resolve; });
    const firstStarted = new Promise<void>((resolve) => { markFirstStarted = resolve; });
    const { routeRuntime } = createRouteHarness({
      threadStore,
      getTargetOwnership: target => target.threadId === 43 ? rerouteFollowerOwnership("follower-b") : undefined,
      foreignOwnedUpdateForwarder: { forwardMessage: async () => {
        forwarded += 1;
        if (forwarded === 1) {
          markFirstStarted();
          await firstGate;
          throw new Error("fixture follower connection lost");
        }
        return accepted ? acceptedForeignUpdateSettlement() : retryableForeignUpdateSettlement();
      } },
    });
    const update = { update_id: 123, message: {
      message_id: 12, date: Date.now() / 1000, chat: { id: 100, type: "private" as const },
      from: { id: 7, is_bot: false }, text: "/start",
    } };
    const journal = Journal.createTelegramUpdateJournalStore({
      path: `${path}.inbox`,
      botIdentity: Journal.createTelegramUpdateJournalBotIdentity({ botToken: "123:follower-command" }),
    });
    journal.appendBatch([update]);
    const worker = Updates.createTelegramUpdateAdmissionWorkerRuntime<typeof update, TestContext>({
      journal, hasAuthority: () => true,
      defaultHandle: (input, ctx) => routeRuntime.handleUpdate(input, ctx),
    });
    const click = () => routeRuntime.handleUpdate({ callback_query: {
      id: "selected", from: { id: 7, is_bot: false },
      message: { message_id: 99, chat: { id: 100, type: "private" } },
      data: "reroute:1:43",
    } }, { cwd: "/repo" });
    try {
      worker.start({ cwd: "/repo" });
      await worker.waitForDrain();
      const firstClick = click();
      await firstStarted;
      await click();
      assert.equal(forwarded, 1);
      t.mock.timers.tick(Routing.TELEGRAM_ALL_TAB_COMMAND_MAX_AGE_MS - 1);
      await new Promise<void>((resolve) => setImmediate(resolve));
      assert.equal(journal.read().entries.length, 0);
      releaseFirst();
      await firstClick;
      await click();
      assert.equal(forwarded, 2);
      assert.equal(journal.read().entries.length, 0);
      if (finish === "accepted") {
        accepted = true;
        await click();
      } else {
        if (finish === "restart") await worker.stop();
        t.mock.timers.tick(1);
        if (finish === "restart") {
          worker.start({ cwd: "/repo" });
          await worker.waitForDrain();
        }
      }
      await new Promise<void>((resolve) => setImmediate(resolve));
      const expectedForwards = finish === "accepted" ? 3 : 2;
      assert.equal(forwarded, expectedForwards);
      assert.deepEqual(journal.read().entries, []);
      await click();
      assert.equal(forwarded, expectedForwards);
    } finally {
      releaseFirst();
      await worker.stop();
    }
  });
});
}

for (const stopWorker of [false, true]) {
  test(`All chooser delayed target lookup cannot dispatch after ${stopWorker ? "worker stop" : "expiry"}`, async (t) => {
    t.mock.timers.enable({ apis: ["Date", "setTimeout"], now: 10_000_000 });
    await withTopicStore(async (threadStore, path) => {
      threadStore.upsert({
        profileKey: "cwd:/repo", target: { chatId: 100, threadId: 42 },
        status: "active", createdAtMs: Date.now(), updatedAtMs: Date.now(),
        instanceId: "leader-a", slot: "A", threadName: "Axial",
      });
      await threadStore.persist();
      const { events, routeRuntime } = createRouteHarness({ threadStore });
      const update = { update_id: 123, message: {
        message_id: 12, date: Date.now() / 1000, chat: { id: 100, type: "private" as const },
        from: { id: 7, is_bot: false }, text: "/start",
      } };
      const journal = Journal.createTelegramUpdateJournalStore({
        path: `${path}.inbox`,
        botIdentity: Journal.createTelegramUpdateJournalBotIdentity({ botToken: "123:expiry-race" }),
      });
      journal.appendBatch([update]);
      const worker = Updates.createTelegramUpdateAdmissionWorkerRuntime<typeof update, TestContext>({
        journal, hasAuthority: () => true,
        defaultHandle: (input, ctx) => routeRuntime.handleUpdate(input, ctx),
      });
      let release!: () => void;
      const gate = new Promise<void>((resolve) => { release = resolve; });
      let lookupStarted = false;
      let callback: Promise<void> | undefined;
      try {
        worker.start({ cwd: "/repo" });
        await worker.waitForDrain();
        t.mock.method(threadStore, "load", async () => { lookupStarted = true; await gate; });
        callback = routeRuntime.handleUpdate({ callback_query: {
          id: "delayed", from: { id: 7, is_bot: false },
          message: { message_id: 99, chat: { id: 100, type: "private" } },
          data: "reroute:1:42",
        } }, { cwd: "/repo" });
        await new Promise<void>((resolve) => setImmediate(resolve));
        assert.equal(lookupStarted, true);
        if (stopWorker) await worker.stop();
        t.mock.timers.tick(stopWorker ? 1 : Routing.TELEGRAM_ALL_TAB_COMMAND_MAX_AGE_MS);
        await new Promise<void>((resolve) => setImmediate(resolve));
        assert.equal(journal.read().entries.length, 0);
        release();
        await callback;
        assert.ok(events.includes("answer:⌛ Routing choice expired."));
        assert.equal(events.includes("status-menu"), false);
        assert.equal(events.includes("dispatch"), false);
        if (stopWorker) {
          t.mock.timers.tick(Routing.TELEGRAM_ALL_TAB_COMMAND_MAX_AGE_MS);
          await new Promise<void>((resolve) => setImmediate(resolve));
          assert.equal(journal.read().entries.length, 0);
        }
      } finally {
        release();
        await callback;
        await worker.stop();
      }
    });
  });
}

test("Published All command chooser does not replay across worker and routing restarts", async (t) => {
  let now = 10_000_000;
  t.mock.method(Date, "now", () => now);
  await withTopicStore(async (threadStore, path) => {
    threadStore.upsert({
      profileKey: "cwd:/repo", target: { chatId: 100, threadId: 42 },
      status: "active", createdAtMs: now, updatedAtMs: now,
      instanceId: "leader-a", slot: "A", threadName: "Axial",
    });
    await threadStore.persist();
    const update = {
      update_id: 123,
      message: {
        message_id: 12, date: now / 1000, chat: { id: 100, type: "private" as const },
        from: { id: 7, is_bot: false }, text: "/start",
      },
    };
    const initial = createRouteHarness({ threadStore });
    const admit = (runtime: typeof initial.routeRuntime) => Updates.createTelegramUpdateAdmissionHandle({
      defaultHandle: (input: typeof update, ctx: TestContext) => runtime.handleUpdate(input, ctx),
    });
    const openJournal = () => Journal.createTelegramUpdateJournalStore({
      path: `${path}.inbox`,
      botIdentity: Journal.createTelegramUpdateJournalBotIdentity({ botToken: "123:expiry-fixture" }),
      getNowMs: () => now,
    });
    const journal = openJournal();
    journal.appendBatch([update]);
    const runWorker = async (runtime: typeof initial.routeRuntime) => {
      const reopened = openJournal();
      const handle = admit(runtime);
      const executed: number[] = [];
      const worker = Updates.createTelegramUpdateWorkerRuntime<TestContext>({
        journal: reopened,
        hasAuthority: () => true,
        getNowMs: () => now,
        executeUpdate(input, ctx, signal) {
          executed.push(input.update_id);
          return handle(input as typeof update, ctx, signal);
        },
      });
      try {
        worker.start({ cwd: "/repo" });
        await worker.waitForDrain();
        return { executed, deferred: worker.getState().deferredClaimCount };
      } finally {
        await worker.stop();
      }
    };
    assert.deepEqual(await runWorker(initial.routeRuntime), { executed: [123], deferred: 0 });
    assert.equal(initial.events.filter((event) => event.startsWith("interactive:html:")).length, 1);
    assert.deepEqual(openJournal().read().entries, []);
    now += Routing.TELEGRAM_ALL_TAB_COMMAND_MAX_AGE_MS;
    const replacement = createRouteHarness({ threadStore });
    assert.deepEqual(await runWorker(replacement.routeRuntime), { executed: [], deferred: 0 });
    assert.equal(replacement.events.some((event) => event.startsWith("interactive:")), false);
    assert.deepEqual(replacement.telegramQueueStore.getQueuedItems(), []);
    assert.deepEqual(openJournal().read().entries, []);
    const finalReplacement = createRouteHarness({ threadStore });
    assert.deepEqual(await runWorker(finalReplacement.routeRuntime), { executed: [], deferred: 0 });
    assert.equal(finalReplacement.events.some((event) => event.startsWith("interactive:")), false);
  });
});

test("Routing runtime treats All menu commands as threaded target chooser", async () => {
  await withTopicStore(async (threadStore) => {
    threadStore.upsert({
      profileKey: "cwd:/repo",
      target: { chatId: 100, threadId: 42 },
      status: "active",
      createdAtMs: 1000,
      updatedAtMs: 1000,
      instanceId: "leader-a",
      slot: "A",
      threadName: "Axial",
    });
    threadStore.upsert({
      profileKey: "manual:follower-b",
      owner: { kind: "manual-follower", instanceId: "follower-b" },
      target: { chatId: 100, threadId: 43 },
      status: "starting",
      createdAtMs: 1000,
      updatedAtMs: 1000,
      instanceId: "follower-b",
      slot: "B",
    });
    threadStore.upsert({
      profileKey: "manual:follower-c",
      owner: { kind: "manual-follower", instanceId: "follower-c" },
      target: { chatId: 100, threadId: 44 },
      status: "active",
      createdAtMs: 1000,
      updatedAtMs: 1000,
      instanceId: "follower-c",
      slot: "C",
    });
    await threadStore.persist();
    const { events, routeRuntime, telegramQueueStore } = createRouteHarness({
      threadStore,
    });

    for (const [index, text] of ["/start", "/status"].entries()) {
      await routeRuntime.handleUpdate(
        {
          message: {
            message_id: 12 + index,
            chat: { id: 100, type: "private" },
            from: { id: 7, is_bot: false },
            text,
          },
        },
        { cwd: "/repo" },
      );
    }

    assert.deepEqual(telegramQueueStore.getQueuedItems(), []);
    assert.equal(events.includes("status-menu"), false);
    const chooserMessages = events.filter((event) =>
      event.startsWith("interactive:html:"),
    );
    assert.equal(chooserMessages.length, 2);
    for (const chooser of chooserMessages) {
      assert.match(chooser, /<b>🧵 Choose target thread:<\/b>/);
      assert.match(chooser, /You used <code>\/(?:start|status)<\/code> from the <b>All<\/b> tab\./);
      assert.match(chooser, /Select the Pi thread that should handle it:/);
      assert.doesNotMatch(chooser, /<code>active<\/code>/);
      assert.doesNotMatch(chooser, /<code>starting<\/code>/);
    }
    const roots = events.filter((event) => event.startsWith("markup:"));
    assert.equal(roots.length, 2);
    assert.match(roots[0]!, /reroutemenu:1/);
    assert.match(roots[1]!, /reroutemenu:2/);
    const initialOptions = events.filter(event => event.startsWith("interactive-options:"));
    const markups = [await openRerouteSubmenu(routeRuntime, events), await openRerouteSubmenu(routeRuntime, events, "2")];
    assert.match(markups[0] ?? "", /"text":"↪️ Axial"/);
    assert.match(markups[0] ?? "", /"text":"↪️ Coral"/);
    assert.doesNotMatch(markups[0] ?? "", /"text":"A Axial"/);
    assert.match(markups[0] ?? "", /reroute:1:42/);
    assert.match(markups[0] ?? "", /reroute:1:44/);
    assert.doesNotMatch(markups[0] ?? "", /rerouterestore:1/);
    assert.match(markups[1] ?? "", /reroute:2:42/);
    assert.doesNotMatch(markups[1] ?? "", /rerouterestore:2/);
    assert.doesNotMatch(markups.join("\n"), /reroute:[12]:43/);
    assert.deepEqual(initialOptions, [
      'interactive-options:{"replyToMessageId":12}',
      'interactive-options:{"replyToMessageId":13}',
    ]);
  });
});

test("Routing runtime filters All chooser buttons to live routable thread targets", async () => {
  await withTopicStore(async (threadStore) => {
    for (const record of [
      {
        profileKey: "leader:/repo",
        target: { chatId: 100, threadId: 42 },
        instanceId: "leader-a",
        slot: "A",
        threadName: "Axial",
      },
      {
        profileKey: "manual:follower-old",
        target: { chatId: 100, threadId: 99 },
        instanceId: "follower-old",
        slot: "Z",
        threadName: "Zombie",
      },
    ]) {
      threadStore.upsert({
        ...record,
        status: "active",
        createdAtMs: 1000,
        updatedAtMs: 1000,
      });
    }
    await threadStore.persist();
    const { events, routeRuntime } = createRouteHarness({
      threadStore,
      getLiveThreadTargets: () => [{ chatId: 100, threadId: 42 }],
    });

    await routeRuntime.handleUpdate(
      {
        message: {
          message_id: 20,
          chat: { id: 100, type: "private" },
          from: { id: 7, is_bot: false },
          text: "/start",
        },
      },
      { cwd: "/repo" },
    );
    const root = events.find((event) => event.startsWith("markup:"));
    assert.match(root ?? "", /reroutemenu:1/);
    const markup = await openRerouteSubmenu(routeRuntime, events);
    assert.match(markup ?? "", /reroute:1:42/);
    assert.doesNotMatch(markup ?? "", /rerouterestore:1/);
    assert.doesNotMatch(markup ?? "", /reroute:1:99/);
    assert.doesNotMatch(markup ?? "", /Zombie/);
    await routeRuntime.handleUpdate({ callback_query: {
      id: "threadless-restore-menu",
      from: { id: 7, is_bot: false },
      message: { message_id: 99, chat: { id: 100, type: "private" } },
      data: "rerouterestore:1",
    } }, { cwd: "/repo" });
    assert.equal(events.some((event) => event.includes("Restore needs a destination thread")), true);
    assert.equal(threadStore.getByProfileKey("leader:/repo")?.target.threadId, 42);

    await routeRuntime.handleUpdate(
      {
        callback_query: {
          id: "stale-cb",
          from: { id: 7, is_bot: false },
          message: {
            message_id: 99,
            chat: { id: 100, type: "private" },
          },
          data: "reroute:1:99",
        },
      },
      { cwd: "/repo" },
    );
    assert.equal(events.includes("status-menu"), false);
    assert.equal(events.includes("answer:Thread is not active yet."), true);
  });
});

test("Thread choosers use acknowledged labels without granting liveness or changing captured numeric targets", async () => {
  await withTopicStore(async (threadStore) => {
    for (const [threadId, threadName, instanceId] of [[7, "Anchor", "leader-a"], [8, "Briar", "offline"]] as const) {
      threadStore.upsert({ profileKey: `instance:${instanceId}`, instanceId,
        target: { chatId: 100, threadId }, threadName, slot: threadId === 7 ? "A" : "B",
        status: "active", createdAtMs: 1, updatedAtMs: 1 });
    }
    await threadStore.persist();
    let title = "repo_a";
    const { events, routeRuntime } = createRouteHarness({
      threadStore, getLiveThreadTargets: () => [{ chatId: 100, threadId: 7 }],
      getDisplayTitle: ({ threadId }) => threadId === 7 ? title : "offline_b",
    });
    await routeRuntime.handleUpdate({ message: {
      message_id: 12, chat: { id: 100, type: "private" },
      from: { id: 7, is_bot: false }, text: "/status",
    } }, { cwd: "/repo" });
    await routeRuntime.handleUpdate(unboundTopicUpdate(), { cwd: "/repo" });
    const roots = events.filter((event) => event.startsWith("markup:"));
    assert.equal(roots.length, 2);
    const markups = [await openRerouteSubmenu(routeRuntime, events), await openRerouteSubmenu(routeRuntime, events, "2")];
    for (const markup of markups) {
      assert.match(markup, /"text":"↪️ repo_a"/);
      assert.doesNotMatch(markup, /Anchor|Briar|offline_b/);
    }
    title = "A";
    await routeRuntime.handleUpdate({ callback_query: {
      id: "display-restore", from: { id: 7, is_bot: false },
      message: { message_id: 99, message_thread_id: 42, chat: { id: 100, type: "private" } },
      data: "rerouterestore:2",
    } }, { cwd: "/repo" });
    const restore = events.filter((event) => event.startsWith("markup:")).at(-1);
    assert.match(restore ?? "", /"text":"➡️ A"/);
    assert.match(restore ?? "", /reroutenew:2:7/);
    await routeRuntime.handleUpdate({ callback_query: {
      id: "display-route", from: { id: 7, is_bot: false },
      message: { message_id: 99, chat: { id: 100, type: "private" } },
      data: "reroute:1:7",
    } }, { cwd: "/repo" });
    assert.equal(events.includes("status-menu"), true);
    assert.equal(threadStore.getByProfileKey("instance:leader-a")?.threadName, "Anchor");
    assert.deepEqual(threadStore.getByProfileKey("instance:leader-a")?.target, { chatId: 100, threadId: 7 });
  });
});

test("Routing runtime treats extension and prompt-template commands as All chooser commands", async () => {
  await withTopicStore(async (threadStore) => {
    threadStore.upsert({
      profileKey: "cwd:/repo",
      target: { chatId: 100, threadId: 42 },
      status: "active",
      createdAtMs: 1000,
      updatedAtMs: 1000,
      instanceId: "leader-a",
      slot: "A",
      threadName: "Axial",
    });
    await threadStore.persist();
    const dispose = Commands.registerTelegramCommand({
      name: "review",
      handler: async () => undefined,
    });
    try {
      const { events, routeRuntime, telegramQueueStore } = createRouteHarness({
        threadStore,
        getCommands: () => [
          {
            name: "fix-tests",
            source: "prompt",
            sourceInfo: { path: "/tmp/fix-tests.md" },
          },
        ],
      });

      for (const [index, text] of ["/review", "/fix_tests"].entries()) {
        await routeRuntime.handleUpdate(
          {
            message: {
              message_id: 20 + index,
              chat: { id: 100, type: "private" },
              from: { id: 7, is_bot: false },
              text,
            },
          },
          { cwd: "/repo" },
        );
      }

      assert.deepEqual(telegramQueueStore.getQueuedItems(), []);
      const chooserMessages = events.filter((event) =>
        event.startsWith("interactive:html:"),
      );
      assert.equal(chooserMessages.length, 2);
      assert.match(chooserMessages[0] ?? "", /You used <code>\/review<\/code>/);
      assert.match(
        chooserMessages[1] ?? "",
        /You used <code>\/fix_tests<\/code>/,
      );
      const roots = events.filter((event) => event.startsWith("markup:"));
      assert.match(roots[0]!, /reroutemenu:1/);
      assert.match(roots[1]!, /reroutemenu:2/);
      const markups = [await openRerouteSubmenu(routeRuntime, events), await openRerouteSubmenu(routeRuntime, events, "2")];
      assert.match(markups[0] ?? "", /reroute:1:42/);
      assert.doesNotMatch(markups[0] ?? "", /rerouterestore:1/);
      assert.match(markups[1] ?? "", /reroute:2:42/);
      assert.doesNotMatch(markups[1] ?? "", /rerouterestore:2/);
    } finally {
      dispose();
    }
  });
});

test("Routing runtime keeps extension command replies in the invoking thread", async () => {
  await withTopicStore(async (threadStore) => {
    threadStore.upsert({
      profileKey: "cwd:/repo",
      target: { chatId: 100, threadId: 42 },
      status: "active",
      createdAtMs: 1000,
      updatedAtMs: 1000,
      instanceId: "leader-a",
      slot: "A",
    });
    await threadStore.persist();
    const successDispose = Commands.registerTelegramCommand({
      name: "pingx",
      handler: async ({ reply }) => reply("pong"),
    });
    const failureDispose = Commands.registerTelegramCommand({
      name: "failx",
      handler: async () => {
        throw new Error("boom");
      },
    });
    try {
      const { events, routeRuntime } = createRouteHarness({
        threadStore,
      });

      for (const [index, text] of ["/pingx", "/failx"].entries()) {
        await routeRuntime.handleUpdate(
          {
            message: {
              message_id: 30 + index,
              chat: { id: 100, type: "private" },
              from: { id: 7, is_bot: false },
              message_thread_id: 42,
              text,
            },
          },
          { cwd: "/repo" },
        );
      }

      assert.equal(events.includes("reply:pong"), true);
      assert.equal(events.includes("reply:Command failed."), true);
      assert.equal(
        events.filter((event) => event === "reply-target:100:42").length,
        2,
      );
    } finally {
      successDispose();
      failureDispose();
    }
  });
});

test("Routing runtime opens selected All menu command in the target thread", async () => {
  await withTopicStore(async (threadStore) => {
    threadStore.upsert({
      profileKey: "cwd:/repo",
      target: { chatId: 100, threadId: 42 },
      status: "active",
      createdAtMs: 1000,
      updatedAtMs: 1000,
      instanceId: "leader-a",
      slot: "A",
      threadName: "Axial",
    });
    await threadStore.persist();
    const apiCalls: Array<{ method: string; body: Record<string, unknown> }> = [];
    const { events, routeRuntime } = createRouteHarness({
      threadStore,
      callApi: async (method, body) => {
        apiCalls.push({ method, body });
        return {} as never;
      },
    });

    await routeRuntime.handleUpdate(
      {
        message: {
          message_id: 12,
          message_thread_id: 55,
          chat: { id: 100, type: "private" },
          from: { id: 7, is_bot: false },
          text: "/start",
        },
      },
      { cwd: "/repo" },
    );
    await routeRuntime.handleUpdate(
      {
        callback_query: {
          id: "cb1",
          from: { id: 7, is_bot: false },
          message: {
            message_id: 99,
            message_thread_id: 55,
            chat: { id: 100, type: "private" },
          },
          data: "reroute:1:42",
        },
      },
      { cwd: "/repo" },
    );

    assert.equal(events.includes("status-menu"), true);
    assert.equal(events.includes("delete-message:100:99"), true);
    assert.equal(
      apiCalls.some(
        (call) =>
          call.method === "deleteForumTopic" &&
          call.body.message_thread_id === 55,
      ),
      true,
    );
  });
});

test("Threadless chooser Restore fails closed rather than dispatching to the old leader", async () => {
  await withTopicStore(async (threadStore) => {
    threadStore.upsert({ profileKey: "cwd:/repo", instanceId: "leader-a", target: { chatId: 100, threadId: 42 }, status: "active", createdAtMs: 1000, updatedAtMs: 1000 });
    await threadStore.persist();
    const { routeRuntime, events } = createRouteHarness({ threadStore });
    await routeRuntime.handleUpdate({ message: { message_id: 12, chat: { id: 100, type: "private" }, from: { id: 7, is_bot: false }, text: "/start" } }, { cwd: "/repo" });
    await routeRuntime.handleUpdate({ callback_query: { id: "restore-classic", from: { id: 7, is_bot: false }, message: { message_id: 99, date: 0, chat: { id: 100, type: "private" } }, data: "reroutenew:1:42" } }, { cwd: "/repo" });
    assert.equal(events.includes("status-menu"), false);
    assert.equal(events.includes("delete-message:100:99"), false);
    assert.equal(threadStore.getByProfileKey("cwd:/repo")?.target.threadId, 42);
    assert.match(events.join("\n"), /Restore needs a destination thread/);
  });
});

test("Unnegotiated Restore refuses before selection and keeps the chooser's other routes", async () => {
  await withTopicStore(async (threadStore) => {
    threadStore.upsert({ profileKey: "cwd:/repo", owner: { kind: "leader", instanceId: "leader-a", cwd: "/repo" }, instanceId: "leader-a",
      target: { chatId: 100, threadId: 7 }, status: "active", slot: "A", createdAtMs: 1, updatedAtMs: 1 });
    await threadStore.persist();
    const calls: string[] = [];
    const untouched = () => assert.fail("Unnegotiated Restore must not change Restore evidence or reach a recipient");
    // Protection may still read retained operations; every transition is forbidden.
    const readOnlyStore = new Proxy({}, { get: (_target, name) =>
      name === "list" || name === "listTemporaryThreads" ? () => [] : untouched }) as Threads.TelegramWorkspaceRestore;
    const { routeRuntime, telegramQueueStore, events } = createRouteHarness({ threadStore, hasWorkspaceRestoreAuthority: () => false,
      getWorkspaceRestoreStore: () => readOnlyStore,
      workspaceRestoreRecipient: { getSessionId: untouched, getCwd: untouched, getLeaderIdentity: untouched,
        followerRegistry: Bus.createTelegramBusFollowerRegistry(), runFollower: untouched as never },
      callApi: async (method) => { calls.push(method); return true as never; } });
    await routeRuntime.handleUpdate(unboundTopicUpdate(), { cwd: "/repo" });
    const callback = (id: string, data: string): TestCallbackQuery => ({ id, from: { id: 7, is_bot: false },
      message: { message_id: 99, date: 0, chat: { id: 100, type: "private" } }, data });
    await routeRuntime.handleUpdate({ callback_query: callback("restore", "reroutenew:1:7") }, { cwd: "/repo" });
    assert.ok(events.includes("answer:🚫 Thread restore is unavailable right now. The message stays pending; choose another route or cancel."), events.join("\n"));
    assert.equal(telegramQueueStore.getQueuedItems().length, 0, "a refused Restore dispatches nothing");
    assert.deepEqual(calls.filter(method => method.includes("ForumTopic")), [], "a refused Restore performs no Thread API effect");
    await routeRuntime.handleUpdate({ callback_query: callback("route", "reroute:1:7") }, { cwd: "/repo" });
    assert.equal(telegramQueueStore.getQueuedItems().length, 1, "the refused Restore did not lock the chooser's ordinary route");
  });
});

test("Routing completes already-deleted chooser cleanup without redispatch", async () => {
  const originalFetch = globalThis.fetch;
  let deletes = 0;
  globalThis.fetch = async () => {
    deletes++;
    return new Response(JSON.stringify({ ok: false, description: "Bad Request: message to delete not found" }), { status: 400 });
  };
  try {
    await withTopicStore(async (threadStore) => {
      threadStore.upsert({ profileKey: "cwd:/repo", instanceId: "leader-a", target: { chatId: 100, threadId: 7 }, status: "active", createdAtMs: 1, updatedAtMs: 1 });
      await threadStore.persist();
      const api = createDefaultTelegramBridgeApiRuntime({ getBotToken: () => "test-token", recordRuntimeEvent: () => {} });
      const { routeRuntime, events, telegramQueueStore } = createRouteHarness({ threadStore, deleteMessage: api.deleteMessage, callApi: async () => true as never });
      await routeRuntime.handleUpdate(unboundTopicUpdate(), { cwd: "/repo" });
      for (const id of ["first", "retry"]) {
        await routeRuntime.handleUpdate({ callback_query: { id, from: { id: 7, is_bot: false }, message: { message_id: 99, date: 0, chat: { id: 100, type: "private" } }, data: "reroute:1:7" } }, { cwd: "/repo" });
      }
      assert.equal(deletes, 1);
      assert.equal(telegramQueueStore.getQueuedItems().length, 1);
      assert.equal(events.includes("answer:⌛ Routing choice expired."), true);
      assert.equal(events.some((event) => event.includes("Chooser cleanup is still pending")), false);
    });
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("Routing runtime retries stale-epoch and chooser cleanup without redispatch", async () => {
  await withTopicStore(async (threadStore) => {
    const apiCalls: unknown[] = [];
    let epochReads = 0;
    let chooserDeleteAttempts = 0;
    threadStore.upsert({
      profileKey: "cwd:/repo",
      target: { chatId: 100, threadId: 7 },
      status: "active",
      createdAtMs: 1000,
      updatedAtMs: 1000,
      instanceId: "leader-a",
      slot: "A",
    });
    await threadStore.persist();
    const { events, routeRuntime, telegramQueueStore } = createRouteHarness({
      threadStore,
      getCurrentLeaderEpoch: () => {
        epochReads += 1;
        return epochReads === 1 ? 1 : 2;
      },
      callApi: async (method, body) => {
        apiCalls.push({ method, body });
        return {} as never;
      },
      deleteMessage: async () => {
        chooserDeleteAttempts += 1;
        if (chooserDeleteAttempts === 1) {
          throw new Error("temporary chooser deletion failure");
        }
      },
    });

    await routeRuntime.handleUpdate(unboundTopicUpdate(), { cwd: "/repo" });

    assert.equal(threadStore.getByProfileKey("topic:100:42"), undefined);
    assert.deepEqual(apiCalls, [
      {
        method: "sendChatAction",
        body: { chat_id: 100, message_thread_id: 7, action: "typing" },
      },
    ]);
    assert.deepEqual(telegramQueueStore.getQueuedItems(), []);
    const chooser = events.find((event) => event.startsWith("interactive:"));
    assert.match(chooser ?? "", /Choose where to send your message/);
    assert.match(chooser ?? "", /Select the Pi thread that should handle it, or restore a Pi into this tab:/);
    const root = events.find((event) => event.startsWith("markup:"));
    assert.match(root ?? "", /"callback_data":"reroutemenu:1"/);
    assert.match(root ?? "", /"callback_data":"rerouterestore:1"/);
    const markup = await openRerouteSubmenu(routeRuntime, events);
    assert.match(markup, /"callback_data":"reroute:1:7"/);

    await routeRuntime.handleUpdate(
      {
        callback_query: {
          id: "reroute-cb",
          from: { id: 7, is_bot: false },
          message: {
            message_id: 99,
            message_thread_id: 42,
            chat: { id: 100, type: "private" },
          },
          data: "reroute:1:7",
        },
      },
      { cwd: "/repo" },
    );

    const record = threadStore.getByProfileKey("cwd:/repo");
    assert.deepEqual(record?.target, { chatId: 100, threadId: 7 });
    const queued = telegramQueueStore.getQueuedItems()[0];
    assert.equal(queued?.kind, "prompt");
    assert.deepEqual(queued?.target, { chatId: 100, threadId: 7 });
    assert.equal(
      queued?.kind === "prompt" && queued.content[0]?.type === "text"
        ? queued.content[0].text
        : "",
      "[telegram|thread:Anchor] hello",
    );
    assert.equal(events.includes("delete-message:100:99"), false);
    assert.equal(
      events.includes(
        "answer:Message routed, but thread cleanup is still pending. Try again.",
      ),
      true,
    );

    await routeRuntime.handleUpdate(
      {
        callback_query: {
          id: "reroute-cleanup-retry",
          from: { id: 7, is_bot: false },
          message: {
            message_id: 99,
            message_thread_id: 42,
            chat: { id: 100, type: "private" },
          },
          data: "reroute:1:7",
        },
      },
      { cwd: "/repo" },
    );

    assert.equal(telegramQueueStore.getQueuedItems().length, 1);
    assert.equal(chooserDeleteAttempts, 1);
    assert.match(events.join("\n"), /Chooser cleanup is still pending/);

    await routeRuntime.handleUpdate(
      {
        callback_query: {
          id: "reroute-chooser-cleanup-retry",
          from: { id: 7, is_bot: false },
          message: {
            message_id: 99,
            message_thread_id: 42,
            chat: { id: 100, type: "private" },
          },
          data: "reroute:1:7",
        },
      },
      { cwd: "/repo" },
    );

    assert.equal(telegramQueueStore.getQueuedItems().length, 1);
    assert.equal(chooserDeleteAttempts, 2);
    assert.equal(events.includes("answer:Thread cleanup completed."), true);
    assert.deepEqual(apiCalls, [
      {
        method: "sendChatAction",
        body: { chat_id: 100, message_thread_id: 7, action: "typing" },
      },
      {
        method: "closeForumTopic",
        body: { chat_id: 100, message_thread_id: 42 },
      },
      {
        method: "deleteForumTopic",
        body: { chat_id: 100, message_thread_id: 42 },
      },
    ]);
  });
});

test("Routing runtime answers expired reroute callbacks without queueing", async () => {
  const { events, routeRuntime, telegramQueueStore } = createRouteHarness({
  });

  await routeRuntime.handleUpdate(
    {
      callback_query: {
        id: "reroute-expired",
        from: { id: 7, is_bot: false },
        message: {
          message_id: 99,
          message_thread_id: 42,
          chat: { id: 100, type: "private" },
        },
        data: "reroute:missing:7",
      },
    },
    { cwd: "/repo" },
  );

  assert.equal(events.includes("answer:⌛ Routing choice expired."), true);
  assert.deepEqual(telegramQueueStore.getQueuedItems(), []);
});

test("Routing runtime answers stale reroute target callbacks gracefully", async () => {
  await withTopicStore(async (threadStore) => {
    threadStore.upsert({
      profileKey: "cwd:/repo",
      target: { chatId: 100, threadId: 7 },
      status: "active",
      createdAtMs: 1000,
      updatedAtMs: 1000,
      instanceId: "leader-a",
      slot: "A",
    });
    await threadStore.persist();
    const { events, routeRuntime, telegramQueueStore } = createRouteHarness({
      threadStore,
      callApi: async () => ({}) as never,
    });

    await routeRuntime.handleUpdate(unboundTopicUpdate(), { cwd: "/repo" });
    threadStore.markStaleByTarget(
      { chatId: 100, threadId: 7 },
      "deleted",
      "target deleted before callback",
    );
    await threadStore.persist();

    await routeRuntime.handleUpdate(
      {
        callback_query: {
          id: "reroute-stale-target",
          from: { id: 7, is_bot: false },
          message: {
            message_id: 99,
            message_thread_id: 42,
            chat: { id: 100, type: "private" },
          },
          data: "reroute:1:7",
        },
      },
      { cwd: "/repo" },
    );

    assert.equal(events.includes("answer:Thread is not active yet."), true);
    assert.deepEqual(telegramQueueStore.getQueuedItems(), []);
  });
});

test("Routing runtime retries only failed foreign media-group messages", async () => {
  await withTopicStore(async (threadStore) => {
    const forwardedMessages: TestMessage[] = [];
    const apiCalls: unknown[] = [];
    let photoBFailed = false;
    threadStore.upsert({
      profileKey: "cwd:/repo",
      owner: { kind: "leader", cwd: "/repo", instanceId: "leader-a" },
      target: { chatId: 100, threadId: 7 },
      status: "active",
      createdAtMs: 1000,
      updatedAtMs: 1000,
      instanceId: "leader-a",
      slot: "A",
      rerouteConfirmedAtMs: 1500,
    });
    threadStore.upsert({
      profileKey: "follower:beta",
      owner: { kind: "manual-follower", instanceId: "follower-b" },
      target: { chatId: 100, threadId: 8 },
      status: "active",
      createdAtMs: 1000,
      updatedAtMs: 1000,
      instanceId: "follower-b",
      slot: "B",
      threadName: "Beta",
    });
    await threadStore.persist();
    const { events, routeRuntime, telegramQueueStore } = createRouteHarness({
      threadStore,
      callApi: async (method, body) => {
        apiCalls.push({ method, body });
        return {} as never;
      },
      getLiveThreadTargets: () => [
        { chatId: 100, threadId: 7 },
        { chatId: 100, threadId: 8 },
      ],
      getTargetOwnership: target => target.threadId === 8 ? rerouteFollowerOwnership("follower-b") : undefined,
      foreignOwnedUpdateForwarder: {
        forwardMessage: ({ message }) => {
          forwardedMessages.push(message);
          if (message.photo?.[0]?.file_id === "photo-b" && !photoBFailed) {
            photoBFailed = true;
            return retryableForeignUpdateSettlement();
          }
          return acceptedForeignUpdateSettlement();
        },
      },
    });

    await routeRuntime.handleUpdate(
      {
        message: {
          message_id: 11,
          message_thread_id: 42,
          media_group_id: "album-1",
          chat: { id: 100, type: "private" },
          from: { id: 7, is_bot: false },
          photo: [
            {
              file_id: "photo-a",
              file_unique_id: "photo-a",
              width: 320,
              height: 240,
            },
          ],
          caption: "first",
        },
      },
      { cwd: "/repo" },
    );
    await routeRuntime.handleUpdate(
      {
        message: {
          message_id: 12,
          message_thread_id: 42,
          media_group_id: "album-1",
          chat: { id: 100, type: "private" },
          from: { id: 7, is_bot: false },
          photo: [
            {
              file_id: "photo-b",
              file_unique_id: "photo-b",
              width: 320,
              height: 240,
            },
          ],
        },
      },
      { cwd: "/repo" },
    );
    await new Promise((resolve) => setTimeout(resolve, 1250));

    const root = events.find((event) => event.startsWith("markup:"));
    assert.match(root ?? "", /"callback_data":"reroutemenu:1"/);
    const markup = await openRerouteSubmenu(routeRuntime, events);
    assert.match(markup, /"callback_data":"reroute:1:8"/);
    await routeRuntime.handleUpdate(
      {
        callback_query: {
          id: "reroute-cb",
          from: { id: 7, is_bot: false },
          message: {
            message_id: 99,
            message_thread_id: 42,
            chat: { id: 100, type: "private" },
          },
          data: "reroute:1:8",
        },
      },
      { cwd: "/repo" },
    );

    assert.equal(telegramQueueStore.getQueuedItems().length, 0);
    assert.equal(events.includes("delete-message:100:99"), false);
    assert.match(
      events.join("\n"),
      /retrying will send only remaining messages/,
    );

    await routeRuntime.handleUpdate(
      {
        callback_query: {
          id: "reroute-retry-cb",
          from: { id: 7, is_bot: false },
          message: {
            message_id: 99,
            message_thread_id: 42,
            chat: { id: 100, type: "private" },
          },
          data: "reroute:1:8",
        },
      },
      { cwd: "/repo" },
    );

    assert.deepEqual(
      forwardedMessages.map((message) => ({
        messageId: message.message_id,
        threadId: message.message_thread_id,
        photoId: message.photo?.[0]?.file_id,
      })),
      [
        { messageId: 0, threadId: 8, photoId: "photo-a" },
        { messageId: 0, threadId: 8, photoId: "photo-b" },
        { messageId: 0, threadId: 8, photoId: "photo-b" },
      ],
    );
    assert.equal(events.includes("delete-message:100:99"), true);
    assert.deepEqual(apiCalls.at(-2), {
      method: "closeForumTopic",
      body: { chat_id: 100, message_thread_id: 42 },
    });
    assert.deepEqual(apiCalls.at(-1), {
      method: "deleteForumTopic",
      body: { chat_id: 100, message_thread_id: 42 },
    });
  });
});

test("Routing runtime routes reroute source to confirmed current leader thread", async () => {
  await withTopicStore(async (threadStore) => {
    const apiCalls: unknown[] = [];
    threadStore.upsert({
      profileKey: "cwd:/repo",
      owner: { kind: "leader", cwd: "/repo", instanceId: "leader-a" },
      target: { chatId: 100, threadId: 7 },
      status: "active",
      createdAtMs: 1000,
      updatedAtMs: 1000,
      instanceId: "leader-a",
      slot: "A",
      lastReconcileAction: "leader-startup-probe",
      rerouteConfirmedAtMs: 1500,
    });
    await threadStore.persist();
    const { events, routeRuntime, telegramQueueStore } = createRouteHarness({
      threadStore,
      callApi: async (method, body) => {
        apiCalls.push({ method, body });
        return {} as never;
      },
    });

    await routeRuntime.handleUpdate(unboundTopicUpdate(), { cwd: "/repo" });
    const root = events.find((event) => event.startsWith("markup:"));
    assert.match(root ?? "", /"callback_data":"rerouterestore:1"/);
    assert.match(root ?? "", /"text":"🔁 Restore…"/);
    assert.match(root ?? "", /"callback_data":"reroutemenu:1"/);
    const markup = await openRerouteSubmenu(routeRuntime, events);
    assert.match(markup, /"callback_data":"reroute:1:7"/);
    assert.doesNotMatch(markup, /"callback_data":"reroutenew:1:7"/);
    await routeRuntime.handleUpdate(
      {
        callback_query: {
          id: "reroute-cb",
          from: { id: 7, is_bot: false },
          message: {
            message_id: 99,
            message_thread_id: 42,
            chat: { id: 100, type: "private" },
          },
          data: "reroute:1:7",
        },
      },
      { cwd: "/repo" },
    );

    const record = threadStore.getByProfileKey("cwd:/repo");
    assert.deepEqual(record?.target, { chatId: 100, threadId: 7 });
    const queued = telegramQueueStore.getQueuedItems()[0];
    assert.equal(queued?.kind, "prompt");
    assert.deepEqual(queued?.target, { chatId: 100, threadId: 7 });
    assert.deepEqual(apiCalls, [
      {
        method: "sendChatAction",
        body: { chat_id: 100, message_thread_id: 7, action: "typing" },
      },
      {
        method: "closeForumTopic",
        body: { chat_id: 100, message_thread_id: 42 },
      },
      {
        method: "deleteForumTopic",
        body: { chat_id: 100, message_thread_id: 42 },
      },
    ]);
  });
});

for (const sourceKind of ["forwarded", "completed", "queued"] as const) {
const role = sourceKind === "forwarded" ? "follower" : "leader";
for (const scenario of ["completion", "partial", "missing", "foreign", "unreadable", "no-reader", "wrong-binding",
  "authority-ended", "late-read-missing", "recipient-changed", "cleanup-unknown", "cleanup-recipient-changed", "already-issued",
  "publication-before", "publication-after", ...(role === "leader" ? ["same-instance", "foreign-session", "other-cwd", "old-target", "owner-missing", "owner-scope", "canonical-changed"] as const : ["heartbeat"] as const),
  ...(sourceKind === "queued" ? ["admission-only", "no-admission", "multi-receipt"] as const : [])] as const) {
  test(`Cold Restore scoped ACK continuation never replays source or recipient effects (${role}, ${scenario}, ${sourceKind})`, { skip: strictJournalUnsupported }, async () => {
    await fixture(async ({ store, threads, request, path, auth }) => {
      const config = createTelegramConfigStore({ agentDir: dirname(path) });
      const queueIdentity = { instanceId: "old", processId: process.pid, processBirthId: `${process.pid}:queued-recovery`, sessionGeneration: 1 };
      const resolve = Journal.createTelegramUpdateJournalRuntimeBindingResolver({ getProfileName: () => undefined, getBotToken: () => "fixture",
        getBotId: () => undefined, getJournalPath: () => `${path}.source`, getQueueRuntimeIdentity: () => queueIdentity,
        withSourceSerialization: config.withSourceSerialization });
      const source = resolve()!, journal = source.journal;
      const scopedRequest = { ...request, source: { ...request.source, journalBindingKey: source.recoveryKey } };
      journal.appendBatch([100, 101].map(update_id => ({ update_id, message: { message_id: update_id,
        message_thread_id: request.target.threadId, chat: { id: 7, type: "private" },
        text: sourceKind === "completed" ? update_id === 100 ? "/start" : "/compact" : "Accepted source fixture" } })));
      const queueProofs: Journal.TelegramUpdateJournalQueuedReceiptEvidence[] = [];
      if (sourceKind === "queued") for (const sourceUpdateIds of scenario === "multi-receipt" ? [[100], [101]] : [[100, 101]]) {
        const receiptId = `cold-receipt-${sourceUpdateIds[0]}`;
        journal.markQueued({ receiptId, queueKind: "prompt", sourceUpdateIds, owner: queueIdentity });
        const proof = journal.inspectQueuedReceipt({ receiptId, queueKind: "prompt", sourceUpdateIds,
          queueOwner: journal.read().entries.find(value => value.updateId === sourceUpdateIds[0])!.queueOwner! });
        assert.ok(proof); queueProofs.push(proof);
      }
      const queueProofFor = (id: number) => queueProofs.find(value => value.receipt.sourceUpdateIds.includes(id))!;
      const digests = journal.read().entries.map(Journal.createTelegramUpdateJournalEntryDigest);
      // Seed post-disposal authority; producer execution, startup and work clearance remain separate preconditions.
      const originalRecipient = { ...recipient(role), ...(role === "leader" ? { generation: "1" } : {}) };
      const originalAuth = role === "leader" ? { ...auth, executor: { instanceId: "old", leaderEpoch: "epoch" } } : auth;
      let intent = store.issueRouting(store.confirmReady(store.issueRecipient((await store.commit(scopedRequest, originalAuth))!,
        originalRecipient, originalAuth)!.intent, originalRecipient, originalAuth)!, originalAuth)!.intent;
      for (const digest of digests) intent = store.recordSourceAcceptance(intent, { ...digest, journalBindingKey: source.recoveryKey,
        recipient: originalRecipient, ...(sourceKind === "queued" ? { kind: "queued" as const, receiptId: queueProofFor(digest.updateId).receipt.receiptId,
          queueKind: queueProofFor(digest.updateId).receipt.queueKind, queueOwnerSha256: queueProofFor(digest.updateId).queueOwnerSha256 } : role === "leader" ? { kind: "completed" as const } : { kind: "forwarded" as const, recipientBindingKey: "manual:old",
        deliveryId: Bus.createTelegramBusFollowerDeliveryIdentity({ kind: "leader.forwardMessage", recipientBindingKey: "manual:old",
          sourceUpdateId: digest.updateId }).deliveryId }) }, originalAuth)!;
      const markers = intent.routing!.acceptances!.map(acceptance => ({ updateId: acceptance.updateId, sourceSha256: acceptance.sourceSha256,
        completionSha256: Threads.getTelegramWorkspaceRestoreSourceCompletionSha256(intent, acceptance) }));
      if (sourceKind === "queued" && scenario !== "no-admission") {
        for (const proof of queueProofs) intent = store.recordSourceSettlement(intent, { journalBindingKey: source.recoveryKey,
          updateIds: [...proof.receipt.sourceUpdateIds], kind: "queued", receiptId: proof.receipt.receiptId, queueKind: proof.receipt.queueKind }, originalAuth)!;
        assert.equal(store.issueCleanup(intent, originalAuth), undefined, "queue admission never grants cleanup");
      }
      if (scenario !== "missing" && scenario !== "admission-only") {
        const selected = (scenario === "partial" ? markers.slice(0, 1) : markers).map(value => scenario === "foreign"
          ? { ...value, completionSha256: createHash("sha256").update(`foreign:${value.updateId}`).digest("hex") } : value);
        if (sourceKind === "queued") journal.completeQueuedExact(queueProofs.map(value => value.receipt), selected);
        else journal.removeCompletedExact(selected.map(value => value.updateId), selected.map(({ updateId, sourceSha256 }) => ({ updateId, sourceSha256 })), selected);
      }
      if (scenario === "already-issued") {
        intent = store.recordSourceSettlement(intent, { ...scopedRequest.source, ...(sourceKind === "queued"
          ? { kind: "queue-completed" as const, receiptId: queueProofs[0]!.receipt.receiptId, queueKind: queueProofs[0]!.receipt.queueKind }
          : { kind: "completed" as const }) }, originalAuth)!;
        intent = store.issueCleanup(intent, originalAuth)!.intent;
      }
      if (scenario === "unreadable") writeFileSync(`${path}.source`, "{broken", { mode: 0o600 });
      const sourceBytes = readFileSync(`${path}.source`, "utf8");
      const currentInstance = scenario === "same-instance" ? "old" : "leader-next";
      if (role === "leader" && scenario !== "owner-missing") {
        // Actual leader startup remains a precondition: the same-session successor already owns the relocated record.
        threads.upsert({ ...threads.list()[0]!, instanceId: currentInstance,
          owner: { kind: "leader", cwd: "/repo", instanceId: scenario === "owner-scope" ? "foreign" : currentInstance } });
        await threads.persist();
      }
      const canonicalBefore = await readFile(path, "utf8");
      // A fresh leader and cold store; actual successor registration/startup publication remains a fixture precondition.
      const coldThreads = Threads.createTelegramTopicTargetStore({ path }); await coldThreads.load();
      const cold = coldThreads.workspaceRestore({ profileName: "default", tokenSha256: "a".repeat(64) });
      let current = true, queryCount = 0, requestCount = 0;
      let localTarget = scenario === "old-target" ? request.binding.target : request.target;
      let publicationFault = scenario === "publication-before" || scenario === "publication-after";
      const terminalPublications: Threads.TelegramWorkspaceRestoreSourceSettlement[] = [];
      const ctx: TestContext = { cwd: "/repo" };
      const calls: string[] = [];
      const protocol = Bus.createTelegramBusProtocolIdentity({ runtimeBuild: "fixture",
        capabilities: [Bus.TELEGRAM_BUS_CAPABILITY_WORKSPACE_RESTORE, Bus.TELEGRAM_BUS_CAPABILITY_DURABLE_FOLLOWER_ADMISSION] });
      const ledger = createTelegramWorkspaceAdmissionLedger({ path: `${path}.admission`, profileKey: "profile:restore",
        owner: { processId: process.pid, processBirthId: `${process.pid}:ack-recovery` }, getProcessLiveness: () => "alive" });
      const operations = createTelegramWorkspaceOperationRuntime({ getWorkspaceAdmission: () => ledger });
      const socketPath = Bus.getTelegramBusFollowerSocketPath("old", dirname(path));
      const registry = Bus.createTelegramBusFollowerRegistry();
      registry.register({ instanceId: "old", sessionId: "session", cwd: "/repo", slot: "A", target: request.target,
        registrationGeneration: "fresh", protocol, busSocketPath: socketPath, profileKey: "manual:old", connectedAtMs: 1 });
      const registration = createTelegramBusFollowerRegistrationState();
      registration.setRegistered(true, request.target, { slot: "A", generation: "fresh", leaderProtocol: protocol });
      const readonlyThreads = Threads.createTelegramTopicTargetStore({ path, canPersist: () => false });
      const readonlyRestore = readonlyThreads.workspaceRestore({ profileName: "default", tokenSha256: "a".repeat(64) });
      const handler = createTelegramBusFollowerWorkspaceRestoreHandler({ instanceId: "old", registrationState: registration,
        topicTargetStore: readonlyThreads, getWorkspaceAdmission: () => ledger,
        readRestoreIntent: id => readonlyRestore.list().find(value => value.request.operationId === id),
        getContextAuthority: () => current ? { profileBindingKey: "profile:restore", operatorUserId: 7,
          executor: { instanceId: "leader-next", leaderEpoch: "next" }, sessionId: "session", cwd: "/repo", generation: 1,
          leaderProtocol: protocol } : undefined });
      const receiver = createTelegramBusForwardedUpdateReceiverRuntime({ socketPath, instanceId: "old", getAuthSecret: () => "ack-secret",
        getRegistrationGeneration: registration.getGeneration, getRecipientBindingKey: () => "manual:old", getContext: () => ctx,
        isWorkspaceRestoreEnabled: () => true, async handleWorkspaceRestore(input) {
          assert.equal(input.mode, "inspect"); calls.push(input.mode);
          const result = await handler(input, ctx);
          if (scenario === "recipient-changed") registry.register({ ...registry.get("old")!, registrationGeneration: "changed" });
          return result;
        }, durableAdmission: { async admit() { assert.fail("ACK recovery cannot forward or execute input"); } } });
      const runFollower = Bus.createTelegramBusWorkspaceRestoreController({ getFollower: registry.get, localProtocolIdentity: protocol,
        getAuthSecret: () => "ack-secret", createRequestId: () => `ack-${++requestCount}` });
      const references = Journal.createTelegramUpdateJournalReferenceRegistry();
      const { routeRuntime, events } = createRouteHarness({ threadStore: coldThreads, instanceId: currentInstance,
        getCurrentLeaderEpoch: () => "next", getSessionGeneration: () => 1, getAdmissionJournalBinding: () => source.recoveryKey,
        hasWorkspaceRestoreAuthority: () => current, runWorkspaceOperation: operations.run,
        getWorkspaceRestoreStore: () => ({ ...cold, confirmInspectedReady(expected, observed, authority) {
          if (role === "leader") calls.push("inspect");
          const result = cold.confirmInspectedReady(expected, observed, authority);
          if (role === "leader" && scenario === "recipient-changed") localTarget = request.binding.target;
          return result;
        }, recordSourceSettlement(expected, evidence, authority) {
          if (evidence.kind === "queue-completed") terminalPublications.push(structuredClone(evidence));
          return publicationFault ? coldThreads.workspaceRestore({ profileName: "default", tokenSha256: "a".repeat(64),
            onPublicationBoundary(point) {
              if (point === (scenario === "publication-before" ? "after-write-before-rename" : "after-rename")) {
                throw new Error("Fixture recovered settlement publication interrupted");
              }
            } }).recordSourceSettlement(expected, evidence, authority) : cold.recordSourceSettlement(expected, evidence, authority);
        } }),
        // Clearance is a separate fixture precondition; this suite proves ACK recovery, not recipient work clearance.
        captureWorkspaceExternalProtection: () => ({ liveOwner: "clear", acceptedWork: "clear", deliveryAuthority: "clear" }),
        inspectRestoreSourceCompletion: scenario === "no-reader" ? undefined : expected => {
          assert.ok(ledger.read().leases.length > 0, "every ACK query is inside fresh Workspace admission");
          queryCount++;
          return references.withReference({ referenceClass: "operator-disposition", recoveryKey: source.recoveryKey }, () => {
            const observed = resolve()!.journal.inspectSourceCompletion({ updateId: expected.updateId, sourceSha256: expected.sourceSha256,
              completionSha256: expected.completionSha256 });
            if (scenario === "authority-ended") current = false;
            if (scenario === "late-read-missing" && queryCount > markers.length) return undefined;
            return observed && { ...observed, journalBindingKey: scenario === "wrong-binding" ? "foreign" : source.recoveryKey };
          });
        },
        setCurrentLeaderIdentity() { assert.fail("Scoped ACK continuation cannot apply a local target"); },
        workspaceRestoreRecipient: { getSessionId: () => role === "leader" ? scenario === "foreign-session" ? "foreign" : "session" : "leader-session",
          getCwd: () => role === "leader" ? scenario === "other-cwd" ? "/other" : "/repo/" : "/leader",
          getLeaderIdentity: () => role === "leader" ? { slot: "A", target: localTarget } : undefined, followerRegistry: registry,
          runFollower: role === "leader" ? async () => { assert.fail("A local leader uses no follower IPC"); } : runFollower },
        async callApi<TResponse>(method: string) {
          calls.push(method);
          if (scenario === "cleanup-unknown") throw new Error("Fixture cleanup reply lost");
          if (scenario === "cleanup-recipient-changed") {
            if (role === "leader") localTarget = request.binding.target;
            else registry.register({ ...registry.get("old")!, registrationGeneration: "changed" });
          }
          if (scenario === "canonical-changed") {
            const snapshot = JSON.parse(readFileSync(path, "utf8"));
            snapshot.threads[0].instanceId = "foreign"; snapshot.threads[0].owner.instanceId = "foreign";
            writeFileSync(path, JSON.stringify(snapshot), { mode: 0o600 });
          }
          return true as TResponse;
        },
      });
      try {
        if (role === "follower") await receiver.start();
        if (scenario === "heartbeat") await routeRuntime.onWorkspaceRestoreRecipientObserved(registry.get("old")!, () => current, ctx);
        else { routeRuntime.onUpdateCompleted(999, ctx, source.recoveryKey); await routeRuntime.waitForRestoreSettlement(); }
        const retained = cold.list();
        const recovered = ["completion", "heartbeat", "same-instance", "partial", "cleanup-unknown", "cleanup-recipient-changed", "canonical-changed", "no-admission", "multi-receipt"].includes(scenario);
        if (recovered && scenario !== "partial" && scenario !== "cleanup-unknown" && scenario !== "cleanup-recipient-changed" && scenario !== "canonical-changed") assert.deepEqual(retained, []);
        else {
          assert.equal(retained.length, 1);
          assert.deepEqual(retained[0]!.request, intent.request);
          assert.deepEqual(retained[0]!.recipient, intent.recipient, "original grant stays immutable");
          assert.deepEqual(retained[0]!.routing?.acceptances, intent.routing?.acceptances);
          assert.deepEqual(retained[0]!.routing?.settlements.filter(value => value.kind !== "queued").flatMap(value => value.updateIds), scenario === "partial" ? [100]
            : ["cleanup-unknown", "cleanup-recipient-changed", "canonical-changed", "already-issued", "publication-after"].includes(scenario) ? [100, 101] : []);
          assert.equal(retained[0]!.routing?.cleanup, ["cleanup-unknown", "cleanup-recipient-changed", "canonical-changed", "already-issued"].includes(scenario) ? "issued" : undefined);
          if (sourceKind === "queued") assert.ok(retained[0]!.routing?.settlements.filter(value => value.kind !== "queued")
            .every(value => value.kind === "queue-completed"), "only receipt-terminal ACKs upgrade queued admission");
        }
        assert.equal(calls.filter(value => value === "inspect").length, scenario === "same-instance" ? 0
          : recovered || ["late-read-missing", "recipient-changed", "publication-before", "publication-after"].includes(scenario) ? 1 : 0,
          "an unchanged local ready recipient needs no readiness republication; follower recovery uses one inspect RPC");
        assert.equal(calls.includes("apply"), false);
        if (["missing", "foreign", "unreadable", "no-reader", "wrong-binding", "authority-ended", "already-issued", "foreign-session", "other-cwd", "old-target", "owner-missing", "owner-scope", "admission-only"].includes(scenario)) {
          assert.equal(await readFile(path, "utf8"), canonicalBefore, "unknown evidence cannot adopt or settle");
          assert.deepEqual(calls, []);
        }
        assert.equal(readFileSync(`${path}.source`, "utf8"), sourceBytes, "continuation inspects; it never retries removal or repairs the source");
        if (scenario === "multi-receipt") assert.deepEqual(terminalPublications.map(value => value.updateIds), [[100], [101]],
          "distinct receipts remain separate terminal evidence, never a fabricated combined receipt");
        assert.deepEqual(references.list(), []);
        assert.equal(coldThreads.listWorkspaceBindings()[0]!.slot, "A");
        assert.deepEqual(coldThreads.listWorkspaceBindings()[0]!.target, request.target);
        const count = calls.length;
        publicationFault = false;
        routeRuntime.onUpdateCompleted(999, ctx, source.recoveryKey); await routeRuntime.waitForRestoreSettlement();
        if (["cleanup-unknown", "cleanup-recipient-changed", "canonical-changed", "already-issued"].includes(scenario)) assert.equal(calls.length, count, "issued deletion never replays");
        if (scenario === "publication-before" || scenario === "publication-after") {
          assert.deepEqual(cold.list(), [], "a new hint reconciles retained ACKs after interrupted canonical publication, never disposal");
          assert.equal(calls.filter(value => value === "inspect").length, role === "leader" ? 1 : 2,
            "retained local readiness is checked read-only rather than republished");
          assert.equal(readFileSync(`${path}.source`, "utf8"), sourceBytes);
        }
        if (scenario === "foreign" || scenario === "unreadable" || scenario === "authority-ended") assert.ok(events.length > 0);
      } finally { if (role === "follower") await receiver.stop(); }
    }, role);
  });
}
}

const recipientWakeCases = ["recipient-wake", "recipient-cwd", "recipient-held", "recipient-stale", "recipient-unknown", "recipient-lost-source", "recipient-changed", "recipient-cleanup-unknown"];
const cleanupWakeCases = ["wake-normal", "wake-unknown", "wake-foreign", "wake-authority"];
const journalProtectionCases = ["journal-protected", "journal-unknown", "journal-missing", "journal-late", "journal-observation-error"];
const ownerBearingReferenceCases = ["session-reference-queued-group", "session-reference-shared-offer", "session-reference-shared-control"];
const sessionReferenceCases = ["session-reference-empty", "session-reference-pending", "session-reference-unreadable", "session-reference-missing", ...ownerBearingReferenceCases];
const leaderBoundaryFaults = ["leader-recovery", "leader-disk", "leader-owner", "leader-context"];
const warmCleanupCases = ["warm-cleanup-context", "warm-cleanup-generation", "warm-cleanup-epoch", "warm-cleanup-canonical"];
const warmDisposalCases = ["warm-disposal-before", "warm-readback-lost", "warm-readback-missing", "warm-readback-context", "warm-readback-canonical"];
const queuedTerminalMixedCases = ["queue-terminal-mixed", "queue-terminal-mixed-lost"];
const queuedTerminalCases = ["queue-terminal", "queue-terminal-group", "queue-terminal-lost", "queue-terminal-reader", "queue-terminal-publication", "queue-terminal-discard", ...queuedTerminalMixedCases];
const queuedGroupCases = ["queue-group", "queue-group-before", "queue-group-after", "queue-group-missing",
  "queue-group-source", "queue-group-offer", "queue-group-cold", "queue-terminal-group"];
const queuedProofCases = ["queue-proof", "queue-before-rename", "queue-after-rename", "queue-no-proof", "queue-read-missing",
  "queue-read-throws", "queue-authority", "queue-recipient", "queue-post-read-missing", "queue-post-read-source", "queue-post-read-recipient",
  "queue-post-admission-recipient", "queue-cold", ...queuedGroupCases, ...queuedTerminalCases.filter(value => !queuedGroupCases.includes(value))];
const forwardProofCases = ["acceptance-before-rename", "acceptance-after-rename", "acceptance-return-no-proof",
  "acceptance-source-changed", "source-removal-before-write", "source-removal-lost-ack", "source-cas-changed"];
for (const role of ["leader", "follower"] as const) {
  for (const lostReply of [false, true]) {
    for (const delivery of lostReply ? ["normal", "local-regression", "canonical-regression", ...(role === "leader" ? leaderBoundaryFaults : [])] : role === "follower"
      ? ["normal", "lost-ack", "registration-change", "cleanup-unknown", "authority-ended", "settlement-publication-interrupted", "session-change", "target-change", "queued-before", "queued-during", "active-before", "canonical-before-ack", "canonical-after-ready", "registry-recovery", "registry-disk-regression", ...journalProtectionCases, ...sessionReferenceCases, ...cleanupWakeCases, ...recipientWakeCases, ...forwardProofCases, ...warmCleanupCases, ...warmDisposalCases, "warm-cleanup-registration"]
      : ["normal", "cleanup-unknown", "authority-ended", "settlement-publication-interrupted", "session-change", "target-change", "queued-before", "queued-during", "active-before", "canonical-before-ack", "canonical-after-ready", ...leaderBoundaryFaults, ...journalProtectionCases, ...cleanupWakeCases, ...queuedProofCases, ...warmCleanupCases, ...warmDisposalCases]) {
    test(`Restore button uses durable relocation at full capacity (${role}, lost reply: ${lostReply}, delivery: ${delivery})`, {
      skip: strictJournalUnsupported,
    }, async () => {
      await withTopicStore(async (threadStore, path) => {
        const instanceId = role === "leader" ? "leader-a" : "follower-a";
        const oldTarget = { chatId: 7, threadId: 7 };
        const target = { chatId: 7, threadId: 42 };
        threadStore.upsert({ profileKey: role === "leader" ? "cwd:/repo" : "manual:owner",
          owner: role === "leader" ? { kind: "leader", cwd: "/repo", instanceId } : { kind: "manual-follower", instanceId: "owner" },
          instanceId, target: oldTarget, slot: "A", threadName: "Coral", status: "active", createdAtMs: 1, updatedAtMs: 1 });
        const binding: Threads.TelegramWorkspaceThreadBinding = { ...Threads.createTelegramWorkspaceBindingIdentity("/repo", 0, "session")!,
          target: oldTarget, slot: "A", threadName: "Coral", updatedAtMs: 1,
          ...(delivery === "journal-protected" || delivery === "journal-unknown" || warmDisposalCases.includes(delivery)
            ? { journalBindingKeys: ["manual:independent"], journalBindingsComplete: true as const } :
            recipientWakeCases.includes(delivery) ? { journalBindingKeys: [`manual:${instanceId}`], journalBindingsComplete: true as const } :
            sessionReferenceCases.includes(delivery) ? { journalBindingKeys: [], journalBindingsComplete: true as const,
              journalSources: ["predecessor", "current"].map(sessionId => ({ sessionId, recipientBindingKey: "manual:independent" })) } : {}) };
        threadStore.upsertWorkspaceBinding(binding);
        for (let index = 1; index < 26; index += 1) {
          const slot = String.fromCharCode(65 + index);
          const otherTarget = { chatId: 7, threadId: 100 + index };
          const controller = role === "follower" && index === 1;
          threadStore.upsert({ profileKey: controller ? "cwd:/repo" : `manual:other-${slot}`,
            owner: controller ? { kind: "leader", cwd: "/repo", instanceId: "leader-a" } : { kind: "manual-follower", instanceId: `other-${slot}` },
            instanceId: controller ? "leader-a" : `other-${slot}`, target: otherTarget, slot, status: "active", createdAtMs: 1, updatedAtMs: 1 });
          threadStore.upsertWorkspaceBinding({ ...Threads.createTelegramWorkspaceBindingIdentity(controller ? "/repo" : `/other-${slot}`, 0, `session-${slot}`)!,
            target: otherTarget, slot, updatedAtMs: 1 });
        }
        if (delivery === "leader-recovery") {
          const other = threadStore.listWorkspaceBindings().find(value => value.slot === "Z")!;
          threadStore.upsertPendingProvision({ id: "leader-late", owner: "manual-follower", instanceId: "other-Z",
            profileKey: "manual:other-Z", workspaceBindingKey: other.bindingKey, slot: "Z", target: other.target, startedAtMs: 1 });
        }
        await threadStore.persist();
        const warmDisposal = warmDisposalCases.includes(delivery);
        const queueProof = queuedProofCases.includes(delivery) || role === "leader" && warmDisposal;
        const journalOptions = { path: sessionReferenceCases.includes(delivery)
          ? Paths.resolveTelegramSessionPollingJournalPath("controller", dirname(path), "default") : `${path}.inbox`,
          botIdentity: Journal.createTelegramUpdateJournalBotIdentity({ botToken: "fixture:restore", botId: 7 }) };
        const resolveJournal = Journal.createTelegramUpdateJournalRuntimeBindingResolver({ getProfileName: () => undefined,
          getBotToken: () => "fixture:restore", getBotId: () => 7, getJournalPath: () => journalOptions.path,
          getQueueRuntimeIdentity: queuedTerminalCases.includes(delivery)
            ? () => ({ instanceId: "leader-a", processId: process.pid, processBirthId: `${process.pid}:restore-controls` }) : undefined,
          withSourceSerialization: createTelegramConfigStore({ agentDir: dirname(path) }).withSourceSerialization });
        const journal = resolveJournal()!.journal;
        const journalBindingKey = Journal.createTelegramUpdateJournalBindingKey(journalOptions);
        const store = threadStore.workspaceRestore({ profileName: "default", tokenSha256: "a".repeat(64) });
        const admission = createTelegramWorkspaceAdmissionLedger({ path: `${path}.admission`, profileKey: "profile:restore",
          owner: { processId: process.pid, processBirthId: `${process.pid}:restore-controls` }, getProcessLiveness: () => "alive" });
        const operations = createTelegramWorkspaceOperationRuntime({ getWorkspaceAdmission: () => admission });
        const calls: string[] = [];
        const operationIds: string[] = [];
        const ctx = { cwd: "/repo" };
        let current = true, contextActive = true, generation = 1, epoch = "epoch";
        let warmCleanupInterrupted = false;
        let independentImage: string | undefined;
        let sessionId = role === "leader" ? "session" : "session-B";
        const recipientFrames: Array<() => boolean> = [];
        let repeatAck: (() => void) | undefined;
        const leaderState = Threads.createTelegramLeaderThreadStateRuntime();
        leaderState.set({ target: role === "leader" ? oldTarget : { chatId: 7, threadId: 101 }, slot: role === "leader" ? "A" : "B", threadName: "Coral" });
        const protocol = Bus.createTelegramBusProtocolIdentity({ runtimeBuild: "fixture",
          capabilities: [Bus.TELEGRAM_BUS_CAPABILITY_DURABLE_FOLLOWER_ADMISSION, Bus.TELEGRAM_BUS_CAPABILITY_WORKSPACE_RESTORE] });
        const recipientJournal = Journal.createTelegramUpdateJournalStore({ ...journalOptions, path: `${path}.recipient` });
        const independentPath = `${path}.independent`;
        const independentJournal = Journal.createTelegramUpdateJournalStore({ ...journalOptions, path: independentPath });
        const addIndependentWork = () => {
          independentJournal.appendBatch([{ update_id: 99, message: { message_id: 99, message_thread_id: 77,
            chat: { id: 7, type: "private" }, from: { id: 7, is_bot: false }, text: "Independent accepted work" } }]);
          independentJournal.markQueued({ queueKind: "prompt", receiptId: "independent", sourceUpdateIds: [99],
            owner: { instanceId: "independent", processId: process.pid, processBirthId: `${process.pid}:independent`, sessionGeneration: 1 } });
        };
        if (delivery === "journal-protected" || delivery === "journal-unknown" || warmDisposal) addIndependentWork();
        if (warmDisposal) independentImage = readFileSync(independentPath, "utf8");
        if (delivery === "journal-unknown") writeFileSync(independentPath, "{broken", { mode: 0o600 });
        const inspectJournal = (journalPath: string) => {
          const evidence = Journal.inspectTelegramUpdateJournalFamily({ directory: dirname(path), path: journalPath,
            profile: "default", botIdentity: journalOptions.botIdentity,
            limits: { maxFiles: 128, maxBytes: 1_000_000, maxEntries: 128, maxWork: 10_000 } });
          if (evidence.kind !== "present") throw new Error("Protection evidence unavailable");
          return { entries: evidence.file.entries };
        };
        const references = Journal.createTelegramUpdateJournalReferenceRegistry();
        const sessionPaths = ["predecessor", "current", "successor"].map(sessionId =>
          Paths.resolveTelegramSessionJournalPath(sessionId, "manual:independent", dirname(path), "default"));
        const sessionJournals = sessionReferenceCases.includes(delivery) ? sessionPaths.map(sourcePath => {
          const source = Journal.createTelegramUpdateJournalStore({ ...journalOptions, path: sourcePath });
          source.appendBatch([{ update_id: 99, message: { message_id: 99, message_thread_id: 77,
            chat: { id: 7, type: "private" }, from: { id: 7, is_bot: false }, text: "Historical source" } }]);
          source.removeCompleted([99]);
          return source;
        }) : [];
        const protectionConfig = createTelegramConfigStore({ agentDir: dirname(path) });
        const sessionBindings = Journal.createTelegramUpdateJournalBindingRuntime({
          base: { getProfileName: () => "default", getBotToken: () => "fixture:restore", getBotId: () => 7,
            withSourceSerialization: protectionConfig.withSourceSerialization },
          getLeaderJournalPath: () => journalOptions.path, getRuntimeDir: () => Paths.resolveTelegramTempDir(dirname(path)),
          getFollowerJournalPath: (key, profile, sessionId) => sessionId
            ? Paths.resolveTelegramSessionJournalPath(sessionId, key, dirname(path), profile)
            : Paths.resolveTelegramFollowerJournalPath(key, dirname(path), profile),
          getActiveFollowerBindingKey: () => "manual:independent", getActiveFollowerSessionId: () => "current", isFollowerRegistered: () => false,
        });
        const captureProtection = createTelegramWorkspaceExternalProtectionCapture({ listFollowers: () => [],
          getActiveTurnTarget: () => undefined, getQueuedItems: () => [],
          resolveLeaderJournal: () => ({ recoveryKey: journalOptions.path, journal,
            readForProtection: () => inspectJournal(journalOptions.path) }),
          createFollowerJournalResolver(key) {
            if (recipientWakeCases.includes(delivery)) {
              assert.equal(key, `manual:${instanceId}`);
              return () => ({ recoveryKey: `${path}.recipient`, journal: recipientJournal,
                readForProtection: () => inspectJournal(`${path}.recipient`) });
            }
            assert.equal(key, "manual:independent");
            return () => ({ recoveryKey: independentPath, journal: independentJournal,
              readForProtection: () => inspectJournal(independentPath) });
          },
          ...(sessionReferenceCases.includes(delivery) ? {
            createSessionJournalResolver: sessionBindings.createRecipientResolver,
            createJournalPathResolver: sessionBindings.createPathResolver,
            inspectJournalNamespace: () => Journal.inspectTelegramSessionJournalNamespace({
              directory: Paths.resolveTelegramTempDir(dirname(path)), profile: "default", pollingPath: journalOptions.path,
              botIdentity: journalOptions.botIdentity,
              limits: { maxDirectoryEntries: 1000, maxFiles: 256, maxBytes: 1_000_000, maxEntries: 1000, maxWork: 10_000 },
            }),
          } : {}),
          withJournalReference(reader, observe) {
            return references.withReference({ referenceClass: "workspace-retirement", recoveryKey: reader.recoveryKey! }, () => {
              assert.ok(references.list().some(reference => reference.recoveryKey === reader.recoveryKey));
              return observe();
            });
          },
        });
        let allowCleanupWake = false;
        let protectionReads = 0;
        let nativeProtectionReads = 0;
        const sessionReferenceObservations: Array<{ afterClose: boolean; sources: Threads.TelegramWorkspaceJournalSource[] | undefined }> = [];
        let sessionReferenceImage: string | undefined;
        let ownerBearingJournal: Journal.TelegramUpdateJournalStore | undefined;
        let ownerBearingPath: string | undefined;
        let ownerBearingImage: string | undefined;
        let ownerBearingOriginals: Journal.TelegramUpdateJournalEntry[] | undefined;
        const ownerBearingProtection: Array<ReturnType<typeof captureProtection>["acceptedWork"]> = [];
        let sessionReferenceBoundaryCompleted = false;
        const socketPath = Bus.getTelegramBusFollowerSocketPath(instanceId, dirname(path));
        const followerRegistry = Bus.createTelegramBusFollowerRegistry();
        followerRegistry.register({ instanceId, registrationGeneration: "registered", profileKey: `manual:${instanceId}`,
          cwd: delivery === "recipient-cwd" ? "/repo/" : "/repo", sessionId: "session", slot: "A", target: oldTarget, busSocketPath: socketPath, protocol, connectedAtMs: 1 });
        const followerState = createTelegramBusFollowerRegistrationState();
        followerState.setRegistered(true, oldTarget, { slot: "A", threadName: "Coral", generation: "registered", leaderProtocol: protocol });
        const followerStore = Threads.createTelegramTopicTargetStore({ path });
        const followerRestore = followerStore.workspaceRestore({ profileName: "default", tokenSha256: "a".repeat(64) });
        const restoreHandler = createTelegramBusFollowerWorkspaceRestoreHandler({ instanceId, registrationState: followerState,
          topicTargetStore: followerStore, getWorkspaceAdmission: () => admission,
          readRestoreIntent: id => followerRestore.list().find(value => value.request.operationId === id),
          getContextAuthority: () => current ? { profileBindingKey: "profile:restore", operatorUserId: 7,
            executor: { instanceId: "leader-a", leaderEpoch: "epoch" }, sessionId: "session", cwd: "/repo", generation: 1,
            leaderProtocol: protocol } : undefined });
        const admissionPort = createTelegramBusFollowerDurableAdmissionRuntime({ journal: recipientJournal,
          signalWorker() {
            calls.push("forward");
            if (delivery === "registration-change") followerRegistry.register({ ...followerRegistry.get(instanceId)!, registrationGeneration: "replacement" });
          } });
        const receiver = createTelegramBusForwardedUpdateReceiverRuntime({ socketPath, instanceId,
          getAuthSecret: () => "restore-forward-secret", getRegistrationGeneration: followerState.getGeneration,
          getRecipientBindingKey: () => `manual:${instanceId}`, getContext: () => ctx,
          isWorkspaceRestoreEnabled: () => true, async handleWorkspaceRestore(input) {
            calls.push(input.mode); operationIds.push(input.operationId);
            const observation = await restoreHandler(input, ctx);
            if (lostReply && input.mode === "apply") throw new Error("Readiness reply lost after apply");
            return observation;
          },
          durableAdmission: { async admit(envelope, context) {
            const receipt = await admissionPort.admit(envelope, context);
            if (delivery === "lost-ack" || delivery === "recipient-lost-source") throw new Error("Reply lost after recipient append");
            if (delivery === "acceptance-source-changed") journal.markQueued({ queueKind: "prompt", receiptId: "independent-source",
              sourceUpdateIds: [100], owner: { instanceId: "independent", processId: process.pid,
                processBirthId: `${process.pid}:changed-source`, sessionGeneration: 1 } });
            return receipt;
          } } });
        const forwarder = Bus.createTelegramBusForeignOwnedUpdateForwarder<TestContext,
          Updates.TelegramMessageReactionUpdated, TestCallbackQuery, TestMessage>({ socketPath,
          createRequestId: () => "restore-forward", getAuthSecret: () => "restore-forward-secret" });
        let requestIndex = 0;
        const runFollower = Bus.createTelegramBusWorkspaceRestoreController({ getFollower: followerRegistry.get,
          localProtocolIdentity: protocol, getAuthSecret: () => "restore-forward-secret", createRequestId: () => `restore-${++requestIndex}` });
        const regressCanonical = (updateMemory = true) => {
          // Simulate an unsupported writer/corrupt snapshot, bypassing the supporting publisher's guard.
          const owner = store.list()[0]!.request.owner;
          const snapshot = JSON.parse(readFileSync(path, "utf8"));
          snapshot.workspaceBindings = snapshot.workspaceBindings.map((value: Threads.TelegramWorkspaceThreadBinding) =>
            value.bindingKey === binding.bindingKey ? { ...binding } : value);
          snapshot.threads = snapshot.threads.map((value: Threads.TelegramTopicTargetRecord) =>
            value.instanceId === instanceId ? { ...value, target: oldTarget } : value);
          writeFileSync(path, JSON.stringify(snapshot), { mode: 0o600 });
          if (updateMemory) {
            threadStore.upsertWorkspaceBinding({ ...binding });
            threadStore.upsert({ ...owner });
          }
        };
        const independentTurn: Queue.PendingTelegramTurn = { kind: "prompt", chatId: 7, target: oldTarget, replyToMessageId: 0,
          queueOrder: 0, queueLane: "default", laneOrder: 0, statusSummary: "Independent work", historyText: "Independent work",
          sourceMessageIds: [], queuedAttachments: [], content: [{ type: "text", text: "Independent work" }] };
        let observations = 0;
        let settlementUnavailable = delivery === "settlement-publication-interrupted";
        let interruptedPublications = 0;
        let guardedCanonical: string | undefined;
        let acceptancePublications = 0;
        let capturedSourceHash: string | undefined;
        const grouped = queuedGroupCases.includes(delivery), sourceIds = grouped ? [100, 101] : [100];
        const sourceHashes = new Map<number, string>();
        let queueObservers = 0, inboundGroups = 0;
        let queueProofReads = 0, queuePublicationUnavailable = delivery === "queue-cold" || delivery === "queue-group-cold";
        let warmSourceDisposals = 0, warmProofReads = 0, warmReadbackFault = true, warmContinuationStarted = false;
        let warmRetainedProof: Journal.TelegramUpdateJournalSourceCompletion | undefined;
        let scopedDisposals = 0, terminalHints = 0, terminalReaderUnavailable = delivery === "queue-terminal-reader",
          terminalPublicationUnavailable = delivery === "queue-terminal-publication";
        const { routeRuntime, telegramQueueStore, activeTurnRuntime, events } = createRouteHarness({
          processInbound: grouped ? async (files, rawText) => {
            inboundGroups++; return { rawText, promptFiles: files, handlerOutputs: [], handledFiles: [] };
          } : undefined,
          threadStore: { ...threadStore, withWorkspaceRestoreSnapshot(expected, observe) {
            observations += 1;
            const inject = leaderBoundaryFaults.includes(delivery) && observations === (lostReply ? 2 : 1);
            if (inject) {
              if (delivery === "leader-recovery") withTelegramFileTransaction(`${path}.transaction`, () => {
                writeFileSync(`${path}.provision-recovery.json`, JSON.stringify({ "leader-late": {
                  instanceId: "other-Z", profileKey: "manual:other-Z", target } }), { mode: 0o600 });
              });
              if (delivery === "leader-disk") regressCanonical(false);
              if (delivery === "leader-owner") {
                const snapshot = JSON.parse(readFileSync(path, "utf8"));
                snapshot.threads = snapshot.threads.filter((value: Threads.TelegramTopicTargetRecord) => value.instanceId !== instanceId);
                writeFileSync(path, JSON.stringify(snapshot), { mode: 0o600 });
              }
              guardedCanonical = readFileSync(path, "utf8");
            }
            threadStore.withWorkspaceRestoreSnapshot(expected, snapshot => {
              assert.throws(() => withTelegramFileTransaction(`${path}.transaction`, () => assert.fail("unlocked leader observation"),
                { attempts: 1, retryDelayMs: 0 }), /Timed out acquiring Telegram lock transaction/);
              if (inject && delivery === "leader-context") sessionId = "replacement-session";
              return observe(snapshot);
            });
            if (delivery === "journal-observation-error" && expected.routing) throw new Error("Observation boundary failed after callback");
          } }, instanceId: "leader-a",
          async runWorkspaceOperation(input, operation) {
            const result = await operations.run(input, operation);
            if (delivery === "queue-post-admission-recipient" && input.operationKind === "workspace.restore-queue-acceptance") {
              leaderState.set({ target: oldTarget, slot: "A", threadName: "Coral" });
            }
            return result;
          }, getAdmissionJournalBinding: () => journalBindingKey,
          getWorkspaceRestoreStore: () => ({ ...store, recordSourceAcceptance(expected, evidence, authority) {
            acceptancePublications += 1;
            const original = journal.read().entries.find(entry => entry.updateId === evidence.updateId)!;
            assert.equal(original.state, queueProof ? "queued" : "pending", "acceptance publication precedes source removal or queue readiness");
            capturedSourceHash = createHash("sha256").update(JSON.stringify(original)).digest("hex");
            sourceHashes.set(evidence.updateId, capturedSourceHash);
            assert.equal(evidence.sourceSha256, capturedSourceHash, "hash belongs to the journal original, not the relocated message");
            assert.ok(admission.read().leases.length > 0, "proof publication uses the dispatch admission, not a reentrant gate");
            const secondGroupSource = grouped && evidence.updateId === 101;
            if (delivery === "acceptance-return-no-proof" || delivery === "queue-no-proof" || secondGroupSource && delivery === "queue-group-missing") return expected;
            if (delivery === "queue-before-rename" || delivery === "queue-after-rename" || queuePublicationUnavailable && (!grouped || secondGroupSource) ||
                secondGroupSource && (delivery === "queue-group-before" || delivery === "queue-group-after")) {
              assert.equal(evidence.kind, "queued");
              return threadStore.workspaceRestore({ profileName: "default", tokenSha256: "a".repeat(64), onPublicationBoundary(point) {
                if (point === (delivery === "queue-before-rename" || delivery === "queue-group-before" || queuePublicationUnavailable ? "after-write-before-rename" : "after-rename")) {
                  throw new Error("Fixture queued acceptance publication interrupted");
                }
              } }).recordSourceAcceptance(expected, evidence, authority);
            }
            if (delivery === "acceptance-before-rename" || delivery === "acceptance-after-rename") {
              return threadStore.workspaceRestore({ profileName: "default", tokenSha256: "a".repeat(64), onPublicationBoundary(point) {
                if (point === (delivery === "acceptance-before-rename" ? "after-write-before-rename" : "after-rename")) {
                  throw new Error("Fixture forwarding acceptance publication interrupted");
                }
              } }).recordSourceAcceptance(expected, evidence, authority);
            }
            const duplicateBytes = grouped && expected.routing?.acceptances?.some(value => value.updateId === evidence.updateId)
              ? readFileSync(path, "utf8") : undefined;
            const accepted = store.recordSourceAcceptance(expected, evidence, authority);
            if (duplicateBytes !== undefined) assert.equal(readFileSync(path, "utf8"), duplicateBytes, "duplicate group proof does not create another canonical revision");
            if (secondGroupSource && delivery === "queue-group-offer") {
              journal.offerQueuedHandoff({ queueKind: original.queueKind!, receiptId: original.queueReceiptId!, sourceUpdateIds: sourceIds,
                expectedOwner: original.queueOwner!, recipientOwner: { instanceId: "independent", processId: process.pid + 1,
                  processBirthId: "fixture-group-recipient", sessionGeneration: 1 }, handoffToken: "b".repeat(32) });
            }
            if (delivery === "source-cas-changed") journal.markExecutionFailure({ updateId: 100, expectedAttemptCount: 0,
              failedAtMs: 1000, failureClass: "fixture", summary: "changed source after acceptance", disposition: "retry-wait",
              nextRetryAtMs: Date.now() + 60_000 });
            return accepted;
          }, recordSourceSettlement(expected, evidence, authority) {
            if (terminalPublicationUnavailable && evidence.kind === "queue-completed") throw new Error("Fixture terminal queue publication interrupted");
            if (settlementUnavailable) {
              interruptedPublications += 1;
              const source = journal.read().entries.find(entry => entry.updateId === 100);
              assert.equal(source?.state, role === "leader" ? "queued" : undefined,
                "the observer runs after queue receipt commit or source removal, not before it");
              throw new Error("Fixture Restore settlement publication unavailable");
            }
            return store.recordSourceSettlement(expected, evidence, authority);
          } }), hasWorkspaceRestoreAuthority: () => current, getSessionGeneration: () => generation,
          isContextActive: context => context === ctx && contextActive,
          captureWorkspaceExternalProtection: delivery === "journal-missing" ? undefined : (candidate, options) => {
            protectionReads += 1;
            assert.equal(options?.requireBindingProvenance, true);
            assert.ok(admission.read().leases.length > 0);
            assert.throws(() => withTelegramFileTransaction(`${path}.transaction`, () => assert.fail("unlocked protection"),
              { attempts: 1, retryDelayMs: 0 }), /Timed out acquiring Telegram lock transaction/);
            assert.deepEqual(candidate.target, oldTarget);
            if (recipientWakeCases.includes(delivery)) return captureProtection(candidate, options);
            if (sessionReferenceCases.includes(delivery)) {
              sessionReferenceObservations.push({ afterClose: calls.includes("closeForumTopic"), sources: structuredClone(candidate.journalSources) });
              const evidence = captureProtection(candidate, options);
              nativeProtectionReads++;
              if (ownerBearingReferenceCases.includes(delivery) && calls.includes("closeForumTopic")) ownerBearingProtection.push(evidence.acceptedWork);
              return evidence;
            }
            if (delivery === "settlement-publication-interrupted" || forwardProofCases.includes(delivery) || queuedProofCases.includes(delivery)) return { acceptedWork: "unknown",
              liveOwner: "unknown", deliveryAuthority: "unknown" };
            if (cleanupWakeCases.includes(delivery)) return { acceptedWork: allowCleanupWake ? "clear" : "unknown",
              liveOwner: "unknown", deliveryAuthority: "unknown" };
            if (delivery === "journal-protected" || delivery === "journal-unknown" || warmDisposal ||
                (delivery === "journal-late" && calls.includes("closeForumTopic"))) {
              assert.deepEqual(candidate.journalBindingKeys, ["manual:independent"], "fresh canonical references reach inspection");
              const evidence = captureProtection(candidate, options);
              nativeProtectionReads += 1;
              assert.equal(evidence.acceptedWork, delivery === "journal-unknown" ? "unknown" : "protected");
              return evidence;
            }
            // Other cases isolate dispatch/cleanup with an explicitly supplied clear-evidence precondition.
            return { acceptedWork: "clear", liveOwner: "unknown", deliveryAuthority: "unknown" };
          },
          getCurrentLeaderEpoch: () => epoch,
          callApi: async (method, body) => {
            calls.push(method);
            if (method === "closeForumTopic" && (warmCleanupCases.includes(delivery) || delivery === "warm-cleanup-registration")) {
              addIndependentWork();
              independentImage = readFileSync(independentPath, "utf8");
              await Promise.resolve();
              warmCleanupInterrupted = true;
              if (delivery === "warm-cleanup-context") contextActive = false;
              if (delivery === "warm-cleanup-generation") generation = 2;
              if (delivery === "warm-cleanup-epoch") epoch = "replacement-epoch";
              if (delivery === "warm-cleanup-canonical") regressCanonical();
              if (delivery === "warm-cleanup-registration") followerRegistry.register({ ...followerRegistry.get(instanceId)!, registrationGeneration: "replacement" });
            }
            if (method === "closeForumTopic" && delivery === "recipient-changed") {
              followerRegistry.register({ ...followerRegistry.get(instanceId)!, registrationGeneration: "replacement" });
            }
            if (method === "closeForumTopic" && cleanupWakeCases.includes(delivery)) {
              assert.deepEqual(store.list()[0]?.routing?.settlements.flatMap(value => value.updateIds), [100],
                "the wake never becomes settlement evidence for unrelated source IDs");
            }
            if (method === "closeForumTopic" && sessionReferenceCases.includes(delivery)) {
              const writer = Threads.createTelegramTopicTargetStore({ path }); await writer.load();
              const canonical = writer.listWorkspaceBindings().find(value => value.bindingKey === binding.bindingKey)!;
              const pruned = writer.commitWorkspaceJournalEvidence(canonical, [], true, binding.journalSources!.slice(1));
              assert.ok(pruned);
              writer.upsertWorkspaceBinding({ ...pruned, journalSources: [...pruned.journalSources!,
                { sessionId: "successor", recipientBindingKey: "manual:independent" }] });
              await writer.persist();
              const published = Threads.createTelegramTopicTargetStore({ path }); await published.load();
              assert.deepEqual(published.listWorkspaceBindings().find(value => value.bindingKey === binding.bindingKey)?.journalSources,
                [{ sessionId: "current", recipientBindingKey: "manual:independent" },
                  { sessionId: "successor", recipientBindingKey: "manual:independent" }]);
              assert.deepEqual(store.list()[0]!.request.binding.journalSources, binding.journalSources,
                "the immutable request still requires the predecessor even after canonical pruning");
              if (delivery === "session-reference-pending") sessionJournals[0]!.appendBatch([{ update_id: 101,
                message: { message_id: 101, message_thread_id: 77, chat: { id: 7, type: "private" },
                  from: { id: 7, is_bot: false }, voice: { file_id: "unsupported", duration: 1 } } }]);
              if (ownerBearingReferenceCases.includes(delivery)) {
                ownerBearingPath = delivery === "session-reference-queued-group" ? sessionPaths[0]! :
                  Paths.resolveTelegramSessionJournalPath("unrelated", "manual:shared", dirname(path), "default");
                ownerBearingJournal = Journal.createTelegramUpdateJournalStore({ ...journalOptions, path: ownerBearingPath });
                const sourceUpdateIds = delivery === "session-reference-shared-control" ? [110] : [110, 111];
                ownerBearingJournal.appendBatch(sourceUpdateIds.map(update_id => ({ update_id, message: {
                  message_id: update_id, message_thread_id: 77, chat: { id: 7, type: "private" }, from: { id: 7, is_bot: false },
                  text: delivery === "session-reference-shared-control" ? "/continue" : `Accepted group ${update_id}` } })));
                ownerBearingJournal.markQueued({ queueKind: delivery === "session-reference-shared-control" ? "control" : "prompt",
                  receiptId: "independent-group", sourceUpdateIds, owner: { instanceId: "independent", processId: process.pid,
                    processBirthId: `${process.pid}:independent`, sessionGeneration: 1 } });
                const entry = ownerBearingJournal.read().entries.find(entry => entry.updateId === 110)!;
                const receipt = { queueKind: entry.queueKind!, receiptId: entry.queueReceiptId!, sourceUpdateIds, queueOwner: entry.queueOwner! };
                const ownerKey = Journal.createTelegramUpdateJournalBindingKey({ ...journalOptions, path: ownerBearingPath });
                assert.equal(sessionBindings.inspectQueuedReceipt(ownerKey, receipt)?.sources.length, sourceUpdateIds.length);
                if (delivery === "session-reference-shared-offer") {
                  ownerBearingJournal.offerQueuedHandoff({ ...receipt, expectedOwner: receipt.queueOwner,
                    recipientOwner: { instanceId: "recipient", processId: process.pid + 1, processBirthId: "recipient-birth", sessionGeneration: 2 },
                    handoffToken: Journal.createTelegramUpdateQueueHandoffToken() });
                  assert.equal(sessionBindings.inspectQueuedReceipt(ownerKey, receipt), undefined);
                }
                ownerBearingOriginals = ownerBearingJournal.read().entries;
                ownerBearingImage = readFileSync(ownerBearingPath, "utf8");
              }
              if (delivery === "session-reference-unreadable") await writeFile(sessionPaths[0]!, "{broken");
              if (delivery === "session-reference-missing") {
                await rm(sessionPaths[0]!);
                await rm(`${sessionPaths[0]}.segments`, { recursive: true, force: true });
              } else sessionReferenceImage = readFileSync(sessionPaths[0]!, "utf8");
              sessionReferenceBoundaryCompleted = true;
            }
            if (method === "closeForumTopic" && delivery === "journal-late") {
              addIndependentWork();
              const writer = Threads.createTelegramTopicTargetStore({ path }); await writer.load();
              writer.upsertWorkspaceBinding({ ...writer.listWorkspaceBindings().find(value => value.bindingKey === binding.bindingKey)!,
                journalBindingKeys: ["manual:independent"], journalBindingsComplete: true });
              await writer.persist();
              assert.equal(threadStore.listWorkspaceBindings().find(value => value.bindingKey === binding.bindingKey)?.journalBindingKeys, undefined,
                "the working projection intentionally lacks the newly published reference");
            }
            if (method === "editForumTopic" && delivery === "canonical-after-ready") {
              regressCanonical();
            }
            if ((method === "editForumTopic" && delivery === "queued-before") ||
                (method === "closeForumTopic" && delivery === "queued-during")) {
              telegramQueueStore.setQueuedItems([...telegramQueueStore.getQueuedItems(), independentTurn]);
            }
            if (method === "editForumTopic" && delivery === "active-before") activeTurnRuntime.set(independentTurn);
            if (method === "editForumTopic" && delivery === "session-change") {
              if (role === "leader") sessionId = "replacement-session";
              else followerRegistry.register({ ...followerRegistry.get(instanceId)!, sessionId: "replacement-session" });
            }
            if (method === "editForumTopic" && delivery === "target-change") {
              if (role === "leader") leaderState.set({ target: oldTarget, slot: "A", threadName: "Coral" });
              else followerRegistry.register({ ...followerRegistry.get(instanceId)!, target: oldTarget });
            }
            if (method === "closeForumTopic" || method === "deleteForumTopic") {
              assert.equal(body.message_thread_id, oldTarget.threadId, "cleanup is bound to the old target");
              if (method === "deleteForumTopic" && (delivery === "cleanup-unknown" || delivery === "recipient-cleanup-unknown")) throw new Error("Deletion reply lost");
            }
            return {} as never;
          },
          setCurrentLeaderIdentity(value) {
            assert.ok(admission.read().leases.length > 0);
            assert.throws(() => withTelegramFileTransaction(`${path}.transaction`, () => assert.fail("unlocked leader effect"),
              { attempts: 1, retryDelayMs: 0 }), /Timed out acquiring Telegram lock transaction/);
            calls.push("apply"); operationIds.push(store.list()[0]!.request.operationId);
            leaderState.set(value);
            if (delivery === "canonical-before-ack") regressCanonical();
            if (lostReply) throw new Error("Local readiness reply lost after apply");
          },
          workspaceRestoreRecipient: { getSessionId: () => sessionId, getCwd: () => "/repo",
            getLeaderIdentity: leaderState.getIdentity, followerRegistry,
            async runFollower(input) {
              recipientFrames.push(input.isCurrent);
              const observation = await runFollower(input);
              if (delivery === "canonical-before-ack") regressCanonical();
              if (input.mode === "apply" && delivery === "registry-disk-regression") regressCanonical(false);
              if (input.mode === "apply" && delivery === "registry-recovery") {
                const other = threadStore.listWorkspaceBindings().find(value => value.slot === "Z")!;
                const pending = { id: "late-registry", owner: "manual-follower" as const, instanceId: "other-Z",
                  profileKey: "manual:other-Z", workspaceBindingKey: other.bindingKey, slot: "Z", startedAtMs: 1 };
                threadStore.upsertPendingProvision(pending); await threadStore.persist();
                await threadStore.recordPendingProvisionTargetRecovery(pending, target);
              }
              return observation;
            } },
          getLiveThreadTargets: () => [leaderState.getIdentity()!.target!,
            ...(role === "follower" ? followerRegistry.list().flatMap(value => value.target ? [value.target] : []) : [])],
          getMessageOwnership: (chatId, messageId) => chatId === 7 && messageId === 99 ? { instanceId: "leader-a" } : undefined,
          getTargetOwnership: candidate => role === "follower" ? Bus.getTelegramFollowerTargetOwnership({ target: candidate,
            currentInstanceId: "leader-a", followers: followerRegistry.list(), activeThreadRecords: threadStore.list() }) : undefined,
          inspectRestoreQueuedReceipt(expected) {
            queueProofReads++;
            if (queueProofReads === 2 && delivery === "queue-post-read-missing") return undefined;
            if (queueProofReads === 2 && delivery === "queue-post-read-recipient") leaderState.set({ target: oldTarget, slot: "A", threadName: "Coral" });
            assert.ok(admission.read().leases.length > 0, "queued proof uses fresh Workspace admission");
            if (delivery === "queue-read-missing") return undefined;
            if (delivery === "queue-read-throws") throw new Error("Fixture queue observation refused");
            if (delivery === "queue-authority") current = false;
            if (delivery === "queue-recipient") leaderState.set({ target: oldTarget, slot: "A", threadName: "Coral" });
            assert.equal(expected.journalBindingKey, journalBindingKey);
            const proof = references.withReference({ referenceClass: "operator-disposition", recoveryKey: journalBindingKey }, () =>
              resolveJournal()!.journal.inspectQueuedReceipt({ queueKind: expected.queueKind, receiptId: expected.receiptId,
                sourceUpdateIds: [...expected.sourceUpdateIds], queueOwner: { ...expected.queueOwner } }));
            if (proof && queueProofReads === 2 && (delivery === "queue-post-read-source" || delivery === "queue-group-source")) {
              return { ...proof, sources: proof.sources.map(source => ({ ...source,
                sourceSha256: delivery === "queue-group-source" && source.updateId === 100 ? source.sourceSha256 : "b".repeat(64) })) };
            }
            return proof;
          },
          inspectRestoreSourceCompletion: queuedTerminalCases.includes(delivery) || warmDisposal ? expected => {
            const { journalBindingKey: bindingKey, ...scope } = expected;
            assert.equal(bindingKey, journalBindingKey);
            assert.ok(admission.read().leases.length > 0, "readback holds fresh profile admission");
            const proof = references.withReference({ referenceClass: "operator-disposition", recoveryKey: journalBindingKey },
              () => resolveJournal()!.journal.inspectSourceCompletion(scope));
            if (warmContinuationStarted) {
              warmProofReads++;
              assert.deepEqual(proof, warmRetainedProof, "continuation reads only the exact immutable ACK");
              if (warmReadbackFault && warmProofReads === 2) {
                assert.equal(store.list()[0]?.readyRecipient?.generation, "2", "fresh successor inspection precedes post-await proof reread");
                if (delivery === "warm-readback-missing") return undefined;
                if (delivery === "warm-readback-context") contextActive = false;
              }
            }
            return proof ? { ...proof, journalBindingKey } : undefined;
          } : undefined,
          foreignOwnedUpdateForwarder: forwarder });
        const heartbeatPath = `${path}.leader.sock`;
        const heartbeatFrames: Array<() => boolean> = [];
        const heartbeatRuntime = createTelegramBusLeaderRuntime<TestContext>({ socketPath: heartbeatPath,
          authSecret: "restore-forward-secret", protocolIdentity: protocol, followerRegistry,
          getCurrentLeaderEpoch: () => "epoch", getAllowedUserId: () => 7, getTelegramProfile: () => "default",
          startPolling() {}, stopPolling() {}, isFollowerProcessAlive: () => true,
          onWorkspaceRestoreRecipientObserved(follower, isCurrent) {
            assert.deepEqual(admission.read().leases, [], "heartbeat lends no Workspace admission");
            heartbeatFrames.push(isCurrent);
            return routeRuntime.onWorkspaceRestoreRecipientObserved(follower, isCurrent, ctx);
          } });
        let heartbeats = 0;
        const heartbeat = async () => {
          const response = await Bus.sendTelegramBusLocalEnvelope({ socketPath: heartbeatPath, envelope: {
            kind: "follower.heartbeat", requestId: `wake:${++heartbeats}`, auth: "restore-forward-secret", instanceId,
            registrationGeneration: followerRegistry.get(instanceId)!.registrationGeneration, sentAtMs: 1,
          } });
          assert.equal(response?.kind === "bus.ack" && response.ok, true);
          await routeRuntime.waitForRestoreSettlement();
        };
        const recipientExecutions: number[] = [];
        const recipientWorker = Updates.createTelegramUpdateAdmissionWorkerRuntime<Journal.TelegramJournaledUpdate, TestContext>({
          journal: recipientJournal, hasAuthority: () => true,
          async defaultHandle(update) { recipientExecutions.push(update.update_id); },
        });
        const failures: unknown[] = [];
        const workerOwner = Updates.createTelegramUpdateWorkerOwnerRuntime<TestContext>({ instanceId: "leader-a",
          processId: process.pid, processBirthId: `${process.pid}:restore-controls`, getSessionGeneration: () => 1,
          isContextCurrent: () => true, dispatchNext() {}, requestQueueHandoffReconciliation() {},
          afterQueueReceiptCommitted(receipt, context) {
            queueObservers++;
            if (queuedProofCases.includes(delivery) && receipt.sourceUpdateIds.some(id => sourceIds.includes(id))) {
              for (const id of sourceIds) {
                const proof = store.list()[0]?.routing?.acceptances?.find(value => value.updateId === id);
                assert.equal(proof?.kind, "queued", "all canonical acceptances must precede dispatch readiness observer");
                assert.equal(proof.sourceSha256, sourceHashes.get(id));
              }
              assert.deepEqual(receipt.sourceUpdateIds, sourceIds);
            }
            repeatAck = () => routeRuntime.onQueueReceiptCommitted(receipt, context);
            repeatAck();
            assert.equal(store.list()[0]?.routing?.cleanup, undefined, "ACK cannot reenter the still-held dispatch gate");
            if (delivery === "authority-ended") current = false;
          }, afterUpdateCompleted(id, context, sourceBinding) {
            routeRuntime.onUpdateCompleted(id, context, sourceBinding);
            if (id !== 100) return;
            repeatAck = () => routeRuntime.onUpdateCompleted(id, context, sourceBinding);
            assert.equal(store.list()[0]?.routing?.cleanup, undefined, "ACK cannot reenter the still-held dispatch gate");
            if (delivery === "authority-ended") current = false;
          } });
        const worker = Updates.createTelegramUpdateAdmissionWorkerRuntime<Journal.TelegramJournaledUpdate & Updates.TelegramUpdateFlow, TestContext>({
          journal: { ...journal, completeQueuedExact(receipts, completions) {
            scopedDisposals++;
            if (warmDisposal && delivery === "warm-disposal-before") throw new Error("Fixture warm disposal interrupted before publication");
            const result = journal.completeQueuedExact(receipts, completions);
            if (warmDisposal) throw new Error("Fixture warm disposal reply lost after publication");
            if (delivery === "queue-terminal-lost" || delivery === "queue-terminal-mixed-lost") throw new Error("Fixture queued disposal ACK lost");
            return result;
          }, inspectSourceCompletion(scope) {
            if (scopedDisposals && terminalReaderUnavailable) return undefined;
            return journal.inspectSourceCompletion(scope);
          }, removeCompletedExact(updateIds, expectedSources, completions) {
            if (warmDisposal && updateIds.includes(100)) {
              warmSourceDisposals++;
              if (delivery === "warm-disposal-before") throw new Error("Fixture warm source disposal interrupted before publication");
            }
            if (updateIds.includes(100) && role === "follower") {
              const accepted = store.list()[0]?.routing?.acceptances?.find(value => value.updateId === 100);
              assert.equal(accepted?.kind, "forwarded", "the worker cannot remove a forwarded Restore original before acceptance publishes");
              assert.equal(accepted.sourceSha256, capturedSourceHash);
              assert.deepEqual(expectedSources, [{ updateId: 100, sourceSha256: accepted.sourceSha256 }]);
              assert.deepEqual(completions, [{ updateId: 100, sourceSha256: accepted.sourceSha256,
                completionSha256: Threads.getTelegramWorkspaceRestoreSourceCompletionSha256(store.list()[0]!, accepted) }]);
              if (delivery === "source-removal-before-write") throw new Error("Fixture source removal not published");
            }
            const result = journal.removeCompletedExact(updateIds, expectedSources, completions);
            if (updateIds.includes(100) && (delivery === "source-removal-lost-ack" || warmDisposal)) throw new Error("Fixture source removal ACK lost");
            return result;
          } }, getJournalBindingKey: () => journalBindingKey, hasAuthority: () => true,
          getQueueOwnerIdentity: () => ({ instanceId: "leader-a", processId: process.pid, processBirthId: `${process.pid}:restore-controls`, sessionGeneration: 1 }),
          defaultHandle: (update, ctx) => routeRuntime.handleUpdate(update as TestUpdate, ctx),
          beforeQueueReceiptPublished: queueProof ? routeRuntime.beforeQueueReceiptPublished : undefined,
          onQueueReceiptCompleted(receipt, context) {
            terminalHints++;
            assert.equal(worker.getState().queuedClaimCount, queuedTerminalMixedCases.includes(delivery) ? 1 : 0,
              "terminal notification clears only its scoped sources, not independent ordinary siblings");
            routeRuntime.onQueueReceiptCompleted(receipt, context);
          },
          onQueueReceiptCommitted: workerOwner.onQueueReceiptCommitted,
          onUpdateCompleted: workerOwner.onUpdateCompleted,
          recordRuntimeEvent(_category, error) { failures.push(error); } });
        let callbackId = 1000;
        const click = async (data: string, threadId = 42) => {
          journal.appendBatch([{ update_id: ++callbackId, callback_query: { id: `${data}:${callbackId}`, from: { id: 7, is_bot: false },
            message: { message_id: 99, message_thread_id: threadId, chat: { id: 7, type: "private" } }, data } }]);
          worker.signal();
          await worker.waitForDrain();
        };
        try {
          if (role === "follower") await receiver.start();
          if (recipientWakeCases.includes(delivery)) await heartbeatRuntime.startPolling(ctx);
          worker.start(ctx);
          await worker.waitForDrain();
          journal.appendBatch(sourceIds.map(id => ({ update_id: id, message: { ...unboundTopicUpdate().message!,
            chat: { id: 7, type: "private" }, ...(grouped ? { message_id: id, text: undefined, caption: `Grouped source ${id}`, media_group_id: "restore-group" } : {}) } })));
          worker.signal();
          await worker.waitForDrain();
          if (grouped) {
            assert.equal(inboundGroups, 0, "unbound album originals are deferred before semantic admission");
            await new Promise<void>(resolve => setTimeout(resolve, 1250));
            assert.match(events.join("\n"), /rerouterestore:1/, "one native coalesced chooser governs both originals");
            await click("rerouterestore:1");
            assert.match(events.join("\n"), /reroutenew:1:7/, "the shared Restore selection names the same binding");
          }
          assert.equal(journal.read().entries[0]?.state, "pending");
          await click("reroutenew:1:7");
          if (leaderBoundaryFaults.includes(delivery)) {
            if (lostReply) await click("reroutenew:1:7");
            assert.equal(observations, lostReply ? 2 : 1);
            assert.equal(calls.filter(value => value === "apply").length, lostReply ? 1 : 0);
            assert.deepEqual(leaderState.getIdentity()?.target, lostReply ? target : oldTarget);
            assert.equal(store.list()[0]?.phase, "recipient-issued");
            assert.equal(store.list()[0]?.routing, undefined);
            assert.equal(journal.read().entries.find(entry => entry.updateId === 100)?.state, "pending");
            assert.equal(telegramQueueStore.getQueuedItems().length + recipientJournal.read().entries.length, 0);
            assert.equal(calls.includes("deleteForumTopic") || calls.includes("createForumTopic") || calls.includes("forward"), false);
            assert.equal(readFileSync(path, "utf8"), guardedCanonical);
            assert.deepEqual(admission.read().leases, []);
            assert.equal(withTelegramFileTransaction(`${path}.transaction`, () => true, { attempts: 1, retryDelayMs: 0 }), true);
            return;
          }
          assert.equal(operationIds.length, 1, events.join("\n"));
          assert.ok(recipientFrames.every(isCurrent => !isCurrent()), "recipient callbacks cannot lend an ended dispatch admission");
          if (delivery === "registry-recovery" || delivery === "registry-disk-regression") {
            assert.deepEqual(followerRegistry.get(instanceId)?.target, oldTarget, "readiness reply alone cannot publish routing authority");
            assert.deepEqual(followerState.getTarget(), target, "uncertainty never rolls back the recipient's issued apply");
            assert.equal(store.list()[0]?.phase, "recipient-issued");
            assert.equal(store.list()[0]?.routing, undefined);
            assert.equal(journal.read().entries.find(entry => entry.update.update_id === 100)?.state, "pending");
            assert.equal(telegramQueueStore.getQueuedItems().length + recipientJournal.read().entries.length, 0);
            await click("reroutenew:1:7");
            assert.deepEqual(followerRegistry.get(instanceId)?.target, oldTarget);
            assert.equal(calls.filter(value => value === "apply").length, 1);
            assert.equal(calls.includes("forward") || calls.includes("deleteForumTopic"), false);
            if (delivery === "registry-recovery") {
              assert.equal(journal.read().entries.find(entry => entry.updateId === callbackId)?.state, "retry-wait",
                "failed recovery consumption retains the callback under the existing worker failure policy");
              assert.equal(journal.read().entries.find(entry => entry.updateId === 100)?.state, "pending");
            }
            return;
          }
          if (delivery === "canonical-before-ack" || delivery === "canonical-after-ready") {
            const cold = Threads.createTelegramTopicTargetStore({ path }); await cold.load();
            assert.deepEqual(cold.listWorkspaceBindings().find(value => value.bindingKey === binding.bindingKey)?.target, oldTarget);
            assert.equal(store.list()[0]?.phase, delivery === "canonical-before-ack" ? "recipient-issued" : "ready");
            assert.equal(store.list()[0]?.routing, undefined, "regressed canonical ownership cannot issue dispatch");
            assert.equal(journal.read().entries[0]?.state, "pending");
            assert.equal(telegramQueueStore.getQueuedItems().length + recipientJournal.read().entries.length, 0);
            assert.equal(calls.includes("deleteForumTopic"), false);
            return;
          }
          if (delivery === "session-change" || delivery === "target-change") {
            assert.equal(store.list()[0]?.phase, "ready");
            assert.equal(store.list()[0]?.routing, undefined, "a changed recipient cannot obtain the dispatch grant");
            assert.equal(journal.read().entries[0]?.state, "pending");
            assert.equal(telegramQueueStore.getQueuedItems().length + recipientJournal.read().entries.length, 0);
            assert.equal(calls.includes("deleteForumTopic"), false);
            return;
          }
          if (delivery === "lost-ack" || delivery === "registration-change") {
            assert.deepEqual(store.list()[0]?.routing, { settlements: [] });
            assert.equal(journal.read().entries[0]?.state, "pending", "no source ACK was fabricated");
            assert.equal(recipientJournal.read().entries[0]?.state, "pending", "accepted recipient work survives uncertainty");
            await click("reroutenew:1:7");
            assert.equal(calls.filter(value => value === "forward").length, 1, "retained dispatch grant never resends");
            assert.equal(protectionReads, 0, "callback completion cannot wake cleanup without positive original-source settlement");
            assert.deepEqual(store.list()[0]?.routing, { settlements: [] });
            assert.equal(calls.includes("deleteForumTopic"), false);
            if (delivery === "lost-ack") {
              journal.appendBatch([{ update_id: 200, message: { ...unboundTopicUpdate().message!, message_id: 200,
                chat: { id: 7, type: "private" }, message_thread_id: oldTarget.threadId } }]);
              worker.signal(); await worker.waitForDrain();
              await click("reroutenew:2:101", oldTarget.threadId);
              assert.deepEqual(threadStore.list().find(value => value.slot === "B")?.target, { chatId: 7, threadId: 101 },
                "another Restore cannot reuse the retained old target");
              await click("reroute:2:101", oldTarget.threadId);
              assert.equal(telegramQueueStore.getQueuedItems().length, 1, "independent routing still accepts its own original");
              await click("reroute:2:101", oldTarget.threadId);
              assert.equal(telegramQueueStore.getQueuedItems().length, 1, "cleanup retry cannot redispatch accepted work");
              assert.equal(calls.includes("closeForumTopic") || calls.includes("deleteForumTopic"), false,
                "ordinary unbound cleanup cannot bypass a retained Restore");
              assert.deepEqual(store.list()[0]?.routing, { settlements: [] });
            }
            return;
          }
          if (lostReply) {
            assert.equal(store.list()[0]?.phase, "recipient-issued");
            assert.deepEqual(role === "leader" ? leaderState.getIdentity()!.target : followerState.getTarget(), target,
              "local recipient applied exactly once despite the lost readiness reply");
            if (role === "follower") assert.deepEqual(followerRegistry.get(instanceId)?.target, oldTarget,
              "unacknowledged readiness cannot publish follower routing authority");
            if (delivery === "canonical-regression") {
              regressCanonical();
              await click("reroutenew:1:7");
              assert.equal(store.list()[0]?.phase, "recipient-issued", "local readiness alone cannot confirm canonical relocation");
              assert.equal(calls.filter(value => value === "apply").length, 1);
              assert.equal(store.list()[0]?.routing, undefined);
              assert.equal(journal.read().entries[0]?.state, "pending");
              assert.equal(telegramQueueStore.getQueuedItems().length + recipientJournal.read().entries.length, 0);
              assert.equal(calls.includes("deleteForumTopic"), false);
              return;
            }
            if (delivery === "local-regression") {
              if (role === "leader") leaderState.set({ target: oldTarget, slot: "A", threadName: "Coral" });
              else followerState.setRegistered(true, oldTarget, { slot: "A", generation: "registered", leaderProtocol: protocol });
              await click("reroutenew:1:7");
              assert.equal(calls.filter(value => value === "apply").length, 1, "inspection cannot apply again");
              assert.equal(store.list()[0]?.phase, "recipient-issued");
              assert.equal(journal.read().entries[0]?.state, "pending");
              assert.equal(telegramQueueStore.getQueuedItems().length + recipientJournal.read().entries.length, 0);
              assert.equal(calls.includes("deleteForumTopic"), false);
              return;
            }
            await click("reroute:1:101");
            assert.equal(operationIds.length, 1, "another selection cannot reuse the pending operation");
            await click("reroutenew:1:7");
          }
          await routeRuntime.waitForRestoreSettlement();
          if (warmDisposal) {
            const initial = store.list()[0]!;
            assert.equal(initial.phase, "ready");
            assert.equal(initial.routing?.acceptances?.length, 1);
            assert.deepEqual(initial.routing?.settlements.map(value => value.kind), role === "leader" ? ["queued"] : []);
            if (role === "leader") {
              const turn = telegramQueueStore.getQueuedItems()[0]!;
              const mux = Updates.createTelegramQueueAdmissionSettlementMuxRuntime([
                Updates.createTelegramQueueAdmissionSettlementRuntime(worker),
              ]);
              assert.equal(mux.isItemReady(turn), true);
              assert.equal(mux.onPromptHandedOff(turn, ctx), false, "interrupted disposal is never a terminal ACK");
              await routeRuntime.waitForRestoreSettlement();
            }
            assert.equal(role === "leader" ? scopedDisposals : warmSourceDisposals, 1);
            const acceptance = initial.routing!.acceptances![0]!;
            const scope = { updateId: acceptance.updateId, sourceSha256: acceptance.sourceSha256,
              completionSha256: Threads.getTelegramWorkspaceRestoreSourceCompletionSha256(initial, acceptance) };
            warmRetainedProof = journal.inspectSourceCompletion(scope);
            assert.deepEqual(warmRetainedProof, delivery === "warm-disposal-before" ? undefined : scope);
            const sourceBefore = journal.read().entries.find(value => value.updateId === 100);
            assert.equal(sourceBefore?.state, delivery === "warm-disposal-before" ? role === "leader" ? "queued" : "pending" : undefined);
            const sourceBytes = readFileSync(journalOptions.path, "utf8");
            const recipientBytes = role === "follower" ? readFileSync(`${path}.recipient`, "utf8") : undefined;
            const acceptedQueue = JSON.stringify(telegramQueueStore.getQueuedItems());
            const siblings = threadStore.listWorkspaceBindings().filter(value => value.bindingKey !== binding.bindingKey);
            generation = 2;
            if (role === "follower") {
              followerRegistry.register({ ...followerRegistry.get(instanceId)!, registrationGeneration: "2" });
              followerState.setRegistered(true, target, { slot: "A", threadName: "Coral", generation: "2", leaderProtocol: protocol });
            }
            if (delivery === "warm-readback-canonical") regressCanonical();
            warmContinuationStarted = true;
            const hint = async () => {
              routeRuntime.onUpdateCompleted(999, ctx, journalBindingKey);
              await routeRuntime.waitForRestoreSettlement();
            };
            await hint();
            const blocked = delivery !== "warm-readback-lost";
            assert.deepEqual(store.list()[0]?.routing?.settlements.map(value => value.kind), blocked
              ? role === "leader" ? ["queued"] : [] : [role === "leader" ? "queue-completed" : "completed"], events.join("\n"));
            if (delivery === "warm-readback-missing" || delivery === "warm-readback-context") {
              assert.equal(warmProofReads, 2, "the fault occurs after awaited successor inspection, not initial proof admission");
              warmReadbackFault = false;
              contextActive = true;
              await hint();
              assert.deepEqual(store.list()[0]?.routing?.settlements.map(value => value.kind), [role === "leader" ? "queue-completed" : "completed"]);
            }
            const continued = store.list()[0]!;
            assert.deepEqual(continued.request, initial.request, "warm continuation retains the exact original intent");
            assert.deepEqual(continued.recipient, initial.recipient, "successor inspection cannot rewrite original issuance");
            assert.deepEqual(continued.routing?.acceptances, initial.routing?.acceptances);
            assert.equal(continued.routing?.cleanup, undefined, "independent accepted custody protects the old tab");
            if (delivery === "warm-disposal-before" || delivery === "warm-readback-canonical") {
              assert.equal(continued.readyRecipient?.generation, initial.readyRecipient?.generation,
                "absent ACK or stale canonical identity cannot publish successor readiness");
            } else {
              assert.equal(continued.readyRecipient?.generation, "2");
              assert.ok(warmProofReads >= 2, "exact proof is rechecked across the await");
              assert.ok(nativeProtectionReads > 0, "real independent receipt inspection holds cleanup");
            }
            const retained = store.list();
            await hint(); await hint();
            assert.deepEqual(store.list(), retained, "duplicate hints do not reset progress or grants");
            assert.equal(readFileSync(journalOptions.path, "utf8"), sourceBytes, "warm continuation is source readback only");
            assert.deepEqual(journal.inspectSourceCompletion(scope), warmRetainedProof, "the scoped ACK stays immutable");
            assert.deepEqual(journal.read().entries.find(value => value.updateId === 100), sourceBefore);
            assert.equal(readFileSync(independentPath, "utf8"), independentImage, "independent whole receipt and originals remain exact");
            if (recipientBytes !== undefined) assert.equal(readFileSync(`${path}.recipient`, "utf8"), recipientBytes);
            assert.equal(JSON.stringify(telegramQueueStore.getQueuedItems()), acceptedQueue);
            await click("reroutenew:1:7"); await routeRuntime.waitForRestoreSettlement();
            if (delivery === "warm-disposal-before") {
              const inspected = store.list()[0]!;
              assert.equal(inspected.readyRecipient?.generation, "2", "re-click may inspect readiness without a terminal ACK");
              assert.deepEqual(inspected.request, continued.request);
              assert.deepEqual(inspected.recipient, continued.recipient);
              assert.deepEqual(inspected.routing, continued.routing, "readiness is not source disposition or cleanup");
            } else assert.deepEqual(store.list(), retained, "re-click cannot repeat apply, delivery or disposal");
            assert.deepEqual(journal.inspectSourceCompletion(scope), warmRetainedProof);
            assert.deepEqual(journal.read().entries.find(value => value.updateId === 100), sourceBefore);
            assert.equal(readFileSync(independentPath, "utf8"), independentImage);
            if (recipientBytes !== undefined) assert.equal(readFileSync(`${path}.recipient`, "utf8"), recipientBytes);
            assert.equal(JSON.stringify(telegramQueueStore.getQueuedItems()), acceptedQueue);
            assert.deepEqual(threadStore.listWorkspaceBindings().filter(value => value.bindingKey !== binding.bindingKey), siblings);
            assert.equal(threadStore.listWorkspaceBindings().length, 26);
            if (delivery !== "warm-readback-canonical") {
              assert.deepEqual(threadStore.listWorkspaceBindings().find(value => value.bindingKey === binding.bindingKey)?.target, target);
              assert.equal(threadStore.listWorkspaceBindings().find(value => value.bindingKey === binding.bindingKey)?.slot, "A");
            }
            assert.equal(calls.filter(value => value === "apply").length, 1);
            assert.equal(calls.filter(value => value === "forward").length, role === "follower" ? 1 : 0);
            assert.equal(role === "leader" ? scopedDisposals : warmSourceDisposals, 1, "warm recovery never retries issued disposal");
            assert.equal(calls.includes("closeForumTopic") || calls.includes("deleteForumTopic") || calls.includes("createForumTopic"), false);
            assert.deepEqual(admission.read().leases, []);
            assert.deepEqual(references.list(), []);
            return;
          }
          if (forwardProofCases.includes(delivery)) {
            const proofPublished = ["acceptance-after-rename", "source-removal-before-write", "source-removal-lost-ack", "source-cas-changed"].includes(delivery);
            const sourceRemoved = delivery === "acceptance-after-rename" || delivery === "source-removal-lost-ack";
            const coldThreads = Threads.createTelegramTopicTargetStore({ path }); await coldThreads.load();
            const coldRestore = coldThreads.workspaceRestore({ profileName: "default", tokenSha256: "a".repeat(64) });
            const retained = coldRestore.list();
            assert.equal(retained[0]?.phase, "ready");
            assert.equal(retained[0]?.routing?.acceptances?.length ?? 0, proofPublished ? 1 : 0);
            assert.equal(retained[0]?.routing?.settlements.length, delivery === "acceptance-after-rename" ? 1 : 0);
            assert.equal(retained[0]?.routing?.cleanup, undefined);
            const acceptance = retained[0]?.routing?.acceptances?.[0];
            if (acceptance) {
              const completion = { updateId: acceptance.updateId, sourceSha256: acceptance.sourceSha256,
                completionSha256: Threads.getTelegramWorkspaceRestoreSourceCompletionSha256(retained[0]!, acceptance) };
              assert.deepEqual(resolveJournal()!.journal.inspectSourceCompletion(completion), sourceRemoved ? completion : undefined,
                "cold native binding observes durable disposal only when the exact journal publication succeeded");
            }
            const original = journal.read().entries.find(entry => entry.updateId === 100);
            assert.equal(original?.state, sourceRemoved ? undefined : delivery === "acceptance-source-changed" ? "queued" : delivery === "source-cas-changed" ? "retry-wait" : "pending");
            if (original?.state === "queued") assert.equal(original.queueOwner?.instanceId, "independent");
            assert.equal(acceptancePublications, delivery === "acceptance-source-changed" ? 0 : 1);
            assert.equal(calls.filter(value => value === "forward").length, 1);
            assert.equal(calls.filter(value => value === "apply").length, 1);
            const recipientBefore = readFileSync(`${path}.recipient`, "utf8");
            assert.equal(recipientJournal.read().entries[0]?.state, "pending", "positive acceptance preserves recipient custody");
            await click("reroutenew:1:7"); await routeRuntime.waitForRestoreSettlement();
            assert.deepEqual(store.list(), retained, "reclick cannot repeat apply/forwarding or infer disposition from acceptance");
            assert.equal(readFileSync(`${path}.recipient`, "utf8"), recipientBefore);
            assert.equal(calls.filter(value => value === "forward").length, 1);
            assert.equal(calls.includes("closeForumTopic") || calls.includes("deleteForumTopic"), false);
            assert.deepEqual(admission.read().leases, []);
            return;
          }
          if (queuedTerminalCases.includes(delivery)) {
            const mixed = queuedTerminalMixedCases.includes(delivery);
            if (mixed) {
              journal.appendBatch([{ update_id: 200, message: { ...unboundTopicUpdate().message!, message_id: 200,
                chat: { id: 7, type: "private" }, text: "Independent ordinary input" } }]);
              worker.signal(); await worker.waitForDrain(); await routeRuntime.waitForRestoreSettlement();
            }
            const turns = telegramQueueStore.getQueuedItems();
            const turn = mixed ? { admissionReceipts: turns.flatMap(value => value.admissionReceipts ?? []) } : turns[0]!;
            const settlement = Updates.createTelegramQueueAdmissionSettlementRuntime(worker);
            const mux = Updates.createTelegramQueueAdmissionSettlementMuxRuntime([settlement]);
            assert.equal(mux.isItemReady(turn), true);
            assert.deepEqual(store.list()[0]!.routing!.settlements.map(value => value.kind), ["queued"]);
            const complete = () => delivery === "queue-terminal-discard" ? mux.onItemsDiscarded([turn], ctx) : mux.onPromptHandedOff(turn, ctx);
            const uncertain = delivery === "queue-terminal-lost" || delivery === "queue-terminal-reader" || delivery === "queue-terminal-mixed-lost";
            assert.equal(complete(), !uncertain, "the production lifecycle caller supplies no explicit source scopes");
            await routeRuntime.waitForRestoreSettlement();
            assert.equal(scopedDisposals, 1);
            assert.equal(journal.read().entries.some(value => sourceIds.includes(value.updateId)), false);
            for (const acceptance of store.list()[0]!.routing!.acceptances!) {
              const scope = { updateId: acceptance.updateId, sourceSha256: acceptance.sourceSha256,
                completionSha256: Threads.getTelegramWorkspaceRestoreSourceCompletionSha256(store.list()[0]!, acceptance) };
              assert.deepEqual(journal.inspectSourceCompletion(scope), scope);
            }
            assert.equal(terminalHints, uncertain ? 0 : 1);
            assert.deepEqual(store.list()[0]!.routing!.settlements.map(value => value.kind),
              uncertain || terminalPublicationUnavailable ? ["queued"] : ["queue-completed"]);
            const disposedBytes = readFileSync(journalOptions.path, "utf8");
            if (uncertain) {
              assert.equal(mux.isItemReady(turn), false, "issued disposal reconciliation is never dispatch readiness");
              assert.equal(mux.onPromptHandedOff(turn, { cwd: "/foreign-context" }), false, "completion-only selection cannot borrow another context");
              assert.equal(mux.onItemsDiscarded([turn], ctx), false, "an issued handoff attempt cannot become a discard grant");
              if (mixed) assert.equal(journal.read().entries.some(value => value.updateId === 200 && value.state === "queued"), true,
                "uncertain scoped disposal preserves the independent ordinary receipt");
              terminalReaderUnavailable = false;
              assert.equal(complete(), true, "completion-only owner selection permits exact retained proof reconciliation");
              await routeRuntime.waitForRestoreSettlement();
              assert.equal(terminalHints, 1);
            }
            if (terminalPublicationUnavailable) {
              terminalPublicationUnavailable = false;
              routeRuntime.onUpdateCompleted(999, ctx, journalBindingKey);
              await routeRuntime.waitForRestoreSettlement();
            }
            assert.equal(scopedDisposals, 1, "no lost reply can replay an issued receipt disposal");
            if (!mixed || !uncertain) assert.equal(readFileSync(journalOptions.path, "utf8"), disposedBytes);
            if (mixed) {
              assert.equal(journal.read().entries.some(value => value.updateId === 200), false);
              assert.equal(journal.inspectSourceCompletion({ updateId: 200, sourceSha256: "f".repeat(64), completionSha256: "f".repeat(64) }), undefined,
                "ordinary lifecycle completion never invents a Restore marker");
            }
            assert.deepEqual(store.list()[0]!.routing!.settlements.map(value => [value.kind, value.updateIds]), [["queue-completed", sourceIds]]);
            assert.equal(worker.getState().queuedClaimCount, 0);
            assert.equal(inboundGroups, grouped ? 1 : 0);
            assert.equal(telegramQueueStore.getQueuedItems().length, mixed ? 2 : 1, "this fixture supplies lifecycle handoff, not actual Pi execution");
            assert.equal(calls.filter(value => value === "apply").length, 1);
            assert.equal(calls.includes("closeForumTopic") || calls.includes("deleteForumTopic"), false, "unknown accepted-work clearance still protects the old target");
            assert.equal(store.list()[0]!.routing!.cleanup, undefined);
            assert.deepEqual(admission.read().leases, []);
            return;
          }
          if (grouped) {
            const ready = delivery === "queue-group" || delivery === "queue-group-after";
            const partial = ["queue-group-before", "queue-group-missing", "queue-group-cold"].includes(delivery);
            const sources = journal.read().entries.filter(value => sourceIds.includes(value.updateId));
            assert.deepEqual(sources.map(value => [value.updateId, value.state]), sourceIds.map(id => [id, "queued"]));
            const receipt = { journalBindingKey, receiptId: sources[0]!.queueReceiptId!, queueKind: sources[0]!.queueKind!, sourceUpdateIds: sourceIds };
            assert.equal(worker.isQueueReceiptCommitted(receipt), ready, "one missing source proof holds the entire group");
            const retained = store.list()[0]!;
            assert.deepEqual(retained.request.source.updateIds, sourceIds);
            assert.deepEqual(retained.routing?.acceptances?.map(value => value.updateId), partial ? [100] : sourceIds);
            assert.deepEqual(retained.routing?.settlements.flatMap(value => value.updateIds), ready ? sourceIds : []);
            assert.equal(queueObservers, ready ? 1 : 0, "grouped late reports publish readiness only once, after every proof");
            assert.equal(inboundGroups, 1);
            assert.equal(telegramQueueStore.getQueuedItems().length, 1);
            assert.deepEqual(telegramQueueStore.getQueuedItems()[0]?.admissionReceipts?.map(value => value.sourceUpdateIds), [sourceIds]);
            assert.deepEqual(sources[0]!.queueOwner, sources[1]!.queueOwner);
            assert.equal(sources.every(value => !!value.queueHandoff), delivery === "queue-group-offer");
            const originalBytes = readFileSync(journalOptions.path, "utf8"), queueBefore = JSON.stringify(telegramQueueStore.getQueuedItems());
            if (delivery === "queue-group-cold") {
              const firstProof = retained.routing!.acceptances![0]!;
              await worker.stop(); queuePublicationUnavailable = false;
              worker.start(ctx); await worker.waitForDrain(); await routeRuntime.waitForRestoreSettlement();
              assert.equal(worker.isQueueReceiptCommitted(receipt), true);
              assert.deepEqual(store.list()[0]?.routing?.acceptances?.map(value => value.updateId), sourceIds);
              assert.deepEqual(store.list()[0]?.routing?.acceptances?.[0], firstProof, "partial committed evidence survives exact duplicate readback");
              assert.deepEqual(store.list()[0]?.routing?.settlements.flatMap(value => value.updateIds), sourceIds);
              assert.equal(queueObservers, 1);
              assert.equal(readFileSync(journalOptions.path, "utf8"), originalBytes, "reconstruction publishes proof, not journal disposition");
            }
            const canonical = store.list();
            if (delivery === "queue-group-source") queueProofReads = 0;
            await click("reroutenew:1:7"); await routeRuntime.waitForRestoreSettlement();
            assert.deepEqual(store.list(), canonical);
            assert.equal(JSON.stringify(telegramQueueStore.getQueuedItems()), queueBefore);
            assert.equal(inboundGroups, 1, "neither retry nor worker reconstruction reprocesses the album");
            assert.equal(calls.filter(value => value === "apply").length, 1);
            assert.equal(calls.includes("closeForumTopic") || calls.includes("deleteForumTopic") || calls.includes("createForumTopic"), false);
            assert.deepEqual(threadStore.listWorkspaceBindings().find(value => value.bindingKey === binding.bindingKey)?.target, target);
            assert.equal(threadStore.listWorkspaceBindings().find(value => value.bindingKey === binding.bindingKey)?.slot, "A");
            assert.equal(threadStore.listWorkspaceBindings().length, 26, "a grouped Restore preserves full-capacity binding ownership");
            assert.deepEqual(admission.read().leases, []);
            if (ready) assert.deepEqual(failures, []);
            else assert.ok(failures.length > 0, "uncertain group proof emits a diagnostic without readiness");
            assert.ok(failures.every(value => !String(value).includes("Timed out acquiring")));
            return;
          }
          if (queuedProofCases.includes(delivery)) {
            const positive = delivery === "queue-proof" || delivery === "queue-after-rename";
            const source = journal.read().entries.find(value => value.updateId === 100)!;
            assert.equal(source.state, "queued", "accepted local work retains its durable owner on proof failure");
            const receipt = { journalBindingKey, receiptId: source.queueReceiptId!, queueKind: source.queueKind!, sourceUpdateIds: [100] };
            assert.equal(worker.isQueueReceiptCommitted(receipt), positive);
            const published = positive || delivery.startsWith("queue-post-");
            assert.equal(store.list()[0]?.routing?.acceptances?.length ?? 0, published ? 1 : 0);
            assert.deepEqual(store.list()[0]?.routing?.settlements.flatMap(value => value.updateIds), positive ? [100] : []);
            assert.equal(acceptancePublications, ["queue-proof", "queue-before-rename", "queue-after-rename", "queue-no-proof", "queue-cold"].includes(delivery) || delivery.startsWith("queue-post-") ? 1 : 0);
            assert.equal(telegramQueueStore.getQueuedItems().length, 1, "queue acceptance never rolls back local work");
            if (delivery === "queue-cold") {
              const queued = JSON.stringify(telegramQueueStore.getQueuedItems());
              await worker.stop();
              queuePublicationUnavailable = false;
              worker.start(ctx); await worker.waitForDrain(); await routeRuntime.waitForRestoreSettlement();
              assert.equal(worker.isQueueReceiptCommitted(receipt), true);
              assert.equal(store.list()[0]?.routing?.acceptances?.[0]?.kind, "queued");
              assert.deepEqual(store.list()[0]?.routing?.settlements.flatMap(value => value.updateIds), [100]);
              assert.equal(JSON.stringify(telegramQueueStore.getQueuedItems()), queued, "same-process reconstruction never enqueues original again");
            }
            const retained = store.list();
            const before = JSON.stringify(telegramQueueStore.getQueuedItems());
            // Post-publication fault probes leave the accepted proof protected; do not lend changed recipient authority.
            if (delivery === "queue-post-read-missing" || delivery === "queue-post-read-source") queueProofReads = 0;
            await click("reroutenew:1:7"); await routeRuntime.waitForRestoreSettlement();
            assert.deepEqual(store.list(), retained, "reclick never publishes another proof or delivery grant");
            assert.equal(JSON.stringify(telegramQueueStore.getQueuedItems()), before);
            assert.equal(calls.filter(value => value === "apply").length, 1);
            assert.equal(calls.includes("closeForumTopic") || calls.includes("deleteForumTopic"), false);
            assert.deepEqual(admission.read().leases, []);
            return;
          }
          if (delivery === "settlement-publication-interrupted") {
            assert.equal(interruptedPublications, 1);
            await worker.stop();
            const coldThreads = Threads.createTelegramTopicTargetStore({ path });
            await coldThreads.load();
            const coldRestore = coldThreads.workspaceRestore({ profileName: "default", tokenSha256: "a".repeat(64) });
            const retained = coldRestore.list();
            const coldJournal = Journal.createTelegramUpdateJournalStore(journalOptions);
            const source = coldJournal.read().entries.find(entry => entry.updateId === 100);
            assert.equal(source?.state, role === "leader" ? "queued" : undefined);
            assert.equal(retained[0]?.phase, "ready");
            assert.deepEqual(retained[0]?.routing?.settlements, []);
            assert.equal(retained[0]?.routing?.acceptances?.length ?? 0, role === "follower" ? 1 : 0,
              "forwarding acceptance now survives independently of a lost source-removal settlement publication");
            assert.deepEqual(coldThreads.listWorkspaceBindings().find(value => value.bindingKey === binding.bindingKey)?.target, target);
            assert.equal(calls.includes("closeForumTopic") || calls.includes("deleteForumTopic"), false);
            const accepted = role === "leader" ? structuredClone(telegramQueueStore.getQueuedItems()) : recipientJournal.read();
            settlementUnavailable = false;
            // Restart this worker's claims/cache, not the Pi process. Only surviving queue receipts can re-emit proof.
            worker.start(ctx); await worker.waitForDrain(); await routeRuntime.waitForRestoreSettlement();
            await click("reroutenew:1:7"); await routeRuntime.waitForRestoreSettlement();
            assert.equal(store.list()[0]?.routing?.settlements.length, role === "leader" ? 1 : 0,
              "acceptance alone and unrelated callback completion cannot stand in for journal-owner source disposition");
            assert.equal(store.list()[0]?.routing?.cleanup, undefined);
            assert.equal(calls.filter(value => value === "apply").length, 1);
            assert.equal(role === "leader" ? telegramQueueStore.getQueuedItems().length : calls.filter(value => value === "forward").length, 1);
            assert.deepEqual(role === "leader" ? telegramQueueStore.getQueuedItems() : recipientJournal.read(), accepted);
            assert.equal(calls.includes("closeForumTopic") || calls.includes("deleteForumTopic"), false);
            assert.deepEqual(admission.read().leases, []);
            return;
          }
          if (recipientWakeCases.includes(delivery)) {
            assert.equal(store.list()[0]?.routing?.settlements.length, delivery === "recipient-lost-source" ? 0 : 1);
            assert.equal(store.list()[0]?.routing?.cleanup, undefined);
            assert.equal(calls.includes("closeForumTopic") || calls.includes("deleteForumTopic"), false);
            await heartbeat();
            assert.equal(calls.includes("deleteForumTopic"), false, "presence cannot clear durably accepted recipient input");
            assert.equal(recipientJournal.read().entries[0]?.state, "pending");
            // A real recipient worker consumes a deterministic marker; no source-leader completion callback is emitted.
            recipientWorker.start(ctx); await recipientWorker.waitForDrain(); await recipientWorker.stop();
            assert.deepEqual(recipientExecutions, [100]);
            assert.deepEqual(inspectJournal(`${path}.recipient`).entries, []);
            if (delivery === "recipient-held") recipientJournal.appendBatch([{ update_id: 99,
              message: { message_id: 99, message_thread_id: 77, chat: { id: 7, type: "private" },
                from: { id: 7, is_bot: false }, text: "Independent accepted marker" } }]);
            if (delivery === "recipient-stale") followerRegistry.register({ ...followerRegistry.get(instanceId)!, registrationGeneration: "replacement" });
            if (delivery === "recipient-unknown") writeFileSync(`${path}.recipient`, "{broken", { mode: 0o600 });
            await heartbeat();
            if (delivery === "recipient-held") {
              assert.equal(calls.includes("closeForumTopic") || calls.includes("deleteForumTopic"), false);
              assert.equal(inspectJournal(`${path}.recipient`).entries[0]?.updateId, 99);
              recipientWorker.start(ctx); await recipientWorker.waitForDrain(); await recipientWorker.stop();
              assert.deepEqual(recipientExecutions, [100, 99]);
              await heartbeat();
            }
            if (delivery === "recipient-stale" || delivery === "recipient-unknown" || delivery === "recipient-lost-source" || delivery === "recipient-changed" || delivery === "recipient-cleanup-unknown") {
              const retained = structuredClone(store.list());
              const issued = delivery === "recipient-changed" || delivery === "recipient-cleanup-unknown";
              assert.equal(retained[0]?.routing?.cleanup, issued ? "issued" : undefined);
              assert.equal(calls.filter(value => value === "deleteForumTopic").length, delivery === "recipient-cleanup-unknown" ? 1 : 0);
              assert.equal(calls.filter(value => value === "closeForumTopic").length, issued ? 1 : 0);
              if (delivery === "recipient-lost-source") {
                assert.deepEqual(retained[0]?.routing?.settlements, []);
                assert.equal(journal.read().entries.find(entry => entry.updateId === 100)?.state, "pending");
              }
              if (delivery === "recipient-unknown") assert.equal(readFileSync(`${path}.recipient`, "utf8"), "{broken");
              await heartbeat();
              assert.deepEqual(store.list(), retained, "heartbeat cannot invent settlement, adopt a successor or replay issued cleanup");
              assert.equal(calls.filter(value => value === "deleteForumTopic").length, delivery === "recipient-cleanup-unknown" ? 1 : 0);
            } else {
              assert.deepEqual(store.list(), []);
              assert.equal(calls.filter(value => value === "deleteForumTopic").length, 1);
              await heartbeat();
              assert.equal(calls.filter(value => value === "deleteForumTopic").length, 1);
            }
            assert.equal(calls.filter(value => value === "apply").length, 1);
            assert.equal(calls.filter(value => value === "forward").length, 1);
            assert.deepEqual(admission.read().leases, []);
            assert.deepEqual(references.list(), []);
            assert.ok(heartbeatFrames.length > 0);
            assert.ok(heartbeatFrames.every(current => !current()));
            return;
          }
          if (store.list()[0]?.routing?.settlements.some(value => value.kind === "queued")) {
            const retained = structuredClone(store.list());
            assert.equal(retained[0]?.routing?.settlements.length, 1);
            assert.equal(retained[0]?.routing?.settlements[0]?.kind, "queued");
            assert.equal(retained[0]?.routing?.cleanup, undefined, "admitted work is not terminal disposition, even with supplied clearance");
            assert.equal(calls.includes("closeForumTopic") || calls.includes("deleteForumTopic"), false);
            assert.equal(journal.read().entries.find(value => value.updateId === 100)?.state, "queued");
            const accepted = JSON.stringify(telegramQueueStore.getQueuedItems());
            assert.ok(repeatAck); repeatAck();
            routeRuntime.onUpdateCompleted(999, ctx, journalBindingKey); await routeRuntime.waitForRestoreSettlement();
            assert.deepEqual(store.list(), retained, "unrelated completion cannot turn admission into a terminal ACK");
            assert.equal(JSON.stringify(telegramQueueStore.getQueuedItems()), accepted, "accepted recipient work remains intact");
            assert.equal(calls.filter(value => value === "apply").length, 1);
            assert.deepEqual(admission.read().leases, []);
            return;
          }
          if (cleanupWakeCases.includes(delivery)) {
            const retained = structuredClone(store.list());
            assert.equal(retained[0]?.routing?.settlements.length, 1);
            assert.equal(retained[0]?.routing?.cleanup, undefined);
            assert.equal(calls.includes("closeForumTopic") || calls.includes("deleteForumTopic"), false);
            const reads = protectionReads;
            allowCleanupWake = delivery !== "wake-unknown";
            routeRuntime.onQueueReceiptCommitted({ receiptId: "unrelated", queueKind: "prompt", sourceUpdateIds: [999], journalBindingKey }, ctx);
            await routeRuntime.waitForRestoreSettlement();
            assert.equal(protectionReads, reads, "new unrelated queued work is not a completion wake");
            assert.deepEqual(store.list(), retained);
            if (delivery === "wake-foreign" || delivery === "wake-authority") {
              if (delivery === "wake-authority") current = false;
              routeRuntime.onUpdateCompleted(999, ctx, delivery === "wake-foreign" ? "foreign-journal" : journalBindingKey);
              await routeRuntime.waitForRestoreSettlement();
              assert.equal(protectionReads, reads, "a foreign or stale wake cannot inspect or mutate this operation");
              assert.deepEqual(store.list(), retained);
              assert.equal(calls.includes("deleteForumTopic"), false);
              if (delivery === "wake-authority") return;
            }
            // Real worker completion of this later chooser callback is a wake, not original-source evidence.
            await click("reroutenew:1:7");
            await routeRuntime.waitForRestoreSettlement();
            assert.ok(protectionReads > reads);
            assert.equal(calls.filter(value => value === "apply").length, 1);
            assert.equal(role === "leader" ? telegramQueueStore.getQueuedItems().length : calls.filter(value => value === "forward").length, 1);
            if (delivery === "wake-unknown") {
              assert.deepEqual(store.list(), retained, "unknown protection remains nonterminal after a valid wake");
              assert.equal(calls.includes("deleteForumTopic"), false);
            } else {
              assert.deepEqual(store.list(), []);
              assert.equal(calls.filter(value => value === "deleteForumTopic").length, 1);
              assert.ok(repeatAck); repeatAck(); await routeRuntime.waitForRestoreSettlement();
              assert.equal(calls.filter(value => value === "deleteForumTopic").length, 1);
            }
            assert.deepEqual(admission.read().leases, []);
            return;
          }
          if (sessionReferenceCases.includes(delivery)) {
            assert.equal(sessionReferenceBoundaryCompleted, true, "the injected close-await boundary completed without a swallowed fixture assertion");
            assert.equal(threadStore.listWorkspaceBindings().length, 26);
            assert.ok(nativeProtectionReads >= 2, "fresh production address resolution runs before and after close await");
            assert.ok(sessionReferenceObservations.some(observation => observation.afterClose));
            assert.deepEqual(references.list(), []);
            assert.equal(calls.filter(value => value === "closeForumTopic").length, 1);
            assert.equal(calls.filter(value => value === "deleteForumTopic").length, delivery === "session-reference-empty" ? 1 : 0,
              "missing, nonempty or unreadable predecessor evidence must refuse deletion after the close await");
            for (const observation of sessionReferenceObservations) assert.deepEqual(observation.sources, observation.afterClose
              ? [...binding.journalSources!, { sessionId: "successor", recipientBindingKey: "manual:independent" }]
              : binding.journalSources, "exact predecessor/current session addresses survive canonical metadata pruning without duplicates");
            assert.deepEqual(threadStore.listWorkspaceBindings().find(value => value.bindingKey === binding.bindingKey)?.target, target);
            const retained = structuredClone(store.list());
            assert.equal(retained.length, delivery === "session-reference-empty" ? 0 : 1);
            if (retained.length) assert.equal(retained[0]?.routing?.cleanup, "issued");
            const accepted = readFileSync(`${path}.recipient`, "utf8");
            await click("reroutenew:1:7"); await routeRuntime.waitForRestoreSettlement();
            assert.deepEqual(store.list(), retained, "a blocked issued cleanup never repeats after another hint");
            assert.equal(readFileSync(`${path}.recipient`, "utf8"), accepted, "independent recipient custody stays unchanged");
            if (delivery === "session-reference-missing") await assert.rejects(readFile(sessionPaths[0]!), { code: "ENOENT" });
            else assert.equal(readFileSync(sessionPaths[0]!, "utf8"), sessionReferenceImage, "inspection never repairs or disposes predecessor evidence");
            if (ownerBearingJournal) {
              assert.ok(ownerBearingProtection.length > 0);
              for (const protection of ownerBearingProtection) assert.equal(protection,
                delivery === "session-reference-queued-group" ? "protected" : "unknown",
                "Binding-owned work protects irrespective of source target; nonempty discovered/shared custody never gains an exemption");
              assert.equal(readFileSync(ownerBearingPath!, "utf8"), ownerBearingImage, "Protection cannot settle or rewrite independent custody");
              assert.deepEqual(ownerBearingJournal.read().entries, ownerBearingOriginals, "Whole group, owner/acquisition and handoff stay exact");
              assert.ok(ownerBearingOriginals!.every(entry => entry.state === "queued" && (entry.update.message as TestMessage).message_thread_id === 77));
              assert.notEqual(oldTarget.threadId, 77);
              assert.notEqual(target.threadId, 77);
            }
            assert.equal(calls.filter(value => value === "apply").length, 1);
            assert.equal(calls.filter(value => value === "forward").length, 1);
            assert.deepEqual(admission.read().leases, []);
            return;
          }
          if (journalProtectionCases.includes(delivery)) {
            const retained = structuredClone(store.list());
            assert.equal(retained[0]?.routing?.settlements.length, 1);
            assert.equal(retained[0]?.routing?.cleanup, delivery === "journal-late" ? "issued" : undefined);
            assert.equal(calls.includes("deleteForumTopic"), false);
            assert.equal(calls.filter(value => value === "closeForumTopic").length, delivery === "journal-late" ? 1 : 0);
            assert.ok(delivery === "journal-missing" ||
              (protectionReads > 0 && (delivery === "journal-observation-error" || nativeProtectionReads > 0)));
            assert.deepEqual(references.list(), []);
            if (delivery === "journal-unknown") assert.equal(readFileSync(independentPath, "utf8"), "{broken");
            else if (delivery !== "journal-missing" && delivery !== "journal-observation-error") assert.equal(inspectJournal(independentPath).entries[0]?.state, "queued");
            assert.deepEqual(admission.read().leases, []);
            assert.ok(repeatAck); repeatAck(); await routeRuntime.waitForRestoreSettlement();
            assert.deepEqual(store.list(), retained);
            assert.equal(calls.includes("deleteForumTopic"), false, "protection cannot confirm or replay cleanup");
            return;
          }
          if (delivery === "queued-before" || delivery === "queued-during" || delivery === "active-before") {
            const retained = structuredClone(store.list());
            assert.equal(retained[0]?.routing?.settlements.length, 1);
            assert.equal(retained[0]?.routing?.cleanup, delivery === "queued-during" ? "issued" : undefined);
            assert.equal(calls.includes("deleteForumTopic"), false);
            assert.equal(calls.filter(value => value === "closeForumTopic").length, delivery === "queued-during" ? 1 : 0);
            assert.deepEqual(delivery === "active-before" ? activeTurnRuntime.get() :
              telegramQueueStore.getQueuedItems().find(value => value === independentTurn), independentTurn,
              "independent accepted work remains untouched");
            if (delivery === "queued-during") telegramQueueStore.setQueuedItems(telegramQueueStore.getQueuedItems().filter(value => value !== independentTurn));
            assert.ok(repeatAck); repeatAck(); await routeRuntime.waitForRestoreSettlement();
            assert.deepEqual(store.list(), retained, "protection is not deletion evidence or permission to retry an issued grant");
            assert.equal(calls.includes("deleteForumTopic"), false);
            return;
          }
          if (warmCleanupCases.includes(delivery) || delivery === "warm-cleanup-registration") {
            assert.equal(warmCleanupInterrupted, true, "the fence changes inside the actual close await");
            const retained = structuredClone(store.list());
            assert.equal(retained.length, 1);
            assert.equal(retained[0]?.routing?.cleanup, "issued");
            assert.equal(retained[0]?.routing?.settlements.length, 1);
            assert.equal(calls.filter(value => value === "closeForumTopic").length, 1);
            assert.equal(calls.includes("deleteForumTopic"), false, "post-await fence prevents the second transport effect");
            const accepted = structuredClone(role === "leader" ? journal.read().entries : recipientJournal.read().entries);
            assert.equal(accepted[0]?.state, role === "leader" ? "queued" : "pending");
            const relocated = readFileSync(path, "utf8");
            contextActive = true; generation = 1; epoch = "epoch";
            if (delivery === "warm-cleanup-registration") followerRegistry.register({ ...followerRegistry.get(instanceId)!, registrationGeneration: "registered" });
            assert.ok(repeatAck);
            repeatAck(); await routeRuntime.waitForRestoreSettlement();
            await click("reroutenew:1:7");
            assert.deepEqual(store.list(), retained, "regained warm authority cannot retire or replay issued cleanup");
            assert.equal(readFileSync(path, "utf8"), relocated, "hints and re-click leave canonical publication unchanged");
            assert.deepEqual(role === "leader" ? journal.read().entries.filter(entry => entry.updateId === 100) : recipientJournal.read().entries, accepted);
            assert.equal(readFileSync(independentPath, "utf8"), independentImage, "independent receipt and whole owner remain byte-exact");
            assert.equal(calls.filter(value => value === "apply").length, 1);
            assert.equal(calls.filter(value => value === "forward").length, role === "follower" ? 1 : 0);
            assert.equal(calls.filter(value => value === "closeForumTopic").length, 1);
            assert.equal(calls.includes("deleteForumTopic") || calls.includes("createForumTopic"), false);
            assert.deepEqual(admission.read().leases, []);
            return;
          }
          if (delivery === "cleanup-unknown" || delivery === "authority-ended") {
            const retained = structuredClone(store.list());
            assert.equal(retained[0]?.routing?.cleanup, delivery === "cleanup-unknown" ? "issued" : undefined);
            assert.equal(retained[0]?.routing?.settlements.length, delivery === "cleanup-unknown" ? 1 : 0);
            assert.equal(calls.filter(value => value === "deleteForumTopic").length, delivery === "cleanup-unknown" ? 1 : 0);
            assert.equal(role === "leader" ? journal.read().entries[0]?.state : recipientJournal.read().entries[0]?.state,
              role === "leader" ? "queued" : "pending", "accepted work is untouched");
            assert.ok(repeatAck);
            repeatAck();
            await routeRuntime.waitForRestoreSettlement();
            assert.deepEqual(store.list(), retained, "duplicate or stale proof cannot replay cleanup");
            assert.equal(calls.filter(value => value === "deleteForumTopic").length, delivery === "cleanup-unknown" ? 1 : 0);
            return;
          }
          assert.equal(calls.filter(value => value === "apply").length, 1);
          if (role === "follower") assert.equal(calls.filter(value => value === "inspect").length, lostReply ? 1 : 0);
          assert.equal(new Set(operationIds).size, 1);
          assert.deepEqual(store.list(), []);
          assert.deepEqual(failures, []);
          assert.equal(threadStore.listWorkspaceBindings().find(value => value.bindingKey === binding.bindingKey)?.slot, "A");
          assert.deepEqual(threadStore.listWorkspaceBindings().find(value => value.bindingKey === binding.bindingKey)?.target, target);
          assert.equal(threadStore.list().find(value => value.instanceId === instanceId)?.slot, "A");
          assert.equal(threadStore.listWorkspaceBindings().length, 26);
          const cold = Threads.createTelegramTopicTargetStore({ path });
          await cold.load();
          assert.deepEqual(cold.listWorkspaceBindings().find(value => value.bindingKey === binding.bindingKey)?.target, target);
          assert.equal(cold.list().find(value => value.instanceId === instanceId)?.slot, "A");
          assert.equal(calls.includes("createForumTopic"), false);
          assert.equal(calls.filter(value => value === "deleteForumTopic").length, 1, "fresh-admission cleanup executes once");
          assert.equal(role === "leader" ? telegramQueueStore.getQueuedItems().length : calls.filter(value => value === "forward").length, 1);
          assert.equal(journal.read().entries.length, role === "leader" ? 1 : 0);
          if (role === "follower") {
            assert.deepEqual(recipientJournal.read().entries.map(entry => ({ id: entry.updateId, state: entry.state })),
              [{ id: 100, state: "pending" }], "retiring source authority preserves durably accepted follower work");
          }
          await click("reroutenew:1:7");
          assert.equal(operationIds.length, role === "follower" && lostReply ? 2 : 1, "completed chooser cannot dispatch again");
        } finally {
          await worker.stop(); await recipientWorker.stop(); await receiver.stop();
          if (recipientWakeCases.includes(delivery)) await heartbeatRuntime.stopPolling();
          if (!grouped && queuedProofCases.includes(delivery) && !queuedTerminalCases.includes(delivery) && !["queue-proof", "queue-after-rename"].includes(delivery)) {
            assert.ok(failures.length > 0, "uncertain acceptance suppresses readiness and records a diagnostic");
            assert.ok(failures.every(value => !String(value).includes("Timed out acquiring")), "fresh admission never reenters canonical storage");
          } else if (delivery === "registry-recovery") {
            assert.equal(failures.length, 1);
            assert.match(String(failures[0]), /Protected Workspace Restore provisioning recovery conflict/);
          } else if (delivery === "source-removal-before-write" || delivery === "source-removal-lost-ack") {
            assert.ok(failures.some(value => String(value).includes("Fixture source removal")));
          } else if (delivery === "queue-terminal-lost" || delivery === "queue-terminal-reader" || delivery === "queue-terminal-mixed-lost") {
            assert.equal(failures.length, 1);
            assert.match(String(failures[0]), /queued disposal ACK lost|ACK was not retained/);
          } else if (delivery === "source-cas-changed") {
            assert.ok(failures.some(value => String(value).includes("exact completion source changed")));
          } else if (warmDisposal) {
            assert.equal(failures.length, 1);
            assert.match(String(failures[0]), /Fixture warm .*disposal|Fixture source removal ACK lost/);
          } else if (!grouped) assert.deepEqual(failures, []);
        }
      });
    });
    }
  }
}

test("Retained Workspace fence blocks reroute replacement before store or API mutation", async () => {
  for (const phase of ["fenced", "deletion-issued"] as const) {
    await withTopicStore(async (threadStore) => {
      const admissionDir = await mkdtemp(join(tmpdir(), "pi-telegram-routing-admission-"));
      try {
        const admission = createTelegramWorkspaceAdmissionLedger({
          path: join(admissionDir, "workspace-admission.json"),
          profileKey: "profile:routing",
          owner: {
            processId: process.pid,
            processBirthId: `${process.pid}:routing-admission-test`,
          },
          getProcessLiveness: () => "alive",
        });
        const operationRuntime = createTelegramWorkspaceOperationRuntime({
          getWorkspaceAdmission: () => admission,
        });
        threadStore.upsert({
          profileKey: "cwd:/repo",
          owner: { kind: "leader", cwd: "/repo", instanceId: "leader-a" },
          target: { chatId: 100, threadId: 7 },
          status: "active",
          createdAtMs: 1000,
          updatedAtMs: 1000,
          instanceId: "leader-a",
          slot: "A",
          threadName: "Coral",
          rerouteConfirmedAtMs: 1500,
        });
        await threadStore.persist();
        const apiCalls: unknown[] = [];
        const leaderIdentities: unknown[] = [];
        const { events, routeRuntime, telegramQueueStore } = createRouteHarness({
          threadStore,
          runWorkspaceOperation: operationRuntime.run,
          setCurrentLeaderIdentity: (identity) => {
            leaderIdentities.push(identity);
          },
          callApi: async (method, body) => {
            apiCalls.push({ method, body });
            return {} as never;
          },
        });
        await routeRuntime.handleUpdate(unboundTopicUpdate(), { cwd: "/repo" });
        await routeRuntime.handleUpdate(
          {
            callback_query: {
              id: `restore-menu-${phase}`,
              from: { id: 7, is_bot: false },
              message: {
                message_id: 99,
                message_thread_id: 42,
                chat: { id: 100, type: "private" },
              },
              data: "rerouterestore:1",
            },
          },
          { cwd: "/repo" },
        );
        assert.match(
          events.filter((event) => event.startsWith("markup:")).at(-1) ?? "",
          /"callback_data":"reroutenew:1:7"/u,
        );
        const acquired = admission.acquireRetirementFence({
          operationId: `routing-fence:${phase}`,
          retirementIntentId: `routing-intent:${phase}`,
          bindingKey: "routing-binding",
          slot: "A",
          target: { chatId: 100, threadId: 7 },
          leaderEpoch: 1,
          retirementRequestedAtMs: 1,
        });
        assert.equal(acquired.kind, "acquired");
        if (acquired.kind !== "acquired") return;
        if (phase === "deletion-issued") {
          assert.equal(admission.issueDeletionPermit(acquired.fence).kind, "issued");
        }
        const callsBeforeBlockedRestore = apiCalls.length;

        await assert.rejects(
          () => routeRuntime.handleUpdate(
            {
              callback_query: {
                id: `reroute-${phase}`,
                from: { id: 7, is_bot: false },
                message: {
                  message_id: 99,
                  message_thread_id: 42,
                  chat: { id: 100, type: "private" },
                },
                data: "reroutenew:1:7",
              },
            },
            { cwd: "/repo" },
          ),
          (error) => error instanceof TelegramWorkspaceAdmissionError &&
            error.code === "admission-blocked",
        );
        assert.deepEqual(
          threadStore.getByProfileKey("cwd:/repo")?.target,
          { chatId: 100, threadId: 7 },
        );
        assert.equal(threadStore.getByProfileKey("cwd:/repo")?.slot, "A");
        assert.equal(apiCalls.length, callsBeforeBlockedRestore);
        assert.deepEqual(leaderIdentities, []);
        assert.equal(telegramQueueStore.getQueuedItems().length, 0);
      } finally {
        await rm(admissionDir, { recursive: true, force: true });
      }
    });
  }
});

test("Retained Workspace fence blocks unbound target adoption before store mutation", async () => {
  for (const phase of ["fenced", "deletion-issued"] as const) {
    await withTopicStore(async (threadStore) => {
      const admissionDir = await mkdtemp(join(tmpdir(), "pi-telegram-unbound-admission-"));
      try {
        const admission = createTelegramWorkspaceAdmissionLedger({
          path: join(admissionDir, "workspace-admission.json"),
          profileKey: "profile:unbound-routing",
          owner: {
            processId: process.pid,
            processBirthId: `${process.pid}:unbound-routing-test`,
          },
          getProcessLiveness: () => "alive",
        });
        const acquired = admission.acquireRetirementFence({
          operationId: `unbound-fence:${phase}`,
          retirementIntentId: `unbound-intent:${phase}`,
          bindingKey: "unbound-binding",
          slot: "A",
          target: { chatId: 100, threadId: 7 },
          leaderEpoch: 1,
          retirementRequestedAtMs: 1,
        });
        assert.equal(acquired.kind, "acquired");
        if (acquired.kind !== "acquired") return;
        if (phase === "deletion-issued") {
          assert.equal(admission.issueDeletionPermit(acquired.fence).kind, "issued");
        }
        const operationRuntime = createTelegramWorkspaceOperationRuntime({
          getWorkspaceAdmission: () => admission,
        });
        let apiCalls = 0;
        const { events, routeRuntime, telegramQueueStore } = createRouteHarness({
          threadStore,
          runWorkspaceOperation: operationRuntime.run,
          callApi: async <TResponse>() => {
            apiCalls += 1;
            return { ok: true } as TResponse;
          },
        });

        await assert.rejects(
          () => routeRuntime.handleUpdate(unboundTopicUpdate(), { cwd: "/repo" }),
          (error) => error instanceof TelegramWorkspaceAdmissionError &&
            error.code === "admission-blocked",
        );
        assert.deepEqual(threadStore.list(), []);
        assert.deepEqual(threadStore.listReservations(), []);
        assert.equal(apiCalls, 0);
        assert.equal(telegramQueueStore.getQueuedItems().length, 0);
        assert.deepEqual(events, []);
      } finally {
        await rm(admissionDir, { recursive: true, force: true });
      }
    });
  }
});

test("Routing runtime reclaims unbound prompt without visible rename when current leader target is stale", async () => {
  await withTopicStore(async (threadStore) => {
    const apiCalls: unknown[] = [];
    threadStore.upsert({
      profileKey: "cwd:/repo",
      owner: { kind: "leader", cwd: "/repo", instanceId: "leader-a" },
      target: { chatId: 100, threadId: 7 },
      status: "active",
      createdAtMs: 1000,
      updatedAtMs: 1000,
      instanceId: "leader-a",
      slot: "A",
      threadName: "Axial",
    });
    await threadStore.persist();
    const { events, routeRuntime, telegramQueueStore } = createRouteHarness({
      threadStore,
      callApi: async (method, body) => {
        apiCalls.push({ method, body });
        if (method === "sendChatAction") {
          throw new Error("Telegram API sendChatAction failed: HTTP 400: Bad Request: message thread not found");
        }
        return {} as never;
      },
    });

    await routeRuntime.handleUpdate(unboundTopicUpdate(), { cwd: "/repo" });

    const record = threadStore.getByProfileKey("cwd:/repo");
    assert.deepEqual(record?.target, { chatId: 100, threadId: 42 });
    assert.equal(record?.threadName, "Axial");
    const queued = telegramQueueStore.getQueuedItems()[0];
    assert.equal(queued?.kind, "prompt");
    assert.deepEqual(queued?.target, { chatId: 100, threadId: 42 });
    assert.equal(
      queued?.kind === "prompt" && queued.content[0]?.type === "text"
        ? queued.content[0].text
        : "",
      "[telegram|thread:Axial] hello",
    );
    assert.deepEqual(apiCalls, [
      {
        method: "sendChatAction",
        body: { chat_id: 100, message_thread_id: 7, action: "typing" },
      },
    ]);
    assert.equal(events.some((event) => event.startsWith("interactive:")), false);
    assert.equal(events.some((event) => event.includes("closeForumTopic")), false);
    assert.equal(events.some((event) => event.includes("deleteForumTopic")), false);
  });
});

test("Routing runtime assigns internal baked name when reclaiming unnamed stale current leader target", async () => {
  await withTopicStore(async (threadStore) => {
    const apiCalls: unknown[] = [];
    threadStore.upsert({
      profileKey: "cwd:/repo",
      owner: { kind: "leader", cwd: "/repo", instanceId: "leader-a" },
      target: { chatId: 100, threadId: 7 },
      status: "active",
      createdAtMs: 1000,
      updatedAtMs: 1000,
      instanceId: "leader-a",
      slot: "A",
    });
    await threadStore.persist();
    const { routeRuntime, telegramQueueStore } = createRouteHarness({
      threadStore,
      callApi: async (method, body) => {
        apiCalls.push({ method, body });
        if (method === "sendChatAction") {
          throw new Error("Telegram API sendChatAction failed: HTTP 400: Bad Request: message thread not found");
        }
        return {} as never;
      },
    });

    await routeRuntime.handleUpdate(unboundTopicUpdate(), { cwd: "/repo" });

    const record = threadStore.getByProfileKey("cwd:/repo");
    assert.deepEqual(record?.target, { chatId: 100, threadId: 42 });
    assert.equal(record?.threadName, "Anchor");
    const queued = telegramQueueStore.getQueuedItems()[0];
    assert.equal(
      queued?.kind === "prompt" && queued.content[0]?.type === "text"
        ? queued.content[0].text
        : "",
      "[telegram|thread:Anchor] hello",
    );
    assert.deepEqual(apiCalls, [
      {
        method: "sendChatAction",
        body: { chat_id: 100, message_thread_id: 7, action: "typing" },
      },
    ]);
  });
});

test("Routing runtime forwards without rebinding a selected leader identity from a prior runtime", async () => {
  await withTopicStore(async (threadStore) => {
    const apiCalls: unknown[] = [];
    threadStore.upsert({
      profileKey: "cwd:/repo",
      owner: { kind: "leader", cwd: "/repo", instanceId: "old-leader" },
      target: { chatId: 100, threadId: 7 },
      status: "active",
      createdAtMs: 1000,
      updatedAtMs: 1000,
      instanceId: "old-leader",
      slot: "A",
      threadName: "Axial",
    });
    await threadStore.persist();
    let chatActionCalls = 0;
    const { events, routeRuntime, telegramQueueStore } = createRouteHarness({
      threadStore,
      callApi: async (method, body) => {
        apiCalls.push({ method, body });
        if (method === "sendChatAction") {
          chatActionCalls += 1;
          if (chatActionCalls > 1) {
            throw new Error("Telegram API sendChatAction failed: HTTP 400: Bad Request: message thread not found");
          }
        }
        return {} as never;
      },
    });

    await routeRuntime.handleUpdate(unboundTopicUpdate(), { cwd: "/repo" });
    await routeRuntime.handleUpdate(
      {
        callback_query: {
          id: "reroute-cb",
          from: { id: 7, is_bot: false },
          message: {
            message_id: 99,
            message_thread_id: 42,
            chat: { id: 100, type: "private" },
          },
          data: "reroute:1:7",
        },
      },
      { cwd: "/repo" },
    );

    const record = threadStore.getByProfileKey("cwd:/repo");
    assert.deepEqual(record?.target, { chatId: 100, threadId: 7 });
    assert.equal(record?.threadName, "Axial");
    const queued = telegramQueueStore.getQueuedItems()[0];
    assert.equal(queued?.kind, "prompt");
    assert.deepEqual(queued?.target, { chatId: 100, threadId: 7 });
    assert.equal(
      queued?.kind === "prompt" && queued.content[0]?.type === "text"
        ? queued.content[0].text
        : "",
      "[telegram] hello",
    );
    assert.deepEqual(apiCalls, [
      {
        method: "sendChatAction",
        body: { chat_id: 100, message_thread_id: 7, action: "typing" },
      },
      {
        method: "closeForumTopic",
        body: { chat_id: 100, message_thread_id: 42 },
      },
      {
        method: "deleteForumTopic",
        body: { chat_id: 100, message_thread_id: 42 },
      },
    ]);
    assert.equal(events.includes("delete-message:100:99"), true);
  });
});

test("Routing runtime defers unbound guidance until user content in created topics", async () => {
  await withTopicStore(async (threadStore) => {
    const apiCalls: Array<{ method: string; body: Record<string, unknown> }> = [];
    threadStore.upsert({
      profileKey: "cwd:/repo",
      target: { chatId: 100, threadId: 7 },
      status: "active",
      createdAtMs: 1000,
      updatedAtMs: 1000,
      instanceId: "leader-a",
      slot: "A",
    });
    await threadStore.persist();
    const { events, routeRuntime } = createRouteHarness({
      threadStore,
      callApi: async (method, body) => {
        apiCalls.push({ method, body });
        return {} as never;
      },
    });

    await routeRuntime.handleUpdate(
      {
        message: {
          message_id: 10,
          message_thread_id: 42,
          chat: { id: 100, type: "private" },
          from: { id: 7, is_bot: false },
          forum_topic_created: {},
        },
      },
      { cwd: "/repo" },
    );

    assert.deepEqual(apiCalls, []);

    await routeRuntime.handleUpdate(unboundTopicUpdate("reroute me"), {
      cwd: "/repo",
    });

    const chooser = events.find((event) => event.startsWith("interactive:"));
    assert.match(chooser ?? "", /Choose where to send your message/);
    assert.match(chooser ?? "", /Select the Pi thread that should handle it, or restore a Pi into this tab:/);
    assert.doesNotMatch(chooser ?? "", /<code>active<\/code>/);
  });
});

test("Routing runtime keeps known-command unbound threads open with chooser", async () => {
  await withTopicStore(async (threadStore) => {
    const apiCalls: unknown[] = [];
    threadStore.upsert({
      profileKey: "cwd:/repo",
      target: { chatId: 100, threadId: 7 },
      status: "active",
      createdAtMs: 1000,
      updatedAtMs: 1000,
      instanceId: "leader-a",
      slot: "A",
      threadName: "Axial",
    });
    await threadStore.persist();
    const { events, routeRuntime, telegramQueueStore } = createRouteHarness({
      threadStore,
      callApi: async (method, body) => {
        apiCalls.push({ method, body });
        return {} as never;
      },
    });

    await routeRuntime.handleUpdate(unboundTopicUpdate("/status"), {
      cwd: "/repo",
    });

    assert.deepEqual(telegramQueueStore.getQueuedItems(), []);
    const chooser = events.find((event) => event.startsWith("interactive:"));
    assert.match(chooser ?? "", /<b>🧵 Choose target thread:<\/b>/);
    assert.match(chooser ?? "", /You used <code>\/status<\/code> from the <b>All<\/b> tab\./);
    assert.doesNotMatch(chooser ?? "", /New thread is not a Pi instance/);
    const options = events.find((event) => event.startsWith("interactive-options:"));
    assert.equal(
      options,
      'interactive-options:{"target":{"chatId":100,"threadId":42},"replyToMessageId":11}',
    );
    const root = events.find((event) => event.startsWith("markup:"));
    assert.match(root ?? "", /reroutemenu:1/);
    assert.match(root ?? "", /rerouterestore:1/);
    const markup = await openRerouteSubmenu(routeRuntime, events);
    assert.match(markup, /"text":"↪️ Axial"/);
    assert.match(markup, /reroute:1:7/);
    assert.deepEqual(apiCalls, [{ method: "editForumTopic", body: { chat_id: 100, message_thread_id: 42, name: "/status" } }]);
  });
});

test("Routing runtime skips stale-epoch unbound thread deletion", async () => {
  await withTopicStore(async (threadStore) => {
    const apiCalls: unknown[] = [];
    let epochReads = 0;
    threadStore.upsert({
      profileKey: "cwd:/repo",
      target: { chatId: 100, threadId: 7 },
      status: "active",
      createdAtMs: 1000,
      updatedAtMs: 1000,
      instanceId: "leader-a",
      slot: "A",
    });
    await threadStore.persist();
    const { routeRuntime } = createRouteHarness({
      threadStore,
      getCurrentLeaderEpoch: () => {
        epochReads += 1;
        return epochReads === 1 ? 1 : 2;
      },
      callApi: async (method, body) => {
        apiCalls.push({ method, body });
        return {} as never;
      },
    });

    await routeRuntime.handleUpdate(unboundTopicUpdate(), { cwd: "/repo" });

    assert.deepEqual(apiCalls, [
      {
        method: "sendChatAction",
        body: { chat_id: 100, message_thread_id: 7, action: "typing" },
      },
    ]);
  });
});

test("Routing runtime deletes reserved old leader topics through reconciler", async () => {
  await withTopicStore(async (threadStore) => {
    const apiCalls: unknown[] = [];
    threadStore.upsert({
      profileKey: "cwd:/repo",
      target: { chatId: 100, threadId: 7 },
      status: "active",
      createdAtMs: 1000,
      updatedAtMs: 1000,
      instanceId: "leader-a",
      slot: "A",
    });
    threadStore.reserveThread({
      target: { chatId: 100, threadId: 42 },
      slot: "B",
      reason: "previous-leader",
      createdAtMs: 1000,
      updatedAtMs: 1000,
      expiresAtMs: Date.now() + 60_000,
    });
    await threadStore.persist();
    const { events, routeRuntime, telegramQueueStore } = createRouteHarness({
      threadStore,
      callApi: async (method, body) => {
        apiCalls.push({ method, body });
        return {} as never;
      },
    });

    await routeRuntime.handleUpdate(unboundTopicUpdate(), { cwd: "/repo" });

    assert.deepEqual(apiCalls, [
      {
        method: "closeForumTopic",
        body: { chat_id: 100, message_thread_id: 42 },
      },
      {
        method: "deleteForumTopic",
        body: { chat_id: 100, message_thread_id: 42 },
      },
    ]);
    assert.deepEqual(telegramQueueStore.getQueuedItems(), []);
    assert.equal(
      events.some((event) => event.includes("Previous leader thread")),
      true,
    );
  });
});

test("Routing runtime treats pruned failed topic history as unbound", async () => {
  await withTopicStore(async (threadStore) => {
    threadStore.upsert({
      profileKey: "topic:100:42",
      target: { chatId: 100, threadId: 42 },
      status: "failed",
      createdAtMs: 1000,
      updatedAtMs: 1000,
      slot: "C",
      lastError: "previous failure",
    });
    await threadStore.persist();
    const { events, routeRuntime, telegramQueueStore } = createRouteHarness({
      threadStore,
      callApi: async () => ({}) as never,
    });

    await routeRuntime.handleUpdate(unboundTopicUpdate("again"), {
      cwd: "/repo",
    });

    assert.equal(threadStore.getByProfileKey("topic:100:42"), undefined);
    const leaderRecord = threadStore.getByProfileKey("cwd:/repo");
    assert.equal(leaderRecord?.status, "active");
    assert.equal(leaderRecord?.instanceId, "leader-a");
    assert.equal(telegramQueueStore.getQueuedItems().length, 1);
    assert.equal(events.includes("reply:Starting agent in topic C…"), false);
  });
});


test("Inspect-only recovery never commits a new relocation or applies a target", async () => {
  await fixture(async ({ store, request, auth, path }) => {
    const before = await readFile(path, "utf8").catch(() => undefined);
    const modes: string[] = [];
    const successor = { kind: "leader" as const, instanceId: "next", sessionId: "session", generation: "2" };
    const run = () => advanceTelegramWorkspaceRestore({ request, authority: auth, restoreStore: store, inspectOnly: true,
      getRecipient: () => successor, async runRecipient(action) {
        modes.push(action.mode);
        return { operationId: request.operationId, recipient: successor, target: request.target, slot: "A", ready: true };
      } });
    assert.equal(await run(), undefined);
    assert.deepEqual(store.list(), [], "no retained operation means nothing to recover");
    assert.equal(await readFile(path, "utf8").catch(() => undefined), before);
    await store.commit(request, auth);
    assert.equal((await run())?.phase, "ready");
    assert.deepEqual(modes, ["inspect"], "the first grant is proven by inspection, never applied");
    assert.deepEqual(store.list()[0]?.recipient, successor);
  });
});

test("Restore routing rechecks authority after its final ready observation", async () => {
  await fixture(async ({ store, request, auth }) => {
    let current = true;
    auth.isCurrent = () => current;
    const result = await advanceTelegramWorkspaceRestore({ request, authority: auth,
      restoreStore: { ...store, list() {
        const operations = store.list();
        if (operations[0]?.phase === "ready") current = false;
        return operations;
      } }, getRecipient: () => recipient("leader"),
      async runRecipient() { return { operationId: request.operationId, recipient: recipient("leader"), target: request.target, slot: "A", ready: true }; },
    });
    assert.equal(result, undefined);
    assert.equal(store.list()[0]?.phase, "ready");
    assert.equal(store.list()[0]?.routing, undefined, "a readiness observation never grants source dispatch");
  });
});

for (const phase of ["relocated", "issued", "ready", "routed"] as const) {
  for (const fault of ["none", "adopt-reply", "authority", "operator", "request", "ended-successor"] as const) {
    test(`Restore successor adopts only exact retained authority (${phase}/${fault})`, async () => {
      await fixture(async ({ store, open, request, auth, path }) => {
        let retained = (await store.commit(request, auth))!;
        if (phase !== "relocated") retained = store.issueRecipient(retained, recipient("leader"), auth)!.intent;
        if (phase === "ready" || phase === "routed") retained = store.confirmReady(retained, recipient("leader"), auth)!;
        if (phase === "routed") {
          retained = store.issueRouting(retained, auth)!.intent;
          retained = store.recordSourceSettlement(retained, { ...request.source, kind: "completed" }, auth)!;
          retained = store.issueCleanup(retained, auth)!.intent;
        }
        const old = { ...auth, executor: { ...auth.executor } };
        let current = fault !== "ended-successor";
        const next = { executor: { instanceId: "next-leader", leaderEpoch: "next-epoch" },
          operatorUserId: fault === "operator" ? 8 : 7, isCurrent: () => current };
        let armed = fault === "adopt-reply" || fault === "authority";
        const successor = open({ onPublicationBoundary(boundary) {
          const operation = successor.list()[0];
          if (!armed || boundary !== "after-rename" || operation?.executor.instanceId !== "next-leader" ||
              operation.phase !== retained.phase) return;
          armed = false;
          if (fault === "authority") current = false;
          else throw new Error("Lost adoption publication response");
        } });
        const modes: string[] = [];
        const attempt = () => advanceTelegramWorkspaceRestore({ request: fault === "request"
          ? { ...request, source: { ...request.source, updateIds: [100] } } : request,
          authority: next, restoreStore: successor, getRecipient: () => recipient("leader"),
          async runRecipient(action) {
            modes.push(action.mode);
            assert.equal(action.isCurrent(), true);
            assert.deepEqual(successor.list()[0]?.executor, next.executor, "effects follow durable adoption");
            return { operationId: request.operationId, recipient: recipient("leader"), target: request.target, slot: "A", ready: true };
          } });
        const before = await readFile(path, "utf8");
        const result = await attempt().catch(() => undefined);
        if (fault === "operator" || fault === "request" || fault === "ended-successor") {
          assert.equal(result, undefined);
          assert.equal(await readFile(path, "utf8"), before, "mismatched authority cannot adopt or rewrite evidence");
          assert.deepEqual(modes, []);
          assert.deepEqual(store.list(), [retained]);
          return;
        }
        if (fault === "authority") {
          assert.equal(result, undefined);
          assert.deepEqual(modes, [], "authority lost after adoption cannot reach the recipient");
          assert.deepEqual(store.list(), [{ ...retained, executor: next.executor, revision: retained.revision + 1,
            updatedAtMs: store.list()[0]!.updatedAtMs }]);
          current = true;
        }
        const ready = fault === "authority" ? await attempt() : result;
        assert.equal(ready?.phase, "ready");
        assert.deepEqual(modes, [phase === "relocated" ? "apply" : "inspect"], "a successor never replays an issued apply");
        assert.deepEqual(ready?.executor, next.executor);
        assert.deepEqual(ready?.recipient, recipient("leader"));
        assert.deepEqual(ready?.request, request);
        assert.deepEqual(ready?.routing, retained.routing, "adoption cannot reset or settle routing and cleanup");
        assert.equal(successor.issueRecipient(ready!, recipient("leader"), next), undefined);
        if (phase === "routed") {
          assert.equal(successor.issueRouting(ready!, next), undefined);
          assert.equal(successor.issueCleanup(ready!, next), undefined);
          assert.equal(successor.retire(ready!, next), undefined);
        }
        old.isCurrent = () => true;
        assert.equal(store.confirmReady(retained, recipient("leader"), old), undefined, "the predecessor executor is fenced");
        assert.equal(store.issueRecipient(retained, recipient("leader"), old), undefined);
        const settled = await readFile(path, "utf8");
        assert.deepEqual(await attempt(), ready, "unchanged adopted readiness is idempotent");
        assert.equal(await readFile(path, "utf8"), settled);
        assert.equal(modes.at(-1), "inspect");
      });
    });
  }
}

for (const role of ["leader", "follower"] as const) {
  for (const fault of ["none", "apply-reply", "issuance-reply", "ready-reply", "target", "slot", "generation", "authority"] as const) {
    test(`Restore attempt reaches readiness or stays protected across re-entry (${role}/${fault})`, async () => {
      await fixture(async ({ open, threads, request, auth }) => {
        let armed = true;
        const store = open({ onPublicationBoundary(boundary) {
          if (!armed || boundary !== "after-rename") return;
          const phase = store.list()[0]?.phase;
          if ((fault === "issuance-reply" && phase === "recipient-issued") || (fault === "ready-reply" && phase === "ready")) {
            armed = false;
            throw new Error("Lost publication response");
          }
        } });
        let target = { ...request.binding.target };
        let liveRecipient = recipient(role);
        let current = true;
        let applies = 0;
        const modes: string[] = [];
        auth.isCurrent = () => current;
        const attempt = (restoreStore = store) => advanceTelegramWorkspaceRestore({ request, authority: auth, restoreStore,
          getRecipient: () => liveRecipient,
          async runRecipient(action) {
            modes.push(action.mode);
            assert.equal(restoreStore.list()[0]?.phase === "recipient-issued" || restoreStore.list()[0]?.phase === "ready", true);
            assert.deepEqual(threads.listWorkspaceBindings()[0]?.target, request.target);
            assert.equal(threads.list()[0]?.slot, request.binding.slot);
            if (action.mode === "apply") {
              applies += 1;
              target = { ...request.target };
              if (fault === "apply-reply" && armed) { armed = false; throw new Error("Lost recipient response"); }
              if (fault === "generation") liveRecipient = { ...liveRecipient, generation: "new-generation" };
              if (fault === "authority") current = false;
            }
            return { operationId: request.operationId, recipient: recipient(role), target: fault === "target" ? request.binding.target : target,
              slot: fault === "slot" ? "B" : request.binding.slot!, ready: target.threadId === request.target.threadId };
          } });
        if (fault === "apply-reply") await assert.rejects(() => attempt(), /Lost recipient response/);
        else assert.equal((await attempt())?.phase, fault === "none" || fault === "ready-reply" ? "ready" : undefined);
        const recovered = await attempt(open());
        const canRecover = ["none", "ready-reply", "apply-reply"].includes(fault);
        assert.equal(recovered?.phase, canRecover ? "ready" : undefined);
        assert.equal(applies, fault === "issuance-reply" ? 0 : 1);
        assert.deepEqual(modes, fault === "authority" ? ["apply"] :
          fault === "issuance-reply" ? ["inspect", "inspect"] : ["apply", "inspect"]);
        assert.equal(store.list()[0]?.phase, canRecover ? "ready" : "recipient-issued");
        assert.deepEqual(store.list()[0]?.request.source, request.source, "readiness never retires source custody");
        assert.equal(threads.list()[0]?.slot, "A");
        assert.deepEqual(threads.listWorkspaceBindings()[0]?.target, request.target);
      }, role);
    });
  }
}

for (const mode of ["registration", "process", "executor", "reply-loss", "old-target", "foreign-session", "ambiguous-owner"] as const) {
  test(`Restore successor can only inspect the same session's canonical target (${mode})`, async () => {
    await fixture(async ({ store, open, threads, request, auth, path }) => {
      const relocated = await store.commit(request, auth);
      let issued = store.issueRecipient(relocated!, recipient("follower"), auth)!.intent;
      if (mode === "executor") {
        auth.executor = { instanceId: "new-leader", leaderEpoch: "new-epoch" };
        issued = store.adopt(issued, auth)!;
      }
      const instanceId = mode === "registration" ? "old" : "successor";
      threads.upsert({ ...threads.list()[0]!, instanceId });
      if (mode === "ambiguous-owner") threads.upsert({ ...threads.list()[0]!, profileKey: "manual:other",
        owner: { kind: "manual-follower", instanceId: "other" }, instanceId: "other" });
      await threads.persist();
      const state = createTelegramBusFollowerRegistrationState();
      state.setRegistered(true, mode === "old-target" ? request.binding.target : request.target,
        { slot: "A", generation: "successor-registration" });
      const scope = { executor: auth.executor, profileBindingKey: "profile:restore", operatorUserId: 7,
        cwd: "/repo", sessionId: mode === "foreign-session" ? "different-session" : "session", generation: 2 };
      const ledger = createTelegramWorkspaceAdmissionLedger({ path: join(dirname(path), "admission.json"),
        profileKey: scope.profileBindingKey, owner: { processId: process.pid, processBirthId: `${process.pid}:successor` },
        getProcessLiveness: () => "alive" });
      const handler = createTelegramBusFollowerWorkspaceRestoreHandler({ instanceId, getContextAuthority: () => scope,
        readRestoreIntent: id => store.list().find(value => value.request.operationId === id), topicTargetStore: threads,
        registrationState: { ...state, setRegistered() { assert.fail("Successor must not apply the old grant"); } },
        getWorkspaceAdmission: () => ledger });
      const packet = { operationId: request.operationId, registrationGeneration: "successor-registration", mode: "inspect" as const };
      await assert.rejects(() => handler({ ...packet, mode: "apply" }, {}), /does not match this recipient/);
      const actual = { kind: "follower" as const, instanceId, sessionId: scope.sessionId, generation: packet.registrationGeneration };
      if (mode === "foreign-session" || mode === "ambiguous-owner") {
        await assert.rejects(() => handler(packet, {}), /does not match this recipient|canonical binding is not committed/);
        assert.equal(store.list()[0]?.phase, "recipient-issued");
        return;
      }
      const socketPath = getTelegramBusFollowerSocketPath(instanceId, dirname(path));
      const protocol = createTelegramBusProtocolIdentity({ runtimeBuild: "fixture", capabilities: [TELEGRAM_BUS_CAPABILITY_WORKSPACE_RESTORE] });
      const ctx = {};
      const receiver = createTelegramBusForwardedUpdateReceiverRuntime({ socketPath, instanceId,
        getAuthSecret: () => "successor-secret", getRegistrationGeneration: state.getGeneration, getRecipientBindingKey: () => "unused",
        getContext: () => ctx, isWorkspaceRestoreEnabled: () => true, handleWorkspaceRestore: handler,
        durableAdmission: { async admit() { assert.fail("Readiness is not input admission"); } } });
      let sequence = 0;
      const control = createTelegramBusWorkspaceRestoreController({ localProtocolIdentity: protocol,
        getAuthSecret: () => "successor-secret", createRequestId: () => `successor-${++sequence}`,
        getFollower: () => ({ instanceId, registrationGeneration: actual.generation, sessionId: actual.sessionId, slot: "A",
          cwd: "/repo", target: state.getTarget(), busSocketPath: socketPath, protocol, connectedAtMs: 1, lastHeartbeatMs: 1 }) });
      const modes: string[] = [];
      let loseReadyReply = mode === "reply-loss";
      const attempt = () => advanceTelegramWorkspaceRestore({ request, authority: auth, restoreStore: open({ onPublicationBoundary(boundary) {
        if (loseReadyReply && boundary === "after-rename" && store.list()[0]?.phase === "ready") {
          loseReadyReply = false;
          throw new Error("Lost successor readiness publication response");
        }
      } }),
        getRecipient: () => actual, async runRecipient(action) {
          modes.push(action.mode);
          assert.equal(action.isCurrent(), true);
          return control({ operationId: request.operationId, instanceId, sessionId: actual.sessionId, slot: "A",
            target: request.target, oldTarget: request.binding.target, mode: action.mode, isCurrent: action.isCurrent });
        } });
      await receiver.start();
      try {
        const ready = await attempt();
        assert.equal(ready?.phase, mode === "old-target" ? undefined : "ready");
        if (ready) {
          assert.deepEqual(ready.recipient, issued.recipient, "original issuance identity never changes");
          assert.deepEqual(ready.readyRecipient, actual);
          assert.deepEqual(ready.request.source, request.source);
          assert.deepEqual(await attempt(), ready, "unchanged ready observation does not rewrite evidence");
        }
        assert.equal(modes.every(value => value === "inspect"), true);
        assert.equal(store.issueRecipient(store.list()[0]!, actual, auth), undefined);
        assert.deepEqual(state.getTarget(), mode === "old-target" ? request.binding.target : request.target);
      } finally { await receiver.stop(); }
    }, "follower");
  });
}

test("Configured forum prompt bypasses stale private Workspace routing", async () => {
  const dir = await mkdtemp(join(tmpdir(), "telegram-forum-routing-"));
  try {
    const store = Threads.createTelegramTopicTargetStore({ path: join(dir, "state.json") });
    store.upsert({ profileKey: "cwd:/repo", owner: { kind: "leader", cwd: "/repo", instanceId: "old" },
      target: { chatId: 7, threadId: 42 }, status: "active", createdAtMs: 1, updatedAtMs: 1 });
    await store.persist();
    const { routeRuntime, events } = createRouteHarness({ config: { forumTarget: { chatId: -1007, threadId: 16 } }, threadStore: store,
      callApi: async () => { throw new Error("must not probe private chat"); } });
    await routeRuntime.handleUpdate({ message: { message_id: 13, message_thread_id: 16,
      chat: { id: -1007, type: "supergroup" }, from: { id: 8, is_bot: false }, text: "Hello" } }, { cwd: "/repo" });
    assert.equal(events.includes("dispatch"), true);
  } finally { await rm(dir, { recursive: true, force: true }); }
});
