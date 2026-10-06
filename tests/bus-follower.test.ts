/**
 * Regression tests for Telegram multi-instance bus follower helpers
 * Covers follower registration, forwarded update receiving, and follower-routed API calls
 */

import assert from "node:assert/strict";
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { readFile } from "node:fs/promises";
import { advanceTelegramWorkspaceRestore } from "../lib/routing.ts";
import { withWorkspaceRestoreFixture as fixture, restoreFixtureRecipient as recipient } from "./fixtures/workspace.ts";
import test from "node:test";

import {
  createTelegramBusFollowerApiCaller,
  createTelegramBusFollowerClientRuntime,
  createTelegramBusFollowerControlState,
  createTelegramBusFollowerDurableAdmissionRuntime,
  createTelegramBusFollowerSourceReferenceAdmissionRuntime,
  createTelegramBusFollowerPairedAdmission,
  createTelegramBusFollowerHeartbeatRecoveryHandler,
  createTelegramBusFollowerInputCustodyPorts,
  type TelegramBusFollowerInputCustodyBundle,
  createTelegramBusFollowerRegistrationRuntime as createRawTelegramBusFollowerRegistrationRuntime,
  createTelegramBusFollowerPromotionHandler,
  createTelegramBusFollowerQueueHandoffClient,
  createTelegramBusFollowerRegistrationState,
  createTelegramBusFollowerRestoreContextGetter,
  createTelegramBusFollowerWorkspaceRestoreHandler,
  createTelegramBusFollowerRuntimeAssembly,
  createTelegramBusFollowerSessionRefreshHook,
  createTelegramBusFollowerSessionReplacementSuspender,
  createTelegramBusForwardedUpdateReceiverRuntime,
  createTelegramFollowerProfileKeyResolver,
  getTelegramFollowerSessionHandoff,
  prepareTelegramBusFollowerJournaledUpdateForExecution,
  setTelegramFollowerSessionHandoff,
  TELEGRAM_BUS_FOLLOWER_HEARTBEAT_TIMEOUT_MS,
} from "../lib/bus-follower.ts";
import {
  createTelegramBusFollowerDeliveryIdentity,
  type TelegramBusEnvelope,
  createTelegramBusFollowerRegistry,
  createTelegramBusProtocolIdentity,
  createTelegramBusWorkspaceRestoreController,
  getTelegramBusFollowerSocketPath,
  TELEGRAM_BUS_CAPABILITY_WORKSPACE_RESTORE,
  type TelegramBusFollowerView,
  createTelegramBusLocalServer as createRawTelegramBusLocalServer,
  resolveTelegramBusSocketPath,
  sendTelegramBusLocalEnvelope,
  TELEGRAM_BUS_CAPABILITY_DURABLE_FOLLOWER_ADMISSION,
  TELEGRAM_BUS_CAPABILITY_QUEUE_HANDOFF,
  TELEGRAM_BUS_CAPABILITY_WORKSPACE_FOLLOWER_AUTO_CONNECT,
  TELEGRAM_BUS_CAPABILITY_WORKSPACE_THREAD_RENAME,
  TELEGRAM_BUS_CAPABILITY_THREAD_DISPLAY_MODE,
} from "../lib/bus.ts";
import { getTelegramBusTransportKind } from "../lib/bus-transport.ts";
import { createTelegramConfigStore } from "../lib/config.ts";
import { TELEGRAM_BUS_LEADER_STALE_HEARTBEAT_MS, withTelegramFileTransaction } from "../lib/locks.ts";
import { createTelegramUpdateJournalBotIdentity, createTelegramUpdateJournalStore,
  createTelegramUpdateJournalBindingRuntime, getTelegramUpdateJournalBindingPath } from "../lib/journal.ts";
import { resolveTelegramSessionJournalPath, resolveTelegramFollowerJournalPath } from "../lib/paths.ts";
import {
  createTelegramBusFollowerTargetProvisioner,
  createTelegramBusLeaderEnvelopeHandler as createRawTelegramBusLeaderEnvelopeHandler,
} from "../lib/bus-leader.ts";
import {
  createTelegramTopicTargetStore,
  createTelegramWorkspaceBindingIdentity,
  getTelegramLeaderSessionHandoff,
  setTelegramLeaderSessionHandoff,
} from "../lib/threads.ts";
import {
  getTelegramApiErrorRequestTarget,
  isTelegramApiCommitUnknownError,
} from "../lib/telegram-api.ts";
import { createTelegramWorkspaceAdmissionLedger } from "../lib/workspace-admission.ts";

const TEST_BUS_PROTOCOL_IDENTITY = createTelegramBusProtocolIdentity({
  runtimeBuild: "test",
  capabilities: [
    TELEGRAM_BUS_CAPABILITY_DURABLE_FOLLOWER_ADMISSION,
    TELEGRAM_BUS_CAPABILITY_QUEUE_HANDOFF,
    TELEGRAM_BUS_CAPABILITY_WORKSPACE_FOLLOWER_AUTO_CONNECT,
  ],
});

function createTelegramBusFollowerRegistrationRuntime<TContext extends {
  cwd?: string;
}>(
  deps: Omit<
    Parameters<typeof createRawTelegramBusFollowerRegistrationRuntime<TContext>>[0],
    "protocolIdentity"
  > & {
    protocolIdentity?: Parameters<
      typeof createRawTelegramBusFollowerRegistrationRuntime<TContext>
    >[0]["protocolIdentity"];
  },
) {
  const { protocolIdentity = TEST_BUS_PROTOCOL_IDENTITY, ...ports } = deps;
  return createRawTelegramBusFollowerRegistrationRuntime({
    getSessionId: () => "test-session",
    ...ports,
    protocolIdentity,
  });
}

function createTelegramBusLocalServer(
  deps: Parameters<typeof createRawTelegramBusLocalServer>[0],
) {
  const handleEnvelope = deps.handleEnvelope;
  return createRawTelegramBusLocalServer({
    ...deps,
    async handleEnvelope(envelope) {
      const response = await handleEnvelope(envelope);
      if (
        envelope.kind === "follower.register" &&
        response?.kind === "bus.ack" &&
        !response.protocol
      ) {
        return { ...response, protocol: TEST_BUS_PROTOCOL_IDENTITY };
      }
      return response;
    },
  });
}

function createTelegramBusLeaderEnvelopeHandler(
  deps: Omit<
    Parameters<typeof createRawTelegramBusLeaderEnvelopeHandler>[0],
    "protocolIdentity"
  > & {
    protocolIdentity?: Parameters<
      typeof createRawTelegramBusLeaderEnvelopeHandler
    >[0]["protocolIdentity"];
  },
) {
  const { protocolIdentity = TEST_BUS_PROTOCOL_IDENTITY, ...ports } = deps;
  const handle = createRawTelegramBusLeaderEnvelopeHandler({
    ...ports,
    protocolIdentity,
  });
  return (envelope: Parameters<typeof handle>[0]) =>
    handle(
      envelope.kind === "follower.register" &&
        !envelope.registration.protocol
        ? {
            ...envelope,
            registration: {
              ...envelope.registration,
              protocol: protocolIdentity,
            },
          }
        : envelope,
    );
}

async function waitForCondition(
  predicate: () => boolean,
  timeoutMs = 250,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  assert.fail("Timed out waiting for condition");
}

test("Follower control state owns active auth and transient lifecycle projection", () => {
  const state = createTelegramBusFollowerControlState();
  assert.equal(state.getActiveAuthSecret(), undefined);
  assert.equal(state.getLifecyclePhase(), undefined);

  state.setActiveAuthSecret("secret");
  state.setLifecyclePhase("electing");
  assert.equal(state.getActiveAuthSecret(), "secret");
  assert.equal(state.getLifecyclePhase(), "electing");

  state.setActiveAuthSecret(undefined);
  state.setLifecyclePhase(undefined);
  assert.equal(state.getActiveAuthSecret(), undefined);
  assert.equal(state.getLifecyclePhase(), undefined);
});

test("Bus follower profile key resolver follows the active profile", () => {
  let profileName: string | undefined;
  const resolveProfileKey = createTelegramFollowerProfileKeyResolver({
    getActiveProfileName: () => profileName,
    manualFollowerOwnerId: "7",
  });
  assert.equal(resolveProfileKey(), "manual:7");
  profileName = "work";
  assert.equal(resolveProfileKey(), "profile:work:manual:7");
});

test("Bus follower promotion handler transfers binding only after leadership acquisition", async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-follower-promotion-"));
  const store = createTelegramTopicTargetStore({ path: join(dir, "state.json") });
  const events: unknown[] = [];
  store.upsertWorkspaceBinding({
    ...createTelegramWorkspaceBindingIdentity("/repo", 0, "session-a")!,
    target: { chatId: 42, threadId: 11 }, slot: "E", threadName: "Ember",
    displayTitle: "repo_e", updatedAtMs: 100,
  });
  await store.persist();
  const promote = createTelegramBusFollowerPromotionHandler({
    topicTargetStore: store,
    instanceId: "inst-a",
    getActiveProfileName: () => "work",
    getSessionId: () => "session-a",
    startLeader: async (ctx: { cwd: string }, _election, onAcquired) => {
      events.push(`acquired:${ctx.cwd}`);
      await onAcquired();
      return true;
    },
    recordRuntimeEvent: (category, message, details) => {
      events.push({ category, message, details });
    },
    getPid: () => 10,
    getNowMs: () => 500,
  });
  try {
    await promote(
      { cwd: "/repo" },
      {
        target: { chatId: 42, threadId: 11 },
        slot: "E",
        threadName: "Ember",
      },
      {},
    );
    assert.equal(store.list()[0]?.profileKey, "profile:work:cwd:/repo");
    assert.equal(store.list()[0]?.owner?.kind, "leader");
    assert.equal(store.getWorkspaceBinding("/repo"), undefined);
    assert.equal(store.getWorkspaceBinding("/repo", "a", "session-a")?.threadName, "Ember");
    assert.equal(store.getWorkspaceBinding("/repo", "a", "session-a")?.displayTitle, "repo_e");
    assert.equal(events[0], "acquired:/repo");
    assert.deepEqual(events[1], {
      category: "bus",
      message: "Follower thread binding promoted to leader",
      details: {
        phase: "follower-promoted-binding",
        chatId: 42,
        threadId: 11,
        slot: "E",
        threadName: "Ember",
      },
    });
    assert.deepEqual(events[2], {
      category: "bus",
      message: "Promoted leader binding retained for session replacement",
      details: {
        phase: "follower-promoted-session-handoff",
        chatId: 42,
        threadId: 11,
        slot: "E",
        threadName: "Ember",
      },
    });
    assert.deepEqual(getTelegramLeaderSessionHandoff(), {
      pid: 10,
      instanceId: "inst-a",
      createdAtMs: 500,
      profileKey: "profile:work:cwd:/repo",
      target: { chatId: 42, threadId: 11 },
      slot: "E",
      threadName: "Ember",
    });
  } finally {
    setTelegramLeaderSessionHandoff(undefined);
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Bus follower promotion is rejected before leadership acquisition by a retained fence", async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-follower-promotion-fence-"));
  const admission = createTelegramWorkspaceAdmissionLedger({
    path: join(dir, "workspace-admission.json"),
    profileKey: "profile:follower-promotion",
    owner: {
      processId: process.pid,
      processBirthId: `${process.pid}:follower-promotion-test`,
    },
    getProcessLiveness: () => "alive",
  });
  const fence = admission.acquireRetirementFence({
    operationId: "follower-promotion-fence",
    retirementIntentId: "follower-promotion-intent",
    bindingKey: "follower-promotion-binding",
    slot: "E",
    target: { chatId: 42, threadId: 11 },
    leaderEpoch: 1,
    retirementRequestedAtMs: 1,
  });
  assert.equal(fence.kind, "acquired");
  let leadershipAttempted = false;
  const promote = createTelegramBusFollowerPromotionHandler({
    topicTargetStore: createTelegramTopicTargetStore({
      path: join(dir, "state.json"),
    }),
    instanceId: "inst-a",
    getActiveProfileName: () => "work",
    getWorkspaceAdmission: () => admission,
    startLeader: async () => {
      leadershipAttempted = true;
      return true;
    },
    recordRuntimeEvent() {},
  });
  try {
    await assert.rejects(
      promote(
        { cwd: "/repo" },
        { target: { chatId: 42, threadId: 11 }, slot: "E" },
        {},
      ),
      /blocked by retirement/u,
    );
    assert.equal(leadershipAttempted, false);
  } finally {
    if (fence.kind === "acquired") {
      admission.releaseUnissuedRetirementFence(fence.fence);
    }
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Bus follower promotion rejects slotless authority at global capacity", async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-follower-promotion-capacity-"));
  const store = createTelegramTopicTargetStore({ path: join(dir, "state.json") });
  store.upsert({
    profileKey: "manual:inst-a",
    owner: { kind: "manual-follower", instanceId: "inst-a" },
    target: { chatId: 42, threadId: 11 },
    status: "active",
    createdAtMs: 1,
    updatedAtMs: 1,
    instanceId: "inst-a",
  });
  for (const [index, slot] of Array.from("ABCDEFGHIJKLMNOPQRSTUVWXYZ").entries()) {
    store.upsertWorkspaceBinding({
      ...createTelegramWorkspaceBindingIdentity(`/retained/${index}`)!,
      target: { chatId: 42, threadId: 100 + index },
      slot,
      updatedAtMs: index + 1,
    });
  }
  await store.persist();
  const promote = createTelegramBusFollowerPromotionHandler({
    topicTargetStore: store,
    instanceId: "inst-a",
    getActiveProfileName: () => undefined,
    startLeader: async (_ctx: { cwd: string }, _election, onAcquired) => {
      await onAcquired();
      return true;
    },
    recordRuntimeEvent: () => undefined,
  });
  try {
    await assert.rejects(promote(
      { cwd: "/repo" },
      { target: { chatId: 42, threadId: 11 } },
      {},
    ), /promotion slot authority is unavailable/u);
    const retained = store.getByProfileKey("manual:inst-a");
    assert.equal(retained?.owner?.kind, "manual-follower");
    assert.equal(retained?.slot, undefined);
    assert.equal(store.getByProfileKey("cwd:/repo"), undefined);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Bus follower promotion leaves binding unchanged when election is lost", async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-follower-election-lost-"));
  const store = createTelegramTopicTargetStore({ path: join(dir, "state.json") });
  const promote = createTelegramBusFollowerPromotionHandler({
    topicTargetStore: store,
    instanceId: "inst-a",
    getActiveProfileName: () => "work",
    startLeader: async () => false,
    recordRuntimeEvent: () => undefined,
  });
  try {
    assert.equal(
      await promote(
        { cwd: "/repo" },
        {
          target: { chatId: 42, threadId: 11 },
          slot: "E",
          threadName: "Ember",
        },
        { expectedOwner: { pid: 99 } },
      ),
      false,
    );
    assert.deepEqual(store.list(), []);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Bus follower receiver stages authenticated queue handoff payloads", async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-queue-handoff-receiver-"));
  const socketPath = join(dir, "follower.sock");
  const staged: unknown[] = [];
  const receiver = createTelegramBusForwardedUpdateReceiverRuntime({
    socketPath,
    instanceId: "inst-b",
    getAuthSecret: () => "secret",
    getRegistrationGeneration: () => "generation-b",
    getRecipientBindingKey: () => "manual:owner-b",
    getContext: () => "ctx",
    durableAdmission: {
      admit: async () => assert.fail("queue handoff must not enter update admission"),
    },
    handleQueueHandoff(envelope, ctx) {
      staged.push({ envelope, ctx });
      return {
      status: "staged",
      receiptId: "receipt-1",
      sourceUpdateIds: [1],
      queueOwner: {
        instanceId: "inst-b",
        processId: 20,
        processBirthId: "20:start:inst-b",
        sessionGeneration: 1,
        acquisitionId: "recipient-acquisition",
        acquiredAtMs: 1,
      },
    };
    },
  });
  const payload = {
    kind: "prompt" as const,
    chatId: 7,
    replyToMessageId: 10,
    queueOrder: 1,
    queueLane: "default" as const,
    laneOrder: 1,
    statusSummary: "handoff",
    admissionReceipts: [
      {
        queueKind: "prompt" as const,
        receiptId: "receipt-1",
        sourceUpdateIds: [1],
      },
    ],
    sourceMessageIds: [10],
    queuedAttachments: [],
    content: [{ type: "text" as const, text: "handoff prompt" }],
    historyText: "handoff",
  };
  const envelope = {
    kind: "leader.offerQueueHandoff" as const,
    requestId: "handoff:1",
    auth: "secret",
    recipientInstanceId: "inst-b",
    recipientRegistrationGeneration: "generation-b",
    donorInstanceId: "inst-a",
    donorProcessId: 101,
    donorProcessBirthId: "101:start:a",
    donorSessionGeneration: 1,
    donorAcquisitionId: "acquisition-a",
    donorAcquiredAtMs: 1000,
    handoffToken: "x".repeat(32),
    payload,
    sentAtMs: 2000,
  };
  try {
    await receiver.start();
    assert.deepEqual(
      await sendTelegramBusLocalEnvelope({ socketPath, envelope }),
      {
        kind: "bus.ack",
        requestId: "handoff:1",
        ok: true,
        message: undefined,
        result: {
          status: "staged",
          receiptId: "receipt-1",
          sourceUpdateIds: [1],
          queueOwner: {
            instanceId: "inst-b",
            processId: 20,
            processBirthId: "20:start:inst-b",
            sessionGeneration: 1,
            acquisitionId: "recipient-acquisition",
            acquiredAtMs: 1,
          },
        },
      },
    );
    assert.deepEqual(staged, [{ envelope, ctx: "ctx" }]);
    assert.deepEqual(
      await sendTelegramBusLocalEnvelope({
        socketPath,
        envelope: { ...envelope, requestId: "handoff:2", auth: "tamper" },
      }),
      {
        kind: "bus.ack",
        requestId: "handoff:2",
        ok: false,
        message: "Unauthorized Telegram bus envelope.",
      },
    );
    assert.deepEqual(
      await sendTelegramBusLocalEnvelope({
        socketPath,
        envelope: {
          ...envelope,
          requestId: "handoff:3",
          recipientRegistrationGeneration: "stale",
        },
      }),
      {
        kind: "bus.ack",
        requestId: "handoff:3",
        ok: false,
        message: "Stale Telegram bus follower registration generation.",
      },
    );
    assert.equal(staged.length, 1);
  } finally {
    await receiver.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Bus follower receiver handles leader-forwarded updates", async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-bus-forward-"));
  const leaderSocketPath = join(dir, "leader.sock");
  const followerSocketPath = join(dir, "follower.sock");
  const registry = createTelegramBusFollowerRegistry();
  const received: unknown[] = [];
  let nowMs = 2000;
  const delivery = (
    kind:
      | "leader.forwardCallback"
      | "leader.forwardReaction"
      | "leader.forwardMessage"
      | "leader.forwardEditedMessage",
    sourceUpdateId: number,
  ) =>
    createTelegramBusFollowerDeliveryIdentity({
      kind,
      recipientBindingKey: "manual:owner-b",
      sourceUpdateId,
    });
  const receiver = createTelegramBusForwardedUpdateReceiverRuntime({
    socketPath: followerSocketPath,
    instanceId: "inst-b",
    getRegistrationGeneration: () => "generation-b",
    getRecipientBindingKey: () => "manual:owner-b",
    getContext() {
      return "ctx";
    },
    durableAdmission: {
      async admit(envelope, ctx) {
        if (envelope.kind === "leader.forwardCallback") {
          received.push({ kind: "callback", query: envelope.query, ctx });
        } else if (envelope.kind === "leader.forwardReaction") {
          received.push({
            kind: "reaction",
            reactionUpdate: envelope.reactionUpdate,
            ctx,
          });
        } else if (envelope.kind === "leader.forwardMessage") {
          received.push({ kind: "message", message: envelope.message, ctx });
        } else if (envelope.kind === "leader.forwardEditedMessage") {
          received.push({
            kind: "edited-message",
            message: envelope.message,
            ctx,
          });
        } else {
          assert.fail("custody wake entered legacy durable admission");
        }
        return {
          deliveryId: envelope.delivery!.deliveryId,
          sourceUpdateId: envelope.delivery!.sourceUpdateId,
        };
      },
    },
  });
  const leader = createTelegramBusLocalServer({
    socketPath: leaderSocketPath,
    handleEnvelope: createTelegramBusLeaderEnvelopeHandler({
      followerRegistry: registry,
      getNowMs: () => nowMs,
    }),
  });
  try {
    await receiver.start();
    await leader.start();
    registry.register({
      instanceId: "inst-b",
      busSocketPath: followerSocketPath,
      registrationGeneration: "generation-b",
      connectedAtMs: 1000,
    });
    const callbackResponse = await sendTelegramBusLocalEnvelope({
      socketPath: leaderSocketPath,
      envelope: {
        kind: "leader.forwardCallback",
        requestId: "leader:1",
        recipientInstanceId: "inst-b",
        recipientRegistrationGeneration: "generation-b",
        delivery: delivery("leader.forwardCallback", 1),
        query: { id: "cb-1", data: "queue:pause" },
        sentAtMs: 2000,
      },
    });
    assert.equal(registry.get("inst-b")?.lastHeartbeatMs, 2000);
    nowMs = 3000;
    const reactionResponse = await sendTelegramBusLocalEnvelope({
      socketPath: leaderSocketPath,
      envelope: {
        kind: "leader.forwardReaction",
        requestId: "leader:2",
        recipientInstanceId: "inst-b",
        recipientRegistrationGeneration: "generation-b",
        delivery: delivery("leader.forwardReaction", 2),
        reactionUpdate: { message_id: 9, new_reaction: [] },
        sentAtMs: 3000,
      },
    });
    assert.equal(registry.get("inst-b")?.lastHeartbeatMs, 3000);
    nowMs = 4000;
    const messageResponse = await sendTelegramBusLocalEnvelope({
      socketPath: leaderSocketPath,
      envelope: {
        kind: "leader.forwardMessage",
        requestId: "leader:3",
        recipientInstanceId: "inst-b",
        recipientRegistrationGeneration: "generation-b",
        delivery: delivery("leader.forwardMessage", 3),
        message: { message_id: 10, text: "hi" },
        sentAtMs: 4000,
      },
    });
    assert.equal(registry.get("inst-b")?.lastHeartbeatMs, 4000);
    nowMs = 5000;
    const editedMessageResponse = await sendTelegramBusLocalEnvelope({
      socketPath: leaderSocketPath,
      envelope: {
        kind: "leader.forwardEditedMessage",
        requestId: "leader:4",
        recipientInstanceId: "inst-b",
        recipientRegistrationGeneration: "generation-b",
        delivery: delivery("leader.forwardEditedMessage", 4),
        message: { message_id: 10, text: "edited" },
        sentAtMs: 5000,
      },
    });
    assert.deepEqual(callbackResponse, {
      kind: "bus.ack",
      requestId: "leader:1",
      ok: true,
      message: undefined,
      result: {
        deliveryId: delivery("leader.forwardCallback", 1).deliveryId,
        sourceUpdateId: 1,
      },
    });
    assert.deepEqual(reactionResponse, {
      kind: "bus.ack",
      requestId: "leader:2",
      ok: true,
      message: undefined,
      result: {
        deliveryId: delivery("leader.forwardReaction", 2).deliveryId,
        sourceUpdateId: 2,
      },
    });
    assert.deepEqual(messageResponse, {
      kind: "bus.ack",
      requestId: "leader:3",
      ok: true,
      message: undefined,
      result: {
        deliveryId: delivery("leader.forwardMessage", 3).deliveryId,
        sourceUpdateId: 3,
      },
    });
    assert.deepEqual(editedMessageResponse, {
      kind: "bus.ack",
      requestId: "leader:4",
      ok: true,
      message: undefined,
      result: {
        deliveryId: delivery("leader.forwardEditedMessage", 4).deliveryId,
        sourceUpdateId: 4,
      },
    });
    assert.equal(registry.get("inst-b")?.lastHeartbeatMs, 5000);
    assert.deepEqual(received, [
      {
        kind: "callback",
        query: { id: "cb-1", data: "queue:pause" },
        ctx: "ctx",
      },
      {
        kind: "reaction",
        reactionUpdate: { message_id: 9, new_reaction: [] },
        ctx: "ctx",
      },
      { kind: "message", message: { message_id: 10, text: "hi" }, ctx: "ctx" },
      {
        kind: "edited-message",
        message: { message_id: 10, text: "edited" },
        ctx: "ctx",
      },
    ]);
  } finally {
    await leader.stop();
    await receiver.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Bus follower rejects retired target replacement over IPC without recipient effects", async () => {
  await fixture(async ({ path, request }) => {
    const state = createTelegramBusFollowerRegistrationState();
    state.setRegistered(true, request.binding.target, { generation: "registration", slot: "A" });
    const before = await readFile(path, "utf8");
    const socketPath = getTelegramBusFollowerSocketPath("retired-restore", dirname(path));
    const receiver = createTelegramBusForwardedUpdateReceiverRuntime({
      socketPath, instanceId: "old", getAuthSecret: () => "secret",
      getRegistrationGeneration: state.getGeneration, getRecipientBindingKey: () => "manual:old",
      getContext: () => assert.fail("retired request reached recipient context"),
      isWorkspaceRestoreEnabled: () => true,
      handleWorkspaceRestore: async () => assert.fail("retired request reached Restore"),
      durableAdmission: { async admit() { assert.fail("retired request reached admission"); } },
    });
    try {
      await receiver.start();
      for (const generation of [undefined, "registration"]) {
        // Send the retired wire shape deliberately; it is no longer a typed producer contract.
        const envelope = { kind: "leader.replaceFollowerTarget", requestId: "retired",
          recipientInstanceId: "old", recipientRegistrationGeneration: generation,
          target: request.target, oldTarget: request.binding.target,
          reason: "thread-restore", auth: "secret", sentAtMs: 1000 } as unknown as TelegramBusEnvelope;
        assert.deepEqual(await sendTelegramBusLocalEnvelope({ socketPath, envelope,
          retry: { attempts: 1, delayMs: 0 } }), {
          kind: "bus.ack", requestId: "invalid", ok: false,
          message: "Invalid Telegram bus envelope.",
        });
        assert.equal(await readFile(path, "utf8"), before);
        assert.deepEqual(state.getTarget(), request.binding.target);
        assert.equal(state.getGeneration(), "registration");
      }
    } finally {
      await receiver.stop();
    }
  }, "follower");
});

test("Bus follower receiver rejects delayed work from a replaced registration generation", async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-bus-forward-generation-"));
  const socketPath = join(dir, "follower.sock");
  let handled = 0;
  const receiver = createTelegramBusForwardedUpdateReceiverRuntime({
    socketPath,
    instanceId: "inst-b",
    getRegistrationGeneration: () => "generation-new",
    getRecipientBindingKey: () => "manual:owner-b",
    getContext: () => "ctx",
    durableAdmission: {
      async admit(envelope) {
        handled += 1;
        return {
          deliveryId: envelope.delivery!.deliveryId,
          sourceUpdateId: envelope.delivery!.sourceUpdateId,
        };
      },
    },
  });
  try {
    await receiver.start();
    const response = await sendTelegramBusLocalEnvelope({
      socketPath,
      envelope: {
        kind: "leader.forwardCallback",
        requestId: "leader:old:1",
        recipientInstanceId: "inst-b",
        recipientRegistrationGeneration: "generation-old",
        delivery: createTelegramBusFollowerDeliveryIdentity({
          kind: "leader.forwardCallback",
          recipientBindingKey: "manual:owner-b",
          sourceUpdateId: 1,
        }),
        query: { id: "old", pi_telegram_source_update_id: 1 },
        sentAtMs: 2000,
      },
    });
    assert.equal(handled, 0);
    assert.deepEqual(response, {
      kind: "bus.ack",
      requestId: "leader:old:1",
      ok: false,
      message: "Stale Telegram bus follower registration generation.",
    });
  } finally {
    await receiver.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Follower paired admission extracts only exact human senders from canonical forwarded kinds", () => {
  for (const kind of ["message", "edited_message", "callback_query", "message_reaction"]) {
    for (const scenario of ["valid", "bot", "missing-flag", "invalid-flag", "wrong-author-field", "invalid-id", "anonymous", "source-mismatch", "extra-carrier", "invalid-carrier", "unsupported-kind", "bad-position"] as const) {
      let checked = 0;
      let published = 0;
      let fenced = 0;
      const gate = createTelegramBusFollowerPairedAdmission({
        profileName: "work", tokenSha256: "a".repeat(64),
        assertExecutionCurrent: () => { fenced++; },
        configStore: { withPairedUserAdmission(profile, hash, userId, publish, fence) {
          checked++;
          assert.equal(profile, "work");
          assert.equal(hash, "a".repeat(64));
          assert.equal(userId, 7);
          fence?.();
          return { admitted: true, value: publish() };
        } },
      });
      const sender: Record<string, unknown> = { id: 7, is_bot: false };
      if (scenario === "bot") sender.is_bot = true;
      if (scenario === "missing-flag") delete sender.is_bot;
      if (scenario === "invalid-flag") sender.is_bot = "false";
      if (scenario === "invalid-id") sender.id = 0;
      const authorField = kind === "message_reaction" ? "user" : "from";
      const carrier: Record<string, unknown> = {
        pi_telegram_source_update_id: scenario === "source-mismatch" ? 21 : 20,
        [authorField]: sender,
        forward_origin: { sender_user: { id: 99, is_bot: false } },
        message: { from: { id: 99, is_bot: true } },
      };
      if (scenario === "wrong-author-field") { delete carrier[authorField]; carrier[authorField === "user" ? "from" : "user"] = sender; }
      if (scenario === "anonymous") carrier[kind === "message_reaction" ? "actor_chat" : "sender_chat"] = null;
      const update = { update_id: 20, [scenario === "unsupported-kind" ? "guest_message" : kind]: carrier };
      if (scenario === "invalid-carrier") Object.assign(update, { [kind]: null });
      if (scenario === "extra-carrier") Object.assign(update, { [kind === "message" ? "edited_message" : "message"]: {} });
      if (scenario === "bad-position") Object.assign(update, { pi_telegram_forward_comment_batch_position: "invalid" });
      const result = gate([update], () => { published++; return "published"; });
      const valid = scenario === "valid";
      assert.deepEqual(result, valid ? { admitted: true, value: "published" } : { admitted: false }, `${kind}/${scenario}`);
      assert.equal(checked, Number(valid));
      assert.equal(published, Number(valid));
      assert.equal(fenced, Number(valid));
      if (valid) {
        assert.deepEqual(gate([], () => assert.fail("empty publication")), { admitted: false });
        assert.deepEqual(gate([update, update], () => assert.fail("batch publication")), { admitted: false });
      }
    }
  }
});

test("Paired follower receiver preserves provenance, unordered v1 admission and post-lock wakeup", async () => {
  const dir = mkdtempSync(join(tmpdir(), "telegram-paired-receiver-"));
  const configPath = join(dir, "telegram.json");
  const journalPath = join(dir, "inbox.json");
  const socketPath = join(dir, "receiver.sock");
  const identity = createTelegramUpdateJournalBotIdentity({ botToken: "fixture-token" });
  writeFileSync(configPath, JSON.stringify({ profiles: { work: { botToken: "fixture-token" } } }));
  const config = createTelegramConfigStore({ agentDir: dir, configPath });
  await config.load();
  config.activateProfile("work");
  const ledger = createTelegramWorkspaceAdmissionLedger({
    path: join(dir, "admission.json"), profileKey: "fixture:work",
    owner: { processId: process.pid, processBirthId: `${process.pid}:receiver` }, getProcessLiveness: () => "alive",
  });
  let current = true;
  let failPublication = false;
  let checks = 0;
  let signals = 0;
  const assertCurrent = () => { checks++; if (!current) throw new Error("stale fixture receiver"); };
  const pairedGate = createTelegramBusFollowerPairedAdmission({
    profileName: "work", tokenSha256: identity.tokenSha256, configStore: config, assertExecutionCurrent: assertCurrent,
  });
  const journal = createTelegramUpdateJournalStore({
    path: journalPath, profileName: "work", botIdentity: identity, workspaceAdmission: ledger,
    withPairedAdmission: pairedGate,
    onPublicationBoundary: () => { assertCurrent(); if (failPublication) throw new Error("fixture publication failed"); },
  });
  const durableAdmission = createTelegramBusFollowerDurableAdmissionRuntime({
    journal,
    signalWorker: () => {
      assert.equal(existsSync(`${configPath}.transaction`), false);
      assert.equal(existsSync(`${journalPath}.transaction`), false);
      assert.deepEqual(ledger.read().leases, []);
      assert.equal(config.getAllowedUserId(), 7);
      signals++;
    },
  });
  const receiver = createTelegramBusForwardedUpdateReceiverRuntime({
    socketPath, instanceId: "fixture", getAuthSecret: () => "fixture-auth",
    getRegistrationGeneration: () => "generation", getRecipientBindingKey: () => "binding", getContext: () => "ctx",
    durableAdmission,
  });
  const kinds = ["leader.forwardMessage", "leader.forwardEditedMessage", "leader.forwardCallback", "leader.forwardReaction"] as const;
  let requests = 0;
  const envelope = (kind: typeof kinds[number], id: number, sender = 7): TelegramBusEnvelope => {
    const base = { requestId: `request:${id}:${++requests}`, auth: "fixture-auth", recipientInstanceId: "fixture",
      recipientRegistrationGeneration: "generation", sentAtMs: 1,
      delivery: createTelegramBusFollowerDeliveryIdentity({ kind, recipientBindingKey: "binding", sourceUpdateId: id }) };
    const carrier = { pi_telegram_source_update_id: id, from: { id: sender, is_bot: false },
      user: { id: sender, is_bot: false }, chat: { id: 7, type: "private" }, message_id: id,
      old_reaction: [], new_reaction: [], id: `query:${id}` };
    if (kind === "leader.forwardCallback") return { ...base, kind, query: carrier };
    if (kind === "leader.forwardReaction") return { ...base, kind, reactionUpdate: carrier };
    if (kind === "leader.forwardMessage") return { ...base, kind, message: carrier, forwardCommentBatchPosition: "forward" };
    return { ...base, kind, message: carrier };
  };
  const send = async (value: TelegramBusEnvelope) => {
    const response = await sendTelegramBusLocalEnvelope({ socketPath, envelope: value });
    assert.equal(response?.kind, "bus.ack");
    if (response?.kind !== "bus.ack") throw new Error("missing fixture ACK");
    return response;
  };
  try {
    await receiver.start();
    assert.equal((await send(envelope(kinds[0], 40))).ok, false);
    assert.equal(existsSync(journalPath), false);
    assert.equal(signals, 0);
    assert.equal(config.getAllowedUserId(), undefined);
    const peer = createTelegramConfigStore({ agentDir: dir, configPath });
    await peer.load();
    assert.equal(peer.activateProfile("work"), true);
    assert.equal(await peer.persistAllowedUserId(7), true);
    const granted = readFileSync(configPath, "utf8");
    for (const [index, kind] of kinds.entries()) {
      const response = await send(envelope(kind, 40 - index * 10));
      assert.equal(response.ok, true, `${kind}: ${response.message}`);
    }
    assert.equal(signals, 4);
    assert.equal(journal.read().version, 1);
    assert.equal(journal.read().acceptedThroughUpdateId, undefined);
    assert.deepEqual(journal.read().entries.map((entry) => entry.updateId), [10, 20, 30, 40]);
    const before = checks;
    assert.equal((await send({ ...envelope(kinds[0], 50), auth: "wrong" })).ok, false);
    assert.equal((await send({ ...envelope(kinds[0], 50), recipientRegistrationGeneration: "stale" } as TelegramBusEnvelope)).ok, false);
    assert.equal((await send({ ...envelope(kinds[0], 50), delivery: createTelegramBusFollowerDeliveryIdentity({
      kind: kinds[0], recipientBindingKey: "wrong", sourceUpdateId: 50,
    }) } as TelegramBusEnvelope)).ok, false);
    assert.equal(checks, before, "Provenance rejection must precede config admission");
    for (const kind of kinds) assert.equal((await send(envelope(kind, 50, 8))).ok, false);
    current = false;
    assert.equal((await send(envelope(kinds[0], 50))).ok, false);
    current = true; failPublication = true;
    assert.equal((await send(envelope(kinds[0], 50))).ok, false);
    assert.equal(signals, 4);
    assert.deepEqual(journal.read().entries.map((entry) => entry.updateId), [10, 20, 30, 40]);
    assert.deepEqual(ledger.read().leases, []);
    assert.equal(readFileSync(configPath, "utf8"), granted);
  } finally {
    await receiver.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Bus follower custody ports follow lifecycle bundle replacement without cached authority", async () => {
  let accepts = 0;
  let wakes = 0;
  let bundle: TelegramBusFollowerInputCustodyBundle<string> | undefined = {
      acceptHandoff() { accepts += 1; return { duplicate: false }; },
      wakeSource() { wakes += 1; },
      resolveForwardReference({ sourceUpdateId }) { return { sourceRecoveryKey: "journal:source",
        source: { updateId: sourceUpdateId, owner: {
          acquisitionId: "acquisition", handoffId: "handoff" } } }; },
    };
  const ports = createTelegramBusFollowerInputCustodyPorts<string>({
    getInputCustodyBus: () => bundle });
  const source = { journalBindingKey: "journal:source", tokenSha256: "a".repeat(64), updateId: 43 };
  const handoffEnvelope = { kind: "leader.offerInputCustodyHandoff" as const,
    requestId: "leader:handoff", recipientInstanceId: "recipient",
    recipientRegistrationGeneration: "g1", recipientBindingKey: "workspace:recipient",
    sourceRecoveryKey: "journal:source", source, handoffId: "handoff", sentAtMs: 1 };
  assert.deepEqual(ports.handleInputCustodyHandoff(handoffEnvelope, "ctx"), { duplicate: false });
  const delivery = createTelegramBusFollowerDeliveryIdentity({ kind: "leader.wakeInputCustody",
    recipientBindingKey: "workspace:recipient", sourceUpdateId: 43,
    sourceRecoveryKey: "journal:source",
    sourceClaim: { acquisitionId: "acquisition", handoffId: "handoff" } });
  await ports.sourceReferenceAdmission.admit({ kind: "leader.wakeInputCustody",
    requestId: "leader:wake", recipientInstanceId: "recipient",
    recipientRegistrationGeneration: "g1", delivery, sentAtMs: 2 }, "ctx");
  assert.deepEqual([accepts, wakes], [1, 1]);
  assert.equal(ports.resolveInputCustodyReference({ sourceUpdateId: 43,
    recipientBindingKey: "workspace:recipient" })?.source.updateId, 43);
  bundle = undefined;
  assert.equal(ports.isSourceReferenceAdmissionEnabled(), false);
  assert.equal(ports.resolveInputCustodyReference({ sourceUpdateId: 43,
    recipientBindingKey: "workspace:recipient" }), undefined);
  assert.throws(() => ports.handleInputCustodyHandoff(handoffEnvelope, "ctx"),
    /bus binding is unavailable/);
  await assert.rejects(ports.sourceReferenceAdmission.admit({ kind: "leader.wakeInputCustody",
    requestId: "leader:wake-stale", recipientInstanceId: "recipient",
    recipientRegistrationGeneration: "g2", delivery, sentAtMs: 3 }, "ctx"),
  /bus binding is unavailable/);
  assert.deepEqual([accepts, wakes], [1, 1]);
});

test("Bus follower receiver invalidates custody ports across downgrade and reconnect", async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-custody-port-reconnect-"));
  const socketPath = join(dir, "follower.sock");
  let generation = "g1";
  let firstAccepts = 0;
  let secondAccepts = 0;
  let secondWakes = 0;
  const makeBundle = (accept: () => void, wake: () => void): TelegramBusFollowerInputCustodyBundle<string> => ({
    acceptHandoff() { accept(); return { duplicate: false }; },
    wakeSource() { wake(); },
    resolveForwardReference() { return undefined; },
  });
  let bundle: TelegramBusFollowerInputCustodyBundle<string> | undefined =
    makeBundle(() => { firstAccepts += 1; }, () => {});
  const ports = createTelegramBusFollowerInputCustodyPorts<string>({
    getInputCustodyBus: () => bundle });
  const receiver = createTelegramBusForwardedUpdateReceiverRuntime({ socketPath,
    instanceId: "recipient", getAuthSecret: () => "secret",
    getRegistrationGeneration: () => generation,
    getRecipientBindingKey: () => "workspace:recipient", getContext: () => "ctx",
    durableAdmission: { async admit() { assert.fail("legacy admission invoked"); } },
    hasAuthenticatedSourceReferenceTransport: () => true, ...ports,
  });
  const source = { journalBindingKey: "journal:source", tokenSha256: "a".repeat(64), updateId: 43 };
  const sendHandoff = (requestId: string, requestedGeneration: string) =>
    sendTelegramBusLocalEnvelope({ socketPath, envelope: {
      kind: "leader.offerInputCustodyHandoff", requestId,
      recipientInstanceId: "recipient", recipientRegistrationGeneration: requestedGeneration,
      recipientBindingKey: "workspace:recipient", sourceRecoveryKey: "journal:source",
      source, handoffId: "handoff", sentAtMs: 1, auth: "secret" } });
  try {
    await receiver.start();
    assert.equal((await sendHandoff("leader:first", "g1"))?.kind, "bus.ack");
    assert.equal(firstAccepts, 1);
    bundle = undefined;
    const downgraded = await sendHandoff("leader:downgraded", "g1");
    assert.equal(downgraded?.kind === "bus.ack" && downgraded.ok, false);
    assert.equal(firstAccepts, 1);
    generation = "g2";
    bundle = makeBundle(() => { secondAccepts += 1; }, () => { secondWakes += 1; });
    assert.equal((await sendHandoff("leader:second", "g2"))?.kind, "bus.ack");
    const stale = await sendHandoff("leader:stale", "g1");
    assert.equal(stale?.kind === "bus.ack" && stale.ok, false);
    const delivery = createTelegramBusFollowerDeliveryIdentity({ kind: "leader.wakeInputCustody",
      recipientBindingKey: "workspace:recipient", sourceUpdateId: 43,
      sourceRecoveryKey: "journal:source",
      sourceClaim: { acquisitionId: "acquisition", handoffId: "handoff" } });
    const wake = await sendTelegramBusLocalEnvelope({ socketPath, envelope: {
      kind: "leader.wakeInputCustody", requestId: "leader:wake", recipientInstanceId: "recipient",
      recipientRegistrationGeneration: "g2", delivery, sentAtMs: 2, auth: "secret" } });
    assert.equal(wake?.kind === "bus.ack" && wake.ok, true);
    assert.deepEqual([firstAccepts, secondAccepts, secondWakes], [1, 1, 1]);
  } finally { await receiver.stop(); rmSync(dir, { recursive: true, force: true }); }
});

test("Bus follower source-reference admission wakes durable custody without journaling a copy", async () => {
  const delivery = createTelegramBusFollowerDeliveryIdentity({
    kind: "leader.wakeInputCustody", recipientBindingKey: "workspace:recipient", sourceUpdateId: 43,
    sourceRecoveryKey: "journal:source-43",
    sourceClaim: { acquisitionId: "acquisition-43", handoffId: "handoff-43" } });
  const wakes: unknown[] = [];
  const admission = createTelegramBusFollowerSourceReferenceAdmissionRuntime({
    wakeSource(input, ctx) { wakes.push({ input, ctx }); },
  });
  const result = await admission.admit({ kind: "leader.wakeInputCustody", requestId: "leader:ref",
    recipientInstanceId: "recipient", recipientRegistrationGeneration: "g1", delivery,
    sentAtMs: 2_000 }, "ctx");
  assert.deepEqual(result, { deliveryId: delivery.deliveryId, sourceUpdateId: 43 });
  assert.deepEqual(wakes, [{ input: { deliveryId: delivery.deliveryId, sourceUpdateId: 43,
    recipientBindingKey: "workspace:recipient", sourceRecoveryKey: "journal:source-43",
    sourceClaim: { acquisitionId: "acquisition-43", handoffId: "handoff-43" } }, ctx: "ctx" }]);
  const legacyDelivery = createTelegramBusFollowerDeliveryIdentity({ kind: "leader.forwardMessage",
    recipientBindingKey: "workspace:recipient", sourceUpdateId: 43 });
  await assert.rejects(admission.admit({ kind: "leader.forwardMessage", requestId: "leader:mixed",
    recipientInstanceId: "recipient", recipientRegistrationGeneration: "g1",
    delivery: legacyDelivery, message: { pi_telegram_source_update_id: 43 }, sentAtMs: 2_001 }, "ctx"),
  /requires a recovery key/);
  assert.equal(wakes.length, 1);
  const missingClaim = createTelegramBusFollowerDeliveryIdentity({ kind: "leader.forwardMessage",
    recipientBindingKey: "workspace:recipient", sourceUpdateId: 43,
    sourceRecoveryKey: "journal:source-43" });
  await assert.rejects(admission.admit({ kind: "leader.forwardMessage", requestId: "leader:no-claim",
    recipientInstanceId: "recipient", recipientRegistrationGeneration: "g1",
    delivery: missingClaim, message: { pi_telegram_source_update_id: 43 }, sentAtMs: 2_002 }, "ctx"),
  /requires exact claim evidence/);
  assert.equal(wakes.length, 1);
});

test("Bus follower receiver gates source-reference wake across replay and replacement", async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-bus-source-reference-"));
  const socketPath = join(dir, "follower.sock");
  const delivery = createTelegramBusFollowerDeliveryIdentity({ kind: "leader.wakeInputCustody",
    recipientBindingKey: "workspace:recipient", sourceUpdateId: 43,
    sourceRecoveryKey: "journal:source-43",
    sourceClaim: { acquisitionId: "acquisition-43", handoffId: "handoff-43" } });
  const wakes: unknown[] = [];
  const sourceReferenceAdmission = createTelegramBusFollowerSourceReferenceAdmissionRuntime({
    wakeSource(input) { wakes.push(input); },
  });
  let legacyAdmissions = 0;
  const createReceiver = (withWake: boolean) => createTelegramBusForwardedUpdateReceiverRuntime({
    socketPath, instanceId: "recipient", getRegistrationGeneration: () => "g2",
    getRecipientBindingKey: () => "workspace:recipient", isSourceReferenceAdmissionEnabled: () => true,
    hasAuthenticatedSourceReferenceTransport: () => true,
    ...(withWake ? { sourceReferenceAdmission } : {}),
    durableAdmission: { async admit() { legacyAdmissions += 1; throw new Error("legacy copy invoked"); } },
    getContext: () => "ctx",
  });
  const send = async (requestId: string, generation = "g2") => {
    const response = await sendTelegramBusLocalEnvelope({ socketPath,
      envelope: { kind: "leader.wakeInputCustody", requestId, recipientInstanceId: "recipient",
        recipientRegistrationGeneration: generation, delivery, sentAtMs: 2_000 } });
    if (response?.kind !== "bus.ack") throw new Error("missing bus ACK");
    return response;
  };
  let receiver = createReceiver(true);
  try {
    await receiver.start();
    assert.equal((await send("leader:ref-1")).ok, true);
    assert.equal((await send("leader:ref-2")).ok, true);
    assert.equal((await send("leader:stale", "g1")).ok, false);
    assert.deepEqual(wakes, [{ deliveryId: delivery.deliveryId, sourceUpdateId: 43,
      recipientBindingKey: "workspace:recipient", sourceRecoveryKey: "journal:source-43",
      sourceClaim: { acquisitionId: "acquisition-43", handoffId: "handoff-43" } },
    { deliveryId: delivery.deliveryId, sourceUpdateId: 43,
      recipientBindingKey: "workspace:recipient", sourceRecoveryKey: "journal:source-43",
      sourceClaim: { acquisitionId: "acquisition-43", handoffId: "handoff-43" } }]);
    assert.equal(legacyAdmissions, 0);
    await receiver.stop();
    receiver = createReceiver(false);
    await receiver.start();
    const unavailable = await send("leader:no-wake");
    assert.equal(unavailable.ok, false);
    assert.match(unavailable.message ?? "", /enabled without a wake authority/);
    assert.equal(legacyAdmissions, 0);
  } finally {
    await receiver.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Bus follower receiver ACKs durable append before downstream execution and deduplicates replay", async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-bus-durable-admission-"));
  const socketPath = join(dir, "follower.sock");
  const admitted = new Set<number>();
  const journaled: unknown[] = [];
  let signals = 0;
  const durableAdmission = createTelegramBusFollowerDurableAdmissionRuntime({
    journal: {
      appendBatch(updates) {
        const updateId = updates[0]!.update_id;
        if (!admitted.has(updateId)) journaled.push(...updates);
        admitted.add(updateId);
      },
    },
    signalWorker() {
      signals += 1;
    },
  });
  const receiver = createTelegramBusForwardedUpdateReceiverRuntime({
    socketPath,
    instanceId: "inst-b",
    getRegistrationGeneration: () => "generation-b",
    getRecipientBindingKey: () => "manual:owner-b",
    durableAdmission,
    getContext: () => "ctx",
  });
  const delivery = createTelegramBusFollowerDeliveryIdentity({
    kind: "leader.forwardCallback",
    recipientBindingKey: "manual:owner-b",
    sourceUpdateId: 44,
  });
  const send = (requestId: string) =>
    sendTelegramBusLocalEnvelope({
      socketPath,
      envelope: {
        kind: "leader.forwardCallback",
        requestId,
        recipientInstanceId: "inst-b",
        recipientRegistrationGeneration: "generation-b",
        delivery,
        query: { id: "callback", pi_telegram_source_update_id: 44 },
        sentAtMs: 2000,
      },
    });
  try {
    await receiver.start();
    assert.deepEqual(await send("leader:1"), {
      kind: "bus.ack",
      requestId: "leader:1",
      ok: true,
      message: undefined,
      result: {
        deliveryId: delivery.deliveryId,
        sourceUpdateId: 44,
      },
    });
    assert.deepEqual(await send("leader:2"), {
      kind: "bus.ack",
      requestId: "leader:2",
      ok: true,
      message: undefined,
      result: {
        deliveryId: delivery.deliveryId,
        sourceUpdateId: 44,
      },
    });
    assert.deepEqual(journaled, [
      {
        update_id: 44,
        callback_query: {
          id: "callback",
          pi_telegram_source_update_id: 44,
        },
      },
    ]);
    assert.equal(signals, 1, "a repeated delivery is acknowledged without waking the worker again");
  } finally {
    await receiver.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Follower delivery replay window is bounded by age and capacity", async () => {
  const appended: number[] = [];
  let nowMs = 1000;
  const admission = createTelegramBusFollowerDurableAdmissionRuntime<string>({
    journal: { appendBatch(updates) { appended.push(...updates.map(update => update.update_id)); } },
    signalWorker() {}, getNowMs: () => nowMs, recentDeliveryLimit: { maxAgeMs: 100, maxEntries: 2 },
  });
  const admit = (sourceUpdateId: number) => admission.admit({
    kind: "leader.forwardMessage", requestId: `request-${sourceUpdateId}-${nowMs}`, recipientInstanceId: "inst-b",
    recipientRegistrationGeneration: "generation-b", sentAtMs: nowMs,
    delivery: createTelegramBusFollowerDeliveryIdentity({ kind: "leader.forwardMessage", recipientBindingKey: "manual:owner-b", sourceUpdateId }),
    message: { message_id: sourceUpdateId, chat: { id: 7, type: "private" }, pi_telegram_source_update_id: sourceUpdateId },
  } as never, "ctx");
  await admit(1); await admit(1);
  assert.deepEqual(appended, [1], "a retry inside the window is acknowledged without another append");
  nowMs += 100;
  await admit(1);
  assert.deepEqual(appended, [1, 1], "an expired delivery is admitted again");
  await admit(2); await admit(3); await admit(1);
  assert.deepEqual(appended, [1, 1, 2, 3, 1], "capacity evicts the oldest delivery first");
  await admit(3);
  assert.deepEqual(appended, [1, 1, 2, 3, 1], "recent deliveries inside capacity remain deduplicated");
});

test("Follower replay restores persisted forward grouping metadata without exposing it", () => {
  const prepared: unknown[] = [];
  const journaled = {
    update_id: 45,
    pi_telegram_forward_comment_batch_position: "forward",
    message: { message_id: 9 },
  };
  const update = prepareTelegramBusFollowerJournaledUpdateForExecution(
    journaled,
    (message, position) => prepared.push({ message, position }),
  );
  assert.deepEqual(prepared, [
    { message: { message_id: 9 }, position: "forward" },
  ]);
  assert.deepEqual(update, {
    update_id: 45,
    message: { message_id: 9 },
  });
  assert.equal(
    "pi_telegram_forward_comment_batch_position" in journaled,
    true,
  );
});

test("Bus follower receiver rejects a mismatched durable delivery binding", async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-bus-delivery-binding-"));
  const socketPath = join(dir, "follower.sock");
  let handled = 0;
  const receiver = createTelegramBusForwardedUpdateReceiverRuntime({
    socketPath,
    instanceId: "inst-b",
    getRegistrationGeneration: () => "generation-b",
    getRecipientBindingKey: () => "manual:owner-b",
    getContext: () => "ctx",
    durableAdmission: {
      async admit(envelope) {
        handled += 1;
        return {
          deliveryId: envelope.delivery!.deliveryId,
          sourceUpdateId: envelope.delivery!.sourceUpdateId,
        };
      },
    },
  });
  try {
    await receiver.start();
    const response = await sendTelegramBusLocalEnvelope({
      socketPath,
      envelope: {
        kind: "leader.forwardCallback",
        requestId: "leader:1",
        recipientInstanceId: "inst-b",
        recipientRegistrationGeneration: "generation-b",
        delivery: createTelegramBusFollowerDeliveryIdentity({
          kind: "leader.forwardCallback",
          recipientBindingKey: "manual:other-owner",
          sourceUpdateId: 44,
        }),
        query: { id: "callback" },
        sentAtMs: 2000,
      },
    });
    assert.equal(handled, 0);
    assert.deepEqual(response, {
      kind: "bus.ack",
      requestId: "leader:1",
      ok: false,
      message: "Mismatched Telegram follower delivery identity.",
    });
  } finally {
    await receiver.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Bus follower receiver rejects journal admission failure without a receipt", async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-bus-admission-failure-"));
  const socketPath = join(dir, "follower.sock");
  const receiver = createTelegramBusForwardedUpdateReceiverRuntime({
    socketPath,
    instanceId: "inst-b",
    getRegistrationGeneration: () => "generation-b",
    getRecipientBindingKey: () => "manual:owner-b",
    getContext: () => "ctx",
    durableAdmission: {
      async admit() {
        throw new Error("Telegram inbound journal capacity exceeded.");
      },
    },
  });
  try {
    await receiver.start();
    const response = await sendTelegramBusLocalEnvelope({
      socketPath,
      envelope: {
        kind: "leader.forwardCallback",
        requestId: "leader:1",
        recipientInstanceId: "inst-b",
        recipientRegistrationGeneration: "generation-b",
        delivery: createTelegramBusFollowerDeliveryIdentity({
          kind: "leader.forwardCallback",
          recipientBindingKey: "manual:owner-b",
          sourceUpdateId: 44,
        }),
        query: {
          id: "callback",
          pi_telegram_source_update_id: 44,
        },
        sentAtMs: 2000,
      },
    });
    assert.deepEqual(response, {
      kind: "bus.ack",
      requestId: "leader:1",
      ok: false,
      message: "Telegram inbound journal capacity exceeded.",
    });
  } finally {
    await receiver.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Bus follower heartbeat recovery passes current binding into promotion", async () => {
  const promoted: unknown[] = [];
  let leaderStateCalls = 0;
  const registrationState = createTelegramBusFollowerRegistrationState();
  registrationState.setRegistered(
    true,
    { chatId: 42, threadId: 10 },
    {
      slot: "F",
      threadName: "Fjord",
    },
  );
  const handler = createTelegramBusFollowerHeartbeatRecoveryHandler({
    registrationState,
    getRegistrationRuntime: () => ({
      registerWithLeader: async () => false,
      setContext: () => undefined,
      stop: () => undefined,
    }),
    getLeaderState: () => {
      leaderStateCalls += 1;
      return leaderStateCalls === 1
        ? { kind: "active-elsewhere", lock: { pid: 99 } }
        : { kind: "inactive" };
    },
    setLifecyclePhase: () => undefined,
    updateStatus: () => undefined,
    promoteToLeader: async (_ctx, binding) => {
      promoted.push(binding);
      return true;
    },
    sleep: async () => undefined,
    promotionGraceMs: 0,
    recordRuntimeEvent: () => undefined,
  });

  await handler(new Error("heartbeat failed"), "ctx");

  assert.deepEqual(promoted, [
    { target: { chatId: 42, threadId: 10 }, slot: "F", threadName: "Fjord" },
  ]);
});

test("Bus follower recovery contains promotion authority failure and schedules retry", async () => {
  const registrationState = createTelegramBusFollowerRegistrationState();
  registrationState.setRegistered(
    true,
    { chatId: 42, threadId: 10 },
    { threadName: "Fjord" },
  );
  let scheduledRetry: (() => void) | undefined;
  const events: Array<{ error: unknown; phase?: unknown }> = [];
  const handler = createTelegramBusFollowerHeartbeatRecoveryHandler({
    registrationState,
    getRegistrationRuntime: () => ({
      registerWithLeader: async () => false,
      setContext: () => undefined,
      stop: () => undefined,
    }),
    getLeaderState: () => ({ kind: "inactive" }),
    setLifecyclePhase: () => undefined,
    updateStatus: () => undefined,
    promoteToLeader: async () => {
      throw new Error("Telegram follower promotion slot authority is unavailable.");
    },
    scheduleRetry: (retry) => {
      scheduledRetry = retry;
    },
    promotionGraceMs: 0,
    recordRuntimeEvent: (_category, error, details) => {
      events.push({ error, phase: details?.phase });
    },
  });

  await handler(new Error("leader disconnected"), "ctx");

  assert.equal(typeof scheduledRetry, "function");
  assert.equal(
    events.some(
      (event) =>
        event.phase === "follower-promotion-failed" &&
        event.error instanceof Error &&
        /promotion slot authority is unavailable/u.test(event.error.message),
    ),
    true,
  );
});

test("Bus follower election defers a higher slot to the lowest live candidate", async () => {
  const registrationState = createTelegramBusFollowerRegistrationState();
  registrationState.setRegistered(
    true,
    { chatId: 42, threadId: 10 },
    { slot: "D", threadName: "Dawn" },
  );
  registrationState.setEligibleElectionSlots(["D", "C"]);
  let state: "inactive" | "winner" = "inactive";
  let promoted = 0;
  let registered = 0;
  const events: Array<Record<string, unknown> | undefined> = [];
  const handler = createTelegramBusFollowerHeartbeatRecoveryHandler({
    registrationState,
    getRegistrationRuntime: () => ({
      registerWithLeader: async () => {
        registered += 1;
        return true;
      },
      setContext: () => undefined,
      stop: () => registrationState.setRegistered(false),
    }),
    getLeaderState: () =>
      state === "inactive"
        ? { kind: "inactive" }
        : {
            kind: "active-elsewhere",
            lock: { pid: 99, instanceId: "slot-c", leaderEpoch: "epoch-c" },
          },
    setLifecyclePhase: () => undefined,
    updateStatus: () => undefined,
    promoteToLeader: async () => {
      promoted += 1;
      return true;
    },
    sleep: async () => {
      state = "winner";
    },
    promotionGraceMs: 2500,
    recordRuntimeEvent: (_category, _message, details) => {
      events.push(details);
    },
  });

  await handler(new Error("leader disconnected"), "ctx");

  assert.equal(promoted, 0);
  assert.equal(registered, 1);
  assert.equal(
    events.some(
      (details) =>
        details?.phase === "follower-promotion-slot-priority" &&
        details.lowerEligibleSlot === "C",
    ),
    true,
  );
});

test("Bus follower heartbeat recovery never promotes over a live leader lease", async () => {
  const promoted: unknown[] = [];
  const phases: Array<string | undefined> = [];
  const events: Array<{ message: unknown; phase?: unknown }> = [];
  const registrationState = createTelegramBusFollowerRegistrationState();
  registrationState.setRegistered(true, { chatId: 42, threadId: 10 });
  const liveLeader = {
    kind: "active-elsewhere" as const,
    lock: {
      pid: 99,
      instanceId: "leader-a",
      leaderEpoch: "epoch-a",
    },
  };
  const handler = createTelegramBusFollowerHeartbeatRecoveryHandler({
    registrationState,
    getRegistrationRuntime: () => ({
      registerWithLeader: async () => false,
      setContext: () => undefined,
      stop: () => undefined,
    }),
    getLeaderState: () => liveLeader,
    setLifecyclePhase: (phase) => {
      phases.push(phase);
    },
    updateStatus: () => undefined,
    promoteToLeader: async (_ctx, binding) => {
      promoted.push(binding);
      return true;
    },
    sleep: async () => undefined,
    promotionGraceMs: 0,
    recordRuntimeEvent: (_category, message, details) => {
      events.push({ message, phase: details?.phase });
    },
  });

  await handler(new Error("heartbeat failed"), "ctx");

  assert.deepEqual(promoted, []);
  assert.equal(phases.at(-1), undefined);
  assert.equal(
    events.some(
      (event) => event.phase === "follower-promotion-live-owner",
    ),
    true,
  );
});

test("Bus follower heartbeat recovery retries until a live lease becomes stale", async () => {
  let stateReadCount = 0;
  let scheduledRetry: (() => void) | undefined;
  let resolvePromoted: (() => void) | undefined;
  const promoted = new Promise<void>((resolve) => {
    resolvePromoted = resolve;
  });
  const registrationState = createTelegramBusFollowerRegistrationState();
  registrationState.setRegistered(
    true,
    { chatId: 42, threadId: 10 },
    { slot: "F", threadName: "Fjord" },
  );
  const liveLock = {
    pid: 99,
    instanceId: "leader-a",
    leaderEpoch: "epoch-a",
  };
  const handler = createTelegramBusFollowerHeartbeatRecoveryHandler({
    registrationState,
    getRegistrationRuntime: () => ({
      registerWithLeader: async () => false,
      setContext: () => undefined,
      stop: () => undefined,
    }),
    getLeaderState: () => {
      stateReadCount += 1;
      return stateReadCount <= 2
        ? { kind: "active-elsewhere", lock: liveLock }
        : { kind: "stale", lock: liveLock };
    },
    setLifecyclePhase: () => undefined,
    updateStatus: () => undefined,
    promoteToLeader: async (_ctx, binding, election) => {
      assert.deepEqual(binding, {
        target: { chatId: 42, threadId: 10 },
        slot: "F",
        threadName: "Fjord",
      });
      assert.deepEqual(election, { expectedOwner: liveLock });
      resolvePromoted?.();
      return true;
    },
    sleep: async () => undefined,
    scheduleRetry: (retry) => {
      scheduledRetry = retry;
    },
    getActiveContext: () => "ctx",
    promotionGraceMs: 0,
    recordRuntimeEvent: () => undefined,
  });

  await handler(new Error("heartbeat failed"), "ctx");
  assert.ok(scheduledRetry);
  scheduledRetry();
  await promoted;
});

test("Bus follower election loser schedules re-registration with the winner", async () => {
  const scheduled: Array<() => void> = [];
  let registrationCalls = 0;
  let promotionCalls = 0;
  let registrationTarget: unknown;
  let resolveRegistered: (() => void) | undefined;
  const registered = new Promise<void>((resolve) => {
    resolveRegistered = resolve;
  });
  const registrationState = createTelegramBusFollowerRegistrationState();
  registrationState.setRegistered(true, { chatId: 42, threadId: 10 });
  const staleLock = { pid: 99, leaderEpoch: "old-epoch" };
  const winnerLock = { pid: 100, leaderEpoch: "winner-epoch" };
  let state: "stale" | "winner" = "stale";
  const handler = createTelegramBusFollowerHeartbeatRecoveryHandler({
    registrationState,
    getRegistrationRuntime: () => ({
      registerWithLeader: async (_ctx, _leader, options) => {
        registrationCalls += 1;
        registrationTarget = options?.target;
        resolveRegistered?.();
        return true;
      },
      setContext: () => undefined,
      stop: () => {
        registrationState.setRegistered(false);
      },
    }),
    getLeaderState: () =>
      state === "stale"
        ? { kind: "stale", lock: staleLock }
        : { kind: "active-elsewhere", lock: winnerLock },
    setLifecyclePhase: () => undefined,
    updateStatus: () => undefined,
    promoteToLeader: async () => {
      promotionCalls += 1;
      state = "winner";
      return false;
    },
    sleep: async () => undefined,
    scheduleRetry: (retry) => {
      scheduled.push(retry);
    },
    getActiveContext: () => "ctx",
    promotionGraceMs: 0,
    recordRuntimeEvent: () => undefined,
  });

  await handler(new Error("heartbeat failed"), "ctx");
  assert.equal(promotionCalls, 1);
  assert.equal(scheduled.length, 1);
  scheduled.shift()?.();
  await registered;
  assert.equal(registrationCalls, 1);
  assert.deepEqual(registrationTarget, { chatId: 42, threadId: 10 });
});

test("Bus follower scheduled recovery transfers across session context replacement", async () => {
  const scheduled: Array<() => void> = [];
  let activeContext: string | undefined = "old-ctx";
  let stateReads = 0;
  let promotedContext: string | undefined;
  let resolvePromoted: (() => void) | undefined;
  const promoted = new Promise<void>((resolve) => {
    resolvePromoted = resolve;
  });
  const registrationState = createTelegramBusFollowerRegistrationState();
  registrationState.setRegistered(true, { chatId: 42, threadId: 10 });
  const lock = { pid: 99, leaderEpoch: "epoch-a" };
  const handler = createTelegramBusFollowerHeartbeatRecoveryHandler({
    registrationState,
    getRegistrationRuntime: () => ({
      registerWithLeader: async () => false,
      setContext: () => undefined,
      stop: () => undefined,
    }),
    getLeaderState: () => {
      stateReads += 1;
      return stateReads <= 2
        ? { kind: "active-elsewhere", lock }
        : { kind: "stale", lock };
    },
    setLifecyclePhase: () => undefined,
    updateStatus: () => undefined,
    promoteToLeader: async (ctx) => {
      promotedContext = ctx;
      resolvePromoted?.();
      return true;
    },
    sleep: async () => undefined,
    scheduleRetry: (retry) => {
      scheduled.push(retry);
    },
    getActiveContext: () => activeContext,
    promotionGraceMs: 0,
    recordRuntimeEvent: () => undefined,
  });

  await handler(new Error("heartbeat failed"), "old-ctx");
  activeContext = undefined;
  scheduled.shift()?.();
  assert.equal(scheduled.length, 1);
  activeContext = "new-ctx";
  scheduled.shift()?.();
  await promoted;
  assert.equal(promotedContext, "new-ctx");
});

test("Bus follower heartbeat recovery swallows stale-context status updates", async () => {
  const events: unknown[] = [];
  let leaderStateCalls = 0;
  const registrationState = createTelegramBusFollowerRegistrationState();
  registrationState.setRegistered(true, { chatId: 42, threadId: 10 });
  const handler = createTelegramBusFollowerHeartbeatRecoveryHandler({
    registrationState,
    getRegistrationRuntime: () => ({
      registerWithLeader: async () => false,
      setContext: () => undefined,
      stop: () => undefined,
    }),
    getLeaderState: () => {
      leaderStateCalls += 1;
      return leaderStateCalls === 1
        ? { kind: "active-elsewhere", lock: { pid: 99 } }
        : { kind: "inactive" };
    },
    setLifecyclePhase: () => undefined,
    updateStatus: () => {
      throw new Error("This extension ctx is stale after session replacement");
    },
    promoteToLeader: async () => true,
    sleep: async () => undefined,
    promotionGraceMs: 0,
    recordRuntimeEvent: (category, error, details) => {
      events.push({ category, error, details });
    },
  });

  await handler(new Error("heartbeat failed"), "stale-ctx");

  assert.equal(registrationState.getTarget(), undefined);
  assert.equal(
    events.some(
      (event) =>
        typeof event === "object" &&
        event !== null &&
        (event as { details?: { phase?: string } }).details?.phase ===
          "follower-stale-context-status",
    ),
    true,
  );
});

for (const scenario of ["stable", "session-drift", "session-id-drift", "superseded", "stopped", "startup-drift", "refreshed-during-heartbeat"] as const) {
test(`Registration response retains exact request/session authority (${scenario})`, { timeout: 5000 }, async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-registration-authority-"));
  const socketPath = join(dir, "leader.sock");
  const entered = Promise.withResolvers<void>();
  const released = Promise.withResolvers<void>();
  const state = createTelegramBusFollowerRegistrationState();
  const ctx = { cwd: "/fixture" };
  let sessionGeneration = 1;
  let sequence = 0;
  let receivingStops = 0;
  let sessionId = "session-before-reply";
  const requests: Array<number | undefined> = [];
  const prepared: number[] = [];
  const server = createTelegramBusLocalServer({ socketPath, async handleEnvelope(envelope) {
    if (envelope.kind === "follower.register") {
      requests.push(envelope.registration.sessionGeneration);
      if (requests.length === 1 && scenario !== "startup-drift" && scenario !== "refreshed-during-heartbeat") {
        entered.resolve(); await released.promise;
      }
    }
    if (envelope.kind === "follower.heartbeat" && scenario === "refreshed-during-heartbeat") {
      entered.resolve(); await released.promise;
    }
    return { kind: "bus.ack", requestId: envelope.requestId, ok: true,
      protocol: TEST_BUS_PROTOCOL_IDENTITY, result: { target: { chatId: 7, threadId: 42 }, slot: "A" } };
  } });
  const runtime = createTelegramBusFollowerRegistrationRuntime({ instanceId: "fixture",
    registrationState: state, protocolIdentity: TEST_BUS_PROTOCOL_IDENTITY,
    createRequestId: () => `fixture:${++sequence}`, getSessionGeneration: () => sessionGeneration,
    getSessionId: () => sessionId,
    isContextActive: current => current === ctx, heartbeatMs: 60_000,
    async startReceiving() { if (scenario === "startup-drift") { entered.resolve(); await released.promise; } },
    stopReceiving: async () => { receivingStops++; },
    onRegistered: () => { prepared.push(sessionGeneration); },
  });
  let first: Promise<boolean> | undefined;
  try {
    await server.start();
    first = runtime.registerWithLeader(ctx, { busSocketPath: socketPath });
    await entered.promise;
    if (scenario === "session-drift" || scenario === "startup-drift") sessionGeneration++;
    if (scenario === "session-id-drift") sessionId = "session-after-reply";
    if (scenario === "stopped") runtime.stop();
    let newerGeneration: string | undefined;
    if (scenario === "refreshed-during-heartbeat") {
      sessionGeneration++;
      await runtime.setContext(ctx);
      newerGeneration = state.getGeneration();
    }
    if (scenario === "superseded") {
      assert.equal(await runtime.registerWithLeader(ctx, { busSocketPath: socketPath }), true);
      newerGeneration = state.getGeneration();
    }
    const stopsBeforeReply = receivingStops;
    released.resolve();
    assert.equal(await first, scenario === "stable", "A late response cannot mint authority for an expired request");
    if (scenario === "superseded" || scenario === "refreshed-during-heartbeat") {
      assert.equal(state.getGeneration(), newerGeneration);
      assert.equal(receivingStops, stopsBeforeReply, "An obsolete request cannot stop the newer receiver");
      assert.deepEqual(prepared, [1]);
    } else {
      assert.equal(state.isRegistered(), scenario === "stable");
      assert.deepEqual(prepared, scenario === "stable" ? [1] : []);
    }
    assert.deepEqual(requests, scenario === "startup-drift" ? [] : scenario === "superseded" ? [1, 1] : [1]);
    if (scenario === "session-drift" || scenario === "session-id-drift") {
      assert.equal(await runtime.registerWithLeader(ctx, { busSocketPath: socketPath }), true);
      assert.deepEqual(prepared, [sessionGeneration]);
      assert.deepEqual(requests, [1, sessionGeneration]);
    }
  } finally {
    released.resolve(); await first?.catch(() => undefined); runtime.stop();
    await server.stop(); rmSync(dir, { recursive: true, force: true });
  }
});
}

test("Follower assembly keeps same-session refresh stable and requires registration for a changed session", async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-follower-assembly-"));
  const leaderSocketPath = join(dir, "leader.sock");
  const followerSocketPath = join(dir, "follower.sock");
  const ctx = { cwd: "/repo" };
  let sessionId = "session-a";
  let sessionGeneration = 1;
  const followerRegistry = createTelegramBusFollowerRegistry();
  const registrationState = createTelegramBusFollowerRegistrationState();
  let requestSequence = 0;
  const leader = createTelegramBusLocalServer({
    socketPath: leaderSocketPath,
    handleEnvelope: createTelegramBusLeaderEnvelopeHandler({
      followerRegistry,
      protocolIdentity: TEST_BUS_PROTOCOL_IDENTITY,
      provisionFollowerTarget: () => ({ chatId: 7, threadId: 42, slot: "A" }),
    }),
  });
  const assembly = createTelegramBusFollowerRuntimeAssembly<{
    cwd: string;
  }>({
    instanceId: "inst-a",
    registrationState,
    recordRuntimeEvent: () => undefined,
    receiver: {
      socketPath: followerSocketPath,
      getContext: () => ctx,
      getRecipientBindingKey: () => "manual:inst-a",
      durableAdmission: {
        async admit(envelope) {
          return {
            deliveryId: envelope.delivery!.deliveryId,
            sourceUpdateId: envelope.delivery!.sourceUpdateId,
          };
        },
      },
    },
    recovery: {
      getLeaderState: () => ({ kind: "inactive" }),
      setLifecyclePhase: () => undefined,
      updateStatus: () => undefined,
      promoteToLeader: async () => true,
      sleep: async () => undefined,
      promotionGraceMs: 1,
    },
    registration: {
      protocolIdentity: TEST_BUS_PROTOCOL_IDENTITY,
      getFollowerBusSocketPath: () => followerSocketPath,
      getLeaderSocketPath: () => leaderSocketPath,
      createRequestId: () => `inst-a:${++requestSequence}`,
      getSessionId: () => sessionId,
      getSessionGeneration: () => sessionGeneration,
      isContextActive: current => current === ctx,
    },
  });
  try {
    await leader.start();
    assert.equal(
      await assembly.registration.registerWithLeader(
        ctx,
        { busSocketPath: leaderSocketPath },
      ),
      true,
    );
    if (process.platform === "win32") {
      assert.equal(
        getTelegramBusTransportKind(
          resolveTelegramBusSocketPath(followerSocketPath),
        ),
        "pipe",
      );
    } else {
      assert.equal(
        existsSync(resolveTelegramBusSocketPath(followerSocketPath)),
        true,
      );
    }
    assert.deepEqual(registrationState.getTarget(), {
      chatId: 7,
      threadId: 42,
    });
    assert.equal(registrationState.getSlot(), "A");
    assert.equal(assembly.getReadySessionId(), "session-a");
    const registeredGeneration = registrationState.getGeneration();
    assert.equal(followerRegistry.get("inst-a")?.sessionId, "session-a");
    sessionGeneration++;
    await assembly.registration.setContext(ctx);
    assert.equal(assembly.getReadySessionId(), "session-a");
    assert.equal(registrationState.getGeneration(), registeredGeneration,
      "Same-session refresh does not re-register or replace transport generation");
    sessionId = "session-b";
    assert.equal(assembly.getReadySessionId(), undefined);
    await assert.rejects(async () => assembly.registration.setContext(ctx), /requires acknowledged leader registration/);
    assert.equal(assembly.getReadySessionId(), undefined);
    assert.equal(registrationState.getGeneration(), registeredGeneration);
    assert.equal(followerRegistry.get("inst-a")?.sessionId, "session-a");
    assert.equal(await assembly.registration.registerWithLeader(ctx, { busSocketPath: leaderSocketPath }), true);
    assert.equal(assembly.getReadySessionId(), "session-b");
    assert.equal(followerRegistry.get("inst-a")?.sessionId, "session-b");
    assert.notEqual(registrationState.getGeneration(), registeredGeneration);
  } finally {
    assembly.registration.stop();
    await assembly.receiver.stop();
    await leader.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Follower readiness captures session identity across preparation and reused-context refresh", async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-follower-session-ready-"));
  const leaderSocketPath = join(dir, "leader.sock");
  const followerSocketPath = join(dir, "follower.sock");
  const state = createTelegramBusFollowerRegistrationState();
  const ctx = { cwd: "/repo" };
  let sessionId = "before-startup";
  let sessionGeneration = 1;
  let sequence = 0;
  let admissions = 0;
  let entered = Promise.withResolvers<void>();
  let released = Promise.withResolvers<void>();
  let pausePreparation = true;
  const journals = createTelegramUpdateJournalBindingRuntime({
    base: { getProfileName: () => "work", getBotToken: () => "fixture-session-admission", getBotId: () => 7 },
    getLeaderJournalPath: () => join(dir, "inbox.work.json"),
    getFollowerJournalPath: (key, profileName, id) => id === undefined
      ? resolveTelegramFollowerJournalPath(key, dir, profileName)
      : resolveTelegramSessionJournalPath(id, key, dir, profileName),
    getActiveFollowerBindingKey: () => "manual:session-ready",
    getActiveFollowerSessionId: () => state.getSessionId(sessionId),
    isFollowerRegistered: state.isRegistered,
  });
  const leader = createTelegramBusLocalServer({ socketPath: leaderSocketPath,
    handleEnvelope: createTelegramBusLeaderEnvelopeHandler({
      followerRegistry: createTelegramBusFollowerRegistry(), protocolIdentity: TEST_BUS_PROTOCOL_IDENTITY,
      provisionFollowerTarget: () => ({ chatId: 7, threadId: 42, slot: "A" }),
    }),
  });
  const assembly = createTelegramBusFollowerRuntimeAssembly({ instanceId: "session-ready", registrationState: state,
    recordRuntimeEvent: () => {},
    receiver: { socketPath: followerSocketPath, getContext: () => ctx,
      getRecipientBindingKey: () => "manual:session-ready",
      durableAdmission: createTelegramBusFollowerDurableAdmissionRuntime({
        journal: { appendBatch(updates) {
          const binding = journals.resolveFollower();
          if (!binding) throw new Error("Fixture session journal is not prepared");
          binding.journal.appendBatch(updates);
        } },
        signalWorker() { admissions++; },
      }),
    },
    recovery: { getLeaderState: () => ({ kind: "inactive" }), setLifecyclePhase: () => {},
      updateStatus: () => {}, promoteToLeader: async () => false },
    registration: { protocolIdentity: TEST_BUS_PROTOCOL_IDENTITY,
      getFollowerBusSocketPath: () => followerSocketPath, createRequestId: () => `ready:${++sequence}`,
      getProfileKey: () => "manual:session-ready", getSessionId: () => sessionId,
      getSessionGeneration: () => sessionGeneration, isContextActive: current => current === ctx,
      async onRegistered() {
        if (pausePreparation) { entered.resolve(); await released.promise; }
      },
    },
  });
  const send = () => sendTelegramBusLocalEnvelope({ socketPath: followerSocketPath, timeoutMs: 1000,
    envelope: { kind: "leader.forwardMessage", requestId: `input:${++sequence}`,
      recipientInstanceId: "session-ready", recipientRegistrationGeneration: state.getGeneration()!,
      delivery: createTelegramBusFollowerDeliveryIdentity({ kind: "leader.forwardMessage",
        recipientBindingKey: "manual:session-ready", sourceUpdateId: sequence }),
      message: { message_id: sequence, chat: { id: 7, type: "private" }, from: { id: 7, is_bot: false },
        text: "session-bound", pi_telegram_source_update_id: sequence }, sentAtMs: 1 },
  }).then(response => response?.kind === "bus.ack" ? response : undefined);
  let registration: Promise<boolean> | undefined;
  let refresh: Promise<void> | undefined;
  try {
    await leader.start();
    registration = assembly.registration.registerWithLeader(ctx, { busSocketPath: leaderSocketPath });
    await entered.promise;
    assert.equal(assembly.getReadySessionId(), undefined);
    sessionId = "session-a";
    released.resolve();
    assert.equal(await registration, false, "An ID changed during preparation cannot finalize registration");
    assert.equal(assembly.getReadySessionId(), undefined);
    assert.equal(state.isRegistered(), false);
    assert.equal(admissions, 0);
    pausePreparation = false;
    assert.equal(await assembly.registration.registerWithLeader(ctx, { busSocketPath: leaderSocketPath }), true);
    assert.equal(assembly.getReadySessionId(), "session-a");
    assert.equal((await send())?.ok, true);
    assert.equal(admissions, 1);
    const originalJournal = journals.createRecipientResolver("manual:session-ready", "session-a")()!;
    const originalSnapshot = originalJournal.journal.read();
    assert.equal(originalSnapshot.entries.length, 1);
    assert.equal(getTelegramUpdateJournalBindingPath(originalJournal.recoveryKey),
      resolveTelegramSessionJournalPath("session-a", "manual:session-ready", dir, "work"));
    entered = Promise.withResolvers<void>();
    released = Promise.withResolvers<void>();
    pausePreparation = true;
    sessionGeneration++;
    refresh = Promise.resolve(assembly.registration.setContext(ctx));
    await entered.promise;
    assert.equal(assembly.getReadySessionId(), undefined);
    sessionId = "session-b";
    released.resolve();
    await refresh;
    assert.equal(assembly.getReadySessionId(), undefined, "Refresh cannot publish an ID captured before an await");
    assert.equal(journals.resolveFollower(), undefined, "Lifecycle lookup cannot select an unacknowledged successor journal");
    assert.equal((await send())?.ok, false);
    assert.equal(admissions, 1);
    await assert.rejects(async () => assembly.registration.setContext(ctx), /requires acknowledged leader registration/);
    pausePreparation = false;
    assert.equal(await assembly.registration.registerWithLeader(ctx, { busSocketPath: leaderSocketPath }), true);
    assert.equal(assembly.getReadySessionId(), "session-b");
    assert.equal((await send())?.ok, true);
    assert.equal(admissions, 2);
    assert.deepEqual(originalJournal.journal.read(), originalSnapshot, "Refresh and successor admission cannot consume old-session custody");
    const successorJournal = journals.resolveFollower()!;
    assert.equal(successorJournal.journal.read().entries.length, 1);
    assert.equal(getTelegramUpdateJournalBindingPath(successorJournal.recoveryKey),
      resolveTelegramSessionJournalPath("session-b", "manual:session-ready", dir, "work"));
    assert.notEqual(successorJournal.recoveryKey, originalJournal.recoveryKey);
    assert.equal(existsSync(resolveTelegramFollowerJournalPath("manual:session-ready", dir, "work")), false,
      "Session-aware admission must not publish a new flat follower inbox");
    assembly.registration.stop();
    assert.equal(assembly.getReadySessionId(), undefined);
  } finally {
    released.resolve();
    await registration?.catch(() => undefined);
    await refresh?.catch(() => undefined);
    assembly.registration.stop();
    await assembly.receiver.stop();
    await leader.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Acknowledged follower session identity survives same-generation metadata but never crosses generations", () => {
  const state = createTelegramBusFollowerRegistrationState();
  const target = { chatId: 7, threadId: 42 };
  assert.equal(state.getSessionId("session-a"), undefined);
  state.setRegistered(true, target, { generation: "generation-a", sessionId: "session-a" });
  assert.equal(state.getSessionId("session-a"), "session-a");
  assert.equal(state.getSessionId("session-b"), undefined);
  assert.equal(state.getSessionId(undefined), undefined);
  state.setRegistered(true, { ...target, threadId: 43 }, { generation: "generation-a", slot: "A" });
  assert.equal(state.getSessionId("session-a"), "session-a", "Restore/rename metadata keeps acknowledged session identity");
  state.setRegistered(true, target, { generation: "generation-b" });
  assert.equal(state.getSessionId("session-a"), undefined, "A new generation cannot inherit an older acknowledgement");
  state.setRegistered(true, target, { generation: "generation-b", sessionId: "session-b" });
  assert.equal(state.getSessionId("session-b"), "session-b");
  state.setRegistered(false);
  assert.equal(state.getSessionId("session-b"), undefined);
});

test("Bus follower registration state tracks successful registration and stop", async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-bus-follower-state-"));
  const socketPath = join(dir, "bus.sock");
  const registry = createTelegramBusFollowerRegistry();
  const availability: boolean[] = [];
  let state: ReturnType<typeof createTelegramBusFollowerRegistrationState>;
  state = createTelegramBusFollowerRegistrationState({
    onAvailabilityChanged: () => availability.push(state.isRegistered()),
  });
  const server = createTelegramBusLocalServer({
    socketPath,
    handleEnvelope: createTelegramBusLeaderEnvelopeHandler({
      followerRegistry: registry,
      provisionFollowerTarget() {
        return {
          chatId: -1007,
          threadId: 42,
          slot: "E",
          threadName: "Ember",
        };
      },
    }),
  });
  const follower = createTelegramBusFollowerRegistrationRuntime({
    instanceId: "inst-a",
    createRequestId: () => "inst-a:1",
    getNowMs: () => 1000,
    registrationState: state,
  });
  try {
    await server.start();
    assert.equal(state.isRegistered(), false);
    assert.equal(state.getTarget(), undefined);
    assert.equal(
      await follower.registerWithLeader(
        { cwd: "/repo" },
        { busSocketPath: socketPath },
      ),
      true,
    );
    assert.equal(state.isRegistered(), true);
    assert.deepEqual(state.getTarget(), { chatId: -1007, threadId: 42 });
    assert.equal(state.getSlot(), "E");
    assert.equal(state.getThreadName(), "Ember");
    follower.stop();
    assert.equal(state.isRegistered(), false);
    assert.equal(state.getTarget(), undefined);
    assert.equal(state.getSlot(), undefined);
    assert.equal(state.getThreadName(), undefined);
    assert.deepEqual(availability, [true, false]);
  } finally {
    follower.stop();
    await server.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Bus follower restore-only registration exits quietly without a remembered Workspace", async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-follower-auto-connect-"));
  const socketPath = join(dir, "bus.sock");
  let restoreOnly = false;
  const reasons: string[] = [];
  const registry = createTelegramBusFollowerRegistry();
  const server = createTelegramBusLocalServer({
    socketPath,
    handleEnvelope: createTelegramBusLeaderEnvelopeHandler({
      followerRegistry: registry,
      provisionFollowerTarget(_registration, options) {
        restoreOnly = options?.existingWorkspaceBindingOnly === true;
        return undefined;
      },
    }),
  });
  const state = createTelegramBusFollowerRegistrationState();
  const follower = createTelegramBusFollowerRegistrationRuntime({
    instanceId: "inst-a",
    createRequestId: () => "inst-a:restore:1",
    registrationState: state,
    recordRuntimeEvent(_category, _message, details) {
      if (typeof details?.reason === "string") reasons.push(details.reason);
    },
  });
  try {
    await server.start();
    assert.equal(
      await follower.registerWithLeader(
        { cwd: "/repo" },
        { busSocketPath: socketPath },
        { restoreWorkspace: true },
      ),
      false,
    );
    assert.equal(restoreOnly, true);
    assert.deepEqual(reasons, ["leader-binding-unavailable"]);
    assert.equal(state.isRegistered(), false);
    assert.equal(registry.get("inst-a"), undefined);
  } finally {
    follower.stop();
    await server.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Restore-only registration carries its acknowledged title before the first heartbeat without allocating missing Workspaces", { timeout: 5000 }, async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-display-auto-connect-"));
  const socketPath = join(dir, "bus.sock");
  const store = createTelegramTopicTargetStore({ path: join(dir, "state.json"), getNowMs: () => 1000 });
  store.upsertWorkspaceBinding({
    ...createTelegramWorkspaceBindingIdentity("/repo", 0, "test-session")!,
    target: { chatId: 7, threadId: 42 }, slot: "A", threadName: "Anchor",
    displayTitle: "repo_a", updatedAtMs: 1 });
  const calls: string[] = [];
  let syncState = {};
  const provisionFollowerTarget = createTelegramBusFollowerTargetProvisioner({
    getAllowedUserId: () => 7, topicTargetStore: store,
    async callApi<TResponse>(method: string) {
      calls.push(method);
      if (method === "createForumTopic") throw new Error("restore-only startup must not create a Thread");
      return { ok: true } as TResponse;
    },
    getSyncState: () => syncState, setSyncState(state) { syncState = state; },
    recordRuntimeEvent() {}, getNowMs: () => 1000,
  });
  const registry = createTelegramBusFollowerRegistry();
  const protocol = createTelegramBusProtocolIdentity({ runtimeBuild: "test",
    capabilities: [...TEST_BUS_PROTOCOL_IDENTITY.capabilities, TELEGRAM_BUS_CAPABILITY_THREAD_DISPLAY_MODE] });
  const server = createTelegramBusLocalServer({
    socketPath,
    handleEnvelope: createTelegramBusLeaderEnvelopeHandler({
      followerRegistry: registry, protocolIdentity: protocol,
      getThreadDisplayMode: () => "directories", provisionFollowerTarget,
      getFollowerDisplayTitle(follower) {
        return store.listWorkspaceBindings().find((binding) =>
          binding.target.chatId === follower.target?.chatId &&
          binding.target.threadId === follower.target?.threadId,
        )?.displayTitle;
      },
    }),
  });
  const state = createTelegramBusFollowerRegistrationState();
  let titleAtRegistration: string | undefined;
  let sequence = 0;
  const follower = createTelegramBusFollowerRegistrationRuntime({
    instanceId: "reopened", protocolIdentity: protocol,
    createRequestId: () => `reopened:${++sequence}`, registrationState: state,
    heartbeatMs: 60_000,
    onRegistered() { titleAtRegistration = state.getDisplayTitle(); },
  });
  const missingState = createTelegramBusFollowerRegistrationState();
  const missing = createTelegramBusFollowerRegistrationRuntime({
    instanceId: "missing", protocolIdentity: protocol,
    createRequestId: () => `missing:${++sequence}`, registrationState: missingState,
  });
  try {
    await store.persist();
    await server.start();
    assert.equal(await follower.registerWithLeader({ cwd: "/repo/" }, { busSocketPath: socketPath },
      { restoreWorkspace: true }), true);
    assert.equal(titleAtRegistration, "repo_a");
    assert.equal(state.getThreadName(), "Anchor");
    assert.equal(state.getDisplayTitle(), "repo_a");
    assert.deepEqual(state.getTarget(), { chatId: 7, threadId: 42 });
    assert.equal(state.getSlot(), "A");
    assert.equal(await missing.registerWithLeader({ cwd: "/missing" }, { busSocketPath: socketPath },
      { restoreWorkspace: true }), false);
    assert.equal(missingState.isRegistered(), false);
    assert.equal(store.hasWorkspaceBinding("/missing"), false);
    assert.equal(registry.get("missing"), undefined);
    assert.deepEqual(calls, ["sendMessage"]);
  } finally {
    follower.stop(); missing.stop();
    await server.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Heartbeat ACK carries display titles without changing the follower's stable name", { timeout: 5000 }, async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-display-heartbeat-"));
  const socketPath = join(dir, "bus.sock");
  const state = createTelegramBusFollowerRegistrationState();
  let shown!: () => void;
  const updated = new Promise<void>((resolve) => { shown = resolve; });
  let displayTitle: string | undefined;
  const server = createTelegramBusLocalServer({
    socketPath,
    handleEnvelope: createTelegramBusLeaderEnvelopeHandler({
      followerRegistry: createTelegramBusFollowerRegistry(),
      provisionFollowerTarget: () => ({ chatId: 7, threadId: 42, slot: "A", threadName: "Anchor" }),
      getFollowerDisplayTitle: () => displayTitle,
    }),
  });
  let sequence = 0;
  const follower = createTelegramBusFollowerRegistrationRuntime({
    instanceId: "inst-a",
    createRequestId: () => `inst-a:${++sequence}`,
    registrationState: state,
    heartbeatMs: 5,
    onDisplayTitleChanged() {
      shown();
      throw new Error("UI unavailable");
    },
  });
  try {
    await server.start();
    await follower.registerWithLeader({ cwd: "/repo" }, { busSocketPath: socketPath });
    displayTitle = "extensions_a";
    await updated;
    assert.equal(state.getDisplayTitle(), "extensions_a");
    assert.equal(state.getThreadName(), "Anchor");
    assert.equal(state.isRegistered(), true);
    assert.equal(state.setDisplayTitle("obsolete", "wrong-generation"), false);
    assert.equal(state.getDisplayTitle(), "extensions_a");
    follower.stop();
    assert.equal(state.getDisplayTitle(), undefined);
  } finally {
    follower.stop();
    await server.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Follower display setting requests negotiate capability and reject lost leader authority", async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-display-setting-ipc-"));
  const socketPath = join(dir, "bus.sock");
  const protocol = createTelegramBusProtocolIdentity({ runtimeBuild: "test",
    capabilities: [...TEST_BUS_PROTOCOL_IDENTITY.capabilities, TELEGRAM_BUS_CAPABILITY_THREAD_DISPLAY_MODE] });
  let epoch = 1;
  const modes: string[] = [];
  const server = createTelegramBusLocalServer({ socketPath,
    handleEnvelope: createTelegramBusLeaderEnvelopeHandler({
      protocolIdentity: protocol,
      followerRegistry: createTelegramBusFollowerRegistry(),
      getCurrentLeaderEpoch: () => epoch,
      provisionFollowerTarget: () => ({ chatId: 7, threadId: 42, slot: "A" }),
      async applyThreadDisplayMode(mode, isCurrent) {
        assert.equal(isCurrent(), true);
        modes.push(mode);
        if (mode === "names") epoch++;
      },
    }),
  });
  const state = createTelegramBusFollowerRegistrationState();
  let sequence = 0;
  const follower = createTelegramBusFollowerRegistrationRuntime({
    instanceId: "follower", protocolIdentity: protocol, registrationState: state,
    createRequestId: () => `follower:${++sequence}`,
  });
  try {
    await server.start();
    await follower.registerWithLeader({ cwd: "/repo" }, { busSocketPath: socketPath });
    await follower.setThreadDisplayMode?.("letters");
    assert.deepEqual(modes, ["letters"]);
    await assert.rejects(follower.setThreadDisplayMode!("names"), /stale registration/);
    state.setRegistered(true, state.getTarget(), {
      generation: state.getGeneration(), leaderProtocol: TEST_BUS_PROTOCOL_IDENTITY,
    });
    await assert.rejects(follower.setThreadDisplayMode!("letters"), /do not support/);
    assert.deepEqual(modes, ["letters", "names"]);
  } finally {
    follower.stop(); await server.stop(); rmSync(dir, { recursive: true, force: true });
  }
});

test("Registration title admission rejects malformed titles and requires target and generation", () => {
  const state = createTelegramBusFollowerRegistrationState();
  const target = { chatId: 7, threadId: 42 };
  for (const displayTitle of ["", "  ", "x".repeat(129)]) {
    state.setRegistered(true, target, { generation: "one", displayTitle });
    assert.equal(state.getDisplayTitle(), undefined);
  }
  state.setRegistered(true, target, { displayTitle: "repo" });
  assert.equal(state.getDisplayTitle(), undefined);
  state.setRegistered(true, undefined, { generation: "one", displayTitle: "repo" });
  assert.equal(state.getDisplayTitle(), undefined);
  state.setRegistered(true, target, { generation: "one", displayTitle: "repo", threadName: "Anchor" });
  assert.equal(state.getDisplayTitle(), "repo");
  assert.equal(state.getThreadName(), "Anchor");
  state.setRegistered(false);
  assert.equal(state.getDisplayTitle(), undefined);
});

test("Follower metadata refresh keeps the display title only within one target and generation", () => {
  const state = createTelegramBusFollowerRegistrationState();
  const target = { chatId: 7, threadId: 42 };
  state.setRegistered(true, target, { generation: "one", threadName: "Anchor" });
  assert.equal(state.setDisplayTitle("repo_a", "one"), true);
  state.setRegistered(true, target, { generation: "one", threadName: "Navigator" });
  assert.equal(state.getDisplayTitle(), "repo_a");
  assert.equal(state.getThreadName(), "Navigator");
  state.setRegistered(true, target, { generation: "two", threadName: "Navigator" });
  assert.equal(state.getDisplayTitle(), undefined);
  state.setDisplayTitle("repo_a", "two");
  state.setRegistered(true, { chatId: 7, threadId: 43 }, { generation: "two", threadName: "Navigator" });
  assert.equal(state.getDisplayTitle(), undefined);
});

test("Bus follower re-registration carries its last known target", async () => {
  const dir = mkdtempSync(
    join(tmpdir(), "pi-telegram-follower-reload-target-"),
  );
  const socketPath = join(dir, "bus.sock");
  const state = createTelegramBusFollowerRegistrationState();
  const registrations: Array<{
    target?: unknown;
    slot?: string;
    threadName?: string;
  }> = [];
  const server = createTelegramBusLocalServer({
    socketPath,
    handleEnvelope: createTelegramBusLeaderEnvelopeHandler({
      followerRegistry: createTelegramBusFollowerRegistry(),
      provisionFollowerTarget(registration) {
        registrations.push({
          target: registration.target,
          slot: registration.slot,
          threadName: registration.threadName,
        });
        return {
          chatId: 7,
          threadId: 42,
          slot: "E",
          threadName: "Ember",
        };
      },
    }),
  });
  let requestSequence = 0;
  const follower = createTelegramBusFollowerRegistrationRuntime({
    instanceId: "inst-a",
    createRequestId: () => `inst-a:reload:${++requestSequence}`,
    registrationState: state,
  });
  try {
    await server.start();
    assert.equal(
      await follower.registerWithLeader(
        { cwd: "/repo" },
        { busSocketPath: socketPath },
      ),
      true,
    );
    state.setRegistered(false);
    assert.equal(
      await follower.registerWithLeader(
        { cwd: "/repo" },
        { busSocketPath: socketPath },
      ),
      true,
    );
    assert.deepEqual(registrations, [
      { target: undefined, slot: undefined, threadName: "repo" },
      {
        target: { chatId: 7, threadId: 42 },
        slot: "E",
        threadName: "Ember",
      },
    ]);
  } finally {
    follower.stop();
    await server.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Bus follower registration runtime retries while leader endpoint is starting", async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-bus-follower-retry-"));
  const socketPath = join(dir, "bus.sock");
  const registry = createTelegramBusFollowerRegistry();
  const state = createTelegramBusFollowerRegistrationState();
  const events: Array<Record<string, unknown> | undefined> = [];
  const server = createTelegramBusLocalServer({
    socketPath,
    handleEnvelope: createTelegramBusLeaderEnvelopeHandler({
      followerRegistry: registry,
      provisionFollowerTarget() {
        return { chatId: -1007, threadId: 42, slot: "A" };
      },
    }),
  });
  const follower = createTelegramBusFollowerRegistrationRuntime({
    instanceId: "inst-a",
    createRequestId: () => "inst-a:1",
    getNowMs: () => 1000,
    registrationState: state,
    registrationTimeoutMs: 50,
    registrationRetryAttempts: 10,
    registrationRetryDelayMs: 10,
    recordRuntimeEvent(_category, _error, details) {
      events.push(details);
    },
  });
  try {
    setTimeout(() => {
      void server.start();
    }, 25);
    assert.equal(
      await follower.registerWithLeader(
        { cwd: "/repo" },
        { busSocketPath: socketPath },
      ),
      true,
    );
    assert.equal(state.isRegistered(), true);
    assert.deepEqual(state.getTarget(), { chatId: -1007, threadId: 42 });
    assert.equal(
      events.some((event) => event?.phase === "follower-register-client-retry"),
      true,
    );
  } finally {
    follower.stop();
    await server.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Bus follower registration runtime waits for slow target provisioning", async () => {
  const dir = mkdtempSync(
    join(tmpdir(), "pi-telegram-bus-follower-slow-register-"),
  );
  const socketPath = join(dir, "bus.sock");
  const registry = createTelegramBusFollowerRegistry();
  const state = createTelegramBusFollowerRegistrationState();
  const server = createTelegramBusLocalServer({
    socketPath,
    handleEnvelope: createTelegramBusLeaderEnvelopeHandler({
      followerRegistry: registry,
      async provisionFollowerTarget() {
        await new Promise((resolve) => setTimeout(resolve, 50));
        return { chatId: -1007, threadId: 42, slot: "A" };
      },
    }),
  });
  const follower = createTelegramBusFollowerRegistrationRuntime({
    instanceId: "inst-a",
    createRequestId: () => "inst-a:1",
    getNowMs: () => 1000,
    registrationState: state,
    timeoutMs: 20,
    registrationTimeoutMs: 250,
  });
  try {
    await server.start();
    assert.equal(
      await follower.registerWithLeader(
        { cwd: "/repo" },
        { busSocketPath: socketPath },
      ),
      true,
    );
    assert.equal(state.isRegistered(), true);
    assert.deepEqual(state.getTarget(), { chatId: -1007, threadId: 42 });
    assert.deepEqual(registry.get("inst-a")?.target, {
      chatId: -1007,
      threadId: 42,
      slot: "A",
    });
  } finally {
    follower.stop();
    await server.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Bus follower registration runtime registers and explicitly disconnects", async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-bus-follower-"));
  const socketPath = join(dir, "bus.sock");
  const registry = createTelegramBusFollowerRegistry();
  const leaderProtocol = createTelegramBusProtocolIdentity({
    runtimeBuild: "0.28.0",
    capabilities: [TELEGRAM_BUS_CAPABILITY_WORKSPACE_THREAD_RENAME],
  });
  const followerProtocol = createTelegramBusProtocolIdentity({
    runtimeBuild: "0.28.1",
    capabilities: [TELEGRAM_BUS_CAPABILITY_WORKSPACE_THREAD_RENAME],
  });
  const registrationState = createTelegramBusFollowerRegistrationState();
  let disconnects = 0;
  const renames: string[] = [];
  const server = createTelegramBusLocalServer({
    socketPath,
    handleEnvelope: createTelegramBusLeaderEnvelopeHandler({
      followerRegistry: registry,
      protocolIdentity: leaderProtocol,
      getNowMs: () => 1000,
      provisionFollowerTarget() {
        return { chatId: 7, threadId: 42, slot: "A" };
      },
      onFollowerDisconnected() {
        disconnects += 1;
      },
      renameFollowerThread(_follower, threadName) {
        renames.push(threadName);
        return { threadName };
      },
      resetFollowerThreadName() {
        return { threadName: "A" };
      },
    }),
  });
  let sequence = 0;
  const follower = createTelegramBusFollowerRegistrationRuntime({
    instanceId: "inst-a",
    createRequestId: () => `inst-a:${++sequence}`,
    protocolIdentity: followerProtocol,
    registrationState,
    getNowMs: () => 1000,
    getPid: () => 123,
    getProcessBirthId: () => "123:start:abc",
    getSessionId: () => "session-a",
    getSessionGeneration: () => 4,
  });
  try {
    await server.start();
    assert.equal(
      await follower.registerWithLeader(
        { cwd: "/repo" },
        { busSocketPath: socketPath },
      ),
      true,
    );
    assert.deepEqual(registry.get("inst-a"), {
      instanceId: "inst-a",
      profileKey: "cwd:/repo",
      threadName: "repo",
      cwd: "/repo",
      sessionId: "session-a",
      pid: 123,
      processBirthId: "123:start:abc",
      sessionGeneration: 4,
      registrationGeneration: "inst-a:1",
      protocol: followerProtocol,
      connectedAtMs: 1000,
      lastHeartbeatMs: 1000,
      target: { chatId: 7, threadId: 42, slot: "A" },
      slot: "A",
    });
    assert.deepEqual(registrationState.getLeaderProtocol(), leaderProtocol);
    assert.equal(await follower.renameThread?.(
      { chatId: 7, threadId: 42 }, "Navigator",
    ), "Navigator");
    assert.deepEqual(renames, ["Navigator"]);
    assert.equal(registry.get("inst-a")?.threadName, "Navigator");
    assert.equal(registrationState.getThreadName(), "Navigator");
    assert.equal(await follower.resetThreadName?.(
      { chatId: 7, threadId: 42 },
    ), "A");
    assert.equal(registry.get("inst-a")?.threadName, "A");
    assert.equal(registrationState.getThreadName(), "A");
    assert.equal(await follower.disconnectFromLeader?.(), true);
    assert.equal(disconnects, 1);
    assert.equal(registry.get("inst-a"), undefined);
  } finally {
    follower.stop();
    await server.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Bus follower rejects an acknowledgement without protocol identity", async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-bus-follower-protocol-"));
  const socketPath = join(dir, "bus.sock");
  const server = createRawTelegramBusLocalServer({
    socketPath,
    handleEnvelope: (envelope) => ({
      kind: "bus.ack",
      requestId: envelope.requestId,
      ok: true,
    }),
  });
  const state = createTelegramBusFollowerRegistrationState();
  const follower = createTelegramBusFollowerRegistrationRuntime({
    instanceId: "inst-a",
    createRequestId: () => "inst-a:1",
    protocolIdentity: createTelegramBusProtocolIdentity({
      runtimeBuild: "0.28.0",
    }),
    registrationState: state,
    getNowMs: () => 1000,
  });
  try {
    await server.start();
    await assert.rejects(
      follower.registerWithLeader(
        { cwd: "/repo" },
        { busSocketPath: socketPath },
      ),
      /missing-identity/u,
    );
    assert.equal(state.isRegistered(), false);
  } finally {
    follower.stop();
    await server.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Bus follower reports an identity-less rejection as the rejection, not a protocol mismatch", async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-bus-follower-rejection-"));
  const socketPath = join(dir, "bus.sock");
  const server = createRawTelegramBusLocalServer({
    socketPath,
    handleEnvelope: (envelope) => ({ kind: "bus.ack", requestId: envelope.requestId, ok: false,
      message: "Telegram bus handler failed." }),
  });
  const state = createTelegramBusFollowerRegistrationState();
  const follower = createTelegramBusFollowerRegistrationRuntime({
    instanceId: "inst-a", createRequestId: () => "inst-a:1",
    protocolIdentity: createTelegramBusProtocolIdentity({ runtimeBuild: "0.28.0" }),
    registrationState: state, getNowMs: () => 1000,
  });
  try {
    await server.start();
    await assert.rejects(follower.registerWithLeader({ cwd: "/repo" }, { busSocketPath: socketPath }),
      (error: unknown) => error instanceof Error && error.message === "Telegram bus handler failed.");
    assert.equal(state.isRegistered(), false);
  } finally {
    follower.stop();
    await server.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Bus follower rejects a pre-session protocol leader", async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-bus-follower-capability-"));
  const socketPath = join(dir, "bus.sock");
  const server = createTelegramBusLocalServer({
    socketPath,
    handleEnvelope: (envelope) => ({
      kind: "bus.ack",
      requestId: envelope.requestId,
      ok: true,
      protocol: {
        protocolVersion: 1,
        runtimeBuild: "0.45.11",
        capabilities: [TELEGRAM_BUS_CAPABILITY_DURABLE_FOLLOWER_ADMISSION],
      },
    }),
  });
  const state = createTelegramBusFollowerRegistrationState();
  const follower = createTelegramBusFollowerRegistrationRuntime({
    instanceId: "inst-a",
    createRequestId: () => "inst-a:1",
    protocolIdentity: createTelegramBusProtocolIdentity({
      runtimeBuild: "0.28.0",
      capabilities: [TELEGRAM_BUS_CAPABILITY_DURABLE_FOLLOWER_ADMISSION],
    }),
    registrationState: state,
  });
  try {
    await server.start();
    await assert.rejects(follower.registerWithLeader(
      { cwd: "/repo" },
      { busSocketPath: socketPath },
    ), /version-mismatch/u);
    assert.equal(state.isRegistered(), false);
  } finally {
    follower.stop();
    await server.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Bus follower registration runtime accepts explicit manual profile keys", async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-bus-follower-profile-"));
  const socketPath = join(dir, "bus.sock");
  const registry = createTelegramBusFollowerRegistry();
  const server = createTelegramBusLocalServer({
    socketPath,
    handleEnvelope: createTelegramBusLeaderEnvelopeHandler({
      followerRegistry: registry,
    }),
  });
  const follower = createTelegramBusFollowerRegistrationRuntime({
    instanceId: "inst-a",
    createRequestId: () => "inst-a:1",
    getNowMs: () => 1000,
    getProfileKey: () => "manual:inst-a",
  });
  try {
    await server.start();
    assert.equal(
      await follower.registerWithLeader(
        { cwd: "/repo" },
        { busSocketPath: socketPath },
      ),
      true,
    );
    assert.equal(registry.get("inst-a")?.profileKey, "manual:inst-a");
  } finally {
    await server.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Bus follower heartbeat tolerates one delayed leader acknowledgement without flapping", async () => {
  assert.equal(
    TELEGRAM_BUS_FOLLOWER_HEARTBEAT_TIMEOUT_MS,
    TELEGRAM_BUS_LEADER_STALE_HEARTBEAT_MS,
  );
  const dir = mkdtempSync(
    join(tmpdir(), "pi-telegram-bus-follower-heartbeat-delay-"),
  );
  const socketPath = join(dir, "bus.sock");
  const state = createTelegramBusFollowerRegistrationState();
  const failures: unknown[] = [];
  let heartbeatCalls = 0;
  const server = createTelegramBusLocalServer({
    socketPath,
    async handleEnvelope(envelope) {
      if (envelope.kind === "follower.heartbeat") {
        heartbeatCalls += 1;
        if (heartbeatCalls === 2) {
          await new Promise((resolve) => setTimeout(resolve, 1_500));
        }
      }
      return {
        kind: "bus.ack",
        requestId: envelope.requestId,
        ok: true,
      };
    },
  });
  let requestSequence = 0;
  const follower = createTelegramBusFollowerRegistrationRuntime({
    instanceId: "inst-a",
    createRequestId: () => `inst-a:${++requestSequence}`,
    registrationState: state,
    heartbeatMs: 5,
    onHeartbeatFailure(error) {
      failures.push(error);
    },
  });
  try {
    await server.start();
    assert.equal(
      await follower.registerWithLeader(
        { cwd: "/repo" },
        { busSocketPath: socketPath },
      ),
      true,
    );
    await waitForCondition(() => heartbeatCalls >= 3, 2_500);
    assert.deepEqual(failures, []);
    assert.equal(state.isRegistered(), true);
  } finally {
    follower.stop();
    await server.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Bus follower registration runtime reports heartbeat failure with active context", async () => {
  const dir = mkdtempSync(
    join(tmpdir(), "pi-telegram-bus-follower-heartbeat-fail-"),
  );
  const socketPath = join(dir, "bus.sock");
  const failures: unknown[] = [];
  const server = createTelegramBusLocalServer({
    socketPath,
    handleEnvelope: (envelope) => ({
      kind: "bus.ack",
      requestId: envelope.requestId,
      ok: true,
    }),
  });
  const follower = createTelegramBusFollowerRegistrationRuntime({
    instanceId: "inst-a",
    createRequestId: () => "inst-a:1",
    registrationState: createTelegramBusFollowerRegistrationState(),
    heartbeatMs: 10,
    timeoutMs: 50,
    onHeartbeatFailure(error, ctx) {
      failures.push({ error: String(error), ctx });
    },
  });
  try {
    await server.start();
    await follower.registerWithLeader(
      { cwd: "/repo" },
      { busSocketPath: socketPath },
    );
    await server.stop();
    await waitForCondition(() => failures.length > 0, 200);
    assert.deepEqual((failures[0] as { ctx: unknown }).ctx, { cwd: "/repo" });
  } finally {
    follower.stop();
    await server.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Bus follower registration runtime reports rejected heartbeat with active context", async () => {
  const dir = mkdtempSync(
    join(tmpdir(), "pi-telegram-bus-follower-heartbeat-reject-"),
  );
  const socketPath = join(dir, "bus.sock");
  const failures: unknown[] = [];
  let requestSequence = 0;
  const server = createTelegramBusLocalServer({
    socketPath,
    handleEnvelope: (envelope) => ({
      kind: "bus.ack",
      requestId: envelope.requestId,
      ok: envelope.kind === "follower.register",
      message:
        envelope.kind === "follower.register"
          ? undefined
          : "Unknown Telegram bus follower instance.",
    }),
  });
  const follower = createTelegramBusFollowerRegistrationRuntime({
    instanceId: "inst-a",
    createRequestId: () => `inst-a:${++requestSequence}`,
    registrationState: createTelegramBusFollowerRegistrationState(),
    heartbeatMs: 10,
    timeoutMs: 50,
    onHeartbeatFailure(error, ctx) {
      failures.push({ error: String(error), ctx });
    },
  });
  try {
    await server.start();
    await follower.registerWithLeader(
      { cwd: "/repo" },
      { busSocketPath: socketPath },
    );
    await waitForCondition(() => failures.length > 0, 200);
    assert.deepEqual(failures[0], {
      error: "Error: Unknown Telegram bus follower instance.",
      ctx: { cwd: "/repo" },
    });
  } finally {
    follower.stop();
    await server.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Bus follower registration runtime owns one in-flight heartbeat", async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-bus-heartbeat-gate-"));
  const socketPath = join(dir, "bus.sock");
  let heartbeatCalls = 0;
  let releaseBlockedHeartbeat: (() => void) | undefined;
  const blockedHeartbeat = new Promise<void>((resolve) => {
    releaseBlockedHeartbeat = resolve;
  });
  const server = createTelegramBusLocalServer({
    socketPath,
    async handleEnvelope(envelope) {
      if (envelope.kind === "follower.register") {
        return { kind: "bus.ack", requestId: envelope.requestId, ok: true };
      }
      heartbeatCalls += 1;
      if (heartbeatCalls > 1) await blockedHeartbeat;
      return { kind: "bus.ack", requestId: envelope.requestId, ok: true };
    },
  });
  let requestSequence = 0;
  const follower = createTelegramBusFollowerRegistrationRuntime({
    instanceId: "inst-a",
    createRequestId: () => `inst-a:${++requestSequence}`,
    registrationState: createTelegramBusFollowerRegistrationState(),
    heartbeatMs: 5,
  });
  try {
    await server.start();
    await follower.registerWithLeader(
      { cwd: "/repo" },
      { busSocketPath: socketPath },
    );
    await waitForCondition(() => heartbeatCalls === 2, 100);
    await new Promise((resolve) => setTimeout(resolve, 30));
    assert.equal(heartbeatCalls, 2);
    follower.stop();
    releaseBlockedHeartbeat?.();
  } finally {
    follower.stop();
    releaseBlockedHeartbeat?.();
    await server.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Bus follower registration runtime heartbeats until stopped", async () => {
  const dir = mkdtempSync(
    join(tmpdir(), "pi-telegram-bus-follower-heartbeat-"),
  );
  const socketPath = join(dir, "bus.sock");
  const registry = createTelegramBusFollowerRegistry();
  let nowMs = 1000;
  const server = createTelegramBusLocalServer({
    socketPath,
    handleEnvelope: createTelegramBusLeaderEnvelopeHandler({
      followerRegistry: registry,
      getNowMs: () => nowMs,
    }),
  });
  let requestSequence = 0;
  const follower = createTelegramBusFollowerRegistrationRuntime({
    instanceId: "inst-a",
    createRequestId: () => `inst-a:${++requestSequence}`,
    getNowMs: () => nowMs,
    heartbeatMs: 50,
  });
  try {
    await server.start();
    assert.equal(
      await follower.registerWithLeader(
        { cwd: "/repo" },
        { busSocketPath: socketPath },
      ),
      true,
    );
    nowMs = 2000;
    await waitForCondition(
      () => registry.get("inst-a")?.lastHeartbeatMs === 2000,
      500,
    );
    follower.stop();
    nowMs = 3000;
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.equal(registry.get("inst-a")?.lastHeartbeatMs, 2000);
  } finally {
    follower.stop();
    await server.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Bus follower registration runtime surfaces leader rejection reasons", async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-bus-follower-reject-"));
  const socketPath = join(dir, "bus.sock");
  const server = createTelegramBusLocalServer({
    socketPath,
    handleEnvelope: () => ({
      kind: "bus.ack",
      requestId: "inst-a:1",
      ok: false,
      message: "Unauthorized Telegram bus envelope.",
    }),
  });
  const stopped: string[] = [];
  const follower = createTelegramBusFollowerRegistrationRuntime({
    instanceId: "inst-a",
    createRequestId: () => "inst-a:1",
    stopReceiving: () => {
      stopped.push("stop");
    },
  });
  try {
    await server.start();
    await assert.rejects(
      () =>
        follower.registerWithLeader(
          { cwd: "/repo" },
          { busSocketPath: socketPath },
        ),
      /Unauthorized Telegram bus envelope/,
    );
    assert.deepEqual(stopped, ["stop"]);
  } finally {
    await server.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Bus follower registration runtime derives leader socket when lock omits it", async () => {
  const dir = mkdtempSync(
    join(tmpdir(), "pi-telegram-bus-follower-derived-socket-"),
  );
  const socketPath = join(dir, "bus.sock");
  const registry = createTelegramBusFollowerRegistry();
  const server = createTelegramBusLocalServer({
    socketPath,
    handleEnvelope: createTelegramBusLeaderEnvelopeHandler({
      followerRegistry: registry,
      getNowMs: () => 1000,
    }),
  });
  const follower = createTelegramBusFollowerRegistrationRuntime({
    instanceId: "inst-a",
    createRequestId: () => "inst-a:1",
    getLeaderSocketPath: () => socketPath,
  });
  try {
    await server.start();
    assert.equal(await follower.registerWithLeader({ cwd: "/repo" }, {}), true);
    assert.equal(registry.get("inst-a")?.instanceId, "inst-a");
  } finally {
    follower.stop();
    await server.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Bus follower queue handoff client rejects a mismatched staged receipt", async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-queue-handoff-mismatch-"));
  const socketPath = join(dir, "leader.sock");
  const server = createTelegramBusLocalServer({
    socketPath,
    handleEnvelope: (envelope) => ({
      kind: "bus.ack",
      requestId: envelope.requestId,
      ok: true,
      result: { status: "staged", receiptId: "wrong", sourceUpdateIds: [1] },
    }),
  });
  const client = createTelegramBusFollowerQueueHandoffClient({
    socketPath,
    instanceId: "donor",
    createRequestId: () => "handoff:mismatch",
    getRegistrationGeneration: () => "donor-generation",
  });
  try {
    await server.start();
    await assert.rejects(
      client({
        recipientInstanceId: "recipient",
        recipientRegistrationGeneration: "recipient-generation",
        donorProcessId: 101,
        donorProcessBirthId: "101:start:donor",
        donorSessionGeneration: 1,
        donorAcquisitionId: "donor-acquisition",
        donorAcquiredAtMs: 1000,
        handoffToken: "x".repeat(32),
        payload: {
          kind: "prompt",
          chatId: 7,
          replyToMessageId: 10,
          queueOrder: 1,
          queueLane: "default",
          laneOrder: 1,
          statusSummary: "handoff",
          admissionReceipts: [
            { queueKind: "prompt", receiptId: "receipt-1", sourceUpdateIds: [1] },
          ],
          sourceMessageIds: [10],
          queuedAttachments: [],
          content: [{ type: "text", text: "handoff prompt" }],
          historyText: "handoff",
        },
      }),
      /queue handoff was rejected/u,
    );
  } finally {
    await server.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Bus follower queue handoff client requires an exact staged acknowledgement", async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-queue-handoff-client-"));
  const socketPath = join(dir, "leader.sock");
  const received: unknown[] = [];
  const server = createTelegramBusLocalServer({
    socketPath,
    handleEnvelope(envelope) {
      received.push(envelope);
      return {
        kind: "bus.ack",
        requestId: envelope.requestId,
        ok: true,
        result: {
          status: "staged",
          receiptId: "receipt-1",
          sourceUpdateIds: [1],
          queueOwner: {
            instanceId: "recipient",
            processId: 202,
            processBirthId: "202:start:recipient",
            sessionGeneration: 2,
            acquisitionId: "recipient-acquisition",
            acquiredAtMs: 2_000,
          },
        },
      };
    },
  });
  const client = createTelegramBusFollowerQueueHandoffClient({
    socketPath,
    instanceId: "donor",
    createRequestId: () => "handoff:1",
    getAuthSecret: () => "secret",
    getRegistrationGeneration: () => "donor-generation",
    getNowMs: () => 2000,
  });
  const payload = {
    kind: "prompt" as const,
    chatId: 7,
    replyToMessageId: 10,
    queueOrder: 1,
    queueLane: "default" as const,
    laneOrder: 1,
    statusSummary: "handoff",
    admissionReceipts: [
      { queueKind: "prompt" as const, receiptId: "receipt-1", sourceUpdateIds: [1] },
    ],
    sourceMessageIds: [10],
    queuedAttachments: [],
    content: [{ type: "text" as const, text: "handoff prompt" }],
    historyText: "handoff",
  };
  try {
    await server.start();
    assert.deepEqual(
      await client({
        recipientInstanceId: "recipient",
        recipientRegistrationGeneration: "recipient-generation",
        donorProcessId: 101,
        donorProcessBirthId: "101:start:donor",
        donorSessionGeneration: 1,
        donorAcquisitionId: "donor-acquisition",
        donorAcquiredAtMs: 1000,
        handoffToken: "x".repeat(32),
        payload,
      }),
      {
        status: "staged",
        receiptId: "receipt-1",
        sourceUpdateIds: [1],
        queueOwner: {
          instanceId: "recipient",
          processId: 202,
          processBirthId: "202:start:recipient",
          sessionGeneration: 2,
          acquisitionId: "recipient-acquisition",
          acquiredAtMs: 2_000,
        },
      },
    );
    assert.deepEqual(received, [
      {
        kind: "follower.offerQueueHandoff",
        requestId: "handoff:1",
        auth: "secret",
        instanceId: "donor",
        registrationGeneration: "donor-generation",
        recipientInstanceId: "recipient",
        recipientRegistrationGeneration: "recipient-generation",
        donorProcessId: 101,
        donorProcessBirthId: "101:start:donor",
        donorSessionGeneration: 1,
        donorAcquisitionId: "donor-acquisition",
        donorAcquiredAtMs: 1000,
        handoffToken: "x".repeat(32),
        payload,
        sentAtMs: 2000,
      },
    ]);
  } finally {
    await server.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Bus follower API caller sends method and multipart voice calls over local transport", async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-bus-api-caller-"));
  const socketPath = join(dir, "bus.sock");
  const voicePath = join(dir, "voice output.ogg");
  const received: unknown[] = [];
  let requestSequence = 0;
  const server = createTelegramBusLocalServer({
    socketPath,
    handleEnvelope: (envelope) => {
      received.push(envelope);
      return {
        kind: "bus.ack",
        requestId: envelope.requestId,
        ok: true,
        result: { message_id: 55 },
      };
    },
  });
  const callApi = createTelegramBusFollowerApiCaller({
    socketPath,
    instanceId: "inst-a",
    createRequestId: () => `inst-a:${++requestSequence}`,
    getRegistrationGeneration: () => "generation-a",
    getNowMs: () => 7000,
  });
  try {
    await server.start();
    assert.deepEqual(await callApi("sendRichMessage", [{ chat_id: 1 }]), {
      message_id: 55,
    });
    assert.deepEqual(
      await callApi("callMultipart", [
        "sendVoice",
        { chat_id: "7", message_thread_id: "42" },
        "voice",
        voicePath,
        "voice output.ogg",
      ]),
      { message_id: 55 },
    );
    assert.deepEqual(received, [
      {
        kind: "follower.callApi",
        requestId: "inst-a:1",
        instanceId: "inst-a",
        registrationGeneration: "generation-a",
        method: "sendRichMessage",
        args: [{ chat_id: 1 }],
        sentAtMs: 7000,
      },
      {
        kind: "follower.callApi",
        requestId: "inst-a:2",
        instanceId: "inst-a",
        registrationGeneration: "generation-a",
        method: "callMultipart",
        args: [
          "sendVoice",
          { chat_id: "7", message_thread_id: "42" },
          "voice",
          voicePath,
          "voice output.ogg",
        ],
        sentAtMs: 7000,
      },
    ]);
  } finally {
    await server.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Bus follower API calls wait for heartbeat recovery before transport", async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-bus-api-recovery-"));
  const socketPath = join(dir, "bus.sock");
  const received: unknown[] = [];
  const registrationState = createTelegramBusFollowerRegistrationState();
  registrationState.setRegistered(
    true,
    { chatId: 1, threadId: 2 },
    { generation: "generation-old" },
  );
  registrationState.beginRecovery();
  registrationState.setRegistered(false);
  const server = createTelegramBusLocalServer({
    socketPath,
    handleEnvelope: (envelope) => {
      received.push(envelope);
      return {
        kind: "bus.ack",
        requestId: envelope.requestId,
        ok: true,
        result: { message_id: 56 },
      };
    },
  });
  const callApi = createTelegramBusFollowerApiCaller({
    socketPath,
    instanceId: "inst-a",
    createRequestId: () => "inst-a:recovery:1",
    getRegistrationGeneration: registrationState.getGeneration,
    waitForRegistrationGeneration: registrationState.waitForGeneration,
    getNowMs: () => 7001,
  });
  try {
    await server.start();
    const delivery = callApi("sendRichMessage", [{ chat_id: 1 }]);
    await new Promise((resolve) => setImmediate(resolve));
    assert.deepEqual(received, []);

    registrationState.setRegistered(
      true,
      { chatId: 1, threadId: 2 },
      { generation: "generation-restored" },
    );

    assert.deepEqual(await delivery, { message_id: 56 });
    assert.equal(received.length, 1);
    assert.equal(
      (received[0] as { registrationGeneration?: string })
        .registrationGeneration,
      "generation-restored",
    );
  } finally {
    await server.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Bus follower API calls fail before transport when registration is not restored", async () => {
  let transportRequested = false;
  const registrationState = createTelegramBusFollowerRegistrationState();
  registrationState.beginRecovery();
  const callApi = createTelegramBusFollowerApiCaller({
    socketPath: () => {
      transportRequested = true;
      return "unused.sock";
    },
    instanceId: "inst-a",
    createRequestId: () => "inst-a:unregistered:1",
    getRegistrationGeneration: registrationState.getGeneration,
    waitForRegistrationGeneration: registrationState.waitForGeneration,
    timeoutMs: 10,
  });

  await assert.rejects(
    () => callApi("sendRichMessage", [{ chat_id: 1 }]),
    /Telegram bus follower is not registered/,
  );
  assert.equal(transportRequested, false);
});

test("Bus follower API calls do not cross an explicit recovery cancellation", async () => {
  let transportRequested = false;
  const registrationState = createTelegramBusFollowerRegistrationState();
  registrationState.beginRecovery();
  const callApi = createTelegramBusFollowerApiCaller({
    socketPath: () => {
      transportRequested = true;
      return "unused.sock";
    },
    instanceId: "inst-a",
    createRequestId: () => "inst-a:cancelled:1",
    getRegistrationGeneration: registrationState.getGeneration,
    waitForRegistrationGeneration: registrationState.waitForGeneration,
  });

  const delivery = callApi("sendRichMessage", [{ chat_id: 1 }]);
  await new Promise((resolve) => setImmediate(resolve));
  registrationState.cancelRecovery();
  registrationState.setRegistered(
    true,
    { chatId: 1, threadId: 2 },
    { generation: "unrelated-generation" },
  );

  await assert.rejects(
    () => delivery,
    /Telegram bus follower is not registered/,
  );
  assert.equal(transportRequested, false);
});

test("Bus follower API caller preserves structured commit-unknown errors", async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-bus-api-ambiguous-"));
  const socketPath = join(dir, "bus.sock");
  const server = createTelegramBusLocalServer({
    socketPath,
    handleEnvelope: (envelope) => ({
      kind: "bus.ack",
      requestId: envelope.requestId,
      ok: false,
      message: "sendMessage response was lost",
      error: { code: "commit-unknown", method: "sendMessage" },
    }),
  });
  const callApi = createTelegramBusFollowerApiCaller({
    socketPath,
    instanceId: "inst-a",
    createRequestId: () => "inst-a:ambiguous:1",
    getRegistrationGeneration: () => "generation-a",
  });
  try {
    await server.start();
    await assert.rejects(
      () => callApi("call", ["sendMessage", { chat_id: 1, text: "hello" }]),
      isTelegramApiCommitUnknownError,
    );
  } finally {
    await server.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Bus follower API caller preserves structured stale-target evidence", async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-bus-api-stale-target-"));
  const socketPath = join(dir, "bus.sock");
  const server = createTelegramBusLocalServer({
    socketPath,
    handleEnvelope: (envelope) => ({
      kind: "bus.ack",
      requestId: envelope.requestId,
      ok: false,
      message: "Bad Request: message thread not found",
      error: { code: "stale-target", chatId: 1, threadId: 2 },
    }),
  });
  const callApi = createTelegramBusFollowerApiCaller({
    socketPath,
    instanceId: "inst-a",
    createRequestId: () => "inst-a:stale-target:1",
    getRegistrationGeneration: () => "generation-a",
  });
  try {
    await server.start();
    const error = await callApi("call", [
      "sendMessage",
      { chat_id: 1, message_thread_id: 2, text: "hello" },
    ]).catch((failure: unknown) => failure);
    assert.deepEqual(getTelegramApiErrorRequestTarget(error), {
      chatId: 1,
      threadId: 2,
    });
  } finally {
    await server.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Bus follower API caller classifies non-idempotent acknowledgement loss as commit-unknown", async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-bus-api-ack-loss-"));
  const socketPath = join(dir, "bus.sock");
  let executions = 0;
  const server = createTelegramBusLocalServer({
    socketPath,
    handleEnvelope: (envelope) => {
      executions += 1;
      return {
        kind: "bus.ack",
        requestId: envelope.requestId,
        ok: true,
        result: { message_id: 77 },
      };
    },
    shouldDropResponse: () => true,
  });
  const callApi = createTelegramBusFollowerApiCaller({
    socketPath,
    instanceId: "inst-a",
    createRequestId: () => "inst-a:ack-loss:1",
    getRegistrationGeneration: () => "generation-a",
    timeoutMs: 100,
  });
  try {
    await server.start();
    await assert.rejects(
      () => callApi("call", ["sendMessage", { chat_id: 1, text: "hello" }]),
      isTelegramApiCommitUnknownError,
    );
    assert.equal(executions, 1);
  } finally {
    await server.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Bus follower initial registration consumes a pending session handoff after acknowledgement", async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-follower-handoff-"));
  const socketPath = join(dir, "bus.sock");
  const registrations: Array<{
    target: unknown;
    previousInstanceId: string | undefined;
  }> = [];
  const server = createTelegramBusLocalServer({
    socketPath,
    handleEnvelope: createTelegramBusLeaderEnvelopeHandler({
      followerRegistry: createTelegramBusFollowerRegistry(),
      provisionFollowerTarget(registration) {
        registrations.push({
          target: registration.target,
          previousInstanceId: registration.previousInstanceId,
        });
        return { chatId: 1, threadId: 2, slot: "B", threadName: "Beryl" };
      },
    }),
  });
  const follower = createTelegramBusFollowerRegistrationRuntime({
    instanceId: "new-inst",
    createRequestId: () => "new-inst:1",
    registrationRetryAttempts: 1,
    registrationTimeoutMs: 50,
    registrationState: createTelegramBusFollowerRegistrationState(),
  });
  setTelegramFollowerSessionHandoff({
    pid: process.pid,
    instanceId: "old-inst",
    createdAtMs: Date.now(),
    target: { chatId: 1, threadId: 2 },
    slot: "B",
    threadName: "Beryl",
  });
  try {
    await assert.rejects(() =>
      follower.registerWithLeader(
        { cwd: "/repo" },
        { busSocketPath: socketPath },
      ),
    );
    assert.equal(getTelegramFollowerSessionHandoff()?.instanceId, "old-inst");

    await server.start();
    assert.equal(
      await follower.registerWithLeader(
        { cwd: "/repo" },
        { busSocketPath: socketPath },
      ),
      true,
    );
    assert.deepEqual(registrations, [
      {
        target: { chatId: 1, threadId: 2 },
        previousInstanceId: "old-inst",
      },
    ]);
    assert.equal(getTelegramFollowerSessionHandoff(), undefined);
  } finally {
    follower.stop();
    setTelegramFollowerSessionHandoff(undefined);
    await server.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Bus follower session replacement preserves a same-process handoff", async () => {
  const registrationState = createTelegramBusFollowerRegistrationState();
  registrationState.setRegistered(
    true,
    { chatId: 1, threadId: 2 },
    { slot: "B", threadName: "Beryl" },
  );
  const events: unknown[] = [];
  let suspended = false;
  const suspend = createTelegramBusFollowerSessionReplacementSuspender({
    registrationState,
    instanceId: "old-inst",
    async suspendPolling() {
      suspended = true;
      registrationState.setRegistered(false);
    },
    recordRuntimeEvent(category, message, details) {
      events.push({ category, message, details });
    },
    getPid: () => 10,
    getNowMs: () => 500,
  });

  await suspend();

  assert.equal(suspended, true);
  assert.equal(registrationState.isRegistered(), false);
  assert.deepEqual(getTelegramFollowerSessionHandoff(), {
    pid: 10,
    instanceId: "old-inst",
    createdAtMs: 500,
    target: { chatId: 1, threadId: 2 },
    slot: "B",
    threadName: "Beryl",
  });
  assert.deepEqual(events, [
    {
      category: "bus",
      message: "Telegram follower registration suspended for session replacement",
      details: {
        phase: "follower-session-handoff",
        instanceId: "old-inst",
        chatId: 1,
        threadId: 2,
      },
    },
  ]);
  await suspend(false);
  assert.equal(getTelegramFollowerSessionHandoff(), undefined, "Resume never hands off source target");
});

test("Bus session replacement preserves the promoted leader binding", async () => {
  const registrationState = createTelegramBusFollowerRegistrationState();
  const events: unknown[] = [];
  const suspend = createTelegramBusFollowerSessionReplacementSuspender({
    registrationState,
    instanceId: "promoted-inst",
    suspendPolling: async () => undefined,
    isLeader: () => true,
    getLeaderBinding: () => ({
      target: { chatId: 1, threadId: 3 },
      slot: "C",
      threadName: "Cinder",
    }),
    getActiveContext: () => ({ cwd: "/repo" }),
    getActiveProfileName: () => "work",
    recordRuntimeEvent(category, message, details) {
      events.push({ category, message, details });
    },
    getPid: () => 10,
    getNowMs: () => 500,
  });

  try {
    await suspend();
    assert.deepEqual(getTelegramLeaderSessionHandoff(), {
      pid: 10,
      instanceId: "promoted-inst",
      createdAtMs: 500,
      profileKey: "profile:work:cwd:/repo",
      target: { chatId: 1, threadId: 3 },
      slot: "C",
      threadName: "Cinder",
    });
    assert.deepEqual(events, [
      {
        category: "bus",
        message: "Telegram leader binding suspended for session replacement",
        details: {
          phase: "leader-session-handoff",
          instanceId: "promoted-inst",
          chatId: 1,
          threadId: 3,
          slot: "C",
          threadName: "Cinder",
        },
      },
    ]);
    await suspend(false);
    assert.equal(getTelegramLeaderSessionHandoff(), undefined, "Resume never hands off source target");
  } finally {
    setTelegramLeaderSessionHandoff(undefined);
  }
});

test("Bus follower session refresh re-registers with the handed-off target", async () => {
  const registrationState = createTelegramBusFollowerRegistrationState();
  const registrations: unknown[] = [];
  const events: unknown[] = [];
  setTelegramFollowerSessionHandoff({
    pid: process.pid,
    instanceId: "old-inst",
    createdAtMs: Date.now(),
    target: { chatId: 1, threadId: 2 },
    slot: "B",
    threadName: "Beryl",
  });
  const refresh = createTelegramBusFollowerSessionRefreshHook({
    registrationState,
    registrationRuntime: {
      async registerWithLeader(ctx, leader, options) {
        registrations.push({ ctx, leader, options });
        registrationState.setRegistered(
          true,
          options?.target,
          { slot: "B", threadName: "Beryl" },
        );
        return true;
      },
      setContext: () => undefined,
    },
    getLeaderState: () => ({
      kind: "active-elsewhere",
      lock: { pid: 20, busSocketPath: "/tmp/leader.sock" },
    }),
    updateStatus: () => undefined,
    recordRuntimeEvent(category, message, details) {
      events.push({ category, message, details });
    },
  });

  await refresh({}, { cwd: "/repo" });

  assert.deepEqual(registrations, [
    {
      ctx: { cwd: "/repo" },
      leader: { pid: 20, busSocketPath: "/tmp/leader.sock" },
      options: {
        target: { chatId: 1, threadId: 2 },
        previousInstanceId: "old-inst",
      },
    },
  ]);
  assert.equal(registrationState.isRegistered(), true);
  assert.deepEqual(registrationState.getTarget(), { chatId: 1, threadId: 2 });
  assert.equal(getTelegramFollowerSessionHandoff(), undefined);
  assert.deepEqual(events, [
    {
      category: "bus",
      message: "Telegram follower registration restored after session replacement",
      details: {
        phase: "follower-session-restore",
        previousInstanceId: "old-inst",
      },
    },
    {
      category: "bus",
      message: "Telegram follower session context refreshed",
      details: { phase: "follower-session-refresh" },
    },
  ]);
});

test("follower client runtime exposes authenticated queue handoff transport", async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-bus-follower-client-handoff-"));
  const socketPath = join(dir, "bus.sock");
  const received: unknown[] = [];
  const server = createTelegramBusLocalServer({
    socketPath,
    handleEnvelope: (envelope) => {
      received.push(envelope);
      return {
        kind: "bus.ack",
        requestId: envelope.requestId,
        ok: true,
        result: {
          status: "staged",
          receiptId: "receipt-1",
          sourceUpdateIds: [1],
          queueOwner: {
            instanceId: "recipient",
            processId: 202,
            processBirthId: "202:start:recipient",
            sessionGeneration: 2,
            acquisitionId: "recipient-acquisition",
            acquiredAtMs: 2_000,
          },
        },
      };
    },
  });
  const client = createTelegramBusFollowerClientRuntime<
    { cwd: string },
    unknown,
    unknown,
    unknown
  >({
    socketPath,
    instanceId: "donor",
    getApiAuthSecret: () => "secret",
    getRegistrationGeneration: () => "donor-generation",
  });
  try {
    await server.start();
    assert.deepEqual(
      await client.queueHandoff({
        recipientInstanceId: "recipient",
        recipientRegistrationGeneration: "recipient-generation",
        donorProcessId: 101,
        donorProcessBirthId: "101:start:donor",
        donorSessionGeneration: 1,
        donorAcquisitionId: "donor-acquisition",
        donorAcquiredAtMs: 1000,
        handoffToken: "x".repeat(32),
        payload: {
          kind: "prompt",
          chatId: 7,
          replyToMessageId: 10,
          queueOrder: 1,
          queueLane: "default",
          laneOrder: 1,
          statusSummary: "handoff",
          admissionReceipts: [
            { queueKind: "prompt", receiptId: "receipt-1", sourceUpdateIds: [1] },
          ],
          sourceMessageIds: [10],
          queuedAttachments: [],
          content: [{ type: "text", text: "handoff prompt" }],
          historyText: "handoff",
        },
      }),
      {
        status: "staged",
        receiptId: "receipt-1",
        sourceUpdateIds: [1],
        queueOwner: {
          instanceId: "recipient",
          processId: 202,
          processBirthId: "202:start:recipient",
          sessionGeneration: 2,
          acquisitionId: "recipient-acquisition",
          acquiredAtMs: 2_000,
        },
      },
    );
    assert.equal((received[0] as { auth?: string }).auth, "secret");
  } finally {
    await server.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("follower client defaults the forwarding timeout to the 30s bus window", async () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-bus-follower-timeout-"));
  const socketPath = join(dir, "bus.sock");
  const server = createTelegramBusLocalServer({
    socketPath,
    handleEnvelope: async (envelope) => {
      await new Promise((resolve) => setTimeout(resolve, 1_500));
      return {
        kind: "bus.ack",
        requestId: envelope.requestId,
        ok: true,
        result:
          "delivery" in envelope && envelope.delivery
            ? {
                deliveryId: envelope.delivery.deliveryId,
                sourceUpdateId: envelope.delivery.sourceUpdateId,
              }
            : undefined,
      };
    },
  });
  const client = createTelegramBusFollowerClientRuntime<
    { cwd: string },
    unknown,
    unknown,
    unknown
  >({
    socketPath,
    instanceId: "inst-a",
    getRegistrationGeneration: () => "generation-a",
  });
  try {
    await server.start();
    const settlement = await client.foreignOwnedUpdateForwarder.forwardMessage({
      message: {
        message_id: 1,
        chat: { id: 7, type: "supergroup" },
        pi_telegram_source_update_id: 44,
      },
      ownership: {
        instanceId: "inst-a",
        ownerGeneration: "generation-a",
        recipientBindingKey: "manual:owner-a",
      },
      ctx: { cwd: "/repo" },
    });
    assert.deepEqual(settlement, {
      status: "accepted",
      delivery: createTelegramBusFollowerDeliveryIdentity({
        kind: "leader.forwardMessage",
        recipientBindingKey: "manual:owner-a",
        sourceUpdateId: 44,
      }),
    });
  } finally {
    await server.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});


test("Production Restore context uses actual Pi lifetime and authenticated owner observations", () => {
  const ctx = { cwd: "/repo" };
  let active = true;
  let session = "session";
  let generation = 1;
  let secret: string | undefined = "fixture-secret";
  let profile: string | undefined = "profile:fixture";
  let kind: "active-elsewhere" | "active-here" | "stale" = "active-elsewhere";
  const lock = { pid: 123, instanceId: "leader", leaderEpoch: "epoch", busSecret: "fixture-secret" };
  const protocol = createTelegramBusProtocolIdentity({ runtimeBuild: "fixture", capabilities: [TELEGRAM_BUS_CAPABILITY_WORKSPACE_RESTORE] });
  const get = createTelegramBusFollowerRestoreContextGetter({ isContextCurrent: value => active && value === ctx,
    getSessionId: () => session, getCwd: value => (value as typeof ctx).cwd, getGeneration: () => generation,
    getProfileBindingKey: () => profile, getOperatorUserId: () => 7,
    getLeaderState: () => ({ kind, lock }), getAuthenticatedSecret: () => secret, getLeaderProtocol: () => protocol });
  const observed = get(ctx)!;
  assert.deepEqual(observed, { executor: { instanceId: "leader", leaderEpoch: "epoch" },
    profileBindingKey: profile, operatorUserId: 7, sessionId: session, cwd: "/repo", generation: 1, leaderProtocol: protocol });
  assert.equal(get({ cwd: "/repo" }), undefined);
  for (const rejected of ["active-here", "stale"] as const) { kind = rejected; assert.equal(get(ctx), undefined); }
  kind = "active-elsewhere";
  secret = "old-secret";
  assert.equal(get(ctx), undefined);
  secret = undefined;
  assert.equal(get(ctx), undefined);
  secret = "fixture-secret";
  profile = undefined;
  assert.equal(get(ctx), undefined);
  profile = "profile:fixture";
  session = "successor-session";
  generation = 2;
  assert.equal(get(ctx)?.sessionId, session);
  assert.equal(get(ctx)?.generation, 2);
  assert.equal(observed.sessionId, "session", "earlier captured authority never changes with the live context");
  protocol.capabilities.length = 0;
  assert.equal(get(ctx), undefined);
  assert.equal(observed.leaderProtocol?.capabilities.includes(TELEGRAM_BUS_CAPABILITY_WORKSPACE_RESTORE), true);
  protocol.capabilities.push(TELEGRAM_BUS_CAPABILITY_WORKSPACE_RESTORE);
  active = false;
  assert.equal(get(ctx), undefined);
});

for (const mode of ["apply", "inspect"] as const) for (const fault of ["context", "target", "slot"] as const) {
  test(`Follower Restore acknowledgement recaptures authority after admission release (${mode}, ${fault})`, async () => {
    await fixture(async ({ store, threads, request, auth, path }) => {
      const relocated = (await store.commit(request, auth))!;
      const issued = store.issueRecipient(relocated, recipient("follower"), auth)!.intent;
      const registration = createTelegramBusFollowerRegistrationState();
      const initial = mode === "apply" ? request.binding.target : request.target;
      registration.setRegistered(true, initial, { generation: "registration", slot: "A" });
      const scope = { executor: auth.executor, profileBindingKey: "profile:restore", operatorUserId: 7,
        sessionId: "session", cwd: "/repo", generation: 1 };
      const ledger = createTelegramWorkspaceAdmissionLedger({ path: `${path}.release-admission`, profileKey: scope.profileBindingKey,
        owner: { processId: process.pid, processBirthId: `${process.pid}:released-recipient` }, getProcessLiveness: () => "alive" });
      let applications = 0;
      const handle = createTelegramBusFollowerWorkspaceRestoreHandler({ instanceId: "old", getContextAuthority: () => ({ ...scope }),
        readRestoreIntent: id => store.list().find(intent => intent.request.operationId === id), topicTargetStore: threads,
        getWorkspaceAdmission: () => ({ ...ledger, releaseAdmission(input) {
          const result = ledger.releaseAdmission(input);
          if (fault === "context") scope.generation++;
          if (fault === "target") registration.setRegistered(true, { chatId: 7, threadId: 99 }, { generation: "registration", slot: "A" });
          if (fault === "slot") registration.setRegistered(true, request.target, { generation: "registration", slot: "B" });
          return result;
        } }), registrationState: { ...registration, setRegistered(...args) { applications++; registration.setRegistered(...args); } } });
      const before = await readFile(path, "utf8");
      await assert.rejects(handle({ operationId: request.operationId, registrationGeneration: "registration", mode }, {}), /Stale Telegram follower Restore authority|changed after admission release/);
      assert.equal(applications, mode === "apply" ? 1 : 0, "a refused ACK does not roll back a separately applied local target");
      assert.equal(await readFile(path, "utf8"), before); assert.deepEqual(store.list(), [issued]);
      assert.deepEqual(ledger.read().leases, []);
    }, "follower");
  });
}

for (const mode of ["apply", "inspect-old", "inspect-new", "session", "cwd", "operator", "context", "generation", "executor", "recipient", "relocated", "ready-old", "slot", "cached-binding", "intent", "fence"] as const) {
  test(`Proof-aware follower Restore consumes canonical relocation without writing it (${mode})`, async () => {
    await fixture(async ({ store, threads, request, auth, path }) => {
      const relocated = await store.commit(request, auth);
      const issued = mode === "relocated" ? relocated! : store.issueRecipient(relocated!,
        { ...recipient("follower"), generation: mode === "recipient" ? "previous" : "registration" }, auth)!.intent;
      if (mode === "ready-old") store.confirmReady(issued, recipient("follower"), auth);
      const registration = createTelegramBusFollowerRegistrationState();
      registration.setRegistered(true, mode === "inspect-new" ? request.target : request.binding.target,
        { generation: mode === "generation" ? "other" : "registration", slot: mode === "slot" ? "B" : "A" });
      const scope = { executor: mode === "executor" ? { instanceId: "other", leaderEpoch: "other" } : auth.executor,
        profileBindingKey: "profile:restore", operatorUserId: mode === "operator" ? 8 : 7, cwd: mode === "cwd" ? "/other" : "/repo",
        sessionId: mode === "session" ? "other" : "session", generation: 1 };
      const ledger = createTelegramWorkspaceAdmissionLedger({ path: join(dirname(path), "admission.json"),
        profileKey: scope.profileBindingKey, owner: { processId: process.pid, processBirthId: `${process.pid}:restore-test` },
        getProcessLiveness: () => "alive" });
      if (mode === "fence") assert.equal(ledger.acquireRetirementFence({ operationId: "fence",
        retirementIntentId: "retirement", bindingKey: request.binding.bindingKey, slot: "A",
        target: request.binding.target, leaderEpoch: 1, retirementRequestedAtMs: 1 }).kind, "acquired");
      let applications = 0;
      let reads = 0;
      const handle = createTelegramBusFollowerWorkspaceRestoreHandler({ instanceId: "old",
        getContextAuthority: () => ({ ...scope }), getWorkspaceAdmission: () => ledger,
        readRestoreIntent(id, profile) {
          reads += 1;
          assert.equal(profile, scope.profileBindingKey);
          return store.list().find(value => value.request.operationId === id);
        },
        topicTargetStore: { ...threads, async load() {
          await threads.load();
          if (mode === "context") scope.generation += 1;
          if (mode === "cached-binding") threads.upsertWorkspaceBinding({ ...threads.listWorkspaceBindings()[0]!, target: { chatId: 7, threadId: 99 } });
          if (mode === "intent") {
            store.adopt(issued, { ...auth, executor: { instanceId: "next", leaderEpoch: "next" } });
            before = await readFile(path, "utf8");
          }
        } },
        registrationState: { ...registration, setRegistered(...args) { applications += 1; registration.setRegistered(...args); } },
      });
      const input = { operationId: request.operationId, registrationGeneration: "registration",
        mode: mode.startsWith("inspect") ? "inspect" as const : "apply" as const };
      let before = await readFile(path, "utf8");
      if (["apply", "inspect-old", "inspect-new", "cached-binding"].includes(mode)) {
        const result = await handle(input, {});
        assert.equal(await readFile(path, "utf8"), before, "the follower handler publishes nothing");
        assert.equal(result.ready, mode !== "inspect-old");
        assert.equal(result.target.threadId, mode === "inspect-old" ? 10 : 42);
        assert.equal(result.slot, "A");
        assert.equal(applications, mode === "apply" || mode === "cached-binding" ? 1 : 0);
        if (mode === "apply") {
          assert.equal((await handle({ ...input, mode: "inspect" }, {})).ready, true, "lost ACK is resolved without another switch");
          assert.equal((await handle(input, {})).ready, true);
          assert.equal(applications, 1);
          assert.equal(store.confirmReady(issued, result.recipient, auth)?.phase, "ready");
          before = await readFile(path, "utf8");
          assert.equal((await handle(input, {})).ready, true);
          assert.equal(applications, 1);
        }
      } else {
        await assert.rejects(handle(input, {}));
        assert.equal(applications, 0);
      }
      if (mode === "fence") assert.equal(reads, 0);
      assert.equal(await readFile(join(dirname(path), "state.json"), "utf8"), before);
    }, "follower");
  });
}

for (const fault of ["normal", "late-recovery", "disk-binding", "owner-detached", "intent-changed", "context-changed"] as const) {
  for (const mode of ["apply", "inspect"] as const) test(`Read-only recipient observation fences local effects (${fault}, ${mode})`, async () => {
    await fixture(async ({ store, threads, request, auth, path }) => {
      const relocated = (await store.commit(request, auth))!;
      const issued = store.issueRecipient(relocated, recipient("follower"), auth)!.intent;
      const pending = { id: "late", owner: "manual-follower" as const, instanceId: "creator", profileKey: "manual:creator",
        workspaceBindingKey: "other-binding", slot: "B", startedAtMs: 1000 };
      threads.upsertPendingProvision(pending); await threads.persist();
      const reader = createTelegramTopicTargetStore({ path, getNowMs: () => 1000, canPersist: () => false });
      await reader.load();
      assert.throws(() => reader.commitWorkspaceRestoreRegistration({ target: request.target,
        bindingKey: request.binding.bindingKey, slot: "A" }, () => assert.fail("follower cannot publish canonical authority")), /registration authority changed/);
      const registration = createTelegramBusFollowerRegistrationState();
      const originalTarget = mode === "apply" ? request.binding.target : request.target;
      registration.setRegistered(true, originalTarget, { generation: "registration", slot: "A" });
      const ledger = createTelegramWorkspaceAdmissionLedger({ path: `${path}.admission`, profileKey: "profile:restore",
        owner: { processId: process.pid, processBirthId: `${process.pid}:recipient-observation` }, getProcessLiveness: () => "alive" });
      const scope = { executor: auth.executor, profileBindingKey: "profile:restore", operatorUserId: 7,
        cwd: "/repo", sessionId: "session", generation: 1 };
      let applications = 0;
      let canonical = "";
      let recovery: string | undefined;
      const handle = createTelegramBusFollowerWorkspaceRestoreHandler({ instanceId: "old",
        getContextAuthority: () => ({ ...scope }), getWorkspaceAdmission: () => ledger,
        readRestoreIntent: id => store.list().find(value => value.request.operationId === id),
        topicTargetStore: { ...reader, async load() {
          await reader.load();
          if (fault === "late-recovery") {
            await threads.recordPendingProvisionTargetRecovery(pending, request.target);
            recovery = await readFile(`${path}.provision-recovery.json`, "utf8");
          }
          if (fault === "disk-binding") {
            const snapshot = JSON.parse(await readFile(path, "utf8"));
            snapshot.workspaceBindings[0].target = { chatId: 7, threadId: 99 };
            writeFileSync(path, JSON.stringify(snapshot));
          }
          if (fault === "owner-detached") assert.equal(await threads.detachTargetOwner(threads.list()[0]!, () => true), true);
          if (fault === "intent-changed") assert.ok(store.adopt(issued, { ...auth, executor: { instanceId: "next", leaderEpoch: "next" } }));
          canonical = await readFile(path, "utf8");
        }, withWorkspaceRestoreSnapshot(expected, observe) {
          reader.withWorkspaceRestoreSnapshot(expected, snapshot => {
            assert.ok(ledger.read().leases.length > 0);
            assert.throws(() => withTelegramFileTransaction(`${path}.transaction`, () => assert.fail("unlocked observation"),
              { attempts: 1, retryDelayMs: 0 }), /Timed out acquiring Telegram lock transaction/);
            if (fault === "context-changed") scope.generation += 1;
            return observe(snapshot);
          });
        } },
        registrationState: { ...registration, setRegistered(...args) {
          assert.throws(() => withTelegramFileTransaction(`${path}.transaction`, () => assert.fail("unlocked effect"),
            { attempts: 1, retryDelayMs: 0 }), /Timed out acquiring Telegram lock transaction/);
          applications += 1; registration.setRegistered(...args);
        } },
      });
      const input = { operationId: request.operationId, registrationGeneration: "registration", mode };
      if (fault === "normal") {
        assert.equal((await handle(input, {})).ready, true);
        assert.equal((await handle(input, {})).ready, true);
        assert.equal(applications, mode === "apply" ? 1 : 0, "reobservation grants no repeated apply");
      } else {
        await assert.rejects(handle(input, {}), /Protected Workspace Restore|observation changed|canonical binding is not committed|Stale Telegram follower Restore authority/);
        assert.equal(applications, 0);
        assert.deepEqual(registration.getTarget(), originalTarget);
      }
      assert.equal(await readFile(path, "utf8"), canonical);
      if (recovery) assert.equal(await readFile(`${path}.provision-recovery.json`, "utf8"), recovery);
      assert.equal(store.list()[0]?.phase, "recipient-issued");
      assert.equal(store.list()[0]?.routing, undefined);
      assert.deepEqual(ledger.read().leases, []);
      assert.equal(withTelegramFileTransaction(`${path}.transaction`, () => true, { attempts: 1, retryDelayMs: 0 }), true);
    }, "follower");
  });
}

for (const mode of ["ready", "issued-recipient", "issued-old-target", "issued-cleanup", "unadopted", "foreign-session", "old-target", "ended-epoch"] as const) {
  test(`Cold Restore successor inspection preserves original grants (${mode})`, async () => {
    await fixture(async ({ store, threads, request, auth, path, open }) => {
      let retained = store.issueRecipient((await store.commit(request, auth))!, recipient("follower"), auth)!.intent;
      if (mode !== "issued-recipient" && mode !== "issued-old-target") {
        retained = store.confirmReady(retained, recipient("follower"), auth)!;
        retained = store.issueRouting(retained, auth)!.intent;
        retained = store.recordSourceSettlement(retained, { ...request.source, kind: "completed" }, auth)!;
        if (mode === "issued-cleanup") retained = store.issueCleanup(retained, auth)!.intent;
      }
      // Registration publication is an explicit precondition, not startup proof supplied by this fixture.
      threads.upsert({ ...threads.list()[0]!, instanceId: "successor" });
      await threads.persist();
      const leader = createTelegramTopicTargetStore({ path }); await leader.load();
      const recovered = open({ threadStore: leader });
      const successorAuthority = { ...auth, executor: { instanceId: "next-leader", leaderEpoch: "next-epoch" } };
      if (mode !== "unadopted") retained = recovered.adopt(recovered.list()[0]!, successorAuthority)!;
      const before = await readFile(path, "utf8");
      const reader = createTelegramTopicTargetStore({ path, canPersist: () => false });
      const readonlyRestore = open({ threadStore: reader });
      const protocol = createTelegramBusProtocolIdentity({ runtimeBuild: "fixture", capabilities: [TELEGRAM_BUS_CAPABILITY_WORKSPACE_RESTORE] });
      const state = createTelegramBusFollowerRegistrationState();
      state.setRegistered(true, mode === "old-target" || mode === "issued-old-target" ? request.binding.target : request.target,
        { generation: "next-registration", slot: "A", leaderProtocol: protocol });
      const scope = { executor: successorAuthority.executor, profileBindingKey: "profile:restore", operatorUserId: 7,
        sessionId: mode === "foreign-session" ? "foreign" : "session", cwd: "/repo", generation: 2 };
      const ledger = createTelegramWorkspaceAdmissionLedger({ path: join(dirname(path), "successor-admission.json"),
        profileKey: scope.profileBindingKey, owner: { processId: process.pid, processBirthId: `${process.pid}:successor` },
        getProcessLiveness: () => "alive" });
      const ctx = {};
      let applications = 0, observations = 0;
      const handle = createTelegramBusFollowerWorkspaceRestoreHandler({ instanceId: "successor",
        getContextAuthority: createTelegramBusFollowerRestoreContextGetter({
          isContextCurrent: value => value === ctx, getSessionId: () => scope.sessionId, getCwd: () => scope.cwd,
          getGeneration: () => scope.generation, getProfileBindingKey: () => scope.profileBindingKey,
          getOperatorUserId: () => scope.operatorUserId, getAuthenticatedSecret: () => "secret",
          getLeaderProtocol: state.getLeaderProtocol,
          getLeaderState: () => ({ kind: "active-elsewhere", lock: { pid: 123, instanceId: scope.executor.instanceId,
            leaderEpoch: scope.executor.leaderEpoch, busSecret: "secret" } }),
        }),
        getWorkspaceAdmission: () => ledger, readRestoreIntent: id => readonlyRestore.list().find(value => value.request.operationId === id),
        topicTargetStore: { async load() {
          await reader.load();
          if (mode === "ended-epoch") scope.executor = { ...scope.executor, leaderEpoch: "ended" };
        }, withWorkspaceRestoreSnapshot(expected, observe) {
          reader.withWorkspaceRestoreSnapshot(expected, snapshot => {
            observations += 1;
            assert.equal(ledger.read().leases.length, 1);
            assert.equal(existsSync(`${path}.transaction`), true);
            return observe(snapshot);
          });
        } },
        registrationState: { ...state, setRegistered(...args) { applications += 1; state.setRegistered(...args); } } });
      const socketPath = getTelegramBusFollowerSocketPath("successor", dirname(path));
      const follower: TelegramBusFollowerView = { instanceId: "successor", registrationGeneration: "next-registration",
        cwd: scope.cwd, sessionId: scope.sessionId, target: state.getTarget(), slot: "A", protocol,
        busSocketPath: socketPath, connectedAtMs: 1, lastHeartbeatMs: 1 };
      const receiver = createTelegramBusForwardedUpdateReceiverRuntime({ socketPath, instanceId: "successor",
        getAuthSecret: () => "secret", getRegistrationGeneration: state.getGeneration, getRecipientBindingKey: () => "unused",
        getContext: () => ctx, isWorkspaceRestoreEnabled: () => true,
        durableAdmission: { async admit() { assert.fail("Inspection cannot dispatch accepted input"); } }, handleWorkspaceRestore: handle });
      const control = createTelegramBusWorkspaceRestoreController({ getFollower: () => follower, localProtocolIdentity: protocol,
        createRequestId: () => "cold-inspection", getAuthSecret: () => "secret", timeoutMs: 1000 });
      await receiver.start();
      try {
        const observed = await control({ operationId: request.operationId, instanceId: "successor", sessionId: scope.sessionId,
          target: request.target, oldTarget: request.binding.target, slot: "A", mode: "inspect", isCurrent: () => true });
        const ready = mode === "ready" || mode === "issued-recipient" || mode === "issued-cleanup";
        assert.equal(observed?.ready === true, ready);
        assert.equal(await readFile(path, "utf8"), before, "follower inspection cannot rewrite canonical evidence");
        assert.equal(applications, 0, "a successor never consumes the original apply grant");
        assert.equal(observations, ready || mode === "old-target" || mode === "issued-old-target" ? 1 : 0);
        assert.deepEqual(state.getTarget(), mode === "old-target" || mode === "issued-old-target" ? request.binding.target : request.target);
        assert.deepEqual(ledger.read().leases, []);
        assert.equal(existsSync(`${path}.transaction`), false);
        if (ready) {
          const confirmed = recovered.confirmInspectedReady(retained, observed!.recipient, successorAuthority)!;
          assert.deepEqual(confirmed.recipient, recipient("follower"));
          assert.deepEqual(confirmed.readyRecipient, { kind: "follower", instanceId: "successor", sessionId: "session", generation: "next-registration" });
          assert.deepEqual(confirmed.routing, retained.routing, "inspection cannot settle sources or reset issued grants");
          assert.deepEqual(confirmed.request, request);
          assert.equal(recovered.issueRecipient(confirmed, observed!.recipient, successorAuthority), undefined);
          if (mode === "issued-cleanup") {
            assert.equal(recovered.issueCleanup(confirmed, successorAuthority), undefined);
            assert.equal(recovered.retire(confirmed, successorAuthority), undefined);
          }
        } else assert.deepEqual(recovered.list(), [retained]);
      } finally { await receiver.stop(); }
    }, "follower");
  });
}

for (const mode of ["normal", "capability", "version", "disabled", "auth", "generation", "reply-mismatch", "lost-ack", "transport-loss", "registry-change", "owner-replaced", "pi-replaced", "protocol-downgrade", "protocol-replacement"] as const) {
  test(`Workspace Restore crosses authenticated native IPC without replay (${mode})`, async () => {
    await fixture(async ({ store, threads, request, auth, path }) => {
      const relocated = await store.commit(request, auth);
      const issued = mode === "normal" ? undefined : store.issueRecipient(relocated!, recipient("follower"), auth)!.intent;
      const protocol = createTelegramBusProtocolIdentity({ runtimeBuild: "fixture", capabilities: [TELEGRAM_BUS_CAPABILITY_WORKSPACE_RESTORE] });
      const state = createTelegramBusFollowerRegistrationState();
      state.setRegistered(true, request.binding.target, { generation: "registration", slot: "A", leaderProtocol: protocol });
      const scope = { executor: auth.executor, profileBindingKey: "profile:restore", operatorUserId: 7,
        cwd: "/repo", sessionId: "session", generation: 1 };
      const ledger = createTelegramWorkspaceAdmissionLedger({ path: join(dirname(path), "admission.json"),
        profileKey: scope.profileBindingKey, owner: { processId: process.pid, processBirthId: `${process.pid}:restore-ipc` },
        getProcessLiveness: () => "alive" });
      const socketPath = getTelegramBusFollowerSocketPath("old", dirname(path));
      let follower: TelegramBusFollowerView = { instanceId: "old", registrationGeneration: mode === "generation" ? "old" : "registration",
        cwd: "/repo", sessionId: "session", slot: "A", target: request.binding.target, busSocketPath: socketPath,
        protocol: mode === "capability" ? { ...protocol, capabilities: [] } : mode === "version" ? { ...protocol, protocolVersion: 999 } : protocol,
        connectedAtMs: 1, lastHeartbeatMs: 1 };
      let applications = 0;
      let requests = 0;
      const ctx = {};
      const handle = createTelegramBusFollowerWorkspaceRestoreHandler({ instanceId: "old",
        getContextAuthority: createTelegramBusFollowerRestoreContextGetter({
          isContextCurrent: value => value === ctx, getSessionId: () => scope.sessionId, getCwd: () => scope.cwd,
          getGeneration: () => scope.generation, getProfileBindingKey: () => scope.profileBindingKey,
          getOperatorUserId: () => scope.operatorUserId, getAuthenticatedSecret: () => "secret",
          getLeaderProtocol: state.getLeaderProtocol,
          getLeaderState: () => ({ kind: "active-elsewhere", lock: { pid: 123, instanceId: scope.executor.instanceId,
            leaderEpoch: scope.executor.leaderEpoch, busSecret: "secret" } }),
        }),
        getWorkspaceAdmission: () => ledger, readRestoreIntent: id => store.list().find(value => value.request.operationId === id),
        topicTargetStore: { ...threads, async load() {
          await threads.load();
          if (mode === "owner-replaced") scope.executor = { ...scope.executor, leaderEpoch: "replacement" };
          if (mode === "pi-replaced") scope.generation += 1;
          if (mode === "protocol-downgrade" || mode === "protocol-replacement") state.setRegistered(true, state.getTarget(), {
            generation: state.getGeneration(), slot: state.getSlot(), leaderProtocol: mode === "protocol-downgrade"
              ? { ...protocol, capabilities: [] } : { ...protocol, runtimeBuild: "replacement" } });
        } }, registrationState: { ...state, setRegistered(...args) { applications += 1; state.setRegistered(...args); } } });
      const handleWorkspaceRestore = async (input: { operationId: string; registrationGeneration: string; mode: "apply" | "inspect" }, context: object) => {
        requests += 1;
        const result = await handle(input, context);
        if (mode === "lost-ack" && input.mode === "apply") throw new Error("Response lost after local switch");
        if (mode === "registry-change") follower = { ...follower, registrationGeneration: "replacement" };
        return mode === "reply-mismatch" ? { ...result, slot: "B" } : result;
      };
      let droppedReplies = 0;
      const receiver = mode === "transport-loss" ? createRawTelegramBusLocalServer({ socketPath,
        async handleEnvelope(envelope) {
          assert.equal(envelope.auth, "secret");
          assert.equal(envelope.kind, "leader.workspaceRestore");
          if (envelope.kind !== "leader.workspaceRestore") assert.fail("Unexpected envelope");
          const result = await handleWorkspaceRestore({ operationId: envelope.operationId, mode: envelope.mode,
            registrationGeneration: envelope.recipientRegistrationGeneration }, ctx);
          return { kind: "bus.ack", requestId: envelope.requestId, ok: true, result };
        }, shouldDropResponse(envelope) {
          if (envelope.kind !== "leader.workspaceRestore" || envelope.mode !== "apply") return false;
          droppedReplies += 1;
          return true;
        } }) : createTelegramBusForwardedUpdateReceiverRuntime({ socketPath, instanceId: "old",
        getAuthSecret: () => "secret", getRegistrationGeneration: state.getGeneration, getRecipientBindingKey: () => "unused",
        getContext: () => ctx, isWorkspaceRestoreEnabled: () => mode !== "disabled" &&
          state.getLeaderProtocol()?.capabilities.includes(TELEGRAM_BUS_CAPABILITY_WORKSPACE_RESTORE) === true,
        durableAdmission: { async admit() { assert.fail("Restore must never enter input admission"); } },
        handleWorkspaceRestore });
      let requestId = 0;
      const control = createTelegramBusWorkspaceRestoreController({ getFollower: () => follower, localProtocolIdentity: protocol,
        createRequestId: () => `restore-${++requestId}`, getAuthSecret: () => mode === "auth" ? "wrong" : "secret", timeoutMs: 1000 });
      const input = { operationId: request.operationId, instanceId: "old", sessionId: "session", slot: "A",
        target: request.target, oldTarget: request.binding.target, mode: "apply" as const, isCurrent: () => true };
      await receiver.start();
      try {
        if (mode === "normal") {
          const attempt = () => advanceTelegramWorkspaceRestore({ request, authority: auth, restoreStore: store,
            getRecipient: () => recipient("follower"), runRecipient: action => control({ ...input, mode: action.mode, isCurrent: action.isCurrent }) });
          assert.equal((await attempt())?.phase, "ready");
          assert.equal((await attempt())?.phase, "ready", "ready replay inspects without another apply");
        } else {
          const result = mode === "transport-loss"
            ? (await assert.rejects(() => control(input), /Timed out waiting for Telegram bus response/), undefined)
            : await control(input);
          assert.equal(result, undefined);
          assert.equal(store.list()[0]?.phase, "recipient-issued");
          assert.equal(store.issueRecipient(store.list()[0]!, recipient("follower"), auth), undefined);
          if (mode === "lost-ack" || mode === "transport-loss") {
            const observed = await control({ ...input, mode: "inspect" });
            assert.equal(observed?.ready, true);
            assert.equal(store.confirmReady(issued!, observed!.recipient, auth)?.phase, "ready");
          }
        }
        const admitted = ["normal", "lost-ack", "transport-loss", "reply-mismatch", "registry-change"].includes(mode);
        assert.equal(applications, admitted ? 1 : 0);
        if (admitted) assert.deepEqual(state.getLeaderProtocol(), protocol, "Restore retains negotiated transport capability");
        else assert.deepEqual(state.getTarget(), request.binding.target, "rejected authority cannot change local target");
        assert.equal(requests, mode === "normal" || mode === "lost-ack" || mode === "transport-loss" ? 2 :
          admitted || mode === "owner-replaced" || mode === "pi-replaced" || mode.startsWith("protocol-") ? 1 : 0);
        assert.equal(droppedReplies, mode === "transport-loss" ? 1 : 0);
      } finally { await receiver.stop(); }
    }, "follower");
  });
}


test("Forum followers under the same service manager keep distinct stable routing identities", () => {
 const cinema = createTelegramFollowerProfileKeyResolver({ getActiveProfileName: () => undefined, manualFollowerOwnerId: "819:start:systemd", forumTarget: { chatId: -100123, threadId: 3 } });
 const health = createTelegramFollowerProfileKeyResolver({ getActiveProfileName: () => undefined, manualFollowerOwnerId: "819:start:systemd", forumTarget: { chatId: -100123, threadId: 359 } });
 const registry = createTelegramBusFollowerRegistry();
 registry.register({ instanceId: "cinema", profileKey: cinema(), target: { chatId: -100123, threadId: 3 }, connectedAtMs: 1 });
 registry.register({ instanceId: "health", profileKey: health(), target: { chatId: -100123, threadId: 359 }, connectedAtMs: 2 });
 assert.notEqual(cinema(),health()); assert.equal(registry.list().length,2);
 assert.ok(registry.heartbeat("cinema",3)); assert.ok(registry.heartbeat("health",3));
 const restarted = createTelegramFollowerProfileKeyResolver({ getActiveProfileName: () => undefined, manualFollowerOwnerId: "999:start:new-manager", forumTarget: { chatId: -100123, threadId: 359 } });
 assert.equal(restarted(),health());
 registry.register({instanceId:"health-new",profileKey:restarted(),target:{chatId:-100123,threadId:359},connectedAtMs:4});
 assert.deepEqual(registry.list().map(row=>row.instanceId),["cinema","health-new"]);
});
