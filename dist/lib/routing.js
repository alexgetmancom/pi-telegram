/**
 * Telegram inbound routing composition
 * Zones: telegram inbound, orchestration, queue/menu/command composition
 * Wires authorized updates into menus, commands, media grouping, and prompt queueing, and owns exact assistant-output target/route authority capture
 */
import { randomBytes } from "node:crypto";
import { readFile } from "node:fs/promises";
import { basename, dirname } from "node:path";
import { isDeepStrictEqual } from "node:util";
import * as Bus from "./bus.js";
import * as Commands from "./commands.js";
import * as Media from "./media.js";
import * as Menu from "./menu.js";
import * as OutboundHandlers from "./outbound.js";
import * as PromptTemplates from "./prompt-templates.js";
import * as Queue from "./queue.js";
import * as Replies from "./replies.js";
import * as TextGroups from "./text-groups.js";
import * as ThreadNaming from "./thread-naming.js";
import * as ThreadReconciler from "./thread-reconciler.js";
import * as Turns from "./turns.js";
import * as WorkspaceIdentity from "./workspace-identity.js";
function formatTelegramPromptPeer(peer) {
    if (!peer)
        return undefined;
    if (typeof peer.username === "string" && peer.username.length > 0) {
        return peer.username;
    }
    const displayName = [peer.first_name, peer.last_name]
        .filter((part) => typeof part === "string" && part.length > 0)
        .join(" ");
    if (displayName)
        return displayName;
    if (typeof peer.title === "string" && peer.title.length > 0) {
        return peer.title;
    }
    return typeof peer.id === "number" ? String(peer.id) : undefined;
}
function isTelegramPromptOwnerPeer(peer, ownerUserId) {
    return ownerUserId !== undefined && peer?.id === ownerUserId;
}
function isTelegramPromptBotPeer(peer) {
    return peer?.is_bot === true;
}
export function resolveTelegramGuestPromptPeer(input) {
    if (input.chatType !== "private") {
        return formatTelegramPromptPeer(input.chat);
    }
    if (!isTelegramPromptOwnerPeer(input.from, input.ownerUserId) &&
        !isTelegramPromptBotPeer(input.from)) {
        return formatTelegramPromptPeer(input.from);
    }
    for (const candidate of [
        input.chat,
        input.guestBotCallerUser,
        input.guestBotCallerChat,
        input.replyFrom,
    ]) {
        if (isTelegramPromptOwnerPeer(candidate, input.ownerUserId) ||
            isTelegramPromptBotPeer(candidate)) {
            continue;
        }
        const peer = formatTelegramPromptPeer(candidate);
        if (peer)
            return peer;
    }
    return undefined;
}
/** Stable file scope of the remote Guest Mode peer: username, else numeric id; never the bot's own scope. */
export function resolveTelegramGuestFileScope(input) {
    const candidates = input.chatType !== "private" ? [input.chat]
        : [input.from, input.chat, input.guestBotCallerUser, input.guestBotCallerChat, input.replyFrom];
    for (const candidate of candidates) {
        if (!candidate || (input.chatType === "private" &&
            (isTelegramPromptOwnerPeer(candidate, input.ownerUserId) || isTelegramPromptBotPeer(candidate))))
            continue;
        if (typeof candidate.username === "string" && candidate.username.length > 0)
            return candidate.username;
        if (typeof candidate.id === "number" && Number.isSafeInteger(candidate.id))
            return String(Math.abs(candidate.id));
    }
    return "guest";
}
function appendTelegramSourceAttachmentSection(text, from, files, outputs = []) {
    if (files.length === 0 && outputs.length === 0)
        return text;
    const dirs = [...new Set(files.map((file) => dirname(file.path)))];
    const sameDir = dirs.length === 1;
    const source = from ? `|from:${from}` : "";
    const header = sameDir
        ? `[attachments${source}] ${dirs[0]}`
        : `[attachments${source}]`;
    const items = sameDir
        ? files.map((file) => `/${basename(file.path)}`)
        : files.map((file) => file.path);
    const sections = text ? [text] : [];
    if (items.length > 0) {
        sections.push(`${header}\n${items.map((item) => `- ${item}`).join("\n")}`);
    }
    if (outputs.length > 0) {
        const outputHeader = `[outputs${source}]`;
        sections.push(`${outputHeader}\n${outputs.map((output) => `- ${output}`).join("\n")}`);
    }
    return sections.join("\n\n");
}
function getContextCwd(ctx) {
    if (!ctx || typeof ctx !== "object")
        return undefined;
    const cwd = ctx.cwd;
    return typeof cwd === "string" && cwd.length > 0 ? cwd : undefined;
}
function getLeaderTopicProfileKey(ctx, instanceId) {
    const cwd = getContextCwd(ctx);
    if (cwd)
        return `cwd:${cwd}`;
    return instanceId ? `leader:${instanceId}` : undefined;
}
function isCurrentLeaderTopicRecord(record, profileKey, instanceId) {
    if (instanceId && record.instanceId === instanceId)
        return true;
    return !!profileKey && record.profileKey === profileKey;
}
function hasActiveLeaderTopic(records, profileKey, instanceId) {
    return records.some((record) => {
        if (record.status !== "active")
            return false;
        return isCurrentLeaderTopicRecord(record, profileKey, instanceId);
    });
}
function escapeHtml(text) {
    return text
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;");
}
const TELEGRAM_UNBOUND_REROUTE_CALLBACK_PREFIX = "reroute:";
const TELEGRAM_UNBOUND_REROUTE_RESTORE_MENU_CALLBACK_PREFIX = "rerouterestore:";
const TELEGRAM_UNBOUND_REROUTE_MENU_CALLBACK_PREFIX = "reroutemenu:";
const TELEGRAM_UNBOUND_REROUTE_ROOT_CALLBACK_PREFIX = "rerouteroot:";
const TELEGRAM_UNBOUND_REROUTE_NEW_SLOT_CALLBACK_PREFIX = "reroutenew:";
const TELEGRAM_UNBOUND_REROUTE_CANCEL_CALLBACK_PREFIX = "reroutecancel:";
const TELEGRAM_PENDING_CANCELLATION_REVIEW_PREFIX = "reroutecancel:review:";
const TELEGRAM_RETIRED_HISTORICAL_REVIEW_PREFIX = "reroutecancel:history:";
const TELEGRAM_SLOT_CAPACITY_MESSAGE = "No Telegram instance slot is available. Automatic reclamation is disabled for safety.";
function formatTelegramUnboundRerouteCallbackData(rerouteId, threadId) {
    return `${TELEGRAM_UNBOUND_REROUTE_CALLBACK_PREFIX}${rerouteId}:${threadId}`;
}
function formatTelegramUnboundRerouteRestoreMenuCallbackData(rerouteId) {
    return `${TELEGRAM_UNBOUND_REROUTE_RESTORE_MENU_CALLBACK_PREFIX}${rerouteId}`;
}
function formatTelegramUnboundRerouteNewSlotCallbackData(rerouteId, threadId) {
    return `${TELEGRAM_UNBOUND_REROUTE_NEW_SLOT_CALLBACK_PREFIX}${rerouteId}:${threadId}`;
}
function parseTelegramUnboundRerouteRestoreMenuCallbackData(data) {
    const match = data?.match(/^(rerouterestore|reroutemenu|rerouteroot):([a-z0-9]+)$/);
    const rerouteId = match?.[2];
    return rerouteId ? { rerouteId, restore: match?.[1] === "rerouterestore", root: match?.[1] === "rerouteroot" } : undefined;
}
function parseTelegramUnboundRerouteCallbackData(data) {
    const match = data?.match(/^(reroute|reroutenew):([a-z0-9]+):(\d+)$/);
    const prefix = match?.[1];
    const rerouteId = match?.[2];
    const threadId = Number(match?.[3]);
    if (!prefix || !rerouteId || !Number.isSafeInteger(threadId))
        return undefined;
    return { rerouteId, threadId, useNewSlot: prefix === "reroutenew" };
}
function getTelegramThreadRecordLabel(record, getDisplayTitle) {
    return getDisplayTitle?.(record.target) ?? getRestoredThreadName(record, record.slot ?? "");
}
function getRestoredThreadName(record, slot) {
    return record.threadName &&
        ThreadNaming.isTelegramTopicThreadNameValidForSlot(record.threadName, slot)
        ? record.threadName
        : (ThreadNaming.chooseTelegramThreadName({ slot }) ?? "Pi");
}
function isTelegramLiveThreadTarget(record, liveTargets) {
    if (!liveTargets)
        return record.status === "active";
    return liveTargets.some((target) => target.chatId === record.target.chatId &&
        target.threadId === record.target.threadId);
}
function getTelegramRoutableThreadRecords(records, liveTargets) {
    return records.filter((record) => record.status === "active" &&
        isTelegramLiveThreadTarget(record, liveTargets));
}
function formatTelegramTemporaryThreadChooserText(command) {
    return [
        "<b>🧵 Choose target thread:</b>",
        "",
        command ? `You used <code>/${escapeHtml(command)}</code> from the <b>All</b> tab.` : "Choose where to send your message.",
        "Unaccepted routing expires after 60 minutes; disposable tabs are removed when clear. Accepted work and restored Threads are kept.",
        "Select the Pi thread that should handle it, or restore a Pi into this tab:",
    ].join("\n");
}
function formatTelegramAllTabMenuChooserText(command) {
    return [
        "<b>🧵 Choose target thread:</b>",
        "",
        `You used <code>/${escapeHtml(command)}</code> from the <b>All</b> tab.`,
        "Select the Pi thread that should handle it:",
        "To restore into a new thread, send a plain message in that destination thread first.",
    ].join("\n");
}
function buildTelegramUnboundRerouteChooserMarkup(rerouteId, records, options) {
    const activeRecords = records.filter((record) => record.status === "active");
    const canRestoreAnyLiveThread = options.canRestore && activeRecords.length > 0;
    const rows = activeRecords.length
        ? [[{ text: "↪️ Reroute…", callback_data: `${TELEGRAM_UNBOUND_REROUTE_MENU_CALLBACK_PREFIX}${rerouteId}` }]] : [];
    if (canRestoreAnyLiveThread)
        rows.push([{
                text: "🔁 Restore…",
                callback_data: formatTelegramUnboundRerouteRestoreMenuCallbackData(rerouteId),
            }]);
    if (options.canCancel)
        rows.push([{
                text: "⛔️ Cancel routing",
                callback_data: `${TELEGRAM_UNBOUND_REROUTE_CANCEL_CALLBACK_PREFIX}${rerouteId}`,
            }]);
    return { inline_keyboard: rows };
}
function buildTelegramUnboundRerouteRestoreChooserMarkup(rerouteId, records, getDisplayTitle) {
    return {
        inline_keyboard: records
            .filter((record) => record.status === "active")
            .map((record) => [
            {
                text: `➡️ ${getTelegramThreadRecordLabel(record, getDisplayTitle)}`,
                callback_data: formatTelegramUnboundRerouteNewSlotCallbackData(rerouteId, record.target.threadId),
            },
        ]),
    };
}
function formatTelegramUnboundRerouteRestoreChooserText() {
    return [
        "<b>🧵 Replace/restore Telegram thread:</b>",
        "",
        "Choose the Pi instance to move to this new Telegram thread:",
    ].join("\n");
}
function formatTelegramUnboundTopicGuidance() {
    return [
        "<b>⚠️ New thread is not a Pi instance.</b>",
        "",
        "To create a bound Telegram tab:",
        "<code>1.</code> Start another Pi instance in your terminal.",
        "<code>2.</code> Run <code>/telegram-connect</code> in that instance.",
        "<code>3.</code> The bridge will create and bind a fresh Telegram tab for it.",
    ].join("\n");
}
function formatTelegramTargetKey(target) {
    return `${target.chatId}:${target.threadId ?? "all"}`;
}
import * as Threads from "./threads.js";
import * as Updates from "./updates.js";
import { getTelegramVoiceReplyMode } from "./voice.js";
/** One admitted Restore attempt through recipient readiness, never source dispatch or cleanup. */
export async function advanceTelegramWorkspaceRestore(input) {
    const isCurrent = input.authority.isCurrent.bind(input.authority);
    if (!isCurrent())
        return undefined;
    const request = structuredClone(input.request);
    if (!Threads.isTelegramWorkspaceRestoreRequest(request))
        return undefined;
    const getRecipient = input.getRecipient;
    const runRecipient = input.runRecipient;
    const observedRecipient = getRecipient();
    if (!Threads.isTelegramWorkspaceRestoreRecipient(observedRecipient) || observedRecipient.sessionId !== request.binding.sessionId)
        return undefined;
    const recipient = structuredClone(observedRecipient);
    const executor = structuredClone(input.authority.executor);
    const operatorUserId = input.authority.operatorUserId;
    const current = () => isCurrent() && input.authority.operatorUserId === operatorUserId &&
        isDeepStrictEqual(input.authority.executor, executor) && isDeepStrictEqual(getRecipient(), recipient);
    const authority = { executor, operatorUserId, isCurrent: current };
    const { restoreStore } = input;
    const retained = () => {
        if (!current())
            return undefined;
        const found = restoreStore.list().find(value => value.request.operationId === request.operationId);
        return current() && found && isDeepStrictEqual(found.request, request) && found.operatorUserId === operatorUserId &&
            isDeepStrictEqual(found.executor, executor) ? found : undefined;
    };
    let intent = retained();
    const predecessor = intent ? undefined : restoreStore.list().find(value => value.request.operationId === request.operationId);
    if (predecessor && current() && isDeepStrictEqual(predecessor.request, request) &&
        predecessor.operatorUserId === operatorUserId && !isDeepStrictEqual(predecessor.executor, executor)) {
        // Adoption transfers executor authority only. Issued grants stay issued, so a successor can merely inspect them.
        try {
            intent = restoreStore.adopt(predecessor, authority);
        }
        catch (error) {
            intent = retained();
            if (!intent)
                throw error;
        }
        if (!current() || !intent)
            return undefined;
        intent = retained();
        if (!intent)
            return undefined;
    }
    if (input.inspectOnly && !intent)
        return undefined;
    if (!intent || intent.phase === "relocated") {
        if ((!input.inspectOnly && recipient.instanceId !== request.owner.instanceId) ||
            recipient.kind !== (request.owner.owner?.kind === "leader" ? "leader" : "follower"))
            return undefined;
        intent = await restoreStore.commit(request, authority);
        if (!current() || !intent)
            return undefined;
    }
    let mode = "inspect";
    if (intent.phase === "relocated") {
        try {
            const issuance = restoreStore.issueRecipient(intent, recipient, authority);
            if (issuance) {
                intent = issuance.intent;
                mode = input.inspectOnly ? "inspect" : "apply";
            }
            else
                intent = retained();
        }
        catch (error) {
            intent = retained();
            if (intent?.phase !== "recipient-issued")
                throw error;
            // A retained issued phase is not a fresh grant, even when publication's reply was lost.
        }
    }
    if (!current() || !intent || !["recipient-issued", "ready"].includes(intent.phase) ||
        (mode === "apply" && !isDeepStrictEqual(intent.recipient, recipient)))
        return undefined;
    const expected = structuredClone(intent);
    if (!isDeepStrictEqual(retained(), expected))
        return undefined;
    const observation = await runRecipient({ intent: structuredClone(expected), mode, isCurrent: current });
    if (!current() || !isDeepStrictEqual(retained(), expected) || !observation || observation.ready !== true ||
        observation.operationId !== request.operationId || !isDeepStrictEqual(observation.recipient, recipient) ||
        !isDeepStrictEqual(observation.target, request.target) || observation.slot !== request.binding.slot)
        return undefined;
    if (expected.phase === "ready" && isDeepStrictEqual(expected.readyRecipient ?? expected.recipient, recipient))
        return retained();
    try {
        intent = mode === "inspect" ? restoreStore.confirmInspectedReady(expected, recipient, authority) :
            restoreStore.confirmReady(expected, recipient, authority);
    }
    catch (error) {
        intent = retained();
        if (intent?.phase !== "ready" || !isDeepStrictEqual(intent.readyRecipient ?? intent.recipient, recipient))
            throw error;
    }
    const confirmed = retained();
    return current() && confirmed?.phase === "ready" &&
        isDeepStrictEqual(confirmed.readyRecipient ?? confirmed.recipient, recipient) ? confirmed : undefined;
}
async function deleteReservedTelegramTopicThroughReconciler(deps, target, messageId) {
    if (!deps.threadStore)
        return false;
    const nowMs = Date.now();
    const currentLeaderEpoch = deps.getCurrentLeaderEpoch?.();
    const plan = ThreadReconciler.planThreadReconciliation({
        nowMs,
        currentLeaderEpoch,
        previousState: deps.getThreadReconciliationMachineState?.(),
        records: deps.threadStore.list(),
        reservations: deps.threadStore.listReservations(),
        observations: deps.threadStore.listSyncObservations(),
        reservedMessages: [
            {
                target,
                observedAtMs: nowMs,
                messageId,
                ...(currentLeaderEpoch !== undefined
                    ? { leaderEpoch: currentLeaderEpoch }
                    : {}),
            },
        ],
    });
    deps.recordThreadReconciliationPlan?.(plan);
    await ThreadReconciler.applyThreadReconciliationPlan(plan, {
        isCleanupTargetProtected: Threads.createTelegramCleanupTargetProtection(deps.threadStore),
        callApi: deps.callApi,
        markStaleByTarget: (staleTarget, syncStatus, lastSyncError) => deps.threadStore?.markStaleByTarget(staleTarget, syncStatus, lastSyncError) ?? false,
        persist: () => deps.threadStore?.persist() ?? Promise.resolve(),
        getCurrentLeaderEpoch: deps.getCurrentLeaderEpoch,
        recordRuntimeEvent: deps.recordRuntimeEvent,
    });
    return plan.actions.some((action) => action.kind === "close-delete-reserved-topic");
}
export const TELEGRAM_ALL_TAB_COMMAND_MAX_AGE_MS = 60 * 60_000;
/** One user-facing answer for expired or previous-process routing controls. */
export const TELEGRAM_ROUTING_CHOICE_EXPIRED = "⌛ Routing choice expired.";
export function isTelegramAllTabCommandExpired(message, nowMs = Date.now()) {
    return message.message_thread_id === undefined &&
        typeof message.date === "number" && Number.isFinite(message.date) &&
        message.date > 0 && Number.isFinite(nowMs) &&
        nowMs - message.date * 1000 >= TELEGRAM_ALL_TAB_COMMAND_MAX_AGE_MS;
}
export function createTelegramInboundBusProjectionRuntime(deps) {
    return {
        getTargetOwnership(target) {
            return Bus.getTelegramFollowerTargetOwnership({
                target,
                followers: deps.listFollowers(),
                activeThreadRecords: deps.listThreadRecords(),
                currentInstanceId: deps.instanceId,
            });
        },
        getLiveThreadTargets() {
            return Bus.listTelegramBusLiveThreadTargets({
                leaderTarget: deps.getLeaderTarget(),
                followers: deps.listFollowers(),
            });
        },
        getLocalThreadLabelForTarget(target) {
            const followerTarget = deps.getFollowerTarget();
            const leaderTarget = deps.getLeaderTarget();
            const isLocalFollowerTarget = deps.isFollowerRegistered() &&
                followerTarget?.chatId === target.chatId &&
                followerTarget.threadId === target.threadId;
            const isLocalLeaderTarget = leaderTarget?.chatId === target.chatId &&
                leaderTarget.threadId === target.threadId;
            if (!isLocalFollowerTarget && !isLocalLeaderTarget)
                return undefined;
            return deps.getCurrentIdentity(target).threadName;
        },
    };
}
const TELEGRAM_OWNED_CALLBACK_PREFIXES = [
    "allmenu:",
    TELEGRAM_UNBOUND_REROUTE_CALLBACK_PREFIX,
    TELEGRAM_UNBOUND_REROUTE_CANCEL_CALLBACK_PREFIX,
    "compact:",
    "menu:",
    "model:",
    "new:",
    "queue:",
    "section:",
    "settings:",
    "status:",
    "tgbtn:",
    "thinking:",
];
function isTelegramOwnedCallbackData(data) {
    return TELEGRAM_OWNED_CALLBACK_PREFIXES.some((prefix) => data.startsWith(prefix));
}
export function createTelegramInboundRouteRuntime(deps) {
    const pendingUnboundReroutes = new Map();
    const implicitThreadCreations = new Map();
    const guidedUnboundTopicKeys = new Set();
    let nextUnboundRerouteId = 0;
    let cancellationReview;
    const requestDispatchNextQueuedTelegramTurn = (ctx) => {
        deps.dispatchNextQueuedTelegramTurn(ctx);
        if (deps.requestDeferredDispatchNextQueuedTelegramTurn &&
            deps.hasDeferredDispatchContext?.() !== false) {
            deps.requestDeferredDispatchNextQueuedTelegramTurn(deps.dispatchNextQueuedTelegramTurn);
        }
    };
    const resolveTelegramThreadLabel = (message) => {
        const chatId = message.chat.id;
        const threadId = message.message_thread_id;
        if (!threadId)
            return undefined;
        const localLabel = deps.getDisplayTitle?.({ chatId, threadId }) ??
            deps.getLocalThreadLabelForTarget?.({ chatId, threadId });
        if (localLabel)
            return localLabel;
        if (!deps.threadStore)
            return undefined;
        const records = deps.threadStore.list();
        const currentInstanceId = deps.getCurrentInstanceId?.();
        for (const record of records) {
            if (record.target.chatId !== chatId ||
                record.target.threadId !== threadId) {
                continue;
            }
            if (currentInstanceId &&
                record.instanceId &&
                record.instanceId !== currentInstanceId) {
                continue;
            }
            return record.threadName &&
                ThreadNaming.isTelegramTopicThreadNameValidForSlot(record.threadName, record.slot)
                ? record.threadName
                : getRestoredThreadName(record, record.slot ?? "");
        }
        return undefined;
    };
    const createAdmissionReceipts = (queueKind, sources) => {
        const sourceUpdateIds = Updates.collectTelegramAdmissionSourceUpdateIds(sources);
        if (sourceUpdateIds.length === 0)
            return [];
        const receipt = Queue.createTelegramQueueAdmissionReceipt({
            queueKind,
            scope: deps.getAdmissionScope?.() ?? "",
            sourceUpdateIds,
        });
        const journalBindingKey = deps.getAdmissionJournalBinding?.();
        return receipt
            ? [{
                    ...receipt,
                    ...(journalBindingKey ? { journalBindingKey } : {}),
                }]
            : [];
    };
    const reportQueueAdmission = (sources, receipts) => {
        Updates.reportTelegramQueueAdmission(sources, receipts);
    };
    const removePendingReroute = (id) => {
        pendingUnboundReroutes.get(id)?.stopExpiry?.();
        pendingUnboundReroutes.delete(id);
    };
    const expirePendingCommand = (id, pending) => {
        if (pending.destinationSelected || pending.expiresAtMs === undefined ||
            Date.now() < pending.expiresAtMs)
            return false;
        if (pendingUnboundReroutes.get(id) !== pending)
            return true;
        for (const message of pending.messages)
            Updates.reportTelegramUpdateCompleted(message);
        removePendingReroute(id);
        return true;
    };
    const armPendingCommandExpiry = (id, pending) => {
        if (pending.dispatchKind !== "command" || pending.sourceTarget.threadId !== undefined)
            return;
        pending.stopExpiry?.();
        const execution = Updates.getTelegramUpdateExecutionFence(pending.messages[0]);
        const onAbort = () => removePendingReroute(id);
        let timer;
        let stopped = false;
        pending.pauseExpiry = () => {
            if (timer !== undefined)
                clearTimeout(timer);
            timer = undefined;
        };
        pending.stopExpiry = () => {
            stopped = true;
            pending.pauseExpiry?.();
            execution?.signal.removeEventListener("abort", onAbort);
        };
        execution?.signal.addEventListener("abort", onAbort, { once: true });
        if (execution?.signal.aborted) {
            onAbort();
            return;
        }
        if (pending.expiresAtMs === undefined) {
            const date = pending.messages[0]?.date;
            if (typeof date !== "number" || !Number.isFinite(date) || date <= 0 || date * 1000 > Date.now())
                return;
            pending.expiresAtMs = date * 1000 + TELEGRAM_ALL_TAB_COMMAND_MAX_AGE_MS;
        }
        const schedule = () => {
            if (stopped || pending.destinationSelected || pendingUnboundReroutes.get(id) !== pending)
                return;
            if (expirePendingCommand(id, pending))
                return;
            const delay = Math.min(TELEGRAM_ALL_TAB_COMMAND_MAX_AGE_MS, pending.expiresAtMs - Date.now());
            timer = setTimeout(schedule, Math.max(1, delay));
            timer.unref?.();
        };
        schedule();
    };
    const prunePendingCommandReroutes = () => {
        // Deferred prompts and selected routes with cleanup work must retain their control.
        // Age is not settlement; the chooser capacity limit still bounds retained entries.
        for (const [id, entry] of pendingUnboundReroutes) {
            if (entry.dispatchKind === "command" && entry.sourceTarget.threadId === undefined) {
                expirePendingCommand(id, entry);
            }
        }
    };
    const storePendingUnboundReroute = (messages, dispatchKind = "prompt", presentationTarget) => {
        prunePendingCommandReroutes();
        if (pendingUnboundReroutes.size >= 100) {
            throw new Error("Telegram route chooser capacity reached; source remains retryable.");
        }
        nextUnboundRerouteId += 1;
        const id = nextUnboundRerouteId.toString(36);
        pendingUnboundReroutes.set(id, {
            sourceTarget: presentationTarget ? { ...presentationTarget } : {
                chatId: messages[0].chat.id,
                ...(typeof messages[0].message_thread_id === "number"
                    ? { threadId: messages[0].message_thread_id }
                    : {}),
            },
            messages,
            dispatchKind,
        });
        const pending = pendingUnboundReroutes.get(id);
        armPendingCommandExpiry(id, pending);
        return id;
    };
    const rememberRerouteChooser = (id, messageId) => {
        const pending = pendingUnboundReroutes.get(id);
        if (!pending)
            return;
        pending.chooserMessageId = messageId;
        if (Number.isSafeInteger(messageId) && messageId > 0) {
            const owner = deps.configStore.getAllowedUserId();
            if (owner !== undefined && pending.messages.every(source => source.from?.id === owner && !source.from.is_bot && source.chat.type === "private")) {
                try {
                    const lifetime = Updates.armTelegramRoutingInputs(pending.messages, owner);
                    if (lifetime)
                        pending.routingOperatorUserId = lifetime.operatorUserId;
                }
                catch (error) {
                    deps.recordRuntimeEvent?.("routing", error, { phase: "routing-input-clock" });
                }
            }
        }
        if (messageId === undefined || pending.dispatchKind !== "command" ||
            pending.sourceTarget.threadId !== undefined || pending.selectionAttempted)
            return;
        const source = pending.messages[0];
        const text = source?.text?.trim();
        const execution = Updates.getTelegramUpdateExecutionFence(source);
        const sourceIds = Updates.collectTelegramAdmissionSourceUpdateIds(pending.messages);
        if (!text || Commands.parseTelegramCommand(text)?.name !== "start" ||
            source?.from?.id === undefined || !execution?.isCurrent() || sourceIds.length !== 1 ||
            expirePendingCommand(id, pending))
            return;
        for (const [oldId, old] of pendingUnboundReroutes) {
            if (oldId === id || old.dispatchKind !== "command" || old.selectionAttempted || old.dispatching ||
                old.sourceTarget.threadId !== undefined || old.sourceTarget.chatId !== pending.sourceTarget.chatId)
                continue;
            const oldSource = old.messages[0];
            const oldIds = Updates.collectTelegramAdmissionSourceUpdateIds(old.messages);
            if (oldSource?.from?.id !== source.from.id || oldSource?.text?.trim() !== text ||
                oldIds.length !== 1 || oldIds[0] >= sourceIds[0] ||
                Updates.getTelegramUpdateExecutionFence(oldSource)?.signal !== execution.signal)
                continue;
            if (Updates.reportTelegramUpdateCompleted(oldSource))
                removePendingReroute(oldId);
        }
    };
    const matchesRerouteChooser = (pending, query) => {
        const message = query.message;
        return !!message && pending.chooserMessageId !== undefined &&
            message.message_id === pending.chooserMessageId &&
            message.chat.id === pending.sourceTarget.chatId &&
            (message.message_thread_id === undefined ||
                message.message_thread_id === pending.sourceTarget.threadId);
    };
    const pendingUnboundRerouteMediaGroups = new Map();
    const threadNameDialog = ThreadNaming.createTelegramThreadNameDialogRuntime();
    const getThreadNameDialogScope = () => deps.getCurrentInstanceId?.() ?? "local";
    const menuCallbackHandler = Menu.createTelegramMenuCallbackHandlerForContext({
        getStoredModelMenuState: deps.modelMenuRuntime.getState,
        getActiveModel: deps.currentModelRuntime.get,
        getThinkingLevel: deps.getThinkingLevel,
        setThinkingLevel: deps.setThinkingLevel,
        updateStatus: deps.updateStatus,
        updateModelMenuMessage: deps.menuActions.updateModelMenuMessage,
        updateThinkingMenuMessage: deps.menuActions.updateThinkingMenuMessage,
        updateStatusMessage: deps.menuActions.updateStatusMessage,
        updateSettingsMenuMessage: deps.updateSettingsMenuMessage,
        answerCallbackQuery: deps.answerCallbackQuery,
        isIdle: deps.isIdle,
        hasAbortHandler: deps.bridgeRuntime.abort.hasHandler,
        getActiveToolExecutions: deps.bridgeRuntime.lifecycle.getActiveToolExecutions,
        persistScopedModelPatterns: deps.persistScopedModelPatterns,
        setModel: deps.setModel,
        setCurrentModel: deps.currentModelRuntime.setCurrentModel,
        stagePendingModelSwitch: deps.modelSwitchController.stagePendingSwitch,
        restartInterruptedTelegramTurn: deps.modelSwitchController.restartInterruptedTurn,
        sectionRegistry: deps.sectionRegistry,
        editInteractiveMessage: deps.editInteractiveMessage,
        sendInteractiveMessage: deps.sendInteractiveMessage,
        sendSectionRichMessage: deps.sendSectionRichMessage,
        deleteMessage: deps.deleteMessage,
        enqueueSectionPrompt: async (prompt, ctx, target, source) => {
            const chatId = target?.chatId ?? deps.configStore.getAllowedUserId();
            if (typeof chatId !== "number")
                return;
            const order = deps.bridgeRuntime.queue.allocateItemOrder();
            const admissionReceipts = createAdmissionReceipts("prompt", source === undefined ? [] : [source]);
            const turn = {
                kind: "prompt",
                chatId,
                ...(target ? { target } : {}),
                replyToMessageId: 0,
                sourceMessageIds: [],
                queueOrder: order,
                queueLane: "default",
                laneOrder: order,
                queuedAttachments: [],
                content: [
                    {
                        type: "text",
                        text: `[telegram] ${prompt}`,
                    },
                ],
                historyText: Turns.truncateTelegramQueueSummary(prompt),
                statusSummary: Turns.truncateTelegramQueueSummary(prompt),
                ...(admissionReceipts.length > 0 ? { admissionReceipts } : {}),
            };
            deps.queueMutationRuntime.append(turn, ctx);
            reportQueueAdmission(source === undefined ? [] : [source], admissionReceipts);
            deps.updateStatus(ctx);
            requestDispatchNextQueuedTelegramTurn(ctx);
        },
    });
    const cloneTelegramMessagesForThread = (messages, threadId) => {
        return messages.map((message) => Updates.carryTelegramUpdateExecutionFence(message, {
            ...message,
            message_id: 0,
            message_thread_id: threadId,
            reply_to_message: undefined,
        }));
    };
    /** Whether a chooser is still pending in this Thread, ignoring the ones the caller itself owns. */
    const hasPendingRerouteForTarget = (target, own) => [...pendingUnboundReroutes.values()].some(pending => pending.sourceTarget.chatId === target.chatId &&
        pending.sourceTarget.threadId === target.threadId && !own?.(pending));
    const isOwnTemporaryChooser = (entry) => (pending) => pending.temporaryThread?.token === entry.token && isDeepStrictEqual(pending.temporaryThread.source, entry.source);
    const hasOtherTemporaryThreadReroute = (target, own) => hasPendingRerouteForTarget(target, isOwnTemporaryChooser(own));
    const applyThreadCleanupPlan = async (plan, assertExecutionCurrent, restoreCleanup, temporaryCleanup) => {
        assertExecutionCurrent?.();
        if ((restoreCleanup && restoreCleanup.routing?.cleanup !== "issued") ||
            (temporaryCleanup && !temporaryCleanup.cleanupIssued))
            return false;
        let protectedTarget = false, temporaryDeleteConfirmed = false, temporaryTransportUncertain = false;
        deps.recordThreadReconciliationPlan?.(plan);
        const result = await ThreadReconciler.applyThreadReconciliationPlan(plan, {
            skipCloseBeforeDelete: (temporaryCleanup?.target?.chatId ?? 0) > 0,
            isCleanupTargetProtected(target) {
                assertExecutionCurrent?.();
                // A tab's first source is not authority over sibling choosers, including unsettled cleanup-only work.
                const siblingProtected = !!temporaryCleanup && hasOtherTemporaryThreadReroute(target, temporaryCleanup);
                const lostTemporaryGrant = !!temporaryCleanup && !deps.getWorkspaceRestoreStore?.()?.listTemporaryThreads()
                    .some(entry => isDeepStrictEqual(entry, temporaryCleanup));
                const protectedNow = temporaryTransportUncertain || lostTemporaryGrant || siblingProtected || isRerouteTargetProtected(target, restoreCleanup, temporaryCleanup);
                protectedTarget ||= protectedNow;
                return protectedNow;
            },
            callApi: temporaryCleanup && deps.callApi
                ? async (method, body) => {
                    try {
                        assertExecutionCurrent?.();
                        const result = await deps.callApi(method, body, { maxAttempts: 1, retrySafety: "non-idempotent" });
                        assertExecutionCurrent?.();
                        if (result !== true)
                            throw new Error("Temporary Thread cleanup lacks a positive API acknowledgement.");
                        if (method === "deleteForumTopic")
                            temporaryDeleteConfirmed = true;
                        return result;
                    }
                    catch (error) {
                        temporaryTransportUncertain = true;
                        throw error;
                    }
                }
                : deps.callApi,
            markStaleByTarget: (staleTarget, syncStatus, lastSyncError) => deps.threadStore?.markStaleByTarget(staleTarget, syncStatus, lastSyncError) ?? false,
            persist: () => deps.threadStore?.persist() ?? Promise.resolve(),
            removePendingProvisionById: (id) => deps.threadStore?.removePendingProvision(id) ?? false,
            getCurrentLeaderEpoch: deps.getCurrentLeaderEpoch,
            recordRuntimeEvent: deps.recordRuntimeEvent,
        });
        assertExecutionCurrent?.();
        return !protectedTarget && (!temporaryCleanup || temporaryDeleteConfirmed) && (result.incompleteActions?.length ?? 0) === 0;
    };
    const findCreatedTemporaryThread = (target) => {
        try {
            return deps.getWorkspaceRestoreStore?.()?.listTemporaryThreads().find(value => value.phase === "created" && isDeepStrictEqual(value.target, target));
        }
        catch {
            return undefined;
        }
    };
    /**
     * One fenced capture of leader epoch, operator, journal binding and context for a temporary-Thread effect. `isCurrent`
     * re-reads every captured fact (plus an optional extra fence) at each boundary, so a delayed effect keeps the identity it
     * was scheduled under; `adopt` re-keys an entry to this executor before a write.
     */
    const captureTemporaryThreadAuthority = (ctx, extra) => {
        const store = deps.getWorkspaceRestoreStore?.();
        const instanceId = deps.getCurrentInstanceId?.(), epoch = deps.getCurrentLeaderEpoch?.();
        const operatorUserId = deps.configStore.getAllowedUserId(), journalBindingKey = deps.getAdmissionJournalBinding?.();
        const generation = deps.getSessionGeneration?.(), scope = deps.getAdmissionScope?.();
        if (!store || !instanceId || epoch === undefined || operatorUserId === undefined || !journalBindingKey)
            return undefined;
        const isCurrent = () => deps.isContextActive?.(ctx) === true && deps.getCurrentInstanceId?.() === instanceId &&
            deps.getSessionGeneration?.() === generation && deps.getAdmissionScope?.() === scope &&
            deps.getCurrentLeaderEpoch?.() === epoch && deps.configStore.getAllowedUserId() === operatorUserId &&
            deps.getAdmissionJournalBinding?.() === journalBindingKey && (extra?.() ?? true);
        const authority = { executor: { instanceId, leaderEpoch: String(epoch) }, operatorUserId, isCurrent };
        return { store, operatorUserId, journalBindingKey, epoch, authority, isCurrent,
            adopt: entry => isDeepStrictEqual(entry.executor, authority.executor) ? entry : store.adoptTemporaryThread(entry, authority) };
    };
    const isTemporaryTabTarget = (target) => !!target && findCreatedTemporaryThread(target) !== undefined;
    /** Restore may choose one input only when every other known group is cancelled or a live unassigned chooser that can be retained. */
    const areTemporaryThreadSiblingsAccountedForRestore = (entry, from) => {
        const target = entry.target;
        if (!target)
            return false;
        const group = { journalBindingKey: entry.source.journalBindingKey, updateIds: Updates.collectTelegramAdmissionSourceUpdateIds(from.messages) };
        const inputs = Threads.getTelegramTemporaryThreadInputs(entry), resolved = [...(entry.cancelledInputs ?? []), ...(entry.completedInputs ?? [])];
        if (!inputs.some(input => isDeepStrictEqual(input, group)) || resolved.some(input => isDeepStrictEqual(input, group)))
            return false;
        const others = [...pendingUnboundReroutes.values()].filter(pending => pending !== from &&
            pending.sourceTarget.chatId === target.chatId && pending.sourceTarget.threadId === target.threadId);
        const groupOf = (pending) => ({ journalBindingKey: entry.source.journalBindingKey,
            updateIds: Updates.collectTelegramAdmissionSourceUpdateIds(pending.messages) });
        if (!others.every(pending => !!pending.abandonment && !pending.abandonment.result && !pending.destinationSelected &&
            !pending.selectionAttempted && !pending.dispatching && !pending.foreignForwardIssued && !pending.foreignRetry && !pending.cleanup && !pending.finalizeMessage &&
            !pending.workspaceRestore && !entry.forwardedInputs?.some(input => isDeepStrictEqual(input, groupOf(pending))) &&
            inputs.some(input => isDeepStrictEqual(input, groupOf(pending)))))
            return false;
        return inputs.every(input => isDeepStrictEqual(input, group) || resolved.some(other => isDeepStrictEqual(other, input)) ||
            others.some(pending => isDeepStrictEqual(groupOf(pending), input)));
    };
    const isRerouteTargetProtected = (target, ownRestore, ownTemporary, restoreFrom) => {
        const matches = (candidate) => candidate.chatId === target.chatId && candidate.threadId === target.threadId;
        const activeTarget = deps.activeTurnRuntime.getTarget();
        if ((activeTarget && matches(activeTarget)) ||
            deps.telegramQueueStore.getQueuedItems().some(item => item.target && matches(item.target)) ||
            (deps.getLiveThreadTargets?.() ?? []).some(matches) ||
            (deps.threadStore?.list() ?? []).some(record => matches(record.target)) ||
            (deps.threadStore?.listReservations() ?? []).some(record => matches(record.target)) ||
            (deps.threadStore?.listPendingProvisions() ?? []).some(record => record.target && matches(record.target)))
            return true;
        const store = deps.getWorkspaceRestoreStore?.();
        if (deps.getWorkspaceRestoreStore && !store)
            return true;
        let temporary;
        try {
            temporary = store?.listTemporaryThreads() ?? [];
        }
        catch {
            return true;
        }
        // Only the source's own Forward/Cancel may remove its retained tab; adoption may have changed its executor.
        const ownTab = (entry) => !!ownTemporary && entry.token === ownTemporary.token &&
            isDeepStrictEqual(entry.source, ownTemporary.source) && isDeepStrictEqual(entry.target, ownTemporary.target);
        if (temporary.some(entry => !!entry.target && matches(entry.target) &&
            (!ownTab(entry) || (Threads.getTelegramTemporaryThreadInputs(entry).length > 1 &&
                !Threads.isTelegramTemporaryThreadFullyResolved(entry) &&
                !(restoreFrom && areTemporaryThreadSiblingsAccountedForRestore(entry, restoreFrom))))))
            return true;
        // Whole-tab cleanup after cancellation also needs a fresh census of retained arrivals no membership has seen.
        if (!restoreFrom && temporary.some(entry => ownTab(entry) && !!(entry.cancelledInputs?.length || entry.completedInputs?.length))) {
            try {
                const required = [...new Set([deps.getAdmissionJournalBinding?.(), ownTemporary.source.journalBindingKey,
                        ...Threads.getTelegramTemporaryThreadInputs(ownTemporary).map(input => input.journalBindingKey)].filter((key) => !!key))];
                if (deps.inspectTemporaryThreadSources?.({ chatId: target.chatId, threadId: target.threadId }, required)?.length !== 0)
                    return true;
            }
            catch {
                return true;
            }
        }
        const retained = store?.list() ?? [];
        // Exempt only the exact operation's old target. Cleanup itself also requires its issued grant.
        if (ownRestore && (!matches(ownRestore.request.binding.target) ||
            !retained.some(intent => isDeepStrictEqual(intent, ownRestore))))
            return true;
        if (retained.some(intent => (matches(intent.request.binding.target) || matches(intent.request.target)) &&
            !(ownRestore && isDeepStrictEqual(intent, ownRestore) && matches(intent.request.binding.target))))
            return true;
        if (!ownRestore)
            return false;
        let acceptedWorkClear = false;
        try {
            deps.threadStore?.withWorkspaceRestoreSnapshot(ownRestore, snapshot => {
                const bindings = snapshot.workspaceBindings?.filter(binding => binding.bindingKey === ownRestore.request.binding.bindingKey) ?? [];
                if (bindings.length !== 1 || !deps.captureWorkspaceExternalProtection)
                    return undefined;
                const binding = bindings[0];
                // Relocation does not erase predecessor references or prove where their work executes.
                const journalBindingKeys = [...new Set([
                        ...(ownRestore.request.binding.journalBindingKeys ?? []), ...(binding.journalBindingKeys ?? []),
                    ])];
                const journalSources = [...new Map([
                        ...(ownRestore.request.binding.journalSources ?? []), ...(binding.journalSources ?? []),
                    ].map(source => [JSON.stringify([source.sessionId, source.recipientBindingKey]), source])).values()];
                acceptedWorkClear = deps.captureWorkspaceExternalProtection({ ...binding, target: ownRestore.request.binding.target,
                    journalBindingKeys, journalSources }, { requireBindingProvenance: true }).acceptedWork === "clear";
                return undefined;
            });
        }
        catch (error) {
            acceptedWorkClear = false;
            deps.recordRuntimeEvent?.("telegram", error, { phase: "workspace-restore-protection" });
        }
        return !acceptedWorkClear;
    };
    const dismissRerouteChooserMessage = async (query, assertExecutionCurrent) => {
        const chatId = query.message?.chat?.id;
        const messageId = query.message?.message_id;
        if (typeof chatId !== "number" ||
            typeof messageId !== "number" ||
            !deps.deleteMessage) {
            return false;
        }
        try {
            assertExecutionCurrent?.();
            await deps.deleteMessage(chatId, messageId);
            assertExecutionCurrent?.();
            return true;
        }
        catch (error) {
            assertExecutionCurrent?.();
            deps.recordRuntimeEvent?.("telegram", error, {
                phase: "reroute-chooser-delete",
                chatId,
                messageId,
                threadId: query.message?.message_thread_id,
            });
            return false;
        }
    };
    const closeReroutedUnboundTopic = async (target, messageId, assertExecutionCurrent, ownTemporary) => {
        if (!target || !deps.threadStore)
            return true;
        const nowMs = Date.now();
        const currentLeaderEpoch = deps.getCurrentLeaderEpoch?.();
        const plan = ThreadReconciler.planThreadReconciliation({
            nowMs,
            currentLeaderEpoch,
            previousState: deps.getThreadReconciliationMachineState?.(),
            records: deps.threadStore.list(),
            reservations: deps.threadStore.listReservations(),
            pendingProvisions: deps.threadStore.listPendingProvisions(),
            unboundMessages: [
                {
                    target,
                    observedAtMs: nowMs,
                    ...(typeof messageId === "number" ? { messageId } : {}),
                    ...(currentLeaderEpoch !== undefined
                        ? { leaderEpoch: currentLeaderEpoch }
                        : {}),
                },
            ],
        });
        // A temporary grant licenses this tab only, not unrelated cleanup emitted by the general planner.
        const scoped = ownTemporary ? { ...plan, actions: plan.actions.filter(action => action.kind === "close-delete-unbound-topic" && isDeepStrictEqual(action.target, target)) } : plan;
        return applyThreadCleanupPlan(scoped, assertExecutionCurrent, undefined, ownTemporary);
    };
    const closePreviousLeaderThread = async (target, assertExecutionCurrent, restoreCleanup) => {
        if (!target || !deps.threadStore)
            return true;
        const currentLeaderEpoch = deps.getCurrentLeaderEpoch?.();
        return applyThreadCleanupPlan({
            actions: [
                {
                    kind: "close-delete-previous-leader-topic",
                    target,
                    reason: "previous-leader",
                    instanceId: deps.getCurrentInstanceId?.(),
                    ...(currentLeaderEpoch !== undefined
                        ? { leaderEpoch: currentLeaderEpoch }
                        : {}),
                },
            ],
        }, assertExecutionCurrent, restoreCleanup);
    };
    const closeReplacedFollowerThread = async (target, instanceId, assertExecutionCurrent, restoreCleanup) => {
        if (!target || !deps.threadStore)
            return true;
        const currentLeaderEpoch = deps.getCurrentLeaderEpoch?.();
        return applyThreadCleanupPlan({
            actions: [
                {
                    kind: "close-delete-replaced-follower-topic",
                    target,
                    reason: "replaced-follower",
                    instanceId,
                    ...(currentLeaderEpoch !== undefined
                        ? { leaderEpoch: currentLeaderEpoch }
                        : {}),
                },
            ],
        }, assertExecutionCurrent, restoreCleanup);
    };
    // Worker ACK observers outlive callback admission. Re-enter the existing Workspace gate;
    // never await this continuation while holding the dispatch invocation's gate.
    const restoreSettlementTasks = new Set();
    /**
     * After a Restore into a temporary tab reached positive terminal settlement, every still-unassigned sibling there is retained
     * privately and cancelled without Pi delivery. Missing proof, authority or abandonment leaves that sibling pending and the
     * tab's temporary entry protected; selected, queued, running or unknown work is never touched.
     */
    const disposeTemporaryThreadSiblingsAfterRestore = async (intent, ctx, restoreCurrent) => {
        const target = intent.request.target, cap = captureTemporaryThreadAuthority(ctx, restoreCurrent);
        const entry = findCreatedTemporaryThread(target);
        if (!cap || !entry || !entry.target || !cap.isCurrent())
            return;
        const operatorUserId = intent.operatorUserId, journalBindingKey = intent.request.source.journalBindingKey;
        if (cap.operatorUserId !== operatorUserId || cap.journalBindingKey !== journalBindingKey ||
            entry.operatorUserId !== operatorUserId || entry.source.journalBindingKey !== journalBindingKey)
            return;
        const group = { journalBindingKey, updateIds: [...intent.request.source.updateIds] };
        if (!Threads.getTelegramTemporaryThreadInputs(entry).some(input => isDeepStrictEqual(input, group)))
            return;
        for (const [id, pending] of [...pendingUnboundReroutes]) {
            const cancellation = pending.abandonment, source = pending.messages[0];
            if (pending.sourceTarget.chatId !== target.chatId || pending.sourceTarget.threadId !== target.threadId ||
                pending.workspaceRestore?.operationId === intent.request.operationId || !cancellation || !source || cancellation.result ||
                cancellation.running)
                continue;
            const current = () => cap.isCurrent() &&
                pendingUnboundReroutes.get(id) === pending && pending.abandonment === cancellation &&
                !pending.destinationSelected && !pending.selectionAttempted && !pending.dispatching && !pending.foreignForwardIssued && !pending.foreignRetry &&
                !pending.cleanup && !pending.finalizeMessage && !pending.workspaceRestore &&
                Updates.getTelegramUpdateExecutionFence(source)?.signal.aborted === false &&
                cancellation.ownerUserId === operatorUserId && cancellation.journalBindingKey === journalBindingKey;
            if (!current())
                continue;
            cancellation.running = true;
            try {
                const result = Updates.abandonTelegramDeferredUpdate(source, { operatorAuthorityId: `telegram-owner:${operatorUserId}`, isCurrent: current });
                if (!result)
                    continue;
                cancellation.attempted = true;
                cancellation.result = result;
                if (!recordCancelledTemporaryThreadInput(pending, ctx, current))
                    continue;
                await retireCancellationChooser(id, pending, current);
            }
            catch (error) {
                deps.recordRuntimeEvent?.("routing", error, { phase: "temporary-thread-restore-sibling", rerouteId: id });
            }
            finally {
                cancellation.running = false;
            }
        }
        // The entry is released only after every sibling is gone and the Restore group's completion is the single uncancelled input.
        if (hasPendingRerouteForTarget(target, pending => pending.workspaceRestore?.operationId === intent.request.operationId) ||
            !cap.isCurrent())
            return;
        const fresh = findCreatedTemporaryThread(target), owned = fresh && cap.adopt(fresh);
        if (owned && cap.isCurrent())
            cap.store.retireTemporaryThread(owned, cap.authority, group);
    };
    const observeRestoreSettlement = (signal, ctx) => {
        if (!deps.hasWorkspaceRestoreAuthority?.() || !deps.runWorkspaceOperation || !deps.getWorkspaceRestoreStore ||
            !deps.getSessionGeneration || deps.isContextActive?.(ctx) === false)
            return;
        const evidence = signal.kind === "source" ? structuredClone(signal.evidence) : undefined;
        const follower = signal.kind === "recipient" ? structuredClone(signal.follower) : undefined;
        const recipientCurrent = () => {
            if (signal.kind !== "recipient" || !follower)
                return true;
            const live = deps.workspaceRestoreRecipient?.followerRegistry.get(follower.instanceId);
            return signal.isCurrent() && !!live && Bus.hasTelegramBusCapability(live.protocol, Bus.TELEGRAM_BUS_CAPABILITY_WORKSPACE_RESTORE) &&
                isDeepStrictEqual({ ...live, lastHeartbeatMs: undefined, connectedAtMs: undefined, threadName: undefined }, { ...follower, lastHeartbeatMs: undefined, connectedAtMs: undefined, threadName: undefined });
        };
        const recipientMatches = (intent) => {
            if (!follower)
                return true;
            const ready = intent.readyRecipient ?? intent.recipient;
            return ready?.kind === "follower" && ready.instanceId === follower.instanceId && ready.sessionId === follower.sessionId &&
                ready.generation === follower.registrationGeneration && !!follower.cwd &&
                intent.request.binding.cwd === WorkspaceIdentity.normalizeTelegramWorkspacePath(follower.cwd) &&
                intent.request.binding.slot === follower.slot && isDeepStrictEqual(intent.request.target, follower.target);
        };
        // A same-session successor already registered on the relocated target may prove an issued grant by inspection only.
        const inspectableSuccessor = (intent) => !!follower?.registrationGeneration && !!follower.cwd && (intent.phase === "relocated" || intent.phase === "recipient-issued") &&
            intent.request.owner.owner?.kind === "manual-follower" && follower.sessionId === intent.request.binding.sessionId &&
            intent.request.binding.cwd === WorkspaceIdentity.normalizeTelegramWorkspacePath(follower.cwd) &&
            intent.request.binding.slot === follower.slot && isDeepStrictEqual(intent.request.target, follower.target);
        const instanceId = deps.getCurrentInstanceId?.(), epoch = deps.getCurrentLeaderEpoch?.();
        const operatorUserId = deps.configStore.getAllowedUserId();
        const generation = deps.getSessionGeneration(), scope = deps.getAdmissionScope?.();
        const sourceBinding = deps.getAdmissionJournalBinding?.();
        if (!instanceId || epoch === undefined || operatorUserId === undefined || !scope ||
            !sourceBinding || (evidence && evidence.journalBindingKey !== sourceBinding) || !recipientCurrent())
            return;
        // This same-session leader already runs on the relocated target; it may only inspect an issued leader grant.
        const leaderSessionId = deps.workspaceRestoreRecipient?.getSessionId(ctx);
        const leaderCwd = deps.workspaceRestoreRecipient?.getCwd(ctx);
        const leaderAt = (request) => {
            const ports = deps.workspaceRestoreRecipient;
            const cwd = ports?.getCwd(ctx), local = ports?.getLeaderIdentity();
            return !!cwd && cwd === leaderCwd && ports?.getSessionId(ctx) === leaderSessionId &&
                leaderSessionId === request.binding.sessionId && WorkspaceIdentity.normalizeTelegramWorkspacePath(cwd) === request.binding.cwd &&
                !!local && local.slot === request.binding.slot && isDeepStrictEqual(local.target, request.target);
        };
        const observeLeaderRestore = (intent, recipient, isCurrent) => {
            const threads = deps.threadStore;
            if (!threads || !isCurrent() || !leaderAt(intent.request))
                return undefined;
            const { request } = intent;
            let observation;
            threads.withWorkspaceRestoreSnapshot(intent, snapshot => {
                if (!isCurrent() || !leaderAt(request))
                    return;
                const bindings = (snapshot.workspaceBindings ?? []).filter(value => value.bindingKey === request.binding.bindingKey);
                const live = bindings[0];
                const owners = snapshot.threads.filter(value => value.status === "active" &&
                    (value.slot === request.binding.slot || isDeepStrictEqual(value.target, request.target)));
                if (bindings.length !== 1 || live?.sessionId !== request.binding.sessionId || live.cwd !== request.binding.cwd ||
                    live.slot !== request.binding.slot || live.inactiveSinceMs !== undefined || !isDeepStrictEqual(live.target, request.target) ||
                    owners.length !== 1 || owners[0]?.owner?.kind !== "leader" || owners[0].instanceId !== recipient.instanceId ||
                    owners[0].owner.instanceId !== recipient.instanceId || !owners[0].owner.cwd ||
                    WorkspaceIdentity.normalizeTelegramWorkspacePath(owners[0].owner.cwd) !== request.binding.cwd ||
                    owners[0].profileKey !== request.owner.profileKey || owners[0].slot !== request.binding.slot || !isDeepStrictEqual(owners[0].target, request.target))
                    return;
                observation = { operationId: request.operationId, recipient, target: request.target, slot: request.binding.slot, ready: true };
            });
            return isCurrent() && leaderAt(request) ? observation : undefined;
        };
        // Same-process lost replies stay with their chooser; this path only serves a restarted leader process.
        const inspectableLeader = (intent) => !!evidence &&
            intent.request.owner.owner?.kind === "leader" && leaderAt(intent.request) &&
            (intent.phase === "relocated" ? intent.request.owner.instanceId !== instanceId
                : intent.phase === "recipient-issued" && intent.recipient?.kind === "leader" && intent.recipient.instanceId !== instanceId);
        const current = () => deps.hasWorkspaceRestoreAuthority?.() === true && deps.isContextActive?.(ctx) !== false &&
            deps.getSessionGeneration?.() === generation && deps.getAdmissionScope?.() === scope &&
            deps.getAdmissionJournalBinding?.() === sourceBinding && deps.getCurrentInstanceId?.() === instanceId &&
            deps.getCurrentLeaderEpoch?.() === epoch && deps.configStore.getAllowedUserId() === operatorUserId && recipientCurrent();
        const recoveryRecipient = (intent) => {
            const ready = intent.readyRecipient ?? intent.recipient;
            if (!deps.inspectRestoreSourceCompletion || !deps.workspaceRestoreRecipient || intent.phase !== "ready" ||
                !intent.routing || intent.routing.cleanup !== undefined || intent.operatorUserId !== operatorUserId ||
                !intent.routing.acceptances?.some(value => value.kind === "forwarded"))
                return undefined;
            const live = deps.workspaceRestoreRecipient.followerRegistry.get(follower?.instanceId ?? ready?.instanceId ?? "");
            return ready?.kind === "follower" && live?.registrationGeneration && live.cwd &&
                Bus.hasTelegramBusCapability(live.protocol, Bus.TELEGRAM_BUS_CAPABILITY_WORKSPACE_RESTORE) &&
                Bus.hasTelegramBusCapability(live.protocol, Bus.TELEGRAM_BUS_CAPABILITY_DURABLE_FOLLOWER_ADMISSION) &&
                live.sessionId === intent.request.binding.sessionId && WorkspaceIdentity.normalizeTelegramWorkspacePath(live.cwd) === intent.request.binding.cwd &&
                live.slot === intent.request.binding.slot && isDeepStrictEqual(live.target, intent.request.target) ? live : undefined;
        };
        const recoveryLeader = (intent) => {
            const ready = intent.readyRecipient ?? intent.recipient;
            if (evidence?.kind !== "completed" || !deps.inspectRestoreSourceCompletion || !deps.threadStore ||
                intent.phase !== "ready" || !intent.routing || intent.routing.cleanup !== undefined || intent.operatorUserId !== operatorUserId ||
                intent.request.owner.owner?.kind !== "leader" || ready?.kind !== "leader" || ready.sessionId !== intent.request.binding.sessionId ||
                !leaderAt(intent.request) ||
                !intent.routing.acceptances?.some(value => value.kind === "completed" || value.kind === "queued"))
                return undefined;
            return { kind: "leader", instanceId, sessionId: intent.request.binding.sessionId, generation: String(generation) };
        };
        let selected;
        try {
            selected = deps.getWorkspaceRestoreStore()?.list().filter(intent => {
                if (intent.request.source.journalBindingKey !== sourceBinding)
                    return false;
                if (evidence && intent.request.source.updateIds.some(id => evidence.updateIds.includes(id)))
                    return true;
                if (((evidence?.kind === "completed" || follower) && recoveryRecipient(intent)) || recoveryLeader(intent))
                    return true;
                if (follower && deps.workspaceRestoreRecipient && inspectableSuccessor(intent))
                    return true;
                if (deps.threadStore && inspectableLeader(intent))
                    return true;
                // Completion rechecks an unsent Restore whose originals may all have been abandoned before its retirement published.
                if (evidence?.kind === "completed" && deps.inspectRestoreSourceAbandonment && intent.phase === "ready" &&
                    intent.routing === undefined)
                    return true;
                // Hints only wake fully settled, unissued cleanup. They never settle another source.
                return (evidence?.kind === "completed" || !!follower) && intent.phase === "ready" && intent.routing?.cleanup === undefined &&
                    recipientMatches(intent) && intent.request.source.updateIds.every(id => intent.routing?.settlements.some(value => value.kind !== "queued" && value.updateIds.includes(id)));
            }).map(intent => intent.request.operationId) ?? [];
        }
        catch (error) {
            deps.recordRuntimeEvent?.("telegram", error, { phase: "workspace-restore-settlement" });
            return;
        }
        if (!selected.length || !current())
            return;
        const task = Promise.resolve().then(() => deps.runWorkspaceOperation({ operationId: `restore-settlement-${randomBytes(16).toString("hex")}`,
            operationKind: "workspace.restore-settlement", scopes: [{ kind: "profile" }] }, async () => {
            if (!current())
                return;
            const store = deps.getWorkspaceRestoreStore();
            if (!store)
                return;
            const authority = {
                executor: { instanceId, leaderEpoch: String(epoch) }, operatorUserId, isCurrent: current
            };
            const assertCurrent = () => { if (!current())
                throw new Error("Workspace Restore settlement authority ended."); };
            // Exact owner abandonment of every original is terminal for an unsent Restore; no cleanup or delivery follows.
            const abandoned = (value) => {
                if (evidence?.kind !== "completed" || !value.request.source.updateIds.length)
                    return false;
                return value.request.source.updateIds.every(updateId => {
                    const proof = deps.inspectRestoreSourceAbandonment?.(updateId, sourceBinding);
                    assertCurrent();
                    return proof?.journalBindingKey === sourceBinding && proof.updateId === updateId &&
                        proof.operatorAuthorityId === `telegram-owner:${operatorUserId}`;
                });
            };
            for (const operationId of selected) {
                assertCurrent();
                let intent = store.list().find(value => value.request.operationId === operationId);
                let settlementCurrent = current;
                let settlementCanonicalCurrent;
                let retired = false;
                if (follower && intent && inspectableSuccessor(intent) && intent.operatorUserId === operatorUserId) {
                    const ports = deps.workspaceRestoreRecipient;
                    const successor = { kind: "follower", instanceId: follower.instanceId,
                        sessionId: follower.sessionId, generation: follower.registrationGeneration };
                    const { request } = intent;
                    // Adoption plus inspect never consumes the original apply grant, delivers input or issues cleanup.
                    await advanceTelegramWorkspaceRestore({ request, authority, restoreStore: store, inspectOnly: true,
                        getRecipient: () => current() ? successor : undefined,
                        async runRecipient(action) {
                            if (action.mode !== "inspect" || !action.isCurrent())
                                return undefined;
                            return ports.runFollower({ operationId: request.operationId, instanceId: successor.instanceId,
                                sessionId: successor.sessionId, slot: request.binding.slot, target: request.target,
                                oldTarget: request.binding.target, mode: "inspect", isCurrent: action.isCurrent });
                        } });
                    continue;
                }
                if (intent && inspectableLeader(intent) && intent.operatorUserId === operatorUserId) {
                    const threads = deps.threadStore;
                    const leader = { kind: "leader", instanceId,
                        sessionId: intent.request.binding.sessionId, generation: String(generation) };
                    const { request } = intent;
                    await threads.load();
                    assertCurrent();
                    await advanceTelegramWorkspaceRestore({ request, authority, restoreStore: store, inspectOnly: true,
                        getRecipient: () => current() && leaderAt(request) ? leader : undefined,
                        async runRecipient(action) {
                            if (action.mode !== "inspect")
                                return undefined;
                            return observeLeaderRestore(action.intent, leader, action.isCurrent);
                        } });
                    continue;
                }
                if (intent?.phase === "ready" && intent.routing === undefined && intent.operatorUserId === operatorUserId &&
                    intent.request.source.journalBindingKey === sourceBinding && abandoned(intent)) {
                    const owned = isDeepStrictEqual(intent.executor, authority.executor) ? intent : store.adopt(intent, authority);
                    assertCurrent();
                    if (owned)
                        store.retireAbandoned(owned, owned.request.source.updateIds, authority);
                    continue;
                }
                const liveRecipient = intent && recoveryRecipient(intent), localRecipient = intent && recoveryLeader(intent);
                if (intent && (liveRecipient || localRecipient) && intent.request.source.journalBindingKey === sourceBinding) {
                    const expected = intent, ports = deps.workspaceRestoreRecipient;
                    const recoveryCurrent = () => current() && (localRecipient ? leaderAt(expected.request) && isDeepStrictEqual(recoveryLeader(expected), localRecipient) : isDeepStrictEqual({ ...recoveryRecipient(expected), lastHeartbeatMs: undefined, connectedAtMs: undefined, threadName: undefined }, { ...liveRecipient, lastHeartbeatMs: undefined, connectedAtMs: undefined, threadName: undefined }));
                    // Storage authority callbacks check live fences only; canonical reads cannot re-enter their transaction lock.
                    const recoveryAuthority = { ...authority, isCurrent: recoveryCurrent };
                    settlementCurrent = recoveryCurrent;
                    if (localRecipient) {
                        settlementCanonicalCurrent = () => !!intent && !!observeLeaderRestore(intent, localRecipient, recoveryCurrent);
                        if (!settlementCanonicalCurrent())
                            continue; // A local target alone cannot authorize executor adoption.
                    }
                    const inspect = (acceptance, operation) => {
                        const completion = { journalBindingKey: sourceBinding, updateId: acceptance.updateId,
                            sourceSha256: acceptance.sourceSha256,
                            completionSha256: Threads.getTelegramWorkspaceRestoreSourceCompletionSha256(operation, acceptance) };
                        const observed = deps.inspectRestoreSourceCompletion({ ...completion });
                        if (!recoveryCurrent())
                            throw new Error("Workspace Restore completion observation authority ended.");
                        return isDeepStrictEqual(observed, completion);
                    };
                    // Inspect proof before adopting: missing/foreign ACKs cannot even re-key the retained operation.
                    const accepted = expected.routing.acceptances.filter(value => (localRecipient ? value.kind === "completed" || value.kind === "queued" : value.kind === "forwarded") && inspect(value, expected));
                    if (accepted.length) {
                        const successor = localRecipient ?? { kind: "follower", instanceId: liveRecipient.instanceId,
                            sessionId: liveRecipient.sessionId, generation: liveRecipient.registrationGeneration };
                        const { request } = expected;
                        const inspected = await advanceTelegramWorkspaceRestore({ request, authority: recoveryAuthority, restoreStore: store, inspectOnly: true,
                            getRecipient: () => recoveryCurrent() ? successor : undefined,
                            async runRecipient(action) {
                                if (action.mode !== "inspect" || !action.isCurrent())
                                    return undefined;
                                if (localRecipient)
                                    return observeLeaderRestore(action.intent, successor, action.isCurrent);
                                return ports.runFollower({ operationId: request.operationId, instanceId: successor.instanceId,
                                    sessionId: successor.sessionId, slot: request.binding.slot, target: request.target,
                                    oldTarget: request.binding.target, mode: "inspect", isCurrent: action.isCurrent });
                            } });
                        if (!inspected || !recoveryCurrent())
                            continue;
                        intent = inspected;
                        if (settlementCanonicalCurrent && !settlementCanonicalCurrent())
                            continue;
                        // The await cannot lend earlier readback; confirm each immutable proof again before publication.
                        const settledIds = new Set(intent.routing.settlements.filter(value => value.kind !== "queued").flatMap(value => value.updateIds));
                        const recovered = new Map();
                        for (const acceptance of accepted.filter(value => !settledIds.has(value.updateId) && inspect(value, intent))) {
                            const key = acceptance.kind === "queued" ? JSON.stringify([acceptance.receiptId, acceptance.queueKind]) : "completed";
                            const settlement = recovered.get(key) ?? { journalBindingKey: sourceBinding, updateIds: [],
                                ...(acceptance.kind === "queued" ? { kind: "queue-completed", receiptId: acceptance.receiptId, queueKind: acceptance.queueKind }
                                    : { kind: "completed" }) };
                            settlement.updateIds.push(acceptance.updateId);
                            recovered.set(key, settlement);
                        }
                        for (const settlement of recovered.values()) {
                            intent = store.recordSourceSettlement(intent, settlement, recoveryAuthority);
                            if (!intent)
                                break;
                        }
                        if (!intent)
                            continue;
                    }
                }
                // Source hints carry no registry fence; cleanup must keep the acknowledged recipient exact across awaits.
                const readyRecipientCurrent = () => {
                    const ready = intent?.readyRecipient ?? intent?.recipient;
                    if (intent?.phase !== "ready" || ready?.kind !== "follower")
                        return true;
                    const live = deps.workspaceRestoreRecipient?.followerRegistry.get(ready.instanceId);
                    return !!live?.cwd && live.registrationGeneration === ready.generation && live.sessionId === ready.sessionId &&
                        WorkspaceIdentity.normalizeTelegramWorkspacePath(live.cwd) === intent.request.binding.cwd &&
                        live.slot === intent.request.binding.slot && isDeepStrictEqual(live.target, intent.request.target) &&
                        Bus.hasTelegramBusCapability(live.protocol, Bus.TELEGRAM_BUS_CAPABILITY_WORKSPACE_RESTORE) &&
                        Bus.hasTelegramBusCapability(live.protocol, Bus.TELEGRAM_BUS_CAPABILITY_DURABLE_FOLLOWER_ADMISSION);
                };
                const settlementAuthority = { ...authority, isCurrent: function () { return settlementCurrent() && readyRecipientCurrent(); } };
                const assertSettlementCurrent = () => {
                    if (!settlementAuthority.isCurrent() || (!retired && settlementCanonicalCurrent && !settlementCanonicalCurrent())) {
                        throw new Error("Workspace Restore settlement authority ended.");
                    }
                };
                assertSettlementCurrent();
                if (!intent || intent.phase !== "ready" || !intent.routing || !isDeepStrictEqual(intent.executor, authority.executor) ||
                    intent.operatorUserId !== operatorUserId || intent.request.source.journalBindingKey !== sourceBinding)
                    continue;
                const settled = new Set(intent.routing.settlements.flatMap(value => value.updateIds));
                if (follower && (intent.routing.cleanup !== undefined || !recipientMatches(intent) ||
                    !intent.request.source.updateIds.every(id => settled.has(id))))
                    continue;
                const updateIds = evidence?.updateIds.filter(id => intent.request.source.updateIds.includes(id) && !settled.has(id)) ?? [];
                if (evidence && updateIds.length) {
                    intent = store.recordSourceSettlement(intent, { ...evidence, updateIds }, settlementAuthority);
                    if (!intent)
                        continue;
                }
                if (intent.routing?.cleanup === undefined) {
                    if (!intent.request.source.updateIds.every(id => intent.routing.settlements.some(value => value.kind !== "queued" && value.updateIds.includes(id))))
                        continue;
                    // Positive settlement of every selected source is the success proof; old-thread cleanup may stay protected indefinitely.
                    await disposeTemporaryThreadSiblingsAfterRestore(intent, ctx, settlementAuthority.isCurrent);
                    assertSettlementCurrent();
                    if (isRerouteTargetProtected(intent.request.binding.target, intent))
                        continue;
                    const grant = store.issueCleanup(intent, settlementAuthority);
                    if (!grant)
                        continue;
                    intent = grant.intent;
                    const oldTarget = intent.request.binding.target;
                    const completed = intent.request.owner.owner?.kind === "leader"
                        ? await closePreviousLeaderThread(oldTarget, assertSettlementCurrent, intent)
                        : await closeReplacedFollowerThread(oldTarget, intent.request.owner.instanceId, assertSettlementCurrent, intent);
                    assertSettlementCurrent();
                    if (!completed)
                        continue; // Issued uncertainty stays protected, never downgraded or retried.
                    intent = store.recordCleanup(intent, { kind: "completed", target: oldTarget }, settlementAuthority);
                    if (!intent)
                        continue;
                }
                if (!store.retire(intent, settlementAuthority))
                    continue;
                retired = true;
                for (const [id, pending] of pendingUnboundReroutes) {
                    if (pending.workspaceRestore?.operationId !== operationId)
                        continue;
                    pending.finalizeMessage = "Message routed.";
                    if (!deps.editInteractiveMessage || pending.chooserMessageId === undefined)
                        continue;
                    assertSettlementCurrent();
                    await deps.editInteractiveMessage(pending.sourceTarget.chatId, pending.chooserMessageId, "<b>✅ Message routed.</b>", "html", { inline_keyboard: [] });
                    assertSettlementCurrent();
                    if (pendingUnboundReroutes.get(id) === pending)
                        removePendingReroute(id);
                }
            }
        })).catch(error => { deps.recordRuntimeEvent?.("telegram", error, { phase: "workspace-restore-settlement" }); });
        restoreSettlementTasks.add(task);
        void task.finally(() => restoreSettlementTasks.delete(task));
        return task;
    };
    const beforeQueueReceiptPublished = async (receipt, queueOwner, ctx, isWorkerCurrent) => {
        const store = deps.getWorkspaceRestoreStore?.();
        if (!store)
            return;
        const selected = store.list().filter(intent => intent.request.source.journalBindingKey === receipt.journalBindingKey &&
            intent.request.source.updateIds.some(id => receipt.sourceUpdateIds.includes(id))).map(intent => intent.request.operationId);
        if (!selected.length)
            return;
        const instanceId = deps.getCurrentInstanceId?.(), epoch = deps.getCurrentLeaderEpoch?.();
        const generation = deps.getSessionGeneration?.(), scope = deps.getAdmissionScope?.(), operatorUserId = deps.configStore.getAllowedUserId();
        const binding = receipt.journalBindingKey, ports = deps.workspaceRestoreRecipient, threads = deps.threadStore;
        if (!instanceId || epoch === undefined || generation === undefined || !scope || !binding || operatorUserId === undefined ||
            !ports || !threads || !deps.runWorkspaceOperation || !deps.inspectRestoreQueuedReceipt) {
            throw new Error("Workspace Restore queued acceptance authority is unavailable.");
        }
        const expectedReceipt = { queueKind: receipt.queueKind, receiptId: receipt.receiptId,
            sourceUpdateIds: [...receipt.sourceUpdateIds], queueOwner: { ...queueOwner } };
        const current = () => isWorkerCurrent() && deps.hasWorkspaceRestoreAuthority?.() === true &&
            deps.isContextActive?.(ctx) !== false && deps.getSessionGeneration?.() === generation && deps.getAdmissionScope?.() === scope &&
            deps.getAdmissionJournalBinding?.() === binding && deps.getCurrentInstanceId?.() === instanceId &&
            deps.getCurrentLeaderEpoch?.() === epoch && deps.configStore.getAllowedUserId() === operatorUserId &&
            queueOwner.instanceId === instanceId && queueOwner.sessionGeneration === generation;
        const assertCurrent = () => { if (!current())
            throw new Error("Workspace Restore queued acceptance authority changed."); };
        assertCurrent();
        const recipientGuards = [];
        const sourceCompletions = [];
        await deps.runWorkspaceOperation({ operationId: `restore-queue-acceptance-${randomBytes(16).toString("hex")}`,
            operationKind: "workspace.restore-queue-acceptance", scopes: [{ kind: "profile" }] }, async () => {
            assertCurrent();
            const active = deps.getWorkspaceRestoreStore?.();
            if (!active)
                throw new Error("Workspace Restore queued acceptance storage disappeared.");
            const proof = deps.inspectRestoreQueuedReceipt({ ...structuredClone(expectedReceipt), journalBindingKey: binding });
            assertCurrent();
            if (!proof || !isDeepStrictEqual(proof.receipt, expectedReceipt) || !/^[a-f0-9]{64}$/u.test(proof.queueOwnerSha256) ||
                proof.sources.length !== receipt.sourceUpdateIds.length || proof.sources.some((source, index) => source.updateId !== receipt.sourceUpdateIds[index] || !/^[a-f0-9]{64}$/u.test(source.sourceSha256))) {
                throw new Error("Workspace Restore queued receipt proof is unavailable.");
            }
            for (const operationId of selected) {
                let intent = active.list().find(value => value.request.operationId === operationId);
                const recipient = intent?.readyRecipient ?? intent?.recipient;
                if (!intent || intent.phase !== "ready" || !intent.routing || intent.operatorUserId !== operatorUserId ||
                    !isDeepStrictEqual(intent.executor, { instanceId, leaderEpoch: String(epoch) }) ||
                    intent.request.source.journalBindingKey !== binding || recipient?.kind !== "leader" || recipient.instanceId !== instanceId ||
                    recipient.generation !== String(generation) || recipient.sessionId !== intent.request.binding.sessionId) {
                    throw new Error("Workspace Restore queued recipient proof changed.");
                }
                const request = intent.request;
                const liveRecipientCurrent = () => {
                    const local = ports.getLeaderIdentity(), cwd = ports.getCwd(ctx);
                    return current() && !!local && local.slot === request.binding.slot && isDeepStrictEqual(local.target, request.target) &&
                        ports.getSessionId(ctx) === recipient.sessionId && !!cwd && WorkspaceIdentity.normalizeTelegramWorkspacePath(cwd) === request.binding.cwd;
                };
                const recipientCurrent = () => {
                    if (!liveRecipientCurrent())
                        return false;
                    let committed = false;
                    threads.withWorkspaceRestoreSnapshot(intent, snapshot => {
                        const live = snapshot.workspaceBindings?.filter(value => value.bindingKey === request.binding.bindingKey) ?? [];
                        const owners = snapshot.threads.filter(value => value.status === "active" &&
                            (value.slot === request.binding.slot || isDeepStrictEqual(value.target, request.target)));
                        const local = ports.getLeaderIdentity(), cwd = ports.getCwd(ctx);
                        committed = live.length === 1 && live[0]?.sessionId === recipient.sessionId && live[0].cwd === request.binding.cwd &&
                            live[0].slot === request.binding.slot && live[0].inactiveSinceMs === undefined && isDeepStrictEqual(live[0].target, request.target) &&
                            owners.length === 1 && owners[0]?.owner?.kind === "leader" && owners[0].instanceId === instanceId &&
                            owners[0].owner.instanceId === instanceId && !!owners[0].owner.cwd &&
                            WorkspaceIdentity.normalizeTelegramWorkspacePath(owners[0].owner.cwd) === request.binding.cwd && owners[0].profileKey === request.owner.profileKey &&
                            owners[0].slot === request.binding.slot && isDeepStrictEqual(owners[0].target, request.target) &&
                            !!local && local.slot === request.binding.slot && isDeepStrictEqual(local.target, request.target) &&
                            ports.getSessionId(ctx) === recipient.sessionId && !!cwd && WorkspaceIdentity.normalizeTelegramWorkspacePath(cwd) === request.binding.cwd;
                    });
                    return committed && liveRecipientCurrent();
                };
                recipientGuards.push(recipientCurrent);
                // Storage owns the canonical CAS; its in-transaction authority callback cannot reacquire the snapshot lock.
                const authority = { executor: { instanceId, leaderEpoch: String(epoch) }, operatorUserId, isCurrent: liveRecipientCurrent };
                for (const source of proof.sources.filter(value => request.source.updateIds.includes(value.updateId))) {
                    if (!recipientCurrent())
                        throw new Error("Workspace Restore queued recipient authority changed.");
                    const evidence = { ...source, journalBindingKey: binding, recipient,
                        kind: "queued", receiptId: receipt.receiptId, queueKind: receipt.queueKind, queueOwnerSha256: proof.queueOwnerSha256 };
                    let accepted, failure;
                    try {
                        accepted = active.recordSourceAcceptance(intent, evidence, authority);
                    }
                    catch (error) {
                        failure = error;
                    }
                    if (!accepted)
                        accepted = active.list().find(value => value.request.operationId === operationId &&
                            isDeepStrictEqual(value.request, request) && isDeepStrictEqual(value.executor, authority.executor) && value.operatorUserId === operatorUserId &&
                            value.routing?.acceptances?.some(retained => isDeepStrictEqual(retained, evidence)));
                    if (!accepted || !isDeepStrictEqual(accepted.request, request) || !isDeepStrictEqual(accepted.executor, authority.executor) ||
                        accepted.operatorUserId !== operatorUserId || !accepted.routing?.acceptances?.some(value => isDeepStrictEqual(value, evidence))) {
                        throw failure ?? new Error("Workspace Restore queued acceptance was not published.");
                    }
                    intent = accepted;
                    sourceCompletions.push({ ...source, journalBindingKey: binding,
                        completionSha256: Threads.getTelegramWorkspaceRestoreSourceCompletionSha256(accepted, evidence) });
                    if (!recipientCurrent())
                        throw new Error("Workspace Restore queued recipient authority changed.");
                }
            }
            const observed = deps.inspectRestoreQueuedReceipt({ ...structuredClone(expectedReceipt), journalBindingKey: binding });
            assertCurrent();
            if (!isDeepStrictEqual(observed, proof))
                throw new Error("Workspace Restore queued receipt changed after publication.");
        });
        assertCurrent();
        if (recipientGuards.some(guard => !guard()))
            throw new Error("Workspace Restore queued recipient changed after admission.");
        return sourceCompletions.sort((a, b) => a.updateId - b.updateId);
    };
    const onQueueReceiptCompleted = (receipt, ctx) => {
        // A hint re-enters admission and rereads scoped proof; it never supplies completed command evidence for a queued source.
        if (!receipt.journalBindingKey)
            return;
        observeRestoreSettlement({ kind: "source", evidence: { journalBindingKey: receipt.journalBindingKey,
                updateIds: [...receipt.sourceUpdateIds], kind: "completed" } }, ctx);
        for (const updateId of receipt.sourceUpdateIds)
            retireCompletedTemporaryThread(updateId, ctx, receipt.journalBindingKey);
    };
    const onQueueReceiptCommitted = (receipt, ctx) => {
        if (receipt.journalBindingKey)
            observeRestoreSettlement({ kind: "source", evidence: { journalBindingKey: receipt.journalBindingKey,
                    updateIds: [...receipt.sourceUpdateIds], kind: "queued", receiptId: receipt.receiptId, queueKind: receipt.queueKind } }, ctx);
    };
    // Worker-confirmed source completion is the only evidence for a Forwarded input. It records a durable group fact and
    // starts the same delayed, fully rechecked cleanup as the last Cancel; deletion itself never happens here.
    const completedTemporaryInputIds = new Map();
    const retireCompletedTemporaryThread = (updateId, ctx, journalBindingKey) => {
        const cap = captureTemporaryThreadAuthority(ctx);
        if (!cap || !deps.runWorkspaceOperation || cap.journalBindingKey !== journalBindingKey)
            return;
        const store = cap.store;
        const matches = (entry) => entry.source.journalBindingKey === journalBindingKey && entry.source.updateId === updateId;
        const inGroup = (entry) => Threads.getTelegramTemporaryThreadInputs(entry).some(input => input.journalBindingKey === journalBindingKey && input.updateIds.includes(updateId));
        let restoreOwned = false;
        try {
            if (!store.listTemporaryThreads().some(entry => matches(entry) || inGroup(entry)))
                return;
            restoreOwned = store.list().some(intent => intent.request.source.journalBindingKey === journalBindingKey &&
                intent.request.source.updateIds.includes(updateId));
        }
        catch (error) {
            deps.recordRuntimeEvent?.("routing", error, { phase: "temporary-thread-retire" });
            return;
        }
        const task = Promise.resolve().then(() => deps.runWorkspaceOperation({ operationId: `temporary-thread-retire-${randomBytes(16).toString("hex")}`,
            operationKind: "workspace.temporary-thread", scopes: [{ kind: "profile" }] }, async () => {
            if (!cap.isCurrent())
                return;
            let entry = store.listTemporaryThreads().find(restoreOwned ? matches : inGroup);
            if (!entry)
                return;
            if (restoreOwned) {
                // A Restore keeps its own lifecycle: only its source entry may be released, never by Forward bookkeeping.
                if (entry.target && hasOtherTemporaryThreadReroute(entry.target, entry))
                    return;
                entry = cap.adopt(entry);
                if (entry && cap.isCurrent())
                    store.retireTemporaryThread(entry, cap.authority);
                return;
            }
            const group = Threads.getTelegramTemporaryThreadInputs(entry).find(input => input.journalBindingKey === journalBindingKey && input.updateIds.includes(updateId));
            const target = entry.target;
            if (!group || entry.phase !== "created" || !target)
                return;
            if (entry.cancelledInputs?.some(input => isDeepStrictEqual(input, group)) ||
                entry.completedInputs?.some(input => isDeepStrictEqual(input, group)))
                return;
            // Process-local accumulation only: a restart forgets partial groups and the tab stays protected.
            const seen = completedTemporaryInputIds.get(entry.token) ?? new Set();
            seen.add(updateId);
            completedTemporaryInputIds.set(entry.token, seen);
            if (!group.updateIds.every(id => seen.has(id)))
                return;
            entry = cap.adopt(entry);
            if (!entry || !cap.isCurrent() || !store.recordTemporaryThreadInputCompletion(entry, group, cap.authority) || !cap.isCurrent())
                return;
            scheduleTemporaryThreadCleanup(target, ctx);
            return true;
        })).then(published => {
            // A sibling may complete after Restore already settled. Reinspect only after its terminal fact publishes and admission releases.
            if (published && cap.isCurrent())
                observeRestoreSettlement({ kind: "source",
                    evidence: { journalBindingKey, updateIds: [updateId], kind: "completed" } }, ctx);
        }).catch(error => { deps.recordRuntimeEvent?.("routing", error, { phase: "temporary-thread-retire" }); });
        restoreSettlementTasks.add(task);
        void task.finally(() => restoreSettlementTasks.delete(task));
    };
    const onUpdateCompleted = (updateId, ctx, journalBindingKey) => {
        if (!journalBindingKey)
            return;
        observeRestoreSettlement({ kind: "source", evidence: { journalBindingKey, updateIds: [updateId], kind: "completed" } }, ctx);
        retireCompletedTemporaryThread(updateId, ctx, journalBindingKey);
    };
    const restoreWorkspace = deps.workspaceRestoreRecipient ? async (input) => {
        const ports = deps.workspaceRestoreRecipient;
        const threads = deps.threadStore;
        if (!threads || !deps.getWorkspaceRestoreStore || !deps.getSessionGeneration ||
            !deps.hasWorkspaceRestoreAuthority?.() || !input.isCurrent() || input.target.threadId === undefined)
            return "protected";
        const instanceId = deps.getCurrentInstanceId?.(), epoch = deps.getCurrentLeaderEpoch?.();
        const operatorUserId = deps.configStore.getAllowedUserId(), scope = deps.getAdmissionScope?.();
        const journalBindingKey = deps.getAdmissionJournalBinding?.(), generation = deps.getSessionGeneration();
        const sourceIds = Updates.collectTelegramAdmissionSourceUpdateIds(input.messages);
        if (!instanceId || epoch === undefined || operatorUserId === undefined || !scope || !journalBindingKey || !sourceIds.length)
            return "protected";
        const current = () => input.isCurrent() && deps.hasWorkspaceRestoreAuthority?.() === true &&
            deps.isContextActive?.(input.ctx) !== false && deps.getSessionGeneration?.() === generation &&
            deps.getCurrentInstanceId?.() === instanceId && deps.getCurrentLeaderEpoch?.() === epoch &&
            deps.getAdmissionScope?.() === scope && deps.getAdmissionJournalBinding?.() === journalBindingKey &&
            deps.configStore.getAllowedUserId() === operatorUserId;
        await threads.load();
        if (!current())
            return "protected";
        const store = deps.getWorkspaceRestoreStore();
        if (!store)
            return "protected";
        const retained = store.list().find(value => value.request.operationId === input.operationId);
        const bindings = threads.listWorkspaceBindings().filter(value => isDeepStrictEqual(value.target, input.record.target));
        const binding = retained?.request.binding ?? (bindings.length === 1 ? bindings[0] : undefined);
        if (!binding || !binding.sessionId || !binding.slot)
            return "protected";
        const { sessionId, slot } = binding;
        const request = { operationId: input.operationId,
            binding, owner: structuredClone(input.record), target: { chatId: input.target.chatId, threadId: input.target.threadId },
            source: { journalBindingKey, updateIds: sourceIds } };
        if (retained && !isDeepStrictEqual(retained.request, request))
            return "protected";
        const getRecipient = (snapshot) => {
            if (!current())
                return undefined;
            const liveBindings = (snapshot ? snapshot.workspaceBindings ?? [] : threads.listWorkspaceBindings())
                .filter(value => value.bindingKey === binding.bindingKey);
            const live = liveBindings[0];
            if (liveBindings.length !== 1 || live?.sessionId !== binding.sessionId || live.cwd !== binding.cwd || live.slot !== binding.slot ||
                live.inactiveSinceMs !== undefined || (!isDeepStrictEqual(live.target, binding.target) && !isDeepStrictEqual(live.target, request.target)))
                return undefined;
            const records = (snapshot ? snapshot.threads : threads.list()).filter(value => value.status === "active" &&
                (value.slot === binding.slot || isDeepStrictEqual(value.target, live.target)));
            const record = records[0];
            if (records.length !== 1 || record?.slot !== binding.slot || !isDeepStrictEqual(record.target, live.target))
                return undefined;
            if (record.owner?.kind === "leader" && record.instanceId === instanceId) {
                const cwd = ports.getCwd(input.ctx);
                if (ports.getSessionId(input.ctx) !== sessionId || !cwd || WorkspaceIdentity.normalizeTelegramWorkspacePath(cwd) !== binding.cwd ||
                    !deps.setCurrentLeaderIdentity)
                    return undefined;
                return { kind: "leader", instanceId, sessionId, generation: String(generation) };
            }
            if (record.owner?.kind !== "manual-follower" || !record.instanceId)
                return undefined;
            const follower = ports.followerRegistry.get(record.instanceId);
            if (!follower?.registrationGeneration || !follower.cwd || follower.sessionId !== sessionId ||
                WorkspaceIdentity.normalizeTelegramWorkspacePath(follower.cwd) !== binding.cwd || follower.slot !== slot ||
                !Bus.hasTelegramBusCapability(follower.protocol, Bus.TELEGRAM_BUS_CAPABILITY_WORKSPACE_RESTORE))
                return undefined;
            return { kind: "follower", instanceId: follower.instanceId, sessionId, generation: follower.registrationGeneration };
        };
        const hasCommittedTarget = () => threads.listWorkspaceBindings().some(value => value.bindingKey === binding.bindingKey && isDeepStrictEqual(value.target, request.target));
        const authority = { executor: { instanceId, leaderEpoch: String(epoch) }, operatorUserId, isCurrent: current };
        const ready = await advanceTelegramWorkspaceRestore({ request, authority, restoreStore: store, getRecipient,
            async runRecipient(action) {
                const recipient = getRecipient();
                if (!recipient || !action.isCurrent() || !hasCommittedTarget())
                    return undefined;
                if (recipient.kind === "follower") {
                    const follower = ports.followerRegistry.get(recipient.instanceId);
                    const observation = await ports.runFollower({ operationId: request.operationId, instanceId: recipient.instanceId,
                        sessionId: recipient.sessionId, slot: binding.slot, target: request.target, oldTarget: binding.target,
                        mode: action.mode, isCurrent: action.isCurrent });
                    if (!action.isCurrent() || !hasCommittedTarget())
                        return undefined;
                    if (!follower || !observation?.ready)
                        return observation;
                    if (observation.operationId !== request.operationId || !isDeepStrictEqual(observation.recipient, recipient) ||
                        !isDeepStrictEqual(observation.target, request.target) || observation.slot !== binding.slot)
                        return undefined;
                    const actual = ports.followerRegistry.get(recipient.instanceId);
                    if (!actual || actual.registrationGeneration !== follower.registrationGeneration || actual.sessionId !== follower.sessionId ||
                        actual.cwd !== follower.cwd || actual.slot !== follower.slot || actual.profileKey !== follower.profileKey ||
                        actual.busSocketPath !== follower.busSocketPath || !isDeepStrictEqual(actual.target, follower.target) ||
                        !isDeepStrictEqual(actual.protocol, follower.protocol))
                        return undefined;
                    threads.commitWorkspaceRestoreRegistration({ target: request.target, bindingKey: binding.bindingKey, slot: binding.slot }, () => {
                        if (!action.isCurrent())
                            throw new Error("Workspace Restore recipient authority changed.");
                        ports.followerRegistry.register({ ...actual, target: request.target, connectedAtMs: actual.connectedAtMs });
                    });
                    return observation;
                }
                let observation;
                threads.withWorkspaceRestoreSnapshot(action.intent, snapshot => {
                    if (!action.isCurrent() || !isDeepStrictEqual(getRecipient(snapshot), recipient))
                        return;
                    const live = snapshot.workspaceBindings?.find(value => value.bindingKey === binding.bindingKey);
                    if (!live || !isDeepStrictEqual(live.target, request.target))
                        return;
                    const local = ports.getLeaderIdentity();
                    if (!local || local.slot !== binding.slot || (!isDeepStrictEqual(local.target, binding.target) &&
                        !isDeepStrictEqual(local.target, request.target)))
                        return;
                    if (action.mode === "apply" && !isDeepStrictEqual(local.target, request.target)) {
                        if (action.intent.phase !== "recipient-issued" || !action.isCurrent())
                            return;
                        deps.setCurrentLeaderIdentity({ target: request.target, slot: binding.slot, threadName: request.owner.threadName });
                    }
                    if (!action.isCurrent() || !isDeepStrictEqual(getRecipient(snapshot), recipient))
                        return;
                    const observed = ports.getLeaderIdentity();
                    if (!observed || observed.slot !== binding.slot)
                        return;
                    observation = { operationId: request.operationId, recipient, target: observed.target,
                        slot: binding.slot, ready: isDeepStrictEqual(observed.target, request.target) };
                });
                return observation;
            } }).catch(error => {
            deps.recordRuntimeEvent?.("telegram", error, { phase: "workspace-restore-recipient" });
            return undefined;
        });
        if (!ready || !current() || ready.routing)
            return "protected";
        const recipient = ready.readyRecipient ?? ready.recipient;
        const recipientCurrent = () => {
            if (!current() || !recipient || !hasCommittedTarget() || !isDeepStrictEqual(getRecipient(), recipient))
                return false;
            const observed = recipient.kind === "leader" ? ports.getLeaderIdentity() : ports.followerRegistry.get(recipient.instanceId);
            return observed?.slot === slot && isDeepStrictEqual(observed.target, request.target);
        };
        if (!recipient || !recipientCurrent())
            return "protected";
        if (deps.callApi) {
            try {
                await deps.callApi("editForumTopic", { chat_id: request.target.chatId, message_thread_id: request.target.threadId,
                    name: deps.getDisplayTitle?.(request.target) ?? ThreadNaming.getTelegramTopicTitleForThreadName(getRestoredThreadName(request.owner, slot), slot) });
            }
            catch (error) {
                deps.recordRuntimeEvent?.("telegram", error, { phase: "workspace-restore-title" });
            }
        }
        if (!recipientCurrent() || !store.issueRouting(ready, { ...authority, isCurrent: recipientCurrent }))
            return "protected";
        const recordAcceptance = (message, delivery) => {
            if (!recipientCurrent() || recipient.kind !== (delivery ? "follower" : "leader"))
                throw new Error("Workspace Restore acceptance authority changed.");
            const source = Updates.inspectTelegramDeferredSource(message);
            const follower = ports.followerRegistry.get(recipient.instanceId);
            const ownership = deps.getTargetOwnership?.(request.target);
            if (!source || source.journalBindingKey !== journalBindingKey || !request.source.updateIds.includes(source.updateId) ||
                delivery && (!follower || ownership?.instanceId !== recipient.instanceId || ownership.ownerGeneration !== recipient.generation ||
                    !ownership.recipientBindingKey || delivery.sourceUpdateId !== source.updateId ||
                    delivery.recipientBindingKey !== ownership.recipientBindingKey || delivery.deliveryId !== Bus.createTelegramBusFollowerDeliveryIdentity({
                    kind: "leader.forwardMessage", recipientBindingKey: ownership.recipientBindingKey, sourceUpdateId: source.updateId
                }).deliveryId) ||
                !recipientCurrent())
                throw new Error("Workspace Restore acceptance source or delivery changed.");
            const evidence = { ...source, recipient,
                ...(delivery ? { kind: "forwarded", deliveryId: delivery.deliveryId, recipientBindingKey: delivery.recipientBindingKey }
                    : { kind: "completed" }) };
            const expected = store.list().find(value => value.request.operationId === request.operationId);
            if (!expected || !isDeepStrictEqual(expected.request, request) || !expected.routing ||
                !recipientCurrent())
                throw new Error("Workspace Restore dispatch proof is unavailable.");
            let accepted;
            let failure;
            const acceptanceAuthority = { ...authority, isCurrent: recipientCurrent };
            try {
                accepted = store.recordSourceAcceptance(expected, evidence, acceptanceAuthority);
            }
            catch (error) {
                failure = { error };
            }
            if (!recipientCurrent())
                throw new Error("Workspace Restore acceptance authority changed.");
            // A lost rename reply observes only the exact full retained proof; it never resends the accepted message.
            if (!accepted) {
                const observed = store.list().find(value => value.request.operationId === request.operationId);
                if (observed && isDeepStrictEqual(observed.request, request) && isDeepStrictEqual(observed.executor, authority.executor) &&
                    observed.operatorUserId === operatorUserId && observed.routing?.acceptances?.some(value => isDeepStrictEqual(value, evidence)))
                    accepted = observed;
            }
            if (!accepted || !isDeepStrictEqual(accepted.request, request) || !isDeepStrictEqual(accepted.executor, authority.executor) ||
                accepted.operatorUserId !== operatorUserId || !accepted.routing?.acceptances?.some(value => isDeepStrictEqual(value, evidence)) ||
                !recipientCurrent())
                throw failure?.error ?? new Error("Workspace Restore acceptance was not published.");
            return { ...source, completionSha256: Threads.getTelegramWorkspaceRestoreSourceCompletionSha256(accepted, evidence) };
        };
        try {
            await input.dispatch(recipient, recipientCurrent, recordAcceptance, message => recordAcceptance(message));
        }
        catch (error) {
            deps.recordRuntimeEvent?.("telegram", error, { phase: "workspace-restore-dispatch" });
        }
        // Positive worker ACKs own settlement and cleanup under a subsequent admission.
        return "protected";
    } : undefined;
    const retryPendingRerouteCleanup = async (cleanup, assertExecutionCurrent) => {
        if (cleanup.kind === "unbound") {
            return closeReroutedUnboundTopic(cleanup.target, cleanup.messageId, assertExecutionCurrent, cleanup.temporaryThread);
        }
        if (cleanup.kind === "previous-leader") {
            return closePreviousLeaderThread(cleanup.target, assertExecutionCurrent);
        }
        return closeReplacedFollowerThread(cleanup.target, cleanup.instanceId, assertExecutionCurrent);
    };
    let dispatchReroutedCommandMessages;
    const dispatchPendingRerouteMessages = async (pending, messages, ctx) => {
        if (pending.dispatchKind === "command" && dispatchReroutedCommandMessages) {
            await dispatchReroutedCommandMessages(messages, ctx);
            // Every command chooser deferred its exact original, whether the tab came from Telegram or the bridge.
            // A queued command retains its separately reported receipt; this cannot settle accepted queue custody.
            for (const message of messages)
                Updates.reportTelegramUpdateCompleted(message);
            return;
        }
        await promptEnqueue(messages, ctx);
    };
    const finalizePendingReroute = async (rerouteId, pending, query, successMessage, assertExecutionCurrent = Updates.createTelegramUpdateExecutionFenceGuard(query)) => {
        assertExecutionCurrent();
        const dismissed = await dismissRerouteChooserMessage(query, assertExecutionCurrent);
        assertExecutionCurrent();
        if (dismissed) {
            removePendingReroute(rerouteId);
            await deps.answerCallbackQuery(query.id, successMessage);
            return;
        }
        pending.finalizeMessage = successMessage;
        await deps.answerCallbackQuery(query.id, `${successMessage} Chooser cleanup is still pending. Try again.`);
    };
    const isTemporaryReroute = (pending) => !!pending.temporaryThread || pending.temporaryMembership === true || isTemporaryTabTarget(pending.sourceTarget);
    const isTemporaryThreadForwardIssued = (pending) => {
        try {
            const entry = pending.temporaryThread ?? findCreatedTemporaryThread(pending.sourceTarget);
            const store = deps.getWorkspaceRestoreStore?.();
            if ((entry || pending.temporaryMembership) && !store)
                return true;
            const stored = entry && store?.listTemporaryThreads().find(value => value.token === entry.token);
            const updateIds = Updates.collectTelegramAdmissionSourceUpdateIds(pending.messages);
            return !!stored?.forwardedInputs?.some(input => input.updateIds.some(id => updateIds.includes(id)));
        }
        catch {
            return true;
        }
    };
    /** Publishes the durable one-time Forward fact for this chooser's exact group; false means nothing may be sent. */
    const issueTemporaryThreadForward = (pending, ctx) => {
        const entry = pending.temporaryThread ?? findCreatedTemporaryThread(pending.sourceTarget);
        const cap = captureTemporaryThreadAuthority(ctx);
        if (!entry || !cap)
            return false;
        const group = { journalBindingKey: entry.source.journalBindingKey, updateIds: Updates.collectTelegramAdmissionSourceUpdateIds(pending.messages) };
        try {
            const current = cap.store.listTemporaryThreads().find(value => value.token === entry.token);
            const adopted = current && cap.isCurrent() ? cap.adopt(current) : undefined;
            return !!adopted && cap.isCurrent() && !!cap.store.recordTemporaryThreadForwardIssued(adopted, group, cap.authority) && cap.isCurrent();
        }
        catch (error) {
            deps.recordRuntimeEvent?.("routing", error, { phase: "temporary-thread-forward-issue" });
            return false;
        }
    };
    const forwardPendingRerouteMessages = async (pending, instanceId, threadId, ctx, assertExecutionCurrent, recordForwardAcceptance) => {
        assertExecutionCurrent?.();
        const forwardMessage = deps.foreignOwnedUpdateForwarder?.forwardMessage;
        if (!forwardMessage || pending.foreignForwardIssued)
            return false;
        const target = { ...pending.sourceTarget, threadId };
        const liveOwnership = deps.getTargetOwnership?.(target);
        if (!liveOwnership || liveOwnership.instanceId !== instanceId ||
            !liveOwnership.ownerGeneration || !liveOwnership.recipientBindingKey)
            return false;
        const ownership = structuredClone(liveOwnership);
        const messages = cloneTelegramMessagesForThread(pending.messages, threadId);
        // Issuance is durable before the RPC: an unacknowledged temporary-tab Forward never becomes another dispatch grant,
        // even after restart. Any refusal or publication failure sends nothing and keeps the input held.
        if (!recordForwardAcceptance && isTemporaryReroute(pending) && !issueTemporaryThreadForward(pending, ctx)) {
            if (isTemporaryThreadForwardIssued(pending))
                pending.foreignForwardIssued = true;
            return false;
        }
        if (!recordForwardAcceptance && isTemporaryReroute(pending))
            pending.foreignForwardIssued = true;
        const outcomes = await Promise.allSettled(messages.map((message) => forwardMessage({
            message,
            ownership,
            ctx,
        })));
        assertExecutionCurrent?.();
        if (!isDeepStrictEqual(deps.getTargetOwnership?.(target), ownership))
            return false;
        pending.messages = pending.messages.filter((_, index) => {
            const outcome = outcomes[index];
            if (outcome?.status === "fulfilled" &&
                outcome.value.status === "accepted") {
                // Restore publishes acceptance before reporting completion; cleanup still waits for journal disposition.
                const expectedSource = recordForwardAcceptance?.(pending.messages[index], outcome.value.delivery);
                assertExecutionCurrent?.();
                Updates.reportTelegramUpdateCompleted(pending.messages[index], expectedSource);
                return false;
            }
            if (outcome?.status === "rejected") {
                deps.recordRuntimeEvent?.("bus", outcome.reason, {
                    phase: "reroute-foreign-forward",
                    instanceId,
                    threadId,
                    messageIndex: index,
                });
            }
            return true;
        });
        if (pending.messages.length === 0)
            pending.foreignForwardIssued = false;
        return pending.messages.length === 0;
    };
    const temporaryCleanupTimers = new Map();
    const cancelTemporaryThreadCleanup = (token) => {
        const scheduled = temporaryCleanupTimers.get(token);
        if (!scheduled)
            return;
        clearTimeout(scheduled.timer);
        temporaryCleanupTimers.delete(token);
        scheduled.settle();
    };
    /**
     * One cleanup attempt after the quiet period. Cancellation facts are only a precondition: fresh authority, proof,
     * absence of chooser and journal custody, and the ordinary protection checks must all hold again. The attempt is
     * durably issued once; a skipped or unknown removal keeps the entry protecting the tab across restart.
     */
    const runTemporaryThreadCleanup = async (token, cap, reconcileExpiry) => {
        const store = cap.store;
        if ((!deps.inspectRestoreSourceAbandonment && !deps.inspectRoutingInputGroupExpiry) || !deps.runWorkspaceOperation || !deps.callApi || !cap.isCurrent())
            return;
        await deps.runWorkspaceOperation({ operationId: `temporary-thread-cleanup-${randomBytes(16).toString("hex")}`,
            operationKind: "workspace.temporary-thread", scopes: [{ kind: "profile" }] }, async () => {
            if (!cap.isCurrent())
                return;
            let entry = store.listTemporaryThreads().find(value => value.token === token && value.phase === "created" && !!value.target);
            const target = entry?.target;
            if (!entry || !target || entry.cleanupIssued || entry.operatorUserId !== cap.operatorUserId ||
                entry.source.journalBindingKey !== cap.journalBindingKey)
                return;
            const owner = `telegram-owner:${cap.operatorUserId}`;
            if (reconcileExpiry && deps.inspectRoutingInputGroupExpiry) {
                for (const group of Threads.getTelegramTemporaryThreadInputs(entry)) {
                    if ([...(entry.cancelledInputs ?? []), ...(entry.completedInputs ?? [])].some(value => isDeepStrictEqual(value, group)))
                        continue;
                    const inspect = (id) => deps.inspectRoutingInputGroupExpiry(group)?.find(value => value.updateId === id);
                    if (!group.updateIds.every(id => inspect(id)?.operatorAuthorityId === owner))
                        continue;
                    entry = cap.adopt(entry);
                    if (!entry || !cap.isCurrent())
                        return;
                    let recorded, failure;
                    try {
                        recorded = store.recordTemporaryThreadInputExpiry(entry, group, cap.authority, inspect);
                    }
                    catch (error) {
                        failure = error;
                    }
                    if (!cap.isCurrent())
                        return;
                    const retained = store.listTemporaryThreads().find(value => value.token === token);
                    // A lost publication ACK may already have recorded the group or retired a bound tab's temporary frame.
                    if (!retained)
                        return;
                    if (!recorded && !retained.cancelledInputs?.some(value => isDeepStrictEqual(value, group)))
                        throw failure ?? new Error("Chooser expiry metadata was not confirmed.");
                    entry = retained;
                }
            }
            if (!Threads.isTelegramTemporaryThreadFullyResolved(entry) || hasPendingRerouteForTarget(target))
                return;
            const cancelled = entry.cancelledInputs ?? [];
            const cancelledCurrent = () => cancelled.every(input => {
                const expiry = deps.inspectRoutingInputGroupExpiry?.(input);
                return input.updateIds.every(updateId => {
                    const evidence = deps.inspectRestoreSourceAbandonment?.(updateId, input.journalBindingKey) ?? expiry?.find(value => value.updateId === updateId);
                    return evidence?.journalBindingKey === input.journalBindingKey && evidence.updateId === updateId && evidence.operatorAuthorityId === owner;
                });
            });
            if (!cancelledCurrent())
                return;
            entry = cap.adopt(entry);
            if (!entry || !cap.isCurrent() || isRerouteTargetProtected(target, undefined, entry))
                return;
            const issued = store.issueTemporaryThreadCleanup(entry, cap.authority);
            if (!issued || !cap.isCurrent())
                return;
            entry = issued.entry;
            const issuedEntry = entry;
            const assertCurrent = () => {
                if (!cap.isCurrent() || !cancelledCurrent() || !store.isTemporaryThreadCleanupCurrent(issuedEntry, cap.authority))
                    throw new Error("Temporary Thread cleanup authority or exact grant evidence changed.");
            };
            if (!await closeReroutedUnboundTopic(target, undefined, assertCurrent, entry) || !cap.isCurrent())
                return;
            store.retireTemporaryThread(entry, cap.authority);
        });
    };
    // Reuse the one timer per tab for body-free metadata retries. Future retry timers do not join settlement waits;
    // only their running attempt does. No retry is licensed once a destructive grant may have been issued.
    const queueTemporaryThreadCleanup = (token, cap, delay, reconcileExpiry, retry = false) => {
        cancelTemporaryThreadCleanup(token);
        let settle = () => undefined;
        const task = new Promise(resolve => { settle = resolve; });
        const timer = setTimeout(() => {
            if (temporaryCleanupTimers.get(token)?.timer !== timer)
                return;
            temporaryCleanupTimers.delete(token);
            if (retry)
                restoreSettlementTasks.add(task);
            runTemporaryThreadCleanup(token, cap, reconcileExpiry)
                .catch(error => {
                deps.recordRuntimeEvent?.("routing", error, { phase: "temporary-thread-cleanup" });
                if (!reconcileExpiry || !cap.isCurrent())
                    return;
                try {
                    const retained = cap.store.listTemporaryThreads().find(value => value.token === token);
                    if (retained && !retained.cleanupIssued && cap.isCurrent())
                        queueTemporaryThreadCleanup(token, cap, 60_000, true, true);
                }
                catch { /* Unknown grant state never licenses a retry. */ }
            })
                .finally(settle);
        }, delay);
        timer.unref?.();
        temporaryCleanupTimers.set(token, { timer, settle });
        if (!retry)
            restoreSettlementTasks.add(task);
        void task.finally(() => restoreSettlementTasks.delete(task));
    };
    /** Starts the quiet period for resolved groups or body-free expiry reconciliation; fresh input/authority revokes it. */
    const scheduleTemporaryThreadCleanup = (target, ctx, reconcileExpiry = false) => {
        const cap = captureTemporaryThreadAuthority(ctx);
        if (!cap || !deps.runWorkspaceOperation || target.threadId === undefined || hasPendingRerouteForTarget(target))
            return;
        let entry;
        try {
            entry = cap.store.listTemporaryThreads().find(value => value.phase === "created" && isDeepStrictEqual(value.target, target));
        }
        catch (error) {
            deps.recordRuntimeEvent?.("routing", error, { phase: "temporary-thread-cleanup-schedule" });
            return;
        }
        if (!entry || entry.cleanupIssued || entry.operatorUserId !== cap.operatorUserId || entry.source.journalBindingKey !== cap.journalBindingKey)
            return;
        if (!reconcileExpiry && !Threads.isTelegramTemporaryThreadFullyResolved(entry)) {
            try {
                reconcileExpiry = Threads.getTelegramTemporaryThreadInputs(entry).some(group => !!deps.inspectRoutingInputGroupExpiry?.(group));
            }
            catch (error) {
                deps.recordRuntimeEvent?.("routing", error, { phase: "temporary-thread-cleanup-schedule" });
                return;
            }
        }
        if (!reconcileExpiry && !Threads.isTelegramTemporaryThreadFullyResolved(entry))
            return;
        const requested = deps.temporaryThreadCleanupDelayMs;
        const delay = typeof requested === "number" && Number.isFinite(requested) ? Math.max(0, requested) : 1000;
        queueTemporaryThreadCleanup(entry.token, cap, delay, reconcileExpiry);
    };
    const recordCancelledTemporaryThreadInput = (pending, ctx, isCurrent) => {
        const store = deps.getWorkspaceRestoreStore?.(), target = pending.sourceTarget;
        if (!store || target.threadId === undefined)
            return !pending.temporaryThread;
        let entry = store.listTemporaryThreads().find(value => value.phase === "created" && isDeepStrictEqual(value.target, target));
        if (!entry)
            return !pending.temporaryThread;
        const cap = captureTemporaryThreadAuthority(ctx, isCurrent), inspect = deps.inspectRestoreSourceAbandonment;
        if (!cap || !inspect || cap.operatorUserId !== entry.operatorUserId || cap.journalBindingKey !== entry.source.journalBindingKey)
            return false;
        const { operatorUserId, journalBindingKey } = cap, current = cap.isCurrent;
        const input = { journalBindingKey, updateIds: Updates.collectTelegramAdmissionSourceUpdateIds(pending.messages) };
        if (!current())
            return false;
        entry = cap.adopt(entry);
        if (!entry || !current())
            return false;
        const recorded = store.recordTemporaryThreadInputCancellation(entry, input, cap.authority, updateId => inspect(updateId, journalBindingKey));
        if (!recorded || !current() || !recorded.cancelledInputs?.some(value => isDeepStrictEqual(value, input)))
            return false;
        const observed = store.listTemporaryThreads().find(value => isDeepStrictEqual(value, recorded));
        return !!observed && input.updateIds.every(updateId => {
            const evidence = inspect(updateId, journalBindingKey);
            return evidence?.journalBindingKey === journalBindingKey && evidence.updateId === updateId &&
                evidence.operatorAuthorityId === `telegram-owner:${operatorUserId}`;
        }) && current();
    };
    const retireCancellationChooser = async (id, pending, isCurrent) => {
        if (!pending.abandonment?.result || !isCurrent() || !deps.editInteractiveMessage || pending.chooserMessageId === undefined)
            return false;
        await deps.editInteractiveMessage(pending.sourceTarget.chatId, pending.chooserMessageId, "<b>⛔️ Routing cancelled.</b>", "html", { inline_keyboard: [] });
        if (!isCurrent() || pendingUnboundReroutes.get(id) !== pending)
            return false;
        removePendingReroute(id);
        return true;
    };
    const isReviewText = (message) => {
        if (!message || message.chat?.type !== "private" || !Number.isSafeInteger(message.chat.id) ||
            !Number.isSafeInteger(message.from?.id) || message.from?.is_bot !== false ||
            !Number.isSafeInteger(message.message_id) || message.message_id <= 0 ||
            !Number.isSafeInteger(message.message_thread_id) || message.message_thread_id <= 0 ||
            typeof message.text !== "string" || !message.text.trim() || message.text.trim().startsWith("/"))
            return false;
        const fields = message;
        return ["media_group_id", "business_connection_id", "photo", "video", "audio", "voice", "document", "animation", "sticker", "contact", "location", "venue", "poll", "dice", "story"].every(key => fields[key] === undefined);
    };
    const reviewMessage = (entry) => {
        const message = entry.update.message;
        if (entry.state !== "pending" || entry.preApprovalExcluded || entry.inputClaim || entry.inputProvenance ||
            entry.queueOwner || entry.queueReceiptId || entry.queueHandoff || entry.failure ||
            Object.keys(entry.update).some(key => key !== "update_id" && key !== "message") ||
            !isReviewText(message) || Reflect.has(message, "pi_telegram_source_update_id"))
            return undefined;
        return message;
    };
    const needsHistoricalReview = (message) => {
        if (!deps.threadStore)
            throw new Error("Historical routing requires a current Thread snapshot.");
        if (deps.threadStore.getBotState().threadMode === "disabled")
            return true;
        return !getTelegramRoutableThreadRecords(deps.threadStore.list(), deps.getLiveThreadTargets?.()).some(record => record.target.chatId === message.chat.id && record.target.threadId === message.message_thread_id);
    };
    /** New-world restart: forget previous-instance Restore/temporary state, then one silent delete per positively disposable tab. */
    const forgetPreviousWorld = async (input, captureTransport) => {
        const result = { forgotten: 0, deleted: 0 };
        const { ctx, signal, journalBindingKey, isCurrent: preparedCurrent } = input;
        const transport = captureTransport ? captureTransport(ctx) : () => true;
        const cap = transport && captureTemporaryThreadAuthority(ctx, () => !signal.aborted && preparedCurrent() && transport() &&
            deps.hasWorkspaceRestoreAuthority?.() === true);
        const threads = deps.threadStore;
        if (!cap?.isCurrent() || cap.journalBindingKey !== journalBindingKey || !threads || !deps.runWorkspaceOperation)
            return result;
        try {
            await deps.runWorkspaceOperation({ operationId: `new-world-${randomBytes(16).toString("hex")}`,
                operationKind: "workspace.forget-previous-world", scopes: [{ kind: "profile" }] }, async () => {
                if (!cap.isCurrent())
                    return;
                await threads.load();
                if (!cap.isCurrent())
                    return;
                const self = cap.authority.executor.instanceId;
                const entries = cap.store.listTemporaryThreads();
                const preserved = entries.filter(entry => entry.operatorUserId === cap.operatorUserId && entry.phase === "created" &&
                    cap.store.inspectTemporaryThreadTarget(entry, cap.authority)?.kind === "temporary" &&
                    Threads.getTelegramTemporaryThreadInputs(entry).some(source => source.journalBindingKey === journalBindingKey &&
                        source.updateIds.some(id => input.routingSourceIds?.includes(id)))).map(entry => entry.token);
                const disposable = entries.flatMap(entry => entry.operatorUserId === cap.operatorUserId && !preserved.includes(entry.token) &&
                    entry.executor.instanceId !== self && entry.phase === "created" && entry.target &&
                    cap.store.inspectTemporaryThreadTarget(entry, cap.authority)?.kind === "temporary" ? [entry] : []);
                const forgotten = cap.store.forgetPreviousWorld(cap.authority, preserved);
                if (!forgotten)
                    return;
                result.forgotten = forgotten.operations.length + forgotten.temporaryThreads.length;
                for (const entry of disposable) {
                    const target = entry.target;
                    if (!cap.isCurrent() || !deps.callApi)
                        break;
                    await threads.load();
                    const same = (value) => value?.chatId === target.chatId && value.threadId === target.threadId;
                    if (!cap.isCurrent() || threads.listWorkspaceBindings().some(value => same(value.target)) ||
                        threads.list().some(record => record.status === "active" && same(record.target)) ||
                        cap.store.listTemporaryThreads().some(entry => same(entry.target)) ||
                        cap.store.list().some(({ request }) => same(request.target) || same(request.binding.target)))
                        continue;
                    try {
                        const required = [...new Set([cap.journalBindingKey, entry.source.journalBindingKey,
                                ...Threads.getTelegramTemporaryThreadInputs(entry).map(input => input.journalBindingKey)])];
                        if (deps.inspectTemporaryThreadSources?.(target, required)?.length !== 0 || !cap.isCurrent())
                            continue;
                        // Exactly one attempt; the entry is already forgotten, so failure or silence leaves the tab without retry.
                        if (await deps.callApi("deleteForumTopic", { chat_id: target.chatId, message_thread_id: target.threadId }, { maxAttempts: 1, retrySafety: "non-idempotent" }) === true)
                            result.deleted++;
                    }
                    catch (error) {
                        deps.recordRuntimeEvent?.("routing", error, { phase: "new-world-tab-delete" });
                    }
                }
                // Same-instance session replacement may have lost a metadata-only timer after the source was already spent.
                // Reconstruct from retained tab/source proof under fresh authority, never from a prompt body or an issued delete.
                for (const entry of cap.store.listTemporaryThreads()) {
                    if (!cap.isCurrent())
                        break;
                    if (entry.phase !== "created" || !entry.target || entry.cleanupIssued || entry.operatorUserId !== cap.operatorUserId ||
                        entry.source.journalBindingKey !== journalBindingKey)
                        continue;
                    if (Threads.getTelegramTemporaryThreadInputs(entry).some(group => !!deps.inspectRoutingInputGroupExpiry?.(group)))
                        scheduleTemporaryThreadCleanup(entry.target, ctx, true);
                }
            });
        }
        catch (error) {
            deps.recordRuntimeEvent?.("routing", error, { phase: "new-world-forget" });
        }
        return result;
    };
    /** Historical temporary sources await the cold policy; warm bound/unknown sources never fall into ordinary dispatch. */
    const isRecordedTemporaryThreadInputHeld = (entry, ctx, signal, historical = false) => {
        const store = deps.getWorkspaceRestoreStore?.(), binding = deps.getAdmissionJournalBinding?.();
        if (!store || !binding)
            return false;
        const members = store.listTemporaryThreads().filter(temporary => Threads.getTelegramTemporaryThreadInputs(temporary).some(input => input.journalBindingKey === binding && input.updateIds.includes(entry.updateId)));
        const cap = captureTemporaryThreadAuthority(ctx, () => !signal.aborted);
        return members.some(temporary => {
            if (!cap?.isCurrent())
                return true;
            const observation = store.inspectTemporaryThreadTarget(temporary, cap.authority);
            return historical || !observation || observation.kind !== "temporary";
        });
    };
    const shouldHoldPendingInput = async (entry, ctx, signal) => {
        const binding = deps.getAdmissionJournalBinding?.();
        const current = () => !signal.aborted && deps.isContextActive?.(ctx) === true && deps.getAdmissionJournalBinding?.() === binding;
        if (!current())
            throw new Error("Temporary Thread source generation ended.");
        const held = isRecordedTemporaryThreadInputHeld(entry, ctx, signal);
        if (!current())
            throw new Error("Temporary Thread source authority changed.");
        return held;
    };
    const shouldReviewHistoricalInput = async (entry, ctx, signal) => {
        if (signal.aborted)
            throw new Error("Historical routing generation ended.");
        const message = entry.update.message;
        const unsupported = !reviewMessage(entry);
        const retainable = unsupported && entry.state === "pending" && !entry.preApprovalExcluded && !entry.inputClaim && !entry.inputProvenance &&
            !entry.queueOwner && !entry.queueReceiptId && !entry.queueHandoff && !entry.failure &&
            Object.keys(entry.update).every(key => key === "update_id" || key === "message") &&
            message?.chat?.type === "private" && Number.isSafeInteger(message.chat.id) &&
            Number.isSafeInteger(message.from?.id) && message.from?.is_bot === false &&
            Number.isSafeInteger(message.message_id) && message.message_id > 0 &&
            Number.isSafeInteger(message.message_thread_id) && message.message_thread_id > 0 &&
            !Reflect.has(message, "pi_telegram_source_update_id") && !Reflect.has(message, "business_connection_id");
        const temporary = isRecordedTemporaryThreadInputHeld(entry, ctx, signal, true);
        if (!retainable && temporary)
            return true;
        if (!retainable && unsupported)
            return false;
        if (signal.aborted)
            throw new Error("Historical routing generation ended.");
        const epoch = deps.getCurrentLeaderEpoch?.();
        const binding = deps.getAdmissionJournalBinding?.();
        const current = () => !signal.aborted && deps.isContextActive?.(ctx) === true &&
            epoch !== undefined && deps.getCurrentLeaderEpoch?.() === epoch && !!binding && deps.getAdmissionJournalBinding?.() === binding;
        if (!current() || !deps.threadStore)
            throw new Error("Historical routing authority is unavailable.");
        await deps.threadStore.load();
        if (!current())
            throw new Error("Historical routing authority changed.");
        // Raw unsupported startup originals stay protected even when a bound target outlives forgotten Restore membership.
        // Holding protects evidence, not sender authorization; owner-bearing sources keep their existing paths.
        return retainable ? "retain" : needsHistoricalReview(message);
    };
    const recoveryPreview = (source, owner, chatId, binding) => {
        const entry = source.original;
        const message = reviewMessage(entry);
        if (!message || message.chat.id !== chatId || message.from.id !== owner)
            return undefined;
        for (const pending of pendingUnboundReroutes.values()) {
            if (pending.abandonment?.journalBindingKey === binding &&
                Updates.collectTelegramAdmissionSourceUpdateIds(pending.messages).includes(entry.updateId) &&
                // Revoked carriers describe a past attempt, not a live selection veto.
                // Current claim/entry authority still comes from the worker and journal CAS.
                pending.messages.some(value => Updates.getTelegramUpdateExecutionFence(value)?.signal.aborted !== true) &&
                (pending.selectionAttempted || pending.destinationSelected || pending.dispatching || pending.foreignForwardIssued || pending.foreignRetry || pending.cleanup ||
                    pending.abandonment.running))
                return undefined;
        }
        const text = Array.from(message.text.replace(/\s+/gu, " ").trim());
        return escapeHtml(text.slice(0, 80).join("")) + (text.length > 80 ? "…" : "");
    };
    const expireRoutingInput = async (source, ctx, signal) => {
        const operator = deps.configStore.getAllowedUserId(), epoch = deps.getCurrentLeaderEpoch?.();
        const message = source.original.update.message;
        const runtimeCurrent = () => !signal.aborted && operator !== undefined && epoch !== undefined &&
            deps.isContextActive?.(ctx) === true && deps.configStore.getAllowedUserId() === operator &&
            deps.getCurrentLeaderEpoch?.() === epoch && deps.getAdmissionJournalBinding?.() === source.journalBindingKey &&
            source.original.routingInput?.operatorUserId === operator && message?.from?.id === operator && message.from.is_bot !== true && message.chat?.type === "private";
        const current = () => runtimeCurrent() && source.isCurrent();
        if (!current() || !deps.runWorkspaceOperation)
            return;
        await deps.runWorkspaceOperation({ operationId: `routing-expiry-${randomBytes(16).toString("hex")}`,
            operationKind: "workspace.expire-unbound-routing", scopes: [{ kind: "profile" }] }, async () => {
            if (!current())
                return;
            const restores = deps.getWorkspaceRestoreStore?.();
            if (deps.getWorkspaceRestoreStore && !restores)
                return;
            const temporary = restores?.listTemporaryThreads().find(entry => Threads.getTelegramTemporaryThreadInputs(entry).some(input => input.journalBindingKey === source.journalBindingKey && input.updateIds.includes(source.original.updateId)));
            const result = source.expire();
            if (!result)
                return;
            // Expiry revokes the donor carrier; old controls and late ACKs cannot issue another delivery. No body is archived.
            for (const [id, pending] of pendingUnboundReroutes) {
                if (!Updates.collectTelegramAdmissionSourceUpdateIds(pending.messages).includes(source.original.updateId))
                    continue;
                removePendingReroute(id);
                if (signal.aborted || deps.isContextActive?.(ctx) !== true || deps.configStore.getAllowedUserId() !== operator ||
                    deps.getCurrentLeaderEpoch?.() !== epoch || deps.getAdmissionJournalBinding?.() !== source.journalBindingKey)
                    continue;
                if (deps.editInteractiveMessage && pending.chooserMessageId !== undefined) {
                    try {
                        await deps.editInteractiveMessage(pending.sourceTarget.chatId, pending.chooserMessageId, `<b>${TELEGRAM_ROUTING_CHOICE_EXPIRED}</b>`, "html", { inline_keyboard: [] });
                    }
                    catch (error) {
                        deps.recordRuntimeEvent?.("routing", error, { phase: "routing-input-expiry-view" });
                    }
                }
            }
            // The scheduled stage captures only context/epoch and a tab token. It reconstructs disposition from body-free proof,
            // never closes over this source or asks the worker to retain a terminal prompt while metadata publication fails.
            if (temporary?.target && runtimeCurrent())
                scheduleTemporaryThreadCleanup(temporary.target, ctx, true);
        });
    };
    const renderCancellationReview = async (view, isCurrent) => {
        if (!isCurrent())
            return;
        const data = (action) => `${TELEGRAM_PENDING_CANCELLATION_REVIEW_PREFIX}${view.id}:${action}`;
        const rows = [[{ text: "⬆️ Main menu", callback_data: "menu:back" }]];
        const lines = ["<b>☑️ Review pending cancellations:</b>", "",
            "Finish stopping routing retries to retain the original privately. No new delivery is started; previously accepted work may continue. No Telegram message or Thread is deleted."];
        if (!view.sources.length)
            lines.push("", "No supported pending cancellations on this page.");
        for (const [index, source] of view.sources.entries()) {
            lines.push("", `<b>${index + 1}. ${source.result ? "⛔️ Routing cancelled." : "Protected input"}</b>`, source.preview);
            if (!source.result)
                rows.push([{ text: `❌ Finish cancellation ${index + 1}`, callback_data: data(`retry:${index}`) }]);
        }
        if (view.unsupported)
            lines.push("", "Other protected inputs are not available in this view and remain untouched.");
        if (view.nextAfterUpdateId !== undefined)
            rows.push([{ text: "🟣 More", callback_data: data("more") }]);
        rows.push([{ text: "🔄 Refresh", callback_data: data("refresh") }]);
        await deps.editInteractiveMessage(view.target.chatId, view.messageId, lines.join("\n"), "html", { inline_keyboard: rows });
    };
    const handleCancellationReview = async (query, ctx) => {
        const matches = (view) => query.message?.message_id === view.messageId &&
            query.message.chat.id === view.target.chatId &&
            (query.message.message_thread_id === undefined || query.message.message_thread_id === view.target.threadId);
        if (query.data?.startsWith(TELEGRAM_RETIRED_HISTORICAL_REVIEW_PREFIX)) {
            await deps.answerCallbackQuery(query.id, "🚫 This control is no longer available.");
            return true;
        }
        const ownsReview = query.data?.startsWith(TELEGRAM_PENDING_CANCELLATION_REVIEW_PREFIX) === true;
        const prefix = TELEGRAM_PENDING_CANCELLATION_REVIEW_PREFIX;
        if (!ownsReview && cancellationReview && matches(cancellationReview)) {
            if (cancellationReview.running && cancellationReview.isCurrent()) {
                await deps.answerCallbackQuery(query.id, "⏳ Cancellation is in progress.");
                return true;
            }
            cancellationReview = undefined;
            return false;
        }
        if (!ownsReview)
            return false;
        const unavailable = async () => { await deps.answerCallbackQuery(query.id, "🚫 Cancellation review expired or unavailable. Open Status again."); return true; };
        const execution = Updates.getTelegramUpdateExecutionFence(query);
        const chatId = query.message?.chat.id;
        const messageId = query.message?.message_id;
        if (execution?.isCurrent() !== true || deps.isContextActive?.(ctx) !== true ||
            !deps.editInteractiveMessage || !deps.runWorkspaceOperation || query.message?.chat.type !== "private" ||
            typeof chatId !== "number" || !Number.isSafeInteger(chatId) ||
            typeof messageId !== "number" || !Number.isSafeInteger(messageId) || messageId <= 0)
            return unavailable();
        const owner = deps.configStore.getAllowedUserId();
        const journalBindingKey = deps.getAdmissionJournalBinding?.();
        const epoch = deps.getCurrentLeaderEpoch?.();
        if (owner === undefined || query.from.id !== owner || query.from.is_bot || !journalBindingKey || epoch === undefined)
            return unavailable();
        const current = () => execution.isCurrent() && deps.isContextActive?.(ctx) === true &&
            deps.configStore.getAllowedUserId() === owner && deps.getAdmissionJournalBinding?.() === journalBindingKey &&
            deps.getCurrentLeaderEpoch?.() === epoch;
        const parsed = query.data.slice(prefix.length).match(/^([a-z0-9]+):(retry:(\d+)|refresh|more)$/);
        const prior = cancellationReview;
        const open = query.data === `${prefix}open`;
        if (!open && (!parsed || !prior || parsed[1] !== prior.id || !matches(prior) || !prior.isCurrent()))
            return unavailable();
        if (prior?.running && prior.isCurrent()) {
            await deps.answerCallbackQuery(query.id, "⏳ Cancellation is in progress.");
            return true;
        }
        let view = prior;
        let selected;
        try {
            if (open || parsed?.[2] === "refresh" || parsed?.[2] === "more") {
                if (parsed?.[2] === "more" && prior?.nextAfterUpdateId === undefined)
                    return unavailable();
                const next = { id: randomBytes(16).toString("hex"),
                    target: { chatId, ...(query.message.message_thread_id !== undefined ? { threadId: query.message.message_thread_id } : {}) },
                    messageId, ownerUserId: owner, journalBindingKey, sources: [], unsupported: false,
                    isCurrent: () => cancellationReview === next && current() };
                cancellationReview = view = next;
                const page = Updates.inspectTelegramAbandoningUpdates(query, { journalBindingKey, isCurrent: next.isCurrent,
                    ...(parsed?.[2] === "more" ? { afterUpdateId: prior.nextAfterUpdateId } : {}) });
                if (!page) {
                    if (cancellationReview === next)
                        cancellationReview = undefined;
                    return unavailable();
                }
                for (const source of page.sources) {
                    const preview = recoveryPreview(source, owner, next.target.chatId, journalBindingKey);
                    if (preview === undefined) {
                        next.unsupported = true;
                        continue;
                    }
                    if (next.sources.length === 5) {
                        next.nextAfterUpdateId = next.sources.at(-1).original.updateId;
                        break;
                    }
                    next.sources.push({ ...source, preview });
                }
                next.nextAfterUpdateId ??= page.nextAfterUpdateId;
                next.running = true;
                await renderCancellationReview(next, next.isCurrent);
                if (next.isCurrent())
                    await deps.answerCallbackQuery(query.id);
            }
            else {
                if (!view || !parsed?.[3] || !parsed[2].startsWith("retry:"))
                    return unavailable();
                selected = view.sources[Number(parsed[3])];
                if (!selected)
                    return unavailable();
                const activeView = view;
                const source = selected;
                const isCurrent = () => current() && activeView.isCurrent() &&
                    recoveryPreview(source, owner, activeView.target.chatId, journalBindingKey) !== undefined;
                view.running = true;
                await deps.runWorkspaceOperation({ operationId: `workspace-cancellation-recovery:${query.id}`,
                    operationKind: "workspace.recover-unbound-cancellation", scopes: [{ kind: "profile" }] }, async () => {
                    if (!isCurrent())
                        return;
                    source.result ??= source.retry({ operatorAuthorityId: `telegram-owner:${owner}`, isCurrent });
                    if (!source.result) {
                        await unavailable();
                        return;
                    }
                    for (const [id, pending] of pendingUnboundReroutes) {
                        if (pending.abandonment?.ownerUserId !== owner || pending.abandonment.journalBindingKey !== journalBindingKey ||
                            !Updates.collectTelegramAdmissionSourceUpdateIds(pending.messages).includes(source.original.updateId))
                            continue;
                        pending.abandonment.result = source.result;
                        pending.abandonment.attempted = true;
                        try {
                            await retireCancellationChooser(id, pending, isCurrent);
                        }
                        catch (error) {
                            deps.recordRuntimeEvent?.("routing", error, { phase: "recovered-chooser-cleanup" });
                        }
                    }
                    await renderCancellationReview(activeView, isCurrent);
                    if (isCurrent())
                        await deps.answerCallbackQuery(query.id, "⛔️ Routing cancelled.");
                });
            }
        }
        catch (error) {
            deps.recordRuntimeEvent?.("routing", error, { phase: "unbound-cancellation-recovery" });
            if (current())
                await deps.answerCallbackQuery(query.id, selected?.result
                    ? "⚠️ Routing cancelled; view update failed. Retry the same button."
                    : "⚠️ Recovery incomplete; inputs stay protected. Check /telegram-status --debug.");
        }
        finally {
            if (view)
                view.running = false;
        }
        return true;
    };
    const handleUnboundRerouteCancelCallback = async (query, ctx) => {
        if (!query.data?.startsWith(TELEGRAM_UNBOUND_REROUTE_CANCEL_CALLBACK_PREFIX))
            return false;
        const rerouteId = query.data.match(/^reroutecancel:([a-z0-9]+)$/)?.[1];
        const pending = rerouteId ? pendingUnboundReroutes.get(rerouteId) : undefined;
        const cancellation = pending?.abandonment;
        const source = pending?.messages[0];
        const execution = Updates.getTelegramUpdateExecutionFence(query);
        const sourceExecution = Updates.getTelegramUpdateExecutionFence(source);
        const isAuthorityCurrent = () => !!cancellation && execution?.isCurrent() === true &&
            sourceExecution?.signal.aborted === false && deps.isContextActive?.(ctx) === true &&
            query.from.id === cancellation.ownerUserId && !query.from.is_bot &&
            deps.configStore.getAllowedUserId() === cancellation.ownerUserId &&
            deps.getAdmissionJournalBinding?.() === cancellation.journalBindingKey &&
            deps.getCurrentLeaderEpoch?.() === cancellation.leaderEpoch;
        const isCurrent = () => isAuthorityCurrent() && !!pending && !!cancellation && !!source &&
            pendingUnboundReroutes.get(rerouteId) === pending && pending.abandonment === cancellation &&
            matchesRerouteChooser(pending, query) && query.message?.chat.type === "private" &&
            source.from?.id === cancellation.ownerUserId &&
            pending.messages.length === 1 &&
            !pending.destinationSelected && !pending.selectionAttempted && !pending.dispatching &&
            !pending.foreignForwardIssued && !pending.foreignRetry && !pending.cleanup && !pending.finalizeMessage;
        if (!isCurrent() || !deps.runWorkspaceOperation || !deps.editInteractiveMessage) {
            // A chooser from a previous process (new world) or an expired one simply reads as expired.
            await deps.answerCallbackQuery(query.id, pending ? "🚫 Message route is unavailable for cancellation." : TELEGRAM_ROUTING_CHOICE_EXPIRED);
            return true;
        }
        if (cancellation.running) {
            await deps.answerCallbackQuery(query.id, "⏳ Cancellation is already in progress.");
            return true;
        }
        cancellation.running = true;
        try {
            await deps.runWorkspaceOperation({ operationId: `workspace-reroute-cancel:${query.id}`,
                operationKind: "workspace.cancel-unbound-routing", scopes: [{ kind: "profile" }] }, async () => {
                if (!isCurrent())
                    return;
                if (!cancellation.result) {
                    const wasAttempted = cancellation.attempted;
                    cancellation.attempted = true;
                    const result = Updates.abandonTelegramDeferredUpdate(source, {
                        operatorAuthorityId: `telegram-owner:${cancellation.ownerUserId}`, isCurrent,
                    });
                    if (!result) {
                        cancellation.attempted = wasAttempted;
                        await deps.answerCallbackQuery(query.id, "🚫 Message routing cannot be cancelled now.");
                        return;
                    }
                    cancellation.result = result;
                }
                if (!isCurrent())
                    return;
                if (!recordCancelledTemporaryThreadInput(pending, ctx, isCurrent)) {
                    if (isAuthorityCurrent())
                        await deps.answerCallbackQuery(query.id, "⚠️ Routing cancelled; Thread cleanup remains protected. Retry Cancel routing.");
                    return;
                }
                if (await retireCancellationChooser(rerouteId, pending, isCurrent) && isAuthorityCurrent()) {
                    scheduleTemporaryThreadCleanup(pending.sourceTarget, ctx);
                    await deps.answerCallbackQuery(query.id, "⛔️ Routing cancelled.");
                }
            });
        }
        catch (error) {
            deps.recordRuntimeEvent?.("routing", error, { phase: "unbound-cancellation", rerouteId });
            if (isCurrent())
                await deps.answerCallbackQuery(query.id, cancellation.result ? "⚠️ Routing cancelled; chooser update failed. Retry Cancel routing." :
                    "⚠️ Cancellation incomplete. Retry Cancel routing.");
        }
        finally {
            cancellation.running = false;
        }
        return true;
    };
    const withPendingRerouteSelection = async (rerouteId, query, operation) => {
        Updates.assertTelegramUpdateExecutionCurrent(query);
        const pending = pendingUnboundReroutes.get(rerouteId);
        if (pending?.foreignForwardIssued && matchesRerouteChooser(pending, query)) {
            await deps.answerCallbackQuery(query.id, "🚫 Forward is pending; this button will not resend uncertain input.");
            return true;
        }
        if (pending && matchesRerouteChooser(pending, query) &&
            (pending.abandonment?.attempted || pending.abandonment?.running || pending.messages.some(message => Updates.getTelegramUpdateExecutionFence(message)?.isCurrent() === false))) {
            await deps.answerCallbackQuery(query.id, TELEGRAM_ROUTING_CHOICE_EXPIRED);
            return true;
        }
        const releases = [];
        try {
            if (pending && matchesRerouteChooser(pending, query)) {
                try {
                    for (const message of pending.messages)
                        releases.push(Updates.acquireTelegramUpdateRouting(message));
                }
                catch (error) {
                    deps.recordRuntimeEvent?.("routing", error, { phase: "routing-input-selection" });
                    await deps.answerCallbackQuery(query.id, "🚫 Message route expired or unavailable.");
                    return true;
                }
            }
            return await operation();
        }
        finally {
            for (const release of releases)
                release();
        }
    };
    const executeUnboundRerouteRestoreMenuCallback = async (query, _ctx) => {
        const parsed = parseTelegramUnboundRerouteRestoreMenuCallbackData(query.data);
        if (!parsed)
            return false;
        const assertExecutionCurrent = Updates.createTelegramUpdateExecutionFenceGuard(query);
        assertExecutionCurrent();
        const chatId = query.message?.chat?.id;
        const messageId = query.message?.message_id;
        const pending = pendingUnboundReroutes.get(parsed.rerouteId);
        if (typeof chatId !== "number" ||
            typeof messageId !== "number" ||
            !deps.threadStore ||
            !pending ||
            !matchesRerouteChooser(pending, query) ||
            (parsed.root && pending.rootChooserText === undefined) ||
            expirePendingCommand(parsed.rerouteId, pending)) {
            await deps.answerCallbackQuery(query.id, TELEGRAM_ROUTING_CHOICE_EXPIRED);
            return true;
        }
        if (parsed.restore && pending.sourceTarget.threadId === undefined) {
            await deps.answerCallbackQuery(query.id, "Restore needs a destination thread. Send a plain message in a new Telegram thread first.");
            return true;
        }
        await deps.threadStore.load();
        assertExecutionCurrent();
        const activeRecords = getTelegramRoutableThreadRecords(deps.threadStore.list(), deps.getLiveThreadTargets?.());
        const replyMarkup = parsed.root
            ? buildTelegramUnboundRerouteChooserMarkup(parsed.rerouteId, activeRecords, {
                canRestore: pending.sourceTarget.threadId !== undefined, canCancel: !!pending.abandonment,
                getDisplayTitle: deps.getDisplayTitle,
            })
            : parsed.restore
                ? buildTelegramUnboundRerouteRestoreChooserMarkup(parsed.rerouteId, activeRecords, deps.getDisplayTitle)
                : { inline_keyboard: activeRecords.map(record => [{
                            text: `↪️ ${getTelegramThreadRecordLabel(record, deps.getDisplayTitle)}`,
                            callback_data: formatTelegramUnboundRerouteCallbackData(parsed.rerouteId, record.target.threadId),
                        }]) };
        const chooserText = parsed.root
            ? pending.rootChooserText
            : parsed.restore ? formatTelegramUnboundRerouteRestoreChooserText()
                : "<b>🧵 Reroute:</b>\n\nChoose the thread that should handle this input:";
        if (!parsed.root)
            replyMarkup.inline_keyboard.unshift([{
                    text: "⬆️ Back", callback_data: `${TELEGRAM_UNBOUND_REROUTE_ROOT_CALLBACK_PREFIX}${parsed.rerouteId}`,
                }]);
        if (deps.editInteractiveMessage) {
            await deps.editInteractiveMessage(chatId, messageId, chooserText, "html", replyMarkup);
        }
        else if (deps.sendInteractiveMessage) {
            const chooserId = await deps.sendInteractiveMessage(chatId, chooserText, "html", replyMarkup, { target: pending.sourceTarget, replyToMessageId: messageId });
            assertExecutionCurrent();
            rememberRerouteChooser(parsed.rerouteId, chooserId);
        }
        assertExecutionCurrent();
        await deps.answerCallbackQuery(query.id, parsed.root ? "Choose routing mode." : parsed.restore ? "Choose instance to restore." : "Choose target thread.");
        return true;
    };
    const executeUnboundRerouteCallbackOperation = async (query, ctx) => {
        const parsed = parseTelegramUnboundRerouteCallbackData(query.data);
        if (!parsed)
            return false;
        const assertExecutionCurrent = Updates.createTelegramUpdateExecutionFenceGuard(query);
        assertExecutionCurrent();
        const chatId = query.message?.chat?.id;
        const pending = pendingUnboundReroutes.get(parsed.rerouteId);
        if (typeof chatId !== "number" || !deps.threadStore || !pending ||
            !matchesRerouteChooser(pending, query) || expirePendingCommand(parsed.rerouteId, pending)) {
            await deps.answerCallbackQuery(query.id, TELEGRAM_ROUTING_CHOICE_EXPIRED);
            return true;
        }
        await deps.threadStore.load();
        assertExecutionCurrent();
        if (pendingUnboundReroutes.get(parsed.rerouteId) !== pending ||
            expirePendingCommand(parsed.rerouteId, pending)) {
            await deps.answerCallbackQuery(query.id, TELEGRAM_ROUTING_CHOICE_EXPIRED);
            return true;
        }
        if (pending.finalizeMessage) {
            await finalizePendingReroute(parsed.rerouteId, pending, query, pending.finalizeMessage, assertExecutionCurrent);
            return true;
        }
        if (pending.workspaceRestore && (!restoreWorkspace || !parsed.useNewSlot ||
            pending.workspaceRestore.record.target.threadId !== parsed.threadId)) {
            await deps.answerCallbackQuery(query.id, "🚫 Restore is already selected. Use the original Restore choice.");
            return true;
        }
        if (pending.foreignRetry) {
            const retry = pending.foreignRetry;
            const allForwarded = await forwardPendingRerouteMessages(pending, retry.instanceId, retry.threadId, ctx, assertExecutionCurrent);
            if (!allForwarded) {
                await deps.answerCallbackQuery(query.id, pending.foreignForwardIssued ? "🚫 Forward is pending; this button will not resend uncertain input." :
                    "Target thread is unavailable; retrying will send only remaining messages.");
                return true;
            }
            pending.foreignRetry = undefined;
            if (retry.cleanup)
                pending.cleanup = retry.cleanup;
        }
        if (pending.cleanup) {
            const cleanupComplete = await retryPendingRerouteCleanup(pending.cleanup, assertExecutionCurrent);
            if (!cleanupComplete) {
                await deps.answerCallbackQuery(query.id, "Message routed, but thread cleanup is still pending. Try again.");
                return true;
            }
            pending.cleanup = undefined;
            await finalizePendingReroute(parsed.rerouteId, pending, query, "Thread cleanup completed.", assertExecutionCurrent);
            return true;
        }
        if (!pending.workspaceRestore && isTemporaryReroute(pending) && isTemporaryThreadForwardIssued(pending)) {
            await deps.answerCallbackQuery(query.id, "🚫 Forward is pending; this button will not resend uncertain input.");
            return true;
        }
        const record = pending.workspaceRestore?.record ?? getTelegramRoutableThreadRecords(deps.threadStore.list(), deps.getLiveThreadTargets?.()).find((candidate) => candidate.target.chatId === chatId &&
            candidate.target.threadId === parsed.threadId);
        if (!record) {
            await deps.answerCallbackQuery(query.id, "Thread is not active yet.");
            return true;
        }
        const reroutedMessages = cloneTelegramMessagesForThread(pending.messages, parsed.threadId);
        const sourceTarget = typeof pending.sourceTarget.threadId === "number"
            ? { chatId, threadId: pending.sourceTarget.threadId }
            : undefined;
        const sourceMessageId = pending.chooserMessageId;
        if (parsed.useNewSlot && !sourceTarget) {
            await deps.answerCallbackQuery(query.id, "Restore needs a destination thread. Send a plain message in a new Telegram thread first.");
            return true;
        }
        if (parsed.useNewSlot && sourceTarget && record.target.chatId === sourceTarget.chatId &&
            record.target.threadId === sourceTarget.threadId) {
            await deps.answerCallbackQuery(query.id, "🚫 Selected thread is already the destination.");
            return true;
        }
        // A source's own temporary tab is its Restore destination; only other protection refuses it.
        if (parsed.useNewSlot && sourceTarget && !pending.workspaceRestore &&
            isRerouteTargetProtected(sourceTarget, undefined, pending.temporaryThread ?? findCreatedTemporaryThread(sourceTarget), pending)) {
            await deps.answerCallbackQuery(query.id, "🚫 Thread restore source is already owned.");
            return true;
        }
        // Restore has one controller. Without it or its negotiated authority, refuse before selection so the chooser keeps
        // its other routes and Cancel.
        if (parsed.useNewSlot && sourceTarget && !pending.workspaceRestore &&
            (!restoreWorkspace || deps.hasWorkspaceRestoreAuthority?.() !== true)) {
            await deps.answerCallbackQuery(query.id, "🚫 Thread restore is unavailable right now. The message stays pending; choose another route or cancel.");
            return true;
        }
        const currentInstanceId = deps.getCurrentInstanceId?.();
        const leaderProfileKey = getLeaderTopicProfileKey(ctx, currentInstanceId);
        const isCurrentLeaderRecord = isCurrentLeaderTopicRecord(record, leaderProfileKey, currentInstanceId);
        if (!parsed.useNewSlot && record.instanceId && record.instanceId !== currentInstanceId && !isCurrentLeaderRecord &&
            !deps.foreignOwnedUpdateForwarder?.forwardMessage) {
            if (pending.routingOperatorUserId === undefined) {
                pending.destinationSelected = true;
                pending.selectionAttempted = true;
                pending.pauseExpiry?.();
            }
            await deps.answerCallbackQuery(query.id, "Open that thread and resend the message there.");
            return true;
        }
        if (pending.routingOperatorUserId !== undefined && (pending.routingOperatorUserId !== query.from.id ||
            pending.routingOperatorUserId !== deps.configStore.getAllowedUserId())) {
            await deps.answerCallbackQuery(query.id, "🚫 Message route authority changed.");
            return true;
        }
        // Freeze only a validated actual destination, inside the same admission as its effect. Menu browsing,
        // unavailable recipients and refused Restore capability never turn a waiting source into a selected grant.
        try {
            const release = Updates.acquireTelegramUpdateRouting(pending.messages[0], true, Updates.collectTelegramAdmissionSourceUpdateIds(pending.messages));
            release();
        }
        catch (error) {
            deps.recordRuntimeEvent?.("routing", error, { phase: "routing-input-selection" });
            await deps.answerCallbackQuery(query.id, "🚫 Message route expired or unavailable.");
            return true;
        }
        pending.destinationSelected = true;
        pending.selectionAttempted = true;
        pending.pauseExpiry?.();
        if (parsed.useNewSlot && sourceTarget && restoreWorkspace) {
            const selection = pending.workspaceRestore ??= { operationId: `restore-${randomBytes(16).toString("hex")}`,
                record: structuredClone(record), messages: [...pending.messages] };
            let active = true;
            const ownerUserId = deps.configStore.getAllowedUserId();
            const leaderEpoch = deps.getCurrentLeaderEpoch?.();
            const admissionScope = deps.getAdmissionScope?.();
            const journalBinding = deps.getAdmissionJournalBinding?.();
            const selectionCurrent = () => {
                try {
                    assertExecutionCurrent();
                }
                catch {
                    return false;
                }
                return ownerUserId === query.from.id && deps.configStore.getAllowedUserId() === ownerUserId &&
                    deps.getCurrentLeaderEpoch?.() === leaderEpoch && deps.getAdmissionScope?.() === admissionScope &&
                    deps.getAdmissionJournalBinding?.() === journalBinding && deps.isContextActive?.(ctx) !== false &&
                    pendingUnboundReroutes.get(parsed.rerouteId) === pending && pending.workspaceRestore === selection;
            };
            const current = () => active && selectionCurrent();
            const assertRestoreCurrent = () => {
                assertExecutionCurrent();
                if (!current())
                    throw new Error("Stale Workspace Restore callback.");
            };
            try {
                await restoreWorkspace({ operationId: selection.operationId, record: structuredClone(selection.record),
                    target: { ...sourceTarget }, messages: [...selection.messages], ctx, isCurrent: current,
                    async dispatch(recipient, isRecipientCurrent, recordForwardAcceptance, recordLocalAcceptance) {
                        const assertRecipientCurrent = () => {
                            assertRestoreCurrent();
                            if (!isRecipientCurrent())
                                throw new Error("Workspace Restore recipient authority changed.");
                        };
                        assertRecipientCurrent();
                        if (recipient.kind === "leader") {
                            if (recipient.instanceId !== currentInstanceId)
                                return false;
                            const routed = cloneTelegramMessagesForThread(pending.messages, sourceTarget.threadId);
                            const messages = pending.dispatchKind === "command" ? routed.map((message, index) => Updates.bindTelegramUpdateCompletionAcceptance(message, () => recordLocalAcceptance(pending.messages[index]))) : routed;
                            await dispatchPendingRerouteMessages(pending, messages, ctx);
                            assertRecipientCurrent();
                            return true;
                        }
                        return forwardPendingRerouteMessages(pending, recipient.instanceId, sourceTarget.threadId, ctx, assertRecipientCurrent, recordForwardAcceptance);
                    },
                });
            }
            finally {
                active = false;
            }
            assertExecutionCurrent();
            if (!selectionCurrent())
                return true;
            await deps.answerCallbackQuery(query.id, "🚫 Restore is pending; this button will not resend uncertain input.");
            return true;
        }
        if (record.instanceId &&
            record.instanceId !== currentInstanceId &&
            !isCurrentLeaderRecord) {
            const allForwarded = await forwardPendingRerouteMessages(pending, record.instanceId, parsed.threadId, ctx, assertExecutionCurrent);
            if (!allForwarded) {
                await deps.answerCallbackQuery(query.id, pending.foreignForwardIssued ? "🚫 Forward is pending; this button will not resend uncertain input." :
                    "Target thread is unavailable; retrying will send only remaining messages.");
                return true;
            }
            // A temporary tab is removed only after every input in it resolves, never by one Forward.
            const cleanupComplete = isTemporaryTabTarget(sourceTarget) || await closeReroutedUnboundTopic(sourceTarget, sourceMessageId, assertExecutionCurrent, pending.temporaryThread);
            if (!cleanupComplete && sourceTarget) {
                pending.cleanup = {
                    kind: "unbound",
                    target: sourceTarget,
                    ...(typeof sourceMessageId === "number"
                        ? { messageId: sourceMessageId }
                        : {}),
                    ...(pending.temporaryThread ? { temporaryThread: pending.temporaryThread } : {}),
                };
                await deps.answerCallbackQuery(query.id, "Message routed, but thread cleanup is still pending. Try again.");
                return true;
            }
            await finalizePendingReroute(parsed.rerouteId, pending, query, "Message routed.");
            if (sourceTarget && isTemporaryTabTarget(sourceTarget))
                scheduleTemporaryThreadCleanup(sourceTarget, ctx);
            return true;
        }
        // Local handlers and queue admission consume the same one-time group grant as follower forwarding.
        if (isTemporaryReroute(pending) && !issueTemporaryThreadForward(pending, ctx)) {
            await deps.answerCallbackQuery(query.id, "🚫 Forward is pending; this button will not resend uncertain input.");
            return true;
        }
        await dispatchPendingRerouteMessages(pending, reroutedMessages, ctx);
        pending.messages = [];
        // A temporary tab is removed only after every input in it resolves, never by one Forward.
        const cleanupComplete = isTemporaryTabTarget(sourceTarget) || await closeReroutedUnboundTopic(sourceTarget, sourceMessageId, assertExecutionCurrent, pending.temporaryThread);
        if (!cleanupComplete && sourceTarget) {
            pending.cleanup = {
                kind: "unbound",
                target: sourceTarget,
                ...(typeof sourceMessageId === "number"
                    ? { messageId: sourceMessageId }
                    : {}),
                ...(pending.temporaryThread ? { temporaryThread: pending.temporaryThread } : {}),
            };
            await deps.answerCallbackQuery(query.id, "Message routed, but thread cleanup is still pending. Try again.");
            return true;
        }
        await finalizePendingReroute(parsed.rerouteId, pending, query, "Message routed.");
        if (sourceTarget && isTemporaryTabTarget(sourceTarget))
            scheduleTemporaryThreadCleanup(sourceTarget, ctx);
        return true;
    };
    const executeUnboundRerouteCallback = async (query, ctx) => {
        const parsed = parseTelegramUnboundRerouteCallbackData(query.data);
        if (!parsed)
            return false;
        return withPendingRerouteSelection(parsed.rerouteId, query, () => {
            if (!deps.runWorkspaceOperation)
                return executeUnboundRerouteCallbackOperation(query, ctx);
            return deps.runWorkspaceOperation({
                operationId: `workspace-reroute:${query.id}`,
                operationKind: "workspace.route-unbound-thread",
                scopes: [{ kind: "profile" }],
            }, () => executeUnboundRerouteCallbackOperation(query, ctx));
        });
    };
    const handleUnboundRerouteCallback = async (query, ctx) => {
        const parsed = parseTelegramUnboundRerouteCallbackData(query.data);
        const pending = parsed && pendingUnboundReroutes.get(parsed.rerouteId);
        if (!parsed || !pending || pending.dispatchKind !== "command" ||
            (pending.sourceTarget.threadId !== undefined && !pending.temporaryThread) || !matchesRerouteChooser(pending, query)) {
            return executeUnboundRerouteCallback(query, ctx);
        }
        if (pending.dispatching) {
            await deps.answerCallbackQuery(query.id, "Command routing is already in progress.");
            return true;
        }
        pending.dispatching = true;
        try {
            return await executeUnboundRerouteCallback(query, ctx);
        }
        finally {
            pending.dispatching = false;
            if (pendingUnboundReroutes.get(parsed.rerouteId) === pending &&
                pending.destinationSelected && pending.messages.length > 0 &&
                !pending.cleanup && !pending.finalizeMessage) {
                pending.destinationSelected = false;
                armPendingCommandExpiry(parsed.rerouteId, pending);
            }
        }
    };
    const callbackHandler = async (query, ctx) => {
        const assertExecutionCurrent = Updates.createTelegramUpdateExecutionFenceGuard(query);
        assertExecutionCurrent();
        if (await handleCancellationReview(query, ctx))
            return;
        if (await handleUnboundRerouteCancelCallback(query, ctx))
            return;
        const restore = parseTelegramUnboundRerouteRestoreMenuCallbackData(query.data);
        if (restore && await withPendingRerouteSelection(restore.rerouteId, query, () => executeUnboundRerouteRestoreMenuCallback(query, ctx)))
            return;
        if (await handleUnboundRerouteCallback(query, ctx))
            return;
        if (deps.buttonActionStore) {
            const handled = await OutboundHandlers.handleTelegramButtonCallbackQuery(query, ctx, {
                resolveAction: deps.buttonActionStore.resolve,
                answerCallbackQuery: deps.answerCallbackQuery,
                ...(deps.invokeBoundButtonAction
                    ? {
                        invokeBoundAction: (buttonQuery, action, context) => deps.invokeBoundButtonAction(action, buttonQuery, context),
                    }
                    : {}),
                editMessageReplyMarkup: deps.editMessageReplyMarkup
                    ? async (chatId, messageId, replyMarkup) => {
                        try {
                            await deps.editMessageReplyMarkup?.(chatId, messageId, replyMarkup);
                        }
                        catch (error) {
                            deps.recordRuntimeEvent?.("telegram", error, {
                                phase: "button-selection-mark",
                                chatId,
                                messageId,
                            });
                        }
                    }
                    : undefined,
                enqueueButtonPrompt: (buttonQuery, action, context) => {
                    const chatId = buttonQuery.message?.chat?.id;
                    const messageId = buttonQuery.message?.message_id;
                    if (typeof chatId !== "number" || typeof messageId !== "number")
                        return false;
                    const queueOrder = deps.bridgeRuntime.queue.allocateItemOrder();
                    const admissionReceipts = createAdmissionReceipts("prompt", [
                        buttonQuery,
                    ]);
                    const turn = {
                        ...OutboundHandlers.createTelegramButtonPromptTurn({
                            chatId,
                            target: typeof buttonQuery.message?.message_thread_id === "number"
                                ? {
                                    chatId,
                                    threadId: buttonQuery.message.message_thread_id,
                                }
                                : { chatId },
                            replyToMessageId: messageId,
                            queueOrder,
                            action,
                            telegramPrefix: Turns.createTelegramTurnPrefix({
                                thread: resolveTelegramThreadLabel({
                                    chat: { id: chatId },
                                    message_thread_id: buttonQuery.message?.message_thread_id,
                                }),
                            }),
                        }),
                        ...(admissionReceipts.length > 0 ? { admissionReceipts } : {}),
                    };
                    const result = Queue.appendTelegramPromptTurnOnce(deps.telegramQueueStore.getQueuedItems(), turn);
                    if (!result.appended) {
                        reportQueueAdmission([buttonQuery], admissionReceipts);
                        return false;
                    }
                    Updates.assertTelegramUpdateExecutionCurrent(buttonQuery);
                    deps.telegramQueueStore.setQueuedItems(result.items);
                    reportQueueAdmission([buttonQuery], admissionReceipts);
                    deps.updateStatus(context);
                    requestDispatchNextQueuedTelegramTurn(context);
                    return true;
                },
            });
            assertExecutionCurrent();
            if (handled)
                return;
        }
        if (query.data?.startsWith("thread-name:")) {
            const chatId = query.message?.chat?.id;
            const dialogMessageId = query.message?.message_id;
            if (typeof chatId !== "number" || typeof dialogMessageId !== "number") {
                await deps.answerCallbackQuery(query.id, "⌛ Rename dialog expired.");
                return;
            }
            const target = typeof query.message?.message_thread_id === "number"
                ? { chatId, threadId: query.message.message_thread_id }
                : { chatId };
            const action = query.data.slice("thread-name:".length);
            if (action !== "reset" && action !== "cancel") {
                await deps.answerCallbackQuery(query.id, "⌛ Rename dialog expired.");
                return;
            }
            const selected = threadNameDialog.select({
                scope: getThreadNameDialogScope(),
                target,
                dialogMessageId,
                action,
            });
            if (selected.kind === "expired") {
                await deps.answerCallbackQuery(query.id, "⌛ Rename dialog expired.");
                return;
            }
            if (selected.kind === "cancel") {
                await deps.editInteractiveMessage?.(chatId, dialogMessageId, "<b>✖ Rename cancelled.</b>", "html", { inline_keyboard: [] });
                await deps.answerCallbackQuery(query.id);
                return;
            }
            try {
                const result = await deps.resetCurrentThreadName?.(target);
                if (!result?.ok) {
                    throw new Error(result?.message ?? "Thread display name reset is unavailable.");
                }
                await deps.editInteractiveMessage?.(chatId, dialogMessageId, result.message
                    ? Commands.formatTelegramInformationHeading("✅", result.message)
                    : Commands.formatTelegramAutomaticThreadDisplayNameRestoredHeading(result.threadName ?? "automatic"), "html", { inline_keyboard: [] });
                await deps.answerCallbackQuery(query.id);
            }
            catch (error) {
                threadNameDialog.open({
                    scope: getThreadNameDialogScope(), target, dialogMessageId,
                });
                deps.recordRuntimeEvent?.("telegram-command", error, {
                    command: "name", phase: "reset",
                });
                await deps.answerCallbackQuery(query.id, "⚠️ Thread name reset failed.");
            }
            return;
        }
        const handledByNew = await Commands.handleTelegramNewConfirmationCallback(query, {
            ctx,
            answerCallbackQuery: deps.answerCallbackQuery,
            editInteractiveMessage: deps.editInteractiveMessage ?? (async () => { }),
            deleteMessage: deps.deleteMessage ?? (async () => { }),
            runNew: async (newCtx) => {
                await Commands.handleTelegramNewCommand({
                    isIdle: () => deps.isIdle(newCtx),
                    hasPendingMessages: () => deps.hasPendingMessages(newCtx),
                    hasActiveTelegramTurn: deps.activeTurnRuntime.has,
                    hasDispatchPending: deps.bridgeRuntime.lifecycle.hasDispatchPending,
                    hasQueuedTelegramItems: deps.telegramQueueStore.hasQueuedItems,
                    isCompactionInProgress: deps.bridgeRuntime.lifecycle.isCompactionInProgress,
                    requestNewSession: deps.requestNewSession
                        ? () => deps.requestNewSession(query)
                        : undefined,
                    sendTextReply: async (text) => {
                        const chatId = query.message?.chat?.id;
                        const messageId = query.message?.message_id;
                        if (typeof chatId !== "number" || typeof messageId !== "number")
                            return;
                        await deps.editInteractiveMessage?.(chatId, messageId, text, "html", { inline_keyboard: [] });
                    },
                    recordRuntimeEvent: deps.recordRuntimeEvent,
                });
            },
        });
        assertExecutionCurrent();
        if (handledByNew)
            return;
        const handledByCompact = await Commands.handleTelegramCompactConfirmationCallback(query, {
            ctx,
            answerCallbackQuery: deps.answerCallbackQuery,
            editInteractiveMessage: deps.editInteractiveMessage ?? (async () => { }),
            runCompact: async (compactCtx, chatId, replyToMessageId, target) => {
                await Commands.handleTelegramCompactCommand({
                    isIdle: () => deps.isIdle(compactCtx),
                    hasPendingMessages: () => deps.hasPendingMessages(compactCtx),
                    hasActiveTelegramTurn: deps.activeTurnRuntime.has,
                    hasDispatchPending: deps.bridgeRuntime.lifecycle.hasDispatchPending,
                    hasQueuedTelegramItems: deps.telegramQueueStore.hasQueuedItems,
                    isCompactionInProgress: deps.bridgeRuntime.lifecycle.isCompactionInProgress,
                    setCompactionInProgress: deps.bridgeRuntime.lifecycle.setCompactionInProgress,
                    updateStatus: () => deps.updateStatus(compactCtx),
                    dispatchNextQueuedTelegramTurn: () => deps.dispatchNextQueuedTelegramTurn(compactCtx),
                    requestDeferredDispatchNextQueuedTelegramTurn: deps.requestDeferredDispatchNextQueuedTelegramTurn
                        ? (dispatch) => deps.requestDeferredDispatchNextQueuedTelegramTurn?.(() => dispatch())
                        : undefined,
                    compact: (callbacks) => deps.compact(compactCtx, callbacks),
                    startTypingLoop: deps.startTypingLoop
                        ? () => deps.startTypingLoop?.(compactCtx, chatId, {
                            target,
                        })
                        : undefined,
                    stopTypingLoop: deps.stopTypingLoop,
                    sendTextReply: (text, options) => deps
                        .sendTextReply(chatId, replyToMessageId, text, {
                        target,
                        parseMode: options?.parseMode,
                    })
                        .then(() => { }),
                    suppressStartNotice: true,
                    recordRuntimeEvent: deps.recordRuntimeEvent,
                });
            },
        });
        assertExecutionCurrent();
        if (handledByCompact)
            return;
        const handledByQueue = await deps.queueMenuCallbackHandler(query, ctx);
        assertExecutionCurrent();
        if (handledByQueue)
            return;
        const handledBySettings = await deps.settingsMenuCallbackHandler?.(query, ctx);
        assertExecutionCurrent();
        if (handledBySettings)
            return;
        const callbackData = query.data;
        if (callbackData && !isTelegramOwnedCallbackData(callbackData)) {
            const chatId = query.message?.chat?.id;
            const messageId = query.message?.message_id;
            if (typeof chatId === "number" && typeof messageId === "number") {
                const queueOrder = deps.bridgeRuntime.queue.allocateItemOrder();
                const target = typeof query.message?.message_thread_id === "number"
                    ? { chatId, threadId: query.message.message_thread_id }
                    : { chatId };
                const admissionReceipts = createAdmissionReceipts("prompt", [query]);
                const turn = {
                    kind: "prompt",
                    chatId,
                    target,
                    replyToMessageId: messageId,
                    sourceMessageIds: [messageId],
                    queueOrder,
                    queueLane: "priority",
                    laneOrder: queueOrder,
                    queuedAttachments: [],
                    content: [{ type: "text", text: `[callback] ${callbackData}` }],
                    historyText: callbackData,
                    statusSummary: callbackData,
                    ...(admissionReceipts.length > 0 ? { admissionReceipts } : {}),
                };
                const result = Queue.appendTelegramPromptTurnOnce(deps.telegramQueueStore.getQueuedItems(), turn);
                if (result.appended) {
                    Updates.assertTelegramUpdateExecutionCurrent(query);
                    deps.telegramQueueStore.setQueuedItems(result.items);
                    reportQueueAdmission([query], admissionReceipts);
                    deps.updateStatus(ctx);
                    requestDispatchNextQueuedTelegramTurn(ctx);
                }
                else {
                    reportQueueAdmission([query], admissionReceipts);
                }
            }
            await deps.answerCallbackQuery(query.id);
            return;
        }
        await menuCallbackHandler(query, ctx);
    };
    const preparePromptTurn = Turns.createTelegramPromptTurnRuntimePreparer({
        allocateQueueOrder: deps.bridgeRuntime.queue.allocateItemOrder,
        downloadFile: deps.downloadFile,
        processAttachments: deps.inboundHandlerRuntime.process,
        resolveTimeLine: deps.resolveTimeLine,
        getAllowedUserId: deps.configStore.getAllowedUserId,
        getAdmissionScope: deps.getAdmissionScope,
        getAdmissionJournalBinding: deps.getAdmissionJournalBinding,
        assertExecutionCurrent(message) {
            Updates.assertTelegramUpdateExecutionCurrent(message);
        },
        // Voice policy resolves missing, invalid, and legacy manual config to hidden.
        getVoiceReplyMode: () => getTelegramVoiceReplyMode(deps.configStore.get()),
        getTelegramThreadLabel: resolveTelegramThreadLabel,
    });
    const enqueueContinueTurn = async (message, ctx) => {
        deps.bridgeRuntime.lifecycle.setFoldQueuedPromptsIntoHistory(false);
        const continueMessage = {
            ...message,
            text: "continue",
            caption: undefined,
        };
        const buildTurn = await preparePromptTurn([continueMessage], ctx);
        const turn = buildTurn([]);
        const continueTurn = {
            ...turn,
            queueLane: "control",
            laneOrder: deps.bridgeRuntime.queue.allocateControlOrder(),
            statusSummary: "continue",
        };
        Updates.assertTelegramUpdateExecutionCurrent(message);
        deps.queueMutationRuntime.append(continueTurn, ctx);
        reportQueueAdmission([continueMessage], continueTurn.admissionReceipts ?? []);
        requestDispatchNextQueuedTelegramTurn(ctx);
    };
    const reservedCommandNames = () => new Set(Commands.getTelegramReservedCommandNames());
    const getPromptTemplateCommands = () => PromptTemplates.getTelegramPromptTemplateCommands(deps.getCommands(), reservedCommandNames());
    const commandHandler = Commands.createTelegramCommandHandlerTargetRuntime({
        assertExecutionCurrent(message) {
            Updates.assertTelegramUpdateExecutionCurrent(message);
        },
        hasAbortHandler: deps.bridgeRuntime.abort.hasHandler,
        clearPendingModelSwitch: deps.modelSwitchController.clearPendingSwitch,
        hasQueuedTelegramItems: deps.telegramQueueStore.hasQueuedItems,
        clearQueuedTelegramItems: deps.queueMutationRuntime.clear,
        setFoldQueuedPromptsIntoHistory: deps.bridgeRuntime.lifecycle.setFoldQueuedPromptsIntoHistory,
        abortCurrentTurn: deps.bridgeRuntime.abort.abortTurn,
        isIdle: deps.isIdle,
        hasPendingMessages: deps.hasPendingMessages,
        hasActiveTelegramTurn: deps.activeTurnRuntime.has,
        hasDispatchPending: deps.bridgeRuntime.lifecycle.hasDispatchPending,
        isCompactionInProgress: deps.bridgeRuntime.lifecycle.isCompactionInProgress,
        setCompactionInProgress: deps.bridgeRuntime.lifecycle.setCompactionInProgress,
        updateStatus: deps.updateStatus,
        isContextActive: deps.isContextActive,
        dispatchNextQueuedTelegramTurn: deps.dispatchNextQueuedTelegramTurn,
        requestNextDispatchAnnouncement: deps.requestNextDispatchAnnouncement,
        cancelNextTransitionAnnouncements: () => {
            deps.activeTurnRuntime.clearNextAbortAnnouncement();
            deps.cancelNextDispatchAnnouncement?.();
        },
        requestDeferredDispatchNextQueuedTelegramTurn: deps.requestDeferredDispatchNextQueuedTelegramTurn,
        startTypingLoop: deps.startTypingLoop,
        stopTypingLoop: deps.stopTypingLoop,
        enqueueContinueTurn,
        compact: deps.compact,
        requestNewSession: deps.requestNewSession,
        allocateItemOrder: deps.bridgeRuntime.queue.allocateItemOrder,
        allocateControlOrder: deps.bridgeRuntime.queue.allocateControlOrder,
        appendControlItem: deps.queueMutationRuntime.append,
        getAdmissionScope: deps.getAdmissionScope,
        getAdmissionJournalBinding: deps.getAdmissionJournalBinding,
        onControlQueued: (message, receipt) => reportQueueAdmission([message], [receipt]),
        showStatus: deps.menuActions.sendStatusMessage,
        openModelMenu: deps.menuActions.openModelMenu,
        openThinkingMenu: (message, ctx) => {
            const target = Commands.getTelegramCommandMessageTarget(message);
            return deps.menuActions.openThinkingMenu(target.chatId, target.replyToMessageId, ctx, target.threadId);
        },
        openQueueMenu: (message, ctx) => {
            const target = Commands.getTelegramCommandMessageTarget(message);
            return deps.openQueueMenu(target.chatId, target.replyToMessageId, ctx, target.threadId);
        },
        openSettingsMenu: deps.openSettingsMenu,
        getAllowedUserId: deps.configStore.getAllowedUserId,
        persistAllowedUserId: deps.configStore.persistAllowedUserId,
        setMyCommands: deps.setMyCommands,
        validateThreadName: deps.validateThreadName,
        renameCurrentThread: deps.renameCurrentThread,
        resetCurrentThreadName: deps.resetCurrentThreadName,
        openThreadNameDialog: async (message) => {
            const target = Updates.getTelegramMessageTarget(message);
            if (!deps.sendInteractiveMessage || !target) {
                await deps.sendTextReply(message.chat.id, message.message_id, Commands.formatTelegramInformationHeading("🏷️", "Usage: /name Navigator"), { parseMode: "HTML", target });
                return;
            }
            const hasManualName = deps.threadStore?.listWorkspaceBindings().some((binding) => binding.target.chatId === target.chatId &&
                binding.target.threadId === target.threadId &&
                typeof binding.manualThreadName === "string") ?? false;
            const instructions = hasManualName
                ? "<b>🏷️ Send a new Thread name using printable ASCII, reset to automatic, or cancel.</b>"
                : "<b>🏷️ Send a Thread name using printable ASCII, or cancel.</b>";
            const buttons = hasManualName
                ? [
                    { text: "↩️ Reset to automatic", callback_data: "thread-name:reset" },
                    { text: "✖ Cancel rename", callback_data: "thread-name:cancel" },
                ]
                : [{ text: "✖ Cancel rename", callback_data: "thread-name:cancel" }];
            const dialogMessageId = await deps.sendInteractiveMessage(target.chatId, instructions, "html", { inline_keyboard: buttons.map((button) => [button]) }, { target });
            if (typeof dialogMessageId !== "number")
                return;
            threadNameDialog.open({
                scope: getThreadNameDialogScope(),
                target,
                dialogMessageId,
            });
        },
        getPromptTemplateCommands,
        sendTextReply: deps.sendTextReply,
        markActiveTurnNextAbortAnnouncement: deps.activeTurnRuntime.markNextAbortAnnouncement,
        getActiveTurnReply: () => {
            const activeTurn = deps.activeTurnRuntime.get();
            if (!activeTurn)
                return undefined;
            return async (text, options) => {
                await deps.sendTextReply(activeTurn.chatId, activeTurn.replyToMessageId, text, { target: activeTurn.target, parseMode: options?.parseMode });
            };
        },
        sendInteractiveMessage: deps.sendInteractiveMessage,
        recordRuntimeEvent: deps.recordRuntimeEvent,
    });
    const promptEnqueueController = Queue.createTelegramPromptEnqueueController({
        ...deps.telegramQueueStore,
        hasPendingDispatch: deps.bridgeRuntime.lifecycle.hasDispatchPending,
        getFoldQueuedPromptsIntoHistory: deps.bridgeRuntime.lifecycle.shouldFoldQueuedPromptsIntoHistory,
        setFoldQueuedPromptsIntoHistory: deps.bridgeRuntime.lifecycle.setFoldQueuedPromptsIntoHistory,
        prepareTurn: async (messages, turnCtx) => {
            const buildTurn = await preparePromptTurn(messages, turnCtx);
            return (historyTurns) => {
                const turn = buildTurn(historyTurns);
                return turn.replyToMessageId > 0
                    ? turn
                    : { ...turn, replyToMessageId: 0 };
            };
        },
        updateStatus: deps.updateStatus,
        dispatchNextQueuedTelegramTurn: requestDispatchNextQueuedTelegramTurn,
        assertExecutionCurrent: (messages) => Updates.assertTelegramUpdateExecutionCurrent(messages[0]),
    });
    const promptEnqueue = async (messages, ctx) => {
        return promptEnqueueController.enqueue(messages, ctx, (turn) => {
            reportQueueAdmission(messages, turn.admissionReceipts ?? []);
        });
    };
    const recordUnboundTemporaryThreadInput = async (messages, ctx) => {
        const message = messages[0], target = message && Updates.getTelegramMessageTarget(message);
        const store = deps.getWorkspaceRestoreStore?.();
        if (!target?.threadId || !store)
            return false;
        let found = store.listTemporaryThreads().find(entry => entry.phase === "created" && entry.target &&
            entry.target.chatId === target.chatId && entry.target.threadId === target.threadId);
        const key = formatTelegramTargetKey(target), implicit = implicitThreadCreations.get(key);
        if (!found && !implicit)
            return false;
        const updateIds = Updates.collectTelegramAdmissionSourceUpdateIds(messages);
        const operatorUserId = deps.configStore.getAllowedUserId();
        const cap = captureTemporaryThreadAuthority(ctx, () => messages.every(source => Updates.getTelegramUpdateExecutionFence(source)?.isCurrent() === true &&
            source.chat.type === "private" && source.chat.id === target.chatId && source.message_thread_id === target.threadId &&
            source.from?.id === operatorUserId && source.from?.is_bot === false));
        if (!found && implicit) {
            if (updateIds.length !== 1) {
                implicitThreadCreations.delete(key);
                return false;
            }
            if (!deps.runWorkspaceOperation || !cap || implicit.ctx !== ctx || implicit.operatorUserId !== cap.operatorUserId ||
                implicit.journalBindingKey !== cap.journalBindingKey || !isDeepStrictEqual(implicit.executor, cap.authority.executor) ||
                implicit.generation !== deps.getSessionGeneration?.() || implicit.scope !== deps.getAdmissionScope?.() ||
                !updateIds.length || updateIds.some(id => id <= implicit.updateId) || !cap.isCurrent()) {
                implicitThreadCreations.delete(key);
                return false;
            }
            found = store.registerImplicitTemporaryThread({ journalBindingKey: cap.journalBindingKey, updateIds }, { chatId: target.chatId, threadId: target.threadId }, randomBytes(16).toString("hex"), {
                ...cap.authority, isCurrent: () => cap.isCurrent() && implicitThreadCreations.get(key) === implicit,
            });
            if (!found || !cap.isCurrent())
                throw new Error("Implicit Telegram Thread registration was not acknowledged; source remains protected.");
            implicitThreadCreations.delete(key);
        }
        if (!found)
            return false;
        cancelTemporaryThreadCleanup(found.token);
        if (!deps.runWorkspaceOperation || !cap || cap.operatorUserId !== found.operatorUserId ||
            cap.journalBindingKey !== found.source.journalBindingKey || !updateIds.length || !cap.isCurrent()) {
            throw new Error("Temporary Thread input membership requires exact current source authority.");
        }
        // The caller already holds the profile Workspace admission (the non-reentrant gate): no nested operation here.
        let entry = store.listTemporaryThreads().find(value => value.token === found.token &&
            isDeepStrictEqual(value.source, found.source) && isDeepStrictEqual(value.target, target));
        if (!entry)
            throw new Error("Temporary Thread input target changed before membership publication.");
        entry = cap.adopt(entry);
        if (!entry || !cap.isCurrent() || !store.recordTemporaryThreadInput(entry, { journalBindingKey: cap.journalBindingKey, updateIds }, cap.authority) || !cap.isCurrent()) {
            throw new Error("Temporary Thread input membership was not acknowledged; the source remains protected.");
        }
        return true;
    };
    const sendUnboundRerouteChooserNow = async (messages, ctx, reportDeferred = true) => {
        const message = messages[0];
        if (!message || !deps.threadStore)
            return;
        const records = deps.threadStore.list();
        const activeRecords = getTelegramRoutableThreadRecords(records, deps.getLiveThreadTargets?.());
        const sourceTarget = typeof message.message_thread_id === "number"
            ? { chatId: message.chat.id, threadId: message.message_thread_id }
            : undefined;
        const sourceKey = sourceTarget
            ? formatTelegramTargetKey(sourceTarget)
            : undefined;
        const includeGuidance = sourceKey
            ? !guidedUnboundTopicKeys.has(sourceKey)
            : true;
        if (sourceKey)
            guidedUnboundTopicKeys.add(sourceKey);
        if (activeRecords.length === 0) {
            await deps.sendTextReply(message.chat.id, message.message_id, [
                includeGuidance ? formatTelegramUnboundTopicGuidance() : undefined,
                `This thread is not bound to a Pi instance. Open an active Pi thread or run ${Commands.formatTelegramPiCommandHtml("/telegram-connect")} from a Pi session to bind one.`,
            ]
                .filter((line) => typeof line === "string")
                .join("\n\n"), { parseMode: "HTML", target: sourceTarget });
            return;
        }
        const command = messages.length === 1 ? getKnownTelegramAllTabCommand(Media.extractFirstTelegramMessageText(messages).trim()) : undefined;
        const rerouteId = storePendingUnboundReroute(messages, command ? "command" : "prompt");
        if (reportDeferred) {
            for (const source of messages) {
                Updates.reportTelegramUpdateDeferred(source);
            }
        }
        const inTemporaryThread = await recordUnboundTemporaryThreadInput(messages, ctx);
        const pending = pendingUnboundReroutes.get(rerouteId);
        if (inTemporaryThread)
            pending.temporaryMembership = true;
        if (messages.length === 1 && sourceTarget &&
            message.chat.type === "private" && typeof message.text === "string" && message.text.trim() &&
            deps.runWorkspaceOperation && deps.editInteractiveMessage &&
            deps.getAdmissionJournalBinding && deps.getCurrentLeaderEpoch &&
            Updates.getTelegramUpdateExecutionFence(message)?.isCurrent() === true && deps.isContextActive?.(ctx) === true) {
            const ownerUserId = deps.configStore.getAllowedUserId();
            const journalBindingKey = deps.getAdmissionJournalBinding();
            if (ownerUserId !== undefined && message.from?.id === ownerUserId && journalBindingKey &&
                Updates.supportsTelegramDeferredAbandonment(message, journalBindingKey)) {
                const leaderEpoch = deps.getCurrentLeaderEpoch();
                if (leaderEpoch !== undefined)
                    pending.abandonment = { ownerUserId, journalBindingKey, leaderEpoch };
            }
        }
        if (command && sourceTarget && !inTemporaryThread && includeGuidance && deps.callApi) {
            // Telegram can create a private tab before delivering an All command not listed in the bot menu.
            // Naming that unbound tab is not creation evidence or authority to delete it.
            Updates.createTelegramUpdateExecutionFenceGuard(message)();
            try {
                await deps.callApi("editForumTopic", { chat_id: sourceTarget.chatId, message_thread_id: sourceTarget.threadId, name: `/${command.name}` });
            }
            catch (error) {
                deps.recordRuntimeEvent?.("routing", error, { phase: "unbound-command-title" });
            }
            Updates.createTelegramUpdateExecutionFenceGuard(message)();
        }
        const text = formatTelegramTemporaryThreadChooserText(command?.name);
        pending.rootChooserText = text;
        const replyMarkup = buildTelegramUnboundRerouteChooserMarkup(rerouteId, activeRecords, { canRestore: sourceTarget !== undefined, canCancel: !!pending.abandonment, getDisplayTitle: deps.getDisplayTitle });
        if (deps.sendInteractiveMessage) {
            const chooserId = await deps.sendInteractiveMessage(message.chat.id, text, "html", replyMarkup, sourceTarget
                ? { target: sourceTarget, replyToMessageId: message.message_id }
                : { replyToMessageId: message.message_id });
            rememberRerouteChooser(rerouteId, chooserId);
            return;
        }
        const chooserId = await deps.sendTextReply(message.chat.id, message.message_id, text, {
            parseMode: "HTML",
            target: sourceTarget,
        });
        rememberRerouteChooser(rerouteId, chooserId);
    };
    const sendUnboundRerouteChooser = async (message, ctx) => {
        const groupKey = Media.getTelegramMediaGroupKey(message);
        if (!groupKey) {
            await sendUnboundRerouteChooserNow([message], ctx);
            return;
        }
        const existing = pendingUnboundRerouteMediaGroups.get(groupKey);
        if (existing)
            clearTimeout(existing.timer);
        const messages = [...(existing?.messages ?? []), message];
        const timer = setTimeout(() => {
            pendingUnboundRerouteMediaGroups.delete(groupKey);
            // A media group's timer runs outside any request, so it takes the same profile admission as a single message.
            const send = () => sendUnboundRerouteChooserNow(messages, ctx, false);
            const task = deps.runWorkspaceOperation
                ? deps.runWorkspaceOperation({ operationId: `workspace-unbound-group:${groupKey}:${randomBytes(8).toString("hex")}`,
                    operationKind: "workspace.route-unbound-thread", scopes: [{ kind: "profile" }] }, send)
                : send();
            void task.catch(error => { deps.recordRuntimeEvent?.("routing", error, { phase: "unbound-media-group-chooser" }); });
        }, 1200);
        timer.unref?.();
        pendingUnboundRerouteMediaGroups.set(groupKey, { messages, timer });
        Updates.reportTelegramUpdateDeferred(message);
    };
    const getKnownTelegramAllTabCommand = (text) => {
        const command = Commands.parseTelegramCommand(text);
        if (!command)
            return undefined;
        if (reservedCommandNames().has(command.name))
            return command;
        if (Commands.findTelegramExtensionCommand(command.name))
            return command;
        if (getPromptTemplateCommands().some((template) => template.command === command.name)) {
            return command;
        }
        return undefined;
    };
    /**
     * An All input gets one source-bound tab. The original stays deferred in All until explicit routing,
     * and an existing entry is reused on replay, so restart never creates a second tab or retries an unknown one.
     */
    const sendAllTabTemporaryInputChooser = async (command, commandText, message, ctx) => {
        const [updateId] = Updates.collectTelegramAdmissionSourceUpdateIds([message]);
        const execution = Updates.getTelegramUpdateExecutionFence(message);
        const cap = captureTemporaryThreadAuthority(ctx, () => execution?.isCurrent() === true);
        if (!cap || !deps.runWorkspaceOperation || !deps.callApi || updateId === undefined ||
            message.chat.type !== "private" || message.chat.id !== cap.operatorUserId || message.from?.id !== cap.operatorUserId ||
            !cap.isCurrent())
            return false;
        const { store, operatorUserId, journalBindingKey, epoch, authority } = cap, current = cap.isCurrent;
        const source = { journalBindingKey, updateId };
        const find = () => store.listTemporaryThreads().find(entry => entry.source.journalBindingKey === journalBindingKey && entry.source.updateId === updateId);
        let entry;
        await deps.runWorkspaceOperation({ operationId: `temporary-thread-${randomBytes(16).toString("hex")}`,
            operationKind: "workspace.temporary-thread", scopes: [{ kind: "profile" }] }, async () => {
            if (!current())
                return;
            let found = find();
            found = found && cap.adopt(found);
            if (found) {
                entry = found;
                return;
            }
            const reservation = store.reserveTemporaryThread(source, randomBytes(16).toString("hex"), authority);
            entry = reservation?.entry;
            if (!reservation?.reserved || !current())
                return;
            let created;
            try {
                // The single creation attempt; any failure leaves `creating` as an unknown outcome, never a retry.
                created = await deps.callApi("createForumTopic", { chat_id: operatorUserId, name: command ? `/${command.name}` : "New chat" });
            }
            catch (error) {
                deps.recordRuntimeEvent?.("routing", error, { phase: "temporary-thread-create" });
                return;
            }
            const threadId = created?.message_thread_id;
            if (!current() || typeof threadId !== "number" || !Number.isSafeInteger(threadId) || threadId <= 0)
                return;
            entry = store.acknowledgeTemporaryThread(reservation.entry, { chatId: operatorUserId, threadId }, authority) ?? entry;
        });
        if (!current() || !entry)
            throw new Error("Temporary Thread authority changed; the input remains retryable.");
        const commandMessage = Updates.carryTelegramUpdateExecutionFence(message, {
            ...message, text: commandText, caption: undefined
        });
        if (entry.phase !== "created" || !entry.target) {
            Updates.reportTelegramUpdateDeferred(commandMessage);
            await deps.sendTextReply(message.chat.id, message.message_id, "<b>⚠️ The routing tab for this command could not be confirmed.</b> It stays held and was not sent to Pi.", { parseMode: "HTML" });
            return true;
        }
        const target = entry.target;
        const records = getTelegramRoutableThreadRecords(deps.threadStore?.list() ?? [], deps.getLiveThreadTargets?.());
        const rerouteId = storePendingUnboundReroute([commandMessage], command ? "command" : "prompt", target);
        const pending = pendingUnboundReroutes.get(rerouteId);
        pending.temporaryThread = entry;
        Updates.reportTelegramUpdateDeferred(commandMessage);
        // Cancel is offered only when the exact source can still be abandoned; the tab removal needs that retention first.
        if (deps.editInteractiveMessage && Updates.supportsTelegramDeferredAbandonment(commandMessage, journalBindingKey)) {
            pending.abandonment = { ownerUserId: operatorUserId, journalBindingKey, leaderEpoch: epoch };
        }
        pending.rootChooserText = formatTelegramTemporaryThreadChooserText(command?.name);
        try {
            if (!deps.sendInteractiveMessage)
                throw new Error("Temporary Thread chooser publication is unavailable.");
            const chooserId = await deps.sendInteractiveMessage(target.chatId, pending.rootChooserText, "html", buildTelegramUnboundRerouteChooserMarkup(rerouteId, records, { canRestore: true, canCancel: !!pending.abandonment,
                getDisplayTitle: deps.getDisplayTitle }), { target });
            rememberRerouteChooser(rerouteId, chooserId);
        }
        catch (error) {
            removePendingReroute(rerouteId);
            throw error;
        }
        return true;
    };
    const sendAllTabCommandChooser = async (command, commandText, message, options = {}) => {
        if (!deps.threadStore)
            return false;
        const records = deps.threadStore.list();
        const activeRecords = getTelegramRoutableThreadRecords(records, deps.getLiveThreadTargets?.());
        if (activeRecords.length === 0)
            return false;
        const commandMessage = Updates.carryTelegramUpdateExecutionFence(message, {
            ...message,
            text: commandText,
            caption: undefined,
        });
        const rerouteId = storePendingUnboundReroute([commandMessage], "command");
        Updates.reportTelegramUpdateDeferred(commandMessage);
        const text = formatTelegramAllTabMenuChooserText(command.name);
        pendingUnboundReroutes.get(rerouteId).rootChooserText = text;
        const replyMarkup = buildTelegramUnboundRerouteChooserMarkup(rerouteId, activeRecords, { canRestore: typeof message.message_thread_id === "number", getDisplayTitle: deps.getDisplayTitle });
        let chooserId;
        try {
            if (deps.sendInteractiveMessage) {
                chooserId = await deps.sendInteractiveMessage(message.chat.id, text, "html", replyMarkup, options.target || options.replyToSource
                    ? {
                        ...(options.target ? { target: options.target } : {}),
                        ...(options.replyToSource
                            ? { replyToMessageId: message.message_id }
                            : {}),
                    }
                    : undefined);
            }
            else if (deps.callApi) {
                const chooser = await deps.callApi("sendMessage", {
                    chat_id: message.chat.id,
                    text,
                    parse_mode: "HTML",
                    reply_markup: replyMarkup,
                    ...(typeof options.target?.threadId === "number"
                        ? { message_thread_id: options.target.threadId }
                        : {}),
                    ...(options.replyToSource
                        ? {
                            reply_parameters: {
                                message_id: message.message_id,
                                allow_sending_without_reply: true,
                            },
                        }
                        : {}),
                });
                chooserId = chooser?.message_id;
            }
            else {
                chooserId = await deps.sendTextReply(message.chat.id, message.message_id, text, {
                    parseMode: "HTML",
                    target: options.target,
                });
            }
        }
        catch (error) {
            removePendingReroute(rerouteId);
            throw error;
        }
        rememberRerouteChooser(rerouteId, chooserId);
        Updates.reportTelegramUpdateCompleted(commandMessage);
        return true;
    };
    const commandOrPrompt = Commands.createTelegramCommandOrPromptRuntime({
        extractRawText: Media.extractFirstTelegramMessageText,
        assertExecutionCurrent(message) {
            Updates.assertTelegramUpdateExecutionCurrent(message);
        },
        shouldIgnoreMessages: (messages) => !Media.hasTelegramMessagesPromptContent(messages),
        consumeThreadNameInput: async (messages) => {
            const message = messages[0];
            if (!message || messages.length !== 1)
                return false;
            const target = Updates.getTelegramMessageTarget(message);
            if (!target)
                return false;
            const candidate = threadNameDialog.inspect(target);
            if (!candidate || candidate.scope !== getThreadNameDialogScope() ||
                candidate.phase !== "input")
                return false;
            const name = Media.extractFirstTelegramMessageText(messages).trim();
            if (/^[A-Z]$/.test(name) && deps.resetCurrentThreadName) {
                const consumed = threadNameDialog.consumeName({
                    scope: getThreadNameDialogScope(), target, text: name,
                });
                if (consumed.kind !== "name")
                    return false;
                const result = await deps.resetCurrentThreadName(target);
                if (!result.ok) {
                    threadNameDialog.open({
                        scope: getThreadNameDialogScope(), target,
                        dialogMessageId: candidate.dialogMessageId,
                    });
                }
                const replyText = result.ok && !result.message
                    ? Commands.formatTelegramAutomaticThreadDisplayNameRestoredHeading(result.threadName ?? name)
                    : Commands.formatTelegramInformationHeading(result.ok ? "✅" : "⚠️", result.message ?? "Thread display name reset failed.");
                if (result.ok) {
                    Updates.assertTelegramUpdateExecutionCurrent(message);
                    Updates.reportTelegramUpdateCompleted(message);
                    void deps.sendTextReply(target.chatId, message.message_id, replyText, { parseMode: "HTML", target }).catch((error) => deps.recordRuntimeEvent?.("telegram-command", error, {
                        command: "name", phase: "reset-result",
                    }));
                    return true;
                }
                await deps.sendTextReply(target.chatId, message.message_id, replyText, { parseMode: "HTML", target });
                return true;
            }
            const validationError = deps.validateThreadName?.(name);
            if (!name || validationError) {
                await deps.sendTextReply(target.chatId, message.message_id, validationError
                    ? Commands.formatTelegramInvalidInstanceName(validationError)
                    : Commands.formatTelegramInformationHeading("⚠️", "Send 1–96 printable ASCII characters."), { parseMode: "HTML", target });
                return true;
            }
            const consumed = threadNameDialog.consumeName({
                scope: getThreadNameDialogScope(), target, text: name,
            });
            if (consumed.kind !== "name")
                return false;
            const result = await deps.renameCurrentThread?.(target, consumed.name);
            if (!result?.ok) {
                threadNameDialog.open({
                    scope: getThreadNameDialogScope(),
                    target,
                    dialogMessageId: candidate.dialogMessageId,
                });
            }
            const replyText = result?.ok && !result.message
                ? Commands.formatTelegramThreadDisplayNameSavedHeading(result.threadName ?? consumed.name)
                : Commands.formatTelegramInformationHeading(result?.ok ? "✅" : "⚠️", result?.message ?? "Thread display name update failed.");
            if (result?.ok) {
                Updates.assertTelegramUpdateExecutionCurrent(message);
                Updates.reportTelegramUpdateCompleted(message);
                void deps.sendTextReply(target.chatId, message.message_id, replyText, { parseMode: "HTML", target }).catch((error) => deps.recordRuntimeEvent?.("telegram-command", error, {
                    command: "name", phase: "rename-result",
                }));
                return true;
            }
            await deps.sendTextReply(target.chatId, message.message_id, replyText, { parseMode: "HTML", target });
            return true;
        },
        handleCommand: commandHandler,
        executeExtensionCommand: async (command, message, ctx) => {
            const extensionCommand = Commands.findTelegramExtensionCommand(command.name);
            if (!extensionCommand)
                return false;
            const sourceTarget = Updates.getTelegramMessageTarget(message);
            const assertExecutionCurrent = Updates.createTelegramUpdateExecutionFenceGuard(message);
            try {
                assertExecutionCurrent();
                await extensionCommand.handler({
                    name: command.name,
                    args: command.args,
                    reply: async (text) => {
                        assertExecutionCurrent();
                        await deps.sendTextReply(message.chat.id, message.message_id, text, { target: sourceTarget });
                        assertExecutionCurrent();
                    },
                    enqueuePrompt: async (prompt) => {
                        assertExecutionCurrent();
                        await promptEnqueue([
                            {
                                ...message,
                                text: prompt,
                                caption: undefined,
                            },
                        ], ctx);
                    },
                });
                assertExecutionCurrent();
            }
            catch (error) {
                deps.recordRuntimeEvent?.("telegram-command", error, {
                    command: command.name,
                });
                assertExecutionCurrent();
                await deps.sendTextReply(message.chat.id, message.message_id, "Command failed.", { target: sourceTarget });
            }
            return true;
        },
        expandPromptTemplateCommand: (commandName, args) => PromptTemplates.expandTelegramPromptTemplateCommand(commandName, args, getPromptTemplateCommands()),
        replaceMessageText: (message, text) => ({ ...message, text, caption: undefined }),
        enqueueTurn: async (messages, ctx) => {
            await promptEnqueue(messages, ctx);
        },
    });
    dispatchReroutedCommandMessages = (messages, ctx) => commandOrPrompt.dispatchMessages(messages, ctx);
    const mediaDispatch = Media.createTelegramMediaGroupDispatchRuntime({
        mediaGroups: deps.mediaGroupRuntime,
        dispatchMessages: commandOrPrompt.dispatchMessages,
        onDeferredMessage: Updates.reportTelegramUpdateDeferred,
    });
    const textDispatch = TextGroups.createTelegramTextGroupDispatchRuntime({
        textGroups: deps.textGroupRuntime,
        dispatchMessages: commandOrPrompt.dispatchMessages,
        dispatchSingleMessage: mediaDispatch.handleMessage,
        onDeferredMessage: Updates.reportTelegramUpdateDeferred,
    });
    const editRuntime = Turns.createTelegramQueuedPromptEditRuntime({
        ...deps.telegramQueueStore,
        updateStatus: deps.updateStatus,
    });
    const handleTelegramTopicLifecycleUpdate = async (lifecycle, ctx) => {
        const assertExecutionCurrent = Updates.createTelegramUpdateExecutionFenceGuard(lifecycle.message);
        assertExecutionCurrent();
        await deps.handleTelegramTopicLifecycleUpdate?.(lifecycle, ctx);
        assertExecutionCurrent();
        const key = formatTelegramTargetKey(lifecycle.target);
        if (lifecycle.kind !== "created" || !deps.threadStore) {
            implicitThreadCreations.delete(key);
            return;
        }
        await deps.threadStore.load();
        assertExecutionCurrent();
        const message = lifecycle.message, created = message.forum_topic_created;
        const [updateId] = Updates.collectTelegramAdmissionSourceUpdateIds([message]);
        const cap = captureTemporaryThreadAuthority(ctx, () => Updates.getTelegramUpdateExecutionFence(message)?.isCurrent() === true);
        if (created && typeof created === "object" && "is_name_implicit" in created && created.is_name_implicit === true &&
            cap && deps.runWorkspaceOperation && updateId !== undefined && !Updates.isTelegramHistoricalInput(message) &&
            message.chat.type === "private" && message.chat.id === cap.operatorUserId && message.from?.id === cap.operatorUserId &&
            message.from.is_bot === false && cap.isCurrent() && implicitThreadCreations.size < 100) {
            implicitThreadCreations.set(key, { ctx, updateId, operatorUserId: cap.operatorUserId,
                journalBindingKey: cap.journalBindingKey, executor: structuredClone(cap.authority.executor),
                generation: deps.getSessionGeneration?.(), scope: deps.getAdmissionScope?.() });
        }
        else
            implicitThreadCreations.delete(key);
    };
    // Answer the guest query immediately so the agent-end edit can replace the
    // early ACK once the turn settles. The ACK is the first placeholder frame and
    // the loop rotates through the remaining frames until the replacement.
    const TELEGRAM_GUEST_ACK_HTML = Replies.buildTelegramGuestPlaceholderFrame(0);
    const handleAuthorizedTelegramGuestMessage = async (guestMessage, ctx) => {
        const assertExecutionCurrent = Updates.createTelegramUpdateExecutionFenceGuard(guestMessage);
        assertExecutionCurrent();
        let guestInlineMessageId;
        if (deps.answerGuestQueryForInlineMessage) {
            try {
                guestInlineMessageId = await deps.answerGuestQueryForInlineMessage(guestMessage.guest_query_id, TELEGRAM_GUEST_ACK_HTML, { parseMode: "HTML" });
                if (guestInlineMessageId) {
                    deps.startGuestPlaceholder?.(guestInlineMessageId);
                }
                deps.recordRuntimeEvent?.("guest", new Error("Guest ACK answered the guest query"), {
                    phase: "guest-ack-sent",
                    guestQueryId: guestMessage.guest_query_id,
                    hasInlineMessageId: !!guestInlineMessageId,
                });
            }
            catch (error) {
                deps.recordRuntimeEvent?.("guest", error, {
                    phase: "guest-ack-failed",
                    guestQueryId: guestMessage.guest_query_id,
                });
            }
            assertExecutionCurrent();
        }
        // Media messages carry the user's text as a caption.
        const text = guestMessage.text ?? guestMessage.caption ?? "";
        const gm = guestMessage;
        // Build telegram prefix with guest context
        const chatRaw = gm.chat;
        const chatType = chatRaw?.type;
        const fromRaw = gm.from;
        const replyMsg = gm.reply_to_message;
        const replyFromRaw = replyMsg?.from;
        const guestBotCallerUser = gm.guest_bot_caller_user;
        const guestBotCallerChat = gm.guest_bot_caller_chat;
        const ownerUserId = deps.configStore.getAllowedUserId();
        const replyPeer = formatTelegramPromptPeer(replyFromRaw);
        const guestPeer = resolveTelegramGuestPromptPeer({
            chatType,
            chat: chatRaw,
            from: fromRaw,
            replyFrom: replyFromRaw,
            guestBotCallerUser,
            guestBotCallerChat,
            ownerUserId,
        });
        const prefixParts = ["telegram"];
        if (guestPeer) {
            prefixParts.push(`guest:${guestPeer}`);
        }
        else if (chatType === "private") {
            deps.recordRuntimeEvent?.("guest", new Error("Private Guest Mode remote peer could not be resolved"), {
                phase: "peer-attribution",
                chatId: typeof chatRaw?.id === "number" ? chatRaw.id : undefined,
                fromId: typeof fromRaw?.id === "number" ? fromRaw.id : undefined,
                hasReplyFrom: !!replyFromRaw,
                hasCallerUser: !!guestBotCallerUser,
                hasCallerChat: !!guestBotCallerChat,
            });
        }
        const telegramPrefix = `[${prefixParts.join("|")}]`;
        // Extract reply context
        const replyText = replyMsg
            ? (replyMsg.text || replyMsg.caption || "").trim()
            : "";
        // Download files, run inbound handlers
        const guestMsg = guestMessage;
        // Guest message IDs belong to the peer's chat; scope files to that peer so they cannot collide with bot-chat files.
        const guestScope = resolveTelegramGuestFileScope({ chatType, chat: chatRaw, from: fromRaw, replyFrom: replyFromRaw,
            guestBotCallerUser, guestBotCallerChat, ownerUserId });
        const downloadGuestFile = (fileId, fileName, source) => deps.downloadFile(fileId, fileName, source ? { ...source, scope: guestScope } : source);
        const replyFiles = guestMsg.reply_to_message
            ? await Media.downloadTelegramMessageFiles([guestMsg.reply_to_message], { downloadFile: downloadGuestFile })
            : [];
        assertExecutionCurrent();
        const processedReply = replyFiles.length > 0
            ? await deps.inboundHandlerRuntime.process(replyFiles, "", ctx)
            : undefined;
        assertExecutionCurrent();
        const files = await Media.downloadTelegramMessageFiles([guestMsg], {
            downloadFile: downloadGuestFile,
        });
        assertExecutionCurrent();
        const processed = await deps.inboundHandlerRuntime.process(files, text, ctx);
        assertExecutionCurrent();
        const rawText = processed.rawText || text;
        let sourceContext = "";
        if (replyMsg) {
            const replyHeader = replyPeer ? `[reply|from:${replyPeer}]` : "[reply]";
            const replyBlock = replyText
                ? `${replyHeader} ${replyText}`
                : replyHeader;
            sourceContext = appendTelegramSourceAttachmentSection(replyBlock, replyPeer, processedReply?.promptFiles ?? replyFiles, processedReply?.handlerOutputs);
        }
        const promptText = Turns.buildTelegramTurnPrompt({
            telegramPrefix,
            rawText,
            files,
            promptFiles: processed.promptFiles,
            handlerOutputs: processed.handlerOutputs,
            sourceContext,
            // Guest Mode allows exactly one reply within Telegram's limited response
            // window; the note travels with the turn text so the agent sees it at
            // execution time without a guest-specific system prompt variant.
            guestTurn: true,
        });
        const order = deps.bridgeRuntime.queue.allocateItemOrder();
        const content = [
            { type: "text", text: promptText },
        ];
        for (const file of processed.promptFiles) {
            if (file.isImage && file.mimeType) {
                try {
                    const buffer = await readFile(file.path);
                    assertExecutionCurrent();
                    content.push({
                        type: "image",
                        data: Buffer.from(buffer).toString("base64"),
                        mimeType: file.mimeType,
                    });
                }
                catch {
                    // skip unreadable files
                }
            }
        }
        const admissionReceipts = createAdmissionReceipts("prompt", [guestMessage]);
        const guestTurn = {
            kind: "prompt",
            chatId: 0,
            replyToMessageId: 0,
            guestQueryId: guestMessage.guest_query_id,
            ...(guestInlineMessageId ? { guestInlineMessageId } : {}),
            sourceMessageIds: [],
            queueOrder: order,
            queueLane: "default",
            laneOrder: order,
            queuedAttachments: [],
            content,
            historyText: Turns.formatTelegramTurnStatusSummary(processed.rawText || text, processed.promptFiles, processed.handlerOutputs),
            statusSummary: Turns.truncateTelegramQueueSummary(processed.rawText || text),
            ...(admissionReceipts.length > 0 ? { admissionReceipts } : {}),
        };
        const items = deps.telegramQueueStore.getQueuedItems();
        Updates.assertTelegramUpdateExecutionCurrent(guestMessage);
        deps.telegramQueueStore.setQueuedItems(Queue.appendTelegramQueueItem(items, guestTurn));
        reportQueueAdmission([guestMessage], admissionReceipts);
        deps.updateStatus(ctx);
        requestDispatchNextQueuedTelegramTurn(ctx);
    };
    const runtime = Updates.createTelegramPairedUpdateRuntime({
        getAllowedUserId: deps.configStore.getAllowedUserId,
        getForumTarget: () => deps.configStore.get().forumTarget,
        getCurrentInstanceId: deps.getCurrentInstanceId,
        getMessageOwnership: deps.getMessageOwnership,
        getTargetOwnership: deps.getTargetOwnership,
        recordMessageOwnership: deps.recordMessageOwnership,
        handleTelegramTopicLifecycleUpdate,
        foreignOwnedUpdateForwarder: deps.foreignOwnedUpdateForwarder,
        persistAllowedUserId: deps.configStore.persistAllowedUserId,
        updateStatus: deps.updateStatus,
        removePendingMediaGroupMessages: deps.mediaGroupRuntime.removeMessages,
        flushPendingMediaGroupMessage: deps.mediaGroupRuntime.flushMessage,
        flushPendingTextGroupMessage: deps.textGroupRuntime.flushMessage,
        removeQueuedTelegramTurnsByMessageIds: deps.queueMutationRuntime.removeByMessageIds,
        applyQueuedTelegramTurnReactionByMessageId: deps.queueMutationRuntime.applyReactionByMessageId,
        answerCallbackQuery: deps.answerCallbackQuery,
        answerGuestQuery: deps.answerGuestQuery,
        handleAuthorizedTelegramCallbackQuery: callbackHandler,
        sendTextReply: deps.sendTextReply,
        handleAuthorizedTelegramMessage: async (message, ctx) => {
            const assertExecutionCurrent = Updates.createTelegramUpdateExecutionFenceGuard(message);
            assertExecutionCurrent();
            if (typeof message.message_thread_id === "number") {
                await deps.handleTelegramThreadTargetObserved?.({
                    chatId: message.chat.id,
                    threadId: message.message_thread_id,
                }, ctx);
                assertExecutionCurrent();
            }
            const text = Media.extractFirstTelegramMessageText([
                message,
            ]).trim();
            if (deps.threadStore && typeof message.message_thread_id !== "number") {
                await deps.threadStore.load();
                assertExecutionCurrent();
                if (deps.threadStore.getBotState().threadMode === "disabled") {
                    await textDispatch.handleMessage(message, ctx);
                    return;
                }
                const records = deps.threadStore.list();
                const bindings = getTelegramRoutableThreadRecords(records, deps.getLiveThreadTargets?.());
                const command = getKnownTelegramAllTabCommand(text);
                const [sourceUpdateId] = Updates.collectTelegramAdmissionSourceUpdateIds([message]);
                const presented = sourceUpdateId !== undefined && deps.getWorkspaceRestoreStore?.()?.listTemporaryThreads().some(entry => entry.source.updateId === sourceUpdateId && entry.source.journalBindingKey === deps.getAdmissionJournalBinding?.()) === true;
                // Returning before deferral lets the admission worker terminally settle expired replay; a presented tab is not expiry.
                if (command && command.name !== "thread" && !presented && isTelegramAllTabCommandExpired(message))
                    return;
                if (command?.name !== "thread" && (bindings.length > 0 || presented) &&
                    typeof message.text === "string" && message.text.trim() &&
                    await sendAllTabTemporaryInputChooser(command, text, message, ctx))
                    return;
                if (bindings.length > 0 && command && command.name !== "thread") {
                    if (await sendAllTabCommandChooser(command, text, message, {
                        replyToSource: true,
                    })) {
                        return;
                    }
                }
                if (bindings.length > 0 && !text.startsWith("/")) {
                    const probeTarget = bindings[0]?.target;
                    if (probeTarget?.threadId && deps.callApi) {
                        try {
                            await deps.callApi("sendChatAction", {
                                chat_id: probeTarget.chatId,
                                message_thread_id: probeTarget.threadId,
                                action: "typing",
                            });
                        }
                        catch (error) {
                            if (Threads.isTelegramTopicModeUnavailableError(error) ||
                                Threads.isTelegramTopicTargetStaleError(error)) {
                                deps.threadStore.setBotState({
                                    threadMode: "disabled",
                                    updatedAtMs: Date.now(),
                                    lastReconcileAction: "thread-mode-unavailable-threadless-prompt",
                                });
                                await deps.threadStore.persist();
                                assertExecutionCurrent();
                                await textDispatch.handleMessage(message, ctx);
                                return;
                            }
                            deps.recordRuntimeEvent?.("telegram", error, {
                                phase: "threadless-topic-capability-check",
                                chatId: probeTarget.chatId,
                                threadId: probeTarget.threadId,
                            });
                        }
                    }
                    await deps.sendTextReply(message.chat.id, message.message_id, "This bot is in threaded multi-instance mode. Send prompts in a bound Pi thread tab so they route to the right instance.");
                    return;
                }
            }
            await textDispatch.handleMessage(message, ctx);
        },
        handleAuthorizedTelegramEditedMessage: editRuntime.updateFromEditedMessage,
        handleAuthorizedTelegramGuestMessage,
        handleUnboundTelegramTopicMessage: (message, ctx) => {
            const operation = async () => {
                const assertExecutionCurrent = Updates.createTelegramUpdateExecutionFenceGuard(message);
                assertExecutionCurrent();
                if (!deps.threadStore) {
                    await textDispatch.handleMessage(message, ctx);
                    return;
                }
                await deps.threadStore.load();
                assertExecutionCurrent();
                if (Updates.isTelegramHistoricalInput(message, entry => reviewMessage(entry) !== undefined) && isReviewText(message) &&
                    deps.getCurrentLeaderEpoch?.() !== undefined && needsHistoricalReview(message)) {
                    if (!Updates.reportTelegramHistoricalRoutingReview(message))
                        throw new Error("Historical routing hold is unavailable.");
                    return;
                }
                if (deps.threadStore.getBotState().threadMode === "disabled") {
                    await textDispatch.handleMessage(message, ctx);
                    return;
                }
                const target = Updates.getTelegramMessageTarget(message);
                if (!target?.threadId) {
                    await textDispatch.handleMessage(message, ctx);
                    return;
                }
                const text = Media.extractFirstTelegramMessageText([
                    message,
                ]).trim();
                const instanceId = deps.getCurrentInstanceId?.();
                const leaderProfileKey = getLeaderTopicProfileKey(ctx, instanceId);
                const records = deps.threadStore.list();
                const routableRecords = getTelegramRoutableThreadRecords(records, deps.getLiveThreadTargets?.());
                const hasAnyRoutableThread = routableRecords.length > 0;
                const existing = records.find((r) => {
                    return (r.target.chatId === target.chatId &&
                        r.target.threadId === target.threadId);
                });
                if (existing) {
                    const isLeaderTopic = (instanceId && existing.instanceId === instanceId) ||
                        (!!leaderProfileKey && existing.profileKey === leaderProfileKey);
                    if (existing.status === "active" && isLeaderTopic) {
                        if (typeof existing.rerouteConfirmedAtMs !== "number") {
                            const nowMs = Date.now();
                            deps.threadStore.upsert({
                                ...existing,
                                updatedAtMs: nowMs,
                                rerouteConfirmedAtMs: nowMs,
                            });
                            await deps.threadStore.persist();
                            assertExecutionCurrent();
                        }
                        await textDispatch.handleMessage(message, ctx);
                        return;
                    }
                    if (existing.status === "starting") {
                        await deps.sendTextReply(target.chatId, message.message_id, "Instance " +
                            getTelegramThreadRecordLabel(existing, deps.getDisplayTitle) +
                            " is starting. Please wait…", { target });
                        return;
                    }
                    if (existing.status === "active") {
                        await deps.sendTextReply(target.chatId, message.message_id, "Instance " +
                            escapeHtml(getTelegramThreadRecordLabel(existing, deps.getDisplayTitle)) +
                            ` is not currently registered with the Telegram bus. This thread is preserved; retry shortly. If it does not recover, run ${Commands.formatTelegramPiCommandHtml("/telegram-connect")} in that Pi instance.`, { parseMode: "HTML", target });
                        return;
                    }
                    await deps.sendTextReply(target.chatId, message.message_id, "Topic " +
                        (existing.slot ?? "?") +
                        " is " +
                        existing.status +
                        ". Start a Pi instance to claim it.", { target });
                    return;
                }
                const deletedObservation = deps.threadStore
                    .listSyncObservations()
                    .find((observation) => observation.syncStatus === "deleted" &&
                    observation.target.chatId === target.chatId &&
                    observation.target.threadId === target.threadId);
                if (deletedObservation) {
                    deps.recordRuntimeEvent?.("inbound-worker", "Discarded update from a confirmed deleted Telegram thread", {
                        phase: "discard-deleted-thread",
                        chatId: target.chatId,
                        threadId: target.threadId,
                        messageId: message.message_id,
                    });
                    return;
                }
                const reservations = deps.threadStore.listReservations();
                const reservation = reservations.find((reservation) => reservation.target.chatId === target.chatId &&
                    reservation.target.threadId === target.threadId);
                if (reservation) {
                    await deps.sendTextReply(target.chatId, message.message_id, "Previous leader thread (" +
                        (reservation.slot ?? "?") +
                        "). Closing and deleting this old topic. Use the current thread tab instead.", { target });
                    await deleteReservedTelegramTopicThroughReconciler(deps, { chatId: target.chatId, threadId: target.threadId }, message.message_id);
                    return;
                }
                const command = getKnownTelegramAllTabCommand(text);
                if (command && hasAnyRoutableThread) {
                    await sendUnboundRerouteChooser(message, ctx);
                    return;
                }
                if (leaderProfileKey && deps.callApi) {
                    const currentLeaderRecord = records.find((record) => {
                        if (record.status !== "active")
                            return false;
                        if (instanceId && record.instanceId === instanceId)
                            return true;
                        return record.profileKey === leaderProfileKey;
                    });
                    if (currentLeaderRecord &&
                        (currentLeaderRecord.target.chatId !== target.chatId ||
                            currentLeaderRecord.target.threadId !== target.threadId)) {
                        let currentLeaderIsStale = false;
                        try {
                            await deps.callApi("sendChatAction", {
                                chat_id: currentLeaderRecord.target.chatId,
                                message_thread_id: currentLeaderRecord.target.threadId,
                                action: "typing",
                            });
                        }
                        catch (error) {
                            currentLeaderIsStale =
                                Threads.isTelegramTopicTargetStaleError(error);
                            if (!currentLeaderIsStale)
                                throw error;
                        }
                        if (currentLeaderIsStale) {
                            const slot = deps.threadStore.allocateSlot(leaderProfileKey);
                            if (!slot) {
                                deps.threadStore.markStaleByTarget(currentLeaderRecord.target, "deleted", "Current leader thread is stale during unbound prompt routing.");
                                await deps.threadStore.persist();
                                assertExecutionCurrent();
                                await deps.sendTextReply(target.chatId, message.message_id, TELEGRAM_SLOT_CAPACITY_MESSAGE, { target });
                                return;
                            }
                            deps.threadStore.markStaleByTarget(currentLeaderRecord.target, "deleted", "Current leader thread is stale during unbound prompt routing.");
                            const threadName = getRestoredThreadName(currentLeaderRecord, slot);
                            deps.threadStore.upsert({
                                ...currentLeaderRecord,
                                profileKey: leaderProfileKey,
                                owner: {
                                    kind: "leader",
                                    cwd: typeof ctx.cwd === "string"
                                        ? ctx.cwd
                                        : undefined,
                                    instanceId,
                                },
                                target: { chatId: target.chatId, threadId: target.threadId },
                                status: "active",
                                updatedAtMs: Date.now(),
                                threadName,
                                instanceId,
                                slot,
                            });
                            await deps.threadStore.persist();
                            assertExecutionCurrent();
                            deps.setCurrentLeaderIdentity?.({
                                target: { chatId: target.chatId, threadId: target.threadId },
                                slot,
                                threadName,
                            });
                            deps.recordRuntimeEvent?.("bus", "Bus leader reclaimed stale-current unbound thread", {
                                phase: "leader-topic-unbound-stale-reclaim",
                                chatId: target.chatId,
                                threadId: target.threadId,
                                staleThreadId: currentLeaderRecord.target.threadId,
                                slot,
                                profileKey: leaderProfileKey,
                            });
                            await textDispatch.handleMessage(message, ctx);
                            return;
                        }
                    }
                }
                if (leaderProfileKey &&
                    !hasActiveLeaderTopic(records, leaderProfileKey, instanceId) &&
                    !hasAnyRoutableThread) {
                    const priorLeaderRecord = deps.threadStore.getByProfileKey(leaderProfileKey);
                    const priorLeaderIdentity = deps.threadStore.getIdentityByProfileKey(leaderProfileKey);
                    const slot = deps.threadStore.allocateSlot(leaderProfileKey, priorLeaderRecord?.slot ?? priorLeaderIdentity?.slot);
                    if (!slot) {
                        await deps.sendTextReply(target.chatId, message.message_id, TELEGRAM_SLOT_CAPACITY_MESSAGE, { target });
                        return;
                    }
                    const identityThreadName = priorLeaderIdentity?.threadName &&
                        ThreadNaming.isTelegramTopicThreadNameValidForSlot(priorLeaderIdentity.threadName, slot)
                        ? priorLeaderIdentity.threadName
                        : undefined;
                    const threadName = priorLeaderRecord?.threadName ??
                        identityThreadName ??
                        ThreadNaming.chooseTelegramThreadName({ slot }) ??
                        "Pi";
                    deps.threadStore.upsert({
                        profileKey: leaderProfileKey,
                        owner: {
                            kind: "leader",
                            cwd: typeof ctx.cwd === "string"
                                ? ctx.cwd
                                : undefined,
                            instanceId,
                        },
                        target: { chatId: target.chatId, threadId: target.threadId },
                        status: "active",
                        createdAtMs: priorLeaderRecord?.createdAtMs ?? Date.now(),
                        updatedAtMs: Date.now(),
                        threadName,
                        instanceId,
                        slot,
                    });
                    await deps.threadStore.persist();
                    assertExecutionCurrent();
                    deps.setCurrentLeaderIdentity?.({
                        target: { chatId: target.chatId, threadId: target.threadId },
                        slot,
                        threadName,
                    });
                    deps.recordRuntimeEvent?.("bus", "Bus leader reclaimed unbound thread", {
                        phase: "leader-topic-reclaim",
                        chatId: target.chatId,
                        threadId: target.threadId,
                        slot,
                        profileKey: leaderProfileKey,
                    });
                    await textDispatch.handleMessage(message, ctx);
                    return;
                }
                await sendUnboundRerouteChooser(message, ctx);
                return;
            };
            if (!deps.runWorkspaceOperation)
                return operation();
            return deps.runWorkspaceOperation({
                operationId: `workspace-unbound:${message.chat.id}:${message.message_id}`,
                operationKind: "workspace.route-unbound-thread",
                scopes: [{ kind: "profile" }],
            }, operation);
        },
    });
    return { ...runtime, expireRoutingInput, shouldReviewHistoricalInput, shouldHoldPendingInput, forgetPreviousWorld, beforeQueueReceiptPublished, onQueueReceiptCommitted, onQueueReceiptCompleted, onUpdateCompleted,
        onWorkspaceRestoreRecipientObserved(follower, isCurrent, ctx) {
            if (ctx !== undefined)
                return observeRestoreSettlement({ kind: "recipient", follower, isCurrent }, ctx);
            return undefined;
        },
        async waitForRestoreSettlement() { await Promise.all([...restoreSettlementTasks]); } };
}
export function createTelegramAssistantOutputAuthorityRuntime(deps) {
    const getCurrentTarget = () => {
        const preferred = deps.getPreferredTarget();
        if (preferred)
            return { ...preferred };
        const chatId = deps.getFallbackChatId();
        return chatId === undefined ? undefined : { chatId };
    };
    return {
        captureAuthority() {
            const target = getCurrentTarget();
            const directEpoch = deps.ownsDirect() ? deps.getDirectEpoch() : undefined;
            const followerGeneration = deps.isFollowerRegistered()
                ? deps.getFollowerGeneration()
                : undefined;
            return {
                transportStamp: deps.getTransportStamp(),
                route: directEpoch !== undefined
                    ? "direct"
                    : followerGeneration !== undefined
                        ? "follower"
                        : "none",
                directEpoch,
                followerGeneration,
                target,
            };
        },
        isAuthorityActive(authority) {
            if (!deps.isTransportStampActive(authority.transportStamp))
                return false;
            const target = getCurrentTarget();
            if (authority.target === undefined ||
                target?.chatId !== authority.target.chatId ||
                target?.threadId !== authority.target.threadId) {
                return false;
            }
            if (authority.route === "direct") {
                return (deps.ownsDirect() && deps.getDirectEpoch() === authority.directEpoch);
            }
            if (authority.route === "follower") {
                return (!deps.ownsDirect() &&
                    deps.isFollowerRegistered() &&
                    deps.getFollowerGeneration() === authority.followerGeneration);
            }
            return false;
        },
        canDeliver() {
            return deps.ownsDirect() || deps.isFollowerRegistered();
        },
    };
}
