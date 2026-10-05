/** Cinema's native Pi session and bounded CLI capability; the existing bridge owns Telegram. */
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { existsSync, readFileSync, writeFileSync, renameSync, unlinkSync } from "node:fs";
import { createHash, randomUUID } from "node:crypto";
import { join } from "node:path";
import { homedir } from "node:os";
import { Type } from "@sinclair/typebox";
import { createAgentSessionRuntime, createAgentSessionServices, createAgentSessionFromServices, SessionManager } from "@earendil-works/pi-coding-agent";
import { registerTelegramUpdateHandler } from "../dist/api/updates.js";
import { transcribeTelegramVoiceMessage } from "../dist/api/inbound.js";
import { registerTelegramDeliveryTarget, sendTelegramView, sendTelegramChatAction } from "../dist/api/delivery.js";

const executeFile = promisify(execFile);
export const cinemaTarget = Object.freeze({ chatId: -1003985826484, threadId: 3 });
const actions = ["search", "series", "episodes", "releases", "download", "download-season", "downloads", "stop", "remove", "library", "item", "refresh"];
const sources = ["all", "lostfilm", "rutor", "nnm", "rutracker"];
const memoryFiles = ["alex.md", "maru.md", "watchlist.md"];
const cinemaTools = ["botflix", "family_memory"];
const help = "Кино: ищу фильмы и сериалы, управляю загрузками и проверяю Jellyfin.\n/new — новая кино-сессия\n/compact — сжать историю\n/stop — остановить ответ и очистить очередь\n/model — модель; /model provider/model — переключить";

export function cinemaArguments(p) {
  if (!actions.includes(p.action)) throw new Error("Unsupported BotFlix action");
  const args = [p.action];
  const required = name => {
    const value = p[name];
    if (typeof value !== "string" || !value.trim() || value.length > 8192 || /[\x00-\x1f]/.test(value)) throw new Error(`Invalid ${name}`);
    return value;
  };
  const limit = () => {
    if (p.limit !== undefined) {
      if (!Number.isInteger(p.limit) || p.limit < 1 || p.limit > 50) throw new Error("Invalid limit");
      args.push("--limit", String(p.limit));
    }
  };
  const season = () => {
    if (!Number.isInteger(p.season) || p.season < 1 || p.season > 99) throw new Error("Invalid season");
    args.push("--season", String(p.season));
  };
  const source = () => {
    if (p.source !== undefined) {
      if (!sources.includes(p.source) || p.action === "download" && p.source === "all") throw new Error("Invalid source");
      args.push("--source", p.source);
    }
  };
  const seriesURL = () => {
    const value = required("url");
    const u = new URL(value, "https://www.lostfilm.tv");
    if (u.protocol !== "https:" || u.username || u.password ||
        !["www.lostfilm.tv", "lostfilm.tv"].includes(u.hostname) ||
        !/^\/series\/[A-Za-z0-9_]+(?:\/seasons)?\/?$/.test(u.pathname) || u.search || u.hash) throw new Error("Invalid series URL");
    args.push(u.href);
  };
  switch (p.action) {
    case "search": source(); limit(); args.push(required("query")); break;
    case "series": seriesURL(); break;
    case "episodes": season(); seriesURL(); break;
    case "releases": {
      const code = required("code");
      if (!/^\d{7,9}$/.test(code)) throw new Error("Invalid episode code");
      args.push(code); break;
    }
    case "download-season":
      season();
      if (!["1080p", "720p", "SD"].includes(p.quality ?? "1080p")) throw new Error("Invalid quality");
      args.push("--quality", p.quality ?? "1080p"); seriesURL(); break;
    case "download": {
      source();
      if (p.series !== undefined || p.season !== undefined) { args.push("--series", required("series")); season(); }
      const value = required("url");
      const u = new URL(value);
      if (u.protocol === "magnet:") {
        if (!/^urn:btih:(?:[a-f\d]{40}|[a-z2-7]{32})$/i.test(u.searchParams.get("xt") ?? "")) throw new Error("Invalid magnet");
      } else if (u.protocol !== "https:" || u.username || u.password || u.port ||
          !["lostfilm.tv", "tracktor.site", "rutor.info", "rutor.is", "nnmclub.to", "rutracker.org"].some(host => u.hostname === host || u.hostname.endsWith(`.${host}`))) {
        throw new Error("Only tracker HTTPS URLs and torrent magnets are allowed; local files are unavailable");
      }
      args.push(value); break;
    }
    case "downloads": limit(); if (p.hash !== undefined) args.push("--hash", hash(p.hash)); break;
    case "stop": case "remove": args.push(hash(required("hash"))); break;
    case "library":
      limit(); if (p.recent) args.push("--recent");
      if (p.itemType !== undefined) {
        if (!["Movie", "Series", "Episode"].includes(p.itemType)) throw new Error("Invalid item type");
        args.push("--type", p.itemType);
      }
      if (p.query !== undefined) args.push(required("query")); break;
    case "item": {
      const id = required("itemId"); if (!/^[a-f\d]{32}$/i.test(id)) throw new Error("Invalid item id");
      args.push(id); break;
    }
  }
  return args;
}

function hash(value) {
  if (typeof value !== "string" || !/^[a-f\d]{40}$/i.test(value)) throw new Error("Invalid exact torrent hash");
  return value.toLowerCase();
}

export const botflixTool = {
  name: "botflix", label: "BotFlix", description: "Search trackers, browse LostFilm, manage torrents and query Jellyfin. No host shell or files. Removing a torrent keeps its files.",
  parameters: Type.Object({
    action: Type.Union(actions.map(v => Type.Literal(v))),
    source: Type.Optional(Type.Union(sources.map(v => Type.Literal(v)))),
    query: Type.Optional(Type.String()), url: Type.Optional(Type.String()), code: Type.Optional(Type.String()),
    season: Type.Optional(Type.Integer({ minimum: 1, maximum: 99 })), series: Type.Optional(Type.String()),
    quality: Type.Optional(Type.Union([Type.Literal("1080p"), Type.Literal("720p"), Type.Literal("SD")])),
    limit: Type.Optional(Type.Integer({ minimum: 1, maximum: 50 })), hash: Type.Optional(Type.String()),
    itemId: Type.Optional(Type.String()), itemType: Type.Optional(Type.Union([Type.Literal("Movie"), Type.Literal("Series"), Type.Literal("Episode")])),
    recent: Type.Optional(Type.Boolean()),
  }, { additionalProperties: false }),
  async execute(_id, parameters, signal) {
    const args = cinemaArguments(parameters);
    let stdout;
    try {
      ({ stdout } = await executeFile(join(homedir(), ".local/bin/botflix"), args, {
        shell: false, signal, timeout: 180000, maxBuffer: 2 * 1024 * 1024,
        cwd: join(homedir(), "projects/home/cli-botlix"), env: { HOME: homedir(), PATH: "/usr/bin:/bin" },
      }));
    } catch (error) {
      if (!error.stdout) throw new Error(signal?.aborted ? "BotFlix cancelled" : "BotFlix failed; no usable JSON result");
      stdout = error.stdout;
    }
    const result = JSON.parse(stdout);
    return { content: [{ type: "text", text: JSON.stringify(result) }], details: { ok: result.ok }, isError: !result.ok };
  },
};

export const familyMemoryTool = {
  name: "family_memory", label: "Семейная память",
  description: "Read or update only alex.md, maru.md and watchlist.md. Read before writing, preserve existing facts, save only confirmed participant statements or requested watchlist changes. No other paths are accessible.",
  parameters: Type.Object({
    action: Type.Union([Type.Literal("read"), Type.Literal("write")]),
    file: Type.Union(memoryFiles.map(file => Type.Literal(file))),
    content: Type.Optional(Type.String()), expectedSha256: Type.Optional(Type.String()),
  }, { additionalProperties: false }),
  async execute(_id, p) {
    if (!memoryFiles.includes(p.file) || !["read", "write"].includes(p.action)) throw new Error("Unsupported family memory file or action");
    const path = join(homedir(), ".local/share/family", p.file);
    const before = readFileSync(path, "utf8");
    const digest = value => createHash("sha256").update(value).digest("hex");
    if (p.action === "read") return { content: [{ type: "text", text: JSON.stringify({ file: p.file, content: before, sha256: digest(before) }) }], details: {} };
    if (p.expectedSha256 !== digest(before)) throw new Error("Family memory changed; read the current file before writing");
    if (typeof p.content !== "string" || !p.content.trim() || p.content.includes("\0") ||
        p.content.length > (p.file === "watchlist.md" ? 60000 : 6000)) throw new Error("Invalid family memory contents");
    const temporary = `${path}.${randomUUID()}.tmp`;
    try {
      writeFileSync(temporary, p.content, { flag: "wx", mode: 0o600 });
      if (digest(readFileSync(path, "utf8")) !== p.expectedSha256) throw new Error("Family memory changed during writing; reread it");
      renameSync(temporary, path);
    } finally { if (existsSync(temporary)) unlinkSync(temporary); }
    if (readFileSync(path, "utf8") !== p.content) throw new Error("Family memory write could not be verified");
    return { content: [{ type: "text", text: JSON.stringify({ file: p.file, saved: true, sha256: digest(p.content) }) }], details: {} };
  },
};

export async function createCinemaRuntime(root, agentDir, sessionDir) {
  const cwd = join(homedir(), "projects/home/cli-botlix");
  return createAgentSessionRuntime(async ({ cwd, agentDir, sessionManager, sessionStartEvent }) => {
    const services = await createAgentSessionServices({ cwd, agentDir, resourceLoaderOptions: {
      noExtensions: true, noSkills: true, noContextFiles: true, noPromptTemplates: true, noThemes: true,
      systemPrompt: readFileSync(join(root, "agent/CINEMA.md"), "utf8"),
      extensionFactories: [pi => {
        pi.on("before_agent_start", event => ({ systemPrompt: event.systemPrompt + "\n<family_preferences>\n" +
          memoryFiles.map(name =>
            `${name}:\n${readFileSync(join(homedir(), ".local/share/family", name), "utf8")}`).join("\n\n") +
          "\n</family_preferences>" }));
      }],
    } });
    const result = await createAgentSessionFromServices({ services, sessionManager, sessionStartEvent,
      tools: cinemaTools, customTools: [botflixTool, familyMemoryTool] });
    if (result.session.getActiveToolNames().sort().join() !== cinemaTools.join() || result.extensionsResult.extensions.length !== 1 || result.extensionsResult.errors.length) throw new Error("Cinema tool isolation failed");
    await result.session.bindExtensions({ mode: "rpc" });
    if (result.session.getActiveToolNames().sort().join() !== cinemaTools.join()) throw new Error("Cinema tool isolation changed after binding");
    const file = sessionManager.getSessionFile();
    if (file && !existsSync(file)) {
      writeFileSync(file, [sessionManager.getHeader(), ...sessionManager.getEntries()].map(entry => JSON.stringify(entry)).join("\n") + "\n", { flag: "wx", mode: 0o600 });
      sessionManager.setSessionFile(file);
    }
    return { ...result, services, diagnostics: services.diagnostics };
  }, { cwd, agentDir, sessionManager: SessionManager.continueRecent(cwd, sessionDir) });
}

export function cinemaMessage(update) {
  const m = update?.message;
  if (!m || m.chat?.type !== "supergroup" || m.chat.id !== cinemaTarget.chatId || m.message_thread_id !== cinemaTarget.threadId ||
      !m.from || m.from.is_bot || !Number.isSafeInteger(m.from.id) || m.from.id <= 0) return;
  if (m.text?.match(/^\/\w+@/i) && !m.text.match(/^\/\w+@getmanmaru_bot(?:\s|$)/i)) return;
  return m;
}

export function connectCinema(runtime, log) {
  const unregisterTarget = registerTelegramDeliveryTarget(cinemaTarget);
  const scope = { kind: "target", target: cinemaTarget };
  let queue = Promise.resolve(), generation = 0;
  const send = async text => {
    const result = await sendTelegramView({ text, parseMode: "markdown" }, { scope });
    if (!result.ok) throw new Error(`Cinema delivery failed: ${result.reason}`);
  };
  const unregisterUpdates = registerTelegramUpdateHandler((update, execution) => {
    const message = cinemaMessage(update);
    if (!message) return "pass";
    if (execution && !execution.isCurrent()) return "consume";
    const command = message.text?.match(/^\/(\w+)(?:@getmanmaru_bot)?(?:\s+(.*))?$/is);
    if (command?.[1].toLowerCase() === "stop" || command?.[1].toLowerCase() === "abort") {
      generation++;
      runtime.session.clearQueue();
      runtime.session.abort().then(() => send("Остановлено. Очередь кино очищена.")).catch(error => log("cinema_error", { error: error.message }));
      return "consume";
    }
    const acceptedGeneration = generation;
    queue = queue.then(async () => {
      if (acceptedGeneration !== generation) return;
      const typing = setInterval(() => { sendTelegramChatAction("typing", { scope }).catch(() => {}); }, 4000);
      try {
        const text = message.text ?? (message.voice || message.audio
          ? await transcribeTelegramVoiceMessage(message, runtime.services.cwd) : undefined);
        if (acceptedGeneration !== generation) return;
        if (!text) { await send("Отправь текст или голосовое. Остальные файлы пока доступны в теме AI."); return; }
        if (command) {
          switch (command[1].toLowerCase()) {
            case "new": await runtime.newSession(); await send("Начата новая кино-сессия."); return;
            case "compact": await runtime.session.compact(); await send("История кино сжата."); return;
            case "model": {
              if (command[2]) {
                const split = command[2].trim().indexOf("/");
                const provider = command[2].trim().slice(0, split), id = command[2].trim().slice(split + 1);
                const model = runtime.services.modelRuntime.getModel(provider, id);
                if (split < 1 || !model || !(await runtime.services.modelRuntime.getAvailable()).some(m => m.provider === provider && m.id === id)) { await send("Модель недоступна. Формат: /model provider/model"); return; }
                await runtime.session.setModel(model, { persist: false });
              }
              await send(`Модель кино: ${runtime.session.model.provider}/${runtime.session.model.id}`); return;
            }
            default: await send(help); return;
          }
        }
        log("cinema_turn_started", { sessionId: runtime.session.sessionId, user: message.from.id, topic: cinemaTarget.threadId });
        await runtime.session.prompt(`user=${message.from.id} name=${JSON.stringify(message.from.first_name ?? "")}\n${text}`);
        if (acceptedGeneration !== generation) return;
        const last = runtime.session.messages.findLast(m => m.role === "assistant");
        if (last?.errorMessage || last?.stopReason === "error") throw new Error("Cinema model request failed");
        await send(runtime.session.getLastAssistantText() || "Ответ пуст. Повтори запрос.");
        log("cinema_turn_completed", { sessionId: runtime.session.sessionId, topic: cinemaTarget.threadId });
      } finally { clearInterval(typing); }
    }).catch(async error => {
      log("cinema_error", { error: error.message });
      await send("Не удалось завершить запрос в «Кино». Подробности можно проверить в теме AI.").catch(() => {});
    });
    return "consume";
  });
  return async () => { generation++; unregisterUpdates(); unregisterTarget(); await runtime.dispose(); };
}
