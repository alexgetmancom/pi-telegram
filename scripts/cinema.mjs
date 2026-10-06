/** Cinema's native Pi session and bounded CLI capability; the existing bridge owns Telegram. */
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { existsSync, readFileSync, writeFileSync, renameSync, unlinkSync, mkdtempSync, rmSync } from "node:fs";
import { createHash, randomUUID } from "node:crypto";
import { join } from "node:path";
import { homedir, tmpdir } from "node:os";
import { Type } from "@sinclair/typebox";
import telegram from "../dist/index.js";
import { webSearchTool, webFetchTool } from "./web.mjs";
import { createAgentSessionRuntime, createAgentSessionServices, createAgentSessionFromServices, SessionManager } from "@earendil-works/pi-coding-agent";
import { sendTelegramView, sendTelegramPhoto } from "../dist/api/delivery.js";

const executeFile = promisify(execFile);
export const cinemaTarget = Object.freeze({ chatId: -1003985826484, threadId: 3 });
const actions = ["search", "series", "episodes", "releases", "download", "download-season", "downloads", "stop", "remove", "library", "item", "poster", "refresh", "subscribe", "unsubscribe", "subscriptions", "schedule", "history", "stats", "disk", "diagnostics", "check"];
const sources = ["all", "lostfilm", "rutor", "nnm", "rutracker"];
const memoryFiles = ["alex.md", "maru.md", "watchlist.md"];
const cinemaTools = ["botflix", "family_memory", "web_search", "web_fetch", "telegram_attach"];
const cinemaArtifacts = new Set();

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
    case "subscribe":
      if (!["1080p", "720p", "SD"].includes(p.quality ?? "1080p")) throw new Error("Invalid quality");
      args.push("--quality", p.quality ?? "1080p"); seriesURL(); break;
    case "unsubscribe": case "series": seriesURL(); break;
    case "history": limit(); break;
    case "stats":
      if (!["day", "week", "month", "last"].includes(p.period ?? "week")) throw new Error("Invalid period");
      args.push("--period", p.period ?? "week"); break;
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
  name: "botflix", label: "BotFlix", description: "Search trackers, browse LostFilm, manage torrents, subscriptions, schedule, download history, viewing stats, disk space and diagnostics. No host shell or files. Removing a torrent keeps its files.",
  parameters: Type.Object({
    action: Type.Union(actions.map(v => Type.Literal(v))),
    source: Type.Optional(Type.Union(sources.map(v => Type.Literal(v)))),
    query: Type.Optional(Type.String()), url: Type.Optional(Type.String()), code: Type.Optional(Type.String()),
    season: Type.Optional(Type.Integer({ minimum: 1, maximum: 99 })), series: Type.Optional(Type.String()),
    quality: Type.Optional(Type.Union([Type.Literal("1080p"), Type.Literal("720p"), Type.Literal("SD")])),
    limit: Type.Optional(Type.Integer({ minimum: 1, maximum: 50 })), hash: Type.Optional(Type.String()),
    itemId: Type.Optional(Type.String()), itemType: Type.Optional(Type.Union([Type.Literal("Movie"), Type.Literal("Series"), Type.Literal("Episode")])),
    recent: Type.Optional(Type.Boolean()),
    period: Type.Optional(Type.Union([Type.Literal("day"), Type.Literal("week"), Type.Literal("month"), Type.Literal("last")])),
  }, { additionalProperties: false }),
  async execute(_id, parameters, signal) {
    let poster;
    let args;
    if (parameters.action === "poster") {
      if (!/^[a-f\d]{32}$/i.test(parameters.itemId ?? "")) throw new Error("Invalid poster item id");
      poster = join(mkdtempSync(join(tmpdir(), "cinema-artifact-")), "poster.jpg");
      args = ["poster", "--output", poster, parameters.itemId];
    } else { args = cinemaArguments(parameters); }
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
    if (poster && result.ok) { cinemaArtifacts.add(poster); result.poster_path = poster; }
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
      extensionFactories: [pi => telegram(pi, { forumTarget: cinemaTarget }), pi => {
        pi.on("tool_call", event => {
          if (event.toolName === "telegram_attach" &&
              (!Array.isArray(event.input.paths) || event.input.paths.some(path => !cinemaArtifacts.has(path)))) {
            return { block: true, reason: "Only posters created by botflix in this process may be attached; arbitrary files are unavailable." };
          }
        });
        pi.on("before_agent_start", event => ({ systemPrompt: event.systemPrompt + "\n<family_preferences>\n" +
          memoryFiles.map(name =>
            `${name}:\n${readFileSync(join(homedir(), ".local/share/family", name), "utf8")}`).join("\n\n") +
          "\n</family_preferences>" }));
      }],
    } });
    const result = await createAgentSessionFromServices({ services, sessionManager, sessionStartEvent,
      tools: cinemaTools, customTools: [botflixTool, familyMemoryTool, webSearchTool, webFetchTool] });
    if (result.session.getActiveToolNames().some(name => !cinemaTools.includes(name)) || result.extensionsResult.extensions.length !== 2 || result.extensionsResult.errors.length) throw new Error("Cinema tool isolation failed");
    const file = sessionManager.getSessionFile();
    if (file && !existsSync(file)) {
      writeFileSync(file, [sessionManager.getHeader(), ...sessionManager.getEntries()].map(entry => JSON.stringify(entry)).join("\n") + "\n", { flag: "wx", mode: 0o600 });
      sessionManager.setSessionFile(file);
    }
    return { ...result, services, diagnostics: services.diagnostics };
  }, { cwd, agentDir, sessionManager: SessionManager.continueRecent(cwd, sessionDir) });
}

export function mediaEventView(event) {
  const escape = text => String(text).replace(/[&<>]/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" })[c]);
  const item = event.item;
  return { text: escape(event.text.slice(0, 220)) + (item ? `\n\n<b>${escape(item.Name.slice(0, 120))}</b>\n${escape((item.Overview ?? "").slice(0, 350))}` : ""),
    parseMode: "html", ...(item?.watch_url ? { replyMarkup: { inline_keyboard: [[{ text: "▶ Открыть в Jellyfin", url: item.watch_url }]] } } : {}) };
}

export function startMediaAutomation(log) {
  const scope = { kind: "target", target: cinemaTarget };
  const mediaController = new AbortController();
  let mediaTimer, mediaRun;
  const mediaCLI = async args => {
    const { stdout } = await executeFile(join(homedir(), ".local/bin/botflix"), args, {
      shell: false, signal: mediaController.signal, timeout: 180000, maxBuffer: 4 * 1024 * 1024,
      env: { HOME: homedir(), PATH: "/usr/bin:/bin" },
    });
    const result = JSON.parse(stdout);
    if (!result.ok) throw new Error(result.error || "Media command failed");
    return result.data;
  };
  const pollMedia = async () => {
    try {
      const tick = await mediaCLI(["tick"]);
      if (tick.error) log("media_tick_error", { error: tick.error });
      const events = await mediaCLI(["events"]);
      for (const event of events) {
        if (mediaController.signal.aborted) return;
        if (event.delivery) continue;
        const view = mediaEventView(event);
        let result;
        if (event.item?.ImageTags?.Primary) {
          const dir = mkdtempSync(join(tmpdir(), "botflix-poster-"));
          const path = join(dir, "poster.jpg");
          try {
            try { await mediaCLI(["poster", "--output", path, event.item.Id]); }
            catch (error) { if (mediaController.signal.aborted) return; log("media_poster_error", { id: event.id, error: error.message }); }
            if (mediaController.signal.aborted) return;
            await mediaCLI(["claim", String(event.id)]);
            result = existsSync(path) ? await sendTelegramPhoto(path, view, { scope }) : await sendTelegramView(view, { scope });
          } finally { rmSync(dir, { recursive: true }); }
        } else { await mediaCLI(["claim", String(event.id)]); result = await sendTelegramView(view, { scope }); }
        if (!result.ok) {
          if (result.reason !== "commit-unknown" && !result.partial) await mediaCLI(["retry-event", String(event.id)]);
          throw new Error(`Media delivery failed: ${result.reason}; event ${event.id}`);
        }
        await mediaCLI(["ack", String(event.id)]);
        log("media_event_delivered", { id: event.id, kind: event.kind, topic: cinemaTarget.threadId });
      }
    } catch (error) {
      if (!mediaController.signal.aborted) log("media_automation_error", { error: error.message });
    } finally {
      if (!mediaController.signal.aborted) { mediaTimer = setTimeout(() => { mediaRun = pollMedia(); }, 60000); mediaTimer.unref(); }
    }
  };
  mediaTimer = setTimeout(() => { mediaRun = pollMedia(); }, 1000); mediaTimer.unref();
  return async () => { mediaController.abort(); clearTimeout(mediaTimer); await mediaRun; };
}
