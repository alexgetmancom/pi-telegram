/** Existing BotFlix CLI subscriptions and Telegram notifications. */
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { join } from "node:path";
import { homedir } from "node:os";
import { sendTelegramView, sendTelegramPhoto } from "../dist/api/delivery.js";
const executeFile = promisify(execFile);
const cinemaTarget = { chatId: -1003985826484, threadId: 3 };

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
