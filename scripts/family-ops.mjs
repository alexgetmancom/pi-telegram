/** Family CLI backup delivery and freshness, run by the existing Pi leader. */
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { mkdirSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { basename, dirname, join } from "node:path";
import { homedir } from "node:os";
import { sendTelegramDocument, sendTelegramView } from "../dist/api/delivery.js";

const executeFile = promisify(execFile);
const commands = ["botflix", "health", "check-radar"];
const statePath = join(homedir(), ".local/state/family/pi-ops.json");
const scope = { kind: "owner" };

export function moscowClock(now) {
  const p = Object.fromEntries(new Intl.DateTimeFormat("en-GB", {
    timeZone: "Europe/Moscow", year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", hourCycle: "h23",
  }).formatToParts(now).map(part => [part.type, part.value]));
  return { day: `${p.year}-${p.month}-${p.day}`, hour: `${p.year}-${p.month}-${p.day}T${p.hour}`, backupDue: Number(p.hour) >= 4 };
}

export function staleText(command, sources) {
  return `${command}: устарели ${sources.filter(source => source.stale).map(source =>
    `${source.name} (${source.age_seconds ?? "нет данных"} с${source.error ? `; ${source.error}` : ""})`).join(", ")}`;
}

export function startFamilyOps(log) {
  const controller = new AbortController();
  let timer, running;
  let state = {};
  try { state = JSON.parse(readFileSync(statePath, "utf8")); }
  catch (error) { if (error.code !== "ENOENT") throw error; }
  const save = () => {
    mkdirSync(dirname(statePath), { recursive: true, mode: 0o700 });
    const tmp = `${statePath}.tmp`;
    writeFileSync(tmp, JSON.stringify(state) + "\n", { mode: 0o600 });
    renameSync(tmp, statePath);
  };
  const cli = async (command, args) => {
    let stdout;
    try {
      ({ stdout } = await executeFile(join(homedir(), ".local/bin", command), args, {
        shell: false, signal: controller.signal, timeout: 600000, maxBuffer: 8 * 1024 * 1024,
        env: { HOME: homedir(), PATH: "/usr/bin:/bin" },
      }));
    } catch (error) {
      if (!error.stdout) throw error;
      stdout = error.stdout;
    }
    return JSON.parse(stdout);
  };
  const sendText = async text => {
    const result = await sendTelegramView({ text }, { scope });
    if (!result.ok) throw new Error(`Owner delivery failed: ${result.reason}`);
  };
  const backup = async day => {
    if (state.backupDay !== day) {
      state.backupDay = day;
      state.backups = {};
      save();
    }
    for (const command of commands) {
      if (controller.signal.aborted) return;
      let task = state.backups[command];
      if (!task) {
        try {
          const result = await cli(command, ["backup", "--if-older", "20"]);
          if (result.ok && !result.data?.skipped && !Array.isArray(result.data?.files)) throw new Error("backup returned no file list");
          task = result.ok ? { files: result.data?.skipped ? [] : result.data?.files ?? [], sent: [] }
            : { files: [], sent: [], error: result.error || "backup failed" };
        } catch (error) { task = { files: [], sent: [], error: error.message }; }
        state.backups[command] = task;
        save();
      }
      if (task.error && !task.errorSent) {
        await sendText(`${command} backup: ${task.error}`);
        task.errorSent = true;
        save();
      }
      if (task.sending && !task.uncertainSent) {
        await sendText(`${command} backup: отправка SHA-256 ${task.sending} не подтверждена; проверьте документ вручную.`);
        task.uncertainSent = true;
        save();
      }
      for (const file of task.files) {
        if (task.sent.includes(file.sha256) || task.sending === file.sha256) continue;
        task.sending = file.sha256;
        save();
        const caption = `${command} · ${day}\nSHA-256: ${file.sha256}`;
        const result = await sendTelegramDocument(file.path, { text: caption }, { scope });
        if (!result.ok) {
          log("family_backup_delivery_error", { command, file: basename(file.path), reason: result.reason });
          if (result.reason !== "commit-unknown") { delete task.sending; save(); }
          break;
        }
        task.sent.push(file.sha256);
        delete task.sending;
        save();
      }
    }
  };
  const freshness = async hour => {
    if (state.freshnessHour === hour) return;
    for (const command of commands) {
      if (controller.signal.aborted) return;
      try {
        const result = await cli(command, ["freshness"]);
        const stale = result.ok ? Boolean(result.data?.stale) : true;
        const previous = state.freshness?.[command];
        if (previous === false && stale) await sendText(result.ok ? staleText(command, result.data.sources) : `${command}: проверка свежести: ${result.error}`);
        if (previous === true && !stale) await sendText(`${command}: восстановлено`);
        state.freshness ??= {};
        state.freshness[command] = stale;
        save();
      } catch (error) { log("family_freshness_error", { command, error: error.message }); }
    }
    state.freshnessHour = hour;
    save();
  };
  const poll = async () => {
    try {
      const clock = moscowClock(new Date());
      if (clock.backupDue) await backup(clock.day);
      await freshness(clock.hour);
    } catch (error) { if (!controller.signal.aborted) log("family_ops_error", { error: error.message }); }
    finally { if (!controller.signal.aborted) { timer = setTimeout(() => { running = poll(); }, 60000); timer.unref(); } }
  };
  timer = setTimeout(() => { running = poll(); }, 1000); timer.unref();
  return async () => { controller.abort(); clearTimeout(timer); await running; };
}
