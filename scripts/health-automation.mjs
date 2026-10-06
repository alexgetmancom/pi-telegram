/** Health CLI owns Xiaomi/SQLite; this native Pi session owns cadence and delivery. */
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { readFileSync, writeFileSync, renameSync } from "node:fs";
import { join } from "node:path";
import { homedir } from "node:os";
import { sendTelegramView } from "../dist/api/delivery.js";
const executeFile = promisify(execFile);
export function healthWeeklyKey(now) {
  const parts = Object.fromEntries(new Intl.DateTimeFormat("en-GB", { timeZone: "Europe/Moscow", weekday: "short", year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", hourCycle: "h23" }).formatToParts(now).map(p => [p.type, p.value]));
  return parts.weekday === "Sun" && Number(parts.hour) >= 21 ? `${parts.year}-${parts.month}-${parts.day}` : null;
}
export function healthReportView(people) {
  const lines = ["Здоровье — последние 7 дней"];
  for (const name of Object.keys(people).sort()) {
    const summary = people[name];
    lines.push(`\n${name === "alex" ? "Алекс" : name === "maru" ? "Маша" : name}`);
    if (!summary?.metrics) { lines.push("Данные недоступны"); continue; }
    for (const [key, label, unit] of [["steps", "Шаги", "в день"], ["sleep", "Сон", "мин"], ["heart_rate_daily", "Пульс", "уд/мин"]]) {
      const row = summary.metrics[key]?.data?.[0];
      lines.push(`${label}: ${row?.average == null ? "нет данных" : `${Math.round(row.average)} ${unit}, дней: ${row.observed_days}`}`);
    }
    const failures = summary.sync?.filter(metric => metric.error).map(metric => metric.metric) ?? [];
    if (failures.length) lines.push(`Не обновились: ${failures.join(", ")}`);
  }
  lines.push("\nПропуски не считаются нулями. Оценки браслета не являются диагнозом.");
  return { text: lines.join("\n") };
}
export function startHealthAutomation(log) {
  const controller = new AbortController();
  const receiptPath = join(homedir(), ".local/share/family/health/weekly-delivery.json");
  let timer, running;
  const cli = async args => {
    let stdout;
    try { ({ stdout } = await executeFile(join(homedir(), ".local/bin/health"), args, { shell: false, signal: controller.signal, timeout: 1100000, maxBuffer: 8 * 1024 * 1024, env: { HOME: homedir(), PATH: "/usr/bin:/bin" } })); }
    catch (error) { if (!error.stdout) throw new Error("Health CLI execution failed"); stdout = error.stdout; }
    const result = JSON.parse(stdout);
    if (!result.ok) log("health_source_error", { error: result.error });
    return result;
  };
  const save = receipt => { const temporary = receiptPath + ".tmp"; writeFileSync(temporary, JSON.stringify(receipt) + "\n", { mode: 0o600 }); renameSync(temporary, receiptPath); };
  const poll = async () => {
    try {
      await cli(["tick"]);
      if (controller.signal.aborted) return;
      const week = healthWeeklyKey(new Date());
      if (week) {
        let receipt = {};
        try { receipt = JSON.parse(readFileSync(receiptPath, "utf8")); } catch (error) { if (error.code !== "ENOENT") throw error; }
        if (receipt.week !== week || receipt.state === "retry") {
          const result = await cli(["compare", "--period", "7d"]);
          if (!result.data || controller.signal.aborted) return;
          save({ week, state: "claimed" });
          const delivered = await sendTelegramView(healthReportView(result.data), { scope: { kind: "target", target: { chatId: -1003985826484, threadId: 359 } } });
          save({ week, state: delivered.ok ? "delivered" : delivered.reason === "commit-unknown" || delivered.partial ? "uncertain" : "retry" });
          log("health_weekly_delivery", { week, ok: delivered.ok, reason: delivered.reason });
        }
      }
    } catch (error) { if (!controller.signal.aborted) log("health_automation_error", { error: error.message }); }
    finally { if (!controller.signal.aborted) { timer = setTimeout(() => { running = poll(); }, 60000); timer.unref(); } }
  };
  timer = setTimeout(() => { running = poll(); }, 1000); timer.unref();
  return async () => { controller.abort(); clearTimeout(timer); await running; };
}
