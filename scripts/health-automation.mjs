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
export function healthReportView(people, shopping, journals = {}) {
  const lines = ["Здоровье — последние 7 дней"];
  for (const name of [...new Set([...Object.keys(people), ...Object.keys(journals)])].sort()) {
    const summary = people[name];
    lines.push(`\n${name === "alex" ? "Алекс" : name === "maru" ? "Маша" : name}`);
    if (!summary?.metrics) lines.push("Данные браслета недоступны");
    else for (const [key, label, unit] of [["steps", "Шаги", "в день"], ["sleep", "Сон", "мин"], ["heart_rate_daily", "Пульс", "уд/мин"]]) {
      const row = summary.metrics[key]?.data?.[0];
      lines.push(`${label}: ${row?.average == null ? "нет данных" : `${Math.round(row.average)} ${unit}, дней: ${row.observed_days}`}`);
    }
    const failures = summary?.sync?.filter(metric => metric.error).map(metric => metric.metric) ?? [];
    if (failures.length) lines.push(`Не обновились: ${failures.join(", ")}`);
    const journal = journals[name];
    if (journal?.training) lines.push(`Записано тренировок: ${journal.training.sessions.length}`);
    if (journal?.nutrition) {
      const food = journal.nutrition;
      lines.push(`Дневник питания: ${food.recorded_days} дней, полностью: ${food.complete_days}`);
      if (food.average_kcal_complete_days != null) lines.push(`Средние калории: ${Math.round(food.average_kcal_complete_days)} ккал за ${food.days_with_complete_kcal} полностью заполненных дней`);
      else lines.push("Средние калории: нет полных данных");
    }
  }
  if (shopping) {
    lines.push(`\nПокупки продуктов за 7 дней: ${shopping.receipts} чеков`);
    lines.push(`Распознаны продукты: ${shopping.classified_food_items} позиций, ${Math.round(shopping.classified_food_sum_rub)} ₽`);
    if (shopping.uncertain_grocery_items) lines.push(`Ещё ${shopping.uncertain_grocery_items} позиций в продуктовых магазинах требуют проверки.`);
  }
  lines.push("\nПокупки не равны съеденному. Пропуски измерений не считаются нулями. Оценки браслета не являются диагнозом.");
  return { text: lines.join("\n") };
}
export function startHealthAutomation(log) {
  const controller = new AbortController();
  const receiptPath = join(homedir(), ".local/share/family/health/weekly-delivery.json");
  let timer, running;
  const cli = async (binary, args) => {
    let stdout;
    try { ({ stdout } = await executeFile(join(homedir(), `.local/bin/${binary}`), args, { shell: false, signal: controller.signal, timeout: 1100000, maxBuffer: 8 * 1024 * 1024, env: { HOME: homedir(), PATH: "/usr/bin:/bin" } })); }
    catch (error) { if (!error.stdout) throw new Error("Family CLI execution failed"); stdout = error.stdout; }
    const result = JSON.parse(stdout);
    if (!result.ok) log("family_source_error", { source: binary, error: result.error });
    return result;
  };
  const save = receipt => { const temporary = receiptPath + ".tmp"; writeFileSync(temporary, JSON.stringify(receipt) + "\n", { mode: 0o600 }); renameSync(temporary, receiptPath); };
  const poll = async () => {
    try {
      await cli("health", ["tick"]);
      try { await cli("check-radar", ["tick"]); } catch (error) { if (!controller.signal.aborted) log("shopping_sync_error", { error: error.message }); }
      if (controller.signal.aborted) return;
      const week = healthWeeklyKey(new Date());
      if (week) {
        let receipt = {};
        try { receipt = JSON.parse(readFileSync(receiptPath, "utf8")); } catch (error) { if (error.code !== "ENOENT") throw error; }
        if (receipt.week !== week || receipt.state === "retry") {
          const result = await cli("health", ["compare", "--period", "7d"]);
          let shopping;
          try { shopping = (await cli("check-radar", ["shopping", "--period", "7d"])).data; }
          catch (error) { log("shopping_report_error", { error: error.message }); }
          const journals = {};
          await Promise.all(["alex", "maru"].map(async person => {
            const entry = journals[person] = {};
            await Promise.all([
              cli("health", ["training", "list", "--person", person, "--period", "7d"]).then(value => { entry.training = value.data; }).catch(error => log("training_report_error", { person, error: error.message })),
              cli("health", ["nutrition", "summary", "--person", person, "--period", "7d"]).then(value => { entry.nutrition = value.data; }).catch(error => log("nutrition_report_error", { person, error: error.message })),
            ]);
          }));
          if (!result.data || controller.signal.aborted) return;
          save({ week, state: "claimed" });
          const delivered = await sendTelegramView(healthReportView(result.data, shopping, journals), { scope: { kind: "target", target: { chatId: -1003985826484, threadId: 359 } } });
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
