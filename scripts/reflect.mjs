#!/usr/bin/env node

/** Daily family-memory reflection over every native Telegram session, without Telegram or model tools; the host supplies read-only CLI evidence. */
import { readFile, readdir, mkdir, writeFile, rename } from "node:fs/promises";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { homedir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { createAgentSession, DefaultResourceLoader, getAgentDir, ModelRuntime, SessionManager } from "@earendil-works/pi-coding-agent";

const names = ["alex.md", "maru.md", "watchlist.md"];
const healthNames = ["health/alex.md", "health/alex-training.md", "health/alex-nutrition.md",
  "health/maru.md", "health/maru-training.md", "health/maru-nutrition.md", "health/notes.md"];

export function reflectionWindow(date, now = new Date()) {
  date ??= new Date(now.getTime() + 3 * 3600000 - 86400000).toISOString().slice(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) throw new Error("Use --date YYYY-MM-DD");
  const start = Date.parse(`${date}T00:00:00+03:00`);
  if (!Number.isFinite(start) || new Date(start + 3 * 3600000).toISOString().slice(0, 10) !== date) throw new Error("Invalid date");
  const end = Math.min(start + 86400000, now.getTime());
  if (end <= start) throw new Error("The requested day has not started");
  return { date, start, end, partial: end < start + 86400000 };
}

export function redact(text) {
  for (const [key, value] of Object.entries(process.env)) {
    if (value && value.length >= 12 && /TOKEN|SECRET|PASSWORD|API_KEY/.test(key)) text = text.replaceAll(value, "[redacted]");
  }
  return text.replace(/\bsk-[A-Za-z0-9_-]{16,}|\b\d{8,12}:[A-Za-z0-9_-]{30,}|\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g, "[redacted]")
    .replace(/("(?:access(?:_?token)?|refresh(?:_?token)?|api_?key|password|token|cookie|secret|authorization)"\s*:\s*")[^"]+/gi, "$1[redacted]")
    .replace(/(Bearer\s+)[A-Za-z0-9_.-]+/gi, "$1[redacted]");
}

export async function collectTranscript(directory, window) {
  const messages = [];
  for (const name of (await readdir(directory)).filter(name => name.endsWith(".jsonl")).sort()) {
    const raw = await readFile(join(directory, name), "utf8");
    const lines = raw.split("\n");
    if (!raw.endsWith("\n")) lines.pop(); // An active session may be halfway through appending its last line.
    for (const line of lines.filter(Boolean)) {
      const entry = JSON.parse(line);
      const timestamp = Date.parse(entry.timestamp);
      const message = entry.message;
      if (entry.type !== "message" || timestamp < window.start || timestamp >= window.end || !Number.isFinite(timestamp) ||
          !["user", "assistant"].includes(message?.role) ||
          message.role === "assistant" && (message.errorMessage || message.stopReason && message.stopReason !== "stop" ||
            Array.isArray(message.content) && message.content.some(block => block.type === "toolCall"))) continue;
      const content = typeof message.content === "string" ? message.content : (message.content ?? []).flatMap(block => {
        if (block.type === "text") return [block.text];
        return []; // Only conversational text is family-memory evidence.
      }).join("\n");
      if (content.trim()) messages.push({ source: `${name}#${entry.id}`, timestamp: entry.timestamp,
        role: message.role, content: redact(content) });
    }
  }
  return messages.sort((a, b) => a.timestamp.localeCompare(b.timestamp));
}

const execFileAsync = promisify(execFile);

async function readCLI(command, args) {
  let stdout, failed = false;
  try {
    ({ stdout } = await execFileAsync(join(homedir(), ".local/bin", command), args,
      { timeout: 60000, maxBuffer: 8 * 1024 * 1024 }));
  } catch (error) {
    // A failed source must never masquerade as an empty journal.
    failed = true;
    stdout = error.stdout;
    if (!stdout) throw new Error(`CLI read failed: ${command} ${args.join(" ")} (exit ${error.code ?? "unknown"})`);
  }
  let result;
  try { result = JSON.parse(stdout); }
  catch { throw new Error(`Invalid CLI JSON: ${command} ${args.join(" ")}`); }
  if (failed && result.ok === true) throw new Error(`CLI read failed despite successful JSON: ${command} ${args.join(" ")}`);
  return result;
}

export async function collectCLIEvidence(run = readCLI) {
  const reads = [];
  const read = async (command, args) => {
    const source = `${command} ${args.join(" ")}`;
    const result = await run(command, args);
    if (result?.ok !== true || result.error || result.data === undefined) {
      throw new Error(redact(`CLI evidence unavailable: ${source}: ${result?.error ?? "invalid result"}`));
    }
    const data = JSON.parse(redact(JSON.stringify(result.data)));
    reads.push({ source, data });
    return data;
  };
  const results = await Promise.allSettled([
    read("botflix", ["stats", "--period", "30d"]),
    read("botflix", ["history", "--limit", "500"]),
    read("botflix", ["subscriptions"]),
    read("botflix", ["library", "--limit", "500"]),
    read("health", ["equipment", "list"]),
    read("health", ["exercise", "list"]),
    ...["alex", "maru"].map(async person => {
      await read("health", ["training", "list", "--person", person, "--period", "30d"]);
      const nutrition = await read("health", ["nutrition", "summary", "--person", person, "--period", "30d"]);
      if (!Array.isArray(nutrition.rows)) throw new Error(`Invalid nutrition rows for ${person}`);
      for (const row of nutrition.rows) {
        if (!/^\d{4}-\d{2}-\d{2}$/.test(row.date)) throw new Error(`Invalid nutrition date for ${person}`);
        await read("health", ["nutrition", "get", "--person", person, "--date", row.date]);
      }
    }),
  ]);
  const failures = results.filter(result => result.status === "rejected");
  if (failures.length) throw new Error(failures.map(result => redact(result.reason.message)).join("; "));
  // Keep context ordering independent of request completion order.
  reads.sort((a, b) => a.source.localeCompare(b.source));
  return { checkedAt: new Date().toISOString(), coverage: "Current CLI state; training, nutrition and playback cover the reported 30-day windows. Media history/library are limited to 500 entries; absence beyond coverage proves nothing.", reads };
}

export async function saveReflection(directory, before, result, window, evidence) {
  if (!result || typeof result.healthReport !== "string" || !result.healthReport.trim() || result.healthReport.length > 60000 ||
      !result.healthProposals || typeof result.healthProposals !== "object" || Array.isArray(result.healthProposals) || typeof result.report !== "string" || !result.report.trim() || result.report.length > 60000 || !result.updates || typeof result.updates !== "object" || Array.isArray(result.updates)) throw new Error("Reflection must return {updates, report, healthProposals, healthReport}");
  for (const [name, text] of Object.entries(result.updates)) {
    if (!names.includes(name) || typeof text !== "string" || !text.trim() || text.length > (name === "alex.md" || name === "maru.md" ? 6000 : 60000)) throw new Error(`Invalid memory update: ${name}`);
  }
  for (const [name, text] of Object.entries(result.healthProposals)) {
    if (!healthNames.includes(name) || typeof text !== "string" || !text.trim() || text.length > 60000) throw new Error(`Invalid health proposal: ${name}`);
  }
  for (const name of [...names, ...healthNames]) {
    if (await readFile(join(directory, name), "utf8") !== before[name]) throw new Error(`Memory changed during reflection: ${name}; rerun without overwriting the participant's edit`);
  }
  const reports = join(directory, "reflections");
  await mkdir(reports, { recursive: true, mode: 0o700 });
  const healthReports = join(reports, "health");
  await mkdir(healthReports, { recursive: true, mode: 0o700 });
  const checked = evidence ? `\nСверка CLI: ${evidence.checkedAt}.\n${evidence.reads.map(read => `- ${read.source}`).join("\n")}\n` : "";
  const healthDrafts = Object.entries(result.healthProposals).map(([name, text]) =>
    `## ${name} — предлагаемый полный текст\n\n${text}`).join("\n\n");
  const files = { ...result.updates, [`reflections/${window.date}.md`]:
    `# Рефлексия ${window.date}\n\nМодель: openai-codex/gpt-6-luna · max.\nПериод UTC: ${new Date(window.start).toISOString()} — ${new Date(window.end).toISOString()}${window.partial ? " (неполный день, тестовый запуск)" : ""}.\n\n${checked}\n${result.report}\n`,
    [`reflections/health/${window.date}.md`]:
      `# HP: рефлексия ${window.date}\n\nРежим: только предложения для ручного просмотра. Файлы здоровья не изменены.\nМодель: openai-codex/gpt-6-luna · max.\nПериод UTC: ${new Date(window.start).toISOString()} — ${new Date(window.end).toISOString()}${window.partial ? " (неполный день, тестовый запуск)" : ""}.\n\n${checked}\n${result.healthReport}\n\n${healthDrafts}\n` };
  for (const [name, text] of Object.entries(files)) {
    const path = join(directory, name);
    await writeFile(`${path}.tmp`, text, { mode: 0o600 });
    await rename(`${path}.tmp`, path);
  }
  return Object.keys(result.updates);
}

async function main() {
  process.umask(0o077);
  const args = process.argv.slice(2);
  const dateIndex = args.indexOf("--date");
  if (args.some((arg, index) => arg !== "--inspect" && arg !== "--date" && !(dateIndex >= 0 && index === dateIndex + 1)) || dateIndex >= 0 && !args[dateIndex + 1]) throw new Error("Usage: reflect.mjs [--date YYYY-MM-DD] [--inspect]");
  const window = reflectionWindow(dateIndex < 0 ? undefined : args[dateIndex + 1]);
  const family = join(homedir(), ".local/share/family");
  const agentDir = getAgentDir();
  const transcript = await collectTranscript(join(agentDir, "sessions/pi-telegram"), window);
  const counts = { date: window.date, partial: window.partial, sessions: new Set(transcript.map(item => item.source.split("#")[0])).size, messages: transcript.length };
  if (args.includes("--inspect")) { console.log(JSON.stringify({ ...counts, sources: transcript.map(item => item.source) })); return; }
  const before = Object.fromEntries(await Promise.all([...names, ...healthNames].map(async name => [name, await readFile(join(family, name), "utf8")])));
  if (!transcript.length) {
    await saveReflection(family, before, { updates: {}, report: "Новых семейных фактов нет: за день нет сообщений.",
      healthProposals: {}, healthReport: "Новых подтверждённых фактов HP нет: за день нет сообщений." }, window);
    console.log(JSON.stringify({ ...counts, skipped: "no messages", reportsSaved: true }));
    return;
  }
  const evidence = await collectCLIEvidence();
  console.log(JSON.stringify({ event: "reflection_cli_checked", ...counts, checkedAt: evidence.checkedAt, sources: evidence.reads.map(read => read.source) }));
  const modelRuntime = await ModelRuntime.create({ agentDir });
  const model = modelRuntime.getModel("openai-codex", "gpt-6-luna");
  if (!model || !(await modelRuntime.getAvailable()).some(item => item.provider === model.provider && item.id === model.id)) throw new Error("GPT-6 Luna is unavailable; restore OpenAI Codex authorization");
  const loader = new DefaultResourceLoader({ cwd: family, agentDir, noExtensions: true, noSkills: true,
    noContextFiles: true, noPromptTemplates: true, noThemes: true,
    systemPrompt: await readFile(join(dirname(dirname(fileURLToPath(import.meta.url))), "agent/REFLECTION.md"), "utf8") });
  await loader.reload();
  const { session } = await createAgentSession({ cwd: family, agentDir, modelRuntime, model, thinkingLevel: "max",
    tools: [], noTools: "all", resourceLoader: loader,
    sessionManager: SessionManager.create(family, join(agentDir, "sessions/family-reflection")) });
  try {
    if (session.model?.id !== "gpt-6-luna" || session.thinkingLevel !== "max" || session.getActiveToolNames().length) throw new Error("Reflection model/thinking/tool configuration mismatch");
    const started = Date.now();
    const prompt = `Review this day's text conversation in one pass. Compare with current memory AND the supplied read-only CLI evidence first. Do not propose duplicate CLI records in Markdown. Assistant replies provide context, never proof. General family/cinema memory: return updates and report for automatic application. Health memory: return healthProposals and healthReport for manual review ONLY; proposals never change health files. Return ONLY {updates, report, healthProposals, healthReport} JSON with complete contents for proposed changed files. If nothing is new, the corresponding map is {} and report says "Новых семейных фактов нет." or "Новых подтверждённых фактов HP нет.". No code fences.\n${JSON.stringify({ window, memory: before, cliEvidence: evidence, conversation: transcript })}`;
    console.log(JSON.stringify({ event: "reflection_started", ...counts, requests: 1, conversationCharacters: JSON.stringify(transcript).length,
      model: session.model.id, thinking: session.thinkingLevel }));
    await session.prompt(prompt);
    const last = session.messages.findLast(message => message.role === "assistant");
    if (!last || last.errorMessage || ["error", "aborted", "length"].includes(last.stopReason)) throw new Error(`Reflection failed: ${redact(last?.errorMessage ?? last?.stopReason ?? "no response")}`);
    const result = JSON.parse(session.getLastAssistantText() ?? "");
    const updated = await saveReflection(family, before, result, window, evidence);
    console.log(JSON.stringify({ event: "reflection_completed", ...counts, elapsedSeconds: Math.round((Date.now() - started) / 1000), updated,
      report: join(family, "reflections", `${window.date}.md`),
      healthReport: join(family, "reflections/health", `${window.date}.md`), healthProposals: Object.keys(result.healthProposals), sessionId: session.sessionId }));
  } finally { session.dispose(); }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch(error => { console.error(redact(error.message)); process.exitCode = 1; });
}
