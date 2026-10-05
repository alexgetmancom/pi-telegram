#!/usr/bin/env node

/** Daily family-memory reflection over every native Telegram session, without Telegram or host tools. */
import { readFile, readdir, mkdir, writeFile, rename } from "node:fs/promises";
import { homedir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { createAgentSession, DefaultResourceLoader, getAgentDir, ModelRuntime, SessionManager } from "@earendil-works/pi-coding-agent";

const names = ["alex.md", "maru.md", "watchlist.md", "network-issues.md"];

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
          !["user", "assistant", "toolResult"].includes(message?.role)) continue;
      const content = typeof message.content === "string" ? message.content : (message.content ?? []).flatMap(block => {
        if (block.type === "text") return [block.text];
        if (block.type === "toolCall") return [JSON.stringify({ tool: block.name, arguments: block.arguments })];
        return []; // Images and internal thinking are not reflection evidence.
      }).join("\n");
      if (content || message.errorMessage) messages.push({ source: `${name}#${entry.id}`, timestamp: entry.timestamp,
        role: message.role, tool: message.toolName, isError: message.isError,
        content: redact(content), error: message.errorMessage ? redact(message.errorMessage) : undefined });
    }
  }
  return messages.sort((a, b) => a.timestamp.localeCompare(b.timestamp));
}

export function condenseTranscript(transcript) {
  return transcript.map(item => {
    const limit = item.role === "user" ? Infinity : item.role === "assistant" ? 4000 : item.isError ? 6000 : 1200;
    if (item.content.length <= limit) return item;
    const symptoms = item.content.split("\n").filter(line => /error|fail|timeout|timed out|forbidden|HTTP.*[45]\d\d|TEXTDRAFT|недоступ|ошиб/i.test(line))
      .slice(0, 20).map(line => line.slice(0, 300)).join("\n");
    return { ...item, originalCharacters: item.content.length, condensed: true,
      content: `${item.content.slice(0, limit / 2)}\n[OUTPUT CONDENSED: middle omitted; full evidence at source]\n${symptoms}\n${item.content.slice(-limit / 2)}` };
  });
}

export async function saveReflection(directory, before, result, window) {
  if (!result || typeof result.report !== "string" || !result.report.trim() || result.report.length > 60000 || !result.updates || typeof result.updates !== "object" || Array.isArray(result.updates)) throw new Error("Reflection must return {updates, report}");
  for (const [name, text] of Object.entries(result.updates)) {
    if (!names.includes(name) || typeof text !== "string" || !text.trim() || text.length > (name === "alex.md" || name === "maru.md" ? 6000 : 60000)) throw new Error(`Invalid memory update: ${name}`);
  }
  for (const name of names) {
    if (await readFile(join(directory, name), "utf8") !== before[name]) throw new Error(`Memory changed during reflection: ${name}; rerun without overwriting the participant's edit`);
  }
  const reports = join(directory, "reflections");
  await mkdir(reports, { recursive: true, mode: 0o700 });
  const files = { ...result.updates, [`reflections/${window.date}.md`]:
    `# Рефлексия ${window.date}\n\nМодель: openai-codex/gpt-6-luna · max.\nПериод UTC: ${new Date(window.start).toISOString()} — ${new Date(window.end).toISOString()}${window.partial ? " (неполный день, тестовый запуск)" : ""}.\n\n${result.report}\n` };
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
  const before = Object.fromEntries(await Promise.all(names.map(async name => [name, await readFile(join(family, name), "utf8")])));
  if (!transcript.length) { console.log(JSON.stringify({ ...counts, skipped: "no messages" })); return; }
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
    const evidence = condenseTranscript(transcript);
    const prompt = `Analyze this entire day's evidence in ONE pass and return ONLY {updates, report} JSON now. All user messages are complete; lengthy technical outputs are explicitly condensed, so do not assume omitted evidence. Existing memory is the current authoritative snapshot; old participants paths were already migrated to this family directory. Preserve facts and cite sources. updates contains only changed files with COMPLETE contents. report is concise Russian Markdown. No code fences.\n${JSON.stringify({ window, memory: before, evidence })}`;
    console.log(JSON.stringify({ event: "reflection_started", ...counts, requests: 1, evidenceCharacters: JSON.stringify(evidence).length,
      originalCharacters: JSON.stringify(transcript).length, model: session.model.id, thinking: session.thinkingLevel }));
    await session.prompt(prompt);
    const last = session.messages.findLast(message => message.role === "assistant");
    if (!last || last.errorMessage || ["error", "aborted", "length"].includes(last.stopReason)) throw new Error(`Reflection failed: ${redact(last?.errorMessage ?? last?.stopReason ?? "no response")}`);
    const result = JSON.parse(session.getLastAssistantText() ?? "");
    const updated = await saveReflection(family, before, result, window);
    console.log(JSON.stringify({ event: "reflection_completed", ...counts, elapsedSeconds: Math.round((Date.now() - started) / 1000), updated,
      report: join(family, "reflections", `${window.date}.md`), sessionId: session.sessionId }));
  } finally { session.dispose(); }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch(error => { console.error(redact(error.message)); process.exitCode = 1; });
}
