/** Regressions for daily Moscow boundaries, all-session collection, redaction and non-destructive memory publication. */
import assert from "node:assert/strict";
import { mkdir, stat, mkdtemp, readFile, writeFile, readdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

const script = new URL("../scripts/reflect.mjs", import.meta.url);
const { reflectionWindow, collectTranscript, redact, collectCLIEvidence, saveReflection } = await import(script.href);

test("Reflection uses the complete previous Moscow calendar day and validates manual dates", () => {
  const day = reflectionWindow(undefined, new Date("2026-10-05T21:01:00Z"));
  assert.equal(day.date, "2026-10-05");
  assert.equal(new Date(day.start).toISOString(), "2026-10-04T21:00:00.000Z");
  assert.equal(new Date(day.end).toISOString(), "2026-10-05T21:00:00.000Z");
  assert.equal(day.partial, false);
  assert.equal(reflectionWindow(undefined, new Date("2026-10-05T20:59:00Z")).date, "2026-10-04");
  assert.equal(reflectionWindow("2026-10-05", new Date("2026-10-05T20:00:00Z")).partial, true);
  assert.throws(() => reflectionWindow("2026-02-30"));
});

test("Reflection collects messages from every session by entry date, excludes thinking and handles active appends", async () => {
  const dir = await mkdtemp(join(tmpdir(), "reflection-history-"));
  try {
    const row = (id: string, timestamp: string, role: string, content: unknown) => JSON.stringify({ type: "message", id, timestamp, message: { role, content } });
    await writeFile(join(dir, "old-session.jsonl"), [
      row("outside", "2026-10-04T20:59:00Z", "user", "outside"),
      row("alex", "2026-10-04T21:00:00Z", "user", "[telegram|user:101|name:Alex] fact"),
      row("thought", "2026-10-05T01:00:00Z", "assistant", [{ type: "thinking", thinking: "private" }]),
      row("system", "2026-10-05T02:00:00Z", "system", "instructions"),
    ].join("\n") + "\n");
    await writeFile(join(dir, "new-session.jsonl"), [
      row("maru", "2026-10-05T03:00:00Z", "user", "[telegram|user:202|name:Maru] fact"),
      row("failure", "2026-10-05T04:00:00Z", "toolResult", [{ type: "text", text: "curl timed out" }]),
      row("tool", "2026-10-05T04:01:00Z", "assistant", [{ type: "text", text: "technical preamble" }, { type: "toolCall", name: "bash", arguments: { command: "secret command" } }]),
      row("reply", "2026-10-05T04:02:00Z", "assistant", [{ type: "text", text: "Which film?" }, { type: "thinking", thinking: "private reasoning" }]),
      row("empty", "2026-10-05T04:03:00Z", "assistant", [{ type: "image", data: "image data" }]),
      JSON.stringify({ type: "message", id: "aborted", timestamp: "2026-10-05T04:04:00Z", message: { role: "assistant", stopReason: "aborted", content: "unfinished response" } }),
      row("tomorrow", "2026-10-05T21:00:00Z", "user", "outside"),
      '{"partial":',
    ].join("\n"));
    const rows = await collectTranscript(dir, reflectionWindow("2026-10-05", new Date("2026-10-06T00:00:00Z")));
    assert.deepEqual(rows.map((row: { source: string }) => row.source), ["old-session.jsonl#alex", "new-session.jsonl#maru", "new-session.jsonl#reply"]);
    assert.equal(rows[2].content, "Which film?");
    assert.ok(!JSON.stringify(rows).includes("private"));
    assert.ok(!JSON.stringify(rows).includes("technical preamble"));
    assert.ok(!JSON.stringify(rows).includes("curl timed out"));
    assert.ok(!JSON.stringify(rows).includes("unfinished response"));
    assert.ok(!redact('Bearer abc.def.xyz {"refresh":"secret-value"} sk-abcdefghijklmnopqrstuvwxyz012345').includes("secret-value"));
    assert.ok(!redact("sk-abcdefghijklmnopqrstuvwxyz012345").includes("abcdefghijklmnopqrstuvwxyz"));
  } finally { await rm(dir, { recursive: true, force: true }); }
});

test("Reflection rejects unknown files and concurrent changes before writing any memory", async () => {
  const dir = await mkdtemp(join(tmpdir(), "reflection-save-"));
  try {
    const before = Object.fromEntries(["alex.md", "maru.md", "watchlist.md", "health/alex.md", "health/alex-training.md", "health/alex-nutrition.md", "health/maru.md", "health/maru-training.md", "health/maru-nutrition.md", "health/notes.md"].map(name => [name, "original " + name]));
    await mkdir(join(dir, "health"));
    const review = { healthProposals: {}, healthReport: "Новых подтверждённых фактов HP нет." };
    for (const [name, text] of Object.entries(before)) await writeFile(join(dir, name), text);
    const day = reflectionWindow("2026-10-05", new Date("2026-10-06T00:00:00Z"));
    await assert.rejects(saveReflection(dir, before, { ...review, updates: { "../AGENTS.md": "bad" }, report: "report" }, day));
    await assert.rejects(saveReflection(dir, before, { ...review, updates: { "network-issues.md": "technical analysis" }, report: "report" }, day));
    await writeFile(join(dir, "maru.md"), "live edit");
    await assert.rejects(saveReflection(dir, before, { ...review, updates: { "alex.md": "new" }, report: "report" }, day));
    assert.equal(await readFile(join(dir, "alex.md"), "utf8"), before["alex.md"]);
    before["maru.md"] = "live edit";
    assert.deepEqual(await saveReflection(dir, before, { ...review, updates: { "alex.md": "confirmed fact" }, report: "saved from source#entry" }, day), ["alex.md"]);
    assert.equal(await readFile(join(dir, "alex.md"), "utf8"), "confirmed fact");
    assert.match(await readFile(join(dir, "reflections/2026-10-05.md"), "utf8"), /gpt-6-luna · max/);
    assert.ok(!(await readdir(dir)).some(name => name.endsWith(".tmp")));
    await assert.rejects(saveReflection(dir, before, { ...review, updates: { "health/alex.md": "forbidden write" }, report: "report" }, day));
    await assert.rejects(saveReflection(dir, before, { ...review, updates: {}, report: "report", healthProposals: { "../health/alex.md": "bad" } }, day));
    await assert.rejects(saveReflection(dir, before, { ...review, updates: {}, report: "report", healthProposals: { "alex.md": "wrong domain" } }, day));
    before["alex.md"] = "confirmed fact";
    const result = { updates: {}, report: "No general changes", healthProposals: { "health/alex-training.md": "New confirmed workout source#entry" }, healthReport: "Workout reported by Alex source#entry" };
    assert.deepEqual(await saveReflection(dir, before, result, day), []);
    for (const [name, text] of Object.entries(before)) assert.equal(await readFile(join(dir, name), "utf8"), text);
    const report = join(dir, "reflections/health/2026-10-05.md");
    assert.match(await readFile(report, "utf8"), /New confirmed workout source#entry/);
    assert.equal((await stat(report)).mode & 0o777, 0o600);
    assert.equal((await stat(join(dir, "reflections/health"))).mode & 0o777, 0o700);
    await writeFile(join(dir, "health/alex-training.md"), "live health edit");
    await assert.rejects(saveReflection(dir, before, result, day), /Memory changed/);
  } finally { await rm(dir, { recursive: true, force: true }); }
});


test("Reflection reads both CLIs and full nutrition records before proposing journal changes", async () => {
  const calls: string[] = [];
  const evidence = await collectCLIEvidence(async (command: string, args: string[]) => {
    calls.push(`${command} ${args.join(" ")}`);
    if (args[0] === "nutrition" && args[1] === "summary") return { ok: true, data: { rows: [{ date: "2026-10-07" }] } };
    if (args[0] === "training") return { ok: true, data: { sessions: [{ id: "2026-10-07-1", person: args[3], exercises: [{ name: "Corrected lateral raise", load_basis: "per_hand" }] }] } };
    if (args[0] === "nutrition" && args[1] === "get") return { ok: true, data: { day: { date: "2026-10-07", meals: [{ name: "Breakfast" }] } } };
    return { ok: true, data: { token: "private-value", records: [] } };
  });
  assert.equal(calls.length, 12);
  for (const person of ["alex", "maru"]) {
    assert.ok(calls.includes(`health training list --person ${person} --period 30d`));
    assert.ok(calls.includes(`health nutrition get --person ${person} --date 2026-10-07`));
  }
  for (const call of ["botflix stats --period 30d", "botflix history --limit 500", "botflix subscriptions", "botflix library --limit 500", "health equipment list", "health exercise list"]) assert.ok(calls.includes(call));
  assert.ok(!calls.some(call => /\b(save|sync|tick|export|refresh|download|subscribe|ack)\b/.test(call)));
  assert.match(JSON.stringify(evidence), /2026-10-07-1/);
  assert.match(JSON.stringify(evidence), /Corrected lateral raise/);
  assert.match(JSON.stringify(evidence), /Breakfast/);
  assert.ok(!JSON.stringify(evidence).includes("private-value"));
  assert.match(evidence.coverage, /500/);
  assert.deepEqual(evidence.reads.map((read: { source: string }) => read.source), [...calls].sort((a, b) => a.localeCompare(b)));
});

test("Failed or malformed CLI evidence cannot be treated as an empty successful journal", async () => {
  await assert.rejects(collectCLIEvidence(async () => ({ ok: false, data: { rows: [] }, error: "source unavailable" })), /source unavailable/);
  await assert.rejects(collectCLIEvidence(async () => ({ ok: true, data: { rows: [] }, error: "partial failure" })), /partial failure/);
  await assert.rejects(collectCLIEvidence(async () => ({ ok: true, data: { rows: [{ date: "invalid" }] } })), /Invalid nutrition date/);
  await assert.rejects(collectCLIEvidence(async () => ({ ok: true, data: {} })), /Invalid nutrition rows/);
});
