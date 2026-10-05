/** Regressions for daily Moscow boundaries, all-session collection, redaction and non-destructive memory publication. */
import assert from "node:assert/strict";
import { mkdtemp, readFile, writeFile, readdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

const script = new URL("../scripts/reflect.mjs", import.meta.url);
const { reflectionWindow, collectTranscript, redact, saveReflection } = await import(script.href);

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
    const before = Object.fromEntries(["alex.md", "maru.md", "watchlist.md"].map(name => [name, "original " + name]));
    for (const [name, text] of Object.entries(before)) await writeFile(join(dir, name), text);
    const day = reflectionWindow("2026-10-05", new Date("2026-10-06T00:00:00Z"));
    await assert.rejects(saveReflection(dir, before, { updates: { "../AGENTS.md": "bad" }, report: "report" }, day));
    await assert.rejects(saveReflection(dir, before, { updates: { "network-issues.md": "technical analysis" }, report: "report" }, day));
    await writeFile(join(dir, "maru.md"), "live edit");
    await assert.rejects(saveReflection(dir, before, { updates: { "alex.md": "new" }, report: "report" }, day));
    assert.equal(await readFile(join(dir, "alex.md"), "utf8"), before["alex.md"]);
    before["maru.md"] = "live edit";
    assert.deepEqual(await saveReflection(dir, before, { updates: { "alex.md": "confirmed fact" }, report: "saved from source#entry" }, day), ["alex.md"]);
    assert.equal(await readFile(join(dir, "alex.md"), "utf8"), "confirmed fact");
    assert.match(await readFile(join(dir, "reflections/2026-10-05.md"), "utf8"), /gpt-6-luna · max/);
    assert.ok(!(await readdir(dir)).some(name => name.endsWith(".tmp")));
  } finally { await rm(dir, { recursive: true, force: true }); }
});
