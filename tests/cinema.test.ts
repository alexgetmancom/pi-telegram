/** Cinema tool capability and exact-topic admission regressions. Zones: host, telegram. */
import assert from "node:assert/strict";
import test from "node:test";
import fs from "node:fs";
import { syncBuiltinESMExports } from "node:module";
import { join } from "node:path";
import { homedir } from "node:os";
// @ts-expect-error The deployed SDK host runs native JavaScript.
import { cinemaArguments, familyMemoryTool, mediaEventView } from "../scripts/cinema.mjs";

test("Cinema cannot invoke shell, local file IO, browser auth or delete files", () => {
  for (const p of [
    { action: "bash", query: "id" }, { action: "auth" },
    { action: "download", url: "/etc/passwd" }, { action: "download", url: "file:///etc/passwd" },
    { action: "download", url: "https://127.0.0.1/private" },
    { action: "download", url: "https://rutracker.org.evil.test/file" },
    { action: "episodes", url: "/series/Shogun", season: "1;id" },
    { action: "remove", hash: "all" }, { action: "series", url: "https://evil.test/series/Test" },
  ]) assert.throws(() => cinemaArguments(p));
  assert.deepEqual(cinemaArguments({ action: "search", source: "lostfilm", query: "test; id" }),
    ["search", "--source", "lostfilm", "test; id"]);
  assert.deepEqual(cinemaArguments({ action: "remove", hash: "A".repeat(40), deleteFiles: true }), ["remove", "a".repeat(40)]);
  assert.deepEqual(cinemaArguments({ action: "episodes", season: 1, url: "/series/Shogun" }),
    ["episodes", "--season", "1", "https://www.lostfilm.tv/series/Shogun"]);
});

test("Family memory confines writes to three files, refuses stale reads and verifies atomic publication", async t => {
  const path = join(homedir(), ".local/share/family/alex.md");
  const files = new Map([[path, "Existing preferences\n"]]);
  let mode = 0;
  t.mock.method(fs, "readFileSync", (p: string) => { if (!files.has(p)) throw new Error("Missing file"); return files.get(p); });
  t.mock.method(fs, "existsSync", (p: string) => files.has(p));
  t.mock.method(fs, "writeFileSync", (p: string, text: string, options: { mode: number; flag: string }) => {
    assert.equal(options.flag, "wx"); assert.equal(files.has(p), false); mode = options.mode; files.set(p, text);
  });
  t.mock.method(fs, "renameSync", (from: string, to: string) => { files.set(to, files.get(from)!); files.delete(from); });
  t.mock.method(fs, "unlinkSync", (p: string) => files.delete(p));
  syncBuiltinESMExports();
  try {
    for (const file of ["../auth.json", "/etc/passwd", "network-issues.md"]) {
      await assert.rejects(familyMemoryTool.execute("id", { action: "read", file }));
    }
    const read = JSON.parse((await familyMemoryTool.execute("id", { action: "read", file: "alex.md" })).content[0].text);
    await assert.rejects(familyMemoryTool.execute("id", { action: "write", file: "alex.md", content: "new", expectedSha256: "stale" }));
    const result = await familyMemoryTool.execute("id", { action: "write", file: "alex.md", content: "Updated preferences\n", expectedSha256: read.sha256 });
    assert.equal(JSON.parse(result.content[0].text).saved, true);
    assert.equal(files.get(path), "Updated preferences\n"); assert.equal(mode, 0o600);
    assert.equal(files.size, 1);
    await assert.rejects(familyMemoryTool.execute("id", { action: "write", file: "alex.md", content: "overwrite", expectedSha256: read.sha256 }));
  } finally { t.mock.restoreAll(); syncBuiltinESMExports(); }
});

test("Cinema exposes subscriptions and reports but not automation event delivery controls", async () => {
  assert.deepEqual(cinemaArguments({ action: "subscribe", quality: "720p", url: "/series/From" }), ["subscribe", "--quality", "720p", "https://www.lostfilm.tv/series/From"]);
  assert.deepEqual(cinemaArguments({ action: "stats", period: "month" }), ["stats", "--period", "month"]);
  for (const action of ["events", "ack", "claim", "tick", "retry-event"]) assert.throws(() => cinemaArguments({ action }));
  assert.throws(() => cinemaArguments({ action: "stats", period: "arbitrary SQL" }));
  const view = mediaEventView({ text: "Downloaded <movie>", item: { Name: "Movie & name", Overview: "<bad>", watch_url: "https://jf.i/watch" } });
  assert.equal(view.parseMode, "html"); assert.match(view.text, /&lt;movie&gt;/); assert.match(view.text, /Movie &amp; name/);
  assert.equal(view.replyMarkup.inline_keyboard[0][0].url, "https://jf.i/watch");
});

// @ts-expect-error Read-only web capabilities are native JavaScript host tools.
import { braveResults, cleanHTML, publicURL } from "../scripts/web.mjs";

test("Direct search preserves real URLs and rejects private/local page inputs", async () => {
  const html = `<div class="snippet" data-type="web"><a href="https://example.com/review?q=1&amp;p=2"><div class="title x">Review &amp; opinions</div></a><p>Viewers disagree.</p></div><div class="snippet" data-type="ad"><a href="https://ads.test"><div class="title">Ad</div></a></div>`;
  const rows = braveResults(html);
  assert.equal(rows.length, 1); assert.equal(rows[0].title, "Review & opinions");
  assert.equal(rows[0].url, "https://example.com/review?q=1&p=2");
  assert.equal(cleanHTML('<script>secret()</script><p>Visible</p>'), "Visible");
  for (const value of ["file:///etc/passwd", "http://example.com", "https://localhost", "https://127.0.0.1/"]) {
    await assert.rejects(publicURL(value));
  }
});
