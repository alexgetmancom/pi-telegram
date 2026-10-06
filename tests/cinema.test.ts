/** Media notification rendering regression. */
import assert from "node:assert/strict";
import test from "node:test";
import { readFileSync, writeFileSync, existsSync, mkdtempSync, rmSync } from "node:fs";
import { homedir, tmpdir } from "node:os";
import { join } from "node:path";
import { Script, createContext } from "node:vm";
// @ts-expect-error The deployed SDK host runs native JavaScript.
import { mediaEventView } from "../scripts/media-automation.mjs";

test("Media notification escapes titles and preserves Jellyfin action", () => {
  const view = mediaEventView({ text: "Downloaded <movie>", item: { Name: "Movie & name", Overview: "<bad>", watch_url: "https://jf.i/watch" } });
  assert.equal(view.parseMode, "html"); assert.match(view.text, /&lt;movie&gt;/); assert.match(view.text, /Movie &amp; name/);
  assert.equal(view.replyMarkup.inline_keyboard[0][0].url, "https://jf.i/watch");
});

test("Media automation sends a poster, acknowledges only delivery and cleans temporary files", async () => {
  const scheduled: Array<() => void> = [];
  const calls: string[] = [];
  const errors: string[] = [];
  let posterPath = "";
  let photos = 0;
  const event = { id: 1, text: "Downloaded", kind: "completed", item: { Id: "a".repeat(32), Name: "Movie", ImageTags: { Primary: "tag" } } };
  const source = readFileSync(new URL("../scripts/media-automation.mjs", import.meta.url), "utf8")
    .replace(/^import \{([^}]+)\} from "([^"]+)";/gm, (_all, names: string, module: string) => `const {${names}} = imports[${JSON.stringify(module)}];`)
    .replace(/^export function /gm, "function ");
  const context = createContext({
    AbortController,
    setTimeout: (fn: () => void) => { scheduled.push(fn); return { unref() {} }; },
    clearTimeout: () => {},
    imports: {
      "node:child_process": { execFile: async (_binary: string, args: string[]) => {
        calls.push(args[0]);
        if (args[0] === "poster") { posterPath = args[2]; writeFileSync(posterPath, "poster"); }
        return { stdout: JSON.stringify({ ok: true, data: args[0] === "events" ? [event] : {} }) };
      } },
      "node:util": { promisify: (fn: unknown) => fn },
      "node:fs": { existsSync, mkdtempSync, rmSync },
      "node:path": { join },
      "node:os": { homedir, tmpdir },
      "../dist/api/delivery.js": {
        sendTelegramView: async () => { throw new Error("Expected photo delivery"); },
        sendTelegramPhoto: async (path: string) => { assert.equal(existsSync(path), true); photos++; return { ok: true }; },
      },
    },
  });
  new Script(source + "\nglobalThis.startAudit = startMediaAutomation;").runInContext(context);
  const stop = context.startAudit((kind: string, detail: { error?: string }) => { if (kind.includes("error")) errors.push(detail.error ?? kind); });
  scheduled.shift()?.();
  await new Promise(resolve => setImmediate(resolve));
  await stop();
  assert.deepEqual(errors, []);
  assert.equal(photos, 1);
  assert.deepEqual(calls, ["tick", "events", "poster", "claim", "ack"]);
  assert.equal(existsSync(posterPath), false);
});
