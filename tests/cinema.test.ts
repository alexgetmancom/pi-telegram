/** Media notification rendering regression. */
import assert from "node:assert/strict";
import test from "node:test";
// @ts-expect-error The deployed SDK host runs native JavaScript.
import { mediaEventView } from "../scripts/media-automation.mjs";

test("Media notification escapes titles and preserves Jellyfin action", () => {
  const view = mediaEventView({ text: "Downloaded <movie>", item: { Name: "Movie & name", Overview: "<bad>", watch_url: "https://jf.i/watch" } });
  assert.equal(view.parseMode, "html"); assert.match(view.text, /&lt;movie&gt;/); assert.match(view.text, /Movie &amp; name/);
  assert.equal(view.replyMarkup.inline_keyboard[0][0].url, "https://jf.i/watch");
});
