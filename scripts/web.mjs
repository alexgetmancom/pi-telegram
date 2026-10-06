/** Bounded direct Brave search and public-page reads, matching the AI topic's curl-based search. */
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { lookup } from "node:dns/promises";
import { Type } from "@sinclair/typebox";
const executeFile = promisify(execFile);
const agent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36";

export async function publicURL(value) {
  const url = new URL(value);
  if (url.protocol !== "https:" || url.username || url.password || url.port || !url.hostname.includes(".")) throw new Error("Only public HTTPS pages are allowed");
  for (const { address } of await lookup(url.hostname, { all: true })) {
    if (/^(?:127\.|10\.|192\.168\.|169\.254\.|0\.|172\.(?:1[6-9]|2\d|3[01])\.|::|f[cd]|fe80|::ffff:)/i.test(address)) throw new Error("Private network pages are unavailable");
  }
  return url.href;
}

async function readHTML(value, signal) {
  let url = value;
  for (let hop = 0; hop < 5; hop++) {
    url = await publicURL(url);
    const { stdout } = await executeFile("/usr/bin/curl", ["--silent", "--show-error", "--max-time", "25", "--max-filesize", "2097152", "--user-agent", agent,
      "--write-out", "\n%{http_code}\n%{redirect_url}", "--url", url], { shell: false, signal, timeout: 30000, maxBuffer: 2100000, env: { PATH: "/usr/bin:/bin" } });
    const status = stdout.match(/\n(\d{3})\n([^\n]*)$/);
    if (!status) throw new Error("Invalid page response");
    if (+status[1] >= 300 && +status[1] < 400 && status[2]) { url = status[2]; continue; }
    if (+status[1] !== 200) throw new Error(`Web page HTTP ${status[1]}`);
    return { url, html: stdout.slice(0, status.index) };
  }
  throw new Error("Too many page redirects");
}

export function cleanHTML(value) {
  return value.replace(/<(script|style|svg|noscript)\b[^>]*>[\s\S]*?<\/\1>/gi, " ").replace(/<!--[\s\S]*?-->/g, " ")
    .replace(/<[^>]+>/g, " ").replace(/&#(x[\da-f]+|\d+);/gi, (_m, n) => { const c = n[0].toLowerCase() === "x" ? parseInt(n.slice(1),16) : +n; return c <= 0x10ffff ? String.fromCodePoint(c) : ""; })
    .replace(/&(amp|lt|gt|quot|apos|nbsp);/g, (_m, n) => ({ amp: "&", lt: "<", gt: ">", quot: '"', apos: "'", nbsp: " " })[n]).replace(/\s+/g, " ").trim();
}

export function braveResults(html) {
  const results = [], seen = new Set();
  for (const block of html.split(/<div class="snippet[^"]*"/).slice(1)) {
    if (!/^[^>]*data-type="web"/.test(block)) continue;
    const title = block.match(/<div class="title[^"]*"[^>]*>([\s\S]*?)<\/div>/)?.[1];
    const url = block.match(/href="(https?:\/\/[^\"]+)"/)?.[1];
    if (!title || !url || seen.has(url)) continue;
    seen.add(url);
    results.push({ title: cleanHTML(title), url: url.replace(/&amp;/g,"&"), snippet: cleanHTML(block).slice(0,1600) });
  }
  return results.slice(0,12);
}

export const webSearchTool = {
  name: "web_search", label: "Веб-поиск", description: "Search the public web directly through Brave, without paid search APIs. Snippets are not full pages; cite original URLs and do not invent a viewer consensus.",
  parameters: Type.Object({ query: Type.String({ minLength: 1, maxLength: 1000 }) }, { additionalProperties: false }),
  async execute(_id, p, signal) {
    if (typeof p.query !== "string" || !p.query.trim() || p.query.length > 1000) throw new Error("Invalid search query");
    const { html } = await readHTML(`https://search.brave.com/search?q=${encodeURIComponent(p.query)}`, signal);
    const results = braveResults(html);
    if (!results.length) throw new Error("Search returned no usable results; possible verification or page-format change");
    return { content: [{ type: "text", text: JSON.stringify({ results }) }], details: {} };
  },
};
export const webFetchTool = {
  name: "web_fetch", label: "Чтение страницы", description: "Read a public HTTPS page directly. No browser, private URLs, local files, scripts, cookies or custom headers.",
  parameters: Type.Object({ url: Type.String() }, { additionalProperties: false }),
  async execute(_id, p, signal) {
    const { url, html } = await readHTML(p.url, signal);
    const title = cleanHTML(html.match(/<title[^>]*>([\s\S]*?)<\/title>/i)?.[1] ?? "");
    const text = cleanHTML(html).slice(0,30000);
    if (!text || /captcha|just a moment|access denied|security check/i.test(title)) throw new Error("Page requires verification or returned no readable text");
    return { content: [{ type: "text", text: JSON.stringify({ url, title, text }) }], details: {} };
  },
};
