#!/usr/bin/env node

/** Native Telegram inbound command: transcribe with VM 106's existing ASR. */
import { execFileSync } from "node:child_process";
import { readFile } from "node:fs/promises";
import { basename } from "node:path";

try {
  const [file, ...extra] = process.argv.slice(2);
  if (!file || extra.length) throw new Error("Usage: transcribe.mjs <audio-file>");
  const address = execFileSync("docker", ["inspect", "agent-asr", "--format",
    '{{(index .NetworkSettings.Networks "agent_default").IPAddress}}'],
    { encoding: "utf8", timeout: 10_000 }).trim();
  if (!/^\d{1,3}(\.\d{1,3}){3}$/.test(address)) {
    throw new Error("agent-asr has no address on agent_default");
  }
  const body = new FormData();
  body.set("file", new Blob([await readFile(file)]), basename(file));
  body.set("language", "ru");
  const response = await fetch(`http://${address}:8787/transcribe`, {
    method: "POST", body, signal: AbortSignal.timeout(300_000),
  });
  if (!response.ok) throw new Error(`ASR returned HTTP ${response.status}`);
  const result = await response.json();
  if (typeof result.text !== "string" || !result.text.trim()) {
    throw new Error("ASR returned an empty transcript");
  }
  process.stdout.write(`${result.text.trim()}\n`);
} catch (error) {
  process.stderr.write(`Transcription failed: ${error.message}\n`);
  process.exitCode = 1;
}
