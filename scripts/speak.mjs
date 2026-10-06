/** Native voice-reply handler using the existing local Piper service. */
import { execFileSync } from "node:child_process";
const [output, ...extra] = process.argv.slice(2);
if (!output || extra.length) throw new Error("Usage: speak.mjs <output.ogg>; text on stdin");
let text = "";
for await (const chunk of process.stdin) text += chunk;
if (!text.trim() || text.length > 24000) throw new Error("Invalid speech text");
const address = execFileSync("docker", ["inspect", "agent-tts", "--format", '{{(index .NetworkSettings.Networks "agent_default").IPAddress}}'], { encoding: "utf8", timeout: 10000 }).trim();
if (!/^\d{1,3}(\.\d{1,3}){3}$/.test(address)) throw new Error("Local TTS unavailable");
const response = await fetch(`http://${address}:5000/`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ text }), signal: AbortSignal.timeout(60000) });
if (!response.ok) throw new Error(`TTS returned HTTP ${response.status}`);
execFileSync("ffmpeg", ["-loglevel", "error", "-f", "wav", "-i", "pipe:0", "-c:a", "libopus", "-b:a", "32k", output], { input: Buffer.from(await response.arrayBuffer()), timeout: 60000 });
process.stdout.write(output + "\n");
