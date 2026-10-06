# Family forum deployment

The forum uses two native Pi instances with the same pi-telegram bridge. AI owns topic 16; Cinema owns topic 3. The native leader/follower bus keeps one Telegram poller and routes each topic's messages, callbacks, edits, reactions and media to its own session. Both participants share each topic; the two histories remain separate. General stays silent.

Both topics use the normal buttons, settings/model menus, draft streaming, reply context, inbound voice/files and outbound voice/media. Voice transcription uses the existing local ASR. Optional voice replies use `scripts/speak.mjs` and local Piper; manual/text replies remain the default. Image understanding depends on the selected model's vision support.

Both sessions use the same full native Pi and Telegram tools and skills. Their agent files define their focus; there is no Cinema tool allowlist or attachment filter. Cinema calls the BotFlix CLI through bash. Both read and edit family Markdown files with native file tools; current profiles are loaded before each model turn. Existing CLI subscriptions and notifications remain active. Family viewing uses the single Jellyfin `root` account. `botflix continue` and `next SERIES_ID` use its real viewing state. Torrent file selection uses `prepare` → `torrent-files` → `select-files` → `resume`; preparation does not start content downloads. Direct `qbit GET|POST ENDPOINT` is available for diagnostics and rare operations; normal media additions use `download`/`prepare` so CLI tracking remains authoritative. From is owned by the CLI after its one-time subscription transfer.

The shared Telegram profile keeps its AI forumTarget. Cinema supplies its own fixed target at runtime without overwriting that profile when settings change. Configured forum topics are operator-owned and are never automatically deleted on shutdown. `/new` affects only the topic's native session.

## Install on the host

Use Node 22.19+ and Pi 1.0.3. Keep one checkout at `~/projects/home/pi-telegram`.

```sh
npm install -g @earendil-works/pi-coding-agent@1.0.3
git clone https://github.com/alexgetmancom/pi-telegram.git ~/projects/home/pi-telegram
cd ~/projects/home/pi-telegram
npm ci
npm run build
pi install ~/projects/home/pi-telegram
```

Put `TELEGRAM_BOT_TOKEN` and `DEEPSEEK_API_KEY` in `~/.config/pi-telegram/secrets.env` with permissions 0600. In `~/.pi/agent/auth.json`, use `{"type":"api_key","key":"${DEEPSEEK_API_KEY}"}` for `deepseek`. Preserve other configured provider credentials.

In Pi, load those environment variables and run `/telegram-setup`, then `/telegram-connect`. Test `/start` in Telegram. Stop that interactive Pi before enabling the service so there is one poller.

Set `profiles.default.forumTarget` in `~/.pi/agent/telegram.json` to the negative supergroup `chatId` and positive `threadId`. Keep its `botToken` as `$TELEGRAM_BOT_TOKEN`. Set `assistant.activity` to `quiet` to hide thinking and tool output.

Set Pi's `defaultProvider` to `deepseek` and `defaultModel` to `deepseek-flash`. For compaction near 200,000 tokens with its 1,000,000-token window, set `compaction.modelOverrides["deepseek/deepseek-flash"].reserveTokens` to `800000`. The ordinary reserve remains available to other models. History and summaries are Pi's native JSONL, not a separate database.

```sh
mkdir -p ~/.config/systemd/user
cp systemd/pi-telegram.service systemd/pi-telegram-cinema.service ~/.config/systemd/user/
sudo loginctl enable-linger "$USER"
systemctl --user daemon-reload
systemctl --user enable --now pi-telegram
systemctl --user enable --now pi-telegram-cinema
curl --fail http://127.0.0.1:8186/healthz
curl --fail http://127.0.0.1:8187/healthz
```

`/start` opens the menu; `/model` selects the model; `/compact` summarizes context; `/new` starts a fresh shared session with native confirmation and retains this topic. A model switch continues the same native conversation. Older sessions remain on disk.

Runtime instructions are in [agent/AGENTS.md](../agent/AGENTS.md) for AI and [agent/CINEMA.md](../agent/CINEMA.md) for Cinema. Both native histories live under `~/.pi/agent/sessions/pi-telegram`, isolated by session CWD (home for AI, `~/projects/home/cli-botlix` for Cinema), and resume after restart. `/new` affects only its topic. Diagnose with `journalctl --user -u pi-telegram`; health at ports 8186 (AI) and 8187 (Cinema) includes each session ID, tools and transport role. Interrupted in-memory queued work is not replayed; resend it.

## Private family memory and daily reflection

Before starting the service, create `~/.local/share/family/` (0700) with `alex.md`, `maru.md`, `watchlist.md` and `network-issues.md` (0600). Keep personal data outside Git. Profiles load into Pi's initial context; the agent re-reads them before personalized recommendations so later edits are visible without replacing the shared session.

Copy `systemd/pi-telegram-reflection.service` and `.timer` into `~/.config/systemd/user/`, run `systemctl --user daemon-reload`, then `systemctl --user enable --now pi-telegram-reflection.timer`. The timer runs at **03:00 Europe/Moscow** and reviews the previous Moscow calendar day across every file in `~/.pi/agent/sessions/pi-telegram/`, including sessions created with `/new` and sessions that began earlier. An offline host catches the missed timer on startup (`Persistent=true`).

Reflection uses the existing `openai-codex` OAuth authorization with **gpt-6-luna / max**. It has a separate native session under `~/.pi/agent/sessions/family-reflection/`, no Telegram extension and no tools. The host supplies only user text and completed assistant replies, excluding tool calls, tool results, failed responses and internal thinking. Assistant replies provide context; only explicit participant statements prove family facts. Credentials are redacted and returned memory changes are validated. Concurrent edits reject publication instead of overwriting live memory. Failures appear in `journalctl --user -u pi-telegram-reflection`; there is no model fallback.

Reflection can update only `alex.md`, `maru.md` and `watchlist.md`; it cannot read or change `network-issues.md`. Short reports of newly saved facts are at `~/.local/share/family/reflections/YYYY-MM-DD.md`. Technical analysis and improvement proposals are excluded. If nothing new is confirmed, the report says “Новых семейных фактов нет.” A rerun replaces that day's report and merges facts against current memory.

For a live test, run `node scripts/reflect.mjs --date YYYY-MM-DD` with the service environment loaded. An unfinished day is marked partial and will be reviewed again by the scheduled run. `--inspect` lists the selected source entries without calling the model or changing memory. After setup, check `systemctl --user list-timers pi-telegram-reflection.timer`.

To update, pull `main`, run `npm ci` and `npm run build`, then `systemctl --user restart pi-telegram pi-telegram-cinema`. Keep this as the deployment path; GitHub Actions validates the fork.

## Local Bot API and family media

On VM106 use `scripts/local-bot-api.compose.yaml` as the Docker Compose source,
installed at `~/.config/telegram-bot-api/compose.yaml`. Its private `secrets.env`
contains TELEGRAM_API_ID and TELEGRAM_API_HASH (0600, outside Git). The service
runs as uid/gid 1000, listens only at 127.0.0.1:8081 and shares the same absolute
on-disk paths for Bot API data and generated attachments. Do not expose its port
or print/list bot-token-named data directories.

Both Pi services read these additions from their existing private EnvironmentFile:

```sh
PI_TELEGRAM_API_BASE=http://127.0.0.1:8081
PI_TELEGRAM_LOCAL_FILE_ROOTS=/home/alex/.local/share/telegram-bot-api:/home/alex/.local/share/botflix/attachments:/home/alex/.pi/agent/tmp/pi-telegram/attachments
PI_TELEGRAM_OUTBOUND_ATTACHMENT_MAX_BYTES=2000000000
PI_TELEGRAM_INBOUND_FILE_MAX_BYTES=2000000000
```

Local Bot API supports 2000 MB uploads. Shared real paths are sent as file URIs;
local getFile results are copied only from configured shared roots. Other paths
use streamed multipart. Before moving a bot from api.telegram.org, stop both Pi
services and call cloud logOut once, then restart both against the local server.
Never keep the same bot logged in at both endpoints. This briefly interrupts
chat replies; native sessions remain on disk. Reverting to cloud Bot API after
logOut is subject to Telegram's ten-minute restriction.

`telegram_attach` batches consecutive photos into albums of 2–10, keeps the exact
forum target and reply, and sends MP4 through sendVideo. An uncertain album
result is not retried as individual uploads. One photo or a document stays one
attachment. Both direct and follower delivery use the same path.

BotFlix CLI supplies catalog, exact-ID poster/trailers, advisory woke checks and
video download/preparation. Telegram delivery remains in Pi. General AGENTS.md
loads in both topics; CINEMA.md adds only the cinema workflow. Family Markdown
files contain tastes and viewing facts, not credentials or a second ratings
cache. Site scores may warn but never filter recommendations or downloads.
