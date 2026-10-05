# Family forum deployment

The forum has two independent native Pi sessions, served by one process and one Telegram poller. AI keeps the ordinary bridge and its shared controls. Cinema (topic 3) has its own history, prompt and model, with only the bounded `botflix` tool. Both Alex and Maru share each topic's conversation; bot messages are ignored. General remains silent. The old Telegram BotFlix runs independently.

Cinema accepts text requests and `/start`, `/new`, `/compact`, `/stop`, `/model`; `/model provider/model` changes only Cinema's model. Voice, attachments and the ordinary inline settings menu remain in AI. Cinema has no shell, filesystem tools, browser authentication, arbitrary local torrent files, poster file writes or file deletion. Download URLs are limited to the configured tracker domains. Removing a torrent preserves its files. BotFlix's service credentials remain outside model context.

The trusted host registers Cinema's exact forum destination with `registerTelegramDeliveryTarget` from the delivery API and consumes its human text updates through the public update-handler API. It neither starts another poller nor grants a model a destination-registration tool. Disposing the host revokes that delivery target.

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
cp systemd/pi-telegram.service ~/.config/systemd/user/
sudo loginctl enable-linger "$USER"
systemctl --user daemon-reload
systemctl --user enable --now pi-telegram
curl --fail http://127.0.0.1:8186/healthz
```

`/start` opens the menu; `/model` selects the model; `/compact` summarizes context; `/new` starts a fresh shared session with native confirmation and retains this topic. A model switch continues the same native conversation. Older sessions remain on disk.

Runtime instructions are in [agent/AGENTS.md](../agent/AGENTS.md) for AI and [agent/CINEMA.md](../agent/CINEMA.md) for Cinema. Both native histories live under `~/.pi/agent/sessions/pi-telegram`, isolated by session CWD (home for AI, `~/projects/home/cli-botlix` for Cinema), and resume after restart. `/new` affects only its topic. Diagnose with `journalctl --user -u pi-telegram`; health at `http://127.0.0.1:8186/healthz` includes both session IDs and Cinema's active tools. Interrupted in-memory queued work is not replayed; resend it.

## Private family memory and daily reflection

Before starting the service, create `~/.local/share/family/` (0700) with `alex.md`, `maru.md`, `watchlist.md` and `network-issues.md` (0600). Keep personal data outside Git. Profiles load into Pi's initial context; the agent re-reads them before personalized recommendations so later edits are visible without replacing the shared session.

Copy `systemd/pi-telegram-reflection.service` and `.timer` into `~/.config/systemd/user/`, run `systemctl --user daemon-reload`, then `systemctl --user enable --now pi-telegram-reflection.timer`. The timer runs at **03:00 Europe/Moscow** and reviews the previous Moscow calendar day across every file in `~/.pi/agent/sessions/pi-telegram/`, including sessions created with `/new` and sessions that began earlier. An offline host catches the missed timer on startup (`Persistent=true`).

Reflection uses the existing `openai-codex` OAuth authorization with **gpt-6-luna / max**. It has a separate native session under `~/.pi/agent/sessions/family-reflection/`, no Telegram extension and no tools. The host supplies only user text and completed assistant replies, excluding tool calls, tool results, failed responses and internal thinking. Assistant replies provide context; only explicit participant statements prove family facts. Credentials are redacted and returned memory changes are validated. Concurrent edits reject publication instead of overwriting live memory. Failures appear in `journalctl --user -u pi-telegram-reflection`; there is no model fallback.

Reflection can update only `alex.md`, `maru.md` and `watchlist.md`; it cannot read or change `network-issues.md`. Short reports of newly saved facts are at `~/.local/share/family/reflections/YYYY-MM-DD.md`. Technical analysis and improvement proposals are excluded. If nothing new is confirmed, the report says “Новых семейных фактов нет.” A rerun replaces that day's report and merges facts against current memory.

For a live test, run `node scripts/reflect.mjs --date YYYY-MM-DD` with the service environment loaded. An unfinished day is marked partial and will be reviewed again by the scheduled run. `--inspect` lists the selected source entries without calling the model or changing memory. After setup, check `systemctl --user list-timers pi-telegram-reflection.timer`.

To update, pull `main`, run `npm ci` and `npm run build`, then `systemctl --user restart pi-telegram`. Keep this as the deployment path; GitHub Actions validates the fork.
