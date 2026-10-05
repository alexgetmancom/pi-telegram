# Family forum deployment

The forum uses one native Pi session. Every human in the configured supergroup topic can write, edit prompts, press shared buttons, switch models, compact and start a new session. `forumTarget` replaces the owner-ID admission rule for that profile; other chats, topics and bot senders are ignored. Default private-DM behavior is unchanged when this field is absent.

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

Runtime instructions are in [agent/AGENTS.md](../agent/AGENTS.md). Review its host paths before copying this setup to another machine. Diagnose with `journalctl --user -u pi-telegram` and `~/.pi/agent/tmp/pi-telegram/logs.jsonl`. Session history lives under `~/.pi/agent/sessions/pi-telegram` and resumes after restart. Interrupted in-memory queued work is not replayed; resend it.

To update, pull `main`, run `npm ci` and `npm run build`, then `systemctl --user restart pi-telegram`. Keep this as the deployment path; GitHub Actions validates the fork.
