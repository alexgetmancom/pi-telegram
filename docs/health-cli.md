# Family Health CLI

The native Health Pi session binds forum topic 359, uses its own conversation and private health profiles, and has the same host and Telegram tools as AI/Cinema. Its Go CLI is maintained in `/home/alex/projects/home/health-cli` and installed at `/home/alex/.local/bin/health`. See that repository's README for configuration and commands.

Use `health status` to check each metric's coverage/error, `health compare --period 7d` for both people, and `health export --person maru --metric sleep --period 30d --format csv` to produce a private attachment. Pi runs `health tick` each minute; the CLI enforces 15-minute sync cadence and person locks. Weekly delivery is Sunday after 21:00 Moscow into Health, with private receipts under the Health memory directory. Claimed/uncertain deliveries are not retried automatically; verify Telegram before resetting a receipt.

History remains in the existing SQLite files under `/opt/miband-tracker/data`; confirmed ownership lives in private Health profiles. CLI tokens/configuration live separately under `~/.config/health` with permissions 0600. The old `miband-bot-ts` container is stopped and its restart disabled to prevent a second writer. Its source and original credentials remain preserved. Do not restart it alongside CLI sync.

Maru's FDS nighttime detail currently returns Xiaomi `device not exist`. CLI preserves this metric error while still importing other metrics. Alex's newest data can be stale independently by metric. Check actual coverage rather than infer recency from sync success. No missing samples are synthesized.
