# Health CLI: initial read-only review

Health is now the third native Pi session. Mi Band Bot remains running and owns
Xiaomi sync, its Telegram UI, credentials and existing SQLite records. This review
does not authorize changing it or implementing the migration yet.

The source is in the existing miband-bot repository; VM106 deploys it from
`~/miband-bot-ts` with data at `/opt/miband-tracker/data`. The service is ready at
127.0.0.1:18080. Its HTTP endpoints expose readiness and webhooks, not a health-data
API. Calling its sync routine can refresh tokens and write databases and status
files, so it is not a read-only query.

## Useful CLI scope

- `status`: coverage and latest data per person and metric, source errors and auth state without secrets.
- `summary` and periods: steps, distance, activity, sleep, heart rate, oxygen, stress, weight and workouts.
- `trends` and `compare`: retain dates, metric types and missing data; do not mix people or mistake daily averages for point measurements.
- `export`: explicit private JSON/CSV artifacts, attached by Pi when requested.
- A later deliberate ownership cutover for Xiaomi QR login, refresh and sync. Pi owns scheduling and Telegram; CLI should not contain a second bot or scheduler.

Keep the future implementation a separate Go CLI, like BotFlix: direct source
access, one JSON object, no HTTP server, Telegram client or ORM.

Start with the existing SQLite data. Each person already has a separate
`miband_<Telegram ID>.db`; the plain `miband.db` is empty. Confirm the identity
mapping before showing another person's records. The second configured account
must not be assigned to Maru merely because two databases exist.

Tables cover steps_daily, sleep_daily, sleep_stages, heart_rate, blood_oxygen,
stress, calories_daily, weight and workouts. Some endpoints can fail while the
current sync result still reports success, so metric-level errors and freshness
need to survive into CLI JSON. Keep existing records and exact account ownership.
Do not backfill absent data with zero.

## Leave in Pi or discard during cutover

Telegram menus, callback state, language switchers, menu refresh loops and
notification delivery belong to Pi. The bot's game-like family rankings and
hardcoded outing suggestions are optional conversation, not a required storage
or CLI abstraction. Preserve collection and exports before rebuilding decoration.

Health's plans and checkboxes belong to its private Markdown notes. Wearable
estimates are not clinical diagnoses; medical research must cite current primary
sources. No data or credentials from Mi Band Bot were copied into Git.
