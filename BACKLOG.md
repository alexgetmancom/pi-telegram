# Project Backlog

_This file owns unresolved project work only. Completed behavior belongs in `CHANGELOG.md`; durable contracts belong in `AGENTS.md` and `/docs`._

## Post-Release Transport Recovery Acceptance

- Release boundary: The operator authorized the hotfix on native regression and CI evidence; real Telegram smoke/fault acceptance remains open below, not a claimed completed check. Installing/reloading Pi or injecting live faults needs separate authorization.
- CI boundary: Linux checks exercise capped poll/admission backoff, preserved persistent-conflict stand-down and generation-fenced manual disconnect. Green CI does not replace operator acceptance.
- [ ] Obtain operator acceptance for ordinary disconnect, unconfirmed-cleanup reporting and outage recovery without manual reconnect on a disposable setup. Local fake-transport/storage regressions do not certify real Telegram or platform fault behavior; installation/reload and live fault injection require separate authorization.
- Acceptance: Captured manual stop never affects a replacement connection or bypasses Thread deletion fences, uncertain stop is not retried, and accepted local work is preserved. Session-restart cleanup remains admission-required.

## Consistent Callback Notification Copy Across Other Extensions

- [ ] Inventory Telegram button callback acknowledgements across all local extensions, including companion callbacks. Standardize the transient non-interactive notification bubbles (callback answers/toasts) to concise, consistent text with no trailing period.
- [ ] Distinguish transient callback bubbles from in-chat notices, chooser headings and blocking alerts; reconcile the owning UI-copy contracts before implementation rather than stripping punctuation from every message.
- Acceptance: All participating extensions follow the same callback-notification format, with focused copy checks for success, no-op and rejection outcomes; in-chat notice formatting and callback behavior remain unchanged.
