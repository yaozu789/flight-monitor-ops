# Flight Monitor: context for Claude Code

Personal cheap-flight radar for Yaozu. It watches SFO/OAK/SJC round trips to destination groups (Phase 1: Japan $500, NYC $250), 3–9 months out, 5–14-day trips, economy, ≤1 stop. It alerts only on **rechecked, under-cap** fares. Honest framing: best-effort sampled coverage, "price rechecked", never "bookable".

## Roles (see `WORKFLOW.md`)
- **Claude Code:** writes code and tests, opens PRs from `claude/<topic>` branches, and merges after CI is green and review has no blocking objections.
- **Codex** (`codex exec -m gpt-6-sol`; Astra only for big design reviews or full audits, since it burns quota): main reviewer. **Cursor** (`cursor-agent -p --mode ask --model gpt-5.6-sol-xhigh`) is the backup when Codex is out of quota. The author is never the main reviewer, and reviewers stay non-Claude.
- **Muse:** hosts and runs the monitor on its cloud VM, as an agent turn per scheduled run. It pulls over **HTTPS** with a read-only fine-grained token, stored in `/home/hatch/.git-credentials` (mode 600) on its VM. Its SSH deploy key is a backup, because the proxy often kills SSH. It reads requests from `ops/muse-inbox/` (newest file) and replies in chat. If it can't pull, paste the inbox file, including code patches with sha256 hashes. Muse's real home is `/home/hatch`, not `/root`. Yaozu pastes the replies here, and Claude files them as issues.
- **Yaozu:** decides and **approves every deploy**, schedule, limit, destination or credential change.

## Commands
```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt   # fli pinned to git commit 881aee5 (0.10.0 is not on PyPI)
.venv/bin/python -m unittest discover -s tests                        # offline: fake transport, no network, no secrets
```
CI (`.github/workflows/tests.yml`) runs the same suite on every push and PR. Its count is the verdict.

## Architecture
- `run.py`: the CLI. `check --scheduled` is the scheduled cycle, and `run_guarded` wraps it with: lock → drain → backoff/slot no-op → fn with run deadline → heartbeat.
- `cmd_check` order: mint cells → drain → retry recheck → leftover verify → `sentinel.collect` → **same-slot verify** → burst triggers → prepare handoffs. Every output, including no-ops and stops, carries `handoff.alerts` for the agent to relay verbatim.
- `transport.py`: wraps curl_cffi `Session.get/post`. It counts every HTTP attempt before sending, fails closed with no guard, forces concurrency to 1, and detects 429/CAPTCHA/blank pages and persists the backoff.
- `budget.py`: `BudgetGuard.reserve()` is atomic (`BEGIN IMMEDIATE`, separate connection). Limits: 150/day (acceptance phase), a per-run allowance, a 24-attempt verify reserve, and category caps. `BudgetExhausted` **must stay a BaseException**, or fli wraps and retries it.
- `sentinel.py`: about 27 fixed departure anchors per group (7-day stays) and a weekly background grid, both on a fixed calendar lattice (`GRID_EPOCH`), so the grid doesn't grow as days pass. Also bursts (60 attempts, resumable) and pacing across the day's runs at `run_every_minutes`. Known limits: changing `trip_length_days` during an active burst can skip or repeat a few of its duration steps, and changing a group's airports reuses its cells' price baselines for drop triggers (72 h window). Bursts expire in 24 h.
- `verifier.py`: rechecks the same fingerprint (includes flight numbers and times). Price must be ≤ cap, with **no tolerance**, and there are **no new-low alerts**.
- `outbox.py` + `store.py`: states are `pending → handoff_prepared → relay_attempted_unconfirmed` (scheduler), and `user_confirmed` is human-only via `outbox-confirm`. There is no "sent" state, because Muse push gives no receipt. At most 2 handoffs, rechecked before a retry. Dedup is per (watch, departure week), re-alerting only on a drop of at least max($15, 5%).
- `heartbeat.py`: healthchecks.io, pinged only on `--scheduled` runs. Success on a finished cycle, including legitimate no-ops. `/fail` on a crash, a non-zero return, 3 lock losses in a row, or 3 deadline stops in a row. The URL is read from the `HEALTHCHECKS_PING_URL` env var (on Muse: `.env`, mode 600), never logged.
- fli convention: for round trips, **the outbound leg's price is the full round-trip total** (fli's own CLI and tests).
- Itinerary endpoints are each direction's first departure and **final** arrival. Observations written before PR #17 stored the first leg's arrival (the layover) as `destination` for connecting flights, so their fingerprints differ from new rows. They are historical only (the live outbox was empty when this changed). Rechecks query the watch's grouped airports and match the exact fingerprint.

## Rules
- Never commit `data/`, `*.db`, `.env`, tokens, or the ping URL.
- Test fixtures stay inside the repo, with no machine-specific paths.
- **A new test must fail on the code before its fix.** Check this by running the tests against the old file.
- Prefer tests that drive the real `BudgetGuard`/outbox/`run_guarded` paths and assert on printed output. Tests that stubbed both collection and verification missed real bugs.
- Don't send real flight searches from the Mac without Yaozu's OK. Google rate-limits, and the budget exists for a reason.

## Design docs
- `docs/spec.md`: the agreed design (the Claude × Codex debate, converged 2026-09-24).
- **Decisions after the spec, which override it:**
  - alerts go through Muse push, not Telegram, and are never marked sent
  - scheduled runs are agent turns on Muse, not a script-only cron, accepted with code-enforced guardrails
  - Claude writes the code, not Muse
  - trial cadence is 30 min, dropping to 2h if Muse's weekly quota projection says so
  - heartbeat alerts go by email
- `PLAN.md` and `PROGRESS.md`: Muse's original plan and Phase 1 report (historical).
- Current status and next steps: open GitHub issues/PRs, and the newest file in `ops/muse-inbox/`.
