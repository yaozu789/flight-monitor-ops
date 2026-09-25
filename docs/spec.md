# Flight Monitor — Final Spec (Claude × Codex, converged)

**Date:** 2026-09-24 · **Status:** agreed design, not built · **Supersedes:** `Flight_monitor_plan.md` where they conflict
**Sources:** Muse plan, Codex validation + runtime plan, 3-round debate in `Flight_monitor_debate.md`.

**Promise (honest):** best-effort, sampled deal monitoring. It detects a sale only if the sale affects a cell we sample and is still live at the next successful observation. Not exhaustive, not a mistake-fare guarantee, not bookability confirmation.

## 1. Scope (unchanged from Muse plan)

- Origins: SFO, OAK, SJC (grouped). Economy, 1 adult, USD round trip, ≤1 stop.
- Trip length 5–14 days eligible; departures 3–9 months out (rolling).
- Groups and caps: NYC (EWR/JFK/LGA) $250 · Japan (NRT/HND/KIX) $500 · HKG $500 · Shanghai (PVG/SHA) $550 · Beijing (PEK/PKX) $550 · LA (LAX/BUR/LGB/SNA/ONT) $120 · San Diego (SAN) $150.
- Backstop outside the code: Google Flights "Track prices → Any dates" per group.

## 2. Runtime

| Piece | Choice |
|---|---|
| Host | **Decision (Yaozu, 2026-09-24): Muse cloud first**, if it passes Phase 0. Fallback: always-on **Windows PC** (native, no WSL) |
| Scheduler | Muse scheduler running the script with no LLM per run. Fallback: **Hermes cron `no_agent=True`** on the PC (PC rarely reboots; Hermes already on Telegram). Either way: every 30 min, no overlapping instances, bounded run time, no catch-up bursts |
| Code | Deterministic Python + SQLite, one worker lock, network waits outside write transactions |
| Data source | `fli` pinned to an exact **git commit** (PyPI 0.9.0 ≠ upstream 0.10.0), all deps pinned, internal concurrency = 1 |
| Alerts | **Delivery adapter** returning provider acknowledgment + receipt ID where available. Muse: programmatic push if it returns an ack; otherwise direct Telegram Bot API. PC fallback: Hermes Telegram, or direct Telegram Bot API if Hermes stdout delivery gives no ack. Printing to stdout is not proof of delivery; without an ack the outbox row stays "unconfirmed", never "sent" |
| Liveness | healthchecks.io (notifies via its **own** integration, independent of the host): ping every run including intentional budget exhaustion/backoff, **period 30 min + grace 60 min** (alerts 90 min after last heartbeat); own Telegram integration |
| Build/deploy | Built on Mac (Claude Code or Codex), git; deploy tested commit manually to the host (Muse or PC). No auto-pull in production |
| Hermes | Scheduler + Telegram on PC fallback; optional chat front-end (read SQLite, edit `watches.yaml`) |

Windows setup: sleep disabled on AC, Windows Update active hours set, explicit working directory.

## 3. Collection budget

Counting = actual HTTP attempts at transport layer, retries included. Sequential. Circuit breaker on throttling (honor `Retry-After`, suspend collection, don't burn reserve).

| Work | Attempts/day (production) |
|---|---:|
| Daily sentinels: 27 fixed departure anchors × 7 groups × 7-day stay | 189 |
| Background exploration across all 5–14 durations | 100 |
| Up to 2 bursts × 60 | 120 |
| Complete-itinerary checks + price rechecks | 42 |
| Retries / diagnostics reserve | 49 |
| **Daily hard ceiling** | **500** |

- Acceptance phase: **150/day**, Japan + NYC only.
- Per-run max **80 attempts**; normal runs paced near ~10–11.
- Persist per-category spend + calendar-day ceiling in SQLite (not only a token bucket).
- Priority: verification > bursts > background. Unused burst budget flows to background.
- Sentinels: roughly weekly anchors spread across weekdays, **fixed** until they leave the window; spread through 24h.
- Background coverage is slow (~127 days for full non-sentinel space) — reported, not hidden.

## 4. Detection

**Triggers for exploration:**
- New under-cap observation (event, not a persistent state — no infinite re-bursts).
- ≥20% drop vs same cell's last successful observation (≤72h old).

**Burst (60 attempts):**
1. Verify + alert on the triggering deal first (verification budget). Never delay a verified alert to search for better.
2. 20 neighboring departure dates at the trigger's duration.
3. ≤40 previously unchecked neighboring date × duration cells across the other 9 durations.

Dedup bursts per destination + departure week for 24h; prefer destinations not yet served today; revalidate queued triggers before spending next day's budget. Burst = sample, not proof nothing better exists.

**Deferred:** relative-only consumer alerts, Google typical-range scoring (time-box ≤2h payload investigation; require match with visible Google range on 3 searches before use).

## 5. Alerts

- Consumer alert only if **rechecked and under cap**. Label "price rechecked <timestamp>", never "bookable".
- First observation seeds history; may still alert via cap.
- Aggregate per destination, ≤3 options.
- Same departure week: re-alert only after drop ≥ **max($15, 5%)**.
- Newly qualifying departure week: next daily digest for that destination.
- Message shows: total RT USD, dates, actual airports both ways, airline, stops, elapsed time, baggage/basic-economy = unknown, booking link, observation age.
- Durable outbox, stable alert IDs; mark sent only on provider ack (e.g. Telegram `ok:true`) + store receipt/message ID; test forced delivery failure + retry. Occasional duplicate acceptable; exactly-once not promised.
- Hidden-city / self-transfer results excluded from normal alerts.

## 6. Health

Tracked separately:
- Scheduler liveness (healthchecks.io), pinged even when collection is paused; collection failure reported separately.
- Per-group successful collection freshness; failed vs empty vs priced-no-deal distinguished (empty ≠ "no deals").
- Source health probe = one designated sentinel cell; failure classified, not auto-declared outage.
- Pending delivery backlog.
- Dedup health alerts; send recovery notice.

**Sunday digest:** successful coverage, observation age, failed/unpriced cells, attempts used by category, burst yield (which path found each verified deal), cheapest observations *with timestamps*.

## 7. Data (SQLite)

`watches`, `cells` (group, depart_date, duration, role: sentinel/background), `observations` (cell, fingerprint, airports, airline, stops, price_usd, currency, source, commit, observed_at, status), `triggers`, `bursts`, `alerts` + `outbox` (alert_id, telegram_message_id, sent_at), `budget_ledger` (date, category, attempts), `health`. Raw observations kept; daily per-cell summaries derived.

## 8. Phases and acceptance

**Phase 0 — feasibility on the target host (Muse first; see `Muse_feasibility_test.md`):** pinned fli commit, concurrency 1, `top_n=1`, ≤80 attempts total; 5 Japan/NYC searches; demonstrated persistence, model-free scheduled execution, background execution, delivery ack, outbound heartbeat. PASS = permission to start the 72-hour run only. Transient 429 = PARTIAL + bounded retest; repeated blocks = FAIL → Windows PC Phase 0.

**Phase 1 — MVP, 72-hour acceptance at 150/day, Japan + NYC.** Must prove:
1. Restart recovery: **Muse** — scheduled runs continue with app closed and survive a supported runtime restart. **Windows fallback** — reboot to login screen (no login) → collection resumes, credentials readable, delivery acked (required even though PC rarely reboots).
2. Controlled qualifying fare → phone notification; duplicate suppression works.
3. Injected retries + mid-run kill → budget ledger stays correct.
4. Scheduler disabled / network down → healthchecks alert (own integration) within ~90 min; recovery notice.

**Phase 2:** all 7 groups at 500/day; Sunday digest; Hermes chat front-end optional.
**After 2 weeks:** reallocate budget using burst-yield data. No invented recall numbers.

## 9. Known limits (say these out loud)

- Sampled coverage; misses sales on unsampled dates/durations/airports.
- Grouped-airport minimum may not equal best per-pair fare.
- Recheck ≠ bookable; baggage inclusions unknown.
- Google page changes break `fli` unpredictably; health alerts catch it, but a working upstream fix is not guaranteed.
- Scraping Google Flights is outside Google's intended use; keep volume modest.
