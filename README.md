# Cheap Flight Monitor — Phase 1 (MVP)

Personal flight-deal radar: Bay Area (SFO/OAK/SJC) → Japan + NYC area, flexible
dates. Polls Google Flights via the `flights` PyPI package (fli), keeps price
history in SQLite, scores deals, verifies fares with a second live query before
alerting, and self-checks with a canary query.

## Setup

Uses the existing venv with `flights` installed:

```bash
V=~/workspace/flitest_venv/bin/python
```

No cron is wired here — the scheduler invokes `run.py` and reads its JSON output.

## Commands

```bash
cd ~/workspace/"Flight Monitor"

# Flex-date sweep for a watch (SLOW: ~3 min per 15-day window chunk;
# the 3-9 month window is chunked into <=93-day pieces). Prints cheapest
# date combos; stores them in broad_scans for Phase 2 baselines.
$V run.py broad --watch japan [--duration 7]

# Specific-date round trip across all origin x destination airports (~2s per query)
$V run.py narrow --watch japan --depart 2026-12-04 --return 2026-12-11

# Self-health: fixed SFO->NRT reference query. Exit 0 = data layer alive,
# exit 2 = blind (Google changed something / rate-limited).
$V run.py canary

# The 2-hourly job: narrow polls on sample dates for all watches (or one),
# stores observations, scores (absolute cap / new low), verifies candidates
# with a second query, records alerts. Prints JSON summary; exit 0 even
# with zero deals, 1 only on infrastructure failure.
$V run.py check [--watch japan]
```

## How alerting works

`check` prints `{"watches": {"japan": {"observations": N, "alerts": [...]}}}`.
Each alert was live-verified and recorded in the `alerts` table. The scheduler
turns these into chat + push messages. Dedup: same itinerary fingerprint won't
re-alert within `cooldown_hours` (24h).

## Files

- `watches.yaml` — watch config (2 Phase-1 watches; Phase 2 adds 5 more)
- `run.py` — CLI entrypoint (broad / narrow / canary / check)
- `poller.py` — `broad_scan()` + `narrow_query()` via `flights`; empty results
  return `None` ("unknown"), never zero
- `store.py` — SQLite schema + helpers (`data/monitor.db`)
- `scorer.py` — absolute-cap + new-observed-low scoring (rolling median: Phase 2)
- `verifier.py` — second live query before any alert
- `canary.py` — fixed SFO→NRT reference query → `health` table
- `data/` — SQLite DB (created on first run)

## Known limitations (Phase 1)

- `check` uses fixed sample departures (+45/+75/+105/+135d, 7-day trips); using
  `broad` output to pick candidate dates is Phase 2.
- Scoring is absolute cap + new low only; no rolling-median anomaly yet.
- The `flights` library reverse-engineers Google Flights — expect breakage a few
  times a year; the canary exists to detect it.
