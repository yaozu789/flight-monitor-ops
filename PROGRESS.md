# Cheap Flight Monitor — Progress Report

**Date:** 2026-09-24 · **Status:** Phase 1 live · **Owner:** Yaozu Li

## 1. Research

Started from a ChatGPT research handoff (`flight_price_monitor_research_handoff.md`) covering consumer products (Google Flights, Secret Flying, Going), GitHub projects (Flight Finder, Fli, fast-flights), historical data, mistake-fare policy, and data-source options.

I ran three parallel investigations to verify and extend it:

- **Flight-data tooling (current state, Sep 2026):** Fli is actively maintained and working (survived Google gating its internal RPC in Aug 2026 by pivoting to page-HTML parsing). fast-flights is maintained but fragile. Flight Finder is very active (1,159 commits, full self-hosted tracker with Playwright+LLM). Amadeus free tier returns mock data — rejected. SerpAPI is viable paid ($25–75/mo for a small watchlist). New free options found: Skiplagged MCP and Kiwi.com MCP (no API key), RapidAPI Google Flights Live, ScrapingBee credits.
- **Live hands-on test:** installed the library (PyPI name is `flights`, v0.10.0 — `pip install fli` fails) and ran real queries. SFO→NRT flexible-date search: 15 date combos in ~2m49s. Specific-date round trip: ~2.2s with full itinerary detail and booking deep links. No CAPTCHA, no rate limiting.
- **Codex vs Claude Code for 24/7 monitoring:** Codex has cloud automations that run with the laptop off but no native phone push on a condition. Claude Code has native phone push but its cloud sandbox likely can't reach flight APIs, and its local loop dies when the laptop sleeps. Verdict: neither is turnkey; Muse (built-in scheduler + chat/push alerts, $0 extra) is best positioned to own build + run + alert end to end.

## 2. Plan finalized

`~/workspace/Flight Monitor/PLAN.md`

- **Origins:** SFO, OAK, SJC (grouped as one metro)
- **Destinations (all nearby airports):** NYC area (EWR/JFK/LGA), Japan (NRT/HND/KIX), Hong Kong (HKG), Shanghai (PVG/SHA), Beijing (PEK/PKX), LA area (LAX/BUR/LGB/SNA/ONT), San Diego (SAN)
- **Defaults:** economy, max 1 stop, 5–14 day trips, 3–9 month window; price caps Japan $500, HKG $500, Shanghai/Beijing $550, NYC $250, LA $120, SD $150
- **Architecture:** `watches.yaml` → poller → SQLite → scorer → verifier → alerter (Muse cron → chat + phone push), plus a canary self-health check. Two rhythms: daily broad flex-date scan + 2-hourly narrow re-checks of candidate dates. Empty results treated as "unknown", never "no deals". Every alert re-verified with a second live query before notifying.

## 3. Codex detour (dropped)

Installed Codex CLI v0.149.0 and completed device-code login with Yaozu's ChatGPT account — then dropped per his decision ("maybe i have mistaken you as a CLI agent… best for you to just use your own ability"). The CLI remains installed and authenticated but unused.

## 4. Phase 1 build

Built in `~/workspace/Flight Monitor/` (Python, venv at `~/workspace/flitest_venv`):

| File | Role |
|---|---|
| `watches.yaml` | 2 watches: `japan` ($500 cap), `nyc` ($250 cap) |
| `poller.py` | `broad_scan()` flex sweeps (multi-airport works via Python API) + `narrow_query()` specific-date searches; sequential with jitter, one retry |
| `store.py` | SQLite (`data/monitor.db`): watches, observations, alerts, health, broad_scans |
| `scorer.py` | Absolute-cap breach + new observed low, 24h fingerprint cooldown dedup |
| `verifier.py` | Second live query per candidate; confirms only if fare still live (2% tolerance) |
| `canary.py` | Fixed SFO→NRT reference query → health table; exit 2 when data layer is blind |
| `run.py` | CLI: `broad` / `narrow` / `canary` / `check`; JSON to stdout; exit 0 on success even with zero deals |

**Verification (live, 2026-09-24):** full `run.py check` recorded **885 real observations** — Japan min $798, NYC min $337, both above caps so zero alerts (correct). Cross-checked: no under-cap price lacked an alert. Sample: SFO→NRT, ZIPAIR Tokyo, nonstop, $1,023.

## 5. Live operations

- Tracked item: **Cheap flight monitor** (`goal_4f18690ecb31`)
- Cron **`flight-monitor-check`** — every 2h (first run ~19:46 PDT 2026-09-24): canary → check → chat message with phone push only on verified alerts or canary failure; silent otherwise
- Cron **`flight-monitor-broad-scan`** — Sundays ~02:15 PT: full flex-date sweeps for both watches, building price history for Phase 2 anomaly detection

## 6. Next: Phase 2

Add the remaining 5 destination groups (Hong Kong, Shanghai, Beijing, LA area, San Diego) to `watches.yaml`, enable rolling-median anomaly alerts (≥35% below normal) once history accumulates, stagger scheduling for the larger route set. Phase 3 (later): anywhere-discovery, Telegram, adaptive scheduling.
