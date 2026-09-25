# Cheap Flight Monitor — Finalized Plan

**Owner:** Yaozu Li · **Date:** 2026-09-24 · **Status:** Plan finalized, ready for detailed technical planning (Codex) then build
**Runtime owner:** Muse (scheduler + alerts) · **Code planner:** Codex · **Build orchestrator:** Muse

## 1. Goal

A personal, continuously running flight-deal radar: Bay Area → user's destinations, flexible dates, that messages Yaozu (chat + phone push) when a genuinely cheap fare appears — including flash sales and possible mistake fares — with price history so "cheap" is measured, not guessed.

Non-goals: booking/purchasing, multi-user support, public product, fare prediction.

## 2. Scope (locked)

**Origins (grouped as one metro):** SFO, OAK, SJC

**Destination groups (all nearby airports included, per user):**

| Group | Airports | Default price cap (USD RT) |
|---|---|---|
| NYC area | EWR, JFK, LGA | $250 |
| Japan | NRT, HND, KIX | $500 |
| Hong Kong | HKG | $500 |
| Shanghai | PVG, SHA | $550 |
| Beijing | PEK, PKX | $550 |
| LA area | LAX, BUR, LGB, SNA, ONT | $120 |
| San Diego | SAN | $150 |

("11 Dickinson Street, NJ" mapped to NYC-area airports — assumption noted, user did not dispute.)

**Defaults (per-watch overridable):** economy cabin, max 1 stop, trip length 5–14 days, departure window next 3–9 months. Price caps above are starting defaults; user adjusts.

## 3. Architecture

```
watches.yaml ──▶ poller ──▶ SQLite ──▶ scorer ──▶ verifier ──▶ alerter (Muse cron → chat + push)
                    │                     ▲
                    └──── canary ─────────┘ (self-health: poller broken → alert "monitor needs attention")
```

**Components:**
- **Config** (`watches.yaml`): one watch per destination group — origins, airports, date window, trip-length range, stops, cabin, absolute cap, relative threshold, cadence. See Appendix A.
- **Poller** (Python): uses the `flights` PyPI package (punitarani/fli, v0.10.0 — **verified working 2026-09-24** via live SFO→NRT queries: flex sweep 15 date combos in ~2m49s; specific-date RT query ~2.2s with full itinerary detail + booking deep links). Two rhythms:
  - *Broad scan (daily):* flex-date sweep per origin-metro × destination-group (7 sweeps ≈ ~20 min). Identifies cheap date combos; refreshes baselines.
  - *Narrow poll (every 2h):* specific-date round-trip queries only for candidate dates from the broad scan (~2s each; ~35 queries ≈ minutes). Cheap enough for frequent polling.
  - Metro-grouped queries keep the request budget bounded (see §4). **Open verification item:** confirm the flex endpoint supports multi-airport origin/destination in one query; if not, fall back to per-airport sweeps on a staggered rotation.
- **Store** (SQLite, single file): `watches`, `observations` (itinerary fingerprint, price, timestamps, airline, stops, booking URL, source), `alerts`. Schema in Appendix B.
- **Scorer:** absolute cap breach → alert; new observed low → alert; relative discount ≥35% vs rolling median → alert (enabled after ≥30 observations per route; cold start uses absolute + new-low only).
- **Verifier:** every alert candidate gets a second live specific-date query; alert only if the fare confirms. Kills false positives from stale/cached data.
- **Alerter:** owned by Muse's scheduler — cron job runs poller+scorer, result message → chat + phone push. Dedup by itinerary fingerprint + 24h cooldown (re-alert only on further meaningful drop or material itinerary change).
- **Canary:** each run includes a fixed reference query; if it fails or returns empty unexpectedly → alert "monitor needs attention" instead of silent missing. **Critical:** empty results must be treated as "unknown," never "no deals" (quiet-market vs broken-pipe).

**Request politeness:** jitter between requests, exponential backoff on 429/empty, tenacity retries, modest concurrency (sequential preferred). No parallel hammering of Google.

## 4. Request-budget math

- 7 destination groups × 1 origin metro = **7 flex sweeps/day** ≈ 20 min/day. Well within polite limits.
- Narrow polls: ~5 candidate dates × 7 groups = 35 queries every 2h ≈ 420/day at ~2s each. Fine.
- Full per-airport fallback (if metro queries unsupported): 51 O-D pairs — too heavy for daily sweeps; stagger across days or drop to top-2 airports per group for broad scan, full set for narrow verification only.

## 5. Failure modes & mitigations

| Failure | Mitigation |
|---|---|
| Google changes page payload / gates RPC (happened Aug 2026; expect a few times/year) | Canary detects; alert user; `flights` lib is actively maintained — usually fixed in days; optional fallback to Skiplagged/Kiwi MCP (no key) for continuity |
| HTTP 429 / IP throttling | Backoff + jitter; reduce cadence automatically; alert if persistent |
| Stale/cached fare → false alert | Second-query verification before every alert |
| Alert spam | Fingerprint dedup + 24h cooldown |
| Cold-start (no history) | Absolute caps + new-low alerts only until 30 observations/route |

## 6. Phases

- **Phase 1 (MVP):** config + poller + SQLite + scorer + verifier + canary. 2 watches live (Japan, NYC). Muse cron: daily broad scan + 2-hourly narrow poll. Verify real alerts end-to-end.
- **Phase 2:** all 7 destination groups; rolling-median anomaly alerts; staggered scheduling; weekly digest option.
- **Phase 3 (later):** region/anywhere discovery, Telegram alerts, adaptive scheduling, booking-link validation.

## 7. Open questions for Codex detailed planning

1. Confirm multi-airport support in `flights` flex-date endpoint; design fallback if absent.
2. Exact cron split: one job vs per-group jobs; overlap/locking strategy for the ~20-min daily scan.
3. SQLite schema finalization + retention policy (how long to keep observations).
4. Threshold tuning: per-group relative thresholds after history accumulates; new-low definition (per itinerary fingerprint vs per route-date).
5. Notification copy format: what each alert message contains (price, dates, airline, stops, booking link, vs-baseline context).
6. Telegram bot as secondary channel — deferred to Phase 3 unless trivial.
7. Repo layout + how Muse's cron invokes the poller (venv path, entrypoint contract: exit codes, JSON output).

## Appendix A — watches.yaml sketch

```yaml
defaults:
  origins: [SFO, OAK, SJC]
  cabin: economy
  max_stops: 1
  trip_length_days: [5, 14]
  window_months: [3, 9]
  relative_threshold: 0.35
  cooldown_hours: 24

watches:
  - id: japan
    destinations: [NRT, HND, KIX]
    absolute_cap_usd: 500
  - id: nyc
    destinations: [EWR, JFK, LGA]
    absolute_cap_usd: 250
  # ... hkg, shanghai, beijing, la, san-diego
```

## Appendix B — SQLite tables sketch

```sql
watches(id, destinations_json, absolute_cap_usd, relative_threshold, config_json);
observations(id, watch_id, fingerprint, origin, destination, depart_date, return_date,
             airline, stops, cabin, price_usd, booking_url, source, observed_at);
alerts(id, observation_id, reason, score, verified_at, sent_at, channel);
health(id, check_name, ok, detail, checked_at);  -- canary results
```

---

*Research basis: ChatGPT handoff (2026-09) + Muse verification 2026-09-24: live `flights` v0.10.0 test (SFO→NRT flex + specific-date, no CAPTCHA/rate-limit); fli repo active (commits Sep 2026, page-HTML pivot after Aug 2026 Google RPC gating); Codex/Claude Code comparison (Codex: cloud automations run laptop-off but no native condition-push; Claude Code: native push but cloud egress likely blocks flight APIs, /loop dies on sleep). Decision: Muse owns runtime (cron + chat/push alerts, $0 extra); Codex does detailed code planning; Muse orchestrates the build.*
