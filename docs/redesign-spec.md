# Flight Monitor redesign spec (converged 2026-09-28)

Source: the Claude × Codex redesign debate (6 rounds, 2026-09-28; kept in Yaozu's local project folder). Yaozu's decisions are fixed; open items are marked **[Yaozu]**.

## Goals (Yaozu)
- No absolute caps as the alert rule.
- A weekly digest of the cheapest observed fares.
- "Unusually cheap" alerts relative to each route's own history, armed only once enough history exists.
- A near-term window for all groups; a long-term window only for Asia.
- Phase 2 groups: HKG, Shanghai, Beijing, LA, San Diego.

## Windows (named, per watch in `watches.yaml`)
| Window | Departures | Lattice | Anchors | Refresh target | Groups |
|---|---|---|---|---|---|
| `near` | D+7 … D+42 | every 2 days, no weekday snap | 18 | 48 h | all 7 |
| `far` | 3–9 months | existing weekly lattice | about 26 | 72 h | Japan, HKG, Shanghai, Beijing |

- Mint the union of windows. Retire per (group, role, window). Schedule by per-window refresh deadline.
- Bursts are clipped to the trigger's window, for both neighbour dates and durations.
- **Trip lengths (Yaozu, 2026-09-28):**
  - **Asia** (Japan, HKG, Shanghai, Beijing): about a month, so sentinel 28 nights.
  - **LA / SD** (before activation in PR B, add a departure-time filter so Saturday means a *morning* flight): weekend trips, leaving **Friday or Saturday morning** and returning Sunday (Fri 2 nights, Sat 1; `return_weekday: 6`). The near window enumerates **every Friday and Saturday** in D+7…D+42, independent of the 2-day lattice: 5–6 of each, with no fixed cap. Bursts on these windows give each neighbour its own Sunday-return stay and skip the duration sweep.
  - **NYC:** 1 or 2 weeks, so sentinel cells at both 7 and 14 nights (2 × 18 near anchors).
- The background grid (10-duration exploration) is **disabled** (cap 0). **Later (Yaozu, 2026-09-28):** checking every trip length is wanted if bandwidth allows, but the priority lengths above come first. So exploration of other lengths returns only as **opportunistic surplus**: it spends only what is left after due sentinel work, verify, retry and bursts, and never delays a priority cell. That is a separate PR after stage 300 is stable.
- Windows of one watch must not overlap (config error), because cells are keyed by (group, date, length). A background upsert never changes an active sentinel's window or refresh target.
- **Durations are explicit per group, with no clamping.** Today `_sentinel_duration()` clamps to the 5–14 range, and `_burst_steps()` sweeps that range. Asia must include 28, LA/SD 2, and NYC must mint both 7- and 14-night cells. Test: each group mints its lengths; burst duration steps stay within the group's lengths and the 60-attempt cap, and never retry an unaffordable step.
- The window bounds the **departure** only. A return after the window (e.g. D+42 + 28) is valid.

## Budget (2 attempts per check at `top_n=1`)
- Full demand, with Yaozu's trip lengths. Each cell is checked every 48 h (near) or 72 h (far) at 2 attempts per check:

  | Part | Cells | Attempts/day |
  |---|---|---|
  | Asia near | 4 groups × 18 | 72 |
  | NYC near | 18 anchors × 2 lengths | 36 |
  | LA + SD near | 2 groups × 10–12 Fri/Sat | 20–24 |
  | Asia far | 4 groups × 26 | 69.3 |
  | **Total sentinel** | | **197.3–201.3** |

  This is below the 195.3 used in the debate, so every gate still holds.
- **Ceiling ramp 150 → 225 → 300.** At 300: 201.3 + verify 24 + retry 12 = 237.3 committed, with bursts (cap 60) running from the remaining surplus. Worst case 297.3. Gate 0.9 × 264 = 237.6 ≥ 201.3.
- **Stage activation gate:** demand ≤ 0.9 × (ceiling − verify reserve − retry cap).
  - Stage A (at 150): Japan near + far and NYC near (2 lengths) = 18 + 17.3 + 36 = **71.3/day**.
  - At 225: + LA and SD near, + HKG = 71.3 + 24 + 35.3 = 130.6 (gate 170.1).
  - At 300: all 7 (gate 237.6).
- **Step advancement** requires, over ≥ 5 days:
  - 0 throttles
  - unknown-result rate < 5% (provisional)
  - 0 deadline stops
  - worst group × window p95 refresh age ≤ 1.5× target, counting never-succeeded cells from minting
- One function, `effective_verify_reserve(watches)`, returns 0 when every active watch is in collect mode and 24 otherwise. `BudgetGuard` and `daily_headroom()` both use it (test: guard and pacer agree).
- Outcome counters per query: ok / transport_fail / empty / unparseable / budget_cancel / deadline_cancel, plus verifier fingerprint misses.

## Alert modes (per watch)
- **`collect`** (default): no scoring, verification, retries, bursts or drain. Entering it retires non-terminal outbox rows (`mode_collect`) and expires queued or partial bursts in one transaction. Test: first run after a cap-based pending row exists.
- **`relative`:**
  - **Bucket:** (actual origin–destination pair, window, nights). *As built (PR C):* the window is derived from depart date − observation day instead of being stored per observation; a change to a window's bounds re-reads old fares under the new bounds (acceptable for a personal monitor).
  - **Samples:** one minimum per cell per day, over a 28-day lookback, excluding the candidate's day.
  - **Arming:** ≥ 50 samples and ≥ 14 eligible days. Until then the bucket shows "insufficient history" and stays silent.
  - **Trigger:** price ≤ 0.70 × bucket median. The verify recheck must also pass (same fingerprint).
  - One persisted `baseline_context` is shared by scoring, verification, retries and burst eligibility. It replaces the 20%/72 h drop trigger.
- `absolute_cap_usd` becomes nullable and is ignored. An optional `max_alert_usd` sanity ceiling exists but is unset.
- Fingerprint matching, `event_no`, dedup and the handoff limits all stay.

## Weekly digest (Monday, extends `weekly.py`)
- "Cheapest observed" per group × window: price, dates, nights, airline, stops and observation age.
- Only departures inside the current window count.
- Missing or stale coverage (older than one refresh period) is shown explicitly.
- Once a bucket is armed, its median is shown ("typical $X").
- The dashboard link stays.

## #28 runbook amendments (before approval)
- **Rehearsal:** run from the scratch release directory, so the base-relative `DB_PATH` points at the copied DB.
- **Ownership:** `.env` owned by `hatch`, mode 600; `data/` writable by `hatch`.
- **Rollback:**
  1. Pause the schedule.
  2. Leave `relay_attempted_unconfirmed` rows as they are.
  3. Mark `handoff_prepared` rows only if the relay provably happened; otherwise retire them as `rollback_ambiguous`.
  4. Record confirmations only if they are real.
  5. `.backup` the latest DB.
  6. Install the old prompt **before** resuming the old release.
- **Re-upgrade:** retire unresolved rows that the old release touched and bump their `event_no` before re-enabling delivery.

## Sequencing
0. Amend #28 → Yaozu approves → deploy. Keep the 30-minute schedule running meanwhile (it collects Japan far-window history).
1. **PR A**, in four parts. Stage A (Japan, NYC) switches on in A2, never before collect mode:
   - **A1:** named windows, exact trip lengths, weekend returns, burst clipping (dormant on `main`).
   - **A2:** collect mode (expires queued/partial bursts on entry), verify-reserve function, category caps, background grid off, Stage A config at 150.
   - **A3:** outcome counters and refresh-age reporting.
   - **A4:** weekly digest rewrite.
2. **PR B:** Phase 2 groups and ceiling ramp steps, each gated and each approved by Yaozu.
3. **PR C:** relative alerts, switched on per watch as its buckets arm (≥ 14 days after collection starts).
