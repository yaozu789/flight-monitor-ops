"""Sentinel rotation + burst scheduling (plan 04 of the Flight Monitor v2 patch set).

Rolling tripwires over the watch groups plus on-demand burst expansion when the
verifier flags a deal. The canary concept from the old design is folded into the
sentinel layer: the sentinel cells ARE the tripwires (there is no separate
canary role).

Plan-04 arithmetic correction (read this before touching the budgets)
---------------------------------------------------------------------
A round-trip narrow query costs ``1 + top_n`` HTTP attempts: one outbound page
fetch plus one expansion request per top_n choice.

* Sentinel cells run with ``top_n=1`` -- the cheapest itinerary is all a
  tripwire needs -- so each cell check costs exactly **2 attempts**.
* The daily sentinel budget is **54 attempts**, which buys **27 cell
  checks/day**, NOT 54 cells. With 27 anchors minted per group (54 cells across
  the two watch groups), each anchor is re-checked roughly every **2 days**.
* A burst costs at most **60 attempts**: 20 neighbour departures x 2 attempts
  (top_n=1) + up to 10 duration cells x 2 attempts (top_n=1). The duration
  sweep skips the trigger's own duration, so a full burst actually spends
  2 (revalidation) + 40 (neighbours) + 18 (9 durations) = 60 attempts.

Spec item 12 mapping
---------------------
* Pace work across 48 daily slots -> :func:`per_run_allowance` (adaptive
  4..12 attempts/run, paced by slots left today).
* Rolling sentinel tripwires -> :func:`due_sentinels` (least-recently-checked
  first rotation) + :func:`mint_sentinels` / retirement rolling the
  +90..+270d window forward.
* Canary folded into sentinel -> no canary role; tripwires run top_n=1.
* Priority verify > bursts > background -> :func:`collect` runs bursts first
  (they were queued by the verifier), then sentinels, then background.
* Reserve verify budget -> per_run_allowance holds back
  ``max(0, 24 - spent_today("verify"))`` from the pacing pool.
* Hand off ready alerts BEFORE collecting -> run.py's job
  (outbox.prepare_handoff runs before collect()); noted here, not implemented.
* Verify traffic bills to "verify", never "sentinel" -> burst revalidation
  uses ``attempt_category="verify"``; sentinel cells bill ``"sentinel"``.
  Billing separation is entirely by the category argument passed through
  :func:`check_cell` to ``poller.narrow_query``.

Budget wiring
-------------
``DAILY_PARTITION`` is the suggested per-category cap table -- pass it as
``category_caps`` when constructing the v2 ``BudgetGuard``. The caps sum above
the 150/day ceiling on purpose: caps are per-category maxima, the daily
ceiling is the binding total. ``BudgetExhausted`` always propagates out of
:func:`check_cell`; :func:`collect` stops cleanly on it.

API notes / deviations from the plan-04 brief
---------------------------------------------
* ``BudgetGuard``/``BudgetExhausted`` come from the parent's ``budget`` module
  (v2 API). Until that module is assembled, ``BudgetExhausted`` falls back to
  a local ``Exception`` subclass with ``.scope``/``.category``; the base-tree
  ``s4/budget.py`` (whose ``BudgetExhausted`` is a ``BaseException`` for the
  old poller) is deliberately NOT used.
* ``check_cell(..., run_start=None)``: ``run_start`` defaults to ``None``
  (skip observation persistence) so one-off callers without a run context
  keep working; ``collect`` always passes a real ``run_start``.
* ``check_cell`` accepts a real cell row, a synthetic ``{"id": None,
  "depart_date": ..., "duration": ...}`` dict (don't record), or ``cell=None``
  (don't record; then ``depart_date=``/``duration=`` kwargs are required).
* ``execute_burst(..., run_start=None)``: additive optional kwarg so
  burst observations can be persisted like everything else.
* ``store.queue_trigger`` returns a *stale* ``lastrowid`` on dedup hits
  (SQLite ``INSERT OR IGNORE`` quirk), so "newly queued" counts are derived
  from ``db.total_changes`` deltas, not the return value.
* ``detect_under_cap_triggers`` takes deals as dicts
  (``watch_id``/``fingerprint``/``price_usd``/``depart_date``/``duration``/
  ``cap``); 6-tuples in the same order are also accepted.
"""

from __future__ import annotations

import math
from datetime import date, datetime, timedelta, timezone

import store
from poller import narrow_query

try:
    from budget import BudgetExhausted as _ImportedBudgetExhausted
    if not (isinstance(_ImportedBudgetExhausted, type)
            and issubclass(_ImportedBudgetExhausted, BaseException)):
        # The base-tree s4/budget.py still on sys.path defines BudgetExhausted
        # with an older API; the v3 contract needs the BaseException
        # subclass carrying .scope/.category.
        raise ImportError("budget.BudgetExhausted is not the v3 BaseException subclass")
    BudgetExhausted = _ImportedBudgetExhausted
except ImportError:
    class BudgetExhausted(BaseException):
        """Fallback until the v3 budget module is assembled on sys.path."""

        def __init__(self, scope=None, category=None,
                     message="budget exhausted"):
            self.scope = scope
            self.category = category
            super().__init__(message)


SENTINEL_ANCHORS_PER_GROUP = 27
SENTINEL_TOP_N = 1
DETAIL_TOP_N = 3
PER_RUN_ATTEMPT_CAP = 12
# v3 item 7: the old PER_RUN_ATTEMPT_FLOOR = 4 minimum is removed. When the
# budget is exhausted the allowance is 0 -- no floor of attempts the run
# cannot afford.

# Suggested per-category caps for BudgetGuard(category_caps=...). Caps are
# maxima, not an allocation: they intentionally sum above the 150/day ceiling.
DAILY_PARTITION = {
    "sentinel": 54,
    "verify": 24,
    "retry_reserve": 12,
    "background": 48,
    "burst": 60,
}

DAILY_CEILING = 150
VERIFY_RESERVE = 24

# v3 group 4: resumable bursts. A burst runs at most 60 attempts total
# (2 per step); a partial burst resumes in later slots until 60 attempts,
# all steps complete, or BURST_EXPIRY_HOURS after the trigger was queued.
BURST_MAX_ATTEMPTS = 60
BURST_EXPIRY_HOURS = 24


# ---------------------------------------------------------------- minting

def _coerce_today(today) -> date:
    if today is None:
        return date.fromisoformat(store.today_str())
    if isinstance(today, date):
        return today
    return date.fromisoformat(today)


def mint_sentinels(db, groups_cfg, today=None) -> int:
    """Idempotently mint 27 weekly anchors per group across the 3-9 month
    window (today+90d .. today+270d).

    Anchor ``i``: ``window_start + i * (window_days // 27)`` days, snapped to
    ``weekday == i % 7`` (+-3 days, clamped to the window). ``duration=7``,
    ``role="sentinel"``. Sentinel cells of these groups with
    ``depart_date < today+75d`` or ``> today+285d`` are retired (active=0).
    Returns the new-cell count.
    """
    today_d = _coerce_today(today)
    win_start = today_d + timedelta(days=90)
    win_end = today_d + timedelta(days=270)
    window_days = (win_end - win_start).days
    step = window_days // SENTINEL_ANCHORS_PER_GROUP

    cells = []
    for group_id in groups_cfg:
        for i in range(SENTINEL_ANCHORS_PER_GROUP):
            base = win_start + timedelta(days=i * step)
            target_wd = i % 7
            delta = (target_wd - base.weekday()) % 7
            if delta > 3:
                delta -= 7
            d = base + timedelta(days=delta)
            d = min(max(d, win_start), win_end)  # clamp to window
            cells.append({
                "group_id": group_id,
                "depart_date": d.isoformat(),
                "duration": 7,
                "role": "sentinel",
                "weekday": d.weekday(),
            })
    new_count = store.ensure_cells(db, cells)

    # Roll the window forward: retire anchors that drifted out of range.
    lo = (today_d + timedelta(days=75)).isoformat()
    hi = (today_d + timedelta(days=285)).isoformat()
    for group_id in groups_cfg:
        stale = db.execute(
            """SELECT id FROM cells
               WHERE group_id = ? AND role = 'sentinel' AND active = 1
                 AND (depart_date < ? OR depart_date > ?)""",
            (group_id, lo, hi)).fetchall()
        for row in stale:
            store.retire_cell(db, row["id"])
    return new_count


def ensure_background_grid(db, groups_cfg, today=None) -> int:
    """Idempotently mint the background grid: per group, weekly departures
    today+14d..today+180d x durations 5..14, role="background".
    Returns the new-cell count.
    """
    today_d = _coerce_today(today)
    start = today_d + timedelta(days=14)
    end = today_d + timedelta(days=180)

    cells = []
    d = start
    while d <= end:
        for group_id in groups_cfg:
            for duration in range(5, 15):
                cells.append({
                    "group_id": group_id,
                    "depart_date": d.isoformat(),
                    "duration": duration,
                    "role": "background",
                    "weekday": d.weekday(),
                })
        d += timedelta(days=7)
    return store.ensure_cells(db, cells)


def due_sentinels(db, limit) -> list[dict]:
    """Least-recently-checked active sentinel cells first (rotation)."""
    return store.due_cells(db, "sentinel", limit)


def due_background(db, limit) -> list[dict]:
    """Least-recently-checked active background cells first (rotation)."""
    return store.due_cells(db, "background", limit)


# ---------------------------------------------------------------- cell checks

def _normalize_row(row: dict, depart_date: str, return_date: str) -> dict:
    """Normalize a poller row for observation persistence.

    The v2 poller returns ``price_usd``; the base-tree poller returned
    ``price`` -- accept both so this module works before/after assembly.
    """
    d = dict(row)
    price = d.get("price_usd")
    if price is None and d.get("price") is not None:
        price = d["price"]
    d["price_usd"] = float(price) if price is not None else None
    d.setdefault("depart_date", depart_date)
    d.setdefault("return_date", return_date)
    d.setdefault("currency", "USD")
    d.setdefault("cabin", "economy")
    return d


def check_cell(db, cell, guard, group_id, group_cfg, *, top_n, category, slot,
               run_start=None, depart_date=None, duration=None
               ) -> tuple[float | None, int]:
    """One grouped narrow query for the cell.

    Calls ``narrow_query(",".join(origins), ",".join(destinations),
    depart_date, return_date=depart+duration, cfg, top_n=top_n,
    attempt_category=category)`` with
    ``cfg = {"origins": [...], "destinations": [...], "stops": [0, 1],
    "seat": "economy", "pax": {"adults": 1}}``.

    On a successful query (rows non-empty) the rows are ALWAYS persisted via
    ``store.save_observations(db, group_id, rows, run_start)`` when
    ``run_start`` is given (``group_id`` is the watch id). ``cell`` may be a
    real cell row (recorded via ``store.record_cell_check``), a synthetic
    ``{"id": None, ...}`` dict, or ``None`` (then ``depart_date=`` and
    ``duration=`` kwargs are required) -- the latter two skip recording.

    Returns ``(cheapest_price_usd | None, attempts_used_this_call)`` where
    attempts_used is the ``guard.spent_run()`` delta. ``BudgetExhausted``
    propagates (caller stops); other poller failures are treated as unknown
    (``None``) per the poller contract.
    """
    if cell is not None:
        depart_date = cell["depart_date"]
        duration = cell["duration"]
        cell_id = cell.get("id")
    else:
        cell_id = None
        if depart_date is None or duration is None:
            raise ValueError(
                "check_cell with cell=None requires depart_date= and "
                "duration= kwargs")

    spent_before = guard.spent_run()
    origin = ",".join(group_cfg["origins"])
    destination = ",".join(group_cfg["destinations"])
    return_date = (date.fromisoformat(depart_date)
                   + timedelta(days=int(duration))).isoformat()
    cfg = {"origins": list(group_cfg["origins"]),
           "destinations": list(group_cfg["destinations"]),
           "stops": [0, 1], "seat": "economy", "pax": {"adults": 1}}
    try:
        rows = narrow_query(origin, destination, depart_date, return_date,
                            cfg, top_n=top_n, attempt_category=category)
    except BudgetExhausted:
        raise
    except Exception:
        rows = None  # unknown, never "no deals"
    attempts_used = guard.spent_run() - spent_before

    cheapest = None
    if rows:
        normed = [_normalize_row(r, depart_date, return_date) for r in rows]
        normed = [r for r in normed if r["price_usd"] is not None]
        if normed and run_start is not None:
            store.save_observations(db, group_id, normed, run_start)
        prices = [r["price_usd"] for r in normed]
        cheapest = min(prices) if prices else None

    if cell_id is not None:
        store.record_cell_check(db, cell_id, slot, cheapest is not None,
                                cheapest)
    return cheapest, attempts_used


# ---------------------------------------------------------------- pacing

def per_run_allowance(guard, slot) -> int:
    """Adaptive pacing across the 48 daily slots.

    ``remaining = 150 - spent_today() - max(0, 24 - spent_today("verify"))``
    (the verify reserve is held back for the verifier); spread over the
    slots left today, clamped to at most 12. v3 item 7: no minimum floor --
    when the budget is exhausted (remaining <= 0) the allowance is 0, so
    the run stops instead of burning a floor of 4 attempts it cannot afford.
    """
    remaining = (DAILY_CEILING - guard.spent_today()
                 - max(0, VERIFY_RESERVE - guard.spent_today("verify")))
    if remaining <= 0:
        return 0
    slots_left = store.slots_left_today(slot)
    paced = math.ceil(remaining / max(1, slots_left))
    return min(PER_RUN_ATTEMPT_CAP, paced)


# ---------------------------------------------------------------- collect

def _parse_ts(ts: str) -> datetime:
    """Parse a store timestamp. Tolerant: the store's utcnow_iso() format
    has varied between "%Y-%m-%dT%H:%M:%SZ" and ISO "+00:00" across
    revisions -- accept both."""
    try:
        dt = datetime.fromisoformat(ts)
    except ValueError:
        dt = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _recent_success_within(prev_ts: str | None, hours: int) -> bool:
    if not prev_ts:
        return False
    try:
        prev_dt = _parse_ts(prev_ts)
    except ValueError:
        return False
    return (datetime.now(timezone.utc) - prev_dt) <= timedelta(hours=hours)


def _is_drop(prev_price: float | None, prev_ts: str | None,
             price: float | None) -> bool:
    """Tripwire drop rule: previous price exists, new <= 80% of previous,
    previous success within 72h."""
    if price is None or prev_price is None or prev_price <= 0:
        return False
    if price > 0.8 * prev_price:
        return False
    return _recent_success_within(prev_ts, 72)


def _queue_drop_trigger(db, cell: dict, price: float, slot: int) -> bool:
    """Queue a kind="drop" trigger; True when it was newly inserted."""
    before = db.total_changes
    store.queue_trigger(
        db, kind="drop", group_id=cell["group_id"],
        depart_week=store.depart_week(cell["depart_date"]),
        depart_date=cell["depart_date"], duration=cell["duration"],
        price_usd=price, fingerprint=None, slot=slot)
    # queue_trigger's return value is unreliable on dedup hits (stale
    # lastrowid from INSERT OR IGNORE), so count actual inserts instead.
    return db.total_changes > before


def _collect_phase(db, guard, groups_cfg, slot, run_start, role: str,
                   top_n: int, category: str, result: dict, allowance: int,
                   count_key: str, deadline_fn=None) -> None:
    remaining = allowance - result["attempts"]
    if remaining <= 0:
        return
    for cell in store.due_cells(db, role, remaining):
        if deadline_fn is not None and deadline_fn():
            raise store.DeadlineExceeded(
                f"{role} phase deadline exceeded")
        if result["attempts"] >= allowance:
            break
        group_cfg = groups_cfg.get(cell["group_id"])
        if group_cfg is None:
            continue
        prev_price = cell.get("last_price_usd")
        prev_ts = cell.get("last_success_at")
        price, used = check_cell(
            db, cell, guard, cell["group_id"], group_cfg,
            top_n=top_n, category=category, slot=slot, run_start=run_start)
        result["attempts"] += used
        result[count_key] += 1
        if _is_drop(prev_price, prev_ts, price):
            if _queue_drop_trigger(db, cell, price, slot):
                result["drop_triggers"] += 1


def collect(db, guard, groups_cfg, slot, run_start, deadline_fn=None) -> dict:
    """One collection pass (v3 order).

    Priority: sentinels (due_sentinels, top_n=1, "sentinel") -> bursts
    (due_triggers -> execute_burst, resumable across slots) -> background
    (due_background, top_n=3, "background"). Verification of new candidates
    runs BEFORE this (run.py cmd_check), so the verifier's budget is spent
    before collection burns it. Budget: per_run_allowance total; stops
    cleanly on BudgetExhausted (partial progress is kept).

    The run deadline is checked at loop tops; DeadlineExceeded propagates
    to run.run_guarded for a controlled stop (never swallowed here).

    Drop triggers: when a sentinel/background cell check succeeds and its
    previous last_price_usd exists and new <= 0.8*previous and the previous
    last_success_at is within 72h -> queue_trigger(kind="drop", ...).

    Returns {"attempts": int, "bursts": [...], "sentinels": n,
    "background": n, "drop_triggers": n}.
    """
    result = {"attempts": 0, "bursts": [], "sentinels": 0,
              "background": 0, "drop_triggers": 0}
    allowance = per_run_allowance(guard, slot)
    # v3 item 5: enforce the allowance on EVERY send, not just before a
    # multi-attempt query. reserve() refuses once the run-local total hits
    # this, so a top_n=3 query against a 2-attempt allowance stops at 2
    # sends instead of overshooting to 4.
    guard.set_run_allowance(allowance)
    try:
        _collect_phase(db, guard, groups_cfg, slot, run_start,
                       "sentinel", SENTINEL_TOP_N, "sentinel",
                       result, allowance, "sentinels", deadline_fn)
        for trig in store.due_triggers(db, slot):
            if deadline_fn is not None and deadline_fn():
                raise store.DeadlineExceeded("burst phase deadline exceeded")
            if result["attempts"] >= allowance:
                break
            group_cfg = groups_cfg.get(trig["group_id"])
            if group_cfg is None:
                continue
            used = execute_burst(db, trig, guard, group_cfg, slot,
                                 run_start=run_start, deadline_fn=deadline_fn)
            result["attempts"] += used
            result["bursts"].append(trig["id"])
        _collect_phase(db, guard, groups_cfg, slot, run_start,
                       "background", DETAIL_TOP_N, "background",
                       result, allowance, "background", deadline_fn)
    except BudgetExhausted:
        pass  # stop cleanly; partial progress is kept
    return result


# ---------------------------------------------------------------- triggers

def detect_under_cap_triggers(db, verified_deals, run_start, slot) -> int:
    """Queue kind="under_cap" burst triggers for newly verified deals.

    ``verified_deals``: dicts (or 6-tuples) of ``watch_id``/``fingerprint``/
    ``price_usd``/``depart_date``/``duration``/``cap``. A deal is queued only
    when ``store.has_under_cap_before(fingerprint, cap, run_start)`` is
    False. Returns the newly-queued count.
    """
    queued = 0
    for deal in verified_deals:
        if isinstance(deal, dict):
            watch_id = deal["watch_id"]
            fingerprint = deal["fingerprint"]
            price_usd = deal["price_usd"]
            depart_date = deal["depart_date"]
            duration = deal["duration"]
            cap = deal.get("cap")
        else:
            watch_id, fingerprint, price_usd, depart_date, duration, cap = deal
        if cap is None:
            continue
        if store.has_under_cap_before(db, fingerprint, cap, run_start):
            continue
        before = db.total_changes
        store.queue_trigger(
            db, kind="under_cap", group_id=watch_id,
            depart_week=store.depart_week(depart_date),
            depart_date=depart_date, duration=duration,
            price_usd=price_usd, fingerprint=fingerprint, slot=slot)
        if db.total_changes > before:
            queued += 1
    return queued


# ---------------------------------------------------------------- bursts

def _burst_steps(trigger):
    """Ordered burst steps as (step_index, kind, depart_date, duration).

    Step 0 revalidates the trigger (billed "verify"); steps 1..20 are the
    neighbour departures +-1..10 days at the trigger duration; steps 21..29
    are the duration exploration (5..14 days, excluding the trigger's own
    duration). 30 steps x 2 attempts = 60 attempts max.
    """
    yield 0, "revalidate", trigger["depart_date"], trigger["duration"]
    base = date.fromisoformat(trigger["depart_date"])
    idx = 1
    for delta in range(1, 11):
        for s in (-1, 1):
            yield (idx, "neighbour",
                   (base + timedelta(days=s * delta)).isoformat(),
                   trigger["duration"])
            idx += 1
    for dur in range(5, 15):
        if dur == trigger["duration"]:
            continue
        yield idx, "duration", trigger["depart_date"], dur
        idx += 1


def execute_burst(db, trigger, guard, group_cfg, slot, run_start=None,
                  deadline_fn=None) -> int:
    """Expand one due trigger (dict from ``store.due_triggers``).

    v3 group 4: the burst is RESUMABLE across slots. Progress (cumulative
    attempts used, next step, expiry) persists in ``store.burst_progress``
    after every step. A burst that hits the per-run allowance, the run
    deadline, or the budget stops as "partial" and later slots resume it
    via ``due_triggers`` until it reaches 60 attempts, completes all 30
    steps, or expires (24h after the trigger was queued). It is NEVER
    marked permanently done after a single partial allowance.

    1. Revalidate the trigger's depart/duration with top_n=1 billed to
       "verify" (first execution only). If the cheapest is unknown or above
       the trigger price -> mark_trigger "dropped", progress "dropped",
       record_burst(status="dropped"), return attempts used.
    2. Neighbour departures +-1..10 days (20 dates) at the trigger duration,
       top_n=1, billed "burst".
    3. Duration exploration: durations 5..14 excluding the trigger duration,
       same depart_date, top_n=1, "burst".
    4. record_burst(status="done"/"partial"); the trigger goes to "burst"
       when finished, "burst_active" (resumable) on partial.

    One-off checks are never recorded as cell checks (synthetic cell dicts
    with ``id=None``). ``BudgetExhausted`` / deadline mid-burst keeps the
    persisted progress, marks the trigger "burst_active", records a
    partial burst, and re-raises. Returns attempts used by THIS call.
    """
    # Load or initialize resumable progress.
    prog = store.get_burst_progress(db, trigger["id"])
    if prog is None:
        created = _parse_ts(trigger["created_at"])
        expires_at = (created + timedelta(hours=BURST_EXPIRY_HOURS)).isoformat()
        prog = store.init_burst_progress(db, trigger["id"],
                                         trigger["group_id"],
                                         trigger["depart_week"], expires_at)
    # Defensive: expired or terminal progress never executes.
    if prog["status"] != "active" or prog["expires_at"] <= store.utcnow_iso():
        if prog["status"] == "active":
            store.save_burst_progress(db, trigger["id"], status="expired")
            store.mark_trigger(db, trigger["id"], "expired")
        return 0

    total = 0
    cumulative = prog["attempts_used"]
    next_step = prog["next_step"]
    allowance = per_run_allowance(guard, slot)
    run_spent_entry = guard.spent_run()
    group_id = trigger["group_id"]

    def _one_off(d_date: str, dur: int, category: str):
        cell = {"id": None, "depart_date": d_date, "duration": dur}
        return check_cell(db, cell, guard, group_id, group_cfg,
                          top_n=SENTINEL_TOP_N, category=category,
                          slot=slot, run_start=run_start)

    def _allowance_exhausted() -> bool:
        return (guard.spent_run() - run_spent_entry) >= allowance

    def _record(status: str) -> None:
        store.record_burst(db, trigger_id=trigger["id"], group_id=group_id,
                           depart_week=trigger["depart_week"],
                           attempts=total, status=status)

    steps = list(_burst_steps(trigger))
    try:
        for step_idx, kind, d_date, dur in steps:
            if step_idx < next_step:
                continue  # already done in an earlier slot: resume point
            if cumulative >= BURST_MAX_ATTEMPTS:
                break
            if _allowance_exhausted():
                break
            if deadline_fn is not None and deadline_fn():
                raise store.DeadlineExceeded("burst phase deadline exceeded")
            category = "verify" if kind == "revalidate" else "burst"
            price, used = _one_off(d_date, dur, category)
            total += used
            cumulative += used
            next_step = step_idx + 1
            if kind == "revalidate" and (price is None
                                        or price > trigger["price_usd"]):
                store.mark_trigger(db, trigger["id"], "dropped")
                store.save_burst_progress(db, trigger["id"], status="dropped",
                                          next_step=next_step,
                                          attempts_used=cumulative)
                _record("dropped")
                return total
            # Persist every step so a later slot resumes exactly here.
            store.save_burst_progress(db, trigger["id"], status="active",
                                      next_step=next_step,
                                      attempts_used=cumulative)
        finished = (next_step >= len(steps)
                    or cumulative >= BURST_MAX_ATTEMPTS)
        if finished:
            store.mark_trigger(db, trigger["id"], "burst")
            store.save_burst_progress(db, trigger["id"], status="done")
            _record("done")
        else:
            # Partial: resumable, NEVER terminal after one allowance.
            store.mark_trigger(db, trigger["id"], "burst_active")
            _record("partial")
        return total
    except (BudgetExhausted, store.DeadlineExceeded):
        # Account carefully for a transport allowance ending mid-query: the
        # interrupted step may have partially spent before raising. Fold
        # that spend into the cumulative total so the 60-attempt cap stays
        # accurate across resumes. (Progress was persisted per completed
        # step above; the interrupted step itself is retried on resume.)
        partial_spend = (guard.spent_run() - run_spent_entry) - total
        if partial_spend > 0:
            cumulative += partial_spend
            store.save_burst_progress(db, trigger["id"], status="active",
                                      next_step=next_step,
                                      attempts_used=cumulative)
        # Mark resumable and let the caller stop cleanly (collect) /
        # controlled (run_guarded).
        store.mark_trigger(db, trigger["id"], "burst_active")
        _record("partial")
        raise
