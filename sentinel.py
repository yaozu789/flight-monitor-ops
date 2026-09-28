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

import poller
import store
from poller import narrow_query
from transport import ThrottleDetected

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
PER_RUN_ATTEMPT_CAP = 12   # per 30-minute slot of cadence
PER_RUN_HARD_CAP = 80      # spec: never more than 80 attempts in one run
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
# Worst-case sends of one burst step (outbound page + 1 return page at
# SENTINEL_TOP_N=1): a step is only started if it cannot push the burst
# past BURST_MAX_ATTEMPTS.
BURST_STEP_MAX_ATTEMPTS = 1 + 1
BURST_EXPIRY_HOURS = 24


# ---------------------------------------------------------------- minting

def _coerce_today(today) -> date:
    if today is None:
        return date.fromisoformat(store.today_str())
    if isinstance(today, date):
        return today
    return date.fromisoformat(today)


def _window_days(group_cfg) -> tuple[int, int]:
    """(earliest, latest) departure offset in days for a group. Defaults to
    the spec's 3-9 month window."""
    lo, hi = (group_cfg or {}).get("window_days", (90, 270))
    return int(lo), int(hi)


def _durations(group_cfg) -> list[int]:
    """Trip lengths (days) explored for a group: the explicit ``nights``
    list when configured (named windows), else the ``durations`` range
    (default 5..14)."""
    nights = (group_cfg or {}).get("nights")
    if nights:
        return sorted({int(n) for n in nights})
    lo, hi = (group_cfg or {}).get("durations", (5, 14))
    return list(range(int(lo), int(hi) + 1))


def _window_dates(win: dict, start: date, end: date) -> list[date]:
    """Departure anchors of one named window within [start, end].

    - ``weekdays`` set: every date on those weekdays (e.g. Fridays for
      weekend trips); no cap, so a window holding 6 Fridays keeps all 6.
    - ``step_days`` set: every point of the GRID_EPOCH lattice with that
      step; no weekday snap and no cap.
    - neither: the legacy spread (about 27 anchors, snapped across
      weekdays), used for the long-term window.
    """
    if win.get("weekdays"):
        wanted = {int(w) for w in win["weekdays"]}
        return [start + timedelta(days=i)
                for i in range((end - start).days + 1)
                if (start + timedelta(days=i)).weekday() in wanted]
    if win.get("step_days"):
        return [d for _, d in _lattice(start, end, int(win["step_days"]))]
    span = (end - start).days
    step = max(1, round(span / SENTINEL_ANCHORS_PER_GROUP))
    out = []
    for k, base in _lattice(start, end, step):
        if len(out) >= SENTINEL_ANCHORS_PER_GROUP:
            break
        delta = ((k % 7) - base.weekday()) % 7
        if delta > 3:
            delta -= 7
        d = base + timedelta(days=delta)
        if start <= d <= end:
            out.append(d)
    return out


def _fixed_return_nights(win: dict | None, d: date) -> int | None:
    """Stay that returns on the window's ``return_weekday`` (e.g. leave Fri
    -> 2 nights, Sat -> 1 night for a Sunday return), or None when the
    window uses the watch's ``nights``."""
    if not win or win.get("return_weekday") is None:
        return None
    return (int(win["return_weekday"]) - d.weekday()) % 7 or 7


def _window_bounds(win: dict, today_d: date) -> tuple[date, date]:
    return (today_d + timedelta(days=int(win["lo"])),
            today_d + timedelta(days=int(win["hi"])))


def _sentinel_duration(group_cfg) -> int:
    """Sentinel stay length: 7 days, clamped into the group's range."""
    durs = _durations(group_cfg)
    return min(max(7, durs[0]), durs[-1])


# Fixed calendar lattice (a Monday). Grid dates are computed from it, not
# from "today", so the same dates are re-minted every day: cells enter at
# the far end of the window and retire at the near end instead of a new set
# being added daily (the old grid grew to 40 sentinels / 1,700+ background
# cells per group).
GRID_EPOCH = date(2026, 1, 5)


def _lattice(start: date, end: date, step: int):
    """Lattice points GRID_EPOCH + k*step within [start, end], as (k, date)."""
    k = -(-(start - GRID_EPOCH).days // step)  # ceil
    while True:
        d = GRID_EPOCH + timedelta(days=k * step)
        if d > end:
            return
        yield k, d
        k += 1


def mint_sentinels(db, groups_cfg, today=None) -> int:
    """Idempotently mint ~27 fixed anchors per group across the group's
    departure window (``window_days``, default today+90d .. today+270d).

    Anchors sit on a fixed lattice (step = window // 27 days from
    GRID_EPOCH), each snapped +-3 days to weekday ``k % 7`` so they spread
    across weekdays. They stay fixed until they leave the window. The stay
    is 7 days clamped into the group's ``durations``; ``role="sentinel"``.
    Sentinels outside the window, or with a stay no longer configured, are
    retired. Returns the new/reactivated cell count.
    """
    today_d = _coerce_today(today)

    cells = []
    for group_id, group_cfg in groups_cfg.items():
        if group_cfg.get("windows"):
            # Named windows: mint the union of every window x every
            # configured trip length, so one retire pass below never drops
            # another window's cells.
            for win in group_cfg["windows"]:
                start, end = _window_bounds(win, today_d)
                for d in _window_dates(win, start, end):
                    fixed = _fixed_return_nights(win, d)
                    for duration in ([fixed] if fixed is not None
                                     else _durations(group_cfg)):
                        cells.append({
                            "group_id": group_id,
                            "depart_date": d.isoformat(),
                            "duration": duration,
                            "role": "sentinel",
                            "weekday": d.weekday(),
                            "window": win["name"],
                            "refresh_hours": int(win["refresh_hours"]),
                        })
            continue
        lo_d, hi_d = _window_days(group_cfg)
        win_start = today_d + timedelta(days=lo_d)
        win_end = today_d + timedelta(days=hi_d)
        # Nearest-integer spacing, capped at 27 anchors: ~27 for any window
        # (ceil spacing gave only 16 for a 30-day window).
        step = max(1, round((hi_d - lo_d) / SENTINEL_ANCHORS_PER_GROUP))
        duration = _sentinel_duration(group_cfg)
        picked = 0
        for k, base in _lattice(win_start, win_end, step):
            if picked >= SENTINEL_ANCHORS_PER_GROUP:
                break
            delta = ((k % 7) - base.weekday()) % 7
            if delta > 3:
                delta -= 7
            d = base + timedelta(days=delta)
            if not (win_start <= d <= win_end):
                continue
            picked += 1
            cells.append({
                "group_id": group_id,
                "depart_date": d.isoformat(),
                "duration": duration,
                "role": "sentinel",
                "weekday": d.weekday(),
            })
    new_count = store.ensure_cells(db, cells)

    # Anything active that is not in today's computed grid is retired:
    # anchors that left the window, a changed stay or window, and cells
    # left over from the old today-relative grid.
    _retire_not_in(db, groups_cfg, "sentinel", cells)
    return new_count


def _retire_not_in(db, groups_cfg, role, cells):
    """Retire active *role* cells of these groups that are not in *cells*
    (the grid just computed), so the active grid is exactly the lattice."""
    wanted = {(c["group_id"], c["depart_date"], c["duration"]) for c in cells}
    for group_id in groups_cfg:
        for row in db.execute(
                """SELECT id, depart_date, duration FROM cells
                   WHERE group_id = ? AND role = ? AND active = 1""",
                (group_id, role)).fetchall():
            if (group_id, row["depart_date"], row["duration"]) not in wanted:
                store.retire_cell(db, row["id"])


def ensure_background_grid(db, groups_cfg, today=None) -> int:
    """Idempotently mint the background grid: per group, weekly departures
    across the group's window x its trip lengths, role="background".
    Cells that have left the window (e.g. departures now in the past) or
    whose stay is no longer configured are retired, so the rotation never
    spends budget on them. Returns the new-cell count.
    """
    today_d = _coerce_today(today)

    cells = []
    for group_id, group_cfg in groups_cfg.items():
        if group_cfg.get("windows"):
            # Named-window watches have no background grid (redesign: the
            # 10-duration exploration is off); the retire pass below drops
            # any cells an earlier config left.
            continue
        lo_d, hi_d = _window_days(group_cfg)
        durations = _durations(group_cfg)
        # Weekly departures on the fixed lattice (Mondays from GRID_EPOCH).
        for _, d in _lattice(today_d + timedelta(days=lo_d),
                             today_d + timedelta(days=hi_d), 7):
            for duration in durations:
                cells.append({
                    "group_id": group_id,
                    "depart_date": d.isoformat(),
                    "duration": duration,
                    "role": "background",
                    "weekday": d.weekday(),
                })
    new_count = store.ensure_cells(db, cells)
    _retire_not_in(db, groups_cfg, "background", cells)
    return new_count


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
    window_name = cell.get("window_name") if cell is not None else None

    def _log(outcome):
        store.log_query(db, slot=slot, group_id=group_id,
                        window_name=window_name, category=category,
                        outcome=outcome)

    poller.LAST_OUTCOME = None
    outcome = None
    try:
        rows = narrow_query(origin, destination, depart_date, return_date,
                            cfg, top_n=top_n, attempt_category=category)
    except BudgetExhausted:
        _log("budget_cancel")
        raise
    except ThrottleDetected:
        _log("throttle")
        raise
    except Exception:
        rows = None  # unknown, never "no deals"
        outcome = "error"
    attempts_used = guard.spent_run() - spent_before

    cheapest = None
    if rows:
        normed = [_normalize_row(r, depart_date, return_date) for r in rows]
        normed = [r for r in normed if r["price_usd"] is not None]
        if normed and run_start is not None:
            store.save_observations(db, group_id, normed, run_start)
        prices = [r["price_usd"] for r in normed]
        cheapest = min(prices) if prices else None
    if outcome is None:
        if cheapest is not None:
            outcome = "ok"
        elif rows:
            outcome = "unparseable"
        else:
            outcome = poller.LAST_OUTCOME or "empty"
    _log(outcome)

    if cell_id is not None:
        store.record_cell_check(db, cell_id, slot, cheapest is not None,
                                cheapest)
    return cheapest, attempts_used


# ---------------------------------------------------------------- pacing

def daily_headroom(guard) -> int:
    """Non-verify attempts still spendable today (the verify reserve is
    held back for the verifier). Ceiling and reserve come from the guard,
    so pacing matches what ``reserve()`` will actually allow."""
    ceiling = getattr(guard, "daily_ceiling", DAILY_CEILING)
    reserve = getattr(guard, "verify_reserve", VERIFY_RESERVE)
    return (ceiling - guard.spent_today()
            - max(0, reserve - guard.spent_today("verify")))


def per_run_allowance(guard, slot, cadence_slots: int = 1) -> int:
    """Adaptive pacing across the 48 daily slots.

    ``remaining = 150 - spent_today() - max(0, 24 - spent_today("verify"))``
    (the verify reserve is held back for the verifier); spread over the
    slots left today, clamped to at most 12. v3 item 7: no minimum floor --
    when the budget is exhausted (remaining <= 0) the allowance is 0, so
    the run stops instead of burning a floor of 4 attempts it cannot afford.
    """
    remaining = daily_headroom(guard)
    if remaining <= 0:
        return 0
    # Runs left today at the configured cadence (every cadence_slots
    # 30-minute slots), not raw 30-minute slots: at a 2-hour cadence the
    # day has 12 runs, each needing ~4x the per-slot share.
    cadence_slots = max(1, int(cadence_slots))
    runs_left = math.ceil(store.slots_left_today(slot) / cadence_slots)
    paced = math.ceil(remaining / max(1, runs_left))
    return min(PER_RUN_ATTEMPT_CAP * cadence_slots, PER_RUN_HARD_CAP, paced)


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
                   count_key: str, deadline_fn=None, seen=None,
                   collect_groups=frozenset(), overdue_only=False) -> None:
    remaining = allowance - result["attempts"]
    if remaining <= 0:
        return
    seen = seen if seen is not None else set()
    # Over-fetch so cells already checked this run (a second pass on
    # leftover allowance) can be skipped without starving the phase.
    for cell in store.due_cells(db, role, remaining + len(seen), slot=slot,
                                overdue_only=overdue_only,
                                group_ids=groups_cfg):
        if cell["id"] in seen:
            continue
        if deadline_fn is not None and deadline_fn():
            raise store.DeadlineExceeded(
                f"{role} phase deadline exceeded")
        # Only start a check this phase can afford in full (outbound page +
        # top_n return pages); a half-finished check wastes its sends.
        if allowance - result["attempts"] < 1 + top_n:
            break
        seen.add(cell["id"])  # only once its check actually starts
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
        if cell["group_id"] in collect_groups:
            continue  # collect-only: prices are stored, nothing triggers
        if _is_drop(prev_price, prev_ts, price):
            if _queue_drop_trigger(db, cell, price, slot):
                result["drop_triggers"] += 1


def collect(db, guard, groups_cfg, slot, run_start, deadline_fn=None,
            cadence_slots: int = 1, collect_groups=frozenset()) -> dict:
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
    allowance = per_run_allowance(guard, slot, cadence_slots)
    # The guard's allowance is a cap on the RUN total, so set it relative to
    # what this run already spent (retry/verify passes run before
    # collection); an absolute value could leave collection 0 attempts.
    run_start_spent = guard.spent_run()
    guard.set_run_allowance(
        min(PER_RUN_HARD_CAP, run_start_spent + allowance))
    # Never let the relative allowance lift the run past the 80 hard cap.
    allowance = min(allowance, max(0, PER_RUN_HARD_CAP - run_start_spent))

    def phase(fn):
        """Run one phase. A CATEGORY cap (e.g. sentinels' daily 54) only
        ends that phase; daily/per-run/verify-reserve exhaustion ends
        collection. Spend is reconciled with the guard afterwards, so sends
        made just before an exception are still counted."""
        try:
            fn()
            return True
        except BudgetExhausted as e:
            return getattr(e, "scope", None) == "category"
        finally:
            # Sends made just before an exception are in the guard's total
            # but not yet in result["attempts"].
            result["attempts"] = max(result["attempts"],
                                     guard.spent_run() - run_start_spent)

    # Sentinels get at most half of this run's allowance up front, so
    # bursts and background progress all day instead of only after the
    # sentinel cap is hit; unused allowance flows back to sentinels last.
    sentinel_share = math.ceil(allowance / 2)
    checked: set = set()  # cells checked this run: never twice

    def sentinels(limit, overdue_only=False):
        _collect_phase(db, guard, groups_cfg, slot, run_start,
                       "sentinel", SENTINEL_TOP_N, "sentinel",
                       result, limit, "sentinels", deadline_fn, checked,
                       collect_groups, overdue_only)

    def bursts():
        # Bursts spend only surplus: while any named-window sentinel is due,
        # its refresh comes first (a 60-attempt burst could otherwise eat
        # the budget the sentinel targets depend on).
        if store.overdue_sentinels(db, slot, groups_cfg):
            return
        for trig in store.due_triggers(db, slot):
            if trig["group_id"] in collect_groups:
                continue  # expired on entering collect mode; never burst
            if deadline_fn is not None and deadline_fn():
                raise store.DeadlineExceeded("burst phase deadline exceeded")
            left = allowance - result["attempts"]
            if left < BURST_STEP_MAX_ATTEMPTS:
                break  # only start a burst that can finish a whole step
            group_cfg = groups_cfg.get(trig["group_id"])
            if group_cfg is None:
                continue
            used = execute_burst(db, trig, guard, group_cfg, slot,
                                 run_start=run_start, deadline_fn=deadline_fn,
                                 allowance=left)
            result["attempts"] += used
            result["bursts"].append(trig["id"])

    def background():
        _collect_phase(db, guard, groups_cfg, slot, run_start,
                       "background", DETAIL_TOP_N, "background",
                       result, allowance, "background", deadline_fn, checked,
                       collect_groups)

    order = [lambda: sentinels(sentinel_share), bursts, background,
             lambda: sentinels(allowance)]
    windowed = any(c.get("windows") for c in groups_cfg.values())
    if (not windowed and slot % 2 == 1 and 0 < allowance < 1 + DETAIL_TOP_N
            and daily_headroom(guard) >= 1 + DETAIL_TOP_N):
        # A background check needs 1 + DETAIL_TOP_N sends; the ~3-attempt
        # share of a 30-minute run never fits one. Odd runs borrow up to a
        # whole check (the daily ceiling and verify reserve still apply in
        # reserve(), and pacing shrinks later runs to compensate).
        allowance = min(1 + DETAIL_TOP_N,
                        max(0, PER_RUN_HARD_CAP - run_start_spent))
        guard.set_run_allowance(run_start_spent + allowance)
    if slot % 2 == 1:
        # Alternate runs lead with exploration, so a small allowance (about
        # 3 at a 30-minute cadence) still reaches bursts and background.
        order = [bursts, background, lambda: sentinels(sentinel_share),
                 lambda: sentinels(allowance)]
    if windowed:
        # Named windows (redesign): every overdue sentinel first, with the
        # whole allowance; then bursts from what is left (they wait while
        # any sentinel is still due); then the plain rotation. No slot
        # parity: the refresh targets, not alternation, decide priority.
        order = [lambda: sentinels(allowance, overdue_only=True), bursts,
                 background, lambda: sentinels(allowance)]
    result["allowance"] = allowance
    for step in order:
        if not phase(step):
            break  # global budget stop: partial progress is kept
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

def _burst_steps(trigger, durations=None, window=None):
    """Ordered burst steps as (step_index, kind, depart_date, duration).

    Step 0 revalidates the trigger (billed "verify"); steps 1..20 are the
    neighbour departures +-1..10 days at the trigger duration; steps 21..29
    are the duration exploration (the group's trip lengths, default 5..14,
    excluding the trigger's own duration). 30 steps x 2 attempts = 60
    attempts max with the default range.

    ``window`` (``{"start", "end", "weekdays"}``, from ``_trigger_window``)
    clips neighbours to the trigger's departure window and weekdays, so a
    near-term trigger never searches past dates or a weekend window a
    Tuesday. A fixed-return window (``return_weekday``) gives each
    neighbour its own stay (Fri 2 nights, Sat 1) and has no duration sweep.
    Step indices stay consecutive over the kept steps.
    """
    yield 0, "revalidate", trigger["depart_date"], trigger["duration"]
    base = date.fromisoformat(trigger["depart_date"])
    idx = 1
    for delta in range(1, 11):
        for s in (-1, 1):
            d = base + timedelta(days=s * delta)
            if window is not None:
                if not (window["start"] <= d <= window["end"]):
                    continue
                if window["weekdays"] and d.weekday() not in window["weekdays"]:
                    continue
            fixed = _fixed_return_nights(window, d)
            yield (idx, "neighbour", d.isoformat(),
                   fixed if fixed is not None else trigger["duration"])
            idx += 1
    if window is not None and window.get("return_weekday") is not None:
        return
    for dur in (durations or range(5, 15)):
        if dur == trigger["duration"]:
            continue
        yield idx, "duration", trigger["depart_date"], dur
        idx += 1


def _trigger_window(trigger, group_cfg):
    """The named window holding the trigger's departure, as burst clipping
    bounds, or None (legacy config, or no window contains it).

    Bounds are computed from the day the trigger was queued, not today, so
    a burst resumed on a later day keeps the same step list (its resume
    index stays valid).
    """
    if not group_cfg.get("windows"):
        return None
    queued = _parse_ts(trigger["created_at"]).date()
    dep = date.fromisoformat(trigger["depart_date"])
    for win in group_cfg["windows"]:
        start, end = _window_bounds(win, queued)
        if start <= dep <= end:
            return {"start": start, "end": end,
                    "weekdays": {int(w) for w in win.get("weekdays") or []},
                    "return_weekday": win.get("return_weekday")}
    return None


def execute_burst(db, trigger, guard, group_cfg, slot, run_start=None,
                  deadline_fn=None, allowance=None) -> int:
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
    if allowance is None:
        allowance = per_run_allowance(guard, slot)
    run_spent_entry = guard.spent_run()
    group_id = trigger["group_id"]

    def _one_off(d_date: str, dur: int, category: str):
        cell = {"id": None, "depart_date": d_date, "duration": dur}
        return check_cell(db, cell, guard, group_id, group_cfg,
                          top_n=SENTINEL_TOP_N, category=category,
                          slot=slot, run_start=run_start)

    def _allowance_exhausted() -> bool:
        # A step is only started if the allowance covers the whole step.
        return (allowance - (guard.spent_run() - run_spent_entry)
                < BURST_STEP_MAX_ATTEMPTS)

    def _record(status: str) -> None:
        store.record_burst(db, trigger_id=trigger["id"], group_id=group_id,
                           depart_week=trigger["depart_week"],
                           attempts=total, status=status)

    steps = list(_burst_steps(trigger, _durations(group_cfg),
                              _trigger_window(trigger, group_cfg)))
    # Resuming on a later UTC day: recheck the trigger fare first, before
    # spending a new day's budget on a deal that may already be gone.
    revalidated = prog.get("revalidated_at") or ""
    revalidate_again = (next_step > 0
                        and revalidated[:10] < store.today_str())
    try:
        for step_idx, kind, d_date, dur in steps:
            redo = revalidate_again and step_idx == 0
            if step_idx < next_step and not redo:
                continue  # already done in an earlier slot: resume point
            if cumulative + BURST_STEP_MAX_ATTEMPTS > BURST_MAX_ATTEMPTS:
                break
            if _allowance_exhausted():
                break
            if deadline_fn is not None and deadline_fn():
                raise store.DeadlineExceeded("burst phase deadline exceeded")
            category = "verify" if kind == "revalidate" else "burst"
            price, used = _one_off(d_date, dur, category)
            total += used
            cumulative += used
            if redo:
                revalidate_again = False  # resume point stays where it was
            else:
                next_step = step_idx + 1
            if kind == "revalidate" and (price is None
                                        or price > trigger["price_usd"]):
                store.mark_trigger(db, trigger["id"], "dropped")
                store.save_burst_progress(db, trigger["id"], status="dropped",
                                          next_step=next_step,
                                          attempts_used=cumulative)
                _record("dropped")
                return total
            # Persist every step so a later slot resumes exactly here; a
            # passed revalidation is recorded separately, so a budget stop
            # right after it cannot fake a same-day recheck.
            store.save_burst_progress(db, trigger["id"], status="active",
                                      next_step=next_step,
                                      attempts_used=cumulative,
                                      revalidated=(kind == "revalidate"))
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
