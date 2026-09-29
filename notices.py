"""One-time milestone notices for Yaozu, relayed by the scheduler.

Scheduled runs don't see chat, so anything Yaozu should hear about (a budget
step being ready, alerts going live, a problem at the current stage) is
emitted here and relayed VERBATIM, like the weekly report. Each notice has a
stable key and is created once; it rides at most NOTICE_MAX_EMITS relay
batches until the agent records it with ``notice-mark --key``.

Built from stored data only (``run.coverage_report``): no searches.
"""
from __future__ import annotations

from datetime import datetime, timezone

import store

NOTICE_MAX_EMITS = 2

# What each budget stage unlocks next (docs/redesign-spec.md, Budget).
NEXT_STEP = {
    150: "raise the daily limit to 225 and switch on LA, San Diego and "
         "Hong Kong",
    225: "raise the daily limit to 300 and switch on Shanghai and Beijing",
    300: "switch on trip-length exploration (explore_per_day)",
}
# Gate checks worth a "hold" notice once the stage has enough history.
HOLD_CHECKS = {
    "no_throttles": "Google throttled or showed a CAPTCHA",
    "unknown_rate": "5% or more searches came back with no usable price",
    "no_deadline_stops": "a run hit its time limit",
    "refresh_p95": "some departure dates are not being rechecked in time",
}

# Notices created in THIS process; claim() hands them to one relay batch.
_armed = False


def _add(db, key: str, message: str) -> None:
    db.execute(
        """INSERT OR IGNORE INTO notices (key, message_text, created_at)
           VALUES (?, ?, ?)""", (key, message, store.utcnow_iso()))
    db.commit()


def prepare(db, watches, guard, now: datetime | None = None) -> None:
    """Scheduled runs only: create any newly reached notices and arm the
    unrelayed ones for this invocation's relay batch."""
    global _armed
    _armed = False
    import run  # lazy: run imports this module
    now = now or datetime.now(timezone.utc)
    report = run.coverage_report(db, watches, guard, now=now)
    ceiling = guard.daily_ceiling
    checks = report["gate"]["checks"]
    if report["gate"]["pass"]:
        nxt = NEXT_STEP.get(ceiling, "the next step")
        rate = report["queries"]["unknown_rate"] or 0
        _add(db, f"gate_ready:{ceiling}",
             f"Flight monitor: budget step ready. {report['days']} clean days "
             f"at {ceiling} requests/day (0 throttles, {rate:.1%} "
             f"unusable searches, dates rechecked on time). Next: {nxt}. "
             "Ask Claude to prepare the switch; nothing changes until you "
             "approve it.")
    elif checks.get("history_days"):
        # One hold notice per stage, naming every failing check.
        reasons = [why for check, why in HOLD_CHECKS.items()
                   if not checks.get(check, True)]
        if reasons:
            _add(db, f"gate_hold:{ceiling}",
                 f"Flight monitor: hold at {ceiling} requests/day -- "
                 f"{'; '.join(reasons)} (last {report['days']} days). No "
                 "action needed unless it keeps happening; `run.py "
                 "coverage` has the details.")
    by_id = {w["id"]: w for w in watches}
    for wid, r in (report.get("relative_routes") or {}).items():
        if (r["armed"] and by_id.get(wid, {}).get("alert_mode")
                == "relative"):
            _add(db, f"alerts_live:{wid}",
                 f"Flight monitor: 'unusually cheap' alerts are now live "
                 f"for {wid} ({r['armed']} of {r['routes']} routes have "
                 "enough price history). You'll get an alert when a "
                 "fare drops to 70% or less of its route's typical price, "
                 "rechecked live first.")
    _armed = True


def claim(db, slot: int) -> list[dict]:
    """Called while building the relay batch: count one emit for each
    unrelayed notice (under the cap, not already sent in *slot*) and return
    them. Only runs that prepared notices carry them."""
    global _armed
    armed, _armed = _armed, False
    if not armed:
        return []
    rows = db.execute(
        """UPDATE notices SET emits = emits + 1, last_slot = ?
           WHERE relayed_at IS NULL AND emits < ?
             AND (last_slot IS NULL OR last_slot <> ?)
           RETURNING key, message_text""",
        (slot, NOTICE_MAX_EMITS, slot)).fetchall()
    db.commit()
    return [{"key": r["key"], "message": r["message_text"]}
            for r in sorted(rows, key=lambda r: r["key"])]


def mark(db, key: str) -> bool:
    cur = db.execute(
        "UPDATE notices SET relayed_at = ? WHERE key = ? AND relayed_at IS NULL",
        (store.utcnow_iso(), key))
    db.commit()
    return cur.rowcount == 1
