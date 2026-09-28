"""Outcome counters, refresh ages and the budget-ramp gate (redesign A3).

Every collection query logs one outcome (ok / empty / transport_fail /
unparseable / error / budget_cancel / throttle). ``run.py coverage`` turns
those, the refresh age of each group x window, deadline stops and
throttled runs into the pass/fail verdict for the next budget step.
Offline: fli's client and poller sleeps are faked, temp file DBs.
"""
from __future__ import annotations

import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

import importlib.util as _ilu

import poller  # noqa: E402
import sentinel  # noqa: E402
import store  # noqa: E402
from budget import BudgetExhausted, BudgetGuard  # noqa: E402
from transport import ThrottleDetected  # noqa: E402


def _load_run():
    spec = _ilu.spec_from_file_location("fm_run_cov",
                                        str(_PROJECT_ROOT / "run.py"))
    mod = _ilu.module_from_spec(spec)
    sys.modules["fm_run_cov"] = mod
    spec.loader.exec_module(mod)
    return mod


fm_run = _load_run()

GROUP = {"origins": ["SFO"], "destinations": ["JFK"]}


def fresh_db(testcase):
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    db = store.init_db(path)
    testcase.addCleanup(
        lambda: (db.close(), os.path.exists(path) and os.unlink(path)))
    return db


def _row(price):
    return {"origin": "SFO", "destination": "JFK",
            "depart_date": "2026-11-01", "return_date": "2026-11-08",
            "airline": "UA", "stops": 0, "price_usd": price,
            "currency": "USD", "booking_url": "https://example.invalid/b",
            "cabin": "economy"}


class CheckCellOutcomeTests(unittest.TestCase):
    def setUp(self):
        self.db = fresh_db(self)
        self.guard = BudgetGuard(self.db)
        self.cell = {"id": None, "depart_date": "2026-11-01", "duration": 7,
                     "window_name": "near"}

    def _check(self, fake):
        with mock.patch.object(sentinel, "narrow_query", fake):
            return sentinel.check_cell(
                self.db, self.cell, self.guard, "nyc", GROUP, top_n=1,
                category="sentinel", slot=7)

    def _logged(self):
        return [(r["group_id"], r["window_name"], r["category"],
                 r["outcome"]) for r in self.db.execute(
            "SELECT * FROM query_log ORDER BY id")]

    def test_each_result_logs_one_outcome(self):
        def returning(rows, last=None):
            def fake(*a, **kw):
                poller.LAST_OUTCOME = last
                return rows
            return fake

        def raising(exc):
            def fake(*a, **kw):
                raise exc
            return fake

        self._check(returning([_row(400.0)], "ok"))
        self._check(returning([], "empty"))
        self._check(returning([], "transport_fail"))
        self._check(returning([_row(None)], "ok"))  # no usable price
        self._check(raising(RuntimeError("flatten bug")))
        with self.assertRaises(BudgetExhausted):
            self._check(raising(BudgetExhausted("daily", "sentinel", 150,
                                                150)))
        with self.assertRaises(ThrottleDetected):
            self._check(raising(ThrottleDetected("429")))
        self.assertEqual([o for *_, o in self._logged()],
                         ["ok", "empty", "transport_fail", "unparseable",
                          "error", "budget_cancel", "throttle"])
        self.assertEqual({(g, w, c) for g, w, c, _ in self._logged()},
                         {("nyc", "near", "sentinel")})

    def test_stale_poller_outcome_never_leaks(self):
        """A fake (or failed) query that sets nothing reads as empty, not
        as the previous query's outcome."""
        poller.LAST_OUTCOME = "transport_fail"
        self._check(lambda *a, **kw: [])
        self.assertEqual(self._logged()[-1][-1], "empty")


class PollerOutcomeTests(unittest.TestCase):
    def _query(self, client_cls):
        with mock.patch.object(poller, "SearchFlights", client_cls), \
                mock.patch.object(poller.time, "sleep", lambda s: None), \
                mock.patch.object(poller, "_polite", lambda: None):
            return poller.narrow_query(
                "SFO", "JFK", "2026-11-01", "2026-11-08",
                {"origins": ["SFO"], "destinations": ["JFK"]},
                top_n=1, require_guard=False)

    def test_failed_attempts_are_transport_fail_not_empty(self):
        class Failing:
            def search(self, *a, **kw):
                raise ConnectionError("reset")

        class Empty:
            def search(self, *a, **kw):
                return []

        self.assertEqual(self._query(Failing), [])
        self.assertEqual(poller.LAST_OUTCOME, "transport_fail")
        self.assertEqual(self._query(Empty), [])
        self.assertEqual(poller.LAST_OUTCOME, "empty")


class RefreshAgeTests(unittest.TestCase):
    def test_never_succeeded_cells_count_from_minting(self):
        db = fresh_db(self)
        now = datetime(2026, 10, 10, 12, tzinfo=timezone.utc)
        cells = [{"group_id": "nyc", "depart_date": f"2026-11-{d:02d}",
                  "duration": 7, "role": "sentinel", "weekday": 0,
                  "window": "near", "refresh_hours": 48}
                 for d in range(1, 21)]
        store.ensure_cells(db, cells)
        ids = [r["id"] for r in db.execute("SELECT id FROM cells ORDER BY id")]
        db.execute("UPDATE cells SET created_at=?",
                   ((now - timedelta(hours=200)).isoformat(),))
        for i in ids[:19]:  # 19 fresh, 1 never succeeded since minting
            db.execute("UPDATE cells SET last_success_at=? WHERE id=?",
                       ((now - timedelta(hours=10)).isoformat(), i))
        db.commit()
        (row,) = store.refresh_ages(db, ["nyc"], now.isoformat())
        self.assertEqual((row["window_name"], row["cells"]), ("near", 20))
        self.assertEqual(row["p50_h"], 10.0)
        self.assertEqual(row["p95_h"], 10.0)
        self.assertEqual(row["max_h"], 200.0)
        store.ensure_cells(db, [dict(cells[0], depart_date="2026-12-01"),
                                dict(cells[0], depart_date="2026-12-02")])
        db.execute("UPDATE cells SET created_at=? WHERE last_success_at "
                   "IS NULL", ((now - timedelta(hours=200)).isoformat(),))
        db.commit()
        (row,) = store.refresh_ages(db, ["nyc"], now.isoformat())
        self.assertEqual(row["p95_h"], 200.0)  # 3/22 stale: the p95 sees it
        self.assertGreater(row["p95_ratio"], fm_run.GATE_MAX_P95_RATIO)


class GateTests(unittest.TestCase):
    NOW = datetime(2026, 10, 10, 12, tzinfo=timezone.utc)

    def _db(self, *, outcomes, fresh=True, old_history=True):
        db = fresh_db(self)
        store.ensure_cells(db, [{
            "group_id": "nyc", "depart_date": "2026-11-01", "duration": 7,
            "role": "sentinel", "weekday": 6, "window": "near",
            "refresh_hours": 48}])
        age = 10 if fresh else 100
        db.execute("UPDATE cells SET last_success_at=?, created_at=?",
                   ((self.NOW - timedelta(hours=age)).isoformat(),
                    (self.NOW - timedelta(days=9)).isoformat()))
        if old_history:  # one query before the window: 5 full days seen
            db.execute(
                """INSERT INTO query_log (at, slot, group_id, window_name,
                   category, outcome) VALUES (?, 1, 'nyc', 'near',
                   'sentinel', 'ok')""",
                ((self.NOW - timedelta(days=6)).isoformat(),))
        at = self.NOW - timedelta(days=1)
        for outcome, n in outcomes.items():
            for _ in range(n):
                db.execute(
                    """INSERT INTO query_log (at, slot, group_id, window_name,
                       category, outcome) VALUES (?, 1, 'nyc', 'near',
                       'sentinel', ?)""", (at.isoformat(), outcome))
                at += timedelta(minutes=1)
        db.commit()
        return db

    def _gate(self, db):
        return fm_run.coverage_report(
            db, [{"id": "nyc"}], BudgetGuard(db), now=self.NOW)["gate"]

    def test_clean_days_pass(self):
        gate = self._gate(self._db(outcomes={"ok": 99, "empty": 1}))
        self.assertTrue(gate["pass"], gate)

    def test_each_failure_blocks_the_step(self):
        cases = {
            "unknown_rate": self._db(outcomes={"ok": 90, "transport_fail": 10}),
            "no_throttles": self._db(outcomes={"ok": 99, "throttle": 1}),
            "refresh_p95": self._db(outcomes={"ok": 100}, fresh=False),
            "history_days": self._db(outcomes={"ok": 100}, old_history=False),
        }
        for check, db in cases.items():
            with self.subTest(check=check):
                gate = self._gate(db)
                self.assertFalse(gate["pass"])
                self.assertFalse(gate["checks"][check])
        db = self._db(outcomes={"ok": 100})
        for status in ("deadline", "throttled"):
            db.execute(
                """INSERT INTO run_slots (slot_id, command, status, claimed_at)
                   VALUES (?, 'check', ?, ?)""",
                (1 if status == "deadline" else 2, status,
                 (self.NOW - timedelta(hours=3)).isoformat()))
        db.commit()
        gate = self._gate(db)
        self.assertFalse(gate["checks"]["no_deadline_stops"])
        self.assertFalse(gate["checks"]["no_throttles"])

    def test_budget_cancels_are_not_unknowns(self):
        report = fm_run.coverage_report(
            self._db(outcomes={"ok": 50, "budget_cancel": 50}),
            [{"id": "nyc"}], BudgetGuard(fresh_db(self)), now=self.NOW)
        self.assertEqual(report["queries"]["completed"], 50)
        self.assertEqual(report["queries"]["unknown_rate"], 0)


if __name__ == "__main__":
    unittest.main()
