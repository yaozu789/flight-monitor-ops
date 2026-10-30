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
from datetime import date, datetime, timedelta, timezone
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

    def test_deadline_and_storage_crash_are_logged(self):
        def deadline(*a, **kw):
            raise store.DeadlineExceeded("run deadline")

        with self.assertRaises(store.DeadlineExceeded):
            self._check(deadline)

        def returning(*a, **kw):
            return [_row(400.0)]

        with mock.patch.object(store, "save_observations",
                               side_effect=RuntimeError("disk full")):
            with self.assertRaises(RuntimeError):
                self._check_with_start(returning)
        self.assertEqual([o for *_, o in self._logged()],
                         ["deadline_cancel", "error"])
        ceilings = {r["ceiling"] for r in self.db.execute(
            "SELECT ceiling FROM query_log")}
        self.assertEqual(ceilings, {self.guard.daily_ceiling})

    def _check_with_start(self, fake):
        with mock.patch.object(sentinel, "narrow_query", fake):
            return sentinel.check_cell(
                self.db, self.cell, self.guard, "nyc", GROUP, top_n=1,
                category="sentinel", slot=7, run_start=store.utcnow_iso())

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
            # fli's request builder rejects past dates: stay in the future.
            dep = date.today() + timedelta(days=30)
            return poller.narrow_query(
                "SFO", "JFK", dep.isoformat(),
                (dep + timedelta(days=7)).isoformat(),
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

        class FailThenEmpty:
            calls = 0

            def search(self, *a, **kw):
                FailThenEmpty.calls += 1
                if FailThenEmpty.calls == 1:
                    raise ConnectionError("reset")
                return []

        self.assertEqual(self._query(FailThenEmpty), [])
        self.assertEqual(poller.LAST_OUTCOME, "empty")
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

    def _db(self, *, outcomes, fresh=True, old_history=True, ceiling=150):
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
                   category, outcome, ceiling) VALUES (?, 1, 'nyc', 'near',
                   'sentinel', 'ok', ?)""",
                ((self.NOW - timedelta(days=6)).isoformat(timespec="seconds"),
                 ceiling))
        at = self.NOW - timedelta(days=1)
        for outcome, n in outcomes.items():
            for _ in range(n):
                db.execute(
                    """INSERT INTO query_log (at, slot, group_id, window_name,
                       category, outcome, ceiling) VALUES (?, 1, 'nyc', 'near',
                       'sentinel', ?, 150)""",
                    (at.isoformat(timespec="seconds"), outcome))
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

    def test_one_mid_run_throttle_counts_once(self):
        """One scheduled CAPTCHA leaves a throttle query row, a throttled
        slot and one transport health row: one event, not two."""
        db = self._db(outcomes={"ok": 99, "throttle": 1})
        db.execute("""INSERT INTO run_slots (slot_id, command, status,
                      claimed_at) VALUES (2, 'check', 'throttled', ?)""",
                   ((self.NOW - timedelta(hours=3)).isoformat(),))
        store.set_backoff(db, "captcha", 0.0, None, "mid-run")
        db.execute("UPDATE health SET checked_at=? WHERE check_name='throttle'",
                   ((self.NOW - timedelta(hours=3)).isoformat(
                       timespec="seconds"),))
        db.commit()
        report = fm_run.coverage_report(db, [{"id": "nyc"}], BudgetGuard(db),
                                        now=self.NOW)
        self.assertEqual(report["throttle_records"], 1)

    def test_manual_search_throttle_blocks_the_step(self):
        """A 429 during a manual narrow/canary search writes no query_log
        or run_slots row, only the transport's health record."""
        db = self._db(outcomes={"ok": 100})
        store.set_backoff(db, "429", 0.0, None, "manual canary")
        db.execute("UPDATE health SET checked_at=? WHERE check_name='throttle'",
                   ((self.NOW - timedelta(days=1)).isoformat(
                       timespec="seconds"),))
        db.commit()
        self.assertFalse(self._gate(db)["checks"]["no_throttles"])

    def test_short_window_cannot_shorten_the_gate(self):
        db = self._db(outcomes={"ok": 100}, old_history=False)
        report = fm_run.coverage_report(db, [{"id": "nyc"}], BudgetGuard(db),
                                        days=1, now=self.NOW)
        self.assertEqual(report["days"], fm_run.GATE_DAYS)
        self.assertFalse(report["gate"]["checks"]["history_days"])

    def test_stop_counted_by_when_it_finished(self):
        db = self._db(outcomes={"ok": 100})
        db.execute(
            """INSERT INTO run_slots (slot_id, command, status, claimed_at,
               finished_at) VALUES (9, 'check', 'deadline', ?, ?)""",
            ((self.NOW - timedelta(days=5, seconds=30)).isoformat(
                timespec="seconds"),
             (self.NOW - timedelta(days=4, hours=23)).isoformat(
                 timespec="seconds")))
        db.commit()
        self.assertFalse(self._gate(db)["checks"]["no_deadline_stops"])

    def test_history_at_previous_stage_does_not_count(self):
        """Raising the ceiling restarts the 5-day clock."""
        db = self._db(outcomes={"ok": 100}, ceiling=100)
        self.assertFalse(self._gate(db)["checks"]["history_days"])

    def test_restored_ceiling_restarts_the_stage_clock(self):
        db = self._db(outcomes={"ok": 100})  # 150 since 6 days ago
        db.execute(
            """INSERT INTO query_log (at, slot, group_id, window_name,
               category, outcome, ceiling) VALUES (?, 1, 'nyc', 'near',
               'sentinel', 'ok', 100)""",
            ((self.NOW - timedelta(days=3)).isoformat(timespec="seconds"),))
        db.execute(
            """INSERT INTO query_log (at, slot, group_id, window_name,
               category, outcome, ceiling) VALUES (?, 1, 'nyc', 'near',
               'sentinel', 'ok', 150)""",
            ((self.NOW - timedelta(hours=2)).isoformat(timespec="seconds"),))
        db.commit()
        self.assertFalse(self._gate(db)["checks"]["history_days"])

    def test_group_added_mid_stage_restarts_the_clock(self):
        """A new watch's fresh cells pass the refresh check at once, so its
        own 5 days at this ceiling must count, not the older groups'."""
        db = self._db(outcomes={"ok": 100})
        watches = [{"id": "nyc"}, {"id": "hawaii"}]
        report = fm_run.coverage_report(db, watches, BudgetGuard(db),
                                        now=self.NOW)
        self.assertFalse(report["gate"]["checks"]["history_days"])
        db.execute(
            """INSERT INTO query_log (at, slot, group_id, window_name,
               category, outcome, ceiling) VALUES (?, 1, 'hawaii', 'near',
               'sentinel', 'ok', 150)""",
            ((self.NOW - timedelta(days=2)).isoformat(timespec="seconds"),))
        db.commit()
        report = fm_run.coverage_report(db, watches, BudgetGuard(db),
                                        now=self.NOW)
        self.assertFalse(report["gate"]["checks"]["history_days"])
        db.execute("UPDATE query_log SET at=? WHERE group_id='hawaii'",
                   ((self.NOW - timedelta(days=6)).isoformat(
                       timespec="seconds"),))
        db.commit()
        report = fm_run.coverage_report(db, watches, BudgetGuard(db),
                                        now=self.NOW)
        self.assertTrue(report["gate"]["checks"]["history_days"])

    def test_configured_window_without_cells_fails(self):
        db = self._db(outcomes={"ok": 100})
        watch = {"id": "nyc", "origins": ["SFO"], "destinations": ["JFK"],
                 "nights": [7],
                 "windows": {"near": {"days": [7, 42], "refresh_hours": 48},
                             "far": {"months": [3, 9],
                                     "refresh_hours": 72}}}
        report = fm_run.coverage_report(db, [watch], BudgetGuard(db),
                                        now=self.NOW)
        self.assertFalse(report["gate"]["checks"]["refresh_p95"])
        self.assertIn(("nyc", "far", 0), {(r["group_id"], r["window_name"],
                                           r["cells"])
                                          for r in report["refresh"]})

    def test_ratio_just_over_limit_fails(self):
        """72.2 h against a 48 h target is 1.504: rounding must not pass it."""
        db = self._db(outcomes={"ok": 100})
        db.execute("UPDATE cells SET last_success_at=?",
                   ((self.NOW - timedelta(hours=72.2)).isoformat(),))
        db.commit()
        self.assertFalse(self._gate(db)["checks"]["refresh_p95"])

    def test_budget_cancels_are_not_unknowns(self):
        report = fm_run.coverage_report(
            self._db(outcomes={"ok": 50, "budget_cancel": 50}),
            [{"id": "nyc"}], BudgetGuard(fresh_db(self)), now=self.NOW)
        self.assertEqual(report["queries"]["completed"], 50)
        self.assertEqual(report["queries"]["unknown_rate"], 0)


if __name__ == "__main__":
    unittest.main()
