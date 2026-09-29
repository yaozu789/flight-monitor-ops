"""One-time milestone notices and the configurable daily ceiling.

Scheduled runs create a notice once when a milestone is reached (budget
step ready, a hold at the current stage, alerts live on a relative watch);
it rides at most two relay batches until notice-mark. The daily ceiling
comes from watches.yaml, so a budget step is a config change.
"""
from __future__ import annotations

import os
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

import budget  # noqa: E402
import dashboard  # noqa: E402
import notices  # noqa: E402
import store  # noqa: E402
import weekly  # noqa: E402
from budget import BudgetGuard  # noqa: E402

import importlib.util as _ilu


def _load_run():
    spec = _ilu.spec_from_file_location("run", str(_PROJECT_ROOT / "run.py"))
    mod = _ilu.module_from_spec(spec)
    sys.modules.setdefault("run", mod)
    spec.loader.exec_module(mod)
    return sys.modules["run"]


fm_run = _load_run()
NOW = datetime(2026, 10, 4, 18, 0, tzinfo=timezone.utc)
WATCHES = [{"id": "japan", "alert_mode": "relative", "daily_ceiling": 150},
           {"id": "nyc", "alert_mode": "collect", "daily_ceiling": 150}]


def fresh_db(testcase):
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    db = store.init_db(path)
    testcase.addCleanup(
        lambda: (db.close(), os.path.exists(path) and os.unlink(path)))
    return db


def report(passed=True, history=True, throttles=True, routes=None):
    return {"days": 5, "gate": {"pass": passed, "checks": {
                "history_days": history, "no_throttles": throttles,
                "unknown_rate": True, "no_deadline_stops": True,
                "refresh_p95": True}},
            "queries": {"unknown_rate": 0.012},
            "relative_routes": routes or {}}


class NoticeTests(unittest.TestCase):
    def setUp(self):
        self.db = fresh_db(self)
        self.guard = BudgetGuard(self.db, daily_ceiling=150)
        self.addCleanup(setattr, notices, "_armed", False)

    def _prepare(self, rep):
        with mock.patch.object(fm_run, "coverage_report",
                               lambda *a, **k: rep):
            notices.prepare(self.db, WATCHES, self.guard, NOW)

    def test_gate_ready_once_with_next_step(self):
        self._prepare(report())
        got = notices.claim(self.db, 10)
        self.assertEqual([n["key"] for n in got], ["gate_ready:150"])
        self.assertIn("225", got[0]["message"])
        self.assertIn("LA, San Diego and Hong Kong", got[0]["message"])
        self._prepare(report())  # the same milestone again: no new row
        self.assertEqual(self.db.execute(
            "SELECT COUNT(*) AS c FROM notices").fetchone()["c"], 1)

    def test_rides_two_batches_once_per_slot_until_marked(self):
        self._prepare(report())
        self.assertEqual(len(notices.claim(self.db, 10)), 1)
        self._prepare(report())
        self.assertEqual(notices.claim(self.db, 10), [])      # same slot
        self._prepare(report())
        self.assertEqual(len(notices.claim(self.db, 11)), 1)  # 2nd emit
        self._prepare(report())
        self.assertEqual(notices.claim(self.db, 12), [])      # cap reached
        self.assertTrue(notices.mark(self.db, "gate_ready:150"))
        self.assertFalse(notices.mark(self.db, "gate_ready:150"))

    def test_hold_only_after_history_and_alerts_live_only_for_relative(self):
        self._prepare(report(passed=False, history=False, throttles=False))
        self.assertEqual(notices.claim(self.db, 10), [])
        rep = report(passed=False, throttles=False, routes={
            "japan": {"armed": 3, "routes": 18},
            "nyc": {"armed": 5, "routes": 18}})
        rep["gate"]["checks"]["refresh_p95"] = False
        self._prepare(rep)
        got = notices.claim(self.db, 11)
        self.assertEqual([n["key"] for n in got],
                         ["alerts_live:japan", "gate_hold:150"])
        hold = got[1]["message"]  # one notice naming both failing checks
        self.assertIn("throttled", hold)
        self.assertIn("not being rechecked in time", hold)

    def test_unarmed_run_carries_nothing(self):
        self._prepare(report())
        notices._armed = False  # e.g. a manual run: prepare() never ran
        self.assertEqual(notices.claim(self.db, 10), [])

    def test_handoff_and_instruction_carry_notices(self):
        self._prepare(report())
        payload = fm_run._handoff_payload(self.db, 10, [])
        self.assertEqual(payload["notices"][0]["key"], "gate_ready:150")
        self.assertIn("notice-mark --key", payload["agent_instruction"])


class CeilingTests(unittest.TestCase):
    def test_ceiling_from_config_everywhere(self):
        ws = [dict(w, daily_ceiling=225) for w in WATCHES]
        self.assertEqual(budget.daily_ceiling(ws), 225)
        self.assertEqual(budget.daily_ceiling([{"id": "x"}]),
                         budget.DAILY_CEILING)
        with self.assertRaises(ValueError):
            budget.daily_ceiling([dict(WATCHES[0], daily_ceiling=0)])
        with self.assertRaises(ValueError):
            budget.daily_ceiling([dict(WATCHES[0], daily_ceiling=150),
                                  dict(WATCHES[1], daily_ceiling=225)])
        db = fresh_db(self)
        ws = [dict(w, absolute_cap_usd=500) for w in ws]
        self.assertEqual(dashboard.payload(db, ws, NOW)["ceiling"], 225)
        self.assertIn("(limit 225/day)",
                      weekly.build_report(db, ws, NOW, "2026-W41"))

    def test_shipped_config_ceiling(self):
        _, watches = fm_run.load_config()
        self.assertEqual(budget.daily_ceiling(watches), 150)


if __name__ == "__main__":
    unittest.main()
