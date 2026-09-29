"""Relative ("unusually cheap") alerts (redesign PR C).

A route bucket (watch, origin, destination, window, nights) arms once it has
50 daily-low samples over 14 distinct days in the 28 days before today; its
threshold is then 70% of the median. Every alert step uses that threshold;
unarmed routes and collect-mode watches never alert. Offline, temp DBs.
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

import baseline  # noqa: E402
import outbox  # noqa: E402
import scorer  # noqa: E402
import sentinel  # noqa: E402
import store  # noqa: E402
import verifier  # noqa: E402
import weekly  # noqa: E402
from budget import BudgetGuard  # noqa: E402

import importlib.util as _ilu


def _load_run():
    spec = _ilu.spec_from_file_location("fm_run_rel",
                                        str(_PROJECT_ROOT / "run.py"))
    mod = _ilu.module_from_spec(spec)
    sys.modules["fm_run_rel"] = mod
    spec.loader.exec_module(mod)
    return mod


fm_run = _load_run()

NOW = datetime.now(timezone.utc).replace(microsecond=0)
TODAY = NOW.date()
WATCH = {"id": "nyc", "origins": ["SFO"], "destinations": ["JFK"],
         "absolute_cap_usd": 250, "alert_mode": "relative", "nights": [7],
         "windows": {"near": {"days": [7, 42], "step_days": 2,
                              "refresh_hours": 48}}}


def fresh_db(testcase):
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    db = store.init_db(path)
    testcase.addCleanup(
        lambda: (db.close(), os.path.exists(path) and os.unlink(path)))
    return db


def obs(db, seen: datetime, depart: date, price, origin="SFO", dest="JFK",
        nights=7, watch="nyc"):
    store.add_observation(
        db, watch_id=watch, origin=origin, destination=dest,
        depart_date=depart.isoformat(),
        return_date=(depart + timedelta(days=nights)).isoformat(),
        airline="UA", stops=0, cabin="economy", price_usd=price,
        booking_url="https://example.invalid/b",
        observed_at=seen.isoformat(timespec="seconds"))


def seed_history(db, days=20, per_day=3, price=400.0):
    """*days* observation days before today, *per_day* departure dates each
    (all 10-30 days out, inside the near window): days x per_day samples."""
    for d in range(1, days + 1):
        seen = NOW - timedelta(days=d)
        for k in range(per_day):
            obs(db, seen, seen.date() + timedelta(days=10 + 2 * k),
                price + (k - 1) * 10)


class BaselineTests(unittest.TestCase):
    def setUp(self):
        self.db = fresh_db(self)
        self.cfg = fm_run.group_cfg_for(WATCH)

    def _base(self, depart=None):
        depart = depart or TODAY + timedelta(days=14)
        return baseline.baseline(self.db, "nyc", self.cfg, "SFO", "JFK",
                                 depart.isoformat(),
                                 (depart + timedelta(days=7)).isoformat(),
                                 NOW)

    def test_arms_at_50_samples_over_14_days(self):
        seed_history(self.db, days=20, per_day=3)          # 60 samples
        b = self._base()
        self.assertTrue(b["armed"])
        self.assertEqual((b["samples"], b["days"], b["median"]),
                         (60, 20, 400.0))

    def test_not_armed_below_14_days_or_50_samples(self):
        seed_history(self.db, days=13, per_day=5)          # 65 samples, 13 days
        self.assertFalse(self._base()["armed"])
        db2 = fresh_db(self)
        self.db = db2
        seed_history(db2, days=20, per_day=2)              # 40 samples
        self.assertFalse(self._base()["armed"])

    def test_today_never_judges_itself_and_is_stable_all_day(self):
        seed_history(self.db)
        first = self._base()
        for k in range(30):  # a flood of cheap fares today
            obs(self.db, NOW, TODAY + timedelta(days=12 + k % 5), 100.0)
        self.assertEqual(self._base()["median"], first["median"])

    def test_threshold_by_mode(self):
        seed_history(self.db)
        dep = TODAY + timedelta(days=14)
        args = ("SFO", "JFK", dep.isoformat(),
                (dep + timedelta(days=7)).isoformat(), NOW)
        limit, _ = baseline.threshold(self.db, WATCH, self.cfg, *args)
        self.assertAlmostEqual(limit, 280.0)
        capped = dict(WATCH, max_alert_usd=200)
        self.assertEqual(baseline.threshold(self.db, capped, self.cfg,
                                            *args)[0], 200.0)
        self.assertEqual(baseline.threshold(
            self.db, dict(WATCH, alert_mode="cap"), self.cfg, *args)[0], 250.0)
        self.assertIsNone(baseline.threshold(
            self.db, dict(WATCH, alert_mode="collect"), self.cfg, *args)[0])
        # Another airport pair has no history of its own: silent.
        self.assertIsNone(baseline.threshold(
            self.db, WATCH, self.cfg, "OAK", "JFK", *args[2:])[0])


class AlertPathTests(unittest.TestCase):
    def setUp(self):
        self.db = fresh_db(self)
        seed_history(self.db)

    def test_scorer_uses_relative_threshold(self):
        dep = TODAY + timedelta(days=16)
        obs(self.db, NOW, dep, 270.0)                      # <= 280: candidate
        obs(self.db, NOW, dep + timedelta(days=2), 300.0)  # > 280: no
        obs(self.db, NOW, dep, 150.0, origin="OAK")        # unarmed pair: no
        cands = scorer.score_new_observations(
            self.db, WATCH, (NOW - timedelta(minutes=1)).isoformat())
        self.assertEqual([(c["origin"], c["price_usd"], c["reason"])
                          for c in cands], [("SFO", 270.0, "relative")])
        self.assertAlmostEqual(cands[0]["threshold"], 280.0)
        self.assertEqual(cands[0]["median"], 400.0)
        self.assertEqual(scorer.score_new_observations(
            self.db, dict(WATCH, alert_mode="collect"),
            (NOW - timedelta(minutes=1)).isoformat()), [])

    def test_retry_recheck_without_threshold_never_searches(self):
        row = {"depart_date": (TODAY + timedelta(days=16)).isoformat(),
               "return_date": (TODAY + timedelta(days=23)).isoformat(),
               "origin": "OAK", "destination": "JFK", "fingerprint": "x",
               "created_at": NOW.isoformat()}
        with mock.patch.object(verifier.poller, "narrow_query",
                               side_effect=AssertionError("searched")):
            self.assertEqual(verifier.recheck_alert_detailed(
                row, self.db, WATCH, None), ("unknown", None))

    def test_alert_message_names_the_typical_price(self):
        conf = {"price_usd": 270.0, "typical_usd": 400.0,
                "depart_date": "2026-11-01", "return_date": "2026-11-08",
                "origin": "SFO", "destination": "JFK", "airline": "UA",
                "stops": 0, "observed_at": NOW.isoformat()}
        text = outbox.render_alert_message(WATCH, conf, "fp", NOW.isoformat())
        self.assertIn("Typical for this route: $400 (28-day median of "
                      "stored fares); this fare is 32% below.", text)

    def test_relative_watch_queues_no_drop_trigger(self):
        cfg = {"nyc": fm_run.group_cfg_for(WATCH)}
        sentinel.mint_sentinels(self.db, cfg)
        cell = store.due_cells(self.db, "sentinel", 1, group_ids=cfg)[0]
        store.record_cell_check(self.db, cell["id"], slot=1, success=True,
                                price_usd=1000.0)
        self.db.execute("UPDATE cells SET last_checked_slot = NULL")
        self.db.commit()

        def cheap(db, cell, guard, group_id, group_cfg, *, top_n, category,
                  slot, run_start=None):
            return (300.0, 2)

        guard = BudgetGuard(self.db, per_run_max=10_000, verify_reserve=0,
                            category_caps={})
        with mock.patch.object(sentinel, "check_cell", cheap), \
                mock.patch.object(sentinel, "per_run_allowance",
                                  lambda *a, **k: 4):
            res = sentinel.collect(self.db, guard, cfg, store.slot_id(),
                                   store.utcnow_iso())
        self.assertGreater(res["sentinels"], 0)
        self.assertEqual(res["drop_triggers"], 0)


class ReportTests(unittest.TestCase):
    def test_digest_and_coverage_show_route_readiness(self):
        db = fresh_db(self)
        seed_history(db)
        obs(db, NOW - timedelta(hours=2), TODAY + timedelta(days=16), 390.0)
        obs(db, NOW - timedelta(hours=2), TODAY + timedelta(days=16), 380.0,
            origin="OAK")
        both = dict(WATCH, origins=["SFO", "OAK"])
        text = weekly.build_report(db, [both], NOW, "2026-W41")
        # The cheapest fare is the OAK leg, which has no history of its own.
        self.assertIn("$380 OAK -> JFK", text)
        self.assertIn("typical price: building history (0/50 samples, "
                      "0/14 days)", text)
        db2 = fresh_db(self)
        seed_history(db2)
        obs(db2, NOW - timedelta(hours=2), TODAY + timedelta(days=16), 390.0)
        text = weekly.build_report(db2, [WATCH], NOW, "2026-W41")
        self.assertIn("typical $400 for SFO -> JFK (median of 60 daily lows)",
                      text)
        report = fm_run.coverage_report(db2, [WATCH], BudgetGuard(db2),
                                        now=NOW)
        self.assertEqual(report["relative_routes"]["nyc"],
                         {"armed": 1, "routes": 1})


if __name__ == "__main__":
    unittest.main()
