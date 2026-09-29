"""Weekly price report: built from stored data only, due from Monday 16:00
UTC, carried by scheduled relay batches (no-op and stop outputs included)
until report-mark, at most REPORT_MAX_EMITS times."""
import contextlib
import io
import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock
from urllib.parse import unquote

_TESTS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(_TESTS_DIR))
sys.path.insert(0, str(_TESTS_DIR.parent))

import heartbeat  # noqa: E402
import store  # noqa: E402
import transport  # noqa: E402
import weekly  # noqa: E402
from budget import BudgetGuard  # noqa: E402
from test_run_lifecycle import FAKE_URL, args_for, fm_run  # noqa: E402

# Monday 2026-09-28 is in ISO week 2026-W40.
MON_DUE = datetime(2026, 9, 28, 16, 0, tzinfo=timezone.utc)
WATCHES = [{"id": "japan", "absolute_cap_usd": 500},
           {"id": "nyc", "absolute_cap_usd": 250}]


def fresh_db(testcase):
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    db = store.init_db(path)
    testcase.addCleanup(lambda: (db.close(), os.unlink(path)))
    return db


def obs(db, when, price, watch="japan", depart="2027-01-12",
        ret="2027-01-19", origin="SFO", dest="NRT"):
    store.add_observation(
        db, watch_id=watch, origin=origin, destination=dest,
        depart_date=depart, return_date=ret, airline="ANA", stops=0,
        cabin="economy", price_usd=price, booking_url="",
        observed_at=when.isoformat(timespec="seconds"))


class ReportWeekTests(unittest.TestCase):
    def test_due_from_monday_1600_utc(self):
        self.assertIsNone(weekly.report_week(MON_DUE - timedelta(minutes=1)))
        self.assertEqual(weekly.report_week(MON_DUE), "2026-W40")
        # A run missed on Monday still sends it later that week.
        self.assertEqual(weekly.report_week(MON_DUE + timedelta(days=5)),
                         "2026-W40")
        # The previous Sunday belongs to the previous week.
        self.assertEqual(weekly.report_week(MON_DUE - timedelta(hours=17)),
                         "2026-W39")


class BuildReportTests(unittest.TestCase):
    def test_report_content_from_stored_data(self):
        db = fresh_db(self)
        obs(db, MON_DUE - timedelta(days=2), 812)
        obs(db, MON_DUE - timedelta(days=3), 790, depart="2027-02-03",
            ret="2027-02-13")
        obs(db, MON_DUE - timedelta(days=10), 845)  # prior week
        obs(db, MON_DUE - timedelta(days=1), 340, watch="nyc", dest="JFK",
            depart="2026-12-02", ret="2026-12-09")
        for _ in range(3):
            store.ledger_insert(db, (MON_DUE - timedelta(days=1)).date()
                                .isoformat(), "sentinel", 1)
        text = weekly.build_report(db, WATCHES, MON_DUE, "2026-W40")
        self.assertIn("Weekly flight price report -- 2026-09-21 to "
                      "2026-09-28", text)
        self.assertIn("Cheapest seen: $790, SFO -> NRT 2027-02-03 -> "
                      "2027-02-13 (10 nights), ANA, seen 09-25", text)
        self.assertIn("Prior 7 days: $845 (down $55)", text)
        self.assertIn("By departure month: Jan 27 $812 | Feb 27 $790", text)
        self.assertIn("2 fares stored on 2 day(s)", text)
        self.assertIn("nyc (cap $250)", text)
        self.assertIn("Prior 7 days: no fares stored.", text)
        self.assertIn("Deal alerts handed off this week: none", text)
        self.assertIn("Google requests: 3 in 7 days", text)
        self.assertTrue(text.endswith("Weekly report: 2026-W40"))
        self.assertIn("Dashboard (zoom, runs, requests): "
                      "https://yaozu789.github.io/flight-monitor/dashboard/#v1.", text)
        url = next(l for l in text.splitlines() if l.startswith("Chart"))
        config = json.loads(unquote(url.split("?c=", 1)[1]))
        japan = config["data"]["datasets"][0]
        self.assertEqual(japan["label"], "japan daily low")
        self.assertEqual([p for p in japan["data"] if p is not None],
                         [845, 790, 812])

    def test_empty_watch_says_so(self):
        db = fresh_db(self)
        text = weekly.build_report(db, WATCHES, MON_DUE, "2026-W40")
        self.assertIn("No fares stored in the last 7 days.", text)
        self.assertNotIn("Chart", text)


JAPAN_W = {"id": "japan", "origins": ["SFO", "OAK", "SJC"],
           "destinations": ["NRT", "HND", "KIX"], "absolute_cap_usd": 500,
           "alert_mode": "collect", "nights": [28],
           "windows": {"near": {"days": [7, 42], "step_days": 2,
                                "refresh_hours": 48},
                       "far": {"months": [3, 9], "refresh_hours": 72}}}
LA_W = {"id": "la", "origins": ["SFO"], "destinations": ["LAX"],
        "absolute_cap_usd": 120, "alert_mode": "collect",
        "windows": {"near": {"days": [7, 42], "weekdays": [4, 5],
                             "return_weekday": 6, "refresh_hours": 48}}}


def _d(days, stay=28):
    dep = (MON_DUE + timedelta(days=days)).date()
    return dep.isoformat(), (dep + timedelta(days=stay)).isoformat()


class _ObsMixin:
    def _obs(self, db, days_out, price, *, stay=28, seen_h=24, dest="NRT",
             watch="japan"):
        dep, ret = _d(days_out, stay)
        obs(db, MON_DUE - timedelta(hours=seen_h), price, depart=dep,
            ret=ret, dest=dest, watch=watch)


class DigestTests(_ObsMixin, unittest.TestCase):
    """Redesign A4: per departure window, the cheapest stored fare whose
    departure is inside the window now and whose stay the watch tracks."""

    def test_cheapest_per_window_only_in_window_and_stay(self):
        db = fresh_db(self)
        self._obs(db, 10, 812)
        self._obs(db, 12, 300, stay=7)        # not a tracked stay
        self._obs(db, 3, 250)                 # departs before the window
        self._obs(db, 120, 700, dest="KIX")
        self._obs(db, 120, 760, seen_h=24 * 9, dest="KIX")  # prior week
        text = weekly.build_report(db, [JAPAN_W], MON_DUE, "2026-W40")
        dep, ret = _d(10)
        self.assertIn("japan (about 28 nights)", text)
        self.assertIn(f"  Next 1-6 weeks: $812 SFO -> NRT {dep} -> {ret} "
                      "(28 nights), ANA nonstop, seen 24h ago", text)
        dep, ret = _d(120)
        self.assertIn(f"  3-9 months out: $700 SFO -> KIX {dep} -> {ret}",
                      text)
        self.assertIn("    prior week low $760 (down $60)", text)
        self.assertIn("Deal alerts: off while price history builds up",
                      text)
        self.assertNotIn("cap", text.split("Chart")[0])
        self.assertNotIn("$300", text)
        self.assertNotIn("$250", text)

    def test_missing_and_stale_coverage_is_explicit(self):
        db = fresh_db(self)
        self._obs(db, 10, 812, seen_h=72)      # older than the 48 h target
        import run
        import sentinel
        cfg = {"japan": run.group_cfg_for(JAPAN_W)}
        sentinel.mint_sentinels(db, cfg, today=MON_DUE.date())
        near = [r["id"] for r in db.execute(
            "SELECT id FROM cells WHERE window_name='near' ORDER BY id")]
        db.execute("UPDATE cells SET last_success_at=? WHERE id=?",
                   ((MON_DUE - timedelta(hours=5)).isoformat(
                       timespec="seconds"), near[0]))
        db.commit()
        text = weekly.build_report(db, [JAPAN_W], MON_DUE, "2026-W40")
        self.assertIn("seen 3d ago (stale)", text)
        self.assertIn("    dates checked in the last 48h: 1/18; 17 not "
                      "checked recently", text)
        self.assertIn("  3-9 months out: no fares stored this week", text)

    def test_weekend_watch_and_no_cap_line_in_chart(self):
        db = fresh_db(self)
        self._obs(db, 11, 118, stay=2, dest="LAX", watch="la")  # Fri-Sun
        text = weekly.build_report(db, [LA_W], MON_DUE, "2026-W40")
        self.assertIn("la (weekend trips)", text)
        self.assertIn("  Next 1-6 weeks: $118", text)
        self.assertIn("    dates checked in the last 48h: 0/10; 10 not "
                      "checked recently", text)  # 5 Fri + 5 Sat, none yet
        url = next(l for l in text.splitlines() if l.startswith("Chart"))
        labels = [d["label"] for d in json.loads(
            unquote(url.split("?c=", 1)[1]))["data"]["datasets"]]
        self.assertEqual(labels, ["la daily low"])


NYC_W = {"id": "nyc", "origins": ["SFO"], "destinations": ["JFK"],
         "absolute_cap_usd": 250, "alert_mode": "collect", "nights": [7, 14],
         "windows": {"near": {"days": [7, 42], "step_days": 2,
                              "refresh_hours": 48}}}


class DigestReviewTests(_ObsMixin, unittest.TestCase):
    """Codex review of PR #36: weekday/return filters, dates not cells,
    bounds at report time, offset-aware ages, prior week without this week."""

    def test_weekend_window_ignores_midweek_fares(self):
        db = fresh_db(self)
        self._obs(db, 8, 60, stay=2, dest="LAX", watch="la")    # Tue-Thu
        self._obs(db, 12, 95, stay=1, dest="LAX", watch="la")   # Sat-Sun
        text = weekly.build_report(db, [LA_W], MON_DUE, "2026-W40")
        self.assertNotIn("$60", text)
        dep, ret = _d(12, 1)
        self.assertIn(f"  Next 1-6 weeks: $95 SFO -> LAX {dep} -> {ret} "
                      "(1 nights)", text)

    def test_coverage_counts_dates_inside_window_now(self):
        db = fresh_db(self)
        import run
        import sentinel
        sentinel.mint_sentinels(db, {"nyc": run.group_cfg_for(NYC_W)},
                                today=MON_DUE.date() - timedelta(days=1))
        # 7- and 14-night cells: 18 dates, not 36. Minted yesterday, so the
        # date that fell out of the window today must not count.
        db.execute("UPDATE cells SET last_success_at=?",
                   ((MON_DUE - timedelta(hours=1)).isoformat(
                       timespec="seconds"),))
        db.commit()
        text = weekly.build_report(db, [NYC_W], MON_DUE, "2026-W40")
        line = next(l for l in text.splitlines() if "dates checked" in l)
        # Today's window has 18 dates on the 2-day lattice. Cells minted
        # yesterday cover 17 of them: the new one is unchecked, the one that
        # left the window is not counted.
        self.assertEqual(line, "    dates checked in the last 48h: 17/18; "
                               "1 not checked recently")

    def test_offset_timestamps_are_converted_not_overwritten(self):
        db = fresh_db(self)
        dep, ret = _d(10)
        seen = (MON_DUE - timedelta(hours=42)).astimezone(
            timezone(timedelta(hours=-7)))
        store.add_observation(
            db, watch_id="japan", origin="SFO", destination="NRT",
            depart_date=dep, return_date=ret, airline="ANA", stops=0,
            cabin="economy", price_usd=812, booking_url="",
            observed_at=seen.isoformat(timespec="seconds"))
        text = weekly.build_report(db, [JAPAN_W], MON_DUE, "2026-W40")
        self.assertIn("seen 42h ago", text)
        self.assertNotIn("(stale)", text)

    def test_digest_only_tracked_airports_and_stops(self):
        """An old row with the layover stored as the destination, or with
        more stops than the watch allows, is not a fare the watch tracks."""
        db = fresh_db(self)
        dep, ret = _d(10, 7)
        for dest, stops, price in (("ATL", 1, 150), ("JFK", 2, 200),
                                   ("JFK", 1, 343)):
            store.add_observation(
                db, watch_id="nyc", origin="SFO", destination=dest,
                depart_date=dep, return_date=ret, airline="F9", stops=stops,
                cabin="economy", price_usd=price, booking_url="",
                observed_at=(MON_DUE - timedelta(hours=5)).isoformat(
                    timespec="seconds"))
        text = weekly.build_report(db, [NYC_W], MON_DUE, "2026-W40")
        self.assertIn("  Next 1-6 weeks: $343 SFO -> JFK", text)
        self.assertNotIn("ATL", text)
        self.assertNotIn("$200", text)

    def test_prior_week_low_shown_without_this_week(self):
        db = fresh_db(self)
        self._obs(db, 10, 830, seen_h=24 * 9)
        text = weekly.build_report(db, [JAPAN_W], MON_DUE, "2026-W40")
        self.assertIn("  Next 1-6 weeks: no fares stored this week\n"
                      "    prior week low $830", text)


class PrepareClaimTests(unittest.TestCase):
    def setUp(self):
        self.db = fresh_db(self)
        self.addCleanup(setattr, weekly, "_armed_week", None)

    def emits(self):
        return self.db.execute(
            "SELECT emits FROM weekly_reports").fetchone()["emits"]

    def test_not_stored_before_due(self):
        early = MON_DUE - timedelta(minutes=1)
        self.assertIsNone(weekly.prepare(self.db, WATCHES, now=early))
        self.assertIsNone(weekly.claim(self.db, 100))
        self.assertIsNone(self.db.execute(
            "SELECT 1 FROM weekly_reports").fetchone())

    def test_emit_counted_at_claim_not_prepare(self):
        """A run killed between prepare and printing spends no emit."""
        weekly.prepare(self.db, WATCHES, now=MON_DUE)
        self.assertEqual(self.emits(), 0)
        self.assertEqual(weekly.claim(self.db, 100)["week"], "2026-W40")
        self.assertEqual(self.emits(), 1)

    def test_once_per_invocation_and_slot_max_two(self):
        weekly.prepare(self.db, WATCHES, now=MON_DUE)
        self.assertIsNotNone(weekly.claim(self.db, 100))
        self.assertIsNone(weekly.claim(self.db, 100))  # disarmed
        weekly.prepare(self.db, WATCHES, now=MON_DUE)  # same slot, new run
        self.assertIsNone(weekly.claim(self.db, 100))
        weekly.prepare(self.db, WATCHES, now=MON_DUE)
        self.assertIsNotNone(weekly.claim(self.db, 104))
        weekly.prepare(self.db, WATCHES, now=MON_DUE)
        self.assertIsNone(weekly.claim(self.db, 108))  # cap reached
        self.assertEqual(self.emits(), 2)

    def test_mark_stops_emits(self):
        weekly.prepare(self.db, WATCHES, now=MON_DUE)
        weekly.claim(self.db, 100)
        self.assertTrue(weekly.mark(self.db, "2026-W40"))
        self.assertFalse(weekly.mark(self.db, "2026-W40"))
        weekly.prepare(self.db, WATCHES, now=MON_DUE)
        self.assertIsNone(weekly.claim(self.db, 104))


class ScheduledRunTests(unittest.TestCase):
    def setUp(self):
        old = os.environ.get("HEALTHCHECKS_PING_URL")
        os.environ["HEALTHCHECKS_PING_URL"] = FAKE_URL
        self.addCleanup(lambda: os.environ.__setitem__(
            "HEALTHCHECKS_PING_URL", old) if old is not None
            else os.environ.pop("HEALTHCHECKS_PING_URL", None))
        heartbeat.start_capture()
        self.addCleanup(heartbeat.stop_capture)
        self.db = fresh_db(self)
        self.guard = BudgetGuard(self.db)
        self.guard.reset_run()
        transport.install_transport_guard(self.guard)
        self.addCleanup(transport.uninstall_transport_guard)
        patcher = mock.patch.object(weekly, "report_week",
                                    return_value="2026-W40")
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(setattr, weekly, "_armed_week", None)

    def run_check(self, scheduled, fn=lambda *a, **k: 0):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            rc = fm_run.run_guarded("check", args_for("check", scheduled),
                                    self.db, WATCHES, self.guard, fn)
        return rc, out.getvalue()

    def test_noop_output_carries_report(self):
        store.claim_slot(self.db, store.slot_id(), "check")
        rc, out = self.run_check(True)
        self.assertEqual(rc, 0)
        doc = json.loads(out)
        self.assertEqual(doc["reason"], "slot_already_completed")
        rep = doc["handoff"]["weekly_report"]
        self.assertEqual(rep["week"], "2026-W40")
        self.assertIn("Weekly flight price report", rep["message"])
        self.assertIn("report-mark --week", doc["handoff"]["agent_instruction"])

    def test_stop_output_carries_report_without_alerts(self):
        def stop(*a, deadline_fn=None, **k):
            fm_run._check_deadline(deadline_fn)
        with mock.patch.object(fm_run, "RUN_DEADLINE_S", 0):
            rc, out = self.run_check(True, stop)
        self.assertEqual(rc, 0)
        doc = json.loads(out)
        self.assertEqual(doc["reason"], "deadline")
        self.assertEqual(doc["handoff"]["alerts"], [])
        self.assertEqual(doc["handoff"]["weekly_report"]["week"], "2026-W40")

    def test_repeat_runs_in_slot_do_not_resend(self):
        store.claim_slot(self.db, store.slot_id(), "check")
        _, out = self.run_check(True)
        self.assertIn("weekly_report", json.loads(out)["handoff"])
        for scheduled in (True, False):
            _, out = self.run_check(scheduled)
            self.assertNotIn("weekly_report", json.loads(out)["handoff"])

    def test_manual_run_does_not_prepare(self):
        store.claim_slot(self.db, store.slot_id(), "check")
        _, out = self.run_check(False)
        self.assertNotIn("weekly_report", json.loads(out)["handoff"])
        self.assertIsNone(self.db.execute(
            "SELECT 1 FROM weekly_reports").fetchone())


if __name__ == "__main__":
    unittest.main()
