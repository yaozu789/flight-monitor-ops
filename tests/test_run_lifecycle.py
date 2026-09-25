"""Run-lifecycle tests (v3 group 3): drain-first, run deadline, heartbeat gaps.

- run_guarded drains pending alerts BEFORE the backoff check and the slot
  claim, so stranded handoffs are prepared even when collection is skipped.
- A run exceeding RUN_DEADLINE_S stops in a controlled way:
  finish_slot("deadline"), health note, lock released, success ping --
  never /fail.
- Heartbeat gaps closed: SystemExit pings /fail then re-raises; a non-zero
  fn return pings /fail instead of success.

All URLs are fake (example.invalid); heartbeat._send is captured, so no
real network happens. No flight searches are performed.
"""
import os
import sys
import time
import unittest
from pathlib import Path as _Path
from types import SimpleNamespace
from unittest import mock

_TESTS_DIR = _Path(__file__).resolve().parent
_PROJECT_ROOT = _TESTS_DIR.parent
sys.path.insert(0, str(_TESTS_DIR))
sys.path.insert(0, str(_PROJECT_ROOT))

import store  # noqa: E402
import heartbeat  # noqa: E402
import transport  # noqa: E402
from budget import BudgetGuard  # noqa: E402

import importlib.util as _ilu

def _load_run():
    spec = _ilu.spec_from_file_location("fm_run_lifecycle",
                                        str(_PROJECT_ROOT / "run.py"))
    mod = _ilu.module_from_spec(spec)
    sys.modules["fm_run_lifecycle"] = mod
    spec.loader.exec_module(mod)
    return mod

fm_run = _load_run()

FAKE_URL = "https://example.invalid/hc/fake-uuid-for-tests"


def fresh_db(testcase):
    import tempfile
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    db = store.init_db(path)
    testcase.addCleanup(lambda: (db.close(), os.path.exists(path) and os.unlink(path)))
    return db


def args_for(cmd, scheduled=False):
    return SimpleNamespace(cmd=cmd, scheduled=scheduled, watch=None,
                          duration=7, depart="2026-10-01", return_="2026-10-08")


WATCH = {"id": "w1", "origins": ["SFO"], "destinations": ["JFK"],
         "absolute_cap_usd": 500.0, "cooldown_hours": 24, "cabin": "economy"}


def seed_pending(db, alert_id="w1:drain1"):
    store.enqueue_alert(
        db, alert_id=alert_id, watch_id="w1", fingerprint="fp-drain",
        price_usd=400.0, depart_date="2026-11-01", return_date="2026-11-08",
        airline="UA", stops=0, booking_url="https://example.invalid/b",
        message_text="drain me", origin="SFO", destination="JFK")


class RunLifecycleTests(unittest.TestCase):
    def setUp(self):
        self._old = os.environ.get("HEALTHCHECKS_PING_URL")
        os.environ["HEALTHCHECKS_PING_URL"] = FAKE_URL
        self.captured = heartbeat.start_capture()
        self.addCleanup(heartbeat.stop_capture)
        self.addCleanup(self._restore_env)

    def _restore_env(self):
        if self._old is None:
            os.environ.pop("HEALTHCHECKS_PING_URL", None)
        else:
            os.environ["HEALTHCHECKS_PING_URL"] = self._old

    def _ctx(self):
        db = fresh_db(self)
        store.sync_watches(db, [WATCH], {})
        guard = BudgetGuard(db)
        guard.reset_run()
        transport.uninstall_transport_guard()
        transport.install_transport_guard(guard)
        self.addCleanup(transport.uninstall_transport_guard)
        return db, guard

    def test_drain_runs_before_backoff_noop(self):
        """Persisted backoff short-circuits collection, but the drain
        already ran: the stranded pending row is handoff_prepared."""
        db, guard = self._ctx()
        seed_pending(db)
        store.set_backoff(db, "http429", time.time() + 3600, 60,
                          "test backoff")
        ran = []
        rc = fm_run.run_guarded("check", args_for("check", False), db,
                                [WATCH], guard,
                                lambda *a, **k: ran.append(1) or 0)
        self.assertEqual(rc, 0)
        self.assertEqual(ran, [])  # fn never ran: backoff no-op
        row = store.outbox_get(db, "w1:drain1")
        self.assertEqual(row["state"], "handoff_prepared")

    def test_drain_runs_before_slot_noop(self):
        """An already-claimed slot short-circuits collection, but the
        drain already ran."""
        db, guard = self._ctx()
        seed_pending(db)
        self.assertTrue(store.claim_slot(db, store.slot_id(), "check"))
        ran = []
        rc = fm_run.run_guarded("check", args_for("check", False), db,
                                [WATCH], guard,
                                lambda *a, **k: ran.append(1) or 0)
        self.assertEqual(rc, 0)
        self.assertEqual(ran, [])  # fn never ran: slot no-op
        row = store.outbox_get(db, "w1:drain1")
        self.assertEqual(row["state"], "handoff_prepared")

    def test_deadline_controlled_stop(self):
        """Tiny deadline: fn hits it at a loop top -> finish_slot
        ("deadline"), health note, lock released, success ping, never
        /fail."""
        db, guard = self._ctx()

        def slow_fn(args, db_, watches, guard_, drained=None,
                     deadline_fn=None):
            fm_run._check_deadline(deadline_fn)  # raises immediately
            return 0  # pragma: no cover

        with mock.patch.object(fm_run, "RUN_DEADLINE_S", 0):
            rc = fm_run.run_guarded("check", args_for("check", True), db,
                                    [WATCH], guard, slow_fn)
        self.assertEqual(rc, 0)
        slot = store.slot_id()
        row = db.execute("SELECT status FROM run_slots WHERE slot_id=?",
                         (slot,)).fetchone()
        self.assertEqual(row["status"], "deadline")
        # Lock released by the finally.
        self.assertIsNone(store.lock_holder(db))
        # Health note recorded.
        h = store.latest_health(db, "run_deadline")
        self.assertIsNotNone(h)
        self.assertTrue(h["ok"])
        # Success ping, never /fail.
        kinds = [k for k, _ in self.captured]
        self.assertEqual(kinds, ["success"])

    def test_deadline_shorter_than_lock_ttl(self):
        """The deadline must stay below the lock TTL so a stopped run
        never wedges the lock for the next slot."""
        self.assertLess(fm_run.RUN_DEADLINE_S, fm_run.LOCK_TTL_S)

    def test_systemexit_pings_fail_then_reraises(self):
        """SystemExit from fn: /fail ping, then the exception propagates."""
        db, guard = self._ctx()

        def sysexit_fn(*a, **k):
            raise SystemExit(3)

        with self.assertRaises(SystemExit):
            fm_run.run_guarded("check", args_for("check", True), db,
                               [WATCH], guard, sysexit_fn)
        kinds = [k for k, _ in self.captured]
        self.assertEqual(kinds, ["fail"])
        self.assertIn("/fail", self.captured[0][1])

    def test_nonzero_return_pings_fail(self):
        """fn returning non-zero without raising: /fail ping, rc kept."""
        db, guard = self._ctx()
        rc = fm_run.run_guarded("check", args_for("check", True), db,
                                [WATCH], guard, lambda *a, **k: 2)
        self.assertEqual(rc, 2)
        kinds = [k for k, _ in self.captured]
        self.assertEqual(kinds, ["fail"])
        self.assertIn("/fail", self.captured[0][1])

    def test_zero_return_still_pings_success(self):
        """Sanity: a clean fn return keeps the success ping."""
        db, guard = self._ctx()
        rc = fm_run.run_guarded("check", args_for("check", True), db,
                                [WATCH], guard, lambda *a, **k: 0)
        self.assertEqual(rc, 0)
        kinds = [k for k, _ in self.captured]
        self.assertEqual(kinds, ["success"])


class PipelineOrderTests(unittest.TestCase):
    """v3 group 4: verify -> sentinels -> bursts -> background through the
    REAL sentinel.collect (collect itself is never stubbed)."""

    def setUp(self):
        self.db = fresh_db(self)
        store.sync_watches(self.db, [WATCH], {})
        self.guard = BudgetGuard(self.db)
        self.guard.reset_run()
        transport.uninstall_transport_guard()
        transport.install_transport_guard(self.guard)
        self.addCleanup(transport.uninstall_transport_guard)
        self._patched = []
        self.addCleanup(self._unpatch)

    def _unpatch(self):
        for obj, name, orig in self._patched:
            setattr(obj, name, orig)

    def _patch(self, obj, name, new):
        self._patched.append((obj, name, getattr(obj, name)))
        setattr(obj, name, new)

    def test_verify_sentinels_bursts_background_order(self):
        from datetime import date as _date, timedelta as _td
        events = []

        # Seed: one observation from the "previous slot's collection".
        now_iso = store.utcnow_iso()
        depart = (_date.fromisoformat("2026-10-01")).isoformat()
        ret = (_date.fromisoformat("2026-10-08")).isoformat()
        self.db.execute(
            """INSERT INTO observations
               (watch_id, fingerprint, origin, destination, depart_date,
                return_date, airline, stops, cabin, price_usd, booking_url,
                observed_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            ("w1", "fp-pipe", "SFO", "JFK", depart, ret, "UA", 0,
             "economy", 400.0, "https://example.invalid/b", now_iso))
        self.db.commit()

        # Seed: one due trigger + one sentinel cell + one background cell.
        trig_depart = (_date.fromisoformat("2026-10-01")
                       + _td(days=100)).isoformat()
        store.queue_trigger(
            self.db, kind="under_cap", group_id="w1",
            depart_week=store.depart_week(trig_depart), depart_date=trig_depart,
            duration=7, price_usd=400.0, fingerprint="fp-pipe-trig", slot=1)
        store.ensure_cells(self.db, [
            {"group_id": "w1", "depart_date": trig_depart, "duration": 7,
             "role": "sentinel", "weekday": 0},
            {"group_id": "w1", "depart_date": trig_depart, "duration": 8,
             "role": "background", "weekday": 0},
        ])
        groups_cfg = {"w1": {"origins": ["SFO"], "destinations": ["JFK"]}}

        def fake_verify(db, watch, candidate, guard, **kw):
            events.append("verify_phase")
            return True, {"price_usd": 400.0, "depart_date": depart,
                          "return_date": ret, "airline": "UA", "stops": 0,
                          "booking_url": "https://example.invalid/b",
                          "observed_at": now_iso}

        import sentinel as _sentinel

        def fake_check(db, cell, guard, group_id, group_cfg, *, top_n,
                       category, slot, run_start=None):
            events.append(category)
            return (400.0, 2)

        self._patch(fm_run, "verify", fake_verify)
        self._patch(_sentinel, "check_cell", fake_check)
        self._patch(_sentinel, "per_run_allowance",
                    lambda guard, slot: 200)

        rc = fm_run.cmd_check(args_for("check", False), self.db, [WATCH],
                              self.guard)
        self.assertEqual(rc, 0)

        # v3 order: verify first, then sentinels, then bursts (whose
        # revalidate step bills "verify"), then background.
        self.assertGreater(len(events), 4)
        self.assertEqual(events[0], "verify_phase")
        first_sentinel = events.index("sentinel")
        first_burst = events.index("burst")
        first_background = events.index("background")
        # The burst revalidate ("verify") is part of the burst phase: it
        # must come after the sentinels and before the background.
        revalidate_at = events.index("verify", 1)
        self.assertLess(first_sentinel, revalidate_at)
        self.assertLess(revalidate_at, first_burst)
        self.assertLess(first_burst, first_background)
        for i, e in enumerate(events):
            if e == "sentinel":
                self.assertLess(i, first_burst)
            elif e == "background":
                self.assertGreater(i, first_burst)
        # The seeded candidate was actually verified and enqueued.
        row = store.outbox_get(self.db, "w1:" + store.depart_week(depart))
        self.assertIsNotNone(row)


if __name__ == "__main__":
    unittest.main()
