"""Run-level heartbeat semantics (spec item 11).

Verifies run.py's wiring, not heartbeat.py's HTTP layer (covered by
test_ping.py):
- scheduled check/broad pings success ONLY after the cycle finishes,
  including legitimate budget/backoff/slot no-ops;
- crashes and 3x lock contention ping /fail;
- manual commands never ping;
- a --scheduled run with HEALTHCHECKS_PING_URL unset/empty refuses to
  start (exit 2) and attempts no ping.

All URLs are fake (example.invalid); heartbeat._send is captured, so no
real network happens.
"""
import os
import sys
import unittest
from pathlib import Path as _Path
from types import SimpleNamespace

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
    # Load the project's run.py by explicit path (as fm_run) so the test
    # exercises exactly the CLI wiring under test.
    spec = _ilu.spec_from_file_location("fm_run", str(_PROJECT_ROOT / "run.py"))
    mod = _ilu.module_from_spec(spec)
    sys.modules["fm_run"] = mod
    spec.loader.exec_module(mod)
    return mod

fm_run = _load_run()

FAKE_URL = "https://example.invalid/hc/fake-uuid-for-tests"


def fresh_db(testcase):
    import sqlite3
    import tempfile
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    db = store.init_db(path)
    testcase.addCleanup(lambda: (db.close(), os.path.exists(path) and os.unlink(path)))
    return db


def args_for(cmd, scheduled=False):
    return SimpleNamespace(cmd=cmd, scheduled=scheduled, watch=None,
                          duration=7, depart="2026-10-01", return_="2026-10-08")


def ok_fn(args, db, watches, guard, **kwargs):
    return 0


class RunHeartbeatTests(unittest.TestCase):
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

    def _ctx(self, testcase):
        db = fresh_db(testcase)
        guard = BudgetGuard(db)
        guard.reset_run()
        transport.uninstall_transport_guard()
        transport.install_transport_guard(guard)
        self.addCleanup(transport.uninstall_transport_guard)
        return db, guard

    def test_scheduled_success_pings(self):
        db, guard = self._ctx(self)
        rc = fm_run.run_guarded("check", args_for("check", True), db, [],
                                guard, ok_fn)
        self.assertEqual(rc, 0)
        self.assertEqual(len(self.captured), 1)
        kind, url = self.captured[0]
        self.assertEqual(kind, "success")
        self.assertIn("fake-uuid-for-tests", url)
        self.assertNotIn("/fail", url)

    def test_scheduled_slot_noop_still_pings_success(self):
        db, guard = self._ctx(self)
        # First run claims the slot; second is a legitimate no-op.
        fm_run.run_guarded("check", args_for("check", True), db, [], guard, ok_fn)
        self.captured.clear()
        rc = fm_run.run_guarded("broad", args_for("broad", True), db, [],
                                guard, ok_fn)
        self.assertEqual(rc, 0)
        self.assertEqual(len(self.captured), 1)
        kind, url = self.captured[0]
        self.assertEqual(kind, "success")
        self.assertNotIn("/fail", url)

    def test_scheduled_crash_pings_fail(self):
        db, guard = self._ctx(self)

        def boom(args, db, watches, guard, **kwargs):
            raise RuntimeError("simulated crash")

        rc = fm_run.run_guarded("check", args_for("check", True), db, [],
                                guard, boom)
        self.assertEqual(rc, 1)
        self.assertEqual(len(self.captured), 1)
        kind, url = self.captured[0]
        self.assertEqual(kind, "fail")
        self.assertIn("/fail", url)

    def test_manual_never_pings(self):
        db, guard = self._ctx(self)
        rc = fm_run.run_manual("narrow", args_for("narrow", False), db, [],
                               guard, ok_fn)
        self.assertEqual(rc, 0)
        self.assertEqual(self.captured, [])

    def test_unscheduled_check_never_pings(self):
        db, guard = self._ctx(self)
        rc = fm_run.run_guarded("check", args_for("check", False), db, [],
                                guard, ok_fn)
        self.assertEqual(rc, 0)
        self.assertEqual(self.captured, [])

    def test_missing_env_refuses_scheduled_run(self):
        db, guard = self._ctx(self)
        os.environ.pop("HEALTHCHECKS_PING_URL", None)
        rc = fm_run.run_guarded("check", args_for("check", True), db, [],
                                guard, ok_fn)
        self.assertEqual(rc, 2)
        self.assertEqual(self.captured, [])

    def test_empty_env_refuses_scheduled_run(self):
        db, guard = self._ctx(self)
        os.environ["HEALTHCHECKS_PING_URL"] = ""
        rc = fm_run.run_guarded("check", args_for("check", True), db, [],
                                guard, ok_fn)
        self.assertEqual(rc, 2)
        self.assertEqual(self.captured, [])

    def test_three_lock_losses_ping_fail(self):
        db, guard = self._ctx(self)
        # Hold the lock from the outside so every run loses contention.
        self.assertTrue(store.acquire_lock(db, "outsider", ttl_s=1500))
        try:
            for i in range(2):
                rc = fm_run.run_guarded("check", args_for("check", True), db,
                                        [], guard, ok_fn)
                self.assertEqual(rc, 0)  # quiet no-op, no ping yet
                self.assertEqual(self.captured, [])
            rc = fm_run.run_guarded("check", args_for("check", True), db,
                                    [], guard, ok_fn)
            self.assertEqual(rc, 1)
            self.assertEqual(len(self.captured), 1)
            kind, url = self.captured[0]
            self.assertEqual(kind, "fail")
            self.assertIn("/fail", url)
        finally:
            store.release_lock(db, "outsider")


if __name__ == "__main__":
    unittest.main()
