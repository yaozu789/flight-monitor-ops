"""Stall detection (issue #29): runs that collect nothing although a check
was affordable are flagged, and a streak of them fails the heartbeat
instead of pinging green. Real cmd_check / sentinel.collect / BudgetGuard;
no network (cells are made undue, so nothing is ever sent)."""
import contextlib
import io
import os
import sys
import unittest
from pathlib import Path
from unittest import mock

_TESTS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(_TESTS_DIR))
sys.path.insert(0, str(_TESTS_DIR.parent))

import heartbeat  # noqa: E402
import store  # noqa: E402
import transport  # noqa: E402
from budget import BudgetGuard  # noqa: E402
from test_run_lifecycle import FAKE_URL, WATCH, args_for, fm_run  # noqa: E402


def spend(db, category, n):
    for _ in range(n):
        store.ledger_insert(db, store.today_str(), category, 0)
    db.commit()


class StalledRuleTests(unittest.TestCase):
    def setUp(self):
        import tempfile
        fd, path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        self.db = store.init_db(path)
        self.addCleanup(lambda: (self.db.close(), os.unlink(path)))
        self.guard = BudgetGuard(self.db)

    def stalled(self, attempts=0, allowance=10):
        return fm_run._stalled(self.guard, {"attempts": attempts,
                                            "allowance": allowance})

    def test_rules(self):
        self.assertFalse(self.stalled(attempts=2))
        self.assertTrue(self.stalled())
        self.assertFalse(self.stalled(allowance=1))  # nothing affordable

    def test_sentinel_capped_background_open_is_a_stall(self):
        """The 17b8e82 shape: sentinel cap spent, background untouched."""
        spend(self.db, "sentinel", 54)
        self.assertTrue(self.stalled(allowance=4))
        # A background check costs 4: a 3-attempt allowance affords nothing.
        self.assertFalse(self.stalled(allowance=3))

    def test_both_caps_spent_is_legitimate(self):
        spend(self.db, "sentinel", 54)
        spend(self.db, "background", 48)
        self.assertFalse(self.stalled())


class StallStreakTests(unittest.TestCase):
    def setUp(self):
        old = os.environ.get("HEALTHCHECKS_PING_URL")
        os.environ["HEALTHCHECKS_PING_URL"] = FAKE_URL
        self.addCleanup(lambda: os.environ.__setitem__(
            "HEALTHCHECKS_PING_URL", old) if old is not None
            else os.environ.pop("HEALTHCHECKS_PING_URL", None))
        self.captured = heartbeat.start_capture()
        self.addCleanup(heartbeat.stop_capture)
        import tempfile
        fd, path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        self.db = store.init_db(path)
        self.addCleanup(lambda: (self.db.close(), os.unlink(path)))
        store.sync_watches(self.db, [WATCH], {})
        # A scheduler bug: no cell is ever picked up.
        patcher = mock.patch.object(store, "due_cells", return_value=[])
        patcher.start()
        self.addCleanup(patcher.stop)

    def run_at(self, slot):
        guard = BudgetGuard(self.db)
        guard.reset_run()
        transport.uninstall_transport_guard()
        transport.install_transport_guard(guard)
        self.addCleanup(transport.uninstall_transport_guard)
        del self.captured[:]
        with mock.patch.object(fm_run, "slot_id", return_value=slot), \
                contextlib.redirect_stdout(io.StringIO()):
            return fm_run.run_guarded("check", args_for("check", True),
                                      self.db, [WATCH], guard,
                                      fm_run.cmd_check)

    def test_third_idle_run_pings_fail(self):
        base = store.slot_id()
        for i in range(2):
            self.assertEqual(self.run_at(base + i), 0)
            self.assertEqual([k for k, _ in self.captured], ["success"])
        self.assertEqual(self.run_at(base + 2), 1)
        self.assertEqual([k for k, _ in self.captured], ["fail"])
        h = store.latest_health(self.db, "collection_stall")
        self.assertFalse(h["ok"])
        row = self.db.execute("SELECT attempts, stalled FROM run_slots "
                              "WHERE slot_id = ?", (base + 2,)).fetchone()
        self.assertEqual((row["attempts"], row["stalled"]), (0, 1))

    def test_neutral_runs_neither_count_nor_break(self):
        base = store.slot_id()
        for i, (attempts, stalled) in enumerate(
                [(0, 1), (0, 0), (0, 1), (0, 0), (0, 1)]):
            store.claim_slot(self.db, base - 10 + i, "check")
            store.record_run_stats(self.db, base - 10 + i, attempts, stalled)
        self.assertEqual(store.stall_streak(self.db), 3)
        store.claim_slot(self.db, base - 4, "check")
        store.record_run_stats(self.db, base - 4, 6, False)  # progress
        self.assertEqual(store.stall_streak(self.db), 0)

    def test_skipped_run_breaks_streak(self):
        base = store.slot_id()
        self.run_at(base)
        self.run_at(base + 1)
        self.assertEqual(self.run_at(base + 3), 0)  # a run was missed


if __name__ == "__main__":
    unittest.main()
