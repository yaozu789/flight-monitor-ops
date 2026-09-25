"""Stage-4 run.py orchestration tests (spec item 12).

Verifies the collection pipeline ordering in run.cmd_check without any
network: sentinel.collect and the poller are stubbed, the fake transport
guard is installed so any accidental real send would fail loudly.
"""
import os
import sys
import unittest
from pathlib import Path as _Path
from types import SimpleNamespace
from unittest import mock

_TESTS_DIR = _Path(__file__).resolve().parent
_PROJECT_ROOT = _TESTS_DIR.parent
sys.path.insert(0, str(_TESTS_DIR))
sys.path.insert(0, str(_PROJECT_ROOT))

import store  # noqa: E402
import transport  # noqa: E402
from budget import BudgetGuard  # noqa: E402

import importlib.util as _ilu

def _load_run():
    # Load the project's run.py by explicit path (as fm_run4) so the test
    # exercises exactly the CLI wiring under test.
    spec = _ilu.spec_from_file_location("fm_run4", str(_PROJECT_ROOT / "run.py"))
    mod = _ilu.module_from_spec(spec)
    sys.modules["fm_run4"] = mod
    spec.loader.exec_module(mod)
    return mod

fm_run = _load_run()
import sentinel as fm_sentinel  # noqa: E402


def fresh_db(testcase):
    import tempfile
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    db = store.init_db(path)
    testcase.addCleanup(lambda: (db.close(), os.path.exists(path) and os.unlink(path)))
    return db


WATCH = {"id": "w1", "origins": ["SFO"], "destinations": ["JFK"],
         "absolute_cap_usd": 500.0, "cooldown_hours": 24, "cabin": "economy"}


def _ctx(testcase):
    db = fresh_db(testcase)
    store.sync_watches(db, [WATCH], {})
    guard = BudgetGuard(db)
    guard.reset_run()
    transport.uninstall_transport_guard()
    transport.install_transport_guard(guard)
    testcase.addCleanup(transport.uninstall_transport_guard)
    return db, guard


class Stage4OrchestrationTests(unittest.TestCase):
    def test_drain_before_collect(self):
        """Pending handoffs are prepared BEFORE sentinel.collect runs."""
        db, guard = _ctx(self)
        order = []
        aid, _ = store.enqueue_alert(
            db, alert_id="w1:fp1", watch_id="w1", fingerprint="fp1",
            price_usd=400.0, depart_date="2026-11-01", return_date="2026-11-08",
            airline="UA", stops=0, booking_url="https://example.invalid/b",
            message_text="deal", origin="SFO", destination="JFK")

        orig_collect = fm_sentinel.collect
        def spy_collect(db_, guard_, groups, slot, run_start, **kwargs):
            order.append("collect")
            row = store.outbox_get(db_, aid)
            order.append(f"drain_state={row['state']}")
            return {"attempts": 0, "bursts": 0, "sentinels": 0,
                    "background": 0, "drop_triggers": 0}
        with mock.patch.object(fm_sentinel, "collect", spy_collect):
            with mock.patch.object(fm_sentinel, "mint_sentinels", return_value=0):
                with mock.patch.object(fm_sentinel, "ensure_background_grid",
                                        return_value=0):
                    with mock.patch.object(fm_sentinel,
                                            "detect_under_cap_triggers",
                                            return_value=0):
                        args = SimpleNamespace(cmd="check", scheduled=False,
                                               watch="w1")
                        rc = fm_run.cmd_check(args, db, [WATCH], guard)
        self.assertEqual(rc, 0)
        # drain ran first: by the time collect ran, the row was already
        # handoff_prepared.
        self.assertEqual(order[0], "collect")
        self.assertEqual(order[1], "drain_state=handoff_prepared")

    def test_three_handoffs_one_slot(self):
        """Drained + retried + new handoffs can all prepare in one slot."""
        db, guard = _ctx(self)
        slot = store.slot_id()
        # one pending (to drain)
        store.enqueue_alert(
            db, alert_id="w1:drain", watch_id="w1", fingerprint="fpd",
            price_usd=400.0, depart_date="2026-11-01", return_date="2026-11-08",
            airline="UA", stops=0, booking_url="https://example.invalid/b",
            message_text="drain", origin="SFO", destination="JFK")
        # one prepared long ago (to retry)
        store.enqueue_alert(
            db, alert_id="w1:retry", watch_id="w1", fingerprint="fpr",
            price_usd=410.0, depart_date="2026-11-01", return_date="2026-11-08",
            airline="UA", stops=0, booking_url="https://example.invalid/b",
            message_text="retry", origin="SFO", destination="JFK")
        store.prepare_handoff(db, slot - 10, ["w1:retry"])
        # one new pending (simulating a fresh enqueue)
        store.enqueue_alert(
            db, alert_id="w1:new", watch_id="w1", fingerprint="fpn",
            price_usd=420.0, depart_date="2026-11-01", return_date="2026-11-08",
            airline="UA", stops=0, booking_url="https://example.invalid/b",
            message_text="new", origin="SFO", destination="JFK")

        with mock.patch.object(fm_sentinel, "mint_sentinels", return_value=0):
            with mock.patch.object(fm_sentinel, "ensure_background_grid",
                                    return_value=0):
                with mock.patch.object(
                        fm_sentinel, "collect",
                        return_value={"attempts": 0, "bursts": 0, "sentinels": 0,
                                      "background": 0, "drop_triggers": 0}):
                    with mock.patch.object(fm_sentinel,
                                            "detect_under_cap_triggers",
                                            return_value=0):
                        # stub the retry recheck to confirm the retry
                        with mock.patch.object(
                                fm_run, "recheck_retries",
                                return_value={"retry_ids": ["w1:retry"],
                                              "retired": [],
                                              "unknown": []}):
                            args = SimpleNamespace(cmd="check", scheduled=False,
                                                   watch="w1")
                            rc = fm_run.cmd_check(args, db, [WATCH], guard)
        self.assertEqual(rc, 0)
        for aid in ("w1:drain", "w1:retry", "w1:new"):
            row = store.outbox_get(db, aid)
            self.assertEqual(row["state"], "handoff_prepared", aid)
            self.assertEqual(row["prepared_slot"], slot, aid)

    def test_slot_claimed_before_collect(self):
        """run_guarded claims the slot before fn runs (no send before claim)."""
        db, guard = _ctx(self)
        seen = {}

        def probe(args, db_, watches, guard_, **kwargs):
            seen["slot_done"] = store.slot_done(db_, store.slot_id())
            return 0

        args = SimpleNamespace(cmd="check", scheduled=False, watch=None)
        rc = fm_run.run_guarded("check", args, db, [WATCH], guard, probe)
        self.assertEqual(rc, 0)
        self.assertTrue(seen["slot_done"])

    def test_check_and_broad_share_slot(self):
        """A second collecting command in the same slot no-ops."""
        db, guard = _ctx(self)
        args = SimpleNamespace(cmd="check", scheduled=False, watch=None)
        rc1 = fm_run.run_guarded("check", args, db, [WATCH], guard,
                                 lambda *a, **k: 0)
        self.assertEqual(rc1, 0)
        # broad in the same slot: slot already claimed -> noop, fn not run
        ran = []
        rc2 = fm_run.run_guarded(
            "broad", SimpleNamespace(cmd="broad", scheduled=False, watch="w1",
                                     duration=7),
            db, [WATCH], guard, lambda *a, **k: ran.append(1) or 0)
        self.assertEqual(rc2, 0)
        self.assertEqual(ran, [])

    def test_all_collecting_commands_share_lock(self):
        """narrow/canary take the same DB lock as check/broad."""
        db, guard = _ctx(self)
        self.assertTrue(store.acquire_lock(db, "outsider", ttl_s=1500))
        try:
            for cmd, fn in [("narrow", fm_run.cmd_narrow),
                            ("canary", fm_run.cmd_canary)]:
                args = SimpleNamespace(cmd=cmd, scheduled=False, watch="w1",
                                       depart="2026-11-01", return_="2026-11-08")
                rc = fm_run.run_manual(cmd, args, db, [WATCH], guard, fn)
                self.assertEqual(rc, 0)  # overlap_lock no-op, not a crash
        finally:
            store.release_lock(db, "outsider")


if __name__ == "__main__":
    unittest.main()
