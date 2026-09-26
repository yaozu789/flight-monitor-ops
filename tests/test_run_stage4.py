"""Stage-4 run.py orchestration tests (spec item 12).

Verifies the collection pipeline ordering in run.cmd_check without any
network: sentinel.collect and the poller are stubbed, the fake transport
guard is installed so any accidental real send would fail loudly.
"""
import io
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

    def test_same_slot_verify(self):
        """A deal collected in this run is verified and handed off in the
        SAME run, after collection -- not deferred to the next slot."""
        db, guard = _ctx(self)
        slot = store.slot_id()
        order = []

        def collect_writes_deal(db_, guard_, groups, slot_, run_start, **kw):
            order.append("collect")
            # Like the real collect(): narrow the per-run allowance to the
            # paced share and spend all of it.
            guard_.set_run_allowance(guard_.spent_run())
            store.add_observation(
                db_, watch_id="w1", origin="SFO", destination="JFK",
                depart_date="2026-11-01", return_date="2026-11-08",
                airline="UA", stops=0, cabin="economy", price_usd=420.0,
                booking_url="https://example.invalid/b")
            return {"attempts": 1, "bursts": 0, "sentinels": 1,
                    "background": 0, "drop_triggers": 0}

        def fake_verify(db_, watch, cand, guard_, **kw):
            order.append("verify")
            guard_.reserve("verify")  # real budget path, no network
            return True, {"price_usd": cand["price_usd"],
                          "depart_date": cand["depart_date"],
                          "return_date": cand["return_date"],
                          "airline": cand["airline"], "stops": cand["stops"],
                          "booking_url": cand["booking_url"],
                          "observed_at": store.utcnow_iso()}

        with mock.patch.object(fm_sentinel, "mint_sentinels", return_value=0), \
                mock.patch.object(fm_sentinel, "ensure_background_grid",
                                  return_value=0), \
                mock.patch.object(fm_sentinel, "collect", collect_writes_deal), \
                mock.patch.object(fm_sentinel, "detect_under_cap_triggers",
                                  return_value=0), \
                mock.patch.object(fm_run, "verify", fake_verify):
            args = SimpleNamespace(cmd="check", scheduled=False, watch="w1")
            rc = fm_run.cmd_check(args, db, [WATCH], guard)
        self.assertEqual(rc, 0)
        self.assertEqual(order, ["collect", "verify"])
        rows = db.execute("SELECT state, prepared_slot FROM outbox").fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["state"], "handoff_prepared")
        self.assertEqual(rows[0]["prepared_slot"], slot)

    def test_verified_deal_hands_off_when_budget_stops_run(self):
        """If the budget runs out mid-verify, deals already verified in this
        run are still handed off in this slot (not left pending)."""
        from budget import BudgetExhausted
        db, guard = _ctx(self)
        slot = store.slot_id()

        def collect_writes_two(db_, guard_, groups, slot_, run_start, **kw):
            for price, dep in ((400.0, "2026-11-01"), (450.0, "2026-12-01")):
                store.add_observation(
                    db_, watch_id="w1", origin="SFO", destination="JFK",
                    depart_date=dep, return_date=dep[:8] + "08",
                    airline="UA", stops=0, cabin="economy", price_usd=price,
                    booking_url="https://example.invalid/b")
            return {"attempts": 0, "bursts": 0, "sentinels": 0,
                    "background": 0, "drop_triggers": 0}

        calls = []

        def verify_then_exhaust(db_, watch, cand, guard_, **kw):
            calls.append(cand["price_usd"])
            if len(calls) > 1:
                raise BudgetExhausted("daily", "verify", 150, 150)
            return True, {"price_usd": cand["price_usd"],
                          "depart_date": cand["depart_date"],
                          "return_date": cand["return_date"],
                          "airline": cand["airline"], "stops": cand["stops"],
                          "booking_url": cand["booking_url"],
                          "observed_at": store.utcnow_iso()}

        with mock.patch.object(fm_sentinel, "mint_sentinels", return_value=0), \
                mock.patch.object(fm_sentinel, "ensure_background_grid",
                                  return_value=0), \
                mock.patch.object(fm_sentinel, "collect", collect_writes_two), \
                mock.patch.object(fm_run, "verify", verify_then_exhaust):
            args = SimpleNamespace(cmd="check", scheduled=False, watch="w1")
            with self.assertRaises(BudgetExhausted):
                fm_run.cmd_check(args, db, [WATCH], guard)
        rows = db.execute("SELECT state, prepared_slot, price_usd "
                          "FROM outbox").fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["price_usd"], 400.0)
        self.assertEqual(rows[0]["state"], "handoff_prepared")
        self.assertEqual(rows[0]["prepared_slot"], slot)

    def _run_with_stop(self, stop_exc, trigger_error=None, extra_patches=(),
                       seed=None):
        """Drive run_guarded(cmd_check) where the 2nd verification raises
        *stop_exc*; return (rc, parsed stdout JSON docs, db)."""
        import contextlib
        import io
        import json as _json
        db, guard = _ctx(self)
        if seed is not None:
            seed(db)

        def collect_writes_two(db_, guard_, groups, slot_, run_start, **kw):
            for price, dep in ((400.0, "2026-11-01"), (450.0, "2026-12-01")):
                store.add_observation(
                    db_, watch_id="w1", origin="SFO", destination="JFK",
                    depart_date=dep, return_date=dep[:8] + "08",
                    airline="UA", stops=0, cabin="economy", price_usd=price,
                    booking_url="https://example.invalid/b")
            return {"attempts": 0, "bursts": 0, "sentinels": 0,
                    "background": 0, "drop_triggers": 0}

        calls = []

        def verify_then_stop(db_, watch, cand, guard_, **kw):
            calls.append(1)
            if len(calls) > 1:
                raise stop_exc
            return True, {"price_usd": cand["price_usd"],
                          "depart_date": cand["depart_date"],
                          "return_date": cand["return_date"],
                          "airline": cand["airline"], "stops": cand["stops"],
                          "booking_url": cand["booking_url"],
                          "observed_at": store.utcnow_iso()}

        patches = [
            mock.patch.object(fm_sentinel, "mint_sentinels", return_value=0),
            mock.patch.object(fm_sentinel, "ensure_background_grid",
                              return_value=0),
            mock.patch.object(fm_sentinel, "collect", collect_writes_two),
            mock.patch.object(fm_run, "verify", verify_then_stop),
        ]
        if trigger_error is not None:
            patches.append(mock.patch.object(
                fm_sentinel, "detect_under_cap_triggers",
                side_effect=trigger_error))
        patches.extend(extra_patches)
        out = io.StringIO()
        with contextlib.ExitStack() as st:
            for p_ in patches:
                st.enter_context(p_)
            st.enter_context(contextlib.redirect_stdout(out))
            args = SimpleNamespace(cmd="check", scheduled=False, watch="w1")
            rc = fm_run.run_guarded("check", args, db, [WATCH], guard,
                                    fm_run.cmd_check)
        docs = [_json.loads(d) for d in
                out.getvalue().replace("}\n{", "}\x00{").split("\x00")
                if d.strip()]
        return rc, docs, db

    def _relayed_prices(self, docs):
        return [a["price_usd"] for d in docs
                for a in d.get("handoff", {}).get("alerts", [])]

    def test_stops_still_emit_verified_deal(self):
        """Budget, deadline and throttle stops mid-verify still PRINT the
        already-verified deal for the agent to relay (not just mark it
        handoff_prepared in the DB)."""
        from budget import BudgetExhausted
        from transport import ThrottleDetected
        for exc in (BudgetExhausted("daily", "verify", 150, 150),
                    store.DeadlineExceeded("run deadline exceeded"),
                    ThrottleDetected("http_429", 60, "test")):
            with self.subTest(stop=type(exc).__name__):
                rc, docs, db = self._run_with_stop(exc)
                self.assertEqual(rc, 0)
                self.assertEqual(self._relayed_prices(docs), [400.0])
                row = db.execute("SELECT state FROM outbox").fetchone()
                self.assertEqual(row["state"], "handoff_prepared")

    def test_trigger_failure_does_not_mask_stop(self):
        """A failing burst-trigger write during stop cleanup is recorded,
        the handoff is still prepared and emitted, and the original stop
        (budget) decides the outcome, not a crash."""
        from budget import BudgetExhausted
        rc, docs, db = self._run_with_stop(
            BudgetExhausted("daily", "verify", 150, 150),
            trigger_error=RuntimeError("trigger insert failed"))
        self.assertEqual(rc, 0)
        self.assertEqual(docs[-1]["reason"], "budget_exhausted")
        self.assertEqual(self._relayed_prices(docs), [400.0])
        h = store.latest_health(db, "burst_triggers")
        self.assertFalse(h["ok"])

    def test_retry_phase_stop_emits_drained(self):
        """A stop in the RETRY pass (before any new verification) still
        prints the alerts drained at the start of the run."""
        from budget import BudgetExhausted

        def seed(db):
            store.enqueue_alert(
                db, alert_id="w1:drained", watch_id="w1", fingerprint="fpd",
                price_usd=380.0, depart_date="2026-10-01",
                return_date="2026-10-08", airline="UA", stops=0,
                booking_url="https://example.invalid/b",
                message_text="drained deal", origin="SFO", destination="JFK")

        # A DAILY stop (a category cap alone no longer ends the run).
        rc, docs, db = self._run_with_stop(
            BudgetExhausted("daily", "retry_reserve", 150, 150), seed=seed,
            extra_patches=[mock.patch.object(
                fm_run, "recheck_retries",
                side_effect=BudgetExhausted("daily", "retry_reserve",
                                            150, 150))])
        self.assertEqual(rc, 0)
        self.assertEqual(docs[-1]["reason"], "budget_exhausted")
        self.assertEqual(self._relayed_prices(docs), [380.0])

    def test_cleanup_health_failure_does_not_mask_stop(self):
        """Trigger write AND its health note both fail during stop cleanup:
        the budget stop still decides the outcome and the deal is emitted."""
        from budget import BudgetExhausted
        real_add_health = fm_run.add_health

        def flaky_health(db_, kind, ok, detail):
            if kind == "burst_triggers":
                raise RuntimeError("health write failed")
            return real_add_health(db_, kind, ok, detail)

        rc, docs, db = self._run_with_stop(
            BudgetExhausted("daily", "verify", 150, 150),
            trigger_error=RuntimeError("trigger insert failed"),
            extra_patches=[mock.patch.object(fm_run, "add_health",
                                             side_effect=flaky_health)])
        self.assertEqual(rc, 0)
        self.assertEqual(docs[-1]["reason"], "budget_exhausted")
        self.assertEqual(self._relayed_prices(docs), [400.0])

    def test_failed_prepare_is_not_relayed(self):
        """If handoff preparation fails during stop cleanup, the still-
        pending row is NOT emitted for relay (the next drain handles it)."""
        from budget import BudgetExhausted
        rc, docs, db = self._run_with_stop(
            BudgetExhausted("daily", "verify", 150, 150),
            extra_patches=[mock.patch.object(
                fm_run, "prepare_handoff",
                side_effect=RuntimeError("prepare failed"))])
        self.assertEqual(rc, 0)
        self.assertEqual(self._relayed_prices(docs), [])
        row = db.execute("SELECT state FROM outbox").fetchone()
        self.assertEqual(row["state"], "pending")

    def test_retry_category_cap_does_not_stop_run(self):
        """The retry category's own cap ends only the retry pass: retries
        confirmed before it are handed off and collection still runs."""
        from budget import BudgetExhausted
        import outbox as fm_outbox
        db, guard = _ctx(self)
        slot = store.slot_id()
        for aid, fp in (("w1:r1", "fp1"), ("w1:r2", "fp2")):
            store.enqueue_alert(
                db, alert_id=aid, watch_id="w1", fingerprint=fp,
                price_usd=400.0, depart_date="2026-11-01",
                return_date="2026-11-08", airline="UA", stops=0,
                booking_url="", message_text="m", origin="SFO",
                destination="JFK")
            store.prepare_handoff(db, slot - 10, [aid])
        calls = []

        def recheck(row, db_, watch, guard_):
            calls.append(row["alert_id"])
            if len(calls) > 1:
                raise BudgetExhausted("category", "retry_reserve", 12, 12)
            return "confirmed", {"price_usd": 390.0,
                                 "depart_date": row["depart_date"],
                                 "return_date": row["return_date"],
                                 "airline": "UA", "stops": 0,
                                 "booking_url": "", "observed_at": "now",
                                 "origin": "SFO", "destination": "JFK"}

        collected = []
        with mock.patch.object(fm_outbox.verifier, "recheck_alert_detailed",
                               recheck), \
                mock.patch.object(fm_sentinel, "mint_sentinels", return_value=0), \
                mock.patch.object(fm_sentinel, "ensure_background_grid",
                                  return_value=0), \
                mock.patch.object(fm_sentinel, "collect",
                                  lambda *a, **k: collected.append(1) or {
                                      "attempts": 0, "bursts": [],
                                      "sentinels": 0, "background": 0,
                                      "drop_triggers": 0}):
            args = SimpleNamespace(cmd="check", scheduled=False, watch="w1")
            rc = fm_run.cmd_check(args, db, [WATCH], guard, slot=slot)
        self.assertEqual(rc, 0)
        self.assertEqual(collected, [1])
        first = store.outbox_get(db, calls[0])
        self.assertEqual(first["prepared_slot"], slot)

    def test_cmd_check_uses_claimed_slot(self):
        """cmd_check works under the slot run_guarded claimed, not a
        recomputed one (a run can cross a half-hour boundary)."""
        db, guard = _ctx(self)
        with mock.patch.object(fm_sentinel, "mint_sentinels", return_value=0), \
                mock.patch.object(fm_sentinel, "ensure_background_grid",
                                  return_value=0), \
                mock.patch.object(fm_sentinel, "collect",
                                  lambda *a, **k: {"attempts": 0, "bursts": [],
                                                   "sentinels": 0,
                                                   "background": 0,
                                                   "drop_triggers": 0}), \
                mock.patch.object(fm_run, "slot_id", return_value=999999):
            import io, json as _json, contextlib
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                fm_run.cmd_check(SimpleNamespace(cmd="check", scheduled=False,
                                                 watch="w1"),
                                 db, [WATCH], guard, slot=42)
        self.assertEqual(_json.loads(out.getvalue())["slot_id"], 42)
    def test_group_cfg_reads_watch_window(self):
        cfg = fm_run.group_cfg_for({"id": "w", "origins": ["SFO"],
                                    "destinations": ["NRT"],
                                    "window_months": [1, 4],
                                    "trip_length_days": [3, 8]})
        self.assertEqual(cfg["window_days"], (30, 120))
        self.assertEqual(cfg["durations"], (3, 8))
        default = fm_run.group_cfg_for({"id": "w", "origins": ["SFO"],
                                        "destinations": ["NRT"]})
        self.assertEqual(default["window_days"], (90, 270))
        self.assertEqual(default["durations"], (5, 14))

    def _stub_collection(self):
        return [mock.patch.object(fm_sentinel, "mint_sentinels", return_value=0),
                mock.patch.object(fm_sentinel, "ensure_background_grid",
                                  return_value=0),
                mock.patch.object(fm_sentinel, "collect",
                                  lambda *a, **k: {"attempts": 0, "bursts": [],
                                                   "sentinels": 0,
                                                   "background": 0,
                                                   "drop_triggers": 0}),
                mock.patch.object(fm_sentinel, "detect_under_cap_triggers",
                                  return_value=0)]

    def _run_check(self, db, guard, verify_fn):
        import contextlib
        with contextlib.ExitStack() as st:
            for p_ in self._stub_collection():
                st.enter_context(p_)
            st.enter_context(mock.patch.object(fm_run, "verify", verify_fn))
            st.enter_context(contextlib.redirect_stdout(io.StringIO()))
            return fm_run.cmd_check(
                SimpleNamespace(cmd="check", scheduled=False, watch="w1"),
                db, [WATCH], guard)

    def _obs(self, db, hours_ago, price=420.0, dep="2026-11-01"):
        from datetime import datetime, timedelta, timezone
        store.add_observation(
            db, watch_id="w1", origin="SFO", destination="JFK",
            depart_date=dep, return_date=dep[:8] + "08", airline="UA",
            stops=0, cabin="economy", price_usd=price, booking_url="",
            observed_at=(datetime.now(timezone.utc)
                         - timedelta(hours=hours_ago)).isoformat())

    def test_deferred_candidate_is_verified_later(self):
        """An under-cap observation from hours ago that no run managed to
        recheck (budget/deadline) is still picked up (audit #18)."""
        db, guard = _ctx(self)
        self._obs(db, hours_ago=5)
        calls = []

        def v(db_, w, c, g, **k):
            calls.append(c["fingerprint"])
            return False, None

        self._run_check(db, guard, v)
        self.assertEqual(len(calls), 1)

    def test_each_observation_verified_once(self):
        db, guard = _ctx(self)
        self._obs(db, hours_ago=5)
        calls = []

        def v(db_, w, c, g, **k):
            calls.append(c["observed_at"])
            return False, None

        self._run_check(db, guard, v)
        self._run_check(db, guard, v)
        self.assertEqual(len(calls), 1)          # not re-verified
        self._obs(db, hours_ago=0)               # fresh evidence
        self._run_check(db, guard, v)
        self.assertEqual(len(calls), 2)

    def test_verify_category_cap_does_not_stop_run(self):
        from budget import BudgetExhausted
        db, guard = _ctx(self)
        self._obs(db, hours_ago=0)  # inside the old and new windows alike
        collected = []

        def v(*a, **k):
            raise BudgetExhausted("category", "verify", 24, 24)

        import contextlib
        with contextlib.ExitStack() as st:
            for p_ in self._stub_collection()[:2] + self._stub_collection()[3:]:
                st.enter_context(p_)
            st.enter_context(mock.patch.object(
                fm_sentinel, "collect",
                lambda *a, **k: collected.append(1) or {
                    "attempts": 0, "bursts": [], "sentinels": 0,
                    "background": 0, "drop_triggers": 0}))
            st.enter_context(mock.patch.object(fm_run, "verify", v))
            st.enter_context(contextlib.redirect_stdout(io.StringIO()))
            rc = fm_run.cmd_check(
                SimpleNamespace(cmd="check", scheduled=False, watch="w1"),
                db, [WATCH], guard)
        self.assertEqual((rc, collected), (0, [1]))

    def test_failed_enqueue_leaves_observation_eligible(self):
        """If queuing the confirmed alert fails, the observation is NOT
        logged as verified, so a later run retries it."""
        db, guard = _ctx(self)
        self._obs(db, hours_ago=1)
        ok = lambda *a, **k: (True, {  # noqa: E731
            "price_usd": 400.0, "depart_date": "2026-11-01",
            "return_date": "2026-11-08", "airline": "UA", "stops": 0,
            "booking_url": "", "observed_at": "now",
            "origin": "SFO", "destination": "JFK"})
        with mock.patch.object(fm_run, "enqueue_from_confirmation",
                               side_effect=RuntimeError("disk full")):
            with self.assertRaises(RuntimeError):
                self._run_check(db, guard, ok)
        self.assertEqual(db.execute(
            "SELECT COUNT(*) AS c FROM verify_log").fetchone()["c"], 0)
        self._run_check(db, guard, ok)
        self.assertEqual(db.execute(
            "SELECT COUNT(*) AS c FROM outbox").fetchone()["c"], 1)

    def test_backlog_leaves_verify_reserve_for_bursts(self):
        db, guard = _ctx(self)
        for i in range(3):
            self._obs(db, hours_ago=2 + i, dep=f"2026-11-0{i + 1}")
        for _ in range(19):  # 19 + 2 > 24 - 4: backlog must stop
            guard.reserve("verify")
        calls = []
        self._run_check(db, guard,
                        lambda *a, **k: calls.append(1) or (False, None))
        self.assertEqual(calls, [])

    def test_verify_log_is_per_watch(self):
        store.init_db  # noqa: B018 - module import check
        db, _ = _ctx(self)
        store.log_verify(db, "w1", "fp", "2026-09-25T00:00:00+00:00",
                         "not_confirmed")
        self.assertTrue(store.verify_logged(db, "w1", "fp",
                                            "2026-09-25T00:00:00+00:00"))
        self.assertFalse(store.verify_logged(db, "w2", "fp",
                                             "2026-09-25T00:00:00+00:00"))

    def test_tied_observations_score_once(self):
        """Rows of one fingerprint sharing an observed_at (collection stamps
        a whole run with run_start) yield ONE candidate, not duplicates."""
        db, _ = _ctx(self)
        ts = store.utcnow_iso()
        for _ in range(2):
            store.add_observation(
                db, watch_id="w1", origin="SFO", destination="JFK",
                depart_date="2026-11-01", return_date="2026-11-08",
                airline="UA", stops=0, cabin="economy", price_usd=400.0,
                booking_url="https://example.invalid/b", observed_at=ts)
        rows = store.latest_observations_since(db, "w1", ts)
        self.assertEqual(len(rows), 1)

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
                        def fake_retries(*a, out=None, **k):
                            out["retry_ids"].append("w1:retry")
                            return out

                        with mock.patch.object(
                                fm_run, "recheck_retries",
                                side_effect=fake_retries):
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
