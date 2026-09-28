"""Collect-only mode and the released budget (redesign PR A2).

A watch with ``alert_mode: collect`` stores prices and nothing else: no
scoring, verification, retries, drop/burst triggers or handoffs, and
entering the mode retires alerts and bursts queued under the old cap rule
BEFORE the drain. While every watch collects, the verify reserve and the
alert-path category caps are released to sentinels, and the guard and the
pacer agree on the headroom. Offline: no network, temp file DBs.
"""
from __future__ import annotations

import contextlib
import io
import json
import os
import sys
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

import budget  # noqa: E402
import store  # noqa: E402
import transport  # noqa: E402
from budget import BudgetExhausted, BudgetGuard  # noqa: E402

import importlib.util as _ilu


def _load_run():
    spec = _ilu.spec_from_file_location("fm_run_collect",
                                        str(_PROJECT_ROOT / "run.py"))
    mod = _ilu.module_from_spec(spec)
    sys.modules["fm_run_collect"] = mod
    spec.loader.exec_module(mod)
    return mod


fm_run = _load_run()
import sentinel as fm_sentinel  # noqa: E402

COLLECT = {"id": "w1", "origins": ["SFO"], "destinations": ["JFK"],
           "absolute_cap_usd": 500.0, "cooldown_hours": 24,
           "cabin": "economy", "alert_mode": "collect"}
CAP = dict(COLLECT, id="w2", alert_mode="cap")


def fresh_db(testcase):
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    db = store.init_db(path)
    testcase.addCleanup(
        lambda: (db.close(), os.path.exists(path) and os.unlink(path)))
    return db


def _enqueue(db, watch_id, fp):
    aid, _ = store.enqueue_alert(
        db, alert_id=f"{watch_id}:{fp}", watch_id=watch_id, fingerprint=fp,
        price_usd=400.0, depart_date="2026-11-01", return_date="2026-11-08",
        airline="UA", stops=0, booking_url="https://example.invalid/b",
        message_text="deal", origin="SFO", destination="JFK")
    return aid


def _set_state(db, aid, state):
    db.execute("UPDATE outbox SET state=? WHERE alert_id=?", (state, aid))
    db.commit()


def _state(db, aid):
    return store.outbox_get(db, aid)["state"]


class BudgetReleaseTests(unittest.TestCase):
    def _guard(self, db, watches, caps=None):
        # per_run_max above the ceiling: these tests measure the daily
        # limits, not the 80-attempt per-run cap.
        return BudgetGuard(
            db, per_run_max=10_000,
            verify_reserve=budget.effective_verify_reserve(watches),
            category_caps=(budget.effective_category_caps(watches)
                           if caps is None else caps))

    def test_all_collect_releases_reserve_and_caps(self):
        self.assertEqual(budget.effective_verify_reserve([COLLECT]), 0)
        self.assertEqual(budget.effective_verify_reserve([COLLECT, CAP]),
                         budget.VERIFY_RESERVE)
        caps = budget.effective_category_caps([COLLECT])
        self.assertEqual(caps["sentinel"], budget.DAILY_CEILING)
        for cat in ("verify", "retry_reserve", "background", "burst"):
            self.assertEqual(caps[cat], 0)
        self.assertEqual(budget.effective_category_caps([CAP]),
                         budget.CATEGORY_CAPS)  # legacy config unchanged

    def test_mixed_modes_fund_sentinels_and_keep_background_off(self):
        """One alerting watch must not drop sentinels back to the legacy 54
        (Stage A alone needs 71.3) or re-enable the background grid."""
        windowed = dict(COLLECT, windows={"near": {}})
        caps = budget.effective_category_caps(
            [windowed, dict(CAP, windows={"near": {}})])
        self.assertEqual(caps["sentinel"],
                         budget.DAILY_CEILING - budget.VERIFY_RESERVE
                         - budget.CATEGORY_CAPS["retry_reserve"])
        self.assertGreater(caps["sentinel"], 71.3 / 0.9)
        self.assertEqual(caps["background"], 0)
        self.assertEqual(caps["verify"], budget.VERIFY_RESERVE)
        self.assertEqual(budget.effective_category_caps([windowed, CAP])
                         ["background"], 0)

    def test_config_default_is_collect_and_unknown_mode_rejected(self):
        """watches.yaml defaults alert_mode to collect, so a watch added
        without one never alerts on the cap rule."""
        defaults, _ = fm_run.load_config()
        merged = {**defaults, "id": "new", "origins": ["SFO"],
                  "destinations": ["LAX"], "absolute_cap_usd": 100}
        self.assertEqual(budget.alert_mode(merged), "collect")
        self.assertEqual(fm_run.collect_watch_ids([merged]), {"new"})
        with self.assertRaises(ValueError):
            budget.alert_mode(dict(COLLECT, alert_mode="relative-ish"))

    def test_collect_sentinels_can_spend_the_whole_ceiling(self):
        """Stage A needs 71.3 sentinel attempts/day; the old 54 cap and the
        24 reserve would have stopped it."""
        db = fresh_db(self)
        guard = self._guard(db, [COLLECT])
        for _ in range(budget.DAILY_CEILING):
            guard.reserve("sentinel")
        with self.assertRaises(BudgetExhausted):
            guard.reserve("sentinel")

    def test_guard_and_pacer_agree_in_both_modes(self):
        for watches in ([COLLECT], [COLLECT, CAP]):
            with self.subTest(modes=[w["alert_mode"] for w in watches]):
                db = fresh_db(self)
                guard = self._guard(db, watches, caps={})
                headroom = fm_sentinel.daily_headroom(guard)
                spent = 0
                while True:
                    try:
                        guard.reserve("sentinel")
                    except BudgetExhausted:
                        break
                    spent += 1
                self.assertEqual(spent, headroom)
                self.assertEqual(fm_sentinel.daily_headroom(guard), 0)


class EnterCollectModeTests(unittest.TestCase):
    def test_retires_alerts_and_expires_bursts_in_one_pass(self):
        db = fresh_db(self)
        pending = _enqueue(db, "w1", "a")
        prepared = _enqueue(db, "w1", "b")
        _set_state(db, prepared, "handoff_prepared")
        relayed = _enqueue(db, "w1", "c")
        _set_state(db, relayed, "relay_attempted_unconfirmed")
        confirmed = _enqueue(db, "w1", "d")
        _set_state(db, confirmed, "user_confirmed")
        other = _enqueue(db, "w2", "e")
        tid = store.queue_trigger(
            db, kind="drop", group_id="w1", depart_week="2026-W45",
            depart_date="2026-11-02", duration=7, price_usd=300.0,
            fingerprint=None, slot=5)
        store.init_burst_progress(db, tid, "w1", "2026-W45",
                                  "2099-01-01T00:00:00+00:00")
        store.mark_trigger(db, tid, "burst_active")

        counts = store.enter_collect_mode(db, {"w1"})

        self.assertEqual(counts["outbox"], 3)
        logged = [r["detail"] for r in db.execute(
            "SELECT detail FROM health WHERE check_name='outbox_retire'")]
        self.assertEqual(sorted(logged), sorted(
            f"{a}: mode_collect" for a in (pending, prepared, relayed)))
        for aid in (pending, prepared, relayed):
            self.assertEqual(_state(db, aid), "retired")
        self.assertEqual(_state(db, confirmed), "user_confirmed")
        self.assertEqual(_state(db, other), "pending")
        self.assertEqual(db.execute(
            "SELECT status FROM triggers WHERE id=?", (tid,)
        ).fetchone()["status"], "expired")
        self.assertEqual(store.get_burst_progress(db, tid)["status"],
                         "expired")
        self.assertEqual(store.enter_collect_mode(db, {"w1"}),
                         {"outbox": 0, "triggers": 0, "bursts": 0})


class CollectRunTests(unittest.TestCase):
    def _run(self, db, watches, collect_fn, verify_fn):
        guard = BudgetGuard(
            db, verify_reserve=budget.effective_verify_reserve(watches),
            category_caps=budget.effective_category_caps(watches))
        guard.reset_run()
        transport.uninstall_transport_guard()
        transport.install_transport_guard(guard)
        self.addCleanup(transport.uninstall_transport_guard)
        out = io.StringIO()
        with contextlib.ExitStack() as st:
            st.enter_context(mock.patch.object(
                fm_sentinel, "mint_sentinels", return_value=0))
            st.enter_context(mock.patch.object(
                fm_sentinel, "ensure_background_grid", return_value=0))
            st.enter_context(mock.patch.object(
                fm_sentinel, "collect", collect_fn))
            st.enter_context(mock.patch.object(fm_run, "verify", verify_fn))
            st.enter_context(contextlib.redirect_stdout(out))
            args = SimpleNamespace(cmd="check", scheduled=False, watch=None)
            rc = fm_run.run_guarded("check", args, db, watches, guard,
                                    fm_run.cmd_check)
        docs = [json.loads(d) for d in
                out.getvalue().replace("}\n{", "}\x00{").split("\x00")
                if d.strip()]
        return rc, docs

    def test_first_collect_run_relays_nothing(self):
        """A cap-based alert queued before the switch is retired, not
        relayed, and an under-cap fare collected now is never verified."""
        db = fresh_db(self)
        store.sync_watches(db, [COLLECT], {})
        old = _enqueue(db, "w1", "old")
        seen = {"collect_groups": None, "verify": 0}

        def collect_writes_cheap(db_, guard_, groups, slot, run_start, **kw):
            seen["collect_groups"] = kw.get("collect_groups")
            store.add_observation(
                db_, watch_id="w1", origin="SFO", destination="JFK",
                depart_date="2026-11-01", return_date="2026-11-08",
                airline="UA", stops=0, cabin="economy", price_usd=120.0,
                booking_url="https://example.invalid/b")
            return {"attempts": 2, "bursts": [], "sentinels": 1,
                    "background": 0, "drop_triggers": 0, "allowance": 4}

        def verify_spy(*a, **kw):
            seen["verify"] += 1
            return False, None

        rc, docs = self._run(db, [COLLECT], collect_writes_cheap, verify_spy)
        self.assertEqual(rc, 0)
        relayed = [a for d in docs
                   for a in d.get("handoff", {}).get("alerts", [])]
        self.assertEqual(relayed, [])
        self.assertEqual(_state(db, old), "retired")
        self.assertEqual(seen["verify"], 0)
        self.assertEqual(seen["collect_groups"], {"w1"})


class CollectGroupTests(unittest.TestCase):
    def setUp(self):
        self.db = fresh_db(self)

    def test_no_drop_trigger_for_collect_groups(self):
        cfg = {"g1": {"origins": ["SFO"], "destinations": ["JFK"]}}
        today = store.today_str()
        fm_sentinel.mint_sentinels(self.db, cfg, today=today)
        cell = store.due_cells(self.db, "sentinel", 1)[0]
        store.record_cell_check(self.db, cell["id"], slot=1, success=True,
                                price_usd=1000.0)
        guard = BudgetGuard(self.db, verify_reserve=0,
                            category_caps={"sentinel": 150})

        def cheap(db, cell, guard, group_id, group_cfg, *, top_n, category,
                  slot, run_start=None):
            return (300.0, 2)  # a 70% drop within 72 h

        with mock.patch.object(fm_sentinel, "check_cell", cheap), \
                mock.patch.object(fm_sentinel, "per_run_allowance",
                                  lambda *a, **k: 2):
            res = fm_sentinel.collect(self.db, guard, cfg, 2,
                                      store.utcnow_iso(),
                                      collect_groups={"g1"})
        self.assertEqual(res["sentinels"], 1)
        self.assertEqual(res["drop_triggers"], 0)
        self.assertEqual(self.db.execute(
            "SELECT COUNT(*) AS c FROM triggers").fetchone()["c"], 0)

    def test_named_window_watch_has_no_background_grid(self):
        cfg = {"g1": {"origins": ["SFO"], "destinations": ["JFK"]}}
        fm_sentinel.ensure_background_grid(self.db, cfg,
                                           today="2026-09-28")
        self.assertGreater(self.db.execute(
            "SELECT COUNT(*) AS c FROM cells WHERE role='background' "
            "AND active=1").fetchone()["c"], 0)
        windowed = {"g1": fm_run.group_cfg_for({
            "id": "g1", "origins": ["SFO"], "destinations": ["JFK"],
            "nights": [7],
            "windows": {"near": {"days": [7, 42], "step_days": 2,
                                 "refresh_hours": 48}}})}
        fm_sentinel.ensure_background_grid(self.db, windowed,
                                           today="2026-09-28")
        self.assertEqual(self.db.execute(
            "SELECT COUNT(*) AS c FROM cells WHERE role='background' "
            "AND active=1").fetchone()["c"], 0)


class BurstSurplusTests(unittest.TestCase):
    def test_bursts_wait_for_due_sentinels(self):
        """A queued burst runs only once no named-window sentinel is due."""
        db = fresh_db(self)
        cfg = {"g1": fm_run.group_cfg_for({
            "id": "g1", "origins": ["SFO"], "destinations": ["JFK"],
            "nights": [7],
            "windows": {"near": {"days": [7, 42], "step_days": 2,
                                 "refresh_hours": 48}}})}
        fm_sentinel.mint_sentinels(db, cfg)
        slot = store.slot_id()
        store.queue_trigger(
            db, kind="drop", group_id="g1", depart_week="2026-W45",
            depart_date="2026-11-02", duration=7, price_usd=300.0,
            fingerprint=None, slot=slot)
        guard = BudgetGuard(db, per_run_max=10_000, verify_reserve=0,
                            category_caps={})
        bursts = []

        def fake_burst(db_, trig, guard_, group_cfg, slot_, **kw):
            bursts.append(trig["id"])
            return 0

        def fake_check(db_, cell, guard_, group_id, group_cfg, *, top_n,
                       category, slot, run_start=None):
            store.record_cell_check(db_, cell["id"], slot, True, 500.0)
            return (500.0, 2)

        with mock.patch.object(fm_sentinel, "execute_burst", fake_burst), \
                mock.patch.object(fm_sentinel, "check_cell", fake_check), \
                mock.patch.object(fm_sentinel, "per_run_allowance",
                                  lambda *a, **k: 4):
            # Odd slot: bursts lead the phase order. 18 sentinels are due.
            odd = slot | 1
            fm_sentinel.collect(db, guard, cfg, odd, store.utcnow_iso())
            self.assertEqual(bursts, [])
            db.execute("UPDATE cells SET last_checked_slot=?", (odd,))
            db.commit()
            fm_sentinel.collect(db, guard, cfg, odd, store.utcnow_iso())
        self.assertEqual(len(bursts), 1)


class StageAConfigTests(unittest.TestCase):
    def test_stage_a_config(self):
        """The shipped watches.yaml: Japan + NYC, collect-only, 71.3
        sentinel attempts/day at the refresh targets, all under 150."""
        _, watches = fm_run.load_config()
        self.assertEqual({w["id"] for w in watches}, {"japan", "nyc"})
        self.assertTrue(all(w["alert_mode"] == "collect" for w in watches))
        self.assertEqual(budget.effective_verify_reserve(watches), 0)
        db = fresh_db(self)
        cfg = {w["id"]: fm_run.group_cfg_for(w) for w in watches}
        fm_sentinel.mint_sentinels(db, cfg, today=date(2026, 9, 28))
        demand = sum(r["refresh_hours"] and 2 * 24 / r["refresh_hours"]
                     for r in db.execute(
                         "SELECT refresh_hours FROM cells WHERE active=1"))
        self.assertAlmostEqual(demand, 71.33, places=1)
        self.assertLess(demand, 0.9 * budget.DAILY_CEILING)


if __name__ == "__main__":
    unittest.main()
