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

    def _setup(self, db):
        cfg = {"g1": fm_run.group_cfg_for({
            "id": "g1", "origins": ["SFO"], "destinations": ["JFK"],
            "nights": [7],
            "windows": {"near": {"days": [7, 42], "step_days": 2,
                                 "refresh_hours": 48}}})}
        fm_sentinel.mint_sentinels(db, cfg)
        slot = store.slot_id() | 1
        store.queue_trigger(
            db, kind="drop", group_id="g1", depart_week="2026-W45",
            depart_date="2026-11-02", duration=7, price_usd=300.0,
            fingerprint=None, slot=slot)
        return cfg, slot

    def _collect(self, db, cfg, slot, allowance):
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
                                  lambda *a, **k: allowance):
            fm_sentinel.collect(db, guard, cfg, slot, store.utcnow_iso())
        return bursts

    def test_orphan_cells_of_removed_watch_do_not_block_bursts(self):
        db = fresh_db(self)
        cfg, slot = self._setup(db)
        db.execute("UPDATE cells SET last_checked_slot=?", (slot,))
        store.ensure_cells(db, [{
            "group_id": "gone", "depart_date": "2026-11-10", "duration": 7,
            "role": "sentinel", "weekday": 1, "window": "near",
            "refresh_hours": 48}])  # never checked, no config any more
        db.commit()
        self.assertEqual(len(self._collect(db, cfg, slot, 4)), 1)

    def test_orphan_cells_never_fill_the_batch(self):
        """Four never-checked cells of a removed watch must not fill the
        batch ahead of this run's one due cell."""
        db = fresh_db(self)
        # Orphans first: lower ids sort ahead of the live cell on ties.
        store.ensure_cells(db, [{
            "group_id": "gone", "depart_date": f"2026-11-1{i}", "duration": 7,
            "role": "sentinel", "weekday": 1, "window": "near",
            "refresh_hours": 48} for i in range(4)])
        cfg, slot = self._setup(db)
        db.execute("UPDATE cells SET last_checked_slot=? WHERE group_id='g1'",
                   (slot,))
        live = db.execute("SELECT MIN(id) AS i FROM cells "
                          "WHERE group_id='g1'").fetchone()["i"]
        db.execute("UPDATE cells SET last_checked_slot=NULL WHERE id=?",
                   (live,))
        db.commit()
        self.assertEqual(len(self._collect(db, cfg, slot, 4)), 1)
        self.assertIsNotNone(db.execute(
            "SELECT last_checked_slot FROM cells WHERE id=?",
            (live,)).fetchone()["last_checked_slot"])

    def test_run_that_clears_due_cells_still_bursts(self):
        """Odd run: bursts lead but 1 cell is due; the sentinel passes clear
        it, and the trailing burst pass then runs the burst."""
        db = fresh_db(self)
        cfg, slot = self._setup(db)
        db.execute("UPDATE cells SET last_checked_slot=?", (slot,))
        db.execute("UPDATE cells SET last_checked_slot=NULL WHERE id = "
                   "(SELECT MIN(id) FROM cells)")
        db.commit()
        self.assertEqual(len(self._collect(db, cfg, slot, 10)), 1)


class _DueOnlyBase:
    def _cfg(self):
        return {"g1": fm_run.group_cfg_for({
            "id": "g1", "origins": ["SFO"], "destinations": ["JFK"],
            "nights": [7],
            "windows": {"near": {"days": [7, 42], "step_days": 2,
                                 "refresh_hours": 48}}})}

    def _collect(self, db, cfg, slot, allowance):
        guard = BudgetGuard(db, per_run_max=10_000, verify_reserve=0,
                            category_caps={})
        calls = []

        def fake_check(db_, cell, guard_, group_id, group_cfg, *, top_n,
                       category, slot, run_start=None):
            calls.append(cell["id"])
            store.record_cell_check(db_, cell["id"], slot, True, 500.0)
            return (500.0, 2)

        with mock.patch.object(fm_sentinel, "check_cell", fake_check), \
                mock.patch.object(fm_sentinel, "per_run_allowance",
                                  lambda *a, **k: allowance):
            res = fm_sentinel.collect(db, guard, cfg, slot,
                                      store.utcnow_iso())
        return res, calls, guard


class DueOnlyTests(_DueOnlyBase, unittest.TestCase):
    """Follow-up to the 240b629 deploy: named-window runs check only DUE
    cells (daily use follows the refresh targets, ~71/day, not the 150
    ceiling), and an idle run with nothing due is not a stall."""

    def test_fresh_cells_are_not_rechecked(self):
        db = fresh_db(self)
        cfg = self._cfg()
        fm_sentinel.mint_sentinels(db, cfg)
        slot = store.slot_id()
        db.execute("UPDATE cells SET last_checked_slot=?", (slot,))
        db.execute("UPDATE cells SET last_checked_slot=NULL WHERE id = "
                   "(SELECT MIN(id) FROM cells)")
        db.commit()
        res, calls, guard = self._collect(db, cfg, slot, 10)
        self.assertEqual(len(calls), 1)  # only the one due cell
        self.assertEqual(res["overdue_at_start"], 1)
        res, calls, guard = self._collect(db, cfg, slot, 10)
        self.assertEqual(calls, [])       # nothing due: idle
        self.assertEqual(res["overdue_at_start"], 0)
        self.assertFalse(fm_run._stalled(guard, res))
        # ...but sending nothing while cells ARE due is still a stall.
        self.assertTrue(fm_run._stalled(guard, dict(res, overdue_at_start=3,
                                                    allowance=6)))


class DueOnlyReviewTests(_DueOnlyBase, unittest.TestCase):
    """Codex review of PR #38: legacy watches in a mixed config still
    collect, and an idle run ends a stall streak."""

    def test_mixed_config_still_collects_legacy_watches(self):
        db = fresh_db(self)
        cfg = dict(self._cfg(),
                   old={"origins": ["SFO"], "destinations": ["NRT"]})
        fm_sentinel.mint_sentinels(db, cfg)
        slot = store.slot_id()
        db.execute("UPDATE cells SET last_checked_slot=? "
                   "WHERE window_name IS NOT NULL", (slot,))
        db.commit()
        res, calls, guard = self._collect(db, cfg, slot, 4)
        groups = {r["group_id"] for r in db.execute(
            f"SELECT group_id FROM cells WHERE id IN "
            f"({','.join(str(c) for c in calls) or 'NULL'})")}
        self.assertEqual(groups, {"old"})
        self.assertGreater(res["overdue_at_start"], 0)

    def test_idle_run_ends_a_stall_streak(self):
        db = fresh_db(self)
        for slot, attempts, stalled, idle in ((1, 0, 1, 0), (2, 0, 1, 0),
                                              (3, 0, 0, 1), (4, 0, 1, 0),
                                              (5, 0, 1, 0)):
            store.claim_slot(db, slot, "check")
            store.record_run_stats(db, slot, attempts, bool(stalled),
                                   idle=bool(idle))
        self.assertEqual(store.stall_streak(db), 2)


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


class ExploreTests(unittest.TestCase):
    """Trip-length exploration: extra stays on the window's dates, only from
    leftover budget (no sentinel due), cheap checks, capped per day; off
    unless a watch sets explore_nights and explore_per_day."""

    WATCH = {"id": "japan", "origins": ["SFO"], "destinations": ["NRT"],
             "absolute_cap_usd": 500, "alert_mode": "collect", "nights": [28],
             "explore_nights": [14, 21, 28], "explore_per_day": 10,
             "windows": {"near": {"days": [7, 42], "step_days": 2,
                                  "refresh_hours": 48},
                         "far": {"months": [3, 9], "refresh_hours": 72}}}

    def test_grid_caps_and_off_by_default(self):
        db = fresh_db(self)
        cfg = {"japan": fm_run.group_cfg_for(self.WATCH)}
        self.assertEqual(cfg["japan"]["explore_nights"], [14, 21])  # 28 is main
        fm_sentinel.ensure_background_grid(db, cfg)
        rows = db.execute(
            "SELECT window_name, duration, refresh_hours, COUNT(*) AS n "
            "FROM cells WHERE role = 'background' AND active = 1 "
            "GROUP BY 1, 2, 3").fetchall()
        got = {(r["window_name"], r["duration"], r["refresh_hours"]): r["n"]
               for r in rows}
        self.assertEqual(got[("near", 14, 144)], 18)
        self.assertEqual(got[("near", 21, 144)], 18)
        self.assertTrue(24 <= got[("far", 14, 216)] <= 27)
        self.assertEqual(budget.effective_category_caps([self.WATCH])
                         ["background"], 10)
        off = dict(self.WATCH, explore_per_day=0)
        fm_sentinel.ensure_background_grid(
            db, {"japan": fm_run.group_cfg_for(off)})
        self.assertEqual(db.execute(
            "SELECT COUNT(*) AS c FROM cells WHERE role = 'background' AND "
            "active = 1").fetchone()["c"], 0)
        self.assertEqual(budget.effective_category_caps([off])["background"], 0)

    def test_explores_only_when_nothing_is_due_within_its_cap(self):
        db = fresh_db(self)
        cfg = {"japan": fm_run.group_cfg_for(self.WATCH)}
        fm_sentinel.mint_sentinels(db, cfg)
        fm_sentinel.ensure_background_grid(db, cfg)
        slot = store.slot_id()
        guard = BudgetGuard(db, per_run_max=10_000, verify_reserve=0,
                            category_caps=budget.effective_category_caps(
                                [self.WATCH]))
        calls = []

        def fake_check(db_, cell, guard_, group_id, group_cfg, *, top_n,
                       category, slot, run_start=None):
            calls.append((cell["role"], category, top_n))
            guard_.reserve(category)
            guard_.reserve(category)
            store.record_cell_check(db_, cell["id"], slot, True, 900.0)
            return (900.0, 2)

        def run_once(allowance):
            calls.clear()
            with mock.patch.object(fm_sentinel, "check_cell", fake_check), \
                    mock.patch.object(fm_sentinel, "per_run_allowance",
                                      lambda *a, **k: allowance):
                fm_sentinel.collect(db, guard, cfg, slot, store.utcnow_iso())
            return list(calls)

        # Sentinels are due: the run spends everything on them.
        first = run_once(6)
        self.assertEqual({c[0] for c in first}, {"sentinel"})
        # Nothing due: leftover goes to cheap exploration checks...
        db.execute("UPDATE cells SET last_checked_slot=? "
                   "WHERE role = 'sentinel'", (slot,))
        db.commit()
        second = run_once(40)
        self.assertEqual(set(second), {("background", "background", 1)})
        # ...but never past the daily explore cap (10 attempts = 5 checks).
        self.assertEqual(guard.spent_today("background"), 10)


class ExploreReviewTests(unittest.TestCase):
    """Codex review of PR #43."""

    def test_only_due_cells_of_exploring_watches_and_metadata_updates(self):
        db = fresh_db(self)
        w = ExploreTests.WATCH
        cfg = {"japan": fm_run.group_cfg_for(w),
               "old": {"origins": ["SFO"], "destinations": ["JFK"]}}
        fm_sentinel.mint_sentinels(db, cfg)
        fm_sentinel.ensure_background_grid(db, cfg)
        slot = store.slot_id()
        db.execute("UPDATE cells SET last_checked_slot=? "
                   "WHERE role = 'sentinel'", (slot,))
        # Explore cells fresh (checked 1 h ago) except exactly one.
        db.execute("UPDATE cells SET last_checked_slot=? "
                   "WHERE role = 'background'", (slot - 2,))
        one = db.execute("SELECT MIN(id) AS i FROM cells WHERE role = "
                         "'background' AND group_id = 'japan'").fetchone()["i"]
        db.execute("UPDATE cells SET last_checked_slot=NULL WHERE id=?", (one,))
        db.commit()
        guard = BudgetGuard(db, per_run_max=10_000, verify_reserve=0,
                            category_caps={"background": 100})
        seen = []

        def fake_check(db_, cell, guard_, group_id, group_cfg, *, top_n,
                       category, slot, run_start=None):
            seen.append((cell["group_id"], cell["role"], cell["id"]))
            store.record_cell_check(db_, cell["id"], slot, True, 900.0)
            return (900.0, 2)

        with mock.patch.object(fm_sentinel, "check_cell", fake_check), \
                mock.patch.object(fm_sentinel, "per_run_allowance",
                                  lambda *a, **k: 40):
            fm_sentinel.collect(db, guard, {"japan": cfg["japan"]}, slot,
                                store.utcnow_iso())
        self.assertEqual([s for s in seen if s[1] == "background"],
                         [("japan", "background", one)])
        # A changed refresh target reaches existing exploration cells.
        w2 = dict(w, windows={"near": {"days": [7, 42], "step_days": 2,
                                       "refresh_hours": 24},
                              "far": w["windows"]["far"]})
        fm_sentinel.ensure_background_grid(
            db, {"japan": fm_run.group_cfg_for(w2)})
        self.assertEqual({r["refresh_hours"] for r in db.execute(
            "SELECT refresh_hours FROM cells WHERE role = 'background' AND "
            "active = 1 AND window_name = 'near'")}, {72})

    def test_explore_budget_never_makes_a_stall(self):
        guard = BudgetGuard(fresh_db(self), verify_reserve=0,
                            category_caps={"sentinel": 0, "background": 20})
        # A sentinel is due but its category is spent; exploration waits.
        self.assertFalse(fm_run._stalled(
            guard, {"attempts": 0, "allowance": 6, "overdue_at_start": 2}))


class Phase2ConfigTests(unittest.TestCase):
    """The Phase 2 destinations are written in watches.yaml but switched
    off; switched on, all seven fit the spec's budget."""

    def _all(self):
        import yaml
        with open(_PROJECT_ROOT / "watches.yaml") as f:
            cfg = yaml.safe_load(f)
        d = cfg.get("defaults", {})
        return [{**d, **w} for w in cfg["watches"]]

    def test_disabled_watches_are_not_loaded(self):
        _, watches = fm_run.load_config()
        self.assertEqual({w["id"] for w in watches}, {"japan", "nyc"})
        off = {w["id"] for w in self._all() if w.get("enabled") is False}
        self.assertEqual(off, {"la", "sd", "hkg", "shanghai", "beijing"})

    def test_all_seven_parse_and_fit_the_300_stage(self):
        from datetime import date
        db = fresh_db(self)
        watches = self._all()
        cfg = {w["id"]: fm_run.group_cfg_for(w) for w in watches}
        fm_sentinel.mint_sentinels(db, cfg, today=date(2026, 10, 1))
        demand = sum(2 * 24 / r["refresh_hours"] for r in db.execute(
            "SELECT refresh_hours FROM cells WHERE active = 1"))
        # Spec: 197.3-201.3 attempts/day for all seven groups.
        self.assertTrue(197 <= demand <= 202, demand)
        self.assertLess(demand, 0.9 * (300 - 24 - 12))
        per = {}
        for r in db.execute(
                """SELECT group_id, window_name, weekday, duration, COUNT(*) AS n
                   FROM cells WHERE active = 1
                   GROUP BY 1, 2, 3, 4"""):
            g = per.setdefault((r["group_id"], r["window_name"]),
                               {"n": 0, "wd": set(), "dur": set()})
            g["n"] += r["n"]
            g["wd"].add(r["weekday"])
            g["dur"].add(r["duration"])
        for gid in ("la", "sd"):  # every Fri (2 nights) and Sat (1 night)
            g = per.pop((gid, "near"))
            self.assertEqual((g["wd"], g["dur"]), ({4, 5}, {1, 2}))
            self.assertTrue(10 <= g["n"] <= 12, (gid, g["n"]))
            self.assertEqual(cfg[gid]["windows"][0]["depart_hours"],
                             {5: "5-11"})
        for gid in ("japan", "hkg", "shanghai", "beijing"):
            self.assertEqual(per.pop((gid, "near"))["n"], 18)
            far = per.pop((gid, "far"))
            self.assertTrue(24 <= far["n"] <= 27 and far["dur"] == {28},
                            (gid, far))
        nyc = per.pop(("nyc", "near"))
        self.assertEqual((nyc["n"], nyc["dur"]), (36, {7, 14}))
        self.assertEqual(per, {})

    def test_disabled_watch_queued_alert_is_retired_not_relayed(self):
        """A watch switched off after it alerted must not relay its queued
        alert on the next run."""
        db = fresh_db(self)
        aid = _enqueue(db, "la", "old")
        self.assertIn("la", fm_run.disabled_watch_ids())

        def collect_nothing(db_, guard_, groups, slot, run_start, **kw):
            return {"attempts": 0, "bursts": [], "sentinels": 0,
                    "background": 0, "drop_triggers": 0, "allowance": 0,
                    "overdue_at_start": 0}

        rc, docs = CollectRunTests._run(self, db, [COLLECT], collect_nothing,
                                        lambda *a, **k: (False, None))
        self.assertEqual(rc, 0)
        relayed = [a for d in docs
                   for a in d.get("handoff", {}).get("alerts", [])]
        self.assertEqual(relayed, [])
        self.assertEqual(_state(db, aid), "retired")


if __name__ == "__main__":
    unittest.main()
