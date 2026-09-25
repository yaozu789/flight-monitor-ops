"""Tests for sentinel.py (plan 04: sentinel rotation + burst scheduling).

unittest only (the venv has no pytest). No real network: tests either
monkeypatch ``sentinel.check_cell`` / ``sentinel.narrow_query`` or use the
offline fake transport. Temp file DBs throughout.

BudgetGuard wiring: the parent's v2 budget module is not assembled yet, so
``try: from budget import BudgetGuard`` falls back to a minimal shim
implementing the exact v2 API (``BudgetGuard(db, per_run_max=80,
daily_ceiling=150, category_caps=None)``; ``reserve(category)`` raising a
``BudgetExhausted`` Exception subclass with ``.scope``/``.category``;
``spent_today(category=None)``; ``spent_run(category=None)``; ``reset_run()``)
backed by ``store.count_today``. The shim is single-threaded and non-atomic
-- fine for these tests. The ``try`` branch also API-checks the import: the
stale base-tree ``s4/budget.py`` (old guard API) must NOT be picked up. The
parent runs this suite against the real ``budget.py`` at assembly, where the
``try`` branch wins. The shim raises ``sentinel.BudgetExhausted`` so
``collect``'s ``except`` catches it pre-assembly too.
"""
from __future__ import annotations

import os
import shutil
import sys
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
_TESTS_DIR = HERE
_PROJECT_ROOT = os.path.dirname(HERE)
# Project root FIRST so `import store` gets the project's v2 store.
sys.path.insert(0, _PROJECT_ROOT)
sys.path.insert(0, _TESTS_DIR)  # fake_transport

import store  # noqa: E402
import sentinel  # noqa: E402

# ---------------------------------------------------------------- guard wiring

try:
    from budget import BudgetGuard as _BudgetGuard  # noqa: E402
    import inspect as _inspect  # noqa: E402
    _sig_params = _inspect.signature(_BudgetGuard.__init__).parameters
    if not ("per_run_max" in _sig_params
            and all(hasattr(_BudgetGuard, n)
                    for n in ("reserve", "spent_today", "spent_run",
                              "reset_run"))):
        raise ImportError("budget.BudgetGuard lacks the v2 API")
    BudgetGuard = _BudgetGuard
    REAL_GUARD = True
except ImportError:
    REAL_GUARD = False

    class BudgetGuard:
        """Minimal v2-API shim backed by store.count_today.

        Single-threaded and non-atomic; only a stopgap until the parent's
        budget.py exists. Raises sentinel.BudgetExhausted so sentinel.collect
        catches it exactly like the real guard's exception.
        """

        def __init__(self, db, per_run_max=80, daily_ceiling=150,
                     category_caps=None):
            self.db = db
            self.per_run_max = per_run_max
            self.daily_ceiling = daily_ceiling
            self.category_caps = dict(category_caps or {})
            self._run_total = 0
            self._run_by_cat = {}

        def _day(self):
            return store.today_str()

        def reserve(self, category):
            day = self._day()
            if self._run_total >= self.per_run_max:
                raise sentinel.BudgetExhausted("per_run", category)
            if store.count_today(self.db, day) >= self.daily_ceiling:
                raise sentinel.BudgetExhausted("daily", category)
            cap = self.category_caps.get(category)
            if (cap is not None
                    and store.count_today(self.db, day, category) >= cap):
                raise sentinel.BudgetExhausted("category", category)
            store.ledger_insert(self.db, day, category, 0)
            self.db.commit()
            self._run_total += 1
            self._run_by_cat[category] = self._run_by_cat.get(category, 0) + 1

        def spent_today(self, category=None):
            return store.count_today(self.db, self._day(), category)

        def spent_run(self, category=None):
            if category is None:
                return self._run_total
            return self._run_by_cat.get(category, 0)

        def reset_run(self):
            self._run_total = 0
            self._run_by_cat = {}


GROUPS = {
    "japan_fall_2026": {"origins": ["SFO", "OAK", "SJC"],
                        "destinations": ["NRT", "HND", "KIX"]},
    "nyc_winter_2026": {"origins": ["SFO", "OAK", "SJC"],
                        "destinations": ["JFK", "EWR", "LGA"]},
}
TODAY = "2026-09-24"


def _row(price, depart="2026-12-01", ret="2026-12-08"):
    return {"origin": "SFO", "destination": "NRT", "depart_date": depart,
            "return_date": ret, "airline": "JL", "stops": 0,
            "price_usd": price, "currency": "USD",
            "booking_url": "https://example.com/x", "cabin": "economy"}


class SentinelTestBase(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="sentinel-test-")
        self.db = store.get_db(os.path.join(self.tmpdir, "t.db"))
        store.init_db(self.db)
        self._patched = []

    def tearDown(self):
        for obj, name, orig in self._patched:
            setattr(obj, name, orig)
        self.db.close()
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def patch(self, obj, name, new):
        self._patched.append((obj, name, getattr(obj, name)))
        setattr(obj, name, new)

    def guard(self, **kw):
        return BudgetGuard(self.db, **kw)

    def spend(self, category, n, day=None):
        day = day or store.today_str()
        for _ in range(n):
            store.ledger_insert(self.db, day, category, 0)
        self.db.commit()


# ---------------------------------------------------------------- minting

class TestMint(SentinelTestBase):
    def test_mint_creates_27_per_group(self):
        n = sentinel.mint_sentinels(self.db, GROUPS, today=TODAY)
        self.assertEqual(n, 54)
        for gid in GROUPS:
            rows = self.db.execute(
                "SELECT * FROM cells WHERE group_id=? AND role='sentinel' "
                "AND active=1", (gid,)).fetchall()
            self.assertEqual(len(rows), 27)
            for r in rows:
                self.assertEqual(r["duration"], 7)

    def test_mint_weekday_span(self):
        sentinel.mint_sentinels(self.db, GROUPS, today=TODAY)
        wds = {r["weekday"] for r in self.db.execute(
            "SELECT weekday FROM cells WHERE role='sentinel'")}
        self.assertGreaterEqual(len(wds), 4)

    def test_mint_window(self):
        sentinel.mint_sentinels(self.db, GROUPS, today=TODAY)
        lo = (date.fromisoformat(TODAY) + timedelta(days=90)).isoformat()
        hi = (date.fromisoformat(TODAY) + timedelta(days=270)).isoformat()
        bad = self.db.execute(
            "SELECT COUNT(*) AS c FROM cells WHERE role='sentinel' "
            "AND (depart_date < ? OR depart_date > ?)", (lo, hi)).fetchone()
        self.assertEqual(bad["c"], 0)

    def test_mint_idempotent(self):
        self.assertEqual(sentinel.mint_sentinels(self.db, GROUPS, today=TODAY), 54)
        self.assertEqual(sentinel.mint_sentinels(self.db, GROUPS, today=TODAY), 0)
        n = self.db.execute(
            "SELECT COUNT(*) AS c FROM cells WHERE role='sentinel'").fetchone()
        self.assertEqual(n["c"], 54)

    def test_mint_retires_out_of_window(self):
        today_d = date.fromisoformat(TODAY)
        seeded = [
            # too soon (< +75d) -> retired
            ("japan_fall_2026", (today_d + timedelta(days=10)).isoformat()),
            # too far (> +285d) -> retired
            ("japan_fall_2026", (today_d + timedelta(days=300)).isoformat()),
            # in window -> stays active
            ("japan_fall_2026", (today_d + timedelta(days=100)).isoformat()),
        ]
        store.ensure_cells(self.db, [
            {"group_id": gid, "depart_date": d, "duration": 7,
             "role": "sentinel", "weekday": 0} for gid, d in seeded])
        sentinel.mint_sentinels(self.db, GROUPS, today=TODAY)
        flags = {}
        for gid, d in seeded:
            row = self.db.execute(
                "SELECT active FROM cells WHERE group_id=? AND depart_date=? "
                "AND duration=7 AND role='sentinel'", (gid, d)).fetchone()
            flags[d] = row["active"]
        self.assertEqual(flags[seeded[0][1]], 0)
        self.assertEqual(flags[seeded[1][1]], 0)
        self.assertEqual(flags[seeded[2][1]], 1)

    def test_background_grid_counts(self):
        n = sentinel.ensure_background_grid(self.db, GROUPS, today=TODAY)
        # 24 weekly departures (14..180d) x 10 durations (5..14) x 2 groups
        self.assertEqual(n, 480)
        self.assertEqual(
            sentinel.ensure_background_grid(self.db, GROUPS, today=TODAY), 0)
        durs = {r["duration"] for r in self.db.execute(
            "SELECT DISTINCT duration FROM cells WHERE role='background'")}
        self.assertEqual(durs, set(range(5, 15)))


# ---------------------------------------------------------------- rotation

class TestRotation(SentinelTestBase):
    def test_due_sentinels_rotation(self):
        one = {"g1": GROUPS["japan_fall_2026"]}
        sentinel.mint_sentinels(self.db, one, today=TODAY)
        first = self.db.execute(
            "SELECT id FROM cells WHERE role='sentinel' ORDER BY id ASC "
            "LIMIT 1").fetchone()["id"]
        store.record_cell_check(self.db, first, slot=99, success=True,
                                price_usd=400.0)
        due = sentinel.due_sentinels(self.db, 27)
        self.assertEqual(len(due), 27)
        self.assertEqual(due[-1]["id"], first)
        self.assertNotEqual(due[0]["id"], first)

    def test_due_background(self):
        sentinel.ensure_background_grid(
            self.db, {"g1": GROUPS["japan_fall_2026"]}, today=TODAY)
        due = sentinel.due_background(self.db, 5)
        self.assertEqual(len(due), 5)
        self.assertTrue(all(c["role"] == "background" for c in due))


# ---------------------------------------------------------------- pacing

class TestAllowance(SentinelTestBase):
    def test_fresh_day_slot_0(self):
        # ceil(126/48) = ceil(2.625) = 3 (verify reserve held back; v3: no
        # minimum floor, the paced value stands on its own)
        self.assertEqual(sentinel.per_run_allowance(self.guard(), 0), 3)

    def test_zero_when_exhausted(self):
        self.spend("sentinel", 140)
        # remaining = 150-140-max(0, 24-0) = -14 <= 0 -> allowance 0 (v3
        # item 7: no floor of 4 when the budget is exhausted)
        self.assertEqual(sentinel.per_run_allowance(self.guard(), 0), 0)

    def test_midday(self):
        self.spend("sentinel", 75)
        # remaining = 150-75-24 = 51; ceil(51/24) = 3
        self.assertEqual(sentinel.per_run_allowance(self.guard(), 24), 3)

    def test_verify_reserve_held_back(self):
        self.spend("verify", 10)
        g = self.guard()
        # remaining = 150-10-max(0, 24-10) = 126 -> ceil(126/48)=3
        self.assertEqual(sentinel.per_run_allowance(g, 0), 3)
        # without the reserve it would be ceil(140/48)=3 too here;
        # the reserve bites harder late-day:
        self.spend("verify", 14)  # verify total 24 -> reserve 0
        self.assertEqual(sentinel.per_run_allowance(self.guard(), 40), 12)

    def test_late_day_ramps_up(self):
        # remaining = 150-0-24 = 126 over 8 slots -> ceil(15.75)=16 -> cap 12
        self.assertEqual(sentinel.per_run_allowance(self.guard(), 40), 12)

    def test_allowance_never_negative(self):
        self.spend("sentinel", 150)
        self.assertEqual(sentinel.per_run_allowance(self.guard(), 0), 0)
        self.assertEqual(sentinel.per_run_allowance(self.guard(), 47), 0)


# ---------------------------------------------------------------- collect priority

class TestCollectPriority(SentinelTestBase):
    def test_cadence_aware_pacing(self):
        """At a 2-hour cadence (4 slots per run) a run gets ~4x the share,
        within the 80 hard cap."""
        g = self.guard()
        per_30 = sentinel.per_run_allowance(g, 0, 1)
        per_2h = sentinel.per_run_allowance(g, 0, 4)
        self.assertGreater(per_2h, per_30)
        self.assertLessEqual(per_2h, sentinel.PER_RUN_HARD_CAP)

    def test_odd_slot_leads_with_exploration(self):
        """With a small allowance (3, about the 30-minute share) an odd
        slot reaches background before sentinels; whole checks only."""
        depart = (date.fromisoformat(TODAY) + timedelta(days=100)).isoformat()
        store.ensure_cells(self.db, [
            {"group_id": "japan_fall_2026", "depart_date": depart,
             "duration": 7, "role": "sentinel", "weekday": 0},
            {"group_id": "japan_fall_2026", "depart_date": depart,
             "duration": 8, "role": "background", "weekday": 0}])
        calls = []

        def fake_check(db, cell, guard, group_id, group_cfg, *, top_n,
                       category, slot, run_start=None):
            calls.append((category, top_n))
            return (400.0, 1 + top_n)

        self.patch(sentinel, "check_cell", fake_check)
        self.patch(sentinel, "per_run_allowance", lambda guard, slot, *a: 3)
        sentinel.collect(self.db, self.guard(), GROUPS, slot=5,
                         run_start=store.utcnow_iso())
        self.assertEqual(calls[0][0], "background")
        # A 3-attempt share is raised to one whole background check (4);
        # it uses it all and no partial sentinel check starts after it.
        self.assertEqual(calls, [("background", 3)])

    def test_no_borrow_when_day_nearly_spent(self):
        """With under one whole background check of daily headroom left,
        an odd slot does not borrow (it would stop mid-check)."""
        g = self.guard()
        self.patch(sentinel, "daily_headroom", lambda guard: 3)
        self.patch(sentinel, "per_run_allowance", lambda guard, slot, *a: 3)
        sentinel.collect(self.db, g, {}, slot=5, run_start=store.utcnow_iso())
        self.assertEqual(g.run_allowance(), 3)

    def test_skipped_cell_is_retried_in_leftover_pass(self):
        """A cell that did not fit the first sentinel share is still
        checked by the leftover pass (it was never started)."""
        depart = (date.fromisoformat(TODAY) + timedelta(days=100)).isoformat()
        store.ensure_cells(self.db, [
            {"group_id": "japan_fall_2026", "depart_date": depart,
             "duration": 7, "role": "sentinel", "weekday": 0}])
        calls = []

        def fake_check(db, cell, guard, group_id, group_cfg, *, top_n,
                       category, slot, run_start=None):
            calls.append(category)
            return (400.0, 1 + top_n)

        self.patch(sentinel, "check_cell", fake_check)
        # allowance 3: sentinel share ceil(3/2)=2 fits one 2-send check.
        # allowance 2: share 1 fits none; the leftover pass (2) fits one.
        self.patch(sentinel, "per_run_allowance", lambda guard, slot, *a: 2)
        sentinel.collect(self.db, self.guard(), GROUPS, slot=4,
                         run_start=store.utcnow_iso())
        self.assertEqual(calls, ["sentinel"])

    def test_run_total_never_exceeds_80(self):
        g = self.guard()
        g.set_run_allowance(80)
        for _ in range(36):
            g.reserve("verify") if _ < 24 else g.reserve("retry_reserve")
        self.patch(sentinel, "per_run_allowance", lambda guard, slot, *a: 48)
        sentinel.collect(self.db, g, {}, slot=4, run_start=store.utcnow_iso())
        self.assertLessEqual(g.run_allowance(), sentinel.PER_RUN_HARD_CAP)

    def test_sentinel_cap_does_not_end_collection(self):
        """Hitting the sentinel CATEGORY cap ends only the sentinel phase;
        background still runs in the same collection pass."""
        from budget import BudgetExhausted
        depart = (date.fromisoformat(TODAY) + timedelta(days=100)).isoformat()
        store.ensure_cells(self.db, [
            {"group_id": "japan_fall_2026", "depart_date": depart,
             "duration": 7, "role": "sentinel", "weekday": 0},
            {"group_id": "japan_fall_2026", "depart_date": depart,
             "duration": 8, "role": "background", "weekday": 0}])
        calls = []

        def fake_check(db, cell, guard, group_id, group_cfg, *, top_n,
                       category, slot, run_start=None):
            calls.append(category)
            if category == "sentinel":
                raise BudgetExhausted("category", "sentinel", 54, 54)
            return (400.0, 1)

        self.patch(sentinel, "check_cell", fake_check)
        self.patch(sentinel, "per_run_allowance", lambda guard, slot, *a: 6)
        sentinel.collect(self.db, self.guard(), GROUPS, slot=5,
                         run_start=store.utcnow_iso())
        self.assertIn("background", calls)

    def test_allowance_is_relative_to_run_spend(self):
        """Attempts spent earlier in the run (retry/verify) do not eat the
        collection allowance."""
        g = self.guard()
        g.set_run_allowance(80)
        for _ in range(5):
            g.reserve("verify")
        self.patch(sentinel, "per_run_allowance", lambda guard, slot, *a: 3)
        sentinel.collect(self.db, g, {}, slot=4, run_start=store.utcnow_iso())
        self.assertEqual(g.run_allowance(), 5 + 3)

    def test_burst_never_passes_60(self):
        """A resumed burst at 59 attempts does not start a 2-send step."""
        from datetime import datetime, timedelta as td, timezone as tz
        depart = (date.fromisoformat(TODAY) + timedelta(days=100)).isoformat()
        tid = store.queue_trigger(
            self.db, kind="under_cap", group_id="japan_fall_2026",
            depart_week=store.depart_week(depart), depart_date=depart,
            duration=7, price_usd=400.0, fingerprint="fp-cap", slot=1)
        trig = [t for t in store.due_triggers(self.db, 2) if t["id"] == tid][0]
        expires = (datetime.now(tz.utc) + td(hours=12)).isoformat()
        store.init_burst_progress(self.db, tid, "japan_fall_2026",
                                  trig["depart_week"], expires)
        store.save_burst_progress(self.db, tid, next_step=5, attempts_used=59)
        calls = []
        self.patch(sentinel, "check_cell",
                   lambda *a, **k: calls.append(1) or (400.0, 2))
        used = sentinel.execute_burst(self.db, trig, self.guard(),
                                      GROUPS["japan_fall_2026"], 3,
                                      allowance=10)
        self.assertEqual((used, calls), (0, []))

    def test_sentinels_before_bursts_before_background(self):
        # v3 group 4: collection priority is sentinels -> bursts ->
        # background (bursts no longer jump the queue).
        depart = (date.fromisoformat(TODAY) + timedelta(days=100)).isoformat()
        tid = store.queue_trigger(
            self.db, kind="under_cap", group_id="japan_fall_2026",
            depart_week=store.depart_week(depart), depart_date=depart,
            duration=7, price_usd=400.0, fingerprint="fp-prio", slot=1)
        store.ensure_cells(self.db, [
            {"group_id": "japan_fall_2026", "depart_date": depart,
             "duration": 7, "role": "sentinel", "weekday": 0},
            {"group_id": "japan_fall_2026", "depart_date": depart,
             "duration": 8, "role": "background", "weekday": 0},
        ])
        calls = []

        def fake_burst(db, trigger, guard, group_cfg, slot, run_start=None,
                       **kwargs):
            calls.append(("burst", trigger["id"]))
            return 0

        def fake_check(db, cell, guard, group_id, group_cfg, *, top_n,
                       category, slot, run_start=None):
            calls.append((category, top_n))
            return (400.0, 2)

        self.patch(sentinel, "execute_burst", fake_burst)
        self.patch(sentinel, "check_cell", fake_check)
        self.patch(sentinel, "per_run_allowance", lambda guard, slot, *a: 6)

        result = sentinel.collect(self.db, self.guard(), GROUPS, slot=4,
                                  run_start=store.utcnow_iso())
        kinds = [c[0] for c in calls]
        self.assertEqual(kinds[0], "sentinel")
        burst_at = kinds.index("burst")
        first_bg = kinds.index("background")
        self.assertGreater(burst_at, 0)
        self.assertGreater(first_bg, burst_at)
        self.assertTrue(all(k == "sentinel" for k in kinds[:burst_at]))
        # After background, only leftover allowance flowing back to
        # sentinels may follow (never another burst).
        tail = kinds[first_bg:]
        self.assertNotIn("burst", tail)
        if "sentinel" in tail:
            self.assertTrue(all(k == "sentinel"
                                for k in tail[tail.index("sentinel"):]))
        self.assertEqual(result["bursts"], [tid])
        self.assertEqual(result["sentinels"], 1)
        self.assertEqual(result["background"], 1)
        # sentinel phase used top_n=1, background top_n=3
        self.assertEqual(calls[0], ("sentinel", 1))
        self.assertEqual(calls[-1], ("background", 3))

    def test_collect_stops_cleanly_on_budget_exhausted(self):
        depart = (date.fromisoformat(TODAY) + timedelta(days=100)).isoformat()
        store.ensure_cells(self.db, [
            {"group_id": "japan_fall_2026",
             "depart_date": (date.fromisoformat(depart)
                             + timedelta(days=i)).isoformat(),
             "duration": 7, "role": "sentinel", "weekday": 0}
            for i in range(5)
        ])

        def fake_check(db, cell, guard, group_id, group_cfg, *, top_n,
                       category, slot, run_start=None):
            guard.reserve(category)  # bills for real
            guard.reserve(category)
            return (400.0, 2)

        self.patch(sentinel, "check_cell", fake_check)
        # v3: collect() drives the per-send allowance from the pacer, so pin
        # the paced value for this test; the guard's per_run_max=4 is what
        # the fake checks exhaust (2 checks x 2 reserves, then raise).
        self.patch(sentinel, "per_run_allowance", lambda guard, slot, *a: 4)
        g = self.guard(per_run_max=4)  # 2 checks, then BudgetExhausted
        result = sentinel.collect(self.db, g, GROUPS, slot=5,
                                  run_start=store.utcnow_iso())
        self.assertEqual(result["sentinels"], 2)
        self.assertEqual(result["attempts"], 4)


# ---------------------------------------------------------------- check_cell

class TestCheckCell(SentinelTestBase):
    def _seed_cell(self):
        depart = (date.fromisoformat(TODAY) + timedelta(days=100)).isoformat()
        store.ensure_cells(self.db, [
            {"group_id": "japan_fall_2026", "depart_date": depart,
             "duration": 7, "role": "sentinel", "weekday": 0}])
        row = self.db.execute(
            "SELECT * FROM cells WHERE role='sentinel'").fetchone()
        return dict(row)

    def test_forwards_attempt_category(self):
        seen = []

        def fake_nq(origin, destination, depart_date, return_date, cfg, *,
                    top_n, attempt_category):
            seen.append({"origin": origin, "destination": destination,
                         "top_n": top_n, "category": attempt_category,
                         "cfg": cfg})
            return [_row(400.0)]

        self.patch(sentinel, "narrow_query", fake_nq)
        cell = self._seed_cell()
        g = self.guard()
        rs = store.utcnow_iso()
        sentinel.check_cell(self.db, cell, g, "japan_fall_2026",
                            GROUPS["japan_fall_2026"],
                            top_n=1, category="sentinel", slot=1, run_start=rs)
        sentinel.check_cell(self.db, cell, g, "japan_fall_2026",
                            GROUPS["japan_fall_2026"],
                            top_n=1, category="verify", slot=1, run_start=rs)
        self.assertEqual([s["category"] for s in seen],
                         ["sentinel", "verify"])
        # grouped airport codes are comma-joined into one query
        self.assertEqual(seen[0]["origin"], "SFO,OAK,SJC")
        self.assertEqual(seen[0]["destination"], "NRT,HND,KIX")
        self.assertEqual(seen[0]["cfg"]["stops"], [0, 1])

    def test_persists_observations_on_success(self):
        rows = [_row(412.5), _row(500.0)]
        self.patch(sentinel, "narrow_query", lambda *a, **k: rows)
        cell = self._seed_cell()
        g = self.guard()
        rs = "2026-09-24T21:00:00Z"
        price, used = sentinel.check_cell(
            self.db, cell, g, "japan_fall_2026", GROUPS["japan_fall_2026"],
            top_n=1, category="sentinel", slot=3, run_start=rs)
        self.assertEqual(price, 412.5)
        obs = self.db.execute(
            "SELECT * FROM observations WHERE watch_id='japan_fall_2026'"
            ).fetchall()
        self.assertEqual(len(obs), 2)
        self.assertEqual(sorted(o["price_usd"] for o in obs), [412.5, 500.0])
        self.assertTrue(all(o["observed_at"] == rs for o in obs))
        # cell recorded as a successful check with the cheapest price
        row = self.db.execute(
            "SELECT last_price_usd, last_checked_slot FROM cells WHERE id=?",
            (cell["id"],)).fetchone()
        self.assertEqual(row["last_price_usd"], 412.5)
        self.assertEqual(row["last_checked_slot"], 3)

    def test_unknown_no_persist(self):
        self.patch(sentinel, "narrow_query", lambda *a, **k: None)
        cell = self._seed_cell()
        price, used = sentinel.check_cell(
            self.db, cell, self.guard(), "japan_fall_2026",
            GROUPS["japan_fall_2026"], top_n=1, category="sentinel", slot=3,
            run_start="2026-09-24T21:00:00Z")
        self.assertIsNone(price)
        n = self.db.execute(
            "SELECT COUNT(*) AS c FROM observations").fetchone()["c"]
        self.assertEqual(n, 0)
        row = self.db.execute(
            "SELECT last_price_usd FROM cells WHERE id=?",
            (cell["id"],)).fetchone()
        self.assertIsNone(row["last_price_usd"])

    def test_cell_none_skips_record(self):
        self.patch(sentinel, "narrow_query", lambda *a, **k: [_row(400.0)])
        price, used = sentinel.check_cell(
            self.db, None, self.guard(), "japan_fall_2026",
            GROUPS["japan_fall_2026"], top_n=1, category="burst", slot=3,
            run_start="2026-09-24T21:00:00Z",
            depart_date="2026-12-01", duration=7)
        self.assertEqual(price, 400.0)
        n = self.db.execute("SELECT COUNT(*) AS c FROM cells").fetchone()["c"]
        self.assertEqual(n, 0)  # nothing recorded, nothing persisted as cell
        obs = self.db.execute(
            "SELECT COUNT(*) AS c FROM observations").fetchone()["c"]
        self.assertEqual(obs, 1)  # ... but the observation WAS persisted

    def test_budget_exhausted_propagates(self):
        def fake_nq(*a, **k):
            raise sentinel.BudgetExhausted("daily", "sentinel")

        self.patch(sentinel, "narrow_query", fake_nq)
        cell = self._seed_cell()
        with self.assertRaises(sentinel.BudgetExhausted):
            sentinel.check_cell(self.db, cell, self.guard(),
                                "japan_fall_2026", GROUPS["japan_fall_2026"],
                                top_n=1, category="sentinel", slot=1,
                                run_start=store.utcnow_iso())

    def _v2_stack_available(self):
        """Full billing path needs poller v2 + transport + budget v2."""
        try:
            import transport  # noqa: F401
            import poller
            import inspect
            p = inspect.signature(poller.narrow_query).parameters.get("top_n")
            if p is None or p.kind is not inspect.Parameter.KEYWORD_ONLY:
                return False
            return REAL_GUARD
        except Exception:
            return False

    def test_billing_fake_transport(self):
        if not self._v2_stack_available():
            self.skipTest("poller v2 + transport + budget v2 not assembled "
                          "yet; runs at assembly")
        import fake_transport
        guard = BudgetGuard(self.db, per_run_max=80, daily_ceiling=150)
        fake = fake_transport.install_fake_transport(guard, mode="ok")
        try:
            cell = self._seed_cell()
            rs = store.utcnow_iso()
            price, used = sentinel.check_cell(
                self.db, cell, guard, "x", {"origins": ["JFK"],
                                            "destinations": ["LHR"]},
                top_n=1, category="sentinel", slot=3, run_start=rs)
            # 1 outbound page + 1 expansion per top_n choice = 2 attempts
            self.assertEqual(fake.sends, 2)
            self.assertEqual(fake.categories, ["sentinel", "sentinel"])
            self.assertEqual(used, 2)
            self.assertIsNotNone(price)
            # verify billing goes to the verify ledger, never sentinel
            sentinel.check_cell(
                self.db, cell, guard, "x", {"origins": ["JFK"],
                                            "destinations": ["LHR"]},
                top_n=1, category="verify", slot=3, run_start=rs)
            day = store.today_str()
            self.assertEqual(
                store.count_today(self.db, day, "verify"), 2)
            self.assertEqual(
                store.count_today(self.db, day, "sentinel"), 2)
            self.assertEqual(fake.categories,
                             ["sentinel", "sentinel", "verify", "verify"])
        finally:
            fake_transport.uninstall_fake_transport()


# ---------------------------------------------------------------- bursts

class TestBurst(SentinelTestBase):
    def _trigger(self, price=400.0, kind="under_cap", fp="fp-burst"):
        depart = (date.fromisoformat(TODAY) + timedelta(days=100)).isoformat()
        tid = store.queue_trigger(
            self.db, kind=kind, group_id="japan_fall_2026",
            depart_week=store.depart_week(depart), depart_date=depart,
            duration=7, price_usd=price, fingerprint=fp, slot=7)
        trig = [t for t in store.due_triggers(self.db, 7)
                if t["id"] == tid][0]
        return trig

    def test_burst_dedup(self):
        trig = self._trigger()
        calls = []

        def fake_check(db, cell, guard, group_id, group_cfg, *, top_n,
                       category, slot, run_start=None):
            calls.append(category)
            return (400.0, 2)

        self.patch(sentinel, "check_cell", fake_check)
        used = sentinel.execute_burst(
            self.db, trig, self.guard(), GROUPS["japan_fall_2026"], slot=7,
            run_start=store.utcnow_iso())
        # 1 revalidate (verify) + 20 neighbours + 9 durations (burst)
        self.assertEqual(len(calls), 30)
        self.assertEqual(calls[0], "verify")
        self.assertTrue(all(c == "burst" for c in calls[1:]))
        self.assertEqual(used, 60)
        # trigger consumed: due_triggers excludes it (burst < 24h old)
        self.assertEqual(store.due_triggers(self.db, 7), [])
        trow = self.db.execute(
            "SELECT status FROM triggers WHERE id=?", (trig["id"],)).fetchone()
        self.assertEqual(trow["status"], "burst")
        brow = self.db.execute(
            "SELECT status, attempts FROM bursts WHERE trigger_id=?",
            (trig["id"],)).fetchone()
        self.assertEqual(brow["status"], "done")

    def test_revalidation_drops_dead_deal(self):
        trig = self._trigger()
        calls = []

        def fake_check(db, cell, guard, group_id, group_cfg, *, top_n,
                       category, slot, run_start=None):
            calls.append(category)
            return (None, 2)  # unknown -> deal is dead

        self.patch(sentinel, "check_cell", fake_check)
        used = sentinel.execute_burst(
            self.db, trig, self.guard(), GROUPS["japan_fall_2026"], slot=7,
            run_start=store.utcnow_iso())
        self.assertEqual(len(calls), 1)  # no neighbour checks
        self.assertEqual(used, 2)
        trow = self.db.execute(
            "SELECT status FROM triggers WHERE id=?", (trig["id"],)).fetchone()
        self.assertEqual(trow["status"], "dropped")
        brow = self.db.execute(
            "SELECT status FROM bursts WHERE trigger_id=?",
            (trig["id"],)).fetchone()
        self.assertEqual(brow["status"], "dropped")
        self.assertEqual(store.due_triggers(self.db, 7), [])

    def test_revalidation_drops_overpriced_deal(self):
        trig = self._trigger(price=400.0)

        def fake_check(db, cell, guard, group_id, group_cfg, *, top_n,
                       category, slot, run_start=None):
            return (450.0, 2)  # revalidated above the trigger price

        self.patch(sentinel, "check_cell", fake_check)
        sentinel.execute_burst(self.db, trig, self.guard(),
                               GROUPS["japan_fall_2026"], slot=7,
                               run_start=store.utcnow_iso())
        trow = self.db.execute(
            "SELECT status FROM triggers WHERE id=?", (trig["id"],)).fetchone()
        self.assertEqual(trow["status"], "dropped")

    def test_burst_partial_on_allowance(self):
        # v3 group 4: a partial burst is RESUMABLE, never terminal. The
        # trigger goes to "burst_active" (not "burst") and progress
        # persists so the next slot resumes at the right step.
        trig = self._trigger()

        def fake_check(db, cell, guard, group_id, group_cfg, *, top_n,
                       category, slot, run_start=None):
            guard.reserve(category)  # bill for real so pacing engages
            guard.reserve(category)
            return (400.0, 2)

        self.patch(sentinel, "check_cell", fake_check)
        self.patch(sentinel, "per_run_allowance", lambda guard, slot, *a: 4)
        used = sentinel.execute_burst(
            self.db, trig, self.guard(), GROUPS["japan_fall_2026"], slot=7,
            run_start=store.utcnow_iso())
        # revalidate (2) + 1 neighbour (2) = 4 -> allowance exhausted
        self.assertEqual(used, 4)
        brow = self.db.execute(
            "SELECT status, attempts FROM bursts WHERE trigger_id=?",
            (trig["id"],)).fetchone()
        self.assertEqual(brow["status"], "partial")
        self.assertEqual(brow["attempts"], 4)
        trow = self.db.execute(
            "SELECT status FROM triggers WHERE id=?", (trig["id"],)).fetchone()
        self.assertEqual(trow["status"], "burst_active")
        prog = store.get_burst_progress(self.db, trig["id"])
        self.assertEqual(prog["status"], "active")
        self.assertEqual(prog["attempts_used"], 4)
        self.assertEqual(prog["next_step"], 2)  # revalidate + 1 neighbour done
        # Still due: the next slot must resume it.
        due = [t["id"] for t in store.due_triggers(self.db, 8)]
        self.assertIn(trig["id"], due)


# ---------------------------------------------------------------- resumable bursts (v3 group 4)

class TestResumableBursts(SentinelTestBase):
    def _trigger(self, price=400.0, fp="fp-resume"):
        depart = (date.fromisoformat(TODAY) + timedelta(days=100)).isoformat()
        tid = store.queue_trigger(
            self.db, kind="under_cap", group_id="japan_fall_2026",
            depart_week=store.depart_week(depart), depart_date=depart,
            duration=7, price_usd=price, fingerprint=fp, slot=7)
        trig = [t for t in store.due_triggers(self.db, 7)
                if t["id"] == tid][0]
        return trig

    def _fake_check(self, price=400.0):
        def fake_check(db, cell, guard, group_id, group_cfg, *, top_n,
                       category, slot, run_start=None):
            guard.reserve(category)
            guard.reserve(category)
            return (price, 2)
        return fake_check

    def _run_burst(self, trig, allowance):
        self.patch(sentinel, "check_cell", self._fake_check())
        self.patch(sentinel, "per_run_allowance", lambda guard, slot, *a: allowance)
        return sentinel.execute_burst(
            self.db, trig, self.guard(), GROUPS["japan_fall_2026"], slot=7,
            run_start=store.utcnow_iso())

    def test_partial_persists_progress(self):
        """A partial run persists attempts_used / next_step / active."""
        trig = self._trigger()
        used = self._run_burst(trig, allowance=4)
        self.assertEqual(used, 4)
        prog = store.get_burst_progress(self.db, trig["id"])
        self.assertEqual((prog["attempts_used"], prog["next_step"],
                          prog["status"]), (4, 2, "active"))

    def test_next_slot_resumes_same_burst(self):
        """The next slot resumes at the persisted step; completed steps
        are not repeated (no duplicate work)."""
        trig = self._trigger()
        self._run_burst(trig, allowance=4)  # steps 0..1 done
        seen = []

        def fake_check(db, cell, guard, group_id, group_cfg, *, top_n,
                       category, slot, run_start=None):
            seen.append((cell["depart_date"], cell["duration"]))
            return (400.0, 2)

        self.patch(sentinel, "check_cell", fake_check)
        # generous allowance: finish everything remaining (28 steps x 2)
        self.patch(sentinel, "per_run_allowance", lambda guard, slot, *a: 60)
        trig2 = [t for t in store.due_triggers(self.db, 8)
                 if t["id"] == trig["id"]][0]
        used = sentinel.execute_burst(
            self.db, trig2, self.guard(), GROUPS["japan_fall_2026"], slot=8,
            run_start=store.utcnow_iso())
        self.assertEqual(used, 56)  # 28 remaining steps x 2 attempts
        # No step repeated: 28 distinct (date, duration) pairs.
        self.assertEqual(len(seen), 28)
        self.assertEqual(len(set(seen)), 28)
        prog = store.get_burst_progress(self.db, trig["id"])
        self.assertEqual(prog["status"], "done")
        self.assertEqual(prog["attempts_used"], 60)
        trow = self.db.execute(
            "SELECT status FROM triggers WHERE id=?", (trig["id"],)).fetchone()
        self.assertEqual(trow["status"], "burst")
        # Consumed: no longer due.
        self.assertNotIn(trig["id"],
                         [t["id"] for t in store.due_triggers(self.db, 9)])

    def test_cumulative_attempts_cap_at_60(self):
        """Cumulative attempts stop at 60 even across many partial slots."""
        trig = self._trigger()
        total = 0
        for slot in (7, 8, 9, 10, 11):
            trig_n = [t for t in store.due_triggers(self.db, slot)
                      if t["id"] == trig["id"]]
            if not trig_n:
                break
            total += self._run_burst(trig_n[0], allowance=14)
        prog = store.get_burst_progress(self.db, trig["id"])
        # 14+14+14+14+4 = 60: the last slot stops mid-allowance at the cap.
        self.assertEqual(prog["attempts_used"], 60)
        self.assertEqual(prog["status"], "done")
        self.assertEqual(total, 60)
        trow = self.db.execute(
            "SELECT status FROM triggers WHERE id=?", (trig["id"],)).fetchone()
        self.assertEqual(trow["status"], "burst")

    def test_expired_burst_not_resumed(self):
        """Progress past its 24h expiry is marked expired and never runs."""
        trig = self._trigger()
        self._run_burst(trig, allowance=4)
        past = (datetime.now(timezone.utc)
                - timedelta(hours=25)).isoformat()
        self.db.execute(
            "UPDATE burst_progress SET expires_at=? WHERE trigger_id=?",
            (past, trig["id"]))
        self.db.commit()
        calls = []

        def fake_check(db, cell, guard, group_id, group_cfg, *, top_n,
                       category, slot, run_start=None):
            calls.append(1)
            return (400.0, 2)

        self.patch(sentinel, "check_cell", fake_check)
        trig2 = [t for t in store.due_triggers(self.db, 8)
                 if t["id"] == trig["id"]]
        # due_triggers already excludes it once expired...
        self.assertEqual(trig2, [])
        # ...and a direct execute_burst call is a no-op that marks expiry.
        stale = dict(trig)
        used = sentinel.execute_burst(
            self.db, stale, self.guard(), GROUPS["japan_fall_2026"], slot=8,
            run_start=store.utcnow_iso())
        self.assertEqual(used, 0)
        self.assertEqual(calls, [])
        prog = store.get_burst_progress(self.db, trig["id"])
        self.assertEqual(prog["status"], "expired")
        trow = self.db.execute(
            "SELECT status FROM triggers WHERE id=?", (trig["id"],)).fetchone()
        self.assertEqual(trow["status"], "expired")

    def test_mid_query_budget_exhausted_keeps_partial_spend(self):
        """A transport allowance ending mid-query: the partial spend is
        folded into the cumulative total, the trigger stays resumable,
        and BudgetExhausted propagates so collect() stops cleanly."""
        from budget import BudgetExhausted
        trig = self._trigger()

        def fake_check(db, cell, guard, group_id, group_cfg, *, top_n,
                       category, slot, run_start=None):
            # Spend 1 attempt, then blow the budget mid-query.
            guard.reserve(category)
            raise BudgetExhausted("boom")

        self.patch(sentinel, "check_cell", fake_check)
        self.patch(sentinel, "per_run_allowance", lambda guard, slot, *a: 200)
        with self.assertRaises(BudgetExhausted):
            sentinel.execute_burst(
                self.db, trig, self.guard(), GROUPS["japan_fall_2026"],
                slot=7, run_start=store.utcnow_iso())
        prog = store.get_burst_progress(self.db, trig["id"])
        # The 1 partially-spent attempt is accounted for; the step itself
        # (next_step still 0) will be retried on resume.
        self.assertEqual(prog["attempts_used"], 1)
        self.assertEqual(prog["next_step"], 0)
        self.assertEqual(prog["status"], "active")
        trow = self.db.execute(
            "SELECT status FROM triggers WHERE id=?", (trig["id"],)).fetchone()
        self.assertEqual(trow["status"], "burst_active")
        brow = self.db.execute(
            "SELECT status FROM bursts WHERE trigger_id=?",
            (trig["id"],)).fetchone()
        self.assertEqual(brow["status"], "partial")


# ---------------------------------------------------------------- triggers

class TestTriggers(SentinelTestBase):
    def test_drop_trigger(self):
        depart = (date.fromisoformat(TODAY) + timedelta(days=100)).isoformat()
        store.ensure_cells(self.db, [
            {"group_id": "japan_fall_2026", "depart_date": depart,
             "duration": 7, "role": "sentinel", "weekday": 0}])
        cell = dict(self.db.execute(
            "SELECT * FROM cells WHERE role='sentinel'").fetchone())
        store.record_cell_check(self.db, cell["id"], slot=10, success=True,
                                price_usd=500.0)

        def fake_check(db, cell, guard, group_id, group_cfg, *, top_n,
                       category, slot, run_start=None):
            return (390.0, 2)  # 390 <= 0.8 * 500 -> drop

        self.patch(sentinel, "check_cell", fake_check)
        result = sentinel.collect(self.db, self.guard(), GROUPS, slot=11,
                                  run_start=store.utcnow_iso())
        self.assertEqual(result["drop_triggers"], 1)
        rows = self.db.execute(
            "SELECT * FROM triggers WHERE kind='drop'").fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["price_usd"], 390.0)
        self.assertEqual(rows[0]["group_id"], "japan_fall_2026")

    def test_no_drop_trigger_when_price_holds(self):
        depart = (date.fromisoformat(TODAY) + timedelta(days=100)).isoformat()
        store.ensure_cells(self.db, [
            {"group_id": "japan_fall_2026", "depart_date": depart,
             "duration": 7, "role": "sentinel", "weekday": 0}])
        cell = dict(self.db.execute(
            "SELECT * FROM cells WHERE role='sentinel'").fetchone())
        store.record_cell_check(self.db, cell["id"], slot=10, success=True,
                                price_usd=500.0)

        def fake_check(db, cell, guard, group_id, group_cfg, *, top_n,
                       category, slot, run_start=None):
            return (460.0, 2)  # 460 > 0.8 * 500 -> no drop

        self.patch(sentinel, "check_cell", fake_check)
        result = sentinel.collect(self.db, self.guard(), GROUPS, slot=11,
                                  run_start=store.utcnow_iso())
        self.assertEqual(result["drop_triggers"], 0)

    def test_detect_under_cap_triggers(self):
        run_start = "2026-09-24T22:00:00Z"
        fp_old = store.make_fingerprint("SFO", "NRT", "2026-12-01",
                                        "2026-12-08", "JL", 0)
        store.save_observations(self.db, "japan_fall_2026",
                                [_row(380.0, "2026-12-01", "2026-12-08")],
                                "2026-09-20T00:00:00Z")
        deals = [
            {"watch_id": "japan_fall_2026", "fingerprint": fp_old,
             "price_usd": 390.0, "depart_date": "2026-12-01", "duration": 7,
             "cap": 400.0},  # seen under cap before -> skipped
            {"watch_id": "japan_fall_2026", "fingerprint": "fp-new-xyz",
             "price_usd": 390.0, "depart_date": "2026-12-08", "duration": 7,
             "cap": 400.0},  # new -> queued
        ]
        n = sentinel.detect_under_cap_triggers(self.db, deals, run_start,
                                               slot=9)
        self.assertEqual(n, 1)
        rows = self.db.execute(
            "SELECT kind, fingerprint, group_id FROM triggers").fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["kind"], "under_cap")
        self.assertEqual(rows[0]["fingerprint"], "fp-new-xyz")
        # second call dedups (same kind/group/week/UTC-day)
        self.assertEqual(
            sentinel.detect_under_cap_triggers(self.db, deals, run_start,
                                               slot=9), 0)


if __name__ == "__main__":
    unittest.main()
