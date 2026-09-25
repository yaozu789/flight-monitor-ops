"""Unit tests for stage 02 (outbox v2 + scorer + verifier).

Covers the spec items: durable outbox rows, eligibility inside the UPDATE,
no double-increment of handoff_count, retry rechecks (unknown / gone /
confirmed with fresh render), absolute-cap-only scoring (no new_low
anywhere), guarded transitions, user-confirm audit, meaningful-drop dedup,
and old-schema backup/migrate/restore.

Run from the project root, e.g.:
    python -m unittest discover -s tests

No network, no credentials. poller.narrow_query is monkeypatched per test.
"""
import os
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path as _Path

_TESTS_DIR = _Path(__file__).resolve().parent
_PROJECT_ROOT = _TESTS_DIR.parent
sys.path.insert(0, str(_TESTS_DIR))  # test helpers (none here, harmless)
sys.path.insert(0, str(_PROJECT_ROOT))  # project modules

import store  # noqa: E402
import poller  # noqa: E402
import scorer  # noqa: E402
import verifier  # noqa: E402
import outbox  # noqa: E402

assert store.__file__.startswith(str(_PROJECT_ROOT)), \
    f"tests must exercise the project store.py, got {store.__file__}"

WATCH = {
    "watch_id": "w1",
    "origins": ["SFO"],
    "destinations": ["JFK"],
    "absolute_cap_usd": 500.0,
    "stops": [0, 1],
    "cabin": "economy",
}
DEP = "2026-10-01"
RET = "2026-10-08"
FP = store.make_fingerprint("SFO", "JFK", DEP, RET, "UA", 0)
RUN1 = "2026-09-24T20:00:00+00:00"
RUN2 = "2026-09-24T21:00:00+00:00"


def make_obs(price, run_start=RUN1, airline="UA", stops=0,
             origin="SFO", destination="JFK"):
    return {
        "origin": origin, "destination": destination,
        "depart_date": DEP, "return_date": RET,
        "airline": airline, "stops": stops, "cabin": "economy",
        "price_usd": price, "currency": "USD",
        "booking_url": f"https://example.com/book/{price}",
    }


def fresh_db(testcase):
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    os.unlink(path)
    db = store.init_db(store.get_db(path))

    def _cleanup():
        try:
            db.close()
        finally:
            for suffix in ("", "-wal", "-shm"):
                try:
                    os.unlink(path + suffix)
                except OSError:
                    pass
    testcase.addCleanup(_cleanup)
    return db, path


def seed_outbox_row(db, alert_id, price, message_text=None, watch_id="w1"):
    """Enqueue one durable pending row through the store contract."""
    return store.enqueue_alert(
        db, alert_id=alert_id, watch_id=watch_id, fingerprint=FP,
        price_usd=price, depart_date=DEP, return_date=RET,
        airline="UA", stops=0, booking_url="https://example.com/book",
        message_text=message_text or f"stale message price {price}",
        origin="SFO", destination="JFK")


def alert_row_dict(alert_id="w1:" + FP, price=460.0):
    """Synthetic outbox row (dict from store.outbox_get) for recheck tests."""
    return {
        "alert_id": alert_id, "watch_id": "w1", "fingerprint": FP,
        "origin": "SFO", "destination": "JFK", "price_usd": price,
        "depart_date": DEP, "return_date": RET, "airline": "UA", "stops": 0,
        "booking_url": "https://example.com/book",
    }


def make_spy(rows, calls):
    def spy(origin, destination, depart_date, return_date, cfg,
            top_n=3, attempt_category="manual"):
        calls.append({
            "attempt_category": attempt_category,
            "origin": origin, "destination": destination,
            "depart_date": depart_date, "return_date": return_date,
            "cfg": cfg, "top_n": top_n,
        })
        return rows
    return spy


def patch_narrow_query(testcase, rows):
    calls = []
    original = poller.narrow_query
    poller.narrow_query = make_spy(rows, calls)
    testcase.addCleanup(setattr, poller, "narrow_query", original)
    return calls


class ScorerTests(unittest.TestCase):
    def test_first_run_under_cap_yields_absolute_candidate(self):
        db, _ = fresh_db(self)
        store.save_observations(db, "w1", [make_obs(495)], RUN1)
        cands = scorer.score_new_observations(db, WATCH, RUN1)
        self.assertEqual(len(cands), 1)
        c = cands[0]
        self.assertEqual(c["reason"], "absolute")
        self.assertEqual(c["price_usd"], 495)
        self.assertEqual(c["fingerprint"], FP)
        self.assertEqual(c["watch_id"], "w1")
        self.assertNotIn("new_low", str(cands))

    def test_prior_and_this_run_above_cap_yields_none(self):
        db, _ = fresh_db(self)
        store.save_observations(db, "w1", [make_obs(495)], RUN1)
        store.save_observations(db, "w1", [make_obs(504)], RUN2)
        cands = scorer.score_new_observations(db, WATCH, RUN2)
        self.assertEqual(cands, [])

    def test_second_run_above_cap_yields_none(self):
        db, _ = fresh_db(self)
        store.save_observations(db, "w1", [make_obs(495)], RUN1)
        self.assertEqual(
            len(scorer.score_new_observations(db, WATCH, RUN1)), 1)
        store.save_observations(db, "w1", [make_obs(504)], RUN2)
        self.assertEqual(scorer.score_new_observations(db, WATCH, RUN2), [])

    def test_cooldown_blocks_recandidate_after_add_alert(self):
        db, _ = fresh_db(self)
        store.save_observations(db, "w1", [make_obs(495)], RUN1)
        self.assertEqual(
            len(scorer.score_new_observations(db, WATCH, RUN1)), 1)
        # v2-style add_alert resolves fingerprint -> the observation row.
        store.add_alert(db, fingerprint=FP, reason="absolute",
                        watch_id="w1", price_usd=495.0,
                        depart_date=DEP, return_date=RET,
                        airline="UA", stops=0,
                        booking_url="https://example.com/book",
                        observed_at=RUN1)
        self.assertTrue(store.alerted_recently(db, FP, 24))
        self.assertEqual(scorer.score_new_observations(db, WATCH, RUN1), [])

    def test_drop_above_cap_produces_nothing_and_no_new_low(self):
        db, _ = fresh_db(self)
        store.save_observations(db, "w1", [make_obs(600)], RUN1)
        store.save_observations(db, "w1", [make_obs(550)], RUN2)
        cands = scorer.score_new_observations(db, WATCH, RUN2)
        self.assertEqual(cands, [])
        self.assertNotIn("new_low", str(cands))
        self.assertNotIn("new_low", str(scorer.score_new_observations(
            db, WATCH, RUN1)))


class VerifierTests(unittest.TestCase):
    def _candidate(self, price=495.0):
        return {
            "watch_id": "w1", "fingerprint": FP,
            "origin": "SFO", "destination": "JFK",
            "depart_date": DEP, "return_date": RET,
            "airline": "UA", "stops": 0, "price_usd": price,
            "booking_url": "https://example.com/book",
            "observed_at": RUN1, "reason": "absolute",
        }

    def test_verify_confirms_fingerprint_match_under_cap_v1_price_key(self):
        db, _ = fresh_db(self)
        # v1 poller shape uses "price" instead of "price_usd".
        rows = [{"origin": "SFO", "destination": "JFK", "airline": "UA",
                 "stops": 0, "price": 480.0, "currency": "USD",
                 "booking_url": "https://example.com/fresh",
                 "observed_at": RUN2}]
        calls = patch_narrow_query(self, rows)
        ok, conf = verifier.verify(db, WATCH, self._candidate(), None)
        self.assertTrue(ok)
        self.assertEqual(conf["price_usd"], 480.0)
        self.assertEqual(conf["depart_date"], DEP)
        self.assertEqual(conf["return_date"], RET)
        self.assertEqual(conf["airline"], "UA")
        self.assertEqual(conf["stops"], 0)
        self.assertEqual(conf["booking_url"], "https://example.com/fresh")
        self.assertEqual(calls[0]["attempt_category"], "verify")

    def test_verify_unknown_when_no_match(self):
        db, _ = fresh_db(self)
        rows = [{"origin": "SFO", "destination": "JFK", "airline": "DL",
                 "stops": 1, "price_usd": 480.0, "currency": "USD",
                 "booking_url": "https://example.com/other",
                 "observed_at": RUN2}]
        patch_narrow_query(self, rows)
        ok, conf = verifier.verify(db, WATCH, self._candidate(), None)
        self.assertEqual((ok, conf), (False, None))

    def test_verify_unknown_when_no_results(self):
        db, _ = fresh_db(self)
        patch_narrow_query(self, [])
        ok, conf = verifier.verify(db, WATCH, self._candidate(), None)
        self.assertEqual((ok, conf), (False, None))

    def test_recheck_unknown_leaves_row_alone(self):
        db, _ = fresh_db(self)
        calls = patch_narrow_query(self, [])
        status, conf = verifier.recheck_alert_detailed(
            alert_row_dict(), db, WATCH, None)
        self.assertEqual((status, conf), ("unknown", None))
        self.assertEqual(calls[0]["attempt_category"], "retry_reserve")

    def test_recheck_gone_when_fare_disappeared(self):
        db, _ = fresh_db(self)
        rows = [{"origin": "SFO", "destination": "JFK", "airline": "UA",
                 "stops": 0, "price_usd": 620.0, "currency": "USD",
                 "booking_url": "https://example.com/gone",
                 "observed_at": RUN2}]
        calls = patch_narrow_query(self, rows)
        status, conf = verifier.recheck_alert_detailed(
            alert_row_dict(), db, WATCH, None)
        self.assertEqual((status, conf), ("gone", None))
        self.assertEqual(calls[0]["attempt_category"], "retry_reserve")

    def test_recheck_confirmed_returns_fresh_confirmation(self):
        db, _ = fresh_db(self)
        rows = [{"origin": "SFO", "destination": "JFK", "airline": "UA",
                 "stops": 0, "price_usd": 490.0, "currency": "USD",
                 "booking_url": "https://example.com/fresh",
                 "observed_at": RUN2}]
        patch_narrow_query(self, rows)
        status, conf = verifier.recheck_alert_detailed(
            alert_row_dict(price=460.0), db, WATCH, None)
        self.assertEqual(status, "confirmed")
        self.assertEqual(conf["price_usd"], 490.0)
        self.assertEqual(conf["booking_url"], "https://example.com/fresh")
        self.assertEqual(conf["observed_at"], RUN2)


class RenderTests(unittest.TestCase):
    def test_render_includes_required_fields_and_never_bookable(self):
        confirmation = {
            "price_usd": 490.0, "depart_date": DEP, "return_date": RET,
            "airline": "UA", "stops": 0,
            "booking_url": "https://example.com/fresh",
            "observed_at": RUN2,
        }
        text = outbox.render_alert_message(WATCH, confirmation, FP, RUN1)
        self.assertIn("SFO", text)
        self.assertIn("JFK", text)
        self.assertIn(DEP, text)
        self.assertIn(RET, text)
        self.assertIn("UA", text)
        self.assertIn("nonstop", text)
        self.assertIn("490", text)
        self.assertIn("https://example.com/fresh", text)
        self.assertIn("rechecked at", text)
        self.assertIn(RUN2, text)
        self.assertIn(RUN1, text)
        self.assertNotIn("bookable", text.lower())


class OutboxEnqueueTests(unittest.TestCase):
    def test_enqueue_inserts_durable_pending_row(self):
        db, path = fresh_db(self)
        alert_id = outbox.make_alert_id("w1", FP)
        self.assertEqual(alert_id, f"w1:{FP}")
        aid, action = seed_outbox_row(db, alert_id, 495.0)
        self.assertEqual((aid, action), (alert_id, "inserted"))
        # Visible from a second connection: the row is durable, not a cache.
        db2 = store.get_db(path)
        self.addCleanup(db2.close)
        row = store.outbox_get(db2, alert_id)
        self.assertIsNotNone(row)
        self.assertEqual(row["state"], "pending")
        self.assertEqual(row["price_usd"], 495.0)
        self.assertIn("495", row["message_text"])

    def test_prepare_eligibility_inside_update(self):
        db, _ = fresh_db(self)
        aid_pending = outbox.make_alert_id("w1", FP + "a")
        aid_prepared = outbox.make_alert_id("w1", FP + "b")
        seed_outbox_row(db, aid_pending, 495.0)
        seed_outbox_row(db, aid_prepared, 490.0)
        store.prepare_handoff(db, 100, [aid_prepared])
        before = store.outbox_get(db, aid_prepared)["handoff_count"]
        self.assertEqual(before, 1)

        prepared = store.prepare_handoff(db, 101, [aid_pending, aid_prepared])
        self.assertEqual([r["alert_id"] for r in prepared], [aid_pending])

        row_a = store.outbox_get(db, aid_pending)
        self.assertEqual(row_a["state"], "handoff_prepared")
        self.assertEqual(row_a["handoff_count"], 1)
        self.assertEqual(row_a["prepared_slot"], 101)
        row_b = store.outbox_get(db, aid_prepared)
        self.assertEqual(row_b["handoff_count"], 1)  # not re-incremented
        self.assertEqual(row_b["prepared_slot"], 100)

        # Preparing the same ids again must not double-increment.
        store.prepare_handoff(db, 102, [aid_pending, aid_prepared])
        self.assertEqual(
            store.outbox_get(db, aid_pending)["handoff_count"], 1)
        self.assertEqual(
            store.outbox_get(db, aid_prepared)["handoff_count"], 1)

    def test_drain_pending_prepares_all_pending_rows(self):
        db, _ = fresh_db(self)
        ids = [outbox.make_alert_id("w1", FP + s) for s in ("c", "d")]
        for aid in ids:
            seed_outbox_row(db, aid, 495.0)
        batch = outbox.drain_pending(db, 200)
        self.assertEqual({r["alert_id"] for r in batch}, set(ids))
        self.assertEqual(store.outbox_list(db, "pending"), [])


class OutboxRetryTests(unittest.TestCase):
    def _seed_prepared(self, db, slot, alert_id=None, price=460.0):
        alert_id = alert_id or outbox.make_alert_id("w1", FP)
        seed_outbox_row(db, alert_id, price)
        store.prepare_handoff(db, slot, [alert_id])
        return alert_id

    def test_retry_unknown_row_stays_handoff_prepared(self):
        db, _ = fresh_db(self)
        aid = self._seed_prepared(db, 100)
        patch_narrow_query(self, [])
        result = outbox.recheck_retries(db, [WATCH], None, 103)
        self.assertEqual(result, {"retry_ids": [], "retired": [], "unknown": [aid]})
        row = store.outbox_get(db, aid)
        self.assertEqual(row["state"], "handoff_prepared")  # not retired
        self.assertEqual(row["handoff_count"], 1)            # not re-prepared

    def test_retry_gone_retires(self):
        db, _ = fresh_db(self)
        aid = self._seed_prepared(db, 100)
        patch_narrow_query(self, [{"origin": "SFO", "destination": "JFK",
                                   "airline": "UA", "stops": 0,
                                   "price_usd": 620.0, "currency": "USD",
                                   "booking_url": "https://example.com/gone",
                                   "observed_at": RUN2}])
        result = outbox.recheck_retries(db, [WATCH], None, 103)
        self.assertEqual(result, {"retry_ids": [], "retired": [aid], "unknown": []})
        self.assertEqual(store.outbox_get(db, aid)["state"], "retired")

    def test_retry_confirmed_rerenders_fresh_message(self):
        db, _ = fresh_db(self)
        aid = self._seed_prepared(db, 100, price=460.0)
        store.save_observations(db, "w1", [make_obs(460.0)], RUN1)
        patch_narrow_query(self, [{"origin": "SFO", "destination": "JFK",
                                   "airline": "UA", "stops": 0,
                                   "price_usd": 490.0, "currency": "USD",
                                   "booking_url": "https://example.com/fresh",
                                   "observed_at": RUN2}])
        result = outbox.recheck_retries(db, [WATCH], None, 103)
        self.assertEqual(result["retry_ids"], [aid])
        row = store.outbox_get(db, aid)
        self.assertIn("490", row["message_text"])       # fresh recheck price
        self.assertNotIn("460", row["message_text"])   # stale price replaced
        self.assertEqual(row["price_usd"], 490.0)


class OutboxTransitionTests(unittest.TestCase):
    def _mk(self, db, suffix, state):
        aid = outbox.make_alert_id("w1", FP + suffix)
        seed_outbox_row(db, aid, 495.0)
        if state != "pending":
            if state in ("handoff_prepared", "relay_attempted_unconfirmed"):
                store.prepare_handoff(db, 100, [aid])
            if state == "relay_attempted_unconfirmed":
                self.assertTrue(
                    store.update_outbox_state(
                        db, aid, "relay_attempted_unconfirmed", actor="agent"))
            elif state not in ("handoff_prepared", "relay_attempted_unconfirmed"):
                self.assertTrue(
                    store.update_outbox_state(db, aid, state))
        return aid

    def test_transition_guards(self):
        db, _ = fresh_db(self)
        # scheduler may never set user_confirmed via update_outbox_state.
        self.assertFalse(store.update_outbox_state(
            db, self._mk(db, "t1", "pending"), "user_confirmed"))
        # pending may not skip straight to relay_attempted_unconfirmed.
        self.assertFalse(store.update_outbox_state(
            db, self._mk(db, "t2", "pending"), "relay_attempted_unconfirmed"))
        # pending -> handoff_prepared is legal.
        self.assertTrue(store.update_outbox_state(
            db, self._mk(db, "t3", "pending"), "handoff_prepared"))
        # handoff_prepared -> relay_attempted_unconfirmed is legal.
        aid = self._mk(db, "t4", "handoff_prepared")
        self.assertTrue(store.update_outbox_state(
            db, aid, "relay_attempted_unconfirmed"))
        # user_confirmed is terminal: nothing leaves it.
        aid = self._mk(db, "t5", "relay_attempted_unconfirmed")
        self.assertTrue(store.set_outbox_user_confirmed(db, aid, "yaozu"))
        self.assertFalse(store.update_outbox_state(db, aid, "retired"))
        self.assertFalse(store.update_outbox_state(db, aid, "pending"))
        self.assertFalse(store.update_outbox_state(db, aid, "handoff_prepared"))

    def test_relay_attempted_unconfirmed_cannot_self_confirm_via_state(self):
        db, _ = fresh_db(self)
        aid = self._mk(db, "t6", "relay_attempted_unconfirmed")
        self.assertFalse(store.update_outbox_state(
            db, aid, "user_confirmed", actor="scheduler"))

    def test_user_confirm_records_actor_and_is_idempotent(self):
        db, _ = fresh_db(self)
        aid = self._mk(db, "t7", "relay_attempted_unconfirmed")
        self.assertTrue(store.set_outbox_user_confirmed(db, aid, "yaozu"))
        row = store.outbox_get(db, aid)
        self.assertEqual(row["state"], "user_confirmed")
        self.assertEqual(row["confirmed_by"], "yaozu")
        self.assertFalse(store.set_outbox_user_confirmed(db, aid, "yaozu"))


class OutboxDedupTests(unittest.TestCase):
    def test_meaningful_drop_dedup(self):
        db, _ = fresh_db(self)
        aid = outbox.make_alert_id("w1", FP)
        _, a1 = seed_outbox_row(db, aid, 400.0)
        self.assertEqual(a1, "inserted")
        store.prepare_handoff(db, 100, [aid])

        # $5 below the last outboxed price: below the max($15, 5%) bar.
        _, a2 = seed_outbox_row(db, aid, 395.0)
        self.assertEqual(a2, "deduped")
        row = store.outbox_get(db, aid)
        self.assertEqual(row["state"], "handoff_prepared")
        self.assertEqual(row["price_usd"], 400.0)

        # $20 below (== max($15, 5% of 400)): re-arms to pending.
        _, a3 = seed_outbox_row(db, aid, 380.0)
        self.assertEqual(a3, "rearmed")
        row = store.outbox_get(db, aid)
        self.assertEqual(row["state"], "pending")
        self.assertEqual(row["price_usd"], 380.0)


class MigrationTests(unittest.TestCase):
    def test_old_schema_backup_migrate_restore(self):
        workdir = tempfile.mkdtemp()
        self.addCleanup(lambda: __import__("shutil").rmtree(workdir, True))
        old_path = os.path.join(workdir, "old.db")
        backup_path = os.path.join(workdir, "backup.db")
        restored_path = os.path.join(workdir, "restored.db")

        # Build the old (Phase-1) schema with sqlite3 directly.
        src = sqlite3.connect(old_path)
        src.execute("""CREATE TABLE outbox (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            alert_id TEXT NOT NULL UNIQUE,
            state TEXT NOT NULL,
            message_text TEXT NOT NULL,
            handoffs INTEGER NOT NULL DEFAULT 0,
            last_handoff_slot INTEGER,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL)""")
        src.execute("""CREATE TABLE run_slots (
            slot_id INTEGER NOT NULL,
            command TEXT NOT NULL,
            PRIMARY KEY (slot_id, command))""")
        now = store.utcnow_iso()
        src.execute(
            "INSERT INTO outbox (alert_id, state, message_text, handoffs,"
            " created_at, updated_at) VALUES (?,?,?,?,?,?)",
            ("w1:fp1", "pending", "hello", 0, now, now))
        src.execute("INSERT INTO run_slots (slot_id, command) VALUES (?,?)",
                    (123, "check"))
        src.commit()

        # Backup via the sqlite3 Connection.backup API.
        dst = sqlite3.connect(backup_path)
        with dst:
            src.backup(dst)
        dst.close()

        # Migrate the original: new columns appear, old data intact.
        mdb = store.get_db(old_path)
        store.migrate_db(mdb)
        store.init_db(mdb)
        cols = {r["name"] for r in mdb.execute("PRAGMA table_info(outbox)")}
        for c in ("origin", "destination", "last_price_usd", "confirmed_by",
                  "handoff_count", "prepared_slot", "prepared_at",
                  "price_usd", "depart_date", "return_date", "airline",
                  "stops", "booking_url"):
            self.assertIn(c, cols)
        row = mdb.execute(
            "SELECT alert_id, state, message_text FROM outbox"
            " WHERE alert_id='w1:fp1'").fetchone()
        self.assertEqual(tuple(row), ("w1:fp1", "pending", "hello"))
        rcols = {r["name"] for r in mdb.execute("PRAGMA table_info(run_slots)")}
        self.assertIn("claimed_at", rcols)  # recreated keyed by slot_id alone
        mdb.close()
        src.close()

        # Restore the backup file into a third file: old schema/rows intact.
        bsrc = sqlite3.connect(backup_path)
        cdst = sqlite3.connect(restored_path)
        with cdst:
            bsrc.backup(cdst)
        bsrc.close()
        cdst.close()
        cdb = sqlite3.connect(restored_path)
        cdb.row_factory = sqlite3.Row
        ccols = {r["name"] for r in cdb.execute("PRAGMA table_info(outbox)")}
        self.assertNotIn("last_price_usd", ccols)
        self.assertNotIn("origin", ccols)
        crow = cdb.execute(
            "SELECT alert_id, state, message_text FROM outbox"
            " WHERE alert_id='w1:fp1'").fetchone()
        self.assertEqual(tuple(crow), ("w1:fp1", "pending", "hello"))
        rinfo = {r[1] for r in cdb.execute("PRAGMA table_info(run_slots)")}
        self.assertNotIn("claimed_at", rinfo)
        self.assertIn("command", rinfo)
        cdb.close()


if __name__ == "__main__":
    unittest.main()
