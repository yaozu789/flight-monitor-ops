"""Unit tests for stage 02 (outbox v2 + scorer + verifier).

Covers the spec items: durable outbox rows, eligibility inside the UPDATE,
no double-increment of handoff_count, retry rechecks (unknown / gone /
confirmed with fresh render), absolute-cap-only scoring (no new_low
anywhere), guarded transitions, user-confirm audit, meaningful-drop dedup,
and old-schema backup/migrate/restore.

v3 additions (2026-09-25): richer fingerprints (flight numbers + departure
times, Phase-1-compatible when absent), (watch, departure week) alert
identity, handoff-count cap + slot gap inside prepare_handoff, pruned
scheduler transition map, separate guarded retirement, human confirmation
only from handed-off states, unknown-counter retirement on the third
consecutive unknown, and full scorer -> verifier -> enqueue pipeline tests
including the $495 -> $504 over-cap scenario.

Run from the project root, e.g.:
    python -m unittest discover -s tests

No network, no credentials. poller.narrow_query is monkeypatched per test.
"""
import os
import json
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
from budget import BudgetGuard  # noqa: E402
from fake_transport import (  # noqa: E402
    install_fake_transport,
    uninstall_fake_transport,
)

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
WEEK = store.depart_week(DEP)  # "2026-W40"
AID = f"w1:{WEEK}"
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


def alert_row_dict(alert_id=AID, price=460.0):
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

    def test_verify_rejects_recheck_over_cap(self):
        """$495 candidate, recheck comes back $504 over the $500 cap:
        verify() must NOT confirm (no alert), even though the scorer
        passed the candidate."""
        db, _ = fresh_db(self)
        rows = [{"origin": "SFO", "destination": "JFK", "airline": "UA",
                 "stops": 0, "price_usd": 504.0, "currency": "USD",
                 "booking_url": "https://example.com/overcap",
                 "observed_at": RUN2}]
        calls = patch_narrow_query(self, rows)
        ok, conf = verifier.verify(db, WATCH, self._candidate(price=495.0),
                                    None)
        self.assertEqual((ok, conf), (False, None))
        self.assertEqual(calls[0]["attempt_category"], "verify")

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
        # v3 alert identity: (watch, departure week).
        alert_id = outbox.make_alert_id("w1", WEEK)
        self.assertEqual(alert_id, f"w1:{WEEK}")
        self.assertEqual(alert_id, AID)
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
        aid_pending = outbox.make_alert_id("w1", "2026-W40")
        aid_prepared = outbox.make_alert_id("w1", "2026-W41")
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
        ids = [outbox.make_alert_id("w1", f"2026-W{42 + i}") for i in range(2)]
        for aid in ids:
            seed_outbox_row(db, aid, 495.0)
        batch = outbox.drain_pending(db, 200)
        self.assertEqual({r["alert_id"] for r in batch}, set(ids))
        self.assertEqual(store.outbox_list(db, "pending"), [])


class OutboxRetryTests(unittest.TestCase):
    def _seed_prepared(self, db, slot, alert_id=None, price=460.0):
        alert_id = alert_id or outbox.make_alert_id("w1", WEEK)
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
        # Stale price replaced in the headline (the history section may
        # legitimately list the earlier $460 observation, dated).
        self.assertNotIn("460", row["message_text"].splitlines()[0])
        self.assertEqual(row["price_usd"], 490.0)


class OutboxTransitionTests(unittest.TestCase):
    def _mk(self, db, suffix, state):
        # Distinct weekly alert slots per suffix (v3 identity).
        aid = outbox.make_alert_id("w1", f"2026-W{40 + int(suffix[1:])}")
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
        aid = outbox.make_alert_id("w1", WEEK)
        self.assertEqual(aid, AID)
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
                  "stops", "booking_url", "unknown_count"):  # v3
            self.assertIn(c, cols)
        # v3 observation leg columns arrive idempotently too.
        ocols = {r["name"] for r in mdb.execute("PRAGMA table_info(observations)")}
        for c in ("flight_numbers", "depart_times"):
            self.assertIn(c, ocols)
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


class FingerprintV3Tests(unittest.TestCase):
    """Richer fingerprints: leg fields join the hash; absent fields keep the
    Phase-1 scheme byte-identical (old rows keep their hashes)."""
    FNS = ("UA1107", "UA100")
    DTS = ("2026-10-01T09:45:00", "2026-10-08T19:15:00")

    def test_absent_leg_fields_keep_phase1_scheme(self):
        self.assertEqual(
            store.make_fingerprint("SFO", "JFK", DEP, RET, "UA", 0), FP)
        self.assertEqual(
            store.make_fingerprint("SFO", "JFK", DEP, RET, "UA", 0, None, None),
            FP)
        self.assertEqual(
            store.make_fingerprint("SFO", "JFK", DEP, RET, "UA", 0, [], []),
            FP)

    def test_leg_fields_change_fingerprint(self):
        rich = store.make_fingerprint("SFO", "JFK", DEP, RET, "UA", 0,
                                      list(self.FNS), list(self.DTS))
        self.assertNotEqual(rich, FP)
        # Different flight numbers -> different fingerprint.
        other_fn = store.make_fingerprint(
            "SFO", "JFK", DEP, RET, "UA", 0,
            ["UA999", "UA100"], list(self.DTS))
        self.assertNotEqual(rich, other_fn)
        # Different departure times -> different fingerprint.
        other_dt = store.make_fingerprint(
            "SFO", "JFK", DEP, RET, "UA", 0,
            list(self.FNS), ["2026-10-01T10:45:00", "2026-10-08T19:15:00"])
        self.assertNotEqual(rich, other_dt)
        # Same leg fields -> deterministic, identical fingerprint.
        self.assertEqual(
            rich, store.make_fingerprint("SFO", "JFK", DEP, RET, "UA", 0,
                                         list(self.FNS), list(self.DTS)))

    def test_observations_persist_leg_fields_and_rich_fingerprint(self):
        db, _ = fresh_db(self)
        rich_fp = store.make_fingerprint(
            "SFO", "JFK", DEP, RET, "UA", 0, list(self.FNS), list(self.DTS))
        oid = store.add_observation(
            db, watch_id="w1", origin="SFO", destination="JFK",
            depart_date=DEP, return_date=RET, airline="UA", stops=0,
            cabin="economy", price_usd=495.0,
            booking_url="https://example.com/book",
            flight_numbers=list(self.FNS), depart_times=list(self.DTS))
        row = db.execute(
            "SELECT fingerprint, flight_numbers, depart_times FROM observations"
            " WHERE id=?", (oid,)).fetchone()
        self.assertEqual(row["fingerprint"], rich_fp)
        self.assertNotEqual(row["fingerprint"], FP)
        self.assertEqual(json.loads(row["flight_numbers"]), list(self.FNS))
        self.assertEqual(json.loads(row["depart_times"]), list(self.DTS))

    def test_old_rows_keep_null_leg_columns(self):
        db, _ = fresh_db(self)
        oid = store.add_observation(
            db, watch_id="w1", origin="SFO", destination="JFK",
            depart_date=DEP, return_date=RET, airline="UA", stops=0,
            cabin="economy", price_usd=495.0,
            booking_url="https://example.com/book")
        row = db.execute(
            "SELECT fingerprint, flight_numbers, depart_times FROM observations"
            " WHERE id=?", (oid,)).fetchone()
        self.assertEqual(row["fingerprint"], FP)
        self.assertIsNone(row["flight_numbers"])
        self.assertIsNone(row["depart_times"])


class AlertIdentityV3Tests(unittest.TestCase):
    def test_alert_id_is_watch_and_departure_week(self):
        self.assertEqual(WEEK, "2026-W40")
        self.assertEqual(outbox.make_alert_id("w1", WEEK), "w1:2026-W40")
        # Same week, different deal -> same alert slot.
        self.assertEqual(outbox.make_alert_id("w1", store.depart_week("2026-10-02")),
                         outbox.make_alert_id("w1", store.depart_week(DEP)))
        # Different week -> different alert slot.
        self.assertNotEqual(
            outbox.make_alert_id("w1", store.depart_week("2026-10-12")),
            outbox.make_alert_id("w1", WEEK))

    def test_weekly_dedup_across_different_flights(self):
        db, _ = fresh_db(self)
        aid = outbox.make_alert_id("w1", WEEK)
        # First deal of the week inserts.
        _, a1 = seed_outbox_row(db, aid, 400.0)
        self.assertEqual(a1, "inserted")
        # A different flight the same week is the SAME alert slot: only a
        # meaningful drop re-arms it.
        _, a2 = store.enqueue_alert(
            db, alert_id=aid, watch_id="w1",
            fingerprint=store.make_fingerprint(
                "SFO", "JFK", DEP, RET, "UA", 0, ["UA999"], ["2026-10-01T10:45"]),
            price_usd=395.0, depart_date=DEP, return_date=RET,
            airline="UA", stops=0, booking_url="https://example.com/other",
            message_text="other flight", origin="SFO", destination="JFK")
        self.assertEqual(a2, "deduped")
        self.assertEqual(store.outbox_get(db, aid)["price_usd"], 400.0)


class PrepareHandoffV3Tests(unittest.TestCase):
    def _rearm(self, db, aid, price):
        _, action = store.enqueue_alert(
            db, alert_id=aid, watch_id="w1", fingerprint=FP,
            price_usd=price, depart_date=DEP, return_date=RET,
            airline="UA", stops=0, booking_url="https://example.com/x",
            message_text=f"deal at {price}", origin="SFO",
            destination="JFK")
        self.assertEqual(action, "rearmed")
        return store.outbox_get(db, aid)

    def test_handoff_count_cap_and_slot_gap_inside_update(self):
        db, _ = fresh_db(self)
        aid = outbox.make_alert_id("w1", "2026-W40")
        seed_outbox_row(db, aid, 495.0)
        store.prepare_handoff(db, 100, [aid])  # handoff_count = 1
        row = self._rearm(db, aid, 460.0)      # meaningful drop -> pending
        self.assertEqual(row["state"], "pending")
        self.assertEqual(row["handoff_count"], 1)

        # Slot 101: last handoff (slot 100) is inside the 3-slot gap.
        self.assertEqual(store.prepare_handoff(db, 101, [aid]), [])
        self.assertEqual(store.outbox_get(db, aid)["state"], "pending")
        self.assertEqual(store.outbox_get(db, aid)["handoff_count"], 1)

        # Slot 104: gap satisfied -> prepared, count becomes 2.
        prepared = store.prepare_handoff(db, 104, [aid])
        self.assertEqual([r["alert_id"] for r in prepared], [aid])
        row = store.outbox_get(db, aid)
        self.assertEqual(row["state"], "handoff_prepared")
        self.assertEqual(row["handoff_count"], 2)

        # Re-arm again, then prepare at slot 110: the count cap (2) blocks it.
        row = self._rearm(db, aid, 425.0)
        self.assertEqual(row["handoff_count"], 2)
        self.assertEqual(store.prepare_handoff(db, 110, [aid]), [])
        row = store.outbox_get(db, aid)
        self.assertEqual(row["state"], "pending")      # untouched
        self.assertEqual(row["handoff_count"], 2)      # not incremented

    def test_drain_respects_gap_and_cap(self):
        db, _ = fresh_db(self)
        aid = outbox.make_alert_id("w1", "2026-W40")
        seed_outbox_row(db, aid, 495.0)
        store.prepare_handoff(db, 100, [aid])
        self._rearm(db, aid, 460.0)
        # Drain inside the gap prepares nothing...
        self.assertEqual(outbox.drain_pending(db, 101), [])
        self.assertEqual(store.outbox_get(db, aid)["state"], "pending")
        # ...and past the gap it prepares.
        drained = outbox.drain_pending(db, 104)
        self.assertEqual([r["alert_id"] for r in drained], [aid])
        self.assertEqual(store.outbox_get(db, aid)["handoff_count"], 2)

    def test_retry_cap_is_two(self):
        db, _ = fresh_db(self)
        self.assertEqual(store.RETRY_MAX_ATTEMPTS, 2)
        aid = outbox.make_alert_id("w1", "2026-W40")
        seed_outbox_row(db, aid, 460.0)
        store.prepare_handoff(db, 100, [aid])  # count 1
        cands = store.eligible_retry_candidates(db, 103)
        self.assertEqual([r["alert_id"] for r in cands], [aid])
        store.reprepare_retries(db, 103, [aid])  # count 2
        self.assertEqual(store.eligible_retry_candidates(db, 106), [])


class TransitionMapV3Tests(unittest.TestCase):
    def test_scheduler_map_is_exact(self):
        self.assertEqual(store.OUTBOX_TRANSITIONS, {
            "pending": {"handoff_prepared"},
            "handoff_prepared": {"relay_attempted_unconfirmed"},
            "relay_attempted_unconfirmed": set(),
            "user_confirmed": set(),
            "retired": set(),
        })

    def test_retired_unreachable_via_update_outbox_state(self):
        db, _ = fresh_db(self)
        aid = outbox.make_alert_id("w1", "2026-W40")
        seed_outbox_row(db, aid, 495.0)
        self.assertFalse(store.update_outbox_state(db, aid, "retired"))
        store.prepare_handoff(db, 100, [aid])
        self.assertFalse(store.update_outbox_state(db, aid, "retired"))
        # ... but the separate guarded retirement works from handoff states.
        self.assertTrue(store.retire_outbox(db, aid, "test retirement"))
        self.assertEqual(store.outbox_get(db, aid)["state"], "retired")
        # Retiring twice is a no-op: retired rows stay retired.
        self.assertFalse(store.retire_outbox(db, aid, "again"))

    def test_retire_never_from_user_confirmed(self):
        db, _ = fresh_db(self)
        aid = outbox.make_alert_id("w1", "2026-W40")
        seed_outbox_row(db, aid, 495.0)
        store.prepare_handoff(db, 100, [aid])
        self.assertTrue(store.update_outbox_state(
            db, aid, "relay_attempted_unconfirmed"))
        self.assertTrue(store.set_outbox_user_confirmed(db, aid, "yaozu"))
        self.assertFalse(store.retire_outbox(db, aid, "must not retire"))
        self.assertEqual(store.outbox_get(db, aid)["state"], "user_confirmed")

    def test_pending_cannot_be_user_confirmed(self):
        db, _ = fresh_db(self)
        aid = outbox.make_alert_id("w1", "2026-W40")
        seed_outbox_row(db, aid, 495.0)
        self.assertFalse(store.set_outbox_user_confirmed(db, aid, "yaozu"))
        self.assertEqual(store.outbox_get(db, aid)["state"], "pending")
        # Handed-off states can be confirmed, with the actor recorded.
        store.prepare_handoff(db, 100, [aid])
        self.assertTrue(store.set_outbox_user_confirmed(db, aid, "yaozu"))
        row = store.outbox_get(db, aid)
        self.assertEqual(row["state"], "user_confirmed")
        self.assertEqual(row["confirmed_by"], "yaozu")


class UnknownRecheckV3Tests(unittest.TestCase):
    def _seed_prepared(self, db, slot, week="2026-W40"):
        aid = outbox.make_alert_id("w1", week)
        seed_outbox_row(db, aid, 460.0)
        store.prepare_handoff(db, slot, [aid])
        return aid

    def test_three_unknowns_retire(self):
        db, _ = fresh_db(self)
        aid = self._seed_prepared(db, 100)
        patch_narrow_query(self, [])  # unknown on every recheck
        r1 = outbox.recheck_retries(db, [WATCH], None, 103)
        self.assertEqual(r1, {"retry_ids": [], "retired": [], "unknown": [aid]})
        self.assertEqual(store.outbox_get(db, aid)["unknown_count"], 1)
        self.assertEqual(store.outbox_get(db, aid)["state"], "handoff_prepared")

        r2 = outbox.recheck_retries(db, [WATCH], None, 106)
        self.assertEqual(r2["unknown"], [aid])
        self.assertEqual(store.outbox_get(db, aid)["unknown_count"], 2)

        r3 = outbox.recheck_retries(db, [WATCH], None, 109)
        self.assertEqual(r3, {"retry_ids": [], "retired": [aid], "unknown": []})
        row = store.outbox_get(db, aid)
        self.assertEqual(row["state"], "retired")
        self.assertEqual(row["unknown_count"], 3)
        # Retired: no longer a retry candidate.
        self.assertEqual(store.eligible_retry_candidates(db, 112), [])

    def test_confirmed_recheck_clears_unknown_streak(self):
        db, _ = fresh_db(self)
        aid = self._seed_prepared(db, 100)
        patch_narrow_query(self, [])
        outbox.recheck_retries(db, [WATCH], None, 103)  # first unknown
        self.assertEqual(store.outbox_get(db, aid)["unknown_count"], 1)

        # A confirmed recheck re-renders the message...
        patch_narrow_query(self, [{"origin": "SFO", "destination": "JFK",
                                   "airline": "UA", "stops": 0,
                                   "price_usd": 460.0, "currency": "USD",
                                   "booking_url": "https://example.com/fresh",
                                   "observed_at": RUN2}])
        result = outbox.recheck_retries(db, [WATCH], None, 106)
        self.assertEqual(result["retry_ids"], [aid])
        # ...and the re-handoff clears the unknown streak.
        store.reprepare_retries(db, 106, result["retry_ids"])
        self.assertEqual(store.outbox_get(db, aid)["unknown_count"], 0)


class RelayableActionsTests(unittest.TestCase):
    def test_relayable_actions_exact(self):
        self.assertEqual(outbox.RELAYABLE_ACTIONS, ("inserted", "rearmed"))

    def test_deduped_candidate_not_relayable(self):
        db, _ = fresh_db(self)
        aid = outbox.make_alert_id("w1", WEEK)
        _, a1 = seed_outbox_row(db, aid, 400.0)
        self.assertEqual(a1, "inserted")
        self.assertIn(a1, outbox.RELAYABLE_ACTIONS)
        # $5 below the outboxed price: below the max($15, 5%) bar.
        _, a2 = seed_outbox_row(db, aid, 395.0)
        self.assertEqual(a2, "deduped")
        self.assertNotIn(a2, outbox.RELAYABLE_ACTIONS)
        # $20 below: re-arms and is relayable again.
        _, a3 = seed_outbox_row(db, aid, 380.0)
        self.assertEqual(a3, "rearmed")
        self.assertIn(a3, outbox.RELAYABLE_ACTIONS)


class FullPipelineV3Tests(unittest.TestCase):
    """Scorer -> verifier -> enqueue through the v3 weekly alert identity."""

    FNS = ("UA1107", "UA100")
    DTS = ("2026-10-01T09:45:00", "2026-10-08T19:15:00")

    def _obs(self, price, run_start=RUN1, flight_numbers=FNS,
             depart_times=DTS):
        o = make_obs(price, run_start)
        o["flight_numbers"] = list(flight_numbers)
        o["depart_times"] = list(depart_times)
        return o

    def _verify_rows(self, price, flight_numbers=FNS, depart_times=DTS):
        return [{"origin": "SFO", "destination": "JFK", "airline": "UA",
                 "stops": 0, "price_usd": price, "currency": "USD",
                 "booking_url": "https://example.com/verify",
                 "observed_at": RUN2,
                 "flight_numbers": list(flight_numbers),
                 "depart_times": list(depart_times)}]

    def _run_pipeline(self, db, run_start, verify_rows):
        """One scorer -> verifier -> enqueue pass.

        Returns (candidates, enqueue_actions, narrow_query_calls). Mirrors
        run.cmd_check step 5, including the alerts-table scoring history.
        """
        calls = patch_narrow_query(self, verify_rows)
        cands = scorer.score_new_observations(db, WATCH, run_start)
        actions = []
        for c in cands:
            ok, conf = verifier.verify(db, WATCH, c, None)
            if not ok:
                actions.append("unverified")
                continue
            store.add_alert(db, fingerprint=c["fingerprint"], reason="absolute",
                            verified_at=store.utcnow_iso())
            _, action = outbox.enqueue_from_confirmation(db, WATCH, c, conf)
            actions.append(action)
        return cands, actions, calls

    def test_first_observation_under_cap_alerts_end_to_end(self):
        db, _ = fresh_db(self)
        store.save_observations(db, "w1", [self._obs(495.0)], RUN1)
        cands, actions, calls = self._run_pipeline(
            db, RUN1, self._verify_rows(495.0))
        self.assertEqual(len(cands), 1)
        self.assertEqual(actions, ["inserted"])
        self.assertEqual(calls[0]["attempt_category"], "verify")
        # The candidate carries the RICH fingerprint (not the Phase-1 one).
        rich_fp = store.make_fingerprint(
            "SFO", "JFK", DEP, RET, "UA", 0, list(self.FNS), list(self.DTS))
        self.assertEqual(cands[0]["fingerprint"], rich_fp)
        self.assertNotEqual(cands[0]["fingerprint"], FP)
        # The alert lands in the weekly slot, pending.
        row = store.outbox_get(db, AID)
        self.assertIsNotNone(row)
        self.assertEqual(row["state"], "pending")
        self.assertEqual(row["fingerprint"], rich_fp)
        self.assertEqual(row["price_usd"], 495.0)

    def test_495_then_504_no_second_alert(self):
        """$495 -> $504 under a $500 cap: the second run emits nothing.

        The $504 observation is over the cap, so the scorer yields no
        candidate, the verifier never runs, and nothing is enqueued.
        """
        db, _ = fresh_db(self)
        store.save_observations(db, "w1", [self._obs(495.0)], RUN1)
        _, actions, _ = self._run_pipeline(db, RUN1, self._verify_rows(495.0))
        self.assertEqual(actions, ["inserted"])

        store.save_observations(db, "w1", [self._obs(504.0, RUN2)], RUN2)
        cands2, actions2, calls2 = self._run_pipeline(
            db, RUN2, self._verify_rows(504.0))
        self.assertEqual(cands2, [])
        self.assertEqual(actions2, [])
        self.assertEqual(calls2, [])  # verifier never invoked
        # Still exactly one outbox row for the week.
        self.assertEqual(len(store.outbox_list(db, "pending")), 1)
        self.assertEqual(store.outbox_get(db, AID)["price_usd"], 495.0)

    def test_second_candidate_same_week_deduped_not_relayed(self):
        """A cheaper same-week deal without a meaningful drop stays silent."""
        db, _ = fresh_db(self)
        store.save_observations(db, "w1", [self._obs(495.0)], RUN1)
        _, actions, _ = self._run_pipeline(db, RUN1, self._verify_rows(495.0))
        self.assertEqual(actions, ["inserted"])

        fns2 = ("UA999", "UA100")
        dts2 = ("2026-10-01T10:45:00", "2026-10-08T19:15:00")
        store.save_observations(db, "w1", [self._obs(490.0, RUN2, fns2, dts2)],
                                RUN2)
        cands, actions, _ = self._run_pipeline(
            db, RUN2, self._verify_rows(490.0, fns2, dts2))
        self.assertEqual(len(cands), 1)          # under cap, new fingerprint
        self.assertEqual(actions, ["deduped"])   # ...but no meaningful drop
        self.assertNotIn("deduped", outbox.RELAYABLE_ACTIONS)
        row = store.outbox_get(db, AID)
        self.assertEqual(row["price_usd"], 495.0)  # outboxed price unchanged
        self.assertEqual(row["state"], "pending")

    def test_meaningful_drop_realerts_same_week(self):
        """A max($15, 5%) drop re-arms the weekly slot and is relayable."""
        db, _ = fresh_db(self)
        store.save_observations(db, "w1", [self._obs(495.0)], RUN1)
        _, actions, _ = self._run_pipeline(db, RUN1, self._verify_rows(495.0))
        self.assertEqual(actions, ["inserted"])

        fns2 = ("UA999", "UA100")
        dts2 = ("2026-10-01T10:45:00", "2026-10-08T19:15:00")
        store.save_observations(db, "w1", [self._obs(460.0, RUN2, fns2, dts2)],
                                RUN2)
        _, actions, _ = self._run_pipeline(
            db, RUN2, self._verify_rows(460.0, fns2, dts2))
        # $35 below the outboxed $495 >= max($15, 5%) -> rearmed, relayable.
        self.assertEqual(actions, ["rearmed"])
        self.assertIn("rearmed", outbox.RELAYABLE_ACTIONS)
        row = store.outbox_get(db, AID)
        self.assertEqual(row["state"], "pending")
        self.assertEqual(row["price_usd"], 460.0)


class RealFliShapePipelineTests(unittest.TestCase):
    """End-to-end through real fli result parsing (fake transport, no
    network): poller._flatten -> save_observations -> scorer ->
    verifier.verify. Proves the rich fingerprint round-trips through real
    leg shapes, not just synthetic rows."""

    def test_flatten_feeds_rich_fingerprint_round_trip(self):
        db, _ = fresh_db(self)
        guard = BudgetGuard(db)
        install_fake_transport(guard)
        self.addCleanup(uninstall_fake_transport)
        rows = poller.narrow_query(
            "SFO", "JFK", DEP, RET,
            {"origins": ["SFO"], "destinations": ["JFK"], "stops": [0, 1],
             "seat": "economy", "pax": {"adults": 1}, "trip_days": (7, 7)},
            top_n=3, attempt_category="manual")
        self.assertTrue(rows)
        for r in rows:
            # Every new fli result populates the v3 leg fields.
            self.assertTrue(r["flight_numbers"])
            self.assertTrue(r["depart_times"])
        n = store.save_observations(db, "w1", rows, RUN1)
        self.assertEqual(n, len(rows))
        watch = dict(WATCH, absolute_cap_usd=100000.0)
        cands = scorer.score_new_observations(db, watch, RUN1)
        self.assertTrue(cands)
        for c in cands:
            # Rich fingerprints differ from the Phase-1 scheme.
            self.assertNotEqual(
                c["fingerprint"],
                store.make_fingerprint(
                    c["origin"], c["destination"], c["depart_date"],
                    c["return_date"], c["airline"], c["stops"]))
        # The verifier re-matches the SAME flights through a fresh query.
        ok, conf = verifier.verify(db, watch, cands[0], None)
        self.assertTrue(ok)
        self.assertIsNotNone(conf)
        self.assertLessEqual(conf["price_usd"], 100000.0)


if __name__ == "__main__":
    unittest.main()