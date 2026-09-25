"""Stage 01 tests: atomic budget guard + HTTP transport instrumentation.

Offline only: every HTTP attempt is routed to the fake transport
(tests/fake_transport.py); no real network is touched.

Run from the project root, e.g.:
    python -m unittest discover -s tests
The project root is put first on sys.path so ``import store`` resolves to
the project's store.py.
"""
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path as _Path
from types import SimpleNamespace

_TESTS_DIR = _Path(__file__).resolve().parent
_PROJECT_ROOT = _TESTS_DIR.parent
sys.path.insert(0, str(_TESTS_DIR))  # fake_transport
sys.path.insert(0, str(_PROJECT_ROOT))  # project modules: store, budget, transport

import store  # noqa: E402  -- shared contract
import budget  # noqa: E402
import transport  # noqa: E402
import fli.search._concurrency  # noqa: E402
import fli.search._tfs  # noqa: E402
from curl_cffi.requests import Session  # noqa: E402
from fake_transport import (  # noqa: E402
    FakeResponse,
    install_fake_transport,
    uninstall_fake_transport,
)


class BudgetTransportTest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="s1test-")
        self.db_path = self.tmpdir + "/test.db"
        self.db = store.get_db(self.db_path)
        store.init_db(self.db)
        self.day = store.today_str()

    def tearDown(self):
        # Transport state must not leak between tests.
        uninstall_fake_transport()
        try:
            self.db.close()
        finally:
            shutil.rmtree(self.tmpdir, ignore_errors=True)

    def make_guard(self, **kw):
        kw.setdefault("day", self.day)
        return budget.BudgetGuard(self.db, **kw)

    # -- atomic daily ceiling --------------------------------------

    def test_atomic_daily_ceiling_151_threads(self):
        """151 threads x separate connections x separate guards hammering
        reserve("verify"): exactly 150 succeed, 1 raises BudgetExhausted,
        and the ledger holds exactly 150 rows (atomic ceiling proof).

        Category "verify" is used because v3 holds a verify reserve: a
        non-"verify" category would stop at 126 (the last 24 attempts are
        held for the verifier -- see test_verify_reserve_blocks_non_verify).
        The daily ceiling itself is what this test proves atomic."""
        results = []
        lock = threading.Lock()

        def worker():
            conn = store.get_db(self.db_path)
            try:
                # No category caps: the DAILY ceiling is what must bind.
                # Category "verify" is exempt from the v3 verify reserve,
                # so the ceiling is proved atomic without interference.
                g = budget.BudgetGuard(conn, day=self.day, category_caps={})
                try:
                    g.reserve("verify")
                    outcome = "ok"
                except budget.BudgetExhausted:
                    outcome = "exhausted"
                with lock:
                    results.append(outcome)
            finally:
                conn.close()

        threads = [threading.Thread(target=worker) for _ in range(151)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(results), 151)
        self.assertEqual(results.count("ok"), 150)
        self.assertEqual(results.count("exhausted"), 1)
        self.assertEqual(store.count_today(self.db, self.day), 150)
        n = self.db.execute(
            "SELECT COUNT(*) AS n FROM budget_ledger WHERE day = ?",
            (self.day,)).fetchone()["n"]
        self.assertEqual(n, 150)

    # -- category cap ----------------------------------------------

    def test_category_cap(self):
        g = self.make_guard(category_caps={"manual": 3})
        for _ in range(3):
            g.reserve("manual")
        with self.assertRaises(budget.BudgetExhausted) as cm:
            g.reserve("manual")
        exc = cm.exception
        self.assertEqual(exc.scope, "category")
        self.assertEqual(exc.category, "manual")
        self.assertEqual(exc.limit, 3)
        self.assertEqual(exc.spent, 3)
        self.assertEqual(g.spent_today("manual"), 3)

    # -- per-run cap -----------------------------------------------

    def test_per_run_cap(self):
        g = self.make_guard(per_run_max=2)
        g.reserve("manual")
        g.reserve("manual")
        with self.assertRaises(budget.BudgetExhausted) as cm:
            g.reserve("manual")
        self.assertEqual(cm.exception.scope, "per_run")
        # The failed reserve consumed nothing in the DB ledger either.
        self.assertEqual(g.spent_today(), 2)
        self.assertEqual(g.spent_run(), 2)
        g.reset_run()
        self.assertEqual(g.spent_run(), 0)
        self.assertEqual(g.spent_run("manual"), 0)
        g.reserve("manual")  # works again after reset
        self.assertEqual(g.spent_today(), 3)

    # -- fail closed -----------------------------------------------

    def test_fail_closed_after_uninstall(self):
        g = self.make_guard()
        install_fake_transport(g, mode="ok")
        uninstall_fake_transport()
        with self.assertRaisesRegex(RuntimeError,
                                    "transport guard not installed"):
            transport.require_guard()
        # The Session wrappers stay armed and fail closed: nothing is sent.
        with self.assertRaisesRegex(RuntimeError,
                                    "transport guard not installed"):
            Session().get("http://example.invalid")
        with self.assertRaisesRegex(RuntimeError,
                                    "transport guard not installed"):
            Session().post("http://example.invalid")

    # -- per-send counting (not per-search) -------------------------

    def test_per_send_counting(self):
        g = self.make_guard()
        fake = install_fake_transport(g, mode="ok")
        s = Session()
        s.get("http://example.invalid/a")
        s.get("http://example.invalid/b")
        self.assertEqual(g.spent_today(), 2)
        self.assertEqual(g.spent_today("manual"), 2)
        self.assertEqual(fake.sends, 2)
        self.assertEqual(fake.categories, ["manual", "manual"])

    def test_category_scope_labels_sends(self):
        g = self.make_guard()
        fake = install_fake_transport(g, mode="ok")
        self.assertEqual(transport.current_category(), "manual")
        with transport.category_scope("verify"):
            self.assertEqual(transport.current_category(), "verify")
            Session().get("http://example.invalid/x")
        self.assertEqual(transport.current_category(), "manual")
        self.assertEqual(fake.categories, ["verify"])
        self.assertEqual(g.spent_today("verify"), 1)

    # -- BudgetExhausted inside the wrapper -------------------------

    def test_budget_exhausted_inside_wrapper(self):
        g = self.make_guard(daily_ceiling=0)
        fake = install_fake_transport(g, mode="ok")
        with self.assertRaises(budget.BudgetExhausted):
            Session().get("http://example.invalid")
        self.assertEqual(fake.sends, 0)  # nothing sent
        self.assertEqual(g.spent_today(), 0)

    # -- 429 throttle -----------------------------------------------

    def test_429_throttle(self):
        g = self.make_guard()
        fake = install_fake_transport(g, mode="http429", retry_after="120")
        t0 = time.time()
        with self.assertRaises(transport.ThrottleDetected) as cm:
            Session().get("http://example.invalid")
        exc = cm.exception
        # BaseException, NOT Exception: fli's tenacity retry must not retry.
        self.assertFalse(isinstance(exc, Exception))
        self.assertTrue(isinstance(exc, BaseException))
        self.assertEqual(exc.kind, "http_429")
        backoff = store.get_backoff(self.db)
        self.assertIsNotNone(backoff)
        self.assertTrue(backoff["until_ts"] > time.time())
        self.assertEqual(backoff["kind"], "http_429")
        # v3 item 8: Retry-After header honored when present --
        # until ~= now + 120, NOT max(120, exponential).
        self.assertAlmostEqual(backoff["until_ts"], t0 + 120, delta=60)
        self.assertEqual(backoff["retry_after_s"], 120)
        self.assertEqual(exc.retry_after_s, 120)
        # The attempt was still made (and budgeted) before the 429 arrived.
        self.assertEqual(fake.sends, 1)
        self.assertEqual(g.spent_today(), 1)

    # -- CAPTCHA throttle --------------------------------------------

    def test_captcha_throttle(self):
        g = self.make_guard()
        install_fake_transport(g, mode="captcha")
        with self.assertRaises(transport.ThrottleDetected) as cm:
            Session().get("http://example.invalid")
        exc = cm.exception
        self.assertFalse(isinstance(exc, Exception))
        self.assertEqual(exc.kind, "captcha")
        backoff = store.get_backoff(self.db)
        self.assertIsNotNone(backoff)
        self.assertEqual(backoff["kind"], "captcha")
        self.assertTrue(backoff["until_ts"] > time.time())

    def test_live_results_page_is_not_captcha(self):
        """A normal results page (ds:1 payload + Google's route list with
        "/recaptcha/challenge") passes through: no throttle, no backoff."""
        g = self.make_guard()
        fake = install_fake_transport(g, mode="ok")
        resp = Session().get("http://example.invalid")
        self.assertIn("/recaptcha/challenge", resp.text)
        self.assertIsNone(store.get_backoff(self.db))
        self.assertEqual(fake.sends, 1)

    def _classify_via_send(self, resp):
        """Send one request through the real transport wrapper, returning
        the ThrottleDetected kind (or None) and the persisted backoff."""
        uninstall_fake_transport()
        store.clear_backoff(self.db)
        g = self.make_guard()
        install_fake_transport(g, mode="ok")
        transport.set_send_handler(lambda m, u, k, proceed: resp)
        try:
            Session().get("http://example.invalid")
            kind = None
        except transport.ThrottleDetected as e:
            kind = e.kind
        return kind, store.get_backoff(self.db)

    def test_block_page_with_incidental_ds1_is_captcha(self):
        """A block page mentioning "ds:1"/"ds:10" in its JS (no real
        payload) is still a CAPTCHA."""
        from fake_transport import CAPTCHA_BODY
        for extra in ('<script>var k="ds:1";</script>',
                      '<script>var k="ds:10";</script>',
                      "<script>AF_initDataCallback({key: 'ds:1', data:</script>"):
            with self.subTest(extra=extra):
                kind, backoff = self._classify_via_send(
                    FakeResponse(text=CAPTCHA_BODY + extra))
                self.assertEqual(kind, "captcha")
                self.assertEqual(backoff["kind"], "captcha")

    def test_sorry_url_is_captcha_even_with_payload(self):
        """Google's /sorry/ block URL is a CAPTCHA regardless of body."""
        for url in ("https://www.google.com/sorry/index?continue=x",
                    "https://sorry.google.com/"):
            with self.subTest(url=url):
                kind, _ = self._classify_via_send(FakeResponse(url=url))
                self.assertEqual(kind, "captcha")

    def test_429_with_payload_is_still_429(self):
        kind, _ = self._classify_via_send(FakeResponse(status_code=429))
        self.assertEqual(kind, "http_429")

    # -- blank pages tripwire -----------------------------------------

    def test_blank_pages_tripwire(self):
        g = self.make_guard()
        install_fake_transport(g, mode="ok")
        orig_sleep = fli.search._tfs._sleep
        fli.search._tfs._sleep = lambda s: None  # keep the test fast
        try:
            client = SimpleNamespace(
                get=lambda url, **kw: FakeResponse(text="<html></html>"))
            patched = fli.search._tfs.fetch_payload
            self.assertIsNone(patched(client, "http://example.invalid/"))
            self.assertIsNone(patched(client, "http://example.invalid/"))
            with self.assertRaises(transport.ThrottleDetected) as cm:
                patched(client, "http://example.invalid/")
            exc = cm.exception
            self.assertFalse(isinstance(exc, Exception))
            self.assertEqual(exc.kind, "blank_pages")
            backoff = store.get_backoff(self.db)
            self.assertIsNotNone(backoff)
            self.assertEqual(backoff["kind"], "blank_pages")
            self.assertTrue(backoff["until_ts"] > time.time())
        finally:
            fli.search._tfs._sleep = orig_sleep

    # -- concurrency ---------------------------------------------------

    def test_concurrency_forced_sequential(self):
        g = self.make_guard()
        install_fake_transport(g, mode="ok")
        self.assertEqual(
            fli.search._concurrency._executor_max_workers, 1)

    # -- install contract ----------------------------------------------

    def test_install_idempotent_and_exclusive(self):
        g = self.make_guard()
        transport.install_transport_guard(g)
        transport.install_transport_guard(g)  # same guard: no-op
        other = self.make_guard()
        with self.assertRaises(RuntimeError):
            transport.install_transport_guard(other)
        self.assertIs(transport.require_guard(), g)


# ---------------------------------------------------------------------------
# v3 reviewer items
# ---------------------------------------------------------------------------

class BudgetV3Test(BudgetTransportTest):
    """Group 1 (v3): BudgetExhausted as BaseException, own-connection
    reserve, verify reserve, per-send allowance, import-time Session patch,
    returned-429/suspect-403 inspection in the success path."""

    def test_budget_exhausted_is_base_exception(self):
        # v3 item 2: deliberately NOT an Exception subclass, so fli's
        # `except Exception` -> _wrap_request_error cannot convert it.
        self.assertTrue(issubclass(budget.BudgetExhausted, BaseException))
        self.assertFalse(issubclass(budget.BudgetExhausted, Exception))

    # -- verify reserve (item 6) --------------------------------------

    def test_verify_reserve_blocks_non_verify(self):
        # No category caps and a high per-run cap: only the daily ceiling
        # and the verify reserve are in play.
        g = self.make_guard(category_caps={}, per_run_max=1000)
        for _ in range(126):
            g.reserve("sentinel")
        # daily_remaining = 24; 24 - 1 < max(0, 24 - 0) -> refused, even
        # though daily budget remains.
        with self.assertRaises(budget.BudgetExhausted) as cm:
            g.reserve("sentinel")
        self.assertEqual(cm.exception.scope, "verify_reserve")
        # Nothing was recorded for the refused attempt.
        self.assertEqual(g.spent_today(), 126)
        # The held-back attempts are still available to the verifier.
        g.reserve("verify")
        self.assertEqual(g.spent_today("verify"), 1)
        self.assertEqual(g.spent_today(), 127)

    def test_verify_spend_releases_headroom(self):
        g = self.make_guard(category_caps={}, per_run_max=1000)
        for _ in range(24):
            g.reserve("verify")
        # headroom is now 0: non-verify categories may use the full ceiling.
        for _ in range(126):
            g.reserve("sentinel")
        self.assertEqual(g.spent_today(), 150)
        with self.assertRaises(budget.BudgetExhausted) as cm:
            g.reserve("sentinel")
        self.assertEqual(cm.exception.scope, "daily")

    # -- own-connection reserve (item 3) -------------------------------

    def test_reserve_leaves_caller_transaction_open(self):
        g = self.make_guard()
        # Caller holds an open DEFERRED (read) transaction. (An open *write*
        # transaction would inherently block any other BEGIN IMMEDIATE; the
        # contract is that the guard never commits or rolls back the
        # caller's transaction, whatever it holds.)
        self.db.execute("BEGIN")
        before = self.db.execute(
            "SELECT COUNT(*) AS n FROM budget_ledger").fetchone()["n"]
        self.assertTrue(self.db.in_transaction)
        g.reserve("manual")
        # Still open and intact: the guard used its OWN connection and never
        # touched the caller's (the v2 bug committed it here).
        self.assertTrue(self.db.in_transaction)
        after = self.db.execute(
            "SELECT COUNT(*) AS n FROM budget_ledger").fetchone()["n"]
        self.assertEqual(after, before)  # our snapshot is untouched
        self.db.rollback()
        self.assertFalse(self.db.in_transaction)
        # ...and the reserve really landed (via the guard's own connection).
        self.assertEqual(
            store.count_today(self.db, self.day, "manual"), 1)

    # -- import-time patch / fail closed (item 1) -----------------------

    def test_fresh_import_patches_session_immediately(self):
        """A fresh subprocess importing transport (no install call) has
        Session.get/post patched and fails closed on the first send."""
        code = (
            "import sys\n"
            "sys.path.insert(0, sys.argv[1])\n"
            "import transport\n"
            "from curl_cffi.requests import Session\n"
            "assert Session.get.__name__ == '_guarded_get', Session.get\n"
            "assert Session.post.__name__ == '_guarded_post', Session.post\n"
            "try:\n"
            "    Session().get('http://example.invalid/')\n"
            "except RuntimeError as e:\n"
            "    assert 'transport guard not installed' in str(e), e\n"
            "    print('FAIL_CLOSED_OK')\n"
            "else:\n"
            "    raise SystemExit('unarmed send did not fail closed')\n"
        )
        proc = subprocess.run(
            [sys.executable, "-c", code, str(_PROJECT_ROOT)],
            capture_output=True, text=True, timeout=120)
        self.assertIn("FAIL_CLOSED_OK", proc.stdout, proc.stderr)

    # -- success-path inspection (item 4) --------------------------------

    def test_suspect_403_detected_in_success_path(self):
        g = self.make_guard()
        install_fake_transport(g, mode="ok")

        def handler(method, url, kwargs, proceed):
            return FakeResponse(
                text="<html><body>rate-limit exceeded, slow down</body></html>",
                status_code=403)

        transport.set_send_handler(handler)
        with self.assertRaises(transport.ThrottleDetected) as cm:
            Session().get("http://example.invalid")
        exc = cm.exception
        self.assertFalse(isinstance(exc, Exception))
        self.assertEqual(exc.kind, "suspect_403")
        backoff = store.get_backoff(self.db)
        self.assertIsNotNone(backoff)
        self.assertEqual(backoff["kind"], "suspect_403")
        self.assertTrue(backoff["until_ts"] > time.time())
        # The attempt was made (and budgeted) before the 403 arrived.
        self.assertEqual(g.spent_today(), 1)

    def test_retry_after_absent_uses_exponential(self):
        # v3 item 8: no Retry-After header -> exponential 900 * 2**(n-1).
        g = self.make_guard()
        install_fake_transport(g, mode="http429")  # no retry_after
        t0 = time.time()
        with self.assertRaises(transport.ThrottleDetected) as cm:
            Session().get("http://example.invalid")
        self.assertIsNone(cm.exception.retry_after_s)
        backoff = store.get_backoff(self.db)
        self.assertIsNone(backoff["retry_after_s"])
        self.assertAlmostEqual(backoff["until_ts"], t0 + 900, delta=60)
        # While the backoff is active, the send gate blocks without sending.
        fake = transport._send_handler
        sends_before = fake.sends
        with self.assertRaises(transport.ThrottleDetected):
            Session().get("http://example.invalid")
        self.assertEqual(fake.sends, sends_before)
        # After it expires (no clean response in between), history is kept:
        # the next 429 without a header doubles the backoff.
        self.db.execute("UPDATE throttle_state SET until_ts = ?",
                        (time.time() - 1,))
        self.db.commit()
        t1 = time.time()
        with self.assertRaises(transport.ThrottleDetected):
            Session().get("http://example.invalid")
        backoff2 = store.get_backoff(self.db)
        self.assertAlmostEqual(backoff2["until_ts"], t1 + 1800, delta=120)
        self.assertEqual(fake.sends, sends_before + 1)

    def test_clean_response_resets_throttle_history(self):
        g = self.make_guard()
        install_fake_transport(g, mode="ok")
        store.set_backoff(self.db, "http_429", time.time() - 1, None, "old")
        Session().get("http://example.invalid")
        self.assertIsNone(store.get_throttle_state(self.db))

    def test_stale_success_keeps_newer_throttle(self):
        """A success that saw an expired throttle must not delete a newer
        throttle written after it read the row."""
        store.set_backoff(self.db, "http_429", time.time() - 1, None, "old")
        seen = store.get_throttle_state(self.db)
        time.sleep(0.01)
        store.set_backoff(self.db, "captcha", time.time() + 900, None, "new")
        self.assertFalse(store.clear_backoff_if_unchanged(self.db, seen))
        self.assertEqual(store.get_backoff(self.db)["kind"], "captcha")
        self.assertTrue(store.clear_backoff_if_unchanged(
            self.db, store.get_throttle_state(self.db)))

    def test_redirect_hops_are_each_budgeted(self):
        """Redirects are followed by the wrapper, one reserved send per hop
        (libcurl auto-follow would hide hops from the ledger)."""
        g = self.make_guard()
        fake = install_fake_transport(g, mode="ok")
        seen = []

        def handler(method, url, kwargs, proceed):
            seen.append((url, kwargs.get("allow_redirects")))
            if len(seen) < 3:
                return FakeResponse(text="", status_code=302,
                                    headers={"Location": f"/hop{len(seen)}"})
            return FakeResponse()

        transport.set_send_handler(handler)
        resp = Session().get("https://www.google.com/travel/flights")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual([u for u, _ in seen], [
            "https://www.google.com/travel/flights",
            "https://www.google.com/hop1", "https://www.google.com/hop2"])
        self.assertTrue(all(a is False for _, a in seen))
        self.assertEqual(g.spent_today(), 3)

    def test_redirect_loop_is_bounded(self):
        g = self.make_guard()
        install_fake_transport(g, mode="ok")
        transport.set_send_handler(lambda m, u, k, p: FakeResponse(
            text="", status_code=302, headers={"location": "/again"}))
        resp = Session().get("https://www.google.com/x")
        self.assertEqual(resp.status_code, 302)
        self.assertEqual(g.spent_today(), transport.MAX_REDIRECTS + 1)

    def test_deadline_blocks_sends(self):
        g = self.make_guard()
        fake = install_fake_transport(g, mode="ok")
        transport.set_deadline_fn(lambda: True)
        self.addCleanup(transport.set_deadline_fn, None)
        with self.assertRaises(store.DeadlineExceeded):
            Session().get("http://example.invalid")
        self.assertEqual(fake.sends, 0)
        self.assertFalse(isinstance(store.DeadlineExceeded("x"), Exception))

    def test_day_resolved_per_reserve(self):
        """A guard created before UTC midnight charges the new day after it."""
        from unittest import mock
        g = budget.BudgetGuard(self.db)  # no fixed day
        with mock.patch.object(store, "today_str", return_value="2030-01-01"):
            g.reserve("sentinel")
        with mock.patch.object(store, "today_str", return_value="2030-01-02"):
            g.reserve("sentinel")
            self.assertEqual(g.spent_today(), 1)
        self.assertEqual(store.count_today(self.db, "2030-01-01"), 1)

    def test_migration_backfills_counters_from_ledger(self):
        for _ in range(150):
            self.db.execute(
                "INSERT INTO budget_ledger (day, category, attempts, created_at)"
                " VALUES ('2030-01-01', 'sentinel', 1, 'x')")
        self.db.execute("DELETE FROM daily_counters")
        self.db.commit()
        store.init_db(self.db)
        store.init_db(self.db)  # idempotent
        self.assertEqual(store.count_today(self.db, "2030-01-01"), 150)
        g = budget.BudgetGuard(self.db, day="2030-01-01")
        with self.assertRaises(budget.BudgetExhausted):
            g.reserve("verify")

    # -- BudgetExhausted through real fli Client.get (item 2) -----------

    def test_budget_exhausted_through_real_fli_client_get(self):
        """The real fli Client.get must propagate the ORIGINAL
        BudgetExhausted: not wrapped by `except Exception` into
        _wrap_request_error, never converted to an "unknown". Tenacity may
        still spin its attempts, but every attempt fails fast inside
        reserve(): zero sends, zero ledger rows."""
        from fli.search.client import Client
        g = self.make_guard(daily_ceiling=0)
        fake = install_fake_transport(g, mode="ok")
        client = Client()
        with self.assertRaises(budget.BudgetExhausted) as cm:
            client.get("http://example.invalid/")
        exc = cm.exception
        self.assertIs(type(exc), budget.BudgetExhausted)  # not wrapped
        self.assertFalse(isinstance(exc, Exception))
        self.assertTrue(isinstance(exc, BaseException))
        self.assertEqual(exc.scope, "daily")
        self.assertEqual(fake.sends, 0)      # nothing left the machine
        self.assertEqual(g.spent_today(), 0)  # nothing recorded

    # -- per-send allowance (item 5) ------------------------------------

    def test_top_n_3_against_allowance_2_stops_at_two_sends(self):
        """set_run_allowance(2), then a top_n=3 query: exactly 2 sends go
        out; the 3rd raises BudgetExhausted mid-query (per-send, not
        per-query enforcement)."""
        import poller
        orig_polite = poller._polite
        poller._polite = lambda: None
        self.addCleanup(setattr, poller, "_polite", orig_polite)
        g = self.make_guard()
        g.set_run_allowance(2)
        fake = install_fake_transport(g, mode="ok")
        watch = {"origins": ["JFK"], "destinations": ["LHR"],
                 "max_stops": 0, "cabin": "economy"}
        with self.assertRaises(budget.BudgetExhausted) as cm:
            poller.narrow_query("JFK", "LHR", "2026-12-01", "2026-12-08",
                                watch, top_n=3, attempt_category="sentinel")
        self.assertEqual(cm.exception.scope, "per_run")
        self.assertFalse(isinstance(cm.exception, Exception))
        self.assertEqual(fake.sends, 2)
        self.assertEqual(g.spent_today("sentinel"), 2)

    # -- persisted backoff skips the follow-up run ------------------------

    def test_followup_scheduled_run_skips_collection_on_backoff(self):
        """First run: a returned 429 persists a backoff. Follow-up scheduled
        run: collection fn never runs, nothing is sent, and the legitimate
        no-op still pings success (never /fail)."""
        import importlib.util as ilu
        import heartbeat
        spec = ilu.spec_from_file_location(
            "fm_run_v3t", str(_PROJECT_ROOT / "run.py"))
        fm_run = ilu.module_from_spec(spec)
        sys.modules["fm_run_v3t"] = fm_run
        spec.loader.exec_module(fm_run)

        g = self.make_guard()
        fake = install_fake_transport(g, mode="http429", retry_after="120")
        with self.assertRaises(transport.ThrottleDetected):
            Session().get("http://example.invalid")
        self.assertIsNotNone(store.get_backoff(self.db))

        old_url = os.environ.get("HEALTHCHECKS_PING_URL")
        os.environ["HEALTHCHECKS_PING_URL"] = \
            "https://example.invalid/hc/fake-v3-backoff"
        self.addCleanup(self._restore_hc_env, old_url)
        captured = heartbeat.start_capture()
        self.addCleanup(heartbeat.stop_capture)

        calls = []

        def spy(args, db, watches, guard):
            calls.append(1)
            return 0

        args = SimpleNamespace(cmd="check", scheduled=True, watch=None,
                               duration=7, depart="2026-10-01",
                               return_="2026-10-08")
        rc = fm_run.run_guarded("check", args, self.db, [], g, spy)
        self.assertEqual(rc, 0)
        self.assertEqual(calls, [])       # collection never ran
        self.assertEqual(fake.sends, 1)   # only the throttled first-run send
        kinds = [k for k, _ in captured]
        self.assertEqual(kinds, ["success"])
        self.assertFalse(any("/fail" in u for _, u in captured))

    @staticmethod
    def _restore_hc_env(old_url):
        if old_url is None:
            os.environ.pop("HEALTHCHECKS_PING_URL", None)
        else:
            os.environ["HEALTHCHECKS_PING_URL"] = old_url


if __name__ == "__main__":
    unittest.main()
