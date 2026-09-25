"""Stage 01 tests: atomic budget guard + HTTP transport instrumentation.

Offline only: every HTTP attempt is routed to the fake transport
(tests/fake_transport.py); no real network is touched.

Run from the project root, e.g.:
    python -m unittest discover -s tests
The project root is put first on sys.path so ``import store`` resolves to
the project's store.py.
"""
import shutil
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
        reserve("manual"): exactly 150 succeed, 1 raises BudgetExhausted,
        and the ledger holds exactly 150 rows (atomic ceiling proof)."""
        results = []
        lock = threading.Lock()

        def worker():
            conn = store.get_db(self.db_path)
            try:
                g = budget.BudgetGuard(conn, day=self.day)
                try:
                    g.reserve("manual")
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
        # until ~= now + max(120, 900 * 2**0) = now + 900
        self.assertAlmostEqual(backoff["until_ts"], t0 + 900, delta=60)
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


if __name__ == "__main__":
    unittest.main()
