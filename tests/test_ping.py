"""unittest coverage for heartbeat.py (stage 03 rebuild, v2).

Safety rules for this file:
  - NEVER touches ~/workspace/Flight Monitor/.env or the real check.
  - The fake HEALTHCHECKS_PING_URL always points at https://example.invalid.
  - The fake _send asserts that every URL it receives starts with
    "https://example.invalid", so no test can ever ping the real check.
  - No URL value is ever printed or logged.
"""
import os
import sys
import unittest
from pathlib import Path as _Path

_TESTS_DIR = _Path(__file__).resolve().parent
_PROJECT_ROOT = _TESTS_DIR.parent
sys.path.insert(0, str(_TESTS_DIR))
sys.path.insert(0, str(_PROJECT_ROOT))

import store  # noqa: E402
import heartbeat  # noqa: E402

assert store.__file__.startswith(str(_PROJECT_ROOT)), (
    "test precondition: the project store.py must win the import, got "
    + str(store.__file__)
)

FAKE_URL = "https://example.invalid/hc/9f3b1a2e-4c5d-4e6f-8a7b-9c0d1e2f3a4b"
OTHER_FAKE_URL = "https://example.invalid/hc/00000000-1111-2222-3333-444444444444"
SAFETY_PREFIX = "https://example.invalid"


def make_db():
    db = store.get_db(":memory:")
    store.init_db(db)
    return db


class HeartbeatPingTest(unittest.TestCase):
    def setUp(self):
        # Fake env (never the real URL) + capture of everything _send gets.
        self._orig_env = os.environ.get("HEALTHCHECKS_PING_URL", None)
        os.environ["HEALTHCHECKS_PING_URL"] = FAKE_URL
        self.sent_urls = []
        self._orig_send = heartbeat._send

        def fake_send(url, timeout=10):
            # Hard safety rail: no test may ever send anywhere real.
            assert url.startswith(SAFETY_PREFIX), (
                "SAFETY: refusing to send to non-example.invalid URL"
            )
            self.sent_urls.append((url, timeout))

        heartbeat._send = fake_send
        self.sleeps = []
        self._orig_sleep = heartbeat._sleep
        heartbeat._sleep = self.sleeps.append  # no real waiting in tests

    def tearDown(self):
        heartbeat._send = self._orig_send
        heartbeat._sleep = self._orig_sleep
        heartbeat.stop_capture()  # never let capture leak between tests
        if self._orig_env is None:
            os.environ.pop("HEALTHCHECKS_PING_URL", None)
        else:
            os.environ["HEALTHCHECKS_PING_URL"] = self._orig_env

    # -- success / fail URL shape --------------------------------------

    def test_ping_success_uses_exact_env_url(self):
        result = heartbeat.ping_success()
        self.assertEqual(result, {"ping": "ok", "url_set": True})
        self.assertEqual(len(self.sent_urls), 1)
        url, timeout = self.sent_urls[0]
        self.assertEqual(url, FAKE_URL)  # exactly the env URL, no /fail suffix
        self.assertEqual(timeout, heartbeat.PING_TIMEOUT_S)

    def test_ping_fail_appends_fail_suffix(self):
        result = heartbeat.ping_fail()
        self.assertEqual(result, {"ping": "ok", "url_set": True})
        self.assertEqual(len(self.sent_urls), 1)
        url, _timeout = self.sent_urls[0]
        self.assertEqual(url, FAKE_URL + "/fail")

    # -- env read at call time ------------------------------------------

    def test_env_read_at_call_time(self):
        heartbeat.ping_success()
        self.assertEqual(self.sent_urls[-1][0], FAKE_URL)
        # Change the env mid-test: the next ping must use the new value.
        os.environ["HEALTHCHECKS_PING_URL"] = OTHER_FAKE_URL
        heartbeat.ping_success()
        self.assertEqual(self.sent_urls[-1][0], OTHER_FAKE_URL)
        self.assertEqual(len(self.sent_urls), 2)

    # -- unset / empty env -----------------------------------------------

    def test_unset_env_skips_without_sending(self):
        del os.environ["HEALTHCHECKS_PING_URL"]
        self.assertEqual(
            heartbeat.ping_success(), {"ping": "skipped", "url_set": False}
        )
        self.assertEqual(
            heartbeat.ping_fail(), {"ping": "skipped", "url_set": False}
        )
        self.assertEqual(self.sent_urls, [])  # _send never called

    def test_empty_env_skips_without_sending(self):
        os.environ["HEALTHCHECKS_PING_URL"] = ""
        self.assertEqual(
            heartbeat.ping_success(), {"ping": "skipped", "url_set": False}
        )
        self.assertEqual(self.sent_urls, [])

    def test_validate_schedule_env_ok(self):
        self.assertEqual(heartbeat.validate_schedule_env(), (True, ""))

    def test_validate_schedule_env_unset(self):
        del os.environ["HEALTHCHECKS_PING_URL"]
        ok, reason = heartbeat.validate_schedule_env()
        self.assertIs(ok, False)
        self.assertTrue(reason, "reason must be non-empty")
        self.assertNotIn("example.invalid", reason)
        self.assertNotIn(FAKE_URL, reason)

    def test_validate_schedule_env_empty(self):
        os.environ["HEALTHCHECKS_PING_URL"] = ""
        ok, reason = heartbeat.validate_schedule_env()
        self.assertIs(ok, False)
        self.assertTrue(reason, "reason must be non-empty")
        self.assertNotIn("example.invalid", reason)

    # -- URL never leaks into results or health rows ---------------------

    def test_url_never_leaks(self):
        db = make_db()
        try:
            results = [heartbeat.ping_success(db), heartbeat.ping_fail(db)]
            del os.environ["HEALTHCHECKS_PING_URL"]
            results.append(heartbeat.ping_success(db))

            for result in results:
                self.assertNotIn("example.invalid", str(result))

            rows = db.execute(
                "SELECT detail FROM health WHERE check_name = 'hc_ping'"
            ).fetchall()
            self.assertEqual(len(rows), 3)
            details = [r["detail"] for r in rows]
            self.assertIn("hc ping ok", details)
            self.assertIn(
                "hc ping skipped: HEALTHCHECKS_PING_URL not set", details
            )
            for detail in details:
                self.assertNotIn("example.invalid", detail)
        finally:
            db.close()

    # -- send failures ----------------------------------------------------

    def test_send_exception_becomes_failed_result(self):
        def raising_send(url, timeout=10):
            assert url.startswith(SAFETY_PREFIX)
            raise ConnectionError("boom")

        heartbeat._send = raising_send
        db = make_db()
        try:
            result = heartbeat.ping_success(db)  # must not raise
            self.assertEqual(result, {"ping": "failed", "url_set": True})
            row = store.latest_health(db, "hc_ping")
            self.assertIsNotNone(row)
            self.assertEqual(row["ok"], 0)
            self.assertEqual(
                row["detail"],
                "hc ping failed after 3 attempts: "
                "ConnectionError, ConnectionError, ConnectionError")
            self.assertNotIn("example.invalid", row["detail"])
        finally:
            db.close()

    def test_transient_timeout_is_retried(self):
        """A ping that times out, then succeeds, is recorded ok -- one
        stalled request through Muse's proxy must not lose the heartbeat."""
        calls = []

        def flaky_send(url, timeout=10):
            assert url.startswith(SAFETY_PREFIX)
            calls.append(url)
            if len(calls) < 3:
                raise TimeoutError("read timed out")

        heartbeat._send = flaky_send
        db = make_db()
        try:
            result = heartbeat.ping_success(db)
            self.assertEqual(result, {"ping": "ok", "url_set": True})
            self.assertEqual(len(calls), 3)
            self.assertEqual(self.sleeps, [heartbeat.PING_RETRY_DELAY_S] * 2)
            row = store.latest_health(db, "hc_ping")
            self.assertEqual(row["ok"], 1)
            self.assertIn("attempt 3", row["detail"])
            self.assertNotIn("example.invalid", row["detail"])
        finally:
            db.close()

    def test_retries_are_bounded(self):
        calls = []

        def dead_send(url, timeout=10):
            calls.append(url)
            raise TimeoutError("read timed out")

        heartbeat._send = dead_send
        result = heartbeat.ping_success()
        self.assertEqual(result, {"ping": "failed", "url_set": True})
        self.assertEqual(len(calls), heartbeat.PING_ATTEMPTS)

    # -- capture mode ------------------------------------------------------

    def test_capture_mode(self):
        captured = heartbeat.start_capture()
        r1 = heartbeat.ping_success()
        r2 = heartbeat.ping_fail()
        self.assertEqual(
            captured,
            [("success", FAKE_URL), ("fail", FAKE_URL + "/fail")],
        )
        self.assertEqual(self.sent_urls, [])  # nothing actually sent
        self.assertEqual(r1, {"ping": "ok", "url_set": True, "captured": True})
        self.assertEqual(r2, {"ping": "ok", "url_set": True, "captured": True})
        heartbeat.stop_capture()
        # After stop_capture, pings go through _send again.
        heartbeat.ping_success()
        self.assertEqual(len(self.sent_urls), 1)
        self.assertEqual(self.sent_urls[0][0], FAKE_URL)


if __name__ == "__main__":
    unittest.main()
