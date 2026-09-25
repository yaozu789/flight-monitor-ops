# 72-hour trial runbook (2026-09-25)

**Yaozu approved:**
- the heartbeat-retry update (commit `3f9cbb5`)
- one one-off push test job
- enabling the **30-minute** schedule for a 72-hour trial
- two one-off report jobs

**Not approved:** any other schedule, destination, limit, or credential change. You also may not change the cadence yourself; if quota looks too hot, report it and Yaozu decides.

**Stop rule:** stop at the first failed or surprising check in steps 1–3 and report. Once the trial is running, you **never** pause or change the schedule yourself. You report, and Yaozu decides.

## 1. Build release `fm-release-3f9cbb5` (same method as the 981ba6a redeploy)
1. Copy `fm-release-981ba6a` without `data/` and `.env` into `~/workspace/fm-release-3f9cbb5`.
2. Apply the patch below with `patch -p1`.
3. Verify these hashes:
```
daa1710dc17197eceffeaac44f6527e4b042b2dcfc3bc3e6e5fa9897d2f3c715  heartbeat.py
1f471fa937a334d52640885df7e5c8d7612d174674f75628773c6e555dc42846  tests/test_ping.py
```
4. Nothing may be running from `fm-release-981ba6a` (no schedule exists yet). Take a fresh `.backup` of its `data/monitor.db` into `~/workspace/fm-backups/`, then run `integrity_check`.
5. Copy `data/` and `.env` in (root-owned, mode 600).
6. Run the offline suite. Expect **168 OK**.

`fm-release-981ba6a` stays untouched as the rollback.

## 2. Push test (one-off job)
Create **one** scheduled job about 10 minutes out. Its only action is to send Yaozu, with push: "TEST 2 — scheduled push test from the flight monitor, not a deal. Reply 'got it' in chat." After it fires, delete the job.

**Wait for Yaozu to confirm receipt before step 3.** If he doesn't get it, stop and report.

## 3. Enable the trial schedule
- **Every 30 minutes.** Command:
  ```sh
  /bin/sh -c 'cd "/home/hatch/workspace/fm-release-3f9cbb5" && set -a && . ./.env && set +a && exec /home/hatch/workspace/flitest_venv/bin/python run.py check --scheduled'
  ```
- Use the agent prompt you wrote in A6, with the path changed to `/home/hatch/workspace/fm-release-3f9cbb5`. Nothing else in it changes.
- Record the exact enable time as **trial start**. **Trial end** = start + 72h.
- The old crons `flight-monitor-check` and `flight-monitor-broad-scan` stay disabled.

## 4. Two one-off report jobs, at start + 24h and at trial end
Each report job does the following:
- **Reads only.** It never edits the schedule.
- **Reports in chat:**
  - runs fired vs expected (`run_slots` rows and statuses)
  - HTTP attempts per day by category (`daily_counters`)
  - observations added
  - throttles (`throttle_state`, `health`)
  - alerts relayed (outbox states)
  - heartbeat failures (`health` `hc_ping`)
  - deadline stops
  - quota % now vs trial start, with a projection to the Oct 1 reset
- **At +24h:** if the projection says the free allowance runs out before Oct 1, say so plainly. Yaozu decides whether to switch to every 2 hours, which needs a code change first.

## Report now (after steps 1–3)
Pass/fail + evidence per step, the exact trial start/end times (PDT), and the final prompt text.

## Patch for step 1 (save as `~/workspace/fix-3f9cbb5.diff`)
```diff
diff --git a/heartbeat.py b/heartbeat.py
index 3066892..75ed763 100644
--- a/heartbeat.py
+++ b/heartbeat.py
@@ -28,6 +28,13 @@ import store  # noqa: E402  (project store.py: the v2 contract with add_health)
 
 PING_ENV_VAR = "HEALTHCHECKS_PING_URL"
 
+# Muse's egress proxy intermittently stalls hc-ping.com (observed ReadTimeouts
+# on 2 of 3 pings with the old single 10 s attempt). A lost success ping can
+# raise a false "down" alert, so retry a few times with a longer timeout.
+PING_ATTEMPTS = 3
+PING_TIMEOUT_S = 20
+PING_RETRY_DELAY_S = 5
+
 # In-memory capture for tests: when not None, pings are recorded instead of
 # sent. Never used in production paths.
 _capture: list[tuple[str, str]] | None = None
@@ -41,7 +48,14 @@ def _ping_url() -> str | None:
     return os.environ.get(PING_ENV_VAR) or None
 
 
-def _send(url: str, timeout: int = 10) -> None:
+def _sleep(seconds: float) -> None:
+    """Overridable delay between ping attempts (tests make it instant)."""
+    import time
+
+    time.sleep(seconds)
+
+
+def _send(url: str, timeout: int = PING_TIMEOUT_S) -> None:
     """Single overridable send primitive (tests monkeypatch this).
 
     GET the URL and raise for non-2xx. Anything raised propagates to the
@@ -62,7 +76,7 @@ def _record(db, ok: bool, detail: str) -> None:
         pass
 
 
-def _ping(kind: str, db=None, timeout: int = 10) -> dict:
+def _ping(kind: str, db=None, timeout: int = PING_TIMEOUT_S) -> dict:
     """Shared implementation for ping_success ("success") and ping_fail ("fail").
 
     Never raises. Returns {"ping": "ok"|"failed"|"skipped", "url_set": bool},
@@ -84,19 +98,28 @@ def _ping(kind: str, db=None, timeout: int = 10) -> dict:
         _record(db, True, "hc ping ok")
         return {"ping": "ok", "url_set": True, "captured": True}
 
-    try:
-        _send(target, timeout=timeout)
-    except Exception as e:  # noqa: BLE001 - heartbeat must never raise
-        # Exception class name only: the URL (or its value inside an error
-        # message) must never leak.
-        _record(db, False, f"hc ping failed: {type(e).__name__}")
-        return {"ping": "failed", "url_set": True}
-
-    _record(db, True, "hc ping ok")
-    return {"ping": "ok", "url_set": True}
-
-
-def ping_success(db=None, timeout: int = 10) -> dict:
+    errors: list[str] = []
+    for attempt in range(1, PING_ATTEMPTS + 1):
+        try:
+            _send(target, timeout=timeout)
+        except Exception as e:  # noqa: BLE001 - heartbeat must never raise
+            # Exception class name only: the URL (or its value inside an
+            # error message) must never leak.
+            errors.append(type(e).__name__)
+            if attempt < PING_ATTEMPTS:
+                _sleep(PING_RETRY_DELAY_S)
+            continue
+        detail = "hc ping ok" if attempt == 1 else (
+            f"hc ping ok on attempt {attempt} (after {', '.join(errors)})")
+        _record(db, True, detail)
+        return {"ping": "ok", "url_set": True}
+
+    _record(db, False, f"hc ping failed after {PING_ATTEMPTS} attempts: "
+                       f"{', '.join(errors)}")
+    return {"ping": "failed", "url_set": True}
+
+
+def ping_success(db=None, timeout: int = PING_TIMEOUT_S) -> dict:
     """GET <url> (plain ping = success).
 
     Returns {"ping": "ok"|"failed"|"skipped", "url_set": bool}. "skipped"
@@ -107,7 +130,7 @@ def ping_success(db=None, timeout: int = 10) -> dict:
     return _ping("success", db=db, timeout=timeout)
 
 
-def ping_fail(db=None, timeout: int = 10) -> dict:
+def ping_fail(db=None, timeout: int = PING_TIMEOUT_S) -> dict:
     """GET <url>/fail. Same return contract as ping_success."""
     return _ping("fail", db=db, timeout=timeout)
 
diff --git a/tests/test_ping.py b/tests/test_ping.py
index 4d038ba..ff70c5c 100644
--- a/tests/test_ping.py
+++ b/tests/test_ping.py
@@ -52,9 +52,13 @@ class HeartbeatPingTest(unittest.TestCase):
             self.sent_urls.append((url, timeout))
 
         heartbeat._send = fake_send
+        self.sleeps = []
+        self._orig_sleep = heartbeat._sleep
+        heartbeat._sleep = self.sleeps.append  # no real waiting in tests
 
     def tearDown(self):
         heartbeat._send = self._orig_send
+        heartbeat._sleep = self._orig_sleep
         heartbeat.stop_capture()  # never let capture leak between tests
         if self._orig_env is None:
             os.environ.pop("HEALTHCHECKS_PING_URL", None)
@@ -69,7 +73,7 @@ class HeartbeatPingTest(unittest.TestCase):
         self.assertEqual(len(self.sent_urls), 1)
         url, timeout = self.sent_urls[0]
         self.assertEqual(url, FAKE_URL)  # exactly the env URL, no /fail suffix
-        self.assertEqual(timeout, 10)
+        self.assertEqual(timeout, heartbeat.PING_TIMEOUT_S)
 
     def test_ping_fail_appends_fail_suffix(self):
         result = heartbeat.ping_fail()
@@ -167,11 +171,51 @@ class HeartbeatPingTest(unittest.TestCase):
             row = store.latest_health(db, "hc_ping")
             self.assertIsNotNone(row)
             self.assertEqual(row["ok"], 0)
-            self.assertEqual(row["detail"], "hc ping failed: ConnectionError")
+            self.assertEqual(
+                row["detail"],
+                "hc ping failed after 3 attempts: "
+                "ConnectionError, ConnectionError, ConnectionError")
             self.assertNotIn("example.invalid", row["detail"])
         finally:
             db.close()
 
+    def test_transient_timeout_is_retried(self):
+        """A ping that times out, then succeeds, is recorded ok -- one
+        stalled request through Muse's proxy must not lose the heartbeat."""
+        calls = []
+
+        def flaky_send(url, timeout=10):
+            assert url.startswith(SAFETY_PREFIX)
+            calls.append(url)
+            if len(calls) < 3:
+                raise TimeoutError("read timed out")
+
+        heartbeat._send = flaky_send
+        db = make_db()
+        try:
+            result = heartbeat.ping_success(db)
+            self.assertEqual(result, {"ping": "ok", "url_set": True})
+            self.assertEqual(len(calls), 3)
+            self.assertEqual(self.sleeps, [heartbeat.PING_RETRY_DELAY_S] * 2)
+            row = store.latest_health(db, "hc_ping")
+            self.assertEqual(row["ok"], 1)
+            self.assertIn("attempt 3", row["detail"])
+            self.assertNotIn("example.invalid", row["detail"])
+        finally:
+            db.close()
+
+    def test_retries_are_bounded(self):
+        calls = []
+
+        def dead_send(url, timeout=10):
+            calls.append(url)
+            raise TimeoutError("read timed out")
+
+        heartbeat._send = dead_send
+        result = heartbeat.ping_success()
+        self.assertEqual(result, {"ping": "failed", "url_set": True})
+        self.assertEqual(len(calls), heartbeat.PING_ATTEMPTS)
+
     # -- capture mode ------------------------------------------------------
 
     def test_capture_mode(self):
```
