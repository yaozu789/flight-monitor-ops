"""healthchecks.io heartbeat (stage 03 rebuild, v2).

Spec item 11 defects fixed vs v1:
  - v1 pinged in a `finally` for every command (including manual ones) and on
    crashes / lock no-ops, falsely marking failures green.
  - v2: ping success ONLY after a scheduled check/broad cycle actually finishes
    (including legitimate budget/backoff/slot no-ops); ping `<url>/fail` on
    crash or repeated lock contention; manual commands NEVER ping; refuse to
    run the schedule when HEALTHCHECKS_PING_URL is unset/empty.

Contract:
  - The ping URL comes ONLY from the HEALTHCHECKS_PING_URL process
    environment variable, read AT CALL TIME. It is never hardcoded, never
    logged, never printed.
  - ping_success(): GET <url>          (plain ping = success)
  - ping_fail():    GET <url>/fail
  - A failed ping never raises; the outcome is recorded in the `health` table
    (check_name "hc_ping") when a db is given. Exception details are reduced
    to the exception class name so the URL cannot leak through an error.
  - validate_schedule_env(): item-11 gate used by the scheduler before a
    scheduled cycle runs.
"""
from __future__ import annotations

import os

import store  # noqa: E402  (project store.py: the v2 contract with add_health)

PING_ENV_VAR = "HEALTHCHECKS_PING_URL"

# In-memory capture for tests: when not None, pings are recorded instead of
# sent. Never used in production paths.
_capture: list[tuple[str, str]] | None = None


def _ping_url() -> str | None:
    """Read HEALTHCHECKS_PING_URL from the process environment AT CALL TIME.

    Returns None when unset or empty.
    """
    return os.environ.get(PING_ENV_VAR) or None


def _send(url: str, timeout: int = 10) -> None:
    """Single overridable send primitive (tests monkeypatch this).

    GET the URL and raise for non-2xx. Anything raised propagates to the
    caller (ping_success/ping_fail convert it to a "failed" result).
    """
    import requests

    requests.get(url, timeout=timeout).raise_for_status()


def _record(db, ok: bool, detail: str) -> None:
    """Record the ping outcome in `health`. Never raises, never logs the URL."""
    if db is None:
        return
    try:
        store.add_health(db, "hc_ping", ok, detail)
    except Exception:  # noqa: BLE001 - recording must not fail the run
        pass


def _ping(kind: str, db=None, timeout: int = 10) -> dict:
    """Shared implementation for ping_success ("success") and ping_fail ("fail").

    Never raises. Returns {"ping": "ok"|"failed"|"skipped", "url_set": bool},
    plus "captured": True when the capture hook recorded the ping instead of
    sending it.
    """
    url = _ping_url()
    if url is None:
        result: dict = {"ping": "skipped", "url_set": False}
        if _capture is not None:
            result["captured"] = True
        _record(db, False, "hc ping skipped: HEALTHCHECKS_PING_URL not set")
        return result

    target = url if kind == "success" else url + "/fail"

    if _capture is not None:
        _capture.append((kind, target))
        _record(db, True, "hc ping ok")
        return {"ping": "ok", "url_set": True, "captured": True}

    try:
        _send(target, timeout=timeout)
    except Exception as e:  # noqa: BLE001 - heartbeat must never raise
        # Exception class name only: the URL (or its value inside an error
        # message) must never leak.
        _record(db, False, f"hc ping failed: {type(e).__name__}")
        return {"ping": "failed", "url_set": True}

    _record(db, True, "hc ping ok")
    return {"ping": "ok", "url_set": True}


def ping_success(db=None, timeout: int = 10) -> dict:
    """GET <url> (plain ping = success).

    Returns {"ping": "ok"|"failed"|"skipped", "url_set": bool}. "skipped"
    (no send attempted) when the env var is unset/empty. Never raises.
    Records via store.add_health(db, "hc_ping", ok, detail) when db is given;
    detail never contains the URL value.
    """
    return _ping("success", db=db, timeout=timeout)


def ping_fail(db=None, timeout: int = 10) -> dict:
    """GET <url>/fail. Same return contract as ping_success."""
    return _ping("fail", db=db, timeout=timeout)


def validate_schedule_env() -> tuple[bool, str]:
    """Item-11 gate: (True, "") when HEALTHCHECKS_PING_URL is set and
    non-empty, else (False, reason). The reason never contains the URL value.
    """
    if _ping_url():
        return True, ""
    return (
        False,
        f"{PING_ENV_VAR} is not set or empty; refusing to run the schedule "
        "(healthchecks.io would mark the cycle green on a false no-op).",
    )


# ---------------------------------------------------------------- test capture

def start_capture() -> list:
    """Subsequent pings append ("success"|"fail", url) to the returned list
    instead of being sent. For tests only; no network is touched."""
    global _capture
    _capture = []
    return _capture


def stop_capture() -> None:
    """Disable capture; pings go through _send again."""
    global _capture
    _capture = None
