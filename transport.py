"""HTTP transport instrumentation for the Flight Monitor trial (v3).

Core defect this fixes (spec item 1): the v1 patch counted ``.search()``
calls, but one fli round-trip search costs 1 + top_n real HTTP page fetches
(outbound page + one expansion per top_n choice). This module therefore
counts ACTUAL HTTP attempts at the real send path by wrapping
``curl_cffi.requests.Session.get`` / ``.post`` (the class attributes fli's
client ultimately calls per page).

It also:
- funnels ``fli.search._tfs.fetch_payload`` (patched in all three namespaces
  that reference it: ``fli.search._tfs``, ``fli.search.flights``,
  ``fli.search.dates``) through a consecutive-blank-page tripwire, and
- forces ``fli.search._concurrency.configure_concurrency(1)`` (sequential).

v3 changes:
- ``Session.get``/``Session.post`` are patched AT MODULE IMPORT, not during
  ``install_transport_guard()``: a fresh subprocess importing this module is
  fail-closed from the first send, with no install step needed to make
  unarmed sends refuse (item 1).
- The success path inspects RETURNED responses for 429 and suspect 403
  (status 403 + the throttle body marker) BEFORE returning them -- the old
  code only checked CAPTCHA markers there, so a returned 429 sailed through
  to fli as a normal response (item 4).
- Backoff is "Retry-After header, else exponential": the parsed Retry-After
  header is used when present; the exponential 2**n curve applies ONLY when
  the header is absent (item 8). The old code took max(header, exponential),
  so the exponential always dominated small headers.
- ``BudgetExhausted`` is a ``BaseException`` (budget.py, item 2): it is
  raised by ``guard.reserve()`` BEFORE the send and propagates untouched --
  never wrapped by fli's ``except Exception`` into ``_wrap_request_error``.
  (This tenacity version still spins its 3 attempts, but every attempt fails
  fast inside ``reserve()`` with zero sends and zero ledger rows, and the
  original ``BudgetExhausted`` propagates unwrapped -- never a "unknown".)
- Throttle backoff is persisted through the guard's private connection
  (``guard.persist_throttle``), never the caller's.

``import store`` below resolves to the project's ``store.py``; the budget
ledger and throttle persistence live there.
"""
from __future__ import annotations

import contextlib
import functools
import threading
import time

from curl_cffi.requests import Session

import store  # shared DB contract (project store.py)

CAPTCHA_MARKERS = ("unusual traffic", "captcha", "recaptcha")
# v3 item 4: body marker identifying a suspect-403 throttle response.
SUSPECT_403_MARKERS = ("rate-limit", "rate limit", "too many requests")


class ThrottleDetected(BaseException):
    """Raised when Google throttles us: HTTP 429, suspect 403, CAPTCHA /
    unusual-traffic page, or consecutive blank search pages.

    MUST stay a BaseException (never Exception): fli's tenacity retry wrapper
    only retries Exception subclasses, so a throttle must propagate without
    being retried.
    """

    def __init__(self, kind: str, retry_after_s: float | None = None,
                 detail: str = ""):
        super().__init__(
            "throttle detected: %s%s" % (kind, f" ({detail})" if detail else ""))
        self.kind = kind
        self.retry_after_s = retry_after_s
        self.detail = detail


# ---------------------------------------------------------------------------
# module state
# ---------------------------------------------------------------------------

_installed_guard = None          # the BudgetGuard currently armed (or None)
_send_handler = None            # test seam: fn(method, url, kwargs, proceed)
_orig_fetch_payload = None      # saved original fetch_payload
_fetch_patched = False          # fetch_payload refs patched?

_thread_state = threading.local()

_blank_lock = threading.Lock()
_blank_consecutive = 0           # module-level consecutive-blank counter


# ---------------------------------------------------------------------------
# Session patch: applied AT IMPORT (v3 item 1)
# ---------------------------------------------------------------------------
#
# Fail closed from the first send: importing this module patches
# Session.get/post immediately. With no guard installed, require_guard()
# raises RuntimeError and nothing is sent. install_transport_guard() only
# registers the guard (+ fetch_payload patch + sequential concurrency).

_orig_session_get = Session.get
_orig_session_post = Session.post


def _guarded_get(self, *args, **kwargs):
    return _do_send("GET", _orig_session_get, self, args, kwargs)


def _guarded_post(self, *args, **kwargs):
    return _do_send("POST", _orig_session_post, self, args, kwargs)


Session.get = _guarded_get
Session.post = _guarded_post


# ---------------------------------------------------------------------------
# public API
# ---------------------------------------------------------------------------

def install_transport_guard(guard) -> None:
    """Arm the transport instrumentation for *guard*.

    The Session.get/post wrappers have been in place since import (v3 item
    1); this registers the guard, patches the three ``fetch_payload``
    references, and forces fli to sequential execution. Idempotent for the
    same guard object; raises RuntimeError if a DIFFERENT guard is already
    installed.
    """
    global _installed_guard
    global _orig_fetch_payload, _fetch_patched

    if _installed_guard is not None:
        if _installed_guard is guard:
            return  # idempotent
        raise RuntimeError("a different transport guard is already installed")

    import fli.search._concurrency
    import fli.search._tfs
    import fli.search.dates
    import fli.search.flights

    if not _fetch_patched:
        _orig_fetch_payload = fli.search._tfs.fetch_payload
        wrapper = _make_fetch_payload_wrapper()
        fli.search._tfs.fetch_payload = wrapper
        fli.search.flights.fetch_payload = wrapper
        fli.search.dates.fetch_payload = wrapper
        _fetch_patched = True

    fli.search._concurrency.configure_concurrency(1)
    _installed_guard = guard


def uninstall_transport_guard() -> None:
    """Disarm the transport guard (test isolation).

    Clears the installed guard and restores the ``fetch_payload`` references
    to their originals. NOTE: the ``Session.get``/``Session.post`` wrappers
    intentionally STAY installed: with no guard they fail closed
    (``require_guard()`` raises ``RuntimeError``) instead of silently
    restoring raw network access. This is what the fail-closed test requires:
    after uninstall, any ``Session().get(...)`` raises RuntimeError and sends
    nothing.
    """
    global _installed_guard, _fetch_patched, _blank_consecutive

    _installed_guard = None

    if _fetch_patched:
        import fli.search._tfs
        import fli.search.dates
        import fli.search.flights
        fli.search._tfs.fetch_payload = _orig_fetch_payload
        fli.search.flights.fetch_payload = _orig_fetch_payload
        fli.search.dates.fetch_payload = _orig_fetch_payload
        _fetch_patched = False

    with _blank_lock:
        _blank_consecutive = 0


def require_guard():
    """Return the installed guard, or raise RuntimeError (fail closed)."""
    if _installed_guard is None:
        raise RuntimeError("transport guard not installed")
    return _installed_guard


def current_category() -> str:
    """Thread-local budget category for the next send; default "manual"."""
    return getattr(_thread_state, "category", "manual")


@contextlib.contextmanager
def category_scope(category):
    """Set the thread-local budget category for the enclosed block."""
    prev = getattr(_thread_state, "category", "manual")
    _thread_state.category = category
    try:
        yield
    finally:
        _thread_state.category = prev


def set_send_handler(fn) -> None:
    """Test seam. ``fn(method, url, kwargs, proceed) -> response``.

    When None (default), the real session method is called.
    """
    global _send_handler
    _send_handler = fn


# ---------------------------------------------------------------------------
# send wrapper (wraps original Session.get / Session.post)
# ---------------------------------------------------------------------------

def _do_send(method, orig, self_obj, args, kwargs):
    # 1. Fail closed: no guard installed -> RuntimeError, nothing sent.
    guard = require_guard()
    # 2. Reserve budget for this category. BudgetExhausted (BaseException)
    #    propagates untouched: nothing is sent, nothing is recorded, and
    #    fli's `except Exception` cannot wrap it (v3 item 2).
    category = current_category()
    guard.reserve(category)

    url = args[0] if args else kwargs.get("url", "")
    handler = _send_handler

    def do_real():
        return orig(self_obj, *args, **kwargs)

    call = (lambda: handler(method, url, kwargs, do_real)) if handler else do_real

    # 3. Call handler-or-original; classify failures.
    try:
        resp = call()
    except BaseException as exc:
        if isinstance(exc, ThrottleDetected):
            raise
        _raise_if_throttle(guard, method, url, exc)
        raise  # not a throttle -> re-raise unchanged

    # 4. Success path (v3 item 4): inspect the RETURNED response for 429 /
    #    suspect 403 / CAPTCHA before handing it back to fli. The old code
    #    only checked the CAPTCHA marker here, so a returned 429 was treated
    #    as a normal response.
    _raise_if_throttled_response(guard, method, url, resp)
    return resp


def _classify_throttle(status, body) -> str | None:
    """Kind of throttle for a (status, body) pair, or None.

    429 -> "http_429"; 403 with the throttle body marker -> "suspect_403";
    CAPTCHA / unusual-traffic marker -> "captcha".
    """
    if status == 429:
        return "http_429"
    if status == 403 and _has_suspect_403_marker(body):
        return "suspect_403"
    if _has_captcha_marker(body):
        return "captcha"
    return None


def _raise_if_throttled_response(guard, method, url, resp) -> None:
    """v3 item 4: inspect a RETURNED response; on a throttle, persist the
    backoff and raise ThrottleDetected. Returns silently otherwise."""
    status = getattr(resp, "status_code", None)
    headers = getattr(resp, "headers", {}) or {}
    body = getattr(resp, "text", "") or ""
    kind = _classify_throttle(status, body)
    if kind is None:
        return
    detail = "%s on %s %s" % (kind.replace("_", " "), method, url)
    retry_after_s = _record_throttle(guard, kind, headers, detail=detail)
    raise ThrottleDetected(kind, retry_after_s, detail)


def _raise_if_throttle(guard, method, url, exc) -> None:
    """Duck-type *exc*; on 429 / suspect 403 / CAPTCHA marker persist backoff
    and raise ThrottleDetected. Returns (caller re-raises) when it is not a
    throttle."""
    resp = getattr(exc, "response", None)
    status = getattr(resp, "status_code", None)
    headers = getattr(resp, "headers", {}) or {}
    body = getattr(resp, "text", "") or ""
    kind = _classify_throttle(status, body)
    if kind is None:
        return
    detail = "%s on %s %s (from error)" % (kind.replace("_", " "), method, url)
    retry_after_s = _record_throttle(guard, kind, headers, detail=detail)
    raise ThrottleDetected(kind, retry_after_s, detail)


def _has_captcha_marker(body) -> bool:
    lowered = (body or "").lower()
    return any(marker in lowered for marker in CAPTCHA_MARKERS)


def _has_suspect_403_marker(body) -> bool:
    """v3 item 4: the throttle body marker on a 403 response."""
    lowered = (body or "").lower()
    return any(marker in lowered for marker in SUSPECT_403_MARKERS)


def _parse_retry_after(headers) -> float | None:
    """Retry-After header in seconds, clamped to 60..7200; None if
    absent/unparseable."""
    try:
        items = headers.items()
    except AttributeError:
        return None
    raw = None
    for key, value in items:
        if str(key).lower() == "retry-after":
            raw = value
            break
    if raw is None:
        return None
    try:
        secs = int(str(raw).strip())
    except (TypeError, ValueError):
        return None
    return float(max(60, min(7200, secs)))


def _record_throttle(guard, kind, headers, detail="") -> float | None:
    """Persist a throttle backoff and return the parsed Retry-After.

    ``consecutive`` = previous consecutive + 1 (read via the guard's private
    connection). v3 item 8: the parsed Retry-After header is used WHEN
    PRESENT; the exponential curve ``min(900 * 2**(consecutive-1), 14400)``
    applies ONLY when the header is absent.
    """
    retry_after_s = _parse_retry_after(headers or {})
    prev = guard.read_backoff() or {}
    consecutive = int(prev.get("consecutive", 0) or 0) + 1
    if retry_after_s is not None:
        backoff_s = retry_after_s
    else:
        backoff_s = min(900 * 2 ** (consecutive - 1), 14400)
    until = time.time() + backoff_s
    guard.persist_throttle(kind, until, retry_after_s, detail or kind)
    return retry_after_s


# ---------------------------------------------------------------------------
# fetch_payload wrapper (blank-page tripwire)
# ---------------------------------------------------------------------------

def _make_fetch_payload_wrapper():
    orig = _orig_fetch_payload

    @functools.wraps(orig)
    def guarded_fetch_payload(client, url):
        result = orig(client, url)
        global _blank_consecutive
        with _blank_lock:
            if result is None:
                _blank_consecutive += 1
                n = _blank_consecutive
            else:
                _blank_consecutive = 0
                return result
        if n >= 3:
            guard = require_guard()
            with _blank_lock:
                _blank_consecutive = 0
            detail = "3 consecutive blank search pages"
            _record_throttle(guard, "blank_pages", {}, detail=detail)
            raise ThrottleDetected("blank_pages", None, detail)
        return result

    return guarded_fetch_payload
