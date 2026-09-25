"""Offline fake for the fli HTTP transport (tests only — never touches the network).

Interposes at the transport layer's send-handler seam: every HTTP attempt the
budget guard reserves is routed here instead of the real session. Modes:

- "ok": returns the captured ds:1 search page (JFK->LHR fixture).
- "http429": returns HTTP 429 (with Retry-After when given). v3: the
  transport wrapper must detect the throttle in the SUCCESS path by
  inspecting the returned response -- the fake no longer raises.
- "captcha": returns HTTP 200 with a CAPTCHA / unusual-traffic body.
- "blank": returns HTTP 200 with an empty body (fli decodes no payload).

Recorded stats: total sends and the budget category each send billed.
"""
from __future__ import annotations

import sys
import threading
from pathlib import Path
from types import SimpleNamespace

# Project root (parent of tests/): holds store.py and the modules under test.
# (Test modules already put it on sys.path before importing this file; this
# keeps fake_transport importable standalone too.)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Captured Google Flights page from fli's test suite (pinned commit 881aee5,
# MIT license, see tests/fixtures/LICENSE-fli.txt). Kept inside the repo so
# the suite runs on any clean checkout.
FIXTURE_HTML = (
    Path(__file__).resolve().parent / "fixtures"
    / "search_page_jfk_lhr_nonstop_ds1.html"
).read_text()

# Live results pages (captured 2026-09-25) also embed Google's site route
# list, which contains "/recaptcha/challenge". The vendored fixture is
# trimmed and lacks it, which let a substring CAPTCHA detector pass every
# test while flagging every real search. Append a verbatim excerpt so all
# "ok" responses look like production pages.
LIVE_ROUTE_LIST = (
    '<script>["/preferences/notifications","/preferences/notifications/*",'
    '"/producer","/producer/*","/recaptcha/challenge","/recaptcha/challenge/*",'
    '"/recaptcha/challengepage","/recaptcha/challengepage/*","/retail",'
    '"/retail/*","/s2","/s2/*"]</script>'
)
FIXTURE_HTML = FIXTURE_HTML + LIVE_ROUTE_LIST

CAPTCHA_BODY = (
    "<html><body><h1>About this page</h1>"
    "<p>Our systems have detected unusual traffic from your computer network. "
    "Please try your request again later.</p></body></html>"
)


class FakeResponse:
    """Duck-typed stand-in for curl_cffi's Response (only what fli reads)."""

    def __init__(self, text: str = FIXTURE_HTML, status_code: int = 200,
                 headers: dict | None = None,
                 url: str = "https://www.google.com/travel/flights"):
        self.text = text
        self.status_code = status_code
        self.url = url
        self.headers = headers or {}

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise FakeHTTPError(self.status_code, self.headers, self.text)


class FakeHTTPError(Exception):
    """Duck-typed like curl_cffi.requests.exceptions.HTTPError: carries a
    .response with .status_code / .headers / .text so the transport's
    throttle detector classifies it without importing curl_cffi."""

    def __init__(self, status_code: int, headers: dict | None = None,
                 body: str = ""):
        super().__init__(f"Fake HTTP {status_code}")
        self.response = SimpleNamespace(
            status_code=status_code, headers=headers or {}, text=body)


class FakeTransport:
    """Stateful fake send handler. Thread-safe: the 151-attempt test hammers
    it from many threads."""

    def __init__(self, mode: str = "ok", retry_after: str | None = None):
        self.mode = mode
        self.retry_after = retry_after
        self._lock = threading.Lock()
        self.sends = 0
        self.categories: list[str] = []
        self.urls: list[str] = []

    def __call__(self, method: str, url: str, kwargs: dict, proceed):
        import transport as transport_mod
        with self._lock:
            self.sends += 1
            self.categories.append(transport_mod.current_category())
            self.urls.append(url)
        if self.mode == "ok":
            return FakeResponse()
        if self.mode == "http429":
            # v3: return the 429 response (with Retry-After when given) so
            # the transport wrapper must detect it in the success path by
            # inspecting the returned response. Never raises.
            headers = {"Retry-After": self.retry_after} if self.retry_after else {}
            return FakeResponse(text="Too Many Requests", status_code=429,
                                headers=headers)
        if self.mode == "captcha":
            return FakeResponse(text=CAPTCHA_BODY)
        if self.mode == "blank":
            return FakeResponse(text="<html><body></body></html>")
        raise AssertionError(f"unknown fake mode {self.mode!r}")


_installed: FakeTransport | None = None


def install_fake_transport(guard, mode: str = "ok",
                           retry_after: str | None = None) -> FakeTransport:
    """Install the budget guard AND route its HTTP attempts to the fake."""
    import transport as transport_mod
    global _installed
    fake = FakeTransport(mode=mode, retry_after=retry_after)
    transport_mod.install_transport_guard(guard)
    transport_mod.set_send_handler(fake)
    _installed = fake
    return fake


def uninstall_fake_transport() -> None:
    import transport as transport_mod
    global _installed
    transport_mod.set_send_handler(None)
    transport_mod.uninstall_transport_guard()
    _installed = None
