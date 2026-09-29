"""Monitoring dashboard link: the data rides in the URL fragment.

The page (``dashboard/index.html``, served statically) holds no data. This
module packs stored numbers into ``#v1.<base64url(deflate-raw(json))>``;
browsers never send the fragment to the server, so the host only serves
the page. Built from the local database only: no flight searches.
"""
from __future__ import annotations

import base64
import json
import zlib

import budget
from datetime import datetime, timedelta, timezone

DASHBOARD_URL = "https://yaozu789.github.io/flight-monitor/dashboard/"
PRICE_DAYS = 180
RUN_DAYS = 14


def payload(db, watches, now: datetime | None = None) -> dict:
    now = now or datetime.now(timezone.utc)
    price_since = (now - timedelta(days=PRICE_DAYS)).date().isoformat()
    run_since = now - timedelta(days=RUN_DAYS)
    prices = {}
    for w in watches:
        prices[w["id"]] = [
            [r["day"], round(r["low"]), r["n"]] for r in db.execute(
                """SELECT substr(observed_at, 1, 10) AS day,
                          MIN(price_usd) AS low, COUNT(*) AS n
                   FROM observations
                   WHERE watch_id = ? AND price_usd IS NOT NULL
                     AND observed_at >= ?
                   GROUP BY day ORDER BY day""", (w["id"], price_since))]
    first_slot = int(run_since.timestamp() // 1800)
    runs, prev = [], first_slot
    for r in db.execute(
            """SELECT slot_id, status, attempts, stalled FROM run_slots
               WHERE slot_id >= ? ORDER BY slot_id""", (first_slot,)):
        # Slot deltas keep the link short.
        runs.append([r["slot_id"] - prev, r["status"], r["attempts"],
                     r["stalled"]])
        prev = r["slot_id"]
    spend: dict[str, dict[str, int]] = {}
    for r in db.execute(
            "SELECT day, category, n FROM daily_counters WHERE day >= ? "
            "ORDER BY day", (run_since.date().isoformat(),)):
        spend.setdefault(r["day"], {})[r["category"]] = r["n"]
    events = [[r["checked_at"][:16], r["check_name"], r["detail"][:80]]
              for r in db.execute(
                  """SELECT checked_at, check_name, detail FROM health
                     WHERE ok = 0 AND check_name <> 'hc_ping'
                       AND checked_at >= ?
                     ORDER BY checked_at DESC LIMIT 20""",
                  (run_since.isoformat(),))]
    return {"v": 1, "at": now.isoformat(timespec="minutes"),
            # A cap is only an alert rule in cap mode; otherwise null, and
            # the page draws no cap line.
            "watches": [[w["id"], (w["absolute_cap_usd"]
                                   if budget.alert_mode(w) == "cap"
                                   else None)] for w in watches],
            "prices": prices, "run0": first_slot, "runs": runs,
            "spend": spend, "events": events}


def encode(data: dict) -> str:
    raw = json.dumps(data, separators=(",", ":")).encode()
    c = zlib.compressobj(9, zlib.DEFLATED, -15)  # raw deflate
    packed = c.compress(raw) + c.flush()
    return "v1." + base64.urlsafe_b64encode(packed).decode().rstrip("=")


def decode(fragment: str) -> dict:
    """Inverse of encode (tests; the page does the same in JavaScript)."""
    body = fragment.removeprefix("v1.")
    packed = base64.urlsafe_b64decode(body + "=" * (-len(body) % 4))
    return json.loads(zlib.decompress(packed, -15))


def link(db, watches, now: datetime | None = None) -> str:
    return DASHBOARD_URL + "#" + encode(payload(db, watches, now))
