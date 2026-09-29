"""Route baselines and the alert threshold (redesign PR C).

Every alert step (scoring, the verify recheck, retries, burst triggers)
asks ``threshold()`` instead of reading the watch's cap:

- ``cap`` mode: the watch's absolute cap (legacy rule).
- ``collect`` mode: no threshold -- nothing alerts.
- ``relative`` mode: ``RELATIVE_FRACTION`` x the route's median, once the
  route's history is armed; before that, no threshold (silence).

A route bucket is (watch, origin airport, destination airport, departure
window, nights). The window is derived from how far ahead a fare was seen
(depart date minus observation day), so stored history needs no extra
columns. A sample is the cheapest stored fare for one departure date on one
observation day. The baseline uses the LOOKBACK_DAYS before today (today's
own fares never judge themselves) and arms at MIN_SAMPLES samples on
MIN_DAYS distinct days. It is computed once per bucket per UTC day and
stored, so every step that day compares against the same number.
"""
from __future__ import annotations

import statistics
from datetime import date, datetime, timedelta, timezone

import store

RELATIVE_FRACTION = 0.70   # alert at or below 70% of the route median
LOOKBACK_DAYS = 28
MIN_SAMPLES = 50
MIN_DAYS = 14

def _day(now: datetime | None) -> date:
    return (now or datetime.now(timezone.utc)).date()


def window_for(group_cfg: dict, depart_date: str, seen_day: date):
    """The named window a departure fell in on *seen_day*, or None."""
    offset = (date.fromisoformat(depart_date) - seen_day).days
    for win in group_cfg.get("windows") or []:
        if win["lo"] <= offset <= win["hi"]:
            return win
    return None


def bucket_key(watch_id, origin, destination, win, nights) -> str:
    # The window's bounds are part of the key, so changing them starts a
    # fresh baseline the same day instead of reusing a cached one.
    return (f"{watch_id}|{origin}|{destination}|{win['name']}"
            f"[{win['lo']}-{win['hi']}]|{int(nights)}")


def _samples(db, watch_id, origin, destination, win, nights, today: date):
    """(observation day, depart date) -> cheapest price, over the lookback
    window before *today*, for fares whose departure sat in *win* on the day
    they were seen."""
    since = (today - timedelta(days=LOOKBACK_DAYS)).isoformat()
    # Raw timestamp bounds (not substr) so the observed_at index applies:
    # "YYYY-MM-DD" <= "YYYY-MM-DDThh..." < the next day's "YYYY-MM-DD".
    return db.execute(
        """SELECT substr(observed_at, 1, 10) AS day, depart_date,
                  MIN(price_usd) AS low
           FROM observations
           WHERE watch_id = ? AND origin = ? AND destination = ?
             AND price_usd IS NOT NULL
             AND observed_at >= ? AND observed_at < ?
             AND CAST(julianday(return_date) - julianday(depart_date)
                      AS INTEGER) = ?
             AND CAST(julianday(depart_date)
                      - julianday(substr(observed_at, 1, 10)) AS INTEGER)
                 BETWEEN ? AND ?
           GROUP BY day, depart_date""",
        (watch_id, origin, destination, since, today.isoformat(),
         int(nights), win["lo"], win["hi"])).fetchall()


def baseline(db, watch_id, group_cfg, origin, destination, depart_date,
             return_date, now: datetime | None = None) -> dict | None:
    """Today's stored baseline for the route bucket of this itinerary
    (computed and stored on first use each UTC day), or None when the
    departure is outside every window."""
    today = _day(now)
    win = window_for(group_cfg, depart_date, today)
    if win is None:
        return None
    nights = (date.fromisoformat(return_date)
              - date.fromisoformat(depart_date)).days
    key = bucket_key(watch_id, origin, destination, win, nights)
    row = db.execute("SELECT * FROM baselines WHERE bucket = ? AND day = ?",
                     (key, today.isoformat())).fetchone()
    if row is None:
        rows = _samples(db, watch_id, origin, destination, win, nights, today)
        prices = [r["low"] for r in rows]
        days = len({r["day"] for r in rows})
        med = statistics.median(prices) if prices else None
        armed = int(len(prices) >= MIN_SAMPLES and days >= MIN_DAYS)
        db.execute(
            """INSERT OR IGNORE INTO baselines
               (bucket, day, median, samples, days, armed, computed_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (key, today.isoformat(), med, len(prices), days, armed,
             store.utcnow_iso()))
        db.commit()
        row = db.execute("SELECT * FROM baselines WHERE bucket = ? AND day = ?",
                         (key, today.isoformat())).fetchone()
    return dict(row)


def threshold(db, watch: dict, group_cfg: dict, origin, destination,
              depart_date, return_date, now: datetime | None = None
              ) -> tuple[float | None, dict | None]:
    """(alert threshold in USD, baseline) for one itinerary; threshold None
    means "do not alert" (collect mode, or a relative route not armed)."""
    mode = watch.get("alert_mode", "cap")
    if mode == "cap":
        return float(watch["absolute_cap_usd"]), None
    if mode != "relative":
        return None, None
    base = baseline(db, watch["id"], group_cfg, origin, destination,
                    depart_date, return_date, now)
    if base is None or not base["armed"] or base["median"] is None:
        return None, base
    limit = RELATIVE_FRACTION * base["median"]
    if watch.get("max_alert_usd") is not None:
        limit = min(limit, float(watch["max_alert_usd"]))
    return limit, base


def bucket_status(db, watch: dict, group_cfg: dict,
                  now: datetime | None = None) -> list[dict]:
    """Every CONFIGURED route bucket (origin x destination x window x
    nights), with today's stored baseline -- the same numbers alerting
    uses -- including buckets with no samples yet (for ``run.py
    coverage``)."""
    today = _day(now)
    out = []
    for win in group_cfg.get("windows") or []:
        if win.get("return_weekday") is not None:
            nights_list = sorted({(int(win["return_weekday"]) - int(d)) % 7
                                  or 7 for d in win.get("weekdays") or []})
        else:
            nights_list = group_cfg.get("nights") or []
        # A departure inside this window today, for the baseline lookup.
        probe = today + timedelta(days=win["lo"])
        for o in group_cfg["origins"]:
            for d in group_cfg["destinations"]:
                for n in nights_list:
                    base = baseline(db, watch["id"], group_cfg, o, d,
                                    probe.isoformat(),
                                    (probe + timedelta(days=n)).isoformat(),
                                    now)
                    out.append({"origin": o, "destination": d,
                                "window": win["name"], "nights": n,
                                "samples": base["samples"],
                                "days": base["days"],
                                "armed": bool(base["armed"])})
    return out
