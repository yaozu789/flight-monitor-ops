"""Weekly price report: a Monday summary of stored prices, sent even when
there were no deals, so the user sees the general price level.

Built ONLY from the local database (observations, outbox, budget ledger,
health): it sends no flight searches. The first scheduled run at or after
Monday REPORT_HOUR_UTC of an ISO week stores that week's report; the relay
batch carries it until the agent records it with ``report-mark`` (at most
REPORT_MAX_EMITS runs, so a missed mark cannot repeat it all week).
"""
from __future__ import annotations

import json
import math
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

import baseline
import budget
import dashboard
from outbox import CHART_BASE

# Monday 16:00 UTC = 09:00 PDT / 08:00 PST (no tz database needed on the VM).
REPORT_WEEKDAY = 0
REPORT_HOUR_UTC = 16
REPORT_MAX_EMITS = 2
REPORT_DAYS = 7
CHART_DAYS = 28


def report_week(now: datetime) -> str | None:
    """ISO week label (YYYY-Www) once this week's report time has passed."""
    monday = (now - timedelta(days=now.weekday())).replace(
        hour=REPORT_HOUR_UTC, minute=0, second=0, microsecond=0)
    if now < monday:
        return None
    y, w, _ = now.isocalendar()
    return f"{y}-W{w:02d}"


def _cheapest(db, watch_id, since, until):
    return db.execute(
        """SELECT origin, destination, depart_date, return_date, airline,
                  stops, price_usd, observed_at
           FROM observations
           WHERE watch_id = ? AND price_usd IS NOT NULL
             AND observed_at >= ? AND observed_at < ?
           ORDER BY price_usd, observed_at LIMIT 1""",
        (watch_id, since, until)).fetchone()


def _nights(row) -> int:
    return (datetime.fromisoformat(row["return_date"])
            - datetime.fromisoformat(row["depart_date"])).days


def _watch_lines(db, w, now) -> list[str]:
    since = (now - timedelta(days=REPORT_DAYS)).isoformat()
    prior = (now - timedelta(days=2 * REPORT_DAYS)).isoformat()
    until = now.isoformat()
    cap = w["absolute_cap_usd"]
    lines = [f"{w['id']} (cap ${cap:,.0f})"]
    n = db.execute(
        """SELECT COUNT(*) AS n, COUNT(DISTINCT substr(observed_at, 1, 10))
                  AS days
           FROM observations WHERE watch_id = ? AND price_usd IS NOT NULL
             AND observed_at >= ? AND observed_at < ?""",
        (w["id"], since, until)).fetchone()
    best = _cheapest(db, w["id"], since, until)
    if best is None:
        lines.append("  No fares stored in the last 7 days.")
        return lines
    lines.append(
        f"  Cheapest seen: ${best['price_usd']:,.0f}, {best['origin']} -> "
        f"{best['destination']} {best['depart_date']} -> "
        f"{best['return_date']} ({_nights(best)} nights), "
        f"{best['airline'] or 'unknown airline'}, seen "
        f"{best['observed_at'][5:10]}")
    before = _cheapest(db, w["id"], prior, since)
    if before is None:
        lines.append("  Prior 7 days: no fares stored.")
    else:
        diff = best["price_usd"] - before["price_usd"]
        trend = ("same" if diff == 0 else
                 f"{'down' if diff < 0 else 'up'} ${abs(diff):,.0f}")
        lines.append(f"  Prior 7 days: ${before['price_usd']:,.0f} ({trend})")
    months = db.execute(
        """SELECT substr(depart_date, 1, 7) AS m, MIN(price_usd) AS low
           FROM observations WHERE watch_id = ? AND price_usd IS NOT NULL
             AND observed_at >= ? AND observed_at < ?
           GROUP BY m ORDER BY m""", (w["id"], since, until)).fetchall()
    lines.append("  By departure month: " + " | ".join(
        f"{datetime.strptime(r['m'], '%Y-%m').strftime('%b %y')} "
        f"${r['low']:,.0f}" for r in months))
    lines.append(f"  {n['n']} fares stored on {n['days']} day(s)")
    return lines


def _window_label(win: dict) -> str:
    lo, hi = win["lo"], win["hi"]
    if hi <= 60:
        return f"Next {max(1, lo // 7)}-{hi // 7} weeks"
    return f"{lo // 30}-{hi // 30} months out"


def _trip_label(cfg: dict) -> str:
    if cfg["windows"] and all(x.get("return_weekday") is not None
                              for x in cfg["windows"]):
        return "weekend trips"
    nights = cfg["nights"]
    return ("about " if len(nights) == 1 else "") + (
        " or ".join(str(n) for n in nights) + " nights")


def _utc(ts: str) -> datetime:
    """A stored timestamp in UTC: naive means UTC; an explicit offset is
    converted, never overwritten."""
    dt = datetime.fromisoformat(ts)
    return (dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None
            else dt.astimezone(timezone.utc))


def _age(observed_at: str, now: datetime) -> str:
    hours = (now - _utc(observed_at)).total_seconds() / 3600
    return f"{hours:.0f}h ago" if hours < 48 else f"{hours / 24:.0f}d ago"


def _sqlite_wd(python_weekday: int) -> str:
    """Python weekday (0 = Mon) as SQLite strftime('%w') (0 = Sun)."""
    return str((int(python_weekday) + 1) % 7)


def _window_cheapest(db, watch_id, win, lo_date, hi_date, nights, since,
                     until, origins, destinations, max_stops):
    """Cheapest stored fare whose departure is inside the window NOW, on
    one of its weekdays (if any), with a stay the watch tracks and, for a
    fixed-return window, returning on that weekday."""
    where = ["watch_id = ?", "price_usd IS NOT NULL", "observed_at >= ?",
             "observed_at < ?", "depart_date >= ?", "depart_date <= ?",
             "CAST(julianday(return_date) - julianday(depart_date) AS INTEGER)"
             f" IN ({','.join('?' * len(nights))})"]
    params = [watch_id, since, until, lo_date, hi_date, *nights]
    # Only itineraries the watch actually tracks: its airports and stop
    # limit (older rows can carry a layover as the destination, or more
    # stops than allowed; an unknown stop count cannot be shown to fit).
    for col, codes in (("origin", origins), ("destination", destinations)):
        where.append(f"{col} IN ({','.join('?' * len(codes))})")
        params += list(codes)
    where.append("stops IS NOT NULL AND stops <= ?")
    params.append(int(max_stops))
    if win.get("weekdays"):
        days = [_sqlite_wd(d) for d in win["weekdays"]]
        where.append("strftime('%w', depart_date) IN "
                     f"({','.join('?' * len(days))})")
        params += days
    if win.get("return_weekday") is not None:
        where.append("strftime('%w', return_date) = ?")
        params.append(_sqlite_wd(win["return_weekday"]))
    return db.execute(
        f"""SELECT origin, destination, depart_date, return_date, airline,
                   stops, price_usd, observed_at
            FROM observations WHERE {' AND '.join(where)}
            ORDER BY price_usd, observed_at LIMIT 1""", params).fetchone()


def _windowed_lines(db, w, cfg, now) -> list[str]:
    """Digest block for a named-window watch (redesign A4): per window, the
    cheapest observed fare with its dates, stay, airline, stops and age,
    last week's low, and how many departure dates are fresh."""
    since = (now - timedelta(days=REPORT_DAYS)).isoformat()
    prior = (now - timedelta(days=2 * REPORT_DAYS)).isoformat()
    until = now.isoformat()
    lines = [f"{w['id']} ({_trip_label(cfg)})"]
    today = now.date()
    for win in cfg["windows"]:
        lo_date = (today + timedelta(days=win["lo"])).isoformat()
        hi_date = (today + timedelta(days=win["hi"])).isoformat()
        track = (cfg["origins"], cfg["destinations"], w.get("max_stops", 1))
        best = _window_cheapest(db, w["id"], win, lo_date, hi_date,
                                cfg["nights"], since, until, *track)
        before = _window_cheapest(db, w["id"], win, lo_date, hi_date,
                                  cfg["nights"], prior, since, *track)
        label = _window_label(win)
        if best is None:
            lines.append(f"  {label}: no fares stored this week")
            if before is not None:
                lines.append(f"    prior week low "
                             f"${before['price_usd']:,.0f}")
        else:
            stops = ("nonstop" if best["stops"] == 0
                     else f"{best['stops']} stop"
                     + ("s" if (best["stops"] or 0) > 1 else ""))
            stale = (" (stale)" if _utc(best["observed_at"]) < now - timedelta(
                hours=win["refresh_hours"]) else "")
            lines.append(
                f"  {label}: ${best['price_usd']:,.0f} {best['origin']} -> "
                f"{best['destination']} {best['depart_date']} -> "
                f"{best['return_date']} ({_nights(best)} nights), "
                f"{best['airline'] or 'unknown airline'} {stops}, seen "
                f"{_age(best['observed_at'], now)}{stale}")
            base = baseline.baseline(db, w["id"], cfg, best["origin"],
                                     best["destination"], best["depart_date"],
                                     best["return_date"], now)
            if base and base["armed"] and base["median"] is not None:
                lines.append(f"    typical ${base['median']:,.0f} for "
                             f"{best['origin']} -> {best['destination']} "
                             f"(median of {base['samples']} daily lows)")
            elif base:
                lines.append(f"    typical price: building history "
                             f"({base['samples']}/{baseline.MIN_SAMPLES} "
                             f"samples, {base['days']}/{baseline.MIN_DAYS} "
                             f"days)")
            if before is not None:
                diff = best["price_usd"] - before["price_usd"]
                trend = ("same" if diff == 0 else
                         f"{'down' if diff < 0 else 'up'} ${abs(diff):,.0f}")
                lines.append(f"    prior week low ${before['price_usd']:,.0f}"
                             f" ({trend})")
        fresh_since = (now - timedelta(hours=win["refresh_hours"])
                       ).isoformat(timespec="seconds")
        # Expected departure DATES come from the window itself, not from
        # the cells minted so far: the report can run before today's
        # minting, and a date with no cells yet is unchecked, not absent.
        # A date is fresh only when every trip length on it was checked
        # within the target.
        import sentinel  # lazy: only named-window reports need it
        start, end = sentinel._window_bounds(win, today)
        expected = {d.isoformat()
                    for d in sentinel._window_dates(win, start, end)}
        per_date = (1 if win.get("return_weekday") is not None
                    else len(cfg["nights"]))
        fresh_dates = {r["depart_date"] for r in db.execute(
            """SELECT depart_date FROM cells
               WHERE group_id = ? AND role = 'sentinel' AND active = 1
                 AND window_name = ? AND depart_date >= ? AND depart_date <= ?
                 AND last_success_at IS NOT NULL AND last_success_at >= ?
               GROUP BY depart_date HAVING COUNT(*) >= ?""",
            (w["id"], win["name"], lo_date, hi_date, fresh_since, per_date))}
        n, fresh = len(expected), len(expected & fresh_dates)
        if n == 0:
            lines.append("    no departure dates in this window")
            continue
        note = "" if fresh == n else f"; {n - fresh} not checked recently"
        lines.append(f"    dates checked in the last "
                     f"{win['refresh_hours']}h: {fresh}/{n}{note}")
    return lines


def chart_url(db, watches, now) -> str | None:
    """Daily low per watch over CHART_DAYS, caps dashed. The data is in the
    URL; nothing is sent anywhere until the link is opened."""
    start = (now - timedelta(days=CHART_DAYS)).date()
    days = [(start + timedelta(days=i + 1)).isoformat()
            for i in range(CHART_DAYS)]
    datasets = []
    for w in watches:
        rows = db.execute(
            """SELECT substr(observed_at, 1, 10) AS day, MIN(price_usd) AS low
               FROM observations WHERE watch_id = ? AND price_usd IS NOT NULL
                 AND observed_at >= ? AND observed_at < ?
               GROUP BY day""",
            (w["id"], days[0], now.isoformat())).fetchall()
        lows = {r["day"]: r["low"] for r in rows
                if math.isfinite(r["low"])}
        if not lows:
            continue
        datasets.append({"label": f"{w['id']} daily low",
                         "data": [round(lows[d]) if d in lows else None
                                  for d in days],
                         "fill": False, "spanGaps": True})
        if budget.alert_mode(w) != "cap":
            continue  # caps are not an alert rule outside cap mode
        datasets.append({"label": f"{w['id']} cap",
                         # Two end points keep the URL short.
                         "data": ([round(w["absolute_cap_usd"])]
                                  + [None] * (len(days) - 2)
                                  + [round(w["absolute_cap_usd"])]),
                         "fill": False, "spanGaps": True,
                         "borderDash": [6, 4],
                         "pointRadius": 0})
    if not datasets:
        return None
    config = {"type": "line",
              "data": {"labels": [d[5:] for d in days],
                       "datasets": datasets},
              "options": {"title": {"display": True, "text": (
                  f"Daily lowest stored fare, last {CHART_DAYS} days, USD")}}}
    return (CHART_BASE + "?c="
            + quote(json.dumps(config, separators=(",", ":")), safe=""))


def build_report(db, watches, now: datetime, week: str) -> str:
    since = now - timedelta(days=REPORT_DAYS)
    windowed = any(w.get("windows") for w in watches)
    lines = [f"Weekly flight price report -- {since.date()} to {now.date()}",
             ("(cheapest fare stored this week per departure window; prices "
              "are round trip, rechecked only when alerted)" if windowed else
              "(stored fares, any dates and trip lengths in the window)")]
    for w in watches:
        if w.get("windows"):
            import run  # group_cfg_for parses the windows (lazy: no cycle)
            lines += _windowed_lines(db, w, run.group_cfg_for(w), now)
        else:
            lines += _watch_lines(db, w, now)
    if budget.all_collect(watches):
        lines.append("Deal alerts: off while price history builds up")
    else:
        ids = [w["id"] for w in watches]
        alerts = db.execute(
            f"""SELECT watch_id, COUNT(*) AS n FROM outbox
                WHERE prepared_at >= ?
                  AND watch_id IN ({','.join('?' * len(ids))})
                GROUP BY watch_id ORDER BY watch_id""",
            (since.isoformat(), *ids)).fetchall()
        lines.append("Deal alerts handed off this week: " + (", ".join(
            f"{r['watch_id']} {r['n']}" for r in alerts) or "none"))
    used = db.execute(
        "SELECT COALESCE(SUM(attempts), 0) AS n FROM budget_ledger "
        "WHERE created_at >= ?",
        (since.isoformat(timespec="seconds"),)).fetchone()["n"]
    throttles = db.execute(
        "SELECT COUNT(*) AS n FROM health WHERE check_name = 'throttle' "
        "AND ok = 0 AND checked_at >= ?", (since.isoformat(),)).fetchone()["n"]
    lines.append(f"Google requests: {used:,} in 7 days (limit "
                 f"{budget.daily_ceiling(watches)}/day); throttles: {throttles}")
    url = chart_url(db, watches, now)
    if url:
        lines.append(f"Chart (daily lows, last {CHART_DAYS} days): {url}")
    lines.append("Dashboard (zoom, runs, requests): "
                 + dashboard.link(db, watches, now))
    lines.append(f"Weekly report: {week}")
    return "\n".join(lines)


# The week armed by prepare() in THIS process: claim() hands it to at most
# one relay batch per invocation, so repeat or manual runs cannot re-send it.
_armed_week: str | None = None


def prepare(db, watches, now: datetime | None = None) -> str | None:
    """Scheduled runs only: store this week's report once it is due and arm
    it for this invocation's relay batch. Returns the week or None."""
    global _armed_week
    _armed_week = None
    now = now or datetime.now(timezone.utc)
    week = report_week(now)
    if week is None:
        return None
    if db.execute("SELECT 1 FROM weekly_reports WHERE week = ?",
                  (week,)).fetchone() is None:
        db.execute(
            """INSERT OR IGNORE INTO weekly_reports (week, message_text,
                                                    created_at)
               VALUES (?, ?, ?)""",
            (week, build_report(db, watches, now, week),
             now.isoformat(timespec="seconds")))
        db.commit()
    _armed_week = week
    return week


def claim(db, slot: int) -> dict | None:
    """Called while building the relay batch, right before it is printed:
    count one emit (unrelayed, under the cap, not already sent in *slot*)
    and return the report for the batch. Disarms either way."""
    global _armed_week
    week, _armed_week = _armed_week, None
    if week is None:
        return None
    row = db.execute(
        """UPDATE weekly_reports SET emits = emits + 1, last_slot = ?
           WHERE week = ? AND relayed_at IS NULL AND emits < ?
             AND (last_slot IS NULL OR last_slot <> ?)
           RETURNING week, message_text""",
        (slot, week, REPORT_MAX_EMITS, slot)).fetchone()
    db.commit()
    return {"week": row["week"], "message": row["message_text"]} if row else None


def mark(db, week: str) -> bool:
    row = db.execute(
        """UPDATE weekly_reports SET relayed_at = ?
           WHERE week = ? AND relayed_at IS NULL RETURNING week""",
        (datetime.now(timezone.utc).isoformat(timespec="seconds"),
         week)).fetchone()
    db.commit()
    return row is not None
