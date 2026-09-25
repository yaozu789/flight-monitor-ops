"""Outbox orchestration (v3): alert rendering, durable queueing, retry rechecks.

All state lives in the shared store.py contract; this module only
orchestrates: render -> enqueue -> prepare (via store) -> retry (recheck
first, re-render the message on confirmation, retire on disappearance or on
a third consecutive unknown recheck).

v3 alert identity is (watch_id, departure ISO week): one alert slot per
watch per week. store.enqueue_alert applies the meaningful-drop dedup
(re-arm to pending only on a drop >= max($15, 5%) below the last outboxed
price). Deduped candidates are never emitted to relay: cmd_check relays
only enqueue actions in RELAYABLE_ACTIONS.
"""
from __future__ import annotations

import json
from urllib.parse import quote

import store
import verifier

# Enqueue actions that may be emitted to the relay batch. "deduped" means
# the candidate was already outboxed for this (watch, week) without a
# meaningful drop -- it stays silent.
RELAYABLE_ACTIONS = ("inserted", "rearmed")


def make_alert_id(watch_id: str, depart_week: str) -> str:
    """v3 alert identity: one slot per (watch, departure week)."""
    return f"{watch_id}:{depart_week}"



def _stops_text(stops) -> str:
    if stops == 0:
        return "nonstop"
    if stops == 1:
        return "1 stop"
    # stops is the total over both directions (the search allows at most
    # one stop EACH way), so say so rather than imply one long trip.
    return f"{stops} stops total, both directions"


def _watch_leg(watch: dict, plural_key: str, singular_key: str) -> str:
    plural = watch.get(plural_key)
    if isinstance(plural, (list, tuple)) and plural:
        return str(plural[0])
    if isinstance(plural, str):
        return plural
    single = watch.get(singular_key)
    return str(single) if single is not None else "?"


MIN_HISTORY_DAYS = 3  # distinct observation days before calling a fare a low


def _history_lines(history, price) -> list[str]:
    """Dated price context from STORED observations (never computed by the
    relaying agent), so the alert can be checked rather than trusted.

    The rechecked fare itself is not in the stored history; it is compared
    against it. "Lowest" is only claimed with enough distinct days of data.
    """
    if not history:
        return []
    route = ""
    if history.get("origin") and history.get("destination"):
        route = f"{history['origin']} -> {history['destination']}, "
    scope = (f"{route}{history['depart_from']} to {history['depart_to']} "
             f"departures, {history['duration']}-night trips, last "
             f"{history['lookback_days']} days")
    if not history.get("checks"):
        return [f"Stored history ({scope}): none yet."]
    head = (f"Stored history ({scope}): {history['checks']} fares seen on "
            f"{history['days']} day(s) since {history['since']}.")
    stats = (f"  Low ${history['low']:,.0f} ({history['low_day']}) | median "
             f"daily low ${history['median_daily_low']:,.0f}")
    recent = "  Recent daily lows: " + " | ".join(
        f"{day[5:]} ${p:,.0f}" for day, p in history["recent"])
    if history["days"] < MIN_HISTORY_DAYS:
        verdict = (f"  This recheck ${price:,.0f}: too little history "
                   f"({history['days']} day(s)) to call it a low.")
    elif price < history["low"]:
        verdict = (f"  This recheck ${price:,.0f}: below every stored fare "
                   f"in this window.")
    elif price == history["low"]:
        verdict = (f"  This recheck ${price:,.0f}: ties the stored low.")
    else:
        verdict = (f"  This recheck ${price:,.0f}: "
                   f"${price - history['low']:,.0f} above the stored low.")
    return [head, stats, recent, verdict]


CHART_BASE = "https://quickchart.io/chart"


def history_chart_url(history, price, cap=None) -> str | None:
    """A chart link built ONLY from stored daily lows: the data is encoded
    in the URL (quickchart.io renders it when the link is opened; nothing is
    sent anywhere until then). Line = daily low, dashed = cap, final point
    = this recheck."""
    if not history or not history.get("daily_lows"):
        return None
    labels = [day[5:] for day, _ in history["daily_lows"]] + ["recheck"]
    lows = [round(p) for _, p in history["daily_lows"]]
    datasets = [
        {"label": "daily low (stored)", "data": lows + [None],
         "fill": False},
        {"label": "this recheck", "data": [None] * len(lows) + [round(price)],
         "pointRadius": 6, "showLine": False},
    ]
    if cap is not None:
        datasets.append({"label": f"cap ${cap:,.0f}",
                         "data": [round(cap)] * len(labels), "fill": False,
                         "borderDash": [6, 4], "pointRadius": 0})
    route = ""
    if history.get("origin") and history.get("destination"):
        route = f"{history['origin']}-{history['destination']} "
    config = {
        "type": "line",
        "data": {"labels": labels, "datasets": datasets},
        "options": {"title": {"display": True, "text": (
            f"{route}{history['depart_from']}..{history['depart_to']}, "
            f"{history['duration']}-night, USD")}},
    }
    return (CHART_BASE + "?c="
            + quote(json.dumps(config, separators=(",", ":")), safe=""))


def render_alert_message(watch, confirmation, fingerprint, first_seen_at,
                         history=None) -> str:
    """Plain-text handoff.

    Includes: route, dates, airline/stops, price USD, booking URL,
    "rechecked at <ts>", first-seen ts. Never uses the word "bookable" and
    never paraphrases safety claims beyond the data (no bookability claim).
    """
    # Actual airports from the rechecked itinerary; the watch's airport
    # list is only a last resort (it would name e.g. SFO for an OAK fare).
    origin = confirmation.get("origin") or _watch_leg(watch, "origins", "origin")
    destination = (confirmation.get("destination")
                   or _watch_leg(watch, "destinations", "destination"))
    ret_from = confirmation.get("return_origin")
    ret_to = confirmation.get("return_destination")
    route = f"{origin} -> {destination}"
    if (ret_from and ret_to) and (ret_from, ret_to) != (destination, origin):
        route += f", returning {ret_from} -> {ret_to}"
    watch_id = watch.get("watch_id", watch.get("id", "watch"))
    lines = [
        f"Flight deal -- {watch_id}: ${confirmation['price_usd']:,.0f} round trip (USD)",
        f"{route}, {confirmation['depart_date']} -> "
        f"{confirmation['return_date']}",
        f"{confirmation.get('airline') or 'Unknown airline'} - "
        f"{_stops_text(confirmation.get('stops'))}",
        f"First seen {first_seen_at}; rechecked at {confirmation['observed_at']}.",
    ]
    lines.extend(_history_lines(history, confirmation["price_usd"]))
    chart = history_chart_url(history, confirmation["price_usd"],
                              watch.get("absolute_cap_usd"))
    if chart:
        lines.append(f"Chart: {chart}")
    if confirmation.get("booking_url"):
        lines.append(f"Book: {confirmation['booking_url']}")
    lines.append(f"Reference: {fingerprint}")
    text = "\n".join(lines)
    assert "bookable" not in text.lower(), \
        "alert text must never claim bookability"
    return text


def enqueue_from_confirmation(db, watch, candidate, confirmation) -> tuple[str, str]:
    """Build alert_id, render message, store.enqueue_alert(...).

    v3: alert identity is (watch_id, departure week) -- one alert slot per
    watch per week; the fingerprint is still stored on the row for the
    verifier's recheck matching. Returns (alert_id, action) with action in
    {"inserted", "deduped", "rearmed"}.
    """
    watch_id = watch.get("watch_id", watch.get("id"))
    alert_id = make_alert_id(watch_id, store.depart_week(candidate["depart_date"]))
    message = render_alert_message(
        watch, confirmation, candidate["fingerprint"],
        store.first_seen(db, candidate["fingerprint"]),
        history=store.price_history(
            db, watch_id, confirmation["depart_date"],
            confirmation["return_date"],
            origin=confirmation.get("origin") or candidate.get("origin"),
            destination=(confirmation.get("destination")
                         or candidate.get("destination"))))
    return store.enqueue_alert(
        db,
        alert_id=alert_id,
        watch_id=watch_id,
        fingerprint=candidate["fingerprint"],
        price_usd=confirmation["price_usd"],
        depart_date=confirmation["depart_date"],
        return_date=confirmation["return_date"],
        airline=confirmation["airline"],
        stops=confirmation["stops"],
        booking_url=confirmation["booking_url"],
        message_text=message,
        origin=confirmation.get("origin") or candidate.get("origin"),
        destination=(confirmation.get("destination")
                     or candidate.get("destination")),
    )


def drain_pending(db, slot) -> list[dict]:
    """Prepare all durable pending rows before collection (handoff)."""
    return store.prepare_handoff(db, slot, None)


def recheck_retries(db, watches, guard, slot, deadline_fn=None) -> dict:
    """Recheck every eligible retry candidate before a retry handoff.

    - confirmed: re-render the message from the FRESH confirmation
      (spec item 8) and collect the alert_id for re-prepare;
    - unknown: bump the persisted unknown counter; on the third
      consecutive unknown the row is retired (v3);
    - gone: retire the row (fare disappeared on recheck).

    deadline_fn (optional): checked at the top of the candidate loop;
    raises store.DeadlineExceeded for a controlled run stop.

    Returns {"retry_ids": [...], "retired": [...], "unknown": [...]}.
    """
    if isinstance(watches, dict):
        watch_map = watches
    else:
        watch_map = {w.get("watch_id", w.get("id")): w for w in watches}
    retry_ids: list[str] = []
    retired: list[str] = []
    unknown: list[str] = []
    for row in store.eligible_retry_candidates(db, slot):
        if deadline_fn is not None and deadline_fn():
            raise store.DeadlineExceeded("retry recheck deadline exceeded")
        watch = watch_map.get(row["watch_id"])
        if watch is None:
            unknown.append(row["alert_id"])
            continue
        status, confirmation = verifier.recheck_alert_detailed(row, db, watch, guard)
        if status == "confirmed":
            message = render_alert_message(
                watch, confirmation, row["fingerprint"],
                store.first_seen(db, row["fingerprint"]),
                history=store.price_history(
                    db, row["watch_id"], confirmation["depart_date"],
                    confirmation["return_date"],
                    origin=confirmation.get("origin") or row.get("origin"),
                    destination=(confirmation.get("destination")
                                 or row.get("destination"))))
            store.update_outbox_message(db, row["alert_id"], message,
                                        confirmation["price_usd"])
            retry_ids.append(row["alert_id"])
        elif status == "gone":
            store.retire_outbox(db, row["alert_id"], "fare disappeared on recheck")
            retired.append(row["alert_id"])
        else:
            n = store.note_unknown_recheck(db, row["alert_id"])
            if n >= store.UNKNOWN_MAX:
                store.retire_outbox(
                    db, row["alert_id"],
                    f"unknown on {n} consecutive rechecks")
                retired.append(row["alert_id"])
            else:
                unknown.append(row["alert_id"])
    return {"retry_ids": retry_ids, "retired": retired, "unknown": unknown}
