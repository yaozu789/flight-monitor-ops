"""Outbox orchestration (v2): alert rendering, durable queueing, retry rechecks.

All state lives in the shared store.py contract; this module only
orchestrates: render -> enqueue -> prepare (via store) -> retry (recheck
first, re-render the message on confirmation, retire on disappearance).

Alerts are idempotent on alert_id: store.enqueue_alert applies the
meaningful-drop dedup (re-arm to pending only on a drop >= max($15, 5%)).
"""
from __future__ import annotations

import store
import verifier


def make_alert_id(watch_id: str, fingerprint: str) -> str:
    """Stable across runs for the same deal: f"{watch_id}:{fingerprint}"."""
    return f"{watch_id}:{fingerprint}"


def _stops_text(stops) -> str:
    if stops == 0:
        return "nonstop"
    if stops == 1:
        return "1 stop"
    return f"{stops} stops"


def _watch_leg(watch: dict, plural_key: str, singular_key: str) -> str:
    plural = watch.get(plural_key)
    if isinstance(plural, (list, tuple)) and plural:
        return str(plural[0])
    if isinstance(plural, str):
        return plural
    single = watch.get(singular_key)
    return str(single) if single is not None else "?"


def render_alert_message(watch, confirmation, fingerprint, first_seen_at) -> str:
    """Plain-text handoff.

    Includes: route, dates, airline/stops, price USD, booking URL,
    "rechecked at <ts>", first-seen ts. Never uses the word "bookable" and
    never paraphrases safety claims beyond the data (no bookability claim).
    """
    origin = _watch_leg(watch, "origins", "origin")
    destination = _watch_leg(watch, "destinations", "destination")
    watch_id = watch.get("watch_id", watch.get("id", "watch"))
    lines = [
        f"Flight deal -- {watch_id}: ${confirmation['price_usd']:,.0f} round trip (USD)",
        f"{origin} -> {destination}, {confirmation['depart_date']} -> "
        f"{confirmation['return_date']}",
        f"{confirmation.get('airline') or 'Unknown airline'} - "
        f"{_stops_text(confirmation.get('stops'))}",
        f"First seen {first_seen_at}; rechecked at {confirmation['observed_at']}.",
    ]
    if confirmation.get("booking_url"):
        lines.append(f"Book: {confirmation['booking_url']}")
    lines.append(f"Reference: {fingerprint}")
    text = "\n".join(lines)
    assert "bookable" not in text.lower(), \
        "alert text must never claim bookability"
    return text


def enqueue_from_confirmation(db, watch, candidate, confirmation) -> tuple[str, str]:
    """Build alert_id, render message, store.enqueue_alert(...).

    Returns (alert_id, action) with action in {"inserted","deduped","rearmed"}.
    """
    watch_id = watch.get("watch_id", watch.get("id"))
    alert_id = make_alert_id(watch_id, candidate["fingerprint"])
    message = render_alert_message(
        watch, confirmation, candidate["fingerprint"],
        store.first_seen(db, candidate["fingerprint"]))
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
        origin=candidate.get("origin"),
        destination=candidate.get("destination"),
    )


def drain_pending(db, slot) -> list[dict]:
    """Prepare all durable pending rows before collection (handoff)."""
    return store.prepare_handoff(db, slot, None)


def recheck_retries(db, watches, guard, slot) -> dict:
    """Recheck every eligible retry candidate before a retry handoff.

    - confirmed: re-render the message from the FRESH confirmation
      (spec item 8) and collect the alert_id for re-prepare;
    - unknown: leave the row untouched (no alert, no retire);
    - gone: retire the row (fare disappeared on recheck).

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
        watch = watch_map.get(row["watch_id"])
        if watch is None:
            unknown.append(row["alert_id"])
            continue
        status, confirmation = verifier.recheck_alert_detailed(row, db, watch, guard)
        if status == "confirmed":
            message = render_alert_message(
                watch, confirmation, row["fingerprint"],
                store.first_seen(db, row["fingerprint"]))
            store.update_outbox_message(db, row["alert_id"], message,
                                        confirmation["price_usd"])
            retry_ids.append(row["alert_id"])
        elif status == "gone":
            store.retire_outbox(db, row["alert_id"], "fare disappeared on recheck")
            retired.append(row["alert_id"])
        else:
            unknown.append(row["alert_id"])
    return {"retry_ids": retry_ids, "retired": retired, "unknown": unknown}
