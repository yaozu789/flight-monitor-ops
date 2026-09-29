"""Scorer stage (v2): absolute-cap candidates only.

Reads this run's observations via store.latest_observations_since and emits
one candidate per itinerary priced at or below the watch's absolute cap,
subject to the per-fingerprint alert cooldown. There is no new_low logic
anywhere in this stage or its siblings: a price drop that stays above the cap
is not an alertable event.
"""
from __future__ import annotations

import store


def score_new_observations(db, watch, run_start, cooldown_hours=24) -> list[dict]:
    """Absolute-cap candidates only.

    Candidate dict: {watch_id, fingerprint, origin, destination, depart_date,
    return_date, airline, stops, price_usd, booking_url, observed_at,
    reason="absolute"}.

    Rules: from store.latest_observations_since (this run's rows); keep
    price_usd <= watch["absolute_cap_usd"]; skip if
    store.alerted_recently(fingerprint, cooldown_hours). NO new_low logic
    anywhere. Sorted by price_usd ascending.
    """
    watch_id = watch.get("watch_id", watch.get("id"))
    cap = watch["absolute_cap_usd"]
    candidates = []
    for r in store.latest_observations_since(db, watch_id, run_start):
        price = r.get("price_usd")
        if price is None or price > cap:
            continue
        fingerprint = r["fingerprint"]
        if store.alerted_recently(db, fingerprint, cooldown_hours):
            continue
        candidates.append({
            "watch_id": watch_id,
            "fingerprint": fingerprint,
            "origin": r.get("origin"),
            "destination": r.get("destination"),
            "depart_date": r.get("depart_date"),
            "return_date": r.get("return_date"),
            "airline": r.get("airline"),
            "stops": r.get("stops"),
            "price_usd": price,
            "booking_url": r.get("booking_url"),
            "observed_at": r.get("observed_at"),
            "outbound_hours": r.get("outbound_hours"),
            "reason": "absolute",
        })
    candidates.sort(key=lambda c: c["price_usd"])
    return candidates
