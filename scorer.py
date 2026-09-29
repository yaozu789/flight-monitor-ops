"""Scorer stage: candidates at or below the watch's alert threshold.

Reads this run's observations via store.latest_observations_since and emits
one candidate per itinerary priced at or below the threshold from
``baseline.threshold``: the absolute cap in ``cap`` mode, or 70% of the
route's median in ``relative`` mode (only once that route's history is
armed). ``collect`` mode has no threshold, so nothing is a candidate. There
is still no "new low" rule: a drop that stays above the threshold is not an
alertable event.
"""
from __future__ import annotations

import baseline
import store


def score_new_observations(db, watch, run_start, cooldown_hours=24) -> list[dict]:
    """Threshold candidates.

    Candidate dict: {watch_id, fingerprint, origin, destination, depart_date,
    return_date, airline, stops, price_usd, booking_url, observed_at,
    reason ("absolute" | "relative"), threshold, median}. Skips
    fingerprints alerted within *cooldown_hours*. Sorted by price.
    """
    import run  # lazy: run imports this module
    watch_id = watch.get("watch_id", watch.get("id"))
    mode = watch.get("alert_mode", "cap")
    if mode == "collect":
        return []
    gcfg = run.group_cfg_for(watch) if mode == "relative" else {}
    reason = "relative" if mode == "relative" else "absolute"
    candidates = []
    for r in store.latest_observations_since(db, watch_id, run_start):
        price = r.get("price_usd")
        if price is None:
            continue
        limit, base = baseline.threshold(
            db, dict(watch, id=watch_id), gcfg, r.get("origin"),
            r.get("destination"), r.get("depart_date"), r.get("return_date"))
        if limit is None or price > limit:
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
            "window_name": r.get("window_name"),
            "reason": reason,
            "threshold": limit,
            "median": base["median"] if base else None,
        })
    candidates.sort(key=lambda c: c["price_usd"])
    return candidates
