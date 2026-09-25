"""Verification: re-run a live narrow query for an alert candidate.

An alert is only confirmed if the fare is still bookable at (about) the
candidate price. Kills false positives from stale/cached data.
"""
from __future__ import annotations

from poller import narrow_query

PRICE_TOLERANCE = 1.02  # allow 2% drift (rounding / currency wobble)


def verify_candidate(candidate: dict, cfg: dict) -> dict | None:
    """Re-query the candidate itinerary. Returns confirmation dict or None."""
    obs = candidate["observation"]
    results = narrow_query(obs["origin"], obs["destination"],
                           obs["depart_date"], obs["return_date"], cfg)
    if not results:
        return None  # unknown -> do NOT alert
    cheapest = min(r["price"] for r in results)
    ceiling = obs["price_usd"] * PRICE_TOLERANCE
    if candidate["reason"] == "absolute":
        ceiling = max(ceiling, cfg["absolute_cap_usd"] * PRICE_TOLERANCE)
    if cheapest <= ceiling:
        return {"confirmed_price": cheapest, "currency": results[0]["currency"],
                "booking_url": results[0]["booking_url"]}
    return None
