"""Verifier stage (v2): re-check a candidate itinerary with a fresh query.

A candidate is only alertable when a live narrow query returns a
fingerprint-matching itinerary priced at or below the watch's absolute cap.
On "unknown" (no results, or no fingerprint match) the caller must NOT alert.
This module never synthesizes a "new_low" reason: that concept does not
exist in v2.

`poller.narrow_query` is imported from the poller module so unit tests can
monkeypatch it; the real implementation bills its attempt_category per HTTP
send through the installed transport guard.
"""
from __future__ import annotations

import poller
import store


def _price(row: dict) -> float | None:
    """Read the itinerary price across poller row shapes (v2 uses price_usd,
    v1 uses price)."""
    return row.get("price_usd", row.get("price"))


def _grouped_endpoints(watch: dict, origin, destination) -> tuple[str, str]:
    """Recheck with the SAME grouped airports the collection used (e.g.
    SFO,OAK,SJC -> NRT,HND,KIX), then match the exact itinerary by
    fingerprint. Querying the candidate's own endpoints would force a
    symmetric return (NRT->SFO) and miss open-jaw itineraries that return
    from another airport in the group (HND->SFO)."""
    origins = watch.get("origins") or [origin]
    destinations = watch.get("destinations") or [destination]
    return ",".join(origins), ",".join(destinations)


def _cfg_for(watch: dict, origin: str | None, destination: str | None,
             outbound_hours: str | None = None) -> dict:
    cfg = {
        "origins": [origin],
        "destinations": [destination],
        "stops": watch.get("stops", [0, 1]),
        "seat": watch.get("cabin", "economy"),
        "pax": {"adults": 1},
    }
    # A recheck repeats the search that found the fare, including its
    # outbound departure-time limit (e.g. Saturday morning, stored with the
    # observation), or fli could expand different outbound flights.
    if outbound_hours:
        cfg["outbound_hours"] = outbound_hours
    return cfg


def _match(row: dict, depart_date: str, return_date: str) -> str:
    """Fingerprint of a poller result for the candidate's exact dates.

    v3: the row's per-leg flight numbers / departure times join the hash,
    exactly as they did when the candidate's observation was saved -- so a
    recheck only matches the same flights, and old rows (fields absent)
    fall back to the Phase-1 scheme.
    """
    return store.make_fingerprint(
        row.get("origin") or "", row.get("destination") or "",
        depart_date, return_date, row.get("airline"), row.get("stops"),
        row.get("flight_numbers"), row.get("depart_times"))


def _confirmation(match: dict, depart_date: str, return_date: str) -> dict:
    return {
        "origin": match.get("origin"),
        "destination": match.get("destination"),
        "return_origin": match.get("return_origin"),
        "return_destination": match.get("return_destination"),
        "price_usd": _price(match),
        "depart_date": depart_date,
        "return_date": return_date,
        "airline": match.get("airline"),
        "stops": match.get("stops"),
        "booking_url": match.get("booking_url"),
        "observed_at": match.get("observed_at") or store.utcnow_iso(),
    }


def _cheapest_match(rows: list[dict], fingerprint: str,
                    depart_date: str, return_date: str) -> dict | None:
    matches = [r for r in rows if _match(r, depart_date, return_date) == fingerprint]
    if not matches:
        return None
    return min(matches, key=lambda r: (_price(r) is None, _price(r)))


def verify(db, watch, candidate, guard, *, top_n=3) -> tuple[bool, dict | None]:
    """Recheck the candidate's exact itinerary.

    Returns (True, confirmation) iff a fingerprint match prices at or below
    watch["absolute_cap_usd"]; (False, None) on unknown (no results / no
    match) -- the caller must NOT alert.
    """
    if watch.get("alert_mode", "cap") == "collect":
        return False, None  # collect-only watches never confirm a deal
    # The threshold the scorer used for this candidate (cap or relative);
    # the recheck must pass the same number.
    cap = candidate.get("threshold", watch.get("absolute_cap_usd"))
    depart = candidate["depart_date"]
    return_ = candidate["return_date"]
    q_from, q_to = _grouped_endpoints(watch, candidate["origin"],
                                      candidate["destination"])
    rows = poller.narrow_query(
        q_from, q_to, depart, return_,
        _cfg_for(watch, candidate["origin"], candidate["destination"],
                 candidate.get("outbound_hours")),
        top_n=top_n, attempt_category="verify")
    if not rows:
        return False, None  # unknown -- do NOT alert
    match = _cheapest_match(rows, candidate["fingerprint"], depart, return_)
    if match is None:
        return False, None  # unknown -- do NOT alert
    price = _price(match)
    if cap is None or price is None or price > cap:
        return False, None
    conf = _confirmation(match, depart, return_)
    if candidate.get("median") is not None:
        conf["typical_usd"] = candidate["median"]
    return True, conf


def recheck_alert_detailed(alert_row, db, watch, guard) -> tuple[str, dict | None]:
    """Recheck an outbox row (dict from store.outbox_get) before a retry handoff.

    Returns:
      ("confirmed", confirmation) -- fingerprint matches, price <= cap;
      ("gone", None)              -- fingerprint matches, price > cap * 1.10
                                     (deal disappeared -> retire);
      ("unknown", None)           -- no results or no match (do NOT alert,
                                     do NOT retire).
    Never synthesizes reason "new_low".
    """
    import baseline
    import run
    depart = alert_row["depart_date"]
    return_ = alert_row["return_date"]
    gcfg = (run.group_cfg_for(watch) if watch.get("alert_mode") == "relative"
            else {})
    cap, base = baseline.threshold(db, watch, gcfg, alert_row["origin"],
                                   alert_row["destination"], depart, return_)
    if cap is None:
        return "unknown", None  # no threshold today: never re-alert
    q_from, q_to = _grouped_endpoints(watch, alert_row["origin"],
                                      alert_row["destination"])
    rows = poller.narrow_query(
        q_from, q_to, depart, return_,
        _cfg_for(watch, alert_row["origin"], alert_row["destination"],
                 alert_row.get("outbound_hours")),
        top_n=3, attempt_category="retry_reserve")
    if not rows:
        return "unknown", None
    match = _cheapest_match(rows, alert_row["fingerprint"], depart, return_)
    if match is None:
        return "unknown", None
    price = _price(match)
    if price is None:
        return "unknown", None
    if price <= cap:
        conf = _confirmation(match, depart, return_)
        if base and base.get("median") is not None:
            conf["typical_usd"] = base["median"]
        return "confirmed", conf
    if price > cap * 1.10:
        return "gone", None
    return "unknown", None
