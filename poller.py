#!/usr/bin/env python3
"""Round-trip flight poller (v2: real HTTP-attempt accounting).

Every real HTTP send goes through the transport guard installed on
fli's session layer (see transport.py). The budget therefore counts
actual network attempts -- retries, pagination, and blank-page refetches
included -- not ``.search()`` calls (spec item 1).

Two entry points:
- broad_scan(watch, duration_days) -> list of combos | None
- narrow_query(origin, dest, depart, ret, watch, ...) -> list of itinerary rows

Rows are dicts with keys: origin, destination, depart_date, return_date,
airline, stops, price_usd, currency, booking_url, cabin. ``price_usd`` is
the total round-trip price in USD: fli's SearchFlights returns the full
round-trip total per itinerary (see audit Q12).
"""
from __future__ import annotations

import random
import time
from datetime import date, timedelta

from fli.core import (
    build_date_search_segments,
    build_flight_segments,
    build_time_restrictions,
    parse_cabin_class,
    parse_max_stops,
    resolve_airports,
)
from fli.models import (
    DateSearchFilters,
    FlightSearchFilters,
    PassengerInfo,
    TripType,
)
from fli.search import SearchClientError, SearchDates, SearchFlights

import transport
from transport import ThrottleDetected

CURRENCY = "USD"
MAX_DATES_PER_SWEEP = 93  # library cap: one page fetch per date
_JITTER = (1.0, 3.0)  # seconds between requests


def _polite() -> None:
    time.sleep(random.uniform(*_JITTER))


def _base(cfg: dict):
    # Comma-separated airport lists are supported by resolve_airports, so one
    # query can cover several airports (grouped metro queries).
    origins = resolve_airports(",".join(cfg["origins"]))
    dests = resolve_airports(",".join(cfg["destinations"]))
    stops = parse_max_stops({0: "ANY", 1: "ONE_STOP"}.get(cfg.get("max_stops", 1), "ONE_STOP"))
    seat = parse_cabin_class(cfg.get("cabin", "economy"))
    pax = PassengerInfo(adults=1)
    return origins, dests, stops, seat, pax


def _apt(a) -> str | None:
    """Airport is an Enum; the IATA code is the member name."""
    return a.name.lstrip("_") if a is not None else None


def _flatten(result, client: SearchFlights, seat, pax, cabin: str,
             depart: str, ret: str) -> dict | None:
    """Flatten a FlightResult or RT tuple into a plain itinerary dict."""
    try:
        booking_url = client.build_flight_booking_url(
            result, currency=CURRENCY, seat_type=seat, passenger_info=pax)
    except Exception:
        booking_url = None

    back = None
    if isinstance(result, tuple):
        outbound, back = result[0], result[1]
        price, currency = outbound.price, outbound.currency or CURRENCY
        stops = (outbound.stops or 0) + (back.stops or 0)
        airline = outbound.primary_airline_name or (
            outbound.primary_airline.name.lstrip("_") if outbound.primary_airline else None)
        raw_legs = outbound.legs + back.legs
    else:
        price, currency = result.price, result.currency or CURRENCY
        stops = result.stops or 0
        airline = result.primary_airline_name or (
            result.primary_airline.name.lstrip("_") if result.primary_airline else None)
        raw_legs = result.legs
        outbound = result
    if price is None:
        return None
    # Only USD fares can be compared with USD caps.
    if (currency or CURRENCY).upper() != CURRENCY:
        return None
    # Spec: self-transfer itineraries are excluded from normal alerts.
    if getattr(outbound, "self_transfer", None) or (
            back is not None and getattr(back, "self_transfer", None)):
        return None
    # Endpoints of each direction: first departure and FINAL arrival (the
    # first leg's arrival is only the layover on a connecting flight).
    out_legs = list(outbound.legs)
    back_legs = list(back.legs) if back is not None else []
    origin = _apt(out_legs[0].departure_airport) if out_legs else None
    destination = _apt(out_legs[-1].arrival_airport) if out_legs else None
    return_origin = (_apt(back_legs[0].departure_airport)
                     if back_legs else None)
    return_destination = (_apt(back_legs[-1].arrival_airport)
                          if back_legs else None)

    # v3: per-leg flight numbers and departure datetimes feed the richer
    # fingerprint (store.make_fingerprint) and are persisted on the
    # observation row. Legs that lack them are skipped; absent fields keep
    # the Phase-1 fingerprint for old callers.
    flight_numbers: list[str] = []
    depart_times: list[str] = []
    for leg in raw_legs:
        fn = getattr(leg, "flight_number", None)
        if fn:
            flight_numbers.append(str(fn))
        dt = getattr(leg, "departure_datetime", None)
        if dt is not None:
            depart_times.append(dt.isoformat() if hasattr(dt, "isoformat")
                                else str(dt))

    return {"origin": origin, "destination": destination,
            "return_origin": return_origin,
            "return_destination": return_destination, "airline": airline,
            "stops": stops, "price_usd": float(price), "currency": currency,
            "booking_url": booking_url or "",
            "depart_date": depart, "return_date": ret, "cabin": cabin,
            "flight_numbers": flight_numbers, "depart_times": depart_times}


# Why the last narrow_query came back as it did (redesign A3 counters):
# "ok", "empty" (Google answered, nothing usable), "transport_fail" (every
# attempt raised) or "unparseable" (results, but none flattened). Read by
# sentinel.check_cell right after the call; concurrency is 1.
LAST_OUTCOME: str | None = None


def narrow_query(origin: str, dest: str, depart: str, ret: str, watch: dict, *,
                 attempt_category: str = "sentinel", top_n: int = 3,
                 require_guard: bool = True) -> list[dict]:
    """One specific-date round trip. Returns itinerary rows (empty list =
    unknown, never "no deals"). Raises ThrottleDetected on 429/CAPTCHA
    (BaseException: callers must catch it explicitly).

    attempt_category is one of: manual, sentinel, verify, burst, background,
    retry_reserve.
    """
    if require_guard:
        transport.require_guard()
    _origins, _dests, stops, seat, pax = _base(watch)
    cabin = watch.get("cabin", "economy")
    o = resolve_airports(origin)
    d = resolve_airports(dest)
    segments, _ = build_flight_segments(
        origin=o, destination=d, departure_date=depart, return_date=ret)
    # "outbound_hours" (e.g. "5-12" for a Saturday-morning departure)
    # restricts only the OUTBOUND flight; fli's builder would apply it to
    # both legs, so it is set on the first segment here.
    if watch.get("outbound_hours"):
        segments[0].time_restrictions = build_time_restrictions(
            departure_window=watch["outbound_hours"])
    filters = FlightSearchFilters(
        trip_type=TripType.ROUND_TRIP, passenger_info=pax,
        flight_segments=segments, stops=stops, seat_type=seat)

    last_err: Exception | None = None
    results = None
    with transport.category_scope(attempt_category):
        for attempt in range(2):
            try:
                # Only the network call is retried; flatten bugs must crash
                # loudly, never masquerade as "unknown". ThrottleDetected is
                # a BaseException, so it is NOT swallowed here.
                client = SearchFlights()
                results = client.search(filters, top_n=top_n, currency=CURRENCY)
            except (SearchClientError, Exception) as e:  # noqa: BLE001 - network lib
                last_err = e
                time.sleep(2 ** attempt * 2)
                continue
            last_err = None  # this attempt answered: an empty answer is "empty"
            break
    global LAST_OUTCOME
    _polite()
    if not results:
        LAST_OUTCOME = "transport_fail" if last_err is not None else "empty"
        return []  # unknown, not "no deals"
    out = []
    for r in results:
        flat = _flatten(r, client, seat, pax, cabin, depart, ret)
        if flat:
            out.append(flat)
    out.sort(key=lambda x: x["price_usd"])
    LAST_OUTCOME = "ok" if out else "unparseable"
    return out


def _window(cfg: dict) -> tuple[date, date]:
    lo_mo, hi_mo = cfg.get("window_months", [3, 9])
    today = date.today()
    return today + timedelta(days=lo_mo * 30), today + timedelta(days=hi_mo * 30)


def _chunks(start: date, end: date, size: int = MAX_DATES_PER_SWEEP):
    cur = start
    while cur <= end:
        chunk_end = min(cur + timedelta(days=size - 1), end)
        yield cur, chunk_end
        cur = chunk_end + timedelta(days=1)


def broad_scan(watch: dict, duration_days: int = 7) -> list[dict] | None:
    """Flex-date round-trip sweep: cheapest price per departure date.

    Returns [{depart_date, return_date, price_usd}] sorted by price, or None
    when the sweep failed / returned nothing usable (unknown, not "no deals").
    """
    transport.require_guard()
    origins, dests, stops, seat, pax = _base(watch)
    start, end = _window(watch)
    out: list[dict] = []
    seen_ok = False

    for cs, ce in _chunks(start, end):
        segments, _ = build_date_search_segments(
            origin=origins, destination=dests,
            start_date=cs.isoformat(), trip_duration=duration_days, is_round_trip=True,
        )
        filters = DateSearchFilters(
            trip_type=TripType.ROUND_TRIP, passenger_info=pax,
            flight_segments=segments, stops=stops, seat_type=seat,
            from_date=cs.isoformat(), to_date=ce.isoformat(), duration=duration_days,
        )
        with transport.category_scope("background"):
            try:
                results = SearchDates().search(filters, currency=CURRENCY)
            except ThrottleDetected:
                raise
            except (SearchClientError, Exception):  # noqa: BLE001 - network lib
                results = None
        _polite()
        if not results:
            continue
        seen_ok = True
        for dp in results:
            if dp.price is None:
                continue
            d0 = dp.date[0].date().isoformat() if dp.date else None
            d1 = dp.date[1].date().isoformat() if dp.date and len(dp.date) > 1 else None
            if not d0:
                continue
            out.append({"depart_date": d0, "return_date": d1,
                        "price_usd": float(dp.price)})

    if not seen_ok or not out:
        return None  # unknown, not "no deals"
    out.sort(key=lambda r: r["price_usd"])
    return out
