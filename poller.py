"""Flight searches via the `flights` PyPI package (punitarani/fli).

Two primitives:
  broad_scan(watch_cfg)  -- flex-date sweep over the watch window; finds cheap date combos.
  narrow_query(...)      -- specific-date round trip with full itinerary detail.

Contract: empty / failed searches return None ("unknown"), NEVER zero or an
empty deal list. The caller must treat None as "could not determine", not
"no deals". Requests are sequential with jitter; one retry with backoff.
"""
from __future__ import annotations

import random
import time
from datetime import date, timedelta

from fli.core import (
    build_date_search_segments,
    build_flight_segments,
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

CURRENCY = "USD"
MAX_DATES_PER_SWEEP = 93  # library cap: one page fetch per date
_JITTER = (1.0, 3.0)      # seconds between requests


def _polite() -> None:
    time.sleep(random.uniform(*_JITTER))


def _base(cfg: dict):
    origins = resolve_airports(",".join(cfg["origins"]))
    dests = resolve_airports(",".join(cfg["destinations"]))
    stops = parse_max_stops({0: "ANY", 1: "ONE_STOP"}.get(cfg.get("max_stops", 1), "ONE_STOP"))
    seat = parse_cabin_class(cfg.get("cabin", "economy"))
    pax = PassengerInfo(adults=1)
    return origins, dests, stops, seat, pax


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


def broad_scan(cfg: dict, duration_days: int = 7) -> list[dict] | None:
    """Flex-date round-trip sweep: cheapest price per departure date.

    Returns [{depart_date, return_date, price, currency}] sorted by price,
    or None when the sweep failed / returned nothing usable (unknown).
    """
    origins, dests, stops, seat, pax = _base(cfg)
    start, end = _window(cfg)
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
        try:
            results = SearchDates().search(filters, currency=CURRENCY)
        except (SearchClientError, Exception):
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
                        "price": float(dp.price), "currency": dp.currency or CURRENCY})

    if not seen_ok or not out:
        return None  # unknown, not "no deals"
    out.sort(key=lambda r: r["price"])
    return out


def _apt(a) -> str | None:
    """Airport is an Enum; the IATA code is the member name."""
    return a.name.lstrip("_") if a is not None else None


def _flatten(result, client: SearchFlights, seat, pax) -> dict | None:
    """Flatten a FlightResult or RT tuple into a plain itinerary dict."""
    try:
        booking_url = client.build_flight_booking_url(
            result, currency=CURRENCY, seat_type=seat, passenger_info=pax)
    except Exception:
        booking_url = None

    if isinstance(result, tuple):
        outbound, ret = result[0], result[1]
        price, currency = outbound.price, outbound.currency or CURRENCY
        stops = (outbound.stops or 0) + (ret.stops or 0)
        airline = outbound.primary_airline_name or (
            outbound.primary_airline.name.lstrip("_") if outbound.primary_airline else None)
        legs = [{"from": _apt(l.departure_airport), "to": _apt(l.arrival_airport)}
                for l in (outbound.legs + ret.legs)]
    else:
        price, currency = result.price, result.currency or CURRENCY
        stops = result.stops or 0
        airline = result.primary_airline_name or (
            result.primary_airline.name.lstrip("_") if result.primary_airline else None)
        legs = [{"from": _apt(l.departure_airport), "to": _apt(l.arrival_airport)}
                for l in result.legs]
    if price is None:
        return None
    origin = legs[0]["from"] if legs else None
    destination = legs[0]["to"] if legs else None
    return {"origin": origin, "destination": destination, "airline": airline,
            "stops": stops, "price": float(price), "currency": currency,
            "booking_url": booking_url}


def narrow_query(origin: str, dest: str, depart: str, return_: str,
                 cfg: dict, top_n: int = 3) -> list[dict] | None:
    """Specific-date round trip. Returns itineraries sorted by price, or None (unknown)."""
    origins, dests, stops, seat, pax = _base(cfg)
    o = resolve_airports(origin)
    d = resolve_airports(dest)
    segments, _ = build_flight_segments(
        origin=o, destination=d, departure_date=depart, return_date=return_)
    filters = FlightSearchFilters(
        trip_type=TripType.ROUND_TRIP, passenger_info=pax,
        flight_segments=segments, stops=stops, seat_type=seat)

    last_err: Exception | None = None
    results = None
    for attempt in range(2):
        try:
            # Only the network call is retried; flatten bugs must crash loudly,
            # never masquerade as "unknown".
            client = SearchFlights()
            results = client.search(filters, top_n=top_n, currency=CURRENCY)
        except (SearchClientError, Exception) as e:  # noqa: BLE001 - network lib, be liberal
            last_err = e
            time.sleep(2 ** attempt * 2)
            continue
        break
    _polite()
    if not results:
        return None  # unknown, not "no deals"
    out = []
    for r in results:
        flat = _flatten(r, client, seat, pax)
        if flat:
            flat.update({"depart_date": depart, "return_date": return_,
                         "cabin": cfg.get("cabin", "economy")})
            out.append(flat)
    out.sort(key=lambda x: x["price"])
    return out or None
