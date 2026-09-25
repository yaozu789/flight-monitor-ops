"""poller._flatten on real fli result objects (no network).

Covers the audit findings: a connecting flight's destination is its FINAL
arrival (not the layover), self-transfer and non-USD fares are dropped, and
alert text names the actual airports of the rechecked itinerary.
"""
import sys
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from fli.models import Airline, Airport  # noqa: E402
from fli.models.google_flights.base import FlightLeg, FlightResult  # noqa: E402

import outbox  # noqa: E402
import poller  # noqa: E402
import store  # noqa: E402


def leg(frm, to, num, day, hour):
    dep = datetime(2026, 12, day, hour)
    return FlightLeg(airline=Airline.UA, flight_number=num,
                     departure_airport=getattr(Airport, frm),
                     arrival_airport=getattr(Airport, to),
                     departure_datetime=dep,
                     arrival_datetime=dep.replace(hour=hour + 2),
                     duration=120)


def result(legs, price=450.0, currency="USD", self_transfer=None):
    return FlightResult(legs=legs, price=price, currency=currency,
                        duration=600, stops=len(legs) - 1,
                        self_transfer=self_transfer)


class _NoBookingClient:
    def build_flight_booking_url(self, *a, **k):
        return "https://example.invalid/book"


def flatten(rt):
    return poller._flatten(rt, _NoBookingClient(), None, None, "economy",
                           "2026-12-04", "2026-12-11")


class FlattenTests(unittest.TestCase):
    def test_connecting_flight_uses_final_arrival(self):
        out = result([leg("SFO", "LAX", "UA1", 4, 8),
                      leg("LAX", "NRT", "UA2", 4, 12)])
        back = result([leg("HND", "SFO", "UA3", 11, 9)])
        row = flatten((out, back))
        self.assertEqual((row["origin"], row["destination"]), ("SFO", "NRT"))
        self.assertEqual((row["return_origin"], row["return_destination"]),
                         ("HND", "SFO"))

    def test_self_transfer_is_dropped(self):
        for flagged in ("out", "back"):
            with self.subTest(flagged=flagged):
                out = result([leg("SFO", "NRT", "UA1", 4, 8)],
                             self_transfer=(flagged == "out"))
                back = result([leg("NRT", "SFO", "UA2", 11, 9)],
                              self_transfer=(flagged == "back"))
                self.assertIsNone(flatten((out, back)))

    def test_non_usd_fare_is_dropped(self):
        out = result([leg("SFO", "NRT", "UA1", 4, 8)], currency="EUR")
        back = result([leg("NRT", "SFO", "UA2", 11, 9)], currency="EUR")
        self.assertIsNone(flatten((out, back)))

    def test_alert_names_actual_airports(self):
        conf = {"price_usd": 450.0, "depart_date": "2026-12-04",
                "return_date": "2026-12-11", "airline": "United", "stops": 1,
                "booking_url": "", "observed_at": "2026-09-25T20:00:00+00:00",
                "origin": "OAK", "destination": "NRT",
                "return_origin": "HND", "return_destination": "OAK"}
        watch = {"id": "japan", "origins": ["SFO", "OAK", "SJC"],
                 "destinations": ["NRT", "HND", "KIX"]}
        text = outbox.render_alert_message(watch, conf, "fp", "then")
        self.assertIn("OAK -> NRT, returning HND -> OAK", text)
        self.assertNotIn("SFO", text)


class VerifierQueryTests(unittest.TestCase):
    def test_recheck_uses_grouped_airports(self):
        """verify() and recheck_alert_detailed() query the watch's airport
        groups, so an open-jaw itinerary (in NRT, back from HND) can match."""
        from unittest import mock
        import verifier
        calls = []

        def fake_query(o, d, dep, ret, cfg, **kw):
            calls.append((o, d))
            return []

        watch = {"id": "japan", "absolute_cap_usd": 500,
                 "origins": ["SFO", "OAK", "SJC"],
                 "destinations": ["NRT", "HND", "KIX"]}
        cand = {"origin": "SFO", "destination": "NRT", "fingerprint": "fp",
                "depart_date": "2026-12-04", "return_date": "2026-12-11"}
        with mock.patch.object(verifier.poller, "narrow_query", fake_query):
            verifier.verify(None, watch, cand, None)
            verifier.recheck_alert_detailed(cand, None, watch, None)
        self.assertEqual(calls, [("SFO,OAK,SJC", "NRT,HND,KIX")] * 2)


class RearmTests(unittest.TestCase):
    def test_rearm_replaces_whole_itinerary(self):
        """A cheaper, DIFFERENT flight in the same departure week replaces
        every itinerary field, so later rechecks verify the alerted flight."""
        import os
        import tempfile
        fd, path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        db = store.init_db(path)
        self.addCleanup(lambda: (db.close(), os.unlink(path)))
        common = dict(alert_id="japan:w49", watch_id="japan",
                      booking_url="", message_text="m")
        store.enqueue_alert(db, fingerprint="fp-old", price_usd=480.0,
                            depart_date="2026-12-01", return_date="2026-12-08",
                            airline="UA", stops=1, origin="SFO",
                            destination="NRT", **common)
        _, action = store.enqueue_alert(
            db, fingerprint="fp-new", price_usd=430.0,
            depart_date="2026-12-03", return_date="2026-12-10", airline="JL",
            stops=0, origin="OAK", destination="HND", **common)
        self.assertEqual(action, "rearmed")
        row = store.outbox_get(db, "japan:w49")
        self.assertEqual(
            (row["fingerprint"], row["origin"], row["destination"],
             row["depart_date"], row["return_date"], row["airline"],
             row["stops"], row["price_usd"]),
            ("fp-new", "OAK", "HND", "2026-12-03", "2026-12-10", "JL", 0,
             430.0))


if __name__ == "__main__":
    unittest.main()
