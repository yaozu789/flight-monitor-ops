"""Alert price history comes from stored observations, with dates, scoped to
the alerted route, and only claims a low with enough distinct days."""
import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import outbox  # noqa: E402
import store  # noqa: E402


def iso_days_ago(n, hours=0):
    return (datetime.now(timezone.utc)
            - timedelta(days=n, hours=hours)).isoformat()


CONF = {"depart_date": "2026-12-04", "return_date": "2026-12-11",
        "airline": "UA", "stops": 0, "booking_url": "", "observed_at": "now",
        "origin": "SFO", "destination": "NRT"}


class PriceHistoryTests(unittest.TestCase):
    def setUp(self):
        fd, self.path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        self.db = store.init_db(self.path)
        self.addCleanup(lambda: (self.db.close(), os.unlink(self.path)))

    def obs(self, days_ago, price, depart="2026-12-04", ret="2026-12-11",
            watch="japan", origin="SFO", dest="NRT", hours=0):
        store.add_observation(
            self.db, watch_id=watch, origin=origin, destination=dest,
            depart_date=depart, return_date=ret, airline="UA", stops=0,
            cabin="economy", price_usd=price, booking_url="",
            observed_at=iso_days_ago(days_ago, hours))

    def hist(self, **kw):
        return store.price_history(self.db, "japan", "2026-12-04",
                                   "2026-12-11", origin="SFO",
                                   destination="NRT", **kw)

    def text(self, price, history):
        return outbox.render_alert_message({"id": "japan"},
                                           {**CONF, "price_usd": price},
                                           "fp", "then", history=history)

    def test_scope_filters(self):
        self.obs(5, 540.0)
        self.obs(5, 520.0)                                   # same day
        self.obs(3, 505.0, depart="2026-12-07", ret="2026-12-14")  # +3d edge
        self.obs(1, 470.0, depart="2026-12-01", ret="2026-12-08")  # -3d edge
        self.obs(1, 300.0, ret="2026-12-09")                 # 5-night: out
        self.obs(1, 250.0, depart="2026-12-08", ret="2026-12-15")  # +4d: out
        self.obs(1, 200.0, watch="nyc")                      # other watch
        self.obs(1, 210.0, origin="OAK")                     # other route
        self.obs(1, 220.0, dest="HND")                       # other route
        self.obs(15, 100.0)                                  # past lookback
        h = self.hist()
        self.assertEqual(h["checks"], 4)
        self.assertEqual(h["days"], 3)
        self.assertEqual([p for _, p in h["daily_lows"]], [520.0, 505.0, 470.0])
        self.assertEqual((h["low"], h["median_daily_low"]), (470.0, 505.0))

    def test_even_median_and_recent_truncation(self):
        for d, p in ((6, 600.0), (5, 500.0), (4, 400.0), (3, 300.0),
                     (2, 200.0), (1, 100.0)):
            self.obs(d, p)
        h = self.hist()
        self.assertEqual(h["median_daily_low"], 350.0)
        self.assertEqual(len(h["recent"]), 5)
        self.assertEqual(h["recent"][0][1], 500.0)

    def test_sparse_history_makes_no_claim(self):
        self.obs(1, 500.0)
        self.obs(1, 480.0, hours=1)
        t = self.text(450.0, self.hist())
        self.assertIn("SFO -> NRT", t)
        self.assertIn("2 fares seen on 1 day(s)", t)
        self.assertIn("too little history (1 day(s)) to call it a low", t)
        self.assertNotIn("below every stored fare", t)

    def test_verdicts_with_enough_days(self):
        for d, p in ((3, 520.0), (2, 505.0), (1, 470.0)):
            self.obs(d, p)
        h = self.hist()
        self.assertIn("below every stored fare", self.text(431.0, h))
        self.assertIn("ties the stored low", self.text(470.0, h))
        self.assertIn("$30 above the stored low", self.text(500.0, h))

    def test_no_history_says_so(self):
        self.assertIn("none yet", self.text(431.0, self.hist()))


if __name__ == "__main__":
    unittest.main()
