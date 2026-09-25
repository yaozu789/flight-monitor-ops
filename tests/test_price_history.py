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
        self.obs(1, 480.0)  # same timestamp: always the same UTC day
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

    def test_chart_link_encodes_stored_data_only(self):
        """The chart URL carries exactly the stored daily lows, the cap and
        the recheck -- decodable, no other data, no network call."""
        import json
        from urllib.parse import parse_qs, urlparse
        for d, p in ((3, 520.0), (2, 505.0), (1, 470.0)):
            self.obs(d, p)
        h = self.hist()
        url = outbox.history_chart_url(h, 431.0, cap=500)
        parsed = urlparse(url)
        self.assertEqual((parsed.scheme, parsed.netloc, parsed.path),
                         ("https", "quickchart.io", "/chart"))
        cfg = json.loads(parse_qs(parsed.query)["c"][0])
        sets = {d["label"]: d["data"] for d in cfg["data"]["datasets"]}
        self.assertEqual(sets["daily low (stored)"], [520, 505, 470, None])
        self.assertEqual(sets["this recheck"], [None, None, None, 431])
        self.assertEqual(sets["cap $500"], [500] * 4)
        self.assertEqual(cfg["data"]["labels"][-1], "recheck")
        self.assertIn("SFO-NRT", cfg["options"]["title"]["text"])
        text = outbox.render_alert_message(
            {"id": "japan", "absolute_cap_usd": 500},
            {**CONF, "price_usd": 431.0}, "fp", "then", history=h)
        self.assertIn("Chart: https://quickchart.io/chart?c=", text)
        self.assertLess(len(url), 2000)

    def test_no_chart_without_history(self):
        self.assertIsNone(outbox.history_chart_url(self.hist(), 431.0, 500))
        self.assertNotIn("Chart:", self.text(431.0, self.hist()))

    def test_bad_stored_price_never_blocks_alert(self):
        """Non-finite prices are left out of the chart, and if the chart
        builder fails for any reason the alert still renders, without it."""
        from unittest import mock
        h = {"daily_lows": [("2026-09-24", float("inf")), ("2026-09-25", 500.0)],
             "origin": "SFO", "destination": "NRT", "depart_from": "a",
             "depart_to": "b", "duration": 7}
        self.assertNotIn("Infinity", outbox.history_chart_url(h, 431.0, 500))
        self.assertIsNone(outbox.history_chart_url(
            {**h, "daily_lows": [("2026-09-24", float("nan"))]}, 431.0, 500))
        for d, p in ((3, 520.0), (2, 505.0), (1, 470.0)):
            self.obs(d, p)
        with mock.patch.object(outbox, "history_chart_url",
                               side_effect=ValueError("bad data")):
            text = outbox.render_alert_message(
                {"id": "japan", "absolute_cap_usd": 500},
                {**CONF, "price_usd": 431.0}, "fp", "then",
                history=self.hist())
        self.assertIn("Flight deal", text)
        self.assertIn("Stored history", text)
        self.assertNotIn("Chart:", text)

    def test_full_window_url_length(self):
        from datetime import date, timedelta
        days = [((date(2026, 9, 1) + timedelta(days=i)).isoformat(), 400.0 + i)
                for i in range(15)]
        h = {"daily_lows": days, "origin": "SFO", "destination": "NRT",
             "depart_from": "2026-12-01", "depart_to": "2026-12-07",
             "duration": 7}
        self.assertLess(len(outbox.history_chart_url(h, 431.0, 500)), 2000)
        self.assertIsNotNone(outbox.history_chart_url(h, 431.0, None))

    def test_no_history_says_so(self):
        self.assertIn("none yet", self.text(431.0, self.hist()))


if __name__ == "__main__":
    unittest.main()
