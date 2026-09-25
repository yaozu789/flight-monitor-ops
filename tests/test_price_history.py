"""Alert price history comes from stored observations, with dates."""
import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import outbox  # noqa: E402
import store  # noqa: E402


def iso_days_ago(n):
    return (datetime.now(timezone.utc) - timedelta(days=n)).isoformat()


class PriceHistoryTests(unittest.TestCase):
    def setUp(self):
        fd, self.path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        self.db = store.init_db(self.path)
        self.addCleanup(lambda: (self.db.close(), os.unlink(self.path)))

    def obs(self, days_ago, price, depart="2026-12-04", ret="2026-12-11",
            watch="japan"):
        store.add_observation(
            self.db, watch_id=watch, origin="SFO", destination="NRT",
            depart_date=depart, return_date=ret, airline="UA", stops=0,
            cabin="economy", price_usd=price, booking_url="",
            observed_at=iso_days_ago(days_ago))

    def test_history_stats_are_from_stored_rows(self):
        self.obs(5, 540.0)
        self.obs(5, 520.0)                      # two checks, one day
        self.obs(3, 505.0, depart="2026-12-06", ret="2026-12-13")  # +2d, same 7n
        self.obs(1, 470.0)
        self.obs(1, 300.0, ret="2026-12-09")    # 5-night trip: excluded
        self.obs(1, 250.0, depart="2026-12-20", ret="2026-12-27")  # +16d: excluded
        self.obs(1, 200.0, watch="nyc")         # other watch: excluded
        self.obs(30, 100.0)                     # outside 14-day lookback
        h = store.price_history(self.db, "japan", "2026-12-04", "2026-12-11")
        self.assertEqual(h["checks"], 4)
        self.assertEqual(h["duration"], 7)
        self.assertEqual([p for _, p in h["daily_lows"]], [520.0, 505.0, 470.0])
        self.assertEqual(h["low"], 470.0)
        self.assertEqual(h["median_daily_low"], 505.0)

    def test_alert_text_shows_dated_history(self):
        self.obs(2, 512.0)
        self.obs(1, 431.0)
        h = store.price_history(self.db, "japan", "2026-12-04", "2026-12-11")
        conf = {"price_usd": 431.0, "depart_date": "2026-12-04",
                "return_date": "2026-12-11", "airline": "UA", "stops": 0,
                "booking_url": "", "observed_at": "now",
                "origin": "SFO", "destination": "NRT"}
        text = outbox.render_alert_message({"id": "japan"}, conf, "fp", "then",
                                           history=h)
        self.assertIn("2 checks since", text)
        self.assertIn("14-day low $431", text)
        self.assertIn("this fare $431 = lowest seen", text)
        self.assertIn("Recent daily lows:", text)
        self.assertIn("$512", text)

    def test_no_history_says_so(self):
        h = store.price_history(self.db, "japan", "2026-12-04", "2026-12-11")
        conf = {"price_usd": 431.0, "depart_date": "2026-12-04",
                "return_date": "2026-12-11", "airline": "UA", "stops": 0,
                "booking_url": "", "observed_at": "now"}
        text = outbox.render_alert_message({"id": "japan"}, conf, "fp", "then",
                                           history=h)
        self.assertIn("History: no earlier checks for these dates yet.", text)


if __name__ == "__main__":
    unittest.main()
