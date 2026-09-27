"""Dashboard link: stored data packed into the URL fragment (no network)."""
import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import dashboard  # noqa: E402
import store  # noqa: E402

NOW = datetime(2026, 9, 28, 16, 0, tzinfo=timezone.utc)
WATCHES = [{"id": "japan", "absolute_cap_usd": 500},
           {"id": "nyc", "absolute_cap_usd": 250}]


class DashboardTests(unittest.TestCase):
    def setUp(self):
        fd, path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        self.db = store.init_db(path)
        self.addCleanup(lambda: (self.db.close(), os.unlink(path)))

    def obs(self, when, price, watch="japan"):
        store.add_observation(
            self.db, watch_id=watch, origin="SFO", destination="NRT",
            depart_date="2027-01-12", return_date="2027-01-19",
            airline="ANA", stops=0, cabin="economy", price_usd=price,
            booking_url="", observed_at=when.isoformat(timespec="seconds"))

    def test_link_round_trips_stored_data(self):
        self.obs(NOW - timedelta(days=2), 812)
        self.obs(NOW - timedelta(days=2, hours=1), 790)
        self.obs(NOW - timedelta(days=200), 100)  # outside PRICE_DAYS
        base = int(NOW.timestamp() // 1800)
        for s, attempts, stalled in ((base - 8, 10, 0), (base - 4, 0, 1)):
            store.claim_slot(self.db, s, "check")
            store.finish_slot(self.db, s, "completed")
            store.record_run_stats(self.db, s, attempts, stalled)
        self.db.execute("INSERT INTO daily_counters VALUES (?, ?, ?)",
                        ((NOW - timedelta(days=1)).date().isoformat(),
                         "sentinel", 54))
        store.add_health(self.db, "collection_stall", False, "3 runs idle")
        store.add_health(self.db, "hc_ping", False, "ping noise")
        url = dashboard.link(self.db, WATCHES, NOW)
        self.assertTrue(url.startswith(dashboard.DASHBOARD_URL + "#v1."))
        d = dashboard.decode(url.split("#", 1)[1])
        day = (NOW - timedelta(days=2)).date().isoformat()
        self.assertEqual(d["prices"]["japan"], [[day, 790, 2]])
        self.assertEqual(d["prices"]["nyc"], [])
        self.assertEqual(d["watches"], [["japan", 500], ["nyc", 250]])
        slots, s = [], d["run0"]
        for delta, status, attempts, stalled in d["runs"]:
            s += delta
            slots.append((s, status, attempts, stalled))
        self.assertEqual(slots, [(base - 8, "completed", 10, 0),
                                 (base - 4, "completed", 0, 1)])
        self.assertEqual(
            d["spend"], {(NOW - timedelta(days=1)).date().isoformat():
                         {"sentinel": 54}})
        # health checked_at is real time: only compare names.
        names = [e[1] for e in d["events"]]
        self.assertIn("collection_stall", names)
        self.assertNotIn("hc_ping", names)

    def test_empty_db_still_encodes(self):
        d = dashboard.decode(dashboard.encode(
            dashboard.payload(self.db, WATCHES, NOW)))
        self.assertEqual(d["runs"], [])


if __name__ == "__main__":
    unittest.main()
