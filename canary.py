"""Canary: fixed reference query proving the data layer is alive.

If this fails, the monitor is blind — the scheduler must report
"monitor needs attention" instead of silently missing deals.
"""
from __future__ import annotations

import sqlite3
import traceback
from datetime import date, timedelta

from poller import narrow_query
from store import add_health, utcnow_iso

# Fixed reference: SFO -> NRT, 6 weeks out, 7-day trip. Chosen because it is a
# heavily served route that should essentially always return results.
REFERENCE = {"origin": "SFO", "dest": "NRT", "depart_offset": 42, "return_offset": 49}


def run_canary(db: sqlite3.Connection, cfg: dict) -> tuple[bool, str]:
    depart = (date.today() + timedelta(days=REFERENCE["depart_offset"])).isoformat()
    ret = (date.today() + timedelta(days=REFERENCE["return_offset"])).isoformat()
    try:
        results = narrow_query(REFERENCE["origin"], REFERENCE["dest"], depart, ret, cfg)
    except Exception:  # noqa: BLE001 - canary must never crash the caller
        detail = f"exception: {traceback.format_exc(limit=3).strip().splitlines()[-1]}"
        add_health(db, "canary", False, detail)
        return False, detail
    if not results:
        detail = f"empty/failed query {REFERENCE['origin']}->{REFERENCE['dest']} {depart}/{ret}"
        add_health(db, "canary", False, detail)
        return False, detail
    cheapest = min(r["price"] for r in results)
    detail = (f"{len(results)} itineraries {REFERENCE['origin']}->{REFERENCE['dest']} "
              f"{depart}/{ret}, cheapest ${cheapest:.0f}")
    add_health(db, "canary", True, detail)
    return True, detail


if __name__ == "__main__":
    import sys
    sys.path.insert(0, __file__.rsplit("/", 1)[0])
    from store import get_db
    db = get_db("data/monitor.db")
    ok, detail = run_canary(db, {"origins": ["SFO"], "destinations": ["NRT"],
                                 "cabin": "economy", "max_stops": 1})
    print(detail)
    sys.exit(0 if ok else 2)
