#!/usr/bin/env python3
"""Cheap Flight Monitor — CLI entrypoint (Phase 1).

Usage:
  python run.py broad  --watch japan [--duration 7]
  python run.py narrow --watch japan --depart 2026-12-04 --return 2026-12-11
  python run.py canary
  python run.py check [--watch japan]

Exit codes: 0 = success (even with zero deals); 1 = infrastructure failure;
2 = canary failed (data layer blind).
Every command prints a JSON summary to stdout for the scheduler to consume.
"""
from __future__ import annotations

import argparse
import json
import sys
import traceback
from datetime import date, timedelta
from pathlib import Path

BASE = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE))

import yaml  # noqa: E402

from canary import run_canary  # noqa: E402
from poller import broad_scan, narrow_query  # noqa: E402
from scorer import score_new_observations  # noqa: E402
from store import (  # noqa: E402
    add_alert,
    add_broad_scan,
    add_observation,
    init_db,
    sync_watches,
    utcnow_iso,
)
from verifier import verify_candidate  # noqa: E402

DB_PATH = BASE / "data" / "monitor.db"

# check: sample departure offsets (days out) and trip length for the 2-hourly poll
CHECK_DEPART_OFFSETS = [45, 75, 105, 135]
CHECK_TRIP_DAYS = 7


def load_config() -> tuple[dict, list[dict]]:
    with open(BASE / "watches.yaml") as f:
        cfg = yaml.safe_load(f)
    defaults = cfg.get("defaults", {})
    watches = []
    for w in cfg.get("watches", []):
        merged = {**defaults, **w}
        watches.append(merged)
    return defaults, watches


def get_watch(watches: list[dict], watch_id: str) -> dict:
    for w in watches:
        if w["id"] == watch_id:
            return w
    raise SystemExit(f"unknown watch: {watch_id} (have: {[w['id'] for w in watches]})")


def cmd_broad(args, db, watches):
    w = get_watch(watches, args.watch)
    combos = broad_scan(w, duration_days=args.duration)
    summary = {"watch": w["id"], "duration_days": args.duration,
               "combos": combos if combos else None,
               "unknown": combos is None}
    if combos:
        now = utcnow_iso()
        for c in combos:
            add_broad_scan(db, watch_id=w["id"], depart_date=c["depart_date"],
                           return_date=c["return_date"], trip_days=args.duration,
                           price_usd=c["price"], observed_at=now)
        summary["combos"] = combos[:10]
        summary["total_combos"] = len(combos)
    print(json.dumps(summary, indent=2))
    return 0


def cmd_narrow(args, db, watches):
    w = get_watch(watches, args.watch)
    itineraries = []
    for o in w["origins"]:
        for d in w["destinations"]:
            res = narrow_query(o, d, args.depart, args.return_, w)
            if res:
                itineraries.extend(res)
    itineraries.sort(key=lambda x: x["price"])
    print(json.dumps({"watch": w["id"], "depart": args.depart, "return": args.return_,
                      "unknown": not itineraries, "itineraries": itineraries[:10]}, indent=2))
    return 0


def cmd_canary(args, db, watches):
    w = watches[0]  # canary uses its own fixed reference; cfg shape only
    ok, detail = run_canary(db, w)
    print(json.dumps({"ok": ok, "detail": detail}))
    return 0 if ok else 2


def cmd_check(args, db, watches):
    run_start = utcnow_iso()
    targets = [get_watch(watches, args.watch)] if args.watch else watches
    summary = {"watches": {}}
    for w in targets:
        obs_count = 0
        for offset in CHECK_DEPART_OFFSETS:
            depart = (date.today() + timedelta(days=offset)).isoformat()
            ret = (date.today() + timedelta(days=offset + CHECK_TRIP_DAYS)).isoformat()
            for o in w["origins"]:
                for d in w["destinations"]:
                    results = narrow_query(o, d, depart, ret, w)
                    if not results:
                        continue  # unknown, not "no deals"
                    for r in results:
                        add_observation(
                            db, watch_id=w["id"], origin=r["origin"] or o,
                            destination=r["destination"] or d, depart_date=depart,
                            return_date=ret, airline=r["airline"], stops=r["stops"],
                            cabin=r.get("cabin", w.get("cabin", "economy")),
                            price_usd=r["price"], booking_url=r["booking_url"],
                            observed_at=run_start)
                        obs_count += 1
        candidates = score_new_observations(
            db, w["id"], w["absolute_cap_usd"], run_start,
            cooldown_hours=w.get("cooldown_hours", 24))
        alerts = []
        for c in candidates:
            conf = verify_candidate(c, w)
            if not conf:
                continue
            obs = c["observation"]
            alert_id = add_alert(db, observation_id=obs["id"], reason=c["reason"],
                                 score=c["score"], verified_at=utcnow_iso())
            alerts.append({"id": alert_id, "reason": c["reason"], "score": c["score"],
                           "origin": obs["origin"], "destination": obs["destination"],
                           "depart": obs["depart_date"], "return": obs["return_date"],
                           "airline": obs["airline"], "stops": obs["stops"],
                           "price": obs["price_usd"],
                           "confirmed_price": conf["confirmed_price"],
                           "booking_url": conf["booking_url"]})
        summary["watches"][w["id"]] = {"observations": obs_count, "alerts": alerts}
    print(json.dumps(summary, indent=2))
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Cheap Flight Monitor (Phase 1)")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("broad", help="flex-date sweep for a watch")
    p.add_argument("--watch", required=True)
    p.add_argument("--duration", type=int, default=7)

    p = sub.add_parser("narrow", help="specific-date round trip for a watch")
    p.add_argument("--watch", required=True)
    p.add_argument("--depart", required=True)
    p.add_argument("--return", dest="return_", required=True)

    sub.add_parser("canary", help="self-health reference query")

    p = sub.add_parser("check", help="narrow poll + score + verify + record alerts")
    p.add_argument("--watch", default=None)

    args = ap.parse_args()
    try:
        defaults, watches = load_config()
        db = init_db(DB_PATH)
        sync_watches(db, watches, defaults)
        if args.cmd == "broad":
            return cmd_broad(args, db, watches)
        if args.cmd == "narrow":
            return cmd_narrow(args, db, watches)
        if args.cmd == "canary":
            return cmd_canary(args, db, watches)
        if args.cmd == "check":
            return cmd_check(args, db, watches)
    except SystemExit:
        raise
    except Exception:  # noqa: BLE001 - infrastructure failure -> non-zero exit
        traceback.print_exc()
        return 1
    return 1


if __name__ == "__main__":
    sys.exit(main())
