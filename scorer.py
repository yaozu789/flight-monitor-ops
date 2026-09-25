"""Deal scoring (Phase 1): absolute cap breach + new observed low.

Rolling-median anomaly scoring arrives in Phase 2 (needs history).
"""
from __future__ import annotations

import sqlite3

from store import alerted_recently, latest_observations_since, min_price_before


def score_new_observations(db: sqlite3.Connection, watch_id: str, cap_usd: float,
                           since_iso: str, cooldown_hours: float = 24) -> list[dict]:
    """Score observations recorded since `since_iso`.

    Returns candidate dicts: {"observation": row-dict, "reason": ..., "score": ...}.
    Dedups against recent alerts (cooldown).
    """
    candidates: list[dict] = []
    for row in latest_observations_since(db, watch_id, since_iso):
        price = row["price_usd"]
        if price is None:
            continue
        fp = row["fingerprint"]
        reasons: list[tuple[str, float]] = []
        if price <= cap_usd:
            reasons.append(("absolute", (cap_usd - price) / cap_usd if cap_usd else 0.0))
        hist_min = min_price_before(db, watch_id, fp, since_iso)
        if hist_min is not None and price < hist_min:
            reasons.append(("new_low", (hist_min - price) / hist_min if hist_min else 0.0))
        if not reasons:
            continue
        reason, score = max(reasons, key=lambda r: r[1])
        if alerted_recently(db, fp, cooldown_hours):
            continue
        candidates.append({"observation": dict(row), "reason": reason, "score": round(score, 4)})
    candidates.sort(key=lambda c: c["score"], reverse=True)
    return candidates
