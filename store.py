"""SQLite storage for the cheap flight monitor.

Tables (per PLAN.md Appendix B, plus broad_scans for flex-sweep history):
  watches, observations, alerts, health, broad_scans
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS watches (
    id TEXT PRIMARY KEY,
    destinations_json TEXT NOT NULL,
    absolute_cap_usd REAL NOT NULL,
    relative_threshold REAL NOT NULL,
    config_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS observations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    watch_id TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    origin TEXT NOT NULL,
    destination TEXT NOT NULL,
    depart_date TEXT NOT NULL,
    return_date TEXT NOT NULL,
    airline TEXT,
    stops INTEGER,
    cabin TEXT,
    price_usd REAL,
    booking_url TEXT,
    source TEXT NOT NULL DEFAULT 'fli',
    observed_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_obs_watch_fp ON observations (watch_id, fingerprint, observed_at);
CREATE TABLE IF NOT EXISTS alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    observation_id INTEGER NOT NULL REFERENCES observations(id),
    reason TEXT NOT NULL,
    score REAL,
    verified_at TEXT,
    sent_at TEXT,
    channel TEXT
);
CREATE TABLE IF NOT EXISTS health (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    check_name TEXT NOT NULL,
    ok INTEGER NOT NULL,
    detail TEXT,
    checked_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS broad_scans (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    watch_id TEXT NOT NULL,
    depart_date TEXT NOT NULL,
    return_date TEXT NOT NULL,
    trip_days INTEGER NOT NULL,
    price_usd REAL NOT NULL,
    observed_at TEXT NOT NULL
);
"""


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def get_db(path: str | Path) -> sqlite3.Connection:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(str(path))
    db.row_factory = sqlite3.Row
    return db


def init_db(path: str | Path) -> sqlite3.Connection:
    db = get_db(path)
    db.executescript(SCHEMA)
    db.commit()
    return db


def make_fingerprint(origin: str, destination: str, depart_date: str,
                     return_date: str, airline: str | None, stops: int | None) -> str:
    raw = f"{origin}|{destination}|{depart_date}|{return_date}|{airline or ''}|{stops if stops is not None else ''}"
    return hashlib.sha1(raw.encode()).hexdigest()[:16]


def sync_watches(db: sqlite3.Connection, watches_cfg: list[dict], defaults: dict) -> None:
    """Upsert watches from watches.yaml into the DB."""
    for w in watches_cfg:
        db.execute(
            """INSERT INTO watches (id, destinations_json, absolute_cap_usd, relative_threshold, config_json)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(id) DO UPDATE SET
                 destinations_json=excluded.destinations_json,
                 absolute_cap_usd=excluded.absolute_cap_usd,
                 relative_threshold=excluded.relative_threshold,
                 config_json=excluded.config_json""",
            (w["id"], json.dumps(w["destinations"]), w["absolute_cap_usd"],
             defaults.get("relative_threshold", 0.35), json.dumps({**defaults, **w})),
        )
    db.commit()


def add_observation(db: sqlite3.Connection, *, watch_id: str, origin: str,
                    destination: str, depart_date: str, return_date: str,
                    airline: str | None, stops: int | None, cabin: str,
                    price_usd: float | None, booking_url: str | None,
                    observed_at: str | None = None) -> int:
    fp = make_fingerprint(origin, destination, depart_date, return_date, airline, stops)
    cur = db.execute(
        """INSERT INTO observations
           (watch_id, fingerprint, origin, destination, depart_date, return_date,
            airline, stops, cabin, price_usd, booking_url, observed_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (watch_id, fp, origin, destination, depart_date, return_date, airline,
         stops, cabin, price_usd, booking_url, observed_at or utcnow_iso()),
    )
    db.commit()
    return cur.lastrowid


def add_broad_scan(db: sqlite3.Connection, *, watch_id: str, depart_date: str,
                   return_date: str, trip_days: int, price_usd: float,
                   observed_at: str | None = None) -> None:
    db.execute(
        """INSERT INTO broad_scans (watch_id, depart_date, return_date, trip_days, price_usd, observed_at)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (watch_id, depart_date, return_date, trip_days, price_usd, observed_at or utcnow_iso()),
    )
    db.commit()


def latest_observations_since(db: sqlite3.Connection, watch_id: str, since_iso: str) -> list[sqlite3.Row]:
    """Latest observation per itinerary fingerprint, restricted to rows seen since `since_iso`."""
    return db.execute(
        """SELECT * FROM observations o
           WHERE watch_id = ? AND observed_at >= ?
             AND observed_at = (SELECT MAX(observed_at) FROM observations
                                WHERE watch_id = o.watch_id AND fingerprint = o.fingerprint)""",
        (watch_id, since_iso),
    ).fetchall()


def min_price_before(db: sqlite3.Connection, watch_id: str, fingerprint: str,
                     before_iso: str) -> float | None:
    row = db.execute(
        """SELECT MIN(price_usd) AS m FROM observations
           WHERE watch_id = ? AND fingerprint = ? AND observed_at < ? AND price_usd IS NOT NULL""",
        (watch_id, fingerprint, before_iso),
    ).fetchone()
    return row["m"] if row else None


def alerted_recently(db: sqlite3.Connection, fingerprint: str, cooldown_hours: float) -> bool:
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=cooldown_hours)).isoformat(timespec="seconds")
    row = db.execute(
        """SELECT 1 FROM alerts a JOIN observations o ON o.id = a.observation_id
           WHERE o.fingerprint = ? AND a.sent_at >= ? LIMIT 1""",
        (fingerprint, cutoff),
    ).fetchone()
    return row is not None


def add_alert(db: sqlite3.Connection, *, observation_id: int, reason: str,
              score: float | None, verified_at: str | None,
              channel: str = "chat") -> int:
    cur = db.execute(
        """INSERT INTO alerts (observation_id, reason, score, verified_at, sent_at, channel)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (observation_id, reason, score, verified_at, utcnow_iso(), channel),
    )
    db.commit()
    return cur.lastrowid


def add_health(db: sqlite3.Connection, check_name: str, ok: bool, detail: str) -> None:
    db.execute(
        "INSERT INTO health (check_name, ok, detail, checked_at) VALUES (?, ?, ?, ?)",
        (check_name, 1 if ok else 0, detail, utcnow_iso()),
    )
    db.commit()


def latest_health(db: sqlite3.Connection, check_name: str) -> sqlite3.Row | None:
    return db.execute(
        "SELECT * FROM health WHERE check_name = ? ORDER BY checked_at DESC LIMIT 1",
        (check_name,),
    ).fetchone()
