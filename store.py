"""SQLite storage for the Flight Monitor trial (schema v2).

v2 is a strict SUPERSET of the Phase-1 / 01-03 schema: every existing table
keeps its shape (the live data/monitor.db has 1743 observations under the
Phase-1 layout, and fingerprints use the Phase-1 SHA1 scheme -- both are
preserved verbatim). v2 only ADDS tables/columns/functions:

- run_slots re-keyed to slot_id alone (one collection per slot; check and
  broad share it) -- the table is ephemeral scheduling state, recreated when
  its key shape differs.
- DB-backed collection lock + lock-loss counter (replaces the file lock).
- daily_counters for the atomic budget reserve (budget_ledger keeps one row
  per attempt as the audit trail).
- Outbox v2 columns (origin, destination, last_price_usd, confirmed_by, ...)
  added idempotently; guarded transitions; slot-gap retry eligibility.
- throttle_state: persisted 429 / CAPTCHA backoff.
- cells / triggers / bursts: sentinel rotation + burst scheduling (plan 04).

v3 is a strict superset of v2. Review-driven changes:
- Fingerprints may now include per-leg flight numbers and departure
  datetimes. The Phase-1 scheme is UNCHANGED when those fields are absent
  (old rows keep their hashes); when present they join the hash input so
  different flights hash differently.
- observations gains flight_numbers / depart_times columns (JSON lists,
  NULL for old rows); outbox gains unknown_count.
- Alert identity is (watch_id, departure ISO week): one alert slot per
  watch per week; re-alert only on a meaningful drop.
- The scheduler transition map is exactly pending -> handoff_prepared ->
  relay_attempted_unconfirmed. Retirement is a separate guarded operation
  (never from user_confirmed); human confirmation only from the two
  handed-off states, with the actor recorded.
- prepare_handoff enforces the handoff-count cap and slot gap inside the
  UPDATE. Retry cap is 2; a third consecutive unknown recheck retires.
"""
from __future__ import annotations

import hashlib
import json
import math
import sqlite3
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path


class DeadlineExceeded(BaseException):
    """A collection run exceeded its run deadline (run.RUN_DEADLINE_S).

    Control-flow only: run.run_guarded catches it for a CONTROLLED stop --
    finish_slot("deadline"), a health note, lock release, and a success
    ping -- never /fail. Collection loops raise it at loop tops via the
    deadline_fn passed down from run_guarded. It must never be swallowed by
    generic crash handlers.
    """

# ---------------------------------------------------------------------------
# Phase-1 / 01-03 tables: preserved VERBATIM (live DB compatibility).
# ---------------------------------------------------------------------------
SCHEMA_BASE = """
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
CREATE INDEX IF NOT EXISTS idx_obs_watch_time ON observations (watch_id, observed_at);
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
CREATE TABLE IF NOT EXISTS budget_ledger (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    day TEXT NOT NULL,              -- UTC calendar date YYYY-MM-DD
    category TEXT NOT NULL,         -- sentinel|background|burst|verify|retry_reserve|manual
    attempts INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ledger_day_cat ON budget_ledger (day, category);
CREATE TABLE IF NOT EXISTS outbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    alert_id TEXT NOT NULL UNIQUE,  -- stable id, kept across retries
    state TEXT NOT NULL,            -- pending|handoff_prepared|relay_attempted_unconfirmed|user_confirmed
    message_text TEXT NOT NULL,     -- final message, rendered by the script
    handoffs INTEGER NOT NULL DEFAULT 0,
    last_handoff_slot INTEGER,      -- slot_id of most recent handoff (NULL if never)
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_outbox_state ON outbox (state);
"""

# ---------------------------------------------------------------------------
# v2 additions.
# ---------------------------------------------------------------------------
SCHEMA_V2 = """
CREATE TABLE IF NOT EXISTS run_slots (
    slot_id INTEGER PRIMARY KEY,   -- floor(unix_time / 1800); ONE collection per slot
    command TEXT NOT NULL,         -- which command claimed it (check|broad)
    status TEXT NOT NULL,          -- in_progress|completed|budget_exhausted|throttled|crashed
    claimed_at TEXT NOT NULL,
    finished_at TEXT
);
CREATE TABLE IF NOT EXISTS daily_counters (
    day TEXT NOT NULL,
    category TEXT NOT NULL,
    n INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (day, category)
);
CREATE TABLE IF NOT EXISTS collection_lock (
    lock_id INTEGER PRIMARY KEY CHECK (lock_id = 1),
    owner TEXT NOT NULL,
    acquired_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS lock_loss (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    count INTEGER NOT NULL DEFAULT 0,
    last_at TEXT
);
CREATE TABLE IF NOT EXISTS throttle_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    kind TEXT NOT NULL,            -- http_429|captcha|blank_pages
    until_ts REAL NOT NULL,        -- unix time: do not collect before this
    retry_after_s REAL,            -- parsed Retry-After, if any
    consecutive INTEGER NOT NULL DEFAULT 1,
    detail TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS cells (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    group_id TEXT NOT NULL,
    depart_date TEXT NOT NULL,
    duration INTEGER NOT NULL,
    role TEXT NOT NULL,            -- sentinel|background
    weekday INTEGER NOT NULL,
    last_checked_slot INTEGER,
    last_success_at TEXT,
    last_price_usd REAL,
    active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    retired_at TEXT,
    window_name TEXT,              -- named window (near|far); NULL = legacy
    refresh_hours INTEGER,         -- refresh target; NULL = legacy rotation
    UNIQUE (group_id, depart_date, duration)
);
CREATE INDEX IF NOT EXISTS idx_cells_role_active ON cells (role, active, last_checked_slot);
CREATE TABLE IF NOT EXISTS triggers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL,            -- under_cap|drop
    group_id TEXT NOT NULL,
    depart_week TEXT NOT NULL,     -- YYYY-Www, for burst dedup
    depart_date TEXT NOT NULL,
    duration INTEGER NOT NULL,
    price_usd REAL NOT NULL,
    fingerprint TEXT,
    day TEXT NOT NULL,             -- UTC day queued (dedup scope)
    created_slot INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'queued',  -- queued|burst_active|burst|dropped|expired
    UNIQUE (kind, group_id, depart_week, day)
);
CREATE TABLE IF NOT EXISTS bursts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trigger_id INTEGER NOT NULL REFERENCES triggers (id),
    group_id TEXT NOT NULL,
    depart_week TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,          -- done|partial|dropped
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_bursts_group_week ON bursts (group_id, depart_week, created_at);
-- v3 group 4: resumable burst progress. A burst that exhausts the per-run
-- allowance (or the run deadline / budget) stops as "partial" and is
-- resumed by later slots until 60 attempts, all steps, or expiry -- it is
-- NEVER marked permanently done after one partial allowance.
CREATE TABLE IF NOT EXISTS verify_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    watch_id TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    observed_at TEXT NOT NULL,   -- the candidate observation that was checked
    verified_at TEXT NOT NULL,
    outcome TEXT NOT NULL        -- confirmed | not_confirmed
);
CREATE INDEX IF NOT EXISTS idx_verify_log_fp ON verify_log (fingerprint, observed_at);
CREATE TABLE IF NOT EXISTS burst_progress (
    trigger_id INTEGER PRIMARY KEY REFERENCES triggers (id),
    group_id TEXT NOT NULL,
    depart_week TEXT NOT NULL,
    attempts_used INTEGER NOT NULL DEFAULT 0,  -- cumulative across slots
    next_step INTEGER NOT NULL DEFAULT 0,      -- 0=revalidate, 1..20=neighbours, 21..29=durations
    status TEXT NOT NULL DEFAULT 'active',     -- active|done|dropped|expired
    expires_at TEXT NOT NULL,                  -- trigger created_at + 24h
    updated_at TEXT NOT NULL,
    revalidated_at TEXT                        -- last successful fare recheck
);
-- Weekly price report (weekly.py): one row per ISO week, relayed at most
-- REPORT_MAX_EMITS times until the agent records it with report-mark.
CREATE TABLE IF NOT EXISTS weekly_reports (
    week TEXT PRIMARY KEY,        -- ISO week YYYY-Www
    message_text TEXT NOT NULL,
    created_at TEXT NOT NULL,
    emits INTEGER NOT NULL DEFAULT 0,
    last_slot INTEGER,            -- slot whose relay batch carries it
    relayed_at TEXT               -- set by report-mark
);
-- One row per collection query (redesign A3): the outcome counters behind
-- the budget-ramp gate. outcome: ok | empty | transport_fail |
-- unparseable | error | budget_cancel | deadline_cancel | throttle.
-- ceiling = the daily ceiling (budget stage) the query ran under.
CREATE TABLE IF NOT EXISTS query_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    at TEXT NOT NULL,
    slot INTEGER,
    group_id TEXT,
    window_name TEXT,
    category TEXT NOT NULL,
    outcome TEXT NOT NULL,
    ceiling INTEGER
);
CREATE INDEX IF NOT EXISTS idx_query_log_at ON query_log (at);
"""

# Outbox lifecycle (v3): the SCHEDULER transition map is exactly
#   pending -> handoff_prepared -> relay_attempted_unconfirmed
# Retirement is a separate guarded operation (retire_outbox), never from
# user_confirmed; user_confirmed is terminal and may ONLY be set by the
# human-driven outbox-confirm command via set_outbox_user_confirmed.
OUTBOX_TRANSITIONS = {
    "pending": {"handoff_prepared"},
    "handoff_prepared": {"relay_attempted_unconfirmed"},
    "relay_attempted_unconfirmed": set(),
    "user_confirmed": set(),
    "retired": set(),
}

# Retry policy (v3): a prepared handoff becomes eligible for a rechecked
# retry after RETRY_MIN_SLOT_GAP slots, at most RETRY_MAX_ATTEMPTS handoffs
# per alert. A third consecutive unknown recheck retires the alert.
RETRY_MIN_SLOT_GAP = 3
RETRY_MAX_ATTEMPTS = 2
# A retired week re-alerts the same fare only after this long (a
# meaningful drop re-alerts at once).
RETIRED_REARM_HOURS = 72
UNKNOWN_MAX = 3

# Departure-week meaningful-drop dedup (smaller item): an already-outboxed
# deal re-arms to pending only if the fresh price dropped by at least
# max($15, 5%) below the last outboxed price.
SIGNIFICANT_DROP_USD = 15.0
SIGNIFICANT_DROP_PCT = 0.05

# v2 outbox columns added idempotently by migrate_db().
_OUTBOX_V2_COLUMNS = [
    ("watch_id", "TEXT"),
    ("fingerprint", "TEXT"),
    ("origin", "TEXT"),
    ("destination", "TEXT"),
    ("price_usd", "REAL"),
    ("depart_date", "TEXT"),
    ("return_date", "TEXT"),
    ("airline", "TEXT"),
    ("stops", "INTEGER"),
    ("booking_url", "TEXT"),
    ("handoff_count", "INTEGER NOT NULL DEFAULT 0"),
    # Bumped on every rearm (a new price event under the same alert_id), so
    # a late outbox-mark/outbox-confirm cannot land on a newer deal.
    ("event_no", "INTEGER NOT NULL DEFAULT 1"),
    ("prepared_slot", "INTEGER"),
    ("prepared_at", "TEXT"),
    ("last_price_usd", "REAL"),
    ("confirmed_by", "TEXT"),
    ("unknown_count", "INTEGER NOT NULL DEFAULT 0"),  # v3: unknown rechecks
    # The outbound departure-time limit the alerted fare was found with
    # ("5-11"); retry rechecks repeat exactly that search.
    ("outbound_hours", "TEXT"),
]

# Burst triggers remember the window they came from, so every burst step
# (a Friday or a Saturday neighbour) gets its own weekday's limit from it.
_TRIGGER_COLUMNS = [
    ("window_name", "TEXT"),
]

# Per-run collection stats on run_slots (NULL for runs before they existed):
# collection attempts, and whether the run collected nothing although a
# sentinel or background check was affordable (a stall).
_RUN_SLOTS_COLUMNS = [
    ("attempts", "INTEGER"),
    ("stalled", "INTEGER"),
    ("idle", "INTEGER"),   # 1 = nothing was due (named windows); ends a stall streak
]

# v3 columns on the observations table: per-leg flight numbers and departure
# datetimes (JSON lists). NULL for old rows -- old fingerprints (computed
# without these fields) keep matching the Phase-1 scheme verbatim.
_OBS_V3_COLUMNS = [
    ("flight_numbers", "TEXT"),
    ("depart_times", "TEXT"),
    # The outbound departure-time limit the fare was searched with ("5-11"),
    # so verify and retry rechecks repeat exactly the same search.
    ("outbound_hours", "TEXT"),
    # The named window whose check found the fare (bursts inherit it).
    ("window_name", "TEXT"),
]

# Named departure windows (redesign PR A1): which window minted a cell and
# how often it should be refreshed. NULL for cells minted by the legacy
# single-window config, which keep plain least-recently-checked rotation.
_CELLS_WINDOW_COLUMNS = [
    ("window_name", "TEXT"),
    ("refresh_hours", "INTEGER"),
]


# ---------------------------------------------------------------- time utils

def utcnow_iso() -> str:
    # Phase-1 format (kept for lexicographic comparability with existing rows).
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def today_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def slot_id(now: float | None = None) -> int:
    """Half-hour slot index since the Unix epoch (48 slots per UTC day)."""
    return int((now if now is not None else time.time()) // 1800)


def slot_start_ts(slot: int) -> float:
    return slot * 1800.0


def slots_left_today(slot: int) -> int:
    """Slots remaining in the current UTC day, including this one (1..48)."""
    return 48 - (slot % 48)


def depart_week(depart_date: str) -> str:
    """ISO week label (YYYY-Www) of a departure date.

    v3 alert identity is (watch_id, depart_week): one alert slot per watch
    per week. Also used for burst dedup.
    """
    d = date.fromisoformat(depart_date)
    iso_year, iso_week, _ = d.isocalendar()
    return f"{iso_year}-W{iso_week:02d}"


# ---------------------------------------------------------------- db plumbing

def get_db(path: str | Path) -> sqlite3.Connection:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(str(path), timeout=30.0)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL;")
    db.execute("PRAGMA busy_timeout=5000;")
    db.execute("PRAGMA foreign_keys=ON;")
    return db


def migrate_db(db: sqlite3.Connection) -> None:
    """Additive migrations. Never drops user data; run_slots is ephemeral
    scheduling state and is recreated when its key shape differs."""
    cols = {r["name"] for r in db.execute("PRAGMA table_info(outbox)")}
    if cols:
        for name, ddl in _OUTBOX_V2_COLUMNS:
            if name not in cols:
                db.execute(f"ALTER TABLE outbox ADD COLUMN {name} {ddl}")
    obs_cols = {r["name"] for r in db.execute("PRAGMA table_info(observations)")}
    if obs_cols:
        for name, ddl in _OBS_V3_COLUMNS:
            if name not in obs_cols:
                db.execute(f"ALTER TABLE observations ADD COLUMN {name} {ddl}")
    trig_cols = {r["name"] for r in db.execute("PRAGMA table_info(triggers)")}
    if trig_cols:
        for name, ddl in _TRIGGER_COLUMNS:
            if name not in trig_cols:
                db.execute(f"ALTER TABLE triggers ADD COLUMN {name} {ddl}")
    cell_cols = {r["name"] for r in db.execute("PRAGMA table_info(cells)")}
    if cell_cols:
        for name, ddl in _CELLS_WINDOW_COLUMNS:
            if name not in cell_cols:
                db.execute(f"ALTER TABLE cells ADD COLUMN {name} {ddl}")
    bp_cols = {r["name"] for r in db.execute(
        "PRAGMA table_info(burst_progress)")}
    if bp_cols and "revalidated_at" not in bp_cols:
        # When the trigger fare was last rechecked successfully (separate
        # from updated_at, which any partial progress also bumps).
        db.execute("ALTER TABLE burst_progress ADD COLUMN revalidated_at TEXT")
    info = {r["name"] for r in db.execute("PRAGMA table_info(run_slots)")}
    if info and "claimed_at" not in info:
        # Patch-01 key was (slot_id, command); v2 keys slot_id alone.
        db.execute("DROP TABLE IF EXISTS run_slots")
        info = set()
    if info:
        for name, ddl in _RUN_SLOTS_COLUMNS:
            if name not in info:
                db.execute(f"ALTER TABLE run_slots ADD COLUMN {name} {ddl}")
    db.commit()


def init_db(path_or_db: str | Path | sqlite3.Connection) -> sqlite3.Connection:
    """Accepts a path (Phase-1 convention) or an open connection."""
    if isinstance(path_or_db, sqlite3.Connection):
        db = path_or_db
    else:
        db = get_db(path_or_db)
    migrate_db(db)
    db.executescript(SCHEMA_BASE)
    db.executescript(SCHEMA_V2)
    migrate_db(db)  # add v2 columns to tables the base script just created
    # daily_counters (which enforce the limits) arrived after budget_ledger:
    # backfill them from the ledger so a migrated DB does not forget today's
    # spend. MAX keeps this idempotent (ledger_insert bumps both together).
    db.execute(
        """INSERT INTO daily_counters (day, category, n)
           SELECT day, category, SUM(attempts) FROM budget_ledger
           WHERE true GROUP BY day, category
           ON CONFLICT (day, category) DO UPDATE SET n = MAX(n, excluded.n)""")
    db.commit()
    return db


# ---------------------------------------------------------------- Phase-1 API (preserved verbatim)

def make_fingerprint(origin: str, destination: str, depart_date: str,
                     return_date: str, airline: str | None, stops: int | None,
                     flight_numbers: list[str] | None = None,
                     depart_times: list[str] | None = None) -> str:
    # Phase-1 SHA1 scheme -- must not change: 1743 existing observations use
    # it. When the optional v3 leg fields are absent, the input is byte-
    # identical to Phase-1, so old rows keep their hashes verbatim. When
    # present (every new fli result carries them), they join the hash so two
    # itineraries that differ only in flight number / departure time hash
    # differently.
    raw = f"{origin}|{destination}|{depart_date}|{return_date}|{airline or ''}|{stops if stops is not None else ''}"
    fns = "|".join(str(f) for f in flight_numbers) if flight_numbers else ""
    dts = "|".join(str(t) for t in depart_times) if depart_times else ""
    if fns or dts:
        raw = f"{raw}|{fns}|{dts}"
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
                    observed_at: str | None = None,
                    flight_numbers: list[str] | None = None,
                    depart_times: list[str] | None = None,
                    outbound_hours: str | None = None,
                    window_name: str | None = None) -> int:
    # v3: every new fli result populates the leg fields; old callers omit
    # them and get the Phase-1 fingerprint verbatim (compat).
    fp = make_fingerprint(origin, destination, depart_date, return_date,
                          airline, stops, flight_numbers, depart_times)
    cur = db.execute(
        """INSERT INTO observations
           (watch_id, fingerprint, origin, destination, depart_date, return_date,
            airline, stops, cabin, price_usd, booking_url, observed_at,
            flight_numbers, depart_times, outbound_hours, window_name)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (watch_id, fp, origin, destination, depart_date, return_date, airline,
         stops, cabin, price_usd, booking_url, observed_at or utcnow_iso(),
         json.dumps(flight_numbers) if flight_numbers else None,
         json.dumps(depart_times) if depart_times else None,
         outbound_hours, window_name),
    )
    db.commit()
    return cur.lastrowid


def save_observations(db: sqlite3.Connection, watch_id: str,
                      observations: list[dict], run_start: str) -> int:
    """Batch variant of add_observation for poller rows (keys: origin,
    destination, depart_date, return_date, airline, stops, cabin, price_usd,
    currency, booking_url, flight_numbers, depart_times)."""
    n = 0
    for o in observations:
        add_observation(
            db, watch_id=watch_id, origin=o.get("origin") or "",
            destination=o.get("destination") or "", depart_date=o["depart_date"],
            return_date=o["return_date"], airline=o.get("airline"),
            stops=o.get("stops"), cabin=o.get("cabin", "economy"),
            price_usd=o.get("price_usd"), booking_url=o.get("booking_url"),
            observed_at=run_start,
            flight_numbers=o.get("flight_numbers"),
            depart_times=o.get("depart_times"),
            outbound_hours=o.get("outbound_hours"),
            window_name=o.get("window_name"))
        n += 1
    return n


def add_broad_scan(db: sqlite3.Connection, *, watch_id: str, depart_date: str,
                   return_date: str, trip_days: int, price_usd: float,
                   observed_at: str | None = None) -> None:
    db.execute(
        """INSERT INTO broad_scans (watch_id, depart_date, return_date, trip_days, price_usd, observed_at)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (watch_id, depart_date, return_date, trip_days, price_usd, observed_at or utcnow_iso()),
    )
    db.commit()


def latest_observations_since(db: sqlite3.Connection, watch_id: str, since_iso: str) -> list[dict]:
    """Latest observation per itinerary fingerprint, restricted to rows seen since `since_iso`."""
    return [dict(r) for r in db.execute(
        """SELECT * FROM observations o
           WHERE watch_id = ? AND observed_at >= ?
             AND id = (SELECT id FROM observations
                       WHERE watch_id = o.watch_id AND fingerprint = o.fingerprint
                       ORDER BY observed_at DESC, id DESC LIMIT 1)""",
        (watch_id, since_iso),
    ).fetchall()]


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


def add_alert(db: sqlite3.Connection, *, observation_id: int | None = None,
              reason: str, score: float | None = None,
              verified_at: str | None = None, channel: str = "chat",
              fingerprint: str | None = None, **_ignored) -> int:
    """Scorer-history record. Phase-1 callers pass observation_id; v2 callers
    may pass fingerprint instead (resolved to the latest observation)."""
    if observation_id is None:
        if fingerprint is None:
            raise TypeError("add_alert requires observation_id or fingerprint")
        row = db.execute(
            "SELECT id FROM observations WHERE fingerprint = ? ORDER BY observed_at DESC LIMIT 1",
            (fingerprint,)).fetchone()
        if row is None:
            raise KeyError(f"no observation for fingerprint {fingerprint}")
        observation_id = row["id"]
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


def price_history(db: sqlite3.Connection, watch_id: str, depart_date: str,
                  return_date: str, *, origin: str | None = None,
                  destination: str | None = None, lookback_days: int = 14,
                  depart_slack_days: int = 3, recent: int = 5) -> dict:
    """Observed prices for the same watch and (when given) the same
    origin->destination airport pair, departures within +-slack days of
    *depart_date*, and the same trip length, over the last *lookback_days*.

    Everything comes from stored observations, so an alert can show real,
    dated numbers instead of a claim. Daily lows are per UTC observation day
    (observed_at is always written as canonical UTC by utcnow_iso/run_start).
    """
    dep = datetime.fromisoformat(depart_date).date()
    ret = datetime.fromisoformat(return_date).date()
    duration = (ret - dep).days
    lo = (dep - timedelta(days=depart_slack_days)).isoformat()
    hi = (dep + timedelta(days=depart_slack_days)).isoformat()
    since = (datetime.now(timezone.utc)
             - timedelta(days=lookback_days)).isoformat()
    route_sql, route_args = "", []
    if origin and destination:
        route_sql = " AND origin = ? AND destination = ?"
        route_args = [origin, destination]
    rows = db.execute(
        f"""SELECT substr(observed_at, 1, 10) AS day, MIN(price_usd) AS low,
                   COUNT(*) AS n
            FROM observations
            WHERE watch_id = ? AND price_usd IS NOT NULL
              AND observed_at >= ? AND depart_date BETWEEN ? AND ?
              AND CAST(julianday(return_date) - julianday(depart_date)
                       AS INTEGER) = ?{route_sql}
            GROUP BY day ORDER BY day""",
        (watch_id, since, lo, hi, duration, *route_args)).fetchall()
    daily = [(r["day"], float(r["low"])) for r in rows]
    checks = sum(int(r["n"]) for r in rows)
    out = {"lookback_days": lookback_days, "depart_from": lo, "depart_to": hi,
           "duration": duration, "checks": checks, "days": len(daily),
           "daily_lows": daily, "origin": origin, "destination": destination}
    if daily:
        low_day, low = min(daily, key=lambda x: (x[1], x[0]))
        lows = sorted(p for _, p in daily)
        mid = len(lows) // 2
        median = lows[mid] if len(lows) % 2 else (lows[mid - 1] + lows[mid]) / 2
        out.update({"since": daily[0][0], "low": low, "low_day": low_day,
                    "median_daily_low": median, "recent": daily[-recent:]})
    return out


def log_verify(db: sqlite3.Connection, watch_id: str, fingerprint: str,
               observed_at: str, outcome: str) -> None:
    """Record that the candidate observation (fingerprint, observed_at) was
    rechecked, so it is verified at most once however long it stays in the
    candidate lookback."""
    db.execute(
        """INSERT INTO verify_log (watch_id, fingerprint, observed_at,
                                   verified_at, outcome)
           VALUES (?, ?, ?, ?, ?)""",
        (watch_id, fingerprint, observed_at, utcnow_iso(), outcome))
    db.commit()


def verify_logged(db: sqlite3.Connection, watch_id: str, fingerprint: str,
                  observed_at: str) -> bool:
    """True when this watch already rechecked this fingerprint for an
    observation at or after *observed_at* (a newer observation is fresh
    evidence)."""
    return db.execute(
        "SELECT 1 FROM verify_log WHERE watch_id = ? AND fingerprint = ? "
        "AND observed_at >= ?",
        (watch_id, fingerprint, observed_at)).fetchone() is not None


def first_seen(db: sqlite3.Connection, fingerprint: str) -> str | None:
    row = db.execute(
        "SELECT MIN(observed_at) AS m FROM observations WHERE fingerprint = ?",
        (fingerprint,)).fetchone()
    return row["m"] if row else None


def has_under_cap_before(db: sqlite3.Connection, fingerprint: str, cap: float,
                         run_start: str) -> bool:
    """True if this fingerprint was ever observed at/below cap before this run
    (burst trigger: 'new under-cap observation')."""
    row = db.execute(
        """SELECT 1 FROM observations
           WHERE fingerprint = ? AND price_usd <= ? AND observed_at < ?
           LIMIT 1""", (fingerprint, cap, run_start)).fetchone()
    return row is not None


# ---------------------------------------------------------------- outbox v2

def enqueue_alert(db: sqlite3.Connection, *, alert_id: str, watch_id: str,
                  fingerprint: str, price_usd: float, depart_date: str,
                  return_date: str, airline: str | None, stops: int | None,
                  booking_url: str | None, message_text: str,
                  origin: str | None = None,
                  destination: str | None = None,
                  outbound_hours: str | None = None) -> tuple[str, str]:
    """Durably queue an alert as pending.

    Idempotent on alert_id. An existing week re-arms to pending as a new
    event (fresh retry allowance) when the new price is at least
    max($15, 5%) below the last outboxed price -- or unconditionally when
    the week's earlier alert was retired. Otherwise the duplicate is
    ignored. Returns (alert_id, action) with action in
    {"inserted", "deduped", "rearmed"}.
    """
    now = utcnow_iso()
    row = db.execute("SELECT * FROM outbox WHERE alert_id = ?",
                     (alert_id,)).fetchone()
    if row is None:
        db.execute(
            """INSERT INTO outbox
               (alert_id, watch_id, fingerprint, origin, destination, price_usd,
                depart_date, return_date, airline, stops, booking_url,
                message_text, state, handoff_count, last_price_usd,
                created_at, updated_at, outbound_hours)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?,
                       ?)""",
            (alert_id, watch_id, fingerprint, origin, destination, price_usd,
             depart_date, return_date, airline, stops, booking_url, message_text,
             price_usd, now, now, outbound_hours))
        db.commit()
        return alert_id, "inserted"
    baseline = row["last_price_usd"] if row["last_price_usd"] is not None \
        else row["price_usd"]
    threshold = (max(SIGNIFICANT_DROP_USD, SIGNIFICANT_DROP_PCT * baseline)
                 if baseline is not None else None)
    dropped = threshold is not None and price_usd <= baseline - threshold
    if row["state"] == "retired":
        # The earlier fare disappeared (or kept rechecking unknown). A
        # meaningful drop is news at once; the same fare coming back is
        # only re-alerted after RETIRED_REARM_HOURS, so a fare flickering
        # between gone and available cannot alert daily.
        retired_at = datetime.fromisoformat(row["updated_at"])
        aged = (datetime.now(timezone.utc) - retired_at
                >= timedelta(hours=RETIRED_REARM_HOURS))
        rearm = dropped or aged
    else:
        # Active or user-confirmed: only a meaningful drop is news.
        rearm = dropped
    if not rearm:
        return alert_id, "deduped"
    # A new price event: replace ALL itinerary fields (it may be a different
    # flight) and start a fresh retry allowance. Keeping the old
    # handoff_count left a row that had used its 2 handoffs stuck pending.
    db.execute(
        """UPDATE outbox
           SET state='pending', fingerprint=?, origin=?, destination=?,
               depart_date=?, return_date=?, airline=?, stops=?,
               price_usd=?, booking_url=?, message_text=?,
               last_price_usd=?, handoff_count=0, prepared_slot=NULL,
               prepared_at=NULL, unknown_count=0, confirmed_by=NULL,
               event_no=event_no + 1, updated_at=?, outbound_hours=?
           WHERE alert_id=?""",
        (fingerprint, origin, destination, depart_date, return_date,
         airline, stops, price_usd, booking_url, message_text,
         price_usd, now, outbound_hours, alert_id))
    db.commit()
    return alert_id, "rearmed"


def outbox_get(db: sqlite3.Connection, alert_id: str) -> dict | None:
    row = db.execute("SELECT * FROM outbox WHERE alert_id = ?",
                     (alert_id,)).fetchone()
    return dict(row) if row else None


def outbox_list(db: sqlite3.Connection, state: str | None = None) -> list[dict]:
    if state:
        rows = db.execute("SELECT * FROM outbox WHERE state = ? ORDER BY updated_at DESC",
                          (state,))
    else:
        rows = db.execute("SELECT * FROM outbox ORDER BY updated_at DESC")
    return [dict(r) for r in rows]


def prepare_handoff(db: sqlite3.Connection, slot: int,
                    new_ids: list[str] | None = None) -> list[dict]:
    """Flip eligible pending rows to handoff_prepared in one UPDATE.

    Eligibility is enforced inside the UPDATE itself, so concurrent callers
    cannot prepare the same row twice and non-pending rows are never touched
    (v2 spec item 6). v3 adds two predicates, also inside the UPDATE (they
    apply to the filtered-ID form as well):
      - handoff_count < RETRY_MAX_ATTEMPTS (the retry cap),
      - the last handoff was at least RETRY_MIN_SLOT_GAP slots ago
        (prepared_slot NULL = never handed off = eligible).
    Returns the prepared rows.
    """
    now = utcnow_iso()
    where_extra = ("AND handoff_count < ? "
                   "AND (prepared_slot IS NULL OR prepared_slot <= ?)")
    params_extra = (RETRY_MAX_ATTEMPTS, slot - RETRY_MIN_SLOT_GAP)
    # RETURNING: only rows THIS call transitioned. Selecting every
    # prepared row of the slot re-emitted rows prepared by an earlier call
    # in the same slot (duplicate relays).
    if new_ids is None:
        cur = db.execute(
            f"""UPDATE outbox
               SET state='handoff_prepared', handoff_count=handoff_count + 1,
                   prepared_slot=?, prepared_at=?, updated_at=?
               WHERE state='pending' {where_extra}
               RETURNING *""",
            (slot, now, now, *params_extra))
    elif new_ids:
        marks = ",".join("?" for _ in new_ids)
        cur = db.execute(
            f"""UPDATE outbox
                SET state='handoff_prepared', handoff_count=handoff_count + 1,
                    prepared_slot=?, prepared_at=?, updated_at=?
                WHERE state='pending' {where_extra}
                  AND alert_id IN ({marks})
                RETURNING *""",
            (slot, now, now, *params_extra, *new_ids))
    else:
        return []
    rows = [dict(r) for r in cur.fetchall()]
    db.commit()
    return sorted(rows, key=lambda r: (r["price_usd"] is None, r["price_usd"]))


def reprepare_retries(db: sqlite3.Connection, slot: int,
                      retry_ids: list[str]) -> None:
    """Re-handoff confirmed retries in the current slot.

    Retry rows are already in a handoff state (handoff_prepared or
    relay_attempted_unconfirmed); a re-handoff bumps handoff_count (so the
    2-handoff cap in eligible_retry_candidates is honored) and stamps the
    current prepared_slot. A confirmed recheck also clears the unknown streak.
    Rows not in a handoff state are never touched.
    """
    if not retry_ids:
        return
    now = utcnow_iso()
    marks = ",".join("?" for _ in retry_ids)
    db.execute(
        f"""UPDATE outbox
            SET handoff_count=handoff_count + 1, prepared_slot=?,
                prepared_at=?, updated_at=?, unknown_count=0
            WHERE alert_id IN ({marks})
              AND state IN ('handoff_prepared', 'relay_attempted_unconfirmed')
              AND handoff_count < ?
              AND (prepared_slot IS NULL OR prepared_slot <= ?)""",
        (slot, now, now, *retry_ids, RETRY_MAX_ATTEMPTS,
         slot - RETRY_MIN_SLOT_GAP))
    db.commit()


def eligible_retry_candidates(db: sqlite3.Connection, slot: int) -> list[dict]:
    """Prepared-but-unconfirmed handoffs eligible for a rechecked retry."""
    return [dict(r) for r in db.execute(
        """SELECT * FROM outbox
           WHERE state IN ('handoff_prepared', 'relay_attempted_unconfirmed')
             AND handoff_count < ?
             AND prepared_slot <= ?
           ORDER BY prepared_slot ASC""",
        (RETRY_MAX_ATTEMPTS, slot - RETRY_MIN_SLOT_GAP))]


def update_outbox_state(db: sqlite3.Connection, alert_id: str, new_state: str,
                        actor: str = "scheduler",
                        event_no: int | None = None) -> bool:
    """Guarded state transition. Returns False when the transition is illegal.

    v3: the scheduler map is exactly pending -> handoff_prepared ->
    relay_attempted_unconfirmed. In particular "retired" is not reachable
    here (use retire_outbox) and user_confirmed is never settable through
    this path -- only set_outbox_user_confirmed (the human-driven
    outbox-confirm command) may set it.
    """
    row = outbox_get(db, alert_id)
    if row is None:
        return False
    allowed = OUTBOX_TRANSITIONS.get(row["state"], set())
    if new_state not in allowed:
        return False
    if new_state == "user_confirmed" and actor != "user":
        return False
    # Expected-state predicate IN the update: a concurrent change (e.g. a
    # human confirmation) between the read above and this write wins.
    if event_no is not None and row["event_no"] != event_no:
        return False  # a newer price event replaced the one being marked
    cur = db.execute(
        "UPDATE outbox SET state=?, updated_at=? WHERE alert_id=? AND state=?"
        " AND event_no=?",
        (new_state, utcnow_iso(), alert_id, row["state"], row["event_no"]))
    db.commit()
    return cur.rowcount > 0


def note_unknown_recheck(db: sqlite3.Connection, alert_id: str) -> int:
    """Bump the persisted unknown-recheck counter; returns the new count.

    The counter is cleared by reprepare_retries (a confirmed recheck breaks
    the streak); the caller retires the row at store.UNKNOWN_MAX.
    """
    db.execute(
        "UPDATE outbox SET unknown_count = unknown_count + 1, updated_at=? "
        "WHERE alert_id=?",
        (utcnow_iso(), alert_id),
    )
    db.commit()
    row = outbox_get(db, alert_id)
    return row["unknown_count"] if row else 0


def set_outbox_user_confirmed(db: sqlite3.Connection, alert_id: str,
                              actor: str, event_no: int | None = None) -> bool:
    """Final user confirmation. Only a human-driven outbox-confirm call may
    invoke this (spec item 10); the actor name is recorded for audit.

    v3: a pending alert has never been handed off, so there is nothing for
    the user to confirm -- only handoff_prepared and
    relay_attempted_unconfirmed may be confirmed.
    """
    row = outbox_get(db, alert_id)
    if row is None:
        return False
    if row["state"] not in ("handoff_prepared", "relay_attempted_unconfirmed"):
        return False
    if event_no is not None and row["event_no"] != event_no:
        return False  # the user saw an earlier deal; a newer one replaced it
    cur = db.execute(
        """UPDATE outbox SET state='user_confirmed', confirmed_by=?,
           updated_at=? WHERE alert_id=? AND event_no=?
             AND state IN ('handoff_prepared', 'relay_attempted_unconfirmed')""",
        (actor, utcnow_iso(), alert_id, row["event_no"]))
    db.commit()
    if cur.rowcount == 0:
        return False  # retired or changed concurrently
    add_health(db, "outbox_confirm", True,
               f"{alert_id} confirmed by {actor}")
    return True


def update_outbox_message(db: sqlite3.Connection, alert_id: str,
                          message_text: str, price_usd: float) -> None:
    """Re-render the stored handoff text from a fresh recheck (spec item 8)."""
    db.execute(
        """UPDATE outbox SET message_text=?, price_usd=?, last_price_usd=?,
           updated_at=? WHERE alert_id=?""",
        (message_text, price_usd, price_usd, utcnow_iso(), alert_id))
    db.commit()


def retire_outbox(db: sqlite3.Connection, alert_id: str, reason: str) -> bool:
    """Guarded retirement, separate from update_outbox_state.

    The v3 scheduler transition map has no edge into retired: retirement is
    an explicit operation, never a transition. user_confirmed is terminal
    and can never be retired; retired rows stay retired.
    """
    row = outbox_get(db, alert_id)
    if row is None:
        return False
    if row["state"] not in ("pending", "handoff_prepared",
                            "relay_attempted_unconfirmed"):
        return False
    cur = db.execute(
        """UPDATE outbox SET state='retired', updated_at=? WHERE alert_id=?
             AND state IN ('pending', 'handoff_prepared',
                           'relay_attempted_unconfirmed')""",
        (utcnow_iso(), alert_id))
    db.commit()
    if cur.rowcount == 0:
        return False  # confirmed (terminal) or changed concurrently
    add_health(db, "outbox_retire", True, f"{alert_id}: {reason}")
    return True


def enter_collect_mode(db: sqlite3.Connection, watch_ids) -> dict:
    """Silence the alert path for collect-only watches (redesign A2).

    In ONE transaction: retire every non-terminal outbox row of these
    watches, and expire their queued/active burst triggers together with
    the burst progress. Run before the drain, so the first collect-mode run
    can never relay an alert queued under the old cap rule. Idempotent: a
    watch already in collect mode has nothing left to change. Returns the
    changed counts; each retired alert is logged to health as
    "mode_collect".
    """
    ids = sorted(set(watch_ids))
    if not ids:
        return {"outbox": 0, "triggers": 0, "bursts": 0}
    marks = ",".join("?" * len(ids))
    now = utcnow_iso()
    try:
        db.execute("BEGIN IMMEDIATE")
        # Read, retire and log inside the same transaction, so the health
        # audit and the returned count match exactly what was retired.
        retired = [r["alert_id"] for r in db.execute(
            f"""SELECT alert_id FROM outbox WHERE watch_id IN ({marks})
                AND state IN ('pending', 'handoff_prepared',
                              'relay_attempted_unconfirmed')""", ids)]
        db.executemany(
            """INSERT INTO health (check_name, ok, detail, checked_at)
               VALUES ('outbox_retire', 1, ?, ?)""",
            [(f"{aid}: mode_collect", now) for aid in retired])
        db.execute(
            f"""UPDATE outbox SET state='retired', updated_at=?
                WHERE watch_id IN ({marks})
                  AND state IN ('pending', 'handoff_prepared',
                                'relay_attempted_unconfirmed')""",
            [now, *ids])
        trig = db.execute(
            f"""UPDATE triggers SET status='expired'
                WHERE group_id IN ({marks})
                  AND status IN ('queued', 'burst_active')""", ids).rowcount
        bursts = db.execute(
            f"""UPDATE burst_progress SET status='expired', updated_at=?
                WHERE group_id IN ({marks}) AND status='active'""",
            [now, *ids]).rowcount
        db.commit()
    except BaseException:
        db.rollback()
        raise
    return {"outbox": len(retired), "triggers": trig, "bursts": bursts}


# ---------------------------------------------------------------- slots & lock

def claim_slot(db: sqlite3.Connection, slot: int, command: str) -> bool:
    """Claim a slot for collection. True only on a fresh claim; any existing
    row (any status) means this slot already ran and the caller must no-op.
    This is what keeps check and broad from double-collecting (spec item 4)."""
    try:
        db.execute(
            """INSERT INTO run_slots (slot_id, command, status, claimed_at)
               VALUES (?, ?, 'in_progress', ?)""",
            (slot, command, utcnow_iso()))
        db.commit()
        return True
    except sqlite3.IntegrityError:
        return False


def finish_slot(db: sqlite3.Connection, slot: int, status: str) -> None:
    db.execute(
        "UPDATE run_slots SET status=?, finished_at=? WHERE slot_id=?",
        (status, utcnow_iso(), slot))
    db.commit()


def deadline_streak(db: sqlite3.Connection, max_gap: int = 1) -> int:
    """Number of back-to-back runs, ending at the most recent one, whose
    status is "deadline". Runs are back-to-back when their slots are at
    most *max_gap* apart (the run cadence in slots: 1 at 30 min, 4 at
    2 h). Any other status (including an abandoned in_progress row) or a
    larger gap (a skipped run) ends the streak."""
    streak = 0
    prev = None
    for r in db.execute("SELECT slot_id, status FROM run_slots "
                        "ORDER BY slot_id DESC"):
        if r["status"] != "deadline":
            break
        if prev is not None and prev - r["slot_id"] > max_gap:
            break
        streak += 1
        prev = r["slot_id"]
    return streak


def record_run_stats(db: sqlite3.Connection, slot: int, attempts: int,
                     stalled: bool, idle: bool = False) -> None:
    db.execute("UPDATE run_slots SET attempts=?, stalled=?, idle=? "
               "WHERE slot_id=?", (attempts, int(stalled), int(idle), slot))
    db.commit()


def stall_streak(db: sqlite3.Connection, max_gap: int = 1) -> int:
    """Stalled runs back to back, ending at the most recent run (same gap
    rule as deadline_streak). A neutral run -- 0 attempts because nothing
    was affordable -- neither counts nor breaks the streak; an idle run
    (nothing was due: the collector was healthy) ends it, as does any other
    run (progress, or no stats recorded: stop or crash)."""
    streak = 0
    prev = None
    for r in db.execute("SELECT slot_id, attempts, stalled, idle "
                        "FROM run_slots ORDER BY slot_id DESC"):
        if prev is not None and prev - r["slot_id"] > max_gap:
            break
        prev = r["slot_id"]
        if r["idle"]:
            break
        if r["stalled"]:
            streak += 1
        elif r["attempts"] == 0 and r["stalled"] == 0:
            continue  # neutral
        else:
            break
    return streak


def slot_done(db: sqlite3.Connection, slot: int) -> bool:
    row = db.execute("SELECT 1 FROM run_slots WHERE slot_id=?",
                     (slot,)).fetchone()
    return row is not None


def acquire_lock(db: sqlite3.Connection, owner: str,
                 ttl_s: int = 1500) -> bool:
    """DB-backed collection lock (spec item 3). Stale locks (older than ttl)
    are broken by the acquirer."""
    db.execute(
        "DELETE FROM collection_lock WHERE lock_id = 1 AND "
        "(strftime('%s','now') - strftime('%s', acquired_at)) > ?",
        (ttl_s,))
    try:
        db.execute(
            "INSERT INTO collection_lock (lock_id, owner, acquired_at) "
            "VALUES (1, ?, ?)", (owner, utcnow_iso()))
        db.commit()
        return True
    except sqlite3.IntegrityError:
        db.commit()
        return False


def release_lock(db: sqlite3.Connection, owner: str) -> None:
    db.execute("DELETE FROM collection_lock WHERE lock_id = 1 AND owner = ?",
               (owner,))
    db.commit()


def lock_holder(db: sqlite3.Connection) -> dict | None:
    row = db.execute("SELECT * FROM collection_lock WHERE lock_id = 1").fetchone()
    return dict(row) if row else None


def record_lock_loss(db: sqlite3.Connection) -> int:
    db.execute(
        """INSERT INTO lock_loss (id, count, last_at) VALUES (1, 1, ?)
           ON CONFLICT (id) DO UPDATE SET count = count + 1, last_at = ?""",
        (utcnow_iso(), utcnow_iso()))
    db.commit()
    row = db.execute("SELECT count FROM lock_loss WHERE id = 1").fetchone()
    return row["count"]


def reset_lock_loss(db: sqlite3.Connection) -> None:
    db.execute("UPDATE lock_loss SET count = 0 WHERE id = 1")
    db.commit()


# ---------------------------------------------------------------- budget ledger

def ledger_insert(db: sqlite3.Connection, day: str, category: str,
                  slot: int) -> None:
    """Record one attempt: audit row in budget_ledger + bump daily_counters.
    Must be called inside a BEGIN IMMEDIATE transaction owned by the caller."""
    db.execute(
        """INSERT INTO budget_ledger (day, category, attempts, created_at)
           VALUES (?, ?, 1, ?)""",
        (day, category, utcnow_iso()))
    db.execute(
        """INSERT INTO daily_counters (day, category, n) VALUES (?, ?, 1)
           ON CONFLICT (day, category) DO UPDATE SET n = n + 1""",
        (day, category))


def count_today(db: sqlite3.Connection, day: str,
                category: str | None = None) -> int:
    if category:
        row = db.execute(
            "SELECT n FROM daily_counters WHERE day=? AND category=?",
            (day, category)).fetchone()
        return row["n"] if row else 0
    row = db.execute("SELECT COALESCE(SUM(n),0) AS s FROM daily_counters WHERE day=?",
                     (day,)).fetchone()
    return row["s"]


# ---------------------------------------------------------------- throttle

def get_throttle_state(db: sqlite3.Connection) -> dict | None:
    """The persisted throttle row, active or expired (for backoff history)."""
    row = db.execute("SELECT * FROM throttle_state WHERE id = 1").fetchone()
    return dict(row) if row else None


def get_backoff(db: sqlite3.Connection) -> dict | None:
    """The throttle row only while its backoff is still active."""
    d = get_throttle_state(db)
    if d is None or d["until_ts"] <= time.time():
        return None
    return d


def set_backoff(db: sqlite3.Connection, kind: str, until_ts: float,
                retry_after_s: float | None, detail: str = "") -> None:
    db.execute(
        """INSERT INTO throttle_state
           (id, kind, until_ts, retry_after_s, consecutive, detail, updated_at)
           VALUES (1, ?, ?, ?, 1, ?, ?)
           ON CONFLICT (id) DO UPDATE SET kind=excluded.kind,
             until_ts=excluded.until_ts, retry_after_s=excluded.retry_after_s,
             consecutive = throttle_state.consecutive + 1,
             detail=excluded.detail, updated_at=excluded.updated_at""",
        (kind, until_ts, retry_after_s, detail, utcnow_iso()))
    db.commit()
    add_health(db, "throttle", False, f"{kind}: {detail}")


def clear_backoff_if_unchanged(db: sqlite3.Connection, seen: dict) -> bool:
    """Delete the throttle row only if it is still exactly the row *seen*
    (same until_ts and updated_at), so a stale success cannot erase a newer
    throttle written concurrently. Returns True when a row was deleted."""
    cur = db.execute(
        "DELETE FROM throttle_state WHERE id = 1 AND until_ts = ? "
        "AND updated_at = ?", (seen["until_ts"], seen["updated_at"]))
    db.commit()
    return cur.rowcount > 0


def clear_backoff(db: sqlite3.Connection) -> None:
    db.execute("DELETE FROM throttle_state WHERE id = 1")
    db.commit()


# ---------------------------------------------------------------- cells / triggers / bursts (plan 04)

def ensure_cells(db: sqlite3.Connection, cells: list[dict]) -> int:
    """Insert missing cells; reactivate retired ones that are wanted again
    (e.g. after a config change); promote a background cell to sentinel
    when a sentinel anchor lands on the same (group, date, duration) --
    the unique key has no role, so without this one of them was silently
    dropped. Returns the number of cells inserted or changed."""
    n = 0
    now = utcnow_iso()
    for c in cells:
        # A sentinel takes its window's name and refresh target (a cell
        # minted before windows existed is relabelled, not duplicated). A
        # background upsert never changes an active sentinel's window.
        cur = db.execute(
            """INSERT INTO cells
               (group_id, depart_date, duration, role, weekday, created_at,
                window_name, refresh_hours)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT (group_id, depart_date, duration) DO UPDATE SET
                 role = CASE WHEN excluded.role = 'sentinel'
                               OR cells.active = 0
                             THEN excluded.role ELSE cells.role END,
                 active = 1, retired_at = NULL,
                 window_name = CASE WHEN excluded.role = 'sentinel'
                                      OR cells.active = 0
                                    THEN excluded.window_name
                                    ELSE cells.window_name END,
                 refresh_hours = CASE WHEN excluded.role = 'sentinel'
                                        OR cells.active = 0
                                      THEN excluded.refresh_hours
                                      ELSE cells.refresh_hours END
               WHERE cells.active = 0
                  OR (excluded.role = 'sentinel' AND cells.role <> 'sentinel')
                  OR (excluded.role = 'sentinel'
                      AND (cells.window_name IS NOT excluded.window_name
                           OR cells.refresh_hours IS NOT excluded.refresh_hours))""",
            (c["group_id"], c["depart_date"], c["duration"], c["role"],
             c["weekday"], now, c.get("window"), c.get("refresh_hours")))
        n += cur.rowcount
    db.commit()
    return n


def due_cells(db: sqlite3.Connection, role: str, limit: int,
              slot: int | None = None, overdue_only: bool = False,
              group_ids=None) -> list[dict]:
    """Most overdue active cells first.

    Never-checked cells come first. With ``slot`` (the current 30-minute
    slot), the rest are ordered by how far past their refresh target they
    are: hours since the last check / ``refresh_hours``. That way a 48 h
    near-term cell outranks a 72 h far-term cell of the same age. Legacy
    cells (``refresh_hours`` NULL) and calls without ``slot`` fall back to
    plain least-recently-checked rotation. ``overdue_only`` (needs
    ``slot``) keeps only named-window cells at or past their target.
    ``group_ids`` restricts to those groups BEFORE the limit, so cells of a
    watch that is no longer configured never fill a run's batch.
    """
    where, params = "role = ? AND active = 1", [role]
    if group_ids is not None:
        ids = sorted(set(group_ids))
        if not ids:
            return []
        where += f" AND group_id IN ({','.join('?' * len(ids))})"
        params += ids
    if overdue_only:
        return [dict(r) for r in db.execute(
            f"""SELECT * FROM cells
                WHERE {where} AND refresh_hours IS NOT NULL
                  AND (last_checked_slot IS NULL
                       OR (? - last_checked_slot) >= refresh_hours * 2)
                ORDER BY last_checked_slot IS NULL DESC,
                         (? - last_checked_slot) / (refresh_hours * 2.0) DESC,
                         id ASC
                LIMIT ?""", [*params, slot, slot, limit])]
    if slot is None:
        return [dict(r) for r in db.execute(
            f"""SELECT * FROM cells
                WHERE {where}
                ORDER BY last_checked_slot IS NULL DESC,
                         last_checked_slot ASC, id ASC
                LIMIT ?""", [*params, limit])]
    return [dict(r) for r in db.execute(
        f"""SELECT * FROM cells
            WHERE {where}
            ORDER BY last_checked_slot IS NULL DESC,
                     (? - last_checked_slot)
                         / (COALESCE(refresh_hours, 48) * 2.0) DESC,
                     last_checked_slot ASC, id ASC
            LIMIT ?""", [*params, slot, limit])]


QUERY_OUTCOMES = ("ok", "empty", "transport_fail", "unparseable", "error",
                  "budget_cancel", "deadline_cancel", "throttle")
QUERY_LOG_KEEP_DAYS = 120  # the gate needs 5 days; keep a season
# Completed queries that returned no usable price (the gate's unknown rate).
UNKNOWN_OUTCOMES = ("empty", "transport_fail", "unparseable", "error")


def log_query(db: sqlite3.Connection, *, slot: int | None, group_id: str,
              window_name: str | None, category: str, outcome: str,
              ceiling: int | None = None) -> None:
    if outcome not in QUERY_OUTCOMES:
        raise ValueError(f"unknown query outcome {outcome!r}")
    now = datetime.now(timezone.utc)
    db.execute(
        """INSERT INTO query_log (at, slot, group_id, window_name, category,
                                  outcome, ceiling)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (now.isoformat(timespec="seconds"), slot, group_id, window_name,
         category, outcome, ceiling))
    db.execute("DELETE FROM query_log WHERE at < ?",
               ((now - timedelta(days=QUERY_LOG_KEEP_DAYS))
                .isoformat(timespec="seconds"),))
    db.commit()


def query_outcomes(db: sqlite3.Connection, since_iso: str) -> dict:
    """Outcome counts since ``since_iso``: {outcome: n} plus
    ``completed`` (ok + unknown) and ``unknown_rate`` (unknown / completed;
    None with nothing completed). Budget/throttle cancellations are not
    completed queries, nor are deadline cancels."""
    counts = {o: 0 for o in QUERY_OUTCOMES}
    for r in db.execute(
            """SELECT outcome, COUNT(*) AS c FROM query_log WHERE at >= ?
               GROUP BY outcome""", (since_iso,)):
        counts[r["outcome"]] = r["c"]
    unknown = sum(counts[o] for o in UNKNOWN_OUTCOMES)
    completed = counts["ok"] + unknown
    counts["completed"] = completed
    counts["unknown_rate"] = (unknown / completed) if completed else None
    return counts


def _percentile(sorted_vals: list[float], p: float) -> float:
    """Nearest-rank percentile of an ascending list (p in 0..100)."""
    k = max(0, math.ceil(p / 100 * len(sorted_vals)) - 1)
    return sorted_vals[k]


def refresh_ages(db: sqlite3.Connection, group_ids, now_iso: str | None = None
                 ) -> list[dict]:
    """Refresh age per (group, window) of active named-window sentinels.

    Age = hours since the last SUCCESSFUL check; a cell that never
    succeeded counts from when it was minted (spec: never hidden). Returns
    rows {group_id, window_name, refresh_hours, cells, p50_h, p95_h,
    max_h, p95_ratio} where p95_ratio = p95 / refresh target, unrounded
    (the gate compares it; the hour fields are rounded for display).
    """
    ids = sorted(set(group_ids))
    if not ids:
        return []
    now = datetime.fromisoformat(now_iso or utcnow_iso())
    marks = ",".join("?" * len(ids))
    buckets: dict = {}
    for r in db.execute(
            f"""SELECT group_id, window_name, refresh_hours, created_at,
                       last_success_at FROM cells
                WHERE role = 'sentinel' AND active = 1
                  AND refresh_hours IS NOT NULL AND group_id IN ({marks})""",
            ids):
        ref = r["last_success_at"] or r["created_at"]
        seen = datetime.fromisoformat(ref)
        if seen.tzinfo is None:
            seen = seen.replace(tzinfo=timezone.utc)
        age_h = max(0.0, (now - seen).total_seconds() / 3600)
        key = (r["group_id"], r["window_name"], r["refresh_hours"])
        buckets.setdefault(key, []).append(age_h)
    out = []
    for (gid, win, refresh), ages in sorted(buckets.items()):
        ages.sort()
        p95 = _percentile(ages, 95)
        out.append({"group_id": gid, "window_name": win,
                    "refresh_hours": refresh, "cells": len(ages),
                    "p50_h": round(_percentile(ages, 50), 1),
                    "p95_h": round(p95, 1), "max_h": round(ages[-1], 1),
                    "p95_ratio": p95 / refresh})
    return out


def overdue_sentinels(db: sqlite3.Connection, slot: int, group_ids) -> int:
    """Active named-window sentinels of ``group_ids`` at or past their
    refresh target (or never checked) at ``slot``. Only groups this run can
    check count, so a removed watch's leftover cells never block bursts.
    Legacy cells (no refresh target) never count. Bursts wait while this is
    non-zero (spec: surplus only)."""
    ids = sorted(set(group_ids))
    if not ids:
        return 0
    marks = ",".join("?" * len(ids))
    return db.execute(
        f"""SELECT COUNT(*) AS c FROM cells
            WHERE role = 'sentinel' AND active = 1
              AND group_id IN ({marks})
              AND refresh_hours IS NOT NULL
              AND (last_checked_slot IS NULL
                   OR (? - last_checked_slot) >= refresh_hours * 2)""",
        [*ids, slot]).fetchone()["c"]


def record_cell_check(db: sqlite3.Connection, cell_id: int, slot: int,
                      success: bool, price_usd: float | None = None) -> None:
    if success:
        db.execute(
            """UPDATE cells SET last_checked_slot=?, last_success_at=?,
               last_price_usd=? WHERE id=?""",
            (slot, utcnow_iso(), price_usd, cell_id))
    else:
        db.execute("UPDATE cells SET last_checked_slot=? WHERE id=?",
                   (slot, cell_id))
    db.commit()


def retire_cell(db: sqlite3.Connection, cell_id: int) -> None:
    db.execute("UPDATE cells SET active=0, retired_at=? WHERE id=?",
               (utcnow_iso(), cell_id))
    db.commit()


def queue_trigger(db: sqlite3.Connection, *, kind: str, group_id: str,
                  depart_week: str, depart_date: str, duration: int,
                  price_usd: float, fingerprint: str | None,
                  slot: int, window_name: str | None = None) -> int | None:
    """Queue a burst trigger. Deduped per (kind, group, week) per UTC day.
    ``window_name`` is the window the triggering fare came from."""
    cur = db.execute(
        """INSERT OR IGNORE INTO triggers
           (kind, group_id, depart_week, depart_date, duration, price_usd,
            fingerprint, day, created_slot, created_at, status, window_name)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?)""",
        (kind, group_id, depart_week, depart_date, duration, price_usd,
         fingerprint, today_str(), slot, utcnow_iso(), window_name))
    db.commit()
    return cur.lastrowid or None


def due_triggers(db: sqlite3.Connection, slot: int) -> list[dict]:
    """Triggers ready for (re)execution.

    - status='queued', younger than 24h, with no burst for (group, week) in
      the last 24h (spec item 4 dedup);
    - OR status='burst_active' with unexpired, still-active burst progress
      (v3 group 4: a partial burst resumes in later slots until it reaches
      60 attempts, finishes all steps, or expires -- never terminal after
      one partial allowance).
    """
    day_ago = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat(timespec="seconds")
    now_iso = utcnow_iso()
    return [dict(r) for r in db.execute(
        """SELECT t.* FROM triggers t
           LEFT JOIN burst_progress p ON p.trigger_id = t.id
           WHERE t.created_at >= ?
             AND ((t.status = 'queued'
                   AND NOT EXISTS (
                         SELECT 1 FROM bursts b
                         WHERE b.group_id = t.group_id
                           AND b.depart_week = t.depart_week
                           AND b.created_at >= ?))
                  OR (t.status = 'burst_active'
                      AND p.status = 'active'
                      AND p.expires_at > ?))
           ORDER BY t.created_at ASC""", (day_ago, day_ago, now_iso))]


def mark_trigger(db: sqlite3.Connection, trigger_id: int, status: str) -> None:
    db.execute("UPDATE triggers SET status=? WHERE id=?", (status, trigger_id))
    db.commit()


def record_burst(db: sqlite3.Connection, *, trigger_id: int, group_id: str,
                 depart_week: str, attempts: int, status: str) -> None:
    db.execute(
        """INSERT INTO bursts (trigger_id, group_id, depart_week, attempts,
           status, created_at) VALUES (?, ?, ?, ?, ?, ?)""",
        (trigger_id, group_id, depart_week, attempts, status, utcnow_iso()))
    db.commit()


# ---------------------------------------------------------------- burst progress (v3 group 4: resumable bursts)

def init_burst_progress(db: sqlite3.Connection, trigger_id: int,
                        group_id: str, depart_week: str,
                        expires_at: str) -> dict:
    """Create the resumable progress row for a trigger's first burst
    execution. attempts_used=0, next_step=0 (revalidate), status='active'."""
    db.execute(
        """INSERT OR IGNORE INTO burst_progress
           (trigger_id, group_id, depart_week, attempts_used, next_step,
            status, expires_at, updated_at)
           VALUES (?, ?, ?, 0, 0, 'active', ?, ?)""",
        (trigger_id, group_id, depart_week, expires_at, utcnow_iso()))
    db.commit()
    return get_burst_progress(db, trigger_id)


def get_burst_progress(db: sqlite3.Connection,
                       trigger_id: int) -> dict | None:
    row = db.execute("SELECT * FROM burst_progress WHERE trigger_id=?",
                     (trigger_id,)).fetchone()
    return dict(row) if row else None


def save_burst_progress(db: sqlite3.Connection, trigger_id: int, *,
                        next_step: int | None = None,
                        attempts_used: int | None = None,
                        status: str | None = None,
                        revalidated: bool = False) -> dict:
    """Persist burst progress (called after every burst step so a later
    slot -- or a BudgetExhausted/deadline -- resumes exactly where this
    execution stopped). Returns the updated row."""
    sets = ["updated_at=?"]
    vals: list = [utcnow_iso()]
    if next_step is not None:
        sets.append("next_step=?")
        vals.append(next_step)
    if attempts_used is not None:
        sets.append("attempts_used=?")
        vals.append(attempts_used)
    if status is not None:
        sets.append("status=?")
        vals.append(status)
    if revalidated:
        sets.append("revalidated_at=?")
        vals.append(vals[0])
    vals.append(trigger_id)
    db.execute(f"UPDATE burst_progress SET {', '.join(sets)} "
               f"WHERE trigger_id=?", vals)
    db.commit()
    return get_burst_progress(db, trigger_id)
