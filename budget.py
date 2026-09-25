"""Atomic HTTP-attempt budget guard (v3).

Replaces the v1 approach of counting ``.search()`` calls: every real HTTP
attempt is reserved through ``BudgetGuard.reserve()`` by the transport
wrapper in ``transport.py`` (one fli round-trip search costs 1 + top_n page
fetches, so search-call counting undercounted real traffic).

The daily ceiling is enforced atomically: ``reserve()`` runs
check-and-insert inside a single ``BEGIN IMMEDIATE`` transaction on the
guard's OWN sqlite connection, so N threads hammering the same ceiling
from separate connections can never overshoot it -- and the guard never
commits or rolls back the caller's open transaction (v3 item 3).

``BudgetExhausted`` is a ``BaseException`` (v3 item 2): fli's
``Client.get`` wraps every ``Exception`` in ``_wrap_request_error`` and
tenacity retries ``Exception`` subclasses, which turned a clean ceiling
stop into silent retried "unknowns". As a ``BaseException`` it passes
through both untouched. Callers (poller/run/sentinel) catch it explicitly.

The per-run allowance (v3 item 5) is enforced on EVERY send through the
guard, not just before a multi-attempt query: the orchestrator sets it via
``set_run_allowance()`` (e.g. the sentinel pacer), and ``reserve()`` refuses
once the run-local total reaches it -- a top_n=3 query against a 2-attempt
allowance stops at 2 instead of overshooting to 4. The check is atomic with
a provisional bump, so concurrent sends cannot overshoot either.

Verify reserve (v3 item 6): the last ``VERIFY_RESERVE`` daily attempts are
held for the verifier. A non-"verify" reserve is refused when it would dip
into that headroom, even though daily budget remains.

``import store`` below resolves to the project's ``store.py`` (the shared
DB contract: budget ledger, run slots, throttle state).
"""
from __future__ import annotations

import threading
from pathlib import Path

import store  # shared DB contract (project store.py)

DAILY_CEILING = 150
PER_RUN_MAX = 80
VERIFY_RESERVE = 24
CATEGORY_CAPS = {
    "sentinel": 54,
    "verify": 24,
    "retry_reserve": 12,
    "background": 48,
    "burst": 60,
    "manual": 150,
}


class BudgetExhausted(BaseException):
    """Raised when a reserve would exceed a budget. ``scope`` is one of
    "daily", "category", "per_run", "verify_reserve".

    Deliberately a BaseException, NOT an Exception (v3 item 2): fli's
    Client.get catches ``except Exception`` and wraps it via
    ``_wrap_request_error``, and its tenacity retry decorator retries
    Exception subclasses. A BaseException passes through both, so a ceiling
    hit is a clean recorded stop -- never a retried, wrapped "unknown".
    Poller/run/sentinel catch it explicitly."""

    def __init__(self, scope: str, category: str | None = None,
                 limit: int = 0, spent: int = 0):
        super().__init__(
            "budget exhausted: scope=%s category=%s limit=%d spent=%d"
            % (scope, category, limit, spent))
        self.scope = scope
        self.category = category
        self.limit = limit
        self.spent = spent


class BudgetGuard:
    """Guards HTTP attempts against daily / per-category / per-run budgets.

    ``db`` is a sqlite3 connection to the shared store (or a path to the DB
    file). The guard opens its OWN connection for the reserve transaction
    and for throttle persistence (v3 item 3); ``transport.py`` reaches those
    through ``guard.persist_throttle()`` / ``guard.read_backoff()``, so a
    caller holding an open transaction is never disturbed.
    """

    def __init__(self, db, *, per_run_max=PER_RUN_MAX,
                 daily_ceiling=DAILY_CEILING, category_caps=None, day=None):
        self._db = db
        self._db_path = self._resolve_db_path(db)
        self._per_run_max = per_run_max
        self._daily_ceiling = daily_ceiling
        self._category_caps = (dict(CATEGORY_CAPS) if category_caps is None
                               else dict(category_caps))
        self._day = day or store.today_str()
        self._run_lock = threading.Lock()
        self._reserve_lock = threading.Lock()
        self._run_total = 0
        self._run_by_category: dict[str, int] = {}
        self._run_allowance = per_run_max
        self._reserve_db = None  # lazy private connection for reserve()
        self._reserve_owns_conn = False

    # -- connection handling ------------------------------------------

    @staticmethod
    def _resolve_db_path(db) -> str | None:
        """Filesystem path of the DB behind *db* (or None for :memory:)."""
        if isinstance(db, (str, Path)):
            return str(db)
        try:
            row = db.execute("PRAGMA database_list").fetchone()
        except Exception:  # noqa: BLE001 - best effort
            return None
        if not row:
            return None
        # Row is (seq, name, file); sqlite3.Row or tuple.
        try:
            path = row["file"]
        except (TypeError, KeyError, IndexError):
            path = row[2]
        return path or None

    def _reserve_conn(self):
        """The guard's private connection for the reserve transaction.

        Never the caller's connection: the caller's open transaction is
        left untouched (v3 item 3). Falls back to the shared connection
        only for :memory: databases, where a second connection would see a
        different database.
        """
        if self._reserve_db is None:
            if self._db_path:
                conn = store.get_db(self._db_path)
                conn.execute("PRAGMA busy_timeout=2000;")
                self._reserve_db = conn
                self._reserve_owns_conn = True
            else:
                self._reserve_db = self._db
                self._reserve_owns_conn = False
        return self._reserve_db

    # -- run-local accounting -------------------------------------------

    def reset_run(self) -> None:
        """Zero the per-run counters (DB ledger is untouched) and restore
        the per-run allowance to the default (per_run_max)."""
        with self._run_lock:
            self._run_total = 0
            self._run_by_category = {}
            self._run_allowance = self._per_run_max

    def set_run_allowance(self, n: int) -> None:
        """Cap enforced on EVERY send through the guard (v3 item 5).

        The orchestrator (e.g. the sentinel pacer) calls this per phase;
        ``reserve()`` refuses once the run-local total reaches *n*.
        """
        with self._run_lock:
            self._run_allowance = max(0, int(n))

    def run_allowance(self) -> int:
        with self._run_lock:
            return self._run_allowance

    def spent_run(self, category=None) -> int:
        """Attempts reserved by this guard instance since the last reset."""
        with self._run_lock:
            if category is None:
                return self._run_total
            return self._run_by_category.get(category, 0)

    def spent_today(self, category=None) -> int:
        """Attempts recorded in the DB ledger for the guard's day."""
        return store.count_today(self._db, self._day, category)

    # -- throttle persistence (guard-owned write) ------------------------

    def persist_throttle(self, kind: str, until_ts: float,
                         retry_after_s: float | None, detail: str = "") -> None:
        """Persist a throttle backoff on the guard's private connection."""
        with self._reserve_lock:
            store.set_backoff(self._reserve_conn(), kind, until_ts,
                              retry_after_s, detail)

    def read_backoff(self) -> dict | None:
        """Read the persisted backoff (for consecutive-count computation)."""
        with self._reserve_lock:
            return store.get_backoff(self._reserve_conn())

    # -- the atomic reserve ---------------------------------------------

    def reserve(self, category: str) -> None:
        """Atomically check budgets and record one attempt for *category*.

        The per-run allowance is checked on EVERY send (v3 item 5), atomically
        with a provisional bump so concurrent sends cannot overshoot. The DB
        check-and-insert runs on the guard's OWN connection inside a single
        ``BEGIN IMMEDIATE`` (v3 item 3): the caller's open transaction -- if
        any -- is never committed or rolled back.

        Raises BudgetExhausted (nothing recorded, nothing sent) when the
        per-run allowance, daily ceiling, verify reserve, or category cap
        is already spent.
        """
        # Per-run allowance check, atomic with a provisional bump: concurrent
        # sends through one guard cannot overshoot the allowance.
        with self._run_lock:
            if self._run_total >= self._run_allowance:
                raise BudgetExhausted("per_run", category,
                                      self._run_allowance, self._run_total)
            self._run_total += 1
            self._run_by_category[category] = \
                self._run_by_category.get(category, 0) + 1
        try:
            self._reserve_in_db(category)
        except BaseException:
            # Roll back the provisional bump; the DB recorded nothing.
            with self._run_lock:
                self._run_total -= 1
                left = self._run_by_category.get(category, 0) - 1
                if left <= 0:
                    self._run_by_category.pop(category, None)
                else:
                    self._run_by_category[category] = left
            raise

    def _reserve_in_db(self, category: str) -> None:
        """Check-and-insert on the private connection (v3 item 3).

        Short BEGIN IMMEDIATE ... COMMIT. The caller's connection is never
        touched: no commit, no rollback of a caller-held transaction.
        """
        db = self._reserve_conn()
        day = self._day
        with self._reserve_lock:
            db.execute("BEGIN IMMEDIATE")
            try:
                total = store.count_today(db, day)
                if total >= self._daily_ceiling:
                    raise BudgetExhausted("daily", category,
                                          self._daily_ceiling, total)
                # Verify reserve (v3 item 6): non-"verify" categories must
                # not dip into the last VERIFY_RESERVE unspent attempts.
                if category != "verify":
                    verify_spent = store.count_today(db, day, "verify")
                    headroom = max(0, VERIFY_RESERVE - verify_spent)
                    # Allowed only if daily_remaining - 1 >= headroom.
                    if (self._daily_ceiling - total) - 1 < headroom:
                        raise BudgetExhausted("verify_reserve", category,
                                              headroom, verify_spent)
                cap = self._category_caps.get(category)
                if cap is not None:
                    cat_spent = store.count_today(db, day, category)
                    if cat_spent >= cap:
                        raise BudgetExhausted("category", category, cap,
                                              cat_spent)
                store.ledger_insert(db, day, category, store.slot_id())
                db.commit()
            except BaseException:
                try:
                    db.rollback()
                except BaseException:
                    pass
                raise
