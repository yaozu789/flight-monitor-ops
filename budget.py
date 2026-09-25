"""Atomic HTTP-attempt budget guard (stage 01 rewrite).

Replaces the v1 approach of counting ``.search()`` calls: every real HTTP
attempt is reserved through ``BudgetGuard.reserve()`` by the transport
wrapper in ``transport.py`` (one fli round-trip search costs 1 + top_n page
fetches, so search-call counting undercounted real traffic).

The daily ceiling is enforced atomically: ``reserve()`` runs
check-and-insert inside a single ``BEGIN IMMEDIATE`` transaction, so N
threads hammering the same ceiling from separate connections can never
overshoot it.

``import store`` below resolves to the project's ``store.py`` (the shared
DB contract: budget ledger, run slots, throttle state).
"""
from __future__ import annotations

import threading

import store  # shared DB contract (project store.py)

DAILY_CEILING = 150
PER_RUN_MAX = 80
CATEGORY_CAPS = {
    "sentinel": 54,
    "verify": 24,
    "retry_reserve": 12,
    "background": 48,
    "burst": 60,
    "manual": 150,
}


class BudgetExhausted(Exception):
    """Raised when a reserve would exceed a budget. ``scope`` is one of
    "daily", "category", "per_run"."""

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

    ``db`` is a sqlite3 connection to the shared store (``transport.py``
    reads ``guard._db`` to persist throttle backoff).
    """

    def __init__(self, db, *, per_run_max=PER_RUN_MAX,
                 daily_ceiling=DAILY_CEILING, category_caps=None, day=None):
        self._db = db
        self._per_run_max = per_run_max
        self._daily_ceiling = daily_ceiling
        self._category_caps = (dict(CATEGORY_CAPS) if category_caps is None
                               else dict(category_caps))
        self._day = day or store.today_str()
        self._run_lock = threading.Lock()
        self._run_total = 0
        self._run_by_category: dict[str, int] = {}

    # -- run-local accounting -------------------------------------------

    def reset_run(self) -> None:
        """Zero the per-run counters (DB ledger is untouched)."""
        with self._run_lock:
            self._run_total = 0
            self._run_by_category = {}

    def spent_run(self, category=None) -> int:
        """Attempts reserved by this guard instance since the last reset."""
        with self._run_lock:
            if category is None:
                return self._run_total
            return self._run_by_category.get(category, 0)

    def spent_today(self, category=None) -> int:
        """Attempts recorded in the DB ledger for the guard's day."""
        return store.count_today(self._db, self._day, category)

    # -- the atomic reserve ---------------------------------------------

    def reserve(self, category: str) -> None:
        """Atomically check budgets and record one attempt for *category*.

        Raises BudgetExhausted (nothing recorded, nothing sent) when the
        per-run, daily, or category budget is already spent.
        """
        # Per-run check BEFORE the DB transaction.
        with self._run_lock:
            run_spent = self._run_total
        if run_spent >= self._per_run_max:
            raise BudgetExhausted("per_run", category, self._per_run_max,
                                  run_spent)

        db = self._db
        day = self._day
        # Never nest: finish any stray open transaction first so the
        # BEGIN IMMEDIATE below always starts a fresh one.
        if db.in_transaction:
            db.commit()
        db.execute("BEGIN IMMEDIATE")
        try:
            total = store.count_today(db, day)
            if total >= self._daily_ceiling:
                raise BudgetExhausted("daily", category, self._daily_ceiling,
                                      total)
            cap = self._category_caps.get(category)
            if cap is not None:
                cat_spent = store.count_today(db, day, category)
                if cat_spent >= cap:
                    raise BudgetExhausted("category", category, cap, cat_spent)
            store.ledger_insert(db, day, category, store.slot_id())
            db.commit()
        except BaseException:
            try:
                db.rollback()
            except BaseException:
                pass
            raise

        # Only bump run-local counters after the DB commit succeeded.
        with self._run_lock:
            self._run_total += 1
            self._run_by_category[category] = \
                self._run_by_category.get(category, 0) + 1
