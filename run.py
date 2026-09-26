#!/usr/bin/env python3
"""Cheap Flight Monitor — CLI entrypoint (v2 stage 4: sentinel + bursts).

Stage-04 changes vs stage 03 (plan 04, spec item 12):
- Rolling sentinel tripwires replace the fixed per-pair check loop: 27
  weekly anchors per group across the 3-9 month window, re-checked in
  least-recently-checked rotation. The canary's reference-query role is
  folded into the sentinel schedule (the manual `canary` command remains
  for diagnostics).
- Collection is paced across the 48 daily slots (adaptive per-run attempt
  cap), with priority: bursts > sentinels > background, and a reserved
  verification budget. Ready handoffs are drained BEFORE any collection.
- Verification traffic bills to "verify", never "sentinel".
- Burst triggers: a newly under-cap deal, or a >=20% week-over-week drop on
  a recently successful cell. Bursts are deduped to 1/(group, week)/24h and
  revalidate the trigger before spending the burst budget.

Budget reality (spec item 1): one round-trip narrow query costs 1 + top_n
HTTP attempts, so a sentinel cell (top_n=1) costs 2 attempts; 54 sentinel
attempts/day = 27 cells/day, i.e. each anchor re-checks roughly every 2 days.
See sentinel.py for the full arithmetic.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import traceback
from datetime import date, datetime, timezone
from pathlib import Path

BASE = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE))

import yaml  # noqa: E402

import heartbeat  # noqa: E402
import sentinel  # noqa: E402
import transport  # noqa: E402
from budget import PER_RUN_MAX, BudgetExhausted, BudgetGuard  # noqa: E402
from canary import run_canary  # noqa: E402
from outbox import (  # noqa: E402
    RELAYABLE_ACTIONS,
    drain_pending,
    enqueue_from_confirmation,
    recheck_retries,
)
from poller import narrow_query  # noqa: E402
from scorer import score_new_observations  # noqa: E402
from store import (  # noqa: E402
    DeadlineExceeded,
    acquire_lock,
    add_alert,
    add_health,
    claim_slot,
    deadline_streak,
    finish_slot,
    get_backoff,
    init_db,
    outbox_get,
    outbox_list,
    prepare_handoff,
    reprepare_retries,
    record_lock_loss,
    release_lock,
    reset_lock_loss,
    set_outbox_user_confirmed,
    slot_id,
    slot_start_ts,
    sync_watches,
    update_outbox_state,
    utcnow_iso,
)
from transport import ThrottleDetected  # noqa: E402
from verifier import verify  # noqa: E402

DB_PATH = BASE / "data" / "monitor.db"

LOCK_TTL_S = 1500

# Run deadline, deliberately shorter than the lock TTL: a run that exceeds
# it stops in a controlled way (finish_slot("deadline"), lock released)
# instead of wedging the lock until TTL expiry. Checked at loop tops in
# collection code via the deadline_fn passed to fn.
RUN_DEADLINE_S = 1200
# A deadline stop is a designed bound, so it pings success -- unless it
# keeps happening: this many consecutive deadline slots ping /fail instead.
DEADLINE_FAIL_STREAK = 3

# Only scheduled runs of these commands ping Healthchecks.
PING_COMMANDS = {"check", "broad"}


RELAY_INSTRUCTION = (
    "Relay each message below VERBATIM to the user via the normal push. "
    "Do not paraphrase, do not add claims, never use the word 'bookable'. "
    "After relaying each, record it with: "
    "python run.py outbox-mark --alert-id <alert_id> --event <event> "
    "--state relay_attempted_unconfirmed. "
    "After the USER confirms a deal, finalize it (human-driven only) with: "
    "python run.py outbox-confirm --alert-id <alert_id> --event <event> "
    "--by <your name>."
)


RELAYABLE_STATES = ("handoff_prepared", "relay_attempted_unconfirmed")


def _handoff_payload(db, slot, alert_ids, **extra) -> dict:
    """The agent-facing relay batch for *alert_ids* (duplicates dropped)."""
    seen: list[str] = []
    for aid in alert_ids:
        if aid not in seen:
            seen.append(aid)
    # Only rows actually handed off: a row whose preparation failed is still
    # pending and must not be relayed (outbox-mark would reject it and the
    # next drain would relay it again, untracked).
    batch = [r for r in (outbox_get(db, aid) for aid in seen)
             if r is not None and r["state"] in RELAYABLE_STATES]
    return {
        "slot_id": slot,
        "alerts": [{"alert_id": r["alert_id"], "event": r["event_no"],
                    "state": r["state"], "price_usd": r["price_usd"],
                    # The user-visible text carries the alert id and event,
                    # so a confirmation names exactly the deal they saw.
                    "message": (f"{r['message_text']}\n"
                                f"Alert: {r['alert_id']} event {r['event_no']}")}
                   for r in batch],
        **extra,
        "agent_instruction": RELAY_INSTRUCTION,
    }


def _noop(reason: str, **extra) -> int:
    """Emit a machine-readable no-op summary. Exit 0: a bounded no-op is a
    successful run (the scheduler stays quiet)."""
    print(json.dumps({"status": "noop", "reason": reason, **extra}, indent=2))
    return 0


def _ping_ok(args, cmd_name: str, db) -> None:
    """Success ping: only for scheduled check/broad cycles."""
    if getattr(args, "scheduled", False) and cmd_name in PING_COMMANDS:
        heartbeat.ping_success(db)


def _ping_fail(args, cmd_name: str, db) -> None:
    """Failure ping: only for scheduled check/broad cycles."""
    if getattr(args, "scheduled", False) and cmd_name in PING_COMMANDS:
        heartbeat.ping_fail(db)


def _check_deadline(deadline_fn) -> None:
    """Raise DeadlineExceeded when the run deadline has passed.

    deadline_fn is None outside run_guarded (direct calls, manual
    commands) -- then there is no deadline to enforce.
    """
    if deadline_fn is not None and deadline_fn():
        raise DeadlineExceeded(
            f"run deadline ({RUN_DEADLINE_S}s) exceeded")


def _ids(rows) -> list[str]:
    return [r["alert_id"] for r in rows or []]


def _stop_handoff(exc, db=None, slot=None, drained=None) -> dict:
    """Extra no-op fields carrying the relay batch of a stopped run: the
    batch cmd_check attached to the exception, or -- when there is none
    (the stop came before cmd_check could attach it, or the exception
    rejected the attribute) -- the alerts run_guarded drained."""
    payload = getattr(exc, "handoff", None)
    if payload is None and db is not None and drained:
        try:
            payload = _handoff_payload(db, slot, _ids(drained))
        except Exception:  # noqa: BLE001 - never mask the stop
            payload = None
    return {"handoff": payload} if payload and payload["alerts"] else {}


def _emit_stop_handoff(exc, db=None, slot=None, drained=None) -> None:
    """Print the relay batch of a stop that does not go through _noop."""
    extra = _stop_handoff(exc, db, slot, drained)
    if extra:
        print(json.dumps({"status": "stopped", **extra}, indent=2))


def run_guarded(cmd_name: str, args, db, watches, guard: BudgetGuard, fn) -> int:
    """Overlap lock + drain-first + throttle backoff + slot idempotency +
    budget handling + run deadline + heartbeat semantics (spec item 11).

    Order after lock acquisition is deliberate:

    1. drain pending alerts (no network) FIRST -- stranded handoffs are
       prepared even when a persisted backoff or an already-claimed slot
       short-circuits the run below;
    2. persisted throttle backoff -> legitimate no-op;
    3. slot claim -> legitimate no-op when check/broad already ran;
    4. the collection fn, bounded by the run deadline.

    Heartbeat: success ONLY after a scheduled check/broad cycle finishes
    (including legitimate no-ops and controlled deadline stops); /fail on
    crash, SystemExit, repeated lock contention, or a non-zero fn return.
    Manual/unscheduled runs never ping.
    """
    if getattr(args, "scheduled", False):
        ok, reason = heartbeat.validate_schedule_env()
        if not ok:
            print(json.dumps({"status": "refused", "reason": reason}, indent=2))
            return 2
    owner = f"{cmd_name}-{os.getpid()}"
    if not acquire_lock(db, owner, ttl_s=LOCK_TTL_S):
        losses = record_lock_loss(db)
        if losses >= 3:
            add_health(db, "overlap_lock", False,
                       f"{losses} consecutive lock losses; failing the check")
            _ping_fail(args, cmd_name, db)
            return 1
        return _noop("overlap_lock", losses=losses,
                     detail="another collection run holds the lock; exiting")
    try:
        reset_lock_loss(db)
        slot = slot_id()
        # Drain FIRST (no network): must run before the backoff / slot
        # early-returns below so stranded handoffs are never stuck behind a
        # skipped collection.
        drained = drain_pending(db, slot)
        bo = get_backoff(db)
        if bo is not None:
            add_health(db, "throttle_backoff", True,
                       f"not collecting: {bo['kind']} backoff until "
                       f"{bo['until_ts']:.0f}")
            rc = _noop("throttle_backoff", kind=bo["kind"],
                       retry_at=bo["until_ts"],
                       handoff=_handoff_payload(db, slot, _ids(drained)))
            _ping_ok(args, cmd_name, db)
            return rc
        if not claim_slot(db, slot, "collect"):
            rc = _noop("slot_already_completed", slot_id=slot,
                       detail="check and broad share one collection per slot",
                       handoff=_handoff_payload(db, slot, _ids(drained)))
            _ping_ok(args, cmd_name, db)
            return rc
        # Run deadline, shorter than the lock TTL: on exceed the run stops
        # in a controlled way instead of wedging until TTL expiry.
        deadline_at = time.monotonic() + RUN_DEADLINE_S

        def _deadline_exceeded() -> bool:
            return time.monotonic() >= deadline_at

        # The deadline is also enforced before every HTTP send, so one long
        # search cannot run past it (a stale lock could then be broken while
        # this run is still sending).
        transport.set_deadline_fn(_deadline_exceeded)
        try:
            rc = fn(args, db, watches, guard,
                    drained=drained, deadline_fn=_deadline_exceeded,
                    slot=slot)
        except SystemExit as e:
            # An explicit exit from fn is a failure signal, not a success:
            # /fail, then let it propagate.
            _emit_stop_handoff(e, db, slot, drained)
            _ping_fail(args, cmd_name, db)
            raise
        except ThrottleDetected as e:  # BaseException: caught explicitly
            finish_slot(db, slot, "throttled")
            add_health(db, "throttle", False,
                       f"throttle mid-run ({e.kind}); backoff persisted")
            rc = _noop("throttle_backoff", kind=e.kind,
                       retry_after_s=e.retry_after_s, slot_id=slot,
                       **_stop_handoff(e, db, slot, drained))
            _ping_ok(args, cmd_name, db)
            return rc
        except BudgetExhausted as e:  # BaseException: caught explicitly
            finish_slot(db, slot, "budget_exhausted")
            add_health(db, "budget", True,
                       f"budget exhausted, collection stopped: {e}")
            rc = _noop("budget_exhausted", slot_id=slot,
                       spent_today=guard.spent_today(),
                       spent_run=guard.spent_run(), detail=str(e),
                       **_stop_handoff(e, db, slot, drained))
            _ping_ok(args, cmd_name, db)
            return rc
        except DeadlineExceeded as e:
            # Controlled stop, NOT a failure: the slot is finished, the
            # lock is released by the finally below, and the cycle pings
            # success -- never /fail.
            finish_slot(db, slot, "deadline")
            streak = deadline_streak(db, max_gap=cadence_slots(watches))
            if streak >= DEADLINE_FAIL_STREAK:
                add_health(db, "run_deadline", False,
                           f"{streak} consecutive deadline stops: {e}")
                _emit_stop_handoff(e, db, slot, drained)
                _ping_fail(args, cmd_name, db)
                return 1
            add_health(db, "run_deadline", True,
                       f"controlled stop after {RUN_DEADLINE_S}s: {e}")
            rc = _noop("deadline", slot_id=slot, detail=str(e),
                       **_stop_handoff(e, db, slot, drained))
            _ping_ok(args, cmd_name, db)
            return rc
        except BaseException as e:  # noqa: BLE001 - crash: /fail, never green
            finish_slot(db, slot, "crashed")
            add_health(db, "crash", False, f"{type(e).__name__}: {e}")
            traceback.print_exc()
            _emit_stop_handoff(e, db, slot, drained)
            _ping_fail(args, cmd_name, db)
            return 1
        if rc != 0:
            # fn reported failure without raising: not a success, never
            # green.
            finish_slot(db, slot, "completed")
            _ping_fail(args, cmd_name, db)
            return rc
        finish_slot(db, slot, "completed")
        _ping_ok(args, cmd_name, db)
        return rc
    finally:
        transport.set_deadline_fn(None)
        release_lock(db, owner)


def run_manual(cmd_name: str, args, db, watches, guard: BudgetGuard, fn) -> int:
    """Manual collecting commands (narrow, canary): take the shared collection
    lock (item 3) and count against the budget, but never claim a scheduler
    slot and NEVER ping (spec item 11)."""
    owner = f"{cmd_name}-{os.getpid()}"
    if not acquire_lock(db, owner, ttl_s=LOCK_TTL_S):
        return _noop("overlap_lock",
                     detail="another collection run holds the lock; exiting")
    try:
        reset_lock_loss(db)
        return fn(args, db, watches, guard)
    except BudgetExhausted as e:
        return _noop("budget_exhausted", detail=str(e))
    finally:
        release_lock(db, owner)


def load_config() -> tuple[dict, list[dict]]:
    with open(BASE / "watches.yaml") as f:
        cfg = yaml.safe_load(f)
    defaults = cfg.get("defaults", {})
    watches = []
    for w in cfg.get("watches", []):
        merged = {**defaults, **w}
        watches.append(merged)
    return defaults, watches


def cadence_slots(watches: list[dict]) -> int:
    """Configured run cadence in 30-minute slots (watches.yaml
    ``run_every_minutes``, default 30). Pacing and the deadline streak
    follow it; the scheduler itself is configured on Muse to match."""
    minutes = 30
    for w in watches:
        if w.get("run_every_minutes"):
            minutes = int(w["run_every_minutes"])
            break
    if minutes < 30 or minutes % 30:
        raise ValueError(
            f"run_every_minutes must be a multiple of 30, got {minutes}")
    return minutes // 30


def group_cfg_for(w: dict) -> dict:
    """Collection config for one watch: airports plus the departure window
    (``window_months``, 30-day months) and trip lengths
    (``trip_length_days``) from watches.yaml."""
    lo_mo, hi_mo = w.get("window_months", [3, 9])
    lo_len, hi_len = w.get("trip_length_days", [5, 14])
    return {"origins": w["origins"], "destinations": w["destinations"],
            "window_days": (int(lo_mo) * 30, int(hi_mo) * 30),
            "durations": (int(lo_len), int(hi_len))}


def get_watch(watches: list[dict], watch_id: str) -> dict:
    for w in watches:
        if w["id"] == watch_id:
            return w
    raise SystemExit(f"unknown watch: {watch_id} (have: {[w['id'] for w in watches]})")


def _add_scheduled(p) -> None:
    p.add_argument("--scheduled", action="store_true",
                   help="mark this run as the scheduled (cron) invocation; "
                        "enables the Healthchecks ping and the env gate")


def cmd_broad(args, db, watches, guard, *, drained=None, deadline_fn=None,
              slot=None):
    """Disabled. A flex-date sweep sends one request per date (up to 93 per
    chunk, 180+ for the 3-9 month window), which exceeds the whole daily
    background budget (48) and would stop mid-sweep with nothing saved.
    The sentinel/background grid covers the window instead. main() answers
    before the lock and slot claim, so this can never make a same-slot
    check skip collection."""
    payload = {"status": "noop", "reason": "broad_scan_disabled",
               "detail": "a sweep exceeds the daily background budget; the "
                         "sentinel/background grid covers the window instead",
               "watch": getattr(args, "watch", None)}
    if drained:
        payload["handoff"] = _handoff_payload(
            db, slot if slot is not None else slot_id(), _ids(drained))
    print(json.dumps(payload, indent=2))
    return 0


def cmd_narrow(args, db, watches, guard):
    w = get_watch(watches, args.watch)
    itineraries = []
    for o in w["origins"]:
        for d in w["destinations"]:
            res = narrow_query(o, d, args.depart, args.return_, w,
                               attempt_category="manual")
            if res:
                itineraries.extend(res)
    itineraries.sort(key=lambda x: x["price_usd"])
    print(json.dumps({"watch": w["id"], "depart": args.depart, "return": args.return_,
                      "unknown": not itineraries, "itineraries": itineraries[:10]}, indent=2))
    return 0


def cmd_canary(args, db, watches, guard):
    w = watches[0]  # canary uses its own fixed reference; cfg shape only
    ok, detail = run_canary(db, w)
    print(json.dumps({"ok": ok, "detail": detail}))
    return 0 if ok else 2


def _note(db, kind: str, detail: str) -> None:
    """Best-effort health note during stop cleanup: never raises."""
    try:
        add_health(db, kind, False, detail)
    except Exception:  # noqa: BLE001
        pass


def _finish_verified(db, slot, run_start, verified_deals, new_ids,
                     strict=False) -> int:
    """Steps 7-8: queue burst triggers for verified deals, then prepare
    their handoffs. Each step is attempted independently; with
    strict=False (a stop is already propagating) failures are recorded in
    health instead of raised, so they cannot mask the original stop.
    Returns the number of triggers queued."""
    n_triggers = 0
    try:
        n_triggers = sentinel.detect_under_cap_triggers(
            db, verified_deals, run_start, slot)
    except Exception as e:  # noqa: BLE001
        if strict:
            raise
        _note(db, "burst_triggers", f"{type(e).__name__}: {e}")
    if new_ids:
        try:
            prepare_handoff(db, slot, new_ids)
        except Exception as e:  # noqa: BLE001
            if strict:
                raise
            _note(db, "handoff_prepare", f"{type(e).__name__}: {e}")
    return n_triggers


def _verify_candidates(db, targets, guard, since_iso, new_ids, verified_deals,
                       deadline_fn=None):
    """Score observations written since *since_iso*, verify each under-cap
    candidate (verify budget), and enqueue confirmed deals.

    Appends relayable alert IDs to *new_ids* and confirmed deals to
    *verified_deals*; returns {watch_id: [relayable alert IDs]}.
    """
    per_watch: dict[str, list[str]] = {}
    for w in targets:
        candidates = score_new_observations(
            db, w, since_iso, cooldown_hours=w.get("cooldown_hours", 24))
        watch_new: list[str] = []
        for c in candidates:
            _check_deadline(deadline_fn)
            ok, conf = verify(db, w, c, guard)
            if not ok:
                continue  # unknown: never alert on an unverified fare
            # The alerts table stays the scoring-history record (the scorer's
            # cooldown reads it); delivery state lives in the outbox.
            add_alert(db, fingerprint=c["fingerprint"], reason="absolute",
                      verified_at=utcnow_iso())
            aid, action = enqueue_from_confirmation(db, w, c, conf)
            # v3: dedup is per (watch, departure week). A deduped candidate
            # was already outboxed without a meaningful drop and must NOT
            # be emitted to relay again -- only inserted and rearmed
            # alerts join the relay batch.
            # Record immediately (not after the loop) so a budget/deadline
            # stop later in this pass still hands this deal off.
            if action in RELAYABLE_ACTIONS and aid not in new_ids:
                watch_new.append(aid)
                new_ids.append(aid)
            duration = (date.fromisoformat(conf["return_date"])
                        - date.fromisoformat(conf["depart_date"])).days
            verified_deals.append({
                "watch_id": w["id"], "fingerprint": c["fingerprint"],
                "price_usd": conf["price_usd"],
                "depart_date": conf["depart_date"], "duration": duration,
                "cap": w["absolute_cap_usd"]})
        per_watch[w["id"]] = watch_new
    return per_watch


def cmd_check(args, db, watches, guard, *, drained=None, deadline_fn=None,
              slot=None):
    """One scheduled collection cycle (v3 group 4 ordering):

    1. Mint sentinel anchors + background grid (idempotent).
    2. Drain durable pending handoffs -- before any collection.
    3. Recheck prepared-but-unconfirmed handoffs (verify budget).
    4. Leftover verify (verify budget): candidates from the previous slot
       that were not verified there (e.g. that run hit its deadline or
       budget before its own verify pass).
    5. Collect: sentinels > bursts > background, paced across the 48 slots.
    6. Same-slot verify: candidates from THIS run's collection are verified
       immediately, so a deal found now is alerted now, not ~30 min later
       (spec: never delay a verified alert). The 24-attempt verify reserve
       guarantees collection cannot starve this pass.
    7. Queue burst triggers (new under-cap deals).
    8. Prepare the new handoffs for the agent to relay.
    """
    run_start = utcnow_iso()
    # The slot run_guarded CLAIMED, not a recomputed one: a run crossing a
    # half-hour boundary must keep working under its claimed slot.
    slot = slot if slot is not None else slot_id()
    summary: dict = {"slot_id": slot, "watches": {}}
    targets = [get_watch(watches, args.watch)] if args.watch else watches
    groups_cfg = {w["id"]: group_cfg_for(w) for w in targets}

    # 1. Rolling sentinel anchors + background grid (idempotent).
    n_sentinels = sentinel.mint_sentinels(db, groups_cfg)
    n_bg = sentinel.ensure_background_grid(db, groups_cfg)

    # 2. Hand off ready alerts BEFORE collecting. run_guarded already
    # drained (drain-first); direct callers drain here.
    if drained is None:
        drained = drain_pending(db, slot)
    drained_ids = [r["alert_id"] for r in drained]

    # 3-4. Retry pass, then leftover verify: the previous slot's candidates
    # that it did not
    # verify itself. Candidates it did verify are skipped by the
    # alerts-table cooldown.
    prev_start_iso = datetime.fromtimestamp(
        slot_start_ts(slot - 1), tz=timezone.utc).isoformat()
    new_ids: list[str] = []
    retries: dict = {"retry_ids": [], "retired": [], "unknown": []}
    retry_ids: list[str] = retries["retry_ids"]
    reprepared = False
    verified_deals: list[dict] = []
    per_watch: dict[str, list[str]] = {}
    try:
        # 3. Retry pass (verify budget): recheck, re-render, retire.
        # retry_ids is filled in place, so a stop mid-pass keeps the
        # retries confirmed before it (re-prepared in the except below).
        recheck_retries(db, watches, guard, slot, deadline_fn=deadline_fn,
                        out=retries)
        if retry_ids:
            reprepare_retries(db, slot, retry_ids)
            reprepared = True

        per_watch = _verify_candidates(db, targets, guard, prev_start_iso,
                                       new_ids, verified_deals,
                                       deadline_fn=deadline_fn)

        # 5. Collect, paced: sentinels > bursts > background.
        collected = sentinel.collect(db, guard, groups_cfg, slot, run_start,
                                     deadline_fn=deadline_fn,
                                     cadence_slots=cadence_slots(watches))
        summary["collect"] = {
            "attempts": collected["attempts"],
            "bursts": collected["bursts"],
            "sentinels": collected["sentinels"],
            "background": collected["background"],
            "drop_triggers": collected["drop_triggers"],
            "new_sentinel_cells": n_sentinels,
            "new_background_cells": n_bg,
        }

        # 6. Same-slot verify: this run's own collection output. collect()
        # narrowed the per-run allowance to its paced share; verification is
        # not paced (the daily ceiling and verify reserve still bound it), so
        # restore the default before verifying.
        guard.set_run_allowance(PER_RUN_MAX)
        same_slot = _verify_candidates(db, targets, guard, run_start,
                                       new_ids, verified_deals,
                                       deadline_fn=deadline_fn)
        for wid, ids in same_slot.items():
            per_watch.setdefault(wid, []).extend(ids)
    except BaseException as stop:
        # A budget/deadline/throttle stop (or crash) mid-verify: deals
        # already verified this slot must still be handed off AND emitted,
        # or the agent never relays them. Cleanup failures are recorded but
        # never replace the original stop.
        _finish_verified(db, slot, run_start, verified_deals, new_ids)
        if retry_ids and not reprepared:
            try:
                reprepare_retries(db, slot, retry_ids)
            except Exception as e:  # noqa: BLE001 - never mask the stop
                _note(db, "retry_reprepare", f"{type(e).__name__}: {e}")
        try:
            stop.handoff = _handoff_payload(
                db, slot, drained_ids + retry_ids + new_ids)
        except Exception as e:  # noqa: BLE001 - never mask the stop
            _note(db, "handoff_emit", f"{type(e).__name__}: {e}")
        raise
    n_triggers = _finish_verified(db, slot, run_start, verified_deals,
                                  new_ids, strict=True)
    for wid, ids in per_watch.items():
        summary["watches"][wid] = {"new_alerts": ids}
    summary["triggers_queued"] = n_triggers
    summary["handoff"] = _handoff_payload(
        db, slot, drained_ids + retry_ids + new_ids,
        retried=retry_ids, retired=retries["retired"],
        unknown=retries["unknown"])
    print(json.dumps(summary, indent=2))
    return 0


def cmd_outbox_mark(args, db, watches, guard):
    """Agent-facing: record that a prepared handoff was relayed.

    Only the handoff_prepared -> relay_attempted_unconfirmed transition is
    allowed here. user_confirmed is NEVER settable through this path --
    use outbox-confirm (human-driven) for that.
    """
    ok = update_outbox_state(db, args.alert_id, args.state, actor="agent",
                             event_no=args.event)
    if not ok:
        row = outbox_get(db, args.alert_id)
        have = row["state"] if row else "<unknown alert_id>"
        raise SystemExit(
            f"outbox-mark failed: cannot move {args.alert_id} from "
            f"{have} to {args.state}")
    print(json.dumps({"alert_id": args.alert_id, "state": args.state}))
    return 0


def cmd_outbox_list(args, db, watches, guard):
    """List outbox alerts: the digest input."""
    rows = outbox_list(db, args.state)
    print(json.dumps({"alerts": [
        {"alert_id": r["alert_id"], "event": r["event_no"],
         "state": r["state"],
         "handoff_count": r["handoff_count"],
         "prepared_slot": r["prepared_slot"],
         "price_usd": r["price_usd"],
         "confirmed_by": r["confirmed_by"],
         "created_at": r["created_at"], "updated_at": r["updated_at"],
         "message": r["message_text"]}
        for r in rows]}, indent=2))
    return 0


def cmd_outbox_confirm(args, db, watches, guard):
    """Human-only: finalize an alert after the USER confirms the deal.

    This is the ONLY path to the user_confirmed state (spec item 10), and
    scheduled code must never call it. The actor name is recorded for audit.
    """
    if not args.by:
        raise SystemExit("outbox-confirm requires --by <actor> for audit")
    ok = set_outbox_user_confirmed(db, args.alert_id, args.by,
                                   event_no=args.event)
    if not ok:
        raise SystemExit(
            f"outbox-confirm failed for {args.alert_id} event {args.event}: "
            f"unknown id, already terminal, or replaced by a newer deal")
    print(json.dumps({"alert_id": args.alert_id, "state": "user_confirmed",
                      "confirmed_by": args.by}))
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Cheap Flight Monitor (v2)")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("broad", help="flex-date sweep for a watch")
    p.add_argument("--watch", required=True)
    p.add_argument("--duration", type=int, default=7)
    _add_scheduled(p)

    p = sub.add_parser("narrow", help="specific-date round trip for a watch")
    p.add_argument("--watch", required=True)
    p.add_argument("--depart", required=True)
    p.add_argument("--return", dest="return_", required=True)
    _add_scheduled(p)

    p = sub.add_parser("canary", help="self-health reference query")
    _add_scheduled(p)

    p = sub.add_parser("check", help="drain handoffs, collect, score, verify, handoff")
    p.add_argument("--watch", default=None)
    _add_scheduled(p)

    p = sub.add_parser("outbox-mark", help="record a relayed handoff (agent use)")
    p.add_argument("--alert-id", required=True)
    p.add_argument("--event", type=int, required=True,
                   help="the handoff's event number, from the relay batch")
    p.add_argument("--state", required=True,
                   choices=["relay_attempted_unconfirmed"])

    p = sub.add_parser("outbox-list", help="list outbox alerts (digest input)")
    p.add_argument("--state", default=None,
                   choices=["pending", "handoff_prepared",
                            "relay_attempted_unconfirmed", "user_confirmed",
                            "retired"])

    p = sub.add_parser("outbox-confirm",
                       help="finalize an alert after USER confirmation (human only)")
    p.add_argument("--alert-id", required=True)
    p.add_argument("--event", type=int, required=True,
                   help="the alert's event number, from the relay batch")
    p.add_argument("--by", required=True, help="actor name, recorded for audit")

    args = ap.parse_args()
    db = None
    try:
        defaults, watches = load_config()
        cadence_slots(watches)  # reject a bad run_every_minutes before any work
        db = init_db(DB_PATH)
        sync_watches(db, watches, defaults)
        # The transport guard counts ACTUAL HTTP attempts and refuses to send
        # when no guard is installed (fail closed). Installed for every
        # collecting command, so even manual runs count against the ceilings.
        guard = BudgetGuard(db)
        guard.reset_run()
        if args.cmd in ("broad", "narrow", "canary", "check"):
            transport.install_transport_guard(guard)
        if args.cmd == "broad":
            # Disabled: answer before the lock and slot claim, so it can
            # never make a same-slot check skip collection. Pending alerts
            # are still drained (atomic UPDATE ... RETURNING: no slot or
            # lock needed) and printed for relay.
            return cmd_broad(args, db, watches, guard,
                             drained=drain_pending(db, slot_id()))
        if args.cmd == "narrow":
            return run_manual("narrow", args, db, watches, guard, cmd_narrow)
        if args.cmd == "canary":
            return run_manual("canary", args, db, watches, guard, cmd_canary)
        if args.cmd == "outbox-mark":
            return cmd_outbox_mark(args, db, watches, guard)
        if args.cmd == "outbox-list":
            return cmd_outbox_list(args, db, watches, guard)
        if args.cmd == "outbox-confirm":
            return cmd_outbox_confirm(args, db, watches, guard)
        if args.cmd == "check":
            return run_guarded("check", args, db, watches, guard, cmd_check)
    except SystemExit:
        raise
    except BudgetExhausted as e:  # manual commands hitting the ceiling: clean no-op
        print(json.dumps({"status": "noop", "reason": "budget_exhausted",
                          "detail": str(e)}, indent=2))
        return 0
    except Exception:  # noqa: BLE001 - infrastructure failure -> non-zero exit
        # A crash OUTSIDE run_guarded (config/DB failure): still /fail the
        # scheduled check so a dead scheduler is never green.
        if getattr(args, "scheduled", False) and args.cmd in PING_COMMANDS:
            try:
                heartbeat.ping_fail(db)
            except Exception:
                pass
        traceback.print_exc()
        return 1
    return 1


if __name__ == "__main__":
    sys.exit(main())
