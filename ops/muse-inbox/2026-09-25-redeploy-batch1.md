# Mid-trial redeploy: audit batch 1 (2026-09-25)

**Yaozu approved:** redeploying commit `17b8e82` (`17b8e8270225cb01ea5bd9fb7241b3a77ed797ea`) during the trial, including a **brief pause** of the trial schedule and changing its command path to the new release. Nothing else changes: same cadence, same prompt text apart from the path, same trial end time.

**Why:** the full audit (issue #16) found alert bugs. The worst: connecting flights stored the layover as the destination, so 1-stop deals could never be confirmed. Alerts also named the wrong airports. Fixes are in PR #17.

**Stop rule:** stop at the first failed or surprising check and report. If something fails after the pause, **resume the old schedule unchanged** (the old release `fm-release-3f9cbb5`) before reporting.

## Steps
1. **Pull over HTTPS:** `git -C ~/workspace/flight-monitor-repo pull`. Confirm `git rev-parse 17b8e82` resolves to `17b8e8270225cb01ea5bd9fb7241b3a77ed797ea`.
2. **Build the release while the trial keeps running:**
   - `git -C ~/workspace/flight-monitor-repo archive 17b8e82` into `~/workspace/fm-release-17b8e82`.
   - Run the offline suite there **before copying data**, with a temporary empty `data/`: expect **174 OK**.
   - Remove the temporary `data/`.
3. **Pause:**
   - Right after a scheduled run finishes, pause the trial schedule.
   - Confirm no `run.py` process is running (`ps`) and the collection lock is free.
4. **Back up:** `.backup` `fm-release-3f9cbb5/data/monitor.db` into `~/workspace/fm-backups/`, then run `integrity_check`.
5. **Copy** `data/` and `.env` into the new release (root-owned, mode 600). Run `init_db` once and `integrity_check`. Record observation, outbox and run_slots counts: they must equal the backup's.
6. **Update the schedule command** to `cd "/home/hatch/workspace/fm-release-17b8e82"`. Everything else in the command and prompt stays identical.
7. **Resume the schedule.** Record the pause and resume times.
8. **Watch the next scheduled run:** exit 0, `run_slots` completed, heartbeat ping ok, observations increase.

## Report (in chat)
Pass/fail + evidence per step, the pause window, and the first post-deploy run's numbers. `fm-release-3f9cbb5` stays untouched as the rollback. Then stop.
