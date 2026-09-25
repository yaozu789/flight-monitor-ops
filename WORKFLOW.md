# How changes land

1. **Muse** works on a branch (`muse/<topic>`), commits, pushes, and opens a pull request.
2. **CI** (`.github/workflows/tests.yml`) runs the offline test suite on a clean checkout. Test counts come from CI, not from anyone's report.
3. **Claude / Codex** review the PR diff.
4. **Yaozu merges.** Nobody else merges to `main`.
5. **Muse deploys only a merged commit from `main`** to its runtime, and only after Yaozu says so. No auto-pull, no schedule changes without approval.

Rules:
- Never commit `data/`, `*.db`, `.env`, tokens, or the healthchecks ping URL (runtime env var `HEALTHCHECKS_PING_URL` only).
- Test fixtures must live inside this repo. No absolute paths to any machine.
- Dependencies go in `requirements.txt` with exact pins.
- Design source of truth: `Flight_monitor_final_spec.md` (kept outside the repo in Yaozu's project folder; ask for changes via PR description).
