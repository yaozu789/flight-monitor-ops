# How changes land

| Role | Who |
|---|---|
| Write code + tests, open PRs | **Claude Code** (on Yaozu's Mac) |
| Main code review | **Codex** (gpt-6-sol; gpt-6-astra only for big design reviews or full audits); **Cursor** (non-Claude model) as backup |
| Runtime-fit review, deploy, run, report | **Muse** (hosts the monitor) |
| Test verdict | **CI** (`.github/workflows/tests.yml`, clean checkout) |
| Decisions, deploy approval | **Yaozu** |

1. Claude works on a `claude/<topic>` branch and opens a PR.
2. CI runs the offline suite. The pass/fail count comes from CI, not from anyone's report.
3. Codex reviews (Cursor if Codex is out of quota). The author is never the main reviewer. Findings get fixed on the branch and re-reviewed until "no blocking objections".
4. Claude merges once CI is green and review has no blocking objections. Yaozu may merge or veto at any point.
5. Muse reviews for runtime fit when a change touches how it runs the monitor (paths, Python version, how the scheduled turn invokes `run.py`). Muse reports runtime facts (quota, logs, whether runs fired) as **GitHub issues**.
6. **Muse deploys only a merged `main` commit, and only after Yaozu approves.** No auto-pull. No schedule, destination, limit, or credential change without Yaozu's OK.

Rules:
- Never commit `data/`, `*.db`, `.env`, tokens, or the healthchecks ping URL. At runtime it reaches the process only as the env var `HEALTHCHECKS_PING_URL`; on Muse it is stored in the gitignored `.env` (mode 600) and exported per run.
- Test fixtures live inside this repo. No absolute paths to any machine.
- Dependencies go in `requirements.txt` with exact pins.
- A new test must fail on the code before its fix.
- Design source of truth: `docs/spec.md`, plus the post-spec decisions listed in `CLAUDE.md`.
