# Runtime fit check + deploy plan (2026-09-25)

The repo is live, your deploy key works, and the workflow has changed. Read `WORKFLOW.md` on `main`.

**Roles:**
- **Claude Code** writes the code and opens PRs.
- **Codex** reviews, with Cursor as backup.
- **CI** decides whether the tests pass.
- **You (Muse):** runtime review, deploying, running the monitor, and reporting.
- **Yaozu** decides and approves deploys.

**What merged (#1):**
- The test fixture is now inside the repo. With it, your v3 suite passes on a clean checkout: 146 tests, so your count was right. 158 now.
- New deals are verified in the same slot they're found, not the next one.
- Three `deadline` stops in a row now ping `/fail`.
- Verified deals and drained alerts are now included in the JSON output on *every* stop: budget, deadline, throttle, crash, and backoff or slot no-ops. **Your scheduled turn must relay everything under `handoff.alerts` in whatever JSON `run.py check --scheduled` prints, including no-op output.**

**Please do now (read-only, nothing enabled):**
1. Pull `main` over SSH (`git -C ~/workspace/flight-monitor-repo pull`). Don't touch the live tree. Also confirm the GitHub host keys: `ssh-keygen -lf /root/.ssh/known_hosts` must show exactly ED25519 `SHA256:+DiY3wvvV6TuJJhbpZisF/zLDA0zPMSvHdkr4UvCOqU`, ECDSA `SHA256:p2QAMXNIC1TJYWeIOttrVc98/R1BUFWu3/LiyKgUfQM`, and RSA `SHA256:uNiVztksCsDhcc0u9e8BujQXVUpKZIDTMczCvj3tD2s`.
2. Run the suite there with your venv, `python -m unittest discover -s tests`, and report the count. It should be 158 OK.
3. Write a **"Runtime fit check"** report here in chat. The deploy key is read-only, so I'll have Claude file it as a GitHub issue. Cover:
   - Python version and venv path
   - Is fli from your clone identical to the pinned commit in `requirements.txt`?
   - Anything in the repo that assumes a path or layout your VM doesn't have
   - The exact command your scheduled turn would run
   - How you'll pass `HEALTHCHECKS_PING_URL` without printing it
   - Current quota %
4. Propose a deploy plan in the same report: how you'll switch the live tree to `main`, what happens to `data/monitor.db` (back up first), and how you'll roll back. **Don't deploy or enable anything until I approve.**

You no longer need the GitHub token; it has been revoked. The read-only deploy key is your only repo access.
