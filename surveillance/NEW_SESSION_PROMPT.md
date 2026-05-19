# Copy-paste prompt for a new Claude session

**HOW TO USE:** Open a new chat with Claude Code in `~/Documents/Kalshi_Research_Project_repo/`. Paste everything between the `---BEGIN---` and `---END---` lines as your first message. It's self-contained — Claude won't have read the previous session.

---BEGIN---

I'm continuing a Kalshi prediction-market research project. You won't have any prior context — read `surveillance/PROJECT_STATUS.md` first (full state), then `surveillance/HANDOFF.md` (architecture), then `surveillance/RUNBOOK.md` (operations). Those three files are designed for this exact handoff.

## Quick facts

- **Repo:** working copy at `~/Documents/Kalshi_Research_Project_repo/`, branch `surveillance/whale-detection` (NOT pushed to GitHub yet — exists only on local + EC2 EBS).
- **GitHub:** `https://github.com/deniel-nankov/Kalshi_Research_Project.git` (main branch only).
- **EC2:** `i-0db0f22e57ce23a1f`, r7i.2xlarge, us-east-1. SSH key at `~/Documents/Kalshi_Research.pem`. **I stop the box overnight to save cost, so the IP changes every restart — ask me for the current IP, or use `aws ec2 describe-instances --instance-ids i-0db0f22e57ce23a1f --query 'Reservations[0].Instances[0].PublicIpAddress' --output text` to look it up.**
- **EC2 working dir:** `/opt/kalshi-pipeline/` (git repo, on `main` branch by default — surveillance branch lives separately).
- **S3:** `s3://kalshi-pipeline-189979486522-us-east-1-an/Kalshi_Data_Set/` (cross-account share with my professor's AWS 184471688530).

## Where we left off

**Headline result (committed):** `volume_burst_v2` detector at edge threshold 0.07 produced **+$11.63 M paper PnL on a 10-day train/test holdout** (548K flagged trades, 63% hit rate, daily Sharpe 26.73, precision@100 = 0.66, ROC AUC = 0.574). No outcome leakage. No look-ahead. Backtest in `surveillance/reports/v2_backtest_20260519T022947Z.md`.

**Pipeline is LIVE on EC2:**
- 4 new data captures running on systemd timers (milestones / live_data / forecast_pct / lifecycle_ws)
- Daily surveillance orchestrator at 04:30 UTC writes `surveillance/alerts/<dt>.jsonl` + `surveillance/reports/daily_<dt>.md` + `surveillance/reports/daily_<dt>.html`
- 18 commits on `surveillance/whale-detection` (see PROJECT_STATUS.md table)

**At the moment the previous session ended, three jobs were running on EC2:**
1. `whale_profile_report.py --days 31` (generating narrative report)
2. `ml_baseline_v3.py --days 31` (logistic regression baseline comparison)
3. `enrich_trades.py --all` (extending enrichment to all 110 days for full-history backtest)

Check whether they completed:
```bash
ssh -i ~/Documents/Kalshi_Research.pem ubuntu@<ip> '
  ls -la /opt/kalshi-pipeline/surveillance/reports/whale_profiles_*.md 2>&1 | tail -1
  ls -la /opt/kalshi-pipeline/surveillance/reports/ml_baseline_v3_*.md 2>&1 | tail -1
  ls /opt/kalshi-pipeline/data/kalshi/derived/enriched_trades/ | wc -l
  tail -5 /tmp/wp.log; echo "---"; tail -5 /tmp/ml.log; echo "---"; tail -5 /tmp/enrich_all.log'
```

## What's left to do

| Priority | Item | Effort |
|---|---|---|
| HIGH | Pull `whale_profiles_*.md` locally + open it — concrete trade examples + signature buckets | 5 min once it finishes |
| HIGH | Pull `ml_baseline_v3_*.md` locally + compare v2 vs v3 — confirms or refutes "v2 is enough" | 5 min once it finishes |
| HIGH | After full enrichment finishes, run final backtest on 110 days: `detect_v2_pnl_aware.py --all` + `tune_v2_thresholds.py --all` | 10 min |
| MED  | Push `surveillance/whale-detection` branch to GitHub (needs GH PAT on EC2 or pull-from-EC2-to-local-then-push) | 30 min |
| MED  | Pull `tune_v2_*` curve to compare 31-day vs 110-day best edge threshold | 5 min |
| LOW  | Bucket-level cross-market backtest (current is trade-level) | 1 hour |
| LOW  | Persist v2 calibration as a parquet under `state/calibration/` for weekly retraining instead of daily refit | 4 hours |
| LOW  | Slack/email alert routing (alerts.jsonl is already produced) | 2 hours |
| LOW  | S3 publish of daily HTML dashboards | 30 min |

## My working style preferences

- **Be concise.** Numbers > prose. No "I will now do X" preambles.
- **Be honest about limits.** When a backtest says NO-SHIP, that's the answer, not a problem to hide.
- **Casual tone.** Email-to-the-professor voice, not academic.
- **Numbered options.** When choices exist, give 2-4 numbered options; I'll respond with "1" or "1, 2, 3".
- **Execute then summarize.** If I say "run X" or "do X", just do it. Don't ask "should I?"
- **Institutional polish is required.** SHA256 sidecars on every parquet. Audit logs in `state/*_runs/<run_id>.json`. Train/test split for any detector validation. No outcome leakage.
- **PnL, not hit rate.** Prediction-market payoffs are asymmetric. Quote mean PnL per trade and Sharpe, not just hit rate.
- **No new ML in production path** without explicit comparison. v2 calibration is intentionally explainable.

## Non-negotiables (do not break these)

1. **Every derived parquet** gets a co-located `<file>.parquet.sha256` sidecar. The institutional release gate (PARQUET_AUDIT_TRAIL in `validate_data_health.py`) refuses to S3-sync if any forward parquet is missing one. Never skip this.
2. **Every multi-step script** writes an audit log to `state/<topic>_runs/<run_id>.json`. Run_id format: `{ISO_ts}_{uuid_hex8}`.
3. **Atomic writes only.** Pattern: `tmp_path = out.with_suffix(".parquet.tmp"); pq.write_table(...); tmp_path.replace(out)`.
4. **Scores never reference outcome columns** (`was_taker_correct`, `winning_side`, `taker_pnl_per_contract_dollars`) at scoring time. The backtest harness will catch this — if a new detector suddenly shows AUC > 0.95 with no methodology change, that's leakage.
5. **Ship gates for any new detector:** `precision@100 >= 0.60 AND ROC AUC >= 0.55 AND mean PnL/trade > 0` on a train/test holdout. If it doesn't pass all three, it stays in the codebase as anomaly-surfacing only.

## To start, please do this

1. Read `surveillance/PROJECT_STATUS.md`, then `surveillance/HANDOFF.md`, then `surveillance/RUNBOOK.md` (in that order).
2. Find the current EC2 IP (or ask me).
3. Verify the 3 pending jobs from the previous session — pull `whale_profiles_*.md` and `ml_baseline_v3_*.md` if they exist, summarize what they show.
4. Report status back to me with: (a) what's complete, (b) what's still running, (c) what's recommended next. Give me 3 numbered options for what to tackle first.

Do NOT push to GitHub or change systemd unit configs without asking.

---END---

## Optional shorter prompt (if Claude has already been onboarded once)

> Continue the Kalshi surveillance project. Repo at `~/Documents/Kalshi_Research_Project_repo/` on branch `surveillance/whale-detection`. Read `surveillance/PROJECT_STATUS.md` for state. EC2 IP changes per restart — current IP is **[ASK ME]**. Last working result: `volume_burst_v2` ships with +$11.6 M paper PnL on 10-day holdout. What's next: [your task].
