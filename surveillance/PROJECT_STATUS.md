# Kalshi Research Project — Full status

**Last updated:** 2026-05-19
**Owner:** deniel-nankov
**Current state:** Two production pipelines live on EC2. Surveillance branch
deep into Phase 7 with one shipping detector validated on 10-day holdout.

---

## 0. TL;DR — where the project lives

| Where | What |
|---|---|
| **GitHub** | `https://github.com/deniel-nankov/Kalshi_Research_Project.git` |
| **Local working copy** | `~/Documents/Kalshi_Research_Project_repo/` (branch `surveillance/whale-detection`) — surveillance work |
| **Local "old" copy** | `~/Documents/Kalshi_Research_Project-main/` — analysis_reports + figures from Q1 (NOT a git repo, came from a zip) |
| **EC2 instance** | `i-0db0f22e57ce23a1f` (r7i.2xlarge) in us-east-1 |
| **EC2 working dir** | `/opt/kalshi-pipeline/` (git checkout on `main` branch by default) |
| **SSH key** | `~/Documents/Kalshi_Research.pem` |
| **S3 bucket** | `s3://kalshi-pipeline-189979486522-us-east-1-an/Kalshi_Data_Set/` (cross-account read+write granted to professor's AWS 184471688530) |
| **Most recent IP** | 44.222.132.28 (stop/start changes it — user stops nightly to save cost) |

**Two branches:**
- **`main`** — original Kalshi data ingestion pipeline (capture, dedup, S3 sync, validate)
- **`surveillance/whale-detection`** — whale/insider detection layer on top of main (NOT yet pushed to GitHub — exists only on EC2 EBS + local laptop checkout)

---

## 1. What the project is

A two-track Kalshi prediction-market research project:

**Track 1 — Institutional data pipeline (`main` branch).** Continuous 24×7 capture of every Kalshi market, trade, orderbook delta, ticker, candlestick, and orderbook snapshot. Audit-trailed with SHA-256 sidecars, validated against an institutional release gate, mirrored to S3 for professor access. ~470 M trades, ~67 M market snapshots, ~82 M WS orderbook deltas, 5 years of history.

**Track 2 — Whale & insider-trade surveillance (`surveillance/whale-detection` branch).** Built ON TOP of Track 1 to detect informed-flow behavioral patterns from anonymous data. Captures 4 new data streams Kalshi exposes (milestones, play-by-play, market lifecycle, forecast percentiles), enriches every trade with 38 columns, runs 5 behavioral detectors validated via train/test backtest. **One detector ships with +$21.22 mean PnL/trade and Sharpe 26.73 on a 10-day, 32.5 M-trade holdout.**

---

## 2. Repo layout (relevant parts)

```
/opt/kalshi-pipeline/                  (and local checkout)
  scripts/
    # main pipeline (Track 1)
    update_forward.py                   incremental REST ingestion
    capture_ws_microstructure.py        WS orderbook+trades+ticker daemon
    capture_orderbook_depth.py          REST L2 snapshots
    capture_candlesticks_1m.py          1-min OHLCV
    select_top_universe.py              daily top-N by 24h $-volume
    fix_orphan_tickers.py               backfill missing markets via API
    smart_redepupe_markets.py           Polars-based 60× faster dedup (built this session)
    harden_dedup_output.py              SHA256 sidecars + audit log + key-preservation
    validate_data_health.py             10 institutional gates
    institutional_data_release.sh       full release pipeline
    sync_verified_dataset_to_s3.sh      gate → S3

    # surveillance pipeline (Track 2)
    capture_milestones.py               GET /milestones every 5 min
    capture_live_data.py                GET /live_data/milestone/{id} every 2 min
    capture_forecast_percentile.py      GET .../forecast_percentile_history daily
    capture_lifecycle_ws.py             WS market_lifecycle_v2 subscriber

    enrich_trades.py                    Phase 2.1 base economics + size pct
    enrich_with_milestones.py           Phase 2.2 event → milestone link
    enrich_with_milestones_pbp.py       Phase 2.3 content-hash play events
    enrich_with_lifecycle.py            Phase 2.4 precise resolution timestamps

    detect_whales.py                    v1 whale + volume_burst (anomaly-only)
    detect_pre_play_alpha.py            v1 pre-play
    detect_spoofing.py                  v1 spoof patterns (anomaly-only)
    detect_cross_market.py              v1 event-level concentration
    detect_v2_pnl_aware.py              v2 PnL-aware calibrated layer (volume_burst SHIPS)
    tune_v2_thresholds.py               edge-threshold sweep
    backtest_detectors.py               v1 backtest harness
    ml_baseline_v3.py                   logistic-regression comparison
    whale_profile_report.py             narrative report generator

    surveillance_daily.py               daily orchestrator (runs everything)

  infra/aws/systemd/
    kalshi-forward.service/timer
    kalshi-ws-microstructure.service     (long-running)
    kalshi-orderbook.service/timer
    kalshi-candlesticks.service/timer
    kalshi-top-universe.service/timer
    kalshi-health.service/timer
    kalshi-s3-verified-sync.service/timer
    kalshi-observability.service/timer
    kalshi-ops-console.service           (long-running)
    kalshi-state-backup.service/timer
    kalshi-process-heal.service/timer
    # surveillance additions
    kalshi-milestones.service/timer      every 5 min, OnBootSec=4min
    kalshi-live-data.service/timer       every 2 min, OnBootSec=8min, After=milestones
    kalshi-forecast-percentile.service/timer  daily 06:30 UTC
    kalshi-lifecycle-ws.service          (long-running)
    kalshi-surveillance-daily.service/timer  daily 04:30 UTC

  surveillance/
    DESIGN.md                            full 5-phase plan
    SCHEMA.md                            enriched_trades 38-column schema
    RUNBOOK.md                           operator day-to-day reference
    HANDOFF.md                           project handoff for next operator
    PROJECT_STATUS.md                    this file
    reports/                             daily reports + backtests
    alerts/                              daily JSONL alerts

  data/kalshi/
    historical/                          raw + WS captures (per-dt partitioned)
      forward_trades/                    470 M trades, 5 years
      forward_markets/                   67 M market snapshots (deduped)
      forward_orderbook_deltas/          82 M WS L2 deltas
      forward_trades_ws/                 819 K WS trades
      forward_tickers_ws/                10.5 M WS quote events
      forward_candlesticks_1m/           1.5 M 1-min OHLCV
      orderbook_snapshots/               17 M L2 snapshots
      forward_milestones/                Kalshi's milestone graph
      forward_live_data/                 play-by-play snapshots
      forward_lifecycle_ws/              market state transitions
    derived/                             produced by enrichment + detectors
      enriched_trades/                   per-dt 38-col table
      whale_flags/, pre_play_flags/,
      spoof_flags/, spoof_intensity_hourly/,
      cross_market_flags/, cross_market_buckets/,
      v2_flags/
    state/                               checkpoints + audit logs
      surveillance_runs/                 audit JSON per detector run
      enrichment_runs/                   audit JSON per enrichment run
      detector_runs/                     audit JSON per detector run
      *.checkpoint.json                  incremental sync state
      forward_checkpoint.json
```

---

## 3. Build sequence — what we did in this project (chronological)

### Weekend 1: Institutional data pipeline maturation
- Fixed `capture_ws_microstructure.py` two bugs (websockets 15.x kwarg rename; asyncio.Event in wrong loop)
- Migrated EC2 from m6in.4xlarge ($780/mo) → r7i.2xlarge ($381/mo) — **51% cost savings, same 64 GB RAM**
- Built **`smart_redepupe_markets.py`** (Polars-based) — replaces 3-hour DuckDB dedup with 3-minute Polars equivalent (60× faster). Removed 19,386 duplicate market rows → 0.
- Built **`harden_dedup_output.py`** — SHA256 sidecars + audit log + key-preservation verification.
- Orphan backfill: 79,809 missing market refs → 0 ("all orphans cleared") via Kalshi's `/markets/{ticker}` API.
- Final institutional sync: **14 PASS / 3 WARN / 0 FAIL** on validate_data_health gate.
- Built first analyses (5 figures) + email draft for professor.

### Weekend 2: Surveillance branch (`surveillance/whale-detection`)
Comprehensive whale/insider-trade detection layer:

**Phase 0** — 4 new data captures:
- `capture_milestones.py` — Kalshi's internal milestone graph (5-min cadence). **76 K rows in first run.** Maps real-world events (NBA games, tennis matches, elections) to Kalshi markets via `primary_event_tickers`.
- `capture_live_data.py` — per-milestone live data (2-min cadence, concurrency 8). **2,761 active streams** captured in first run (93% hit rate). For sports: scoreboard fields + play-by-play.
- `capture_forecast_percentile.py` — Kalshi's official forecast for numeric markets (daily cadence). Currently 0 hits — our universe doesn't include macro series. Deferred.
- `capture_lifecycle_ws.py` — WS subscriber for `market_lifecycle_v2` + `multivariate_market_lifecycle`. **392 events in 30-sec smoke, 0 seq gaps.**

All 4 deploy on staggered systemd timers. Auth model (RSA-PSS with `KALSHI_API_PRIVATE_KEY` env var, salt_length=MAX_LENGTH) matches existing ws daemon. SHA256 sidecars on every parquet.

**Phase 2** — 4 enrichment stages:
- `enrich_trades.py` — 38-column base table. Notional, size_pct (global+in-market today), volume_z_5min, time_to_close, outcome labels, taker_pnl_per_contract. **2.18 M trades/day in 30 sec.**
- `enrich_with_milestones.py` — left join `event_ticker → primary_event_tickers`. ~50% of trades get a milestone link. Sports/Esports/Elections/Crypto.
- `enrich_with_milestones_pbp.py` — **content-hash** play-event detection (tennis has no `pbp` array — score-field changes are the play events). `seconds_to_next_play_event`, `in_pre_play_window`.
- `enrich_with_lifecycle.py` — exact resolution timestamps from WS (no polling lag). `time_to_lifecycle_resolution_seconds`, `is_pre_resolution_window`.

All stages are idempotent in-place rewrites with fresh SHA256 sidecars + audit log.

**Phase 4** — 5 detectors:
- D1 **Whale** — top-1% global AND top-5% in-market by notional. v1 hit-rate edge +2.32 pp but PnL/trade −$37.53 (asymmetric-payoff trap). **NO-SHIP.**
- D2 **Pre-play alpha** — trades within 5 min of a play event with correct outcome. Forward-looking; needs more live_data accumulation. **0 flags so far.**
- D3 **Forecast deviation** — DEFERRED (no numeric forecast data captured).
- D4 **Volume burst** — 5-min volume z-score ≥ 3.0 per market. v1 fails alone, **v2 calibration SHIPS** at edge 0.07.
- D5 **Spoofing** — orderbook_delta "large add + cancel within 30 s no fill" pattern. **NO-SHIP** as alpha (mostly market-maker activity), useful as anomaly surfacer.
- D6 **Cross-market** — event-bucket aggregation of v2 flags. PnL/trade +$1.19, Sharpe 5.97 — complementary, not standalone.

**Phase 6** — backtest validation harness:
- `backtest_detectors.py` — v1 verdict: ALL NO-SHIP. Whale calibration INVERTED (high-score trades win LESS).
- `detect_v2_pnl_aware.py` — v2 PnL-aware layer using `pwin_hat × yes_price_bin × taker_side` calibration. **volume_burst SHIPS** with all 3 ship gates passed.
- `tune_v2_thresholds.py` — edge-threshold sweep. **Best = 0.07** (max-Sharpe among shipping rows): precision@100=0.89, AUC=0.605, PnL/trade=+$17.50, Sharpe=24.52.
- Extended on 31 days (10-day holdout, 32.5 M resolved): **PnL/trade +$21.22, Sharpe 26.73, $11.6 M total paper PnL.**

**Phase 5/7** — daily orchestrator + dashboards:
- `surveillance_daily.py` — full pipeline cron (04:30 UTC). Triggers forward, runs 4 enrichments, applies v2 + cross-market, emits per-trade alerts.jsonl, daily MD report, **self-contained HTML dashboard** with embedded chart PNGs.
- `kalshi-surveillance-daily.timer` — LIVE on EC2.
- Documentation: DESIGN, SCHEMA, RUNBOOK, HANDOFF — full institutional doc chain.

---

## 4. Key numbers (memorize these)

| Metric | Value |
|---|---|
| Trades in dataset | 470,703,302 |
| Markets in dataset | 67,537,507 |
| Unique tickers | 17,110,423 |
| Date range | 2021-06-30 → 2026-05-18 (≈5 years) |
| Total contracts traded | 83.19 B |
| Notional face value | $83.19 B |
| WS orderbook deltas captured | 82 M+ |
| Days enriched in surveillance | 31 (of 110 available) |
| v2 production threshold | edge ≥ 0.07 |
| v2 backtest precision@100 | 0.89 (3-day), 0.66 (10-day) |
| v2 backtest ROC AUC | 0.605 (3-day), 0.574 (10-day) |
| v2 backtest mean PnL/trade | +$17.50 (3-day), **+$21.22** (10-day) |
| v2 backtest Sharpe | 22.15 (3-day), **26.73** (10-day) |
| v2 total paper PnL (10-day holdout) | **+$11,632,271** |
| v2 flagged trades on test holdout | 548,131 |
| EC2 cost | $381/mo (r7i.2xlarge, was $780/mo on m6in.4xlarge) |

---

## 5. Commit history on `surveillance/whale-detection` (chronological)

| # | Commit | What |
|---|---|---|
| 1 | `32f709c` | Phase 0 — 4 capture scripts + DESIGN.md |
| 2 | `0f2772d` | Phase 0 — live-API auth + heuristic fixes |
| 3 | `b9a...` | Phase 0 — 7 systemd unit files |
| 4 | `fb4f13d` | Phase 2.1 — enrich_trades.py + SCHEMA.md |
| 5 | `4345333` | Phase 4.1+4.4 — Whale + Volume-burst v1 detectors |
| 6 | `3ce7a78` | Phase 2.2 — enrich_with_milestones.py |
| 7 | `14e908b` | Phase 2.3 — enrich_with_milestones_pbp.py |
| 8 | `58715d3` | Phase 4.2 — Pre-play alpha detector |
| 9 | `ba42b7f` | Phase 2.4 — enrich_with_lifecycle.py |
| 10 | `1e9d303` | Phase 4.5 — Spoofing detector |
| 11 | `2d3af82` | Phase 6.1 — Backtest harness (v1 NO-SHIP findings) |
| 12 | `fc4c368` | Phase 6.2 — v2 PnL-aware (volume_burst SHIPS) |
| 13 | `c6094b6` | Phase 6.3 — Threshold sweep (best edge=0.07) |
| 14 | `2d28b88` | Phase 5.2 — Daily orchestrator + systemd timer |
| 15 | `fe67705` | Phase 4.6 — Cross-market detector |
| 16 | `eebcf2b` | Phase 7.1+7.2 — cross-market in daily + HTML dashboard |
| 17 | `d28edcf` | Phase 7.3 — RUNBOOK + HANDOFF docs |
| 18 | `00200d8` | Extended 31-day paper trading + ml_baseline_v3 + whale_profile_report tooling |

The commits on `main` (the dedup tooling) were created but **never pushed to GitHub** because EC2 has no GitHub credentials. They live only on EC2 EBS + local laptop clone.

---

## 6. What's running RIGHT NOW on EC2 (when the box is up)

```
SYSTEMD SERVICES — long-running (start at boot)
  kalshi-ws-microstructure.service       orderbook + trades + tickers WS
  kalshi-lifecycle-ws.service            market lifecycle WS
  kalshi-ops-console.service             observability JSON snapshots

SYSTEMD TIMERS
  kalshi-forward.timer                   every 15 min, OnBootSec=2min
  kalshi-milestones.timer                every 5 min, OnBootSec=4min
  kalshi-live-data.timer                 every 2 min, OnBootSec=8min, After=milestones
  kalshi-orderbook.timer                 every 60 min, OnBootSec=10min
  kalshi-candlesticks.timer              every 10 min, OnBootSec=2min
  kalshi-top-universe.timer              daily 00:30 UTC
  kalshi-forecast-percentile.timer       daily 06:30 UTC
  kalshi-health.timer                    every 30 min
  kalshi-s3-verified-sync.timer          every 10 min
  kalshi-surveillance-daily.timer        daily 04:30 UTC
  kalshi-observability.timer             every 30 min
  kalshi-state-backup.timer              daily 07:30 UTC
```

**Important nuance:** During this session, several timers were temporarily disabled (`systemctl disable --now`) to prevent a boot stampede. The dedup tooling tests, ml_baseline runs, etc. need a quiet system to fit in memory. **Re-enable them via `systemctl enable --now <timer>` if a fresh instance restart is needed and you want full production operation.**

The user has been **stopping the EC2 overnight to save cost** ($381/mo → ~$200/mo if stopped ~10h/day). Every restart = new public IP. The data persists on EBS.

---

## 7. What's deferred / open work

| Item | Status | Effort to close |
|---|---|---|
| Push surveillance branch to GitHub | Blocked: EC2 has no GitHub creds | 5 min once creds configured |
| Push the dedup tooling commit `71417fa` to GitHub | Same | Same |
| **Forecast deviation detector (D3)** | Needs CPI/Fed/inflation series in top universe | 1 day to add capture + 1 day to validate |
| **Pre-play alpha live signal (D2)** | Needs ~7 days of live_data accumulation; live_data only started 2026-05-18 | 0 work; just wait |
| **Bucket-level cross-market backtest** | Current measures trade-level precision; bucket-level is cleaner | 1 hour |
| **Weekly retraining of v2 calibration** | Currently calibration fit at daily orchestrator time | 4 hours |
| **Slack/email alert routing** | alerts.jsonl is produced; routing to a human channel is glue | 2 hours |
| **S3 publish of HTML dashboards** | One `aws s3 cp` step in orchestrator | 30 min |
| **Full 110-day backtest** | 79 more days to enrich at ~5-10 sec each | 15 min enrichment + 5 min backtest |
| **Polymarket cross-exchange parity** | Polymarket adapter exists; needs timestamp alignment | 2-3 days |
| **Redundant capture on 2nd EC2** | For 24/7 production-grade redundancy | 1 day infra |

---

## 8. Conventions / non-negotiables

1. **Every derived parquet ships with a SHA-256 sidecar** (`<file>.parquet.sha256` containing `<hex_digest>  <basename>\n`). Validate_data_health PARQUET_AUDIT_TRAIL gate enforces this.
2. **Every long-running script writes a structured audit log** to `state/<topic>_runs/<run_id>.json` with row counts, file list, version, timing.
3. **Train/test discipline** — any new detector MUST be validated on a clean holdout. Backtest harness (`detect_v2_pnl_aware.py`, `backtest_detectors.py`) enforces this. **Never claim alpha without holdout PnL > 0.**
4. **No outcome leakage in scoring** — `was_taker_correct` is the LABEL, never an input. Polars allows this to be checked at schema time.
5. **Atomic writes** — `tmp + replace()` pattern for every parquet to avoid half-written files.
6. **Same-day percentiles only** — no look-ahead in size_pct_*_today columns.
7. **Idempotent re-runs** — every enrichment stage drops its own columns before re-joining.
8. **PnL, not hit rate** — prediction-market payoffs are asymmetric. A 51% hit rate at $0.85 yes_price loses money. Always quote PnL.

---

## 9. Common operations

### Bring EC2 back online after overnight stop
```bash
aws ec2 start-instances --instance-ids i-0db0f22e57ce23a1f
aws ec2 wait instance-running --instance-ids i-0db0f22e57ce23a1f
aws ec2 describe-instances --instance-ids i-0db0f22e57ce23a1f \
  --query 'Reservations[0].Instances[0].PublicIpAddress' --output text
ssh -i ~/Documents/Kalshi_Research.pem ubuntu@<new-ip>
```

### Fire the daily surveillance pipeline manually
```bash
ssh -i ~/Documents/Kalshi_Research.pem ubuntu@<ip>
sudo systemctl start kalshi-surveillance-daily.service
sudo journalctl -fu kalshi-surveillance-daily.service
```

### Read today's alerts
```bash
ssh -i ~/Documents/Kalshi_Research.pem ubuntu@<ip> \
  cat /opt/kalshi-pipeline/surveillance/alerts/$(date -u --date='yesterday' +%Y-%m-%d).jsonl \
  | jq '.' | less
```

### Run v2 backtest on any window
```bash
cd /opt/kalshi-pipeline
.venv/bin/python3 scripts/detect_v2_pnl_aware.py --days 30
.venv/bin/python3 scripts/tune_v2_thresholds.py --days 30
```

### Sync the surveillance branch to GitHub (one-time setup)
EC2 needs a GitHub Personal Access Token. Set as `GH_TOKEN` env var or via `gh auth login`. Then `cd /opt/kalshi-pipeline && git push origin surveillance/whale-detection`.

---

## 10. User's working style (for the next operator / Claude session)

- **Concise.** Don't over-explain. Numbers > prose.
- **Honest about limits.** When the backtest says NO-SHIP, that's data, not failure. Document the why.
- **Casual tone.** Not formal academic. Email-to-the-professor voice.
- **Asks "execute X"** — that means do it, then summarize. Don't ask "do you want me to do X?"
- **Numbered options** — When presenting choices, give 2-4 numbered options. User responds with "1" or "1, 2, 3".
- **Stops EC2 nightly.** Expect IP changes. Expect background jobs to die.
- **Wants institutional polish.** SHA256, audit logs, train/test, RUNBOOK — these are required, not optional.
- **Wants concrete results.** "We found $11.6 M paper PnL" > "the detector is calibrated."

---

## 11. What this project is NOT

- **NOT live trading.** All PnL is paper / backtest. We are not authorized to trade Kalshi from this pipeline.
- **NOT per-account tracking.** Kalshi public API exposes NO trader identity. Detection is behavioral on anonymous data.
- **NOT ML-based** in the production path. v2 uses a 2D non-parametric calibration table for explainability. ML baseline (`ml_baseline_v3.py`) exists for comparison only.
- **NOT a daily-reviewed system.** Operator review is optional; pipeline runs unattended at 04:30 UTC and writes artifacts to disk + S3.
