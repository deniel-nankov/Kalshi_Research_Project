# Surveillance project — handoff

This document is the entry point for anyone picking up the
`surveillance/whale-detection` work. Read this first, then `DESIGN.md`
(theory) and `RUNBOOK.md` (operations).

---

## One-paragraph summary

We built an institutional-grade whale-and-insider-trade detection
pipeline on Kalshi data. It captures four new data streams (milestones,
play-by-play, market lifecycle, forecast percentiles), enriches every
trade with size/temporal/outcome features and milestone+pbp+lifecycle
joins, runs five behavioral detectors, validates them on a clean
train/test holdout, and produces daily alert JSONL + Markdown +
self-contained HTML dashboards. One detector ships with positive PnL
(volume_burst_v2 at edge threshold 0.07: precision@100=0.89, ROC
AUC=0.605, +$17.50 PnL/trade, daily-rebalanced Sharpe 24.52 on a 3-day
holdout). The whole pipeline runs unattended daily at 04:30 UTC.

---

## What this gives a successor operator

1. A working detector with documented ship-gate metrics.
2. A reusable train/test backtest harness — the discipline that
   separated "patterns" from "alpha" during development.
3. Six pre-built detectors at varying maturity (one shipping, one
   complementary, four in anomaly-surfacing mode).
4. Live capture of four data streams Kalshi exposes but most
   researchers don't pull (milestones, live_data, market_lifecycle_v2
   WS, forecast_percentile_history).
5. Per-day audit logs and SHA-256 sidecars on every output parquet —
   the institutional reproducibility chain.

---

## Project layout

```
scripts/
  capture_milestones.py           Phase 0 — milestones poller
  capture_live_data.py            Phase 0 — play-by-play poller
  capture_forecast_percentile.py  Phase 0 — Kalshi forecast (deferred)
  capture_lifecycle_ws.py         Phase 0 — WS market_lifecycle_v2 subscriber

  enrich_trades.py                Phase 2.1 — base economics + size pct + outcome
  enrich_with_milestones.py       Phase 2.2 — event→milestone link
  enrich_with_milestones_pbp.py   Phase 2.3 — seconds_to_next_play_event
  enrich_with_lifecycle.py        Phase 2.4 — precise resolution timestamps

  detect_whales.py                Phase 4.1 — v1 whale + volume_burst (anomaly-only)
  detect_pre_play_alpha.py        Phase 4.2 — pre-play alpha
  detect_spoofing.py              Phase 4.5 — orderbook spoof patterns
  detect_cross_market.py          Phase 4.6 — event-level concentration
  detect_v2_pnl_aware.py          Phase 6.2 — PnL-aware calibrated layer
  tune_v2_thresholds.py           Phase 6.3 — edge-threshold sweep

  backtest_detectors.py           Phase 6.1 — v1 validation harness
  surveillance_daily.py           Phase 5.2/7.1/7.2 — daily orchestrator

infra/aws/systemd/
  kalshi-milestones.timer + .service
  kalshi-live-data.timer + .service
  kalshi-forecast-percentile.timer + .service
  kalshi-lifecycle-ws.service           (long-running)
  kalshi-surveillance-daily.timer + .service

surveillance/
  DESIGN.md                       The full 5-phase plan + hard constraints
  RUNBOOK.md                      Operator day-to-day reference
  HANDOFF.md                      This document
  SCHEMA.md                       enriched_trades 38-column schema
  reports/                        Generated daily reports + backtests
```

---

## Architecture, top to bottom

```
KALSHI API (public)
  │ REST: /markets, /trades, /milestones, /live_data/milestone/{id}
  │       /events/{ticker}/forecast_percentile_history
  │ WS:   trades, ticker, orderbook_delta, orderbook_snapshot,
  │       market_lifecycle_v2, multivariate_market_lifecycle
  ▼
CAPTURE LAYER  (systemd timers + long-running WS daemons)
  forward_trades, forward_markets,
  forward_orderbook_deltas, forward_trades_ws, forward_tickers_ws,
  forward_orderbook_snapshots, forward_candlesticks_1m,
  forward_milestones, forward_live_data, forward_lifecycle_ws
  → parquet partitioned by dt=, SHA-256 sidecar on every file
  ▼
ENRICHMENT LAYER (4 stages, atomic in-place rewrites)
  enrich_trades            base economics, size_pct, outcome labels
  enrich_with_milestones   event→milestone link via primary_event_tickers
  enrich_with_milestones_pbp  content-hash play-event timestamps
  enrich_with_lifecycle    precise resolution timestamps from WS
  ▼
DETECTION LAYER
  v1 detectors (raw signal)  → anomaly surfacing
  v2 PnL-aware calibration   → ships if all 3 gates pass
  cross-market aggregation   → boost layer on top of v2
  ▼
ORCHESTRATION
  surveillance_daily.py  (cron 04:30 UTC):
    1. ensure forward fresh
    2. run 4 enrichment stages
    3. apply v2 calibration + edge filter
    4. attach cross-market bucket scores
    5. write surveillance/alerts/<dt>.jsonl
    6. write surveillance/reports/daily_<dt>.md
    7. write surveillance/reports/daily_<dt>.html
    8. append daily_digest.tsv
    9. audit log
```

---

## The single most important methodological point

**Hit rate is misleading; only PnL is honest.**

Prediction markets have asymmetric payoffs (a taker who pays $0.85
and wins makes $0.15; if they lose they lose $0.85). A detector that
flags trades with 51% hit rate at $0.85 paid loses money. The v1
backtest discovered this — the v2 calibration layer fixes it by
scoring trades as `pwin_hat - taker_break_even_price` instead of by
size or burst-z directly.

Any future detector MUST be validated on `mean_pnl_per_trade`, not
just hit rate. The harness enforces this. Don't override it.

---

## Hard constraints from Kalshi's API

These are NOT bugs to fix; they are facts of the public API:

- **No per-order ID** in public orderbook_delta. Only
  `client_order_id` for your own orders.
- **No trader identity** anywhere. Trades are anonymous.
- **No leaderboard / "follow this account" endpoint**.
- **No real-time alerts API** — you must poll or use the WS streams
  Kalshi does expose.

So per-account tracking is not possible. The pipeline detects
behavioral patterns from anonymous data, which is what CFTC/exchange
surveillance teams actually do.

---

## What's deferred / future work

| Item | Why deferred | Effort |
|---|---|---|
| Forecast deviation detector (D3) | `forecast_percentile_history` returns no data for our current numeric markets (mostly sports/political binary, not macro). Need to add CPI/Fed/inflation series to capture universe first. | 1 day |
| Pre-play alpha live signal | `forward_live_data` only began capturing 2026-05-18. Need ~7 days of overlap before a meaningful holdout test. Re-run M6.2 / M6.3 on 2026-05-25+ data. | 0 work; just waiting |
| Bucket-level cross-market backtest | Current M4.6 backtest measures precision@K on TRADES inside flagged buckets; the cleaner metric is precision@K on BUCKETS. Code change in `detect_cross_market.py`. | 1 hour |
| Weekly retraining of v2 calibration | Currently calibration is fit from the daily orchestrator's own data (single-day). Should fit on a rolling 7-day train and persist to `state/calibration/<flag>.parquet`. | 4 hours |
| Slack / email alert routing | The pipeline writes alerts.jsonl; routing to a human channel is glue work. Add a small follow-on cron that reads the JSONL and posts top-N to a webhook. | 2 hours |
| S3 publish for the HTML dashboards | Easy `aws s3 cp` step in the orchestrator if you want a shareable URL. | 30 min |

---

## How the work was sequenced (so you can audit / re-do)

The commits on this branch document the build order:

```
1. Phase 0 captures              (4 new datasets, deployed live)
2. SCHEMA.md design              (decide the enriched_trades target)
3. enrich_trades.py              (base columns, no joins)
4. v1 detectors                  (look impressive on single-day eyeball)
5. v1 backtest                   (DISCOVERY: v1 is wrong)
6. v2 PnL-aware calibration      (volume_burst SHIPS)
7. Threshold sweep               (tune to max-Sharpe shipping point)
8. Daily orchestrator + timer    (production cron)
9. Milestone/pbp/lifecycle joins (later enrichment, lower-priority signals)
10. Cross-market detector        (complementary booster)
11. HTML dashboard               (institutional polish)
12. Documentation                (this file + RUNBOOK + DESIGN)
```

If you redo this from scratch, the most-valuable sequence is
**1 → 3 → first v1 detector → backtest → v2 → ship → everything else**.
The backtest harness is what disciplines the project — do not skip it.
