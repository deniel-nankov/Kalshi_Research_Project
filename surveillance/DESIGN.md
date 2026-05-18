# Whale Detection & Insider-Trade Surveillance — Design

Branch: `surveillance/whale-detection`
Author: deniel-nankov
Status: Phase 0 — data capture design

---

## 1. Problem statement

Build an institutional-grade detection system for:
1. **Whale trades** — single trades or rapid clusters that imply one large actor
2. **Pre-event informed flow** — trades placed shortly before a market-moving event (goal scored, election called, resolution announced) that statistically beat the field
3. **Forecast-deviation alpha** — trades placed when traded price diverges sharply from Kalshi's official forecast (for numeric markets)
4. **Spoofing / layering** — order-book quoting patterns that look like manipulation
5. **Cross-market alpha leak** — informed flow propagating across correlated markets

Goal: every flagged trade carries a 0-1 score with a known precision (>80% on backtest holdout) and lands in a daily "smart-money board" report.

---

## 2. Hard constraint — what Kalshi does NOT expose

| Field / Endpoint | Status | Implication |
|---|---|---|
| Per-trade taker / maker identity | ❌ Not in public API | No per-account tracking |
| Public order_id in orderbook_delta | ❌ Only `client_order_id` for own orders | No per-order lifecycle tracking |
| Leaderboard / popular accounts / trader rankings | ❌ No such endpoint | No "follow the smart account" |
| Maker_order_id linkage in trade fills | ❌ Not exposed | Can't bind a trade to a specific resting order |

So we **cannot** literally "follow successful accounts." What we **can** do is build behavioral fingerprints from anonymous data — the same technique used by CFTC/exchange surveillance teams.

---

## 3. The data goldmine — what Kalshi DOES expose (and we weren't pulling)

| Endpoint / Channel | What it gives us | Why it matters |
|---|---|---|
| `GET /milestones` | Sports games, elections, crypto events with `primary_event_tickers` linking to markets | Maps Kalshi markets to real-world events |
| `GET /live_data/milestone/{id}` | **Play-by-play** for sports — goals, plays, periods, player stats | Event-time ground truth WITHOUT external sports feed |
| `GET /series/{series}/events/{ticker}/forecast_percentile_history` | Kalshi's **official forecast** at percentile points over time | Compare market price vs. Kalshi forecast → divergence signal |
| WS `market_lifecycle_v2` | `metadata_updated` events including exact **resolution timestamps** | Pre-resolution alpha detection (no guessing) |
| `GET /markets/orderbooks` (batch) | Multiple orderbooks per request | Efficient surveillance polling |
| `GET /events/multivariate` + `multivariate/event_collections/*` | Correlated multi-market collections | Cross-market alpha leak detection |
| `GET /exchange/announcements` | Suspensions, clarifications | Surveillance context (avoid false positives) |

---

## 4. New data captures we need

| Script | Endpoint | Cadence | Output dataset |
|---|---|---|---|
| `capture_milestones.py` | `GET /milestones?min_updated_ts=...` | 5 min | `forward_milestones/dt=*/...parquet` |
| `capture_live_data.py` | `GET /live_data/milestone/{id}` per active milestone | 30 sec | `forward_live_data/dt=*/...parquet` |
| `capture_forecast_percentile.py` | `GET /events/{ticker}/forecast_percentile_history` for numeric events | daily | `forward_forecast_percentile/dt=*/...parquet` |
| `capture_lifecycle_ws.py` | WS `market_lifecycle_v2` | continuous | `forward_lifecycle_ws/dt=*/...parquet` |

Each writes parquets with co-located SHA-256 sidecars (same institutional pattern as `forward_markets`).

---

## 5. Detection layer (Phase 2)

Six anonymous-data detectors, each producing a 0-1 score per trade/market:

### Detector 1 — Whale (size-based)
- Trigger: `notional_usd > p99` AND `size_percentile_market > 0.99`
- Score: $\text{rank}_\text{notional}$ × $\text{rank}_\text{size}$
- Output: per-trade

### Detector 2 — Pre-play alpha (timing-based, NEW)
- For each trade in a sports market, find nearest pbp event with `Δt = trade_ts − play_event_ts` in [−300s, 0s]
- For resolved markets, regress `Δt × winning_side` over the population
- Trades in pre-event windows that align with `winning_side` get higher scores
- This is the **textbook insider signature**

### Detector 3 — Forecast deviation alpha (numeric, NEW)
- For numeric events, at each trade timestamp, compute `forecast_deviation = (trade_price − forecast_p50) / forecast_iqr`
- Score = |deviation| scaled by recent regime volatility
- Flags trades placed when the market disagrees sharply with Kalshi's own forecast

### Detector 4 — Abnormal volume burst
- Per market, compute rolling 5-min volume + price change z-scores
- Flag windows where both z > 3
- All trades in those windows tagged

### Detector 5 — Spoofing / layering
- From `orderbook_delta`: identify quantity-level patterns
  - Large add → cancel within X seconds without fill (spoofing)
  - Sequential adds across price levels with same delta_fp signature (layering)
- Score per price-level-per-market-per-window

### Detector 6 — Cross-market alpha leak
- Build correlated-market graph from multivariate event collections
- For each correlated pair, compute lag-cross-correlation of mid-price moves
- Detect when one market leads another by abnormal lag (informed flow propagation)

---

## 6. Smart-money board (Phase 3)

Two daily reports:

**A. Per-market report**
- One row per market with >=1 flagged trade
- Columns: total flagged notional, # detectors fired, detector-firing pattern, outcome-consistent?

**B. Per-fingerprint cluster report**
- Cluster trades by behavioral fingerprint (size signature × time-of-day × market category × side bias)
- Rank clusters by:
  - Win rate on resolved markets
  - Total notional
  - Sharpe of implied strategy
  - Persistence (clusters present across multiple weeks)
- This is the closest thing to "successful accounts" we can produce

---

## 7. Backtest + validation (Phase 4)

For honest precision:

1. Holdout last 6 months of resolved markets
2. Re-run detection on each market using ONLY pre-resolution data
3. Compute precision@K, ROC AUC, calibration plots
4. Build a paper-trading strategy that follows the highest-scoring flagged side
5. Report Sharpe, max drawdown, hit rate

**Ship criteria:**
- Per-detector precision > 80% on holdout
- Paper-strategy Sharpe > 1.5 (after slippage assumption)
- ROC AUC > 0.75

If any detector misses these thresholds, it stays in the codebase but doesn't contribute to the live score.

---

## 8. Live monitoring (Phase 5)

- Daily cron at 04:00 UTC: re-run all detectors on prior 24h trades
- Write reports to `s3://.../surveillance/YYYY-MM-DD/`
- Threshold-based alerts: trades > Y score → write to `alerts.jsonl` (later: email/Slack)
- Streamlit dashboard for per-market drill-down

---

## 9. Timeline

| Phase | Calendar | Output |
|---|---|---|
| **Phase 0 — Data captures** | week 1 | 4 new datasets flowing to S3 |
| **Phase 1 — Enrichment pipeline** | week 2 | `enriched_trades` table with all features |
| **Phase 2 — Six detectors** | week 3 | per-trade scores |
| **Phase 3 — Smart-money board** | week 4 | daily reports |
| **Phase 4 — Backtest** | week 5 | precision report; detectors ship or stay sidelined |
| **Phase 5 — Live monitoring** | end of week 5 | daily cron live |

---

## 10. Institutional gates (every phase)

Every Phase X must produce, before moving on:
1. **Audit log** at `state/surveillance_runs/<phase>_<ts>.json` (counts, file list, version)
2. **SHA-256 sidecars** on every output parquet
3. **Schema doc** describing every field in derived tables
4. **Validation script** that re-checks key invariants (e.g. "every flagged trade has a non-null score")
5. **Reproducible**: full pipeline re-runnable from the audit log

This is non-negotiable. If a step skips any of these, it goes back.

---

## 11. Sources

- [Kalshi API Help Center](https://help.kalshi.com/en/articles/13823854-kalshi-api)
- [Kalshi Docs — Welcome](https://docs.kalshi.com/welcome)
- [Kalshi API — Get Milestones](https://docs.kalshi.com/api-reference/milestone/get-milestones)
- [Kalshi API — Get Live Data](https://docs.kalshi.com/api-reference/live-data/get-live-data)
- [Kalshi API — Forecast Percentile History](https://docs.kalshi.com/api-reference/events/get-event-forecast-percentile-history)
- [Kalshi WebSocket — Orderbook Updates](https://docs.kalshi.com/websockets/orderbook-updates)
- [Kalshi API Changelog](https://docs.kalshi.com/changelog)
