# Surveillance pipeline — operator runbook

This is the day-to-day reference for the on-call operator. For project
background and design, see `DESIGN.md`. For full architecture + ship
criteria, see `HANDOFF.md`.

---

## TL;DR — the one thing you need to know

```
Production detector:    volume_burst_v2 @ edge threshold 0.07
Live alerts:            surveillance/alerts/<dt>.jsonl
Daily HTML report:      surveillance/reports/daily_<dt>.html
Daily MD digest:        surveillance/reports/daily_digest.tsv

systemd timer:          kalshi-surveillance-daily.timer (04:30 UTC)
Service unit:           kalshi-surveillance-daily.service
```

The pipeline is fully automated. You only intervene when a daily run
fails (the timer's `OnFailure` will surface via `systemctl status`) or
when you want to drill into a specific alert.

---

## What gets produced every day

After the 04:30 UTC cron fires:

| Artifact | Location | Purpose |
|---|---|---|
| Per-trade alerts | `surveillance/alerts/YYYY-MM-DD.jsonl` | One JSON line per flagged trade — ingest into a downstream tool |
| Markdown daily report | `surveillance/reports/daily_YYYY-MM-DD.md` | Human review, top tables |
| HTML dashboard | `surveillance/reports/daily_YYYY-MM-DD.html` | Self-contained, shareable, charts embedded |
| Digest TSV | `surveillance/reports/daily_digest.tsv` | One line per day; machine-readable trend |
| Audit log | `state/surveillance_runs/daily_<run_id>.json` | Reproducibility record |
| Underlying parquets | `data/kalshi/derived/enriched_trades/dt=YYYY-MM-DD/` | Re-runnable substrate |

---

## How to read an alert JSONL line

Each line is one flagged trade. Key fields:

```json
{
  "trade_id":                  "...",       // Kalshi-side trade id
  "market_ticker":             "KXIPLGAME-26MAY15CSKLSG-CSK",
  "event_ticker":              "KXIPLGAME-26MAY15CSKLSG",
  "milestone_id":              "...",       // sports milestone, if any
  "milestone_category":        "Sports",
  "taker_side":                "no",        // "yes" or "no"
  "yes_price_dollars":         0.45,
  "no_price_dollars":          0.55,
  "count":                     150,
  "notional_usd":              67.50,
  "edge_volume_burst":         0.21,        // pwin_hat - taker_break_even_price
  "pwin_hat_used":             0.71,        // calibrated win prob
  "taker_break_even_price":    0.50,        // price the taker paid
  "cross_market_flag":         true,        // bucket-level confirmation
  "score_cross_market":        5.79,
  "cm_markets_touched":        2,
  "cm_burst_density":          125,
  "cm_notional_concentration": 76156,
  "created_time":              "2026-05-15T14:35:22Z",
  "was_taker_correct":         true,        // null until resolved
  "alert_version":             "v2_volume_burst@1.0.0+cm@1.0.0",
  "run_id":                    "..."
}
```

**Higher `edge_volume_burst` = stronger signal.** Cross-market boosted
alerts (where `cross_market_flag = true`) sort to the top of the file.

---

## Operations: common scenarios

### "The daily timer didn't fire"

```bash
# Check status
sudo systemctl status kalshi-surveillance-daily.timer
sudo systemctl status kalshi-surveillance-daily.service
sudo journalctl -u kalshi-surveillance-daily.service --since '24h ago'

# Re-fire manually (uses yesterday UTC by default)
sudo systemctl start kalshi-surveillance-daily.service

# Re-run for a specific date
cd /opt/kalshi-pipeline
.venv/bin/python3 scripts/surveillance_daily.py --dt 2026-05-15 --force
```

### "Forward data is stale"

The orchestrator triggers `kalshi-forward.service` if it isn't active
when it starts. If forward itself is broken:

```bash
sudo systemctl status kalshi-forward.service
sudo journalctl -u kalshi-forward.service --since '6h ago'
sudo systemctl start kalshi-forward.service
```

Forward is the upstream dependency for everything; if it's broken,
nothing downstream produces fresh data.

### "An alert looks suspicious — drill in"

```bash
# Pull the day's alerts
jq '.' surveillance/alerts/2026-05-15.jsonl | less

# Filter to a specific market
jq -c 'select(.market_ticker == "KXIPLGAME-26MAY15CSKLSG-CSK")' \
   surveillance/alerts/2026-05-15.jsonl

# Read the raw enriched trades for the same market
.venv/bin/python3 -c "
import polars as pl, glob
files = sorted(glob.glob('data/kalshi/derived/enriched_trades/dt=2026-05-15/*.parquet'))
df = pl.read_parquet(files).filter(pl.col('market_ticker') == 'KXIPLGAME-26MAY15CSKLSG-CSK')
print(df.sort('created_time').select(['created_time','taker_side','yes_price_dollars','count','notional_usd','volume_z_5min','was_taker_correct']).head(50))
"
```

### "Need to re-enrich a date" (e.g. after a fix)

```bash
cd /opt/kalshi-pipeline
.venv/bin/python3 scripts/enrich_trades.py             --dt 2026-05-15 --force
.venv/bin/python3 scripts/enrich_with_milestones.py    --dt 2026-05-15
.venv/bin/python3 scripts/enrich_with_milestones_pbp.py --dt 2026-05-15
.venv/bin/python3 scripts/enrich_with_lifecycle.py     --dt 2026-05-15
.venv/bin/python3 scripts/surveillance_daily.py        --dt 2026-05-15 --skip-forward --force
```

### "Want to re-validate / re-tune"

```bash
# Full v1 backtest on whatever's enriched
.venv/bin/python3 scripts/backtest_detectors.py --days 14

# v2 PnL-aware (train/test split)
.venv/bin/python3 scripts/detect_v2_pnl_aware.py --days 14

# Sweep edge thresholds
.venv/bin/python3 scripts/tune_v2_thresholds.py --days 14

# Cross-market backtest
.venv/bin/python3 scripts/detect_cross_market.py --days 14
```

Each writes a Markdown report under `surveillance/reports/`.

---

## Ship gates (what makes a detector "live")

A detector graduates to live alerts only when, on a clean train/test
holdout (default 70/30 by date):

1. **precision@100 ≥ 0.60** — top-100 flagged trades win > 60% of the time
2. **ROC AUC ≥ 0.55**       — score actually discriminates winners from losers
3. **mean PnL/trade > 0**   — paper-strategy is positive after the asymmetric prediction-market payoff structure

If any gate fails, the detector stays in the codebase as anomaly-
surfacing for manual review but does NOT contribute to live alerts.

As of the most recent calibration window:

```
volume_burst_v2  @ edge 0.07   SHIP
                   precision@100=0.89  AUC=0.605  PnL/trade=+$17.50  Sharpe=24.52

cross_market     (bucket-level) SHIP (as boost layer)
                   PnL/trade=+$1.19  Sharpe=5.97  on top of v2

whale            DO NOT SHIP — calibration inverted, no AUC,
                                useful only as anomaly surfacer

pre_play         DO NOT SHIP YET — live_data hasn't accumulated
                                    long enough to produce a holdout
                                    test set
```

Re-run `tune_v2_thresholds.py` weekly; if `volume_burst_v2` stops
shipping, lower the edge threshold in `PRODUCTION_EDGE_THRESHOLD`
(top of `surveillance_daily.py`) until it ships again, or open a
separate calibration retraining task.

---

## Hard constraints (do not violate)

1. **Never reference outcome columns in scoring.** Scores must depend
   only on data observable BEFORE the trade resolves. The backtest
   harness catches this — if a "fixed" detector starts showing AUC
   > 0.95 with no methodology change, you have leakage.
2. **Never claim alpha without holdout.** Single-day hit-rate edges
   are misleading because of asymmetric payoffs. Always validate via
   `detect_v2_pnl_aware.py` (which enforces train/test).
3. **Never disable the SHA-256 sidecar emission.** Every derived
   parquet must ship with a co-located `.sha256` so the audit chain
   is preserved. The institutional release gate enforces this.

---

## Contacts / escalation

This pipeline runs on EC2 instance `i-0db0f22e57ce23a1f` (r7i.2xlarge).
For credential or hardware issues, see `infra/aws/` for keypairs and
the deploy README.
