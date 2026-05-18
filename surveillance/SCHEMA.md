# `enriched_trades` schema

One row per Kalshi trade, augmented with per-market percentile features,
outcome labels, milestone/pbp linkage, and lifecycle context. Drives all
six surveillance detectors.

Source: `forward_trades` (base) + joins from `forward_markets`,
`forward_milestones`, `forward_live_data`, `forward_lifecycle_ws`,
`forward_tickers_ws`.

Partition: `dt=YYYY-MM-DD` (date of trade.created_time, UTC).
Compression: zstd. Co-located SHA-256 sidecar.

---

## Columns (38 total)

### Core trade identity
| Column | Type | Description |
|---|---|---|
| `trade_id` | string | Kalshi trade id (PK) |
| `market_ticker` | string | Full market ticker (e.g. `KXNFLGAME-26JAN18LACHI-CHI`) |
| `event_ticker` | string | Event ticker (parent of market) |
| `series_ticker` | string | Series ticker (derived from event prefix) |
| `created_time` | timestamp | Trade timestamp (microsecond UTC) |
| `created_time_unix` | int64 | Trade timestamp as unix sec (faster joins) |

### Trade economics
| Column | Type | Description |
|---|---|---|
| `taker_side` | string | "yes" or "no" — who initiated |
| `yes_price_dollars` | float | Price as fraction of $1 contract |
| `no_price_dollars` | float | Complement |
| `count` | int32 | Contracts traded |
| `notional_usd` | float | `count * yes_price_dollars` (taker side notional) |
| `notional_taker_usd` | float | Notional in the direction the taker actually took |

### Size percentile (whale signal)
| Column | Type | Description |
|---|---|---|
| `size_pct_in_market_90d` | float | Where this trade's `notional_usd` sits in the rolling 90-day distribution of trades for THIS market (0-1) |
| `size_pct_in_category_24h` | float | Same vs. all trades in the same category in the last 24h |
| `size_pct_global_24h` | float | Same vs. all trades globally in last 24h |
| `is_whale` | bool | `size_pct_global_24h >= 0.99` |

### Temporal context
| Column | Type | Description |
|---|---|---|
| `time_to_close_seconds` | int64 | `market.close_time - trade.created_time` |
| `time_to_resolution_seconds` | int64 | `market.settlement_ts - trade.created_time` (null until resolved) |
| `hour_of_day_utc` | int8 | 0-23 |
| `day_of_week_utc` | int8 | 0-6 (Mon=0) |
| `is_market_hours_us` | bool | 13:30-21:00 UTC ≈ 9:30am-5pm ET |

### Outcome (post-resolution only)
| Column | Type | Description |
|---|---|---|
| `market_status` | string | open / closed / settled / resolved |
| `market_result` | string | "yes" / "no" / "" |
| `winning_side` | string | "yes" / "no" / "" (resolved markets only) |
| `was_taker_correct` | bool | True if taker_side == winning_side. Null if unresolved. |
| `taker_pnl_per_contract_dollars` | float | (1 - yes_price_dollars) if taker on winning side else -yes_price_dollars |

### Liquidity / quote context (best-effort, requires tickers_ws join)
| Column | Type | Description |
|---|---|---|
| `last_quoted_spread_cents` | float | Spread at most-recent ticker_ws snapshot ≤ trade time |
| `best_bid_size_at_fill` | int32 | Size at best bid |
| `best_ask_size_at_fill` | int32 | Size at best ask |
| `implied_price_jump_cents` | float | `100 * (yes_price_dollars - last_price_before_trade)` |

### Volume burst signal
| Column | Type | Description |
|---|---|---|
| `volume_z_5min` | float | z-score of this market's 5-min trade volume vs. its 30-day baseline |

### Milestone link (sports / event-time signal)
| Column | Type | Description |
|---|---|---|
| `milestone_id` | string | Matching milestone (via `primary_event_tickers ⊃ event_ticker`) |
| `milestone_type` | string | e.g. `football_game`, `tennis_tournament_singles`, `political_race` |
| `milestone_category` | string | Sports / Esports / Elections / Crypto / etc. |

### Pre-play alpha signal (the high-value one)
| Column | Type | Description |
|---|---|---|
| `seconds_to_next_play_event` | int64 | Time from trade to the next pbp event in `forward_live_data` for the linked milestone. Null if no milestone or no future pbp event captured. |
| `next_play_event_type` | string | Best-effort label of the next pbp event |
| `in_pre_play_window` | bool | True if `0 < seconds_to_next_play_event <= 300` |

### Provenance / audit
| Column | Type | Description |
|---|---|---|
| `enrichment_run_id` | string | Run that produced this row (matches `state/enrichment_runs/<run_id>.json`) |
| `enrichment_version` | string | e.g. `enrich_trades@1.0.0` |
| `enriched_at_unix` | int64 | When enrichment ran |

---

## Computation order (4 enrichment stages)

Each stage reads/writes the same parquet partition, so a re-run of any single
stage doesn't require re-running the others.

1. **`enrich_trades.py`** — Core trade economics, size percentiles, temporal,
   outcome labels. Pure DataFrame work, no external joins.

2. **`enrich_with_milestones.py`** — Join `event_ticker → milestone_id` via
   `forward_milestones.primary_event_tickers`. Populates milestone_*.

3. **`enrich_with_lifecycle.py`** — Join via `lifecycle_ws` to fill
   `time_to_resolution_seconds` more precisely than `forward_markets`
   alone (lifecycle WS reports resolution to the second; markets table
   updates on next poll).

4. **`enrich_with_milestones_pbp.py`** — For trades with a milestone, find
   the NEXT pbp event in `forward_live_data` after the trade timestamp.
   This is the most expensive join (per-trade nearest-future-event lookup).

5. **`enrich_with_forecast.py`** *(deferred)* — Populates forecast_deviation
   columns once `forward_forecast_percentile` has data (currently 0 rows
   because Kalshi only publishes percentile distributions for macro series).

---

## Scale notes

- Forward trades: 470 M rows globally; ~3 M new trades/day.
- For development we'll build the pipeline against a **rolling 30-day
  window** (~90 M trades) then expand to full history once the detectors
  are calibrated.
- Polars + lazy parquet scans + per-dt partition writes keeps peak memory
  bounded (~5-10 GB on r7i.2xlarge).

---

## Output paths

```
data/kalshi/derived/enriched_trades/dt=YYYY-MM-DD/<run_id>.parquet
data/kalshi/derived/enriched_trades/dt=YYYY-MM-DD/<run_id>.parquet.sha256
data/kalshi/state/enrichment_runs/<run_id>.json   (audit log per stage)
```
