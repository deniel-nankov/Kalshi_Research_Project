# Data Validation Checklist

What to check for every column and across datasets to keep Kalshi data trustworthy. The script `scripts/validate_data_health.py` implements many of these; this doc is the full checklist and the “why” behind each check.

---

## 1. Trades dataset

### Per-column checks

| Column | Check | Rule | Health check |
|--------|--------|------|--------------|
| **trade_id** | Required, unique | Not null/empty; must be unique across all trade rows (historical + forward). | UNIQUENESS_TRADES |
| **ticker** | Required, referential | Not null/empty; should exist in markets (see cross-dataset). | SCHEMA_TRADES, REFERENTIAL_INTEGRITY |
| **taker_side** | Allowed values | `yes` or `no` (optional strict check). | — |
| **count** | Non-negative, optional fill | ≥ 0; we fill from `count_fp` when API sends 0. | COMPLETENESS, FORWARD_COUNT_COMPLETENESS |
| **count_fp** | Present for contract size | Non-empty for every trade (API populates 100%); used as source of truth for `count`. | — |
| **yes_price** | Range | 0–100 (cents). | VALUE_CONSTRAINTS_TRADES |
| **no_price** | Range + consistency | 0–100; should satisfy `yes_price + no_price = 100` for binary markets. | VALUE_CONSTRAINTS_TRADES |
| **price** | Range | 0.0–1.0 (fractional). | — |
| **created_time** | Required, parseable, not future | Not null/empty; must parse as timestamp; should not be in the future. | TEMPORAL_TRADES |

Optional / informational: `yes_price_dollars`, `no_price_dollars` (string from API); forward-only: `_run_id`, `_ingested_at`, `_source`.

### Within-trades checks

- **No duplicate trade_ids** across historical and forward (same trade must not appear in both).  
  → BOUNDARY_OVERLAP
- **No big time skip** between last historical trade and first forward trade (forward should pick up within ~24h of historical end).  
  → BOUNDARY_GAP
- **Dense timeline** — no unexpectedly large gaps between consecutive trading days (e.g. trades to 7pm then jump to days later = possible missing data).  
  → TRADE_TIMELINE_DENSITY

---

## 2. Markets dataset

### Per-column checks

| Column | Check | Rule | Health check |
|--------|--------|------|--------------|
| **ticker** | Required | Not null/empty. | SCHEMA_MARKETS |
| **close_time** / **created_time** | Key for uniqueness | Market key = `(ticker, close_time)` or `(ticker, created_time)`; used for dedupe. | UNIQUENESS_MARKETS |
| **volume** | Non-negative, sanity | ≥ 0; max vs p99 not absurd (a few super-liquid markets are expected). | STATISTICAL_SANITY |
| **status** | Allowed values | e.g. `open`, `closed`, `active`, `finalized` (optional). | — |
| **yes_bid / yes_ask / no_bid / no_ask** | Range when present | 0–100 (cents) if non-null. | — |
| **created_time** | Present, parseable | Used in market key; should parse as timestamp. | SCHEMA_MARKETS |

Other columns (event_ticker, title, open_time, updated_time, etc.): optional presence/format as needed.

### Within-markets checks

- **No duplicate market keys** — each `(ticker, close_time)` (or fallback `created_time`) should appear at most once; duplicates can occur from overlapping runs and are deduped at write.  
  → UNIQUENESS_MARKETS

---

## 3. Cross-dataset checks

| Check | Rule | Health check |
|--------|------|--------------|
| **Every trade has a market** | Each `trades.ticker` should appear in `markets.ticker`. Some orphans are expected (historical lag, windowing, delisted markets). | REFERENTIAL_INTEGRITY |
| **No historical/forward trade overlap** | No `trade_id` may appear in both historical and forward data. | BOUNDARY_OVERLAP |
| **No boundary time skip** | First forward trade time should be within ~24h of last historical trade time (no multi-day gap). | BOUNDARY_GAP |
| **Trade timeline density** | No very large gaps between consecutive trading days (max gap between end-of-day and next-day start). | TRADE_TIMELINE_DENSITY |
| **Boundary vs API** | Max trade `created_time` should not exceed the API historical cutoff (`trades_created_ts`) when applicable. | BOUNDARY_ALIGNMENT |

---

## 4. Summary: what to check for each column

### Trades

- **trade_id** — required, unique globally (no dupes, no overlap historical vs forward).
- **ticker** — required; should exist in markets.
- **taker_side** — `yes` or `no`.
- **count** — ≥ 0; filled from `count_fp` when API sends 0.
- **count_fp** — non-empty (source of truth for contract size).
- **yes_price, no_price** — 0–100; yes + no = 100 for binary.
- **price** — 0.0–1.0.
- **created_time** — required, parseable, not future.

### Markets

- **ticker** — required.
- **close_time / created_time** — part of market key; no duplicate `(ticker, close_time)`.
- **volume** — ≥ 0; outlier check (max vs p99) is relaxed for headline markets.
- **status** — valid enum if you enforce it.
- Price fields (yes_bid, yes_ask, etc.) — 0–100 when present.

### Cross

- Trades reference only tickers that exist in markets (referential integrity).
- No duplicate trade_ids between historical and forward (boundary overlap).
- No big time skip between last historical and first forward trade (boundary gap).
- No large gaps in the trade timeline between consecutive days (density).
- Max trade time aligned with API cutoff when checked (boundary alignment).

---

## 5. How to run the checks

```bash
uv run python scripts/validate_data_health.py
uv run python scripts/validate_data_health.py --output data/kalshi/state/health_report.json
uv run python scripts/validate_data_health.py --strict   # exit 1 on any FAIL
```

The script reports PASS / WARN / FAIL per check; `--strict` fails the process if any check is FAIL.
