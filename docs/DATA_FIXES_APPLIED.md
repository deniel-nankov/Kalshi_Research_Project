# Data Fixes Applied (and How to Re-run / Prevent)

## 1. UNIQUENESS_MARKETS — Duplicate market keys (within forward)

**What we did:** Ran a one-time dedupe of **forward** markets by `(ticker, close_time)` and replaced the `forward_markets` directory with a single canonical set.

**Script:** `scripts/fix_forward_markets_dedupe.py`

**Steps (already executed):**
- Backed up `forward_markets` to `data/kalshi/historical/forward_markets_backup_<timestamp>`
- Deduped all forward market files → 27,726 unique rows (39 duplicates removed)
- Replaced `forward_markets` with `dt=canonical/markets.parquet`

**Re-run if needed:**
```bash
uv run python scripts/fix_forward_markets_dedupe.py --dry-run   # show what would happen
uv run python scripts/fix_forward_markets_dedupe.py --yes       # backup, dedupe, replace (no prompt)
```

**Fixing the “duplicate keys” WARN (historical + forward overlap):** Run with `--exclude-historical` so forward only keeps markets whose `(ticker, close_time)` are **not** in historical. The script also reads **legacy** forward markets (`data/kalshi/incremental/markets`), dedupes them with forward, and then **archives** that legacy directory so the validator no longer sees duplicate rows from it. After that, UNIQUENESS_MARKETS should pass.

```bash
uv run python scripts/fix_forward_markets_dedupe.py --yes --exclude-historical
```

**Prevention:** The main pipeline (`update_forward.py`) already:
- Dedupes within each API fetch (`seen_key_in_fetch`)
- Dedupes at write time against existing keys and within the batch  
New forward runs write only to `forward_markets` (not legacy). Keeping legacy archived avoids duplicate keys in the combined set.

---

## 2. REFERENTIAL_INTEGRITY — Orphan tickers (trades without markets)

**What we did:** Fetched missing markets for every orphan ticker via `GET /markets/{ticker}` and appended them to `forward_markets`.

**Script:** `scripts/fix_orphan_tickers.py`

**Steps (already executed):**
- Computed 2,859 orphan tickers (in trades but not in markets)
- Fetched each from API (200 OK for all 2,859)
- Wrote `forward_markets/dt=2026-03-16/markets_orphan_backfill_*.parquet` with 2,859 rows

**Re-run if new orphans appear:**
```bash
uv run python scripts/fix_orphan_tickers.py --dry-run   # list orphans only
uv run python scripts/fix_orphan_tickers.py             # fetch and append (progress every 100 + summary)
uv run python scripts/fix_orphan_tickers.py --delay 0.3 # slower API rate (default 0.2s)
```

**Prevention:** The main pipeline already uses `MARKET_LOOKBACK_DAYS = 7` so the market fetch window is 7 days before the trade window, reducing new orphans. If the health check reports new orphan tickers later, run `fix_orphan_tickers.py` again (no dry-run) to backfill them.

---

## Health report after fixes

- **REFERENTIAL_INTEGRITY:** PASS (all trade tickers exist in markets)
- **UNIQUENESS_MARKETS:** May still show a WARN with "Duplicate market keys: N" when the same market exists in both historical and forward; that is overlap, not within-forward duplication. Within-forward duplicates were removed.
- **Backups:** `data/kalshi/historical/forward_markets_backup_<timestamp>` holds the pre-dedupe forward markets if you need to restore.
