# Forward data repair (dedupe + orphans)

Use this when `validate_data_health.py` reports duplicate `trade_id`s / market keys or orphan tickers.

## One-command institutional flow (recommended)

High-visibility terminal output (banners, phases, disk/lock preflight):

```bash
# Fast preflight only — duplicate *estimates*, no rewrites
uv run python scripts/run_institutional_data_repair.py

# Full dedupe + validators (long on large data)
uv run python scripts/run_institutional_data_repair.py --apply

# Apply + skip the slow full health script; still runs validate_forward_pipeline
uv run python scripts/run_institutional_data_repair.py --apply --skip-full-health

# Include orphan ticker count (distinct-query can take time)
uv run python scripts/run_institutional_data_repair.py --orphan-dry-run
```

## Safety while ingestion is running

`scripts/update_forward.py` only **creates new** Parquet files (never overwrites existing ones). The dedupe scripts below **snapshot** the list of files at startup and rewrite **only those files**. A pull that is writing **new** files after the snapshot is unaffected; run dedupe again later to fold in new files.

If `data/kalshi/state/forward_ingestion.lock` exists, the scripts **exit** unless you pass `--ignore-lock` (same safety model: they do not delete the lock or stop the pull).

## 1. Dedupe forward trades (duplicate `trade_id`)

```bash
uv run python scripts/dedupe_forward_trades.py --dry-run
uv run python scripts/dedupe_forward_trades.py --yes --ignore-lock   # if a long ingestion holds the lock
```

- Backs up each touched file under `data/kalshi/historical/forward_trades_pre_dedupe_<timestamp>/`.
- Keeps one row per `trade_id` (latest `created_time`, then filename).

## 2. Dedupe forward markets (duplicate market keys)

```bash
uv run python scripts/dedupe_forward_markets.py --dry-run
uv run python scripts/dedupe_forward_markets.py --yes --ignore-lock
```

- Backs up under `data/kalshi/historical/forward_markets_pre_dedupe_<timestamp>/`.
- Key: `(ticker, COALESCE(close_time, created_time, ''))`.

## 3. Orphan tickers (trades without a market row)

Fetch missing markets from the API (rate-limited). Large sets require **many** runs:

```bash
uv run python scripts/fix_orphan_tickers.py --dry-run
uv run python scripts/fix_orphan_tickers.py \
  --checkpoint data/kalshi/state/orphan_backfill_checkpoint.txt \
  --max 10000 \
  --delay 0.15
```

Re-run the same command until remaining orphans are 0 (checkpoint skips completed tickers). Use `--max` to cap each session.

## 4. Validate

```bash
uv run python scripts/validate_data_health.py
uv run python scripts/validate_forward_pipeline.py --skip-run
```

## DuckDB `OutOfMemoryException` / `max_temp_directory_size`

Large trade dedupe runs a window over **all** forward rows; DuckDB spills to disk. If you see:

`failed to offload data block … max_temp_directory_size … 17.4 GiB/17.4 GiB used`

the dedupe scripts now:

- Set **`temp_directory`** to `data/kalshi/state/duckdb_dedupe_spill/` (ignored by git)
- Set **`max_temp_directory_size`** from **free disk − reserve** (default reserve **5 GiB**)
- Use **`preserve_insertion_order=false`** and **4 threads** by default (less RAM)

Tuning:

```bash
# More headroom for macOS / other writers (smaller spill cap)
uv run python scripts/dedupe_forward_trades.py --yes --ignore-lock --reserve-gib 8

# Spill on an external volume with lots of free space
uv run python scripts/dedupe_forward_trades.py --yes --ignore-lock --temp-directory /Volumes/YourDisk/kalshi_duckdb_spill

# Hard cap (still capped by free space − 0.5 GiB)
uv run python scripts/dedupe_forward_trades.py --yes --ignore-lock --max-temp-gib 40
```

If spill still fills the disk, free space first or use a larger external `--temp-directory`.

If materialization fails after using **most** of `max_temp_directory_size` (e.g. **30 GiB / 32 GiB**), DuckDB often needs **more temp headroom** *and* a **lower in-memory budget** so it spills earlier:

```bash
uv run python scripts/dedupe_forward_trades.py --yes --ignore-lock \
  --reserve-gib 1 --memory-limit-gb 4 --threads 2
```

- **`--memory-limit-gb`** sets DuckDB’s `memory_limit` at connect time (default **6** in scripts).
- **`--reserve-gib`** (default **1.5**) controls how much free disk is left when sizing temp; smaller → larger spill cap (riskier if the disk fills).

## Optional: historical/forward overlap

`fix_forward_markets_dedupe.py` (whole-directory replace) can drop forward rows that duplicate **historical** `markets.parquet` keys. Run only during a **maintenance window** (not while replacing the whole `forward_markets` tree during concurrent ingestion without understanding the risk).
