# Forward data repair (dedupe + orphans)

Use this when `validate_data_health.py` reports duplicate `trade_id`s / market keys or orphan tickers.

## Phased procedure (rigorous)

Do **not** skip preflight on a full corpus.

1. **Preflight (read-only, ~1–2 min on large data):** `uv run python scripts/run_institutional_data_repair.py` — confirms duplicate *estimates*, disk headroom, lock status. No Parquet writes.
2. **Optional deeper dry-run (heavy):** `uv run python scripts/dedupe_forward_trades.py --dry-run` — materializes a winner plan; long. Use when you need per-file detail before apply.
3. **Apply dedupe (rewrites forward Parquet; backups under `historical/forward_*_pre_dedupe_*`):** `uv run python scripts/run_institutional_data_repair.py --apply` — trades then markets, then `validate_forward_pipeline.py --skip-run`, then full health.
4. **Strict health gate:** add `--health-strict` so `validate_data_health.py` exits non-zero on any FAIL; JSON at `data/kalshi/state/health_report_post_repair.json` (override with `--health-output PATH`).
5. **REFERENTIAL_INTEGRITY WARN (orphans):** not fixed by dedupe. After keys are clean, use `fix_orphan_tickers.py` with `.env` API keys, checkpoint, and repeated `--max` batches (`docs/HEALTH_REPORT_WARNINGS_EXAMINED.md`).

**Windows console:** If Unicode banners error, use Git Bash or `chcp 65001`, or rely on the ASCII fallback in `terminal_report.py` (recent versions).

**Legacy / non-canonical files:** Canonical Kalshi data lives under `data/kalshi/historical/` (trades shards, `markets.parquet`, `forward_trades/`, `forward_markets/`). The small CSVs under `data/kalshi/raw/` are only used by the older `run_all_analyses.py` demo path; do **not** delete them unless you change that script. Optional legacy dirs `data/kalshi/trades/`, `data/kalshi/markets/`, `data/kalshi/incremental/` are read by the health validator **only if present** — remove only after confirming they are empty or duplicated and backups exist.

## One-command institutional flow (recommended)

High-visibility terminal output (banners, phases, disk/lock preflight):

```bash
# Fast preflight only — duplicate *estimates*, no rewrites
uv run python scripts/run_institutional_data_repair.py

# Full dedupe + validators (long on large data)
uv run python scripts/run_institutional_data_repair.py --apply

# Same, but fail the run if health has any FAIL (--strict) and save JSON artifact
uv run python scripts/run_institutional_data_repair.py --apply --health-strict

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

### OutOfMemory on `rewrite_file_1` (per-file COPY after `trade_winners` built)

The window query may succeed at `--memory-limit-gb 6`, but **each Parquet COPY** can use extra RAM (buffers + compression). The dedupe script now **raises `memory_limit` and sets `threads=1` for the COPY phase** (default export target **max(12 GiB, 2× window limit)**), and defaults to **Snappy** compression instead of ZSTD for exports.

If you still OOM: `uv run python scripts/dedupe_forward_trades.py --yes --ignore-lock --export-memory-limit-gb 40` (or higher on a 64 GiB machine), or `--copy-compression uncompressed` (larger files, lower CPU/RAM for codec). The script avoids `COUNT(*)` per file (uses `EXISTS`) so the first file does not scan hundreds of millions of rows just to get a count.

To avoid the **connect-time 6 GiB** cap during COPY, use **`--no-export-memory-limit`** (sets a **high** `memory_limit` for export, default **52 GiB** via `SET`, or env **`KALSHI_DEDUPE_EXPORT_GIB`**). Plain `RESET memory_limit` does **not** override the limit from `duckdb.connect(config=...)`.

If materialization fails after using **most** of `max_temp_directory_size` (e.g. **30 GiB / 32 GiB**), DuckDB often needs **more temp headroom** *and* a **lower in-memory budget** so it spills earlier:

```bash
uv run python scripts/dedupe_forward_trades.py --yes --ignore-lock \
  --reserve-gib 1 --memory-limit-gb 4 --threads 2
```

- **`--memory-limit-gb`** sets DuckDB’s `memory_limit` at connect time (default **6** in scripts).
- **`--reserve-gib`** (default **1.5**) controls how much free disk is left when sizing temp; smaller → larger spill cap (riskier if the disk fills).

## Optional: historical/forward overlap

`fix_forward_markets_dedupe.py` (whole-directory replace) can drop forward rows that duplicate **historical** `markets.parquet` keys. Run only during a **maintenance window** (not while replacing the whole `forward_markets` tree during concurrent ingestion without understanding the risk).
