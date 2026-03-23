# Getting your data up to “now” (e.g. March 2026)

## Why your numbers didn’t change after the long run

The run that fetched **~57 million** rows was **killed** (exit code 137) **before** it could:

1. **Write** the new rows to Parquet under `forward_trades/` and `forward_markets/`
2. **Update** `data/kalshi/state/forward_checkpoint.json`

So those 57M rows existed only **in memory**; when the process was killed, **nothing was saved**. The checkpoint stayed at **2025-03-12** and the forward dataset on disk stayed the same. **Data is only updated when the script exits normally** after writing and checkpoint update.

---

## How to get data through March 17, 2026 (or “now”)

### Option A: One full run (simplest, but long)

1. **Remove stale lock** (only if a previous run was killed):
   ```bash
   rm -f data/kalshi/state/forward_ingestion.lock
   ```

2. **Run forward ingestion to “now”** and **leave it running until it finishes** (can take **hours** for ~1 year of data):
   ```bash
   uv run python scripts/update_forward.py
   ```

3. When it completes, it will have:
   - Written new Parquet under `forward_trades/` and `forward_markets/`
   - Advanced the checkpoint to the current time

4. Re-run stats to see updated totals:
   ```bash
   uv run python scripts/data_stats.py
   uv run python scripts/compare_to_kalshidata.py
   ```

**Do not** stop the process (Ctrl+C or kill) if you want the new data saved.

---

## Data quality: duplicates & orphans (after big runs)

See **[DATA_REPAIR.md](DATA_REPAIR.md)** for live-safe dedupe of forward trades/markets and optional orphan backfill **without stopping** a running `update_forward.py` pull.

---

### Option B: Chunked runs (see progress, same result)

Run in steps with `--end-ts` so each step finishes and writes before the next:

1. Remove lock if needed: `rm -f data/kalshi/state/forward_ingestion.lock`

2. Run chunk 1 (to 2025-04-01), **wait until it exits**:
   ```bash
   uv run python scripts/update_forward.py --end-ts 1743465600
   ```

3. Chunk 2 (to 2025-07-01):
   ```bash
   uv run python scripts/update_forward.py --end-ts 1751328000
   ```

4. Chunk 3 (to 2026-01-01):
   ```bash
   uv run python scripts/update_forward.py --end-ts 1767225600
   ```

5. Chunk 4 (to “now”, e.g. March 18, 2026) — **no** `--end-ts`:
   ```bash
   uv run python scripts/update_forward.py
   ```

After each chunk, the checkpoint and forward Parquet are updated; your totals will increase step by step.

---

### Option C: Single run with `--chunk-days` (resumable, like download_historical)

Use **one** command; the script splits the window into N-day chunks and **writes Parquet and advances the checkpoint after each chunk**. If the process is killed, only the current chunk is lost; the next run resumes from the saved checkpoint.

#### Hard reset (stop run + clear lock + restart with fresh process)

Use this when you want a **new OS process** (RSS reset) and to pick up **new defaults** (e.g. smaller market slices):

1. **Stop** the running `update_forward.py` (terminal: **Ctrl+C**, or `kill <pid>` for the PID in your logs — **not** `kill -9` unless it won’t exit).
2. **Remove the lock** (only after the process has exited):
   ```bash
   rm -f data/kalshi/state/forward_ingestion.lock
   ```
3. **Restart** with your usual flags (checkpoint is unchanged; the next run continues from the saved watermark):
   ```bash
   uv run python scripts/update_forward.py --chunk-days 7 --progress-verbose --resource-log-seconds 5
   ```
   Default **`--market-slice-hours` is now 12** (was 24): shorter API windows per slice, **more slices per chunk**, often better progress visibility. Override with `--market-slice-hours 24` if you prefer.

**Normal flow** (lock file absent — first run or after reset above):

1. If a stale lock exists and no process is running: `rm -f data/kalshi/state/forward_ingestion.lock`

2. Run with N-day chunks (use 7 or 14 if you hit OOM) to “now”:
   ```bash
   uv run python scripts/update_forward.py --chunk-days 7
   ```
   Use `--chunk-days 14` for a balance; 30-day chunks can use a lot of RAM in busy months.

   If you want very detailed progress + live RAM/CPU telemetry:
   ```bash
   uv run python scripts/update_forward.py --chunk-days 7 --progress-verbose --resource-log-seconds 5
   ```

3. Optional: cap the run with `--end-ts` (e.g. backfill to 2025-04-01 first):
   ```bash
   uv run python scripts/update_forward.py --chunk-days 7 --end-ts 1743465600
   ```

Output files are named per chunk (e.g. `trades_{run_id}_chunk1.parquet`, `trades_{run_id}_chunk2.parquet`). Default `--chunk-days 0` keeps the original behavior (one big fetch, one write at the end).

#### Market fetch looks “slow” (e.g. `avg_rate` vs `recent_rate`)

- Logs show **`avg_rate`** (scanned ÷ time since this chunk’s market fetch started) and **`recent_rate`** (last 25 pages only). Early in a fetch, `avg_rate` looks high; after a long run, `avg_rate` drops toward the long-run average even if **`recent_rate`** is still healthy.
- **Smaller** `--chunk-days` (e.g. **3** instead of **7**) **shortens each market API window** (still plus 7-day lookback for close times), so fewer pages per chunk and **more frequent checkpoint writes** — often the practical way to reduce “one giant market pull.”
- **Restarting** the process does **not** “refresh the CPU” in a meaningful way. It can reset the HTTP client’s adaptive delay after **429** rate limits, but steady slowness is usually **many sequential pages** + **server/network latency**, not a stale CPU.

- **`--market-slice-hours H` (default 12)** splits each **market** fetch into multiple API windows on `close_ts` (`H` hours per window, **new cursor each slice**). **`--chunk-days` is unchanged** (trade chunks / checkpoint steps stay the same). Use **`--market-slice-hours 0`** for one long pagination (legacy behavior).

---

## Getting closer to official Kalshi Data stats

- Official numbers (e.g. **$52B volume**) include **all** platform data through **today**.
- Your numbers are low mainly because your **forward data stopped at March 2025** (and the run that had fetched more was killed before writing).
- Once you **successfully run** `update_forward.py` through **March 2026** and let it **finish**, your:
  - **Trade count** will grow by tens of millions
  - **Notional USD** will move toward the official range for the same period

Then run `compare_to_kalshidata.py` again to see your updated share of official volume.
