# Kalshi data pipeline

This repo pulls **historical** and **forward** (up-to-date) market and trade data from the Kalshi API, stores it in Parquet files, and gives you scripts to check that the data is healthy and to fix common issues. Everything is designed so that historical data is never overwritten and new data is appended in a clear, repeatable way.

---

## What you need

- **Python 3.9 or newer**
- A way to run the project: we use **[uv](https://docs.astral.sh/uv/)** (e.g. `uv run python scripts/...`). You can also use a normal virtualenv and `pip install -e .` if you prefer.

---

## Install

From the repo root:

```bash
uv sync
```

That installs the project and its dependencies. To run a script you do:

```bash
uv run python scripts/script_name.py
```

If you don't use uv, create a virtualenv, then:

```bash
pip install -e .
```

Then run scripts with `python scripts/script_name.py` (with the venv active).

---

## Where the data lives

All Kalshi data goes under one folder: **`data/kalshi/`**.

- **Historical** (one-time snapshot): `data/kalshi/historical/markets.parquet` and `data/kalshi/historical/trades/*.parquet`
- **Forward** (new data added over time): `data/kalshi/historical/forward_markets/` and `data/kalshi/historical/forward_trades/`
- **State** (checkpoints and health report): `data/kalshi/state/`

We never overwrite historical files. New data is only appended under the forward folders. The full layout and the reasons for it are described in [docs/KALSHI_DATA_PIPELINE_SUMMARY.md](docs/KALSHI_DATA_PIPELINE_SUMMARY.md).

---

## Data and state in this repo

**We keep everything in the repo** so you don’t lose data or state: Parquet files, checkpoint and health-report JSON under `data/kalshi/state/`, and any backup folders (e.g. `incremental_markets_backup_*`) created by the dedupe or fix scripts. Large files are tracked with **Git LFS** (see `.gitattributes`), so the repo stays usable while still containing the full dataset.

- **To refresh or rebuild data:** Run `download_historical.py` (one-time or when you want a full refresh), then run `update_forward.py` on a schedule to add new data. The repo will then have the latest you’ve pulled.
- **Backups:** Folders like `incremental_markets_backup_*` are intentional; they are created when we fix duplicate markets and are kept in the repo so we have a full record. You can remove old backups locally if you need space, but they are not ignored by git so you can commit them if you want everything in version control.

---

## Step 1: Pull historical data (one-time)

This downloads all markets and all trades up to the API’s “historical cutoff.” It can take a long time (for example, two days) because it hits the API for every market and its trades.

```bash
uv run python scripts/download_historical.py
```

- Output: `data/kalshi/historical/markets.parquet` and many files under `data/kalshi/historical/trades/`.
- You can stop and resume; progress is saved in a checkpoint file.
- To try it on a small set first (e.g. 100 markets):

  ```bash
  uv run python scripts/download_historical.py --test 100
  ```

---

## Step 2: Keep data up to date (forward runs)

After you have historical data, you run the forward script regularly so that new trades and markets are appended.

```bash
uv run python scripts/update_forward.py
```

- This reads the last run time from `data/kalshi/state/forward_checkpoint.json`, fetches only **new** trades and markets since then, and appends them to the forward folders. It then updates the checkpoint.
- Run it on a schedule (e.g. daily or hourly) so your dataset stays current.
- To catch up only to the API’s historical cutoff (no “live” data after that):

  ```bash
  uv run python scripts/update_forward.py --historical-only
  ```

If the health report later shows a **gap** between the last historical trade and the first forward trade, you can backfill that window using `--end-ts`; the exact steps are in [docs/HEALTH_REPORT_WARNINGS_EXAMINED.md](docs/HEALTH_REPORT_WARNINGS_EXAMINED.md).

---

## Step 3: Check that the data is healthy

Run the health checker to validate schema, uniqueness, referential integrity, time ranges, and more:

```bash
uv run python scripts/validate_data_health.py
```

To save the report to a file (e.g. for later inspection):

```bash
uv run python scripts/validate_data_health.py --output data/kalshi/state/health_report.json
```

To make the script exit with code 1 if any check **fails** (useful in CI):

```bash
uv run python scripts/validate_data_health.py --strict
```

If you see **warnings** (e.g. duplicate market keys, orphan tickers, or a gap between historical and forward), what they mean and what to do are explained in [docs/HEALTH_REPORT_WARNINGS_EXAMINED.md](docs/HEALTH_REPORT_WARNINGS_EXAMINED.md). There are also one-off fix scripts (e.g. for orphans or duplicate markets); when to run them is documented in the pipeline summary.

---

## Running tests

Tests live in **`tests/`**. They cover paths, forward update logic, the data validation checklist, and boundary/gap behaviour.

Run all tests:

```bash
uv run pytest tests/
```

Some tests are marked **slow** (they run the full health validator or need data). To skip those:

```bash
uv run pytest tests/ -m "not slow"
```

To run only the fast tests and then, when you have data, run the full suite:

```bash
uv run pytest tests/ -m "not slow"
uv run pytest tests/
```

We rely on these tests to ensure that the pipeline and validation behave correctly, so running them after changes is important.

---

## Quick reference: useful commands

| What you want to do | Command |
|---------------------|--------|
| Pull all historical data (one-time) | `uv run python scripts/download_historical.py` |
| Pull historical for 100 markets only (test) | `uv run python scripts/download_historical.py --test 100` |
| Update with new data (forward) | `uv run python scripts/update_forward.py` |
| Check data health | `uv run python scripts/validate_data_health.py` |
| Save health report to a file | `uv run python scripts/validate_data_health.py --output data/kalshi/state/health_report.json` |
| Inspect counts and date ranges | `uv run python scripts/inspect_kalshi_data.py` |
| Run all tests | `uv run pytest tests/` |
| Run tests without slow ones | `uv run pytest tests/ -m "not slow"` |

---

## Documentation

- **[docs/README.md](docs/README.md)** — Index of all pipeline docs (schemas, validation checklist, health warnings).
- **[docs/KALSHI_DATA_PIPELINE_SUMMARY.md](docs/KALSHI_DATA_PIPELINE_SUMMARY.md)** — The main document: how the pipeline works, what each script does and when to run it, problems we ran into and how we fixed them, and how we keep the dataset consistent. Start here if you want the full picture.

For health-report warnings and how to fix them (including backfilling a gap), see **[docs/HEALTH_REPORT_WARNINGS_EXAMINED.md](docs/HEALTH_REPORT_WARNINGS_EXAMINED.md)**.

To compare our numbers with **[Kalshi Data](https://www.kalshidata.com)** and save a snapshot:

```bash
uv run python scripts/data_stats.py | tee data/kalshi/state/dataset_stats_latest.txt
```

See **[docs/KALSHIDATA_COMPARISON.md](docs/KALSHIDATA_COMPARISON.md)** for definitions and a table to fill in with official stats.

**How far are you from official Kalshi Data?** Update `data/kalshi/state/kalshidata_baseline.json` with current numbers from [kalshidata.com](https://www.kalshidata.com), then run:

```bash
uv run python scripts/compare_to_kalshidata.py
```

It shows your **share of official volume (USD)** and explains which metrics you **cannot** compare directly (e.g. their “Total Trades”).
