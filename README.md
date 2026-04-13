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

### EC2 clone (canonical GitHub repo)

Repository: **[github.com/deniel-nankov/Kalshi_Research_Project](https://github.com/deniel-nankov/Kalshi_Research_Project)**.

**Option A — first-run script from GitHub (fresh box, as root):** clones to `/opt/kalshi-pipeline` by default.

```bash
curl -fsSL https://raw.githubusercontent.com/deniel-nankov/Kalshi_Research_Project/main/infra/aws/ec2-first-run.sh | sudo bash
```

Override clone URL or branch only if you use a fork: `sudo GIT_URL=... GIT_BRANCH=main bash ec2-first-run.sh`.

**Option B — manual clone as `ubuntu`:**

```bash
sudo mkdir -p /opt && sudo chown ubuntu:ubuntu /opt
cd /opt
git clone https://github.com/deniel-nankov/Kalshi_Research_Project.git kalshi-pipeline
cd kalshi-pipeline
```

### AWS Tier 2 — graphs and snapshots (simple explanation)

**Tier 2** means: after the daily health JSON is written, a short script sends the important numbers to **Amazon CloudWatch** so you can open a chart anytime (orphan tickers, duplicate rows, pass/warn/fail counts, etc.). Optionally it can also copy the health file plus a **dataset stats** text file to **S3** each run for a dated history.

On the server: `infra/aws/bootstrap.sh` installs the **AWS CLI** (`aws`) for this script; run `sudo bash infra/aws/install-systemd.sh` (enables `kalshi-observability.timer` a few minutes after the daily health run). Attach an IAM role to the EC2 instance with `cloudwatch:PutMetricData` and, if you use it, `s3:PutObject` on your prefix. Edit `/etc/kalshi/observability.env` from `infra/aws/observability.env.example`. Try locally: `uv run python scripts/publish_tier2_observability.py --dry-run`.

If **`aws`** is missing on Ubuntu ( **`apt install awscli`** has no candidate), run **`sudo bash infra/aws/install-aws-cli-v2.sh`** from the repo (or re-run **`sudo bash infra/aws/bootstrap.sh`** on a fresh host). If **`uv`** is missing at **`/usr/local/bin/uv`**, run **`sudo bash infra/aws/install-uv.sh`** (the forward and verified-S3 paths expect it there).

The same install enables **`kalshi-s3-verified-sync.timer`** (daily **07:00** UTC): full **`data/kalshi/`** upload runs only after the institutional gate passes. Until **`ENABLE_KALSHI_S3_VERIFIED_SYNC=1`** and **`S3_KALSHI_URI`** are set in **`/etc/kalshi/s3-verified-sync.env`** (from `infra/aws/s3-verified-sync.env.example`), the unit logs a skip and exits **0**. Grant the instance role **`s3:PutObject`** / sync permissions on that URI. Smoke test: **`sudo systemctl start kalshi-s3-verified-sync.service`** then **`journalctl -u kalshi-s3-verified-sync.service -n 200`**.

### Pre-deployment dry run (before EC2 or S3)

Run **`./infra/aws/deploy_dry_run.sh`** from the repo root before `install-systemd`, real `update_forward`, or **`CONFIRM_SYNC=1`** S3 upload. It checks shell syntax, **preflight** Parquet, **`update_forward.py --dry-run`** (if `.env` exists), **strict health** (fails only on FAIL), forward audit, duplicate preflight, optional orphan audit, Tier 2 **CloudWatch dry-run**, and **`aws s3 sync --dryrun`** when **`S3_KALSHI_URI`** and the AWS CLI are available. Use **`SKIP_SLOW=1`** to skip the orphan audit. Use **`STRICT_RELEASE=1`** to also run the full **`institutional_data_release.sh`** gate. Writes **`data/kalshi/state/health_report_deploy_dry_run.json`**.

### Planned maintenance (dry-run, then execute)

Use **`scripts/institutional_maintenance.sh`** for a documented window: **`--dry-run`** runs the same institutional validation as release (including orphan audit) and only *prints* the `systemctl stop/start` lines — no writers stopped, no dedupe. **`--execute`** (as **root**) stops `kalshi-*.timer` services, runs `institutional_data_release.sh` with **`APPLY_REPAIR=1`**, then restarts timers; requires **`CONFIRM_MAINTENANCE=I_ACCEPT_DOWNTIME`**. Optional **`RUN_MARKETS_CANONICAL=1`** runs `fix_forward_markets_dedupe.py --yes --exclude-historical` after dedupe. Shell syntax is checked in CI (`bash -n`).

### Institutional ops console (terminal + AWS)

**`scripts/institutional_ops_console.py`** refreshes a Rich dashboard (health summary, checkpoints, latest `state/runs` JSON, `kalshi` systemd timers when present) and writes **`data/kalshi/state/ops_snapshot.json`** each refresh. Use **`--once`** for a non-interactive snapshot; use **`--daemon --interval SEC`** for headless mode (used by **`kalshi-ops-console.service`** on EC2 after `sudo bash infra/aws/install-systemd.sh`). Edit **`/etc/kalshi/ops-console.env`** from `infra/aws/ops-console.env.example`. Tier 2 can upload the snapshot if **`UPLOAD_OPS_SNAPSHOT=1`** (see `infra/aws/observability.env.example`); for log streaming see **`infra/aws/CLOUDWATCH_OPS_LOGS.txt`**.

### Optional self-heal on WARN (limited)

By default **nothing** repairs the dataset when `validate_data_health.py` reports WARN - you run fix scripts or `infra/aws/server-data-perfection.sh` yourself. **Orphan tickers** (trades without matching market rows) can be healed a little at a time: set `AUTO_HEAL_ORPHANS_MAX` in `/etc/kalshi/auto-heal.env` and run `sudo bash infra/aws/install-auto-heal-systemd.sh` to enable a **weekly** `kalshi-auto-heal.timer` that calls `scripts/auto_heal_guarded.py` then `fix_orphan_tickers.py` with a checkpoint. **`AUTO_HEAL_ORPHANS_MAX=0` stays the default** (no-op). **Duplicate markets / dedupe** is not auto-run while forward ingestion is active; use a maintenance window and `server-data-perfection.sh repair` (see `docs/DATA_REPAIR.md`).

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
| Pre-deploy dry run (no writes / no S3 upload) | `./infra/aws/deploy_dry_run.sh` |
| Upload to S3 only after strict health gate passes | `./scripts/sync_verified_dataset_to_s3.sh` (set `S3_KALSHI_URI`) |
| EC2: daily verified S3 upload (systemd) | Edit `/etc/kalshi/s3-verified-sync.env`, then `sudo systemctl start kalshi-s3-verified-sync.service` (timer: `kalshi-s3-verified-sync.timer`) |
| Planned maintenance rehearsal | `./scripts/institutional_maintenance.sh --dry-run` |
| Ops terminal / headless snapshot | `uv run python scripts/institutional_ops_console.py` |

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
