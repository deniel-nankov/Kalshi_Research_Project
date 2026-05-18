#!/usr/bin/env python3
# ruff: noqa: E402
"""
Capture Kalshi milestones (sports games, elections, crypto events) for the
surveillance pipeline.

Why this script exists
----------------------
Kalshi's public API exposes a `/milestones` endpoint that maps real-world
events (a basketball game, a political race, a crypto target) to the markets
that resolve on their outcomes. Each milestone carries:
  - id, category (Sports / Elections / Esports / Crypto), type (football_game, …)
  - start_date / end_date
  - primary_event_tickers, related_event_tickers — direct links to our markets
  - last_updated_ts — pollable change marker

Pulling this data closes the most important data gap for the surveillance
project: we now have a structured map of "which markets are tied to which
real-world events" so detection logic can look for pre-event informed trades.

What it does
------------
1. Polls `GET /milestones` with `min_updated_ts` filter for incremental sync
2. Writes Parquet partitioned by dt= (date of update)
3. Co-located SHA-256 sidecar on every parquet (institutional gate)
4. Stable checkpoint at state/milestones_checkpoint.json so re-runs are cheap
5. Audit log entry at state/surveillance_runs/milestones_<run_id>.json

Usage
-----
  uv run python scripts/capture_milestones.py            # one-shot incremental
  uv run python scripts/capture_milestones.py --backfill # full history (slow)
  uv run python scripts/capture_milestones.py --category Sports

Designed to be invoked from a systemd timer (5-min cadence).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = PROJECT_ROOT / "data" / "kalshi"
HISTORICAL = DATA_ROOT / "historical"
STATE = DATA_ROOT / "state"
OUT_DIR = HISTORICAL / "forward_milestones"
CHECKPOINT = STATE / "milestones_checkpoint.json"
AUDIT_DIR = STATE / "surveillance_runs"

API_BASE = os.environ.get("KALSHI_API_BASE", "https://api.elections.kalshi.com/trade-api/v2")
PAGE_LIMIT = 500
DEFAULT_TIMEOUT_S = 30
MAX_RETRIES = 4
RETRY_BACKOFF_BASE = 1.0


# -------------------- helpers --------------------

def _log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"{ts}  {msg}", flush=True)


def _sha256_file(path: Path, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(chunk)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def _write_sha256_sidecar(parquet_path: Path) -> None:
    digest = _sha256_file(parquet_path)
    sidecar = parquet_path.with_suffix(parquet_path.suffix + ".sha256")
    tmp = sidecar.with_suffix(sidecar.suffix + ".tmp")
    tmp.write_text(f"{digest}  {parquet_path.name}\n", encoding="utf-8")
    os.replace(tmp, sidecar)


def _load_checkpoint() -> dict[str, Any]:
    if CHECKPOINT.exists():
        try:
            return json.loads(CHECKPOINT.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def _save_checkpoint(state: dict[str, Any]) -> None:
    CHECKPOINT.parent.mkdir(parents=True, exist_ok=True)
    tmp = CHECKPOINT.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, CHECKPOINT)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# -------------------- API fetch --------------------

def _fetch_page(client: httpx.Client, params: dict[str, Any]) -> dict[str, Any]:
    url = f"{API_BASE}/milestones"
    last_exc: Optional[Exception] = None
    for attempt in range(MAX_RETRIES):
        try:
            r = client.get(url, params=params, timeout=DEFAULT_TIMEOUT_S)
            if r.status_code == 429:
                sleep = RETRY_BACKOFF_BASE * (2 ** attempt)
                _log(f"WARN  429 rate-limited, sleeping {sleep:.1f}s")
                time.sleep(sleep)
                continue
            r.raise_for_status()
            return r.json()
        except (httpx.HTTPError, httpx.TimeoutException) as exc:
            last_exc = exc
            sleep = RETRY_BACKOFF_BASE * (2 ** attempt)
            _log(f"WARN  HTTP error ({exc!r}), sleeping {sleep:.1f}s")
            time.sleep(sleep)
    raise RuntimeError(f"all {MAX_RETRIES} retries exhausted: {last_exc!r}")


def _fetch_milestones_paginated(
    client: httpx.Client,
    min_updated_ts: Optional[int],
    category: Optional[str],
    minimum_start_date: Optional[str],
) -> list[dict[str, Any]]:
    """Page through /milestones until done. Returns one flat list."""
    rows: list[dict[str, Any]] = []
    cursor: Optional[str] = None
    page = 0
    while True:
        page += 1
        params: dict[str, Any] = {"limit": PAGE_LIMIT}
        if cursor:
            params["cursor"] = cursor
        if min_updated_ts is not None:
            params["min_updated_ts"] = int(min_updated_ts)
        if category:
            params["category"] = category
        if minimum_start_date:
            params["minimum_start_date"] = minimum_start_date

        body = _fetch_page(client, params)
        batch = body.get("milestones", []) or []
        rows.extend(batch)
        _log(f"  page {page}: {len(batch):,} milestones (cumulative {len(rows):,})")
        cursor = body.get("cursor")
        if not cursor or not batch:
            break
    return rows


# -------------------- row normalization --------------------

# Schema is intentionally explicit so we get stable Parquet types
SCHEMA = pa.schema([
    pa.field("id",                    pa.string()),
    pa.field("category",              pa.string()),
    pa.field("type",                  pa.string()),
    pa.field("title",                 pa.string()),
    pa.field("start_date",            pa.string()),
    pa.field("end_date",              pa.string()),
    pa.field("primary_event_tickers", pa.list_(pa.string())),
    pa.field("related_event_tickers", pa.list_(pa.string())),
    pa.field("notification_message",  pa.string()),
    pa.field("source_id",             pa.string()),
    pa.field("source_ids_json",       pa.string()),
    pa.field("details_json",          pa.string()),
    pa.field("last_updated_ts",       pa.string()),
    pa.field("last_updated_unix",     pa.int64()),
    pa.field("ingested_at_unix",      pa.int64()),
    pa.field("run_id",                pa.string()),
])


def _parse_unix(ts: Any) -> int:
    if ts is None or ts == "":
        return 0
    if isinstance(ts, (int, float)):
        return int(ts)
    try:
        # accept RFC3339 / ISO 8601
        return int(datetime.fromisoformat(str(ts).replace("Z", "+00:00")).timestamp())
    except Exception:
        return 0


def _normalize(rows: list[dict[str, Any]], run_id: str, ingested_at: int) -> pa.Table:
    cols: dict[str, list[Any]] = {f.name: [] for f in SCHEMA}
    for m in rows:
        cols["id"].append(m.get("id"))
        cols["category"].append(m.get("category"))
        cols["type"].append(m.get("type"))
        cols["title"].append(m.get("title"))
        cols["start_date"].append(m.get("start_date"))
        cols["end_date"].append(m.get("end_date"))
        cols["primary_event_tickers"].append(list(m.get("primary_event_tickers") or []))
        cols["related_event_tickers"].append(list(m.get("related_event_tickers") or []))
        cols["notification_message"].append(m.get("notification_message"))
        cols["source_id"].append(m.get("source_id"))
        cols["source_ids_json"].append(
            json.dumps(m.get("source_ids") or {}, sort_keys=True) if m.get("source_ids") else None
        )
        cols["details_json"].append(
            json.dumps(m.get("details") or {}, sort_keys=True) if m.get("details") else None
        )
        cols["last_updated_ts"].append(m.get("last_updated_ts"))
        cols["last_updated_unix"].append(_parse_unix(m.get("last_updated_ts")))
        cols["ingested_at_unix"].append(ingested_at)
        cols["run_id"].append(run_id)
    return pa.Table.from_pydict(cols, schema=SCHEMA)


# -------------------- audit log --------------------

def _write_audit_log(run_id: str, payload: dict[str, Any]) -> None:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    out = AUDIT_DIR / f"milestones_{run_id}.json"
    tmp = out.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, out)


# -------------------- main --------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="Capture Kalshi milestones")
    ap.add_argument("--backfill",     action="store_true",
                    help="Ignore checkpoint; pull everything (slow on first run)")
    ap.add_argument("--category",     type=str, default=None,
                    help="Filter to a single category (Sports / Elections / Esports / Crypto)")
    ap.add_argument("--minimum-start-date", type=str, default=None,
                    help="RFC3339 start date filter")
    ap.add_argument("--max-pages",    type=int, default=0,
                    help="Cap pages (0 = no cap, useful for testing)")
    ap.add_argument("--dry-run",      action="store_true",
                    help="Fetch + report but do not write parquet or update checkpoint")
    args = ap.parse_args()

    run_id = f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"
    ingested_at = int(time.time())
    started = time.time()

    checkpoint = _load_checkpoint()
    last_high_ts = 0 if args.backfill else int(checkpoint.get("max_last_updated_unix", 0))

    _log("=" * 72)
    _log(f"capture_milestones run_id={run_id}")
    _log(f"  base       = {API_BASE}")
    _log(f"  checkpoint = {CHECKPOINT}")
    _log(f"  mode       = {'BACKFILL' if args.backfill else 'INCREMENTAL'}")
    _log(f"  since_unix = {last_high_ts}  ({datetime.fromtimestamp(last_high_ts, tz=timezone.utc).isoformat() if last_high_ts else 'beginning of time'})")
    _log(f"  category   = {args.category or 'ALL'}")
    if args.dry_run:
        _log("  DRY-RUN — no writes")
    _log("=" * 72)

    with httpx.Client(timeout=DEFAULT_TIMEOUT_S) as client:
        rows = _fetch_milestones_paginated(
            client,
            min_updated_ts=last_high_ts if last_high_ts > 0 else None,
            category=args.category,
            minimum_start_date=args.minimum_start_date,
        )

    if not rows:
        _log("no new milestones — nothing to write")
        if not args.dry_run:
            # Touch the checkpoint so we record a heartbeat
            checkpoint["last_run_completed_at"] = _now_iso()
            checkpoint["last_run_id"] = run_id
            checkpoint["last_run_rows"] = 0
            _save_checkpoint(checkpoint)
        return 0

    # Compute new high-watermark
    new_max_ts = max((_parse_unix(m.get("last_updated_ts")) for m in rows), default=last_high_ts)

    # Group by date for partitioning
    by_date: dict[str, list[dict[str, Any]]] = {}
    for m in rows:
        ts = _parse_unix(m.get("last_updated_ts"))
        dt = datetime.fromtimestamp(ts or ingested_at, tz=timezone.utc).strftime("%Y-%m-%d")
        by_date.setdefault(dt, []).append(m)

    if args.dry_run:
        _log(f"DRY-RUN  would write {len(rows):,} rows across {len(by_date)} dt partitions")
        for dt, batch in sorted(by_date.items()):
            _log(f"  dt={dt}  {len(batch):,} rows")
        _log(f"new max_last_updated_unix would be {new_max_ts}")
        return 0

    written_paths: list[Path] = []
    rows_written = 0
    for dt, batch in sorted(by_date.items()):
        out_dir = OUT_DIR / f"dt={dt}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / f"milestones_{run_id}.parquet"
        table = _normalize(batch, run_id=run_id, ingested_at=ingested_at)
        tmp = out_file.with_suffix(".parquet.tmp")
        pq.write_table(table, tmp, compression="zstd")
        tmp.replace(out_file)
        _write_sha256_sidecar(out_file)
        written_paths.append(out_file)
        rows_written += table.num_rows
        _log(f"  wrote {table.num_rows:,} rows -> {out_file.name}")

    # Audit log
    payload = {
        "run_id": run_id,
        "started_at_utc": datetime.fromtimestamp(started, tz=timezone.utc).isoformat(),
        "completed_at_utc": _now_iso(),
        "duration_seconds": round(time.time() - started, 3),
        "mode": "backfill" if args.backfill else "incremental",
        "since_unix": last_high_ts,
        "new_max_last_updated_unix": new_max_ts,
        "category_filter": args.category,
        "minimum_start_date_filter": args.minimum_start_date,
        "milestones_fetched": len(rows),
        "milestones_written": rows_written,
        "files_written": [str(p.relative_to(PROJECT_ROOT)) for p in written_paths],
        "api_base": API_BASE,
    }
    _write_audit_log(run_id, payload)

    # Update checkpoint
    checkpoint["max_last_updated_unix"] = new_max_ts
    checkpoint["last_run_completed_at"] = _now_iso()
    checkpoint["last_run_id"] = run_id
    checkpoint["last_run_rows"] = rows_written
    _save_checkpoint(checkpoint)

    _log("=" * 72)
    _log(f"DONE rows={rows_written:,}  files={len(written_paths)}  duration={time.time()-started:.1f}s")
    _log(f"checkpoint advanced to last_updated_unix={new_max_ts}")
    _log(f"audit log -> {(AUDIT_DIR / f'milestones_{run_id}.json').relative_to(PROJECT_ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
