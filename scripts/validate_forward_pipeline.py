#!/usr/bin/env python3
"""
End-to-end validator for forward ingestion.

What it checks:
  1) Local historical max timestamps vs API historical cutoff
  2) Optional execution of scripts/update_forward.py
  3) Duplicate/overlap audits for incremental trades

Usage examples:
  uv run python scripts/validate_forward_pipeline.py
  uv run python scripts/validate_forward_pipeline.py --skip-run
  uv run python scripts/validate_forward_pipeline.py --strict
"""

from __future__ import annotations

import argparse
import glob
import json
import subprocess
import sys
import urllib.request
from pathlib import Path
from typing import Optional

import duckdb

ROOT = Path(__file__).resolve().parents[1]
HIST_TRADES_GLOB = ROOT / "data" / "kalshi" / "historical" / "trades" / "*.parquet"
HIST_MARKETS_FILE = ROOT / "data" / "kalshi" / "historical" / "markets.parquet"
HIST_CHECKPOINT = ROOT / "data" / "kalshi" / "historical" / ".checkpoint.json"
INCR_TRADES_GLOB = ROOT / "data" / "kalshi" / "incremental" / "trades" / "*" / "*.parquet"
FORWARD_CHECKPOINT = ROOT / "data" / "kalshi" / "state" / "forward_checkpoint.json"
UPDATE_SCRIPT = ROOT / "scripts" / "update_forward.py"

CUTOFF_URL = "https://api.elections.kalshi.com/trade-api/v2/historical/cutoff"


def fetch_cutoff() -> dict:
    with urllib.request.urlopen(CUTOFF_URL) as resp:
        return json.loads(resp.read().decode())


def ts_value(con: duckdb.DuckDBPyConnection, query: str, params: Optional[list] = None):
    return con.execute(query, params or []).fetchone()[0]


def has_glob(path: Path) -> bool:
    return len(glob.glob(str(path), recursive=True)) > 0


def run_forward_update(max_trade_pages: Optional[int], max_market_pages: Optional[int], historical_only: bool) -> int:
    cmd = [sys.executable, str(UPDATE_SCRIPT)]
    if historical_only:
        cmd.append("--historical-only")
    if max_trade_pages is not None:
        cmd.extend(["--max-trade-pages", str(max_trade_pages)])
    if max_market_pages is not None:
        cmd.extend(["--max-market-pages", str(max_market_pages)])

    print("\n[STEP] Running forward ingestion...")
    print("       " + " ".join(cmd))
    proc = subprocess.run(cmd, cwd=ROOT)
    return proc.returncode


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate forward ingestion pipeline")
    parser.add_argument("--skip-run", action="store_true", help="Skip executing update_forward")
    parser.add_argument("--max-trade-pages", type=int, default=1, help="Bounded trade pages for validation run")
    parser.add_argument("--max-market-pages", type=int, default=1, help="Bounded market pages for validation run")
    parser.add_argument("--historical-only", action="store_true", default=True, help="Use historical cutoff for bounded run")
    parser.add_argument("--strict", action="store_true", help="Return non-zero if any validation fails")
    args = parser.parse_args()

    failures: list[str] = []

    print("[STEP] Fetching API historical cutoff...")
    cutoff = fetch_cutoff()
    print(f"       trades_created_ts: {cutoff['trades_created_ts']}")
    print(f"       orders_updated_ts: {cutoff['orders_updated_ts']}")
    print(f"       market_settled_ts: {cutoff['market_settled_ts']}")

    if not HIST_MARKETS_FILE.exists() or not has_glob(HIST_TRADES_GLOB):
        msg = "Historical parquet dataset not found. Run historical backfill first."
        print(f"[WARN] {msg}")
        if args.strict:
            failures.append(msg)
    else:
        print("\n[STEP] Checking local historical boundary...")
        con = duckdb.connect()
        try:
            max_trade_created = ts_value(
                con,
                f"SELECT MAX(TRY_CAST(created_time AS TIMESTAMP)) FROM '{HIST_TRADES_GLOB}'",
            )
            max_market_created = ts_value(
                con,
                f"SELECT MAX(TRY_CAST(created_time AS TIMESTAMP)) FROM '{HIST_MARKETS_FILE}'",
            )
            max_market_updated = ts_value(
                con,
                f"SELECT MAX(TRY_CAST(updated_time AS TIMESTAMP)) FROM '{HIST_MARKETS_FILE}'",
            )
            cutoff_trade_ts = ts_value(con, "SELECT TRY_CAST(? AS TIMESTAMP)", [cutoff["trades_created_ts"]])

            print(f"       max historical trade created_time:  {max_trade_created}")
            print(f"       max historical market created_time: {max_market_created}")
            print(f"       max historical market updated_time: {max_market_updated}")
            print(f"       cutoff trades_created_ts:           {cutoff_trade_ts}")

            if max_trade_created and cutoff_trade_ts and max_trade_created > cutoff_trade_ts:
                failures.append("Historical trades exceed API cutoff")
            if max_market_created and cutoff_trade_ts and max_market_created > cutoff_trade_ts:
                failures.append("Historical market created_time exceeds API cutoff")
        finally:
            con.close()

    if not args.skip_run:
        rc = run_forward_update(
            max_trade_pages=args.max_trade_pages,
            max_market_pages=args.max_market_pages,
            historical_only=args.historical_only,
        )
        if rc != 0:
            failures.append(f"update_forward.py exited with status {rc}")

    print("\n[STEP] Auditing incremental duplicates/overlap...")
    if not has_glob(INCR_TRADES_GLOB):
        print("       No incremental trade parquet found (nothing to audit yet).")
    else:
        con = duckdb.connect()
        try:
            incr_total = ts_value(con, f"SELECT COUNT(*) FROM '{INCR_TRADES_GLOB}'")
            incr_distinct = ts_value(con, f"SELECT COUNT(DISTINCT trade_id) FROM '{INCR_TRADES_GLOB}'")
            incr_dupes = incr_total - incr_distinct
            overlap = ts_value(
                con,
                f"""
                SELECT COUNT(*)
                FROM (
                    SELECT DISTINCT trade_id FROM '{HIST_TRADES_GLOB}'
                    WHERE trade_id IS NOT NULL AND trade_id <> ''
                ) h
                INNER JOIN (
                    SELECT DISTINCT trade_id FROM '{INCR_TRADES_GLOB}'
                    WHERE trade_id IS NOT NULL AND trade_id <> ''
                ) i
                ON h.trade_id = i.trade_id
                """,
            )

            print(f"       incremental rows:             {incr_total:,}")
            print(f"       incremental distinct ids:     {incr_distinct:,}")
            print(f"       incremental duplicate ids:    {incr_dupes:,}")
            print(f"       overlap with historical ids:  {overlap:,}")

            if incr_dupes > 0:
                failures.append(f"Incremental duplicates detected: {incr_dupes}")
            if overlap > 0:
                failures.append(f"Historical/incremental trade_id overlap detected: {overlap}")
        finally:
            con.close()

    if FORWARD_CHECKPOINT.exists():
        cp = json.loads(FORWARD_CHECKPOINT.read_text())
        print("\n[STEP] Forward checkpoint snapshot")
        print(f"       watermark_trade_ts:  {cp.get('watermark_trade_ts')}")
        print(f"       watermark_market_ts: {cp.get('watermark_market_ts')}")
        print(f"       last_run_id:         {cp.get('last_successful_run_id')}")

    print("\n[RESULT]")
    if failures:
        for f in failures:
            print(f"  - FAIL: {f}")
        if args.strict:
            return 1
        print("  Validation completed with warnings/failures (non-strict mode).")
        return 0

    print("  PASS: forward ingestion validations succeeded.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
