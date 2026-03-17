#!/usr/bin/env python3
"""
End-to-end validator for forward ingestion.

What it checks:
  1) Local historical max timestamps vs API historical cutoff
  2) Optional execution of scripts/update_forward.py
  3) Duplicate/overlap audits for forward trades and forward markets

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

# Ensure project root on path when run as script
_SCRIPT_ROOT = Path(__file__).resolve().parents[1]
if str(_SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_ROOT))

from src.kalshi_forward.paths import (
    CHECKPOINT_FILE,
    CUTOFF_URL,
    FORWARD_MARKETS_GLOB,
    FORWARD_TRADES_GLOB,
    HISTORICAL_CHECKPOINT_FILE,
    HISTORICAL_MARKETS_FILE,
    HISTORICAL_TRADES_GLOB,
    LEGACY_FORWARD_MARKETS_GLOB,
    LEGACY_FORWARD_TRADES_GLOB,
    PROJECT_ROOT,
)

CUTOFF_TIMEOUT_SECONDS = 30
UPDATE_SCRIPT = PROJECT_ROOT / "scripts" / "update_forward.py"


def fetch_cutoff() -> dict:
    req = urllib.request.Request(CUTOFF_URL)
    with urllib.request.urlopen(req, timeout=CUTOFF_TIMEOUT_SECONDS) as resp:
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
    proc = subprocess.run(cmd, cwd=PROJECT_ROOT)
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
    try:
        cutoff = fetch_cutoff()
    except Exception as exc:
        failures.append(f"Failed to fetch cutoff: {exc}")
        cutoff = {}
    if cutoff:
        print(f"       trades_created_ts: {cutoff.get('trades_created_ts', '')}")
        print(f"       orders_updated_ts: {cutoff.get('orders_updated_ts', '')}")
        print(f"       market_settled_ts: {cutoff.get('market_settled_ts', '')}")

    if not HISTORICAL_MARKETS_FILE.exists() or not has_glob(HISTORICAL_TRADES_GLOB):
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
                f"SELECT MAX(TRY_CAST(created_time AS TIMESTAMP)) FROM '{HISTORICAL_TRADES_GLOB}'",
            )
            max_market_created = ts_value(
                con,
                f"SELECT MAX(TRY_CAST(created_time AS TIMESTAMP)) FROM '{HISTORICAL_MARKETS_FILE}'",
            )
            max_market_updated = ts_value(
                con,
                f"SELECT MAX(TRY_CAST(updated_time AS TIMESTAMP)) FROM '{HISTORICAL_MARKETS_FILE}'",
            )
            cutoff_trade_ts = ts_value(con, "SELECT TRY_CAST(? AS TIMESTAMP)", [cutoff.get("trades_created_ts", "")])

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

    if not args.skip_run and not failures:
        rc = run_forward_update(
            max_trade_pages=args.max_trade_pages,
            max_market_pages=args.max_market_pages,
            historical_only=args.historical_only,
        )
        if rc != 0:
            failures.append(f"update_forward.py exited with status {rc}")

    print("\n[STEP] Auditing forward trades: duplicates and overlap with historical...")
    trade_glob = FORWARD_TRADES_GLOB if has_glob(FORWARD_TRADES_GLOB) else (LEGACY_FORWARD_TRADES_GLOB if has_glob(LEGACY_FORWARD_TRADES_GLOB) else None)
    if trade_glob is None:
        print("       No forward trade parquet found (nothing to audit yet).")
    else:
        con = duckdb.connect()
        try:
            incr_total = ts_value(con, f"SELECT COUNT(*) FROM '{trade_glob}'")
            incr_distinct = ts_value(con, f"SELECT COUNT(DISTINCT trade_id) FROM '{trade_glob}'")
            incr_dupes = incr_total - incr_distinct
            overlap = ts_value(
                con,
                f"""
                SELECT COUNT(*)
                FROM (
                    SELECT DISTINCT trade_id FROM '{HISTORICAL_TRADES_GLOB}'
                    WHERE trade_id IS NOT NULL AND trade_id <> ''
                ) h
                INNER JOIN (
                    SELECT DISTINCT trade_id FROM '{trade_glob}'
                    WHERE trade_id IS NOT NULL AND trade_id <> ''
                ) i
                ON h.trade_id = i.trade_id
                """,
            )

            print(f"       forward trade rows:           {incr_total:,}")
            print(f"       forward distinct trade_ids:    {incr_distinct:,}")
            print(f"       forward duplicate trade_ids:   {incr_dupes:,}")
            print(f"       overlap with historical ids:    {overlap:,}")

            if incr_dupes > 0:
                failures.append(f"Forward trade duplicates detected: {incr_dupes}")
            if overlap > 0:
                failures.append(f"Historical/forward trade_id overlap detected: {overlap}")
        finally:
            con.close()

    print("\n[STEP] Auditing forward markets: duplicates and overlap with historical...")
    market_glob = FORWARD_MARKETS_GLOB if has_glob(FORWARD_MARKETS_GLOB) else (LEGACY_FORWARD_MARKETS_GLOB if has_glob(LEGACY_FORWARD_MARKETS_GLOB) else None)
    if market_glob is None:
        print("       No forward market parquet found (nothing to audit yet).")
    else:
        con = duckdb.connect()
        try:
            # Market key: ticker + close_time (or created_time)
            incr_total = ts_value(con, f"SELECT COUNT(*) FROM '{market_glob}'")
            incr_distinct = ts_value(
                con,
                f"""
                SELECT COUNT(DISTINCT ticker || '|' || COALESCE(close_time, created_time, ''))
                FROM '{market_glob}'
                WHERE ticker IS NOT NULL AND ticker <> ''
                """,
            )
            incr_dupes = incr_total - incr_distinct
            overlap = ts_value(
                con,
                f"""
                SELECT COUNT(*)
                FROM (
                    SELECT DISTINCT ticker, COALESCE(close_time, created_time, '') AS key_ts
                    FROM '{HISTORICAL_MARKETS_FILE}'
                    WHERE ticker IS NOT NULL AND ticker <> ''
                ) h
                INNER JOIN (
                    SELECT DISTINCT ticker, COALESCE(close_time, created_time, '') AS key_ts
                    FROM '{market_glob}'
                    WHERE ticker IS NOT NULL AND ticker <> ''
                ) i
                ON h.ticker = i.ticker AND h.key_ts = i.key_ts
                """,
            )

            print(f"       forward market rows:          {incr_total:,}")
            print(f"       forward distinct market keys:  {incr_distinct:,}")
            print(f"       forward duplicate market keys: {incr_dupes:,}")
            print(f"       overlap with historical:       {overlap:,}")

            if incr_dupes > 0:
                failures.append(f"Forward market duplicates detected: {incr_dupes}")
            if overlap > 0:
                failures.append(f"Historical/forward market key overlap detected: {overlap}")
        finally:
            con.close()

    if CHECKPOINT_FILE.exists():
        cp = json.loads(CHECKPOINT_FILE.read_text())
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
