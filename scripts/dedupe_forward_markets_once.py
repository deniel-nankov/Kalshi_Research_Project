#!/usr/bin/env python3
"""
One-time cleanup: dedupe forward market Parquet files by (ticker, close_time).

Reads all forward (and legacy forward) market files, keeps one row per market key,
writes to data/kalshi/historical/forward_markets_dedupe/markets.parquet.

Usage:
  uv run python scripts/dedupe_forward_markets_once.py

Then optionally replace forward_markets with the deduped data:
  mv data/kalshi/historical/forward_markets data/kalshi/historical/forward_markets_backup
  mv data/kalshi/historical/forward_markets_dedupe data/kalshi/historical/forward_markets
  mkdir data/kalshi/historical/forward_markets
  mv data/kalshi/historical/forward_markets/* data/kalshi/historical/forward_markets/
  # Actually: deduped is a single file; health check expects dt=*/ structure. So we
  # write one file; for full compatibility we could write to dt=YYYY-MM-DD/markets.parquet
  # using min(close_time) date. Simplest: write one file to forward_markets_dedupe/markets.parquet.
  # Validator uses FORWARD_MARKETS_GLOB = FORWARD_MARKETS_DIR / "*" / "*.parquet", so it expects
  # at least one subdir. So write to forward_markets_dedupe/dt=1970-01-01/markets.parquet or
  # just document that this is for analytics use (query this file for canonical markets).
"""

from __future__ import annotations

import glob
import sys
from pathlib import Path

_SCRIPT_ROOT = Path(__file__).resolve().parents[1]
if str(_SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_ROOT))

import duckdb

from src.kalshi_forward.paths import (
    FORWARD_MARKETS_DIR,
    FORWARD_MARKETS_GLOB,
    LEGACY_FORWARD_MARKETS_GLOB,
    PROJECT_ROOT,
)


def main() -> int:
    patterns = [str(FORWARD_MARKETS_GLOB), str(LEGACY_FORWARD_MARKETS_GLOB)]
    files = []
    for p in patterns:
        files.extend(glob.glob(p))
    files = sorted(set(files))
    if not files:
        print("No forward market files found. Nothing to do.")
        return 0

    out_dir = PROJECT_ROOT / "data" / "kalshi" / "historical" / "forward_markets_dedupe"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "markets.parquet"

    # DuckDB: read all parquets, dedupe by (ticker, close_time) keeping first row
    con = duckdb.connect()
    try:
        union_sql = " UNION ALL ".join(
            f"SELECT * FROM read_parquet('{f}')" for f in files
        )
        con.execute(
            f"""
            COPY (
                SELECT * EXCLUDE (row_num)
                FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY ticker, COALESCE(close_time, '')) AS row_num
                    FROM ({union_sql})
                ) WHERE row_num = 1
            ) TO '{out_file}' (FORMAT PARQUET)
            """
        )
        total = con.execute(f"SELECT COUNT(*) FROM read_parquet('{out_file}')").fetchone()[0]
        print(f"Wrote {total:,} deduped markets to {out_file}")
        print("To use: point analytics at this file for canonical markets, or replace forward_markets (see script docstring).")
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
