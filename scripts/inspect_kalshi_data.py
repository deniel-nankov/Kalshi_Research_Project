#!/usr/bin/env python3
"""
Inspect Kalshi dataset: counts, date ranges, and sample rows.

Shows where data lives (historical vs forward) and how the pipeline
checks for "already in data" and correctness.
"""

from __future__ import annotations

import sys
from pathlib import Path

_SCRIPT_ROOT = Path(__file__).resolve().parents[1]
if str(_SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_ROOT))

import duckdb

from src.kalshi_forward.paths import (
    FORWARD_MARKETS_GLOB,
    FORWARD_TRADES_GLOB,
    HISTORICAL_MARKETS_FILE,
    HISTORICAL_TRADES_GLOB,
    LEGACY_FORWARD_MARKETS_GLOB,
    LEGACY_FORWARD_TRADES_GLOB,
)


def has_any(glob_path: Path) -> bool:
    import glob as glob_module
    return len(glob_module.glob(str(glob_path))) > 0


def main() -> None:
    con = duckdb.connect()

    print("=" * 72)
    print("KALSHI DATA INSPECTION")
    print("=" * 72)

    # ---- Historical ----
    print("\n--- HISTORICAL (backfill from download_historical.py) ---")
    if HISTORICAL_MARKETS_FILE.exists():
        n_m = con.execute(f"SELECT COUNT(*) FROM '{HISTORICAL_MARKETS_FILE}'").fetchone()[0]
        r_m = con.execute(
            f"SELECT MIN(created_time), MAX(created_time) FROM '{HISTORICAL_MARKETS_FILE}'"
        ).fetchone()
        print(f"  Markets:  {n_m:,} rows  |  created_time: {r_m[0]} .. {r_m[1]}")
    else:
        print("  Markets:  (no file)")
    if has_any(HISTORICAL_TRADES_GLOB):
        n_t = con.execute(f"SELECT COUNT(*) FROM '{HISTORICAL_TRADES_GLOB}'").fetchone()[0]
        r_t = con.execute(
            f"SELECT MIN(created_time), MAX(created_time) FROM '{HISTORICAL_TRADES_GLOB}'"
        ).fetchone()
        print(f"  Trades:   {n_t:,} rows  |  created_time: {r_t[0]} .. {r_t[1]}")
    else:
        print("  Trades:   (no files)")

    # ---- Forward (new location under historical/) ----
    print("\n--- FORWARD (incremental, under data/kalshi/historical/) ---")
    use_new_trades = has_any(FORWARD_TRADES_GLOB)
    use_legacy_trades = has_any(LEGACY_FORWARD_TRADES_GLOB)
    if use_new_trades:
        n = con.execute(f"SELECT COUNT(*) FROM '{FORWARD_TRADES_GLOB}'").fetchone()[0]
        r = con.execute(f"SELECT MIN(created_time), MAX(created_time) FROM '{FORWARD_TRADES_GLOB}'").fetchone()
        print(f"  Trades:   {n:,} rows  |  created_time: {r[0]} .. {r[1]}  [forward_trades/]")
    if use_legacy_trades:
        n = con.execute(f"SELECT COUNT(*) FROM '{LEGACY_FORWARD_TRADES_GLOB}'").fetchone()[0]
        r = con.execute(f"SELECT MIN(created_time), MAX(created_time) FROM '{LEGACY_FORWARD_TRADES_GLOB}'").fetchone()
        print(f"  Trades:   {n:,} rows  |  created_time: {r[0]} .. {r[1]}  [legacy incremental/]")
    if not use_new_trades and not use_legacy_trades:
        print("  Trades:   (none yet)")
    use_new_markets = has_any(FORWARD_MARKETS_GLOB)
    use_legacy_markets = has_any(LEGACY_FORWARD_MARKETS_GLOB)
    if use_new_markets:
        n = con.execute(f"SELECT COUNT(*) FROM '{FORWARD_MARKETS_GLOB}'").fetchone()[0]
        print(f"  Markets:  {n:,} rows  [forward_markets/]")
    if use_legacy_markets:
        n = con.execute(f"SELECT COUNT(*) FROM '{LEGACY_FORWARD_MARKETS_GLOB}'").fetchone()[0]
        print(f"  Markets:  {n:,} rows  [legacy incremental/]")
    if not use_new_markets and not use_legacy_markets:
        print("  Markets:  (none yet)")

    # ---- Combined totals (what analyses would see) ----
    print("\n--- COMBINED TOTALS (historical + forward) ---")
    if HISTORICAL_MARKETS_FILE.exists():
        hist_m = con.execute(f"SELECT COUNT(*) FROM '{HISTORICAL_MARKETS_FILE}'").fetchone()[0]
    else:
        hist_m = 0
    if use_new_markets:
        fwd_m = con.execute(f"SELECT COUNT(*) FROM '{FORWARD_MARKETS_GLOB}'").fetchone()[0]
    elif use_legacy_markets:
        fwd_m = con.execute(f"SELECT COUNT(*) FROM '{LEGACY_FORWARD_MARKETS_GLOB}'").fetchone()[0]
    else:
        fwd_m = 0
    print(f"  Markets:  historical {hist_m:,}  +  forward {fwd_m:,}  =  {hist_m + fwd_m:,} total")
    if has_any(HISTORICAL_TRADES_GLOB):
        hist_t = con.execute(f"SELECT COUNT(*) FROM '{HISTORICAL_TRADES_GLOB}'").fetchone()[0]
    else:
        hist_t = 0
    if use_new_trades:
        fwd_t = con.execute(f"SELECT COUNT(*) FROM '{FORWARD_TRADES_GLOB}'").fetchone()[0]
    elif use_legacy_trades:
        fwd_t = con.execute(f"SELECT COUNT(*) FROM '{LEGACY_FORWARD_TRADES_GLOB}'").fetchone()[0]
    else:
        fwd_t = 0
    print(f"  Trades:   historical {hist_t:,}  +  forward {fwd_t:,}  =  {hist_t + fwd_t:,} total")

    # ---- Sample rows (forward trades if any) ----
    print("\n--- SAMPLE ROWS (forward trades, first 3) ---")
    trade_glob = FORWARD_TRADES_GLOB if use_new_trades else (LEGACY_FORWARD_TRADES_GLOB if use_legacy_trades else None)
    if trade_glob and con.execute(f"SELECT COUNT(*) FROM '{trade_glob}'").fetchone()[0] > 0:
        sample = con.execute(
            f"""
            SELECT trade_id, ticker, taker_side, count, yes_price, no_price, created_time, _source
            FROM '{trade_glob}'
            ORDER BY created_time
            LIMIT 3
            """
        ).fetchdf()
        print(sample.to_string(index=False))
    else:
        print("  (no forward trades to sample)")

    print("\n--- SAMPLE ROWS (forward markets, first 3) ---")
    market_glob = FORWARD_MARKETS_GLOB if use_new_markets else (LEGACY_FORWARD_MARKETS_GLOB if use_legacy_markets else None)
    if market_glob and con.execute(f"SELECT COUNT(*) FROM '{market_glob}'").fetchone()[0] > 0:
        sample = con.execute(
            f"""
            SELECT ticker, title, status, close_time, created_time, _source
            FROM '{market_glob}'
            LIMIT 3
            """
        ).fetchdf()
        print(sample.to_string(index=False))
    else:
        print("  (no forward markets to sample)")

    # ---- How the pipeline checks data ----
    print("\n" + "=" * 72)
    print("HOW THE PIPELINE CHECKS DATA")
    print("=" * 72)
    print("""
1) ALREADY IN DATA (update_forward.py)
   • Before writing, it loads existing trade_id and (ticker, close_time) market keys
     from: historical parquet + legacy paths + forward parquet (new and legacy).
   • Fetch window uses a lookback (default 24h) so re-runs overlap slightly.
   • Every row from the API is deduped: if trade_id or market key is already in
     any of those sources, the row is skipped. So re-running never duplicates.

2) CORRECTNESS (validate_forward_pipeline.py)
   • Historical boundary: max(created_time) in local historical trades/markets
     must not exceed the API /historical/cutoff trades_created_ts.
   • Forward trade audit: COUNT(*) vs COUNT(DISTINCT trade_id); must be equal
     (no duplicate trade_ids in forward data).
   • Forward market audit: same for (ticker, close_time) market keys.
   • Overlap audit: no trade_id that appears in historical may appear in forward
     (and same for market keys). So historical and forward are disjoint.

3) RUN THIS VALIDATION
   uv run python scripts/validate_forward_pipeline.py
   uv run python scripts/validate_forward_pipeline.py --strict   # exit 1 on any failure
""")
    con.close()


if __name__ == "__main__":
    main()
