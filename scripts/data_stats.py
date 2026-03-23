#!/usr/bin/env python3
"""Print dataset stats (counts, volume, top tickers) from historical + forward data."""
from __future__ import annotations

import glob
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
    LEGACY_FORWARD_TRADES_GLOB,
)


def has_any(glob_path: Path) -> bool:
    return len(glob.glob(str(glob_path))) > 0


def main() -> None:
    con = duckdb.connect()

    has_hist_t = has_any(HISTORICAL_TRADES_GLOB)
    has_fwd_t = has_any(FORWARD_TRADES_GLOB)
    has_hist_m = HISTORICAL_MARKETS_FILE.exists()
    has_fwd_m = has_any(FORWARD_MARKETS_GLOB)

    if not has_hist_t and not has_fwd_t:
        print("No trade data found.")
        return

    # Use common columns only (historical and forward can have different cols e.g. _source)
    cols = "ticker, created_time, count, yes_price"
    if has_hist_t and has_fwd_t:
        trade_sql = f"""
            SELECT {cols} FROM read_parquet('{HISTORICAL_TRADES_GLOB}')
            UNION ALL
            SELECT {cols} FROM read_parquet('{FORWARD_TRADES_GLOB}')
        """
    else:
        pattern = str(HISTORICAL_TRADES_GLOB) if has_hist_t else str(FORWARD_TRADES_GLOB)
        trade_sql = f"SELECT {cols} FROM read_parquet('{pattern}')"

    print()
    print("=" * 60)
    print("DATASET STATS (historical + forward)")
    print("=" * 60)

    n_trades = con.execute(f"SELECT COUNT(*) FROM ({trade_sql}) t").fetchone()[0]
    print(f"  Total trades:        {n_trades:,}")

    if has_hist_m and has_fwd_m:
        n_m = con.execute(f"SELECT COUNT(*) FROM read_parquet('{HISTORICAL_MARKETS_FILE}')").fetchone()[0]
        n_m += con.execute(f"SELECT COUNT(*) FROM read_parquet('{FORWARD_MARKETS_GLOB}')").fetchone()[0]
    elif has_hist_m:
        n_m = con.execute(f"SELECT COUNT(*) FROM read_parquet('{HISTORICAL_MARKETS_FILE}')").fetchone()[0]
    elif has_fwd_m:
        n_m = con.execute(f"SELECT COUNT(*) FROM read_parquet('{FORWARD_MARKETS_GLOB}')").fetchone()[0]
    else:
        n_m = 0
    print(f"  Total markets:       {n_m:,}")

    r = con.execute(f"""
        SELECT COUNT(DISTINCT ticker), MIN(created_time), MAX(created_time)
        FROM ({trade_sql}) u
    """).fetchone()
    print(f"  Unique tickers:      {r[0]:,}")
    print(f"  Date range:          {r[1]} .. {r[2]}")

    # Contract volume: use count_fp when count=0 so totals are comparable to official sources (e.g. kalshidata.com)
    vol = 0.0
    trade_patterns = [
        (str(HISTORICAL_TRADES_GLOB), "hist"),
        (str(FORWARD_TRADES_GLOB), "fwd"),
    ]
    if has_any(LEGACY_FORWARD_TRADES_GLOB):
        trade_patterns.append((str(LEGACY_FORWARD_TRADES_GLOB), "legacy_fwd"))
    for pattern, label in trade_patterns:
        if not glob.glob(pattern):
            continue
        try:
            q = f"""
                SELECT SUM(COALESCE(NULLIF(count, 0), TRY_CAST(count_fp AS DOUBLE)))
                FROM read_parquet('{pattern}')
            """
            vol += con.execute(q).fetchone()[0] or 0
        except Exception:
            q = f"SELECT SUM(count) FROM read_parquet('{pattern}')"
            vol += con.execute(q).fetchone()[0] or 0
    if vol > 0:
        print(f"  Total contracts:    {vol:,.0f}")
    else:
        print("  Total contracts:    (no data or column not available)")

    # Dollar notional: API often leaves yes_price=0 but fills yes_price_dollars (per-contract USD)
    dv_cents = 0.0
    dv_dollars = 0.0
    for pattern, _ in trade_patterns:
        if not glob.glob(pattern):
            continue
        try:
            q = f"""
                SELECT SUM(COALESCE(NULLIF(count, 0), TRY_CAST(count_fp AS DOUBLE)) * yes_price / 100.0)
                FROM read_parquet('{pattern}')
            """
            dv_cents += con.execute(q).fetchone()[0] or 0
        except Exception:
            pass
        try:
            q = f"""
                SELECT SUM(COALESCE(NULLIF(count, 0), TRY_CAST(count_fp AS DOUBLE)) * TRY_CAST(yes_price_dollars AS DOUBLE))
                FROM read_parquet('{pattern}')
            """
            dv_dollars += con.execute(q).fetchone()[0] or 0
        except Exception:
            try:
                q = f"""
                    SELECT SUM(COALESCE(NULLIF(count, 0), TRY_CAST(count_fp AS DOUBLE)) * yes_price / 100.0)
                    FROM read_parquet('{pattern}')
                """
                dv_dollars += con.execute(q).fetchone()[0] or 0
            except Exception:
                pass
    if dv_dollars > 0:
        print(f"  Est. notional USD:   ${dv_dollars:,.0f}  (contracts × yes_price_dollars; compare to kalshidata.com)")
    if dv_cents > 0 and dv_cents != dv_dollars:
        print(f"  (alt: cents only)    ${dv_cents:,.0f}  (only rows where yes_price > 0)")

    print()
    print("  Top 5 tickers by trade count:")
    top = con.execute(f"""
        SELECT ticker, COUNT(*) AS n
        FROM ({trade_sql}) u
        GROUP BY ticker
        ORDER BY n DESC
        LIMIT 5
    """).fetchall()
    for ticker, n in top:
        print(f"    {ticker}: {n:,} trades")
    print()
    print("=" * 60)


if __name__ == "__main__":
    main()
