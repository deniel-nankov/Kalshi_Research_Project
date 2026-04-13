#!/usr/bin/env python3
# ruff: noqa: E402
"""Tier-1 calibration metrics on the full Kalshi parquet corpus (historical + forward).

Implements research items **2.1–2.2** from the quant framework (reliability diagram inputs +
Brier score with Murphy 1973 bin decomposition), using **last traded YES price** on each ticker
as the market-implied probability at end of tape (not a true point-in-time forecast — see notes).

Requirements: DuckDB; data under ``src/kalshi_forward/paths.py`` (same layout as ``data_stats.py``).

Example::

  uv run python scripts/tier1_resolution_forecasts.py
  uv run python scripts/tier1_resolution_forecasts.py --bins 25
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_SCRIPT_ROOT = Path(__file__).resolve().parents[1]
if str(_SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_ROOT))

import duckdb
import numpy as np

from src.common.kalshi_union_queries import markets_union_sql, trade_union_sql
from src.common.scoring_rules import murphy_decomposition_from_bins


def main() -> int:
    parser = argparse.ArgumentParser(description="Market-level Brier + Murphy decomposition (last trade price).")
    parser.add_argument("--bins", type=int, default=20, help="Number of equal-width probability bins on [0,1).")
    parser.add_argument(
        "--legacy-trades",
        action="store_true",
        help="Include legacy incremental forward trades glob if present.",
    )
    parser.add_argument(
        "--clip-extremes",
        action="store_true",
        help=(
            "Keep all last prints 0-100c: map 0 to 0.005 and 100 to 0.995 for scoring; "
            "default mode keeps only 1-98c (excludes many settled-at-the-tape contracts)."
        ),
    )
    args = parser.parse_args()
    if args.bins < 3:
        print("bins must be >= 3", file=sys.stderr)
        return 2

    cols_m = "ticker, result, updated_time, close_time, created_time"
    cols_t = "ticker, created_time, yes_price"
    try:
        markets_u = markets_union_sql(cols=cols_m)
        trades_u = trade_union_sql(cols=cols_t, include_legacy_forward=args.legacy_trades)
    except FileNotFoundError as e:
        print(e, file=sys.stderr)
        return 1

    con = duckdb.connect()
    p_expr = (
        "CASE WHEN lt.yp < 1 THEN 0.005 WHEN lt.yp > 99 THEN 0.995 ELSE lt.yp / 100.0 END"
        if args.clip_extremes
        else "lt.yp / 100.0"
    )
    join_filter = "" if args.clip_extremes else "\n      WHERE lt.yp BETWEEN 1 AND 99"
    summary_sql = f"""
    WITH
    raw_m AS ({markets_u}),
    m AS (
      SELECT
        ticker,
        arg_max(
          lower(result),
          COALESCE(
            try_cast(updated_time AS TIMESTAMPTZ),
            try_cast(close_time AS TIMESTAMPTZ),
            try_cast(created_time AS TIMESTAMPTZ),
            TIMESTAMPTZ '1970-01-01'
          )
        ) AS outcome
      FROM raw_m
      WHERE lower(result) IN ('yes', 'no')
      GROUP BY ticker
    ),
    raw_t AS ({trades_u}),
    lt AS (
      SELECT
        ticker,
        arg_max(yes_price, try_cast(created_time AS TIMESTAMPTZ)) AS yp
      FROM raw_t
      GROUP BY ticker
    ),
    j AS (
      SELECT
        lt.ticker,
        {p_expr} AS p,
        CASE WHEN m.outcome = 'yes' THEN 1.0 ELSE 0.0 END AS y
      FROM lt
      INNER JOIN m ON lt.ticker = m.ticker{join_filter}
    )
    SELECT
      AVG(power(p - y, 2)) AS brier_exact,
      AVG(y) AS o_bar,
      COUNT(*)::BIGINT AS n
    FROM j
    """
    row = con.execute(summary_sql).fetchone()
    brier_exact, o_bar, n_total = float(row[0]), float(row[1]), int(row[2])

    hist_sql = f"""
    WITH
    raw_m AS ({markets_u}),
    m AS (
      SELECT
        ticker,
        arg_max(
          lower(result),
          COALESCE(
            try_cast(updated_time AS TIMESTAMPTZ),
            try_cast(close_time AS TIMESTAMPTZ),
            try_cast(created_time AS TIMESTAMPTZ),
            TIMESTAMPTZ '1970-01-01'
          )
        ) AS outcome
      FROM raw_m
      WHERE lower(result) IN ('yes', 'no')
      GROUP BY ticker
    ),
    raw_t AS ({trades_u}),
    lt AS (
      SELECT
        ticker,
        arg_max(yes_price, try_cast(created_time AS TIMESTAMPTZ)) AS yp
      FROM raw_t
      GROUP BY ticker
    ),
    j AS (
      SELECT
        {p_expr} AS p,
        CASE WHEN m.outcome = 'yes' THEN 1.0 ELSE 0.0 END AS y
      FROM lt
      INNER JOIN m ON lt.ticker = m.ticker{join_filter}
    ),
    b AS (
      SELECT
        LEAST({args.bins - 1}, GREATEST(0, floor(p * {args.bins})::INTEGER)) AS bin_id,
        p,
        y
      FROM j
    )
    SELECT
      bin_id,
      COUNT(*)::BIGINT AS n_k,
      AVG(p) AS p_bar_k,
      AVG(y) AS o_bar_k
    FROM b
    GROUP BY bin_id
    ORDER BY bin_id
    """
    bins = con.execute(hist_sql).fetchall()
    if not bins:
        print("No overlapping resolved markets with last-trade prices.")
        return 0

    n_k = np.array([int(r[1]) for r in bins], dtype=float)
    p_bar = np.array([float(r[2]) for r in bins], dtype=float)
    o_bar_k = np.array([float(r[3]) for r in bins], dtype=float)
    murphy = murphy_decomposition_from_bins(n_k, p_bar, o_bar_k)

    print()
    print("=" * 72)
    sample = (
        "last YES print 0-100c (extremes clipped for scoring)"
        if args.clip_extremes
        else "last YES print 1-98c only"
    )
    print(f"  Tier-1: market-level Brier ({sample} vs resolution)")
    print("=" * 72)
    n_label = (
        "resolved markets with last print 0-100c (extremes clipped for scoring)"
        if args.clip_extremes
        else "resolved markets with last print 1-98c only"
    )
    print(f"  N markets ({n_label}): {n_total:,}")
    print(f"  Base rate P(YES resolves):             {o_bar:.4f}")
    print(f"  Brier score (exact, trade-level join): {brier_exact:.6f}")
    print()
    print("  Murphy (1973) decomposition from equal-width probability bins:")
    for k, v in murphy.as_dict().items():
        if k == "n_total":
            print(f"    {k}: {int(v):,}")
        else:
            print(f"    {k}: {float(v):.6f}")
    print()
    print("  Per-bin calibration (favorite-longshot: positive bias => YES underpriced in bin):")
    print(f"  {'bin':>4}  {'n':>10}  {'p_bar':>8}  {'o_bar':>8}  {'bias':>8}")
    for r in bins:
        bid, nk, pb, ob = int(r[0]), int(r[1]), float(r[2]), float(r[3])
        print(f"  {bid:>4}  {nk:>10,}  {pb:>8.4f}  {ob:>8.4f}  {ob - pb:>+8.4f}")
    print()
    print("  NOTE: 'last trade' is not a formal forecast timestamp; for publication-grade")
    print("  calibration vs Whelan et al., snapshot prices at fixed horizons before close.")
    print("=" * 72)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
