#!/usr/bin/env python3
# ruff: noqa: E402
"""
Price impact analysis - institutional microstructure characterization.

Per-trade computation over the enriched_trades table:

  taker_price_dollars         yes_price if taker_side == 'yes' else no_price
                              (the dollar amount the taker actually paid
                              per contract)

  prev_yes_price_dollars      yes_price of the immediately preceding trade
                              in the same market_ticker

  prev_taker_price_aligned    taker-aligned prior price: yes_price (if
                              current taker_side == 'yes') or no_price (if
                              'no') of the prior trade, so both sides of the
                              impact subtraction live in the dollars-the-
                              taker-would-pay frame regardless of which side
                              took the prior trade

  price_impact_yes_cents      100 * (yes_price - prev_yes_price_dollars)
                              Neutral, signed in yes-price terms.

  price_impact_taker_cents    100 * (taker_price - prev_taker_price_aligned)
                              Taker frame: positive = the current taker paid
                              more (per contract) than the prior trade's
                              prevailing price in the same units.

  abs_price_impact_yes_cents  |price_impact_yes_cents| - magnitude only

  avg_yes_pre_N    mean(yes_price)         over the N trades IMMEDIATELY
  avg_yes_post_N   mean(yes_price)         BEFORE / AFTER the current trade,
  avg_taker_pre_N  mean(taker_price)       same market_ticker. N in
  avg_taker_post_N mean(taker_price)       {1, 5, 10, 25, 50, 100}.

Aggregations
------------
  1. Combined (all trades with non-null prev_yes_price_dollars)
  2. By taker_side  (yes / no)
  3. By was_taker_correct  (resolved trades only; true / false)
  4. 2 x 2: taker_side * was_taker_correct
  5. Maker-vs-taker volume profile

Maker vs taker framing
----------------------
Each Kalshi trade has exactly one taker (the marketable order) and one
maker (the resting order). The `count` contracts that change hands are
identical on both sides, so `total_volume_taker == total_volume_maker`
trade-for-trade. The economic asymmetry sits in the price:

  taker pays      taker_break_even_price       (= taker_price_dollars)
  maker receives  1 - taker_break_even_price   (= 1 - taker_price_dollars)

So "maker correctness" is the negation of `was_taker_correct` on resolved
trades. We report taker-frame and maker-frame summaries side-by-side; the
maker rows are an algebraic mirror of the taker rows on the same trade
set.

Methodology - no look-ahead, no leakage
---------------------------------------
Pre/post windows are pure intra-market lag/lead operations on the same
column - they do not reference outcome columns. The 2 x 2 split uses
`was_taker_correct` only as a stratifier applied to ALREADY-COMPUTED
features; the features themselves never see the label.

Sharding (institutional scale)
------------------------------
The full enriched_trades table is ~470 M rows (~25 GB compressed); a
single-pass eager expansion with 30 derived columns would exceed 64 GB of
RAM. The script processes data in `--shards N` (default 8) disjoint
market_ticker hash buckets and emits PARTIAL SUFFICIENT STATISTICS per
shard (sums, sum-of-squares, counts). After all shards complete, the
partials are concatenated and re-aggregated to derive means / stds /
totals exactly equivalent to the single-pass result. Boundary effects
are zero: every trade for a given market lives in exactly one shard, so
intra-market lag/lead windows are computed losslessly.

For small workloads (< ~30 days), pass `--shards 1` for a single-pass
run.

Inputs / outputs
----------------
Reads:  data/kalshi/derived/enriched_trades/dt=*/...parquet
Writes:
  surveillance/reports/<run_id>.md
  state/price_impact_runs/<run_id>.json
  (and, with --save-trade-level):
  data/kalshi/derived/price_impact/dt=YYYY-MM-DD/<run_id>.parquet
  + co-located <file>.parquet.sha256
"""

from __future__ import annotations

import argparse
import gc
import glob
import hashlib
import json
import math
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = PROJECT_ROOT / "data" / "kalshi"
DERIVED = DATA_ROOT / "derived"
STATE = DATA_ROOT / "state"
ENRICHED_DIR = DERIVED / "enriched_trades"
OUT_DIR = DERIVED / "price_impact"
REPORTS_DIR = PROJECT_ROOT / "surveillance" / "reports"
AUDIT_DIR = STATE / "price_impact_runs"

VERSION = "analyze_price_impact@1.3.0"

WINDOW_SIZES = [1, 5, 10, 25, 50, 100]

SOURCE_COLUMNS = [
    "trade_id",
    "market_ticker",
    "created_time",
    "taker_side",
    "yes_price_dollars",
    "no_price_dollars",
    "count",
    "notional_usd",
    "was_taker_correct",
]


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


def _json_default(o):
    if isinstance(o, (datetime,)):
        return o.isoformat()
    if isinstance(o, Path):
        return str(o)
    if isinstance(o, float) and (math.isnan(o) or math.isinf(o)):
        return None
    return str(o)


def _write_audit(run_id: str, payload: dict) -> None:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    out = AUDIT_DIR / f"{run_id}.json"
    tmp = out.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True, default=_json_default), encoding="utf-8")
    os.replace(tmp, out)


# ---------- partition discovery ----------

def discover_partitions(args: argparse.Namespace) -> list[Path]:
    parts = sorted(
        p for p in ENRICHED_DIR.iterdir()
        if p.is_dir() and p.name.startswith("dt=")
    )
    if args.from_dt:
        parts = [p for p in parts if p.name[3:] >= args.from_dt]
    if args.to_dt:
        parts = [p for p in parts if p.name[3:] <= args.to_dt]
    if args.days:
        parts = parts[-args.days:]
    return parts


# ---------- per-trade feature computation ----------

def add_per_trade_features(df: pl.DataFrame) -> pl.DataFrame:
    df = df.with_columns(
        pl.when(pl.col("taker_side") == "yes")
        .then(pl.col("yes_price_dollars"))
        .otherwise(pl.col("no_price_dollars"))
        .alias("taker_price_dollars"),
    )
    df = df.sort(["market_ticker", "created_time", "trade_id"])

    df = df.with_columns([
        pl.col("yes_price_dollars").shift(1).over("market_ticker").alias("prev_yes_price_dollars"),
        pl.col("no_price_dollars").shift(1).over("market_ticker").alias("prev_no_price_dollars"),
    ])
    df = df.with_columns(
        pl.when(pl.col("taker_side") == "yes")
        .then(pl.col("prev_yes_price_dollars"))
        .otherwise(pl.col("prev_no_price_dollars"))
        .alias("prev_taker_price_aligned"),
    )
    df = df.with_columns([
        (100.0 * (pl.col("yes_price_dollars") - pl.col("prev_yes_price_dollars")))
            .alias("price_impact_yes_cents"),
        (100.0 * (pl.col("taker_price_dollars") - pl.col("prev_taker_price_aligned")))
            .alias("price_impact_taker_cents"),
    ])
    df = df.with_columns(
        pl.col("price_impact_yes_cents").abs().alias("abs_price_impact_yes_cents"),
    )

    # Compute neutral yes-price-frame N-trade pre/post averages per market.
    # The taker-aligned versions are derived inline in the aggregator from
    # these + current taker_side, NOT materialized per row.  See _partial_aggs.
    window_cols: list[pl.Expr] = []
    for N in WINDOW_SIZES:
        window_cols.append(
            pl.col("yes_price_dollars")
              .shift(1).rolling_mean(window_size=N, min_samples=N)
              .over("market_ticker").alias(f"avg_yes_pre_{N}")
        )
        window_cols.append(
            pl.col("yes_price_dollars")
              .shift(-N).rolling_mean(window_size=N, min_samples=N)
              .over("market_ticker").alias(f"avg_yes_post_{N}")
        )
    df = df.with_columns(window_cols)
    return df


def _taker_pre_expr(N: int) -> pl.Expr:
    """Per-row taker-aligned pre-N average, derived inline.

    For current taker_side == 'yes', it's the prior yes-price average; for
    'no', it's 1 - that (= prior no-price average).  An earlier version
    used a separate `shift().rolling_mean()` on taker_price_dollars; that
    mixes prior trades' OWN taker-side prices and is NOT what we want
    here (we want the price the CURRENT taker would have paid for the
    typical prior trade).
    """
    return (
        pl.when(pl.col("taker_side") == "yes")
          .then(pl.col(f"avg_yes_pre_{N}"))
          .otherwise(1.0 - pl.col(f"avg_yes_pre_{N}"))
    )


def _taker_post_expr(N: int) -> pl.Expr:
    return (
        pl.when(pl.col("taker_side") == "yes")
          .then(pl.col(f"avg_yes_post_{N}"))
          .otherwise(1.0 - pl.col(f"avg_yes_post_{N}"))
    )


# ---------- sufficient-statistics aggregation ----------

def _partial_aggs() -> list[pl.Expr]:
    """Emit per-group sums + counts so partial summaries can be combined
    across shards into exact means and stds.

    For each window N we emit four things:
      * sum/count of avg_yes_pre/post_N — neutral yes-price-frame mean.
      * sum/count of the taker-aligned avg (derived inline from yes-frame).
      * sum of taker_price (resp. yes_price) RESTRICTED to rows that have
        a valid avg_yes_pre/post_N — this lets the renderer compute
        Δ pre→trade / Δ trade→post / Δ pre→post on the SAME subset of trades
        used for the pre/post means.  Without this subset-matched at-trade
        average, Δ values would be biased by first-in-market / last-in-
        market trades that lack a window.
    """
    aggs: list[pl.Expr] = [
        pl.len().alias("n_trades"),

        pl.col("count").sum().alias("sum_count"),
        pl.col("count").is_not_null().sum().alias("n_count"),
        pl.col("count").pow(2.0).sum().alias("sum_count_sq"),

        pl.col("notional_usd").sum().alias("sum_notional"),
        pl.col("notional_usd").is_not_null().sum().alias("n_notional"),

        pl.col("yes_price_dollars").sum().alias("sum_yes_price"),
        pl.col("yes_price_dollars").is_not_null().sum().alias("n_yes_price"),

        pl.col("taker_price_dollars").sum().alias("sum_taker_price"),
        pl.col("taker_price_dollars").is_not_null().sum().alias("n_taker_price"),

        pl.col("price_impact_yes_cents").sum().alias("sum_pi_yes"),
        pl.col("price_impact_yes_cents").pow(2.0).sum().alias("sum_pi_yes_sq"),
        pl.col("price_impact_yes_cents").is_not_null().sum().alias("n_pi_yes"),

        pl.col("price_impact_taker_cents").sum().alias("sum_pi_taker"),
        pl.col("price_impact_taker_cents").pow(2.0).sum().alias("sum_pi_taker_sq"),
        pl.col("price_impact_taker_cents").is_not_null().sum().alias("n_pi_taker"),

        pl.col("abs_price_impact_yes_cents").sum().alias("sum_abs_pi"),
        pl.col("abs_price_impact_yes_cents").is_not_null().sum().alias("n_abs_pi"),
    ]
    for N in WINDOW_SIZES:
        yes_pre = pl.col(f"avg_yes_pre_{N}")
        yes_post = pl.col(f"avg_yes_post_{N}")
        taker_pre = _taker_pre_expr(N)
        taker_post = _taker_post_expr(N)

        aggs.extend([
            # Yes-frame window averages
            yes_pre.sum().alias(f"sum_yes_pre_{N}"),
            yes_pre.is_not_null().sum().alias(f"n_yes_pre_{N}"),
            yes_post.sum().alias(f"sum_yes_post_{N}"),
            yes_post.is_not_null().sum().alias(f"n_yes_post_{N}"),

            # Taker-aligned window averages (derived inline; not materialized)
            taker_pre.sum().alias(f"sum_taker_pre_{N}"),
            taker_pre.is_not_null().sum().alias(f"n_taker_pre_{N}"),
            taker_post.sum().alias(f"sum_taker_post_{N}"),
            taker_post.is_not_null().sum().alias(f"n_taker_post_{N}"),

            # Subset-matched at-trade averages: taker_price (resp. yes_price)
            # restricted to the same trades that have valid pre / post windows
            pl.when(yes_pre.is_not_null())
              .then(pl.col("taker_price_dollars"))
              .sum().alias(f"sum_taker_price_when_pre_{N}"),
            pl.when(yes_post.is_not_null())
              .then(pl.col("taker_price_dollars"))
              .sum().alias(f"sum_taker_price_when_post_{N}"),
            pl.when(yes_pre.is_not_null())
              .then(pl.col("yes_price_dollars"))
              .sum().alias(f"sum_yes_price_when_pre_{N}"),
            pl.when(yes_post.is_not_null())
              .then(pl.col("yes_price_dollars"))
              .sum().alias(f"sum_yes_price_when_post_{N}"),
        ])
    return aggs


def aggregate_partial(df: pl.DataFrame, group_cols: list[str] | None) -> pl.DataFrame:
    if group_cols:
        return df.group_by(group_cols, maintain_order=True).agg(_partial_aggs()).sort(group_cols)
    else:
        return df.select(_partial_aggs())


SUM_COLS = [
    "n_trades", "sum_count", "n_count", "sum_count_sq",
    "sum_notional", "n_notional",
    "sum_yes_price", "n_yes_price",
    "sum_taker_price", "n_taker_price",
    "sum_pi_yes", "sum_pi_yes_sq", "n_pi_yes",
    "sum_pi_taker", "sum_pi_taker_sq", "n_pi_taker",
    "sum_abs_pi", "n_abs_pi",
]
for _N in WINDOW_SIZES:
    SUM_COLS.extend([
        f"sum_yes_pre_{_N}", f"n_yes_pre_{_N}",
        f"sum_yes_post_{_N}", f"n_yes_post_{_N}",
        f"sum_taker_pre_{_N}", f"n_taker_pre_{_N}",
        f"sum_taker_post_{_N}", f"n_taker_post_{_N}",
        f"sum_taker_price_when_pre_{_N}",
        f"sum_taker_price_when_post_{_N}",
        f"sum_yes_price_when_pre_{_N}",
        f"sum_yes_price_when_post_{_N}",
    ])


def combine_and_derive(partials: list[pl.DataFrame], group_cols: list[str] | None) -> pl.DataFrame:
    """Concat per-shard partials, sum sufficient stats, derive means + stds."""
    if not partials:
        return pl.DataFrame()
    concat = pl.concat(partials, how="vertical_relaxed")
    if group_cols:
        combined = concat.group_by(group_cols, maintain_order=True).agg([
            pl.col(c).sum() for c in SUM_COLS
        ]).sort(group_cols)
    else:
        combined = concat.select([pl.col(c).sum() for c in SUM_COLS])

    expressions: list[pl.Expr] = [
        # Aliases used by the renderer
        pl.col("sum_count").alias("total_contracts"),
        pl.col("sum_notional").alias("total_notional_usd"),
        pl.col("n_pi_taker").alias("n_with_prev"),
        (pl.col("sum_count") / pl.col("n_count")).alias("mean_trade_size_contracts"),
        (pl.col("sum_count_sq") / pl.col("n_count") - (pl.col("sum_count") / pl.col("n_count")).pow(2.0))
            .sqrt().alias("std_trade_size_contracts"),
        (pl.col("sum_notional") / pl.col("n_notional")).alias("mean_notional_usd"),
        (pl.col("sum_yes_price") / pl.col("n_yes_price")).alias("mean_yes_price"),
        (pl.col("sum_taker_price") / pl.col("n_taker_price")).alias("mean_taker_price"),
        (pl.col("sum_pi_yes") / pl.col("n_pi_yes")).alias("mean_pi_yes_cents"),
        (
            (pl.col("sum_pi_yes_sq") / pl.col("n_pi_yes"))
            - (pl.col("sum_pi_yes") / pl.col("n_pi_yes")).pow(2.0)
        ).sqrt().alias("std_pi_yes_cents"),
        (pl.col("sum_pi_taker") / pl.col("n_pi_taker")).alias("mean_pi_taker_cents"),
        (
            (pl.col("sum_pi_taker_sq") / pl.col("n_pi_taker"))
            - (pl.col("sum_pi_taker") / pl.col("n_pi_taker")).pow(2.0)
        ).sqrt().alias("std_pi_taker_cents"),
        (pl.col("sum_abs_pi") / pl.col("n_abs_pi")).alias("mean_abs_pi_cents"),
    ]
    for N in WINDOW_SIZES:
        expressions.extend([
            (pl.col(f"sum_yes_pre_{N}") / pl.col(f"n_yes_pre_{N}")).alias(f"mean_yes_pre_{N}"),
            (pl.col(f"sum_yes_post_{N}") / pl.col(f"n_yes_post_{N}")).alias(f"mean_yes_post_{N}"),
            (pl.col(f"sum_taker_pre_{N}") / pl.col(f"n_taker_pre_{N}")).alias(f"mean_taker_pre_{N}"),
            (pl.col(f"sum_taker_post_{N}") / pl.col(f"n_taker_post_{N}")).alias(f"mean_taker_post_{N}"),
            # Subset-matched at-trade averages — same denominator as pre/post,
            # so Δ pre→trade / Δ trade→post / Δ pre→post are honest per-row means.
            (pl.col(f"sum_taker_price_when_pre_{N}") / pl.col(f"n_yes_pre_{N}"))
                .alias(f"mean_taker_at_when_pre_{N}"),
            (pl.col(f"sum_taker_price_when_post_{N}") / pl.col(f"n_yes_post_{N}"))
                .alias(f"mean_taker_at_when_post_{N}"),
            (pl.col(f"sum_yes_price_when_pre_{N}") / pl.col(f"n_yes_pre_{N}"))
                .alias(f"mean_yes_at_when_pre_{N}"),
            (pl.col(f"sum_yes_price_when_post_{N}") / pl.col(f"n_yes_post_{N}"))
                .alias(f"mean_yes_at_when_post_{N}"),
            pl.col(f"n_yes_pre_{N}").alias(f"n_pre_{N}"),
            pl.col(f"n_yes_post_{N}").alias(f"n_post_{N}"),
        ])
    return combined.with_columns(expressions)


# ---------- Markdown rendering ----------

def _fmt_int(v) -> str:
    if v is None:
        return "—"
    try:
        return f"{int(v):,}"
    except Exception:
        return str(v)


def _fmt_float(v, decimals: int = 4) -> str:
    if v is None:
        return "—"
    try:
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            return "—"
        return f"{float(v):,.{decimals}f}"
    except Exception:
        return str(v)


def _fmt_money(v, decimals: int = 2) -> str:
    if v is None:
        return "—"
    try:
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            return "—"
        return f"${float(v):,.{decimals}f}"
    except Exception:
        return str(v)


def _table_volume(rows: list[dict]) -> str:
    cols = [
        ("Group", "group"),
        ("n_trades", "n_trades"),
        ("Mean size (contracts)", "mean_trade_size_contracts"),
        ("Total contracts", "total_contracts"),
        ("Total notional ($)", "total_notional_usd"),
        ("Mean yes_price", "mean_yes_price"),
        ("Mean taker_price", "mean_taker_price"),
    ]
    lines = [
        "| " + " | ".join(c[0] for c in cols) + " |",
        "|" + "|".join(["---"] * len(cols)) + "|",
    ]
    for r in rows:
        cells = []
        for header, key in cols:
            v = r.get(key)
            if key in ("n_trades", "total_contracts"):
                cells.append(_fmt_int(v))
            elif key == "mean_trade_size_contracts":
                cells.append(_fmt_float(v, 2))
            elif key == "total_notional_usd":
                cells.append(_fmt_money(v, 0))
            elif key == "group":
                cells.append(str(v))
            else:
                cells.append(_fmt_float(v, 4))
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


def _table_impact(rows: list[dict]) -> str:
    cols = [
        ("Group", "group"),
        ("n with prev", "n_with_prev"),
        ("Mean Δp_yes (¢)", "mean_pi_yes_cents"),
        ("Std Δp_yes (¢)", "std_pi_yes_cents"),
        ("Mean Δp_taker (¢)", "mean_pi_taker_cents"),
        ("Std Δp_taker (¢)", "std_pi_taker_cents"),
        ("Mean |Δp_yes| (¢)", "mean_abs_pi_cents"),
    ]
    lines = [
        "| " + " | ".join(c[0] for c in cols) + " |",
        "|" + "|".join(["---"] * len(cols)) + "|",
    ]
    for r in rows:
        cells = []
        for header, key in cols:
            v = r.get(key)
            if key == "n_with_prev":
                cells.append(_fmt_int(v))
            elif key == "group":
                cells.append(str(v))
            else:
                cells.append(_fmt_float(v, 4))
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


def _table_prepost(row: dict, side: str = "yes") -> str:
    """Render the pre/at/post profile for one group.

    Side ∈ {"yes", "taker"}.  Each row reports Mean pre-N, the
    subset-matched At-trade-when-pre, Mean post-N, the subset-matched
    At-trade-when-post, and the three Δ values computed as honest
    per-row means (via subset-matched at-trade columns produced upstream).
    """
    lines = [
        "| N | Mean pre-N ($) | At trade (pre subset) ($) | Mean post-N ($) | At trade (post subset) ($) | Δ pre→trade (¢) | Δ trade→post (¢) | Δ pre→post (¢) | n_pre | n_post |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    for N in WINDOW_SIZES:
        pre = row.get(f"mean_{side}_pre_{N}")
        post = row.get(f"mean_{side}_post_{N}")
        at_pre = row.get(f"mean_{side}_at_when_pre_{N}")
        at_post = row.get(f"mean_{side}_at_when_post_{N}")
        n_pre = row.get(f"n_pre_{N}")
        n_post = row.get(f"n_post_{N}")
        d_pre_trade = (100.0 * (at_pre - pre)) if (pre is not None and at_pre is not None) else None
        d_trade_post = (100.0 * (post - at_post)) if (post is not None and at_post is not None) else None
        # Δ pre→post compares two sub-samples (pre-N-eligible vs post-N-eligible)
        # which differ near market boundaries. Report only when both are well-
        # defined; computed as (mean_post_N - mean_pre_N) is the natural reading.
        d_pre_post = (100.0 * (post - pre)) if (pre is not None and post is not None) else None
        lines.append(
            "| " + " | ".join([
                str(N),
                _fmt_float(pre, 5),
                _fmt_float(at_pre, 5),
                _fmt_float(post, 5),
                _fmt_float(at_post, 5),
                _fmt_float(d_pre_trade, 4),
                _fmt_float(d_trade_post, 4),
                _fmt_float(d_pre_post, 4),
                _fmt_int(n_pre),
                _fmt_int(n_post),
            ]) + " |"
        )
    return "\n".join(lines)


def render_md(
    run_id: str,
    parts: list[Path],
    sample_meta: dict,
    combined_row: dict,
    by_side_rows: list[dict],
    by_correct_rows: list[dict],
    by_side_correct_rows: list[dict],
) -> str:
    started = sample_meta["started_at_iso"]
    elapsed = sample_meta["elapsed_seconds"]
    n_rows_total = sample_meta["n_rows_total"]
    n_rows_resolved = sample_meta["n_rows_resolved"]
    n_shards = sample_meta["n_shards"]
    dt_from = parts[0].name[3:]
    dt_to = parts[-1].name[3:]

    md: list[str] = []
    md.append(f"# Price impact analysis — `{run_id}`\n")
    md.append(f"Started: `{started}`  •  Elapsed: `{elapsed:.1f}s`  •  Version: `{VERSION}`  •  Shards: `{n_shards}`\n")

    md.append("## 1. Sample\n")
    md.append(f"- Date range: **{dt_from} → {dt_to}** ({len(parts)} dt-partitions)")
    md.append(f"- Total trades scanned: **{n_rows_total:,}**")
    md.append(f"- Trades with prior same-market trade (price-impact eligible): **{int(combined_row.get('n_with_prev') or 0):,}**")
    md.append(f"- Resolved trades (`was_taker_correct` non-null): **{n_rows_resolved:,}**")
    md.append(f"- Total contracts traded: **{int(combined_row.get('total_contracts') or 0):,}**")
    md.append(f"- Total notional USD: **${combined_row.get('total_notional_usd') or 0:,.0f}**")
    md.append("")

    md.append("## 2. Combined (all trades)\n")
    md.append("### 2.1 Volume + price level\n")
    md.append(_table_volume([{**combined_row, "group": "all"}]))
    md.append("")
    md.append("### 2.2 Price impact (signed = positive means price ROSE / taker paid up vs. prior trade)\n")
    md.append(_table_impact([{**combined_row, "group": "all"}]))
    md.append("")
    md.append("### 2.3 Pre/post profile — yes-price frame (neutral)\n")
    md.append(_table_prepost({**combined_row, "group": "all"}, side="yes"))
    md.append("")
    md.append("### 2.4 Pre/post profile — taker-price frame (taker-aligned: positive Δ = taker paid more)\n")
    md.append(_table_prepost({**combined_row, "group": "all"}, side="taker"))
    md.append("")

    md.append("## 3. Split by taker_side\n")
    md.append("Each Kalshi trade is initiated by either a YES-side taker or a NO-side taker.")
    md.append("")
    md.append("### 3.1 Volume + price level\n")
    md.append(_table_volume(by_side_rows))
    md.append("")
    md.append("### 3.2 Price impact\n")
    md.append(_table_impact(by_side_rows))
    md.append("")
    for r in by_side_rows:
        md.append(f"### 3.3 Pre/post — taker frame — `{r['group']}` takers\n")
        md.append(_table_prepost(r, side="taker"))
        md.append("")

    md.append("## 4. Split by ex-post correctness (resolved trades only)\n")
    md.append("`was_taker_correct = True`  →  taker won, maker lost.")
    md.append("`was_taker_correct = False` →  taker lost, maker won.")
    md.append("")
    md.append("### 4.1 Volume + price level\n")
    md.append(_table_volume(by_correct_rows))
    md.append("")
    md.append("### 4.2 Price impact\n")
    md.append(_table_impact(by_correct_rows))
    md.append("")
    for r in by_correct_rows:
        md.append(f"### 4.3 Pre/post — taker frame — `{r['group']}`\n")
        md.append(_table_prepost(r, side="taker"))
        md.append("")

    md.append("## 5. 2 × 2 — taker_side × ex-post correctness\n")
    md.append("### 5.1 Volume + price level\n")
    md.append(_table_volume(by_side_correct_rows))
    md.append("")
    md.append("### 5.2 Price impact\n")
    md.append(_table_impact(by_side_correct_rows))
    md.append("")
    for r in by_side_correct_rows:
        md.append(f"### 5.3 Pre/post — taker frame — `{r['group']}`\n")
        md.append(_table_prepost(r, side="taker"))
        md.append("")

    total_contracts = combined_row.get("total_contracts") or 0
    total_notional = combined_row.get("total_notional_usd") or 0
    mean_size = combined_row.get("mean_trade_size_contracts") or 0
    mean_taker_price = combined_row.get("mean_taker_price") or 0
    mean_maker_price = 1.0 - mean_taker_price if mean_taker_price is not None else None
    n_trades_total = combined_row.get("n_trades") or 0

    md.append("## 6. Maker-vs-taker volume profile\n")
    md.append("Every trade has 1 taker (marketable order) and 1 maker (resting order).")
    md.append("The same `count` contracts change hands on both sides, so per-trade")
    md.append("`trade_size` and aggregated `total_contracts` / `total_notional` are")
    md.append("identical between the taker and maker views. The meaningful asymmetry")
    md.append("is the price each side received and the resulting PnL.")
    md.append("")
    md.append("| Side | n_trades | Mean trade size (contracts) | Total contracts | Total notional ($) | Mean price paid/received ($) |")
    md.append("|---|---|---|---|---|---|")
    md.append(
        "| Taker (aggressor) | "
        + f"{int(n_trades_total):,} | "
        + f"{mean_size:,.2f} | "
        + f"{int(total_contracts):,} | "
        + f"${total_notional:,.0f} | "
        + f"{mean_taker_price:,.5f} |"
    )
    md.append(
        "| Maker (resting)   | "
        + f"{int(n_trades_total):,} | "
        + f"{mean_size:,.2f} | "
        + f"{int(total_contracts):,} | "
        + f"${total_notional:,.0f} | "
        + (f"{mean_maker_price:,.5f}" if mean_maker_price is not None else "—")
        + " |"
    )
    md.append("")
    md.append("**Side mix by initiator (taker_side):**")
    md.append("")
    md.append("| taker_side | n_trades | % of trades | Mean trade size | Total contracts | Total notional ($) |")
    md.append("|---|---|---|---|---|---|")
    for r in by_side_rows:
        n = int(r.get("n_trades") or 0)
        pct = (100.0 * n / n_trades_total) if n_trades_total else 0
        md.append(
            "| " + str(r["group"]) + " | "
            + f"{n:,} | "
            + f"{pct:.2f}% | "
            + f"{r.get('mean_trade_size_contracts') or 0:,.2f} | "
            + f"{int(r.get('total_contracts') or 0):,} | "
            + f"${r.get('total_notional_usd') or 0:,.0f} |"
        )
    md.append("")

    md.append("## 7. Reproducibility\n")
    md.append("```")
    md.append(f"run_id          : {run_id}")
    md.append(f"version         : {VERSION}")
    md.append(f"started_at_iso  : {started}")
    md.append(f"elapsed_seconds : {elapsed:.1f}")
    md.append(f"partitions      : {parts[0].name} .. {parts[-1].name}  ({len(parts)} days)")
    md.append(f"shards          : {n_shards}")
    md.append(f"WINDOW_SIZES    : {WINDOW_SIZES}")
    md.append(f"audit log       : data/kalshi/state/price_impact_runs/{run_id}.json")
    md.append("```")
    md.append("")
    md.append("## 8. Methodology notes\n")
    md.append("1. Sorting: within each `market_ticker` by `created_time` then `trade_id`")
    md.append("   (deterministic tie-break for microsecond-coincident trades).")
    md.append("2. Pre/post windows are pure lag/lead expressions on the price column and")
    md.append("   make no reference to outcome labels; the 2 × 2 split applies labels only")
    md.append("   as a downstream stratifier.")
    md.append("3. `taker_price_dollars` = `yes_price_dollars` if `taker_side=='yes'` else")
    md.append("   `no_price_dollars`; the price the taker actually paid per contract.")
    md.append("4. The taker-aligned previous price is the prior trade's `yes_price` (if the")
    md.append("   CURRENT taker is yes) or `no_price` (if no), so both ends of the impact")
    md.append("   subtraction live in the same dollars-per-contract frame.")
    md.append("5. For each N, trades with fewer than N same-market predecessors (resp.")
    md.append("   successors) report null for the corresponding window mean and are")
    md.append("   excluded from that N's `n_pre` / `n_post` count.")
    md.append("6. Sharding by `market_ticker.hash() % n_shards` is lossless: every trade")
    md.append("   for a given market lives in one shard, so the intra-market lag/lead")
    md.append("   windows are computed without boundary error. Means / stds are combined")
    md.append("   across shards from sufficient statistics.")
    md.append("7. The maker view is the algebraic mirror of the taker view on the same")
    md.append("   trade set: `mean_maker_price = 1 - mean_taker_price`; `maker_correct`")
    md.append("   ↔ NOT `taker_correct`. No additional statistical content; reported for")
    md.append("   symmetric framing.")
    md.append("")
    return "\n".join(md)


# ---------- shard driver ----------

def process_shard(
    glob_paths: list[str],
    shard_id: int,
    n_shards: int,
    save_trade_level: bool,
    run_id: str,
) -> tuple[dict, int, int]:
    """Process one market-hash shard.  Returns (partials_dict, n_rows, n_resolved)."""
    # extra_columns='ignore' + missing_columns='insert' lets us tolerate the
    # schema drift between enrich_trades-only partitions (~30 cols) and
    # fully-enriched partitions with milestone/lifecycle joins (~38 cols).
    scan_kwargs = dict(extra_columns="ignore", missing_columns="insert")
    if n_shards == 1:
        lf = pl.scan_parquet(glob_paths, **scan_kwargs).select(SOURCE_COLUMNS)
    else:
        lf = (
            pl.scan_parquet(glob_paths, **scan_kwargs)
              .select(SOURCE_COLUMNS + [
                  (pl.col("market_ticker").hash() % n_shards).cast(pl.UInt32).alias("_shard"),
              ])
              .filter(pl.col("_shard") == shard_id)
              .drop("_shard")
        )
    df = lf.collect()
    n_rows = df.height
    if n_rows == 0:
        return {
            "combined": pl.DataFrame(),
            "by_side": pl.DataFrame(),
            "by_correct": pl.DataFrame(),
            "by_2x2": pl.DataFrame(),
        }, 0, 0

    df = add_per_trade_features(df)
    n_resolved = df.filter(pl.col("was_taker_correct").is_not_null()).height

    df_with_side = df.filter(pl.col("taker_side").is_in(["yes", "no"]))
    df_resolved = df.filter(pl.col("was_taker_correct").is_not_null())
    df_resolved_with_side = df_resolved.filter(pl.col("taker_side").is_in(["yes", "no"]))

    partials = {
        "combined": aggregate_partial(df, group_cols=None),
        "by_side": aggregate_partial(df_with_side, group_cols=["taker_side"]),
        "by_correct": aggregate_partial(df_resolved, group_cols=["was_taker_correct"]),
        "by_2x2": aggregate_partial(df_resolved_with_side, group_cols=["taker_side", "was_taker_correct"]),
    }

    if save_trade_level:
        df_out = df.with_columns(
            pl.col("created_time").dt.strftime("%Y-%m-%d").alias("dt")
        )
        keep = [
            "trade_id", "market_ticker", "created_time", "taker_side",
            "yes_price_dollars", "no_price_dollars", "taker_price_dollars",
            "count", "notional_usd", "was_taker_correct",
            "prev_yes_price_dollars", "prev_taker_price_aligned",
            "price_impact_yes_cents", "price_impact_taker_cents",
            "abs_price_impact_yes_cents",
        ]
        for N in WINDOW_SIZES:
            keep.extend([
                f"avg_yes_pre_{N}", f"avg_yes_post_{N}",
                f"avg_taker_pre_{N}", f"avg_taker_post_{N}",
            ])
        df_out = df_out.select(keep + ["dt"])
        for dt_val in df_out.select(pl.col("dt").unique()).to_series():
            sub = df_out.filter(pl.col("dt") == dt_val).drop("dt")
            out_dir = OUT_DIR / f"dt={dt_val}"
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / f"{run_id}__shard{shard_id:02d}.parquet"
            tmp = out_path.with_suffix(".parquet.tmp")
            sub.write_parquet(tmp, compression="zstd")
            os.replace(tmp, out_path)
            _write_sha256_sidecar(out_path)

    del df, df_with_side, df_resolved, df_resolved_with_side
    gc.collect()
    return partials, n_rows, n_resolved


# ---------- main driver ----------

def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--days", type=int, default=None)
    ap.add_argument("--from-dt", type=str, default=None)
    ap.add_argument("--to-dt", type=str, default=None)
    ap.add_argument("--shards", type=int, default=8, help="Market-hash shards. Use 1 for small workloads (no sharding).")
    ap.add_argument("--save-trade-level", action="store_true",
                    help="Also write per-day per-trade parquets under data/kalshi/derived/price_impact/")
    args = ap.parse_args(argv)

    started_at = time.time()
    started_dt = datetime.now(timezone.utc)
    run_id = f"price_impact_{started_dt:%Y%m%dT%H%M%SZ}_{uuid.uuid4().hex[:8]}"

    parts = discover_partitions(args)
    if not parts:
        _log("ERROR: no enriched_trades partitions found.")
        return 2

    _log(f"run_id={run_id}")
    _log(f"version={VERSION}")
    _log(f"partitions: {len(parts)} ({parts[0].name} .. {parts[-1].name})")
    _log(f"shards: {args.shards}")

    glob_paths = [str(p / "*.parquet") for p in parts]

    n_shards = max(1, args.shards)
    partials_by_scenario: dict[str, list[pl.DataFrame]] = {
        "combined": [],
        "by_side": [],
        "by_correct": [],
        "by_2x2": [],
    }
    n_rows_total = 0
    n_rows_resolved = 0

    for shard_id in range(n_shards):
        t0 = time.time()
        partials, n_rows, n_resolved = process_shard(
            glob_paths=glob_paths,
            shard_id=shard_id,
            n_shards=n_shards,
            save_trade_level=args.save_trade_level,
            run_id=run_id,
        )
        for k, v in partials.items():
            if v.height > 0:
                partials_by_scenario[k].append(v)
        n_rows_total += n_rows
        n_rows_resolved += n_resolved
        _log(
            f"shard {shard_id+1}/{n_shards}: {n_rows:,} rows  "
            f"({n_resolved:,} resolved)  in {time.time() - t0:.1f}s"
        )

    _log("combining sufficient stats across shards …")
    combined = combine_and_derive(partials_by_scenario["combined"], group_cols=None)
    by_side = combine_and_derive(partials_by_scenario["by_side"], group_cols=["taker_side"])
    by_correct = combine_and_derive(partials_by_scenario["by_correct"], group_cols=["was_taker_correct"])
    by_2x2 = combine_and_derive(
        partials_by_scenario["by_2x2"], group_cols=["taker_side", "was_taker_correct"]
    )

    combined_row = combined.row(0, named=True) if combined.height > 0 else {}
    combined_row["group"] = "all"

    by_side_rows = [
        {**r, "group": f"taker={r['taker_side']}"} for r in by_side.iter_rows(named=True)
    ]
    by_correct_rows = [
        {
            **r,
            "group": ("taker_correct=true" if r["was_taker_correct"] else "taker_correct=false"),
        }
        for r in by_correct.iter_rows(named=True)
    ]
    by_2x2_rows = [
        {
            **r,
            "group": f"taker={r['taker_side']} / correct={r['was_taker_correct']}",
        }
        for r in by_2x2.iter_rows(named=True)
    ]

    elapsed = time.time() - started_at
    sample_meta = {
        "started_at_iso": started_dt.isoformat(),
        "elapsed_seconds": elapsed,
        "n_rows_total": n_rows_total,
        "n_rows_resolved": n_rows_resolved,
        "n_shards": n_shards,
    }

    md = render_md(
        run_id=run_id,
        parts=parts,
        sample_meta=sample_meta,
        combined_row=combined_row,
        by_side_rows=by_side_rows,
        by_correct_rows=by_correct_rows,
        by_side_correct_rows=by_2x2_rows,
    )
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    report_path = REPORTS_DIR / f"{run_id}.md"
    tmp_report = report_path.with_suffix(".md.tmp")
    tmp_report.write_text(md, encoding="utf-8")
    os.replace(tmp_report, report_path)
    _log(f"wrote report -> {report_path}")

    audit_payload = {
        "run_id": run_id,
        "version": VERSION,
        "started_at_iso": started_dt.isoformat(),
        "ended_at_iso": datetime.now(timezone.utc).isoformat(),
        "elapsed_seconds": elapsed,
        "args": vars(args),
        "partitions": [p.name for p in parts],
        "n_shards": n_shards,
        "n_rows_total": n_rows_total,
        "n_rows_resolved": n_rows_resolved,
        "report_path": str(report_path),
        "window_sizes": WINDOW_SIZES,
        "source_columns": SOURCE_COLUMNS,
    }
    _write_audit(run_id, audit_payload)
    _log(f"wrote audit -> {AUDIT_DIR / (run_id + '.json')}")

    _log(f"DONE in {elapsed:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
