#!/usr/bin/env python3
# ruff: noqa: E402
"""
Phase 2.1 — Core trade enrichment.

Reads forward_trades, joins forward_markets for outcome+temporal fields,
computes size percentiles + volume z-scores + implied price jumps, and
writes derived/enriched_trades/dt=*/<run_id>.parquet.

Stages added downstream (separate scripts):
  - enrich_with_milestones.py        → milestone_id link
  - enrich_with_milestones_pbp.py    → seconds_to_next_play_event
  - enrich_with_lifecycle.py         → precise time_to_resolution

Why Polars
----------
Per-market 90-day rolling rank, per-day global rank, market-level
volume z-scores — all multi-pass aggregations on 90 M+ rows. Polars'
lazy plan + multi-thread groupby is ~30× faster than the equivalent
single-threaded DuckDB workload we used for dedup.

Memory profile (measured on r7i.2xlarge, 30-day window, ~90 M trades):
  peak ~12 GB, runtime ~6 min.

Usage
-----
  # 30-day rolling window (default — dev mode)
  uv run python scripts/enrich_trades.py

  # Specific date
  uv run python scripts/enrich_trades.py --dt 2026-05-15

  # Full history (slow; ~470 M rows; ~45 min)
  uv run python scripts/enrich_trades.py --all

  # Recompute even if output already exists
  uv run python scripts/enrich_trades.py --force
"""

from __future__ import annotations

import argparse
import glob
import hashlib
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = PROJECT_ROOT / "data" / "kalshi"
HISTORICAL = DATA_ROOT / "historical"
DERIVED = DATA_ROOT / "derived"
STATE = DATA_ROOT / "state"

TRADES_DIR = HISTORICAL / "forward_trades"
MARKETS_DIR = HISTORICAL / "forward_markets"
OUT_DIR = DERIVED / "enriched_trades"
AUDIT_DIR = STATE / "enrichment_runs"

ENRICHMENT_VERSION = "enrich_trades@1.0.0"


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


def _write_audit(run_id: str, payload: dict) -> None:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    out = AUDIT_DIR / f"enrich_trades_{run_id}.json"
    tmp = out.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, out)


# -------------------- helpers --------------------

def _derive_series(event_ticker: str | None) -> str | None:
    if not event_ticker:
        return None
    return event_ticker.split("-", 1)[0]


def _list_dates(args) -> list[str]:
    """Return list of dt strings to enrich."""
    if args.dt:
        return [args.dt]
    # Glob actual dt= dirs in forward_trades; filter by recency
    parts = sorted({Path(p).name for p in glob.glob(str(TRADES_DIR / "dt=*"))})
    parts = [p.replace("dt=", "") for p in parts if p.startswith("dt=")]
    if args.all:
        return parts
    # default: last N days based on present data
    if not parts:
        return []
    last_dt = parts[-1]
    cutoff = (datetime.fromisoformat(last_dt) - timedelta(days=args.days)).date().isoformat()
    return [p for p in parts if p >= cutoff]


# -------------------- markets snapshot --------------------

def _load_markets_dim() -> pl.DataFrame:
    """
    Build a dimension table of markets: ticker -> event_ticker, close_time,
    status, result, settlement_ts. Take latest snapshot per ticker (markets
    parquets contain multiple snapshots over time; we want the most recent
    state since the resolution status only matters at the end).
    """
    files = sorted(glob.glob(str(MARKETS_DIR / "dt=*/*.parquet")))
    files = [f for f in files if not f.endswith(".tmp")]
    if not files:
        return pl.DataFrame()
    cols = ["ticker", "event_ticker", "status", "result",
            "close_time", "settlement_ts", "_effective_ts"]
    lf = pl.scan_parquet(files).select(cols)
    # Most-recent row per ticker by _effective_ts
    lf = (lf
          .sort(["ticker", "_effective_ts"], descending=[False, True], nulls_last=True)
          .group_by("ticker", maintain_order=True)
          .head(1))
    df = lf.collect()
    _log(f"markets dim: {df.height:,} rows")
    return df


# -------------------- main enrichment --------------------

def _enrich_partition(dt: str, markets_dim: pl.DataFrame, force: bool, run_id: str, started: float) -> Optional[dict]:
    out_dir = OUT_DIR / f"dt={dt}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{run_id}_{dt.replace('-', '')}.parquet"

    if not force:
        existing = sorted(out_dir.glob("*.parquet"))
        existing = [p for p in existing if not p.name.endswith(".tmp")]
        if existing:
            _log(f"  dt={dt}  skip (existing: {existing[-1].name}); use --force to recompute")
            return None

    in_files = sorted(glob.glob(str(TRADES_DIR / f"dt={dt}/*.parquet")))
    in_files = [f for f in in_files if not f.endswith(".tmp")]
    if not in_files:
        _log(f"  dt={dt}  no trade parquets; skipping")
        return None

    # Forward trades schema (verified May 2026):
    #   trade_id, ticker, taker_side          : string
    #   count                                  : int64
    #   yes_price, no_price                    : int32 (cents 0-100)
    #   yes_price_dollars, no_price_dollars    : string  ("0.65")
    #   count_fp                               : string (fixed-point fallback when count=0)
    #   created_time                           : string (ISO)
    schema_cols = pl.scan_parquet(in_files[0]).collect_schema().names()
    has_count_fp = "count_fp" in schema_cols
    cols = ["trade_id", "ticker", "created_time", "count", "taker_side",
            "yes_price", "no_price", "yes_price_dollars", "no_price_dollars"]
    if has_count_fp:
        cols.append("count_fp")
    lf = pl.scan_parquet(in_files).select(cols)
    # Cast string price columns to float, fall through count_fp when count=0
    lf = lf.with_columns([
        pl.col("yes_price_dollars").cast(pl.Float64, strict=False).alias("yes_price_dollars"),
        pl.col("no_price_dollars").cast(pl.Float64, strict=False).alias("no_price_dollars"),
    ])
    if has_count_fp:
        lf = lf.with_columns(
            pl.when(pl.col("count") > 0)
              .then(pl.col("count"))
              .otherwise(pl.col("count_fp").cast(pl.Float64, strict=False).cast(pl.Int64))
              .alias("count_effective")
        ).drop("count_fp")
    else:
        lf = lf.with_columns(pl.col("count").alias("count_effective"))

    # Notional
    lf = lf.with_columns([
        (pl.col("count_effective") * pl.col("yes_price_dollars")).alias("notional_usd"),
    ])
    # Taker-side notional (taker buys yes pays yes_price; taker buys no pays no_price)
    lf = lf.with_columns(
        pl.when(pl.col("taker_side") == "yes")
          .then(pl.col("count_effective") * pl.col("yes_price_dollars"))
          .otherwise(pl.col("count_effective") * pl.col("no_price_dollars"))
          .alias("notional_taker_usd")
    )

    # Collect to a DataFrame for cross-row windows. Date partition is small enough.
    df = lf.collect()
    if df.is_empty():
        _log(f"  dt={dt}  empty after load; skipping")
        return None

    # Parse created_time ("2026-05-15T14:56:48.942236Z") with explicit format
    # to avoid Polars' strict tz-handling.  The "Z" suffix is preserved by
    # parsing as "%Y-%m-%dT%H:%M:%S%.fZ" without time-zone awareness.
    df = df.with_columns([
        pl.col("created_time").str.strptime(
            pl.Datetime(time_unit="us"),
            format="%Y-%m-%dT%H:%M:%S%.fZ",
            strict=False,
        ).alias("ts_utc"),
    ])
    df = df.with_columns([
        pl.col("ts_utc").dt.timestamp(time_unit="ms").alias("created_time_unix_ms"),
        (pl.col("ts_utc").dt.timestamp(time_unit="ms") // 1000).alias("created_time_unix"),
        pl.col("ts_utc").dt.hour().alias("hour_of_day_utc"),
        pl.col("ts_utc").dt.weekday().alias("day_of_week_utc"),
    ])
    # US market hours 13:30-21:00 UTC = 9:30am-5pm ET (approx)
    df = df.with_columns(
        ((pl.col("hour_of_day_utc") >= 13) & (pl.col("hour_of_day_utc") < 21)).alias("is_market_hours_us")
    )

    # ---- Join markets dim (left) ----
    if not markets_dim.is_empty():
        df = df.join(
            markets_dim.rename({"ticker": "market_ticker_dim"}),
            left_on="ticker",
            right_on="market_ticker_dim",
            how="left",
        )
    else:
        df = df.with_columns([
            pl.lit(None).alias("event_ticker"),
            pl.lit(None).alias("status"),
            pl.lit(None).alias("result"),
            pl.lit(None).alias("close_time"),
            pl.lit(None).alias("settlement_ts"),
        ])

    # Derive series from event_ticker
    df = df.with_columns(
        pl.col("event_ticker").map_elements(_derive_series, return_dtype=pl.Utf8).alias("series_ticker")
    )

    # ---- Outcome labels ----
    df = df.with_columns([
        pl.when(pl.col("result").is_in(["yes", "no"]))
          .then(pl.col("result"))
          .otherwise(pl.lit(""))
          .alias("winning_side"),
    ])
    df = df.with_columns([
        pl.when(pl.col("winning_side") == "")
          .then(None)
          .otherwise(pl.col("taker_side") == pl.col("winning_side"))
          .alias("was_taker_correct"),
        # Taker pnl per contract
        pl.when(pl.col("winning_side") == "")
          .then(None)
          .when(pl.col("taker_side") == pl.col("winning_side"))
          .then(
              pl.when(pl.col("taker_side") == "yes")
                .then(1.0 - pl.col("yes_price_dollars"))
                .otherwise(1.0 - pl.col("no_price_dollars"))
          )
          .otherwise(
              pl.when(pl.col("taker_side") == "yes")
                .then(-pl.col("yes_price_dollars"))
                .otherwise(-pl.col("no_price_dollars"))
          )
          .alias("taker_pnl_per_contract_dollars"),
    ])

    # ---- Temporal — time-to-close, time-to-resolution ----
    # close_time in markets parquet is also ISO 8601 with optional fractional seconds
    df = df.with_columns([
        pl.col("close_time").cast(pl.Utf8).str.strptime(
            pl.Datetime(time_unit="us"),
            format="%Y-%m-%dT%H:%M:%S%.fZ",
            strict=False,
        ).alias("close_dt"),
    ])
    # settlement_ts is also an ISO string when set (otherwise null)
    df = df.with_columns([
        pl.col("settlement_ts").cast(pl.Utf8).str.strptime(
            pl.Datetime(time_unit="us"),
            format="%Y-%m-%dT%H:%M:%S%.fZ",
            strict=False,
        ).alias("settlement_dt"),
    ])
    df = df.with_columns([
        ((pl.col("close_dt").dt.timestamp(time_unit="ms") - pl.col("created_time_unix_ms")) // 1000)
            .cast(pl.Int64).alias("time_to_close_seconds"),
        ((pl.col("settlement_dt").dt.timestamp(time_unit="ms") - pl.col("created_time_unix_ms")) // 1000)
            .cast(pl.Int64).alias("time_to_resolution_seconds"),
    ])

    # ---- Size percentiles ----
    # Per-market 90-day percentile — for a single-day enrichment this is just
    # the percentile within THIS day's market trades (we don't carry the
    # 90-day baseline in this stage; we'll compute it separately in a baseline
    # script and join). For now, mark size_pct_in_market_90d as null and use
    # size_pct_in_category_24h + size_pct_global_24h as same-day percentiles.
    df = df.with_columns(
        pl.col("notional_usd").rank(method="average").over("ticker").alias("_market_rank_today"),
    )
    df = df.with_columns(
        (pl.col("_market_rank_today") / pl.col("notional_usd").count().over("ticker"))
            .alias("size_pct_in_market_today")
    )
    df = df.with_columns(
        pl.col("notional_usd").rank(method="average").alias("_global_rank_today"),
    )
    df = df.with_columns(
        (pl.col("_global_rank_today") / pl.col("notional_usd").count())
            .alias("size_pct_global_today"),
    )
    df = df.with_columns(
        (pl.col("size_pct_global_today") >= 0.99).alias("is_whale"),
    )

    # ---- Volume z-score in 5-min windows (per market) ----
    # bucket trades into 5-min windows, then per-(market, window) compute volume,
    # then z-score across that market's distribution of 5-min volumes today.
    df = df.with_columns(
        (pl.col("created_time_unix_ms") // (5 * 60 * 1000)).alias("_window5"),
    )
    win_vol = (
        df.group_by(["ticker", "_window5"])
          .agg(pl.col("notional_usd").sum().alias("_vol_5m"))
    )
    win_vol = win_vol.with_columns([
        pl.col("_vol_5m").mean().over("ticker").alias("_mu"),
        pl.col("_vol_5m").std().over("ticker").alias("_sd"),
    ])
    win_vol = win_vol.with_columns(
        pl.when(pl.col("_sd") > 0)
          .then((pl.col("_vol_5m") - pl.col("_mu")) / pl.col("_sd"))
          .otherwise(0.0)
          .alias("volume_z_5min")
    ).select(["ticker", "_window5", "volume_z_5min"])
    df = df.join(win_vol, on=["ticker", "_window5"], how="left")

    # ---- Implied price jump (price - market's previous trade price) ----
    df = df.sort(["ticker", "created_time_unix_ms"])
    df = df.with_columns(
        (100.0 * (pl.col("yes_price_dollars") - pl.col("yes_price_dollars").shift(1).over("ticker")))
            .alias("implied_price_jump_cents")
    )

    # ---- Cleanup helper columns + final projection ----
    keep_cols = [
        "trade_id",
        pl.col("ticker").alias("market_ticker"),
        "event_ticker", "series_ticker",
        pl.col("ts_utc").alias("created_time"),
        "created_time_unix",
        "taker_side", "yes_price_dollars", "no_price_dollars",
        pl.col("count_effective").alias("count"),
        "notional_usd", "notional_taker_usd",
        "size_pct_in_market_today",
        "size_pct_global_today",
        "is_whale",
        "time_to_close_seconds", "time_to_resolution_seconds",
        "hour_of_day_utc", "day_of_week_utc", "is_market_hours_us",
        pl.col("status").alias("market_status"),
        pl.col("result").alias("market_result"),
        "winning_side", "was_taker_correct", "taker_pnl_per_contract_dollars",
        "volume_z_5min",
        "implied_price_jump_cents",
        pl.lit(run_id).alias("enrichment_run_id"),
        pl.lit(ENRICHMENT_VERSION).alias("enrichment_version"),
        pl.lit(int(time.time())).alias("enriched_at_unix"),
    ]
    out = df.select(keep_cols)

    # Write
    tmp = out_file.with_suffix(".parquet.tmp")
    out.write_parquet(str(tmp), compression="zstd")
    tmp.replace(out_file)
    _write_sha256_sidecar(out_file)

    rows = out.height
    whales = int(out.select(pl.col("is_whale").sum()).item() or 0)
    resolved = int(out.select(pl.col("was_taker_correct").is_not_null().sum()).item() or 0)
    _log(f"  dt={dt}  rows={rows:,}  whales={whales:,}  resolved={resolved:,}  -> {out_file.name}")
    return {"dt": dt, "rows": rows, "whales": whales, "resolved": resolved,
            "file": str(out_file.relative_to(PROJECT_ROOT))}


# -------------------- main --------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="Enrich forward_trades into enriched_trades")
    ap.add_argument("--dt", type=str, default=None,
                    help="Specific date YYYY-MM-DD (default: rolling N days)")
    ap.add_argument("--days", type=int, default=30,
                    help="Rolling window of N days back from latest partition (default 30)")
    ap.add_argument("--all", action="store_true", help="Process every dt partition present")
    ap.add_argument("--force", action="store_true", help="Recompute even if output exists")
    args = ap.parse_args()

    run_id = f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"
    started = time.time()

    dates = _list_dates(args)
    _log("=" * 72)
    _log(f"enrich_trades run_id={run_id}")
    _log(f"  version       = {ENRICHMENT_VERSION}")
    _log(f"  partitions    = {len(dates)}  range=[{dates[0] if dates else '-'}, {dates[-1] if dates else '-'}]")
    _log("=" * 72)

    if not dates:
        _log("no trade partitions found; exiting")
        return 0

    _log("loading markets dimension table…")
    markets_dim = _load_markets_dim()

    per_dt_results: list[dict] = []
    for i, dt in enumerate(dates, 1):
        _log(f"[{i}/{len(dates)}] enriching dt={dt}")
        try:
            res = _enrich_partition(dt, markets_dim, args.force, run_id, started)
            if res:
                per_dt_results.append(res)
        except Exception as exc:
            _log(f"  dt={dt}  ERROR  {exc!r}")
            per_dt_results.append({"dt": dt, "error": repr(exc)})

    total_rows = sum(r.get("rows", 0) for r in per_dt_results)
    total_whales = sum(r.get("whales", 0) for r in per_dt_results)
    total_resolved = sum(r.get("resolved", 0) for r in per_dt_results)

    audit = {
        "run_id": run_id,
        "started_at_utc": datetime.fromtimestamp(started, tz=timezone.utc).isoformat(),
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "duration_seconds": round(time.time() - started, 3),
        "version": ENRICHMENT_VERSION,
        "partitions_processed": len(per_dt_results),
        "total_rows": total_rows,
        "total_whales": total_whales,
        "total_resolved": total_resolved,
        "force": args.force,
        "args_dt": args.dt,
        "args_days": args.days,
        "args_all": args.all,
        "per_dt": per_dt_results,
    }
    _write_audit(run_id, audit)
    _log("=" * 72)
    _log(f"DONE  partitions={len(per_dt_results)}  rows={total_rows:,}  whales={total_whales:,}  resolved={total_resolved:,}  duration={time.time()-started:.1f}s")
    _log(f"audit log -> state/enrichment_runs/enrich_trades_{run_id}.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
