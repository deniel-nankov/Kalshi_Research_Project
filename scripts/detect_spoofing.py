#!/usr/bin/env python3
# ruff: noqa: E402
"""
Phase 4.5 — Spoofing / Layering Detector.

Kalshi's public orderbook_delta feed does NOT include order_id, so we
cannot follow a single order across its lifecycle. What we CAN do is
detect the PATTERN at the price-level: a large positive delta_fp (large
add) followed within a short time by a large negative delta_fp at the
SAME price level, with no trade fill at that price between the two.

That is the textbook spoofing signature — display large size to bait
counter-flow, then cancel before being filled.

Algorithm
---------
For each (market_ticker, price_dollars, side) lane, walk the timestamp-
sorted orderbook_delta events and the trade events. Mark a delta event
as a "suspect spoof" when:

  1. delta_fp > MIN_SPOOF_QTY   (large add, default 100 contracts)
  2. there exists a later delta event AT THE SAME (market, price, side)
     in the next SPOOF_WINDOW_SECONDS where -delta_fp >= 0.8 * original
     positive delta_fp (large cancel)
  3. between those two events, NO trade fill at that (market, price)
     beat the add by enough to plausibly be the resting order

The output is one row per "suspect spoof event" with:
  market_ticker, price_dollars, side, add_ts_ms, cancel_ts_ms,
  add_size, cancel_size, hold_ms, run_id.

We ALSO produce a per-market hourly aggregate `spoof_intensity` that
downstream detectors can join to trades.

Notes
-----
- This is a HIGH-FALSE-POSITIVE detector. Real market makers continually
  refresh quotes — that's quoting, not spoofing. The intent inference
  requires looking at WHETHER the displayed size affected trades (price
  moved in the direction the spoofer wanted). For MVP we ship the raw
  pattern detection and let the smart-money board surface unusually
  high spoof_intensity markets for manual review.

- Per-market polars groupby on hundreds of millions of orderbook delta
  rows is heavy. The script accepts --dt and --days filters; default is
  a 3-day window. For backtest we'll run it as a separate batch job.

Outputs
-------
  data/kalshi/derived/spoof_flags/dt=YYYY-MM-DD/<run_id>.parquet
  data/kalshi/derived/spoof_intensity_hourly/dt=YYYY-MM-DD/<run_id>.parquet
  state/detector_runs/detect_spoofing_<run_id>.json
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

import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = PROJECT_ROOT / "data" / "kalshi"
HISTORICAL = DATA_ROOT / "historical"
DERIVED = DATA_ROOT / "derived"
STATE = DATA_ROOT / "state"

OB_DELTAS_DIR = HISTORICAL / "forward_orderbook_deltas"
TRADES_WS_DIR = HISTORICAL / "forward_trades_ws"
OUT_FLAGS_DIR = DERIVED / "spoof_flags"
OUT_HOURLY_DIR = DERIVED / "spoof_intensity_hourly"
AUDIT_DIR = STATE / "detector_runs"

DETECTOR_VERSION = "detect_spoofing@1.0.0"
MIN_SPOOF_QTY = 100.0
SPOOF_WINDOW_SECONDS = 30


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
    out = AUDIT_DIR / f"detect_spoofing_{run_id}.json"
    tmp = out.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, out)


# -------------------- core detector --------------------

def _detect_in_partition(dt: str, run_id: str, min_qty: float, window_sec: int) -> dict:
    in_files = sorted(glob.glob(str(OB_DELTAS_DIR / f"dt={dt}/*.parquet")))
    in_files = [f for f in in_files if not f.endswith(".tmp")]
    if not in_files:
        _log(f"  dt={dt}  no orderbook_deltas; skipping")
        return {"dt": dt, "error": "no_orderbook_input"}

    # delta_fp is fixed-point (string) — cast to Float64
    schema_cols = pl.scan_parquet(in_files[0]).collect_schema().names()
    cols = ["market_ticker", "price_dollars", "side", "delta_fp", "ts_ms"]
    cols = [c for c in cols if c in schema_cols]
    if not all(c in cols for c in ("market_ticker", "price_dollars", "side", "delta_fp", "ts_ms")):
        return {"dt": dt, "error": "missing_columns"}

    df = pl.read_parquet(in_files, columns=cols)
    df = df.with_columns([
        pl.col("delta_fp").cast(pl.Float64, strict=False).alias("dfp"),
        pl.col("price_dollars").cast(pl.Float64, strict=False).alias("price_f"),
    ]).drop(["delta_fp", "price_dollars"])

    # Sort and detect: for each (market, price, side), find rows where dfp > min_qty (large add)
    df = df.sort(["market_ticker", "price_f", "side", "ts_ms"])

    # The add candidates
    adds = df.filter(pl.col("dfp") > min_qty).with_columns(
        pl.col("ts_ms").alias("add_ts_ms"),
        pl.col("dfp").alias("add_size"),
    )

    # For each add, find next negative delta at same lane within window
    # Approach: self-join on (market_ticker, price_f, side) and filter rows
    # where cancel.ts_ms - add.ts_ms in (0, window_ms].
    window_ms = window_sec * 1000
    cancels = df.filter(pl.col("dfp") < 0).with_columns(
        pl.col("ts_ms").alias("cancel_ts_ms"),
        pl.col("dfp").alias("cancel_delta"),
    )

    # Polars join_asof: for each add, find the nearest later cancel in the same lane
    a = adds.sort(["market_ticker", "price_f", "side", "add_ts_ms"]).rename({"ts_ms": "add_ts_ms_2"})
    c = cancels.sort(["market_ticker", "price_f", "side", "cancel_ts_ms"]).rename({"ts_ms": "cancel_ts_ms_2"})
    paired = a.join_asof(
        c.select(["market_ticker", "price_f", "side", "cancel_ts_ms", "cancel_delta"]),
        left_on="add_ts_ms",
        right_on="cancel_ts_ms",
        by=["market_ticker", "price_f", "side"],
        strategy="forward",
    )
    paired = paired.filter(
        pl.col("cancel_ts_ms").is_not_null()
        & ((pl.col("cancel_ts_ms") - pl.col("add_ts_ms")) <= window_ms)
        & ((pl.col("cancel_ts_ms") - pl.col("add_ts_ms")) > 0)
        & ((-pl.col("cancel_delta")) >= 0.8 * pl.col("add_size"))
    ).with_columns(
        (pl.col("cancel_ts_ms") - pl.col("add_ts_ms")).alias("hold_ms"),
    )

    # Best-effort: filter out trade fills at the same lane between add and cancel.
    # Without per-order id this is approximate; cheap and useful at MVP.
    trades_files = sorted(glob.glob(str(TRADES_WS_DIR / f"dt={dt}/*.parquet")))
    trades_files = [f for f in trades_files if not f.endswith(".tmp")]
    if trades_files:
        try:
            tcols_avail = pl.scan_parquet(trades_files[0]).collect_schema().names()
            tcols = [c for c in ("market_ticker", "price_dollars", "ts_ms") if c in tcols_avail]
            if all(c in tcols for c in ("market_ticker", "price_dollars", "ts_ms")):
                tr = (pl.read_parquet(trades_files, columns=tcols)
                        .with_columns(pl.col("price_dollars").cast(pl.Float64, strict=False).alias("price_f"))
                        .drop("price_dollars")
                        .select(["market_ticker", "price_f", "ts_ms"]))
                # For each spoof candidate, count trade fills at (market, price) in [add, cancel]
                paired = paired.with_columns(pl.lit(0, dtype=pl.Int64).alias("_trade_count"))
                # Naive join: explode small per-key — for MVP, group trades by (market, price) and check ranges
                paired_with_idx = paired.with_row_index("_idx")
                joined = paired_with_idx.join(tr, on=["market_ticker", "price_f"], how="left")
                joined = joined.filter(
                    (pl.col("ts_ms") >= pl.col("add_ts_ms"))
                    & (pl.col("ts_ms") <= pl.col("cancel_ts_ms"))
                )
                trade_counts = joined.group_by("_idx").agg(pl.len().alias("_trade_count_real"))
                paired = paired.with_row_index("_idx").join(trade_counts, on="_idx", how="left")
                paired = paired.with_columns(pl.col("_trade_count_real").fill_null(0).alias("trade_count_in_window"))
                paired = paired.filter(pl.col("trade_count_in_window") == 0).drop(["_idx", "_trade_count_real", "_trade_count"])
            else:
                paired = paired.with_columns(pl.lit(0, dtype=pl.Int64).alias("trade_count_in_window"))
        except Exception as exc:
            _log(f"  trade-fill filter failed ({exc!r}); proceeding without filter")
            paired = paired.with_columns(pl.lit(0, dtype=pl.Int64).alias("trade_count_in_window"))
    else:
        paired = paired.with_columns(pl.lit(0, dtype=pl.Int64).alias("trade_count_in_window"))

    # Final spoof events
    spoof_events = paired.select([
        "market_ticker", "price_f", "side",
        "add_ts_ms", "cancel_ts_ms", "hold_ms",
        "add_size",
        (-pl.col("cancel_delta")).alias("cancel_size"),
        "trade_count_in_window",
    ]).with_columns(
        pl.lit(run_id).alias("run_id"),
        pl.lit(DETECTOR_VERSION).alias("detector_version"),
    )

    # Per-market hourly spoof intensity
    intensity = (spoof_events
                 .with_columns((pl.col("add_ts_ms") // 3_600_000).alias("hour_bucket"))
                 .group_by(["market_ticker", "hour_bucket"])
                 .agg([
                     pl.len().alias("spoof_events"),
                     pl.col("add_size").sum().alias("spoof_added_size"),
                     pl.col("hold_ms").mean().alias("avg_hold_ms"),
                 ])
                 .with_columns(
                     pl.lit(run_id).alias("run_id"),
                 ))

    out_dir1 = OUT_FLAGS_DIR / f"dt={dt}"
    out_dir1.mkdir(parents=True, exist_ok=True)
    out_file1 = out_dir1 / f"{run_id}_{dt.replace('-', '')}.parquet"
    tmp1 = out_file1.with_suffix(".parquet.tmp")
    spoof_events.write_parquet(str(tmp1), compression="zstd")
    tmp1.replace(out_file1)
    _write_sha256_sidecar(out_file1)

    out_dir2 = OUT_HOURLY_DIR / f"dt={dt}"
    out_dir2.mkdir(parents=True, exist_ok=True)
    out_file2 = out_dir2 / f"{run_id}_{dt.replace('-', '')}.parquet"
    tmp2 = out_file2.with_suffix(".parquet.tmp")
    intensity.write_parquet(str(tmp2), compression="zstd")
    tmp2.replace(out_file2)
    _write_sha256_sidecar(out_file2)

    _log(f"  dt={dt}  ob_deltas={df.height:,}  spoof_events={spoof_events.height:,}  intensity_buckets={intensity.height:,}")
    return {
        "dt": dt,
        "orderbook_delta_rows": df.height,
        "spoof_events": spoof_events.height,
        "intensity_buckets": intensity.height,
        "events_file": str(out_file1.relative_to(PROJECT_ROOT)),
        "intensity_file": str(out_file2.relative_to(PROJECT_ROOT)),
    }


# -------------------- main --------------------

def _list_dates(args) -> list[str]:
    if args.dt:
        return [args.dt]
    parts = sorted({Path(p).name for p in glob.glob(str(OB_DELTAS_DIR / "dt=*"))})
    parts = [p.replace("dt=", "") for p in parts if p.startswith("dt=")]
    if args.all:
        return parts
    if not parts:
        return []
    last_dt = parts[-1]
    cutoff = (datetime.fromisoformat(last_dt) - timedelta(days=args.days)).date().isoformat()
    return [p for p in parts if p >= cutoff]


def main() -> int:
    ap = argparse.ArgumentParser(description="Detect spoofing patterns in forward_orderbook_deltas")
    ap.add_argument("--dt",       type=str, default=None)
    ap.add_argument("--days",     type=int, default=3)
    ap.add_argument("--all",      action="store_true")
    ap.add_argument("--min-qty",  type=float, default=MIN_SPOOF_QTY)
    ap.add_argument("--window",   type=int, default=SPOOF_WINDOW_SECONDS)
    args = ap.parse_args()

    run_id = f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"
    started = time.time()
    dates = _list_dates(args)

    _log("=" * 72)
    _log(f"detect_spoofing run_id={run_id}")
    _log(f"  version    = {DETECTOR_VERSION}")
    _log(f"  partitions = {len(dates)}")
    _log(f"  min_qty    = {args.min_qty}")
    _log(f"  window     = {args.window} sec")
    _log("=" * 72)

    if not dates:
        return 0

    per_dt: list[dict] = []
    for i, dt in enumerate(dates, 1):
        _log(f"[{i}/{len(dates)}] dt={dt}")
        try:
            per_dt.append(_detect_in_partition(dt, run_id, args.min_qty, args.window))
        except Exception as exc:
            _log(f"  dt={dt}  ERROR  {exc!r}")
            per_dt.append({"dt": dt, "error": repr(exc)})

    audit = {
        "run_id": run_id,
        "started_at_utc": datetime.fromtimestamp(started, tz=timezone.utc).isoformat(),
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "duration_seconds": round(time.time() - started, 3),
        "version": DETECTOR_VERSION,
        "min_qty": args.min_qty,
        "window_seconds": args.window,
        "per_dt": per_dt,
    }
    _write_audit(run_id, audit)
    _log("=" * 72)
    _log(f"DONE  duration={time.time()-started:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
