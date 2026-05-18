#!/usr/bin/env python3
# ruff: noqa: E402
"""
Phase 2.4 — Lifecycle (resolution-event) enrichment.

The forward_markets table reflects market state from periodic polling, so
its `settlement_ts` lags the true resolution by minutes. The
`market_lifecycle_v2` WebSocket pushes resolution events the moment they
happen — typically 0-2 seconds end-to-end.

This script joins forward_lifecycle_ws → enriched_trades to add:

  lifecycle_resolved_unix              int64    exact resolution timestamp
  lifecycle_resolved_result            string   "yes" / "no" / null
  time_to_lifecycle_resolution_seconds int64    (resolved - created)
  is_pre_resolution_window             bool     true if 0 < t <= 600 (10 min)

`is_pre_resolution_window` is the high-conviction signal for "trade
arrived just before resolution" — paired with `was_taker_correct == true`
this is the classic pre-resolution insider signature.

How resolution events are identified
------------------------------------
A lifecycle message marks a resolution when either:
  - status field contains "settled" / "resolved" / "final"
  - result field is populated with "yes" or "no"
  - msg_type contains "settle" / "resolve" / "final"

Per market_ticker we keep the EARLIEST such message (the moment of
resolution, not subsequent re-broadcasts).

Idempotent: drops pre-existing lifecycle_* columns on re-run.

Usage
-----
  uv run python scripts/enrich_with_lifecycle.py --dt 2026-05-15
  uv run python scripts/enrich_with_lifecycle.py --days 30
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

ENRICHED_DIR = DERIVED / "enriched_trades"
LIFECYCLE_DIR = HISTORICAL / "forward_lifecycle_ws"
AUDIT_DIR = STATE / "enrichment_runs"

STAGE_VERSION = "enrich_with_lifecycle@1.0.0"
PRE_RESOLUTION_WINDOW_SECONDS = 600   # 10-min lookback

RESOLUTION_STATUS_TOKENS = ("settle", "resolve", "final")


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
    out = AUDIT_DIR / f"enrich_with_lifecycle_{run_id}.json"
    tmp = out.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, out)


# -------------------- build resolution dim --------------------

def _build_resolution_dim() -> pl.DataFrame:
    """
    Scan forward_lifecycle_ws and produce a (market_ticker, resolved_unix,
    resolved_result) table — earliest resolution event per market.

    Detection rule:
      EITHER status matches one of RESOLUTION_STATUS_TOKENS
      OR     msg_type matches one of those tokens
      OR     result is non-null and non-empty

    Then per market_ticker keep the row with the smallest ts_ms.
    """
    files = sorted(glob.glob(str(LIFECYCLE_DIR / "dt=*/*.parquet")))
    files = [f for f in files if not f.endswith(".tmp")]
    if not files:
        _log("WARN  no forward_lifecycle_ws parquets; skipping resolution derivation")
        return pl.DataFrame()
    _log(f"scanning {len(files):,} lifecycle parquets…")

    lf = pl.scan_parquet(files).select([
        "market_ticker", "ts_ms", "msg_type", "status", "result",
    ]).filter(pl.col("market_ticker").is_not_null())

    # Identify resolution rows: status or msg_type contains a resolution token,
    # OR result is non-null and in {"yes","no"}.
    token_pattern = "|".join(RESOLUTION_STATUS_TOKENS)
    lf = lf.with_columns([
        pl.col("status").cast(pl.Utf8).fill_null("").str.to_lowercase().alias("_status_lc"),
        pl.col("msg_type").cast(pl.Utf8).fill_null("").str.to_lowercase().alias("_type_lc"),
        pl.col("result").cast(pl.Utf8).fill_null("").str.to_lowercase().alias("_result_lc"),
    ])
    lf = lf.filter(
        pl.col("_status_lc").str.contains(token_pattern)
        | pl.col("_type_lc").str.contains(token_pattern)
        | pl.col("_result_lc").is_in(["yes", "no"])
    )
    # Earliest resolution per market_ticker
    lf = (lf.sort(["market_ticker", "ts_ms"], descending=[False, False], nulls_last=True)
            .group_by("market_ticker", maintain_order=True)
            .head(1))
    df = (lf.select([
            "market_ticker",
            (pl.col("ts_ms") // 1000).cast(pl.Int64).alias("lifecycle_resolved_unix"),
            pl.when(pl.col("_result_lc").is_in(["yes", "no"]))
              .then(pl.col("_result_lc"))
              .otherwise(pl.lit(None, dtype=pl.Utf8))
              .alias("lifecycle_resolved_result"),
          ])
          .collect())
    _log(f"resolution dim: {df.height:,} markets with captured resolution events")
    return df


# -------------------- per-partition enrich --------------------

def _enrich_partition(dt: str, dim: pl.DataFrame, run_id: str) -> dict:
    in_files = sorted(glob.glob(str(ENRICHED_DIR / f"dt={dt}/*.parquet")))
    in_files = [f for f in in_files if not f.endswith(".tmp")]
    if not in_files:
        return {"dt": dt, "error": "no_enriched_input"}

    n_files = 0
    n_rows = 0
    n_with_lifecycle = 0
    n_in_window = 0
    for fp in in_files:
        df = pl.read_parquet(fp)
        for c in ("lifecycle_resolved_unix", "lifecycle_resolved_result",
                  "time_to_lifecycle_resolution_seconds", "is_pre_resolution_window"):
            if c in df.columns:
                df = df.drop(c)

        if dim.is_empty():
            df = df.with_columns([
                pl.lit(None, dtype=pl.Int64).alias("lifecycle_resolved_unix"),
                pl.lit(None, dtype=pl.Utf8).alias("lifecycle_resolved_result"),
                pl.lit(None, dtype=pl.Int64).alias("time_to_lifecycle_resolution_seconds"),
                pl.lit(False, dtype=pl.Boolean).alias("is_pre_resolution_window"),
            ])
        else:
            df = df.join(dim, on="market_ticker", how="left")
            df = df.with_columns(
                (pl.col("lifecycle_resolved_unix") - pl.col("created_time_unix"))
                    .cast(pl.Int64)
                    .alias("time_to_lifecycle_resolution_seconds")
            )
            df = df.with_columns(
                ((pl.col("time_to_lifecycle_resolution_seconds") > 0)
                 & (pl.col("time_to_lifecycle_resolution_seconds") <= PRE_RESOLUTION_WINDOW_SECONDS))
                    .alias("is_pre_resolution_window")
            )

        n_rows += df.height
        n_with_lifecycle += int((df["lifecycle_resolved_unix"].is_not_null()).sum() or 0)
        n_in_window += int((df["is_pre_resolution_window"]).sum() or 0)

        tmp_path = Path(fp).with_suffix(".parquet.tmp")
        df.write_parquet(str(tmp_path), compression="zstd")
        tmp_path.replace(fp)
        _write_sha256_sidecar(Path(fp))
        n_files += 1

    pct = (100.0 * n_with_lifecycle / n_rows) if n_rows else 0.0
    pct_win = (100.0 * n_in_window / n_rows) if n_rows else 0.0
    _log(f"  dt={dt}  files={n_files}  rows={n_rows:,}  with_lifecycle={n_with_lifecycle:,} ({pct:.1f}%)  in_pre_resolution={n_in_window:,} ({pct_win:.2f}%)")
    return {
        "dt": dt,
        "files": n_files,
        "rows": n_rows,
        "with_lifecycle": n_with_lifecycle,
        "in_pre_resolution_window": n_in_window,
    }


# -------------------- main --------------------

def _list_dates(args) -> list[str]:
    if args.dt:
        return [args.dt]
    parts = sorted({Path(p).name for p in glob.glob(str(ENRICHED_DIR / "dt=*"))})
    parts = [p.replace("dt=", "") for p in parts if p.startswith("dt=")]
    if args.all:
        return parts
    if not parts:
        return []
    last_dt = parts[-1]
    cutoff = (datetime.fromisoformat(last_dt) - timedelta(days=args.days)).date().isoformat()
    return [p for p in parts if p >= cutoff]


def main() -> int:
    ap = argparse.ArgumentParser(description="Stage 4 - join lifecycle WS resolution events")
    ap.add_argument("--dt",   type=str, default=None)
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--all",  action="store_true")
    args = ap.parse_args()

    run_id = f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"
    started = time.time()
    dates = _list_dates(args)

    _log("=" * 72)
    _log(f"enrich_with_lifecycle run_id={run_id}")
    _log(f"  version    = {STAGE_VERSION}")
    _log(f"  partitions = {len(dates)}")
    _log(f"  pre_resolution_window = {PRE_RESOLUTION_WINDOW_SECONDS} sec")
    _log("=" * 72)

    if not dates:
        return 0

    dim = _build_resolution_dim()

    per_dt: list[dict] = []
    for i, dt in enumerate(dates, 1):
        _log(f"[{i}/{len(dates)}] dt={dt}")
        try:
            per_dt.append(_enrich_partition(dt, dim, run_id))
        except Exception as exc:
            _log(f"  dt={dt}  ERROR  {exc!r}")
            per_dt.append({"dt": dt, "error": repr(exc)})

    audit = {
        "run_id": run_id,
        "started_at_utc": datetime.fromtimestamp(started, tz=timezone.utc).isoformat(),
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "duration_seconds": round(time.time() - started, 3),
        "version": STAGE_VERSION,
        "pre_resolution_window_seconds": PRE_RESOLUTION_WINDOW_SECONDS,
        "resolution_dim_rows": dim.height,
        "per_dt": per_dt,
    }
    _write_audit(run_id, audit)
    _log("=" * 72)
    _log(f"DONE  duration={time.time()-started:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
