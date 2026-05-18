#!/usr/bin/env python3
# ruff: noqa: E402
"""
Phase 2.2 — Milestone-link enrichment.

Adds 4 columns to enriched_trades parquets:
  - milestone_id
  - milestone_type           (e.g. football_game, tennis_tournament_singles)
  - milestone_category       (Sports / Esports / Elections / Crypto / ...)
  - milestone_title          (e.g. "Indiana at New York")

Linkage: trade.event_ticker is matched against milestones.primary_event_tickers
(an array). One event can be referenced by multiple milestones over time; we
keep the milestone with the highest last_updated_unix as the canonical link.

This script is idempotent: it overwrites existing milestone_* columns on
re-run, so calibration changes are safe.

Why an in-place rewrite (vs. a separate table)?
  Each enrichment stage owns a few columns, but they all live in the same
  enriched_trades parquet so downstream detectors get a single dense scan.
  This matches the SCHEMA.md design — atomic per-partition rewrite via
  tmp+rename, fresh SHA-256 sidecar after every rewrite.

Usage
-----
  uv run python scripts/enrich_with_milestones.py --dt 2026-05-15
  uv run python scripts/enrich_with_milestones.py --days 30
  uv run python scripts/enrich_with_milestones.py --all
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
MILESTONES_DIR = HISTORICAL / "forward_milestones"
AUDIT_DIR = STATE / "enrichment_runs"

STAGE_VERSION = "enrich_with_milestones@1.0.0"


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
    out = AUDIT_DIR / f"enrich_with_milestones_{run_id}.json"
    tmp = out.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, out)


# -------------------- build milestone dimension --------------------

def _build_event_to_milestone_dim() -> pl.DataFrame:
    """
    Return a DataFrame mapping event_ticker -> milestone metadata, using the
    most-recent snapshot of each milestone (max last_updated_unix).

    Schema:
      event_ticker (str), milestone_id (str), milestone_type (str),
      milestone_category (str), milestone_title (str)

    Strategy:
      1. Lazy-scan all forward_milestones parquets, project the relevant cols.
      2. For each milestone_id, keep the most-recent row (highest last_updated_unix).
      3. Explode primary_event_tickers so each (milestone, event) is one row.
      4. If an event maps to >1 milestone, keep the most-recently-updated one.
    """
    files = sorted(glob.glob(str(MILESTONES_DIR / "dt=*/*.parquet")))
    files = [f for f in files if not f.endswith(".tmp")]
    if not files:
        return pl.DataFrame()

    lf = pl.scan_parquet(files).select([
        "id", "category", "type", "title",
        "primary_event_tickers", "last_updated_unix",
    ])
    # Keep latest snapshot per milestone_id
    lf = (lf.sort(["id", "last_updated_unix"], descending=[False, True], nulls_last=True)
            .group_by("id", maintain_order=True)
            .head(1))

    # Explode primary_event_tickers
    lf = lf.explode("primary_event_tickers").filter(pl.col("primary_event_tickers").is_not_null())
    lf = lf.rename({"primary_event_tickers": "event_ticker",
                    "id": "milestone_id",
                    "type": "milestone_type",
                    "category": "milestone_category",
                    "title": "milestone_title"})
    # If an event_ticker maps to multiple milestones, keep the most-recent
    lf = (lf.sort(["event_ticker", "last_updated_unix"], descending=[False, True], nulls_last=True)
            .group_by("event_ticker", maintain_order=True)
            .head(1))
    df = lf.select(["event_ticker", "milestone_id", "milestone_type",
                    "milestone_category", "milestone_title"]).collect()
    _log(f"milestone dim: {df.height:,} event -> milestone mappings")
    return df


# -------------------- enrich one partition --------------------

def _enrich_partition(dt: str, milestone_dim: pl.DataFrame, run_id: str) -> dict:
    in_files = sorted(glob.glob(str(ENRICHED_DIR / f"dt={dt}/*.parquet")))
    in_files = [f for f in in_files if not f.endswith(".tmp")]
    if not in_files:
        _log(f"  dt={dt}  no enriched parquets; run enrich_trades.py first")
        return {"dt": dt, "error": "no_enriched_input"}

    # We rewrite each enriched file in place
    n_files = 0
    n_rows_total = 0
    n_with_milestone = 0
    for fp in in_files:
        df = pl.read_parquet(fp)
        # Drop any pre-existing milestone columns so a re-run is idempotent
        drop_cols = [c for c in df.columns if c.startswith("milestone_")]
        if drop_cols:
            df = df.drop(drop_cols)
        # Left join — if event_ticker is null in trade, no match
        if milestone_dim.is_empty():
            df = df.with_columns([
                pl.lit(None, dtype=pl.Utf8).alias("milestone_id"),
                pl.lit(None, dtype=pl.Utf8).alias("milestone_type"),
                pl.lit(None, dtype=pl.Utf8).alias("milestone_category"),
                pl.lit(None, dtype=pl.Utf8).alias("milestone_title"),
            ])
        else:
            df = df.join(milestone_dim, on="event_ticker", how="left")
        n_rows = df.height
        n_with = int((df["milestone_id"].is_not_null()).sum() or 0)
        n_rows_total += n_rows
        n_with_milestone += n_with

        # Atomic rewrite + fresh sidecar
        tmp_path = Path(fp).with_suffix(".parquet.tmp")
        df.write_parquet(str(tmp_path), compression="zstd")
        tmp_path.replace(fp)
        _write_sha256_sidecar(Path(fp))
        n_files += 1

    pct = (100.0 * n_with_milestone / n_rows_total) if n_rows_total else 0.0
    _log(f"  dt={dt}  files={n_files}  rows={n_rows_total:,}  with_milestone={n_with_milestone:,} ({pct:.1f}%)")
    return {
        "dt": dt,
        "files": n_files,
        "rows": n_rows_total,
        "with_milestone": n_with_milestone,
        "pct_with_milestone": round(pct, 3),
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
    ap = argparse.ArgumentParser(description="Stage 2 - link milestone_id to each enriched trade")
    ap.add_argument("--dt",   type=str, default=None)
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--all",  action="store_true")
    args = ap.parse_args()

    run_id = f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"
    started = time.time()
    dates = _list_dates(args)

    _log("=" * 72)
    _log(f"enrich_with_milestones run_id={run_id}")
    _log(f"  version    = {STAGE_VERSION}")
    _log(f"  partitions = {len(dates)}  range=[{dates[0] if dates else '-'}, {dates[-1] if dates else '-'}]")
    _log("=" * 72)

    if not dates:
        _log("no enriched_trades partitions; exiting")
        return 0

    _log("building milestone dimension table…")
    dim = _build_event_to_milestone_dim()
    if dim.is_empty():
        _log("WARN  no milestones available; run capture_milestones.py first. Writing null milestone columns.")

    per_dt: list[dict] = []
    for i, dt in enumerate(dates, 1):
        _log(f"[{i}/{len(dates)}] dt={dt}")
        try:
            per_dt.append(_enrich_partition(dt, dim, run_id))
        except Exception as exc:
            _log(f"  dt={dt}  ERROR  {exc!r}")
            per_dt.append({"dt": dt, "error": repr(exc)})

    total_rows = sum(r.get("rows", 0) for r in per_dt)
    total_with = sum(r.get("with_milestone", 0) for r in per_dt)
    pct = (100.0 * total_with / total_rows) if total_rows else 0.0

    audit = {
        "run_id": run_id,
        "started_at_utc": datetime.fromtimestamp(started, tz=timezone.utc).isoformat(),
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "duration_seconds": round(time.time() - started, 3),
        "version": STAGE_VERSION,
        "milestone_dim_rows": dim.height,
        "total_rows": total_rows,
        "total_with_milestone": total_with,
        "pct_with_milestone": round(pct, 3),
        "args_dt": args.dt,
        "args_days": args.days,
        "args_all": args.all,
        "per_dt": per_dt,
    }
    _write_audit(run_id, audit)

    _log("=" * 72)
    _log(f"DONE  rows={total_rows:,}  with_milestone={total_with:,} ({pct:.1f}%)  duration={time.time()-started:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
