#!/usr/bin/env python3
# ruff: noqa: E402
"""
Phase 2.3 — Play-by-play (PBP) enrichment.

This is the surveillance project's highest-value enrichment: for every trade
that links to a sports milestone (via milestone_id), find the NEXT play
event captured in `forward_live_data` after the trade timestamp, and add
three columns:

  seconds_to_next_play_event   int64    (null if no future pbp captured)
  next_play_event_type         string   (best-effort label or "")
  in_pre_play_window           bool     (true if 0 < secs_to_next <= 300)

Trades that arrive within 5 minutes BEFORE a play event are the classic
informed-trade signature. The Pre-play Alpha detector (M4.2) reads this
column directly.

How the join works
------------------
1. Read forward_live_data parquets (each row is a snapshot of one milestone
   at one fetch time; `live_data_json` holds the full pbp dict).
2. Per milestone_id, collect all `events_count` deltas observed. When
   events_count increases between two consecutive snapshots, an event
   happened "between" those fetch times. We approximate the play event
   time as the later snapshot's fetched_at_unix (worst case 30-sec lag,
   which is fine for a 5-min pre-play window).
3. For each trade in the enriched_trades partition, find the smallest
   play_event_time > trade.created_time_unix for the trade's milestone.
4. Compute seconds_to_next_play_event = play_event_time - created_time_unix.

For efficiency we:
  - Build a sorted per-milestone list of play_event_times.
  - Use a vectorized merge_asof-style search (Polars `join_asof` with
    strategy="forward").

Schema-stable: re-running with new live_data refines the per-trade
nearest-future-event lookup, which is naturally idempotent.

Usage
-----
  uv run python scripts/enrich_with_milestones_pbp.py --dt 2026-05-15
  uv run python scripts/enrich_with_milestones_pbp.py --days 7
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
LIVE_DATA_DIR = HISTORICAL / "forward_live_data"
AUDIT_DIR = STATE / "enrichment_runs"

STAGE_VERSION = "enrich_with_milestones_pbp@1.0.0"
PRE_PLAY_WINDOW_SECONDS = 300  # 5-min lookahead


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
    out = AUDIT_DIR / f"enrich_with_milestones_pbp_{run_id}.json"
    tmp = out.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, out)


# -------------------- derive play-event times --------------------

def _build_play_event_times(dt_range_days_back: int) -> pl.DataFrame:
    """
    Scan forward_live_data and derive a (milestone_id, play_event_unix)
    table. A "play event" is detected when the live_data_json content for
    a single milestone changes between two consecutive snapshots — i.e.
    any state mutation (score change, set won, period end, goal scored,
    inning advance, etc.).

    Why content-hash instead of `events_count`:
        Kalshi's live_data shape varies by milestone_type. Football and
        basketball expose a `pbp` array (where events_count is well-defined),
        but tennis exposes a scoreboard (competitor1_overall_score etc.)
        with no `pbp` array. Hashing the JSON detects mutations universally.

    Per-content-hash dedup: two adjacent snapshots with identical content
    are NOT a play event. The first occurrence of a new hash IS.

    Output schema:
        milestone_id (str), play_event_unix (int64), milestone_type (str),
        next_play_event_type (str)  -- best-effort label
    """
    all_files = sorted(glob.glob(str(LIVE_DATA_DIR / "dt=*/*.parquet")))
    all_files = [f for f in all_files if not f.endswith(".tmp")]
    if not all_files:
        _log("WARN  no forward_live_data parquets; skipping pbp derivation")
        return pl.DataFrame()
    cutoff_dt = (datetime.now(timezone.utc) - timedelta(days=dt_range_days_back)).date().isoformat()
    files = [f for f in all_files if Path(f).parent.name.replace("dt=", "") >= cutoff_dt]
    if not files:
        files = all_files[-50:]
    _log(f"scanning {len(files):,} live_data parquets for content deltas (cutoff dt>={cutoff_dt})")

    lf = pl.scan_parquet(files).select([
        "milestone_id", "fetched_at_unix", "milestone_type",
        "latest_event_summary", "live_data_json",
    ]).filter(
        pl.col("milestone_id").is_not_null()
        & pl.col("live_data_json").is_not_null()
    )

    # Hash the JSON content per row so we can detect mutations.
    # Polars' hash function uses internal hashing; sufficient for change detection.
    lf = lf.with_columns(pl.col("live_data_json").hash().alias("_content_hash"))
    # Sort and detect transitions per milestone
    lf = lf.sort(["milestone_id", "fetched_at_unix"])
    lf = lf.with_columns(
        pl.col("_content_hash").shift(1).over("milestone_id").alias("_prev_hash")
    )
    # A "play event" snapshot is one where the hash differs from the previous
    # (excluding the first snapshot per milestone, where _prev_hash is null —
    # we don't know if state changed at that moment).
    lf = lf.filter(pl.col("_prev_hash").is_not_null() & (pl.col("_content_hash") != pl.col("_prev_hash")))

    df = (lf.select([
            pl.col("milestone_id"),
            pl.col("fetched_at_unix").alias("play_event_unix"),
            pl.col("milestone_type"),
            pl.col("latest_event_summary").alias("next_play_event_type"),
          ])
          .collect())
    _log(f"derived {df.height:,} play-event timestamps across {df['milestone_id'].n_unique():,} milestones")
    return df


# -------------------- per-partition enrich --------------------

def _enrich_partition(dt: str, pbp_df: pl.DataFrame, run_id: str) -> dict:
    in_files = sorted(glob.glob(str(ENRICHED_DIR / f"dt={dt}/*.parquet")))
    in_files = [f for f in in_files if not f.endswith(".tmp")]
    if not in_files:
        _log(f"  dt={dt}  no enriched parquets; run enrich_trades + enrich_with_milestones first")
        return {"dt": dt, "error": "no_enriched_input"}

    n_files = 0
    n_rows_total = 0
    n_with_pbp = 0
    n_in_window = 0

    for fp in in_files:
        df = pl.read_parquet(fp)
        # Idempotency: drop pre-existing pbp columns
        for c in ("seconds_to_next_play_event", "next_play_event_type", "in_pre_play_window"):
            if c in df.columns:
                df = df.drop(c)
        # Confirm milestone_id is present
        if "milestone_id" not in df.columns:
            _log(f"  dt={dt}  WARN milestone_id missing in {Path(fp).name}; run enrich_with_milestones.py first")
            return {"dt": dt, "error": "missing_milestone_link"}

        if pbp_df.is_empty():
            df = df.with_columns([
                pl.lit(None, dtype=pl.Int64).alias("seconds_to_next_play_event"),
                pl.lit(None, dtype=pl.Utf8).alias("next_play_event_type"),
                pl.lit(False, dtype=pl.Boolean).alias("in_pre_play_window"),
            ])
        else:
            # Polars join_asof: for each trade, find the NEAREST play_event_unix
            # AFTER (>) trade.created_time_unix within the same milestone_id group.
            # Strategy "forward" means next future value.
            left = df.sort(["milestone_id", "created_time_unix"])
            right = pbp_df.sort(["milestone_id", "play_event_unix"]).select([
                "milestone_id", "play_event_unix", "next_play_event_type",
            ])
            joined = left.join_asof(
                right,
                left_on="created_time_unix",
                right_on="play_event_unix",
                by="milestone_id",
                strategy="forward",
            )
            joined = joined.with_columns([
                (pl.col("play_event_unix") - pl.col("created_time_unix"))
                    .cast(pl.Int64)
                    .alias("seconds_to_next_play_event"),
            ])
            joined = joined.with_columns([
                ((pl.col("seconds_to_next_play_event") > 0)
                 & (pl.col("seconds_to_next_play_event") <= PRE_PLAY_WINDOW_SECONDS))
                    .alias("in_pre_play_window"),
            ]).drop(["play_event_unix"])
            df = joined

        n_rows = df.height
        n_pbp = int((df["seconds_to_next_play_event"].is_not_null()).sum() or 0)
        n_win = int((df["in_pre_play_window"]).sum() or 0)
        n_rows_total += n_rows
        n_with_pbp += n_pbp
        n_in_window += n_win

        tmp_path = Path(fp).with_suffix(".parquet.tmp")
        df.write_parquet(str(tmp_path), compression="zstd")
        tmp_path.replace(fp)
        _write_sha256_sidecar(Path(fp))
        n_files += 1

    pct_pbp = (100.0 * n_with_pbp / n_rows_total) if n_rows_total else 0.0
    pct_win = (100.0 * n_in_window / n_rows_total) if n_rows_total else 0.0
    _log(f"  dt={dt}  files={n_files}  rows={n_rows_total:,}  with_pbp={n_with_pbp:,} ({pct_pbp:.1f}%)  in_pre_play_window={n_in_window:,} ({pct_win:.3f}%)")
    return {
        "dt": dt,
        "files": n_files,
        "rows": n_rows_total,
        "with_pbp": n_with_pbp,
        "in_pre_play_window": n_in_window,
        "pct_with_pbp": round(pct_pbp, 3),
        "pct_in_pre_play_window": round(pct_win, 3),
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
    ap = argparse.ArgumentParser(description="Stage 3 - join play-by-play event times")
    ap.add_argument("--dt",   type=str, default=None)
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--all",  action="store_true")
    ap.add_argument("--live-data-days", type=int, default=14,
                    help="How many days of forward_live_data to scan for pbp deltas (default 14)")
    args = ap.parse_args()

    run_id = f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"
    started = time.time()
    dates = _list_dates(args)

    _log("=" * 72)
    _log(f"enrich_with_milestones_pbp run_id={run_id}")
    _log(f"  version    = {STAGE_VERSION}")
    _log(f"  partitions = {len(dates)}  range=[{dates[0] if dates else '-'}, {dates[-1] if dates else '-'}]")
    _log(f"  pre_play_window = {PRE_PLAY_WINDOW_SECONDS} sec")
    _log("=" * 72)

    if not dates:
        _log("no enriched_trades partitions; exiting")
        return 0

    pbp = _build_play_event_times(args.live_data_days)

    per_dt: list[dict] = []
    for i, dt in enumerate(dates, 1):
        _log(f"[{i}/{len(dates)}] dt={dt}")
        try:
            per_dt.append(_enrich_partition(dt, pbp, run_id))
        except Exception as exc:
            _log(f"  dt={dt}  ERROR  {exc!r}")
            per_dt.append({"dt": dt, "error": repr(exc)})

    total_rows = sum(r.get("rows", 0) for r in per_dt)
    total_pbp = sum(r.get("with_pbp", 0) for r in per_dt)
    total_win = sum(r.get("in_pre_play_window", 0) for r in per_dt)

    audit = {
        "run_id": run_id,
        "started_at_utc": datetime.fromtimestamp(started, tz=timezone.utc).isoformat(),
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "duration_seconds": round(time.time() - started, 3),
        "version": STAGE_VERSION,
        "pre_play_window_seconds": PRE_PLAY_WINDOW_SECONDS,
        "pbp_event_count": pbp.height,
        "pbp_milestone_count": int(pbp["milestone_id"].n_unique()) if not pbp.is_empty() else 0,
        "total_rows": total_rows,
        "total_with_pbp": total_pbp,
        "total_in_pre_play_window": total_win,
        "per_dt": per_dt,
    }
    _write_audit(run_id, audit)

    _log("=" * 72)
    _log(f"DONE  rows={total_rows:,}  pbp_linked={total_pbp:,}  pre_play_window={total_win:,}  duration={time.time()-started:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
