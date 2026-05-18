#!/usr/bin/env python3
# ruff: noqa: E402
"""
Phase 4.2 — Pre-Play Alpha Detector.

The textbook insider-trade signature: a trade placed in the 5-minute window
BEFORE a play-changing event (goal scored, set won, period end, etc.), where
the taker's side later turned out to be the winning side at a rate
significantly above baseline.

Inputs
------
  enriched_trades parquets with these columns already populated:
    - in_pre_play_window         bool   (created by enrich_with_milestones_pbp.py)
    - seconds_to_next_play_event int64
    - was_taker_correct          bool   (resolved markets only)
    - taker_side, milestone_id, milestone_type, milestone_category
    - notional_usd, ticker, event_ticker

How the detector works
----------------------
Per trade, compute:

  score_pre_play = clip( log(1 + 1/seconds_to_next_play_event) / log(1 + 1/30), 0, 1 )

Rationale:
  - A trade 30 seconds before an event scores 1.0 (highest urgency).
  - A trade 5 minutes (300s) before scores ~0.34.
  - Anything outside the pre-play window scores 0.

Flag: in_pre_play_window AND notional_usd >= threshold (default $100).

We ALSO compute a per-(milestone_id) aggregate: count of pre-play trades
on each side. If one side has a heavily skewed pre-play volume just before
the event, that's the smart-money tell.

Outputs
-------
  derived/pre_play_flags/dt=YYYY-MM-DD/<run_id>.parquet
      One row per flagged trade with detector score + winning_side outcome.
  surveillance/reports/pre_play_<dt>.md
      Daily Markdown summary: top markets by pre-play alpha, win-rate
      vs. baseline, per-category breakdown.
  state/detector_runs/detect_pre_play_alpha_<run_id>.json

Usage
-----
  uv run python scripts/detect_pre_play_alpha.py --dt 2026-05-18
  uv run python scripts/detect_pre_play_alpha.py --days 7
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
DERIVED = DATA_ROOT / "derived"
STATE = DATA_ROOT / "state"
ENRICHED_DIR = DERIVED / "enriched_trades"
OUT_DIR = DERIVED / "pre_play_flags"
REPORTS_DIR = PROJECT_ROOT / "surveillance" / "reports"
AUDIT_DIR = STATE / "detector_runs"

DETECTOR_VERSION = "detect_pre_play_alpha@1.0.0"
DEFAULT_MIN_NOTIONAL = 100.0
PRE_PLAY_WINDOW_SECONDS = 300


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
    out = AUDIT_DIR / f"detect_pre_play_alpha_{run_id}.json"
    tmp = out.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, out)


# -------------------- detector --------------------

def _apply_detector(df: pl.DataFrame, min_notional: float) -> pl.DataFrame:
    """
    Add score_pre_play and flag_pre_play columns.

    Formula:
      base = log(1 + 1/seconds_to_next_play_event)
      norm = log(1 + 1/30)
      score = clip(base / norm, 0, 1) if in_pre_play_window else 0

    Flag: in_pre_play_window AND notional_usd >= min_notional
    """
    import math
    norm = math.log(1.0 + 1.0 / 30.0)
    df = df.with_columns(
        pl.when(pl.col("in_pre_play_window") & (pl.col("seconds_to_next_play_event") > 0))
          .then(
              (1.0 + 1.0 / pl.col("seconds_to_next_play_event").cast(pl.Float64)).log() / norm
          )
          .otherwise(0.0)
          .clip(0.0, 1.0)
          .alias("score_pre_play")
    )
    df = df.with_columns(
        (pl.col("in_pre_play_window") & (pl.col("notional_usd") >= min_notional))
            .alias("flag_pre_play")
    )
    return df


# -------------------- per-dt --------------------

def _process_dt(dt: str, run_id: str, min_notional: float) -> dict:
    in_files = sorted(glob.glob(str(ENRICHED_DIR / f"dt={dt}/*.parquet")))
    in_files = [f for f in in_files if not f.endswith(".tmp")]
    if not in_files:
        return {"dt": dt, "error": "no_enriched_input"}

    df = pl.read_parquet(in_files)
    if "in_pre_play_window" not in df.columns:
        return {"dt": dt, "error": "missing_pbp_columns"}

    df = _apply_detector(df, min_notional)
    flagged = df.filter(pl.col("flag_pre_play"))

    out_dir = OUT_DIR / f"dt={dt}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{run_id}_{dt.replace('-', '')}.parquet"
    tmp = out_file.with_suffix(".parquet.tmp")
    flagged.write_parquet(str(tmp), compression="zstd")
    tmp.replace(out_file)
    _write_sha256_sidecar(out_file)

    res = {
        "dt": dt,
        "trades_total": df.height,
        "trades_flagged": flagged.height,
        "flagged_notional_usd": float(flagged["notional_usd"].sum() or 0.0),
        "out_file": str(out_file.relative_to(PROJECT_ROOT)),
    }
    res_with = flagged.filter(pl.col("was_taker_correct").is_not_null())
    if res_with.height > 0:
        res["pre_play_win_rate"] = float(res_with["was_taker_correct"].mean() or 0.0)
        res["pre_play_resolved"] = res_with.height
    base = df.filter(pl.col("was_taker_correct").is_not_null())
    if base.height > 0:
        res["baseline_win_rate"] = float(base["was_taker_correct"].mean() or 0.0)

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    report_path = REPORTS_DIR / f"pre_play_{dt}.md"
    _write_report(report_path, dt, df, flagged, res)
    res["report_path"] = str(report_path.relative_to(PROJECT_ROOT))

    _log(f"  dt={dt}  trades={df.height:,}  flagged={flagged.height:,}  notional=${res['flagged_notional_usd']:,.0f}  -> {out_file.name}")
    return res


def _write_report(path: Path, dt: str, df: pl.DataFrame, flagged: pl.DataFrame, res: dict) -> None:
    lines = []
    lines.append(f"# Pre-play alpha — {dt}")
    lines.append("")
    lines.append(f"_Generated by `{DETECTOR_VERSION}` on {datetime.now(timezone.utc).isoformat()}._")
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append(f"- Total trades: **{df.height:,}**")
    lines.append(f"- Trades with pbp linkage: **{(df['seconds_to_next_play_event'].is_not_null()).sum():,}**")
    lines.append(f"- Pre-play flagged trades: **{flagged.height:,}**  (in window <= {PRE_PLAY_WINDOW_SECONDS}s)")
    lines.append(f"- Pre-play flagged notional: **${res['flagged_notional_usd']:,.0f}**")
    if "baseline_win_rate" in res:
        lines.append(f"- Baseline taker win rate: {100*res['baseline_win_rate']:.2f}%")
    if "pre_play_win_rate" in res:
        edge = res["pre_play_win_rate"] - res.get("baseline_win_rate", 0.0)
        lines.append(f"- **Pre-play win rate: {100*res['pre_play_win_rate']:.2f}%**  ({edge*100:+.2f} pp vs baseline)")
    lines.append("")

    if flagged.height == 0:
        lines.append("_No pre-play flagged trades on this date. Either no live-data capture overlap,_")
        lines.append("_or the milestone link covered only post-event trades._")
        path.write_text("\n".join(lines), encoding="utf-8")
        return

    lines.append("## Top 20 pre-play trades by notional")
    lines.append("")
    lines.append("| Notional | Seconds to event | Score | Market | Taker | Outcome |")
    lines.append("|---:|---:|---:|:---|:---:|:---:|")
    top = flagged.sort("notional_usd", descending=True).head(20)
    for r in top.iter_rows(named=True):
        won = r.get("was_taker_correct")
        won_str = "✓" if won is True else ("✗" if won is False else "—")
        sec = r.get("seconds_to_next_play_event")
        sec_str = f"{sec}s" if sec is not None else "—"
        lines.append(f"| ${r['notional_usd']:>10,.0f} | {sec_str} | {r['score_pre_play']:.2f} | `{r['market_ticker'][:50]}` | {r['taker_side']} | {won_str} |")
    lines.append("")

    # Per-category breakdown
    if "milestone_category" in flagged.columns:
        cat = (flagged.filter(pl.col("milestone_category").is_not_null())
                       .group_by("milestone_category")
                       .agg([
                           pl.len().alias("trades"),
                           pl.col("notional_usd").sum().alias("notional"),
                           pl.col("was_taker_correct").mean().alias("win_rate"),
                       ])
                       .sort("notional", descending=True))
        lines.append("## Pre-play activity by category")
        lines.append("")
        lines.append("| Category | Trades | Notional | Win rate |")
        lines.append("|:---|---:|---:|---:|")
        for r in cat.iter_rows(named=True):
            wr = r.get("win_rate")
            wr_str = f"{100*wr:.1f}%" if wr is not None else "—"
            lines.append(f"| {r['milestone_category']} | {r['trades']:>4,} | ${r['notional']:>9,.0f} | {wr_str} |")
        lines.append("")

    # Most-flagged markets
    mkt = (flagged.group_by("market_ticker")
                   .agg([
                       pl.len().alias("trades"),
                       pl.col("notional_usd").sum().alias("notional"),
                       pl.col("was_taker_correct").mean().alias("win_rate"),
                   ])
                   .sort("notional", descending=True)
                   .head(15))
    lines.append("## Top 15 markets by pre-play notional")
    lines.append("")
    lines.append("| Notional | Trades | Win rate | Market |")
    lines.append("|---:|---:|---:|:---|")
    for r in mkt.iter_rows(named=True):
        wr = r.get("win_rate")
        wr_str = f"{100*wr:.1f}%" if wr is not None else "—"
        lines.append(f"| ${r['notional']:>9,.0f} | {r['trades']:>4} | {wr_str} | `{r['market_ticker'][:60]}` |")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("**Methodology.** A trade is pre-play flagged when its `created_time` is within")
    lines.append(f"`{PRE_PLAY_WINDOW_SECONDS}` seconds of the next captured state change in the linked")
    lines.append("milestone's `forward_live_data` snapshot. State change = content-hash change in the")
    lines.append("milestone's live_data JSON between two captures. Trades on resolved markets carry an")
    lines.append("outcome label (`was_taker_correct`); higher-than-baseline win rate is the smart-money signal.")
    path.write_text("\n".join(lines), encoding="utf-8")


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
    ap = argparse.ArgumentParser(description="Detect pre-play alpha trades on enriched_trades")
    ap.add_argument("--dt",   type=str, default=None)
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--all",  action="store_true")
    ap.add_argument("--min-notional", type=float, default=DEFAULT_MIN_NOTIONAL)
    args = ap.parse_args()

    run_id = f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"
    started = time.time()
    dates = _list_dates(args)

    _log("=" * 72)
    _log(f"detect_pre_play_alpha run_id={run_id}")
    _log(f"  version       = {DETECTOR_VERSION}")
    _log(f"  partitions    = {len(dates)}")
    _log(f"  min_notional  = ${args.min_notional:,.0f}")
    _log(f"  pre_play_window = {PRE_PLAY_WINDOW_SECONDS} sec")
    _log("=" * 72)

    if not dates:
        return 0

    per_dt: list[dict] = []
    for i, dt in enumerate(dates, 1):
        _log(f"[{i}/{len(dates)}] dt={dt}")
        try:
            per_dt.append(_process_dt(dt, run_id, args.min_notional))
        except Exception as exc:
            _log(f"  dt={dt}  ERROR  {exc!r}")
            per_dt.append({"dt": dt, "error": repr(exc)})

    audit = {
        "run_id": run_id,
        "started_at_utc": datetime.fromtimestamp(started, tz=timezone.utc).isoformat(),
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "duration_seconds": round(time.time() - started, 3),
        "version": DETECTOR_VERSION,
        "min_notional": args.min_notional,
        "pre_play_window_seconds": PRE_PLAY_WINDOW_SECONDS,
        "per_dt": per_dt,
    }
    _write_audit(run_id, audit)
    _log("=" * 72)
    _log(f"DONE  duration={time.time()-started:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
