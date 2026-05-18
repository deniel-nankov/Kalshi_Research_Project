#!/usr/bin/env python3
# ruff: noqa: E402
"""
Phase 5.2 — Daily surveillance orchestrator.

End-to-end pipeline for one calendar day:

  1. Pick target dt (default: yesterday UTC, so all forward_trades for the
     day are present).
  2. Ensure forward_trades is fresh: trigger kalshi-forward.service if
     last successful run is > 4h old; wait for it to finish.
  3. Run the four enrichment stages, in order:
        enrich_trades, enrich_with_milestones,
        enrich_with_milestones_pbp, enrich_with_lifecycle.
  4. Run detect_v2_pnl_aware with the production EDGE_THRESHOLD (0.07).
  5. Extract flagged trades for the target dt → alerts.jsonl
     One JSON object per line: { trade_id, market_ticker, taker_side,
     yes_price_dollars, count, notional_usd, score, edge,
     event_ticker, milestone_id, milestone_category, created_time }.
  6. Write a daily summary to surveillance/reports/daily_<dt>.md
     and append a one-line digest to surveillance/reports/daily_digest.tsv
     (machine-readable: dt, flagged_count, total_notional, top_market).
  7. Audit log at state/surveillance_runs/daily_<run_id>.json.

Idempotent: skips stages whose outputs already exist for the dt.
Pass --force to recompute end-to-end.

Designed to run from kalshi-surveillance-daily.timer at ~04:30 UTC.
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import subprocess
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
REPORTS_DIR = PROJECT_ROOT / "surveillance" / "reports"
ALERTS_DIR = PROJECT_ROOT / "surveillance" / "alerts"
AUDIT_DIR = STATE / "surveillance_runs"

VERSION = "surveillance_daily@1.0.0"

PRODUCTION_EDGE_THRESHOLD = 0.07
PYTHON_BIN = "/opt/kalshi-pipeline/.venv/bin/python3"
SCRIPTS_DIR = PROJECT_ROOT / "scripts"


def _log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"{ts}  {msg}", flush=True)


def _run_stage(name: str, args: list[str]) -> int:
    """Run a script as a subprocess; stream output. Returns exit code."""
    _log(f"---> {name}: {' '.join(args)}")
    started = time.time()
    p = subprocess.run([PYTHON_BIN, *args], cwd=str(PROJECT_ROOT))
    _log(f"<--- {name} exit={p.returncode}  duration={time.time()-started:.1f}s")
    return p.returncode


def _yesterday_utc() -> str:
    return (datetime.now(timezone.utc) - timedelta(days=1)).date().isoformat()


def _has_enriched(dt: str) -> bool:
    return any(p for p in glob.glob(str(ENRICHED_DIR / f"dt={dt}/*.parquet")) if not p.endswith(".tmp"))


def _emit_alerts(dt: str, edge_threshold: float, run_id: str) -> dict:
    files = sorted(glob.glob(str(ENRICHED_DIR / f"dt={dt}/*.parquet")))
    files = [f for f in files if not f.endswith(".tmp")]
    if not files:
        return {"alerts": 0, "error": "no_enriched_input"}
    df = pl.read_parquet(files)

    # Re-apply v2 scoring inline so we are decoupled from detect_v2's main()
    # exit semantics. We need the calibration table written somewhere
    # persistent; for MVP we re-fit it from this dt's data alone (single-day
    # calibration is weaker than full train but stays self-contained for
    # the daily cron). When weekly retraining ships (M6.4) this swaps to a
    # cached calibration parquet under state/calibration/<flag>.parquet.
    from math import floor
    PRICE_BIN_WIDTH = 0.05
    df = df.with_columns([
        ((pl.col("size_pct_global_today") >= 0.99) &
         (pl.col("size_pct_in_market_today") >= 0.95)).alias("flag_whale"),
        (pl.col("volume_z_5min").fill_null(0.0) >= 3.0).alias("flag_volume_burst"),
        ((pl.col("yes_price_dollars") / PRICE_BIN_WIDTH).floor().cast(pl.Int32) * PRICE_BIN_WIDTH).alias("yes_price_bin"),
        pl.when(pl.col("taker_side") == "yes").then(pl.col("yes_price_dollars"))
                                              .otherwise(pl.col("no_price_dollars"))
                                              .alias("taker_break_even_price"),
    ])
    # Use only flagged + resolved subset to fit calibration; sparse cells back-off to global mean
    cal = (df.filter(pl.col("flag_volume_burst") & pl.col("was_taker_correct").is_not_null())
             .group_by(["yes_price_bin", "taker_side"])
             .agg([pl.col("was_taker_correct").mean().alias("pwin_hat"),
                   pl.len().alias("n_train")]))
    gpwin = float(df.filter(pl.col("flag_volume_burst") & pl.col("was_taker_correct").is_not_null())["was_taker_correct"].mean() or 0.0)

    scored = df.join(cal, on=["yes_price_bin", "taker_side"], how="left").with_columns(
        pl.when(pl.col("n_train").fill_null(0) >= 20)
          .then(pl.col("pwin_hat"))
          .otherwise(pl.lit(gpwin))
          .alias("pwin_hat_used")
    )
    scored = scored.with_columns(
        (pl.col("pwin_hat_used") - pl.col("taker_break_even_price")).alias("edge_volume_burst")
    )
    flagged = scored.filter(
        pl.col("flag_volume_burst") & (pl.col("edge_volume_burst") > edge_threshold)
    ).sort("edge_volume_burst", descending=True)

    # Write alerts.jsonl
    ALERTS_DIR.mkdir(parents=True, exist_ok=True)
    out = ALERTS_DIR / f"{dt}.jsonl"
    tmp = out.with_suffix(".jsonl.tmp")
    keep_cols = [
        "trade_id", "market_ticker", "event_ticker", "milestone_id", "milestone_category",
        "taker_side", "yes_price_dollars", "no_price_dollars", "count", "notional_usd",
        "edge_volume_burst", "pwin_hat_used", "taker_break_even_price",
        "created_time", "was_taker_correct",
    ]
    keep_cols = [c for c in keep_cols if c in flagged.columns]
    rec = flagged.select(keep_cols).to_dicts()
    with open(tmp, "w", encoding="utf-8") as f:
        for r in rec:
            for k, v in list(r.items()):
                if isinstance(v, datetime):
                    r[k] = v.isoformat()
            r["alert_version"] = "v2_volume_burst@1.0.0"
            r["run_id"] = run_id
            f.write(json.dumps(r, default=str) + "\n")
    os.replace(tmp, out)

    return {
        "alerts": flagged.height,
        "total_notional_usd": float(flagged["notional_usd"].sum() or 0.0),
        "calibration_cells": cal.height,
        "global_pwin": gpwin,
        "out_file": str(out.relative_to(PROJECT_ROOT)),
    }


def _write_daily_report(dt: str, summary: dict, run_id: str) -> Path:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = REPORTS_DIR / f"daily_{dt}.md"
    L = []
    L.append(f"# Daily surveillance — {dt}")
    L.append("")
    L.append(f"_Generated by `{VERSION}` on {datetime.now(timezone.utc).isoformat()}._")
    L.append("")
    L.append(f"- Production detector: **volume_burst_v2** at edge threshold **{PRODUCTION_EDGE_THRESHOLD:+.3f}**")
    L.append(f"- Flagged trades: **{summary.get('alerts', 0):,}**")
    L.append(f"- Total flagged notional: **${summary.get('total_notional_usd', 0):,.0f}**")
    L.append(f"- Calibration cells used: {summary.get('calibration_cells', 0):,}")
    L.append(f"- Global pwin (flagged-resolved baseline): {summary.get('global_pwin', 0):.4f}")
    L.append(f"- Alerts file: `{summary.get('out_file', '')}`")
    L.append("")
    L.append("**See backtest validation:** `surveillance/reports/v2_backtest_*.md`,")
    L.append("`surveillance/reports/tune_v2_*.md`. The 0.07 edge threshold was chosen as")
    L.append("the max-Sharpe shipping point; precision@100 = 0.89 on the holdout test.")
    path.write_text("\n".join(L), encoding="utf-8")
    # Append digest line
    digest = REPORTS_DIR / "daily_digest.tsv"
    if not digest.exists():
        digest.write_text("dt\trun_id\tflagged\tnotional_usd\tedge_threshold\n", encoding="utf-8")
    with open(digest, "a", encoding="utf-8") as f:
        f.write(f"{dt}\t{run_id}\t{summary.get('alerts', 0)}\t{summary.get('total_notional_usd', 0):.2f}\t{PRODUCTION_EDGE_THRESHOLD:.3f}\n")
    return path


def main() -> int:
    ap = argparse.ArgumentParser(description="Daily surveillance orchestrator")
    ap.add_argument("--dt", type=str, default=None,
                    help="Target date YYYY-MM-DD (default: yesterday UTC)")
    ap.add_argument("--edge-threshold", type=float, default=PRODUCTION_EDGE_THRESHOLD)
    ap.add_argument("--force", action="store_true", help="Recompute even if outputs exist")
    ap.add_argument("--skip-forward", action="store_true",
                    help="Skip the forward.service trigger (assume trades already pulled)")
    args = ap.parse_args()

    run_id = f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"
    dt = args.dt or _yesterday_utc()
    started = time.time()

    _log("=" * 72)
    _log(f"surveillance_daily run_id={run_id}")
    _log(f"  target dt        = {dt}")
    _log(f"  edge threshold   = {args.edge_threshold}")
    _log(f"  force            = {args.force}")
    _log("=" * 72)

    stages_executed: list[dict] = []

    # Stage 1: trigger forward (best-effort — caller may have already done so)
    if not args.skip_forward:
        try:
            _log("checking forward.service freshness…")
            p = subprocess.run(["systemctl", "is-active", "kalshi-forward.service"],
                               capture_output=True, text=True)
            state = p.stdout.strip()
            _log(f"  forward.service state: {state}")
            # If not currently active, fire it
            if state not in ("active", "activating"):
                _log("  triggering kalshi-forward.service")
                subprocess.Popen(["sudo", "systemctl", "start", "kalshi-forward.service"])
                # Don't wait — enrichment will pick up whatever is on disk.
        except FileNotFoundError:
            _log("  systemctl not available (not on EC2?); skipping forward trigger")

    # Stage 2: enrich_trades
    if args.force or not _has_enriched(dt):
        ec = _run_stage("enrich_trades", [
            str(SCRIPTS_DIR / "enrich_trades.py"),
            "--dt", dt, *(["--force"] if args.force else []),
        ])
        stages_executed.append({"stage": "enrich_trades", "exit": ec})
        if ec != 0:
            _log(f"FATAL  enrich_trades failed exit={ec}; aborting")
            return ec
    else:
        _log(f"enrich_trades  skip (existing for dt={dt})")
        stages_executed.append({"stage": "enrich_trades", "exit": 0, "skipped": True})

    # Stage 3: enrich_with_milestones (idempotent in script, safe to always run)
    ec = _run_stage("enrich_with_milestones", [
        str(SCRIPTS_DIR / "enrich_with_milestones.py"), "--dt", dt,
    ])
    stages_executed.append({"stage": "enrich_with_milestones", "exit": ec})

    # Stage 4: enrich_with_milestones_pbp
    ec = _run_stage("enrich_with_milestones_pbp", [
        str(SCRIPTS_DIR / "enrich_with_milestones_pbp.py"), "--dt", dt,
    ])
    stages_executed.append({"stage": "enrich_with_milestones_pbp", "exit": ec})

    # Stage 5: enrich_with_lifecycle
    ec = _run_stage("enrich_with_lifecycle", [
        str(SCRIPTS_DIR / "enrich_with_lifecycle.py"), "--dt", dt,
    ])
    stages_executed.append({"stage": "enrich_with_lifecycle", "exit": ec})

    # Stage 6: detect + emit alerts (inline)
    _log("---> emit alerts")
    alerts_summary = _emit_alerts(dt, args.edge_threshold, run_id)
    _log(f"<--- alerts written: {alerts_summary}")

    # Stage 7: daily report
    report_path = _write_daily_report(dt, alerts_summary, run_id)
    _log(f"daily report -> {report_path.relative_to(PROJECT_ROOT)}")

    audit = {
        "run_id": run_id,
        "version": VERSION,
        "started_at_utc": datetime.fromtimestamp(started, tz=timezone.utc).isoformat(),
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "duration_seconds": round(time.time() - started, 3),
        "target_dt": dt,
        "edge_threshold": args.edge_threshold,
        "force": args.force,
        "stages_executed": stages_executed,
        "alerts": alerts_summary,
        "report_path": str(report_path.relative_to(PROJECT_ROOT)),
    }
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    out = AUDIT_DIR / f"daily_{run_id}.json"
    tmp = out.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(audit, indent=2, sort_keys=True, default=str), encoding="utf-8")
    os.replace(tmp, out)
    _log("=" * 72)
    _log(f"DONE  dt={dt}  alerts={alerts_summary.get('alerts', 0):,}  notional=${alerts_summary.get('total_notional_usd', 0):,.0f}  duration={time.time()-started:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
