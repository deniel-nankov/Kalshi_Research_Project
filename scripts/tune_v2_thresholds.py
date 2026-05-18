#!/usr/bin/env python3
# ruff: noqa: E402
"""
Phase 6.3 — Sweep EDGE_THRESHOLD for v2 detectors and report the curve.

Re-uses detect_v2_pnl_aware's calibration + scoring; varies only the
edge cutoff. Produces:

  surveillance/reports/tune_v2_<run_id>.md
    Table: edge_threshold → flagged_count, precision@100, ROC AUC,
    mean PnL/trade, total PnL, Sharpe (daily-rebalanced)
    "Best" pick = max Sharpe among rows where all 3 ship gates pass.

  state/detector_runs/tune_v2_<run_id>.json
    Same data + recommended thresholds per detector.

Methodology: train/test split same as detect_v2_pnl_aware. The
calibration itself doesn't depend on EDGE_THRESHOLD; only the flag
decision does. So we fit calibration once, then iterate thresholds
cheaply.

Usage
-----
  uv run python scripts/tune_v2_thresholds.py --days 7
  uv run python scripts/tune_v2_thresholds.py --thresholds 0.01,0.02,0.03,0.05,0.07,0.10
"""

from __future__ import annotations

import argparse
import glob
import json
import math
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
REPORTS_DIR = PROJECT_ROOT / "surveillance" / "reports"
AUDIT_DIR = STATE / "detector_runs"

VERSION = "tune_v2_thresholds@1.0.0"

WHALE_GLOBAL_THRESHOLD = 0.99
WHALE_MARKET_THRESHOLD = 0.95
VOLUME_BURST_Z = 3.0
PRICE_BIN_WIDTH = 0.05

DEFAULT_THRESHOLDS = [0.005, 0.01, 0.02, 0.03, 0.04, 0.05, 0.07, 0.10, 0.15, 0.20]
SHIP_PRECISION_AT_100 = 0.60
SHIP_ROC_AUC = 0.55


def _log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"{ts}  {msg}", flush=True)


def _write_audit(run_id: str, payload: dict) -> None:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    out = AUDIT_DIR / f"tune_v2_{run_id}.json"
    tmp = out.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True, default=float), encoding="utf-8")
    os.replace(tmp, out)


# -------------------- v1 flags + price bin --------------------

def _add_v1_flags(df: pl.DataFrame) -> pl.DataFrame:
    df = df.with_columns([
        ((pl.col("size_pct_global_today") >= WHALE_GLOBAL_THRESHOLD) &
         (pl.col("size_pct_in_market_today") >= WHALE_MARKET_THRESHOLD)).alias("flag_whale"),
        (pl.col("volume_z_5min").fill_null(0.0) >= VOLUME_BURST_Z).alias("flag_volume_burst"),
    ])
    if "in_pre_play_window" in df.columns:
        df = df.with_columns(
            (pl.col("in_pre_play_window").fill_null(False) & (pl.col("notional_usd") >= 100.0))
                .alias("flag_pre_play")
        )
    else:
        df = df.with_columns(pl.lit(False).alias("flag_pre_play"))
    df = df.with_columns(
        ((pl.col("yes_price_dollars") / PRICE_BIN_WIDTH).floor().cast(pl.Int32) * PRICE_BIN_WIDTH)
            .alias("yes_price_bin")
    )
    df = df.with_columns(
        pl.when(pl.col("taker_side") == "yes")
          .then(pl.col("yes_price_dollars"))
          .otherwise(pl.col("no_price_dollars"))
          .alias("taker_break_even_price")
    )
    return df


def _fit_calibration(train: pl.DataFrame, flag_col: str):
    flagged = train.filter(pl.col(flag_col) & pl.col("was_taker_correct").is_not_null())
    cal = (flagged.group_by(["yes_price_bin", "taker_side"])
                  .agg([pl.col("was_taker_correct").mean().alias("pwin_hat"),
                        pl.len().alias("n_train")]))
    gpwin = float(flagged["was_taker_correct"].mean() or 0.0)
    return cal, gpwin


def _score(test: pl.DataFrame, cal: pl.DataFrame, flag_col: str, gpwin: float, min_n: int = 20) -> pl.DataFrame:
    j = test.join(cal, on=["yes_price_bin", "taker_side"], how="left")
    j = j.with_columns(
        pl.when(pl.col("n_train").fill_null(0) >= min_n)
          .then(pl.col("pwin_hat"))
          .otherwise(pl.lit(gpwin))
          .alias("pwin_hat_used")
    )
    edge_col = f"edge_{flag_col.replace('flag_', '')}"
    j = j.with_columns((pl.col("pwin_hat_used") - pl.col("taker_break_even_price")).alias(edge_col))
    return j.drop(["pwin_hat", "n_train", "pwin_hat_used"])


# -------------------- metrics --------------------

def _pnl_for_flag(test: pl.DataFrame, flag_expr: pl.Expr) -> dict:
    res = test.filter(pl.col("was_taker_correct").is_not_null()).filter(flag_expr)
    n = res.height
    if n == 0:
        return {"n": 0, "total_pnl": 0.0, "mean_pnl_per_trade": 0.0,
                "hit_rate": 0.0, "sharpe": None, "n_days": 0}
    r = res.with_columns(
        (pl.col("taker_pnl_per_contract_dollars") * pl.col("count")).alias("_pnl"),
        pl.from_epoch(pl.col("created_time_unix"), time_unit="s").cast(pl.Date).alias("_d"),
    )
    daily = r.group_by("_d").agg(pl.col("_pnl").sum().alias("daily_pnl"))["daily_pnl"].to_list()
    mean_d = sum(daily) / max(len(daily), 1)
    var_d = sum((x - mean_d) ** 2 for x in daily) / max(len(daily), 1)
    std_d = var_d ** 0.5
    sharpe = (math.sqrt(252) * mean_d / std_d) if std_d > 0 else None
    return {
        "n": n,
        "total_pnl": float(r["_pnl"].sum() or 0.0),
        "mean_pnl_per_trade": float(r["_pnl"].mean() or 0.0),
        "hit_rate": float(res["was_taker_correct"].mean() or 0.0),
        "sharpe": sharpe,
        "n_days": len(daily),
    }


def _precision_at_k(test: pl.DataFrame, score_col: str, flag_expr: pl.Expr, K: int) -> float | None:
    res = test.filter(pl.col("was_taker_correct").is_not_null() & flag_expr).select([score_col, "was_taker_correct"])
    if res.height == 0:
        return None
    top = res.sort(score_col, descending=True).head(K)
    if top.height == 0:
        return None
    return float(top["was_taker_correct"].mean() or 0.0)


def _roc_auc_on_flagged(test: pl.DataFrame, score_col: str, flag_expr: pl.Expr) -> float | None:
    res = test.filter(pl.col("was_taker_correct").is_not_null() & flag_expr).select([
        score_col, "was_taker_correct"
    ]).rename({score_col: "score"})
    if res.height == 0:
        return None
    pos = res.filter(pl.col("was_taker_correct"))["score"].to_list()
    neg = res.filter(~pl.col("was_taker_correct"))["score"].to_list()
    if not pos or not neg:
        return None
    if len(pos) + len(neg) > 200_000:
        import random
        rng = random.Random(42)
        pos = rng.sample(pos, 100_000); neg = rng.sample(neg, 100_000)
    combined = sorted([(s, 1) for s in pos] + [(s, 0) for s in neg], key=lambda x: x[0])
    n_pos = len(pos); n_neg = len(neg)
    rank_sum = 0.0; i, rank = 0, 1
    while i < len(combined):
        j = i
        while j + 1 < len(combined) and combined[j + 1][0] == combined[i][0]:
            j += 1
        avg = (rank + (rank + (j - i))) / 2.0
        for k in range(i, j + 1):
            if combined[k][1] == 1: rank_sum += avg
        rank += (j - i + 1); i = j + 1
    u = rank_sum - n_pos * (n_pos + 1) / 2.0
    return float(u / (n_pos * n_neg))


# -------------------- main --------------------

DETECTORS = [
    ("whale",        "flag_whale",        "edge_whale"),
    ("volume_burst", "flag_volume_burst", "edge_volume_burst"),
    ("pre_play",     "flag_pre_play",     "edge_pre_play"),
]


def main() -> int:
    ap = argparse.ArgumentParser(description="Sweep v2 EDGE_THRESHOLD to find max-Sharpe")
    ap.add_argument("--days", type=int, default=14)
    ap.add_argument("--all",  action="store_true")
    ap.add_argument("--train-frac", type=float, default=0.70)
    ap.add_argument("--thresholds", type=str, default=",".join(str(t) for t in DEFAULT_THRESHOLDS),
                    help="Comma-separated edge thresholds to sweep")
    args = ap.parse_args()
    thresholds = [float(t) for t in args.thresholds.split(",")]

    run_id = f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"
    started = time.time()

    parts = sorted({Path(p).name for p in glob.glob(str(ENRICHED_DIR / "dt=*"))})
    parts = [p.replace("dt=", "") for p in parts if p.startswith("dt=")]
    if not args.all:
        last_dt = parts[-1]
        cutoff = (datetime.fromisoformat(last_dt) - timedelta(days=args.days)).date().isoformat()
        parts = [p for p in parts if p >= cutoff]
    if len(parts) < 3:
        _log("need >= 3 enriched partitions; exiting")
        return 1
    n_train = max(1, int(len(parts) * args.train_frac))
    train_dates, test_dates = parts[:n_train], parts[n_train:]

    _log("=" * 72)
    _log(f"tune_v2_thresholds run_id={run_id}")
    _log(f"  TRAIN = {train_dates}")
    _log(f"  TEST  = {test_dates}")
    _log(f"  thresholds = {thresholds}")
    _log("=" * 72)

    def _load(dates: list[str]) -> pl.DataFrame:
        files = []
        for dt in dates:
            files.extend(sorted(glob.glob(str(ENRICHED_DIR / f"dt={dt}/*.parquet"))))
        files = [f for f in files if not f.endswith(".tmp")]
        return pl.concat([pl.read_parquet(f) for f in files], how="diagonal_relaxed")

    train = _add_v1_flags(_load(train_dates))
    test  = _add_v1_flags(_load(test_dates))
    _log(f"  train rows: {train.height:,}  test rows: {test.height:,}")

    all_curves: dict[str, list[dict]] = {}
    best: dict[str, dict | None] = {}

    for det_id, v1_flag, edge_col in DETECTORS:
        _log(f"detector={det_id}")
        cal, gpwin = _fit_calibration(train, v1_flag)
        scored = _score(test, cal, v1_flag, gpwin)

        rows: list[dict] = []
        for thr in thresholds:
            flag_expr = pl.col(v1_flag) & (pl.col(edge_col) > thr)
            pnl  = _pnl_for_flag(scored, flag_expr)
            p100 = _precision_at_k(scored, edge_col, flag_expr, 100)
            p1k  = _precision_at_k(scored, edge_col, flag_expr, 1_000)
            auc  = _roc_auc_on_flagged(scored, edge_col, flag_expr)
            ships = (
                (p100 is not None and p100 >= SHIP_PRECISION_AT_100)
                and (auc is not None and auc >= SHIP_ROC_AUC)
                and pnl["mean_pnl_per_trade"] > 0
            )
            rows.append({
                "threshold": thr,
                "flagged": pnl["n"],
                "precision_at_100": p100,
                "precision_at_1k": p1k,
                "roc_auc": auc,
                "mean_pnl_per_trade": pnl["mean_pnl_per_trade"],
                "total_pnl": pnl["total_pnl"],
                "hit_rate": pnl["hit_rate"],
                "sharpe": pnl["sharpe"],
                "ships": ships,
            })
        all_curves[det_id] = rows
        # Best = max Sharpe among ships=True rows; if none, best=None
        eligible = [r for r in rows if r["ships"] and r["sharpe"] is not None]
        best[det_id] = max(eligible, key=lambda r: r["sharpe"]) if eligible else None

    # -------------------- report --------------------
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    report = REPORTS_DIR / f"tune_v2_{run_id}.md"
    L = []
    L.append("# v2 EDGE_THRESHOLD sweep")
    L.append("")
    L.append(f"_Generated by `{VERSION}` on {datetime.now(timezone.utc).isoformat()}._")
    L.append("")
    L.append(f"- TRAIN window: {train_dates[0]} → {train_dates[-1]}")
    L.append(f"- TEST window:  {test_dates[0]} → {test_dates[-1]}")
    L.append(f"- Ship gates: precision@100 ≥ {SHIP_PRECISION_AT_100:.2f} AND ROC AUC ≥ {SHIP_ROC_AUC:.2f} AND PnL/trade > 0")
    L.append("")
    L.append("## Recommended thresholds (max-Sharpe among shipping rows)")
    L.append("")
    L.append("| Detector | Best threshold | Sharpe | PnL/trade | Total PnL | Flagged |")
    L.append("|:---|---:|---:|---:|---:|---:|")
    for det in DETECTORS:
        b = best[det[0]]
        if b is None:
            L.append(f"| {det[0]} | — | — | — | — | — |  ← no shipping threshold")
        else:
            L.append(f"| {det[0]} | **{b['threshold']:.3f}** | {b['sharpe']:.2f} | ${b['mean_pnl_per_trade']:.4f} | ${b['total_pnl']:>10,.0f} | {b['flagged']:,} |")
    L.append("")
    for det_id, _, _ in DETECTORS:
        L.append(f"## {det_id}")
        L.append("")
        L.append("| Threshold | Flagged | precision@100 | precision@1K | ROC AUC | PnL/trade | Total PnL | Sharpe | Ships? |")
        L.append("|---:|---:|---:|---:|---:|---:|---:|---:|:---:|")
        for r in all_curves[det_id]:
            p100 = f"{r['precision_at_100']:.3f}" if r['precision_at_100'] is not None else "—"
            p1k  = f"{r['precision_at_1k']:.3f}"  if r['precision_at_1k']  is not None else "—"
            auc  = f"{r['roc_auc']:.3f}"          if r['roc_auc']          is not None else "—"
            sh   = f"{r['sharpe']:.2f}"           if r['sharpe']           is not None else "—"
            ship = "✓" if r['ships'] else "✗"
            L.append(f"| {r['threshold']:.3f} | {r['flagged']:>6,} | {p100} | {p1k} | {auc} | ${r['mean_pnl_per_trade']:>8.4f} | ${r['total_pnl']:>10,.0f} | {sh} | {ship} |")
        L.append("")
    report.write_text("\n".join(L), encoding="utf-8")

    audit = {
        "run_id": run_id,
        "version": VERSION,
        "started_at_utc": datetime.fromtimestamp(started, tz=timezone.utc).isoformat(),
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "duration_seconds": round(time.time() - started, 3),
        "train_dates": train_dates,
        "test_dates":  test_dates,
        "thresholds":  thresholds,
        "curves":      all_curves,
        "best":        best,
        "report_path": str(report.relative_to(PROJECT_ROOT)),
    }
    _write_audit(run_id, audit)
    _log("=" * 72)
    for det_id, _, _ in DETECTORS:
        b = best[det_id]
        if b:
            _log(f"  {det_id:15s}  best edge={b['threshold']:.3f}  Sharpe={b['sharpe']:.2f}  PnL/trade=${b['mean_pnl_per_trade']:.4f}")
        else:
            _log(f"  {det_id:15s}  no shipping threshold")
    _log(f"report -> {report.relative_to(PROJECT_ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
