#!/usr/bin/env python3
# ruff: noqa: E402
"""
Phase 6.2 — PnL-aware v2 detection.

What v1 got wrong
-----------------
v1 detectors scored trades by "this trade is a whale" (size-based) or
"this trade is in a volume burst" (z-based). The single-day eyeball
finding showed +2.32 pp hit-rate edge for whales. The 6-day backtest then
showed mean PnL/trade = -$37.53, ROC AUC = 0.465 (worse than random),
and an INVERTED calibration curve — high-score trades won LESS often.

Root cause: prediction-market payoffs are asymmetric. A yes-side taker
at price p pays p and wins (1-p); break-even win rate = p. Hit rate
alone ignores this. A whale buying favorites ($0.85+) at 51% hit rate
still loses money on average (51% × 0.15 - 49% × 0.85 = ~-$0.34 EV).

v2 design
---------
For each candidate flag (v1's whale / volume_burst / pre_play), instead
of ranking by size or burst-z directly:

  1. Fit a calibration table on TRAINING data only:
       pwin_hat(detector, yes_price_bin, taker_side) =
         P(was_taker_correct = True | this trade is flagged by detector
                                       AND yes_price_dollars in this bin
                                       AND taker_side == this side)

  2. For each trade in TEST data, look up pwin_hat.
  3. Compute taker_break_even_price:
       if taker_side == "yes": yes_price_dollars
       else:                   no_price_dollars
  4. Compute edge_per_contract = pwin_hat - taker_break_even_price.
  5. Flag if v1_flag AND edge_per_contract > EDGE_THRESHOLD.
  6. Score = edge_per_contract.

The flag is now an EXPECTED-VALUE-POSITIVE filter on top of the existing
v1 candidate gate. The score is the directly-tradable edge.

Train/test split
----------------
Default: first 70% of dt partitions in the window are train; last 30% are
test. Calibration is fit ONLY on train (no leakage); all metrics report
TEST-side performance. The v1 backtest harness used the full window
without split, which is methodologically weaker — v2 is honest.

Inputs/outputs
--------------
Reads:  data/kalshi/derived/enriched_trades/dt=*/...parquet
Writes: data/kalshi/derived/v2_flags/dt=YYYY-MM-DD/<run_id>.parquet
        surveillance/reports/v2_backtest_<run_id>.md
        state/detector_runs/detect_v2_pnl_aware_<run_id>.json
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
OUT_DIR = DERIVED / "v2_flags"
REPORTS_DIR = PROJECT_ROOT / "surveillance" / "reports"
AUDIT_DIR = STATE / "detector_runs"

VERSION = "detect_v2_pnl_aware@1.0.0"

# v1 candidate-gate thresholds (mirror live detectors)
WHALE_GLOBAL_THRESHOLD = 0.99
WHALE_MARKET_THRESHOLD = 0.95
VOLUME_BURST_Z = 3.0

# v2 edge thresholds
EDGE_THRESHOLD = 0.03   # 3 cents per contract — overcomes spread cost
PRICE_BIN_WIDTH = 0.05  # 5-cent yes_price bins for calibration

# Ship gates (same as v1, applied to TEST set only)
SHIP_PRECISION_AT_100 = 0.60
SHIP_ROC_AUC = 0.55


def _log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"{ts}  {msg}", flush=True)


def _write_audit(run_id: str, payload: dict) -> None:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    out = AUDIT_DIR / f"detect_v2_pnl_aware_{run_id}.json"
    tmp = out.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True, default=float), encoding="utf-8")
    os.replace(tmp, out)


# -------------------- v1 flags + price-bin helper --------------------

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


# -------------------- calibration --------------------

def _fit_calibration(train: pl.DataFrame, flag_col: str) -> pl.DataFrame:
    """
    For trades where v1 flag is true AND outcome is known, group by
    (yes_price_bin, taker_side) and compute the empirical win rate +
    sample size. Add a global pwin for cells with sparse data fallback.
    """
    flagged_resolved = train.filter(
        pl.col(flag_col) & pl.col("was_taker_correct").is_not_null()
    )
    cal = (flagged_resolved
           .group_by(["yes_price_bin", "taker_side"])
           .agg([
               pl.col("was_taker_correct").mean().alias("pwin_hat"),
               pl.len().alias("n_train"),
           ]))
    # Global fallback per flag (used for bins with too-few-samples)
    global_pwin = float(flagged_resolved["was_taker_correct"].mean() or 0.0)
    return cal, global_pwin


def _apply_calibration(test: pl.DataFrame, cal: pl.DataFrame, flag_col: str, global_pwin: float, min_n: int = 20) -> pl.DataFrame:
    """
    Join the calibration table to test rows; for cells with n_train < min_n,
    fall back to global pwin. Compute edge = pwin_hat - taker_break_even_price.
    """
    joined = test.join(cal, on=["yes_price_bin", "taker_side"], how="left")
    joined = joined.with_columns(
        pl.when(pl.col("n_train").fill_null(0) >= min_n)
          .then(pl.col("pwin_hat"))
          .otherwise(pl.lit(global_pwin))
          .alias("pwin_hat_used")
    )
    edge_col = f"edge_{flag_col.replace('flag_', '')}"
    score_col = f"score_{flag_col.replace('flag_', '')}_v2"
    joined = joined.with_columns(
        (pl.col("pwin_hat_used") - pl.col("taker_break_even_price")).alias(edge_col)
    )
    joined = joined.with_columns(
        pl.col(edge_col).alias(score_col)
    )
    v2_flag_col = flag_col + "_v2"
    joined = joined.with_columns(
        (pl.col(flag_col) & (pl.col(edge_col) > EDGE_THRESHOLD)).alias(v2_flag_col)
    )
    return joined.drop(["pwin_hat", "n_train", "pwin_hat_used"])


# -------------------- metrics --------------------

def _precision_at_k(test: pl.DataFrame, score_col: str, K: int) -> float | None:
    res = test.filter(pl.col("was_taker_correct").is_not_null() & pl.col(score_col).is_not_null())
    if res.height == 0:
        return None
    top = res.sort(score_col, descending=True).head(K)
    if top.height == 0:
        return None
    return float(top["was_taker_correct"].mean() or 0.0)


def _roc_auc(test: pl.DataFrame, score_col: str) -> float | None:
    res = test.filter(pl.col("was_taker_correct").is_not_null() & pl.col(score_col).is_not_null()).select([
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
        pos = rng.sample(pos, min(len(pos), 100_000))
        neg = rng.sample(neg, min(len(neg), 100_000))
    combined = sorted([(s, 1) for s in pos] + [(s, 0) for s in neg], key=lambda x: x[0])
    n_pos = len(pos); n_neg = len(neg)
    rank_sum_pos = 0.0
    i, rank = 0, 1
    while i < len(combined):
        j = i
        while j + 1 < len(combined) and combined[j + 1][0] == combined[i][0]:
            j += 1
        avg_rank = (rank + (rank + (j - i))) / 2.0
        for k in range(i, j + 1):
            if combined[k][1] == 1:
                rank_sum_pos += avg_rank
        rank += (j - i + 1)
        i = j + 1
    u = rank_sum_pos - n_pos * (n_pos + 1) / 2.0
    return float(u / (n_pos * n_neg))


def _pnl_stats(test: pl.DataFrame, flag_col: str) -> dict:
    res = test.filter(pl.col("was_taker_correct").is_not_null()).filter(pl.col(flag_col))
    n = res.height
    if n == 0:
        return {"n": 0, "total_pnl": 0.0, "mean_pnl_per_trade": 0.0, "hit_rate": 0.0,
                "sharpe": None, "n_days": 0}
    r = res.with_columns(
        (pl.col("taker_pnl_per_contract_dollars") * pl.col("count")).alias("_pnl"),
        pl.from_epoch(pl.col("created_time_unix"), time_unit="s").cast(pl.Date).alias("_d"),
    )
    daily = r.group_by("_d").agg(pl.col("_pnl").sum().alias("daily_pnl"))
    daily_pnl = daily["daily_pnl"].to_list()
    mean_d = sum(daily_pnl) / max(len(daily_pnl), 1)
    var_d = sum((x - mean_d) ** 2 for x in daily_pnl) / max(len(daily_pnl), 1)
    std_d = var_d ** 0.5
    sharpe = (math.sqrt(252) * mean_d / std_d) if std_d > 0 else None
    return {
        "n": n,
        "total_pnl": float(r["_pnl"].sum() or 0.0),
        "mean_pnl_per_trade": float(r["_pnl"].mean() or 0.0),
        "hit_rate": float(res["was_taker_correct"].mean() or 0.0),
        "sharpe": sharpe,
        "n_days": len(daily_pnl),
    }


# -------------------- report --------------------

DETECTORS_V2 = [
    ("whale",        "flag_whale",        "score_whale_v2",        "flag_whale_v2"),
    ("volume_burst", "flag_volume_burst", "score_volume_burst_v2", "flag_volume_burst_v2"),
    ("pre_play",     "flag_pre_play",     "score_pre_play_v2",     "flag_pre_play_v2"),
]


def _write_report(path: Path, train_window: tuple[str, str], test_window: tuple[str, str],
                  results: list[dict], n_test: int, n_test_resolved: int, baseline_wr: float | None) -> None:
    L = []
    L.append("# v2 PnL-aware backtest")
    L.append("")
    L.append(f"_Generated by `{VERSION}` on {datetime.now(timezone.utc).isoformat()}._")
    L.append("")
    L.append(f"- TRAIN window: {train_window[0]} → {train_window[1]}")
    L.append(f"- TEST window:  {test_window[0]} → {test_window[1]}")
    L.append(f"- Test trades:  {n_test:,}")
    L.append(f"- Test resolved: {n_test_resolved:,}")
    if baseline_wr is not None:
        L.append(f"- Test baseline win rate: **{100*baseline_wr:.2f}%**")
    L.append(f"- Edge threshold: pwin_hat − taker_break_even_price > {EDGE_THRESHOLD:+.2f}")
    L.append(f"- Ship gates: precision@100 ≥ {SHIP_PRECISION_AT_100:.2f}  AND  ROC AUC ≥ {SHIP_ROC_AUC:.2f}  AND  mean PnL/trade > 0")
    L.append("")
    L.append("## Verdicts (TEST set)")
    L.append("")
    L.append("| Detector | Verdict | precision@100 | precision@1K | ROC AUC | PnL/trade | Sharpe | Flagged |")
    L.append("|:---|:---|---:|---:|---:|---:|---:|---:|")
    for r in results:
        p100 = f"{r['precision_at_100']:.3f}" if r['precision_at_100'] is not None else "—"
        p1k  = f"{r['precision_at_1k']:.3f}"  if r['precision_at_1k']  is not None else "—"
        auc  = f"{r['roc_auc']:.3f}"          if r['roc_auc']          is not None else "—"
        pnl  = f"${r['pnl']['mean_pnl_per_trade']:.4f}" if r['pnl']['n'] > 0 else "—"
        sh   = f"{r['pnl']['sharpe']:.2f}"    if r['pnl']['sharpe']   is not None else "—"
        L.append(f"| {r['detector']}_v2 | **{r['verdict']}** | {p100} | {p1k} | {auc} | {pnl} | {sh} | {r['pnl']['n']:,} |")
    L.append("")
    for r in results:
        L.append(f"## {r['detector']}_v2")
        L.append("")
        L.append(f"- v1 candidate gate: {r['v1_flag_col']} = True")
        L.append(f"- v2 flag: v1_flag AND edge > {EDGE_THRESHOLD:+.2f}")
        L.append(f"- TRAIN flagged: {r['n_train_flagged']:,}  (calibration sample)")
        L.append(f"- TEST  flagged: {r['pnl']['n']:,}")
        L.append(f"- precision@100:  {r['precision_at_100']:.4f}" if r['precision_at_100'] is not None else "- precision@100:  n/a")
        L.append(f"- precision@1K:   {r['precision_at_1k']:.4f}" if r['precision_at_1k']  is not None else "- precision@1K:   n/a")
        L.append(f"- ROC AUC:        {r['roc_auc']:.4f}" if r['roc_auc'] is not None else "- ROC AUC:        n/a")
        L.append(f"- Hit rate:       {100*r['pnl']['hit_rate']:.2f}%")
        L.append(f"- Mean PnL/trade: ${r['pnl']['mean_pnl_per_trade']:.4f}")
        L.append(f"- Total PnL:      ${r['pnl']['total_pnl']:>12,.2f}")
        if r['pnl']['sharpe'] is not None:
            L.append(f"- Sharpe (daily): {r['pnl']['sharpe']:.2f}  ({r['pnl']['n_days']} days)")
        if r['ship_reasons_failed']:
            L.append("")
            L.append("**Reasons failed to ship:**")
            for s in r['ship_reasons_failed']:
                L.append(f"  - {s}")
        L.append("")
    L.append("---")
    L.append("")
    L.append(f"**Methodology.** Calibration of P(win | flag, yes_price_bin, taker_side) is fit ONLY")
    L.append(f"on the train window. Per-trade score on test = pwin_hat − taker_break_even_price.")
    L.append(f"v2 flag = v1_flag AND score > {EDGE_THRESHOLD:+.2f}. Train ≠ test (no leakage).")
    L.append(f"PnL uses taker_pnl_per_contract_dollars × count. Sharpe = √252 · mean / std of daily PnL.")
    path.write_text("\n".join(L), encoding="utf-8")


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
    ap = argparse.ArgumentParser(description="PnL-aware v2 detectors + train/test backtest")
    ap.add_argument("--dt", type=str, default=None)
    ap.add_argument("--days", type=int, default=14)
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--train-frac", type=float, default=0.70,
                    help="Fraction of dt partitions to use as TRAIN (default 0.70)")
    args = ap.parse_args()

    run_id = f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"
    started = time.time()
    dates = _list_dates(args)
    if len(dates) < 3:
        _log("need >= 3 enriched partitions for a train/test split; exiting")
        return 1
    n_train = max(1, int(len(dates) * args.train_frac))
    train_dates = dates[:n_train]
    test_dates = dates[n_train:]

    _log("=" * 72)
    _log(f"detect_v2_pnl_aware run_id={run_id}")
    _log(f"  version    = {VERSION}")
    _log(f"  TRAIN      = {train_dates}")
    _log(f"  TEST       = {test_dates}")
    _log("=" * 72)

    def _load(dates: list[str]) -> pl.DataFrame:
        files = []
        for dt in dates:
            files.extend(sorted(glob.glob(str(ENRICHED_DIR / f"dt={dt}/*.parquet"))))
        files = [f for f in files if not f.endswith(".tmp")]
        parts = [pl.read_parquet(f) for f in files]
        return pl.concat(parts, how="diagonal_relaxed") if parts else pl.DataFrame()

    _log("loading train…")
    train = _load(train_dates)
    train = _add_v1_flags(train)
    _log(f"  train rows: {train.height:,}")
    _log("loading test…")
    test = _load(test_dates)
    test = _add_v1_flags(test)
    _log(f"  test rows:  {test.height:,}")

    results = []
    for det_id, v1_flag, score_col, v2_flag in DETECTORS_V2:
        _log(f"detector={det_id}")
        cal, gpwin = _fit_calibration(train, v1_flag)
        _log(f"  calibration cells: {cal.height:,}  global pwin: {gpwin:.3f}")
        # Apply calibration to test
        test = _apply_calibration(test, cal, v1_flag, gpwin, min_n=20)
        n_train_flagged = int(train.filter(pl.col(v1_flag)).height)

        p100 = _precision_at_k(test, score_col, 100)
        p1k  = _precision_at_k(test, score_col, 1_000)
        p10k = _precision_at_k(test, score_col, 10_000)
        auc  = _roc_auc(test, score_col)
        pnl  = _pnl_stats(test, v2_flag)

        ship_reasons = []
        if p100 is None or p100 < SHIP_PRECISION_AT_100:
            ship_reasons.append(f"precision@100={p100} < {SHIP_PRECISION_AT_100}")
        if auc is None or auc < SHIP_ROC_AUC:
            ship_reasons.append(f"ROC_AUC={auc} < {SHIP_ROC_AUC}")
        if pnl["mean_pnl_per_trade"] <= 0:
            ship_reasons.append(f"mean_pnl_per_trade={pnl['mean_pnl_per_trade']:.4f} <= 0")
        verdict = "SHIP" if not ship_reasons else "NO-SHIP — retune"

        results.append({
            "detector": det_id,
            "v1_flag_col": v1_flag,
            "v2_score_col": score_col,
            "v2_flag_col": v2_flag,
            "n_train_flagged": n_train_flagged,
            "calibration_cells": cal.height,
            "global_pwin": gpwin,
            "precision_at_100": p100,
            "precision_at_1k": p1k,
            "precision_at_10k": p10k,
            "roc_auc": auc,
            "pnl": pnl,
            "verdict": verdict,
            "ship_reasons_failed": ship_reasons,
        })

    test_resolved = int(test.filter(pl.col("was_taker_correct").is_not_null()).height)
    baseline = float(test.filter(pl.col("was_taker_correct").is_not_null())["was_taker_correct"].mean() or 0.0) if test_resolved else None

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    report = REPORTS_DIR / f"v2_backtest_{run_id}.md"
    _write_report(report, (train_dates[0], train_dates[-1]), (test_dates[0], test_dates[-1]),
                  results, test.height, test_resolved, baseline)
    _log(f"report -> {report.relative_to(PROJECT_ROOT)}")

    audit = {
        "run_id": run_id,
        "version": VERSION,
        "started_at_utc": datetime.fromtimestamp(started, tz=timezone.utc).isoformat(),
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "duration_seconds": round(time.time() - started, 3),
        "train_dates": train_dates,
        "test_dates": test_dates,
        "train_frac": args.train_frac,
        "test_rows": test.height,
        "test_resolved": test_resolved,
        "test_baseline_win_rate": baseline,
        "edge_threshold": EDGE_THRESHOLD,
        "ship_gates": {
            "precision_at_100_min": SHIP_PRECISION_AT_100,
            "roc_auc_min": SHIP_ROC_AUC,
        },
        "detectors": results,
        "report_path": str(report.relative_to(PROJECT_ROOT)),
    }
    _write_audit(run_id, audit)
    _log("=" * 72)
    for r in results:
        _log(f"  {r['detector']:15s}  {r['verdict']}")
    _log(f"DONE  duration={time.time()-started:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
