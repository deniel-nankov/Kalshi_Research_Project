#!/usr/bin/env python3
# ruff: noqa: E402
"""
Phase 6 — Detector validation / backtest harness.

For each surveillance detector currently shipped, compute on a window of
resolved enriched_trades:

  Precision @ K           K = 100 / 1K / 10K
  ROC AUC                 score → was_taker_correct
  Calibration table       10 score bins → actual win rate per bin
  Mean PnL per trade      flagged vs. baseline   (taker_pnl_per_contract × count)
  Total PnL               sum across flagged trades
  Sharpe (daily-rebalanced) of "follow the flagged side" paper strategy

Detectors covered:
  1. Whale          (size_pct_global_today × size_pct_in_market_today)
  2. Volume burst   (volume_z_5min)
  3. Pre-play alpha (in_pre_play_window × notional)

Spoofing requires a market-hour join against spoof_intensity_hourly —
covered in a separate backtest (see scripts/backtest_spoofing.py — TODO).

Ship gates (per detector):
  precision@100 >= 0.60    AND
  ROC AUC       >= 0.55    AND
  mean PnL/trade > 0

A detector that fails any gate is flagged "NO-SHIP — retune" in the
report; the codebase keeps it but it does not contribute to live alerts
until it passes a future re-run.

Critical methodology notes:
  - Only trades with `was_taker_correct IS NOT NULL` (i.e. resolved
    markets) are scored. Unresolved trades are completely excluded.
  - Score functions never reference `was_taker_correct` or
    `winning_side` — no outcome leakage.
  - All percentile features are computed per-day, so a trade is
    scored against its same-day peer distribution; no look-ahead.
  - The paper strategy uses `taker_pnl_per_contract_dollars * count`
    as per-trade PnL (already computed by enrich_trades.py from the
    eventual resolution).
  - Sharpe is daily-rebalanced: sum PnL per day, compute
    sqrt(252) * mean / std across days.

Usage
-----
  uv run python scripts/backtest_detectors.py                # all enriched dates
  uv run python scripts/backtest_detectors.py --days 30      # rolling window
  uv run python scripts/backtest_detectors.py --dt 2026-05-15
"""

from __future__ import annotations

import argparse
import glob
import hashlib
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

VERSION = "backtest_detectors@1.0.0"

# Detector thresholds (must match the live detectors so the backtest measures
# THE SAME flag rule used in production).
WHALE_GLOBAL_THRESHOLD = 0.99
WHALE_MARKET_THRESHOLD = 0.95
VOLUME_BURST_Z = 3.0
PRE_PLAY_WINDOW_SEC = 300
PRE_PLAY_MIN_NOTIONAL = 100.0

# Ship gates
SHIP_PRECISION_AT_100 = 0.60
SHIP_ROC_AUC = 0.55


# -------------------- helpers --------------------

def _log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"{ts}  {msg}", flush=True)


def _write_audit(run_id: str, payload: dict) -> None:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    out = AUDIT_DIR / f"backtest_{run_id}.json"
    tmp = out.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True, default=float), encoding="utf-8")
    os.replace(tmp, out)


# -------------------- detector scoring (must match live) --------------------

def _add_scores(df: pl.DataFrame) -> pl.DataFrame:
    """Add score_<detector> and flag_<detector> columns. No outcome leakage."""
    # Whale
    df = df.with_columns([
        (pl.col("size_pct_global_today").fill_null(0.0)
         * pl.col("size_pct_in_market_today").fill_null(0.0)
         ).alias("score_whale"),
    ])
    df = df.with_columns([
        ((pl.col("size_pct_global_today") >= WHALE_GLOBAL_THRESHOLD) &
         (pl.col("size_pct_in_market_today") >= WHALE_MARKET_THRESHOLD)).alias("flag_whale"),
    ])

    # Volume burst — z>3 in 5-min window, score normalized
    df = df.with_columns([
        pl.when(pl.col("volume_z_5min").is_null())
          .then(0.0)
          .otherwise(
              pl.min_horizontal([
                  pl.max_horizontal([pl.col("volume_z_5min") / 5.0, pl.lit(0.0)]),
                  pl.lit(1.0),
              ])
          )
          .alias("score_volume_burst"),
        (pl.col("volume_z_5min").fill_null(0.0) >= VOLUME_BURST_Z).alias("flag_volume_burst"),
    ])

    # Pre-play alpha
    if "in_pre_play_window" in df.columns and "seconds_to_next_play_event" in df.columns:
        norm = math.log(1.0 + 1.0 / 30.0)
        df = df.with_columns([
            pl.when(pl.col("in_pre_play_window") & (pl.col("seconds_to_next_play_event") > 0))
              .then((1.0 + 1.0 / pl.col("seconds_to_next_play_event").cast(pl.Float64)).log() / norm)
              .otherwise(0.0)
              .clip(0.0, 1.0)
              .alias("score_pre_play"),
            (pl.col("in_pre_play_window") &
             (pl.col("notional_usd") >= PRE_PLAY_MIN_NOTIONAL)).alias("flag_pre_play"),
        ])
    else:
        df = df.with_columns([
            pl.lit(0.0).alias("score_pre_play"),
            pl.lit(False).alias("flag_pre_play"),
        ])
    return df


# -------------------- metrics --------------------

def _precision_at_k(scored: pl.DataFrame, score_col: str, K: int) -> float | None:
    """Top-K trades by score: fraction with was_taker_correct == True."""
    res = scored.filter(pl.col("was_taker_correct").is_not_null())
    if res.height == 0:
        return None
    top = res.sort(score_col, descending=True).head(K)
    if top.height == 0:
        return None
    return float(top["was_taker_correct"].mean() or 0.0)


def _roc_auc(scored: pl.DataFrame, score_col: str) -> float | None:
    """
    Compute ROC AUC manually via the Mann-Whitney U identity:
        AUC = P(score(pos) > score(neg))
    Resolved trades only.
    """
    res = scored.filter(pl.col("was_taker_correct").is_not_null()).select([
        score_col, "was_taker_correct"
    ]).rename({score_col: "score"})
    if res.height == 0:
        return None
    pos = res.filter(pl.col("was_taker_correct"))["score"].to_list()
    neg = res.filter(~pl.col("was_taker_correct"))["score"].to_list()
    if not pos or not neg:
        return None
    # Rank-based AUC (efficient enough for ~15M trades by sampling if huge)
    if len(pos) + len(neg) > 200_000:
        import random
        rng = random.Random(42)
        pos_s = rng.sample(pos, min(len(pos), 100_000))
        neg_s = rng.sample(neg, min(len(neg), 100_000))
    else:
        pos_s, neg_s = pos, neg
    # Combined ranking
    combined = sorted(((s, lbl) for s, lbl in [(s, 1) for s in pos_s] + [(s, 0) for s in neg_s]),
                      key=lambda x: x[0])
    n_pos = sum(1 for _, l in combined if l == 1)
    n_neg = sum(1 for _, l in combined if l == 0)
    if n_pos == 0 or n_neg == 0:
        return None
    rank_sum_pos = 0
    rank = 1
    i = 0
    while i < len(combined):
        j = i
        # Handle ties: average rank
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


def _calibration(scored: pl.DataFrame, score_col: str, n_bins: int = 10) -> list[dict]:
    res = scored.filter(pl.col("was_taker_correct").is_not_null()).filter(pl.col(score_col) > 0)
    if res.height == 0:
        return []
    # quantile-based binning
    bins = []
    qs = [i / n_bins for i in range(n_bins + 1)]
    edges = res[score_col].quantile_n(qs).to_list() if hasattr(res[score_col], "quantile_n") else None
    if edges is None:
        # Fallback: use np-like quantiles via Polars repeat-call
        edges = [float(res[score_col].quantile(q) or 0.0) for q in qs]
    edges = sorted(set(edges))
    for lo, hi in zip(edges[:-1], edges[1:]):
        bucket = res.filter((pl.col(score_col) >= lo) & (pl.col(score_col) < hi))
        if bucket.height == 0:
            continue
        bins.append({
            "lo": float(lo),
            "hi": float(hi),
            "n": bucket.height,
            "win_rate": float(bucket["was_taker_correct"].mean() or 0.0),
            "mean_score": float(bucket[score_col].mean() or 0.0),
        })
    return bins


def _pnl_stats(scored: pl.DataFrame, flag_col: str) -> dict:
    """
    Paper-strategy: bet `count` contracts at the taker's side for every flagged
    trade. PnL/contract = taker_pnl_per_contract_dollars. Sum across resolved
    flagged trades.

    Daily rebalanced Sharpe:
      group by date(created_time_unix), sum pnl per day, then sqrt(252) × mean / std.
    """
    res = scored.filter(pl.col("was_taker_correct").is_not_null()).filter(pl.col(flag_col))
    n = res.height
    if n == 0:
        return {"n": 0, "total_pnl": 0.0, "mean_pnl_per_trade": 0.0, "hit_rate": 0.0,
                "sharpe": None, "n_days": 0, "total_count": 0.0}
    pnl_expr = (pl.col("taker_pnl_per_contract_dollars") * pl.col("count")).alias("_pnl")
    r = res.with_columns(pnl_expr).with_columns(
        pl.from_epoch(pl.col("created_time_unix"), time_unit="s").cast(pl.Date).alias("_d")
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
        "n_days": len(daily_pnl),
        "daily_pnl": [float(x) for x in daily_pnl],
        "sharpe": sharpe,
        "total_count": float(res["count"].sum() or 0.0),
    }


def _baseline_winrate(scored: pl.DataFrame) -> float | None:
    res = scored.filter(pl.col("was_taker_correct").is_not_null())
    if res.height == 0:
        return None
    return float(res["was_taker_correct"].mean() or 0.0)


# -------------------- per-detector backtest --------------------

DETECTORS = [
    {"id": "whale",         "score_col": "score_whale",         "flag_col": "flag_whale"},
    {"id": "volume_burst",  "score_col": "score_volume_burst",  "flag_col": "flag_volume_burst"},
    {"id": "pre_play",      "score_col": "score_pre_play",      "flag_col": "flag_pre_play"},
]


def _evaluate_detector(scored: pl.DataFrame, d: dict, baseline_wr: float | None) -> dict:
    score_col = d["score_col"]
    flag_col = d["flag_col"]
    pAt100 = _precision_at_k(scored, score_col, 100)
    pAt1K  = _precision_at_k(scored, score_col, 1_000)
    pAt10K = _precision_at_k(scored, score_col, 10_000)
    auc = _roc_auc(scored, score_col)
    calib = _calibration(scored, score_col)
    pnl = _pnl_stats(scored, flag_col)

    # Ship verdict
    ship_reasons = []
    if pAt100 is None or pAt100 < SHIP_PRECISION_AT_100:
        ship_reasons.append(f"precision@100={pAt100} < {SHIP_PRECISION_AT_100}")
    if auc is None or auc < SHIP_ROC_AUC:
        ship_reasons.append(f"ROC_AUC={auc} < {SHIP_ROC_AUC}")
    if pnl["mean_pnl_per_trade"] <= 0:
        ship_reasons.append(f"mean_pnl_per_trade={pnl['mean_pnl_per_trade']:.4f} <= 0")
    verdict = "SHIP" if not ship_reasons else "NO-SHIP — retune"

    return {
        "detector": d["id"],
        "precision_at_100": pAt100,
        "precision_at_1k": pAt1K,
        "precision_at_10k": pAt10K,
        "roc_auc": auc,
        "calibration_bins": calib,
        "pnl": pnl,
        "baseline_win_rate": baseline_wr,
        "verdict": verdict,
        "ship_reasons_failed": ship_reasons,
    }


# -------------------- report writer --------------------

def _write_report(path: Path, window: tuple[str, str], results: list[dict], scored_height: int, resolved: int, baseline_wr: float | None) -> None:
    L = []
    L.append(f"# Backtest report — {window[0]} → {window[1]}")
    L.append("")
    L.append(f"_Generated by `{VERSION}` on {datetime.now(timezone.utc).isoformat()}._")
    L.append("")
    L.append("## Window")
    L.append("")
    L.append(f"- Trades evaluated:  {scored_height:,}")
    L.append(f"- Resolved (labeled): {resolved:,}")
    L.append(f"- Baseline taker win rate: **{100*baseline_wr:.2f}%**" if baseline_wr is not None else "- Baseline taker win rate: n/a")
    L.append(f"- Ship gates: precision@100 ≥ {SHIP_PRECISION_AT_100:.2f}  AND  ROC AUC ≥ {SHIP_ROC_AUC:.2f}  AND  mean PnL/trade > 0")
    L.append("")
    L.append("## Verdicts")
    L.append("")
    L.append("| Detector | Verdict | precision@100 | precision@1K | ROC AUC | PnL/trade | Sharpe | Flagged trades |")
    L.append("|:---|:---|---:|---:|---:|---:|---:|---:|")
    for r in results:
        p100 = f"{r['precision_at_100']:.3f}" if r['precision_at_100'] is not None else "—"
        p1k  = f"{r['precision_at_1k']:.3f}" if r['precision_at_1k'] is not None else "—"
        auc  = f"{r['roc_auc']:.3f}" if r['roc_auc'] is not None else "—"
        pnl  = f"${r['pnl']['mean_pnl_per_trade']:.4f}" if r['pnl']['n'] > 0 else "—"
        sh   = f"{r['pnl']['sharpe']:.2f}" if r['pnl']['sharpe'] is not None else "—"
        n    = r['pnl']['n']
        verdict = r['verdict']
        L.append(f"| {r['detector']} | **{verdict}** | {p100} | {p1k} | {auc} | {pnl} | {sh} | {n:,} |")
    L.append("")

    for r in results:
        L.append(f"## {r['detector']}")
        L.append("")
        L.append(f"- precision@100:  {r['precision_at_100']:.4f}" if r['precision_at_100'] is not None else "- precision@100:  n/a")
        L.append(f"- precision@1K:   {r['precision_at_1k']:.4f}" if r['precision_at_1k'] is not None else "- precision@1K:   n/a")
        L.append(f"- precision@10K:  {r['precision_at_10k']:.4f}" if r['precision_at_10k'] is not None else "- precision@10K:  n/a")
        L.append(f"- ROC AUC:        {r['roc_auc']:.4f}" if r['roc_auc'] is not None else "- ROC AUC:        n/a")
        L.append(f"- Flagged trades: {r['pnl']['n']:,}")
        L.append(f"- Flagged total PnL:    ${r['pnl']['total_pnl']:>12,.2f}")
        L.append(f"- Mean PnL per trade:   ${r['pnl']['mean_pnl_per_trade']:.4f}")
        L.append(f"- Hit rate (resolved):  {100*r['pnl']['hit_rate']:.2f}%")
        if r['pnl']['sharpe'] is not None:
            L.append(f"- Daily-rebalanced Sharpe: {r['pnl']['sharpe']:.2f}  (over {r['pnl']['n_days']} days)")
        if r['ship_reasons_failed']:
            L.append("")
            L.append("**Reasons failed to ship:**")
            for s in r['ship_reasons_failed']:
                L.append(f"  - {s}")
        if r['calibration_bins']:
            L.append("")
            L.append("### Calibration (10 quantile bins of score)")
            L.append("")
            L.append("| Score bin | n | Mean score | Actual win rate |")
            L.append("|:---|---:|---:|---:|")
            for b in r['calibration_bins']:
                L.append(f"| [{b['lo']:.3f}, {b['hi']:.3f}) | {b['n']:,} | {b['mean_score']:.3f} | {100*b['win_rate']:.1f}% |")
        L.append("")
    L.append("---")
    L.append("")
    L.append("**Methodology.** Resolved-trade-only evaluation; scores never reference outcome columns")
    L.append("(no leakage). All percentile features are same-day to avoid look-ahead. Paper-strategy")
    L.append(f"PnL uses taker_pnl_per_contract_dollars × count as per-trade PnL. Sharpe uses √252 ×")
    L.append("mean(daily_pnl) / std(daily_pnl). Ship gates: precision@100 ≥ 0.60 AND ROC AUC ≥ 0.55 AND")
    L.append("mean PnL/trade > 0.")
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
    ap = argparse.ArgumentParser(description="Backtest surveillance detectors on enriched_trades")
    ap.add_argument("--dt",   type=str, default=None)
    ap.add_argument("--days", type=int, default=14)
    ap.add_argument("--all",  action="store_true")
    args = ap.parse_args()

    run_id = f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"
    started = time.time()
    dates = _list_dates(args)

    _log("=" * 72)
    _log(f"backtest_detectors run_id={run_id}")
    _log(f"  version    = {VERSION}")
    _log(f"  partitions = {len(dates)}  range=[{dates[0] if dates else '-'}, {dates[-1] if dates else '-'}]")
    _log("=" * 72)

    if not dates:
        _log("no enriched_trades partitions; exiting")
        return 0

    files = []
    for dt in dates:
        files.extend(sorted(glob.glob(str(ENRICHED_DIR / f"dt={dt}/*.parquet"))))
    files = [f for f in files if not f.endswith(".tmp")]

    _log(f"loading {len(files):,} enriched parquets…")
    # Schemas may vary across partitions because each enrichment stage adds
    # columns (a partition that hasn't been pbp-enriched yet won't have
    # in_pre_play_window etc.). Use diagonal_relaxed concat to union schemas.
    parts = [pl.read_parquet(f) for f in files]
    df = pl.concat(parts, how="diagonal_relaxed")
    del parts
    _log(f"loaded {df.height:,} trades in {time.time()-started:.1f}s")

    df = _add_scores(df)
    resolved = int((df["was_taker_correct"].is_not_null()).sum() or 0)
    baseline = _baseline_winrate(df)
    _log(f"resolved (labeled): {resolved:,}  baseline win rate: {100*baseline:.2f}%" if baseline is not None else "no resolved trades; AUC will be n/a")

    results = []
    for d in DETECTORS:
        _log(f"  evaluating {d['id']}…")
        results.append(_evaluate_detector(df, d, baseline))

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    report = REPORTS_DIR / f"backtest_{run_id}.md"
    _write_report(report, (dates[0], dates[-1]), results, df.height, resolved, baseline)
    _log(f"report -> {report.relative_to(PROJECT_ROOT)}")

    audit = {
        "run_id": run_id,
        "version": VERSION,
        "started_at_utc": datetime.fromtimestamp(started, tz=timezone.utc).isoformat(),
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "duration_seconds": round(time.time() - started, 3),
        "window_start": dates[0],
        "window_end": dates[-1],
        "trades_evaluated": df.height,
        "resolved": resolved,
        "baseline_win_rate": baseline,
        "ship_gates": {
            "precision_at_100_min": SHIP_PRECISION_AT_100,
            "roc_auc_min": SHIP_ROC_AUC,
            "mean_pnl_per_trade_min": 0.0,
        },
        "detectors": results,
        "report_path": str(report.relative_to(PROJECT_ROOT)),
    }
    _write_audit(run_id, audit)
    _log("=" * 72)
    _log(f"DONE  duration={time.time()-started:.1f}s")
    for r in results:
        _log(f"  {r['detector']:15s}  {r['verdict']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
