#!/usr/bin/env python3
# ruff: noqa: E402
"""
Phase 4.6 — Cross-market event-level alpha detector.

An event_ticker on Kalshi typically groups multiple correlated markets
(e.g., KXNBAGAME-26MAY15SASMIN has both SASMIN-MIN and SASMIN-SAS;
KXMLBGAME-* has team-win + run-total + first-to-score markets).
Informed flow on a single event often appears CONCURRENTLY across
several of its markets — that concentration is a stronger signal than
any single-market burst.

What we detect
--------------
For every event_ticker × 5-min bucket on the test window:

  burst_density = (# flagged volume_burst_v2 trades in bucket
                   across all of the event's markets)
  notional_concentration = (total flagged_notional in bucket)
  markets_touched         = (# distinct markets flagged in bucket)

A bucket is `cross_market_flag = True` when:
    markets_touched >= MIN_MARKETS_TOUCHED   AND
    burst_density   >= MIN_FLAGGED_TRADES    AND
    notional_concentration >= MIN_NOTIONAL_USD

The detector then attaches every constituent trade in a flagged bucket
to the bucket's score:

  score_cross_market = log1p(notional_concentration / 1000)
                       × clip(markets_touched / 3, 0, 1.5)
                       × clip(burst_density / 10,  0, 2.0)

Why event-level
---------------
Cross-market concentration is the textbook propagation signature:
informed flow rarely sits in just one contract. When a Knicks-related
piece of news arrives, money usually appears in both "Knicks win game"
AND "Knicks reach playoffs" AND "total points >X" at the same time.
Single-market detectors miss this; event-level catches it.

Methodology guardrails
----------------------
- Train/test split, same convention as detect_v2_pnl_aware (default
  70/30 by partition date).
- Calibration fit on TRAIN only; metrics reported on TEST only.
- Score never references was_taker_correct (no leakage).

Outputs
-------
  data/kalshi/derived/cross_market_flags/dt=YYYY-MM-DD/<run_id>.parquet
    Per-trade flags + scores (joined back from event-level bucket flags).
  data/kalshi/derived/cross_market_buckets/dt=YYYY-MM-DD/<run_id>.parquet
    The bucket-level records themselves.
  surveillance/reports/cross_market_backtest_<run_id>.md
  state/detector_runs/detect_cross_market_<run_id>.json
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
OUT_FLAGS_DIR = DERIVED / "cross_market_flags"
OUT_BUCKETS_DIR = DERIVED / "cross_market_buckets"
REPORTS_DIR = PROJECT_ROOT / "surveillance" / "reports"
AUDIT_DIR = STATE / "detector_runs"

VERSION = "detect_cross_market@1.0.0"

# v1 base-flag thresholds (used to define the source of "flagged trades")
WHALE_GLOBAL_THRESHOLD = 0.99
WHALE_MARKET_THRESHOLD = 0.95
VOLUME_BURST_Z = 3.0
PRICE_BIN_WIDTH = 0.05
V2_EDGE_THRESHOLD = 0.07

# Cross-market bucket rules
BUCKET_SECONDS = 300                # 5-min buckets
MIN_MARKETS_TOUCHED = 2
MIN_FLAGGED_TRADES = 5
MIN_NOTIONAL_USD = 1_000.0

# Ship gates (TEST-set only)
SHIP_PRECISION_AT_100 = 0.60
SHIP_ROC_AUC = 0.55


def _log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"{ts}  {msg}", flush=True)


def _sha256_file(path: Path, chunk: int = 1 << 20) -> str:
    import hashlib
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
    out = AUDIT_DIR / f"detect_cross_market_{run_id}.json"
    tmp = out.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True, default=float), encoding="utf-8")
    os.replace(tmp, out)


# -------------------- helpers --------------------

def _add_v2_volume_burst_flag(df: pl.DataFrame, train: pl.DataFrame | None = None) -> pl.DataFrame:
    """
    Apply v2 volume_burst calibration to df. If train is None, fit
    calibration on df itself (single-window mode). If train is provided,
    fit on it (train/test mode).
    """
    base = train if train is not None else df
    base = base.with_columns([
        (pl.col("volume_z_5min").fill_null(0.0) >= VOLUME_BURST_Z).alias("flag_volume_burst"),
        ((pl.col("yes_price_dollars") / PRICE_BIN_WIDTH).floor().cast(pl.Int32) * PRICE_BIN_WIDTH).alias("yes_price_bin"),
        pl.when(pl.col("taker_side") == "yes").then(pl.col("yes_price_dollars"))
                                              .otherwise(pl.col("no_price_dollars"))
                                              .alias("taker_break_even_price"),
    ])
    cal = (base.filter(pl.col("flag_volume_burst") & pl.col("was_taker_correct").is_not_null())
               .group_by(["yes_price_bin", "taker_side"])
               .agg([pl.col("was_taker_correct").mean().alias("pwin_hat"),
                     pl.len().alias("n_train")]))
    gpwin = float(base.filter(pl.col("flag_volume_burst") & pl.col("was_taker_correct").is_not_null())["was_taker_correct"].mean() or 0.0)

    df = df.with_columns([
        (pl.col("volume_z_5min").fill_null(0.0) >= VOLUME_BURST_Z).alias("flag_volume_burst"),
        ((pl.col("yes_price_dollars") / PRICE_BIN_WIDTH).floor().cast(pl.Int32) * PRICE_BIN_WIDTH).alias("yes_price_bin"),
        pl.when(pl.col("taker_side") == "yes").then(pl.col("yes_price_dollars"))
                                              .otherwise(pl.col("no_price_dollars"))
                                              .alias("taker_break_even_price"),
    ])
    df = df.join(cal, on=["yes_price_bin", "taker_side"], how="left").with_columns(
        pl.when(pl.col("n_train").fill_null(0) >= 20)
          .then(pl.col("pwin_hat"))
          .otherwise(pl.lit(gpwin))
          .alias("pwin_hat_used")
    )
    df = df.with_columns([
        (pl.col("pwin_hat_used") - pl.col("taker_break_even_price")).alias("edge_volume_burst"),
    ])
    df = df.with_columns(
        (pl.col("flag_volume_burst") & (pl.col("edge_volume_burst") > V2_EDGE_THRESHOLD)).alias("flag_v2_volume_burst")
    )
    return df.drop(["pwin_hat", "n_train", "pwin_hat_used"])


def _build_event_buckets(df: pl.DataFrame) -> pl.DataFrame:
    """
    For each (event_ticker × 5-min bucket), aggregate the v2-flagged trades.
    """
    flagged = df.filter(pl.col("flag_v2_volume_burst") & pl.col("event_ticker").is_not_null())
    if flagged.is_empty():
        return pl.DataFrame()
    bucketed = flagged.with_columns(
        (pl.col("created_time_unix") // BUCKET_SECONDS * BUCKET_SECONDS).alias("bucket_unix")
    )
    buckets = (bucketed.group_by(["event_ticker", "bucket_unix"])
                       .agg([
                           pl.len().alias("burst_density"),
                           pl.col("notional_usd").sum().alias("notional_concentration"),
                           pl.col("market_ticker").n_unique().alias("markets_touched"),
                           pl.col("trade_id").alias("trade_ids"),
                       ]))
    buckets = buckets.with_columns(
        ((pl.col("markets_touched") >= MIN_MARKETS_TOUCHED) &
         (pl.col("burst_density") >= MIN_FLAGGED_TRADES) &
         (pl.col("notional_concentration") >= MIN_NOTIONAL_USD)).alias("cross_market_flag")
    )
    buckets = buckets.with_columns(
        (
            (pl.lit(1.0) + pl.col("notional_concentration") / 1000.0).log()
            * pl.min_horizontal([pl.col("markets_touched") / 3.0, pl.lit(1.5)])
            * pl.min_horizontal([pl.col("burst_density") / 10.0, pl.lit(2.0)])
        ).alias("score_cross_market")
    )
    return buckets


def _attach_bucket_to_trades(df: pl.DataFrame, buckets: pl.DataFrame) -> pl.DataFrame:
    """
    Join each underlying trade back to its (event, bucket). A trade gets
    cross_market_flag=True iff its bucket was flagged.
    """
    if buckets.is_empty():
        return df.with_columns([
            pl.lit(False).alias("cross_market_flag"),
            pl.lit(0.0).alias("score_cross_market"),
        ])
    df = df.with_columns(
        (pl.col("created_time_unix") // BUCKET_SECONDS * BUCKET_SECONDS).alias("bucket_unix")
    )
    keep = buckets.select(["event_ticker", "bucket_unix", "cross_market_flag", "score_cross_market",
                           "burst_density", "markets_touched", "notional_concentration"])
    df = df.join(keep, on=["event_ticker", "bucket_unix"], how="left")
    df = df.with_columns([
        pl.col("cross_market_flag").fill_null(False),
        pl.col("score_cross_market").fill_null(0.0),
    ])
    return df


# -------------------- metrics --------------------

def _precision_at_k(df: pl.DataFrame, score_col: str, flag_col: str, K: int) -> float | None:
    res = df.filter(pl.col("was_taker_correct").is_not_null() & pl.col(flag_col)).select([score_col, "was_taker_correct"])
    if res.height == 0:
        return None
    top = res.sort(score_col, descending=True).head(K)
    if top.height == 0:
        return None
    return float(top["was_taker_correct"].mean() or 0.0)


def _roc_auc(df: pl.DataFrame, score_col: str, flag_col: str) -> float | None:
    res = df.filter(pl.col("was_taker_correct").is_not_null() & pl.col(flag_col)).select([
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


def _pnl_stats(df: pl.DataFrame, flag_col: str) -> dict:
    res = df.filter(pl.col("was_taker_correct").is_not_null() & pl.col(flag_col))
    n = res.height
    if n == 0:
        return {"n": 0, "total_pnl": 0.0, "mean_pnl_per_trade": 0.0, "hit_rate": 0.0,
                "sharpe": None, "n_days": 0}
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


# -------------------- main --------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="Cross-market event-level detector + backtest")
    ap.add_argument("--days", type=int, default=14)
    ap.add_argument("--all",  action="store_true")
    ap.add_argument("--train-frac", type=float, default=0.70)
    args = ap.parse_args()

    run_id = f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"
    started = time.time()

    parts = sorted({Path(p).name for p in glob.glob(str(ENRICHED_DIR / "dt=*"))})
    parts = [p.replace("dt=", "") for p in parts if p.startswith("dt=")]
    if not args.all:
        last = parts[-1]
        cutoff = (datetime.fromisoformat(last) - timedelta(days=args.days)).date().isoformat()
        parts = [p for p in parts if p >= cutoff]
    if len(parts) < 3:
        _log("need >= 3 enriched partitions; exiting")
        return 1
    n_train = max(1, int(len(parts) * args.train_frac))
    train_dates, test_dates = parts[:n_train], parts[n_train:]

    _log("=" * 72)
    _log(f"detect_cross_market run_id={run_id}")
    _log(f"  TRAIN  = {train_dates}")
    _log(f"  TEST   = {test_dates}")
    _log(f"  bucket = {BUCKET_SECONDS}s,  min_markets={MIN_MARKETS_TOUCHED},")
    _log(f"           min_flagged={MIN_FLAGGED_TRADES},  min_notional=${MIN_NOTIONAL_USD:,.0f}")
    _log("=" * 72)

    def _load(dates: list[str]) -> pl.DataFrame:
        files = []
        for dt in dates:
            files.extend(sorted(glob.glob(str(ENRICHED_DIR / f"dt={dt}/*.parquet"))))
        files = [f for f in files if not f.endswith(".tmp")]
        return pl.concat([pl.read_parquet(f) for f in files], how="diagonal_relaxed")

    train = _load(train_dates)
    test  = _load(test_dates)
    _log(f"  train: {train.height:,}  test: {test.height:,}")

    # Apply v2 volume_burst with TRAIN calibration to BOTH train and test
    # (we only need it on test for the backtest, but training data needs flag
    # to know which trades cluster into buckets too).
    _log("applying v2 volume_burst calibration…")
    test_v2  = _add_v2_volume_burst_flag(test, train=train)
    _log(f"  test v2-flagged: {int(test_v2['flag_v2_volume_burst'].sum() or 0):,}")

    # Build event buckets from TEST data only — the cross-market signal
    # itself is computed on test using fresh v2 flags + their event groupings.
    _log("building event buckets…")
    buckets = _build_event_buckets(test_v2)
    _log(f"  buckets: {buckets.height:,}  flagged: {int(buckets['cross_market_flag'].sum() or 0) if not buckets.is_empty() else 0:,}")

    # Attach bucket back to trade rows
    test_v2 = _attach_bucket_to_trades(test_v2, buckets)

    # Metrics on cross_market_flag
    p100 = _precision_at_k(test_v2, "score_cross_market", "cross_market_flag", 100)
    p1k  = _precision_at_k(test_v2, "score_cross_market", "cross_market_flag", 1_000)
    p10k = _precision_at_k(test_v2, "score_cross_market", "cross_market_flag", 10_000)
    auc  = _roc_auc(test_v2, "score_cross_market", "cross_market_flag")
    pnl  = _pnl_stats(test_v2, "cross_market_flag")

    ship_reasons = []
    if p100 is None or p100 < SHIP_PRECISION_AT_100:
        ship_reasons.append(f"precision@100={p100} < {SHIP_PRECISION_AT_100}")
    if auc is None or auc < SHIP_ROC_AUC:
        ship_reasons.append(f"ROC_AUC={auc} < {SHIP_ROC_AUC}")
    if pnl["mean_pnl_per_trade"] <= 0:
        ship_reasons.append(f"mean_pnl_per_trade={pnl['mean_pnl_per_trade']:.4f} <= 0")
    verdict = "SHIP" if not ship_reasons else "NO-SHIP — retune"

    # Persist artifacts
    test_dt = test_dates[-1]
    OUT_FLAGS_DIR.mkdir(parents=True, exist_ok=True)
    OUT_BUCKETS_DIR.mkdir(parents=True, exist_ok=True)
    for outdir, payload, name in [
        (OUT_FLAGS_DIR,   test_v2.filter(pl.col("cross_market_flag")), "flags"),
        (OUT_BUCKETS_DIR, buckets.filter(pl.col("cross_market_flag")), "buckets"),
    ]:
        out_dt = outdir / f"dt={test_dt}"
        out_dt.mkdir(parents=True, exist_ok=True)
        fp = out_dt / f"{run_id}_{name}.parquet"
        tmp = fp.with_suffix(".parquet.tmp")
        if payload.height == 0:
            # Write empty marker with schema preserved
            payload.write_parquet(str(tmp), compression="zstd")
        else:
            payload.write_parquet(str(tmp), compression="zstd")
        tmp.replace(fp)
        _write_sha256_sidecar(fp)

    # Top events by score for the report
    top_events = (buckets.filter(pl.col("cross_market_flag"))
                  .sort("score_cross_market", descending=True)
                  .head(15)) if not buckets.is_empty() else pl.DataFrame()

    # Report
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    report = REPORTS_DIR / f"cross_market_backtest_{run_id}.md"
    L = []
    L.append("# Cross-market event-level detector backtest")
    L.append("")
    L.append(f"_Generated by `{VERSION}` on {datetime.now(timezone.utc).isoformat()}._")
    L.append("")
    L.append(f"- TRAIN window: {train_dates[0]} → {train_dates[-1]}")
    L.append(f"- TEST window:  {test_dates[0]} → {test_dates[-1]}")
    L.append(f"- Bucket size:   {BUCKET_SECONDS} sec")
    L.append(f"- Flag rule:     markets_touched ≥ {MIN_MARKETS_TOUCHED}  AND  burst_density ≥ {MIN_FLAGGED_TRADES}  AND  notional ≥ ${MIN_NOTIONAL_USD:,.0f}")
    L.append(f"- Ship gates:    precision@100 ≥ {SHIP_PRECISION_AT_100}  AND  ROC AUC ≥ {SHIP_ROC_AUC}  AND  mean PnL/trade > 0")
    L.append("")
    L.append("## Result")
    L.append("")
    L.append(f"- TEST trades:         {test_v2.height:,}")
    L.append(f"- v2-flagged in test:  {int(test_v2['flag_v2_volume_burst'].sum() or 0):,}")
    L.append(f"- Event buckets:       {buckets.height if not buckets.is_empty() else 0:,}")
    L.append(f"- Cross-market flagged buckets: {int(buckets['cross_market_flag'].sum() or 0) if not buckets.is_empty() else 0:,}")
    L.append(f"- Cross-market flagged trades:  {pnl['n']:,}")
    L.append(f"- Total flagged notional:       ${pnl['total_pnl'] + (pnl['mean_pnl_per_trade']*pnl['n'] if False else 0):>10,.0f}  (sum of bucket notional concentration)")
    L.append("")
    L.append("## Verdict")
    L.append("")
    L.append(f"| Verdict | precision@100 | precision@1K | ROC AUC | PnL/trade | Sharpe | Flagged |")
    L.append("|:---|---:|---:|---:|---:|---:|---:|")
    p100_s = f"{p100:.3f}" if p100 is not None else "—"
    p1k_s  = f"{p1k:.3f}"  if p1k  is not None else "—"
    auc_s  = f"{auc:.3f}"  if auc  is not None else "—"
    pnl_s  = f"${pnl['mean_pnl_per_trade']:.4f}" if pnl['n'] > 0 else "—"
    sh_s   = f"{pnl['sharpe']:.2f}" if pnl['sharpe'] is not None else "—"
    L.append(f"| **{verdict}** | {p100_s} | {p1k_s} | {auc_s} | {pnl_s} | {sh_s} | {pnl['n']:,} |")
    L.append("")
    if ship_reasons:
        L.append("**Reasons failed to ship:**")
        for r in ship_reasons:
            L.append(f"  - {r}")
        L.append("")
    if not top_events.is_empty():
        L.append("## Top 15 flagged event-buckets by score")
        L.append("")
        L.append("| Score | Markets | Burst density | Notional | Event ticker | Bucket time UTC |")
        L.append("|---:|---:|---:|---:|:---|:---|")
        for r in top_events.iter_rows(named=True):
            ts = datetime.fromtimestamp(r['bucket_unix'], tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
            L.append(f"| {r['score_cross_market']:.2f} | {r['markets_touched']} | {r['burst_density']} | ${r['notional_concentration']:>9,.0f} | `{r['event_ticker'][:50]}` | {ts} |")
        L.append("")
    L.append("---")
    L.append("")
    L.append("**Methodology.** TRAIN calibration of v2 volume_burst → flagged trades in TEST →")
    L.append(f"aggregate by (event_ticker × {BUCKET_SECONDS}s bucket). A bucket is `cross_market_flag`")
    L.append(f"when markets_touched ≥ {MIN_MARKETS_TOUCHED}, burst_density ≥ {MIN_FLAGGED_TRADES},")
    L.append(f"notional ≥ ${MIN_NOTIONAL_USD:,.0f}. Bucket flag attaches to each constituent trade; PnL = ")
    L.append("taker_pnl_per_contract × count summed over flagged trades.")
    report.write_text("\n".join(L), encoding="utf-8")

    audit = {
        "run_id": run_id,
        "version": VERSION,
        "started_at_utc": datetime.fromtimestamp(started, tz=timezone.utc).isoformat(),
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "duration_seconds": round(time.time() - started, 3),
        "train_dates": train_dates,
        "test_dates": test_dates,
        "bucket_seconds": BUCKET_SECONDS,
        "min_markets_touched": MIN_MARKETS_TOUCHED,
        "min_flagged_trades": MIN_FLAGGED_TRADES,
        "min_notional_usd": MIN_NOTIONAL_USD,
        "buckets_total": buckets.height if not buckets.is_empty() else 0,
        "buckets_flagged": int(buckets['cross_market_flag'].sum() or 0) if not buckets.is_empty() else 0,
        "test_trades": test_v2.height,
        "test_v2_flagged": int(test_v2['flag_v2_volume_burst'].sum() or 0),
        "cross_market_flagged_trades": pnl['n'],
        "precision_at_100": p100,
        "precision_at_1k":  p1k,
        "precision_at_10k": p10k,
        "roc_auc": auc,
        "pnl": pnl,
        "verdict": verdict,
        "ship_reasons_failed": ship_reasons,
        "report_path": str(report.relative_to(PROJECT_ROOT)),
    }
    _write_audit(run_id, audit)
    _log("=" * 72)
    _log(f"  verdict: {verdict}")
    _log(f"  precision@100={p100}  AUC={auc}  PnL/trade=${pnl['mean_pnl_per_trade']:.4f}  Sharpe={pnl['sharpe']}")
    _log(f"  report -> {report.relative_to(PROJECT_ROOT)}")
    _log(f"DONE  duration={time.time()-started:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
