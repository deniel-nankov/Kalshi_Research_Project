#!/usr/bin/env python3
# ruff: noqa: E402
"""
Phase 4 — Whale & Volume-Burst Detection (MVP).

Reads enriched_trades for a given dt (or rolling window) and produces three
artifacts:

  1. Per-trade flag table at
     data/kalshi/derived/whale_flags/dt=YYYY-MM-DD/<run_id>.parquet
     One row per flagged trade with detector scores.

  2. Daily smart-money board at
     surveillance/reports/smart_money_<dt>.md
     Human-readable Markdown summary: top whale trades, per-market
     concentration, win-rate stats, top categories.

  3. Audit log at state/detector_runs/<run_id>.json.

Detectors implemented at this stage:
  Detector 1: WHALE
    - score = size_pct_global_today * size_pct_in_market_today
    - threshold: is_whale==true (i.e. size_pct_global_today >= 0.99)
    - Strength rationale: a trade in the top 1% globally AND high-percentile
      in its own market is far more likely a single informed actor than two
      coincident large trades.

  Detector 4: VOLUME_BURST
    - score = clip(volume_z_5min / 5.0, 0, 1)
    - threshold: volume_z_5min > 3.0 (3-sigma 5-min window)
    - Catches the "news arrival" pattern: rapid concentrated activity in a
      single market within a 5-minute window.

  Composite score:
    - max of the two scores (a trade can be both a whale AND in a burst).

The full plan (DESIGN.md) calls for 4 more detectors (pre-play alpha,
forecast deviation, spoofing, cross-market). Those are layered in once
the milestones/pbp/lifecycle enrichment stages land — they're the
higher-precision detectors but require enriched joins. This MVP gives
us a working surveillance pipeline today.

Usage
-----
  uv run python scripts/detect_whales.py --dt 2026-05-15
  uv run python scripts/detect_whales.py --days 7     # rolling window
  uv run python scripts/detect_whales.py --all
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
OUT_DIR = DERIVED / "whale_flags"
REPORTS_DIR = PROJECT_ROOT / "surveillance" / "reports"
AUDIT_DIR = STATE / "detector_runs"

DETECTOR_VERSION = "detect_whales@1.0.0"


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
    out = AUDIT_DIR / f"detect_whales_{run_id}.json"
    tmp = out.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, out)


# -------------------- detectors --------------------

def _apply_detectors(df: pl.DataFrame) -> pl.DataFrame:
    """Add per-trade detector scores and a composite score."""
    df = df.with_columns([
        # Detector 1: WHALE
        (pl.col("size_pct_global_today").fill_null(0.0)
         * pl.col("size_pct_in_market_today").fill_null(0.0)
         ).alias("score_whale"),
        # Detector 4: VOLUME_BURST (normalized to 0-1 via /5)
        pl.when(pl.col("volume_z_5min").is_null())
          .then(0.0)
          .otherwise(
              pl.min_horizontal([pl.max_horizontal([pl.col("volume_z_5min") / 5.0, pl.lit(0.0)]), pl.lit(1.0)])
          )
          .alias("score_volume_burst"),
    ])
    df = df.with_columns(
        pl.max_horizontal([pl.col("score_whale"), pl.col("score_volume_burst")]).alias("score_composite")
    )
    # Strict whale: must be top 1% globally AND top 5% in its own market.
    # This rules out "the biggest trade in a tiny illiquid market" being
    # called a whale, and rules out "globally large but routine for this
    # market" trades (e.g. a market that always has $20K trades).
    df = df.with_columns([
        ((pl.col("size_pct_global_today") >= 0.99) &
         (pl.col("size_pct_in_market_today") >= 0.95)).alias("flag_whale"),
        (pl.col("score_volume_burst") >= 0.6).alias("flag_volume_burst"),  # z >= 3
    ])
    df = df.with_columns(
        (pl.col("flag_whale") | pl.col("flag_volume_burst")).alias("flagged")
    )
    return df


# -------------------- per-dt run --------------------

def _process_dt(dt: str, run_id: str) -> dict:
    in_files = sorted(glob.glob(str(ENRICHED_DIR / f"dt={dt}/*.parquet")))
    in_files = [f for f in in_files if not f.endswith(".tmp")]
    if not in_files:
        _log(f"  dt={dt}  no enriched parquets; run enrich_trades.py first")
        return {"dt": dt, "error": "no_enriched_input"}

    df = pl.read_parquet(in_files)
    df = _apply_detectors(df)
    flagged = df.filter(pl.col("flagged"))

    out_dir = OUT_DIR / f"dt={dt}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{run_id}_{dt.replace('-', '')}.parquet"
    tmp = out_file.with_suffix(".parquet.tmp")
    # Keep the full flagged trade record + scores
    flagged.write_parquet(str(tmp), compression="zstd")
    tmp.replace(out_file)
    _write_sha256_sidecar(out_file)

    # Per-market aggregation
    per_market = (
        flagged.group_by("market_ticker")
               .agg([
                   pl.len().alias("flagged_trades"),
                   pl.col("notional_usd").sum().alias("flagged_notional_usd"),
                   pl.col("flag_whale").sum().alias("whale_trades"),
                   pl.col("flag_volume_burst").sum().alias("burst_trades"),
                   pl.col("was_taker_correct").mean().alias("flagged_taker_win_rate"),
                   pl.col("event_ticker").first().alias("event_ticker"),
               ])
               .sort("flagged_notional_usd", descending=True)
    )

    res = {
        "dt": dt,
        "trades_total": df.height,
        "trades_flagged": flagged.height,
        "whales": int(df["flag_whale"].sum() or 0),
        "bursts": int(df["flag_volume_burst"].sum() or 0),
        "flagged_notional_usd": float(flagged["notional_usd"].sum() or 0.0),
        "out_file": str(out_file.relative_to(PROJECT_ROOT)),
    }
    # Win-rate stats
    res_with_outcome = flagged.filter(pl.col("was_taker_correct").is_not_null())
    if res_with_outcome.height > 0:
        res["flagged_win_rate"] = float(res_with_outcome["was_taker_correct"].mean() or 0.0)
        res["flagged_resolved"] = res_with_outcome.height
    whales_with_outcome = flagged.filter(pl.col("flag_whale") & pl.col("was_taker_correct").is_not_null())
    if whales_with_outcome.height > 0:
        res["whale_win_rate"] = float(whales_with_outcome["was_taker_correct"].mean() or 0.0)
    bursts_with_outcome = flagged.filter(pl.col("flag_volume_burst") & pl.col("was_taker_correct").is_not_null())
    if bursts_with_outcome.height > 0:
        res["burst_win_rate"] = float(bursts_with_outcome["was_taker_correct"].mean() or 0.0)
    # Baseline overall win rate
    base = df.filter(pl.col("was_taker_correct").is_not_null())
    if base.height > 0:
        res["baseline_win_rate"] = float(base["was_taker_correct"].mean() or 0.0)

    # Write the daily report
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    report_path = REPORTS_DIR / f"smart_money_{dt}.md"
    _write_report(report_path, dt, df, flagged, per_market, res)
    res["report_path"] = str(report_path.relative_to(PROJECT_ROOT))

    _log(f"  dt={dt}  trades={df.height:,}  flagged={flagged.height:,}  whales={res['whales']:,}  bursts={res['bursts']:,}  -> {out_file.name}")
    return res


# -------------------- report writer --------------------

def _write_report(path: Path, dt: str, df: pl.DataFrame, flagged: pl.DataFrame, per_market: pl.DataFrame, res: dict) -> None:
    lines = []
    lines.append(f"# Smart-money board — {dt}")
    lines.append("")
    lines.append(f"_Generated by `{DETECTOR_VERSION}` on {datetime.now(timezone.utc).isoformat()}._")
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append(f"- Total trades: **{df.height:,}**")
    lines.append(f"- Flagged trades: **{flagged.height:,}** ({100*flagged.height/max(df.height,1):.2f}%)")
    lines.append(f"  - Whales: {res['whales']:,}")
    lines.append(f"  - Volume bursts: {res['bursts']:,}")
    lines.append(f"- Flagged notional: **${res['flagged_notional_usd']:,.0f}**")
    if "baseline_win_rate" in res:
        lines.append(f"- Baseline taker win rate: {100*res['baseline_win_rate']:.2f}%")
    if "whale_win_rate" in res:
        edge = res['whale_win_rate'] - res.get('baseline_win_rate', 0.0)
        lines.append(f"- **Whale win rate: {100*res['whale_win_rate']:.2f}%**  ({edge*100:+.2f} pp vs baseline)")
    if "burst_win_rate" in res:
        edge = res['burst_win_rate'] - res.get('baseline_win_rate', 0.0)
        lines.append(f"- Burst win rate: {100*res['burst_win_rate']:.2f}%  ({edge*100:+.2f} pp vs baseline)")
    lines.append("")

    lines.append("## Top 20 whale trades by notional")
    lines.append("")
    lines.append("| Notional | Market | Taker | Vol-z 5min | Resolved? | Correct? |")
    lines.append("|---:|:---|:---:|---:|:---:|:---:|")
    top = (flagged.filter(pl.col("flag_whale"))
                   .sort("notional_usd", descending=True)
                   .head(20))
    for r in top.iter_rows(named=True):
        won = r.get("was_taker_correct")
        won_str = "✓" if won is True else ("✗" if won is False else "—")
        resolved_str = "yes" if won is not None else "no"
        z = r.get("volume_z_5min")
        z_str = f"{z:.1f}" if z is not None else "—"
        lines.append(f"| ${r['notional_usd']:>10,.0f} | `{r['market_ticker'][:50]}` | {r['taker_side']} | {z_str} | {resolved_str} | {won_str} |")
    lines.append("")

    lines.append("## Top 20 markets by flagged notional")
    lines.append("")
    lines.append("| Notional | Trades | Whales | Bursts | Flagged win-rate | Market |")
    lines.append("|---:|---:|---:|---:|---:|:---|")
    for r in per_market.head(20).iter_rows(named=True):
        wr = r.get("flagged_taker_win_rate")
        wr_str = f"{100*wr:.1f}%" if wr is not None else "—"
        lines.append(f"| ${r['flagged_notional_usd']:>10,.0f} | {r['flagged_trades']:>4} | {r['whale_trades']:>3} | {r['burst_trades']:>3} | {wr_str} | `{r['market_ticker'][:50]}` |")
    lines.append("")

    # Whale concentration in categories — bucket by ticker prefix (best-effort)
    flagged_with_prefix = flagged.with_columns(
        pl.col("market_ticker").str.split("-").list.get(0).alias("_prefix")
    )
    cat = (flagged_with_prefix.group_by("_prefix")
                              .agg([
                                  pl.len().alias("trades"),
                                  pl.col("notional_usd").sum().alias("notional"),
                              ])
                              .sort("notional", descending=True)
                              .head(15))
    lines.append("## Top 15 series prefixes by flagged notional")
    lines.append("")
    lines.append("| Notional | Trades | Series prefix |")
    lines.append("|---:|---:|:---|")
    for r in cat.iter_rows(named=True):
        lines.append(f"| ${r['notional']:>10,.0f} | {r['trades']:>5} | `{r['_prefix']}` |")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("**Disclaimer.** Anonymous-trade detection — no per-account identity is")
    lines.append("available from Kalshi public data. Scores are behavioral signals. See")
    lines.append("`surveillance/DESIGN.md` for methodology + ship criteria.")
    path.write_text("\n".join(lines), encoding="utf-8")


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
    ap = argparse.ArgumentParser(description="Detect whales + volume bursts on enriched_trades")
    ap.add_argument("--dt",   type=str, default=None)
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--all",  action="store_true")
    args = ap.parse_args()

    run_id = f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"
    started = time.time()
    dates = _list_dates(args)

    _log("=" * 72)
    _log(f"detect_whales run_id={run_id}")
    _log(f"  version    = {DETECTOR_VERSION}")
    _log(f"  partitions = {len(dates)}  range=[{dates[0] if dates else '-'}, {dates[-1] if dates else '-'}]")
    _log("=" * 72)

    if not dates:
        _log("no enriched_trades partitions found; exiting")
        return 0

    per_dt: list[dict] = []
    for i, dt in enumerate(dates, 1):
        _log(f"[{i}/{len(dates)}] dt={dt}")
        try:
            per_dt.append(_process_dt(dt, run_id))
        except Exception as exc:
            _log(f"  dt={dt}  ERROR  {exc!r}")
            per_dt.append({"dt": dt, "error": repr(exc)})

    audit = {
        "run_id": run_id,
        "started_at_utc": datetime.fromtimestamp(started, tz=timezone.utc).isoformat(),
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "duration_seconds": round(time.time() - started, 3),
        "version": DETECTOR_VERSION,
        "args_dt": args.dt,
        "args_days": args.days,
        "args_all": args.all,
        "per_dt": per_dt,
    }
    _write_audit(run_id, audit)
    _log("=" * 72)
    _log(f"DONE  partitions={len(per_dt)}  duration={time.time()-started:.1f}s")
    _log(f"reports -> surveillance/reports/smart_money_*.md")
    return 0


if __name__ == "__main__":
    sys.exit(main())
