#!/usr/bin/env python3
# ruff: noqa: E402
"""
Generate a "whale profile" narrative report from enriched_trades.

What it produces:
  surveillance/reports/whale_profiles_<run_id>.md
  surveillance/reports/whale_profiles_<run_id>.json

Sections:
  1. Universe stats: total trades, resolved, whales, PnL trajectory
  2. Top 25 single trades (by notional) with outcomes + edge
  3. Top 25 single trades by REALIZED PnL (winners) and LOSERS
  4. Top 20 markets by flagged-trade concentration
  5. Top 20 events by cross-market activity
  6. Series prefix breakdown (where smart money concentrates)
  7. Category breakdown (Sports / Esports / mentions / etc.) with hit rates
  8. Edge × Outcome calibration table (visual proof v2 is calibrated)
  9. Whale "signature buckets" — patterns of trade size × time-of-day ×
     price-range, ranked by hit rate (the closest thing to a "fingerprint
     account" we can build from anonymous data)

Reads the enriched_trades parquets (after all 4 stages have run).

Usage
-----
  uv run python scripts/whale_profile_report.py
  uv run python scripts/whale_profile_report.py --days 30
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
ENRICHED_DIR = DERIVED / "enriched_trades"
REPORTS_DIR = PROJECT_ROOT / "surveillance" / "reports"

VERSION = "whale_profile_report@1.0.0"

PRICE_BIN_WIDTH = 0.05
V2_EDGE_THRESHOLD = 0.07


def _log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"{ts}  {msg}", flush=True)


def _list_dates(args) -> list[str]:
    if args.dt:
        return [args.dt]
    parts = sorted({Path(p).name for p in glob.glob(str(ENRICHED_DIR / "dt=*"))})
    parts = [p.replace("dt=", "") for p in parts if p.startswith("dt=")]
    if args.all:
        return parts
    if not parts:
        return []
    last = parts[-1]
    cutoff = (datetime.fromisoformat(last) - timedelta(days=args.days)).date().isoformat()
    return [p for p in parts if p >= cutoff]


def _apply_v2(df: pl.DataFrame) -> pl.DataFrame:
    df = df.with_columns([
        (pl.col("volume_z_5min").fill_null(0.0) >= 3.0).alias("flag_volume_burst"),
        ((pl.col("yes_price_dollars") / PRICE_BIN_WIDTH).floor().cast(pl.Int32) * PRICE_BIN_WIDTH).alias("yes_price_bin"),
        pl.when(pl.col("taker_side") == "yes").then(pl.col("yes_price_dollars"))
                                              .otherwise(pl.col("no_price_dollars"))
                                              .alias("taker_break_even_price"),
    ])
    base = df.filter(pl.col("flag_volume_burst") & pl.col("was_taker_correct").is_not_null())
    cal = (base.group_by(["yes_price_bin", "taker_side"])
                .agg([pl.col("was_taker_correct").mean().alias("pwin_hat"),
                      pl.len().alias("n_train")]))
    gpwin = float(base["was_taker_correct"].mean() or 0.0)
    df = df.join(cal, on=["yes_price_bin", "taker_side"], how="left").with_columns(
        pl.when(pl.col("n_train").fill_null(0) >= 20)
          .then(pl.col("pwin_hat"))
          .otherwise(pl.lit(gpwin))
          .alias("pwin_hat_used")
    )
    df = df.with_columns([
        (pl.col("pwin_hat_used") - pl.col("taker_break_even_price")).alias("edge_volume_burst"),
        (pl.col("taker_pnl_per_contract_dollars") * pl.col("count")).alias("realized_pnl_usd"),
    ])
    df = df.with_columns(
        (pl.col("flag_volume_burst") & (pl.col("edge_volume_burst") > V2_EDGE_THRESHOLD))
            .alias("flag_v2_volume_burst")
    )
    return df


def _fmt_pnl(x: float | None) -> str:
    if x is None:
        return "—"
    return f"${x:+,.0f}" if abs(x) >= 100 else f"${x:+.2f}"


def main() -> int:
    ap = argparse.ArgumentParser(description="Whale profile narrative report")
    ap.add_argument("--dt",   type=str, default=None)
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--all",  action="store_true")
    args = ap.parse_args()

    run_id = f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"
    started = time.time()
    dates = _list_dates(args)
    if not dates:
        _log("no enriched partitions; exiting")
        return 1

    _log("=" * 72)
    _log(f"whale_profile_report run_id={run_id}")
    _log(f"  window = {dates[0]} → {dates[-1]}  ({len(dates)} days)")
    _log("=" * 72)

    files = []
    for dt in dates:
        files.extend(sorted(glob.glob(str(ENRICHED_DIR / f"dt={dt}/*.parquet"))))
    files = [f for f in files if not f.endswith(".tmp")]
    _log(f"loading {len(files)} parquets…")
    df = pl.concat([pl.read_parquet(f) for f in files], how="diagonal_relaxed")
    _log(f"loaded {df.height:,} trades in {time.time()-started:.1f}s")

    df = _apply_v2(df)

    res = df.filter(pl.col("was_taker_correct").is_not_null())
    whales = df.filter(pl.col("is_whale").fill_null(False))
    whales_res = whales.filter(pl.col("was_taker_correct").is_not_null())
    flagged = df.filter(pl.col("flag_v2_volume_burst"))
    flagged_res = flagged.filter(pl.col("was_taker_correct").is_not_null())

    findings = {
        "run_id": run_id,
        "window_start": dates[0],
        "window_end": dates[-1],
        "trades_total": df.height,
        "trades_resolved": res.height,
        "trades_whale": whales.height,
        "trades_v2_flagged": flagged.height,
    }

    # Top single trades by notional
    top_notional = (df.sort("notional_usd", descending=True)
                       .select(["created_time", "market_ticker", "event_ticker",
                                "milestone_category", "taker_side", "yes_price_dollars",
                                "count", "notional_usd", "edge_volume_burst",
                                "was_taker_correct", "realized_pnl_usd"])
                       .head(25))

    # Top winners by realized PnL
    top_winners = (res.sort("realized_pnl_usd", descending=True)
                       .select(["created_time", "market_ticker", "event_ticker",
                                "milestone_category", "taker_side", "yes_price_dollars",
                                "count", "notional_usd", "realized_pnl_usd",
                                "edge_volume_burst", "flag_v2_volume_burst"])
                       .head(25))

    top_losers = (res.sort("realized_pnl_usd", descending=False)
                       .select(["created_time", "market_ticker", "event_ticker",
                                "milestone_category", "taker_side", "yes_price_dollars",
                                "count", "notional_usd", "realized_pnl_usd",
                                "edge_volume_burst", "flag_v2_volume_burst"])
                       .head(25))

    # Top markets by flagged concentration
    top_markets = (flagged.group_by("market_ticker")
                          .agg([
                              pl.len().alias("flagged_trades"),
                              pl.col("notional_usd").sum().alias("flagged_notional"),
                              pl.col("realized_pnl_usd").sum().alias("paper_pnl"),
                              pl.col("was_taker_correct").mean().alias("hit_rate"),
                              pl.col("event_ticker").first().alias("event_ticker"),
                          ])
                          .sort("flagged_notional", descending=True)
                          .head(20))

    # Series-prefix breakdown (split at first '-')
    flagged_with_series = flagged.with_columns(
        pl.col("market_ticker").str.split("-").list.get(0).alias("series")
    )
    series_break = (flagged_with_series.group_by("series")
                     .agg([
                         pl.len().alias("trades"),
                         pl.col("notional_usd").sum().alias("notional"),
                         pl.col("realized_pnl_usd").sum().alias("paper_pnl"),
                         pl.col("was_taker_correct").mean().alias("hit_rate"),
                     ])
                     .sort("paper_pnl", descending=True)
                     .head(15))

    # Category breakdown (using milestone_category where available)
    if "milestone_category" in flagged.columns:
        cat_break = (flagged.filter(pl.col("milestone_category").is_not_null())
                            .group_by("milestone_category")
                            .agg([
                                pl.len().alias("trades"),
                                pl.col("notional_usd").sum().alias("notional"),
                                pl.col("realized_pnl_usd").sum().alias("paper_pnl"),
                                pl.col("was_taker_correct").mean().alias("hit_rate"),
                            ])
                            .sort("paper_pnl", descending=True))
    else:
        cat_break = pl.DataFrame()

    # Calibration table — edge buckets × hit rate
    calib = (flagged_res
             .with_columns((pl.col("edge_volume_burst") * 20).floor() / 20)
             .rename({"edge_volume_burst": "edge_bucket"})
             .group_by("edge_bucket")
             .agg([
                 pl.len().alias("n"),
                 pl.col("was_taker_correct").mean().alias("hit_rate"),
                 pl.col("realized_pnl_usd").mean().alias("mean_pnl"),
                 pl.col("realized_pnl_usd").sum().alias("total_pnl"),
             ])
             .sort("edge_bucket"))

    # Whale "signature buckets" — group by (notional decile × hour bucket × yes_price bucket × taker_side)
    sig = whales_res.with_columns([
        pl.col("notional_usd").qcut([0.2, 0.4, 0.6, 0.8], labels=["XS","S","M","L","XL"]).alias("size_bucket"),
        ((pl.col("yes_price_dollars") * 10).floor() / 10).alias("price_bucket"),
        (pl.col("hour_of_day_utc") // 6).alias("tod_bucket"),  # 4 ToD windows
    ])
    sig_groups = (sig.group_by(["size_bucket", "price_bucket", "tod_bucket", "taker_side"])
                     .agg([
                         pl.len().alias("n"),
                         pl.col("was_taker_correct").mean().alias("hit_rate"),
                         pl.col("realized_pnl_usd").mean().alias("mean_pnl"),
                         pl.col("realized_pnl_usd").sum().alias("total_pnl"),
                     ])
                     .filter(pl.col("n") >= 100)
                     .sort("total_pnl", descending=True)
                     .head(15))

    # Compile findings
    findings.update({
        "v2_total_flagged_pnl": float(flagged_res["realized_pnl_usd"].sum() or 0.0) if flagged_res.height else 0.0,
        "v2_mean_pnl_per_trade": float(flagged_res["realized_pnl_usd"].mean() or 0.0) if flagged_res.height else 0.0,
        "v2_hit_rate": float(flagged_res["was_taker_correct"].mean() or 0.0) if flagged_res.height else 0.0,
        "whale_baseline_pnl": float(whales_res["realized_pnl_usd"].sum() or 0.0) if whales_res.height else 0.0,
        "whale_hit_rate": float(whales_res["was_taker_correct"].mean() or 0.0) if whales_res.height else 0.0,
        "trades_baseline_pnl": float(res["realized_pnl_usd"].sum() or 0.0) if res.height else 0.0,
        "trades_baseline_hit_rate": float(res["was_taker_correct"].mean() or 0.0) if res.height else 0.0,
    })

    # -------------------- write report --------------------
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    md_path = REPORTS_DIR / f"whale_profiles_{run_id}.md"
    L: list[str] = []
    L.append(f"# Whale & informed-flow profile report — {dates[0]} to {dates[-1]}")
    L.append("")
    L.append(f"_Generated by `{VERSION}` on {datetime.now(timezone.utc).isoformat()}._")
    L.append("")
    L.append("## Universe stats")
    L.append("")
    L.append(f"- Days covered: **{len(dates)}**  ({dates[0]} → {dates[-1]})")
    L.append(f"- Total trades: **{findings['trades_total']:,}**")
    L.append(f"- Resolved trades: **{findings['trades_resolved']:,}**  ({100*findings['trades_resolved']/max(findings['trades_total'],1):.1f}%)")
    L.append(f"- Whales (top 1% global × top 5% in-market): **{findings['trades_whale']:,}**")
    L.append(f"- v2_volume_burst flagged: **{findings['trades_v2_flagged']:,}**")
    L.append("")
    L.append("### Paper-PnL trajectory")
    L.append("")
    L.append("| Population | Trades resolved | Hit rate | Total realized PnL |")
    L.append("|:---|---:|---:|---:|")
    L.append(f"| All takers (baseline)      | {findings['trades_resolved']:,} | {100*findings['trades_baseline_hit_rate']:.2f}% | {_fmt_pnl(findings['trades_baseline_pnl'])} |")
    L.append(f"| Whales only                | {whales_res.height:,} | {100*findings['whale_hit_rate']:.2f}% | {_fmt_pnl(findings['whale_baseline_pnl'])} |")
    L.append(f"| **v2_volume_burst flagged**| **{flagged_res.height:,}** | **{100*findings['v2_hit_rate']:.2f}%** | **{_fmt_pnl(findings['v2_total_flagged_pnl'])}** |")
    L.append("")
    L.append(f"_Mean v2 PnL/trade: **{_fmt_pnl(findings['v2_mean_pnl_per_trade'])}** per flagged trade._")
    L.append("")

    L.append("## Top 25 single trades by notional")
    L.append("")
    L.append("| Time UTC | Market | Side | Yes price | Count | Notional | Edge | Won? | Realized PnL |")
    L.append("|:---|:---|:---:|---:|---:|---:|---:|:---:|---:|")
    for r in top_notional.iter_rows(named=True):
        ts = (r['created_time'] or '')[:19] if isinstance(r['created_time'], str) else str(r['created_time'])[:19]
        won = "✓" if r.get("was_taker_correct") is True else ("✗" if r.get("was_taker_correct") is False else "—")
        edge = r.get("edge_volume_burst")
        edge_s = f"{edge:+.3f}" if edge is not None else "—"
        L.append(f"| {ts} | `{(r.get('market_ticker') or '')[:45]}` | {r['taker_side']} | ${r['yes_price_dollars']:.3f} | {r['count']:,} | ${r['notional_usd']:>10,.0f} | {edge_s} | {won} | {_fmt_pnl(r.get('realized_pnl_usd'))} |")
    L.append("")

    L.append("## Top 25 biggest winners (single trades)")
    L.append("")
    L.append("| Time UTC | Market | Side | Yes price | Count | PnL | v2-flagged? | Edge |")
    L.append("|:---|:---|:---:|---:|---:|---:|:---:|---:|")
    for r in top_winners.iter_rows(named=True):
        ts = (r['created_time'] or '')[:19] if isinstance(r['created_time'], str) else str(r['created_time'])[:19]
        flagged_s = "✓" if r.get("flag_v2_volume_burst") else " "
        edge = r.get("edge_volume_burst")
        edge_s = f"{edge:+.3f}" if edge is not None else "—"
        L.append(f"| {ts} | `{(r.get('market_ticker') or '')[:45]}` | {r['taker_side']} | ${r['yes_price_dollars']:.3f} | {r['count']:,} | {_fmt_pnl(r['realized_pnl_usd'])} | {flagged_s} | {edge_s} |")
    L.append("")

    L.append("## Top 25 biggest losers (single trades)")
    L.append("")
    L.append("| Time UTC | Market | Side | Yes price | Count | PnL | v2-flagged? | Edge |")
    L.append("|:---|:---|:---:|---:|---:|---:|:---:|---:|")
    for r in top_losers.iter_rows(named=True):
        ts = (r['created_time'] or '')[:19] if isinstance(r['created_time'], str) else str(r['created_time'])[:19]
        flagged_s = "✓" if r.get("flag_v2_volume_burst") else " "
        edge = r.get("edge_volume_burst")
        edge_s = f"{edge:+.3f}" if edge is not None else "—"
        L.append(f"| {ts} | `{(r.get('market_ticker') or '')[:45]}` | {r['taker_side']} | ${r['yes_price_dollars']:.3f} | {r['count']:,} | {_fmt_pnl(r['realized_pnl_usd'])} | {flagged_s} | {edge_s} |")
    L.append("")

    L.append("## Top 20 markets by flagged-trade concentration")
    L.append("")
    L.append("| Market | Flagged trades | Flagged notional | Paper PnL | Hit rate |")
    L.append("|:---|---:|---:|---:|---:|")
    for r in top_markets.iter_rows(named=True):
        hr = r.get("hit_rate")
        hr_s = f"{100*hr:.1f}%" if hr is not None else "—"
        L.append(f"| `{r['market_ticker'][:60]}` | {r['flagged_trades']:,} | ${r['flagged_notional']:>10,.0f} | {_fmt_pnl(r['paper_pnl'])} | {hr_s} |")
    L.append("")

    L.append("## Top 15 series prefixes by paper PnL")
    L.append("")
    L.append("| Series | Trades | Notional | Paper PnL | Hit rate |")
    L.append("|:---|---:|---:|---:|---:|")
    for r in series_break.iter_rows(named=True):
        hr = r.get("hit_rate")
        hr_s = f"{100*hr:.1f}%" if hr is not None else "—"
        L.append(f"| `{r['series']}` | {r['trades']:,} | ${r['notional']:>10,.0f} | {_fmt_pnl(r['paper_pnl'])} | {hr_s} |")
    L.append("")

    if not cat_break.is_empty():
        L.append("## Milestone-category breakdown")
        L.append("")
        L.append("| Category | Trades | Notional | Paper PnL | Hit rate |")
        L.append("|:---|---:|---:|---:|---:|")
        for r in cat_break.iter_rows(named=True):
            hr = r.get("hit_rate")
            hr_s = f"{100*hr:.1f}%" if hr is not None else "—"
            L.append(f"| {r['milestone_category']} | {r['trades']:,} | ${r['notional']:>10,.0f} | {_fmt_pnl(r['paper_pnl'])} | {hr_s} |")
        L.append("")

    L.append("## v2 edge × hit-rate calibration (proof of monotonicity)")
    L.append("")
    L.append("| Edge bucket | n trades | Hit rate | Mean PnL | Total PnL |")
    L.append("|---:|---:|---:|---:|---:|")
    for r in calib.iter_rows(named=True):
        L.append(f"| {r['edge_bucket']:+.2f} | {r['n']:,} | {100*r['hit_rate']:.1f}% | {_fmt_pnl(r['mean_pnl'])} | {_fmt_pnl(r['total_pnl'])} |")
    L.append("")

    if not sig_groups.is_empty():
        L.append("## Whale signature buckets (top 15 by total PnL)")
        L.append("")
        L.append("_Closest thing to a 'fingerprint account' we can build from anonymous data: pattern_")
        L.append("_of (size × time-of-day × yes_price × side) ranked by realized PnL on resolved trades._")
        L.append("")
        L.append("| Size | Yes price | ToD (UTC) | Side | n trades | Hit rate | Mean PnL | Total PnL |")
        L.append("|:---:|---:|:---:|:---:|---:|---:|---:|---:|")
        for r in sig_groups.iter_rows(named=True):
            tod_label = {0: "0-6", 1: "6-12", 2: "12-18", 3: "18-24"}.get(r['tod_bucket'], "?")
            L.append(f"| {r['size_bucket']} | ${r['price_bucket']:.2f} | {tod_label} | {r['taker_side']} | {r['n']:,} | {100*r['hit_rate']:.1f}% | {_fmt_pnl(r['mean_pnl'])} | {_fmt_pnl(r['total_pnl'])} |")
        L.append("")

    L.append("---")
    L.append("")
    L.append("**Methodology.** v2 calibration fit on the window above (yes_price × taker_side bins).")
    L.append(f"Edge = pwin_hat − taker_break_even_price. Flag = edge > {V2_EDGE_THRESHOLD:.2f}.")
    L.append("Realized PnL = `taker_pnl_per_contract_dollars × count` on resolved trades. No per-account")
    L.append("identity is exposed by Kalshi public data; signature buckets approximate behavioral clusters.")
    md_path.write_text("\n".join(L), encoding="utf-8")

    json_path = REPORTS_DIR / f"whale_profiles_{run_id}.json"
    json_path.write_text(json.dumps(findings, indent=2, default=str), encoding="utf-8")

    _log(f"report -> {md_path.relative_to(PROJECT_ROOT)}")
    _log(f"json   -> {json_path.relative_to(PROJECT_ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
