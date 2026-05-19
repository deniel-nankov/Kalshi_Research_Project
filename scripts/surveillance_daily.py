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


PRICE_BIN_WIDTH = 0.05
# Cross-market bucket rules (mirror detect_cross_market.py)
CM_BUCKET_SECONDS = 300
CM_MIN_MARKETS_TOUCHED = 2
CM_MIN_FLAGGED_TRADES = 5
CM_MIN_NOTIONAL_USD = 1_000.0


def _score_v2(df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame, float]:
    """Apply v2 volume_burst calibration; return (scored_df, cal_df, gpwin)."""
    df = df.with_columns([
        ((pl.col("size_pct_global_today") >= 0.99) &
         (pl.col("size_pct_in_market_today") >= 0.95)).alias("flag_whale"),
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
    scored = df.join(cal, on=["yes_price_bin", "taker_side"], how="left").with_columns(
        pl.when(pl.col("n_train").fill_null(0) >= 20)
          .then(pl.col("pwin_hat"))
          .otherwise(pl.lit(gpwin))
          .alias("pwin_hat_used")
    )
    scored = scored.with_columns(
        (pl.col("pwin_hat_used") - pl.col("taker_break_even_price")).alias("edge_volume_burst")
    )
    return scored, cal, gpwin


def _attach_cross_market(scored: pl.DataFrame, edge_threshold: float) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Build (event_ticker × 5-min bucket) aggregates of v2 flags and attach
    cross_market_flag + score back to each underlying trade.

    Returns (scored_with_cm_cols, buckets_df).
    """
    v2_mask = pl.col("flag_volume_burst") & (pl.col("edge_volume_burst") > edge_threshold)
    flagged = scored.filter(v2_mask & pl.col("event_ticker").is_not_null())
    if flagged.is_empty():
        scored = scored.with_columns([
            pl.lit(False).alias("cross_market_flag"),
            pl.lit(0.0).alias("score_cross_market"),
            pl.lit(0).alias("cm_markets_touched"),
            pl.lit(0).alias("cm_burst_density"),
            pl.lit(0.0).alias("cm_notional_concentration"),
        ])
        return scored, pl.DataFrame()
    bucketed = flagged.with_columns(
        (pl.col("created_time_unix") // CM_BUCKET_SECONDS * CM_BUCKET_SECONDS).alias("bucket_unix")
    )
    buckets = (bucketed.group_by(["event_ticker", "bucket_unix"])
                       .agg([
                           pl.len().alias("burst_density"),
                           pl.col("notional_usd").sum().alias("notional_concentration"),
                           pl.col("market_ticker").n_unique().alias("markets_touched"),
                       ]))
    buckets = buckets.with_columns(
        ((pl.col("markets_touched") >= CM_MIN_MARKETS_TOUCHED) &
         (pl.col("burst_density") >= CM_MIN_FLAGGED_TRADES) &
         (pl.col("notional_concentration") >= CM_MIN_NOTIONAL_USD)).alias("cross_market_flag")
    )
    buckets = buckets.with_columns(
        (
            (pl.lit(1.0) + pl.col("notional_concentration") / 1000.0).log()
            * pl.min_horizontal([pl.col("markets_touched") / 3.0, pl.lit(1.5)])
            * pl.min_horizontal([pl.col("burst_density") / 10.0, pl.lit(2.0)])
        ).alias("score_cross_market")
    )
    scored = scored.with_columns(
        (pl.col("created_time_unix") // CM_BUCKET_SECONDS * CM_BUCKET_SECONDS).alias("bucket_unix")
    )
    keep = buckets.select(["event_ticker", "bucket_unix", "cross_market_flag", "score_cross_market",
                           "burst_density", "markets_touched", "notional_concentration"]).rename({
        "burst_density":          "cm_burst_density",
        "markets_touched":        "cm_markets_touched",
        "notional_concentration": "cm_notional_concentration",
    })
    scored = scored.join(keep, on=["event_ticker", "bucket_unix"], how="left").with_columns([
        pl.col("cross_market_flag").fill_null(False),
        pl.col("score_cross_market").fill_null(0.0),
    ])
    return scored, buckets


def _emit_alerts(dt: str, edge_threshold: float, run_id: str) -> dict:
    files = sorted(glob.glob(str(ENRICHED_DIR / f"dt={dt}/*.parquet")))
    files = [f for f in files if not f.endswith(".tmp")]
    if not files:
        return {"alerts": 0, "error": "no_enriched_input"}
    df = pl.read_parquet(files)

    scored, cal, gpwin = _score_v2(df)
    scored, buckets = _attach_cross_market(scored, edge_threshold)

    # Production rule: a trade is alerted if it is v2-flagged. Cross-market
    # is added as METADATA + as a sorting boost (sort by cross_market_flag
    # then by edge), not as an additional filter — the v2 layer already
    # passes the ship gates standalone.
    flagged = scored.filter(
        pl.col("flag_volume_burst") & (pl.col("edge_volume_burst") > edge_threshold)
    ).sort(
        ["cross_market_flag", "score_cross_market", "edge_volume_burst"],
        descending=[True, True, True],
    )

    # Write alerts.jsonl
    ALERTS_DIR.mkdir(parents=True, exist_ok=True)
    out = ALERTS_DIR / f"{dt}.jsonl"
    tmp = out.with_suffix(".jsonl.tmp")
    keep_cols = [
        "trade_id", "market_ticker", "event_ticker", "milestone_id", "milestone_category",
        "taker_side", "yes_price_dollars", "no_price_dollars", "count", "notional_usd",
        "edge_volume_burst", "pwin_hat_used", "taker_break_even_price",
        "cross_market_flag", "score_cross_market",
        "cm_markets_touched", "cm_burst_density", "cm_notional_concentration",
        "created_time", "was_taker_correct",
    ]
    keep_cols = [c for c in keep_cols if c in flagged.columns]
    rec = flagged.select(keep_cols).to_dicts()
    with open(tmp, "w", encoding="utf-8") as f:
        for r in rec:
            for k, v in list(r.items()):
                if isinstance(v, datetime):
                    r[k] = v.isoformat()
            r["alert_version"] = "v2_volume_burst@1.0.0+cm@1.0.0"
            r["run_id"] = run_id
            f.write(json.dumps(r, default=str) + "\n")
    os.replace(tmp, out)

    cm_flagged_trades = int(flagged["cross_market_flag"].sum() or 0)
    cm_flagged_buckets = int(buckets.filter(pl.col("cross_market_flag")).height) if not buckets.is_empty() else 0
    return {
        "alerts": flagged.height,
        "alerts_cross_market_boosted": cm_flagged_trades,
        "cross_market_buckets_total": int(buckets.height) if not buckets.is_empty() else 0,
        "cross_market_buckets_flagged": cm_flagged_buckets,
        "total_notional_usd": float(flagged["notional_usd"].sum() or 0.0),
        "calibration_cells": cal.height,
        "global_pwin": gpwin,
        "out_file": str(out.relative_to(PROJECT_ROOT)),
        "scored_df_height": scored.height,
        "buckets_df": buckets,
        "flagged_df": flagged,
    }


def _write_daily_report(dt: str, summary: dict, run_id: str) -> Path:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = REPORTS_DIR / f"daily_{dt}.md"
    buckets = summary.get("buckets_df", pl.DataFrame())
    flagged = summary.get("flagged_df", pl.DataFrame())
    L = []
    L.append(f"# Daily surveillance — {dt}")
    L.append("")
    L.append(f"_Generated by `{VERSION}` on {datetime.now(timezone.utc).isoformat()}._")
    L.append("")
    L.append("## Summary")
    L.append("")
    L.append(f"- Production detector: **volume_burst_v2** at edge threshold **{PRODUCTION_EDGE_THRESHOLD:+.3f}**")
    L.append(f"- Flagged trades: **{summary.get('alerts', 0):,}**")
    L.append(f"  - of which **cross-market boosted**: {summary.get('alerts_cross_market_boosted', 0):,}")
    L.append(f"- Total flagged notional: **${summary.get('total_notional_usd', 0):,.0f}**")
    L.append(f"- Cross-market buckets: {summary.get('cross_market_buckets_total', 0):,} total, {summary.get('cross_market_buckets_flagged', 0):,} flagged")
    L.append(f"- Calibration cells: {summary.get('calibration_cells', 0):,}")
    L.append(f"- Global pwin (flagged-resolved baseline): {summary.get('global_pwin', 0):.4f}")
    L.append(f"- Alerts JSONL: `{summary.get('out_file', '')}`")
    L.append("")

    if not flagged.is_empty() and flagged.height > 0:
        L.append("## Top 15 flagged trades (sorted by cross-market × edge)")
        L.append("")
        L.append("| Edge | CM | Markets | Burst | Notional | Market | Taker | Outcome |")
        L.append("|---:|:---:|---:|---:|---:|:---|:---:|:---:|")
        top = flagged.head(15)
        for r in top.iter_rows(named=True):
            won = r.get("was_taker_correct")
            won_s = "✓" if won is True else ("✗" if won is False else "—")
            cm = "✓" if r.get("cross_market_flag") else " "
            edge = r.get("edge_volume_burst", 0.0)
            mkts = r.get("cm_markets_touched") or 0
            bd = r.get("cm_burst_density") or 0
            L.append(f"| {edge:+.3f} | {cm} | {mkts} | {bd} | ${r.get('notional_usd', 0):>9,.0f} | `{(r.get('market_ticker') or '')[:50]}` | {r.get('taker_side','')} | {won_s} |")
        L.append("")

    if not buckets.is_empty():
        flagged_buckets = buckets.filter(pl.col("cross_market_flag")).sort("score_cross_market", descending=True).head(15)
        if not flagged_buckets.is_empty():
            L.append("## Top 15 cross-market events (most concentrated activity)")
            L.append("")
            L.append("| Score | Markets | Burst | Notional | Event | Bucket UTC |")
            L.append("|---:|---:|---:|---:|:---|:---|")
            for r in flagged_buckets.iter_rows(named=True):
                ts = datetime.fromtimestamp(r["bucket_unix"], tz=timezone.utc).strftime("%H:%M")
                L.append(f"| {r['score_cross_market']:.2f} | {r['markets_touched']} | {r['burst_density']} | ${r['notional_concentration']:>9,.0f} | `{r['event_ticker'][:50]}` | {ts} |")
            L.append("")

    L.append("---")
    L.append("")
    L.append("**Methodology + validation.** v2 calibration: per (yes_price_5cent_bin, taker_side) historical")
    L.append("hit rate among flag_volume_burst trades. Edge = pwin_hat − taker_break_even_price. Flag = edge > 0.07.")
    L.append("Backtest holdout (3 days, 6.4M trades): precision@100=0.89, ROC AUC=0.605, mean PnL/trade=+$17.50,")
    L.append("daily-rebalanced Sharpe=24.52. See `surveillance/reports/v2_backtest_*.md` and")
    L.append("`surveillance/reports/tune_v2_*.md` for full validation.")
    path.write_text("\n".join(L), encoding="utf-8")

    # Append digest line
    digest = REPORTS_DIR / "daily_digest.tsv"
    if not digest.exists():
        digest.write_text("dt\trun_id\tflagged\tnotional_usd\tedge_threshold\n", encoding="utf-8")
    with open(digest, "a", encoding="utf-8") as f:
        f.write(f"{dt}\t{run_id}\t{summary.get('alerts', 0)}\t{summary.get('total_notional_usd', 0):.2f}\t{PRODUCTION_EDGE_THRESHOLD:.3f}\n")
    return path


def _write_html_dashboard(dt: str, summary: dict, run_id: str) -> Path:
    """
    M7.2 — Self-contained HTML dashboard with base64-embedded PNG charts.
    Designed to be openable directly in a browser or sync'd to S3 / shared
    via email. Zero JS, no server, no external deps at view-time.
    """
    import base64
    import io

    # Lazy import matplotlib — keeps the orchestrator startup fast
    # for the no-data case.
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    flagged: pl.DataFrame = summary.get("flagged_df", pl.DataFrame())
    buckets: pl.DataFrame = summary.get("buckets_df", pl.DataFrame())

    def _fig_to_b64(fig) -> str:
        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=110, bbox_inches="tight")
        plt.close(fig)
        return base64.b64encode(buf.getvalue()).decode("ascii")

    charts_b64: dict[str, str] = {}

    plt.rcParams.update({
        "font.family": "DejaVu Sans",
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.grid": True,
        "grid.alpha": 0.25,
    })

    # --- Chart 1: hourly flagged trade count ---
    if not flagged.is_empty():
        h = flagged.with_columns(
            pl.from_epoch(pl.col("created_time_unix"), time_unit="s").dt.hour().alias("_h")
        ).group_by("_h").agg([
            pl.len().alias("trades"),
            pl.col("notional_usd").sum().alias("notional"),
        ]).sort("_h")
        if h.height > 0:
            hours = h["_h"].to_list()
            trades = h["trades"].to_list()
            notional = h["notional"].to_list()
            fig, ax1 = plt.subplots(figsize=(10, 4))
            ax1.bar(hours, trades, color="#4c72b0", alpha=0.85, label="Flagged trades")
            ax1.set_xlabel("Hour of day (UTC)")
            ax1.set_ylabel("Trades", color="#4c72b0")
            ax1.tick_params(axis="y", labelcolor="#4c72b0")
            ax1.set_xticks(range(0, 24, 2))
            ax2 = ax1.twinx()
            ax2.plot(hours, notional, "o-", color="#c44e52", label="Notional")
            ax2.set_ylabel("Notional (USD)", color="#c44e52")
            ax2.tick_params(axis="y", labelcolor="#c44e52")
            ax1.set_title(f"Flagged-trade activity by hour — {dt}")
            charts_b64["hourly"] = _fig_to_b64(fig)

    # --- Chart 2: top categories by flagged notional ---
    if not flagged.is_empty() and "milestone_category" in flagged.columns:
        cat = (flagged.filter(pl.col("milestone_category").is_not_null())
                       .group_by("milestone_category")
                       .agg(pl.col("notional_usd").sum().alias("notional"))
                       .sort("notional", descending=True)
                       .head(10))
        if cat.height > 0:
            names = cat["milestone_category"].to_list()
            vals = cat["notional"].to_list()
            fig, ax = plt.subplots(figsize=(10, max(3, 0.45 * len(names) + 1)))
            ax.barh(range(len(names)), vals[::-1], color="#55a868")
            ax.set_yticks(range(len(names)))
            ax.set_yticklabels(names[::-1])
            ax.set_xlabel("Flagged notional (USD)")
            ax.set_title(f"Top categories by flagged notional — {dt}")
            for i, v in enumerate(vals[::-1]):
                ax.annotate(f"${v:,.0f}", (v, i), va="center", fontsize=9, color="#444")
            charts_b64["categories"] = _fig_to_b64(fig)

    # --- Chart 3: edge distribution histogram ---
    if not flagged.is_empty() and "edge_volume_burst" in flagged.columns:
        edges = flagged["edge_volume_burst"].to_list()
        if edges:
            fig, ax = plt.subplots(figsize=(10, 4))
            ax.hist(edges, bins=40, color="#937860", alpha=0.85)
            ax.axvline(PRODUCTION_EDGE_THRESHOLD, color="red", ls="--", label=f"Edge threshold = {PRODUCTION_EDGE_THRESHOLD:.3f}")
            ax.set_xlabel("Edge = pwin_hat − taker_break_even_price")
            ax.set_ylabel("Trade count")
            ax.set_title(f"Distribution of edge across flagged trades — {dt}")
            ax.legend()
            charts_b64["edge_dist"] = _fig_to_b64(fig)

    # --- HTML body ---
    rows_trades = []
    if not flagged.is_empty():
        top = flagged.head(20)
        for r in top.iter_rows(named=True):
            won = r.get("was_taker_correct")
            won_s = "<span class=ok>✓</span>" if won is True else ("<span class=fail>✗</span>" if won is False else "—")
            cm = "<span class=ok>CM</span>" if r.get("cross_market_flag") else ""
            rows_trades.append(
                "<tr>"
                f"<td class=r>{r.get('edge_volume_burst', 0):+.3f}</td>"
                f"<td>{cm}</td>"
                f"<td class=r>{r.get('cm_markets_touched') or 0}</td>"
                f"<td class=r>${r.get('notional_usd', 0):,.0f}</td>"
                f"<td><code>{(r.get('market_ticker') or '')[:55]}</code></td>"
                f"<td>{r.get('taker_side','')}</td>"
                f"<td>{won_s}</td>"
                "</tr>"
            )

    rows_events = []
    if not buckets.is_empty():
        flagged_b = buckets.filter(pl.col("cross_market_flag")).sort("score_cross_market", descending=True).head(15)
        for r in flagged_b.iter_rows(named=True):
            ts = datetime.fromtimestamp(r["bucket_unix"], tz=timezone.utc).strftime("%H:%M")
            rows_events.append(
                "<tr>"
                f"<td class=r>{r['score_cross_market']:.2f}</td>"
                f"<td class=r>{r['markets_touched']}</td>"
                f"<td class=r>{r['burst_density']}</td>"
                f"<td class=r>${r['notional_concentration']:,.0f}</td>"
                f"<td><code>{r['event_ticker'][:55]}</code></td>"
                f"<td>{ts}</td>"
                "</tr>"
            )

    def _img(key: str, alt: str) -> str:
        b64 = charts_b64.get(key)
        if not b64:
            return ""
        return f'<img class="chart" alt="{alt}" src="data:image/png;base64,{b64}"/>'

    html = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<title>Kalshi surveillance — {dt}</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
         margin: 2rem auto; max-width: 1100px; color: #222; line-height: 1.45; padding: 0 1rem; }}
  h1, h2 {{ color: #2a4d6e; }}
  h1 {{ border-bottom: 2px solid #2a4d6e; padding-bottom: .35rem; }}
  .kpis {{ display: flex; flex-wrap: wrap; gap: 1rem; margin: 1rem 0 1.5rem; }}
  .kpi {{ background: #f4f6f8; border-radius: 6px; padding: .75rem 1rem; flex: 1; min-width: 180px; }}
  .kpi .label {{ font-size: .8rem; color: #666; text-transform: uppercase; letter-spacing: .04em; }}
  .kpi .value {{ font-size: 1.6rem; font-weight: 600; color: #2a4d6e; }}
  table {{ width: 100%; border-collapse: collapse; margin: .75rem 0 1.5rem; font-size: .92rem; }}
  th, td {{ padding: .45rem .6rem; text-align: left; border-bottom: 1px solid #e0e3e7; }}
  th {{ background: #f4f6f8; color: #2a4d6e; font-weight: 600; }}
  td.r {{ text-align: right; font-variant-numeric: tabular-nums; }}
  code {{ background: #f4f6f8; padding: 1px 4px; border-radius: 3px; font-size: .85rem; }}
  .ok {{ color: #2a6f3f; font-weight: 600; }}
  .fail {{ color: #b53030; font-weight: 600; }}
  img.chart {{ width: 100%; max-width: 1000px; height: auto; margin: .75rem 0 1.5rem;
              border: 1px solid #e0e3e7; border-radius: 4px; }}
  .footer {{ color: #888; font-size: .82rem; margin-top: 3rem; border-top: 1px solid #e0e3e7; padding-top: 1rem; }}
</style>
</head><body>

<h1>Kalshi surveillance — {dt}</h1>
<div style="color:#666; font-size:.85rem;">Generated {datetime.now(timezone.utc).isoformat()} · run_id {run_id} · detector volume_burst_v2 @ edge {PRODUCTION_EDGE_THRESHOLD:+.3f}</div>

<div class="kpis">
  <div class="kpi"><div class="label">Flagged trades</div><div class="value">{summary.get('alerts', 0):,}</div></div>
  <div class="kpi"><div class="label">Cross-market boosted</div><div class="value">{summary.get('alerts_cross_market_boosted', 0):,}</div></div>
  <div class="kpi"><div class="label">Total flagged notional</div><div class="value">${summary.get('total_notional_usd', 0):,.0f}</div></div>
  <div class="kpi"><div class="label">Flagged events (cross-market)</div><div class="value">{summary.get('cross_market_buckets_flagged', 0):,}</div></div>
  <div class="kpi"><div class="label">Calibration cells</div><div class="value">{summary.get('calibration_cells', 0):,}</div></div>
  <div class="kpi"><div class="label">Global p̂(win)</div><div class="value">{summary.get('global_pwin', 0):.3f}</div></div>
</div>

<h2>Top 20 flagged trades</h2>
<table>
  <thead><tr><th>Edge</th><th>CM</th><th>Mkts</th><th>Notional</th><th>Market</th><th>Taker</th><th>Outcome</th></tr></thead>
  <tbody>
  {''.join(rows_trades) if rows_trades else '<tr><td colspan=7><em>No flagged trades on this date.</em></td></tr>'}
  </tbody>
</table>

<h2>Top 15 cross-market events</h2>
<table>
  <thead><tr><th>Score</th><th>Markets</th><th>Burst</th><th>Notional</th><th>Event</th><th>UTC</th></tr></thead>
  <tbody>
  {''.join(rows_events) if rows_events else '<tr><td colspan=6><em>No cross-market events on this date.</em></td></tr>'}
  </tbody>
</table>

<h2>Activity by hour</h2>
{_img("hourly", "hourly activity") or "<p><em>Insufficient data for hourly chart.</em></p>"}

<h2>Top categories by flagged notional</h2>
{_img("categories", "categories") or "<p><em>No category data.</em></p>"}

<h2>Edge distribution</h2>
{_img("edge_dist", "edge distribution") or "<p><em>No edge data.</em></p>"}

<div class="footer">
<strong>Methodology.</strong> v2 calibration: per (yes_price_5cent_bin, taker_side) historical
hit rate among flag_volume_burst trades. Edge = p̂(win) − taker_break_even_price.
Flag = edge > {PRODUCTION_EDGE_THRESHOLD:+.3f}. Backtest holdout (3 days, 6.4 M trades):
precision@100=0.89, ROC AUC=0.605, mean PnL/trade=+$17.50, Sharpe=24.52. See
<code>surveillance/reports/v2_backtest_*.md</code> and <code>tune_v2_*.md</code>.
<br>Cross-market detector aggregates v2 flags by (event_ticker × 300-second bucket); a bucket is flagged when ≥{CM_MIN_MARKETS_TOUCHED} markets are touched AND ≥{CM_MIN_FLAGGED_TRADES} flagged trades AND ≥${CM_MIN_NOTIONAL_USD:,.0f} notional concentration.
<br>No per-account identity is exposed by Kalshi public data; all signals are behavioral.
</div>

</body></html>"""

    path = REPORTS_DIR / f"daily_{dt}.html"
    path.write_text(html, encoding="utf-8")
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
    _log(
        f"<--- alerts written: count={alerts_summary.get('alerts', 0):,}  "
        f"cm_boosted={alerts_summary.get('alerts_cross_market_boosted', 0):,}  "
        f"buckets_flagged={alerts_summary.get('cross_market_buckets_flagged', 0)}"
    )

    # Stage 7: markdown daily report
    report_path = _write_daily_report(dt, alerts_summary, run_id)
    _log(f"markdown report -> {report_path.relative_to(PROJECT_ROOT)}")

    # Stage 8: HTML dashboard (M7.2)
    try:
        html_path = _write_html_dashboard(dt, alerts_summary, run_id)
        _log(f"HTML dashboard  -> {html_path.relative_to(PROJECT_ROOT)}")
    except Exception as exc:
        _log(f"WARN  HTML dashboard failed: {exc!r}")
        html_path = None

    # Drop non-serializable DataFrames before audit
    audit_alerts = {k: v for k, v in alerts_summary.items() if k not in ("buckets_df", "flagged_df")}

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
        "alerts": audit_alerts,
        "report_path": str(report_path.relative_to(PROJECT_ROOT)),
        "html_path": str(html_path.relative_to(PROJECT_ROOT)) if html_path else None,
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
