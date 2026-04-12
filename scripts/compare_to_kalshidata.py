#!/usr/bin/env python3
"""
Compare our Parquet dataset to official Kalshi Data (kalshidata.com) baselines.

1. Copy data/kalshi/state/kalshidata_baseline.example.json to kalshidata_baseline.json
2. Paste current numbers from https://www.kalshidata.com into that JSON
3. Run: uv run python scripts/compare_to_kalshidata.py

Shows how far your ingested data is from the official headline stats (with caveats).

Kalshi Data (https://www.kalshidata.com/) renders metrics in the browser only; there is no
static JSON of headline numbers in the HTML. For an apples-to-apples run:

  1. Open the site, note the date/as-of if shown, and copy each headline number you care about.
  2. Put them in data/kalshi/state/kalshidata_baseline.json (see keys below).
  3. Run: uv run python scripts/compare_to_kalshidata.py

Metric alignment (closest analogues — definitions still may differ; see docs/KALSHIDATA_COMPARISON.md):

  | Typical Kalshi Data headline   | JSON key in kalshidata_baseline.json | Our Parquet computation        |
  |--------------------------------|--------------------------------------|--------------------------------|
  | Total volume (USD) cumulative  | total_volume_usd                     | SUM(count_eff * yes_price_dollars) (+ cents fallback) |
  | Total / cumulative contracts   | total_contracts_official             | SUM(COALESCE(count, count_fp)) |
  | "Total trades" (big number)    | total_trades_on_dashboard            | COUNT(*) trade rows (API-style rows; NOT same as dashboard) |
  | Avg open interest (USD)        | avg_open_interest_usd                | (not in trade Parquet — show official only) |

Use --print-mapping to print this table. Use --ours-json to print only your aggregates (paste
alongside the website for a manual check).
"""

from __future__ import annotations

import argparse
import glob
import json
import sys
from pathlib import Path

_SCRIPT_ROOT = Path(__file__).resolve().parents[1]
if str(_SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_ROOT))

import duckdb

from src.kalshi_forward.paths import (
    FORWARD_TRADES_GLOB,
    HISTORICAL_TRADES_GLOB,
    LEGACY_FORWARD_TRADES_GLOB,
    STATE_DIR,
)

BASELINE_FILE = STATE_DIR / "kalshidata_baseline.json"
EXAMPLE_FILE = STATE_DIR / "kalshidata_baseline.example.json"


def _trade_patterns():
    out = [(str(HISTORICAL_TRADES_GLOB), "hist"), (str(FORWARD_TRADES_GLOB), "fwd")]
    if glob.glob(str(LEGACY_FORWARD_TRADES_GLOB)):
        out.append((str(LEGACY_FORWARD_TRADES_GLOB), "legacy"))
    return out


def compute_ours(con: duckdb.DuckDBPyConnection) -> dict:
    patterns = _trade_patterns()
    vol = 0.0
    dv = 0.0
    n_trades = 0
    t_min = None
    t_max = None
    for pat, _ in patterns:
        if not glob.glob(pat):
            continue
        vol += con.execute(
            f"SELECT COALESCE(SUM(COALESCE(NULLIF(count,0), TRY_CAST(count_fp AS DOUBLE))), 0) FROM read_parquet('{pat}')"
        ).fetchone()[0] or 0
        try:
            dv += con.execute(
                f"""SELECT COALESCE(SUM(COALESCE(NULLIF(count,0), TRY_CAST(count_fp AS DOUBLE))
                    * TRY_CAST(yes_price_dollars AS DOUBLE)), 0) FROM read_parquet('{pat}')"""
            ).fetchone()[0] or 0
        except Exception:
            dv += con.execute(
                f"""SELECT COALESCE(SUM(COALESCE(NULLIF(count,0), TRY_CAST(count_fp AS DOUBLE)) * yes_price / 100.0), 0)
                    FROM read_parquet('{pat}')"""
            ).fetchone()[0] or 0
        n_trades += con.execute(f"SELECT COUNT(*) FROM read_parquet('{pat}')").fetchone()[0]
        r = con.execute(
            f"SELECT MIN(created_time), MAX(created_time) FROM read_parquet('{pat}')"
        ).fetchone()
        if r[0]:
            t_min = r[0] if t_min is None or str(r[0]) < str(t_min) else t_min
        if r[1]:
            t_max = r[1] if t_max is None or str(r[1]) > str(t_max) else t_max

    return {
        "n_trade_rows": n_trades,
        "total_contracts": vol,
        "notional_usd": dv,
        "date_min": str(t_min) if t_min else "",
        "date_max": str(t_max) if t_max else "",
    }


def pct_of_official(ours: float, official: float) -> str:
    if not official or official <= 0:
        return "n/a"
    p = 100.0 * ours / official
    return f"{p:.2f}% of official"


def gap_from_official(ours: float, official: float) -> str:
    if not official or official <= 0:
        return "n/a"
    gap = 100.0 * (1.0 - ours / official)
    return f"~{gap:.1f}% below official (you have {100 * ours / official:.2f}%)"


def _print_metric_mapping() -> None:
    print()
    print("  Kalshi Data (https://www.kalshidata.com/) loads headline stats in the browser.")
    print("  Copy numbers from the site into data/kalshi/state/kalshidata_baseline.json, then run without --print-mapping.")
    print()
    print("  " + "-" * 68)
    print(f"  {'Kalshi Data (typical headline)':<34} {'baseline JSON key':<28}")
    print("  " + "-" * 68)
    rows = [
        ("Total volume USD (cumulative)", "total_volume_usd"),
        ("Total contracts (cumulative)", "total_contracts_official"),
        ('"Total trades" / dashboard trades', "total_trades_on_dashboard"),
        ("Avg open interest USD", "avg_open_interest_usd"),
        ("As-of / snapshot note", "as_of_website (string)"),
    ]
    for left, key in rows:
        print(f"  {left:<34} {key}")
    print("  " + "-" * 68)
    print()


def main() -> int:
    ap = argparse.ArgumentParser(description="Compare Parquet aggregates to Kalshi Data baselines")
    ap.add_argument(
        "--print-mapping",
        action="store_true",
        help="Print how website headlines map to baseline JSON keys and exit",
    )
    ap.add_argument(
        "--ours-json",
        action="store_true",
        help="Print only our aggregates as JSON (for side-by-side with the website) and exit",
    )
    ap.add_argument(
        "--baseline",
        type=Path,
        default=None,
        metavar="PATH",
        help=(
            "Baseline JSON to compare against (default: data/kalshi/state/kalshidata_baseline.json). "
            "Use a separate file with numbers copied from kalshidata.com to avoid editing the default."
        ),
    )
    ap.add_argument(
        "--stdin-baseline",
        action="store_true",
        help="Read baseline JSON from stdin instead of --baseline file",
    )
    args = ap.parse_args()

    if args.print_mapping:
        _print_metric_mapping()
        return 0

    con = duckdb.connect()
    try:
        ours = compute_ours(con)
    finally:
        con.close()

    if args.ours_json:
        print(json.dumps({"source": "local_parquet", **{k: ours[k] for k in sorted(ours)}}, indent=2))
        return 0

    if args.stdin_baseline:
        baseline_raw = json.load(sys.stdin)
        baseline_label = "stdin"
    else:
        baseline_path = args.baseline if args.baseline is not None else BASELINE_FILE
        baseline_label = str(baseline_path)
        if not baseline_path.exists():
            print()
            print("No baseline file yet.")
            print(f"  1. Copy:  {EXAMPLE_FILE}")
            print(f"  2. To:    {BASELINE_FILE}")
            print("  3. Paste numbers from https://www.kalshidata.com into the JSON")
            print("  4. Run:   uv run python scripts/compare_to_kalshidata.py")
            print("  Or:      uv run python scripts/compare_to_kalshidata.py --baseline path/to/snapshot.json")
            print("  Or:      uv run python scripts/compare_to_kalshidata.py --print-mapping")
            print()
            return 1
        baseline_raw = json.loads(baseline_path.read_text(encoding="utf-8"))

    # strip comment / readme keys
    baseline = {k: v for k, v in baseline_raw.items() if not str(k).startswith("_")}

    print()
    print("=" * 72)
    print("  YOUR DATA vs KALSHI DATA (kalshidata.com headline baseline)")
    print("=" * 72)
    print(f"  Baseline source:           {baseline_label}")
    print(f"  Your trade rows date range: {ours['date_min'][:10]} .. {ours['date_max'][:10]}")
    print(f"  Official baseline as_of:   {baseline.get('as_of_website', baseline.get('as_of', '?'))}")
    print()

    off_vol = baseline.get("total_volume_usd")
    if off_vol:
        print("  --- Total volume (USD) ---")
        print(f"  Official (Kalshi Data):  ${off_vol:,.0f}")
        print(f"  Your dataset (notional): ${ours['notional_usd']:,.0f}")
        print(f"  -> {pct_of_official(ours['notional_usd'], off_vol)}")
        print(f"  -> {gap_from_official(ours['notional_usd'], off_vol)}")
        print("  Caveat: official may include both sides, full platform, and data after your last run.")
        ratio_v = float(ours["notional_usd"]) / float(off_vol)
        if ratio_v >= 0.85:
            print("  Closeness: your notional is in the same ballpark as this baseline (>=85%).")
        elif ratio_v >= 0.40:
            print(
                "  Closeness: same order of magnitude as the baseline (~40-85% often seen when "
                "definitions/time range differ); not evidence of broken ingestion by itself."
            )
        else:
            print(
                "  Closeness: well under baseline; may be date coverage, notional definition, or stale baseline "
                "(refresh numbers from the live site into a JSON file and use --baseline)."
            )
        print()

    off_c = baseline.get("total_contracts_official")
    if off_c:
        print("  --- Total contracts ---")
        print(f"  Official:  {off_c:,.0f}")
        print(f"  Yours:     {ours['total_contracts']:,.0f}")
        print(f"  -> {pct_of_official(ours['total_contracts'], float(off_c))}")
        print()

    off_trades = baseline.get("total_trades_on_dashboard")
    if off_trades:
        print('  --- "Total trades" on dashboard vs your API trade rows ---')
        print(f"  Kalshi dashboard number: {off_trades:,.0f}  (likely NOT same definition as API rows)")
        print(f"  Your stored trade rows:  {ours['n_trade_rows']:,.0f}")
        ratio = off_trades / max(ours["n_trade_rows"], 1)
        print(f"  -> Dashboard is ~{ratio:,.0f}x larger - do not compare these as the same metric.")
        print("  See docs/KALSHIDATA_COMPARISON.md")
        print()

    off_oi = baseline.get("avg_open_interest_usd")
    if off_oi:
        print("  --- Open interest ---")
        print(f"  Official avg OI: ${off_oi:,.0f}")
        print("  Your dataset:     (not computed from trades - need OI/positions data)")
        print()

    print("=" * 72)
    print("  Summary: You CAN compare volume & contracts if definitions match.")
    print("  Your gap is mostly: shorter date range + different volume definition + API scope.")
    print("=" * 72)
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
