#!/usr/bin/env python3
"""
Compare our Parquet dataset to official Kalshi Data (kalshidata.com) baselines.

1. Copy data/kalshi/state/kalshidata_baseline.example.json → kalshidata_baseline.json
2. Paste current numbers from https://www.kalshidata.com into that JSON
3. Run: uv run python scripts/compare_to_kalshidata.py

Shows how far your ingested data is from the official headline stats (with caveats).
"""

from __future__ import annotations

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


def main() -> int:
    if not BASELINE_FILE.exists():
        print()
        print("No baseline file yet.")
        print(f"  1. Copy:  {EXAMPLE_FILE}")
        print(f"  2. To:    {BASELINE_FILE}")
        print("  3. Paste numbers from https://www.kalshidata.com into the JSON")
        print("  4. Run this script again.")
        print()
        return 1

    baseline = json.loads(BASELINE_FILE.read_text())
    # strip comment keys
    baseline = {k: v for k, v in baseline.items() if not k.startswith("_")}

    con = duckdb.connect()
    ours = compute_ours(con)

    print()
    print("=" * 72)
    print("  YOUR DATA vs KALSHI DATA (kalshidata.com)")
    print("=" * 72)
    print(f"  Your trade rows date range: {ours['date_min'][:10]} … {ours['date_max'][:10]}")
    print(f"  Official baseline as_of:   {baseline.get('as_of_website', baseline.get('as_of', '?'))}")
    print()

    off_vol = baseline.get("total_volume_usd")
    if off_vol:
        print("  --- Total volume (USD) ---")
        print(f"  Official (Kalshi Data):  ${off_vol:,.0f}")
        print(f"  Your dataset (notional): ${ours['notional_usd']:,.0f}")
        print(f"  → {pct_of_official(ours['notional_usd'], off_vol)}")
        print(f"  → {gap_from_official(ours['notional_usd'], off_vol)}")
        print("  Caveat: official may include both sides, full platform, and data after your last run.")
        print()

    off_c = baseline.get("total_contracts_official")
    if off_c:
        print("  --- Total contracts ---")
        print(f"  Official:  {off_c:,.0f}")
        print(f"  Yours:     {ours['total_contracts']:,.0f}")
        print(f"  → {pct_of_official(ours['total_contracts'], float(off_c))}")
        print()

    off_trades = baseline.get("total_trades_on_dashboard")
    if off_trades:
        print("  --- “Total trades” on dashboard vs your API trade rows ---")
        print(f"  Kalshi dashboard number: {off_trades:,.0f}  (likely NOT same definition as API rows)")
        print(f"  Your stored trade rows:  {ours['n_trade_rows']:,.0f}")
        ratio = off_trades / max(ours["n_trade_rows"], 1)
        print(f"  → Dashboard is ~{ratio:,.0f}× larger — do not compare these as the same metric.")
        print("  See docs/KALSHIDATA_COMPARISON.md")
        print()

    off_oi = baseline.get("avg_open_interest_usd")
    if off_oi:
        print("  --- Open interest ---")
        print(f"  Official avg OI: ${off_oi:,.0f}")
        print("  Your dataset:     (not computed from trades — need OI/positions data)")
        print()

    print("=" * 72)
    print("  Summary: You CAN compare volume & contracts if definitions match.")
    print("  Your gap is mostly: shorter date range + different volume definition + API scope.")
    print("=" * 72)
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
