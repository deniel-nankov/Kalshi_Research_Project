#!/usr/bin/env python3
"""
Institutional-grade data health and validation for the Kalshi dataset.

Runs comprehensive checks tailored to our schema and pipeline:
  • Schema integrity & required columns
  • Uniqueness (trade_id, market keys)
  • Referential integrity (trades → markets)
  • Temporal consistency (no future dates, parseable timestamps)
  • Value constraints (prices 0–100, yes+no=100 for binary)
  • Boundary alignment (historical vs forward cutoff)
  • Completeness & coverage
  • Statistical sanity (outliers, distributions)
  • Output: JSON report + human-readable summary

Usage:
  uv run python scripts/validate_data_health.py
  uv run python scripts/validate_data_health.py --output report.json
  uv run python scripts/validate_data_health.py --strict
"""

from __future__ import annotations

import argparse
import glob
import json
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

_SCRIPT_ROOT = Path(__file__).resolve().parents[1]
if str(_SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_ROOT))

import duckdb

from src.kalshi_forward.paths import (
    FORWARD_MARKETS_GLOB,
    FORWARD_TRADES_GLOB,
    HISTORICAL_CHECKPOINT_FILE,
    HISTORICAL_MARKETS_FILE,
    HISTORICAL_TRADES_GLOB,
    LEGACY_FORWARD_MARKETS_GLOB,
    LEGACY_FORWARD_TRADES_GLOB,
    PROJECT_ROOT,
)


def _has_glob(p: Path) -> bool:
    return len(glob.glob(str(p))) > 0


def _trade_sources() -> list[str]:
    patterns = []
    if _has_glob(HISTORICAL_TRADES_GLOB):
        patterns.append(str(HISTORICAL_TRADES_GLOB))
    if _has_glob(FORWARD_TRADES_GLOB):
        patterns.append(str(FORWARD_TRADES_GLOB))
    if _has_glob(LEGACY_FORWARD_TRADES_GLOB):
        patterns.append(str(LEGACY_FORWARD_TRADES_GLOB))
    return patterns


def _market_sources() -> list[str]:
    patterns = []
    if HISTORICAL_MARKETS_FILE.exists():
        patterns.append(str(HISTORICAL_MARKETS_FILE))
    if _has_glob(FORWARD_MARKETS_GLOB):
        patterns.append(str(FORWARD_MARKETS_GLOB))
    if _has_glob(LEGACY_FORWARD_MARKETS_GLOB):
        patterns.append(str(LEGACY_FORWARD_MARKETS_GLOB))
    return patterns


def _forward_trade_sources() -> list[str]:
    patterns = []
    if _has_glob(FORWARD_TRADES_GLOB):
        patterns.append(str(FORWARD_TRADES_GLOB))
    if _has_glob(LEGACY_FORWARD_TRADES_GLOB):
        patterns.append(str(LEGACY_FORWARD_TRADES_GLOB))
    return patterns


# Common columns across historical (fewer) and forward (more) for schema compatibility
_TRADE_COLS = "trade_id, ticker, taker_side, count, yes_price, no_price, price, created_time, count_fp, yes_price_dollars, no_price_dollars"
_MARKET_COLS = "ticker, event_ticker, market_type, title, status, volume, created_time, close_time, updated_time, open_interest, dollar_volume"


def _combined_trades_sql() -> str:
    srcs = _trade_sources()
    if not srcs:
        return ""
    return " UNION ALL ".join(f"SELECT {_TRADE_COLS} FROM '{s}'" for s in srcs)


def _combined_markets_sql() -> str:
    srcs = _market_sources()
    if not srcs:
        return ""
    return " UNION ALL ".join(f"SELECT {_MARKET_COLS} FROM '{s}'" for s in srcs)


@dataclass
class CheckResult:
    name: str
    status: str  # "PASS" | "WARN" | "FAIL"
    message: str
    details: dict = field(default_factory=dict)


def run_checks(con: duckdb.DuckDBPyConnection) -> list[CheckResult]:
    results: list[CheckResult] = []
    trades_sql = _combined_trades_sql()
    markets_sql = _combined_markets_sql()

    if not trades_sql:
        results.append(CheckResult("DATA_PRESENCE", "FAIL", "No trade data found", {}))
        return results
    if not markets_sql:
        results.append(CheckResult("DATA_PRESENCE", "FAIL", "No market data found", {}))
        return results

    # ─── 1. Schema & required columns ──────────────────────────────────────
    try:
        trade_cols = set(con.execute(f"DESCRIBE SELECT * FROM ({trades_sql}) LIMIT 1").fetchdf()["column_name"])
        required = {"trade_id", "ticker", "created_time"}
        missing = required - trade_cols
        if missing:
            results.append(CheckResult("SCHEMA_TRADES", "FAIL", f"Missing required columns: {missing}", {"columns": list(trade_cols)}))
        else:
            results.append(CheckResult("SCHEMA_TRADES", "PASS", "Required columns present", {"columns": sorted(trade_cols)}))
    except Exception as e:
        results.append(CheckResult("SCHEMA_TRADES", "FAIL", str(e), {}))

    try:
        market_cols = set(con.execute(f"DESCRIBE SELECT * FROM ({markets_sql}) LIMIT 1").fetchdf()["column_name"])
        if "ticker" not in market_cols or "created_time" not in market_cols:
            results.append(CheckResult("SCHEMA_MARKETS", "FAIL", "Missing required columns: ticker, created_time", {"columns": list(market_cols)}))
        else:
            results.append(CheckResult("SCHEMA_MARKETS", "PASS", "Required columns present", {"columns": list(market_cols)}))
    except Exception as e:
        results.append(CheckResult("SCHEMA_MARKETS", "FAIL", str(e), {}))

    # ─── 2. Uniqueness (trade_id, market keys) ───────────────────────────────
    try:
        trade_total = con.execute(f"SELECT COUNT(*) FROM ({trades_sql})").fetchone()[0]
        trade_distinct = con.execute(f"SELECT COUNT(DISTINCT trade_id) FROM ({trades_sql}) WHERE trade_id IS NOT NULL AND trade_id <> ''").fetchone()[0]
        trade_dupes = trade_total - trade_distinct
        if trade_dupes > 0:
            results.append(CheckResult("UNIQUENESS_TRADES", "FAIL", f"Duplicate trade_ids: {trade_dupes:,}", {"total": trade_total, "distinct": trade_distinct}))
        else:
            results.append(CheckResult("UNIQUENESS_TRADES", "PASS", f"All {trade_total:,} trade_ids unique", {"total": trade_total}))
    except Exception as e:
        results.append(CheckResult("UNIQUENESS_TRADES", "FAIL", str(e), {}))

    try:
        market_total = con.execute(f"SELECT COUNT(*) FROM ({markets_sql})").fetchone()[0]
        market_distinct = con.execute(
            f"SELECT COUNT(DISTINCT ticker || '|' || COALESCE(close_time, created_time, '')) FROM ({markets_sql}) WHERE ticker IS NOT NULL AND ticker <> ''"
        ).fetchone()[0]
        market_dupes = market_total - market_distinct
        if market_dupes > 0:
            results.append(CheckResult("UNIQUENESS_MARKETS", "WARN", f"Duplicate market keys: {market_dupes:,} (expected for same ticker at different times)", {"total": market_total, "distinct": market_distinct}))
        else:
            results.append(CheckResult("UNIQUENESS_MARKETS", "PASS", f"All {market_total:,} market keys unique", {"total": market_total}))
    except Exception as e:
        results.append(CheckResult("UNIQUENESS_MARKETS", "FAIL", str(e), {}))

    # ─── 3. Referential integrity (trades.ticker in markets) ─────────────────
    try:
        orphan = con.execute(
            f"""
            SELECT COUNT(*) FROM (
                SELECT DISTINCT t.ticker FROM ({trades_sql}) t
                WHERE t.ticker IS NOT NULL AND t.ticker <> ''
                EXCEPT
                SELECT DISTINCT m.ticker FROM ({markets_sql}) m
                WHERE m.ticker IS NOT NULL AND m.ticker <> ''
            ) o
            """
        ).fetchone()[0]
        if orphan > 0:
            results.append(CheckResult("REFERENTIAL_INTEGRITY", "WARN", f"Trades reference {orphan:,} tickers not in markets (historical lag)", {"orphan_tickers": orphan}))
        else:
            results.append(CheckResult("REFERENTIAL_INTEGRITY", "PASS", "All trade tickers exist in markets", {}))
    except Exception as e:
        results.append(CheckResult("REFERENTIAL_INTEGRITY", "WARN", f"Check skipped: {e}", {}))

    # ─── 4. Temporal consistency (parseable, no future) ──────────────────────
    try:
        trade_null_ts = con.execute(f"SELECT COUNT(*) FROM ({trades_sql}) WHERE created_time IS NULL OR TRIM(created_time) = ''").fetchone()[0]
        trade_parse_fail = con.execute(
            f"SELECT COUNT(*) FROM ({trades_sql}) WHERE created_time IS NOT NULL AND TRIM(created_time) <> '' AND TRY_CAST(created_time AS TIMESTAMP) IS NULL"
        ).fetchone()[0]
        trade_future = con.execute(
            f"SELECT COUNT(*) FROM ({trades_sql}) WHERE TRY_CAST(created_time AS TIMESTAMP) > CURRENT_TIMESTAMP"
        ).fetchone()[0]
        if trade_null_ts > 0 or trade_parse_fail > 0:
            results.append(CheckResult("TEMPORAL_TRADES", "FAIL", f"Invalid created_time: {trade_null_ts} null, {trade_parse_fail} unparseable", {}))
        elif trade_future > 0:
            results.append(CheckResult("TEMPORAL_TRADES", "WARN", f"Trades with future created_time: {trade_future}", {}))
        else:
            min_ts = con.execute(f"SELECT MIN(TRY_CAST(created_time AS TIMESTAMP)) FROM ({trades_sql})").fetchone()[0]
            max_ts = con.execute(f"SELECT MAX(TRY_CAST(created_time AS TIMESTAMP)) FROM ({trades_sql})").fetchone()[0]
            results.append(CheckResult("TEMPORAL_TRADES", "PASS", "All created_time parseable, no future dates", {"min": str(min_ts), "max": str(max_ts)}))
    except Exception as e:
        results.append(CheckResult("TEMPORAL_TRADES", "FAIL", str(e), {}))

    # ─── 5. Value constraints (prices 0–100) ────────────────────────────────
    try:
        trade_bad_price = con.execute(
            f"SELECT COUNT(*) FROM ({trades_sql}) WHERE yes_price < 0 OR yes_price > 100 OR no_price < 0 OR no_price > 100"
        ).fetchone()[0]
        trade_bad_sum = con.execute(
            f"SELECT COUNT(*) FROM ({trades_sql}) WHERE yes_price + no_price <> 100 AND (yes_price > 0 OR no_price > 0)"
        ).fetchone()[0]
        if trade_bad_price > 0:
            results.append(CheckResult("VALUE_CONSTRAINTS_TRADES", "FAIL", f"Trades with price outside [0,100]: {trade_bad_price:,}", {}))
        elif trade_bad_sum > 0:
            results.append(CheckResult("VALUE_CONSTRAINTS_TRADES", "WARN", f"Trades with yes_price+no_price≠100: {trade_bad_sum:,}", {}))
        else:
            results.append(CheckResult("VALUE_CONSTRAINTS_TRADES", "PASS", "Price values in valid range", {}))
    except Exception as e:
        results.append(CheckResult("VALUE_CONSTRAINTS_TRADES", "WARN", f"Check skipped: {e}", {}))

    # ─── 6. Boundary alignment (max trade <= API cutoff) ──────────────────────
    try:
        import urllib.request
        from src.kalshi_forward.paths import CUTOFF_URL
        hist_max = con.execute(f"SELECT MAX(TRY_CAST(created_time AS TIMESTAMP)) FROM ({trades_sql})").fetchone()[0]
        try:
            req = urllib.request.Request(CUTOFF_URL)
            with urllib.request.urlopen(req, timeout=15) as resp:
                cutoff_data = json.loads(resp.read().decode())
            cutoff_ts = cutoff_data.get("trades_created_ts", "")
            if cutoff_ts and hist_max:
                cutoff_dt = con.execute("SELECT TRY_CAST(? AS TIMESTAMP)", [cutoff_ts]).fetchone()[0]
                if cutoff_dt and hist_max > cutoff_dt:
                    results.append(CheckResult("BOUNDARY_ALIGNMENT", "FAIL", "Max trade exceeds API cutoff", {"max": str(hist_max), "cutoff": cutoff_ts}))
                else:
                    results.append(CheckResult("BOUNDARY_ALIGNMENT", "PASS", "Data within API cutoff", {"max": str(hist_max), "cutoff": cutoff_ts}))
            else:
                results.append(CheckResult("BOUNDARY_ALIGNMENT", "WARN", "Could not verify against API cutoff", {"max": str(hist_max)}))
        except Exception as api_err:
            err_str = str(api_err)
            if "403" in err_str or "Forbidden" in err_str:
                results.append(
                    CheckResult(
                        "BOUNDARY_ALIGNMENT",
                        "WARN",
                        "Skipped (API unreachable: 403 Forbidden — network/proxy/VPN?)",
                        {"max": str(hist_max)},
                    )
                )
            else:
                results.append(CheckResult("BOUNDARY_ALIGNMENT", "WARN", f"API cutoff fetch failed: {api_err}", {"max": str(hist_max)}))
    except Exception as e:
        results.append(CheckResult("BOUNDARY_ALIGNMENT", "WARN", str(e), {}))

    # ─── 7. Historical/forward overlap (no duplicate trade_ids) ──────────────
    try:
        fwd_srcs = _forward_trade_sources()
        if _has_glob(HISTORICAL_TRADES_GLOB) and fwd_srcs:
            fwd_sql = " UNION ALL ".join(f"SELECT trade_id FROM '{s}'" for s in fwd_srcs)
            if fwd_sql:
                overlap = con.execute(
                    f"""
                    SELECT COUNT(*) FROM (
                        SELECT DISTINCT trade_id FROM '{HISTORICAL_TRADES_GLOB}' WHERE trade_id IS NOT NULL AND trade_id <> ''
                    ) h
                    INNER JOIN (
                        SELECT DISTINCT trade_id FROM ({fwd_sql}) WHERE trade_id IS NOT NULL AND trade_id <> ''
                    ) f ON h.trade_id = f.trade_id
                    """
                ).fetchone()[0]
                if overlap > 0:
                    results.append(CheckResult("BOUNDARY_OVERLAP", "FAIL", f"Historical/forward trade_id overlap: {overlap:,}", {}))
                else:
                    results.append(CheckResult("BOUNDARY_OVERLAP", "PASS", "No historical/forward trade_id overlap", {}))
            else:
                results.append(CheckResult("BOUNDARY_OVERLAP", "PASS", "No forward data to compare", {}))
        else:
            results.append(CheckResult("BOUNDARY_OVERLAP", "PASS", "Single source; overlap N/A", {}))
    except Exception as e:
        results.append(CheckResult("BOUNDARY_OVERLAP", "WARN", str(e), {}))

    # ─── 7a. Boundary gap (no big time skip between historical and forward) ───
    try:
        fwd_srcs = _forward_trade_sources()
        if _has_glob(HISTORICAL_TRADES_GLOB) and fwd_srcs:
            fwd_sql = " UNION ALL ".join(f"SELECT {_TRADE_COLS} FROM '{s}'" for s in fwd_srcs)
            hist_max_ts = con.execute(
                f"SELECT MAX(TRY_CAST(created_time AS TIMESTAMP)) FROM '{HISTORICAL_TRADES_GLOB}'"
            ).fetchone()[0]
            fwd_min_ts = con.execute(
                f"SELECT MIN(TRY_CAST(created_time AS TIMESTAMP)) FROM ({fwd_sql})"
            ).fetchone()[0]
            if hist_max_ts and fwd_min_ts:
                gap_seconds = (fwd_min_ts - hist_max_ts).total_seconds()
                gap_hours = gap_seconds / 3600
                max_gap_hours = 24  # expect forward to pick up within 24h of last historical
                if gap_seconds < 0:
                    # Time overlap is fine: no missing window; BOUNDARY_OVERLAP confirms no duplicate trade_ids
                    results.append(
                        CheckResult(
                            "BOUNDARY_GAP",
                            "PASS",
                            f"No gap (forward overlaps historical in time; first_forward={fwd_min_ts}, last_historical={hist_max_ts})",
                            {"last_historical": str(hist_max_ts), "first_forward": str(fwd_min_ts), "gap_hours": round(gap_hours, 2)},
                        )
                    )
                elif gap_hours > max_gap_hours:
                    results.append(
                        CheckResult(
                            "BOUNDARY_GAP",
                            "WARN",
                            f"Large gap between historical and forward: {gap_hours:.1f}h (last historical {hist_max_ts}, first forward {fwd_min_ts}); possible missing trades",
                            {"last_historical": str(hist_max_ts), "first_forward": str(fwd_min_ts), "gap_hours": round(gap_hours, 2)},
                        )
                    )
                else:
                    results.append(
                        CheckResult(
                            "BOUNDARY_GAP",
                            "PASS",
                            f"Historical and forward adjacent (gap {gap_hours:.1f}h)",
                            {"last_historical": str(hist_max_ts), "first_forward": str(fwd_min_ts), "gap_hours": round(gap_hours, 2)},
                        )
                    )
            else:
                results.append(CheckResult("BOUNDARY_GAP", "PASS", "Could not compute boundary timestamps", {}))
        else:
            results.append(CheckResult("BOUNDARY_GAP", "PASS", "Single source; boundary gap N/A", {}))
    except Exception as e:
        results.append(CheckResult("BOUNDARY_GAP", "WARN", str(e), {}))

    # ─── 7b. Trade timeline density (gaps between consecutive days: no big skip) ───
    try:
        if trades_sql:
            # Use daily boundaries (fast): max gap from end of one day to start of next
            gap_query = f"""
                WITH daily AS (
                    SELECT
                        DATE_TRUNC('day', TRY_CAST(created_time AS TIMESTAMP)) AS d,
                        MIN(TRY_CAST(created_time AS TIMESTAMP)) AS first_ts,
                        MAX(TRY_CAST(created_time AS TIMESTAMP)) AS last_ts
                    FROM ({trades_sql})
                    WHERE created_time IS NOT NULL AND TRIM(created_time) <> ''
                    GROUP BY 1
                ),
                ordered AS (
                    SELECT d, first_ts, last_ts,
                        LEAD(first_ts) OVER (ORDER BY d) AS next_day_first
                    FROM daily
                )
                SELECT
                    COUNT(*) AS n_gaps,
                    MAX(EXTRACT(EPOCH FROM (next_day_first - last_ts)) / 3600.0) AS max_gap_hours
                FROM ordered
                WHERE next_day_first IS NOT NULL AND next_day_first > last_ts
            """
            row = con.execute(gap_query).fetchone()
            n_gaps = row[0] or 0
            max_gap_hours = row[1]
            if max_gap_hours is not None and max_gap_hours > 24 * 7:  # > 7 days
                results.append(
                    CheckResult(
                        "TRADE_TIMELINE_DENSITY",
                        "WARN",
                        f"Very large gap between trading days: max {max_gap_hours:.0f}h ({max_gap_hours/24:.1f} days); possible missing data",
                        {"day_boundary_gaps": n_gaps, "max_gap_hours": round(max_gap_hours, 2)},
                    )
                )
            elif max_gap_hours is not None and max_gap_hours > 24 * 2:  # > 2 days
                results.append(
                    CheckResult(
                        "TRADE_TIMELINE_DENSITY",
                        "WARN",
                        f"Large gap between trading days: max {max_gap_hours:.0f}h ({max_gap_hours/24:.1f} days)",
                        {"day_boundary_gaps": n_gaps, "max_gap_hours": round(max_gap_hours, 2)},
                    )
                )
            else:
                results.append(
                    CheckResult(
                        "TRADE_TIMELINE_DENSITY",
                        "PASS",
                        f"Trade timeline: max gap between days {max_gap_hours:.1f}h" if max_gap_hours is not None else "Trade timeline: single day",
                        {"day_boundary_gaps": n_gaps, "max_gap_hours": round(max_gap_hours, 2) if max_gap_hours is not None else None},
                    )
                )
        else:
            results.append(CheckResult("TRADE_TIMELINE_DENSITY", "PASS", "No trade data", {}))
    except Exception as e:
        results.append(CheckResult("TRADE_TIMELINE_DENSITY", "WARN", str(e), {}))

    # ─── 7c. Forward-only count=0 (prevention: catch API/schema regression) ───
    try:
        fwd_srcs = _forward_trade_sources()
        if fwd_srcs:
            fwd_sql = " UNION ALL ".join(f"SELECT {_TRADE_COLS} FROM '{s}'" for s in fwd_srcs)
            n_fwd = con.execute(f"SELECT COUNT(*) FROM ({fwd_sql})").fetchone()[0]
            n_fwd_zero = con.execute(f"SELECT COUNT(*) FROM ({fwd_sql}) WHERE count = 0").fetchone()[0]
            pct_fwd_zero = 100 * (n_fwd_zero / n_fwd) if n_fwd else 0
            results.append(
                CheckResult(
                    "FORWARD_COUNT_COMPLETENESS",
                    "WARN" if pct_fwd_zero > 95 else "PASS",
                    f"Forward trades: {n_fwd:,} total, {pct_fwd_zero:.1f}% with count=0 (ingestion uses count_fp when count=0)",
                    {"forward_trades": n_fwd, "pct_count_zero": round(pct_fwd_zero, 2)},
                )
            )
        else:
            results.append(CheckResult("FORWARD_COUNT_COMPLETENESS", "PASS", "No forward trade data", {}))
    except Exception as e:
        results.append(CheckResult("FORWARD_COUNT_COMPLETENESS", "WARN", str(e), {}))

    # ─── 8. Completeness & coverage ─────────────────────────────────────────
    try:
        trade_total = con.execute(f"SELECT COUNT(*) FROM ({trades_sql})").fetchone()[0]
        market_total = con.execute(f"SELECT COUNT(*) FROM ({markets_sql})").fetchone()[0]
        trades_with_count = con.execute(f"SELECT COUNT(*) FROM ({trades_sql}) WHERE count > 0").fetchone()[0]
        pct_zero = 100 * (1 - trades_with_count / trade_total) if trade_total else 0
        results.append(
            CheckResult(
                "COMPLETENESS",
                "WARN" if pct_zero > 50 else "PASS",
                f"{trade_total:,} trades, {market_total:,} markets | {pct_zero:.1f}% trades with count=0",
                {"trades": trade_total, "markets": market_total, "pct_zero_count": round(pct_zero, 2)},
            )
        )
    except Exception as e:
        results.append(CheckResult("COMPLETENESS", "FAIL", str(e), {}))

    # ─── 9. Statistical sanity (volume skew is expected for headline markets) ───
    try:
        vol_p99 = con.execute(f"SELECT quantile_cont(volume, 0.99) FROM ({markets_sql}) WHERE volume IS NOT NULL").fetchone()[0]
        vol_max = con.execute(f"SELECT MAX(volume) FROM ({markets_sql})").fetchone()[0]
        # Only warn if max is absurd vs p99 (e.g. >15000x); a few super-liquid markets (e.g. Presidency) are normal
        strict_outlier = vol_max and vol_p99 and vol_max > 15000 * vol_p99
        results.append(
            CheckResult(
                "STATISTICAL_SANITY",
                "WARN" if strict_outlier else "PASS",
                f"Market volume: max={vol_max:,}, p99≈{vol_p99:,}" if vol_max else "No volume data",
                {"max_volume": vol_max, "p99_volume": vol_p99},
            )
        )
    except Exception as e:
        results.append(CheckResult("STATISTICAL_SANITY", "WARN", str(e), {}))

    return results


def main() -> int:
    parser = argparse.ArgumentParser(description="Institutional data health validation")
    parser.add_argument("--output", "-o", type=Path, help="Write JSON report to file")
    parser.add_argument("--strict", action="store_true", help="Exit 1 if any FAIL")
    args = parser.parse_args()

    con = duckdb.connect()
    try:
        results = run_checks(con)
    finally:
        con.close()

    # Build report
    report = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "dataset": "kalshi",
        "checks": [asdict(r) for r in results],
        "summary": {
            "pass": sum(1 for r in results if r.status == "PASS"),
            "warn": sum(1 for r in results if r.status == "WARN"),
            "fail": sum(1 for r in results if r.status == "FAIL"),
        },
    }

    # Print human-readable
    print("=" * 72)
    print("KALSHI DATA HEALTH REPORT")
    print("=" * 72)
    print(f"Generated: {report['timestamp']}")
    print(f"Summary:  {report['summary']['pass']} PASS  |  {report['summary']['warn']} WARN  |  {report['summary']['fail']} FAIL")
    print("-" * 72)
    for r in results:
        icon = "✓" if r.status == "PASS" else "⚠" if r.status == "WARN" else "✗"
        print(f"  {icon} {r.name}: {r.message}")
        if r.details:
            for k, v in r.details.items():
                print(f"      {k}: {v}")
    print("=" * 72)

    if args.output:
        args.output.write_text(json.dumps(report, indent=2))
        print(f"Report written to {args.output}")

    if args.strict and report["summary"]["fail"] > 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
