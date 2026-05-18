#!/usr/bin/env python3
# ruff: noqa: E402
"""
Capture Kalshi forecast_percentile_history for numeric events.

Why this script exists
----------------------
Kalshi publishes their own forecast distribution for numeric events at
percentile points over time, via:
  GET /trade-api/v2/series/{series_ticker}/events/{ticker}/forecast_percentile_history

This is Kalshi's "official forecast" we can compare to actual traded prices
to detect forecast-deviation alpha. When traded price diverges sharply from
the Kalshi forecast, that's a strong informed-flow signal.

Scope: this endpoint is meaningful for NUMERIC events (inflation prints,
crypto price targets, indicator releases). Binary categorical events don't
have a percentile distribution.

What it does
------------
1. Enumerate active numeric events from forward_markets (filtering by
   series category / market type heuristics)
2. For each, fetch percentile history at a configurable period interval
3. Write one row per (event, ts, percentile) point
4. SHA-256 sidecar + audit log

Usage
-----
  uv run python scripts/capture_forecast_percentile.py
  uv run python scripts/capture_forecast_percentile.py --series KXNFLGAME
  uv run python scripts/capture_forecast_percentile.py --start-ts 1747000000 --end-ts 1747086400
  uv run python scripts/capture_forecast_percentile.py --period-interval hour

Designed to run once per day from systemd timer (this data updates slowly).
"""

from __future__ import annotations

import argparse
import concurrent.futures
import glob
import hashlib
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = PROJECT_ROOT / "data" / "kalshi"
HISTORICAL = DATA_ROOT / "historical"
STATE = DATA_ROOT / "state"
MARKETS_DIR = HISTORICAL / "forward_markets"
OUT_DIR = HISTORICAL / "forward_forecast_percentile"
CHECKPOINT = STATE / "forecast_percentile_checkpoint.json"
AUDIT_DIR = STATE / "surveillance_runs"

API_BASE = os.environ.get("KALSHI_API_BASE", "https://api.elections.kalshi.com/trade-api/v2")
DEFAULT_PERCENTILES = [10, 25, 50, 75, 90]
DEFAULT_PERIOD_INTERVAL = "hour"   # minute | hour | day per docs
DEFAULT_TIMEOUT_S = 25
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 1.0


SCHEMA = pa.schema([
    pa.field("event_ticker",         pa.string()),
    pa.field("series_ticker",        pa.string()),
    pa.field("end_period_ts_unix",   pa.int64()),
    pa.field("end_period_iso",       pa.string()),
    pa.field("period_interval",      pa.string()),
    pa.field("percentile",           pa.int32()),
    pa.field("raw_forecast",         pa.float64()),
    pa.field("numerical_forecast",   pa.float64()),
    pa.field("formatted_forecast",   pa.string()),
    pa.field("fetched_at_unix",      pa.int64()),
    pa.field("run_id",               pa.string()),
])


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


# -------------------- target enumeration --------------------

def _list_target_events(series_filter: Optional[str], category_filter: Optional[str]) -> list[tuple[str, str]]:
    """
    Read forward_markets to find (series_ticker, event_ticker) pairs that
    appear to be numeric (heuristic: market_type contains scalar/numeric or
    event ticker matches known numeric series patterns).

    Returns deduped (series_ticker, event_ticker) pairs.
    """
    files = sorted(glob.glob(str(MARKETS_DIR / "dt=*/*.parquet")))
    files = [f for f in files if not f.endswith(".tmp")]
    if not files:
        _log("WARN  no forward_markets parquets — cannot enumerate numeric events")
        return []
    seen: set[tuple[str, str]] = set()
    # Read minimal columns; project just the latest snapshot per event_ticker
    for fp in files:
        try:
            tab = pq.read_table(fp, columns=["event_ticker", "series_ticker", "market_type", "category"]).to_pylist()
        except Exception:
            try:
                tab = pq.read_table(fp, columns=["event_ticker", "series_ticker"]).to_pylist()
            except Exception:
                continue
        for row in tab:
            ev = row.get("event_ticker")
            sr = row.get("series_ticker")
            if not ev or not sr:
                continue
            if series_filter and series_filter.upper() not in str(sr).upper():
                continue
            if category_filter and str(row.get("category", "")).lower() != category_filter.lower():
                continue
            mt = (row.get("market_type") or "").lower()
            # Heuristic: keep events whose market_type indicates a numeric / scalar shape
            # (binary categorical markets don't have a percentile forecast). We also keep
            # everything if --no-numeric-filter via env (and the API will return empty).
            if mt and not any(k in mt for k in ("scalar", "numeric", "range", "value")):
                continue
            seen.add((sr, ev))
    return sorted(seen)


# -------------------- API fetch --------------------

def _fetch_one(client: httpx.Client, series: str, event: str, percentiles: list[int],
               start_ts: Optional[int], end_ts: Optional[int], period_interval: str) -> tuple[int, Optional[dict[str, Any]], Optional[str]]:
    url = f"{API_BASE}/series/{series}/events/{event}/forecast_percentile_history"
    params: dict[str, Any] = {"period_interval": period_interval}
    # API accepts repeated `percentiles` parameter
    params_multi = [("percentiles", str(p)) for p in percentiles]
    if start_ts is not None:
        params["start_ts"] = int(start_ts)
    if end_ts is not None:
        params["end_ts"] = int(end_ts)
    last_err: Optional[Exception] = None
    for attempt in range(MAX_RETRIES):
        try:
            # httpx supports list of tuples for repeated params via httpx.QueryParams
            qp = httpx.QueryParams([*params.items(), *params_multi])
            r = client.get(url, params=qp, timeout=DEFAULT_TIMEOUT_S)
            if r.status_code == 404:
                return 404, None, "not_found"
            if r.status_code == 429:
                time.sleep(RETRY_BACKOFF_BASE * (2 ** attempt))
                continue
            r.raise_for_status()
            return r.status_code, r.json(), None
        except (httpx.HTTPError, httpx.TimeoutException) as exc:
            last_err = exc
            time.sleep(RETRY_BACKOFF_BASE * (2 ** attempt))
    return 0, None, repr(last_err) if last_err else "unknown"


def _normalize_rows(series: str, event: str, body: dict[str, Any], period_interval: str, run_id: str, fetched_at: int) -> list[dict[str, Any]]:
    """
    Per docs the response includes: event_ticker, end_period_ts, period_interval,
    and an array of points each containing percentile + raw_numerical_forecast +
    numerical_forecast + formatted_forecast.

    We emit ONE ROW per (point_end_period_ts × percentile).
    """
    rows: list[dict[str, Any]] = []
    # The exact shape depends on Kalshi response; we handle two common shapes.
    # Shape A: { "forecast_percentile_history": [ {end_period_ts, percentiles: [{percentile, ...}]}, ... ] }
    # Shape B: { "history": [...] }
    history = body.get("forecast_percentile_history") or body.get("history") or []
    if isinstance(history, dict):
        # sometimes returned as { "points": [...] }
        history = history.get("points") or []
    for point in history:
        if not isinstance(point, dict):
            continue
        end_ts = point.get("end_period_ts") or point.get("ts") or point.get("end_period_ts_unix")
        try:
            end_ts_unix = int(end_ts) if end_ts is not None else 0
        except Exception:
            end_ts_unix = 0
        end_iso = datetime.fromtimestamp(end_ts_unix, tz=timezone.utc).isoformat() if end_ts_unix else ""
        pts = point.get("percentile_points") or point.get("percentiles") or []
        for pp in pts:
            if not isinstance(pp, dict):
                continue
            try:
                pct = int(pp.get("percentile"))
            except Exception:
                continue
            rows.append({
                "event_ticker":       event,
                "series_ticker":      series,
                "end_period_ts_unix": end_ts_unix,
                "end_period_iso":     end_iso,
                "period_interval":    period_interval,
                "percentile":         pct,
                "raw_forecast":       _f(pp.get("raw_numerical_forecast", pp.get("raw_forecast"))),
                "numerical_forecast": _f(pp.get("numerical_forecast")),
                "formatted_forecast": pp.get("formatted_forecast"),
                "fetched_at_unix":    fetched_at,
                "run_id":             run_id,
            })
    return rows


def _f(x: Any) -> Optional[float]:
    if x is None or x == "":
        return None
    try:
        return float(x)
    except Exception:
        return None


# -------------------- writer --------------------

def _write_parquet(rows: list[dict[str, Any]], run_id: str) -> Optional[Path]:
    if not rows:
        return None
    dt = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_dir = OUT_DIR / f"dt={dt}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"forecast_percentile_{run_id}.parquet"
    cols: dict[str, list[Any]] = {f.name: [] for f in SCHEMA}
    for r in rows:
        for fname in cols:
            cols[fname].append(r.get(fname))
    table = pa.Table.from_pydict(cols, schema=SCHEMA)
    tmp = out_file.with_suffix(".parquet.tmp")
    pq.write_table(table, tmp, compression="zstd")
    tmp.replace(out_file)
    _write_sha256_sidecar(out_file)
    return out_file


def _write_audit_log(run_id: str, payload: dict[str, Any]) -> None:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    out = AUDIT_DIR / f"forecast_percentile_{run_id}.json"
    tmp = out.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, out)


# -------------------- main --------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="Capture Kalshi forecast percentile history")
    ap.add_argument("--series",         type=str, default=None, help="Series ticker substring filter")
    ap.add_argument("--category",       type=str, default=None, help="Category filter (e.g. financial)")
    ap.add_argument("--percentiles",    type=str, default=",".join(str(p) for p in DEFAULT_PERCENTILES))
    ap.add_argument("--start-ts",       type=int, default=None)
    ap.add_argument("--end-ts",         type=int, default=None)
    ap.add_argument("--period-interval", type=str, default=DEFAULT_PERIOD_INTERVAL,
                    choices=["minute", "hour", "day"])
    ap.add_argument("--concurrency",    type=int, default=4,
                    help="Parallel HTTP workers (default 4; this endpoint can be slow)")
    ap.add_argument("--max-events",     type=int, default=0,
                    help="Cap number of events processed (0 = no cap)")
    ap.add_argument("--dry-run",        action="store_true")
    args = ap.parse_args()

    percentiles = [int(p) for p in args.percentiles.split(",") if p.strip()]
    run_id = f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"
    fetched_at = int(time.time())
    started = time.time()

    targets = _list_target_events(args.series, args.category)
    if args.max_events > 0:
        targets = targets[: args.max_events]

    _log("=" * 72)
    _log(f"capture_forecast_percentile run_id={run_id}")
    _log(f"  base             = {API_BASE}")
    _log(f"  percentiles      = {percentiles}")
    _log(f"  period_interval  = {args.period_interval}")
    _log(f"  series_filter    = {args.series or 'ALL'}")
    _log(f"  category_filter  = {args.category or 'ALL'}")
    _log(f"  target events    = {len(targets):,}")
    if args.dry_run:
        _log("  DRY-RUN — no writes")
    _log("=" * 72)

    if not targets:
        _log("no target events; exiting")
        return 0

    rows: list[dict[str, Any]] = []
    n_ok = n_404 = n_err = n_empty = 0
    with httpx.Client(timeout=DEFAULT_TIMEOUT_S, limits=httpx.Limits(max_connections=args.concurrency)) as client:
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as ex:
            futures = {
                ex.submit(_fetch_one, client, sr, ev, percentiles, args.start_ts, args.end_ts, args.period_interval): (sr, ev)
                for sr, ev in targets
            }
            for fut in concurrent.futures.as_completed(futures):
                sr, ev = futures[fut]
                try:
                    status, body, err = fut.result()
                except Exception as exc:
                    status, body, err = 0, None, repr(exc)
                if status == 200 and body:
                    new_rows = _normalize_rows(sr, ev, body, args.period_interval, run_id, fetched_at)
                    if new_rows:
                        rows.extend(new_rows)
                        n_ok += 1
                    else:
                        n_empty += 1
                elif status == 404:
                    n_404 += 1
                else:
                    n_err += 1

    _log(f"events: ok={n_ok}  404={n_404}  empty={n_empty}  err={n_err}  total_rows={len(rows):,}")

    if args.dry_run:
        _log("DRY-RUN  done")
        return 0

    out_file = _write_parquet(rows, run_id)
    _log(f"  wrote {len(rows):,} rows -> {out_file.name if out_file else '(none)'}")

    payload = {
        "run_id": run_id,
        "started_at_utc": datetime.fromtimestamp(started, tz=timezone.utc).isoformat(),
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "duration_seconds": round(time.time() - started, 3),
        "percentiles": percentiles,
        "period_interval": args.period_interval,
        "start_ts": args.start_ts,
        "end_ts": args.end_ts,
        "series_filter": args.series,
        "category_filter": args.category,
        "target_events": len(targets),
        "events_ok": n_ok,
        "events_404": n_404,
        "events_empty": n_empty,
        "events_error": n_err,
        "rows_written": len(rows),
        "output_file": str(out_file.relative_to(PROJECT_ROOT)) if out_file else None,
        "api_base": API_BASE,
    }
    _write_audit_log(run_id, payload)

    _log("=" * 72)
    _log(f"DONE rows={len(rows):,}  ok_events={n_ok}/{len(targets)}  duration={time.time()-started:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
