#!/usr/bin/env python3
# ruff: noqa: E402
"""
Capture Kalshi live-data (play-by-play) per active milestone.

Why this script exists
----------------------
Kalshi exposes `GET /live_data/milestone/{milestone_id}` which returns
real-time play-by-play data for sports games, election milestones and
similar. Pulling this at 30-second cadence gives us **event-time ground
truth** — the exact moment of each goal / play / score change.

Combined with our trade stream, this enables the textbook insider-trade
signature: "trade fired N seconds before a play-changing event."

What it does
------------
1. Reads the current active milestones from forward_milestones (latest
   parquet per partition) and filters to milestones that are:
   - within their start_date/end_date window
   - sports / live categories (configurable)
2. For each active milestone, fetches /live_data/milestone/{id} and writes
   one row per fetch (full snapshot) to the live-data parquet.
3. Each parquet has a co-located SHA-256 sidecar.
4. Audit log entry per run.

Live-data response shape (per Kalshi docs):
  { live_data: { type: str, details: dict, milestone_id: str } }
  For sports types: details contains `pbp` (play-by-play) organized by
  periods, each period has an `events` array.

We store the FULL `live_data` blob as JSON in the parquet so we can
re-parse downstream without re-fetching. We ALSO denormalize key fields
(type, last seen pbp event count, current period) for fast SQL filtering.

Usage
-----
  uv run python scripts/capture_live_data.py            # one-shot pass
  uv run python scripts/capture_live_data.py --categories Sports,Esports
  uv run python scripts/capture_live_data.py --concurrency 10
  uv run python scripts/capture_live_data.py --milestone <id>  # single
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
MILESTONES_DIR = HISTORICAL / "forward_milestones"
OUT_DIR = HISTORICAL / "forward_live_data"
CHECKPOINT = STATE / "live_data_checkpoint.json"
AUDIT_DIR = STATE / "surveillance_runs"

API_BASE = os.environ.get("KALSHI_API_BASE", "https://api.elections.kalshi.com/trade-api/v2")
DEFAULT_TIMEOUT_S = 20
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 0.8


SCHEMA = pa.schema([
    pa.field("milestone_id",        pa.string()),
    pa.field("milestone_type",      pa.string()),     # e.g. football_game
    pa.field("milestone_category",  pa.string()),     # e.g. Sports
    pa.field("milestone_title",     pa.string()),
    pa.field("data_type",           pa.string()),     # live_data.type
    pa.field("fetched_at_unix",     pa.int64()),
    pa.field("fetched_at_iso",      pa.string()),
    pa.field("period_count",        pa.int32()),      # # of periods in pbp
    pa.field("events_count",        pa.int32()),      # # of pbp events across all periods
    pa.field("latest_event_summary", pa.string()),    # short text desc of most recent event (best-effort)
    pa.field("primary_event_tickers", pa.list_(pa.string())),  # for fast join to trades
    pa.field("live_data_json",      pa.string()),     # full JSON blob for downstream re-parse
    pa.field("http_status",         pa.int32()),
    pa.field("error",               pa.string()),
    pa.field("run_id",              pa.string()),
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


def _load_checkpoint() -> dict[str, Any]:
    if CHECKPOINT.exists():
        try:
            return json.loads(CHECKPOINT.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def _save_checkpoint(state: dict[str, Any]) -> None:
    CHECKPOINT.parent.mkdir(parents=True, exist_ok=True)
    tmp = CHECKPOINT.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, CHECKPOINT)


# -------------------- active milestone selection --------------------

def _list_active_milestones(
    categories: Optional[list[str]],
    explicit_ids: Optional[list[str]],
) -> list[dict[str, Any]]:
    """
    Find all milestones with start_date <= now < end_date (or no end_date),
    reading the latest parquet rows from forward_milestones.
    """
    if explicit_ids:
        # Build placeholder rows for explicit IDs (we won't have metadata
        # until /live_data returns); minimal projection
        return [{"id": mid, "type": "", "category": "", "title": "",
                 "primary_event_tickers": []} for mid in explicit_ids]

    files = sorted(glob.glob(str(MILESTONES_DIR / "dt=*/*.parquet")))
    files = [f for f in files if not f.endswith(".tmp")]
    if not files:
        _log("WARN  no forward_milestones parquets found — run capture_milestones.py first")
        return []

    now_unix = int(time.time())
    out: dict[str, dict[str, Any]] = {}
    # Read latest snapshot of each milestone (highest last_updated_unix wins)
    for fp in files:
        tab = pq.read_table(fp, columns=[
            "id", "category", "type", "title", "start_date", "end_date",
            "primary_event_tickers", "last_updated_unix",
        ]).to_pylist()
        for row in tab:
            mid = row.get("id")
            if not mid:
                continue
            existing = out.get(mid)
            if existing is None or row["last_updated_unix"] >= existing.get("last_updated_unix", 0):
                out[mid] = row

    def _start_unix(s: Any) -> int:
        if not s:
            return 0
        try:
            return int(datetime.fromisoformat(str(s).replace("Z", "+00:00")).timestamp())
        except Exception:
            return 0

    def _end_unix(s: Any) -> int:
        if not s:
            return 9_999_999_999  # treat missing end_date as far future
        try:
            return int(datetime.fromisoformat(str(s).replace("Z", "+00:00")).timestamp())
        except Exception:
            return 9_999_999_999

    rows = []
    for row in out.values():
        if categories and row.get("category") not in categories:
            continue
        start = _start_unix(row.get("start_date"))
        end = _end_unix(row.get("end_date"))
        # window: from 30 min before start until 2 hours after end
        if (start - 1800) <= now_unix <= (end + 7200):
            rows.append(row)
    return rows


# -------------------- fetch live-data per milestone --------------------

def _fetch_live(client: httpx.Client, milestone_id: str, include_player_stats: bool) -> tuple[int, Optional[dict[str, Any]], Optional[str]]:
    url = f"{API_BASE}/live_data/milestone/{milestone_id}"
    params = {"include_player_stats": "true" if include_player_stats else "false"}
    last_err: Optional[Exception] = None
    for attempt in range(MAX_RETRIES):
        try:
            r = client.get(url, params=params, timeout=DEFAULT_TIMEOUT_S)
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


def _summarize_pbp(live_data: dict[str, Any]) -> tuple[int, int, str]:
    """Return (period_count, total_events, latest_event_summary)."""
    if not live_data:
        return 0, 0, ""
    details = live_data.get("details") or {}
    pbp = details.get("pbp") or details.get("play_by_play") or []
    if not isinstance(pbp, list):
        return 0, 0, ""
    period_count = len(pbp)
    total_events = 0
    latest = ""
    for period in pbp:
        events = (period or {}).get("events", []) or []
        total_events += len(events)
        if events:
            ev = events[-1]
            # Best-effort short summary — text or summary fields if they exist
            for key in ("text", "summary", "description", "event_type", "type"):
                val = ev.get(key) if isinstance(ev, dict) else None
                if val:
                    latest = str(val)[:200]
                    break
    return period_count, total_events, latest


def _build_row(milestone: dict[str, Any], status: int, body: Optional[dict[str, Any]], error: Optional[str], run_id: str, fetched_at: int) -> dict[str, Any]:
    live = (body or {}).get("live_data") or {}
    period_count, events_count, latest = _summarize_pbp(live)
    return {
        "milestone_id":         milestone.get("id", ""),
        "milestone_type":       milestone.get("type") or "",
        "milestone_category":   milestone.get("category") or "",
        "milestone_title":      milestone.get("title") or "",
        "data_type":            live.get("type") or "",
        "fetched_at_unix":      fetched_at,
        "fetched_at_iso":       datetime.fromtimestamp(fetched_at, tz=timezone.utc).isoformat(),
        "period_count":         int(period_count),
        "events_count":         int(events_count),
        "latest_event_summary": latest,
        "primary_event_tickers": list(milestone.get("primary_event_tickers") or []),
        "live_data_json":       json.dumps(live, sort_keys=True) if live else None,
        "http_status":          int(status),
        "error":                error,
        "run_id":                run_id,
    }


# -------------------- writer --------------------

def _write_parquet(rows: list[dict[str, Any]], run_id: str) -> Path:
    if not rows:
        return None  # type: ignore[return-value]
    dt = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_dir = OUT_DIR / f"dt={dt}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"live_data_{run_id}.parquet"
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
    out = AUDIT_DIR / f"live_data_{run_id}.json"
    tmp = out.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, out)


# -------------------- main --------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="Capture Kalshi live-data per active milestone")
    ap.add_argument("--categories", type=str, default="Sports,Esports",
                    help="Comma-separated category filter (default: Sports,Esports)")
    ap.add_argument("--milestone",  type=str, default=None,
                    help="Single milestone_id (skips active-window logic)")
    ap.add_argument("--concurrency", type=int, default=8,
                    help="Parallel HTTP workers (default 8; keep low to avoid 429)")
    ap.add_argument("--no-player-stats", action="store_true",
                    help="Skip include_player_stats=true (smaller payloads)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    run_id = f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"
    fetched_at = int(time.time())
    started = time.time()

    categories = [c.strip() for c in (args.categories or "").split(",") if c.strip()] or None
    explicit_ids = [args.milestone] if args.milestone else None

    milestones = _list_active_milestones(categories, explicit_ids)

    _log("=" * 72)
    _log(f"capture_live_data run_id={run_id}")
    _log(f"  base        = {API_BASE}")
    _log(f"  categories  = {categories or 'ALL'}")
    _log(f"  active set  = {len(milestones):,} milestones")
    _log(f"  concurrency = {args.concurrency}")
    if args.dry_run:
        _log("  DRY-RUN — no writes")
    _log("=" * 72)

    if not milestones:
        _log("no active milestones — exiting")
        return 0

    include_ps = not args.no_player_stats

    # Fetch concurrently
    rows: list[dict[str, Any]] = []
    n_ok = n_404 = n_err = 0
    with httpx.Client(timeout=DEFAULT_TIMEOUT_S, limits=httpx.Limits(max_connections=args.concurrency)) as client:
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as ex:
            futures = {
                ex.submit(_fetch_live, client, m["id"], include_ps): m
                for m in milestones
            }
            for fut in concurrent.futures.as_completed(futures):
                m = futures[fut]
                try:
                    status, body, err = fut.result()
                except Exception as exc:
                    status, body, err = 0, None, repr(exc)
                if status == 200 and body is not None:
                    n_ok += 1
                elif status == 404:
                    n_404 += 1
                else:
                    n_err += 1
                rows.append(_build_row(m, status, body, err, run_id, fetched_at))

    _log(f"fetched {len(rows)}  ok={n_ok}  404={n_404}  err={n_err}")

    if args.dry_run:
        _log("DRY-RUN  would write 1 parquet")
        return 0

    out_file = _write_parquet(rows, run_id)
    _log(f"  wrote {len(rows):,} rows -> {out_file.name if out_file else '(none)'}")

    payload = {
        "run_id": run_id,
        "started_at_utc": datetime.fromtimestamp(started, tz=timezone.utc).isoformat(),
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "duration_seconds": round(time.time() - started, 3),
        "categories": categories,
        "explicit_milestone_id": args.milestone,
        "active_milestones": len(milestones),
        "rows_fetched": len(rows),
        "rows_ok": n_ok,
        "rows_404": n_404,
        "rows_error": n_err,
        "include_player_stats": include_ps,
        "concurrency": args.concurrency,
        "output_file": str(out_file.relative_to(PROJECT_ROOT)) if out_file else None,
        "api_base": API_BASE,
    }
    _write_audit_log(run_id, payload)

    state = _load_checkpoint()
    state["last_run_completed_at"] = datetime.now(timezone.utc).isoformat()
    state["last_run_id"] = run_id
    state["last_run_rows"] = len(rows)
    _save_checkpoint(state)

    _log("=" * 72)
    _log(f"DONE rows={len(rows):,}  ok={n_ok}  404={n_404}  err={n_err}  duration={time.time()-started:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
