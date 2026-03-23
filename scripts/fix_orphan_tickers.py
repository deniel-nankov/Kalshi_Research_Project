#!/usr/bin/env python3
"""
Fix REFERENTIAL_INTEGRITY: fetch missing markets for orphan tickers (tickers that
appear in trades but not in markets) via GET /markets/{ticker} and append to
forward_markets. Very visible progress.

Usage:
  uv run python scripts/fix_orphan_tickers.py --dry-run   # list orphans only
  uv run python scripts/fix_orphan_tickers.py             # fetch and append (with progress)
  uv run python scripts/fix_orphan_tickers.py --delay 0.3  # delay between API calls (default 0.2)
  uv run python scripts/fix_orphan_tickers.py --max 5000  # cap this run (large sets need many runs)
  uv run python scripts/fix_orphan_tickers.py --checkpoint data/kalshi/state/orphan_backfill_checkpoint.txt
"""

from __future__ import annotations

import importlib.util
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

_SCRIPT_ROOT = Path(__file__).resolve().parents[1]
if str(_SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_ROOT))

import duckdb
import httpx

from src.kalshi_forward.paths import (
    BASE_URL,
    FORWARD_MARKETS_DIR,
    FORWARD_MARKETS_GLOB,
    FORWARD_TRADES_GLOB,
    HISTORICAL_MARKETS_FILE,
    HISTORICAL_TRADES_GLOB,
    LEGACY_FORWARD_MARKETS_GLOB,
    LEGACY_FORWARD_TRADES_GLOB,
    PROJECT_ROOT,
)


def _load_update_forward():
    spec = importlib.util.spec_from_file_location(
        "update_forward", PROJECT_ROOT / "scripts" / "update_forward.py"
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load update_forward.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules["update_forward"] = mod
    spec.loader.exec_module(mod)
    return mod


def _log(msg: str) -> None:
    print(f"  {msg}")


def _section(title: str) -> None:
    print()
    print("=" * 72)
    print(f"  {title}")
    print("=" * 72)


def _has_glob(p: Path) -> bool:
    import glob
    return len(glob.glob(str(p))) > 0


def _get_orphan_tickers(con: duckdb.DuckDBPyConnection) -> list[str]:
    """Return sorted list of tickers that appear in trades but not in markets."""
    # Same column set as validate_data_health for combined queries
    trade_cols = "trade_id, ticker, taker_side, count, yes_price, no_price, price, created_time, count_fp, yes_price_dollars, no_price_dollars"
    market_cols = "ticker, event_ticker, market_type, title, status, volume, created_time, close_time, updated_time, open_interest, dollar_volume"

    trade_srcs = []
    if _has_glob(HISTORICAL_TRADES_GLOB):
        trade_srcs.append(f"SELECT {trade_cols} FROM '{HISTORICAL_TRADES_GLOB}'")
    if _has_glob(FORWARD_TRADES_GLOB):
        trade_srcs.append(f"SELECT {trade_cols} FROM '{FORWARD_TRADES_GLOB}'")
    if _has_glob(LEGACY_FORWARD_TRADES_GLOB):
        trade_srcs.append(f"SELECT {trade_cols} FROM '{LEGACY_FORWARD_TRADES_GLOB}'")
    trades_sql = " UNION ALL ".join(trade_srcs) if trade_srcs else ""

    market_srcs = []
    if HISTORICAL_MARKETS_FILE.exists():
        market_srcs.append(f"SELECT {market_cols} FROM '{HISTORICAL_MARKETS_FILE}'")
    if _has_glob(FORWARD_MARKETS_GLOB):
        market_srcs.append(f"SELECT {market_cols} FROM '{FORWARD_MARKETS_GLOB}'")
    if _has_glob(LEGACY_FORWARD_MARKETS_GLOB):
        market_srcs.append(f"SELECT {market_cols} FROM '{LEGACY_FORWARD_MARKETS_GLOB}'")
    markets_sql = " UNION ALL ".join(market_srcs) if market_srcs else ""
    if not trades_sql or not markets_sql:
        return []

    query = f"""
        SELECT DISTINCT trim(CAST(t.ticker AS VARCHAR)) AS ticker
        FROM ({trades_sql}) t
        WHERE t.ticker IS NOT NULL AND trim(CAST(t.ticker AS VARCHAR)) <> ''
        EXCEPT
        SELECT DISTINCT trim(CAST(m.ticker AS VARCHAR)) AS ticker
        FROM ({markets_sql}) m
        WHERE m.ticker IS NOT NULL AND trim(CAST(m.ticker AS VARCHAR)) <> ''
        ORDER BY ticker
    """
    rows = con.execute(query).fetchall()
    return [r[0] for r in rows]


def _load_checkpoint(path: Path) -> set[str]:
    if not path.exists():
        return set()
    done: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        done.add(line)
    return done


def _append_checkpoint(path: Path, ticker: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(ticker + "\n")


def main() -> int:
    import argparse
    parser = argparse.ArgumentParser(description="Fetch missing markets for orphan tickers")
    parser.add_argument("--dry-run", action="store_true", help="Only list orphan tickers, do not call API")
    parser.add_argument("--delay", type=float, default=0.2, help="Seconds between API calls (default 0.2)")
    parser.add_argument(
        "--max",
        type=int,
        default=None,
        metavar="N",
        help="Process at most N tickers this run (after checkpoint filter)",
    )
    parser.add_argument(
        "--checkpoint",
        type=Path,
        default=None,
        help="Text file: one ticker per line already processed; append on success (resume long runs)",
    )
    parser.add_argument(
        "--batch-rows",
        type=int,
        default=50_000,
        help="Flush Parquet part after this many rows (default 50000)",
    )
    args = parser.parse_args()

    dry = args.dry_run
    if dry:
        _section("DRY RUN — no API calls, no writes")

    # ─── Step 1: Compute orphan tickers ───────────────────────────────────────
    _section("STEP 1: Compute orphan tickers")
    con = duckdb.connect()
    try:
        orphans = _get_orphan_tickers(con)
    finally:
        con.close()

    _log(f"Orphan tickers (in trades but not in markets): {len(orphans):,}")
    if not orphans:
        _log("None. REFERENTIAL_INTEGRITY is already satisfied.")
        return 0

    done: set[str] = set()
    if args.checkpoint:
        done = _load_checkpoint(args.checkpoint)
        if done:
            before = len(orphans)
            orphans = [t for t in orphans if t not in done]
            _log(f"Checkpoint: skipping {before - len(orphans):,} already done; {len(orphans):,} remaining")

    if args.max is not None and len(orphans) > args.max:
        _log(f"Capping this run to --max {args.max:,} tickers ({len(orphans):,} would run otherwise).")
        orphans = orphans[: args.max]

    if not orphans:
        _log("Nothing to do after checkpoint / --max.")
        return 0

    if len(orphans) <= 30:
        for t in orphans:
            _log(f"    {t}")
    else:
        for t in orphans[:15]:
            _log(f"    {t}")
        _log(f"    ... and {len(orphans) - 15} more")

    if dry:
        _section("DRY RUN complete")
        _log(f"Would attempt to fetch {len(orphans):,} markets from API (GET /markets/{{ticker}}).")
        return 0

    # ─── Step 2: Load update_forward for row mapping ───────────────────────────
    _section("STEP 2: Load row mapper and schema")
    uf = _load_update_forward()
    market_row_from_api = uf.market_row_from_api
    MARKET_SCHEMA = uf.MARKET_SCHEMA
    atomic_write_parquet = uf.atomic_write_parquet
    _log("Loaded market_row_from_api and MARKET_SCHEMA from update_forward.py")

    # ─── Step 3: Fetch each orphan from API ───────────────────────────────────
    _section("STEP 3: Fetch markets from API")
    run_id = "orphan_backfill_" + datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    ingested_at = datetime.now(timezone.utc).isoformat()
    url_base = BASE_URL.rstrip("/")
    rows: list[dict] = []
    found = 0
    not_found = 0
    errors = 0
    parts_written = 0
    dt_partition = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_dir = FORWARD_MARKETS_DIR / f"dt={dt_partition}"

    def flush_batch() -> None:
        nonlocal rows, parts_written
        if not rows:
            return
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / f"markets_{run_id}_part{parts_written}.parquet"
        _log(f"Writing part {parts_written} -> {out_file.name} ({len(rows):,} rows)")
        atomic_write_parquet(out_file, rows, schema=MARKET_SCHEMA)
        parts_written += 1
        rows = []

    with httpx.Client(timeout=15.0) as client:
        for i, ticker in enumerate(orphans, 1):
            url = f"{url_base}/markets/{ticker}"
            try:
                resp = client.get(url)
                if resp.status_code == 200:
                    data = resp.json()
                    m = data.get("market") or data
                    row = market_row_from_api(m, run_id=run_id, ingested_at=ingested_at)
                    rows.append(row)
                    found += 1
                    if args.checkpoint:
                        _append_checkpoint(args.checkpoint, ticker)
                    if found <= 10 or found % 100 == 0 or i == len(orphans):
                        _log(f"  [{i}/{len(orphans)}] {ticker}  -> 200 OK (total fetched: {found})")
                    if len(rows) >= args.batch_rows:
                        flush_batch()
                elif resp.status_code == 404:
                    not_found += 1
                    if args.checkpoint:
                        _append_checkpoint(args.checkpoint, ticker)
                    if not_found <= 5:
                        _log(f"  [{i}/{len(orphans)}] {ticker}  -> 404 Not Found")
                else:
                    errors += 1
                    _log(f"  [{i}/{len(orphans)}] {ticker}  -> {resp.status_code} {resp.text[:80]}")
            except Exception as e:
                errors += 1
                _log(f"  [{i}/{len(orphans)}] {ticker}  -> ERROR: {e}")
            time.sleep(args.delay)

    flush_batch()

    _log("")
    _log(f"Summary: fetched={found}, 404={not_found}, errors={errors}")

    if parts_written == 0:
        _section("DONE (nothing to write)")
        _log("No markets could be fetched; dataset unchanged.")
        return 0

    # ─── Step 4: Write summary ─────────────────────────────────────────────────
    _section("STEP 4: Output")
    _log(f"Wrote {parts_written} parquet part(s) under {out_dir}")
    meta = {
        "run_id": run_id,
        "parts": parts_written,
        "fetched": found,
        "not_found": not_found,
        "errors": errors,
    }
    meta_path = out_dir / f"orphan_backfill_{run_id}_meta.json"
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    _log(f"Meta: {meta_path}")

    _section("DONE")
    _log(f"Added markets for {found:,} previously orphan tickers (404/404 also checkpointed when using --checkpoint).")
    _log("Re-run: uv run python scripts/validate_data_health.py")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
