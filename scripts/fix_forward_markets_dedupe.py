#!/usr/bin/env python3
"""
Fix UNIQUENESS_MARKETS: dedupe forward markets by (ticker, close_time) and replace
the forward_markets directory with a single canonical set. Very visible steps.

Use --exclude-historical to also remove forward rows whose (ticker, close_time) already
exist in historical markets, so the combined dataset has no duplicate keys (UNIQUENESS_MARKETS passes).

Usage:
  uv run python scripts/fix_forward_markets_dedupe.py --dry-run   # show what would happen
  uv run python scripts/fix_forward_markets_dedupe.py --yes       # backup, dedupe, replace (no prompt)
  uv run python scripts/fix_forward_markets_dedupe.py --yes --exclude-historical  # also drop overlap with historical
"""

from __future__ import annotations

import glob
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

_SCRIPT_ROOT = Path(__file__).resolve().parents[1]
if str(_SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_ROOT))

import duckdb

from src.kalshi_forward.paths import (
    FORWARD_MARKETS_DIR,
    FORWARD_MARKETS_GLOB,
    HISTORICAL_MARKETS_FILE,
    KALSHI_DATA_DIR,
    LEGACY_FORWARD_MARKETS_GLOB,
    PROJECT_ROOT,
)


def _log(msg: str) -> None:
    print(f"  {msg}")


def _section(title: str) -> None:
    print()
    print("=" * 72)
    print(f"  {title}")
    print("=" * 72)


def main() -> int:
    import argparse
    parser = argparse.ArgumentParser(description="Dedupe forward markets and replace directory")
    parser.add_argument("--dry-run", action="store_true", help="Only show what would be done")
    parser.add_argument("--yes", "-y", action="store_true", help="Skip confirmation prompt")
    parser.add_argument(
        "--exclude-historical",
        action="store_true",
        help="Drop forward rows whose (ticker, close_time) exist in historical; combined set will have no duplicate keys",
    )
    args = parser.parse_args()

    dry = args.dry_run
    if dry:
        _section("DRY RUN — no files will be changed")

    # ─── Step 1: Current state (forward + legacy so we dedupe everything) ───────
    _section("STEP 1: Current state")
    patterns = [str(FORWARD_MARKETS_GLOB), str(LEGACY_FORWARD_MARKETS_GLOB)]
    files = []
    for p in patterns:
        files.extend(glob.glob(p))
    files = sorted(set(files))
    legacy_dir = KALSHI_DATA_DIR / "incremental" / "markets"
    if not files:
        _log("No forward market files found. Nothing to do.")
        return 0

    con = duckdb.connect()
    try:
        union_sql = " UNION ALL ".join(f"SELECT * FROM read_parquet('{f}')" for f in files)
        total_before = con.execute(f"SELECT COUNT(*) FROM ({union_sql})").fetchone()[0]
        distinct_before = con.execute(
            f"""
            SELECT COUNT(DISTINCT ticker || '|' || COALESCE(close_time, ''))
            FROM ({union_sql})
            WHERE ticker IS NOT NULL AND ticker <> ''
            """
        ).fetchone()[0]
        dupes = total_before - distinct_before
    finally:
        con.close()

    _log(f"Forward market files:     {len(files)}")
    _log(f"Total rows:               {total_before:,}")
    _log(f"Distinct (ticker, close): {distinct_before:,}")
    _log(f"Duplicate keys (within forward): {dupes:,}")
    if dupes == 0 and not args.exclude_historical:
        _log("No within-forward duplicates; run with --exclude-historical to remove overlap with historical.")
        return 0

    # ─── Step 2: Backup ───────────────────────────────────────────────────────
    _section("STEP 2: Backup")
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    backup_dir = PROJECT_ROOT / "data" / "kalshi" / "historical" / f"forward_markets_backup_{timestamp}"
    _log(f"Backup path: {backup_dir}")
    if not dry:
        if backup_dir.exists():
            _log("Backup dir already exists; aborting to avoid overwrite.")
            return 1
        shutil.copytree(FORWARD_MARKETS_DIR, backup_dir)
        _log("Backup created.")
    else:
        _log("[DRY-RUN] Would create backup and copy current forward_markets there.")

    # ─── Step 3: Dedupe (and optionally exclude historical overlap) ────────────
    _section("STEP 3: Dedupe and write staging")
    staging_dir = PROJECT_ROOT / "data" / "kalshi" / "historical" / "forward_markets_dedupe_staging"
    canonical_partition = staging_dir / "dt=canonical"
    out_file = canonical_partition / "markets.parquet"
    _log(f"Staging path: {out_file}")
    if args.exclude_historical:
        _log("Excluding rows whose (ticker, close_time) exist in historical markets.")

    con = duckdb.connect()
    try:
        union_sql = " UNION ALL ".join(f"SELECT * FROM read_parquet('{f}')" for f in files)
        dedupe_sql = f"""
            SELECT * EXCLUDE (row_num)
            FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY ticker, COALESCE(close_time, '')) AS row_num
                FROM ({union_sql})
            ) WHERE row_num = 1
        """
        if args.exclude_historical and HISTORICAL_MARKETS_FILE.exists():
            hist_path = str(HISTORICAL_MARKETS_FILE)
            # Use same key as validator: ticker || '|' || COALESCE(close_time, created_time, '')
            # Keep only forward rows whose key is NOT in historical
            final_sql = f"""
                SELECT f.* FROM ({dedupe_sql}) f
                WHERE (f.ticker || '|' || COALESCE(f.close_time, f.created_time, '')) NOT IN (
                    SELECT ticker || '|' || COALESCE(close_time, created_time, '')
                    FROM read_parquet('{hist_path}')
                    WHERE ticker IS NOT NULL AND ticker <> ''
                )
            """
        else:
            final_sql = dedupe_sql
            if args.exclude_historical and not HISTORICAL_MARKETS_FILE.exists():
                _log("Warning: historical markets file not found; skipping exclude step.")

        total_after = con.execute(f"SELECT COUNT(*) FROM ({final_sql})").fetchone()[0]
        if args.exclude_historical and HISTORICAL_MARKETS_FILE.exists():
            overlap_removed = con.execute(f"SELECT COUNT(*) FROM ({dedupe_sql})").fetchone()[0] - total_after
            _log(f"Rows after excluding historical overlap: {total_after:,} (removed {overlap_removed:,} overlapping)")
        if not dry:
            canonical_partition.mkdir(parents=True, exist_ok=True)
            con.execute(f"COPY ({final_sql}) TO '{out_file}' (FORMAT PARQUET)")
    finally:
        con.close()

    _log(f"Deduped rows (to write):  {total_after:,}")
    _log(f"Within-forward dupes removed: {total_before - distinct_before:,}")

    if dry:
        _section("DRY RUN complete")
        _log("Run without --dry-run to perform backup and replace.")
        return 0

    # ─── Step 4: Replace forward_markets ──────────────────────────────────────
    _section("STEP 4: Replace forward_markets")
    _log(f"Current forward_markets:  {FORWARD_MARKETS_DIR}")
    _log("Will remove all contents and replace with staging/dt=canonical/")
    if not args.yes:
        try:
            reply = input("  Proceed? [y/N]: ").strip().lower()
        except EOFError:
            reply = "n"
        if reply != "y" and reply != "yes":
            _log("Aborted.")
            return 0

    # Remove everything under forward_markets, then copy canonical partition in
    for child in FORWARD_MARKETS_DIR.iterdir():
        if child.is_dir():
            shutil.rmtree(child)
        else:
            child.unlink()
    (FORWARD_MARKETS_DIR / "dt=canonical").mkdir(parents=True)
    shutil.copy2(out_file, FORWARD_MARKETS_DIR / "dt=canonical" / "markets.parquet")
    _log("Replaced. forward_markets now contains only dt=canonical/markets.parquet")

    # Optional: remove staging dir to avoid confusion
    shutil.rmtree(staging_dir)
    _log("Staging directory removed.")

    # ─── Step 5: Archive legacy forward markets (so validator sees no duplicates) ─
    if legacy_dir.exists():
        _section("STEP 5: Archive legacy forward markets")
        legacy_backup = KALSHI_DATA_DIR / f"incremental_markets_backup_{timestamp}"
        _log(f"Moving {legacy_dir} -> {legacy_backup} so validator does not see duplicate rows.")
        shutil.move(str(legacy_dir), str(legacy_backup))
        _log("Legacy markets archived.")

    _section("DONE")
    _log(f"Backup:     {backup_dir}")
    _log(f"Rows now:   {total_after:,} (was {total_before:,}, removed {total_before - total_after:,} duplicates)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
