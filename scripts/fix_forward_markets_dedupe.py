#!/usr/bin/env python3
# ruff: noqa: E402
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
import math
import os
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

_SCRIPT_ROOT = Path(__file__).resolve().parents[1]
if str(_SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_ROOT))

import duckdb

from src.kalshi_forward.duckdb_heavy import connect_for_dedupe_spill
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


def _parquet_path_sql(path: Path | str) -> str:
    s = str(Path(path).resolve()).replace("\\", "/")
    return s.replace("'", "''")


def _union_all_parquet(files: list[str]) -> str:
    parts = [_parquet_path_sql(f) for f in files]
    return " UNION ALL ".join(f"SELECT * FROM read_parquet('{p}')" for p in parts)


def _split_parquet_if_oversized(path: Path, *, max_bytes: int = 1_900_000_000) -> None:
    """GitHub LFS rejects objects > 2 GiB; split large canonical outputs into row shards."""
    if not path.exists() or path.stat().st_size <= max_bytes:
        return
    con = duckdb.connect()
    try:
        n = con.execute("SELECT count(*) FROM read_parquet(?)", [str(path)]).fetchone()[0]
        parts = max(2, int(math.ceil(path.stat().st_size / float(max_bytes))))
        chunk = (n + parts - 1) // parts
        srcp = _parquet_path_sql(path)
        parent = path.parent
        for i in range(parts):
            off = i * chunk
            lim = min(chunk, n - off)
            if lim <= 0:
                break
            out = parent / f"markets_canonical_part{i:02d}.parquet"
            outp = _parquet_path_sql(out)
            con.execute(
                f"COPY (SELECT * FROM read_parquet('{srcp}') LIMIT {lim} OFFSET {off}) "
                f"TO '{outp}' (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
    finally:
        con.close()
    path.unlink()


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
    parser.add_argument(
        "--memory-limit-gb",
        type=float,
        default=None,
        metavar="G",
        help="DuckDB RAM cap per heavy sub-step (default: env KALSHI_FIX_MARKETS_MEMORY_GIB or 28)",
    )
    parser.add_argument(
        "--temp-directory",
        type=Path,
        default=None,
        help="Spill directory for heavy step (default: data/kalshi/state/duckdb_fix_markets_spill)",
    )
    args = parser.parse_args()

    mem_gb = args.memory_limit_gb
    if mem_gb is None:
        raw = os.environ.get("KALSHI_FIX_MARKETS_MEMORY_GIB", "28").strip()
        try:
            mem_gb = float(raw) if raw else 28.0
        except ValueError:
            mem_gb = 28.0
    args.memory_limit_gb = min(max(mem_gb, 4.0), 62.0)

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

    union_sql = _union_all_parquet(files)
    con = duckdb.connect()
    try:
        total_before = con.execute(f"SELECT COUNT(*) FROM ({union_sql})").fetchone()[0]
        distinct_before = con.execute(
            f"""
            SELECT COUNT(DISTINCT ticker || '|' || COALESCE(close_time, created_time, ''))
            FROM ({union_sql})
            WHERE ticker IS NOT NULL AND ticker <> ''
            """
        ).fetchone()[0]
        dupes = total_before - distinct_before
    finally:
        con.close()

    _log(f"Forward market files:     {len(files)}")
    _log(f"Total rows:               {total_before:,}")
    _log(f"Distinct market key:      {distinct_before:,}")
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
        _log("Excluding rows whose market key exists in historical markets (join-based; spills to disk).")

    # Forward market files do not include a `filename` column (unlike read_parquet(..., filename=true)).
    dedupe_window_sql = f"""
            SELECT * EXCLUDE (row_num)
            FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY ticker, COALESCE(close_time, created_time, '')
                    ORDER BY
                        TRY_CAST(updated_time AS TIMESTAMP) DESC NULLS LAST,
                        TRY_CAST(created_time AS TIMESTAMP) DESC NULLS LAST
                ) AS row_num
                FROM ({union_sql})
            ) WHERE row_num = 1
        """
    # When stats show no duplicate keys across the union, a global window is redundant (and ~33M rows OOM-prone).
    dedupe_sql = dedupe_window_sql if dupes > 0 else f"SELECT * FROM ({union_sql})"
    if args.exclude_historical and HISTORICAL_MARKETS_FILE.exists():
        hist_esc = _parquet_path_sql(HISTORICAL_MARKETS_FILE)
        final_sql = f"""
                SELECT f.*
                FROM ({dedupe_sql}) f
                LEFT JOIN (
                    SELECT DISTINCT ticker, COALESCE(close_time, created_time, '') AS kt
                    FROM read_parquet('{hist_esc}')
                    WHERE ticker IS NOT NULL AND ticker <> ''
                ) h
                  ON f.ticker = h.ticker
                 AND COALESCE(f.close_time, f.created_time, '') = h.kt
                WHERE h.ticker IS NULL
            """
    else:
        final_sql = dedupe_sql
        if args.exclude_historical and not HISTORICAL_MARKETS_FILE.exists():
            _log("Warning: historical markets file not found; skipping exclude step.")

    spill_root = (
        args.temp_directory.resolve()
        if args.temp_directory
        else (PROJECT_ROOT / "data" / "kalshi" / "state" / "duckdb_fix_markets_spill")
    )

    if dry:
        # Full window + join COUNT can need large RAM; dry-run uses default connection (no tight cap).
        _log("Dry-run: counting result rows (may use significant RAM briefly)…")
        con = duckdb.connect()
        try:
            total_after = con.execute(f"SELECT COUNT(*) FROM ({final_sql})").fetchone()[0]
            if args.exclude_historical and HISTORICAL_MARKETS_FILE.exists():
                deduped_only = con.execute(f"SELECT COUNT(*) FROM ({dedupe_sql})").fetchone()[0]
                overlap_removed = deduped_only - total_after
                _log(f"Rows after excluding historical overlap: {total_after:,} (removed {overlap_removed:,} overlapping)")
            else:
                overlap_removed = total_before - total_after
        finally:
            con.close()
    else:
        canonical_partition.mkdir(parents=True, exist_ok=True)
        import pyarrow.parquet as pq

        mem = float(args.memory_limit_gb)
        if args.exclude_historical and HISTORICAL_MARKETS_FILE.exists():
            # Two phases: global window alone can need 20–40+ GiB on ~33M rows; phase 2 is lighter.
            deduped_tmp = canonical_partition / "_deduped_forward_only.parquet"
            deduped_esc = _parquet_path_sql(deduped_tmp)
            _log(
                "Phase 1/2: materialize forward markets to temp Parquet "
                + ("(ROW_NUMBER dedupe)" if dupes > 0 else "(stream union; already key-unique)")
                + "…"
            )
            con1, spill_meta = connect_for_dedupe_spill(
                spill_root,
                reserve_gib=1.5,
                threads=1,
                memory_limit_gb=mem,
            )
            _log(
                f"DuckDB spill: temp={spill_meta['temp_directory']}, "
                f"max_temp={spill_meta['max_temp_directory_size']}, memory_limit={spill_meta['memory_limit_gb']} GiB, threads=1"
            )
            try:
                # Uncompressed intermediate lowers ZSTD buffer pressure during COPY.
                con1.execute(
                    f"COPY ({dedupe_sql}) TO '{deduped_esc}' (FORMAT PARQUET, COMPRESSION uncompressed)"
                )
            finally:
                con1.close()

            hist_esc = _parquet_path_sql(HISTORICAL_MARKETS_FILE)
            anti_sql = f"""
                SELECT f.*
                FROM read_parquet('{deduped_esc}') f
                LEFT JOIN (
                    SELECT DISTINCT ticker, COALESCE(close_time, created_time, '') AS kt
                    FROM read_parquet('{hist_esc}')
                    WHERE ticker IS NOT NULL AND ticker <> ''
                ) h
                  ON f.ticker = h.ticker
                 AND COALESCE(f.close_time, f.created_time, '') = h.kt
                WHERE h.ticker IS NULL
            """
            _log("Phase 2/2: anti-join vs historical and write canonical Parquet…")
            con2, spill_meta2 = connect_for_dedupe_spill(
                spill_root,
                reserve_gib=1.5,
                threads=1,
                memory_limit_gb=min(mem, 16.0),
            )
            _log(
                f"DuckDB spill (phase 2): memory_limit={spill_meta2['memory_limit_gb']} GiB, "
                f"max_temp={spill_meta2['max_temp_directory_size']}"
            )
            out_esc = _parquet_path_sql(out_file)
            try:
                con2.execute(f"COPY ({anti_sql}) TO '{out_esc}' (FORMAT PARQUET, COMPRESSION ZSTD)")
            finally:
                con2.close()
            deduped_tmp.unlink(missing_ok=True)
        else:
            con, spill_meta = connect_for_dedupe_spill(
                spill_root,
                reserve_gib=1.5,
                threads=1,
                memory_limit_gb=mem,
            )
            _log(
                f"DuckDB spill: temp={spill_meta['temp_directory']}, "
                f"max_temp={spill_meta['max_temp_directory_size']}, memory_limit={spill_meta['memory_limit_gb']} GiB"
            )
            _log("Writing staging Parquet (no pre-COPY COUNT)…")
            out_esc = _parquet_path_sql(out_file)
            try:
                con.execute(f"COPY ({final_sql}) TO '{out_esc}' (FORMAT PARQUET, COMPRESSION ZSTD)")
            finally:
                con.close()

        _split_parquet_if_oversized(out_file)
        parquet_outputs = sorted(
            p for p in canonical_partition.glob("*.parquet") if not p.name.startswith("_")
        )
        total_after = sum(int(pq.ParquetFile(p).metadata.num_rows) for p in parquet_outputs)
        overlap_removed = total_before - total_after
        if args.exclude_historical and HISTORICAL_MARKETS_FILE.exists():
            _log(f"Rows after excluding historical overlap: {total_after:,} (removed {overlap_removed:,} overlapping)")

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
    for p in sorted(canonical_partition.glob("*.parquet")):
        if p.name.startswith("_"):
            continue
        shutil.copy2(p, FORWARD_MARKETS_DIR / "dt=canonical" / p.name)
    _log("Replaced. forward_markets/dt=canonical/ now holds the deduped Parquet shard(s).")

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
