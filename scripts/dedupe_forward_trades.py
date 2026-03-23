#!/usr/bin/env python3
# ruff: noqa: E402
"""
Globally dedupe forward trade Parquet files by trade_id without stopping ingestion.

Ingestion only ever *creates* new files (atomic tmp→rename); it never rewrites an
existing Parquet. This script:

  1) Snapshots the list of *.parquet paths under forward_trades (+ legacy forward).
  2) Materializes one winning row per trade_id (latest created_time, then filename).
  3) Rewrites each snapshotted file with only the rows that “won” for that file.
  4) Deletes a file if every row was a duplicate of a row kept elsewhere.

Safe to run while `update_forward.py` is pulling: new files written after the
snapshot are left untouched (run again later to fold them in).

Usage:
  uv run python scripts/dedupe_forward_trades.py --stats-only    # fast: counts only
  uv run python scripts/dedupe_forward_trades.py --dry-run       # full plan + sample rows
  uv run python scripts/dedupe_forward_trades.py --yes --ignore-lock
"""

from __future__ import annotations

import argparse
import glob
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

_SCRIPT_ROOT = Path(__file__).resolve().parents[1]
if str(_SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_ROOT))

from tqdm import tqdm

from src.kalshi_forward.duckdb_heavy import connect_for_dedupe_spill
from src.kalshi_forward.paths import (
    FORWARD_TRADES_GLOB,
    LEGACY_FORWARD_TRADES_GLOB,
    LOCK_FILE,
    PROJECT_ROOT,
)
from src.kalshi_forward.terminal_report import (
    banner,
    blank,
    err,
    kv_table,
    notice,
    phase,
    simple_banner,
    success,
    utc_now_iso,
    warn,
)


def _collect_forward_trade_files() -> list[Path]:
    patterns = [str(FORWARD_TRADES_GLOB), str(LEGACY_FORWARD_TRADES_GLOB)]
    files: set[str] = set()
    for p in patterns:
        files.update(glob.glob(p))
    return sorted(Path(f) for f in files if f.endswith(".parquet"))


def _escape_sql_string(s: str) -> str:
    return s.replace("'", "''")


def _read_parquet_list_sql(paths: list[Path]) -> str:
    inner = ", ".join(f"'{_escape_sql_string(str(p))}'" for p in paths)
    return f"read_parquet([{inner}], filename=true)"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Dedupe forward trades by trade_id (live-safe, institutional output)"
    )
    parser.add_argument(
        "--stats-only",
        action="store_true",
        help="Fast: duplicate row counts only (no heavy materialization, no writes)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Full dry-run: materialize winners + sample per-file counts; no writes")
    parser.add_argument("--yes", "-y", action="store_true", help="Apply rewrites without prompt")
    parser.add_argument(
        "--ignore-lock",
        action="store_true",
        help=f"Run even if {LOCK_FILE.name} exists (snapshotted files only)",
    )
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable tqdm progress bar during file rewrites",
    )
    parser.add_argument(
        "--temp-directory",
        type=Path,
        default=None,
        help="DuckDB spill directory (default: data/kalshi/state/duckdb_dedupe_spill)",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=2,
        help="DuckDB threads for heavy window query (lower = less RAM/temp pressure; default 2)",
    )
    parser.add_argument(
        "--max-temp-gib",
        type=float,
        default=None,
        metavar="G",
        help="Hard cap on temp spill size (default: most of free disk minus --reserve-gib)",
    )
    parser.add_argument(
        "--reserve-gib",
        type=float,
        default=1.5,
        metavar="G",
        help="GiB to leave free on the volume when sizing spill (default 1.5; lower = larger DuckDB temp cap)",
    )
    parser.add_argument(
        "--memory-limit-gb",
        type=float,
        default=6.0,
        metavar="G",
        help="DuckDB memory_limit (default 6). Lower → more spill to disk; helps huge window queries.",
    )
    args = parser.parse_args()

    banner(
        "Kalshi forward trades — deduplication",
        f"Run started {utc_now_iso()}  |  script: dedupe_forward_trades.py",
    )
    kv_table(
        [
            ("Mode", "STATS-ONLY (fast counts)" if args.stats_only else "DRY-RUN" if args.dry_run else "APPLY" if args.yes else "INTERACTIVE"),
            ("Ingestion lock", str(LOCK_FILE)),
            ("Lock ignored", "yes (--ignore-lock)" if args.ignore_lock else "no"),
        ],
        title="Execution context",
    )
    blank()

    if LOCK_FILE.exists() and not args.ignore_lock:
        phase(1, "BLOCKED — ingestion lock", "Another run may hold the lock; dedupe does not remove it.")
        warn(f"Lock present: {LOCK_FILE}")
        notice("Re-run with --ignore-lock to rewrite only files that exist now (safe alongside new ingest files).")
        return 2

    if LOCK_FILE.exists() and args.ignore_lock:
        warn("Proceeding with --ignore-lock — only snapshotted Parquet files will be rewritten.")

    files = _collect_forward_trade_files()
    if not files:
        phase(1, "No data")
        notice("No forward trade Parquet files matched. Nothing to do.")
        return 0

    phase(1, "Snapshot", "Enumerating Parquet files under forward_trades (+ legacy incremental trades).")
    notice(f"Files in snapshot: {len(files):,}")
    if len(files) <= 5:
        for f in files:
            notice(str(f))
    else:
        for f in files[:3]:
            notice(str(f))
        notice(f"… plus {len(files) - 3:,} more files (sorted lexicographically)")

    rp = _read_parquet_list_sql(files)
    spill_root = (
        args.temp_directory.resolve()
        if args.temp_directory
        else (PROJECT_ROOT / "data" / "kalshi" / "state" / "duckdb_dedupe_spill")
    )
    con, spill_cfg = connect_for_dedupe_spill(
        spill_root,
        reserve_gib=float(args.reserve_gib),
        threads=int(args.threads),
        max_temp_gib=float(args.max_temp_gib) if args.max_temp_gib is not None else None,
        memory_limit_gb=float(args.memory_limit_gb),
    )
    simple_banner("DuckDB spill (out-of-core)")
    kv_table([(k, str(v)) for k, v in spill_cfg.items()], title="Configured for large window/dedupe")
    blank()

    try:
        simple_banner("Aggregate statistics (DuckDB scan)")
        t0 = __import__("time").time()
        total_before = con.execute(f"SELECT COUNT(*) FROM {rp}").fetchone()[0]
        distinct_ids = con.execute(
            f"""
            SELECT COUNT(DISTINCT CAST(trade_id AS VARCHAR)) FROM {rp}
            WHERE trade_id IS NOT NULL AND trim(CAST(trade_id AS VARCHAR)) <> ''
            """
        ).fetchone()[0]
        dup_rows = total_before - distinct_ids
        elapsed = __import__("time").time() - t0
        kv_table(
            [
                ("Total rows (forward)", f"{total_before:,}"),
                ("Distinct trade_id", f"{distinct_ids:,}"),
                ("Estimated duplicate rows", f"{dup_rows:,}"),
                ("Scan time", f"{elapsed:.2f}s"),
            ],
            title="Results",
        )

        if dup_rows == 0:
            success("No duplicate trade_ids across forward files — dataset already unique.")
            return 0

        if args.stats_only:
            blank()
            phase(2, "STATS-ONLY complete", "Heavy dedupe (materialize winners) was skipped.")
            notice("Next: run without --stats-only to dry-run or apply (see docs/DATA_REPAIR.md).")
            warn(f"Approximately {dup_rows:,} redundant rows exist; full run will remove them.")
            return 0

        notice("Materializing winning rows (single row per trade_id) — CPU/disk intensive…")
        con.execute("DROP TABLE IF EXISTS trade_winners")
        con.execute(
            f"""
            CREATE TABLE trade_winners AS
            SELECT * EXCLUDE (rn)
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY CAST(trade_id AS VARCHAR)
                        ORDER BY
                            TRY_CAST(created_time AS TIMESTAMP) DESC NULLS LAST,
                            filename DESC
                    ) AS rn
                FROM {rp}
                WHERE trade_id IS NOT NULL AND trim(CAST(trade_id AS VARCHAR)) <> ''
            ) s
            WHERE rn = 1
            """
        )
        winners_count = con.execute("SELECT COUNT(*) FROM trade_winners").fetchone()[0]
        kv_table(
            [
                ("Winning rows (output)", f"{winners_count:,}"),
                ("Rows removed by dedupe", f"{total_before - winners_count:,}"),
            ],
            title="After materialization",
        )

        notice("Indexing by source filename for export…")
        try:
            con.execute("CREATE INDEX idx_trade_winners_filename ON trade_winners(filename)")
            success("Index idx_trade_winners_filename created.")
        except Exception as exc:
            warn(f"Index creation skipped ({exc}); export may be slower.")

        if args.dry_run:
            simple_banner("DRY-RUN — per-file row counts (sample)")
            for fp in files[:25]:
                cnt = con.execute(
                    "SELECT COUNT(*) FROM trade_winners WHERE filename = ?",
                    [str(fp)],
                ).fetchone()[0]
                notice(f"{fp.name}: {cnt:,} rows retained in this file")
            if len(files) > 25:
                notice(f"… {len(files) - 25:,} additional files not listed")
            success("Dry-run complete — no files modified. Use --yes to apply.")
            return 0

        if not args.yes:
            try:
                reply = input("\n  Rewrite all snapshotted files? [y/N]: ").strip().lower()
            except EOFError:
                reply = "n"
            if reply not in ("y", "yes"):
                err("Aborted (no --yes and no interactive confirmation).")
                return 1

        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        backup_root = PROJECT_ROOT / "data" / "kalshi" / "historical" / f"forward_trades_pre_dedupe_{ts}"
        phase(2, "Backup + atomic rewrite", "Each file copied to backup tree, then replaced via .tmp → rename.")
        notice(f"Backup directory: {backup_root}")
        backup_root.mkdir(parents=True, exist_ok=True)

        written = 0
        removed = 0
        iter_files = files if args.no_progress else tqdm(files, desc="Trades dedupe", unit="file", ncols=100)
        for i, fp in enumerate(iter_files, 1):
            fp_s = str(fp)
            cnt = con.execute(
                "SELECT COUNT(*) FROM trade_winners WHERE filename = ?",
                [fp_s],
            ).fetchone()[0]

            dest_backup = backup_root / f"{i:05d}_{fp.name}"
            dest_backup.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(fp, dest_backup)

            out_tmp = fp.with_suffix(fp.suffix + ".compact.tmp")
            esc = _escape_sql_string(fp_s)

            if cnt == 0:
                if fp.exists():
                    fp.unlink()
                removed += 1
                continue

            con.execute(
                f"""
                COPY (
                    SELECT * EXCLUDE (filename)
                    FROM trade_winners
                    WHERE filename = '{esc}'
                ) TO '{_escape_sql_string(str(out_tmp))}' (FORMAT PARQUET, COMPRESSION ZSTD)
                """
            )
            out_tmp.replace(fp)
            written += 1

        blank()
        banner("Trades deduplication — COMPLETE", utc_now_iso())
        kv_table(
            [
                ("Files rewritten", f"{written:,}"),
                ("Files removed (empty after dedupe)", f"{removed:,}"),
                ("Backup location", str(backup_root)),
            ],
            title="Summary",
        )
        success("Validate with: uv run python scripts/validate_data_health.py")
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
