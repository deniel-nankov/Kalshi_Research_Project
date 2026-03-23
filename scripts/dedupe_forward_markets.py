#!/usr/bin/env python3
# ruff: noqa: E402
"""
Globally dedupe forward market Parquet files by (ticker, close_time/created_time).

Same live-safe model as dedupe_forward_trades.py: snapshot existing files, pick one
row per market key, rewrite each file in place. Ingestion only adds new files.

Usage:
  uv run python scripts/dedupe_forward_markets.py --stats-only
  uv run python scripts/dedupe_forward_markets.py --dry-run
  uv run python scripts/dedupe_forward_markets.py --yes --ignore-lock
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
    FORWARD_MARKETS_GLOB,
    LEGACY_FORWARD_MARKETS_GLOB,
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


def _collect_forward_market_files() -> list[Path]:
    patterns = [str(FORWARD_MARKETS_GLOB), str(LEGACY_FORWARD_MARKETS_GLOB)]
    files: set[str] = set()
    for p in patterns:
        files.update(glob.glob(p))
    return sorted(Path(f) for f in files if f.endswith(".parquet"))


def _escape_sql_string(s: str) -> str:
    return s.replace("'", "''")


def _read_parquet_list_sql(paths: list[Path]) -> str:
    inner = ", ".join(f"'{_escape_sql_string(str(p))}'" for p in paths)
    return f"read_parquet([{inner}], filename=true, union_by_name=true)"


def main() -> int:
    parser = argparse.ArgumentParser(description="Dedupe forward markets (live-safe)")
    parser.add_argument("--stats-only", action="store_true", help="Fast: counts only, no materialization")
    parser.add_argument("--dry-run", action="store_true", help="Materialize + sample; no writes")
    parser.add_argument("--yes", "-y", action="store_true", help="Apply without prompt")
    parser.add_argument("--ignore-lock", action="store_true", help=f"Proceed if {LOCK_FILE.name} exists")
    parser.add_argument("--no-progress", action="store_true", help="Disable tqdm during rewrites")
    parser.add_argument(
        "--temp-directory",
        type=Path,
        default=None,
        help="DuckDB spill directory (default: data/kalshi/state/duckdb_dedupe_spill)",
    )
    parser.add_argument("--threads", type=int, default=2, help="DuckDB threads (default 2)")
    parser.add_argument("--max-temp-gib", type=float, default=None, metavar="G", help="Cap spill size (GiB)")
    parser.add_argument(
        "--reserve-gib",
        type=float,
        default=1.5,
        metavar="G",
        help="GiB to leave free when sizing spill (default 1.5)",
    )
    parser.add_argument(
        "--memory-limit-gb",
        type=float,
        default=6.0,
        metavar="G",
        help="DuckDB memory_limit GB (default 6)",
    )
    args = parser.parse_args()

    banner(
        "Kalshi forward markets — deduplication",
        f"Run started {utc_now_iso()}  |  script: dedupe_forward_markets.py",
    )
    kv_table(
        [
            ("Mode", "STATS-ONLY" if args.stats_only else "DRY-RUN" if args.dry_run else "APPLY" if args.yes else "INTERACTIVE"),
            ("Market key", "ticker + COALESCE(close_time, created_time, '')"),
            ("Lock ignored", "yes" if args.ignore_lock else "no"),
        ],
        title="Execution context",
    )
    blank()

    if LOCK_FILE.exists() and not args.ignore_lock:
        phase(1, "BLOCKED — ingestion lock")
        warn(f"Lock present: {LOCK_FILE}")
        notice("Use --ignore-lock to proceed (rewrites only snapshotted files).")
        return 2

    if LOCK_FILE.exists() and args.ignore_lock:
        warn("Proceeding with --ignore-lock.")

    files = _collect_forward_market_files()
    if not files:
        notice("No forward market Parquet files found.")
        return 0

    phase(1, "Snapshot", f"{len(files):,} Parquet files")
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
    kv_table([(k, str(v)) for k, v in spill_cfg.items()], title="Configured")
    blank()

    try:
        simple_banner("Aggregate statistics")
        total_before = con.execute(f"SELECT COUNT(*) FROM {rp}").fetchone()[0]
        distinct_keys = con.execute(
            f"""
            SELECT COUNT(DISTINCT
                ticker || '|' || COALESCE(close_time, created_time, '')
            ) FROM {rp}
            WHERE ticker IS NOT NULL AND trim(CAST(ticker AS VARCHAR)) <> ''
            """
        ).fetchone()[0]
        dup_rows = total_before - distinct_keys
        kv_table(
            [
                ("Total rows (forward markets)", f"{total_before:,}"),
                ("Distinct market keys", f"{distinct_keys:,}"),
                ("Estimated duplicate rows", f"{dup_rows:,}"),
            ],
            title="Results",
        )

        if dup_rows == 0:
            success("No duplicate market keys — nothing to do.")
            return 0

        if args.stats_only:
            phase(2, "STATS-ONLY complete")
            warn(f"~{dup_rows:,} redundant rows; full run required to remove.")
            return 0

        notice("Materializing winning rows…")
        con.execute("DROP TABLE IF EXISTS market_winners")
        con.execute(
            f"""
            CREATE TABLE market_winners AS
            SELECT * EXCLUDE (rn)
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY ticker, COALESCE(close_time, created_time, '')
                        ORDER BY
                            TRY_CAST(updated_time AS TIMESTAMP) DESC NULLS LAST,
                            filename DESC
                    ) AS rn
                FROM {rp}
                WHERE ticker IS NOT NULL AND trim(CAST(ticker AS VARCHAR)) <> ''
            ) s
            WHERE rn = 1
            """
        )
        winners_count = con.execute("SELECT COUNT(*) FROM market_winners").fetchone()[0]
        kv_table(
            [
                ("Winning rows", f"{winners_count:,}"),
                ("Rows removed", f"{total_before - winners_count:,}"),
            ],
            title="Materialization",
        )

        try:
            con.execute("CREATE INDEX idx_market_winners_filename ON market_winners(filename)")
            success("Index created.")
        except Exception as exc:
            warn(f"Index skipped: {exc}")

        if args.dry_run:
            simple_banner("DRY-RUN sample")
            for fp in files[:15]:
                cnt = con.execute(
                    "SELECT COUNT(*) FROM market_winners WHERE filename = ?",
                    [str(fp)],
                ).fetchone()[0]
                notice(f"{fp.name}: {cnt:,} rows")
            if len(files) > 15:
                notice(f"… +{len(files) - 15:,} files")
            success("Dry-run complete.")
            return 0

        if not args.yes:
            try:
                reply = input("\n  Rewrite all files? [y/N]: ").strip().lower()
            except EOFError:
                reply = "n"
            if reply not in ("y", "yes"):
                err("Aborted.")
                return 1

        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        backup_root = PROJECT_ROOT / "data" / "kalshi" / "historical" / f"forward_markets_pre_dedupe_{ts}"
        phase(2, "Backup + rewrite", str(backup_root))
        backup_root.mkdir(parents=True, exist_ok=True)

        written = 0
        removed = 0
        iter_files = files if args.no_progress else tqdm(files, desc="Markets dedupe", unit="file", ncols=100)
        for i, fp in enumerate(iter_files, 1):
            fp_s = str(fp)
            cnt = con.execute(
                "SELECT COUNT(*) FROM market_winners WHERE filename = ?",
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
                    FROM market_winners
                    WHERE filename = '{esc}'
                ) TO '{_escape_sql_string(str(out_tmp))}' (FORMAT PARQUET, COMPRESSION ZSTD)
                """
            )
            out_tmp.replace(fp)
            written += 1

        blank()
        banner("Markets deduplication — COMPLETE", utc_now_iso())
        kv_table(
            [
                ("Files rewritten", f"{written:,}"),
                ("Files removed", f"{removed:,}"),
                ("Backup", str(backup_root)),
            ],
            title="Summary",
        )
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
