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
import os
import shutil
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path

_SCRIPT_ROOT = Path(__file__).resolve().parents[1]
if str(_SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_ROOT))

import duckdb
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
    failure_recovery,
    kv_table,
    milestone,
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


def _int_env(name: str, default: int = 0) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return max(0, int(raw))
    except ValueError:
        return default


def _merge_parquet_parts(part_paths: list[Path], out_path: Path, *, copy_compression: str) -> None:
    """Stream-merge part files into one Parquet without holding the full dataset in RAM."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    if not part_paths:
        raise RuntimeError("merge: no part files")
    comp: str | None
    if copy_compression == "uncompressed":
        comp = "none"
    elif copy_compression == "zstd":
        comp = "zstd"
    else:
        comp = "snappy"
    writer: pq.ParquetWriter | None = None
    try:
        for p in part_paths:
            pf = pq.ParquetFile(p)
            for batch in pf.iter_batches(batch_size=65_536):
                tab = pa.Table.from_batches([batch])
                if writer is None:
                    writer = pq.ParquetWriter(str(out_path), tab.schema, compression=comp)
                writer.write_table(tab)
    finally:
        if writer is not None:
            writer.close()


def _copy_one_file_chunked(
    con: duckdb.DuckDBPyConnection,
    *,
    filename_sql_escaped: str,
    out_tmp: Path,
    copy_fmt: str,
    chunk_rows: int,
    copy_compression: str,
) -> None:
    """Range export on precomputed _dedupe_export_rn (no ORDER BY / top-N sort in COPY)."""
    import pyarrow.parquet as pq

    if chunk_rows < 100_000:
        raise ValueError("copy_chunk_rows must be >= 100000")
    part_paths: list[Path] = []
    lo = 0
    k = 0
    while True:
        hi = lo + chunk_rows
        part_path = out_tmp.with_name(f"{out_tmp.stem}.part{k}{out_tmp.suffix}")
        part_esc = _escape_sql_string(str(part_path))
        sql = f"""
            COPY (
                SELECT * EXCLUDE (filename, _dedupe_export_rn)
                FROM trade_winners
                WHERE filename = '{filename_sql_escaped}'
                  AND _dedupe_export_rn > {lo} AND _dedupe_export_rn <= {hi}
            ) TO '{part_esc}' {copy_fmt}
            """
        con.execute(sql)
        if not part_path.exists() or part_path.stat().st_size == 0:
            part_path.unlink(missing_ok=True)
            break
        nr = pq.ParquetFile(part_path).metadata.num_rows
        if nr == 0:
            part_path.unlink(missing_ok=True)
            break
        part_paths.append(part_path)
        lo = hi
        k += 1
        if nr < chunk_rows:
            break

    if not part_paths:
        raise RuntimeError("chunked export produced no row parts (unexpected)")

    if len(part_paths) == 1:
        if out_tmp.exists():
            out_tmp.unlink()
        shutil.move(str(part_paths[0]), str(out_tmp))
        return

    merge_target = out_tmp.with_suffix(out_tmp.suffix + ".merged.tmp")
    try:
        _merge_parquet_parts(part_paths, merge_target, copy_compression=copy_compression)
        if out_tmp.exists():
            out_tmp.unlink()
        shutil.move(str(merge_target), str(out_tmp))
    finally:
        merge_target.unlink(missing_ok=True)
        for p in part_paths:
            p.unlink(missing_ok=True)


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
        help="DuckDB memory_limit during window/materialize (default 6). Lower → more spill; helps huge window queries.",
    )
    parser.add_argument(
        "--export-memory-limit-gb",
        type=float,
        default=None,
        metavar="G",
        help=(
            "DuckDB memory_limit during per-file COPY to Parquet (default: max(2× --memory-limit-gb, 32), cap 56). "
            "Use 0 with --no-export-memory-limit (or alone) to RESET to DuckDB default instead of a GiB cap."
        ),
    )
    parser.add_argument(
        "--no-export-memory-limit",
        action="store_true",
        help=(
            "Before COPY: SET a high memory_limit (~52 GiB, or KALSHI_DEDUPE_EXPORT_GIB) so COPY overrides "
            "the connect-time 6 GiB cap (RESET would not). Use when COPY OOMs after materialize."
        ),
    )
    parser.add_argument(
        "--copy-compression",
        choices=("snappy", "zstd", "uncompressed"),
        default="snappy",
        help="Parquet compression for per-file COPY (default snappy; zstd smaller but higher peak RAM).",
    )
    parser.add_argument(
        "--copy-chunk-rows",
        type=int,
        default=0,
        metavar="N",
        help=(
            "If >= 100000, export each source file in keyset chunks of N rows (lower peak RAM during COPY). "
            "Env: KALSHI_DEDUPE_COPY_CHUNK_ROWS (default 0 = one COPY per file)."
        ),
    )
    args = parser.parse_args()
    copy_chunk_rows = int(args.copy_chunk_rows or 0)
    if copy_chunk_rows <= 0:
        copy_chunk_rows = _int_env("KALSHI_DEDUPE_COPY_CHUNK_ROWS", 0)
    args.copy_chunk_rows = copy_chunk_rows

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

    stage = "duckdb_open"
    try:
        milestone("START", "dedupe_forward_trades: DuckDB connected; beginning scan")
        simple_banner("Aggregate statistics (DuckDB scan)")
        t0 = __import__("time").time()
        stage = "aggregate_scan"
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
            milestone("STATS_ONLY_DONE", "counts only; Parquet on disk unchanged")
            blank()
            phase(2, "STATS-ONLY complete", "Heavy dedupe (materialize winners) was skipped.")
            notice("Next: run without --stats-only to dry-run or apply (see docs/DATA_REPAIR.md).")
            warn(f"Approximately {dup_rows:,} redundant rows exist; full run will remove them.")
            return 0

        stage = "materialize_winners"
        milestone("MATERIALIZE_START", "building trade_winners table (long; may spill to duckdb_dedupe_spill)")
        notice("Materializing winning rows (single row per trade_id) — CPU/disk intensive…")
        con.execute("DROP TABLE IF EXISTS trade_winners")
        con.execute(
            f"""
            CREATE TABLE trade_winners AS
            SELECT * EXCLUDE (rn)
            FROM (
                SELECT
                    w.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY w.filename
                        ORDER BY
                            length(trim(CAST(w.trade_id AS VARCHAR))),
                            trim(CAST(w.trade_id AS VARCHAR))
                    ) AS _dedupe_export_rn
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
                ) AS w
                WHERE w.rn = 1
            ) AS t
            """
        )
        winners_count = con.execute("SELECT COUNT(*) FROM trade_winners").fetchone()[0]
        milestone(
            "MATERIALIZE_DONE",
            f"winners={winners_count:,} (disk Parquet files not modified yet)",
        )
        kv_table(
            [
                ("Winning rows (output)", f"{winners_count:,}"),
                ("Rows removed by dedupe", f"{total_before - winners_count:,}"),
            ],
            title="After materialization",
        )

        stage = "index_winners"
        notice("Indexing by source filename for export…")
        try:
            con.execute("CREATE INDEX idx_trade_winners_filename ON trade_winners(filename)")
            success("Index idx_trade_winners_filename created.")
        except Exception as exc:
            warn(f"Index creation skipped ({exc}); export may be slower.")
        try:
            con.execute(
                "CREATE INDEX idx_trade_winners_filename_export_rn ON trade_winners(filename, _dedupe_export_rn)"
            )
            success("Index idx_trade_winners_filename_export_rn created (chunked COPY).")
        except Exception as exc:
            warn(f"Export row-number index skipped ({exc}); chunked COPY may be slow or OOM.")

        if args.dry_run:
            milestone("DRY_RUN", "no backup and no Parquet writes")
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
        stage = "backup_and_rewrite"
        milestone(
            "BACKUP_PHASE_START",
            "original files will be copied to backup tree, then replaced (from here on, I/O is destructive)",
        )
        phase(
            2,
            "Backup + atomic rewrite",
            "Each file copied to backup tree, then replaced via .tmp -> rename (ASCII arrow).",
        )
        notice(f"Backup directory: {backup_root}")
        backup_root.mkdir(parents=True, exist_ok=True)

        # COPY from a large in-memory table + ZSTD can exceed the window-phase memory_limit; bump for export only.
        # Default 32 GiB+: COPY + holding trade_winners can exceed 12 GiB on huge first files.
        # NOTE: connect_for_dedupe_spill() sets memory_limit in connect(config=...). In DuckDB, RESET memory_limit
        # restores *that* bound (~6 GiB), not OS-wide default — so RESET does NOT raise the COPY budget.
        # --no-export-memory-limit therefore uses an explicit high SET (override connect default for this phase).
        use_high_export_memory = bool(args.no_export_memory_limit) or (
            args.export_memory_limit_gb is not None and float(args.export_memory_limit_gb) <= 0
        )
        if use_high_export_memory:
            # Explicit high cap overrides connect() ~6 GiB (RESET would not). Env optional.
            high_gib = float(os.environ.get("KALSHI_DEDUPE_EXPORT_GIB", "52"))
            high_gib = min(max(high_gib, 16.0), 62.0)
            milestone(
                "COPY_PHASE_TUNING",
                f"SET memory_limit={high_gib:.0f}GiB (explicit high cap; overrides connect 6GiB) + threads=1 + "
                f"compression={args.copy_compression}",
            )
            notice(
                "COPY phase: explicit high memory_limit (connect() used ~6 GiB for window; that cap would "
                "otherwise remain after RESET). Tune with --export-memory-limit-gb or env KALSHI_DEDUPE_EXPORT_GIB."
            )
            con.execute(f"SET memory_limit='{high_gib:.1f}GB'")
        else:
            export_mem = float(args.export_memory_limit_gb) if args.export_memory_limit_gb is not None else max(
                float(args.memory_limit_gb) * 2.0, 48.0
            )
            # 64 GiB hosts: COPY can need >30 GiB headroom on top of buffered trade_winners; allow up to ~62 GiB SET.
            export_mem = min(max(export_mem, float(args.memory_limit_gb)), 62.0)
            milestone(
                "COPY_PHASE_TUNING",
                f"SET memory_limit={export_mem:.1f}GiB threads=1 compression={args.copy_compression} (per-file Parquet export)",
            )
            notice(
                "Tuning DuckDB for COPY phase: higher memory limit than window phase; threads=1; "
                f"compression={args.copy_compression} (see --export-memory-limit-gb, --copy-compression, "
                "--no-export-memory-limit)."
            )
            con.execute(f"SET memory_limit='{export_mem:.1f}GB'")
        con.execute("SET threads=1")
        try:
            con.execute("SET preserve_insertion_order=false")
        except Exception:
            pass

        # DuckDB: COMPRESSION snappy|zstd|uncompressed (lowercase in docs)
        if args.copy_compression == "uncompressed":
            copy_fmt = "(FORMAT PARQUET, COMPRESSION uncompressed)"
        else:
            copy_fmt = f"(FORMAT PARQUET, COMPRESSION {args.copy_compression})"

        if args.copy_chunk_rows > 0:
            milestone(
                "COPY_CHUNKED",
                f"per-file export in chunks of {args.copy_chunk_rows:,} rows "
                f"(_dedupe_export_rn ranges, no ORDER BY in COPY; PyArrow merge if multi-part)",
            )

        written = 0
        removed = 0
        iter_files = files if args.no_progress else tqdm(files, desc="Trades dedupe", unit="file", ncols=100)
        for i, fp in enumerate(iter_files, 1):
            stage = f"rewrite_file_{i}_of_{len(files)}"
            fp_s = str(fp)
            # Use EXISTS, not COUNT(*): counting hundreds of millions of rows per file can OOM; EXISTS stops at first row.
            has_rows = con.execute(
                "SELECT EXISTS(SELECT 1 FROM trade_winners WHERE filename = ? LIMIT 1)",
                [fp_s],
            ).fetchone()[0]

            dest_backup = backup_root / f"{i:05d}_{fp.name}"
            dest_backup.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(fp, dest_backup)

            out_tmp = fp.with_suffix(fp.suffix + ".compact.tmp")
            esc = _escape_sql_string(fp_s)

            if not has_rows:
                if fp.exists():
                    fp.unlink()
                removed += 1
                continue

            if args.copy_chunk_rows >= 100_000:
                _copy_one_file_chunked(
                    con,
                    filename_sql_escaped=esc,
                    out_tmp=out_tmp,
                    copy_fmt=copy_fmt,
                    chunk_rows=args.copy_chunk_rows,
                    copy_compression=args.copy_compression,
                )
            else:
                con.execute(
                    f"""
                    COPY (
                        SELECT * EXCLUDE (filename, _dedupe_export_rn)
                        FROM trade_winners
                        WHERE filename = '{esc}'
                    ) TO '{_escape_sql_string(str(out_tmp))}' {copy_fmt}
                    """
                )
            out_tmp.replace(fp)
            written += 1

        milestone("BACKUP_PHASE_DONE", f"rewritten={written}, removed_empty={removed}")
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
        milestone("COMPLETE", "forward trades dedupe finished OK")
        return 0
    except KeyboardInterrupt:
        blank()
        err("Interrupted (Ctrl+C).")
        failure_recovery(
            f"Last stage variable: {stage}",
            [
                "If last MILESTONE was BEFORE BACKUP_PHASE_START: original Parquet under forward_trades was NOT modified.",
                "If during BACKUP_PHASE / rewrite_file_*: some files may be copied to forward_trades_pre_dedupe_*; check backup folder and partial writes before re-running.",
                "DuckDB spill dir data/kalshi/state/duckdb_dedupe_spill/ may be large; safe to delete when no dedupe process is running.",
                "Re-run when ready: uv run python scripts/dedupe_forward_trades.py --yes --ignore-lock",
            ],
        )
        return 130
    except Exception as exc:
        blank()
        err(f"{type(exc).__name__}: {exc}")
        print(traceback.format_exc())
        sys.stdout.flush()
        safe_note = (
            "If failure was BEFORE milestone BACKUP_PHASE_START: original forward_trades Parquet was NOT changed."
        )
        risky_note = (
            "If failure was DURING or AFTER BACKUP_PHASE_START: inspect backup under data/kalshi/historical/forward_trades_pre_dedupe_* before deleting anything."
        )
        failure_recovery(
            f"Stage at crash: {stage} | {safe_note} {risky_note}",
            [
                "Read the Python traceback above (bottom line is the immediate cause).",
                "Unicode / console errors: use UTF-8 terminal (chcp 65001) or Git Bash; repo terminal_report.py also sanitizes output.",
                "DuckDB OOM at COPY/rewrite_file_*: use --copy-chunk-rows 8000000 (range export uses _dedupe_export_rn; "
                "avoid older ORDER BY+LIMIT chunks), KALSHI_DEDUPE_EXPORT_GIB, --copy-compression uncompressed; see docs/DATA_REPAIR.md.",
                "Disk full: free space on the volume holding data/kalshi and duckdb_dedupe_spill, then re-run.",
                "Retry: uv run python scripts/dedupe_forward_trades.py --yes --ignore-lock",
            ],
        )
        return 1
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
