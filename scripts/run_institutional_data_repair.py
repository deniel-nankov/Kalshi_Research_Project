#!/usr/bin/env python3
"""
Institutional runner: preflight → optional apply (trades + markets dedupe) → validation.

Output is designed for terminal review: timestamps, phases, disk/lock context, and
clear next steps.

  # Preflight only (fast): duplicate estimates + disk + lock — no heavy scans beyond COUNT(*)
  uv run python scripts/run_institutional_data_repair.py

  # Execute dedupe + validators (long-running on large datasets)
  uv run python scripts/run_institutional_data_repair.py --apply

  # Apply but skip validate_data_health (can take many minutes)
  uv run python scripts/run_institutional_data_repair.py --apply --skip-full-health

Orphan API backfill is NOT run automatically (can be millions of HTTP calls). See
docs/DATA_REPAIR.md for fix_orphan_tickers with --checkpoint.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path


def _flush() -> None:
    sys.stdout.flush()
    sys.stderr.flush()

_SCRIPT_ROOT = Path(__file__).resolve().parents[1]
if str(_SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_ROOT))

from src.kalshi_forward.paths import (
    KALSHI_DATA_DIR,
    LOCK_FILE,
    PROJECT_ROOT,
)
from src.kalshi_forward.terminal_report import (
    banner,
    blank,
    bullet_list,
    err,
    failure_recovery,
    format_bytes,
    kv_table,
    notice,
    phase,
    preflight_disk,
    success,
    utc_now_iso,
    warn,
)


def _recovery_for_step(title: str) -> None:
    """After a non-zero exit, tell the operator what likely broke and what to run next."""
    if "dedupe forward trades" in title.lower():
        failure_recovery(
            f"Orchestrator step failed: {title}",
            [
                "Scroll up for the child script output; search for MILESTONE and === FAILURE === lines.",
                "If the last MILESTONE was before BACKUP_PHASE_START: your original Parquet was not rewritten; fix the error and re-run trades dedupe only.",
                "If failure was during BACKUP_PHASE / rewrite_file_*: check data/kalshi/historical/forward_trades_pre_dedupe_* for backups before changing live files.",
                "Retry trades only: uv run python scripts/dedupe_forward_trades.py --yes --ignore-lock --no-export-memory-limit "
                "[--copy-chunk-rows 8000000] [--copy-compression uncompressed]",
                "Or full orchestrator: uv run python scripts/run_institutional_data_repair.py --apply --health-strict "
                "--trades-no-export-memory-limit --trades-copy-compression uncompressed [--trades-copy-chunk-rows 8000000]",
                "Full runbook: docs/DATA_REPAIR.md",
            ],
        )
    elif "dedupe forward markets" in title.lower():
        failure_recovery(
            f"Orchestrator step failed: {title}",
            [
                "Forward trades step may have completed; check output above.",
                "Retry markets only: uv run python scripts/dedupe_forward_markets.py --yes --ignore-lock",
                "docs/DATA_REPAIR.md",
            ],
        )
    elif "validate_forward_pipeline" in title.lower() or "forward pipeline" in title.lower():
        failure_recovery(
            f"Orchestrator step failed: {title}",
            [
                "Dedupe may have finished; this step audits overlap/duplicates.",
                "Run manually: uv run python scripts/validate_forward_pipeline.py --skip-run",
            ],
        )
    elif "validate_data_health" in title.lower() or "data health" in title.lower():
        failure_recovery(
            f"Orchestrator step failed: {title}",
            [
                "Open the JSON path printed above (or data/kalshi/state/health_report_post_repair.json).",
                "FAIL rows need fixing; WARN rows (e.g. orphans) are documented in docs/HEALTH_REPORT_WARNINGS_EXAMINED.md.",
                "Re-run health only: uv run python scripts/validate_data_health.py --strict --output data/kalshi/state/health_report_post_repair.json",
            ],
        )
    else:
        failure_recovery(
            f"Orchestrator step failed: {title}",
            ["See output above.", "docs/DATA_REPAIR.md"],
        )


def _run_step(title: str, cmd: list[str], *, cwd: Path) -> int:
    _flush()
    phase(0, title, " ".join(cmd))
    print()
    _flush()
    env = os.environ.copy()
    env.setdefault("PYTHONUNBUFFERED", "1")
    # -u: unbuffered stdout so child banners appear in order with parent
    if len(cmd) >= 1 and "python" in Path(cmd[0]).name.lower():
        cmd = [cmd[0], "-u", *cmd[1:]]
    rc = subprocess.call(cmd, cwd=cwd, env=env)
    print()
    if rc != 0:
        err(f"Step exited with code {rc}: {' '.join(cmd)}")
        _recovery_for_step(title)
    else:
        success(f"Step completed (exit 0): {title}")
    return rc


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Institutional Kalshi forward data repair orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Run trades + markets dedupe with --yes --ignore-lock (otherwise only preflight/stats)",
    )
    parser.add_argument(
        "--skip-full-health",
        action="store_true",
        help="After dedupe, skip scripts/validate_data_health.py (still runs validate_forward_pipeline --skip-run)",
    )
    parser.add_argument(
        "--orphan-dry-run",
        action="store_true",
        help="After other steps, run fix_orphan_tickers.py --dry-run (may take time: distinct orphan query)",
    )
    parser.add_argument(
        "--health-strict",
        action="store_true",
        help="When running validate_data_health.py, pass --strict (exit 1 on any FAIL)",
    )
    parser.add_argument(
        "--health-output",
        type=Path,
        default=None,
        metavar="PATH",
        help="Write validate_data_health JSON to this path (default: data/kalshi/state/health_report_post_repair.json)",
    )
    parser.add_argument(
        "--trades-no-export-memory-limit",
        action="store_true",
        help="Forward to dedupe_forward_trades.py: RESET memory_limit for COPY (use if SET ...GB still OOMs)",
    )
    parser.add_argument(
        "--trades-copy-compression",
        choices=("snappy", "zstd", "uncompressed"),
        default="snappy",
        help="Forward to dedupe_forward_trades.py Parquet COPY codec (uncompressed uses less RAM than zstd)",
    )
    parser.add_argument(
        "--trades-copy-chunk-rows",
        type=int,
        default=0,
        metavar="N",
        help="Forward to dedupe_forward_trades.py --copy-chunk-rows (0 = disabled; try 8000000 if COPY OOMs)",
    )
    args = parser.parse_args()

    try:
        sys.stdout.reconfigure(line_buffering=True)  # type: ignore[attr-defined]
    except Exception:
        pass

    py = sys.executable
    cwd = PROJECT_ROOT

    banner(
        "Kalshi data repair — institutional runbook",
        f"Started {utc_now_iso()}  |  project: {PROJECT_ROOT}",
    )
    _flush()

    free_b, total_b = preflight_disk(KALSHI_DATA_DIR)
    kv_table(
        [
            ("Python", sys.version.split()[0]),
            ("Data directory", str(KALSHI_DATA_DIR)),
            ("Disk free / total", f"{format_bytes(free_b)} / {format_bytes(total_b)}"),
            ("Ingestion lock file", str(LOCK_FILE)),
            ("Lock present now", "yes" if LOCK_FILE.exists() else "no"),
            ("Mode", "APPLY (destructive dedupe)" if args.apply else "PREFLIGHT + fast stats only"),
        ],
        title="Preflight — environment",
    )
    blank()

    notice("What this suite does:")
    bullet_list(
        [
            "Preflight: duplicate row estimates via fast COUNT/DISTINCT (no full materialization).",
            "With --apply: global dedupe for forward trades, then forward markets (snapshotted files only).",
            "Validation: forward pipeline audit; optional full institutional health report.",
        ]
    )
    notice("What it does NOT do: stop update_forward, remove forward_ingestion.lock, or bulk-fetch orphan markets.")
    blank()
    _flush()

    # ─── Fast stats ─────────────────────────────────────────────────────────
    rc = _run_step(
        "FAST STATS — forward trades (duplicate estimate)",
        [py, str(cwd / "scripts" / "dedupe_forward_trades.py"), "--stats-only", "--ignore-lock"],
        cwd=cwd,
    )
    if rc == 2:
        warn("Trades stats returned 2 (lock). Re-running with --ignore-lock should not happen — check script.")
    elif rc != 0:
        return rc

    rc = _run_step(
        "FAST STATS — forward markets (duplicate estimate)",
        [py, str(cwd / "scripts" / "dedupe_forward_markets.py"), "--stats-only", "--ignore-lock"],
        cwd=cwd,
    )
    if rc != 0:
        return rc

    if not args.apply:
        blank()
        banner("PREFLIGHT COMPLETE — no files modified", utc_now_iso())
        kv_table(
            [
                ("Next command to dedupe on disk", "uv run python scripts/run_institutional_data_repair.py --apply"),
                ("Documentation", "docs/DATA_REPAIR.md"),
            ],
            title="Recommended next step",
        )
        return 0

    # ─── Apply dedupe ────────────────────────────────────────────────────────
    banner("APPLY PHASE — deduplication", "This rewrites existing Parquet files; backups are created alongside.")
    notice("Apply order (each step prints its own MILESTONE lines):")
    bullet_list(
        [
            "1) dedupe_forward_trades.py --yes (long: global window + rewrite Parquet)",
            "2) dedupe_forward_markets.py --yes",
            "3) validate_forward_pipeline.py --skip-run",
            "4) validate_data_health.py (if not --skip-full-health)",
        ]
    )
    blank()
    _flush()
    trades_apply = [
        py,
        str(cwd / "scripts" / "dedupe_forward_trades.py"),
        "--yes",
        "--ignore-lock",
        "--copy-compression",
        args.trades_copy_compression,
    ]
    if args.trades_no_export_memory_limit:
        trades_apply.append("--no-export-memory-limit")
    if int(args.trades_copy_chunk_rows or 0) >= 100_000:
        trades_apply.extend(["--copy-chunk-rows", str(int(args.trades_copy_chunk_rows))])
    rc = _run_step(
        "APPLY — dedupe forward trades",
        trades_apply,
        cwd=cwd,
    )
    if rc != 0:
        return rc

    rc = _run_step(
        "APPLY — dedupe forward markets",
        [py, str(cwd / "scripts" / "dedupe_forward_markets.py"), "--yes", "--ignore-lock"],
        cwd=cwd,
    )
    if rc != 0:
        return rc

    # ─── Validation ──────────────────────────────────────────────────────────
    rc = _run_step(
        "VALIDATION — forward pipeline (duplicates / overlap)",
        [py, str(cwd / "scripts" / "validate_forward_pipeline.py"), "--skip-run"],
        cwd=cwd,
    )
    if rc != 0:
        warn("Non-zero exit from validate_forward_pipeline; review output above.")

    if not args.skip_full_health:
        health_cmd = [py, str(cwd / "scripts" / "validate_data_health.py")]
        out = args.health_output
        if out is None:
            out = cwd / "data" / "kalshi" / "state" / "health_report_post_repair.json"
        health_cmd.extend(["--output", str(out)])
        if args.health_strict:
            health_cmd.append("--strict")
        rc = _run_step(
            "VALIDATION — full data health report (long on large data)",
            health_cmd,
            cwd=cwd,
        )
        if rc != 0:
            warn("validate_data_health reported issues; see summary block in output.")
    else:
        phase(0, "Skipped validate_data_health.py", "(--skip-full-health)")

    if args.orphan_dry_run:
        rc = _run_step(
            "ORPHAN AUDIT — fix_orphan_tickers dry-run",
            [py, str(cwd / "scripts" / "fix_orphan_tickers.py"), "--dry-run"],
            cwd=cwd,
        )
        if rc != 0:
            return rc

    blank()
    banner("INSTITUTIONAL DATA REPAIR — FINISHED", utc_now_iso())
    success("Review all sections above for exit codes and numeric summaries.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
