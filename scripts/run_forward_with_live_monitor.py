#!/usr/bin/env python3
"""
Run forward ingestion with live terminal monitoring.

Pulls data from Kalshi and appends to the dataset (after historical is available).
This wrapper runs scripts/update_forward.py and prints:
  • real-time ingestion logs from the child process
  • periodic checkpoint snapshot (watermarks, totals)
  • periodic file snapshot (forward files under data/kalshi/historical/ + size)

Defaults to live mode (upper bound = now). Use --historical-only for catch-up
to the API historical cutoff only.

Examples:
  uv run python scripts/run_forward_with_live_monitor.py
  uv run python scripts/run_forward_with_live_monitor.py --historical-only
"""

from __future__ import annotations

import sys
from pathlib import Path

# Ensure project root on path when run as script (e.g. uv run python scripts/...)
_SCRIPT_ROOT = Path(__file__).resolve().parents[1]
if str(_SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_ROOT))

import argparse
import json
import signal
import subprocess
import time

try:
    import select
except ImportError:
    select = None  # Windows: no select on pipes; fall back to line-based snapshot

from src.kalshi_forward.paths import (
    CHECKPOINT_FILE,
    FORWARD_MARKETS_DIR,
    FORWARD_TRADES_DIR,
    PROJECT_ROOT,
    RUN_MANIFEST_DIR,
)


def human_bytes(n: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    value = float(n)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{n} B"


def snapshot_checkpoint() -> str:
    if not CHECKPOINT_FILE.exists():
        return "checkpoint: (missing yet)"
    try:
        data = json.loads(CHECKPOINT_FILE.read_text())
        return (
            "checkpoint: "
            f"trade_wm={data.get('watermark_trade_ts')} "
            f"market_wm={data.get('watermark_market_ts')} "
            f"total_trade_rows={data.get('total_trade_rows_written', 0)} "
            f"total_market_rows={data.get('total_market_rows_written', 0)}"
        )
    except Exception as exc:
        return f"checkpoint: unreadable ({exc})"


def snapshot_files() -> str:
    trade_files = list(FORWARD_TRADES_DIR.rglob("*.parquet")) if FORWARD_TRADES_DIR.exists() else []
    market_files = list(FORWARD_MARKETS_DIR.rglob("*.parquet")) if FORWARD_MARKETS_DIR.exists() else []

    total_bytes = 0
    for path in trade_files + market_files:
        try:
            total_bytes += path.stat().st_size
        except OSError:
            pass

    runs = len(list(RUN_MANIFEST_DIR.glob("*.json"))) if RUN_MANIFEST_DIR.exists() else 0
    return (
        "files: "
        f"trade_files={len(trade_files)} "
        f"market_files={len(market_files)} "
        f"size={human_bytes(total_bytes)} "
        f"run_manifests={runs}"
    )


def build_cmd(args: argparse.Namespace) -> list[str]:
    cmd = [sys.executable, str(PROJECT_ROOT / "scripts" / "update_forward.py")]
    if args.historical_only:
        cmd.append("--historical-only")
    if args.lookback_seconds is not None:
        cmd.extend(["--lookback-seconds", str(args.lookback_seconds)])
    if args.max_trade_pages is not None:
        cmd.extend(["--max-trade-pages", str(args.max_trade_pages)])
    if args.max_market_pages is not None:
        cmd.extend(["--max-market-pages", str(args.max_market_pages)])
    if args.dry_run:
        cmd.append("--dry-run")
    if getattr(args, "bootstrap_from_historical", None) is True:
        cmd.append("--bootstrap-from-historical")
    if args.start_ts is not None:
        cmd.extend(["--start-ts", str(args.start_ts)])
    return cmd


def _read_with_snapshot_interval(proc: subprocess.Popen, monitor_interval: float) -> int:
    """Read child stdout; print periodic snapshots on interval (Unix). On Windows, snapshots when lines arrive."""
    assert proc.stdout is not None
    start = time.time()
    next_snapshot = start
    read_timeout = 1.0

    while True:
        if select is not None:
            try:
                ready, _, _ = select.select([proc.stdout], [], [], read_timeout)
            except (ValueError, OSError):
                ready = []
            now = time.time()
            if now >= next_snapshot:
                elapsed = now - start
                print(
                    "[monitor]",
                    f"elapsed={elapsed:.1f}s",
                    "|",
                    snapshot_checkpoint(),
                    "|",
                    snapshot_files(),
                    flush=True,
                )
                next_snapshot = now + monitor_interval
            if proc.poll() is not None:
                remainder = proc.stdout.read()
                if remainder:
                    print(remainder.rstrip(), flush=True)
                break
            if ready:
                line = proc.stdout.readline()
                if line:
                    print(line.rstrip(), flush=True)
        else:
            # Windows: no select on pipes; snapshot only when we get a line
            line = proc.stdout.readline()
            if line:
                print(line.rstrip(), flush=True)
            now = time.time()
            if now >= next_snapshot:
                elapsed = now - start
                print(
                    "[monitor]",
                    f"elapsed={elapsed:.1f}s",
                    "|",
                    snapshot_checkpoint(),
                    "|",
                    snapshot_files(),
                    flush=True,
                )
                next_snapshot = now + monitor_interval
            if proc.poll() is not None:
                remainder = proc.stdout.read()
                if remainder:
                    print(remainder.rstrip(), flush=True)
                break

    return proc.returncode or 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Run update_forward.py with live monitoring")
    parser.add_argument(
        "--historical-only",
        action="store_true",
        default=False,
        help="Use API historical cutoff as upper bound (default: False = live now)",
    )
    parser.add_argument("--lookback-seconds", type=int, default=None)
    parser.add_argument("--max-trade-pages", type=int, default=None)
    parser.add_argument("--max-market-pages", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--bootstrap-from-historical", action="store_true", help="Bootstrap checkpoint from historical cutoff when missing")
    parser.add_argument("--start-ts", type=int, default=None, help="Bootstrap start timestamp when checkpoint missing")
    parser.add_argument("--monitor-interval", type=float, default=5.0)
    args = parser.parse_args()

    cmd = build_cmd(args)
    print("=" * 88)
    print("FORWARD INGESTION LIVE MONITOR")
    print("=" * 88)
    print("command:", " ".join(cmd))
    print("monitor interval:", f"{args.monitor_interval:.1f}s")
    print("-" * 88)

    proc = subprocess.Popen(
        cmd,
        cwd=PROJECT_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    def _forward_signal(signum, frame):
        del frame
        print(f"\n[monitor] received signal {signum}, forwarding to child...")
        if proc.poll() is None:
            proc.send_signal(signum)

    signal.signal(signal.SIGINT, _forward_signal)
    signal.signal(signal.SIGTERM, _forward_signal)

    rc = _read_with_snapshot_interval(proc, args.monitor_interval)

    print("-" * 88)
    print("[monitor] done", f"exit_code={rc}")
    print("[monitor]", snapshot_checkpoint())
    print("[monitor]", snapshot_files())
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
