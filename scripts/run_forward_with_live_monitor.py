#!/usr/bin/env python3
"""
Run forward catch-up ingestion with live terminal monitoring.

This wrapper executes scripts/update_forward.py and prints:
  • real-time ingestion logs from the child process
  • periodic checkpoint snapshot (watermarks, totals)
  • periodic file snapshot (incremental files + size)

Example:
  uv run python scripts/run_forward_with_live_monitor.py --historical-only
"""

from __future__ import annotations

import argparse
import json
import signal
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
STATE_CHECKPOINT = ROOT / "data" / "kalshi" / "state" / "forward_checkpoint.json"
INCR_TRADES_DIR = ROOT / "data" / "kalshi" / "incremental" / "trades"
INCR_MARKETS_DIR = ROOT / "data" / "kalshi" / "incremental" / "markets"
RUNS_DIR = ROOT / "data" / "kalshi" / "state" / "runs"


def human_bytes(n: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    value = float(n)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{n} B"


def snapshot_checkpoint() -> str:
    if not STATE_CHECKPOINT.exists():
        return "checkpoint: (missing yet)"
    try:
        data = json.loads(STATE_CHECKPOINT.read_text())
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
    trade_files = list(INCR_TRADES_DIR.rglob("*.parquet")) if INCR_TRADES_DIR.exists() else []
    market_files = list(INCR_MARKETS_DIR.rglob("*.parquet")) if INCR_MARKETS_DIR.exists() else []

    total_bytes = 0
    for path in trade_files + market_files:
        try:
            total_bytes += path.stat().st_size
        except OSError:
            pass

    runs = len(list(RUNS_DIR.glob("*.json"))) if RUNS_DIR.exists() else 0
    return (
        "files: "
        f"trade_files={len(trade_files)} "
        f"market_files={len(market_files)} "
        f"size={human_bytes(total_bytes)} "
        f"run_manifests={runs}"
    )


def build_cmd(args: argparse.Namespace) -> list[str]:
    cmd = [sys.executable, str(ROOT / "scripts" / "update_forward.py")]
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
    return cmd


def main() -> int:
    parser = argparse.ArgumentParser(description="Run update_forward.py with live monitoring")
    parser.add_argument("--historical-only", action="store_true", default=True)
    parser.add_argument("--lookback-seconds", type=int, default=None)
    parser.add_argument("--max-trade-pages", type=int, default=None)
    parser.add_argument("--max-market-pages", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--monitor-interval", type=float, default=5.0)
    args = parser.parse_args()

    cmd = build_cmd(args)
    print("=" * 88)
    print("FORWARD CATCH-UP LIVE MONITOR")
    print("=" * 88)
    print("command:", " ".join(cmd))
    print("monitor interval:", f"{args.monitor_interval:.1f}s")
    print("-" * 88)

    proc = subprocess.Popen(
        cmd,
        cwd=ROOT,
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

    assert proc.stdout is not None
    start = time.time()
    next_snapshot = start

    while True:
        line = proc.stdout.readline()
        if line:
            print(line.rstrip())

        now = time.time()
        if now >= next_snapshot:
            elapsed = now - start
            print("[monitor]", f"elapsed={elapsed:.1f}s", "|", snapshot_checkpoint(), "|", snapshot_files())
            next_snapshot = now + args.monitor_interval

        if proc.poll() is not None:
            # flush any remaining output
            remainder = proc.stdout.read()
            if remainder:
                print(remainder.rstrip())
            break

    rc = proc.returncode
    print("-" * 88)
    print("[monitor] done", f"exit_code={rc}")
    print("[monitor]", snapshot_checkpoint())
    print("[monitor]", snapshot_files())
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
