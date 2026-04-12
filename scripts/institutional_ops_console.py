#!/usr/bin/env python3
"""
Institutional ops console - always-on terminal view of pipeline health + checkpoints.

Writes a machine-readable snapshot (default: data/kalshi/state/ops_snapshot.json) on each
refresh so Tier 2 can upload it to S3 next to health metrics (set UPLOAD_OPS_SNAPSHOT=1 in
observability.env). On AWS, open CloudWatch metrics (KalshiData) and S3 tier2/ prefixes;
optionally ship ops_console.log via CloudWatch Agent (see infra/aws/CLOUDWATCH_OPS_LOGS.txt).

Usage:
  uv run python scripts/institutional_ops_console.py
  uv run python scripts/institutional_ops_console.py --interval 15 --snapshot data/kalshi/state/ops_snapshot.json
  uv run python scripts/institutional_ops_console.py --once   # print snapshot JSON and exit
  uv run python scripts/institutional_ops_console.py --daemon --interval 300   # headless (systemd)
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.kalshi_forward.ops_status import build_ops_snapshot  # noqa: E402


def write_ops_snapshot(snapshot_path: Path, log_file: Path | None) -> dict[str, Any]:
    snap = build_ops_snapshot(_PROJECT_ROOT)
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot_path.write_text(json.dumps(snap, indent=2, default=str), encoding="utf-8")
    if log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(snap, separators=(",", ":"), default=str)
        with log_file.open("a", encoding="utf-8") as lf:
            lf.write(line + "\n")
    return snap


def _format_snapshot(snapshot: dict[str, Any]) -> Table:
    t = Table(title="Ops snapshot", expand=True)
    t.add_column("Field", style="cyan")
    t.add_column("Value", style="white")
    t.add_row("generated_at", str(snapshot.get("generated_at", "")))
    hs = snapshot.get("health_summary")
    if isinstance(hs, dict):
        t.add_row("health pass/warn/fail", f"{hs.get('pass')}/{hs.get('warn')}/{hs.get('fail')}")
    else:
        t.add_row("health_summary", "(no health_report.json)")
    t.add_row("health_report_mtime", str(snapshot.get("health_report_mtime") or "-"))
    ck = snapshot.get("forward_checkpoint")
    if isinstance(ck, dict):
        dumped = json.dumps(ck, default=str)
        t.add_row("forward_checkpoint", dumped[:200] + ("..." if len(dumped) > 200 else ""))
    else:
        t.add_row("forward_checkpoint", "(absent)")
    t.add_row("latest_run_file", str(snapshot.get("latest_run_file") or "-"))
    sysd = snapshot.get("systemd_kalshi_timers")
    t.add_row("systemd (kalshi)", (sysd or "N/A (no systemctl or not Linux host)")[:500])
    return t


def main() -> int:
    p = argparse.ArgumentParser(description="Institutional ops console")
    p.add_argument("--interval", type=float, default=10.0, help="Seconds between refreshes (live UI or daemon)")
    p.add_argument(
        "--snapshot",
        type=Path,
        default=_PROJECT_ROOT / "data/kalshi/state/ops_snapshot.json",
        help="Write JSON snapshot here each refresh",
    )
    p.add_argument("--once", action="store_true", help="Write snapshot + print JSON to stdout, then exit")
    p.add_argument(
        "--daemon",
        action="store_true",
        help="Headless loop for systemd: write snapshot every --interval (no Rich UI)",
    )
    p.add_argument(
        "--log-file",
        type=Path,
        default=None,
        help="Append one-line JSON log per refresh (for CloudWatch Logs agent tailing)",
    )
    args = p.parse_args()

    if args.once and args.daemon:
        p.error("Use only one of --once or --daemon")

    if args.daemon:
        if args.interval < 5:
            print("institutional_ops_console: --interval < 5s in daemon mode is not recommended", flush=True)
        print(
            f"institutional_ops_console: daemon started interval={args.interval}s snapshot={args.snapshot}",
            flush=True,
        )
        while True:
            write_ops_snapshot(args.snapshot, args.log_file)
            time.sleep(args.interval)

    if args.once:
        snap = write_ops_snapshot(args.snapshot, args.log_file)
        print(json.dumps(snap, indent=2, default=str))
        return 0

    console = Console()

    def tick() -> Panel:
        snap = write_ops_snapshot(args.snapshot, args.log_file)
        title = f"Kalshi ops - {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}"
        inner = Group(_format_snapshot(snap), Text(f"Snapshot: {args.snapshot}", style="dim"))
        return Panel(inner, title=title, border_style="blue")

    console.print("[dim]Ctrl+C to quit[/dim]")
    try:
        with Live(tick(), refresh_per_second=4, console=console) as live:
            while True:
                time.sleep(args.interval)
                live.update(tick())
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
