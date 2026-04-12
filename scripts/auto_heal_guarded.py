#!/usr/bin/env python3
"""
Optional self-heal after a WARN health report — **very limited by design**.

What runs automatically (only if you turn it on):
  - **Orphan tickers (REFERENTIAL_INTEGRITY WARN):** run a *small* batch of
    `fix_orphan_tickers.py` with a checkpoint file, so each scheduled run heals
    a little without slamming the Kalshi API.

What does **not** run automatically (unsafe or not meaningful without you):
  - **Duplicate markets (UNIQUENESS_MARKETS WARN):** dedupe rewrites Parquet; the
    repo expects you to **stop** `kalshi-forward` first — use
    `infra/aws/server-data-perfection.sh repair` in a maintenance window.
  - **Other WARNs** (boundary, cutoff fetch, count=0 patterns, etc.): usually need
    human judgment or a config/API fix, not a blind script.

Enable on a server:
  - Set `AUTO_HEAL_ORPHANS_MAX` to a positive number in `/etc/kalshi/auto-heal.env`
    (see `infra/aws/auto-heal.env.example`) and enable `kalshi-auto-heal.timer`.

Environment:
  AUTO_HEAL_ORPHANS_MAX   — max orphan tickers to process this run (0 = disabled, default 0)
  AUTO_HEAL_ORPHANS_DELAY — seconds between API calls (default 0.25)
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
STATE_DIR = PROJECT_ROOT / "data" / "kalshi" / "state"
DEFAULT_HEALTH = STATE_DIR / "health_report.json"
DEFAULT_CHECKPOINT = STATE_DIR / "orphan_auto_heal_checkpoint.txt"


def _load_health(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _referential(report: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    for c in report.get("checks", []):
        if c.get("name") == "REFERENTIAL_INTEGRITY":
            return str(c.get("status") or ""), dict(c.get("details") or {})
    return "", {}


def main() -> int:
    p = argparse.ArgumentParser(description="Guarded auto-heal from health_report WARNs")
    p.add_argument("--health-report", type=Path, default=DEFAULT_HEALTH)
    p.add_argument("--dry-run", action="store_true", help="Print actions only; no API calls")
    args = p.parse_args()

    max_orphans = int(os.environ.get("AUTO_HEAL_ORPHANS_MAX", "0"))
    delay = float(os.environ.get("AUTO_HEAL_ORPHANS_DELAY", "0.25"))

    if max_orphans <= 0:
        print("auto_heal_guarded: AUTO_HEAL_ORPHANS_MAX is 0 or unset - auto-heal disabled (no-op).")
        return 0

    if not args.health_report.is_file():
        print(f"auto_heal_guarded: no health report at {args.health_report}; skipping.", file=sys.stderr)
        return 0

    report = _load_health(args.health_report)
    status, details = _referential(report)
    orphans = int(details.get("orphan_tickers") or 0)

    if status == "PASS":
        print("auto_heal_guarded: REFERENTIAL_INTEGRITY PASS - nothing to heal.")
        return 0

    if status == "FAIL":
        print(
            "auto_heal_guarded: REFERENTIAL_INTEGRITY FAIL - not running auto-heal; fix the failure first.",
            file=sys.stderr,
        )
        return 0

    if status != "WARN" or orphans <= 0:
        print(f"auto_heal_guarded: REFERENTIAL_INTEGRITY status={status!r} orphans={orphans} - no orphan heal.")
        return 0

    cap = min(max_orphans, orphans)
    print(
        f"auto_heal_guarded: REFERENTIAL_INTEGRITY WARN ({orphans:,} orphan tickers in report). "
        f"Will process up to {cap:,} this run (AUTO_HEAL_ORPHANS_MAX={max_orphans})."
    )

    uv = os.environ.get("UV_BIN", "uv")
    cmd = [
        uv,
        "run",
        "python",
        "scripts/fix_orphan_tickers.py",
        "--max",
        str(cap),
        "--delay",
        str(delay),
        "--checkpoint",
        str(DEFAULT_CHECKPOINT),
    ]
    if args.dry_run:
        print("[dry-run]", " ".join(cmd))
        return 0

    proc = subprocess.run(cmd, cwd=str(PROJECT_ROOT))
    return int(proc.returncode)


if __name__ == "__main__":
    raise SystemExit(main())
