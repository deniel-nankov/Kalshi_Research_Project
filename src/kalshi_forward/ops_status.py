"""Build a JSON-serializable ops snapshot for dashboards and S3 / CloudWatch-adjacent tooling."""

from __future__ import annotations

import json
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _safe_json_load(path: Path) -> Any | None:
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _latest_run_json(state_runs: Path) -> tuple[str | None, Any | None]:
    if not state_runs.is_dir():
        return None, None
    files = sorted(state_runs.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        return None, None
    p = files[0]
    return p.name, _safe_json_load(p)


def _systemd_kalshi_snippet() -> str | None:
    exe = shutil.which("systemctl")
    if not exe:
        return None
    try:
        out = subprocess.check_output(
            [exe, "list-timers", "--all", "--no-pager"],
            text=True,
            timeout=8,
            stderr=subprocess.DEVNULL,
        )
    except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return None
    lines = [ln for ln in out.splitlines() if "kalshi" in ln.lower()]
    return "\n".join(lines) if lines else "(no kalshi timers in list)"


def build_ops_snapshot(project_root: Path) -> dict[str, Any]:
    """Single dict for ops console + optional upload to S3 (Tier 2)."""
    state = project_root / "data" / "kalshi" / "state"
    health_path = state / "health_report.json"
    ckpt_path = state / "forward_checkpoint.json"
    hist_ckpt = project_root / "data" / "kalshi" / "historical" / ".checkpoint.json"

    health = _safe_json_load(health_path)
    summary = (health or {}).get("summary") if isinstance(health, dict) else None
    run_name, run_body = _latest_run_json(state / "runs")

    def mtime_iso(p: Path) -> str | None:
        if not p.is_file():
            return None
        try:
            ts = p.stat().st_mtime
            return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
        except OSError:
            return None

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "health_report_path": str(health_path),
        "health_report_mtime": mtime_iso(health_path),
        "health_summary": summary,
        "forward_checkpoint": _safe_json_load(ckpt_path),
        "historical_checkpoint": _safe_json_load(hist_ckpt),
        "latest_run_file": run_name,
        "latest_run": run_body,
        "systemd_kalshi_timers": _systemd_kalshi_snippet(),
    }
