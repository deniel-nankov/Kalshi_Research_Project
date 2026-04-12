"""Tests for ops snapshot used by institutional_ops_console and Tier 2 S3."""

from __future__ import annotations

import json
from pathlib import Path

from src.kalshi_forward.ops_status import build_ops_snapshot


def test_build_ops_snapshot_minimal(tmp_path: Path) -> None:
    root = tmp_path
    state = root / "data" / "kalshi" / "state"
    runs = state / "runs"
    runs.mkdir(parents=True)
    (runs / "run_a.json").write_text('{"ok": true}', encoding="utf-8")
    (state / "health_report.json").write_text(
        json.dumps({"timestamp": "2026-01-01T00:00:00+00:00", "summary": {"pass": 1, "warn": 0, "fail": 0}}),
        encoding="utf-8",
    )
    snap = build_ops_snapshot(root)
    assert snap["health_summary"]["pass"] == 1
    assert snap["latest_run_file"] == "run_a.json"
    assert snap["latest_run"] == {"ok": True}
    assert "generated_at" in snap
