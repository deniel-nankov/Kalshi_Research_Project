"""Institutional ops console --once and snapshot writer."""

from __future__ import annotations

import importlib.util
import json
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def test_write_ops_snapshot_function_writes_file(tmp_path: Path) -> None:
    """Headless path used by --daemon / systemd calls write_ops_snapshot."""
    path = ROOT / "scripts" / "institutional_ops_console.py"
    spec = importlib.util.spec_from_file_location("_ioc_test", path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    snapf = tmp_path / "w.json"
    data = mod.write_ops_snapshot(snapf, None)
    assert snapf.is_file()
    assert data["generated_at"]


@pytest.mark.skipif(not shutil.which("uv"), reason="uv not on PATH")
def test_institutional_ops_console_once_writes_snapshot(tmp_path: Path) -> None:
    snap = tmp_path / "ops_snapshot.json"
    r = subprocess.run(
        [
            "uv",
            "run",
            "python",
            "scripts/institutional_ops_console.py",
            "--once",
            "--snapshot",
            str(snap),
        ],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert r.returncode == 0, r.stderr
    assert snap.is_file()
    data = json.loads(snap.read_text(encoding="utf-8"))
    assert "generated_at" in data
    assert "health_summary" in data
