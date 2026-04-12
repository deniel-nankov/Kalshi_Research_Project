"""Institutional maintenance shell must be syntactically valid (bash -n)."""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
MAINT = ROOT / "scripts" / "institutional_maintenance.sh"


def _bash_works() -> bool:
    if not shutil.which("bash"):
        return False
    try:
        subprocess.run(
            ["bash", "-c", "echo ok"],
            check=True,
            capture_output=True,
            timeout=5,
        )
        return True
    except (subprocess.CalledProcessError, FileNotFoundError, OSError, subprocess.TimeoutExpired):
        return False


@pytest.mark.skipif(sys.platform == "win32", reason="Git bash/WSL shims often break bash -n on Windows")
@pytest.mark.skipif(not _bash_works(), reason="working bash required")
def test_institutional_maintenance_bash_syntax() -> None:
    subprocess.check_call(["bash", "-n", str(MAINT)], cwd=str(ROOT))
