"""sync_verified_dataset_to_s3.sh must be valid bash."""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "sync_verified_dataset_to_s3.sh"


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


@pytest.mark.skipif(sys.platform == "win32", reason="bash -n often unreliable on Windows")
@pytest.mark.skipif(not _bash_works(), reason="working bash required")
def test_sync_verified_dataset_s3_bash_syntax() -> None:
    subprocess.check_call(["bash", "-n", str(SCRIPT)], cwd=str(ROOT))
