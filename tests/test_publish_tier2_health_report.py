"""publish_tier2_observability rejects LFS pointer health_report content."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.publish_tier2_observability import _is_git_lfs_pointer_text, _load_report


def test_is_git_lfs_pointer_detects_spec() -> None:
    sample = "version https://git-lfs.github.com/spec/v1\noid sha256:abc\nsize 99\n"
    assert _is_git_lfs_pointer_text(sample) is True
    assert _is_git_lfs_pointer_text('{"summary": {}}') is False


def test_load_report_rejects_empty(tmp_path: Path) -> None:
    p = tmp_path / "health_report.json"
    p.write_text("   \n", encoding="utf-8")
    with pytest.raises(ValueError, match="empty"):
        _load_report(p)


def test_load_report_rejects_lfs_pointer(tmp_path: Path) -> None:
    p = tmp_path / "health_report.json"
    p.write_text(
        "version https://git-lfs.github.com/spec/v1\noid sha256:x\nsize 1\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="Git LFS pointer"):
        _load_report(p)


def test_load_report_accepts_json(tmp_path: Path) -> None:
    p = tmp_path / "health_report.json"
    p.write_text(json.dumps({"summary": {"pass": 1}, "checks": []}), encoding="utf-8")
    out = _load_report(p)
    assert out["summary"]["pass"] == 1
