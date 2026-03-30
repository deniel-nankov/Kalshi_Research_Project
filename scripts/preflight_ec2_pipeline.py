#!/usr/bin/env python3
"""
Server preflight: verify .env keys present, Parquet under data/kalshi are real files
(not Git LFS pointers), and DuckDB can read each file. Exit 0 = OK to run update_forward.

Usage (on EC2, repo root):
  uv run python scripts/preflight_ec2_pipeline.py
  uv run python scripts/preflight_ec2_pipeline.py --strict   # exit 1 if any bad file
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

_SCRIPT_ROOT = Path(__file__).resolve().parents[1]
if str(_SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_ROOT))

import duckdb
from dotenv import load_dotenv

from src.kalshi_forward.paths import (
    HISTORICAL_CHECKPOINT_FILE,
    HISTORICAL_DIR,
    HISTORICAL_MARKETS_FILE,
    HISTORICAL_TRADES_DIR,
    PROJECT_ROOT,
)

load_dotenv(PROJECT_ROOT / ".env")


def _is_git_lfs_pointer(path: Path) -> bool:
    try:
        head = path.read_bytes()[:200]
    except OSError:
        return False
    return head.startswith(b"version https://git-lfs.github.com/spec/v1")


def _check_parquet(path: Path) -> tuple[bool, str]:
    if not path.is_file():
        return False, "missing"
    if _is_git_lfs_pointer(path):
        return False, "git_lfs_pointer (run: git lfs pull — or rsync real Parquet from a machine that has full files)"
    try:
        con = duckdb.connect()
        try:
            con.execute("SELECT 1 FROM read_parquet(?) LIMIT 1", [str(path.resolve())]).fetchone()
        finally:
            con.close()
    except Exception as exc:
        return False, str(exc)[:200]
    return True, "ok"


def _check_json_optional(path: Path, label: str) -> None:
    if not path.exists():
        print(f"  {label}: (absent — optional)")
        return
    raw = path.read_text().strip()
    if not raw:
        print(f"  {label}: EMPTY — fix or remove; forward will bootstrap with watermark 0")
        return
    if raw.startswith("version https://git-lfs.github.com/spec/v1"):
        print(f"  {label}: GIT LFS POINTER — not valid JSON; run git lfs pull or copy real file")
        return
    print(f"  {label}: present ({len(raw)} chars)")


def main() -> int:
    import argparse

    p = argparse.ArgumentParser(description="EC2 / server pipeline preflight")
    p.add_argument("--strict", action="store_true", help="Exit 1 if any parquet fails")
    args = p.parse_args()

    print("=" * 72)
    print("PREFLIGHT — Kalshi pipeline (read-only checks)")
    print("=" * 72)
    print(f"Project root: {PROJECT_ROOT}")

    kid = os.environ.get("KALSHI_API_KEY_ID", "").strip()
    pk = os.environ.get("KALSHI_API_PRIVATE_KEY", "").strip()
    if not kid or not pk:
        print("FAIL: KALSHI_API_KEY_ID and/or KALSHI_API_PRIVATE_KEY missing from environment (.env)")
        return 1
    print(f"OK: API key id present ({len(kid)} chars), private key material present ({len(pk)} chars)")

    print("\n--- JSON metadata ---")
    _check_json_optional(HISTORICAL_CHECKPOINT_FILE, "historical .checkpoint.json")

    print("\n--- Parquet files ---")
    bad = 0
    total = 0
    if HISTORICAL_MARKETS_FILE.exists():
        total += 1
        ok, msg = _check_parquet(HISTORICAL_MARKETS_FILE)
        print(f"  markets: {HISTORICAL_MARKETS_FILE.name} -> {msg}")
        if not ok:
            bad += 1
    else:
        print(f"  markets: {HISTORICAL_MARKETS_FILE} (absent)")

    if HISTORICAL_TRADES_DIR.is_dir():
        pq_files = sorted(HISTORICAL_TRADES_DIR.glob("*.parquet"))
        print(f"  historical trades: {len(pq_files)} *.parquet under {HISTORICAL_TRADES_DIR.name}/")
        max_verbose = 40
        for i, f in enumerate(pq_files):
            total += 1
            ok, msg = _check_parquet(f)
            if not ok:
                bad += 1
                print(f"    [BAD] {f.name}: {msg}")
            elif i < max_verbose:
                print(f"    [OK] {f.name}")
        if len(pq_files) > max_verbose:
            print(f"    ... ({len(pq_files) - max_verbose} additional OK files not listed)")
    else:
        print(f"  historical trades dir: absent ({HISTORICAL_TRADES_DIR})")

    print("\n" + "=" * 72)
    if bad:
        print(f"SUMMARY: {bad} unreadable file(s) of {total} checked.")
        print("Fix: replace corrupt files, or `git lfs pull` in the repo, or rsync Parquet from your Mac.")
        if args.strict:
            return 1
        print("(--strict not set: exit 0 anyway; update_forward will skip bad files with warnings.)")
        return 0
    print("SUMMARY: all checked Parquet files are readable.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
