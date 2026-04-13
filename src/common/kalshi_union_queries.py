"""Build DuckDB `read_parquet` unions for the full Kalshi dataset (historical + forward).

Used by scripts that must stay aligned with `scripts/data_stats.py` path logic.
"""

from __future__ import annotations

import glob
from pathlib import Path

from src.kalshi_forward.paths import (
    FORWARD_MARKETS_GLOB,
    FORWARD_TRADES_GLOB,
    HISTORICAL_MARKETS_FILE,
    HISTORICAL_TRADES_GLOB,
    LEGACY_FORWARD_TRADES_GLOB,
)


def _posix(path: Path | str) -> str:
    return str(Path(path).resolve()).replace("\\", "/")


def trade_union_sql(*, cols: str, include_legacy_forward: bool = False) -> str:
    """SQL subquery body: ``SELECT {cols} FROM (...)`` sources combined with UNION ALL."""

    slices: list[str] = []
    if glob.glob(str(HISTORICAL_TRADES_GLOB)):
        slices.append(f"SELECT {cols} FROM read_parquet('{_posix(HISTORICAL_TRADES_GLOB)}')")
    if glob.glob(str(FORWARD_TRADES_GLOB)):
        slices.append(f"SELECT {cols} FROM read_parquet('{_posix(FORWARD_TRADES_GLOB)}')")
    if include_legacy_forward and glob.glob(str(LEGACY_FORWARD_TRADES_GLOB)):
        slices.append(f"SELECT {cols} FROM read_parquet('{_posix(LEGACY_FORWARD_TRADES_GLOB)}')")
    if not slices:
        raise FileNotFoundError("No trade parquet under historical/forward (and optional legacy) globs.")
    return " UNION ALL ".join(slices)


def markets_union_sql(*, cols: str) -> str:
    """SQL subquery body for historical markets file + all forward market shards."""
    slices: list[str] = []
    if HISTORICAL_MARKETS_FILE.exists():
        slices.append(f"SELECT {cols} FROM read_parquet('{_posix(HISTORICAL_MARKETS_FILE)}')")
    if glob.glob(str(FORWARD_MARKETS_GLOB)):
        slices.append(f"SELECT {cols} FROM read_parquet('{_posix(FORWARD_MARKETS_GLOB)}')")
    if not slices:
        raise FileNotFoundError("No markets data under historical file and/or forward_markets glob.")
    return " UNION ALL ".join(slices)
