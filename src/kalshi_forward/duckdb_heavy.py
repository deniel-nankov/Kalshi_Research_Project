"""
DuckDB settings for large out-of-core queries (dedupe, window functions).

``SET`` after ``duckdb.connect()`` often does *not* raise ``max_temp_directory_size``
effectively for :memory: workloads. Pass ``config=`` at connect time instead.

For ~100M+ row window aggregates, pair a **low memory_limit** (forces spill) with a
**large max_temp_directory_size** (from free disk minus a small reserve).
"""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

import duckdb


def connect_for_dedupe_spill(
    temp_dir: Path,
    *,
    reserve_gib: float = 1.5,
    threads: int = 2,
    max_temp_gib: float | None = None,
    memory_limit_gb: float = 6.0,
) -> tuple[duckdb.DuckDBPyConnection, dict[str, Any]]:
    """
    Open DuckDB with temp + spill limits at connection time.

    - ``memory_limit_gb``: cap process RAM for DuckDB; lower → more spill to disk
      (often needed so the window aggregate does not exhaust ``max_temp_directory_size``).
    - ``reserve_gib``: GiB left free on the volume when sizing temp cap.
    """
    temp_dir = temp_dir.resolve()
    temp_dir.mkdir(parents=True, exist_ok=True)
    temp_s = str(temp_dir)

    _total_b, _used_b, free_b = shutil.disk_usage(temp_dir)
    free_gib = free_b / (1024**3)
    reserve_b = int(max(0.5, float(reserve_gib)) * (1024**3))
    derived_cap_gib = max(2.0, (free_b - reserve_b) / (1024**3))

    if max_temp_gib is not None:
        cap_gib = min(float(max_temp_gib), max(2.0, free_gib - 0.5))
    else:
        cap_gib = derived_cap_gib

    cap_gib_int = max(2, int(cap_gib))
    cap_str = f"{cap_gib_int}GiB"

    cfg: dict[str, Any] = {
        "temp_directory": temp_s,
        "max_temp_directory_size": cap_str,
        "threads": int(threads),
        "preserve_insertion_order": False,
    }
    if memory_limit_gb and memory_limit_gb > 0:
        cfg["memory_limit"] = f"{float(memory_limit_gb):.1f}GB"

    con = duckdb.connect(":memory:", config=cfg)

    meta = {
        "temp_directory": temp_s,
        "max_temp_directory_size": cap_str,
        "memory_limit_gb": memory_limit_gb if memory_limit_gb > 0 else "(default)",
        "disk_free_gib": round(free_gib, 2),
        "reserve_gib": reserve_gib,
        "threads": int(threads),
    }
    return con, meta
