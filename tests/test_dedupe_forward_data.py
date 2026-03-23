"""Tests for live-safe forward dedupe semantics (DuckDB row_number)."""

from __future__ import annotations

import sys
from pathlib import Path

_SCRIPT_ROOT = Path(__file__).resolve().parents[1]
if str(_SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_ROOT))

import duckdb  # noqa: E402
import pyarrow as pa  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402

from src.kalshi_forward.duckdb_heavy import connect_for_dedupe_spill  # noqa: E402


def _write_trades(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, path, compression="zstd")


def test_global_trade_dedupe_keeps_latest_created_time(tmp_path: Path) -> None:
    """Same trade_id in two files: keep row with latest created_time."""
    f1 = tmp_path / "a" / "t1.parquet"
    f2 = tmp_path / "b" / "t2.parquet"
    _write_trades(
        f1,
        [
            {
                "trade_id": "x",
                "ticker": "T-A",
                "created_time": "2025-01-01T00:00:00Z",
                "taker_side": "yes",
                "count": 1,
                "yes_price": 50,
                "no_price": 50,
                "price": 0.5,
                "count_fp": "1",
                "yes_price_dollars": "0.5",
                "no_price_dollars": "0.5",
            }
        ],
    )
    _write_trades(
        f2,
        [
            {
                "trade_id": "x",
                "ticker": "T-A",
                "created_time": "2025-06-01T00:00:00Z",
                "taker_side": "yes",
                "count": 1,
                "yes_price": 50,
                "no_price": 50,
                "price": 0.5,
                "count_fp": "1",
                "yes_price_dollars": "0.5",
                "no_price_dollars": "0.5",
            }
        ],
    )

    rp = f"read_parquet(['{f1.as_posix()}', '{f2.as_posix()}'], filename=true)"
    con = duckdb.connect()
    try:
        one = con.execute(
            f"""
            SELECT trade_id, filename, created_time FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY trade_id
                    ORDER BY TRY_CAST(created_time AS TIMESTAMP) DESC NULLS LAST, filename DESC
                ) AS rn
                FROM {rp}
            ) s WHERE rn = 1
            """
        ).fetchall()
        assert len(one) == 1
        assert one[0][0] == "x"
        assert "t2.parquet" in one[0][1]
    finally:
        con.close()


def test_market_dedupe_key_matches_validator(tmp_path: Path) -> None:
    f1 = tmp_path / "m1.parquet"
    _write_trades(
        f1,
        [
            {
                "ticker": "M1",
                "close_time": "2025-01-01T00:00:00Z",
                "created_time": "2024-12-01T00:00:00Z",
                "updated_time": "2025-01-02T00:00:00Z",
                "event_ticker": "E",
                "market_type": "binary",
                "title": "t",
                "status": "open",
                "volume": 0,
                "open_interest": 0,
                "dollar_volume": 0.0,
            },
            {
                "ticker": "M1",
                "close_time": "2025-01-01T00:00:00Z",
                "created_time": "2024-12-01T00:00:00Z",
                "updated_time": "2025-01-03T00:00:00Z",
                "event_ticker": "E",
                "market_type": "binary",
                "title": "t2",
                "status": "open",
                "volume": 1,
                "open_interest": 0,
                "dollar_volume": 0.0,
            },
        ],
    )
    con = duckdb.connect()
    try:
        n = con.execute(
            f"""
            SELECT COUNT(*) FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY ticker, COALESCE(close_time, created_time, '')
                    ORDER BY TRY_CAST(updated_time AS TIMESTAMP) DESC NULLS LAST
                ) AS rn
                FROM read_parquet('{f1.as_posix()}')
            ) s WHERE rn = 1
            """
        ).fetchone()[0]
        assert n == 1
    finally:
        con.close()


def test_connect_for_dedupe_spill_sets_memory_and_temp(tmp_path: Path) -> None:
    con, meta = connect_for_dedupe_spill(
        tmp_path / "spill",
        reserve_gib=1.0,
        threads=2,
        memory_limit_gb=4.0,
    )
    try:
        assert "temp_directory" in meta
        assert "max_temp_directory_size" in meta
        row = con.execute(
            "SELECT name, value FROM duckdb_settings() WHERE name IN ('memory_limit', 'max_temp_directory_size')"
        ).fetchall()
        names = {r[0]: r[1] for r in row}
        assert "memory_limit" in names
        assert "max_temp_directory_size" in names
    finally:
        con.close()
