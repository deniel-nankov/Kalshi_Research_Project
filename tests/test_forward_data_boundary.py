from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest


HIST_CHECKPOINT = Path("data/kalshi/historical/.checkpoint.json")
HIST_TRADES_GLOB = "data/kalshi/historical/trades/*.parquet"
HIST_MARKETS_FILE = "data/kalshi/historical/markets.parquet"
INCR_TRADES_GLOB = "data/kalshi/incremental/trades/*/*.parquet"


def _has_historical_data() -> bool:
    return HIST_CHECKPOINT.exists() and Path("data/kalshi/historical/trades").exists() and Path(HIST_MARKETS_FILE).exists()


@pytest.mark.slow
def test_historical_created_times_not_after_checkpoint_cutoff():
    if not _has_historical_data():
        pytest.skip("Historical dataset not available in workspace")

    cutoff_iso = json.loads(HIST_CHECKPOINT.read_text()).get("cutoff_ts")
    assert cutoff_iso, "Missing cutoff_ts in historical checkpoint"

    con = duckdb.connect()
    try:
        max_trade_created = con.execute(
            f"SELECT MAX(TRY_CAST(created_time AS TIMESTAMP)) FROM '{HIST_TRADES_GLOB}'"
        ).fetchone()[0]
        max_market_created = con.execute(
            f"SELECT MAX(TRY_CAST(created_time AS TIMESTAMP)) FROM '{HIST_MARKETS_FILE}'"
        ).fetchone()[0]
        cutoff_ts = con.execute("SELECT TRY_CAST(? AS TIMESTAMP)", [cutoff_iso]).fetchone()[0]
    finally:
        con.close()

    assert max_trade_created is not None
    assert max_market_created is not None
    assert cutoff_ts is not None

    assert max_trade_created <= cutoff_ts
    assert max_market_created <= cutoff_ts


@pytest.mark.slow
def test_no_trade_id_overlap_between_historical_and_incremental_when_incremental_exists():
    """If incremental data exists, ensure it does not duplicate historical trade_ids."""
    if not _has_historical_data():
        pytest.skip("Historical dataset not available in workspace")

    if not any(Path("data/kalshi/incremental/trades").rglob("*.parquet")):
        pytest.skip("No incremental trades present; overlap check not applicable")

    con = duckdb.connect()
    try:
        overlap = con.execute(
            f"""
            SELECT COUNT(*)
            FROM (
                SELECT DISTINCT trade_id FROM '{HIST_TRADES_GLOB}' WHERE trade_id IS NOT NULL AND trade_id <> ''
            ) h
            INNER JOIN (
                SELECT DISTINCT trade_id FROM '{INCR_TRADES_GLOB}' WHERE trade_id IS NOT NULL AND trade_id <> ''
            ) i
            ON h.trade_id = i.trade_id
            """
        ).fetchone()[0]
    finally:
        con.close()

    assert overlap == 0
