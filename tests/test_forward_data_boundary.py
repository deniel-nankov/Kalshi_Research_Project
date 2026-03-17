from __future__ import annotations

import json

import duckdb
import pytest

from src.kalshi_forward.paths import (
    FORWARD_TRADES_DIR,
    FORWARD_TRADES_GLOB,
    HISTORICAL_CHECKPOINT_FILE,
    HISTORICAL_MARKETS_FILE,
    HISTORICAL_TRADES_DIR,
    HISTORICAL_TRADES_GLOB,
)


def _has_historical_data() -> bool:
    return (
        HISTORICAL_CHECKPOINT_FILE.exists()
        and HISTORICAL_TRADES_DIR.exists()
        and HISTORICAL_MARKETS_FILE.exists()
    )


@pytest.mark.slow
def test_historical_created_times_not_after_checkpoint_cutoff():
    if not _has_historical_data():
        pytest.skip("Historical dataset not available in workspace")

    cutoff_iso = json.loads(HISTORICAL_CHECKPOINT_FILE.read_text()).get("cutoff_ts")
    assert cutoff_iso, "Missing cutoff_ts in historical checkpoint"

    con = duckdb.connect()
    try:
        max_trade_created = con.execute(
            f"SELECT MAX(TRY_CAST(created_time AS TIMESTAMP)) FROM '{HISTORICAL_TRADES_GLOB}'"
        ).fetchone()[0]
        max_market_created = con.execute(
            f"SELECT MAX(TRY_CAST(created_time AS TIMESTAMP)) FROM '{HISTORICAL_MARKETS_FILE}'"
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
def test_no_trade_id_overlap_between_historical_and_forward_when_forward_exists():
    """If forward data exists, ensure it does not duplicate historical trade_ids."""
    if not _has_historical_data():
        pytest.skip("Historical dataset not available in workspace")

    if not FORWARD_TRADES_DIR.exists() or not any(FORWARD_TRADES_DIR.rglob("*.parquet")):
        pytest.skip("No forward trades present; overlap check not applicable")

    con = duckdb.connect()
    try:
        overlap = con.execute(
            f"""
            SELECT COUNT(*)
            FROM (
                SELECT DISTINCT trade_id FROM '{HISTORICAL_TRADES_GLOB}' WHERE trade_id IS NOT NULL AND trade_id <> ''
            ) h
            INNER JOIN (
                SELECT DISTINCT trade_id FROM '{FORWARD_TRADES_GLOB}' WHERE trade_id IS NOT NULL AND trade_id <> ''
            ) i
            ON h.trade_id = i.trade_id
            """
        ).fetchone()[0]
    finally:
        con.close()

    assert overlap == 0
