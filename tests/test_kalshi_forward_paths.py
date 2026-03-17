"""Tests for shared Kalshi forward paths (one place, under historical)."""

from pathlib import Path

import pytest

from src.kalshi_forward.paths import (
    CHECKPOINT_FILE,
    FORWARD_MARKETS_DIR,
    FORWARD_TRADES_DIR,
    HISTORICAL_DIR,
    HISTORICAL_MARKETS_FILE,
    KALSHI_DATA_DIR,
    PROJECT_ROOT,
    STATE_DIR,
)


def test_project_root_is_repo_root():
    """PROJECT_ROOT should be the repo root (parent of src, scripts, data)."""
    assert PROJECT_ROOT.is_dir()
    assert (PROJECT_ROOT / "src").is_dir()
    assert (PROJECT_ROOT / "scripts").is_dir()
    assert (PROJECT_ROOT / "data").is_dir() or not (PROJECT_ROOT / "data").exists()


def test_forward_data_under_historical():
    """Forward trades and markets live under historical (one place)."""
    assert str(FORWARD_TRADES_DIR).startswith(str(HISTORICAL_DIR))
    assert str(FORWARD_MARKETS_DIR).startswith(str(HISTORICAL_DIR))
    assert FORWARD_TRADES_DIR == HISTORICAL_DIR / "forward_trades"
    assert FORWARD_MARKETS_DIR == HISTORICAL_DIR / "forward_markets"


def test_kalshi_data_dir_under_project_root():
    """Kalshi data is under project root."""
    assert str(KALSHI_DATA_DIR).startswith(str(PROJECT_ROOT))
    assert KALSHI_DATA_DIR == PROJECT_ROOT / "data" / "kalshi"


def test_state_and_checkpoint_under_kalshi_data():
    """State and checkpoint live under data/kalshi/state."""
    assert str(STATE_DIR).startswith(str(KALSHI_DATA_DIR))
    assert CHECKPOINT_FILE == STATE_DIR / "forward_checkpoint.json"


def test_historical_markets_file_path():
    """Historical markets single file path is correct."""
    assert HISTORICAL_MARKETS_FILE == HISTORICAL_DIR / "markets.parquet"
