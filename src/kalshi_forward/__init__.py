"""Shared config and paths for Kalshi forward (incremental) ingestion.

Forward data is stored under the same tree as historical data so all
Kalshi dataset files live in one place: data/kalshi/historical/.
"""

from src.kalshi_forward.paths import (
    CHECKPOINT_FILE,
    FORWARD_MARKETS_DIR,
    FORWARD_TRADES_DIR,
    HISTORICAL_CHECKPOINT_FILE,
    HISTORICAL_DIR,
    HISTORICAL_MARKETS_FILE,
    KALSHI_DATA_DIR,
    LOCK_FILE,
    PROJECT_ROOT,
    RUN_MANIFEST_DIR,
    STATE_DIR,
)

__all__ = [
    "CHECKPOINT_FILE",
    "FORWARD_MARKETS_DIR",
    "FORWARD_TRADES_DIR",
    "HISTORICAL_CHECKPOINT_FILE",
    "HISTORICAL_DIR",
    "HISTORICAL_MARKETS_FILE",
    "KALSHI_DATA_DIR",
    "LOCK_FILE",
    "PROJECT_ROOT",
    "RUN_MANIFEST_DIR",
    "STATE_DIR",
]
