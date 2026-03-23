"""Single source of truth for Kalshi data paths and API config.

All paths are resolved from the project root so scripts work regardless of cwd.
Historical and forward data live under data/kalshi/historical/ (one place).
"""

from __future__ import annotations

from pathlib import Path

# Project root (repo root). paths.py lives in src/kalshi_forward/paths.py.
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# ─── Data root ─────────────────────────────────────────────────────────────
KALSHI_DATA_DIR = PROJECT_ROOT / "data" / "kalshi"
HISTORICAL_DIR = KALSHI_DATA_DIR / "historical"

# Historical data (from download_historical.py)
HISTORICAL_TRADES_DIR = HISTORICAL_DIR / "trades"
HISTORICAL_MARKETS_FILE = HISTORICAL_DIR / "markets.parquet"
HISTORICAL_CHECKPOINT_FILE = HISTORICAL_DIR / ".checkpoint.json"

# Forward (incremental) data — stored under historical so everything is in one place
FORWARD_TRADES_DIR = HISTORICAL_DIR / "forward_trades"
FORWARD_MARKETS_DIR = HISTORICAL_DIR / "forward_markets"

# State (checkpoints, lock, run manifests)
STATE_DIR = KALSHI_DATA_DIR / "state"
CHECKPOINT_FILE = STATE_DIR / "forward_checkpoint.json"
RUN_MANIFEST_DIR = STATE_DIR / "runs"
LOCK_FILE = STATE_DIR / "forward_ingestion.lock"

# Globs for reading (historical + forward)
HISTORICAL_TRADES_GLOB = HISTORICAL_TRADES_DIR / "*.parquet"
FORWARD_TRADES_GLOB = FORWARD_TRADES_DIR / "*" / "*.parquet"
FORWARD_MARKETS_GLOB = FORWARD_MARKETS_DIR / "*" / "*.parquet"

# Legacy locations (backward compatibility if they exist)
LEGACY_TRADES_DIR = KALSHI_DATA_DIR / "trades"
LEGACY_MARKETS_DIR = KALSHI_DATA_DIR / "markets"
LEGACY_TRADES_GLOB = LEGACY_TRADES_DIR / "*.parquet"
LEGACY_MARKETS_GLOB = LEGACY_MARKETS_DIR / "*.parquet"
# Old forward layout (before consolidating under historical/)
LEGACY_FORWARD_TRADES_GLOB = KALSHI_DATA_DIR / "incremental" / "trades" / "*" / "*.parquet"
LEGACY_FORWARD_MARKETS_GLOB = KALSHI_DATA_DIR / "incremental" / "markets" / "*" / "*.parquet"

# ─── API ──────────────────────────────────────────────────────────────────
BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
HISTORICAL_MARKETS_PATH = "/historical/markets"
LIVE_MARKETS_PATH = "/markets"
# Backward-compat alias (prefer explicit HISTORICAL_MARKETS_PATH/LIVE_MARKETS_PATH)
MARKETS_PATH = HISTORICAL_MARKETS_PATH
MARKET_TRADES_PATH = "/markets/trades"
HISTORICAL_CUTOFF_PATH = "/historical/cutoff"
CUTOFF_URL = f"{BASE_URL}{HISTORICAL_CUTOFF_PATH}"
