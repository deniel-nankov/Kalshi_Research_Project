from __future__ import annotations

import importlib.util
import json
import sys
from argparse import Namespace
from pathlib import Path
from uuid import uuid4

import duckdb


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "update_forward.py"


def load_update_forward_module():
    module_name = f"update_forward_{uuid4().hex}"
    spec = importlib.util.spec_from_file_location(module_name, SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def patch_paths(module, tmp_path: Path):
    root = tmp_path / "data" / "kalshi"
    hist = root / "historical"
    module.CHECKPOINT_FILE = root / "state" / "forward_checkpoint.json"
    module.RUN_MANIFEST_DIR = root / "state" / "runs"
    module.LOCK_FILE = root / "state" / "forward_ingestion.lock"
    module.FORWARD_TRADES_DIR = hist / "forward_trades"
    module.FORWARD_MARKETS_DIR = hist / "forward_markets"
    module.FORWARD_TRADES_GLOB = module.FORWARD_TRADES_DIR / "*" / "*.parquet"
    module.FORWARD_MARKETS_GLOB = module.FORWARD_MARKETS_DIR / "*" / "*.parquet"
    module.HISTORICAL_CHECKPOINT_FILE = hist / ".checkpoint.json"
    module.HISTORICAL_TRADES_GLOB = hist / "trades" / "*.parquet"
    module.HISTORICAL_MARKETS_FILE = hist / "markets.parquet"
    module.LEGACY_TRADES_GLOB = root / "trades" / "*.parquet"
    module.LEGACY_MARKETS_GLOB = root / "markets" / "*.parquet"


def base_args(**overrides):
    defaults = dict(
        dry_run=False,
        historical_only=True,
        lookback_seconds=86400,
        bootstrap_from_historical=False,
        start_ts=0,
        max_trade_pages=None,
        max_market_pages=None,
    )
    defaults.update(overrides)
    return Namespace(**defaults)


def sample_trade(trade_id: str, created_time: str):
    return {
        "trade_id": trade_id,
        "ticker": "TICK-1",
        "taker_side": "yes",
        "count": 10,
        "yes_price": 55,
        "no_price": 45,
        "price": 0.55,
        "created_time": created_time,
        "count_fp": "10",
        "yes_price_dollars": "0.55",
        "no_price_dollars": "0.45",
        "_run_id": "run",
        "_ingested_at": "2026-03-14T00:00:00Z",
        "_source": "forward_trades",
    }


def sample_market(key_ts: str):
    return {
        "ticker": "TICK-1",
        "event_ticker": "EV-1",
        "market_type": "binary",
        "title": "Title",
        "subtitle": "",
        "yes_sub_title": "",
        "no_sub_title": "",
        "status": "open",
        "result": "",
        "yes_bid": 50,
        "yes_ask": 51,
        "no_bid": 49,
        "no_ask": 50,
        "last_price": 50,
        "previous_price": 49,
        "previous_yes_bid": 48,
        "previous_yes_ask": 49,
        "volume": 0,
        "volume_24h": 0,
        "open_interest": 0,
        "liquidity": 0,
        "tick_size": 1,
        "strike_type": "",
        "floor_strike": 0.0,
        "cap_strike": 0.0,
        "can_close_early": False,
        "rules_primary": "",
        "rules_secondary": "",
        "expiration_value": "",
        "settlement_value": "",
        "open_time": "",
        "close_time": "",
        "created_time": key_ts,
        "updated_time": key_ts,
        "expected_expiration_time": "",
        "expiration_time": "",
        "latest_expiration_time": "",
        "settlement_ts": "",
        "notional_value": 100,
        "dollar_volume": 0.0,
        "dollar_open_interest": 0.0,
        "_effective_ts": 1741910400,
        "_run_id": "run",
        "_ingested_at": "2026-03-14T00:00:00Z",
        "_source": "forward_markets",
    }


def test_dry_run_does_not_persist_checkpoint_or_parquet(tmp_path, monkeypatch):
    module = load_update_forward_module()
    patch_paths(module, tmp_path)

    monkeypatch.setattr(module, "get_upper_bound_ts", lambda client, historical_only: (1741910400, {"mode": "test"}))
    monkeypatch.setattr(module, "fetch_trade_deltas", lambda **kwargs: [sample_trade("a", "2025-03-13T23:00:00Z")])
    monkeypatch.setattr(module, "fetch_market_deltas", lambda **kwargs: [sample_market("2025-03-13T23:00:00Z")])

    module.run(base_args(dry_run=True))

    assert not module.CHECKPOINT_FILE.exists()
    assert not list(module.FORWARD_TRADES_DIR.rglob("*.parquet"))
    assert not list(module.FORWARD_MARKETS_DIR.rglob("*.parquet"))
    # Dry run must not write run manifest
    assert not module.RUN_MANIFEST_DIR.exists() or len(list(module.RUN_MANIFEST_DIR.glob("*.json"))) == 0


def test_run_writes_deduped_rows_and_advances_checkpoint(tmp_path, monkeypatch):
    module = load_update_forward_module()
    patch_paths(module, tmp_path)

    monkeypatch.setattr(module, "get_upper_bound_ts", lambda client, historical_only: (1741910400, {"mode": "test"}))
    monkeypatch.setattr(module, "load_existing_trade_ids", lambda window_start_ts: {"already"})
    monkeypatch.setattr(module, "load_existing_market_keys", lambda window_start_ts: {"TICK-1|2025-03-13T22:00:00Z"})

    def fake_fetch_trade_deltas(**kwargs):
        return [
            sample_trade("already", "2025-03-13T23:00:00Z"),  # existing
            sample_trade("dup-in-run", "2025-03-13T23:01:00Z"),
            sample_trade("dup-in-run", "2025-03-13T23:01:00Z"),  # duplicate in run
            sample_trade("new", "2025-03-13T23:02:00Z"),
        ]

    def fake_fetch_market_deltas(**kwargs):
        return [
            sample_market("2025-03-13T22:00:00Z"),  # existing key
            sample_market("2025-03-13T23:00:00Z"),
            sample_market("2025-03-13T23:00:00Z"),  # duplicate in run
        ]

    monkeypatch.setattr(module, "fetch_trade_deltas", fake_fetch_trade_deltas)
    monkeypatch.setattr(module, "fetch_market_deltas", fake_fetch_market_deltas)

    module.run(base_args(dry_run=False, lookback_seconds=0))

    assert module.CHECKPOINT_FILE.exists()
    checkpoint = json.loads(module.CHECKPOINT_FILE.read_text())
    assert checkpoint["watermark_trade_ts"] == 1741910400
    assert checkpoint["watermark_market_ts"] == 1741910400
    assert checkpoint["total_trade_rows_written"] == 2
    assert checkpoint["total_market_rows_written"] == 1

    trade_files = list(module.FORWARD_TRADES_DIR.rglob("*.parquet"))
    market_files = list(module.FORWARD_MARKETS_DIR.rglob("*.parquet"))
    assert len(trade_files) == 1
    assert len(market_files) == 1

    con = duckdb.connect()
    try:
        trade_count = con.execute(f"SELECT COUNT(*) FROM '{trade_files[0]}'").fetchone()[0]
        trade_distinct = con.execute(f"SELECT COUNT(DISTINCT trade_id) FROM '{trade_files[0]}'").fetchone()[0]
        market_count = con.execute(f"SELECT COUNT(*) FROM '{market_files[0]}'").fetchone()[0]
    finally:
        con.close()

    assert trade_count == 2
    assert trade_distinct == 2
    assert market_count == 1


def test_second_run_is_idempotent_with_existing_incremental_files(tmp_path, monkeypatch):
    module = load_update_forward_module()
    patch_paths(module, tmp_path)

    monkeypatch.setattr(module, "get_upper_bound_ts", lambda client, historical_only: (1741910400, {"mode": "test"}))

    rows = [sample_trade("stable-id", "2025-03-13T23:00:00Z")]
    monkeypatch.setattr(module, "fetch_trade_deltas", lambda **kwargs: rows)
    monkeypatch.setattr(module, "fetch_market_deltas", lambda **kwargs: [])

    # First run writes one trade.
    module.run(base_args(dry_run=False, lookback_seconds=0))

    # Second run sees existing incremental trade_id and writes nothing new.
    module.run(base_args(dry_run=False, lookback_seconds=0))

    trade_files = list(module.FORWARD_TRADES_DIR.rglob("*.parquet"))
    assert len(trade_files) == 1

    con = duckdb.connect()
    try:
        total_count = con.execute(
            f"SELECT COUNT(*) FROM '{module.FORWARD_TRADES_DIR}/**/*.parquet'"
        ).fetchone()[0]
        distinct_count = con.execute(
            f"SELECT COUNT(DISTINCT trade_id) FROM '{module.FORWARD_TRADES_DIR}/**/*.parquet'"
        ).fetchone()[0]
    finally:
        con.close()

    assert total_count == 1
    assert distinct_count == 1


def test_noop_when_upper_bound_not_ahead(tmp_path, monkeypatch):
    module = load_update_forward_module()
    patch_paths(module, tmp_path)

    # Pre-create checkpoint at high watermark.
    module.CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)
    module.atomic_write_json(
        module.CHECKPOINT_FILE,
        {
            "checkpoint_version": 1,
            "watermark_trade_ts": 2000,
            "watermark_market_ts": 2000,
            "last_successful_run_id": "",
            "last_successful_run_started_at": "",
            "last_successful_run_completed_at": "",
            "total_trade_rows_written": 0,
            "total_market_rows_written": 0,
            "created_at": "2026-03-14T00:00:00Z",
            "updated_at": "2026-03-14T00:00:00Z",
        },
    )

    monkeypatch.setattr(module, "get_upper_bound_ts", lambda client, historical_only: (1000, {"mode": "test"}))

    called = {"trade": False, "market": False}

    def _trade(**kwargs):
        called["trade"] = True
        return []

    def _market(**kwargs):
        called["market"] = True
        return []

    monkeypatch.setattr(module, "fetch_trade_deltas", _trade)
    monkeypatch.setattr(module, "fetch_market_deltas", _market)

    module.run(base_args(dry_run=False, lookback_seconds=0))

    # No fetch should happen because there is no new processing window.
    assert called["trade"] is False
    assert called["market"] is False
    assert not list(module.FORWARD_TRADES_DIR.rglob("*.parquet"))
    assert not list(module.FORWARD_MARKETS_DIR.rglob("*.parquet"))
