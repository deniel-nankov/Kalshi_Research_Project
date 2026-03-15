#!/usr/bin/env python3
"""
Incremental forward ingestion for Kalshi markets/trades.

What this script does on each run:
  1) Reads forward checkpoint watermark(s)
  2) Pulls only rows newer than watermark (with safety lookback)
  3) Appends delta rows to incremental Parquet files
  4) Advances checkpoint only after successful write/validation

Key guarantees:
  • Append-only incremental files (historical base is never rewritten)
  • Crash-safe checkpointing (atomic JSON writes)
  • Overlap-window + dedupe for idempotent re-runs
  • File lock to prevent concurrent writers

Examples:
  uv run python scripts/update_forward.py
  uv run python scripts/update_forward.py --dry-run
  uv run python scripts/update_forward.py --historical-only
  uv run python scripts/update_forward.py --lookback-seconds 43200
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

import duckdb
import httpx
import pyarrow as pa
import pyarrow.parquet as pq

# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────
BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
MARKETS_PATH = "/historical/markets"
MARKET_TRADES_PATH = "/markets/trades"
HISTORICAL_CUTOFF_PATH = "/historical/cutoff"

ROOT_DIR = Path("data/kalshi")
INCREMENTAL_DIR = ROOT_DIR / "incremental"
INCREMENTAL_TRADES_DIR = INCREMENTAL_DIR / "trades"
INCREMENTAL_MARKETS_DIR = INCREMENTAL_DIR / "markets"
STATE_DIR = ROOT_DIR / "state"
CHECKPOINT_FILE = STATE_DIR / "forward_checkpoint.json"
RUN_MANIFEST_DIR = STATE_DIR / "runs"
LOCK_FILE = STATE_DIR / "forward_ingestion.lock"

HISTORICAL_CHECKPOINT_FILE = ROOT_DIR / "historical" / ".checkpoint.json"
HISTORICAL_TRADES_GLOB = ROOT_DIR / "historical" / "trades" / "*.parquet"
HISTORICAL_MARKETS_FILE = ROOT_DIR / "historical" / "markets.parquet"
LEGACY_TRADES_GLOB = ROOT_DIR / "trades" / "*.parquet"
LEGACY_MARKETS_GLOB = ROOT_DIR / "markets" / "*.parquet"

TRADE_PAGE_LIMIT = 1000
MARKET_PAGE_LIMIT = 1000
DEFAULT_LOOKBACK_SECONDS = 24 * 60 * 60
MAX_RETRIES = 6
INITIAL_BACKOFF = 1.0
MAX_BACKOFF = 120.0
BASE_DELAY = 0.10
RATE_LIMIT_DELAY = 5.0
CHECKPOINT_VERSION = 1

# ──────────────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-5s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("forward-update")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

# ──────────────────────────────────────────────────────────────────────────────
# Graceful shutdown
# ──────────────────────────────────────────────────────────────────────────────
_shutdown_requested = False


def _signal_handler(signum, frame):
    del signum, frame
    global _shutdown_requested
    _shutdown_requested = True
    log.warning("Shutdown requested. Finishing current page safely...")


signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)


# ──────────────────────────────────────────────────────────────────────────────
# Utility helpers
# ──────────────────────────────────────────────────────────────────────────────
def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utc_now().isoformat().replace("+00:00", "Z")


def iso_from_epoch(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def parse_iso_to_epoch_seconds(value: str) -> int:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return int(parsed.timestamp())


def parse_api_ts_to_epoch_seconds(value: Optional[str]) -> int:
    if not value:
        return 0
    try:
        return parse_iso_to_epoch_seconds(value)
    except Exception:
        return 0


def atomic_write_json(path: Path, data: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=False))
    tmp.replace(path)


def atomic_write_parquet(path: Path, rows: list[dict], schema: Optional[pa.Schema] = None):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    table = pa.Table.from_pylist(rows, schema=schema) if schema else pa.Table.from_pylist(rows)
    pq.write_table(table, tmp, compression="zstd")
    tmp.replace(path)


def safe_glob_exists(pattern: Path) -> bool:
    return any(pattern.parent.glob(pattern.name))


# ──────────────────────────────────────────────────────────────────────────────
# File lock
# ──────────────────────────────────────────────────────────────────────────────
class FileLock:
    def __init__(self, lock_path: Path):
        self.lock_path = lock_path
        self.fd: Optional[int] = None

    def acquire(self):
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self.fd = os.open(str(self.lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(self.fd, str(os.getpid()).encode("utf-8"))
        except FileExistsError as exc:
            raise RuntimeError(f"Lock already exists: {self.lock_path}") from exc

    def release(self):
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None
        if self.lock_path.exists():
            self.lock_path.unlink()

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb):
        del exc_type, exc, tb
        self.release()


# ──────────────────────────────────────────────────────────────────────────────
# Checkpoint
# ──────────────────────────────────────────────────────────────────────────────
@dataclass
class ForwardCheckpoint:
    checkpoint_version: int = CHECKPOINT_VERSION
    watermark_trade_ts: int = 0
    watermark_market_ts: int = 0
    last_successful_run_id: str = ""
    last_successful_run_started_at: str = ""
    last_successful_run_completed_at: str = ""
    total_trade_rows_written: int = 0
    total_market_rows_written: int = 0
    created_at: str = field(default_factory=iso_now)
    updated_at: str = field(default_factory=iso_now)

    def save(self):
        self.updated_at = iso_now()
        atomic_write_json(CHECKPOINT_FILE, asdict(self))

    @classmethod
    def load_or_create(
        cls,
        bootstrap_from_historical: bool,
        explicit_start_ts: Optional[int],
        persist_if_created: bool = True,
    ) -> "ForwardCheckpoint":
        if CHECKPOINT_FILE.exists():
            raw = json.loads(CHECKPOINT_FILE.read_text())
            cp = cls()
            for key, value in raw.items():
                if hasattr(cp, key):
                    setattr(cp, key, value)
            return cp

        start_ts = 0
        if explicit_start_ts is not None:
            start_ts = explicit_start_ts
        elif bootstrap_from_historical:
            start_ts = _bootstrap_ts_from_historical_checkpoint()

        cp = cls(
            watermark_trade_ts=start_ts,
            watermark_market_ts=start_ts,
        )
        if persist_if_created:
            cp.save()
        return cp


def _bootstrap_ts_from_historical_checkpoint() -> int:
    if not HISTORICAL_CHECKPOINT_FILE.exists():
        return 0
    try:
        raw = json.loads(HISTORICAL_CHECKPOINT_FILE.read_text())
        cutoff_ts = raw.get("cutoff_ts", "")
        if cutoff_ts:
            return parse_iso_to_epoch_seconds(cutoff_ts)
    except Exception as exc:
        log.warning(f"Could not bootstrap from historical checkpoint: {exc}")
    return 0


# ──────────────────────────────────────────────────────────────────────────────
# HTTP Client
# ──────────────────────────────────────────────────────────────────────────────
class RobustClient:
    def __init__(self):
        self.client = httpx.Client(base_url=BASE_URL, timeout=60.0)
        self._delay = BASE_DELAY
        self.request_count = 0
        self.error_count = 0
        self.rate_limit_count = 0

    def close(self):
        self.client.close()

    def get(self, path: str, params: Optional[dict] = None) -> dict:
        backoff = INITIAL_BACKOFF
        for attempt in range(1, MAX_RETRIES + 1):
            if _shutdown_requested:
                raise RuntimeError("Shutdown requested")
            try:
                time.sleep(self._delay)
                response = self.client.get(path, params=params)
                self.request_count += 1

                if response.status_code == 429:
                    self.rate_limit_count += 1
                    self._delay = min(self._delay * 2, 2.0)
                    wait = RATE_LIMIT_DELAY * attempt
                    log.warning(f"429 rate-limited, sleeping {wait:.0f}s")
                    time.sleep(wait)
                    continue

                if response.status_code in (500, 502, 503, 504):
                    self.error_count += 1
                    wait = min(backoff, MAX_BACKOFF)
                    log.warning(f"Server {response.status_code}, retry in {wait:.0f}s")
                    time.sleep(wait)
                    backoff *= 2
                    continue

                response.raise_for_status()
                self._delay = max(BASE_DELAY, self._delay * 0.95)
                return response.json()
            except (httpx.ConnectError, httpx.TimeoutException, httpx.ReadTimeout, httpx.ReadError) as exc:
                self.error_count += 1
                wait = min(backoff, MAX_BACKOFF)
                log.warning(f"Network error {type(exc).__name__}, retry in {wait:.0f}s")
                time.sleep(wait)
                backoff *= 2
                continue

        raise RuntimeError(f"Failed after {MAX_RETRIES} attempts: GET {path} {params}")


# ──────────────────────────────────────────────────────────────────────────────
# Schemas
# ──────────────────────────────────────────────────────────────────────────────
TRADE_SCHEMA = pa.schema([
    ("trade_id", pa.string()),
    ("ticker", pa.string()),
    ("taker_side", pa.string()),
    ("count", pa.int64()),
    ("yes_price", pa.int32()),
    ("no_price", pa.int32()),
    ("price", pa.float64()),
    ("created_time", pa.string()),
    ("count_fp", pa.string()),
    ("yes_price_dollars", pa.string()),
    ("no_price_dollars", pa.string()),
    ("_run_id", pa.string()),
    ("_ingested_at", pa.string()),
    ("_source", pa.string()),
])

MARKET_SCHEMA = pa.schema([
    ("ticker", pa.string()),
    ("event_ticker", pa.string()),
    ("market_type", pa.string()),
    ("title", pa.string()),
    ("subtitle", pa.string()),
    ("yes_sub_title", pa.string()),
    ("no_sub_title", pa.string()),
    ("status", pa.string()),
    ("result", pa.string()),
    ("yes_bid", pa.int32()),
    ("yes_ask", pa.int32()),
    ("no_bid", pa.int32()),
    ("no_ask", pa.int32()),
    ("last_price", pa.int32()),
    ("previous_price", pa.int32()),
    ("previous_yes_bid", pa.int32()),
    ("previous_yes_ask", pa.int32()),
    ("volume", pa.int64()),
    ("volume_24h", pa.int64()),
    ("open_interest", pa.int64()),
    ("liquidity", pa.int64()),
    ("tick_size", pa.int32()),
    ("strike_type", pa.string()),
    ("floor_strike", pa.float64()),
    ("cap_strike", pa.float64()),
    ("can_close_early", pa.bool_()),
    ("rules_primary", pa.string()),
    ("rules_secondary", pa.string()),
    ("expiration_value", pa.string()),
    ("settlement_value", pa.string()),
    ("open_time", pa.string()),
    ("close_time", pa.string()),
    ("created_time", pa.string()),
    ("updated_time", pa.string()),
    ("expected_expiration_time", pa.string()),
    ("expiration_time", pa.string()),
    ("latest_expiration_time", pa.string()),
    ("settlement_ts", pa.string()),
    ("notional_value", pa.int32()),
    ("dollar_volume", pa.float64()),
    ("dollar_open_interest", pa.float64()),
    ("_effective_ts", pa.int64()),
    ("_run_id", pa.string()),
    ("_ingested_at", pa.string()),
    ("_source", pa.string()),
])


# ──────────────────────────────────────────────────────────────────────────────
# Row mappers
# ──────────────────────────────────────────────────────────────────────────────
def market_row_from_api(m: dict, run_id: str, ingested_at: str) -> dict:
    close_time = m.get("close_time", "")
    updated_time = m.get("updated_time", "")
    created_time = m.get("created_time", "")
    # Historical market endpoint windowing is based on close-time semantics.
    # Use close_time as primary effective timestamp (fallback to created_time),
    # not updated_time (which can be much later due metadata refreshes).
    effective_ts = max(
        parse_api_ts_to_epoch_seconds(close_time),
        parse_api_ts_to_epoch_seconds(created_time),
    )

    return {
        "ticker": m.get("ticker", ""),
        "event_ticker": m.get("event_ticker", ""),
        "market_type": m.get("market_type", ""),
        "title": m.get("title", ""),
        "subtitle": m.get("subtitle", ""),
        "yes_sub_title": m.get("yes_sub_title", ""),
        "no_sub_title": m.get("no_sub_title", ""),
        "status": m.get("status", ""),
        "result": m.get("result", ""),
        "yes_bid": m.get("yes_bid", 0) or 0,
        "yes_ask": m.get("yes_ask", 0) or 0,
        "no_bid": m.get("no_bid", 0) or 0,
        "no_ask": m.get("no_ask", 0) or 0,
        "last_price": m.get("last_price", 0) or 0,
        "previous_price": m.get("previous_price", 0) or 0,
        "previous_yes_bid": m.get("previous_yes_bid", 0) or 0,
        "previous_yes_ask": m.get("previous_yes_ask", 0) or 0,
        "volume": m.get("volume", 0) or 0,
        "volume_24h": m.get("volume_24h", 0) or 0,
        "open_interest": m.get("open_interest", 0) or 0,
        "liquidity": m.get("liquidity", 0) or 0,
        "tick_size": m.get("tick_size", 1) or 1,
        "strike_type": m.get("strike_type", ""),
        "floor_strike": float(m.get("floor_strike") or 0),
        "cap_strike": float(m.get("cap_strike") or 0),
        "can_close_early": m.get("can_close_early", False),
        "rules_primary": m.get("rules_primary", ""),
        "rules_secondary": m.get("rules_secondary", ""),
        "expiration_value": str(m.get("expiration_value", "")),
        "settlement_value": str(m.get("settlement_value", "")),
        "open_time": m.get("open_time", ""),
        "close_time": close_time,
        "created_time": created_time,
        "updated_time": updated_time,
        "expected_expiration_time": m.get("expected_expiration_time", ""),
        "expiration_time": m.get("expiration_time", ""),
        "latest_expiration_time": m.get("latest_expiration_time", ""),
        "settlement_ts": m.get("settlement_ts", ""),
        "notional_value": m.get("notional_value", 0) or 0,
        "dollar_volume": float(m.get("dollar_volume", 0) or 0),
        "dollar_open_interest": float(m.get("dollar_open_interest", 0) or 0),
        "_effective_ts": effective_ts,
        "_run_id": run_id,
        "_ingested_at": ingested_at,
        "_source": "forward_markets",
    }


def trade_row_from_api(t: dict, run_id: str, ingested_at: str) -> dict:
    return {
        "trade_id": t.get("trade_id", ""),
        "ticker": t.get("ticker", ""),
        "taker_side": t.get("taker_side", ""),
        "count": int(t.get("count", 0) or 0),
        "yes_price": int(t.get("yes_price", 0) or 0),
        "no_price": int(t.get("no_price", 0) or 0),
        "price": float(t.get("price", 0) or 0),
        "created_time": t.get("created_time", ""),
        "count_fp": t.get("count_fp", ""),
        "yes_price_dollars": t.get("yes_price_dollars", ""),
        "no_price_dollars": t.get("no_price_dollars", ""),
        "_run_id": run_id,
        "_ingested_at": ingested_at,
        "_source": "forward_trades",
    }


# ──────────────────────────────────────────────────────────────────────────────
# Dedupe helpers
# ──────────────────────────────────────────────────────────────────────────────
def _existing_trade_id_sources() -> list[str]:
    patterns: list[str] = []
    if safe_glob_exists(HISTORICAL_TRADES_GLOB):
        patterns.append(str(HISTORICAL_TRADES_GLOB))
    if safe_glob_exists(LEGACY_TRADES_GLOB):
        patterns.append(str(LEGACY_TRADES_GLOB))
    if INCREMENTAL_TRADES_DIR.exists() and any(INCREMENTAL_TRADES_DIR.rglob("*.parquet")):
        patterns.append(str(INCREMENTAL_TRADES_DIR / "*" / "*.parquet"))
    return patterns


def _existing_market_key_sources() -> list[str]:
    patterns: list[str] = []
    if HISTORICAL_MARKETS_FILE.exists():
        patterns.append(str(HISTORICAL_MARKETS_FILE))
    if safe_glob_exists(LEGACY_MARKETS_GLOB):
        patterns.append(str(LEGACY_MARKETS_GLOB))
    if INCREMENTAL_MARKETS_DIR.exists() and any(INCREMENTAL_MARKETS_DIR.rglob("*.parquet")):
        patterns.append(str(INCREMENTAL_MARKETS_DIR / "*" / "*.parquet"))
    return patterns


def load_existing_trade_ids(window_start_ts: int) -> set[str]:
    sources = _existing_trade_id_sources()
    if not sources:
        return set()

    window_start_iso = iso_from_epoch(window_start_ts)
    existing: set[str] = set()
    con = duckdb.connect()
    try:
        for src in sources:
            query = f"""
                SELECT DISTINCT trade_id
                FROM '{src}'
                WHERE trade_id IS NOT NULL
                  AND trade_id <> ''
                  AND TRY_CAST(created_time AS TIMESTAMP) >= TRY_CAST(? AS TIMESTAMP)
            """
            rows = con.execute(query, [window_start_iso]).fetchall()
            existing.update(row[0] for row in rows if row and row[0])
    finally:
        con.close()

    return existing


def market_key(row: dict) -> str:
    ticker = row.get("ticker", "")
    close = row.get("close_time", "")
    created = row.get("created_time", "")
    effective = close or created
    return f"{ticker}|{effective}"


def load_existing_market_keys(window_start_ts: int) -> set[str]:
    sources = _existing_market_key_sources()
    if not sources:
        return set()

    window_start_iso = iso_from_epoch(window_start_ts)
    existing: set[str] = set()
    con = duckdb.connect()
    try:
        for src in sources:
            query = f"""
                SELECT DISTINCT
                    ticker,
                                        COALESCE(close_time, created_time, '') AS key_ts
                FROM '{src}'
                WHERE ticker IS NOT NULL
                  AND ticker <> ''
                  AND COALESCE(
                                                TRY_CAST(close_time AS TIMESTAMP),
                        TRY_CAST(created_time AS TIMESTAMP)
                      ) >= TRY_CAST(? AS TIMESTAMP)
            """
            rows = con.execute(query, [window_start_iso]).fetchall()
            for ticker, key_ts in rows:
                existing.add(f"{ticker}|{key_ts or ''}")
    finally:
        con.close()

    return existing


# ──────────────────────────────────────────────────────────────────────────────
# Fetchers
# ──────────────────────────────────────────────────────────────────────────────
def get_upper_bound_ts(client: RobustClient, historical_only: bool) -> tuple[int, dict]:
    if historical_only:
        cutoff_data = client.get(HISTORICAL_CUTOFF_PATH)
        cutoff_ts = parse_iso_to_epoch_seconds(cutoff_data["trades_created_ts"])
        return cutoff_ts, cutoff_data

    now_ts = int(utc_now().timestamp())
    return now_ts, {"mode": "live_now", "ts": iso_from_epoch(now_ts)}


def fetch_trade_deltas(
    client: RobustClient,
    min_ts_exclusive: int,
    max_ts_inclusive: int,
    run_id: str,
    max_pages: Optional[int],
) -> list[dict]:
    ingested_at = iso_now()
    rows: list[dict] = []
    cursor: Optional[str] = None
    page = 0
    started = time.time()

    while True:
        if _shutdown_requested:
            raise RuntimeError("Shutdown requested during trade fetch")

        params: dict[str, Any] = {
            "limit": TRADE_PAGE_LIMIT,
            "min_ts": min_ts_exclusive,
            "max_ts": max_ts_inclusive,
        }
        if cursor:
            params["cursor"] = cursor

        data = client.get(MARKET_TRADES_PATH, params=params)
        page += 1

        trade_rows = data.get("trades", [])
        if not trade_rows:
            break

        rows.extend(trade_row_from_api(t, run_id=run_id, ingested_at=ingested_at) for t in trade_rows)

        if page % 25 == 0:
            elapsed = max(time.time() - started, 1e-6)
            log.info(
                "Trades fetch progress: pages=%s rows=%s rate=%.1f rows/s",
                f"{page:,}",
                f"{len(rows):,}",
                len(rows) / elapsed,
            )

        cursor = data.get("cursor")
        if not cursor:
            break

        if max_pages is not None and page >= max_pages:
            log.warning("Reached --max-trade-pages limit; stopping early")
            break

    return rows


def fetch_market_deltas(
    client: RobustClient,
    min_ts_exclusive: int,
    max_ts_inclusive: int,
    run_id: str,
    max_pages: Optional[int],
) -> list[dict]:
    ingested_at = iso_now()
    rows: list[dict] = []
    cursor: Optional[str] = None

    page = 0
    scanned_markets = 0
    started = time.time()
    while True:
        if _shutdown_requested:
            raise RuntimeError("Shutdown requested during market fetch")

        params: dict[str, Any] = {"limit": MARKET_PAGE_LIMIT}
        if cursor:
            params["cursor"] = cursor
        # Server-side windowing greatly reduces scan volume for historical catch-up.
        # /historical/markets supports close-time filters.
        params["min_close_ts"] = max(min_ts_exclusive + 1, 0)
        params["max_close_ts"] = max_ts_inclusive

        data = client.get(MARKETS_PATH, params=params)
        page += 1
        markets = data.get("markets", [])
        if not markets:
            break

        scanned_markets += len(markets)

        for market in markets:
            row = market_row_from_api(market, run_id=run_id, ingested_at=ingested_at)
            effective_ts = int(row.get("_effective_ts", 0) or 0)
            if effective_ts <= min_ts_exclusive:
                continue
            if effective_ts > max_ts_inclusive:
                continue
            rows.append(row)

        if page % 25 == 0:
            elapsed = max(time.time() - started, 1e-6)
            log.info(
                "Markets fetch progress: pages=%s scanned=%s matched=%s rate=%.1f markets/s",
                f"{page:,}",
                f"{scanned_markets:,}",
                f"{len(rows):,}",
                scanned_markets / elapsed,
            )

        cursor = data.get("cursor")
        if not cursor:
            break

        if max_pages is not None and page >= max_pages:
            log.warning("Reached --max-market-pages limit; stopping early")
            break

    return rows


# ──────────────────────────────────────────────────────────────────────────────
# Validation
# ──────────────────────────────────────────────────────────────────────────────
def validate_trade_rows(rows: Iterable[dict]):
    for row in rows:
        if not row.get("trade_id"):
            raise ValueError("Trade row missing trade_id")
        if not row.get("ticker"):
            raise ValueError("Trade row missing ticker")


def validate_market_rows(rows: Iterable[dict]):
    for row in rows:
        if not row.get("ticker"):
            raise ValueError("Market row missing ticker")
        if int(row.get("_effective_ts", 0) or 0) <= 0:
            raise ValueError("Market row missing valid _effective_ts")


# ──────────────────────────────────────────────────────────────────────────────
# Main run
# ──────────────────────────────────────────────────────────────────────────────
def run(args: argparse.Namespace):
    run_id = utc_now().strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:8]
    run_started_at = iso_now()

    cp = ForwardCheckpoint.load_or_create(
        bootstrap_from_historical=args.bootstrap_from_historical,
        explicit_start_ts=args.start_ts,
        persist_if_created=not args.dry_run,
    )

    log.info("=" * 72)
    log.info("FORWARD INCREMENTAL INGESTION")
    log.info("=" * 72)
    log.info(f"Run ID:                    {run_id}")
    log.info(f"Checkpoint file:           {CHECKPOINT_FILE}")
    log.info(f"Current trade watermark:   {cp.watermark_trade_ts} ({iso_from_epoch(cp.watermark_trade_ts)})")
    log.info(f"Current market watermark:  {cp.watermark_market_ts} ({iso_from_epoch(cp.watermark_market_ts)})")

    client = RobustClient()
    try:
        upper_bound_ts, upper_meta = get_upper_bound_ts(client, historical_only=args.historical_only)
        log.info(f"Upper bound mode:          {'historical cutoff' if args.historical_only else 'live now'}")
        log.info(f"Upper bound ts:            {upper_bound_ts} ({iso_from_epoch(upper_bound_ts)})")

        min_trade_ts = max(0, cp.watermark_trade_ts - args.lookback_seconds)
        min_market_ts = max(0, cp.watermark_market_ts - args.lookback_seconds)

        if upper_bound_ts <= min_trade_ts and upper_bound_ts <= min_market_ts:
            log.info("No new window to process; exiting.")
            return

        log.info(f"Trade fetch window:        ({min_trade_ts}, {upper_bound_ts}]")
        log.info(f"Market fetch window:       ({min_market_ts}, {upper_bound_ts}]")

        # Existing keys for dedupe (window-bounded)
        existing_trade_ids = load_existing_trade_ids(min_trade_ts)
        existing_market_keys = load_existing_market_keys(min_market_ts)
        log.info(f"Existing trade IDs loaded: {len(existing_trade_ids):,}")
        log.info(f"Existing market keys:      {len(existing_market_keys):,}")

        # Fetch raw deltas
        raw_trade_rows = fetch_trade_deltas(
            client=client,
            min_ts_exclusive=min_trade_ts,
            max_ts_inclusive=upper_bound_ts,
            run_id=run_id,
            max_pages=args.max_trade_pages,
        )
        raw_market_rows = fetch_market_deltas(
            client=client,
            min_ts_exclusive=min_market_ts,
            max_ts_inclusive=upper_bound_ts,
            run_id=run_id,
            max_pages=args.max_market_pages,
        )

        # Dedupe trades
        trade_seen: set[str] = set()
        trade_rows: list[dict] = []
        skipped_trade_dupes = 0
        for row in raw_trade_rows:
            trade_id = row.get("trade_id", "")
            if not trade_id:
                continue
            if trade_id in trade_seen or trade_id in existing_trade_ids:
                skipped_trade_dupes += 1
                continue
            trade_seen.add(trade_id)
            trade_rows.append(row)

        # Dedupe markets
        market_seen: set[str] = set()
        market_rows: list[dict] = []
        skipped_market_dupes = 0
        for row in raw_market_rows:
            key = market_key(row)
            if key in market_seen or key in existing_market_keys:
                skipped_market_dupes += 1
                continue
            market_seen.add(key)
            market_rows.append(row)

        validate_trade_rows(trade_rows)
        validate_market_rows(market_rows)

        log.info(f"Fetched trades raw:        {len(raw_trade_rows):,}")
        log.info(f"Trades after dedupe:       {len(trade_rows):,} (skipped {skipped_trade_dupes:,})")
        log.info(f"Fetched markets raw:       {len(raw_market_rows):,}")
        log.info(f"Markets after dedupe:      {len(market_rows):,} (skipped {skipped_market_dupes:,})")

        run_date = datetime.fromtimestamp(upper_bound_ts, tz=timezone.utc).strftime("%Y-%m-%d")
        written_trade_path: Optional[Path] = None
        written_market_path: Optional[Path] = None

        if not args.dry_run:
            if trade_rows:
                written_trade_path = INCREMENTAL_TRADES_DIR / f"dt={run_date}" / f"trades_{run_id}.parquet"
                atomic_write_parquet(written_trade_path, trade_rows, schema=TRADE_SCHEMA)

            if market_rows:
                written_market_path = INCREMENTAL_MARKETS_DIR / f"dt={run_date}" / f"markets_{run_id}.parquet"
                atomic_write_parquet(written_market_path, market_rows, schema=MARKET_SCHEMA)

            # Advance checkpoint only after successful writes
            cp.watermark_trade_ts = max(cp.watermark_trade_ts, upper_bound_ts)
            cp.watermark_market_ts = max(cp.watermark_market_ts, upper_bound_ts)
            cp.last_successful_run_id = run_id
            cp.last_successful_run_started_at = run_started_at
            cp.last_successful_run_completed_at = iso_now()
            cp.total_trade_rows_written += len(trade_rows)
            cp.total_market_rows_written += len(market_rows)
            cp.save()

        run_manifest = {
            "run_id": run_id,
            "started_at": run_started_at,
            "completed_at": iso_now(),
            "dry_run": args.dry_run,
            "historical_only": args.historical_only,
            "lookback_seconds": args.lookback_seconds,
            "upper_bound": {
                "ts": upper_bound_ts,
                "iso": iso_from_epoch(upper_bound_ts),
                "meta": upper_meta,
            },
            "windows": {
                "trade": {"min_exclusive": min_trade_ts, "max_inclusive": upper_bound_ts},
                "market": {"min_exclusive": min_market_ts, "max_inclusive": upper_bound_ts},
            },
            "counts": {
                "trade_raw": len(raw_trade_rows),
                "trade_written": len(trade_rows),
                "trade_skipped_dupes": skipped_trade_dupes,
                "market_raw": len(raw_market_rows),
                "market_written": len(market_rows),
                "market_skipped_dupes": skipped_market_dupes,
            },
            "outputs": {
                "trades_file": str(written_trade_path) if written_trade_path else "",
                "markets_file": str(written_market_path) if written_market_path else "",
            },
            "client_stats": {
                "requests": client.request_count,
                "errors": client.error_count,
                "rate_limited": client.rate_limit_count,
            },
        }
        RUN_MANIFEST_DIR.mkdir(parents=True, exist_ok=True)
        atomic_write_json(RUN_MANIFEST_DIR / f"{run_id}.json", run_manifest)

        if args.dry_run:
            log.info("Dry run complete. No files or checkpoint were modified.")
        else:
            log.info("Run complete and checkpoint advanced.")

    finally:
        client.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Incremental forward ingestion for Kalshi data")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and compute dedupe counts, but do not write files/checkpoint",
    )
    parser.add_argument(
        "--historical-only",
        action="store_true",
        help="Use /historical/cutoff as the run upper bound instead of current time",
    )
    parser.add_argument(
        "--lookback-seconds",
        type=int,
        default=DEFAULT_LOOKBACK_SECONDS,
        help=f"Safety overlap for idempotent replay (default: {DEFAULT_LOOKBACK_SECONDS})",
    )
    parser.add_argument(
        "--bootstrap-from-historical",
        action="store_true",
        default=True,
        help="Initialize checkpoint from data/kalshi/historical/.checkpoint.json cutoff_ts when missing",
    )
    parser.add_argument(
        "--start-ts",
        type=int,
        default=None,
        help="Explicit bootstrap UNIX timestamp (used only when checkpoint is missing)",
    )
    parser.add_argument(
        "--max-trade-pages",
        type=int,
        default=None,
        help="Optional safety cap for trade pagination during testing",
    )
    parser.add_argument(
        "--max-market-pages",
        type=int,
        default=None,
        help="Optional safety cap for market pagination during testing",
    )
    return parser


def main():
    args = build_parser().parse_args()

    try:
        with FileLock(LOCK_FILE):
            run(args)
    except Exception as exc:
        log.error(f"Run failed: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
