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
  uv run python scripts/update_forward.py --markets-only --chunk-days 7
  uv run python scripts/update_forward.py --lookback-seconds 43200
  uv run python scripts/update_forward.py --chunk-days 30   # process in 30-day chunks, write + checkpoint each chunk (resumable)
  uv run python scripts/update_forward.py --chunk-days 7 --progress-verbose --resource-log-seconds 5
  uv run python scripts/update_forward.py --chunk-days 3 --market-slice-hours 12   # explicit 12h slices (default is 12)
  uv run python scripts/update_forward.py --market-slice-hours 24   # wider slices if you prefer fewer cursors
  uv run python scripts/update_forward.py --market-slice-hours 0   # one long /markets pagination (legacy)
"""

from __future__ import annotations

import sys
from pathlib import Path

# Ensure project root on path when run as script
_SCRIPT_ROOT = Path(__file__).resolve().parents[1]
if str(_SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_ROOT))

import argparse
import gc
import json
import logging
import os
import signal
import subprocess
import threading
import time
import uuid
import resource
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable, Optional

import duckdb
import httpx
import pyarrow as pa
import pyarrow.parquet as pq

# ──────────────────────────────────────────────────────────────────────────────
# Config (paths from shared module; single place under data/kalshi/historical)
# ──────────────────────────────────────────────────────────────────────────────
from src.kalshi_forward.paths import (
    BASE_URL,
    CHECKPOINT_FILE,
    FORWARD_MARKETS_DIR,
    FORWARD_MARKETS_GLOB,
    FORWARD_TRADES_DIR,
    FORWARD_TRADES_GLOB,
    HISTORICAL_CHECKPOINT_FILE,
    HISTORICAL_CUTOFF_PATH,
    HISTORICAL_MARKETS_FILE,
    HISTORICAL_MARKETS_PATH,
    HISTORICAL_TRADES_GLOB,
    LIVE_MARKETS_PATH,
    LEGACY_FORWARD_MARKETS_GLOB,
    LEGACY_FORWARD_TRADES_GLOB,
    LEGACY_MARKETS_GLOB,
    LEGACY_TRADES_GLOB,
    LOCK_FILE,
    MARKET_TRADES_PATH,
    RUN_MANIFEST_DIR,
    STATE_DIR,
)

TRADE_PAGE_LIMIT = 1000
MARKET_PAGE_LIMIT = 1000
DEFAULT_LOOKBACK_SECONDS = 24 * 60 * 60
MARKET_LOOKBACK_DAYS = 7  # Widen market fetch backwards so trades have matching markets (reduce orphans)
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


def _fmt_metrics(metrics: dict[str, Any]) -> str:
    parts: list[str] = []
    for key, value in metrics.items():
        if value is None:
            continue
        if isinstance(value, int):
            parts.append(f"{key}={value:,}")
        elif isinstance(value, float):
            parts.append(f"{key}={value:.2f}")
        else:
            parts.append(f"{key}={value}")
    return " | ".join(parts)


def log_phase_start(enabled: bool, phase: str, **metrics: Any):
    if not enabled:
        return
    suffix = _fmt_metrics(metrics)
    if suffix:
        log.info("[phase] %s start | %s", phase, suffix)
    else:
        log.info("[phase] %s start", phase)


def log_phase_end(enabled: bool, phase: str, started_at: float, **metrics: Any):
    if not enabled:
        return
    elapsed_s = max(time.time() - started_at, 1e-6)
    metric_data: dict[str, Any] = {"elapsed_s": elapsed_s}
    metric_data.update(metrics)
    suffix = _fmt_metrics(metric_data)
    log.info("[phase] %s done  | %s", phase, suffix)


def _read_process_usage(pid: int) -> tuple[Optional[float], Optional[float], Optional[float]]:
    """Return rss_mb, cpu_pct, cpu_time_s for this process (cpu_pct from ps when available)."""
    try:
        usage = resource.getrusage(resource.RUSAGE_SELF)
        cpu_time_s = usage.ru_utime + usage.ru_stime
        # macOS: ru_maxrss is bytes; Linux: ru_maxrss is kilobytes
        if sys.platform == "darwin":
            rss_mb = float(usage.ru_maxrss) / (1024.0 * 1024.0)
        else:
            rss_mb = float(usage.ru_maxrss) / 1024.0
    except Exception:
        rss_mb, cpu_time_s = None, None

    cpu_pct: Optional[float] = None
    try:
        out = subprocess.check_output(
            ["ps", "-o", "rss=,%cpu=", "-p", str(pid)],
            text=True,
            stderr=subprocess.DEVNULL,
            timeout=5,
        ).strip()
        if out:
            if "," in out:
                rss_kb_s, cpu_pct_s = out.split(",", 1)
            else:
                parts = out.split()
                if len(parts) >= 2:
                    rss_kb_s, cpu_pct_s = parts[0], parts[1]
                else:
                    rss_kb_s, cpu_pct_s = "", ""
            if rss_kb_s.strip():
                rss_mb = float(rss_kb_s.strip()) / 1024.0
            if cpu_pct_s.strip():
                cpu_pct = float(cpu_pct_s.strip())
    except Exception:
        pass

    if rss_mb is None and cpu_time_s is None:
        return None, None, None
    return rss_mb, cpu_pct, cpu_time_s


def start_resource_logger(interval_seconds: float) -> tuple[Optional[threading.Thread], Optional[threading.Event]]:
    if interval_seconds <= 0:
        return None, None

    stop_event = threading.Event()
    pid = os.getpid()

    def _worker():
        while not stop_event.wait(interval_seconds):
            rss_mb, cpu_pct, cpu_time_s = _read_process_usage(pid)
            if rss_mb is None and cpu_time_s is None:
                log.info("[resource] pid=%s usage unavailable", pid)
            elif cpu_pct is None:
                log.info("[resource] pid=%s maxrss_mb=%.1f cpu_time_s=%.1f", pid, rss_mb or 0.0, cpu_time_s or 0.0)
            else:
                log.info("[resource] pid=%s rss_mb=%.1f cpu_pct=%.1f cpu_time_s=%.1f", pid, rss_mb, cpu_pct, cpu_time_s or 0.0)

    thread = threading.Thread(target=_worker, name="resource-logger", daemon=True)
    thread.start()
    return thread, stop_event


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


def _parse_count(t: dict) -> int:
    """Use count_fp as the source of truth for contract size (API populates it for 100% of trades; when count is also set they match). Fall back to count if count_fp is missing."""
    cfp = t.get("count_fp") or ""
    if cfp:
        try:
            return int(float(cfp))
        except (TypeError, ValueError):
            pass
    return int(t.get("count", 0) or 0)


def trade_row_from_api(t: dict, run_id: str, ingested_at: str) -> dict:
    return {
        "trade_id": t.get("trade_id", ""),
        "ticker": t.get("ticker", ""),
        "taker_side": t.get("taker_side", ""),
        "count": _parse_count(t),
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
    if FORWARD_TRADES_DIR.exists() and any(FORWARD_TRADES_DIR.rglob("*.parquet")):
        patterns.append(str(FORWARD_TRADES_GLOB))
    if safe_glob_exists(LEGACY_FORWARD_TRADES_GLOB):
        patterns.append(str(LEGACY_FORWARD_TRADES_GLOB))
    return patterns


def _existing_market_key_sources() -> list[str]:
    patterns: list[str] = []
    if HISTORICAL_MARKETS_FILE.exists():
        patterns.append(str(HISTORICAL_MARKETS_FILE))
    if safe_glob_exists(LEGACY_MARKETS_GLOB):
        patterns.append(str(LEGACY_MARKETS_GLOB))
    if FORWARD_MARKETS_DIR.exists() and any(FORWARD_MARKETS_DIR.rglob("*.parquet")):
        patterns.append(str(FORWARD_MARKETS_GLOB))
    if safe_glob_exists(LEGACY_FORWARD_MARKETS_GLOB):
        patterns.append(str(LEGACY_FORWARD_MARKETS_GLOB))
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
    last_tick_time = started
    last_tick_rows = 0

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
            now = time.time()
            elapsed_total = max(now - started, 1e-6)
            elapsed_seg = max(now - last_tick_time, 1e-6)
            delta_rows = len(rows) - last_tick_rows
            last_tick_time = now
            last_tick_rows = len(rows)
            log.info(
                "Trades fetch progress: pages=%s rows=%s avg_rate=%.1f rows/s recent_rate=%.1f rows/s",
                f"{page:,}",
                f"{len(rows):,}",
                len(rows) / elapsed_total,
                delta_rows / elapsed_seg,
            )

        cursor = data.get("cursor")
        if not cursor:
            break

        if max_pages is not None and page >= max_pages:
            log.warning("Reached --max-trade-pages limit; stopping early")
            break

    return rows


def _iter_market_api_close_ts_slices(
    min_ts_exclusive: int,
    max_ts_inclusive: int,
    slice_seconds: int,
) -> list[tuple[int, int]]:
    """Split API close_ts range into contiguous [api_min, api_max] slices (inclusive)."""
    market_lookback = MARKET_LOOKBACK_DAYS * 24 * 60 * 60
    lo = max(min_ts_exclusive + 1 - market_lookback, 0)
    hi = max_ts_inclusive
    if hi < lo:
        return [(lo, hi)]
    if slice_seconds <= 0 or (hi - lo) <= slice_seconds:
        return [(lo, hi)]
    out: list[tuple[int, int]] = []
    cur = lo
    while cur <= hi:
        seg_end = min(cur + slice_seconds, hi)
        out.append((cur, seg_end))
        cur = seg_end + 1
    return out


def _fetch_market_deltas_single_api_window(
    client: RobustClient,
    min_ts_exclusive: int,
    max_ts_inclusive: int,
    api_min_close_ts: int,
    api_max_close_ts: int,
    run_id: str,
    max_pages: Optional[int],
    market_path: str,
    *,
    slice_index: int,
    num_slices: int,
    page_global_start: int,
    scanned_global_start: int,
    started: float,
    last_tick_time: float,
    last_tick_scanned: int,
    seen_key_in_fetch: set[str],
    rows: list[dict],
    skip_stats: dict[str, int],
    periodic_gc_market_pages: int = 0,
) -> tuple[int, int, float, float, int]:
    """One cursor chain for [api_min_close_ts, api_max_close_ts]. Mutates rows/seen_key."""
    ingested_at = iso_now()
    cursor: Optional[str] = None
    page = 0
    scanned_markets = 0
    while True:
        if _shutdown_requested:
            raise RuntimeError("Shutdown requested during market fetch")

        params: dict[str, Any] = {
            "limit": MARKET_PAGE_LIMIT,
            "min_close_ts": api_min_close_ts,
            "max_close_ts": api_max_close_ts,
        }
        if cursor:
            params["cursor"] = cursor

        data = client.get(market_path, params=params)
        page += 1
        markets = data.get("markets", [])
        if not markets:
            break

        scanned_markets += len(markets)

        for market in markets:
            row = market_row_from_api(market, run_id=run_id, ingested_at=ingested_at)
            effective_ts = int(row.get("_effective_ts", 0) or 0)
            if effective_ts <= min_ts_exclusive:
                skip_stats["skip_eff_ts_low"] = skip_stats.get("skip_eff_ts_low", 0) + 1
                continue
            if effective_ts > max_ts_inclusive:
                skip_stats["skip_eff_ts_high"] = skip_stats.get("skip_eff_ts_high", 0) + 1
                continue
            key = market_key(row)
            if key in seen_key_in_fetch:
                skip_stats["skip_dup_key"] = skip_stats.get("skip_dup_key", 0) + 1
                continue
            seen_key_in_fetch.add(key)
            rows.append(row)

        global_page = page_global_start + page
        global_scanned = scanned_global_start + scanned_markets
        if periodic_gc_market_pages > 0 and global_page % periodic_gc_market_pages == 0:
            gc.collect()

        if global_page % 25 == 0:
            now = time.time()
            elapsed_total = max(now - started, 1e-6)
            elapsed_seg = max(now - last_tick_time, 1e-6)
            delta_scanned = global_scanned - last_tick_scanned
            last_tick_time = now
            last_tick_scanned = global_scanned
            sk_low = skip_stats.get("skip_eff_ts_low", 0)
            sk_hi = skip_stats.get("skip_eff_ts_high", 0)
            sk_dup = skip_stats.get("skip_dup_key", 0)
            log.info(
                "Markets fetch progress: slice=%s/%s pages=%s scanned=%s kept=%s "
                "(skipped eff_ts≤min=%s eff_ts>max=%s dup_key=%s) "
                "avg_rate=%.1f markets/s recent_rate=%.1f markets/s",
                slice_index,
                num_slices,
                f"{global_page:,}",
                f"{global_scanned:,}",
                f"{len(rows):,}",
                f"{sk_low:,}",
                f"{sk_hi:,}",
                f"{sk_dup:,}",
                global_scanned / elapsed_total,
                delta_scanned / elapsed_seg,
            )

        cursor = data.get("cursor")
        if not cursor:
            break

        if max_pages is not None and (page_global_start + page) >= max_pages:
            log.warning("Reached --max-market-pages limit; stopping early")
            break

    return page, scanned_markets, last_tick_time, last_tick_scanned, page_global_start + page


def fetch_market_deltas(
    client: RobustClient,
    min_ts_exclusive: int,
    max_ts_inclusive: int,
    run_id: str,
    max_pages: Optional[int],
    market_path: str,
    market_slice_hours: float = 0.0,
    *,
    periodic_gc_market_pages: int = 0,
) -> list[dict]:
    """Fetch markets; optional time-slicing on API close_ts shortens each cursor chain (chunk size unchanged)."""
    slice_seconds = int(max(0.0, float(market_slice_hours)) * 3600.0)
    slices = _iter_market_api_close_ts_slices(min_ts_exclusive, max_ts_inclusive, slice_seconds)
    num_slices = len(slices)
    if num_slices > 1:
        log.info(
            "Market fetch: %s API time slice(s) of ~%s h (fresh cursor each slice; --chunk-days unchanged)",
            num_slices,
            market_slice_hours,
        )

    rows: list[dict] = []
    seen_key_in_fetch: set[str] = set()
    skip_stats: dict[str, int] = {}
    started = time.time()
    last_tick_time = started
    last_tick_scanned = 0
    page_global_start = 0
    scanned_global_start = 0

    for si, (api_min_close_ts, api_max_close_ts) in enumerate(slices, start=1):
        if num_slices > 1:
            log.info(
                "Markets slice %s/%s API close_ts [%s, %s] (%s → %s)",
                si,
                num_slices,
                api_min_close_ts,
                api_max_close_ts,
                iso_from_epoch(api_min_close_ts),
                iso_from_epoch(api_max_close_ts),
            )
        pages_used, scanned_slice, last_tick_time, last_tick_scanned, page_global_start = _fetch_market_deltas_single_api_window(
            client,
            min_ts_exclusive,
            max_ts_inclusive,
            api_min_close_ts,
            api_max_close_ts,
            run_id,
            max_pages,
            market_path,
            slice_index=si,
            num_slices=num_slices,
            page_global_start=page_global_start,
            scanned_global_start=scanned_global_start,
            started=started,
            last_tick_time=last_tick_time,
            last_tick_scanned=last_tick_scanned,
            seen_key_in_fetch=seen_key_in_fetch,
            rows=rows,
            skip_stats=skip_stats,
            periodic_gc_market_pages=periodic_gc_market_pages,
        )
        scanned_global_start += scanned_slice
        if max_pages is not None and page_global_start >= max_pages:
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
    progress_verbose = bool(getattr(args, "progress_verbose", False))
    resource_log_seconds = float(getattr(args, "resource_log_seconds", 0) or 0)
    markets_only = bool(getattr(args, "markets_only", False))
    resource_thread: Optional[threading.Thread] = None
    resource_stop_event: Optional[threading.Event] = None

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
    if markets_only:
        log.info("[config] markets_only=true (trade watermark will not advance)")
    if progress_verbose:
        log.info("[config] progress_verbose=true")
    if resource_log_seconds > 0:
        log.info("[config] resource_log_seconds=%.1f", resource_log_seconds)
        log.info(
            "[resource] rss_mb/cpu_pct are live OS readings (not limits). "
            "High CPU usually means the client is busy; fluctuating RSS is normal. "
            "Optional: --periodic-gc-market-pages 100 during huge market scans; only restarting the process fully resets RSS."
        )
        resource_thread, resource_stop_event = start_resource_logger(resource_log_seconds)

    client = RobustClient()
    try:
        market_path = HISTORICAL_MARKETS_PATH if args.historical_only else LIVE_MARKETS_PATH
        log.info(f"Market endpoint:            {market_path}")
        _msh = float(getattr(args, "market_slice_hours", 12.0) or 0.0)
        log.info(
            "Market fetch slice width:  %s h (0 = one cursor for whole window; --chunk-days unchanged)",
            _msh,
        )
        base_upper_ts, upper_meta = get_upper_bound_ts(client, historical_only=args.historical_only)
        end_ts = getattr(args, "end_ts", None)
        upper_bound_ts = min(base_upper_ts, end_ts) if end_ts is not None else base_upper_ts
        if end_ts is not None:
            log.info(f"Upper bound capped by --end-ts: {end_ts} ({iso_from_epoch(end_ts)})")
        log.info(f"Upper bound mode:          {'historical cutoff' if args.historical_only else 'live now'}")
        log.info(f"Upper bound ts:            {upper_bound_ts} ({iso_from_epoch(upper_bound_ts)})")

        min_trade_ts = max(0, cp.watermark_trade_ts - args.lookback_seconds)
        min_market_ts = max(0, cp.watermark_market_ts - args.lookback_seconds)

        no_trade_window = upper_bound_ts <= min_trade_ts
        no_market_window = upper_bound_ts <= min_market_ts
        if (markets_only and no_market_window) or ((not markets_only) and no_trade_window and no_market_window):
            log.info("No new window to process; exiting.")
            return

        if not markets_only:
            log.info(f"Trade fetch window:        ({min_trade_ts}, {upper_bound_ts}]")
        log.info(f"Market fetch window:       ({min_market_ts}, {upper_bound_ts}]")

        # Existing keys for dedupe (window-bounded)
        existing_trade_ids = load_existing_trade_ids(min_trade_ts) if not markets_only else set()
        existing_market_keys = load_existing_market_keys(min_market_ts)
        if not markets_only:
            log.info(f"Existing trade IDs loaded: {len(existing_trade_ids):,}")
        log.info(f"Existing market keys:      {len(existing_market_keys):,}")

        chunk_days = getattr(args, "chunk_days", 0) or 0
        chunk_seconds = (chunk_days * 86400) if chunk_days > 0 else 0

        if chunk_seconds > 0:
            # Process in time chunks: fetch chunk -> write Parquet -> advance checkpoint (resumable)
            current_start = cp.watermark_market_ts if markets_only else cp.watermark_trade_ts
            chunk_index = 0
            total_trade_written = 0
            total_market_written = 0
            total_trade_raw = 0
            total_market_raw = 0
            total_skipped_trade_dupes = 0
            total_skipped_market_dupes = 0
            outputs: list[dict] = []

            while current_start < upper_bound_ts:
                chunk_end = min(current_start + chunk_seconds, upper_bound_ts)
                chunk_index += 1
                run_date_chunk = datetime.fromtimestamp(chunk_end, tz=timezone.utc).strftime("%Y-%m-%d")
                log.info(
                    "Chunk %s: window (%s, %s] -> %s",
                    chunk_index,
                    iso_from_epoch(current_start),
                    iso_from_epoch(chunk_end),
                    run_date_chunk,
                )

                chunk_min_market_ts = max(0, current_start - (MARKET_LOOKBACK_DAYS * 24 * 60 * 60))

                raw_trade_rows: list[dict] = []
                if not markets_only:
                    phase_started = time.time()
                    log_phase_start(
                        progress_verbose,
                        f"chunk{chunk_index}/fetch_trades",
                        min_ts=current_start,
                        max_ts=chunk_end,
                    )
                    raw_trade_rows = fetch_trade_deltas(
                        client=client,
                        min_ts_exclusive=current_start,
                        max_ts_inclusive=chunk_end,
                        run_id=run_id,
                        max_pages=args.max_trade_pages,
                    )
                    log_phase_end(
                        progress_verbose,
                        f"chunk{chunk_index}/fetch_trades",
                        phase_started,
                        rows_raw=len(raw_trade_rows),
                        rows_per_sec=(len(raw_trade_rows) / max(time.time() - phase_started, 1e-6)),
                    )

                phase_started = time.time()
                log_phase_start(
                    progress_verbose,
                    f"chunk{chunk_index}/fetch_markets",
                    min_ts=chunk_min_market_ts,
                    max_ts=chunk_end,
                )
                raw_market_rows = fetch_market_deltas(
                    client=client,
                    min_ts_exclusive=chunk_min_market_ts,
                    max_ts_inclusive=chunk_end,
                    run_id=run_id,
                    max_pages=args.max_market_pages,
                    market_path=market_path,
                    market_slice_hours=float(getattr(args, "market_slice_hours", 0.0) or 0.0),
                    periodic_gc_market_pages=int(getattr(args, "periodic_gc_market_pages", 0) or 0),
                )
                log_phase_end(
                    progress_verbose,
                    f"chunk{chunk_index}/fetch_markets",
                    phase_started,
                    rows_raw=len(raw_market_rows),
                    rows_per_sec=(len(raw_market_rows) / max(time.time() - phase_started, 1e-6)),
                )

                trade_rows_chunk: list[dict] = []
                skipped_trade_dupes_existing = 0
                skipped_trade_dupes_within = 0
                skipped_trade_dupes = 0
                if not markets_only:
                    trade_seen: set[str] = set()
                    phase_started = time.time()
                    log_phase_start(progress_verbose, f"chunk{chunk_index}/dedupe_trades", rows_raw=len(raw_trade_rows))
                    for row in raw_trade_rows:
                        trade_id = row.get("trade_id", "")
                        if not trade_id:
                            continue
                        if trade_id in trade_seen:
                            skipped_trade_dupes_within += 1
                            continue
                        if trade_id in existing_trade_ids:
                            skipped_trade_dupes_existing += 1
                            continue
                        trade_seen.add(trade_id)
                        existing_trade_ids.add(trade_id)
                        trade_rows_chunk.append(row)
                    skipped_trade_dupes = skipped_trade_dupes_existing + skipped_trade_dupes_within
                    log_phase_end(
                        progress_verbose,
                        f"chunk{chunk_index}/dedupe_trades",
                        phase_started,
                        rows_raw=len(raw_trade_rows),
                        rows_written=len(trade_rows_chunk),
                        dupes_existing=skipped_trade_dupes_existing,
                        dupes_within=skipped_trade_dupes_within,
                    )

                market_seen: set[str] = set()
                market_rows_chunk: list[dict] = []
                skipped_market_dupes_existing = 0
                skipped_market_dupes_within = 0
                phase_started = time.time()
                log_phase_start(progress_verbose, f"chunk{chunk_index}/dedupe_markets", rows_raw=len(raw_market_rows))
                for row in raw_market_rows:
                    key = market_key(row)
                    if key in market_seen:
                        skipped_market_dupes_within += 1
                        continue
                    if key in existing_market_keys:
                        skipped_market_dupes_existing += 1
                        continue
                    market_seen.add(key)
                    existing_market_keys.add(key)
                    market_rows_chunk.append(row)
                skipped_market_dupes = skipped_market_dupes_existing + skipped_market_dupes_within
                log_phase_end(
                    progress_verbose,
                    f"chunk{chunk_index}/dedupe_markets",
                    phase_started,
                    rows_raw=len(raw_market_rows),
                    rows_written=len(market_rows_chunk),
                    dupes_existing=skipped_market_dupes_existing,
                    dupes_within=skipped_market_dupes_within,
                )

                phase_started = time.time()
                log_phase_start(progress_verbose, f"chunk{chunk_index}/validate_rows")
                if not markets_only:
                    validate_trade_rows(trade_rows_chunk)
                validate_market_rows(market_rows_chunk)
                log_phase_end(
                    progress_verbose,
                    f"chunk{chunk_index}/validate_rows",
                    phase_started,
                    trade_rows=len(trade_rows_chunk),
                    market_rows=len(market_rows_chunk),
                )

                total_trade_raw += len(raw_trade_rows)
                total_market_raw += len(raw_market_rows)
                total_skipped_trade_dupes += skipped_trade_dupes
                total_skipped_market_dupes += skipped_market_dupes
                total_trade_written += len(trade_rows_chunk)
                total_market_written += len(market_rows_chunk)

                log.info(
                    "Chunk %s: trades raw=%s written=%s (skipped %s dupes); markets raw=%s written=%s (skipped %s dupes)",
                    chunk_index,
                    f"{len(raw_trade_rows):,}",
                    f"{len(trade_rows_chunk):,}",
                    skipped_trade_dupes,
                    f"{len(raw_market_rows):,}",
                    f"{len(market_rows_chunk):,}",
                    skipped_market_dupes,
                )

                written_trade_path_chunk: Optional[Path] = None
                written_market_path_chunk: Optional[Path] = None
                if not args.dry_run:
                    phase_started = time.time()
                    log_phase_start(progress_verbose, f"chunk{chunk_index}/write_parquet")
                    if trade_rows_chunk:
                        fname_t = f"trades_{run_id}_chunk{chunk_index}.parquet"
                        written_trade_path_chunk = FORWARD_TRADES_DIR / f"dt={run_date_chunk}" / fname_t
                        written_trade_path_chunk.parent.mkdir(parents=True, exist_ok=True)
                        atomic_write_parquet(written_trade_path_chunk, trade_rows_chunk, schema=TRADE_SCHEMA)
                    if market_rows_chunk:
                        fname_m = f"markets_{run_id}_chunk{chunk_index}.parquet"
                        written_market_path_chunk = FORWARD_MARKETS_DIR / f"dt={run_date_chunk}" / fname_m
                        written_market_path_chunk.parent.mkdir(parents=True, exist_ok=True)
                        atomic_write_parquet(written_market_path_chunk, market_rows_chunk, schema=MARKET_SCHEMA)
                    log_phase_end(
                        progress_verbose,
                        f"chunk{chunk_index}/write_parquet",
                        phase_started,
                        trade_rows=len(trade_rows_chunk),
                        market_rows=len(market_rows_chunk),
                    )

                    phase_started = time.time()
                    log_phase_start(progress_verbose, f"chunk{chunk_index}/checkpoint_save")
                    if not markets_only:
                        cp.watermark_trade_ts = max(cp.watermark_trade_ts, chunk_end)
                    cp.watermark_market_ts = max(cp.watermark_market_ts, chunk_end)
                    cp.last_successful_run_id = run_id
                    cp.last_successful_run_started_at = run_started_at
                    cp.last_successful_run_completed_at = iso_now()
                    cp.total_trade_rows_written += len(trade_rows_chunk)
                    cp.total_market_rows_written += len(market_rows_chunk)
                    cp.save()
                    log_phase_end(
                        progress_verbose,
                        f"chunk{chunk_index}/checkpoint_save",
                        phase_started,
                        checkpoint_trade_ts=cp.watermark_trade_ts,
                        checkpoint_market_ts=cp.watermark_market_ts,
                    )

                    outputs.append({
                        "chunk_index": chunk_index,
                        "window": {"min_exclusive": current_start, "max_inclusive": chunk_end},
                        "trades_file": str(written_trade_path_chunk) if written_trade_path_chunk else "",
                        "markets_file": str(written_market_path_chunk) if written_market_path_chunk else "",
                    })
                    log.info("Chunk %s complete; checkpoint advanced to %s.", chunk_index, iso_from_epoch(chunk_end))

                current_start = chunk_end

            if not args.dry_run:
                run_manifest = {
                    "run_id": run_id,
                    "started_at": run_started_at,
                    "completed_at": iso_now(),
                    "dry_run": args.dry_run,
                    "historical_only": args.historical_only,
                    "lookback_seconds": args.lookback_seconds,
                    "chunk_days": chunk_days,
                    "chunks": len(outputs),
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
                        "trade_raw": total_trade_raw,
                        "trade_written": total_trade_written,
                        "trade_skipped_dupes": total_skipped_trade_dupes,
                        "market_raw": total_market_raw,
                        "market_written": total_market_written,
                        "market_skipped_dupes": total_skipped_market_dupes,
                    },
                    "outputs": outputs,
                    "client_stats": {
                        "requests": client.request_count,
                        "errors": client.error_count,
                        "rate_limited": client.rate_limit_count,
                    },
                }
                RUN_MANIFEST_DIR.mkdir(parents=True, exist_ok=True)
                atomic_write_json(RUN_MANIFEST_DIR / f"{run_id}.json", run_manifest)

            if args.dry_run:
                log.info("Dry run complete (chunked). No files or checkpoint were modified.")
            else:
                log.info("Run complete (chunked). %s chunk(s); checkpoint advanced to %s.", chunk_index, iso_from_epoch(cp.watermark_trade_ts))
            return

        # Single-window path (chunk_days == 0)
        raw_trade_rows: list[dict] = []
        if not markets_only:
            phase_started = time.time()
            log_phase_start(progress_verbose, "single/fetch_trades", min_ts=min_trade_ts, max_ts=upper_bound_ts)
            raw_trade_rows = fetch_trade_deltas(
                client=client,
                min_ts_exclusive=min_trade_ts,
                max_ts_inclusive=upper_bound_ts,
                run_id=run_id,
                max_pages=args.max_trade_pages,
            )
            log_phase_end(
                progress_verbose,
                "single/fetch_trades",
                phase_started,
                rows_raw=len(raw_trade_rows),
                rows_per_sec=(len(raw_trade_rows) / max(time.time() - phase_started, 1e-6)),
            )
        phase_started = time.time()
        log_phase_start(progress_verbose, "single/fetch_markets", min_ts=min_market_ts, max_ts=upper_bound_ts)
        raw_market_rows = fetch_market_deltas(
            client=client,
            min_ts_exclusive=min_market_ts,
            max_ts_inclusive=upper_bound_ts,
            run_id=run_id,
            max_pages=args.max_market_pages,
            market_path=market_path,
            market_slice_hours=float(getattr(args, "market_slice_hours", 0.0) or 0.0),
            periodic_gc_market_pages=int(getattr(args, "periodic_gc_market_pages", 0) or 0),
        )
        log_phase_end(
            progress_verbose,
            "single/fetch_markets",
            phase_started,
            rows_raw=len(raw_market_rows),
            rows_per_sec=(len(raw_market_rows) / max(time.time() - phase_started, 1e-6)),
        )

        trade_rows: list[dict] = []
        skipped_trade_dupes_existing = 0
        skipped_trade_dupes_within = 0
        if not markets_only:
            trade_seen: set[str] = set()
            for row in raw_trade_rows:
                trade_id = row.get("trade_id", "")
                if not trade_id:
                    continue
                if trade_id in trade_seen:
                    skipped_trade_dupes_within += 1
                    continue
                if trade_id in existing_trade_ids:
                    skipped_trade_dupes_existing += 1
                    continue
                trade_seen.add(trade_id)
                trade_rows.append(row)
        skipped_trade_dupes = skipped_trade_dupes_existing + skipped_trade_dupes_within

        market_seen: set[str] = set()
        market_rows: list[dict] = []
        skipped_market_dupes_existing = 0
        skipped_market_dupes_within = 0
        for row in raw_market_rows:
            key = market_key(row)
            if key in market_seen:
                skipped_market_dupes_within += 1
                continue
            if key in existing_market_keys:
                skipped_market_dupes_existing += 1
                continue
            market_seen.add(key)
            market_rows.append(row)
        skipped_market_dupes = skipped_market_dupes_existing + skipped_market_dupes_within
        if progress_verbose:
            log.info(
                "[phase] single/dedupe summary | trade_dupes_existing=%s trade_dupes_within=%s market_dupes_existing=%s market_dupes_within=%s",
                f"{skipped_trade_dupes_existing:,}",
                f"{skipped_trade_dupes_within:,}",
                f"{skipped_market_dupes_existing:,}",
                f"{skipped_market_dupes_within:,}",
            )

        if not markets_only:
            validate_trade_rows(trade_rows)
        validate_market_rows(market_rows)

        if not markets_only:
            log.info(f"Fetched trades raw:        {len(raw_trade_rows):,}")
            log.info(f"Trades after dedupe:       {len(trade_rows):,} (skipped {skipped_trade_dupes:,})")
        log.info(f"Fetched markets raw:       {len(raw_market_rows):,}")
        log.info(f"Markets after dedupe:      {len(market_rows):,} (skipped {skipped_market_dupes:,})")

        run_date = datetime.fromtimestamp(upper_bound_ts, tz=timezone.utc).strftime("%Y-%m-%d")
        written_trade_path: Optional[Path] = None
        written_market_path: Optional[Path] = None

        if not args.dry_run:
            if trade_rows:
                (FORWARD_TRADES_DIR / f"dt={run_date}").mkdir(parents=True, exist_ok=True)
                written_trade_path = FORWARD_TRADES_DIR / f"dt={run_date}" / f"trades_{run_id}.parquet"
                atomic_write_parquet(written_trade_path, trade_rows, schema=TRADE_SCHEMA)

            if market_rows:
                (FORWARD_MARKETS_DIR / f"dt={run_date}").mkdir(parents=True, exist_ok=True)
                written_market_path = FORWARD_MARKETS_DIR / f"dt={run_date}" / f"markets_{run_id}.parquet"
                atomic_write_parquet(written_market_path, market_rows, schema=MARKET_SCHEMA)

            if not markets_only:
                cp.watermark_trade_ts = max(cp.watermark_trade_ts, upper_bound_ts)
            cp.watermark_market_ts = max(cp.watermark_market_ts, upper_bound_ts)
            cp.last_successful_run_id = run_id
            cp.last_successful_run_started_at = run_started_at
            cp.last_successful_run_completed_at = iso_now()
            cp.total_trade_rows_written += len(trade_rows)
            cp.total_market_rows_written += len(market_rows)
            cp.save()

        if not args.dry_run:
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
        if resource_stop_event is not None:
            resource_stop_event.set()
        if resource_thread is not None:
            resource_thread.join(timeout=2.0)
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
        "--markets-only",
        action="store_true",
        help="Fetch/write markets only (do not fetch trades, do not advance trade watermark). Useful for market backfill repairs.",
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
        "--end-ts",
        type=int,
        default=None,
        help="Cap upper bound to this UNIX timestamp (for backfilling a gap; e.g. first forward trade time)",
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
    parser.add_argument(
        "--chunk-days",
        type=int,
        default=0,
        metavar="N",
        help="Process window in N-day chunks: fetch chunk, write Parquet, advance checkpoint, repeat. Resumable if killed. Default 0 = one big run. Use 7 or 14 if you hit OOM (e.g. --chunk-days 7).",
    )
    parser.add_argument(
        "--progress-verbose",
        action="store_true",
        help="Log clear phase-by-phase progress with timings and duplicate breakdowns.",
    )
    parser.add_argument(
        "--resource-log-seconds",
        type=float,
        default=0.0,
        metavar="S",
        help="Log process RAM/CPU every S seconds (0 disables resource logging).",
    )
    parser.add_argument(
        "--periodic-gc-market-pages",
        type=int,
        default=0,
        metavar="N",
        help="Every N market API pages run gc.collect() (0=off). May slightly reduce RSS on long scans; can add pauses.",
    )
    parser.add_argument(
        "--market-slice-hours",
        type=float,
        default=12.0,
        metavar="H",
        help="Split each market fetch into API close_ts windows of H hours (new cursor each slice). "
        "0 = single long pagination (legacy). Does not change --chunk-days. Default: 12.",
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
