#!/usr/bin/env python3
"""
Download ALL Kalshi historical markets and trades.

Pipeline overview:
  Phase 1 — Download every historical market listing → data/kalshi/historical/markets.parquet
  Phase 2 — For each market, download all trades     → data/kalshi/historical/trades/*.parquet

Features:
  • Fully resumable — checkpoint file tracks progress; safe to Ctrl-C and restart
  • Adaptive rate limiting — backs off on 429 / 5xx, speeds up when clear
  • Batched Parquet writes — trades flushed to disk every N markets (not held in RAM)
  • Rich progress display — ETA, throughput, live counters
  • Idempotent — running twice produces the same result without duplicate work

Endpoints used:
  /historical/markets          — paginated market listings (100/page)
  /markets/trades?ticker=X     — paginated trade history per market (1000/page)

Usage:
  uv run python scripts/download_historical.py              # full download
  uv run python scripts/download_historical.py --test 100   # test with first 100 markets
  uv run python scripts/download_historical.py --resume     # resume interrupted download
"""
from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

# ──────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────
BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
HISTORICAL_MARKETS_PATH = "/historical/markets"
MARKET_TRADES_PATH = "/markets/trades"
CUTOFF_PATH = "/historical/cutoff"

MARKET_PAGE_LIMIT = 100      # max per page for /historical/markets
TRADE_PAGE_LIMIT = 1000      # max per page for /markets/trades

DATA_DIR = Path("data/kalshi/historical")
CHECKPOINT_FILE = DATA_DIR / ".checkpoint.json"
MARKETS_PARQUET = DATA_DIR / "markets.parquet"
TRADES_DIR = DATA_DIR / "trades"

TRADE_BATCH_SIZE = 500       # flush trades to disk every N markets
MAX_RETRIES = 6
INITIAL_BACKOFF = 1.0
MAX_BACKOFF = 120.0
BASE_DELAY = 0.10            # seconds between requests (≈10 req/s)
RATE_LIMIT_DELAY = 5.0       # extra delay after a 429

# ──────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-5s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("download")

# Suppress noisy httpx request logs
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

# ──────────────────────────────────────────────────────────────────────
# Graceful shutdown
# ──────────────────────────────────────────────────────────────────────
_shutdown_requested = False


def _signal_handler(signum, frame):
    global _shutdown_requested
    if _shutdown_requested:
        log.warning("Force quit.")
        sys.exit(1)
    _shutdown_requested = True
    log.warning("Shutdown requested — finishing current batch and saving checkpoint...")


signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)

# ──────────────────────────────────────────────────────────────────────
# Checkpoint — tracks pipeline progress for resumability
# ──────────────────────────────────────────────────────────────────────

@dataclass
class Checkpoint:
    """Tracks download progress. Persisted to disk as JSON."""
    # Phase 1
    markets_downloaded: bool = False
    markets_cursor: Optional[str] = None
    markets_page: int = 0
    total_markets: int = 0

    # Phase 2
    trades_completed_tickers: list[str] = field(default_factory=list)
    trades_batch_index: int = 0
    total_trades: int = 0
    total_trade_requests: int = 0

    # Metadata
    started_at: str = ""
    updated_at: str = ""
    cutoff_ts: str = ""

    def save(self):
        CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)
        self.updated_at = datetime.now().isoformat()
        CHECKPOINT_FILE.write_text(json.dumps(asdict(self), indent=2))

    @classmethod
    def load(cls) -> "Checkpoint":
        if CHECKPOINT_FILE.exists():
            data = json.loads(CHECKPOINT_FILE.read_text())
            # Handle list field
            cp = cls()
            for k, v in data.items():
                if hasattr(cp, k):
                    setattr(cp, k, v)
            return cp
        return cls(started_at=datetime.now().isoformat())


# ──────────────────────────────────────────────────────────────────────
# HTTP Client with retry + adaptive rate limiting
# ──────────────────────────────────────────────────────────────────────

class RobustClient:
    """httpx client with exponential-backoff retries and adaptive throttling."""

    def __init__(self):
        self.client = httpx.Client(base_url=BASE_URL, timeout=60.0)
        self._delay = BASE_DELAY
        self._request_count = 0
        self._error_count = 0
        self._rate_limit_count = 0

    def close(self):
        self.client.close()

    def get(self, path: str, params: Optional[dict] = None) -> dict:
        """GET with retries, backoff, and adaptive rate limiting."""
        backoff = INITIAL_BACKOFF

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                time.sleep(self._delay)
                response = self.client.get(path, params=params)
                self._request_count += 1

                if response.status_code == 429:
                    self._rate_limit_count += 1
                    self._delay = min(self._delay * 2, 2.0)  # slow down
                    wait = RATE_LIMIT_DELAY * attempt
                    log.warning(f"Rate limited (429). Waiting {wait:.0f}s. Delay now {self._delay:.2f}s")
                    time.sleep(wait)
                    continue

                if response.status_code in (500, 502, 503, 504):
                    self._error_count += 1
                    wait = min(backoff, MAX_BACKOFF)
                    log.warning(f"Server error {response.status_code}. Retry {attempt}/{MAX_RETRIES} in {wait:.0f}s")
                    time.sleep(wait)
                    backoff *= 2
                    continue

                response.raise_for_status()

                # Success — gently speed back up
                self._delay = max(BASE_DELAY, self._delay * 0.95)
                return response.json()

            except (httpx.ConnectError, httpx.TimeoutException, httpx.ReadTimeout, httpx.ReadError) as e:
                self._error_count += 1
                wait = min(backoff, MAX_BACKOFF)
                log.warning(f"Network error ({type(e).__name__}): {e}. Retry {attempt}/{MAX_RETRIES} in {wait:.0f}s")
                time.sleep(wait)
                backoff *= 2
                continue

        raise RuntimeError(f"Failed after {MAX_RETRIES} attempts: {path}")

    @property
    def stats(self) -> str:
        return f"requests={self._request_count:,}  errors={self._error_count}  429s={self._rate_limit_count}  delay={self._delay:.2f}s"


# ──────────────────────────────────────────────────────────────────────
# Arrow Schemas
# ──────────────────────────────────────────────────────────────────────

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
])

TRADE_SCHEMA = pa.schema([
    ("trade_id", pa.string()),
    ("ticker", pa.string()),
    ("taker_side", pa.string()),
    ("count", pa.int64()),
    ("yes_price", pa.int32()),
    ("no_price", pa.int32()),
    ("price", pa.float64()),
    ("created_time", pa.string()),
    # New-format fields (coexist with old)
    ("count_fp", pa.string()),
    ("yes_price_dollars", pa.string()),
    ("no_price_dollars", pa.string()),
])


# ──────────────────────────────────────────────────────────────────────
# Phase 1 — Download all historical markets
# ──────────────────────────────────────────────────────────────────────

def _market_row(m: dict) -> dict:
    """Extract a flat row from a raw market JSON object."""
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
        "close_time": m.get("close_time", ""),
        "created_time": m.get("created_time", ""),
        "updated_time": m.get("updated_time", ""),
        "expected_expiration_time": m.get("expected_expiration_time", ""),
        "expiration_time": m.get("expiration_time", ""),
        "latest_expiration_time": m.get("latest_expiration_time", ""),
        "settlement_ts": m.get("settlement_ts", ""),
        "notional_value": m.get("notional_value", 0) or 0,
        "dollar_volume": float(m.get("dollar_volume", 0) or 0),
        "dollar_open_interest": float(m.get("dollar_open_interest", 0) or 0),
    }


def download_markets(client: RobustClient, cp: Checkpoint, test_limit: int = 0) -> list[dict]:
    """Phase 1: Download all historical market listings.

    Returns list of raw market dicts (used in phase 2 for tickers).
    Writes markets.parquet incrementally and checkpoints cursor for resumability.
    """
    if cp.markets_downloaded and MARKETS_PARQUET.exists():
        log.info(f"Phase 1 already complete ({cp.total_markets:,} markets). Loading from disk...")
        table = pq.read_table(MARKETS_PARQUET)
        tickers = table.column("ticker").to_pylist()
        volumes = table.column("volume").to_pylist()
        return [{"ticker": t, "volume": v} for t, v in zip(tickers, volumes)]

    log.info("=" * 70)
    log.info("PHASE 1: Downloading all historical markets")
    log.info("=" * 70)

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    all_rows: list[dict] = []
    cursor = cp.markets_cursor
    page = cp.markets_page
    start_time = time.time()

    # If resuming, we need to re-fetch from the beginning up to the saved cursor
    # because we can't skip pages. Instead, we just restart and rebuild.
    if cursor:
        log.info(f"Resuming from page {page} (cursor exists)")

    while True:
        if _shutdown_requested:
            log.warning("Shutdown — saving market checkpoint...")
            cp.markets_cursor = cursor
            cp.markets_page = page
            cp.total_markets = len(all_rows)
            cp.save()
            _write_markets_parquet(all_rows)
            sys.exit(0)

        params: dict[str, Any] = {"limit": MARKET_PAGE_LIMIT}
        if cursor:
            params["cursor"] = cursor

        data = client.get(HISTORICAL_MARKETS_PATH, params=params)
        markets = data.get("markets", [])

        if not markets:
            break

        page += 1
        for m in markets:
            all_rows.append(_market_row(m))

        cursor = data.get("cursor")

        # Progress
        elapsed = time.time() - start_time
        rate = len(all_rows) / elapsed if elapsed > 0 else 0
        sys.stdout.write(
            f"\r   Page {page:,}  |  {len(all_rows):,} markets  |  "
            f"{rate:.0f} mkts/sec  |  {client.stats}"
        )
        sys.stdout.flush()

        # Checkpoint every 100 pages
        if page % 100 == 0:
            cp.markets_cursor = cursor
            cp.markets_page = page
            cp.total_markets = len(all_rows)
            cp.save()

        if not cursor:
            break

        if test_limit and len(all_rows) >= test_limit:
            all_rows = all_rows[:test_limit]  # trim to exact limit
            log.info(f"\n   Test limit reached ({test_limit} markets)")
            break

    elapsed = time.time() - start_time
    print()  # newline after \r
    log.info(f"Phase 1 complete: {len(all_rows):,} markets in {elapsed:.0f}s ({client.stats})")

    # Write final parquet
    _write_markets_parquet(all_rows)

    cp.markets_downloaded = True
    cp.markets_cursor = None
    cp.markets_page = page
    cp.total_markets = len(all_rows)
    cp.save()

    return [{"ticker": r["ticker"], "volume": r["volume"]} for r in all_rows]


def _write_markets_parquet(rows: list[dict]):
    """Write market rows to a single Parquet file."""
    if not rows:
        return
    MARKETS_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows, schema=MARKET_SCHEMA)
    pq.write_table(table, MARKETS_PARQUET, compression="zstd")
    log.info(f"   Wrote {MARKETS_PARQUET} ({len(rows):,} rows, {MARKETS_PARQUET.stat().st_size / 1024 / 1024:.1f} MB)")


# ──────────────────────────────────────────────────────────────────────
# Phase 2 — Download trades for every market
# ──────────────────────────────────────────────────────────────────────

def _trade_row(t: dict) -> dict:
    """Extract a flat row from a raw trade JSON object."""
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
    }


def _fetch_trades_for_ticker(client: RobustClient, ticker: str) -> list[dict]:
    """Fetch all trades for a single ticker, handling pagination."""
    all_trades: list[dict] = []
    cursor = None

    while True:
        params: dict[str, Any] = {"ticker": ticker, "limit": TRADE_PAGE_LIMIT}
        if cursor:
            params["cursor"] = cursor

        data = client.get(MARKET_TRADES_PATH, params=params)
        trades = data.get("trades", [])

        if not trades:
            break

        all_trades.extend(_trade_row(t) for t in trades)

        cursor = data.get("cursor")
        if not cursor:
            break

    return all_trades


def _flush_trades(trade_buffer: list[dict], batch_index: int) -> Path:
    """Write a batch of trade rows to a numbered Parquet file."""
    TRADES_DIR.mkdir(parents=True, exist_ok=True)
    path = TRADES_DIR / f"batch_{batch_index:04d}.parquet"
    table = pa.Table.from_pylist(trade_buffer, schema=TRADE_SCHEMA)
    pq.write_table(table, path, compression="zstd")
    size_mb = path.stat().st_size / 1024 / 1024
    log.info(f"   💾 Wrote {path.name}: {len(trade_buffer):,} trades ({size_mb:.1f} MB)")
    return path


def download_trades(client: RobustClient, cp: Checkpoint, market_info: list[dict]):
    """Phase 2: Download all trades for every historical market.

    Processes markets in order, batching trade rows to disk every TRADE_BATCH_SIZE
    markets. Tracks completed tickers in checkpoint for resumability.
    """
    log.info("=" * 70)
    log.info("PHASE 2: Downloading trades for all markets")
    log.info("=" * 70)

    completed_set = set(cp.trades_completed_tickers)
    remaining = [(m["ticker"], m["volume"]) for m in market_info if m["ticker"] not in completed_set]

    total = len(market_info)
    done = len(completed_set)
    log.info(f"   Total markets:     {total:,}")
    log.info(f"   Already completed: {done:,}")
    log.info(f"   Remaining:         {len(remaining):,}")

    if not remaining:
        log.info("Phase 2 already complete!")
        return

    trade_buffer: list[dict] = []
    batch_tickers: list[str] = []  # tickers in current buffer
    batch_index = cp.trades_batch_index
    markets_in_batch = 0

    start_time = time.time()
    phase_trades = 0
    phase_requests_start = client._request_count

    # Stats for progress display
    markets_with_trades = 0
    markets_without_trades = 0

    for i, (ticker, volume) in enumerate(remaining, 1):
        if _shutdown_requested:
            # Flush whatever we have
            if trade_buffer:
                _flush_trades(trade_buffer, batch_index)
                cp.trades_completed_tickers.extend(batch_tickers)
            cp.trades_batch_index = batch_index
            cp.total_trades += phase_trades
            cp.total_trade_requests += (client._request_count - phase_requests_start)
            cp.save()
            log.warning(f"Shutdown — checkpoint saved. {done + i - 1:,}/{total:,} markets done.")
            sys.exit(0)

        # Fetch trades
        trades = _fetch_trades_for_ticker(client, ticker)

        if trades:
            trade_buffer.extend(trades)
            phase_trades += len(trades)
            markets_with_trades += 1
        else:
            markets_without_trades += 1

        batch_tickers.append(ticker)
        markets_in_batch += 1

        # Progress display
        progress = done + i
        elapsed = time.time() - start_time
        rate = i / elapsed if elapsed > 0 else 0
        eta_seconds = (len(remaining) - i) / rate if rate > 0 else 0
        eta = str(timedelta(seconds=int(eta_seconds)))

        sys.stdout.write(
            f"\r   [{progress:,}/{total:,}] "
            f"{ticker[:35]:35s}  "
            f"trades={len(trades):>5d}  "
            f"buf={len(trade_buffer):>7,}  "
            f"{rate:.1f} mkts/s  "
            f"ETA {eta}  "
        )
        sys.stdout.flush()

        # Flush batch to disk
        if markets_in_batch >= TRADE_BATCH_SIZE:
            print()  # newline
            _flush_trades(trade_buffer, batch_index)
            cp.trades_completed_tickers.extend(batch_tickers)
            cp.trades_batch_index = batch_index + 1
            cp.total_trades += phase_trades
            cp.save()

            batch_index += 1
            trade_buffer = []
            batch_tickers = []
            markets_in_batch = 0
            phase_trades = 0

    # Flush remaining
    if trade_buffer:
        print()
        _flush_trades(trade_buffer, batch_index)
        cp.trades_completed_tickers.extend(batch_tickers)
        cp.trades_batch_index = batch_index + 1

    # Final checkpoint
    cp.total_trades += phase_trades
    cp.total_trade_requests += (client._request_count - phase_requests_start)
    cp.save()

    elapsed = time.time() - start_time
    print()
    log.info(f"Phase 2 complete in {timedelta(seconds=int(elapsed))}")
    log.info(f"   Markets with trades:    {markets_with_trades:,}")
    log.info(f"   Markets without trades: {markets_without_trades:,}")
    log.info(f"   Total trades saved:     {cp.total_trades:,}")


# ──────────────────────────────────────────────────────────────────────
# Phase 3 — Final summary & validation
# ──────────────────────────────────────────────────────────────────────

def summarize(cp: Checkpoint):
    """Print final summary of downloaded data."""
    log.info("=" * 70)
    log.info("DOWNLOAD COMPLETE — SUMMARY")
    log.info("=" * 70)

    # Markets file
    if MARKETS_PARQUET.exists():
        table = pq.read_table(MARKETS_PARQUET)
        size_mb = MARKETS_PARQUET.stat().st_size / 1024 / 1024
        log.info(f"   Markets:  {table.num_rows:,} rows  ({size_mb:.1f} MB)  → {MARKETS_PARQUET}")

    # Trade files
    trade_files = sorted(TRADES_DIR.glob("batch_*.parquet"))
    total_trade_rows = 0
    total_trade_bytes = 0
    for f in trade_files:
        t = pq.read_metadata(f)
        total_trade_rows += t.num_rows
        total_trade_bytes += f.stat().st_size

    trade_mb = total_trade_bytes / 1024 / 1024
    log.info(f"   Trades:   {total_trade_rows:,} rows across {len(trade_files)} files ({trade_mb:.1f} MB)  → {TRADES_DIR}/")
    log.info(f"   Total disk: {size_mb + trade_mb:.1f} MB")

    log.info(f"\n   Checkpoint: {CHECKPOINT_FILE}")
    log.info(f"   Started:    {cp.started_at}")
    log.info(f"   Finished:   {cp.updated_at}")
    log.info(f"   API stats:  {cp.total_trade_requests:,} trade requests")


# ──────────────────────────────────────────────────────────────────────
# CLI entry point
# ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Download all Kalshi historical markets and trades.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--test", type=int, default=0, metavar="N",
        help="Download only the first N markets (for testing)",
    )
    parser.add_argument(
        "--resume", action="store_true",
        help="Resume from the last checkpoint (default: auto-detect)",
    )
    parser.add_argument(
        "--clean", action="store_true",
        help="Delete existing data and start fresh",
    )
    args = parser.parse_args()

    # Clean start if requested
    if args.clean:
        log.info("Cleaning previous data...")
        if CHECKPOINT_FILE.exists():
            CHECKPOINT_FILE.unlink()
        if MARKETS_PARQUET.exists():
            MARKETS_PARQUET.unlink()
        for f in TRADES_DIR.glob("*.parquet"):
            f.unlink()
        log.info("   Done.")

    # Load or create checkpoint
    cp = Checkpoint.load()
    if not cp.started_at:
        cp.started_at = datetime.now().isoformat()

    # Print banner
    log.info("╔══════════════════════════════════════════════════════════════╗")
    log.info("║     KALSHI HISTORICAL DATA PIPELINE                        ║")
    log.info("╚══════════════════════════════════════════════════════════════╝")
    log.info(f"   Data directory:  {DATA_DIR}")
    log.info(f"   Checkpoint:      {CHECKPOINT_FILE}")
    if args.test:
        log.info(f"   ⚡ TEST MODE:    First {args.test} markets only")

    # Fetch cutoff info
    client = RobustClient()
    try:
        cutoff_data = client.get(CUTOFF_PATH)
        cp.cutoff_ts = cutoff_data.get("trades_created_ts", "")
        log.info(f"   Cutoff:          {cp.cutoff_ts}")
        cp.save()
    except Exception as e:
        log.warning(f"   Could not fetch cutoff: {e}")

    overall_start = time.time()

    try:
        # Phase 1: Markets
        market_info = download_markets(client, cp, test_limit=args.test)

        if _shutdown_requested:
            sys.exit(0)

        # Phase 2: Trades
        download_trades(client, cp, market_info)

        # Phase 3: Summary
        summarize(cp)

    finally:
        client.close()

    elapsed = time.time() - overall_start
    log.info(f"\n   Total pipeline time: {timedelta(seconds=int(elapsed))}")
    log.info("   🎉 All done!")


if __name__ == "__main__":
    main()
