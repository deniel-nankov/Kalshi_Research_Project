#!/usr/bin/env python3
# ruff: noqa: E402
"""
Capture Kalshi market_lifecycle_v2 and multivariate_market_lifecycle WS channels.

Why this script exists
----------------------
These channels emit events when a market or event:
  - is created
  - has metadata updated (yes_sub_title, floor_strike, etc.)
  - opens, suspends, or resolves
  - settles

For surveillance, the **exact moment of resolution** matters: any trade
clustered tight to the resolution event is a candidate for pre-resolution
informed flow. Polling the markets endpoint to detect resolution can lag
by minutes; the WS channel reports the moment it happens.

We persist every lifecycle event with run_id, msg_type, market_ticker (or
event_ticker), and the full message JSON for downstream replay.

Auth & connection
-----------------
- WS endpoint: wss://api.elections.kalshi.com/trade-api/ws/v2
- Auth: RSA-PSS signed headers (KALSHI_API_KEY_ID + KALSHI_API_PRIVATE_KEY_PEM)
  Sign the lower-case method+path with the private key, base64 the signature.
- Channels (public, no auth needed for public lifecycle):
    market_lifecycle_v2
    multivariate_market_lifecycle

Writes
------
- forward_lifecycle_ws/dt=YYYY-MM-DD/<run_id>_<seq>.parquet  (zstd)
- Co-located SHA-256 sidecar on every flushed parquet
- Heartbeat in state/lifecycle_ws_heartbeat.json (last metrics)
- Audit log in state/surveillance_runs/lifecycle_ws_<run_id>.json on graceful exit

Run
---
  uv run python scripts/capture_lifecycle_ws.py            # foreground
  uv run python scripts/capture_lifecycle_ws.py --backfill-window 0   # live only

Designed to run as a long-lived systemd Type=simple service alongside the
existing capture_ws_microstructure.py.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import json
import os
import signal
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pyarrow as pa
import pyarrow.parquet as pq
import websockets

# Auth deps
try:
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding
except Exception as exc:  # pragma: no cover
    serialization = None  # type: ignore[assignment]
    padding = None        # type: ignore[assignment]
    hashes = None         # type: ignore[assignment]
    _CRYPTO_IMPORT_ERR = exc
else:
    _CRYPTO_IMPORT_ERR = None

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = PROJECT_ROOT / "data" / "kalshi"
HISTORICAL = DATA_ROOT / "historical"
STATE = DATA_ROOT / "state"
OUT_DIR = HISTORICAL / "forward_lifecycle_ws"
HEARTBEAT = STATE / "lifecycle_ws_heartbeat.json"
AUDIT_DIR = STATE / "surveillance_runs"

WS_URL = os.environ.get("KALSHI_WS_URL", "wss://api.elections.kalshi.com/trade-api/ws/v2")
WS_PATH = "/trade-api/ws/v2"

# Channels to subscribe to
CHANNELS = ["market_lifecycle_v2", "multivariate_market_lifecycle"]

FLUSH_ROWS = 500          # flush after this many rows
FLUSH_SECONDS = 60        # or after this many seconds


SCHEMA = pa.schema([
    pa.field("ts_ms",            pa.int64()),         # message ts_ms if present
    pa.field("ingested_at_ms",   pa.int64()),         # our receive time
    pa.field("channel",          pa.string()),        # market_lifecycle_v2 etc.
    pa.field("msg_type",         pa.string()),        # type field on msg
    pa.field("market_ticker",    pa.string()),
    pa.field("event_ticker",     pa.string()),
    pa.field("series_ticker",    pa.string()),
    pa.field("sub_type",         pa.string()),        # nested sub-type if any
    pa.field("status",           pa.string()),        # market status if present
    pa.field("result",           pa.string()),        # resolution result if present
    pa.field("seq",              pa.int64()),
    pa.field("sid",              pa.int64()),
    pa.field("msg_json",         pa.string()),        # full message JSON
    pa.field("run_id",           pa.string()),
])


_shutdown: Optional[asyncio.Event] = None


def _log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"{ts}  {msg}", flush=True)


# -------------------- audit / sidecar --------------------

def _sha256_file(path: Path, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(chunk)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def _write_sha256_sidecar(parquet_path: Path) -> None:
    digest = _sha256_file(parquet_path)
    sidecar = parquet_path.with_suffix(parquet_path.suffix + ".sha256")
    tmp = sidecar.with_suffix(sidecar.suffix + ".tmp")
    tmp.write_text(f"{digest}  {parquet_path.name}\n", encoding="utf-8")
    os.replace(tmp, sidecar)


def _write_heartbeat(state: dict[str, Any]) -> None:
    HEARTBEAT.parent.mkdir(parents=True, exist_ok=True)
    tmp = HEARTBEAT.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, HEARTBEAT)


def _write_audit_log(run_id: str, payload: dict[str, Any]) -> None:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    out = AUDIT_DIR / f"lifecycle_ws_{run_id}.json"
    tmp = out.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, out)


# -------------------- auth --------------------

def _build_auth_headers() -> dict[str, str]:
    """
    Kalshi RSA-PSS auth: sign "<timestamp_ms>GET<path>" with the private key,
    SHA-256, PSS padding, base64 the signature. Send as X-KALSHI-* headers.

    Env vars:
      KALSHI_API_KEY_ID — UUID-like key id
      KALSHI_API_PRIVATE_KEY_PEM — PEM-encoded private key (multiline string,
                                   or path to file)
    """
    if _CRYPTO_IMPORT_ERR:
        raise RuntimeError(f"cryptography import failed: {_CRYPTO_IMPORT_ERR!r}")

    key_id = os.environ.get("KALSHI_API_KEY_ID")
    pem = os.environ.get("KALSHI_API_PRIVATE_KEY_PEM")
    if not key_id or not pem:
        # Public channels do NOT need auth, but if env is set we use it
        return {}
    if pem.startswith("/") and os.path.exists(pem):
        pem = Path(pem).read_text(encoding="utf-8")
    pkey = serialization.load_pem_private_key(pem.encode("utf-8"), password=None)
    ts_ms = str(int(time.time() * 1000))
    payload = f"{ts_ms}GET{WS_PATH}".encode("utf-8")
    sig = pkey.sign(
        payload,
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
        hashes.SHA256(),
    )
    return {
        "KALSHI-ACCESS-KEY":       key_id,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode("ascii"),
        "KALSHI-ACCESS-TIMESTAMP": ts_ms,
    }


# -------------------- buffered writer --------------------

class ChannelWriter:
    def __init__(self, run_id: str):
        self.run_id = run_id
        self.buf: list[dict[str, Any]] = []
        self.flushes = 0
        self.last_flush = time.time()
        self.total_rows = 0

    def add(self, row: dict[str, Any]) -> None:
        self.buf.append(row)
        if len(self.buf) >= FLUSH_ROWS:
            self.flush()

    def maybe_flush(self) -> None:
        if self.buf and (time.time() - self.last_flush) >= FLUSH_SECONDS:
            self.flush()

    def flush(self) -> None:
        if not self.buf:
            return
        dt = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        out_dir = OUT_DIR / f"dt={dt}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / f"{self.run_id}_{self.flushes:06d}.parquet"
        cols: dict[str, list[Any]] = {f.name: [] for f in SCHEMA}
        for row in self.buf:
            for fname in cols:
                cols[fname].append(row.get(fname))
        table = pa.Table.from_pydict(cols, schema=SCHEMA)
        tmp = out_file.with_suffix(".parquet.tmp")
        pq.write_table(table, tmp, compression="zstd")
        tmp.replace(out_file)
        _write_sha256_sidecar(out_file)
        n = len(self.buf)
        self.total_rows += n
        _log(f"  flushed {n:,} rows -> {out_file.name} (run total {self.total_rows:,})")
        self.buf.clear()
        self.flushes += 1
        self.last_flush = time.time()


# -------------------- message handling --------------------

def _parse_message(raw: str, run_id: str) -> Optional[dict[str, Any]]:
    try:
        msg = json.loads(raw)
    except Exception:
        return None
    if not isinstance(msg, dict):
        return None

    inner = msg.get("msg") if isinstance(msg.get("msg"), dict) else msg
    msg_type = str(msg.get("type") or inner.get("type") or "")

    # Filter to just lifecycle channels — skip subscribed/ack/error noise
    if msg_type in ("subscribed", "ok", "error", "pong", ""):
        # Still log errors for visibility
        if msg_type == "error":
            _log(f"WARN  ws error msg: {raw[:240]}")
        return None

    ts_ms = inner.get("ts_ms") or msg.get("ts_ms")
    try:
        ts_ms_i = int(ts_ms) if ts_ms is not None else 0
    except Exception:
        ts_ms_i = 0

    channel = msg.get("channel") or msg.get("sid_channel") or ""
    # If channel is missing, infer from type
    if not channel:
        if msg_type.startswith("market_") or "lifecycle" in msg_type:
            channel = "market_lifecycle_v2"
        elif "multivariate" in msg_type:
            channel = "multivariate_market_lifecycle"

    return {
        "ts_ms":         ts_ms_i,
        "ingested_at_ms": int(time.time() * 1000),
        "channel":       channel or "unknown",
        "msg_type":      msg_type,
        "market_ticker": inner.get("market_ticker") or inner.get("ticker"),
        "event_ticker":  inner.get("event_ticker"),
        "series_ticker": inner.get("series_ticker"),
        "sub_type":      str(inner.get("sub_type")) if inner.get("sub_type") else None,
        "status":        inner.get("status"),
        "result":        inner.get("result"),
        "seq":           int(msg.get("seq", -1)) if msg.get("seq") is not None else None,
        "sid":           int(msg.get("sid", -1)) if msg.get("sid") is not None else None,
        "msg_json":      raw,
        "run_id":        run_id,
    }


# -------------------- websocket loop --------------------

async def _ws_loop(run_id: str) -> dict[str, Any]:
    """Connect, subscribe, stream lifecycle messages until shutdown."""
    writer = ChannelWriter(run_id)
    metrics = {"connects": 0, "messages": 0, "errors": 0, "seq_gaps": 0}
    last_seq_per_sid: dict[int, int] = {}

    while not _shutdown.is_set():  # type: ignore[union-attr]
        try:
            headers = _build_auth_headers()
            metrics["connects"] += 1
            _log(f"connecting to {WS_URL} (run_id={run_id})")
            async with websockets.connect(WS_URL, additional_headers=headers, ping_interval=20, ping_timeout=10) as ws:
                # Subscribe
                for i, channel in enumerate(CHANNELS, 1):
                    cmd = {"id": i, "cmd": "subscribe", "params": {"channels": [channel]}}
                    await ws.send(json.dumps(cmd))
                _log(f"subscribed: {CHANNELS}")

                last_hb = time.time()
                while not _shutdown.is_set():  # type: ignore[union-attr]
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=10.0)
                    except asyncio.TimeoutError:
                        writer.maybe_flush()
                        if time.time() - last_hb > 60:
                            _write_heartbeat({
                                "run_id": run_id,
                                "ts_iso": datetime.now(timezone.utc).isoformat(),
                                "messages": metrics["messages"],
                                "flushes": writer.flushes,
                                "total_rows": writer.total_rows,
                                "connects": metrics["connects"],
                                "errors": metrics["errors"],
                                "seq_gaps": metrics["seq_gaps"],
                                "channels": CHANNELS,
                            })
                            last_hb = time.time()
                        continue

                    metrics["messages"] += 1
                    row = _parse_message(raw, run_id)
                    if row is None:
                        continue

                    # Detect seq gaps per sid
                    sid = row["sid"]
                    seq = row["seq"]
                    if sid is not None and seq is not None and sid >= 0 and seq >= 0:
                        prev = last_seq_per_sid.get(sid)
                        if prev is not None and seq > prev + 1:
                            metrics["seq_gaps"] += 1
                            _log(f"WARN  seq gap sid={sid} prev={prev} cur={seq} (+{seq-prev})")
                        last_seq_per_sid[sid] = seq

                    writer.add(row)
                    writer.maybe_flush()

        except asyncio.CancelledError:
            break
        except Exception as exc:
            metrics["errors"] += 1
            _log(f"ERROR  ws loop exception: {exc!r} — reconnecting in 5s")
            try:
                await asyncio.sleep(5)
            except asyncio.CancelledError:
                break

    # Final flush
    writer.flush()
    metrics["total_rows"] = writer.total_rows
    metrics["flushes"] = writer.flushes
    return metrics


async def _run(args: argparse.Namespace) -> int:
    global _shutdown
    _shutdown = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig_num in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig_num, _shutdown.set)
        except NotImplementedError:
            signal.signal(sig_num, lambda *_: _shutdown.set())  # type: ignore[union-attr]

    run_id = f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"
    started_at = datetime.now(timezone.utc).isoformat()
    started_unix = time.time()
    _log("=" * 72)
    _log(f"capture_lifecycle_ws run_id={run_id}")
    _log(f"  ws_url   = {WS_URL}")
    _log(f"  channels = {CHANNELS}")
    _log("=" * 72)

    metrics = await _ws_loop(run_id)

    payload = {
        "run_id": run_id,
        "started_at_utc": started_at,
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "duration_seconds": round(time.time() - started_unix, 3),
        "channels": CHANNELS,
        "metrics": metrics,
        "ws_url": WS_URL,
    }
    _write_audit_log(run_id, payload)
    _log("=" * 72)
    _log(f"DONE rows={metrics.get('total_rows', 0):,} flushes={metrics.get('flushes', 0)} connects={metrics.get('connects', 0)} errors={metrics.get('errors', 0)} seq_gaps={metrics.get('seq_gaps', 0)}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Capture Kalshi market_lifecycle_v2 WS channel")
    args = ap.parse_args()
    return asyncio.run(_run(args))


if __name__ == "__main__":
    sys.exit(main())
