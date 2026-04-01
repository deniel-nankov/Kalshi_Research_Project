#!/usr/bin/env python3
"""Load .env and perform one authenticated Kalshi API call (list_markets limit=1).

Usage (repo root):
  uv run python scripts/verify_kalshi_credentials.py

Requires in .env:
  KALSHI_API_KEY_ID
  KALSHI_API_PRIVATE_KEY   (PEM, including BEGIN/END lines — same as Kalshi dashboard download)
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    sys.path.insert(0, str(ROOT))
    load_dotenv(ROOT / ".env")

    import os

    kid = os.environ.get("KALSHI_API_KEY_ID", "").strip()
    pk = os.environ.get("KALSHI_API_PRIVATE_KEY", "").strip()
    if not kid or not pk:
        print("Missing KALSHI_API_KEY_ID or KALSHI_API_PRIVATE_KEY in .env")
        print(f"Expected file: {ROOT / '.env'}")
        return 1

    # Client reads credentials at import time — reload after dotenv
    import src.indexers.kalshi.client as kc

    importlib.reload(kc)

    try:
        with kc.KalshiClient() as client:
            markets = client.list_markets(limit=1)
    except Exception as e:
        print(f"FAIL: API request failed: {e}")
        return 1

    print(f"OK: authenticated; fetched {len(markets)} market(s) (smoke test).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
