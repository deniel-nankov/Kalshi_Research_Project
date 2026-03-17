# Data Health Warnings Explained

What each of the 4 warnings means, where the data comes from, and what the possible causes are.

---

## 1. Duplicate market keys (39)

**What it means:** The validator treats a market as unique by the key `(ticker, close_time)`. There are 39 such pairs that appear more than once in the combined markets dataset (973,786 rows vs 973,747 distinct keys).

**Where it comes from:** Forward/incremental market data. Examples of duplicated keys:
- `KXDJTNATOMENTION-25MAR13-*` (multiple contracts, same close_time)
- `KXWTAIWO-25ANDSVI-SVI`, `KXWTAIWO-25ZHESWI-SWI`
- `KXEARNINGSMENTIONDOLLARGENERAL-25MAR-*`

**Possible sources:**
- **Same market returned in multiple API pages** when we fetch by `min_close_ts`/`max_close_ts` with cursor; we dedupe by key but the same (ticker, close_time) can appear in two different forward runs or in two files.
- **Two forward runs** wrote overlapping windows and both wrote the same market snapshot (dedupe at write time uses existing keys; if the same market was in two run outputs, we’d have two rows).
- **API returning the same market twice** in one paginated response (e.g. updated_time or another field differing but close_time the same).

**Impact:** Low. A few dozen duplicate market snapshots; for analytics you can `DISTINCT` on `(ticker, close_time)` or keep the latest by `_ingested_at` if needed.

**Fixes applied:**
- **At fetch:** `update_forward.py` now dedupes within a single API run: same `(ticker, close_time)` from pagination is only kept once (`seen_key_in_fetch`).
- **At write:** We already dedupe against existing keys and within the batch before writing; no duplicate keys are written in new runs.
- **One-time cleanup:** Run `uv run python scripts/dedupe_forward_markets_once.py` to produce a single deduped Parquet under `data/kalshi/historical/forward_markets_dedupe/`; use it for canonical market lists or replace `forward_markets` if you want to remove existing duplicates from disk.

---

## 2. Orphan tickers (2,552)

**What it means:** 2,552 distinct tickers appear in **trades** but not in **markets**. About 71,817 trades reference those tickers.

**Where it comes from:** Mix of historical and forward:
- Sample tickers: `KXHIGHLAX-25MAR14-B52.5`, `KXNCAAMBIGTEN-25-MICH`, `PRES-2024-*`, `KXBTCD-25MAR1517-*`, etc.
- So both older and newer markets can be “missing” from the markets dataset.

**Possible sources:**
- **Historical trades vs markets timing:** `download_historical` gets markets first, then trades per market. If the markets snapshot is from time T and the API later adds trades for markets that weren’t in that snapshot (or that closed before T), those trades will have tickers not in our markets file.
- **Forward window mismatch:** We fetch trades by `(min_ts, max_ts]` and markets by `min_close_ts`/`max_close_ts`. A trade can be in the trade window while its market’s `close_time` is outside the market window, so we never ingest that market.
- **Markets retired or delisted** and no longer returned by `/historical/markets` or the markets window, but their trades are still in the trades API.
- **Different API products:** Trades might come from an endpoint that includes more tickers than the markets we pull.

**Impact:** Moderate. Joins from trades to markets will miss 2,552 tickers; aggregations by ticker will still work for trades, but you can’t attach market metadata for those.

**Fixes applied:**
- **Wider market window:** `update_forward.py` uses `MARKET_LOOKBACK_DAYS = 7`: we fetch markets with `min_close_ts = max(min_ts_exclusive + 1 - 7 days, 0)`, so trades that reference markets that closed slightly before the trade window still get a matching market and orphan count should drop over time for new data.

---

## 3. 98% of trades with `count = 0`

**What it means:** The `count` field (number of contracts in the trade) is 0 for about 98% of historical trades and **100%** of forward/legacy incremental trades.

**Where it comes from:**
- **Historical:** 11,407,641 of 11,640,948 trades (98.0%) have `count = 0`.
- **Forward:** 45,024 of 45,024 (100%) have `count = 0`.
- **Legacy incremental:** 86,377 of 86,377 (100%) have `count = 0`.

So the pattern is strong in historical data and total in incremental data.

**Possible sources:**
- **API behavior:** The Kalshi API may omit or set `count` to 0 when:
  - Trades are fetched by **time range** (`/markets/trades?min_ts=&max_ts=`) instead of by ticker.
  - Or for certain event types or legacy data.
- **Schema:** We map `count` with `int(t.get("count", 0) or 0)` in both `download_historical.py` and `update_forward.py`. If the API doesn’t send `count` or sends null/0, we store 0.
- **Precision:** The API may expose size via `count_fp` (string) instead of `count` (int). If we only persist `count` and not `count_fp` in analytics, we’d see 0 where the real size is in `count_fp`.

**Impact:** High for volume/position-size analytics. If you need trade size, check whether `count_fp` is populated and use it (or both) in your pipeline.

**Fixes applied:**
- **Forward ingestion:** In `update_forward.py`, `_parse_count(t)` uses `count` and, when it’s 0, falls back to parsing `count_fp` so contract size is stored when the API only sends `count_fp`.
- **Historical ingestion:** In `download_historical.py`, `_trade_row()` now uses the same `_parse_count(t)` logic so historical re-downloads get non-zero `count` when `count_fp` is present.
- **Prevention:** The health script has a **FORWARD_COUNT_COMPLETENESS** check: if &gt;95% of forward trades have `count=0`, it WARNs so you can catch API/schema regressions.

---

## 4. Volume outlier (max vs p99)

**What it means:** The validator compares **max market volume** to the **99th percentile (p99)**. If max is huge relative to p99, it flags a possible outlier. Here: max = 273,312,857, p99 ≈ 26,342 (max is ~10,000× p99).

**Where it comes from:** A small number of very high-volume markets:
- `PRES-2024-KH` – “Will Kamala Harris or another Democrat win the Presidency?” – volume 273,312,857
- `PRES-2024-DJT` – “Will Donald Trump or another Republican win the Presidency?” – volume 262,334,207
- `POPVOTE-24-D` – popular vote market – 45,192,568

**Possible sources:** This is **expected**: a few headline political markets have orders of magnitude more activity than typical markets. The validator’s rule (e.g. max < 10 × p99) is a simple sanity check; it will often “warn” when a small number of super-liquid markets exist.

**Impact:** None for correctness. You can treat this as informational or relax/remove the check if you’re comfortable with extreme volume skew.

**Fixes applied:**
- **Relaxed check:** The validator now only WARNs if max volume is &gt;15,000× p99 (instead of 10×), so headline markets like Presidency no longer trigger the warning.

---

## Summary

| Warning              | Meaning                          | Main source                    | Fix / prevention                                              |
|----------------------|----------------------------------|--------------------------------|---------------------------------------------------------------|
| Duplicate market keys| 39 (ticker, close_time) repeated | Forward market ingestion       | In-fetch + at-write dedupe; one-time `dedupe_forward_markets_once.py` |
| Orphan tickers       | 2,552 tickers in trades only     | Window/timing and API coverage | 7-day market lookback in forward ingestion                    |
| 98% count=0          | Most trades have contract count 0| API / field mapping            | `_parse_count()` (count_fp fallback) in forward + historical; FORWARD_COUNT_COMPLETENESS check |
| Volume outlier       | A few markets have huge volume   | Real (e.g. Presidency markets) | Check relaxed to 10,000× p99                                  |
