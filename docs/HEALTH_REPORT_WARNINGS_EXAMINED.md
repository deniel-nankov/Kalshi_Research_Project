# Health Report Warnings — Careful Examination

This doc walks through each **WARN** from the current health report, what it means, how serious it is, and what to do.

---

## 1. UNIQUENESS_MARKETS — Duplicate market keys: 39

**What it says:** 973,786 market rows but only 973,747 distinct keys `(ticker, close_time)`. So 39 keys appear more than once.

**Why it happens:** The same market snapshot was written in more than one place (e.g. two forward runs with overlapping windows, or same market on two API pages before we added in-fetch dedupe). This is **legacy** from before the current dedupe logic.

**How serious:** **Low.** It’s 39 out of ~974k markets. Analytics can use `DISTINCT (ticker, close_time)` or “keep latest by _ingested_at” for a canonical market list.

**What to do:**
- **Optional cleanup:** Run `uv run python scripts/dedupe_forward_markets_once.py` to build a deduped market dataset under `data/kalshi/historical/forward_markets_dedupe/`.
- **No action:** New forward runs won’t add new duplicates (in-fetch + at-write dedupe). You can ignore the 39 if you dedupe in queries.

---

## 2. REFERENTIAL_INTEGRITY — 2,552 orphan tickers

**What it says:** 2,552 tickers appear in **trades** but not in **markets**. So some trades reference markets we don’t have.

**Why it happens:** Mix of: (a) historical snapshot timing (markets at time T, trades for markets added/closed before T), (b) forward windowing (trade in window, market’s `close_time` outside the market fetch window), (c) delisted/retired markets still in trade history. We already widened the market window by 7 days to reduce this.

**How serious:** **Moderate.** Joins from trades → markets will miss those 2,552 tickers. Trade-level analytics (by ticker, price, time) are fine; attaching market metadata (title, volume, etc.) will be incomplete for those tickers.

**What to do:**
- **Accept:** For many use cases (e.g. trade volume by ticker, price series) this is acceptable.
- **Improve over time:** Keep running forward ingestion; the 7-day lookback will reduce new orphans. Re-downloading historical with a fresh markets snapshot could reduce historical orphans.
- **No code bug:** This is expected given how the API and our windows work.

---

## 3. BOUNDARY_ALIGNMENT — API cutoff fetch failed: 403 Forbidden

**What it says:** The validator tried to call the Kalshi API (`/historical/cutoff`) to get `trades_created_ts` and compare it to your max trade time. The request failed with **403 Forbidden** (often due to network/proxy/VPN or API auth).

**Why it happens:** The script uses `urllib.request.urlopen(CUTOFF_URL)`. 403 usually means: (a) request blocked by firewall/proxy, (b) VPN or corporate network blocking the host, (c) API requires auth/cookies and we’re not sending them. It’s an **environment/network** issue, not a data bug.

**How serious:** **Low for data quality.** Your data is not wrong because of this. We simply **couldn’t verify** that “max trade time ≤ API cutoff.” So we don’t know from this run whether you’re within the official historical boundary.

**What to do:**
- **If you’re on VPN/proxy:** Try from a network that can reach `api.elections.kalshi.com` (or whatever CUTOFF_URL resolves to).
- **Otherwise:** Treat BOUNDARY_ALIGNMENT as **“skipped”** when you see this. The validator now reports: *“Skipped (API unreachable: 403 Forbidden — network/proxy/VPN?)”* so it’s clear the check was not run, not that your data failed. You can still trust the rest of the report.

---

## 4. BOUNDARY_GAP — Large gap: 25.0h (last historical → first forward)

**What it says:** Last historical trade: **2025-03-10 22:59:55**. First forward trade: **2025-03-12 00:00:00**. So there’s a **25-hour** gap with no trades in your dataset between those two times.

**Why it happens:** Either (a) **missing ingestion** — no forward run covered 2025-03-11 (e.g. pipeline wasn’t running that day), or (b) **intentional** — e.g. historical batch ended at a cutoff and the first forward run started later (e.g. next day). So we may be **missing trades** that occurred on 2025-03-11.

**How serious:** **Worth checking.** If you intended to have continuous coverage, you’re missing about a day of trades. If the gap is intentional (e.g. you only started forward ingestion on 2025-03-12), you can document that and treat the WARN as expected.

**What to do:**
- **Backfill:** Use the steps in **"How to backfill the BOUNDARY_GAP"** below to fill the gap with `update_forward.py --end-ts`.
- **Document:** If the gap is intentional, note it (e.g. “Forward data starts 2025-03-12; no data for 2025-03-11 by design”).
- **Threshold:** The validator WARNs when gap > 24h. Your 25h is just over that; fixing the missing day would turn this to PASS.

---

## 5. FORWARD_COUNT_COMPLETENESS — 100% of forward trades with count=0

**What it says:** In the **stored** forward data, the `count` column is 0 for 100% of forward trades (131,401 trades).

**Why it happens:** The **API** often sends `count=0` and puts contract size in `count_fp`. We **now** fill `count` from `count_fp` at ingestion time, but the 131,401 forward rows were **ingested before** that change (or the Parquet was written with the old logic). So on disk, `count` is still 0 even though `count_fp` is populated.

**How serious:** **Low for correctness.** You have contract size in `count_fp` for those trades. The WARN is to catch **future** regressions (e.g. if we stopped writing count_fp or broke _parse_count). For **existing** data, use `count_fp` (or re-ingest forward to get `count` filled from count_fp).

**What to do:**
- **Analytics:** Use `count_fp` for contract size when `count=0`, or use the column we derive at read time (e.g. `COALESCE(NULLIF(count, 0), CAST(count_fp AS INT)`).
- **Reduce WARN over time:** New forward runs will write non-zero `count` (from count_fp). As you add more new data, the share of forward trades with count=0 will drop; the check WARNs when >95% have count=0.
- **One-time fix:** Re-running forward ingestion from the same checkpoint would re-fetch and re-write with the new _parse_count logic, but that’s usually not worth it just for this; using count_fp in queries is enough.

---

## 6. COMPLETENESS — 98.0% of all trades with count=0

**What it says:** Across **all** trades (historical + forward), 98.02% have `count=0` in the stored data.

**Why it happens:** Same as above: the API often sends only `count_fp`, and (a) historical data was downloaded with the old mapping that didn’t fill `count` from `count_fp`, and (b) forward data was ingested before we added _parse_count. So the **stored** `count` is 0 for most rows even though `count_fp` usually has the value.

**How serious:** **Low for correctness** as long as you use `count_fp` (or a derived “effective count”) where you need contract size. The WARN is mainly informational: “most rows have count=0 on disk.”

**What to do:**
- **Same as FORWARD_COUNT_COMPLETENESS:** Use `count_fp` (or a view that coalesces count/count_fp) for volume/contract-size analytics.
- **Optional:** Re-download historical and re-run forward with current code to backfill `count` from `count_fp`; then the percentage would drop. Not required if you’re happy using count_fp in queries.

---

## Summary Table

| WARN | Severity | Action |
|------|----------|--------|
| UNIQUENESS_MARKETS (39 dupes) | Low | Optional: run `dedupe_forward_markets_once.py` or dedupe in queries. |
| REFERENTIAL_INTEGRITY (2,552 orphans) | Moderate | Accept or improve over time with 7-day lookback; consider historical re-download. |
| BOUNDARY_ALIGNMENT (403) | Low | Network/environment; skip or run from a network that can reach the API. |
| BOUNDARY_GAP (25h) | Worth checking | Backfill 2025-03-11 if you want continuous coverage; or document as intentional. |
| FORWARD_COUNT_COMPLETENESS (100% count=0) | Low | Use `count_fp` for size; new data will have `count` filled. |
| COMPLETENESS (98% count=0) | Low | Use `count_fp` or derived count; optional re-ingest to backfill `count`. |

**Overall:** No FAILs; the 6 WARNs are either expected (duplicates, orphans, count=0 with count_fp present), environment-related (403), or actionable (BOUNDARY_GAP). The only one that might mean **missing data** is BOUNDARY_GAP (possible missing trades on 2025-03-11).

---

## How to backfill the BOUNDARY_GAP

When the report shows a gap between last historical and first forward (e.g. last historical **2025-03-10 22:59**, first forward **2025-03-12 00:00**), you can fill that window with one run of forward ingestion using `--end-ts`. You do **not** need the API cutoff (BOUNDARY_ALIGNMENT) for this; the window is defined by your own data.

**1. Choose the exact window (from the health report)**  
- **Gap start:** last historical trade time, e.g. `2025-03-10 22:59:55` → use the next second as “start after this.”  
- **Gap end:** first forward trade time, e.g. `2025-03-12 00:00:00` → use this as “fetch up to and including.”

**2. Convert to UNIX timestamps (UTC)**  
Example (you can confirm with `python -c "from datetime import datetime, timezone; print(int(datetime(2025,3,10,22,59,55,tzinfo=timezone.utc).timestamp()))"`):
- Start (exclusive): `2025-03-10 22:59:55 UTC` → **1741647595**
- End (inclusive): `2025-03-12 00:00:00 UTC` → **1741737600**

**3. Back up the checkpoint**  
```bash
cp data/kalshi/state/forward_checkpoint.json data/kalshi/state/forward_checkpoint.json.bak
```

**4. Set the checkpoint to the gap start**  
Edit `data/kalshi/state/forward_checkpoint.json` and set both watermarks to the **exclusive** start of the gap (so we fetch *after* that time):
- `"watermark_trade_ts": 1741647595`
- `"watermark_market_ts": 1741647595`

**5. Run forward ingestion with `--end-ts`**  
This fetches only the gap window and writes new Parquet; checkpoint will advance to `--end-ts`.
```bash
uv run python scripts/update_forward.py --end-ts 1741737600
```

**6. (Optional) Run again without `--end-ts`**  
To move the checkpoint to “now” and pick up any newer data:  
`uv run python scripts/update_forward.py`  
Existing forward data is deduped by `trade_id`, so you won’t duplicate rows.

**7. Re-run the health report**  
BOUNDARY_GAP should PASS (gap ≤ 24h). BOUNDARY_ALIGNMENT is independent; if the API still returns 403, that check will still show “Skipped (API unreachable: 403 …)” and does not affect the backfill.
