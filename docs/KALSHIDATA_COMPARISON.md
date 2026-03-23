# Comparison: our dataset vs [Kalshi Data (kalshidata.com)](https://www.kalshidata.com)

## Quick answer: can you compare?

| Question | Answer |
|----------|--------|
| **Can I compare at all?** | **Yes** for **volume (USD)** and **contracts** in a *rough* way: run the script below to see **what % of official volume** your snapshot represents. |
| **Same stats, apples-to-apples?** | **Not fully.** Official **Total Trades (~52B)** is **not** the same as your **~12M API trade rows**. Open interest is **not** in your trade files. |
| **How far from “reality”?** | Run **`compare_to_kalshidata.py`** — it prints **your % of official volume** and explains gaps. |

```bash
# 1. Update official numbers in data/kalshi/state/kalshidata_baseline.json (or copy from .example.json)
# 2. Run:
uv run python scripts/compare_to_kalshidata.py
```

---

Official **Kalshi Data** reports platform-wide metrics from **the first trade** (baseline **Jun 27, 2021**). Our pipeline uses the **historical + forward API** into Parquet; numbers below are **not** expected to match 1:1 until definitions and coverage align.

---

## Side-by-side (Kalshi Data vs our pipeline)

| Metric | [Kalshi Data](https://www.kalshidata.com) (Daily / “since first trade”) | Our pipeline (latest run) |
|--------|--------------------------------------------------------------------------|---------------------------|
| **Baseline / range** | From first trade; trends “since Jun 27, 2021” | Trades **2021-06-30** … **2025-03-14** (no data after last forward run) |
| **Total volume** | **$52,032,842,071** (~$52B) | **~$960M** notional (Σ contracts × `yes_price_dollars`) |
| **Daily avg volume** | **$30,446,368** | *(not computed in repo)* |
| **Total trades** *(their label)* | **52,019,359,454** (~52B) | **~11.8M** distinct **trade rows** (`trade_id` in API) |
| **Avg open interest** | **$82,051,301** | *(not in our trade files; would need positions / OI API)* |
| **Contracts traded** *(their charts)* | Cumulative over time (see site) | **~2.65B** contracts (Σ `count`/`count_fp`) |

---

## How to refresh our numbers

```bash
uv run python scripts/data_stats.py | tee data/kalshi/state/dataset_stats_latest.txt
```

---

## Why the gaps are so large

### 1. **“Total trades” on Kalshi Data ≠ our trade row count**

We store **one row per trade execution** from the trades API (~**11.8M** rows). Kalshi Data shows **~52 billion** under **Total Trades**. That is **~4,400×** our count, so they are **not** using the same definition as “rows in the trades endpoint.”

Likely interpretations (until Kalshi publishes exact definitions):

- **Cumulative micro-transactions** or **fills** (many per user-facing “trade”), or  
- **Contract-legs** counted separately, or  
- **A different internal metric** labeled “trades” on the dashboard.

**Takeaway:** Do **not** expect our `COUNT(*)` of trades to equal their **Total Trades** without their methodology doc.

### 2. **Volume: ~$52B vs ~$960M (~54×)**

Our notional is **taker-side style**: for each API trade row, **contracts × `yes_price_dollars`**. Reasons we can still be far below **$52B**:

| Factor | Effect |
|--------|--------|
| **Coverage** | Our snapshot ends **2025-03-14**. Kalshi Data is **live** (2025–2026+). Later activity adds billions in volume. |
| **Definition** | They may sum **both sides**, **maker+taker**, or **market-level** volume; we only sum what’s in each **trade row** once. |
| **API vs internal** | Official analytics may use **internal books**; we only have what the **public historical + trades API** returns. |
| **Missing markets** | Any market not in our historical/forward pull contributes **zero** to our total. |

### 3. **Contracts: ~2.65B us vs their cumulative charts**

Our **~2.65B** is Σ contract size from stored rows. If their cumulative **contracts** chart is higher, similar causes apply: **full platform coverage**, **longer time range**, and possibly **double-counting** or **different counting rules**.

### 4. **Open interest**

We **do not** compute **avg open interest** from trades alone. That needs **positions / OI** data (different API or product). Kalshi Data’s **$82M avg OI** is a separate metric.

---

## How we compute our metrics (for apples-to-apples if Kalshi publishes formulas)

| Our metric | Definition |
|------------|------------|
| **Trade rows** | `COUNT(*)` over historical ∪ forward trade Parquet (legacy forward optional in volume). |
| **Total contracts** | `SUM(COALESCE(NULLIF(count,0), TRY_CAST(count_fp AS DOUBLE)))`. |
| **Notional USD** | `SUM(effective_count × TRY_CAST(yes_price_dollars AS DOUBLE))` (fallback where column missing). |

---

## What you can do next

1. **Extend coverage:** Run `update_forward.py` regularly so our **end date** approaches Kalshi Data’s “today”; volume and trades will grow closer in **time coverage**.  
2. **Ask Kalshi / read site docs:** Confirm what **Total Trades** and **Total Volume** include (fills, both sides, all venues, etc.).  
3. **Use Kalshi Data for headline platform stats** and **our Parquet** for **row-level research** on the subset we ingested—treat them as **complementary**, not identical counters, until definitions match.

---

*Kalshi Data figures above copied from user report of [kalshidata.com](https://www.kalshidata.com) (Daily, “since first trade”). Update our side after each `data_stats.py` run.*
