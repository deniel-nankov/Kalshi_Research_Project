# Kalshi Calibration Explained (Simple Terms)

## What is Calibration?

**Calibration** measures how accurate the market's probability predictions are compared to reality.

### Simple Analogy

Imagine a weather forecaster who says:
- "30% chance of rain" on 100 days
- It actually rains on 30 of those days → **Perfectly calibrated!**
- It actually rains on 50 of those days → **Not calibrated** (off by 20%)

## How We Calculate It

### Step 1: Look at Each Trade
For every trade on Kalshi:
- **Price = Predicted Probability** (e.g., price of 75 = 75% probability)
- **Outcome = Reality** (did it happen? Yes/No → 100% or 0%)

### Step 2: Group by Price Level
- All trades at price 75: Did they win 75% of the time?
- All trades at price 50: Did they win 50% of the time?
- All trades at price 25: Did they win 25% of the time?

### Step 3: Calculate the Error
For each price level:
```
Deviation = |Actual Win Rate - Predicted Probability|
```

Example:
- Trades at 75 cents won 68% of the time
- Deviation = |68% - 75%| = 7 percentage points

### Step 4: Average All Errors
Take the average deviation across all price levels = **Mean Absolute Deviation**

## Your Results

**Deviation: 7.88 percentage points**

### What This Means:

✅ **Good News:** Less than 10pp is generally considered decent calibration

⚠️ **What It Tells Us:**
- On average, Kalshi's market prices were off by ~7.88%
- If a market traded at 60%, it might actually resolve at 52% or 68%
- Markets aren't perfectly efficient (there's some prediction error)

### Example:
If you see a market at **70%**:
- **Perfect calibration:** Should resolve Yes exactly 70% of the time
- **Your data (7.88% error):** Might resolve Yes anywhere from 62% to 78% of the time

## Why Volume Over Time Looks "Empty"

Your dataset only covers **2 days** (March 5-6, 2025):
- **All trades fall in Q1 2025** (January-March)
- So you only have **1 quarter** of data
- That's why the CSV shows just 1 row:

```csv
quarter,volume_usd
2025-01-01,202643.0
```

This is **correct** - you'd need multiple months/quarters of data to see a trend over time.

### To Get More Data Points:

You'd need to fetch trades spanning multiple quarters, like:
- Q4 2024: October-December
- Q1 2025: January-March  
- Q2 2025: April-June
- Q3 2025: July-September

Then you'd see 4 rows showing how volume changed quarter by quarter.

## How to Read Calibration Results

### Perfect Calibration (Deviation = 0%)
- Market prices exactly match reality
- If price = 80%, outcomes happen exactly 80% of the time
- **Extremely rare** in real markets

### Good Calibration (Deviation < 5%)
- Market is very accurate
- Prices are reliable predictors
- Professional traders trust these markets

### Decent Calibration (Deviation 5-10%)
- **← Your data is here (7.88%)**
- Market is reasonably accurate
- Some inefficiencies exist but not major
- Typical for emerging markets

### Poor Calibration (Deviation > 10%)
- Market has significant errors
- Prices don't reliably predict outcomes
- Opportunity for informed traders

## Verification of Your Results

Let's verify the calculation is correct:

**Your Data:**
- **Total positions:** 1,516 trades
- **Both sides counted:** Taker + Maker = 3,032 positions
- **Date range:** March 5-6, 2025
- **Result:** 7.88% mean absolute deviation on March 6

**Is This Correct?** ✅ **Yes!**

The calculation:
1. Takes all 1,516 trades
2. Counts both the taker (buyer) and maker (seller) positions = 3,032 positions
3. For each position, records:
   - The price they paid (predicted probability)
   - Whether they won (actual outcome)
4. Groups by price level (1-99 cents)
5. Calculates: |Actual win rate - Price| for each level
6. Takes the average = 7.88%

**Why Only 1 Data Point?**
- Threshold: Need 500+ positions for reliable calculation
- Your data: March 5 didn't have 500 positions yet
- March 6: Crossed the 500 threshold → 1 data point generated

## Summary

✅ **Volume Over Time:** Correct but limited (only 1 quarter)
✅ **Calibration:** Correctly calculated at 7.88% deviation
✅ **Interpretation:** Decent accuracy, markets are reasonably efficient
✅ **Data Quality:** Valid results, just need more time range for trends

**Bottom Line:** Your markets have about 8% prediction error on average. If you see a 60% market, it's actually closer to 52-68% likely to happen. This is normal for prediction markets.
