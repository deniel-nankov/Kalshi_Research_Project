# Analysis Guide: What Each Statistic Means

A simple explanation of all 19 analyses and what they measure.

## 1. Meta Stats

**What it measures:** Overall dataset statistics - how much trading activity exists.

**What we learn:** The scale of the market. Shows total trades (1,516), total volume (\$75,818), number of markets (2,000), and how many had actual trading activity (61 markets = 3%). This gives the big picture of market liquidity and participation.

**Why it matters:** Tells you if you have enough data to draw meaningful conclusions. Small datasets mean less reliable patterns.

## 2. Market Types

**What it measures:** Distribution of trading volume across different market categories (Crypto, Politics, Sports, etc.).

**What we learn:** Which topics attract the most betting activity. For example, if Crypto has 99% of volume, that tells you where traders are most interested. Helps identify which markets are most liquid and actively traded.

**Why it matters:** Focus analysis on categories with enough volume. Categories with low activity may have unreliable prices due to low liquidity.

## 3. Trade Size by Role

**What it measures:** How big are positions when you're a maker (limit order) vs taker (market order)?

**What we learn:** Whether informed traders use different position sizes than casual traders. Makers typically place limit orders and wait, while takers hit existing orders immediately. If makers consistently trade larger sizes, it suggests they're more confident/informed.

**Why it matters:** Position size can signal conviction. Larger trades by makers might indicate they have better information or stronger opinions about market outcomes.

## 4. Maker vs Taker Returns

**What it measures:** Who makes more money - the patient limit order traders (makers) or the aggressive market order traders (takers)?

**What we learn:** Whether being patient pays off. Makers get better prices because they wait, but takers get immediacy. This analysis shows if the price improvement from waiting outweighs the risk of markets moving against you.

**Why it matters:** Helps decide trading strategy. If makers consistently outperform by 2-3%, it's worth placing limit orders and waiting rather than hitting the market immediately.

## 5. Maker Win Rate by Direction

**What it measures:** Do maker orders win more often when betting YES vs betting NO?

**What we learn:** Whether there's a systematic difference in market accuracy between bullish (YES) and bearish (NO) bets. If YES bets win 30% when priced at 30% but NO bets only win 25% when priced at 30%, that's a directional bias.

**Why it matters:** Identifies if the market systematically overprices or underprices one direction. Could reveal opportunities to consistently bet the undervalued side.

## 6. Maker Returns by Direction

**What it measures:** Profitability of YES bets vs NO bets at each price level for makers.

**What we learn:** Whether you make more money betting on events to happen (YES) or not happen (NO). For example, at 70% prices, do YES bets or NO bets have better excess returns?

**Why it matters:** Shows which side of the market is more profitable to trade. If NO bets consistently outperform, it suggests the market overestimates probabilities (events happen less often than prices suggest).

## 7. Mispricing by Price

**What it measures:** How far off are market prices from reality at different probability levels?

**What we learn:** Where the market makes the biggest mistakes. If prices at 80% only resolve YES 72% of the time, that's 8 percentage points of mispricing. Shows which price ranges are most accurate vs most biased.

**Why it matters:** Find the sweet spots for profitable trading. If low-probability events (10-20%) are consistently underpriced, you can profit by betting on longshots.

## 8. Win Rate by Price

**What it measures:** Classic calibration curve - do things priced at 70% actually happen 70% of the time?

**What we learn:** Overall market accuracy. Perfect calibration means a 70% price → 70% win rate. Deviations show systematic errors in crowd wisdom. Markets might be over-confident (prices too extreme) or under-confident (prices too moderate).

**Why it matters:** The fundamental test of prediction market efficiency. Poor calibration means exploitable opportunities. Good calibration means the crowd is collectively smart.

## 9. Yes vs No by Price

**What it measures:** Distribution of trading activity between YES and NO sides at each price level.

**What we learn:** Trading patterns and liquidity balance. Do people prefer betting on things happening (YES) or not happening (NO)? At 20% prices, are more people betting YES (longshot) or NO (favorite)?

**Why it matters:** Identifies liquidity imbalances. If everyone wants to bet YES on longshots, NO side might offer better prices due to supply/demand. Shows behavioral biases in how people prefer to bet.

## 10. Volume Over Time

**What it measures:** How much trading volume occurs each quarter (or month/week).

**What we learn:** Growth or decline in market activity. Seasonal patterns. Whether markets are getting more or less popular. Helps identify if the market is maturing or dying.

**Why it matters:** Growing volume means increasing liquidity and tighter spreads. Declining volume might mean you can't execute large trades without moving prices. Timing matters for strategy deployment.

## 11. Returns by Hour

**What it measures:** Profitability of trades at different hours of the day (Eastern Time).

**What we learn:** When are the best times to trade? Markets might be more efficient during business hours (more participants) and less efficient late at night (fewer informed traders). Shows intraday patterns in market quality.

**Why it matters:** Time your trades strategically. If 3 PM shows consistent mispricing, focus trading activity then. Avoid hours where spreads are wide or prices are unreliable.

## 12. VWAP by Hour

**What it measures:** Volume-weighted average price throughout the day.

**What we learn:** When do people trade at higher or lower prices? Shows intraday price patterns. Markets might open high and drift lower, or vice versa. Reveals whether specific hours have price momentum.

**Why it matters:** Identify optimal execution times. If prices tend to be lower in morning, place orders then. Understanding intraday patterns helps with order timing and fills.

## 13. Maker Taker Gap Over Time

**What it measures:** How much makers outperform takers each quarter (or month).

**What we learn:** Whether the maker advantage is consistent over time or just a fluke from one period. Stable gaps suggest persistent market structure. Changing gaps indicate evolving market efficiency or participant sophistication.

**Why it matters:** Validates whether maker strategies are reliably profitable or time-dependent. A 2% gap that persists across quarters is a robust edge. A gap that varies wildly means the strategy is unreliable.

## 14. Maker Taker Returns by Category

**What it measures:** Maker vs taker performance separately for each market category (Crypto, Politics, Sports, etc.).

**What we learn:** Which market types reward patience (maker orders) vs speed (taker orders)? Crypto markets might favor aggressive takers if prices move fast. Political markets might favor patient makers if prices are more stable.

**Why it matters:** Customize strategy by market type. Use maker orders in slow-moving politics markets, taker orders in volatile crypto markets. One size doesn't fit all categories.

## 15. Longshot Volume Share Over Time

**What it measures:** What percentage of trading volume goes into low-probability events (longshots, typically \<20%) over time.

**What we learn:** Whether people are chasing lottery-ticket outcomes more or less over time. High longshot volume suggests retail/casual traders dominating. Low longshot volume suggests sophisticated traders focusing on likely outcomes.

**Why it matters:** Longshot bias (overpricing low-probability events) is a well-known market inefficiency. Tracking this over time shows if the bias is getting worse (more exploitation opportunity) or better (market maturing).

## 16. Kalshi Calibration Deviation Over Time

**What it measures:** How far off market prices are from reality, measured over time (cumulative).

**What we learn:** Is the market getting better or worse at predictions? Improving calibration (decreasing deviation) means the crowd is learning. Worsening calibration suggests market quality degradation or changing participant mix.

**Why it matters:** Market efficiency matters for everyone. Traders need accurate prices to make money. Researchers need good calibration to trust the market's predictions. This is the health scorecard.

## 17. Statistical Tests

**What it measures:** Runs rigorous significance tests on key market patterns to separate real effects from random noise.

**What we learn:** Which findings are statistically meaningful vs. coincidental. Tests include: Do makers really trade larger sizes? Are YES/NO returns actually different? Do categories genuinely differ? Provides confidence levels (p-values).

**Why it matters:** Prevents false conclusions. A pattern that looks profitable might just be luck with small data. Statistical tests tell you what's real and actionable vs. what's random variation.

## 18. EV Yes vs No

**What it measures:** Expected value (profit) of betting YES vs betting NO at each price level.

**What we learn:** Which side of each price offers better risk-adjusted returns. At 30%, is it more profitable to bet YES (event happens) or NO (event doesn't happen)? Shows where directional biases exist.

**Why it matters:** Direct trading signal. If NO consistently has higher EV at most price levels, you should systematically bet NO (fade the crowd's optimism). This is the "where's the edge" map.

## 19. Win Rate by Trade Size

**What it measures:** Do bigger bets win more often than smaller bets?

**What we learn:** Whether position size signals information or conviction. If large trades win 55% but small trades win 48%, it suggests big traders know something. If no difference, then size doesn't predict success.

**Why it matters:** Follow the smart money. If large trades outperform, consider copying their direction. If large trades underperform, fade them (bet the opposite). Size can be a signal of informed trading.

## Summary: What Are We Trying to Learn Overall?

Across all 19 analyses, we're answering fundamental questions:

1.  **Market Efficiency:** Are prices accurate? (calibration, mispricing)
2.  **Trading Strategy:** What works? Maker vs taker, YES vs NO, timing (returns by hour, gap over time)
3.  **Market Patterns:** Where are the biases? (longshot bias, directional asymmetries)
4.  **Market Quality:** Is liquidity good? Is it getting better? (volume over time, calibration over time)
5.  **Smart Money:** Who wins? What do they do differently? (trade size signals, maker/taker differences)
