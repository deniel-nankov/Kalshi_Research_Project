# Price impact on Kalshi — institutional summary

**Run id:** `price_impact_20260520T014216Z_080303dc`
**Window:** 2025-03-12 → 2026-05-18 (110 daily partitions, 464.1 M trades)
**Script:** `scripts/analyze_price_impact.py` `@1.3.0`
**Full report:** [`surveillance/reports/price_impact_20260520T014216Z_080303dc.md`](./price_impact_20260520T014216Z_080303dc.md)

Method — market-hash sharded (8) sufficient statistics; pre/post windows are
pure intra-market lag/lead expressions; subset-matched at-trade averages so
the Δ pre→trade columns are honest per-row means, not biased between-group
differences. No outcome columns enter the per-trade features; the
correctness split applies labels only as a downstream stratifier.

---

## Sample

| Metric | Value |
|---|---|
| Trades scanned | 464,136,621 |
| Impact-eligible (has prior same-market trade) | 446,995,451 |
| Resolved (`was_taker_correct` non-null) | 450,601,932 |
| Total contracts exchanged | 81,363,374,740 |
| Total taker-side notional | $32,298,795,758 |
| Mean trade size | 175.3 contracts |
| Yes-side taker share | 69.30% |
| No-side taker share | 30.70% |
| Taker-correct rate (resolved) | 45.43% |

Volume per side is identical by construction (every trade has 1 taker, 1
maker, same `count`). The economic asymmetry sits in the price each side
receives.

---

## 1. Combined — immediate price impact

| Metric | Value (¢) |
|---|---|
| Mean Δp_taker (signed, taker frame: positive = paid up vs. prior trade) | **+0.539** |
| Std Δp_taker | 3.872 |
| Mean Δp_yes (neutral) | +0.030 |
| Mean \|Δp_yes\| (magnitude) | **1.214** |

So on average a taker pays **~0.5 cents above the prior prevailing price**
in same-market dollars, with a typical absolute move of 1.2 cents per trade.

## Multi-horizon impact (combined, taker frame)

Δ pre→trade(N) = mean of `taker_price - avg_taker_pre_N` across all trades
with a full N-trade pre-window in the same market. Positive = taker paid
more than the running mean of the prior N trades.

| N | Mean pre-N taker ($) | At trade ($) | Mean post-N taker ($) | Δ pre→trade (¢) | Δ trade→post (¢) | Δ pre→post (¢) |
|---|---|---|---|---|---|---|
| 1 | 0.4692 | 0.4746 | 0.4712 | **+0.54** | -0.31 | +0.20 |
| 5 | 0.4682 | 0.4769 | 0.4734 | +0.88 | -0.44 | +0.53 |
| 10 | 0.4671 | 0.4773 | 0.4737 | +1.02 | -0.46 | +0.67 |
| 25 | 0.4658 | 0.4773 | 0.4738 | +1.15 | -0.46 | +0.80 |
| 50 | 0.4648 | 0.4767 | 0.4731 | +1.19 | -0.44 | +0.83 |
| 100 | 0.4637 | 0.4752 | 0.4717 | +1.15 | -0.42 | +0.80 |

Reading the curve:
- `Δ pre→trade` grows from 0.54¢ (N=1) to 1.15-1.19¢ by N≈25-50 and then
  flattens. Takers aggress on **momentum**: prices have already been
  trending in their direction over the prior 25-50 trades by ~1.2¢ before
  they pull the trigger.
- `Δ trade→post` is **mildly negative** (-0.3¢ to -0.5¢). The market drifts
  back against the taker by about a third of the up-move after the trade.
  Net of bid-ask, this reads as **partial mean reversion**.
- `Δ pre→post` is positive at ~0.8¢ — the longer-horizon drift is in the
  taker's direction, but smaller than the round-trip pre→trade move.

---

## 2. Split by `taker_side`

| taker_side | n_trades | % | Mean size | Total notional ($) | Mean taker_price ($) | Mean Δp_taker (¢) | Mean \|Δp_yes\| (¢) |
|---|---|---|---|---|---|---|---|
| yes | 321,668,313 | 69.30% | 187.4 | $22.19 B | 0.4295 | **+0.416** | 1.026 |
| no  | 142,468,308 | 30.70% | 148.0 | $10.11 B | 0.5414 | **+0.802** | 1.620 |

Key asymmetries:
- **YES takers** are more numerous (2.26 × NO takers) and trade **27%
  larger** on average (187 vs 148 contracts).
- **NO takers pay up almost 2 × more in cents** (+0.80¢ vs +0.42¢) and
  absolute price impact is **58% larger** (1.62¢ vs 1.03¢).
- Mean YES taker price is 0.43, mean NO taker price is 0.54. So NO
  takers on average buy at higher dollar prices (in their own NO frame)
  than YES takers, consistent with a market where NO-side aggression is
  typically the more decisive / informed side of a contested market.

Pre→trade horizon profile (taker frame):

| N | YES takers Δ pre→trade (¢) | NO takers Δ pre→trade (¢) |
|---|---|---|
| 1 | +0.42 | +0.80 |
| 10 | +0.77 | +1.57 |
| 25 | +0.87 | +1.77 |
| 100 | +0.86 | +1.76 |

NO takers pay up roughly **2 × what YES takers do** at every horizon.

---

## 3. Split by ex-post correctness (resolved trades only)

| was_taker_correct | n_trades | Mean size | Mean taker_price ($) | Mean Δp_taker (¢) | Mean \|Δp_yes\| (¢) |
|---|---|---|---|---|---|
| true (taker won) | 204,722,828 | 154.4 | **0.6473** | **+0.682** | 1.312 |
| false (taker lost) | 245,879,104 | 190.3 | **0.3135** | +0.415 | 1.136 |

Headline asymmetries:
- **Winning takers pay ~$0.65 / contract on average; losing takers pay
  ~$0.31.** Winners aggress when the market has already concentrated
  probability on the eventual outcome (paying the favorite price);
  losers are largely long-shot bettors who lose 100% of those payoffs.
- **Winning-taker trades are 19% smaller** (154 vs 190 contracts) — winners
  size down at the higher-probability prices; losers size up on cheaper
  picks.
- **Winning takers pay 65% more above the prior price in cents** (+0.68¢
  vs +0.41¢ for losers), consistent with informed urgency at the price
  spike.

Pre→trade horizon profile (taker frame):

| N | Correct takers Δ pre→trade (¢) | Incorrect takers Δ pre→trade (¢) |
|---|---|---|
| 1 | +0.68 | +0.41 |
| 5 | +1.14 | +0.65 |
| 10 | +1.41 | +0.69 |
| 25 | +1.82 | +0.57 |
| 50 | +2.21 | +0.29 |
| 100 | **+2.69** | -0.20 |

The trajectories diverge sharply:
- **Correct takers** show a **monotonically growing pre→trade impact**:
  they're not just paying the immediate up-tick — they're paying ~2.7¢
  above the 100-trade running mean. The market has been climbing **for
  100 trades** before they buy. Classic momentum chase, ex-post
  validated.
- **Incorrect takers** plateau and turn slightly negative at the 100-trade
  horizon (-0.2¢). They're buying flat-to-slightly-below the running
  mean — i.e. picking up cheap longshots that don't pay off.

Post-trade (Δ trade→post taker frame, in cents):

| N | Correct takers | Incorrect takers |
|---|---|---|
| 1 | -0.23 | -0.38 |
| 25 | -0.01 | -0.85 |
| 50 | +0.26 | -1.05 |
| 100 | **+0.61** | **-1.31** |

For incorrect takers, the market drifts **against them** even further
after they trade (-1.31¢ at N=100). For correct takers, the market drifts
**further in their direction** (+0.61¢). This is the **information content
of taker direction**: the trade itself is mildly predictive of subsequent
drift in both groups, but with opposite sign for the two ex-post
outcomes.

---

## 4. 2 × 2 — `taker_side` × ex-post correctness

| group | n_trades | Mean taker_price ($) | Mean trade size | Mean Δp_taker (¢) |
|---|---|---|---|---|
| yes / correct   | 130,716,012 | 0.6127 | 162.2 | +0.514 |
| yes / incorrect | 181,686,173 | 0.3030 | 202.8 | +0.343 |
| no / correct    | 74,006,816  | 0.7084 | 140.6 | **+0.973** |
| no / incorrect  | 64,192,931  | 0.3429 | 154.7 | +0.603 |

NO/correct trades dominate the impact magnitude: takers paying ~$0.71
per NO contract that paid off, with the largest immediate impact of all
four groups (+0.97¢). Far-out-of-the-money winning bets — when the NO
side closes against a YES favorite — are the largest momentum signals in
the data.

---

## 5. Maker vs taker volume

| Side | n_trades | Mean trade size | Total contracts | Total notional ($) | Mean price paid / received ($) |
|---|---|---|---|---|---|
| Taker (aggressor) | 464,136,621 | 175.3 | 81,363,374,740 | $32,298,795,758 | **0.4639** |
| Maker (resting)   | 464,136,621 | 175.3 | 81,363,374,740 | $32,298,795,758 | **0.5361** |

Volume per side is identical by construction. Two structural facts make
the maker side the economically favored leg:

1. **Mean maker receive price (0.5361) exceeds mean taker pay price
   (0.4639) by 7.2¢ per contract** — the bid-ask spread internalized by
   the maker on every fill (counting both sides of the book).
2. **Maker-correct rate = 1 − taker-correct rate = 54.6%** — makers win
   54.6% of the time vs takers' 45.4%. Whether that translates to PnL
   advantage depends on the joint distribution of size × price × outcome
   (the surveillance project's PnL gates would have to be applied to
   answer); on raw hit-rate alone, the maker side has the edge.

---

## What this changes for the project

1. **Replicates a textbook microstructure pattern** in a prediction-market
   substrate: takers pay up vs. running mean, market partially reverts
   post-trade. The magnitudes (≈1.2¢ typical impact, ≈0.5¢ signed mean,
   ≈30-40% post-trade reversion at N=100) are now characterized.
2. **Asymmetric taker direction** — NO takers pay nearly 2× the impact
   of YES takers. Future detectors on the surveillance branch should
   stratify by taker_side; treating the two sides symmetrically loses
   signal.
3. **Ex-post correctness signature** — correct takers display a clean
   100-trade pre-trade momentum curve (+2.7¢) and modest favorable
   post-trade drift (+0.6¢). Incorrect takers are roughly flat pre-trade
   and drift further against (-1.3¢). This is consistent with a small
   amount of taker-direction predictive content — useful as a model
   input, though never adequate alone given the asymmetric payoff
   structure.

---

## Reproducibility

```
Script           : scripts/analyze_price_impact.py
Version          : analyze_price_impact@1.3.0
Run id           : price_impact_20260520T014216Z_080303dc
Started          : 2026-05-20T01:42:16.055415+00:00
Elapsed          : 3052.8 s  (50.9 min)
Shards           : 8 (market-hash, lossless)
Source columns   : trade_id, market_ticker, created_time, taker_side,
                   yes_price_dollars, no_price_dollars, count, notional_usd,
                   was_taker_correct
Sufficient stats : sum, sum_sq, count per (group, metric); subset-matched
                   at-trade sums per N. Means and stds combined across
                   shards exactly from the partials.
Full report      : surveillance/reports/price_impact_20260520T014216Z_080303dc.md
Audit log        : data/kalshi/state/price_impact_runs/price_impact_20260520T014216Z_080303dc.json
```
