# Why Sector Put Spreads Underperform SH.US — Root Cause Analysis

## Summary

The sector-specific put strategy generates less ROI than SH.US not because
granularity is wrong in principle, but due to 5 implementation issues that
systematically handicap the sector approach.

## Issue 1: The "Dead Zone" Between Thresholds (CRITICAL)

The sector put spread strategy only activates when `0.15 < SHI < 0.25`.

```
SHI ≥ 0.25 → CLOSE hedges (sector "healthy")
0.15 < SHI < 0.25 → OPEN put spread
SHI ≤ 0.15 → SKIP ("allocator handles liquidation")
```

**Problem**: This is an extremely narrow activation window (10-point range on
a 0-100 scale). Sectors can go from 0.30 to 0.10 in a single bad day,
completely skipping the hedge window. When a sector crashes fast enough to
trigger the kill_threshold (0.15), the put spread never opens — the allocator
just liquidates the equity position. The hedge is most valuable exactly in
this fast-crash scenario, but it's never activated.

**SH.US comparison**: SH.US activates based on the FRAGILITY signal with a
smooth allocation ramp, not a narrow window. It starts hedging early and
scales up gradually.

**Fix**: Remove the `shi_kill_threshold` floor. If SHI < 0.25, always open
the put spread regardless of how low it goes. The put spread provides
payoff on further downside even after the allocator liquidates equities.

## Issue 2: Put Spread Max Size Cap is Too Low (HIGH)

```python
max_nav_pct: float = 0.01  # Cap 1% of NAV per sector hedge
```

With 11 GICS sectors, the maximum total hedge allocation via sector puts is
~11% of NAV. Compare to SH.US which can allocate up to 100% of NAV in a
SYSTEMIC_CRISIS. The sector puts are capped at an order of magnitude less
hedge notional.

Even during a GFC-style crash where ALL sectors are deteriorating, the put
spreads can only provide 11% × leverage × sector drop payoff, while SH.US
can provide 100% × (-1x SPY) payoff.

**Fix**: Increase `max_nav_pct` to 0.03-0.05 per sector, and add a total
portfolio hedge cap (e.g., 30% of NAV across all sector puts combined).

## Issue 3: Spread Width Too Narrow → Limited Payoff (HIGH)

```python
spread_width_pct: float = 0.07  # 7% between long & short strikes
```

A 7% put spread on XLK ($150) gives:
- Long put @ $150
- Short put @ $139.50
- Max payoff: $10.50 per share = $1,050 per contract

If the sector drops 20%, you only capture 7% of that drop (the spread
width). Meanwhile SH.US captures ~20% × allocation weight. The spread
structure gives up most of the tail payoff to the short leg.

**Fix**: Either widen to 15-20% spread, or use outright protective puts
(no short leg) when SHI is very low (< 0.20). The short leg saves
premium but kills the tail hedge.

## Issue 4: ATM Long Strike Means Expensive Premium (MEDIUM)

```python
long_strike = round(etf_price, 0)  # ATM
```

ATM puts are the most expensive part of the vol surface. The strategy pays
full ATM premium for the long leg while selling cheaper OTM premium on the
short leg. This creates a high breakeven — the sector needs to drop at
least 2-3% just to break even on the spread cost.

**Comparison**: SH.US has zero premium cost — you just buy the inverse ETF.
The sector puts need to earn back their premium before generating any
hedge value.

**Fix**: Use slightly OTM long strikes (2-5% OTM) to reduce premium.
The first 2-5% of downside can be accepted as unhedged (the allocator
already reduces equity exposure for those sectors).

## Issue 5: Hedge Timing Lag — Sector Health Signals Are Slow (MEDIUM)

The SHI uses:
- SMA200 (200-day trend — extremely lagging)
- 21d/63d/126d momentum (1-6 month lookback)
- 252d vol percentile (1-year lookback)
- FRED macro data (1-7 day lag)

These are all **backward-looking**. By the time SHI drops below 0.25, the
sector has already declined significantly. The put spread is opened
AFTER the drop, when implied volatility is elevated and puts are expensive.

**SH.US comparison**: SH.US sizing is also driven by backward signals, but
its "premium" is just the ETF price — it doesn't suffer from IV expansion
making the hedge more expensive.

**Fix**: Add forward-looking signals to the SHI:
- Options implied volatility skew (IV put > IV call → sector stress)
- Credit default swap spreads for sector leaders
- ETF fund flows (outflows → sector rotation)
These can trigger hedges 1-2 weeks before the price-based signals confirm.

## Backtest Results (2007-2024)

The SECTOR ALLOCATOR (equity weight adjustment + SH.US hedge) works
excellently during crashes:
- GFC: SPY -56.4%, Sector system **+8.6%**
- COVID: SPY -34.1%, Sector system **+11.3%**
- 2022: SPY -25.4%, Sector system **+4.2%**

The sector-level equity adjustment (kill sick sectors, reduce weak, hedge
with SH.US) is the winning component — not the put spreads specifically.

## Updated Findings After Backtesting

**The sector allocator EQUITY adjustment outperforms.** The issue was never
the sector-specific approach per se — it's that put spreads as the hedge
instrument have negative expected value in bull markets due to premium
bleed. SH.US also decays (-3%/year from daily reset), but less than put
premium costs.

**Key insight:** The sector health scoring system is the VALUE — it
correctly identifies which sectors to exit. The HEDGE INSTRUMENT choice
(puts vs SH.US) is secondary. The sector allocator + SH.US combination
is already the best approach.

## Implemented Fixes

1. Added 5-day fast momentum to SHI (triggers 1-2 weeks earlier in crashes)
2. Widened put strategy config (activation, sizing, spread width, OTM strikes)
3. Removed dead zone floor in put activation

## Recommendation

Keep the sector allocator + SH.US as the primary approach. Use sector puts
as a SUPPLEMENTARY tail hedge (not a replacement for SH.US) — sized at
3% NAV per sector for targeted exposure on the worst-performing sectors.
The sector health scoring improvements (fast momentum) will help both
the equity adjustment AND the put timing.
