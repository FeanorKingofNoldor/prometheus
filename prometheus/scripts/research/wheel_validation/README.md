# Wheel strategy validation campaign (2026-08-02/03)

Evidence base for the core+wheel strategy decision. ~280 backtests, 1998-01-02..2026-07-31.

Chronology and verdicts:
1. `run_core_wheel_campaign.py` + C++ `core_wheel_backtest` engine (prometheus_v2/cpp):
   mechanized dip-buying REJECTED (Sharpe <=0.58, DD -66%, survivorship-flattered).
   Fragility dial as exposure timer REJECTED (reactive; every curve cut return more than risk).
2. `wheel_sim.py`: SPY wheel (CSP 2% OTM / CC 8% OTM, 30d) beats SPY buy-and-hold on
   Sharpe + drawdown across eras. Wide calls essential.
3. `dip_wheel_sim.py`: dip-entries + wheel — direction robust (16/16 cells improved) but level
   is an option-pricing artifact: CAGR 23.7%/13%/3% at IV factor 1.1/0.95/0.80.
   Fill quality IS the strategy -> SPY core + mega-cap-only satellite.
   Also found EODHD backfill corruption in 32 tickers (script trims; ingestion fix TODO).
4. `wheel_sim_managed.py`: management folklore tested. PT50 = wash (friction >= theta saved,
   helps only high-vol chop). 7-DTE-exit = destructive (avoiding assignment amputates the
   wheel: put assignment IS the entry, call assignment IS the trim). Hold-to-expiry wins.
   Also fixes Sharpe inflation (daily BS marking instead of intrinsic).
5. `wheel_sim_v2.py`: adaptive variants. ROBUST WINNER: VIXCOND (VIX>25 -> puts 5% OTM +
   PT50 on; VIX<13 -> skip the cheap covered call) — beats baseline on CAGR+Sharpe at both
   IV factors, DD ~2.5pp shallower; edge concentrated 2020-2026, thin activation sample
   (24-40 in 28y). LADDER3 Sharpe-robust but CAGR drag from lot rounding. DTE45 loses.

Caveats throughout: r=0 (no collateral yield — understates wheel), VIX-proxied flat IV
surface (no skew), survivorship-biased universe in dip variants, European exercise.
Adopted spec: see configs/ + the wheel book implementation (task: core+wheel cutover).
