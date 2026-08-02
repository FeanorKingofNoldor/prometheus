"""Unit tests for the options backtester's next-bar fill convention.

Proves that ``fill_mode="next_bar"`` (the honest default) prices and enters
an option against the POST-DECISION bar (t+1), while ``fill_mode="same_bar"``
reproduces the legacy look-ahead behaviour (fill at the signal bar t).

All data is mocked in-memory; no database is touched.
"""

from __future__ import annotations

from datetime import date

import pytest

from prometheus.backtest.option_pricer import bs_price, fill_price
from prometheus.backtest.options_backtest import (
    OptionsBacktestConfig,
    OptionsBacktestEngine,
)
from prometheus.execution.options_strategy import OptionTradeDirective, TradeAction


# ── Synthetic fixture ────────────────────────────────────────────────

# Three consecutive weekdays. The underlying price JUMPS between the
# decision bar (D1) and the fill bar (D2), so the option's Black-Scholes
# price is materially different on each bar — that price difference is how
# we distinguish a same-bar fill from a next-bar fill.
_D0 = date(2020, 1, 1)   # Wed — warmup / first bar
_D1 = date(2020, 1, 2)   # Thu — DECISION bar
_D2 = date(2020, 1, 3)   # Fri — FILL bar (t+1)

_SYMBOL = "SPY"
_PRICE_D0 = 300.0
_PRICE_D1 = 300.0   # decision bar underlying
_PRICE_D2 = 330.0   # fill bar underlying (a clear +10% jump)
_STRIKE = 300.0
_VIX = 20.0
_EXPIRY = "20200214"  # ~43 DTE from the decision bar


def _build_engine(fill_mode: str) -> OptionsBacktestEngine:
    """Build an engine with mocked price/VIX caches and no DataReader."""
    cfg = OptionsBacktestConfig(
        start_date=_D0,
        end_date=_D2,
        initial_nav=1_000_000.0,
        guardrails_enabled=False,
        fill_mode=fill_mode,
    )
    engine = OptionsBacktestEngine(cfg, data_reader=None)

    # Mock the underlying price + VIX caches directly (bypassing _preload_data).
    engine._price_cache = {
        _SYMBOL: {_D0: _PRICE_D0, _D1: _PRICE_D1, _D2: _PRICE_D2},
    }
    engine._vix_cache = {_D0: _VIX, _D1: _VIX, _D2: _VIX}
    # Flat equity NAV so total NAV is dominated by the options leg.
    engine._equity_nav = {
        _D0.isoformat(): 1_000_000.0,
        _D1.isoformat(): 1_000_000.0,
        _D2.isoformat(): 1_000_000.0,
    }
    return engine


def _one_open_directive() -> OptionTradeDirective:
    """A single deterministic long-put OPEN directive on a known contract."""
    return OptionTradeDirective(
        strategy="vix_tail_hedge",
        action=TradeAction.OPEN,
        symbol=_SYMBOL,
        right="P",
        expiry=_EXPIRY,
        strike=_STRIKE,
        quantity=1,
    )


def _price_on_bar(engine: OptionsBacktestEngine, fill_day: date, underlying: float) -> float:
    """Replicate _execute_directive's OPEN pricing for the test contract."""
    dte = max((date(2020, 2, 14) - fill_day).days, 1)
    rfr = engine._iv_engine.get_risk_free_rate(fill_day.year)
    iv = engine._iv_engine.get_iv(
        strike=_STRIKE, underlying_price=underlying, dte=dte, vix=_VIX,
        realized_vol_21d=0.0, symbol=_SYMBOL, right="P",
        term_structure=None,
    )
    mid = bs_price(underlying, _STRIKE, dte / 365.0, rfr, iv, "P")
    return fill_price(mid, underlying, _STRIKE, dte, True, _SYMBOL, engine._config.slippage_pct)


# ── Tests ─────────────────────────────────────────────────────────────

def _run_with_stub_open(engine: OptionsBacktestEngine):
    """Drive run() but force exactly one OPEN directive on the decision bar D1.

    The stub returns the OPEN only when invoked with as_of_date == D1, so the
    decision happens on D1 and (in next_bar mode) fills on D2.
    """
    def _stub_evaluate(as_of_date, signals, allocations, underlying_prices):
        if as_of_date == _D1:
            return [_one_open_directive()]
        return []

    engine._evaluate_strategies = _stub_evaluate  # type: ignore[assignment]
    # No lifecycle directives.
    return engine.run()


def test_next_bar_fills_at_post_decision_bar():
    """next_bar: decision on D1, but the position is priced at D2's underlying."""
    engine = _build_engine("next_bar")
    _run_with_stub_open(engine)

    positions = list(engine._book.positions.values())
    assert len(positions) == 1, "expected exactly one opened position"
    pos = positions[0]

    # Entered at the FILL bar D2, NOT the decision bar D1.
    assert pos.entry_date == _D2, (
        f"next_bar must enter at the post-decision bar {_D2}, got {pos.entry_date}"
    )

    expected_fill_price = _price_on_bar(engine, _D2, _PRICE_D2)
    expected_signal_price = _price_on_bar(engine, _D1, _PRICE_D1)

    # The two bars price very differently because the underlying jumped 300→330.
    assert abs(expected_fill_price - expected_signal_price) > 1.0, (
        "fixture sanity: the two bars must price differently"
    )
    assert pos.entry_price == pytest.approx(expected_fill_price, rel=1e-6), (
        "next_bar entry must use the fill bar (D2) underlying/IV, not the signal bar"
    )
    # And explicitly NOT the same-bar (D1) price.
    assert pos.entry_price != pytest.approx(expected_signal_price, rel=1e-6)


def test_same_bar_reproduces_legacy_signal_bar_fill():
    """same_bar: decision and fill both on D1 — the legacy look-ahead behaviour."""
    engine = _build_engine("same_bar")
    _run_with_stub_open(engine)

    positions = list(engine._book.positions.values())
    assert len(positions) == 1
    pos = positions[0]

    assert pos.entry_date == _D1, (
        f"same_bar must enter at the signal bar {_D1}, got {pos.entry_date}"
    )
    expected_signal_price = _price_on_bar(engine, _D1, _PRICE_D1)
    assert pos.entry_price == pytest.approx(expected_signal_price, rel=1e-6), (
        "same_bar entry must use the signal bar (D1) underlying/IV"
    )


def test_resolve_fill_bar_next_vs_same():
    """_resolve_fill_bar returns t+1 for next_bar and t for same_bar."""
    engine = _build_engine("next_bar")
    next_day_of = {_D0: _D1, _D1: _D2, _D2: None}

    # next_bar: D1 fills against D2's data
    fill_day, fill_md = engine._resolve_fill_bar(_D1, next_day_of, "next_bar")
    assert fill_day == _D2
    assert fill_md is not None
    assert fill_md["underlying_prices"][_SYMBOL] == _PRICE_D2

    # next_bar on the LAST bar has no next bar → no fill (entries skipped).
    fill_day_last, fill_md_last = engine._resolve_fill_bar(_D2, next_day_of, "next_bar")
    assert fill_md_last is None

    # same_bar: D1 fills against D1's own data
    fill_day_same, fill_md_same = engine._resolve_fill_bar(_D1, next_day_of, "same_bar")
    assert fill_day_same == _D1
    assert fill_md_same is not None
    assert fill_md_same["underlying_prices"][_SYMBOL] == _PRICE_D1
