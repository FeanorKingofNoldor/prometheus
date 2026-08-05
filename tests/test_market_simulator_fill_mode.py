"""Fill-timing convention tests for :class:`MarketSimulator`.

These prove the look-ahead fix: a signal decided on ``close[t]`` must fill at
``open[t+1]`` (``next_open``, the honest default), while the legacy
``same_bar`` mode reproduces the old ``close[t]`` fill. The price series is
tiny and hand-computable so the two modes give provably different P&L. No DB.
"""

from __future__ import annotations

from datetime import date

import pandas as pd

from prometheus.execution.broker_interface import Order, OrderSide, OrderType
from prometheus.execution.market_simulator import FillConfig, MarketSimulator
from prometheus.execution.time_machine import TimeMachine

_PRICE_COLUMNS = [
    "instrument_id",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "adjusted_close",
    "volume",
    "currency",
    "metadata",
]

# Two trading days (a Mon/Tue) with distinct open vs close so same-bar and
# next-open are unambiguously different:
#   day1 (2020-01-06): open=100, close=110
#   day2 (2020-01-07): open=130, close=140
_ROWS = {
    "AAA": {
        date(2020, 1, 6): {"open": 100.0, "close": 110.0},
        date(2020, 1, 7): {"open": 130.0, "close": 140.0},
    }
}


class _FakeReader:
    """Minimal DataReader stand-in returning the synthetic series above."""

    def read_prices(self, instrument_ids, start_date, end_date):
        rows = []
        for inst in instrument_ids:
            for d, px in _ROWS.get(inst, {}).items():
                if start_date <= d <= end_date:
                    rows.append(
                        {
                            "instrument_id": inst,
                            "trade_date": d,
                            "open": px["open"],
                            "high": max(px["open"], px["close"]),
                            "low": min(px["open"], px["close"]),
                            "close": px["close"],
                            "adjusted_close": px["close"],
                            "volume": 1_000_000.0,
                            "currency": "USD",
                            "metadata": None,
                        }
                    )
        return pd.DataFrame(rows, columns=_PRICE_COLUMNS)


def _make_simulator(fill_mode: str) -> MarketSimulator:
    tm = TimeMachine(
        start_date=date(2020, 1, 6),
        end_date=date(2020, 1, 7),
        market="US_EQ",
        data_reader=_FakeReader(),
        strict_mode=True,
    )
    tm.set_date(date(2020, 1, 6))
    return MarketSimulator(
        time_machine=tm,
        initial_cash=1_000_000.0,
        fill_config=FillConfig(fill_mode=fill_mode),
    )


def _buy_order() -> Order:
    return Order(
        order_id="o1",
        instrument_id="AAA",
        side=OrderSide.BUY,
        quantity=10.0,
        order_type=OrderType.MARKET,
    )


def test_same_bar_fills_at_close_t():
    sim = _make_simulator("same_bar")
    fills = sim.simulate_fills(date(2020, 1, 6), [_buy_order()])
    assert len(fills) == 1
    # Legacy look-ahead: filled at close[t] = 110.
    assert fills[0].price == 110.0
    assert fills[0].timestamp.date() == date(2020, 1, 6)


def test_next_open_fills_at_open_t_plus_1():
    sim = _make_simulator("next_open")
    fills = sim.simulate_fills(date(2020, 1, 6), [_buy_order()])
    assert len(fills) == 1
    # Honest: a signal on close[t]=2020-01-06 fills at open[t+1] = 130.
    assert fills[0].price == 130.0
    assert fills[0].timestamp.date() == date(2020, 1, 7)


def test_modes_give_different_pnl():
    """The decided weight earns from the fill price onward, not the same bar.

    same_bar: buy at close[t]=110, mark at close[t]=110 -> unrealized = 0.
    next_open: buy at open[t+1]=130, mark at close[t+1]=140 -> unrealized = +10/sh.
    The two are provably different.
    """
    sb = _make_simulator("same_bar")
    sb.simulate_fills(date(2020, 1, 6), [_buy_order()])
    # Same-bar position marked at close[t]=110 == fill price -> no P&L yet.
    sb_pos = sb.get_positions(date(2020, 1, 6))["AAA"]
    assert sb_pos.avg_cost == 110.0
    assert sb_pos.unrealized_pnl == 0.0

    no = _make_simulator("next_open")
    no.simulate_fills(date(2020, 1, 6), [_buy_order()])
    # Next-open filled at open[t+1]=130; mark to close[t+1]=140 -> +10/sh.
    # Advance the sim clock to t+1 as the runner does before repricing.
    no.time_machine.set_date(date(2020, 1, 7))
    no_pos = no.get_positions(date(2020, 1, 7))["AAA"]
    assert no_pos.avg_cost == 130.0
    assert no_pos.unrealized_pnl == (140.0 - 130.0) * 10.0


def test_next_open_at_window_edge_leaves_unfilled():
    """A signal on the final bar has no next session, so it never fills."""
    sim = _make_simulator("next_open")
    fills = sim.simulate_fills(date(2020, 1, 7), [_buy_order()])
    assert fills == []


def test_default_fill_mode_is_next_open():
    assert FillConfig().fill_mode == "next_open"
