"""Tests for trailing-stop logic in PositionLifecycleManager.

Covers the peak-tracking + give-back close pattern wired to CONVEX
and COMMODITY templates at trailing_stop_pct=0.30.
"""

from __future__ import annotations

from prometheus.execution.options_strategy import TradeAction
from prometheus.execution.position_lifecycle import (
    PositionLifecycleManager,
    STRATEGY_TRAILING_STOPS,
)


def _pos(strategy: str, *, entry=1.0, current=1.0, qty=1):
    return {
        "strategy": strategy,
        "symbol": "BZ",
        "right": "C",
        "expiry": "20260825",
        "strike": 75.0,
        "quantity": qty,
        "entry_price": entry,
        "current_price": current,
    }


# ── Coverage ─────────────────────────────────────────────────────────


def test_trailing_stops_table_covers_convex_and_commodity():
    expected = {
        "convex.thematic_sector_put",
        "convex.vix_escalation_call",
        "convex.convergence_straddle",
        "commodity.crude_chokepoint_call",
        "commodity.natgas_supply_call",
        "commodity.gold_sanctions_call",
        "commodity.wheat_blacksea_call",
    }
    assert expected.issubset(set(STRATEGY_TRAILING_STOPS.keys()))


def test_legacy_strategies_not_in_trailing_table():
    """Trailing only applies to CONVEX + COMMODITY; legacy ones use the
    existing TP/SL machinery."""
    assert "protective_put" not in STRATEGY_TRAILING_STOPS
    assert "iron_condor" not in STRATEGY_TRAILING_STOPS
    assert "covered_call" not in STRATEGY_TRAILING_STOPS


# ── Behavior ─────────────────────────────────────────────────────────


def test_no_trailing_when_position_never_profitable():
    mgr = PositionLifecycleManager()
    # Position immediately drops 20%.
    pos = _pos("commodity.crude_chokepoint_call", entry=1.0, current=0.80)
    assert mgr.check_trailing_stops([pos]) == []


def test_trailing_tracks_peak_across_evaluations():
    mgr = PositionLifecycleManager()
    pos = _pos("commodity.crude_chokepoint_call", entry=1.0, current=1.0)
    # Day 1: at entry, no peak yet
    mgr.check_trailing_stops([pos])
    # Day 2: +150% (the profit_target_pct level)
    pos["current_price"] = 2.50
    out = mgr.check_trailing_stops([pos])
    assert out == []
    # Day 3: backed off to +100% (gave back 1/3 of peak gain of 1.5)
    # Peak was 1.50 (150%), trailing_stop=0.30, threshold = 1.50 * 0.70 = 1.05
    # Current PnL% = 1.00 (100%) < 1.05 → CLOSE
    pos["current_price"] = 2.00
    out = mgr.check_trailing_stops([pos])
    assert len(out) == 1
    assert out[0].action == TradeAction.CLOSE
    assert out[0].metadata["lifecycle"] == "trailing_stop"
    assert out[0].metadata["peak_pnl_pct"] > 1.49


def test_trailing_holds_when_giveback_under_threshold():
    mgr = PositionLifecycleManager()
    pos = _pos("commodity.gold_sanctions_call", entry=1.0, current=1.0)
    # Day 1
    mgr.check_trailing_stops([pos])
    # Day 2: peak at +200%
    pos["current_price"] = 3.00
    mgr.check_trailing_stops([pos])
    # Day 3: back to +180% — gave back 1/10 of peak of 2.0. Below
    # 30% trailing → still hold. Peak was 2.0, threshold = 2.0 * 0.70 = 1.40.
    # Current = 1.80 > 1.40 → no close.
    pos["current_price"] = 2.80
    out = mgr.check_trailing_stops([pos])
    assert out == []


def test_trailing_skips_strategies_not_in_table():
    mgr = PositionLifecycleManager()
    pos = _pos("protective_put", entry=1.0, current=2.50)
    pos["strategy"] = "protective_put"
    # Even with massive gain, no trailing fires for non-listed strategy.
    assert mgr.check_trailing_stops([pos]) == []


def test_trailing_short_position_signs_flipped():
    mgr = PositionLifecycleManager()
    # Short position: profit when price drops. qty=-1.
    pos = _pos("convex.convergence_straddle", entry=2.0, current=2.0, qty=-1)
    mgr.check_trailing_stops([pos])
    # Price drops to 0.50 → for short, that's (2.0 - 0.50) / 2.0 = +75% gain
    pos["current_price"] = 0.50
    mgr.check_trailing_stops([pos])
    # Price moves back up to 1.20 → (2.0 - 1.20) / 2.0 = +40% gain
    # Peak was 0.75, threshold = 0.75 * 0.70 = 0.525. Current 0.40 < 0.525 → CLOSE.
    pos["current_price"] = 1.20
    out = mgr.check_trailing_stops([pos])
    assert len(out) == 1
    assert out[0].action == TradeAction.CLOSE
    # Closing a short: directive quantity = -qty = +1
    assert out[0].quantity == 1


def test_trailing_separate_peaks_per_position():
    """Two different positions get independent peak tracking."""
    mgr = PositionLifecycleManager()
    pos_a = _pos("commodity.crude_chokepoint_call", entry=1.0, current=1.0)
    pos_b = _pos("commodity.crude_chokepoint_call", entry=1.0, current=1.0)
    pos_b["strike"] = 80.0   # different contract
    # A peaks at +200%
    pos_a["current_price"] = 3.00
    mgr.check_trailing_stops([pos_a, pos_b])
    # B never moves
    # A drops to +100% (well below threshold of 3.0 * 0.70 = 2.10)
    pos_a["current_price"] = 2.00
    out = mgr.check_trailing_stops([pos_a, pos_b])
    closes = [d for d in out if d.action == TradeAction.CLOSE]
    assert len(closes) == 1
    assert closes[0].strike == 75.0   # A closed, B untouched


def test_trailing_directive_metadata_records_peak_and_current():
    mgr = PositionLifecycleManager()
    pos = _pos("convex.vix_escalation_call", entry=1.0, current=1.0)
    mgr.check_trailing_stops([pos])
    pos["current_price"] = 4.00   # +300% peak
    mgr.check_trailing_stops([pos])
    pos["current_price"] = 2.50   # +150%; threshold = 3.0 * 0.70 = 2.10 → 1.50 < 2.10 → close
    out = mgr.check_trailing_stops([pos])
    assert len(out) == 1
    md = out[0].metadata
    assert md["lifecycle"] == "trailing_stop"
    assert abs(md["peak_pnl_pct"] - 3.0) < 1e-6
    assert abs(md["current_pnl_pct"] - 1.5) < 1e-6
    assert md["trailing_pct"] == 0.30
