"""Tests for OptionsStrategyManager._submit_directives leg handling.

Pins the 2026-07 audit fixes:

* leg failures are returned to the caller (threaded into the run
  summary's warnings by run_derivatives_daily) instead of being
  swallowed by logger.error;
* when a chained ``spread_leg`` fails AFTER its parent leg was
  submitted, the parent is cancelled (best-effort) so no naked short
  leg is left working overnight — and the cancel outcome is reported
  loudly either way;
* the submission recorder (strategy provenance) fires per successful
  order and its failures never masquerade as submission failures.
"""

from __future__ import annotations

from typing import Any, List, Optional

from prometheus.execution.broker_interface import BrokerInterface
from prometheus.execution.instrument_mapper import InstrumentMapper
from prometheus.execution.options_strategy import (
    OptionsStrategyManager,
    OptionTradeDirective,
    TradeAction,
)


class _ScriptedBroker(BrokerInterface):
    """Submits successfully unless the order's instrument_id contains a
    configured marker. Records submissions and cancels."""

    def __init__(self, *, fail_on: str = "", cancel_result: bool = True,
                 cancel_raises: bool = False) -> None:
        self.fail_on = fail_on
        self.cancel_result = cancel_result
        self.cancel_raises = cancel_raises
        self.submitted: List[Any] = []
        self.cancelled: List[str] = []
        self._next_id = 0

    def submit_order(self, order):
        if self.fail_on and self.fail_on in order.instrument_id:
            raise RuntimeError(f"no market for {order.instrument_id}")
        self._next_id += 1
        self.submitted.append(order)
        return f"OID-{self._next_id}"

    def cancel_order(self, order_id):
        if self.cancel_raises:
            raise RuntimeError("cancel rejected")
        self.cancelled.append(str(order_id))
        return self.cancel_result

    def get_positions(self):
        return {}

    def get_order_status(self, order_id):
        return None

    def get_account_state(self):
        return {}

    def get_fills(self, since=None):
        return []

    def sync(self):
        pass


def _mgr(broker: BrokerInterface,
         recorder: Optional[Any] = None) -> OptionsStrategyManager:
    return OptionsStrategyManager(
        broker=broker,
        mapper=InstrumentMapper.__new__(InstrumentMapper),
        strategies=[],
        submission_recorder=recorder,
    )


def _short_put_with_wing() -> OptionTradeDirective:
    """Iron-condor-style pair: short put (parent) + long put wing (child)."""
    short_put = OptionTradeDirective(
        strategy="iron_condor",
        action=TradeAction.OPEN,
        symbol="SPY", right="P", expiry="20260918",
        strike=500.0, quantity=-3, limit_price=4.20,
        reason="test short put",
    )
    wing = OptionTradeDirective(
        strategy="iron_condor",
        action=TradeAction.OPEN,
        symbol="SPY", right="P", expiry="20260918",
        strike=490.0, quantity=3, limit_price=2.10,
        reason="test long wing",
    )
    short_put.spread_leg = wing
    return short_put


def test_all_legs_submitted_no_failures():
    broker = _ScriptedBroker()
    failures = _mgr(broker)._submit_directives([_short_put_with_wing()])

    assert failures == []
    assert [o.instrument_id for o in broker.submitted] == [
        "SPY_260918_500P.US", "SPY_260918_490P.US",
    ]
    assert broker.cancelled == []


def test_failed_wing_cancels_parent_and_reports():
    broker = _ScriptedBroker(fail_on="490P")
    failures = _mgr(broker)._submit_directives([_short_put_with_wing()])

    # Parent (the naked short) was cancelled.
    assert broker.cancelled == ["OID-1"]
    # Both the leg failure and the cancel are reported to the caller.
    assert any("submit failed" in f and "490" in f for f in failures)
    assert any("NAKED-LEG GUARD" in f for f in failures)


def test_failed_wing_with_failed_cancel_screams_for_manual_intervention():
    broker = _ScriptedBroker(fail_on="490P", cancel_result=False)
    failures = _mgr(broker)._submit_directives([_short_put_with_wing()])

    assert any("NAKED-LEG RISK" in f and "MANUAL INTERVENTION" in f
               for f in failures)


def test_failed_wing_with_cancel_exception_is_reported_not_raised():
    broker = _ScriptedBroker(fail_on="490P", cancel_raises=True)
    failures = _mgr(broker)._submit_directives([_short_put_with_wing()])

    assert any("cancel rejected" in f for f in failures)
    assert any("MANUAL INTERVENTION" in f for f in failures)


def test_failed_parent_skips_wing_and_reports():
    broker = _ScriptedBroker(fail_on="500P")
    failures = _mgr(broker)._submit_directives([_short_put_with_wing()])

    # Nothing was submitted (the wing must not go out alone either).
    assert broker.submitted == []
    assert broker.cancelled == []
    assert any("submit failed" in f and "500" in f for f in failures)
    assert any("spread aborted" in f for f in failures)


def test_single_leg_failure_still_reported():
    broker = _ScriptedBroker(fail_on="SPY")
    lone = OptionTradeDirective(
        strategy="protective_put", action=TradeAction.OPEN,
        symbol="SPY", right="P", expiry="20260918",
        strike=450.0, quantity=1, limit_price=5.0,
    )
    failures = _mgr(broker)._submit_directives([lone])
    assert len(failures) == 1
    assert "submit failed" in failures[0]


def test_submission_recorder_called_per_successful_order():
    broker = _ScriptedBroker()
    calls: List[tuple] = []

    def _recorder(directive, instrument_id, order_id):
        calls.append((directive.strategy, instrument_id, order_id))

    failures = _mgr(broker, recorder=_recorder)._submit_directives(
        [_short_put_with_wing()],
    )
    assert failures == []
    assert calls == [
        ("iron_condor", "SPY_260918_500P.US", "OID-1"),
        ("iron_condor", "SPY_260918_490P.US", "OID-2"),
    ]


def test_submission_recorder_failure_does_not_fail_submission():
    broker = _ScriptedBroker()

    def _recorder(directive, instrument_id, order_id):
        raise RuntimeError("db down")

    failures = _mgr(broker, recorder=_recorder)._submit_directives(
        [_short_put_with_wing()],
    )
    # Orders went out; recorder failure is logged, not a submission failure.
    assert failures == []
    assert len(broker.submitted) == 2


def test_recorder_not_called_for_failed_leg():
    broker = _ScriptedBroker(fail_on="490P")
    calls: List[tuple] = []

    def _recorder(directive, instrument_id, order_id):
        calls.append((instrument_id, order_id))

    _mgr(broker, recorder=_recorder)._submit_directives(
        [_short_put_with_wing()],
    )
    assert calls == [("SPY_260918_500P.US", "OID-1")]
