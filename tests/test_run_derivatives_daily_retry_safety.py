"""Retry-safety tests for prometheus.scripts.run.run_derivatives_daily.

The daemon retries the whole script when ``result["errors"]`` is
non-empty, so the contract under test is:

* FATAL pre-submission failures (IBKR connect, ...) → ``errors``
  (retry is safe — nothing was submitted);
* anything that can fail at/after order submission → ``warnings``
  (never retried — a retry would re-submit orders);
* every option order carries a deterministic IBKR ``orderRef`` and
  submission is skipped when a non-terminal trade with the same ref is
  already working from a previous attempt.
"""

from __future__ import annotations

import re
from datetime import date
from types import SimpleNamespace
from typing import Any

import pytest

from prometheus.derivatives.order_refs import deterministic_option_order_ref
from prometheus.scripts.run import run_derivatives_daily as rdd

# ── Fake IB ──────────────────────────────────────────────────────────


class _FakeTicker:
    def __init__(self, px: float) -> None:
        self.last = px
        self.close = px
        self.bid = px
        self.ask = px


class _FakeIB:
    """Stands in for the ib_insync/ib_async ``IB`` client."""

    def __init__(self) -> None:
        self._open_trades: list = []
        self.placed: list = []

    # Connection / event loop
    def connect(self, host: str = "", port: int = 0,
                clientId: int = 0, timeout: int = 0) -> "_FakeIB":
        return self

    def disconnect(self) -> None:
        return None

    def sleep(self, secs: float = 0) -> None:
        return None

    # Account / positions
    def accountValues(self) -> list:
        return [
            SimpleNamespace(tag="NetLiquidation", value="1000000", currency="USD"),
            SimpleNamespace(tag="AvailableFunds", value="500000", currency="USD"),
        ]

    def positions(self) -> list:
        return []

    def portfolio(self) -> list:
        return []

    # Market data
    def reqMarketDataType(self, data_type: int) -> None:
        return None

    def qualifyContracts(self, *contracts: Any) -> list:
        out = []
        for c in contracts:
            c.conId = 1234
            out.append(c)
        return out

    def reqMktData(self, contract: Any, *args: Any, **kwargs: Any) -> _FakeTicker:
        return _FakeTicker(20.0)

    def cancelMktData(self, contract: Any) -> None:
        return None

    # Orders
    def trades(self) -> list:
        return list(self._open_trades)

    def placeOrder(self, contract: Any, order: Any) -> Any:
        order.orderId = len(self.placed) + 1
        trade = SimpleNamespace(
            contract=contract,
            order=order,
            orderStatus=SimpleNamespace(status="Submitted"),
        )
        self.placed.append(trade)
        self._open_trades.append(trade)
        return trade


class _ExplodingIB(_FakeIB):
    def connect(self, host: str = "", port: int = 0,
                clientId: int = 0, timeout: int = 0) -> "_FakeIB":
        raise ConnectionRefusedError("gateway down")


# ── Shared wiring ────────────────────────────────────────────────────

_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)


def _expected_ref(*, strategy: str = "protective_put",
                  strike: float = 450.0, side: str = "BUY") -> str:
    # Default port is 4002 (paper), so the run derives US_OPTIONS_PAPER
    # (portfolio/mode come from the gateway port, not from dry_run).
    return deterministic_option_order_ref(
        portfolio_id="US_OPTIONS_PAPER",
        strategy=strategy,
        underlying="SPY",
        right="P",
        expiry="20260918",
        strike=strike,
        side=side,
        as_of_date=date.today(),
    )


def _wire_live_run(monkeypatch: pytest.MonkeyPatch, fake_ib: _FakeIB) -> None:
    """Route run_derivatives_daily(dry_run=False) through the fake IB
    with exactly one approved directive (via the lifecycle manager)
    and no real DB access."""
    from prometheus.execution.options_strategy import (
        OptionsStrategyManager,
        OptionTradeDirective,
        TradeAction,
    )
    from prometheus.execution.position_lifecycle import PositionLifecycleManager

    monkeypatch.delenv("PROMETHEUS_DERIVATIVES_SHADOW", raising=False)
    monkeypatch.setattr("prometheus.execution.ib_compat.IB", lambda: fake_ib)
    monkeypatch.setattr(
        "apatheon.core.database.get_db_manager", lambda: SimpleNamespace(),
    )

    # No legacy-strategy directives — the single test directive comes
    # from the lifecycle manager and flows through risk checks + Step 8.
    monkeypatch.setattr(
        OptionsStrategyManager, "evaluate_all",
        lambda self, portfolio, signals, existing_options=None,
        allocations=None: [],
    )

    directive = OptionTradeDirective(
        strategy="protective_put",
        action=TradeAction.OPEN,
        symbol="SPY",
        right="P",
        expiry="20260918",
        strike=450.0,
        quantity=1,
        limit_price=5.0,
        reason="retry-safety test",
    )
    monkeypatch.setattr(
        PositionLifecycleManager, "evaluate",
        lambda self, positions, signals: [directive],
    )

    class _StubTracker:
        def __init__(self, db_manager: Any = None) -> None:
            return None

        def record_options_decision(self, **kwargs: Any) -> None:
            return None

    import prometheus.decisions.tracker as tracker_mod
    monkeypatch.setattr(tracker_mod, "DecisionTracker", _StubTracker)


# ── Deterministic order ref ──────────────────────────────────────────


def test_order_ref_stable_for_same_inputs():
    assert _expected_ref() == _expected_ref()
    assert _UUID_RE.match(_expected_ref())


def test_order_ref_changes_with_strike():
    assert _expected_ref(strike=450.0) != _expected_ref(strike=455.0)


def test_order_ref_changes_with_strategy_and_side():
    assert _expected_ref(strategy="protective_put") != \
        _expected_ref(strategy="sector_put_spread")
    assert _expected_ref(side="BUY") != _expected_ref(side="SELL")


def test_order_ref_changes_with_date():
    a = deterministic_option_order_ref(
        portfolio_id="P", strategy="s", underlying="SPY", right="P",
        expiry="20260918", strike=450.0, side="BUY",
        as_of_date=date(2026, 7, 2),
    )
    b = deterministic_option_order_ref(
        portfolio_id="P", strategy="s", underlying="SPY", right="P",
        expiry="20260918", strike=450.0, side="BUY",
        as_of_date=date(2026, 7, 3),
    )
    assert a != b


# ── Error taxonomy ───────────────────────────────────────────────────


def test_connect_failure_is_fatal_error(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr("prometheus.execution.ib_compat.IB", _ExplodingIB)

    result = rdd.run_derivatives_daily(dry_run=True)

    assert result["errors"]
    assert result["errors"][0].startswith("connect:")
    assert result["warnings"] == []
    assert "connect" not in result["steps_completed"]


def test_post_submission_step_failures_are_warnings(
    monkeypatch: pytest.MonkeyPatch,
):
    """Reconcile / shadow pass / diff report blowing up must land in
    ``warnings`` (empty ``errors``) so the daemon never retries — the
    pipeline itself completed through order submission."""
    monkeypatch.setenv("PROMETHEUS_DERIVATIVES_SHADOW", "1")
    monkeypatch.setattr("prometheus.execution.ib_compat.IB", _FakeIB)
    monkeypatch.setattr(
        "apatheon.core.database.get_db_manager", lambda: SimpleNamespace(),
    )

    def _boom(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("boom")

    monkeypatch.setattr(
        "prometheus.execution.options_storage.reconcile_positions", _boom,
    )
    monkeypatch.setattr(rdd, "_run_shadow_pass", _boom)
    monkeypatch.setattr(
        "prometheus.derivatives.diff_report.run_daily_diff", _boom,
    )

    result = rdd.run_derivatives_daily(dry_run=True)

    assert result["errors"] == []
    labels = {w.split(":", 1)[0] for w in result["warnings"]}
    assert {"reconcile", "shadow_pass", "diff_report"} <= labels
    assert "submit_orders" in result["steps_completed"]


def test_futures_roll_failure_is_warning(monkeypatch: pytest.MonkeyPatch):
    from prometheus.execution.futures_manager import FuturesManager

    monkeypatch.delenv("PROMETHEUS_DERIVATIVES_SHADOW", raising=False)
    monkeypatch.setattr("prometheus.execution.ib_compat.IB", _FakeIB)
    monkeypatch.setattr(
        "apatheon.core.database.get_db_manager", lambda: SimpleNamespace(),
    )

    def _boom(self: Any) -> None:
        raise RuntimeError("roll detection boom")

    monkeypatch.setattr(FuturesManager, "check_rolls", _boom)

    result = rdd.run_derivatives_daily(dry_run=True)

    assert result["errors"] == []
    assert any(w.startswith("futures_rolls:") for w in result["warnings"])
    assert "submit_orders" in result["steps_completed"]


def test_exception_after_submission_is_warning_not_error(
    monkeypatch: pytest.MonkeyPatch,
):
    """Once orders went to IBKR, a later crash (Step 9 here) must not
    surface in ``errors`` — a daemon retry would re-submit."""
    from prometheus.execution.options_portfolio import OptionsPortfolio

    fake_ib = _FakeIB()
    _wire_live_run(monkeypatch, fake_ib)

    def _boom_status(self: Any) -> None:
        raise RuntimeError("status boom")

    monkeypatch.setattr(OptionsPortfolio, "get_status", _boom_status)

    result = rdd.run_derivatives_daily(dry_run=False)

    assert len(fake_ib.placed) == 1  # the order really went out
    assert result["errors"] == []
    assert any(w.startswith("post_submission:") for w in result["warnings"])


def test_exception_before_submission_stays_error(
    monkeypatch: pytest.MonkeyPatch,
):
    """A crash before any order is placed keeps the retryable-error
    contract (here: risk checks blow up in Step 7)."""
    fake_ib = _FakeIB()
    _wire_live_run(monkeypatch, fake_ib)

    def _boom(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("risk boom")

    monkeypatch.setattr(rdd, "_apply_risk_checks", _boom)

    result = rdd.run_derivatives_daily(dry_run=False)

    assert fake_ib.placed == []
    assert result["errors"] == ["risk boom"]


# ── Deterministic refs + open-order skip in the submission path ─────


def test_submitted_order_carries_deterministic_ref(
    monkeypatch: pytest.MonkeyPatch,
):
    fake_ib = _FakeIB()
    _wire_live_run(monkeypatch, fake_ib)

    result = rdd.run_derivatives_daily(dry_run=False)

    assert result["errors"] == []
    assert len(fake_ib.placed) == 1
    assert fake_ib.placed[0].order.orderRef == _expected_ref()


def test_working_order_with_same_ref_skips_resubmission(
    monkeypatch: pytest.MonkeyPatch,
):
    fake_ib = _FakeIB()
    _wire_live_run(monkeypatch, fake_ib)
    # An order from a previous (crashed) attempt is still working.
    fake_ib._open_trades.append(SimpleNamespace(
        order=SimpleNamespace(orderRef=_expected_ref()),
        orderStatus=SimpleNamespace(status="Submitted"),
    ))

    result = rdd.run_derivatives_daily(dry_run=False)

    assert result["errors"] == []
    assert fake_ib.placed == []  # skipped — no double submission


def test_terminal_order_with_same_ref_does_not_block_submission(
    monkeypatch: pytest.MonkeyPatch,
):
    fake_ib = _FakeIB()
    _wire_live_run(monkeypatch, fake_ib)
    # A cancelled order from a previous attempt is terminal → resubmit.
    fake_ib._open_trades.append(SimpleNamespace(
        order=SimpleNamespace(orderRef=_expected_ref()),
        orderStatus=SimpleNamespace(status="Cancelled"),
    ))

    result = rdd.run_derivatives_daily(dry_run=False)

    assert result["errors"] == []
    assert len(fake_ib.placed) == 1
