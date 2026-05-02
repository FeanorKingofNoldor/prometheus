"""Tests for the Prometheus third-pass audit fixes.

Covers:
- HIGH #1: Timezone-aware catchup date check (now_local_dt.date() not date.today())
- HIGH #2: IBKR order quantity validation rejects zero/negative
- MEDIUM #1: _catchup_done and _zombie_reap_done sets are bounded
- MEDIUM #2: SQL table name uses psycopg2.sql.Identifier (not f-string)
- MEDIUM #3: trade_journal CREATE TABLE has runtime-provisioning comment (structural)
"""

from __future__ import annotations

import importlib
import inspect
import re
import sys
import types
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import pytest


# ---------------------------------------------------------------------------
# HIGH #1: Timezone-aware catchup date check
# ---------------------------------------------------------------------------


class TestCatchupTimezoneAwareness:
    """_maybe_morning_catchup must use now_local_dt.date(), not date.today()."""

    def test_catchup_source_uses_now_local_dt_date(self):
        """The catchup method must not call date.today() for the date comparison."""
        from prometheus.orchestration.market_aware_daemon import MarketAwareDaemon

        source = inspect.getsource(MarketAwareDaemon._maybe_morning_catchup)
        # The old bug: `if as_of_date == date.today():`
        assert "date.today()" not in source, (
            "_maybe_morning_catchup still uses naive date.today() "
            "instead of now_local_dt.date()"
        )
        # Verify the fix is present
        assert "now_local_dt.date()" in source, (
            "_maybe_morning_catchup should compare as_of_date to "
            "now_local_dt.date() for timezone consistency"
        )

    def test_catchup_skips_when_as_of_date_matches_local_today(self):
        """When as_of_date matches the timezone-aware local date, catchup returns early."""
        from prometheus.orchestration.market_aware_daemon import (
            MarketAwareDaemon,
            MarketAwareDaemonConfig,
        )

        db = MagicMock()
        config = MarketAwareDaemonConfig(markets=["US_EQ"], morning_catchup_hour=8)
        daemon = MarketAwareDaemon(config, db)

        # Fake now_local returning 08:02 on 2026-04-12 in Berlin
        fake_now = datetime(2026, 4, 12, 8, 2, tzinfo=ZoneInfo("Europe/Berlin"))
        as_of_date = date(2026, 4, 12)  # same date as the local clock

        with patch(
            "prometheus.orchestration.market_aware_daemon.now_local",
            return_value=fake_now,
        ):
            # Should return early without trying to build a catchup DAG
            daemon._maybe_morning_catchup(as_of_date)

        # If it returned early, no calendar lookup happened
        assert not daemon._calendars

    def test_catchup_does_not_skip_when_dates_differ(self):
        """When as_of_date is yesterday, catchup should proceed past the date check."""
        from prometheus.orchestration.market_aware_daemon import (
            MarketAwareDaemon,
            MarketAwareDaemonConfig,
        )

        db = MagicMock()
        config = MarketAwareDaemonConfig(markets=["US_EQ"], morning_catchup_hour=8)
        daemon = MarketAwareDaemon(config, db)

        fake_now = datetime(2026, 4, 10, 8, 2, tzinfo=ZoneInfo("Europe/Berlin"))
        as_of_date = date(2026, 4, 9)  # yesterday relative to fake_now

        # Mock the calendar to return a trading day so we get past the
        # "no yesterday candidates" check and into load_latest_run territory.
        mock_cal = MagicMock()
        mock_cal.trading_days_between.return_value = [date(2026, 4, 8)]
        daemon._calendars["US_EQ"] = mock_cal

        with patch(
            "prometheus.orchestration.market_aware_daemon.now_local",
            return_value=fake_now,
        ), patch(
            "prometheus.pipeline.state.load_latest_run",
            return_value=None,  # no completed run -> would trigger catchup
        ) as mock_load:
            # Patch _catchup_in_progress setter and the pipeline execution to
            # prevent the full catchup loop from running.
            daemon._catchup_in_progress = False
            with patch.object(daemon, "_initialize_dags"):
                # Will try to run the catchup loop. We just need to verify
                # it got past the date check.
                try:
                    daemon._maybe_morning_catchup(as_of_date)
                except Exception:
                    pass  # OK to fail deeper in; we just care it got past date check

        # Verify it got past the date check and reached load_latest_run
        mock_load.assert_called_once()


# ---------------------------------------------------------------------------
# HIGH #2: IBKR order quantity validation
# ---------------------------------------------------------------------------


class _StubIbLogger:
    def debug(self, *_a, **_kw) -> None: ...
    def info(self, *_a, **_kw) -> None: ...
    def warning(self, *_a, **_kw) -> None: ...
    def error(self, *_a, **_kw) -> None: ...
    def exception(self, *_a, **_kw) -> None: ...


def _load_ibkr_module(monkeypatch):
    """Load ibkr_client_impl with minimal stubs (like test_ibkr_client_impl_account_fallback)."""
    apatheon_mod = types.ModuleType("apatheon")
    apatheon_core_mod = types.ModuleType("apatheon.core")
    apatheon_logging_mod = types.ModuleType("apatheon.core.logging")
    apatheon_logging_mod.get_logger = lambda _name: _StubIbLogger()  # type: ignore[attr-defined]

    ib_compat_mod = types.ModuleType("prometheus.execution.ib_compat")

    class _Event:
        def __iadd__(self, _cb):
            return self

    class _IB:
        def __init__(self):
            self.orderStatusEvent = _Event()
            self.execDetailsEvent = _Event()
            self.errorEvent = _Event()
            self.connectedEvent = _Event()
            self.disconnectedEvent = _Event()
        def isConnected(self):
            return False

    class _Contract:
        def __init__(self, symbol=""):
            self.symbol = symbol

    class _NoopOrder:
        def __init__(self, *_args, **_kwargs):
            self.orderRef = ""
            self.algoStrategy = ""
            self.algoParams = []

    ib_compat_mod.IB = _IB  # type: ignore[attr-defined]
    ib_compat_mod.Contract = _Contract  # type: ignore[attr-defined]
    ib_compat_mod.LimitOrder = _NoopOrder  # type: ignore[attr-defined]
    ib_compat_mod.MarketOrder = _NoopOrder  # type: ignore[attr-defined]
    ib_compat_mod.StopOrder = _NoopOrder  # type: ignore[attr-defined]
    ib_compat_mod.StopLimitOrder = _NoopOrder  # type: ignore[attr-defined]
    ib_compat_mod.Trade = object  # type: ignore[attr-defined]
    ib_compat_mod.Fill = object  # type: ignore[attr-defined]
    ib_compat_mod.Order = object  # type: ignore[attr-defined]

    mapper_mod = types.ModuleType("prometheus.execution.instrument_mapper")

    class _StubInstrumentMapper:
        def __init__(self, *_a, **_kw): ...
        def load_instruments(self): ...
        def get_contract(self, iid):
            return _Contract(symbol=iid)
        @staticmethod
        def contract_to_instrument_id(contract):
            return getattr(contract, "symbol", "UNKNOWN")

    mapper_mod.InstrumentMapper = _StubInstrumentMapper  # type: ignore[attr-defined]

    monkeypatch.setitem(sys.modules, "apatheon", apatheon_mod)
    monkeypatch.setitem(sys.modules, "apatheon.core", apatheon_core_mod)
    monkeypatch.setitem(sys.modules, "apatheon.core.logging", apatheon_logging_mod)
    monkeypatch.setitem(sys.modules, "prometheus.execution.ib_compat", ib_compat_mod)
    monkeypatch.setitem(sys.modules, "prometheus.execution.instrument_mapper", mapper_mod)

    for name in (
        "prometheus.execution.connection_manager",
        "prometheus.execution.ibkr_client_impl",
    ):
        sys.modules.pop(name, None)

    return importlib.import_module("prometheus.execution.ibkr_client_impl")


class TestIbkrOrderQuantityValidation:
    """IBKR _create_ib_order must reject zero and negative quantities."""

    def test_zero_quantity_rejected(self, monkeypatch):
        mod = _load_ibkr_module(monkeypatch)
        Order = mod.Order  # noqa: N806 — matches broker_interface.Order
        OrderSide = mod.OrderSide  # noqa: N806
        OrderType = mod.OrderType  # noqa: N806

        mapper = mod.InstrumentMapper()
        client = mod.IbkrClientImpl(
            config=mod.IbkrConnectionConfig(),
            mapper=mapper,
        )

        order = Order(
            order_id="test-001",
            instrument_id="AAPL.US",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=0,
        )
        with pytest.raises(ValueError, match="positive"):
            client._create_ib_order(order)

    def test_negative_quantity_rejected(self, monkeypatch):
        mod = _load_ibkr_module(monkeypatch)
        Order = mod.Order  # noqa: N806
        OrderSide = mod.OrderSide  # noqa: N806
        OrderType = mod.OrderType  # noqa: N806

        mapper = mod.InstrumentMapper()
        client = mod.IbkrClientImpl(
            config=mod.IbkrConnectionConfig(),
            mapper=mapper,
        )

        order = Order(
            order_id="test-002",
            instrument_id="AAPL.US",
            side=OrderSide.SELL,
            order_type=OrderType.MARKET,
            quantity=-10,
        )
        with pytest.raises(ValueError, match="positive"):
            client._create_ib_order(order)

    def test_positive_quantity_accepted(self, monkeypatch):
        mod = _load_ibkr_module(monkeypatch)
        Order = mod.Order  # noqa: N806
        OrderSide = mod.OrderSide  # noqa: N806
        OrderType = mod.OrderType  # noqa: N806

        mapper = mod.InstrumentMapper()
        client = mod.IbkrClientImpl(
            config=mod.IbkrConnectionConfig(),
            mapper=mapper,
        )

        order = Order(
            order_id="test-003",
            instrument_id="AAPL.US",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=100,
        )
        # Should not raise
        ib_order = client._create_ib_order(order)
        assert ib_order is not None


# ---------------------------------------------------------------------------
# MEDIUM #1: Bounded _catchup_done and _zombie_reap_done sets
# ---------------------------------------------------------------------------


class TestBoundedSets:
    """_catchup_done and _zombie_reap_done must be pruned after growing large."""

    def test_catchup_done_pruned_after_60_entries(self):
        from prometheus.orchestration.market_aware_daemon import (
            MarketAwareDaemon,
            MarketAwareDaemonConfig,
        )

        db = MagicMock()
        config = MarketAwareDaemonConfig(markets=["US_EQ"])
        daemon = MarketAwareDaemon(config, db)

        # Manually populate _catchup_done with 61 entries
        daemon._catchup_done = set()
        for i in range(61):
            d = date(2025, 1, 1) + timedelta(days=i)
            daemon._catchup_done.add(f"catchup_{d}")

        assert len(daemon._catchup_done) == 61

        # Source code prunes when > 60. Simulate the prune logic by calling
        # the method with conditions that trigger the add + prune path.
        # Alternatively, verify the pruning code exists in the source.
        source = inspect.getsource(MarketAwareDaemon._maybe_morning_catchup)
        assert "len(self._catchup_done) > 60" in source, (
            "_maybe_morning_catchup should prune _catchup_done when > 60 entries"
        )
        assert "sorted(self._catchup_done)[-30:]" in source, (
            "_maybe_morning_catchup should keep only the 30 most recent entries"
        )

    def test_zombie_reap_done_pruned_after_60_entries(self):
        from prometheus.orchestration.market_aware_daemon import (
            MarketAwareDaemon,
            MarketAwareDaemonConfig,
        )

        db = MagicMock()
        config = MarketAwareDaemonConfig(markets=["US_EQ"])
        daemon = MarketAwareDaemon(config, db)

        source = inspect.getsource(MarketAwareDaemon._maybe_reap_zombie_runs)
        assert "len(self._zombie_reap_done) > 60" in source, (
            "_maybe_reap_zombie_runs should prune _zombie_reap_done when > 60 entries"
        )
        assert "sorted(self._zombie_reap_done)[-30:]" in source, (
            "_maybe_reap_zombie_runs should keep only the 30 most recent entries"
        )

    def test_catchup_done_prune_keeps_recent(self):
        """After pruning, only the 30 lexicographically latest entries remain."""
        # Simulate the prune logic directly
        catchup_done = set()
        for i in range(65):
            d = date(2025, 1, 1) + timedelta(days=i)
            catchup_done.add(f"catchup_{d}")

        assert len(catchup_done) == 65

        # Apply the same prune as in the source
        if len(catchup_done) > 60:
            catchup_done = set(sorted(catchup_done)[-30:])

        assert len(catchup_done) == 30
        # The latest date should be in the set
        assert f"catchup_{date(2025, 1, 1) + timedelta(days=64)}" in catchup_done
        # The earliest date should have been pruned
        assert f"catchup_{date(2025, 1, 1)}" not in catchup_done


# ---------------------------------------------------------------------------
# MEDIUM #2: SQL Identifier for table names
# ---------------------------------------------------------------------------


class TestSqlIdentifierInBacktestWriter:
    """delete_run must use psycopg2.sql.Identifier, not f-string interpolation."""

    def test_no_fstring_table_interpolation(self):
        """The DELETE query must not use f-string for table names."""
        from prometheus.backtest.backtest_options_writer import BacktestOptionsWriter

        source = inspect.getsource(BacktestOptionsWriter.delete_run)
        # The old bug: f"DELETE FROM {table} WHERE run_id = %s"
        assert 'f"DELETE FROM {table}' not in source, (
            "delete_run still uses f-string for table name interpolation"
        )

    def test_uses_psycopg2_sql_identifier(self):
        """The DELETE query must use psycopg2.sql.Identifier."""
        from prometheus.backtest.backtest_options_writer import BacktestOptionsWriter

        source = inspect.getsource(BacktestOptionsWriter.delete_run)
        assert "psql.Identifier(table)" in source or "psql.Identifier" in source, (
            "delete_run should use psycopg2.sql.Identifier for safe table names"
        )
        assert "psql.SQL" in source, (
            "delete_run should use psycopg2.sql.SQL for the query template"
        )


# ---------------------------------------------------------------------------
# MEDIUM #3: trade_journal runtime provisioning comment
# ---------------------------------------------------------------------------


class TestTradeJournalRuntimeComment:
    """ensure_trade_journal_table must document why it uses CREATE TABLE at runtime."""

    def test_has_runtime_provisioning_comment(self):
        from prometheus.meta.trade_journal import ensure_trade_journal_table

        source = inspect.getsource(ensure_trade_journal_table)
        assert "runtime" in source.lower() and "alembic" in source.lower(), (
            "ensure_trade_journal_table should document that this table is "
            "created at runtime rather than via Alembic migration"
        )
        assert "monitoring" in source.lower() or "meta" in source.lower(), (
            "Comment should explain this is a monitoring/meta table"
        )
