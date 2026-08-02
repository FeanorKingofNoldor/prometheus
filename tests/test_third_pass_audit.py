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
import sys
import types
from datetime import date, datetime, timedelta
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import pytest

# ---------------------------------------------------------------------------
# HIGH #1: Timezone-aware catchup date check
# ---------------------------------------------------------------------------


class TestCatchupTimezoneAwareness:
    """Catch-up gating must be timezone-consistent (no naive date.today()).

    Under the lane scheduler, _maybe_morning_catchup no longer compares
    as_of_date against "today" at all: the hour/minute gate uses
    now_local(), and the candidate window derives from the caller's
    as_of_date anchor. These tests preserve the original intent — no
    naive-local/UTC date skew can misfire or suppress a catch-up.
    """

    def test_catchup_source_uses_now_local(self):
        """The catchup method must not call naive date.today() anywhere;
        its clock gate must come from now_local()."""
        from prometheus.orchestration.market_aware_daemon import MarketAwareDaemon

        source = inspect.getsource(MarketAwareDaemon._maybe_morning_catchup)
        # The old bug: `if as_of_date == date.today():`
        assert "date.today()" not in source, (
            "_maybe_morning_catchup must not use naive date.today()"
        )
        # The hour/minute gate is timezone-aware.
        assert "now_local()" in source, (
            "_maybe_morning_catchup should gate on now_local() for "
            "timezone consistency"
        )

    def test_catchup_candidates_anchor_on_as_of_date(self):
        """The candidate trading-day window derives from the caller's
        as_of_date (the daemon's UTC anchor), never from the host-local
        calendar date — timezone-safety successor of the old
        as_of_date==local-today early-return test."""
        from prometheus.orchestration.market_aware_daemon import (
            MarketAwareDaemon,
            MarketAwareDaemonConfig,
        )

        db = MagicMock()
        config = MarketAwareDaemonConfig(markets=["US_EQ"], morning_catchup_hour=8)
        daemon = MarketAwareDaemon(config, db)

        # Fake now_local returning 08:02 on 2026-04-12 in Berlin
        fake_now = datetime(2026, 4, 12, 8, 2, tzinfo=ZoneInfo("Europe/Berlin"))
        as_of_date = date(2026, 4, 12)

        mock_cal = MagicMock()
        mock_cal.trading_days_between.return_value = []  # no candidates
        daemon._calendars["US_EQ"] = mock_cal

        with patch(
            "prometheus.orchestration.market_aware_daemon.now_local",
            return_value=fake_now,
        ):
            daemon._maybe_morning_catchup(as_of_date)

        # Window is [as_of_date - 7d, as_of_date - 1d] — anchored on the
        # passed date, not any host-local "today".
        mock_cal.trading_days_between.assert_called_once_with(
            date(2026, 4, 5), date(2026, 4, 11),
        )
        # No candidates → nothing attached.
        assert daemon.lanes["US_EQ"].catchup is None

    def test_catchup_does_not_skip_when_dates_differ(self):
        """When the last trading day has no completed run, catch-up
        proceeds: it checks the run history and attaches a CatchupState
        to the market's lane (rather than looping inline as before)."""
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
        # "no candidates" check and into load_latest_run territory.
        mock_cal = MagicMock()
        mock_cal.trading_days_between.return_value = [date(2026, 4, 8)]
        daemon._calendars["US_EQ"] = mock_cal

        with patch(
            "prometheus.orchestration.market_aware_daemon.now_local",
            return_value=fake_now,
        ), patch(
            "prometheus.pipeline.state.load_latest_run",
            return_value=None,  # no completed run -> triggers catchup
        ) as mock_load:
            daemon._maybe_morning_catchup(as_of_date)

        # It consulted the run history for the missed day...
        mock_load.assert_called_once()
        # ...and queued a catch-up on the lane for that fixed past date.
        catchup = daemon.lanes["US_EQ"].catchup
        assert catchup is not None
        assert catchup.catchup_date == date(2026, 4, 8)


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

    class _StubContractQualificationError(Exception):
        pass

    mapper_mod.InstrumentMapper = _StubInstrumentMapper  # type: ignore[attr-defined]
    mapper_mod.ContractQualificationError = _StubContractQualificationError  # type: ignore[attr-defined]

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

    def test_catchup_done_pruned_after_120_entries(self):
        """_catchup_done is pruned >120 → keep the latest 60. (The bound
        doubled with the lane scheduler: keys are now per (market, date),
        so multi-market fleets legitimately hold more live entries.)"""
        from prometheus.orchestration.market_aware_daemon import (
            MarketAwareDaemon,
            MarketAwareDaemonConfig,
        )

        db = MagicMock()
        config = MarketAwareDaemonConfig(markets=["US_EQ"], morning_catchup_hour=8)
        daemon = MarketAwareDaemon(config, db)

        # Manually populate _catchup_done with 121 entries, including the
        # key for the day the method is about to consider (so the loop
        # no-ops and we reach the prune at the tail).
        yesterday = date(2026, 4, 10)
        daemon._catchup_done = {f"catchup_US_EQ_{yesterday}"}
        for i in range(120):
            d = date(2025, 1, 1) + timedelta(days=i)
            daemon._catchup_done.add(f"catchup_US_EQ_{d}")
        assert len(daemon._catchup_done) == 121

        mock_cal = MagicMock()
        mock_cal.trading_days_between.return_value = [yesterday]
        daemon._calendars["US_EQ"] = mock_cal

        with patch(
            "prometheus.orchestration.market_aware_daemon.now_local",
            return_value=datetime(2026, 4, 11, 8, 2),
        ):
            daemon._maybe_morning_catchup(date(2026, 4, 11))

        # Pruned down to the 60 most recent entries; the freshest key kept.
        assert len(daemon._catchup_done) == 60
        assert f"catchup_US_EQ_{yesterday}" in daemon._catchup_done
        assert f"catchup_US_EQ_{date(2025, 1, 1)}" not in daemon._catchup_done

    def test_zombie_reap_done_pruned_after_60_entries(self):
        from prometheus.orchestration.market_aware_daemon import MarketAwareDaemon

        source = inspect.getsource(MarketAwareDaemon._maybe_reap_zombie_runs)
        assert "len(self._zombie_reap_done) > 60" in source, (
            "_maybe_reap_zombie_runs should prune _zombie_reap_done when > 60 entries"
        )
        assert "sorted(self._zombie_reap_done)[-30:]" in source, (
            "_maybe_reap_zombie_runs should keep only the 30 most recent entries"
        )

    def test_catchup_done_prune_keeps_recent(self):
        """After pruning, only the 60 lexicographically latest entries remain."""
        # Simulate the prune logic directly (mirrors _maybe_morning_catchup)
        catchup_done = set()
        for i in range(125):
            d = date(2025, 1, 1) + timedelta(days=i)
            catchup_done.add(f"catchup_US_EQ_{d}")

        assert len(catchup_done) == 125

        # Apply the same prune as in the source
        if len(catchup_done) > 120:
            catchup_done = set(sorted(catchup_done)[-60:])

        assert len(catchup_done) == 60
        # The latest date should be in the set
        assert f"catchup_US_EQ_{date(2025, 1, 1) + timedelta(days=124)}" in catchup_done
        # The earliest date should have been pruned
        assert f"catchup_US_EQ_{date(2025, 1, 1)}" not in catchup_done


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
