"""Prometheus v2 - TimeMachine for backtesting data access.

This module implements the TimeMachine abstraction used by the
backtesting infrastructure to enforce time-gated data access and prevent
look-ahead bias.

Key responsibilities:
- Track the current backtest date within a configured range.
- Provide iteration over trading days using the TradingCalendar.
- Serve time-gated access to historical price data via DataReader.

External dependencies:
- pandas: For tabular data handling.

Database tables accessed (historical_db, via DataReader):
- prices_daily

Thread safety: Not thread-safe. Intended for single-threaded
backtesting loops. Create one instance per backtest run.

Author: Prometheus Team
Created: 2025-11-24
Last Modified: 2025-11-24
Status: Development
Version: v0.3.0
"""

from __future__ import annotations

# ============================================================================
# Imports
# ============================================================================
from datetime import date, timedelta
from typing import Any, Iterator, Mapping

import pandas as pd
from apatheon.core.logging import get_logger
from apatheon.core.time import US_EQ, TradingCalendar, TradingCalendarConfig
from apatheon.data.reader import DataReader

# ============================================================================
# Module setup
# ============================================================================

logger = get_logger(__name__)


class TimeMachine:
    """Time-travel data access helper for backtesting.

    The TimeMachine tracks a current backtest date within a fixed
    [start_date, end_date] window and exposes:

    - :meth:`set_date` to move the simulation date.
    - :meth:`iter_trading_days` to iterate over trading days.
    - :meth:`get_data` to read time-gated historical data.

    For Iteration 3 we implement support for the ``prices_daily`` table
    via :class:`prometheus.data.reader.DataReader`. Additional tables
    (returns, correlation panels, etc.) can be added in later
    iterations.
    """

    def __init__(
        self,
        start_date: date,
        end_date: date,
        market: str = US_EQ,
        data_reader: DataReader | None = None,
        calendar: TradingCalendar | None = None,
        strict_mode: bool = False,
    ) -> None:
        """Initialise the TimeMachine.

        Args:
            start_date: First date of the backtest window (inclusive).
            end_date: Last date of the backtest window (inclusive).
            market: Market identifier, used to pick a trading calendar.
            data_reader: Optional :class:`DataReader` for historical
                prices. If omitted, :meth:`get_data` for ``prices_daily``
                will raise until a reader is assigned.
            calendar: Optional :class:`TradingCalendar` instance. If
                omitted, a default calendar for the given ``market`` is
                constructed.
            strict_mode: When ``True``, attempts to request data beyond
                the current date raise an error. When ``False`` (the
                default for Iteration 3), future rows are simply
                filtered out from the returned data.
        """

        if end_date < start_date:
            raise ValueError("end_date must be >= start_date")

        self._start_date = start_date
        self._end_date = end_date
        self._market = market
        self._calendar = calendar or TradingCalendar(
            TradingCalendarConfig(market=market)
        )
        self._data_reader = data_reader
        self._strict_mode = strict_mode

        self._current_date: date = start_date

        logger.info(
            "TimeMachine initialised for market=%s, start=%s, end=%s, strict_mode=%s",
            self._market,
            self._start_date,
            self._end_date,
            self._strict_mode,
        )

    # ==================================================================
    # Date management
    # ==================================================================

    @property
    def current_date(self) -> date:
        """Return the current simulation date."""

        return self._current_date

    def set_date(self, as_of_date: date) -> None:
        """Set the current simulation date.

        Args:
            as_of_date: New simulation date. Must lie within the
                configured [start_date, end_date] window.

        Raises:
            ValueError: If ``as_of_date`` lies outside the configured
                window.
        """

        if as_of_date < self._start_date or as_of_date > self._end_date:
            raise ValueError(
                "as_of_date %s outside TimeMachine window [%s, %s]"
                % (as_of_date, self._start_date, self._end_date)
            )

        self._current_date = as_of_date

    def iter_trading_days(self) -> Iterator[date]:
        """Yield all trading days between start and end date.

        Uses the configured :class:`TradingCalendar` to skip weekends and
        holidays.
        """

        current = self._start_date
        while current <= self._end_date:
            if self._calendar.is_trading_day(current):
                yield current
            current = current + timedelta(days=1)

    def advance_to_next_trading_day(self) -> date | None:
        """Advance ``current_date`` to the next trading day.

        Returns ``None`` if already past the final trading day.
        """

        next_date = self._current_date + timedelta(days=1)
        while next_date <= self._end_date:
            if self._calendar.is_trading_day(next_date):
                self._current_date = next_date
                return next_date
            next_date = next_date + timedelta(days=1)

        return None

    # ==================================================================
    # Data access
    # ==================================================================

    def get_data(self, table: str, filters: Mapping[str, Any]) -> pd.DataFrame:
        """Return time-gated data for the requested table.

        For Iteration 3 this supports only the ``prices_daily`` table.
        Data is always filtered such that all rows satisfy
        ``trade_date <= current_date``.

        Args:
            table: Logical table name (currently only ``"prices_daily"``
                is supported).
            filters: Dictionary of filter parameters. For
                ``"prices_daily"`` the following keys are honoured:

                - ``instrument_ids`` (Sequence[str]) – required.
                - ``start_date`` (date) – optional, defaults to
                  ``self._start_date``.
                - ``end_date`` (date) – optional, defaults to
                  ``self._end_date``.

        Returns:
            A :class:`pandas.DataFrame` with the requested data, filtered
            so that no row has a ``trade_date`` greater than
            :attr:`current_date`.

        Raises:
            ValueError: If an unsupported table is requested or if
                strict mode is enabled and a filter explicitly requests
                data beyond :attr:`current_date`.
            RuntimeError: If no :class:`DataReader` is configured when
                requesting ``prices_daily``.
        """

        if table != "prices_daily":
            raise ValueError(f"Unsupported table for TimeMachine.get_data: {table}")

        if self._data_reader is None:
            raise RuntimeError("TimeMachine has no DataReader configured")

        instrument_ids = filters.get("instrument_ids")
        if not instrument_ids:
            # Return empty DataFrame with expected columns
            return pd.DataFrame(
                columns=[
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
            )

        start_date: date = filters.get("start_date", self._start_date)  # type: ignore[assignment]
        end_date: date = filters.get("end_date", self._end_date)  # type: ignore[assignment]

        # Enforce no-lookahead constraints on the requested end-date.
        if end_date > self._current_date:
            if self._strict_mode:
                raise ValueError(
                    "Requested end_date %s is after current_date %s"
                    % (end_date, self._current_date)
                )

        # Fetch data using the supplied date range, then filter by
        # current_date to guarantee no rows leak from the future even if
        # the underlying reader misbehaves.
        df = self._data_reader.read_prices(instrument_ids, start_date, end_date)

        if df.empty:
            return df

        if "trade_date" not in df.columns:
            # Defensive: if schema is different from expectations.
            logger.warning("prices_daily DataFrame missing trade_date column; returning as-is")
            return df

        filtered = df[df["trade_date"] <= self._current_date].copy()
        return filtered

    # ==================================================================
    # Internal helpers
    # ==================================================================

    def _validate_no_lookahead(self, requested_date: date) -> None:
        """Validate that ``requested_date`` is not in the future.

        This helper is currently unused by :meth:`get_data` but is kept
        for parity with the architecture diagrams and for use in future
        extensions (e.g. other table types).
        """

        if requested_date > self._current_date:
            raise ValueError(
                "Requested date %s is after current_date %s"
                % (requested_date, self._current_date)
            )
