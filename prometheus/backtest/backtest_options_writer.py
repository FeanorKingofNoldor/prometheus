"""Prometheus v2 – Options Backtest Persistence Writer.

Writes synthetic options backtest results to the tables created by
migration 0085:

- ``backtest_options_runs``         — run-level metadata and summary
- ``backtest_options_trades``       — every OPEN / CLOSE / ROLL / EXPIRE event
- ``backtest_options_daily``        — end-of-day position state with greeks
- ``backtest_options_daily_summary``— portfolio-level daily aggregates

The writer is designed to be called incrementally inside the
:class:`OptionsBacktestEngine` daily loop so that data is persisted as
the simulation progresses (with periodic commits for performance).

Usage::

    from prometheus.backtest.backtest_options_writer import BacktestOptionsWriter
    from apatheon.core.database import get_db_manager

    writer = BacktestOptionsWriter(db_manager=get_db_manager(), run_id="...")
    writer.insert_run(config, start_date, end_date)
    # ... during daily loop:
    writer.insert_trade(...)
    writer.insert_daily_positions(...)
    writer.insert_daily_summary(...)
    # ... at the end:
    writer.update_run_summary(summary)
    writer.flush()
"""

from __future__ import annotations

import uuid
from datetime import date
from typing import Any, Dict, List, Optional

from apatheon.core.database import DatabaseManager
from apatheon.core.logging import get_logger
from psycopg2 import sql as psql
from psycopg2.extras import Json

logger = get_logger(__name__)

# Buffer size before auto-flushing
_TRADE_BUFFER_SIZE = 200
_DAILY_BUFFER_SIZE = 500
_DAILY_SUMMARY_BUFFER_SIZE = 100


def generate_run_id() -> str:
    """Generate a deterministic-length run ID."""
    return f"opt_{uuid.uuid4().hex[:16]}"


class BacktestOptionsWriter:
    """Persist options backtest data to PostgreSQL.

    Parameters
    ----------
    db_manager : DatabaseManager
        Prometheus database manager.
    run_id : str
        Unique identifier for this backtest run.
    """

    def __init__(self, db_manager: DatabaseManager, run_id: str) -> None:
        self._db = db_manager
        self._run_id = run_id
        self._trade_buffer: List[tuple] = []
        self._daily_buffer: List[tuple] = []
        self._daily_summary_buffer: List[tuple] = []

    @property
    def run_id(self) -> str:
        return self._run_id

    # ── Run metadata ─────────────────────────────────────────────────

    def insert_run(
        self,
        *,
        start_date: date,
        end_date: date,
        initial_nav: float,
        derivatives_budget_pct: float,
        config: Dict[str, Any],
        equity_backtest_run_id: Optional[str] = None,
    ) -> None:
        """Insert a row into ``backtest_options_runs``."""

        sql = """
            INSERT INTO backtest_options_runs (
                run_id, start_date, end_date, initial_nav,
                derivatives_budget_pct, equity_backtest_run_id,
                config_json, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
        """

        with self._db.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, (
                    self._run_id,
                    start_date,
                    end_date,
                    float(initial_nav),
                    float(derivatives_budget_pct),
                    equity_backtest_run_id,
                    Json(config),
                ))
                conn.commit()
            finally:
                cursor.close()

        logger.info("Inserted backtest_options_runs row: %s", self._run_id)

    def update_run_summary(self, summary: Dict[str, Any]) -> None:
        """Update the run with final summary stats."""

        def _native(v):
            """Convert numpy scalars to native Python types."""
            if v is None:
                return None
            if hasattr(v, "item"):
                return v.item()
            return v

        # Sanitize summary dict for JSON serialization (strip numpy types)
        clean_summary = {k: _native(v) for k, v in summary.items()}

        sql = """
            UPDATE backtest_options_runs
               SET summary_json        = %s,
                   n_trading_days       = %s,
                   final_nav            = %s,
                   cagr                 = %s,
                   sharpe               = %s,
                   max_drawdown         = %s,
                   options_total_pnl    = %s
             WHERE run_id = %s
        """

        with self._db.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, (
                    Json(clean_summary),
                    _native(summary.get("n_trading_days")),
                    _native(summary.get("final_nav")),
                    _native(summary.get("cagr")),
                    _native(summary.get("sharpe")),
                    _native(summary.get("max_drawdown")),
                    _native(summary.get("options_total_pnl")),
                    self._run_id,
                ))
                conn.commit()
            finally:
                cursor.close()

        logger.info("Updated backtest_options_runs summary: %s", self._run_id)

    # ── Trades ───────────────────────────────────────────────────────

    def insert_trade(
        self,
        *,
        trade_date: date,
        position_id: str,
        symbol: str,
        right: str,
        expiry: str,
        strike: float,
        multiplier: int = 100,
        action: str,
        quantity: int,
        price: float,
        mid_price: Optional[float] = None,
        iv_at_trade: Optional[float] = None,
        underlying_price: Optional[float] = None,
        vix_at_trade: Optional[float] = None,
        strategy: str = "",
        spread_group_id: Optional[str] = None,
        realized_pnl: Optional[float] = None,
        metadata_json: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Buffer a trade row for batch insert."""

        self._trade_buffer.append((
            self._run_id,
            trade_date,
            position_id,
            symbol,
            right,
            expiry,
            float(strike),
            multiplier,
            action,
            quantity,
            float(price),
            float(mid_price) if mid_price is not None else None,
            float(iv_at_trade) if iv_at_trade is not None else None,
            float(underlying_price) if underlying_price is not None else None,
            float(vix_at_trade) if vix_at_trade is not None else None,
            strategy,
            spread_group_id,
            float(realized_pnl) if realized_pnl is not None else None,
            Json(metadata_json) if metadata_json else None,
        ))

        if len(self._trade_buffer) >= _TRADE_BUFFER_SIZE:
            self._flush_trades()

    def _flush_trades(self) -> None:
        """Flush buffered trades to the database."""
        if not self._trade_buffer:
            return

        sql = """
            INSERT INTO backtest_options_trades (
                run_id, trade_date, position_id,
                symbol, "right", expiry, strike, multiplier,
                action, quantity, price, mid_price,
                iv_at_trade, underlying_price, vix_at_trade,
                strategy, spread_group_id,
                realized_pnl, metadata_json
            ) VALUES (
                %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s
            )
        """

        with self._db.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.executemany(sql, self._trade_buffer)
                conn.commit()
            finally:
                cursor.close()

        logger.debug("Flushed %d trade rows", len(self._trade_buffer))
        self._trade_buffer.clear()

    # ── Daily position snapshots ─────────────────────────────────────

    def insert_daily_position(
        self,
        *,
        trade_date: date,
        position_id: str,
        symbol: str,
        right: str,
        expiry: str,
        strike: float,
        quantity: int,
        strategy: str,
        underlying_price: float,
        option_price: float,
        iv: float,
        vix: float,
        delta: float,
        gamma: float,
        theta: float,
        vega: float,
        market_value: float,
        unrealized_pnl: float,
        entry_price: float,
        dte: int,
        market_situation: Optional[str] = None,
    ) -> None:
        """Buffer a daily position snapshot row."""

        self._daily_buffer.append((
            self._run_id,
            trade_date,
            position_id,
            symbol,
            right,
            expiry,
            float(strike),
            quantity,
            strategy,
            float(underlying_price),
            float(option_price),
            float(iv),
            float(vix),
            float(delta),
            float(gamma),
            float(theta),
            float(vega),
            float(market_value),
            float(unrealized_pnl),
            float(entry_price),
            dte,
            market_situation,
        ))

        if len(self._daily_buffer) >= _DAILY_BUFFER_SIZE:
            self._flush_daily()

    def _flush_daily(self) -> None:
        """Flush buffered daily position snapshots."""
        if not self._daily_buffer:
            return

        sql = """
            INSERT INTO backtest_options_daily (
                run_id, trade_date, position_id,
                symbol, "right", expiry, strike, quantity, strategy,
                underlying_price, option_price, iv, vix,
                delta, gamma, theta, vega,
                market_value, unrealized_pnl, entry_price, dte,
                market_situation
            ) VALUES (
                %s, %s, %s,
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s
            )
        """

        with self._db.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.executemany(sql, self._daily_buffer)
                conn.commit()
            finally:
                cursor.close()

        logger.debug("Flushed %d daily position rows", len(self._daily_buffer))
        self._daily_buffer.clear()

    # ── Daily portfolio summary ──────────────────────────────────────

    def insert_daily_summary(
        self,
        *,
        trade_date: date,
        equity_nav: float,
        options_cumulative_pnl: float,
        total_nav: float,
        options_daily_pnl: float,
        net_delta: float,
        net_gamma: float,
        net_theta: float,
        net_vega: float,
        delta_pnl: Optional[float] = None,
        theta_pnl: Optional[float] = None,
        vega_pnl: Optional[float] = None,
        gamma_pnl: Optional[float] = None,
        n_positions: int = 0,
        n_strategies_active: int = 0,
        market_situation: Optional[str] = None,
        vix: Optional[float] = None,
    ) -> None:
        """Buffer a daily portfolio-level summary row."""

        self._daily_summary_buffer.append((
            self._run_id,
            trade_date,
            float(equity_nav),
            float(options_cumulative_pnl),
            float(total_nav),
            float(options_daily_pnl),
            float(net_delta),
            float(net_gamma),
            float(net_theta),
            float(net_vega),
            float(delta_pnl) if delta_pnl is not None else None,
            float(theta_pnl) if theta_pnl is not None else None,
            float(vega_pnl) if vega_pnl is not None else None,
            float(gamma_pnl) if gamma_pnl is not None else None,
            n_positions,
            n_strategies_active,
            market_situation,
            float(vix) if vix is not None else None,
        ))

        if len(self._daily_summary_buffer) >= _DAILY_SUMMARY_BUFFER_SIZE:
            self._flush_daily_summary()

    def _flush_daily_summary(self) -> None:
        """Flush buffered daily summary rows."""
        if not self._daily_summary_buffer:
            return

        sql = """
            INSERT INTO backtest_options_daily_summary (
                run_id, trade_date,
                equity_nav, options_cumulative_pnl, total_nav, options_daily_pnl,
                net_delta, net_gamma, net_theta, net_vega,
                delta_pnl, theta_pnl, vega_pnl, gamma_pnl,
                n_positions, n_strategies_active,
                market_situation, vix
            ) VALUES (
                %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s,
                %s, %s
            )
        """

        with self._db.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.executemany(sql, self._daily_summary_buffer)
                conn.commit()
            finally:
                cursor.close()

        logger.debug("Flushed %d daily summary rows", len(self._daily_summary_buffer))
        self._daily_summary_buffer.clear()

    # ── Bulk helpers ─────────────────────────────────────────────────

    def insert_daily_positions_from_book(
        self,
        trade_date: date,
        book,
        vix: float,
        underlying_prices: Dict[str, float],
        market_situation: Optional[str] = None,
    ) -> None:
        """Snapshot all open positions from a SyntheticOptionsBook.

        This is a convenience method that iterates the book's positions
        and calls :meth:`insert_daily_position` for each one.
        """
        for pos in book.positions.values():
            S = underlying_prices.get(pos.symbol, 0.0)
            self.insert_daily_position(
                trade_date=trade_date,
                position_id=pos.position_id,
                symbol=pos.symbol,
                right=pos.right,
                expiry=pos.expiry,
                strike=pos.strike,
                quantity=pos.quantity,
                strategy=pos.strategy,
                underlying_price=S,
                option_price=pos.current_price,
                iv=pos.current_iv,
                vix=vix,
                delta=pos.current_greeks.delta,
                gamma=pos.current_greeks.gamma,
                theta=pos.current_greeks.theta,
                vega=pos.current_greeks.vega,
                market_value=pos.market_value,
                unrealized_pnl=pos.unrealized_pnl,
                entry_price=pos.entry_price,
                dte=pos.dte(trade_date),
                market_situation=market_situation,
            )

    def flush(self) -> None:
        """Flush all pending buffers to the database."""
        self._flush_trades()
        self._flush_daily()
        self._flush_daily_summary()

    # ── Cleanup ──────────────────────────────────────────────────────

    def delete_run(self) -> None:
        """Delete all data for this run (for re-runs / cleanup)."""
        tables = [
            "backtest_options_daily_summary",
            "backtest_options_daily",
            "backtest_options_trades",
            "backtest_options_runs",
        ]

        with self._db.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                for table in tables:
                    cursor.execute(
                        psql.SQL("DELETE FROM {} WHERE run_id = %s").format(
                            psql.Identifier(table),
                        ),
                        (self._run_id,),
                    )
                conn.commit()
            finally:
                cursor.close()

        logger.info("Deleted all data for run %s", self._run_id)


__all__ = [
    "BacktestOptionsWriter",
    "generate_run_id",
]
