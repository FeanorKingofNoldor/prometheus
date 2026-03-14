"""Options backtest persistence tables

Revision ID: 0085_options_backtest_persistence
Revises: 0084_derivatives_instruments
Create Date: 2026-03-04

Adds three tables to persist synthetic options backtest data so that
results mirror the live trading schema and can be inspected after runs:

- backtest_options_runs: metadata per backtest run (config, summary)
- backtest_options_trades: every option open/close/roll/expire event
- backtest_options_daily: end-of-day position state with greeks, IV,
  P&L attribution per position
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = "0085_options_backtest_persistence"
down_revision: Union[str, None] = "0084_derivatives_instruments"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create options backtest persistence tables."""

    # ── Run metadata ─────────────────────────────────────────────────
    op.create_table(
        "backtest_options_runs",
        sa.Column("run_id", sa.String(length=64), primary_key=True),
        sa.Column("start_date", sa.Date, nullable=False),
        sa.Column("end_date", sa.Date, nullable=False),
        sa.Column("initial_nav", sa.Float, nullable=False),
        sa.Column("derivatives_budget_pct", sa.Float, nullable=False),
        sa.Column("equity_backtest_run_id", sa.String(length=64), nullable=True),
        sa.Column("config_json", postgresql.JSONB, nullable=False),
        sa.Column("summary_json", postgresql.JSONB, nullable=True),
        sa.Column("n_trading_days", sa.Integer, nullable=True),
        sa.Column("final_nav", sa.Float, nullable=True),
        sa.Column("cagr", sa.Float, nullable=True),
        sa.Column("sharpe", sa.Float, nullable=True),
        sa.Column("max_drawdown", sa.Float, nullable=True),
        sa.Column("options_total_pnl", sa.Float, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
    )

    # ── Option trades ────────────────────────────────────────────────
    # Every OPEN / CLOSE / ROLL / EXPIRE event.  One row per leg
    # (spreads produce two rows linked by spread_group_id).
    op.create_table(
        "backtest_options_trades",
        sa.Column("trade_id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("run_id", sa.String(length=64), nullable=False),
        sa.Column("trade_date", sa.Date, nullable=False),
        sa.Column("position_id", sa.String(length=64), nullable=False),
        # Option identity
        sa.Column("symbol", sa.String(length=50), nullable=False),
        sa.Column("right", sa.String(length=2), nullable=False),     # C or P
        sa.Column("expiry", sa.String(length=8), nullable=False),    # YYYYMMDD
        sa.Column("strike", sa.Float, nullable=False),
        sa.Column("multiplier", sa.Integer, nullable=False, server_default="100"),
        # Trade details
        sa.Column("action", sa.String(length=16), nullable=False),   # OPEN/CLOSE/ROLL/EXPIRE
        sa.Column("quantity", sa.Integer, nullable=False),            # Signed: + long, - short
        sa.Column("price", sa.Float, nullable=False),                 # Per-share fill price
        sa.Column("mid_price", sa.Float, nullable=True),              # Theoretical mid
        sa.Column("iv_at_trade", sa.Float, nullable=True),            # IV used for pricing
        sa.Column("underlying_price", sa.Float, nullable=True),
        sa.Column("vix_at_trade", sa.Float, nullable=True),
        # Strategy provenance
        sa.Column("strategy", sa.String(length=64), nullable=False),
        sa.Column("spread_group_id", sa.String(length=64), nullable=True),
        # Realized P&L (for CLOSE/EXPIRE)
        sa.Column("realized_pnl", sa.Float, nullable=True),
        sa.Column("metadata_json", postgresql.JSONB, nullable=True),
        # FK
        sa.ForeignKeyConstraint(
            ["run_id"], ["backtest_options_runs.run_id"],
            name="fk_bt_opt_trades_run",
        ),
    )

    op.create_index(
        "idx_bt_opt_trades_run_date",
        "backtest_options_trades",
        ["run_id", "trade_date"],
    )
    op.create_index(
        "idx_bt_opt_trades_symbol_strategy",
        "backtest_options_trades",
        ["run_id", "symbol", "strategy"],
    )

    # ── Daily position state ─────────────────────────────────────────
    # One row per open position per day.  This is the option-equivalent
    # of positions_snapshots but with greeks, IV, and P&L attribution.
    op.create_table(
        "backtest_options_daily",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("run_id", sa.String(length=64), nullable=False),
        sa.Column("trade_date", sa.Date, nullable=False),
        sa.Column("position_id", sa.String(length=64), nullable=False),
        # Option identity
        sa.Column("symbol", sa.String(length=50), nullable=False),
        sa.Column("right", sa.String(length=2), nullable=False),
        sa.Column("expiry", sa.String(length=8), nullable=False),
        sa.Column("strike", sa.Float, nullable=False),
        sa.Column("quantity", sa.Integer, nullable=False),
        sa.Column("strategy", sa.String(length=64), nullable=False),
        # Pricing
        sa.Column("underlying_price", sa.Float, nullable=False),
        sa.Column("option_price", sa.Float, nullable=False),          # Per-share mid
        sa.Column("iv", sa.Float, nullable=False),                     # IV used
        sa.Column("vix", sa.Float, nullable=False),
        # Greeks (per-share, not per-contract)
        sa.Column("delta", sa.Float, nullable=False),
        sa.Column("gamma", sa.Float, nullable=False),
        sa.Column("theta", sa.Float, nullable=False),                  # Per calendar day
        sa.Column("vega", sa.Float, nullable=False),                   # Per 1% vol
        # P&L
        sa.Column("market_value", sa.Float, nullable=False),           # price × mult × qty
        sa.Column("unrealized_pnl", sa.Float, nullable=False),
        sa.Column("entry_price", sa.Float, nullable=False),
        sa.Column("dte", sa.Integer, nullable=False),
        # Portfolio-level aggregates for this day (denormalized for fast queries)
        sa.Column("market_situation", sa.String(length=16), nullable=True),
        # FK
        sa.ForeignKeyConstraint(
            ["run_id"], ["backtest_options_runs.run_id"],
            name="fk_bt_opt_daily_run",
        ),
    )

    op.create_index(
        "idx_bt_opt_daily_run_date",
        "backtest_options_daily",
        ["run_id", "trade_date"],
    )
    op.create_index(
        "idx_bt_opt_daily_position",
        "backtest_options_daily",
        ["run_id", "position_id"],
    )

    # ── Portfolio-level daily summary ────────────────────────────────
    # One row per day: aggregate NAV, greeks, P&L attribution.
    # This parallels backtest_daily_equity but for the options overlay.
    op.create_table(
        "backtest_options_daily_summary",
        sa.Column("run_id", sa.String(length=64), nullable=False),
        sa.Column("trade_date", sa.Date, nullable=False),
        # NAV
        sa.Column("equity_nav", sa.Float, nullable=False),
        sa.Column("options_cumulative_pnl", sa.Float, nullable=False),
        sa.Column("total_nav", sa.Float, nullable=False),
        sa.Column("options_daily_pnl", sa.Float, nullable=False),
        # Aggregate greeks (share-equivalents × multiplier × quantity)
        sa.Column("net_delta", sa.Float, nullable=False),
        sa.Column("net_gamma", sa.Float, nullable=False),
        sa.Column("net_theta", sa.Float, nullable=False),
        sa.Column("net_vega", sa.Float, nullable=False),
        # P&L attribution
        sa.Column("delta_pnl", sa.Float, nullable=True),
        sa.Column("theta_pnl", sa.Float, nullable=True),
        sa.Column("vega_pnl", sa.Float, nullable=True),
        sa.Column("gamma_pnl", sa.Float, nullable=True),
        # State
        sa.Column("n_positions", sa.Integer, nullable=False),
        sa.Column("n_strategies_active", sa.Integer, nullable=False),
        sa.Column("market_situation", sa.String(length=16), nullable=True),
        sa.Column("vix", sa.Float, nullable=True),
        # PK
        sa.PrimaryKeyConstraint("run_id", "trade_date",
                                name="pk_bt_opt_daily_summary"),
        sa.ForeignKeyConstraint(
            ["run_id"], ["backtest_options_runs.run_id"],
            name="fk_bt_opt_daily_summary_run",
        ),
    )


def downgrade() -> None:
    """Drop options backtest persistence tables."""
    op.drop_table("backtest_options_daily_summary")
    op.drop_index("idx_bt_opt_daily_position", table_name="backtest_options_daily")
    op.drop_index("idx_bt_opt_daily_run_date", table_name="backtest_options_daily")
    op.drop_table("backtest_options_daily")
    op.drop_index("idx_bt_opt_trades_symbol_strategy", table_name="backtest_options_trades")
    op.drop_index("idx_bt_opt_trades_run_date", table_name="backtest_options_trades")
    op.drop_table("backtest_options_trades")
    op.drop_table("backtest_options_runs")
