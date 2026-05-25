"""Options positions persistence tables (production)

Revision ID: 0097_options_positions
Revises: 0096_signal_alert_notify
Create Date: 2026-05-24

Production tables for tracking live and paper options positions and the
events that mutate them. This is the parallel to backtest_options_runs/
trades/daily but for live/paper trading, so the picture survives an
IBKR disconnect or a daemon restart.

- options_positions: current state per open position (mutable; one row
  per instrument_id + portfolio_id while open). Snapshot includes greeks
  and the sleeve/template that opened it (nullable until Phase 1+).
- options_position_events: immutable log of OPEN/CLOSE/ROLL/EXPIRE/MARK
  events. Survives position closure for attribution and audit.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0097_options_positions"
down_revision: Union[str, None] = "0096_signal_alert_notify"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "options_positions",
        sa.Column("position_id", sa.String(length=64), primary_key=True),
        sa.Column("instrument_id", sa.String(length=64), nullable=False),
        sa.Column("portfolio_id", sa.String(length=64), nullable=False),
        sa.Column("mode", sa.String(length=16), nullable=False),
        # Sleeve and template are populated from Phase 1 onward. Nullable
        # so the existing options_strategy.py classes can write through
        # during the migration window with only `strategy` set.
        sa.Column("sleeve", sa.String(length=16), nullable=True),
        sa.Column("template", sa.String(length=64), nullable=True),
        sa.Column("strategy", sa.String(length=64), nullable=True),
        # Contract identity
        sa.Column("symbol", sa.String(length=50), nullable=False),
        sa.Column("right", sa.String(length=2), nullable=False),
        sa.Column("expiry", sa.String(length=8), nullable=False),
        sa.Column("strike", sa.Float, nullable=False),
        sa.Column("multiplier", sa.Integer, nullable=False, server_default="100"),
        sa.Column("sec_type", sa.String(length=8), nullable=False, server_default="OPT"),
        # Position state
        sa.Column("quantity", sa.Integer, nullable=False),
        sa.Column("avg_cost", sa.Float, nullable=False),
        sa.Column("opened_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("opened_decision_id", sa.String(length=64), nullable=True),
        # Latest mark
        sa.Column("market_price", sa.Float, nullable=True),
        sa.Column("market_value", sa.Float, nullable=True),
        sa.Column("unrealized_pnl", sa.Float, nullable=True),
        # Greeks snapshot
        sa.Column("delta", sa.Float, nullable=True),
        sa.Column("gamma", sa.Float, nullable=True),
        sa.Column("theta", sa.Float, nullable=True),
        sa.Column("vega", sa.Float, nullable=True),
        sa.Column("implied_vol", sa.Float, nullable=True),
        sa.Column("underlying_price", sa.Float, nullable=True),
        sa.Column("greeks_updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("metadata_json", postgresql.JSONB, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
    )

    # One open position per (instrument, portfolio, mode). Closed
    # positions are removed from this table (history lives in events).
    op.create_index(
        "ux_options_positions_open",
        "options_positions",
        ["instrument_id", "portfolio_id", "mode"],
        unique=True,
    )
    op.create_index(
        "idx_options_positions_portfolio",
        "options_positions",
        ["portfolio_id", "mode"],
    )
    op.create_index(
        "idx_options_positions_expiry",
        "options_positions",
        ["expiry"],
    )
    op.create_index(
        "idx_options_positions_sleeve",
        "options_positions",
        ["sleeve", "template"],
    )

    op.create_table(
        "options_position_events",
        sa.Column("event_id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("position_id", sa.String(length=64), nullable=False),
        sa.Column("portfolio_id", sa.String(length=64), nullable=False),
        sa.Column("mode", sa.String(length=16), nullable=False),
        sa.Column("event_type", sa.String(length=16), nullable=False),
        sa.Column("event_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("as_of_date", sa.Date, nullable=False),
        # Position identity (denormalised so events survive even if the
        # parent row is deleted on close)
        sa.Column("instrument_id", sa.String(length=64), nullable=False),
        sa.Column("symbol", sa.String(length=50), nullable=False),
        sa.Column("right", sa.String(length=2), nullable=False),
        sa.Column("expiry", sa.String(length=8), nullable=False),
        sa.Column("strike", sa.Float, nullable=False),
        sa.Column("multiplier", sa.Integer, nullable=False, server_default="100"),
        # Event details
        sa.Column("quantity_delta", sa.Integer, nullable=False),
        sa.Column("price", sa.Float, nullable=True),
        sa.Column("realized_pnl", sa.Float, nullable=True),
        # Provenance
        sa.Column("sleeve", sa.String(length=16), nullable=True),
        sa.Column("template", sa.String(length=64), nullable=True),
        sa.Column("strategy", sa.String(length=64), nullable=True),
        sa.Column("decision_id", sa.String(length=64), nullable=True),
        sa.Column("order_id", sa.String(length=64), nullable=True),
        sa.Column("fill_id", sa.String(length=64), nullable=True),
        # Optional greeks snapshot at event time
        sa.Column("greeks_json", postgresql.JSONB, nullable=True),
        sa.Column("metadata_json", postgresql.JSONB, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
    )

    op.create_index(
        "idx_options_events_position",
        "options_position_events",
        ["position_id", "event_at"],
    )
    op.create_index(
        "idx_options_events_portfolio_date",
        "options_position_events",
        ["portfolio_id", "as_of_date"],
    )
    op.create_index(
        "idx_options_events_decision",
        "options_position_events",
        ["decision_id"],
        postgresql_where=sa.text("decision_id IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index("idx_options_events_decision", table_name="options_position_events")
    op.drop_index("idx_options_events_portfolio_date", table_name="options_position_events")
    op.drop_index("idx_options_events_position", table_name="options_position_events")
    op.drop_table("options_position_events")

    op.drop_index("idx_options_positions_sleeve", table_name="options_positions")
    op.drop_index("idx_options_positions_expiry", table_name="options_positions")
    op.drop_index("idx_options_positions_portfolio", table_name="options_positions")
    op.drop_index("ux_options_positions_open", table_name="options_positions")
    op.drop_table("options_positions")
