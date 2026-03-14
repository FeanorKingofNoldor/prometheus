"""meta orchestrator core tables

Revision ID: 0018
Revises: 0017
Create Date: 2025-11-26

This migration introduces core tables used by the Meta-Orchestrator and
related analytics components:

- engine_decisions
- decision_outcomes
- executed_actions
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "0018"
down_revision: Union[str, None] = "0017"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create Meta-Orchestrator core tables."""

    op.create_table(
        "engine_decisions",
        sa.Column("decision_id", sa.String(length=64), primary_key=True),
        sa.Column("engine_name", sa.String(length=64), nullable=False),
        sa.Column("run_id", sa.String(length=64), nullable=True),
        sa.Column("strategy_id", sa.String(length=64), nullable=True),
        sa.Column("market_id", sa.String(length=32), nullable=True),
        sa.Column("as_of_date", sa.Date, nullable=False),
        sa.Column("config_id", sa.String(length=128), nullable=True),
        sa.Column("input_refs", postgresql.JSONB, nullable=True),
        sa.Column("output_refs", postgresql.JSONB, nullable=True),
        sa.Column("metadata", postgresql.JSONB, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )

    op.create_index(
        "idx_engine_decisions_strategy_date",
        "engine_decisions",
        ["strategy_id", "as_of_date"],
    )

    op.create_table(
        "decision_outcomes",
        sa.Column("outcome_id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("decision_id", sa.String(length=64), nullable=False),
        sa.Column("horizon_days", sa.Integer, nullable=False),
        sa.Column("realized_return", sa.Float, nullable=True),
        sa.Column("realized_pnl", sa.Float, nullable=True),
        sa.Column("realized_drawdown", sa.Float, nullable=True),
        sa.Column("realized_vol", sa.Float, nullable=True),
        sa.Column("metadata", postgresql.JSONB, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["decision_id"],
            ["engine_decisions.decision_id"],
            name="fk_decision_outcomes_decision",
        ),
    )

    op.create_index(
        "idx_decision_outcomes_decision_horizon",
        "decision_outcomes",
        ["decision_id", "horizon_days"],
    )

    op.create_table(
        "executed_actions",
        sa.Column("action_id", sa.String(length=64), primary_key=True),
        sa.Column("decision_id", sa.String(length=64), nullable=True),
        sa.Column("run_id", sa.String(length=64), nullable=True),
        sa.Column("portfolio_id", sa.String(length=64), nullable=True),
        sa.Column("instrument_id", sa.String(length=64), nullable=True),
        sa.Column("trade_date", sa.Date, nullable=False),
        sa.Column("side", sa.String(length=8), nullable=False),
        sa.Column("quantity", sa.Float, nullable=False),
        sa.Column("price", sa.Float, nullable=False),
        sa.Column("slippage", sa.Float, nullable=True),
        sa.Column("fees", sa.Float, nullable=True),
        sa.Column("metadata", postgresql.JSONB, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["decision_id"],
            ["engine_decisions.decision_id"],
            name="fk_executed_actions_decision",
        ),
    )

    op.create_index(
        "idx_executed_actions_decision",
        "executed_actions",
        ["decision_id"],
    )
    op.create_index(
        "idx_executed_actions_run_date",
        "executed_actions",
        ["run_id", "trade_date"],
    )


def downgrade() -> None:
    """Drop Meta-Orchestrator core tables."""

    op.drop_index("idx_executed_actions_run_date", table_name="executed_actions")
    op.drop_index("idx_executed_actions_decision", table_name="executed_actions")
    op.drop_table("executed_actions")

    op.drop_index("idx_decision_outcomes_decision_horizon", table_name="decision_outcomes")
    op.drop_table("decision_outcomes")

    op.drop_index("idx_engine_decisions_strategy_date", table_name="engine_decisions")
    op.drop_table("engine_decisions")
