"""risk actions log table

Revision ID: 0019
Revises: 0018
Create Date: 2025-11-26

This migration introduces ``risk_actions``, a lightweight table used by
the Risk Management Service to record how risk constraints modify or
override proposed positions.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = "0019"
down_revision: Union[str, None] = "0018"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create risk_actions table."""

    op.create_table(
        "risk_actions",
        sa.Column("action_id", sa.String(length=64), primary_key=True),
        sa.Column("strategy_id", sa.String(length=64), nullable=True),
        sa.Column("instrument_id", sa.String(length=64), nullable=True),
        sa.Column("decision_id", sa.String(length=64), nullable=True),
        sa.Column("action_type", sa.String(length=32), nullable=False),
        sa.Column("details_json", postgresql.JSONB, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )

    op.create_index(
        "idx_risk_actions_strategy_instrument",
        "risk_actions",
        ["strategy_id", "instrument_id"],
    )


def downgrade() -> None:
    """Drop risk_actions table."""

    op.drop_index("idx_risk_actions_strategy_instrument", table_name="risk_actions")
    op.drop_table("risk_actions")
