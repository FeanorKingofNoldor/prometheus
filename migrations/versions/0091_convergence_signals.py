"""Add convergence_signals table.

Revision ID: 0091_convergence_signals
Revises: 0090_divergence_signals
Create Date: 2026-05-05

Persists per-entity convergence timelines pulled from
``apatheon.intel.convergence_timing.scan_convergence_timelines``.  Each
row pairs with a divergence row (same as_of_date / entity_type /
entity_id) and carries the laddered entry windows the trader can act on.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "0091_convergence_signals"
down_revision: Union[str, None] = "0090_divergence_signals"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "convergence_signals",
        sa.Column("signal_id", sa.String(), primary_key=True),
        sa.Column("as_of_date", sa.Date(), nullable=False),
        sa.Column("entity_type", sa.String(), nullable=False),
        sa.Column("entity_id", sa.String(), nullable=False),
        sa.Column("days_to_hard_deadline", sa.Float(), nullable=True),
        sa.Column("hard_deadline_reason", sa.String(), nullable=True),
        sa.Column("days_to_soft_signal", sa.Float(), nullable=True),
        sa.Column("soft_signal_type", sa.String(), nullable=True),
        sa.Column("infrastructure_lag_days", sa.Float(), nullable=False, server_default="0"),
        sa.Column("buffer_days", sa.Float(), nullable=False, server_default="0"),
        sa.Column("buffer_source", sa.String(), nullable=True),
        sa.Column("estimated_convergence_days", sa.Float(), nullable=True),
        sa.Column("convergence_window_min", sa.Float(), nullable=True),
        sa.Column("convergence_window_max", sa.Float(), nullable=True),
        sa.Column("confidence", sa.Float(), nullable=False, server_default="0"),
        sa.Column("strategy", sa.String(), nullable=True),
        sa.Column(
            "entry_windows",
            sa.dialects.postgresql.JSONB(),
            nullable=False,
            server_default="[]",
        ),
        sa.Column("decision_id", sa.String(), nullable=True),
        sa.Column("computed_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "as_of_date",
            "entity_type",
            "entity_id",
            name="uq_convergence_signals_date_entity",
        ),
        sa.CheckConstraint(
            "btrim(signal_id) <> ''",
            name="ck_convergence_signals_signal_id_nonempty",
        ),
        sa.CheckConstraint(
            "entity_type IN ('chokepoint', 'conflict', 'commodity')",
            name="ck_convergence_signals_entity_type",
        ),
        sa.CheckConstraint(
            "confidence >= 0 AND confidence <= 1",
            name="ck_convergence_signals_confidence_range",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(entry_windows) = 'array'",
            name="ck_convergence_signals_entry_windows_array",
        ),
    )

    op.create_index(
        "ix_convergence_signals_as_of_date",
        "convergence_signals",
        ["as_of_date"],
    )
    op.create_index(
        "ix_convergence_signals_decision_id",
        "convergence_signals",
        ["decision_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_convergence_signals_decision_id", table_name="convergence_signals")
    op.drop_index("ix_convergence_signals_as_of_date", table_name="convergence_signals")
    op.drop_table("convergence_signals")
