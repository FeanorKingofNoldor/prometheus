"""Add divergence_signals table.

Revision ID: 0090_divergence_signals
Revises: 0089_iris_insights
Create Date: 2026-05-05

Persists narrative-vs-reality divergence scans pulled from Apatheon's
``apatheon.intel.signal_classifier``.  Each row is one scan result for a
single chokepoint or conflict on a given date; a unique constraint on
(as_of_date, entity_type, entity_id) keeps the table at one row per
entity per day so downstream consumers can read "today's signals" without
deduping.

Significant / extreme signals are also logged to ``engine_decisions`` by
the divergence scanner so the Meta-Orchestrator can score realised
outcomes the same way it scores assessment / portfolio decisions.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "0090_divergence_signals"
down_revision: Union[str, None] = "0089_iris_insights"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "divergence_signals",
        sa.Column("signal_id", sa.String(), primary_key=True),
        sa.Column("as_of_date", sa.Date(), nullable=False),
        sa.Column("entity_type", sa.String(), nullable=False),
        sa.Column("entity_id", sa.String(), nullable=False),
        sa.Column("behavioral_score", sa.Float(), nullable=False),
        sa.Column("narrative_score", sa.Float(), nullable=False),
        sa.Column("divergence", sa.Float(), nullable=False),
        sa.Column("abs_divergence", sa.Float(), nullable=False),
        sa.Column("direction", sa.String(), nullable=False),
        sa.Column("severity", sa.String(), nullable=False),
        sa.Column("trading_signal", sa.String(), nullable=False),
        sa.Column("decision_id", sa.String(), nullable=True),
        sa.Column("computed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "metadata",
            sa.dialects.postgresql.JSONB(),
            nullable=True,
        ),
        sa.UniqueConstraint(
            "as_of_date",
            "entity_type",
            "entity_id",
            name="uq_divergence_signals_date_entity",
        ),
        sa.CheckConstraint(
            "btrim(signal_id) <> ''",
            name="ck_divergence_signals_signal_id_nonempty",
        ),
        sa.CheckConstraint(
            "entity_type IN ('chokepoint', 'conflict')",
            name="ck_divergence_signals_entity_type",
        ),
        sa.CheckConstraint(
            "direction IN ('ALIGNED', 'NARRATIVE_OVERSTATES', 'REALITY_UNDERSTATED')",
            name="ck_divergence_signals_direction",
        ),
        sa.CheckConstraint(
            "severity IN ('NONE', 'MILD', 'SIGNIFICANT', 'EXTREME')",
            name="ck_divergence_signals_severity",
        ),
        sa.CheckConstraint(
            "trading_signal IN ('NONE', 'FADE_NARRATIVE', 'FRONT_RUN_REALITY')",
            name="ck_divergence_signals_trading_signal",
        ),
    )

    op.create_index(
        "ix_divergence_signals_as_of_date",
        "divergence_signals",
        ["as_of_date"],
    )
    op.create_index(
        "ix_divergence_signals_severity",
        "divergence_signals",
        ["severity"],
    )
    op.create_index(
        "ix_divergence_signals_decision_id",
        "divergence_signals",
        ["decision_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_divergence_signals_decision_id", table_name="divergence_signals")
    op.drop_index("ix_divergence_signals_severity", table_name="divergence_signals")
    op.drop_index("ix_divergence_signals_as_of_date", table_name="divergence_signals")
    op.drop_table("divergence_signals")
