"""Add compound_pressure_alerts table.

Revision ID: 0092_compound_pressure_alerts
Revises: 0091_convergence_signals
Create Date: 2026-05-05

Persists encirclement alerts from
``apatheon.graph.simulator.detect_compound_pressure`` for each watched
sovereign target.  HIGH/CRITICAL alerts trigger an engine_decisions row
so the Meta-Orchestrator can later compare predicted-vs-realised
portfolio outcomes when compound pressure is detected.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "0092_compound_pressure_alerts"
down_revision: Union[str, None] = "0091_convergence_signals"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "compound_pressure_alerts",
        sa.Column("alert_id", sa.String(), primary_key=True),
        sa.Column("as_of_date", sa.Date(), nullable=False),
        sa.Column("target_entity_type", sa.String(), nullable=False),
        sa.Column("target_entity_id", sa.String(), nullable=False),
        sa.Column("lookback_days", sa.Integer(), nullable=False),
        sa.Column("total_pressure_points", sa.Integer(), nullable=False),
        sa.Column("pressure_points_moved", sa.Integer(), nullable=False),
        sa.Column("cluster_days", sa.Float(), nullable=False, server_default="0"),
        sa.Column("encirclement_score", sa.Float(), nullable=False),
        sa.Column("severity", sa.String(), nullable=False),
        sa.Column(
            "adversarial_movements",
            sa.dialects.postgresql.JSONB(),
            nullable=False,
            server_default="[]",
        ),
        sa.Column(
            "likely_orchestrators",
            sa.dialects.postgresql.JSONB(),
            nullable=False,
            server_default="[]",
        ),
        sa.Column("decision_id", sa.String(), nullable=True),
        sa.Column("computed_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "as_of_date",
            "target_entity_type",
            "target_entity_id",
            name="uq_compound_pressure_alerts_target",
        ),
        sa.CheckConstraint(
            "severity IN ('LOW', 'MODERATE', 'HIGH', 'CRITICAL')",
            name="ck_compound_pressure_alerts_severity",
        ),
        sa.CheckConstraint(
            "encirclement_score >= 0 AND encirclement_score <= 1",
            name="ck_compound_pressure_alerts_score_range",
        ),
    )

    op.create_index(
        "ix_compound_pressure_alerts_as_of_date",
        "compound_pressure_alerts",
        ["as_of_date"],
    )
    op.create_index(
        "ix_compound_pressure_alerts_severity",
        "compound_pressure_alerts",
        ["severity"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_compound_pressure_alerts_severity",
        table_name="compound_pressure_alerts",
    )
    op.drop_index(
        "ix_compound_pressure_alerts_as_of_date",
        table_name="compound_pressure_alerts",
    )
    op.drop_table("compound_pressure_alerts")
