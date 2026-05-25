"""Add beneficiary_scores table.

Revision ID: 0094_beneficiary_scores
Revises: 0093_portfolio_geo_risk
Create Date: 2026-05-05

Persists Cui Bono / beneficiary scores for active conflicts.  For each
active (or escalating) conflict, ``apatheon.graph.beneficiary.analyze_beneficiaries``
ranks sovereign candidates by motive / means / opportunity / pattern-match.
Prometheus persists the top-K candidates here so the assessment engine
can read "who benefits structurally from this conflict" as conviction
metadata when scoring instruments tied to the affected nations.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "0094_beneficiary_scores"
down_revision: Union[str, None] = "0093_portfolio_geo_risk"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "beneficiary_scores",
        sa.Column("score_id", sa.String(), primary_key=True),
        sa.Column("as_of_date", sa.Date(), nullable=False),
        sa.Column("victim_entity_type", sa.String(), nullable=False),
        sa.Column("victim_entity_id", sa.String(), nullable=False),
        sa.Column("rank", sa.Integer(), nullable=False),
        sa.Column("candidate_entity_type", sa.String(), nullable=False),
        sa.Column("candidate_entity_id", sa.String(), nullable=False),
        sa.Column("composite_score", sa.Float(), nullable=False),
        sa.Column("motive_score", sa.Float(), nullable=False),
        sa.Column("means_score", sa.Float(), nullable=False),
        sa.Column("opportunity_score", sa.Float(), nullable=False),
        sa.Column("pattern_match_score", sa.Float(), nullable=False),
        sa.Column("asymmetry_detected", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("decision_id", sa.String(), nullable=True),
        sa.Column(
            "metadata",
            sa.dialects.postgresql.JSONB(),
            nullable=False,
            server_default="{}",
        ),
        sa.Column("computed_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "as_of_date",
            "victim_entity_type",
            "victim_entity_id",
            "rank",
            name="uq_beneficiary_scores_victim_rank",
        ),
        sa.CheckConstraint(
            "composite_score >= 0 AND composite_score <= 1",
            name="ck_beneficiary_scores_composite_range",
        ),
        sa.CheckConstraint(
            "rank >= 1",
            name="ck_beneficiary_scores_rank_positive",
        ),
    )

    op.create_index(
        "ix_beneficiary_scores_as_of_date",
        "beneficiary_scores",
        ["as_of_date"],
    )
    op.create_index(
        "ix_beneficiary_scores_victim",
        "beneficiary_scores",
        ["victim_entity_type", "victim_entity_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_beneficiary_scores_victim", table_name="beneficiary_scores")
    op.drop_index("ix_beneficiary_scores_as_of_date", table_name="beneficiary_scores")
    op.drop_table("beneficiary_scores")
