"""Nation Industry Health time-series table

Revision ID: 0087_nation_industry_health
Revises: 0086_nation_profile_engine
Create Date: 2026-03-06

Tracks per-industry health scores for each nation over time.
Feeds into the nation scoring engine's structural/industry dimension.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = "0087_nation_industry_health"
down_revision: Union[str, None] = "0086_nation_profile_engine"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "nation_industry_health",
        sa.Column("nation", sa.String(length=16), nullable=False),
        sa.Column("industry", sa.String(length=64), nullable=False),
        sa.Column("as_of_date", sa.Date, nullable=False),
        sa.Column("health_score", sa.Float, nullable=False),
        sa.Column("pmi_component", sa.Float, nullable=True),
        sa.Column(
            "output_trend",
            sa.String(length=16),
            nullable=True,
        ),  # GROWING | STABLE | CONTRACTING
        sa.Column("regulatory_pressure", sa.Float, nullable=True),
        sa.Column("sentiment", sa.Float, nullable=True),
        sa.Column("growth_yoy_pct", sa.Float, nullable=True),
        sa.Column("metadata", postgresql.JSONB, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint(
            "nation",
            "industry",
            "as_of_date",
            name="pk_nation_industry_health",
        ),
    )

    op.create_index(
        "idx_nation_industry_health_nation_date",
        "nation_industry_health",
        ["nation", "as_of_date"],
    )

    # Constraints
    op.create_check_constraint(
        "ck_nation_industry_health_nation_nonempty",
        "nation_industry_health",
        "btrim(nation) <> ''",
    )
    op.create_check_constraint(
        "ck_nation_industry_health_industry_nonempty",
        "nation_industry_health",
        "btrim(industry) <> ''",
    )
    op.create_check_constraint(
        "ck_nation_industry_health_score_bounds",
        "nation_industry_health",
        "health_score >= 0.0 AND health_score <= 1.0",
    )
    op.create_check_constraint(
        "ck_nation_industry_health_reg_bounds",
        "nation_industry_health",
        "regulatory_pressure IS NULL OR (regulatory_pressure >= 0.0 AND regulatory_pressure <= 1.0)",
    )
    op.create_check_constraint(
        "ck_nation_industry_health_sentiment_bounds",
        "nation_industry_health",
        "sentiment IS NULL OR (sentiment >= -1.0 AND sentiment <= 1.0)",
    )
    op.create_check_constraint(
        "ck_nation_industry_health_trend_valid",
        "nation_industry_health",
        "output_trend IS NULL OR output_trend IN ('GROWING', 'STABLE', 'CONTRACTING')",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_nation_industry_health_trend_valid",
        "nation_industry_health",
        type_="check",
    )
    op.drop_constraint(
        "ck_nation_industry_health_sentiment_bounds",
        "nation_industry_health",
        type_="check",
    )
    op.drop_constraint(
        "ck_nation_industry_health_reg_bounds",
        "nation_industry_health",
        type_="check",
    )
    op.drop_constraint(
        "ck_nation_industry_health_score_bounds",
        "nation_industry_health",
        type_="check",
    )
    op.drop_constraint(
        "ck_nation_industry_health_industry_nonempty",
        "nation_industry_health",
        type_="check",
    )
    op.drop_constraint(
        "ck_nation_industry_health_nation_nonempty",
        "nation_industry_health",
        type_="check",
    )
    op.drop_index(
        "idx_nation_industry_health_nation_date",
        table_name="nation_industry_health",
    )
    op.drop_table("nation_industry_health")
