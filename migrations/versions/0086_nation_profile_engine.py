"""Nation Profile Engine tables

Revision ID: 0086_nation_profile_engine
Revises: 0085_options_backtest_persistence
Create Date: 2026-03-05

Adds three tables for the Nation Profile Engine (spec 036):

- nation_macro_indicators (historical_db): time-series macro data per
  nation/series with value, direction, and rate-of-change.
- person_profiles (runtime_db): LLM-maintained living profiles of key
  officials with policy stance, scores, and behavioral patterns.
- nation_scores (runtime_db): computed composite scores per nation per
  date (10 dimensions feeding into decision engines).
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = "0086_nation_profile_engine"
down_revision: Union[str, None] = "0085_options_backtest_persistence"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ------------------------------------------------------------------
    # nation_macro_indicators  (historical_db)
    # ------------------------------------------------------------------
    op.create_table(
        "nation_macro_indicators",
        sa.Column("nation", sa.String(length=16), nullable=False),
        sa.Column("series_id", sa.String(length=64), nullable=False),
        sa.Column("observation_date", sa.Date, nullable=False),
        sa.Column("value", sa.Float, nullable=False),
        sa.Column(
            "direction",
            sa.String(length=16),
            nullable=True,
        ),  # RISING / FALLING / FLAT
        sa.Column("rate_of_change", sa.Float, nullable=True),
        sa.Column("source", sa.String(length=32), nullable=False),
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
            "series_id",
            "observation_date",
            name="pk_nation_macro_indicators",
        ),
    )

    op.create_index(
        "idx_nation_macro_ind_nation_date",
        "nation_macro_indicators",
        ["nation", "observation_date"],
    )
    op.create_index(
        "idx_nation_macro_ind_series_date",
        "nation_macro_indicators",
        ["series_id", "observation_date"],
    )

    # Basic constraints.
    op.create_check_constraint(
        "ck_nation_macro_ind_nation_nonempty",
        "nation_macro_indicators",
        "btrim(nation) <> ''",
    )
    op.create_check_constraint(
        "ck_nation_macro_ind_series_id_nonempty",
        "nation_macro_indicators",
        "btrim(series_id) <> ''",
    )
    op.create_check_constraint(
        "ck_nation_macro_ind_source_nonempty",
        "nation_macro_indicators",
        "btrim(source) <> ''",
    )
    op.create_check_constraint(
        "ck_nation_macro_ind_direction_valid",
        "nation_macro_indicators",
        "direction IS NULL OR direction IN ('RISING', 'FALLING', 'FLAT')",
    )
    op.create_check_constraint(
        "ck_nation_macro_ind_metadata_object_or_null",
        "nation_macro_indicators",
        "metadata IS NULL OR jsonb_typeof(metadata) = 'object'",
    )

    # ------------------------------------------------------------------
    # person_profiles  (runtime_db)
    # ------------------------------------------------------------------
    op.create_table(
        "person_profiles",
        sa.Column("profile_id", sa.String(length=64), primary_key=True),
        sa.Column("person_name", sa.String(length=200), nullable=False),
        sa.Column("nation", sa.String(length=16), nullable=False),
        sa.Column("role", sa.String(length=64), nullable=False),
        sa.Column("role_tier", sa.Integer, nullable=False),
        sa.Column("in_role_since", sa.Date, nullable=False),
        sa.Column("expected_term_end", sa.Date, nullable=True),
        # Structured JSONB blobs for flexible nested data.
        sa.Column("policy_stance", postgresql.JSONB, nullable=True),
        sa.Column("scores", postgresql.JSONB, nullable=True),
        sa.Column("background", postgresql.JSONB, nullable=True),
        sa.Column("behavioral", postgresql.JSONB, nullable=True),
        sa.Column("recent_statements", postgresql.JSONB, nullable=True),
        sa.Column("confidence", sa.Float, nullable=False, server_default=sa.text("0.5")),
        sa.Column(
            "last_updated",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
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
    )

    op.create_index(
        "idx_person_profiles_nation_role",
        "person_profiles",
        ["nation", "role"],
    )
    op.create_index(
        "ux_person_profiles_nation_person",
        "person_profiles",
        ["nation", "person_name"],
        unique=True,
    )

    op.create_check_constraint(
        "ck_person_profiles_person_name_nonempty",
        "person_profiles",
        "btrim(person_name) <> ''",
    )
    op.create_check_constraint(
        "ck_person_profiles_nation_nonempty",
        "person_profiles",
        "btrim(nation) <> ''",
    )
    op.create_check_constraint(
        "ck_person_profiles_role_nonempty",
        "person_profiles",
        "btrim(role) <> ''",
    )
    op.create_check_constraint(
        "ck_person_profiles_role_tier_valid",
        "person_profiles",
        "role_tier >= 1 AND role_tier <= 3",
    )
    op.create_check_constraint(
        "ck_person_profiles_confidence_bounds",
        "person_profiles",
        "confidence >= 0.0 AND confidence <= 1.0",
    )
    op.create_check_constraint(
        "ck_person_profiles_metadata_object_or_null",
        "person_profiles",
        "metadata IS NULL OR jsonb_typeof(metadata) = 'object'",
    )

    # ------------------------------------------------------------------
    # nation_scores  (runtime_db)
    # ------------------------------------------------------------------
    op.create_table(
        "nation_scores",
        sa.Column("nation", sa.String(length=16), nullable=False),
        sa.Column("as_of_date", sa.Date, nullable=False),
        sa.Column("economic_stability", sa.Float, nullable=False),
        sa.Column("market_stability", sa.Float, nullable=False),
        sa.Column("currency_risk", sa.Float, nullable=False),
        sa.Column("political_stability", sa.Float, nullable=False),
        sa.Column("contagion_risk", sa.Float, nullable=False),
        sa.Column("policy_direction", postgresql.JSONB, nullable=True),
        sa.Column("leadership_risk", sa.Float, nullable=False),
        sa.Column("leadership_composite", sa.Float, nullable=False),
        sa.Column("opportunity_score", sa.Float, nullable=False),
        sa.Column("composite_risk", sa.Float, nullable=False),
        sa.Column("component_details", postgresql.JSONB, nullable=True),
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
            "as_of_date",
            name="pk_nation_scores",
        ),
    )

    op.create_index(
        "idx_nation_scores_nation_date",
        "nation_scores",
        ["nation", "as_of_date"],
    )

    op.create_check_constraint(
        "ck_nation_scores_nation_nonempty",
        "nation_scores",
        "btrim(nation) <> ''",
    )
    # All scores in [0, 1].
    for col in [
        "economic_stability",
        "market_stability",
        "currency_risk",
        "political_stability",
        "contagion_risk",
        "leadership_risk",
        "leadership_composite",
        "opportunity_score",
        "composite_risk",
    ]:
        op.create_check_constraint(
            f"ck_nation_scores_{col}_bounds",
            "nation_scores",
            f"{col} >= 0.0 AND {col} <= 1.0",
        )

    op.create_check_constraint(
        "ck_nation_scores_metadata_object_or_null",
        "nation_scores",
        "metadata IS NULL OR jsonb_typeof(metadata) = 'object'",
    )


def downgrade() -> None:
    # nation_scores
    op.drop_constraint(
        "ck_nation_scores_metadata_object_or_null",
        "nation_scores",
        type_="check",
    )
    for col in [
        "economic_stability",
        "market_stability",
        "currency_risk",
        "political_stability",
        "contagion_risk",
        "leadership_risk",
        "leadership_composite",
        "opportunity_score",
        "composite_risk",
    ]:
        op.drop_constraint(
            f"ck_nation_scores_{col}_bounds",
            "nation_scores",
            type_="check",
        )
    op.drop_constraint(
        "ck_nation_scores_nation_nonempty",
        "nation_scores",
        type_="check",
    )
    op.drop_index("idx_nation_scores_nation_date", table_name="nation_scores")
    op.drop_table("nation_scores")

    # person_profiles
    op.drop_constraint(
        "ck_person_profiles_metadata_object_or_null",
        "person_profiles",
        type_="check",
    )
    op.drop_constraint(
        "ck_person_profiles_confidence_bounds",
        "person_profiles",
        type_="check",
    )
    op.drop_constraint(
        "ck_person_profiles_role_tier_valid",
        "person_profiles",
        type_="check",
    )
    op.drop_constraint(
        "ck_person_profiles_role_nonempty",
        "person_profiles",
        type_="check",
    )
    op.drop_constraint(
        "ck_person_profiles_nation_nonempty",
        "person_profiles",
        type_="check",
    )
    op.drop_constraint(
        "ck_person_profiles_person_name_nonempty",
        "person_profiles",
        type_="check",
    )
    op.drop_index("ux_person_profiles_nation_person", table_name="person_profiles")
    op.drop_index("idx_person_profiles_nation_role", table_name="person_profiles")
    op.drop_table("person_profiles")

    # nation_macro_indicators
    op.drop_constraint(
        "ck_nation_macro_ind_metadata_object_or_null",
        "nation_macro_indicators",
        type_="check",
    )
    op.drop_constraint(
        "ck_nation_macro_ind_direction_valid",
        "nation_macro_indicators",
        type_="check",
    )
    op.drop_constraint(
        "ck_nation_macro_ind_source_nonempty",
        "nation_macro_indicators",
        type_="check",
    )
    op.drop_constraint(
        "ck_nation_macro_ind_series_id_nonempty",
        "nation_macro_indicators",
        type_="check",
    )
    op.drop_constraint(
        "ck_nation_macro_ind_nation_nonempty",
        "nation_macro_indicators",
        type_="check",
    )
    op.drop_index("idx_nation_macro_ind_series_date", table_name="nation_macro_indicators")
    op.drop_index("idx_nation_macro_ind_nation_date", table_name="nation_macro_indicators")
    op.drop_table("nation_macro_indicators")
