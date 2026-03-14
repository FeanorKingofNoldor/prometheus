"""synthetic scenario tables

Revision ID: 0014
Revises: 0013
Create Date: 2025-11-25

This migration creates tables in the runtime database used by the
Synthetic Scenario Engine (170_spec):

- scenario_sets
- scenario_paths
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "0014"
down_revision: Union[str, None] = "0013"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create scenario_sets and scenario_paths tables."""

    op.create_table(
        "scenario_sets",
        sa.Column("scenario_set_id", sa.String(length=64), primary_key=True),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("description", sa.Text, nullable=True),
        sa.Column("category", sa.String(length=64), nullable=False),
        sa.Column("horizon_days", sa.Integer, nullable=False),
        sa.Column("num_paths", sa.Integer, nullable=False),
        sa.Column("base_universe_filter", postgresql.JSONB, nullable=True),
        sa.Column("base_date_start", sa.Date, nullable=True),
        sa.Column("base_date_end", sa.Date, nullable=True),
        sa.Column("regime_filter", postgresql.ARRAY(sa.String(length=64)), nullable=True),
        sa.Column("generator_spec", postgresql.JSONB, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("created_by", sa.String(length=64), nullable=True),
        sa.Column("tags", postgresql.ARRAY(sa.String(length=64)), nullable=True),
        sa.Column("metadata", postgresql.JSONB, nullable=True),
    )

    op.create_index(
        "idx_scenario_sets_category",
        "scenario_sets",
        ["category"],
        unique=False,
    )

    op.create_table(
        "scenario_paths",
        sa.Column("scenario_set_id", sa.String(length=64), nullable=False),
        sa.Column("scenario_id", sa.Integer, nullable=False),
        sa.Column("horizon_index", sa.Integer, nullable=False),
        sa.Column("instrument_id", sa.String(length=64), nullable=True),
        sa.Column("factor_id", sa.String(length=64), nullable=True),
        sa.Column("macro_id", sa.String(length=64), nullable=True),
        sa.Column("return_value", sa.Float, nullable=False),
        sa.Column("price", sa.Float, nullable=True),
        sa.Column("shock_metadata", postgresql.JSONB, nullable=True),
        sa.ForeignKeyConstraint(
            ["scenario_set_id"],
            ["scenario_sets.scenario_set_id"],
            name="fk_scenario_paths_set",
            ondelete="CASCADE",
        ),
    )

    op.create_primary_key(
        "pk_scenario_paths",
        "scenario_paths",
        [
            "scenario_set_id",
            "scenario_id",
            "horizon_index",
            "instrument_id",
            "factor_id",
            "macro_id",
        ],
    )

    op.create_index(
        "idx_scenario_paths_set_scenario",
        "scenario_paths",
        ["scenario_set_id", "scenario_id"],
        unique=False,
    )


def downgrade() -> None:
    """Drop scenario_paths and scenario_sets tables."""

    op.drop_index("idx_scenario_paths_set_scenario", table_name="scenario_paths")
    op.drop_constraint("pk_scenario_paths", "scenario_paths", type_="primary")
    op.drop_table("scenario_paths")

    op.drop_index("idx_scenario_sets_category", table_name="scenario_sets")
    op.drop_table("scenario_sets")
