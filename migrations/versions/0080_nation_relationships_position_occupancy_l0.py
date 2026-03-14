"""Layer 0: add nation_relationships and position_occupancy tables

Revision ID: 0080_nation_relationships_position_occupancy_l0
Revises: 0079_decision_outcomes_l5
Create Date: 2026-02-24

This migration adds the runtime tables required by the Nation Profile Engine
(spec 036 / data model 020):

- nation_relationships: directed weighted edges between sovereigns
- position_occupancy: mapping from stable POSITION roles to occupants over time
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "0080_nation_relationships_position_occupancy_l0"
down_revision: Union[str, None] = "0079_decision_outcomes_l5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ------------------------------------------------------------------
    # nation_relationships
    # ------------------------------------------------------------------
    op.create_table(
        "nation_relationships",
        sa.Column("relationship_id", sa.String(length=64), primary_key=True),
        sa.Column("as_of_date", sa.Date, nullable=False),
        sa.Column("nation_a", sa.String(length=16), nullable=False),
        sa.Column("nation_b", sa.String(length=16), nullable=False),
        sa.Column("trade_dependency", sa.Float, nullable=False, server_default=sa.text("0")),
        sa.Column(
            "investment_dependency",
            sa.Float,
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column("energy_dependency", sa.Float, nullable=False, server_default=sa.text("0")),
        sa.Column(
            "financial_dependency",
            sa.Float,
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "supply_chain_dependency",
            sa.Float,
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "diplomatic_relationship",
            sa.Float,
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column("diplomatic_trend", sa.String(length=32), nullable=True),
        sa.Column(
            "contagion_channel_strength",
            sa.Float,
            nullable=False,
            server_default=sa.text("0"),
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

    # Uniqueness per (as_of_date, nation_a, nation_b).
    op.create_index(
        "ux_nation_relationships_asof_a_b",
        "nation_relationships",
        ["as_of_date", "nation_a", "nation_b"],
        unique=True,
    )

    # Helpful directional indexes.
    op.create_index(
        "idx_nation_relationships_a_asof",
        "nation_relationships",
        ["nation_a", "as_of_date"],
    )
    op.create_index(
        "idx_nation_relationships_b_asof",
        "nation_relationships",
        ["nation_b", "as_of_date"],
    )

    # Basic Layer-0 checks.
    op.create_check_constraint(
        "ck_nation_relationships_nation_a_nonempty",
        "nation_relationships",
        "btrim(nation_a) <> ''",
    )
    op.create_check_constraint(
        "ck_nation_relationships_nation_b_nonempty",
        "nation_relationships",
        "btrim(nation_b) <> ''",
    )
    op.create_check_constraint(
        "ck_nation_relationships_no_self_edges",
        "nation_relationships",
        "nation_a <> nation_b",
    )

    # Dependency weights in [0, 1].
    for col in [
        "trade_dependency",
        "investment_dependency",
        "energy_dependency",
        "financial_dependency",
        "supply_chain_dependency",
        "contagion_channel_strength",
    ]:
        op.create_check_constraint(
            f"ck_nation_relationships_{col}_bounds",
            "nation_relationships",
            f"{col} >= 0.0 AND {col} <= 1.0",
        )

    # Diplomatic relationship in [-1, 1].
    op.create_check_constraint(
        "ck_nation_relationships_diplomatic_relationship_bounds",
        "nation_relationships",
        "diplomatic_relationship >= -1.0 AND diplomatic_relationship <= 1.0",
    )

    # metadata is object when present.
    op.create_check_constraint(
        "ck_nation_relationships_metadata_object_or_null",
        "nation_relationships",
        "metadata IS NULL OR jsonb_typeof(metadata) = 'object'",
    )

    # ------------------------------------------------------------------
    # position_occupancy
    # ------------------------------------------------------------------
    op.create_table(
        "position_occupancy",
        sa.Column("occupancy_id", sa.String(length=64), primary_key=True),
        sa.Column("position_id", sa.String(length=64), nullable=False),
        sa.Column("person_name", sa.String(length=200), nullable=False),
        sa.Column("nation", sa.String(length=16), nullable=True),
        sa.Column("start_date", sa.Date, nullable=False),
        sa.Column("end_date", sa.Date, nullable=True),
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
        "ux_position_occupancy_position_start",
        "position_occupancy",
        ["position_id", "start_date"],
        unique=True,
    )
    op.create_index(
        "idx_position_occupancy_position",
        "position_occupancy",
        ["position_id"],
    )

    op.create_check_constraint(
        "ck_position_occupancy_position_id_nonempty",
        "position_occupancy",
        "btrim(position_id) <> ''",
    )
    op.create_check_constraint(
        "ck_position_occupancy_person_name_nonempty",
        "position_occupancy",
        "btrim(person_name) <> ''",
    )
    op.create_check_constraint(
        "ck_position_occupancy_nation_nonempty_when_present",
        "position_occupancy",
        "nation IS NULL OR btrim(nation) <> ''",
    )
    op.create_check_constraint(
        "ck_position_occupancy_end_date_after_start",
        "position_occupancy",
        "end_date IS NULL OR end_date >= start_date",
    )
    op.create_check_constraint(
        "ck_position_occupancy_metadata_object_or_null",
        "position_occupancy",
        "metadata IS NULL OR jsonb_typeof(metadata) = 'object'",
    )


def downgrade() -> None:
    # position_occupancy
    op.drop_constraint(
        "ck_position_occupancy_metadata_object_or_null",
        "position_occupancy",
        type_="check",
    )
    op.drop_constraint(
        "ck_position_occupancy_end_date_after_start",
        "position_occupancy",
        type_="check",
    )
    op.drop_constraint(
        "ck_position_occupancy_nation_nonempty_when_present",
        "position_occupancy",
        type_="check",
    )
    op.drop_constraint(
        "ck_position_occupancy_person_name_nonempty",
        "position_occupancy",
        type_="check",
    )
    op.drop_constraint(
        "ck_position_occupancy_position_id_nonempty",
        "position_occupancy",
        type_="check",
    )

    op.drop_index("idx_position_occupancy_position", table_name="position_occupancy")
    op.drop_index("ux_position_occupancy_position_start", table_name="position_occupancy")
    op.drop_table("position_occupancy")

    # nation_relationships
    op.drop_constraint(
        "ck_nation_relationships_metadata_object_or_null",
        "nation_relationships",
        type_="check",
    )
    op.drop_constraint(
        "ck_nation_relationships_diplomatic_relationship_bounds",
        "nation_relationships",
        type_="check",
    )

    for col in [
        "trade_dependency",
        "investment_dependency",
        "energy_dependency",
        "financial_dependency",
        "supply_chain_dependency",
        "contagion_channel_strength",
    ]:
        op.drop_constraint(
            f"ck_nation_relationships_{col}_bounds",
            "nation_relationships",
            type_="check",
        )

    op.drop_constraint(
        "ck_nation_relationships_no_self_edges",
        "nation_relationships",
        type_="check",
    )
    op.drop_constraint(
        "ck_nation_relationships_nation_b_nonempty",
        "nation_relationships",
        type_="check",
    )
    op.drop_constraint(
        "ck_nation_relationships_nation_a_nonempty",
        "nation_relationships",
        type_="check",
    )

    op.drop_index("idx_nation_relationships_b_asof", table_name="nation_relationships")
    op.drop_index("idx_nation_relationships_a_asof", table_name="nation_relationships")
    op.drop_index("ux_nation_relationships_asof_a_b", table_name="nation_relationships")
    op.drop_table("nation_relationships")
