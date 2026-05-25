"""Add scenario_branches table.

Revision ID: 0095_scenario_branches
Revises: 0094_beneficiary_scores
Create Date: 2026-05-05

Persists the branches of an Apatheon scenario tree as a flat table the
options-strategy layer can query when weighting Greek allocation across
plausible futures.  Each row is one terminal-or-intermediate branch with
a probability; the ``tree_id`` groups branches that share a trigger.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "0095_scenario_branches"
down_revision: Union[str, None] = "0094_beneficiary_scores"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "scenario_branches",
        sa.Column("branch_id", sa.String(), primary_key=True),
        sa.Column("tree_id", sa.String(), nullable=False),
        sa.Column("as_of_date", sa.Date(), nullable=False),
        sa.Column("trigger_event", sa.Text(), nullable=False),
        sa.Column("trigger_entity_type", sa.String(), nullable=True),
        sa.Column("trigger_entity_id", sa.String(), nullable=True),
        sa.Column("depth", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("parent_branch_id", sa.String(), nullable=True),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("probability", sa.Float(), nullable=False),
        sa.Column(
            "affected_entities",
            sa.dialects.postgresql.JSONB(),
            nullable=False,
            server_default="[]",
        ),
        sa.Column(
            "metadata",
            sa.dialects.postgresql.JSONB(),
            nullable=False,
            server_default="{}",
        ),
        sa.Column("decision_id", sa.String(), nullable=True),
        sa.Column("computed_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "probability >= 0 AND probability <= 1",
            name="ck_scenario_branches_probability_range",
        ),
        sa.CheckConstraint(
            "depth >= 0",
            name="ck_scenario_branches_depth_nonneg",
        ),
    )

    op.create_index(
        "ix_scenario_branches_tree_id",
        "scenario_branches",
        ["tree_id"],
    )
    op.create_index(
        "ix_scenario_branches_trigger_entity",
        "scenario_branches",
        ["trigger_entity_type", "trigger_entity_id"],
    )
    op.create_index(
        "ix_scenario_branches_as_of_date",
        "scenario_branches",
        ["as_of_date"],
    )


def downgrade() -> None:
    op.drop_index("ix_scenario_branches_as_of_date", table_name="scenario_branches")
    op.drop_index("ix_scenario_branches_trigger_entity", table_name="scenario_branches")
    op.drop_index("ix_scenario_branches_tree_id", table_name="scenario_branches")
    op.drop_table("scenario_branches")
