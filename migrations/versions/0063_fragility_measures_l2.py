"""Layer 2: tighten fragility_measures contracts

Revision ID: 0063_fragility_measures_l2
Revises: 0062_scenario_paths_l2
Create Date: 2025-12-16

Layer 2 contract for ``fragility_measures``:
- ids/types are non-empty
- fragility_score is finite and within [0, 1]
- scenario_losses/metadata are JSON objects when present
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0063_fragility_measures_l2"
down_revision: Union[str, None] = "0062_scenario_paths_l2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_NONFINITE = "('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision)"


def upgrade() -> None:
    op.create_check_constraint(
        "ck_fragility_measures_fragility_id_nonempty",
        "fragility_measures",
        "btrim(fragility_id) <> ''",
    )

    op.create_check_constraint(
        "ck_fragility_measures_entity_type_nonempty",
        "fragility_measures",
        "btrim(entity_type) <> ''",
    )

    op.create_check_constraint(
        "ck_fragility_measures_entity_id_nonempty",
        "fragility_measures",
        "btrim(entity_id) <> ''",
    )

    op.create_check_constraint(
        "ck_fragility_measures_fragility_score_finite",
        "fragility_measures",
        f"fragility_score NOT IN {_NONFINITE}",
    )

    op.create_check_constraint(
        "ck_fragility_measures_fragility_score_range_0_1",
        "fragility_measures",
        "fragility_score >= 0.0 AND fragility_score <= 1.0",
    )

    op.create_check_constraint(
        "ck_fragility_measures_scenario_losses_object_or_null",
        "fragility_measures",
        "scenario_losses IS NULL OR jsonb_typeof(scenario_losses) = 'object'",
    )

    op.create_check_constraint(
        "ck_fragility_measures_metadata_object_or_null",
        "fragility_measures",
        "metadata IS NULL OR jsonb_typeof(metadata) = 'object'",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_fragility_measures_metadata_object_or_null",
        "fragility_measures",
        type_="check",
    )
    op.drop_constraint(
        "ck_fragility_measures_scenario_losses_object_or_null",
        "fragility_measures",
        type_="check",
    )
    op.drop_constraint(
        "ck_fragility_measures_fragility_score_range_0_1",
        "fragility_measures",
        type_="check",
    )
    op.drop_constraint(
        "ck_fragility_measures_fragility_score_finite",
        "fragility_measures",
        type_="check",
    )
    op.drop_constraint(
        "ck_fragility_measures_entity_id_nonempty",
        "fragility_measures",
        type_="check",
    )
    op.drop_constraint(
        "ck_fragility_measures_entity_type_nonempty",
        "fragility_measures",
        type_="check",
    )
    op.drop_constraint(
        "ck_fragility_measures_fragility_id_nonempty",
        "fragility_measures",
        type_="check",
    )
