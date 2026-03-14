"""Layer 2: tighten scenario_sets contracts

Revision ID: 0061_scenario_sets_l2
Revises: 0060_regime_transitions_l2
Create Date: 2025-12-16

Layer 2 contract for ``scenario_sets``:
- scenario_set_id/name/category are non-empty
- category is in an allowed set (case-insensitive)
- horizon_days > 0 and num_paths > 0
- base_date_start <= base_date_end when both present
- base_universe_filter/generator_spec/metadata are JSON objects when present
- created_by is either NULL or non-empty
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0061_scenario_sets_l2"
down_revision: Union[str, None] = "0060_regime_transitions_l2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_check_constraint(
        "ck_scenario_sets_scenario_set_id_nonempty",
        "scenario_sets",
        "btrim(scenario_set_id) <> ''",
    )

    op.create_check_constraint(
        "ck_scenario_sets_name_nonempty",
        "scenario_sets",
        "btrim(name) <> ''",
    )

    op.create_check_constraint(
        "ck_scenario_sets_category_nonempty",
        "scenario_sets",
        "btrim(category) <> ''",
    )

    op.create_check_constraint(
        "ck_scenario_sets_category_allowed",
        "scenario_sets",
        "upper(category) IN ('HISTORICAL', 'BOOTSTRAP', 'STRESSED')",
    )

    op.create_check_constraint(
        "ck_scenario_sets_horizon_days_positive",
        "scenario_sets",
        "horizon_days > 0",
    )

    op.create_check_constraint(
        "ck_scenario_sets_num_paths_positive",
        "scenario_sets",
        "num_paths > 0",
    )

    op.create_check_constraint(
        "ck_scenario_sets_base_date_window",
        "scenario_sets",
        "base_date_start IS NULL OR base_date_end IS NULL OR base_date_start <= base_date_end",
    )

    op.create_check_constraint(
        "ck_scenario_sets_base_universe_filter_object_or_null",
        "scenario_sets",
        "base_universe_filter IS NULL OR jsonb_typeof(base_universe_filter) = 'object'",
    )

    op.create_check_constraint(
        "ck_scenario_sets_generator_spec_object_or_null",
        "scenario_sets",
        "generator_spec IS NULL OR jsonb_typeof(generator_spec) = 'object'",
    )

    op.create_check_constraint(
        "ck_scenario_sets_metadata_object_or_null",
        "scenario_sets",
        "metadata IS NULL OR jsonb_typeof(metadata) = 'object'",
    )

    op.create_check_constraint(
        "ck_scenario_sets_created_by_nonempty_when_present",
        "scenario_sets",
        "created_by IS NULL OR btrim(created_by) <> ''",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_scenario_sets_created_by_nonempty_when_present",
        "scenario_sets",
        type_="check",
    )
    op.drop_constraint(
        "ck_scenario_sets_metadata_object_or_null",
        "scenario_sets",
        type_="check",
    )
    op.drop_constraint(
        "ck_scenario_sets_generator_spec_object_or_null",
        "scenario_sets",
        type_="check",
    )
    op.drop_constraint(
        "ck_scenario_sets_base_universe_filter_object_or_null",
        "scenario_sets",
        type_="check",
    )
    op.drop_constraint(
        "ck_scenario_sets_base_date_window",
        "scenario_sets",
        type_="check",
    )
    op.drop_constraint(
        "ck_scenario_sets_num_paths_positive",
        "scenario_sets",
        type_="check",
    )
    op.drop_constraint(
        "ck_scenario_sets_horizon_days_positive",
        "scenario_sets",
        type_="check",
    )
    op.drop_constraint(
        "ck_scenario_sets_category_allowed",
        "scenario_sets",
        type_="check",
    )
    op.drop_constraint(
        "ck_scenario_sets_category_nonempty",
        "scenario_sets",
        type_="check",
    )
    op.drop_constraint(
        "ck_scenario_sets_name_nonempty",
        "scenario_sets",
        type_="check",
    )
    op.drop_constraint(
        "ck_scenario_sets_scenario_set_id_nonempty",
        "scenario_sets",
        type_="check",
    )
