"""Layer 2: tighten scenario_paths contracts

Revision ID: 0062_scenario_paths_l2
Revises: 0061_scenario_sets_l2
Create Date: 2025-12-16

Layer 2 contract for ``scenario_paths``:
- ids are non-empty
- scenario_id/horizon_index are non-negative
- return_value is finite and >= -1.0
- price is either NULL or finite and >= 0.0
- shock_metadata is either NULL or a JSON object

Note: scenario semantics and calibration are higher-level audits.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0062_scenario_paths_l2"
down_revision: Union[str, None] = "0061_scenario_sets_l2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_NONFINITE = "('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision)"


def upgrade() -> None:
    op.create_check_constraint(
        "ck_scenario_paths_scenario_set_id_nonempty",
        "scenario_paths",
        "btrim(scenario_set_id) <> ''",
    )

    op.create_check_constraint(
        "ck_scenario_paths_scenario_id_nonnegative",
        "scenario_paths",
        "scenario_id >= 0",
    )

    op.create_check_constraint(
        "ck_scenario_paths_horizon_index_nonnegative",
        "scenario_paths",
        "horizon_index >= 0",
    )

    op.create_check_constraint(
        "ck_scenario_paths_instrument_id_nonempty",
        "scenario_paths",
        "btrim(instrument_id) <> ''",
    )

    op.create_check_constraint(
        "ck_scenario_paths_factor_id_nonempty",
        "scenario_paths",
        "btrim(factor_id) <> ''",
    )

    op.create_check_constraint(
        "ck_scenario_paths_macro_id_nonempty",
        "scenario_paths",
        "btrim(macro_id) <> ''",
    )

    op.create_check_constraint(
        "ck_scenario_paths_return_value_finite",
        "scenario_paths",
        f"return_value NOT IN {_NONFINITE}",
    )

    op.create_check_constraint(
        "ck_scenario_paths_return_value_ge_neg1",
        "scenario_paths",
        "return_value >= -1.0",
    )

    op.create_check_constraint(
        "ck_scenario_paths_price_finite_when_present",
        "scenario_paths",
        f"price IS NULL OR price NOT IN {_NONFINITE}",
    )

    op.create_check_constraint(
        "ck_scenario_paths_price_nonnegative_when_present",
        "scenario_paths",
        "price IS NULL OR price >= 0.0",
    )

    op.create_check_constraint(
        "ck_scenario_paths_shock_metadata_object_or_null",
        "scenario_paths",
        "shock_metadata IS NULL OR jsonb_typeof(shock_metadata) = 'object'",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_scenario_paths_shock_metadata_object_or_null",
        "scenario_paths",
        type_="check",
    )
    op.drop_constraint(
        "ck_scenario_paths_price_nonnegative_when_present",
        "scenario_paths",
        type_="check",
    )
    op.drop_constraint(
        "ck_scenario_paths_price_finite_when_present",
        "scenario_paths",
        type_="check",
    )
    op.drop_constraint(
        "ck_scenario_paths_return_value_ge_neg1",
        "scenario_paths",
        type_="check",
    )
    op.drop_constraint(
        "ck_scenario_paths_return_value_finite",
        "scenario_paths",
        type_="check",
    )
    op.drop_constraint(
        "ck_scenario_paths_macro_id_nonempty",
        "scenario_paths",
        type_="check",
    )
    op.drop_constraint(
        "ck_scenario_paths_factor_id_nonempty",
        "scenario_paths",
        type_="check",
    )
    op.drop_constraint(
        "ck_scenario_paths_instrument_id_nonempty",
        "scenario_paths",
        type_="check",
    )
    op.drop_constraint(
        "ck_scenario_paths_horizon_index_nonnegative",
        "scenario_paths",
        type_="check",
    )
    op.drop_constraint(
        "ck_scenario_paths_scenario_id_nonnegative",
        "scenario_paths",
        type_="check",
    )
    op.drop_constraint(
        "ck_scenario_paths_scenario_set_id_nonempty",
        "scenario_paths",
        type_="check",
    )
