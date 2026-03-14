"""Layer 1: tighten correlation_panels contracts

Revision ID: 0045_correlation_panels_l1
Revises: 0044_instrument_factors_daily_l1
Create Date: 2025-12-16

Layer 1 contract for ``correlation_panels``:
- panel_id is non-empty
- start_date <= end_date
- universe_spec is a JSON object
- matrix_ref is non-empty

Note: correctness of the referenced matrix artifact (shape, immutability)
requires higher-level audits.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0045_correlation_panels_l1"
down_revision: Union[str, None] = "0044_instrument_factors_daily_l1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_check_constraint(
        "ck_correlation_panels_panel_id_nonempty",
        "correlation_panels",
        "btrim(panel_id) <> ''",
    )

    op.create_check_constraint(
        "ck_correlation_panels_date_range_sane",
        "correlation_panels",
        "start_date <= end_date",
    )

    op.create_check_constraint(
        "ck_correlation_panels_matrix_ref_nonempty",
        "correlation_panels",
        "btrim(matrix_ref) <> ''",
    )

    op.create_check_constraint(
        "ck_correlation_panels_universe_spec_object",
        "correlation_panels",
        "jsonb_typeof(universe_spec) = 'object'",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_correlation_panels_universe_spec_object",
        "correlation_panels",
        type_="check",
    )
    op.drop_constraint(
        "ck_correlation_panels_matrix_ref_nonempty",
        "correlation_panels",
        type_="check",
    )
    op.drop_constraint(
        "ck_correlation_panels_date_range_sane",
        "correlation_panels",
        type_="check",
    )
    op.drop_constraint(
        "ck_correlation_panels_panel_id_nonempty",
        "correlation_panels",
        type_="check",
    )
