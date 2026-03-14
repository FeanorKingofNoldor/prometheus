"""Layer 1: tighten instrument_factors_daily contracts

Revision ID: 0044_instrument_factors_daily_l1
Revises: 0043_factors_daily_l1
Create Date: 2025-12-16

Layer 1 contract for ``instrument_factors_daily``:
- unique per (instrument_id, trade_date, factor_id) (already enforced by PK)
- instrument_id and factor_id are non-empty
- exposures are numeric and finite (no NaN/Inf)

Note: referential integrity with factors_daily and downstream factor model
semantics are validated via higher-level audits rather than hard DB
constraints in this pass.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0044_instrument_factors_daily_l1"
down_revision: Union[str, None] = "0043_factors_daily_l1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Non-empty identifiers.
    op.create_check_constraint(
        "ck_instrument_factors_daily_instrument_id_nonempty",
        "instrument_factors_daily",
        "btrim(instrument_id) <> ''",
    )
    op.create_check_constraint(
        "ck_instrument_factors_daily_factor_id_nonempty",
        "instrument_factors_daily",
        "btrim(factor_id) <> ''",
    )

    # No NaN/Inf.
    op.create_check_constraint(
        "ck_instrument_factors_daily_exposure_finite",
        "instrument_factors_daily",
        "exposure NOT IN ('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision)",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_instrument_factors_daily_exposure_finite",
        "instrument_factors_daily",
        type_="check",
    )
    op.drop_constraint(
        "ck_instrument_factors_daily_factor_id_nonempty",
        "instrument_factors_daily",
        type_="check",
    )
    op.drop_constraint(
        "ck_instrument_factors_daily_instrument_id_nonempty",
        "instrument_factors_daily",
        type_="check",
    )
