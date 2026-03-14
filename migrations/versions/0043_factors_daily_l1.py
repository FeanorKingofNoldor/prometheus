"""Layer 1: tighten factors_daily contracts

Revision ID: 0043_factors_daily_l1
Revises: 0042_volatility_daily_l1
Create Date: 2025-12-16

Layer 1 contract for ``factors_daily``:
- unique per (factor_id, trade_date) (already enforced by PK)
- factor_id is non-empty
- factor values are numeric and finite (no NaN/Inf)
- factor values are bounded below by -1.0

Note: factor semantics (what value represents and how it's computed) are
validated via code review and higher-level audits rather than a hard DB
constraint.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0043_factors_daily_l1"
down_revision: Union[str, None] = "0042_volatility_daily_l1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Non-empty identifiers.
    op.create_check_constraint(
        "ck_factors_daily_factor_id_nonempty",
        "factors_daily",
        "btrim(factor_id) <> ''",
    )

    # No NaN/Inf.
    op.create_check_constraint(
        "ck_factors_daily_value_finite",
        "factors_daily",
        "value NOT IN ('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision)",
    )

    # Lower bound.
    op.create_check_constraint(
        "ck_factors_daily_value_ge_neg1",
        "factors_daily",
        "value >= -1.0",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_factors_daily_value_ge_neg1",
        "factors_daily",
        type_="check",
    )
    op.drop_constraint(
        "ck_factors_daily_value_finite",
        "factors_daily",
        type_="check",
    )
    op.drop_constraint(
        "ck_factors_daily_factor_id_nonempty",
        "factors_daily",
        type_="check",
    )
