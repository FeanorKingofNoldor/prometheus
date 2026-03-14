"""Layer 1: tighten volatility_daily contracts

Revision ID: 0042_volatility_daily_l1
Revises: 0041_returns_daily_l1
Create Date: 2025-12-16

Layer 1 contract for ``volatility_daily``:
- unique per (instrument_id, trade_date) (already enforced by PK)
- volatility values are numeric and finite (no NaN/Inf)
- volatility values are non-negative

Note: window definitions (ddof conventions, annualisation) are validated via
code review and higher-level audits rather than a hard DB constraint.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0042_volatility_daily_l1"
down_revision: Union[str, None] = "0041_returns_daily_l1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Non-empty identifiers.
    op.create_check_constraint(
        "ck_volatility_daily_instrument_id_nonempty",
        "volatility_daily",
        "btrim(instrument_id) <> ''",
    )

    # No NaN/Inf.
    op.create_check_constraint(
        "ck_volatility_daily_vol_21d_finite",
        "volatility_daily",
        "vol_21d NOT IN ('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision)",
    )
    op.create_check_constraint(
        "ck_volatility_daily_vol_63d_finite",
        "volatility_daily",
        "vol_63d NOT IN ('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision)",
    )

    # Non-negative.
    op.create_check_constraint(
        "ck_volatility_daily_vol_21d_ge_0",
        "volatility_daily",
        "vol_21d >= 0.0",
    )
    op.create_check_constraint(
        "ck_volatility_daily_vol_63d_ge_0",
        "volatility_daily",
        "vol_63d >= 0.0",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_volatility_daily_vol_63d_ge_0",
        "volatility_daily",
        type_="check",
    )
    op.drop_constraint(
        "ck_volatility_daily_vol_21d_ge_0",
        "volatility_daily",
        type_="check",
    )
    op.drop_constraint(
        "ck_volatility_daily_vol_63d_finite",
        "volatility_daily",
        type_="check",
    )
    op.drop_constraint(
        "ck_volatility_daily_vol_21d_finite",
        "volatility_daily",
        type_="check",
    )
    op.drop_constraint(
        "ck_volatility_daily_instrument_id_nonempty",
        "volatility_daily",
        type_="check",
    )
