"""Layer 1: tighten returns_daily contracts

Revision ID: 0041_returns_daily_l1
Revises: 0040_prices_daily_l1
Create Date: 2025-12-16

Layer 1 contract for ``returns_daily``:
- unique per (instrument_id, trade_date) (already enforced by PK)
- returns are numeric and finite (no NaN/Inf)
- returns are bounded below by -1.0 (cannot lose more than 100% on simple returns)

Note: consistency with prices_daily (exact recomputation equality) is validated
via higher-level audits rather than a hard DB constraint.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "0041_returns_daily_l1"
down_revision: Union[str, None] = "0040_prices_daily_l1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Non-empty identifiers.
    op.create_check_constraint(
        "ck_returns_daily_instrument_id_nonempty",
        "returns_daily",
        "btrim(instrument_id) <> ''",
    )

    # No NaN/Inf.
    op.create_check_constraint(
        "ck_returns_daily_ret_1d_finite",
        "returns_daily",
        "ret_1d NOT IN ('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision)",
    )
    op.create_check_constraint(
        "ck_returns_daily_ret_5d_finite",
        "returns_daily",
        "ret_5d NOT IN ('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision)",
    )
    op.create_check_constraint(
        "ck_returns_daily_ret_21d_finite",
        "returns_daily",
        "ret_21d NOT IN ('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision)",
    )

    # Lower bound.
    op.create_check_constraint(
        "ck_returns_daily_ret_1d_ge_neg1",
        "returns_daily",
        "ret_1d >= -1.0",
    )
    op.create_check_constraint(
        "ck_returns_daily_ret_5d_ge_neg1",
        "returns_daily",
        "ret_5d >= -1.0",
    )
    op.create_check_constraint(
        "ck_returns_daily_ret_21d_ge_neg1",
        "returns_daily",
        "ret_21d >= -1.0",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_returns_daily_ret_21d_ge_neg1",
        "returns_daily",
        type_="check",
    )
    op.drop_constraint(
        "ck_returns_daily_ret_5d_ge_neg1",
        "returns_daily",
        type_="check",
    )
    op.drop_constraint(
        "ck_returns_daily_ret_1d_ge_neg1",
        "returns_daily",
        type_="check",
    )

    op.drop_constraint(
        "ck_returns_daily_ret_21d_finite",
        "returns_daily",
        type_="check",
    )
    op.drop_constraint(
        "ck_returns_daily_ret_5d_finite",
        "returns_daily",
        type_="check",
    )
    op.drop_constraint(
        "ck_returns_daily_ret_1d_finite",
        "returns_daily",
        type_="check",
    )

    op.drop_constraint(
        "ck_returns_daily_instrument_id_nonempty",
        "returns_daily",
        type_="check",
    )
