"""Layer 1: tighten prices_daily contracts

Revision ID: 0040_prices_daily_l1
Revises: 0039_engine_runs_l0
Create Date: 2025-12-16

Layer 1 contract for ``prices_daily``:
- unique per (instrument_id, trade_date) (already enforced by PK)
- prices and volume are non-negative
- OHLC are internally consistent (high >= low; open/close within [low, high])
- currency values are consistently formatted

Note: calendar correctness (trade_date matches market calendar) is validated
via higher-level audits rather than a hard DB constraint.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "0040_prices_daily_l1"
down_revision: Union[str, None] = "0039_engine_runs_l0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Non-empty identifiers.
    op.create_check_constraint(
        "ck_prices_daily_instrument_id_nonempty",
        "prices_daily",
        "btrim(instrument_id) <> ''",
    )

    # Currency format.
    op.create_check_constraint(
        "ck_prices_daily_currency_nonempty",
        "prices_daily",
        "currency IS NOT NULL AND btrim(currency) <> ''",
    )
    op.create_check_constraint(
        "ck_prices_daily_currency_format",
        "prices_daily",
        "currency ~ '^[A-Z]{3}$'",
    )

    # Non-negative prices and volume.
    op.create_check_constraint(
        "ck_prices_daily_open_ge_0",
        "prices_daily",
        "open >= 0",
    )
    op.create_check_constraint(
        "ck_prices_daily_high_ge_0",
        "prices_daily",
        "high >= 0",
    )
    op.create_check_constraint(
        "ck_prices_daily_low_ge_0",
        "prices_daily",
        "low >= 0",
    )
    op.create_check_constraint(
        "ck_prices_daily_close_ge_0",
        "prices_daily",
        "close >= 0",
    )
    op.create_check_constraint(
        "ck_prices_daily_adjusted_close_ge_0",
        "prices_daily",
        "adjusted_close >= 0",
    )
    op.create_check_constraint(
        "ck_prices_daily_volume_ge_0",
        "prices_daily",
        "volume >= 0",
    )

    # OHLC internal consistency.
    op.create_check_constraint(
        "ck_prices_daily_high_ge_low",
        "prices_daily",
        "high >= low",
    )
    op.create_check_constraint(
        "ck_prices_daily_open_in_range",
        "prices_daily",
        "open >= low AND open <= high",
    )
    op.create_check_constraint(
        "ck_prices_daily_close_in_range",
        "prices_daily",
        "close >= low AND close <= high",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_prices_daily_close_in_range",
        "prices_daily",
        type_="check",
    )
    op.drop_constraint(
        "ck_prices_daily_open_in_range",
        "prices_daily",
        type_="check",
    )
    op.drop_constraint(
        "ck_prices_daily_high_ge_low",
        "prices_daily",
        type_="check",
    )

    op.drop_constraint(
        "ck_prices_daily_volume_ge_0",
        "prices_daily",
        type_="check",
    )
    op.drop_constraint(
        "ck_prices_daily_adjusted_close_ge_0",
        "prices_daily",
        type_="check",
    )
    op.drop_constraint(
        "ck_prices_daily_close_ge_0",
        "prices_daily",
        type_="check",
    )
    op.drop_constraint(
        "ck_prices_daily_low_ge_0",
        "prices_daily",
        type_="check",
    )
    op.drop_constraint(
        "ck_prices_daily_high_ge_0",
        "prices_daily",
        type_="check",
    )
    op.drop_constraint(
        "ck_prices_daily_open_ge_0",
        "prices_daily",
        type_="check",
    )

    op.drop_constraint(
        "ck_prices_daily_currency_format",
        "prices_daily",
        type_="check",
    )
    op.drop_constraint(
        "ck_prices_daily_currency_nonempty",
        "prices_daily",
        type_="check",
    )

    op.drop_constraint(
        "ck_prices_daily_instrument_id_nonempty",
        "prices_daily",
        type_="check",
    )
