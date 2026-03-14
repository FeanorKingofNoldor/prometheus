"""Layer 4: tighten fills contracts

Revision ID: 0074_fills_l4
Revises: 0073_orders_l4
Create Date: 2025-12-16

Layer 4 contract for ``fills``:
- fill_id/order_id/instrument_id/side/mode are non-empty
- side is in an allowed set (BUY/SELL)
- mode is in an allowed set (LIVE/PAPER/BACKTEST)
- quantity is finite and > 0
- price is finite and >= 0
- commission is finite when present
- metadata is either NULL or a JSON object

Note: order/fill reconciliation is higher-level auditing.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "0074_fills_l4"
down_revision: Union[str, None] = "0073_orders_l4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_NONFINITE = "('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision)"


def upgrade() -> None:
    op.create_check_constraint(
        "ck_fills_fill_id_nonempty",
        "fills",
        "btrim(fill_id) <> ''",
    )

    op.create_check_constraint(
        "ck_fills_order_id_nonempty",
        "fills",
        "btrim(order_id) <> ''",
    )

    op.create_check_constraint(
        "ck_fills_instrument_id_nonempty",
        "fills",
        "btrim(instrument_id) <> ''",
    )

    op.create_check_constraint(
        "ck_fills_side_nonempty",
        "fills",
        "btrim(side) <> ''",
    )

    op.create_check_constraint(
        "ck_fills_side_allowed",
        "fills",
        "side IN ('BUY', 'SELL')",
    )

    op.create_check_constraint(
        "ck_fills_mode_nonempty",
        "fills",
        "btrim(mode) <> ''",
    )

    op.create_check_constraint(
        "ck_fills_mode_allowed",
        "fills",
        "mode IN ('LIVE', 'PAPER', 'BACKTEST')",
    )

    op.create_check_constraint(
        "ck_fills_quantity_finite",
        "fills",
        f"quantity NOT IN {_NONFINITE}",
    )

    op.create_check_constraint(
        "ck_fills_quantity_positive",
        "fills",
        "quantity > 0.0",
    )

    op.create_check_constraint(
        "ck_fills_price_finite",
        "fills",
        f"price NOT IN {_NONFINITE}",
    )

    op.create_check_constraint(
        "ck_fills_price_nonnegative",
        "fills",
        "price >= 0.0",
    )

    op.create_check_constraint(
        "ck_fills_commission_finite_when_present",
        "fills",
        f"commission IS NULL OR commission NOT IN {_NONFINITE}",
    )

    op.create_check_constraint(
        "ck_fills_metadata_object_or_null",
        "fills",
        "metadata IS NULL OR jsonb_typeof(metadata) = 'object'",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_fills_metadata_object_or_null",
        "fills",
        type_="check",
    )
    op.drop_constraint(
        "ck_fills_commission_finite_when_present",
        "fills",
        type_="check",
    )
    op.drop_constraint(
        "ck_fills_price_nonnegative",
        "fills",
        type_="check",
    )
    op.drop_constraint(
        "ck_fills_price_finite",
        "fills",
        type_="check",
    )
    op.drop_constraint(
        "ck_fills_quantity_positive",
        "fills",
        type_="check",
    )
    op.drop_constraint(
        "ck_fills_quantity_finite",
        "fills",
        type_="check",
    )
    op.drop_constraint(
        "ck_fills_mode_allowed",
        "fills",
        type_="check",
    )
    op.drop_constraint(
        "ck_fills_mode_nonempty",
        "fills",
        type_="check",
    )
    op.drop_constraint(
        "ck_fills_side_allowed",
        "fills",
        type_="check",
    )
    op.drop_constraint(
        "ck_fills_side_nonempty",
        "fills",
        type_="check",
    )
    op.drop_constraint(
        "ck_fills_instrument_id_nonempty",
        "fills",
        type_="check",
    )
    op.drop_constraint(
        "ck_fills_order_id_nonempty",
        "fills",
        type_="check",
    )
    op.drop_constraint(
        "ck_fills_fill_id_nonempty",
        "fills",
        type_="check",
    )
