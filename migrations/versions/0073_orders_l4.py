"""Layer 4: tighten orders contracts

Revision ID: 0073_orders_l4
Revises: 0072_executed_actions_l3
Create Date: 2025-12-16

Layer 4 contract for ``orders``:
- order_id/instrument_id/side/order_type/status/mode are non-empty
- side is in an allowed set (BUY/SELL)
- order_type is in an allowed set (MARKET/LIMIT/STOP/STOP_LIMIT)
- status is in an allowed set (PENDING/SUBMITTED/FILLED/CANCELLED/REJECTED)
- mode is in an allowed set (LIVE/PAPER/BACKTEST)
- quantity is finite and > 0
- limit_price/stop_price are finite when present
- portfolio_id/decision_id are either NULL or non-empty
- metadata is either NULL or a JSON object

Note: order/fill reconciliation is higher-level auditing.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "0073_orders_l4"
down_revision: Union[str, None] = "0072_executed_actions_l3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_NONFINITE = "('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision)"


def upgrade() -> None:
    op.create_check_constraint(
        "ck_orders_order_id_nonempty",
        "orders",
        "btrim(order_id) <> ''",
    )

    op.create_check_constraint(
        "ck_orders_instrument_id_nonempty",
        "orders",
        "btrim(instrument_id) <> ''",
    )

    op.create_check_constraint(
        "ck_orders_side_nonempty",
        "orders",
        "btrim(side) <> ''",
    )

    op.create_check_constraint(
        "ck_orders_side_allowed",
        "orders",
        "side IN ('BUY', 'SELL')",
    )

    op.create_check_constraint(
        "ck_orders_order_type_nonempty",
        "orders",
        "btrim(order_type) <> ''",
    )

    op.create_check_constraint(
        "ck_orders_order_type_allowed",
        "orders",
        "order_type IN ('MARKET', 'LIMIT', 'STOP', 'STOP_LIMIT')",
    )

    op.create_check_constraint(
        "ck_orders_status_nonempty",
        "orders",
        "btrim(status) <> ''",
    )

    op.create_check_constraint(
        "ck_orders_status_allowed",
        "orders",
        "status IN ('PENDING', 'SUBMITTED', 'FILLED', 'CANCELLED', 'REJECTED')",
    )

    op.create_check_constraint(
        "ck_orders_mode_nonempty",
        "orders",
        "btrim(mode) <> ''",
    )

    op.create_check_constraint(
        "ck_orders_mode_allowed",
        "orders",
        "mode IN ('LIVE', 'PAPER', 'BACKTEST')",
    )

    op.create_check_constraint(
        "ck_orders_quantity_finite",
        "orders",
        f"quantity NOT IN {_NONFINITE}",
    )

    op.create_check_constraint(
        "ck_orders_quantity_positive",
        "orders",
        "quantity > 0.0",
    )

    op.create_check_constraint(
        "ck_orders_limit_price_finite_when_present",
        "orders",
        f"limit_price IS NULL OR limit_price NOT IN {_NONFINITE}",
    )

    op.create_check_constraint(
        "ck_orders_stop_price_finite_when_present",
        "orders",
        f"stop_price IS NULL OR stop_price NOT IN {_NONFINITE}",
    )

    op.create_check_constraint(
        "ck_orders_portfolio_id_nonempty_when_present",
        "orders",
        "portfolio_id IS NULL OR btrim(portfolio_id) <> ''",
    )

    op.create_check_constraint(
        "ck_orders_decision_id_nonempty_when_present",
        "orders",
        "decision_id IS NULL OR btrim(decision_id) <> ''",
    )

    op.create_check_constraint(
        "ck_orders_metadata_object_or_null",
        "orders",
        "metadata IS NULL OR jsonb_typeof(metadata) = 'object'",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_orders_metadata_object_or_null",
        "orders",
        type_="check",
    )
    op.drop_constraint(
        "ck_orders_decision_id_nonempty_when_present",
        "orders",
        type_="check",
    )
    op.drop_constraint(
        "ck_orders_portfolio_id_nonempty_when_present",
        "orders",
        type_="check",
    )
    op.drop_constraint(
        "ck_orders_stop_price_finite_when_present",
        "orders",
        type_="check",
    )
    op.drop_constraint(
        "ck_orders_limit_price_finite_when_present",
        "orders",
        type_="check",
    )
    op.drop_constraint(
        "ck_orders_quantity_positive",
        "orders",
        type_="check",
    )
    op.drop_constraint(
        "ck_orders_quantity_finite",
        "orders",
        type_="check",
    )
    op.drop_constraint(
        "ck_orders_mode_allowed",
        "orders",
        type_="check",
    )
    op.drop_constraint(
        "ck_orders_mode_nonempty",
        "orders",
        type_="check",
    )
    op.drop_constraint(
        "ck_orders_status_allowed",
        "orders",
        type_="check",
    )
    op.drop_constraint(
        "ck_orders_status_nonempty",
        "orders",
        type_="check",
    )
    op.drop_constraint(
        "ck_orders_order_type_allowed",
        "orders",
        type_="check",
    )
    op.drop_constraint(
        "ck_orders_order_type_nonempty",
        "orders",
        type_="check",
    )
    op.drop_constraint(
        "ck_orders_side_allowed",
        "orders",
        type_="check",
    )
    op.drop_constraint(
        "ck_orders_side_nonempty",
        "orders",
        type_="check",
    )
    op.drop_constraint(
        "ck_orders_instrument_id_nonempty",
        "orders",
        type_="check",
    )
    op.drop_constraint(
        "ck_orders_order_id_nonempty",
        "orders",
        type_="check",
    )
