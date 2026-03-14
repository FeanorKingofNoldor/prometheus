"""Layer 4: tighten positions_snapshots contracts

Revision ID: 0075_positions_snapshots_l4
Revises: 0074_fills_l4
Create Date: 2025-12-16

Layer 4 contract for ``positions_snapshots``:
- portfolio_id/instrument_id/mode are non-empty
- mode is in an allowed set (LIVE/PAPER/BACKTEST)
- quantity/avg_cost/market_value/unrealized_pnl are finite

Note: position arithmetic consistency is higher-level auditing.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "0075_positions_snapshots_l4"
down_revision: Union[str, None] = "0074_fills_l4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_NONFINITE = "('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision)"


def upgrade() -> None:
    op.create_check_constraint(
        "ck_positions_snapshots_portfolio_id_nonempty",
        "positions_snapshots",
        "btrim(portfolio_id) <> ''",
    )

    op.create_check_constraint(
        "ck_positions_snapshots_instrument_id_nonempty",
        "positions_snapshots",
        "btrim(instrument_id) <> ''",
    )

    op.create_check_constraint(
        "ck_positions_snapshots_mode_nonempty",
        "positions_snapshots",
        "btrim(mode) <> ''",
    )

    op.create_check_constraint(
        "ck_positions_snapshots_mode_allowed",
        "positions_snapshots",
        "mode IN ('LIVE', 'PAPER', 'BACKTEST')",
    )

    op.create_check_constraint(
        "ck_positions_snapshots_quantity_finite",
        "positions_snapshots",
        f"quantity NOT IN {_NONFINITE}",
    )

    op.create_check_constraint(
        "ck_positions_snapshots_avg_cost_finite",
        "positions_snapshots",
        f"avg_cost NOT IN {_NONFINITE}",
    )

    op.create_check_constraint(
        "ck_positions_snapshots_market_value_finite",
        "positions_snapshots",
        f"market_value NOT IN {_NONFINITE}",
    )

    op.create_check_constraint(
        "ck_positions_snapshots_unrealized_pnl_finite",
        "positions_snapshots",
        f"unrealized_pnl NOT IN {_NONFINITE}",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_positions_snapshots_unrealized_pnl_finite",
        "positions_snapshots",
        type_="check",
    )
    op.drop_constraint(
        "ck_positions_snapshots_market_value_finite",
        "positions_snapshots",
        type_="check",
    )
    op.drop_constraint(
        "ck_positions_snapshots_avg_cost_finite",
        "positions_snapshots",
        type_="check",
    )
    op.drop_constraint(
        "ck_positions_snapshots_quantity_finite",
        "positions_snapshots",
        type_="check",
    )
    op.drop_constraint(
        "ck_positions_snapshots_mode_allowed",
        "positions_snapshots",
        type_="check",
    )
    op.drop_constraint(
        "ck_positions_snapshots_mode_nonempty",
        "positions_snapshots",
        type_="check",
    )
    op.drop_constraint(
        "ck_positions_snapshots_instrument_id_nonempty",
        "positions_snapshots",
        type_="check",
    )
    op.drop_constraint(
        "ck_positions_snapshots_portfolio_id_nonempty",
        "positions_snapshots",
        type_="check",
    )
