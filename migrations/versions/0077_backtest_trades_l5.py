"""Layer 5: tighten backtest_trades contracts

Revision ID: 0077_backtest_trades_l5
Revises: 0076_backtest_runs_l5
Create Date: 2025-12-16

Layer 5 contract for ``backtest_trades``:
- run_id/ticker/direction are non-empty
- direction is in an allowed set (BUY/SELL/LONG/SHORT)
- size/price are finite and > 0
- optional IDs are either NULL or non-empty
- decision_metadata_json is either NULL or a JSON object
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op


revision: str = "0077_backtest_trades_l5"
down_revision: Union[str, None] = "0076_backtest_runs_l5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_NONFINITE = "('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision)"


def upgrade() -> None:
    op.create_check_constraint(
        "ck_backtest_trades_run_id_nonempty",
        "backtest_trades",
        "btrim(run_id) <> ''",
    )

    op.create_check_constraint(
        "ck_backtest_trades_ticker_nonempty",
        "backtest_trades",
        "btrim(ticker) <> ''",
    )

    op.create_check_constraint(
        "ck_backtest_trades_direction_nonempty",
        "backtest_trades",
        "btrim(direction) <> ''",
    )

    op.create_check_constraint(
        "ck_backtest_trades_direction_allowed",
        "backtest_trades",
        "direction IN ('BUY', 'SELL', 'LONG', 'SHORT')",
    )

    op.create_check_constraint(
        "ck_backtest_trades_size_finite",
        "backtest_trades",
        f"size NOT IN {_NONFINITE}",
    )

    op.create_check_constraint(
        "ck_backtest_trades_size_positive",
        "backtest_trades",
        "size > 0.0",
    )

    op.create_check_constraint(
        "ck_backtest_trades_price_finite",
        "backtest_trades",
        f"price NOT IN {_NONFINITE}",
    )

    op.create_check_constraint(
        "ck_backtest_trades_price_positive",
        "backtest_trades",
        "price > 0.0",
    )

    op.create_check_constraint(
        "ck_backtest_trades_regime_id_nonempty_when_present",
        "backtest_trades",
        "regime_id IS NULL OR btrim(regime_id) <> ''",
    )

    op.create_check_constraint(
        "ck_backtest_trades_universe_id_nonempty_when_present",
        "backtest_trades",
        "universe_id IS NULL OR btrim(universe_id) <> ''",
    )

    op.create_check_constraint(
        "ck_backtest_trades_decision_metadata_json_object_or_null",
        "backtest_trades",
        "decision_metadata_json IS NULL OR jsonb_typeof(decision_metadata_json) = 'object'",
    )


def downgrade() -> None:
    op.drop_constraint("ck_backtest_trades_decision_metadata_json_object_or_null", "backtest_trades", type_="check")
    op.drop_constraint("ck_backtest_trades_universe_id_nonempty_when_present", "backtest_trades", type_="check")
    op.drop_constraint("ck_backtest_trades_regime_id_nonempty_when_present", "backtest_trades", type_="check")
    op.drop_constraint("ck_backtest_trades_price_positive", "backtest_trades", type_="check")
    op.drop_constraint("ck_backtest_trades_price_finite", "backtest_trades", type_="check")
    op.drop_constraint("ck_backtest_trades_size_positive", "backtest_trades", type_="check")
    op.drop_constraint("ck_backtest_trades_size_finite", "backtest_trades", type_="check")
    op.drop_constraint("ck_backtest_trades_direction_allowed", "backtest_trades", type_="check")
    op.drop_constraint("ck_backtest_trades_direction_nonempty", "backtest_trades", type_="check")
    op.drop_constraint("ck_backtest_trades_ticker_nonempty", "backtest_trades", type_="check")
    op.drop_constraint("ck_backtest_trades_run_id_nonempty", "backtest_trades", type_="check")
