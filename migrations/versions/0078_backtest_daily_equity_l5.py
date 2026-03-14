"""Layer 5: tighten backtest_daily_equity contracts

Revision ID: 0078_backtest_daily_equity_l5
Revises: 0077_backtest_trades_l5
Create Date: 2025-12-16

Layer 5 contract for ``backtest_daily_equity``:
- run_id is non-empty
- equity_curve_value is finite
- drawdown is finite when present
- exposure_metrics_json is either NULL or a JSON object
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op


revision: str = "0078_backtest_daily_equity_l5"
down_revision: Union[str, None] = "0077_backtest_trades_l5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_NONFINITE = "('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision)"


def upgrade() -> None:
    op.create_check_constraint(
        "ck_backtest_daily_equity_run_id_nonempty",
        "backtest_daily_equity",
        "btrim(run_id) <> ''",
    )

    op.create_check_constraint(
        "ck_backtest_daily_equity_equity_curve_value_finite",
        "backtest_daily_equity",
        f"equity_curve_value NOT IN {_NONFINITE}",
    )

    op.create_check_constraint(
        "ck_backtest_daily_equity_drawdown_finite_when_present",
        "backtest_daily_equity",
        f"drawdown IS NULL OR drawdown NOT IN {_NONFINITE}",
    )

    op.create_check_constraint(
        "ck_backtest_daily_equity_exposure_metrics_json_object_or_null",
        "backtest_daily_equity",
        "exposure_metrics_json IS NULL OR jsonb_typeof(exposure_metrics_json) = 'object'",
    )


def downgrade() -> None:
    op.drop_constraint("ck_backtest_daily_equity_exposure_metrics_json_object_or_null", "backtest_daily_equity", type_="check")
    op.drop_constraint("ck_backtest_daily_equity_drawdown_finite_when_present", "backtest_daily_equity", type_="check")
    op.drop_constraint("ck_backtest_daily_equity_equity_curve_value_finite", "backtest_daily_equity", type_="check")
    op.drop_constraint("ck_backtest_daily_equity_run_id_nonempty", "backtest_daily_equity", type_="check")
