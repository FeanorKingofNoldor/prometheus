"""Layer 5: tighten backtest_runs contracts

Revision ID: 0076_backtest_runs_l5
Revises: 0075_positions_snapshots_l4
Create Date: 2025-12-16

Layer 5 contract for ``backtest_runs``:
- run_id/strategy_id are non-empty
- start_date <= end_date
- config_json is a JSON object
- metrics_json is either NULL or a JSON object
- universe_id is either NULL or non-empty

Note: backtest reproducibility is higher-level auditing.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "0076_backtest_runs_l5"
down_revision: Union[str, None] = "0075_positions_snapshots_l4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_check_constraint(
        "ck_backtest_runs_run_id_nonempty",
        "backtest_runs",
        "btrim(run_id) <> ''",
    )

    op.create_check_constraint(
        "ck_backtest_runs_strategy_id_nonempty",
        "backtest_runs",
        "btrim(strategy_id) <> ''",
    )

    op.create_check_constraint(
        "ck_backtest_runs_date_range_valid",
        "backtest_runs",
        "start_date <= end_date",
    )

    op.create_check_constraint(
        "ck_backtest_runs_config_json_object",
        "backtest_runs",
        "jsonb_typeof(config_json) = 'object'",
    )

    op.create_check_constraint(
        "ck_backtest_runs_universe_id_nonempty_when_present",
        "backtest_runs",
        "universe_id IS NULL OR btrim(universe_id) <> ''",
    )

    op.create_check_constraint(
        "ck_backtest_runs_metrics_json_object_or_null",
        "backtest_runs",
        "metrics_json IS NULL OR jsonb_typeof(metrics_json) = 'object'",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_backtest_runs_metrics_json_object_or_null",
        "backtest_runs",
        type_="check",
    )
    op.drop_constraint(
        "ck_backtest_runs_universe_id_nonempty_when_present",
        "backtest_runs",
        type_="check",
    )
    op.drop_constraint(
        "ck_backtest_runs_config_json_object",
        "backtest_runs",
        type_="check",
    )
    op.drop_constraint(
        "ck_backtest_runs_date_range_valid",
        "backtest_runs",
        type_="check",
    )
    op.drop_constraint(
        "ck_backtest_runs_strategy_id_nonempty",
        "backtest_runs",
        type_="check",
    )
    op.drop_constraint(
        "ck_backtest_runs_run_id_nonempty",
        "backtest_runs",
        type_="check",
    )
