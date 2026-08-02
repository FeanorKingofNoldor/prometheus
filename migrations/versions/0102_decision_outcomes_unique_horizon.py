"""Unique (decision_id, horizon_days) on decision_outcomes.

Revision ID: 0102_decision_outcomes_unique_horizon
Revises: 0101_backtest_live_drift
Create Date: 2026-06-10

``save_decision_outcome`` relies on ``ON CONFLICT (decision_id, horizon_days)``
for idempotent daily-eval re-runs. The original index
``idx_decision_outcomes_decision_horizon`` (migration 0018) was NOT unique, so
the ON CONFLICT clause had no arbiter and would raise at runtime. Replace it
with a unique index so the dedup is actually enforced by the DB.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

revision: str = "0102_decision_outcomes_unique_horizon"
down_revision: Union[str, None] = "0101_backtest_live_drift"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Drop pre-existing duplicate rows (keep the earliest) so the unique
    # index can be built. This is a no-op on a clean table.
    op.execute(
        """
        DELETE FROM decision_outcomes a
        USING decision_outcomes b
        WHERE a.decision_id = b.decision_id
          AND a.horizon_days = b.horizon_days
          AND a.outcome_id > b.outcome_id
        """
    )
    op.drop_index("idx_decision_outcomes_decision_horizon", table_name="decision_outcomes")
    op.create_index(
        "uq_decision_outcomes_decision_horizon",
        "decision_outcomes",
        ["decision_id", "horizon_days"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index("uq_decision_outcomes_decision_horizon", table_name="decision_outcomes")
    op.create_index(
        "idx_decision_outcomes_decision_horizon",
        "decision_outcomes",
        ["decision_id", "horizon_days"],
    )
