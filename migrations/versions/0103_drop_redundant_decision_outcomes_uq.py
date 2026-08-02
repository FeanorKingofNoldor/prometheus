"""Drop redundant unique index on decision_outcomes.

Revision ID: 0103_drop_redundant_decision_outcomes_uq
Revises: 0102_decision_outcomes_unique_horizon
Create Date: 2026-06-10

0102 added ``uq_decision_outcomes_decision_horizon`` on the premise that no
unique arbiter existed for the ``ON CONFLICT (decision_id, horizon_days)``
upsert. That premise was wrong: a pre-existing UNIQUE CONSTRAINT
``uk_decision_outcomes_decision_horizon`` (out-of-band schema drift, absent
from every prior migration) already enforced uniqueness and served as the
arbiter — so ON CONFLICT was never actually broken in the live DB. ``uq_`` is
therefore a redundant duplicate unique index on a hot insert path. Drop it and
rely on the ``uk_`` constraint, which remains the single source of truth.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

revision: str = "0103_drop_redundant_decision_outcomes_uq"
down_revision: Union[str, None] = "0102_decision_outcomes_unique_horizon"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("DROP INDEX IF EXISTS uq_decision_outcomes_decision_horizon")


def downgrade() -> None:
    op.create_index(
        "uq_decision_outcomes_decision_horizon",
        "decision_outcomes",
        ["decision_id", "horizon_days"],
        unique=True,
    )
