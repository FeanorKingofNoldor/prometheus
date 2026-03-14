"""relax executed_actions decision_id foreign key

Revision ID: 0021
Revises: 0020
Create Date: 2025-11-29

This migration drops the strict foreign key constraint from
``executed_actions.decision_id`` to ``engine_decisions.decision_id``.

The execution layer may record executed_actions before a corresponding
engine_decisions row has been written (e.g. during backtests or for
trades that are not yet associated with a specific Meta decision). The
hard FK caused integrity errors in these cases. The logical link is
still preserved via the decision_id column when present, but is no
longer enforced at the database level.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "0021"
down_revision: Union[str, None] = "0020"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Drop FK constraint from executed_actions.decision_id."""

    op.drop_constraint(
        "fk_executed_actions_decision",
        "executed_actions",
        type_="foreignkey",
    )


def downgrade() -> None:
    """Recreate FK constraint from executed_actions to engine_decisions."""

    op.create_foreign_key(
        "fk_executed_actions_decision",
        "executed_actions",
        "engine_decisions",
        ["decision_id"],
        ["decision_id"],
    )
