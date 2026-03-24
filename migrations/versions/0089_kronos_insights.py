"""Reconcile deployed Kronos insights revision.

Revision ID: 0089_kronos_insights
Revises: 0088_meta_policy_controls
Create Date: 2026-03-22

This migration is intentionally schema-neutral in this repository state.
Its purpose is to align source-controlled Alembic lineage with existing
deployed databases currently stamped at this revision.
"""

from __future__ import annotations

from typing import Sequence, Union


# revision identifiers, used by Alembic.
revision: str = "0089_kronos_insights"
down_revision: Union[str, None] = "0088_meta_policy_controls"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """No-op reconciliation migration."""


def downgrade() -> None:
    """No-op reconciliation migration."""
