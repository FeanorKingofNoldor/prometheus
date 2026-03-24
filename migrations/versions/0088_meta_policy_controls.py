"""Reconcile missing migration revision 0088.

Revision ID: 0088_meta_policy_controls
Revises: 0087_nation_industry_health
Create Date: 2026-03-22

This revision is intentionally schema-neutral. It restores a missing
lineage node so environments that already advanced past 0087 can be
represented consistently in source control and historical databases can
upgrade through a continuous revision chain.
"""

from __future__ import annotations

from typing import Sequence, Union


# revision identifiers, used by Alembic.
revision: str = "0088_meta_policy_controls"
down_revision: Union[str, None] = "0087_nation_industry_health"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """No-op reconciliation migration."""


def downgrade() -> None:
    """No-op reconciliation migration."""
