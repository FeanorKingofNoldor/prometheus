"""Notifications inbox for the autopilot feedback loop.

Revision ID: 0100_notifications
Revises: 0099_meta_analysis_tables
Create Date: 2026-05-25

One table that aggregates every thing a human reviewer should know
about: pending config proposals, critical feedback insights,
multi-day signal degradations, diagnostic underperformers, future
drift alerts. The frontend's notifications inbox reads from here.

Designed for the autopilot pattern: the daily DAG analyses + emits
notifications automatically; the human reviews them in the UI; the
human approves any proposals via the existing applicator workflow.

Each notification points back at its source row via
``(source_table, source_id)`` for deep-linking from the inbox UI.
``link_path`` is the frontend route the click-through opens.
Idempotency: a unique partial index on
``(as_of_date, kind, source_id)`` prevents duplicate alerts for the
same event on the same day.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0100_notifications"
down_revision: Union[str, None] = "0099_meta_analysis_tables"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "notifications",
        sa.Column("notification_id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("created_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
        sa.Column("as_of_date", sa.Date, nullable=False),
        sa.Column("kind", sa.String(length=48), nullable=False),
        sa.Column("severity", sa.String(length=16), nullable=False),
        sa.Column("title", sa.String(length=200), nullable=False),
        sa.Column("body", sa.Text, nullable=True),
        # Source pointer — which row drove this notification.
        sa.Column("source_table", sa.String(length=64), nullable=True),
        sa.Column("source_id", sa.String(length=64), nullable=True),
        # Frontend route the click-through navigates to.
        sa.Column("link_path", sa.String(length=200), nullable=True),
        # Read/dismissed state.
        sa.Column("read_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("dismissed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("metadata_json", postgresql.JSONB, nullable=True),
    )

    # Inbox queries: "everything unread", "newest first".
    op.create_index(
        "idx_notifications_unread",
        "notifications",
        ["created_at"],
        postgresql_where=sa.text("read_at IS NULL AND dismissed_at IS NULL"),
    )
    op.create_index(
        "idx_notifications_date_kind",
        "notifications",
        ["as_of_date", "kind"],
    )

    # Idempotency: don't double-record the same alert on the same day.
    # source_id may be NULL for system-wide alerts; in that case we
    # rely on (as_of_date, kind) instead, enforced via a separate
    # partial index.
    op.create_index(
        "ux_notifications_dedup_with_source",
        "notifications",
        ["as_of_date", "kind", "source_id"],
        unique=True,
        postgresql_where=sa.text("source_id IS NOT NULL"),
    )
    op.create_index(
        "ux_notifications_dedup_no_source",
        "notifications",
        ["as_of_date", "kind"],
        unique=True,
        postgresql_where=sa.text("source_id IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("ux_notifications_dedup_no_source", table_name="notifications")
    op.drop_index("ux_notifications_dedup_with_source", table_name="notifications")
    op.drop_index("idx_notifications_date_kind", table_name="notifications")
    op.drop_index("idx_notifications_unread", table_name="notifications")
    op.drop_table("notifications")
