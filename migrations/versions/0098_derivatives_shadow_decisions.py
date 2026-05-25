"""Derivatives shadow-mode decision log.

Revision ID: 0098_derivatives_shadow_decisions
Revises: 0097_options_positions
Create Date: 2026-05-24

Single-table log of what the new sleeve runner *would* trade each day
while the legacy ``options_strategy.py`` classes continue to drive
actual execution. Used during Phases 2-4 to compare new vs old behavior
before each sleeve's cutover.

One row per (run, sleeve, template) — either kind='DIRECTIVE' with the
chosen contract, or kind='SKIP' with the explanation. This lets us
answer "for every template, every day, what happened and why?".
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0098_derivatives_shadow_decisions"
down_revision: Union[str, None] = "0097_options_positions"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "derivatives_shadow_decisions",
        sa.Column("decision_id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("run_id", sa.String(length=64), nullable=False),
        sa.Column("as_of_date", sa.Date, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
        sa.Column("sleeve", sa.String(length=16), nullable=False),
        sa.Column("template_name", sa.String(length=64), nullable=False),
        sa.Column("kind", sa.String(length=16), nullable=False),  # DIRECTIVE | SKIP
        # Snapshot of signals used (small; full snapshot lives elsewhere)
        sa.Column("nav", sa.Float, nullable=False),
        sa.Column("vix_level", sa.Float, nullable=True),
        sa.Column("mhi", sa.Float, nullable=True),
        # Directive contract (nullable for SKIP rows)
        sa.Column("underlying", sa.String(length=50), nullable=True),
        sa.Column("right", sa.String(length=2), nullable=True),
        sa.Column("expiry", sa.String(length=8), nullable=True),
        sa.Column("strike", sa.Float, nullable=True),
        sa.Column("quantity", sa.Integer, nullable=True),
        sa.Column("limit_price", sa.Float, nullable=True),
        sa.Column("iv_used", sa.Float, nullable=True),
        sa.Column("iv_source", sa.String(length=32), nullable=True),
        sa.Column("delta", sa.Float, nullable=True),
        sa.Column("estimated_premium_per_contract", sa.Float, nullable=True),
        # Sizing flags (nullable for SKIP rows)
        sa.Column("sizing_contracts", sa.Integer, nullable=True),
        sa.Column("sizing_capacity_bound", sa.Boolean, nullable=True),
        sa.Column("sizing_budget_bound", sa.Boolean, nullable=True),
        # Provenance + reason
        sa.Column("trigger_reason", sa.String(length=300), nullable=True),
        sa.Column("trigger_metadata_json", postgresql.JSONB, nullable=True),
        sa.Column("selection_trace_json", postgresql.JSONB, nullable=True),
        sa.Column("reason", sa.String(length=500), nullable=False),
        # Skip-only fields
        sa.Column("skip_reason", sa.String(length=32), nullable=True),
        sa.Column("skip_detail", sa.String(length=300), nullable=True),
    )

    op.create_index(
        "idx_dx_shadow_run_sleeve",
        "derivatives_shadow_decisions",
        ["run_id", "sleeve", "template_name"],
    )
    op.create_index(
        "idx_dx_shadow_date_sleeve",
        "derivatives_shadow_decisions",
        ["as_of_date", "sleeve", "kind"],
    )


def downgrade() -> None:
    op.drop_index("idx_dx_shadow_date_sleeve", table_name="derivatives_shadow_decisions")
    op.drop_index("idx_dx_shadow_run_sleeve", table_name="derivatives_shadow_decisions")
    op.drop_table("derivatives_shadow_decisions")
