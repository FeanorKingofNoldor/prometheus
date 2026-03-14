"""Layer 0: make meta_config_proposals immutable (append-only) via events

Revision ID: 0036_meta_config_proposals_l0
Revises: 0035_config_change_log_l0
Create Date: 2025-12-16

Layer 0 contract for ``meta_config_proposals``:
- proposals are immutable
- must include a target config payload + rationale + author/source (tracked via metadata/events)

The original schema stored mutable workflow state (status/approved_by/approved_at/etc.)
inside the proposal row. This migration introduces an append-only events table to
represent proposal state transitions without mutating proposal rows.

Changes
-------
- meta_config_proposals:
  - enforce key non-empty invariants
  - enforce strategy_id non-null (current implementation is strategy-scoped)
  - prevent UPDATE/DELETE via trigger

- meta_config_proposal_events:
  - append-only table tracking CREATED/APPROVED/REJECTED/APPLIED/REVERTED events
  - backfills events for any existing proposals
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = "0036_meta_config_proposals_l0"
down_revision: Union[str, None] = "0035_config_change_log_l0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Ensure strategy_id is always present for proposals.
    op.alter_column(
        "meta_config_proposals",
        "strategy_id",
        existing_type=sa.String(length=64),
        nullable=False,
    )

    # Basic invariants.
    op.create_check_constraint(
        "ck_meta_config_proposals_proposal_id_nonempty",
        "meta_config_proposals",
        "btrim(proposal_id) <> ''",
    )
    op.create_check_constraint(
        "ck_meta_config_proposals_strategy_id_nonempty",
        "meta_config_proposals",
        "btrim(strategy_id) <> ''",
    )
    op.create_check_constraint(
        "ck_meta_config_proposals_proposal_type_nonempty",
        "meta_config_proposals",
        "btrim(proposal_type) <> ''",
    )
    op.create_check_constraint(
        "ck_meta_config_proposals_target_component_nonempty",
        "meta_config_proposals",
        "btrim(target_component) <> ''",
    )
    op.create_check_constraint(
        "ck_meta_config_proposals_confidence_score_range",
        "meta_config_proposals",
        "confidence_score >= 0.0 AND confidence_score <= 1.0",
    )

    # --- proposal events (append-only)
    op.create_table(
        "meta_config_proposal_events",
        sa.Column(
            "event_id",
            sa.BigInteger,
            primary_key=True,
            autoincrement=True,
        ),
        sa.Column(
            "proposal_id",
            sa.String(length=64),
            sa.ForeignKey(
                "meta_config_proposals.proposal_id", ondelete="CASCADE"
            ),
            nullable=False,
        ),
        sa.Column("event_type", sa.String(length=32), nullable=False),
        sa.Column(
            "event_by",
            sa.String(length=64),
            nullable=False,
            server_default=sa.text("'system'"),
        ),
        sa.Column(
            "event_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("reason", sa.Text, nullable=True),
        sa.Column("metadata", postgresql.JSONB, nullable=True),
    )

    op.create_check_constraint(
        "ck_meta_config_proposal_events_event_type_nonempty",
        "meta_config_proposal_events",
        "btrim(event_type) <> ''",
    )
    op.create_check_constraint(
        "ck_meta_config_proposal_events_event_by_nonempty",
        "meta_config_proposal_events",
        "btrim(event_by) <> ''",
    )
    op.create_check_constraint(
        "ck_meta_config_proposal_events_event_type_allowed",
        "meta_config_proposal_events",
        "event_type IN ('CREATED','APPROVED','REJECTED','APPLIED','REVERTED')",
    )

    op.create_index(
        "idx_meta_config_proposal_events_proposal_time",
        "meta_config_proposal_events",
        ["proposal_id", "event_at"],
        unique=False,
    )

    # State transition uniqueness (at most one of each per proposal).
    op.create_index(
        "ux_meta_config_proposal_events_created_once",
        "meta_config_proposal_events",
        ["proposal_id"],
        unique=True,
        postgresql_where=sa.text("event_type = 'CREATED'"),
    )
    op.create_index(
        "ux_meta_config_proposal_events_approved_once",
        "meta_config_proposal_events",
        ["proposal_id"],
        unique=True,
        postgresql_where=sa.text("event_type = 'APPROVED'"),
    )
    op.create_index(
        "ux_meta_config_proposal_events_rejected_once",
        "meta_config_proposal_events",
        ["proposal_id"],
        unique=True,
        postgresql_where=sa.text("event_type = 'REJECTED'"),
    )
    op.create_index(
        "ux_meta_config_proposal_events_applied_once",
        "meta_config_proposal_events",
        ["proposal_id"],
        unique=True,
        postgresql_where=sa.text("event_type = 'APPLIED'"),
    )
    op.create_index(
        "ux_meta_config_proposal_events_reverted_once",
        "meta_config_proposal_events",
        ["proposal_id"],
        unique=True,
        postgresql_where=sa.text("event_type = 'REVERTED'"),
    )

    # Backfill events for existing proposals.
    # Always create a CREATED event.
    op.execute(
        """
        INSERT INTO meta_config_proposal_events (proposal_id, event_type, event_by, event_at)
        SELECT p.proposal_id, 'CREATED', 'system', p.created_at
        FROM meta_config_proposals p
        WHERE NOT EXISTS (
            SELECT 1 FROM meta_config_proposal_events e
            WHERE e.proposal_id = p.proposal_id AND e.event_type = 'CREATED'
        )
        """
    )

    # Derive subsequent status events from legacy columns if present.
    op.execute(
        """
        INSERT INTO meta_config_proposal_events (proposal_id, event_type, event_by, event_at)
        SELECT p.proposal_id, 'APPROVED', COALESCE(p.approved_by, 'system'), COALESCE(p.approved_at, now())
        FROM meta_config_proposals p
        WHERE p.status = 'APPROVED'
          AND NOT EXISTS (
            SELECT 1 FROM meta_config_proposal_events e
            WHERE e.proposal_id = p.proposal_id AND e.event_type = 'APPROVED'
          )
        """
    )
    op.execute(
        """
        INSERT INTO meta_config_proposal_events (proposal_id, event_type, event_by, event_at)
        SELECT p.proposal_id, 'REJECTED', COALESCE(p.approved_by, 'system'), COALESCE(p.approved_at, now())
        FROM meta_config_proposals p
        WHERE p.status = 'REJECTED'
          AND NOT EXISTS (
            SELECT 1 FROM meta_config_proposal_events e
            WHERE e.proposal_id = p.proposal_id AND e.event_type = 'REJECTED'
          )
        """
    )
    op.execute(
        """
        INSERT INTO meta_config_proposal_events (proposal_id, event_type, event_by, event_at)
        SELECT p.proposal_id, 'APPLIED', COALESCE(p.approved_by, 'system'), COALESCE(p.applied_at, now())
        FROM meta_config_proposals p
        WHERE p.status = 'APPLIED'
          AND NOT EXISTS (
            SELECT 1 FROM meta_config_proposal_events e
            WHERE e.proposal_id = p.proposal_id AND e.event_type = 'APPLIED'
          )
        """
    )
    op.execute(
        """
        INSERT INTO meta_config_proposal_events (proposal_id, event_type, event_by, event_at)
        SELECT p.proposal_id, 'REVERTED', COALESCE(p.approved_by, 'system'), COALESCE(p.reverted_at, now())
        FROM meta_config_proposals p
        WHERE p.status = 'REVERTED'
          AND NOT EXISTS (
            SELECT 1 FROM meta_config_proposal_events e
            WHERE e.proposal_id = p.proposal_id AND e.event_type = 'REVERTED'
          )
        """
    )

    # Prevent UPDATE/DELETE on proposals (immutable contract).
    op.execute(
        """
        CREATE OR REPLACE FUNCTION meta_config_proposals_prevent_mutation()
        RETURNS trigger AS $$
        BEGIN
          RAISE EXCEPTION 'meta_config_proposals is append-only; UPDATE/DELETE are not allowed';
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_meta_config_proposals_prevent_mutation
        BEFORE UPDATE OR DELETE ON meta_config_proposals
        FOR EACH ROW
        EXECUTE FUNCTION meta_config_proposals_prevent_mutation();
        """
    )

    # Prevent UPDATE/DELETE on events (append-only).
    op.execute(
        """
        CREATE OR REPLACE FUNCTION meta_config_proposal_events_prevent_mutation()
        RETURNS trigger AS $$
        BEGIN
          RAISE EXCEPTION 'meta_config_proposal_events is append-only; UPDATE/DELETE are not allowed';
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_meta_config_proposal_events_prevent_mutation
        BEFORE UPDATE OR DELETE ON meta_config_proposal_events
        FOR EACH ROW
        EXECUTE FUNCTION meta_config_proposal_events_prevent_mutation();
        """
    )


def downgrade() -> None:
    # Drop events trigger/function.
    op.execute(
        "DROP TRIGGER IF EXISTS trg_meta_config_proposal_events_prevent_mutation ON meta_config_proposal_events"
    )
    op.execute(
        "DROP FUNCTION IF EXISTS meta_config_proposal_events_prevent_mutation()"
    )

    # Drop proposal trigger/function.
    op.execute(
        "DROP TRIGGER IF EXISTS trg_meta_config_proposals_prevent_mutation ON meta_config_proposals"
    )
    op.execute("DROP FUNCTION IF EXISTS meta_config_proposals_prevent_mutation()")

    # Drop events table + indexes/constraints.
    op.drop_index(
        "ux_meta_config_proposal_events_reverted_once",
        table_name="meta_config_proposal_events",
    )
    op.drop_index(
        "ux_meta_config_proposal_events_applied_once",
        table_name="meta_config_proposal_events",
    )
    op.drop_index(
        "ux_meta_config_proposal_events_rejected_once",
        table_name="meta_config_proposal_events",
    )
    op.drop_index(
        "ux_meta_config_proposal_events_approved_once",
        table_name="meta_config_proposal_events",
    )
    op.drop_index(
        "ux_meta_config_proposal_events_created_once",
        table_name="meta_config_proposal_events",
    )
    op.drop_index(
        "idx_meta_config_proposal_events_proposal_time",
        table_name="meta_config_proposal_events",
    )
    op.drop_constraint(
        "ck_meta_config_proposal_events_event_type_allowed",
        "meta_config_proposal_events",
        type_="check",
    )
    op.drop_constraint(
        "ck_meta_config_proposal_events_event_by_nonempty",
        "meta_config_proposal_events",
        type_="check",
    )
    op.drop_constraint(
        "ck_meta_config_proposal_events_event_type_nonempty",
        "meta_config_proposal_events",
        type_="check",
    )
    op.drop_table("meta_config_proposal_events")

    # Drop proposal constraints.
    op.drop_constraint(
        "ck_meta_config_proposals_confidence_score_range",
        "meta_config_proposals",
        type_="check",
    )
    op.drop_constraint(
        "ck_meta_config_proposals_target_component_nonempty",
        "meta_config_proposals",
        type_="check",
    )
    op.drop_constraint(
        "ck_meta_config_proposals_proposal_type_nonempty",
        "meta_config_proposals",
        type_="check",
    )
    op.drop_constraint(
        "ck_meta_config_proposals_strategy_id_nonempty",
        "meta_config_proposals",
        type_="check",
    )
    op.drop_constraint(
        "ck_meta_config_proposals_proposal_id_nonempty",
        "meta_config_proposals",
        type_="check",
    )

    # Relax strategy_id back to nullable.
    op.alter_column(
        "meta_config_proposals",
        "strategy_id",
        existing_type=sa.String(length=64),
        nullable=True,
    )
