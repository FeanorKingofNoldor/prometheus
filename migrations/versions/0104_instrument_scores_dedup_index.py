"""Add covering index for deterministic instrument_scores reads.

Revision ID: 0104_instrument_scores_dedup_index
Revises: 0103_drop_redundant_decision_outcomes_uq
Create Date: 2026-06-10

``instrument_scores`` enforces no uniqueness beyond ``score_id``: every engine
run (and every model) may emit a fresh row for the same
strategy/market/instrument/date/horizon. The universe loader
(``_load_assessment_scores``) now deduplicates to the most recently written row
per instrument via ``SELECT DISTINCT ON (instrument_id) ... ORDER BY
instrument_id, created_at DESC``.

We deliberately do NOT add a unique constraint: ``model_id`` lives only in
``metadata`` (not a column), so multiple models legitimately write the same
(strategy_id, market_id, instrument_id, as_of_date, horizon_days) tuple and a
unique constraint would break those writes. Instead add a non-unique index that
matches the read's filter + ordering so the DISTINCT ON stays cheap.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

revision: str = "0104_instrument_scores_dedup_index"
down_revision: Union[str, None] = "0103_drop_redundant_decision_outcomes_uq"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_index(
        "idx_instrument_scores_pit_lookup",
        "instrument_scores",
        ["strategy_id", "market_id", "as_of_date", "horizon_days", "instrument_id", "created_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("idx_instrument_scores_pit_lookup", table_name="instrument_scores")
