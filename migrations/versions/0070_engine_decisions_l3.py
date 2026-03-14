"""Layer 3: tighten engine_decisions contracts

Revision ID: 0070_engine_decisions_l3
Revises: 0069_portfolio_risk_reports_l3
Create Date: 2025-12-16

Layer 3 contract for ``engine_decisions``:
- decision_id and engine_name are non-empty
- run_id/strategy_id/market_id/config_id are either NULL or non-empty
- input_refs/output_refs/metadata are JSON objects when present

Note: referential checks (markets, engine_runs, etc.) are validated via CLI.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "0070_engine_decisions_l3"
down_revision: Union[str, None] = "0069_portfolio_risk_reports_l3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_check_constraint(
        "ck_engine_decisions_decision_id_nonempty",
        "engine_decisions",
        "btrim(decision_id) <> ''",
    )

    op.create_check_constraint(
        "ck_engine_decisions_engine_name_nonempty",
        "engine_decisions",
        "btrim(engine_name) <> ''",
    )

    op.create_check_constraint(
        "ck_engine_decisions_run_id_nonempty_when_present",
        "engine_decisions",
        "run_id IS NULL OR btrim(run_id) <> ''",
    )

    op.create_check_constraint(
        "ck_engine_decisions_strategy_id_nonempty_when_present",
        "engine_decisions",
        "strategy_id IS NULL OR btrim(strategy_id) <> ''",
    )

    op.create_check_constraint(
        "ck_engine_decisions_market_id_nonempty_when_present",
        "engine_decisions",
        "market_id IS NULL OR btrim(market_id) <> ''",
    )

    op.create_check_constraint(
        "ck_engine_decisions_config_id_nonempty_when_present",
        "engine_decisions",
        "config_id IS NULL OR btrim(config_id) <> ''",
    )

    op.create_check_constraint(
        "ck_engine_decisions_input_refs_object_or_null",
        "engine_decisions",
        "input_refs IS NULL OR jsonb_typeof(input_refs) = 'object'",
    )

    op.create_check_constraint(
        "ck_engine_decisions_output_refs_object_or_null",
        "engine_decisions",
        "output_refs IS NULL OR jsonb_typeof(output_refs) = 'object'",
    )

    op.create_check_constraint(
        "ck_engine_decisions_metadata_object_or_null",
        "engine_decisions",
        "metadata IS NULL OR jsonb_typeof(metadata) = 'object'",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_engine_decisions_metadata_object_or_null",
        "engine_decisions",
        type_="check",
    )
    op.drop_constraint(
        "ck_engine_decisions_output_refs_object_or_null",
        "engine_decisions",
        type_="check",
    )
    op.drop_constraint(
        "ck_engine_decisions_input_refs_object_or_null",
        "engine_decisions",
        type_="check",
    )
    op.drop_constraint(
        "ck_engine_decisions_config_id_nonempty_when_present",
        "engine_decisions",
        type_="check",
    )
    op.drop_constraint(
        "ck_engine_decisions_market_id_nonempty_when_present",
        "engine_decisions",
        type_="check",
    )
    op.drop_constraint(
        "ck_engine_decisions_strategy_id_nonempty_when_present",
        "engine_decisions",
        type_="check",
    )
    op.drop_constraint(
        "ck_engine_decisions_run_id_nonempty_when_present",
        "engine_decisions",
        type_="check",
    )
    op.drop_constraint(
        "ck_engine_decisions_engine_name_nonempty",
        "engine_decisions",
        type_="check",
    )
    op.drop_constraint(
        "ck_engine_decisions_decision_id_nonempty",
        "engine_decisions",
        type_="check",
    )
