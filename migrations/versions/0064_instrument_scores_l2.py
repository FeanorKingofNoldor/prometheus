"""Layer 2: tighten instrument_scores contracts

Revision ID: 0064_instrument_scores_l2
Revises: 0063_fragility_measures_l2
Create Date: 2025-12-16

Layer 2 contract for ``instrument_scores``:
- ids are non-empty
- horizon_days > 0
- expected_return/score/confidence are finite
- score in [-1, 1] and confidence in [0, 1]
- signal_label is in an allowed set
- alpha_components is a JSON object
- metadata is either NULL or a JSON object

Note: uniqueness by run/model is intentional in early iterations.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0064_instrument_scores_l2"
down_revision: Union[str, None] = "0063_fragility_measures_l2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_NONFINITE = "('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision)"


def upgrade() -> None:
    op.create_check_constraint(
        "ck_instrument_scores_score_id_nonempty",
        "instrument_scores",
        "btrim(score_id) <> ''",
    )

    op.create_check_constraint(
        "ck_instrument_scores_strategy_id_nonempty",
        "instrument_scores",
        "btrim(strategy_id) <> ''",
    )

    op.create_check_constraint(
        "ck_instrument_scores_market_id_nonempty",
        "instrument_scores",
        "btrim(market_id) <> ''",
    )

    op.create_check_constraint(
        "ck_instrument_scores_instrument_id_nonempty",
        "instrument_scores",
        "btrim(instrument_id) <> ''",
    )

    op.create_check_constraint(
        "ck_instrument_scores_horizon_days_positive",
        "instrument_scores",
        "horizon_days > 0",
    )

    op.create_check_constraint(
        "ck_instrument_scores_expected_return_finite",
        "instrument_scores",
        f"expected_return NOT IN {_NONFINITE}",
    )

    op.create_check_constraint(
        "ck_instrument_scores_score_finite",
        "instrument_scores",
        f"score NOT IN {_NONFINITE}",
    )

    op.create_check_constraint(
        "ck_instrument_scores_score_range_neg1_1",
        "instrument_scores",
        "score >= -1.0 AND score <= 1.0",
    )

    op.create_check_constraint(
        "ck_instrument_scores_confidence_finite",
        "instrument_scores",
        f"confidence NOT IN {_NONFINITE}",
    )

    op.create_check_constraint(
        "ck_instrument_scores_confidence_range_0_1",
        "instrument_scores",
        "confidence >= 0.0 AND confidence <= 1.0",
    )

    op.create_check_constraint(
        "ck_instrument_scores_signal_label_nonempty",
        "instrument_scores",
        "btrim(signal_label) <> ''",
    )

    op.create_check_constraint(
        "ck_instrument_scores_signal_label_allowed",
        "instrument_scores",
        "signal_label IN ('HOLD', 'BUY', 'STRONG_BUY', 'SELL', 'STRONG_SELL')",
    )

    op.create_check_constraint(
        "ck_instrument_scores_alpha_components_object",
        "instrument_scores",
        "jsonb_typeof(alpha_components) = 'object'",
    )

    op.create_check_constraint(
        "ck_instrument_scores_metadata_object_or_null",
        "instrument_scores",
        "metadata IS NULL OR jsonb_typeof(metadata) = 'object'",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_instrument_scores_metadata_object_or_null",
        "instrument_scores",
        type_="check",
    )
    op.drop_constraint(
        "ck_instrument_scores_alpha_components_object",
        "instrument_scores",
        type_="check",
    )
    op.drop_constraint(
        "ck_instrument_scores_signal_label_allowed",
        "instrument_scores",
        type_="check",
    )
    op.drop_constraint(
        "ck_instrument_scores_signal_label_nonempty",
        "instrument_scores",
        type_="check",
    )
    op.drop_constraint(
        "ck_instrument_scores_confidence_range_0_1",
        "instrument_scores",
        type_="check",
    )
    op.drop_constraint(
        "ck_instrument_scores_confidence_finite",
        "instrument_scores",
        type_="check",
    )
    op.drop_constraint(
        "ck_instrument_scores_score_range_neg1_1",
        "instrument_scores",
        type_="check",
    )
    op.drop_constraint(
        "ck_instrument_scores_score_finite",
        "instrument_scores",
        type_="check",
    )
    op.drop_constraint(
        "ck_instrument_scores_expected_return_finite",
        "instrument_scores",
        type_="check",
    )
    op.drop_constraint(
        "ck_instrument_scores_horizon_days_positive",
        "instrument_scores",
        type_="check",
    )
    op.drop_constraint(
        "ck_instrument_scores_instrument_id_nonempty",
        "instrument_scores",
        type_="check",
    )
    op.drop_constraint(
        "ck_instrument_scores_market_id_nonempty",
        "instrument_scores",
        type_="check",
    )
    op.drop_constraint(
        "ck_instrument_scores_strategy_id_nonempty",
        "instrument_scores",
        type_="check",
    )
    op.drop_constraint(
        "ck_instrument_scores_score_id_nonempty",
        "instrument_scores",
        type_="check",
    )
