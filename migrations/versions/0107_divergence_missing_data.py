"""Persist explicit missing-data divergence states without arming signals."""
from alembic import op

revision = "0107_divergence_missing_data"
down_revision = "0106_equity_history_and_conviction_tombstones"
branch_labels = None
depends_on = None


def upgrade():
    op.drop_constraint("ck_divergence_signals_direction", "divergence_signals", type_="check")
    op.create_check_constraint("ck_divergence_signals_direction", "divergence_signals",
        "direction IN ('ALIGNED', 'NARRATIVE_OVERSTATES', 'REALITY_UNDERSTATED', "
        "'INSUFFICIENT_NARRATIVE_DATA', 'INSUFFICIENT_BEHAVIORAL_DATA')")
    op.create_check_constraint("ck_divergence_missing_data_no_trade", "divergence_signals",
        "direction NOT LIKE 'INSUFFICIENT_%' OR (severity = 'NONE' AND trading_signal = 'NONE')")


def downgrade():
    # Preserve the honest missing-data states: a rollback with these rows must
    # fail rather than silently converting them to an aligned trading signal.
    op.drop_constraint("ck_divergence_missing_data_no_trade", "divergence_signals", type_="check")
    op.drop_constraint("ck_divergence_signals_direction", "divergence_signals", type_="check")
    op.create_check_constraint("ck_divergence_signals_direction", "divergence_signals",
        "direction IN ('ALIGNED', 'NARRATIVE_OVERSTATES', 'REALITY_UNDERSTATED')")
