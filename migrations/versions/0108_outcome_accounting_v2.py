"""Archive invalid target-portfolio accounting and queue recomputation."""
from alembic import op

revision = "0108_outcome_accounting_v2"
down_revision = "0107_divergence_missing_data"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        UPDATE decision_outcomes o SET
            metadata = COALESCE(o.metadata, '{}'::jsonb) || jsonb_build_object(
                'accounting_invalid', true,
                'evaluation_kind', 'target_portfolio',
                'legacy_accounting', jsonb_build_object(
                    'realized_return', o.realized_return, 'realized_pnl', o.realized_pnl,
                    'realized_drawdown', o.realized_drawdown, 'realized_vol', o.realized_vol,
                    'metadata', o.metadata, 'created_at', o.created_at)),
            realized_return = NULL, realized_pnl = NULL,
            realized_drawdown = NULL, realized_vol = NULL
        FROM engine_decisions d
        WHERE d.decision_id = o.decision_id AND d.engine_name IN ('PORTFOLIO', 'ASSESSMENT')
          AND COALESCE(o.metadata->>'accounting_version', '') <> '2'
          AND NOT COALESCE(o.metadata, '{}'::jsonb) ? 'legacy_accounting'
    """)


def downgrade():
    # Restoring the known-bad formula's numbers would reintroduce false PnL.
    # The original values remain available under metadata.legacy_accounting.
    pass
