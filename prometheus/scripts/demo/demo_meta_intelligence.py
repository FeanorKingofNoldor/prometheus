#!/usr/bin/env python3
"""Prometheus v2 – Meta/Kronos Intelligence Demo.

This script demonstrates the Meta/Kronos intelligence layer:
1. Analyzes backtest results using the diagnostics engine
2. Generates configuration improvement proposals
3. Displays proposals with confidence scores and expected impacts
4. Shows approval workflow

Usage:
    python -m prometheus.scripts.demo_meta_intelligence --strategy-id <strategy_id>

Example:
    python -m prometheus.scripts.demo_meta_intelligence --strategy-id STRAT_001

Author: Prometheus Team
Created: 2025-12-02
"""

import argparse
import sys

from apathis.core.config import get_config
from apathis.core.database import get_db_manager
from apathis.core.logging import get_logger

from prometheus.meta.applicator import ProposalApplicator
from prometheus.meta.diagnostics import DiagnosticsEngine
from prometheus.meta.proposal_generator import ProposalGenerator

logger = get_logger(__name__)


def print_section(title: str) -> None:
    """Print a formatted section header."""
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80)


def demo_diagnostics(strategy_id: str, db_manager) -> None:
    """Demonstrate diagnostic analysis."""
    print_section("DIAGNOSTICS ENGINE - Performance Analysis")

    engine = DiagnosticsEngine(db_manager=db_manager)

    try:
        report = engine.analyze_strategy(strategy_id)

        print(f"\n📊 Strategy: {report.strategy_id}")
        print(f"📅 Analysis Date: {report.sample_metadata['analysis_timestamp']}")
        print(f"📈 Total Runs Analyzed: {report.sample_metadata['total_runs']}")

        print("\n📉 OVERALL PERFORMANCE")
        print(f"  Sharpe Ratio:      {report.overall_performance.sharpe:.3f}")
        print(f"  Cumulative Return: {report.overall_performance.return_:.2%}")
        print(f"  Annualized Vol:    {report.overall_performance.volatility:.2%}")
        print(f"  Max Drawdown:      {report.overall_performance.max_drawdown:.2%}")
        print(f"  Sample Size:       {report.overall_performance.sample_size} runs")

        print("\n🎯 REGIME BREAKDOWN")
        for regime in report.regime_breakdown:
            print(f"  {regime.regime_id}:")
            print(f"    Sharpe:          {regime.stats.sharpe:.3f}")
            print(f"    Relative Sharpe: {regime.relative_sharpe:+.3f}")

        print("\n🔬 CONFIG COMPARISONS")
        if report.config_comparisons:
            for i, comp in enumerate(report.config_comparisons[:5], 1):
                print(f"  {i}. {comp.config_key}:")
                print(f"     {comp.baseline_value} → {comp.alternative_value}")
                print(f"     Sharpe Δ: {comp.sharpe_delta:+.3f}")
                print(f"     Return Δ: {comp.return_delta:+.2%}")
                print(f"     Risk Δ:   {comp.risk_delta:+.2%}")
                print(f"     Samples:  {comp.sample_count}")
        else:
            print("  No configuration comparisons available (need varying configs)")

        print("\n⚠️  UNDERPERFORMING CONFIGS")
        if report.underperforming_configs:
            print(f"  Found {len(report.underperforming_configs)} underperforming configurations")
            for i, config in enumerate(report.underperforming_configs[:3], 1):
                print(f"  {i}. Run {config['run_id'][:8]}...")
                print(f"     Sharpe: {config['sharpe']:.3f}")
                print(f"     Reason: {config['reason']}")
        else:
            print("  ✅ All configurations meet minimum performance threshold")

        print("\n🚨 HIGH-RISK CONFIGS")
        if report.high_risk_configs:
            print(f"  Found {len(report.high_risk_configs)} high-risk configurations")
            for i, config in enumerate(report.high_risk_configs[:3], 1):
                print(f"  {i}. Run {config['run_id'][:8]}...")
                print(f"     Volatility: {config['volatility']:.2%}")
                print(f"     Drawdown:   {config['max_drawdown']:.2%}")
                for reason in config['reasons']:
                    print(f"     - {reason}")
        else:
            print("  ✅ All configurations within acceptable risk limits")

        return report

    except ValueError as e:
        print(f"\n❌ Error: {e}")
        print("   Not enough backtest data available for analysis.")
        print("   Run some backtests first before using the intelligence layer.")
        return None


def demo_proposals(strategy_id: str, db_manager) -> None:
    """Demonstrate proposal generation."""
    print_section("PROPOSAL GENERATOR - Configuration Improvements")

    diagnostics_engine = DiagnosticsEngine(db_manager=db_manager)
    generator = ProposalGenerator(
        db_manager=db_manager,
        diagnostics_engine=diagnostics_engine,
        min_confidence_threshold=0.3,
        min_sharpe_improvement=0.1,
    )

    try:
        proposals = generator.generate_proposals(strategy_id, auto_save=True)

        if not proposals:
            print("\n✅ No improvement proposals generated.")
            print("   Current configuration appears optimal given available data.")
            return

        print(f"\n🎯 Generated {len(proposals)} improvement proposals\n")

        for i, proposal in enumerate(proposals, 1):
            print(f"{'─' * 80}")
            print(f"PROPOSAL #{i}: {proposal.proposal_type.upper()}")
            print(f"{'─' * 80}")
            print(f"  Target:     {proposal.target_component}")
            print(f"  Current:    {proposal.current_value}")
            print(f"  Proposed:   {proposal.proposed_value}")
            print("\n  📊 EXPECTED IMPACT:")
            print(f"    Sharpe:   {proposal.expected_sharpe_improvement:+.3f}")
            print(f"    Return:   {proposal.expected_return_improvement:+.2%}")
            print(f"    Risk:     {proposal.expected_risk_reduction:+.2%}")
            print(f"\n  🎲 CONFIDENCE: {proposal.confidence_score:.1%}")
            print("\n  📝 RATIONALE:")
            print(f"    {proposal.rationale}")
            print("\n  📈 SUPPORTING DATA:")
            for key, value in proposal.supporting_metrics.items():
                if isinstance(value, float):
                    print(f"    {key}: {value:.4f}")
                else:
                    print(f"    {key}: {value}")

        print("\n" + "=" * 80)
        print("  Proposals saved to database with status=PENDING")
        print("  Use approve_proposal() or reject_proposal() to manage them")
        print("=" * 80)

    except ValueError as e:
        print(f"\n❌ Error: {e}")
        return


def demo_proposal_workflow(db_manager) -> None:
    """Demonstrate proposal approval workflow."""
    print_section("PROPOSAL WORKFLOW - Approval & Management")

    diagnostics_engine = DiagnosticsEngine(db_manager=db_manager)
    generator = ProposalGenerator(
        db_manager=db_manager,
        diagnostics_engine=diagnostics_engine,
    )

    # Load pending proposals
    pending = generator.load_pending_proposals()

    if not pending:
        print("\n📭 No pending proposals in the system.")
        return

    print(f"\n📋 {len(pending)} PENDING PROPOSALS:\n")

    for i, prop in enumerate(pending[:10], 1):
        print(f"{i}. [{prop['proposal_id'][:8]}...] {prop['proposal_type']}")
        print(f"   Strategy: {prop['strategy_id']}")
        print(f"   Target:   {prop['target_component']}")
        print(f"   Impact:   Sharpe {prop['expected_sharpe_improvement']:+.3f}")
        print(f"   Confidence: {prop['confidence_score']:.1%}")
        print(f"   Status:   {prop['status']}")
        print(f"   Created:  {prop['created_at']}")
        print()

    print("=" * 80)
    print("  To approve: generator.approve_proposal(proposal_id, 'user_id')")
    print("  To reject:  generator.reject_proposal(proposal_id, 'user_id')")
    print("  To apply:   applicator.apply_proposal(proposal_id, 'user_id')")
    print("=" * 80)


def demo_applicator(db_manager) -> None:
    """Demonstrate proposal application and reversion."""
    print_section("APPLICATOR - Apply & Revert Changes")

    ProposalApplicator(db_manager=db_manager, dry_run=False)

    # Show how to apply approved proposals
    print("\n📝 APPLYING APPROVED PROPOSALS")
    print("  Use: applicator.apply_approved_proposals(strategy_id='STRAT_001')")
    print("  - Validates proposal status (must be APPROVED)")
    print("  - Applies config changes")
    print("  - Records in config_change_log")
    print("  - Updates proposal status to APPLIED")

    print("\n🔄 REVERTING BAD CHANGES")
    print("  Use: applicator.revert_change(change_id, reason='Poor performance')")
    print("  - Reverts to previous config value")
    print("  - Records a REVERT entry in config_change_log (append-only)")
    print("  - Updates proposal status to REVERTED")

    print("\n📊 EVALUATING CHANGE PERFORMANCE")
    print("  Use: applicator.evaluate_change_performance(change_id, start_date, end_date)")
    print("  - Compares performance before/after change")
    print("  - Records metrics in config_change_evaluations (append-only)")
    print("  - Returns improvement deltas")

    print("\n🔒 DRY RUN MODE")
    print("  Use: ProposalApplicator(db_manager, dry_run=True)")
    print("  - Validates proposals without applying")
    print("  - Safe for testing")

    print("\n" + "=" * 80)
    print("  Complete feedback loop: Diagnose → Propose → Approve → Apply → Evaluate")
    print("=" * 80)


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Demonstrate Meta/Kronos intelligence layer"
    )
    parser.add_argument(
        "--strategy-id",
        type=str,
        help="Strategy ID to analyze (required for diagnostics/proposals)",
    )
    parser.add_argument(
        "--workflow-only",
        action="store_true",
        help="Only show pending proposals workflow",
    )

    args = parser.parse_args()

    # Initialize configuration and database
    get_config()
    db_manager = get_db_manager()

    print("\n" + "=" * 80)
    print("  PROMETHEUS v2 - META/KRONOS INTELLIGENCE LAYER DEMO")
    print("=" * 80)

    if args.workflow_only:
        demo_proposal_workflow(db_manager)
        return 0

    if not args.strategy_id:
        print("\n❌ Error: --strategy-id is required for diagnostics and proposals")
        print("   Use --workflow-only to view pending proposals without analysis")
        parser.print_help()
        return 1

    # Run diagnostics
    report = demo_diagnostics(args.strategy_id, db_manager)

    if report is None:
        return 1

    # Generate proposals
    demo_proposals(args.strategy_id, db_manager)

    # Show workflow
    demo_proposal_workflow(db_manager)

    # Show applicator
    demo_applicator(db_manager)

    print("\n✅ Demo completed successfully!\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
