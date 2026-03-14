"""Prometheus v2 – Meta/Kronos Proposal Generator.

This module generates actionable configuration improvement proposals based on
diagnostic analysis of backtest results. Proposals include confidence scoring,
expected impact estimation, and rationale generation.

Key responsibilities:
- Generate config change proposals from diagnostic insights
- Estimate expected impact (Sharpe, return, risk improvements)
- Compute confidence scores based on sample size and consistency
- Provide rationale for each proposal
- Persist proposals to database for approval workflow

Author: Prometheus Team
Created: 2025-12-02
Status: Development
Version: v0.1.0
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Any, Sequence

from psycopg2.extras import Json

from apathis.core.database import DatabaseManager
from apathis.core.ids import generate_uuid
from apathis.core.logging import get_logger
from prometheus.meta.diagnostics import DiagnosticReport, DiagnosticsEngine


logger = get_logger(__name__)


@dataclass(frozen=True)
class ConfigProposal:
    """Configuration change proposal.

    Attributes:
        proposal_id: Unique proposal identifier
        strategy_id: Strategy this applies to
        market_id: Optional market identifier
        proposal_type: Type of proposal (universe_adjustment, risk_limit_change, etc.)
        target_component: Specific config parameter to change
        current_value: Current configuration value
        proposed_value: Proposed new value
        confidence_score: Confidence in this proposal (0.0-1.0)
        expected_sharpe_improvement: Expected Sharpe improvement
        expected_return_improvement: Expected return improvement
        expected_risk_reduction: Expected risk reduction
        rationale: Human-readable explanation
        supporting_metrics: Additional supporting data
    """

    proposal_id: str
    strategy_id: str
    market_id: Optional[str]
    proposal_type: str
    target_component: str
    current_value: any
    proposed_value: any
    confidence_score: float
    expected_sharpe_improvement: float
    expected_return_improvement: float
    expected_risk_reduction: float
    rationale: str
    supporting_metrics: Dict[str, any]


@dataclass
class ProposalGenerator:
    """Generates configuration improvement proposals from diagnostic insights.

    The generator analyzes diagnostic reports and produces actionable proposals
    for configuration changes, complete with confidence scores and impact estimates.
    """

    db_manager: DatabaseManager
    diagnostics_engine: DiagnosticsEngine
    min_confidence_threshold: float = 0.3
    min_sharpe_improvement: float = 0.1

    def generate_proposals(
        self, strategy_id: str, auto_save: bool = True
    ) -> List[ConfigProposal]:
        """Generate configuration improvement proposals for a strategy.

        Args:
            strategy_id: Strategy to analyze
            auto_save: Whether to automatically persist proposals to database

        Returns:
            List of generated proposals

        Raises:
            ValueError: If insufficient data for analysis
        """
        # Run diagnostic analysis
        report = self.diagnostics_engine.analyze_strategy(strategy_id)

        logger.info(
            f"ProposalGenerator: generating proposals for strategy={strategy_id}"
        )

        proposals = []

        # Generate proposals from config comparisons
        proposals.extend(self._proposals_from_comparisons(report))

        # Generate proposals from underperforming configs
        proposals.extend(self._proposals_from_underperforming(report))

        # Generate proposals from high-risk configs
        proposals.extend(self._proposals_from_high_risk(report))

        # Filter by confidence and impact thresholds
        proposals = [
            p
            for p in proposals
            if p.confidence_score >= self.min_confidence_threshold
            and p.expected_sharpe_improvement >= self.min_sharpe_improvement
        ]

        # Sort by expected impact (Sharpe improvement)
        proposals.sort(key=lambda p: p.expected_sharpe_improvement, reverse=True)

        logger.info(
            f"ProposalGenerator: generated {len(proposals)} proposals meeting thresholds"
        )

        # Persist to database
        if auto_save and proposals:
            self._save_proposals(proposals)

        return proposals

    def _proposals_from_comparisons(
        self, report: DiagnosticReport
    ) -> List[ConfigProposal]:
        """Generate proposals from config comparisons showing improvements."""
        proposals = []

        for comparison in report.config_comparisons:
            # Only propose changes that show improvement
            if comparison.sharpe_delta <= 0:
                continue

            # Compute confidence based on sample size and effect size
            confidence = self.diagnostics_engine.compute_confidence_score(
                sample_size=comparison.sample_count,
                sharpe_delta=comparison.sharpe_delta,
                consistency=0.7,  # Assume 70% consistency for A/B comparisons
            )

            # Generate rationale
            rationale = self._generate_comparison_rationale(comparison)

            proposal = ConfigProposal(
                proposal_id=generate_uuid(),
                strategy_id=report.strategy_id,
                market_id=None,
                proposal_type="config_parameter_change",
                target_component=comparison.config_key,
                current_value=comparison.baseline_value,
                proposed_value=comparison.alternative_value,
                confidence_score=confidence,
                expected_sharpe_improvement=comparison.sharpe_delta,
                expected_return_improvement=comparison.return_delta,
                expected_risk_reduction=-comparison.risk_delta
                if comparison.risk_delta < 0
                else 0.0,
                rationale=rationale,
                supporting_metrics={
                    "sample_count": comparison.sample_count,
                    "sharpe_delta": comparison.sharpe_delta,
                    "return_delta": comparison.return_delta,
                    "risk_delta": comparison.risk_delta,
                },
            )
            proposals.append(proposal)

        return proposals

    def _proposals_from_underperforming(
        self, report: DiagnosticReport
    ) -> List[ConfigProposal]:
        """Generate proposals to address underperforming configurations."""
        proposals = []

        # If many underperforming configs exist, propose more conservative settings
        if len(report.underperforming_configs) > len(report.overall_performance.run_ids) * 0.3:
            # More than 30% underperforming - suggest risk reduction
            
            # Find common config patterns in underperforming vs performing runs
            underperforming_ids = {c["run_id"] for c in report.underperforming_configs}
            
            # Analyze config differences between performers and underperformers
            # (simplified for now - could be more sophisticated)
            
            confidence = 0.5  # Medium confidence for pattern-based proposals
            
            rationale = (
                f"{len(report.underperforming_configs)} of "
                f"{report.overall_performance.sample_size} runs underperform. "
                f"Recommend reviewing risk parameters and strategy constraints."
            )
            
            # Generic proposal to review configuration
            proposal = ConfigProposal(
                proposal_id=generate_uuid(),
                strategy_id=report.strategy_id,
                market_id=None,
                proposal_type="risk_reduction",
                target_component="strategy_risk_limits",
                current_value=None,
                proposed_value={"action": "review_and_tighten"},
                confidence_score=confidence,
                expected_sharpe_improvement=0.2,  # Conservative estimate
                expected_return_improvement=0.0,
                expected_risk_reduction=0.05,
                rationale=rationale,
                supporting_metrics={
                    "underperforming_count": len(report.underperforming_configs),
                    "total_runs": report.overall_performance.sample_size,
                },
            )
            proposals.append(proposal)

        return proposals

    def _proposals_from_high_risk(
        self, report: DiagnosticReport
    ) -> List[ConfigProposal]:
        """Generate proposals to address high-risk configurations."""
        proposals = []

        # If significant high-risk configs exist, propose risk constraints
        if len(report.high_risk_configs) > 0:
            confidence = 0.6  # Medium-high confidence for risk mitigation
            
            rationale = (
                f"{len(report.high_risk_configs)} runs show excessive risk. "
                f"Recommend implementing stricter position sizing or stop-loss rules."
            )
            
            proposal = ConfigProposal(
                proposal_id=generate_uuid(),
                strategy_id=report.strategy_id,
                market_id=None,
                proposal_type="risk_constraint",
                target_component="max_position_volatility",
                current_value=None,
                proposed_value={"max_vol": 0.25, "max_drawdown": -0.15},
                confidence_score=confidence,
                expected_sharpe_improvement=0.15,
                expected_return_improvement=-0.02,  # May reduce returns slightly
                expected_risk_reduction=0.08,
                rationale=rationale,
                supporting_metrics={
                    "high_risk_count": len(report.high_risk_configs),
                    "avg_volatility": report.overall_performance.volatility,
                    "worst_drawdown": report.overall_performance.max_drawdown,
                },
            )
            proposals.append(proposal)

        return proposals

    def _generate_comparison_rationale(self, comparison) -> str:
        """Generate human-readable rationale for a config comparison proposal."""
        param_name = comparison.config_key.replace("_", " ").title()
        
        if comparison.sharpe_delta > 0.3:
            impact = "significant"
        elif comparison.sharpe_delta > 0.15:
            impact = "moderate"
        else:
            impact = "modest"
        
        rationale = (
            f"Changing {param_name} from {comparison.baseline_value} to "
            f"{comparison.alternative_value} shows {impact} improvement: "
            f"Sharpe +{comparison.sharpe_delta:.2f}, Return +{comparison.return_delta:.2%}. "
            f"Based on {comparison.sample_count} backtest runs."
        )
        
        return rationale

    def _save_proposals(self, proposals: List[ConfigProposal]) -> None:
        """Persist proposals to database (append-only).

        Layer 0 contract: proposals are immutable once written. Workflow state
        transitions are recorded in ``meta_config_proposal_events``.
        """

        sql_insert = """
            INSERT INTO meta_config_proposals (
                proposal_id,
                strategy_id,
                market_id,
                proposal_type,
                target_component,
                current_value,
                proposed_value,
                confidence_score,
                expected_sharpe_improvement,
                expected_return_improvement,
                expected_risk_reduction,
                rationale,
                supporting_metrics,
                status,
                created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        """

        sql_event = """
            INSERT INTO meta_config_proposal_events (proposal_id, event_type, event_by, event_at)
            VALUES (%s, %s, %s, NOW())
        """

        with self.db_manager.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                for proposal in proposals:
                    cursor.execute(
                        sql_insert,
                        (
                            proposal.proposal_id,
                            proposal.strategy_id,
                            proposal.market_id,
                            proposal.proposal_type,
                            proposal.target_component,
                            Json(proposal.current_value),
                            Json(proposal.proposed_value),
                            proposal.confidence_score,
                            proposal.expected_sharpe_improvement,
                            proposal.expected_return_improvement,
                            proposal.expected_risk_reduction,
                            proposal.rationale,
                            Json(proposal.supporting_metrics),
                            "PENDING",
                        ),
                    )
                    # Record creation as an explicit event.
                    cursor.execute(
                        sql_event,
                        (proposal.proposal_id, "CREATED", "system"),
                    )

                conn.commit()
                logger.info(f"Saved {len(proposals)} proposals to database")
            finally:
                cursor.close()

    def load_pending_proposals(
        self, strategy_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Load pending proposals from database.

        Note: proposal status is derived from ``meta_config_proposal_events``.
        A CREATED proposal is considered PENDING.
        """

        sql = """
            SELECT
                p.proposal_id,
                p.strategy_id,
                p.market_id,
                p.proposal_type,
                p.target_component,
                p.current_value,
                p.proposed_value,
                p.confidence_score,
                p.expected_sharpe_improvement,
                p.expected_return_improvement,
                p.expected_risk_reduction,
                p.rationale,
                p.supporting_metrics,
                COALESCE(s.status, 'PENDING') AS status,
                p.created_at
            FROM meta_config_proposals p
            LEFT JOIN LATERAL (
                SELECT
                    CASE
                        WHEN e.event_type = 'CREATED' THEN 'PENDING'
                        ELSE e.event_type
                    END AS status
                FROM meta_config_proposal_events e
                WHERE e.proposal_id = p.proposal_id
                ORDER BY e.event_at DESC, e.event_id DESC
                LIMIT 1
            ) s ON TRUE
            WHERE COALESCE(s.status, 'PENDING') = 'PENDING'
        """

        params: list[Any] = []
        if strategy_id:
            sql += " AND p.strategy_id = %s"
            params.append(strategy_id)

        sql += " ORDER BY p.expected_sharpe_improvement DESC"

        with self.db_manager.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, tuple(params) if params else ())
                rows = cursor.fetchall()
            finally:
                cursor.close()

        proposals: list[Dict[str, Any]] = []
        for row in rows:
            proposals.append(
                {
                    "proposal_id": row[0],
                    "strategy_id": row[1],
                    "market_id": row[2],
                    "proposal_type": row[3],
                    "target_component": row[4],
                    "current_value": row[5],
                    "proposed_value": row[6],
                    "confidence_score": row[7],
                    "expected_sharpe_improvement": row[8],
                    "expected_return_improvement": row[9],
                    "expected_risk_reduction": row[10],
                    "rationale": row[11],
                    "supporting_metrics": row[12],
                    "status": row[13],
                    "created_at": row[14],
                }
            )

        return proposals

    def _get_current_status(self, proposal_id: str) -> str:
        sql = """
            SELECT COALESCE((
                SELECT CASE WHEN e.event_type = 'CREATED' THEN 'PENDING' ELSE e.event_type END
                FROM meta_config_proposal_events e
                WHERE e.proposal_id = %s
                ORDER BY e.event_at DESC, e.event_id DESC
                LIMIT 1
            ), 'PENDING') AS status
        """

        with self.db_manager.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, (proposal_id,))
                row = cursor.fetchone()
            finally:
                cursor.close()

        return str(row[0]) if row and row[0] is not None else "PENDING"

    def _record_event(
        self,
        *,
        proposal_id: str,
        event_type: str,
        event_by: str,
        reason: Optional[str] = None,
    ) -> None:
        sql = """
            INSERT INTO meta_config_proposal_events (proposal_id, event_type, event_by, event_at, reason)
            VALUES (%s, %s, %s, NOW(), %s)
        """

        with self.db_manager.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, (proposal_id, event_type, event_by, reason))
                conn.commit()
            finally:
                cursor.close()

    def approve_proposal(self, proposal_id: str, approved_by: str) -> None:
        """Approve a proposal by recording an APPROVED event (append-only)."""

        status = self._get_current_status(proposal_id)
        if status != "PENDING":
            raise ValueError(
                f"Cannot approve proposal {proposal_id}: current status is {status}"
            )

        self._record_event(
            proposal_id=proposal_id,
            event_type="APPROVED",
            event_by=approved_by,
        )

        logger.info(f"Approved proposal {proposal_id} by {approved_by}")

    def reject_proposal(self, proposal_id: str, approved_by: str) -> None:
        """Reject a proposal by recording a REJECTED event (append-only)."""

        status = self._get_current_status(proposal_id)
        if status != "PENDING":
            raise ValueError(
                f"Cannot reject proposal {proposal_id}: current status is {status}"
            )

        self._record_event(
            proposal_id=proposal_id,
            event_type="REJECTED",
            event_by=approved_by,
        )

        logger.info(f"Rejected proposal {proposal_id} by {approved_by}")
