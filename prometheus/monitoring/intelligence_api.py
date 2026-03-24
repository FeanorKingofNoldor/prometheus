"""Prometheus v2 – Meta/Kronos Intelligence API.

This module provides REST API endpoints for the Meta/Kronos intelligence layer:
- Diagnostics: Performance analysis and insights
- Proposals: Configuration improvement recommendations
- Application: Apply and track changes
- Change Log: Review applied changes and outcomes

Author: Prometheus Team
Created: 2025-12-02
Status: Development
Version: v0.1.0
"""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional

from apathis.core.database import get_db_manager
from fastapi import APIRouter, Body, HTTPException, Path, Query
from pydantic import BaseModel, Field

from prometheus.decisions.lambda_scorecard import LambdaScorecard
from prometheus.decisions.scorecard import PredictionScorecard
from prometheus.meta.applicator import ProposalApplicator
from prometheus.meta.diagnostics import DiagnosticsEngine
from prometheus.meta.proposal_generator import ProposalGenerator

intelligence_router = APIRouter(prefix="/api/intelligence", tags=["intelligence"])


# ============================================================================
# Request/Response Models
# ============================================================================


class DiagnosticReportResponse(BaseModel):
    """Diagnostic analysis report."""

    strategy_id: str
    overall_sharpe: float
    overall_return: float
    overall_volatility: float
    max_drawdown: float
    sample_size: int
    underperforming_count: int
    high_risk_count: int
    config_comparisons_count: int
    analysis_timestamp: str


class ProposalResponse(BaseModel):
    """Configuration proposal."""

    proposal_id: str
    strategy_id: str
    proposal_type: str
    target_component: str
    current_value: Optional[Any]
    proposed_value: Any
    confidence_score: float
    expected_sharpe_improvement: float
    expected_return_improvement: float
    expected_risk_reduction: float
    rationale: str
    status: str
    created_at: str


class ProposalActionRequest(BaseModel):
    """Request to approve/reject/apply a proposal."""

    user_id: str = Field(..., description="User performing the action")


class ApplicationResultResponse(BaseModel):
    """Result of applying a proposal."""

    success: bool
    change_id: Optional[str]
    proposal_id: str
    error_message: Optional[str] = None
    applied_at: Optional[str] = None


class ChangeLogResponse(BaseModel):
    """Configuration change log entry."""

    change_id: str
    proposal_id: Optional[str]
    strategy_id: str
    change_type: str
    target_component: str
    sharpe_before: Optional[float]
    sharpe_after: Optional[float]
    sharpe_improvement: Optional[float]
    is_reverted: bool
    applied_at: str


class ReversionRequest(BaseModel):
    """Request to revert a change."""

    reason: str = Field(..., description="Reason for reversion")
    user_id: str = Field(..., description="User performing reversion")


class SectorBreakdownResponse(BaseModel):
    """Prediction accuracy for one sector."""

    sector: str
    hit_rate: float
    avg_error: float
    count: int
    avg_predicted: float
    avg_realized: float


class ScorecardMissResponse(BaseModel):
    """Single prediction miss/hit row."""

    decision_id: str
    as_of_date: str
    instrument_id: str
    predicted_score: float
    realized_return: float
    hit: bool
    sector: str
    error: float


class ScorecardResponse(BaseModel):
    """Prediction scorecard summary."""

    horizon_days: int
    total_predictions: int
    hit_rate: float
    spearman_rho: float
    avg_predicted_score: float
    avg_realized_return: float
    sector_breakdown: List[SectorBreakdownResponse]
    top_misses: List[ScorecardMissResponse]
    top_hits: List[ScorecardMissResponse]
    date_range_start: str
    date_range_end: str


class LambdaClusterAccuracyResponse(BaseModel):
    """Lambda prediction accuracy for one cluster."""

    cluster_key: str
    sector: str
    soft_target_class: str
    mae: float
    rmse: float
    direction_accuracy: float
    count: int
    avg_predicted: float
    avg_actual: float


class LambdaScorecardResponse(BaseModel):
    """Lambda prediction scorecard summary."""

    market_id: str
    total_predictions: int
    mae: float
    rmse: float
    r_squared: float
    direction_accuracy: float
    avg_predicted: float
    avg_actual: float
    cluster_breakdown: List[LambdaClusterAccuracyResponse]
    date_range_start: str
    date_range_end: str
    data_source: str


# ============================================================================
# Diagnostics Endpoints
# ============================================================================


@intelligence_router.get(
    "/diagnostics/{strategy_id}",
    response_model=DiagnosticReportResponse,
    summary="Get diagnostic report for strategy",
)
async def get_diagnostics(
    strategy_id: str = Path(..., description="Strategy ID to analyze"),
    min_sample_size: int = Query(5, description="Minimum backtest runs required"),
) -> DiagnosticReportResponse:
    """Analyze strategy performance and generate diagnostic report.

    Returns performance statistics, underperforming configs, high-risk configs,
    and configuration comparisons showing potential improvements.
    """
    db_manager = get_db_manager()
    engine = DiagnosticsEngine(db_manager=db_manager)

    try:
        report = engine.analyze_strategy(strategy_id, min_sample_size)

        return DiagnosticReportResponse(
            strategy_id=report.strategy_id,
            overall_sharpe=report.overall_performance.sharpe,
            overall_return=report.overall_performance.return_,
            overall_volatility=report.overall_performance.volatility,
            max_drawdown=report.overall_performance.max_drawdown,
            sample_size=report.overall_performance.sample_size,
            underperforming_count=len(report.underperforming_configs),
            high_risk_count=len(report.high_risk_configs),
            config_comparisons_count=len(report.config_comparisons),
            analysis_timestamp=report.sample_metadata["analysis_timestamp"],
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")


# ============================================================================
# Proposal Endpoints
# ============================================================================


@intelligence_router.post(
    "/proposals/generate/{strategy_id}",
    response_model=List[ProposalResponse],
    summary="Generate improvement proposals",
)
async def generate_proposals(
    strategy_id: str = Path(..., description="Strategy ID"),
    min_confidence: float = Query(0.3, description="Minimum confidence threshold"),
    min_sharpe_improvement: float = Query(
        0.1, description="Minimum Sharpe improvement"
    ),
) -> List[ProposalResponse]:
    """Generate configuration improvement proposals for a strategy.

    Analyzes backtest results and generates actionable recommendations
    with confidence scores and expected impact estimates.
    """
    db_manager = get_db_manager()
    diagnostics_engine = DiagnosticsEngine(db_manager=db_manager)
    generator = ProposalGenerator(
        db_manager=db_manager,
        diagnostics_engine=diagnostics_engine,
        min_confidence_threshold=min_confidence,
        min_sharpe_improvement=min_sharpe_improvement,
    )

    try:
        proposals = generator.generate_proposals(strategy_id, auto_save=True)

        return [
            ProposalResponse(
                proposal_id=p.proposal_id,
                strategy_id=p.strategy_id,
                proposal_type=p.proposal_type,
                target_component=p.target_component,
                current_value=p.current_value,
                proposed_value=p.proposed_value,
                confidence_score=p.confidence_score,
                expected_sharpe_improvement=p.expected_sharpe_improvement,
                expected_return_improvement=p.expected_return_improvement,
                expected_risk_reduction=p.expected_risk_reduction,
                rationale=p.rationale,
                status="PENDING",
                created_at=date.today().isoformat(),
            )
            for p in proposals
        ]

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Generation failed: {str(e)}")


@intelligence_router.get(
    "/proposals",
    response_model=List[ProposalResponse],
    summary="List proposals",
)
async def list_proposals(
    strategy_id: Optional[str] = Query(None, description="Filter by strategy"),
    status: Optional[str] = Query(None, description="Filter by status"),
) -> List[ProposalResponse]:
    """List configuration proposals with optional filters.

    Returns all proposals matching the given filters, ordered by
    expected Sharpe improvement (descending).
    """
    db_manager = get_db_manager()
    diagnostics_engine = DiagnosticsEngine(db_manager=db_manager)
    generator = ProposalGenerator(
        db_manager=db_manager, diagnostics_engine=diagnostics_engine
    )

    # Load pending proposals (or all if status filter provided)
    if status == "PENDING" or status is None:
        proposals_data = generator.load_pending_proposals(strategy_id)
    else:
        # Would need additional method to load by status
        proposals_data = generator.load_pending_proposals(strategy_id)

    return [
        ProposalResponse(
            proposal_id=p["proposal_id"],
            strategy_id=p["strategy_id"],
            proposal_type=p["proposal_type"],
            target_component=p["target_component"],
            current_value=p["current_value"],
            proposed_value=p["proposed_value"],
            confidence_score=p["confidence_score"],
            expected_sharpe_improvement=p["expected_sharpe_improvement"],
            expected_return_improvement=p["expected_return_improvement"],
            expected_risk_reduction=p["expected_risk_reduction"],
            rationale=p["rationale"],
            status=p["status"],
            created_at=p["created_at"].isoformat()
            if hasattr(p["created_at"], "isoformat")
            else str(p["created_at"]),
        )
        for p in proposals_data
    ]


@intelligence_router.post(
    "/proposals/{proposal_id}/approve",
    summary="Approve proposal",
)
async def approve_proposal(
    proposal_id: str = Path(..., description="Proposal ID"),
    request: ProposalActionRequest = Body(...),
) -> Dict[str, str]:
    """Approve a configuration proposal.

    Changes status from PENDING to APPROVED. Approved proposals can then
    be applied using the /apply endpoint.
    """
    db_manager = get_db_manager()
    diagnostics_engine = DiagnosticsEngine(db_manager=db_manager)
    generator = ProposalGenerator(
        db_manager=db_manager, diagnostics_engine=diagnostics_engine
    )

    try:
        generator.approve_proposal(proposal_id, request.user_id)
        return {
            "status": "success",
            "message": f"Proposal {proposal_id} approved by {request.user_id}",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Approval failed: {str(e)}")


@intelligence_router.post(
    "/proposals/{proposal_id}/reject",
    summary="Reject proposal",
)
async def reject_proposal(
    proposal_id: str = Path(..., description="Proposal ID"),
    request: ProposalActionRequest = Body(...),
) -> Dict[str, str]:
    """Reject a configuration proposal.

    Changes status from PENDING to REJECTED. Rejected proposals are
    not applied and remain in history for audit purposes.
    """
    db_manager = get_db_manager()
    diagnostics_engine = DiagnosticsEngine(db_manager=db_manager)
    generator = ProposalGenerator(
        db_manager=db_manager, diagnostics_engine=diagnostics_engine
    )

    try:
        generator.reject_proposal(proposal_id, request.user_id)
        return {
            "status": "success",
            "message": f"Proposal {proposal_id} rejected by {request.user_id}",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Rejection failed: {str(e)}")


# ============================================================================
# Application Endpoints
# ============================================================================


@intelligence_router.post(
    "/proposals/{proposal_id}/apply",
    response_model=ApplicationResultResponse,
    summary="Apply proposal",
)
async def apply_proposal(
    proposal_id: str = Path(..., description="Proposal ID"),
    request: ProposalActionRequest = Body(...),
    dry_run: bool = Query(False, description="Validate without applying"),
) -> ApplicationResultResponse:
    """Apply an approved configuration proposal.

    Validates the proposal, applies the configuration change,
    records it in config_change_log, and updates proposal status to APPLIED.
    """
    db_manager = get_db_manager()
    applicator = ProposalApplicator(db_manager=db_manager, dry_run=dry_run)

    result = applicator.apply_proposal(proposal_id, request.user_id)

    return ApplicationResultResponse(
        success=result.success,
        change_id=result.change_id,
        proposal_id=result.proposal_id,
        error_message=result.error_message,
        applied_at=result.applied_at.isoformat() if result.applied_at else None,
    )


@intelligence_router.post(
    "/proposals/apply-batch",
    response_model=List[ApplicationResultResponse],
    summary="Apply multiple approved proposals",
)
async def apply_batch(
    request: ProposalActionRequest = Body(...),
    strategy_id: Optional[str] = Query(None, description="Filter by strategy"),
    max_proposals: int = Query(10, description="Maximum proposals to apply"),
    dry_run: bool = Query(False, description="Validate without applying"),
) -> List[ApplicationResultResponse]:
    """Apply all approved proposals for a strategy in batch.

    Processes proposals in order of expected Sharpe improvement.
    Stops on first error to avoid cascading failures.
    """
    db_manager = get_db_manager()
    applicator = ProposalApplicator(db_manager=db_manager, dry_run=dry_run)

    results = applicator.apply_approved_proposals(
        strategy_id=strategy_id,
        applied_by=request.user_id,
        max_proposals=max_proposals,
    )

    return [
        ApplicationResultResponse(
            success=r.success,
            change_id=r.change_id,
            proposal_id=r.proposal_id,
            error_message=r.error_message,
            applied_at=r.applied_at.isoformat() if r.applied_at else None,
        )
        for r in results
    ]


# ============================================================================
# Change Log Endpoints
# ============================================================================


@intelligence_router.get(
    "/changes",
    response_model=List[ChangeLogResponse],
    summary="List configuration changes",
)
async def list_changes(
    strategy_id: Optional[str] = Query(None, description="Filter by strategy"),
    is_reverted: Optional[bool] = Query(None, description="Filter by reversion status"),
) -> List[ChangeLogResponse]:
    """List applied configuration changes with performance outcomes."""
    db_manager = get_db_manager()

    # Build query
    # Layer 0 contract: config_change_log is append-only.
    # - "is_reverted" is computed via existence of a REVERT row referencing the original.
    # - performance metrics are sourced from the latest config_change_evaluations row.
    sql = """
        SELECT
            c.change_id,
            c.proposal_id,
            c.strategy_id,
            c.change_type,
            c.target_component,
            e.sharpe_before,
            e.sharpe_after,
            (c.is_reverted OR EXISTS (
                SELECT 1
                FROM config_change_log r
                WHERE r.reverts_change_id = c.change_id
            )) AS is_reverted,
            c.applied_at
        FROM config_change_log c
        LEFT JOIN LATERAL (
            SELECT ee.sharpe_before, ee.sharpe_after
            FROM config_change_evaluations ee
            WHERE ee.change_id = c.change_id
            ORDER BY ee.created_at DESC, ee.evaluation_id DESC
            LIMIT 1
        ) e ON TRUE
        WHERE c.reverts_change_id IS NULL
          AND c.strategy_id IS NOT NULL
    """
    params = []

    if strategy_id:
        sql += " AND c.strategy_id = %s"
        params.append(strategy_id)

    if is_reverted is not None:
        sql += " AND (c.is_reverted OR EXISTS (SELECT 1 FROM config_change_log r WHERE r.reverts_change_id = c.change_id)) = %s"
        params.append(is_reverted)

    sql += " ORDER BY c.applied_at DESC LIMIT 50"

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, tuple(params) if params else ())
            rows = cursor.fetchall()
        finally:
            cursor.close()

    changes = []
    for row in rows:
        sharpe_improvement = None
        if row[5] is not None and row[6] is not None:
            sharpe_improvement = row[6] - row[5]

        changes.append(
            ChangeLogResponse(
                change_id=row[0],
                proposal_id=row[1],
                strategy_id=row[2],
                change_type=row[3],
                target_component=row[4],
                sharpe_before=row[5],
                sharpe_after=row[6],
                sharpe_improvement=sharpe_improvement,
                is_reverted=row[7],
                applied_at=row[8].isoformat() if hasattr(row[8], "isoformat") else str(row[8]),
            )
        )

    return changes


@intelligence_router.post(
    "/changes/{change_id}/revert",
    summary="Revert configuration change",
)
async def revert_change(
    change_id: str = Path(..., description="Change ID"),
    request: ReversionRequest = Body(...),
    dry_run: bool = Query(False, description="Validate without reverting"),
) -> Dict[str, str]:
    """Revert a previously applied configuration change.

    Restores the previous configuration value, records a REVERT entry,
    and updates related proposal status to REVERTED.
    """
    db_manager = get_db_manager()
    applicator = ProposalApplicator(db_manager=db_manager, dry_run=dry_run)

    result = applicator.revert_change(change_id, request.reason, request.user_id)

    if result.success:
        return {
            "status": "success",
            "message": f"Change {change_id} reverted at {result.reverted_at}",
        }
    else:
        raise HTTPException(status_code=400, detail=result.error_message)


# ============================================================================
# Health Check
# ============================================================================


# ============================================================================
# Prediction Scorecard Endpoints
# ============================================================================


@intelligence_router.get(
    "/scorecard",
    response_model=ScorecardResponse,
    summary="Prediction scorecard",
)
async def get_scorecard(
    horizon_days: int = Query(21, description="Forward return horizon in days"),
    max_decisions: int = Query(200, description="Max ASSESSMENT decisions to evaluate"),
) -> ScorecardResponse:
    """Build a prediction accuracy scorecard.

    Compares assessment instrument scores against realized forward returns
    to measure hit rate, rank correlation, and sector-level accuracy.
    """
    db_manager = get_db_manager()
    scorecard = PredictionScorecard(db_manager=db_manager)

    try:
        report = scorecard.build_scorecard(
            horizon_days=horizon_days,
            max_decisions=max_decisions,
        )

        def _miss_resp(r):
            return ScorecardMissResponse(
                decision_id=r.decision_id,
                as_of_date=r.as_of_date.isoformat(),
                instrument_id=r.instrument_id,
                predicted_score=r.predicted_score,
                realized_return=r.realized_return,
                hit=r.hit,
                sector=r.sector,
                error=r.error,
            )

        return ScorecardResponse(
            horizon_days=report.horizon_days,
            total_predictions=report.total_predictions,
            hit_rate=report.hit_rate,
            spearman_rho=report.spearman_rho,
            avg_predicted_score=report.avg_predicted_score,
            avg_realized_return=report.avg_realized_return,
            sector_breakdown=[
                SectorBreakdownResponse(
                    sector=s.sector,
                    hit_rate=s.hit_rate,
                    avg_error=s.avg_error,
                    count=s.count,
                    avg_predicted=s.avg_predicted,
                    avg_realized=s.avg_realized,
                )
                for s in report.sector_breakdown
            ],
            top_misses=[_miss_resp(r) for r in report.top_misses],
            top_hits=[_miss_resp(r) for r in report.top_hits],
            date_range_start=report.date_range[0].isoformat(),
            date_range_end=report.date_range[1].isoformat(),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Scorecard failed: {str(e)}")


@intelligence_router.get(
    "/lambda-scorecard",
    response_model=LambdaScorecardResponse,
    summary="Lambda prediction scorecard",
)
async def get_lambda_scorecard(
    market_id: str = Query("US_EQ", description="Market ID"),
    max_dates: int = Query(200, description="Max dates to evaluate"),
) -> LambdaScorecardResponse:
    """Build a lambda (opportunity density) prediction scorecard.

    Compares predicted lambda_hat against realised next-day lambda per cluster
    to measure MAE, RMSE, R², and direction accuracy.
    """
    db_manager = get_db_manager()
    scorecard = LambdaScorecard(db_manager=db_manager)

    try:
        report = scorecard.build_scorecard(
            market_id=market_id,
            max_dates=max_dates,
        )

        return LambdaScorecardResponse(
            market_id=report.market_id,
            total_predictions=report.total_predictions,
            mae=report.mae,
            rmse=report.rmse,
            r_squared=report.r_squared,
            direction_accuracy=report.direction_accuracy,
            avg_predicted=report.avg_predicted,
            avg_actual=report.avg_actual,
            cluster_breakdown=[
                LambdaClusterAccuracyResponse(
                    cluster_key=c.cluster_key,
                    sector=c.sector,
                    soft_target_class=c.soft_target_class,
                    mae=c.mae,
                    rmse=c.rmse,
                    direction_accuracy=c.direction_accuracy,
                    count=c.count,
                    avg_predicted=c.avg_predicted,
                    avg_actual=c.avg_actual,
                )
                for c in report.cluster_breakdown
            ],
            date_range_start=report.date_range[0].isoformat(),
            date_range_end=report.date_range[1].isoformat(),
            data_source=report.data_source,
        )
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Lambda scorecard failed: {str(e)}",
        )


@intelligence_router.get("/health")
async def health_check() -> Dict[str, str]:
    """Intelligence API health check."""
    return {"status": "healthy", "service": "meta-kronos-intelligence"}
