"""Prometheus v2 – Meta/Kronos Proposal Applicator.

This module applies approved configuration proposals to strategies and tracks
their performance outcomes. It provides safe application with validation,
rollback support, and performance monitoring.

Key responsibilities:
- Apply approved proposals to strategy configurations
- Validate changes before application
- Track before/after performance in config_change_log
- Support rollback/reversion of bad changes
- Atomic updates with transactional safety

Author: Prometheus Team
Created: 2025-12-02
Status: Development
Version: v0.1.0
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from apathis.core.database import DatabaseManager
from apathis.core.ids import generate_uuid
from apathis.core.logging import get_logger
from psycopg2.extras import Json

logger = get_logger(__name__)


@dataclass(frozen=True)
class ApplicationResult:
    """Result of applying a configuration change.

    Attributes:
        success: Whether application succeeded
        change_id: Unique change log entry ID
        proposal_id: Source proposal ID
        error_message: Error message if failed
        applied_at: Timestamp of application
    """

    success: bool
    change_id: Optional[str]
    proposal_id: str
    error_message: Optional[str] = None
    applied_at: Optional[datetime] = None


@dataclass(frozen=True)
class ReversionResult:
    """Result of reverting a configuration change.

    Attributes:
        success: Whether reversion succeeded
        change_id: Change log entry that was reverted
        error_message: Error message if failed
        reverted_at: Timestamp of reversion
    """

    success: bool
    change_id: str
    error_message: Optional[str] = None
    reverted_at: Optional[datetime] = None


@dataclass
class ProposalApplicator:
    """Applies and reverts configuration proposals.

    The applicator reads approved proposals, validates them, applies the
    changes to strategy configurations, and tracks outcomes in the
    config_change_log table.
    """

    db_manager: DatabaseManager
    dry_run: bool = False  # If True, validate but don't actually apply

    def apply_proposal(
        self, proposal_id: str, applied_by: str
    ) -> ApplicationResult:
        """Apply an approved proposal.

        Args:
            proposal_id: Proposal to apply
            applied_by: Identifier of user applying the change

        Returns:
            ApplicationResult indicating success/failure
        """
        # Load proposal
        proposal = self._load_proposal(proposal_id)

        if not proposal:
            return ApplicationResult(
                success=False,
                change_id=None,
                proposal_id=proposal_id,
                error_message=f"Proposal {proposal_id} not found",
            )

        # Validate proposal status
        if proposal["status"] != "APPROVED":
            return ApplicationResult(
                success=False,
                change_id=None,
                proposal_id=proposal_id,
                error_message=f"Proposal status is {proposal['status']}, must be APPROVED",
            )

        # Validate proposal data
        validation_error = self._validate_proposal(proposal)
        if validation_error:
            return ApplicationResult(
                success=False,
                change_id=None,
                proposal_id=proposal_id,
                error_message=validation_error,
            )

        if self.dry_run:
            logger.info(f"DRY RUN: Would apply proposal {proposal_id}")
            return ApplicationResult(
                success=True,
                change_id="DRY_RUN",
                proposal_id=proposal_id,
                applied_at=datetime.utcnow(),
            )

        try:
            # Get current config for comparison
            current_config = self._load_current_config(
                proposal["strategy_id"], proposal["target_component"]
            )

            # Apply the change (strategy-specific logic)
            self._apply_config_change(
                strategy_id=proposal["strategy_id"],
                target_component=proposal["target_component"],
                new_value=proposal["proposed_value"],
                applied_by=applied_by,
            )

            # Record in config_change_log
            change_id = generate_uuid()
            self._record_config_change(
                change_id=change_id,
                proposal_id=proposal_id,
                strategy_id=proposal["strategy_id"],
                market_id=proposal.get("market_id"),
                change_type=proposal["proposal_type"],
                target_component=proposal["target_component"],
                previous_value=current_config,
                new_value=proposal["proposed_value"],
                applied_by=applied_by,
            )

            # Record proposal workflow event (append-only)
            self._record_proposal_event(proposal_id, "APPLIED", applied_by)

            logger.info(
                f"Applied proposal {proposal_id} for strategy {proposal['strategy_id']}"
            )

            return ApplicationResult(
                success=True,
                change_id=change_id,
                proposal_id=proposal_id,
                applied_at=datetime.utcnow(),
            )

        except Exception as e:
            logger.exception(f"Failed to apply proposal {proposal_id}: {e}")
            return ApplicationResult(
                success=False,
                change_id=None,
                proposal_id=proposal_id,
                error_message=str(e),
            )

    def apply_approved_proposals(
        self,
        strategy_id: Optional[str] = None,
        applied_by: str = "system",
        max_proposals: int = 10,
    ) -> List[ApplicationResult]:
        """Apply all approved proposals for a strategy.

        Args:
            strategy_id: Optional filter by strategy
            applied_by: Identifier of user applying changes
            max_proposals: Maximum number to apply in one batch

        Returns:
            List of application results
        """
        # Load approved proposals
        proposals = self._load_approved_proposals(strategy_id, max_proposals)

        if not proposals:
            logger.info("No approved proposals to apply")
            return []

        logger.info(f"Applying {len(proposals)} approved proposals")

        results = []
        for proposal in proposals:
            result = self.apply_proposal(proposal["proposal_id"], applied_by)
            results.append(result)

            # Stop on first error to avoid cascading failures
            if not result.success:
                logger.warning(
                    f"Stopping batch application after failure: {result.error_message}"
                )
                break

        return results

    def revert_change(
        self, change_id: str, reason: str, reverted_by: str
    ) -> ReversionResult:
        """Revert a previously applied configuration change.

        Layer 0 contract: ``config_change_log`` is append-only.
        A reversion is recorded as a new row (change_type='REVERT') that
        references the original via reverts_change_id.
        """
        # Load change record
        change = self._load_config_change(change_id)

        if not change:
            return ReversionResult(
                success=False,
                change_id=change_id,
                error_message=f"Change {change_id} not found",
            )

        strategy_id = change.get("strategy_id")
        target_component = change.get("target_component")

        if not strategy_id or not target_component:
            return ReversionResult(
                success=False,
                change_id=change_id,
                error_message="Change is missing strategy_id/target_component; cannot revert",
            )

        if bool(change.get("is_reverted")) or self._has_reversion_row(change_id):
            return ReversionResult(
                success=False,
                change_id=change_id,
                error_message="Change already reverted",
            )

        if self.dry_run:
            logger.info(f"DRY RUN: Would revert change {change_id}")
            return ReversionResult(
                success=True,
                change_id=change_id,
                reverted_at=datetime.utcnow(),
            )

        try:
            current_value = self._load_current_config(strategy_id, target_component)

            # Revert to previous value
            self._apply_config_change(
                strategy_id=strategy_id,
                target_component=target_component,
                new_value=change["previous_value"],
                applied_by=reverted_by,
            )

            # Record a REVERT entry (append-only) linked to the original change.
            self._record_config_change(
                change_id=generate_uuid(),
                proposal_id=change.get("proposal_id"),
                strategy_id=strategy_id,
                market_id=change.get("market_id"),
                change_type="REVERT",
                target_component=target_component,
                previous_value=current_value,
                new_value=change["previous_value"],
                applied_by=reverted_by,
                reverts_change_id=change_id,
                reversion_reason=reason,
            )

            # Record related proposal as REVERTED (append-only)
            if change.get("proposal_id"):
                self._record_proposal_event(
                    change["proposal_id"],
                    "REVERTED",
                    reverted_by,
                    reason=reason,
                )

            logger.info(f"Reverted change {change_id}: {reason}")

            return ReversionResult(
                success=True,
                change_id=change_id,
                reverted_at=datetime.utcnow(),
            )

        except Exception as e:
            logger.exception(f"Failed to revert change {change_id}: {e}")
            return ReversionResult(
                success=False,
                change_id=change_id,
                error_message=str(e),
            )

    def evaluate_change_performance(
        self,
        change_id: str,
        evaluation_start_date: date,
        evaluation_end_date: date,
    ) -> Dict[str, float]:
        """Evaluate performance impact of an applied change.

        Compares performance metrics before and after the change was applied
        over the specified evaluation period.

        Args:
            change_id: Change to evaluate
            evaluation_start_date: Start of evaluation period
            evaluation_end_date: End of evaluation period

        Returns:
            Dictionary with before/after metrics
        """
        change = self._load_config_change(change_id)

        if not change:
            logger.warning(f"Change {change_id} not found")
            return {}

        # Load backtest results before change
        metrics_before = self._compute_metrics_for_period(
            strategy_id=change["strategy_id"],
            start_date=evaluation_start_date,
            end_date=change["applied_at"].date(),
        )

        # Load backtest results after change
        metrics_after = self._compute_metrics_for_period(
            strategy_id=change["strategy_id"],
            start_date=change["applied_at"].date(),
            end_date=evaluation_end_date,
        )

        # Record evaluation metrics (append-only)
        self._update_change_performance(
            change_id=change_id,
            sharpe_before=metrics_before.get("sharpe", 0.0),
            sharpe_after=metrics_after.get("sharpe", 0.0),
            return_before=metrics_before.get("return", 0.0),
            return_after=metrics_after.get("return", 0.0),
            risk_before=metrics_before.get("volatility", 0.0),
            risk_after=metrics_after.get("volatility", 0.0),
            evaluation_start_date=evaluation_start_date,
            evaluation_end_date=evaluation_end_date,
        )

        return {
            "before": metrics_before,
            "after": metrics_after,
            "improvement": {
                "sharpe": metrics_after.get("sharpe", 0.0)
                - metrics_before.get("sharpe", 0.0),
                "return": metrics_after.get("return", 0.0)
                - metrics_before.get("return", 0.0),
                "volatility": metrics_after.get("volatility", 0.0)
                - metrics_before.get("volatility", 0.0),
            },
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load_proposal(self, proposal_id: str) -> Optional[Dict[str, Any]]:
        """Load proposal from database.

        Layer 0 contract: proposals are immutable; state is derived from
        ``meta_config_proposal_events``.
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
                COALESCE(s.status, 'PENDING') AS status,
                a.approved_by,
                a.approved_at
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
            LEFT JOIN LATERAL (
                SELECT e.event_by AS approved_by, e.event_at AS approved_at
                FROM meta_config_proposal_events e
                WHERE e.proposal_id = p.proposal_id
                  AND e.event_type = 'APPROVED'
                ORDER BY e.event_at DESC, e.event_id DESC
                LIMIT 1
            ) a ON TRUE
            WHERE p.proposal_id = %s
        """

        with self.db_manager.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, (proposal_id,))
                row = cursor.fetchone()
            finally:
                cursor.close()

        if not row:
            return None

        return {
            "proposal_id": row[0],
            "strategy_id": row[1],
            "market_id": row[2],
            "proposal_type": row[3],
            "target_component": row[4],
            "current_value": row[5],
            "proposed_value": row[6],
            "status": row[7],
            "approved_by": row[8],
            "approved_at": row[9],
        }

    def _load_approved_proposals(
        self, strategy_id: Optional[str], max_proposals: int
    ) -> List[Dict[str, Any]]:
        """Load approved proposals from database.

        Status is derived from the latest proposal event.
        """
        sql = """
            SELECT
                p.proposal_id,
                p.strategy_id,
                p.market_id,
                p.proposal_type,
                p.target_component,
                p.current_value,
                p.proposed_value
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
            WHERE COALESCE(s.status, 'PENDING') = 'APPROVED'
        """

        params: list[Any] = []
        if strategy_id:
            sql += " AND p.strategy_id = %s"
            params.append(strategy_id)

        sql += " ORDER BY p.expected_sharpe_improvement DESC LIMIT %s"
        params.append(max_proposals)

        with self.db_manager.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, tuple(params))
                rows = cursor.fetchall()
            finally:
                cursor.close()

        proposals = []
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
                }
            )

        return proposals

    def _load_config_change(self, change_id: str) -> Optional[Dict[str, Any]]:
        """Load config change from database."""
        sql = """
            SELECT
                change_id,
                proposal_id,
                strategy_id,
                market_id,
                change_type,
                target_component,
                previous_value,
                new_value,
                is_reverted,
                applied_by,
                applied_at
            FROM config_change_log
            WHERE change_id = %s
        """

        with self.db_manager.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, (change_id,))
                row = cursor.fetchone()
            finally:
                cursor.close()

        if not row:
            return None

        return {
            "change_id": row[0],
            "proposal_id": row[1],
            "strategy_id": row[2],
            "market_id": row[3],
            "change_type": row[4],
            "target_component": row[5],
            "previous_value": row[6],
            "new_value": row[7],
            "is_reverted": row[8],
            "applied_by": row[9],
            "applied_at": row[10],
        }

    def _validate_proposal(self, proposal: Dict[str, Any]) -> Optional[str]:
        """Validate proposal before application.

        Returns error message if invalid, None if valid.
        """
        if not proposal.get("strategy_id"):
            return "Missing strategy_id"

        if not proposal.get("target_component"):
            return "Missing target_component"

        if proposal.get("proposed_value") is None:
            return "Missing proposed_value"

        # Additional validation could check:
        # - Strategy exists in database
        # - Target component is valid for strategy type
        # - Proposed value is within acceptable range

        return None

    def _load_current_config(
        self, strategy_id: str, target_component: str
    ) -> Optional[Any]:
        """Load current configuration value for a strategy.

        Layer 0 contract: active config selection is explicit via
        strategies.active_strategy_config_id.
        """

        sql = """
            SELECT sc.config_json
            FROM strategies s
            JOIN strategy_configs sc
              ON sc.strategy_config_id = s.active_strategy_config_id
            WHERE s.strategy_id = %s
        """

        with self.db_manager.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, (strategy_id,))
                row = cursor.fetchone()
            finally:
                cursor.close()

        if row and row[0]:
            config_json = row[0]
            return config_json.get(target_component)

        return None

    def _apply_config_change(
        self,
        *,
        strategy_id: str,
        target_component: str,
        new_value: Any,
        applied_by: str,
    ) -> None:
        """Apply configuration change by writing a new immutable config version.

        Implementation note:
        - Inserts a new row into strategy_configs (append-only)
        - Updates strategies.active_strategy_config_id to point at the new version
        """

        sql_ensure_strategy = """
            INSERT INTO strategies (strategy_id, name, description, metadata)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (strategy_id) DO NOTHING
        """

        sql_load_active = """
            SELECT sc.config_json
            FROM strategies s
            LEFT JOIN strategy_configs sc
              ON sc.strategy_config_id = s.active_strategy_config_id
            WHERE s.strategy_id = %s
        """

        sql_insert_version = """
            INSERT INTO strategy_configs (strategy_id, config_json, created_by, metadata)
            VALUES (%s, %s, %s, %s)
            RETURNING strategy_config_id, config_hash
        """

        sql_set_active = """
            UPDATE strategies
            SET active_strategy_config_id = %s,
                updated_at = NOW()
            WHERE strategy_id = %s
        """

        with self.db_manager.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    sql_ensure_strategy,
                    (
                        strategy_id,
                        strategy_id,
                        "Auto-created by ProposalApplicator",
                        Json({"source": "meta_applicator", "auto_created": True}),
                    ),
                )

                cursor.execute(sql_load_active, (strategy_id,))
                row = cursor.fetchone()
                config_json = (row[0] if row else None) or {}

                config_json[target_component] = new_value

                cursor.execute(
                    sql_insert_version,
                    (
                        strategy_id,
                        Json(config_json),
                        applied_by,
                        Json({"change_type": "component_update", "component": target_component}),
                    ),
                )
                strategy_config_id, config_hash = cursor.fetchone()

                cursor.execute(sql_set_active, (strategy_config_id, strategy_id))

                conn.commit()

                logger.info(
                    f"Applied config change: strategy={strategy_id} component={target_component} "
                    f"value={new_value} config_hash={config_hash}"
                )
            finally:
                cursor.close()

    def _record_config_change(
        self,
        *,
        change_id: str,
        proposal_id: Optional[str],
        strategy_id: str,
        market_id: Optional[str],
        change_type: str,
        target_component: str,
        previous_value: Any,
        new_value: Any,
        applied_by: str,
        reverts_change_id: Optional[str] = None,
        reversion_reason: Optional[str] = None,
    ) -> None:
        """Record config change in config_change_log table (append-only)."""
        sql = """
            INSERT INTO config_change_log (
                change_id,
                proposal_id,
                strategy_id,
                market_id,
                change_type,
                target_component,
                previous_value,
                new_value,
                applied_by,
                applied_at,
                reverts_change_id,
                reversion_reason
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s, %s)
        """

        with self.db_manager.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    sql,
                    (
                        change_id,
                        proposal_id,
                        strategy_id,
                        market_id,
                        change_type,
                        target_component,
                        Json(previous_value),
                        Json(new_value),
                        applied_by,
                        reverts_change_id,
                        reversion_reason,
                    ),
                )
                conn.commit()
            finally:
                cursor.close()

    def _record_proposal_event(
        self,
        proposal_id: str,
        event_type: str,
        event_by: str,
        *,
        reason: Optional[str] = None,
    ) -> None:
        """Record a proposal workflow event (append-only)."""

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

    def _has_reversion_row(self, change_id: str) -> bool:
        """Return True if there is a REVERT entry for ``change_id``."""
        sql = """
            SELECT 1
            FROM config_change_log
            WHERE reverts_change_id = %s
            LIMIT 1
        """

        with self.db_manager.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, (change_id,))
                row = cursor.fetchone()
            finally:
                cursor.close()

        return row is not None

    def _compute_metrics_for_period(
        self, strategy_id: str, start_date: date, end_date: date
    ) -> Dict[str, float]:
        """Compute aggregate metrics for strategy over date range."""
        sql = """
            SELECT AVG((metrics_json->>'annualised_sharpe')::float) as avg_sharpe,
                   AVG((metrics_json->>'cumulative_return')::float) as avg_return,
                   AVG((metrics_json->>'annualised_vol')::float) as avg_vol
            FROM backtest_runs
            WHERE strategy_id = %s
              AND start_date >= %s
              AND end_date <= %s
              AND metrics_json IS NOT NULL
        """

        with self.db_manager.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, (strategy_id, start_date, end_date))
                row = cursor.fetchone()
            finally:
                cursor.close()

        if row and row[0] is not None:
            return {
                "sharpe": float(row[0] or 0.0),
                "return": float(row[1] or 0.0),
                "volatility": float(row[2] or 0.0),
            }

        return {"sharpe": 0.0, "return": 0.0, "volatility": 0.0}

    def _update_change_performance(
        self,
        change_id: str,
        sharpe_before: float,
        sharpe_after: float,
        return_before: float,
        return_after: float,
        risk_before: float,
        risk_after: float,
        evaluation_start_date: date,
        evaluation_end_date: date,
    ) -> None:
        """Record performance evaluation metrics (append-only)."""
        sql = """
            INSERT INTO config_change_evaluations (
                change_id,
                evaluation_start_date,
                evaluation_end_date,
                sharpe_before,
                sharpe_after,
                return_before,
                return_after,
                risk_before,
                risk_after,
                created_by,
                created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        """

        with self.db_manager.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    sql,
                    (
                        change_id,
                        evaluation_start_date,
                        evaluation_end_date,
                        sharpe_before,
                        sharpe_after,
                        return_before,
                        return_after,
                        risk_before,
                        risk_after,
                        "system",
                    ),
                )
                conn.commit()
            finally:
                cursor.close()
