"""Prometheus v2 – Portfolio & Risk Engine orchestration.

This module defines the PortfolioModel protocol and PortfolioEngine
orchestrator used to construct target portfolios and (optionally) risk
reports. It follows the same pattern as other engines: models encapsulate
optimisation logic, while the engine coordinates storage and logging.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date
from typing import Protocol

from apathis.core.database import DatabaseManager
from apathis.core.ids import generate_uuid
from apathis.core.logging import get_logger
from psycopg2.extras import Json

from prometheus.risk import apply_risk_constraints
from prometheus.universe.engine import UniverseMember

from .types import RiskReport, TargetPortfolio

logger = get_logger(__name__)


class PortfolioModel(Protocol):
    """Protocol for portfolio construction models.

    Implementations encapsulate all optimisation and risk logic for a
    given portfolio/book. The initial implementation focuses on a simple
    long-only equity book built from universe members.
    """

    def build_target_portfolio(self, portfolio_id: str, as_of_date: date) -> TargetPortfolio:
        ...  # pragma: no cover - interface

    def build_risk_report(
        self,
        portfolio_id: str,
        as_of_date: date,
        target: TargetPortfolio | None = None,
    ) -> RiskReport | None:
        """Optional risk-report generation.

        Models may choose to return None if they do not implement a
        separate risk-report step.
        """


@dataclass
class PortfolioStorage:
    """Persistence helper for portfolio targets.

    For this iteration we persist per-entity weights into the existing
    ``book_targets`` table used by the pipeline's books phase. This keeps
    behaviour backwards compatible while allowing a dedicated portfolio
    engine to drive the data written there.
    """

    db_manager: DatabaseManager

    def save_book_targets(
        self,
        portfolio_id: str,
        region: str,
        as_of_date: date,
        members: list[UniverseMember],
        weights: dict[str, float],
        metadata_extra: dict[str, object] | None = None,
    ) -> None:
        """Persist target weights into ``book_targets``.

        Args:
            portfolio_id: Logical book/portfolio identifier used as
                ``book_id`` in the table.
            region: Logical region for the run (e.g. "US").
            as_of_date: Date of the target snapshot.
            members: Universe members corresponding to the weights.
            weights: Mapping from entity_id to target weight.
            metadata_extra: Additional metadata merged into the JSON
                payload for each row.
        """

        if not members:
            return

        sql = """
            INSERT INTO book_targets (
                target_id,
                book_id,
                as_of_date,
                region,
                entity_type,
                entity_id,
                target_weight,
                metadata,
                created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (book_id, as_of_date, region, entity_type, entity_id)
            DO UPDATE SET
                target_weight = EXCLUDED.target_weight,
                metadata = EXCLUDED.metadata,
                created_at = NOW()
        """

        metadata_extra = metadata_extra or {}

        with self.db_manager.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                for member in members:
                    w = float(weights.get(member.entity_id, 0.0))
                    target_id = generate_uuid()
                    payload = {"universe_id": member.universe_id} | metadata_extra
                    cursor.execute(
                        sql,
                        (
                            target_id,
                            portfolio_id,
                            as_of_date,
                            region,
                            member.entity_type,
                            member.entity_id,
                            w,
                            Json(payload),
                        ),
                    )
                conn.commit()
            finally:
                cursor.close()

    def save_target_portfolio(
        self,
        strategy_id: str,
        target: TargetPortfolio,
    ) -> None:
        """Persist a TargetPortfolio into ``target_portfolios``.

        The ``target_positions`` column stores a JSON mapping from
        instrument_id to weight under the key ``"weights"``.
        """

        if not target.weights:
            return

        sql = """
            INSERT INTO target_portfolios (
                target_id,
                strategy_id,
                portfolio_id,
                as_of_date,
                target_positions,
                metadata,
                created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (target_id) DO NOTHING
        """

        target_id = generate_uuid()
        positions_payload = Json({"weights": target.weights})
        metadata_payload = Json(target.metadata)

        with self.db_manager.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    sql,
                    (
                        target_id,
                        strategy_id,
                        target.portfolio_id,
                        target.as_of_date,
                        positions_payload,
                        metadata_payload,
                    ),
                )
                conn.commit()
            finally:
                cursor.close()

    def save_portfolio_risk_report(
        self,
        model_id: str,
        report: RiskReport,
    ) -> None:
        """Persist a RiskReport into ``portfolio_risk_reports``.

        For v1 we normalise portfolio_value to 1.0 and derive basic
        leverage/gross/net metrics from the report's risk_metrics.
        """

        sql = """
            INSERT INTO portfolio_risk_reports (
                report_id,
                portfolio_id,
                as_of_date,
                portfolio_value,
                cash,
                net_exposure,
                gross_exposure,
                leverage,
                risk_metrics,
                scenario_pnl,
                exposures_by_sector,
                exposures_by_factor,
                metadata,
                created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        """

        report_id = generate_uuid()
        portfolio_value = 1.0
        cash = 0.0
        net_exposure = float(report.risk_metrics.get("net_exposure", 0.0))
        gross_exposure = float(report.risk_metrics.get("gross_exposure", 0.0))
        leverage = gross_exposure

        risk_metrics_payload = Json(report.risk_metrics)
        scenario_pnl_payload = Json(report.scenario_pnl)
        exposures_sector_payload = Json(report.exposures)
        exposures_factor_payload = Json({})
        metadata_payload = Json({"model_id": model_id} | report.metadata)

        with self.db_manager.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    sql,
                    (
                        report_id,
                        report.portfolio_id,
                        report.as_of_date,
                        portfolio_value,
                        cash,
                        net_exposure,
                        gross_exposure,
                        leverage,
                        risk_metrics_payload,
                        scenario_pnl_payload,
                        exposures_sector_payload,
                        exposures_factor_payload,
                        metadata_payload,
                    ),
                )
                conn.commit()
            finally:
                cursor.close()


@dataclass
class PortfolioEngine:
    """Orchestrator for portfolio optimisation and persistence."""

    model: PortfolioModel
    storage: PortfolioStorage
    region: str

    def optimize_and_save(
        self,
        portfolio_id: str,
        as_of_date: date,
        *,
        budget_mult: float | None = None,
        budget_metadata: dict[str, object] | None = None,
        apply_risk: bool = False,
        risk_strategy_id: str | None = None,
    ) -> TargetPortfolio:
        """Run optimisation for a portfolio and persist targets.

        Optionally applies a *budget multiplier* by scaling weights with
        ``budget_mult`` (in [0,1]). This is intended to represent
        **Meta-level capital allocation** (i.e. how much capital the book
        is allowed to deploy) rather than an internal "cash overlay".

        Returns the resulting :class:`TargetPortfolio`.
        """

        target = self.model.build_target_portfolio(portfolio_id, as_of_date)

        if budget_mult is not None:
            m = float(budget_mult)
            if m < 0.0:
                m = 0.0
            if m > 1.0:
                m = 1.0

            if m != 1.0 or budget_metadata:
                weights_scaled = {k: float(v) * m for k, v in target.weights.items()}

                # Scale aggregated exposures (factor/sector buckets) by the same
                # multiplier so they remain consistent with weights.
                exposures_scaled = {k: float(v) * m for k, v in target.factor_exposures.items()}

                risk_metrics = dict(target.risk_metrics)
                net_exposure = float(sum(weights_scaled.values()))
                gross_exposure = float(sum(abs(w) for w in weights_scaled.values()))
                risk_metrics["net_exposure"] = net_exposure
                risk_metrics["gross_exposure"] = gross_exposure
                risk_metrics["cash_weight"] = max(0.0, 1.0 - net_exposure)

                # Scale weight-based aggregates if present.
                if "fragility_exposure" in risk_metrics:
                    risk_metrics["fragility_exposure"] = float(risk_metrics["fragility_exposure"]) * m
                if "expected_volatility" in risk_metrics:
                    risk_metrics["expected_volatility"] = float(risk_metrics["expected_volatility"]) * m

                # Surface allocation diagnostics.
                risk_metrics["meta_budget_mult"] = m
                if budget_metadata is not None:
                    frag_mult = budget_metadata.get("fragility_budget_mult")
                    if frag_mult is not None:
                        risk_metrics["fragility_budget_mult"] = float(frag_mult)

                constraints_status = dict(target.constraints_status)
                constraints_status["meta_budget_applied"] = True

                meta = dict(target.metadata)
                meta["meta_budget"] = {"budget_mult": m} | (budget_metadata or {})

                target = replace(
                    target,
                    weights=weights_scaled,
                    expected_return=float(target.expected_return) * m,
                    expected_volatility=float(target.expected_volatility) * m,
                    risk_metrics=risk_metrics,
                    factor_exposures=exposures_scaled,
                    constraints_status=constraints_status,
                    metadata=meta,
                )

        # Optionally apply Risk Management constraints to the weights.
        # This is intended to be a *binding* safety layer for live targets.
        if apply_risk and target.weights:
            sid = str(risk_strategy_id or portfolio_id)
            decisions = [
                {"instrument_id": inst_id, "target_weight": float(weight)}
                for inst_id, weight in target.weights.items()
            ]

            # Use the same DatabaseManager backing storage when available so
            # risk actions are logged in production. Unit tests may supply a
            # stub storage without db_manager.
            dbm = getattr(self.storage, "db_manager", None)

            adjusted = apply_risk_constraints(
                decisions,
                strategy_id=sid,
                db_manager=dbm,
            )
            weights_adj = {
                str(d["instrument_id"]): float(d.get("target_weight", 0.0))
                for d in adjusted
                if d.get("instrument_id") is not None
            }

            num_capped = sum(1 for d in adjusted if d.get("risk_action_type") == "CAPPED")
            num_rejected = sum(1 for d in adjusted if d.get("risk_action_type") == "REJECTED")

            risk_metrics = dict(target.risk_metrics)
            net_exposure = float(sum(weights_adj.values()))
            gross_exposure = float(sum(abs(w) for w in weights_adj.values()))
            risk_metrics["net_exposure"] = net_exposure
            risk_metrics["gross_exposure"] = gross_exposure
            risk_metrics["cash_weight"] = max(0.0, 1.0 - net_exposure)
            risk_metrics["risk_num_capped"] = float(num_capped)
            risk_metrics["risk_num_rejected"] = float(num_rejected)

            constraints_status = dict(target.constraints_status)
            constraints_status["risk_constraints_applied"] = True

            meta = dict(target.metadata)
            meta["risk_constraints"] = {
                "applied": True,
                "strategy_id": sid,
                "num_capped": int(num_capped),
                "num_rejected": int(num_rejected),
            }

            target = replace(
                target,
                weights=weights_adj,
                risk_metrics=risk_metrics,
                constraints_status=constraints_status,
                metadata=meta,
            )

        # Convert TargetPortfolio representation into book_targets rows.
        # The current model is defined only for INSTRUMENT entity_type and
        # uses the entity_ids implied by the weights.
        entity_ids = list(target.weights.keys())
        if not entity_ids:
            logger.info(
                "PortfolioEngine.optimize_and_save: portfolio %s as_of=%s produced empty target",
                portfolio_id,
                as_of_date,
            )
            return target

        # We need corresponding UniverseMembers to populate entity_type
        # and universe_id; the model should therefore expose or retain
        # these. For v1 we require that it attaches them via a private
        # attribute ``_last_members``. This keeps the public API simple
        # while avoiding an additional storage lookup here.
        members: list[UniverseMember] = getattr(self.model, "_last_members", [])  # type: ignore[assignment]
        if not members:
            logger.warning(
                "PortfolioEngine.optimize_and_save: model %s did not expose members; skipping persistence",
                type(self.model).__name__,
            )
            return target

        metadata_extra = {
            "portfolio_id": portfolio_id,
            "risk_model_id": target.metadata.get("risk_model_id", ""),
        }
        budget_meta = target.metadata.get("meta_budget")
        if budget_meta is not None:
            metadata_extra["meta_budget"] = budget_meta

        self.storage.save_book_targets(
            portfolio_id=portfolio_id,
            region=self.region,
            as_of_date=as_of_date,
            members=members,
            weights=target.weights,
            metadata_extra=metadata_extra,
        )

        # Persist aggregated target and risk report into dedicated tables
        # used by analytics and execution orchestration.
        strategy_id = portfolio_id  # simple default mapping for v1
        self.storage.save_target_portfolio(strategy_id=strategy_id, target=target)

        risk = self.model.build_risk_report(portfolio_id, as_of_date, target=target)
        if risk is not None:
            self.storage.save_portfolio_risk_report(
                model_id=str(target.metadata.get("risk_model_id", "")),
                report=risk,
            )

        logger.info(
            "PortfolioEngine.optimize_and_save: portfolio=%s as_of=%s names=%d gross=%f",
            portfolio_id,
            as_of_date,
            len(target.weights),
            float(target.risk_metrics.get("gross_exposure", 0.0)),
        )

        return target

    def risk_report(self, portfolio_id: str, as_of_date: date) -> RiskReport | None:
        """Compute a risk report via the underlying model, if implemented."""

        return self.model.build_risk_report(portfolio_id, as_of_date, target=None)
