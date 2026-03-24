"""Prometheus v2 – Prediction Scorecard.

Compares assessment scores (predicted instrument views) against realized
forward returns to measure prediction accuracy. Produces:
- Hit rate: fraction of instruments where score direction matched return direction
- Rank correlation: Spearman rho between scores and realized returns
- Sector breakdown: hit rate and avg error per sector
- Top misses: largest absolute mismatches between prediction and outcome

Usage::

    from prometheus.decisions.scorecard import PredictionScorecard
    scorecard = PredictionScorecard(db_manager=db)
    report = scorecard.build_scorecard(horizon_days=21)
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Dict, List, Tuple

from apathis.core.database import DatabaseManager
from apathis.core.logging import get_logger
from apathis.data.reader import DataReader

logger = get_logger(__name__)


@dataclass(frozen=True)
class ScorecardRow:
    """Single instrument prediction vs outcome pair."""

    decision_id: str
    as_of_date: date
    instrument_id: str
    predicted_score: float
    realized_return: float
    hit: bool  # score direction matched return direction
    sector: str
    error: float  # predicted_score - realized_return (sign-normalized)


@dataclass(frozen=True)
class SectorBreakdown:
    """Prediction accuracy breakdown for one sector."""

    sector: str
    hit_rate: float
    avg_error: float
    count: int
    avg_predicted: float
    avg_realized: float


@dataclass(frozen=True)
class ScorecardReport:
    """Complete prediction scorecard report."""

    horizon_days: int
    total_predictions: int
    hit_rate: float
    spearman_rho: float
    avg_predicted_score: float
    avg_realized_return: float
    sector_breakdown: List[SectorBreakdown]
    top_misses: List[ScorecardRow]
    top_hits: List[ScorecardRow]
    date_range: Tuple[date, date]


@dataclass
class PredictionScorecard:
    """Builds prediction accuracy reports from assessment decisions and price data."""

    db_manager: DatabaseManager

    def __post_init__(self) -> None:
        self._data_reader = DataReader(db_manager=self.db_manager)

    def build_scorecard(
        self,
        *,
        horizon_days: int = 21,
        max_decisions: int = 200,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> ScorecardReport:
        """Build a prediction scorecard for ASSESSMENT decisions.

        Loads assessment decisions with instrument_scores, computes realized
        forward returns at the given horizon, and produces accuracy metrics.

        Args:
            horizon_days: Forward return horizon in calendar days.
            max_decisions: Maximum ASSESSMENT decisions to evaluate.
            start_date: Optional start of evaluation window.
            end_date: Optional end (decisions must be at least horizon_days old).

        Returns:
            Complete ScorecardReport.
        """
        if end_date is None:
            end_date = date.today() - timedelta(days=horizon_days)
        else:
            end_date = min(end_date, date.today() - timedelta(days=horizon_days))

        # Load ASSESSMENT decisions with instrument_scores
        sql = """
            SELECT decision_id, as_of_date, output_refs
            FROM engine_decisions
            WHERE engine_name = 'ASSESSMENT'
              AND as_of_date <= %s
              AND output_refs ? 'instrument_scores'
        """
        params: List[Any] = [end_date]
        if start_date:
            sql += " AND as_of_date >= %s"
            params.append(start_date)
        sql += " ORDER BY as_of_date DESC LIMIT %s"
        params.append(max_decisions)

        with self.db_manager.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, tuple(params))
                decisions = cursor.fetchall()
            finally:
                cursor.close()

        logger.info(
            "Scorecard: loaded %d ASSESSMENT decisions (horizon=%dd)",
            len(decisions), horizon_days,
        )

        if not decisions:
            return ScorecardReport(
                horizon_days=horizon_days, total_predictions=0, hit_rate=0.0,
                spearman_rho=0.0, avg_predicted_score=0.0, avg_realized_return=0.0,
                sector_breakdown=[], top_misses=[], top_hits=[],
                date_range=(end_date, end_date),
            )

        # Load sector mapping for instruments
        sector_map = self._load_sector_map()

        # Build prediction rows
        rows: List[ScorecardRow] = []
        min_date = end_date
        max_date = end_date

        for decision_id, as_of_date, output_refs in decisions:
            scores = (output_refs or {}).get("instrument_scores", {})
            if not scores:
                continue

            min_date = min(min_date, as_of_date)
            max_date = max(max_date, as_of_date)

            exit_date = as_of_date + timedelta(days=horizon_days)

            # Get realized returns for this batch
            inst_ids = list(scores.keys())
            returns = self._compute_forward_returns(inst_ids, as_of_date, exit_date)

            for inst_id, predicted in scores.items():
                realized = returns.get(inst_id)
                if realized is None:
                    continue

                predicted_f = float(predicted)
                realized_f = float(realized)

                # Hit = both positive or both negative (or both zero)
                hit = (predicted_f >= 0 and realized_f >= 0) or (predicted_f < 0 and realized_f < 0)

                # Error: difference in sign-normalized direction
                error = predicted_f - realized_f

                sector = sector_map.get(inst_id, "UNKNOWN")

                rows.append(ScorecardRow(
                    decision_id=str(decision_id),
                    as_of_date=as_of_date,
                    instrument_id=inst_id,
                    predicted_score=predicted_f,
                    realized_return=realized_f,
                    hit=hit,
                    sector=sector,
                    error=error,
                ))

        if not rows:
            return ScorecardReport(
                horizon_days=horizon_days, total_predictions=0, hit_rate=0.0,
                spearman_rho=0.0, avg_predicted_score=0.0, avg_realized_return=0.0,
                sector_breakdown=[], top_misses=[], top_hits=[],
                date_range=(min_date, max_date),
            )

        # Compute aggregate metrics
        total = len(rows)
        hits = sum(1 for r in rows if r.hit)
        hit_rate = hits / total if total > 0 else 0.0

        avg_predicted = sum(r.predicted_score for r in rows) / total
        avg_realized = sum(r.realized_return for r in rows) / total

        # Spearman rank correlation
        spearman_rho = self._spearman(
            [r.predicted_score for r in rows],
            [r.realized_return for r in rows],
        )

        # Sector breakdown
        sector_groups: Dict[str, List[ScorecardRow]] = {}
        for r in rows:
            sector_groups.setdefault(r.sector, []).append(r)

        sector_breakdown = []
        for sector, group in sorted(sector_groups.items()):
            n = len(group)
            s_hits = sum(1 for r in group if r.hit)
            sector_breakdown.append(SectorBreakdown(
                sector=sector,
                hit_rate=s_hits / n if n > 0 else 0.0,
                avg_error=sum(r.error for r in group) / n,
                count=n,
                avg_predicted=sum(r.predicted_score for r in group) / n,
                avg_realized=sum(r.realized_return for r in group) / n,
            ))

        # Sort by hit rate to show worst sectors first
        sector_breakdown.sort(key=lambda s: s.hit_rate)

        # Top misses (largest absolute error, wrong direction)
        misses = sorted(
            [r for r in rows if not r.hit],
            key=lambda r: abs(r.error),
            reverse=True,
        )[:20]

        # Top hits (correct direction, largest realized return)
        top_hits = sorted(
            [r for r in rows if r.hit],
            key=lambda r: abs(r.realized_return),
            reverse=True,
        )[:20]

        return ScorecardReport(
            horizon_days=horizon_days,
            total_predictions=total,
            hit_rate=hit_rate,
            spearman_rho=spearman_rho,
            avg_predicted_score=avg_predicted,
            avg_realized_return=avg_realized,
            sector_breakdown=sector_breakdown,
            top_misses=misses,
            top_hits=top_hits,
            date_range=(min_date, max_date),
        )

    def _load_sector_map(self) -> Dict[str, str]:
        """Load instrument_id → sector mapping."""
        sql = """
            SELECT i.instrument_id, COALESCE(NULLIF(ic.sector, ''), 'UNKNOWN')
            FROM instruments i
            LEFT JOIN issuer_classifications ic ON ic.issuer_id = i.issuer_id
            WHERE i.asset_class = 'EQUITY'
              AND i.status = 'ACTIVE'
              AND i.instrument_id NOT LIKE 'SYNTH_%%'
        """
        with self.db_manager.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql)
                return {str(r[0]): str(r[1]) for r in cursor.fetchall()}
            finally:
                cursor.close()

    def _compute_forward_returns(
        self,
        instrument_ids: List[str],
        entry_date: date,
        exit_date: date,
    ) -> Dict[str, float]:
        """Compute forward returns from entry_date to exit_date."""
        returns: Dict[str, float] = {}

        for inst_id in instrument_ids:
            try:
                df = self._data_reader.read_prices(
                    instrument_ids=[inst_id],
                    start_date=entry_date,
                    end_date=exit_date,
                )
                if df.empty or len(df) < 2:
                    continue

                df_sorted = df.sort_values("trade_date")
                entry_px = float(df_sorted.iloc[0]["close"])
                exit_px = float(df_sorted.iloc[-1]["close"])

                if entry_px > 0 and math.isfinite(entry_px) and math.isfinite(exit_px):
                    returns[inst_id] = (exit_px / entry_px) - 1.0
            except Exception:
                continue

        return returns

    @staticmethod
    def _spearman(x: List[float], y: List[float]) -> float:
        """Compute Spearman rank correlation without scipy dependency."""
        n = len(x)
        if n < 3:
            return 0.0

        def _rank(vals: List[float]) -> List[float]:
            indexed = sorted(enumerate(vals), key=lambda t: t[1])
            ranks = [0.0] * n
            for rank_idx, (orig_idx, _) in enumerate(indexed):
                ranks[orig_idx] = float(rank_idx + 1)
            return ranks

        rx = _rank(x)
        ry = _rank(y)

        d_sq = sum((a - b) ** 2 for a, b in zip(rx, ry))
        return 1.0 - (6.0 * d_sq) / (n * (n * n - 1))
