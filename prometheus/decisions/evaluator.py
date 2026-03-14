"""Prometheus v2 – Outcome evaluation service.

This module provides services for evaluating realized outcomes of previously
recorded decisions. It computes returns, volatility, drawdown, and other
metrics at specified horizons by comparing realized prices to decision-time
expectations.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Dict, List, Tuple

from apathis.core.database import DatabaseManager
from apathis.core.logging import get_logger
from apathis.core.time import TradingCalendar
from apathis.data.reader import DataReader
from prometheus.meta.storage import MetaStorage
from prometheus.meta.types import DecisionOutcome

logger = get_logger(__name__)


@dataclass
class OutcomeEvaluator:
    """Service for evaluating decision outcomes at horizons.
    
    Usage:
        evaluator = OutcomeEvaluator(db_manager=db)
        evaluator.evaluate_pending_outcomes(
            as_of_date=date(2024, 12, 15),
            max_decisions=100
        )
    """
    
    db_manager: DatabaseManager
    calendar: TradingCalendar | None = None
    
    def __post_init__(self) -> None:
        self._storage = MetaStorage(db_manager=self.db_manager)
        self._data_reader = DataReader(db_manager=self.db_manager)
        if self.calendar is None:
            from apathis.core.time import TradingCalendarConfig
            config = TradingCalendarConfig(market="US_EQ")
            self.calendar = TradingCalendar(config=config, db_manager=self.db_manager)
    
    def find_pending_decisions(
        self,
        *,
        as_of_date: date,
        engine_name: str | None = None,
        strategy_id: str | None = None,
        max_results: int = 1000,
    ) -> List[Tuple[str, date, int]]:
        """Find decisions that are ready for outcome evaluation.
        
        Returns decisions where:
        - as_of_date + horizon_days <= current as_of_date
        - decision_id not yet in decision_outcomes for that horizon
        
        Args:
            as_of_date: Current date (decisions with horizon <= this date are ready)
            engine_name: Optional filter by engine (e.g., "ASSESSMENT")
            strategy_id: Optional filter by strategy
            max_results: Maximum number of results to return
            
        Returns:
            List of (decision_id, decision_as_of_date, horizon_days) tuples
        """
        # For ASSESSMENT and PORTFOLIO decisions, we typically want to
        # evaluate at standard horizons (5, 21, 63 days). For now, we'll
        # check if metadata contains horizon_days or use defaults.
        
        sql = """
            SELECT 
                d.decision_id,
                d.as_of_date,
                d.metadata
            FROM engine_decisions d
            WHERE d.as_of_date <= %s
        """
        
        params: List[Any] = [as_of_date]
        
        if engine_name is not None:
            sql += " AND d.engine_name = %s"
            params.append(engine_name)
        
        if strategy_id is not None:
            sql += " AND d.strategy_id = %s"
            params.append(strategy_id)
        
        # Use created_at as a tie-breaker so repeated runs with the same
        # as_of_date don't make results effectively random.
        sql += " ORDER BY d.as_of_date DESC, d.created_at DESC LIMIT %s"
        params.append(max_results)
        
        with self.db_manager.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, tuple(params))
                rows = cursor.fetchall()
            finally:
                cursor.close()
        
        # Extract horizon from metadata and filter for pending
        pending: List[Tuple[str, date, int]] = []
        
        for decision_id, decision_date, metadata in rows:
            # Try to extract horizon from metadata
            meta = metadata or {}
            horizon_days = None
            
            # Check various metadata fields
            if isinstance(meta.get("horizon_days"), int):
                horizon_days = meta["horizon_days"]
            elif isinstance(meta.get("reasoning"), dict):
                if isinstance(meta["reasoning"].get("horizon_days"), int):
                    horizon_days = meta["reasoning"]["horizon_days"]
            
            # Default horizons for different engines
            if horizon_days is None:
                # Use standard horizons: 5, 21, 63 days
                horizons_to_check = [5, 21, 63]
            else:
                horizons_to_check = [horizon_days]
            
            for h in horizons_to_check:
                # Check if this decision + horizon is already evaluated
                if self._outcome_exists(decision_id, h):
                    continue
                
                # Check if horizon has elapsed
                if decision_date + timedelta(days=h) <= as_of_date:
                    pending.append((str(decision_id), decision_date, h))
        
        return pending[:max_results]
    
    def _outcome_exists(self, decision_id: str, horizon_days: int) -> bool:
        """Check if outcome already exists for decision + horizon."""
        sql = """
            SELECT 1
            FROM decision_outcomes
            WHERE decision_id = %s
              AND horizon_days = %s
            LIMIT 1
        """
        
        with self.db_manager.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, (decision_id, horizon_days))
                return cursor.fetchone() is not None
            finally:
                cursor.close()
    
    def evaluate_portfolio_decision_outcome(
        self,
        *,
        decision_id: str,
        decision_as_of_date: date,
        horizon_days: int,
        target_weights: Dict[str, float],
        benchmark_id: str | None = None,
    ) -> DecisionOutcome | None:
        """Evaluate outcome for a portfolio decision.
        
        Computes realized return and volatility by:
        1. Getting prices at decision_as_of_date for all instruments
        2. Getting prices at decision_as_of_date + horizon_days
        3. Computing weighted return based on target_weights
        4. Computing volatility over the horizon period
        
        Args:
            decision_id: Decision to evaluate
            decision_as_of_date: Date decision was made
            horizon_days: Forward horizon in calendar days
            target_weights: Dict mapping instrument_id to weight
            benchmark_id: Optional benchmark instrument for comparison
            
        Returns:
            DecisionOutcome if successfully computed, None if insufficient data
        """
        if not target_weights:
            logger.warning(
                "Cannot evaluate portfolio outcome for decision_id=%s: empty weights",
                decision_id,
            )
            return None
        
        # Find exit date (trading day closest to decision_date + horizon)
        exit_date = decision_as_of_date + timedelta(days=horizon_days)
        
        # Get all trading days in the horizon window
        if self.calendar is not None:
            trading_days = self.calendar.trading_days_between(
                start_date=decision_as_of_date,
                end_date=exit_date,
            )
        else:
            # Fallback: approximate trading days
            trading_days = []
            current = decision_as_of_date
            while current <= exit_date:
                trading_days.append(current)
                current += timedelta(days=1)
        
        if len(trading_days) < 2:
            logger.warning(
                "Cannot evaluate outcome for decision_id=%s: insufficient trading days",
                decision_id,
            )
            return None
        
        entry_date = trading_days[0]
        actual_exit_date = trading_days[-1]
        
        # Get prices for all instruments at entry and exit
        instrument_ids = list(target_weights.keys())
        
        entry_prices = self._get_prices_for_instruments(instrument_ids, entry_date)
        exit_prices = self._get_prices_for_instruments(instrument_ids, actual_exit_date)
        
        # Compute per-instrument returns
        instrument_returns: Dict[str, float] = {}
        valid_weight_sum = 0.0
        
        for inst_id, weight in target_weights.items():
            entry_px = entry_prices.get(inst_id)
            exit_px = exit_prices.get(inst_id)
            
            if entry_px is None or exit_px is None or entry_px <= 0:
                continue
            
            ret = (exit_px / entry_px) - 1.0
            instrument_returns[inst_id] = ret
            valid_weight_sum += weight
        
        if not instrument_returns or valid_weight_sum <= 0:
            logger.warning(
                "Cannot evaluate outcome for decision_id=%s: no valid prices",
                decision_id,
            )
            return None
        
        # Compute portfolio return (weighted average)
        realized_return = sum(
            target_weights[inst_id] / valid_weight_sum * ret
            for inst_id, ret in instrument_returns.items()
        )
        
        # Compute realized volatility (simplified: stdev of daily portfolio returns)
        daily_returns = self._compute_daily_portfolio_returns(
            target_weights=target_weights,
            trading_days=trading_days,
        )
        
        realized_vol = self._compute_volatility(daily_returns) if daily_returns else 0.0
        
        # Compute drawdown (max decline from peak over horizon)
        realized_drawdown = self._compute_drawdown(daily_returns) if daily_returns else 0.0
        
        # Compute PnL (assuming $1M notional for normalization)
        notional = 1_000_000.0
        realized_pnl = realized_return * notional
        
        metadata: Dict[str, Any] = {
            "entry_date": entry_date.isoformat(),
            "exit_date": actual_exit_date.isoformat(),
            "valid_instruments": len(instrument_returns),
            "total_instruments": len(target_weights),
        }
        
        # Optional: compare to benchmark
        if benchmark_id is not None:
            bench_entry = self._get_price(benchmark_id, entry_date)
            bench_exit = self._get_price(benchmark_id, actual_exit_date)
            
            if bench_entry and bench_exit and bench_entry > 0:
                benchmark_return = (bench_exit / bench_entry) - 1.0
                metadata["benchmark_return"] = benchmark_return
                metadata["alpha"] = realized_return - benchmark_return
        
        return DecisionOutcome(
            decision_id=decision_id,
            horizon_days=horizon_days,
            realized_return=realized_return,
            realized_pnl=realized_pnl,
            realized_drawdown=realized_drawdown,
            realized_vol=realized_vol,
            metadata=metadata,
        )
    
    def _get_prices_for_instruments(
        self,
        instrument_ids: List[str],
        trade_date: date,
    ) -> Dict[str, float]:
        """Get close prices for instruments on a specific date."""
        prices: Dict[str, float] = {}
        
        for inst_id in instrument_ids:
            price = self._get_price(inst_id, trade_date)
            if price is not None:
                prices[inst_id] = price
        
        return prices
    
    def _get_price(self, instrument_id: str, trade_date: date) -> float | None:
        """Get close price for an instrument on a date."""
        try:
            df = self._data_reader.read_prices(
                instrument_ids=[instrument_id],
                start_date=trade_date,
                end_date=trade_date,
            )

            if df.empty:
                return None

            row = df.iloc[0]

            def _as_price(v: object) -> float | None:
                try:
                    x = float(v)  # handles numpy scalars
                except Exception:
                    return None
                if not math.isfinite(x) or x <= 0:
                    return None
                return x

            # Prefer adjusted_close when it is present and valid; fall back to close.
            price = _as_price(row.get("adjusted_close"))
            if price is None:
                price = _as_price(row.get("close"))

            return price
        except Exception:  # pragma: no cover - defensive
            logger.exception(
                "Error getting price for instrument_id=%s date=%s",
                instrument_id,
                trade_date,
            )
            return None
    
    def _compute_daily_portfolio_returns(
        self,
        target_weights: Dict[str, float],
        trading_days: List[date],
    ) -> List[float]:
        """Compute daily portfolio returns over the horizon."""
        if len(trading_days) < 2:
            return []
        
        daily_returns: List[float] = []
        
        for i in range(1, len(trading_days)):
            prev_date = trading_days[i - 1]
            curr_date = trading_days[i]
            
            prev_prices = self._get_prices_for_instruments(
                list(target_weights.keys()), prev_date
            )
            curr_prices = self._get_prices_for_instruments(
                list(target_weights.keys()), curr_date
            )
            
            # Compute weighted return for this day
            day_return = 0.0
            valid_weight = 0.0
            
            for inst_id, weight in target_weights.items():
                prev_px = prev_prices.get(inst_id)
                curr_px = curr_prices.get(inst_id)
                
                if prev_px and curr_px and prev_px > 0:
                    ret = (curr_px / prev_px) - 1.0
                    day_return += weight * ret
                    valid_weight += weight
            
            if valid_weight > 0:
                daily_returns.append(day_return / valid_weight)
        
        return daily_returns
    
    def _compute_volatility(self, returns: List[float]) -> float:
        """Compute annualized volatility from daily returns."""
        if len(returns) < 2:
            return 0.0
        
        n = len(returns)
        mean = sum(returns) / n
        variance = sum((r - mean) ** 2 for r in returns) / (n - 1)
        daily_vol = math.sqrt(variance) if variance > 0 else 0.0
        
        # Annualize (252 trading days)
        return daily_vol * math.sqrt(252)
    
    def _compute_drawdown(self, returns: List[float]) -> float:
        """Compute maximum drawdown from daily returns."""
        if not returns:
            return 0.0
        
        # Compute cumulative returns
        cumulative = 1.0
        peak = 1.0
        max_dd = 0.0
        
        for ret in returns:
            cumulative *= (1.0 + ret)
            if cumulative > peak:
                peak = cumulative
            
            dd = (cumulative - peak) / peak if peak > 0 else 0.0
            if dd < max_dd:
                max_dd = dd
        
        return max_dd
    
    def evaluate_pending_outcomes(
        self,
        *,
        as_of_date: date,
        engine_name: str | None = None,
        strategy_id: str | None = None,
        max_decisions: int = 100,
    ) -> int:
        """Batch evaluate all pending decision outcomes.
        
        Args:
            as_of_date: Current date (evaluate decisions with horizon <= this date)
            engine_name: Optional filter by engine
            strategy_id: Optional filter by strategy
            max_decisions: Maximum number of decisions to evaluate
            
        Returns:
            Number of outcomes successfully evaluated and saved
        """
        pending = self.find_pending_decisions(
            as_of_date=as_of_date,
            engine_name=engine_name,
            strategy_id=strategy_id,
            max_results=max_decisions,
        )
        
        logger.info(
            "Found %d pending decisions to evaluate as_of=%s",
            len(pending),
            as_of_date,
        )
        
        evaluated_count = 0
        
        for decision_id, decision_date, horizon_days in pending:
            try:
                # Load decision to get weights
                weights = self._load_decision_weights(decision_id)
                if not weights:
                    logger.warning(
                        "Cannot evaluate decision_id=%s: no weights found",
                        decision_id,
                    )
                    continue
                
                outcome = self.evaluate_portfolio_decision_outcome(
                    decision_id=decision_id,
                    decision_as_of_date=decision_date,
                    horizon_days=horizon_days,
                    target_weights=weights,
                )
                
                if outcome is not None:
                    self._storage.save_decision_outcome(outcome)
                    evaluated_count += 1
                    
                    logger.info(
                        "Evaluated outcome: decision_id=%s horizon=%d return=%.4f vol=%.4f",
                        decision_id,
                        horizon_days,
                        outcome.realized_return or 0.0,
                        outcome.realized_vol or 0.0,
                    )
            except Exception:  # pragma: no cover - defensive
                logger.exception(
                    "Error evaluating outcome for decision_id=%s",
                    decision_id,
                )
        
        logger.info(
            "Evaluated %d/%d pending outcomes",
            evaluated_count,
            len(pending),
        )
        
        return evaluated_count
    
    def _load_decision_weights(self, decision_id: str) -> Dict[str, float]:
        """Load target weights from a portfolio decision."""
        sql = """
            SELECT output_refs
            FROM engine_decisions
            WHERE decision_id = %s
        """
        
        with self.db_manager.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, (decision_id,))
                row = cursor.fetchone()
            finally:
                cursor.close()
        
        if row is None:
            return {}
        
        output_refs = row[0] or {}
        
        # Try to extract weights from various possible locations
        if isinstance(output_refs.get("target_weights"), dict):
            return output_refs["target_weights"]
        
        if isinstance(output_refs.get("instrument_scores"), dict):
            # Assessment decision: use scores as proxy weights (normalized)
            scores = output_refs["instrument_scores"]
            total = sum(abs(s) for s in scores.values())
            if total > 0:
                return {k: abs(v) / total for k, v in scores.items()}
        
        return {}
