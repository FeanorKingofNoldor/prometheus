"""Prometheus v2 – Outcome evaluation service.

This module provides services for evaluating realized outcomes of previously
recorded decisions. It computes returns, volatility, drawdown, and other
metrics at specified horizons by comparing realized prices to decision-time
expectations.
"""

from __future__ import annotations

import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any, Dict, List, Set, Tuple

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
    _price_cache: Dict[Tuple[str, date], float] = field(default_factory=dict, init=False, repr=False)

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
    ) -> List[Tuple[str, date, int, str]]:
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
            List of (decision_id, decision_as_of_date, horizon_days, engine_name) tuples
        """
        # For ASSESSMENT and PORTFOLIO decisions, we typically want to
        # evaluate at standard horizons (5, 21, 63 days). For now, we'll
        # check if metadata contains horizon_days or use defaults.

        sql = """
            SELECT
                d.decision_id,
                d.as_of_date,
                d.metadata,
                d.engine_name
            FROM engine_decisions d
            WHERE d.as_of_date <= %s
              AND d.engine_name IN ('PORTFOLIO', 'ASSESSMENT', 'OPTIONS')
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
        # Returns 4-tuples: (decision_id, as_of_date, horizon_days, engine_name)
        pending: List[Tuple[str, date, int, str]] = []

        for decision_id, decision_date, metadata, eng_name in rows:
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
                    pending.append((str(decision_id), decision_date, h, str(eng_name)))

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

    def _preload_prices(
        self,
        instrument_ids: Set[str],
        start_date: date,
        end_date: date,
    ) -> None:
        """Bulk-load prices into the in-memory cache.

        Issues a single SQL query for all instruments across the full date
        range, then stores (instrument_id, trade_date) → close in
        ``_price_cache``.  Subsequent calls to ``_get_price`` become pure
        dict lookups.
        """
        if not instrument_ids:
            return

        ids = list(instrument_ids)
        logger.info(
            "Preloading prices for %d instruments (%s → %s)",
            len(ids), start_date, end_date,
        )

        # Use the lightweight close-only reader to keep memory modest
        # Try adjusted_close first, fall back to close
        sql = """
            SELECT instrument_id, trade_date,
                   COALESCE(NULLIF(adjusted_close, 0), close) AS px
            FROM prices_daily
            WHERE instrument_id = ANY(%s)
              AND trade_date BETWEEN %s AND %s
              AND close > 0
            ORDER BY trade_date, instrument_id
        """
        with self.db_manager.get_historical_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, [ids, start_date, end_date])
                rows = cursor.fetchall()
            finally:
                cursor.close()

        loaded = 0
        for inst_id, td, px in rows:
            try:
                val = float(px)
                if math.isfinite(val) and val > 0:
                    self._price_cache[(str(inst_id), td)] = val
                    loaded += 1
            except (TypeError, ValueError):
                continue

        logger.info("Price cache loaded: %d price points", loaded)

    def _get_price(self, instrument_id: str, trade_date: date) -> float | None:
        """Get close price for an instrument on a date.

        Returns from ``_price_cache`` if populated, otherwise falls back to a
        single-row DB query.
        """
        # Fast path: cache hit
        cached = self._price_cache.get((instrument_id, trade_date))
        if cached is not None:
            return cached

        # Slow path: individual query (used when cache not preloaded)
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

            if price is not None:
                self._price_cache[(instrument_id, trade_date)] = price

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
        """Compute maximum drawdown from daily returns.

        Returns a value in [-1.0, 0.0]. Capped at -1.0 (total loss).
        """
        if not returns:
            return 0.0

        cumulative = 1.0
        peak = 1.0
        max_dd = 0.0

        for ret in returns:
            cumulative *= (1.0 + ret)
            cumulative = max(cumulative, 0.0)  # floor at zero (total loss)
            if cumulative > peak:
                peak = cumulative

            dd = (cumulative - peak) / peak if peak > 0 else 0.0
            if dd < max_dd:
                max_dd = dd

        return max(max_dd, -1.0)  # cap at -100%

    def evaluate_pending_outcomes(
        self,
        *,
        as_of_date: date,
        engine_name: str | None = None,
        strategy_id: str | None = None,
        max_decisions: int = 100,
        num_workers: int = 12,
    ) -> int:
        """Batch evaluate all pending decision outcomes.

        Pre-loads all instrument prices into an in-memory cache, then evaluates
        decisions in parallel using a thread pool.  This is orders of magnitude
        faster than the original serial approach (~3-5 min vs ~30+ min for 2K
        decisions).

        Args:
            as_of_date: Current date (evaluate decisions with horizon <= this date)
            engine_name: Optional filter by engine
            strategy_id: Optional filter by strategy
            max_decisions: Maximum number of decisions to evaluate
            num_workers: Thread pool size for parallel evaluation

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

        if not pending:
            return 0

        # --- Phase 1: split by engine type and batch-load decision data -----
        options_ids = [did for did, _, _, eng in pending if eng == "OPTIONS"]
        non_options_ids = [did for did, _, _, eng in pending if eng != "OPTIONS"]

        weights_map = self._batch_load_decision_weights(list(set(non_options_ids)))
        orders_map = self._batch_load_decision_orders(list(set(options_ids)))

        # Collect all instrument IDs and the global date range needed
        all_instruments: Set[str] = set()
        global_min_date = as_of_date
        global_max_date = as_of_date

        tasks: List[Tuple[str, date, int, Dict[str, float]]] = []
        options_tasks: List[Tuple[str, date, int, List[Dict[str, Any]]]] = []

        for decision_id, decision_date, horizon_days, eng_name in pending:
            exit_date = decision_date + timedelta(days=horizon_days)
            global_min_date = min(global_min_date, decision_date)
            global_max_date = max(global_max_date, exit_date)

            if eng_name == "OPTIONS":
                orders = orders_map.get(decision_id, [])
                if not orders:
                    continue
                options_tasks.append((decision_id, decision_date, horizon_days, orders))
                # Collect underlying IDs for price preloading
                for order in orders:
                    uid = order.get("underlying_id", "")
                    if uid:
                        all_instruments.add(uid)
            else:
                weights = weights_map.get(decision_id, {})
                if not weights:
                    continue
                tasks.append((decision_id, decision_date, horizon_days, weights))
                all_instruments.update(weights.keys())

        # --- Phase 2: bulk preload all prices into cache -------------------
        self._preload_prices(all_instruments, global_min_date, global_max_date)

        # --- Phase 3: evaluate in parallel ---------------------------------
        evaluated_count = 0
        total_tasks = len(tasks) + len(options_tasks)

        def _eval_one(task: Tuple[str, date, int, Dict[str, float]]) -> DecisionOutcome | None:
            d_id, d_date, h_days, w = task
            return self.evaluate_portfolio_decision_outcome(
                decision_id=d_id,
                decision_as_of_date=d_date,
                horizon_days=h_days,
                target_weights=w,
            )

        def _eval_options(task: Tuple[str, date, int, List[Dict[str, Any]]]) -> DecisionOutcome | None:
            d_id, d_date, h_days, orders = task
            return self.evaluate_options_decision_outcome(
                decision_id=d_id,
                decision_as_of_date=d_date,
                horizon_days=h_days,
                orders=orders,
            )

        all_futures: dict = {}
        with ThreadPoolExecutor(max_workers=num_workers) as pool:
            for t in tasks:
                all_futures[pool.submit(_eval_one, t)] = t[0]
            for t in options_tasks:
                all_futures[pool.submit(_eval_options, t)] = t[0]

            for future in as_completed(all_futures):
                decision_id = all_futures[future]
                try:
                    outcome = future.result()
                    if outcome is not None:
                        self._storage.save_decision_outcome(outcome)
                        evaluated_count += 1

                        if evaluated_count % 200 == 0:
                            logger.info(
                                "Progress: %d/%d outcomes evaluated",
                                evaluated_count, total_tasks,
                            )
                except Exception:
                    logger.exception(
                        "Error evaluating outcome for decision_id=%s",
                        decision_id,
                    )

        logger.info(
            "Evaluated %d/%d pending outcomes (%d portfolio/assessment, %d options)",
            evaluated_count,
            total_tasks,
            len(tasks),
            len(options_tasks),
        )

        return evaluated_count

    def evaluate_exit_outcomes(
        self,
        *,
        as_of_date: date,
        strategy_id: str | None = None,
        max_pairs: int = 500,
        num_workers: int = 12,
    ) -> int:
        """Evaluate holding-period returns for instruments dropped between consecutive portfolios.

        For each pair of consecutive PORTFOLIO decisions (same strategy), identifies
        instruments that were held in the earlier portfolio but dropped in the later one.
        Computes realized return over the actual holding period (entry_date → exit_date).

        These are stored as decision_outcomes with horizon_days = actual holding days
        and metadata.exit_triggered = true.

        Returns:
            Number of exit outcomes evaluated and saved
        """
        # Find consecutive PORTFOLIO decision pairs
        sql = """
            WITH ordered AS (
                SELECT decision_id, as_of_date, strategy_id, output_refs,
                       ROW_NUMBER() OVER (
                           PARTITION BY strategy_id ORDER BY as_of_date, created_at
                       ) AS rn
                FROM engine_decisions
                WHERE engine_name = 'PORTFOLIO'
                  AND as_of_date <= %s
        """
        params: List[Any] = [as_of_date]
        if strategy_id:
            sql += " AND strategy_id = %s"
            params.append(strategy_id)
        sql += """
            )
            SELECT
                a.decision_id AS prev_id,
                a.as_of_date  AS prev_date,
                a.output_refs AS prev_refs,
                b.decision_id AS next_id,
                b.as_of_date  AS next_date,
                b.output_refs AS next_refs
            FROM ordered a
            JOIN ordered b ON a.strategy_id = b.strategy_id AND b.rn = a.rn + 1
            ORDER BY a.as_of_date DESC
            LIMIT %s
        """
        params.append(max_pairs)

        with self.db_manager.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, tuple(params))
                pairs = cursor.fetchall()
            finally:
                cursor.close()

        logger.info("Found %d consecutive PORTFOLIO decision pairs", len(pairs))

        # Build exit tasks and collect instruments for preloading
        ExitTask = Tuple[str, date, int, Dict[str, float], str, List[str]]
        tasks: List[ExitTask] = []
        all_instruments: Set[str] = set()
        global_min_date = as_of_date
        global_max_date = as_of_date

        for prev_id, prev_date, prev_refs, next_id, next_date, next_refs in pairs:
            prev_weights = (prev_refs or {}).get("target_weights", {})
            next_weights = (next_refs or {}).get("target_weights", {})

            if not prev_weights:
                continue

            dropped = [
                inst for inst, w in prev_weights.items()
                if w and (inst not in next_weights or not next_weights.get(inst))
            ]

            if not dropped:
                continue

            holding_days = (next_date - prev_date).days
            if holding_days <= 0:
                continue

            if self._outcome_exists(prev_id, holding_days):
                continue

            dropped_weights = {inst: prev_weights[inst] for inst in dropped}
            total = sum(abs(w) for w in dropped_weights.values())
            if total <= 0:
                continue
            norm_weights = {k: abs(v) / total for k, v in dropped_weights.items()}

            tasks.append((str(prev_id), prev_date, holding_days, norm_weights, str(next_id), dropped))
            all_instruments.update(norm_weights.keys())
            global_min_date = min(global_min_date, prev_date)
            global_max_date = max(global_max_date, next_date)

        if not tasks:
            logger.info("No exit-triggered outcomes to evaluate")
            return 0

        # Preload prices
        self._preload_prices(all_instruments, global_min_date, global_max_date)

        # Evaluate in parallel
        evaluated = 0

        def _eval_exit(task: ExitTask) -> Tuple[DecisionOutcome | None, str, List[str]]:
            prev_id, prev_date, holding_days, norm_weights, next_id, dropped = task
            outcome = self.evaluate_portfolio_decision_outcome(
                decision_id=prev_id,
                decision_as_of_date=prev_date,
                horizon_days=holding_days,
                target_weights=norm_weights,
            )
            return outcome, next_id, dropped

        with ThreadPoolExecutor(max_workers=num_workers) as pool:
            futures = {pool.submit(_eval_exit, t): t for t in tasks}
            for future in as_completed(futures):
                prev_id = futures[future][0]
                try:
                    outcome, next_id, dropped = future.result()
                    if outcome is not None:
                        meta = dict(outcome.metadata or {})
                        meta["exit_triggered"] = True
                        meta["dropped_instruments"] = dropped
                        meta["successor_decision_id"] = next_id

                        tagged = DecisionOutcome(
                            decision_id=outcome.decision_id,
                            horizon_days=outcome.horizon_days,
                            realized_return=outcome.realized_return,
                            realized_pnl=outcome.realized_pnl,
                            realized_drawdown=outcome.realized_drawdown,
                            realized_vol=outcome.realized_vol,
                            metadata=meta,
                        )
                        self._storage.save_decision_outcome(tagged)
                        evaluated += 1
                except Exception:
                    logger.exception(
                        "Error evaluating exit outcome for decision_id=%s", prev_id,
                    )

        logger.info("Evaluated %d exit-triggered outcomes", evaluated)
        return evaluated

    def evaluate_options_decision_outcome(
        self,
        *,
        decision_id: str,
        decision_as_of_date: date,
        horizon_days: int,
        orders: List[Dict[str, Any]],
    ) -> "DecisionOutcome | None":
        """Evaluate outcome for an OPTIONS decision.

        For each order, computes the underlying price at the horizon date and
        derives a simplified P&L:
          - Intrinsic value at horizon = max(0, S_T - K) for calls,
                                         max(0, K - S_T) for puts
          - BUY P&L  = (intrinsic_T - entry_price) × qty × 100
          - SELL P&L = (entry_price - intrinsic_T)  × qty × 100

        Note: at horizons shorter than expiry the option still carries time
        value.  Intrinsic provides a conservative lower-bound estimate that is
        still useful for directional signal quality.

        ``realized_return`` = total_pnl / total_premium_at_risk.

        Args:
            decision_id: Decision to evaluate
            decision_as_of_date: Date the options were entered
            horizon_days: Forward horizon in calendar days
            orders: List of order dicts from ``record_options_decision``

        Returns:
            DecisionOutcome if computable, None if no underlying price data.
        """
        if not orders:
            return None

        exit_date = decision_as_of_date + timedelta(days=horizon_days)

        # Find nearest trading days
        if self.calendar is not None:
            trading_days = self.calendar.trading_days_between(
                start_date=decision_as_of_date,
                end_date=exit_date,
            )
            entry_date = trading_days[0] if trading_days else decision_as_of_date
            actual_exit_date = trading_days[-1] if trading_days else exit_date
        else:
            entry_date = decision_as_of_date
            actual_exit_date = exit_date

        total_pnl = 0.0
        total_premium = 0.0
        order_results: List[Dict[str, Any]] = []

        for order in orders:
            underlying_id = order.get("underlying_id", "")
            right = str(order.get("right", "C")).upper()
            strike = float(order.get("strike", 0.0))
            action = str(order.get("action", "BUY")).upper()
            quantity = int(order.get("quantity", 0))
            entry_price = float(order.get("entry_price", 0.0))

            if quantity <= 0 or strike <= 0 or not underlying_id:
                continue

            s_exit = self._get_price(underlying_id, actual_exit_date)
            if s_exit is None:
                logger.debug(
                    "No underlying price for %s on %s — skipping order in decision_id=%s",
                    underlying_id, actual_exit_date, decision_id,
                )
                continue

            # Intrinsic value at exit
            if right == "C":
                intrinsic_exit = max(0.0, s_exit - strike)
            else:
                intrinsic_exit = max(0.0, strike - s_exit)

            multiplier = 100  # Standard equity/index option multiplier
            if action == "BUY":
                order_pnl = (intrinsic_exit - entry_price) * quantity * multiplier
            else:  # SELL
                order_pnl = (entry_price - intrinsic_exit) * quantity * multiplier

            premium_at_risk = entry_price * quantity * multiplier
            total_pnl += order_pnl
            total_premium += premium_at_risk

            order_results.append({
                "underlying_id": underlying_id,
                "strike": strike,
                "right": right,
                "action": action,
                "quantity": quantity,
                "entry_price": entry_price,
                "underlying_price_at_exit": s_exit,
                "intrinsic_at_exit": intrinsic_exit,
                "order_pnl": round(order_pnl, 2),
            })

        if not order_results:
            logger.warning(
                "No valid underlying prices for any order in decision_id=%s at exit_date=%s",
                decision_id, actual_exit_date,
            )
            return None

        # realized_return = P&L / premium at risk
        realized_return = (total_pnl / total_premium) if total_premium > 0 else 0.0

        metadata: Dict[str, Any] = {
            "entry_date": entry_date.isoformat(),
            "exit_date": actual_exit_date.isoformat(),
            "orders_evaluated": len(order_results),
            "order_details": order_results[:10],  # Cap stored detail
        }

        return DecisionOutcome(
            decision_id=decision_id,
            horizon_days=horizon_days,
            realized_return=realized_return,
            realized_pnl=round(total_pnl, 2),
            realized_drawdown=0.0,   # Not applicable for single-point options eval
            realized_vol=0.0,        # Not applicable
            metadata=metadata,
        )

    def _batch_load_decision_orders(
        self, decision_ids: List[str],
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Batch-load orders for OPTIONS decisions in one query."""
        if not decision_ids:
            return {}

        sql = """
            SELECT decision_id, output_refs
            FROM engine_decisions
            WHERE decision_id = ANY(%s)
              AND engine_name = 'OPTIONS'
        """
        with self.db_manager.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, [decision_ids])
                rows = cursor.fetchall()
            finally:
                cursor.close()

        result: Dict[str, List[Dict[str, Any]]] = {}
        for decision_id, output_refs in rows:
            refs = output_refs or {}
            orders = refs.get("orders", [])
            if orders:
                result[str(decision_id)] = orders
        return result

    def _batch_load_decision_weights(
        self, decision_ids: List[str],
    ) -> Dict[str, Dict[str, float]]:
        """Batch-load target weights for multiple decisions in one query."""
        if not decision_ids:
            return {}

        sql = """
            SELECT decision_id, output_refs
            FROM engine_decisions
            WHERE decision_id = ANY(%s)
        """
        with self.db_manager.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, [decision_ids])
                rows = cursor.fetchall()
            finally:
                cursor.close()

        result: Dict[str, Dict[str, float]] = {}
        for decision_id, output_refs in rows:
            did = str(decision_id)
            refs = output_refs or {}
            if isinstance(refs.get("target_weights"), dict):
                result[did] = refs["target_weights"]
            elif isinstance(refs.get("instrument_scores"), dict):
                scores = refs["instrument_scores"]
                total = sum(abs(s) for s in scores.values())
                if total > 0:
                    result[did] = {k: abs(v) / total for k, v in scores.items()}
        return result

    def _load_decision_weights(self, decision_id: str) -> Dict[str, float]:
        """Load target weights from a portfolio decision."""
        return self._batch_load_decision_weights([decision_id]).get(decision_id, {})
