"""Prometheus v2 – Options Backtest Engine.

Daily simulation loop that integrates:
- Equity backtest results (pre-computed NAV, portfolio weights)
- Synthetic option chain generation
- IV surface pricing
- All 15 options strategies via OptionsStrategyManager
- Strategy allocator (regime-adaptive)
- Position lifecycle manager
- Synthetic position tracking with full P&L attribution

The engine reads equity prices, VIX, and realized vol from the
database via DataReader, then runs the options overlay on top of
a pre-computed equity backtest.

Usage::

    from prometheus.backtest.options_backtest import OptionsBacktestEngine

    engine = OptionsBacktestEngine(config)
    result = engine.run()
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from prometheus.backtest.iv_surface import IVSurfaceEngine, VolTermStructure
from prometheus.backtest.option_pricer import bs_price, bs_greeks, fill_price
from prometheus.backtest.options_position import (
    BookGreeks,
    PnLAttribution,
    SyntheticOptionsBook,
)
from prometheus.backtest.synthetic_chain import SyntheticChainGenerator
from apathis.core.logging import get_logger

logger = get_logger(__name__)


# ── Configuration ────────────────────────────────────────────────────

@dataclass
class OptionsBacktestConfig:
    """Configuration for the options backtest engine."""

    # Date range
    start_date: date = date(1997, 1, 2)
    end_date: date = date(2026, 3, 2)

    # Initial capital
    initial_nav: float = 1_000_000.0

    # Derivatives capital as fraction of NAV
    derivatives_budget_pct: float = 0.15

    # Slippage on option fills (fraction of half-spread)
    slippage_pct: float = 0.25

    # Maximum number of strategies to run simultaneously
    max_active_strategies: int = 15

    # Risk limits
    max_delta_pct_nav: float = 0.20
    max_position_count: int = 100

    # Risk guardrails (circuit breakers — only fire in tail scenarios)
    guardrails_enabled: bool = True
    # Halt new short-premium trades when combined NAV drawdown exceeds this
    guardrail_dd_halt_pct: float = -0.30
    # Force-close all short premium when drawdown exceeds this
    guardrail_dd_close_pct: float = -0.45
    # Halt all premium-selling when VIX exceeds this
    guardrail_vix_halt: float = 45.0
    # Cooldown: after guardrail triggers, wait N days before resuming
    guardrail_cooldown_days: int = 5

    # Equity backtest results path (JSON from C++ backtester)
    equity_backtest_path: Optional[str] = None

    # Log frequency
    log_every_n_days: int = 63  # ~quarterly

    # VIX instrument ID in the database
    vix_instrument_id: str = "VIX.INDX"

    # SPY instrument ID
    spy_instrument_id: str = "SPY.US"


# ── Backtest result ──────────────────────────────────────────────────

@dataclass
class DailySnapshot:
    """One day's state in the backtest."""
    date: date
    equity_nav: float = 0.0
    options_pnl: float = 0.0      # Cumulative options P&L
    total_nav: float = 0.0
    options_daily_pnl: float = 0.0

    # Greeks
    net_delta: float = 0.0
    net_gamma: float = 0.0
    net_theta: float = 0.0
    net_vega: float = 0.0

    # P&L attribution
    delta_pnl: float = 0.0
    theta_pnl: float = 0.0
    vega_pnl: float = 0.0
    gamma_pnl: float = 0.0

    # Position counts
    n_positions: int = 0
    n_strategies_active: int = 0
    market_situation: str = ""
    vix: float = 0.0


@dataclass
class OptionsBacktestResult:
    """Complete backtest results."""
    config: Dict[str, Any] = field(default_factory=dict)
    daily_snapshots: List[DailySnapshot] = field(default_factory=list)
    strategy_metrics: Dict[str, Any] = field(default_factory=dict)
    summary: Dict[str, Any] = field(default_factory=dict)

    def compute_summary(self) -> Dict[str, Any]:
        """Compute summary statistics from daily snapshots."""
        if not self.daily_snapshots:
            return {}

        navs = [s.total_nav for s in self.daily_snapshots]
        dates = [s.date for s in self.daily_snapshots]
        n_days = len(navs)

        if n_days < 2:
            return {"n_days": n_days}

        # Returns
        returns = []
        for i in range(1, n_days):
            if navs[i - 1] > 0:
                returns.append(navs[i] / navs[i - 1] - 1.0)

        # CAGR
        years = n_days / 252.0
        total_return = navs[-1] / navs[0] - 1.0 if navs[0] > 0 else 0.0
        cagr = (1.0 + total_return) ** (1.0 / years) - 1.0 if years > 0 and total_return > -1 else 0.0

        # Volatility
        if returns:
            mean_ret = sum(returns) / len(returns)
            var = sum((r - mean_ret) ** 2 for r in returns) / len(returns)
            ann_vol = math.sqrt(var * 252)
        else:
            ann_vol = 0.0

        # Sharpe
        sharpe = cagr / ann_vol if ann_vol > 0 else 0.0

        # Max drawdown
        peak = navs[0]
        max_dd = 0.0
        for nav in navs:
            peak = max(peak, nav)
            dd = nav / peak - 1.0
            max_dd = min(max_dd, dd)

        # Options P&L
        total_options_pnl = self.daily_snapshots[-1].options_pnl
        equity_only_nav = self.daily_snapshots[-1].equity_nav
        equity_only_cagr = (equity_only_nav / navs[0]) ** (1.0 / years) - 1.0 if years > 0 and equity_only_nav > navs[0] * 0.01 else 0.0

        self.summary = {
            "start_date": str(dates[0]),
            "end_date": str(dates[-1]),
            "n_trading_days": n_days,
            "years": round(years, 2),
            "initial_nav": round(navs[0], 2),
            "final_nav": round(navs[-1], 2),
            "total_return": round(total_return, 4),
            "cagr": round(cagr, 4),
            "annualised_vol": round(ann_vol, 4),
            "sharpe": round(sharpe, 3),
            "max_drawdown": round(max_dd, 4),
            "equity_only_cagr": round(equity_only_cagr, 4),
            "options_total_pnl": round(total_options_pnl, 2),
            "options_pnl_pct": round(total_options_pnl / navs[0], 4) if navs[0] > 0 else 0.0,
        }
        return self.summary

    def to_json(self, path: str) -> None:
        """Write results to JSON."""
        self.compute_summary()
        payload = {
            "config": self.config,
            "summary": self.summary,
            "strategy_metrics": self.strategy_metrics,
            "daily_count": len(self.daily_snapshots),
            # Include a sampled subset of daily data (every 21 days)
            "daily_sample": [
                {
                    "date": str(s.date),
                    "total_nav": round(s.total_nav, 2),
                    "equity_nav": round(s.equity_nav, 2),
                    "options_pnl": round(s.options_pnl, 2),
                    "vix": round(s.vix, 2),
                    "n_positions": s.n_positions,
                    "net_delta": round(s.net_delta, 1),
                    "market_situation": s.market_situation,
                }
                for i, s in enumerate(self.daily_snapshots)
                if i % 21 == 0 or i == len(self.daily_snapshots) - 1
            ],
        }
        out = Path(path)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(payload, indent=2))
        logger.info("Results written to %s", path)


# ── Equity backtest loader ───────────────────────────────────────────

def load_equity_backtest(
    path: str,
    *,
    expected_engine: str = "lambda_factorial",
) -> Dict[str, float]:
    """Load pre-computed equity backtest NAV series.

    Expected format: JSON with a "results" list, each containing
    date → NAV mapping, or a flat {date: nav} dict.

    Parameters
    ----------
    path : str
        Path to the equity NAV JSON file.
    expected_engine : str
        The engine that should have produced this NAV.  If the file
        contains an ``engine`` field and it does not match, a warning
        is logged.  Pass ``""`` to skip validation.

    Returns
    -------
    dict[str, float]
        date_str (YYYY-MM-DD) → equity NAV.
    """
    data = json.loads(Path(path).read_text())

    # ── Engine provenance check ──────────────────────────────────
    if expected_engine and isinstance(data, dict):
        file_engine = data.get("engine", "")
        if file_engine and file_engine != expected_engine:
            logger.warning(
                "ENGINE MISMATCH: equity NAV at %s was produced by '%s', "
                "expected '%s'. This is almost certainly a pipeline bug — "
                "the options overlay should run on lambda_factorial NAV, "
                "not allocator NAV.",
                path, file_engine, expected_engine,
            )
        elif not file_engine:
            logger.warning(
                "Equity NAV at %s has no 'engine' provenance field. "
                "Consider re-exporting with export_best_equity_nav() "
                "to stamp it.",
                path,
            )

    nav_series: Dict[str, float] = {}

    # Try multiple formats
    if isinstance(data, dict):
        # Format 1: {results: [{date: ..., nav: ...}, ...]}
        results = data.get("results", data.get("daily_nav", []))
        if isinstance(results, list):
            for row in results:
                if isinstance(row, dict):
                    d = row.get("date", row.get("trade_date", ""))
                    n = row.get("nav", row.get("total_nav", 0))
                    if d and n:
                        nav_series[str(d)] = float(n)
        elif isinstance(results, dict):
            # Format 2: {"2024-01-02": 1000000.0, ...}
            for d, n in results.items():
                nav_series[str(d)] = float(n)

        # Format 3: flat dict at top level
        if not nav_series:
            for k, v in data.items():
                try:
                    _ = date.fromisoformat(str(k))
                    nav_series[str(k)] = float(v)
                except (ValueError, TypeError):
                    continue

    logger.info("Loaded equity backtest: %d daily NAV points", len(nav_series))
    return nav_series


# ── Backtest Engine ──────────────────────────────────────────────────

class OptionsBacktestEngine:
    """Run the full options overlay backtest.

    Parameters
    ----------
    config : OptionsBacktestConfig
        Backtest configuration.
    data_reader : DataReader, optional
        For reading historical prices.  If None, uses synthetic/fallback data.
    """

    def __init__(
        self,
        config: OptionsBacktestConfig,
        data_reader: Any = None,
        writer: Any = None,
        strategy_overrides: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> None:
        self._config = config
        self._data_reader = data_reader
        self._writer = writer  # Optional BacktestOptionsWriter for persistence
        self._strategy_overrides = strategy_overrides or {}  # ConfigClassName → {field: value}

        # Components
        self._iv_engine = IVSurfaceEngine()
        self._chain_gen = SyntheticChainGenerator()

        # State
        self._book = SyntheticOptionsBook()
        self._result = OptionsBacktestResult()
        self._strategy_pnl: Dict[str, float] = {}  # strategy → cumulative P&L
        self._strategy_trades: Dict[str, int] = {}  # strategy → trade count

        # Pre-loaded data caches
        self._equity_nav: Dict[str, float] = {}
        self._vix_cache: Dict[date, float] = {}
        self._price_cache: Dict[str, Dict[date, float]] = {}
        self._vol_cache: Dict[str, Dict[date, float]] = {}

        # Vol term structure caches (keyed by date)
        self._vix9d_cache: Dict[date, float] = {}
        self._vix3m_cache: Dict[date, float] = {}
        self._vix6m_cache: Dict[date, float] = {}
        self._vix1y_cache: Dict[date, float] = {}
        self._skew_cache: Dict[date, float] = {}

        # Risk guardrail state
        self._peak_nav: float = config.initial_nav
        self._guardrail_cooldown_remaining: int = 0
        self._guardrail_triggered_count: int = 0
        self._guardrail_force_close_count: int = 0

    def run(self) -> OptionsBacktestResult:
        """Execute the full backtest.

        Returns
        -------
        OptionsBacktestResult
            Complete results with daily snapshots and summary metrics.
        """
        cfg = self._config
        logger.info(
            "Starting options backtest: %s -> %s (initial NAV: $%.0f, derivatives: %.0f%%)",
            cfg.start_date, cfg.end_date, cfg.initial_nav, cfg.derivatives_budget_pct * 100,
        )

        # Load equity backtest
        if cfg.equity_backtest_path:
            self._equity_nav = load_equity_backtest(cfg.equity_backtest_path)

        # Pre-load VIX and price data
        self._preload_data()

        # Initialize options book
        initial_deriv_capital = cfg.initial_nav * cfg.derivatives_budget_pct
        self._book = SyntheticOptionsBook(initial_capital=initial_deriv_capital)

        # Build strategies (simplified for backtest — no IBKR broker)
        from prometheus.execution.strategy_allocator import StrategyAllocator
        from prometheus.execution.position_lifecycle import PositionLifecycleManager

        allocator = StrategyAllocator()
        lifecycle = PositionLifecycleManager()

        # Trading days
        trading_days = self._get_trading_days(cfg.start_date, cfg.end_date)
        logger.info("Running %d trading days", len(trading_days))

        # Insert run metadata if persisting
        if self._writer is not None:
            self._writer.insert_run(
                start_date=cfg.start_date,
                end_date=cfg.end_date,
                initial_nav=cfg.initial_nav,
                derivatives_budget_pct=cfg.derivatives_budget_pct,
                config={
                    "start_date": str(cfg.start_date),
                    "end_date": str(cfg.end_date),
                    "initial_nav": cfg.initial_nav,
                    "derivatives_budget_pct": cfg.derivatives_budget_pct,
                    "slippage_pct": cfg.slippage_pct,
                    "max_position_count": cfg.max_position_count,
                    "max_active_strategies": cfg.max_active_strategies,
                },
            )

        cumulative_options_pnl = 0.0
        day_count = 0

        # Short-premium strategy names (halted during guardrail triggers)
        _SHORT_PREMIUM_STRATS = {
            "iron_condor", "iron_butterfly", "short_put", "wheel",
            "bull_call_spread", "covered_call",
        }

        for d in trading_days:
            day_count += 1

            # 1. Get equity NAV for today
            equity_nav = self._get_equity_nav(d, cfg.initial_nav)

            # 2. Update derivatives capital
            deriv_capital = equity_nav * cfg.derivatives_budget_pct
            self._book.update_capital(deriv_capital)

            # 3. Get market data
            vix = self._get_vix(d)
            spy_price = self._get_price("SPY", d)
            underlying_prices = self._get_all_underlying_prices(d)
            # Inject VIX into underlying_prices so vix_tail_hedge can execute
            underlying_prices["VIX"] = vix
            realized_vols = self._get_all_realized_vols(d)

            # 4. Risk-free rate
            rfr = self._iv_engine.get_risk_free_rate(d.year)

            # 5. Expire any positions
            expiry_pnl = self._book.expire_positions(d, underlying_prices)

            # 5b. Build vol term structure for today
            term_structure = self._get_term_structure(d, vix)

            # 6. Mark-to-market
            pnl_attr = self._book.mark_to_market(
                d, underlying_prices, vix, self._iv_engine, realized_vols, rfr,
                term_structure=term_structure,
            )

            # 7. Determine market situation
            situation = self._classify_situation(vix, d)

            # 8. Build signals dict
            signals = self._build_signals(
                d, equity_nav, vix, spy_price, underlying_prices, situation,
                realized_vols,
            )

            # 9. Run strategy allocator
            try:
                allocations = allocator.allocate(
                    market_situation=situation,
                    signals=signals,
                    portfolio_greeks=self._book.get_portfolio_greeks().to_dict(),
                    existing_positions=self._book.to_existing_options_list(),
                )
            except Exception as exc:
                logger.debug("Allocator error on %s: %s", d, exc)
                allocations = {}

            # 10. Run lifecycle manager
            try:
                lifecycle_directives = lifecycle.evaluate(
                    positions=self._book.to_existing_options_list(),
                    signals=signals,
                )
            except Exception as exc:
                logger.debug("Lifecycle error on %s: %s", d, exc)
                lifecycle_directives = []

            # 11. Execute lifecycle directives (CLOSE/ROLL)
            for directive in lifecycle_directives:
                self._execute_directive(directive, d, underlying_prices, vix, realized_vols, rfr, term_structure)

            # 11b. Risk guardrails
            halt_new_trades = False
            force_close_short = False

            if cfg.guardrails_enabled:
                total_nav_now = equity_nav + cumulative_options_pnl
                self._peak_nav = max(self._peak_nav, total_nav_now)
                current_dd = (total_nav_now / self._peak_nav - 1.0) if self._peak_nav > 0 else 0.0

                if self._guardrail_cooldown_remaining > 0:
                    self._guardrail_cooldown_remaining -= 1
                    halt_new_trades = True

                if current_dd <= cfg.guardrail_dd_close_pct:
                    # Extreme drawdown: force-close all short premium
                    force_close_short = True
                    halt_new_trades = True
                    self._guardrail_cooldown_remaining = cfg.guardrail_cooldown_days
                    self._guardrail_force_close_count += 1
                elif current_dd <= cfg.guardrail_dd_halt_pct or vix >= cfg.guardrail_vix_halt:
                    # Moderate drawdown or VIX spike: halt new short-premium
                    halt_new_trades = True
                    self._guardrail_cooldown_remaining = cfg.guardrail_cooldown_days
                    self._guardrail_triggered_count += 1

            # 11c. Force-close short premium positions if extreme drawdown
            if force_close_short:
                for pos in list(self._book.positions.values()):
                    if pos.strategy in _SHORT_PREMIUM_STRATS and pos.quantity < 0:
                        # Close at current marked price (updated by step 6)
                        self._book.close_position(pos.position_id, pos.current_price)

            # 12. Evaluate strategies for new trades
            new_directives = self._evaluate_strategies(
                d, signals, allocations, underlying_prices,
            )

            # 12b. Filter directives through guardrails
            if halt_new_trades:
                # Only allow defensive strategies (vix_tail_hedge, momentum_call, leaps)
                new_directives = [
                    d_ for d_ in new_directives
                    if d_.strategy not in _SHORT_PREMIUM_STRATS
                ]

            # 13. Execute new directives (OPEN)
            for directive in new_directives:
                if self._book.open_position_count >= cfg.max_position_count:
                    break
                self._execute_directive(directive, d, underlying_prices, vix, realized_vols, rfr, term_structure)

            # 14. Record daily state
            daily_options_pnl = pnl_attr.total_pnl + expiry_pnl
            cumulative_options_pnl += daily_options_pnl

            # 14a. Attribute daily P&L to strategies
            for pos in self._book.positions.values():
                strat = pos.strategy or "unknown"
                # Each position's daily PnL contribution ≈ price change × qty × multiplier
                daily_pos_pnl = (pos.current_price - pos.prev_price) * pos.multiplier * pos.quantity
                self._strategy_pnl[strat] = self._strategy_pnl.get(strat, 0.0) + daily_pos_pnl
            total_nav = equity_nav + cumulative_options_pnl

            greeks = self._book.get_portfolio_greeks()
            n_active = len(set(
                pos.strategy for pos in self._book.positions.values()
            ))

            snap = DailySnapshot(
                date=d,
                equity_nav=equity_nav,
                options_pnl=cumulative_options_pnl,
                total_nav=total_nav,
                options_daily_pnl=daily_options_pnl,
                net_delta=greeks.net_delta,
                net_gamma=greeks.net_gamma,
                net_theta=greeks.net_theta,
                net_vega=greeks.net_vega,
                delta_pnl=pnl_attr.delta_pnl,
                theta_pnl=pnl_attr.theta_pnl,
                vega_pnl=pnl_attr.vega_pnl,
                gamma_pnl=pnl_attr.gamma_pnl,
                n_positions=self._book.open_position_count,
                n_strategies_active=n_active,
                market_situation=situation,
                vix=vix,
            )
            self._result.daily_snapshots.append(snap)

            # 15. Persist to database
            if self._writer is not None:
                # Record daily summary
                self._writer.insert_daily_summary(
                    trade_date=d,
                    equity_nav=equity_nav,
                    options_cumulative_pnl=cumulative_options_pnl,
                    total_nav=total_nav,
                    options_daily_pnl=daily_options_pnl,
                    net_delta=greeks.net_delta,
                    net_gamma=greeks.net_gamma,
                    net_theta=greeks.net_theta,
                    net_vega=greeks.net_vega,
                    delta_pnl=pnl_attr.delta_pnl,
                    theta_pnl=pnl_attr.theta_pnl,
                    vega_pnl=pnl_attr.vega_pnl,
                    gamma_pnl=pnl_attr.gamma_pnl,
                    n_positions=self._book.open_position_count,
                    n_strategies_active=n_active,
                    market_situation=situation,
                    vix=vix,
                )
                # Snapshot all open positions with greeks
                self._writer.insert_daily_positions_from_book(
                    d, self._book, vix, underlying_prices, situation,
                )

            # 16. Periodic logging
            if day_count % cfg.log_every_n_days == 0:
                logger.info(
                    "[%s] NAV: $%.0f (eq: $%.0f + opt: $%.0f) | "
                    "VIX: %.1f | %s | %d pos | D=%.0f th=$%.0f",
                    d, total_nav, equity_nav, cumulative_options_pnl,
                    vix, situation, self._book.open_position_count,
                    greeks.net_delta, greeks.net_theta,
                )

        # Finalize
        self._result.config = {
            "start_date": str(cfg.start_date),
            "end_date": str(cfg.end_date),
            "initial_nav": cfg.initial_nav,
            "derivatives_budget_pct": cfg.derivatives_budget_pct,
            "n_trading_days": len(trading_days),
        }
        self._result.strategy_metrics = {
            strat: {
                "cumulative_pnl": round(pnl, 2),
                "trade_count": self._strategy_trades.get(strat, 0),
            }
            for strat, pnl in sorted(self._strategy_pnl.items(), key=lambda x: x[1])
        }
        self._result.compute_summary()

        # Add guardrail stats to summary
        self._result.summary["guardrail_halt_triggers"] = self._guardrail_triggered_count
        self._result.summary["guardrail_force_close_triggers"] = self._guardrail_force_close_count

        # Persist final summary
        if self._writer is not None:
            self._writer.flush()
            self._writer.update_run_summary(self._result.summary)

        logger.info(
            "Options backtest complete: %d days, CAGR=%.2f%%, Sharpe=%.3f, "
            "MaxDD=%.2f%%, Options PnL=$%.0f (guardrails: %d halts, %d force-closes)",
            len(trading_days),
            self._result.summary.get("cagr", 0) * 100,
            self._result.summary.get("sharpe", 0),
            self._result.summary.get("max_drawdown", 0) * 100,
            self._result.summary.get("options_total_pnl", 0),
            self._guardrail_triggered_count,
            self._guardrail_force_close_count,
        )

        return self._result

    # ── Strategy evaluation ──────────────────────────────────────────

    def _evaluate_strategies(
        self,
        as_of_date: date,
        signals: Dict[str, Any],
        allocations: Dict[str, Any],
        underlying_prices: Dict[str, float],
    ) -> list:
        """Run all enabled strategies and collect OPEN directives."""
        from prometheus.execution.options_strategy import (
            OptionsStrategyManager,
            OptionTradeDirective,
            TradeAction,
        )
        from prometheus.execution.broker_interface import BrokerInterface

        # Build a minimal portfolio dict from signals
        portfolio: Dict[str, Any] = {}
        for symbol, price in (signals.get("equity_prices", {}) or {}).items():
            portfolio[f"{symbol}.US"] = type("MockPos", (), {
                "quantity": int(signals.get("nav", 1e6) * 0.02 / max(price, 1)),
                "market_value": signals.get("nav", 1e6) * 0.02,
                "avg_cost": price,
            })()

        existing_options = self._book.to_existing_options_list()

        # Create strategies without broker (we handle execution ourselves)
        from prometheus.execution.options_strategy import (
            ProtectivePutStrategy, CoveredCallStrategy,
            SectorPutSpreadStrategy, VixTailHedgeStrategy,
            ShortPutStrategy, FuturesOverlayStrategy, FuturesOptionStrategy,
            BullCallSpreadStrategy, MomentumCallStrategy, LEAPSStrategy,
            IronCondorStrategy, IronButterflyStrategy,
            CollarStrategy, CalendarSpreadStrategy,
            StraddleStrangleStrategy, WheelStrategy,
            VixTailHedgeConfig, IronCondorConfig, IronButterflyConfig,
            ShortPutConfig, FuturesOverlayConfig, FuturesOptionConfig,
            BullCallSpreadConfig, MomentumCallConfig, LEAPSConfig,
            WheelConfig,
        )

        # Build config objects with optional overrides from grid search
        ov = self._strategy_overrides

        def _cfg(cls, key=None):
            """Create a config, applying any grid-search overrides."""
            name = key or cls.__name__
            return cls(**ov[name]) if name in ov else cls()

        strategies = [
            # protective_put DISABLED: structurally -EV (−$16M over 30yr).
            # straddle_strangle DISABLED: long vol bleeds theta (−$17M).
            # sector_put_spread DISABLED: SHI signal too noisy (−$3.2M v6).
            # calendar_spread DISABLED: −$16K/trade, contango timing poor.
            # covered_call DISABLED: lost money in 100/100 synthetic realities.
            VixTailHedgeStrategy(config=_cfg(VixTailHedgeConfig)),
            IronCondorStrategy(config=_cfg(IronCondorConfig)),
            IronButterflyStrategy(config=_cfg(IronButterflyConfig)),
            ShortPutStrategy(config=_cfg(ShortPutConfig)),
            FuturesOverlayStrategy(config=_cfg(FuturesOverlayConfig)),
            FuturesOptionStrategy(config=_cfg(FuturesOptionConfig)),
            BullCallSpreadStrategy(config=_cfg(BullCallSpreadConfig)),
            MomentumCallStrategy(config=_cfg(MomentumCallConfig)),
            LEAPSStrategy(config=_cfg(LEAPSConfig)),
            # collar DISABLED: −$285K over 30yr, pure drag with no crisis alpha.
            WheelStrategy(config=_cfg(WheelConfig)),
        ]

        # Apply allocations to enable/disable
        for strat in strategies:
            strat_name = strat.name
            if strat_name in allocations:
                alloc = allocations[strat_name]
                if hasattr(strat, "_config") and hasattr(strat._config, "enabled"):
                    strat._config.enabled = alloc.enabled
            elif hasattr(strat, "_config") and hasattr(strat._config, "enabled"):
                # Not in allocations → disable unless always-on
                if strat_name != "vix_tail_hedge":
                    strat._config.enabled = False

        all_directives = []
        for strategy in strategies:
            try:
                directives = strategy.evaluate(portfolio, signals, existing_options)
                # Only keep OPEN directives (lifecycle handles CLOSE/ROLL)
                for d in directives:
                    if d.action.value == "OPEN":
                        all_directives.append(d)
            except Exception as exc:
                logger.debug("Strategy %s error: %s", strategy.name, exc)

        return all_directives

    # ── Directive execution ──────────────────────────────────────────

    def _execute_directive(
        self,
        directive,
        as_of_date: date,
        underlying_prices: Dict[str, float],
        vix: float,
        realized_vols: Dict[str, float],
        rfr: float,
        term_structure: Optional[VolTermStructure] = None,
    ) -> None:
        """Execute a single OptionTradeDirective in the synthetic book."""
        symbol = directive.symbol
        S = underlying_prices.get(symbol, 0.0)
        if S <= 0:
            return

        action = directive.action.value

        if action == "CLOSE":
            # Close matching positions
            self._book.close_positions_for_symbol(
                symbol, directive.strategy,
                lambda pos: bs_price(
                    S, pos.strike, max(pos.dte(as_of_date), 1) / 365.0,
                    rfr,
                    self._iv_engine.get_iv(
                        strike=pos.strike, underlying_price=S,
                        dte=max(pos.dte(as_of_date), 1), vix=vix,
                        realized_vol_21d=realized_vols.get(symbol, 0.0),
                        symbol=symbol, right=pos.right,
                        term_structure=term_structure,
                    ),
                    pos.right,
                ),
            )
        elif action == "OPEN" or action == "HEDGE":
            # Price the new option
            dte = 45  # Default DTE
            if directive.expiry:
                try:
                    exp_date = date(
                        int(directive.expiry[:4]),
                        int(directive.expiry[4:6]),
                        int(directive.expiry[6:8]),
                    )
                    dte = max((exp_date - as_of_date).days, 1)
                except (ValueError, IndexError):
                    pass

            # Sanity cap: if DTE > 400 the strategy likely used a wall-clock
            # date instead of the backtest date.  Regenerate from chain.
            if dte > 400:
                dte = 0  # Force chain lookup below

            # If no expiry specified, find one from synthetic chain
            if not directive.expiry or dte <= 0:
                chain = self._chain_gen.generate_chain(symbol, S, as_of_date)
                expiry_str = chain.get_best_expiry(min_dte=30, max_dte=60)
                if expiry_str:
                    directive_expiry = expiry_str
                    exp_date = date(
                        int(expiry_str[:4]), int(expiry_str[4:6]), int(expiry_str[6:8]),
                    )
                    dte = (exp_date - as_of_date).days
                else:
                    return
            else:
                directive_expiry = directive.expiry

            rv = realized_vols.get(symbol, 0.0)
            iv = self._iv_engine.get_iv(
                strike=directive.strike, underlying_price=S,
                dte=dte, vix=vix, realized_vol_21d=rv,
                symbol=symbol, right=directive.right,
                term_structure=term_structure,
            )

            T = dte / 365.0
            mid_price = bs_price(S, directive.strike, T, rfr, iv, directive.right)

            if mid_price <= 0:
                return

            # Fill with slippage
            is_buy = directive.quantity > 0
            entry = fill_price(
                mid_price, S, directive.strike, dte,
                is_buy, symbol, self._config.slippage_pct,
            )

            pid = self._book.open_position(
                symbol=symbol,
                right=directive.right,
                expiry=directive_expiry,
                strike=directive.strike,
                quantity=directive.quantity,
                entry_price=entry,
                entry_date=as_of_date,
                strategy=directive.strategy,
                metadata=directive.metadata,
            )

            # Track trade count per strategy
            strat = directive.strategy or "unknown"
            self._strategy_trades[strat] = self._strategy_trades.get(strat, 0) + 1

            # Persist the trade
            if self._writer is not None:
                self._writer.insert_trade(
                    trade_date=as_of_date,
                    position_id=pid,
                    symbol=symbol,
                    right=directive.right,
                    expiry=directive_expiry,
                    strike=directive.strike,
                    action="OPEN",
                    quantity=directive.quantity,
                    price=entry,
                    mid_price=mid_price,
                    iv_at_trade=iv,
                    underlying_price=S,
                    vix_at_trade=vix,
                    strategy=directive.strategy,
                )

            # Execute spread leg if present
            if directive.spread_leg is not None:
                self._execute_directive(
                    directive.spread_leg, as_of_date,
                    underlying_prices, vix, realized_vols, rfr,
                    term_structure,
                )

        elif action == "ROLL":
            # Close existing, then we let the strategy open new ones
            self._book.close_positions_for_symbol(
                symbol, directive.strategy,
                lambda pos: bs_price(
                    S, pos.strike, max(pos.dte(as_of_date), 1) / 365.0,
                    rfr,
                    self._iv_engine.get_iv(
                        strike=pos.strike, underlying_price=S,
                        dte=max(pos.dte(as_of_date), 1), vix=vix,
                        realized_vol_21d=realized_vols.get(symbol, 0.0),
                        symbol=symbol, right=pos.right,
                        term_structure=term_structure,
                    ),
                    pos.right,
                ),
            )

    # ── Data access helpers

    def _preload_data(self) -> None:
        """Pre-load VIX, SPY prices, and realized vols from database."""
        if self._data_reader is None:
            logger.info("No DataReader configured — using fallback data generation")
            return

        cfg = self._config
        try:
            # Load VIX
            import pandas as pd
            import numpy as np
            vix_df = self._data_reader.read_prices_close(
                [cfg.vix_instrument_id], cfg.start_date, cfg.end_date,
            )
            for _, row in vix_df.iterrows():
                self._vix_cache[row["trade_date"]] = float(row["close"])
            logger.info("Loaded %d VIX data points", len(self._vix_cache))

            # Load SPY prices
            spy_df = self._data_reader.read_prices_close(
                [cfg.spy_instrument_id], cfg.start_date, cfg.end_date,
            )
            spy_prices: Dict[date, float] = {}
            for _, row in spy_df.iterrows():
                spy_prices[row["trade_date"]] = float(row["close"])
            self._price_cache["SPY"] = spy_prices
            logger.info("Loaded %d SPY price points", len(spy_prices))

            # Load sector ETF prices for signal computation
            sector_etfs = [
                "XLK.US", "XLF.US", "XLE.US", "XLV.US", "XLI.US",
                "XLP.US", "XLY.US", "XLU.US", "XLB.US",
            ]
            for etf_id in sector_etfs:
                try:
                    etf_df = self._data_reader.read_prices_close(
                        [etf_id], cfg.start_date, cfg.end_date,
                    )
                    etf_prices: Dict[date, float] = {}
                    for _, row in etf_df.iterrows():
                        etf_prices[row["trade_date"]] = float(row["close"])
                    if etf_prices:
                        symbol = etf_id.replace(".US", "")
                        self._price_cache[symbol] = etf_prices
                except Exception:
                    pass
            loaded_etfs = [s for s in self._price_cache if s != "SPY"]
            if loaded_etfs:
                logger.info(
                    "Loaded sector ETF prices: %s (%d-%d pts each)",
                    ", ".join(sorted(loaded_etfs)),
                    min(len(self._price_cache[s]) for s in loaded_etfs),
                    max(len(self._price_cache[s]) for s in loaded_etfs),
                )

            # Pre-compute 21-day realized vol for SPY and sector ETFs
            for symbol, cache in self._price_cache.items():
                if not cache:
                    continue
                sorted_dates = sorted(cache.keys())
                prices = [cache[d] for d in sorted_dates]
                vol_cache: Dict[date, float] = {}
                for i in range(21, len(prices)):
                    window = prices[i - 21 : i + 1]
                    rets = []
                    for j in range(1, len(window)):
                        if window[j - 1] > 0:
                            rets.append(np.log(window[j] / window[j - 1]))
                    if len(rets) >= 15:
                        rv = float(np.std(rets)) * np.sqrt(252)
                        vol_cache[sorted_dates[i]] = rv
                if vol_cache:
                    self._vol_cache[symbol] = vol_cache

            # Synthesize VIX for dates before VIX data starts.
            # Use 21-day realized vol of SPY × sqrt(252), scaled by
            # empirical IV/HV premium (~1.2) to approximate VIX.
            if spy_prices and self._vix_cache:
                vix_start = min(self._vix_cache.keys())
                spy_dates = sorted(spy_prices.keys())
                spy_closes = [spy_prices[d] for d in spy_dates]
                n_synth = 0
                for i, d in enumerate(spy_dates):
                    if d >= vix_start:
                        break
                    if i < 21:
                        continue
                    window = spy_closes[i - 21 : i + 1]
                    rets = []
                    for j in range(1, len(window)):
                        if window[j - 1] > 0:
                            rets.append(np.log(window[j] / window[j - 1]))
                    if len(rets) >= 15:
                        rv = float(np.std(rets)) * np.sqrt(252)
                        synth_vix = rv * 100.0 * 1.2  # Convert to VIX-like units
                        synth_vix = max(synth_vix, 10.0)  # Floor at 10
                        synth_vix = min(synth_vix, 80.0)  # Cap at 80
                        self._vix_cache[d] = synth_vix
                        n_synth += 1
                if n_synth > 0:
                    logger.info(
                        "Synthesized %d VIX points from SPY realized vol (pre-%s)",
                        n_synth, vix_start,
                    )

        except Exception as exc:
            logger.warning("Error pre-loading data: %s", exc)

        # Load vol term structure indices
        self._preload_vol_indices()

    def _preload_vol_indices(self) -> None:
        """Load VIX9D, VIX3M, VIX6M, VIX1Y, and SKEW from prices_daily."""
        if self._data_reader is None:
            return

        cfg = self._config
        ts_map = {
            "VIX9D.INDX": self._vix9d_cache,
            "VIX3M.INDX": self._vix3m_cache,
            "VIX6M.INDX": self._vix6m_cache,
            "VIX1Y.INDX": self._vix1y_cache,
            "SKEW.INDX": self._skew_cache,
        }

        for instrument_id, cache in ts_map.items():
            try:
                df = self._data_reader.read_prices_close(
                    [instrument_id], cfg.start_date, cfg.end_date,
                )
                for _, row in df.iterrows():
                    cache[row["trade_date"]] = float(row["close"])
                if cache:
                    logger.info(
                        "Loaded %d %s data points (%s → %s)",
                        len(cache), instrument_id,
                        min(cache.keys()), max(cache.keys()),
                    )
            except Exception as exc:
                logger.debug("Could not load %s: %s", instrument_id, exc)

    def _get_term_structure(self, d: date, vix: float) -> Optional[VolTermStructure]:
        """Build a VolTermStructure for a given date from cached data.

        Returns None when no term structure data is available (pre-2007).
        """
        # Helper: look up a value with weekend/holiday fallback
        def _lookup(cache: Dict[date, float]) -> Optional[float]:
            if d in cache:
                return cache[d]
            for offset in range(1, 5):
                prev = d - timedelta(days=offset)
                if prev in cache:
                    return cache[prev]
            return None

        vix9d = _lookup(self._vix9d_cache)
        vix3m = _lookup(self._vix3m_cache)
        vix6m = _lookup(self._vix6m_cache)
        vix1y = _lookup(self._vix1y_cache)
        skew = _lookup(self._skew_cache)

        # If we have at least one extra tenor beyond VIX, build the struct
        if any(v is not None for v in (vix9d, vix3m, vix6m, vix1y, skew)):
            return VolTermStructure(
                vix_9d=vix9d,
                vix_30d=vix,
                vix_3m=vix3m,
                vix_6m=vix6m,
                vix_1y=vix1y,
                skew=skew,
            )
        return None

    def _get_vix(self, d: date) -> float:
        """Get VIX level for a date, with fallback."""
        if d in self._vix_cache:
            return self._vix_cache[d]

        # Try nearby dates (weekends/holidays)
        for offset in range(1, 5):
            prev = d - timedelta(days=offset)
            if prev in self._vix_cache:
                return self._vix_cache[prev]

        # Fallback: estimate from SPY realized vol
        return 20.0  # Long-term VIX average

    def _get_price(self, symbol: str, d: date) -> float:
        """Get underlying price for a symbol on a date."""
        cache = self._price_cache.get(symbol, {})
        if d in cache:
            return cache[d]

        for offset in range(1, 5):
            prev = d - timedelta(days=offset)
            if prev in cache:
                return cache[prev]

        return 0.0

    def _get_equity_nav(self, d: date, fallback: float) -> float:
        """Get equity NAV for a date."""
        d_str = d.isoformat()
        if d_str in self._equity_nav:
            return self._equity_nav[d_str]

        # Try nearby dates
        for offset in range(1, 5):
            prev = (d - timedelta(days=offset)).isoformat()
            if prev in self._equity_nav:
                return self._equity_nav[prev]

        return fallback

    def _get_all_underlying_prices(self, d: date) -> Dict[str, float]:
        """Get prices for all cached underlyings."""
        result: Dict[str, float] = {}
        for symbol, cache in self._price_cache.items():
            price = cache.get(d, 0.0)
            if price <= 0:
                for offset in range(1, 5):
                    prev = d - timedelta(days=offset)
                    price = cache.get(prev, 0.0)
                    if price > 0:
                        break
            if price > 0:
                result[symbol] = price
        return result

    def _get_all_realized_vols(self, d: date) -> Dict[str, float]:
        """Get realized vols for all cached underlyings."""
        result: Dict[str, float] = {}
        for symbol, cache in self._vol_cache.items():
            vol = cache.get(d, 0.0)
            if vol <= 0:
                for offset in range(1, 5):
                    prev = d - timedelta(days=offset)
                    vol = cache.get(prev, 0.0)
                    if vol > 0:
                        break
            if vol > 0:
                result[symbol] = vol
        return result

    def _get_trading_days(
        self, start: date, end: date,
    ) -> List[date]:
        """Generate list of trading days (weekday filter)."""
        days: List[date] = []
        cursor = start
        while cursor <= end:
            # Simple weekday filter (Mon-Fri)
            if cursor.weekday() < 5:
                days.append(cursor)
            cursor += timedelta(days=1)
        return days

    def _classify_situation(self, vix: float, d: date) -> str:
        """Market situation classification from VIX with RECOVERY detection.

        RECOVERY is detected when VIX is in the 20-25 range AND the recent
        63-day maximum VIX was >= 30 (i.e. we're falling from crisis/risk-off).
        """
        # Track VIX history for RECOVERY detection
        if not hasattr(self, '_vix_history'):
            self._vix_history: list = []
        self._vix_history.append(vix)
        if len(self._vix_history) > 63:
            self._vix_history = self._vix_history[-63:]

        if vix >= 35:
            return "CRISIS"
        elif vix >= 25:
            return "RISK_OFF"
        elif vix >= 20:
            recent_max = max(self._vix_history) if self._vix_history else vix
            if recent_max >= 30:
                return "RECOVERY"
            return "NEUTRAL"
        else:
            return "RISK_ON"

    # ── Synthetic signal helpers ─────────────────────────────────────

    def _lookup_price_n_days_ago(
        self, symbol: str, d: date, n_calendar_days: int,
    ) -> float:
        """Look up price approximately *n* calendar days ago (with slack)."""
        cache = self._price_cache.get(symbol, {})
        for offset in range(n_calendar_days, n_calendar_days + 10):
            past = d - timedelta(days=offset)
            if past in cache:
                return cache[past]
        return 0.0

    def _compute_lambda_scores(
        self, d: date, underlying_prices: Dict[str, float],
    ) -> Dict[str, float]:
        """Momentum-based conviction proxy for lambda universe scores.

        Score = 63-day (3-month) return mapped to [0, 1]:
        -10% return → 0.0, 0% → 0.5, +10% → 1.0.
        """
        scores: Dict[str, float] = {}
        for symbol in self._price_cache:
            if symbol == "SPY" or ".INDX" in symbol:
                continue
            price = underlying_prices.get(symbol, 0.0)
            if price <= 0:
                continue
            past_price = self._lookup_price_n_days_ago(symbol, d, 63)
            if past_price <= 0:
                continue
            ret = price / past_price - 1.0
            score = min(1.0, max(0.0, (ret + 0.10) / 0.20))
            scores[symbol] = round(score, 3)
        return scores

    def _compute_stab_scores(
        self, realized_vols: Dict[str, float],
    ) -> Dict[str, float]:
        """Stability scores from realised vol (lower vol → higher stab).

        Vol 5% → stab 1.0, vol 35% → stab 0.50, vol 65% → stab 0.0.
        """
        scores: Dict[str, float] = {}
        for symbol, vol in realized_vols.items():
            if vol <= 0:
                continue
            stab = max(0.0, min(1.0, 1.0 - (vol - 0.05) / 0.60))
            scores[symbol] = round(stab, 3)
        return scores

    _SECTOR_ETF_TO_NAME: Dict[str, str] = {
        "XLK": "Technology",
        "XLF": "Financial Services",
        "XLE": "Energy",
        "XLV": "Healthcare",
        "XLI": "Industrials",
        "XLP": "Consumer Defensive",
        "XLY": "Consumer Cyclical",
        "XLU": "Utilities",
        "XLB": "Basic Materials",
    }

    def _compute_sector_shi(
        self, d: date, underlying_prices: Dict[str, float],
        realized_vols: Dict[str, float],
    ) -> Dict[str, float]:
        """Sector health index from ETF momentum + stability.

        Blends 63-day momentum (60%) with vol-stability (40%).
        """
        shi: Dict[str, float] = {}
        for etf, sector_name in self._SECTOR_ETF_TO_NAME.items():
            price = underlying_prices.get(etf, 0.0)
            if price <= 0:
                continue
            past = self._lookup_price_n_days_ago(etf, d, 63)
            if past <= 0:
                continue
            ret = price / past - 1.0
            momentum = min(1.0, max(0.0, (ret + 0.10) / 0.20))

            vol = realized_vols.get(etf, 0.20)
            stab = max(0.0, min(1.0, 1.0 - (vol - 0.05) / 0.60))

            shi[sector_name] = round(momentum * 0.6 + stab * 0.4, 3)
        return shi

    def _compute_sector_exposures(
        self, equity_nav: float,
    ) -> Dict[str, float]:
        """Equal-weight sector exposure estimate from NAV."""
        n_sectors = len(self._SECTOR_ETF_TO_NAME)
        if n_sectors == 0 or equity_nav <= 0:
            return {}
        per_sector = equity_nav / n_sectors
        return {
            name: per_sector
            for name in self._SECTOR_ETF_TO_NAME.values()
        }

    def _compute_frag(self, d: date, vix: float) -> float:
        """Data-driven fragility from VIX percentile rank (trailing 252d)."""
        values: list = []
        for offset in range(1, 253):
            past = d - timedelta(days=offset)
            if past in self._vix_cache:
                values.append(self._vix_cache[past])
        if len(values) < 20:
            # Not enough history – fall back to VIX-level heuristic
            if vix >= 25:
                return 0.70
            elif vix >= 20:
                return 0.40
            return 0.15
        below = sum(1 for v in values if v < vix)
        return round(below / len(values), 3)

    def _compute_vix_contango(self, d: date, vix: float) -> float:
        """VIX term-structure contango from real VIX3M data.

        contango = (VIX3M - VIX) / VIX.  Falls back to proxy formula
        when VIX3M data is unavailable.
        """
        vix3m = self._vix3m_cache.get(d)
        if vix3m is None:
            for offset in range(1, 5):
                prev = d - timedelta(days=offset)
                if prev in self._vix3m_cache:
                    vix3m = self._vix3m_cache[prev]
                    break
        if vix3m is not None and vix > 0:
            return round((vix3m - vix) / vix, 4)
        # Proxy fallback
        return max(0.0, (20.0 - vix) / 100.0)

    def _build_signals(
        self,
        d: date,
        equity_nav: float,
        vix: float,
        spy_price: float,
        underlying_prices: Dict[str, float],
        situation: str,
        realized_vols: Dict[str, float],
    ) -> Dict[str, Any]:
        """Build the signals dict consumed by strategies."""
        lambda_scores = self._compute_lambda_scores(d, underlying_prices)
        stab_scores = self._compute_stab_scores(realized_vols)
        sector_shi = self._compute_sector_shi(d, underlying_prices, realized_vols)
        frag = self._compute_frag(d, vix)
        mhi = round(1.0 - frag, 3)
        lambda_agg = (
            sum(lambda_scores.values()) / len(lambda_scores)
            if lambda_scores else 0.5
        )

        # SPY 63-day momentum for momentum call overlay
        spy_past = self._lookup_price_n_days_ago("SPY", d, 63)
        spy_momentum_63d = (
            (spy_price / spy_past - 1.0) if spy_past > 0 and spy_price > 0 else 0.0
        )

        return {
            "as_of_date": d,
            "nav": equity_nav,
            "buying_power": equity_nav * self._config.derivatives_budget_pct,
            "market_state": situation,
            "mhi": mhi,
            "frag": frag,
            "vix_level": vix,
            "spy_price": spy_price,
            "spy_momentum_63d": round(spy_momentum_63d, 4),
            "es_price": spy_price * 10.0 if spy_price > 0 else 0.0,
            "lambda_scores": lambda_scores,
            "lambda_aggregate": round(lambda_agg, 3),
            "stab_scores": stab_scores,
            "sector_shi": sector_shi,
            "sector_exposures": self._compute_sector_exposures(equity_nav),
            "vix_contango": self._compute_vix_contango(d, vix),
            "etf_prices": {
                sym: underlying_prices.get(sym, 0.0)
                for sym in ["XLK", "XLF", "XLE", "XLV", "XLI", "XLP", "XLY", "XLU", "XLB"]
            },
            "equity_prices": underlying_prices,
            "futures_positions": {},
        }


__all__ = [
    "OptionsBacktestConfig",
    "OptionsBacktestResult",
    "OptionsBacktestEngine",
    "DailySnapshot",
]
