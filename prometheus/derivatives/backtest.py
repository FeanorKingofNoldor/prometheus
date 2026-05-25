"""Backtest harness for the new sleeve pipeline.

Replays the new ``run_sleeve`` against historical days so we can:

* Verify a new template behaves correctly *before* it sees live shadow
  data — every template change can be validated against a year of
  history in seconds.
* Diff what the new pipeline would have done against what the legacy
  ``options_strategy.py`` classes actually did (via the ``engine_decisions``
  table). That diff is the Phase 2 cutover evidence.
* Stress-test edge cases (regime transitions, illiquid days, missing
  signals) without touching IBKR.

Design: the harness wraps the synthetic chain generator + IV surface +
bid-ask model from ``prometheus.backtest`` into adapters that match the
production ``ContractDiscoveryService`` / ``IvLookupService`` /
``LiquidityFilter`` interfaces. The new ``run_sleeve`` runs unchanged
against these adapters — what we're testing is the real production
selection + sizing code, just with synthetic data underneath.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date
from typing import Any

from apatheon.core.logging import get_logger

from prometheus.backtest.iv_surface import IVSurfaceEngine
from prometheus.backtest.option_pricer import bs_price, compute_bid_ask
from prometheus.backtest.synthetic_chain import (
    SyntheticChainGenerator,
    SyntheticOptionChain,
)
from prometheus.derivatives.iv_lookup import (
    IV_SOURCE_LIVE,
    IvLookupResult,
)
from prometheus.derivatives.liquidity_filter import (
    REJECT_NO_BID,
    REJECT_NO_QUOTE,
    REJECT_WIDE_SPREAD,
    LiquidityFilterResult,
    LiquidityQuote,
    LiquidityRejection,
)
from prometheus.derivatives.runner import (
    SleeveDirective,
    SleeveRunResult,
    SleeveSkip,
    run_sleeve,
)
from prometheus.derivatives.sleeves import (
    Sleeve,
    SleeveConfig,
    default_sleeves,
)
from prometheus.execution.contract_discovery import OptionChainParams

logger = get_logger(__name__)


SignalProvider = Callable[[date], Mapping[str, Any]]
UnderlyingPriceProvider = Callable[[date, str], float]


# ── Adapters: synthetic data → production interfaces ─────────────────


class BacktestDiscovery:
    """Adapter that fits ``select_contract``'s ``discovery`` parameter."""

    def __init__(
        self,
        as_of_date: date,
        underlying_price_provider: UnderlyingPriceProvider,
        *,
        generator: SyntheticChainGenerator | None = None,
    ) -> None:
        self._as_of = as_of_date
        self._price_provider = underlying_price_provider
        self._generator = generator or SyntheticChainGenerator()

    def set_as_of(self, as_of: date) -> None:
        self._as_of = as_of

    def discover_option_chain(
        self, symbol: str, *,
        sec_type: str = "STK",
        exchange: str | None = None,
        trading_class: str | None = None,
    ) -> list[OptionChainParams]:
        price = float(self._price_provider(self._as_of, symbol) or 0.0)
        if price <= 0:
            return []
        synth = self._generator.generate_chain(symbol, price, self._as_of)
        return [_synth_to_chain_params(synth)]


class BacktestIvLookup:
    """In-memory IV source for the backtest harness.

    Mirrors ``IvLookupService`` shape: ``get_iv_batch(contracts, ...) -> dict``.
    """

    def __init__(
        self,
        as_of_date: date,
        signal_provider: SignalProvider,
        underlying_price_provider: UnderlyingPriceProvider,
        *,
        iv_engine: IVSurfaceEngine | None = None,
    ) -> None:
        self._as_of = as_of_date
        self._signal_provider = signal_provider
        self._price_provider = underlying_price_provider
        self._iv_engine = iv_engine or IVSurfaceEngine()

    def set_as_of(self, as_of: date) -> None:
        self._as_of = as_of

    def get_iv_batch(
        self, contracts, *, fallback_iv: float,
    ) -> dict[str, IvLookupResult]:
        signals = self._signal_provider(self._as_of)
        vix = float(signals.get("vix_level", 20.0) or 20.0)
        out: dict[str, IvLookupResult] = {}
        for c in contracts:
            symbol = getattr(c, "symbol", "?")
            strike = float(getattr(c, "strike", 0) or 0)
            right = str(getattr(c, "right", "P") or "P")
            expiry = str(getattr(c, "lastTradeDateOrContractMonth", "") or "")
            dte = _dte(expiry, self._as_of)
            underlying_price = float(
                self._price_provider(self._as_of, symbol) or 0.0
            )
            if underlying_price <= 0:
                # Strike as last-resort spot proxy keeps the surface
                # well-defined; the resulting IV is recorded but the
                # liquidity filter will independently drop the strike.
                underlying_price = strike
            iv = self._iv_engine.get_iv(
                strike=strike,
                underlying_price=underlying_price,
                dte=max(dte, 1),
                vix=vix,
                symbol=symbol,
                right=right,
            )
            key = f"{symbol}:{expiry}:{strike}:{right}"
            out[key] = IvLookupResult(
                iv=iv, source=IV_SOURCE_LIVE,
                underlying_price=underlying_price, fetched_at=0.0,
            )
        return out


class BacktestLiquidityFilter:
    """In-memory liquidity filter using the synthetic bid-ask model."""

    def __init__(
        self,
        as_of_date: date,
        signal_provider: SignalProvider,
        underlying_price_provider: UnderlyingPriceProvider,
        *,
        min_bid: float = 0.05,
        max_spread_pct: float = 0.30,
        iv_engine: IVSurfaceEngine | None = None,
    ) -> None:
        self._as_of = as_of_date
        self._signal_provider = signal_provider
        self._price_provider = underlying_price_provider
        self._min_bid = min_bid
        self._max_spread_pct = max_spread_pct
        self._iv_engine = iv_engine or IVSurfaceEngine()

    def set_as_of(self, as_of: date) -> None:
        self._as_of = as_of

    def filter(self, contracts) -> LiquidityFilterResult:
        signals = self._signal_provider(self._as_of)
        vix = float(signals.get("vix_level", 20.0) or 20.0)
        accepted: list[tuple[Any, LiquidityQuote]] = []
        rejected: list[tuple[Any, LiquidityRejection]] = []

        for c in contracts:
            symbol = getattr(c, "symbol", "?")
            strike = float(getattr(c, "strike", 0) or 0)
            right = str(getattr(c, "right", "P") or "P")
            expiry = str(getattr(c, "lastTradeDateOrContractMonth", "") or "")
            dte = _dte(expiry, self._as_of)
            underlying = float(self._price_provider(self._as_of, symbol) or 0.0)

            if underlying <= 0 or dte <= 0:
                rejected.append((c, LiquidityRejection(REJECT_NO_QUOTE, None)))
                continue

            iv = self._iv_engine.get_iv(
                strike=strike, underlying_price=underlying,
                dte=dte, vix=vix, symbol=symbol, right=right,
            )
            mid = bs_price(
                S=underlying, K=strike, T=max(dte, 1) / 365.0,
                r=0.045, sigma=iv, right=right,
            )
            bid, ask = compute_bid_ask(
                mid_price=mid, underlying_price=underlying,
                strike=strike, dte=dte, symbol=symbol,
            )

            if bid <= 0 and ask <= 0 and mid <= 0:
                rejected.append((c, LiquidityRejection(REJECT_NO_QUOTE, None)))
                continue

            spread_pct = ((ask - bid) / mid) if mid > 0 and ask > bid else 0.0
            quote = LiquidityQuote(
                bid=bid, ask=ask, last=mid, mid=mid,
                spread_pct=spread_pct, fetched_at=0.0,
            )

            if bid <= self._min_bid:
                rejected.append((c, LiquidityRejection(REJECT_NO_BID, quote)))
                continue
            if spread_pct > self._max_spread_pct:
                rejected.append((c, LiquidityRejection(REJECT_WIDE_SPREAD, quote)))
                continue

            accepted.append((c, quote))

        return LiquidityFilterResult(accepted, rejected)


# ── Replay engine ────────────────────────────────────────────────────


@dataclass(frozen=True)
class BacktestDayResult:
    as_of_date: date
    nav: float
    sleeve_results: list[SleeveRunResult]

    @property
    def directives(self) -> list[SleeveDirective]:
        out: list[SleeveDirective] = []
        for r in self.sleeve_results:
            out.extend(r.directives)
        return out

    @property
    def skips(self) -> list[SleeveSkip]:
        out: list[SleeveSkip] = []
        for r in self.sleeve_results:
            out.extend(r.skips)
        return out


@dataclass(frozen=True)
class BacktestReplayResult:
    start_date: date
    end_date: date
    days: list[BacktestDayResult]

    @property
    def total_days(self) -> int:
        return len(self.days)

    @property
    def total_directives(self) -> int:
        return sum(len(d.directives) for d in self.days)

    @property
    def total_skips(self) -> int:
        return sum(len(d.skips) for d in self.days)

    def by_template(self) -> dict[str, dict[str, int]]:
        """Per-template counters: fired, skipped, by skip reason."""
        out: dict[str, dict[str, int]] = {}
        for day in self.days:
            for d in day.directives:
                t = out.setdefault(d.template_name, {"fired": 0, "skipped": 0})
                t["fired"] += 1
            for s in day.skips:
                t = out.setdefault(s.template_name, {"fired": 0, "skipped": 0})
                t["skipped"] += 1
                t[s.reason] = t.get(s.reason, 0) + 1
        return out


def replay_day(
    *,
    as_of_date: date,
    nav: float,
    signal_provider: SignalProvider,
    underlying_price_provider: UnderlyingPriceProvider,
    sleeves_cfg: Mapping[Sleeve, SleeveConfig] | None = None,
    open_contracts_by_template: Mapping[str, int] | None = None,
    iv_engine: IVSurfaceEngine | None = None,
    chain_generator: SyntheticChainGenerator | None = None,
) -> BacktestDayResult:
    """Replay one trading day through every sleeve."""
    sleeves_cfg = sleeves_cfg or default_sleeves()
    open_map = dict(open_contracts_by_template or {})
    iv_engine = iv_engine or IVSurfaceEngine()

    discovery = BacktestDiscovery(
        as_of_date=as_of_date,
        underlying_price_provider=underlying_price_provider,
        generator=chain_generator,
    )
    iv_svc = BacktestIvLookup(
        as_of_date=as_of_date,
        signal_provider=signal_provider,
        underlying_price_provider=underlying_price_provider,
        iv_engine=iv_engine,
    )
    liq_svc = BacktestLiquidityFilter(
        as_of_date=as_of_date,
        signal_provider=signal_provider,
        underlying_price_provider=underlying_price_provider,
        iv_engine=iv_engine,
    )

    signals = signal_provider(as_of_date)

    def _price_fn(symbol: str) -> float:
        return float(underlying_price_provider(as_of_date, symbol) or 0.0)

    results: list[SleeveRunResult] = []
    for cfg in sleeves_cfg.values():
        results.append(
            run_sleeve(
                cfg,
                signals=signals, nav=nav,
                open_contracts_by_template=open_map,
                underlying_price_fn=_price_fn,
                discovery=discovery, iv_lookup=iv_svc, liquidity=liq_svc,
                today=as_of_date,
            )
        )

    return BacktestDayResult(
        as_of_date=as_of_date, nav=nav, sleeve_results=results,
    )


def replay_sleeve_pipeline(
    *,
    start_date: date,
    end_date: date,
    nav: float,
    signal_provider: SignalProvider,
    underlying_price_provider: UnderlyingPriceProvider,
    trading_days: list[date] | None = None,
    sleeves_cfg: Mapping[Sleeve, SleeveConfig] | None = None,
    iv_engine: IVSurfaceEngine | None = None,
) -> BacktestReplayResult:
    """Walk every trading day in the range and replay each sleeve.

    ``trading_days`` is optional — pass an explicit list for backtests
    that must hit a curated calendar (e.g. tests). When not provided,
    the harness walks every weekday in the range; consumers wanting
    holiday-aware behaviour should pass their own list.
    """
    days = trading_days or _weekdays(start_date, end_date)
    results: list[BacktestDayResult] = []
    for d in days:
        results.append(
            replay_day(
                as_of_date=d, nav=nav,
                signal_provider=signal_provider,
                underlying_price_provider=underlying_price_provider,
                sleeves_cfg=sleeves_cfg,
                iv_engine=iv_engine,
            )
        )

    return BacktestReplayResult(
        start_date=start_date, end_date=end_date, days=results,
    )


# ── Legacy-vs-new diff ───────────────────────────────────────────────


@dataclass(frozen=True)
class LegacyOption:
    """One legacy options decision pulled from ``engine_decisions``.

    Kept independent of the engine_decisions schema so the diff can be
    fed from any source (real DB rows in production, hand-built in
    tests).
    """

    as_of_date: date
    symbol: str
    right: str
    strike: float
    expiry: str
    quantity: int
    strategy: str       # legacy class name


@dataclass(frozen=True)
class DiffEntry:
    as_of_date: date
    template_or_strategy: str
    new_side: SleeveDirective | None
    legacy_side: LegacyOption | None

    @property
    def kind(self) -> str:
        if self.new_side and self.legacy_side:
            return "both"
        if self.new_side:
            return "new_only"
        return "legacy_only"


@dataclass(frozen=True)
class DiffSummary:
    entries: list[DiffEntry]

    @property
    def by_kind(self) -> dict[str, int]:
        out: dict[str, int] = {}
        for e in self.entries:
            out[e.kind] = out.get(e.kind, 0) + 1
        return out

    @property
    def strike_divergence_count(self) -> int:
        n = 0
        for e in self.entries:
            if e.new_side is not None and e.legacy_side is not None:
                if abs(e.new_side.strike - e.legacy_side.strike) > 1e-6:
                    n += 1
        return n


def diff_against_legacy(
    *,
    backtest_result: BacktestReplayResult,
    legacy_by_date: Mapping[date, list[LegacyOption]],
    template_to_strategy: Mapping[str, str] | None = None,
) -> DiffSummary:
    """Compare the new pipeline's per-day directives against legacy ones.

    ``template_to_strategy`` maps a new template name to the legacy
    strategy name it replaces (e.g. ``"hedge.spy_protective_put" ->
    "protective_put"``). Entries are paired by (date, symbol, right)
    where strategies match — anything unmatched goes in the
    ``new_only`` / ``legacy_only`` buckets.
    """
    mapping = dict(template_to_strategy or _default_template_to_strategy())
    entries: list[DiffEntry] = []

    for day in backtest_result.days:
        legacy_today = list(legacy_by_date.get(day.as_of_date, []))
        # Index legacy by (strategy, symbol, right) so we can pair up
        legacy_idx: dict[tuple[str, str, str], LegacyOption] = {
            (lo.strategy, lo.symbol.upper(), lo.right.upper()): lo
            for lo in legacy_today
        }

        for d in day.directives:
            legacy_key_strategy = mapping.get(d.template_name, "")
            key = (legacy_key_strategy, d.underlying.upper(), d.right.upper())
            legacy = legacy_idx.pop(key, None)
            entries.append(
                DiffEntry(
                    as_of_date=day.as_of_date,
                    template_or_strategy=d.template_name,
                    new_side=d, legacy_side=legacy,
                )
            )

        # Any legacy entries left over → legacy_only
        for legacy in legacy_idx.values():
            entries.append(
                DiffEntry(
                    as_of_date=day.as_of_date,
                    template_or_strategy=legacy.strategy,
                    new_side=None, legacy_side=legacy,
                )
            )

    return DiffSummary(entries=entries)


def _default_template_to_strategy() -> dict[str, str]:
    """Best-known mapping between the new template names and the legacy
    strategy class names they replace. Phases 2-4 extend this map as
    each sleeve's template set is built out."""
    return {
        "hedge.spy_protective_put": "protective_put",
        "hedge.sector_put_spread": "sector_put_spread",
        "hedge.vix_tail_call": "vix_tail_hedge",
        "hedge.collar": "collar",
        "income.spy_short_put": "short_put",
        "income.spy_iron_butterfly": "iron_butterfly",
        "income.spy_iron_condor": "iron_condor",
        "income.covered_call": "covered_call",
        "income.wheel": "wheel",
        "convex.thematic_sector_put": "crisis_alpha",
        "convex.vix_escalation_call": "vix_tail_hedge",
    }


# ── Helpers ──────────────────────────────────────────────────────────


def _synth_to_chain_params(synth: SyntheticOptionChain) -> OptionChainParams:
    return OptionChainParams(
        exchange=synth.exchange,
        underlying_con_id=0,
        trading_class=synth.trading_class or synth.symbol,
        multiplier=str(synth.multiplier),
        expirations=frozenset(synth.expirations),
        strikes=frozenset(synth.strikes),
    )


def _dte(expiry: str, as_of: date) -> int:
    from datetime import datetime
    try:
        d = datetime.strptime(expiry[:8], "%Y%m%d").date()
    except (ValueError, IndexError):
        return 0
    return (d - as_of).days


def _weekdays(start: date, end: date) -> list[date]:
    from datetime import timedelta
    out: list[date] = []
    d = start
    while d <= end:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


__all__ = [
    "BacktestDiscovery",
    "BacktestIvLookup",
    "BacktestLiquidityFilter",
    "BacktestDayResult",
    "BacktestReplayResult",
    "LegacyOption",
    "DiffEntry",
    "DiffSummary",
    "SignalProvider",
    "UnderlyingPriceProvider",
    "replay_day",
    "replay_sleeve_pipeline",
    "diff_against_legacy",
]
