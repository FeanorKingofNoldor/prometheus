"""Prometheus v2 – Contract Discovery Service.

Programmatically discovers tradeable contracts from Interactive Brokers:

* **Option chains** via ``reqSecDefOptParams`` (fast, non-throttled)
* **Futures chains** via ``reqContractDetails`` with ``secType=FUT``
* **Futures-option chains** via ``reqSecDefOptParams`` on future underlyings
* **Delta-based strike selection** via Black-Scholes approximation

All discovery results are cached in memory with configurable TTL to
respect IBKR rate limits.

Usage::

    from prometheus.execution.contract_discovery import ContractDiscoveryService

    svc = ContractDiscoveryService(ib)
    chain = svc.discover_option_chain("AAPL")
    fut = svc.get_front_month_future("ES", "CME")
    strike = svc.get_option_by_delta("SPY", "20260620", "P", 0.25)
"""

from __future__ import annotations

import math
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, FrozenSet, List, Optional

from apathis.core.logging import get_logger

logger = get_logger(__name__)


# ── Data containers ──────────────────────────────────────────────────

@dataclass(frozen=True)
class OptionChainParams:
    """Discovered option chain parameters for one exchange/tradingClass."""

    exchange: str
    underlying_con_id: int
    trading_class: str
    multiplier: str
    expirations: FrozenSet[str]   # Set of YYYYMMDD strings
    strikes: FrozenSet[float]

    def filter_expirations(
        self,
        min_dte: int = 0,
        max_dte: int = 365,
        today: Optional[date] = None,
    ) -> List[str]:
        """Return sorted expirations within a DTE range."""
        ref = today or date.today()
        result = []
        for exp_str in sorted(self.expirations):
            try:
                exp_date = datetime.strptime(exp_str[:8], "%Y%m%d").date()
                dte = (exp_date - ref).days
                if min_dte <= dte <= max_dte:
                    result.append(exp_str)
            except ValueError:
                continue
        return result

    def filter_strikes(
        self,
        center: float,
        width_pct: float = 0.20,
        step: Optional[float] = None,
    ) -> List[float]:
        """Return sorted strikes within ``center ± width_pct``."""
        lo = center * (1 - width_pct)
        hi = center * (1 + width_pct)
        filtered = sorted(s for s in self.strikes if lo <= s <= hi)
        if step is not None and step > 0:
            filtered = [s for s in filtered if abs(s % step) < 1e-6 or abs(s % step - step) < 1e-6]
        return filtered


@dataclass(frozen=True)
class FuturesContract:
    """Discovered futures contract metadata."""

    con_id: int
    symbol: str
    local_symbol: str
    exchange: str
    currency: str
    last_trade_date: str         # YYYYMMDD
    multiplier: str
    trading_class: str

    @property
    def dte(self) -> int:
        try:
            exp = datetime.strptime(self.last_trade_date[:8], "%Y%m%d").date()
            return (exp - date.today()).days
        except ValueError:
            return 0


@dataclass
class _CacheEntry:
    """TTL-wrapped cache entry."""

    data: Any
    created_at: float  # time.monotonic()
    ttl_sec: float

    @property
    def expired(self) -> bool:
        return (time.monotonic() - self.created_at) > self.ttl_sec


# ── Black-Scholes helpers ────────────────────────────────────────────

def _norm_cdf(x: float) -> float:
    """Standard normal CDF (Abramowitz & Stegun approximation)."""
    a1 = 0.254829592
    a2 = -0.284496736
    a3 = 1.421413741
    a4 = -1.453152027
    a5 = 1.061405429
    p = 0.3275911
    sign = 1.0 if x >= 0 else -1.0
    x = abs(x)
    t = 1.0 / (1.0 + p * x)
    y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * math.exp(-x * x / 2.0)
    return 0.5 * (1.0 + sign * y)


def _bs_delta(
    S: float,
    K: float,
    T: float,
    r: float,
    sigma: float,
    right: str,
) -> float:
    """Black-Scholes delta for a European option.

    Parameters
    ----------
    S : float
        Current underlying price.
    K : float
        Strike price.
    T : float
        Time to expiration in years.
    r : float
        Risk-free rate (annualised).
    sigma : float
        Implied volatility (annualised).
    right : str
        "C" for call, "P" for put.

    Returns
    -------
    float
        Delta ∈ (0, 1) for calls, (-1, 0) for puts.
    """
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return 0.0
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
    if right.upper() == "C":
        return _norm_cdf(d1)
    else:
        return _norm_cdf(d1) - 1.0


# ── Discovery service ────────────────────────────────────────────────

class ContractDiscoveryService:
    """Discover tradeable contracts from IBKR.

    Parameters
    ----------
    ib : Any
        Connected ``ib_async.IB`` (or ``ib_insync.IB``) instance.
    option_chain_ttl_sec : int
        Cache TTL for option chain data (default 3600 = 1 hour).
    futures_chain_ttl_sec : int
        Cache TTL for futures chain data (default 86400 = 1 day).
    qualify_batch_size : int
        Max contracts per ``qualifyContracts`` call.
    qualify_delay_sec : float
        Delay between qualification batches to avoid throttling.
    default_risk_free_rate : float
        Risk-free rate for Black-Scholes delta calculations.
    """

    def __init__(
        self,
        ib: Any,
        *,
        option_chain_ttl_sec: int = 3600,
        futures_chain_ttl_sec: int = 86400,
        qualify_batch_size: int = 50,
        qualify_delay_sec: float = 0.5,
        default_risk_free_rate: float = 0.045,
    ) -> None:
        self._ib = ib
        self._option_chain_ttl = option_chain_ttl_sec
        self._futures_chain_ttl = futures_chain_ttl_sec
        self._qualify_batch_size = qualify_batch_size
        self._qualify_delay = qualify_delay_sec
        self._risk_free_rate = default_risk_free_rate

        self._cache: Dict[str, _CacheEntry] = {}
        self._lock = threading.Lock()

    # ── Cache helpers ─────────────────────────────────────────────────

    def _cache_get(self, key: str) -> Any:
        with self._lock:
            entry = self._cache.get(key)
            if entry is not None and not entry.expired:
                return entry.data
            if entry is not None:
                del self._cache[key]
        return None

    def _cache_set(self, key: str, data: Any, ttl: float) -> None:
        with self._lock:
            self._cache[key] = _CacheEntry(
                data=data,
                created_at=time.monotonic(),
                ttl_sec=ttl,
            )

    def clear_cache(self) -> None:
        """Flush the entire discovery cache."""
        with self._lock:
            self._cache.clear()

    # ── Option chain discovery ────────────────────────────────────────

    def discover_option_chain(
        self,
        symbol: str,
        sec_type: str = "STK",
        con_id: Optional[int] = None,
        exchange: Optional[str] = None,
        trading_class: Optional[str] = None,
    ) -> List[OptionChainParams]:
        """Discover available option chain parameters for an underlying.

        Uses ``reqSecDefOptParams`` which returns strikes and expirations
        grouped by exchange/tradingClass.  This is the fast, non-throttled
        approach recommended by IBKR.

        Parameters
        ----------
        symbol : str
            Underlying symbol (e.g. "AAPL", "SPY", "ES").
        sec_type : str
            Security type of the underlying: "STK", "IND", "FUT".
        con_id : int, optional
            Contract ID of the underlying.  If not provided, the service
            will qualify the underlying first.
        exchange : str, optional
            Filter results to a specific exchange (e.g. "SMART", "CBOE").
        trading_class : str, optional
            Filter to a specific trading class (e.g. "SPX" vs "SPXW").

        Returns
        -------
        list[OptionChainParams]
            One entry per exchange/tradingClass combination.
        """
        cache_key = f"opt_chain:{symbol}:{sec_type}"
        cached = self._cache_get(cache_key)
        if cached is not None:
            return self._filter_chain_params(cached, exchange, trading_class)

        # Resolve conId if not provided
        if con_id is None:
            con_id = self._resolve_con_id(symbol, sec_type)
            if con_id is None:
                logger.warning(
                    "Could not resolve conId for %s (%s), cannot discover option chain",
                    symbol, sec_type,
                )
                return []

        logger.info("Discovering option chain for %s (%s, conId=%d)", symbol, sec_type, con_id)

        try:
            raw_chains = self._ib.reqSecDefOptParams(symbol, "", sec_type, con_id)
        except Exception as exc:
            logger.error("reqSecDefOptParams failed for %s: %s", symbol, exc)
            return []

        results: List[OptionChainParams] = []
        for chain in raw_chains:
            params = OptionChainParams(
                exchange=chain.exchange,
                underlying_con_id=chain.underlyingConId,
                trading_class=chain.tradingClass,
                multiplier=chain.multiplier,
                expirations=frozenset(chain.expirations),
                strikes=frozenset(chain.strikes),
            )
            results.append(params)

        logger.info(
            "Discovered %d chain(s) for %s: %s",
            len(results),
            symbol,
            ", ".join(f"{r.exchange}/{r.trading_class}" for r in results),
        )

        self._cache_set(cache_key, results, self._option_chain_ttl)
        return self._filter_chain_params(results, exchange, trading_class)

    @staticmethod
    def _filter_chain_params(
        chains: List[OptionChainParams],
        exchange: Optional[str],
        trading_class: Optional[str],
    ) -> List[OptionChainParams]:
        result = chains
        if exchange:
            result = [c for c in result if c.exchange == exchange]
        if trading_class:
            result = [c for c in result if c.trading_class == trading_class]
        return result

    # ── Futures chain discovery ────────────────────────────────────────

    def discover_futures_chain(
        self,
        symbol: str,
        exchange: str = "",
        currency: str = "USD",
        include_expired: bool = False,
    ) -> List[FuturesContract]:
        """Discover available futures contracts for a product.

        Uses ``reqContractDetails`` with ``secType=FUT`` to enumerate
        all active expirations.

        Parameters
        ----------
        symbol : str
            Futures product symbol (e.g. "ES", "VX", "GC", "CL").
        exchange : str
            Exchange (e.g. "CME", "CFE", "COMEX", "NYMEX").
        currency : str
            Currency code.
        include_expired : bool
            Include expired contracts (for historical analysis).

        Returns
        -------
        list[FuturesContract]
            Sorted by last trade date (nearest first).
        """
        cache_key = f"fut_chain:{symbol}:{exchange}"
        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached

        from prometheus.execution.ib_compat import Contract as IbContract

        logger.info("Discovering futures chain for %s on %s", symbol, exchange or "any")

        contract = IbContract()
        contract.symbol = symbol
        contract.secType = "FUT"
        contract.currency = currency
        if exchange:
            contract.exchange = exchange
        contract.includeExpired = include_expired

        try:
            details_list = self._ib.reqContractDetails(contract)
        except Exception as exc:
            logger.error("reqContractDetails failed for %s FUT: %s", symbol, exc)
            return []

        results: List[FuturesContract] = []
        for details in details_list:
            c = details.contract
            fc = FuturesContract(
                con_id=c.conId,
                symbol=c.symbol,
                local_symbol=c.localSymbol,
                exchange=c.exchange,
                currency=c.currency,
                last_trade_date=c.lastTradeDateOrContractMonth or "",
                multiplier=c.multiplier or "",
                trading_class=c.tradingClass or "",
            )
            results.append(fc)

        results.sort(key=lambda f: f.last_trade_date)

        logger.info(
            "Discovered %d futures contract(s) for %s: %s",
            len(results),
            symbol,
            ", ".join(f.local_symbol for f in results[:6]),
        )

        self._cache_set(cache_key, results, self._futures_chain_ttl)
        return results

    def get_front_month_future(
        self,
        symbol: str,
        exchange: str = "",
        min_dte: int = 3,
    ) -> Optional[FuturesContract]:
        """Return the nearest active futures contract with at least ``min_dte`` days.

        Parameters
        ----------
        symbol : str
            Product symbol (e.g. "ES").
        exchange : str
            Exchange (e.g. "CME").
        min_dte : int
            Minimum days to expiry to consider (avoids roll-day contracts).

        Returns
        -------
        FuturesContract or None
        """
        chain = self.discover_futures_chain(symbol, exchange)
        for fc in chain:
            if fc.dte >= min_dte:
                return fc
        return None

    # ── Futures-option chain discovery ─────────────────────────────────

    def discover_fop_chain(
        self,
        symbol: str,
        exchange: str = "",
        fut_con_id: Optional[int] = None,
    ) -> List[OptionChainParams]:
        """Discover option chains on futures (FOP).

        Parameters
        ----------
        symbol : str
            Futures product symbol (e.g. "ES", "VX").
        exchange : str
            Exchange of the underlying future.
        fut_con_id : int, optional
            Contract ID of the specific future.  If not provided, uses
            the front-month future.

        Returns
        -------
        list[OptionChainParams]
        """
        if fut_con_id is None:
            front = self.get_front_month_future(symbol, exchange)
            if front is None:
                logger.warning("No front-month future found for %s, cannot discover FOP chain", symbol)
                return []
            fut_con_id = front.con_id

        cache_key = f"fop_chain:{symbol}:{fut_con_id}"
        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached

        logger.info("Discovering FOP chain for %s (futConId=%d)", symbol, fut_con_id)

        try:
            raw_chains = self._ib.reqSecDefOptParams(symbol, "", "FUT", fut_con_id)
        except Exception as exc:
            logger.error("reqSecDefOptParams failed for %s FOP: %s", symbol, exc)
            return []

        results: List[OptionChainParams] = []
        for chain in raw_chains:
            params = OptionChainParams(
                exchange=chain.exchange,
                underlying_con_id=chain.underlyingConId,
                trading_class=chain.tradingClass,
                multiplier=chain.multiplier,
                expirations=frozenset(chain.expirations),
                strikes=frozenset(chain.strikes),
            )
            results.append(params)

        logger.info("Discovered %d FOP chain(s) for %s", len(results), symbol)

        self._cache_set(cache_key, results, self._option_chain_ttl)
        return results

    # ── Batch contract qualification ──────────────────────────────────

    def qualify_contracts(self, contracts: List[Any]) -> List[Any]:
        """Qualify a list of contracts in batches with rate limiting.

        Parameters
        ----------
        contracts : list
            IBKR Contract objects to qualify.

        Returns
        -------
        list
            Successfully qualified contracts (may be fewer than input).
        """
        if not contracts:
            return []

        qualified: List[Any] = []
        batch_size = self._qualify_batch_size

        for i in range(0, len(contracts), batch_size):
            batch = contracts[i : i + batch_size]

            try:
                result = self._ib.qualifyContracts(*batch)
                qualified.extend(result)
            except Exception as exc:
                logger.warning(
                    "qualifyContracts failed for batch %d-%d: %s",
                    i, i + len(batch), exc,
                )

            if i + batch_size < len(contracts):
                time.sleep(self._qualify_delay)

        logger.debug(
            "Qualified %d/%d contracts", len(qualified), len(contracts),
        )
        return qualified

    # ── Delta-based strike selection ──────────────────────────────────

    def get_option_by_delta(
        self,
        symbol: str,
        expiry: str,
        right: str,
        target_delta: float,
        *,
        underlying_price: Optional[float] = None,
        iv: Optional[float] = None,
        exchange: str = "SMART",
        trading_class: Optional[str] = None,
        sec_type: str = "STK",
    ) -> Optional[float]:
        """Find the strike closest to a target delta.

        Uses Black-Scholes to estimate delta for each available strike
        and returns the one closest to ``target_delta``.

        Parameters
        ----------
        symbol : str
            Underlying symbol.
        expiry : str
            Target expiration (YYYYMMDD).
        right : str
            "C" for call, "P" for put.
        target_delta : float
            Target delta magnitude.  For puts, pass a positive number
            (e.g. 0.25 for a -0.25 delta put).
        underlying_price : float, optional
            Current price.  If not provided, queries market data.
        iv : float, optional
            Implied volatility (annualised).  If not provided, uses 0.20
            as a reasonable default for US equities.
        exchange : str
            Exchange filter for the option chain.
        trading_class : str, optional
            Trading class filter.
        sec_type : str
            Security type of the underlying.

        Returns
        -------
        float or None
            The strike price closest to the target delta, or None if
            the chain could not be discovered.
        """
        # Discover chain
        chains = self.discover_option_chain(
            symbol, sec_type=sec_type,
            exchange=exchange, trading_class=trading_class,
        )
        if not chains:
            return None

        # Use the first matching chain (usually SMART)
        chain = chains[0]

        # Check expiry is available
        if expiry not in chain.expirations:
            # Find closest expiry
            sorted_exps = sorted(chain.expirations)
            if not sorted_exps:
                return None
            closest = min(sorted_exps, key=lambda e: abs(int(e) - int(expiry)))
            logger.debug(
                "Exact expiry %s not found for %s, using closest: %s",
                expiry, symbol, closest,
            )
            expiry = closest

        # Get underlying price if not provided
        if underlying_price is None:
            underlying_price = self._get_underlying_price(symbol, sec_type)
            if underlying_price is None or underlying_price <= 0:
                logger.warning("Cannot determine underlying price for %s", symbol)
                return None

        # Default IV
        sigma = iv if iv is not None else 0.20

        # Time to expiry in years
        try:
            exp_date = datetime.strptime(expiry[:8], "%Y%m%d").date()
            T = max((exp_date - date.today()).days, 1) / 365.0
        except ValueError:
            return None

        # Filter strikes to a reasonable range around the money
        candidates = chain.filter_strikes(underlying_price, width_pct=0.30)
        if not candidates:
            candidates = sorted(chain.strikes)

        # For puts, target_delta is positive but BS delta is negative
        if right.upper() == "P":
            target_bs_delta = -abs(target_delta)
        else:
            target_bs_delta = abs(target_delta)

        best_strike: Optional[float] = None
        best_diff = float("inf")

        for strike in candidates:
            d = _bs_delta(
                underlying_price, strike, T,
                self._risk_free_rate, sigma, right,
            )
            diff = abs(d - target_bs_delta)
            if diff < best_diff:
                best_diff = diff
                best_strike = strike

        if best_strike is not None:
            actual_delta = _bs_delta(
                underlying_price, best_strike, T,
                self._risk_free_rate, sigma, right,
            )
            logger.debug(
                "Delta selection for %s %s %s: target=%.2f, strike=%.1f, actual_delta=%.3f",
                symbol, expiry, right, target_delta, best_strike, actual_delta,
            )

        return best_strike

    def get_best_expiry(
        self,
        symbol: str,
        min_dte: int = 30,
        max_dte: int = 60,
        sec_type: str = "STK",
        exchange: str = "SMART",
        prefer_monthly: bool = True,
    ) -> Optional[str]:
        """Find the best expiration within a DTE range.

        Parameters
        ----------
        symbol : str
            Underlying symbol.
        min_dte : int
            Minimum days to expiry.
        max_dte : int
            Maximum days to expiry.
        sec_type : str
            Underlying security type.
        exchange : str
            Exchange filter.
        prefer_monthly : bool
            If True, prefer standard monthly expirations (3rd Friday)
            over weeklies.

        Returns
        -------
        str or None
            YYYYMMDD expiration string, or None.
        """
        chains = self.discover_option_chain(symbol, sec_type=sec_type, exchange=exchange)
        if not chains:
            return None

        chain = chains[0]
        candidates = chain.filter_expirations(min_dte=min_dte, max_dte=max_dte)

        if not candidates:
            return None

        if prefer_monthly and len(candidates) > 1:
            # Monthly expirations fall on the 3rd Friday
            monthly = []
            for exp_str in candidates:
                try:
                    exp_date = datetime.strptime(exp_str[:8], "%Y%m%d").date()
                    # 3rd Friday: day 15-21, weekday=4 (Friday)
                    if exp_date.weekday() == 4 and 15 <= exp_date.day <= 21:
                        monthly.append(exp_str)
                except ValueError:
                    continue
            if monthly:
                return monthly[0]

        return candidates[0]

    # ── Build concrete option contracts ────────────────────────────────

    def build_option_contracts(
        self,
        symbol: str,
        expiry: str,
        strikes: List[float],
        rights: List[str],
        exchange: str = "SMART",
        trading_class: Optional[str] = None,
        qualify: bool = True,
    ) -> List[Any]:
        """Build and optionally qualify a batch of option contracts.

        Parameters
        ----------
        symbol : str
            Underlying symbol.
        expiry : str
            Expiration date YYYYMMDD.
        strikes : list[float]
            Strike prices.
        rights : list[str]
            "C" and/or "P".
        exchange : str
            Exchange.
        trading_class : str, optional
            Trading class.
        qualify : bool
            If True, qualify contracts against IBKR.

        Returns
        -------
        list[Contract]
            Built (and optionally qualified) option contracts.
        """
        from prometheus.execution.ib_compat import Option

        contracts = []
        for strike in strikes:
            for right in rights:
                opt = Option(
                    symbol=symbol,
                    lastTradeDateOrContractMonth=expiry,
                    strike=strike,
                    right=right.upper(),
                    exchange=exchange,
                )
                if trading_class:
                    opt.tradingClass = trading_class
                contracts.append(opt)

        if qualify and contracts:
            contracts = self.qualify_contracts(contracts)

        return contracts

    def build_future_contract(
        self,
        symbol: str,
        expiry: str,
        exchange: str,
        currency: str = "USD",
        qualify: bool = True,
    ) -> Optional[Any]:
        """Build and optionally qualify a single futures contract.

        Returns
        -------
        Contract or None
        """
        from prometheus.execution.ib_compat import Future

        contract = Future(
            symbol=symbol,
            lastTradeDateOrContractMonth=expiry,
            exchange=exchange,
            currency=currency,
        )

        if qualify:
            qualified = self.qualify_contracts([contract])
            return qualified[0] if qualified else None

        return contract

    def build_fop_contract(
        self,
        symbol: str,
        expiry: str,
        strike: float,
        right: str,
        exchange: str,
        multiplier: str = "",
        currency: str = "USD",
        trading_class: str = "",
        qualify: bool = True,
    ) -> Optional[Any]:
        """Build and optionally qualify a futures option (FOP) contract.

        Returns
        -------
        Contract or None
        """
        from prometheus.execution.ib_compat import FuturesOption

        contract = FuturesOption(
            symbol=symbol,
            lastTradeDateOrContractMonth=expiry,
            strike=strike,
            right=right.upper(),
            exchange=exchange,
            currency=currency,
        )
        if multiplier:
            contract.multiplier = multiplier
        if trading_class:
            contract.tradingClass = trading_class

        if qualify:
            qualified = self.qualify_contracts([contract])
            return qualified[0] if qualified else None

        return contract

    # ── Internal helpers ──────────────────────────────────────────────

    def _resolve_con_id(self, symbol: str, sec_type: str) -> Optional[int]:
        """Resolve the conId for an underlying by qualifying a minimal contract."""
        from prometheus.execution.ib_compat import Index, Stock

        if sec_type == "STK":
            contract = Stock(symbol, "SMART", "USD")
        elif sec_type == "IND":
            contract = Index(symbol, "CBOE", "USD")
        elif sec_type == "FUT":
            # For futures, we need to find the front month first
            front = self.get_front_month_future(symbol)
            if front is not None:
                return front.con_id
            return None
        else:
            contract = Stock(symbol, "SMART", "USD")

        try:
            qualified = self._ib.qualifyContracts(contract)
            if qualified:
                return qualified[0].conId
        except Exception as exc:
            logger.warning("Could not resolve conId for %s (%s): %s", symbol, sec_type, exc)

        return None

    def _get_underlying_price(self, symbol: str, sec_type: str) -> Optional[float]:
        """Get the current price of an underlying via a snapshot request."""
        from prometheus.execution.ib_compat import Index, Stock

        if sec_type == "STK":
            contract = Stock(symbol, "SMART", "USD")
        elif sec_type == "IND":
            contract = Index(symbol, "CBOE", "USD")
        else:
            contract = Stock(symbol, "SMART", "USD")

        try:
            qualified = self._ib.qualifyContracts(contract)
            if not qualified:
                return None
            contract = qualified[0]

            tickers = self._ib.reqTickers(contract)
            if tickers:
                ticker = tickers[0]
                # Try last, then close, then midpoint
                price = getattr(ticker, "last", None)
                if price is None or price <= 0:
                    price = getattr(ticker, "close", None)
                if price is None or price <= 0:
                    bid = getattr(ticker, "bid", 0) or 0
                    ask = getattr(ticker, "ask", 0) or 0
                    if bid > 0 and ask > 0:
                        price = (bid + ask) / 2.0
                return float(price) if price and price > 0 else None
        except Exception as exc:
            logger.debug("Could not get price for %s: %s", symbol, exc)

        return None

    # ── Diagnostics ───────────────────────────────────────────────────

    def cache_stats(self) -> Dict[str, Any]:
        """Return cache statistics for monitoring."""
        with self._lock:
            total = len(self._cache)
            expired = sum(1 for e in self._cache.values() if e.expired)
            return {
                "total_entries": total,
                "expired_entries": expired,
                "active_entries": total - expired,
            }


# ── Module-level singleton ────────────────────────────────────────────

_global_discovery: Optional[ContractDiscoveryService] = None


def get_discovery_service(ib: Optional[Any] = None) -> ContractDiscoveryService:
    """Get or create the global ContractDiscoveryService.

    Parameters
    ----------
    ib : IB, optional
        Connected IB instance.  Required on first call.

    Returns
    -------
    ContractDiscoveryService
    """
    global _global_discovery

    if _global_discovery is None:
        if ib is None:
            raise RuntimeError(
                "ContractDiscoveryService not initialised and no IB instance provided"
            )
        _global_discovery = ContractDiscoveryService(ib)

    return _global_discovery


__all__ = [
    "ContractDiscoveryService",
    "OptionChainParams",
    "FuturesContract",
    "get_discovery_service",
]
