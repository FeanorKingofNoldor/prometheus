"""Liquidity filter for option chains.

``ContractDiscoveryService`` returns every strike IBKR lists on an
underlying with zero quality filter. For SPY most strikes are tradeable;
for sector ETFs in stress most are not. Selecting from the raw chain
means we routinely pick strikes with no bid or wide markets and get
either no fill or terrible execution.

This filter takes a list of candidate contracts and an IBKR connection,
issues a batched snapshot ``reqMktData`` per contract, and rejects:

* No-bid strikes (bid ≤ ``min_bid``).
* Wide markets (``(ask − bid) / mid > max_spread_pct``).
* Stale snapshots (no quote populated within the wait window).

Returns the surviving contracts with their quotes, plus a structured
record of every rejection (reason + observed values) so the audit log
can show *why* a candidate was passed over.
"""

from __future__ import annotations

import time
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, Protocol

from apatheon.core.logging import get_logger

logger = get_logger(__name__)


REJECT_NO_BID = "no_bid"
REJECT_WIDE_SPREAD = "wide_spread"
REJECT_NO_QUOTE = "no_quote"
REJECT_INVALID = "invalid_contract"


@dataclass(frozen=True)
class LiquidityQuote:
    bid: float
    ask: float
    last: float
    mid: float
    spread_pct: float          # (ask - bid) / mid, 0 when mid == 0
    fetched_at: float


@dataclass(frozen=True)
class LiquidityRejection:
    reason: str
    quote: LiquidityQuote | None


@dataclass(frozen=True)
class LiquidityFilterResult:
    accepted: list[tuple[Any, LiquidityQuote]]
    rejected: list[tuple[Any, LiquidityRejection]]

    @property
    def accepted_count(self) -> int:
        return len(self.accepted)

    @property
    def rejected_count(self) -> int:
        return len(self.rejected)

    def reasons(self) -> dict[str, int]:
        out: dict[str, int] = {}
        for _, rej in self.rejected:
            out[rej.reason] = out.get(rej.reason, 0) + 1
        return out


class LiquidityLike(Protocol):
    """Structural type satisfied by both the live ``LiquidityFilter``
    and the backtest harness's ``BacktestLiquidityFilter``. Consumers
    in ``selection`` / ``runner`` accept this protocol so either
    source works without inheritance."""

    def filter(self, contracts: Iterable[Any]) -> LiquidityFilterResult: ...


def _contract_key(contract: Any) -> str:
    symbol = getattr(contract, "symbol", "?")
    expiry = getattr(contract, "lastTradeDateOrContractMonth", "")
    strike = getattr(contract, "strike", 0)
    right = getattr(contract, "right", "")
    return f"{symbol}:{expiry}:{strike}:{right}"


class LiquidityFilter:
    """Filter a chain to liquid, tradeable strikes.

    Parameters
    ----------
    ib
        Connected ``ib_async.IB`` (or compatible). ``None`` makes
        ``filter()`` reject everything as ``no_quote`` — useful for
        backtest-mode wiring without disabling the call site.
    min_bid
        Strikes with bid ≤ this are rejected (default $0.05).
    max_spread_pct
        Strikes with relative spread above this are rejected (default
        30%, i.e. (ask-bid)/mid ≤ 0.30).
    snapshot_wait_sec
        Time to wait for the batched snapshot to populate.
    """

    def __init__(
        self,
        ib: Any = None,
        *,
        min_bid: float = 0.05,
        max_spread_pct: float = 0.30,
        snapshot_wait_sec: float = 2.0,
    ) -> None:
        self._ib = ib
        self._min_bid = min_bid
        self._max_spread_pct = max_spread_pct
        self._snapshot_wait = snapshot_wait_sec

    def filter(self, contracts: Iterable[Any]) -> LiquidityFilterResult:
        contracts = list(contracts)
        accepted: list[tuple[Any, LiquidityQuote]] = []
        rejected: list[tuple[Any, LiquidityRejection]] = []

        if not contracts:
            return LiquidityFilterResult(accepted, rejected)

        if self._ib is None:
            for c in contracts:
                rejected.append((c, LiquidityRejection(REJECT_NO_QUOTE, None)))
            return LiquidityFilterResult(accepted, rejected)

        quotes = self._fetch_quotes(contracts)

        for c in contracts:
            key = _contract_key(c)
            q = quotes.get(key)
            if q is None:
                rejected.append((c, LiquidityRejection(REJECT_NO_QUOTE, None)))
                continue
            if q.bid <= self._min_bid:
                rejected.append((c, LiquidityRejection(REJECT_NO_BID, q)))
                continue
            if q.spread_pct > self._max_spread_pct:
                rejected.append((c, LiquidityRejection(REJECT_WIDE_SPREAD, q)))
                continue
            accepted.append((c, q))

        return LiquidityFilterResult(accepted, rejected)

    def _fetch_quotes(self, contracts: list[Any]) -> dict[str, LiquidityQuote]:
        tickers: list[tuple[Any, Any, str]] = []

        for c in contracts:
            key = _contract_key(c)
            try:
                try:
                    ticker = self._ib.reqMktData(c, snapshot=True)
                except TypeError:
                    ticker = self._ib.reqMktData(c)
            except Exception as exc:
                logger.debug("reqMktData failed for %s: %s", key, exc)
                continue
            tickers.append((c, ticker, key))

        if not tickers:
            return {}

        try:
            self._ib.sleep(self._snapshot_wait)
        except Exception:
            time.sleep(self._snapshot_wait)

        out: dict[str, LiquidityQuote] = {}
        now = time.monotonic()

        for c, ticker, key in tickers:
            try:
                bid = float(getattr(ticker, "bid", 0) or 0)
                ask = float(getattr(ticker, "ask", 0) or 0)
                last = float(getattr(ticker, "last", 0) or 0)
                # IBKR's "no quote" sentinels are -1 — coerce to 0 so
                # downstream comparisons treat them uniformly.
                if bid < 0:
                    bid = 0.0
                if ask < 0:
                    ask = 0.0
                if last < 0:
                    last = 0.0
                mid = (bid + ask) / 2.0 if bid > 0 and ask > 0 else last
                spread_pct = ((ask - bid) / mid) if mid > 0 and ask > bid else 0.0
                if bid == 0 and ask == 0 and last == 0:
                    continue
                out[key] = LiquidityQuote(
                    bid=bid, ask=ask, last=last, mid=mid,
                    spread_pct=spread_pct, fetched_at=now,
                )
            finally:
                try:
                    self._ib.cancelMktData(c)
                except Exception:
                    pass

        return out


__all__ = [
    "LiquidityFilter",
    "LiquidityLike",
    "LiquidityFilterResult",
    "LiquidityQuote",
    "LiquidityRejection",
    "REJECT_NO_BID",
    "REJECT_WIDE_SPREAD",
    "REJECT_NO_QUOTE",
    "REJECT_INVALID",
]
