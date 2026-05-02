"""Prometheus v2 – Market Data Service.

Abstract interface and IBKR implementation for:
- Streaming real-time ticks (bid/ask/last/volume + generic ticks)
- Historical OHLCV bars and derived data (vol, IV)
- Scanner subscriptions (put/call ratio, unusual options volume)

The IBKR implementation shares the ``ib_insync.IB`` instance with
:class:`IbkrClientImpl` so market data and order execution use a single
connection.

IBKR limits:
- 100 simultaneous streaming lines (``reqMktData``)
- 60 historical data requests per 10 minutes (``reqHistoricalData``)
"""

from __future__ import annotations

import threading
import time
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from apatheon.core.logging import get_logger

logger = get_logger(__name__)


# ── Data classes ─────────────────────────────────────────────────────

class TickField(str, Enum):
    """Named tick fields from IBKR generic tick types."""
    LAST = "LAST"
    BID = "BID"
    ASK = "ASK"
    VOLUME = "VOLUME"
    CLOSE = "CLOSE"
    # Generic ticks (100-108, 236)
    OPT_VOLUME_CALL = "OPT_VOLUME_CALL"      # 100
    OPT_VOLUME_PUT = "OPT_VOLUME_PUT"        # 100
    OPT_OI_CALL = "OPT_OI_CALL"              # 101
    OPT_OI_PUT = "OPT_OI_PUT"                # 101
    HIST_VOL_30D = "HIST_VOL_30D"            # 104
    AVG_IV_CALL = "AVG_IV_CALL"              # 105
    AVG_IV_PUT = "AVG_IV_PUT"                # 106
    OPT_OI_CALL_TOTAL = "OPT_OI_CALL_TOTAL"  # 107
    OPT_OI_PUT_TOTAL = "OPT_OI_PUT_TOTAL"    # 108
    SHORTABLE_SHARES = "SHORTABLE_SHARES"    # 236


@dataclass
class MarketTick:
    """Single market data tick update."""
    symbol: str
    field: TickField
    value: float
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class BarData:
    """Single OHLCV bar."""
    symbol: str
    trade_date: date
    open: float
    high: float
    low: float
    close: float
    volume: float
    bar_count: int = 0
    average: float = 0.0
    data_type: str = "TRADES"  # TRADES, HISTORICAL_VOLATILITY, OPTION_IMPLIED_VOLATILITY


@dataclass
class ScannerResult:
    """Single result row from an IBKR scanner subscription."""
    rank: int
    symbol: str
    sec_type: str
    exchange: str
    value: float  # The scanned metric value
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TickSnapshot:
    """Aggregated snapshot of the latest ticks for a symbol.

    Updated incrementally as ticks arrive.
    """
    symbol: str
    last: float = 0.0
    bid: float = 0.0
    ask: float = 0.0
    volume: float = 0.0
    close: float = 0.0
    # Options data
    opt_volume_call: float = 0.0
    opt_volume_put: float = 0.0
    opt_oi_call: float = 0.0
    opt_oi_put: float = 0.0
    hist_vol_30d: float = 0.0
    avg_iv_call: float = 0.0
    avg_iv_put: float = 0.0
    shortable_shares: float = 0.0
    last_update: Optional[datetime] = None

    @property
    def put_call_ratio(self) -> float:
        """Put/call volume ratio.  Returns 0 if call volume is zero."""
        if self.opt_volume_call > 0:
            return self.opt_volume_put / self.opt_volume_call
        return 0.0

    def update(self, tick: MarketTick) -> None:
        """Apply a tick update to this snapshot."""
        _FIELD_MAP = {
            TickField.LAST: "last",
            TickField.BID: "bid",
            TickField.ASK: "ask",
            TickField.VOLUME: "volume",
            TickField.CLOSE: "close",
            TickField.OPT_VOLUME_CALL: "opt_volume_call",
            TickField.OPT_VOLUME_PUT: "opt_volume_put",
            TickField.OPT_OI_CALL: "opt_oi_call",
            TickField.OPT_OI_PUT: "opt_oi_put",
            TickField.HIST_VOL_30D: "hist_vol_30d",
            TickField.AVG_IV_CALL: "avg_iv_call",
            TickField.AVG_IV_PUT: "avg_iv_put",
            TickField.SHORTABLE_SHARES: "shortable_shares",
        }
        attr = _FIELD_MAP.get(tick.field)
        if attr is not None:
            setattr(self, attr, tick.value)
            self.last_update = tick.timestamp


# ── Subscription config ──────────────────────────────────────────────

@dataclass(frozen=True)
class TickSubscription:
    """Configuration for a streaming market data subscription."""
    symbol: str
    sec_type: str = "STK"        # STK, IND, FUT, OPT
    exchange: str = "SMART"
    currency: str = "USD"
    generic_ticks: str = ""      # Comma-separated IBKR generic tick IDs
    primary_exchange: str = ""


@dataclass(frozen=True)
class ScannerSubscription:
    """Configuration for a scanner subscription."""
    scan_code: str               # e.g. HIGH_OPT_VOLUME_PUT_CALL_RATIO
    instrument: str = "STK"
    location: str = "STK.US.MAJOR"
    above_price: float = 5.0
    market_cap_above: float = 1e9
    number_of_rows: int = 50


# ── Abstract interface ───────────────────────────────────────────────

# Callback types
TickCallback = Callable[[MarketTick], None]
ScannerCallback = Callable[[str, List[ScannerResult]], None]


class MarketDataService(ABC):
    """Abstract market data service.

    Concrete implementations provide streaming ticks, historical bars,
    and scanner subscriptions from a specific data source (IBKR, etc.).
    """

    @abstractmethod
    def subscribe_ticks(
        self,
        subscription: TickSubscription,
        callback: Optional[TickCallback] = None,
    ) -> int:
        """Start streaming ticks for a symbol.

        Returns a request ID that can be used to unsubscribe.
        """

    @abstractmethod
    def unsubscribe_ticks(self, req_id: int) -> None:
        """Stop streaming ticks for the given request ID."""

    @abstractmethod
    def request_historical_bars(
        self,
        symbol: str,
        duration: str,
        bar_size: str,
        data_type: str = "TRADES",
        sec_type: str = "STK",
        exchange: str = "SMART",
        currency: str = "USD",
        end_date: str = "",
    ) -> List[BarData]:
        """Request historical bars (blocking).

        Parameters
        ----------
        symbol : str
            e.g. "SPY"
        duration : str
            IBKR duration string, e.g. "1 Y", "6 M", "30 D"
        bar_size : str
            IBKR bar size, e.g. "1 day", "1 hour", "5 mins"
        data_type : str
            TRADES, MIDPOINT, BID, ASK, HISTORICAL_VOLATILITY,
            OPTION_IMPLIED_VOLATILITY
        end_date : str
            End datetime string.  Empty = now.

        Returns
        -------
        List[BarData]
        """

    @abstractmethod
    def subscribe_scanner(
        self,
        subscription: ScannerSubscription,
        callback: Optional[ScannerCallback] = None,
    ) -> int:
        """Start a scanner subscription.

        Returns a request ID.
        """

    @abstractmethod
    def unsubscribe_scanner(self, req_id: int) -> None:
        """Cancel a scanner subscription."""

    @abstractmethod
    def get_snapshot(self, symbol: str) -> Optional[TickSnapshot]:
        """Return the latest tick snapshot for a symbol (or None)."""

    @abstractmethod
    def get_all_snapshots(self) -> Dict[str, TickSnapshot]:
        """Return all current tick snapshots."""


# ── IBKR implementation ──────────────────────────────────────────────

class IbkrMarketDataService(MarketDataService):
    """IBKR market data service using ``ib_insync``.

    Shares the ``IB`` instance with :class:`IbkrClientImpl`.
    The caller must pass the already-connected ``IB`` object.

    Parameters
    ----------
    ib : IB
        Connected ``ib_insync.IB`` instance.
    max_streaming_lines : int
        Maximum simultaneous streaming subscriptions (IBKR limit: 100).
    hist_rate_limit : int
        Maximum historical data requests per 10-minute window.
    """

    def __init__(
        self,
        ib: Any,  # ib_insync.IB — avoid import at module level for testability
        max_streaming_lines: int = 100,
        hist_rate_limit: int = 60,
    ) -> None:
        self._ib = ib
        self._max_streaming_lines = max_streaming_lines
        self._hist_rate_limit = hist_rate_limit

        # Tick subscriptions: req_id → (subscription, contract, callback)
        self._tick_subs: Dict[int, tuple] = {}
        self._next_req_id = 5000  # Offset from order IDs

        # Snapshots: symbol → TickSnapshot
        self._snapshots: Dict[str, TickSnapshot] = {}
        self._snapshots_lock = threading.Lock()

        # Scanner subscriptions: req_id → (subscription, callback)
        self._scanner_subs: Dict[int, tuple] = {}

        # Historical rate limiter: timestamps of recent requests
        self._hist_timestamps: deque = deque()
        self._hist_lock = threading.Lock()

        # Active streaming line count
        self._active_lines: int = 0

    # ── Tick subscriptions ───────────────────────────────────────────

    def subscribe_ticks(
        self,
        subscription: TickSubscription,
        callback: Optional[TickCallback] = None,
    ) -> int:

        if self._active_lines >= self._max_streaming_lines:
            raise RuntimeError(
                f"Cannot subscribe: {self._active_lines} active lines "
                f"(max {self._max_streaming_lines})"
            )

        # Build contract
        contract = self._build_contract(subscription)

        # Qualify
        try:
            qualified = self._ib.qualifyContracts(contract)
            if qualified:
                contract = qualified[0]
        except Exception as exc:
            logger.warning("Could not qualify contract for %s: %s",
                           subscription.symbol, exc)

        # Request market data
        ticker = self._ib.reqMktData(
            contract,
            genericTickList=subscription.generic_ticks,
            snapshot=False,
            regulatorySnapshot=False,
        )

        req_id = self._next_req_id
        self._next_req_id += 1
        self._tick_subs[req_id] = (subscription, contract, ticker, callback)
        self._active_lines += 1

        # Initialise snapshot
        with self._snapshots_lock:
            if subscription.symbol not in self._snapshots:
                self._snapshots[subscription.symbol] = TickSnapshot(
                    symbol=subscription.symbol,
                )

        # Attach tick handler
        ticker.updateEvent += lambda t: self._on_tick_update(
            subscription.symbol, t, callback,
        )

        logger.info(
            "Subscribed to ticks: %s (%s/%s) [line %d/%d, ticks=%s]",
            subscription.symbol,
            subscription.sec_type,
            subscription.exchange,
            self._active_lines,
            self._max_streaming_lines,
            subscription.generic_ticks or "default",
        )

        return req_id

    def unsubscribe_ticks(self, req_id: int) -> None:
        entry = self._tick_subs.pop(req_id, None)
        if entry is None:
            logger.warning("No tick subscription found for req_id %d", req_id)
            return

        sub, contract, ticker, _ = entry
        try:
            self._ib.cancelMktData(contract)
        except Exception as exc:
            logger.warning("Error cancelling market data for %s: %s",
                           sub.symbol, exc)

        self._active_lines = max(0, self._active_lines - 1)
        logger.info("Unsubscribed from ticks: %s [%d lines active]",
                     sub.symbol, self._active_lines)

    def _on_tick_update(
        self,
        symbol: str,
        ticker: Any,
        callback: Optional[TickCallback],
    ) -> None:
        """Process a tick update from ib_insync."""
        now = datetime.now(timezone.utc)
        ticks: List[MarketTick] = []

        # Extract standard fields
        for attr, tick_field in [
            ("last", TickField.LAST),
            ("bid", TickField.BID),
            ("ask", TickField.ASK),
            ("volume", TickField.VOLUME),
            ("close", TickField.CLOSE),
        ]:
            val = getattr(ticker, attr, None)
            if val is not None and val > 0:
                ticks.append(MarketTick(symbol, tick_field, float(val), now))

        # Extract generic ticks from ticker fields
        _GENERIC_MAP = {
            "callOpenInterest": TickField.OPT_OI_CALL,
            "putOpenInterest": TickField.OPT_OI_PUT,
            "callVolume": TickField.OPT_VOLUME_CALL,
            "putVolume": TickField.OPT_VOLUME_PUT,
            "histVolatility": TickField.HIST_VOL_30D,
            "impliedVolatility": TickField.AVG_IV_CALL,
            "avOptionComputation": None,  # complex; skip for now
        }
        for attr, tick_field in _GENERIC_MAP.items():
            if tick_field is None:
                continue
            val = getattr(ticker, attr, None)
            if val is not None and isinstance(val, (int, float)) and val > 0:
                ticks.append(MarketTick(symbol, tick_field, float(val), now))

        # Update snapshot and invoke callbacks
        with self._snapshots_lock:
            snap = self._snapshots.get(symbol)
            if snap is None:
                snap = TickSnapshot(symbol=symbol)
                self._snapshots[symbol] = snap
            for tick in ticks:
                snap.update(tick)

        if callback is not None:
            for tick in ticks:
                try:
                    callback(tick)
                except Exception as exc:
                    logger.error("Tick callback error for %s: %s", symbol, exc)

    # ── Historical bars ──────────────────────────────────────────────

    def request_historical_bars(
        self,
        symbol: str,
        duration: str,
        bar_size: str,
        data_type: str = "TRADES",
        sec_type: str = "STK",
        exchange: str = "SMART",
        currency: str = "USD",
        end_date: str = "",
    ) -> List[BarData]:

        self._wait_for_hist_rate_limit()

        contract = self._build_contract_from_params(
            symbol, sec_type, exchange, currency,
        )

        # Qualify
        try:
            qualified = self._ib.qualifyContracts(contract)
            if qualified:
                contract = qualified[0]
        except Exception as exc:
            logger.warning("Could not qualify contract for %s: %s", symbol, exc)

        logger.info(
            "Requesting historical bars: %s %s %s %s (end=%s)",
            symbol, duration, bar_size, data_type, end_date or "now",
        )

        try:
            bars = self._ib.reqHistoricalData(
                contract,
                endDateTime=end_date,
                durationStr=duration,
                barSizeSetting=bar_size,
                whatToShow=data_type,
                useRTH=True,
                formatDate=1,
            )
        except Exception as exc:
            logger.error("Failed to request historical data for %s: %s",
                         symbol, exc)
            return []

        result: List[BarData] = []
        for bar in bars:
            try:
                if hasattr(bar.date, 'date'):
                    trade_date = bar.date.date()
                else:
                    trade_date = date.fromisoformat(str(bar.date)[:10])

                result.append(BarData(
                    symbol=symbol,
                    trade_date=trade_date,
                    open=float(bar.open),
                    high=float(bar.high),
                    low=float(bar.low),
                    close=float(bar.close),
                    volume=float(getattr(bar, "volume", 0) or 0),
                    bar_count=int(getattr(bar, "barCount", 0) or 0),
                    average=float(getattr(bar, "average", 0) or 0),
                    data_type=data_type,
                ))
            except Exception as exc:
                logger.warning("Failed to parse bar for %s: %s", symbol, exc)

        logger.info("Received %d bars for %s (%s)", len(result), symbol, data_type)
        return result

    def _wait_for_hist_rate_limit(self) -> None:
        """Block until a historical request slot is available.

        IBKR allows 60 requests per 10 minutes.
        """
        window_sec = 600  # 10 minutes

        with self._hist_lock:
            now = time.monotonic()

            # Purge timestamps older than the window
            while self._hist_timestamps and (now - self._hist_timestamps[0]) > window_sec:
                self._hist_timestamps.popleft()

            if len(self._hist_timestamps) >= self._hist_rate_limit:
                # Must wait until the oldest request exits the window
                wait_sec = window_sec - (now - self._hist_timestamps[0]) + 0.5
                logger.info(
                    "Historical rate limit reached (%d/%d). Waiting %.1f sec...",
                    len(self._hist_timestamps),
                    self._hist_rate_limit,
                    wait_sec,
                )
                time.sleep(wait_sec)

                # Purge again
                now = time.monotonic()
                while self._hist_timestamps and (now - self._hist_timestamps[0]) > window_sec:
                    self._hist_timestamps.popleft()

            self._hist_timestamps.append(time.monotonic())

    # ── Scanner subscriptions ────────────────────────────────────────

    def subscribe_scanner(
        self,
        subscription: ScannerSubscription,
        callback: Optional[ScannerCallback] = None,
    ) -> int:
        from prometheus.execution.ib_compat import ScannerSubscription as IbScanSub

        ib_sub = IbScanSub(
            instrument=subscription.instrument,
            locationCode=subscription.location,
            scanCode=subscription.scan_code,
            abovePrice=subscription.above_price,
            marketCapAbove=subscription.market_cap_above,
            numberOfRows=subscription.number_of_rows,
        )

        scanner_data = self._ib.reqScannerSubscription(ib_sub)

        req_id = self._next_req_id
        self._next_req_id += 1
        self._scanner_subs[req_id] = (subscription, scanner_data, callback)

        # Attach update handler
        scanner_data.updateEvent += lambda data: self._on_scanner_update(
            subscription.scan_code, data, callback,
        )

        logger.info(
            "Subscribed to scanner: %s (%d rows, location=%s)",
            subscription.scan_code,
            subscription.number_of_rows,
            subscription.location,
        )
        return req_id

    def unsubscribe_scanner(self, req_id: int) -> None:
        entry = self._scanner_subs.pop(req_id, None)
        if entry is None:
            logger.warning("No scanner subscription for req_id %d", req_id)
            return

        sub, scanner_data, _ = entry
        try:
            self._ib.cancelScannerSubscription(scanner_data)
        except Exception as exc:
            logger.warning("Error cancelling scanner %s: %s",
                           sub.scan_code, exc)

        logger.info("Unsubscribed from scanner: %s", sub.scan_code)

    def _on_scanner_update(
        self,
        scan_code: str,
        scanner_data: Any,
        callback: Optional[ScannerCallback],
    ) -> None:
        """Process a scanner data update."""
        results: List[ScannerResult] = []
        try:
            for i, item in enumerate(scanner_data):
                contract = getattr(item, "contractDetails", None)
                symbol = ""
                sec_type = ""
                exchange = ""
                if contract and hasattr(contract, "contract"):
                    c = contract.contract
                    symbol = getattr(c, "symbol", "")
                    sec_type = getattr(c, "secType", "")
                    exchange = getattr(c, "exchange", "")
                elif hasattr(item, "contract"):
                    c = item.contract
                    symbol = getattr(c, "symbol", "")
                    sec_type = getattr(c, "secType", "")
                    exchange = getattr(c, "exchange", "")

                value = float(getattr(item, "value", 0) or 0)

                results.append(ScannerResult(
                    rank=i,
                    symbol=symbol,
                    sec_type=sec_type,
                    exchange=exchange,
                    value=value,
                ))
        except Exception as exc:
            logger.error("Failed to parse scanner results for %s: %s",
                         scan_code, exc)
            return

        logger.debug("Scanner %s: %d results", scan_code, len(results))

        if callback is not None:
            try:
                callback(scan_code, results)
            except Exception as exc:
                logger.error("Scanner callback error for %s: %s",
                             scan_code, exc)

    # ── Snapshot access ──────────────────────────────────────────────

    def get_snapshot(self, symbol: str) -> Optional[TickSnapshot]:
        with self._snapshots_lock:
            return self._snapshots.get(symbol)

    def get_all_snapshots(self) -> Dict[str, TickSnapshot]:
        with self._snapshots_lock:
            return dict(self._snapshots)

    # ── Helpers ──────────────────────────────────────────────────────

    @staticmethod
    def _build_contract(sub: TickSubscription) -> Any:
        """Build an ib_insync Contract from a TickSubscription."""
        from prometheus.execution.ib_compat import Contract, Index, Stock

        if sub.sec_type == "IND":
            return Index(sub.symbol, sub.exchange, sub.currency)
        elif sub.sec_type == "STK":
            c = Stock(sub.symbol, sub.exchange, sub.currency)
            if sub.primary_exchange:
                c.primaryExchange = sub.primary_exchange
            return c
        else:
            c = Contract()
            c.symbol = sub.symbol
            c.secType = sub.sec_type
            c.exchange = sub.exchange
            c.currency = sub.currency
            return c

    @staticmethod
    def _build_contract_from_params(
        symbol: str,
        sec_type: str,
        exchange: str,
        currency: str,
    ) -> Any:
        """Build a contract from individual parameters."""
        from prometheus.execution.ib_compat import Contract, Index, Stock

        if sec_type == "IND":
            return Index(symbol, exchange, currency)
        elif sec_type == "STK":
            return Stock(symbol, exchange, currency)
        else:
            c = Contract()
            c.symbol = symbol
            c.secType = sec_type
            c.exchange = exchange
            c.currency = currency
            return c

    # ── Status ───────────────────────────────────────────────────────

    @property
    def active_streaming_lines(self) -> int:
        return self._active_lines

    @property
    def remaining_streaming_lines(self) -> int:
        return self._max_streaming_lines - self._active_lines

    def get_status(self) -> Dict[str, Any]:
        """Return service status summary."""
        return {
            "active_tick_subscriptions": len(self._tick_subs),
            "active_scanner_subscriptions": len(self._scanner_subs),
            "streaming_lines": self._active_lines,
            "max_streaming_lines": self._max_streaming_lines,
            "snapshots": len(self._snapshots),
            "hist_requests_in_window": len(self._hist_timestamps),
            "hist_rate_limit": self._hist_rate_limit,
        }


# ── Standard subscription sets ───────────────────────────────────────

# Generic ticks for options analytics:
# 100=call/put option volume, 101=call/put OI, 104=30d hist vol,
# 105=avg call IV, 106=avg put IV, 107=call OI total, 108=put OI total,
# 236=shortable shares
GENERIC_TICKS_OPTIONS = "100,101,104,105,106,107,108,236"
GENERIC_TICKS_BASIC = "236"

# Sector ETF symbols
SECTOR_ETF_SYMBOLS = [
    "XLK", "XLF", "XLV", "XLI", "XLY", "XLP", "XLE", "XLU", "XLRE", "XLC", "XLB",
]

def build_signal_feed_subscriptions() -> List[TickSubscription]:
    """Build the standard set of startup tick subscriptions.

    16 streaming lines total:
    - SPY, QQQ: core index (with options ticks)
    - VIX: fear gauge (Index type)
    - HYG: credit stress proxy
    - 11 sector ETFs: sector health signals
    """
    subs: List[TickSubscription] = []

    # Core indices with full options analytics
    subs.append(TickSubscription(
        symbol="SPY", sec_type="STK", exchange="SMART", currency="USD",
        generic_ticks=GENERIC_TICKS_OPTIONS,
    ))
    subs.append(TickSubscription(
        symbol="QQQ", sec_type="STK", exchange="SMART", currency="USD",
        generic_ticks=GENERIC_TICKS_OPTIONS,
    ))

    # VIX — Index, not STK
    subs.append(TickSubscription(
        symbol="VIX", sec_type="IND", exchange="CBOE", currency="USD",
        generic_ticks="",
    ))

    # HYG — credit stress proxy
    subs.append(TickSubscription(
        symbol="HYG", sec_type="STK", exchange="SMART", currency="USD",
        generic_ticks=GENERIC_TICKS_BASIC,
    ))

    # Sector ETFs with options analytics
    for sym in SECTOR_ETF_SYMBOLS:
        subs.append(TickSubscription(
            symbol=sym, sec_type="STK", exchange="SMART", currency="USD",
            generic_ticks=GENERIC_TICKS_OPTIONS,
        ))

    return subs


__all__ = [
    "TickField",
    "MarketTick",
    "BarData",
    "ScannerResult",
    "TickSnapshot",
    "TickSubscription",
    "ScannerSubscription",
    "TickCallback",
    "ScannerCallback",
    "MarketDataService",
    "IbkrMarketDataService",
    "GENERIC_TICKS_OPTIONS",
    "GENERIC_TICKS_BASIC",
    "SECTOR_ETF_SYMBOLS",
    "build_signal_feed_subscriptions",
]
