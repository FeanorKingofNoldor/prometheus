"""Prometheus v2 – Instrument ID to IBKR contract mapper.

This module provides translation between Prometheus instrument_id identifiers
and Interactive Brokers contract specifications.

The mapper:
- Queries the instruments table from the database
- Caches instrument metadata in memory
- Translates instrument_id to IBKR Stock contracts
- Handles refresh when new instruments are added
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

from apathis.core.database import DatabaseManager, get_db_manager
from apathis.core.logging import get_logger

from prometheus.execution.ib_compat import (
    Bag,
    ComboLeg,
    ContFuture,
    Contract,
    Future,
    FuturesOption,
    Index,
    Option,
    Stock,
)

logger = get_logger(__name__)


@dataclass(frozen=True)
class InstrumentMetadata:
    """Metadata for a single instrument from the database.

    Attributes:
        instrument_id: Unique identifier (e.g. "AAPL.US", "SPY_250620_400P.US")
        symbol: Trading symbol (e.g. "AAPL", "SPY")
        exchange: Exchange code (e.g. "US", "SMART")
        currency: Currency code (e.g. "USD")
        asset_class: Asset class (e.g. "EQUITY", "OPTION", "INDEX", "FUTURE")
        expiry: Option/future expiration date (YYYYMMDD string or None).
        strike: Option strike price (or None).
        right: Option right — "C" (call) or "P" (put) (or None).
        multiplier: Contract multiplier (e.g. "100" for US equity options).
    """

    instrument_id: str
    symbol: str
    exchange: str
    currency: str
    asset_class: str
    expiry: Optional[str] = None
    strike: Optional[float] = None
    right: Optional[str] = None
    multiplier: Optional[str] = None


class InstrumentMapper:
    """Maps Prometheus instrument_id to IBKR contracts.

    This class maintains an in-memory cache of instrument metadata loaded
    from the database and provides translation to IBKR contract objects.

    Usage:
        mapper = InstrumentMapper()
        mapper.load_instruments()  # Load from database

        contract = mapper.get_contract("AAPL.US")
        # Returns Stock("AAPL", "SMART", "USD")
    """

    def __init__(self, db_manager: Optional[DatabaseManager] = None) -> None:
        """Initialize the mapper.

        Args:
            db_manager: Database manager instance. If None, uses default.
        """
        self._db = db_manager or get_db_manager()
        self._instruments: Dict[str, InstrumentMetadata] = {}
        self._loaded = False

    def load_instruments(self, force_reload: bool = False) -> None:
        """Load instrument metadata from the database.

        Args:
            force_reload: If True, reload even if already loaded.
        """
        if self._loaded and not force_reload:
            logger.debug("Instruments already loaded, skipping")
            return

        logger.info("Loading instruments from database")

        sql = """
            SELECT
                instrument_id,
                symbol,
                exchange,
                currency,
                asset_class
            FROM instruments
            WHERE status = 'ACTIVE'
              AND instrument_id NOT LIKE 'SYNTH_%%'
        """

        with self._db.get_runtime_connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(sql)
                rows = cur.fetchall()

                self._instruments.clear()

                for row in rows:
                    instrument_id, symbol, exchange, currency, asset_class = row

                    metadata = InstrumentMetadata(
                        instrument_id=instrument_id,
                        symbol=symbol,
                        exchange=exchange,
                        currency=currency,
                        asset_class=asset_class,
                    )

                    self._instruments[instrument_id] = metadata

                self._loaded = True
                logger.info("Loaded %d instruments", len(self._instruments))

            finally:
                cur.close()

    def get_metadata(self, instrument_id: str) -> Optional[InstrumentMetadata]:
        """Get instrument metadata for a given instrument_id.

        Args:
            instrument_id: The instrument identifier (e.g. "AAPL.US")

        Returns:
            InstrumentMetadata if found, None otherwise.
        """
        if not self._loaded:
            self.load_instruments()

        return self._instruments.get(instrument_id)

    def get_contract(self, instrument_id: str) -> Contract:
        """Translate Prometheus instrument_id to IBKR Contract.

        Args:
            instrument_id: The instrument identifier
                (e.g. "AAPL.US", "SPY_250620_400P.US")

        Returns:
            IBKR Contract object.

        Raises:
            ValueError: If instrument not found or asset class not supported.
        """
        metadata = self.get_metadata(instrument_id)

        if metadata is None:
            # Fallback: try to parse instrument_id directly
            logger.warning(
                "Instrument %s not found in database, attempting direct parsing",
                instrument_id,
            )
            return self._parse_instrument_id_fallback(instrument_id)

        return self._metadata_to_contract(metadata)

    def _metadata_to_contract(self, metadata: InstrumentMetadata) -> Contract:
        """Build an IBKR Contract from InstrumentMetadata."""
        # Normalize common US-equity/ETF exchange labels to SMART routing.
        # (IBKR rejects some directed labels like "NYSE_ARCA" in this path.)
        us_smart_aliases = {"US", "NYSE_ARCA", "ARCA", "NASDAQ", "NYSE", "BATS", "IEX"}
        exchange = "SMART" if metadata.exchange in us_smart_aliases else metadata.exchange

        if metadata.asset_class in ("EQUITY", "ETF"):
            contract = Stock(
                symbol=metadata.symbol,
                exchange=exchange,
                currency=metadata.currency,
            )
            logger.debug(
                "Mapped %s (%s) -> Stock(%s, %s, %s)",
                metadata.instrument_id, metadata.symbol,
                metadata.asset_class,
                exchange, metadata.currency,
            )
            return contract

        if metadata.asset_class == "OPTION":
            if not all([metadata.expiry, metadata.strike, metadata.right]):
                raise ValueError(
                    f"Option contract {metadata.instrument_id} missing "
                    f"expiry/strike/right"
                )
            contract = Option(
                symbol=metadata.symbol,
                lastTradeDateOrContractMonth=metadata.expiry,
                strike=metadata.strike,
                right=metadata.right,
                exchange=exchange,
                currency=metadata.currency,
            )
            if metadata.multiplier:
                contract.multiplier = metadata.multiplier
            logger.debug(
                "Mapped %s -> Option(%s, %s, %.1f, %s)",
                metadata.instrument_id, metadata.symbol,
                metadata.expiry, metadata.strike, metadata.right,
            )
            return contract

        if metadata.asset_class == "INDEX":
            contract = Index(
                symbol=metadata.symbol,
                exchange=exchange,
                currency=metadata.currency,
            )
            logger.debug(
                "Mapped %s -> Index(%s, %s)",
                metadata.instrument_id, metadata.symbol, exchange,
            )
            return contract

        if metadata.asset_class == "FUTURE":
            if not metadata.expiry:
                raise ValueError(
                    f"Future contract {metadata.instrument_id} missing expiry"
                )
            contract = Future(
                symbol=metadata.symbol,
                lastTradeDateOrContractMonth=metadata.expiry,
                exchange=exchange,
                currency=metadata.currency,
            )
            if metadata.multiplier:
                contract.multiplier = metadata.multiplier
            logger.debug(
                "Mapped %s -> Future(%s, %s, %s)",
                metadata.instrument_id, metadata.symbol,
                metadata.expiry, exchange,
            )
            return contract

        if metadata.asset_class == "FUTURES_OPTION":
            if not all([metadata.expiry, metadata.strike, metadata.right]):
                raise ValueError(
                    f"Futures option contract {metadata.instrument_id} missing "
                    f"expiry/strike/right"
                )
            contract = FuturesOption(
                symbol=metadata.symbol,
                lastTradeDateOrContractMonth=metadata.expiry,
                strike=metadata.strike,
                right=metadata.right,
                exchange=exchange,
                currency=metadata.currency,
            )
            if metadata.multiplier:
                contract.multiplier = metadata.multiplier
            logger.debug(
                "Mapped %s -> FuturesOption(%s, %s, %.1f, %s)",
                metadata.instrument_id, metadata.symbol,
                metadata.expiry, metadata.strike, metadata.right,
            )
            return contract

        raise ValueError(
            f"Asset class {metadata.asset_class!r} not supported for "
            f"IBKR mapping ({metadata.instrument_id})"
        )

    def _parse_instrument_id_fallback(self, instrument_id: str) -> Contract:
        """Fallback parser when instrument not found in database.

        Assumes format is "SYMBOL.EXCHANGE" (e.g. "AAPL.US")
        """
        parts = instrument_id.split(".")

        if len(parts) >= 2:
            symbol = parts[0].upper()
            exchange_hint = parts[1].upper()

            # Map exchange hint to IBKR exchange
            if exchange_hint == "US":
                exchange = "SMART"
                currency = "USD"
            else:
                exchange = exchange_hint
                currency = "USD"  # Assume USD for now

            logger.info(
                "Fallback parsing: %s -> Stock(%s, %s, %s)",
                instrument_id,
                symbol,
                exchange,
                currency,
            )

            return Stock(symbol, exchange, currency)
        else:
            # Last resort: assume it's just a symbol
            logger.warning(
                "Could not parse instrument_id %s, treating as US equity symbol",
                instrument_id,
            )
            return Stock(instrument_id.upper(), "SMART", "USD")

    def refresh(self) -> None:
        """Reload instruments from database."""
        self.load_instruments(force_reload=True)

    def get_instrument_count(self) -> int:
        """Return the number of loaded instruments."""
        return len(self._instruments)

    # ------------------------------------------------------------------
    # Convenience builders (no DB lookup needed)
    # ------------------------------------------------------------------

    @staticmethod
    def build_option_contract(
        symbol: str,
        expiry: str,
        strike: float,
        right: str,
        exchange: str = "SMART",
        currency: str = "USD",
        multiplier: str = "100",
    ) -> Option:
        """Build an IBKR Option contract directly.

        Args:
            symbol: Underlying symbol (e.g. "SPY").
            expiry: Expiration date string YYYYMMDD (e.g. "20250620").
            strike: Strike price.
            right: "C" for call, "P" for put.
            exchange: IBKR exchange (default SMART).
            currency: Currency code.
            multiplier: Contract multiplier.
        """
        contract = Option(
            symbol=symbol,
            lastTradeDateOrContractMonth=expiry,
            strike=strike,
            right=right.upper(),
            exchange=exchange,
            currency=currency,
        )
        contract.multiplier = multiplier
        return contract

    @staticmethod
    def build_index_contract(
        symbol: str,
        exchange: str = "CBOE",
        currency: str = "USD",
    ) -> Index:
        """Build an IBKR Index contract directly.

        Args:
            symbol: Index symbol (e.g. "VIX", "SPX").
            exchange: IBKR exchange.
            currency: Currency code.
        """
        return Index(symbol=symbol, exchange=exchange, currency=currency)

    @staticmethod
    def build_future_contract(
        symbol: str,
        expiry: str,
        exchange: str = "CFE",
        currency: str = "USD",
        multiplier: str = "",
    ) -> Future:
        """Build an IBKR Future contract directly.

        Args:
            symbol: Future symbol (e.g. "VX" for VIX futures).
            expiry: Expiration YYYYMMDD or YYYYMM.
            exchange: IBKR exchange.
            currency: Currency code.
            multiplier: Contract multiplier.
        """
        contract = Future(
            symbol=symbol,
            lastTradeDateOrContractMonth=expiry,
            exchange=exchange,
            currency=currency,
        )
        if multiplier:
            contract.multiplier = multiplier
        return contract

    # ------------------------------------------------------------------
    # Combo / spread contract builders
    # ------------------------------------------------------------------

    @staticmethod
    def build_combo_contract(
        legs: List[Dict],
        symbol: str = "",
        exchange: str = "SMART",
        currency: str = "USD",
    ) -> Bag:
        """Build a multi-leg combo (BAG) contract.

        Each leg dict must contain:
            - ``con_id``: int — the IBKR conId of the leg contract
            - ``action``: str — "BUY" or "SELL"
            - ``ratio``: int — leg ratio (usually 1)
            - ``exchange``: str — exchange for this leg (default "SMART")

        Example (bull put spread)::

            legs = [
                {"con_id": 12345, "action": "SELL", "ratio": 1},
                {"con_id": 67890, "action": "BUY",  "ratio": 1},
            ]
            combo = InstrumentMapper.build_combo_contract(legs, symbol="SPY")

        Returns
        -------
        Bag
            IBKR combo contract ready for order submission.
        """
        combo = Bag()
        combo.symbol = symbol
        combo.secType = "BAG"
        combo.exchange = exchange
        combo.currency = currency

        combo_legs = []
        for leg in legs:
            cl = ComboLeg()
            cl.conId = leg["con_id"]
            cl.ratio = leg.get("ratio", 1)
            cl.action = leg["action"]
            cl.exchange = leg.get("exchange", exchange)
            combo_legs.append(cl)

        combo.comboLegs = combo_legs
        return combo

    @staticmethod
    def build_contfut_contract(
        symbol: str,
        exchange: str,
        currency: str = "USD",
    ) -> ContFuture:
        """Build a continuous futures contract (for data queries only).

        ``CONTFUT`` contracts cannot be traded but are useful for
        requesting continuous historical data.

        Parameters
        ----------
        symbol : str
            Product symbol (e.g. "ES", "VX").
        exchange : str
            Exchange (e.g. "CME", "CFE").
        currency : str
            Currency code.
        """
        return ContFuture(
            symbol=symbol,
            exchange=exchange,
            currency=currency,
        )

    # ------------------------------------------------------------------
    # Instrument ID helpers
    # ------------------------------------------------------------------

    @staticmethod
    def option_instrument_id(
        symbol: str,
        expiry: str,
        strike: float,
        right: str,
    ) -> str:
        """Generate a human-readable instrument_id for an option.

        Format: ``{SYMBOL}_{YYMMDD}_{STRIKE}{C|P}.US``

        Examples:
            >>> InstrumentMapper.option_instrument_id("SPY", "20250620", 400.0, "P")
            'SPY_250620_400P.US'
            >>> InstrumentMapper.option_instrument_id("AAPL", "20250718", 175.5, "C")
            'AAPL_250718_175.5C.US'
        """
        # YYYYMMDD → YYMMDD
        expiry_short = expiry[2:] if len(expiry) == 8 else expiry
        # Format strike: drop trailing .0 for whole numbers
        strike_str = f"{strike:g}"
        return f"{symbol}_{expiry_short}_{strike_str}{right.upper()}.US"

    @staticmethod
    def contract_to_instrument_id(contract: Contract) -> str:
        """Convert any IBKR contract to a Prometheus instrument_id.

        Handles STK, OPT, FOP, IND, FUT.  Falls back to ``SYMBOL.US``
        for unknown types.
        """
        sec_type = getattr(contract, "secType", "STK")

        if sec_type == "OPT":
            symbol = contract.symbol
            expiry = getattr(contract, "lastTradeDateOrContractMonth", "") or ""
            strike = float(getattr(contract, "strike", 0) or 0)
            right = getattr(contract, "right", "") or ""
            return InstrumentMapper.option_instrument_id(
                symbol, expiry, strike, right,
            )

        if sec_type == "FOP":
            symbol = contract.symbol
            expiry = getattr(contract, "lastTradeDateOrContractMonth", "") or ""
            strike = float(getattr(contract, "strike", 0) or 0)
            right = getattr(contract, "right", "") or ""
            expiry_short = expiry[2:] if len(expiry) == 8 else expiry
            strike_str = f"{strike:g}"
            return f"{symbol}_{expiry_short}_{strike_str}{right.upper()}.FOP"

        if sec_type == "IND":
            getattr(contract, "exchange", "CBOE")
            return f"{contract.symbol}.INDX"

        if sec_type == "FUT":
            expiry = getattr(contract, "lastTradeDateOrContractMonth", "") or ""
            getattr(contract, "exchange", "")
            expiry_short = expiry[2:] if len(expiry) == 8 else expiry
            return f"{contract.symbol}_{expiry_short}.FUT"

        # STK and everything else
        return f"{contract.symbol}.US"


# Global singleton instance for convenience
_global_mapper: Optional[InstrumentMapper] = None


def get_instrument_mapper(db_manager: Optional[DatabaseManager] = None) -> InstrumentMapper:
    """Get the global instrument mapper instance.

    Args:
        db_manager: Optional database manager. Only used on first call.

    Returns:
        Global InstrumentMapper singleton.
    """
    global _global_mapper

    if _global_mapper is None:
        _global_mapper = InstrumentMapper(db_manager)
        _global_mapper.load_instruments()

    return _global_mapper


__all__ = [
    "InstrumentMetadata",
    "InstrumentMapper",
    "get_instrument_mapper",
]
