"""Futures-options contract specifications per commodity.

Single source of truth for the per-symbol fields needed to build a
qualifiable IBKR ``FuturesOption`` contract:

  * exchange       — IBKR exchange code
  * trading_class  — distinct from the futures symbol (e.g. CL → LO)
  * multiplier     — contract multiplier as a string (IBKR accepts string)

Captured 2026-06-06 via a one-off IBKR paper (:4002) probe. Each row
was verified by ``qualifyContracts`` on a real sample contract.

Add new commodities here as their COMMODITY sleeve templates land.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class FuturesOptionSpec:
    symbol: str          # futures symbol (e.g. "CL")
    exchange: str        # IBKR exchange (e.g. "NYMEX")
    trading_class: str   # IBKR tradingClass for the option (e.g. "LO")
    multiplier: str      # IBKR contract multiplier (e.g. "1000")
    currency: str = "USD"


FOP_SPECS: Mapping[str, FuturesOptionSpec] = {
    "CL": FuturesOptionSpec("CL", "NYMEX", "LO",  "1000"),
    "BZ": FuturesOptionSpec("BZ", "NYMEX", "BE",  "1000"),
    "NG": FuturesOptionSpec("NG", "NYMEX", "LNE", "10000"),
    "ZW": FuturesOptionSpec("ZW", "CBOT",  "OZW", "5000"),
    "GC": FuturesOptionSpec("GC", "COMEX", "OG",  "100"),
    "HG": FuturesOptionSpec("HG", "COMEX", "HXE", "25000"),
}


def get_fop_spec(symbol: str) -> FuturesOptionSpec | None:
    """Return the FOP spec for a commodity symbol, or None if not registered."""
    return FOP_SPECS.get(symbol.upper())


def is_commodity_fop_symbol(symbol: str) -> bool:
    """True if `symbol` is a known commodity with FOP support."""
    return symbol.upper() in FOP_SPECS


__all__ = [
    "FuturesOptionSpec",
    "FOP_SPECS",
    "get_fop_spec",
    "is_commodity_fop_symbol",
]
