"""Prometheus v2 – IBKR Library Compatibility Layer.

Provides a single import point for IBKR API classes, supporting both
``ib_async`` (preferred, actively maintained) and ``ib_insync``
(legacy fallback).

**Why two libraries?**

``ib_insync`` was created by Ewald de Wit but is no longer maintained.
``ib_async`` is the community-maintained successor under the
``ib-api-reloaded`` GitHub organisation.  The API surface is identical
— only the package name changed — but ``ib_async`` receives ongoing
bug fixes (e.g. expanded warning codes, protobuf readiness).

**Usage**::

    from prometheus.execution.ib_compat import (
        IB, Stock, Option, Index, Future, Contract,
        MarketOrder, LimitOrder, StopOrder, StopLimitOrder,
    )

    ib = IB()
    ib.connect('127.0.0.1', 4001, clientId=1)

**Backend selection** (in priority order):

1. Environment variable ``PROMETHEUS_IB_BACKEND=ib_async`` or
   ``PROMETHEUS_IB_BACKEND=ib_insync`` forces a specific backend.
2. If not set, tries ``ib_async`` first, then falls back to
   ``ib_insync``.

The active backend name is available as :data:`IB_BACKEND`.
"""

from __future__ import annotations

import asyncio
import importlib
import os
from typing import Any

_BACKEND: str = ""


def _ensure_event_loop() -> None:
    """Ensure the current thread has an asyncio event loop.

    ``eventkit`` (used by both ``ib_async`` and ``ib_insync``) calls
    ``asyncio.get_event_loop()`` at import time.  In Python 3.10+ this
    raises ``RuntimeError`` in non-main threads that lack a running loop.
    We pre-create one so the import succeeds.
    """
    try:
        asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)


def _load_backend() -> str:
    """Detect and load the preferred IBKR library."""
    global _BACKEND

    # Make sure the importing thread has an event loop (needed by eventkit).
    _ensure_event_loop()

    forced = os.environ.get("PROMETHEUS_IB_BACKEND", "").strip().lower()

    if forced in ("ib_async", "ib_insync"):
        try:
            importlib.import_module(forced)
            _BACKEND = forced
            return _BACKEND
        except ImportError:
            pass  # Fall through to auto-detect

    # Auto-detect: prefer ib_async
    for candidate in ("ib_async", "ib_insync"):
        try:
            importlib.import_module(candidate)
            _BACKEND = candidate
            return _BACKEND
        except ImportError:
            continue

    raise ImportError(
        "Neither ib_async nor ib_insync is installed.  "
        "Install one with: pip install ib_async   (or)   pip install ib_insync"
    )


_load_backend()

# ── Re-export everything we use across the codebase ──────────────────
# The two libraries expose identical class names, so the import is
# the same regardless of which backend was loaded.

_mod = importlib.import_module(_BACKEND)


def _get(name: str) -> Any:
    return getattr(_mod, name)


# Core
IB = _get("IB")
Contract = _get("Contract")

# Contract types
Stock = _get("Stock")
Index = _get("Index")
Option = _get("Option")
Future = _get("Future")
ContFuture = _get("ContFuture")  # Continuous futures (for data queries)
Forex = _get("Forex")
FuturesOption = _get("FuturesOption")  # Options on futures (FOP)
Bag = _get("Bag")                      # Multi-leg combo contracts

# Combo building blocks
ComboLeg = _get("ComboLeg")
TagValue = _get("TagValue")

# Order types
MarketOrder = _get("MarketOrder")
LimitOrder = _get("LimitOrder")
StopOrder = _get("StopOrder")
StopLimitOrder = _get("StopLimitOrder")
Order = _get("Order")        # ib_insync/ib_async Order (not our broker_interface.Order)

# Execution / trade
Trade = _get("Trade")
Fill = _get("Fill")

# Scanner
ScannerSubscription = _get("ScannerSubscription")

# Conditions (for crisis orders)
OrderCondition = _get("OrderCondition")
PriceCondition = _get("PriceCondition")

# Utilities
util = _get("util")

# ── Public metadata ──────────────────────────────────────────────────
IB_BACKEND: str = _BACKEND
"""Name of the active IBKR library: ``'ib_async'`` or ``'ib_insync'``."""


__all__ = [
    "IB_BACKEND",
    "IB",
    "Contract",
    "Stock",
    "Index",
    "Option",
    "Future",
    "ContFuture",
    "Forex",
    "FuturesOption",
    "Bag",
    "ComboLeg",
    "TagValue",
    "MarketOrder",
    "LimitOrder",
    "StopOrder",
    "StopLimitOrder",
    "Order",
    "Trade",
    "Fill",
    "ScannerSubscription",
    "OrderCondition",
    "PriceCondition",
    "util",
]
