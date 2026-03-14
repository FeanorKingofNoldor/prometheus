"""Prometheus v2 – PaperBroker stub implementation.

This module defines :class:`PaperBroker`, a thin subclass of
:class:`LiveBroker` intended for PAPER trading against a broker's paper
trading environment (e.g. IBKR paper).

It shares the same interface and (for now) the same stub behaviour as
LiveBroker: all broker-facing methods raise ``NotImplementedError``.

In later passes this class can either:

* Provide paper-specific defaults (e.g. different host/port), or
* Wrap a dedicated paper-trading adapter while reusing shared logic from
  :class:`LiveBroker`.
"""

from __future__ import annotations

from dataclasses import dataclass

from prometheus.execution.live_broker import LiveBroker


@dataclass
class PaperBroker(LiveBroker):
    """Stub BrokerInterface implementation for PAPER trading.

    Inherits all behaviour from :class:`LiveBroker`. The primary
    distinction is semantic (PAPER vs LIVE); real adapters can specialise
    this class in future iterations.
    """

    # No additional behaviour for now; this exists for clarity and future
    # extension.
    pass
