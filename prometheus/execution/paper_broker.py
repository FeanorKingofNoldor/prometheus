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

import os
from dataclasses import dataclass

from apatheon.core.logging import get_logger

from prometheus.execution.live_broker import LiveBroker

logger = get_logger(__name__)


# Live IBKR API ports. A "paper" broker must NEVER be pointed at one of
# these — that would route a paper order to the live gateway. IB Gateway
# live = 4001, TWS live = 7496.
LIVE_PORTS = frozenset({4001, 7496})


class PaperLiveMisconfiguration(RuntimeError):
    """Raised when a PaperBroker is wired to the live gateway/account.

    A hard stop: a config slip (wrong port or a live account number in the
    paper path) must fail loudly at construction rather than silently
    routing a "paper" order to the live gateway.
    """


def assert_not_live(*, port: int | None, account_id: str | None) -> None:
    """Guard the paper path against live port / live account leakage.

    Raises :class:`PaperLiveMisconfiguration` if ``port`` is a live API
    port or ``account_id`` matches the configured ``IBKR_LIVE_ACCOUNT``.
    Live IBKR account numbers begin with ``U`` (paper accounts begin with
    ``DU``), which is used as a secondary heuristic when no env var is set.
    """
    if port is not None and int(port) in LIVE_PORTS:
        raise PaperLiveMisconfiguration(
            f"PaperBroker refusing to start: port {port} is a LIVE IBKR port "
            f"{sorted(LIVE_PORTS)}. Paper trading must use a paper port (e.g. "
            "4002/7497). Refusing to route paper orders to the live gateway."
        )

    if account_id:
        live_account = os.getenv("IBKR_LIVE_ACCOUNT")
        if live_account and account_id == live_account:
            raise PaperLiveMisconfiguration(
                f"PaperBroker refusing to start: account {account_id!r} is the "
                "configured LIVE account (IBKR_LIVE_ACCOUNT). Paper trading must "
                "use the paper account (IBKR_PAPER_ACCOUNT)."
            )
        # Live IBKR accounts are 'U…'; paper accounts are 'DU…'. A bare 'U'
        # account in the paper path is almost certainly a live-account slip.
        if account_id.startswith("U") and not account_id.startswith("DU"):
            raise PaperLiveMisconfiguration(
                f"PaperBroker refusing to start: account {account_id!r} looks "
                "like a LIVE account (live accounts start with 'U', paper with "
                "'DU'). Refusing to route paper orders against a live account."
            )


@dataclass
class PaperBroker(LiveBroker):
    """Stub BrokerInterface implementation for PAPER trading.

    Inherits all behaviour from :class:`LiveBroker`. The primary
    distinction is semantic (PAPER vs LIVE). On construction it asserts it
    is not wired to the live gateway/account as a defense-in-depth guard:
    the only thing otherwise separating paper from live is the port number.
    """

    def __post_init__(self) -> None:
        port = None
        client = getattr(self, "client", None)
        if client is not None:
            cfg = getattr(client, "config", None) or getattr(client, "_config", None)
            port = getattr(cfg, "port", None)
        assert_not_live(port=port, account_id=self.account_id)
