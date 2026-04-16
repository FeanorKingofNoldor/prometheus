"""Prometheus v2 – IBKR configuration management.

This module provides configuration management for IBKR paper and live trading,
loading credentials and connection settings from environment variables.

Configuration is loaded from environment variables:
- IBKR_LIVE_USERNAME: Live trading username
- IBKR_LIVE_PASSWORD: Live trading password
- IBKR_LIVE_ACCOUNT: Live trading account number (default: U22014992)
- IBKR_PAPER_USERNAME: Paper trading username (default: xubtmn245)
- IBKR_PAPER_PASSWORD: Paper trading password
- IBKR_PAPER_ACCOUNT: Paper trading account number (default: DUN807925)

Port configuration:
- IB Gateway: Live=4001, Paper=4002 (recommended)
- TWS: Live=7496, Paper=7497
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum
from typing import Optional

from apathis.core.logging import get_logger

from prometheus.execution.ibkr_client import IbkrConnectionConfig

logger = get_logger(__name__)


class IbkrMode(str, Enum):
    """IBKR trading mode."""

    LIVE = "LIVE"
    PAPER = "PAPER"


class IbkrGatewayType(str, Enum):
    """IBKR Gateway type."""

    GATEWAY = "GATEWAY"  # IB Gateway (recommended)
    TWS = "TWS"          # Trader Workstation


@dataclass(frozen=True)
class IbkrCredentials:
    """IBKR credentials.

    Attributes:
        username: IBKR username
        password: IBKR password (optional, loaded from env)
        account: Account number
    """

    username: str
    password: Optional[str]
    account: str


# Default port mappings
IBKR_PORTS = {
    IbkrGatewayType.GATEWAY: {
        IbkrMode.LIVE: 4001,
        IbkrMode.PAPER: 4002,
    },
    IbkrGatewayType.TWS: {
        IbkrMode.LIVE: 7496,
        IbkrMode.PAPER: 7497,
    },
}


def load_credentials(mode: IbkrMode) -> IbkrCredentials:
    """Load IBKR credentials from environment variables.

    Args:
        mode: Trading mode (LIVE or PAPER)

    Returns:
        IbkrCredentials with username, password, and account.

    Raises:
        ValueError: If required credentials are missing.
    """
    prefix = f"IBKR_{mode.value}_"

    username = os.getenv(f"{prefix}USERNAME")
    password = os.getenv(f"{prefix}PASSWORD")
    account = os.getenv(f"{prefix}ACCOUNT")

    # No hardcoded defaults — credentials MUST come from environment variables.

    if not username:
        raise ValueError(
            f"IBKR {mode.value} username not configured. "
            f"Set {prefix}USERNAME environment variable."
        )

    if not account:
        raise ValueError(
            f"IBKR {mode.value} account not configured. "
            f"Set {prefix}ACCOUNT environment variable."
        )

    # Password is optional - will be required by IB Gateway login screen
    # but not needed for API connection if already logged in
    if not password:
        logger.warning(
            "IBKR %s password not set in environment (%sPASSWORD). "
            "Ensure IB Gateway is already logged in.",
            mode.value,
            prefix,
        )

    return IbkrCredentials(
        username=username,
        password=password,
        account=account,
    )


def create_connection_config(
    mode: IbkrMode,
    gateway_type: IbkrGatewayType = IbkrGatewayType.GATEWAY,
    *,
    host: str = "127.0.0.1",
    client_id: int = 1,
    readonly: bool = False,
) -> IbkrConnectionConfig:
    """Create IBKR connection configuration.

    Args:
        mode: Trading mode (LIVE or PAPER)
        gateway_type: Gateway type (GATEWAY or TWS), defaults to GATEWAY
        host: IBKR host, defaults to localhost
        client_id: API client ID, defaults to 1
        readonly: Whether to enable readonly mode

    Returns:
        IbkrConnectionConfig with appropriate settings for the mode.
    """
    credentials = load_credentials(mode)
    port = IBKR_PORTS[gateway_type][mode]

    logger.info(
        "Creating IBKR connection config: mode=%s, gateway=%s, account=%s, port=%d",
        mode.value,
        gateway_type.value,
        credentials.account,
        port,
    )

    return IbkrConnectionConfig(
        host=host,
        port=port,
        client_id=client_id,
        account_id=credentials.account,
        connect_timeout_sec=60,
        readonly=readonly,
    )


def create_live_config(
    gateway_type: IbkrGatewayType = IbkrGatewayType.GATEWAY,
    **kwargs,
) -> IbkrConnectionConfig:
    """Create IBKR connection configuration for LIVE trading.

    Loads credentials from environment:
    - IBKR_LIVE_USERNAME (default: maximilianhuethmayr)
    - IBKR_LIVE_PASSWORD
    - IBKR_LIVE_ACCOUNT (default: U22014992)

    Args:
        gateway_type: Gateway type (GATEWAY or TWS), defaults to GATEWAY
        **kwargs: Additional arguments passed to create_connection_config

    Returns:
        IbkrConnectionConfig for live trading.
    """
    return create_connection_config(
        mode=IbkrMode.LIVE,
        gateway_type=gateway_type,
        **kwargs,
    )


def create_paper_config(
    gateway_type: IbkrGatewayType = IbkrGatewayType.GATEWAY,
    **kwargs,
) -> IbkrConnectionConfig:
    """Create IBKR connection configuration for PAPER trading.

    Loads credentials from environment:
    - IBKR_PAPER_USERNAME (default: xubtmn245)
    - IBKR_PAPER_PASSWORD
    - IBKR_PAPER_ACCOUNT (default: DUN807925)

    Args:
        gateway_type: Gateway type (GATEWAY or TWS), defaults to GATEWAY
        **kwargs: Additional arguments passed to create_connection_config

    Returns:
        IbkrConnectionConfig for paper trading.
    """
    return create_connection_config(
        mode=IbkrMode.PAPER,
        gateway_type=gateway_type,
        **kwargs,
    )


def validate_credentials_at_startup(*, require_paper: bool = True, require_live: bool = False) -> None:
    """Verify IBKR credentials are present in the environment at boot.

    Daemon failures of the form "couldn't trade because creds missing" are
    silent at 3am — operators only learn about them when the morning report
    is empty. Calling this from the daemon entrypoint surfaces the problem
    immediately, before the first market cycle.

    Args:
        require_paper: Fail boot if PAPER credentials are missing.
        require_live: Fail boot if LIVE credentials are missing. Default
            False because most deployments want paper only by default.

    Raises:
        ValueError: if any required credential is missing. Message lists
            *every* missing variable in one shot, so operators don't have
            to fix-redeploy-fix-redeploy one var at a time.
    """
    missing: list[str] = []
    if require_paper:
        for v in ("IBKR_PAPER_USERNAME", "IBKR_PAPER_ACCOUNT"):
            if not os.getenv(v):
                missing.append(v)
    if require_live:
        for v in ("IBKR_LIVE_USERNAME", "IBKR_LIVE_ACCOUNT"):
            if not os.getenv(v):
                missing.append(v)

    if missing:
        raise ValueError(
            "IBKR credential preflight failed; the following env vars are "
            f"required but unset: {', '.join(missing)}. Set them in the "
            "systemd unit's EnvironmentFile (/etc/sysconfig/prometheus-daemon) "
            "and restart."
        )

    # Soft warnings (do not fail boot) — passwords missing, or live mode
    # not yet provisioned.
    if require_paper and not os.getenv("IBKR_PAPER_PASSWORD"):
        logger.warning(
            "IBKR_PAPER_PASSWORD not set — IB Gateway must already be "
            "logged in or the connection will hang on next paper trade.",
        )
    if require_live and not os.getenv("IBKR_LIVE_PASSWORD"):
        logger.warning(
            "IBKR_LIVE_PASSWORD not set — IB Gateway must already be "
            "logged in or the connection will hang on next live trade.",
        )


__all__ = [
    "IbkrMode",
    "IbkrGatewayType",
    "IbkrCredentials",
    "load_credentials",
    "create_connection_config",
    "create_live_config",
    "create_paper_config",
    "validate_credentials_at_startup",
]
