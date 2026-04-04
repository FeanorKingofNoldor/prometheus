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


__all__ = [
    "IbkrMode",
    "IbkrGatewayType",
    "IbkrCredentials",
    "load_credentials",
    "create_connection_config",
    "create_live_config",
    "create_paper_config",
]
