"""Prometheus v2 – Broker factory for creating configured brokers.

This module provides factory functions for creating LiveBroker and PaperBroker
instances with properly configured IBKR clients.

Usage:
    from prometheus.execution.broker_factory import create_paper_broker
    
    broker = create_paper_broker()
    broker.client.connect()
    
    order = Order(...)
    broker.submit_order(order)
"""

from __future__ import annotations

from typing import Optional

from apathis.core.config import get_config
from apathis.core.database import DatabaseManager
from apathis.core.logging import get_logger
from prometheus.execution.connection_manager import (
    DualConnectionConfig,
    DualConnectionManager,
    create_dual_config_from_mode,
)
from prometheus.execution.ibkr_config import (
    IbkrGatewayType,
    IbkrMode,
    create_connection_config,
    create_live_config,
    create_paper_config,
    load_credentials,
)
from prometheus.execution.live_broker import LiveBroker
from prometheus.execution.paper_broker import PaperBroker
from prometheus.execution.risk_broker import RiskCheckingBroker


logger = get_logger(__name__)


def create_live_broker(
    *,
    gateway_type: IbkrGatewayType = IbkrGatewayType.GATEWAY,
    client_id: int = 1,
    readonly: bool = False,
    db_manager: Optional[DatabaseManager] = None,
    mapper: Optional[InstrumentMapper] = None,
    auto_connect: bool = False,
    dual_connection: bool = True,
) -> LiveBroker:
    """Create a LiveBroker instance for LIVE trading.
    
    This creates a LiveBroker with IbkrClientImpl configured for live trading,
    loading credentials from environment variables:
    - IBKR_LIVE_USERNAME (default: maximilianhuethmayr)
    - IBKR_LIVE_PASSWORD
    - IBKR_LIVE_ACCOUNT (default: U22014992)
    
    Args:
        gateway_type: Gateway type (GATEWAY or TWS), defaults to GATEWAY
        client_id: API client ID, defaults to 1
        readonly: Whether to enable readonly mode (no order submission)
        db_manager: Optional database manager for instrument mapper
        mapper: Optional custom instrument mapper
        auto_connect: If True, automatically connect to IBKR
        dual_connection: If True (default), enables Gateway+TWS failover.
            Set to False to use a single endpoint only.
        
    Returns:
        Configured LiveBroker instance.
        
    Example:
        >>> broker = create_live_broker()
        >>> broker.client.connect()
        >>> broker.submit_order(order)
    """
    logger.info("Creating LiveBroker for LIVE trading")
    
    # Create connection config from environment
    config = create_live_config(
        gateway_type=gateway_type,
        client_id=client_id,
        readonly=readonly,
    )
    
    # Create instrument mapper
    if mapper is None:
        try:
            from prometheus.execution.instrument_mapper import InstrumentMapper
        except ModuleNotFoundError as exc:  # pragma: no cover
            raise RuntimeError(
                "ib_async is required for LIVE/PAPER broker usage. "
                "Install with: pip install ib_async"
            ) from exc

        mapper = InstrumentMapper(db_manager)
    
    # Create IBKR client (optional dependency)
    try:
        from prometheus.execution.ibkr_client_impl import IbkrClientImpl
    except ModuleNotFoundError as exc:  # pragma: no cover
        raise RuntimeError(
            "ib_async is required for LIVE/PAPER broker usage. "
            "Install with: pip install ib_async"
        ) from exc

    # Build connection manager for Gateway ↔ TWS failover
    conn_mgr: Optional[DualConnectionManager] = None
    if dual_connection:
        creds = load_credentials(IbkrMode.LIVE)
        dual_cfg = create_dual_config_from_mode(
            "LIVE",
            host=config.host,
            primary_client_id=client_id,
            backup_client_id=client_id + 10,
            account_id=creds.account,
            readonly=readonly,
        )
        conn_mgr = DualConnectionManager(dual_cfg)
        logger.info(
            "Dual-connection failover enabled: Gateway:%d ↔ TWS:%d",
            dual_cfg.primary.port,
            dual_cfg.backup.port,
        )

    client = IbkrClientImpl(config, mapper, connection_manager=conn_mgr)
    
    # Create broker
    base_broker = LiveBroker(
        account_id=config.account_id,
        client=client,
    )

    exec_risk = get_config().execution_risk
    if exec_risk.enabled:
        broker = RiskCheckingBroker(inner=base_broker, config=exec_risk)
    else:
        broker = base_broker
    
    if auto_connect:
        logger.info("Auto-connecting to IBKR")
        client.connect()
    
    logger.info(
        "LiveBroker created: account=%s, port=%d, readonly=%s, "
        "dual_connection=%s, risk_enabled=%s",
        config.account_id,
        config.port,
        readonly,
        dual_connection,
        exec_risk.enabled,
    )
    
    return broker


def create_paper_broker(
    *,
    gateway_type: IbkrGatewayType = IbkrGatewayType.GATEWAY,
    client_id: int = 1,
    readonly: bool = False,
    db_manager: Optional[DatabaseManager] = None,
    mapper: Optional[InstrumentMapper] = None,
    auto_connect: bool = False,
    dual_connection: bool = True,
) -> PaperBroker:
    """Create a PaperBroker instance for PAPER trading.
    
    This creates a PaperBroker with IbkrClientImpl configured for paper trading,
    loading credentials from environment variables:
    - IBKR_PAPER_USERNAME (default: xubtmn245)
    - IBKR_PAPER_PASSWORD
    - IBKR_PAPER_ACCOUNT (default: DUN807925)
    
    Args:
        gateway_type: Gateway type (GATEWAY or TWS), defaults to GATEWAY
        client_id: API client ID, defaults to 1
        readonly: Whether to enable readonly mode (no order submission)
        db_manager: Optional database manager for instrument mapper
        mapper: Optional custom instrument mapper
        auto_connect: If True, automatically connect to IBKR
        dual_connection: If True (default), enables Gateway+TWS failover.
            Set to False to use a single endpoint only.
        
    Returns:
        Configured PaperBroker instance.
        
    Example:
        >>> broker = create_paper_broker(auto_connect=True)
        >>> broker.submit_order(order)
    """
    logger.info("Creating PaperBroker for PAPER trading")
    
    # Create connection config from environment
    config = create_paper_config(
        gateway_type=gateway_type,
        client_id=client_id,
        readonly=readonly,
    )
    
    # Create instrument mapper
    if mapper is None:
        try:
            from prometheus.execution.instrument_mapper import InstrumentMapper
        except ModuleNotFoundError as exc:  # pragma: no cover
            raise RuntimeError(
                "ib_async is required for LIVE/PAPER broker usage. "
                "Install with: pip install ib_async"
            ) from exc

        mapper = InstrumentMapper(db_manager)
    
    # Create IBKR client (optional dependency)
    try:
        from prometheus.execution.ibkr_client_impl import IbkrClientImpl
    except ModuleNotFoundError as exc:  # pragma: no cover
        raise RuntimeError(
            "ib_async is required for LIVE/PAPER broker usage. "
            "Install with: pip install ib_async"
        ) from exc

    # Build connection manager for Gateway ↔ TWS failover
    conn_mgr: Optional[DualConnectionManager] = None
    if dual_connection:
        creds = load_credentials(IbkrMode.PAPER)
        dual_cfg = create_dual_config_from_mode(
            "PAPER",
            host=config.host,
            primary_client_id=client_id,
            backup_client_id=client_id + 10,
            account_id=creds.account,
            readonly=readonly,
        )
        conn_mgr = DualConnectionManager(dual_cfg)
        logger.info(
            "Dual-connection failover enabled: Gateway:%d ↔ TWS:%d",
            dual_cfg.primary.port,
            dual_cfg.backup.port,
        )

    client = IbkrClientImpl(config, mapper, connection_manager=conn_mgr)
    
    # Create broker
    base_broker = PaperBroker(
        account_id=config.account_id,
        client=client,
    )

    exec_risk = get_config().execution_risk
    if exec_risk.enabled:
        broker = RiskCheckingBroker(inner=base_broker, config=exec_risk)
    else:
        broker = base_broker
    
    if auto_connect:
        logger.info("Auto-connecting to IBKR")
        client.connect()
    
    logger.info(
        "PaperBroker created: account=%s, port=%d, readonly=%s, "
        "dual_connection=%s, risk_enabled=%s",
        config.account_id,
        config.port,
        readonly,
        dual_connection,
        exec_risk.enabled,
    )
    
    return broker


__all__ = [
    "create_live_broker",
    "create_paper_broker",
]
