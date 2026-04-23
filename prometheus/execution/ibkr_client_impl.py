"""Prometheus v2 – IBKR client implementation using ib_async.

This module provides a concrete implementation of :class:`IbkrClient` using
the ``ib_async`` library for Interactive Brokers connectivity.

Key features:
* Automatic connection management with reconnection
* Dual-endpoint failover via :class:`DualConnectionManager`
  (IB Gateway primary, TWS Desktop backup)
* Order submission with contract translation
* Real-time position and account state sync
* Fill tracking and event handling
* Error handling and logging
"""

from __future__ import annotations

import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from apathis.core.logging import get_logger

from prometheus.execution.broker_interface import Fill, Order, OrderSide, OrderStatus, OrderType, Position
from prometheus.execution.connection_manager import DualConnectionManager
from prometheus.execution.ib_compat import (
    IB,
    Contract,
    LimitOrder,
    MarketOrder,
    StopLimitOrder,
    StopOrder,
    Trade,
)
from prometheus.execution.ib_compat import (
    Fill as IbFill,
)
from prometheus.execution.ib_compat import (
    Order as IbOrder,
)
from prometheus.execution.ibkr_client import IbkrClient, IbkrConnectionConfig
from prometheus.execution.instrument_mapper import InstrumentMapper

logger = get_logger(__name__)


class IbkrClientImpl(IbkrClient):
    """Concrete IBKR client implementation using ib_async.

    This implementation:
    - Manages connection to IBKR Gateway/TWS
    - Supports dual-endpoint failover via :class:`DualConnectionManager`
    - Translates Prometheus orders to IBKR contracts and orders
    - Tracks fills via event callbacks
    - Syncs positions and account state
    - Handles reconnection automatically

    If a ``connection_manager`` is provided, the manager owns the ``IB``
    instance and handles all connect/disconnect/failover logic.  The old
    single-config path (no manager) still works for backward compatibility.
    """

    def __init__(
        self,
        config: IbkrConnectionConfig,
        mapper: Optional[InstrumentMapper] = None,
        connection_manager: Optional[DualConnectionManager] = None,
    ) -> None:
        super().__init__(config)
        self._conn_mgr = connection_manager
        self._ib = IB()  # only used when no connection_manager
        self._connected = False

        # Instrument mapper for contract translation
        self._mapper = mapper or InstrumentMapper()

        # Local caches
        self._fills: List[Fill] = []
        self._positions: Dict[str, Position] = {}
        self._account_state: Dict = {}

        # Order tracking
        self._trades_by_ref: Dict[str, Trade] = {}  # Prometheus order_id (orderRef) -> IB Trade
        self._order_statuses: Dict[str, OrderStatus] = {}

        # Health monitoring
        self._last_heartbeat: Optional[datetime] = None
        self._heartbeat_interval_sec = 60  # Check connection every 60 seconds
        self._reconnect_attempts = 0
        self._max_reconnect_attempts = 5
        self._reconnect_delay_sec = 10
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._heartbeat_running = False

        # Setup event handlers (only for the local IB; manager path
        # re-wires after connect via _wire_events).
        if self._conn_mgr is None:
            self._wire_events(self._ib)

    def _wire_events(self, ib: IB) -> None:
        """Attach event handlers to an IB instance."""
        ib.orderStatusEvent += self._on_order_status
        ib.execDetailsEvent += self._on_exec_details
        ib.errorEvent += self._on_error
        ib.connectedEvent += self._on_connected
        ib.disconnectedEvent += self._on_disconnected

    # ------------------------------------------------------------------
    # Active IB handle
    # ------------------------------------------------------------------

    @property
    def ib(self) -> IB:
        """Return the active ``IB`` instance.

        When a :class:`DualConnectionManager` is present, this delegates
        to the manager (which handles failover transparently).  Otherwise
        it returns the locally-owned ``IB`` instance.
        """
        if self._conn_mgr is not None:
            return self._conn_mgr.get_ib()
        return self._ib

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def connect(self) -> None:
        """Establish connection to IBKR Gateway/TWS.

        If a :class:`DualConnectionManager` was provided at construction
        time, connection management (including failover) is delegated
        to the manager.
        """
        if self._conn_mgr is not None:
            return self._connect_via_manager()
        return self._connect_direct()

    def _connect_via_manager(self) -> None:
        """Connect using DualConnectionManager (Gateway ↔ TWS failover)."""
        if self._connected and self._conn_mgr.is_connected():
            logger.debug("IbkrClient already connected via manager")
            return

        try:
            target = self._conn_mgr.connect()
            logger.info("Connected to IBKR via %s (managed)", target.value)

            # Re-bind the internal reference so event handlers work.
            self._ib = self._conn_mgr.get_ib()
            self._wire_events(self._ib)
            self._connected = True

            self._mapper.load_instruments()
            self.sync()
            # No heartbeat thread needed — manager runs its own monitor.
        except Exception as e:
            logger.error("Failed to connect via manager: %s", e, exc_info=True)
            self._connected = False
            raise

    def _connect_direct(self) -> None:
        """Connect directly to a single endpoint (legacy path)."""
        if self._connected and self._ib.isConnected():
            logger.debug("IbkrClient already connected")
            return

        try:
            logger.info(
                "Connecting to IBKR at %s:%d (client_id=%d, account=%s)",
                self._config.host,
                self._config.port,
                self._config.client_id,
                self._config.account_id or "default",
            )

            self._ib.connect(
                host=self._config.host,
                port=self._config.port,
                clientId=self._config.client_id,
                readonly=self._config.readonly,
                timeout=self._config.connect_timeout_sec,
            )

            self._connected = True
            logger.info("Successfully connected to IBKR")

            # Load instruments for contract mapping
            self._mapper.load_instruments()

            # Initial sync
            self.sync()

            # Start heartbeat monitoring
            self._start_heartbeat()

        except Exception as e:
            logger.error("Failed to connect to IBKR: %s", e, exc_info=True)
            self._connected = False
            raise

    def disconnect(self) -> None:
        """Close the IBKR connection."""
        if self._conn_mgr is not None:
            self._conn_mgr.disconnect()
            self._connected = False
            return

        # Legacy direct path
        self._stop_heartbeat()
        if self._ib.isConnected():
            logger.info("Disconnecting from IBKR")
            self._ib.disconnect()
        self._connected = False

    def is_connected(self) -> bool:
        """Return True if connected to IBKR."""
        if self._conn_mgr is not None:
            return self._conn_mgr.is_connected()
        return self._connected and self._ib.isConnected()

    # ------------------------------------------------------------------
    # Order and execution API
    # ------------------------------------------------------------------

    def submit_order(self, order: Order) -> str:
        """Submit an order to IBKR and return the Prometheus order_id.

        We keep Prometheus `order.order_id` as the primary identifier and set it as
        IBKR `orderRef` for correlation.
        """
        if not self.is_connected():
            raise RuntimeError("Not connected to IBKR. Call connect() first.")

        # Always attach a timestamp so storage can persist a realistic event time.
        meta = order.metadata if isinstance(order.metadata, dict) else {}
        meta = dict(meta)
        meta.setdefault("timestamp", datetime.now(timezone.utc).isoformat())

        if self._config.readonly:
            logger.warning("Readonly mode enabled, order not submitted: %s", order.order_id)
            meta.setdefault("readonly", True)
            order.metadata = meta
            return order.order_id

        # Translate Prometheus order to IBKR contract and order.
        # If the caller pre-built a Contract (e.g. options chain discovery
        # produced an ad-hoc instrument_id that won't match the database),
        # honour it rather than going through the DB-backed mapper.
        prebuilt_contract = meta.get("contract") if isinstance(meta, dict) else None
        if prebuilt_contract is not None:
            try:
                qualified = self._ib.qualifyContracts(prebuilt_contract)
                contract = qualified[0] if qualified else prebuilt_contract
            except Exception as exc:
                logger.warning(
                    "Pre-built contract qualify failed for %s: %s — submitting unqualified",
                    order.order_id, exc,
                )
                contract = prebuilt_contract
            # Strip the live Contract object from metadata before persistence
            # (it is not JSON-serialisable for the orders table).
            meta = {k: v for k, v in meta.items() if k != "contract"}
            order.metadata = meta
        else:
            contract = self._create_contract(order.instrument_id)
        ib_order = self._create_ib_order(order)

        logger.info(
            "Submitting order: %s %s %s x %.2f @ %s",
            order.order_id,
            order.side.value,
            order.instrument_id,
            order.quantity,
            order.order_type.value,
        )

        try:
            # Place order
            trade = self._ib.placeOrder(contract, ib_order)

            # Store trade mapping and best-effort status.
            self._trades_by_ref[order.order_id] = trade
            self._order_statuses[order.order_id] = self._map_order_status(trade)

            # Attach broker identifiers for auditing/debugging.
            meta["ibkr"] = {
                "orderId": int(trade.order.orderId) if trade.order else None,
                "permId": int(trade.order.permId) if trade.order and getattr(trade.order, "permId", None) else None,
            }
            order.metadata = meta

            logger.info(
                "Order submitted successfully: %s (IBKR orderId=%s)",
                order.order_id,
                trade.order.orderId if trade.order else "unknown",
            )

            return order.order_id

        except Exception as e:
            logger.error("Failed to submit order %s: %s", order.order_id, e, exc_info=True)
            raise

    def cancel_order(self, order_id: str) -> bool:
        """Attempt to cancel an order by Prometheus `order_id` (IBKR orderRef)."""
        if not self.is_connected():
            raise RuntimeError("Not connected to IBKR")

        if self._config.readonly:
            logger.warning("Readonly mode enabled, cancel not submitted: %s", order_id)
            return False

        trade = self._trades_by_ref.get(order_id)
        if trade is None:
            trade = self._find_trade(order_id)

        if trade is None or trade.order is None:
            logger.warning("Order %s not found in IB trades", order_id)
            return False

        try:
            logger.info("Cancelling order: %s (IBKR orderId=%s)", order_id, trade.order.orderId)
            self._ib.cancelOrder(trade.order)
            # Do NOT set status optimistically — let the IBKR callback
            # (_on_order_status) update it when the cancel is confirmed.
            # The order may have already filled by the time the cancel arrives.
            return True
        except Exception as e:
            logger.error("Failed to cancel order %s: %s", order_id, e, exc_info=True)
            return False

    def get_order_status(self, order_id: str) -> OrderStatus:
        """Return the best-effort order status.

        We primarily rely on cached statuses from IB callbacks, but also
        attempt to locate the trade on-demand.
        """

        status = self._order_statuses.get(order_id)
        if status is not None:
            return status

        trade = self._find_trade(order_id)
        if trade is None:
            # Don't return REJECTED for unknown orders — the order may
            # still be processing or the Trade object may not have the
            # orderRef populated yet. SUBMITTED is a safer default.
            logger.debug("get_order_status: order_id=%s not found in IB trades, returning SUBMITTED", order_id)
            return OrderStatus.SUBMITTED

        status = self._map_order_status(trade)
        self._order_statuses[order_id] = status
        return status

    def _find_trade(self, order_id: str) -> Trade | None:
        """Locate an IB Trade by orderRef (Prometheus order_id)."""

        try:
            for t in self._ib.trades():
                try:
                    if t.order and t.order.orderRef == order_id:
                        return t
                except Exception:
                    continue
        except Exception:
            return None
        return None

    @staticmethod
    def _map_order_status(trade: Trade) -> OrderStatus:
        """Map ib_insync trade status to Prometheus OrderStatus."""

        try:
            raw = str(trade.orderStatus.status or "").strip()
        except Exception:
            raw = ""
        raw_up = raw.upper()

        try:
            filled = float(getattr(trade.orderStatus, "filled", 0.0) or 0.0)
        except Exception:
            filled = 0.0
        try:
            remaining = float(getattr(trade.orderStatus, "remaining", 0.0) or 0.0)
        except Exception:
            remaining = 0.0

        if filled > 0.0 and remaining > 0.0:
            return OrderStatus.PARTIALLY_FILLED

        if raw_up in {"PENDINGSUBMIT", "PENDING_SUBMIT", "PENDINGCANCEL", "PENDING_CANCEL"}:
            return OrderStatus.PENDING
        if raw_up in {"PRESUBMITTED", "PRESUBMIT", "SUBMITTED"}:
            return OrderStatus.SUBMITTED
        if raw_up in {"FILLED"}:
            return OrderStatus.FILLED
        if raw_up in {"CANCELLED", "APICANCELLED", "API_CANCELLED"}:
            return OrderStatus.CANCELLED
        if raw_up in {"INACTIVE"}:
            return OrderStatus.REJECTED

        # Default to SUBMITTED for unknown-but-present statuses.
        return OrderStatus.SUBMITTED

    def get_fills(self, since: Optional[datetime] = None) -> List[Fill]:
        """Return fills since the given timestamp."""
        if since is None:
            return list(self._fills)
        return [f for f in self._fills if f.timestamp >= since]

    def get_positions(self) -> Dict[str, Position]:
        """Return current positions keyed by instrument_id."""
        return dict(self._positions)

    def get_account_state(self) -> Dict:
        """Return account-level information."""
        return dict(self._account_state)

    def sync(self) -> None:
        """Synchronize positions and account state from IBKR."""
        if not self.is_connected():
            logger.warning("Cannot sync: not connected to IBKR")
            return

        logger.debug("Syncing positions and account state from IBKR")

        # Sync positions
        self._sync_positions()

        # Sync account values
        self._sync_account_values()

        # Request open orders so this process can cancel/reconcile orders
        # created in previous runs.
        try:
            if hasattr(self._ib, "reqAllOpenOrders"):
                self._ib.reqAllOpenOrders()
            elif hasattr(self._ib, "reqOpenOrders"):
                self._ib.reqOpenOrders()
        except Exception as e:
            logger.debug("Failed to request open orders: %s", e)

        # Refresh trade/status caches from currently-known trades.
        try:
            for t in self._ib.trades():
                ref = getattr(getattr(t, "order", None), "orderRef", None)
                if isinstance(ref, str) and ref:
                    self._trades_by_ref[ref] = t
                    try:
                        self._order_statuses[ref] = self._map_order_status(t)
                    except Exception:
                        pass
        except Exception:
            pass

        logger.debug("Sync complete: %d positions", len(self._positions))

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _create_contract(self, instrument_id: str) -> Contract:
        """Create IBKR contract from Prometheus instrument_id.

        Uses InstrumentMapper to translate instrument_id to IBKR contract.
        """
        # Use mapper to get contract
        contract = self._mapper.get_contract(instrument_id)

        # Qualify contract to ensure it's valid
        try:
            contracts = self._ib.qualifyContracts(contract)
            if not contracts:
                raise ValueError(f"Could not qualify contract for {instrument_id}")
            return contracts[0]
        except Exception as e:
            logger.error("Failed to qualify contract for %s: %s", instrument_id, e)
            # Return unqualified contract and hope for the best
            return contract

    def _create_ib_order(self, order: Order) -> IbOrder:
        """Create IBKR order from Prometheus order."""
        if order.quantity <= 0:
            raise ValueError(f"Order quantity must be positive, got {order.quantity}")
        action = "BUY" if order.side == OrderSide.BUY else "SELL"

        if order.order_type == OrderType.LIMIT:
            if order.limit_price is None:
                raise ValueError(
                    f"LIMIT order {order.order_id} requires limit_price"
                )
            ib_order = LimitOrder(action, order.quantity, order.limit_price)

        elif order.order_type == OrderType.STOP:
            if order.stop_price is None:
                raise ValueError(
                    f"STOP order {order.order_id} requires stop_price"
                )
            ib_order = StopOrder(action, order.quantity, order.stop_price)

        elif order.order_type == OrderType.STOP_LIMIT:
            if order.stop_price is None or order.limit_price is None:
                raise ValueError(
                    f"STOP_LIMIT order {order.order_id} requires both "
                    f"stop_price and limit_price"
                )
            ib_order = StopLimitOrder(
                action, order.quantity,
                order.limit_price, order.stop_price,
            )

        else:
            # MARKET order (default)
            ib_order = MarketOrder(action, order.quantity)

        # Store Prometheus order_id in order ref for tracking
        ib_order.orderRef = order.order_id

        # Apply adaptive algo if requested via metadata
        meta = order.metadata or {}
        if meta.get("algo") == "adaptive":
            ib_order.algoStrategy = "Adaptive"
            urgency = meta.get("urgency", "Normal")  # Patient / Normal / Urgent
            ib_order.algoParams = [
                {"tag": "adaptivePriority", "value": urgency},
            ]
            logger.debug(
                "Using Adaptive algo (urgency=%s) for order %s",
                urgency, order.order_id,
            )

        return ib_order

    @staticmethod
    def _normalize_account_id(account_id: object) -> str:
        """Normalize account identifiers for robust comparisons."""
        if account_id is None:
            return ""
        return str(account_id).strip().upper()

    def _select_sync_account(self, available_accounts: List[object], *, source: str) -> str:
        """Select account scope for IBKR sync payloads."""
        configured = self._normalize_account_id(self._config.account_id)
        normalized_available = sorted(
            {
                norm
                for norm in (self._normalize_account_id(acc) for acc in available_accounts)
                if norm
            }
        )

        if configured:
            if configured in normalized_available or not normalized_available:
                return configured

            fallback = normalized_available[0]
            logger.warning(
                "Configured IBKR account_id %s not present in %s accounts %s; "
                "falling back to %s for sync.",
                self._config.account_id,
                source,
                normalized_available,
                fallback,
            )
            return fallback

        if len(normalized_available) > 1:
            fallback = normalized_available[0]
            logger.warning(
                "No IBKR account_id configured and multiple %s accounts discovered %s; "
                "using %s for sync.",
                source,
                normalized_available,
                fallback,
            )
            return fallback

        if normalized_available:
            return normalized_available[0]

        return ""

    def _filter_items_by_account(self, items: List[Any], account_id: str, *, account_attr: str) -> List[Any]:
        """Filter records by normalized account id when one is selected."""
        if not account_id:
            return list(items)

        return [
            item
            for item in items
            if self._normalize_account_id(getattr(item, account_attr, None)) == account_id
        ]

    def _sync_positions(self) -> None:
        """Sync positions from IBKR.

        Uses the portfolio view to obtain both quantity and valuation
        information. This avoids relying on fields that are not present on
        the ``Position`` objects returned by :meth:`IB.positions`.
        """
        try:
            # ``portfolio()`` returns a list of PortfolioItem objects with
            # position size, market value and P&L information.
            portfolio_items_all = list(self._ib.portfolio())
            effective_account = self._select_sync_account(
                [getattr(item, "account", None) for item in portfolio_items_all],
                source="portfolio",
            )
            portfolio_items = self._filter_items_by_account(
                portfolio_items_all,
                effective_account,
                account_attr="account",
            )

            self._positions.clear()

            for item in portfolio_items:

                instrument_id = self._contract_to_instrument_id(item.contract)

                position = Position(
                    instrument_id=instrument_id,
                    quantity=float(item.position),
                    avg_cost=float(item.averageCost),
                    market_value=float(item.marketValue),
                    unrealized_pnl=float(item.unrealizedPNL),
                )

                self._positions[instrument_id] = position

            if effective_account:
                logger.debug(
                    "Synced %d IBKR positions for account=%s (raw_items=%d)",
                    len(self._positions),
                    effective_account,
                    len(portfolio_items_all),
                )
            else:
                logger.debug(
                    "Synced %d IBKR positions (raw_items=%d, no account scoping)",
                    len(self._positions),
                    len(portfolio_items_all),
                )

        except Exception as e:
            logger.error("Failed to sync positions: %s", e, exc_info=True)

    def _sync_account_values(self) -> None:
        """Sync account values from IBKR."""
        try:
            configured_account = self._normalize_account_id(self._config.account_id)
            if configured_account:
                account_values = list(self._ib.accountValues(account=self._config.account_id))
            else:
                account_values = list(self._ib.accountValues())

            if configured_account and not account_values:
                all_values = list(self._ib.accountValues())
                effective_account = self._select_sync_account(
                    [getattr(av, "account", None) for av in all_values],
                    source="accountValues",
                )
                filtered = self._filter_items_by_account(
                    all_values,
                    effective_account,
                    account_attr="account",
                )
                account_values = filtered if filtered else all_values
            else:
                effective_account = self._select_sync_account(
                    [getattr(av, "account", None) for av in account_values],
                    source="accountValues",
                )
                filtered = self._filter_items_by_account(
                    account_values,
                    effective_account,
                    account_attr="account",
                )
                if filtered:
                    account_values = filtered

            self._account_state.clear()

            # Extract key account metrics
            for av in account_values:
                # Use tag as key, convert value to float if possible
                key = av.tag
                try:
                    value = float(av.value)
                except ValueError:
                    value = av.value

                self._account_state[key] = value

            # Compute equity if not directly available
            if "NetLiquidation" in self._account_state:
                self._account_state["equity"] = self._account_state["NetLiquidation"]

            # Add cash
            if "TotalCashValue" in self._account_state:
                self._account_state["cash"] = self._account_state["TotalCashValue"]
            if effective_account:
                logger.debug(
                    "Synced %d IBKR account keys for account=%s",
                    len(self._account_state),
                    effective_account,
                )
            else:
                logger.debug(
                    "Synced %d IBKR account keys (no account scoping)",
                    len(self._account_state),
                )

        except Exception as e:
            logger.error("Failed to sync account values: %s", e, exc_info=True)

    def _contract_to_instrument_id(self, contract: Contract) -> str:
        """Convert IBKR contract to Prometheus instrument_id."""
        return InstrumentMapper.contract_to_instrument_id(contract)

    # ------------------------------------------------------------------
    # Event handlers
    # ------------------------------------------------------------------

    def _on_order_status(
        self,
        trade: Any,  # Trade object from ib_insync
    ) -> None:
        """Handle order status updates."""
        order = trade.order
        order_id = getattr(order, "orderRef", None)  # Our Prometheus order_id

        logger.debug(
            "Order status update: %s -> %s (filled=%s/%s)",
            order_id,
            getattr(trade.orderStatus, "status", None),
            getattr(trade.orderStatus, "filled", None),
            getattr(trade.orderStatus, "remaining", None),
        )

        if isinstance(order_id, str) and order_id:
            try:
                self._order_statuses[order_id] = self._map_order_status(trade)
            except Exception:
                pass

    def _on_exec_details(
        self,
        trade: Any,  # Trade object
        fill: IbFill,  # Fill from ib_insync
    ) -> None:
        """Handle execution (fill) events."""
        execution = fill.execution

        # Extract order_id from order ref
        order_id = trade.order.orderRef if trade.order else None
        if not order_id:
            logger.warning("Received fill without order_id ref")
            order_id = f"unknown_{execution.execId}"

        # Determine side
        side = OrderSide.BUY if execution.side == "BOT" else OrderSide.SELL

        # Normalize execution timestamp (ib_insync may provide datetime or str).
        ts_raw = execution.time
        if isinstance(ts_raw, datetime):
            ts = ts_raw if ts_raw.tzinfo is not None else ts_raw.replace(tzinfo=timezone.utc)
        else:
            ts = datetime.fromisoformat(str(ts_raw))
            ts = ts if ts.tzinfo is not None else ts.replace(tzinfo=timezone.utc)

        # Create Prometheus Fill object
        prometheus_fill = Fill(
            fill_id=execution.execId,
            order_id=order_id,
            instrument_id=self._contract_to_instrument_id(trade.contract),
            side=side,
            quantity=float(execution.shares),
            price=float(execution.price),
            timestamp=ts,
            commission=float(fill.commissionReport.commission) if fill.commissionReport else 0.0,
            metadata={
                "exchange": execution.exchange,
                "exec_id": execution.execId,
                "order_id_ibkr": str(execution.orderId),
            },
        )

        # Dedup: IBKR can replay executions on reconnect
        if not hasattr(self, "_seen_fill_ids"):
            self._seen_fill_ids: set = set()
        if prometheus_fill.fill_id in self._seen_fill_ids:
            logger.debug("Duplicate fill %s ignored", prometheus_fill.fill_id)
            return
        self._seen_fill_ids.add(prometheus_fill.fill_id)
        self._fills.append(prometheus_fill)

        # Bound the fills cache to prevent unbounded memory growth
        _MAX_FILLS = 10_000
        if len(self._fills) > _MAX_FILLS:
            self._fills = self._fills[-_MAX_FILLS:]

        logger.info(
            "Fill received: %s %s %s x %.2f @ %.2f (commission=%.2f)",
            prometheus_fill.fill_id,
            side.value,
            prometheus_fill.instrument_id,
            prometheus_fill.quantity,
            prometheus_fill.price,
            prometheus_fill.commission,
        )

    def _on_error(
        self,
        reqId: int,
        errorCode: int,
        errorString: str,
        contract: Optional[Contract],
    ) -> None:
        """Handle error messages from IBKR."""
        # IBKR error codes: 100-449 are real errors (order rejects, limits, etc.),
        # 2100-2199 are informational system messages.
        # Codes 1100-1102 are connection-related (also important).
        if 2100 <= errorCode <= 2199:
            logger.debug("IBKR info [%d]: %s", errorCode, errorString)
        elif errorCode < 100:
            logger.debug("IBKR system [%d]: %s", errorCode, errorString)
        else:
            logger.warning("IBKR error [%d]: %s (reqId=%d)", errorCode, errorString, reqId)

    def _on_connected(self) -> None:
        """Handle connection established event."""
        logger.info("IBKR connection established")
        self._connected = True
        self._reconnect_attempts = 0  # Reset reconnect counter
        self._last_heartbeat = datetime.now(timezone.utc)

    def _on_disconnected(self) -> None:
        """Handle disconnection event."""
        logger.warning("IBKR connection lost")
        self._connected = False

        # When using a connection manager, failover is handled by the
        # manager's monitor thread — do NOT attempt reconnection here.
        if self._conn_mgr is not None:
            return

        # Legacy direct path: attempt auto-reconnection
        if self._reconnect_attempts < self._max_reconnect_attempts:
            self._attempt_reconnect()

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # Connection health monitoring
    # ------------------------------------------------------------------

    def _start_heartbeat(self) -> None:
        """Start heartbeat monitoring thread."""
        if self._heartbeat_running:
            return

        self._heartbeat_running = True
        self._last_heartbeat = datetime.now(timezone.utc)

        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            daemon=True,
            name="IbkrClientHeartbeat",
        )
        self._heartbeat_thread.start()

        logger.info("Heartbeat monitoring started")

    def _stop_heartbeat(self) -> None:
        """Stop heartbeat monitoring thread."""
        if not self._heartbeat_running:
            return

        self._heartbeat_running = False

        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            self._heartbeat_thread.join(timeout=5)

        logger.info("Heartbeat monitoring stopped")

    def _heartbeat_loop(self) -> None:
        """Heartbeat monitoring loop running in background thread."""
        while self._heartbeat_running:
            try:
                time.sleep(self._heartbeat_interval_sec)

                if not self._heartbeat_running:
                    break

                # Check connection health
                if self._ib.isConnected():
                    self._last_heartbeat = datetime.now(timezone.utc)
                    logger.debug("Heartbeat: connection healthy")
                else:
                    logger.warning("Heartbeat: connection lost, attempting reconnect")
                    if self._reconnect_attempts < self._max_reconnect_attempts:
                        self._attempt_reconnect()

            except Exception as e:
                logger.error("Error in heartbeat loop: %s", e, exc_info=True)

    def _attempt_reconnect(self) -> None:
        """Attempt to reconnect to IBKR."""
        self._reconnect_attempts += 1

        logger.info(
            "Attempting reconnection %d/%d in %d seconds",
            self._reconnect_attempts,
            self._max_reconnect_attempts,
            self._reconnect_delay_sec,
        )

        time.sleep(self._reconnect_delay_sec)

        try:
            self.connect()
            logger.info("Reconnection successful")
        except Exception as e:
            logger.error(
                "Reconnection attempt %d failed: %s",
                self._reconnect_attempts,
                e,
            )

            if self._reconnect_attempts >= self._max_reconnect_attempts:
                logger.error(
                    "Max reconnection attempts (%d) reached. Manual intervention required.",
                    self._max_reconnect_attempts,
                )

    def get_connection_health(self) -> Dict:
        """Get connection health status.

        When using a :class:`DualConnectionManager`, delegates to the
        manager for richer failover information.

        Returns:
            Dictionary with connection health information.
        """
        if self._conn_mgr is not None:
            return self._conn_mgr.get_status()

        return {
            "connected": self._connected,
            "ib_connected": self._ib.isConnected(),
            "last_heartbeat": self._last_heartbeat.isoformat() if self._last_heartbeat else None,
            "reconnect_attempts": self._reconnect_attempts,
            "max_reconnect_attempts": self._max_reconnect_attempts,
        }

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    def __del__(self) -> None:
        """Cleanup on deletion."""
        if self._conn_mgr is not None:
            try:
                self._conn_mgr.shutdown()
            except Exception:
                pass
            return

        self._stop_heartbeat()
        if self._ib.isConnected():
            try:
                self._ib.disconnect()
            except Exception:
                pass
