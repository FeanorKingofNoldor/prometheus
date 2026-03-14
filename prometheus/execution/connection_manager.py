"""Prometheus v2 – Dual-connection failover manager.

Manages two IBKR connection endpoints:
- **Primary**: IB Gateway (headless, recommended for automated trading)
- **Backup**: TWS Desktop App (manual fallback)

Both connections use ``ib_async`` via :mod:`prometheus.execution.ib_compat`.
The only difference is the port: Gateway uses 4001/4002, TWS uses 7496/7497.

Failover behaviour:
1. On startup, connect to **primary** (Gateway).
2. If primary is unavailable or drops, automatically switch to **backup** (TWS).
3. A background probe periodically checks whether primary is back online.
4. When primary recovers, migrate back and disconnect from backup.

The active ``IB`` instance is exposed via :meth:`get_ib` so that
:class:`IbkrClientImpl` and :class:`IbkrMarketDataService` always have
a connected handle without caring which endpoint is live.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Callable, Dict, List, Optional

from apathis.core.logging import get_logger
from prometheus.execution.ib_compat import IB
from prometheus.execution.ibkr_client import IbkrConnectionConfig

logger = get_logger(__name__)


# ── Types ─────────────────────────────────────────────────────────────

class ConnectionTarget(str, Enum):
    """Which IBKR endpoint is active."""

    GATEWAY = "GATEWAY"
    TWS = "TWS"


class FailoverEvent(str, Enum):
    """Events emitted during failover lifecycle."""

    CONNECTED = "CONNECTED"
    FAILOVER_START = "FAILOVER_START"
    FAILOVER_COMPLETE = "FAILOVER_COMPLETE"
    FAILBACK_START = "FAILBACK_START"
    FAILBACK_COMPLETE = "FAILBACK_COMPLETE"
    DISCONNECTED = "DISCONNECTED"
    ALL_ENDPOINTS_DOWN = "ALL_ENDPOINTS_DOWN"


FailoverCallback = Callable[[FailoverEvent, ConnectionTarget, Optional[str]], None]
"""Signature: (event, active_target, detail_message)"""


@dataclass
class ConnectionHealth:
    """Snapshot of connection health state."""

    active_target: ConnectionTarget
    connected: bool
    last_heartbeat: Optional[datetime] = None
    failover_count: int = 0
    failback_count: int = 0
    consecutive_failures: int = 0
    uptime_start: Optional[datetime] = None


@dataclass
class DualConnectionConfig:
    """Configuration for dual-endpoint failover.

    Attributes
    ----------
    primary:
        Config for the primary endpoint (IB Gateway).
    backup:
        Config for the backup endpoint (TWS Desktop).
    probe_interval_sec:
        How often (seconds) to probe the inactive endpoint.
        Default 120 s (2 min) — avoids spamming Gateway with connection
        attempts while TWS is active.
    heartbeat_interval_sec:
        How often (seconds) to check the active connection is alive.
    max_failover_attempts:
        Max consecutive attempts to reach *any* endpoint before giving up.
    connect_timeout_sec:
        Per-attempt connect timeout override (applied to both endpoints).
    failback_enabled:
        Whether to automatically switch back to primary when it recovers.
    """

    primary: IbkrConnectionConfig
    backup: IbkrConnectionConfig
    probe_interval_sec: int = 120
    heartbeat_interval_sec: int = 30
    max_failover_attempts: int = 5
    connect_timeout_sec: int = 15
    failback_enabled: bool = True


# ── Connection manager ────────────────────────────────────────────────

class DualConnectionManager:
    """Manages automatic failover between IB Gateway and TWS.

    Usage::

        mgr = DualConnectionManager(dual_cfg)
        mgr.connect()           # tries Gateway, then TWS
        ib = mgr.get_ib()       # always returns the active IB handle
        # ... use ib for orders, market data, etc.
        mgr.shutdown()

    The manager owns the ``IB`` instances.  Do **not** call
    ``ib.connect()`` / ``ib.disconnect()`` externally.
    """

    def __init__(
        self,
        config: DualConnectionConfig,
        callbacks: Optional[List[FailoverCallback]] = None,
    ) -> None:
        self._cfg = config
        self._callbacks: List[FailoverCallback] = list(callbacks or [])

        # Two IB handles — only one is connected at a time.
        self._primary_ib = IB()
        self._backup_ib = IB()

        # State
        self._active: ConnectionTarget = ConnectionTarget.GATEWAY
        self._active_ib: IB = self._primary_ib
        self._connected = False
        self._lock = threading.Lock()

        # Health tracking
        self._failover_count = 0
        self._failback_count = 0
        self._consecutive_failures = 0
        self._last_heartbeat: Optional[datetime] = None
        self._uptime_start: Optional[datetime] = None

        # Background threads
        self._monitor_running = False
        self._monitor_thread: Optional[threading.Thread] = None

    # ── Public API ────────────────────────────────────────────────────

    def connect(self) -> ConnectionTarget:
        """Establish a connection, trying primary then backup.

        Returns the :class:`ConnectionTarget` that succeeded.

        Raises
        ------
        RuntimeError
            If neither endpoint is reachable after max attempts.
        """
        with self._lock:
            if self._connected and self._active_ib.isConnected():
                logger.debug("DualConnectionManager already connected to %s", self._active.value)
                return self._active

            # Try primary first
            target = self._try_connect(
                ConnectionTarget.GATEWAY,
                self._primary_ib,
                self._cfg.primary,
            )
            if target is not None:
                self._activate(target, self._primary_ib)
                self._start_monitor()
                return target

            # Primary failed → try backup
            logger.warning("Primary (Gateway) unavailable, trying backup (TWS)...")
            target = self._try_connect(
                ConnectionTarget.TWS,
                self._backup_ib,
                self._cfg.backup,
            )
            if target is not None:
                self._activate(target, self._backup_ib)
                self._failover_count += 1
                self._emit(FailoverEvent.FAILOVER_COMPLETE, target, "Initial connect via backup")
                self._start_monitor()
                return target

            self._emit(FailoverEvent.ALL_ENDPOINTS_DOWN, self._active, "Neither Gateway nor TWS reachable")
            raise RuntimeError(
                "DualConnectionManager: neither Gateway nor TWS is reachable. "
                "Check that IB Gateway or TWS is running and API connections are enabled."
            )

    def disconnect(self) -> None:
        """Disconnect from the active endpoint and stop monitoring."""
        self._stop_monitor()
        with self._lock:
            self._safe_disconnect(self._primary_ib)
            self._safe_disconnect(self._backup_ib)
            self._connected = False
            self._emit(FailoverEvent.DISCONNECTED, self._active, None)
            logger.info("DualConnectionManager disconnected")

    def shutdown(self) -> None:
        """Alias for :meth:`disconnect`."""
        self.disconnect()

    def get_ib(self) -> IB:
        """Return the currently active ``IB`` instance.

        Raises
        ------
        RuntimeError
            If not connected.
        """
        if not self._connected:
            raise RuntimeError("DualConnectionManager is not connected. Call connect() first.")
        return self._active_ib

    def is_connected(self) -> bool:
        """Return True if the active endpoint is connected."""
        return self._connected and self._active_ib.isConnected()

    @property
    def active_target(self) -> ConnectionTarget:
        return self._active

    def get_health(self) -> ConnectionHealth:
        """Return a snapshot of connection health."""
        return ConnectionHealth(
            active_target=self._active,
            connected=self.is_connected(),
            last_heartbeat=self._last_heartbeat,
            failover_count=self._failover_count,
            failback_count=self._failback_count,
            consecutive_failures=self._consecutive_failures,
            uptime_start=self._uptime_start,
        )

    def add_callback(self, cb: FailoverCallback) -> None:
        self._callbacks.append(cb)

    # ── Internal connection helpers ───────────────────────────────────

    def _try_connect(
        self,
        target: ConnectionTarget,
        ib: IB,
        config: IbkrConnectionConfig,
    ) -> Optional[ConnectionTarget]:
        """Attempt to connect ``ib`` to ``config``. Returns target on success, None on failure."""
        try:
            if ib.isConnected():
                ib.disconnect()

            timeout = self._cfg.connect_timeout_sec or config.connect_timeout_sec
            logger.info(
                "Connecting to %s at %s:%d (client_id=%d, timeout=%ds)",
                target.value, config.host, config.port, config.client_id, timeout,
            )
            ib.connect(
                host=config.host,
                port=config.port,
                clientId=config.client_id,
                readonly=config.readonly,
                timeout=timeout,
            )
            if ib.isConnected():
                logger.info("Connected to %s successfully", target.value)
                return target

        except Exception as exc:
            logger.warning("Failed to connect to %s: %s", target.value, exc)

        return None

    def _activate(self, target: ConnectionTarget, ib: IB) -> None:
        """Set the active endpoint (caller must hold ``_lock``)."""
        self._active = target
        self._active_ib = ib
        self._connected = True
        self._consecutive_failures = 0
        self._last_heartbeat = datetime.now(timezone.utc)
        self._uptime_start = datetime.now(timezone.utc)
        self._emit(FailoverEvent.CONNECTED, target, None)

    @staticmethod
    def _safe_disconnect(ib: IB) -> None:
        try:
            if ib.isConnected():
                ib.disconnect()
        except Exception:
            pass

    # ── Background monitor ────────────────────────────────────────────

    def _start_monitor(self) -> None:
        if self._monitor_running:
            return
        self._monitor_running = True
        self._monitor_thread = threading.Thread(
            target=self._monitor_loop,
            daemon=True,
            name="DualConnMonitor",
        )
        self._monitor_thread.start()
        logger.info("Connection monitor started (heartbeat=%ds, probe=%ds)",
                     self._cfg.heartbeat_interval_sec, self._cfg.probe_interval_sec)

    def _stop_monitor(self) -> None:
        if not self._monitor_running:
            return
        self._monitor_running = False
        if self._monitor_thread and self._monitor_thread.is_alive():
            self._monitor_thread.join(timeout=10)
        logger.info("Connection monitor stopped")

    def _monitor_loop(self) -> None:
        """Background loop: heartbeat + optional probe-back-to-primary."""
        last_probe = time.monotonic()

        while self._monitor_running:
            try:
                time.sleep(self._cfg.heartbeat_interval_sec)
                if not self._monitor_running:
                    break

                # ── Heartbeat: check active connection ────────────
                if self._active_ib.isConnected():
                    self._last_heartbeat = datetime.now(timezone.utc)
                    self._consecutive_failures = 0
                else:
                    logger.warning("Heartbeat: active connection (%s) is down", self._active.value)
                    self._consecutive_failures += 1
                    self._handle_active_down()

                # ── Probe: try to fail back to primary ────────────
                if (
                    self._cfg.failback_enabled
                    and self._active == ConnectionTarget.TWS
                    and self._connected
                    and (time.monotonic() - last_probe) >= self._cfg.probe_interval_sec
                ):
                    last_probe = time.monotonic()
                    self._probe_primary()

            except Exception as exc:
                logger.error("Monitor loop error: %s", exc, exc_info=True)

    def _handle_active_down(self) -> None:
        """Active endpoint dropped — fail over to the other."""
        with self._lock:
            if self._consecutive_failures > self._cfg.max_failover_attempts:
                self._emit(
                    FailoverEvent.ALL_ENDPOINTS_DOWN,
                    self._active,
                    f"Exceeded {self._cfg.max_failover_attempts} consecutive failures",
                )
                logger.error(
                    "Max failover attempts reached (%d). Manual intervention required.",
                    self._cfg.max_failover_attempts,
                )
                return

            # Determine which endpoint to try
            if self._active == ConnectionTarget.GATEWAY:
                alt_target = ConnectionTarget.TWS
                alt_ib = self._backup_ib
                alt_config = self._cfg.backup
            else:
                alt_target = ConnectionTarget.GATEWAY
                alt_ib = self._primary_ib
                alt_config = self._cfg.primary

            self._emit(FailoverEvent.FAILOVER_START, alt_target, f"Failing over from {self._active.value}")

            # Disconnect the dead one
            self._safe_disconnect(self._active_ib)

            # Try the alternative
            result = self._try_connect(alt_target, alt_ib, alt_config)
            if result is not None:
                self._activate(alt_target, alt_ib)
                self._failover_count += 1
                self._emit(FailoverEvent.FAILOVER_COMPLETE, alt_target, None)
                return

            # Alternative also down — try the original one more time
            if self._active == ConnectionTarget.GATEWAY:
                orig_ib, orig_config = self._primary_ib, self._cfg.primary
            else:
                orig_ib, orig_config = self._backup_ib, self._cfg.backup

            result = self._try_connect(self._active, orig_ib, orig_config)
            if result is not None:
                self._activate(self._active, orig_ib)
                return

            self._connected = False
            logger.error("Both Gateway and TWS are unreachable")

    def _probe_primary(self) -> None:
        """While on backup (TWS), probe whether Gateway is back."""
        logger.debug("Probing primary (Gateway) for failback...")

        # Use a temporary IB to avoid disturbing the active backup connection.
        probe_ib = IB()
        try:
            probe_ib.connect(
                host=self._cfg.primary.host,
                port=self._cfg.primary.port,
                clientId=self._cfg.primary.client_id + 100,  # different client_id to avoid collision
                readonly=True,
                timeout=self._cfg.connect_timeout_sec,
            )
            if probe_ib.isConnected():
                probe_ib.disconnect()
                logger.info("Primary (Gateway) is back online — initiating failback")
                self._failback_to_primary()
        except Exception:
            logger.debug("Primary (Gateway) still unreachable")
        finally:
            self._safe_disconnect(probe_ib)

    def _failback_to_primary(self) -> None:
        """Switch back from TWS to Gateway."""
        with self._lock:
            self._emit(FailoverEvent.FAILBACK_START, ConnectionTarget.GATEWAY, None)

            result = self._try_connect(
                ConnectionTarget.GATEWAY,
                self._primary_ib,
                self._cfg.primary,
            )
            if result is not None:
                # Disconnect backup
                self._safe_disconnect(self._backup_ib)
                self._activate(ConnectionTarget.GATEWAY, self._primary_ib)
                self._failback_count += 1
                self._emit(FailoverEvent.FAILBACK_COMPLETE, ConnectionTarget.GATEWAY, None)
                logger.info("Failback to Gateway complete")
            else:
                logger.warning("Failback to Gateway failed — staying on TWS")

    # ── Event emission ────────────────────────────────────────────────

    def _emit(
        self,
        event: FailoverEvent,
        target: ConnectionTarget,
        detail: Optional[str],
    ) -> None:
        for cb in self._callbacks:
            try:
                cb(event, target, detail)
            except Exception as exc:
                logger.error("Failover callback error: %s", exc)

    # ── Diagnostics ───────────────────────────────────────────────────

    def get_status(self) -> Dict:
        """Return a JSON-serialisable status dict."""
        return {
            "active_target": self._active.value,
            "connected": self.is_connected(),
            "primary": {
                "host": self._cfg.primary.host,
                "port": self._cfg.primary.port,
                "ib_connected": self._primary_ib.isConnected(),
            },
            "backup": {
                "host": self._cfg.backup.host,
                "port": self._cfg.backup.port,
                "ib_connected": self._backup_ib.isConnected(),
            },
            "failover_count": self._failover_count,
            "failback_count": self._failback_count,
            "consecutive_failures": self._consecutive_failures,
            "last_heartbeat": self._last_heartbeat.isoformat() if self._last_heartbeat else None,
            "uptime_start": self._uptime_start.isoformat() if self._uptime_start else None,
        }


# ── Factory helpers ───────────────────────────────────────────────────

def create_dual_config_from_mode(
    mode: str,
    *,
    host: str = "127.0.0.1",
    primary_client_id: int = 1,
    backup_client_id: int = 2,
    account_id: Optional[str] = None,
    readonly: bool = False,
) -> DualConnectionConfig:
    """Build a :class:`DualConnectionConfig` for LIVE or PAPER mode.

    Uses the standard IBKR port conventions:
    - LIVE:  Gateway=4001, TWS=7496
    - PAPER: Gateway=4002, TWS=7497

    Parameters
    ----------
    mode:
        ``"LIVE"`` or ``"PAPER"``  (case-insensitive).
    primary_client_id:
        client_id for the Gateway connection (default 1).
    backup_client_id:
        client_id for the TWS connection (default 2).
        Must differ from primary_client_id so both can coexist
        if Gateway and TWS are on the same machine.
    """
    mode_up = mode.upper()
    if mode_up == "LIVE":
        gw_port, tws_port = 4001, 7496
    elif mode_up == "PAPER":
        gw_port, tws_port = 4002, 7497
    else:
        raise ValueError(f"mode must be 'LIVE' or 'PAPER', got {mode!r}")

    primary = IbkrConnectionConfig(
        host=host,
        port=gw_port,
        client_id=primary_client_id,
        account_id=account_id,
        readonly=readonly,
    )
    backup = IbkrConnectionConfig(
        host=host,
        port=tws_port,
        client_id=backup_client_id,
        account_id=account_id,
        readonly=readonly,
    )
    return DualConnectionConfig(primary=primary, backup=backup)


__all__ = [
    "ConnectionTarget",
    "FailoverEvent",
    "FailoverCallback",
    "ConnectionHealth",
    "DualConnectionConfig",
    "DualConnectionManager",
    "create_dual_config_from_mode",
]
