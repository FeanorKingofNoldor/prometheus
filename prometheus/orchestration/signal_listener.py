"""Postgres LISTEN/NOTIFY-driven signal alert listener.

The market-aware daemon polls every 60s, which is fine for the daily
pipeline but lags real-time intel emits.  This module runs a background
thread holding a dedicated psycopg2 connection on ``LISTEN`` for the
``prometheus_signal_alert`` channel.  When an EXTREME divergence or
CRITICAL compound-pressure row is upserted, the trigger fires and the
listener routes the payload to a callback that can short-circuit the
60s wait and trigger an immediate downstream task (e.g. options
re-evaluation, a flash decision row, a notification).

Usage::

    listener = SignalAlertListener(
        db_manager=db,
        on_alert=lambda payload: my_handler(payload),
    )
    listener.start()
    ...
    listener.stop()

The listener is **idempotent** about start/stop and **non-blocking** on
DB read errors — failed reads back off and retry, so a flaky network
doesn't kill the daemon.
"""

from __future__ import annotations

import json
import select
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

from apatheon.core.database import DatabaseManager
from apatheon.core.logging import get_logger

logger = get_logger(__name__)


CHANNEL = "prometheus_signal_alert"

# How long we sleep between failed connection attempts (capped backoff).
_RECONNECT_BASE_SECONDS = 1.0
_RECONNECT_MAX_SECONDS = 60.0

# Poll timeout for select(); the listener wakes up at least this often
# to honour the stop flag.
_SELECT_TIMEOUT_SECONDS = 5.0


@dataclass(frozen=True)
class SignalAlert:
    """Decoded signal alert payload received over LISTEN/NOTIFY."""

    source: str             # "divergence" | "compound_pressure"
    severity: str           # "EXTREME" | "CRITICAL"
    entity_type: str
    entity_id: str
    as_of_date: str
    trading_signal: str | None = None
    encirclement_score: float | None = None
    decision_id: str | None = None
    received_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    raw: dict[str, Any] = field(default_factory=dict)


def parse_payload(raw_payload: str) -> SignalAlert | None:
    """Parse a NOTIFY payload string into a typed alert.

    Returns ``None`` for malformed payloads — the caller should log and
    skip rather than crash the listener.
    """
    if not raw_payload:
        return None
    try:
        data = json.loads(raw_payload)
    except (ValueError, TypeError):
        logger.warning("[signal_listener] dropped non-JSON payload")
        return None
    if not isinstance(data, dict):
        logger.warning("[signal_listener] dropped non-object payload")
        return None

    source = str(data.get("source") or "").strip()
    severity = str(data.get("severity") or "").strip()
    entity_type = str(data.get("entity_type") or "").strip()
    entity_id = str(data.get("entity_id") or "").strip()
    if not source or not severity or not entity_type or not entity_id:
        logger.warning("[signal_listener] dropped payload missing required fields")
        return None

    return SignalAlert(
        source=source,
        severity=severity,
        entity_type=entity_type,
        entity_id=entity_id,
        as_of_date=str(data.get("as_of_date") or ""),
        trading_signal=(
            str(data["trading_signal"]) if data.get("trading_signal") else None
        ),
        encirclement_score=(
            float(data["encirclement_score"])
            if data.get("encirclement_score") is not None
            else None
        ),
        decision_id=(
            str(data["decision_id"]) if data.get("decision_id") else None
        ),
        raw=data,
    )


@dataclass
class SignalAlertListener:
    """Background thread that listens for ``prometheus_signal_alert``.

    Each received alert is parsed and dispatched to ``on_alert``.  All
    callback exceptions are caught and logged — the listener never dies
    because of a buggy handler.
    """

    db_manager: DatabaseManager
    on_alert: Callable[[SignalAlert], None]
    channel: str = CHANNEL

    _thread: threading.Thread | None = field(default=None, init=False, repr=False)
    _stop_event: threading.Event = field(default_factory=threading.Event, init=False, repr=False)
    _started: bool = field(default=False, init=False, repr=False)

    def start(self) -> None:
        if self._started:
            return
        self._stop_event.clear()
        thread = threading.Thread(
            target=self._run,
            name="prometheus-signal-listener",
            daemon=True,
        )
        thread.start()
        self._thread = thread
        self._started = True
        logger.info("[signal_listener] started on channel=%s", self.channel)

    def stop(self, timeout: float = 5.0) -> None:
        if not self._started:
            return
        self._stop_event.set()
        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=timeout)
        self._started = False
        logger.info("[signal_listener] stopped")

    def _run(self) -> None:
        backoff = _RECONNECT_BASE_SECONDS
        while not self._stop_event.is_set():
            try:
                self._listen_loop()
                backoff = _RECONNECT_BASE_SECONDS  # reset on clean exit
            except Exception:
                logger.exception(
                    "[signal_listener] listen loop crashed; reconnecting in %.1fs",
                    backoff,
                )
                # Bounded backoff so a permanent DB outage doesn't
                # busy-loop, but a transient hiccup recovers fast.
                self._stop_event.wait(backoff)
                backoff = min(_RECONNECT_MAX_SECONDS, backoff * 2)

    def _listen_loop(self) -> None:
        # We deliberately bypass the pooled get_runtime_connection() and
        # take a direct connection from the underlying psycopg2 pool —
        # LISTEN holds the connection for the lifetime of the listener.
        with self.db_manager.get_runtime_connection() as conn:
            # autocommit is required for LISTEN so notifications aren't
            # buffered inside a long-running transaction.  The pool's
            # health-check (`SELECT 1`) leaves the connection inside an
            # implicit transaction, so we must rollback before flipping
            # session settings or psycopg2 raises
            # "set_session cannot be used inside a transaction".
            conn.rollback()
            old_autocommit = conn.autocommit
            conn.autocommit = True
            try:
                cur = conn.cursor()
                try:
                    cur.execute(f"LISTEN {self.channel};")
                finally:
                    cur.close()

                logger.info("[signal_listener] LISTEN %s active", self.channel)

                while not self._stop_event.is_set():
                    # select() releases the GIL and lets us honour stop fast.
                    rlist, _, _ = select.select(
                        [conn], [], [], _SELECT_TIMEOUT_SECONDS,
                    )
                    if not rlist:
                        continue
                    try:
                        conn.poll()
                    except Exception:
                        logger.exception("[signal_listener] poll failed")
                        return  # outer loop will reconnect
                    while conn.notifies:
                        notify = conn.notifies.pop(0)
                        self._dispatch(notify.payload)
            finally:
                # Restore the connection's autocommit so the pool's next
                # consumer doesn't get an unexpected mode.
                try:
                    conn.autocommit = old_autocommit
                except Exception:
                    pass

    def _dispatch(self, payload: str) -> None:
        alert = parse_payload(payload)
        if alert is None:
            return
        try:
            self.on_alert(alert)
        except Exception:
            logger.exception(
                "[signal_listener] on_alert handler raised; payload=%s",
                payload[:200],
            )


def default_alert_handler(alert: SignalAlert) -> None:
    """Lightweight default handler — just logs at INFO level.

    The market-aware daemon installs its own handler that triggers an
    options re-evaluation; this default exists so the listener can be
    smoke-tested standalone.
    """
    logger.info(
        "[signal_listener] %s/%s entity=%s:%s decision_id=%s",
        alert.source,
        alert.severity,
        alert.entity_type,
        alert.entity_id,
        alert.decision_id or "—",
    )
