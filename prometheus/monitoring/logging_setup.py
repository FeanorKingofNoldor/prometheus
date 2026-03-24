"""Prometheus v2 – Monitoring logging setup.

This module provides a tiny helper to ensure monitoring components use
Prometheus's central logging configuration.

In early iterations, all logging is handled via :mod:`prometheus.core.logging`;
this module simply exposes a convenience function that can be imported
by monitoring scripts or the monitoring web API.
"""

from __future__ import annotations

from apathis.core.logging import get_logger


def configure_monitoring_logging() -> None:
    """Ensure monitoring loggers are initialised.

    This currently just acquires a module-level logger, which is
    sufficient to trigger the base logging configuration in
    :mod:`prometheus.core.logging`.
    """

    logger = get_logger(__name__)
    logger.debug("Monitoring logging configured")
