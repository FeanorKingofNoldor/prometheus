"""Prometheus v2 – Monitoring metrics stubs.

This module provides a very small, in-process metrics API that can be
used by other components to emit counters and gauges. It is intentionally
minimal and does *not* depend on any external metrics backend; values are
kept in memory and can optionally be exposed via the monitoring web API
in a later pass.

The design is backend-agnostic so that a real metrics sink (Prometheus,
StatsD, etc.) can be plugged in without changing call sites.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from threading import Lock
from typing import Iterable, Mapping, MutableMapping, Optional

from apathis.core.logging import get_logger

logger = get_logger(__name__)


@dataclass
class MetricPoint:
    """Single metric observation.

    Attributes:
        name: Metric name (e.g. "ingestion.rows_loaded").
        value: Numeric value.
        tags: Optional tag mapping (e.g. {"feed": "prices_daily"}).
        timestamp: UTC timestamp of the observation.
    """

    name: str
    value: float
    tags: Mapping[str, str] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


# Simple in-memory store of the latest value per (name, sorted_tags_key).
_latest_metrics: MutableMapping[tuple[str, tuple[tuple[str, str], ...]], MetricPoint] = {}
_lock = Lock()


def _normalise_tags(tags: Optional[Mapping[str, str]]) -> tuple[tuple[str, str], ...]:
    if not tags:
        return ()
    return tuple(sorted((str(k), str(v)) for k, v in tags.items()))


def record_metric(name: str, value: float, tags: Optional[Mapping[str, str]] = None) -> None:
    """Record a metric value.

    For now this stores only the latest value per (name, tags) combination
    in memory. Later this function can be extended to forward metrics to an
    external backend without changing call sites.
    """

    key = (name, _normalise_tags(tags))
    point = MetricPoint(name=name, value=float(value), tags=dict(tags or {}))
    with _lock:
        _latest_metrics[key] = point
    logger.debug("metric recorded", extra={"metric_name": name, "value": value, "tags": tags or {}})


def get_latest_metrics(prefix: str | None = None) -> Iterable[MetricPoint]:
    """Return the latest recorded metrics, optionally filtered by prefix."""

    with _lock:
        points = list(_latest_metrics.values())

    if prefix is None:
        return points
    return [p for p in points if p.name.startswith(prefix)]


def reset_metrics() -> None:
    """Clear all in-memory metrics (useful in tests)."""

    with _lock:
        _latest_metrics.clear()
