"""Prometheus v2 – In-Memory Log Buffer.

Attaches a handler to the root Python logger that captures entries into
a bounded deque.  The C2 API reads from this buffer for the live log
viewer — no DB writes needed for every log line.

Usage::

    from prometheus.monitoring.log_buffer import install_buffer, get_logs

    install_buffer()  # call once at startup
    entries = get_logs(level="ERROR", limit=50)
"""

from __future__ import annotations

import logging
import re
from collections import deque
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

# ── Entry dataclass ──────────────────────────────────────────────────

@dataclass(frozen=True)
class LogEntry:
    timestamp: str
    level: str
    category: str   # logger name (e.g. "prometheus.pipeline.tasks")
    source: str      # module:lineno
    message: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ── Ring buffer ──────────────────────────────────────────────────────

_BUFFER: deque[LogEntry] = deque(maxlen=10_000)

# Category extraction: take the first two dotted segments.
_CAT_RE = re.compile(r"^([^.]+(?:\.[^.]+)?)")


class BufferHandler(logging.Handler):
    """Logging handler that writes records into the in-memory ring buffer."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            ts = datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat()
            m = _CAT_RE.match(record.name or "")
            category = m.group(1) if m else record.name
            source = f"{record.module}:{record.lineno}"
            entry = LogEntry(
                timestamp=ts,
                level=record.levelname,
                category=category,
                source=source,
                message=self.format(record),
            )
            _BUFFER.append(entry)
        except Exception:
            pass  # never let logging handler crash the app


_installed = False


def install_buffer(level: int = logging.DEBUG) -> None:
    """Attach the buffer handler to the root logger (idempotent)."""
    global _installed
    if _installed:
        return
    handler = BufferHandler()
    handler.setLevel(level)
    handler.setFormatter(logging.Formatter("%(message)s"))
    logging.getLogger().addHandler(handler)
    _installed = True


# ── Query API ────────────────────────────────────────────────────────

def get_logs(
    *,
    level: Optional[str] = None,
    category: Optional[str] = None,
    search: Optional[str] = None,
    since: Optional[str] = None,
    limit: int = 200,
) -> List[Dict[str, Any]]:
    """Return log entries from the buffer, newest first.

    Filters:
        level:    exact match (INFO, WARNING, ERROR, DEBUG)
        category: substring match on the category field
        search:   substring match on the message
        since:    ISO timestamp — only entries after this time
    """
    _LEVEL_RANK = {"DEBUG": 0, "INFO": 1, "WARNING": 2, "ERROR": 3, "CRITICAL": 4}
    results: List[Dict[str, Any]] = []
    min_rank = _LEVEL_RANK.get(level.upper(), 0) if level else 0

    for entry in reversed(_BUFFER):
        if min_rank and _LEVEL_RANK.get(entry.level, 0) < min_rank:
            continue
        if category and category.lower() not in entry.category.lower():
            continue
        if search and search.lower() not in entry.message.lower():
            continue
        if since and entry.timestamp < since:
            break  # buffer is chronological, so we can stop
        results.append(entry.to_dict())
        if len(results) >= limit:
            break

    return results


def get_categories() -> List[str]:
    """Return unique categories currently in the buffer."""
    cats = set()
    for entry in _BUFFER:
        cats.add(entry.category)
    return sorted(cats)
