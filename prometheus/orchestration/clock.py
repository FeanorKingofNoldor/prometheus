"""Timezone-aware clock helpers for daemon + monitoring code.

The daemon and monitoring schedulers historically used naive
``datetime.now()`` and ``datetime.utcnow()`` calls. That breaks in two
ways:

1. ``datetime.utcnow()`` is deprecated as of Python 3.12 and will be
   removed in 3.15.
2. Naive ``datetime.now()`` returns local wall time without an attached
   tzinfo, so PostgreSQL stores it as TZ-naive and downstream consumers
   guess the offset (often wrongly).

This module gives the daemon two explicit, timezone-aware primitives:

- :func:`now_utc` — UTC time with ``tzinfo`` attached. Use for *every*
  timestamp written to the database or persisted in JSON.
- :func:`now_local` — Local wall time with ``tzinfo`` attached. Use for
  scheduling decisions ("run at 22:00 local"). The local zone is read
  from the ``PROMETHEUS_LOCAL_TZ`` env var (default
  ``Europe/Berlin``); production runs in CET/CEST.

Both return :class:`datetime.datetime` instances that are
``tzinfo``-aware so arithmetic and comparison are unambiguous.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone, tzinfo
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

# Read once at import time. The env override is for tests / non-CET deploys.
_LOCAL_TZ_NAME = os.environ.get("PROMETHEUS_LOCAL_TZ", "Europe/Berlin")
LOCAL_TZ: tzinfo
try:
    LOCAL_TZ = ZoneInfo(_LOCAL_TZ_NAME)
except ZoneInfoNotFoundError:
    # Fallback: if tzdata is missing on the host, degrade to UTC rather
    # than crashing the daemon at import.
    LOCAL_TZ = timezone.utc


def now_utc() -> datetime:
    """Return the current time in UTC with ``tzinfo=timezone.utc``."""
    return datetime.now(timezone.utc)


def now_local() -> datetime:
    """Return the current time in the configured local timezone.

    Used for scheduler hour-of-day comparisons. The result is
    ``tzinfo``-aware so subsequent arithmetic stays unambiguous.
    """
    return datetime.now(LOCAL_TZ)
