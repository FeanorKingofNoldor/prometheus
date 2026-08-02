"""Economic-event calendar — IV-crush-aware trigger guards.

See ``event_calendar`` for the per-event date functions and per-symbol
sensitivity map. Designed so trigger functions can ask a single
question: "is now too close to an IV-sensitive event for this
underlying?"
"""

from prometheus.calendar.event_calendar import (
    EconomicEvent,
    days_to_iv_event,
    near_iv_event,
    next_iv_events,
    upcoming_events,
)

__all__ = [
    "EconomicEvent",
    "days_to_iv_event",
    "next_iv_events",
    "near_iv_event",
    "upcoming_events",
]
