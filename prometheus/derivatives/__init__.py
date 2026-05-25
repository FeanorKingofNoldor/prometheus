"""Prometheus derivatives layer (Phase 0+).

New home for the unified options selection / sizing / lifecycle work.
The existing per-strategy classes in ``prometheus.execution.options_strategy``
remain authoritative during the migration; modules added here are
introduced one at a time and tested in isolation before any wiring
changes.
"""
