"""Prometheus v2 – shared environment-variable parsing helpers.

Several modules grew their own ad-hoc boolean env parsers with subtly
divergent semantics (``in ("1", "true")`` vs. ``in ("1", "true", "yes",
"on")`` vs. treating any non-empty string as true).  This module is the
single canonical implementation; new code should use :func:`env_flag`
instead of hand-rolling ``os.environ.get(...)`` comparisons.
"""

from __future__ import annotations

import os

from apatheon.core.logging import get_logger

logger = get_logger(__name__)

_TRUE_TOKENS = frozenset({"1", "true", "yes", "on"})
_FALSE_TOKENS = frozenset({"0", "false", "no", "off", ""})


def env_flag(name: str, default: bool = False) -> bool:
    """Parse a boolean flag from the environment.

    Truthy values (case-insensitive): ``1``, ``true``, ``yes``, ``on``.
    Falsy values (case-insensitive): ``0``, ``false``, ``no``, ``off``,
    and the empty string.

    An unset variable returns ``default``.  An unrecognized token logs a
    warning and returns ``default`` so a typo (e.g. ``ture``) is loud
    rather than silently flipping a production flag.

    Args:
        name: Environment variable name.
        default: Value returned when the variable is unset or malformed.

    Returns:
        The parsed boolean.
    """
    raw = os.environ.get(name)
    if raw is None:
        return default
    return parse_flag_token(raw, default=default, name=name)


def parse_flag_token(raw: str | None, *, default: bool = False, name: str | None = None) -> bool:
    """Parse an already-fetched flag value with :func:`env_flag` semantics.

    For call sites that receive the raw string (e.g. from a dict of env
    vars) rather than reading ``os.environ`` themselves.
    """
    if raw is None:
        return default

    token = raw.strip().lower()
    if token in _TRUE_TOKENS:
        return True
    if token in _FALSE_TOKENS:
        return False

    logger.warning(
        "env_flag: %s=%r is not a recognized boolean token "
        "(expected one of 1/true/yes/on or 0/false/no/off) — using default=%s",
        name or "<flag>", raw, default,
    )
    return default


__all__ = ["env_flag", "parse_flag_token"]
