"""Shared test fixtures and apathis module stubs.

The apathis sibling package is not necessarily installed in the test
environment. This conftest registers lightweight stubs in sys.modules so
that ``prometheus.*`` modules can be imported without the real package.

The stubs are injected at session scope (before any test module import)
via an autouse fixture.
"""

from __future__ import annotations

import sys
import types
from dataclasses import dataclass
from typing import Any, Dict, Optional


# ---------------------------------------------------------------------------
# Stub logger
# ---------------------------------------------------------------------------

class _StubLogger:
    def debug(self, *a: Any, **kw: Any) -> None: ...
    def info(self, *a: Any, **kw: Any) -> None: ...
    def warning(self, *a: Any, **kw: Any) -> None: ...
    def error(self, *a: Any, **kw: Any) -> None: ...
    def exception(self, *a: Any, **kw: Any) -> None: ...


def _get_logger(_name: str) -> _StubLogger:
    return _StubLogger()


def _generate_uuid() -> str:
    import uuid
    return str(uuid.uuid4())


# ---------------------------------------------------------------------------
# Stub apathis modules — only if the real package is not importable
# ---------------------------------------------------------------------------

def _install_apathis_stubs() -> None:
    """Register fake apathis.* modules in sys.modules."""
    if "apathis" in sys.modules:
        # Already loaded (either real or previously stubbed) — skip.
        return

    # -- apathis --
    apathis = types.ModuleType("apathis")

    # -- apathis.core --
    core = types.ModuleType("apathis.core")

    # -- apathis.core.database --
    db = types.ModuleType("apathis.core.database")

    class _StubDatabaseManager:
        pass

    db.DatabaseManager = _StubDatabaseManager  # type: ignore[attr-defined]
    db.get_db_manager = lambda: _StubDatabaseManager()  # type: ignore[attr-defined]

    # -- apathis.core.logging --
    logging_mod = types.ModuleType("apathis.core.logging")
    logging_mod.get_logger = _get_logger  # type: ignore[attr-defined]

    # -- apathis.core.ids --
    ids_mod = types.ModuleType("apathis.core.ids")
    ids_mod.generate_uuid = _generate_uuid  # type: ignore[attr-defined]

    # -- apathis.core.types --
    types_mod = types.ModuleType("apathis.core.types")
    types_mod.MetadataDict = Dict[str, Any]  # type: ignore[attr-defined]

    # -- apathis.core.config --
    config_mod = types.ModuleType("apathis.core.config")

    class _StubExecutionRiskConfig:
        def __init__(self, enabled=True, max_order_notional=0.0,
                     max_position_notional=0.0, max_leverage=0.0):
            self.enabled = enabled
            self.max_order_notional = max_order_notional
            self.max_position_notional = max_position_notional
            self.max_leverage = max_leverage

    class _StubConfig:
        def __init__(self):
            self.execution_risk = _StubExecutionRiskConfig()

    config_mod.ExecutionRiskConfig = _StubExecutionRiskConfig  # type: ignore[attr-defined]
    config_mod.get_config = lambda: _StubConfig()  # type: ignore[attr-defined]

    # -- apathis.core.time --
    time_mod = types.ModuleType("apathis.core.time")

    class _StubTradingCalendar:
        def __init__(self, *a: Any, **kw: Any) -> None: ...
        def trading_days_between(self, start, end):
            return []
        def is_trading_day(self, _d) -> bool:
            return True

    class _StubTradingCalendarConfig:
        def __init__(self, market: str = "US_EQ") -> None:
            self.market = market

    time_mod.TradingCalendar = _StubTradingCalendar  # type: ignore[attr-defined]
    time_mod.TradingCalendarConfig = _StubTradingCalendarConfig  # type: ignore[attr-defined]
    time_mod.US_EQ = "US_EQ"  # type: ignore[attr-defined]

    # -- apathis.data --
    data = types.ModuleType("apathis.data")

    # -- apathis.data.reader --
    reader = types.ModuleType("apathis.data.reader")

    class _StubDataReader:
        pass

    reader.DataReader = _StubDataReader  # type: ignore[attr-defined]

    # -- apathis.stability --
    stability = types.ModuleType("apathis.stability")

    # -- apathis.stability.storage --
    stab_storage = types.ModuleType("apathis.stability.storage")

    class _StubStabilityStorage:
        pass

    stab_storage.StabilityStorage = _StubStabilityStorage  # type: ignore[attr-defined]

    # -- apathis.stability.types --
    stab_types = types.ModuleType("apathis.stability.types")

    from enum import Enum

    class SoftTargetClass(str, Enum):
        LOW = "LOW"
        MEDIUM = "MEDIUM"
        HIGH = "HIGH"
        CRITICAL = "CRITICAL"

    @dataclass(frozen=True)
    class SoftTargetState:
        as_of_date: Any = None
        entity_type: str = ""
        entity_id: str = ""
        soft_target_class: Any = SoftTargetClass.LOW
        soft_target_score: float = 0.0
        weak_profile: bool = False
        metadata: Optional[Dict[str, Any]] = None

    stab_types.SoftTargetState = SoftTargetState  # type: ignore[attr-defined]
    stab_types.SoftTargetClass = SoftTargetClass  # type: ignore[attr-defined]

    # -- psycopg2 stubs (needed by state.py) --
    if "psycopg2" not in sys.modules:
        pg2 = types.ModuleType("psycopg2")
        pg2_extras = types.ModuleType("psycopg2.extras")

        class _Json:
            def __init__(self, obj: Any) -> None:
                self.adapted = obj

        pg2_extras.Json = _Json  # type: ignore[attr-defined]
        sys.modules["psycopg2"] = pg2
        sys.modules["psycopg2.extras"] = pg2_extras

    # Wire parent/child relationships
    apathis.core = core  # type: ignore[attr-defined]
    apathis.data = data  # type: ignore[attr-defined]
    apathis.stability = stability  # type: ignore[attr-defined]
    core.database = db  # type: ignore[attr-defined]
    core.logging = logging_mod  # type: ignore[attr-defined]
    core.ids = ids_mod  # type: ignore[attr-defined]
    core.types = types_mod  # type: ignore[attr-defined]
    core.time = time_mod  # type: ignore[attr-defined]
    core.config = config_mod  # type: ignore[attr-defined]
    data.reader = reader  # type: ignore[attr-defined]
    stability.storage = stab_storage  # type: ignore[attr-defined]
    stability.types = stab_types  # type: ignore[attr-defined]

    # Register in sys.modules
    for name, mod in [
        ("apathis", apathis),
        ("apathis.core", core),
        ("apathis.core.database", db),
        ("apathis.core.logging", logging_mod),
        ("apathis.core.ids", ids_mod),
        ("apathis.core.types", types_mod),
        ("apathis.core.time", time_mod),
        ("apathis.core.config", config_mod),
        ("apathis.data", data),
        ("apathis.data.reader", reader),
        ("apathis.stability", stability),
        ("apathis.stability.storage", stab_storage),
        ("apathis.stability.types", stab_types),
    ]:
        sys.modules[name] = mod


# Install stubs at import time (before test module collection).
_install_apathis_stubs()
