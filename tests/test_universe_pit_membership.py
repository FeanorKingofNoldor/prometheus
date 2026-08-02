"""Survivorship / determinism guards for the universe engine.

1. ``_enumerate_instruments`` must include DELISTED equities (not only
   ACTIVE) so historical universes are not survivorship-biased; PIT
   tradability is then enforced by the price-on-as_of_date prefilter.
   A one-shot WARNING documents that this relies on the prefilter because
   the instruments table has no listing/delisting date columns.
2. ``_load_assessment_scores`` must deduplicate duplicate score rows
   deterministically (latest ``created_at`` wins) via ``DISTINCT ON``.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date

import prometheus.universe.engine as engine_mod
from prometheus.universe.engine import BasicUniverseModel


class _RecordingCursor:
    def __init__(self, sink: list, rows) -> None:
        self._sink = sink
        self._rows = rows

    def execute(self, sql, params=None):
        self._sink.append(str(sql))

    def fetchall(self):
        return self._rows

    def close(self):
        pass


class _Conn:
    def __init__(self, sink, rows) -> None:
        self._sink = sink
        self._rows = rows

    def cursor(self):
        return _RecordingCursor(self._sink, self._rows)


def _db(sink, rows):
    class _DB:
        @contextmanager
        def get_runtime_connection(self):
            yield _Conn(sink, rows)

    return _DB()


def _model(sink, rows) -> BasicUniverseModel:
    return BasicUniverseModel(
        db_manager=_db(sink, rows),
        calendar=None,
        data_reader=None,
        profile_service=None,
        stability_storage=None,
        market_ids=["US_EQ"],
    )


def test_enumeration_includes_delisted_and_warns(caplog) -> None:
    engine_mod._SURVIVORSHIP_WARNED[0] = False  # reset one-shot guard
    sink: list = []
    rows = [("LEH.US", "LEH", "Financials", "US_EQ", "issuer_classifications")]
    with caplog.at_level("WARNING"):
        members = _model(sink, rows)._enumerate_instruments(date(2008, 6, 1))

    sql = "\n".join(sink)
    assert "status IN ('ACTIVE', 'DELISTED')" in sql
    assert members and members[0][0] == "LEH.US"
    assert any("survivorship" in r.message.lower() or "DELISTED" in r.message
               for r in caplog.records)


def test_assessment_scores_read_is_deterministic() -> None:
    sink: list = []
    # DISTINCT ON returns one row per instrument; the SQL ordering
    # (created_at DESC) is what makes the latest write win. The mock returns
    # the already-deduplicated row so we assert the query shape + result.
    rows = [("AAA.US", 0.42)]
    model = _model(sink, rows)
    model.use_assessment_scores = True
    model.assessment_strategy_id = "S"
    model.assessment_horizon_days = 21

    scores = model._load_assessment_scores(date(2026, 1, 5))

    sql = "\n".join(sink)
    assert "DISTINCT ON (instrument_id)" in sql
    assert "ORDER BY instrument_id, created_at DESC" in sql
    assert scores == {"AAA.US": 0.42}
