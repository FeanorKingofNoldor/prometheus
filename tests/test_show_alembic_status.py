from pathlib import Path

import pytest

from prometheus.scripts.show.show_alembic_status import _get_alembic_heads, _read_db_version


class _DummyDb:
    pass


def test_get_alembic_heads_from_project_root() -> None:
    project_root = Path(__file__).resolve().parents[1]
    heads = _get_alembic_heads(project_root)

    assert heads
    assert all(isinstance(head, str) and head for head in heads)


def test_read_db_version_rejects_unknown_selector() -> None:
    with pytest.raises(ValueError):
        _read_db_version(_DummyDb(), "unknown")
