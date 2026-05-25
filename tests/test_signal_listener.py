"""Tests for prometheus.orchestration.signal_listener (parse only).

The full listener loop is integration-tested against a real DB; here we
exercise the pure-logic path: payload parsing + alert dispatch.
"""

from __future__ import annotations

import json

import pytest

from prometheus.orchestration.signal_listener import (
    SignalAlert,
    SignalAlertListener,
    parse_payload,
)


def test_parse_valid_divergence_payload() -> None:
    payload = json.dumps({
        "source": "divergence",
        "severity": "EXTREME",
        "entity_type": "chokepoint",
        "entity_id": "hormuz",
        "as_of_date": "2026-05-05",
        "trading_signal": "FRONT_RUN_REALITY",
        "decision_id": "abc-123",
    })
    alert = parse_payload(payload)
    assert alert is not None
    assert alert.source == "divergence"
    assert alert.severity == "EXTREME"
    assert alert.entity_type == "chokepoint"
    assert alert.entity_id == "hormuz"
    assert alert.trading_signal == "FRONT_RUN_REALITY"
    assert alert.decision_id == "abc-123"


def test_parse_valid_compound_payload() -> None:
    payload = json.dumps({
        "source": "compound_pressure",
        "severity": "CRITICAL",
        "entity_type": "SOVEREIGN",
        "entity_id": "IRN",
        "as_of_date": "2026-05-05",
        "encirclement_score": 0.92,
        "decision_id": "xyz-456",
    })
    alert = parse_payload(payload)
    assert alert is not None
    assert alert.encirclement_score == pytest.approx(0.92)


def test_parse_drops_non_json() -> None:
    assert parse_payload("not json") is None


def test_parse_drops_empty() -> None:
    assert parse_payload("") is None


def test_parse_drops_non_object() -> None:
    assert parse_payload(json.dumps([1, 2, 3])) is None


def test_parse_drops_missing_required_field() -> None:
    payload = json.dumps({
        "source": "divergence",
        # missing severity / entity_type / entity_id
    })
    assert parse_payload(payload) is None


def test_dispatch_swallows_callback_exception() -> None:
    """A buggy on_alert handler must not crash the listener."""
    received: list[SignalAlert] = []

    def _good(alert: SignalAlert) -> None:
        received.append(alert)

    def _bad(alert: SignalAlert) -> None:
        raise RuntimeError("boom")

    listener = SignalAlertListener(db_manager=None, on_alert=_bad)  # type: ignore[arg-type]
    # _dispatch is the internal method that handles parse + callback;
    # exceptions from the callback must be caught.
    listener._dispatch(json.dumps({
        "source": "divergence",
        "severity": "EXTREME",
        "entity_type": "chokepoint",
        "entity_id": "hormuz",
        "as_of_date": "2026-05-05",
    }))
    # No raise = pass.

    # Sanity: the good handler does receive the alert
    listener_ok = SignalAlertListener(db_manager=None, on_alert=_good)  # type: ignore[arg-type]
    listener_ok._dispatch(json.dumps({
        "source": "compound_pressure",
        "severity": "CRITICAL",
        "entity_type": "SOVEREIGN",
        "entity_id": "IRN",
        "as_of_date": "2026-05-05",
    }))
    assert len(received) == 1
    assert received[0].entity_id == "IRN"


def test_dispatch_drops_malformed_payload_silently() -> None:
    """Bad payloads are dropped without invoking the callback."""
    received: list[SignalAlert] = []

    listener = SignalAlertListener(
        db_manager=None,  # type: ignore[arg-type]
        on_alert=lambda a: received.append(a),
    )
    listener._dispatch("definitely not json")
    assert received == []
