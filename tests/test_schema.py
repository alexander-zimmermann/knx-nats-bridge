"""Event schema: what the bundled schema accepts and rejects."""

from __future__ import annotations

from typing import Any

import jsonschema
import pytest

from knx_nats_bridge.schema import event_validator


def _event(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "ga": "1/2/3",
        "name": "Licht Flur",
        "dpt": "1.001",
        "value": True,
        "source": "1.1.5",
        "ts": "2026-01-01T00:00:00.000000Z",
    }
    payload.update(overrides)
    return payload


def test_validator_is_available() -> None:
    assert event_validator() is not None


def test_object_value_is_rejected() -> None:
    validate = event_validator()
    assert validate is not None
    with pytest.raises(jsonschema.ValidationError):
        validate(_event(value={"nested": 1}))


def test_raw_array_value_is_accepted() -> None:
    # The RAW unmapped policy emits list[int] values; the schema must accept them.
    validate = event_validator()
    assert validate is not None
    validate(_event(dpt="0.000", value=[12, 26]))
