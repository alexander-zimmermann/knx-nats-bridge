from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import jsonschema
import pytest

SCHEMA = json.loads(
    (
        Path(__file__).resolve().parents[1]
        / "src"
        / "knx_nats_bridge"
        / "_schemas"
        / "event.schema.json"
    ).read_text(encoding="utf-8")
)


_VALID_PAYLOADS: list[dict[str, Any]] = [
    {
        "ga": "1/2/3",
        "name": "X",
        "dpt": "1.001",
        "value": True,
        "ts": "2026-04-22T12:34:56.789Z",
    },
    {
        "ga": "0/0/0",
        "name": "X",
        "dpt": "9.001",
        "value": 21.5,
        "ts": "2026-04-22T12:34:56Z",
    },
    {
        "ga": "3/0/1",
        "name": "E",
        "dpt": "13.013",
        "value": 12345,
        "ts": "2026-04-22T12:34:56.123456789Z",
    },
]

_INVALID_PAYLOADS: list[dict[str, Any]] = [
    {"ga": "1.2.3", "name": "X", "dpt": "1.001", "value": True, "ts": "2026-04-22T12:34:56Z"},
    {"ga": "1/2/3", "dpt": "1.001", "value": True, "ts": "2026-04-22T12:34:56Z"},
    {"ga": "1/2/3", "name": "X", "dpt": "1-001", "value": True, "ts": "2026-04-22T12:34:56Z"},
]


@pytest.mark.parametrize("payload", _VALID_PAYLOADS)
def test_valid_payload(payload: dict[str, Any]) -> None:
    jsonschema.validate(instance=payload, schema=SCHEMA)


@pytest.mark.parametrize("payload", _INVALID_PAYLOADS)
def test_invalid_payload(payload: dict[str, Any]) -> None:
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(instance=payload, schema=SCHEMA)
