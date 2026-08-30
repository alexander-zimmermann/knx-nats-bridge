"""Event payload validation against the bundled JSON schema."""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import jsonschema

_EVENT_SCHEMA_PATH = Path(__file__).resolve().parent / "_schemas" / "event.schema.json"


def event_validator() -> Callable[[dict[str, Any]], None] | None:
    """Validator for the publisher, or None when the schema is not bundled."""
    if not _EVENT_SCHEMA_PATH.exists():
        return None
    schema = json.loads(_EVENT_SCHEMA_PATH.read_text(encoding="utf-8"))

    def validate(payload: dict[str, Any]) -> None:
        jsonschema.validate(instance=payload, schema=schema)

    return validate
