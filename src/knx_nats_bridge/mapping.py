"""KNX-catalog loader: YAML file -> validated {GA -> entry} lookup table."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import jsonschema
import yaml

_SCHEMA_PATH = Path(__file__).resolve().parent / "_schemas" / "knx-catalog.schema.json"


@dataclass(frozen=True, slots=True)
class GAEntry:
    name: str
    dpt: str  # "<main>.<sub>", e.g. "9.001"
    room: str | None = None
    function: str | None = None
    description: str | None = None


class GroupAddressMapping:
    def __init__(self, entries: dict[str, GAEntry]) -> None:
        self._entries = entries

    def __len__(self) -> int:
        return len(self._entries)

    def __contains__(self, ga: str) -> bool:
        return ga in self._entries

    def get(self, ga: str) -> GAEntry | None:
        return self._entries.get(ga)

    @classmethod
    def load(cls, path: Path, schema_path: Path | None = None) -> GroupAddressMapping:
        raw_text = path.read_text(encoding="utf-8")
        data: Any = yaml.safe_load(raw_text) or {}
        if not isinstance(data, dict):
            raise ValueError(
                f"{path}: expected a mapping at the top level, got {type(data).__name__}"
            )

        schema_file = schema_path or _SCHEMA_PATH
        if schema_file.exists():
            schema = json.loads(schema_file.read_text(encoding="utf-8"))
            jsonschema.validate(instance=data, schema=schema)

        entries: dict[str, GAEntry] = {}
        for ga, entry in data.items():
            if not isinstance(entry, dict):
                raise ValueError(f"{path}: entry for {ga!r} must be an object")
            name = entry.get("name")
            dpt = entry.get("dpt")
            if not isinstance(name, str) or not isinstance(dpt, str):
                raise ValueError(f"{path}: entry for {ga!r} requires string 'name' and 'dpt'")
            entries[ga] = GAEntry(
                name=name,
                dpt=dpt,
                room=entry.get("room"),
                function=entry.get("function"),
                description=entry.get("description"),
            )

        return cls(entries)
