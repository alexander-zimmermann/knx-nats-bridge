"""Writer-rules loader: YAML file -> validated list of NATS-subject -> KNX-GA rules."""

from __future__ import annotations

import json
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import jsonschema
import yaml
from xknx.dpt import DPTBase

_SCHEMA_PATH = Path(__file__).resolve().parent / "_schemas" / "writer-rules.schema.json"


@dataclass(frozen=True, slots=True)
class WriterRule:
    subject: str
    ga: str
    dpt: str
    payload_path: str
    description: str | None = None
    # Deadband to suppress bus-spamming jitter: only write when the value moved
    # by more than max(min_delta, min_delta_pct/100 * |last|). Both optional;
    # min_delta=0 (no pct) means "write only on change". See Writer._should_write.
    min_delta: float | None = None
    min_delta_pct: float | None = None


class WriterRules:
    def __init__(self, rules: list[WriterRule]) -> None:
        self._rules = rules
        self._by_subject: dict[str, list[WriterRule]] = {}
        for r in rules:
            self._by_subject.setdefault(r.subject, []).append(r)

    def __len__(self) -> int:
        return len(self._rules)

    def __iter__(self) -> Iterator[WriterRule]:
        return iter(self._rules)

    def subjects(self) -> list[str]:
        return list(self._by_subject.keys())

    def for_subject(self, subject: str) -> list[WriterRule]:
        return self._by_subject.get(subject, [])

    @classmethod
    def load(
        cls,
        path: Path,
        *,
        reader_subject_prefix: str | None = None,
        schema_path: Path | None = None,
    ) -> WriterRules:
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

        rules: list[WriterRule] = []
        for raw in data.get("mappings", []):
            dpt = raw["dpt"]
            if DPTBase.parse_transcoder(dpt) is None:
                raise ValueError(f"{path}: unknown DPT {dpt!r} in rule for {raw['subject']!r}")

            subject = raw["subject"]
            # Loop-protection: a writer subscribed to the reader's own publish-prefix
            # would re-trigger itself via the bus echo. Reject at load time.
            if reader_subject_prefix and (
                subject == reader_subject_prefix or subject.startswith(reader_subject_prefix + ".")
            ):
                raise ValueError(
                    f"{path}: subject {subject!r} falls under reader prefix "
                    f"{reader_subject_prefix!r} — would create a write/read loop"
                )

            rules.append(
                WriterRule(
                    subject=subject,
                    ga=raw["ga"],
                    dpt=dpt,
                    payload_path=raw["payload_path"],
                    description=raw.get("description"),
                    min_delta=raw.get("min_delta"),
                    min_delta_pct=raw.get("min_delta_pct"),
                )
            )

        return cls(rules)


def extract_value(payload: Any, path: str) -> Any:
    """Resolve a `$.field.subfield` path against a JSON-decoded payload.

    `$` alone returns the root. Missing fields raise KeyError so callers can
    decide whether to drop or warn — silently returning None would mask typos
    in the mapping file.
    """
    if not path.startswith("$"):
        raise ValueError(f"payload_path must start with '$', got {path!r}")
    tail = path[1:]
    if tail == "":
        return payload
    parts = tail.lstrip(".").split(".")
    cur: Any = payload
    for part in parts:
        if not isinstance(cur, dict):
            raise KeyError(f"cannot descend into {part!r}: parent is {type(cur).__name__}")
        if part not in cur:
            raise KeyError(part)
        cur = cur[part]
    return cur
