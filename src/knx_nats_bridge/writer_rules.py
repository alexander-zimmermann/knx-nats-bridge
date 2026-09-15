"""Writer-rules loader: YAML file or directory -> validated NATS-subject -> KNX-GA rules."""

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
    # On startup, prime the read responder by loading this GA's last value from
    # JetStream into the writer's cache. Only useful for event-driven subjects
    # whose value otherwise wouldn't arrive again until the next change (e.g.
    # warp.evse.state). Requires the subject to be JetStream-backed.
    seed_on_start: bool = False


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

    def seed_subjects(self) -> list[str]:
        """Distinct subjects with at least one rule flagged seed_on_start."""
        return [
            subject
            for subject, rules in self._by_subject.items()
            if any(r.seed_on_start for r in rules)
        ]

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
        """Load one YAML file, or every `*.yaml` in a directory (sorted by name) as one set.

        Each file is parsed and validated on its own so an error names the file;
        a group address claimed by more than one rule is rejected across the set.
        """
        schema_file = schema_path or _SCHEMA_PATH
        schema: dict[str, Any] | None = (
            json.loads(schema_file.read_text(encoding="utf-8")) if schema_file.exists() else None
        )

        rules: list[WriterRule] = []
        claimed_by: dict[str, list[Path]] = {}
        for file in _rule_files(path):
            for rule in _parse_file(file, schema, reader_subject_prefix):
                claimed_by.setdefault(rule.ga, []).append(file)
                rules.append(rule)

        duplicates = {ga: files for ga, files in claimed_by.items() if len(files) > 1}
        if duplicates:
            detail = "; ".join(
                f"{ga} ({len(files)} rules) in {', '.join(dict.fromkeys(f.name for f in files))}"
                for ga, files in duplicates.items()
            )
            raise ValueError(f"{path}: group address claimed by more than one rule: {detail}")

        return cls(rules)


def _rule_files(path: Path) -> list[Path]:
    """The files to load: the path itself, or the `*.yaml` files of a directory."""
    if not path.is_dir():
        return [path]
    files = sorted(p for p in path.iterdir() if p.is_file() and p.suffix == ".yaml")
    if not files:
        raise FileNotFoundError(f"{path}: no *.yaml rule files in directory")
    return files


def _parse_file(
    path: Path, schema: dict[str, Any] | None, reader_subject_prefix: str | None
) -> list[WriterRule]:
    raw_text = path.read_text(encoding="utf-8")
    data: Any = yaml.safe_load(raw_text) or {}
    if not isinstance(data, dict):
        raise ValueError(f"{path}: expected a mapping at the top level, got {type(data).__name__}")

    if schema is not None:
        try:
            jsonschema.validate(instance=data, schema=schema)
        except jsonschema.ValidationError as exc:
            exc.message = f"{path}: {exc.message}"
            raise

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
                seed_on_start=raw.get("seed_on_start", False),
            )
        )
    return rules


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
