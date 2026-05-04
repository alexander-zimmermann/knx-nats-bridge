from __future__ import annotations

from pathlib import Path

import jsonschema
import pytest

from knx_nats_bridge.mapping import GroupAddressMapping


def _write(tmp_path: Path, body: str) -> Path:
    p = tmp_path / "knx-catalog.yaml"
    p.write_text(body, encoding="utf-8")
    return p


def test_loads_valid_mapping(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
        "1/2/3":
          name: "Hallway light"
          dpt: "1.001"
        "2/1/5":
          name: "Living room temperature"
          dpt: "9.001"
        """,
    )
    mapping = GroupAddressMapping.load(path)
    assert len(mapping) == 2

    entry = mapping.get("1/2/3")
    assert entry is not None
    assert entry.name == "Hallway light"
    assert entry.dpt == "1.001"
    # Optional metadata fields default to None when absent.
    assert entry.room is None
    assert entry.function is None
    assert entry.description is None

    assert mapping.get("9/9/9") is None
    assert "2/1/5" in mapping


def test_loads_full_metadata(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
        "0/1/40":
          name: "Lighting.1F.Bedroom.Ceiling.Switch"
          dpt: "1.001"
          room: "Bedroom"
          function: "Lighting"
          description: "Switch ceiling light"
        """,
    )
    mapping = GroupAddressMapping.load(path)
    entry = mapping.get("0/1/40")
    assert entry is not None
    assert entry.room == "Bedroom"
    assert entry.function == "Lighting"
    assert entry.description == "Switch ceiling light"


def test_rejects_unknown_field(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        '"1/2/3": { name: "x", dpt: "1.001", floor: "OG" }\n',
    )
    with pytest.raises(jsonschema.ValidationError):
        GroupAddressMapping.load(path)


def test_rejects_missing_dpt(tmp_path: Path) -> None:
    path = _write(tmp_path, '"1/2/3": { name: "x" }\n')
    with pytest.raises(jsonschema.ValidationError):
        GroupAddressMapping.load(path)


def test_rejects_invalid_ga_format(tmp_path: Path) -> None:
    path = _write(tmp_path, '"1.2.3": { name: "x", dpt: "1.001" }\n')
    with pytest.raises(jsonschema.ValidationError):
        GroupAddressMapping.load(path)


def test_rejects_top_level_list(tmp_path: Path) -> None:
    path = _write(tmp_path, "- not: a mapping\n")
    with pytest.raises(ValueError, match="expected a mapping"):
        GroupAddressMapping.load(path)
