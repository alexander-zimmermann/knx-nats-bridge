"""Unit tests for the knxproj-to-yaml extractor (no live xknxproject parse)."""

from __future__ import annotations

from typing import Any

from knx_nats_bridge.tools.knxproj_to_yaml import (
    _build_ga_to_function,
    _build_space_id_to_name,
    _extract,
)


def _project_data() -> dict[str, Any]:
    """Hand-built project_data that mirrors xknxproject's parse() shape."""
    return {
        "group_addresses": {
            "0/1/40": {
                "name": "Lighting.1F.Bedroom.Ceiling.Switch",
                "dpt": {"main": 1, "sub": 1},
                "description": "Switch ceiling light",
                "comment": "",
            },
            "0/2/10": {
                "name": "Sensors.1F.Bedroom.Temperature",
                "dpt": {"main": 9, "sub": 1},
                "description": "",
                "comment": "Bedroom temperature sensor",
            },
            "0/3/0": {
                # No Function reference -> stays in output without room/function.
                "name": "General.Central.Scenes",
                "dpt": {"main": 17, "sub": 1},
                "description": "",
                "comment": "",
            },
            "0/4/0": {
                # Missing DPT — should be dropped.
                "name": "Broken.Entry",
                "dpt": None,
            },
        },
        "spaces": {
            "building-1": {
                "name": "TestHouse",
                "type": "Building",
                "spaces": {
                    "floor-1f": {
                        "name": "1F",
                        "type": "Floor",
                        "spaces": {
                            "room-bedroom": {
                                "name": "Bedroom",
                                "type": "Room",
                                "identifier": "space-bedroom",
                                "spaces": {},
                            }
                        },
                    }
                },
            }
        },
        "functions": {
            "fn-light-bedroom": {
                "name": "Lighting Bedroom",
                "function_type": "FT-1",
                "space_id": "space-bedroom",
                "group_addresses": {"0/1/40": {}},
            },
            "fn-temp-bedroom": {
                "name": "Climate Bedroom",
                "function_type": "FT-7",
                "space_id": "space-bedroom",
                "group_addresses": {"0/2/10": {}},
            },
        },
    }


def test_extract_room_from_function_space_id() -> None:
    mapping: dict[str, Any] = {}
    _extract(mapping, _project_data())

    # Lighting GA: Function's space_id resolves to the logical room.
    assert mapping["0/1/40"] == {
        "name": "Lighting.1F.Bedroom.Ceiling.Switch",
        "dpt": "1.001",
        "room": "Bedroom",
        "function": "Lighting Bedroom",
        "description": "Switch ceiling light",
    }

    # Temp GA: same room, function name carries through. Description falls
    # back to `comment` when `description` is empty.
    assert mapping["0/2/10"] == {
        "name": "Sensors.1F.Bedroom.Temperature",
        "dpt": "9.001",
        "room": "Bedroom",
        "function": "Climate Bedroom",
        "description": "Bedroom temperature sensor",
    }


def test_extract_emits_minimal_entry_when_no_function() -> None:
    mapping: dict[str, Any] = {}
    _extract(mapping, _project_data())

    # No ETS Function reference -> only name + dpt. No name-parsing fallback;
    # consumers add room/function via their own enrichment step.
    assert mapping["0/3/0"] == {
        "name": "General.Central.Scenes",
        "dpt": "17.001",
    }


def test_extract_drops_ga_without_dpt() -> None:
    mapping: dict[str, Any] = {}
    _extract(mapping, _project_data())
    assert "0/4/0" not in mapping


def test_build_space_id_to_name_indexes_by_key_and_identifier() -> None:
    spaces = {
        "outer-key": {
            "name": "Building",
            "identifier": "ident-building",
            "spaces": {
                "inner-key": {
                    "name": "Room",
                    "identifier": "ident-room",
                    "spaces": {},
                }
            },
        }
    }
    result = _build_space_id_to_name(spaces)
    assert result["outer-key"] == "Building"
    assert result["ident-building"] == "Building"
    assert result["inner-key"] == "Room"
    assert result["ident-room"] == "Room"


def test_build_ga_to_function_resolves_room_via_space_id() -> None:
    functions = {
        "fn-a": {
            "name": "First",
            "space_id": "room-1",
            "group_addresses": {"1/2/3": {}},
        },
    }
    space_id_to_name = {"room-1": "Bedroom"}
    result = _build_ga_to_function(functions, space_id_to_name)
    assert result["1/2/3"] == {"name": "First", "room": "Bedroom"}


def test_build_ga_to_function_first_wins_when_duplicated() -> None:
    functions = {
        "fn-a": {"name": "First", "space_id": "r1", "group_addresses": {"1/2/3": {}}},
        "fn-b": {"name": "Second", "space_id": "r2", "group_addresses": {"1/2/3": {}}},
    }
    space_id_to_name = {"r1": "RoomA", "r2": "RoomB"}
    result = _build_ga_to_function(functions, space_id_to_name)
    assert result["1/2/3"] == {"name": "First", "room": "RoomA"}


def test_build_ga_to_function_handles_missing_space_id() -> None:
    functions = {
        "fn-a": {"name": "Standalone", "group_addresses": {"1/2/3": {}}},
    }
    result = _build_ga_to_function(functions, {})
    assert result["1/2/3"] == {"name": "Standalone", "room": None}
