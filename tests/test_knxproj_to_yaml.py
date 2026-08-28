"""Unit tests for the knxproj-to-yaml extractor (no live xknxproject parse)."""

from __future__ import annotations

from typing import Any

from knx_nats_bridge.tools.knxproj_to_yaml import (
    _build_ga_to_function,
    _build_space_id_to_name,
    _extract,
    _has_write_flag,
    _is_writable,
    _writable_provenance,
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
                # Actuator input: carries the Write flag.
                "communication_object_ids": ["co-actuator"],
            },
            "0/2/10": {
                "name": "Sensors.1F.Bedroom.Temperature",
                "dpt": {"main": 9, "sub": 1},
                "description": "",
                "comment": "Bedroom temperature sensor",
                # Sensor output: send-only, but a filter-table placeholder
                # also receives it — which must not make it writable.
                "communication_object_ids": ["co-sensor", "co-placeholder"],
            },
            "0/3/0": {
                # No Function reference -> stays in output without room/function.
                "name": "General.Central.Scenes",
                "dpt": {"main": 17, "sub": 1},
                "description": "",
                "comment": "",
                "communication_object_ids": [],
            },
            "0/4/0": {
                # Missing DPT — should be dropped.
                "name": "Broken.Entry",
                "dpt": None,
            },
            "0/5/0": {
                # Several objects, only one of which receives.
                "name": "Mixed.Entry",
                "dpt": {"main": 1, "sub": 1},
                "communication_object_ids": ["co-sensor", "co-actuator"],
            },
            "0/5/1": {
                # Dangling reference: application program never loaded.
                "name": "Dangling.Entry",
                "dpt": {"main": 1, "sub": 1},
                "communication_object_ids": ["co-does-not-exist"],
            },
        },
        "communication_objects": {
            "co-actuator": {
                "device_address": "1.1.1",
                "flags": {
                    "read": False,
                    "write": True,
                    "communication": True,
                    "transmit": False,
                    "update": False,
                    "read_on_init": False,
                },
            },
            "co-placeholder": {
                "device_address": "1.1.9",
                "flags": {
                    "read": False,
                    "write": True,
                    "communication": True,
                    "transmit": False,
                    "update": False,
                    "read_on_init": False,
                },
            },
            "co-sensor": {
                "device_address": "1.1.2",
                "flags": {
                    "read": True,
                    "write": False,
                    "communication": True,
                    "transmit": True,
                    "update": False,
                    "read_on_init": False,
                },
            },
        },
        "devices": {
            "1.1.1": {"manufacturer_name": "ACME", "hardware_name": "Switch Actuator"},
            "1.1.2": {"manufacturer_name": "ACME", "hardware_name": "Temp Sensor"},
            "1.1.9": {
                "name": "Visualisation placeholder",
                "manufacturer_name": "GIRA Giersiepen",
                "hardware_name": "Dummy",
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
        "writable": True,
        "room": "Bedroom",
        "function": "Lighting Bedroom",
        "description": "Switch ceiling light",
    }

    # Temp GA: same room, function name carries through. Description falls
    # back to `comment` when `description` is empty.
    assert mapping["0/2/10"] == {
        "name": "Sensors.1F.Bedroom.Temperature",
        "dpt": "9.001",
        # A placeholder device receives it, so the naive rule says writable.
        "writable": True,
        "room": "Bedroom",
        "function": "Climate Bedroom",
        "description": "Bedroom temperature sensor",
    }


def test_extract_emits_minimal_entry_when_no_function() -> None:
    mapping: dict[str, Any] = {}
    _extract(mapping, _project_data())

    # No ETS Function reference -> only the always-present fields. No
    # name-parsing fallback; consumers add room/function via enrichment.
    assert mapping["0/3/0"] == {
        "name": "General.Central.Scenes",
        "dpt": "17.001",
        "writable": False,
    }


def test_extract_drops_ga_without_dpt() -> None:
    mapping: dict[str, Any] = {}
    _extract(mapping, _project_data())
    assert "0/4/0" not in mapping


def test_extract_writable_when_any_linked_object_receives() -> None:
    mapping: dict[str, Any] = {}
    _extract(mapping, _project_data())
    # Send-only object plus a receiving one -> the receiving one decides.
    assert mapping["0/5/0"]["writable"] is True


def test_extract_dangling_communication_object_is_not_writable() -> None:
    mapping: dict[str, Any] = {}
    _extract(mapping, _project_data())
    # Id listed on the GA but absent from the project: no crash, not writable.
    assert mapping["0/5/1"]["writable"] is False


def test_extract_without_communication_objects_section() -> None:
    data = _project_data()
    del data["communication_objects"]

    mapping: dict[str, Any] = {}
    _extract(mapping, data)

    assert mapping  # still extracts everything else
    assert all(entry["writable"] is False for entry in mapping.values())


def test_is_writable_rules() -> None:
    objects = {
        "w": {"flags": {"write": True}},
        "t": {"flags": {"write": False, "transmit": True}},
    }
    assert _is_writable(["w"], objects) is True
    assert _is_writable(["t"], objects) is False
    assert _is_writable(["t", "w"], objects) is True
    assert _is_writable([], objects) is False
    assert _is_writable(None, objects) is False
    assert _is_writable(["missing"], objects) is False


def test_has_write_flag_tolerates_malformed_objects() -> None:
    assert _has_write_flag(None) is False
    assert _has_write_flag({}) is False
    assert _has_write_flag({"flags": None}) is False
    assert _has_write_flag({"flags": {}}) is False
    assert _has_write_flag({"flags": {"write": True}}) is True


def test_writable_provenance_names_the_supplying_device() -> None:
    mapping: dict[str, Any] = {}
    data = _project_data()
    _extract(mapping, data)

    provenance = _writable_provenance(mapping, data)

    # 0/1/40 and 0/5/0 got theirs from the actuator, 0/2/10 from the
    # placeholder — which is exactly what makes the placeholder visible.
    assert provenance == [
        ("1.1.1 (ACME Switch Actuator)", 2),
        ("1.1.9 (GIRA Giersiepen Dummy)", 1),
    ]


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


def test_ignore_write_from_excludes_placeholder_devices() -> None:
    """A device that receives without acting must not mark a GA writable."""
    mapping: dict[str, Any] = {}
    _extract(mapping, _project_data(), ignore_write_from=["dummy"])

    # Sensor GA: only the placeholder received it -> no longer writable.
    assert mapping["0/2/10"]["writable"] is False
    # Real actuator still counts.
    assert mapping["0/1/40"]["writable"] is True


def test_ignore_write_from_matches_case_insensitively_on_manufacturer() -> None:
    mapping: dict[str, Any] = {}
    _extract(mapping, _project_data(), ignore_write_from=["GIRA GIERSIEPEN"])
    assert mapping["0/2/10"]["writable"] is False


def test_ignore_write_from_ignores_unrelated_patterns() -> None:
    mapping: dict[str, Any] = {}
    _extract(mapping, _project_data(), ignore_write_from=["no-such-device"])
    assert mapping["0/2/10"]["writable"] is True


def test_writable_provenance_honours_the_exclusion() -> None:
    mapping: dict[str, Any] = {}
    data = _project_data()
    _extract(mapping, data, ignore_write_from=["dummy"])

    provenance = _writable_provenance(mapping, data, ignore_write_from=["dummy"])

    assert provenance == [("1.1.1 (ACME Switch Actuator)", 2)]


def test_ignore_write_from_matches_the_device_name() -> None:
    """Several placeholders can share a product, so target one by its name."""
    mapping: dict[str, Any] = {}
    _extract(mapping, _project_data(), ignore_write_from=["visualisation placeholder"])
    assert mapping["0/2/10"]["writable"] is False
