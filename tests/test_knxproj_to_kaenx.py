"""Unit tests for the knxproj-to-kaenx generator (no live xknxproject parse)."""

from __future__ import annotations

from typing import Any

import pytest

from knx_nats_bridge.tools.knxproj_to_kaenx import (
    build_device_model,
    device_group_addresses,
    parse_device_spec,
    read_write_gas,
)


def _template() -> dict[str, Any]:
    """The slice of an empty .ae-manu the builder reads and rewrites."""
    language = {
        "$type": "Kaenx.Creator.Models.Language, Kaenx.Creator.Share",
        "CultureCode": "de-DE",
        "Text": "Deutsch",
    }
    return {
        "ProjectName": "empty",
        "Guid": "00000000-0000-0000-0000-000000000000",
        "FileName": "output",
        "Info": {
            "Name": "Hardware Name",
            "SerialNumber": "1",
            "OrderNumber": "TA-00002.1",
            "AppNumber": 0,
            "MaskId": "MV-07B0",
            "Text": [],
            "Description": [],
        },
        "Application": {
            "ParameterTypes": [],
            "Parameters": [],
            "ComObjects": [],
            "ComObjectRefs": [],
            "Languages": [language],
            "Dynamics": [],
            "Name": "applikation",
            "NameText": "V 1.0 applikation",
            "Text": [],
            "Number": 16,
            "HighestComNumber": 0,
        },
        "ImportVersion": 10,
        "ManufacturerId": 175,
    }


def _project_data() -> dict[str, Any]:
    """Hand-built project_data that mirrors xknxproject's parse() shape."""
    return {
        "group_addresses": {
            "0/2/10": {
                "name": "Sensors.1F.Bedroom.Temperature",
                "dpt": {"main": 9, "sub": 1},
                "communication_object_ids": ["co-sensor", "co-bridge-1"],
            },
            "4/2/60": {
                "name": "Light.Bordbar.OnOff",
                "dpt": {"main": 1, "sub": 1},
                "communication_object_ids": ["co-bridge-2"],
            },
            "0/1/40": {
                "name": "Lighting.1F.Bedroom.Ceiling.Switch",
                "dpt": {"main": 1, "sub": 1},
                "communication_object_ids": ["co-actuator", "co-basalte-1"],
            },
            "0/3/0": {
                "name": "Broken.NoDpt",
                "dpt": None,
                "communication_object_ids": ["co-bridge-3"],
            },
            "0/3/1": {
                "name": "Broken.ExoticDpt",
                "dpt": {"main": 999, "sub": 1},
                "communication_object_ids": ["co-bridge-4"],
            },
            "0/3/2": {
                "name": "Odd.SubType",
                "dpt": {"main": 9, "sub": 999},
                "communication_object_ids": ["co-bridge-5"],
            },
        },
        "communication_objects": {
            "co-sensor": {"device_address": "1.1.2"},
            "co-actuator": {"device_address": "1.1.1"},
            "co-bridge-1": {"device_address": "1.1.9"},
            "co-bridge-2": {"device_address": "1.1.9"},
            "co-bridge-3": {"device_address": "1.1.9"},
            "co-bridge-4": {"device_address": "1.1.9"},
            "co-bridge-5": {"device_address": "1.1.9"},
            "co-basalte-1": {"device_address": "1.1.10"},
        },
        "devices": {
            "1.1.1": {"manufacturer_name": "ACME", "hardware_name": "Switch Actuator"},
            "1.1.2": {"manufacturer_name": "ACME", "hardware_name": "Temp Sensor"},
            "1.1.9": {
                "name": "Bridge placeholder",
                "manufacturer_name": "GIRA Giersiepen",
                "hardware_name": "Dummy",
            },
            "1.1.10": {
                "name": "Basalte placeholder",
                "manufacturer_name": "GIRA Giersiepen",
                "hardware_name": "Dummy",
            },
        },
    }


def test_parse_device_spec() -> None:
    spec = parse_device_spec("Bridge placeholder=KNX-NATS-Bridge:split", 100)
    assert spec.pattern == "Bridge placeholder"
    assert spec.name == "KNX-NATS-Bridge"
    assert spec.mode == "split"
    assert spec.app_number == 100
    assert spec.slug == "KNX-NATS-BRIDGE"

    assert parse_device_spec("b=Basalte Core S4:both", 101).slug == "BASALTE-CORE-S4"


@pytest.mark.parametrize("raw", ["no-equals:split", "p=:split", "p=name", "p=name:neither"])
def test_parse_device_spec_rejects(raw: str) -> None:
    with pytest.raises(SystemExit):
        parse_device_spec(raw, 100)


def test_read_write_gas_accepts_addresses_and_subjects() -> None:
    text = "4/2/60\nknx.15.6.25  # comment\n\n# full-line comment\n"
    assert read_write_gas(text) == frozenset({"4/2/60", "15/6/25"})


def test_read_write_gas_rejects_garbage() -> None:
    with pytest.raises(SystemExit):
        read_write_gas("not-an-address\n")


def test_device_group_addresses_matches_by_substring() -> None:
    gas = device_group_addresses(_project_data(), "bridge placeholder")
    assert set(gas) == {"0/2/10", "4/2/60", "0/3/0", "0/3/1", "0/3/2"}

    # A pattern hitting the shared hardware name collects both placeholders.
    gas = device_group_addresses(_project_data(), "dummy")
    assert "0/1/40" in gas


def test_device_group_addresses_matches_by_individual_address() -> None:
    gas = device_group_addresses(_project_data(), "1.1.10")
    assert set(gas) == {"0/1/40"}


def _build(mode: str = "split", write_gas: frozenset[str] = frozenset({"4/2/60"})) -> Any:
    spec = parse_device_spec(f"bridge placeholder=KNX-NATS-Bridge:{mode}", 100)
    return build_device_model(_template(), spec, _project_data(), write_gas, "knx")


def test_split_mode_flags_and_fields() -> None:
    model, report = _build()
    objects = model["Application"]["ComObjects"]

    # 0/3/* are skipped (no DPT / DPT unknown to Kaenx), 0/3/2 survives
    # with the exotic subtype dropped to its main type.
    assert [o["Name"] for o in objects] == ["knx.0.2.10", "knx.0.3.2", "knx.4.2.60"]
    assert report.objects == 3
    assert [ga for ga, _ in report.skipped] == ["0/3/0", "0/3/1"]

    mirrored = objects[0]
    assert mirrored["Text"][0]["Text"] == "Sensors.1F.Bedroom.Temperature"
    assert mirrored["FunctionText"][0]["Text"] == "0/2/10"
    assert mirrored["TypeNumber"] == "9"
    assert mirrored["SubTypeNumber"] == "1"
    assert mirrored["HasDpts"] is True
    assert mirrored["ObjectSize"] == 16
    assert (mirrored["FlagWrite"], mirrored["FlagTrans"], mirrored["FlagRead"]) == (
        False,
        True,
        True,
    )

    exotic_sub = objects[1]
    assert exotic_sub["HasDpts"] is False
    assert exotic_sub["SubTypeNumber"] is None

    consumed = objects[2]
    assert (consumed["FlagWrite"], consumed["FlagTrans"], consumed["FlagRead"]) == (
        True,
        False,
        False,
    )
    assert report.write == 1
    assert report.transmit == 2


def test_both_mode_flags() -> None:
    model, _ = _build(mode="both", write_gas=frozenset())
    for obj in model["Application"]["ComObjects"]:
        assert obj["FlagWrite"] is True
        assert obj["FlagTrans"] is True


def test_refs_and_dynamics_mirror_objects() -> None:
    model, _ = _build()
    app = model["Application"]
    assert [r["ComObject"] for r in app["ComObjectRefs"]] == [o["UId"] for o in app["ComObjects"]]
    assert all(r["IsAutoGenerated"] for r in app["ComObjectRefs"])

    block = app["Dynamics"][0]["Items"][0]["Items"][0]
    assert [d["ComObjectRef"] for d in block["Items"]] == [r["UId"] for r in app["ComObjectRefs"]]


def test_identity_fields_and_deterministic_guid() -> None:
    model, _ = _build()
    assert model["ProjectName"] == "KNX-NATS-Bridge"
    assert model["Info"]["SerialNumber"] == "KNX-NATS-BRIDGE"
    assert model["Info"]["OrderNumber"] == "KNX-NATS-BRIDGE"
    assert model["Info"]["AppNumber"] == 100
    assert model["Application"]["Number"] == 100
    assert model["Application"]["HighestComNumber"] == 3

    again, _ = _build()
    assert again["Guid"] == model["Guid"]
    assert model["Guid"] != _template()["Guid"]


def test_unmatched_write_gas_is_reported() -> None:
    _, report = _build(write_gas=frozenset({"4/2/60", "7/7/7"}))
    assert report.unmatched_write_gas == ["7/7/7"]


def test_unknown_pattern_lists_devices() -> None:
    spec = parse_device_spec("nonexistent=Ghost:both", 100)
    with pytest.raises(SystemExit, match="Switch Actuator"):
        build_device_model(_template(), spec, _project_data(), frozenset(), "knx")
