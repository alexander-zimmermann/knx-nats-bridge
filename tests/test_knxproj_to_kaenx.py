"""Unit tests for the knxproj-to-kaenx generator (no live xknxproject parse)."""

from __future__ import annotations

from typing import Any

import pytest

from knx_nats_bridge.tools.knxproj_to_kaenx import (
    build_device_model,
    device_group_addresses,
    parse_device_spec,
    read_ga_list,
    wiring_worksheet,
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
        "Catalog": [
            {
                "Name": "Hauptkategorie (wird nicht exportiert)",
                "Number": None,
                "IsSection": True,
                "Items": [
                    {
                        "Name": "Lares",
                        "Number": "Lares",
                        "IsSection": True,
                        "Items": [{"Name": "hardware", "Number": "1", "IsSection": False}],
                    },
                    # The GUI leftover whose empty number fails the publish checks.
                    {"Name": "Neue Kategorie", "Number": None, "IsSection": True, "Items": []},
                ],
            }
        ],
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
        "group_ranges": {
            "0": {"name": "Zentral", "group_ranges": {}},
            "4": {"name": "Bordbar", "group_ranges": {}},
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
    assert spec.source == "Bridge placeholder"
    assert spec.name == "KNX-NATS-Bridge"
    assert spec.mode == "split"
    assert spec.app_number == 100
    assert spec.slug == "KNX-NATS-BRIDGE"

    assert parse_device_spec("b=Basalte Core S4:both", 101).slug == "BASALTE-CORE-S4"


@pytest.mark.parametrize("raw", ["no-equals:split", "p=:split", "p=name", "p=name:neither"])
def test_parse_device_spec_rejects(raw: str) -> None:
    with pytest.raises(SystemExit):
        parse_device_spec(raw, 100)


def test_read_ga_list_accepts_addresses_and_subjects() -> None:
    text = "4/2/60\nknx.15.6.25  # comment\n\n# full-line comment\n"
    assert read_ga_list(text, origin="test") == frozenset({"4/2/60", "15/6/25"})


def test_read_ga_list_rejects_garbage() -> None:
    with pytest.raises(SystemExit, match="test"):
        read_ga_list("not-an-address\n", origin="test")


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
    return build_device_model(_template(), spec, _project_data(), write_gas)


def test_split_mode_collectors() -> None:
    model, report = _build()
    objects = model["Application"]["ComObjects"]

    # One collector per (main group, datapoint type, direction): 0/3/2's
    # subtype is unknown to Kaenx-Creator, so it joins the 9.xxx
    # main-type collector, which sorts before the 9.001 one. 0/3/0 and
    # 0/3/1 are skipped (no DPT / DPT unknown to Kaenx-Creator).
    assert [o["Name"] for o in objects] == [
        "hg0-dpt9.xxx-transmit",
        "hg0-dpt9.001-transmit",
        "hg4-dpt1.001-write",
    ]
    assert report.objects == 3
    assert report.links == 3
    assert [ga for ga, _ in report.skipped] == ["0/3/0", "0/3/1"]

    fallback = objects[0]
    assert fallback["Text"][0]["Text"] == "Zentral · 9.xxx · sendet"
    assert fallback["HasDpts"] is False
    assert fallback["SubTypeNumber"] is None

    mirrored = objects[1]
    assert mirrored["Text"][0]["Text"] == "Zentral · 9.001 · sendet"
    assert mirrored["FunctionText"][0]["Text"] == "sendet auf den Bus"
    assert mirrored["TypeNumber"] == "9"
    assert mirrored["HasDpts"] is True
    assert mirrored["SubTypeNumber"] == "1"
    assert mirrored["ObjectSize"] == 16
    assert (mirrored["FlagWrite"], mirrored["FlagTrans"], mirrored["FlagRead"]) == (
        False,
        True,
        True,
    )

    consumed = objects[2]
    assert consumed["Text"][0]["Text"] == "Bordbar · 1.001 · empfängt"
    assert (consumed["FlagWrite"], consumed["FlagTrans"], consumed["FlagRead"]) == (
        True,
        False,
        False,
    )
    assert report.write == 1
    assert report.transmit == 2

    assert report.collectors[0].entries == [("0/3/2", "Odd.SubType")]
    assert report.collectors[1].entries == [("0/2/10", "Sensors.1F.Bedroom.Temperature")]


def test_both_mode_collectors() -> None:
    model, report = _build(mode="both", write_gas=frozenset())
    objects = model["Application"]["ComObjects"]
    assert [o["Name"] for o in objects] == [
        "hg0-dpt9.xxx-both",
        "hg0-dpt9.001-both",
        "hg4-dpt1.001-both",
    ]
    for obj in objects:
        assert obj["FlagWrite"] is True
        assert obj["FlagTrans"] is True
    # No direction suffix when there is only one direction.
    assert objects[0]["Text"][0]["Text"] == "Zentral · 9.xxx"
    assert report.write == report.transmit == report.links == 3


def test_refs_and_dynamics_group_by_main_group() -> None:
    model, _ = _build()
    app = model["Application"]
    assert [r["ComObject"] for r in app["ComObjectRefs"]] == [o["UId"] for o in app["ComObjects"]]
    assert all(r["IsAutoGenerated"] for r in app["ComObjectRefs"])

    blocks = app["Dynamics"][0]["Items"][0]["Items"]
    assert [b["Text"][0]["Text"] for b in blocks] == ["0 · Zentral", "4 · Bordbar"]
    assert [[d["ComObjectRef"] for d in b["Items"]] for b in blocks] == [[1, 2], [3]]


def test_identity_fields_and_deterministic_guid() -> None:
    model, _ = _build()
    assert model["ProjectName"] == "KNX-NATS-Bridge"
    assert model["Info"]["SerialNumber"] == "KNX-NATS-BRIDGE"
    assert model["Info"]["OrderNumber"] == "KNX-NATS-BRIDGE"
    assert model["Info"]["AppNumber"] == 100
    # Application.Number is the version byte, not the identity — the
    # template's V 1.0 must survive.
    assert model["Application"]["Number"] == 16
    assert model["Application"]["HighestComNumber"] == 3

    again, _ = _build()
    assert again["Guid"] == model["Guid"]
    assert model["Guid"] != _template()["Guid"]


def test_catalog_keeps_numbered_sections_only() -> None:
    model, _ = _build()
    items = model["Catalog"][0]["Items"]
    assert [i["Name"] for i in items] == ["Lares"]
    # Non-section entries survive regardless of their number.
    assert [i["Name"] for i in items[0]["Items"]] == ["hardware"]


def test_catalog_section_rename() -> None:
    spec = parse_device_spec("bridge placeholder=KNX-NATS-Bridge:split", 100)
    model, _ = build_device_model(
        _template(), spec, _project_data(), frozenset({"4/2/60"}), catalog_section="Steinroth"
    )
    section = model["Catalog"][0]["Items"][0]
    assert section["Name"] == section["Number"] == "Steinroth"
    assert section["Text"][0]["Text"] == "Steinroth"


def test_app_version_bumps_above_imported() -> None:
    data = _project_data()
    # The generated device as ETS knows it today: order number = slug,
    # application id carrying version 0x11 = V 1.1.
    data["devices"]["1.1.162"] = {
        "name": "KNX-NATS-Bridge",
        "order_number": "KNX-NATS-BRIDGE",
        "application": "M-00FA_A-AF66-11-0000",
    }
    spec = parse_device_spec("bridge placeholder=KNX-NATS-Bridge:split", 100)
    model, report = build_device_model(_template(), spec, data, frozenset({"4/2/60"}))
    assert model["Application"]["Number"] == 0x12
    assert report.app_version == 0x12
    assert model["Application"]["NameText"] == "V 1.2 KNX-NATS-Bridge"


def test_unmatched_write_gas_is_reported() -> None:
    _, report = _build(write_gas=frozenset({"4/2/60", "7/7/7"}))
    assert report.unmatched_write_gas == ["7/7/7"]


def test_unknown_pattern_lists_devices() -> None:
    spec = parse_device_spec("nonexistent=Ghost:both", 100)
    with pytest.raises(SystemExit, match="Switch Actuator"):
        build_device_model(_template(), spec, _project_data(), frozenset())


def test_file_source_builds_from_listed_addresses(tmp_path: Any) -> None:
    ga_file = tmp_path / "bridge-gas.txt"
    ga_file.write_text("knx.0.1.40\n4/2/60\n9/9/9\n", encoding="utf-8")
    spec = parse_device_spec(f"@{ga_file}=KNX-NATS-Bridge:split", 100)
    model, report = build_device_model(_template(), spec, _project_data(), frozenset({"4/2/60"}))

    objects = model["Application"]["ComObjects"]
    # Listed addresses only — regardless of which ETS device carries them.
    assert [o["Name"] for o in objects] == ["hg0-dpt1.001-transmit", "hg4-dpt1.001-write"]
    # An address the configuration lists but ETS does not know is the
    # wiring error class this modelling exists to expose.
    assert report.missing == ["9/9/9"]


def test_file_source_rejects_empty_list(tmp_path: Any) -> None:
    ga_file = tmp_path / "empty.txt"
    ga_file.write_text("# nothing\n", encoding="utf-8")
    spec = parse_device_spec(f"@{ga_file}=Ghost:both", 100)
    with pytest.raises(SystemExit, match="lists no group addresses"):
        build_device_model(_template(), spec, _project_data(), frozenset())


def test_wiring_worksheet_lists_addresses_per_collector() -> None:
    spec = parse_device_spec("bridge placeholder=KNX-NATS-Bridge:split", 100)
    _, report = _build()
    sheet = wiring_worksheet(spec, report)
    assert "## Objekt 2: Zentral · 9.001 · sendet (1 GAs)" in sheet
    assert "- [ ] `0/2/10` Sensors.1F.Bedroom.Temperature" in sheet
    assert "## Objekt 3: Bordbar · 1.001 · empfängt (1 GAs)" in sheet
    assert "## Nicht aufgenommen" in sheet
    assert "- `0/3/0` — no DPT assigned in ETS" in sheet
