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
    # ETS enables the in-place update only for a version that names the
    # ones it replaces — every earlier one, so a device still on V 1.0
    # steps straight to V 1.2 as well.
    assert model["Application"]["ReplacesVersions"] == "16 17"


def test_first_version_replaces_nothing() -> None:
    model, report = _build()
    assert report.app_version == 16
    assert model["Application"]["ReplacesVersions"] == ""


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


# --- object registry and loss guard -----------------------------------------


def test_parse_collector_key_accepts_both_spellings() -> None:
    from knx_nats_bridge.tools.knxproj_to_kaenx import parse_collector_key

    legacy = parse_collector_key("hg2-dpt1-both")
    assert legacy is not None and (legacy.main_group, legacy.dpt_main, legacy.dpt_sub) == (
        2,
        1,
        None,
    )
    assert legacy.key == "hg2-dpt1.xxx-both"
    sub = parse_collector_key("hg15-dpt5.010-transmit")
    assert sub is not None and sub.dpt_sub == 10 and sub.direction == "transmit"
    assert parse_collector_key("Logik B - Eingangslogik 1") is None


def _registry_build(registry: dict[str, int] | None, installed: dict[int, str] | None) -> Any:
    from knx_nats_bridge.tools.knxproj_to_kaenx import DeviceReport, build_collectors

    spec = parse_device_spec("bridge placeholder=KNX-NATS-Bridge:split", 100)
    report = DeviceReport()
    collectors = build_collectors(
        spec, _project_data(), frozenset({"4/2/60"}), report, registry, installed
    )
    return collectors, report


def test_registry_keeps_numbers_and_appends_new() -> None:
    # The device is installed with two objects in an order the positional
    # numbering would not produce; a third collector is new.
    registry: dict[str, int] = {}
    installed = {1: "hg4-dpt1.001-write", 2: "hg0-dpt9.001-transmit"}
    collectors, report = _registry_build(registry, installed)
    assert [(c.number, c.key) for c in collectors] == [
        (1, "hg4-dpt1.001-write"),
        (2, "hg0-dpt9.001-transmit"),
        (3, "hg0-dpt9.xxx-transmit"),
    ]
    assert registry == {
        "hg4-dpt1.001-write": 1,
        "hg0-dpt9.001-transmit": 2,
        "hg0-dpt9.xxx-transmit": 3,
    }
    assert [c.key for c in report.new_objects] == ["hg0-dpt9.xxx-transmit"]
    assert report.lost == []


def test_registry_keeps_orphaned_objects_as_legacy() -> None:
    registry = {"hg7-dpt1-both": 1, "hg0-dpt9.001-transmit": 2}
    collectors, report = _registry_build(registry, None)
    legacy = collectors[0]
    assert legacy.number == 1 and legacy.legacy and legacy.entries == []
    assert legacy.key == "hg7-dpt1.xxx-both" and legacy.text == "Hauptgruppe 7 · 1.xxx"
    assert [c.number for c in collectors] == [1, 2, 3, 4]


def test_installed_object_dropped_by_new_version_is_reported() -> None:
    # Installed object 1 is a kind the configuration no longer produces and
    # the registry does not know: the new version would renumber past it.
    installed = {1: "hg2-dpt1-both", 2: "hg0-dpt9.001-transmit"}
    _, report = _registry_build(None, installed)
    assert (1, "hg2-dpt1.xxx-both", 0) in report.lost
    # With a registry the installed object is seeded and kept instead.
    _, report = _registry_build({}, installed)
    assert report.lost == []


def test_registry_conflict_is_refused() -> None:
    registry = {"hg0-dpt9.001-transmit": 5}
    with pytest.raises(SystemExit, match="registry holds it as number 5"):
        _registry_build(registry, {1: "hg0-dpt9.001-transmit"})


def test_registry_round_trip(tmp_path: Any) -> None:
    from knx_nats_bridge.tools.knxproj_to_kaenx import (
        DeviceRegistry,
        read_registry,
        write_registry,
    )

    path = tmp_path / "objects.yaml"
    assert read_registry(path) == {}
    section = DeviceRegistry(17, {"hg1-dpt1.001-both": 2, "hg0-dpt1.001-both": 1})
    write_registry(path, {"Dev": section})
    text = path.read_text(encoding="utf-8")
    assert text.startswith("# The generated ETS devices")
    assert text.index("hg0-dpt1.001-both: 1") < text.index("hg1-dpt1.001-both: 2")
    back = read_registry(path)
    assert back["Dev"].version == 17
    assert back["Dev"].objects == {"hg0-dpt1.001-both": 1, "hg1-dpt1.001-both": 2}


def test_version_bumps_only_on_change() -> None:
    data = _project_data()
    data["devices"]["1.1.162"] = {
        "name": "KNX-NATS-Bridge",
        "order_number": "KNX-NATS-BRIDGE",
        "application": "M-00FA_A-AF66-11-0000",
    }
    spec = parse_device_spec("bridge placeholder=KNX-NATS-Bridge:split", 100)
    write_gas = frozenset({"4/2/60"})
    # Installed at V 1.1 with exactly the objects the configuration
    # produces: nothing to publish, the version stays.
    installed = {1: "hg0-dpt9.xxx-transmit", 2: "hg0-dpt9.001-transmit", 3: "hg4-dpt1.001-write"}
    model, report = build_device_model(_template(), spec, data, write_gas, installed=installed)
    assert report.app_version == 0x11 and report.new_objects == []
    # A new object: one above the installed version …
    model, report = build_device_model(
        _template(), spec, data, write_gas, installed={1: "hg0-dpt9.xxx-transmit"}
    )
    assert report.app_version == 0x12
    # … unless the registry knows a higher version was already published.
    model, report = build_device_model(
        _template(), spec, data, write_gas, installed={1: "hg0-dpt9.xxx-transmit"}, published=0x13
    )
    assert report.app_version == 0x14
    assert model["Application"]["ReplacesVersions"] == "16 17 18 19"


def test_sorted_display_keeps_identity() -> None:
    """A new kind that sorts first gets shown as Number 1, but the
    installed objects keep their identities — the Ids links refer to."""
    from knx_nats_bridge.tools.knxproj_to_kaenx import DeviceReport, build_collectors

    spec = parse_device_spec("bridge placeholder=KNX-NATS-Bridge:split", 100)
    registry: dict[str, int] = {}
    # Installed: the write collector is identity 1, the 9.001 one is 2.
    installed = {1: "hg4-dpt1.001-write", 2: "hg0-dpt9.001-transmit"}
    report = DeviceReport()
    collectors = build_collectors(
        spec, _project_data(), frozenset({"4/2/60"}), report, registry, installed, True
    )
    # Shown in collector order; identities unchanged; the new one appended.
    assert [(c.display, c.number, c.key) for c in collectors] == [
        (1, 3, "hg0-dpt9.xxx-transmit"),
        (2, 2, "hg0-dpt9.001-transmit"),
        (3, 1, "hg4-dpt1.001-write"),
    ]
    model, _ = build_device_model(
        _template(),
        spec,
        _project_data(),
        frozenset({"4/2/60"}),
        registry=dict(registry),
        installed=installed,
        sorted_display=True,
    )
    objects = model["Application"]["ComObjects"]
    assert [(o["Number"], o["Id"], o["UId"]) for o in objects] == [(1, 3, 3), (2, 2, 2), (3, 1, 1)]
    assert [r["ComObject"] for r in model["Application"]["ComObjectRefs"]] == [3, 2, 1]
    assert model["Application"]["HighestComNumber"] == 3
