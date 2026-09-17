"""Unit tests for the knxproj-to-kaenx generator (no live xknxproject parse)."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from knx_nats_bridge.tools.knxproj_to_kaenx import (
    build_device_model,
    device_group_addresses,
    parse_device_spec,
    read_footprint,
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


def _co(device: str, write: bool = False, transmit: bool = False) -> dict[str, Any]:
    return {"device_address": device, "flags": {"write": write, "transmit": transmit}}


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
            "co-sensor": _co("1.1.2", transmit=True),
            "co-actuator": _co("1.1.1", write=True),
            "co-bridge-1": _co("1.1.9", transmit=True),
            "co-bridge-2": _co("1.1.9", write=True),
            "co-bridge-3": _co("1.1.9", transmit=True),
            "co-bridge-4": _co("1.1.9", transmit=True),
            "co-bridge-5": _co("1.1.9", transmit=True),
            "co-basalte-1": _co("1.1.10", write=True, transmit=True),
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
    spec = parse_device_spec("Bridge placeholder=KNX-NATS-Bridge", 100)
    assert spec.source == "Bridge placeholder"
    assert spec.name == "KNX-NATS-Bridge"
    assert spec.app_number == 100
    assert spec.slug == "KNX-NATS-BRIDGE"

    assert parse_device_spec("b=Basalte Core S4", 101).slug == "BASALTE-CORE-S4"


@pytest.mark.parametrize("raw", ["no-equals", "p=", "=name", " = "])
def test_parse_device_spec_rejects(raw: str) -> None:
    with pytest.raises(SystemExit):
        parse_device_spec(raw, 100)


def test_read_footprint_accepts_addresses_and_subjects() -> None:
    text = (
        "4/2/60 write\n"
        "knx.15.6.25 write  # comment\n"
        "\n"
        "# full-line comment\n"
        "0/0/251   transmit\n"
        "knx.0.1.40 both\n"
    )
    assert read_footprint(text, origin="test") == {
        "4/2/60": "write",
        "15/6/25": "write",
        "0/0/251": "transmit",
        "0/1/40": "both",
    }


def test_read_footprint_rejects_garbage() -> None:
    with pytest.raises(SystemExit, match=r"test:1: 'not-an-address'"):
        read_footprint("not-an-address transmit\n", origin="test")


def test_read_footprint_requires_a_direction() -> None:
    # Footprints are generated, so a bare address is a generator bug,
    # named by file and line — never silently defaulted.
    with pytest.raises(SystemExit, match=r"footprint\.txt:3:"):
        read_footprint("4/2/60 write\n\n0/0/251\n", origin="footprint.txt")


def test_read_footprint_rejects_unknown_direction() -> None:
    with pytest.raises(SystemExit, match=r"test:1: .*'sideways'"):
        read_footprint("4/2/60 sideways\n", origin="test")


def test_read_footprint_rejects_conflicting_directions() -> None:
    with pytest.raises(SystemExit, match=r"test:2: .*4/2/60"):
        read_footprint("4/2/60 write\nknx.4.2.60 transmit\n", origin="test")


def test_device_group_addresses_matches_by_substring() -> None:
    gas = device_group_addresses(_project_data(), "bridge placeholder")
    assert set(gas) == {"0/2/10", "4/2/60", "0/3/0", "0/3/1", "0/3/2"}

    # A pattern hitting the shared hardware name collects both placeholders.
    gas = device_group_addresses(_project_data(), "dummy")
    assert "0/1/40" in gas


def test_device_group_addresses_matches_by_individual_address() -> None:
    gas = device_group_addresses(_project_data(), "1.1.10")
    assert set(gas) == {"0/1/40"}


def _build(project_data: dict[str, Any] | None = None) -> Any:
    spec = parse_device_spec("bridge placeholder=KNX-NATS-Bridge", 100)
    return build_device_model(_template(), spec, project_data or _project_data())


def test_pattern_source_collectors_take_the_direction_from_the_flags() -> None:
    model, report = _build()
    objects = model["Application"]["ComObjects"]

    # One collector per (main group, datapoint type, direction), the
    # direction being what the linked objects' flags say: 0/3/2's
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


def test_pattern_source_skips_objects_without_a_direction() -> None:
    data = _project_data()
    data["communication_objects"]["co-bridge-5"] = _co("1.1.9")
    _, report = _build(data)
    assert ("0/3/2", "neither Write nor Transmit on the source device") in report.skipped
    assert [c.key for c in report.collectors] == ["hg0-dpt9.001-transmit", "hg4-dpt1.001-write"]


def test_both_direction_collectors(footprint: Callable[..., Path]) -> None:
    ga_file = footprint("0/2/10 both", "4/2/60 both", "0/3/2 both")
    spec = parse_device_spec(f"@{ga_file}=Basalte Core S4", 101)
    model, report = build_device_model(_template(), spec, _project_data())
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


@pytest.mark.parametrize(
    ("direction", "flags"),
    [
        ("transmit", (False, True, True)),
        ("write", (True, False, False)),
        ("both", (True, True, False)),
    ],
)
def test_footprint_direction_drives_the_collector(
    footprint: Callable[..., Path], direction: str, flags: tuple[bool, bool, bool]
) -> None:
    """The same address lands on the sending, the receiving or the
    two-way collector purely by the direction on its line — the flags
    of whatever ETS device carries it today play no part."""
    spec = parse_device_spec(f"@{footprint(f'0/2/10 {direction}')}=Telenot", 103)
    model, report = build_device_model(_template(), spec, _project_data())
    (obj,) = model["Application"]["ComObjects"]
    assert obj["Name"] == f"hg0-dpt9.001-{direction}"
    assert (obj["FlagWrite"], obj["FlagTrans"], obj["FlagRead"]) == flags
    assert report.collectors[0].entries == [("0/2/10", "Sensors.1F.Bedroom.Temperature")]


def test_mixed_directions_order_sending_two_way_receiving(footprint: Callable[..., Path]) -> None:
    # Three directions on one main group and type: the positional
    # numbering must be total, not left to the address order.
    ga_file = footprint("0/2/10 write", "0/3/2 both", "0/1/40 transmit")
    data = _project_data()
    data["group_addresses"]["0/1/40"]["dpt"] = {"main": 9, "sub": 1}
    data["group_addresses"]["0/3/2"]["dpt"] = {"main": 9, "sub": 1}
    spec = parse_device_spec(f"@{ga_file}=Telenot", 103)
    _, report = build_device_model(_template(), spec, data)
    assert [(c.number, c.key) for c in report.collectors] == [
        (1, "hg0-dpt9.001-transmit"),
        (2, "hg0-dpt9.001-both"),
        (3, "hg0-dpt9.001-write"),
    ]
    assert report.write == 2 and report.transmit == 2


def test_footprint_without_direction_names_file_and_line(footprint: Callable[..., Path]) -> None:
    ga_file = footprint("0/2/10 transmit", "4/2/60")
    spec = parse_device_spec(f"@{ga_file}=KNX-NATS-Bridge", 100)
    with pytest.raises(SystemExit, match=rf"{ga_file}:2:"):
        build_device_model(_template(), spec, _project_data())


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
    spec = parse_device_spec("bridge placeholder=KNX-NATS-Bridge", 100)
    model, _ = build_device_model(_template(), spec, _project_data(), catalog_section="Steinroth")
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
    spec = parse_device_spec("bridge placeholder=KNX-NATS-Bridge", 100)
    model, report = build_device_model(_template(), spec, data)
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


def test_unknown_pattern_lists_devices() -> None:
    spec = parse_device_spec("nonexistent=Ghost", 100)
    with pytest.raises(SystemExit, match="Switch Actuator"):
        build_device_model(_template(), spec, _project_data())


def test_file_source_builds_from_listed_addresses(footprint: Callable[..., Path]) -> None:
    # Mixed spellings: a NATS subject and plain addresses on one footprint.
    ga_file = footprint("knx.0.1.40 transmit", "4/2/60 write", "9/9/9 transmit")
    spec = parse_device_spec(f"@{ga_file}=KNX-NATS-Bridge", 100)
    model, report = build_device_model(_template(), spec, _project_data())

    objects = model["Application"]["ComObjects"]
    # Listed addresses only — regardless of which ETS device carries them
    # and of that device's flags (0/1/40 sits on a Write+Transmit object).
    assert [o["Name"] for o in objects] == ["hg0-dpt1.001-transmit", "hg4-dpt1.001-write"]
    # An address the configuration lists but ETS does not know is the
    # wiring error class this modelling exists to expose.
    assert report.missing == ["9/9/9"]


def test_file_source_rejects_empty_list(footprint: Callable[..., Path]) -> None:
    ga_file = footprint("# nothing")
    spec = parse_device_spec(f"@{ga_file}=Ghost", 100)
    with pytest.raises(SystemExit, match="lists no group addresses"):
        build_device_model(_template(), spec, _project_data())


def test_wiring_worksheet_lists_addresses_per_collector() -> None:
    spec = parse_device_spec("bridge placeholder=KNX-NATS-Bridge", 100)
    _, report = _build()
    sheet = wiring_worksheet(spec, report)
    assert "## Objekt 2: Zentral · 9.001 · sendet (1 GAs)" in sheet
    assert "- [ ] `0/2/10` Sensors.1F.Bedroom.Temperature" in sheet
    assert "## Objekt 3: Bordbar · 1.001 · empfängt (1 GAs)" in sheet
    assert "## Nicht aufgenommen" in sheet
    assert "- `0/3/0` — no DPT assigned in ETS" in sheet


# --- positional numbers against the installed device -----------------------


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


def _build_against(installed: dict[int, str] | None) -> Any:
    from knx_nats_bridge.tools.knxproj_to_kaenx import DeviceReport, build_collectors

    spec = parse_device_spec("bridge placeholder=KNX-NATS-Bridge", 100)
    report = DeviceReport()
    collectors = build_collectors(spec, _project_data(), report, installed)
    return collectors, report


def test_numbers_are_positional_and_the_installed_device_tells_the_change() -> None:
    # Installed with two of the three objects, in an order the positional
    # numbering does not produce, plus one the configuration dropped.
    installed = {1: "hg4-dpt1.001-write", 2: "hg0-dpt9.001-transmit", 3: "hg2-dpt1-both"}
    collectors, report = _build_against(installed)
    assert [(c.number, c.key, c.installed_number) for c in collectors] == [
        (1, "hg0-dpt9.xxx-transmit", None),
        (2, "hg0-dpt9.001-transmit", 2),
        (3, "hg4-dpt1.001-write", 1),
    ]
    assert [c.key for c in report.new_objects] == ["hg0-dpt9.xxx-transmit"]
    assert [c.key for c in report.renumbered] == ["hg4-dpt1.001-write"]
    # The dropped object is named as ETS shows it, with its old number.
    assert report.lost == [(3, "Hauptgruppe 2 · 1.xxx", 0)]


def test_without_an_installed_device_nothing_is_new() -> None:
    collectors, report = _build_against(None)
    assert [c.number for c in collectors] == [1, 2, 3]
    assert all(c.installed_number is None for c in collectors)
    assert report.new_objects == [] and report.renumbered == [] and report.lost == []


def test_wiring_worksheet_maps_old_numbers() -> None:
    from knx_nats_bridge.tools.knxproj_to_kaenx import DeviceReport, build_collectors

    spec = parse_device_spec("bridge placeholder=KNX-NATS-Bridge", 100)
    report = DeviceReport()
    installed = {1: "hg4-dpt1.001-write", 2: "hg0-dpt9.001-transmit", 3: "hg2-dpt1-both"}
    build_collectors(spec, _project_data(), report, installed)
    sheet = wiring_worksheet(spec, report)
    assert "1 neu, 1 umnummeriert, 1 entfallen" in sheet
    assert "## Objekt 1: Zentral · 9.xxx · sendet (1 GAs) — NEU" in sheet
    assert "## Objekt 2: Zentral · 9.001 · sendet (1 GAs)\n" in sheet
    assert "## Objekt 3: Bordbar · 1.001 · empfängt (1 GAs) — war 1" in sheet
    assert "## Entfallene Objekte" in sheet
    assert "- Objekt 3: Hauptgruppe 2 · 1.xxx" in sheet


def test_versions_round_trip(tmp_path: Any) -> None:
    from knx_nats_bridge.tools.knxproj_to_kaenx import read_versions, write_versions

    path = tmp_path / "versions.yaml"
    assert read_versions(path) == {}
    write_versions(path, {"Node-Red": 20, "Basalte Core S4": 18})
    text = path.read_text(encoding="utf-8")
    assert text.startswith("# The application version")
    assert text.index("Basalte Core S4: 18") < text.index("Node-Red: 20")
    assert read_versions(path) == {"Basalte Core S4": 18, "Node-Red": 20}


def test_version_bumps_only_on_change() -> None:
    data = _project_data()
    data["devices"]["1.1.162"] = {
        "name": "KNX-NATS-Bridge",
        "order_number": "KNX-NATS-BRIDGE",
        "application": "M-00FA_A-AF66-11-0000",
    }
    spec = parse_device_spec("bridge placeholder=KNX-NATS-Bridge", 100)
    # Installed at V 1.1 with exactly the objects the configuration
    # produces: nothing to publish, the version stays.
    installed = {1: "hg0-dpt9.xxx-transmit", 2: "hg0-dpt9.001-transmit", 3: "hg4-dpt1.001-write"}
    model, report = build_device_model(_template(), spec, data, installed=installed)
    assert report.app_version == 0x11 and report.new_objects == []
    # A new object: one above the installed version …
    model, report = build_device_model(
        _template(), spec, data, installed={1: "hg0-dpt9.xxx-transmit"}
    )
    assert report.app_version == 0x12
    # … unless the versions file knows a higher one was already published.
    model, report = build_device_model(
        _template(), spec, data, installed={1: "hg0-dpt9.xxx-transmit"}, published=0x13
    )
    assert report.app_version == 0x14
    assert model["Application"]["ReplacesVersions"] == "16 17 18 19"
