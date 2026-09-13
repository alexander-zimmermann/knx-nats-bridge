"""Unit tests for knxproj-check-wiring (no live xknxproject parse)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from knx_nats_bridge.tools.check_wiring import check_device, todo_worksheet
from knx_nats_bridge.tools.knxproj_to_kaenx import parse_device_spec


def _project_data() -> dict[str, Any]:
    """A partially wired bridge device as xknxproject's parse() delivers it."""

    def co(device: str, number: int, write: bool = False, transmit: bool = False) -> dict[str, Any]:
        return {
            "device_address": device,
            "number": number,
            "flags": {"write": write, "transmit": transmit, "communication": True},
        }

    return {
        "group_addresses": {
            # Writer target, correctly on a Transmit collector.
            "0/0/251": {
                "name": "Zentral.Fault",
                "dpt": {"main": 1, "sub": 1},
                "communication_object_ids": ["co-t"],
            },
            # Consumed address, correctly on a Write collector.
            "4/2/60": {
                "name": "Bordbar.OnOff",
                "dpt": {"main": 1, "sub": 1},
                "communication_object_ids": ["co-w"],
            },
            # Consumed address linked to a Transmit collector: misflagged.
            "15/6/25": {
                "name": "Wallbox.ModusPV",
                "dpt": {"main": 1, "sub": 1},
                "communication_object_ids": ["co-t"],
            },
            # Linked to the device, but no configuration claims it.
            "9/9/9": {
                "name": "Alt.Verwaist",
                "dpt": {"main": 1, "sub": 1},
                "communication_object_ids": ["co-t"],
            },
            # In the footprint, exists in ETS, but linked elsewhere only.
            "2/0/1": {
                "name": "Schalten.Zentral",
                "dpt": {"main": 1, "sub": 1},
                "communication_object_ids": ["co-other"],
            },
        },
        "group_ranges": {
            "2": {"name": "Schalten", "group_ranges": {}},
        },
        "communication_objects": {
            "co-t": co("1.1.162", 1, transmit=True),
            "co-w": co("1.1.162", 3, write=True),
            "co-other": co("1.1.20", 1, write=True),
        },
        "devices": {
            "1.1.162": {"name": "KNX-NATS-Bridge", "order_number": "KNX-NATS-BRIDGE"},
            "1.1.20": {"name": "Some actuator", "order_number": "KAA-8R"},
        },
    }


def _check(tmp_path: Path, footprint: list[str], write_gas: set[str]) -> Any:
    ga_file = tmp_path / "footprint.txt"
    ga_file.write_text("\n".join(footprint) + "\n", encoding="utf-8")
    spec = parse_device_spec(f"@{ga_file}=KNX-NATS-Bridge:split", 100)
    return spec, check_device(_project_data(), spec, frozenset(write_gas))


def test_all_finding_classes(tmp_path: Path) -> None:
    spec, report = _check(
        tmp_path,
        ["0/0/251", "4/2/60", "15/6/25", "2/0/1", "7/7/7"],
        {"4/2/60", "15/6/25"},
    )
    # 2/0/1 exists in ETS but is linked to another device only: it is
    # the one open link, on its collector with the generated number.
    assert [(number, entries) for number, _, entries in report.todo] == [
        (2, [("2/0/1", "Schalten.Zentral")])
    ]
    assert report.open_links == 1
    # 15/6/25 is consumed but sits on the sending collector: the Write
    # flag is missing and the Transmit flag is one it must not carry.
    assert report.misflagged == [("15/6/25", "write")]
    assert report.cross_linked == [("15/6/25", "transmit")]
    assert report.misplaced == [("15/6/25", 4, [1])]
    assert report.extra == [("9/9/9", "Alt.Verwaist")]
    assert report.missing_from_ets == ["7/7/7"]
    assert not report.clean

    sheet = todo_worksheet(spec, report)
    assert "## Objekt 2: Schalten · 1.001 · sendet (1 von 1 offen)" in sheet
    assert "- [ ] `2/0/1` Schalten.Zentral" in sheet
    assert "- `15/6/25` — gehört auf das empfängt-Objekt" in sheet
    assert "- `15/6/25` — hängt auch am sendet-Objekt" in sheet
    assert "- `9/9/9` Alt.Verwaist" in sheet
    assert "- `7/7/7`" in sheet


def test_cross_link_beside_the_correct_one_is_caught(tmp_path: Path) -> None:
    """A status address dropped on the receiving collector as well.

    The wanted flag is present, so the old check passed it — while the
    catalog turned the address writable off the stray Write flag.
    """
    data = _project_data()
    data["group_addresses"]["0/0/251"]["communication_object_ids"] = ["co-t", "co-w"]
    ga_file = tmp_path / "footprint.txt"
    ga_file.write_text("0/0/251\n", encoding="utf-8")
    spec = parse_device_spec(f"@{ga_file}=KNX-NATS-Bridge:split", 100)
    report = check_device(data, spec, frozenset())

    assert report.todo == []
    assert report.misflagged == []
    assert report.cross_linked == [("0/0/251", "write")]
    assert not report.clean


def test_both_mode_device_cannot_cross_link(tmp_path: Path) -> None:
    data = _project_data()
    data["group_addresses"]["0/0/251"]["communication_object_ids"] = ["co-t", "co-w"]
    ga_file = tmp_path / "footprint.txt"
    ga_file.write_text("0/0/251\n", encoding="utf-8")
    spec = parse_device_spec(f"@{ga_file}=KNX-NATS-Bridge:both", 100)
    report = check_device(data, spec, frozenset())

    # Its collectors carry both directions, so neither flag is forbidden.
    assert report.cross_linked == []
    assert report.misflagged == []


def test_findings_only_what_is_wrong(tmp_path: Path) -> None:
    _, report = _check(tmp_path, ["0/0/251", "4/2/60"], {"4/2/60"})
    assert report.todo == []
    assert report.misflagged == []
    assert report.missing_from_ets == []
    # The two device links no source claims any more.
    assert [ga for ga, _ in report.extra] == ["9/9/9", "15/6/25"]
    assert not report.clean


def test_unimported_device_is_reported(tmp_path: Path) -> None:
    ga_file = tmp_path / "footprint.txt"
    ga_file.write_text("1/1/1\n", encoding="utf-8")
    spec = parse_device_spec(f"@{ga_file}=Ghost:both", 100)
    report = check_device(_project_data(), spec, frozenset())
    assert report.device_found is False
    assert not report.clean


def test_misplaced_link_is_reported(tmp_path: Path) -> None:
    """After a type change the address belongs on a new collector, but its
    link still sits on the old object: flags match, the number does not."""
    data = _project_data()
    # 0/0/251 and a second Zentral address of another subtype: two
    # collectors, numbered 1 and 2; both links sit on object 1.
    data["group_addresses"]["0/0/253"] = {
        "name": "Zentral.Trigger",
        "dpt": {"main": 1, "sub": 17},
        "communication_object_ids": ["co-t"],
    }
    ga_file = tmp_path / "footprint.txt"
    ga_file.write_text("0/0/251\n0/0/253\n", encoding="utf-8")
    spec = parse_device_spec(f"@{ga_file}=KNX-NATS-Bridge:split", 100)
    report = check_device(data, spec, frozenset())

    assert report.misflagged == [] and report.cross_linked == []
    assert report.misplaced == [("0/0/253", 2, [1])]
    assert not report.clean
    sheet = todo_worksheet(spec, report)
    assert "- `0/0/253` — gehört auf Objekt 2, hängt an 1" in sheet


def test_older_installed_version_is_checked_by_object_name(tmp_path: Path) -> None:
    """The device in the project is the previous version: its numbers
    differ from the generated ones, and one object exists only in the
    new version. Findings carry the numbers ETS shows today."""
    data = _project_data()
    data["group_addresses"]["0/0/253"] = {
        "name": "Zentral.Trigger",
        "dpt": {"main": 1, "sub": 17},
        "communication_object_ids": ["co-t"],
    }
    ga_file = tmp_path / "footprint.txt"
    ga_file.write_text("0/0/251\n0/0/253\n4/2/60\n", encoding="utf-8")
    spec = parse_device_spec(f"@{ga_file}=KNX-NATS-Bridge:split", 100)
    # Generated: 1 = hg0 1.001 sendet, 2 = hg0 1.017 sendet, 3 = hg4 1.001
    # empfängt. Installed without the 1.017 object, under other numbers.
    installed = {1: "hg0-dpt1.001-transmit", 3: "hg4-dpt1.001-write"}
    report = check_device(data, spec, frozenset({"4/2/60"}), installed)

    # 0/0/251 on installed object 1, 4/2/60 on installed object 3: fine.
    # 0/0/253 hangs on object 1 but belongs on the new version's object 2.
    assert report.misplaced == [("0/0/253", 2, [1])]
    assert [c.key for c in report.unpublished] == ["hg0-dpt1.017-transmit"]
    assert report.todo == []
    sheet = todo_worksheet(spec, report)
    assert "- `0/0/253` — gehört auf Objekt 2, hängt an 1" in sheet
