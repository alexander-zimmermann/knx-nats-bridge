"""Unit tests for knxproj-check-wiring (no live xknxproject parse)."""

from __future__ import annotations

from typing import Any

import pytest

from knx_nats_bridge.tools.check_wiring import check_device, parse_check_spec


def _project_data() -> dict[str, Any]:
    """A wired bridge device as xknxproject's parse() shape delivers it."""

    def co(device: str, write: bool = False, transmit: bool = False) -> dict[str, Any]:
        return {
            "device_address": device,
            "flags": {"write": write, "transmit": transmit, "communication": True},
        }

    return {
        "group_addresses": {
            # Writer target, correctly on a Transmit collector.
            "0/0/251": {"communication_object_ids": ["co-t"]},
            # Consumed address, correctly on a Write collector.
            "4/2/60": {"communication_object_ids": ["co-w"]},
            # Consumed address linked to a Transmit collector: misflagged.
            "15/6/25": {"communication_object_ids": ["co-t"]},
            # Linked to the device, but no configuration claims it.
            "9/9/9": {"communication_object_ids": ["co-t"]},
            # Exists in ETS, in the footprint, but linked elsewhere only.
            "2/0/1": {"communication_object_ids": ["co-other"]},
        },
        "communication_objects": {
            "co-t": co("1.1.162", transmit=True),
            "co-w": co("1.1.162", write=True),
            "co-other": co("1.1.20", write=True),
        },
        "devices": {
            "1.1.162": {"name": "KNX-NATS-Bridge", "order_number": "KNX-NATS-BRIDGE"},
            "1.1.20": {"name": "Some actuator", "order_number": "KAA-8R"},
        },
    }


def _check(footprint: set[str], write_gas: set[str] | None = None) -> Any:
    spec = parse_check_spec("KNX-NATS-Bridge=@unused.txt")
    if write_gas is None:
        write_gas = {"4/2/60", "15/6/25"}
    return check_device(_project_data(), spec, frozenset(footprint), frozenset(write_gas))


def test_parse_check_spec() -> None:
    spec = parse_check_spec("Basalte Core S4=@/tmp/basalte.txt")
    assert spec.name == "Basalte Core S4"
    assert spec.slug == "BASALTE-CORE-S4"
    assert str(spec.footprint_path) == "/tmp/basalte.txt"


@pytest.mark.parametrize("raw", ["no-equals", "Name=plainfile.txt", "=@x", "Name=@"])
def test_parse_check_spec_rejects(raw: str) -> None:
    with pytest.raises(SystemExit):
        parse_check_spec(raw)


def test_clean_wiring_passes() -> None:
    report = _check({"0/0/251", "4/2/60"}, write_gas={"4/2/60"})
    # 15/6/25 and 9/9/9 are linked but not in this footprint -> extra.
    assert report.missing == []
    assert report.extra == ["9/9/9", "15/6/25"]
    assert report.misflagged == []


def test_all_finding_classes() -> None:
    report = _check({"0/0/251", "4/2/60", "15/6/25", "2/0/1"})
    # 2/0/1 exists in ETS but is linked to another device only.
    assert report.missing == ["2/0/1"]
    assert report.extra == ["9/9/9"]
    # Consumed 15/6/25 sits on a Transmit-only collector.
    assert report.misflagged == ["15/6/25"]
    assert not report.clean


def test_footprint_address_absent_from_ets_is_missing() -> None:
    report = _check({"0/0/251", "4/2/60", "7/7/7"}, write_gas={"4/2/60"})
    assert "7/7/7" in report.missing


def test_unimported_device_is_reported() -> None:
    spec = parse_check_spec("Ghost=@unused.txt")
    report = check_device(_project_data(), spec, frozenset({"1/1/1"}), frozenset())
    assert report.device_found is False
    assert not report.clean
