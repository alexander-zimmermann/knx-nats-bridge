"""Generate Kaenx-Creator projects for the ETS paper devices.

The ETS project models bus participants that live in software — the
KNX-NATS bridge, the Basalte visualisation, Node-Red — as placeholder
devices whose only job is filter-table membership. This tool builds a
real product database for each of them instead: one Kaenx-Creator
project (.ae-manu) per device. Kaenx-Creator (Windows) then exports the
.knxprod that ETS imports.

Objects are **collectors**: one communication object per main group and
datapoint type — the exact subtype where ETS declares one, the main
type for the rest — and, on a ``split`` device, per direction. A device
carries a few dozen objects and every group address of a kind is
linked to the same object with one multi-select in ETS. A wiring
worksheet emitted beside each project lists, per object, exactly which
addresses belong on it.

Where a device's addresses come from is per device: a pattern or
individual address collects what the ETS export links to the matching
device(s), while a ``@file`` source lists the addresses directly, so a
device whose true footprint is defined by configuration (the bridge:
writer-rule targets plus consumer-handled addresses; Basalte: the
Studio export's bindings) is generated from that configuration and ETS
only supplies each address's name and datapoint type.

Flag modes per device:

- ``split``: addresses listed in ``--write-gas`` (a NATS consumer acts
  on writes to them) land on Write-flagged objects, the rest on
  Transmit+Read objects (the bridge sends these and answers reads from
  its responder cache). Keeps the catalog's ``writable`` vote exact.
- ``both``: Write+Transmit objects. For devices that both display and
  send (visualisation) and stay excluded from the write vote anyway.

A template .ae-manu saved by the target Kaenx-Creator installation
supplies everything version-specific (mask, load procedures, language);
only naming, identity and the object tables are rewritten. Kaenx-Creator
re-links datapoint types by number on load, so the emitted objects only
carry ``TypeNumber`` and a correct ``ObjectSize``.

Example:
    knxproj-to-kaenx --input project.knxproj --template empty.ae-manu \\
        --device '@bridge-gas.txt=KNX-NATS-Bridge:split' --write-gas consumed.txt \\
        --device '@basalte-gas.txt=Basalte Core S4:both' --output-dir out/
"""

from __future__ import annotations

import argparse
import copy
import json
import logging
import re
import sys
import uuid
import zipfile
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from knx_nats_bridge.tools.knxproj_to_yaml import _load_project, add_password_argument

logger = logging.getLogger(__name__)

_SHARE = "Kaenx.Creator.Share"

# Datapoint master data as Kaenx-Creator ships it (Data/datapoints.xml):
# main number -> size in bits. Objects must not reference a type the
# target application cannot re-link on load, so anything outside this
# table is skipped and reported.
# fmt: off
_DPT_SIZE_BITS: dict[int, int] = {
    1: 1, 2: 2, 3: 4, 4: 8, 5: 8, 6: 8, 7: 16, 8: 16, 9: 16, 10: 24,
    11: 24, 12: 32, 13: 32, 14: 32, 15: 32, 16: 112, 17: 8, 18: 8,
    19: 64, 20: 8, 21: 8, 22: 16, 23: 2, 25: 8, 26: 8, 27: 32, 29: 64,
    30: 24, 206: 24, 217: 16, 219: 48, 222: 48, 225: 24, 229: 48,
    230: 64, 232: 24, 234: 16, 235: 48, 236: 8, 237: 16, 238: 8,
    240: 24, 241: 32, 242: 48, 244: 16, 245: 48, 246: 16, 249: 48,
    250: 24, 251: 48, 252: 40, 254: 24, 255: 64, 275: 64,
}
# fmt: on

# Known subtype numbers per main type, same source. Collectors are cut
# per subtype so ETS shows the exact unit; an address whose subtype
# Kaenx-Creator does not know joins the main-type collector, because a
# subtype the target application cannot re-link would fail its load.
_DPT_SUBTYPES: dict[int, frozenset[int]] = {
    1: frozenset(
        {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 21, 22, 23, 24, 100}
    ),
    2: frozenset({1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
    3: frozenset({7, 8}),
    4: frozenset({1, 2}),
    5: frozenset({1, 3, 4, 5, 6, 10, 100}),
    6: frozenset({1, 10, 20}),
    7: frozenset({1, 2, 3, 4, 5, 6, 7, 10, 11, 12, 13, 600}),
    8: frozenset({1, 2, 3, 4, 5, 6, 7, 10, 11, 12}),
    9: frozenset({1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30}),
    10: frozenset({1}),
    11: frozenset({1}),
    12: frozenset({1, 100, 101, 102, 1200, 1201}),
    13: frozenset({1, 2, 10, 11, 12, 13, 14, 15, 16, 100, 1200, 1201}),
    14: frozenset(
        {
            0,
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            10,
            11,
            12,
            13,
            14,
            15,
            16,
            17,
            18,
            19,
            20,
            21,
            22,
            23,
            24,
            25,
            26,
            27,
            28,
            29,
            30,
            31,
            32,
            33,
            34,
            35,
            36,
            37,
            38,
            39,
            40,
            41,
            42,
            43,
            44,
            45,
            46,
            47,
            48,
            49,
            50,
            51,
            52,
            53,
            54,
            55,
            56,
            57,
            58,
            59,
            60,
            61,
            62,
            63,
            64,
            65,
            66,
            67,
            68,
            69,
            70,
            71,
            72,
            73,
            74,
            75,
            76,
            77,
            78,
            79,
            1200,
            1201,
        }
    ),
    15: frozenset({0}),
    16: frozenset({0, 1}),
    17: frozenset({1}),
    18: frozenset({1}),
    19: frozenset({1}),
    20: frozenset(
        {
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            11,
            12,
            13,
            14,
            17,
            20,
            21,
            22,
            100,
            101,
            102,
            103,
            104,
            105,
            106,
            107,
            108,
            109,
            110,
            111,
            112,
            113,
            114,
            115,
            116,
            120,
            121,
            122,
            600,
            601,
            602,
            603,
            604,
            605,
            606,
            607,
            608,
            609,
            610,
            611,
            801,
            802,
            803,
            804,
            1000,
            1001,
            1002,
            1003,
        }
    ),
    21: frozenset({1, 2, 100, 101, 102, 103, 104, 105, 106, 107, 601, 1000, 1001, 1010}),
    22: frozenset({100, 101, 102, 103, 1000, 1010}),
    23: frozenset({1, 2, 3, 102}),
    25: frozenset({1000}),
    26: frozenset({1}),
    27: frozenset({1}),
    29: frozenset({10, 11, 12}),
    30: frozenset({1010}),
    206: frozenset({100, 102, 104, 105}),
    217: frozenset({1}),
    219: frozenset({1}),
    222: frozenset({100, 101}),
    225: frozenset({1, 2}),
    229: frozenset({1}),
    230: frozenset({1000}),
    232: frozenset({600}),
    234: frozenset({1}),
    235: frozenset({1}),
    236: frozenset({1}),
    237: frozenset({600}),
    238: frozenset({600}),
    240: frozenset({800}),
    241: frozenset({800}),
    242: frozenset({600}),
    244: frozenset({600}),
    245: frozenset({600}),
    246: frozenset({600}),
    249: frozenset({600}),
    250: frozenset({600}),
    251: frozenset({600}),
    252: frozenset({600}),
    254: frozenset({600}),
    255: frozenset({1}),
    275: frozenset({100, 101}),
}

_GA_RE = re.compile(r"^(\d{1,2})/(\d)/(\d{1,3})$")

# Direction of a collector object, in bus terms: displayed function
# text and the flags that implement it.
_DIRECTIONS = {
    "transmit": ("sendet auf den Bus", {"transmit": True, "read": True}),
    "write": ("empfängt vom Bus", {"write": True}),
    "both": ("sendet und empfängt", {"write": True, "transmit": True}),
}


@dataclass(frozen=True)
class DeviceSpec:
    """One ``--device SOURCE=NAME:MODE`` argument, parsed.

    ``source`` is either a pattern selecting ETS device(s) or, prefixed
    with ``@``, a file listing the group addresses directly — for a
    device whose true footprint lives in configuration rather than in
    the ETS project.
    """

    source: str
    name: str
    mode: str  # "split" | "both"
    app_number: int

    @property
    def slug(self) -> str:
        return re.sub(r"[^A-Z0-9]+", "-", self.name.upper()).strip("-")


@dataclass
class Collector:
    """One collector object and the addresses that belong on it."""

    main_group: int
    dpt_main: int
    dpt_sub: int | None  # None collects the addresses without a (known) subtype
    direction: str  # key into _DIRECTIONS
    number: int = 0  # ETS object number; assigned by the registry, else by position
    text: str = ""
    entries: list[tuple[str, str]] = field(default_factory=list)  # (ga, ETS name)
    legacy: bool = False  # kept from the registry, no address of the configuration

    @property
    def dpt_label(self) -> str:
        if self.dpt_sub is None:
            return f"{self.dpt_main}.xxx"
        return f"{self.dpt_main}.{self.dpt_sub:03d}"

    @property
    def key(self) -> str:
        """The object's identity across versions: also its name in the
        product data, so an installed device tells which objects it has."""
        return f"hg{self.main_group}-dpt{self.dpt_label}-{self.direction}"


_KEY_RE = re.compile(r"^hg(\d+)-dpt(\d+)(?:\.(\d+|xxx))?-(transmit|write|both)$")


def parse_collector_key(key: str) -> Collector | None:
    """Rebuild a collector from its key; ``None`` if it is not one of ours.

    Accepts the first generation's spelling without a subtype part
    (``hg2-dpt1-both``), which meant the same as today's ``.xxx``.
    """
    match = _KEY_RE.match(key)
    if not match:
        return None
    main_group, dpt_main, sub, direction = match.groups()
    dpt_sub = None if sub in (None, "xxx") else int(sub)
    return Collector(
        main_group=int(main_group), dpt_main=int(dpt_main), dpt_sub=dpt_sub, direction=direction
    )


@dataclass
class DeviceReport:
    """What a device build produced and what it had to leave out."""

    objects: int = 0
    links: int = 0
    app_version: int = 0  # version byte; ETS shows high.low nibble
    write: int = 0  # addresses on Write-flagged objects
    transmit: int = 0  # addresses on Transmit-flagged objects
    collectors: list[Collector] = field(default_factory=list)
    skipped: list[tuple[str, str]] = field(default_factory=list)  # (ga, reason)
    unmatched_write_gas: list[str] = field(default_factory=list)
    missing: list[str] = field(default_factory=list)  # listed in a @file, absent from ETS
    new_objects: list[Collector] = field(default_factory=list)  # not on the installed device
    # (number, installed name, links) — installed objects the new version drops or renumbers
    lost: list[tuple[int, str, int]] = field(default_factory=list)


def parse_device_spec(raw: str, app_number: int) -> DeviceSpec:
    """``SOURCE=NAME:MODE`` -> DeviceSpec. NAME may contain colons-free text."""
    source, sep, rest = raw.partition("=")
    name, sep2, mode = rest.rpartition(":")
    if not sep or not sep2 or not source.strip() or not name.strip():
        raise SystemExit(f"invalid --device {raw!r}: expected SOURCE=NAME:MODE")
    if mode not in ("split", "both"):
        raise SystemExit(f"invalid --device {raw!r}: mode must be 'split' or 'both'")
    return DeviceSpec(source=source.strip(), name=name.strip(), mode=mode, app_number=app_number)


def read_ga_list(text: str, origin: str) -> frozenset[str]:
    """One address per line, as ``M/C/S`` or NATS subject ``<prefix>.M.C.S``."""
    gas: set[str] = set()
    for raw in text.splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        if "/" not in line:
            line = "/".join(line.split(".")[-3:])
        if not _GA_RE.match(line):
            raise SystemExit(f"{origin}: {raw.strip()!r} is not a group address")
        gas.add(line)
    return frozenset(gas)


def _ga_sort_key(ga: str) -> tuple[int, int, int]:
    main, middle, sub = ga.split("/")
    return (int(main), int(middle), int(sub))


def device_group_addresses(project_data: Mapping[str, Any], pattern: str) -> dict[str, Any]:
    """Group addresses carried by devices matching ``pattern``.

    Matching mirrors the catalog extractor's ``--ignore-write-from``: a
    case-insensitive substring test against device name, manufacturer
    and hardware name, so one pattern can address a whole product or a
    single named device. A pattern shaped like an individual address
    (``1.1.240``) selects exactly that device instead — the only handle
    left when several placeholders share one product and no name.
    """
    devices = project_data.get("devices", {}) or {}
    comm_objects = project_data.get("communication_objects", {}) or {}
    lowered = pattern.lower()

    if re.fullmatch(r"\d{1,2}\.\d{1,2}\.\d{1,3}", pattern):
        matching_addresses = {addr for addr in devices if str(addr) == pattern}
    else:
        matching_addresses = {
            addr
            for addr, device in devices.items()
            if isinstance(device, dict)
            and lowered
            in " ".join(
                str(device.get(k) or "") for k in ("name", "manufacturer_name", "hardware_name")
            ).lower()
        }

    matching_co_ids = {
        str(co_id)
        for co_id, co in comm_objects.items()
        if isinstance(co, dict) and str(co.get("device_address")) in matching_addresses
    }

    result: dict[str, Any] = {}
    for ga, info in (project_data.get("group_addresses", {}) or {}).items():
        if not isinstance(info, dict):
            continue
        co_ids = info.get("communication_object_ids") or []
        if any(str(co_id) in matching_co_ids for co_id in co_ids):
            result[str(ga)] = info
    return result


def _main_group_names(project_data: Mapping[str, Any]) -> dict[int, str]:
    """{main group number -> ETS range name}, best effort."""
    names: dict[int, str] = {}
    for key, group_range in (project_data.get("group_ranges", {}) or {}).items():
        if isinstance(group_range, dict) and str(key).isdigit():
            name = str(group_range.get("name") or "").strip()
            if name:
                names[int(key)] = name
    return names


def _translation(language: Mapping[str, Any], text: str) -> dict[str, Any]:
    return {
        "$type": f"Kaenx.Creator.Models.Translation, {_SHARE}",
        "Language": dict(language),
        "Text": text,
        "Preview": text,
    }


def _com_object(number: int, collector: Collector, language: Mapping[str, Any]) -> dict[str, Any]:
    function_text, flags = _DIRECTIONS[collector.direction]
    sub = collector.dpt_sub
    return {
        "$type": f"Kaenx.Creator.Models.ComObject, {_SHARE}",
        "UId": number,
        "Id": number,
        "Name": collector.key,
        "Text": [_translation(language, collector.text)],
        "TranslationText": False,
        "FunctionText": [_translation(language, function_text)],
        "TranslationFunctionText": False,
        "Number": number,
        "FlagRead": flags.get("read", False),
        "FlagWrite": flags.get("write", False),
        "FlagTrans": flags.get("transmit", False),
        "FlagComm": True,
        "FlagUpdate": False,
        "FlagOnInit": False,
        "TypeValue": None,
        "HasDpt": True,
        "HasDpts": sub is not None,
        "ObjectSize": _DPT_SIZE_BITS[collector.dpt_main],
        "SubTypeNumber": str(sub) if sub is not None else None,
        "SubType": None,
        "TypeNumber": str(collector.dpt_main),
        "Type": None,
        "UseTextParameter": False,
        "ParameterRef": -1,
    }


def _com_object_ref(com_object: Mapping[str, Any], language: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "$type": f"Kaenx.Creator.Models.ComObjectRef, {_SHARE}",
        "IsAutoGenerated": True,
        "UId": com_object["UId"],
        "Id": com_object["Id"],
        "Name": com_object["Name"],
        "Text": [_translation(language, "")],
        "TranslationText": False,
        "OverwriteText": False,
        "FunctionText": [_translation(language, "")],
        "TranslationFunctionText": False,
        "OverwriteFunctionText": False,
        "OverwriteDpt": False,
        "OverwriteDpst": False,
        "SubTypeNumber": None,
        "SubType": None,
        "TypeNumber": None,
        "Type": None,
        "OverwriteOS": False,
        "ObjectSize": com_object["ObjectSize"],
        "ComObject": com_object["UId"],
        "FlagRead": False,
        "OverwriteFR": False,
        "FlagWrite": False,
        "OverwriteFW": False,
        "FlagTrans": False,
        "OverwriteFT": False,
        "FlagComm": False,
        "OverwriteFC": False,
        "FlagUpdate": False,
        "OverwriteFU": False,
        "FlagOnInit": False,
        "OverwriteFOI": False,
        "UseTextParameter": False,
        "ParameterRef": -1,
    }


def _dyn_block(
    block_id: int, name: str, refs: Iterable[Mapping[str, Any]], language: Mapping[str, Any]
) -> dict[str, Any]:
    return {
        "$type": f"Kaenx.Creator.Models.Dynamic.DynParaBlock, {_SHARE}",
        "IsExpanded": False,
        "Id": block_id,
        "Name": f"block-{block_id}",
        "Text": [_translation(language, name)],
        "TranslationText": False,
        "UseParameterRef": False,
        "ParameterRef": -1,
        "UseTextParameter": False,
        "TextRef": 0,
        "Layout": 0,
        "IsInline": False,
        "UseIcon": False,
        "IconId": -1,
        "Access": 2,
        "ShowInComObjectTree": True,
        "Rows": [],
        "Columns": [],
        "Items": [
            {
                "$type": f"Kaenx.Creator.Models.Dynamic.DynComObject, {_SHARE}",
                "IsExpanded": False,
                "Name": "",
                "ComObjectRef": ref["UId"],
                "Items": None,
            }
            for ref in refs
        ],
    }


def _dynamics(blocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Root -> independent channel -> one block per main group.

    The nesting is the one Kaenx-Creator builds itself; objects placed
    anywhere else fail its publish checks.
    """
    channel = {
        "$type": f"Kaenx.Creator.Models.Dynamic.DynChannelIndependent, {_SHARE}",
        "IsExpanded": True,
        "Name": "",
        "Items": blocks,
    }
    return [
        {
            "$type": f"Kaenx.Creator.Models.Dynamic.DynamicMain, {_SHARE}",
            "IsExpanded": True,
            "Name": "Root Knoten",
            "Items": [channel],
        }
    ]


_APP_VERSION_RE = re.compile(r"_A-[0-9A-Fa-f]+-([0-9A-Fa-f]{2})-")


def _imported_app_version(project_data: Mapping[str, Any], order_number: str) -> int | None:
    """Version byte of the device's application as currently imported.

    Generated devices carry their name slug as the ETS order number, so
    that is the match. The version is the third segment of the
    application program id (``M-00FA_A-AF66-11-0000`` -> 0x11 = V 1.1).
    """
    versions = []
    for device in (project_data.get("devices", {}) or {}).values():
        if not isinstance(device, dict) or device.get("order_number") != order_number:
            continue
        match = _APP_VERSION_RE.search(str(device.get("application") or ""))
        if match:
            versions.append(int(match.group(1), 16))
    return max(versions, default=None)


def _rename_first_section(catalog: list[Any], name: str, language: Mapping[str, Any]) -> None:
    """Give the first exported catalog section the wanted name."""
    for root in catalog:
        if not isinstance(root, dict):
            continue
        for item in root.get("Items") or []:
            if isinstance(item, dict) and item.get("IsSection"):
                item["Name"] = name
                item["Number"] = name
                item["Text"] = [_translation(language, name)]
                return


def _prune_unnumbered_catalog_sections(catalog: list[Any]) -> None:
    """Drop catalog sections without a section number, recursively.

    Creating a project in the Kaenx-Creator GUI leaves an empty "Neue
    Kategorie" section behind, and its missing number fails the publish
    checks. The root section is exempt — it is never exported.
    """
    for root in catalog:
        if not isinstance(root, dict):
            continue
        root["Items"] = _numbered_items(root.get("Items") or [])


def _numbered_items(items: list[Any]) -> list[Any]:
    kept = []
    for item in items:
        if not isinstance(item, dict):
            continue
        if item.get("IsSection") and not (item.get("Number") or "").strip():
            continue
        item["Items"] = _numbered_items(item.get("Items") or [])
        kept.append(item)
    return kept


def build_collectors(
    spec: DeviceSpec,
    project_data: Mapping[str, Any],
    write_gas: frozenset[str],
    report: DeviceReport,
    registry: dict[str, int] | None = None,
    installed: Mapping[int, str] | None = None,
) -> list[Collector]:
    """Resolve the device's addresses and group them into the ordered,
    numbered collector list; counts and gaps land on ``report``.

    Shared between generation and the wiring check so both see the
    identical objects — same cut, same order, same numbers.

    Numbers come from ``registry`` (key -> number, append-only, updated
    in place): a collector keeps its number forever, a new one gets the
    next free number, and a registered key the configuration no longer
    produces is still emitted as a legacy object so links on it
    survive. ``installed`` (number -> key, from the device in the ETS
    export) is merged into the registry first, so the registry is
    seeded from whatever is on the device and never contradicts it.
    Without a registry the numbers are positional.
    """
    if spec.source.startswith("@"):
        path = Path(spec.source[1:])
        listed = read_ga_list(path.read_text(encoding="utf-8"), origin=str(path))
        if not listed:
            raise SystemExit(f"--device {spec.name}: {path} lists no group addresses")
        all_gas = project_data.get("group_addresses", {}) or {}
        gas = {ga: all_gas[ga] for ga in listed if isinstance(all_gas.get(ga), dict)}
        report.missing = sorted(listed - set(gas), key=_ga_sort_key)
    else:
        gas = device_group_addresses(project_data, spec.source)
        if not gas:
            available = sorted(
                f"{addr} {device.get('name') or device.get('hardware_name') or ''}".strip()
                for addr, device in (project_data.get("devices", {}) or {}).items()
                if isinstance(device, dict)
            )
            raise SystemExit(
                f"--device {spec.source!r} matches no device carrying group addresses; "
                "devices in the project:\n  " + "\n  ".join(available)
            )

    hg_names = _main_group_names(project_data)
    collectors: dict[tuple[int, int, int | None, str], Collector] = {}
    for ga in sorted(gas, key=_ga_sort_key):
        info = gas[ga]
        dpt = info.get("dpt")
        main = dpt.get("main") if isinstance(dpt, dict) else None
        if main is None:
            report.skipped.append((ga, "no DPT assigned in ETS"))
            continue
        if int(main) not in _DPT_SIZE_BITS:
            report.skipped.append((ga, f"DPT {main} unknown to Kaenx-Creator"))
            continue

        if spec.mode == "both":
            direction = "both"
        elif ga in write_gas:
            direction = "write"
        else:
            direction = "transmit"
        main_group = int(ga.split("/")[0])
        raw_sub = dpt.get("sub") if isinstance(dpt, dict) else None
        sub = int(raw_sub) if raw_sub is not None else None
        if sub is not None and sub not in _DPT_SUBTYPES.get(int(main), frozenset()):
            # A subtype Kaenx-Creator cannot re-link would fail its load;
            # the address joins the main-type collector instead.
            sub = None
        key = (main_group, int(main), sub, direction)
        collector = collectors.setdefault(
            key,
            Collector(main_group=main_group, dpt_main=int(main), dpt_sub=sub, direction=direction),
        )
        collector.entries.append((ga, str(info.get("name") or "")))
        report.links += 1
        report.write += direction in ("write", "both")
        report.transmit += direction in ("transmit", "both")
    if spec.mode == "split":
        report.unmatched_write_gas = sorted(write_gas - set(gas), key=_ga_sort_key)

    # Positional order: main group, datapoint type (main-type collector
    # before its subtypes), sending before receiving. It numbers a
    # device without a registry, and decides the order in which new
    # collectors take the next free numbers.
    ordered = sorted(
        collectors.values(),
        key=lambda c: (
            c.main_group,
            c.dpt_main,
            -1 if c.dpt_sub is None else c.dpt_sub,
            c.direction == "write",
        ),
    )
    # Installed names in the first generation's spelling mean the same
    # object as today's key; compare and register them normalised.
    installed = {
        number: (parsed.key if (parsed := parse_collector_key(name)) else name)
        for number, name in (installed or {}).items()
    }
    if registry is None:
        for number, collector in enumerate(ordered, start=1):
            collector.number = number
    else:
        ordered = _number_from_registry(ordered, registry, installed, spec.name)
    for collector in ordered:
        hg_name = hg_names.get(collector.main_group, f"Hauptgruppe {collector.main_group}")
        suffix = {"write": " · empfängt", "transmit": " · sendet"}.get(collector.direction, "")
        collector.text = f"{hg_name} · {collector.dpt_label}{suffix}"
    if installed:
        emitted = {c.number: c.key for c in ordered}
        report.new_objects = [c for c in ordered if c.number not in installed]
        report.lost = [
            (number, key, 0)
            for number, key in sorted(installed.items())
            if emitted.get(number) != key
        ]
    report.objects = len(ordered)
    report.collectors = ordered
    return ordered


def _number_from_registry(
    ordered: list[Collector],
    registry: dict[str, int],
    installed: Mapping[int, str],
    device: str,
) -> list[Collector]:
    """Assign registry numbers; append the unknown; keep the orphaned."""
    for number, key in sorted(installed.items()):
        if registry.get(key) not in (None, number):
            raise SystemExit(
                f"{device}: installed object {number} is {key!r} but the registry holds it as "
                f"number {registry[key]} — the device in the export and the registry disagree; "
                "delete the device's registry section to re-seed it from the export"
            )
        taken = {k for k, n in registry.items() if n == number and k != key}
        if taken:
            raise SystemExit(
                f"{device}: installed object {number} is {key!r} but the registry has that "
                f"number for {taken.pop()!r} — delete the device's registry section to re-seed "
                "it from the export"
            )
        registry[key] = number
    next_free = max(registry.values(), default=0) + 1
    by_key = {c.key: c for c in ordered}
    for collector in ordered:
        if collector.key in registry:
            collector.number = registry[collector.key]
        else:
            collector.number = next_free
            registry[collector.key] = next_free
            next_free += 1
    for key, number in registry.items():
        if key in by_key:
            continue
        legacy = parse_collector_key(key)
        if legacy is None:
            raise SystemExit(f"{device}: registry key {key!r} is not a collector key")
        legacy.number = number
        legacy.legacy = True
        by_key[key] = legacy
    return sorted(by_key.values(), key=lambda c: c.number)


def installed_objects(
    knxproj: Path, project_data: Mapping[str, Any], order_number: str
) -> dict[int, str]:
    """{object number -> key} of the application installed on the device
    with that order number, read from the product data in the export;
    empty when the device is not in the project."""
    application_id = next(
        (
            str(device.get("application") or "")
            for device in (project_data.get("devices", {}) or {}).values()
            if isinstance(device, dict) and device.get("order_number") == order_number
        ),
        "",
    )
    if not application_id:
        return {}
    manufacturer = application_id.split("_", 1)[0]
    with zipfile.ZipFile(knxproj) as archive:
        try:
            xml = archive.read(f"{manufacturer}/{application_id}.xml").decode("utf-8")
        except KeyError:
            return {}
    objects: dict[int, str] = {}
    for tag in re.findall(r"<ComObject\b[^>]*/?>", xml):
        name = re.search(r'\bName="([^"]*)"', tag)
        number = re.search(r'\bNumber="(\d+)"', tag)
        if name and number:
            objects[int(number.group(1))] = name.group(1)
    return objects


def installed_links(project_data: Mapping[str, Any], order_number: str) -> dict[int, int]:
    """{object number -> linked group addresses} on the installed device."""
    addresses = {
        str(addr)
        for addr, device in (project_data.get("devices", {}) or {}).items()
        if isinstance(device, dict) and device.get("order_number") == order_number
    }
    counts: dict[int, int] = {}
    for co in (project_data.get("communication_objects", {}) or {}).values():
        if isinstance(co, dict) and str(co.get("device_address")) in addresses:
            number = int(co.get("number") or 0)
            counts[number] = counts.get(number, 0) + len(co.get("group_address_links") or [])
    return counts


def build_device_model(
    template: Mapping[str, Any],
    spec: DeviceSpec,
    project_data: Mapping[str, Any],
    write_gas: frozenset[str],
    catalog_section: str | None = None,
    registry: dict[str, int] | None = None,
    installed: Mapping[int, str] | None = None,
    published: int = 0,
) -> tuple[dict[str, Any], DeviceReport]:
    """Fill a copy of the template with one collector object per
    (main group, datapoint type, direction). ``published`` is the
    highest version already handed to Kaenx-Creator for this device,
    from the registry — the export alone cannot know it."""
    model = copy.deepcopy(dict(template))
    application = model["Application"]
    language = dict(application["Languages"][0])
    report = DeviceReport()

    ordered = build_collectors(spec, project_data, write_gas, report, registry, installed)
    com_objects = [_com_object(collector.number, collector, language) for collector in ordered]

    refs = [_com_object_ref(co, language) for co in com_objects]
    hg_names = _main_group_names(project_data)
    blocks: list[dict[str, Any]] = []
    for main_group in sorted({c.main_group for c in ordered}):
        block_refs = [
            ref
            for ref, collector in zip(refs, ordered, strict=True)
            if collector.main_group == main_group
        ]
        hg_name = hg_names.get(main_group, f"Hauptgruppe {main_group}")
        blocks.append(
            _dyn_block(len(blocks) + 1, f"{main_group} · {hg_name}", block_refs, language)
        )

    application["ComObjects"] = com_objects
    application["ComObjectRefs"] = refs
    application["Dynamics"] = _dynamics(blocks)
    application["HighestComNumber"] = max((c.number for c in ordered), default=0)
    # Application.Number is the version byte (0x10 = V 1.0; ETS shows
    # high.low nibble), not the application's identity — that is
    # Info.AppNumber. ETS silently refuses to re-import an application
    # whose version it already knows, so the version is derived from
    # the ETS export: one above what is imported, or the template's
    # V 1.0 for a device ETS has never seen.
    base = int(application["Number"])
    imported = _imported_app_version(project_data, spec.slug)
    unchanged = bool(installed) and {c.number: c.key for c in ordered} == {
        n: (parsed.key if (parsed := parse_collector_key(k)) else k)
        for n, k in (installed or {}).items()
    }
    if imported is not None and unchanged:
        # Nothing to publish: keep the installed version rather than
        # burning one on every run.
        version = imported
    else:
        version = max(imported or 0, published, base - 1) + 1
        if imported is None and published < base:
            version = base
    application["Number"] = version
    report.app_version = version
    # ETS offers "update application program" only for a version that
    # declares which earlier versions it replaces. It keeps parameters,
    # not group links (tested twice on these products) — the links move
    # by hand onto a second device instance, object by object, which is
    # why numbers must never change. Name every earlier version anyway,
    # so ETS files the new one as an update of the same product.
    application["ReplacesVersions"] = " ".join(str(v) for v in range(base, version))
    application["Name"] = spec.slug.lower()
    application["NameText"] = f"V {version >> 4}.{version & 0xF} {spec.name}"
    application["Text"] = [_translation(language, spec.name)]

    model["ProjectName"] = spec.name
    model["FileName"] = spec.slug.lower()
    # Stable per device so a regeneration is a new version of the same
    # project, not a new project.
    model["Guid"] = str(uuid.uuid5(uuid.NAMESPACE_URL, f"lares-kaenx://{spec.slug}"))
    _prune_unnumbered_catalog_sections(model.get("Catalog") or [])
    if catalog_section:
        _rename_first_section(model.get("Catalog") or [], catalog_section, language)

    info_block = model["Info"]
    info_block["Name"] = spec.name
    info_block["SerialNumber"] = spec.slug
    info_block["OrderNumber"] = spec.slug
    info_block["AppNumber"] = spec.app_number
    info_block["Text"] = [_translation(language, spec.name)]
    info_block["Description"] = [_translation(language, spec.name)]

    return model, report


def wiring_worksheet(spec: DeviceSpec, report: DeviceReport) -> str:
    """Markdown checklist: per collector object, the addresses to link."""
    lines = [
        f"# Verdrahtung: {spec.name}",
        "",
        f"{report.objects} Sammel-Objekte, {report.links} Verknüpfungen. Pro Objekt in ETS:",
        "die GAs unten per Mehrfachauswahl markieren und auf das Objekt ziehen.",
    ]
    for collector in report.collectors:
        if collector.legacy:
            continue
        marker = " — NEU" if collector in report.new_objects else ""
        count = f"({len(collector.entries)} GAs)"
        lines += ["", f"## Objekt {collector.number}: {collector.text} {count}{marker}", ""]
        lines += [f"- [ ] `{ga}` {name}" for ga, name in collector.entries]
    legacy = [c for c in report.collectors if c.legacy]
    if legacy:
        lines += [
            "",
            "## Altobjekte (keine Adresse der Konfiguration; bleiben für bestehende Links)",
            "",
        ]
        lines += [f"- Objekt {c.number}: {c.text}" for c in legacy]
    if report.skipped:
        lines += ["", "## Nicht aufgenommen", ""]
        lines += [f"- `{ga}` — {reason}" for ga, reason in report.skipped]
    lines.append("")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate Kaenx-Creator projects from an ETS export"
    )
    parser.add_argument("--input", "-i", required=True, type=Path, help="Path to .knxproj file")
    add_password_argument(parser)
    parser.add_argument(
        "--template",
        required=True,
        type=Path,
        help="Empty .ae-manu saved by the target Kaenx-Creator installation",
    )
    parser.add_argument(
        "--device",
        action="append",
        required=True,
        metavar="SOURCE=NAME:MODE",
        help=(
            "Device to generate: SOURCE selects the source device(s) by "
            "case-insensitive substring against name, manufacturer and hardware "
            "name, by individual address (1.1.240), or — prefixed with @ — names "
            "a file listing the group addresses directly (one address or NATS "
            "subject per line), for a device whose footprint is defined by "
            "configuration rather than by the ETS project; NAME names the "
            "generated product; MODE is 'split' (Write-flagged collector objects "
            "for --write-gas addresses, Transmit+Read collectors otherwise) or "
            "'both' (Write+Transmit collectors). Repeatable; the application "
            "number is 100 plus the argument's position, so keep the order "
            "stable."
        ),
    )
    parser.add_argument(
        "--write-gas",
        type=Path,
        default=None,
        help=(
            "File with one group address (or NATS subject) per line: the "
            "addresses whose writes a NATS consumer acts on. Required when a "
            "device uses mode 'split'."
        ),
    )
    parser.add_argument(
        "--catalog-section",
        default=None,
        help="Rename the exported catalog section (shown inside the manufacturer in ETS)",
    )
    parser.add_argument(
        "--registry",
        type=Path,
        default=None,
        help=(
            "YAML file holding each device's object numbers (key -> number). Read, "
            "seeded from the devices in the export, extended with new objects and "
            "written back. Without it, numbers are positional and change whenever "
            "the object set does."
        ),
    )
    parser.add_argument(
        "--accept-loss",
        action="store_true",
        help=(
            "Write the projects even though updating a device to them would drop or "
            "renumber objects that carry links in the export."
        ),
    )
    parser.add_argument(
        "--output-dir", "-o", required=True, type=Path, help="Directory for the .ae-manu files"
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    specs = [parse_device_spec(raw, 100 + i) for i, raw in enumerate(args.device)]
    if len({s.slug for s in specs}) != len(specs):
        raise SystemExit("--device names collide after slugging; rename one")

    write_gas = frozenset[str]()
    if args.write_gas is not None:
        write_gas = read_ga_list(args.write_gas.read_text(encoding="utf-8"), origin="--write-gas")
    elif any(s.mode == "split" for s in specs):
        raise SystemExit("--write-gas is required when a device uses mode 'split'")

    template = json.loads(args.template.read_text(encoding="utf-8"))
    logger.info("parsing %s", args.input)
    project_data = _load_project(args.input, args.password)

    registry = read_registry(args.registry) if args.registry else None

    args.output_dir.mkdir(parents=True, exist_ok=True)
    failed = False
    outputs: list[tuple[Path, str]] = []
    for spec in specs:
        installed = installed_objects(args.input, project_data, spec.slug)
        section = registry.setdefault(spec.name, DeviceRegistry()) if registry is not None else None
        model, report = build_device_model(
            template,
            spec,
            project_data,
            write_gas,
            catalog_section=args.catalog_section,
            registry=section.objects if section is not None else None,
            installed=installed,
            published=section.version if section is not None else 0,
        )
        if section is not None and report.app_version > section.version:
            section.version = report.app_version
        links = installed_links(project_data, spec.slug)
        report.lost = [(n, key, links.get(n, 0)) for n, key, _ in report.lost]
        lost_links = sum(count for _, _, count in report.lost)

        content = json.dumps(model, indent=2, ensure_ascii=False)
        outputs.append((args.output_dir / f"{spec.slug.lower()}.ae-manu", content))
        outputs.append(
            (args.output_dir / f"{spec.slug.lower()}-wiring.md", wiring_worksheet(spec, report))
        )
        logger.info(
            "%s: V %d.%d, %d collector objects, %d links (%d write, %d transmit)",
            spec.name,
            report.app_version >> 4,
            report.app_version & 0xF,
            report.objects,
            report.links,
            report.write,
            report.transmit,
        )
        if not installed:
            logger.info("%s: not in the ETS project yet — publish, import and add it", spec.name)
        elif report.new_objects:
            logger.info(
                "%s: %d new object(s) against the installed application — publish it, add "
                "the new version as a second device in ETS, move the links over object by "
                "object (numbers match), delete the old device: %s",
                spec.name,
                len(report.new_objects),
                ", ".join(f"{c.number} {c.text}" for c in report.new_objects[:6])
                + (" …" if len(report.new_objects) > 6 else ""),
            )
        else:
            logger.info(
                "%s: same objects as the installed application — no publish needed, only links",
                spec.name,
            )
        for number, key, count in report.lost:
            level = logging.ERROR if count else logging.WARNING
            logger.log(
                level,
                "%s: installed object %d (%s) with %d link(s) has no same-numbered object in "
                "this version — its links need a new home",
                spec.name,
                number,
                key,
                count,
            )
        if lost_links and not args.accept_loss:
            logger.error(
                "%s: %d link(s) sit on objects this version drops or renumbers — nothing "
                "written; fix the registry, or pass --accept-loss to proceed anyway",
                spec.name,
                lost_links,
            )
            failed = True
        for ga, reason in report.skipped:
            logger.warning(
                "%s: skipped %s: %s — it stays on the placeholder", spec.name, ga, reason
            )
        for ga in report.missing:
            logger.warning(
                "%s: listed address %s does not exist in the ETS project — the "
                "configuration points at nothing",
                spec.name,
                ga,
            )
            failed = True
        for ga in report.unmatched_write_gas:
            logger.warning(
                "%s: consumed address %s is not on the source device — a NATS "
                "consumer acts on it but ETS does not deliver it",
                spec.name,
                ga,
            )
            failed = True
    if failed:
        return 1
    for path, content in outputs:
        path.write_text(content, encoding="utf-8")
        logger.info("wrote %s", path)
    if registry is not None and args.registry is not None:
        write_registry(args.registry, registry)
        logger.info("registry updated: %s", args.registry)
    return 0


@dataclass
class DeviceRegistry:
    """One device's section of the registry: the last version handed to
    Kaenx-Creator, and every object's number."""

    version: int = 0
    objects: dict[str, int] = field(default_factory=dict)


def read_registry(path: Path) -> dict[str, DeviceRegistry]:
    """{device name -> section}; a missing file is an empty registry."""
    if not path.is_file():
        return {}
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise SystemExit(f"{path}: expected a mapping of device names")
    registry: dict[str, DeviceRegistry] = {}
    for device, section in data.items():
        objects = section.get("objects") if isinstance(section, dict) else None
        if not isinstance(objects, dict):
            raise SystemExit(f"{path}: {device}: expected 'version' and 'objects'")
        numbers = {str(k): int(v) for k, v in objects.items()}
        if len(set(numbers.values())) != len(numbers):
            raise SystemExit(f"{path}: {device}: object numbers are not unique")
        registry[str(device)] = DeviceRegistry(int(section.get("version") or 0), numbers)
    return registry


def write_registry(path: Path, registry: Mapping[str, DeviceRegistry]) -> None:
    """Write the registry sorted by number, so the diff reads like the device."""
    header = (
        "# The generated ETS devices, one section per device: the application version\n"
        "# last handed to Kaenx-Creator, and every object's number. knxproj-to-kaenx\n"
        "# maintains this file — it seeds a section from the device in the ETS export,\n"
        "# appends every new collector with the next free number and never renumbers.\n"
        "# Keep it committed: stable numbers make moving the links onto a new device\n"
        "# version a 1:1 job (object 1 to 1, 2 to 2, ...), and the version keeps every\n"
        "# publish above the last one. A key the configuration no longer produces stays\n"
        "# here and is still emitted, so its object keeps existing; delete it only when\n"
        "# its links are gone.\n"
    )
    ordered = {
        device: {
            "version": section.version,
            "objects": dict(sorted(section.objects.items(), key=lambda kv: kv[1])),
        }
        for device, section in sorted(registry.items())
    }
    path.write_text(
        header + yaml.safe_dump(ordered, sort_keys=False, allow_unicode=True), encoding="utf-8"
    )


if __name__ == "__main__":
    sys.exit(main())
