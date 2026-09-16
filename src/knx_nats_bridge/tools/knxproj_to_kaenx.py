"""Generate Kaenx-Creator projects for the generated ETS devices.

Bus participants without a product database — the KNX-NATS bridge, the
Basalte visualisation, Node-Red, the Telenot alarm panel — sit in the
ETS project as placeholder devices whose only job is filter-table
membership. This tool builds a real product database for each of them
instead: one Kaenx-Creator project (.ae-manu) per device. Kaenx-Creator
(Windows) then exports the .knxprod that ETS imports.

Objects are **collectors**: one communication object per main group,
datapoint type — the exact subtype where ETS declares one, the main
type for the rest — and direction. A device carries a few dozen objects
and every group address of a kind is linked to the same object with one
multi-select in ETS. A wiring worksheet emitted beside each project
lists, per object, exactly which addresses belong on it.

Every address has a direction, in bus terms: ``transmit`` — the device
sends it and answers reads (Transmit+Read object); ``write`` — the
device acts on writes to it (Write object); ``both`` (Write+Transmit).
The Write flag is what the catalog's ``writable`` vote counts, so the
direction must be exact per address.

Where a device's addresses come from is per device: a ``@file`` source
is the device's footprint — one ``<address> <direction>`` per line,
extracted from the configuration that defines the device (the bridge:
writer-rule targets and consumer-handled addresses; Basalte: the Studio
export's bindings; the Telenot: its compasX export) — and ETS only
supplies each address's name and datapoint type. A pattern or
individual address instead collects what the ETS export links to the
matching device(s), with the direction the linked objects' flags say.

A template .ae-manu saved by the target Kaenx-Creator installation
supplies everything version-specific (mask, load procedures, language);
only naming, identity and the object tables are rewritten. Kaenx-Creator
re-links datapoint types by number on load, so the emitted objects only
carry ``TypeNumber`` and a correct ``ObjectSize``.

Example:
    knxproj-to-kaenx --input project.knxproj --template empty.ae-manu \\
        --device '@bridge=KNX-NATS-Bridge' --device '@basalte=Basalte Core S4' \\
        --output-dir out/
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
    """One ``--device SOURCE=NAME`` argument, parsed.

    ``source`` is either a pattern selecting ETS device(s) or, prefixed
    with ``@``, the footprint file listing the group addresses with
    their direction — for a device whose true footprint lives in
    configuration rather than in the ETS project.
    """

    source: str
    name: str
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
    number: int = 0  # ETS object number: the position in the sorted collector list
    text: str = ""
    entries: list[tuple[str, str]] = field(default_factory=list)  # (ga, ETS name)
    installed_number: int | None = None  # the object's number on the device in the export

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


# Object order within one main group and type: sending, two-way, receiving.
_DIRECTION_ORDER = ("transmit", "both", "write")

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
    missing: list[str] = field(default_factory=list)  # in the footprint, absent from ETS
    new_objects: list[Collector] = field(default_factory=list)  # not on the installed device
    renumbered: list[Collector] = field(default_factory=list)  # installed under another number
    # (installed number, text, links) — installed objects this version no longer has
    lost: list[tuple[int, str, int]] = field(default_factory=list)


def parse_device_spec(raw: str, app_number: int) -> DeviceSpec:
    """``SOURCE=NAME`` -> DeviceSpec."""
    source, sep, name = raw.partition("=")
    if not sep or not source.strip() or not name.strip():
        raise SystemExit(f"invalid --device {raw!r}: expected SOURCE=NAME")
    return DeviceSpec(source=source.strip(), name=name.strip(), app_number=app_number)


def read_footprint(text: str, origin: str) -> dict[str, str]:
    """{address -> direction} from a footprint: one ``<address>
    <direction>`` per line, the address as ``M/C/S`` or NATS subject
    ``<prefix>.M.C.S``, the direction ``transmit``, ``write`` or
    ``both``; ``#`` starts a comment.

    Footprints are generated, never written by hand, so a line without
    a direction is an error named by file and line, not a default.
    """
    directions: dict[str, str] = {}
    for number, raw in enumerate(text.splitlines(), start=1):
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        where = f"{origin}:{number}"
        fields = line.split()
        if len(fields) != 2:
            raise SystemExit(f"{where}: expected '<address> <direction>', got {line!r}")
        ga, direction = fields
        if "/" not in ga:
            ga = "/".join(ga.split(".")[-3:])
        if not _GA_RE.match(ga):
            raise SystemExit(f"{where}: {fields[0]!r} is not a group address")
        if direction not in _DIRECTIONS:
            raise SystemExit(
                f"{where}: direction must be transmit, write or both, got {direction!r}"
            )
        if directions.get(ga, direction) != direction:
            raise SystemExit(f"{where}: {ga} listed again as {direction!r}, was {directions[ga]!r}")
        directions[ga] = direction
    return directions


def _ga_sort_key(ga: str) -> tuple[int, int, int]:
    main, middle, sub = ga.split("/")
    return (int(main), int(middle), int(sub))


def device_group_addresses(project_data: Mapping[str, Any], pattern: str) -> dict[str, str | None]:
    """{group address -> direction} carried by devices matching ``pattern``.

    Matching mirrors the catalog extractor's ``--ignore-write-from``: a
    case-insensitive substring test against device name, manufacturer
    and hardware name, so one pattern can address a whole product or a
    single named device. A pattern shaped like an individual address
    (``1.1.240``) selects exactly that device instead — the only handle
    left when several placeholders share one product and no name.

    The direction is what the flags of the linked objects say — Write,
    Transmit, or both; ``None`` when they carry neither.
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

    matching = {
        str(co_id): co
        for co_id, co in comm_objects.items()
        if isinstance(co, dict) and str(co.get("device_address")) in matching_addresses
    }

    result: dict[str, str | None] = {}
    for ga, info in (project_data.get("group_addresses", {}) or {}).items():
        if not isinstance(info, dict):
            continue
        linked = [
            matching[str(co_id)]
            for co_id in info.get("communication_object_ids") or []
            if str(co_id) in matching
        ]
        if linked:
            result[str(ga)] = _direction_of(linked)
    return result


def _direction_of(linked: Iterable[Mapping[str, Any]]) -> str | None:
    """Direction the Write/Transmit flags of these objects add up to; ``None`` for neither."""
    flags = {
        flag
        for co in linked
        for flag in ("write", "transmit")
        if isinstance(co.get("flags"), dict) and co["flags"].get(flag)
    }
    if flags == {"write", "transmit"}:
        return "both"
    return next(iter(flags), None)


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
    report: DeviceReport,
    installed: Mapping[int, str] | None = None,
) -> list[Collector]:
    """Resolve the device's addresses and group them into the ordered,
    numbered collector list; counts and gaps land on ``report``.

    Shared between generation and the wiring check so both see the
    identical objects — same cut, same order, same numbers.

    Numbers are positional: main group, datapoint type, direction. A
    changed object set therefore renumbers, which is fine because ETS
    keeps no link across an application update anyway — every new
    version is a second device whose links are moved by hand, and the
    worksheet says for each object where it was. ``installed`` (number
    -> key, from the device in the ETS export) tells which objects are
    new, which moved, and which the new version no longer has.
    """
    directions: Mapping[str, str | None]
    if spec.source.startswith("@"):
        path = Path(spec.source[1:])
        directions = read_footprint(path.read_text(encoding="utf-8"), origin=str(path))
        if not directions:
            raise SystemExit(f"--device {spec.name}: {path} lists no group addresses")
    else:
        directions = device_group_addresses(project_data, spec.source)
        if not directions:
            available = sorted(
                f"{addr} {device.get('name') or device.get('hardware_name') or ''}".strip()
                for addr, device in (project_data.get("devices", {}) or {}).items()
                if isinstance(device, dict)
            )
            raise SystemExit(
                f"--device {spec.source!r} matches no device carrying group addresses; "
                "devices in the project:\n  " + "\n  ".join(available)
            )
    all_gas = project_data.get("group_addresses", {}) or {}
    gas = {ga: all_gas[ga] for ga in directions if isinstance(all_gas.get(ga), dict)}
    report.missing = sorted(set(directions) - set(gas), key=_ga_sort_key)

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
        direction = directions[ga]
        if direction is None:
            report.skipped.append((ga, "neither Write nor Transmit on the source device"))
            continue

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

    # Positional order: main group, datapoint type (main-type collector
    # before its subtypes), sending before two-way before receiving.
    ordered = sorted(
        collectors.values(),
        key=lambda c: (
            c.main_group,
            c.dpt_main,
            -1 if c.dpt_sub is None else c.dpt_sub,
            _DIRECTION_ORDER.index(c.direction),
        ),
    )
    for number, collector in enumerate(ordered, start=1):
        collector.number = number

    def text_of(collector: Collector) -> str:
        hg_name = hg_names.get(collector.main_group, f"Hauptgruppe {collector.main_group}")
        suffix = {"write": " · empfängt", "transmit": " · sendet"}.get(collector.direction, "")
        return f"{hg_name} · {collector.dpt_label}{suffix}"

    for collector in ordered:
        collector.text = text_of(collector)
    # Installed names in the first generation's spelling mean the same
    # object as today's key; compare them normalised.
    installed_by_key = {
        (parsed.key if (parsed := parse_collector_key(name)) else name): number
        for number, name in (installed or {}).items()
    }
    if installed_by_key:
        for collector in ordered:
            collector.installed_number = installed_by_key.get(collector.key)
        report.new_objects = [c for c in ordered if c.installed_number is None]
        report.renumbered = [
            c for c in ordered if c.installed_number is not None and c.installed_number != c.number
        ]
        emitted = {c.key for c in ordered}
        for name, number in sorted(installed_by_key.items(), key=lambda kv: kv[1]):
            if name in emitted:
                continue
            gone = parse_collector_key(name)
            report.lost.append((number, text_of(gone) if gone else name, 0))
    report.objects = len(ordered)
    report.collectors = ordered
    return ordered


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
    catalog_section: str | None = None,
    installed: Mapping[int, str] | None = None,
    published: int = 0,
) -> tuple[dict[str, Any], DeviceReport]:
    """Fill a copy of the template with one collector object per
    (main group, datapoint type, direction). ``published`` is the
    highest version already handed to Kaenx-Creator for this device —
    the export alone cannot know it, and ETS remembers every version
    it ever imported even after the catalog entry is deleted."""
    model = copy.deepcopy(dict(template))
    application = model["Application"]
    language = dict(application["Languages"][0])
    report = DeviceReport()

    ordered = build_collectors(spec, project_data, report, installed)
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
    unchanged = (
        bool(installed) and not report.new_objects and not report.renumbered and not report.lost
    )
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
    # by hand onto a second device instance, object by object, per the
    # worksheet. Name every earlier version anyway, so ETS files the
    # new one as an update of the same product.
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
    changed = report.new_objects or report.renumbered or report.lost
    if changed:
        lines += [
            "",
            f"Gegenüber dem Gerät im Projekt: {len(report.new_objects)} neu, "
            f"{len(report.renumbered)} umnummeriert, {len(report.lost)} entfallen. "
            "Neue Version als zweites Gerät anlegen, die Links je Objekt vom alten Gerät "
            "rüberziehen („war N“ = Objektnummer dort), altes Gerät löschen.",
        ]
    for collector in report.collectors:
        marker = ""
        if changed and collector.installed_number is None:
            marker = " — NEU"
        elif collector.installed_number not in (None, collector.number):
            marker = f" — war {collector.installed_number}"
        count = f"({len(collector.entries)} GAs)"
        lines += ["", f"## Objekt {collector.number}: {collector.text} {count}{marker}", ""]
        lines += [f"- [ ] `{ga}` {name}" for ga, name in collector.entries]
    if report.lost:
        lines += ["", "## Entfallene Objekte (auf dem alten Gerät, in keiner Quelle mehr)", ""]
        lines += [
            f"- Objekt {number}: {text}" + (f" — {links} Links" if links else "")
            for number, text, links in report.lost
        ]
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
        metavar="SOURCE=NAME",
        help=(
            "Device to generate: SOURCE, prefixed with @, names the footprint "
            "file — one '<address> <direction>' per line, the address as M/C/S "
            "or NATS subject, the direction transmit (Transmit+Read collector), "
            "write (Write collector) or both — for a device whose footprint is "
            "defined by configuration rather than by the ETS project; without "
            "@ it selects the source device(s) by case-insensitive substring "
            "against name, manufacturer and hardware name, or by individual "
            "address (1.1.240), the direction then being what the linked "
            "objects' flags say. NAME names the generated product. Repeatable; "
            "the application number is 100 plus the argument's position, so "
            "keep the order stable."
        ),
    )
    parser.add_argument(
        "--catalog-section",
        default=None,
        help="Rename the exported catalog section (shown inside the manufacturer in ETS)",
    )
    parser.add_argument(
        "--versions",
        type=Path,
        default=None,
        help=(
            "YAML file holding the application version last published per device. "
            "Read and written back: ETS remembers every version it ever imported, "
            "also ones deleted from the catalog, so the next publish must go above "
            "what was handed out — not just above what the export has installed."
        ),
    )
    parser.add_argument(
        "--accept-loss",
        action="store_true",
        help=(
            "Write the projects even though a device drops objects that still carry "
            "links in the export."
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

    template = json.loads(args.template.read_text(encoding="utf-8"))
    logger.info("parsing %s", args.input)
    project_data = _load_project(args.input, args.password)

    versions = read_versions(args.versions) if args.versions else None

    args.output_dir.mkdir(parents=True, exist_ok=True)
    failed = False
    outputs: list[tuple[Path, str]] = []
    for spec in specs:
        installed = installed_objects(args.input, project_data, spec.slug)
        published = versions.get(spec.name, 0) if versions is not None else 0
        model, report = build_device_model(
            template,
            spec,
            project_data,
            catalog_section=args.catalog_section,
            installed=installed,
            published=published,
        )
        if versions is not None and report.app_version > published:
            versions[spec.name] = report.app_version
        links = installed_links(project_data, spec.slug)
        report.lost = [(n, text, links.get(n, 0)) for n, text, _ in report.lost]
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
        elif report.new_objects or report.renumbered or report.lost:
            logger.info(
                "%s: %d new, %d renumbered, %d dropped against the installed application — "
                "publish it, add the new version as a second device in ETS, move the links "
                "over per the worksheet, delete the old device%s",
                spec.name,
                len(report.new_objects),
                len(report.renumbered),
                len(report.lost),
                (
                    "; new: "
                    + ", ".join(f"{c.number} {c.text}" for c in report.new_objects[:6])
                    + (" …" if len(report.new_objects) > 6 else "")
                )
                if report.new_objects
                else "",
            )
        else:
            logger.info(
                "%s: same objects as the installed application — no publish needed, only links",
                spec.name,
            )
        for number, text, count in report.lost:
            level = logging.ERROR if count else logging.WARNING
            logger.log(
                level,
                "%s: installed object %d (%s) with %d link(s) is not in this version — no "
                "source claims its addresses any more",
                spec.name,
                number,
                text,
                count,
            )
        if lost_links and not args.accept_loss:
            logger.error(
                "%s: %d link(s) sit on objects this version drops — nothing written; unlink "
                "them in ETS (knxproj-check-wiring lists them as extra), or pass "
                "--accept-loss to proceed anyway",
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
                "%s: footprint address %s does not exist in the ETS project — the "
                "configuration points at nothing",
                spec.name,
                ga,
            )
            failed = True
    if failed:
        return 1
    for path, content in outputs:
        path.write_text(content, encoding="utf-8")
        logger.info("wrote %s", path)
    if versions is not None and args.versions is not None:
        write_versions(args.versions, versions)
        logger.info("versions updated: %s", args.versions)
    return 0


def read_versions(path: Path) -> dict[str, int]:
    """{device name -> version byte last published}; a missing file is empty."""
    if not path.is_file():
        return {}
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict) or not all(isinstance(v, int) for v in data.values()):
        raise SystemExit(f"{path}: expected a mapping of device name to version byte")
    return {str(device): int(version) for device, version in data.items()}


def write_versions(path: Path, versions: Mapping[str, int]) -> None:
    header = (
        "# The application version (byte: 0x11 = 17 = V 1.1) last handed to Kaenx-Creator\n"
        "# per generated ETS device. knxproj-to-kaenx maintains this file. Keep it\n"
        "# committed: ETS remembers every version it ever imported, also ones deleted\n"
        "# from the catalog, so the next publish must go above the last one handed\n"
        "# out — the ETS export only knows which version is installed.\n"
    )
    ordered = {device: versions[device] for device in sorted(versions)}
    path.write_text(
        header + yaml.safe_dump(ordered, sort_keys=False, allow_unicode=True), encoding="utf-8"
    )


if __name__ == "__main__":
    sys.exit(main())
