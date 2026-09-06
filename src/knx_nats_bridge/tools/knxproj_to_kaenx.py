"""Generate Kaenx-Creator projects for the ETS paper devices.

The ETS project models bus participants that live in software — the
KNX-NATS bridge, the Basalte visualisation, Node-Red — as placeholder
devices whose only job is filter-table membership. This tool builds a
real product database for each of them instead: one Kaenx-Creator
project (.ae-manu) per device, with one communication object per group
address the device touches, named after the NATS subject and flagged by
direction. Kaenx-Creator (Windows) then exports the .knxprod that ETS
imports.

Where a device's addresses come from is per device: a pattern or
individual address collects what the ETS export links to the matching
device(s) — right for placeholders that grew by hand — while a ``@file``
source lists the addresses directly, so a device whose true footprint
is defined by configuration (the bridge: writer-rule targets plus
consumer-handled addresses) is generated from that configuration and
ETS only supplies each address's name and datapoint type.

Flag modes per device:

- ``split``: Write on addresses listed in ``--write-gas`` (a NATS
  consumer acts on writes to them), Transmit+Read on the rest (the
  bridge sends these and answers reads from its responder cache).
  Keeps the catalog's ``writable`` vote exact.
- ``both``: Write+Transmit on every object. For devices that both
  display and send (visualisation) and stay excluded from the write
  vote anyway.

A template .ae-manu saved by the target Kaenx-Creator installation
supplies everything version-specific (mask, load procedures, language);
only naming, identity and the object tables are rewritten. Kaenx-Creator
re-links datapoint types by number on load, so the emitted objects only
carry ``TypeNumber``/``SubTypeNumber`` and a correct ``ObjectSize``.

Example:
    knxproj-to-kaenx --input project.knxproj --template empty.ae-manu \\
        --device '@bridge-gas.txt=KNX-NATS-Bridge:split' --write-gas consumed.txt \\
        --device 'basalte=Basalte Core S4:both' --output-dir out/
"""

from __future__ import annotations

import argparse
import copy
import json
import logging
import re
import sys
import uuid
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from knx_nats_bridge.tools.knxproj_to_yaml import _load_project

logger = logging.getLogger(__name__)

_SHARE = "Kaenx.Creator.Share"

# Datapoint master data as Kaenx-Creator ships it (Data/datapoints.xml):
# main number -> (size in bits, known subtype numbers). Objects must not
# reference a type the target application cannot re-link on load, so
# anything outside this table is skipped and reported.
_DPT_MASTER: dict[int, tuple[int, frozenset[int]]] = {
    1: (
        1,
        frozenset(
            {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 21, 22, 23, 24, 100}
        ),
    ),
    2: (2, frozenset({1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})),
    3: (4, frozenset({7, 8})),
    4: (8, frozenset({1, 2})),
    5: (8, frozenset({1, 3, 4, 5, 6, 10, 100})),
    6: (8, frozenset({1, 10, 20})),
    7: (16, frozenset({1, 2, 3, 4, 5, 6, 7, 10, 11, 12, 13, 600})),
    8: (16, frozenset({1, 2, 3, 4, 5, 6, 7, 10, 11, 12})),
    9: (
        16,
        frozenset({1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30}),
    ),
    10: (24, frozenset({1})),
    11: (24, frozenset({1})),
    12: (32, frozenset({1, 100, 101, 102, 1200, 1201})),
    13: (32, frozenset({1, 2, 10, 11, 12, 13, 14, 15, 16, 100, 1200, 1201})),
    14: (32, frozenset(range(0, 80)) | frozenset({1200, 1201})),
    15: (32, frozenset({0})),
    16: (112, frozenset({0, 1})),
    17: (8, frozenset({1})),
    18: (8, frozenset({1})),
    19: (64, frozenset({1})),
    20: (
        8,
        frozenset(
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
    ),
    21: (8, frozenset({1, 2, 100, 101, 102, 103, 104, 105, 106, 107, 601, 1000, 1001, 1010})),
    22: (16, frozenset({100, 101, 102, 103, 1000, 1010})),
    23: (2, frozenset({1, 2, 3, 102})),
    25: (8, frozenset({1000})),
    26: (8, frozenset({1})),
    27: (32, frozenset({1})),
    29: (64, frozenset({10, 11, 12})),
    30: (24, frozenset({1010})),
    206: (24, frozenset({100, 102, 104, 105})),
    217: (16, frozenset({1})),
    219: (48, frozenset({1})),
    222: (48, frozenset({100, 101})),
    225: (24, frozenset({1, 2})),
    229: (48, frozenset({1})),
    230: (64, frozenset({1000})),
    232: (24, frozenset({600})),
    234: (16, frozenset({1})),
    235: (48, frozenset({1})),
    236: (8, frozenset({1})),
    237: (16, frozenset({600})),
    238: (8, frozenset({600})),
    240: (24, frozenset({800})),
    241: (32, frozenset({800})),
    242: (48, frozenset({600})),
    244: (16, frozenset({600})),
    245: (48, frozenset({600})),
    246: (16, frozenset({600})),
    249: (48, frozenset({600})),
    250: (24, frozenset({600})),
    251: (48, frozenset({600})),
    252: (40, frozenset({600})),
    254: (24, frozenset({600})),
    255: (64, frozenset({1})),
    275: (64, frozenset({100, 101})),
}

_GA_RE = re.compile(r"^(\d{1,2})/(\d)/(\d{1,3})$")


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
class DeviceReport:
    """What a device build produced and what it had to leave out."""

    objects: int = 0
    write: int = 0
    transmit: int = 0
    skipped: list[tuple[str, str]] = field(default_factory=list)  # (ga, reason)
    unmatched_write_gas: list[str] = field(default_factory=list)
    missing: list[str] = field(default_factory=list)  # listed in a @file, absent from ETS


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


def _translation(language: Mapping[str, Any], text: str) -> dict[str, Any]:
    return {
        "$type": f"Kaenx.Creator.Models.Translation, {_SHARE}",
        "Language": dict(language),
        "Text": text,
        "Preview": text,
    }


def _com_object(
    number: int,
    name: str,
    text: str,
    function_text: str,
    dpt_main: int,
    dpt_sub: int | None,
    flags: Mapping[str, bool],
    language: Mapping[str, Any],
) -> dict[str, Any]:
    size, known_subs = _DPT_MASTER[dpt_main]
    has_sub = dpt_sub is not None and dpt_sub in known_subs
    return {
        "$type": f"Kaenx.Creator.Models.ComObject, {_SHARE}",
        "UId": number,
        "Id": number,
        "Name": name,
        "Text": [_translation(language, text)],
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
        "HasDpts": has_sub,
        "ObjectSize": size,
        "SubTypeNumber": str(dpt_sub) if has_sub else None,
        "SubType": None,
        "TypeNumber": str(dpt_main),
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


def _dynamics(
    refs: Iterable[Mapping[str, Any]], language: Mapping[str, Any]
) -> list[dict[str, Any]]:
    """Root -> independent channel -> one block holding every object.

    The nesting is the one Kaenx-Creator builds itself; objects placed
    anywhere else fail its publish checks.
    """
    dyn_objects = [
        {
            "$type": f"Kaenx.Creator.Models.Dynamic.DynComObject, {_SHARE}",
            "IsExpanded": False,
            "Name": "",
            "ComObjectRef": ref["UId"],
            "Items": None,
        }
        for ref in refs
    ]
    block = {
        "$type": f"Kaenx.Creator.Models.Dynamic.DynParaBlock, {_SHARE}",
        "IsExpanded": False,
        "Id": 1,
        "Name": "Objects",
        "Text": [_translation(language, "Objekte")],
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
        "Items": dyn_objects,
    }
    channel = {
        "$type": f"Kaenx.Creator.Models.Dynamic.DynChannelIndependent, {_SHARE}",
        "IsExpanded": True,
        "Name": "",
        "Items": [block],
    }
    return [
        {
            "$type": f"Kaenx.Creator.Models.Dynamic.DynamicMain, {_SHARE}",
            "IsExpanded": True,
            "Name": "Root Knoten",
            "Items": [channel],
        }
    ]


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


def build_device_model(
    template: Mapping[str, Any],
    spec: DeviceSpec,
    project_data: Mapping[str, Any],
    write_gas: frozenset[str],
    subject_prefix: str,
) -> tuple[dict[str, Any], DeviceReport]:
    """Fill a copy of the template with one object per group address."""
    model = copy.deepcopy(dict(template))
    application = model["Application"]
    language = dict(application["Languages"][0])
    report = DeviceReport()

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

    com_objects: list[dict[str, Any]] = []
    for ga in sorted(gas, key=_ga_sort_key):
        info = gas[ga]
        dpt = info.get("dpt")
        main = dpt.get("main") if isinstance(dpt, dict) else None
        if main is None:
            report.skipped.append((ga, "no DPT assigned in ETS"))
            continue
        if int(main) not in _DPT_MASTER:
            report.skipped.append((ga, f"DPT {main} unknown to Kaenx-Creator"))
            continue
        sub = dpt.get("sub") if isinstance(dpt, dict) else None

        consumed = spec.mode == "split" and ga in write_gas
        if spec.mode == "both":
            flags = {"write": True, "transmit": True}
        elif consumed:
            flags = {"write": True}
        else:
            flags = {"transmit": True, "read": True}
        report.write += flags.get("write", False)
        report.transmit += flags.get("transmit", False)

        subject = f"{subject_prefix}.{ga.replace('/', '.')}"
        com_objects.append(
            _com_object(
                number=len(com_objects) + 1,
                name=subject,
                text=str(info.get("name") or subject),
                function_text=ga,
                dpt_main=int(main),
                dpt_sub=int(sub) if sub is not None else None,
                flags=flags,
                language=language,
            )
        )
    report.objects = len(com_objects)
    if spec.mode == "split":
        report.unmatched_write_gas = sorted(write_gas - set(gas), key=_ga_sort_key)

    refs = [_com_object_ref(co, language) for co in com_objects]
    application["ComObjects"] = com_objects
    application["ComObjectRefs"] = refs
    application["Dynamics"] = _dynamics(refs, language)
    application["HighestComNumber"] = len(com_objects)
    # Application.Number is the version byte (0x10 = V 1.0), not the
    # application's identity — that is Info.AppNumber. The template's
    # version is kept; bumping it is a publish-time decision.
    application["Name"] = spec.slug.lower()
    application["NameText"] = f"V 1.0 {spec.name}"
    application["Text"] = [_translation(language, spec.name)]

    model["ProjectName"] = spec.name
    model["FileName"] = spec.slug.lower()
    # Stable per device so a regeneration is a new version of the same
    # project, not a new project.
    model["Guid"] = str(uuid.uuid5(uuid.NAMESPACE_URL, f"lares-kaenx://{spec.slug}"))
    _prune_unnumbered_catalog_sections(model.get("Catalog") or [])

    info_block = model["Info"]
    info_block["Name"] = spec.name
    info_block["SerialNumber"] = spec.slug
    info_block["OrderNumber"] = spec.slug
    info_block["AppNumber"] = spec.app_number
    info_block["Text"] = [_translation(language, spec.name)]
    info_block["Description"] = [_translation(language, spec.name)]

    return model, report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate Kaenx-Creator projects from an ETS export"
    )
    parser.add_argument("--input", "-i", required=True, type=Path, help="Path to .knxproj file")
    parser.add_argument("--password", default=None, help="ETS project password (if encrypted)")
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
            "generated product; MODE is 'split' (Write on --write-gas addresses, "
            "Transmit+Read otherwise) or 'both' (Write+Transmit on everything). "
            "Repeatable; the application number is 100 plus the argument's "
            "position, so keep the order stable."
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
        "--subject-prefix",
        default="knx",
        help="NATS subject prefix used for object names (default: knx)",
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

    args.output_dir.mkdir(parents=True, exist_ok=True)
    failed = False
    for spec in specs:
        model, report = build_device_model(
            template, spec, project_data, write_gas, args.subject_prefix
        )
        out = args.output_dir / f"{spec.slug.lower()}.ae-manu"
        out.write_text(json.dumps(model, indent=2, ensure_ascii=False), encoding="utf-8")
        logger.info(
            "%s: %d objects (%d write, %d transmit) -> %s",
            spec.name,
            report.objects,
            report.write,
            report.transmit,
            out,
        )
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
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
