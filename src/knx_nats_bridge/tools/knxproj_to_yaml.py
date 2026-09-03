"""Convert an ETS .knxproj export into the ga-catalog.yaml format.

Extracts per group address: name, DPT, plus the ETS metadata that
downstream consumers (iot-mcp-bridge, state-projector) need to
resolve raw GAs to room/function/description.

Requires the optional dependency `xknxproject` (install via
`pip install "knx-nats-bridge[tools]"`).

Example:
    knxproj-to-yaml --input my-project.knxproj --output ga-catalog.yaml \\
        [--password 'ets-password']
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from collections.abc import Container, Mapping, Sequence
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)


def _load_project(path: Path, password: str | None) -> dict[str, Any]:
    try:
        from xknxproject import XKNXProj
    except ImportError as exc:
        raise SystemExit(
            "xknxproject is not installed. Install the tools extra:\n"
            "  pip install 'knx-nats-bridge[tools]'\n"
            f"(import error: {exc})"
        ) from exc

    project = XKNXProj(path=str(path), password=password) if password else XKNXProj(path=str(path))
    return project.parse()


def _extract(
    mapping: dict[str, Any],
    project_data: dict[str, Any],
    ignore_write_from: Sequence[str] = (),
) -> None:
    """Build the raw catalog entries straight from xknxproject's parse output.

    Output is minimal and project-agnostic: what ETS defines, minus the two
    artefacts ETS adds by itself — the Building-tree numbering around a
    space name and the description it pre-fills from space plus function.
    Both are presentation, not data; stripping them belongs here, where ETS
    is read, rather than in every consumer.

    For each GA we emit:
      * ``name``         — verbatim from ETS
      * ``dpt``          — ``<main>.<sub>`` formatted; entries without a DPT
                            are dropped
      * ``function``     — ETS Function name, if the GA is referenced by one
      * ``room``         — Function's space name, if the Function has a
                            space_id, stripped of ETS' Building numbering
      * ``description``  — ETS description / comment, unless it is the
                            boilerplate ETS pre-fills
      * ``writable``     — True when a linked communication object carries the
                            ETS Write flag, i.e. something on the bus *receives*
                            this address. Pass ``ignore_write_from`` to drop
                            devices that receive without acting (filter-table
                            placeholders). Still only a necessary condition:
                            actuators reached over other transports have no ETS
                            object at all, so a consumer that gates writes must
                            combine this with its own knowledge of which
                            addresses are command inputs.
    """
    group_addresses = project_data.get("group_addresses", {}) or {}
    spaces = project_data.get("spaces") or project_data.get("locations") or {}
    functions = project_data.get("functions", {}) or {}
    comm_objects = project_data.get("communication_objects", {}) or {}
    devices = project_data.get("devices", {}) or {}
    ignored = _ignored_co_ids(comm_objects, devices, ignore_write_from)

    space_id_to_name = _build_space_id_to_name(spaces)
    ga_to_function = _build_ga_to_function(functions, space_id_to_name)

    for ga_str, ga_info in group_addresses.items():
        if not isinstance(ga_info, dict):
            continue
        name = ga_info.get("name") or ga_info.get("identifier") or ga_str
        dpt = _extract_dpt(ga_info.get("dpt"))
        if dpt is None:
            continue

        entry: dict[str, Any] = {
            "name": str(name),
            "dpt": dpt,
            # Always emitted, unlike the optional fields below: `false` is a
            # positive statement from ETS, not absent data. An absent key then
            # means exactly one thing — the catalog predates this field.
            "writable": _is_writable(
                ga_info.get("communication_object_ids"), comm_objects, ignored
            ),
        }

        fn = ga_to_function.get(str(ga_str))
        raw_room = fn.get("room") if fn else None
        if fn:
            entry["function"] = fn["name"]
            if raw_room:
                entry["room"] = _strip_space_numbering(raw_room)

        description = (ga_info.get("description") or ga_info.get("comment") or "").strip()
        # ETS writes the boilerplate with the *raw* space name, so both
        # spellings have to be compared against.
        rooms = [r for r in (raw_room, entry.get("room")) if r]
        if description and not any(
            _is_ets_boilerplate(description, room, entry.get("function")) for room in rooms
        ):
            entry["description"] = description

        mapping[str(ga_str)] = entry


def _is_writable(
    co_ids: Any,
    communication_objects: Mapping[str, Any],
    ignored_co_ids: Container[str] = frozenset(),
) -> bool:
    """True when any communication object linked to the GA has the Write flag.

    A GA with no linked objects — or whose objects are all send-only — is not
    writable. Dangling ids (listed on the GA but missing from the project's
    communication objects, which happens when a device's application program
    was never loaded) resolve to None and count as not writable.

    Objects in ``ignored_co_ids`` do not vote; see ``_ignored_co_ids``.
    """
    if not isinstance(co_ids, list):
        return False
    return any(
        str(co_id) not in ignored_co_ids and _has_write_flag(communication_objects.get(str(co_id)))
        for co_id in co_ids
    )


def _ignored_co_ids(
    communication_objects: Mapping[str, Any],
    devices: Mapping[str, Any],
    patterns: Sequence[str],
) -> frozenset[str]:
    """Communication objects whose Write flag should not count as an actuator.

    Some devices receive group addresses without acting on them. The common
    case is a placeholder device carrying objects purely so a group address
    enters a line coupler's filter table — its Write flag says the telegram is
    forwarded, not that anything responds to it. Matching is a case-insensitive
    substring test against the device's name, manufacturer and hardware name,
    so a pattern can target either a product ("Dummy") or one specific device
    ("Basalte Core S4") when several share a product.
    """
    if not patterns:
        return frozenset()

    lowered = [p.lower() for p in patterns]
    ignored: set[str] = set()
    for co_id, co in communication_objects.items():
        if not isinstance(co, dict):
            continue
        device = devices.get(str(co.get("device_address")))
        if not isinstance(device, dict):
            continue
        label = " ".join(
            str(device.get(k) or "") for k in ("name", "manufacturer_name", "hardware_name")
        ).lower()
        if any(p in label for p in lowered):
            ignored.add(str(co_id))
    return frozenset(ignored)


def _has_write_flag(co: Any) -> bool:
    flags = co.get("flags") if isinstance(co, dict) else None
    return bool(flags.get("write")) if isinstance(flags, dict) else False


def _writable_provenance(
    mapping: Mapping[str, Any],
    project_data: Mapping[str, Any],
    ignore_write_from: Sequence[str] = (),
) -> list[tuple[str, int]]:
    """Count, per device, how often it supplied the Write flag.

    Answers "which devices voted writable" — the one question the emitted YAML
    cannot answer on its own, and the one that tells a filter-table dummy apart
    from a real actuator. Debug output only.
    """
    group_addresses = project_data.get("group_addresses", {}) or {}
    comm_objects = project_data.get("communication_objects", {}) or {}
    devices = project_data.get("devices", {}) or {}
    ignored = _ignored_co_ids(comm_objects, devices, ignore_write_from)

    counts: dict[str, int] = {}
    for ga_str, entry in mapping.items():
        if not entry.get("writable"):
            continue
        ga_info = group_addresses.get(ga_str)
        co_ids = ga_info.get("communication_object_ids") if isinstance(ga_info, dict) else None
        for co_id in co_ids if isinstance(co_ids, list) else []:
            if str(co_id) in ignored:
                continue
            co = comm_objects.get(str(co_id))
            if not _has_write_flag(co):
                continue
            addr = co.get("device_address") if isinstance(co, dict) else None
            device = devices.get(str(addr)) if addr else None
            label = str(addr or "unknown")
            if isinstance(device, dict):
                parts = [device.get("manufacturer_name"), device.get("hardware_name")]
                detail = " ".join(str(p) for p in parts if p)
                if detail:
                    label = f"{label} ({detail})"
            counts[label] = counts.get(label, 0) + 1
    return sorted(counts.items(), key=lambda kv: kv[1], reverse=True)


def _extract_dpt(dpt: Any) -> str | None:
    if dpt is None:
        return None
    if isinstance(dpt, dict):
        main = dpt.get("main")
        sub = dpt.get("sub")
        if main is None:
            return None
        return f"{int(main)}.{int(sub):03d}" if sub is not None else f"{int(main)}.000"
    if isinstance(dpt, str):
        return dpt
    return None


def _build_space_id_to_name(spaces: Mapping[str, Any]) -> dict[str, str]:
    """Walk the Space tree and return {space_id -> space name}."""
    space_id_to_name: dict[str, str] = {}

    def _walk(space_dict: Mapping[str, Any]) -> None:
        if not isinstance(space_dict, dict):
            return
        for space_id, space in space_dict.items():
            if not isinstance(space, dict):
                continue
            name = (space.get("name") or "").strip()
            if name:
                space_id_to_name[str(space_id)] = name
                # Also index by the Space's own `identifier` field — some
                # xknxproject builds key Functions by identifier, not by the
                # outer dict key.
                ident = space.get("identifier")
                if ident:
                    space_id_to_name[str(ident)] = name
            children = space.get("spaces") or {}
            if children:
                _walk(children)

    _walk(spaces)
    return space_id_to_name


# ETS' Building tree numbers its spaces for ordering and appends the space
# id: `3 - Büro (E3)`. Higher levels carry no id: `1 - Gebäude`. Both are
# presentation; the name in between is the room.
_SPACE_NUMBERING_RE = re.compile(r"^\s*\d+\s*-\s*(.+?)\s*(?:\([^)]*\))?\s*$")


def _strip_space_numbering(space: str) -> str:
    """`3 - Büro (E3)` and `1 - Gebäude` -> `Büro` / `Gebäude`. Anything that
    does not carry the numbering is returned trimmed but untouched."""
    match = _SPACE_NUMBERING_RE.match(space)
    return match.group(1).strip() if match else space.strip()


def _is_ets_boilerplate(description: str, room: str | None, function: str | None) -> bool:
    """Whether a description is the text ETS pre-fills a Function with.

    ETS seeds it with the space name followed by the function name, which
    repeats what `room` and `function` already carry. Two spellings occur:
    the plain one, and the Building-numbered one — and the latter keeps the
    numbering the space had when the text was seeded, so a space that was
    renumbered since leaves `3 - Terrasse (A3) Entertainment` beside a room
    now called `2 - Terrasse (A2)`. The number is therefore matched
    structurally, never compared. Case-insensitive and whitespace-tolerant.
    """
    text = " ".join(description.split())
    parts = [part.strip() for part in (room, function) if part]
    if text.lower() == " ".join(parts).lower():
        return True
    if not (room and function):
        return False
    numbered = re.compile(
        r"^\d+\s*-\s*" + re.escape(room.strip()) + r"\s*(?:\([^)]*\))?\s+"
        r"" + re.escape(function.strip()) + r"$",
        re.IGNORECASE,
    )
    return numbered.match(text) is not None


def _build_ga_to_function(
    functions: Mapping[str, Any],
    space_id_to_name: Mapping[str, str],
) -> dict[str, dict[str, str | None]]:
    """Invert the Function dict so we can look up name+room per GA.

    The returned value carries both the function name and the resolved
    *logical* room from the function's `space_id` — which is the room the
    function controls, not the room where the actuator hardware lives.
    """
    ga_to_function: dict[str, dict[str, str | None]] = {}
    for fn in functions.values():
        if not isinstance(fn, dict):
            continue
        fn_name = (fn.get("name") or "").strip()
        if not fn_name:
            continue
        space_id = fn.get("space_id")
        room = space_id_to_name.get(str(space_id)) if space_id else None
        ga_dict = fn.get("group_addresses") or {}
        for ga in ga_dict:
            ga_to_function.setdefault(str(ga), {"name": fn_name, "room": room})
    return ga_to_function


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Convert .knxproj to ga-catalog.yaml")
    parser.add_argument("--input", "-i", required=True, type=Path, help="Path to .knxproj file")
    parser.add_argument(
        "--output", "-o", required=True, type=Path, help="Path to ga-catalog.yaml output"
    )
    parser.add_argument("--password", default=None, help="ETS project password (if encrypted)")
    parser.add_argument(
        "--ignore-write-from",
        action="append",
        default=[],
        metavar="SUBSTRING",
        help=(
            "Device whose Write flag must not mark a group address writable, "
            "matched case-insensitively against device name, manufacturer and "
            "hardware name. "
            "Repeatable. Use for placeholder devices that exist only to get a "
            "group address into a line coupler's filter table."
        ),
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    logger.info("parsing %s", args.input)
    project_data = _load_project(args.input, args.password)

    mapping: dict[str, Any] = {}
    _extract(mapping, project_data, args.ignore_write_from)
    if not mapping:
        logger.error("no group addresses with DPT information found in %s", args.input)
        return 2

    with_room = sum(1 for e in mapping.values() if "room" in e)
    with_function = sum(1 for e in mapping.values() if "function" in e)
    with_description = sum(1 for e in mapping.values() if "description" in e)
    writable = sum(1 for e in mapping.values() if e.get("writable"))

    args.output.write_text(
        yaml.safe_dump(mapping, sort_keys=True, allow_unicode=True, default_flow_style=False),
        encoding="utf-8",
    )
    logger.info(
        "wrote %d entries to %s (room=%d, function=%d, description=%d, writable=%d)",
        len(mapping),
        args.output,
        with_room,
        with_function,
        with_description,
        writable,
    )

    if logger.isEnabledFor(logging.DEBUG):
        for label, count in _writable_provenance(mapping, project_data, args.ignore_write_from):
            logger.debug("write flag supplied by %s on %d group addresses", label, count)
    return 0


if __name__ == "__main__":
    sys.exit(main())
