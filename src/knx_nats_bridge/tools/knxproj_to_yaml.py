"""Convert an ETS .knxproj export into the knx-catalog.yaml format.

Extracts per group address: name, DPT, plus the ETS metadata that
downstream consumers (iot-mcp-bridge, state-projector) need to
resolve raw GAs to room/function/description.

Requires the optional dependency `xknxproject` (install via
`pip install "knx-nats-bridge[tools]"`).

Example:
    knxproj-to-yaml --input my-project.knxproj --output knx-catalog.yaml \\
        [--password 'ets-password']
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Mapping
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


def _extract(mapping: dict[str, Any], project_data: dict[str, Any]) -> None:
    """Build the raw catalog entries straight from xknxproject's parse output.

    Output is intentionally minimal and project-agnostic: only what ETS
    itself defines. Project-specific enrichment (parsing rooms from naming
    conventions, normalising space names, etc.) is the consumer's job.

    For each GA we emit:
      * ``name``         — verbatim from ETS
      * ``dpt``          — ``<main>.<sub>`` formatted; entries without a DPT
                            are dropped
      * ``function``     — ETS Function name, if the GA is referenced by one
      * ``room``         — Function's space name, if the Function has a space_id
      * ``description``  — ETS description / comment, if non-empty
    """
    group_addresses = project_data.get("group_addresses", {}) or {}
    spaces = project_data.get("spaces") or project_data.get("locations") or {}
    functions = project_data.get("functions", {}) or {}

    space_id_to_name = _build_space_id_to_name(spaces)
    ga_to_function = _build_ga_to_function(functions, space_id_to_name)

    for ga_str, ga_info in group_addresses.items():
        if not isinstance(ga_info, dict):
            continue
        name = ga_info.get("name") or ga_info.get("identifier") or ga_str
        dpt = _extract_dpt(ga_info.get("dpt"))
        if dpt is None:
            continue

        entry: dict[str, Any] = {"name": str(name), "dpt": dpt}

        fn = ga_to_function.get(str(ga_str))
        if fn:
            entry["function"] = fn["name"]
            if fn.get("room"):
                entry["room"] = fn["room"]

        description = (ga_info.get("description") or ga_info.get("comment") or "").strip()
        if description:
            entry["description"] = description

        mapping[str(ga_str)] = entry


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
    parser = argparse.ArgumentParser(description="Convert .knxproj to knx-catalog.yaml")
    parser.add_argument("--input", "-i", required=True, type=Path, help="Path to .knxproj file")
    parser.add_argument(
        "--output", "-o", required=True, type=Path, help="Path to knx-catalog.yaml output"
    )
    parser.add_argument("--password", default=None, help="ETS project password (if encrypted)")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    logger.info("parsing %s", args.input)
    project_data = _load_project(args.input, args.password)

    mapping: dict[str, Any] = {}
    _extract(mapping, project_data)
    if not mapping:
        logger.error("no group addresses with DPT information found in %s", args.input)
        return 2

    with_room = sum(1 for e in mapping.values() if "room" in e)
    with_function = sum(1 for e in mapping.values() if "function" in e)
    with_description = sum(1 for e in mapping.values() if "description" in e)

    args.output.write_text(
        yaml.safe_dump(mapping, sort_keys=True, allow_unicode=True, default_flow_style=False),
        encoding="utf-8",
    )
    logger.info(
        "wrote %d entries to %s (room=%d, function=%d, description=%d)",
        len(mapping),
        args.output,
        with_room,
        with_function,
        with_description,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
