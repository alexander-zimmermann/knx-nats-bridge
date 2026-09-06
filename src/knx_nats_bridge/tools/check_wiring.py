"""Hold the ETS export against the software-device footprints.

The generated ETS devices (bridge, Basalte, Node-Red) are views of
configuration that lives elsewhere — writer rules, consumer manifests,
the Basalte Studio export, the Node-Red flows. This check makes the
comparison automatic instead of visual: per device, every footprint
address must be linked to the device in ETS, nothing else may be, and
where direction is known (``--write-gas``), the linked object's flags
must match it.

Findings, per device:

- **missing** — the footprint names the address, ETS does not deliver
  it to the device. A new address whose ETS link was forgotten, or
  configuration pointing at nothing.
- **extra** — linked to the device in ETS, but no configuration claims
  it. Left over from an earlier footprint, or wired by mistake.
- **misflagged** — linked, but to an object whose flags contradict the
  direction: a consumed address (``--write-gas``) needs a Write-flagged
  object, everything else a Transmit-flagged one. Devices whose
  collectors carry both flags always pass.

Any finding fails the run. A footprint device that does not exist in
the project yet (matched by order number, which the generator sets to
the name slug) is reported as such — wiring cannot be checked before
the device is imported.

Example:
    knxproj-check-wiring --input project.knxproj \\
        --device 'KNX-NATS-Bridge=@bridge-gas.txt' --write-gas consumed.txt \\
        --device 'Basalte Core S4=@basalte-gas.txt'
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from knx_nats_bridge.tools.knxproj_to_kaenx import _ga_sort_key, read_ga_list
from knx_nats_bridge.tools.knxproj_to_yaml import _load_project

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CheckSpec:
    """One ``--device NAME=@FILE`` argument, parsed."""

    name: str
    footprint_path: Path

    @property
    def slug(self) -> str:
        return re.sub(r"[^A-Z0-9]+", "-", self.name.upper()).strip("-")


@dataclass
class CheckReport:
    """Wiring findings for one device."""

    linked: int = 0
    missing: list[str] = field(default_factory=list)
    extra: list[str] = field(default_factory=list)
    misflagged: list[str] = field(default_factory=list)
    device_found: bool = True

    @property
    def clean(self) -> bool:
        return self.device_found and not (self.missing or self.extra or self.misflagged)


def parse_check_spec(raw: str) -> CheckSpec:
    name, sep, source = raw.partition("=")
    if not sep or not name.strip() or not source.startswith("@") or not source[1:].strip():
        raise SystemExit(f"invalid --device {raw!r}: expected NAME=@FOOTPRINT-FILE")
    return CheckSpec(name=name.strip(), footprint_path=Path(source[1:]))


def _device_links(
    project_data: Mapping[str, Any], order_number: str
) -> tuple[dict[str, list[Any]], bool]:
    """{ga -> linked communication objects} for the device, plus whether
    any device with that order number exists at all."""
    devices = project_data.get("devices", {}) or {}
    addresses = {
        str(addr)
        for addr, device in devices.items()
        if isinstance(device, dict) and device.get("order_number") == order_number
    }
    comm_objects = project_data.get("communication_objects", {}) or {}
    matching_co_ids = {
        str(co_id)
        for co_id, co in comm_objects.items()
        if isinstance(co, dict) and str(co.get("device_address")) in addresses
    }

    links: dict[str, list[Any]] = {}
    for ga, info in (project_data.get("group_addresses", {}) or {}).items():
        if not isinstance(info, dict):
            continue
        linked = [
            comm_objects[str(co_id)]
            for co_id in info.get("communication_object_ids") or []
            if str(co_id) in matching_co_ids
        ]
        if linked:
            links[str(ga)] = linked
    return links, bool(addresses)


def _has_flag(linked: list[Any], flag: str) -> bool:
    for co in linked:
        flags = co.get("flags") if isinstance(co, dict) else None
        if isinstance(flags, dict) and flags.get(flag):
            return True
    return False


def check_device(
    project_data: Mapping[str, Any],
    spec: CheckSpec,
    footprint: frozenset[str],
    write_gas: frozenset[str],
) -> CheckReport:
    report = CheckReport()
    links, device_found = _device_links(project_data, spec.slug)
    if not device_found:
        report.device_found = False
        return report

    report.linked = len(links)
    report.missing = sorted(footprint - set(links), key=_ga_sort_key)
    report.extra = sorted(set(links) - footprint, key=_ga_sort_key)
    for ga in sorted(footprint & set(links), key=_ga_sort_key):
        wanted = "write" if ga in write_gas else "transmit"
        if not _has_flag(links[ga], wanted):
            report.misflagged.append(ga)
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Hold an ETS export against the software-device footprints"
    )
    parser.add_argument("--input", "-i", required=True, type=Path, help="Path to .knxproj file")
    parser.add_argument("--password", default=None, help="ETS project password (if encrypted)")
    parser.add_argument(
        "--device",
        action="append",
        required=True,
        metavar="NAME=@FILE",
        help=(
            "Device to check: NAME is the generated product name (matched in "
            "ETS via its order number, the name slug), @FILE its footprint — "
            "one group address or NATS subject per line. Repeatable."
        ),
    )
    parser.add_argument(
        "--write-gas",
        type=Path,
        default=None,
        help=(
            "File with the consumed addresses (one per line): these must be "
            "linked to Write-flagged objects, all other footprint addresses "
            "to Transmit-flagged ones."
        ),
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    specs = [parse_check_spec(raw) for raw in args.device]
    write_gas = frozenset[str]()
    if args.write_gas is not None:
        write_gas = read_ga_list(args.write_gas.read_text(encoding="utf-8"), origin="--write-gas")

    logger.info("parsing %s", args.input)
    project_data = _load_project(args.input, args.password)

    clean = True
    for spec in specs:
        footprint = read_ga_list(
            spec.footprint_path.read_text(encoding="utf-8"), origin=str(spec.footprint_path)
        )
        report = check_device(project_data, spec, footprint, write_gas)
        if not report.device_found:
            logger.error(
                "%s: no device with order number %s in the project — import it "
                "before checking its wiring",
                spec.name,
                spec.slug,
            )
            clean = False
            continue
        for ga in report.missing:
            logger.error("%s: %s is in the footprint but not linked in ETS", spec.name, ga)
        for ga in report.extra:
            logger.error("%s: %s is linked in ETS but no configuration claims it", spec.name, ga)
        for ga in report.misflagged:
            wanted = "Write" if ga in write_gas else "Transmit"
            logger.error("%s: %s is linked to an object without the %s flag", spec.name, ga, wanted)
        if report.clean:
            logger.info(
                "%s: OK — %d of %d footprint addresses linked, flags match",
                spec.name,
                report.linked,
                len(footprint),
            )
        clean = clean and report.clean
    return 0 if clean else 1


if __name__ == "__main__":
    sys.exit(main())
