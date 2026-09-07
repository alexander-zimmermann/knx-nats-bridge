"""Hold the ETS export against the software-device footprints.

The generated ETS devices (bridge, Basalte, Node-Red) are views of
configuration that lives elsewhere — writer rules, consumer manifests,
the Basalte Studio export, the Node-Red flows. This check makes the
comparison automatic instead of visual: per device, every footprint
address must be linked to the device in ETS, nothing else may be, and
the linked object's flags must match the direction.

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

Devices are given exactly as to ``knxproj-to-kaenx``; the footprints
are grouped with the generator's own collector logic, so ``--todo-dir``
can write a per-device rest worksheet — the same object numbers and
texts as on the generated device, listing only the links still to make.

Example:
    knxproj-check-wiring --input project.knxproj --write-gas consumed.txt \\
        --device '@bridge-gas.txt=KNX-NATS-Bridge:split' \\
        --device '@basalte-gas.txt=Basalte Core S4:both' --todo-dir ~/Downloads
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from knx_nats_bridge.tools.knxproj_to_kaenx import (
    Collector,
    DeviceReport,
    DeviceSpec,
    _ga_sort_key,
    build_collectors,
    parse_device_spec,
    read_ga_list,
)
from knx_nats_bridge.tools.knxproj_to_yaml import _load_project

logger = logging.getLogger(__name__)

# How many addresses a finding category prints before the log refers
# to the todo worksheet instead.
_LOG_CAP = 20


@dataclass
class CheckReport:
    """Wiring findings for one device."""

    device_found: bool = True
    objects: int = 0
    links_expected: int = 0
    linked: int = 0
    # (object number, collector, addresses still to link on it)
    todo: list[tuple[int, Collector, list[tuple[str, str]]]] = field(default_factory=list)
    missing_from_ets: list[str] = field(default_factory=list)  # in a footprint, not in ETS
    skipped: list[tuple[str, str]] = field(default_factory=list)  # (ga, reason), unmodellable
    extra: list[tuple[str, str]] = field(default_factory=list)  # (ga, ETS name)
    misflagged: list[tuple[str, str]] = field(default_factory=list)  # (ga, wanted flag)

    @property
    def open_links(self) -> int:
        return sum(len(entries) for _, _, entries in self.todo)

    @property
    def clean(self) -> bool:
        return self.device_found and not (
            self.todo or self.missing_from_ets or self.extra or self.misflagged or self.skipped
        )


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
    spec: DeviceSpec,
    write_gas: frozenset[str],
) -> CheckReport:
    report = CheckReport()
    links, device_found = _device_links(project_data, spec.slug)
    if not device_found:
        report.device_found = False
        return report

    build = DeviceReport()
    collectors = build_collectors(spec, project_data, write_gas, build)
    report.objects = build.objects
    report.links_expected = build.links
    report.missing_from_ets = build.missing
    report.skipped = build.skipped
    report.linked = len(links)

    footprint: set[str] = set()
    for number, collector in enumerate(collectors, start=1):
        open_entries: list[tuple[str, str]] = []
        for ga, name in collector.entries:
            footprint.add(ga)
            if ga not in links:
                open_entries.append((ga, name))
                continue
            wanted = "write" if ga in write_gas else "transmit"
            if not _has_flag(links[ga], wanted):
                report.misflagged.append((ga, wanted))
        if open_entries:
            report.todo.append((number, collector, open_entries))

    ga_names = project_data.get("group_addresses", {}) or {}
    report.extra = sorted(
        (
            (ga, str((ga_names.get(ga) or {}).get("name") or ""))
            for ga in set(links) - footprint - set(build.missing)
        ),
        key=lambda pair: _ga_sort_key(pair[0]),
    )
    return report


def todo_worksheet(spec: DeviceSpec, report: CheckReport) -> str:
    """Markdown rest worksheet: only the links still to make, plus what
    is wrong — same object numbers and texts as the generated device."""
    done = report.links_expected - report.open_links - len(report.missing_from_ets)
    lines = [
        f"# Rest-Verdrahtung: {spec.name}",
        "",
        f"{done} von {report.links_expected} Verknüpfungen stehen. Offen: "
        f"{report.open_links} auf {len(report.todo)} Objekten.",
    ]
    for number, collector, entries in report.todo:
        lines += [
            "",
            f"## Objekt {number}: {collector.text} ({len(entries)} von "
            f"{len(collector.entries)} offen)",
            "",
        ]
        lines += [f"- [ ] `{ga}` {name}" for ga, name in entries]
    if report.misflagged:
        lines += ["", "## Falsch verknüpft (Objekt ohne passendes Flag)", ""]
        flag_label = {"write": "Schreiben (empfängt)", "transmit": "Übertragen (sendet)"}
        lines += [f"- `{ga}` — braucht {flag_label[flag]}" for ga, flag in report.misflagged]
    if report.extra:
        lines += ["", "## In ETS verknüpft, aber in keiner Quelle", ""]
        lines += [f"- `{ga}` {name}" for ga, name in report.extra]
    if report.missing_from_ets:
        lines += ["", "## In den Quellen, aber als GA nicht in ETS vorhanden", ""]
        lines += [f"- `{ga}`" for ga in report.missing_from_ets]
    if report.skipped:
        lines += ["", "## Nicht modellierbar (DPT fehlt oder unbekannt)", ""]
        lines += [f"- `{ga}` — {reason}" for ga, reason in report.skipped]
    lines.append("")
    return "\n".join(lines)


def _log_capped(device: str, label: str, items: list[str], todo_hint: str | None) -> None:
    for item in items[:_LOG_CAP]:
        logger.error("%s: %s %s", device, item, label)
    if len(items) > _LOG_CAP:
        where = f" — full list in {todo_hint}" if todo_hint else ""
        logger.error("%s: … and %d more %s%s", device, len(items) - _LOG_CAP, label, where)


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
        metavar="@FILE=NAME:MODE",
        help=(
            "Device to check, exactly as given to knxproj-to-kaenx: @FILE is "
            "the footprint (one group address or NATS subject per line), NAME "
            "the generated product name (matched in ETS via its order number), "
            "MODE 'split' or 'both'. Repeatable."
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
    parser.add_argument(
        "--todo-dir",
        type=Path,
        default=None,
        help=(
            "Write a per-device rest worksheet (<slug>-wiring-todo.md) into "
            "this directory: only the links still to make, grouped by object "
            "with the generated numbers. Clean devices get no file."
        ),
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    specs = [parse_device_spec(raw, 100 + i) for i, raw in enumerate(args.device)]
    for spec in specs:
        if not spec.source.startswith("@"):
            raise SystemExit(f"--device {spec.name}: the check needs a @file footprint source")
    write_gas = frozenset[str]()
    if args.write_gas is not None:
        write_gas = read_ga_list(args.write_gas.read_text(encoding="utf-8"), origin="--write-gas")

    logger.info("parsing %s", args.input)
    project_data = _load_project(args.input, args.password)
    if args.todo_dir is not None:
        args.todo_dir.mkdir(parents=True, exist_ok=True)

    clean = True
    for spec in specs:
        report = check_device(project_data, spec, write_gas)
        if not report.device_found:
            logger.error(
                "%s: no device with order number %s in the project — import it "
                "before checking its wiring",
                spec.name,
                spec.slug,
            )
            clean = False
            continue
        if report.clean:
            logger.info(
                "%s: OK — %d addresses on %d objects, flags match",
                spec.name,
                report.links_expected,
                report.objects,
            )
            continue

        clean = False
        todo_hint = None
        if args.todo_dir is not None:
            todo_path = args.todo_dir / f"{spec.slug.lower()}-wiring-todo.md"
            todo_path.write_text(todo_worksheet(spec, report), encoding="utf-8")
            todo_hint = str(todo_path)
        logger.error(
            "%s: %d of %d links open on %d objects, %d misflagged, %d unclaimed, %d not in ETS%s",
            spec.name,
            report.open_links,
            report.links_expected,
            len(report.todo),
            len(report.misflagged),
            len(report.extra),
            len(report.missing_from_ets) + len(report.skipped),
            f" -> {todo_hint}" if todo_hint else "",
        )
        if todo_hint is None:
            open_flat = [ga for _, _, entries in report.todo for ga, _ in entries]
            _log_capped(spec.name, "not linked in ETS", open_flat, None)
            _log_capped(
                spec.name,
                "misflagged",
                [f"{ga} (needs {flag})" for ga, flag in report.misflagged],
                None,
            )
            _log_capped(spec.name, "linked but unclaimed", [ga for ga, _ in report.extra], None)
            _log_capped(spec.name, "not in the ETS project", report.missing_from_ets, None)
    return 0 if clean else 1


if __name__ == "__main__":
    sys.exit(main())
