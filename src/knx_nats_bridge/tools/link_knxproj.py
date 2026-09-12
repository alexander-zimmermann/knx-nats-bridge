"""Write the software devices' group links straight into an ETS export.

The generated ETS devices (bridge, Basalte, Node-Red) carry collector
objects, and the generator knows exactly which group address belongs
on which object. Linking them by hand in ETS is hours of multi-select
work that is lost whenever the collector cut changes — so this tool
does it in the project file: it takes an ETS export, replaces each
generated device's links with the ones the configuration dictates, and
writes a project ETS imports. Hand wiring goes away entirely.

ETS stores links on the device instance, one line per object::

    <ComObjectInstanceRef RefId="O-2_R-2" Links="GA-4724 GA-4704 …" />

Before touching a device the tool proves that the installed application
is the one the configuration describes: every collector's generated
name must match the installed object of the same number, else the
device is refused with the advice to regenerate and update it first.

The output is written as an unprotected, unsigned project — Python
cannot write the ZipCrypto archive ETS5 uses for password protection,
and the export's signature would not match edited content anyway. ETS
imports it as a project without password; set one again afterwards.

Example:
    knxproj-link --input export.knxproj --password … \\
        --write-gas consumed.txt \\
        --device '@bridge-gas.txt=KNX-NATS-Bridge:split' \\
        --device '@basalte-gas.txt=Basalte Core S4:both' \\
        --output linked.knxproj
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import zipfile
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from xknxproject.zip import extract

from knx_nats_bridge.tools.knxproj_to_kaenx import (
    Collector,
    DeviceReport,
    DeviceSpec,
    build_collectors,
    parse_device_spec,
    read_ga_list,
)
from knx_nats_bridge.tools.knxproj_to_yaml import _load_project

logger = logging.getLogger(__name__)

_MANUFACTURER = "M-00FA"  # KNX Association, the id Kaenx-Creator's OpenKNX template uses


def kaenx_encode(text: str) -> str:
    """Kaenx-Creator's id encoding: anything but a letter or digit becomes
    ``.XX`` with the byte in upper-case hex (``KNX-NATS`` -> ``KNX.2DNATS``)."""
    return "".join(c if c.isalnum() else f".{ord(c):02X}" for c in text)


@dataclass
class LinkReport:
    """What linking one device produced."""

    device_found: bool = True
    objects: int = 0
    links: int = 0
    replaced: int = 0  # links the device carried before


def group_address_ids(project_xml: str) -> dict[str, str]:
    """{``M/C/S`` -> ``GA-n``} from the project's group address elements."""
    ids: dict[str, str] = {}
    for match in re.finditer(
        r'<GroupAddress\b[^>]*\bId="[^"]*_(GA-\d+)"[^>]*\bAddress="(\d+)"', project_xml
    ):
        raw = int(match.group(2))
        ids[f"{raw >> 11}/{(raw >> 8) & 0x7}/{raw & 0xFF}"] = match.group(1)
    return ids


def installed_objects(application_xml: str) -> dict[int, str]:
    """{object number -> generated name} of an installed application program."""
    objects: dict[int, str] = {}
    for match in re.finditer(r"<ComObject\b[^>]*/?>", application_xml):
        tag = match.group(0)
        name = re.search(r'\bName="([^"]*)"', tag)
        number = re.search(r'\bNumber="(\d+)"', tag)
        if name and number:
            objects[int(number.group(1))] = name.group(1)
    return objects


def _collector_name(collector: Collector) -> str:
    return f"hg{collector.main_group}-dpt{collector.dpt_label}-{collector.direction}"


def verify_installed(
    collectors: list[Collector], installed: Mapping[int, str], device: str
) -> None:
    """The installed application must be the one the configuration describes."""
    expected = {n: _collector_name(c) for n, c in enumerate(collectors, start=1)}
    if expected == dict(installed):
        return
    mismatches = [
        f"  object {n}: installed {installed.get(n, '—')!r}, configuration {expected.get(n, '—')!r}"
        for n in sorted(set(expected) | set(installed))
        if expected.get(n) != installed.get(n)
    ]
    raise SystemExit(
        f"{device}: the installed application ({len(installed)} objects) is not the one the "
        f"configuration describes ({len(expected)} objects) — regenerate, publish and update "
        "the device in ETS first:\n"
        + "\n".join(mismatches[:15])
        + ("\n  …" if len(mismatches) > 15 else "")
    )


def link_device(
    project_xml: str,
    spec: DeviceSpec,
    collectors: list[Collector],
    ga_ids: Mapping[str, str],
) -> tuple[str, LinkReport]:
    """Replace one device's links in the project XML."""
    report = LinkReport()
    product_ref = f'_P-{kaenx_encode(spec.slug)}"'
    start = re.search(
        rf'<DeviceInstance\b[^>]*\bProductRefId="{re.escape(_MANUFACTURER)}_H-[^"]*{re.escape(product_ref)}[^>]*>',
        project_xml,
    )
    if not start:
        report.device_found = False
        return project_xml, report
    end = project_xml.index("</DeviceInstance>", start.end())
    body = project_xml[start.end() : end]

    newline = "\r\n" if "\r\n" in project_xml[: start.start()] else "\n"
    head = project_xml[: start.start()]
    indent = head[len(head.rstrip(" \t")) :] + "  "
    lines = [f"{indent}<ComObjectInstanceRefs>"]
    for number, collector in enumerate(collectors, start=1):
        links = [ga_ids[ga] for ga, _ in collector.entries if ga in ga_ids]
        if not links:
            continue
        ref = f"O-{number}_R-{number}"
        lines.append(f'{indent}  <ComObjectInstanceRef RefId="{ref}" Links="{" ".join(links)}" />')
        report.links += len(links)
        report.objects += 1
    lines.append(f"{indent}</ComObjectInstanceRefs>")
    block = newline.join(lines) + newline

    existing = re.search(
        r"[ \t]*<ComObjectInstanceRefs>.*?</ComObjectInstanceRefs>\r?\n?", body, re.S
    )
    if existing:
        report.replaced = len(re.findall(r"\bGA-\d+", existing.group(0)))
        body = body[: existing.start()] + block + body[existing.end() :]
    else:
        anchor = re.search(r"[ \t]*<(?:GroupObjectTree|Security)\b", body)
        if anchor:
            body = body[: anchor.start()] + block + body[anchor.start() :]
        else:
            body = body.rstrip() + newline + block + indent[:-2]
    return project_xml[: start.end()] + body + project_xml[end:], report


def write_unprotected(
    source: Path,
    output: Path,
    project_id: str,
    project_files: Mapping[str, bytes],
    drop_signature: bool = False,
) -> None:
    """Write the export back as an unprotected project archive: the
    source's product data untouched, the project folder in the plain
    layout ETS uses without a password."""
    dropped = {f"{project_id}.zip"}
    if drop_signature:
        dropped |= {f"{project_id}.signature", f"{project_id}.certificate", ".validation"}
    with zipfile.ZipFile(source) as src, zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as dst:
        for info in src.infolist():
            if info.filename in dropped or info.filename.startswith(f"{project_id}/"):
                continue
            dst.writestr(info, src.read(info))
        for name, data in project_files.items():
            dst.writestr(f"{project_id}/{name}", data)


def _strip_project_password(project_meta: bytes) -> bytes:
    text = project_meta.decode("utf-8")
    return re.sub(r'\s+ProjectPassword="[^"]*"', "", text, count=1).encode("utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Write the software devices' group links into an ETS export"
    )
    parser.add_argument("--input", "-i", required=True, type=Path, help="Path to .knxproj file")
    parser.add_argument(
        "--password",
        default=os.environ.get("KNXPROJ_PASSWORD"),
        help="ETS project password (if encrypted); defaults to $KNXPROJ_PASSWORD",
    )
    parser.add_argument(
        "--device",
        action="append",
        required=True,
        metavar="@FILE=NAME:MODE",
        help="Device to link, exactly as given to knxproj-to-kaenx. Repeatable.",
    )
    parser.add_argument(
        "--write-gas",
        type=Path,
        default=None,
        help="File with the consumed addresses (one per line); required for mode 'split'.",
    )
    parser.add_argument(
        "--output", "-o", required=True, type=Path, help="Path of the linked .knxproj to write"
    )
    parser.add_argument(
        "--drop-signature",
        action="store_true",
        help="Leave the export's signature files out instead of carrying them over",
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
            raise SystemExit(f"--device {spec.name}: linking needs a @file footprint source")
    write_gas = frozenset[str]()
    if args.write_gas is not None:
        write_gas = read_ga_list(args.write_gas.read_text(encoding="utf-8"), origin="--write-gas")
    elif any(s.mode == "split" for s in specs):
        raise SystemExit("--write-gas is required when a device uses mode 'split'")

    logger.info("parsing %s", args.input)
    project_data = _load_project(args.input, args.password)

    with extract(args.input, args.password) as proj, zipfile.ZipFile(args.input) as root:
        archive = proj._project_archive  # noqa: SLF001 — xknxproject exposes no public reader
        prefix = proj._project_relative_path  # noqa: SLF001
        project_files = {
            name[len(prefix) :]: archive.read(name)
            for name in archive.namelist()
            if name.startswith(prefix) and not name.endswith("/")
        }
        project_id = next(
            n.split("/", 1)[0].removesuffix(".zip") for n in root.namelist() if n.startswith("P-")
        )
        applications = {
            n.rsplit("/", 1)[-1].removesuffix(".xml"): root.read(n).decode("utf-8")
            for n in root.namelist()
            if n.startswith(f"{_MANUFACTURER}/{_MANUFACTURER}_A-")
        }

    project_xml = project_files["0.xml"].decode("utf-8")
    ga_ids = group_address_ids(project_xml)

    clean = True
    for spec in specs:
        build = DeviceReport()
        collectors = build_collectors(spec, project_data, write_gas, build)
        h2p = re.search(
            rf'<DeviceInstance\b[^>]*\bProductRefId="{re.escape(_MANUFACTURER)}_H-[^"]*_P-{re.escape(kaenx_encode(spec.slug))}"[^>]*\bHardware2ProgramRefId="[^"]*_HP-([0-9A-F]+-[0-9A-F]+-[0-9A-F]+)"',
            project_xml,
        )
        if not h2p:
            logger.error("%s: not in the project — import and add the device first", spec.name)
            clean = False
            continue
        application = applications.get(f"{_MANUFACTURER}_A-{h2p.group(1)}")
        if application is None:
            raise SystemExit(
                f"{spec.name}: application program {h2p.group(1)} missing from the export"
            )
        verify_installed(collectors, installed_objects(application), spec.name)

        project_xml, report = link_device(project_xml, spec, collectors, ga_ids)
        missing = [ga for c in collectors for ga, _ in c.entries if ga not in ga_ids]
        logger.info(
            "%s: %d links on %d objects written (replaced %d)%s",
            spec.name,
            report.links,
            report.objects,
            report.replaced,
            f"; {len(missing)} addresses not in the project" if missing else "",
        )
        for ga in build.missing:
            logger.warning("%s: %s is in the footprint but not in the ETS project", spec.name, ga)

    if not clean:
        return 1
    project_files["0.xml"] = project_xml.encode("utf-8")
    project_files["project.xml"] = _strip_project_password(project_files["project.xml"])
    write_unprotected(args.input, args.output, project_id, project_files, args.drop_signature)
    logger.info("wrote %s — import it into ETS; it carries no project password", args.output)
    return 0


if __name__ == "__main__":
    sys.exit(main())
