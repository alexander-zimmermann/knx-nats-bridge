"""Convert an ETS .knxproj export into the ga-mapping.yaml format.

Requires the optional dependency `xknxproject` (install via
`pip install "knx-nats-bridge[tools]"`).

Example:
    knxproj-to-yaml --input my-project.knxproj --output ga-mapping.yaml \\
        [--password 'ets-password']
"""

from __future__ import annotations

import argparse
import logging
import sys
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
    group_addresses = project_data.get("group_addresses", {}) or {}
    for ga_str, ga_info in group_addresses.items():
        if not isinstance(ga_info, dict):
            continue
        name = ga_info.get("name") or ga_info.get("identifier") or ga_str
        dpt = _extract_dpt(ga_info.get("dpt"))
        if dpt is None:
            continue
        mapping[str(ga_str)] = {"name": str(name), "dpt": dpt}


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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Convert .knxproj to ga-mapping.yaml")
    parser.add_argument("--input", "-i", required=True, type=Path, help="Path to .knxproj file")
    parser.add_argument(
        "--output", "-o", required=True, type=Path, help="Path to ga-mapping.yaml output"
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

    args.output.write_text(
        yaml.safe_dump(mapping, sort_keys=True, allow_unicode=True, default_flow_style=False),
        encoding="utf-8",
    )
    logger.info("wrote %d entries to %s", len(mapping), args.output)
    return 0


if __name__ == "__main__":
    sys.exit(main())
