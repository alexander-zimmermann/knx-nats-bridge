"""Import the GA catalog YAML into TSDB's ``ga_catalog`` table.

One-shot CLI invoked as a Kubernetes Job (PostSync hook after each
Argo sync of the catalog ConfigMap). Idempotent:

* INSERT ... ON CONFLICT (ga) DO UPDATE — UPDATE skipped when nothing
  actually changed (no spurious updated_at churn)
* DELETE rows whose GA is no longer in the YAML (tombstone purge)

Connection from the standard ``MCP_DB_*`` env vars (host / port / name
/ username / password) — same shape iot-mcp-bridge uses, so the same
SealedSecret + Kyverno-clone topology applies. Username + password
URL-encoded because random-generated passwords routinely contain `/`,
`@`, `:`, `+` that break psycopg's URI parser.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Any, cast
from urllib.parse import quote

import psycopg
import yaml

logger = logging.getLogger(__name__)


_UPSERT_SQL = """
INSERT INTO ga_catalog (ga, name, room, function, description, dpt, updated_at)
VALUES (%s, %s, %s, %s, %s, %s, now())
ON CONFLICT (ga) DO UPDATE SET
    name        = EXCLUDED.name,
    room        = EXCLUDED.room,
    function    = EXCLUDED.function,
    description = EXCLUDED.description,
    dpt         = EXCLUDED.dpt,
    updated_at  = now()
WHERE
    ga_catalog.name        IS DISTINCT FROM EXCLUDED.name OR
    ga_catalog.room        IS DISTINCT FROM EXCLUDED.room OR
    ga_catalog.function    IS DISTINCT FROM EXCLUDED.function OR
    ga_catalog.description IS DISTINCT FROM EXCLUDED.description OR
    ga_catalog.dpt         IS DISTINCT FROM EXCLUDED.dpt
"""


def _read_catalog(path: Path) -> dict[str, Any]:
    raw: object = yaml.safe_load(path.read_text(encoding="utf-8"))
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ValueError(f"{path}: expected mapping at top level, got {type(raw).__name__}")
    return cast("dict[str, Any]", raw)


def _to_rows(catalog: dict[str, Any]) -> list[tuple[Any, ...]]:
    rows: list[tuple[Any, ...]] = []
    for ga, entry in catalog.items():
        if not isinstance(entry, dict):
            raise ValueError(f"entry for {ga!r} must be a mapping")
        e = cast("dict[str, Any]", entry)
        name = e.get("name")
        dpt = e.get("dpt")
        if not isinstance(name, str) or not isinstance(dpt, str):
            raise ValueError(f"entry for {ga!r} requires string 'name' and 'dpt'")
        rows.append(
            (
                ga,
                name,
                e.get("room"),
                e.get("function"),
                e.get("description"),
                dpt,
            )
        )
    return rows


def _dsn_from_env() -> str:
    required = ("MCP_DB_HOST", "MCP_DB_NAME", "MCP_DB_USERNAME", "MCP_DB_PASSWORD")
    missing = [v for v in required if not os.environ.get(v)]
    if missing:
        raise SystemExit(f"missing required env vars: {', '.join(missing)}")
    host = os.environ["MCP_DB_HOST"]
    name = os.environ["MCP_DB_NAME"]
    user = os.environ["MCP_DB_USERNAME"]
    password = os.environ["MCP_DB_PASSWORD"]
    port = os.environ.get("MCP_DB_PORT", "5432")
    return f"postgresql://{quote(user, safe='')}:{quote(password, safe='')}@{host}:{port}/{name}"


def run(catalog_path: Path, dsn: str) -> int:
    catalog = _read_catalog(catalog_path)
    rows = _to_rows(catalog)
    if not rows:
        logger.warning("catalog is empty: %s", catalog_path)
        return 0

    keys = list(catalog.keys())
    with psycopg.connect(dsn, autocommit=False) as conn, conn.cursor() as cur:
        cur.executemany(_UPSERT_SQL, rows)
        cur.execute("DELETE FROM ga_catalog WHERE ga != ALL(%s)", (keys,))
        deleted = cur.rowcount
        conn.commit()

    logger.info(
        "imported %d entries from %s; deleted %d stale row(s)",
        len(rows),
        catalog_path,
        max(deleted, 0),
    )
    return len(rows)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Import GA catalog into ga_catalog")
    parser.add_argument(
        "--catalog-path",
        type=Path,
        required=True,
        help="Path to the ga-catalog.yaml file",
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    run(args.catalog_path, _dsn_from_env())
    return 0


if __name__ == "__main__":
    sys.exit(main())
