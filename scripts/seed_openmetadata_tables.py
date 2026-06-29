#!/usr/bin/env python3
# -----------------------------------------------------------------------------
#  seed_openmetadata_tables.py — create the medallion table entities in OpenMetadata
#                                from the dbt catalog, so dbt lineage can attach.
#
#  Location  : scripts/seed_openmetadata_tables.py
#  Repository: https://github.ibm.com/alexander/ibmas-watsonxdata-dbt
#  Project   : watsonx.data · dbt · Spark · Confluent medallion demo
#  Author    : Alexander Seelert — IBM Customer Success Engineer
#  Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.
#
#  Changelog :
#    v1.0 (2026-06-26) — Initial version. Seeds OpenMetadata with the medallion
#      table entities from the dbt catalog so dbt lineage can attach.
# -----------------------------------------------------------------------------
"""Seed OpenMetadata with the medallion tables from the dbt catalog.

WHY this exists
  OpenMetadata's dbt ingestion (``openmetadata/ingestion/run-ingestion.sh``) is
  an *enrichment* pass: it only draws lineage between tables that ALREADY exist
  as entities in OpenMetadata. The normal path creates those entities from a
  live watsonx.data Presto metadata ingestion. If Presto is down or credentials
  are unavailable, this script closes the gap WITHOUT a live connection: it reads the dbt
  ``catalog.json`` (already staged in ``openmetadata/dbt-artifacts/``), which
  carries every model's database/schema/name and its columns + types, and
  creates the matching Database → Schema → Table entities in OpenMetadata via the
  REST API. The FQNs it builds
  (``watsonxdata-presto.iceberg_data.dbt_demo_<layer>.<model>``) are exactly the
  ones the dbt manifest references, so the subsequent dbt ingestion finds every
  table and attaches descriptions + bronze→silver→gold lineage.

WHEN to run it
  AFTER the artifacts are staged (run ``scripts/generate_lineage_docs.sh`` or
  ``scripts/prepare_openmetadata_dbt_artifacts.py`` first) and the local
  OpenMetadata stack is healthy on localhost:8585. Run it BEFORE
  ``openmetadata/ingestion/run-ingestion.sh``. Re-running is safe: every write is
  an idempotent PUT (create-or-update).

ENV VARS read
  - ``OM_BASE`` — OpenMetadata base URL (default ``http://localhost:8585``).
  - ``WXD_DBT_ARTIFACT_DIR`` — where catalog.json was staged
    (default ``openmetadata/dbt-artifacts``).
  The OpenMetadata service name / catalog are read from the catalog.json content
  and the ingestion config, so they stay in sync with the dbt manifest.

PREREQUISITES
  A healthy local OpenMetadata (the ``admin@open-metadata.org`` account and the
  ``ingestion-bot`` both ship with OM); a staged ``catalog.json``; and
  ``openmetadata/ingestion/get_om_token.py`` (used to mint the bot JWT).

USAGE
  python3 scripts/seed_openmetadata_tables.py
  python3 scripts/seed_openmetadata_tables.py --service watsonxdata-presto

SIDE EFFECTS + EXIT
  Creates/updates one Database, the medallion schemas, and one Table per dbt
  model in OpenMetadata. Prints a per-entity summary. Exits 0 on success,
  non-zero if the token cannot be minted or catalog.json is missing.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
OM_BASE = os.getenv("OM_BASE", "http://localhost:8585")
GET_TOKEN = ROOT / "openmetadata" / "ingestion" / "get_om_token.py"

# dbt/Presto column type -> (OpenMetadata dataType, extra fields builder)
# OM requires dataLength for VARCHAR/CHAR and precision/scale for DECIMAL.
_DECIMAL_RE = re.compile(r"decimal\((\d+),\s*(\d+)\)", re.IGNORECASE)


def _map_column_type(raw: str) -> dict:
    t = raw.strip().lower()
    col: dict = {"dataTypeDisplay": raw}
    m = _DECIMAL_RE.match(t)
    if m:
        col["dataType"] = "DECIMAL"
        col["precision"] = int(m.group(1))
        col["scale"] = int(m.group(2))
        return col
    simple = {
        "varchar": ("VARCHAR", 65535),
        "char": ("CHAR", 255),
        "integer": ("INT", None),
        "int": ("INT", None),
        "bigint": ("BIGINT", None),
        "double": ("DOUBLE", None),
        "float": ("FLOAT", None),
        "boolean": ("BOOLEAN", None),
        "date": ("DATE", None),
        "timestamp": ("TIMESTAMP", None),
        "timestamp with time zone": ("TIMESTAMPZ", None),
        "timestamp(3)": ("TIMESTAMP", None),
    }
    if t in simple:
        dtype, length = simple[t]
        col["dataType"] = dtype
        if length is not None:
            col["dataLength"] = length
        return col
    # Unknown/complex type — keep it as UNKNOWN but preserve the display string so
    # the column still appears in OpenMetadata rather than failing the whole table.
    col["dataType"] = "UNKNOWN"
    return col


def _mint_token() -> str:
    try:
        out = subprocess.run(
            [sys.executable, str(GET_TOKEN)],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as exc:
        sys.stderr.write(exc.stderr or "")
        raise SystemExit("Could not mint an OpenMetadata token (is OM up on localhost:8585?).")
    token = out.stdout.strip()
    if not token:
        raise SystemExit("get_om_token.py returned an empty token.")
    return token


def _put(session: requests.Session, path: str, payload: dict) -> dict:
    resp = session.put(f"{OM_BASE}/api/v1/{path}", json=payload)
    if resp.status_code not in (200, 201):
        raise SystemExit(
            f"PUT /{path} failed ({resp.status_code}): {resp.text[:400]}"
        )
    return resp.json()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Create medallion table entities in OpenMetadata from the dbt catalog."
    )
    parser.add_argument(
        "--service",
        default="watsonxdata-presto",
        help="OpenMetadata database service name (must match dbt-ingestion.yaml).",
    )
    parser.add_argument(
        "--catalog-file",
        help="Path to the staged dbt catalog.json.",
    )
    args = parser.parse_args()

    artifact_dir = os.getenv("WXD_DBT_ARTIFACT_DIR", "openmetadata/dbt-artifacts")
    catalog_path = (
        Path(args.catalog_file).expanduser()
        if args.catalog_file
        else ROOT / artifact_dir / "catalog.json"
    )
    if not catalog_path.is_absolute():
        catalog_path = ROOT / catalog_path
    if not catalog_path.exists():
        raise SystemExit(
            f"catalog.json not found at {catalog_path}. "
            "Run scripts/generate_lineage_docs.sh first."
        )

    catalog = json.loads(catalog_path.read_text())
    nodes = catalog.get("nodes", {})
    if not nodes:
        raise SystemExit("catalog.json has no nodes — nothing to seed.")

    token = _mint_token()
    session = requests.Session()
    session.headers.update(
        {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    )

    service = args.service
    # All medallion models live in the same catalog (database); read it from the data.
    databases = sorted({n["metadata"]["database"] for n in nodes.values()})

    created_db: set[str] = set()
    created_schema: set[str] = set()
    table_count = 0

    for db in databases:
        _put(session, "databases", {"name": db, "service": service})
        created_db.add(db)
        print(f"[db]     {service}.{db}")

    for key, node in sorted(nodes.items(), key=lambda kv: kv[1]["metadata"]["schema"]):
        meta = node["metadata"]
        db = meta["database"]
        schema = meta["schema"]
        table = meta["name"]
        db_fqn = f"{service}.{db}"
        schema_fqn = f"{db_fqn}.{schema}"

        if schema_fqn not in created_schema:
            _put(session, "databaseSchemas", {"name": schema, "database": db_fqn})
            created_schema.add(schema_fqn)
            print(f"[schema] {schema_fqn}")

        columns = []
        for col in sorted(node["columns"].values(), key=lambda c: c.get("index", 0)):
            mapped = _map_column_type(col["type"])
            mapped["name"] = col["name"]
            if col.get("comment"):
                mapped["description"] = col["comment"]
            columns.append(mapped)

        _put(
            session,
            "tables",
            {"name": table, "databaseSchema": schema_fqn, "columns": columns},
        )
        table_count += 1
        print(f"[table]  {schema_fqn}.{table}  ({len(columns)} cols)")

    print()
    print(
        f"Seeded {len(created_db)} database(s), {len(created_schema)} schema(s), "
        f"{table_count} table(s) into OpenMetadata service '{service}'."
    )
    print("Next: run openmetadata/ingestion/run-ingestion.sh to attach dbt lineage.")
    return 0


if __name__ == "__main__":
    # Top-level safety net: known errors raise SystemExit with a clear message and
    # are passed through; anything unexpected is logged with context and exits 1.
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n[ERROR] interrupted by user", file=sys.stderr)
        sys.exit(130)
    except SystemExit:
        raise
    except Exception as exc:  # noqa: BLE001 — log the unexpected failure, then exit non-zero
        import traceback
        print(f"[ERROR] unexpected failure: {exc}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)
