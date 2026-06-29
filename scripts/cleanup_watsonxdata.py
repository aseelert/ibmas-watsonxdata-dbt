#!/usr/bin/env python3
# -----------------------------------------------------------------------------
#  cleanup_watsonxdata.py — drop the medallion demo schemas from watsonx.data
#
#  Location  : scripts/cleanup_watsonxdata.py
#  Repository: https://github.ibm.com/alexander/ibmas-watsonxdata-dbt
#  Project   : watsonx.data · dbt · Spark · Confluent medallion demo
#  Author    : Alexander Seelert — IBM Customer Success Engineer
#  Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.
#
#  Changelog :
#    v1.1 (2026-06-26) — Add the Confluent silver+gold schemas to the drop list
#      (CONFLUENT_SILVER_SCHEMA / CONFLUENT_GOLD_SCHEMA) and a --confluent-only
#      flag that drops ONLY those two, for a scoped Confluent reset.
#    v1.0 (earlier) — Initial version. Drops the dbt + Spark + cpdctl demo schemas.
# -----------------------------------------------------------------------------
"""Drop all demo schemas (and their tables/views) from watsonx.data.

This is the catalog-side teardown counterpart to ``bootstrap_watsonxdata.py``.
It connects to the watsonx.data Presto (Iceberg) engine and removes every schema
the demo creates so the medallion pipeline can be rebuilt from a clean slate:
the dbt schemas (``dbt_demo_raw``/``_bronze``/``_silver``/``_gold``), the Spark
schemas (``spark_demo_bronze``/``_silver``/``_gold``), the cpdctl native
ingest raw schema (``WXD_INGEST_SCHEMA``, e.g. ``spark_demo_cpdctl_raw``), and
the Confluent schemas (``CONFLUENT_SILVER_SCHEMA``/``CONFLUENT_GOLD_SCHEMA``,
defaults ``confluent_demo_silver``/``confluent_demo_gold``). It exists so a
presenter can reset the demo deterministically between runs.

Pass ``--confluent-only`` to drop ONLY the two Confluent schemas (silver + gold)
and leave the dbt + Spark + cpdctl schemas untouched — used by the
``--confluent`` surface of ``scripts/reset_demo.sh`` for a scoped Confluent reset.

WHEN to run it
  Run this when you want to tear down the catalog state — typically as part of a
  full reset, before re-running ``bootstrap_watsonxdata.py`` + ingest + dbt. It
  is usually invoked from the all-in-one ``scripts/reset_demo.sh``. It is safe to
  re-run: non-existent schemas are skipped.

  For safety it only touches the exact schema names derived from ``WXD_SCHEMA``,
  ``WXD_SPARK_SCHEMA`` and ``WXD_INGEST_SCHEMA`` (no wildcards). Tables and views
  are dropped first — Iceberg schemas must be empty before they can be dropped —
  then the schema itself.

ENV VARS read
  - WXD_USER ................ Presto user (default ``ibmlhapikey_cpadmin``)
  - WXD_API_KEY ............. IBM Cloud / CPD API key used as the password (required)
  - WXD_HOST ................ Presto host (required)
  - WXD_PORT ................ Presto port (default ``443``)
  - WXD_CATALOG ............. Iceberg catalog name (default ``iceberg_data``)
  - WXD_SCHEMA .............. dbt base schema name (default ``dbt_demo``)
  - WXD_SPARK_SCHEMA ........ Spark base schema name (default ``spark_demo``)
  - WXD_RAW_SCHEMA / WXD_BRONZE_SCHEMA / WXD_SILVER_SCHEMA / WXD_GOLD_SCHEMA
                             override the individual dbt layer schema names
  - WXD_SPARK_BRONZE_SCHEMA / WXD_SPARK_SILVER_SCHEMA / WXD_SPARK_GOLD_SCHEMA
                             override the individual Spark layer schema names
  - WXD_INGEST_SCHEMA ....... cpdctl native-ingest raw schema
                             (default ``<spark base>_cpdctl_raw``)
  - CONFLUENT_SILVER_SCHEMA . Flink-written Confluent silver schema
                             (default ``confluent_demo_silver``)
  - CONFLUENT_GOLD_SCHEMA ... Confluent gold-mart schema
                             (default ``confluent_demo_gold``)
  - WXD_INSTANCE_ID ......... if set, sent as the ``LhInstanceId`` HTTP header
  - WXD_SSL_VERIFY .......... CA bundle path, or ``true``/``false`` to toggle
                             TLS verification (default ``certs/watsonxdata-ca.pem``)

Prerequisites
  - A running, resumed watsonx.data Presto engine reachable at WXD_HOST:WXD_PORT.
  - ``presto-python-client`` installed (``pip install -r requirements.txt``).
  - A valid WXD_API_KEY. No ``oc login`` / ``cpdctl`` is required for this step.

USAGE
    python scripts/cleanup_watsonxdata.py                  # drop ALL demo schemas
    python scripts/cleanup_watsonxdata.py --confluent-only # drop ONLY confluent_*

Side effects + exit behavior
  DESTRUCTIVE: drops the listed schemas and all their tables/views from the
  catalog. Prints the connection target, the full target-schema list, each SQL
  statement, and a final summary of objects/schemas dropped. Returns exit code 0
  on success; raises ``SystemExit`` with a message on missing env vars / missing
  dependency, and propagates Presto errors otherwise.

  Note: dropping the schemas removes the catalog objects, but Iceberg data files
  may linger in object storage. Run ``scripts/cleanup_minio.py`` (or the
  all-in-one ``scripts/reset_demo.sh``) afterwards to delete the underlying MinIO
  files too.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path


try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None


def _env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None or value == "":
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


def _ssl_verify() -> bool | str:
    value = os.getenv("WXD_SSL_VERIFY", "certs/watsonxdata-ca.pem").strip()
    if value.lower() in {"0", "false", "no"}:
        return False
    if value.lower() in {"1", "true", "yes"}:
        return True

    path = Path(value).expanduser()
    if not path.is_absolute():
        path = Path(__file__).resolve().parents[1] / path
    return str(path)


def _http_headers() -> dict[str, str] | None:
    instance_id = os.getenv("WXD_INSTANCE_ID", "").strip()
    if not instance_id:
        return None
    return {"LhInstanceId": instance_id}


def _execute(cur, sql, prestodb):
    """Run a statement, retrying once on the transient AMS token error."""
    try:
        cur.execute(sql)
        cur.fetchall()
    except prestodb.exceptions.HttpError as exc:
        if "AMS_CANNOT_GET_TOKEN" not in str(exc):
            raise
        print("  AMS token not ready (AMS_CANNOT_GET_TOKEN); waiting 2s then retrying once...")
        time.sleep(2)
        cur.execute(sql)
        cur.fetchall()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Drop the medallion demo schemas (and their tables/views) "
        "from watsonx.data.",
    )
    parser.add_argument(
        "--confluent-only",
        action="store_true",
        help="Drop ONLY the Confluent silver+gold schemas "
        "(CONFLUENT_SILVER_SCHEMA / CONFLUENT_GOLD_SCHEMA), leaving the dbt + "
        "Spark + cpdctl schemas in place. Used for a scoped Confluent reset.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()

    if load_dotenv is not None:
        load_dotenv()

    try:
        import prestodb
    except ImportError as exc:
        raise SystemExit(
            "Missing dependency 'presto-python-client'. Install dependencies with: "
            "python -m pip install -r requirements.txt"
        ) from exc

    user = _env("WXD_USER", "ibmlhapikey_cpadmin")
    password = _env("WXD_API_KEY")
    host = _env("WXD_HOST")
    port = int(_env("WXD_PORT", "443"))
    catalog = _env("WXD_CATALOG", "iceberg_data")
    dbt_base = _env("WXD_SCHEMA", "dbt_demo")
    spark_base = os.getenv("WXD_SPARK_SCHEMA", "spark_demo")

    # The Confluent path's silver (Flink-written) + gold (Spark/DataStage-built)
    # schemas. Env-driven so nothing is hardcoded; always part of the demo's own
    # state, so they belong in every full reset.
    confluent_schemas = [
        os.getenv("CONFLUENT_SILVER_SCHEMA", "confluent_demo_silver"),
        os.getenv("CONFLUENT_GOLD_SCHEMA", "confluent_demo_gold"),
    ]

    if args.confluent_only:
        # Scoped Confluent reset: touch ONLY the two confluent_* schemas.
        schemas = list(confluent_schemas)
    else:
        # Full reset: the dbt + Spark + cpdctl schemas, plus the Confluent ones.
        schemas = [
            os.getenv("WXD_RAW_SCHEMA", f"{dbt_base}_raw"),
            os.getenv("WXD_BRONZE_SCHEMA", f"{dbt_base}_bronze"),
            os.getenv("WXD_SILVER_SCHEMA", f"{dbt_base}_silver"),
            os.getenv("WXD_GOLD_SCHEMA", f"{dbt_base}_gold"),
            os.getenv("WXD_SPARK_BRONZE_SCHEMA", f"{spark_base}_bronze"),
            os.getenv("WXD_SPARK_SILVER_SCHEMA", f"{spark_base}_silver"),
            os.getenv("WXD_SPARK_GOLD_SCHEMA", f"{spark_base}_gold"),
            os.getenv("WXD_INGEST_SCHEMA", f"{spark_base}_cpdctl_raw"),
            *confluent_schemas,
        ]

    print(f"Connecting to {host}:{port}, catalog={catalog}")
    print("Target schemas:")
    for schema in schemas:
        print(f"  - {catalog}.{schema}")

    print(f"Connecting to Presto {host}:{port} (catalog={catalog}) ...")
    conn = prestodb.dbapi.connect(
        host=host,
        port=port,
        user=user,
        catalog=catalog,
        http_scheme="https",
        http_headers=_http_headers(),
        auth=prestodb.auth.BasicAuthentication(user, password),
        # Per-request socket timeout so a suspended/resuming engine can't hang.
        request_timeout=60,
    )
    conn._http_session.verify = _ssl_verify()
    print("Connected.")
    cur = conn.cursor()

    dropped_objects = 0
    dropped_schemas = 0

    for schema in schemas:
        # Does the schema exist?
        cur.execute(
            f"select schema_name from {catalog}.information_schema.schemata "
            f"where schema_name = '{schema}'"
        )
        if not cur.fetchall():
            print(f"skip {catalog}.{schema} (does not exist)")
            continue

        # List tables and views in the schema.
        cur.execute(
            f"select table_name, table_type from {catalog}.information_schema.tables "
            f"where table_schema = '{schema}'"
        )
        objects = cur.fetchall()
        for table_name, table_type in objects:
            kind = "view" if str(table_type).upper() == "VIEW" else "table"
            fqn = f"{catalog}.{schema}.{table_name}"
            sql = f"drop {kind} if exists {fqn}"
            print(f"SQL> {sql}")
            _execute(cur, sql, prestodb)
            dropped_objects += 1

        sql = f"drop schema if exists {catalog}.{schema}"
        print(f"SQL> {sql}")
        _execute(cur, sql, prestodb)
        dropped_schemas += 1
        print(f"dropped {catalog}.{schema}")

    print(
        f"\n[OK] Cleanup complete: dropped {dropped_objects} object(s) "
        f"across {dropped_schemas} schema(s)."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
