#!/usr/bin/env python3
"""Drop all demo schemas (and their tables/views) from watsonx.data.

Removes the dbt schemas (dbt_demo_raw/bronze/silver/gold), the Spark schemas
(spark_demo_bronze/silver/gold), and the cpdctl native-ingest raw schema
(WXD_INGEST_SCHEMA, e.g. spark_demo_cpdctl_raw) so the demo can be rebuilt from
scratch.

For safety this only touches the exact schema names derived from WXD_SCHEMA,
WXD_SPARK_SCHEMA, and WXD_INGEST_SCHEMA. Tables and views are dropped first
(Iceberg schemas must be empty before they can be dropped), then the schema
itself.

Note: dropping the schemas removes the catalog objects, but Iceberg data files
may linger in object storage. Run scripts/cleanup_minio.py (or the all-in-one
scripts/reset_demo.sh) afterwards to delete the underlying MinIO files too.
"""

from __future__ import annotations

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


def main() -> int:
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

    schemas = [
        os.getenv("WXD_RAW_SCHEMA", f"{dbt_base}_raw"),
        os.getenv("WXD_BRONZE_SCHEMA", f"{dbt_base}_bronze"),
        os.getenv("WXD_SILVER_SCHEMA", f"{dbt_base}_silver"),
        os.getenv("WXD_GOLD_SCHEMA", f"{dbt_base}_gold"),
        os.getenv("WXD_SPARK_BRONZE_SCHEMA", f"{spark_base}_bronze"),
        os.getenv("WXD_SPARK_SILVER_SCHEMA", f"{spark_base}_silver"),
        os.getenv("WXD_SPARK_GOLD_SCHEMA", f"{spark_base}_gold"),
        os.getenv("WXD_INGEST_SCHEMA", f"{spark_base}_cpdctl_raw"),
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
        f"\nCleanup complete: dropped {dropped_objects} object(s) "
        f"across {dropped_schemas} schema(s)."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
