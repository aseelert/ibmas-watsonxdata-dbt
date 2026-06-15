#!/usr/bin/env python3
"""Load the demo CSV files into watsonx.data with the native ingestion service.

This uses `cpdctl wx-data ingestion create`, which is the **supported, UI-visible**
way to load files. Unlike `dbt seed` (Presto) or the custom Spark application, each
ingestion job shows up in the watsonx.data console under
**Data manager -> Ingestion (history)** as `ingestion-<id>`.

It demonstrates the "CSV upload" path end to end: the CSVs already staged in object
storage (see scripts/upload_spark_assets.py) are ingested into Iceberg tables in a
dedicated schema.

Prerequisites
-------------
1. cpdctl installed and on PATH:   https://github.com/IBM/cpdctl/releases
2. cpdctl configured with a context for this CPD instance. One-time setup
   (values come from your .env):

       cpdctl config user set demo_user --username "$WXD_CPD_USERNAME" --apikey "$WXD_API_KEY"
       cpdctl config profile set demo_profile --url "https://$WXD_CPD_HOST"
       cpdctl config context set demo_ctx --profile demo_profile --user demo_user
       cpdctl config context use demo_ctx

3. The demo CSVs already uploaded to object storage:

       python scripts/upload_spark_assets.py
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None


ROOT = Path(__file__).resolve().parents[1]

# CSV file (under WXD_SPARK_INPUT_BASE) -> target table name in the ingest schema.
TABLES = {
    "raw_customers.csv": "customers",
    "raw_products.csv": "products",
    "raw_orders.csv": "orders",
    "raw_order_items.csv": "order_items",
}


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
        path = ROOT / path
    return str(path)


def _ensure_schema(catalog: str, schema: str) -> None:
    """Create the target schema with Presto so ingestion has somewhere to land."""
    import prestodb

    user = _env("WXD_USER", "ibmlhapikey_cpadmin")
    conn = prestodb.dbapi.connect(
        host=_env("WXD_HOST"),
        port=int(_env("WXD_PORT", "443")),
        user=user,
        catalog=catalog,
        http_scheme="https",
        http_headers={"LhInstanceId": _env("WXD_INSTANCE_ID")},
        auth=prestodb.auth.BasicAuthentication(user, _env("WXD_API_KEY")),
    )
    conn._http_session.verify = _ssl_verify()
    cur = conn.cursor()
    location_base = os.getenv("WXD_SCHEMA_LOCATION_BASE", "").rstrip("/")
    sql = f"create schema if not exists {catalog}.{schema}"
    if location_base:
        sql += f" with (location = '{location_base}/{schema}')"
    print(f"SQL> {sql}")
    cur.execute(sql)
    cur.fetchall()


def _ui_url() -> str:
    base = os.getenv("WXD_CONSOLE_URL")
    if not base:
        base = f"https://{_env('WXD_CPD_HOST')}/watsonx-data/#"
    return f"{base.rstrip('/')}/data-manager?instanceId={_env('WXD_INSTANCE_ID')}"


def main() -> int:
    if load_dotenv is not None:
        load_dotenv(ROOT / ".env")

    if shutil.which("cpdctl") is None:
        raise SystemExit(
            "cpdctl is not installed or not on PATH.\n"
            "Install it from https://github.com/IBM/cpdctl/releases, then configure a "
            "context (see this script's docstring) and re-run."
        )

    catalog = _env("WXD_SPARK_CATALOG", "iceberg_data")
    engine_id = _env("WXD_SPARK_ENGINE_ID", "spark656")
    base_schema = _env("WXD_SCHEMA", "lakehouse_demo")
    ingest_schema = os.getenv("WXD_INGEST_SCHEMA", f"{base_schema}_ingest")

    # cpdctl expects s3:// URIs; our env stores the Spark-style s3a:// base.
    input_base = _env("WXD_SPARK_INPUT_BASE", "s3a://iceberg-bucket/spark_demo/raw")
    s3_base = input_base.replace("s3a://", "s3://").rstrip("/")
    batch = os.getenv("WXD_INGEST_BATCH_ID", str(int(time.time())))

    print(f"Catalog: {catalog}")
    print(f"Spark engine: {engine_id}")
    print(f"Target schema: {catalog}.{ingest_schema}")
    print(f"Source base: {s3_base}")

    _ensure_schema(catalog, ingest_schema)

    failures = 0
    job_ids = []
    for csv_name, table in TABLES.items():
        target = f"{catalog}.{ingest_schema}.{table}"
        job_id = f"ingest-{table}-{batch}"
        cmd = [
            "cpdctl", "wx-data", "ingestion", "create",
            "--instance-id", _env("WXD_INSTANCE_ID"),
            "--source-data-files", f"{s3_base}/{csv_name}",
            "--source-file-type", "csv",
            "--target-table", target,
            "--engine-id", engine_id,
            "--job-id", job_id,
        ]
        if profile := os.getenv("WXD_CPDCTL_PROFILE"):
            cmd += ["--profile", profile]
        if storage := os.getenv("WXD_INGEST_STORAGE_NAME"):
            cmd += ["--storage-name", storage]
        print("\n$ " + " ".join(cmd))
        result = subprocess.run(cmd, text=True)
        if result.returncode == 0:
            job_ids.append(job_id)
        else:
            failures += 1
            print(f"!! ingestion failed for {csv_name} (exit {result.returncode})")

    print("\n" + "=" * 74)
    print(f"Submitted {len(job_ids)} ingestion job(s); {failures} failed.")
    for jid in job_ids:
        print(f"  job_id: {jid}")
    print("\nThese appear in the watsonx.data console:")
    print(f"  {_ui_url()}")
    print("  Data manager -> Ingestion (history)")
    print("=" * 74)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
