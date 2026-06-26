#!/usr/bin/env python3
# =============================================================================
#  prep_iceberg_schemas.py — create watsonx.data schemas + register Flink tables
# -----------------------------------------------------------------------------
#  Location  : confluent/scripts/prep_iceberg_schemas.py
#  Repository: ibmas-watsonxdata-dbt
#
#  TWO PHASES — controlled by --phase argument:
#
#  Phase A (--phase schema):
#    Run BEFORE Flink starts. Creates the two Iceberg schemas in watsonx.data
#    via Presto so they exist when Flink writes the first checkpoint.
#    Docker service: confluent-schema-prep
#
#  Phase B (--phase register):
#    Run AFTER Flink has committed at least one checkpoint. Queries the local
#    Iceberg REST catalog (confluent-iceberg-rest:8181) to discover the current
#    metadata.json location for each silver table, then calls
#    CALL iceberg_data.system.register_table(...) via Presto so each table
#    becomes visible in watsonx.data alongside dbt_demo_silver.* and
#    spark_demo_silver.*.
#    Docker service: confluent-prep
#
#  ENV VARS (read from .env via python-dotenv):
#    WXD_USER, WXD_API_KEY, WXD_HOST, WXD_PORT, WXD_INSTANCE_ID,
#    WXD_SSL_VERIFY, WXD_CATALOG (default: iceberg_data)
#    CONFLUENT_SILVER_SCHEMA    (default: confluent_demo_silver)
#    ICEBERG_REST_URL           (default: http://confluent-iceberg-rest:8181)
#
#  PREREQUISITES  (all in requirements.txt — installed into .venv)
#    pip install -r requirements.txt   # or: bash confluent/start.sh (auto-installs)
# =============================================================================
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

try:
    import prestodb
    import prestodb.auth
    import prestodb.exceptions
except ImportError as exc:
    raise SystemExit(
        "Missing dependency 'presto-python-client'. "
        "Install with: pip install presto-python-client"
    ) from exc

try:
    import requests as _requests
except ImportError as exc:
    raise SystemExit(
        "Missing dependency 'requests'. Install with: pip install requests"
    ) from exc

ROOT = Path(__file__).resolve().parents[2]

# Silver tables Flink produces — must match silver_jobs.sql sink table names
SILVER_TABLES = [
    "confluent_silver_customers",
    "confluent_silver_products",
    "confluent_silver_orders",
    "confluent_silver_order_items",
    "confluent_silver_sales_enriched",
]


# ---------------------------------------------------------------------------
# Helpers (same pattern as scripts/bootstrap_watsonxdata.py)
# ---------------------------------------------------------------------------

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


def _http_headers() -> dict[str, str] | None:
    instance_id = os.getenv("WXD_INSTANCE_ID", "").strip()
    return {"LhInstanceId": instance_id} if instance_id else None


def _presto_connect():
    user = _env("WXD_USER", "ibmlhapikey_cpadmin")
    password = _env("WXD_API_KEY")
    host = _env("WXD_HOST")
    port = int(_env("WXD_PORT", "443"))
    catalog = _env("WXD_CATALOG", "iceberg_data")

    print(f"Connecting to Presto {host}:{port} (catalog={catalog}) ...")
    conn = prestodb.dbapi.connect(
        host=host,
        port=port,
        user=user,
        catalog=catalog,
        http_scheme="https",
        http_headers=_http_headers(),
        auth=prestodb.auth.BasicAuthentication(user, password),
        request_timeout=60,
    )
    conn._http_session.verify = _ssl_verify()
    print("Connected.")
    return conn, catalog


def _execute(cur, sql: str, swallow: bool = False) -> bool:
    print(f"SQL> {sql}")
    try:
        cur.execute(sql)
        cur.fetchall()
        return True
    except prestodb.exceptions.PrestoUserError as exc:
        if swallow:
            print(f"  (ignored) {exc.message}")
            return False
        raise


# ---------------------------------------------------------------------------
# Phase A — create schemas
# ---------------------------------------------------------------------------

def phase_schema() -> int:
    silver_schema = os.getenv("CONFLUENT_SILVER_SCHEMA", "confluent_demo_silver")

    conn, catalog = _presto_connect()
    cur = conn.cursor()

    _execute(cur, f"CREATE SCHEMA IF NOT EXISTS {catalog}.{silver_schema}")
    print(f"ensured {catalog}.{silver_schema}")

    print("[OK] Phase A complete — schemas ready.")
    return 0


# ---------------------------------------------------------------------------
# Phase B — discover metadata locations + register_table
# ---------------------------------------------------------------------------

def _get_metadata_location(iceberg_rest_url: str, namespace: str, table: str) -> str | None:
    """Call the Iceberg REST catalog API to get the current metadata-location."""
    url = f"{iceberg_rest_url.rstrip('/')}/v1/namespaces/{namespace}/tables/{table}"
    try:
        resp = _requests.get(url, timeout=10)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        data = resp.json()
        # metadata-location is in the top-level 'metadata-location' key
        loc = data.get("metadata-location") or data.get("metadata", {}).get("location")
        return loc
    except Exception as exc:
        print(f"  WARN: could not query iceberg-rest for {namespace}/{table}: {exc}")
        return None


def phase_register() -> int:
    silver_schema = os.getenv("CONFLUENT_SILVER_SCHEMA", "confluent_demo_silver")
    iceberg_rest = os.getenv("ICEBERG_REST_URL", "http://confluent-iceberg-rest:8181")

    conn, catalog = _presto_connect()
    cur = conn.cursor()

    # Find already-registered tables so we stay idempotent
    registered: set[str] = set()
    try:
        cur.execute(f"SHOW TABLES IN {catalog}.{silver_schema}")
        registered = {row[0].lower() for row in cur.fetchall()}
        print(f"Already registered in {catalog}.{silver_schema}: {registered or '(none)'}")
    except prestodb.exceptions.PrestoUserError:
        pass  # schema may not exist yet — phase A should have run first

    registered_count = 0
    skipped_count = 0

    for table in SILVER_TABLES:
        if table.lower() in registered:
            print(f"  SKIP {table} — already registered in {catalog}.{silver_schema}")
            skipped_count += 1
            continue

        # Retry getting metadata location for up to 60s (Flink may still be committing)
        metadata_loc = None
        for attempt in range(20):
            metadata_loc = _get_metadata_location(iceberg_rest, silver_schema, table)
            if metadata_loc:
                break
            print(f"  Table {table} not yet in iceberg-rest (attempt {attempt + 1}/20) — wait 3s")
            time.sleep(3)

        if not metadata_loc:
            print(f"  ERROR: could not find metadata location for {table} — skipping")
            continue

        # Presto's register_table(schema, table, location) expects the TABLE LOCATION
        # directory (not the metadata file path). Strip the /metadata/<file>.metadata.json
        # suffix so Presto can discover the latest metadata.json automatically.
        # e.g. s3://iceberg-bucket/.../silver_customers/metadata/00002-....metadata.json
        #   →  s3://iceberg-bucket/.../silver_customers
        table_location = metadata_loc
        if "/metadata/" in metadata_loc and metadata_loc.endswith(".metadata.json"):
            table_location = metadata_loc[: metadata_loc.rfind("/metadata/")]
        print(f"  table_location for {table}: {table_location}")

        # watsonx.data Presto uses positional arguments (not named).
        # Pass the TABLE LOCATION directory — Presto discovers metadata.json from it.
        sql = (
            f"CALL {catalog}.system.register_table("
            f"'{silver_schema}', "
            f"'{table}', "
            f"'{table_location}'"
            f")"
        )
        if _execute(cur, sql, swallow=True):
            print(f"  registered {catalog}.{silver_schema}.{table} ✓")
            registered_count += 1
        else:
            print(f"  WARN: register_table failed for {table}")

    print(
        f"\n[OK] Phase B complete — "
        f"{registered_count} registered, {skipped_count} already existed."
    )
    return 0


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> int:
    if load_dotenv is not None:
        load_dotenv(ROOT / ".env")

    parser = argparse.ArgumentParser(
        description="Create watsonx.data schemas and register Flink Iceberg tables."
    )
    parser.add_argument(
        "--phase",
        choices=["schema", "register", "all"],
        default="all",
        help=(
            "schema  — Phase A: create schemas in watsonx.data (run before Flink). "
            "register — Phase B: register Flink-written tables (run after Flink). "
            "all     — run both phases in sequence (default)."
        ),
    )
    args = parser.parse_args()

    if args.phase in ("schema", "all"):
        rc = phase_schema()
        if rc != 0:
            return rc

    if args.phase in ("register", "all"):
        rc = phase_register()
        if rc != 0:
            return rc

    return 0


if __name__ == "__main__":
    sys.exit(main())
