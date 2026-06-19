#!/usr/bin/env python3
# -----------------------------------------------------------------------------
#  bootstrap_watsonxdata.py — create the medallion demo schemas in watsonx.data
#
#  Location  : scripts/bootstrap_watsonxdata.py
#  Repository: https://github.ibm.com/alexander/ibmas-watsonxdata-dbt
#  Project   : watsonx.data · dbt · Spark medallion demo
#  Author    : Alexander Seelert
#  Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.
# -----------------------------------------------------------------------------
"""Create watsonx.data demo schemas before running dbt.

This is the very first runtime step of the medallion demo: it opens a Presto
(Iceberg) connection to the watsonx.data engine and ensures the four dbt layer
schemas exist — ``<WXD_SCHEMA>_raw``, ``_bronze``, ``_silver`` and ``_gold``
(default base ``dbt_demo``) — so that the subsequent ingest + ``dbt run`` steps
have somewhere to land their tables. Without these schemas dbt's first model
materialisation would fail, which is why this script exists and must run before
any ingest/dbt activity in the demo flow.

WHEN to run it
  Run this once after the watsonx.data engine is up and the Iceberg catalog
  (``WXD_CATALOG``, default ``iceberg_data``) is registered, and before the
  cpdctl/Spark ingest and ``dbt run``. It is idempotent: a plain run only does
  ``create schema if not exists`` per layer, so re-running is harmless.

ENV VARS read
  - WXD_USER ................ Presto user (default ``ibmlhapikey_cpadmin``)
  - WXD_API_KEY ............. IBM Cloud / CPD API key used as the password (required)
  - WXD_HOST ................ Presto host (required)
  - WXD_PORT ................ Presto port (default ``443``)
  - WXD_CATALOG ............. Iceberg catalog name (default ``iceberg_data``)
  - WXD_SCHEMA .............. base schema name (default ``dbt_demo``)
  - WXD_RAW_SCHEMA / WXD_BRONZE_SCHEMA / WXD_SILVER_SCHEMA / WXD_GOLD_SCHEMA
                             override individual layer schema names
  - WXD_SCHEMA_LOCATION_BASE  if set, each schema is created WITH (location =
                             '<base>/<schema>') so its data lands at a known
                             object-storage prefix (uniform bucket-root layout)
  - WXD_INSTANCE_ID ......... if set, sent as the ``LhInstanceId`` HTTP header
  - WXD_SSL_VERIFY .......... CA bundle path, or ``true``/``false`` to toggle
                             TLS verification (default ``certs/watsonxdata-ca.pem``)

Prerequisites
  - A running, resumed watsonx.data Presto engine reachable at WXD_HOST:WXD_PORT.
  - ``presto-python-client`` installed (``pip install -r requirements.txt``).
  - A valid WXD_API_KEY. No ``oc login`` / ``cpdctl`` is required for this step.

USAGE
    python scripts/bootstrap_watsonxdata.py
    python scripts/bootstrap_watsonxdata.py --recreate

  ``--recreate`` DROPs each schema (after dropping every table/view in it, since
  Presto has no DROP SCHEMA CASCADE) and re-creates it. Use this only when you
  changed WXD_SCHEMA_LOCATION_BASE and need to relocate schemas — a plain
  ``create schema if not exists`` is a no-op for an existing schema and will NOT
  move its data. DESTRUCTIVE: it drops all tables in the affected schemas.

Side effects + exit behavior
  Creates (and with ``--recreate`` first drops) catalog schemas in watsonx.data;
  prints each SQL statement and an ``ensured`` breadcrumb per schema. Returns
  exit code 0 on success; raises ``SystemExit`` with a message on missing env
  vars / missing dependency, and propagates Presto errors otherwise.
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


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument(
        "--recreate",
        action="store_true",
        help="DROP each schema (CASCADE) before creating it. Use this to relocate "
             "schemas after changing WXD_SCHEMA_LOCATION_BASE — a plain "
             "'create schema if not exists' is a no-op for an existing schema and "
             "will NOT move its data. DESTRUCTIVE: drops all tables in the schema.",
    )
    args = parser.parse_args()

    if load_dotenv is not None:
        load_dotenv()

    try:
        import prestodb
    except ImportError as exc:
        raise SystemExit(
            "Missing dependency 'presto-python-client'. Install dependencies with: python -m pip install -r requirements.txt"
        ) from exc

    user = _env("WXD_USER", "ibmlhapikey_cpadmin")
    password = _env("WXD_API_KEY")
    host = _env("WXD_HOST")
    port = int(_env("WXD_PORT", "443"))
    catalog = _env("WXD_CATALOG", "iceberg_data")
    base_schema = _env("WXD_SCHEMA", "dbt_demo")
    schemas = [
        os.getenv("WXD_RAW_SCHEMA", f"{base_schema}_raw"),
        os.getenv("WXD_BRONZE_SCHEMA", f"{base_schema}_bronze"),
        os.getenv("WXD_SILVER_SCHEMA", f"{base_schema}_silver"),
        os.getenv("WXD_GOLD_SCHEMA", f"{base_schema}_gold"),
    ]
    location_base = os.getenv("WXD_SCHEMA_LOCATION_BASE", "").rstrip("/")

    if location_base:
        print(f"Using schema location base: {location_base}")

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

    def _execute(sql: str, *, swallow: bool = False) -> bool:
        print(f"SQL> {sql}")
        try:
            cur.execute(sql)
            cur.fetchall()
            return True
        except prestodb.exceptions.HttpError as exc:
            if "AMS_CANNOT_GET_TOKEN" not in str(exc):
                if swallow:
                    print(f"  (ignored) {exc}")
                    return False
                raise
            print("  AMS token not ready (AMS_CANNOT_GET_TOKEN); waiting 2s then retrying once...")
            time.sleep(2)
            cur.execute(sql)
            cur.fetchall()
            return True
        except prestodb.exceptions.PrestoUserError as exc:
            if swallow:
                print(f"  (ignored) {exc.message}")
                return False
            raise

    def _drop_schema(schema: str) -> None:
        """Presto has no DROP SCHEMA CASCADE — drop each table/view first."""
        try:
            cur.execute(f"show tables in {catalog}.{schema}")
            objects = [row[0] for row in cur.fetchall()]
        except prestodb.exceptions.PrestoUserError:
            objects = []  # schema does not exist yet
        for obj in objects:
            fq = f"{catalog}.{schema}.{obj}"
            # An object is either a table or a view; try table first, fall back.
            if not _execute(f"drop table if exists {fq}", swallow=True):
                _execute(f"drop view if exists {fq}", swallow=True)
        _execute(f"drop schema if exists {catalog}.{schema}", swallow=True)

    cur = conn.cursor()
    if args.recreate and not location_base:
        print("  WARNING: --recreate without WXD_SCHEMA_LOCATION_BASE set — schemas "
              "will be recreated at the default warehouse location.")
    for schema in schemas:
        if args.recreate:
            _drop_schema(schema)
        sql = f"create schema if not exists {catalog}.{schema}"
        if location_base:
            sql = f"{sql} with (location = '{location_base}/{schema}')"
        _execute(sql)
        print(f"ensured {catalog}.{schema}")

    print(f"[OK] bootstrap complete — {len(schemas)} schema(s) ready in {catalog}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
