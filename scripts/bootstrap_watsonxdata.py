#!/usr/bin/env python3
"""Create watsonx.data demo schemas before running dbt."""

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

    return 0


if __name__ == "__main__":
    sys.exit(main())
