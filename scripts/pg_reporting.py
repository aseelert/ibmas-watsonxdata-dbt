#!/usr/bin/env python3
# -----------------------------------------------------------------------------
#  pg_reporting.py — create the ibmas_reporting schema in PostgreSQL and run
#                    curated gold-layer reports directly against it.
#
#  Location  : scripts/pg_reporting.py
#  Repository: https://github.com/aseelert/ibmas-watsonxdata-dbt
#  Project   : watsonx.data · dbt · Spark · Confluent medallion demo
#  Author    : Alexander Seelert — IBM Customer Success Engineer
#  Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.
#
#  Changelog :
#    v1.0 (2026-06-29) — Initial version. Creates ibmas_reporting schema in the
#      ibmas-postgres connection (postgresql.cpd-instance.svc.cluster.local),
#      mirrors the dbt_demo_gold views as materialised reporting tables, and
#      provides a query sub-command for ad-hoc SQL.
# -----------------------------------------------------------------------------
"""Manage the ibmas_reporting schema in the ibmas-postgres PostgreSQL instance.

WHY THIS SCRIPT EXISTS
  The watsonx.data Intelligence MCP tools generate_reporting_sql_query and
  execute_reporting_select_query target the CPD-internal IKC reporting service
  (IKCBI2008E).  For this demo environment that service is not enabled, so
  those MCP tools return 404.  This script provides the same capability by
  connecting directly to the PostgreSQL instance that backs the ibmas-postgres
  connection (DSD: IBMAS-Postgres-DSD, host: postgresql.cpd-instance.svc.
  cluster.local:5432) using the standard psycopg2 driver.

  It maintains a dedicated ``ibmas_reporting`` schema that holds reporting-
  friendly copies of the dbt gold marts (gold_customer_360, gold_daily_sales,
  gold_category_performance).  Each refresh TRUNCATEs and re-inserts from the
  source gold schema so the reporting tables stay in sync without requiring DDL
  changes.

SUB-COMMANDS
  init      Create the ibmas_reporting schema and all reporting tables (idempotent).
  refresh   Re-populate reporting tables from the gold source schema.
  query     Execute an arbitrary SELECT against ibmas_reporting and print results.
  list      List all tables in ibmas_reporting.

ENVIRONMENT VARIABLES  (all have defaults; only PG_PASSWORD is required)
  PG_HOST              PostgreSQL host   (default: postgresql.cpd-instance.svc.cluster.local)
  PG_PORT              PostgreSQL port   (default: 5432)
  PG_DATABASE          Database name     (default: postgres)
  PG_USER              DB user           (default: cpadmin)
  PG_PASSWORD          DB password       (required — set from CPD postgres secret)
  PG_GOLD_SCHEMA       Source gold schema (default: dbt_demo_gold)
  PG_REPORTING_SCHEMA  Target schema     (default: ibmas_reporting)
  PG_SSL_MODE          SSL mode          (default: require; set to disable for in-cluster)

PREREQUISITES
  pip install psycopg2-binary>=2.9   (included in requirements.txt)
  Network access to PG_HOST:PG_PORT from wherever this script runs.
  For in-cluster execution the default host resolves automatically.
  From a workstation use an oc port-forward:
    oc -n cpd-instance port-forward svc/postgresql 15432:5432
  then set PG_HOST=localhost PG_PORT=15432 PG_SSL_MODE=disable.

USAGE
  python scripts/pg_reporting.py init
  python scripts/pg_reporting.py refresh
  python scripts/pg_reporting.py query "SELECT * FROM gold_reporting_customer_360 LIMIT 5"
  python scripts/pg_reporting.py list
"""

from __future__ import annotations

import argparse
import os
import sys
from decimal import Decimal
from typing import Any

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


# ---------------------------------------------------------------------------
# Configuration helpers
# ---------------------------------------------------------------------------

def _env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None or value.strip() == "":
        raise SystemExit(f"Missing required environment variable: {name}")
    return value.strip()


def _pg_dsn() -> dict[str, Any]:
    """Build psycopg2 connection kwargs from PG_* env vars."""
    return {
        "host":     _env("PG_HOST",     "postgresql.cpd-instance.svc.cluster.local"),
        "port":     int(_env("PG_PORT", "5432")),
        "dbname":   _env("PG_DATABASE", "postgres"),
        "user":     _env("PG_USER",     "cpadmin"),
        "password": _env("PG_PASSWORD"),
        "sslmode":  _env("PG_SSL_MODE", "require"),
        "connect_timeout": 15,
    }


def _connect():
    try:
        import psycopg2
    except ImportError as exc:
        raise SystemExit(
            "Missing dependency 'psycopg2-binary'. "
            "Install with: pip install psycopg2-binary"
        ) from exc
    dsn = _pg_dsn()
    host = dsn["host"]
    port = dsn["port"]
    dbname = dsn["dbname"]
    print(f"Connecting to PostgreSQL {host}:{port}/{dbname} ...")
    conn = psycopg2.connect(**dsn)
    conn.autocommit = False
    print("[OK] Connected.")
    return conn


# ---------------------------------------------------------------------------
# DDL — reporting schema + tables
# ---------------------------------------------------------------------------

_INIT_SQL_TEMPLATE = """
-- Create the reporting schema (idempotent)
CREATE SCHEMA IF NOT EXISTS {reporting};

-- -----------------------------------------------------------------------
-- gold_reporting_customer_360
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS {reporting}.gold_reporting_customer_360 (
    customer_id               TEXT,
    first_name                TEXT,
    last_name                 TEXT,
    email                     TEXT,
    country                   TEXT,
    signup_date               DATE,
    completed_orders          BIGINT,
    returned_orders           BIGINT,
    pending_orders            BIGINT,
    cancelled_orders          BIGINT,
    lifetime_value            NUMERIC(18, 2),
    last_completed_order_ts   TIMESTAMP,
    last_activity_ts          TIMESTAMP,
    _loaded_at                TIMESTAMP NOT NULL DEFAULT now()
);

-- -----------------------------------------------------------------------
-- gold_reporting_daily_sales
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS {reporting}.gold_reporting_daily_sales (
    order_date    DATE,
    category      TEXT,
    order_count   BIGINT,
    units_sold    BIGINT,
    net_revenue   NUMERIC(18, 2),
    _loaded_at    TIMESTAMP NOT NULL DEFAULT now()
);

-- -----------------------------------------------------------------------
-- gold_reporting_category_performance
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS {reporting}.gold_reporting_category_performance (
    category              TEXT,
    total_orders          BIGINT,
    total_units_sold      BIGINT,
    total_net_revenue     NUMERIC(18, 2),
    avg_order_value       NUMERIC(18, 2),
    _loaded_at            TIMESTAMP NOT NULL DEFAULT now()
);
"""

# Refresh queries: TRUNCATE + INSERT … SELECT from dbt gold schema.
_REFRESH_SQL_TEMPLATE = """
TRUNCATE {reporting}.gold_reporting_customer_360;
INSERT INTO {reporting}.gold_reporting_customer_360 (
    customer_id, first_name, last_name, email, country, signup_date,
    completed_orders, returned_orders, pending_orders, cancelled_orders,
    lifetime_value, last_completed_order_ts, last_activity_ts
)
SELECT
    customer_id::TEXT,
    first_name::TEXT,
    last_name::TEXT,
    email::TEXT,
    country::TEXT,
    signup_date::DATE,
    completed_orders::BIGINT,
    returned_orders::BIGINT,
    pending_orders::BIGINT,
    cancelled_orders::BIGINT,
    lifetime_value::NUMERIC(18,2),
    last_completed_order_ts::TIMESTAMP,
    last_activity_ts::TIMESTAMP
FROM {gold}.gold_customer_360;

TRUNCATE {reporting}.gold_reporting_daily_sales;
INSERT INTO {reporting}.gold_reporting_daily_sales (
    order_date, category, order_count, units_sold, net_revenue
)
SELECT
    order_date::DATE,
    category::TEXT,
    order_count::BIGINT,
    units_sold::BIGINT,
    net_revenue::NUMERIC(18,2)
FROM {gold}.gold_daily_sales;

TRUNCATE {reporting}.gold_reporting_category_performance;
INSERT INTO {reporting}.gold_reporting_category_performance (
    category, total_orders, total_units_sold, total_net_revenue, avg_order_value
)
SELECT
    category::TEXT,
    total_orders::BIGINT,
    total_units_sold::BIGINT,
    total_net_revenue::NUMERIC(18,2),
    avg_order_value::NUMERIC(18,2)
FROM {gold}.gold_category_performance;
"""


# ---------------------------------------------------------------------------
# Formatting helpers  (shared with query_gold.py style)
# ---------------------------------------------------------------------------

def _format_value(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, Decimal | float):
        return f"{value:,.2f}"
    if isinstance(value, int):
        return f"{value:,}"
    return str(value)


def _is_numeric(rows: list[tuple], col: int) -> bool:
    numeric = (Decimal, float, int)
    vals = [r[col] for r in rows if r[col] is not None]
    return bool(vals) and all(isinstance(v, numeric) for v in vals)


def _print_table(columns: list[str], rows: list[tuple]) -> None:
    if not rows:
        print("(no rows)")
        return
    fmt = [[_format_value(v) for v in row] for row in rows]
    widths = [max(len(columns[i]), *(len(r[i]) for r in fmt)) for i in range(len(columns))]
    numeric_cols = {i for i in range(len(columns)) if _is_numeric(rows, i)}

    def cell(val: str, i: int) -> str:
        return val.rjust(widths[i]) if i in numeric_cols else val.ljust(widths[i])

    sep = "+-" + "-+-".join("-" * w for w in widths) + "-+"
    print(sep)
    print("| " + " | ".join(columns[i].upper().ljust(widths[i]) for i in range(len(columns))) + " |")
    print(sep)
    for row in fmt:
        print("| " + " | ".join(cell(row[i], i) for i in range(len(columns))) + " |")
    print(sep)
    print(f"{len(rows)} row{'s' if len(rows) != 1 else ''}")


# ---------------------------------------------------------------------------
# Sub-command implementations
# ---------------------------------------------------------------------------

def cmd_init(reporting: str) -> None:
    """Create the ibmas_reporting schema and reporting tables (idempotent)."""
    conn = _connect()
    sql = _INIT_SQL_TEMPLATE.format(reporting=reporting)
    try:
        with conn.cursor() as cur:
            print(f"Creating schema {reporting!r} and reporting tables ...")
            cur.execute(sql)
        conn.commit()
        print(f"[OK] Schema '{reporting}' initialised.")
    finally:
        conn.close()


def cmd_refresh(reporting: str, gold: str) -> None:
    """Truncate + re-insert all reporting tables from the gold source schema."""
    conn = _connect()
    sql = _REFRESH_SQL_TEMPLATE.format(reporting=reporting, gold=gold)
    try:
        with conn.cursor() as cur:
            print(f"Refreshing {reporting!r} from source schema {gold!r} ...")
            cur.execute(sql)
        conn.commit()
        print(f"[OK] Reporting tables refreshed.")
    finally:
        conn.close()


def cmd_query(sql: str, reporting: str) -> None:
    """Execute a SELECT inside ibmas_reporting and print the result table."""
    conn = _connect()
    try:
        with conn.cursor() as cur:
            # Set search_path so bare table names resolve to ibmas_reporting.
            cur.execute(f"SET search_path TO {reporting}, public")
            print(f"SQL> {sql.strip()}")
            cur.execute(sql)
            if cur.description is None:
                print("(statement returned no result set)")
                return
            columns = [d[0] for d in cur.description]
            rows = cur.fetchall()
        _print_table(columns, rows)
    finally:
        conn.close()


def cmd_list(reporting: str) -> None:
    """List all tables in the reporting schema."""
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT table_name, pg_size_pretty(pg_total_relation_size("
                "    quote_ident(table_schema)||'.'||quote_ident(table_name))) AS size "
                "FROM information_schema.tables "
                "WHERE table_schema = %s "
                "ORDER BY table_name",
                (reporting,),
            )
            rows = cur.fetchall()
        if not rows:
            print(f"Schema '{reporting}' exists but contains no tables.")
            return
        print(f"\nTables in schema '{reporting}':")
        for name, size in rows:
            print(f"  {name:<45}  {size}")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> int:
    reporting = _env("PG_REPORTING_SCHEMA", "ibmas_reporting")
    gold      = _env("PG_GOLD_SCHEMA",      "dbt_demo_gold")

    parser = argparse.ArgumentParser(
        description="Manage the ibmas_reporting schema in the ibmas-postgres PostgreSQL instance.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python scripts/pg_reporting.py init\n"
            "  python scripts/pg_reporting.py refresh\n"
            "  python scripts/pg_reporting.py list\n"
            "  python scripts/pg_reporting.py query "
            "\"SELECT * FROM gold_reporting_customer_360 LIMIT 5\"\n"
        ),
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init",    help="Create schema and tables (idempotent).")
    sub.add_parser("refresh", help="Re-populate reporting tables from the gold schema.")
    sub.add_parser("list",    help="List tables in the reporting schema.")
    query_p = sub.add_parser("query",   help="Run a SELECT against the reporting schema.")
    query_p.add_argument("sql", help="SQL SELECT statement to execute.")

    args = parser.parse_args()

    if args.command == "init":
        cmd_init(reporting)
    elif args.command == "refresh":
        cmd_refresh(reporting, gold)
    elif args.command == "list":
        cmd_list(reporting)
    elif args.command == "query":
        cmd_query(args.sql, reporting)

    return 0


if __name__ == "__main__":
    sys.exit(main())
