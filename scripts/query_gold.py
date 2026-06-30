#!/usr/bin/env python3
# -----------------------------------------------------------------------------
#  query_gold.py — query the dbt gold marts in watsonx.data and pretty-print them
#
#  Location  : scripts/query_gold.py
#  Repository: https://github.com/aseelert/ibmas-watsonxdata-dbt
#  Project   : watsonx.data · dbt · Spark medallion demo
#  Author    : Alexander Seelert
#  Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.
# -----------------------------------------------------------------------------
"""Run a small customer-facing query against the dbt gold layer.

This is the "show me the business value" script at the very end of the medallion
demo. After the bronze -> silver -> gold dbt models have materialised in the
Iceberg catalog, this tool connects to the watsonx.data Presto engine, runs one
or more curated read-only gold-layer reports, and renders the result as a clean
ASCII table (right-aligned, thousands-separated numbers for the metrics) that
reads well on a projector during a live demo.

WHAT / WHY
  - Two built-in reports live in the ``REPORTS`` dict:
      * ``daily_sales``  — gold_daily_sales: order_date / category / order_count
        / units_sold / net_revenue.
      * ``customer_360`` — gold_customer_360: per-customer lifetime value and
        order-state counts, ordered by lifetime value.
  - It exists so a presenter can prove, in one command, that the whole pipeline
    produced query-able, business-ready marts in watsonx.data.

WHEN TO RUN (demo flow)
  - Run LAST, after the dbt gold models exist (i.e. after a successful
    ``dbt run`` / the gold build step). The gold schema and its tables must
    already be populated, otherwise Presto will raise a "table not found" error.

ENV VARS (read at runtime; .env is auto-loaded if python-dotenv is installed)
  - WXD_USER         (default ``ibmlhapikey_cpadmin``) — Presto principal.
  - WXD_API_KEY      (required) — IBM Cloud / CPD apikey used as the password.
  - WXD_HOST         (required) — Presto host name.
  - WXD_PORT         (default ``443``) — Presto HTTPS port.
  - WXD_CATALOG      (default ``iceberg_data``) — Iceberg catalog.
  - WXD_SCHEMA       (default ``dbt_demo``) — base schema name.
  - WXD_GOLD_SCHEMA  (default ``<WXD_SCHEMA>_gold``) — gold mart schema queried.
  - WXD_SSL_VERIFY   (default ``certs/watsonxdata-ca.pem``) — True/False or a CA
    bundle path (relative paths resolve against the repo root).
  - WXD_INSTANCE_ID  (optional) — sent as the ``LhInstanceId`` HTTP header.

PREREQUISITES
  - ``presto-python-client`` installed (``pip install -r requirements.txt``).
  - A reachable, resumed watsonx.data Presto engine; the populated gold schema.

USAGE
  - python scripts/query_gold.py              # default: all reports
  - python scripts/query_gold.py daily_sales  # just the daily sales mart
  - python scripts/query_gold.py customer_360 # just the customer-360 mart

SIDE EFFECTS / EXIT
  - Read-only: issues only SELECTs, writes nothing. Prints the connection
    breadcrumb plus each report's table to stdout. Returns 0 on success; raises
    SystemExit on missing env vars or a missing dependency.
"""

from __future__ import annotations

import os
import sys
import argparse
from decimal import Decimal
from pathlib import Path
from typing import Any


try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None


REPORTS = {
    "daily_sales": """
        select order_date, category, order_count, units_sold, net_revenue
        from gold_daily_sales
        order by order_date, category
    """,
    "customer_360": """
        select
          customer_id,
          first_name,
          last_name,
          email,
          country,
          signup_date,
          completed_orders,
          returned_orders,
          pending_orders,
          cancelled_orders,
          lifetime_value,
          last_completed_order_ts,
          last_activity_ts
        from gold_customer_360
        order by lifetime_value desc, customer_id
    """,
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
        path = Path(__file__).resolve().parents[1] / path
    return str(path)


def _http_headers() -> dict[str, str] | None:
    instance_id = os.getenv("WXD_INSTANCE_ID", "").strip()
    if not instance_id:
        return None
    return {"LhInstanceId": instance_id}


def _format_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, Decimal):
        return f"{value:,.2f}"
    if isinstance(value, float):
        return f"{value:,.2f}"
    if isinstance(value, int):
        return f"{value:,}"
    return str(value)


def _looks_numeric(value: str) -> bool:
    try:
        Decimal(value.replace(",", ""))
    except Exception:
        return False
    return True


def _is_numeric_column(rows: list[tuple[Any, ...]], column_index: int) -> bool:
    numeric_types = (Decimal, float, int)
    values = [row[column_index] for row in rows if row[column_index] is not None]
    if not values:
        return False
    return all(
        isinstance(value, numeric_types)
        or (isinstance(value, str) and _looks_numeric(value))
        for value in values
    )


def _print_table(columns: list[str], rows: list[tuple[Any, ...]]) -> None:
    if not rows:
        print("No rows returned.")
        return

    formatted_rows = [
        [_format_value(value) for value in row]
        for row in rows
    ]
    widths = [
        max(len(columns[index]), *(len(row[index]) for row in formatted_rows))
        for index in range(len(columns))
    ]
    numeric_columns = {
        index for index in range(len(columns))
        if _is_numeric_column(rows, index)
    }

    def format_cell(value: str, index: int) -> str:
        if index in numeric_columns:
            return value.rjust(widths[index])
        return value.ljust(widths[index])

    border = "+-" + "-+-".join("-" * width for width in widths) + "-+"
    header = "| " + " | ".join(
        columns[index].upper().ljust(widths[index])
        for index in range(len(columns))
    ) + " |"

    print(border)
    print(header)
    print(border)
    for row in formatted_rows:
        print("| " + " | ".join(
            format_cell(row[index], index)
            for index in range(len(columns))
        ) + " |")
    print(border)
    print(f"{len(rows)} row{'s' if len(rows) != 1 else ''}")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Query dbt gold marts in watsonx.data."
    )
    parser.add_argument(
        "report",
        nargs="?",
        default="all",
        choices=["all", *REPORTS.keys()],
        help="Gold report to show. Default: all.",
    )
    return parser.parse_args()


def _print_report(cur: Any, name: str, sql: str) -> None:
    title = name.replace("_", " ").title()
    print(f"\n{title}")
    print("=" * len(title))
    cur.execute(sql)
    columns = [desc[0] for desc in cur.description]
    rows = [tuple(row) for row in cur.fetchall()]
    _print_table(columns, rows)


def main() -> int:
    args = _parse_args()

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
    gold_schema = os.getenv("WXD_GOLD_SCHEMA", f"{base_schema}_gold")

    print(f"Connecting to Presto {host}:{port} (catalog={catalog}, schema={gold_schema}) ...")
    conn = prestodb.dbapi.connect(
        host=host,
        port=port,
        user=user,
        catalog=catalog,
        schema=gold_schema,
        http_scheme="https",
        http_headers=_http_headers(),
        auth=prestodb.auth.BasicAuthentication(user, password),
        # Per-request socket timeout so a suspended/resuming engine can't hang.
        request_timeout=60,
    )
    conn._http_session.verify = _ssl_verify()
    print("[OK] Connected.")

    cur = conn.cursor()
    if args.report == "all":
        for name, sql in REPORTS.items():
            _print_report(cur, name, sql)
    else:
        _print_report(cur, args.report, REPORTS[args.report])

    return 0


if __name__ == "__main__":
    sys.exit(main())
