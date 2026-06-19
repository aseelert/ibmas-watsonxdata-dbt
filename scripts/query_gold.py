#!/usr/bin/env python3
"""Run a small customer-facing query against the dbt gold layer."""

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
    print("Connected.")

    cur = conn.cursor()
    if args.report == "all":
        for name, sql in REPORTS.items():
            _print_report(cur, name, sql)
    else:
        _print_report(cur, args.report, REPORTS[args.report])

    return 0


if __name__ == "__main__":
    sys.exit(main())
