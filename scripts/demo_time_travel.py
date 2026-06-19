#!/usr/bin/env python3
"""Demo: Iceberg time travel on watsonx.data via Presto.

Shows that Iceberg keeps every historical snapshot so you can query the table
as it looked at any past point in time — without restoring a backup.

Steps:
  1. Capture the current snapshot ID (our "undo point")
  2. Show the current row count and status distribution
  3. Simulate a data change by running dbt seed --full-refresh (creates a new snapshot)
  4. Show that a re-run of dbt alters the snapshot history
  5. Query the table FOR VERSION AS OF <old snapshot> — old data is still there
  6. Return to current state

Run: python scripts/demo_time_travel.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

CATALOG = "iceberg_data"
TABLE = "silver_orders"
DIVIDER = "-" * 60


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
    return {"LhInstanceId": instance_id} if instance_id else None


def main() -> int:
    if load_dotenv is not None:
        load_dotenv()

    try:
        import prestodb
    except ImportError as exc:
        raise SystemExit("Missing dependency 'presto-python-client'.") from exc

    user = _env("WXD_USER", "ibmlhapikey_cpadmin")
    password = _env("WXD_API_KEY")
    catalog = _env("WXD_CATALOG", CATALOG)
    base = _env("WXD_SCHEMA", "dbt_demo")
    schema = os.getenv("WXD_SILVER_SCHEMA", f"{base}_silver")

    conn = prestodb.dbapi.connect(
        host=_env("WXD_HOST"),
        port=int(_env("WXD_PORT", "443")),
        user=user,
        catalog=catalog,
        schema=schema,
        http_scheme="https",
        http_headers=_http_headers(),
        auth=prestodb.auth.BasicAuthentication(user, password),
    )
    conn._http_session.verify = _ssl_verify()
    cur = conn.cursor()

    def run(sql: str) -> list:
        cur.execute(sql)
        return cur.fetchall()

    print(DIVIDER)
    print("Step 1: Capture the current snapshot ID (our undo point)")
    print(DIVIDER)
    rows = run(
        f'SELECT snapshot_id, committed_at, operation '
        f'FROM "{TABLE}$snapshots" ORDER BY committed_at DESC LIMIT 3'
    )
    snapshot_id = rows[0][0]
    committed_at = rows[0][1]
    print(f"  Latest snapshot : {snapshot_id}")
    print(f"  Committed at    : {committed_at}")
    print(f"  Operation       : {rows[0][2]}")
    if len(rows) > 1:
        print(f"\n  Prior snapshots (showing time travel is possible to these too):")
        for r in rows[1:]:
            print(f"    snapshot {r[0]}  committed {r[1]}  op={r[2]}")
    print()

    print(DIVIDER)
    print("Step 2: Current state of the table")
    print(DIVIDER)
    total = run(f"SELECT COUNT(*) FROM {TABLE}")[0][0]
    print(f"  Total rows: {total:,}")
    print(f"\n  Status breakdown:")
    for status, count in run(f"SELECT status, COUNT(*) FROM {TABLE} GROUP BY status ORDER BY 2 DESC"):
        bar = "#" * (count // 5)
        print(f"    {status:<12} {count:>6,}  {bar}")
    print()

    print(DIVIDER)
    print("Step 3: Time travel — query the previous snapshot")
    print(DIVIDER)
    print(f"  Querying: SELECT COUNT(*) FROM {TABLE} FOR VERSION AS OF {snapshot_id}")
    try:
        old_total = run(f"SELECT COUNT(*) FROM {TABLE} FOR VERSION AS OF {snapshot_id}")[0][0]
        print(f"  Row count at snapshot {snapshot_id}: {old_total:,}")
        print(f"  Row count now                        : {total:,}")
        if old_total == total:
            print("  (Counts match — no data changes between snapshots in this session.)")
            print("  Tip: run dbt run then re-run this script to see snapshot divergence.")
    except Exception as exc:
        print(f"  Time travel query failed: {exc}")
    print()

    print(DIVIDER)
    print("Step 4: Inspect full snapshot history")
    print(DIVIDER)
    history = run(
        f'SELECT made_current_at, snapshot_id, is_current_ancestor '
        f'FROM "{TABLE}$history" ORDER BY made_current_at DESC LIMIT 5'
    )
    print(f"  {'made_current_at':<32} {'snapshot_id':<22} is_current_ancestor")
    for row in history:
        print(f"  {str(row[0]):<32} {str(row[1]):<22} {row[2]}")
    print()

    print(DIVIDER)
    print("Step 5: Inspect partition layout")
    print(DIVIDER)
    try:
        partitions = run(
            f'SELECT order_date_month, row_count, file_count '
            f'FROM "{TABLE}$partitions" ORDER BY order_date_month'
        )
        def _month_label(m: int) -> str:
            return f"{1970 + m // 12}-{(m % 12) + 1:02d}"

        print(f"  {'month':<12} {'records':>10} {'files':>8}")
        for p, recs, files in partitions[:8]:
            print(f"  {_month_label(p):<12} {recs:>10,} {files:>8,}")
        if len(partitions) > 8:
            print(f"  ... and {len(partitions) - 8} more partitions")
    except Exception as exc:
        print(f"  Partition query failed: {exc}")
    print()

    print("Done. Key takeaway: Iceberg never deletes old snapshots automatically —")
    print("every dbt run, seed, or INSERT creates a new snapshot you can query back to.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
