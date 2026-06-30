#!/usr/bin/env python3
# -----------------------------------------------------------------------------
#  demo_time_travel.py — show Iceberg snapshot history / time travel on Presto
#
#  Location  : scripts/demo_time_travel.py
#  Repository: https://github.com/aseelert/ibmas-watsonxdata-dbt
#  Project   : watsonx.data · dbt · Spark · Confluent medallion demo
#  Author    : Alexander Seelert — IBM Customer Success Engineer
#  Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.
#
#  Changelog :
#    v1.0 (2026-06-26) — Initial version. Demonstrates Iceberg snapshot history
#      / time travel on watsonx.data via Presto.
# -----------------------------------------------------------------------------
"""Demo: Iceberg time travel on watsonx.data via Presto.

Shows that Iceberg keeps every historical snapshot so you can query the table
as it looked at any past point in time — without restoring a backup.

WHAT / WHY
  This is the "wow" / governance-story script of the medallion demo. It points
  at the silver_orders Iceberg table and walks an audience through Iceberg's
  snapshot model: each commit (every dbt run/seed/INSERT) produces a new,
  immutable snapshot, and Presto can read any past snapshot with
  ``FOR VERSION AS OF <snapshot_id>``. It also surfaces the Iceberg metadata
  tables ($snapshots, $history, $partitions) so viewers can see the audit
  trail and physical file layout that back the marts.

STEPS (printed live, each under a divider)
  1. Capture the current snapshot ID (our "undo point") plus a few priors.
  2. Show the current row count and status distribution.
  3. Time travel — count rows ``FOR VERSION AS OF`` the captured snapshot and
     compare to "now" (identical counts hint to re-run dbt for divergence).
  4. Inspect the full snapshot history ($history table).
  5. Inspect the partition layout ($partitions table).
  The original narrative also frames running ``dbt seed --full-refresh`` /
  ``dbt run`` between executions to create a NEW snapshot, then re-running this
  script to watch the history diverge and prove the old data is still readable.

WHEN TO RUN (demo flow)
  After the silver layer exists (the silver_orders table must be materialised by
  dbt). Best shown after at least one dbt build; re-run it after a fresh dbt
  build to demonstrate snapshot divergence.

ENV VARS (read at runtime; .env is auto-loaded if python-dotenv is installed)
  - WXD_USER          (default ``ibmlhapikey_cpadmin``) — Presto principal.
  - WXD_API_KEY       (required) — IBM Cloud / CPD apikey used as the password.
  - WXD_HOST          (required) — Presto host name.
  - WXD_PORT          (default ``443``) — Presto HTTPS port.
  - WXD_CATALOG       (default ``iceberg_data``) — Iceberg catalog.
  - WXD_SCHEMA        (default ``dbt_demo``) — base schema name.
  - WXD_SILVER_SCHEMA (default ``<WXD_SCHEMA>_silver``) — schema holding the
    silver_orders table that is time-travelled.
  - WXD_SSL_VERIFY    (default ``certs/watsonxdata-ca.pem``) — True/False or a CA
    bundle path (relative paths resolve against the repo root).
  - WXD_INSTANCE_ID   (optional) — sent as the ``LhInstanceId`` HTTP header.

PREREQUISITES
  - ``presto-python-client`` installed (``pip install -r requirements.txt``).
  - A reachable, resumed watsonx.data Presto engine; populated silver schema.

USAGE
  - python scripts/demo_time_travel.py

SIDE EFFECTS / EXIT
  - Read-only: issues only SELECTs against the table and its Iceberg metadata
    tables; it does NOT itself create snapshots. Prints the connection
    breadcrumb plus each step's output to stdout. Returns 0 on success; raises
    SystemExit on missing env vars or a missing dependency. Individual time
    travel / partition queries are wrapped in try/except so one failure does not
    abort the whole walkthrough.
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

    host = _env("WXD_HOST")
    port = int(_env("WXD_PORT", "443"))
    print(f"Connecting to Presto {host}:{port} (catalog={catalog}, schema={schema}) ...")
    conn = prestodb.dbapi.connect(
        host=host,
        port=port,
        user=user,
        catalog=catalog,
        schema=schema,
        http_scheme="https",
        http_headers=_http_headers(),
        auth=prestodb.auth.BasicAuthentication(user, password),
        # Per-request socket timeout so a suspended/resuming engine can't hang.
        request_timeout=60,
    )
    conn._http_session.verify = _ssl_verify()
    print(f"[OK] Connected. Time-travelling table: {catalog}.{schema}.{TABLE}")
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
    # Top-level safety net: known errors raise SystemExit with a clear message and
    # are passed through; anything unexpected is logged with context and exits 1.
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n[ERROR] interrupted by user", file=sys.stderr)
        sys.exit(130)
    except SystemExit:
        raise
    except Exception as exc:  # noqa: BLE001 — log the unexpected failure, then exit non-zero
        import traceback
        print(f"[ERROR] unexpected failure: {exc}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)
