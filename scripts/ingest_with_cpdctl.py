#!/usr/bin/env python3
# -----------------------------------------------------------------------------
#  ingest_with_cpdctl.py — Load demo CSVs into watsonx.data via the native cpdctl ingestion service
#
#  Location  : scripts/ingest_with_cpdctl.py
#  Repository: https://github.com/aseelert/ibmas-watsonxdata-dbt
#  Project   : watsonx.data · dbt · Spark medallion demo
#  Author    : Alexander Seelert
#  Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.
# -----------------------------------------------------------------------------
"""Load the demo CSV files into watsonx.data with the native ingestion service.

This uses `cpdctl wx-data ingestion create`, which is the **supported, UI-visible**
way to load files. Unlike `dbt seed` (Presto) or the custom Spark application, each
ingestion job shows up in the watsonx.data console under
**Data manager -> Ingestion (history)** as `ingestion-<id>`.

It demonstrates the "CSV upload" path end to end: the CSVs already staged in object
storage (see scripts/upload_spark_assets.py) are ingested into Iceberg tables in a
dedicated schema.

Usage
-----
    python scripts/ingest_with_cpdctl.py                  # submit all jobs
    python scripts/ingest_with_cpdctl.py --wait           # submit, then poll to completion
    python scripts/ingest_with_cpdctl.py --status --batch <id>         # check a prior run
    python scripts/ingest_with_cpdctl.py --status --batch <id> --wait  # poll a prior run

Each submission prints its batch id; pass it back with --status to check all four
jobs at once (via `cpdctl wx-data ingestion get`). Job ids are deterministic
(`ingest-<table>-<batch>`), so no state file is needed.

Prerequisites
-------------
1. cpdctl installed and on PATH:   https://github.com/IBM/cpdctl/releases
2. A valid WXD_API_KEY in .env. You do NOT need to configure a cpdctl profile by
   hand: on every run this script re-syncs cpdctl's cached credentials from .env
   (profile WXD_CPDCTL_PROFILE, default "wxd-demo") and validates them against CPD
   *before* submitting any jobs. This is what prevents the intermittent
   "An error occurred while performing the 'authenticate' step: Unauthorized"
   failure — cpdctl caches credentials in ~/.cpdctl.config.json and never reads
   .env on its own, so a rotated key would otherwise leave a stale cache behind.

   If the .env key itself is rejected, refresh it first:

       python scripts/get_token.py --refresh-key

3. The demo CSVs already uploaded to object storage:

       python scripts/upload_spark_assets.py
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

# watsonx.data ingestion job states that mean "no longer running".
TERMINAL_STATES = {"finished", "completed", "success", "succeeded",
                   "failed", "error", "cancelled", "canceled"}
SUCCESS_STATES = {"finished", "completed", "success", "succeeded"}

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

try:
    import requests
    from requests.packages.urllib3.exceptions import InsecureRequestWarning
    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
except ImportError:
    requests = None


ROOT = Path(__file__).resolve().parents[1]

# Bound every external call so nothing can hang on this (slow) machine.
CPDCTL_TIMEOUT = 120        # quick config/get cpdctl subcommands
CPDCTL_CREATE_TIMEOUT = 300  # `ingestion create` submission (still bounded)
PRESTO_REQUEST_TIMEOUT = 60  # per-request socket timeout for Presto HTTP calls

# CSV file (under WXD_SPARK_INPUT_BASE) -> target table name in the ingest schema.
TABLES = {
    "raw_customers.csv": "customers",
    "raw_products.csv": "products",
    "raw_orders.csv": "orders",
    "raw_order_items.csv": "order_items",
}

# Explicit DDL schemas passed via --schema to avoid Spark CSV type inference.
# Without this the ingestion engine guesses types from a sample of the file,
# which causes two problems for order_items specifically:
#
#  1. discount_pct (values like 0.00 / 0.05 / 0.10) is inferred as DOUBLE. The
#     Iceberg writer accepts it, but the downstream Spark silver job expects
#     DECIMAL(5,2) and the implicit cast under EXACTLY_ONCE checkpointing
#     serialises slowly enough to hit the Spark task timeout on 1,134 rows —
#     the job enters an infinite restart loop and never finishes.
#
#  2. order_item_id / order_id / product_id are inferred as BIGINT on some
#     Spark versions. That makes the cpdctl Presto CREATE TABLE emit BIGINT
#     columns, which then conflict with the INT primary-key definition in the
#     Iceberg sink declared by Flink — breaking cross-engine parity queries.
#
# CHANGELOG 2026-07-03: cpdctl 1.8.233 removed the old --source-file-schema
# flag entirely (fails with "unknown flag: --source-file-schema" — this is why
# order_items ingestion stopped working: it's the only table that ever passed
# this flag, so the other three silently kept working while order_items always
# failed before even creating a table). The replacement is --schema, which:
#   - Uses snake_case keys (header_name/type/field_id), NOT the camelCase shown
#     in `cpdctl wx-data ingestion create --help` (headerName/fieldId) — those
#     are silently rejected as "unsupported fields" and default to null, which
#     then crashes the Spark job with "field name None should be a string"
#     (confirmed by testing both forms directly against a live ingestion job).
#   - Applies types POSITIONALLY, by array order matching the CSV's actual
#     column order — header_name is accepted in the payload but not actually
#     used to match columns by name, so array order must be exactly right.
# The schema string format is the cpdctl --schema JSON array:
#   [{"header_name": "<col>", "type": "<presto/iceberg type>", "field_id": <1-based int>}]
# Types must match seeds/raw_*.csv column headers exactly (case-sensitive) AND
# be listed in the same left-to-right order as the CSV's actual columns.
# None means "let the engine infer" (safe for the three small tables).
TABLE_SCHEMAS: dict[str, list[dict] | None] = {
    "raw_customers.csv":   None,   # 50 rows, all STRING/INT — inference is fine
    "raw_products.csv":    None,   # 20 rows, unit_price DOUBLE is acceptable here
    "raw_orders.csv":      None,   # 500 rows, all STRING/INT — inference is fine
    "raw_order_items.csv": [       # 1,134 rows — MUST pin types explicitly
        {"header_name": "order_item_id", "type": "integer",       "field_id": 1},
        {"header_name": "order_id",      "type": "integer",       "field_id": 2},
        {"header_name": "product_id",    "type": "integer",       "field_id": 3},
        {"header_name": "quantity",      "type": "integer",       "field_id": 4},
        # Pin as decimal(5,2) — matches the Flink silver cast and the dbt model.
        # Without this Spark infers DOUBLE, which triggers the slow-serialisation
        # restart loop described above.
        {"header_name": "discount_pct",  "type": "decimal(5,2)",  "field_id": 5},
    ],
}

# order_items is 22× larger than the next biggest table (orders = 500 rows).
# Give it its own generous timeout so the Spark task has time to commit the
# Iceberg snapshot without triggering a TaskManager heartbeat timeout.
TABLE_TIMEOUTS: dict[str, int] = {
    "raw_order_items.csv": 600,   # 10 min — covers slow Spark cold-start + commit
}

# How long to wait, after submitting each table's job, for it to leave the
# "starting" state before submitting the next one. cpdctl's `ingestion create`
# returns as soon as the job is ACCEPTED, not once the Spark application has
# actually launched — submitting all four back-to-back (no stagger) causes
# the Spark engine's timestamp-based application-id scheme to collide
# ("Target log directory already exists (.../eventlog_v2_app-<ts>-0000)"),
# which silently fails whichever job loses the race (order_items, being the
# slowest to schedule, lost it every time this was observed). Confirmed fix by
# reproducing the exact collision, then submitting sequentially with this
# stagger and re-running clean — all 4 tables landed correctly.
SUBMIT_STAGGER_TIMEOUT = 90
SUBMIT_STAGGER_POLL_INTERVAL = 3


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
    host = _env("WXD_HOST")
    port = int(_env("WXD_PORT", "443"))
    print(f"Connecting to Presto {host}:{port} (catalog={catalog}) ...")
    conn = prestodb.dbapi.connect(
        host=host,
        port=port,
        user=user,
        catalog=catalog,
        http_scheme="https",
        http_headers={"LhInstanceId": _env("WXD_INSTANCE_ID")},
        auth=prestodb.auth.BasicAuthentication(user, _env("WXD_API_KEY")),
        request_timeout=PRESTO_REQUEST_TIMEOUT,
    )
    conn._http_session.verify = _ssl_verify()
    print("Connected.")
    cur = conn.cursor()
    location_base = os.getenv("WXD_SCHEMA_LOCATION_BASE", "").rstrip("/")
    sql = f"create schema if not exists {catalog}.{schema}"
    if location_base:
        sql += f" with (location = '{location_base}/{schema}')"
    print(f"SQL> {sql}")
    cur.execute(sql)
    cur.fetchall()


def _cpdctl(*args: str) -> subprocess.CompletedProcess:
    """Run a cpdctl subcommand, capturing output (raises nothing here).

    Bounded by CPDCTL_TIMEOUT so a hung cpdctl can't stall the run; on timeout we
    synthesize a non-zero CompletedProcess so callers treat it as a failed attempt.
    """
    try:
        return subprocess.run(
            ["cpdctl", *args], text=True, capture_output=True, timeout=CPDCTL_TIMEOUT
        )
    except subprocess.TimeoutExpired:
        print(f"  !! cpdctl {' '.join(args)} timed out after {CPDCTL_TIMEOUT}s [FAIL]")
        return subprocess.CompletedProcess(
            ["cpdctl", *args],
            returncode=124,
            stdout="",
            stderr=f"cpdctl timed out after {CPDCTL_TIMEOUT}s",
        )


def _validate_api_key() -> None:
    """Confirm WXD_API_KEY authenticates against CPD before we touch cpdctl.

    Fails fast with actionable guidance instead of the opaque cpdctl
    "authenticate step: Unauthorized" error that surfaces mid-job otherwise.
    """
    if requests is None:
        print("  (requests not installed — skipping pre-flight API key check)")
        return
    auth_url = _env("WXD_CPD_AUTH_URL")
    username = _env("WXD_CPD_USERNAME", "cpadmin")
    api_key = _env("WXD_API_KEY")
    resp = requests.post(
        auth_url,
        json={"username": username, "api_key": api_key},
        verify=_ssl_verify(),
        timeout=30,
    )
    if resp.status_code == 200:
        print("  WXD_API_KEY valid against CPD  [OK]")
        return
    if resp.status_code == 401:
        raise SystemExit(
            "WXD_API_KEY was rejected by CPD (401). The key is expired or revoked.\n"
            "  Refresh it, then re-run this script:\n"
            "    python scripts/get_token.py --refresh-key"
        )
    raise SystemExit(f"CPD auth check failed ({resp.status_code}): {resp.text}")


def _sync_cpdctl_credentials() -> str:
    """Push the validated .env credentials into cpdctl's cached config.

    cpdctl reads ~/.cpdctl.config.json, never .env, so a rotated WXD_API_KEY
    leaves a stale cached key behind -> "authenticate step: Unauthorized".
    Re-applying the profile/user from .env on every run makes that impossible.
    Returns the profile name to pass as --profile.
    """
    profile = os.getenv("WXD_CPDCTL_PROFILE", "wxd-demo")
    user = os.getenv("WXD_CPDCTL_USER", f"{profile}_user")
    username = _env("WXD_CPD_USERNAME", "cpadmin")
    api_key = _env("WXD_API_KEY")
    url = f"https://{_env('WXD_CPD_HOST')}"

    print(f"  Syncing cpdctl profile '{profile}' (user '{user}') from .env...")
    steps = [
        ("config", "user", "set", user, "--username", username, "--apikey", api_key),
        ("config", "profile", "set", profile, "--url", url, "--user", user),
        ("config", "profile", "use", profile),
    ]
    for step in steps:
        result = _cpdctl(*step)
        if result.returncode != 0:
            # Mask the apikey if it appears in the echoed command.
            shown = " ".join(s if s != api_key else "***" for s in step)
            raise SystemExit(
                f"cpdctl {shown} failed (exit {result.returncode}):\n"
                f"  {result.stderr.strip() or result.stdout.strip()}"
            )
    print(f"  cpdctl profile '{profile}' ready  [OK]")
    return profile


def _ensure_cpdctl_auth() -> str:
    """Validate the .env key, then make cpdctl use it. Returns profile name."""
    print("Checking authentication...")
    _validate_api_key()
    return _sync_cpdctl_credentials()


def _job_status(job_id: str, instance_id: str, profile: str) -> str:
    """Return the current status string for one ingestion job (or 'unknown')."""
    result = _cpdctl(
        "wx-data", "ingestion", "get",
        "--instance-id", instance_id,
        "--job-id", job_id,
        "--profile", profile,
    )
    for line in result.stdout.splitlines():
        # cpdctl prints a "Status   <value>" row in its table output.
        if line.strip().startswith("Status") and not line.strip().startswith('"'):
            parts = line.split()
            if len(parts) >= 2:
                return parts[1].lower()
    return "unknown"


def _report_status(job_ids: list[str], instance_id: str, profile: str,
                   wait: bool, interval: int, timeout: int) -> int:
    """Print a status line per job. If wait, poll until all are terminal or timeout.

    Returns 0 only if every job reached a successful terminal state.
    """
    deadline = time.time() + timeout
    while True:
        statuses = {j: _job_status(j, instance_id, profile) for j in job_ids}
        print("\nIngestion status:")
        for jid, st in statuses.items():
            print(f"  {st:<12} {jid}")
        pending = [j for j, st in statuses.items() if st not in TERMINAL_STATES]
        if not wait or not pending:
            break
        if time.time() >= deadline:
            print(f"\n  Timed out after {timeout}s with {len(pending)} job(s) still running.")
            break
        time.sleep(interval)

    failed = [j for j, st in statuses.items() if st not in SUCCESS_STATES]
    if failed:
        print(f"\n  {len(failed)} job(s) not successful: {', '.join(failed)}")
        return 1
    print(f"\n  All {len(job_ids)} job(s) finished successfully.")
    return 0


def _ui_url() -> str:
    base = os.getenv("WXD_CONSOLE_URL")
    if not base:
        base = f"https://{_env('WXD_CPD_HOST')}/watsonx-data/#"
    return f"{base.rstrip('/')}/data-manager?instanceId={_env('WXD_INSTANCE_ID')}"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument(
        "--status", action="store_true",
        help="Don't submit; just report the status of the batch's jobs and exit.",
    )
    parser.add_argument(
        "--wait", action="store_true",
        help="After submitting (or with --status), poll until all jobs finish.",
    )
    parser.add_argument(
        "--batch",
        help="Batch id to check (default: WXD_INGEST_BATCH_ID or the current timestamp). "
             "Required with --status to target a previous run.",
    )
    parser.add_argument(
        "--interval", type=int, default=20,
        help="Seconds between status polls when waiting (default: 20).",
    )
    parser.add_argument(
        "--timeout", type=int, default=600,
        help="Max seconds to wait for jobs to finish (default: 600, keeping the "
             "total run under the 10-minute budget).",
    )
    parser.add_argument(
        "--table",
        choices=sorted(TABLES.values()),
        help="Submit/check only this table instead of all four. Useful for "
             "isolating a single failing table without re-submitting (and "
             "duplicating rows in) the others.",
    )
    args = parser.parse_args()

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
    # cpdctl lands raw CSVs in its own schema, consumed by the Spark pipeline.
    ingest_schema = os.getenv("WXD_INGEST_SCHEMA", "spark_demo_cpdctl_raw")

    # cpdctl expects s3:// URIs; our env stores the Spark-style s3a:// base.
    input_base = _env("WXD_SPARK_INPUT_BASE", "s3a://iceberg-bucket/spark_demo/raw")
    s3_base = input_base.replace("s3a://", "s3://").rstrip("/")
    batch = args.batch or os.getenv("WXD_INGEST_BATCH_ID") or str(int(time.time()))
    instance_id = _env("WXD_INSTANCE_ID")

    tables = TABLES
    if args.table:
        tables = {k: v for k, v in TABLES.items() if v == args.table}

    # Job ids are deterministic from the batch, so status checks need no state file.
    job_ids = [f"ingest-{table}-{batch}" for table in tables.values()]

    print(f"Catalog: {catalog}")
    print(f"Spark engine: {engine_id}")
    print(f"Target schema: {catalog}.{ingest_schema}")
    print(f"Source base: {s3_base}")
    print(f"Batch: {batch}")
    print()

    # Validate the .env key and re-sync cpdctl's cached credentials BEFORE doing
    # any work, so a stale cpdctl cache can't fail us mid-job.
    cpdctl_profile = _ensure_cpdctl_auth()
    print()

    # --status: skip submission, just report on the batch's jobs.
    if args.status:
        return _report_status(job_ids, instance_id, cpdctl_profile,
                              args.wait, args.interval, args.timeout)

    _ensure_schema(catalog, ingest_schema)

    failures = 0
    submitted = []
    for csv_name, table in tables.items():
        target = f"{catalog}.{ingest_schema}.{table}"
        job_id = f"ingest-{table}-{batch}"
        timeout = TABLE_TIMEOUTS.get(csv_name, CPDCTL_CREATE_TIMEOUT)
        cmd = [
            "cpdctl", "wx-data", "ingestion", "create",
            "--instance-id", instance_id,
            "--source-data-files", f"{s3_base}/{csv_name}",
            "--source-file-type", "csv",
            "--target-table", target,
            "--engine-id", engine_id,
            "--job-id", job_id,
        ]
        # Pin explicit column types when defined — avoids Spark CSV inference
        # problems (DOUBLE vs DECIMAL, BIGINT vs INTEGER) that cause order_items
        # to enter an infinite Spark restart loop and never finish.
        schema = TABLE_SCHEMAS.get(csv_name)
        if schema is not None:
            cmd += ["--schema", json.dumps(schema)]
        cmd += ["--profile", cpdctl_profile]
        if storage := os.getenv("WXD_INGEST_STORAGE_NAME"):
            cmd += ["--storage-name", storage]
        print(f"\nSubmitting ingestion for {csv_name} -> {target} (timeout {timeout}s)")
        print("$ " + " ".join(cmd))
        try:
            result = subprocess.run(cmd, text=True, timeout=timeout)
        except subprocess.TimeoutExpired:
            failures += 1
            print(f"!! ingestion submit for {csv_name} timed out after {timeout}s [FAIL]")
            continue
        if result.returncode == 0:
            submitted.append(job_id)
            print(f"  submitted {job_id} [OK]")
            # Stagger: wait for the Spark application to actually launch before
            # submitting the next table, to avoid the event-log-directory
            # collision described above.
            deadline = time.time() + SUBMIT_STAGGER_TIMEOUT
            status = _job_status(job_id, instance_id, cpdctl_profile)
            while status in {"starting", "unknown"} and time.time() < deadline:
                time.sleep(SUBMIT_STAGGER_POLL_INTERVAL)
                status = _job_status(job_id, instance_id, cpdctl_profile)
            print(f"  {job_id} now {status} — proceeding")
        else:
            failures += 1
            print(f"!! ingestion failed for {csv_name} (exit {result.returncode}) [FAIL]")

    print("\n" + "=" * 74)
    print(f"Submitted {len(submitted)} ingestion job(s); {failures} failed.")
    for jid in submitted:
        print(f"  job_id: {jid}")
    print("\nThese appear in the watsonx.data console:")
    print(f"  {_ui_url()}")
    print("  Data manager -> Ingestion (history)")
    print(f"\nCheck status anytime:  python {Path(__file__).name} --status --batch {batch}")
    print("=" * 74)

    # --wait: poll the jobs we just submitted until they finish.
    if args.wait and submitted:
        rc = _report_status(submitted, instance_id, cpdctl_profile,
                            wait=True, interval=args.interval, timeout=args.timeout)
        return 1 if (failures or rc) else 0

    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
