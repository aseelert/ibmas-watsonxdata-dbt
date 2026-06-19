#!/usr/bin/env python3
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


def _cpdctl(*args: str) -> subprocess.CompletedProcess:
    """Run a cpdctl subcommand, capturing output (raises nothing here)."""
    return subprocess.run(["cpdctl", *args], text=True, capture_output=True)


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
        "--timeout", type=int, default=900,
        help="Max seconds to wait for jobs to finish (default: 900).",
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
    base_schema = _env("WXD_SCHEMA", "lakehouse_demo")
    ingest_schema = os.getenv("WXD_INGEST_SCHEMA", f"{base_schema}_ingest")

    # cpdctl expects s3:// URIs; our env stores the Spark-style s3a:// base.
    input_base = _env("WXD_SPARK_INPUT_BASE", "s3a://iceberg-bucket/spark_demo/raw")
    s3_base = input_base.replace("s3a://", "s3://").rstrip("/")
    batch = args.batch or os.getenv("WXD_INGEST_BATCH_ID") or str(int(time.time()))
    instance_id = _env("WXD_INSTANCE_ID")

    # Job ids are deterministic from the batch, so status checks need no state file.
    job_ids = [f"ingest-{table}-{batch}" for table in TABLES.values()]

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
    for csv_name, table in TABLES.items():
        target = f"{catalog}.{ingest_schema}.{table}"
        job_id = f"ingest-{table}-{batch}"
        cmd = [
            "cpdctl", "wx-data", "ingestion", "create",
            "--instance-id", instance_id,
            "--source-data-files", f"{s3_base}/{csv_name}",
            "--source-file-type", "csv",
            "--target-table", target,
            "--engine-id", engine_id,
            "--job-id", job_id,
        ]
        cmd += ["--profile", cpdctl_profile]
        if storage := os.getenv("WXD_INGEST_STORAGE_NAME"):
            cmd += ["--storage-name", storage]
        print("\n$ " + " ".join(cmd))
        result = subprocess.run(cmd, text=True)
        if result.returncode == 0:
            submitted.append(job_id)
        else:
            failures += 1
            print(f"!! ingestion failed for {csv_name} (exit {result.returncode})")

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
