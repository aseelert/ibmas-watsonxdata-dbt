#!/usr/bin/env python3
# -----------------------------------------------------------------------------
#  submit_confluent_gold.py — submit the Confluent GOLD Spark job to watsonx.data
#
#  Location  : confluent/scripts/submit_confluent_gold.py
#  Repository: https://github.com/aseelert/ibmas-watsonxdata-dbt
#  Project   : watsonx.data · dbt · Spark · Confluent medallion demo
#  Author    : Alexander Seelert — IBM Customer Success Engineer
#  Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.
#
#  Changelog :
#    v1.0 (2026-06-26) — Initial version. POSTs confluent/spark/confluent_gold.py
#      to the watsonx.data Spark engine (same REST/auth pattern as
#      scripts/submit_spark_application.py); argparse with --dry-run/--wait.
#    v1.1 (2026-06-26) — Durable s3a:// fix landed upstream (silver_jobs.sql +
#      docker-compose.yml warehouse now 's3a://'), so the silver metadata records
#      s3a:// paths the engine reads natively. The old per-job s3->s3a filesystem
#      bridge is now DISABLED by default; set CONFLUENT_GOLD_S3_BRIDGE=1 to
#      re-enable it as a legacy fallback for pre-fix 's3://'-pathed tables.
#    v1.2 (2026-06-26) — Gold materialisation parity with dbt. The Spark app now
#      writes ONLY confluent_gold_daily_sales (table); after it FINISHES this
#      submitter creates category_performance + customer_360 as Presto VIEWS via
#      scripts/create_gold_views.py (a Spark view is a Hive view Presto cannot
#      read). View creation requires the app to finish, so it always waits;
#      --no-views restores the old table-only/optional-wait behaviour.
# -----------------------------------------------------------------------------
"""Submit the Confluent GOLD Spark job to the watsonx.data Spark engine.

This is the "compute" half of the Confluent gold track when
``CONFLUENT_GOLD_ENGINE=spark``. It POSTs a Spark application submission to the
watsonx.data native Spark engine REST API
(``.../spark_engines/<engine>/applications``) that runs
``confluent/spark/confluent_gold.py`` on the engine. That job reads the
Flink-written Confluent silver tables and materialises the
``confluent_gold_*`` marts in ``iceberg_data.{CONFLUENT_GOLD_SCHEMA}``.

It reuses the SAME submit/auth pattern as
``scripts/submit_spark_application.py`` (the Spark-medallion submitter):
bearer / ZenApiKey / derived-CPD-token auth, the ``spark.hadoop.wxd.apiKey``
data-access key, runtime env propagated to driver + executors via every prefix
the engine might honor, and a best-effort Presto pre-create of the target
namespace so it lands at the catalog warehouse root with no Hive ``.db`` suffix.

WHEN TO RUN (demo flow)
 - After the Flink silver pipeline has checkpointed AND the silver tables are
   registered in watsonx.data (``confluent/scripts/prep_iceberg_schemas.py
   --phase register``), and while the Spark engine is running.
 - Defaults to a DRY RUN: prints the redacted payload and exits 0. Pass
   ``--no-dry-run`` (or set ``WXD_SPARK_DRY_RUN=false``) to actually submit.
   Add ``--wait`` to poll the application until it reaches a terminal state.

ENV VARS (read; all hosts/schemas come from .env — nothing hardcoded)
 - Endpoint/instance : WXD_SPARK_APPLICATIONS_ENDPOINT, WXD_INSTANCE_ID,
   WXD_SPARK_ENGINE_ID, WXD_CPD_HOST
 - Application/payload: CONFLUENT_GOLD_APPLICATION (the s3a:// URI of
   confluent_gold.py on object storage), WXD_SPARK_CATALOG,
   CONFLUENT_SILVER_SCHEMA, CONFLUENT_GOLD_SCHEMA
 - Spark sizing      : WXD_SPARK_EXECUTOR_CORES, WXD_SPARK_EXECUTOR_MEMORY,
   WXD_SPARK_DRIVER_CORES, WXD_SPARK_DRIVER_MEMORY
 - Auth (any one)    : WXD_SPARK_BEARER_TOKEN, WXD_ZEN_API_KEY,
   WXD_CPD_USERNAME/WXD_USER + WXD_CPD_API_KEY/WXD_API_KEY, WXD_CPD_PASSWORD,
   WXD_CPD_AUTH_URL, WXD_SPARK_WXD_APIKEY
 - Presto pre-create : WXD_HOST, WXD_PORT, WXD_USER, WXD_API_KEY
 - TLS / control     : WXD_SSL_VERIFY, WXD_SPARK_DRY_RUN

USAGE
 - Dry run (default) : python confluent/scripts/submit_confluent_gold.py
 - Real submit       : python confluent/scripts/submit_confluent_gold.py --no-dry-run
 - Submit + wait     : python confluent/scripts/submit_confluent_gold.py --no-dry-run --wait

SIDE EFFECTS / EXIT
 - On a real run: best-effort creates the gold namespace via Presto, then
   launches a Spark application on the engine and prints the application id and
   deep links. With --wait it polls until FINISHED/FAILED/STOPPED. Exits 0 on
   success, non-zero on missing env/auth, a non-2xx submit, or a failed job.
"""

from __future__ import annotations

import argparse
import base64
import json
import logging
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None


# confluent/scripts/ -> repo root is two levels up.
ROOT = Path(__file__).resolve().parents[2]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [submit_confluent_gold] %(levelname)s %(message)s",
)
log = logging.getLogger("submit_confluent_gold")

# Spark application states that mean "done" (used by --wait).
TERMINAL_STATES = {"FINISHED", "FAILED", "STOPPED", "KILLED", "ERROR", "DEAD"}


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


# ---------------------------------------------------------------------------
# Auth — identical pattern to scripts/submit_spark_application.py
# ---------------------------------------------------------------------------

def _cpd_username() -> str | None:
    if username := os.getenv("WXD_CPD_USERNAME"):
        return username
    user = os.getenv("WXD_USER", "")
    if user.startswith("ibmlhapikey_"):
        return user.removeprefix("ibmlhapikey_")
    return user or None


def _cpd_api_key() -> str | None:
    return os.getenv("WXD_CPD_API_KEY") or os.getenv("WXD_API_KEY")


def _derived_zen_api_key() -> str | None:
    username = _cpd_username()
    api_key = _cpd_api_key()
    if not username or not api_key:
        return None
    return base64.b64encode(f"{username}:{api_key}".encode("utf-8")).decode("ascii")


def _cpd_bearer_token() -> str | None:
    username = _cpd_username()
    api_key = _cpd_api_key()
    password = os.getenv("WXD_CPD_PASSWORD")
    if not username or not (api_key or password):
        return None

    try:
        import requests
    except ImportError as exc:
        raise SystemExit(
            "Missing dependency 'requests'. Install dependencies with: python -m pip install -r requirements.txt"
        ) from exc

    cpd_host = _env(
        "WXD_CPD_HOST",
        "cpd-cpd-instance.apps.watson.ibmas-zocp-techcluster.org",
    )
    auth_url = os.getenv("WXD_CPD_AUTH_URL", f"https://{cpd_host}/icp4d-api/v1/authorize")
    payloads = []
    if api_key:
        payloads.append({"username": username, "api_key": api_key})
    if password:
        payloads.append({"username": username, "password": password})

    response = None
    for payload in payloads:
        response = requests.post(auth_url, json=payload, verify=_ssl_verify(), timeout=60)
        if response.status_code < 400:
            break
    assert response is not None
    if response.status_code >= 400:
        raise SystemExit(
            f"Failed to generate CPD bearer token: {response.status_code} {response.text}"
        )
    data = response.json()
    token = data.get("token") or data.get("access_token")
    if not token:
        raise SystemExit(
            f"CPD auth response did not include token/access_token: {json.dumps(data)}"
        )
    return token


def _authorization_header() -> str:
    if token := os.getenv("WXD_SPARK_BEARER_TOKEN"):
        return f"Bearer {token}"
    if zen_key := os.getenv("WXD_ZEN_API_KEY"):
        return f"ZenApiKey {zen_key}"
    if zen_key := _derived_zen_api_key():
        return f"ZenApiKey {zen_key}"
    token = _cpd_bearer_token()
    if token:
        return f"Bearer {token}"
    raise SystemExit(
        "Missing Spark REST auth. Set WXD_SPARK_BEARER_TOKEN or WXD_ZEN_API_KEY, "
        "or set WXD_CPD_USERNAME and WXD_CPD_PASSWORD so a bearer token can be generated."
    )


def _zen_auth_string() -> str:
    """The watsonx.data data-access key passed as spark.hadoop.wxd.apiKey."""
    if encoded := os.getenv("WXD_SPARK_WXD_APIKEY"):
        return encoded
    user = _cpd_username() or _env("WXD_USER", "ibmlhapikey_cpadmin")
    key = _cpd_api_key() or _env("WXD_API_KEY")
    encoded = base64.b64encode(f"{user}:{key}".encode("utf-8")).decode("ascii")
    return f"ZenApiKey {encoded}"


# ---------------------------------------------------------------------------
# Payload
# ---------------------------------------------------------------------------

def _payload() -> dict[str, Any]:
    # The s3a:// URI of confluent_gold.py staged on object storage. Defaults to a
    # path alongside the Confluent gold namespace; override via .env.
    application = _env(
        "CONFLUENT_GOLD_APPLICATION",
        "s3a://iceberg-bucket/confluent_demo_gold/app/confluent_gold.py",
    )
    catalog = _env("WXD_SPARK_CATALOG", "iceberg_data")
    silver_schema = _env("CONFLUENT_SILVER_SCHEMA", "confluent_demo_silver")
    gold_schema = _env("CONFLUENT_GOLD_SCHEMA", "confluent_demo_gold")

    # Env vars confluent_gold.py reads at runtime. Propagated to driver +
    # executors via every prefix the engine might honor so the job sees them
    # wherever it runs.
    app_env = {
        "WXD_SPARK_CATALOG": catalog,
        "CONFLUENT_SILVER_SCHEMA": silver_schema,
        "CONFLUENT_GOLD_SCHEMA": gold_schema,
    }

    conf = {
        "spark.app.name": "watsonxdata-confluent-gold",
        "spark.hadoop.wxd.apiKey": _zen_auth_string(),
        "spark.executor.cores": os.getenv("WXD_SPARK_EXECUTOR_CORES", "1"),
        "spark.executor.memory": os.getenv("WXD_SPARK_EXECUTOR_MEMORY", "2G"),
        "spark.driver.cores": os.getenv("WXD_SPARK_DRIVER_CORES", "1"),
        "spark.driver.memory": os.getenv("WXD_SPARK_DRIVER_MEMORY", "2G"),
    }

    # --- s3:// scheme bridge — DISABLED by default (durable s3a:// fix is live) -
    # DURABLE FIX (active): silver_jobs.sql + docker-compose.yml declare the
    # Iceberg catalog warehouse with the 's3a://' scheme, so the silver metadata
    # records s3a:// data-file paths that the watsonx.data Spark engine reads
    # NATIVELY (it configures only the 's3a' Hadoop filesystem). No bridge needed.
    #
    # LEGACY FALLBACK (opt-in): if a deployment still has 's3://'-pathed silver
    # tables (created before the durable fix), set CONFLUENT_GOLD_S3_BRIDGE=1 to
    # re-enable the old behaviour — alias the 's3' scheme to a Hadoop S3A
    # filesystem pointed at MinIO, using the demo's object-store credentials.
    # Historically this fixed: RuntimeIOException: Failed to get file system for
    # path s3://...  Leave it off to validate the native s3a:// read path.
    bridge_enabled = os.getenv("CONFLUENT_GOLD_S3_BRIDGE", "0").lower() in ("1", "true", "yes")
    s3_endpoint = os.getenv("WXD_OBJECT_STORE_INTERNAL_ENDPOINT") or os.getenv("WXD_OBJECT_STORE_ENDPOINT", "")
    s3_key = os.getenv("WXD_OBJECT_STORE_ACCESS_KEY", "")
    s3_secret = os.getenv("WXD_OBJECT_STORE_SECRET_KEY", "")
    if bridge_enabled and s3_endpoint and s3_key and s3_secret:
        ssl_enabled = "true" if s3_endpoint.startswith("https") else "false"
        conf.update({
            "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.AbstractFileSystem.s3.impl": "org.apache.hadoop.fs.s3a.S3A",
            "spark.hadoop.fs.s3.endpoint": s3_endpoint,
            "spark.hadoop.fs.s3.access.key": s3_key,
            "spark.hadoop.fs.s3.secret.key": s3_secret,
            "spark.hadoop.fs.s3.path.style.access": "true",
            "spark.hadoop.fs.s3.connection.ssl.enabled": ssl_enabled,
            # Use simple key auth for the s3:// scheme (not the Watsonx signer,
            # which is wired only for the s3a iceberg-bucket overrides).
            "spark.hadoop.fs.s3.aws.credentials.provider":
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        })

    for prefix in ("spark.executorEnv", "spark.yarn.appMasterEnv",
                   "spark.driverEnv", "spark.kubernetes.driverEnv"):
        for key, value in app_env.items():
            conf[f"{prefix}.{key}"] = value

    return {"application_details": {"application": application, "conf": conf}}


def _redacted_payload(payload: dict[str, Any]) -> dict[str, Any]:
    safe = json.loads(json.dumps(payload))
    conf = safe.get("application_details", {}).get("conf", {})
    for secret_key in ("spark.hadoop.wxd.apiKey", "spark.hadoop.fs.s3.secret.key",
                       "spark.hadoop.fs.s3.access.key"):
        if secret_key in conf:
            conf[secret_key] = "<redacted>"
    return safe


def _ensure_gold_schema_via_presto() -> None:
    """Pre-create the Confluent gold namespace through Presto so it lands at the
    catalog warehouse root (no Hive `.db` suffix). The watsonx.data Iceberg
    catalog ignores CREATE NAMESPACE ... LOCATION, so Presto is the only lever
    for the on-disk layout. Best-effort: a failure here is non-fatal.
    """
    try:
        import prestodb
    except ImportError:
        log.warning("prestodb not installed — skipping schema pre-create; Spark may add .db")
        return
    gold_schema = _env("CONFLUENT_GOLD_SCHEMA", "confluent_demo_gold")
    catalog = _env("WXD_SPARK_CATALOG", "iceberg_data")
    user = _env("WXD_USER", "ibmlhapikey_cpadmin")
    try:
        conn = prestodb.dbapi.connect(
            host=_env("WXD_HOST"), port=int(os.getenv("WXD_PORT", "443")), user=user,
            catalog=catalog, http_scheme="https",
            http_headers={"LhInstanceId": _env("WXD_INSTANCE_ID")},
            auth=prestodb.auth.BasicAuthentication(user, _env("WXD_API_KEY")),
        )
        conn._http_session.verify = _ssl_verify()
        cur = conn.cursor()
        cur.execute(f"create schema if not exists {catalog}.{gold_schema}")
        cur.fetchall()
        log.info("Pre-created Confluent gold namespace via Presto (no .db): %s", gold_schema)
    except Exception as exc:  # noqa: BLE001 — best-effort, never block submission
        log.warning("Could not pre-create gold schema (%s); Spark may add a .db dir.", exc)


def _status_endpoint(endpoint: str, app_id: str) -> str:
    return f"{endpoint.rstrip('/')}/{app_id}"


def _spark_app_ui_url(endpoint: str, instance_id: str, engine_id: str, app_id: str) -> str:
    from urllib.parse import urlparse
    host = os.getenv("WXD_CPD_HOST") or urlparse(endpoint).netloc
    return (
        f"https://{host}/lakehouse/api/v3/{instance_id}"
        f"/spark_engines/{engine_id}/applications/{app_id}/ui"
    )


def _poll_until_terminal(endpoint: str, instance_id: str, app_id: str,
                         interval: int, timeout: int) -> str | None:
    """Poll the application status endpoint until a terminal state or timeout.

    Returns the final state string (or None if it could not be determined).
    """
    import requests

    deadline = time.time() + timeout
    last_state: str | None = None
    url = _status_endpoint(endpoint, app_id)
    log.info("Polling %s every %ss (timeout %ss)…", url, interval, timeout)
    while time.time() < deadline:
        try:
            resp = requests.get(
                url,
                headers={"Authorization": _authorization_header(), "LhInstanceId": instance_id},
                verify=_ssl_verify(),
                timeout=60,
            )
            if resp.status_code < 400:
                body = resp.json()
                # State can live under a couple of shapes depending on engine ver.
                state = (
                    body.get("state")
                    or body.get("application_state")
                    or (body.get("application_details") or {}).get("state")
                )
                if state and state != last_state:
                    log.info("Application %s state: %s", app_id, state)
                    last_state = state
                if state and state.upper() in TERMINAL_STATES:
                    return state
            else:
                log.warning("Status poll HTTP %s: %s", resp.status_code, resp.text[:300])
        except Exception as exc:  # noqa: BLE001 — keep polling through transient errors
            log.warning("Status poll error (will retry): %s", exc)
        time.sleep(interval)
    log.warning("Timed out after %ss waiting for a terminal state (last=%s).", timeout, last_state)
    return last_state


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Submit confluent/spark/confluent_gold.py to the watsonx.data Spark engine.",
    )
    dry = parser.add_mutually_exclusive_group()
    dry.add_argument(
        "--dry-run", dest="dry_run", action="store_true", default=None,
        help="Print the redacted payload and exit without submitting (default).",
    )
    dry.add_argument(
        "--no-dry-run", dest="dry_run", action="store_false",
        help="Actually submit the Spark application to the engine.",
    )
    parser.add_argument(
        "--wait", action="store_true",
        help="After a real submit, poll the application until it reaches a terminal state.",
    )
    parser.add_argument(
        "--poll-interval", type=int, default=int(os.getenv("WXD_SPARK_POLL_INTERVAL", "15")),
        help="Seconds between status polls when --wait is set (default 15).",
    )
    parser.add_argument(
        "--poll-timeout", type=int, default=int(os.getenv("WXD_SPARK_POLL_TIMEOUT", "1800")),
        help="Max seconds to wait for a terminal state when --wait is set (default 1800).",
    )
    parser.add_argument(
        "--no-views", dest="create_views", action="store_false", default=True,
        help="Skip creating the category_performance + customer_360 Presto VIEWS "
             "after the gold app finishes. They normally land as dbt-parity VIEWS "
             "(a Spark view is a Hive view watsonx Presto cannot read), so the "
             "Spark app writes only the daily_sales table and this step adds the "
             "two views via Presto. Implies waiting for the app to FINISH.",
    )
    return parser.parse_args(argv)


def _resolve_dry_run(cli_value: bool | None) -> bool:
    """CLI flag wins; otherwise fall back to WXD_SPARK_DRY_RUN (default true)."""
    if cli_value is not None:
        return cli_value
    return os.getenv("WXD_SPARK_DRY_RUN", "true").lower() in {"1", "true", "yes"}


def main(argv: list[str] | None = None) -> int:
    if load_dotenv is not None:
        load_dotenv(ROOT / ".env")

    args = _parse_args(sys.argv[1:] if argv is None else argv)

    try:
        import requests
    except ImportError as exc:
        raise SystemExit(
            "Missing dependency 'requests'. Install dependencies with: python -m pip install -r requirements.txt"
        ) from exc

    endpoint = _env(
        "WXD_SPARK_APPLICATIONS_ENDPOINT",
        "https://cpd-cpd-instance.apps.watson.ibmas-zocp-techcluster.org/lakehouse/api/v3/spark_engines/spark656/applications",
    )
    instance_id = _env("WXD_INSTANCE_ID", "1781163689818519")
    engine_id = os.getenv("WXD_SPARK_ENGINE_ID", "spark656")
    payload = _payload()

    log.info("Submitting to: %s", endpoint)
    log.info("LhInstanceId : %s", instance_id)
    print(json.dumps(_redacted_payload(payload), indent=2))

    if _resolve_dry_run(args.dry_run):
        log.info("Dry run only; pass --no-dry-run (or set WXD_SPARK_DRY_RUN=false) to submit.")
        return 0

    _ensure_gold_schema_via_presto()

    response = requests.post(
        endpoint,
        headers={
            "Authorization": _authorization_header(),
            "Content-Type": "application/json",
            "LhInstanceId": instance_id,
        },
        json=payload,
        verify=_ssl_verify(),
        timeout=60,
    )
    log.info("HTTP %s", response.status_code)
    print(response.text)
    if response.status_code >= 400:
        log.error("Confluent gold submission rejected (HTTP %s) by %s",
                  response.status_code, endpoint)
    else:
        log.info("Confluent gold submission accepted (HTTP %s) by %s",
                 response.status_code, endpoint)
    response.raise_for_status()

    # Pull the application id out of the response so the next step is obvious.
    app_id = state = None
    try:
        body = response.json()
        app_id = body.get("id") or body.get("application_id") or body.get("application_uuid")
        state = body.get("state")
    except ValueError:
        pass

    print("\n" + "=" * 74)
    if app_id:
        log.info("Spark application submitted.  Application ID: %s", app_id)
        if state:
            log.info("Initial state: %s", state)
        print(f"\nStatus API: {_status_endpoint(endpoint, app_id)}")
        print(f"Spark application UI: {_spark_app_ui_url(endpoint, instance_id, engine_id, app_id)}")
    else:
        log.warning("Submitted, but no application id was found in the response body above.")
    print("=" * 74)

    # The two gold "view" marts (category_performance, customer_360) read the
    # confluent_gold_daily_sales TABLE this app writes, so we must let the app
    # FINISH before creating them. When views are wanted (the default) we always
    # poll to a terminal state; --no-views falls back to the old --wait behaviour.
    must_wait = app_id and (args.create_views or args.wait)
    if must_wait:
        final_state = _poll_until_terminal(
            endpoint, instance_id, app_id, args.poll_interval, args.poll_timeout
        )
        if not (final_state and final_state.upper() == "FINISHED"):
            log.error("Confluent gold application ended in non-success state: %s", final_state)
            return 1
        log.info("Confluent gold application FINISHED successfully.")

        if args.create_views:
            try:
                _create_gold_views()
            except subprocess.CalledProcessError as exc:
                log.error("Gold VIEW creation failed (exit %s).", exc.returncode)
                return 1
        return 0

    return 0


def _create_gold_views() -> None:
    """Create the category_performance + customer_360 Presto VIEWS (dbt parity).

    Runs scripts/create_gold_views.py --path confluent as a subprocess (sharing
    this process's environment/.env), so the two marts land as Presto VIEWS that
    watsonx Presto can read — unlike a Spark-created Hive view. Idempotent.
    """
    script = ROOT / "scripts" / "create_gold_views.py"
    log.info("Creating gold VIEW marts via Presto: %s --path confluent", script)
    subprocess.run(
        [sys.executable, str(script), "--path", "confluent"],
        check=True,
        env=os.environ.copy(),
    )


if __name__ == "__main__":
    sys.exit(main())
