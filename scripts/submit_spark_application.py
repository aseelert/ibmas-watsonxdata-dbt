#!/usr/bin/env python3
# -----------------------------------------------------------------------------
#  submit_spark_application.py — submit the Spark medallion demo to watsonx.data
#
#  Location  : scripts/submit_spark_application.py
#  Repository: https://github.ibm.com/alexander/ibmas-watsonxdata-dbt
#  Project   : watsonx.data · dbt · Spark medallion demo
#  Author    : Alexander Seelert
#  Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.
# -----------------------------------------------------------------------------
"""Submit the Spark medallion demo to the watsonx.data Spark engine.

This script POSTs a Spark application submission to the watsonx.data native
Spark engine REST API (``.../spark_engines/<engine>/applications``). It is the
"compute" half of the demo's Spark track: it launches ``load_medallion_demo.py``
inside the engine, which builds the bronze/silver/gold Iceberg namespaces from
the raw objects already staged in object storage.

WHAT & WHY
 - Assembles the ``application_details`` payload (application URI + Spark conf),
   propagating the demo's runtime env vars (input base, catalog, schema, batch
   id) to the driver AND executors via every prefix the engine might honor
   (``spark.executorEnv``, ``spark.yarn.appMasterEnv``, ``spark.driverEnv``,
   ``spark.kubernetes.driverEnv``) so the job sees them wherever it runs.
 - Injects the watsonx.data data-access key as ``spark.hadoop.wxd.apiKey`` so
   Spark can read/write the Iceberg catalog's object storage.
 - Best-effort pre-creates the bronze/silver/gold namespaces THROUGH PRESTO so
   they land at the catalog warehouse root with NO Hive ``.db`` suffix (the
   Iceberg catalog ignores ``CREATE NAMESPACE ... LOCATION``, so Presto is the
   only lever for the on-disk layout). A failure here is non-fatal.

WHEN TO RUN (demo flow)
 - After the raw objects have been staged to object storage (the ingest/storage
   steps) and while the Spark engine ``spark656`` is running.
 - Defaults to DRY RUN: it prints the redacted payload and exits 0. Set
   ``WXD_SPARK_DRY_RUN=false`` to actually submit. After submission, poll with
   ``scripts/spark_application_status.py <app_id>``.

ENV VARS (read)
 - Endpoint/instance : WXD_SPARK_APPLICATIONS_ENDPOINT, WXD_INSTANCE_ID,
   WXD_SPARK_ENGINE_ID, WXD_CPD_HOST, WXD_CONSOLE_URL, WXD_CONSOLE_VIEW
 - Payload           : WXD_SPARK_APPLICATION, WXD_SPARK_INPUT_BASE,
   WXD_SPARK_CATALOG, WXD_SPARK_SCHEMA, WXD_SPARK_INGEST_BATCH_ID
 - Spark sizing      : WXD_SPARK_EXECUTOR_CORES, WXD_SPARK_EXECUTOR_MEMORY,
   WXD_SPARK_DRIVER_CORES, WXD_SPARK_DRIVER_MEMORY
 - Auth (any one)    : WXD_SPARK_BEARER_TOKEN, WXD_ZEN_API_KEY,
   WXD_CPD_USERNAME/WXD_USER + WXD_CPD_API_KEY/WXD_API_KEY,
   WXD_CPD_PASSWORD, WXD_CPD_AUTH_URL, WXD_SPARK_WXD_APIKEY
 - Presto pre-create : WXD_HOST, WXD_PORT, WXD_USER, WXD_API_KEY,
   WXD_SPARK_BRONZE_SCHEMA, WXD_SPARK_SILVER_SCHEMA, WXD_SPARK_GOLD_SCHEMA
 - TLS / control     : WXD_SSL_VERIFY, WXD_SPARK_DRY_RUN

PREREQUISITES
 - Python deps from ``requirements.txt`` (``requests``; ``prestodb`` optional
   for the namespace pre-create; ``python-dotenv`` optional for ``.env`` load).
 - A reachable, running watsonx.data Spark engine and valid Spark REST auth.
   No ``oc login`` / ``cpdctl`` needed — this talks straight to the REST API.

USAGE
 - Dry run (default) : ``python scripts/submit_spark_application.py``
 - Real submit       : ``WXD_SPARK_DRY_RUN=false python scripts/submit_spark_application.py``

SIDE EFFECTS / EXIT
 - On a real run: creates Iceberg namespaces via Presto (best-effort) and
   launches a Spark application on the engine. Prints the application id and
   deep links. Exits 0 on success; raises ``SystemExit`` on missing env/auth
   and ``raise_for_status`` on a non-2xx submit response.
"""

from __future__ import annotations

import base64
import json
import os
import sys
from pathlib import Path
from typing import Any

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None


ROOT = Path(__file__).resolve().parents[1]


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


def _watsonx_ui_url(endpoint: str, instance_id: str) -> str:
    """Build the watsonx.data console URL dynamically from the environment.

    Pattern: https://<cpd-host>/watsonx-data/#/<view>?instanceId=<instance-id>
    """
    from urllib.parse import urlparse

    base = os.getenv("WXD_CONSOLE_URL")
    if not base:
        host = os.getenv("WXD_CPD_HOST") or urlparse(endpoint).netloc
        base = f"https://{host}/watsonx-data/#"
    base = base.rstrip("/")
    view = os.getenv("WXD_CONSOLE_VIEW", "infrastructure-manager")
    return f"{base}/{view}?instanceId={instance_id}"


def _spark_app_ui_url(endpoint: str, instance_id: str, engine_id: str, app_id: str) -> str:
    """Direct deep link to a single Spark application's UI (driver/Spark UI)."""
    from urllib.parse import urlparse

    host = os.getenv("WXD_CPD_HOST") or urlparse(endpoint).netloc
    return (
        f"https://{host}/lakehouse/api/v3/{instance_id}"
        f"/spark_engines/{engine_id}/applications/{app_id}/ui"
    )


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

    cpd_host = os.getenv(
        "WXD_CPD_HOST",
        "cpd-cpd-instance.apps.watson.ibmas-zocp-techcluster.org",
    )
    auth_url = os.getenv(
        "WXD_CPD_AUTH_URL",
        f"https://{cpd_host}/icp4d-api/v1/authorize",
    )
    payloads = []
    if api_key:
        payloads.append({"username": username, "api_key": api_key})
    if password:
        payloads.append({"username": username, "password": password})

    response = None
    for payload in payloads:
        response = requests.post(
            auth_url,
            json=payload,
            verify=_ssl_verify(),
            timeout=60,
        )
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


def _zen_auth_string() -> str:
    if encoded := os.getenv("WXD_SPARK_WXD_APIKEY"):
        return encoded

    user = _cpd_username() or _env("WXD_USER", "ibmlhapikey_cpadmin")
    key = _cpd_api_key() or _env("WXD_API_KEY")
    encoded = base64.b64encode(f"{user}:{key}".encode("utf-8")).decode("ascii")
    return f"ZenApiKey {encoded}"


def _payload() -> dict[str, Any]:
    application = _env(
        "WXD_SPARK_APPLICATION",
        "s3a://iceberg-bucket/spark_demo/app/load_medallion_demo.py",
    )
    input_base = _env("WXD_SPARK_INPUT_BASE", "s3a://iceberg-bucket/spark_demo/raw")
    catalog = _env("WXD_SPARK_CATALOG", "iceberg_data")
    schema = _env("WXD_SPARK_SCHEMA", "spark_demo")

    # Env vars the Spark application reads at runtime. Propagated to driver +
    # executors via every prefix the engine might honor (executorEnv, appMasterEnv,
    # driverEnv, kubernetes.driverEnv) so the job sees them wherever it runs.
    app_env = {
        "WXD_SPARK_INPUT_BASE": input_base,
        "WXD_SPARK_CATALOG": catalog,
        "WXD_SPARK_SCHEMA": schema,
        "WXD_SPARK_INGEST_BATCH_ID": os.getenv("WXD_SPARK_INGEST_BATCH_ID", "spark_demo_batch"),
    }

    conf = {
        "spark.app.name": "watsonxdata-medallion-spark-demo",
        "spark.hadoop.wxd.apiKey": _zen_auth_string(),
        "spark.executor.cores": os.getenv("WXD_SPARK_EXECUTOR_CORES", "1"),
        "spark.executor.memory": os.getenv("WXD_SPARK_EXECUTOR_MEMORY", "2G"),
        "spark.driver.cores": os.getenv("WXD_SPARK_DRIVER_CORES", "1"),
        "spark.driver.memory": os.getenv("WXD_SPARK_DRIVER_MEMORY", "2G"),
    }
    for prefix in ("spark.executorEnv", "spark.yarn.appMasterEnv",
                   "spark.driverEnv", "spark.kubernetes.driverEnv"):
        for key, value in app_env.items():
            conf[f"{prefix}.{key}"] = value

    return {"application_details": {"application": application, "conf": conf}}


def _redacted_payload(payload: dict[str, Any]) -> dict[str, Any]:
    safe = json.loads(json.dumps(payload))
    conf = safe.get("application_details", {}).get("conf", {})
    if "spark.hadoop.wxd.apiKey" in conf:
        conf["spark.hadoop.wxd.apiKey"] = "<redacted>"
    return safe


def _ensure_spark_schemas_via_presto() -> None:
    """Pre-create the Spark namespaces through Presto so they land at the catalog
    default warehouse (bucket root, e.g. spark_demo_bronze/) with NO Hive `.db`
    suffix. If Spark created them itself they'd become spark_demo_bronze.db/ —
    the watsonx.data Iceberg catalog ignores CREATE NAMESPACE ... LOCATION, so
    pre-creating via Presto is the only way to control the on-disk layout.
    Best-effort: a failure here is non-fatal (the job can still create its own).
    """
    try:
        import prestodb
    except ImportError:
        print("  (prestodb not installed — skipping schema pre-create; Spark may add .db)")
        return
    base = _env("WXD_SPARK_SCHEMA", "spark_demo")
    schemas = [
        os.getenv("WXD_SPARK_BRONZE_SCHEMA", f"{base}_bronze"),
        os.getenv("WXD_SPARK_SILVER_SCHEMA", f"{base}_silver"),
        os.getenv("WXD_SPARK_GOLD_SCHEMA", f"{base}_gold"),
    ]
    user = _env("WXD_USER", "ibmlhapikey_cpadmin")
    try:
        conn = prestodb.dbapi.connect(
            host=_env("WXD_HOST"), port=int(os.getenv("WXD_PORT", "443")), user=user,
            catalog=_env("WXD_SPARK_CATALOG", "iceberg_data"), http_scheme="https",
            http_headers={"LhInstanceId": _env("WXD_INSTANCE_ID")},
            auth=prestodb.auth.BasicAuthentication(user, _env("WXD_API_KEY")),
        )
        conn._http_session.verify = _ssl_verify()
        cur = conn.cursor()
        for schema in schemas:
            cur.execute(f"create schema if not exists iceberg_data.{schema}")
            cur.fetchall()
        print(f"  Pre-created Spark namespaces via Presto (no .db): {', '.join(schemas)}")
    except Exception as exc:  # noqa: BLE001 — best-effort, never block submission
        print(f"  WARNING: could not pre-create Spark schemas ({exc}); Spark may add .db dirs.")


def main() -> int:
    if load_dotenv is not None:
        load_dotenv(ROOT / ".env")

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
    payload = _payload()

    print(f"Submitting to: {endpoint}")
    print(f"LhInstanceId: {instance_id}")
    print(json.dumps(_redacted_payload(payload), indent=2))
    if os.getenv("WXD_SPARK_DRY_RUN", "true").lower() in {"1", "true", "yes"}:
        print("Dry run only; set WXD_SPARK_DRY_RUN=false to submit.")
        return 0

    _ensure_spark_schemas_via_presto()

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
    print(response.status_code)
    print(response.text)
    if response.status_code >= 400:
        print(f"[FAIL] Spark submission rejected (HTTP {response.status_code}) by {endpoint}")
    else:
        print(f"[OK] Spark submission accepted (HTTP {response.status_code}) by {endpoint}")
    response.raise_for_status()

    # Pull the application id out of the response so the next step is obvious.
    app_id = None
    state = None
    try:
        body = response.json()
        app_id = body.get("id") or body.get("application_id") or body.get("application_uuid")
        state = body.get("state")
    except ValueError:
        pass

    engine_id = os.getenv("WXD_SPARK_ENGINE_ID", "spark656")
    ui_url = _watsonx_ui_url(endpoint, instance_id)

    print("\n" + "=" * 74)
    if app_id:
        print(f"Spark application submitted.  Application ID: {app_id}")
        if state:
            print(f"Initial state: {state}")
        print("\nCheck status / poll until FINISHED:")
        print(f"  python scripts/spark_application_status.py {app_id}")
        print(f"\nStatus API: {endpoint.rstrip('/')}/{app_id}")
        print(
            f"Spark application UI: {_spark_app_ui_url(endpoint, instance_id, engine_id, app_id)}"
        )
    else:
        print("Submitted, but no application id was found in the response body above.")
    print(f"\nwatsonx.data UI: {ui_url}")
    print(
        f"  Infrastructure manager -> Spark engine '{engine_id}' -> Applications tab"
    )
    print("=" * 74)
    return 0


if __name__ == "__main__":
    sys.exit(main())
