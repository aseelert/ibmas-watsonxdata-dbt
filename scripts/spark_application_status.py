#!/usr/bin/env python3
# -----------------------------------------------------------------------------
#  spark_application_status.py — check a watsonx.data Spark application's status
#
#  Location  : scripts/spark_application_status.py
#  Repository: https://github.ibm.com/alexander/ibmas-watsonxdata-dbt
#  Project   : watsonx.data · dbt · Spark medallion demo
#  Author    : Alexander Seelert
#  Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.
# -----------------------------------------------------------------------------
"""Check a watsonx.data Spark application status.

This script GETs a single Spark application from the watsonx.data native Spark
engine REST API (``.../spark_engines/<engine>/applications/<app_id>``) and
prints its current ``state`` (e.g. ``SUBMITTED`` / ``RUNNING`` / ``FINISHED`` /
``FAILED``) together with the full (secret-redacted) response body and handy
deep links to the application and console UIs.

WHAT & WHY
 - It is the "observability" half of the Spark track: after
   ``submit_spark_application.py`` launches ``load_medallion_demo.py``, this
   script lets the presenter confirm the job progressed to ``FINISHED`` before
   showing the resulting bronze/silver/gold Iceberg tables.
 - Auth precedence mirrors the submitter: an explicit bearer token, then an
   explicit ZenApiKey, then a ZenApiKey derived from the CPD username + API key.
 - The response JSON is recursively redacted so ``spark.hadoop.wxd.apiKey`` is
   never echoed to the terminal/log.

WHEN TO RUN (demo flow)
 - Immediately after a real (non dry-run) ``submit_spark_application.py``, using
   the application id it printed. Re-run to poll until the state is terminal.

ENV VARS (read)
 - Endpoint/instance : WXD_SPARK_APPLICATIONS_ENDPOINT, WXD_INSTANCE_ID,
   WXD_SPARK_ENGINE_ID, WXD_CPD_HOST, WXD_CONSOLE_URL, WXD_CONSOLE_VIEW
 - Application id    : WXD_SPARK_APPLICATION_ID (fallback when no CLI arg given)
 - Auth (any one)    : WXD_SPARK_BEARER_TOKEN, WXD_ZEN_API_KEY,
   WXD_CPD_USERNAME/WXD_USER + WXD_CPD_API_KEY/WXD_API_KEY
 - TLS               : WXD_SSL_VERIFY

PREREQUISITES
 - Python deps from ``requirements.txt`` (``requests``; ``python-dotenv``
   optional for ``.env`` load). No ``oc login`` / ``cpdctl`` needed — it talks
   straight to the REST API. A reachable, running Spark engine and valid auth.

USAGE
 - With id arg : ``python scripts/spark_application_status.py <app_id>``
 - From env    : ``WXD_SPARK_APPLICATION_ID=<app_id> python scripts/spark_application_status.py``

SIDE EFFECTS / EXIT
 - Read-only against the engine. Prints status + UI links. Exits 0 on a 2xx
   response; raises ``SystemExit`` on missing env/auth and ``raise_for_status``
   on a non-2xx status response.
"""

from __future__ import annotations

import base64
import json
import os
import sys
from pathlib import Path

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


def _cpd_username() -> str:
    if username := os.getenv("WXD_CPD_USERNAME"):
        return username
    user = _env("WXD_USER", "ibmlhapikey_cpadmin")
    if user.startswith("ibmlhapikey_"):
        return user.removeprefix("ibmlhapikey_")
    return user


def _authorization_header() -> str:
    if token := os.getenv("WXD_SPARK_BEARER_TOKEN"):
        return f"Bearer {token}"
    if zen_key := os.getenv("WXD_ZEN_API_KEY"):
        return f"ZenApiKey {zen_key}"
    api_key = _env("WXD_CPD_API_KEY", os.getenv("WXD_API_KEY"))
    encoded = base64.b64encode(f"{_cpd_username()}:{api_key}".encode("utf-8")).decode("ascii")
    return f"ZenApiKey {encoded}"


def _watsonx_ui_url(endpoint: str, instance_id: str) -> str:
    """Build the watsonx.data console URL dynamically from the environment."""
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


def _redact(data):
    if isinstance(data, dict):
        return {
            key: "<redacted>" if key in {"spark.hadoop.wxd.apiKey"} else _redact(value)
            for key, value in data.items()
        }
    if isinstance(data, list):
        return [_redact(value) for value in data]
    return data


def main() -> int:
    if load_dotenv is not None:
        load_dotenv(ROOT / ".env")

    try:
        import requests
    except ImportError as exc:
        raise SystemExit(
            "Missing dependency 'requests'. Install dependencies with: python -m pip install -r requirements.txt"
        ) from exc

    app_id = sys.argv[1] if len(sys.argv) > 1 else _env("WXD_SPARK_APPLICATION_ID")
    endpoint = _env(
        "WXD_SPARK_APPLICATIONS_ENDPOINT",
        "https://cpd-cpd-instance.apps.watson.ibmas-zocp-techcluster.org/lakehouse/api/v3/spark_engines/spark656/applications",
    )
    instance_id = _env("WXD_INSTANCE_ID", "1781163689818519")
    print(f"Checking Spark application: {app_id}")
    response = requests.get(
        f"{endpoint.rstrip('/')}/{app_id}",
        headers={
            "Authorization": _authorization_header(),
            "LhInstanceId": instance_id,
        },
        verify=_ssl_verify(),
        timeout=60,
    )
    print(response.status_code)
    if response.status_code >= 400:
        print(f"[FAIL] Status query rejected (HTTP {response.status_code}) for app {app_id}")
    else:
        print(f"[OK] Status query returned (HTTP {response.status_code}) for app {app_id}")
    state = None
    try:
        body = response.json()
        state = body.get("state") if isinstance(body, dict) else None
        print(json.dumps(_redact(body), indent=2))
    except ValueError:
        print(response.text)

    engine_id = os.getenv("WXD_SPARK_ENGINE_ID", "spark656")
    print("\n" + "=" * 74)
    print(f"Application ID: {app_id}" + (f"   State: {state}" if state else ""))
    print(
        f"Spark application UI: {_spark_app_ui_url(endpoint, instance_id, engine_id, app_id)}"
    )
    print(f"watsonx.data UI: {_watsonx_ui_url(endpoint, instance_id)}")
    print(f"  Infrastructure manager -> Spark engine '{engine_id}' -> Applications tab")
    print("=" * 74)
    response.raise_for_status()
    return 0


if __name__ == "__main__":
    sys.exit(main())
