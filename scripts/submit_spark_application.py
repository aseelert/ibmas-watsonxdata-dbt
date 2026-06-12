#!/usr/bin/env python3
"""Submit the Spark medallion demo to the watsonx.data Spark engine."""

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

    return {
        "application_details": {
            "application": application,
            "conf": {
                "spark.app.name": "watsonxdata-medallion-spark-demo",
                "spark.hadoop.wxd.apiKey": _zen_auth_string(),
                "spark.executor.cores": os.getenv("WXD_SPARK_EXECUTOR_CORES", "1"),
                "spark.executor.memory": os.getenv("WXD_SPARK_EXECUTOR_MEMORY", "2G"),
                "spark.driver.cores": os.getenv("WXD_SPARK_DRIVER_CORES", "1"),
                "spark.driver.memory": os.getenv("WXD_SPARK_DRIVER_MEMORY", "2G"),
                "spark.executorEnv.WXD_SPARK_INPUT_BASE": input_base,
                "spark.executorEnv.WXD_SPARK_CATALOG": catalog,
                "spark.executorEnv.WXD_SPARK_SCHEMA": schema,
                "spark.executorEnv.WXD_SPARK_INGEST_BATCH_ID": os.getenv(
                    "WXD_SPARK_INGEST_BATCH_ID", "spark_demo_batch"
                ),
                "spark.yarn.appMasterEnv.WXD_SPARK_INPUT_BASE": input_base,
                "spark.yarn.appMasterEnv.WXD_SPARK_CATALOG": catalog,
                "spark.yarn.appMasterEnv.WXD_SPARK_SCHEMA": schema,
                "spark.yarn.appMasterEnv.WXD_SPARK_INGEST_BATCH_ID": os.getenv(
                    "WXD_SPARK_INGEST_BATCH_ID", "spark_demo_batch"
                ),
                "spark.driverEnv.WXD_SPARK_INPUT_BASE": input_base,
                "spark.driverEnv.WXD_SPARK_CATALOG": catalog,
                "spark.driverEnv.WXD_SPARK_SCHEMA": schema,
                "spark.driverEnv.WXD_SPARK_INGEST_BATCH_ID": os.getenv(
                    "WXD_SPARK_INGEST_BATCH_ID", "spark_demo_batch"
                ),
                "spark.kubernetes.driverEnv.WXD_SPARK_INPUT_BASE": input_base,
                "spark.kubernetes.driverEnv.WXD_SPARK_CATALOG": catalog,
                "spark.kubernetes.driverEnv.WXD_SPARK_SCHEMA": schema,
                "spark.kubernetes.driverEnv.WXD_SPARK_INGEST_BATCH_ID": os.getenv(
                    "WXD_SPARK_INGEST_BATCH_ID", "spark_demo_batch"
                ),
            },
        }
    }


def _redacted_payload(payload: dict[str, Any]) -> dict[str, Any]:
    safe = json.loads(json.dumps(payload))
    conf = safe.get("application_details", {}).get("conf", {})
    if "spark.hadoop.wxd.apiKey" in conf:
        conf["spark.hadoop.wxd.apiKey"] = "<redacted>"
    return safe


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
    response.raise_for_status()
    return 0


if __name__ == "__main__":
    sys.exit(main())
