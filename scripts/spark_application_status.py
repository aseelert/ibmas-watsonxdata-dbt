#!/usr/bin/env python3
"""Check a watsonx.data Spark application status."""

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
