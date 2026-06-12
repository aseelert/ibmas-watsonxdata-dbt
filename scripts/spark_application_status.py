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
    print(f"Checking Spark application: {app_id}")
    response = requests.get(
        f"{endpoint.rstrip('/')}/{app_id}",
        headers={
            "Authorization": _authorization_header(),
            "LhInstanceId": _env("WXD_INSTANCE_ID", "1781163689818519"),
        },
        verify=_ssl_verify(),
        timeout=60,
    )
    print(response.status_code)
    try:
        print(json.dumps(_redact(response.json()), indent=2))
    except Exception:
        print(response.text)
    response.raise_for_status()
    return 0


if __name__ == "__main__":
    sys.exit(main())
