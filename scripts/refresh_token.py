#!/usr/bin/env python3
# -----------------------------------------------------------------------------
#  refresh_token.py — fetch a fresh CPD bearer token and write it to .env
#
#  Usage:
#    python scripts/refresh_token.py           # refresh WXD_SPARK_BEARER_TOKEN
#    python scripts/refresh_token.py --check   # decode + print current token expiry
#
#  The CPD bearer token (WXD_SPARK_BEARER_TOKEN) expires after ~12 hours.
#  This script authenticates with WXD_CPD_USERNAME + WXD_API_KEY (already in
#  .env), writes the new token back, and prints the new expiry time.
# -----------------------------------------------------------------------------
from __future__ import annotations

import argparse
import base64
import json
import os
import re
import sys
import time
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

ROOT = Path(__file__).resolve().parents[1]


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


def _decode_exp(token: str) -> int:
    payload_b64 = token.split(".")[1]
    payload_b64 += "=" * (4 - len(payload_b64) % 4)
    payload = json.loads(base64.b64decode(payload_b64))
    return int(payload.get("exp", 0))


def check_current() -> None:
    token = os.getenv("WXD_SPARK_BEARER_TOKEN", "").strip().strip("'\"")
    if not token:
        print("WXD_SPARK_BEARER_TOKEN is not set in .env")
        return
    exp = _decode_exp(token)
    now = int(time.time())
    exp_str = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(exp))
    if now > exp:
        hours_ago = (now - exp) / 3600
        print(f"EXPIRED   : {exp_str}  ({hours_ago:.1f} hours ago)")
    else:
        ttl_h = (exp - now) / 3600
        print(f"Valid until: {exp_str}  ({ttl_h:.1f} hours remaining)")


def refresh() -> None:
    try:
        import requests
    except ImportError as exc:
        raise SystemExit("Missing 'requests'. Run: pip install -r requirements.txt") from exc

    auth_url = os.environ.get("WXD_CPD_AUTH_URL")
    username = os.environ.get("WXD_CPD_USERNAME")
    api_key = os.environ.get("WXD_API_KEY")
    if not all([auth_url, username, api_key]):
        raise SystemExit("Missing WXD_CPD_AUTH_URL / WXD_CPD_USERNAME / WXD_API_KEY in .env")

    print(f"Requesting token from {auth_url} ...")
    r = requests.post(
        auth_url,
        json={"username": username, "api_key": api_key},
        verify=_ssl_verify(),
        timeout=30,
    )
    if r.status_code >= 400:
        raise SystemExit(f"Auth failed HTTP {r.status_code}: {r.text[:300]}")

    token = r.json().get("token") or r.json().get("access_token")
    if not token:
        raise SystemExit(f"No token in response: {r.text[:300]}")

    exp = _decode_exp(token)
    exp_str = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(exp))
    print(f"New token expires: {exp_str}")

    env_path = ROOT / ".env"
    content = env_path.read_text()
    new_line = f"WXD_SPARK_BEARER_TOKEN={token}"
    if re.search(r"^WXD_SPARK_BEARER_TOKEN=", content, flags=re.MULTILINE):
        content = re.sub(r"^WXD_SPARK_BEARER_TOKEN=.*$", new_line, content, flags=re.MULTILINE)
    else:
        content += f"\n{new_line}\n"
    env_path.write_text(content)
    print(f"Updated WXD_SPARK_BEARER_TOKEN in {env_path}")


def main() -> None:
    if load_dotenv:
        load_dotenv(ROOT / ".env")

    parser = argparse.ArgumentParser(description="Refresh the CPD bearer token in .env")
    parser.add_argument("--check", action="store_true", help="Print current token expiry and exit")
    args = parser.parse_args()

    if args.check:
        check_current()
    else:
        check_current()
        refresh()


if __name__ == "__main__":
    main()
