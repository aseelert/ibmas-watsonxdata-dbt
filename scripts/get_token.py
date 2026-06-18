#!/usr/bin/env python3
"""Fetch a CPD bearer token and validate the watsonx.data connection.

Auth strategy (in order):
  1. WXD_API_KEY  → POST /icp4d-api/v1/authorize with api_key
  2. WXD_CPD_PASSWORD (or interactive prompt) → POST with password
     On success the new API key is regenerated and saved to .env automatically.

Usage:
    python scripts/get_token.py               # validate + print token
    python scripts/get_token.py --export      # also write bearer token to .env
    python scripts/get_token.py --refresh-key # force password login + new API key

How to get a fresh API key from the UI (if you prefer):
  1. Open https://<WXD_CPD_HOST>
  2. Log in as cpadmin with your password.
  3. Click your avatar (top-right) → Profile and settings → API key tab.
  4. Click "Regenerate API key" → copy the key.
  5. Paste it into .env as WXD_API_KEY=<new-key>.
"""

from __future__ import annotations

import argparse
import getpass
import os
import sys
from pathlib import Path

try:
    from dotenv import load_dotenv, set_key
except ImportError:
    raise SystemExit("Missing dependency 'python-dotenv'. Run: pip install python-dotenv")

try:
    import requests
    from requests.packages.urllib3.exceptions import InsecureRequestWarning
    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
except ImportError:
    raise SystemExit("Missing dependency 'requests'. Run: pip install requests")


ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = ROOT / ".env"


def _env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if not value:
        raise SystemExit(
            f"Missing required env var: {name}\n"
            f"Copy .env.example to .env and fill in your values."
        )
    return value


def _ssl_verify() -> bool | str:
    value = os.getenv("WXD_SSL_VERIFY", "").strip()
    if not value or value.lower() in {"1", "true", "yes"}:
        return True
    if value.lower() in {"0", "false", "no"}:
        return False
    path = Path(value)
    if not path.is_absolute():
        path = ROOT / path
    if not path.exists():
        print(f"  WARNING: SSL cert file not found: {path} — skipping verify", file=sys.stderr)
        return False
    return str(path)


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------

def _post_authorize(auth_url: str, payload: dict, verify: bool | str) -> requests.Response:
    return requests.post(auth_url, json=payload, verify=verify, timeout=30)


def auth_with_api_key(auth_url: str, username: str, api_key: str, verify: bool | str) -> str | None:
    """Return bearer token or None if the API key is rejected."""
    resp = _post_authorize(auth_url, {"username": username, "api_key": api_key}, verify)
    if resp.status_code == 200:
        return resp.json().get("token")
    if resp.status_code == 401:
        return None
    raise SystemExit(f"CPD auth error ({resp.status_code}): {resp.text}")


def auth_with_password(auth_url: str, username: str, password: str, verify: bool | str) -> str:
    """Return bearer token or exit on failure."""
    resp = _post_authorize(auth_url, {"username": username, "password": password}, verify)
    if resp.status_code == 200:
        token = resp.json().get("token")
        if token:
            return token
    raise SystemExit(
        f"Password login failed ({resp.status_code}): {resp.text}\n"
        f"  Check WXD_CPD_USERNAME and WXD_CPD_PASSWORD."
    )


def regenerate_api_key(cpd_host: str, token: str, verify: bool | str) -> str | None:
    """Call CPD to rotate the current user's API key and return the new key."""
    url = f"https://{cpd_host}/usermgmt/v1/user/apikey/regenerate"
    resp = requests.post(
        url,
        headers={"Authorization": f"Bearer {token}"},
        verify=verify,
        timeout=30,
    )
    if resp.status_code == 200:
        return resp.json().get("apiKey") or resp.json().get("api_key")
    print(
        f"  WARNING: could not regenerate API key ({resp.status_code}): {resp.text}",
        file=sys.stderr,
    )
    return None


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_instance(cpd_host: str, instance_id: str, token: str, verify: bool | str) -> None:
    url = f"https://{cpd_host}/lakehouse/api/v2/lhinstances/{instance_id}"
    resp = requests.get(
        url,
        headers={"Authorization": f"Bearer {token}"},
        verify=verify,
        timeout=30,
    )
    if resp.status_code == 404:
        raise SystemExit(
            f"Instance ID not found: {instance_id}\n"
            f"  WXD_INSTANCE_ID may be stale — re-run:\n"
            f"    python scripts/prepare_watsonx_env.py"
        )
    if resp.status_code == 401:
        raise SystemExit(
            f"Unauthorized accessing instance {instance_id} — token is valid but access denied."
        )
    if resp.status_code in {502, 503, 504}:
        print(f"  Instance endpoint unavailable ({resp.status_code}) — skipping check.")
        print(f"  Instance ID {instance_id} will be validated by Presto on first query.")
        return
    if resp.status_code != 200:
        print(f"  WARNING: instance check returned {resp.status_code}: {resp.text}", file=sys.stderr)
        return
    data = resp.json()
    name = data.get("display_name") or data.get("id") or instance_id
    print(f"  Instance: {name} ({instance_id})  [OK]")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fetch a CPD bearer token and validate the watsonx.data connection."
    )
    parser.add_argument(
        "--export",
        action="store_true",
        help="Write bearer token to WXD_SPARK_BEARER_TOKEN in .env.",
    )
    parser.add_argument(
        "--refresh-key",
        action="store_true",
        help="Force password login, regenerate API key, and update .env.",
    )
    parser.add_argument(
        "--env-file",
        default=str(ENV_FILE),
        help="Path to .env (default: repo root .env).",
    )
    args = parser.parse_args()

    env_path = Path(args.env_file).expanduser()
    if not env_path.exists():
        raise SystemExit(
            f".env not found at {env_path}\n"
            f"  cp .env.example .env  then fill in your values."
        )

    load_dotenv(env_path)

    auth_url    = _env("WXD_CPD_AUTH_URL")
    username    = _env("WXD_CPD_USERNAME", "cpadmin")
    api_key     = os.getenv("WXD_API_KEY", "").strip()
    cpd_host    = _env("WXD_CPD_HOST")
    instance_id = _env("WXD_INSTANCE_ID")
    verify      = _ssl_verify()

    print(f"Auth URL:    {auth_url}")
    print(f"CPD host:    {cpd_host}")
    print(f"Username:    {username}")
    print(f"Instance ID: {instance_id}")
    print()

    token: str | None = None
    used_password = False

    # --- Step 1: try API key unless --refresh-key is forced ---
    if api_key and not args.refresh_key:
        print("1. Trying API key auth...")
        token = auth_with_api_key(auth_url, username, api_key, verify)
        if token:
            print("  API key valid  [OK]")
        else:
            print("  API key rejected (expired or revoked) — falling back to password login.")

    # --- Step 2: fall back to password ---
    if token is None:
        password = os.getenv("WXD_CPD_PASSWORD", "").strip()
        if not password:
            print(f"\nEnter password for {username} on {cpd_host}:")
            password = getpass.getpass("  Password: ")
        print("1. Logging in with password...")
        token = auth_with_password(auth_url, username, password, verify)
        print("  Password login  [OK]")
        used_password = True

    short = token[:12] + "..." if len(token) > 12 else token
    print(f"  Token: {short}")
    print()

    # --- Step 3: regenerate API key after password login ---
    if used_password or args.refresh_key:
        print("2. Regenerating API key...")
        new_key = regenerate_api_key(cpd_host, token, verify)
        if new_key:
            set_key(str(env_path), "WXD_API_KEY", new_key)
            os.environ["WXD_API_KEY"] = new_key
            k = new_key[:8] + "..."
            print(f"  New API key: {k}  — saved to {env_path.name}  [OK]")
        print()

    # --- Step 4: validate instance ---
    print("3. Validating instance ID..." if (used_password or args.refresh_key) else "2. Validating instance ID...")
    validate_instance(cpd_host, instance_id, token, verify)
    print()

    if args.export:
        set_key(str(env_path), "WXD_SPARK_BEARER_TOKEN", token)
        print(f"Wrote WXD_SPARK_BEARER_TOKEN to {env_path.name}")
        print()

    print("Connection looks good. You can now run:")
    print("  python scripts/query_gold.py")
    print()
    print("Bearer token:")
    print(f"  {token}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
