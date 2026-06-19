# -----------------------------------------------------------------------------
#  get_om_token.py — mint a short-lived OpenMetadata ingestion-bot JWT to stdout
#
#  Location  : openmetadata/ingestion/get_om_token.py
#  Repository: https://github.ibm.com/alexander/ibmas-watsonxdata-dbt
#  Project   : watsonx.data · dbt · Spark medallion demo
#  Author    : Alexander Seelert
#  Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.
# -----------------------------------------------------------------------------
"""Mint a fresh OpenMetadata `ingestion-bot` JWT and print it to stdout.

WHAT & WHY
  The demo ships dbt lineage/metadata into a local OpenMetadata (OM) instance.
  OM's ingestion API needs a bearer token for the built-in `ingestion-bot`
  service account. Rather than hard-coding a long-lived token in config, this
  helper logs in as the OM admin, looks up the ingestion-bot's user id, and
  asks OM to generate a short-lived (1 hour) JWT for it. The token is the ONLY
  thing written to stdout so a caller can capture it directly, e.g.:
      OM_TOKEN="$(python3 openmetadata/ingestion/get_om_token.py)"
  All progress/diagnostic breadcrumbs ([OK]/[FAIL]) go to STDERR precisely so
  they never contaminate the captured token on stdout.

WHEN TO RUN (demo flow)
  Run just before kicking off an OpenMetadata ingestion (dbt metadata/lineage),
  to obtain a valid bearer token. A local OpenMetadata stack must already be up
  and reachable at OM_BASE, with the default `admin@open-metadata.org` account
  and the standard `ingestion-bot` present (both ship with OM).

CONFIGURATION (no env vars)
  Values are constants inside the file rather than env vars:
    OM_BASE       OpenMetadata base URL (http://localhost:8585)
    HTTP_TIMEOUT  per-request timeout in seconds (30) so an unreachable OM
                  can't block forever
  The admin credentials are the OM defaults (admin / "admin"); OM 1.13+ requires
  the password to be Base-64 encoded on login, which this script handles.

PREREQUISITES
  No oc/cpdctl login needed. Requires the `requests` library and a running,
  reachable OpenMetadata at OM_BASE.

USAGE
      python3 openmetadata/ingestion/get_om_token.py        # prints JWT to stdout
      TOKEN="$(python3 openmetadata/ingestion/get_om_token.py)"

SIDE EFFECTS & EXIT
  Generates (server-side) a 1-hour JWT for the ingestion-bot user. No files are
  written. Prints the token to stdout and exits 0 on success; on any HTTP/login
  failure it prints a [FAIL] line to stderr and exits 1.
"""
import base64
import requests
import sys

OM_BASE = "http://localhost:8585"
# Bound every HTTP call so a hung/unreachable OpenMetadata can't block forever.
HTTP_TIMEOUT = 30

# Progress goes to STDERR so the printed JWT stays the ONLY thing on stdout
# (callers capture stdout to grab the token).
try:
    # OM 1.13+ requires the password to be Base-64 encoded
    print(f"[get_om_token] Logging in to OpenMetadata at {OM_BASE} as admin...", file=sys.stderr)
    password_b64 = base64.b64encode(b"admin").decode()
    resp = requests.post(
        f"{OM_BASE}/api/v1/users/login",
        json={"email": "admin@open-metadata.org", "password": password_b64},
        timeout=HTTP_TIMEOUT,
    )
    resp.raise_for_status()
    admin_token = resp.json()["accessToken"]
    print("[get_om_token] Admin login succeeded [OK]", file=sys.stderr)
except Exception as e:
    print(f"[get_om_token] Error logging in: {e} [FAIL]", file=sys.stderr)
    sys.exit(1)

try:
    # Get the ingestion-bot entity to find its user ID
    print("[get_om_token] Looking up the 'ingestion-bot' user id...", file=sys.stderr)
    resp = requests.get(
        f"{OM_BASE}/api/v1/bots/name/ingestion-bot",
        headers={"Authorization": f"Bearer {admin_token}"},
        timeout=HTTP_TIMEOUT,
    )
    resp.raise_for_status()
    bot_data = resp.json()
    bot_user_id = bot_data["botUser"]["id"]
    print(f"[get_om_token] Found ingestion-bot user id {bot_user_id}", file=sys.stderr)

    # Generate / retrieve a JWT token for the bot user (OM 1.13+ API)
    print("[get_om_token] Generating a JWT token for the ingestion-bot...", file=sys.stderr)
    resp2 = requests.put(
        f"{OM_BASE}/api/v1/users/generateToken/{bot_user_id}",
        headers={"Authorization": f"Bearer {admin_token}", "Content-Type": "application/json"},
        json={"JWTTokenExpiry": "OneHour"},
        timeout=HTTP_TIMEOUT,
    )
    resp2.raise_for_status()
    token_data = resp2.json()
    token = token_data.get("JWTToken") or token_data.get("jwtToken") or token_data.get("token")
    if not token:
        raise KeyError(f"No token key found in response: {list(token_data.keys())}")
    print("[get_om_token] JWT token generated [OK]", file=sys.stderr)
    print(token)
except Exception as e:
    print(f"[get_om_token] Error fetching ingestion-bot token: {e} [FAIL]", file=sys.stderr)
    sys.exit(1)
