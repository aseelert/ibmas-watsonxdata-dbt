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
