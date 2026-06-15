import base64
import requests
import sys

try:
    # OM 1.13+ requires the password to be Base-64 encoded
    password_b64 = base64.b64encode(b"admin").decode()
    resp = requests.post(
        "http://localhost:8585/api/v1/users/login",
        json={"email": "admin@open-metadata.org", "password": password_b64},
    )
    resp.raise_for_status()
    admin_token = resp.json()["accessToken"]
except Exception as e:
    print(f"Error logging in: {e}", file=sys.stderr)
    sys.exit(1)

try:
    # Get the ingestion-bot entity to find its user ID
    resp = requests.get(
        "http://localhost:8585/api/v1/bots/name/ingestion-bot",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    resp.raise_for_status()
    bot_data = resp.json()
    bot_user_id = bot_data["botUser"]["id"]

    # Generate / retrieve a JWT token for the bot user (OM 1.13+ API)
    resp2 = requests.put(
        f"http://localhost:8585/api/v1/users/generateToken/{bot_user_id}",
        headers={"Authorization": f"Bearer {admin_token}", "Content-Type": "application/json"},
        json={"JWTTokenExpiry": "OneHour"},
    )
    resp2.raise_for_status()
    token_data = resp2.json()
    token = token_data.get("JWTToken") or token_data.get("jwtToken") or token_data.get("token")
    if not token:
        raise KeyError(f"No token key found in response: {list(token_data.keys())}")
    print(token)
except Exception as e:
    print(f"Error fetching ingestion-bot token: {e}", file=sys.stderr)
    sys.exit(1)
