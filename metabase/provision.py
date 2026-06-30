#!/usr/bin/env python3
# -----------------------------------------------------------------------------
#  provision.py — auto-provision Metabase against the watsonx.data Presto catalog
#
#  Location  : metabase/provision.py
#  Repository: https://github.com/aseelert/ibmas-watsonxdata-dbt
#  Project   : watsonx.data · dbt · Spark medallion demo
#  Author    : Alexander Seelert
#  Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.
# -----------------------------------------------------------------------------
"""Auto-provision Metabase for the watsonx.data medallion demo.

WHAT & WHY
  Metabase is the BI front-end of the demo: it lets an audience browse and
  chart the bronze/silver/gold medallion tables that dbt + Spark build in the
  watsonx.data Iceberg catalog. This script removes the manual click-through
  of Metabase's first-run wizard so the BI layer is "ready to demo" the moment
  the container is healthy — no copy/paste of credentials, everything sourced
  straight from `.env`.

  Concretely it:
    1. Creates the first admin user (MB_SETUP_EMAIL / MB_SETUP_PASSWORD).
    2. Adds the watsonx.data Presto data source so the `iceberg_data` catalog
       and its medallion schemas are browsable immediately.

  It is fully IDEMPOTENT: if Metabase is already set up it just logs in and
  ensures the Presto data source exists, so re-running it is always safe.

WHEN TO RUN (demo flow)
  Run once after Metabase is healthy — the `metabase-provision` service in
  docker-compose-metabase.yml invokes this automatically. The watsonx.data
  Presto engine must be running/resumed, because Metabase validates the
  connection synchronously when the data source is added; this script retries
  patiently to tolerate an engine that is still warming up or resuming.

ENV VARS READ
  Metabase side:
    MB_URL              base URL of Metabase            (default http://metabase:3000)
    MB_SETUP_EMAIL      admin email to create / log in  (default admin@admin.com)
    MB_SETUP_PASSWORD   admin password                  (default admin12345)
    MB_SITE_NAME        Metabase site name              (default "watsonx.data medallion demo")
    MB_DB_NAME          display name of the data source (default "watsonx.data (Presto)")
    MB_DB_ADD_ATTEMPTS  max add-connection retries      (default 20)
    MB_DB_ADD_INTERVAL  seconds between retries          (default 15)
    MB_DB_ADD_DEADLINE  overall wall-clock cap, seconds (default 300)
  watsonx.data side (used to build the Presto connection):
    WXD_HOST            Presto host (required)
    WXD_PORT            Presto port                     (default 443)
    WXD_CATALOG         catalog to attach               (default iceberg_data)
    WXD_USER            ibmlhapikey_<user>              (default ibmlhapikey_cpadmin)
    WXD_API_KEY         API key used as the password (required)
    WXD_INSTANCE_ID     injected as the LhInstanceId HTTP header (optional)
    WXD_METABASE_SCHEMA optional default schema to scope to

WATSONX.DATA SPECIFICS HANDLED HERE
  * user  = WXD_USER (ibmlhapikey_<user>), password = WXD_API_KEY.
  * The required `LhInstanceId` HTTP header is injected through the PrestoDB
    JDBC driver's `customHeaders` option (URL-encoded `Name:Value`).
  * TLS verification is satisfied by the CA the entrypoint trusts at boot.

PREREQUISITES
  No oc/cpdctl login needed. Requires a reachable Metabase (the compose stack)
  and a reachable, resumed watsonx.data Presto engine. Python stdlib only.

USAGE
  Normally invoked by the compose `metabase-provision` service. To run by hand
  (with the same env loaded):
      python3 metabase/provision.py
  If the engine was asleep and the add gave up, just re-run the stack — it is
  idempotent:
      docker compose -f docker-compose-metabase.yml up -d

SIDE EFFECTS & EXIT
  Creates a Metabase admin user (first run) and registers a Presto data source
  via the Metabase HTTP API. Exits 0 on success (or when nothing was needed);
  calls sys.exit() with a diagnostic message if Metabase never becomes healthy,
  login fails, or the Presto data source could not be added before the deadline.
"""
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

MB = os.environ.get("MB_URL", "http://metabase:3000")
EMAIL = os.environ.get("MB_SETUP_EMAIL", "admin@admin.com")
PASSWORD = os.environ.get("MB_SETUP_PASSWORD", "admin12345")
SITE_NAME = os.environ.get("MB_SITE_NAME", "watsonx.data medallion demo")
DB_NAME = os.environ.get("MB_DB_NAME", "watsonx.data (Presto)")


def req(path, data=None, headers=None, method=None):
    url = MB + path
    body = json.dumps(data).encode() if data is not None else None
    hdrs = {"Content-Type": "application/json"}
    if headers:
        hdrs.update(headers)
    request = urllib.request.Request(url, data=body, headers=hdrs, method=method)
    with urllib.request.urlopen(request, timeout=30) as resp:
        raw = resp.read().decode()
        return json.loads(raw) if raw else {}


def wait_for_metabase():
    # Wall-clock cap so a never-healthy Metabase can't loop ~70 min (120 x (5s +
    # 30s request timeout)). 5 min is plenty for a local container to come up.
    deadline = time.time() + 300
    attempt = 0
    while time.time() < deadline:
        try:
            print(f"[provision] checking Metabase health at {MB} (attempt {attempt})...")
            req("/api/health")
            print("[provision] Metabase is healthy [OK]")
            return
        except Exception as exc:  # noqa: BLE001 - any failure means "not ready yet"
            remaining = int(deadline - time.time())
            print(f"[provision] waiting for Metabase ({attempt}, ~{remaining}s left)... {exc}")
            time.sleep(5)
            attempt += 1
    sys.exit("[provision] Metabase never became healthy within 300s")


def get_session():
    """Return a Metabase session id, creating the admin user on first run.

    Idempotent: if Metabase is already set up we just log in, so re-running the
    provisioner still ensures the Presto data source exists.
    """
    props = req("/api/session/properties")
    # Metabase keeps returning a setup-token even after the first user exists,
    # so gate on has-user-setup (the authoritative "is setup done?" flag).
    if not props.get("has-user-setup"):
        token = props.get("setup-token")
        setup_payload = {
            "token": token,
            "user": {
                "first_name": "Admin",
                "last_name": "User",
                "email": EMAIL,
                "password": PASSWORD,
                "site_name": SITE_NAME,
            },
            "prefs": {"site_name": SITE_NAME, "allow_tracking": False},
        }
        try:
            session = req("/api/setup", setup_payload).get("id")
        except urllib.error.HTTPError as exc:
            sys.exit(f"[provision] setup failed: {exc.read().decode()}")
        print(f"[provision] admin user created: {EMAIL}")
        return session

    # Already set up — log in so we can verify/add the data source.
    try:
        session = req("/api/session", {"username": EMAIL, "password": PASSWORD}).get("id")
    except urllib.error.HTTPError as exc:
        sys.exit(
            "[provision] Metabase is already set up but login failed for "
            f"{EMAIL} — check MB_SETUP_EMAIL/MB_SETUP_PASSWORD: {exc.read().decode()}"
        )
    print(f"[provision] logged in as existing admin: {EMAIL}")
    return session


def database_exists(session):
    resp = req("/api/database", headers={"X-Metabase-Session": session})
    databases = resp.get("data", resp) if isinstance(resp, dict) else resp
    return any(db.get("name") == DB_NAME for db in databases)


def main():
    wait_for_metabase()
    session = get_session()

    if database_exists(session):
        print(f"[provision] data source '{DB_NAME}' already exists — nothing to do.")
        return

    # Build the Presto connection straight from the WXD_* env values.
    instance_id = os.environ.get("WXD_INSTANCE_ID", "").strip()
    details = {
        "host": os.environ["WXD_HOST"],
        "port": int(os.environ.get("WXD_PORT", "443")),
        "catalog": os.environ.get("WXD_CATALOG", "iceberg_data"),
        "user": os.environ.get("WXD_USER", "ibmlhapikey_cpadmin"),
        "password": os.environ["WXD_API_KEY"],
        "ssl": True,
    }
    schema = os.environ.get("WXD_METABASE_SCHEMA", "").strip()
    if schema:
        details["schema"] = schema
    if instance_id:
        # watsonx.data routes by the LhInstanceId HTTP header; the PrestoDB JDBC
        # driver carries it via the URL-encoded `customHeaders` option.
        header = urllib.parse.quote(f"LhInstanceId:{instance_id}", safe="")
        details["additional-options"] = f"customHeaders={header}"

    db_payload = {
        "engine": "presto-jdbc",
        "name": DB_NAME,
        "details": details,
        "is_full_sync": True,
    }

    # Metabase validates the connection synchronously when adding it, so this
    # call only succeeds once the watsonx.data Presto engine can actually answer
    # a query. Two transient conditions are common on a fresh start and BOTH
    # self-heal with retries:
    #   * Metabase's own 10s validation timeout on the first (cold) query.
    #   * The Presto engine still warming up / resuming — it returns HTTP 500
    #     "authenticator was not loaded" until it is fully ready.
    # We therefore retry patiently (default ~5 min) so a resuming engine is
    # tolerated. database_exists() guards against a double-add if an earlier
    # attempt actually landed. If the engine never wakes, re-running `up -d`
    # later is safe and idempotent.
    headers = {"X-Metabase-Session": session}
    attempts = int(os.environ.get("MB_DB_ADD_ATTEMPTS", "20"))
    interval = int(os.environ.get("MB_DB_ADD_INTERVAL", "15"))
    # Overall wall-clock cap so the retry loop (20 x ~15s + per-request timeouts,
    # ~15 min) can't blow the 10-min budget. Configurable for slow engines.
    db_add_deadline = time.time() + int(os.environ.get("MB_DB_ADD_DEADLINE", "300"))
    print(
        f"[provision] adding Presto data source '{DB_NAME}' "
        f"({details['host']}:{details['port']}, up to {attempts} attempts / "
        f"~{int(db_add_deadline - time.time())}s)..."
    )
    for attempt in range(1, attempts + 1):
        try:
            print(f"[provision] add attempt {attempt}/{attempts} -> {MB}/api/database")
            created = req("/api/database", db_payload, headers=headers)
            print(
                f"[provision] data source '{created.get('name')}' "
                f"(id={created.get('id')}) connected to catalog "
                f"'{details['catalog']}'. [OK]"
            )
            print("[provision] done — open http://localhost:3000")
            return
        except urllib.error.HTTPError as exc:
            body = exc.read().decode()
            if database_exists(session):  # a previous attempt actually landed it
                print(f"[provision] data source '{DB_NAME}' already created — done.")
                return
            print(f"[provision] add attempt {attempt}/{attempts} failed: {body}")
            if time.time() >= db_add_deadline:
                print("[provision] overall deadline reached — stopping retries.")
                break
            if attempt < attempts:
                print(f"[provision] retrying in {interval}s...")
                time.sleep(interval)
    sys.exit(
        "[provision] gave up adding the Presto data source. The watsonx.data "
        "Presto engine was not answering queries (often: suspended/resuming). "
        "Start the engine, then re-run: "
        "docker compose -f docker-compose-metabase.yml up -d"
    )


if __name__ == "__main__":
    main()
