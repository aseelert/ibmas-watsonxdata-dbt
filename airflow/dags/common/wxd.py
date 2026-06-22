# -----------------------------------------------------------------------------
#  wxd.py — Shared watsonx.data auth/TLS/connection helpers for the Airflow DAGs
#
#  Location  : airflow/dags/common/wxd.py
#  Repository: https://github.ibm.com/alexander/ibmas-watsonxdata-dbt
#  Project   : watsonx.data · dbt · Spark medallion demo
#  Author    : Alexander Seelert
#  Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.
# -----------------------------------------------------------------------------
"""Shared watsonx.data auth/TLS/connection helpers for the Airflow DAGs.

This is the SINGLE place where authentication, TLS, and connection logic lives
for the Airflow layer — it mirrors the behaviour of the standalone scripts
(scripts/get_token.py, submit_spark_application.py, bootstrap_watsonxdata.py,
query_gold.py) so we never duplicate that logic across DAGs.

Every value is read from environment variables that originate in .env (loaded
into every container by docker-compose `env_file`). Nothing is hard-coded.

Auth strategy (same as the scripts):
  * Spark REST  -> a fresh CPD **bearer token** minted from WXD_API_KEY on each
    run (long-lived API key -> short-lived token). Robust for scheduled runs.
  * Presto/dbt  -> HTTP **BasicAuth** with user=WXD_USER, password=WXD_API_KEY.

WHAT / WHY
  An importable helper module (no shebang — it is never executed directly). Both
  Airflow DAGs (dag_dbt_medallion.py via dbt's profile, dag_spark_medallion.py
  directly) import it so the watsonx.data connection contract is defined once.
  It provides three families of helpers: bearer-token / ZenApiKey auth for the
  Spark REST API, BasicAuth Presto DB-API connections + scalar queries for
  verification, and the Spark REST endpoint/engine resolvers.

WHEN
  Loaded by the scheduler whenever it parses or runs either DAG; there is no
  standalone entry point. No ordering requirement of its own — it simply has to
  import cleanly so the DAGs can be parsed.

ENV VARS it reads
  * WXD_PROJECT_DIR ........ mount point of the (read-only) repo inside the
                             container; the TLS CA cert is resolved under it.
  * WXD_SSL_VERIFY ......... True/False or a CA-cert path (default
                             certs/watsonxdata-ca.pem, resolved under the project).
  * WXD_INSTANCE_ID ........ value of the LhInstanceId header (required).
  * WXD_CPD_HOST ........... CPD/cluster host used to build the auth + Spark URLs.
  * WXD_CPD_USERNAME / WXD_USER ... CPD username (the ibmlhapikey_ prefix is
                             stripped for the bare-username forms).
  * WXD_CPD_AUTH_URL ....... override for the /icp4d-api/v1/authorize endpoint.
  * WXD_CPD_API_KEY / WXD_API_KEY ... long-lived API key minted into a token.
  * WXD_CPD_PASSWORD ....... optional password fallback for token minting.
  * WXD_SPARK_APPLICATIONS_ENDPOINT / WXD_SPARK_ENGINE_ID ... Spark REST target.
  * WXD_HOST / WXD_PORT / WXD_CATALOG ... Presto coordinator + Iceberg catalog.
  * WXD_PRESTO_REQUEST_TIMEOUT_SEC ... per-request Presto HTTP timeout (s).

PREREQUISITES
  No oc/cpdctl login needed — auth is purely via the API key over HTTPS. A
  reachable watsonx.data instance (CPD host + Presto coordinator) and the CA
  cert on disk (unless WXD_SSL_VERIFY is false). prestodb is imported lazily so
  the module parses even where that wheel is absent.

USAGE (from a DAG / task)
    from common import wxd
    hdr  = wxd.bearer_auth_header()           # 'Bearer <token>' for Spark REST
    conn = wxd.presto_connect(schema="...")   # Presto DB-API connection
    n    = wxd.presto_scalar("select count(*) from t")

SIDE EFFECTS / EXIT
  Makes outbound HTTPS calls (token mint, Presto queries) with bounded timeouts;
  opens Presto connections that callers must close. Helpers raise RuntimeError on
  missing env vars or failed auth — there is no sys.exit() (it is a library).
  Emits [wxd] breadcrumbs naming the host/endpoint being contacted.
"""

from __future__ import annotations

import base64
import os
from pathlib import Path

import requests

# The repo is bind-mounted read-only here by docker-compose.yml.
# The TLS cert (certs/watsonxdata-ca.pem) is read from this path — never copied.
PROJECT_DIR = Path(os.getenv("WXD_PROJECT_DIR", "/opt/airflow/project"))


# ---------------------------------------------------------------------------
# Environment / TLS
# ---------------------------------------------------------------------------

def env(name: str, default: str | None = None) -> str:
    """Return a required env var (or its default), raising if truly missing."""
    value = os.getenv(name, default)
    if value is None or value == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def ssl_verify() -> bool | str:
    """
    Resolve WXD_SSL_VERIFY the same way the scripts do, but relative to the
    mounted project dir so the existing cert is used in-place (no copy).

    Returns True/False for boolean values, or an absolute path to the CA cert.
    """
    value = os.getenv("WXD_SSL_VERIFY", "certs/watsonxdata-ca.pem").strip()
    if value.lower() in {"0", "false", "no"}:
        return False
    if value.lower() in {"1", "true", "yes"}:
        return True
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = PROJECT_DIR / path
    return str(path)


def instance_id() -> str:
    return env("WXD_INSTANCE_ID")


def cpd_host() -> str:
    return env("WXD_CPD_HOST")


# ---------------------------------------------------------------------------
# CPD username / API key (mirrors scripts/submit_spark_application.py)
# ---------------------------------------------------------------------------

def _cpd_username() -> str:
    if username := os.getenv("WXD_CPD_USERNAME"):
        return username
    user = os.getenv("WXD_USER", "ibmlhapikey_cpadmin")
    return user.removeprefix("ibmlhapikey_") if user.startswith("ibmlhapikey_") else user


# ---------------------------------------------------------------------------
# Auth headers
# ---------------------------------------------------------------------------

def mint_bearer_token() -> str:
    """
    POST the CPD authorize endpoint with the long-lived API key and return a
    fresh short-lived bearer token. Falls back to password auth if provided.
    Same call as scripts/get_token.py — re-minting each run keeps the scheduled
    DAG resilient to token expiry.
    """
    host = cpd_host()
    auth_url = os.getenv("WXD_CPD_AUTH_URL", f"https://{host}/icp4d-api/v1/authorize")
    username = _cpd_username()
    print(f"[wxd] minting bearer token from {auth_url} (user={username}) ...")
    api_key = os.getenv("WXD_CPD_API_KEY") or os.getenv("WXD_API_KEY")
    password = os.getenv("WXD_CPD_PASSWORD")

    payloads: list[dict] = []
    if api_key:
        payloads.append({"username": username, "api_key": api_key})
    if password:
        payloads.append({"username": username, "password": password})
    if not payloads:
        raise RuntimeError(
            "No CPD credentials found: set WXD_API_KEY (preferred) or WXD_CPD_PASSWORD."
        )

    last = None
    for payload in payloads:
        last = requests.post(auth_url, json=payload, verify=ssl_verify(), timeout=60)
        if last.status_code < 400:
            token = last.json().get("token") or last.json().get("access_token")
            if token:
                print(f"[wxd] [OK] bearer token minted (user={username})")
                return token
    print(f"[wxd] [FAIL] CPD auth failed ({last.status_code if last else 'n/a'})")
    raise RuntimeError(
        f"CPD auth failed ({last.status_code if last else 'n/a'}): "
        f"{last.text if last else 'no response'}"
    )


def bearer_auth_header() -> str:
    """Authorization header value for the Spark REST API: 'Bearer <token>'."""
    return f"Bearer {mint_bearer_token()}"


def zen_api_key() -> str:
    """
    'ZenApiKey base64(user:apikey)' — used INSIDE the Spark payload as
    spark.hadoop.wxd.apiKey so the Spark job can call back into watsonx.data.
    Mirrors _zen_auth_string() in scripts/submit_spark_application.py, which
    uses the BARE username (cpadmin), NOT the ibmlhapikey_ Presto form: the
    Spark engine rejects 'ibmlhapikey_cpadmin:<key>' and the app fails with
    return_code 1.
    """
    user = _cpd_username()
    key = env("WXD_API_KEY")
    encoded = base64.b64encode(f"{user}:{key}".encode()).decode("ascii")
    return f"ZenApiKey {encoded}"


# ---------------------------------------------------------------------------
# Spark REST endpoints
# ---------------------------------------------------------------------------

def spark_applications_endpoint() -> str:
    # Default mirrors scripts/submit_spark_application.py and .env.example (api/v3).
    # Normally WXD_SPARK_APPLICATIONS_ENDPOINT is set in .env and used verbatim.
    return env(
        "WXD_SPARK_APPLICATIONS_ENDPOINT",
        f"https://{cpd_host()}/lakehouse/api/v3/spark_engines/"
        f"{os.getenv('WXD_SPARK_ENGINE_ID', 'spark656')}/applications",
    )


def spark_engine_id() -> str:
    return os.getenv("WXD_SPARK_ENGINE_ID", "spark656")


# ---------------------------------------------------------------------------
# Presto connection (mirrors bootstrap_watsonxdata.py / query_gold.py)
# ---------------------------------------------------------------------------

def presto_connect(schema: str | None = None):
    """
    Open a Presto DB-API connection to watsonx.data using HTTP BasicAuth and the
    LhInstanceId header, exactly like the standalone Presto scripts.
    """
    import prestodb

    user = env("WXD_USER", "ibmlhapikey_cpadmin")
    password = env("WXD_API_KEY")
    host = env("WXD_HOST")
    port = int(env("WXD_PORT", "443"))
    print(f"[wxd] connecting to Presto {host}:{port} (catalog={env('WXD_CATALOG', 'iceberg_data')}"
          f"{f', schema={schema}' if schema else ''}) ...")
    conn = prestodb.dbapi.connect(
        host=host,
        port=port,
        user=user,
        catalog=env("WXD_CATALOG", "iceberg_data"),
        schema=schema,
        http_scheme="https",
        http_headers={"LhInstanceId": instance_id()},
        auth=prestodb.auth.BasicAuthentication(user, password),
        # Bound every HTTP request so a stalled query can't hang the DAG task.
        request_timeout=int(os.getenv("WXD_PRESTO_REQUEST_TIMEOUT_SEC", "60")),
    )
    conn._http_session.verify = ssl_verify()
    print(f"[wxd] [OK] Presto connection opened to {host}:{port}")
    return conn


def presto_scalar(sql: str, schema: str | None = None):
    """Run a query and return the first column of the first row (or None)."""
    conn = presto_connect(schema=schema)
    try:
        cur = conn.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        conn.close()
