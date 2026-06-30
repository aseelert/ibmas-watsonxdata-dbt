#!/usr/bin/env python3
# -----------------------------------------------------------------------------
#  create_datastage_flow.py — author the Confluent GOLD DataStage flow in CP4D
#
#  Location  : confluent/scripts/create_datastage_flow.py
#  Repository: https://github.com/aseelert/ibmas-watsonxdata-dbt
#  Project   : watsonx.data · dbt · Spark · Confluent medallion demo
#  Author    : Alexander Seelert — IBM Customer Success Engineer
#  Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.
#
#  Changelog :
#    v1.0 (2026-06-26) — Initial version. Reads the parameterized flow template
#      (confluent/datastage/confluent_gold_flow.json), substitutes env-driven
#      placeholders, and POSTs it to the CP4D DataStage flows API. Optionally
#      compiles + triggers a job run.
# -----------------------------------------------------------------------------
"""Create (and optionally run) the Confluent GOLD DataStage flow in CP4D.

WHAT / WHY
  This is the DataStage alternative to the Spark gold engine. When
  CONFLUENT_GOLD_ENGINE=datastage, the Confluent gold marts
  (confluent_gold_daily_sales, _category_performance, _customer_360) are built
  by an IBM DataStage flow in the CP4D project "ibmas-ingest-demo" instead of a
  watsonx.data Spark job. Both engines read the SAME Flink-written
  confluent_demo_silver tables and write the SAME confluent_demo_gold marts, so
  the cross-engine parity contract (see confluent/NAMING.md) still holds.

  This script does the "author it" half: it loads the parameterized flow
  template, fills in project id / catalog / schema names / connection asset from
  .env, and POSTs it to the DataStage flows REST API.

  ┌──────────────────────────────────────────────────────────────────────────┐
  │  THIS NEEDS A LIVE DATASTAGE SERVICE.                                       │
  │  The DataStage flows API only exists on a CP4D cluster with the DataStage  │
  │  cartridge installed. There is no way to validate the POST offline, so by  │
  │  default this script runs in --dry-run mode and only PRINTS the request it │
  │  would send. Pass --apply to actually create the flow on the cluster, and  │
  │  --run to also compile and trigger a job run afterwards.                    │
  └──────────────────────────────────────────────────────────────────────────┘

AUTH (same pattern as scripts/get_token.py — read for reference, not edited)
  1. POST https://<WXD_CPD_HOST>/icp4d-api/v1/authorize
       body {"username": WXD_CPD_USERNAME, "api_key": WXD_API_KEY}  → bearer token
  2. Fallback to {"username", "password": WXD_CPD_PASSWORD} if no/!api_key.
  The bearer token is then sent as  Authorization: Bearer <token>  on every
  DataStage API call.

REST ENDPOINTS USED
  POST /data_intg/v3/data_intg_flows?project_id=<id>            (create flow)
  POST /data_intg/v3/ds_codegen/compile/<flow_id>?project_id=.. (compile, --run)
  POST /v2/jobs?project_id=<id>                                 (create job, --run)
  POST /v2/jobs/<job_id>/runs?project_id=<id>                   (start run, --run)

ENV VARS IT READS (from .env via python-dotenv)
  WXD_CPD_HOST (required), WXD_CPD_AUTH_URL (optional — derived from host),
  WXD_CPD_USERNAME (default cpadmin), WXD_API_KEY (primary cred) /
  WXD_CPD_PASSWORD (fallback), WXD_SSL_VERIFY (True/False/CA-path),
  WXD_DATASTAGE_PROJECT_ID (required), WXD_DATASTAGE_PROJECT_NAME (label only),
  WXD_CATALOG (default iceberg_data),
  CONFLUENT_SILVER_SCHEMA (default confluent_demo_silver),
  CONFLUENT_GOLD_SCHEMA (default confluent_demo_gold),
  WXD_DATASTAGE_CONNECTION_REF (guid of the watsonx.data connection asset —
    optional; if absent a clearly-marked placeholder is used and --apply warns),
  WXD_DATASTAGE_CONNECTION_NAME (display name of that connection asset).

USAGE
    # 1. Preview the exact request (no network calls beyond nothing):
    python confluent/scripts/create_datastage_flow.py            # dry-run default
    python confluent/scripts/create_datastage_flow.py --dry-run

    # 2. Actually create the flow on a live cluster:
    python confluent/scripts/create_datastage_flow.py --apply

    # 3. Create, compile, and trigger a job run:
    python confluent/scripts/create_datastage_flow.py --apply --run

EXIT
  0 on success (including a successful dry-run preview); non-zero on missing
  env / template, auth failure, or any DataStage API error.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

# --- dependencies (requests + python-dotenv are already in requirements.txt) ---
try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None  # tolerate absence; we warn below

try:
    import requests
    from requests.packages.urllib3.exceptions import InsecureRequestWarning
    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
except ImportError:
    raise SystemExit("Missing dependency 'requests'. Run: pip install requests")


# Repo layout: confluent/scripts/ -> parents[2] is the repo root.
ROOT = Path(__file__).resolve().parents[2]
ENV_FILE = ROOT / ".env"
FLOW_TEMPLATE = ROOT / "confluent" / "datastage" / "confluent_gold_flow.json"

FLOW_NAME = "confluent_gold_daily_sales"


# ---------------------------------------------------------------------------
# Tiny logging helpers (stderr so stdout can stay clean for the JSON preview)
# ---------------------------------------------------------------------------
def info(msg: str) -> None:
    print(f"[INFO] {msg}", file=sys.stderr)


def ok(msg: str) -> None:
    print(f"[OK]   {msg}", file=sys.stderr)


def warn(msg: str) -> None:
    print(f"[WARN] {msg}", file=sys.stderr)


def err(msg: str) -> None:
    print(f"[ERR]  {msg}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Env helpers (same conventions as scripts/get_token.py)
# ---------------------------------------------------------------------------
def _env(name: str, default: str | None = None, required: bool = False) -> str:
    value = os.getenv(name, default)
    if required and not value:
        raise SystemExit(
            f"Missing required env var: {name}\n"
            f"  Copy .env.example to .env and fill in your values."
        )
    return value or ""


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
        warn(f"SSL cert file not found: {path} — disabling verification")
        return False
    return str(path)


# ---------------------------------------------------------------------------
# Auth — mirror of scripts/get_token.py (api_key first, password fallback)
# ---------------------------------------------------------------------------
def get_bearer_token(cpd_host: str, username: str, verify: bool | str) -> str:
    auth_url = os.getenv("WXD_CPD_AUTH_URL", "").strip()
    if not auth_url:
        auth_url = f"https://{cpd_host}/icp4d-api/v1/authorize"

    api_key = os.getenv("WXD_API_KEY", "").strip()
    if api_key:
        info("Authenticating with API key ...")
        resp = requests.post(
            auth_url, json={"username": username, "api_key": api_key},
            verify=verify, timeout=30,
        )
        if resp.status_code == 200:
            token = resp.json().get("token")
            if token:
                ok("API key auth succeeded")
                return token
        warn(f"API key auth rejected ({resp.status_code}) — trying password")

    password = os.getenv("WXD_CPD_PASSWORD", "").strip()
    if not password:
        raise SystemExit(
            "Auth failed: no usable WXD_API_KEY and no WXD_CPD_PASSWORD set.\n"
            "  Set one in .env, or run: python scripts/get_token.py --refresh-key"
        )
    info("Authenticating with password ...")
    resp = requests.post(
        auth_url, json={"username": username, "password": password},
        verify=verify, timeout=30,
    )
    if resp.status_code == 200 and resp.json().get("token"):
        ok("Password auth succeeded")
        return resp.json()["token"]
    raise SystemExit(f"Password login failed ({resp.status_code}): {resp.text}")


# ---------------------------------------------------------------------------
# Template loading + placeholder substitution
# ---------------------------------------------------------------------------
def load_and_render_flow() -> dict:
    """Load the JSON template and substitute @@PLACEHOLDER@@ tokens from .env."""
    if not FLOW_TEMPLATE.exists():
        raise SystemExit(f"Flow template not found: {FLOW_TEMPLATE}")

    raw = FLOW_TEMPLATE.read_text(encoding="utf-8")

    conn_ref = _env("WXD_DATASTAGE_CONNECTION_REF", "")
    conn_name = _env("WXD_DATASTAGE_CONNECTION_NAME", "watsonx.data Presto connection")
    if not conn_ref:
        conn_ref = "REPLACE_WITH_CONNECTION_ASSET_GUID"
        warn(
            "WXD_DATASTAGE_CONNECTION_REF is not set. A placeholder guid is used; "
            "the flow will NOT run until you create a watsonx.data connection asset "
            "in the project and set this var (see confluent/datastage/README.md)."
        )

    substitutions = {
        "@@PROJECT_ID@@":      _env("WXD_DATASTAGE_PROJECT_ID", required=True),
        "@@CATALOG@@":         _env("WXD_CATALOG", "iceberg_data"),
        "@@SILVER_SCHEMA@@":   _env("CONFLUENT_SILVER_SCHEMA", "confluent_demo_silver"),
        "@@GOLD_SCHEMA@@":     _env("CONFLUENT_GOLD_SCHEMA", "confluent_demo_gold"),
        "@@CONNECTION_REF@@":  conn_ref,
        "@@CONNECTION_NAME@@": conn_name,
    }
    for token, value in substitutions.items():
        raw = raw.replace(token, value)

    leftover = [t for t in substitutions if t in raw]
    if leftover:
        warn(f"Unsubstituted placeholders remain: {leftover}")

    flow = json.loads(raw)
    # The leading "_header" key is documentation only; strip it before POSTing
    # so the payload is a clean pipeline-flow document.
    flow.pop("_header", None)
    return flow


def build_payload(flow: dict) -> dict:
    """Wrap the pipeline-flow graph in the data_intg_flows request envelope."""
    return {
        "entity": {
            "data_intg_flow": {
                "name": FLOW_NAME,
                "description": (
                    "Confluent GOLD builder (DataStage engine). Functional twin of "
                    "the Spark gold job — same silver in, same confluent_demo_gold out."
                ),
            }
        },
        "attachments": [flow],
    }


# ---------------------------------------------------------------------------
# DataStage REST calls
# ---------------------------------------------------------------------------
def create_flow(cpd_host: str, token: str, project_id: str, payload: dict,
                verify: bool | str) -> str:
    url = f"https://{cpd_host}/data_intg/v3/data_intg_flows"
    info(f"POST {url}?project_id={project_id}")
    resp = requests.post(
        url, params={"project_id": project_id},
        headers={"Authorization": f"Bearer {token}",
                 "Content-Type": "application/json"},
        json=payload, verify=verify, timeout=120,
    )
    if resp.status_code not in (200, 201):
        raise SystemExit(
            f"Flow creation failed ({resp.status_code}): {resp.text}\n"
            f"  Tip: confirm the DataStage cartridge is installed and the project id is correct."
        )
    body = resp.json()
    flow_id = (body.get("metadata", {}) or {}).get("asset_id") or body.get("id", "")
    ok(f"Flow created: {FLOW_NAME} (asset_id={flow_id or 'unknown'})")
    return flow_id


def compile_and_run(cpd_host: str, token: str, project_id: str, flow_id: str,
                    verify: bool | str) -> None:
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    info("Compiling flow ...")
    c_url = f"https://{cpd_host}/data_intg/v3/ds_codegen/compile/{flow_id}"
    c_resp = requests.post(c_url, params={"project_id": project_id},
                           headers=headers, verify=verify, timeout=180)
    if c_resp.status_code not in (200, 201):
        raise SystemExit(f"Compile failed ({c_resp.status_code}): {c_resp.text}")
    ok("Compile succeeded")

    info("Creating job ...")
    job_payload = {
        "job": {
            "asset_ref": flow_id,
            "name": f"{FLOW_NAME}_job",
            "description": "Auto-created by create_datastage_flow.py --run",
            "configuration": {"env_type": "dsxlocal-px"},
        }
    }
    j_url = f"https://{cpd_host}/v2/jobs"
    j_resp = requests.post(j_url, params={"project_id": project_id},
                           headers=headers, json=job_payload, verify=verify, timeout=120)
    if j_resp.status_code not in (200, 201):
        raise SystemExit(f"Job creation failed ({j_resp.status_code}): {j_resp.text}")
    job_id = (j_resp.json().get("metadata", {}) or {}).get("asset_id", "")
    ok(f"Job created (asset_id={job_id or 'unknown'})")

    info("Starting job run ...")
    r_url = f"https://{cpd_host}/v2/jobs/{job_id}/runs"
    r_resp = requests.post(r_url, params={"project_id": project_id},
                           headers=headers, json={"job_run": {}},
                           verify=verify, timeout=120)
    if r_resp.status_code not in (200, 201):
        raise SystemExit(f"Job run failed to start ({r_resp.status_code}): {r_resp.text}")
    ok("Job run started — monitor it in the CP4D DataStage UI or via /v2/jobs/<id>/runs")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(
        description="Author (and optionally run) the Confluent GOLD DataStage flow in CP4D.",
        epilog="By default this is a DRY RUN that only prints the request. Use --apply to call the live service.",
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Print the rendered flow + request and exit (DEFAULT behavior).")
    parser.add_argument("--apply", action="store_true",
                        help="Actually POST the flow to the live DataStage service.")
    parser.add_argument("--run", action="store_true",
                        help="With --apply: also compile the flow, create a job, and start a run.")
    parser.add_argument("--env-file", default=str(ENV_FILE),
                        help="Path to .env (default: repo root .env).")
    args = parser.parse_args()

    # --run implies an apply; --apply turns the default dry-run off.
    do_apply = args.apply and not args.dry_run
    if args.run and not args.apply:
        warn("--run requires --apply; nothing will be sent. Re-run with: --apply --run")

    # --- load env ---
    env_path = Path(args.env_file).expanduser()
    if load_dotenv is None:
        warn("python-dotenv not installed — relying on already-exported environment.")
    elif env_path.exists():
        load_dotenv(env_path)
        info(f"Loaded env from {env_path}")
    else:
        warn(f".env not found at {env_path} — relying on the current environment.")

    try:
        # --- render the flow (validates placeholders + required project id) ---
        flow = load_and_render_flow()
        payload = build_payload(flow)
        project_id = _env("WXD_DATASTAGE_PROJECT_ID", required=True)
        project_name = _env("WXD_DATASTAGE_PROJECT_NAME", "ibmas-ingest-demo")
        gold_schema = _env("CONFLUENT_GOLD_SCHEMA", "confluent_demo_gold")

        info(f"Target project : {project_name} ({project_id})")
        info(f"Target marts   : {gold_schema}.confluent_gold_daily_sales (+ 2 placeholders)")

        if not do_apply:
            info("DRY RUN — the following request would be sent (no network call made):")
            print(json.dumps({
                "method": "POST",
                "url": "https://<WXD_CPD_HOST>/data_intg/v3/data_intg_flows",
                "query": {"project_id": project_id},
                "body": payload,
            }, indent=2))
            warn("This is a preview only. Re-run with --apply against a live CP4D + "
                 "DataStage cluster to actually create the flow.")
            return 0

        # --- live path ---
        cpd_host = _env("WXD_CPD_HOST", required=True)
        username = _env("WXD_CPD_USERNAME", "cpadmin")
        verify = _ssl_verify()

        warn("LIVE MODE: this requires a reachable CP4D cluster with the DataStage "
             "cartridge installed. If anything below 404s, the service is likely absent.")
        token = get_bearer_token(cpd_host, username, verify)
        flow_id = create_flow(cpd_host, token, project_id, payload, verify)

        if args.run:
            if not flow_id:
                raise SystemExit("Cannot run: the create call did not return a flow asset_id.")
            compile_and_run(cpd_host, token, project_id, flow_id, verify)

        ok("Done.")
        return 0

    except SystemExit:
        raise
    except Exception as exc:  # noqa: BLE001 — top-level guard, log with context
        err(f"Unexpected failure: {exc.__class__.__name__}: {exc}")
        import traceback
        traceback.print_exc(file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
