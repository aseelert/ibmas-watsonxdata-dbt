#!/usr/bin/env python3
# -----------------------------------------------------------------------------
#  prepare_watsonx_env.py — fully automated .env bootstrap for CPD watsonx.data
#
#  Location  : scripts/prepare_watsonx_env.py
#  Repository: https://github.com/aseelert/ibmas-watsonxdata-dbt
#  Project   : watsonx.data · dbt · Spark medallion demo
#  Author    : Alexander Seelert
#  Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.
# -----------------------------------------------------------------------------
"""Bootstrap .env for the CPD (on-prem OpenShift) watsonx.data medallion demo.

One command. All 40+ .env variables — no hand-copying.

BARE MINIMUM TO RUN
   Just run:
       python3 scripts/prepare_watsonx_env.py

   If .env is empty/missing, an interactive wizard asks 5 questions:
       1. OpenShift API URL      e.g. https://api.mycluster.example.org:6443
       2. OpenShift username     (default: kubeadmin)
       3. kubeadmin password     (only secret you must type — never stored in git)
       4. Presto connection JSON (paste from watsonx.data UI, or keep existing file)
       5. Spark connection JSON  (optional paste — press Enter to skip)

   Everything else — all 40+ .env values — is auto-derived and written for you.

   If .env already has WXD_OC_PASSWORD and the JSONs are on disk, re-running
   the script will refresh tokens and re-discover all cluster values automatically.

   Opt-outs (rarely needed):
       --no-oc-login      use existing kubeconfig, skip  oc login
       --no-fetch-tokens  keep current WXD_API_KEY / WXD_SPARK_BEARER_TOKEN

HOW IT WORKS
  Step 0: oc login — reads WXD_OC_USER / WXD_OC_PASSWORD from .env and runs
              oc login <WXD_OPENSHIFT_API> -u <user> -p <password>
          (DEFAULT on; disable with --no-oc-login)
  Step 1: Parse the Presto JSON for host/port/engine-id/instance-id/SSL cert.
  Step 2: Write the embedded CA cert chain to certs/watsonxdata-ca.pem.
  Step 3: Derive every URL that can be computed from the JSON values:
            WXD_CPD_AUTH_URL, WXD_OPENSHIFT_CONSOLE, Spark endpoints, MinIO
            internal endpoint, PostgreSQL service address, S3 paths, etc.
  Step 4: Run oc to discover what cannot be derived:
            - WXD_OPENSHIFT_API (oc whoami --show-server)
            - WXD_SPARK_ENGINE_ID (wxdengine CRD, non-presto entry)
            - WXD_OBJECT_STORE_ACCESS_KEY / SECRET_KEY (ibm-lh-minio-secret)
            - PG_PASSWORD (ibmas-reporting-creds)
            - WXD_CPD_PASSWORD (platform-auth-idp-credentials/admin_password)
  Step 5: Reachability checks (informational, non-fatal).
  Step 6: Fetch tokens — log in with WXD_CPD_PASSWORD, rotate WXD_API_KEY,
            derive WXD_SPARK_BEARER_TOKEN.
          (DEFAULT on; disable with --no-fetch-tokens)

  Steps 4-6 are non-fatal — the script skips them gracefully with a warning
  when oc is not logged in or a secret is missing.

PREREQUISITES (CPD / on-prem OpenShift only, not SaaS)
  • watsonx_data/instance_details.json  — Presto connection JSON from the UI.
  • oc logged in to the CPD cluster  (or use --oc-login with WXD_OC_PASSWORD).
  • For --fetch-tokens: WXD_CPD_PASSWORD in .env (auto-written by Step 4).

USAGE
    python scripts/prepare_watsonx_env.py                  # normal run
    python scripts/prepare_watsonx_env.py --oc-login       # oc login first, then run
    python scripts/prepare_watsonx_env.py --dry-run        # show diff, write nothing
    python scripts/prepare_watsonx_env.py --overwrite      # replace existing non-secret values
    python scripts/prepare_watsonx_env.py --fetch-tokens   # also derive API key + bearer token
    python scripts/prepare_watsonx_env.py --oc-login --fetch-tokens  # fully automated
    python scripts/prepare_watsonx_env.py --presto-json /other/export.json
    python scripts/prepare_watsonx_env.py --spark-json watsonx_data/spark_details.json

WHAT IS WRITTEN WITH --fetch-tokens
    WXD_API_KEY            — rotated via POST /usermgmt/v1/user/apikey/regenerate
    WXD_SPARK_BEARER_TOKEN — fresh bearer token from the new API key

WHAT IS NEVER AUTO-WRITTEN (must be set once by hand in .env)
    WXD_OC_PASSWORD        — kubeadmin password (bcrypt hash in OCP, cannot decode)
    WXD_API_KEY            — without --fetch-tokens (generates a live credential)
    WXD_SPARK_BEARER_TOKEN — without --fetch-tokens (short-lived token)

SIDE EFFECTS / EXIT
  Creates / updates certs/watsonxdata-ca.pem and .env in place.
  Returns 0 on success; non-zero on a missing / invalid JSON.
"""

from __future__ import annotations

import argparse
import base64
import json
import logging
import os
import re
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
#  Logging — diagnostics go to stderr; the results table goes to stdout.
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s  %(message)s",
    stream=sys.stderr,
)
log = logging.getLogger("prepare_watsonx_env")

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PRESTO_JSON = ROOT / "watsonx_data" / "instance_details.json"
DEFAULT_SPARK_JSON  = ROOT / "watsonx_data" / "spark_details.json"
DEFAULT_ENV_FILE    = ROOT / ".env"
DEFAULT_CERT_FILE   = ROOT / "certs" / "watsonxdata-ca.pem"

# Variables that must NEVER be auto-written without explicit opt-in.
# --fetch-tokens overrides this for WXD_API_KEY + WXD_SPARK_BEARER_TOKEN only.
# WXD_OC_PASSWORD is also never auto-written (kubeadmin hash is bcrypt — unreadable).
_PROTECTED_SECRETS = {"WXD_API_KEY", "WXD_SPARK_BEARER_TOKEN", "WXD_OC_PASSWORD"}

# Sentinel values that mean "not yet set" in a freshly copied .env.example.
_PLACEHOLDER_PATTERNS = re.compile(
    r"^(replace-with-|<.*>|your-|todo|changeme|placeholder|example)",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
#  .env section layout — determines the order and grouping of keys when the
#  file is regenerated.  Keys not listed here are appended in an "Other" block.
# ---------------------------------------------------------------------------
_SECTION_MAP: dict[str, list[str]] = {
    "# -- OpenShift / CPD --------------------------------------------------------": [
        "WXD_OPENSHIFT_API",
        "WXD_OPENSHIFT_CONTEXT",
        "WXD_OPENSHIFT_NAMESPACE",
        "WXD_OPENSHIFT_CONSOLE",
        "WXD_OC_USER",
        "WXD_OC_PASSWORD",
        "WXD_CPD_HOST",
        "WXD_CPD_AUTH_URL",
        "WXD_CPD_USERNAME",
        "WXD_CPD_PASSWORD",
    ],
    "# -- Presto Engine -----------------------------------------------------------": [
        "WXD_INSTANCE_ID",
        "WXD_HOST",
        "WXD_PORT",
        "WXD_PRESTO_ENGINE_ID",
        "WXD_USER",
        "WXD_API_KEY",
        "WXD_SSL_VERIFY",
    ],
    "# -- Spark Engine ------------------------------------------------------------": [
        "WXD_SPARK_ENGINE_ID",
        "WXD_SPARK_ENGINE_ENDPOINT",
        "WXD_SPARK_APPLICATIONS_ENDPOINT",
        "WXD_SPARK_BEARER_TOKEN",
        "WXD_SPARK_DRY_RUN",
        "WXD_SPARK_CATALOG",
        "WXD_SPARK_SCHEMA",
        "WXD_SPARK_ASSET_BUCKET",
        "WXD_SPARK_ASSET_PREFIX",
        "WXD_SPARK_APPLICATION",
        "WXD_SPARK_INPUT_BASE",
    ],
    "# -- MinIO / Object Store ----------------------------------------------------": [
        "WXD_OBJECT_STORE_ENDPOINT",
        "WXD_OBJECT_STORE_INTERNAL_ENDPOINT",
        "WXD_OBJECT_STORE_ACCESS_KEY",
        "WXD_OBJECT_STORE_SECRET_KEY",
        "WXD_OBJECT_STORE_REGION",
        "WXD_OBJECT_STORE_SSL_VERIFY",
        "WXD_OBJECT_STORE_AUTO_PORT_FORWARD",
        "WXD_OBJECT_STORE_SERVICE",
        "WXD_OBJECT_STORE_SERVICE_PORT",
        "WXD_OBJECT_STORE_SECRET_NAME",
        "WXD_OBJECT_STORE_ACCESS_KEY_NAME",
        "WXD_OBJECT_STORE_SECRET_KEY_NAME",
    ],
    "# -- dbt / Catalog -----------------------------------------------------------": [
        "WXD_CATALOG",
        "WXD_SCHEMA",
        "WXD_INGEST_SCHEMA",
        "WXD_GOLD_MATERIALIZED",
    ],
    "# -- PostgreSQL (local Docker) -----------------------------------------------": [
        "PG_HOST",
        "PG_PORT",
        "PG_DATABASE",
        "PG_USER",
        "PG_PASSWORD",
        "PG_SSL_MODE",
        "PG_GOLD_SCHEMA",
        "PG_REPORTING_SCHEMA",
    ],
    "# -- Misc --------------------------------------------------------------------": [
        "WXD_DATASTAGE_PROJECT_NAME",
    ],
}

# Flat ordered list of all known keys (for use by _write_env).
_KNOWN_KEYS: list[str] = [k for keys in _SECTION_MAP.values() for k in keys]

# Inline comments appended after each key=value line in the written .env.
# Keep them brief — they appear on the same line as the value.
_KEY_COMMENTS: dict[str, str] = {
    # OpenShift / CPD
    "WXD_OPENSHIFT_API":               "auto-derived from oc whoami --show-server",
    "WXD_OPENSHIFT_CONTEXT":           "stable alias set by --oc-login (rename-context admin)",
    "WXD_OPENSHIFT_NAMESPACE":         "CPD namespace, derived from CPD host",
    "WXD_OPENSHIFT_CONSOLE":           "auto-derived from app domain",
    "WXD_OC_USER":                     "default: kubeadmin",
    "WXD_CPD_AUTH_URL":                "auto-derived: https://<cpd_host>/icp4d-api/v1/authorize",
    "WXD_CPD_USERNAME":                "default: cpadmin",
    # Presto Engine
    "WXD_INSTANCE_ID":                 "from Presto connection JSON",
    "WXD_HOST":                        "Presto engine host, from JSON",
    "WXD_PORT":                        "default: 443",
    "WXD_PRESTO_ENGINE_ID":            "from Presto connection JSON (e.g. presto653)",
    "WXD_USER":                        "ibmlhapikey_<cpd_username> — auto-derived",
    "WXD_API_KEY":                     "rotate with --fetch-tokens; or CPD UI → Manage access → API key",
    "WXD_SSL_VERIFY":                  "path to CA cert (certs/watsonxdata-ca.pem) or false to skip",
    # Spark Engine
    "WXD_SPARK_ENGINE_ID":             "auto-discovered via lakehouse API with --fetch-tokens",
    "WXD_SPARK_ENGINE_ENDPOINT":       "auto-derived: https://<cpd_host>/lakehouse/api/v3/spark_engines/<id>",
    "WXD_SPARK_APPLICATIONS_ENDPOINT": "auto-derived: <spark_engine_endpoint>/applications",
    "WXD_SPARK_BEARER_TOKEN":          "short-lived; refreshed with --fetch-tokens",
    "WXD_SPARK_DRY_RUN":               "default: false; set true to skip Spark job submission",
    "WXD_SPARK_CATALOG":               "default: iceberg_data",
    "WXD_SPARK_SCHEMA":                "default: spark_demo",
    "WXD_SPARK_ASSET_BUCKET":          "default: iceberg-bucket",
    "WXD_SPARK_ASSET_PREFIX":          "default: spark_demo",
    "WXD_SPARK_APPLICATION":           "auto-derived: s3a://<bucket>/<prefix>/app/load_medallion_demo.py",
    "WXD_SPARK_INPUT_BASE":            "auto-derived: s3a://<bucket>/<prefix>/raw",
    # MinIO
    "WXD_OBJECT_STORE_ENDPOINT":       "auto-derived: http://ibm-lh-minio-route-<ns>.<app_domain>",
    "WXD_OBJECT_STORE_INTERNAL_ENDPOINT": "cluster-internal: http://ibm-lh-lakehouse-minio-svc.<ns>.svc:9000",
    "WXD_OBJECT_STORE_ACCESS_KEY":     "read from ibm-lh-minio-secret/LH_S3_ACCESS_KEY via oc",
    "WXD_OBJECT_STORE_SECRET_KEY":     "read from ibm-lh-minio-secret/LH_S3_SECRET_KEY via oc",
    "WXD_OBJECT_STORE_REGION":         "default: us-east-1; alternatives: us-south, eu-west-1",
    "WXD_OBJECT_STORE_SSL_VERIFY":     "default: false (MinIO uses self-signed cert on CPD)",
    "WXD_OBJECT_STORE_AUTO_PORT_FORWARD": "default: true — auto port-forward when endpoint unreachable",
    "WXD_OBJECT_STORE_SERVICE":        "k8s service name: ibm-lh-lakehouse-minio-svc",
    "WXD_OBJECT_STORE_SERVICE_PORT":   "default: 9000",
    "WXD_OBJECT_STORE_SECRET_NAME":    "OCP secret holding MinIO credentials",
    "WXD_OBJECT_STORE_ACCESS_KEY_NAME": "key name inside ibm-lh-minio-secret",
    "WXD_OBJECT_STORE_SECRET_KEY_NAME": "key name inside ibm-lh-minio-secret",
    # dbt / Catalog
    "WXD_CATALOG":                     "default: iceberg_data — Iceberg catalog in watsonx.data",
    "WXD_SCHEMA":                      "default: dbt_demo — dbt target schema",
    "WXD_INGEST_SCHEMA":               "default: spark_demo_cpdctl_raw — raw ingestion schema",
    "WXD_GOLD_MATERIALIZED":           "default: view; alternative: table (materialises gold layer)",
    # PostgreSQL
    "PG_HOST":                         "auto-derived: postgresql.<namespace>.svc.cluster.local",
    "PG_PORT":                         "default: 5432",
    "PG_DATABASE":                     "default: ibmas_reporting",
    "PG_USER":                         "default: ibmas_reporting_user",
    "PG_PASSWORD":                     "read from ibmas-reporting-creds/password via oc",
    "PG_SSL_MODE":                     "default: disable (internal cluster traffic)",
    "PG_GOLD_SCHEMA":                  "default: dbt_demo_gold",
    "PG_REPORTING_SCHEMA":             "default: ibmas_reporting",
    # OpenShift / CPD (continued)
    "WXD_CPD_PASSWORD":                "read from platform-auth-idp-credentials/admin_password via oc",
    # Misc
    "WXD_DATASTAGE_PROJECT_NAME":      "default: ibmas-ingest-demo",
}


# ---------------------------------------------------------------------------
#  .env read / write helpers (extended from the original script)
# ---------------------------------------------------------------------------

def _read_env(path: Path) -> tuple[list[tuple[str, str | None]], dict[str, str]]:
    """Return (ordered-items-list, key-value-dict) for the .env file."""
    if not path.exists():
        return [], {}
    items: list[tuple[str, str | None]] = []
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line or line.lstrip().startswith("#") or "=" not in line:
            items.append((line, None))
            continue
        key, _, value = line.partition("=")
        # Strip inline comments (e.g. `KEY=val  # comment`) so the raw comment
        # text is never used as part of a value.  Only strip when the "#" is
        # preceded by whitespace so passwords that contain "#" are preserved.
        value = re.sub(r'\s+#.*$', '', value).strip()
        items.append((key, value))
        values[key] = value
    return items, values


def _write_env(
    path: Path,
    items: list[tuple[str, str | None]],
    values: dict[str, str],
    section_comment: str = "# Auto-generated by prepare_watsonx_env.py",
) -> None:
    """Write values back into the .env file, grouped into labelled sections.

    Layout rules
    ─────────────
    1. A file-header comment block is emitted first (lines that came before the
       first key=value pair in the *original* file, or a default header if the
       file is new).
    2. Each section in _SECTION_MAP is emitted with its header comment.  Only
       keys that are present in *values* are written; keys with no value are
       silently skipped so the file stays clean.
    3. Any key in *values* that is **not** in _KNOWN_KEYS is appended at the
       end under an "Other / unknown keys" comment — this ensures nothing is
       ever lost.
    """
    # ── Collect the preamble (top comments / blanks before first key=value) ──
    preamble: list[str] = []
    for key, val in items:
        if val is not None:
            break
        preamble.append(key)

    lines: list[str] = list(preamble) if preamble else [
        "# Auto-generated by prepare_watsonx_env.py — do not edit by hand.",
        "# Re-run:  python3 scripts/prepare_watsonx_env.py --oc-login --fetch-tokens",
        "",
    ]

    # Ensure the preamble is separated from the first section by exactly one blank.
    if lines and lines[-1] != "":
        lines.append("")

    # ── Emit each section ─────────────────────────────────────────────────────
    known_written: set[str] = set()
    for section_header, section_keys in _SECTION_MAP.items():
        # Only emit the section if at least one key in it has a value.
        section_lines = []
        for k in section_keys:
            if k not in values:
                continue
            comment = _KEY_COMMENTS.get(k)
            line = f"{k}={values[k]}"
            if comment:
                line = f"{line}  # {comment}"
            section_lines.append(line)
        if not section_lines:
            continue
        lines.append(section_header)
        lines.extend(section_lines)
        lines.append("")
        known_written.update(k for k in section_keys if k in values)

    # ── Append any keys not in _KNOWN_KEYS ────────────────────────────────────
    extra = [k for k in values if k not in known_written]
    if extra:
        lines.append("# -- Other / unknown keys ---------------------------------------------------")
        for k in extra:
            comment = _KEY_COMMENTS.get(k)
            line = f"{k}={values[k]}"
            if comment:
                line = f"{line}  # {comment}"
            lines.append(line)
        lines.append("")

    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def _is_placeholder(value: str) -> bool:
    return bool(_PLACEHOLDER_PATTERNS.match(value.strip()))


def _set(
    values: dict[str, str],
    key: str,
    value: str | None,
    overwrite: bool,
) -> str | None:
    """Set key=value if allowed.  Returns the value actually stored, or None."""
    if not value:
        return None
    if key in _PROTECTED_SECRETS:
        return None
    existing = values.get(key, "")
    if overwrite or not existing or _is_placeholder(existing):
        values[key] = value
        return value
    return None


def _relative(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except ValueError:
        return str(path)


# ---------------------------------------------------------------------------
#  Presto / connection JSON loading
# ---------------------------------------------------------------------------

def _parse_connection_json(raw: str, label: str = "connection") -> dict[str, str] | None:
    """Parse a watsonx.data connection JSON string → flat key/value dict.

    Returns None (and logs an error) instead of raising on bad input.
    """
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        log.error("Invalid JSON for %s: %s", label, exc)
        return None

    connection = payload.get("properties", {}).get("connection", [])
    values = {
        str(item["name"]): str(item["value"])
        for item in connection
        if item.get("name") and item.get("value") is not None
    }
    if not values:
        log.error(
            "%s JSON does not look like a watsonx.data connection export. "
            "Expected properties.connection entries with name/value pairs.",
            label,
        )
        return None
    return values


def _load_connection_json(path: Path, label: str = "connection") -> dict[str, str] | None:
    """Load a watsonx.data connection JSON file → flat key/value dict.

    Returns None (and logs an error) instead of raising on bad input.
    """
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        log.error(
            "%s JSON not found: %s\n"
            "  Export it from the watsonx.data UI → "
            "Infrastructure → Engines → ⋮ → Connection info.",
            label, path,
        )
        return None
    return _parse_connection_json(raw, label)


# Keep backward-compatible alias used internally.
_load_presto_json = _load_connection_json


def _extract_cert(ssl_value: str) -> str | None:
    """Extract PEM certificate block(s) from an ssl_certificate field.

    Returns None (and logs an error) if no PEM block is found.
    """
    certs = re.findall(
        r"-----BEGIN CERTIFICATE-----.*?-----END CERTIFICATE-----",
        ssl_value,
        flags=re.DOTALL,
    )
    if not certs:
        log.error(
            "ssl_certificate field contains no PEM block — "
            "cert file will not be written."
        )
        return None
    return "\n".join(c.strip() for c in certs) + "\n"


# ---------------------------------------------------------------------------
#  Namespace / domain derivation from cpd_host
#
#  cpd_host = cpd-cpd-instance.apps.watson.ibmas-zocp-techcluster.org
#   → strip "cpd-" prefix  → "cpd-instance.apps.watson.ibmas-zocp-techcluster.org"
#   → take the part before ".apps"  → "cpd-instance"
#   → app_domain = everything from "apps." onwards
# ---------------------------------------------------------------------------

def _derive_namespace_and_domain(cpd_host: str) -> tuple[str, str]:
    """Return (namespace, app_domain) from the CPD route hostname."""
    # Normalise: strip "cpd-" prefix from the short hostname segment only.
    short = cpd_host.split(".")[0]          # e.g. "cpd-cpd-instance"
    if short.startswith("cpd-"):
        short = short[len("cpd-"):]         # e.g. "cpd-instance"

    apps_idx = cpd_host.find(".apps.")
    if apps_idx == -1:
        # Unexpected format — best-effort fallback.
        log.warning("Could not locate '.apps.' in CPD host '%s'; namespace may be wrong.", cpd_host)
        return short, ""

    app_domain = cpd_host[apps_idx + 1:]    # e.g. "apps.watson.ibmas-zocp-techcluster.org"
    return short, app_domain


# ---------------------------------------------------------------------------
#  oc helper functions (copied verbatim from upload_spark_assets.py L103-151,
#  then adapted to use module-level logger instead of print)
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
#  Local-cluster context detection
#  Orbstack / minikube / k3s contexts must NOT be used for CPD discovery.
# ---------------------------------------------------------------------------

_LOCAL_CONTEXT_PATTERNS = re.compile(
    r"orbstack|minikube|kind|k3s|docker-desktop|rancher-desktop|colima",
    re.IGNORECASE,
)


def _warn_if_local_context(context: str | None) -> None:
    """Emit a prominent warning when the active oc context looks like a local cluster."""
    ctx = context or ""
    if not ctx:
        # Try to read the current context from oc if none pinned.
        try:
            result = subprocess.run(
                ["oc", "config", "current-context"],
                capture_output=True, text=True, timeout=5,
            )
            ctx = result.stdout.strip()
        except Exception:
            return

    if _LOCAL_CONTEXT_PATTERNS.search(ctx):
        log.warning(
            "\n"
            "  ╔══════════════════════════════════════════════════════════════╗\n"
            "  ║  WARNING: current oc context looks like a LOCAL cluster:    ║\n"
            "  ║    %s\n"
            "  ║  Secret discovery will fail or return wrong values.         ║\n"
            "  ║  Fix: set WXD_OPENSHIFT_CONTEXT=<admin-context> in .env     ║\n"
            "  ║       or pass --oc-context <name> if your script supports it║\n"
            "  ╚══════════════════════════════════════════════════════════════╝",
            f"{ctx:<58}",
        )


def _oc_context_args() -> list[str]:
    context = os.getenv("WXD_OPENSHIFT_CONTEXT")
    return ["--context", context] if context else []


def _oc_secret_value(secret_name: str, key: str, namespace: str) -> str | None:
    """Read a base64-encoded value from an OpenShift secret.  Non-fatal."""
    try:
        log.debug("  Reading oc secret %s/%s key '%s' ...", namespace, secret_name, key)
        result = subprocess.run(
            [
                "oc", *_oc_context_args(),
                "get", "secret", secret_name,
                "-n", namespace,
                "-o", f"jsonpath={{.data.{key}}}",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        # Secret does not exist — expected before provisioning; suppress noise.
        if result.returncode != 0:
            stderr = result.stderr.lower()
            if "not found" in stderr or "notfound" in stderr:
                log.debug("  [skip] secret %s/%s not found (pre-provisioning)", namespace, secret_name)
            else:
                log.warning("  [skip] could not read %s/%s key '%s': %s",
                            namespace, secret_name, key, result.stderr.strip())
            return None
        if not result.stdout:
            return None
        decoded = subprocess.run(
            ["base64", "--decode"],
            input=result.stdout,
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
        return decoded.stdout.strip() or None
    except Exception as exc:
        log.warning("  [skip] could not read %s/%s key '%s': %s", namespace, secret_name, key, exc)
        return None


def _oc_run(args: list[str], timeout: int = 20) -> str | None:
    """Run an oc command; return stdout on success, None on any failure."""
    try:
        r = subprocess.run(
            ["oc", *_oc_context_args(), *args],
            check=True,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return r.stdout.strip() or None
    except Exception as exc:
        log.warning("  [skip] oc %s: %s", " ".join(args), exc)
        return None


def _oc_logged_in() -> bool:
    return _oc_run(["whoami"], timeout=10) is not None


def _oc_login(
    api_url: str,
    username: str,
    password: str,
    context_alias: str = "admin",
    token: str | None = None,
) -> bool:
    """Run  oc login  using either a token or username/password.

    If *token* is provided, uses ``oc login <api_url> --token=<token>``.
    Otherwise uses ``oc login <api_url> -u <username> -p <password>``.

    After a successful login the auto-generated context (e.g.
    ``default/api-…/kube:admin``) is renamed to *context_alias* (default
    ``admin``) so that WXD_OPENSHIFT_CONTEXT stays short and stable.
    Returns True on success, False on any failure.
    """
    if token:
        log.info("  [oc-login] oc login %s --token=*** (WXD_OC_TOKEN)", api_url)
        cmd = [
            "oc", *_oc_context_args(),
            "login", api_url,
            f"--token={token}",
            "--insecure-skip-tls-verify=true",
        ]
    else:
        log.info("  [oc-login] oc login %s -u %s ...", api_url, username)
        cmd = [
            "oc", *_oc_context_args(),
            "login", api_url,
            "-u", username,
            "-p", password,
            "--insecure-skip-tls-verify=true",
        ]
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode != 0:
            log.error(
                "  [oc-login] oc login failed (exit %d): %s",
                result.returncode,
                (result.stderr or result.stdout).strip(),
            )
            return False

        log.info("  [oc-login] Logged in successfully.")

        # Rename the auto-generated context to a short stable alias so that
        # WXD_OPENSHIFT_CONTEXT is always just "admin" (or whatever alias is
        # passed), regardless of the long machine-generated context name.
        current_ctx = _oc_run(["config", "current-context"])
        if current_ctx and current_ctx != context_alias:
            rename_result = subprocess.run(
                ["oc", "config", "rename-context", current_ctx, context_alias],
                capture_output=True,
                text=True,
                timeout=10,
            )
            if rename_result.returncode == 0:
                log.info(
                    "  [oc-login] Context renamed: %s → %s",
                    current_ctx,
                    context_alias,
                )
            else:
                # rename-context fails if target alias already exists and points
                # to a different cluster entry — delete the stale one first.
                subprocess.run(
                    ["oc", "config", "delete-context", context_alias],
                    capture_output=True,
                    text=True,
                    timeout=10,
                )
                subprocess.run(
                    ["oc", "config", "rename-context", current_ctx, context_alias],
                    capture_output=True,
                    text=True,
                    timeout=10,
                )
                log.info(
                    "  [oc-login] Context renamed (after removing stale alias): %s → %s",
                    current_ctx,
                    context_alias,
                )

        return True
    except FileNotFoundError:
        log.error("  [oc-login] 'oc' binary not found — install the OpenShift CLI.")
        return False
    except Exception as exc:
        log.error("  [oc-login] Unexpected error: %s", exc)
        return False


def _discover_openshift_api() -> str | None:
    return _oc_run(["whoami", "--show-server"])


def _discover_spark_engine_id(
    cpd_host: str,
    bearer_token: str,
    verify: "bool | str" = False,
    username: str = "cpadmin",
    api_key: "str | None" = None,
    instance_id: "str | None" = None,
) -> str | None:
    """Return the first Spark engine ID from the lakehouse REST API.

    Per the watsonx.data v3 OpenAPI spec (watsonxdata-v3.json):
      GET /lakehouse/api/v3/spark_engines
      Required header: AuthInstanceId — watsonx.data instance ID (software) or CRN (SaaS)
      Auth header:     Authorization: Bearer <token>

    ZenApiKey auth format: base64(username:api_key)  — also tried as fallback.
    Falls back to Bearer <token> when no API key is available.

    Returns the engine id string of the first Spark engine found, or None on any error.
    """
    try:
        import requests as _req
        from requests.packages.urllib3.exceptions import InsecureRequestWarning as _IW
        _req.packages.urllib3.disable_warnings(_IW)
    except ImportError:
        log.debug("_discover_spark_engine_id: requests not available")
        return None

    import base64 as _b64

    # Build auth header variants to try in order:
    #   1. Bearer <token>  + AuthInstanceId   — correct per watsonxdata-v3 spec
    #   2. ZenApiKey base64(username:api_key) + AuthInstanceId  — CPD 5.x alternative
    #   3. Same two again WITHOUT AuthInstanceId  — fallback when instance_id is missing
    auth_headers: list[tuple[str, str]] = []
    auth_headers.append(("Bearer", f"Bearer {bearer_token}"))
    if api_key:
        zen_token = _b64.b64encode(f"{username}:{api_key}".encode()).decode()
        auth_headers.append(("ZenApiKey", f"ZenApiKey {zen_token}"))

    # Try both known URL paths (v3 is current; v2 seen on older CPD 5.x installs)
    url_candidates = [
        f"https://{cpd_host}/lakehouse/api/v3/spark_engines",
        f"https://{cpd_host}/lakehouse/api/v2/spark_engines",
    ]

    for url in url_candidates:
        for auth_label, auth_value in auth_headers:
            # Try with AuthInstanceId header first (required by v3 spec), then without
            header_variants: list[tuple[str, dict]] = []
            if instance_id:
                header_variants.append(
                    (f"{auth_label}+AuthInstanceId",
                     {"Authorization": auth_value, "AuthInstanceId": instance_id}),
                )
            header_variants.append(
                (auth_label, {"Authorization": auth_value}),
            )
            for variant_label, headers in header_variants:
                try:
                    r = _req.get(url, headers=headers, verify=verify, timeout=20)
                    log.info(
                        "  [spark-discovery] GET %s (%s) → %s",
                        url, variant_label, r.status_code,
                    )
                    if r.status_code == 200:
                        data = r.json()
                        # Response shape: {"spark_engines": [{"id": "spark588", ...}, ...]}
                        engines = data.get("spark_engines") or data.get("engines") or []
                        if not engines:
                            log.warning("  [spark-discovery] No Spark engines found in response.")
                            return None
                        # Spec field name is "id"; older exports may use "engine_id"
                        engine_id = engines[0].get("id") or engines[0].get("engine_id")
                        if engine_id:
                            log.info("  [spark-discovery] Found Spark engine: %s", engine_id)
                        return engine_id
                    # 401 → try next variant; 404 → try next URL
                except Exception as exc:
                    log.warning(
                        "  [spark-discovery] Request failed (%s %s): %s",
                        variant_label, url, exc,
                    )

    log.warning(
        "  [spark-discovery] All URL/auth combinations returned non-200 — "
        "cannot auto-discover Spark engine ID. Set WXD_SPARK_ENGINE_ID manually in .env."
    )
    return None


# ---------------------------------------------------------------------------
#  Network reachability helpers (non-fatal checks, purely informational)
# ---------------------------------------------------------------------------

def _tcp_ok(host: str, port: int, timeout: float = 3.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _in_hosts_file(host: str) -> bool:
    hosts = (
        Path("/etc/hosts") if sys.platform != "win32"
        else Path(r"C:\Windows\System32\drivers\etc\hosts")
    )
    try:
        text = hosts.read_text(encoding="utf-8", errors="replace")
        return any(
            host in line and not line.strip().startswith("#")
            for line in text.splitlines()
        )
    except OSError:
        return False


def _reachability_check(cpd_host: str, engine_host: str) -> None:
    log.info("Reachability checks (non-fatal):")
    for host in [cpd_host, engine_host]:
        in_hosts = _in_hosts_file(host)
        reachable = _tcp_ok(host, 443)
        status = "OK" if reachable else ("in hosts" if in_hosts else "not in /etc/hosts")
        icon = "✓" if reachable else "✗"
        log.info("  %s  %s:443  [%s]", icon, host, status)


# ---------------------------------------------------------------------------
#  Dry-run diff printer
# ---------------------------------------------------------------------------

def _print_diff(
    current: dict[str, str],
    proposed: dict[str, str],
) -> None:
    all_keys = list(dict.fromkeys(list(current) + list(proposed)))
    changed = [k for k in all_keys if proposed.get(k) != current.get(k)]
    if not changed:
        print("Nothing to change — .env is already up to date.")
        return

    col = max(len(k) for k in changed) + 2
    print(f"\n{'Key':<{col}}  {'Current':^40}  {'→':^3}  {'Proposed'}")
    print("-" * (col + 90))
    for key in changed:
        old = current.get(key, "<missing>")
        new = proposed.get(key, "<removed>")
        # Truncate long values for readability
        old_disp = (old[:38] + "…") if len(old) > 40 else old
        new_disp = (new[:48] + "…") if len(new) > 50 else new
        print(f"{key:<{col}}  {old_disp:^40}  →    {new_disp}")


# ---------------------------------------------------------------------------
#  Results table printer
# ---------------------------------------------------------------------------

def _print_summary(
    written: dict[str, str],
    skipped: list[str],
    missing_secrets: list[str],
) -> None:
    if written:
        col = max(len(k) for k in written) + 2
        print("\n✓  Values written to .env:")
        print(f"   {'Key':<{col}}  Value")
        print("   " + "-" * (col + 60))
        for key, value in written.items():
            display = (value[:58] + "…") if len(value) > 60 else value
            print(f"   {key:<{col}}  {display}")

    if skipped:
        print(f"\n⊘  Kept existing values ({len(skipped)} keys already set — use --overwrite to replace):")
        for key in skipped:
            print(f"   {key}")

    if missing_secrets:
        print("\n⚠  Secret(s) not yet set — add manually to .env:")
        for key in missing_secrets:
            if key == "WXD_API_KEY":
                print(f"   {key}  ← generate in Software Hub UI: Profile → API key")
            elif key == "WXD_SPARK_BEARER_TOKEN":
                print(f"   {key}  ← run:  python scripts/get_token.py --export")
            else:
                print(f"   {key}")


# ---------------------------------------------------------------------------
#  Token bootstrap helper (--fetch-tokens)
# ---------------------------------------------------------------------------

def _cpd_fetch_tokens(
    cpd_host: str,
    auth_url: str,
    username: str,
    password: str,
    cert_path: Path,
    dry_run: bool,
) -> tuple[str | None, str | None]:
    """Log in with password → rotate API key → get fresh bearer token.

    Returns (api_key, bearer_token).  The caller mirrors both values into
    ``proposed`` and writes them via ``_write_env`` in a single pass.
    """
    if dry_run:
        log.info("  [fetch-tokens] dry-run — skipping HTTP calls, no tokens will be fetched.")
        return None, None

    try:
        import requests
        from requests.packages.urllib3.exceptions import InsecureRequestWarning
        requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
    except ImportError:
        log.error(
            "[fetch-tokens] Missing dependency 'requests'. "
            "Run: pip install requests"
        )
        return None, None

    # Resolve SSL verify — prefer explicit cert_path (just written), then env var.
    ssl_val = os.getenv("WXD_SSL_VERIFY", "").strip()
    if cert_path and cert_path.exists():
        # Use the CA cert we just extracted — most reliable path.
        verify: bool | str = str(cert_path)
    elif not ssl_val or ssl_val.lower() in {"1", "true", "yes"}:
        verify = True
    elif ssl_val.lower() in {"0", "false", "no"}:
        verify = False
    else:
        p = Path(ssl_val) if Path(ssl_val).is_absolute() else ROOT / ssl_val
        verify = str(p) if p.exists() else False
        if not p.exists():
            log.warning("SSL cert file not found: %s — SSL verify disabled", p)

    # ── Step A: password → bearer token ─────────────────────────────────────
    log.info("  [fetch-tokens] POST %s (password auth) ...", auth_url)
    resp = requests.post(
        auth_url,
        json={"username": username, "password": password},
        verify=verify,
        timeout=30,
    )
    if resp.status_code != 200:
        log.error(
            "[fetch-tokens] Password login failed (%s): %s\n"
            "  Check WXD_CPD_USERNAME and WXD_CPD_PASSWORD in .env.",
            resp.status_code, resp.text[:300],
        )
        return None, None
    token = resp.json().get("token")
    if not token:
        log.error(
            "[fetch-tokens] CPD returned no token in response body. "
            "Response: %s", resp.text[:300],
        )
        return None, None
    log.info("  [fetch-tokens] Password auth OK — bearer token obtained.")

    # ── Step B: obtain / rotate API key ─────────────────────────────────────
    # CPD versions differ on which endpoint exposes / creates the API key.
    # We try candidates in priority order and stop at the first success.
    #
    #  1. GET  /usermgmt/v1/user/apikey              → CPD 5.x dedicated key endpoint
    #  2. POST /usermgmt/v1/user/apikey/generate     → CPD 5.x generate (no existing key)
    #  3. GET  /usermgmt/v1/user/<username>          → body may include ApiKey (all versions)
    #  4. POST /usermgmt/v1/user/apikey/regenerate   → CPD 4.5–4.7 (classic)
    #  5. POST /usermgmt/v1/apikey                   → some CPD 4.x variants
    #  6. POST /usermgmt/v1/user/apikey              → CPD 4.8+
    #
    # If all candidates fail we fall back to the password-derived bearer token,
    # which works for authenticated calls but cannot be used as a long-lived API key.
    api_key: str | None = None
    # CPD 5.3.0 spec: GET /usermgmt/v1/user/apiKey requires "ZenApiKey <bearer_token>"
    # (not "Bearer") — ZenApiKey here means: prefix the password-derived JWT with "ZenApiKey ".
    # All other endpoints also accept either ZenApiKey or Bearer per the spec.
    headers_zen  = {"Authorization": f"ZenApiKey {token}"}
    headers_auth = {"Authorization": f"Bearer {token}"}

    # Candidate 1 (CPD 5.3.0 official): GET /usermgmt/v1/user/apiKey
    # Path is case-sensitive on CPD — must be apiKey (capital K), not apikey.
    # This endpoint GENERATES a key if none exists, and RETURNS the current one.
    # Auth: ZenApiKey <bearer_token>  (per IBM docs curl sample)
    log.info("  [fetch-tokens] GET /usermgmt/v1/user/apiKey (CPD 5.3.0 official) ...")
    apikey_url = f"https://{cpd_host}/usermgmt/v1/user/apiKey"
    try:
        r = requests.get(apikey_url, headers=headers_zen, verify=verify, timeout=20)
        if r.status_code == 200:
            log.info(
                "  [fetch-tokens] GET user/apiKey (ZenApiKey) → %s  body=%s",
                r.status_code, r.text[:300],
            )
            body = r.json()
            if isinstance(body, list):
                body = body[0] if body else {}
            api_key = body.get("apiKey") or body.get("api_key") or body.get("ApiKey")
            if api_key:
                log.info("  [fetch-tokens] API key obtained via GET /usermgmt/v1/user/apiKey.")
        else:
            log.debug(
                "  [fetch-tokens] GET user/apiKey (ZenApiKey) → %s (will try Bearer fallback)",
                r.status_code,
            )
    except Exception as exc:
        log.debug("  [fetch-tokens] GET user/apiKey (ZenApiKey) error: %s", exc)

    # Candidate 1b: same URL with Bearer auth (openresty on some CPD installs rejects ZenApiKey)
    if not api_key:
        try:
            r = requests.get(apikey_url, headers=headers_auth, verify=verify, timeout=20)
            if r.status_code == 200:
                body = r.json()
                if isinstance(body, list):
                    body = body[0] if body else {}
                api_key = body.get("apiKey") or body.get("api_key") or body.get("ApiKey")
                if api_key:
                    log.info("  [fetch-tokens] API key obtained.")
            else:
                log.debug(
                    "  [fetch-tokens] GET user/apiKey (Bearer) → %s  body=%s",
                    r.status_code, r.text[:300],
                )
        except Exception as exc:
            log.info("  [fetch-tokens] GET user/apiKey Bearer error: %s", exc)

    # Candidate 2 (legacy / non-standard): POST /usermgmt/v1/user/apikey/generate
    # Not present in CPD 5.3.0 spec but was seen on some installs — kept as fallback.
    if not api_key:
        log.info("  [fetch-tokens] POST /usermgmt/v1/user/apikey/generate (legacy fallback) ...")
        try:
            r = requests.post(
                f"https://{cpd_host}/usermgmt/v1/user/apikey/generate",
                headers=headers_zen,
                verify=verify,
                timeout=20,
            )
            log.info(
                "  [fetch-tokens] POST user/apikey/generate → %s  body=%s",
                r.status_code, r.text[:300],
            )
            if r.status_code == 200:
                body = r.json()
                api_key = body.get("apiKey") or body.get("api_key") or body.get("token")
                if api_key:
                    log.info("  [fetch-tokens] API key obtained via POST user/apikey/generate.")
        except Exception as exc:
            log.info("  [fetch-tokens] POST user/apikey/generate error: %s", exc)

    # Candidate 3: GET user profile — body may include ApiKey field.
    # We always log the full body so the correct field name is visible
    # in the output even when no key is found (different CPD versions differ).
    if not api_key:
        log.info("  [fetch-tokens] GET user profile to check for embedded API key ...")
        user_url = f"https://{cpd_host}/usermgmt/v1/user/{username}"
        try:
            r = requests.get(user_url, headers=headers_zen, verify=verify, timeout=20)
            log.info(
                "  [fetch-tokens] user profile GET → %s  body=%s",
                r.status_code, r.text[:300],
            )
            if r.status_code == 200:
                body = r.json()
                # CPD 5.x wraps the user object in a list; unwrap if needed.
                if isinstance(body, list):
                    body = body[0] if body else {}
                # Field names differ across CPD versions.
                # Note: body["permissions"] is a list of strings on CPD 5.x —
                # guard against calling .get() on a list.
                perms = body.get("permissions")
                api_key = (
                    body.get("ApiKey")
                    or body.get("api_key")
                    or body.get("apiKey")
                    or (perms.get("api_key") if isinstance(perms, dict) else None)
                )
                if api_key:
                    log.info("  [fetch-tokens] Existing API key found in user profile.")
        except Exception as exc:
            log.info("  [fetch-tokens] User profile GET error: %s", exc)

    # Candidates 4–9: additional POST endpoints (CPD 5.x v2 and zen-data, then CPD 4.x legacy)
    # Tuples: (url, label, json_body_or_None)
    if not api_key:
        post_candidates = [
            # CPD 5.x v2 path
            (f"https://{cpd_host}/usermgmt/v2/user/apikey",             "v2/user/apikey",               None),
            # zen-data variants (CPD 5.x) — require a JSON body with the key name
            (f"https://{cpd_host}/zen-data/v3/users/apikey",            "zen-data/v3/users/apikey",     {"api_key_name": "default"}),
            (f"https://{cpd_host}/zen-data/v1/users/apikey",            "zen-data/v1/users/apikey",     {"api_key_name": "default"}),
            # CPD 4.x legacy (no body needed)
            (f"https://{cpd_host}/usermgmt/v1/user/apikey/regenerate",  "regenerate",                   None),
            (f"https://{cpd_host}/usermgmt/v1/apikey",                  "v1/apikey",                    None),
            (f"https://{cpd_host}/usermgmt/v1/user/apikey",             "user/apikey",                  None),
        ]
        for url, label, req_body in post_candidates:
            log.info("  [fetch-tokens] POST %s (%s) ...", url, label)
            try:
                r = requests.post(
                    url,
                    headers=headers_zen,
                    json=req_body,   # None → no body; dict → application/json body
                    verify=verify,
                    timeout=20,
                )
                log.info(
                    "  [fetch-tokens] %s → %s  body=%s",
                    label, r.status_code, r.text[:300],
                )
                if r.status_code == 200:
                    body = r.json()
                    api_key = (
                        body.get("apiKey")
                        or body.get("api_key")
                        or body.get("ApiKey")
                        or body.get("token")  # some variants return "token"
                    )
                    if api_key:
                        log.info("  [fetch-tokens] API key obtained via %s.", label)
                        break
                    # 200 but no key field — already logged above, try next
            except Exception as exc:
                log.info("  [fetch-tokens] %s error: %s", label, exc)

    if not api_key:
        log.warning(
            "  [fetch-tokens] Could not obtain API key from any CPD endpoint "
            "(tried GET user/apikey, POST user/apikey/generate, user profile GET, "
            "POST v2/user/apikey, POST zen-data/v3/users/apikey, POST zen-data/v1/users/apikey, "
            "and 3 legacy POST variants). "
            "WXD_API_KEY will NOT be updated — current value in .env is kept. "
            "To set manually: CPD UI → Profile → API key, then paste into .env as WXD_API_KEY."
        )

    # ── Step C: get fresh bearer using the new key (or fall back to existing) ─
    bearer_token: str | None = None
    if api_key:
        log.info("  [fetch-tokens] Exchanging new API key for bearer token ...")
        resp3 = requests.post(
            auth_url,
            json={"username": username, "api_key": api_key},
            verify=verify,
            timeout=30,
        )
        if resp3.status_code == 200:
            bearer_token = resp3.json().get("token")
            log.info("  [fetch-tokens] Bearer token obtained from new API key.")
        else:
            log.warning(
                "  [fetch-tokens] API-key bearer exchange failed (%s) — "
                "using password-derived token as fallback.",
                resp3.status_code,
            )
            bearer_token = token
    else:
        bearer_token = token  # use the password-derived token as fallback

    # ── Step D: return values to caller ─────────────────────────────────────
    # The caller (main) mirrors these into `proposed` and then writes the whole
    # .env in one pass via _write_env().  We do NOT write here directly to avoid
    # a double-write race where _write_env() would later overwrite our _set_key().
    return api_key, bearer_token


# ---------------------------------------------------------------------------
#  Interactive bootstrap wizard (runs when .env is empty or missing)
# ---------------------------------------------------------------------------

def _read_multiline_json(prompt: str) -> str | None:
    """Read a multiline JSON paste from stdin.

    The user pastes JSON (possibly multi-line) and signals end-of-input with
    either a blank line after the closing brace/bracket, or Ctrl-D / Ctrl-Z.
    Returns the raw string, or None if the user pressed Enter on the very first
    line (meaning they want to skip this optional step).
    """
    print(prompt)
    lines: list[str] = []
    try:
        while True:
            line = input()
            # Empty line after we already have content ending with } or ] → done.
            if not line.strip() and lines:
                combined = "\n".join(lines).strip()
                if combined.endswith(("}","]")):
                    return combined if combined else None
                # Otherwise keep reading (the user might be pasting something
                # that has blank lines in the middle, unlikely but safe).
                lines.append(line)
                continue
            # Very first line is blank → user is skipping.
            if not line.strip() and not lines:
                return None
            lines.append(line)
    except EOFError:
        # Ctrl-D / Ctrl-Z
        combined = "\n".join(lines).strip()
        return combined if combined else None


def _bootstrap_wizard(env_path: Path) -> None:
    """Ask the user for the bare-minimum values when .env is empty/missing.

    5-question flow:
      Q1  OpenShift API URL      (WXD_OPENSHIFT_API)
      Q2  OpenShift username     (WXD_OC_USER, default: kubeadmin)
      Q3  kubeadmin password     (WXD_OC_PASSWORD — never auto-discoverable)
      Q4  Presto connection JSON (paste from watsonx.data UI — saved to
                                  watsonx_data/instance_details.json)
      Q5  Spark connection JSON  (optional paste — saved to
                                  watsonx_data/spark_details.json)

    After writing, the caller re-runs with --oc-login so all remaining values
    are auto-derived.
    """
    import getpass as _gp

    BOX  = "─" * 64
    TICK = "✓"
    print(f"\n┌{BOX}┐")
    print(f"│  🚀  watsonx.data .env bootstrap wizard                       │")
    print(f"│                                                                │")
    print(f"│  Answer 5 questions — the rest will be auto-filled.           │")
    print(f"│  JSON values are saved so you never have to paste them again. │")
    print(f"└{BOX}┘\n")

    # ── Q1  OpenShift API URL ─────────────────────────────────────────────
    print("── Q1 / 5 ─────────────────────────────────────────────────────────")
    print("  OpenShift API URL")
    print("  (shown in: oc whoami --show-server, or OCP console → ? → About)")
    print("  Example:  https://api.watson.ibmas-zocp-techcluster.org:6443")
    oc_api = input("  OpenShift API URL: ").strip()
    while not oc_api.startswith("https://"):
        print("  ✗  Must start with https://  — try again.")
        oc_api = input("  OpenShift API URL: ").strip()

    # ── Q2  OpenShift username ────────────────────────────────────────────
    print()
    print("── Q2 / 5 ─────────────────────────────────────────────────────────")
    print("  OpenShift username  (press Enter for default: kubeadmin)")
    oc_user = input("  OpenShift username [kubeadmin]: ").strip() or "kubeadmin"

    # ── Q3  kubeadmin password ────────────────────────────────────────────
    print()
    print("── Q3 / 5 ─────────────────────────────────────────────────────────")
    print("  kubeadmin password")
    print("  (NOT auto-discoverable — bcrypt-hashed in OCP secret kube-system/kubeadmin)")
    oc_pass = _gp.getpass("  Password: ").strip()
    while not oc_pass:
        print("  ✗  Password cannot be empty.")
        oc_pass = _gp.getpass("  Password: ").strip()

    # ── Q4  Presto connection JSON ────────────────────────────────────────
    print()
    print("── Q4 / 5 ─────────────────────────────────────────────────────────")
    print("  Presto connection JSON")
    print("  How to export:  watsonx.data UI → Infrastructure → Engines")
    print("                  → Presto engine → ⋮ → Connection info → Copy JSON")
    print(f"  Paste JSON below (end with a blank line), or press Enter to skip")
    print(f"  (file path: {_relative(DEFAULT_PRESTO_JSON)})")
    print()
    presto_raw: str | None = None
    if DEFAULT_PRESTO_JSON.exists():
        print(f"  {TICK}  Found existing file — press Enter to keep it, or paste to replace.")
    while True:
        raw = _read_multiline_json("  → Paste Presto JSON (or Enter to skip):")
        if raw is None:
            if DEFAULT_PRESTO_JSON.exists():
                print(f"  {TICK}  Keeping existing {_relative(DEFAULT_PRESTO_JSON)}")
                presto_raw = DEFAULT_PRESTO_JSON.read_text(encoding="utf-8")
            else:
                print("  ✗  Presto JSON is required.  Paste it now or Ctrl-C to abort.")
                continue
        else:
            # Validate the paste before saving.
            result = _parse_connection_json(raw, "Presto")
            if result is None:
                print("  ✗  JSON is not valid — see error above.  Try again.")
                continue
            presto_raw = raw
            print(f"  {TICK}  JSON looks valid.")
        # Save to the default file path.
        if presto_raw and raw is not None:
            DEFAULT_PRESTO_JSON.parent.mkdir(parents=True, exist_ok=True)
            DEFAULT_PRESTO_JSON.write_text(presto_raw, encoding="utf-8")
            print(f"  {TICK}  Saved to {_relative(DEFAULT_PRESTO_JSON)}")
        break

    # ── Q5  Spark connection JSON (optional) ─────────────────────────────
    print()
    print("── Q5 / 5 ─────────────────────────────────────────────────────────")
    print("  Spark connection JSON  (optional — press Enter to skip)")
    print("  How to export:  watsonx.data UI → Infrastructure → Engines")
    print("                  → Spark engine → ⋮ → Connection info → Copy JSON")
    print(f"  (file path: {_relative(DEFAULT_SPARK_JSON)})")
    print()
    if DEFAULT_SPARK_JSON.exists():
        print(f"  {TICK}  Found existing file — press Enter to keep it, or paste to replace.")
    while True:
        raw = _read_multiline_json("  → Paste Spark JSON (or Enter to skip):")
        if raw is None:
            if DEFAULT_SPARK_JSON.exists():
                print(f"  {TICK}  Keeping existing {_relative(DEFAULT_SPARK_JSON)}")
            else:
                print("  ℹ  Spark JSON skipped — WXD_SPARK_ENGINE_ID must be set manually.")
            break
        result = _parse_connection_json(raw, "Spark")
        if result is None:
            print("  ✗  JSON is not valid — see error above.  Try again, or press Enter to skip.")
            continue
        DEFAULT_SPARK_JSON.parent.mkdir(parents=True, exist_ok=True)
        DEFAULT_SPARK_JSON.write_text(raw, encoding="utf-8")
        print(f"  {TICK}  Saved to {_relative(DEFAULT_SPARK_JSON)}")
        break

    # ── Write minimal .env ────────────────────────────────────────────────
    env_path.parent.mkdir(parents=True, exist_ok=True)
    env_path.write_text(
        "# Auto-generated by prepare_watsonx_env.py bootstrap wizard.\n"
        "# Run:  python3 scripts/prepare_watsonx_env.py --oc-login --fetch-tokens\n"
        "#       to fill in all remaining values.\n\n"
        f"WXD_OPENSHIFT_API={oc_api}\n"
        f"WXD_OC_USER={oc_user}\n"
        f"WXD_OC_PASSWORD={oc_pass}\n"
        "\n"
        "# CPD admin username — defaults to cpadmin if not set.\n"
        "# Only change this if your CPD instance uses a different admin account.\n"
        "# WXD_CPD_USERNAME=cpadmin\n",
        encoding="utf-8",
    )
    print(f"\n  {TICK}  Wrote {_relative(env_path)}  (3 values — all others will be auto-filled)")
    print()
    print("  ℹ  CPD admin username defaults to  cpadmin.")
    print("     If your instance uses a different admin account, set  WXD_CPD_USERNAME=<name>")
    print("     in .env before running the next step.")
    print()
    print("  Next step: re-run with  --oc-login  to log in and discover everything:\n")
    print(f"      python3 scripts/prepare_watsonx_env.py --oc-login --fetch-tokens\n")


def _env_is_empty(env_path: Path) -> bool:
    """True when .env does not exist or contains no key=value pairs."""
    if not env_path.exists():
        return True
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            return False
    return True


# ---------------------------------------------------------------------------
#  Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Bootstrap .env for the CPD watsonx.data medallion demo.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--presto-json",
        default=str(DEFAULT_PRESTO_JSON),
        metavar="PATH",
        help="Presto connection JSON export (default: watsonx_data/instance_details.json).",
    )
    parser.add_argument(
        "--spark-json",
        default=None,
        metavar="PATH",
        help="Optional Spark engine connection JSON export.",
    )
    parser.add_argument(
        "--env-file",
        default=str(DEFAULT_ENV_FILE),
        metavar="PATH",
        help="Target .env file (default: .env).",
    )
    parser.add_argument(
        "--cert-file",
        default=str(DEFAULT_CERT_FILE),
        metavar="PATH",
        help="Where to write the CA cert PEM (default: certs/watsonxdata-ca.pem).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would change without writing anything.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace existing non-secret values (default: only fill missing/placeholder values).",
    )
    parser.add_argument(
        "--no-oc",
        action="store_true",
        help="Skip all oc-based discovery (Spark engine ID, MinIO creds, PG password).",
    )
    parser.add_argument(
        "--oc-login",
        action="store_true",
        default=True,
        help=(
            "Run  oc login <WXD_OPENSHIFT_API> -u <WXD_OC_USER> -p <WXD_OC_PASSWORD>  "
            "before discovery (DEFAULT: on). WXD_OC_USER defaults to 'kubeadmin'. "
            "WXD_OC_PASSWORD must be set in .env (never auto-discovered)."
        ),
    )
    parser.add_argument(
        "--no-oc-login",
        dest="oc_login",
        action="store_false",
        help="Skip the oc login step (use current kubeconfig context as-is).",
    )
    parser.add_argument(
        "--fetch-tokens",
        action="store_true",
        default=True,
        help=(
            "Derive WXD_API_KEY and WXD_SPARK_BEARER_TOKEN automatically (DEFAULT: on). "
            "Logs in with WXD_CPD_PASSWORD via the CPD auth API."
        ),
    )
    parser.add_argument(
        "--no-fetch-tokens",
        dest="fetch_tokens",
        action="store_false",
        help="Skip token derivation (keep existing WXD_API_KEY / WXD_SPARK_BEARER_TOKEN).",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable DEBUG logging.",
    )
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Resolve all paths relative to repo root if not absolute.
    def _resolve_path(p: str) -> Path:
        path = Path(p).expanduser()
        return path if path.is_absolute() else ROOT / path

    presto_path = _resolve_path(args.presto_json)
    env_path    = _resolve_path(args.env_file)
    cert_path   = _resolve_path(args.cert_file)

    # ------------------------------------------------------------------
    #  Step 0.A — Bootstrap wizard: prompt if .env is empty or missing
    # ------------------------------------------------------------------
    if _env_is_empty(env_path) and sys.stdin.isatty():
        _bootstrap_wizard(env_path)
        # Ask whether to continue immediately or let the user review first.
        cont = input(
            "  Continue with full bootstrap now? [Y/n]: "
        ).strip().lower()
        if cont in ("n", "no"):
            print("  Run the script again when ready.")
            return 0
        # Both flags are now default-True; make sure they're set explicitly
        # even if the user had passed --no-oc-login before the wizard ran.
        args.oc_login = True
        args.fetch_tokens = True

    # ------------------------------------------------------------------
    #  Step 1 — Load Presto JSON
    # ------------------------------------------------------------------
    log.info("Step 1/5  Reading Presto connection JSON: %s", _relative(presto_path))
    presto = _load_presto_json(presto_path)
    if presto is None:
        log.error(
            "Cannot continue without a valid Presto JSON.\n"
            "  Save the connection export to %s and re-run.",
            _relative(presto_path),
        )
        return 1
    log.info("          Parsed %d connection field(s)", len(presto))

    # Optional Spark JSON (same properties.connection structure).
    # Auto-detect the default path when --spark-json is not given.
    spark_json: dict[str, str] = {}
    spark_path_used: Path | None = None
    if args.spark_json:
        spark_path_used = _resolve_path(args.spark_json)
    elif DEFAULT_SPARK_JSON.exists():
        spark_path_used = DEFAULT_SPARK_JSON
    if spark_path_used:
        log.info("          Reading Spark JSON: %s", _relative(spark_path_used))
        spark_result = _load_presto_json(spark_path_used)
        if spark_result is not None:
            spark_json = spark_result
            # Spark JSON uses spark_engine_id; fall back to engine_id for older exports.
            _sid = spark_json.get("spark_engine_id") or spark_json.get("engine_id")
            if _sid:
                log.info("          Spark engine ID from JSON: %s", _sid)
        else:
            log.warning(
                "          Spark JSON could not be parsed — Spark fields will be skipped."
            )

    # ------------------------------------------------------------------
    #  Step 2 — Write CA cert
    # ------------------------------------------------------------------
    log.info("Step 2/5  Extracting CA certificate chain ...")
    ssl_cert = presto.get("ssl_certificate")
    if ssl_cert:
        pem = _extract_cert(ssl_cert)
        if pem:
            cert_path.parent.mkdir(parents=True, exist_ok=True)
            cert_path.write_text(pem, encoding="utf-8")
            log.info("          Wrote %s", _relative(cert_path))
        # else: _extract_cert already logged the error
    else:
        log.warning("          No ssl_certificate in JSON — skipping cert write.")

    # ------------------------------------------------------------------
    #  Step 3 — Derive all computed values
    # ------------------------------------------------------------------
    log.info("Step 3/5  Deriving URLs and defaults ...")

    # Read existing .env so we can merge safely.
    items, env = _read_env(env_path)

    # We build a separate dict of proposed changes; this lets us do dry-run.
    proposed: dict[str, str] = dict(env)
    ow = args.overwrite

    def S(key: str, value: Optional[str]) -> None:
        _set(proposed, key, value, ow)

    # ── Direct fields from Presto JSON ──────────────────────────────────
    S("WXD_INSTANCE_ID",       presto.get("instance_id"))
    S("WXD_HOST",              presto.get("engine_host"))
    S("WXD_PORT",              presto.get("engine_port") or presto.get("port"))
    S("WXD_PRESTO_ENGINE_ID",  presto.get("engine_id"))

    cpd_host = presto.get("host", "")
    S("WXD_CPD_HOST", cpd_host)

    # ── Namespace + app domain ───────────────────────────────────────────
    namespace, app_domain = ("", "")
    if cpd_host:
        namespace, app_domain = _derive_namespace_and_domain(cpd_host)
        log.info("          Namespace:  %s", namespace)
        log.info("          App domain: %s", app_domain)

    # ── CA cert path ─────────────────────────────────────────────────────
    if ssl_cert:
        _set(proposed, "WXD_SSL_VERIFY", _relative(cert_path), True)

    # ── CPD / OpenShift URLs ─────────────────────────────────────────────
    if cpd_host:
        S("WXD_CPD_AUTH_URL",
          f"https://{cpd_host}/icp4d-api/v1/authorize")
    if app_domain:
        S("WXD_OPENSHIFT_NAMESPACE", namespace)
        S("WXD_OPENSHIFT_CONSOLE",
          f"https://console-openshift-console.{app_domain}/")
        S("WXD_OBJECT_STORE_INTERNAL_ENDPOINT",
          f"http://ibm-lh-lakehouse-minio-svc.{namespace}.svc.cluster.local:9000")
        S("PG_HOST",
          f"postgresql.{namespace}.svc.cluster.local")

    # ── Spark endpoints (derived from CPD host + engine id) ──────────────
    # Engine ID: prefer spark_engine_id field (Spark JSON), fall back to engine_id
    # (older exports), then existing .env value.
    spark_engine_id = (
        spark_json.get("spark_engine_id")
        or spark_json.get("engine_id")
        or env.get("WXD_SPARK_ENGINE_ID")
        or proposed.get("WXD_SPARK_ENGINE_ID")
        or ""
    )
    # Defer final write until after oc discovery (Step 4).

    # ── Object store defaults ────────────────────────────────────────────
    # Use the external MinIO route when app_domain is known (CPD on-prem).
    # The port-forward default (127.0.0.1:19000) is only a fallback.
    if app_domain:
        minio_external = f"http://ibm-lh-minio-route-{namespace}.{app_domain}"
        S("WXD_OBJECT_STORE_ENDPOINT", minio_external)
    else:
        S("WXD_OBJECT_STORE_ENDPOINT", "http://127.0.0.1:19000")
    S("WXD_OBJECT_STORE_REGION",                "us-east-1")
    S("WXD_OBJECT_STORE_SSL_VERIFY",            "false")
    S("WXD_OBJECT_STORE_AUTO_PORT_FORWARD",     "true")
    S("WXD_OBJECT_STORE_SERVICE",               "ibm-lh-lakehouse-minio-svc")
    S("WXD_OBJECT_STORE_SERVICE_PORT",          "9000")
    S("WXD_OBJECT_STORE_SECRET_NAME",           "ibm-lh-minio-secret")
    S("WXD_OBJECT_STORE_ACCESS_KEY_NAME",       "LH_S3_ACCESS_KEY")
    S("WXD_OBJECT_STORE_SECRET_KEY_NAME",       "LH_S3_SECRET_KEY")

    # ── Spark asset paths (need bucket + prefix defaults first) ──────────
    S("WXD_SPARK_ASSET_BUCKET",   "iceberg-bucket")
    S("WXD_SPARK_ASSET_PREFIX",   "spark_demo")
    bucket  = proposed.get("WXD_SPARK_ASSET_BUCKET",  "iceberg-bucket")
    prefix  = proposed.get("WXD_SPARK_ASSET_PREFIX",  "spark_demo")
    S("WXD_SPARK_APPLICATION",
      f"s3a://{bucket}/{prefix}/app/load_medallion_demo.py")
    S("WXD_SPARK_INPUT_BASE",
      f"s3a://{bucket}/{prefix}/raw")
    S("WXD_SPARK_DRY_RUN",        "false")

    # ── dbt / catalog defaults ────────────────────────────────────────────
    S("WXD_CATALOG",              "iceberg_data")
    S("WXD_SCHEMA",               "dbt_demo")
    S("WXD_INGEST_SCHEMA",        "spark_demo_cpdctl_raw")
    S("WXD_GOLD_MATERIALIZED",    "view")
    S("WXD_SPARK_CATALOG",        "iceberg_data")
    S("WXD_SPARK_SCHEMA",         "spark_demo")

    # ── PostgreSQL defaults ───────────────────────────────────────────────
    S("PG_PORT",              "5432")
    S("PG_DATABASE",          "ibmas_reporting")
    S("PG_USER",              "ibmas_reporting_user")
    S("PG_SSL_MODE",          "disable")
    S("PG_GOLD_SCHEMA",       "dbt_demo_gold")
    S("PG_REPORTING_SCHEMA",  "ibmas_reporting")

    # ── Misc defaults ─────────────────────────────────────────────────────
    S("WXD_DATASTAGE_PROJECT_NAME", "ibmas-ingest-demo")

    # ── OC login defaults ────────────────────────────────────────────────
    S("WXD_OC_USER", "kubeadmin")

    # ── Username derivation (ibmlhapikey_<user> ↔ WXD_CPD_USERNAME) ──────
    # Default WXD_CPD_USERNAME to cpadmin when nothing is set.
    S("WXD_CPD_USERNAME", "cpadmin")
    cpd_username = proposed.get("WXD_CPD_USERNAME", "cpadmin")
    wxd_user     = proposed.get("WXD_USER", "")
    if not cpd_username and wxd_user.startswith("ibmlhapikey_"):
        cpd_username = wxd_user[len("ibmlhapikey_"):]
        S("WXD_CPD_USERNAME", cpd_username)
    if cpd_username and not wxd_user:
        S("WXD_USER", f"ibmlhapikey_{cpd_username}")

    # ------------------------------------------------------------------
    #  Step 0 (pre-flight) — optional oc login + context warning
    # ------------------------------------------------------------------
    _warn_if_local_context(proposed.get("WXD_OPENSHIFT_CONTEXT") or env.get("WXD_OPENSHIFT_CONTEXT"))

    if args.oc_login:
        oc_api   = proposed.get("WXD_OPENSHIFT_API", "")
        # Strip any trailing inline comments that may have survived as literal
        # text (e.g. "kubeadmin  # default: kubeadmin") when the user edited
        # .env manually.  _read_env already strips them, but guard here too.
        _strip_comment = lambda s: re.sub(r'\s+#.*$', '', s).strip()
        oc_user  = _strip_comment(proposed.get("WXD_OC_USER", "kubeadmin"))
        oc_pass  = _strip_comment(env.get("WXD_OC_PASSWORD", ""))   # read from existing .env only
        oc_token = _strip_comment(env.get("WXD_OC_TOKEN", ""))      # optional token alternative
        if not oc_api:
            log.warning(
                "  [--oc-login] WXD_OPENSHIFT_API not set — cannot run oc login. "
                "Run once without --oc-login so the API URL is derived from the Presto JSON."
            )
        elif not oc_token and not oc_pass:
            log.warning(
                "  [--oc-login] Neither WXD_OC_TOKEN nor WXD_OC_PASSWORD is set in .env — "
                "set one of them (kubeadmin password cannot be auto-discovered)."
            )
        else:
            if oc_token:
                log.info("  [--oc-login] Using WXD_OC_TOKEN for oc login.")
            ok = _oc_login(oc_api, oc_user, oc_pass, token=oc_token or None)
            if not ok:
                log.error(
                    "  [--oc-login] Login failed — aborting. "
                    "Check WXD_OC_TOKEN or WXD_OC_USER / WXD_OC_PASSWORD in .env."
                )
                return 1

    # ------------------------------------------------------------------
    #  Step 4 — oc-based discovery (non-fatal)
    # ------------------------------------------------------------------
    log.info("Step 4/5  OpenShift discovery via oc ...")
    if args.no_oc:
        log.info("          --no-oc set, skipping.")
    else:
        if not _oc_logged_in():
            log.warning(
                "          oc is not logged in (or not installed) — "
                "skipping Spark engine, MinIO creds, PG password discovery.\n"
                "          Tip: add  --oc-login  to run  oc login  automatically, "
                "or run  oc login ...  manually then re-run this script."
            )
        else:
            log.info("          oc session active")

            # WXD_OPENSHIFT_API
            api_url = _discover_openshift_api()
            if api_url:
                S("WXD_OPENSHIFT_API", api_url)
                log.info("          WXD_OPENSHIFT_API = %s", api_url)

            # WXD_OPENSHIFT_CONTEXT — always write the short stable alias
            # "admin".  If --oc-login was used, the context was already renamed
            # to this alias inside _oc_login().  If the user logged in manually
            # they should have used `oc config rename-context … admin` too.
            _set(proposed, "WXD_OPENSHIFT_CONTEXT", "admin", True)
            log.info("          WXD_OPENSHIFT_CONTEXT = admin")

            # WXD_SPARK_ENGINE_ID — discovered via the lakehouse REST API using the
            # password-derived bearer token obtained in Step 6.  We try this early
            # (Step 4) using any bearer token already present in the existing .env,
            # and repeat after token-fetch in Step 6 if still missing.
            if not spark_engine_id:
                existing_bearer = (
                    env.get("WXD_SPARK_BEARER_TOKEN")
                    or proposed.get("WXD_SPARK_BEARER_TOKEN")
                )
                if existing_bearer and cpd_host:
                    log.info("          Attempting Spark engine auto-discovery via lakehouse API ...")
                    # Resolve SSL verify from existing cert or env.
                    _sv_path = proposed.get("WXD_SSL_VERIFY", "")
                    _sv: bool | str = (
                        str(cert_path) if cert_path and cert_path.exists()
                        else (False if _sv_path.lower() in {"0", "false", "no", ""} else _sv_path)
                    )
                    discovered_id = _discover_spark_engine_id(
                        cpd_host, existing_bearer, verify=_sv,
                        username=proposed.get("WXD_CPD_USERNAME", "cpadmin"),
                        api_key=proposed.get("WXD_API_KEY") or env.get("WXD_API_KEY"),
                        instance_id=proposed.get("WXD_INSTANCE_ID") or env.get("WXD_INSTANCE_ID"),
                    )
                    if discovered_id:
                        spark_engine_id = discovered_id
                        log.info("          WXD_SPARK_ENGINE_ID = %s  [auto-discovered]", spark_engine_id)
                    else:
                        log.warning(
                            "          WXD_SPARK_ENGINE_ID not found via API — "
                            "will retry after token-fetch (Step 6), or set manually in .env "
                            "(CPD UI → watsonx.data → Infrastructure → Engines)"
                        )
                else:
                    log.warning(
                        "          WXD_SPARK_ENGINE_ID not set and no bearer token available "
                        "for auto-discovery — will retry after token-fetch (Step 6), "
                        "or set manually in .env."
                    )

            # MinIO credentials
            if namespace:
                access_key = _oc_secret_value("ibm-lh-minio-secret", "LH_S3_ACCESS_KEY", namespace)
                secret_key = _oc_secret_value("ibm-lh-minio-secret", "LH_S3_SECRET_KEY", namespace)
                if access_key:
                    S("WXD_OBJECT_STORE_ACCESS_KEY", access_key)
                    log.info("          WXD_OBJECT_STORE_ACCESS_KEY  [read from secret]")
                if secret_key:
                    S("WXD_OBJECT_STORE_SECRET_KEY", secret_key)
                    log.info("          WXD_OBJECT_STORE_SECRET_KEY  [read from secret]")

                # PG password — read from (or pre-create) the ibmas-reporting-creds
                # secret.  provision_pg_reporting.sh creates this secret too, but
                # does so with a freshly-generated password every run unless the
                # secret already exists.  By pre-creating it here, the provisioner
                # will find it and read back the password — keeping them in sync.
                _PG_SECRET = "ibmas-reporting-creds"
                pg_pass = (
                    _oc_secret_value(_PG_SECRET, "password",           namespace)
                    or _oc_secret_value(_PG_SECRET, "postgres-password", namespace)
                    or _oc_secret_value(_PG_SECRET, "POSTGRES_PASSWORD", namespace)
                )
                if pg_pass:
                    S("PG_PASSWORD", pg_pass)
                    log.info("          PG_PASSWORD  [read from %s]", _PG_SECRET)
                elif not proposed.get("PG_PASSWORD"):
                    # Secret does not yet exist — pre-create it with a generated
                    # password so that provision_pg_reporting.sh reuses the same
                    # credentials rather than generating a new random one each run.
                    if not args.dry_run:
                        import secrets as _secrets
                        _pg_pass_new = _secrets.token_urlsafe(24).replace("-", "").replace("_", "")[:32]
                        _pg_host = f"postgresql.{namespace}.svc.cluster.local"
                        _pg_db   = "ibmas_reporting"
                        _pg_user = "ibmas_reporting_user"
                        _jdbc    = (
                            f"jdbc:postgresql://{_pg_host}:5432/{_pg_db}"
                            f"?user={_pg_user}&password={_pg_pass_new}"
                        )
                        _create_result = _oc_run([
                            "-n", namespace,
                            "create", "secret", "generic", _PG_SECRET,
                            f"--from-literal=database={_pg_db}",
                            f"--from-literal=schema=ibmas_reporting",
                            f"--from-literal=username={_pg_user}",
                            f"--from-literal=password={_pg_pass_new}",
                            f"--from-literal=host={_pg_host}",
                            f"--from-literal=port=5432",
                            f"--from-literal=jdbc-uri={_jdbc}",
                            "--dry-run=client", "-o", "yaml",
                        ])
                        if _create_result is not None:
                            # Pipe the yaml through oc apply for idempotency
                            try:
                                _apply = subprocess.run(
                                    ["oc", "-n", namespace, "apply", "-f", "-"],
                                    input=_create_result,
                                    capture_output=True,
                                    text=True,
                                    timeout=20,
                                )
                                if _apply.returncode == 0:
                                    S("PG_PASSWORD", _pg_pass_new)
                                    log.info(
                                        "          PG_PASSWORD  [pre-created secret %s/%s]",
                                        namespace, _PG_SECRET,
                                    )
                                    log.info(
                                        "          provision_pg_reporting.sh will reuse these credentials."
                                    )
                                else:
                                    log.warning(
                                        "          Could not pre-create %s: %s",
                                        _PG_SECRET,
                                        (_apply.stderr or _apply.stdout).strip(),
                                    )
                            except Exception as _exc:
                                log.warning("          Secret pre-create failed: %s", _exc)
                        else:
                            log.warning("          oc secret pre-create returned no output.")
                    else:
                        log.info(
                            "          [dry-run] Would pre-create K8s secret %s/%s with a generated PG_PASSWORD.",
                            namespace, _PG_SECRET,
                        )

                # cpadmin password — only populate when not already set in .env.
                # platform-auth-idp-credentials/admin_password is the live value
                # (updated when the CPD admin password changes).
                # admin-user-details/initial_admin_password is the install-time value
                # only — used as a last-resort fallback.
                if not proposed.get("WXD_CPD_PASSWORD"):
                    cpd_pw = _oc_secret_value(
                        "platform-auth-idp-credentials", "admin_password", namespace
                    )
                    if not cpd_pw:
                        # last resort: initial install password (may be stale)
                        cpd_pw = _oc_secret_value(
                            "admin-user-details", "initial_admin_password", namespace
                        )
                    if cpd_pw:
                        S("WXD_CPD_PASSWORD", cpd_pw)
                        log.info("          WXD_CPD_PASSWORD  [read from platform-auth-idp-credentials]")
                    else:
                        log.warning("          WXD_CPD_PASSWORD not found in OCP secrets — set manually")

    # ── Spark engine ID is now finalised — write dependent values ─────────
    if spark_engine_id:
        S("WXD_SPARK_ENGINE_ID", spark_engine_id)
    # Reread so derived URLs use the latest value.
    spark_engine_id = proposed.get("WXD_SPARK_ENGINE_ID", spark_engine_id)
    if spark_engine_id and cpd_host:
        base = f"https://{cpd_host}/lakehouse/api/v3/spark_engines/{spark_engine_id}"
        S("WXD_SPARK_ENGINE_ENDPOINT",       base)
        S("WXD_SPARK_APPLICATIONS_ENDPOINT", f"{base}/applications")

    # ------------------------------------------------------------------
    #  Step 5 — Reachability (purely informational, non-fatal)
    # ------------------------------------------------------------------
    log.info("Step 5/5  Reachability checks ...")
    engine_host = proposed.get("WXD_HOST", "")
    if cpd_host and engine_host:
        _reachability_check(cpd_host, engine_host)
    else:
        log.info("          Skipped (WXD_HOST or WXD_CPD_HOST not known yet).")

    # ------------------------------------------------------------------
    #  Step 6 (optional) — Fetch WXD_API_KEY + WXD_SPARK_BEARER_TOKEN
    # ------------------------------------------------------------------
    tokens_fetched = False
    if args.fetch_tokens:
        log.info("Step 6     Fetching tokens (--fetch-tokens) ...")

        # Resolve password: prefer value just discovered in proposed (OC secret),
        # then existing .env file, then interactive prompt (skipped in dry-run).
        from dotenv import load_dotenv as _load_dotenv
        _load_dotenv(env_path, override=False)
        ft_password = (
            proposed.get("WXD_CPD_PASSWORD")
            or os.getenv("WXD_CPD_PASSWORD")
        )
        if not ft_password:
            if args.dry_run:
                log.warning("  [dry-run] WXD_CPD_PASSWORD not set — skipping token fetch in dry-run mode")
                ft_password = "__dry_run_placeholder__"
            else:
                import getpass as _getpass
                ft_password = _getpass.getpass(
                    "  WXD_CPD_PASSWORD not found in .env.\n"
                    f"  Enter password for {proposed.get('WXD_CPD_USERNAME', 'cpadmin')}: "
                )

        ft_host     = proposed.get("WXD_CPD_HOST", "")
        ft_auth_url = proposed.get("WXD_CPD_AUTH_URL", "")
        ft_username = proposed.get("WXD_CPD_USERNAME", "cpadmin")

        if not ft_host or not ft_auth_url:
            log.error(
                "  [fetch-tokens] WXD_CPD_HOST or WXD_CPD_AUTH_URL is not set — "
                "run without --fetch-tokens first to populate these values."
            )
        else:
            _ft_api_key, _ft_bearer = _cpd_fetch_tokens(
                cpd_host=ft_host,
                auth_url=ft_auth_url,
                username=ft_username,
                password=ft_password,
                cert_path=cert_path,
                dry_run=args.dry_run,
            )
            # Only count as "fetched" when we actually obtained at least one token.
            # A full failure (both None) should still show the missing-secrets warning.
            if _ft_api_key or _ft_bearer:
                tokens_fetched = True
                # Mirror token values into `proposed` so the summary shows them.
                if _ft_api_key:
                    S("WXD_API_KEY", _ft_api_key)
                if _ft_bearer:
                    S("WXD_SPARK_BEARER_TOKEN", _ft_bearer)

            # Retry Spark engine discovery now that we have a fresh bearer token.
            # Only a bearer token can authenticate the lakehouse REST API — the raw
            # API key string cannot be used here directly.
            if not proposed.get("WXD_SPARK_ENGINE_ID"):
                _retry_bearer = _ft_bearer  # must be a bearer, not an API key
                if _retry_bearer and ft_host:
                    log.info("          Retrying Spark engine ID discovery with fresh token ...")
                    _sv_path2 = proposed.get("WXD_SSL_VERIFY", "")
                    _sv2: bool | str = (
                        str(cert_path) if cert_path and cert_path.exists()
                        else (False if _sv_path2.lower() in {"0", "false", "no", ""} else _sv_path2)
                    )
                    _found_id = _discover_spark_engine_id(
                        ft_host, _retry_bearer, verify=_sv2,
                        username=ft_username,
                        api_key=_ft_api_key or proposed.get("WXD_API_KEY") or env.get("WXD_API_KEY"),
                        instance_id=proposed.get("WXD_INSTANCE_ID") or env.get("WXD_INSTANCE_ID"),
                    )
                    if _found_id:
                        S("WXD_SPARK_ENGINE_ID", _found_id)
                        base = f"https://{ft_host}/lakehouse/api/v3/spark_engines/{_found_id}"
                        S("WXD_SPARK_ENGINE_ENDPOINT",       base)
                        S("WXD_SPARK_APPLICATIONS_ENDPOINT", f"{base}/applications")
                        log.info("          WXD_SPARK_ENGINE_ID = %s  [discovered post-token-fetch]", _found_id)

    # ------------------------------------------------------------------
    #  Dry-run: show diff and exit without writing.
    # ------------------------------------------------------------------
    if args.dry_run:
        print("\n── Dry run — no files will be written ──")
        _print_diff(env, proposed)
        print()
        return 0

    # ------------------------------------------------------------------
    #  Write .env
    # ------------------------------------------------------------------
    written: dict[str, str] = {
        k: v for k, v in proposed.items()
        if env.get(k) != v
    }
    _write_env(env_path, items, proposed)
    log.info("Wrote %s", _relative(env_path))

    # ------------------------------------------------------------------
    #  Report missing secrets (suppressed when --fetch-tokens succeeded)
    # ------------------------------------------------------------------
    if tokens_fetched:
        missing_secrets: list[str] = []
    else:
        missing_secrets = [
            k for k in _PROTECTED_SECRETS
            if not proposed.get(k) or _is_placeholder(proposed.get(k, ""))
        ]

    _print_summary(
        written,
        skipped=[k for k in env if env[k] == proposed.get(k, "") and k not in written],
        missing_secrets=missing_secrets,
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())
