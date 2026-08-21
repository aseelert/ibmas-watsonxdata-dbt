#!/usr/bin/env python3
# -----------------------------------------------------------------------------
#  provision_ikc_governance.py — build the IKC governance model in dependency order
#
#  Location  : scripts/06b_provision_ikc_governance.py
#  Repository: https://github.com/aseelert/ibmas-watsonxdata-dbt
#  Project   : watsonx.data · dbt · Spark medallion demo
#  Author    : Alexander Seelert
#  Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.
# -----------------------------------------------------------------------------
"""Provision the Retail Medallion Lakehouse governance model into IBM Knowledge
Catalog (watsonx.data Intelligence) on Cloud Pak for Data / IBM Software Hub 5.3.

WHAT / WHY
  The governance CSVs in governance/ikc/ describe categories, classifications,
  business terms, data classes, reference data and rules. They cannot be applied
  in arbitrary order: each artifact type resolves references against artifacts
  that already exist AND are already PUBLISHED. Importing everything at once, or
  in the wrong order, fails with GIM00015E ("artifact not found in hierarchy").

  This script drives the full cycle non-interactively, one stage at a time:

      import  →  publish the resulting drafts  →  verify  →  next stage

  It replaces the manual "import in the UI, click through the workflow inbox,
  repeat six times" procedure documented in governance/ikc/README.md.

HOW ARTIFACTS ARE CREATED (--mode)
  per-artifact (default)
      One HTTP call per artifact: the CSV is split into single-artifact CSVs
      (the artifact's own row plus its continuation rows for tags, related
      terms and so on), each is imported on its own, and each is published
      immediately before the next one is sent. That is what "in the right
      sequence" actually requires — inside a single file artifacts already
      depend on each other (a sub-category on its parent category, Personal
      Data on Business Data), and a reference only resolves against something
      already PUBLISHED. It also means a failure names the artifact that
      failed, with the IBM error code for that one artifact, instead of an
      aggregate count for the whole file.
  bulk
      The original behaviour: one multipart import per artifact type, then
      publish everything the file created. Fewer calls, but a rejected row
      only shows up as failed_count with a partially applied file.

  Both modes use the SAME verified endpoint —
  POST /v3/governance_artifact_types/{type}/import?merge_option=all — because
  that is the only artifact-creation contract confirmed to work on this
  cluster. There are native JSON endpoints (POST /v3/glossary_terms,
  /v3/categories, /v3/data_classes), but their request bodies for *software*
  are not attested in IBM's shipped client (its glossary service only ever
  does CSV import) nor in cpdctl, so this script does not guess them: it sends
  one artifact at a time over the contract that is known to be accepted.

DEPENDENCY ORDER (each stage is published before the next one runs)
  1. categories        — everything else addresses categories by '>>' path
  2. classifications   — must be live before terms/data classes reference them
  3. business terms    — must be live before data classes link to them
  4. data classes      — reference published terms via full '>>' paths
  5. reference data    — sets, then their values. Skipped on this instance: the
                         CSV-import contract this script uses for every other
                         type (POST .../governance_artifact_types/{type}/import)
                         genuinely does not recognise "reference_data_set" here
                         (confirmed live: HTTP 400 WKCBG2133E "Unrecognized
                         type"). There IS a working native endpoint though —
                         POST /v3/reference_data accepts a body and requires a
                         "type" field (confirmed live: without it, HTTP 400
                         WKCBG3034E "Invalid type: null"). None of the obvious
                         guesses (value_set, code_set, list, code_table,
                         enumeration, mapping, simple, and their case variants)
                         were accepted. Whoever picks this up next: the fix is
                         almost certainly "find the right string for `type`",
                         not "give up" — try IBM's InfoSphere/IGC reference-data
                         terminology (e.g. representation types from the
                         classic Information Governance Catalog reference data
                         model) or capture a real create call via the browser's
                         network tab while adding one through Governance >
                         Reference data > Add in the UI.
  6. rules             — the glossary rule artifact (06_rules.csv), then the
                         data protection rule that enforces it
                         (06_data_protection_rules.json). The DPR is NOT
                         SaaS-only despite appearances: the DSL->native
                         transform (POST /v4/enforcement-transform/utility/
                         json_to_rule) really is unavailable here (HTTP 405
                         WDPPS9010E, confirmed live), but POST
                         /v3/enforcement/rules accepts the friendly DSL body
                         verbatim on this CPD 5.3 build — no transform needed.
                         The one real constraint: DPR names reject punctuation
                         (confirmed live: '-', ':', '(', ')' all HTTP 400
                         WDPPS9019E "not a valid name" — plain alphanumeric +
                         spaces works), unlike glossary artifact names, which
                         have no such restriction.

GLOSSARY RULE vs DATA PROTECTION RULE
  These are two different artifacts and both are created. 06_rules.csv imports a
  governance *rule artifact*: the documented policy, searchable in the glossary,
  enforcing nothing. Masking is done by a *data protection rule*, which is served
  by POST /v3/enforcement/rules and takes a JSON body, so it is defined in
  governance/ikc/06_data_protection_rules.json using the friendly rule DSL.

  On SaaS that DSL is converted by POST /v4/enforcement-transform/utility/
  json_to_rule first. IBM's own client marks that transform as SaaS-only, and
  on this software instance it is confirmed absent (live probe: HTTP 405
  WDPPS9010E, not a bug here). But the transform turns out to be unnecessary
  on CPD 5.3 software: POST /v3/enforcement/rules itself accepts the friendly
  DSL shape verbatim — confirmed live by creating and deleting a probe rule
  with exactly the 06_data_protection_rules.json shape (HTTP 201, no
  transform). So the script tries the transform (in case a future/other
  instance needs it), and when it is absent, POSTs the DSL directly instead
  of giving up. --dpr-dump-existing / --dpr-native remain available as a
  manual escape hatch if a future CPD version rejects the DSL shape outright.

AUTH (software / on-prem — NOT SaaS)
  Everything is a bearer token in the end: Authorization: Bearer <jwt>. Three
  ways to get one are tried in order (--auth picks one explicitly), the same
  strategy scripts/00b_get_token.py uses:

    1. token    an existing JWT in WXD_CPD_BEARER_TOKEN or WXD_SPARK_BEARER_TOKEN.
                Checked for expiry locally, then probed against the governance
                API — a 401 means stale, and the next method is tried.
    2. api-key  POST /icp4d-api/v1/authorize {"username": …, "api_key": …}
                Non-interactive and long-lived; WXD_API_KEY.
    3. password POST /icp4d-api/v1/authorize {"username": …, "password": …}
                WXD_CPD_PASSWORD, or an interactive prompt when the terminal is
                attached. With --save-api-key the script then rotates a fresh
                API key (POST /usermgmt/v1/user/apikey/regenerate) into .env so
                the next run needs no password at all.

  The workflow 'assignee' required when completing publish tasks is the "uid"
  claim decoded from whichever token was obtained — that is how the official IBM
  tooling resolves it on CPD (SaaS uses iam_id instead).

  Config comes from .env: WXD_CPD_HOST, WXD_CPD_AUTH_URL, WXD_CPD_USERNAME
  (default cpadmin), plus one of the credentials above. TLS follows
  WXD_SSL_VERIFY (a CA PEM path, or false to skip verification).

ENDPOINTS
  Every path below was taken from IBM's own shipped client code — the
  ibm-watsonx-data-intelligence-mcp-server package (app/services/constants.py and
  app/services/workflow/), not from the public SaaS API docs, which describe
  different paths for some of these calls.

      POST /icp4d-api/v1/authorize                                        get a token
      POST /usermgmt/v1/user/apikey/regenerate                            rotate API key
      POST /v3/governance_artifact_types/{type}/import?merge_option=all   CSV import
      GET  /v3/governance_artifact_types/{type}?sub_string=&limit=N       list + state
      GET  /v3/workflows?artifact_id=..&include_user_tasks=true           find publish task
      GET  /v3/workflow_user_tasks/{task_id}                              read task form
      POST /v3/workflow_user_tasks/{task_id}/actions                      claim / complete
      POST /v3/search                                                     verify published
      POST /v4/enforcement-transform/utility/json_to_rule                 rule DSL -> native
      GET  /v3/enforcement/rules                                          list / dump DPRs
      POST /v3/enforcement/rules                                          create a DPR

IDEMPOTENCY
  Every import uses merge_option=all, which overwrites matching artifacts in
  place, so the script is safe to re-run in either mode. Re-running after a
  partial failure picks up whatever is still in draft and publishes it.

USAGE
    python scripts/06b_provision_ikc_governance.py                  # all stages, one artifact at a time
    python scripts/06b_provision_ikc_governance.py --dry-run        # plan only, no writes
    python scripts/06b_provision_ikc_governance.py --mode bulk      # one import per artifact type
    python scripts/06b_provision_ikc_governance.py --stage terms    # one stage
    python scripts/06b_provision_ikc_governance.py --stage categories --stage terms
    python scripts/06b_provision_ikc_governance.py --auth password  # force password login
    python scripts/06b_provision_ikc_governance.py --save-api-key   # rotate an API key into .env
    python scripts/06b_provision_ikc_governance.py --no-publish     # import, leave drafts
    python scripts/06b_provision_ikc_governance.py --publish-only   # publish existing drafts
    python scripts/06b_provision_ikc_governance.py --verify-only    # read-only report
    python scripts/06b_provision_ikc_governance.py --keep-going     # do not stop at a failed stage
    python scripts/06b_provision_ikc_governance.py -v               # verbose HTTP logging
    python scripts/06b_provision_ikc_governance.py --skip-dpr       # governance only, no masking
    python scripts/06b_provision_ikc_governance.py --dpr-dump-existing dpr.json
    python scripts/06b_provision_ikc_governance.py --stage rules --dpr-native dpr.json

EXIT
  0 when every requested stage imported, published and verified. Non-zero on an
  auth failure, a missing CSV, an import error, or an artifact still stuck in
  draft after publishing. IBM error codes (GIM…, WKCBG…) are surfaced verbatim
  because they are the only actionable signal when an import is rejected.
"""

from __future__ import annotations

import argparse
import base64
import csv
import getpass
import io
import json
import os
import sys
import time
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
IKC_DIR = ROOT / "governance" / "ikc"

# --- Endpoints -------------------------------------------------------------
# Source: ibm-watsonx-data-intelligence-mcp-server, app/services/constants.py
SEARCH_PATH = "/v3/search"
ARTIFACT_TYPES_PATH = "/v3/governance_artifact_types"
CATEGORIES_PATH = "/v3/categories"
GLOSSARY_TERMS_PATH = "/v3/glossary_terms"
DATA_CLASSES_PATH = "/v3/data_classes"
REFERENCE_DATA_PATH = "/v3/reference_data"
WORKFLOWS_PATH = "/v3/workflows"
WORKFLOW_TASKS_PATH = "/v3/workflow_user_tasks"
DPR_RULES_PATH = "/v3/enforcement/rules"
DP_TRANSFORM_PATH = "/v4/enforcement-transform/utility"
AUTHORIZE_PATH = "/icp4d-api/v1/authorize"
APIKEY_REGEN_PATH = "/usermgmt/v1/user/apikey/regenerate"

# Friendly-DSL definitions for the data protection rules (see the file header).
DPR_JSON = IKC_DIR / "06_data_protection_rules.json"

# Per-artifact-type API roots, for the /{artifact_id}/versions sub-resource.
ARTIFACT_API_PATH = {
    "category": CATEGORIES_PATH,
    "glossary_term": GLOSSARY_TERMS_PATH,
    "data_class": DATA_CLASSES_PATH,
    "reference_data_set": REFERENCE_DATA_PATH,
}

# Form-field values that mean "approve and publish" on a governance workflow
# task. The task's own enum_values are preferred; this is the ranked fallback.
APPROVE_HINTS = ("publish", "approve", "accept", "complete", "yes")

PUBLISH_POLL_SECONDS = 4
PUBLISH_POLL_ATTEMPTS = 15


# ---------------------------------------------------------------------------
# Stage definitions — the dependency-correct order
# ---------------------------------------------------------------------------

class Stage:
    """One artifact type: its CSV, its API artifact_type, and why it sits here."""

    def __init__(self, key, artifact_type, csv_name, label, depends_on, optional=False):
        self.key = key
        self.artifact_type = artifact_type
        self.csv_name = csv_name
        self.label = label
        self.depends_on = depends_on
        self.optional = optional

    @property
    def csv_path(self) -> Path:
        return IKC_DIR / self.csv_name


STAGES = [
    Stage(
        "categories", "category", "01_categories.csv",
        "Categories (Retail Medallion Lakehouse + 5 sub-categories)",
        "nothing — categories are the root of every '>>' path",
    ),
    Stage(
        "classifications", "classification", "03_classifications.csv",
        "Classifications (Business Data, Personal Data)",
        "categories — each classification lives at a '>>' category path",
    ),
    Stage(
        "terms", "glossary_term", "02_business_terms.csv",
        "Business terms (14 medallion concepts)",
        "categories + classifications",
    ),
    Stage(
        "data_classes", "data_class", "04_data_classes.csv",
        "Data classes (14 custom regex DCs, prefixed 'RML ')",
        "published terms — Related Terms needs the full '>>' path to resolve",
    ),
    Stage(
        "reference_data", "reference_data_set", "05_reference_data_sets.csv",
        "Reference data sets (Order Status, Payment Method, Product Category, Country)",
        "categories",
        optional=True,
    ),
    Stage(
        "rules", "rule", "06_rules.csv",
        "Rules (governance rule artifacts + data protection rules)",
        "published terms + classifications — a rule references what it governs",
        optional=True,
    ),
]

STAGE_BY_KEY = {s.key: s for s in STAGES}

# Reference data value files, keyed by the set name in 05_reference_data_sets.csv.
REFERENCE_VALUE_FILES = {
    "Order Status": "05_refvalues_order_status.csv",
    "Payment Method": "05_refvalues_payment_method.csv",
    "Product Category": "05_refvalues_product_category.csv",
    "Country": "05_refvalues_country.csv",
}


# ---------------------------------------------------------------------------
# Environment helpers (same conventions as scripts/00b_get_token.py)
# ---------------------------------------------------------------------------

def _env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if not value:
        raise SystemExit(
            f"Missing required env var: {name}\n"
            f"  Copy .env.example to .env and fill it in, or run:\n"
            f"    python scripts/00a_prepare_watsonx_env.py"
        )
    return value.strip()


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
        print(f"  WARNING: CA cert not found: {path} — continuing without verification",
              file=sys.stderr)
        return False
    return str(path)


def _decode_jwt(token: str) -> dict:
    """Decode a JWT payload without verifying it (we only read our own claims)."""
    try:
        payload_b64 = token.split(".")[1]
        payload_b64 += "=" * (-len(payload_b64) % 4)
        return json.loads(base64.urlsafe_b64decode(payload_b64).decode("utf-8"))
    except Exception:
        return {}


def _decode_jwt_uid(token: str) -> str | None:
    """Return the 'uid' claim from a CPD bearer token.

    Workflow task actions need an explicit assignee. On CPD that identifier is
    the uid claim inside the bearer token (SaaS uses iam_id instead) — this is
    exactly how IBM's own MCP server resolves it in app/core/auth.py.
    """
    uid = _decode_jwt(token).get("uid") or _decode_jwt(token).get("sub")
    return str(uid) if uid is not None else None


def _token_expired(token: str) -> bool:
    """True when the token's own 'exp' claim is in the past.

    Cheap local check so a stale WXD_SPARK_BEARER_TOKEN costs one comparison
    instead of a confusing 401 halfway through a stage.
    """
    exp = _decode_jwt(token).get("exp")
    try:
        return float(exp) <= time.time()
    except (TypeError, ValueError):
        return False


# ---------------------------------------------------------------------------
# Authentication — token, API key or password (in that order)
# ---------------------------------------------------------------------------

def _post_authorize(auth_url: str, payload: dict, verify) -> requests.Response:
    return requests.post(auth_url, json=payload, verify=verify, timeout=30)


def auth_with_api_key(auth_url, username, api_key, verify) -> str | None:
    """Bearer token from an API key, or None when the key is rejected (401)."""
    resp = _post_authorize(auth_url, {"username": username, "api_key": api_key}, verify)
    if resp.status_code == 200:
        return resp.json().get("token")
    if resp.status_code in (401, 403):
        return None
    raise SystemExit(f"CPD auth error (HTTP {resp.status_code}): {resp.text[:300]}")


def auth_with_password(auth_url, username, password, verify) -> str:
    """Bearer token from a password login. Exits with a hint on failure."""
    resp = _post_authorize(auth_url, {"username": username, "password": password}, verify)
    if resp.status_code == 200:
        token = resp.json().get("token")
        if token:
            return token
    raise SystemExit(
        f"   Password login failed (HTTP {resp.status_code}): {resp.text[:300]}\n"
        f"   Check WXD_CPD_USERNAME / WXD_CPD_PASSWORD in .env."
    )


def regenerate_api_key(cpd_host, token, verify) -> str | None:
    """Rotate the current user's API key so later runs need no password."""
    resp = requests.post(
        f"https://{cpd_host}{APIKEY_REGEN_PATH}",
        headers={"Authorization": f"Bearer {token}"},
        verify=verify, timeout=30,
    )
    if resp.status_code == 200:
        body = resp.json()
        return body.get("apiKey") or body.get("api_key")
    print(f"   WARNING: could not regenerate API key (HTTP {resp.status_code}): "
          f"{resp.text[:200]}", file=sys.stderr)
    return None


def probe_token(cpd_host, token, verify) -> bool:
    """Is this token still accepted by the governance API?

    Uses an endpoint the script already depends on. Only 401 counts as a
    rejection: 403 means the token is valid but this account lacks a privilege,
    which is a different problem and must not trigger a credential fallback.
    """
    try:
        resp = requests.get(
            f"https://{cpd_host}{ARTIFACT_TYPES_PATH}/category",
            headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
            params={"sub_string": "", "limit": 1}, verify=verify, timeout=30,
        )
    except requests.RequestException:
        return False
    return resp.status_code != 401


def resolve_token(*, auth_url, cpd_host, username, verify, env_path,
                  method="auto", save_api_key=False, allow_prompt=True) -> str:
    """Obtain a bearer token by whichever means is available.

    Order (all of them, or just the one --auth names): an existing token, then
    the API key, then the password. Returns the token; raises SystemExit with an
    actionable message when nothing works.
    """
    want = {"auto", method}

    # 1. A token we already have. Free, and the fastest path in a demo.
    if want & {"auto", "token"}:
        env_token = (os.getenv("WXD_CPD_BEARER_TOKEN", "")
                     or os.getenv("WXD_SPARK_BEARER_TOKEN", "")).strip()
        if env_token:
            if _token_expired(env_token):
                print("   existing bearer token has expired — trying the next method")
            elif probe_token(cpd_host, env_token, verify):
                print("   using the bearer token from .env  [OK]")
                return env_token
            else:
                print("   bearer token from .env was rejected — trying the next method")
        elif method == "token":
            raise SystemExit(
                "   --auth token needs WXD_CPD_BEARER_TOKEN or WXD_SPARK_BEARER_TOKEN in .env.\n"
                "   Generate one with: python scripts/00b_get_token.py --export"
            )

    # 2. The API key: non-interactive and long-lived, so it is the normal path.
    if want & {"auto", "api-key"}:
        api_key = os.getenv("WXD_API_KEY", "").strip()
        if api_key:
            token = auth_with_api_key(auth_url, username, api_key, verify)
            if token:
                print(f"   API key accepted for {username}  [OK]")
                return token
            print("   API key rejected (expired or revoked) — trying password login")
        elif method == "api-key":
            raise SystemExit(
                "   --auth api-key needs WXD_API_KEY in .env.\n"
                "   Software Hub UI: avatar > Profile and settings > API key > Regenerate."
            )

    # 3. Password login, then optionally mint an API key so the next run is
    #    non-interactive — the same self-healing scripts/00b_get_token.py does.
    if want & {"auto", "password"}:
        password = os.getenv("WXD_CPD_PASSWORD", "").strip()
        if not password and allow_prompt and sys.stdin.isatty():
            print(f"   password for {username} on {cpd_host}:")
            password = getpass.getpass("     Password: ").strip()
        if password:
            token = auth_with_password(auth_url, username, password, verify)
            print(f"   password login as {username}  [OK]")
            if save_api_key:
                new_key = regenerate_api_key(cpd_host, token, verify)
                if new_key:
                    set_key(str(env_path), "WXD_API_KEY", new_key)
                    os.environ["WXD_API_KEY"] = new_key
                    print(f"   new API key {new_key[:8]}… saved to {env_path.name} — "
                          f"later runs need no password  [OK]")
            else:
                print("   tip: --save-api-key rotates an API key into .env so the next "
                      "run is non-interactive")
            return token

    raise SystemExit(
        "   No usable credential found. Set one of these in .env:\n"
        "     WXD_API_KEY           long-lived, non-interactive (recommended)\n"
        "     WXD_CPD_PASSWORD      password for WXD_CPD_USERNAME (default cpadmin)\n"
        "     WXD_CPD_BEARER_TOKEN  a JWT you already have\n"
        "   Or run: python scripts/00b_get_token.py --export"
    )


# ---------------------------------------------------------------------------
# IKC REST client
# ---------------------------------------------------------------------------

class IKCError(RuntimeError):
    pass


class IKCClient:
    """Thin REST client for the CPD 5.3 governance artifact APIs."""

    def __init__(self, host, token, verify, dry_run=False, verbose=False):
        self.host = host
        self.token = token
        self.verify = verify
        self.dry_run = dry_run
        self.verbose = verbose
        self.uid = _decode_jwt_uid(token)
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        })

    # -- plumbing ----------------------------------------------------------

    def _url(self, path: str) -> str:
        return f"https://{self.host}{path}"

    def _request(self, method, path, *, write=False, **kwargs):
        url = self._url(path)
        if write and self.dry_run:
            print(f"     DRY-RUN would {method} {path}")
            return None
        if self.verbose:
            params = kwargs.get("params")
            print(f"     -> {method} {path}" + (f" params={params}" if params else ""))
        kwargs.setdefault("timeout", 180)
        resp = self.session.request(method, url, verify=self.verify, **kwargs)
        if self.verbose:
            print(f"     <- HTTP {resp.status_code} ({len(resp.content)} bytes)")
        return resp

    @staticmethod
    def _json(resp) -> dict:
        if resp is None:
            return {}
        ctype = resp.headers.get("Content-Type", "")
        if "application/json" not in ctype:
            return {}
        try:
            body = resp.json()
        except ValueError:
            return {}
        return body if isinstance(body, dict) else {"resources": body}

    @staticmethod
    def describe_error(resp) -> str:
        """Surface the IBM error code — the only actionable part of a failure."""
        if resp is None:
            return "no response"
        try:
            body = resp.json()
        except ValueError:
            return f"HTTP {resp.status_code}: {resp.text[:400]}"
        parts = []
        for key in ("code", "errorCode", "error_code", "trace"):
            if body.get(key):
                parts.append(f"{key}={body[key]}")
        for key in ("message", "error", "errors", "reason", "failed_artifacts"):
            if body.get(key):
                parts.append(f"{key}={json.dumps(body[key])[:400]}")
        return f"HTTP {resp.status_code}: " + ("; ".join(parts) if parts else json.dumps(body)[:400])

    # -- imports -----------------------------------------------------------

    def import_csv(self, artifact_type: str, csv_path: Path, merge: str = "all") -> dict:
        """POST a per-type governance CSV file. Returns the parsed response body.

        merge_option=all overwrites matching artifacts in place, which is what
        makes this whole script re-runnable.
        """
        with csv_path.open("rb") as fh:
            return self.import_csv_bytes(artifact_type, csv_path.name, fh.read(), merge=merge)

    def import_csv_bytes(self, artifact_type: str, filename: str, data: bytes,
                          merge: str = "all") -> dict:
        """POST in-memory CSV bytes for one artifact type.

        Same endpoint as import_csv, just without a file on disk — this is what
        --mode per-artifact uses to send one artifact (plus its continuation
        rows) per call, built in memory by split_csv_by_artifact().
        """
        path = f"{ARTIFACT_TYPES_PATH}/{artifact_type}/import"
        resp = self._request(
            "POST", path, write=True,
            params={"merge_option": merge},
            files={"file": (filename, data, "text/csv")},
        )
        if resp is None:
            return {}
        if resp.status_code not in (200, 201, 202):
            raise IKCError(f"CSV import of {filename} as '{artifact_type}' failed: "
                           f"{self.describe_error(resp)}")
        return self._json(resp)

    def import_supported(self, artifact_type: str) -> bool:
        """Probe whether this artifact type accepts the per-type CSV import.

        Categories, classifications, glossary terms and data classes are known
        to work. Reference data sets and rules are not documented for this
        endpoint, so the script probes instead of assuming.
        """
        resp = self._request("GET", f"{ARTIFACT_TYPES_PATH}/{artifact_type}",
                             params={"sub_string": "", "limit": 1})
        return resp is not None and resp.status_code < 400

    # -- listing / state ---------------------------------------------------

    def list_artifacts(self, artifact_type: str, limit: int = 200) -> list[dict]:
        """List artifacts of a type, drafts included, with their workflow state."""
        resp = self._request("GET", f"{ARTIFACT_TYPES_PATH}/{artifact_type}",
                             params={"sub_string": "", "limit": limit})
        if resp is None or resp.status_code >= 400:
            return []
        return self._json(resp).get("resources", []) or []

    @staticmethod
    def artifact_id_of(item: dict) -> str | None:
        meta = item.get("metadata", {}) or {}
        entity = item.get("entity", {}) or {}
        artifacts = entity.get("artifacts", {}) if isinstance(entity, dict) else {}
        return meta.get("artifact_id") or item.get("artifact_id") or artifacts.get("artifact_id")

    @staticmethod
    def name_of(item: dict) -> str:
        meta = item.get("metadata", {}) or {}
        entity = item.get("entity", {}) or {}
        return meta.get("name") or entity.get("name") or item.get("name") or "<unnamed>"

    @staticmethod
    def is_draft(item: dict) -> bool:
        """Draft detection mirroring IBM's own is_draft_metadata() helper.

        An artifact counts as draft when its state says DRAFT, when it still has
        a workflow attached, or when draft_mode is set.
        """
        meta = item.get("metadata", {}) or {}
        state = meta.get("state") or item.get("state")
        draft_mode = meta.get("draft_mode") or item.get("draft_mode")
        return (
            (isinstance(state, str) and state.upper() in {"DRAFT", "IMPORT_CREATE", "IMPORT_UPDATE"})
            or meta.get("workflow_id") is not None
            or item.get("workflow_id") is not None
            or draft_mode is True
            or (isinstance(draft_mode, str) and draft_mode.casefold() == "draft")
        )

    def search_published(self, artifact_type: str, limit: int = 200) -> list[dict]:
        """POST /v3/search — returns PUBLISHED artifacts only, so it is the
        independent confirmation that a publish actually took effect."""
        body = {
            "query": {"bool": {"must": [{"match": {"metadata.artifact_type": artifact_type}}]}},
            "size": limit,
        }
        resp = self._request("POST", SEARCH_PATH, json=body,
                             headers={"Content-Type": "application/json"})
        if resp is None or resp.status_code >= 400:
            return []
        return self._json(resp).get("rows", []) or self._json(resp).get("resources", []) or []

    # -- publishing --------------------------------------------------------

    def workflow_tasks_for(self, artifact_id: str) -> list[dict]:
        """Active workflow user tasks for one artifact (the publish approvals)."""
        resp = self._request("GET", WORKFLOWS_PATH, params={
            "artifact_id": artifact_id,
            "limit": 50,
            "include_user_tasks": True,
            "return_active_workflows": True,
            "return_completed_workflows": False,
        })
        if resp is None or resp.status_code >= 400:
            return []
        tasks = []
        for wf in self._json(resp).get("resources", []) or []:
            for task in (wf.get("entity", {}) or {}).get("user_tasks", []) or []:
                tasks.append(task)
        return tasks

    def task_details(self, task_id: str) -> dict:
        resp = self._request("GET", f"{WORKFLOW_TASKS_PATH}/{task_id}")
        if resp is None or resp.status_code >= 400:
            return {}
        return self._json(resp)

    def task_action(self, task_id: str, action: str, form_properties: list[dict] | None = None):
        """claim / complete a workflow task.

        Body shape and the /actions sub-path both come from IBM's MCP server
        (app/services/workflow/tools/task_action.py).
        """
        body = {
            "action": action,
            "assignee": self.uid,
            "form_properties": form_properties or [],
        }
        return self._request(
            "POST", f"{WORKFLOW_TASKS_PATH}/{task_id}/actions", write=True, json=body,
            headers={"Content-Type": "application/json"},
        )

    @staticmethod
    def _approve_value(task: dict) -> str:
        """Pick the enum value on the task's 'action' field that means approve.

        The publish form is instance-configurable, so read its own enum_values
        rather than hardcoding a verb. Reject options are conventionally prefixed
        with '-' and are filtered out.
        """
        props = (task.get("entity", {}) or {}).get("form_properties", []) or []
        action_prop = next((p for p in props if isinstance(p, dict) and p.get("id") == "action"), None)
        choices: list[str] = []
        if action_prop:
            for ev in action_prop.get("enum_values", []) or []:
                if isinstance(ev, dict) and "id" in ev:
                    choices.append(str(ev["id"]))
                elif isinstance(ev, str):
                    choices.append(ev)
        positive = [c for c in choices if not c.startswith("-")]
        for hint in APPROVE_HINTS:
            for choice in positive:
                if hint in choice.casefold():
                    return choice
        if positive:
            return positive[0]
        return "approve"

    def publish_artifact(self, artifact_id: str, name: str) -> tuple[bool, str]:
        """Claim and approve every active workflow task for one artifact.

        Returns (progressed, detail). progressed is True when at least one task
        was completed, or when there was nothing to do because the artifact is
        already published.
        """
        tasks = self.workflow_tasks_for(artifact_id)
        if not tasks:
            return True, "no active workflow task (already published, or workflows disabled)"

        completed = 0
        for task in tasks:
            task_id = (task.get("metadata", {}) or {}).get("task_id")
            if not task_id:
                continue

            # Claim first — an unclaimed task cannot be completed. A task already
            # claimed by us returns a non-2xx here, which is harmless.
            self.task_action(task_id, "claim")

            fresh = self.task_details(task_id) or task
            value = self._approve_value(fresh)
            resp = self.task_action(task_id, "complete", [{"id": "action", "value": value}])
            if self.dry_run:
                completed += 1
                continue
            if resp is not None and resp.status_code < 400:
                completed += 1
            else:
                detail = self.describe_error(resp)
                if "action_config" in detail:
                    detail += (
                        " — this matches a known CPD 5.3.4 workflow-template bug where "
                        "REST publish rejects the completed form; publish this one via "
                        "the UI workflow inbox (Governance > My tasks) instead."
                    )
                return False, f"task {task_id} complete(action={value}) → {detail}"

        return (completed > 0), f"completed {completed}/{len(tasks)} task(s)"

    def publish_all_drafts(self, artifact_type: str) -> tuple[int, int, list[str]]:
        """Publish every draft of a type. Returns (published, total, failures)."""
        items = self.list_artifacts(artifact_type)
        drafts = [i for i in items if self.is_draft(i)]
        if not drafts:
            print(f"     no drafts pending for '{artifact_type}'")
            return 0, 0, []

        print(f"     {len(drafts)} draft(s) to publish")
        failures: list[str] = []
        published = 0
        for item in drafts:
            aid = self.artifact_id_of(item)
            name = self.name_of(item)
            if not aid:
                failures.append(f"{name}: no artifact_id in response")
                continue
            ok, detail = self.publish_artifact(aid, name)
            mark = "OK " if ok else "ERR"
            print(f"       [{mark}] {name} — {detail}")
            if ok:
                published += 1
            else:
                failures.append(f"{name}: {detail}")
        return published, len(drafts), failures

    def wait_until_published(self, artifact_type: str) -> tuple[int, int]:
        """Poll until no drafts remain. Returns (remaining_drafts, total)."""
        if self.dry_run:
            return 0, 0
        for attempt in range(1, PUBLISH_POLL_ATTEMPTS + 1):
            items = self.list_artifacts(artifact_type)
            drafts = [i for i in items if self.is_draft(i)]
            if not drafts:
                return 0, len(items)
            if attempt < PUBLISH_POLL_ATTEMPTS:
                if self.verbose:
                    print(f"     {len(drafts)} still draft — retry {attempt}/{PUBLISH_POLL_ATTEMPTS}")
                time.sleep(PUBLISH_POLL_SECONDS)
        items = self.list_artifacts(artifact_type)
        return len([i for i in items if self.is_draft(i)]), len(items)

    # -- data protection rules --------------------------------------------

    def dpr_list(self) -> list[dict]:
        """Existing data protection rules, for idempotency and shape inspection."""
        resp = self._request("GET", DPR_RULES_PATH, params={"limit": 200})
        if resp is None or resp.status_code >= 400:
            return []
        body = self._json(resp)
        for key in ("rules", "resources", "results", "items"):
            if isinstance(body.get(key), list):
                return body[key]
        return []

    @staticmethod
    def dpr_name_of(rule: dict) -> str:
        return (rule.get("name")
                or rule.get("metadata", {}).get("name")
                or rule.get("entity", {}).get("name")
                or "")

    def dpr_transform(self, dsl: dict) -> dict | None:
        """Convert the friendly rule DSL into the native enforcement payload.

        IBM's own client marks this transform service as SaaS-only, so on
        software it may simply not be routed. Returns None in that case (the
        caller then reports it instead of POSTing a guessed body), and raises
        IKCError when the service is present but rejects the DSL.
        """
        resp = self._request(
            "POST", f"{DP_TRANSFORM_PATH}/json_to_rule", write=True,
            json={"json": json.dumps(dsl)},          # value must be a STRING
            headers={"Content-Type": "application/json"},
        )
        if resp is None:                              # dry run
            return {}
        if resp.status_code in (400, 404, 405, 501, 502, 503):
            if self.verbose:
                print(f"     transform unavailable: {self.describe_error(resp)}")
            return None
        if resp.status_code >= 400:
            raise IKCError(f"rule DSL transform failed: {self.describe_error(resp)}")
        body = self._json(resp)
        status = str(body.get("status", ""))
        if "failed" in status or "error" in status:
            raise IKCError(f"rule DSL rejected: {body.get('message')}")
        # rule_map_json is the native body POST /v3/enforcement/rules expects;
        # rule_json is the validated-but-still-friendly echo of the input.
        return body.get("rule_map_json") or body.get("rule_json") or None

    def dpr_create(self, payload: dict) -> tuple[bool, str]:
        resp = self._request(
            "POST", DPR_RULES_PATH, write=True, json=payload,
            headers={"Content-Type": "application/json"},
        )
        if resp is None:
            return True, "dry-run"
        if resp.status_code >= 400:
            return False, self.describe_error(resp)
        guid = self._json(resp).get("metadata", {}).get("guid", "")
        return bool(guid), guid or f"HTTP {resp.status_code} but no metadata.guid"


# ---------------------------------------------------------------------------
# Per-artifact CSV splitting (--mode per-artifact)
# ---------------------------------------------------------------------------

def split_csv_by_artifact(csv_path: Path) -> list[tuple[str, bytes]]:
    """Split a governance CSV into one single-artifact CSV per primary row.

    Every governance CSV in this repo puts the artifact's Name in column 0 and
    leaves it blank on continuation rows that add a second Tag / Classification
    / Related Term value to the row above. So "one artifact" = one row with a
    non-empty column 0, plus every immediately-following row that has an empty
    column 0. That is the only structural fact this needs — it works unchanged
    across categories/classifications/terms/data_classes/rules because they all
    share that convention (verified against every CSV in governance/ikc/).

    Returns [(label, csv_bytes), ...] in file order, so publishing artifact N
    before importing artifact N+1 reproduces the same dependency-respecting
    sequence the whole multi-stage script already applies at the file level —
    just one row deep instead of one file deep.
    """
    with csv_path.open(newline="", encoding="utf-8-sig") as fh:
        rows = list(csv.reader(fh))
    if not rows:
        return []
    header, body = rows[0], rows[1:]

    groups: list[list[list[str]]] = []
    for row in body:
        if not row or all(not cell.strip() for cell in row):
            continue
        if row[0].strip():
            groups.append([row])
        elif groups:
            groups[-1].append(row)
        # a continuation row with no primary row yet is malformed input — drop it

    out = []
    for group in groups:
        buf = io.StringIO()
        writer = csv.writer(buf, lineterminator="\r\n")
        writer.writerow(header)
        writer.writerows(group)
        out.append((group[0][0].strip(), buf.getvalue().encode("utf-8")))
    return out


# ---------------------------------------------------------------------------
# Reference data values (no CSV import contract — driven over JSON REST)
# ---------------------------------------------------------------------------

def read_reference_values(csv_path: Path) -> list[dict]:
    """Read a 05_refvalues_*.csv into IKC reference-value dicts."""
    values = []
    with csv_path.open(newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            code = (row.get("value") or "").strip()
            if not code:
                continue
            values.append({
                "code": code,
                "value": code,
                "description": (row.get("description") or "").strip(),
            })
    return values


def load_reference_sets() -> list[tuple[str, list[dict]]]:
    """Pair each reference data set name with the values file that backs it."""
    sets = []
    for name, filename in REFERENCE_VALUE_FILES.items():
        path = IKC_DIR / filename
        if not path.exists():
            print(f"     WARNING: value file missing for '{name}': {filename}", file=sys.stderr)
            continue
        sets.append((name, read_reference_values(path)))
    return sets


# ---------------------------------------------------------------------------
# Stage runner
# ---------------------------------------------------------------------------

def _publish_by_name(client: IKCClient, artifact_type: str, name: str) -> tuple[bool, str]:
    """Publish the single draft artifact of this type matching `name`.

    Used by --mode per-artifact right after importing one artifact, so the
    next artifact in the CSV can reference it while it is already live —
    without touching any other draft of the same type that might be mid-import
    from a concurrent stage.
    """
    items = client.list_artifacts(artifact_type)
    match = next((i for i in items
                  if client.is_draft(i) and client.name_of(i) == name), None)
    if match is None:
        # Either already published (nothing to do) or the response shape
        # didn't carry a name we recognise — fall back to publishing every
        # draft of this type, which is still correct, just less targeted.
        return _publish_all_named(client, artifact_type, name)
    aid = client.artifact_id_of(match)
    if not aid:
        return False, f"draft found but no artifact_id in response for '{name}'"
    return client.publish_artifact(aid, name)


def _publish_all_named(client: IKCClient, artifact_type: str, name: str) -> tuple[bool, str]:
    published, total, failures = client.publish_all_drafts(artifact_type)
    if failures:
        return False, "; ".join(failures[:2])
    return True, f"published via publish_all_drafts fallback (no draft matched '{name}' by name)"


def run_stage(client: IKCClient, stage: Stage, *, do_import: bool, do_publish: bool,
             mode: str = "per-artifact") -> dict:
    """Import, publish and verify one stage. Returns a summary dict."""
    print(f"\n── {stage.key}: {stage.label}  [mode={mode}]")
    print(f"   depends on: {stage.depends_on}")

    summary = {"stage": stage.key, "imported": None, "published": None,
               "drafts_left": None, "published_total": None, "failures": []}

    if not stage.csv_path.exists():
        msg = f"CSV not found: {stage.csv_path.relative_to(ROOT)}"
        if stage.optional:
            print(f"   SKIP — {msg}")
            summary["failures"].append(f"skipped: {msg}")
            return summary
        raise IKCError(msg)

    # --- import ---
    if do_import:
        if not client.import_supported(stage.artifact_type):
            msg = (f"artifact type '{stage.artifact_type}' is not served by "
                   f"{ARTIFACT_TYPES_PATH} on this instance — CSV import unavailable")
            if stage.optional:
                print(f"   SKIP — {msg}")
                summary["failures"].append(f"skipped: {msg}")
                return summary
            raise IKCError(msg)

        if mode == "bulk":
            print(f"   import {stage.csv_path.name} (merge_option=all)")
            body = client.import_csv(stage.artifact_type, stage.csv_path)
            created = body.get("created_count", body.get("total_count"))
            failed = body.get("failed_count", 0)
            summary["imported"] = created
            print(f"     created={created} failed={failed}")
            for err in (body.get("errors") or body.get("failed_artifacts") or [])[:8]:
                print(f"     WARN: {json.dumps(err)[:300]}")
                summary["failures"].append(f"import: {json.dumps(err)[:200]}")
        else:
            artifacts = split_csv_by_artifact(stage.csv_path)
            print(f"   import + publish {len(artifacts)} artifact(s) from "
                  f"{stage.csv_path.name}, one at a time")
            created = 0
            for label, data in artifacts:
                print(f"   · {label}")
                try:
                    body = client.import_csv_bytes(
                        stage.artifact_type, f"{stage.artifact_type}.csv", data,
                    )
                except IKCError as exc:
                    print(f"     [ERR] {exc}")
                    summary["failures"].append(f"{label}: {exc}")
                    continue
                failed = body.get("failed_count", 0) or 0
                if failed:
                    for err in (body.get("errors") or body.get("failed_artifacts") or [])[:3]:
                        print(f"     [ERR] {json.dumps(err)[:300]}")
                        summary["failures"].append(f"{label}: {json.dumps(err)[:200]}")
                    continue
                created += 1
                if do_publish:
                    # Publish this one artifact now, before the next artifact in
                    # the file (which may reference it by name) gets imported.
                    ok, detail = _publish_by_name(client, stage.artifact_type, label)
                    mark = "OK " if ok else "ERR"
                    print(f"     [{mark}] imported + {detail}")
                    if not ok:
                        summary["failures"].append(f"{label}: {detail}")
                else:
                    print("     [OK ] imported (left in draft, --no-publish)")
            summary["imported"] = created

    # --- publish ---
    # In per-artifact mode each artifact was already published right after its
    # own import, above — UNLESS this is --publish-only (do_import is False),
    # in which case there is nothing to do inline and this is the only publish
    # pass, same as bulk mode.
    if do_publish and (mode == "bulk" or not do_import):
        print("   publish drafts")
        published, total, failures = client.publish_all_drafts(stage.artifact_type)
        summary["published"] = published
        summary["failures"].extend(failures)

        left, all_count = client.wait_until_published(stage.artifact_type)
        summary["drafts_left"] = left
        if left:
            summary["failures"].append(
                f"{left} artifact(s) still in draft after publishing — the next "
                f"stage will fail to resolve references to them"
            )
            print(f"     WARNING: {left} still in draft")
        else:
            print(f"     all drafts cleared ({all_count} artifact(s) of this type)")
    elif do_publish and mode == "per-artifact":
        left, all_count = client.wait_until_published(stage.artifact_type)
        summary["drafts_left"] = left
        summary["published"] = summary["imported"]
        if left:
            summary["failures"].append(
                f"{left} artifact(s) still in draft after publishing — the next "
                f"stage will fail to resolve references to them"
            )
            print(f"     WARNING: {left} still in draft")

    # --- verify ---
    published_rows = client.search_published(stage.artifact_type)
    summary["published_total"] = len(published_rows)
    print(f"   verify: {len(published_rows)} published '{stage.artifact_type}' artifact(s) found")

    return summary


def run_reference_values(client: IKCClient) -> None:
    """Attach values to the reference data sets, once the sets are published."""
    sets = load_reference_sets()
    if not sets:
        return
    print("\n── reference_data values")
    for name, values in sets:
        print(f"   {name}: {len(values)} value(s) from CSV")
        if client.dry_run:
            print(f"     DRY-RUN would attach {len(values)} value(s)")
            continue
        # Resolve the published set by name, then post its values.
        match = next((r for r in client.search_published("reference_data_set")
                      if client.name_of(r) == name), None)
        if not match:
            print(f"     SKIP — '{name}' is not published yet")
            continue
        aid = client.artifact_id_of(match)
        resp = client._request(
            "POST", f"{REFERENCE_DATA_PATH}/{aid}/values", write=True,
            json={"values": values}, headers={"Content-Type": "application/json"},
        )
        if resp is not None and resp.status_code < 400:
            print(f"     [OK ] {len(values)} value(s) attached")
        else:
            print(f"     [ERR] {client.describe_error(resp)}")


def run_data_protection_rules(client: IKCClient, *, native_path: Path | None = None) -> dict:
    """Create the enforcement rules that actually mask data.

    06_rules.csv only documents the policy in the glossary. Enforcement lives in
    a data protection rule, which has a JSON body rather than a CSV row, so it is
    driven from 06_data_protection_rules.json (or a pre-built native payload
    passed with --dpr-native).
    """
    summary = {"stage": "data_protection_rules", "imported": None, "published": None,
               "drafts_left": None, "published_total": None, "failures": []}
    print("\n── data_protection_rules: enforcement rules (the actual masking)")
    print("   depends on: published data classes + terms; the rule references them by name")

    if native_path is not None:
        if not native_path.exists():
            summary["failures"].append(f"--dpr-native file not found: {native_path}")
            print(f"   [ERR] not found: {native_path}")
            return summary
        payload = json.loads(native_path.read_text())
        rules = payload if isinstance(payload, list) else [payload]
        print(f"   using pre-built native payload(s) from {native_path.name} — no DSL transform")
        dsl_rules = []
    else:
        if not DPR_JSON.exists():
            summary["failures"].append(f"skipped: {DPR_JSON.name} not found")
            print(f"   SKIP — {DPR_JSON.name} not found")
            return summary
        dsl_rules = json.loads(DPR_JSON.read_text()).get("rules", [])
        rules = []
        if not dsl_rules:
            summary["failures"].append(f"skipped: no rules defined in {DPR_JSON.name}")
            print(f"   SKIP — no rules in {DPR_JSON.name}")
            return summary

    existing = {client.dpr_name_of(r) for r in client.dpr_list()}
    if existing:
        print(f"   {len(existing)} data protection rule(s) already on this instance")

    created = 0
    for dsl in dsl_rules:
        name = dsl.get("name", "<unnamed>")
        print(f"   {name}")
        if name in existing:
            print("     [OK ] already exists — left untouched")
            created += 1
            continue
        native = client.dpr_transform(dsl)
        if native is None:
            # The DSL transform (POST /v4/enforcement-transform/utility/json_to_rule)
            # is unavailable on this instance (confirmed live: HTTP 405 WDPPS9010E —
            # a real platform gap, not a bug here). But POST /v3/enforcement/rules
            # itself accepts the friendly DSL shape verbatim on this CPD 5.3 build —
            # confirmed live: a probe rule created with 201 using exactly the DSL
            # shape from 06_data_protection_rules.json, no transform step needed.
            # So: skip the transform, POST the DSL as-is.
            print("     transform endpoint unavailable — POSTing the DSL directly "
                  "instead (confirmed native on this instance, no transform needed).")
            native = dsl
        rules.append(native)

    for payload in rules:
        name = client.dpr_name_of(payload) or "<unnamed>"
        ok, detail = client.dpr_create(payload)
        if ok:
            created += 1
            print(f"     [OK ] created {name} ({detail})")
        else:
            print(f"     [ERR] {name}: {detail}")
            summary["failures"].append(f"{name}: {detail}")

    summary["imported"] = created
    if not client.dry_run:
        live = client.dpr_list()
        summary["published_total"] = len(live)
        print(f"   verify: {len(live)} data protection rule(s) now defined")
    return summary


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Provision the Retail Medallion Lakehouse governance model into "
                    "IBM Knowledge Catalog on CPD 5.3, in dependency order.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--stage", action="append", choices=[s.key for s in STAGES],
                        help="Run only this stage (repeatable). Default: all stages in order.")
    parser.add_argument("--mode", choices=["per-artifact", "bulk"], default="per-artifact",
                        help="per-artifact (default): split each CSV and import+publish one "
                             "artifact at a time, for precise error attribution. bulk: one "
                             "CSV import per artifact type, then publish everything it created.")
    parser.add_argument("--auth", choices=["auto", "token", "api-key", "password"], default="auto",
                        help="Credential to use (default: auto — try an existing bearer token, "
                             "then WXD_API_KEY, then WXD_CPD_PASSWORD, in that order).")
    parser.add_argument("--save-api-key", action="store_true",
                        help="After a password login, rotate a fresh API key into .env so the "
                             "next run needs no password.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show every write that would happen, without performing it.")
    parser.add_argument("--no-publish", action="store_true",
                        help="Import only, leave artifacts in draft.")
    parser.add_argument("--publish-only", action="store_true",
                        help="Publish existing drafts without re-importing.")
    parser.add_argument("--verify-only", action="store_true",
                        help="Read-only: report what is published per artifact type.")
    parser.add_argument("--keep-going", action="store_true",
                        help="Continue to the next stage after one fails, instead of stopping "
                             "(later stages that reference the failed one will likely also fail).")
    parser.add_argument("--skip-dpr", action="store_true",
                        help="Do not create the data protection (masking) rules.")
    parser.add_argument("--dpr-native", metavar="PATH",
                        help="Skip the DSL transform and POST this pre-built native "
                             "enforcement payload (object or array) verbatim.")
    parser.add_argument("--dpr-dump-existing", metavar="PATH",
                        help="Write the instance's current data protection rules to "
                             "PATH as JSON and exit — the way to capture the native "
                             "body shape from a rule built once in the UI.")
    parser.add_argument("--env-file", default=str(ENV_FILE),
                        help="Path to .env (default: repo root .env).")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Log every HTTP request and response status.")
    args = parser.parse_args()

    if args.no_publish and args.publish_only:
        raise SystemExit("--no-publish and --publish-only are mutually exclusive.")

    env_path = Path(args.env_file).expanduser()
    if not env_path.exists():
        raise SystemExit(f".env not found at {env_path}\n  cp .env.example .env  then fill it in.")
    load_dotenv(env_path)

    cpd_host = _env("WXD_CPD_HOST")
    auth_url = os.getenv("WXD_CPD_AUTH_URL", "").strip() or f"https://{cpd_host}{AUTHORIZE_PATH}"
    username = _env("WXD_CPD_USERNAME", "cpadmin")
    verify = _ssl_verify()

    stages = [STAGE_BY_KEY[k] for k in args.stage] if args.stage else list(STAGES)

    print("=== Retail Medallion Lakehouse — IKC governance provisioning ===")
    print(f"CPD host : {cpd_host}")
    print(f"User     : {username}")
    print(f"CSV dir  : {IKC_DIR.relative_to(ROOT)}")
    print(f"Stages   : {', '.join(s.key for s in stages)}")
    print(f"Import   : {args.mode}")
    if args.dry_run:
        print("Mode     : DRY-RUN (no writes)")
    print()

    # --- authenticate ---
    print(f"1. Authenticating as {username} (--auth {args.auth})")
    token = resolve_token(
        auth_url=auth_url, cpd_host=cpd_host, username=username, verify=verify,
        env_path=env_path, method=args.auth, save_api_key=args.save_api_key,
    )

    client = IKCClient(cpd_host, token, verify, dry_run=args.dry_run, verbose=args.verbose)
    print(f"   token {token[:12]}…  uid={client.uid}  [OK]")
    if not client.uid:
        print("   WARNING: could not decode uid from the token — publishing may be "
              "rejected because workflow tasks require an explicit assignee.",
              file=sys.stderr)

    # --- dump-existing short circuit ---
    if args.dpr_dump_existing:
        dump_path = Path(args.dpr_dump_existing).expanduser()
        rules = client.dpr_list()
        dump_path.write_text(json.dumps(rules, indent=2))
        print(f"\n2. Wrote {len(rules)} data protection rule(s) to {dump_path}")
        print("   Edit it down to the rule(s) you want, then replay with:")
        print(f"     python scripts/06b_provision_ikc_governance.py --stage rules --dpr-native {dump_path}")
        return 0

    # --- verify-only short circuit ---
    if args.verify_only:
        print("\n2. Published artifact inventory (read-only)")
        for stage in stages:
            rows = client.search_published(stage.artifact_type)
            drafts = [i for i in client.list_artifacts(stage.artifact_type) if client.is_draft(i)]
            print(f"   {stage.artifact_type:20s} published={len(rows):3d}  draft={len(drafts):3d}")
        dprs = client.dpr_list()
        print(f"   {'data_protection_rule':20s} defined={len(dprs):3d}")
        for rule in dprs:
            print(f"     - {client.dpr_name_of(rule)}")
        return 0

    # --- run stages ---
    print("\n2. Running stages in dependency order")
    summaries = []
    for stage in stages:
        try:
            summaries.append(run_stage(
                client, stage,
                do_import=not args.publish_only,
                do_publish=not args.no_publish,
                mode=args.mode,
            ))
        except IKCError as exc:
            print(f"\n   FAILED at stage '{stage.key}': {exc}", file=sys.stderr)
            summaries.append({"stage": stage.key, "failures": [str(exc)],
                              "imported": None, "published": None,
                              "drafts_left": None, "published_total": None})
            if args.keep_going:
                print("   --keep-going set — continuing to the next stage.", file=sys.stderr)
                continue
            print("   Stopping — later stages reference this one and would fail too. "
                  "Pass --keep-going to continue anyway.", file=sys.stderr)
            _print_summary(summaries, args)
            return 1

    if any(s.key == "reference_data" for s in stages) and not args.no_publish:
        run_reference_values(client)

    if any(s.key == "rules" for s in stages) and not args.skip_dpr:
        summaries.append(run_data_protection_rules(
            client,
            native_path=Path(args.dpr_native).expanduser() if args.dpr_native else None,
        ))

    return _print_summary(summaries, args)


def _print_summary(summaries: list[dict], args) -> int:
    print("\n=== Summary ===")
    print(f"  {'stage':16s} {'imported':>8s} {'published':>9s} {'draft left':>10s} {'live':>5s}")
    failed = False
    for s in summaries:
        imported = "-" if s["imported"] is None else str(s["imported"])
        published = "-" if s["published"] is None else str(s["published"])
        left = "-" if s["drafts_left"] is None else str(s["drafts_left"])
        live = "-" if s["published_total"] is None else str(s["published_total"])
        print(f"  {s['stage']:16s} {imported:>8s} {published:>9s} {left:>10s} {live:>5s}")
        if s["failures"]:
            failed = True
    hard_failures = [f for s in summaries for f in s["failures"]
                     if not f.startswith("skipped:")]
    if hard_failures:
        print("\n  Problems:")
        for f in hard_failures:
            print(f"    - {f}")
    skipped = [f for s in summaries for f in s["failures"] if f.startswith("skipped:")]
    if skipped:
        print("\n  Skipped:")
        for f in skipped:
            print(f"    - {f[9:]}")

    if args.dry_run:
        print("\nDry run — nothing was written.")
        return 0
    if hard_failures:
        print("\nFinished with problems — see above. Re-running is safe "
              "(merge_option=all overwrites in place).")
        return 1
    print("\nAll requested stages imported, published and verified.")
    print("Next: run metadata enrichment on the Gold tables so the data-class "
          "column filters get applied:")
    print("  gold_daily_sales · gold_category_performance · gold_customer_360")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        sys.exit(130)
