#!/usr/bin/env python3
"""Upload Retail Medallion Lakehouse governance CSVs to IBM Knowledge Catalog.

Steps performed:
  1. Authenticate with cpadmin credentials (CPADMIN_PASSWORD from .env)
  2. Upload 01_categories.csv   → REST /v3/glossary_terms/import  (categories + terms share this endpoint)
  3. Upload 03_classifications.csv → REST /v3/classifications/import
  4. Upload 04_data_classes.csv  → REST /v3/data_classes/import
  5. Upload 02_business_terms.csv → REST /v3/glossary_terms/import

After running, all artifacts will be in DRAFT state. Publish them via:
  - IBM Knowledge Catalog UI: Governance → publish workflow tasks, OR
  - MCP tools: get_my_workflow_inbox_tasks + perform_workflow_task_action

Once data classes are published, re-run this script with --terms-only to re-import
business terms so that the data-class ↔ term relationships attach correctly.

Usage:
    cd governance/ikc
    python upload_governance.py               # full upload
    python upload_governance.py --dc-only     # data classes only
    python upload_governance.py --terms-only  # business terms only (after DC publish)
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:
    raise SystemExit("pip install python-dotenv")

try:
    import requests
    requests.packages.urllib3.disable_warnings()
except ImportError:
    raise SystemExit("pip install requests")

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
ENV_FILE = ROOT / ".env"
CATALOG_ID = "d742df95-bfe8-4402-a4f3-ce2d18b1c7fb"


def load_env():
    if not ENV_FILE.exists():
        raise SystemExit(f".env not found at {ENV_FILE}")
    load_dotenv(ENV_FILE)
    cpd_host = os.environ.get("WXD_CPD_HOST", "").strip()
    auth_url = os.environ.get("WXD_CPD_AUTH_URL", "").strip()
    password = os.environ.get("CPADMIN_PASSWORD", "").strip()
    if not cpd_host:
        raise SystemExit("WXD_CPD_HOST missing from .env")
    if not password:
        raise SystemExit("CPADMIN_PASSWORD missing from .env")
    return cpd_host, auth_url, password


def get_token(auth_url: str, password: str) -> str:
    print(f"  Authenticating at {auth_url} ...")
    resp = requests.post(
        auth_url,
        json={"username": "cpadmin", "password": password},
        verify=False,
        timeout=30,
    )
    if resp.status_code != 200:
        raise SystemExit(f"Auth failed ({resp.status_code}): {resp.text[:300]}")
    token = resp.json().get("token", "")
    if not token:
        raise SystemExit(f"No token in auth response: {resp.text[:300]}")
    print(f"  Token: {token[:14]}...  [OK]")
    return token


def import_csv(
    cpd_host: str, token: str, endpoint: str, csv_path: Path, label: str, merge: str = "all"
) -> dict:
    url = f"https://{cpd_host}{endpoint}"
    params = {"merge_option": merge}
    headers = {"Authorization": f"Bearer {token}"}
    print(f"\n── {label}")
    print(f"   POST {url}")
    with csv_path.open("rb") as fh:
        resp = requests.post(
            url,
            params=params,
            headers=headers,
            files={"file": (csv_path.name, fh, "text/csv")},
            verify=False,
            timeout=120,
        )
    print(f"   Status: {resp.status_code}")
    if resp.status_code in (200, 201, 202):
        body = resp.json() if resp.headers.get("Content-Type", "").startswith("application/json") else {}
        created = body.get("created_count", body.get("total_count", "?"))
        failed  = body.get("failed_count", 0)
        print(f"   Created: {created}  Failed: {failed}")
        if body.get("errors") or body.get("failed_artifacts"):
            errs = body.get("errors") or body.get("failed_artifacts") or []
            for e in errs[:5]:
                print(f"   WARN: {e}")
        return body
    # Try to show useful error text
    try:
        err = resp.json()
        print(f"   ERROR body: {err}")
    except Exception:
        print(f"   ERROR text: {resp.text[:500]}")
    return {"status_code": resp.status_code, "error": resp.text[:200]}


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload governance CSVs to IKC.")
    parser.add_argument("--dc-only",    action="store_true", help="Upload data classes only")
    parser.add_argument("--cls-only",   action="store_true", help="Upload classifications only")
    parser.add_argument("--terms-only", action="store_true", help="Upload business terms only")
    parser.add_argument("--cats-only",  action="store_true", help="Upload categories only")
    args = parser.parse_args()

    do_all = not any([args.dc_only, args.cls_only, args.terms_only, args.cats_only])

    print("=== Retail Medallion Lakehouse — IKC Governance Upload ===")
    cpd_host, auth_url, password = load_env()
    token = get_token(auth_url, password)

    results = {}

    # Categories
    if do_all or args.cats_only:
        results["categories"] = import_csv(
            cpd_host, token,
            "/v3/governance_artifact_types/category/import",
            HERE / "01_categories.csv",
            "01 — Categories (incl. Data Governance sub-cat)",
        )

    # Classifications
    if do_all or args.cls_only:
        results["classifications"] = import_csv(
            cpd_host, token,
            "/v3/governance_artifact_types/classification/import",
            HERE / "03_classifications.csv",
            "03 — Classifications (Business Data / Personal Data)",
        )

    # Data classes (XML regex definitions)
    if do_all or args.dc_only:
        results["data_classes"] = import_csv(
            cpd_host, token,
            "/v3/governance_artifact_types/data_class/import",
            HERE / "04_data_classes.csv",
            "04 — Data Classes (14 custom regex data classes)",
        )

    # Business terms (import last — data classes must exist first)
    if do_all or args.terms_only:
        results["terms"] = import_csv(
            cpd_host, token,
            "/v3/governance_artifact_types/glossary_term/import",
            HERE / "02_business_terms.csv",
            "02 — Business Terms (14 terms with DC links and related terms)",
        )

    print("\n=== Summary ===")
    for key, body in results.items():
        if isinstance(body, dict) and "status_code" not in body:
            c = body.get("created_count", body.get("total_count", "?"))
            f = body.get("failed_count", 0)
            print(f"  {key:20s} created={c}  failed={f}")
        else:
            code = body.get("status_code", "err")
            print(f"  {key:20s} HTTP {code} — check output above")

    print("""
Next steps:
  1. Publish all drafts in the KnowledgeCatalog UI (governance workflow inbox tasks)
     OR use MCP tools: get_my_workflow_inbox_tasks → perform_workflow_task_action
  2. After data classes are published, re-import business terms so DC links attach:
       python upload_governance.py --terms-only
  3. Verify via MCP: list_data_classes_by_search_term("Retail Medallion")
                     list_business_terms_by_search_term("Customer ID")
""")


if __name__ == "__main__":
    main()
