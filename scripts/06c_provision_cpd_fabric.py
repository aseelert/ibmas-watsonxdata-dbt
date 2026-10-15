#!/usr/bin/env python3
# -----------------------------------------------------------------------------
#  06c_provision_cpd_fabric.py — project, connection, DSD, metadata import and
#                                metadata enrichment for the medallion demo
#
#  Location  : scripts/06c_provision_cpd_fabric.py
#  Repository: https://github.com/aseelert/ibmas-watsonxdata-dbt
#  Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.
# -----------------------------------------------------------------------------
"""Create the CPD / IBM Knowledge Catalog objects the medallion demo needs.

Runs the steps below in order. Every step is idempotent: it looks the object up
by NAME first and only creates what is missing, so re-running is safe.

  1. project      ibmas-dlh-medallion-project
  2. connection   ibmas-watsonx-data-presto-con   (project + Platform assets catalog)
  3. DSD          ibmas-watsonx-data-presto-dsd   (Platform assets catalog)
  4. MDI          ibmas-dlh-medallion-mdi         (metadata import of the Gold schema)
  5. MDE          ibmas-dlh-medallion-mde         (profile, quality, relationships)

All names are lower case and come from .env (WXD_CPD_PROJECT, WXD_CPD_CONNECTION,
WXD_CPD_DSD, WXD_CPD_MDI, WXD_CPD_MDE), with the defaults above.

Things this script handles because the API does not do them for you:
  * Objects that exist under an older name (ibmas-ingest-demo, ibmas-presto,
    IBMAS-...-DSD) are RENAMED in place, never deleted and recreated, because
    recreating a connection orphans everything imported through it.
  * A connection must be created with origin_country ("us"); setting it later
    does not satisfy the metadata-import service.
  * The saved credential in a connection goes stale whenever the API key is
    rotated (every `bin/demo setup` fetches one), so the key is refreshed in
    the connection on every run.

Usage:
  bin/demo ikc setup               # create / adopt everything, run MDI + MDE if new
  bin/demo ikc setup --run         # also (re-)run the import and enrichment jobs
  bin/demo ikc setup --dry-run     # show what would happen
  bin/demo ikc verify              # read-only check of every object
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sys
import time
from pathlib import Path

import requests
from dotenv import load_dotenv

requests.packages.urllib3.disable_warnings()

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

LAKEHOUSE_TYPE_ID = "d22e3e36-5ab3-4361-b418-2dba4b227879"   # datasource type "lakehouse" (watsonx.data Presto)
ROOT_CATEGORY = "Retail Medallion Lakehouse"

NAMES = {
    "project": os.getenv("WXD_CPD_PROJECT", "ibmas-dlh-medallion-project"),
    "connection": os.getenv("WXD_CPD_CONNECTION", "ibmas-watsonx-data-presto-con"),
    "dsd": os.getenv("WXD_CPD_DSD", "ibmas-watsonx-data-presto-dsd"),
    "mdi": os.getenv("WXD_CPD_MDI", "ibmas-dlh-medallion-mdi"),
    "mde": os.getenv("WXD_CPD_MDE", "ibmas-dlh-medallion-mde"),
}
# Names used before the naming scheme existed; adopted (renamed) when found.
LEGACY = {
    "project": ["ibmas-ingest-demo"],
    "connection": ["ibmas-presto"],
    "dsd": ["IBMAS-Watsonx-Data-Presto-DSD"],
}


def say(msg: str = "") -> None:
    print(msg, flush=True)


class Fabric:
    def __init__(self, dry_run: bool):
        self.dry = dry_run
        self.host = os.environ["WXD_CPD_HOST"]
        self.base = f"https://{self.host}"
        self.s = requests.Session()
        self.s.verify = False
        self.api_key = os.environ["WXD_API_KEY"]
        self.s.headers["Authorization"] = f"Bearer {self._token()}"

    # -- plumbing ----------------------------------------------------------
    def _token(self) -> str:
        tok = os.getenv("WXD_SPARK_BEARER_TOKEN", "")
        if tok:
            r = requests.get(f"{self.base}/v2/projects?limit=1", headers={"Authorization": f"Bearer {tok}"}, verify=False, timeout=30)
            if r.status_code < 400:
                return tok
        r = requests.post(f"{self.base}/icp4d-api/v1/authorize", verify=False, timeout=30,
                          json={"username": os.getenv("WXD_CPD_USERNAME", "cpadmin"), "api_key": self.api_key})
        r.raise_for_status()
        return r.json()["token"]

    def call(self, method, path, *, write=False, **kw):
        if write and self.dry:
            say(f"   [dry-run] {method} {path}")
            return None
        kw.setdefault("timeout", 180)
        return self.s.request(method, self.base + path, **kw)

    def j(self, resp) -> dict:
        try:
            return resp.json() if resp is not None and resp.content else {}
        except ValueError:
            return {}

    def err(self, resp) -> str:
        d = self.j(resp)
        e = (d.get("errors") or [{}])[0]
        return f"HTTP {resp.status_code}: {e.get('message') or resp.text[:200]}"

    # -- project -----------------------------------------------------------
    def project_id(self, name: str) -> str | None:
        r = self.call("GET", "/v2/projects", params={"limit": 100})
        for p in self.j(r).get("resources", []):
            if p["entity"]["name"] == name:
                return p["metadata"]["guid"]
        return None

    def ensure_project(self) -> str | None:
        want = NAMES["project"]
        pid = self.project_id(want)
        if pid:
            say(f"   project present: {want} ({pid})")
            return pid
        for old in LEGACY["project"]:
            pid = self.project_id(old)
            if pid:
                r = self.call("PATCH", f"/v2/projects/{pid}", write=True, json={"name": want})
                say(f"   project renamed {old} -> {want} ({pid})" if r is None or r.status_code < 300 else f"   rename FAILED: {self.err(r)}")
                return pid
        body = {"name": want, "description": "Retail Medallion Lakehouse (Data Lakehouse) demo project", "generator": "provision-cpd-fabric",
                "type": "cpd", "public": False, "enforce_members": False, "storage": {"type": "assetfiles"}, "tags": ["dlh_medallion"]}
        r = self.call("POST", "/transactional/v2/projects", write=True, params={"verify_unique_name": "true"}, json=body)
        if r is None:
            return None
        if r.status_code >= 300:
            say(f"   project create FAILED: {self.err(r)}")
            return None
        loc = self.j(r).get("location", "")
        pid = loc.rsplit("/", 1)[-1] if loc else self.project_id(want)
        say(f"   project created: {want} ({pid})")
        return pid

    # -- connection --------------------------------------------------------
    def platform_catalog_id(self) -> str | None:
        for c in self.j(self.call("GET", "/v2/catalogs", params={"limit": 100})).get("catalogs", []):
            if "platform assets catalog" in c["entity"]["name"].lower():
                return c["metadata"]["guid"]
        return None

    def _conn_props(self) -> dict:
        cert = (ROOT / "certs" / "watsonxdata-ca.pem").read_text()
        return {
            "host": os.environ["WXD_CPD_HOST"], "port": 443, "ssl": True, "ssl_certificate": cert,
            "engine_host": os.environ["WXD_HOST"], "engine_port": 443,
            "engine_id": os.getenv("WXD_PRESTO_ENGINE_ID", "presto"), "engine_ssl": True, "engine_ssl_certificate": cert,
            "instance_id": os.environ["WXD_INSTANCE_ID"], "deployment_type": "software_ent",
            "auth_method": "username_apikey", "username": os.getenv("WXD_CPD_USERNAME", "cpadmin"), "api_key": self.api_key,
        }

    def find_connection(self, scope: str, sid: str, names: list[str]) -> tuple[str, str] | None:
        for c in self.j(self.call("GET", "/v2/connections", params={scope: sid})).get("resources", []):
            if c["entity"]["name"] in names:
                return c["metadata"]["asset_id"], c["entity"]["name"]
        return None

    def ensure_connection(self, scope: str, sid: str, label: str) -> str | None:
        want = NAMES["connection"]
        found = self.find_connection(scope, sid, [want] + LEGACY["connection"])
        if found:
            cid, cur = found
            ops = [{"op": "replace", "path": "/properties/api_key", "value": self.api_key}]
            if cur != want:
                ops.insert(0, {"op": "replace", "path": "/name", "value": want})
            r = self.call("PATCH", f"/v2/connections/{cid}", write=True, params={scope: sid}, json=ops,
                          headers={"Content-Type": "application/json-patch+json"})
            ok = r is None or r.status_code < 300
            say(f"   {label} connection {'renamed ' + cur + ' -> ' if cur != want else 'present: '}{want}{'' if self.dry else '; credential refreshed'}" if ok
                else f"   {label} connection update FAILED: {self.err(r)}")
            return cid
        body = {"name": want, "description": "watsonx.data Presto (iceberg_data) for the Retail Medallion Lakehouse demo",
                "datasource_type": LAKEHOUSE_TYPE_ID, "origin_country": "us", "properties": self._conn_props()}
        r = self.call("POST", "/v2/connections", write=True, params={scope: sid}, json=body)
        if r is None:
            return None
        if r.status_code >= 300:
            say(f"   {label} connection create FAILED: {self.err(r)}")
            return None
        cid = self.j(r)["metadata"]["asset_id"]
        say(f"   {label} connection created: {want} ({cid})")
        return cid

    # -- DSD ---------------------------------------------------------------
    def ensure_dsd(self, catalog_id: str) -> str | None:
        want = NAMES["dsd"]
        r = self.call("POST", "/v2/asset_types/ibm_data_source/search", params={"catalog_id": catalog_id, "limit": 50}, json={"query": "*:*"})
        items = {a["metadata"]["name"]: a["metadata"]["asset_id"] for a in self.j(r).get("results", [])}
        if want in items:
            say(f"   DSD present: {want}")
            return items[want]
        for old in LEGACY["dsd"]:
            if old in items:
                r = self.call("PATCH", f"/v2/assets/{items[old]}", write=True, params={"catalog_id": catalog_id},
                              json=[{"op": "replace", "path": "/metadata/name", "value": want}],
                              headers={"Content-Type": "application/json-patch+json"})
                say(f"   DSD renamed {old} -> {want}")
                return items[old]
        body = {"metadata": {"name": want, "description": "watsonx.data Presto engine (iceberg_data) - Retail Medallion Lakehouse demo",
                             "asset_type": "ibm_data_source", "origin_country": "us", "rov": {"mode": 0}},
                "entity": {"ibm_data_source": {"data_source_state": "ACTIVE", "data_source_type_id": LAKEHOUSE_TYPE_ID,
                                               "data_source_endpoints": {"values": [{"host": os.environ["WXD_HOST"], "port": 443}]}}}}
        r = self.call("POST", "/v2/assets", write=True, params={"catalog_id": catalog_id}, json=body)
        if r is None:
            return None
        if r.status_code >= 300:
            say(f"   DSD create FAILED: {self.err(r)}")
            return None
        say(f"   DSD created: {want}")
        return self.j(r)["metadata"]["asset_id"]

    # -- jobs --------------------------------------------------------------
    def run_job(self, job_id: str, pid: str, label: str, timeout_s: int = 1200) -> str:
        r = self.call("POST", f"/v2/jobs/{job_id}/runs", write=True, params={"project_id": pid}, json={"job_run": {}})
        if r is None:
            return "dry-run"
        if r.status_code >= 300:
            return f"start failed ({self.err(r)})"
        run = self.j(r)["metadata"]["asset_id"]
        end = time.time() + timeout_s
        while time.time() < end:
            time.sleep(10)
            st = self.j(self.call("GET", f"/v2/jobs/{job_id}/runs/{run}", params={"project_id": pid})).get("entity", {}).get("job_run", {}).get("state")
            if st in ("Completed", "Failed", "Canceled", "CompletedWithErrors", "CompletedWithWarnings"):
                return st
        return "timeout"

    # -- MDI ---------------------------------------------------------------
    def ensure_mdi(self, pid: str, conn_id: str, paths: list[str]) -> tuple[str | None, str | None, bool]:
        want = NAMES["mdi"]
        r = self.call("POST", "/v2/asset_types/metadata_import/search", params={"project_id": pid, "limit": 50}, json={"query": "*:*"})
        for m in self.j(r).get("results", []):
            if m["metadata"]["name"] == want:
                mid = m["metadata"]["asset_id"]
                job = self.j(self.call("GET", f"/v2/metadata_imports/{mid}", params={"project_id": pid})).get("entity", {}).get("job_id")
                say(f"   MDI present: {want}")
                return mid, job, False
        body = {"unified_lineage": True, "import_type": "metadata", "name": want, "connection_id": conn_id,
                "description": "Retail Medallion Lakehouse - Gold tables from watsonx.data Presto",
                "target_project_id": pid, "tags": [], "scope": {"paths": paths}}
        r = self.call("POST", "/v2/metadata_imports", write=True,
                      params={"project_id": pid, "job_name": want + "_job", "create_job": "true"}, json=body)
        if r is None:
            return None, None, True
        if r.status_code >= 300:
            say(f"   MDI create FAILED: {self.err(r)}")
            return None, None, False
        d = self.j(r)
        say(f"   MDI created: {want}  scope={paths}")
        return d["metadata"]["asset_id"], d.get("entity", {}).get("job_id"), True

    # -- MDE ---------------------------------------------------------------
    def ensure_mde(self, pid: str, mdi_id: str, category_id: str | None) -> tuple[str | None, str | None, bool]:
        want = NAMES["mde"]
        r = self.call("POST", "/v2/asset_types/metadata_enrichment_area/search", params={"project_id": pid, "limit": 50}, json={"query": "*:*"})
        for a in self.j(r).get("results", []):
            if a["metadata"]["name"] == want:
                job = self._mde_job(pid, a["metadata"]["asset_id"])
                say(f"   MDE present: {want}")
                return a["metadata"]["asset_id"], job, False
        checks = ["case", "completeness", "data_type", "format", "uniqueness", "range", "regex", "length", "possible_values",
                  "data_class", "nonstandard_missing_values", "rule", "suspect_values", "referential_integrity", "history_stability"]
        body = {"name": want, "job": {"name": want},
                "data_scope": {"container_assets": {"metadata_imports": [mdi_id]}},
                "objective": {
                    "enrichment_options": {"structured": {"profile": True, "assign_terms": False, "analyze_quality": True,
                                                          "analyze_relationships": True, "semantic_expansion": False, "data_search": False,
                                                          "dq_sla_assessment": False, "dq_gen_constraints": False}},
                    "governance_scope": [{"id": category_id, "type": "category"}] if category_id else [],
                    "sampling": {"structured": {"method": "top", "analysis_method": "fixed",
                                                "sample_size": {"options": {"row_number": 1000, "classify_value_number": 100}}}},
                    "datascope_of_reruns": "all",
                    "data_quality": {"structured": {"dq_checks_suggested": [{"id": c, "enabled": True} for c in checks],
                                                    "quality_origins": {"profiling": True, "business_terms": False, "relationships": False}}}}}
        r = self.call("POST", "/metadata_enrichment/v3/metadata_enrichment_assets", write=True, params={"project_id": pid}, json=body)
        if r is None:
            return None, None, True
        if r.status_code >= 300:
            say(f"   MDE create FAILED: {self.err(r)}")
            return None, None, False
        mde = self.j(r).get("target_resource_id")
        say(f"   MDE created: {want} ({mde})")
        return mde, self._mde_job(pid, mde), True

    def _mde_job(self, pid: str, mde_id: str) -> str | None:
        d = self.j(self.call("GET", f"/v2/assets/{mde_id}", params={"project_id": pid}))
        return d.get("entity", {}).get("metadata_enrichment_area", {}).get("job", {}).get("id")


def root_category_id(fabric: Fabric) -> str | None:
    """Category ID of the demo's root category, via the governance script's parent walk."""
    spec = importlib.util.spec_from_file_location("gov06b", ROOT / "scripts" / "06b_provision_ikc_governance.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    client = mod.IKCClient(fabric.host, fabric.s.headers["Authorization"].split()[1], False)
    return client.category_ids_via_walk().get(ROOT_CATEGORY)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("action", choices=["setup", "verify"], nargs="?", default="setup")
    ap.add_argument("--dry-run", action="store_true", help="show what would happen, change nothing")
    ap.add_argument("--run", action="store_true", help="(re-)run the import and enrichment jobs even if they already exist")
    ap.add_argument("--schemas", default=os.getenv("WXD_CPD_MDI_SCHEMAS", os.getenv("WXD_GOLD_SCHEMA", "dbt_demo_gold")),
                    help="comma-separated schemas for the metadata import (default: the dbt Gold schema)")
    args = ap.parse_args()

    f = Fabric(args.dry_run or args.action == "verify")
    catalog = os.getenv("WXD_CATALOG", "iceberg_data")
    paths = [f"/{catalog}/{s.strip()}" for s in args.schemas.split(",") if s.strip()]
    say(f"=== CPD fabric: {args.action}{' (dry-run)' if f.dry else ''} on {f.host} ===")

    say("\n1. project")
    pid = f.ensure_project() if args.action == "setup" else f.project_id(NAMES["project"])
    if not pid:
        say("   no project - stopping")
        return 0 if f.dry else 1

    plat = f.platform_catalog_id()
    say("\n2. connection")
    conn = f.ensure_connection("project_id", pid, "project")
    if plat:
        f.ensure_connection("catalog_id", plat, "platform")
    else:
        say("   Platform assets catalog not found - skipping platform connection")

    say("\n3. data source definition")
    if plat:
        f.ensure_dsd(plat)

    say("\n4. metadata import")
    mdi, mdi_job, mdi_new = (None, None, False)
    if conn:
        mdi, mdi_job, mdi_new = f.ensure_mdi(pid, conn, paths)
        if mdi_job and (mdi_new or args.run):
            say(f"   running import job ... {f.run_job(mdi_job, pid, 'MDI', 600)}")

    say("\n5. metadata enrichment")
    if mdi:
        cat = root_category_id(f)
        if not cat:
            say(f"   note: category '{ROOT_CATEGORY}' not found - run `bin/demo governance` first for data-class scoping")
        mde, mde_job, mde_new = f.ensure_mde(pid, mdi, cat)
        if mde_job and (mde_new or args.run):
            say("   running enrichment job (about 8 minutes) ...")
            say(f"   result: {f.run_job(mde_job, pid, 'MDE', 1500)}")

    say("\n=== done ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
