#!/usr/bin/env python3
"""
IBM watsonx Projects MCP Server
Exposes tools to list, validate, and inspect CPD/watsonx.ai projects.
Used by IBM Bob and Claude Code to validate project state before operations.
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

env_path = Path(__file__).parent.parent / '.env'
load_dotenv(env_path)

import httpx
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("watsonx-projects")

_CPD_HOST = os.getenv('WXD_CPD_HOST', '')
_USERNAME = os.getenv('WXD_CPD_USERNAME', '')
_PASSWORD = os.getenv('CPADMIN_PASSWORD', '')
_CA_BUNDLE = os.getenv('WXD_SSL_VERIFY', '')

if _CA_BUNDLE and not _CA_BUNDLE.startswith('/'):
    _CA_BUNDLE = str((Path(__file__).parent.parent / _CA_BUNDLE).absolute())


# --- business logic (plain functions, testable without MCP) ---

def _get_token() -> str:
    url = f"https://{_CPD_HOST}/icp4d-api/v1/authorize"
    r = httpx.post(
        url,
        json={"username": _USERNAME, "password": _PASSWORD},
        verify=_CA_BUNDLE,
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["token"]


def _auth_headers() -> dict:
    return {"Authorization": f"Bearer {_get_token()}"}


def _list_projects() -> dict:
    url = f"https://{_CPD_HOST}/v2/projects?limit=100"
    r = httpx.get(url, headers=_auth_headers(), verify=_CA_BUNDLE, timeout=30)
    r.raise_for_status()
    data = r.json()
    projects = [
        {
            "name": res["entity"].get("name"),
            "guid": res["metadata"].get("guid"),
            "creator": res["entity"].get("creator"),
            "type": res["entity"].get("type"),
            "created_at": res["metadata"].get("created_at"),
            "updated_at": res["metadata"].get("updated_at"),
        }
        for res in data.get("resources", [])
    ]
    return {"total": data.get("total_results", 0), "projects": projects}


def _check_project_exists(project_name: str) -> dict:
    result = _list_projects()
    for p in result["projects"]:
        if p["name"].lower() == project_name.lower():
            return {"exists": True, "project": p}
    return {
        "exists": False,
        "searched_name": project_name,
        "available_projects": [p["name"] for p in result["projects"]],
    }


def _get_project_details(project_name_or_guid: str) -> dict:
    headers = _auth_headers()
    val = project_name_or_guid.strip()

    # UUID shape → direct lookup
    if len(val) == 36 and val.count("-") == 4:
        url = f"https://{_CPD_HOST}/v2/projects/{val}"
        r = httpx.get(url, headers=headers, verify=_CA_BUNDLE, timeout=30)
        if r.status_code == 200:
            return r.json()

    # Name → resolve GUID first
    check = _check_project_exists(val)
    if not check["exists"]:
        return {
            "error": f"Project '{val}' not found",
            "available_projects": check.get("available_projects", []),
        }

    guid = check["project"]["guid"]
    url = f"https://{_CPD_HOST}/v2/projects/{guid}"
    r = httpx.get(url, headers=headers, verify=_CA_BUNDLE, timeout=30)
    r.raise_for_status()
    return r.json()


def _validate_connection() -> dict:
    try:
        _get_token()
        result = _list_projects()
        return {
            "connected": True,
            "cpd_host": _CPD_HOST,
            "username": _USERNAME,
            "ca_bundle": _CA_BUNDLE,
            "project_count": result["total"],
            "projects": [p["name"] for p in result["projects"]],
        }
    except Exception as e:
        return {
            "connected": False,
            "cpd_host": _CPD_HOST,
            "error": str(e),
        }


# --- MCP tool wrappers ---

@mcp.tool()
def list_projects() -> dict:
    """
    List all projects in IBM watsonx / Cloud Pak for Data.
    Returns project names, GUIDs, creators, and creation timestamps.
    """
    return _list_projects()


@mcp.tool()
def check_project_exists(project_name: str) -> dict:
    """
    Check whether a named project exists in IBM watsonx.
    Returns exists=True/False, project details when found, and all available
    project names when not found so the caller can suggest alternatives.
    """
    return _check_project_exists(project_name)


@mcp.tool()
def get_project_details(project_name_or_guid: str) -> dict:
    """
    Get full details of a watsonx project by name or GUID.
    Includes storage, scope, membership enforcement, and all metadata fields.
    """
    return _get_project_details(project_name_or_guid)


@mcp.tool()
def validate_watsonx_connection() -> dict:
    """
    Verify connectivity to the IBM watsonx / CPD platform and return
    environment summary (host, username, CA cert path, project count).
    Use this as a health-check before running other tools.
    """
    return _validate_connection()


if __name__ == "__main__":
    missing = [v for v in ("WXD_CPD_HOST", "WXD_CPD_USERNAME", "CPADMIN_PASSWORD", "WXD_SSL_VERIFY") if not os.getenv(v)]
    if missing:
        print(f"Missing env vars: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)
    mcp.run(transport="stdio")
