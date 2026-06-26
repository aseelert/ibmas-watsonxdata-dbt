#!/usr/bin/env python3
# =============================================================================
#  check_hosts.py — validate required /etc/hosts entries for this cluster
# -----------------------------------------------------------------------------
#  Location  : scripts/check_hosts.py
#  Repository: ibmas-watsonxdata-dbt
#
#  Checks that every required cluster hostname is:
#    1. Present in /etc/hosts  (or resolvable via system DNS)
#    2. Resolves to the expected bastion IP  (9.82.206.23)
#    3. TCP-reachable on the expected port
#
#  USAGE
#    python scripts/check_hosts.py          # from repo root
#    .venv/bin/python scripts/check_hosts.py
#
#  EXIT CODES
#    0 — all entries present and reachable
#    1 — one or more entries missing or unreachable (details printed)
# =============================================================================
from __future__ import annotations

import socket
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Required entries: (hostname, expected_ip, tcp_port)
# ---------------------------------------------------------------------------
BASTION_IP = "9.82.206.23"

REQUIRED: list[tuple[str, str, int]] = [
    ("api.watson.ibmas-zocp-techcluster.org",                                  BASTION_IP, 6443),
    ("api-int.watson.ibmas-zocp-techcluster.org",                              BASTION_IP, 6443),
    ("console-openshift-console.apps.watson.ibmas-zocp-techcluster.org",       BASTION_IP, 443),
    ("oauth-openshift.apps.watson.ibmas-zocp-techcluster.org",                 BASTION_IP, 443),
    ("downloads-openshift-console.apps.watson.ibmas-zocp-techcluster.org",     BASTION_IP, 443),
    ("cpd-cpd-instance.apps.watson.ibmas-zocp-techcluster.org",                BASTION_IP, 443),
    ("ibm-lh-lakehouse-presto651-presto-svc.apps.watson.ibmas-zocp-techcluster.org", BASTION_IP, 443),
    ("ibm-lh-lakehouse-cas-svc-cpd-instance.apps.watson.ibmas-zocp-techcluster.org", BASTION_IP, 443),
    ("ibm-lh-minio-route-cpd-instance.apps.watson.ibmas-zocp-techcluster.org", BASTION_IP, 443),
]

HOSTS_FILE = Path("/etc/hosts") if sys.platform != "win32" \
    else Path(r"C:\Windows\System32\drivers\etc\hosts")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _resolve(host: str) -> str | None:
    """Return first resolved IP or None."""
    try:
        return socket.gethostbyname(host)
    except socket.gaierror:
        return None


def _tcp_ok(host: str, port: int, timeout: float = 3.0) -> bool:
    """Return True if TCP connection to host:port succeeds within timeout."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _in_hosts_file(host: str) -> bool:
    """Return True if host appears literally in the hosts file."""
    try:
        text = HOSTS_FILE.read_text(encoding="utf-8", errors="replace")
        return any(
            host in line and not line.strip().startswith("#")
            for line in text.splitlines()
        )
    except OSError:
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    print(f"\nChecking {HOSTS_FILE} entries for ibmas-zocp-techcluster.org ...\n")

    col = max(len(h) for h, _, _ in REQUIRED) + 2
    failures: list[str] = []
    missing_lines: list[str] = []

    for host, expected_ip, port in REQUIRED:
        in_file   = _in_hosts_file(host)
        resolved  = _resolve(host)
        ip_ok     = resolved == expected_ip
        tcp_ok    = _tcp_ok(host, port) if ip_ok else False

        if ip_ok and tcp_ok:
            status = f"✓  {resolved}  TCP:{port} OK"
        else:
            parts = []
            if not in_file:
                parts.append("MISSING from /etc/hosts")
                missing_lines.append(f"{expected_ip}  {host}")
            elif resolved is None:
                parts.append("DNS resolution failed")
            elif not ip_ok:
                parts.append(f"resolves to {resolved} (expected {expected_ip})")
            if ip_ok and not tcp_ok:
                parts.append(f"TCP:{port} unreachable")
            status = "✗  " + " · ".join(parts)
            failures.append(host)

        print(f"  {host:<{col}} {status}")

    print()

    if missing_lines:
        print("  Add the following lines to /etc/hosts (requires sudo / Administrator):\n")
        for line in missing_lines:
            print(f"    {line}")
        print()
        print("  Quick add (macOS / Linux):")
        print("    sudo tee -a /etc/hosts << 'EOF'")
        for line in missing_lines:
            print(f"    {line}")
        print("    EOF")
        print()

    if failures:
        print(f"  {len(failures)} issue(s) found — fix the entries above and re-run.\n")
        return 1

    print(f"  All {len(REQUIRED)} required entries present and reachable.\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
