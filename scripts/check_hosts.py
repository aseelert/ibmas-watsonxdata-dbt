#!/usr/bin/env python3
# =============================================================================
#  check_hosts.py — validate required /etc/hosts entries for this cluster
# -----------------------------------------------------------------------------
#  Location  : scripts/check_hosts.py
#  Repository: https://github.com/aseelert/ibmas-watsonxdata-dbt
#  Project   : watsonx.data · dbt · Spark · Confluent medallion demo
#  Author    : Alexander Seelert — IBM Customer Success Engineer
#  Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.
#
#  Changelog :
#    v1.0 (2026-06-26) — Initial version. Checks that every required cluster
#      hostname is present in /etc/hosts, resolves to the expected bastion IP,
#      and is TCP-reachable. ALL cluster values (bastion IP, base + apps
#      domains) now come from environment variables / .env — nothing is baked
#      into the code. The previous repo cluster remains the documented default.
# -----------------------------------------------------------------------------
#
#  WHY does this exist?
#    To reach an OpenShift cluster through a bastion host, your laptop's
#    /etc/hosts must map each cluster hostname to the bastion's public IP.
#    This script checks all required mappings in one go so a learner can see at
#    a glance what is missing — instead of hitting cryptic "connection refused"
#    errors later.
#
#  NOTHING IS HARDCODED
#    Every host/IP/domain is read from an environment variable (typically set
#    in <repo-root>/.env). The defaults below simply reproduce the cluster this
#    demo was built against, so the script still works out-of-the-box. Point it
#    at a different cluster by exporting the env vars (or editing .env):
#
#      WXD_BASTION_IP           public IP the bastion forwards from
#                               (default: 9.82.206.23)
#      WXD_CLUSTER_BASE_DOMAIN  base cluster domain, used for the API hosts
#                               (default: watson.ibmas-zocp-techcluster.org)
#      WXD_CLUSTER_APPS_DOMAIN  wildcard *.apps domain for routes/consoles
#                               (default: apps.<WXD_CLUSTER_BASE_DOMAIN>)
#
#  USAGE
#    python scripts/check_hosts.py            # from repo root, uses .env / env
#    python scripts/check_hosts.py --help     # show all options
#    python scripts/check_hosts.py --env-file path/to/.env
#    python scripts/check_hosts.py --bastion-ip 10.0.0.5 \
#        --base-domain my.cluster.example.com
#
#  EXIT CODES
#    0 — all entries present and reachable
#    1 — one or more entries missing or unreachable (details printed)
#    2 — bad arguments / unexpected internal error
# =============================================================================
from __future__ import annotations

import argparse
import logging
import os
import socket
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
#  Logging — student-friendly. We log diagnostics (what the script is doing,
#  warnings, errors) via the logging module to stderr, and keep the pretty,
#  aligned RESULTS table on stdout (so it can be read or piped cleanly).
# ---------------------------------------------------------------------------
log = logging.getLogger("check_hosts")


# ---------------------------------------------------------------------------
#  Defaults — these are EXAMPLE values for the cluster this demo ships with.
#  They are NOT authoritative config: every one can be overridden by an env
#  var or a CLI flag. Keeping them here means the script runs out-of-the-box,
#  while still being fully driven by configuration.
# ---------------------------------------------------------------------------
DEFAULT_BASTION_IP = "9.82.206.23"
DEFAULT_BASE_DOMAIN = "watson.ibmas-zocp-techcluster.org"

# Which short host-names live under the *.apps wildcard domain, and on which
# TCP port we expect them to answer. The fully-qualified name is built at
# runtime as "<prefix>.<apps_domain>" so changing the domain updates them all.
APPS_HOST_PREFIXES: list[tuple[str, int]] = [
    ("console-openshift-console", 443),
    ("oauth-openshift", 443),
    ("downloads-openshift-console", 443),
    ("cpd-cpd-instance", 443),
    ("ibm-lh-lakehouse-presto651-presto-svc", 443),
    ("ibm-lh-lakehouse-cas-svc-cpd-instance", 443),
    ("ibm-lh-minio-route-cpd-instance", 443),
]

# Hosts that live directly under the BASE domain (the OpenShift API endpoints),
# with their expected TCP port.
BASE_HOST_PREFIXES: list[tuple[str, int]] = [
    ("api", 6443),
    ("api-int", 6443),
]

HOSTS_FILE = (
    Path("/etc/hosts")
    if sys.platform != "win32"
    else Path(r"C:\Windows\System32\drivers\etc\hosts")
)


# ---------------------------------------------------------------------------
#  .env loading — tiny, dependency-free parser. We do NOT want to require
#  python-dotenv just to read a few KEY=value lines, so we parse them by hand.
#  Existing environment variables WIN over .env values (so an explicit export
#  or CLI flag always takes precedence — principle of least surprise).
# ---------------------------------------------------------------------------
def load_env_file(env_file: Path) -> None:
    """Load simple KEY=value lines from *env_file* into os.environ.

    Lines that are blank, comments (#...), or malformed are skipped. Values may
    be wrapped in single/double quotes. Variables already present in the real
    environment are left untouched (real env beats the file).
    """
    if not env_file.is_file():
        log.warning("No .env found at %s — using current environment only.", env_file)
        return

    log.info("Loading environment from %s", env_file)
    for raw in env_file.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        # Allow an optional leading "export " for shell-style .env files.
        if line.startswith("export "):
            line = line[len("export "):].lstrip()
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def repo_root() -> Path:
    """Repo root = one directory above this file's 'scripts/' folder."""
    return Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
#  Build the required-entry list from config (env vars + CLI overrides).
# ---------------------------------------------------------------------------
def build_required(
    bastion_ip: str,
    base_domain: str,
    apps_domain: str,
) -> list[tuple[str, str, int]]:
    """Return the list of (hostname, expected_ip, tcp_port) tuples to check."""
    required: list[tuple[str, str, int]] = []
    for prefix, port in BASE_HOST_PREFIXES:
        required.append((f"{prefix}.{base_domain}", bastion_ip, port))
    for prefix, port in APPS_HOST_PREFIXES:
        required.append((f"{prefix}.{apps_domain}", bastion_ip, port))
    return required


# ---------------------------------------------------------------------------
#  Low-level network/file helpers
# ---------------------------------------------------------------------------
def _resolve(host: str) -> str | None:
    """Return the first resolved IP, or None if DNS/hosts resolution fails."""
    try:
        return socket.gethostbyname(host)
    except socket.gaierror:
        return None


def _tcp_ok(host: str, port: int, timeout: float = 3.0) -> bool:
    """Return True if a TCP connection to host:port succeeds within timeout."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _in_hosts_file(host: str) -> bool:
    """Return True if *host* appears in a non-comment line of the hosts file."""
    try:
        text = HOSTS_FILE.read_text(encoding="utf-8", errors="replace")
    except OSError as exc:
        log.warning("Could not read %s: %s", HOSTS_FILE, exc)
        return False
    return any(
        host in line and not line.strip().startswith("#")
        for line in text.splitlines()
    )


# ---------------------------------------------------------------------------
#  Core check — runs every required entry and prints an aligned report.
# ---------------------------------------------------------------------------
def run_checks(required: list[tuple[str, str, int]]) -> int:
    """Run all checks; return 0 if everything passes, 1 otherwise."""
    print(f"\nChecking {HOSTS_FILE} entries ...\n")

    col = max(len(h) for h, _, _ in required) + 2
    failures: list[str] = []
    missing_lines: list[str] = []

    for host, expected_ip, port in required:
        in_file = _in_hosts_file(host)
        resolved = _resolve(host)
        ip_ok = resolved == expected_ip
        tcp_ok = _tcp_ok(host, port) if ip_ok else False

        if ip_ok and tcp_ok:
            status = f"✓  {resolved}  TCP:{port} OK"
        else:
            parts: list[str] = []
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

    print(f"  All {len(required)} required entries present and reachable.\n")
    return 0


# ---------------------------------------------------------------------------
#  Argument parsing
# ---------------------------------------------------------------------------
def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="check_hosts.py",
        description=(
            "Validate that required cluster hostnames are in /etc/hosts, "
            "resolve to the expected bastion IP, and are TCP-reachable. "
            "All values come from env vars / .env — see defaults below."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=repo_root() / ".env",
        help="Path to the .env file to load before reading env vars.",
    )
    parser.add_argument(
        "--bastion-ip",
        default=None,
        help="Override WXD_BASTION_IP (public IP the bastion forwards from).",
    )
    parser.add_argument(
        "--base-domain",
        default=None,
        help="Override WXD_CLUSTER_BASE_DOMAIN (base cluster domain for API hosts).",
    )
    parser.add_argument(
        "--apps-domain",
        default=None,
        help="Override WXD_CLUSTER_APPS_DOMAIN (defaults to 'apps.<base-domain>').",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable DEBUG-level logging.",
    )
    return parser.parse_args(argv)


# ---------------------------------------------------------------------------
#  Main — wires config together and runs the checks, with robust error handling.
# ---------------------------------------------------------------------------
def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="  [%(levelname)s] %(message)s",
        stream=sys.stderr,
    )

    try:
        # 1. Load .env (real environment variables still win over file values).
        load_env_file(args.env_file)

        # 2. Resolve config: CLI flag > env var > built-in default example.
        bastion_ip = (
            args.bastion_ip
            or os.environ.get("WXD_BASTION_IP")
            or DEFAULT_BASTION_IP
        )
        base_domain = (
            args.base_domain
            or os.environ.get("WXD_CLUSTER_BASE_DOMAIN")
            or DEFAULT_BASE_DOMAIN
        )
        apps_domain = (
            args.apps_domain
            or os.environ.get("WXD_CLUSTER_APPS_DOMAIN")
            or f"apps.{base_domain}"
        )

        log.debug("bastion_ip=%s", bastion_ip)
        log.debug("base_domain=%s", base_domain)
        log.debug("apps_domain=%s", apps_domain)

        # 3. Build the required-entry list and run the checks.
        required = build_required(bastion_ip, base_domain, apps_domain)
        return run_checks(required)

    except KeyboardInterrupt:
        log.error("Interrupted by user.")
        return 2
    except Exception:  # noqa: BLE001 — top-level guard: log full context, exit 2
        log.exception("Unexpected error while checking cluster hosts.")
        return 2


if __name__ == "__main__":
    sys.exit(main())
