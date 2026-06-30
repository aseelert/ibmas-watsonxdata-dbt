#!/usr/bin/env bash
# =============================================================================
#  expose_minio_route.sh — create an OpenShift Route for the watsonx.data MinIO
# -----------------------------------------------------------------------------
#  Location  : confluent/scripts/expose_minio_route.sh
#  Repository: https://github.com/aseelert/ibmas-watsonxdata-dbt
#  Project   : watsonx.data · dbt · Spark · Confluent medallion demo
#  Author    : Alexander Seelert — IBM Customer Success Engineer
#  Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.
#
#  Purpose   : One-time setup that exposes ibm-lh-lakehouse-minio-svc as an
#              HTTPS OpenShift Route so Docker containers (Flink, iceberg-rest,
#              confluent-prep) can write to / read from the real iceberg-bucket
#              without an oc port-forward tunnel.
#
#  Run ONCE before: docker compose up -d  (from repo root)
#
#  ENV VARS read (from .env in repo root):
#    WXD_OPENSHIFT_API        — OpenShift API URL   (required)
#    WXD_OC_TOKEN             — oc login token      (required if not logged in)
#    WXD_OPENSHIFT_NAMESPACE  — namespace           (default: cpd-instance)
#    WXD_OPENSHIFT_CONTEXT    — kubeconfig context  (optional, pin cluster)
#    WXD_OC_USER/WXD_OC_PASSWORD — fallback login   (optional)
#    WXD_BASTION_IP           — public bastion IP for /etc/hosts entry
#                               (default example: 9.82.206.23; otherwise
#                               auto-detected from an existing *.apps line)
#    WXD_CLUSTER_APPS_DOMAIN  — *.apps wildcard domain, used only as a hint in
#                               messages (default example:
#                               apps.watson.ibmas-zocp-techcluster.org)
#
#  After success, copy the printed WXD_OBJECT_STORE_ENDPOINT line into .env.
#
#  Changelog :
#    v1.0 (2026-06-26) — Initial version. Creates the MinIO edge Route,
#      registers /etc/hosts, and writes WXD_OBJECT_STORE_ENDPOINT to .env.
#      Sources scripts/lib/log.sh (fixes the undefined warn() on the failure
#      path), installs an ERR trap, and drives ALL cluster hostnames/IPs from
#      env vars — the previous repo cluster stays only as a documented default.
# =============================================================================
set -euo pipefail

# ---------------------------------------------------------------------------
#  Locate the repo root and pull in the shared logging/env library.
#  scripts/lib/log.sh gives us: info/success/warn/error/step, load_env,
#  confirm, run, install_err_trap. Sourcing it FIXES the previous bug where
#  warn() was called on the failure path but never defined.
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "${SCRIPT_DIR}/../.." && pwd)"

LOG_LIB="${REPO}/scripts/lib/log.sh"
if [[ -f "$LOG_LIB" ]]; then
  # shellcheck source=/dev/null
  source "$LOG_LIB"
else
  # Minimal fallback so the script never dies with "warn: command not found"
  # if the shared library is missing. Same [LEVEL] style as log.sh.
  info()    { echo "  [INFO]  $*"; }
  success() { echo "  [OK]    $*"; }
  warn()    { echo "  [WARN]  $*" >&2; }
  error()   { echo "  [ERROR] $*" >&2; }
  step()    { echo ""; echo "▶  $*"; }
  install_err_trap() {
    trap 'rc=$?; error "Command failed (exit ${rc}) at ${BASH_SOURCE[1]:-?}:${BASH_LINENO[0]:-?} → ${BASH_COMMAND}"' ERR
  }
fi

install_err_trap

# ---------------------------------------------------------------------------
#  Load .env (KEY=value lines become exported shell vars). We need them in the
#  current shell so the oc commands below can see them.
# ---------------------------------------------------------------------------
if [[ -f "${REPO}/.env" ]]; then
  # shellcheck disable=SC1090
  set -a; source "${REPO}/.env"; set +a
else
  error "${REPO}/.env not found. Copy .env.example to .env and fill in values."
  exit 1
fi

# ---------------------------------------------------------------------------
#  Configuration — every value below has a sensible default but can be
#  overridden via .env / environment. NOTHING cluster-specific is hardcoded as
#  a code literal; the values shown are the documented EXAMPLE cluster.
# ---------------------------------------------------------------------------
NAMESPACE="${WXD_OPENSHIFT_NAMESPACE:-cpd-instance}"
ROUTE_NAME="ibm-lh-minio-route"
SVC_NAME="ibm-lh-lakehouse-minio-svc"

# Example-only defaults used purely for human-readable hints (never baked into
# the actual route creation, which uses the cluster's own assigned hostname).
EXAMPLE_BASTION_IP="${WXD_BASTION_IP:-9.82.206.23}"

# Optional context args — pin to avoid acting on the wrong cluster.
CTX_ARGS=()
if [[ -n "${WXD_OPENSHIFT_CONTEXT:-}" ]]; then
  CTX_ARGS=("--context" "${WXD_OPENSHIFT_CONTEXT}")
fi

# ---------------------------------------------------------------------------
#  1. Ensure oc is logged in to the correct cluster.
#     Priority: WXD_OPENSHIFT_CONTEXT (existing kubeconfig context, fastest)
#               > WXD_OC_TOKEN  > WXD_OC_USER/WXD_OC_PASSWORD
#     Always re-login if the current server doesn't match WXD_OPENSHIFT_API.
# ---------------------------------------------------------------------------
step "Step 1 — Connect to the OpenShift cluster"

TARGET_API="${WXD_OPENSHIFT_API:-}"
if [[ -z "$TARGET_API" ]]; then
  error "WXD_OPENSHIFT_API is not set in .env."
  exit 1
fi

CURRENT_SERVER=$(oc "${CTX_ARGS[@]}" whoami --show-server 2>/dev/null || true)

# Normalise both URLs (strip trailing slash) for comparison.
_norm() { echo "${1%/}"; }

if [[ "$(_norm "$CURRENT_SERVER")" == "$(_norm "$TARGET_API")" ]]; then
  info "Already connected to ${TARGET_API}"
elif [[ -n "${WXD_OC_TOKEN:-}" ]]; then
  info "Logging in to ${TARGET_API} with WXD_OC_TOKEN ..."
  oc "${CTX_ARGS[@]}" login "${TARGET_API}" \
    --token="${WXD_OC_TOKEN}" \
    --insecure-skip-tls-verify=true
elif [[ -n "${WXD_OC_USER:-}" && -n "${WXD_OC_PASSWORD:-}" ]]; then
  info "Logging in to ${TARGET_API} as ${WXD_OC_USER} ..."
  oc "${CTX_ARGS[@]}" login "${TARGET_API}" \
    --username="${WXD_OC_USER}" \
    --password="${WXD_OC_PASSWORD}" \
    --insecure-skip-tls-verify=true
else
  error "Cannot log in — set WXD_OPENSHIFT_CONTEXT, WXD_OC_TOKEN, or WXD_OC_USER+WXD_OC_PASSWORD in .env."
  exit 1
fi

success "Connected: $(oc "${CTX_ARGS[@]}" whoami) @ $(oc "${CTX_ARGS[@]}" whoami --show-server)"

# ---------------------------------------------------------------------------
#  2. Create the Route (edge TLS — HTTPS from outside, HTTP inside cluster).
#     Idempotent: if the route already exists, skip gracefully.
# ---------------------------------------------------------------------------
step "Step 2 — Ensure the MinIO Route exists"

if oc "${CTX_ARGS[@]}" -n "${NAMESPACE}" get route "${ROUTE_NAME}" >/dev/null 2>&1; then
  info "Route '${ROUTE_NAME}' already exists in namespace '${NAMESPACE}' — skipping creation."
else
  info "Creating Route '${ROUTE_NAME}' for svc/${SVC_NAME} in namespace '${NAMESPACE}'..."
  oc "${CTX_ARGS[@]}" -n "${NAMESPACE}" expose "svc/${SVC_NAME}" \
    --name="${ROUTE_NAME}" \
    --port=9000 \
    --overrides='{
      "spec": {
        "tls": {
          "termination": "edge",
          "insecureEdgeTerminationPolicy": "Redirect"
        }
      }
    }'
  success "Route created."
fi

# ---------------------------------------------------------------------------
#  3. Read the assigned hostname (the cluster decides it from its own *.apps
#     wildcard domain — we never hardcode it).
# ---------------------------------------------------------------------------
step "Step 3 — Read the Route hostname"

MINIO_HOST=$(oc "${CTX_ARGS[@]}" -n "${NAMESPACE}" \
  get route "${ROUTE_NAME}" \
  -o jsonpath='{.spec.host}')

if [[ -z "$MINIO_HOST" ]]; then
  error "Route '${ROUTE_NAME}' has no assigned host yet — re-run in a moment."
  exit 1
fi

MINIO_URL="https://${MINIO_HOST}"
info "Route hostname: ${MINIO_HOST}"

# ---------------------------------------------------------------------------
#  4. Add to /etc/hosts if not already present (needs sudo on macOS/Linux).
#
#     The bastion IP is the public IP that forwards *.apps.* port 443 to the
#     cluster ingress VIP. Resolution order:
#       WXD_BASTION_IP (.env / env)  >  first IP on any existing *.apps line  >
#       hard stop with manual instructions (no silent magic IP).
# ---------------------------------------------------------------------------
step "Step 4 — Register the Route hostname in /etc/hosts"

BASTION_IP="${WXD_BASTION_IP:-$(grep -m1 "\.apps\." /etc/hosts 2>/dev/null | awk '{print $1}')}"

if [[ -z "$BASTION_IP" ]]; then
  warn "Could not determine the bastion IP."
  warn "Set WXD_BASTION_IP in .env (example cluster used: ${EXAMPLE_BASTION_IP}),"
  warn "or add the entry manually:"
  warn "  echo '${EXAMPLE_BASTION_IP} ${MINIO_HOST}' | sudo tee -a /etc/hosts"
elif grep -qF "${MINIO_HOST}" /etc/hosts 2>/dev/null; then
  info "/etc/hosts already contains ${MINIO_HOST} — skipping."
else
  info "Adding to /etc/hosts: ${BASTION_IP} ${MINIO_HOST} (requires sudo)..."
  echo "${BASTION_IP} ${MINIO_HOST}" | sudo tee -a /etc/hosts >/dev/null
  success "/etc/hosts updated."
fi

# ---------------------------------------------------------------------------
#  5. Update WXD_OBJECT_STORE_ENDPOINT in .env.
# ---------------------------------------------------------------------------
step "Step 5 — Record the endpoint in .env"

if grep -qE "^WXD_OBJECT_STORE_ENDPOINT=" "${REPO}/.env" 2>/dev/null; then
  # macOS (BSD) sed needs the empty '' after -i; this repo targets macOS.
  sed -i '' "s|^WXD_OBJECT_STORE_ENDPOINT=.*|WXD_OBJECT_STORE_ENDPOINT=${MINIO_URL}|" "${REPO}/.env"
  success ".env updated: WXD_OBJECT_STORE_ENDPOINT=${MINIO_URL}"
else
  echo "WXD_OBJECT_STORE_ENDPOINT=${MINIO_URL}" >> "${REPO}/.env"
  success ".env appended: WXD_OBJECT_STORE_ENDPOINT=${MINIO_URL}"
fi

# ---------------------------------------------------------------------------
#  6. Verify the Route is reachable.
# ---------------------------------------------------------------------------
step "Step 6 — Test Route connectivity"

HTTP_CODE=$(curl -sk "${MINIO_URL}/minio/health/live" -o /dev/null -w "%{http_code}" 2>/dev/null || echo "000")
if [[ "$HTTP_CODE" == "200" ]]; then
  success "MinIO Route reachable — HTTP ${HTTP_CODE}"
else
  warn "MinIO Route returned HTTP ${HTTP_CODE} — may need a moment or an /etc/hosts entry."
  warn "Retry with:  curl -sk ${MINIO_URL}/minio/health/live -o /dev/null -w '%{http_code}'"
fi

echo ""
echo "============================================================"
echo " MinIO Route ready:"
echo "   ${MINIO_URL}"
echo ""
echo " WXD_OBJECT_STORE_ENDPOINT updated in .env"
echo " Run:  docker compose up -d"
echo "============================================================"
