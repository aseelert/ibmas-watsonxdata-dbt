#!/usr/bin/env bash
# =============================================================================
#  expose_minio_route.sh — create an OpenShift Route for the watsonx.data MinIO
# -----------------------------------------------------------------------------
#  Location  : confluent/scripts/expose_minio_route.sh
#  Repository: ibmas-watsonxdata-dbt
#  Purpose   : One-time setup that exposes ibm-lh-lakehouse-minio-svc as an
#              HTTPS OpenShift Route so Docker containers (Flink, iceberg-rest,
#              confluent-prep) can write to / read from the real iceberg-bucket
#              without an oc port-forward tunnel.
#
#  Run ONCE before: docker compose up -d  (from repo root)
#
#  ENV VARS read (from .env in repo root):
#    WXD_OPENSHIFT_API       — OpenShift API URL  (required)
#    WXD_OC_TOKEN            — oc login token     (required if not already logged in)
#    WXD_OPENSHIFT_NAMESPACE — namespace          (default: cpd-instance)
#    WXD_OPENSHIFT_CONTEXT   — kubeconfig context (optional, pin to avoid wrong cluster)
#
#  After success, copy the printed WXD_OBJECT_STORE_ENDPOINT line into .env.
# =============================================================================
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# ---------------------------------------------------------------------------
# Load .env (non-exported vars are fine here; we just need them in the shell)
# ---------------------------------------------------------------------------
if [[ -f "$REPO/.env" ]]; then
  # shellcheck disable=SC1090
  set -a; source "$REPO/.env"; set +a
else
  echo "ERROR: $REPO/.env not found. Copy .env.example to .env and fill in values." >&2
  exit 1
fi

NAMESPACE="${WXD_OPENSHIFT_NAMESPACE:-cpd-instance}"
ROUTE_NAME="ibm-lh-minio-route"
SVC_NAME="ibm-lh-lakehouse-minio-svc"

# Optional context args — pin to avoid wrong cluster
CTX_ARGS=()
if [[ -n "${WXD_OPENSHIFT_CONTEXT:-}" ]]; then
  CTX_ARGS=("--context" "${WXD_OPENSHIFT_CONTEXT}")
fi

# ---------------------------------------------------------------------------
# 1. Ensure oc is logged in to the correct cluster
#    Priority: WXD_OPENSHIFT_CONTEXT (existing kubeconfig context, fastest)
#              > WXD_OC_TOKEN  > WXD_OC_USER/WXD_OC_PASSWORD
#    Always re-login if the current server doesn't match WXD_OPENSHIFT_API.
# ---------------------------------------------------------------------------
TARGET_API="${WXD_OPENSHIFT_API:-}"
if [[ -z "$TARGET_API" ]]; then
  echo "ERROR: WXD_OPENSHIFT_API is not set in .env." >&2
  exit 1
fi

CURRENT_SERVER=$(oc "${CTX_ARGS[@]}" whoami --show-server 2>/dev/null || true)

# Normalise both URLs (strip trailing slash) for comparison
_norm() { echo "${1%/}"; }

if [[ "$(_norm "$CURRENT_SERVER")" == "$(_norm "$TARGET_API")" ]]; then
  echo "Already connected to ${TARGET_API}"
elif [[ -n "${WXD_OC_TOKEN:-}" ]]; then
  echo "Logging in to ${TARGET_API} with WXD_OC_TOKEN ..."
  oc "${CTX_ARGS[@]}" login "${TARGET_API}" \
    --token="${WXD_OC_TOKEN}" \
    --insecure-skip-tls-verify=true
elif [[ -n "${WXD_OC_USER:-}" && -n "${WXD_OC_PASSWORD:-}" ]]; then
  echo "Logging in to ${TARGET_API} as ${WXD_OC_USER} ..."
  oc "${CTX_ARGS[@]}" login "${TARGET_API}" \
    --username="${WXD_OC_USER}" \
    --password="${WXD_OC_PASSWORD}" \
    --insecure-skip-tls-verify=true
else
  echo "ERROR: Cannot log in — set WXD_OPENSHIFT_CONTEXT, WXD_OC_TOKEN, or WXD_OC_USER+WXD_OC_PASSWORD in .env." >&2
  exit 1
fi

echo "Connected: $(oc "${CTX_ARGS[@]}" whoami) @ $(oc "${CTX_ARGS[@]}" whoami --show-server)"

# ---------------------------------------------------------------------------
# 2. Create the Route (edge TLS — HTTPS from outside, HTTP inside cluster)
#    Idempotent: if route already exists, skip gracefully.
# ---------------------------------------------------------------------------
if oc "${CTX_ARGS[@]}" -n "${NAMESPACE}" get route "${ROUTE_NAME}" >/dev/null 2>&1; then
  echo "Route '${ROUTE_NAME}' already exists in namespace '${NAMESPACE}' — skipping creation."
else
  echo "Creating Route '${ROUTE_NAME}' for svc/${SVC_NAME} in namespace '${NAMESPACE}'..."
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
  echo "Route created."
fi

# ---------------------------------------------------------------------------
# 3. Read the assigned hostname
# ---------------------------------------------------------------------------
MINIO_HOST=$(oc "${CTX_ARGS[@]}" -n "${NAMESPACE}" \
  get route "${ROUTE_NAME}" \
  -o jsonpath='{.spec.host}')

MINIO_URL="https://${MINIO_HOST}"

# ---------------------------------------------------------------------------
# 4. Add to /etc/hosts if not already present (needs sudo on macOS/Linux)
#
#    The bastion IP is the public IP that forwards *.apps.* port 443 to the
#    cluster ingress VIP — derived from any existing apps.* /etc/hosts entry.
#    Override by setting BASTION_IP env var or WXD_BASTION_IP in .env.
# ---------------------------------------------------------------------------
BASTION_IP="${WXD_BASTION_IP:-$(grep -m1 "\.apps\." /etc/hosts 2>/dev/null | awk '{print $1}')}"

if [[ -z "$BASTION_IP" ]]; then
  warn "Could not detect bastion IP from /etc/hosts."
  warn "Run manually:"
  warn "  echo '9.82.206.23 ${MINIO_HOST}' | sudo tee -a /etc/hosts"
elif grep -qF "${MINIO_HOST}" /etc/hosts 2>/dev/null; then
  echo "/etc/hosts already contains ${MINIO_HOST} — skipping."
else
  echo "Adding to /etc/hosts: ${BASTION_IP} ${MINIO_HOST} (requires sudo)..."
  echo "${BASTION_IP} ${MINIO_HOST}" | sudo tee -a /etc/hosts
  echo "/etc/hosts updated."
fi

# ---------------------------------------------------------------------------
# 5. Update WXD_OBJECT_STORE_ENDPOINT in .env
# ---------------------------------------------------------------------------
if grep -qE "^WXD_OBJECT_STORE_ENDPOINT=" "$REPO/.env" 2>/dev/null; then
  sed -i '' "s|^WXD_OBJECT_STORE_ENDPOINT=.*|WXD_OBJECT_STORE_ENDPOINT=${MINIO_URL}|" "$REPO/.env"
  echo ".env updated: WXD_OBJECT_STORE_ENDPOINT=${MINIO_URL}"
else
  echo "WXD_OBJECT_STORE_ENDPOINT=${MINIO_URL}" >> "$REPO/.env"
  echo ".env appended: WXD_OBJECT_STORE_ENDPOINT=${MINIO_URL}"
fi

# ---------------------------------------------------------------------------
# 6. Verify the Route is reachable
# ---------------------------------------------------------------------------
echo ""
echo "Testing Route connectivity..."
HTTP_CODE=$(curl -sk "${MINIO_URL}/minio/health/live" -o /dev/null -w "%{http_code}" 2>/dev/null || echo "000")
if [[ "$HTTP_CODE" == "200" ]]; then
  echo "  MinIO Route reachable — HTTP ${HTTP_CODE} ✓"
else
  echo "  WARNING: MinIO Route returned HTTP ${HTTP_CODE} — may need a moment or /etc/hosts entry."
  echo "  Retry with:  curl -sk ${MINIO_URL}/minio/health/live -o /dev/null -w '%{http_code}'"
fi

echo ""
echo "============================================================"
echo " MinIO Route ready:"
echo "   ${MINIO_URL}"
echo ""
echo " WXD_OBJECT_STORE_ENDPOINT updated in .env ✓"
echo " Run:  docker compose up -d"
echo "============================================================"
