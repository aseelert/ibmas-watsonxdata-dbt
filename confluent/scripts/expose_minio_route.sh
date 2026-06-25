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
# 1. Ensure oc is logged in
# ---------------------------------------------------------------------------
if ! oc "${CTX_ARGS[@]}" whoami --show-server >/dev/null 2>&1; then
  if [[ -z "${WXD_OC_TOKEN:-}" ]]; then
    echo "ERROR: Not logged in with oc and WXD_OC_TOKEN is not set." >&2
    echo "  Either run:  oc login ${WXD_OPENSHIFT_API:-<api-url>}" >&2
    echo "  Or set WXD_OC_TOKEN in .env" >&2
    exit 1
  fi
  echo "Logging in to OpenShift as token..."
  oc "${CTX_ARGS[@]}" login "${WXD_OPENSHIFT_API}" --token="${WXD_OC_TOKEN}"
fi

echo "Connected: $(oc "${CTX_ARGS[@]}" whoami --show-server)"

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
# 3. Read the assigned hostname and print instructions
# ---------------------------------------------------------------------------
MINIO_HOST=$(oc "${CTX_ARGS[@]}" -n "${NAMESPACE}" \
  get route "${ROUTE_NAME}" \
  -o jsonpath='{.spec.host}')

echo ""
echo "============================================================"
echo " MinIO Route ready:"
echo "   https://${MINIO_HOST}"
echo ""
echo " Add or update this line in your .env:"
echo "   WXD_OBJECT_STORE_ENDPOINT=https://${MINIO_HOST}"
echo ""
echo " Then run:  docker compose up -d"
echo "============================================================"
