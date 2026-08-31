#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# install-marquez-ocp.sh
#
# Deploys Marquez (OpenLineage metadata server) into cpd-instance on this
# OpenShift cluster using:
#   - EDB PostgreSQL 16 (dedicated cluster, ibm-entitlement-key pull secret)
#   - Marquez Helm chart pulled directly from GitHub (main branch = latest)
#   - OpenShift Routes (edge TLS) instead of Ingress
#
# Prerequisites (workstation):
#   - oc  (logged in as kube:admin or cluster-admin)
#   - helm >= 3.10
#   - git
#
# Usage:
#   ./install-marquez-ocp.sh [--dry-run] [--uninstall]
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

NAMESPACE="cpd-instance"
RELEASE="marquez"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OCP_DIR="${SCRIPT_DIR}"

DRY_RUN=false
UNINSTALL=false

for arg in "$@"; do
  case $arg in
    --dry-run)   DRY_RUN=true ;;
    --uninstall) UNINSTALL=true ;;
  esac
done

# ── helpers ───────────────────────────────────────────────────────────────────
info()  { echo "  [INFO]  $*"; }
ok()    { echo "  [OK]    $*"; }
warn()  { echo "  [WARN]  $*"; }
run()   {
  if $DRY_RUN; then
    echo "  [DRY]   $*"
  else
    # Use an array so the command is never re-split by eval
    bash -c "$*"
  fi
}

# ── preflight ─────────────────────────────────────────────────────────────────
info "Checking prerequisites..."
command -v oc   >/dev/null || { echo "oc not found"; exit 1; }
command -v helm >/dev/null || { echo "helm not found — brew install helm"; exit 1; }
command -v git  >/dev/null || { echo "git not found"; exit 1; }

OC_USER=$(oc whoami 2>/dev/null) || { echo "Not logged in to OpenShift"; exit 1; }
ok "Logged in as: $OC_USER"

oc get ns "$NAMESPACE" >/dev/null 2>&1 || { echo "Namespace $NAMESPACE not found"; exit 1; }
ok "Namespace $NAMESPACE exists"

# ── uninstall path ────────────────────────────────────────────────────────────
if $UNINSTALL; then
  warn "Uninstalling Marquez from $NAMESPACE..."
  helm uninstall "$RELEASE" -n "$NAMESPACE" 2>/dev/null || true
  oc delete -f "${OCP_DIR}/03-routes.yaml" --ignore-not-found
  warn "Leaving EDB cluster 'marquez-postgres' intact — delete manually if wanted:"
  warn "  oc delete cluster.postgresql.k8s.enterprisedb.io marquez-postgres -n $NAMESPACE"
  warn "  oc delete networkpolicy marquez-postgres-network-policy -n $NAMESPACE"
  ok "Done."
  exit 0
fi

# ── Step 1: EDB PostgreSQL cluster ────────────────────────────────────────────
info "Step 1/4 — Deploying EDB PostgreSQL cluster for Marquez..."

if oc get cluster.postgresql.k8s.enterprisedb.io marquez-postgres -n "$NAMESPACE" >/dev/null 2>&1; then
  ok "EDB cluster 'marquez-postgres' already exists — skipping"
else
  run "oc apply -f '${OCP_DIR}/01-marquez-postgres-edb.yaml'"
  if ! $DRY_RUN; then
    info "Waiting for EDB cluster to become healthy (up to 3 min)..."
    ATTEMPTS=0
    until oc get cluster.postgresql.k8s.enterprisedb.io marquez-postgres -n "$NAMESPACE" \
          -o jsonpath='{.status.phase}' 2>/dev/null | grep -q "Cluster in healthy state"; do
      ATTEMPTS=$((ATTEMPTS+1))
      if [ $ATTEMPTS -gt 36 ]; then
        warn "Timed out waiting for EDB — continuing (the Helm step will wait on the secret)"
        break
      fi
      echo -n "."
      sleep 5
    done
    echo ""
    ok "EDB cluster ready"
  fi
fi

# Verify the app secret was created by the EDB operator
if ! $DRY_RUN; then
  if oc get secret marquez-postgres-app -n "$NAMESPACE" >/dev/null 2>&1; then
    ok "Secret 'marquez-postgres-app' exists"
  else
    warn "Secret 'marquez-postgres-app' not yet created — Helm --wait will time out if EDB is still initialising"
  fi
fi

# ── Step 1b: Create key-remapping secret for Marquez Helm chart ──────────────
# The EDB operator stores the DB password under key "password" in secret
# "marquez-postgres-app".  The Marquez Helm chart (when postgresql.enabled=false)
# looks for key "marquez-db-password" in the secret referenced by
# marquez.existingSecretName.  We create a thin wrapper secret that bridges them.
info "Step 1b/4 — Creating Helm-compatible DB credentials secret..."

if ! $DRY_RUN; then
  # Wait up to 2 min for the EDB secret to appear (it's created after the cluster is healthy)
  ATTEMPTS=0
  until oc get secret marquez-postgres-app -n "$NAMESPACE" >/dev/null 2>&1; do
    ATTEMPTS=$((ATTEMPTS+1))
    if [ $ATTEMPTS -gt 24 ]; then
      echo ""
      warn "Timed out waiting for marquez-postgres-app secret — is the EDB cluster healthy?"
      warn "  oc get cluster.postgresql.k8s.enterprisedb.io marquez-postgres -n $NAMESPACE"
      exit 1
    fi
    echo -n "."
    sleep 5
  done
  echo ""

  # Extract the password from the EDB-managed secret and create the wrapper
  DB_PASS=$(oc get secret marquez-postgres-app -n "$NAMESPACE" \
    -o jsonpath='{.data.password}' | base64 -d)
  DB_USER=$(oc get secret marquez-postgres-app -n "$NAMESPACE" \
    -o jsonpath='{.data.username}' | base64 -d)

  # Create (or replace) the wrapper secret with the key name the Helm chart expects
  oc create secret generic marquez-db-secret \
    --namespace "$NAMESPACE" \
    --from-literal="marquez-db-password=${DB_PASS}" \
    --from-literal="marquez-db-user=${DB_USER}" \
    --dry-run=client -o yaml | oc apply -f -
  ok "Secret 'marquez-db-secret' created/updated"
else
  echo "  [DRY]   oc create secret generic marquez-db-secret --from-literal=marquez-db-password=<edb-password>"
fi

# ── Step 2: Helm chart from GitHub main ───────────────────────────────────────
info "Step 2/4 — Fetching Marquez Helm chart from GitHub (main branch)..."

TMPDIR="$(mktemp -d)"
trap 'rm -rf "${TMPDIR}"' EXIT

# Sparse-clone only the chart/ directory to avoid pulling the full repo
git clone --depth 1 --filter=blob:none --sparse \
  https://github.com/MarquezProject/marquez.git "${TMPDIR}/marquez" >/dev/null 2>&1

# sparse-checkout runs from inside the clone — use absolute path via -C flag
git -C "${TMPDIR}/marquez" sparse-checkout set chart >/dev/null 2>&1

CHART_DIR="${TMPDIR}/marquez/chart"

CHART_VER=$(grep '^version:' "${CHART_DIR}/Chart.yaml" | awk '{print $2}')
ok "Chart fetched — version: ${CHART_VER}"

# Update Helm dependencies (bitnami/common is the only sub-chart; bitnami/postgresql is disabled)
info "Updating Helm chart dependencies..."
if ! helm dependency update "${CHART_DIR}" >/dev/null 2>&1; then
  warn "helm dependency update emitted errors — chart may still work (common is a library chart)"
fi

# ── Step 3: Helm install/upgrade ─────────────────────────────────────────────
info "Step 3/4 — Installing/upgrading Marquez via Helm..."

HELM_FLAGS=""
if $DRY_RUN; then
  HELM_FLAGS="--dry-run"
fi

# shellcheck disable=SC2086
helm upgrade --install "${RELEASE}" "${CHART_DIR}" \
  --namespace "${NAMESPACE}" \
  --values "${OCP_DIR}/02-marquez-values-ocp.yaml" \
  --set marquez.db.host=marquez-postgres-rw \
  --wait \
  --timeout 5m \
  ${HELM_FLAGS}

ok "Helm release deployed"

# ── Step 4: OpenShift Routes ──────────────────────────────────────────────────
info "Step 4/4 — Creating OpenShift Routes..."
run "oc apply -f '${OCP_DIR}/03-routes.yaml'"

# ── Summary ───────────────────────────────────────────────────────────────────
if ! $DRY_RUN; then
  APP_DOMAIN=$(oc get ingresses.config.openshift.io cluster \
    -o jsonpath='{.spec.domain}' 2>/dev/null || echo "apps.watson.ibmas-zocp-techcluster.org")
  echo ""
  echo "══════════════════════════════════════════════════════════════"
  echo "  Marquez deployed successfully in namespace: ${NAMESPACE}"
  echo ""
  echo "  Web UI:  https://marquez-web.${APP_DOMAIN}"
  echo "  API:     https://marquez-api.${APP_DOMAIN}"
  echo "  API (in-cluster): http://marquez.${NAMESPACE}.svc.cluster.local:5000"
  echo ""
  echo "  OpenLineage endpoint:"
  echo "    https://marquez-api.${APP_DOMAIN}/api/v1/lineage"
  echo "    http://marquez.${NAMESPACE}.svc.cluster.local:5000/api/v1/lineage"
  echo ""
  echo "  Spark conf to emit lineage:"
  echo "    spark.jars.packages = io.openlineage:openlineage-spark_2.12:1.32.0"
  echo "    spark.extraListeners = io.openlineage.spark.agent.OpenLineageSparkListener"
  echo "    spark.openlineage.transport.type = http"
  echo "    spark.openlineage.transport.url = http://marquez.${NAMESPACE}.svc.cluster.local:5000"
  echo "    spark.openlineage.transport.endpoint = /api/v1/lineage"
  echo "    spark.openlineage.namespace = watsonxdata-spark"
  echo ""
  echo "  DB (EDB): marquez-postgres-rw.${NAMESPACE}.svc.cluster.local:5432/marquez"
  echo "  DB creds: oc get secret marquez-postgres-app -n ${NAMESPACE} -o yaml"
  echo "══════════════════════════════════════════════════════════════"
fi
