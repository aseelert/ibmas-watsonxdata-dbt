#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/../.env"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "ERROR: .env not found at $ENV_FILE" >&2
  exit 1
fi
source "$ENV_FILE"

echo "=== LOGIN ==="
oc login "${WXD_OPENSHIFT_API}" \
  -u "${WXD_OC_USER}" \
  -p "${WXD_OC_PASSWORD}" \
  --insecure-skip-tls-verify \
  > /dev/null
echo "  logged in as $(oc whoami) → $(oc whoami --show-server)"

NAMESPACE="${WXD_OPENSHIFT_NAMESPACE:-cpd-instance}"

declare -A TARGETS=(
  [ikc-activity-lineage-postgres]=250m
  [ikc-dp-dps-bidata-mde-mdi-postgres]=250m
  [ikc-glossary-workflow-postgres]=250m
  [wdp-profiling-cloud-native-postgresql]=250m
  [ibm-lh-postgres-edb]=300m
)

get_cpu() {
  oc get clusters.postgresql.k8s.enterprisedb.io "$1" -n "$NAMESPACE" \
    -o jsonpath='{.spec.resources.requests.cpu}' 2>/dev/null || echo "n/a"
}

echo "=== BEFORE ==="
printf "%-45s  %s\n" "Cluster" "CPU request"
printf "%-45s  %s\n" "-------" "-----------"
for cluster in "${!TARGETS[@]}"; do
  printf "%-45s  %s\n" "$cluster" "$(get_cpu "$cluster")"
done | sort

echo ""
echo "=== PATCHING ==="
for cluster in "${!TARGETS[@]}"; do
  target="${TARGETS[$cluster]}"
  echo -n "  $cluster → $target ... "
  oc patch clusters.postgresql.k8s.enterprisedb.io "$cluster" \
    -n "$NAMESPACE" --type=merge \
    -p "{\"spec\":{\"resources\":{\"requests\":{\"cpu\":\"$target\"}}}}" \
    > /dev/null
  echo "patched"
done

echo ""
echo "Waiting for rolling restarts ..."
for cluster in "${!TARGETS[@]}"; do
  instances=$(oc get clusters.postgresql.k8s.enterprisedb.io "$cluster" \
    -n "$NAMESPACE" -o jsonpath='{.spec.instances}' 2>/dev/null || echo 2)
  until [ "$(oc get pods -n "$NAMESPACE" -l "postgresql=$cluster" \
      --field-selector=status.phase=Running \
      -o jsonpath='{.items[*].status.containerStatuses[*].ready}' 2>/dev/null \
      | tr ' ' '\n' | grep -c true)" -ge "$instances" ] 2>/dev/null; do
    sleep 5
  done
  echo "  $cluster ready"
done

echo ""
echo "=== AFTER ==="
printf "%-45s  %-12s  %s\n" "Cluster" "CPU request" "Pods ready"
printf "%-45s  %-12s  %s\n" "-------" "-----------" "----------"
for cluster in "${!TARGETS[@]}"; do
  cpu=$(get_cpu "$cluster")
  ready=$(oc get pods -n "$NAMESPACE" -l "postgresql=$cluster" 2>/dev/null \
    | grep -c "1/1" || echo 0)
  printf "%-45s  %-12s  %s/$(oc get clusters.postgresql.k8s.enterprisedb.io "$cluster" \
    -n "$NAMESPACE" -o jsonpath='{.spec.instances}' 2>/dev/null) running\n" \
    "$cluster" "$cpu" "$ready"
done | sort

echo ""
echo "=== NODE CPU SUMMARY ==="
mapfile -t WORKERS < <(oc get nodes -l node-role.kubernetes.io/worker \
  -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null | sort)
for node in "${WORKERS[@]}"; do
  allocatable=$(oc get node "$node" \
    -o jsonpath='{.status.allocatable.cpu}' 2>/dev/null)
  # convert allocatable to millicores (may be "24" or "24000m")
  if [[ "$allocatable" == *m ]]; then
    total_m="${allocatable%m}"
  else
    total_m=$(awk "BEGIN{printf \"%.0f\", $allocatable * 1000}")
  fi
  used=$(oc get pods -A --field-selector="spec.nodeName=$node,status.phase=Running" \
    -o jsonpath='{range .items[*]}{.spec.containers[*].resources.requests.cpu}{"\n"}{end}' \
    2>/dev/null | grep -v "^$" | awk '
    { for(i=1;i<=NF;i++) {
        v=$i
        if(v~/m$/) { sub(/m$/,"",v); s+=v }
        else if(v!="0") { s+=v*1000 }
    }} END { printf "%.0f", s }')
  used="${used:-0}"
  pct=$(awk "BEGIN{printf \"%.0f\", $used/$total_m*100}")
  free=$(awk "BEGIN{printf \"%.1f\", ($total_m-$used)/1000}")
  printf "  %-20s  %6sm / %sm  (%3s%% used, %s cores free)\n" \
    "$node" "$used" "$total_m" "$pct" "$free"
done
