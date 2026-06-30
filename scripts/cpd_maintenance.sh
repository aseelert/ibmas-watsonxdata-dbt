#!/usr/bin/env bash
# =============================================================================
#  cpd_maintenance.sh — Graceful shutdown / restart of CPD services on OCP
#
#  Manages IBM Software Hub (Cloud Pak for Data) services on OpenShift:
#    - watsonx.data (wxd lakehouse + wxdAddon)
#    - IBM Knowledge Catalog / WKC (wkc)
#    - DataStage (datastage)
#
#  Shutdown order : watsonx_data → datastage → wkc   (dependents first)
#  Startup  order : wkc → datastage → watsonx_data   (base layer first)
#
#  Actions:
#    status            Show CRD + per-service pod state (read-only)
#    verify            Wait until EVERY workload is fully ready (read-only gate);
#                      catches pods stuck at 0/1 or 4/5, not just deploy status
#    restart           Rolling restart (no downtime), then gate on readiness
#    shutdown          Quiesce wxd/DataStage/WKC (frees compute)
#    startup           Bring services back, then gate on readiness
#    prepare-upgrade   Quiesce heavy compute before an OCP cluster upgrade so
#                      node drains evict far fewer slow-probe pods (leaves
#                      Zen/IAM/scheduler up). ALWAYS dry-run this first.
#    resume-upgrade    Reverse prepare-upgrade, then gate on readiness
#    drain-node        / uncordon-node <node>  — node maintenance
#
#  Usage:
#    bash scripts/cpd_maintenance.sh status
#    bash scripts/cpd_maintenance.sh verify
#    bash scripts/cpd_maintenance.sh restart
#    bash scripts/cpd_maintenance.sh prepare-upgrade --dry-run   # preview first!
#    bash scripts/cpd_maintenance.sh prepare-upgrade
#    bash scripts/cpd_maintenance.sh resume-upgrade
#    bash scripts/cpd_maintenance.sh drain-node    <node-name>
#    bash scripts/cpd_maintenance.sh uncordon-node <node-name>
#
#  Flags:
#    --dry-run            Print what would run, execute nothing (route via run())
#    --yes                Skip confirmation prompts (for automation)
#    --namespace <ns>     Operand namespace (default: cpd-instance)
#    --operators-ns <ns>  Operator namespace (default: cpd-operators)
#    --wait-timeout <s>   Readiness wait cap in seconds (default: 1800)
#    --restart-dbs        Also rolling-restart EDB clusters during 'restart'
#    --use-oc             Force oc path even if cpd-cli is present
#                         (auto-selected when cpd-cli is not installed)
#    --no-log             Disable file logging (terminal only)
#    --log-dir <dir>      Override log directory (default: <repo>/logs)
#
#  IMPORTANT — watsonx.data restart hazard (learned the hard way):
#    A bare `oc patch wxd lakehouse shutdown=true` then `false` does NOT reliably
#    restart wxd when the engine has `shutdown_by_addon=true`: the wxdAddon is an
#    Ansible operator and a STALE/failed playbook leaves the metastore/MinIO at
#    0/0. Recovery: full addon cycle (shutdown via wxdAddon, then back) and, if
#    the playbook is wedged, restart the operator pod:
#      oc delete pod -n cpd-operators -l control-plane=ibm-lakehouse-controller-manager
#    Therefore 'restart' here uses `oc rollout restart` on the ibm-lh workloads
#    (preserves addon-managed state) and does NOT toggle spec.shutdown.
#
#  Logs:
#    All output is written to logs/cpd_maintenance_<action>_<timestamp>.log
#    automatically. Use --no-log to disable.
#
#  Prerequisites:
#    - oc CLI logged in to the cluster (or .env has WXD_OPENSHIFT_API creds)
#    - cpd-cli (IBM Software Hub CLI) — optional; auto-detected. NOTE: the verb to
#      bring services back is `cpd-cli manage restart` (there is NO `manage startup`).
#
#  References:
#    https://www.ibm.com/docs/en/software-hub/5.3.x?topic=services-shutting-down
#    https://www.ibm.com/docs/en/software-hub/5.1.x?topic=services-restarting
#    https://www.ibm.com/docs/en/watsonx/watsonxdata/2.0.x?topic=resources-shutting-down-restarting
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/log.sh"
source "${SCRIPT_DIR}/lib/readiness.sh"   # verify_ready / wait_until_ready / do_verify
install_err_trap
load_env

# ---------------------------------------------------------------------------
# Defaults (overridable via flags or .env)
# ---------------------------------------------------------------------------
NAMESPACE="${WXD_OPENSHIFT_NAMESPACE:-cpd-instance}"
OPERATORS_NS="${WXD_OPERATORS_NAMESPACE:-cpd-operators}"
DRY_RUN="${DRY_RUN:-false}"
ASSUME_YES="${ASSUME_YES:-false}"
NO_LOG=false
LOG_DIR="${SCRIPT_DIR}/../logs"
ACTION=""
NODE_NAME=""

# Auto-detect cpd-cli; fall back to oc patch/rollout if not found.
# Override with --use-oc to force the oc path even when cpd-cli is present.
if command -v cpd-cli &>/dev/null; then
  USE_OC=false
else
  USE_OC=true
fi

# Ordered service lists
# Shutdown: dependents (wxd, ds) before the base layer (wkc)
SHUTDOWN_ORDER=(watsonx_data datastage wkc)
# Startup:  base layer first, then the services that sit on top
STARTUP_ORDER=(wkc datastage watsonx_data)

# How long to wait for workloads to reach a stable/ready state. Overridable via
# WAIT_TIMEOUT env or --wait-timeout. Default 1800s (30 min): the observed
# slow-probe symptom kept pods at 0/1 for minutes, and an upgrade window wants
# real headroom. readiness.sh reads READY_TIMEOUT — keep them in sync.
WAIT_TIMEOUT="${WAIT_TIMEOUT:-1800}"

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    shutdown|startup|restart|status|verify|prepare-upgrade|resume-upgrade|drain-node|uncordon-node)
      ACTION="$1"; shift
      if [[ "$ACTION" == "drain-node" || "$ACTION" == "uncordon-node" ]]; then
        NODE_NAME="${1:-}"; shift || true
        [[ -z "$NODE_NAME" ]] && { error "Node name required for $ACTION"; exit 1; }
      fi
      ;;
    --namespace|-n)    NAMESPACE="$2"; shift 2 ;;
    --operators-ns)    OPERATORS_NS="$2"; shift 2 ;;
    --use-oc)          USE_OC=true; shift ;;
    --dry-run)         DRY_RUN=true; shift ;;
    --yes|-y)          ASSUME_YES=true; shift ;;
    --no-log)          NO_LOG=true; shift ;;
    --log-dir)         LOG_DIR="$2"; shift 2 ;;
    --wait-timeout)    WAIT_TIMEOUT="$2"; shift 2 ;;
    --restart-dbs)     RESTART_DBS=true; shift ;;
    -h|--help)
      grep '^#  ' "$0" | sed 's/^#  //'
      exit 0
      ;;
    *) error "Unknown argument: $1"; exit 1 ;;
  esac
done

[[ -z "$ACTION" ]] && { error "Usage: $0 <shutdown|startup|restart|verify|prepare-upgrade|resume-upgrade|status|drain-node|uncordon-node> [flags]"; exit 1; }

# Keep readiness.sh tunables in sync with this script's (post-arg-parse) values.
# readiness.sh builds READY_NAMESPACES at source time from defaults; rebuild it
# here so a --namespace / --operators-ns override is actually honored.
export NAMESPACE OPERATORS_NS
READY_TIMEOUT="$WAIT_TIMEOUT"
RESTART_DBS="${RESTART_DBS:-false}"
READY_NAMESPACES=( "$NAMESPACE" "$OPERATORS_NS" ibm-cpd-scheduler ibm-licensing )

# ---------------------------------------------------------------------------
# Logging setup — tee everything (stdout + stderr) to a timestamped log file.
# The exec redirect runs before oc_login so the login output is also captured.
# confirm() reads from /dev/tty so prompts still reach the terminal.
# ---------------------------------------------------------------------------
TIMESTAMP="$(date '+%Y%m%d_%H%M%S')"
if [[ "$NO_LOG" != "true" ]]; then
  mkdir -p "$LOG_DIR"
  LOG_FILE="$(cd "$LOG_DIR" && pwd)/cpd_maintenance_${ACTION}_${TIMESTAMP}.log"
  # Duplicate all stdout+stderr to the log file without buffering.
  exec > >(tee -a "$LOG_FILE") 2>&1
  echo "=== cpd_maintenance.sh  action=${ACTION}  started=${TIMESTAMP} ===" >> "$LOG_FILE"
  # Print the log path BEFORE the redirect swallows it into the file too —
  # we print to the original fd 2 (now also going to tee), so it shows on screen.
  echo "  [LOG]   Writing to: ${LOG_FILE}"
fi

# ---------------------------------------------------------------------------
# OC login (skip if already logged in or no creds in .env)
# ---------------------------------------------------------------------------
oc_login() {
  if [[ -n "${WXD_OPENSHIFT_API:-}" && -n "${WXD_OC_USER:-}" && -n "${WXD_OC_PASSWORD:-}" ]]; then
    step "Logging in to OpenShift"
    run oc login "${WXD_OPENSHIFT_API}" \
      -u "${WXD_OC_USER}" \
      -p "${WXD_OC_PASSWORD}" \
      --insecure-skip-tls-verify \
      > /dev/null
    success "Logged in as $(oc whoami) → $(oc whoami --show-server)"
  else
    info "No OC login vars in .env — assuming oc is already authenticated"
    oc whoami > /dev/null 2>&1 || { error "Not logged in to OpenShift. Run 'oc login ...' first."; exit 1; }
    info "Active session: $(oc whoami) → $(oc whoami --show-server)"
  fi
}

# ---------------------------------------------------------------------------
# Status helpers
# ---------------------------------------------------------------------------
wxd_shutdown_state() {
  oc get wxd lakehouse -n "$NAMESPACE" \
    -o jsonpath='{.spec.shutdown}' 2>/dev/null || echo "unknown"
}

wxdaddon_shutdown_state() {
  oc get wxdAddon wxdaddon -n "$NAMESPACE" \
    -o jsonpath='{.spec.shutdown}' 2>/dev/null || echo "unknown"
}

# Count running pods whose name starts with any of the given prefixes.
# Used by wait_for_pods; print_status defines its own _running() locally
# over a cached pod list to avoid repeated oc calls.
pods_running_by_prefix() {
  local pods
  pods=$(oc get pods -n "$NAMESPACE" --no-headers 2>/dev/null || true)
  local count=0
  for prefix in "$@"; do
    local n
    n=$(echo "$pods" | awk -v p="$prefix" 'index($1,p)==1 && $3=="Running"{c++} END{print c+0}')
    count=$((count + n))
  done
  echo "$count"
}

print_status() {
  step "Current service status in namespace: ${NAMESPACE}"

  # Cache the full pod list once — reused by every section below.
  local ALL_PODS
  ALL_PODS=$(oc get pods -n "$NAMESPACE" --no-headers 2>/dev/null || true)

  # Count running pods whose names start with any of the given prefixes.
  _running() {
    local count=0
    for prefix in "$@"; do
      local n
      n=$(echo "$ALL_PODS" | awk -v p="$prefix" 'index($1,p)==1 && $3=="Running"{c++} END{print c+0}')
      count=$((count + n))
    done
    echo "$count"
  }

  # --- Platform / Zen ---
  echo ""
  info "Platform layer (ZenService / Ibmcpd):"
  oc get ZenService lite-cr Ibmcpd ibmcpd-cr -n "$NAMESPACE" 2>/dev/null || true

  # --- Service-level CRDs (wkc, DataStage, wxd base) ---
  echo ""
  info "Service CRDs:"
  for kn in "wkc/wkc-cr" "DataStage/datastage" "wxd/lakehouse" "wxdAddon/wxdaddon"; do
    local kind="${kn%%/*}" name="${kn##*/}"
    oc get "$kind" "$name" -n "$NAMESPACE" 2>/dev/null || \
      printf "  %-20s  <not found>\n" "${kind}/${name}"
  done

  # --- wxd lakehouse shutdown flags ---
  echo ""
  info "watsonx.data shutdown spec:"
  printf "  %-28s  spec.shutdown=%s\n" "wxd/lakehouse"     "$(wxd_shutdown_state)"
  printf "  %-28s  spec.shutdown=%s\n" "wxdAddon/wxdaddon" "$(wxdaddon_shutdown_state)"

  # --- Presto engine instances (dynamic — all wxdEngine CRs) ---
  echo ""
  info "Presto engine instances (wxdEngine CRs):"
  local presto_engines
  presto_engines=$(oc get wxdengine -n "$NAMESPACE" --no-headers \
    -o custom-columns='NAME:.metadata.name,ENGINE:.spec.engineName,DISPLAY:.spec.engineDisplayName,STATUS:.status.engineStatus,RECONCILE:.status.wxdEngineReconcileStatus,AGE:.metadata.creationTimestamp' \
    2>/dev/null || true)
  if [[ -z "$presto_engines" ]]; then
    echo "  <no wxdEngine instances found>"
  else
    printf "  %-30s  %-12s  %-20s  %-10s  %s\n" "CR Name" "Engine ID" "Display Name" "Status" "Reconcile"
    printf "  %-30s  %-12s  %-20s  %-10s  %s\n" "-------" "---------" "------------" "------" "---------"
    while IFS= read -r line; do
      local cr eng disp stat rec
      cr=$(echo "$line"   | awk '{print $1}')
      eng=$(echo "$line"  | awk '{print $2}')
      disp=$(echo "$line" | awk '{print $3}')
      stat=$(echo "$line" | awk '{print $4}')
      rec=$(echo "$line"  | awk '{print $5}')
      # Count running pods for this specific engine (StatefulSets named ibm-lh-lakehouse-<engine>-*)
      local ep; ep=$(_running "ibm-lh-lakehouse-${eng}-")
      printf "  %-30s  %-12s  %-20s  %-10s  %-10s  pods=%s\n" "$cr" "$eng" "$disp" "$stat" "$rec" "$ep"
    done <<< "$presto_engines"
  fi

  # --- Spark engine instances (no CRD — discovered from pod names) ---
  echo ""
  info "Spark engine instances (spark-master-deployment-* pods):"
  local spark_masters
  spark_masters=$(echo "$ALL_PODS" | awk '$1~/^spark-master-deployment-/{print $1, $3}' || true)
  if [[ -z "$spark_masters" ]]; then
    echo "  <no Spark engine pods found>"
  else
    printf "  %-60s  %-10s  %s\n" "Pod name" "Status" "Workers Running"
    printf "  %-60s  %-10s  %s\n" "--------" "------" "---------------"
    while IFS=' ' read -r pod status; do
      # Engine UUID is between 'spark-master-deployment-' and the trailing pod hash
      local uuid
      uuid=$(echo "$pod" | sed 's/^spark-master-deployment-//' | sed 's/[a-z0-9]*$//' | sed 's/-$//')
      local workers; workers=$(_running "spark-worker-deployment-${uuid}")
      printf "  %-60s  %-10s  %s\n" "$pod" "$status" "$workers"
    done <<< "$spark_masters"
  fi

  # --- Spark History Builder (platform service, not per-engine) ---
  echo ""
  info "Spark History Builder (platform service):"
  printf "  %-20s  %-8s  %s\n" "Service" "Running" "Prefix"
  printf "  %-20s  %-8s  %s\n" "-------" "-------" "------"
  printf "  %-20s  %-8s  %s\n" "spark-hb" "$(_running "spark-hb-")" "spark-hb-*"

  # --- Aggregate pod counts per service ---
  echo ""
  info "Running pod counts (aggregate):"
  printf "  %-22s  %-8s  %s\n" "Service" "Running" "Pod name prefixes"
  printf "  %-22s  %-8s  %s\n" "-------" "-------" "-----------------"
  printf "  %-22s  %-8s  %s\n" "watsonx.data (ibm-lh)" "$(_running "ibm-lh-")"                          "ibm-lh-*"
  printf "  %-22s  %-8s  %s\n" "WKC / IKC"             "$(_running "wkc-" "knowledge-accelerators")"    "wkc-*, knowledge-accelerators*"
  printf "  %-22s  %-8s  %s\n" "DataStage"              "$(_running "datastage-" "ds-px-")"              "datastage-*, ds-px-*"
  printf "  %-22s  %-8s  %s\n" "Spark engines"          "$(_running "spark-master-" "spark-worker-")"   "spark-master-*, spark-worker-*"

  # --- Nodes ---
  echo ""
  info "Nodes:"
  oc get nodes -o wide 2>/dev/null || true
}

# ---------------------------------------------------------------------------
# cpd-cli manage approach (preferred — IBM Software Hub 5.x)
# ---------------------------------------------------------------------------
cpdcli_shutdown() {
  local component="$1"
  step "Shutting down: ${component} (cpd-cli)"
  run cpd-cli manage shutdown \
    --components="${component}" \
    --cpd_instance_ns="${NAMESPACE}"
  success "${component} shutdown initiated"
}

cpdcli_startup() {
  local component="$1"
  step "Starting up: ${component} (cpd-cli)"
  # IMPORTANT: there is NO 'cpd-cli manage startup'. The verb that brings a
  # shut-down component back online is 'restart' (it sets the CR back to
  # running and restarts the component AFTER its dependencies).
  run cpd-cli manage restart \
    --components="${component}" \
    --cpd_instance_ns="${NAMESPACE}"
  success "${component} restart (startup) initiated"
}

# ---------------------------------------------------------------------------
# oc-based approach (auto-selected when cpd-cli is absent, or with --use-oc)
# ---------------------------------------------------------------------------

# watsonx.data exposes a shutdown CRD field. The wxdAddon is the controlling
# authority (it sets shutdown_by_addon on the engines), so we drive the ADDON
# first on shutdown and last on startup. Engines (Presto, possibly several) are
# patched too. Spark engines have no CRD — handled by oc_spark_scale.
oc_wxd_shutdown() {
  step "watsonx.data shutdown (addon → lakehouse → engines)"
  run oc patch wxdAddon wxdaddon -n "$NAMESPACE" --type=merge \
    --patch '{"spec":{"shutdown":"true"}}'
  run oc patch wxd lakehouse -n "$NAMESPACE" --type=merge \
    --patch '{"spec":{"shutdown":"true"}}'
  # Patch every Presto engine instance (there can be more than one).
  local engines
  engines=$(oc get wxdengine -n "$NAMESPACE" --no-headers 2>/dev/null | awk '{print $1}' || true)
  if [[ -n "$engines" ]]; then
    while IFS= read -r e; do
      [[ -z "$e" ]] && continue
      info "  patch wxdEngine/${e} shutdown=true"
      run oc patch wxdEngine "$e" -n "$NAMESPACE" --type=merge \
        --patch '{"spec":{"shutdown":"true"}}'
    done <<< "$engines"
  fi
  success "watsonx.data shutdown patch applied"
  info "Note: Postgres pods remain Running (expected — only the server process stops)"
}

oc_wxd_startup() {
  step "watsonx.data startup (engines → lakehouse → addon)"
  local engines
  engines=$(oc get wxdengine -n "$NAMESPACE" --no-headers 2>/dev/null | awk '{print $1}' || true)
  if [[ -n "$engines" ]]; then
    while IFS= read -r e; do
      [[ -z "$e" ]] && continue
      info "  patch wxdEngine/${e} shutdown=false"
      run oc patch wxdEngine "$e" -n "$NAMESPACE" --type=merge \
        --patch '{"spec":{"shutdown":"false"}}'
    done <<< "$engines"
  fi
  run oc patch wxd lakehouse -n "$NAMESPACE" --type=merge \
    --patch '{"spec":{"shutdown":"false"}}'
  run oc patch wxdAddon wxdaddon -n "$NAMESPACE" --type=merge \
    --patch '{"spec":{"shutdown":"false"}}'
  success "watsonx.data startup patch applied"
  # HARD-LEARNED CAVEAT: when the engine has shutdown_by_addon=true, the addon is
  # an Ansible operator and a stale/failed playbook can leave the metastore/MinIO
  # at 0/0 even after shutdown=false. If wxd does not come back, restart the
  # operator so the playbook re-runs clean:
  #   oc delete pod -n <operators-ns> -l control-plane=ibm-lakehouse-controller-manager
  if oc get wxdengine -n "$NAMESPACE" -o jsonpath='{.items[*].spec.shutdown_by_addon}' 2>/dev/null \
       | grep -q true; then
    warn "An engine has shutdown_by_addon=true: the wxdAddon governs startup."
    warn "If wxd stays at 0/0, restart the operator:"
    warn "  oc delete pod -n ${OPERATORS_NS} -l control-plane=ibm-lakehouse-controller-manager"
  fi
}

# Spark engines (no CRD) — scale spark-master/worker deployments, recording the
# original replica counts to the state file so startup can restore them.
SPARK_STATE_FILE() { echo "${LOG_DIR}/cpd_maintenance_spark_${NAMESPACE}.state"; }

oc_spark_scale() {
  local mode="$1"   # "down" | "restore"
  local sf; sf="$(SPARK_STATE_FILE)"
  local deps
  deps=$(oc get deploy -n "$NAMESPACE" --no-headers 2>/dev/null \
    | awk '$1 ~ /^spark-(master|worker)-deployment-/ {print $1}' || true)
  if [[ "$mode" == "down" ]]; then
    [[ -z "$deps" ]] && { info "No Spark engine deployments to scale down."; return 0; }
    step "Scaling Spark engine deployments → 0 (saving counts)"
    : > "${sf}.tmp" 2>/dev/null || true
    while IFS= read -r d; do
      [[ -z "$d" ]] && continue
      local r; r=$(oc get deploy "$d" -n "$NAMESPACE" -o jsonpath='{.spec.replicas}' 2>/dev/null); r=${r:-0}
      [[ "${DRY_RUN:-false}" != "true" ]] && echo "$d $r" >> "${sf}.tmp"
      run oc scale deploy/"$d" -n "$NAMESPACE" --replicas=0
    done <<< "$deps"
    [[ "${DRY_RUN:-false}" != "true" && -f "${sf}.tmp" ]] && mv "${sf}.tmp" "$sf"
    success "Spark engines scaled to 0 (state → ${sf})"
  else
    if [[ ! -f "$sf" ]]; then warn "No Spark state file — operator/addon will reconcile Spark."; return 0; fi
    step "Restoring Spark engine deployment replicas"
    while IFS= read -r line; do
      [[ -z "$line" ]] && continue
      local d="${line%% *}" r="${line##* }"; [[ -z "$r" || "$r" == "$d" ]] && r=1
      run oc scale deploy/"$d" -n "$NAMESPACE" --replicas="$r"
    done < "$sf"
    success "Spark engine replicas restored"
  fi
}

# WKC and DataStage: rolling restart via oc rollout restart.
# This preserves existing replica counts and keeps services available during restart.
oc_rollout_restart() {
  local component="$1"
  local dep_prefix sts_prefix
  case "$component" in
    wxd)
      # ibm-lh-* workloads, but NOT the EDB postgres (owned by the Cluster CR;
      # restarted separately via --restart-dbs). Rolling-restart leaves the
      # addon-managed spec.shutdown untouched (avoids the 0/0 strand trap).
      dep_prefix="ibm-lh-lakehouse-"
      sts_prefix="ibm-lh-lakehouse-"
      ;;
    wkc)
      dep_prefix="wkc-\|knowledge-accelerators"
      sts_prefix="wkc-"
      ;;
    datastage)
      dep_prefix="datastage-\|ds-px-"
      sts_prefix="ds-px-\|datastage-"
      ;;
    *)  error "Unknown component: $component"; return 1 ;;
  esac

  step "Rolling restart: ${component} deployments"
  local deps
  deps=$(oc get deployment -n "$NAMESPACE" --no-headers 2>/dev/null \
    | awk -v p="$dep_prefix" '$1 ~ p {print $1}' || true)
  if [[ -n "$deps" ]]; then
    local count=0
    while IFS= read -r dep; do
      info "  oc rollout restart deployment/${dep}"
      run oc rollout restart deployment/"${dep}" -n "$NAMESPACE"
      count=$((count + 1))
    done <<< "$deps"
    success "${component}: restarted ${count} deployment(s)"
  else
    warn "No deployments found for ${component}"
  fi

  local stsets
  stsets=$(oc get sts -n "$NAMESPACE" --no-headers 2>/dev/null \
    | awk -v p="$sts_prefix" '$1 ~ p {print $1}' || true)
  if [[ -n "$stsets" ]]; then
    while IFS= read -r sts; do
      info "  oc rollout restart sts/${sts}"
      run oc rollout restart sts/"${sts}" -n "$NAMESPACE"
    done <<< "$stsets"
  fi
}

# ---------------------------------------------------------------------------
# Operator pause / resume (for shutdown / prepare-upgrade). Operators reconcile
# operands, so to keep a service's operands scaled down we pause ITS controller
# first. We pause ONLY the service operators (WKC, DataStage) — NOT zen/iam/
# common-services/ODLM, so the console + foundational reconciliation stay up.
# Original replica counts are saved so resume restores them exactly.
#
# SERVICE_OPERATORS maps the services we manage to their controllers in
# OPERATORS_NS. Override via the SERVICE_OPERATORS env var if your install
# differs. (ccs is shared infra and deliberately left running.)
# ---------------------------------------------------------------------------
SERVICE_OPERATORS="${SERVICE_OPERATORS:-ibm-cpd-wkc-operator ibm-cpd-datastage-operator}"
# Operand name prefixes for the services we scale to 0 (wxd is handled by its
# CRD shutdown; Spark by oc_spark_scale — neither is listed here).
SERVICE_OPERAND_PREFIXES="${SERVICE_OPERAND_PREFIXES:-wkc- knowledge-accelerators datastage- ds-px- ibmas-datastage-}"

OPERATOR_STATE_FILE() { echo "${LOG_DIR}/cpd_maintenance_operators_${OPERATORS_NS}.state"; }
OPERAND_STATE_FILE()  { echo "${LOG_DIR}/cpd_maintenance_operands_${NAMESPACE}.state"; }

oc_pause_operators() {
  local sf; sf="$(OPERATOR_STATE_FILE)"
  step "Pausing service operators in ${OPERATORS_NS}: ${SERVICE_OPERATORS}"
  : > "${sf}.tmp" 2>/dev/null || true
  local count=0 d
  for d in $SERVICE_OPERATORS; do
    oc get deploy "$d" -n "$OPERATORS_NS" >/dev/null 2>&1 || { warn "  operator not found: ${d}"; continue; }
    local r; r=$(oc get deploy "$d" -n "$OPERATORS_NS" -o jsonpath='{.spec.replicas}' 2>/dev/null); r=${r:-0}
    [[ "$r" -eq 0 ]] && { info "  already paused: ${d}"; continue; }
    [[ "${DRY_RUN:-false}" != "true" ]] && echo "$d $r" >> "${sf}.tmp"
    info "  pause deploy/${d} (was ${r})"
    run oc scale deploy/"$d" -n "$OPERATORS_NS" --replicas=0
    count=$((count + 1))
  done
  [[ "${DRY_RUN:-false}" != "true" && -f "${sf}.tmp" ]] && mv "${sf}.tmp" "$sf"
  success "Paused ${count} service operator(s) (state → ${sf})"
}

oc_resume_operators() {
  local sf; sf="$(OPERATOR_STATE_FILE)"
  step "Resuming service operators in ${OPERATORS_NS}"
  if [[ ! -f "$sf" ]]; then warn "No operator state file — nothing to resume."; return 0; fi
  while IFS= read -r line; do
    [[ -z "$line" ]] && continue
    local d="${line%% *}" r="${line##* }"; [[ -z "$r" || "$r" == "$d" ]] && r=1
    info "  resume deploy/${d} → ${r}"
    run oc scale deploy/"$d" -n "$OPERATORS_NS" --replicas="$r"
  done < "$sf"
  success "Service operators resumed"
}

# Scale the SERVICE operands (WKC + DataStage Deployments + StatefulSets) to 0,
# saving counts. Matches only owner-managed resources whose name starts with a
# SERVICE_OPERAND_PREFIXES entry — so zen/platform/IAM operands are untouched.
oc_scale_operands() {
  local mode="$1"   # "down" | "up"
  local sf; sf="$(OPERAND_STATE_FILE)"
  if [[ "$mode" == "down" ]]; then
    step "Scaling service operands in ${NAMESPACE} → 0 (prefixes: ${SERVICE_OPERAND_PREFIXES})"
    local rows kind
    rows=""
    for kind in deployment statefulset; do
      local got
      got=$(oc get "$kind" -n "$NAMESPACE" -o json 2>/dev/null | python3 -c '
import json,sys
raw=sys.stdin.read().strip()
d=json.loads(raw) if raw else {"items":[]}
k=sys.argv[1]
prefixes=sys.argv[2].split()
for o in d.get("items",[]):
    md=o.get("metadata",{}) or {}
    name=md.get("name","?")
    if not md.get("ownerReferences"): continue
    if not any(name.startswith(p) for p in prefixes): continue
    r=(o.get("spec",{}) or {}).get("replicas") or 0
    if r>0: print("%s/%s %d" % (k, name, r))
' "$kind" "$SERVICE_OPERAND_PREFIXES" 2>/dev/null || true)
      [[ -n "$got" ]] && rows+="${got}"$'\n'
    done
    rows="$(printf '%s' "$rows" | sed '/^[[:space:]]*$/d')"
    [[ -z "$rows" ]] && { warn "No running service operands in ${NAMESPACE} (already down?)"; return 0; }
    [[ "${DRY_RUN:-false}" != "true" ]] && printf '%s\n' "$rows" > "$sf"
    while IFS= read -r line; do
      [[ -z "$line" ]] && continue
      info "  scale ${line%% *} → 0 (was ${line##* })"
      run oc scale "${line%% *}" -n "$NAMESPACE" --replicas=0
    done <<< "$rows"
    success "Service operands scaled to 0 (state → ${sf})"
  else
    step "Restoring service operand replica counts in ${NAMESPACE}"
    if [[ ! -f "$sf" ]]; then warn "No operand state — relying on operators to reconcile operands back."; return 0; fi
    while IFS= read -r line; do
      [[ -z "$line" ]] && continue
      local obj="${line%% *}" reps="${line##* }"; [[ -z "$reps" || "$reps" == "$obj" ]] && reps=1
      info "  scale ${obj} → ${reps}"
      run oc scale "$obj" -n "$NAMESPACE" --replicas="$reps"
    done < "$sf"
    success "Service operand replica counts restored"
  fi
}

# Gate: wait until every wxd engine reports engineStatus=shutdown (replaces a
# fixed sleep). Safe under set -e (return is checked by callers).
oc_wait_wxd_stopped() {
  [[ "${DRY_RUN:-false}" == "true" ]] && { info "[dry-run] would wait for wxd engines to stop"; return 0; }
  step "Waiting for wxd engines to report engineStatus=shutdown"
  local elapsed=0
  while [[ $elapsed -lt $WAIT_TIMEOUT ]]; do
    local notdown
    notdown=$(oc get wxdengine -n "$NAMESPACE" \
      -o jsonpath='{range .items[*]}{.status.engineStatus}{"\n"}{end}' 2>/dev/null \
      | grep -vc '^shutdown$' || true); notdown=${notdown:-0}
    [[ "$notdown" -eq 0 ]] && { success "All wxd engines stopped."; return 0; }
    info "  ... ${notdown} engine(s) not yet shutdown (${elapsed}s/${WAIT_TIMEOUT}s)"
    sleep 10; elapsed=$((elapsed + 10))
  done
  warn "wxd engines did not all reach shutdown within ${WAIT_TIMEOUT}s — continuing."
}

# ---------------------------------------------------------------------------
# Wait helper — polls by name prefix until expected_min pods are Running
# ---------------------------------------------------------------------------
wait_for_pods() {
  local label="$1" expected_min="$2"
  shift 2
  local prefixes=("$@")   # remaining args are name prefixes to count
  local elapsed=0
  info "Waiting for ${label} pods (expecting ≥${expected_min} Running) ..."
  while [[ $elapsed -lt $WAIT_TIMEOUT ]]; do
    local running
    running=$(pods_running_by_prefix "${prefixes[@]}")
    if [[ "$running" -ge "$expected_min" ]]; then
      success "${label}: ${running} pod(s) Running"
      return 0
    fi
    info "  ... ${running}/${expected_min} Running (${elapsed}s elapsed)"
    sleep 10
    elapsed=$((elapsed + 10))
  done
  warn "${label} did not reach ${expected_min} running pods within ${WAIT_TIMEOUT}s — check 'oc get pods -n ${NAMESPACE}'"
}

# ---------------------------------------------------------------------------
# Node drain / uncordon (for physical maintenance or OCP upgrades)
# ---------------------------------------------------------------------------
do_drain() {
  step "Draining node: ${NODE_NAME}"
  info "This evicts all non-DaemonSet pods. CPD pods will reschedule on remaining nodes."
  confirm "Drain node ${NODE_NAME}?" || exit 0
  run oc adm drain "${NODE_NAME}" \
    --ignore-daemonsets \
    --delete-emptydir-data \
    --timeout="${WAIT_TIMEOUT}s"
  success "Node ${NODE_NAME} drained — safe to perform maintenance"
}

do_uncordon() {
  step "Uncordoning node: ${NODE_NAME}"
  run oc adm uncordon "${NODE_NAME}"
  success "Node ${NODE_NAME} back in service"
}

# ---------------------------------------------------------------------------
# Main shutdown sequence
# ---------------------------------------------------------------------------
do_shutdown() {
  local method="$([[ $USE_OC == true ]] && echo 'oc patch CRDs (cpd-cli not found)' || echo 'cpd-cli manage')"
  step "CPD MAINTENANCE SHUTDOWN"
  info "Namespace : ${NAMESPACE}"
  info "Method    : ${method}"
  info "Order     : ${SHUTDOWN_ORDER[*]}"
  echo ""

  confirm "Shut down watsonx.data, DataStage, and WKC/IKC in namespace '${NAMESPACE}'?" || exit 0

  if [[ $USE_OC == true ]]; then
    # WKC/DataStage have no shutdown CRD field, and their operators reconcile
    # operands back up if you just scale them. So: wxd via CRD, then pause the
    # operators, then scale the operands to 0. Reverse exactly in do_startup.
    oc_wxd_shutdown
    oc_wait_wxd_stopped
    oc_spark_scale down
    oc_pause_operators
    oc_scale_operands down
  else
    for component in "${SHUTDOWN_ORDER[@]}"; do
      cpdcli_shutdown "$component"
      sleep 5
    done
    oc_wxd_shutdown   # wxd is not in the cpd-cli component model
  fi

  echo ""
  step "Shutdown complete"
  info "wxd shutdown state: $(wxd_shutdown_state)"
  info "Bring it back with:  $0 startup"
  success "Allow ~2-5 min for pods to terminate."
}

# ---------------------------------------------------------------------------
# Main startup sequence
# ---------------------------------------------------------------------------
do_startup() {
  local method="$([[ $USE_OC == true ]] && echo 'oc patch CRDs (cpd-cli not found)' || echo 'cpd-cli manage')"
  step "CPD MAINTENANCE STARTUP"
  info "Namespace : ${NAMESPACE}"
  info "Method    : ${method}"
  info "Order     : ${STARTUP_ORDER[*]}"
  echo ""

  confirm "Start watsonx.data, DataStage, and WKC/IKC in namespace '${NAMESPACE}'?" || exit 0

  if [[ $USE_OC == true ]]; then
    # Reverse of do_shutdown: resume operators first (so they own operands),
    # wait for them, restore operand + Spark replicas, then bring wxd back.
    oc_resume_operators
    info "Waiting for controllers/operands to settle before bringing wxd back..."
    wait_until_ready "$WAIT_TIMEOUT" || warn "Not all workloads ready yet — continuing."
    oc_scale_operands up
    oc_spark_scale restore
    oc_wxd_startup
  else
    for component in "${STARTUP_ORDER[@]}"; do
      cpdcli_startup "$component"
      sleep 10
    done
    oc_wxd_startup
  fi

  echo ""
  step "Startup commands applied — verifying readiness"
  if wait_until_ready "$WAIT_TIMEOUT"; then
    final_report
    success "Startup complete — every workload is fully ready."
    return 0
  fi
  final_report
  error "Some workloads did not reach ready within ${WAIT_TIMEOUT}s. See offenders above."
  warn  "If wxd is stuck at 0/0, the addon playbook may be wedged — restart the operator:"
  warn  "  oc delete pod -n ${OPERATORS_NS} -l control-plane=ibm-lakehouse-controller-manager"
  return 1
}

# ---------------------------------------------------------------------------
# Rolling restart (no downtime — preserves replica counts)
# Use this instead of shutdown/startup when services should stay available.
# ---------------------------------------------------------------------------
do_restart() {
  local method="$([[ $USE_OC == true ]] && echo 'oc rollout restart (cpd-cli not found)' || echo 'cpd-cli manage shutdown+restart')"
  step "CPD ROLLING RESTART"
  info "Namespace : ${NAMESPACE}"
  info "Method    : ${method}"
  info "Scope     : wxd (ibm-lh-*), WKC (wkc-*), DataStage (datastage-*/ds-px-*)"
  [[ "${RESTART_DBS:-false}" == "true" ]] && info "DBs       : EDB clusters WILL be rolling-restarted (--restart-dbs)"
  echo ""

  confirm "Rolling restart wxd + WKC + DataStage in '${NAMESPACE}' (preserves replica counts)?" || exit 0

  if [[ $USE_OC == true ]]; then
    # IMPORTANT: do NOT cycle wxd via spec.shutdown here. When an engine has
    # shutdown_by_addon=true, a shutdown=true→false toggle can strand the
    # metastore/MinIO at 0/0 (the addon's Ansible playbook may not restore it).
    # A plain rollout restart of the ibm-lh workloads recycles pods WITHOUT
    # touching the addon-managed desired state — safe and reversible.
    oc_rollout_restart wxd
    oc_rollout_restart wkc
    oc_rollout_restart datastage

    # EDB CNPG pods are owned by the Cluster CR (not a Deployment/STS), so the
    # rollout restarts above miss them. Only cycle them when explicitly asked.
    if [[ "${RESTART_DBS:-false}" == "true" ]]; then
      step "Rolling-restart EDB CloudNativePG clusters"
      local cls
      cls=$(oc get clusters.postgresql.k8s.enterprisedb.io -n "$NAMESPACE" -o name 2>/dev/null || true)
      if [[ -n "$cls" ]]; then
        while IFS= read -r c; do
          [[ -z "$c" ]] && continue
          info "  annotate restart ${c}"
          run oc annotate "$c" -n "$NAMESPACE" --overwrite \
            "kubectl.kubernetes.io/restartedAt=$(date -u +%FT%TZ)"
        done <<< "$cls"
      fi
      warn "FoundationDBClusters are NOT auto-restarted (no safe per-pod restart)."
    fi
  else
    for component in "${SHUTDOWN_ORDER[@]}"; do cpdcli_shutdown "$component"; sleep 5; done
    info "Waiting for services to quiesce before restart..."; sleep 30
    for component in "${STARTUP_ORDER[@]}"; do cpdcli_startup "$component"; sleep 10; done
  fi

  echo ""
  step "Restart issued — verifying readiness (catches pods stuck at 0/1, 4/5)"
  if wait_until_ready "$WAIT_TIMEOUT"; then
    final_report
    success "Rolling restart complete — every workload is fully ready."
    return 0
  fi
  final_report
  error "Some workloads did not reach ready within ${WAIT_TIMEOUT}s. See offenders above."
  return 1
}

# ---------------------------------------------------------------------------
# verify — read-only: wait until everything is fully ready, then report.
# (do_verify lives in lib/readiness.sh)
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# prepare-upgrade — quiesce heavy compute before an OCP cluster upgrade so each
# node drain evicts far fewer slow-probe pods. Leaves Zen/IAM/scheduler/EDB/FDB
# running. ALWAYS preview with --dry-run first. Reverse with resume-upgrade.
# ---------------------------------------------------------------------------
do_prepare_upgrade() {
  step "CPD PREPARE-UPGRADE (quiesce compute for OCP upgrade)"
  info "Namespace : ${NAMESPACE}   Operators: ${OPERATORS_NS}"
  info "Method    : $([[ $USE_OC == true ]] && echo 'oc' || echo 'cpd-cli manage')"
  info "Quiesces  : wxd (CRD), Spark (scale 0), WKC + DataStage (pause operator + scale 0)"
  info "Stays up  : Zen/console, IAM, scheduler, CCS, EDB, FoundationDB"
  warn "An OCP upgrade does NOT require this — it just makes node drains faster/cleaner"
  warn "by evicting fewer slow-readiness service pods. ALWAYS --dry-run first."
  echo ""

  # Pre-drain DB safety check (warn-only): a single-instance CNPG cluster with a
  # restrictive PDB blocks node drains. Surface it; do not auto-fix.
  step "Pre-drain DB check (CNPG instances / PDBs)"
  oc get clusters.postgresql.k8s.enterprisedb.io -n "$NAMESPACE" \
    -o custom-columns='NAME:.metadata.name,DESIRED:.spec.instances,READY:.status.readyInstances,PHASE:.status.phase' \
    2>/dev/null || warn "  (could not list CNPG clusters)"
  echo ""

  confirm "Quiesce compute in '${NAMESPACE}' for an OCP upgrade?" || exit 0

  if [[ $USE_OC == true ]]; then
    oc_wxd_shutdown          # operator still up so it processes the CRD shutdown
    oc_wait_wxd_stopped      # gate, not a fixed sleep
    oc_spark_scale down
    oc_pause_operators       # pause controllers so operands stay down
    oc_scale_operands down   # now actually take WKC/DataStage/etc. to 0
  else
    cpdcli_shutdown datastage; cpdcli_shutdown wkc
    oc_wxd_shutdown          # wxd is not in the cpd-cli component model
  fi

  echo ""
  step "Compute quiesced — cluster ready for OCP upgrade / node drains"
  info "When the OCP upgrade is done, run:  $0 resume-upgrade"
}

# ---------------------------------------------------------------------------
# resume-upgrade — reverse prepare-upgrade, then gate on full readiness.
# ---------------------------------------------------------------------------
do_resume_upgrade() {
  step "CPD RESUME-UPGRADE (bring compute back)"
  info "Namespace : ${NAMESPACE}   Operators: ${OPERATORS_NS}"
  echo ""
  confirm "Resume CPD services in '${NAMESPACE}' after the upgrade?" || exit 0

  if [[ $USE_OC == true ]]; then
    oc_resume_operators
    info "Waiting for controllers/operands to settle before bringing wxd back..."
    wait_until_ready "$WAIT_TIMEOUT" || warn "Not all workloads ready yet — continuing."
    oc_scale_operands up
    oc_spark_scale restore
    oc_wxd_startup
  else
    cpdcli_startup wkc; cpdcli_startup datastage
    oc_wxd_startup
  fi

  echo ""
  step "Resume issued — verifying readiness"
  if wait_until_ready "$WAIT_TIMEOUT"; then
    final_report
    success "Resume complete — every workload is fully ready."
    return 0
  fi
  final_report
  error "Some workloads did not reach ready within ${WAIT_TIMEOUT}s. See offenders above."
  return 1
}

# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------
oc_login

method_label="$([[ $USE_OC == true ]] && echo 'oc (auto — cpd-cli not found)' || echo 'cpd-cli manage')"
info "Method auto-selected: ${method_label}"

# Actions that gate on readiness can legitimately return 1 (timeout / not ready).
# Guard them with '|| exit 1' so a normal not-ready result exits cleanly instead
# of tripping the ERR trap with a confusing "Command failed" line.
case "$ACTION" in
  status)          print_status ;;
  verify)          do_verify          || exit 1 ;;
  shutdown)        do_shutdown        || exit 1 ;;
  startup)         do_startup         || exit 1 ;;
  restart)         do_restart         || exit 1 ;;
  prepare-upgrade) do_prepare_upgrade || exit 1 ;;
  resume-upgrade)  do_resume_upgrade  || exit 1 ;;
  drain-node)      do_drain ;;
  uncordon-node)   do_uncordon ;;
esac
