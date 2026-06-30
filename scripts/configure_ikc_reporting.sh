#!/usr/bin/env bash
# -----------------------------------------------------------------------------
#  configure_ikc_reporting.sh — Configure IKC reporting settings for IBM
#                               Knowledge Catalog on Cloud Pak for Data.
#
#  Location  : scripts/configure_ikc_reporting.sh
#  Repository: https://github.com/aseelert/ibmas-watsonxdata-dbt
#  Author    : Alexander Seelert — IBM Customer Success Engineer
#  Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.
#
#  Changelog :
#    v1.0 (2026-06-29) — Initial version. Implements full IBM docs procedure:
#      https://www.ibm.com/docs/en/software-hub/5.3.x?topic=administering-configuring-reporting-settings
#
# WHAT THIS SCRIPT DOES (in order)
#   1. Patches ccs-features-configmap with the chosen reporting flags.
#   2. Ensures ibm-cpd-ccs-operator is running (scales up if replicas=0).
#   3. Patches wkc-cr with dummyone=true to trigger CCS reconciliation.
#   4. Patches wkc-cr: wkc_term_assignment_ta_rules_allow_regex=true.
#   5. Patches wkc-cr: wdp_profiling_load_record_count=true.
#   6. Waits for ccs-cr to return to Completed.
#   7. Restarts the 7 pods required by the IBM docs procedure.
#   8. Deletes metadata-discovery and wkc-metadata-imports-ui pods
#      (they are stateless — the deployment recreates them immediately).
#   9. Waits for all restarts to finish.
#  10. Verifies DEFAULT_AUTHORIZE_REPORTING / ENFORCE_AUTHORIZE_REPORTING on
#      ngp-projects-api and defaultAuthorizeReporting / enforceAuthorizeReporting
#      on catalog-api. Injects the configMapKeyRef env block if missing.
#
# USAGE
#   bash scripts/configure_ikc_reporting.sh                  # default: enforce=false default=true
#   bash scripts/configure_ikc_reporting.sh --enforce        # enforce=true  default=true
#   bash scripts/configure_ikc_reporting.sh --disable        # enforce=false default=false (revert)
#   bash scripts/configure_ikc_reporting.sh --dry-run        # preview only
#   bash scripts/configure_ikc_reporting.sh --skip-restart   # patch only, skip pod restarts
#   bash scripts/configure_ikc_reporting.sh --namespace myCPD
#
# OPTIONS
#   --namespace NS      CPD operands namespace  (default: from .env WXD_OPENSHIFT_NAMESPACE or cpd-instance)
#   --operators-ns NS   CCS operator namespace  (default: cpd-operators)
#   --enforce           Set enforceAuthorizeReporting=true  (default: false)
#   --disable           Revert both flags to false (no reporting by default)
#   --skip-restart      Skip all pod restarts and deletion steps
#   --dry-run           Print what would happen; change nothing
#   -h, --help          Show this help
# -----------------------------------------------------------------------------
set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
[[ -f "${REPO}/.env" ]] && set -a && source "${REPO}/.env" && set +a || true

# ── Defaults ──────────────────────────────────────────────────────────────────
NS="${WXD_OPENSHIFT_NAMESPACE:-cpd-instance}"
OPERATORS_NS="cpd-operators"
ENFORCE_VAL="false"
DEFAULT_VAL="true"
SKIP_RESTART=false
DRY_RUN=false

# ── Colour helpers ────────────────────────────────────────────────────────────
BOLD="\033[1m"; RESET="\033[0m"; RED="\033[0;31m"; GREEN="\033[0;32m"
YELLOW="\033[1;33m"; CYAN="\033[0;36m"; DIM="\033[2m"
info()  { echo -e "${CYAN}[INFO]${RESET}  $*"; }
ok()    { echo -e "${GREEN}[OK]${RESET}    $*"; }
warn()  { echo -e "${YELLOW}[WARN]${RESET}  $*"; }
step()  { echo -e "\n${BOLD}── $* ──${RESET}"; }
die()   { echo -e "${RED}[ERROR]${RESET} $*" >&2; exit 1; }
dry()   { echo -e "${DIM}[DRY]${RESET}   $*"; }

# ── Argument parsing ──────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --namespace)     NS="$2";           shift 2 ;;
    --operators-ns)  OPERATORS_NS="$2"; shift 2 ;;
    --enforce)       ENFORCE_VAL="true";shift   ;;
    --disable)       ENFORCE_VAL="false"; DEFAULT_VAL="false"; shift ;;
    --skip-restart)  SKIP_RESTART=true; shift   ;;
    --dry-run)       DRY_RUN=true;      shift   ;;
    -h|--help) sed -n '18,43p' "${BASH_SOURCE[0]}" | sed 's/^#  \{0,1\}//'; exit 0 ;;
    *) die "Unknown option: $1  (try --help)" ;;
  esac
done

$DRY_RUN && warn "DRY-RUN mode — nothing will be changed.\n"

# ── Helpers ───────────────────────────────────────────────────────────────────
_run() {
  # Execute a command or print it in dry-run mode.
  # Arguments are passed as an array — no eval, no word-splitting.
  if $DRY_RUN; then dry "$*"; else "$@"; fi
}

_wait_rollout() {
  local name="$1"
  info "Waiting for rollout: ${name} …"
  if ! $DRY_RUN; then
    oc -n "${NS}" rollout status "deployment/${name}" --timeout=180s 2>&1 | tail -1 \
      && ok "${name} ready." \
      || warn "${name} rollout timed-out (it may still be progressing)."
  fi
}

# ── Pre-flight ────────────────────────────────────────────────────────────────
step "Pre-flight checks"
command -v oc &>/dev/null || die "'oc' not found — install it and run 'oc login'."
oc whoami &>/dev/null      || die "Not logged in — run: oc login <api-url>"
info "oc: $(oc whoami) on $(oc whoami --show-server)"
info "CPD operands namespace : ${NS}"
info "CCS operators namespace: ${OPERATORS_NS}"
info "enforceAuthorizeReporting → ${ENFORCE_VAL}"
info "defaultAuthorizeReporting → ${DEFAULT_VAL}"

# ─────────────────────────────────────────────────────────────────────────────
# STEP 1 — Patch ccs-features-configmap
# ─────────────────────────────────────────────────────────────────────────────
step "Step 1 — Patch ccs-features-configmap"

CURRENT_ENFORCE="$(oc -n "${NS}" get configmap ccs-features-configmap \
  -o jsonpath='{.data.enforceAuthorizeReporting}' 2>/dev/null || echo 'missing')"
CURRENT_DEFAULT="$(oc -n "${NS}" get configmap ccs-features-configmap \
  -o jsonpath='{.data.defaultAuthorizeReporting}' 2>/dev/null || echo 'missing')"
info "Current  enforceAuthorizeReporting : ${CURRENT_ENFORCE}"
info "Current  defaultAuthorizeReporting : ${CURRENT_DEFAULT}"

if [[ "${CURRENT_ENFORCE}" == "${ENFORCE_VAL}" && "${CURRENT_DEFAULT}" == "${DEFAULT_VAL}" ]]; then
  ok "ConfigMap already has the desired values — skipping patch."
else
  _run oc -n "${NS}" patch configmap ccs-features-configmap --type merge \
    --patch "{\"data\": {\"enforceAuthorizeReporting\": \"${ENFORCE_VAL}\", \"defaultAuthorizeReporting\": \"${DEFAULT_VAL}\"}}"
  ok "ccs-features-configmap patched."
fi

# ─────────────────────────────────────────────────────────────────────────────
# STEP 2 — Ensure ccs-operator is running
# ─────────────────────────────────────────────────────────────────────────────
step "Step 2 — Verify ibm-cpd-ccs-operator"

CCS_READY="$(oc -n "${OPERATORS_NS}" get deploy ibm-cpd-ccs-operator \
  -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo '0')"
if [[ "${CCS_READY:-0}" -lt 1 ]]; then
  warn "ibm-cpd-ccs-operator has 0 ready replicas — scaling up."
  _run oc -n "${OPERATORS_NS}" scale deploy ibm-cpd-ccs-operator --replicas=1
  sleep 10
else
  ok "ibm-cpd-ccs-operator: ${CCS_READY}/1 ready."
fi

# ─────────────────────────────────────────────────────────────────────────────
# STEP 3 — Patch wkc-cr: dummyone (trigger CCS reconciliation)
# ─────────────────────────────────────────────────────────────────────────────
step "Step 3 — Patch wkc-cr to trigger CCS reconciliation"

if $DRY_RUN; then
  dry "oc patch wkc wkc-cr -n ${NS} --type merge --patch '{\"spec\":{\"dummyone\":true}}'"
else
  oc patch wkc wkc-cr -n "${NS}" --type merge --patch '{"spec":{"dummyone":true}}'
fi
ok "wkc-cr patched with dummyone=true."

# ─────────────────────────────────────────────────────────────────────────────
# STEP 4 — Patch wkc-cr: wkc_term_assignment_ta_rules_allow_regex
# ─────────────────────────────────────────────────────────────────────────────
step "Step 4 — Patch wkc-cr: wkc_term_assignment_ta_rules_allow_regex=true"

if $DRY_RUN; then
  dry "oc patch wkc wkc-cr -n ${NS} --type merge --patch '{\"spec\":{\"wkc_term_assignment_ta_rules_allow_regex\":true}}'"
else
  oc patch wkc wkc-cr -n "${NS}" --type merge --patch '{"spec":{"wkc_term_assignment_ta_rules_allow_regex":true}}'
fi
ok "wkc-cr: wkc_term_assignment_ta_rules_allow_regex=true"

# ─────────────────────────────────────────────────────────────────────────────
# STEP 5 — Patch wkc-cr: wdp_profiling_load_record_count
# ─────────────────────────────────────────────────────────────────────────────
step "Step 5 — Patch wkc-cr: wdp_profiling_load_record_count=true"

if $DRY_RUN; then
  dry "oc patch wkc wkc-cr -n ${NS} --type merge --patch '{\"spec\":{\"wdp_profiling_load_record_count\":true}}'"
else
  oc patch wkc wkc-cr -n "${NS}" --type merge --patch '{"spec":{"wdp_profiling_load_record_count":true}}'
fi
ok "wkc-cr: wdp_profiling_load_record_count=true"

# ─────────────────────────────────────────────────────────────────────────────
# STEP 6 — Wait for ccs-cr reconciliation to complete
# ─────────────────────────────────────────────────────────────────────────────
step "Step 6 — Wait for ccs-cr reconciliation"

if ! $DRY_RUN; then
  info "Polling ccs-cr status (max 10 min) …"
  DEADLINE=$(( $(date +%s) + 600 ))
  while true; do
    # Try each known field name in order of preference
    CCS_STATUS="$(oc -n "${NS}" get ccs ccs-cr -o jsonpath='{.status.ccsStatus}' 2>/dev/null || true)"
    [[ -z "${CCS_STATUS}" ]] && \
      CCS_STATUS="$(oc -n "${NS}" get ccs ccs-cr -o jsonpath='{.status.controlPlaneStatus}' 2>/dev/null || true)"
    [[ -z "${CCS_STATUS}" ]] && \
      CCS_STATUS="$(oc -n "${NS}" get ccs ccs-cr -o jsonpath='{.status.wkcStatus}' 2>/dev/null || true)"
    [[ -z "${CCS_STATUS}" ]] && CCS_STATUS="unknown"
    info "ccs-cr status: ${CCS_STATUS}"
    [[ "${CCS_STATUS}" == "Completed" ]] && { ok "ccs-cr reconciliation complete."; break; }
    if [[ $(date +%s) -ge ${DEADLINE} ]]; then
      warn "Timed out waiting for ccs-cr Completed. Last status: ${CCS_STATUS}"
      warn "The reconciliation may still be in progress — continue anyway."
      break
    fi
    sleep 20
  done
else
  dry "Would poll ccs-cr until status=Completed (max 10 min)."
fi

# ─────────────────────────────────────────────────────────────────────────────
# STEP 7 — Restart the 7 required deployments
# ─────────────────────────────────────────────────────────────────────────────
if ! $SKIP_RESTART; then
  step "Step 7 — Restart 7 reporting-related deployments"

  DEPLOYMENTS=(
    wkc-bi-data-service
    wkc-glossary-service
    wkc-gov-ui
    catalog-api
    ngp-projects-api
    portal-catalog
    portal-projects
  )

  for d in "${DEPLOYMENTS[@]}"; do
    if oc -n "${NS}" get deployment "${d}" &>/dev/null; then
      _run oc -n "${NS}" rollout restart "deployment/${d}"
      ok "Restarted: ${d}"
    else
      warn "Deployment not found, skipping: ${d}"
    fi
  done

  # ─────────────────────────────────────────────────────────────────────────
  # STEP 8 — Delete metadata-discovery and wkc-metadata-imports-ui pods
  # ─────────────────────────────────────────────────────────────────────────
  step "Step 8 — Delete metadata-discovery and wkc-metadata-imports-ui pods"

  for pattern in metadata-discovery wkc-metadata-imports-ui; do
    PODS="$(oc -n "${NS}" get pods -o name 2>/dev/null | grep "${pattern}" || true)"
    if [[ -n "${PODS}" ]]; then
      while IFS= read -r pod; do
        _run oc -n "${NS}" delete "${pod}" --grace-period=0 --force 2>/dev/null || \
          _run oc -n "${NS}" delete "${pod}"
        ok "Deleted: ${pod}"
      done <<< "${PODS}"
    else
      info "No running pods matching '${pattern}' — nothing to delete."
    fi
  done

  # ─────────────────────────────────────────────────────────────────────────
  # STEP 9 — Wait for rollouts
  # ─────────────────────────────────────────────────────────────────────────
  step "Step 9 — Wait for rollouts to complete"

  for d in "${DEPLOYMENTS[@]}"; do
    oc -n "${NS}" get deployment "${d}" &>/dev/null && _wait_rollout "${d}" || true
  done
else
  warn "--skip-restart set: skipping pod restarts and pod deletions (steps 7-9)."
fi

# ─────────────────────────────────────────────────────────────────────────────
# STEP 10 — Verify env vars (and inject if missing)
# ─────────────────────────────────────────────────────────────────────────────
step "Step 10 — Verify env vars on ngp-projects-api and catalog-api"

if ! $DRY_RUN; then

  # ── helper: ensure a deployment's reporting env vars come from ccs-features-configmap ──
  # Detects hardcoded literals and replaces them; adds missing vars if absent.
  # Re-entrant: if live value already matches desired, skips the patch.
  _fix_reporting_env() {
    local deploy="$1"
    local env_enforce="$2"   # env var name for enforce  (e.g. ENFORCE_AUTHORIZE_REPORTING)
    local env_default="$3"   # env var name for default  (e.g. DEFAULT_AUTHORIZE_REPORTING)
    local cm_enforce="$4"    # configmap key for enforce (e.g. enforceAuthorizeReporting)
    local cm_default="$5"    # configmap key for default (e.g. defaultAuthorizeReporting)

    # Check live resolved value first — if already correct, nothing to do
    local live_e live_d
    live_e="$(oc -n "${NS}" exec "deployment/${deploy}" -- env 2>/dev/null \
      | grep "^${env_enforce}=" | cut -d= -f2 | tr '[:upper:]' '[:lower:]' || true)"
    live_d="$(oc -n "${NS}" exec "deployment/${deploy}" -- env 2>/dev/null \
      | grep "^${env_default}=" | cut -d= -f2 | tr '[:upper:]' '[:lower:]' || true)"

    if [[ "${live_e}" == "${ENFORCE_VAL}" && "${live_d}" == "${DEFAULT_VAL}" ]]; then
      ok "${deploy}: ${env_enforce}=${live_e}  ${env_default}=${live_d}  ✓ (already correct)"
      return
    fi

    # Find indices of both vars in the deployment spec (may be literal or already keyRef)
    local idx_e idx_d
    idx_e="$(oc -n "${NS}" get deployment "${deploy}" -o json 2>/dev/null | python3 -c "
import sys,json; d=json.load(sys.stdin)
for i,e in enumerate(d['spec']['template']['spec']['containers'][0].get('env',[])):
    if e.get('name','')=='${env_enforce}': print(i); break
" 2>/dev/null || true)"
    idx_d="$(oc -n "${NS}" get deployment "${deploy}" -o json 2>/dev/null | python3 -c "
import sys,json; d=json.load(sys.stdin)
for i,e in enumerate(d['spec']['template']['spec']['containers'][0].get('env',[])):
    if e.get('name','')=='${env_default}': print(i); break
" 2>/dev/null || true)"

    local patch_ops
    if [[ -n "${idx_e}" && -n "${idx_d}" ]]; then
      info "${deploy}: replacing hardcoded literals (indices ${idx_d},${idx_e}) with configMapKeyRef …"
      patch_ops="[
        {\"op\":\"replace\",\"path\":\"/spec/template/spec/containers/0/env/${idx_d}\",\"value\":
          {\"name\":\"${env_default}\",\"valueFrom\":{\"configMapKeyRef\":
            {\"key\":\"${cm_default}\",\"name\":\"ccs-features-configmap\",\"optional\":true}}}},
        {\"op\":\"replace\",\"path\":\"/spec/template/spec/containers/0/env/${idx_e}\",\"value\":
          {\"name\":\"${env_enforce}\",\"valueFrom\":{\"configMapKeyRef\":
            {\"key\":\"${cm_enforce}\",\"name\":\"ccs-features-configmap\",\"optional\":true}}}}
      ]"
    else
      info "${deploy}: env vars absent — adding via configMapKeyRef …"
      patch_ops="[
        {\"op\":\"add\",\"path\":\"/spec/template/spec/containers/0/env/-\",\"value\":
          {\"name\":\"${env_default}\",\"valueFrom\":{\"configMapKeyRef\":
            {\"key\":\"${cm_default}\",\"name\":\"ccs-features-configmap\",\"optional\":true}}}},
        {\"op\":\"add\",\"path\":\"/spec/template/spec/containers/0/env/-\",\"value\":
          {\"name\":\"${env_enforce}\",\"valueFrom\":{\"configMapKeyRef\":
            {\"key\":\"${cm_enforce}\",\"name\":\"ccs-features-configmap\",\"optional\":true}}}}
      ]"
    fi

    if oc -n "${NS}" patch deployment "${deploy}" --type=json -p="${patch_ops}"; then
      oc -n "${NS}" rollout status "deployment/${deploy}" --timeout=120s 2>&1 | tail -1
      local new_e
      new_e="$(oc -n "${NS}" exec "deployment/${deploy}" -- env 2>/dev/null \
        | grep "^${env_enforce}=" | cut -d= -f2 || echo 'pending')"
      ok "${deploy}: ${env_enforce}=${new_e}  ✓"
    else
      warn "Patch failed for ${deploy} — edit manually per IBM docs."
    fi
  }

  # ── ngp-projects-api (uses UPPER_CASE env var names) ─────────────────────
  info "Checking ngp-projects-api …"
  _fix_reporting_env ngp-projects-api \
    ENFORCE_AUTHORIZE_REPORTING DEFAULT_AUTHORIZE_REPORTING \
    enforceAuthorizeReporting   defaultAuthorizeReporting

  # ── catalog-api (uses camelCase env var names) ────────────────────────────
  info "Checking catalog-api …"
  _fix_reporting_env catalog-api \
    enforceAuthorizeReporting defaultAuthorizeReporting \
    enforceAuthorizeReporting defaultAuthorizeReporting

else
  dry "Would verify/fix ENFORCE_AUTHORIZE_REPORTING on ngp-projects-api."
  dry "Would verify/fix enforceAuthorizeReporting on catalog-api."
fi

# ─────────────────────────────────────────────────────────────────────────────
# SUMMARY
# ─────────────────────────────────────────────────────────────────────────────
echo
echo -e "${BOLD}╔══════════════════════════════════════════════════════╗${RESET}"
echo -e "${BOLD}║    IKC Reporting Configuration — Summary             ║${RESET}"
echo -e "${BOLD}╠══════════════════════════════════════════════════════╣${RESET}"
printf "${BOLD}║${RESET}  %-30s : %-20s${BOLD}║${RESET}\n" "enforceAuthorizeReporting" "${ENFORCE_VAL}"
printf "${BOLD}║${RESET}  %-30s : %-20s${BOLD}║${RESET}\n" "defaultAuthorizeReporting" "${DEFAULT_VAL}"
printf "${BOLD}║${RESET}  %-30s : %-20s${BOLD}║${RESET}\n" "wkc_term_assignment_regex" "true"
printf "${BOLD}║${RESET}  %-30s : %-20s${BOLD}║${RESET}\n" "wdp_profiling_load_record_count" "true"
printf "${BOLD}║${RESET}  %-30s : %-20s${BOLD}║${RESET}\n" "namespace" "${NS}"
echo -e "${BOLD}╚══════════════════════════════════════════════════════╝${RESET}"

cat <<EOF

${BOLD}Verify manually:${RESET}
  oc set env -n ${NS} deployment/ngp-projects-api --list | grep -i reporting
  oc set env -n ${NS} deployment/catalog-api       --list | grep -i reporting

${BOLD}Check wkc-cr spec fields:${RESET}
  oc -n ${NS} get wkc wkc-cr -o jsonpath='{.spec.wkc_term_assignment_ta_rules_allow_regex}{" "}{.spec.wdp_profiling_load_record_count}' && echo

${BOLD}Re-run idempotently (to enforce=true):${RESET}
  bash scripts/configure_ikc_reporting.sh --enforce

${BOLD}Revert to defaults (both false):${RESET}
  bash scripts/configure_ikc_reporting.sh --disable

EOF
