# =============================================================================
#  readiness.sh — robust, generic readiness verification for CPD / OpenShift
#
#  Location  : scripts/lib/readiness.sh   (sourceable; depends on lib/log.sh)
#  Purpose   : Prove that EVERY CPD workload is FULLY ready after a
#              restart / startup. Catches the cases a naive deployment-level
#              check misses:
#                - pods stuck at READY 0/1 or 4/5 while STATUS=Running
#                  (slow readiness probes during a surge rollout)
#                - CrashLoopBackOff / ImagePullBackOff / CreateContainerError
#                - incomplete rollouts (observedGeneration / revision lag)
#                - EDB CloudNativePG clusters not "healthy"
#                - FoundationDB clusters without full replication
#
#  Public API (all match the log lib: info/success/warn/error/step/run):
#       verify_ready [--quiet]          -> 0 if EVERYTHING ready, 1 otherwise
#       wait_until_ready [timeout] [interval]
#       final_report
#       cpdcli_status_gate              -> optional cpd-cli CR-status gate
#       do_verify                       -> the 'verify' action driver
#  Per-kind helpers (each emits TAB-separated offender rows, returns 0):
#       check_pods <ns>  check_deployments <ns>  check_statefulsets <ns>
#       check_cnpg_clusters <ns>  check_fdb_clusters <ns>
#
#  Offender row format (TAB separated):
#       <namespace>\t<group>\t<kind>\t<name>\t<ready>\t<detail [STUCK|TERMINATING]>
#
#  Tunables (env or flags):
#       NAMESPACE           operand namespace            (default cpd-instance)
#       READY_TIMEOUT       wait_until_ready cap, sec    (default 900)
#       READY_INTERVAL      poll interval, sec           (default 15)
#       READY_NAMESPACES    space-separated ns list to verify
#                           (default: $NAMESPACE cpd-operators
#                            ibm-cpd-scheduler ibm-licensing)
#
#  Design notes (why this is correct):
#   * POD truth  : a pod is ready IFF phase==Running AND every container
#                  .ready==true. Job-owned pods and one-shot terminal pods
#                  (Succeeded/Failed) are EXCLUDED — Completed is correct for
#                  them. During a surge rollout the OLD pod is Terminating
#                  (has .metadata.deletionTimestamp) — we tag those
#                  [TERMINATING] and DO NOT fail on them; the NEW pod that is
#                  not-ready has no deletionTimestamp and is tagged [STUCK].
#   * WORKLOAD   : the authoritative gate. Deployment/StatefulSet readiness is
#                  computed from status fields (NOT 'oc rollout status', which
#                  can hang on a StatefulSet with OnDelete updateStrategy).
#   * python3    : used only to parse JSON robustly. POSIX-ish bash otherwise.
#                  Safe under 'set -euo pipefail' and bash 3.2 (macOS): no
#                  mapfile / associative arrays.
# =============================================================================

# Guard against double-sourcing.
if [[ -n "${__READINESS_SH_LOADED:-}" ]]; then
  return 0 2>/dev/null || true
fi
__READINESS_SH_LOADED=1

# If sourced standalone (not via cpd_maintenance.sh) make sure the log lib and
# a couple of vars exist so the functions still work.
if ! declare -F info >/dev/null 2>&1; then
  __rdir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  # shellcheck disable=SC1090
  source "${__rdir}/log.sh"
fi

NAMESPACE="${NAMESPACE:-cpd-instance}"
READY_TIMEOUT="${READY_TIMEOUT:-900}"
READY_INTERVAL="${READY_INTERVAL:-15}"

# Build the namespace list (operands + operators + scheduler + licensing).
# Operators MUST be checked too: if a controller in cpd-operators is down it
# silently stops reconciling the operands. Word-splitting the default is
# intentional. Override with: READY_NAMESPACES="cpd-instance cpd-operators" ...
if [[ -z "${__READY_NS_INIT:-}" ]]; then
  # shellcheck disable=SC2206
  READY_NAMESPACES=( ${READY_NAMESPACES:-${NAMESPACE} cpd-operators ibm-cpd-scheduler ibm-licensing} )
  __READY_NS_INIT=1
fi

# ---------------------------------------------------------------------------
# check_pods <namespace>
#   POD-LEVEL TRUTH. Emits one offender row per NOT-ready, non-Job, non-terminal
#   pod. Handles the surge case (old pod Terminating vs new pod Stuck) and
#   extracts the real reason (CrashLoopBackOff / ImagePullBackOff / Unschedulable
#   / PodInitializing ...), READY n/m and restartCount.
# ---------------------------------------------------------------------------
check_pods() {
  local ns="${1:-$NAMESPACE}" jf
  jf="$(mktemp)"
  oc get pods -n "$ns" -o json > "$jf" 2>/dev/null || { rm -f "$jf"; return 0; }
  python3 - "$jf" "$ns" <<'PY'
import json, sys
jf, ns = sys.argv[1], sys.argv[2]
def grp(n):
    if n.startswith("ibm-lh-"): return "wxd"
    if n.startswith("wkc-") or n.startswith("knowledge-accelerators"): return "wkc"
    if n.startswith(("datastage-", "ds-px-", "ibmas-datastage-")): return "datastage"
    if n.startswith(("spark-master-", "spark-worker-", "spark-hb-")): return "spark"
    return "platform"
try:
    data = json.load(open(jf))
except Exception:
    sys.exit(0)
for pod in data.get("items", []):
    md = pod.get("metadata", {}) or {}
    st = pod.get("status", {}) or {}
    name = md.get("name", "?")
    # EXCLUDE Job-owned pods (init/migration jobs legitimately end Completed).
    if any((o or {}).get("kind") == "Job" for o in (md.get("ownerReferences") or [])):
        continue
    phase = st.get("phase", "Unknown")
    # A live workload is only ever Running / Pending / Unknown. Succeeded and
    # Failed are terminal leftovers (one-shot pods, evicted pods) -> skip.
    if phase not in ("Running", "Pending", "Unknown"):
        continue
    cs = st.get("containerStatuses") or []
    total = len(cs)
    rdy = sum(1 for c in cs if c.get("ready"))
    # READY iff Running AND every container ready.
    if phase == "Running" and total > 0 and rdy == total:
        continue
    terminating = md.get("deletionTimestamp") is not None
    restarts = sum(int(c.get("restartCount", 0) or 0) for c in cs)
    reason = ""
    for c in cs:
        if not c.get("ready"):
            stt = c.get("state", {}) or {}
            w = stt.get("waiting"); t = stt.get("terminated")
            if w:
                reason = w.get("reason", "") or ""
            elif t:
                reason = "Term:" + (t.get("reason", "") or "")
            if reason:
                break
    if not reason:
        for cond in (st.get("conditions") or []):
            if cond.get("type") == "PodScheduled" and cond.get("status") != "True":
                reason = cond.get("reason", "Unschedulable") or "Unschedulable"
                break
    if not reason:
        # The common case: phase=Running but a container's readiness probe has
        # not passed yet (the 0/1, 4/5 symptom). No waiting/terminated state to
        # mine, so spell it out rather than printing a bare "Running".
        if phase == "Running" and total and rdy < total:
            reason = "containers not ready (%d/%d)" % (rdy, total)
        else:
            reason = phase
    if total:
        ready_str = "%d/%d" % (rdy, total)
    else:
        ready_str = "0/%d" % len(pod.get("spec", {}).get("containers", []) or [])
    klass = "TERMINATING" if terminating else "STUCK"
    detail = "phase=%s restarts=%d reason=%s [%s]" % (phase, restarts, reason, klass)
    print("\t".join([ns, grp(name), "Pod", name, ready_str, detail]))
PY
  rm -f "$jf"
}

# ---------------------------------------------------------------------------
# check_deployments <namespace>
#   WORKLOAD TRUTH. Ready IFF spec.replicas == readyReplicas == availableReplicas
#   == updatedReplicas == status.replicas (no surge left) AND
#   observedGeneration >= metadata.generation. Equivalent to a completed
#   'oc rollout status deploy/X' but without blocking.
#
#   Single-resource jsonpath equivalent (for reference / debugging):
#     oc get deploy/X -n NS -o jsonpath='spec={.spec.replicas} ready={.status.readyReplicas} avail={.status.availableReplicas} upd={.status.updatedReplicas} obs={.status.observedGeneration} gen={.metadata.generation}{"\n"}'
# ---------------------------------------------------------------------------
check_deployments() {
  local ns="${1:-$NAMESPACE}" jf
  jf="$(mktemp)"
  oc get deploy -n "$ns" -o json > "$jf" 2>/dev/null || { rm -f "$jf"; return 0; }
  python3 - "$jf" "$ns" <<'PY'
import json, sys
jf, ns = sys.argv[1], sys.argv[2]
def grp(n):
    if n.startswith("ibm-lh-"): return "wxd"
    if n.startswith("wkc-") or n.startswith("knowledge-accelerators"): return "wkc"
    if n.startswith(("datastage-", "ds-px-", "ibmas-datastage-")): return "datastage"
    if n.startswith(("spark-master-", "spark-worker-", "spark-hb-")): return "spark"
    return "platform"
try:
    data = json.load(open(jf))
except Exception:
    sys.exit(0)
for d in data.get("items", []):
    md = d.get("metadata", {}) or {}
    sp = d.get("spec", {}) or {}
    st = d.get("status", {}) or {}
    name = md.get("name", "?")
    desired = sp.get("replicas", 1) or 0
    rdy   = st.get("readyReplicas", 0) or 0
    avail = st.get("availableReplicas", 0) or 0
    upd   = st.get("updatedReplicas", 0) or 0
    cur   = st.get("replicas", 0) or 0
    og    = st.get("observedGeneration", 0) or 0
    gen   = md.get("generation", 0) or 0
    if (desired == rdy == avail == upd == cur) and og >= gen:
        continue
    detail = ("desired=%d ready=%d avail=%d updated=%d current=%d obsGen=%d/%d [STUCK]"
              % (desired, rdy, avail, upd, cur, og, gen))
    print("\t".join([ns, grp(name), "Deployment", name, "%d/%d" % (rdy, desired), detail]))
PY
  rm -f "$jf"
}

# ---------------------------------------------------------------------------
# check_statefulsets <namespace>
#   Ready IFF spec.replicas == readyReplicas == currentReplicas == updatedReplicas
#   AND currentRevision == updateRevision AND observedGeneration current.
#
#   CAVEAT: for updateStrategy.type == OnDelete the controller does NOT roll
#   pods automatically, so currentRevision can stay != updateRevision forever
#   and 'oc rollout status sts/X' HANGS. For OnDelete we relax to
#   (readyReplicas == spec.replicas) AND observedGeneration current.
#
#   Single-resource jsonpath equivalent:
#     oc get sts/X -n NS -o jsonpath='spec={.spec.replicas} ready={.status.readyReplicas} cur={.status.currentReplicas} upd={.status.updatedReplicas} curRev={.status.currentRevision} updRev={.status.updateRevision}{"\n"}'
# ---------------------------------------------------------------------------
check_statefulsets() {
  local ns="${1:-$NAMESPACE}" jf
  jf="$(mktemp)"
  oc get sts -n "$ns" -o json > "$jf" 2>/dev/null || { rm -f "$jf"; return 0; }
  python3 - "$jf" "$ns" <<'PY'
import json, sys
jf, ns = sys.argv[1], sys.argv[2]
def grp(n):
    if n.startswith("ibm-lh-"): return "wxd"
    if n.startswith("wkc-") or n.startswith("knowledge-accelerators"): return "wkc"
    if n.startswith(("datastage-", "ds-px-", "ibmas-datastage-")): return "datastage"
    if n.startswith(("spark-master-", "spark-worker-", "spark-hb-")): return "spark"
    return "platform"
try:
    data = json.load(open(jf))
except Exception:
    sys.exit(0)
for s in data.get("items", []):
    md = s.get("metadata", {}) or {}
    sp = s.get("spec", {}) or {}
    st = s.get("status", {}) or {}
    name = md.get("name", "?")
    desired = sp.get("replicas", 1) or 0
    strat = ((sp.get("updateStrategy") or {}).get("type") or "RollingUpdate")
    rdy = st.get("readyReplicas", 0) or 0
    cur = st.get("currentReplicas", 0) or 0
    upd = st.get("updatedReplicas", 0) or 0
    crev = st.get("currentRevision", "") or ""
    urev = st.get("updateRevision", "") or ""
    og = st.get("observedGeneration", 0) or 0
    gen = md.get("generation", 0) or 0
    if strat == "OnDelete":
        ok = (desired == rdy) and og >= gen
        note = "OnDelete"
    else:
        ok = (desired == rdy == cur == upd) and (crev == urev) and og >= gen
        note = "rev=" + ("same" if crev == urev else "DIFF")
    if ok:
        continue
    detail = ("desired=%d ready=%d cur=%d upd=%d %s obsGen=%d/%d [STUCK]"
              % (desired, rdy, cur, upd, note, og, gen))
    print("\t".join([ns, grp(name), "StatefulSet", name, "%d/%d" % (rdy, desired), detail]))
PY
  rm -f "$jf"
}

# ---------------------------------------------------------------------------
# check_cnpg_clusters <namespace>
#   EDB CloudNativePG: clusters.postgresql.k8s.enterprisedb.io
#   Ready IFF status.readyInstances == spec.instances
#        AND status.phase == "Cluster in healthy state".
#   (Fully-qualified resource name avoids clashing with other 'cluster' CRDs.)
#
#   Single-resource jsonpath:
#     oc get clusters.postgresql.k8s.enterprisedb.io/X -n NS \
#        -o jsonpath='inst={.spec.instances} ready={.status.readyInstances} phase={.status.phase}{"\n"}'
# ---------------------------------------------------------------------------
check_cnpg_clusters() {
  local ns="${1:-$NAMESPACE}" jf
  jf="$(mktemp)"
  oc get clusters.postgresql.k8s.enterprisedb.io -n "$ns" -o json > "$jf" 2>/dev/null \
    || { rm -f "$jf"; return 0; }
  python3 - "$jf" "$ns" <<'PY'
import json, sys
jf, ns = sys.argv[1], sys.argv[2]
try:
    data = json.load(open(jf))
except Exception:
    sys.exit(0)
for c in data.get("items", []):
    md = c.get("metadata", {}) or {}
    sp = c.get("spec", {}) or {}
    st = c.get("status", {}) or {}
    name = md.get("name", "?")
    desired = sp.get("instances", 0) or 0
    rdy = st.get("readyInstances", 0) or 0
    phase = st.get("phase", "") or ""
    if (rdy == desired) and phase == "Cluster in healthy state":
        continue
    detail = "phase=%r [STUCK]" % phase
    print("\t".join([ns, "edb", "Cluster", name, "%d/%d inst" % (rdy, desired), detail]))
PY
  rm -f "$jf"
}

# ---------------------------------------------------------------------------
# check_fdb_clusters <namespace>
#   FoundationDBCluster. Ready IFF status.health.available == true
#        AND status.health.healthy == true AND status.health.fullReplication == true.
#
#   Single-resource jsonpath:
#     oc get foundationdbcluster/X -n NS \
#        -o jsonpath='avail={.status.health.available} healthy={.status.health.healthy} fullRepl={.status.health.fullReplication}{"\n"}'
# ---------------------------------------------------------------------------
check_fdb_clusters() {
  local ns="${1:-$NAMESPACE}" jf
  jf="$(mktemp)"
  oc get foundationdbcluster -n "$ns" -o json > "$jf" 2>/dev/null || { rm -f "$jf"; return 0; }
  python3 - "$jf" "$ns" <<'PY'
import json, sys
jf, ns = sys.argv[1], sys.argv[2]
try:
    data = json.load(open(jf))
except Exception:
    sys.exit(0)
for c in data.get("items", []):
    md = c.get("metadata", {}) or {}
    st = c.get("status", {}) or {}
    name = md.get("name", "?")
    h = st.get("health", {}) or {}
    av = bool(h.get("available", False))
    he = bool(h.get("healthy", False))
    fr = bool(h.get("fullReplication", False))
    if av and he and fr:
        continue
    detail = "available=%s healthy=%s fullReplication=%s [STUCK]" % (av, he, fr)
    print("\t".join([ns, "fdb", "FoundationDBCluster", name, "unhealthy", detail]))
PY
  rm -f "$jf"
}

# ---------------------------------------------------------------------------
# verify_ready [--quiet]
#   Single-pass authoritative check across every namespace in READY_NAMESPACES.
#   Returns 0 IFF there are zero [STUCK] offenders (terminating surge pods are
#   reported but NOT counted as failures). When READY_OFFENDERS_FILE is set
#   (the wait loop sets it), the raw offender list is copied there so callers
#   can render the shrinking list.
# ---------------------------------------------------------------------------
verify_ready() {
  local quiet=false
  [[ "${1:-}" == "--quiet" ]] && quiet=true

  # FAIL CLOSED: every collector below is "oc get ... 2>/dev/null || true", so a
  # broken/unauthenticated oc (token expiry mid-wait, wrong context, API blip)
  # would yield an EMPTY offender list and falsely report "all ready". The
  # operand namespace ALWAYS has hundreds of pods, so an empty/failed list there
  # is provably a connectivity failure, not health. Probe once and refuse to
  # green on failure. This is the single most important safety property for a
  # tool whose job is to gate upgrades on real readiness.
  if ! oc get pods -n "$NAMESPACE" -o name >/dev/null 2>&1; then
    $quiet || error "oc cannot list pods in ${NAMESPACE} (not logged in / wrong context?). Refusing to report READY."
    return 1
  fi

  local f
  f="$(mktemp)"
  local ns
  for ns in "${READY_NAMESPACES[@]}"; do
    { check_pods "$ns"
      check_deployments "$ns"
      check_statefulsets "$ns"
      check_cnpg_clusters "$ns"
      check_fdb_clusters "$ns"
    } >> "$f" 2>/dev/null || true
  done

  # Publish for the wait loop, if requested.
  if [[ -n "${READY_OFFENDERS_FILE:-}" ]]; then
    cp "$f" "$READY_OFFENDERS_FILE"
  fi

  local stuck term n_stuck n_term
  stuck="$(grep -v '\[TERMINATING\]' "$f" 2>/dev/null || true)"
  term="$(grep    '\[TERMINATING\]' "$f" 2>/dev/null || true)"
  n_stuck=$(printf '%s' "$stuck" | grep -c '[^[:space:]]' || true); n_stuck=${n_stuck:-0}
  n_term=$(printf '%s'  "$term"  | grep -c '[^[:space:]]' || true); n_term=${n_term:-0}
  rm -f "$f"

  if [[ "$n_stuck" -eq 0 ]]; then
    if ! $quiet; then
      success "All workloads READY in: ${READY_NAMESPACES[*]}"
      if [[ "$n_term" -gt 0 ]]; then
        info "(${n_term} old pod(s) still Terminating from a rollout surge — transient, not blocking.)"
      fi
    fi
    return 0
  fi

  if ! $quiet; then
    warn "${n_stuck} workload(s) NOT ready:"
    printf '  %-16s %-9s %-18s %-44s %-12s %s\n' NAMESPACE GROUP KIND NAME READY DETAIL
    printf '%s\n' "$stuck" | while IFS=$'\t' read -r o_ns o_grp o_kind o_name o_ready o_detail; do
      [[ -z "$o_name" ]] && continue
      printf '  %-16s %-9s %-18s %-44s %-12s %s\n' \
        "$o_ns" "$o_grp" "$o_kind" "$o_name" "$o_ready" "$o_detail"
    done
    if [[ "$n_term" -gt 0 ]]; then
      info "(${n_term} terminating pod(s) ignored as transient rollout surge.)"
    fi
  fi
  return 1
}

# ---------------------------------------------------------------------------
# wait_until_ready [timeout_sec] [interval_sec]
#   Poll verify_ready until everything is green or the timeout is hit. Prints
#   the SHRINKING offender list each round. Returns 0 on success, non-zero on
#   timeout with a full red report. Safe under 'set -euo pipefail' (verify_ready
#   is always called in an if-condition so its non-zero return never trips -e).
# ---------------------------------------------------------------------------
wait_until_ready() {
  local timeout="${1:-$READY_TIMEOUT}" interval="${2:-$READY_INTERVAL}" elapsed=0
  READY_OFFENDERS_FILE="$(mktemp)"
  step "Waiting for all CPD workloads to become READY (timeout ${timeout}s, poll ${interval}s)"
  info "Namespaces: ${READY_NAMESPACES[*]}"
  while true; do
    if verify_ready --quiet; then
      success "All workloads READY after ${elapsed}s."
      verify_ready                     # print the green summary
      rm -f "$READY_OFFENDERS_FILE"; unset READY_OFFENDERS_FILE
      return 0
    fi
    local n
    n=$(grep -vc '\[TERMINATING\]' "$READY_OFFENDERS_FILE" 2>/dev/null || true); n=${n:-0}
    if [[ "$elapsed" -ge "$timeout" ]]; then
      error "TIMEOUT after ${elapsed}s — ${n} workload(s) still NOT ready:"
      verify_ready                     # full red report
      rm -f "$READY_OFFENDERS_FILE"; unset READY_OFFENDERS_FILE
      return 1
    fi
    info "  ${n} not-ready at ${elapsed}s — re-checking in ${interval}s:"
    grep -v '\[TERMINATING\]' "$READY_OFFENDERS_FILE" 2>/dev/null \
      | awk -F'\t' 'NF{printf "       - [%s] %-16s %s/%s  %s  (%s)\n",$1,$2,$3,$4,$5,$6}' || true
    sleep "$interval"
    elapsed=$((elapsed + interval))
  done
}

# ---------------------------------------------------------------------------
# final_report
#   Concise per-service-group table (ready/total), per-namespace rollups, the
#   EDB / FDB cluster views, and the authoritative offender list.
# ---------------------------------------------------------------------------
final_report() {
  step "FINAL READINESS REPORT — ${READY_NAMESPACES[*]}"

  local files=() ns jf
  for ns in "${READY_NAMESPACES[@]}"; do
    jf="$(mktemp)"
    oc get pods -n "$ns" -o json > "$jf" 2>/dev/null || echo '{}' > "$jf"
    files+=("$jf")
  done

  python3 - "$NAMESPACE" "${READY_NAMESPACES[*]}" "${files[@]}" <<'PY'
import json, sys
from collections import Counter
operand = sys.argv[1]
nslist = sys.argv[2].split()
files = sys.argv[3:]
def grp(n):
    if n.startswith("ibm-lh-"): return "wxd"
    if n.startswith("wkc-") or n.startswith("knowledge-accelerators"): return "wkc"
    if n.startswith(("datastage-", "ds-px-", "ibmas-datastage-")): return "datastage"
    if n.startswith(("spark-master-", "spark-worker-", "spark-hb-")): return "spark"
    return "platform"
gt = Counter(); gr = Counter()      # operand: per service-group total / ready
nt = Counter(); nr = Counter()      # per-namespace total / ready
for fn in files:
    try:
        data = json.load(open(fn))
    except Exception:
        continue
    for pod in data.get("items", []):
        md = pod.get("metadata", {}) or {}
        st = pod.get("status", {}) or {}
        ns = md.get("namespace", "")
        name = md.get("name", "?")
        if ns not in nslist:
            continue
        if any((o or {}).get("kind") == "Job" for o in (md.get("ownerReferences") or [])):
            continue
        phase = st.get("phase", "Unknown")
        if phase not in ("Running", "Pending", "Unknown"):
            continue
        if md.get("deletionTimestamp"):           # ignore terminating surge pods
            continue
        cs = st.get("containerStatuses") or []
        ready = phase == "Running" and len(cs) > 0 and all(c.get("ready") for c in cs)
        nt[ns] += 1; nr[ns] += 1 if ready else 0
        if ns == operand:
            g = grp(name); gt[g] += 1; gr[g] += 1 if ready else 0
print("  Service groups in %s:" % operand)
print("    %-12s %-9s %s" % ("GROUP", "READY", "STATE"))
for g in ["wxd", "wkc", "datastage", "spark", "platform"]:
    if gt[g] == 0:
        continue
    print("    %-12s %-9s %s" % (g, "%d/%d" % (gr[g], gt[g]),
                                 "OK" if gr[g] == gt[g] else "DEGRADED"))
print("")
print("  Per-namespace pod readiness:")
print("    %-22s %-9s %s" % ("NAMESPACE", "READY", "STATE"))
for ns in nslist:
    if nt[ns] == 0:
        continue
    print("    %-22s %-9s %s" % (ns, "%d/%d" % (nr[ns], nt[ns]),
                                 "OK" if nr[ns] == nt[ns] else "DEGRADED"))
PY
  rm -f "${files[@]}"

  echo ""
  info "EDB CloudNativePG clusters (${NAMESPACE}):"
  oc get clusters.postgresql.k8s.enterprisedb.io -n "$NAMESPACE" \
    -o custom-columns='NAME:.metadata.name,DESIRED:.spec.instances,READY:.status.readyInstances,PHASE:.status.phase' \
    2>/dev/null || echo "  <none / CRD not present>"

  echo ""
  info "FoundationDB clusters (${NAMESPACE}):"
  oc get foundationdbcluster -n "$NAMESPACE" \
    -o custom-columns='NAME:.metadata.name,AVAILABLE:.status.health.available,HEALTHY:.status.health.healthy,FULLREPL:.status.health.fullReplication' \
    2>/dev/null || echo "  <none / CRD not present>"

  echo ""
  verify_ready
}

# ---------------------------------------------------------------------------
# cpdcli_status_gate
#   When cpd-cli is installed, ask it for control-plane CR status as an extra
#   authoritative gate. cpd-cli does NOT solve the pod surge / 0-1 problem, so
#   the oc-based verify_ready ALWAYS runs as well. No-op when cpd-cli absent.
# ---------------------------------------------------------------------------
cpdcli_status_gate() {
  if ! command -v cpd-cli >/dev/null 2>&1; then
    info "cpd-cli not installed — skipping CR-status gate (oc verification is authoritative)."
    return 0
  fi
  step "cpd-cli CR-status gate"
  run cpd-cli manage get-cr-status --cpd_instance_ns="$NAMESPACE" \
    || warn "cpd-cli get-cr-status returned non-zero — relying on the oc verification below."
}

# ---------------------------------------------------------------------------
# do_verify
#   The 'verify' action: optional cpd-cli gate, then wait until everything is
#   ready (oc/jsonpath truth), then print the final report. Returns non-zero
#   when the cluster did not reach full readiness within READY_TIMEOUT.
# ---------------------------------------------------------------------------
do_verify() {
  step "CPD READINESS VERIFICATION"
  info "Namespaces : ${READY_NAMESPACES[*]}"
  info "Timeout    : ${READY_TIMEOUT}s   Poll: ${READY_INTERVAL}s"

  # cpd-cli path (real environments that have it). USE_OC is set by the caller
  # script; default to oc-only when it is unset.
  if [[ "${USE_OC:-true}" == false ]]; then
    cpdcli_status_gate
  fi

  if wait_until_ready "$READY_TIMEOUT" "$READY_INTERVAL"; then
    final_report
    success "VERIFIED: every CPD workload is fully ready."
    return 0
  fi
  final_report
  error "NOT VERIFIED: some workloads did not reach ready within ${READY_TIMEOUT}s."
  return 1
}
