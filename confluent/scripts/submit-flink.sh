#!/usr/bin/env bash
# =============================================================================
#  submit-flink.sh — render placeholders, wait for the SQL Gateway, submit jobs
# -----------------------------------------------------------------------------
#  Location  : confluent/scripts/submit-flink.sh
#  Repository: https://github.com/aseelert/ibmas-watsonxdata-dbt
#  Project   : watsonx.data · dbt · Spark · Confluent medallion demo
#  Author    : Alexander Seelert — IBM Customer Success Engineer
#  Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.
#
#  Changelog :
#    v1.0 (2026-06-26) — Initial version. Substitutes the .env-driven
#      ${WXD_OBJECT_STORE_ENDPOINT}, ${SCHEMA_REGISTRY_URL} and
#      ${CONFLUENT_SILVER_SCHEMA} placeholders into silver_jobs.sql at submit
#      time (so the SQL itself stays free of cluster-specific literals), waits
#      for the Flink JobManager + SQL Gateway, then submits the rendered file.
#      Sources scripts/lib/log.sh when available and installs an ERR trap.
#    v1.1 (2026-06-27) — Idempotent re-run: before submitting, query the Flink
#      REST API and cancel every running/restarting job whose name starts with
#      "confluent-silver-", then wait for CANCELED before submitting fresh ones.
#      Requires silver_jobs.sql to set a unique pipeline.name per INSERT job.
# -----------------------------------------------------------------------------
#
#  WHAT / WHY (for an 18-year-old learner)
#    silver_jobs.sql ships with PLACEHOLDERS instead of hostnames/URLs, e.g.
#    's3.endpoint = ${WXD_OBJECT_STORE_ENDPOINT}'. Right before we submit it we
#    swap those placeholders for the real values from .env. That keeps the SQL
#    portable: a different cluster only needs a different .env, never an edit to
#    the SQL. We run in Flink "gateway" mode so this one-shot container does not
#    try to bind its own SQL Gateway (which fails when the container hostname is
#    not DNS-resolvable).
# =============================================================================
set -euo pipefail

# --- Logging helpers ---------------------------------------------------------
# Prefer the shared library (mounted at /opt/lib/log.sh by docker-compose). If
# it is not present (e.g. running ad-hoc outside the container), fall back to a
# tiny inline implementation so the script still works everywhere.
__sourced_log=false
for __candidate in \
  "/opt/lib/log.sh" \
  "$(cd "$(dirname "${BASH_SOURCE[0]}")" 2>/dev/null && pwd)/../../scripts/lib/log.sh"; do
  if [[ -f "$__candidate" ]]; then
    # shellcheck disable=SC1090
    source "$__candidate" && __sourced_log=true && break
  fi
done
if [[ "$__sourced_log" != "true" ]]; then
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

# --- Connection + file settings ---------------------------------------------
JM_HOST="${JOBMANAGER_HOST:-confluent-flink-jobmanager}"
JM_PORT="${JOBMANAGER_PORT:-8081}"
GW_HOST="${SQL_GATEWAY_HOST:-confluent-flink-sql-gateway}"
GW_PORT="${SQL_GATEWAY_PORT:-8083}"
SQL_FILE="${SQL_FILE:-/opt/sql/silver_jobs.sql}"
RENDERED_SQL="${RENDERED_SQL:-/tmp/silver_jobs.rendered.sql}"

# --- Placeholder values (from .env / compose environment) -------------------
# The Flink jobs run INSIDE the Docker network, so they must reach Schema
# Registry by its container name (confluent-schema-registry:8081), NOT by the
# host-facing SCHEMA_REGISTRY_URL from .env (localhost:28081, used by the Python
# producer). We therefore use a DEDICATED override var with a hard in-container
# default and deliberately ignore any inherited host SCHEMA_REGISTRY_URL.
WXD_OBJECT_STORE_ENDPOINT="${WXD_OBJECT_STORE_ENDPOINT:-}"
SCHEMA_REGISTRY_URL="${FLINK_SCHEMA_REGISTRY_URL:-http://confluent-schema-registry:8081}"
CONFLUENT_SILVER_SCHEMA="${CONFLUENT_SILVER_SCHEMA:-confluent_demo_silver}"

if [[ -z "${WXD_OBJECT_STORE_ENDPOINT}" ]]; then
  error "WXD_OBJECT_STORE_ENDPOINT is empty. Run confluent/scripts/expose_minio_route.sh"
  error "and put the printed Route URL into .env before submitting Flink jobs."
  exit 1
fi
if [[ ! -f "${SQL_FILE}" ]]; then
  error "SQL file not found: ${SQL_FILE}"
  exit 1
fi

# --- Render the placeholders -------------------------------------------------
# We use '|' as the sed delimiter because the values contain '/' (URLs). None of
# our values contain '|', so this is safe.
step "Rendering ${SQL_FILE} → ${RENDERED_SQL}"
info "WXD_OBJECT_STORE_ENDPOINT = ${WXD_OBJECT_STORE_ENDPOINT}"
info "SCHEMA_REGISTRY_URL       = ${SCHEMA_REGISTRY_URL}"
info "CONFLUENT_SILVER_SCHEMA   = ${CONFLUENT_SILVER_SCHEMA}"

sed \
  -e "s|\${WXD_OBJECT_STORE_ENDPOINT}|${WXD_OBJECT_STORE_ENDPOINT}|g" \
  -e "s|\${SCHEMA_REGISTRY_URL}|${SCHEMA_REGISTRY_URL}|g" \
  -e "s|\${CONFLUENT_SILVER_SCHEMA}|${CONFLUENT_SILVER_SCHEMA}|g" \
  "${SQL_FILE}" > "${RENDERED_SQL}"

# Fail loudly if any placeholder survived (typo / missing env var) — never submit
# a half-rendered file to Flink.
if grep -q '\${' "${RENDERED_SQL}"; then
  error "Unsubstituted placeholder(s) remain in ${RENDERED_SQL}:"
  grep -n '\${' "${RENDERED_SQL}" >&2 || true
  exit 1
fi
success "SQL rendered, all placeholders substituted."

# --- Wait for the JobManager -------------------------------------------------
step "Waiting for Flink JobManager at http://${JM_HOST}:${JM_PORT}/v1/overview"
until curl -sf "http://${JM_HOST}:${JM_PORT}/v1/overview" >/dev/null 2>&1; do
  info "JobManager not ready — sleeping 3s"
  sleep 3
done
success "JobManager ready."

# --- Wait for the SQL Gateway ------------------------------------------------
step "Waiting for Flink SQL Gateway at http://${GW_HOST}:${GW_PORT}/v1/info"
until curl -sf "http://${GW_HOST}:${GW_PORT}/v1/info" >/dev/null 2>&1; do
  info "SQL Gateway not ready — sleeping 3s"
  sleep 3
done
success "SQL Gateway ready."

# --- Cancel existing confluent-silver-* jobs (idempotent re-run guard) -------
# Each job in silver_jobs.sql has a unique pipeline.name of the form
# "confluent-silver-*". Before submitting, we cancel every running or
# restarting job whose name matches that prefix so a re-run replaces the old
# jobs instead of adding duplicates.  The Flink REST API is used directly
# (no sql-client dependency), which works in both session and standalone modes.
JM_API="http://${JM_HOST}:${JM_PORT}/v1"
SILVER_PREFIX="kafka-raw-to-silver :: \|kafka-silver-to-iceberg :: "

step "Cancelling any existing silver pipeline jobs on ${JM_HOST}:${JM_PORT}"

# Collect job IDs whose name starts with the prefix AND whose state is not
# already terminal (FINISHED | FAILED | CANCELED).
JOBS_JSON="$(curl -sf "${JM_API}/jobs/overview" || echo '{}')"
ACTIVE_JOB_IDS=()
while IFS= read -r line; do
  ACTIVE_JOB_IDS+=("$line")
done < <(
  echo "$JOBS_JSON" \
  | python3 -c "
import sys, json
data = json.load(sys.stdin)
terminal = {'FINISHED', 'FAILED', 'CANCELED'}
for j in data.get('jobs', []):
    if any(j.get('name','').startswith(p) for p in ('kafka-raw-to-silver :: ','kafka-silver-to-iceberg :: ')) \
       and j.get('state', '') not in terminal:
        print(j['jid'])
" 2>/dev/null || true
)

if [[ ${#ACTIVE_JOB_IDS[@]} -eq 0 ]]; then
  info "No active silver pipeline jobs found — nothing to cancel."
else
  for JID in "${ACTIVE_JOB_IDS[@]}"; do
    JOB_NAME="$(echo "$JOBS_JSON" \
      | python3 -c "
import sys, json
data = json.load(sys.stdin)
for j in data.get('jobs', []):
    if j['jid'] == '${JID}':
        print(j['name'])
        break
" 2>/dev/null || echo "${JID}")"
    info "Cancelling job '${JOB_NAME}' (${JID}) ..."
    curl -sf -X PATCH "${JM_API}/jobs/${JID}?mode=cancel" >/dev/null || \
      warn "Cancel request for ${JID} returned non-zero (may already be stopping)."
  done

  # Wait until all targeted jobs reach a terminal state.
  info "Waiting for cancelled jobs to reach terminal state ..."
  MAX_WAIT=60
  WAITED=0
  while [[ $WAITED -lt $MAX_WAIT ]]; do
    STILL_ACTIVE=0
    CURRENT_JOBS="$(curl -sf "${JM_API}/jobs/overview" || echo '{}')"
    for JID in "${ACTIVE_JOB_IDS[@]}"; do
      STATE="$(echo "$CURRENT_JOBS" \
        | python3 -c "
import sys, json
data = json.load(sys.stdin)
terminal = {'FINISHED', 'FAILED', 'CANCELED'}
for j in data.get('jobs', []):
    if j['jid'] == '${JID}':
        print('terminal' if j.get('state','') in terminal else 'running')
        break
else:
    print('terminal')
" 2>/dev/null || echo "terminal")"
      [[ "$STATE" == "running" ]] && (( STILL_ACTIVE++ )) || true
    done
    [[ $STILL_ACTIVE -eq 0 ]] && break
    info "  ${STILL_ACTIVE} job(s) still stopping — sleeping 2s (${WAITED}/${MAX_WAIT}s elapsed)"
    sleep 2
    (( WAITED += 2 ))
  done

  if [[ $WAITED -ge $MAX_WAIT ]]; then
    warn "Timed out waiting for jobs to cancel after ${MAX_WAIT}s — submitting anyway."
  else
    success "All previous silver pipeline jobs cancelled."
  fi
fi

# --- Submit ------------------------------------------------------------------
step "Submitting rendered SQL: ${RENDERED_SQL}"
exec /opt/flink/bin/sql-client.sh gateway \
  -e "http://${GW_HOST}:${GW_PORT}" \
  -f "${RENDERED_SQL}"
