#!/usr/bin/env bash
# -----------------------------------------------------------------------------
#  provision_pg_reporting.sh — full end-to-end setup of the ibmas_reporting
#                              PostgreSQL database with CPD integration.
#
#  Location  : scripts/provision_pg_reporting.sh
#  Repository: https://github.ibm.com/alexander/ibmas-watsonxdata-dbt
#  Author    : Alexander Seelert — IBM Customer Success Engineer
#  Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.
#
#  Changelog :
#    v3.0 (2026-06-29) — Complete rewrite. Single script covers all steps.
# -----------------------------------------------------------------------------
#
# WHAT THIS SCRIPT DOES (in order)
#   1. PostgreSQL  — creates database ibmas_reporting, user ibmas_reporting_user,
#                    and schema ibmas_reporting inside the standalone OpenShift
#                    DeploymentConfig postgresql pod. Uses local Unix-socket trust
#                    (no superuser password needed). Smoke-tests TCP auth.
#   2. K8s Secret  — stores all credentials in  ibmas-reporting-creds  (namespace
#                    cpd-instance). Idempotent (create-or-update via apply).
#   3. CPD role    — grants  wkc_reporting_administrator  to cpadmin via the
#                    CPD icp4d-api so the reporting UI and MCP tools stop
#                    returning IKCBI2019E.
#   4. CPD connection — registers  ibmas-reporting  as a plain PostgreSQL data
#                    connection in the ibmas-ingest-demo project via the CPD
#                    connections REST API.
#   5. DSD (optional, --dsd) — creates an IBMAS-Reporting-Postgres-DSD entry.
#                    Pass --external-url to also register an external hostname
#                    (e.g. for workstation port-forward access).
#   6. Summary     — prints all connection details and next-step commands.
#
# PREREQUISITES
#   • oc  CLI: logged in   (oc login …)
#   • .env loaded (WXD_API_KEY, WXD_CPD_HOST, WXD_CPD_AUTH_URL, WXD_CPD_USERNAME)
#     OR pass --cpd-host / --cpd-user / --cpd-password explicitly.
#
# USAGE
#   bash scripts/provision_pg_reporting.sh
#   bash scripts/provision_pg_reporting.sh --dry-run
#   bash scripts/provision_pg_reporting.sh --dsd
#   bash scripts/provision_pg_reporting.sh --dsd --external-url myhost.example.com:15432
#   bash scripts/provision_pg_reporting.sh --cpd-user cpadmin --cpd-password secret
#
# OPTIONS
#   --namespace NS       OpenShift namespace      (default: cpd-instance)
#   --db DB              Reporting database        (default: ibmas_reporting)
#   --schema SCHEMA      Schema inside database    (default: ibmas_reporting)
#   --user USER          Reporting PG user         (default: ibmas_reporting_user)
#   --secret NAME        K8s Secret name           (default: ibmas-reporting-creds)
#   --svc SVC            PG Service name           (default: postgresql)
#   --project-id ID      CPD project ID            (default: ibmas-ingest-demo project)
#   --cpd-host HOST      CPD hostname              (default: from WXD_CPD_HOST)
#   --cpd-user USER      CPD username              (default: from WXD_CPD_USERNAME / cpadmin)
#   --cpd-password PASS  CPD password              (default: from WXD_CPD_PASSWORD)
#   --dsd                Also create a CPD DSD asset
#   --external-url H:P   Optionally register an external hostname:port in the DSD
#   --skip-postgres      Skip the PostgreSQL provisioning step
#   --skip-role          Skip the CPD role grant step
#   --skip-connection    Skip the CPD connection registration step
#   --dry-run            Print what would happen; change nothing
#   -h, --help           Show this help
# -----------------------------------------------------------------------------
set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
[[ -f "${REPO}/.env" ]] && set -a && source "${REPO}/.env" && set +a || true

# ── Defaults ──────────────────────────────────────────────────────────────────
NS="cpd-instance"
REPORT_DB="ibmas_reporting"
REPORT_SCHEMA="ibmas_reporting"
REPORT_USER="ibmas_reporting_user"
SECRET_NAME="ibmas-reporting-creds"
SVC="postgresql"
PROJECT_ID=""          # resolved at runtime if not set
CPD_HOST="${WXD_CPD_HOST:-}"
CPD_USER="${WXD_CPD_USERNAME:-cpadmin}"
CPD_PASS="${WXD_CPD_PASSWORD:-}"
DO_DSD=false
EXTERNAL_URL=""        # host:port for workstation access, e.g. localhost:15432
SKIP_PG=false
SKIP_ROLE=false
SKIP_CONN=false
DRY_RUN=false

# ── Colour helpers ────────────────────────────────────────────────────────────
BOLD="\033[1m"; RESET="\033[0m"; RED="\033[0;31m"; GREEN="\033[0;32m"
YELLOW="\033[1;33m"; CYAN="\033[0;36m"; DIM="\033[2m"
info()  { echo -e "${CYAN}[INFO]${RESET}  $*"; }
ok()    { echo -e "${GREEN}[OK]${RESET}    $*"; }
warn()  { echo -e "${YELLOW}[WARN]${RESET}  $*"; }
step()  { echo -e "\n${BOLD}── $* ──${RESET}"; }
die()   { echo -e "${RED}[ERROR]${RESET} $*" >&2; exit 1; }
dryrun(){ echo -e "${DIM}[DRY]${RESET}   $*"; }

# ── Argument parsing ──────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --namespace)      NS="$2";           shift 2 ;;
    --db)             REPORT_DB="$2";    shift 2 ;;
    --schema)         REPORT_SCHEMA="$2";shift 2 ;;
    --user)           REPORT_USER="$2";  shift 2 ;;
    --secret)         SECRET_NAME="$2";  shift 2 ;;
    --svc)            SVC="$2";          shift 2 ;;
    --project-id)     PROJECT_ID="$2";   shift 2 ;;
    --cpd-host)       CPD_HOST="$2";     shift 2 ;;
    --cpd-user)       CPD_USER="$2";     shift 2 ;;
    --cpd-password)   CPD_PASS="$2";     shift 2 ;;
    --dsd)            DO_DSD=true;       shift   ;;
    --external-url)   EXTERNAL_URL="$2"; shift 2 ;;
    --skip-postgres)  SKIP_PG=true;      shift   ;;
    --skip-role)      SKIP_ROLE=true;    shift   ;;
    --skip-connection)SKIP_CONN=true;    shift   ;;
    --dry-run)        DRY_RUN=true;      shift   ;;
    -h|--help) sed -n '18,60p' "${BASH_SOURCE[0]}" | sed 's/^#  \{0,1\}//'; exit 0 ;;
    *) die "Unknown option: $1  (try --help)" ;;
  esac
done

$DRY_RUN && warn "DRY-RUN mode — nothing will be changed.\n"

# ── Static values ─────────────────────────────────────────────────────────────
PG_HOST="${SVC}.${NS}.svc.cluster.local"
PG_PORT="5432"
PG_DATASOURCE_TYPE="e1c23729-99d8-4407-b3df-336e33ffdc82"   # PostgreSQL type UUID

# ── Pre-flight ────────────────────────────────────────────────────────────────
step "Pre-flight checks"

command -v oc &>/dev/null || die "'oc' not found — install it and run 'oc login'"
oc whoami &>/dev/null      || die "Not logged in — run: oc login <api-url>"
info "oc: $(oc whoami) on $(oc whoami --show-server)"

[[ -z "${CPD_HOST}" ]] && die "CPD host unknown. Set WXD_CPD_HOST in .env or pass --cpd-host."
info "CPD host: ${CPD_HOST}"

# ── Locate the postgresql pod ─────────────────────────────────────────────────
step "Locating postgresql pod (deploymentconfig=${SVC}) in ${NS}"
PG_POD="$(oc -n "${NS}" get pods \
  --field-selector=status.phase=Running \
  -l "deploymentconfig=${SVC}" \
  -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
[[ -z "${PG_POD}" ]] && die "No Running pod with label deploymentconfig=${SVC} in ${NS}"
ok "Pod: ${PG_POD}"

# ── Read superuser from pod env ───────────────────────────────────────────────
SU_USER="$(oc -n "${NS}" exec "${PG_POD}" -- bash -c 'echo -n "${POSTGRESQL_USER}"')"
info "Superuser: ${SU_USER}  (local trust — no password needed)"

# ── Generate clean alphanumeric password ──────────────────────────────────────
REPORT_PASS="$(LC_ALL=C tr -dc 'A-Za-z0-9' </dev/urandom 2>/dev/null | head -c 32 || true)"
[[ -z "${REPORT_PASS}" ]] && REPORT_PASS="$(openssl rand -hex 16)"

# ── Helper: run SQL via Unix socket (local trust) ─────────────────────────────
pg_sql() {
  local db="$1" sql="$2"
  oc -n "${NS}" exec -i "${PG_POD}" -- \
    psql -U "${SU_USER}" -d "${db}" -v ON_ERROR_STOP=1 <<< "${sql}" 2>&1
}

# ─────────────────────────────────────────────────────────────────────────────
# STEP 1 — PostgreSQL
# ─────────────────────────────────────────────────────────────────────────────
if ! $SKIP_PG; then
  step "Step 1/5 — PostgreSQL: user + database + schema"

  if $DRY_RUN; then
    dryrun "CREATE USER ${REPORT_USER} WITH PASSWORD '<generated>';"
    dryrun "CREATE DATABASE ${REPORT_DB} OWNER ${REPORT_USER};"
    dryrun "GRANT CONNECT ON DATABASE ${REPORT_DB} TO ${REPORT_USER};"
    dryrun "CREATE SCHEMA IF NOT EXISTS ${REPORT_SCHEMA} AUTHORIZATION ${REPORT_USER};"
  else
    pg_sql postgres "
DO \$\$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${REPORT_USER}') THEN
    EXECUTE format('CREATE USER ${REPORT_USER} WITH PASSWORD %L', '${REPORT_PASS}');
    RAISE NOTICE 'user created';
  ELSE
    EXECUTE format('ALTER  USER ${REPORT_USER} WITH PASSWORD %L', '${REPORT_PASS}');
    RAISE NOTICE 'user exists — password refreshed';
  END IF;
END \$\$;"
    ok "User '${REPORT_USER}' ready."

    pg_sql postgres "
SELECT 'CREATE DATABASE ${REPORT_DB} OWNER ${REPORT_USER}'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${REPORT_DB}') \gexec"
    pg_sql postgres "GRANT CONNECT ON DATABASE ${REPORT_DB} TO ${REPORT_USER};"
    ok "Database '${REPORT_DB}' ready."

    pg_sql "${REPORT_DB}" "
CREATE SCHEMA IF NOT EXISTS ${REPORT_SCHEMA} AUTHORIZATION ${REPORT_USER};
GRANT ALL ON SCHEMA ${REPORT_SCHEMA} TO ${REPORT_USER};
ALTER USER ${REPORT_USER} SET search_path TO ${REPORT_SCHEMA}, public;"
    ok "Schema '${REPORT_SCHEMA}' ready."

    # Smoke-test TCP
    SMOKE="$(oc -n "${NS}" exec -i "${PG_POD}" -- \
      bash -c "PGPASSWORD='${REPORT_PASS}' psql \
        -U '${REPORT_USER}' -d '${REPORT_DB}' -h 127.0.0.1 -p ${PG_PORT} \
        -t -c 'SELECT current_user,current_database(),current_schema()' 2>&1")"
    echo "${SMOKE}" | grep -q "${REPORT_USER}" \
      && ok "TCP smoke-test passed: $(echo "${SMOKE}" | tr -s ' ')" \
      || warn "TCP smoke-test output: ${SMOKE}"
  fi
fi

# ─────────────────────────────────────────────────────────────────────────────
# STEP 2 — Kubernetes Secret
# ─────────────────────────────────────────────────────────────────────────────
step "Step 2/5 — Kubernetes Secret '${SECRET_NAME}'"

JDBC_URI="jdbc:postgresql://${PG_HOST}:${PG_PORT}/${REPORT_DB}?user=${REPORT_USER}&password=${REPORT_PASS}"

if $DRY_RUN; then
  dryrun "oc create secret generic ${SECRET_NAME} --from-literal=... (7 keys) | oc apply"
else
  oc -n "${NS}" create secret generic "${SECRET_NAME}" \
    --from-literal=database="${REPORT_DB}" \
    --from-literal=schema="${REPORT_SCHEMA}" \
    --from-literal=username="${REPORT_USER}" \
    --from-literal=password="${REPORT_PASS}" \
    --from-literal=host="${PG_HOST}" \
    --from-literal=port="${PG_PORT}" \
    --from-literal=jdbc-uri="${JDBC_URI}" \
    --dry-run=client -o yaml | oc -n "${NS}" apply -f - >/dev/null
  ok "Secret '${NS}/${SECRET_NAME}' ready."

  # Always read back from Secret (source of truth)
  REPORT_PASS="$(oc -n "${NS}" get secret "${SECRET_NAME}" \
    -o jsonpath='{.data.password}' | base64 -d)"
fi

# ─────────────────────────────────────────────────────────────────────────────
# STEP 3 — CPD bearer token
# ─────────────────────────────────────────────────────────────────────────────
step "Step 3/5 — CPD authentication"

_cpd_token() {
  local auth_url="https://${CPD_HOST}/icp4d-api/v1/authorize"
  if [[ -n "${WXD_API_KEY:-}" ]]; then
    TOKEN="$(curl -sk -X POST "${auth_url}" \
      -H "Content-Type: application/json" \
      -d "{\"username\":\"${CPD_USER}\",\"api_key\":\"${WXD_API_KEY}\"}" \
      | python3 -c "import sys,json; print(json.load(sys.stdin)['token'])" 2>/dev/null || true)"
  fi
  if [[ -z "${TOKEN:-}" ]] && [[ -n "${CPD_PASS}" ]]; then
    TOKEN="$(curl -sk -X POST "${auth_url}" \
      -H "Content-Type: application/json" \
      -d "{\"username\":\"${CPD_USER}\",\"password\":\"${CPD_PASS}\"}" \
      | python3 -c "import sys,json; print(json.load(sys.stdin)['token'])" 2>/dev/null || true)"
  fi
  [[ -z "${TOKEN:-}" ]] && die \
    "Could not obtain CPD bearer token.\n" \
    "  Set WXD_API_KEY or --cpd-password in .env, or run:\n" \
    "    python scripts/get_token.py"
}

if ! $DRY_RUN; then
  _cpd_token
  info "Bearer token obtained (${#TOKEN} chars)."

  # Resolve project ID if not set
  if [[ -z "${PROJECT_ID}" ]]; then
    PROJECT_ID="$(curl -sk "https://${CPD_HOST}/v2/projects" \
      -H "Authorization: Bearer ${TOKEN}" \
      | python3 -c "
import sys,json
d=json.load(sys.stdin)
for p in d.get('resources',[]):
    if 'ingest-demo' in p.get('entity',{}).get('name','').lower() or \
       'ibmas-ingest' in p.get('entity',{}).get('name','').lower():
        print(p['metadata']['guid'])
        break
" 2>/dev/null || true)"
    [[ -z "${PROJECT_ID}" ]] && PROJECT_ID="2d2415ea-71b5-4215-a7b6-b32a4889611e"
    info "Project ID: ${PROJECT_ID}"
  fi
else
  TOKEN="dry-run-token"
  [[ -z "${PROJECT_ID}" ]] && PROJECT_ID="2d2415ea-71b5-4215-a7b6-b32a4889611e"
fi

# ─────────────────────────────────────────────────────────────────────────────
# STEP 3b — Grant wkc_reporting_administrator role to cpadmin
# ─────────────────────────────────────────────────────────────────────────────
if ! $SKIP_ROLE; then
  step "Step 3b/5 — Grant wkc_reporting_administrator role to ${CPD_USER}"

  if $DRY_RUN; then
    dryrun "PUT https://${CPD_HOST}/icp4d-api/v1/users/${CPD_USER}"
    dryrun '  {"user_roles":["zen_administrator_role","wkc_reporting_administrator"]}'
  else
    # Get current roles
    CURRENT_ROLES="$(curl -sk "https://${CPD_HOST}/icp4d-api/v1/users/${CPD_USER}" \
      -H "Authorization: Bearer ${TOKEN}" \
      | python3 -c "
import sys,json
d=json.load(sys.stdin)
ui = d.get('UserInfo', d)
roles = ui.get('user_roles', [])
# Add the reporting role if not present
if 'wkc_reporting_administrator' not in roles:
    roles.append('wkc_reporting_administrator')
print(json.dumps(roles))
" 2>/dev/null)"

    ROLE_RESP="$(curl -sk -w "\nHTTP:%{http_code}" \
      -X PUT "https://${CPD_HOST}/icp4d-api/v1/users/${CPD_USER}" \
      -H "Authorization: Bearer ${TOKEN}" \
      -H "Content-Type: application/json" \
      -d "{\"user_roles\": ${CURRENT_ROLES}}" 2>&1)"

    HTTP_CODE="$(echo "${ROLE_RESP}" | grep "HTTP:" | tail -1 | cut -d: -f2)"
    ROLE_BODY="$(echo "${ROLE_RESP}" | grep -v "HTTP:")"

    if [[ "${HTTP_CODE}" == "200" ]]; then
      ok "Role wkc_reporting_administrator granted to ${CPD_USER}."
    else
      warn "Role grant returned HTTP ${HTTP_CODE}: ${ROLE_BODY}"
      warn "You may need to grant this role manually in CPD → Manage → Access control → Users."
    fi
  fi
fi

# ─────────────────────────────────────────────────────────────────────────────
# STEP 4 — CPD Connection
# ─────────────────────────────────────────────────────────────────────────────
CONN_ID=""
if ! $SKIP_CONN; then
  step "Step 4/5 — CPD connection 'ibmas-reporting' in project ${PROJECT_ID}"

  CONN_PAYLOAD="{
    \"name\": \"ibmas-reporting\",
    \"description\": \"ibmas_reporting schema — standalone PostgreSQL (OpenShift DeploymentConfig), provisioned by provision_pg_reporting.sh\",
    \"datasource_type\": \"${PG_DATASOURCE_TYPE}\",
    \"origin_country\": \"us\",
    \"properties\": {
      \"host\":          \"${PG_HOST}\",
      \"port\":          \"${PG_PORT}\",
      \"database\":      \"${REPORT_DB}\",
      \"username\":      \"${REPORT_USER}\",
      \"password\":      \"${REPORT_PASS}\",
      \"ssl\":           \"false\",
      \"proxy\":         \"false\",
      \"query_timeout\": \"300\"
    }
  }"

  if $DRY_RUN; then
    dryrun "POST https://${CPD_HOST}/v2/connections?project_id=${PROJECT_ID}"
    dryrun "${CONN_PAYLOAD}"
  else
    CONN_RESP="$(curl -sk -X POST \
      "https://${CPD_HOST}/v2/connections?project_id=${PROJECT_ID}" \
      -H "Authorization: Bearer ${TOKEN}" \
      -H "Content-Type: application/json" \
      -d "${CONN_PAYLOAD}")"

    CONN_ID="$(echo "${CONN_RESP}" | python3 -c \
      "import sys,json; d=json.load(sys.stdin); print(d.get('metadata',{}).get('asset_id','ERROR'))" 2>/dev/null || true)"

    if [[ "${CONN_ID}" != "ERROR" ]] && [[ -n "${CONN_ID}" ]]; then
      ok "Connection registered: ${CONN_ID}"
    else
      MSG="$(echo "${CONN_RESP}" | python3 -c \
        "import sys,json; d=json.load(sys.stdin); e=d.get('errors',[{}]); print(e[0].get('message','') if e else d)" 2>/dev/null)"
      # If already exists (duplicate), extract existing ID
      if echo "${MSG}" | grep -qi "already exist\|duplicate\|conflict"; then
        warn "Connection already exists — looking up existing asset ID."
        CONN_ID="$(curl -sk \
          "https://${CPD_HOST}/v2/connections?project_id=${PROJECT_ID}" \
          -H "Authorization: Bearer ${TOKEN}" \
          | python3 -c "
import sys,json
d=json.load(sys.stdin)
for r in d.get('resources',[]):
    if r.get('entity',{}).get('name','') == 'ibmas-reporting':
        print(r['metadata']['asset_id'])
        break
" 2>/dev/null || true)"
        [[ -n "${CONN_ID}" ]] && ok "Existing connection: ${CONN_ID}" || warn "Could not resolve existing ID."
      else
        warn "Connection registration: ${MSG}"
      fi
    fi
  fi
fi

# ─────────────────────────────────────────────────────────────────────────────
# STEP 5 — Optional DSD
# ─────────────────────────────────────────────────────────────────────────────
DSD_ID=""
if $DO_DSD; then
  step "Step 5/5 — CPD Data Source Definition (DSD)"

  # Internal DSD
  DSD_PAYLOAD="{
    \"name\": \"IBMAS-Reporting-Postgres-DSD\",
    \"description\": \"Standalone PostgreSQL reporting instance — ibmas_reporting database\",
    \"datasource_type\": \"${PG_DATASOURCE_TYPE}\",
    \"origin_country\": \"us\",
    \"properties\": {
      \"host\": \"${PG_HOST}\",
      \"port\": \"${PG_PORT}\",
      \"database\": \"${REPORT_DB}\"
    }
  }"

  if $DRY_RUN; then
    dryrun "POST https://${CPD_HOST}/v2/datasource_definitions"
    dryrun "${DSD_PAYLOAD}"
    [[ -n "${EXTERNAL_URL}" ]] && \
      dryrun "Would also create external DSD with host=${EXTERNAL_URL%%:*} port=${EXTERNAL_URL##*:}"
  else
    DSD_RESP="$(curl -sk -X POST \
      "https://${CPD_HOST}/v2/datasource_definitions" \
      -H "Authorization: Bearer ${TOKEN}" \
      -H "Content-Type: application/json" \
      -d "${DSD_PAYLOAD}")"

    DSD_ID="$(echo "${DSD_RESP}" | python3 -c \
      "import sys,json; d=json.load(sys.stdin); print(d.get('metadata',{}).get('asset_id',d.get('id','ERROR')))" 2>/dev/null || true)"

    if [[ "${DSD_ID}" != "ERROR" ]] && [[ -n "${DSD_ID}" ]]; then
      ok "DSD registered: IBMAS-Reporting-Postgres-DSD (${DSD_ID})"
    else
      warn "DSD response: $(echo "${DSD_RESP}" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('errors',[{}])[0].get('message',''))" 2>/dev/null)"
    fi

    # External DSD if requested
    if [[ -n "${EXTERNAL_URL}" ]]; then
      EXT_HOST="${EXTERNAL_URL%%:*}"
      EXT_PORT="${EXTERNAL_URL##*:}"
      EXT_RESP="$(curl -sk -X POST \
        "https://${CPD_HOST}/v2/datasource_definitions" \
        -H "Authorization: Bearer ${TOKEN}" \
        -H "Content-Type: application/json" \
        -d "{
          \"name\": \"IBMAS-Reporting-Postgres-External-DSD\",
          \"description\": \"External / workstation access to ibmas_reporting (port-forward: oc -n ${NS} port-forward svc/${SVC} ${EXT_PORT}:5432)\",
          \"datasource_type\": \"${PG_DATASOURCE_TYPE}\",
          \"origin_country\": \"us\",
          \"properties\": {
            \"host\": \"${EXT_HOST}\",
            \"port\": \"${EXT_PORT}\",
            \"database\": \"${REPORT_DB}\"
          }
        }")"
      EXT_DSD_ID="$(echo "${EXT_RESP}" | python3 -c \
        "import sys,json; d=json.load(sys.stdin); print(d.get('metadata',{}).get('asset_id',d.get('id','ERROR')))" 2>/dev/null || true)"
      [[ "${EXT_DSD_ID}" != "ERROR" ]] && [[ -n "${EXT_DSD_ID}" ]] \
        && ok "External DSD registered: ${EXT_HOST}:${EXT_PORT} (${EXT_DSD_ID})" \
        || warn "External DSD response: ${EXT_RESP}"
    fi
  fi
fi

# ─────────────────────────────────────────────────────────────────────────────
# STEP 6 — Update .env
# ─────────────────────────────────────────────────────────────────────────────
step "Updating .env"
ENV_FILE="${REPO}/.env"
_set_env() {
  local k="$1" v="$2"
  if grep -q "^${k}=" "${ENV_FILE}" 2>/dev/null; then
    sed -i.bak "s|^${k}=.*|${k}=${v}|" "${ENV_FILE}" && rm -f "${ENV_FILE}.bak"
  else
    echo "${k}=${v}" >> "${ENV_FILE}"
  fi
}
if [[ -f "${ENV_FILE}" ]] && ! $DRY_RUN; then
  _set_env "PG_HOST"             "${PG_HOST}"
  _set_env "PG_PORT"             "${PG_PORT}"
  _set_env "PG_DATABASE"         "${REPORT_DB}"
  _set_env "PG_USER"             "${REPORT_USER}"
  _set_env "PG_PASSWORD"         "${REPORT_PASS}"
  _set_env "PG_SSL_MODE"         "disable"
  _set_env "PG_GOLD_SCHEMA"      "dbt_demo_gold"
  _set_env "PG_REPORTING_SCHEMA" "${REPORT_SCHEMA}"
  ok "PG_* vars written to .env"
else
  $DRY_RUN && dryrun ".env would be updated with PG_* vars" || warn ".env not found."
fi

# ─────────────────────────────────────────────────────────────────────────────
# SUMMARY
# ─────────────────────────────────────────────────────────────────────────────
W=74
line() { printf "${BOLD}║${RESET}  %-26s : %-${W}s${BOLD}║${RESET}\n" "$1" "$2"; }
echo
echo -e "${BOLD}╔$(printf '═%.0s' $(seq 1 $((W+33))))╗${RESET}"
echo -e "${BOLD}║$(printf ' %.0s' $(seq 1 17))ibmas_reporting — Connection Details$(printf ' %.0s' $(seq 1 21))║${RESET}"
echo -e "${BOLD}╠$(printf '═%.0s' $(seq 1 $((W+33))))╣${RESET}"
line "Host (in-cluster)"    "${PG_HOST}"
line "Port"                 "${PG_PORT}"
line "Database"             "${REPORT_DB}"
line "Schema"               "${REPORT_SCHEMA}"
line "User"                 "${REPORT_USER}"
line "Password"             "${REPORT_PASS}"
line "SSL"                  "disable (in-cluster plain TCP)"
line "K8s Secret"           "${NS}/${SECRET_NAME}"
[[ -n "${CONN_ID}" ]] && line "CPD Connection ID" "${CONN_ID}"
[[ -n "${DSD_ID}"  ]] && line "CPD DSD ID"        "${DSD_ID}"
echo -e "${BOLD}╠$(printf '═%.0s' $(seq 1 $((W+33))))╣${RESET}"
line "JDBC URL" "jdbc:postgresql://${PG_HOST}:${PG_PORT}/${REPORT_DB}"
echo -e "${BOLD}╚$(printf '═%.0s' $(seq 1 $((W+33))))╝${RESET}"

cat <<EOF

${BOLD}Retrieve password at any time:${RESET}
  oc -n ${NS} get secret ${SECRET_NAME} \\
    -o jsonpath='{.data.password}' | base64 -d && echo

${BOLD}Workstation access (port-forward):${RESET}
  oc -n ${NS} port-forward svc/${SVC} 15432:5432 &
  PG_HOST=localhost PG_PORT=15432 PG_SSL_MODE=disable \\
    .venv/bin/python scripts/pg_reporting.py list

${BOLD}Initialise and populate reporting tables:${RESET}
  .venv/bin/python scripts/pg_reporting.py init
  .venv/bin/python scripts/pg_reporting.py refresh
  .venv/bin/python scripts/pg_reporting.py list
  .venv/bin/python scripts/pg_reporting.py query \\
    "SELECT * FROM gold_reporting_customer_360 ORDER BY lifetime_value DESC LIMIT 5"

${BOLD}CPD project URL:${RESET}
  https://${CPD_HOST}/projects/${PROJECT_ID}/overview?context=icp4data

EOF
