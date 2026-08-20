#!/usr/bin/env bash
# -----------------------------------------------------------------------------
#  provision_pg_reporting.sh — full end-to-end setup of the ibmas_reporting
#                              PostgreSQL database with CPD integration.
#
#  Location  : scripts/provision_pg_reporting.sh
#  Repository: https://github.com/aseelert/ibmas-watsonxdata-dbt
#  Author    : Alexander Seelert — IBM Customer Success Engineer
#  Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.
#
#  Changelog :
#    v4.5 (2026-08-20) — --dsd was dead code: it POSTed to /v2/datasource_definitions,
#                        confirmed live to be a routing-level 404 ("Error 404 -
#                        Not Found", plain text, not even a CAMS JSON error) —
#                        that path is not wired to any backend service on this
#                        cluster at all. A CPD Data Source Definition is not a
#                        dedicated REST resource; it's a generic catalog asset
#                        of type 'ibm_data_source' (confirmed via
#                        GET /v2/asset_types/ibm_data_source, and a live example
#                        found already in the Platform assets catalog, an
#                        auto-created DSD named "IBM Software Hub"). Its
#                        scope_restrictions lock it to the Platform assets
#                        catalog only — never a project or another catalog.
#                        STEP 5 now POSTs to /v2/assets?catalog_id=<platform
#                        catalog>, with entity.ibm_data_source.data_source_type_id
#                        (the SAME datasource-type UUID Step 4 already resolves
#                        — reused, not re-resolved) and
#                        entity.ibm_data_source.data_source_endpoints.values=
#                        [{host, port}] (port is numeric, not a string; DSDs
#                        have no 'database' field — that stays on the connection
#                        asset). New _resolve_platform_catalog() resolves the
#                        Platform assets catalog GUID by name (override via
#                        --platform-catalog-id). New --platform-connection flag
#                        (opt-in, off by default) registers 'ibmas-reporting' as
#                        a second, CATALOG-scoped connection in that same
#                        Platform assets catalog — alongside, not instead of,
#                        the project-scoped one from Step 4 — so other
#                        projects/catalogs can reuse it later via a connection
#                        copy instead of re-entering credentials. Not yet write-
#                        verified against this exact cluster; run --dry-run
#                        first if in doubt.
#    v4.4 (2026-08-20) — Two more real-cluster corrections found by probing a
#                        live CPD 5.3 build end-to-end with password auth:
#                        • project create sent "tags": [] — this build rejects
#                          that with HTTP 400 "The value of entity.tags field
#                          must contain at least one tag." Default tags is now
#                          a real one-element list, not empty.
#                        • _resolve_datasource_type requested limit=200, which
#                          this build's /v2/datasource_types caps at 100 (flat
#                          HTTP 400 "bad_paging_range"), so the lookup always
#                          fell through to the name fallback. Worse: even at
#                          limit=100 the generic "postgresql" entry
#                          (e1c23729-99d8-4407-b3df-336e33ffdc82 — the exact
#                          GUID v4.2 removed as unverifiable, confirmed real)
#                          sits on page 2 of 155 entries, and entity.name=…
#                          is silently ignored as a filter on this endpoint.
#                          The lookup now paginates for real (limit=100,
#                          offset stepped, 1000-entry safety cap) until it
#                          finds an exact match or runs out of pages.
#                        • project create's "generator" value
#                          ("provision_pg_reporting.sh") failed live: this
#                          build enforces >= 8 chars and rejects '.'/'_'.
#                          Confirmed by direct probing (create-then-delete a
#                          throwaway project) which values pass; changed to
#                          "provision-pg-reporting".
#                        • the printed "Workstation access" hint told users to
#                          'oc port-forward svc/<cluster>-primary' — confirmed
#                          live to fail outright ("Service is defined without
#                          a selector"): Crunchy's *-primary Service is bound
#                          via a hand-managed Endpoints object that follows
#                          the Patroni leader, not a label selector, and
#                          port-forward needs the latter. Now targets
#                          pod/${PG_POD} instead, which does work.
#                        • scripts/get_token.py wrote WXD_API_KEY /
#                          WXD_SPARK_BEARER_TOKEN via python-dotenv's
#                          set_key(), whose default quote_mode="always" wraps
#                          the value in literal single quotes. Every reader in
#                          this repo (this script's own env loader included)
#                          only trims whitespace, not quotes — so the bearer-
#                          token auth path (method 3 of 3) silently sent
#                          "Bearer '<token>'" and CPD rejected it as
#                          malformed. Fixed at the source: both set_key()
#                          calls now pass quote_mode="never", matching every
#                          other unquoted value in .env. All three auth
#                          methods (oc session reuse, cpadmin+password,
#                          bearer token) now verified working end-to-end
#                          against a live cluster.
#    v4.3 (2026-08-19) — Made the CPD steps survive a real 5.3 cluster:
#                        • project create no longer sends verify_unique_name,
#                          which CPD 5.3 rejects with HTTP 400 "Extraneous
#                          properties". The create call is now self-healing: it
#                          parses the rejected key names out of the 400 body,
#                          strips them, and retries (max 4 attempts), so a
#                          version-specific schema difference no longer dead-ends
#                          the run.
#                        • _lookup_project / _create_project return through a
#                          global instead of stdout. Captured with $( ) they ran
#                          in a subshell, so _CURL_HTTP/_CURL_BODY were lost and
#                          the failure message printed an empty status and body.
#                        • the role grant NO LONGER sends a fabricated role list.
#                          PUT user_roles is a full replacement; on a single-admin
#                          cluster the old fallback would have stripped cpadmin's
#                          zen_administrator_role. It now skips with manual
#                          instructions when the current roles cannot be read,
#                          and reads back the result when it can.
#                        • connection payload built with json.dumps (a password
#                          read from an existing Secret could break hand-rolled
#                          JSON), and a bare HTTP 409 is now recognised as
#                          "already exists".
#                        • new read-only deployment verification of watsonx.data
#                          and IKC (--verify-only / --skip-verify).
#    v4.2 (2026-08-19) — Correctness pass against real Crunchy PGO v5 naming:
#                        • primary pod label is role=master (Patroni's leader
#                          value), NOT role=primary — v4.1 could never match a
#                          pod and always timed out after 3 min.
#                        • read-write Service is <cluster>-primary, NOT
#                          <cluster>-rw (that is CloudNativePG/EDB naming).
#                        • PGO's pg_hba.conf permits hostssl only, so TCP
#                          connections need sslmode=require, not disable.
#                        • oc exec now targets the 'database' container.
#                        • pod readiness read from the pod Ready condition.
#                        • operator Deployment (not just CRD) is verified.
#                        • auto-diagnostics dumped on wait timeout.
#                        • oc login fallback from .env; venv python; bearer
#                          token accepted as a third CPD auth method.
#                        • the target CPD project is created when missing, via
#                          POST /transactional/v2/projects — the contract was
#                          taken from `cpdctl project create --help` (operation
#                          transactional_post_project), not guessed.
#                        • all log output moved to stderr: helpers that return a
#                          value through $( ) were capturing their own [DEBUG]
#                          lines into it.
#    v4.1 (2026-07-02) — Hardened: cpd_curl helper, password desync,
#                        SQL error capture, debug mode, macOS sed,
#                        CURRENT_ROLES guard. (Introduced the role=primary
#                        and -rw regressions fixed in v4.2.)
#    v4.0 (2026-07-01) — Deploy via Crunchy Postgres for Kubernetes operator
#                        (PostgresCluster CR) instead of legacy DeploymentConfig.
#    v3.0 (2026-06-29) — Complete rewrite. Single script covers all steps.
# -----------------------------------------------------------------------------
#
# WHAT THIS SCRIPT DOES (in order)
#  -1. Verify     — read-only health check of the CPD namespace: Deployments and
#                    StatefulSets fully Ready, no pods stuck outside
#                    Running/Completed, and every discovered watsonx.data / IKC
#                    operand CR reporting Completed (or Ready=True). The operand
#                    CR kinds are DISCOVERED from the API groups, not hardcoded,
#                    because they differ across CPD versions. Never fatal on its
#                    own — see --skip-verify / --verify-only.
#   0. Cluster    — creates a single-instance Crunchy PostgresCluster CR named
#                    ibmas-reporting in the cpd-instance namespace (idempotent).
#                    Waits for the Patroni leader pod to become Ready
#                    (--wait-minutes, default 6), then dumps full diagnostics
#                    (pods, PVCs, StorageClass, events) if it never does.
#   1. PostgreSQL  — creates database ibmas_reporting, user ibmas_reporting_user,
#                    and schema ibmas_reporting. Connects via Unix-socket peer
#                    auth as the postgres superuser inside the primary pod.
#                    Smoke-tests TCP auth.
#   2. K8s Secret  — stores all credentials in  ibmas-reporting-creds  (namespace
#                    cpd-instance). Idempotent (create-or-update via apply).
#   3. CPD role    — grants  wkc_reporting_administrator  to cpadmin via the
#                    CPD icp4d-api so the reporting UI and MCP tools stop
#                    returning IKCBI2019E.
#   4. CPD connection — registers  ibmas-reporting  as a plain PostgreSQL data
#                    connection in the target project via the CPD connections
#                    REST API. The project is looked up by name and CREATED if
#                    it does not exist (see --project-name).
#   5. DSD (optional, --dsd) — creates an IBMAS-Reporting-Postgres-DSD entry.
#                    Pass --external-url to also register an external hostname
#                    (e.g. for workstation port-forward access).
#   6. Summary     — prints all connection details and next-step commands.
#
# PREREQUISITES
#   • oc CLI installed. A live session is preferred, but if 'oc whoami' fails the
#     script logs in automatically from .env using either
#        WXD_OPENSHIFT_TOKEN                       (token login), or
#        WXD_OPENSHIFT_API + WXD_OC_USER + WXD_OC_PASSWORD   (password login).
#   • Python: the repo venv (.venv/bin/python) is used when present, else python3.
#   • Crunchy Postgres for Kubernetes operator installed AND running, with the
#     target namespace in its watch scope.
#   • CPD auth — the script tries these in order and uses the first that works:
#        1. WXD_API_KEY          + WXD_CPD_USERNAME   (api_key grant)
#        2. WXD_CPD_PASSWORD     + WXD_CPD_USERNAME   (password grant)
#        3. WXD_SPARK_BEARER_TOKEN                    (pre-existing bearer token)
#     Or pass --cpd-host / --cpd-user / --cpd-password / --cpd-token explicitly.
#
# AUTHENTICATION & API ORDER
#   Two independent credentials are needed, in this order. Skipping ahead is the
#   usual cause of confusing failures, because a missing oc session surfaces
#   several steps later as an unrelated error.
#
#     1. oc session (Kubernetes API)  — required for steps 0/1/2 and for the
#        deployment verification. Cluster-admin or namespace-admin on the CPD
#        namespace. Comes from WXD_OPENSHIFT_TOKEN, or
#        WXD_OPENSHIFT_API + WXD_OC_USER + WXD_OC_PASSWORD (kubeadmin works).
#        This is NOT the same identity as the CPD user below: oc talks to
#        OpenShift, CPD talks to its own user registry.
#
#     2. CPD bearer token (Software Hub API) — required for steps 3/4/5, minted
#        by POST /icp4d-api/v1/authorize from an API key, a password, or reused
#        from WXD_SPARK_BEARER_TOKEN. Tokens expire; a stale one in .env is
#        probed and rejected up front rather than failing later. Refresh with:
#            .venv/bin/python scripts/get_token.py --export
#
#   With the token in hand, the CPD calls happen in dependency order:
#        GET  /v2/projects?limit=100          → find the project by name
#        POST /transactional/v2/projects      → create it if absent
#        GET  /v2/projects/<guid>             → confirm it is readable
#        GET  /icp4d-api/v1/users/<user>      → read current roles
#        PUT  /icp4d-api/v1/users/<user>      → append the reporting role
#        POST /v2/connections?project_id=…    → register the PG connection
#        POST /v2/datasource_definitions      → optional DSD (--dsd)
#   A connection cannot be created before the project exists, which is why
#   project resolution/creation runs before step 4 rather than inside it.
#
# NOTE ON TLS
#   Crunchy PGO generates a pg_hba.conf that permits regular users over
#   'hostssl' only — plain TCP is rejected. Every connection this script
#   registers therefore uses sslmode=require (encrypted, but the PGO-internal
#   CA is not verified). Do not set PG_SSL_MODE=disable for this cluster.
#
# USAGE
#   bash scripts/provision_pg_reporting.sh
#   bash scripts/provision_pg_reporting.sh --dry-run
#   bash scripts/provision_pg_reporting.sh --dsd
#   bash scripts/provision_pg_reporting.sh --dsd --external-url myhost.example.com:15432
#   bash scripts/provision_pg_reporting.sh --cpd-user cpadmin --cpd-password secret
#   bash scripts/provision_pg_reporting.sh --verbose
#
# OPTIONS
#   --namespace NS       OpenShift namespace      (default: cpd-instance)
#   --db DB              Reporting database        (default: ibmas_reporting)
#   --schema SCHEMA      Schema inside database    (default: ibmas_reporting)
#   --user USER          Reporting PG user         (default: ibmas_reporting_user)
#   --secret NAME        K8s Secret name           (default: ibmas-reporting-creds)
#   --cluster NAME       Crunchy cluster name      (default: ibmas-reporting)
#   --pg-storage SIZE    PVC size for PG data      (default: 5Gi)
#   --pg-version VER     PostgreSQL major version  (default: 16)
#   --storage-class SC   StorageClass for the PVCs (default: cluster default)
#   --svc NAME           Override the RW Service   (default: auto-detect)
#   --wait-minutes N     Primary-pod wait timeout  (default: 6)
#   --project-id ID      CPD project GUID          (default: resolved by name)
#   --project-name NAME  CPD project to use        (default: ibmas-ingest-demo)
#                        Looked up by name; CREATED automatically if it does not
#                        exist. Override the default via WXD_CPD_PROJECT in .env.
#   --no-create-project  Fail instead of creating a missing project
#   --allow-fuzzy-project
#                        Accept a project whose name only RESEMBLES the requested
#                        one. Off by default: adopting a near match can attach the
#                        reporting connection to the wrong project.
#   --grant-role         Actually write wkc_reporting_administrator to the CPD
#                        user. Off by default: PUT /icp4d-api/v1/users/<user>
#                        REPLACES the whole role list and cpadmin is usually the
#                        only admin, so Step 3b just reports what is held.
#   --cpd-host HOST      CPD hostname              (default: from WXD_CPD_HOST)
#   --cpd-user USER      CPD username              (default: from WXD_CPD_USERNAME / cpadmin)
#   --cpd-password PASS  CPD password              (default: from WXD_CPD_PASSWORD)
#   --cpd-token TOKEN    CPD bearer token          (default: from WXD_SPARK_BEARER_TOKEN)
#   --platform-connection Also register 'ibmas-reporting' as a CATALOG-scoped
#                        ("platform") connection in the Platform assets catalog,
#                        in addition to the project-scoped one from Step 4. This
#                        is what lets other projects/catalogs reuse it later via
#                        a connection COPY, instead of re-entering credentials.
#   --platform-catalog-id ID
#                        Platform assets catalog GUID (default: resolved by name,
#                        matching "platform assets catalog" case-insensitively)
#   --dsd                Also create a CPD DSD asset (an 'ibm_data_source' asset
#                        in the Platform assets catalog — DSDs are scope-locked
#                        there, they cannot live in a project or other catalog)
#   --external-url H:P   Optionally register an external hostname:port in the DSD
#   --skip-cluster       Skip the PostgresCluster CR creation step
#   --skip-postgres      Skip the PostgreSQL provisioning step (DB/user/schema)
#   --skip-role          Skip the CPD role grant step
#   --skip-connection    Skip the CPD connection registration step
#   --skip-verify        Skip the read-only watsonx.data / IKC deployment check
#   --verify-only        Run ONLY the deployment check, then exit (0 = healthy)
#   --dry-run            Print what would happen; change nothing
#   --verbose            Emit [DEBUG] lines for each major operation
#   -h, --help           Show this help
# -----------------------------------------------------------------------------
set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Load .env the same way prepare_watsonx_env.py does:
#   • skip blank lines and full-line comments
#   • strip inline comments (only when # is preceded by whitespace)
#   • export each key=value safely without eval
_load_env() {
  local envfile="$1"
  [[ -f "${envfile}" ]] || return 0
  while IFS= read -r line || [[ -n "${line}" ]]; do
    # skip blank lines and comment lines
    [[ -z "${line}" || "${line}" =~ ^\s*# ]] && continue
    # must contain '='
    [[ "${line}" != *=* ]] && continue
    local key="${line%%=*}"
    local val="${line#*=}"
    # strip trailing inline comment: only when preceded by whitespace (mirrors Python re.sub(r'\s+#.*$','',value))
    val="$(printf '%s' "${val}" | sed 's/[[:blank:]]\{1,\}#.*$//')"
    # strip surrounding whitespace
    val="${val#"${val%%[! ]*}"}"
    val="${val%"${val##*[! ]}"}"
    export "${key}=${val}"
  done < "${envfile}"
}
_load_env "${REPO}/.env"

# ── Defaults ──────────────────────────────────────────────────────────────────
NS="cpd-instance"
REPORT_DB="ibmas_reporting"
REPORT_SCHEMA="ibmas_reporting"
REPORT_USER="ibmas_reporting_user"
SECRET_NAME="ibmas-reporting-creds"
CLUSTER_NAME="ibmas-reporting"
PG_STORAGE="5Gi"
PG_VERSION="16"
STORAGE_CLASS=""       # empty → use the cluster default StorageClass
PG_SVC_OVERRIDE=""     # empty → auto-detect the Crunchy RW Service
WAIT_MINUTES="6"       # primary pod readiness timeout
PROJECT_ID=""          # resolved at runtime if not set
PROJECT_NAME="${WXD_CPD_PROJECT:-ibmas-ingest-demo}"   # looked up, created if absent
NO_CREATE_PROJECT=false                                 # true → fail instead of creating
CPD_HOST="${WXD_CPD_HOST:-}"
CPD_USER="${WXD_CPD_USERNAME:-cpadmin}"
CPD_PASS="${WXD_CPD_PASSWORD:-}"
CPD_TOKEN="${WXD_SPARK_BEARER_TOKEN:-}"
DO_DSD=false
DO_PLATFORM_CONN=false
PLATFORM_CATALOG_ID=""   # resolved at runtime if not set (see _resolve_platform_catalog)
EXTERNAL_URL=""        # host:port for workstation access, e.g. localhost:15432
SKIP_CLUSTER=false
SKIP_PG=false
SKIP_ROLE=false
SKIP_CONN=false
DRY_RUN=false
# Deployment verification runs by default — it is read-only and its findings are
# the first thing you need when a later CPD API call fails. --skip-verify opts
# out; --verify-only runs it and nothing else.
DO_VERIFY=true
VERIFY_ONLY=false
# Adopting a project whose name only *resembles* the requested one can silently
# attach the reporting connection to the wrong project. Opt-in only.
ALLOW_FUZZY_PROJECT=false
# Step 3b writes to the sole admin's role list. PUT /icp4d-api/v1/users/<user>
# is a full replacement and its body contract has no primary source, so the
# write is opt-in; by default Step 3b only reports what the user already holds.
GRANT_ROLE=false
# [ST-6] verbose mode default
VERBOSE=false

# ── Colour helpers ────────────────────────────────────────────────────────────
# $'…' (ANSI-C quoting) puts a real ESC byte in the variable rather than the
# four characters \033. Needed because the closing summary interpolates these
# into a plain `cat <<EOF` here-doc, which does no escape interpretation.
BOLD=$'\033[1m'; RESET=$'\033[0m'; RED=$'\033[0;31m'; GREEN=$'\033[0;32m'
YELLOW=$'\033[1;33m'; CYAN=$'\033[0;36m'; DIM=$'\033[2m'
#
# All log helpers write to **stderr**, never stdout. Several helper functions
# (cpd_curl, _lookup_project, _create_project …) return their value on stdout
# via $( ) — if logs went to stdout too, every captured value would come back
# with the log lines glued onto it. Learned the hard way: PROJECT_ID once ended
# up containing a whole JSON body plus three [DEBUG] lines.
info()  { echo -e "${CYAN}[INFO]${RESET}  $*" >&2; }
ok()    { echo -e "${GREEN}[OK]${RESET}    $*" >&2; }
warn()  { echo -e "${YELLOW}[WARN]${RESET}  $*" >&2; }
step()  { echo -e "\n${BOLD}── $* ──${RESET}" >&2; }
dryrun(){ echo -e "${DIM}[DRY]${RESET}   $*" >&2; }

# [ST-6] Fixed die(): use printf so \n escapes render as real newlines even when
# called with multiple arguments (old $* joined with spaces, breaking \n).
die() {
  local msg
  msg="$(printf '%s' "$*" | sed 's/\\n/\n/g')"
  echo -e "${RED}[ERROR]${RESET} ${msg}" >&2
  exit 1
}

# [ST-6] debug(): emits only when --verbose is set
debug() { $VERBOSE && echo -e "${DIM}[DEBUG]${RESET} $*" >&2 || true; }

# ── Argument parsing ──────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --namespace)      NS="$2";            shift 2 ;;
    --db)             REPORT_DB="$2";     shift 2 ;;
    --schema)         REPORT_SCHEMA="$2"; shift 2 ;;
    --user)           REPORT_USER="$2";   shift 2 ;;
    --secret)         SECRET_NAME="$2";   shift 2 ;;
    --cluster)        CLUSTER_NAME="$2";  shift 2 ;;
    --pg-storage)     PG_STORAGE="$2";    shift 2 ;;
    --pg-version)     PG_VERSION="$2";    shift 2 ;;
    --storage-class)  STORAGE_CLASS="$2"; shift 2 ;;
    --svc)            PG_SVC_OVERRIDE="$2"; shift 2 ;;
    --wait-minutes)   WAIT_MINUTES="$2";  shift 2 ;;
    --project-id)     PROJECT_ID="$2";    shift 2 ;;
    --project-name)   PROJECT_NAME="$2";  shift 2 ;;
    --no-create-project) NO_CREATE_PROJECT=true; shift ;;
    --allow-fuzzy-project) ALLOW_FUZZY_PROJECT=true; shift ;;
    --grant-role)     GRANT_ROLE=true;    shift   ;;
    --cpd-host)       CPD_HOST="$2";      shift 2 ;;
    --cpd-user)       CPD_USER="$2";      shift 2 ;;
    --cpd-password)   CPD_PASS="$2";      shift 2 ;;
    --cpd-token)      CPD_TOKEN="$2";     shift 2 ;;
    --dsd)            DO_DSD=true;        shift   ;;
    --platform-connection) DO_PLATFORM_CONN=true; shift ;;
    --platform-catalog-id) PLATFORM_CATALOG_ID="$2"; shift 2 ;;
    --external-url)   EXTERNAL_URL="$2";  shift 2 ;;
    --skip-cluster)   SKIP_CLUSTER=true;  shift   ;;
    --skip-postgres)  SKIP_PG=true;       shift   ;;
    --skip-role)      SKIP_ROLE=true;     shift   ;;
    --skip-connection)SKIP_CONN=true;     shift   ;;
    --skip-verify)    DO_VERIFY=false;    shift   ;;
    --verify-only)    VERIFY_ONLY=true;   shift   ;;
    --dry-run)        DRY_RUN=true;       shift   ;;
    --verbose)        VERBOSE=true;       shift   ;;
    # Print the banner between 'WHAT THIS SCRIPT DOES' and the 'set -euo' line.
    # Marker-based so the help text can never drift out of sync with edits.
    -h|--help)
      sed -n '/^# WHAT THIS SCRIPT DOES/,/^# ---*$/p' "${BASH_SOURCE[0]}" \
        | sed '$d' | sed -e 's/^#$//' -e 's/^#  \{0,1\}//'
      exit 0 ;;
    *) die "Unknown option: $1  (try --help)" ;;
  esac
done

$DRY_RUN && warn "DRY-RUN mode — nothing will be changed.\n"
$VERBOSE  && info "Verbose / debug mode enabled."

# ── Static values ─────────────────────────────────────────────────────────────
# Crunchy PGO v5 Service naming (internal/naming/names.go):
#   <cluster>-primary   → read-write, always follows the current Patroni leader
#   <cluster>-replicas  → read-only replicas
#   <cluster>-pods      → headless, per-pod DNS identity
#   <cluster>-ha        → Patroni scope / leader-election Endpoints (not for apps)
# There is NO <cluster>-rw service — that is CloudNativePG (EDB) naming, which
# v4.1 wrongly assumed. Resolved for real against the cluster further below.
PG_SVC="${PG_SVC_OVERRIDE:-${CLUSTER_NAME}-primary}"
PG_HOST="${PG_SVC}.${NS}.svc.cluster.local"
PG_PORT="5432"
# The datasource type is RESOLVED AT RUNTIME (see _resolve_datasource_type).
# The GUID that used to be hardcoded here — e1c23729-99d8-4407-b3df-336e33ffdc82
# — has no traceable source: it appears nowhere except this script, and three
# known-real type GUIDs taken from actual CPD exports appear nowhere in cpdctl's
# string table either, so "it's in the binary" proves nothing. Rather than ship
# an unverifiable constant, ask the cluster. "postgresql" is the fallback the
# API also accepts in place of an id ("The id or the name of the data source
# type to connect to. For example cfdcb449-… or db2").
PG_DATASOURCE_NAME="postgresql"
PG_DATASOURCE_TYPE=""     # filled in by _resolve_datasource_type

# PGO's generated pg_hba.conf permits regular users over 'hostssl' ONLY, so all
# TCP clients must negotiate TLS. 'require' encrypts without verifying PGO's
# internal CA — the right choice for in-cluster demo traffic.
PG_SSL_MODE="require"

# Patroni labels its current leader pod with role=master. PGO's RolePrimary
# ("primary") is applied to the Service, never to a pod — selecting on it always
# returns nothing. Later PGO/Patroni combinations may use "leader", so try all
# three in order and report which one matched.
LEADER_ROLE_VALUES=(master primary leader)
LABEL_CLUSTER="postgres-operator.crunchydata.com/cluster"
LABEL_ROLE="postgres-operator.crunchydata.com/role"
LABEL_DATA="postgres-operator.crunchydata.com/data"
PG_CONTAINER="database"   # naming.ContainerDatabase — the PostgreSQL container
# ── Pre-flight ────────────────────────────────────────────────────────────────
step "Pre-flight checks"

command -v oc &>/dev/null || die "'oc' not found — install it and run 'oc login'"

# ── oc session: reuse an existing one, else log in from .env ──────────────────
# Two .env paths are supported, mirroring scripts/reduce_postgres_cpu.sh:
#   WXD_OPENSHIFT_TOKEN                                → token login
#   WXD_OPENSHIFT_API + WXD_OC_USER + WXD_OC_PASSWORD  → password login
_oc_login() {
  local api="${WXD_OPENSHIFT_API:-}"
  local tok="${WXD_OPENSHIFT_TOKEN:-}"
  local ocu="${WXD_OC_USER:-kubeadmin}"
  local ocp="${WXD_OC_PASSWORD:-}"

  [[ -z "${api}" ]] && return 1

  if [[ -n "${tok}" ]]; then
    info "  Trying token login against ${api} …"
    if oc login "${api}" --token="${tok}" --insecure-skip-tls-verify >/dev/null 2>&1; then
      debug "  Token login succeeded."
      return 0
    fi
    warn "  Token login failed (WXD_OPENSHIFT_TOKEN may be expired)."
  fi

  if [[ -n "${ocp}" ]]; then
    info "  Trying password login as '${ocu}' against ${api} …"
    if oc login "${api}" -u "${ocu}" -p "${ocp}" \
         --insecure-skip-tls-verify >/dev/null 2>&1; then
      debug "  Password login succeeded."
      return 0
    fi
    warn "  Password login as '${ocu}' failed."
  fi
  return 1
}

if ! oc whoami &>/dev/null; then
  warn "No active oc session — attempting login from .env …"
  _oc_login || die "Could not establish an oc session.\n\
  Set one of the following in .env, or run 'oc login' manually:\n\
    WXD_OPENSHIFT_TOKEN=<token from the OCP console 'Copy login command'>\n\
    WXD_OPENSHIFT_API + WXD_OC_USER + WXD_OC_PASSWORD\n\
  Current values: WXD_OPENSHIFT_API='${WXD_OPENSHIFT_API:-<unset>}' \
WXD_OC_USER='${WXD_OC_USER:-<unset>}' \
WXD_OC_PASSWORD=$([[ -n "${WXD_OC_PASSWORD:-}" ]] && echo '<set>' || echo '<unset>')"
fi
info "oc: $(oc whoami) on $(oc whoami --show-server)"

# ── Python: prefer the repo venv so JSON parsing matches the rest of the repo ──
if [[ -x "${REPO}/.venv/bin/python" ]]; then
  PY="${REPO}/.venv/bin/python"
  debug "python: ${PY} ($("${PY}" --version 2>&1)) — repo venv"
elif command -v python3 &>/dev/null; then
  PY="python3"
  warn "Repo venv not found at ${REPO}/.venv — falling back to system python3."
  warn "  Create it with: python3.11 -m venv .venv && pip install -r requirements.txt"
  debug "python: $(python3 --version 2>&1)"
else
  die "No Python interpreter found.\n\
  Expected the repo venv at ${REPO}/.venv/bin/python, or python3 on PATH.\n\
  Create the venv with:\n\
    python3.11 -m venv .venv && .venv/bin/pip install -r requirements.txt"
fi

# Confirm the namespace exists before anything tries to write into it
oc get namespace "${NS}" &>/dev/null \
  || die "Namespace '${NS}' not found (or not visible to $(oc whoami)).\n\
  List candidates with: oc get ns | grep cpd\n\
  Override with: --namespace <ns>"
debug "Namespace '${NS}' confirmed."

[[ -z "${CPD_HOST}" ]] && die "CPD host unknown. Set WXD_CPD_HOST in .env or pass --cpd-host."
info "CPD host: ${CPD_HOST}"

# ── Deployment verification ───────────────────────────────────────────────────
# Confirms watsonx.data / IKC are actually up before we start creating assets in
# them. Everything here is DISCOVERED, not hardcoded: the operand CR kinds differ
# between CPD versions (and this repo has no verified list), so guessing a kind
# name would produce false "not installed" alarms. Instead we enumerate the CPD
# operator API groups and read whatever *Status fields each CR exposes.
#
# Never fatal on its own — a degraded add-on may be irrelevant to this script's
# work. Use --verify-only to run just these checks (exit 1 if anything is amiss).
#
# Workload health is NOT reimplemented here: scripts/lib/readiness.sh already
# does it properly (four namespaces, fail-closed session probe, Job-owned pods
# excluded, TERMINATING-vs-STUCK classification, Deployment observedGeneration,
# the StatefulSet-OnDelete caveat, EDB CloudNativePG and FoundationDB clusters).
# We call it instead of guessing. What it does NOT cover — the CPD operand CRs —
# is what the scan further down adds.
_verify_workloads() {
  local lib="${REPO}/scripts/lib/readiness.sh"

  if [[ -r "${lib}" ]]; then
    debug "  Delegating workload checks to scripts/lib/readiness.sh"
    # Deliberately a child process, not `source`: readiness.sh pulls in
    # lib/log.sh, which defines its own info/warn/step. Sourcing would silently
    # replace this script's helpers — and those must write to stderr, because
    # sibling helpers return values through $( ). A subprocess keeps both
    # namespaces intact. Its stdout goes to stderr so this script's own stdout
    # stays clean (`2>&1 >&2` would *swap* the two streams, not merge them).
    if NAMESPACE="${NS}" bash -c 'source "$1"; verify_ready' _ "${lib}" >&2; then
      return 0
    fi
    return 1
  fi

  warn "  ${lib#"${REPO}/"} not found — falling back to a basic inline check."
  _verify_workloads_inline
}

# Fallback only. Weaker than readiness.sh on purpose-built edge cases (it cannot
# tell a rollout-surge Terminating pod from a stuck one, and checks just ${NS}).
_verify_workloads_inline() {
  local problems=0

  # 1. Workloads. `oc get deploy` READY column is "n/m"; anything where n<m or
  #    n=0 is not serving. StatefulSets are checked the same way.
  local not_ready
  for kind in deployment statefulset; do
    not_ready="$(oc -n "${NS}" get "${kind}" --no-headers 2>/dev/null \
      | awk '{split($2,a,"/"); if (a[1]+0 != a[2]+0) print "    " $1 "  READY=" $2}' || true)"
    if [[ -n "${not_ready}" ]]; then
      warn "  ${kind}s not fully ready:"
      echo "${not_ready}" >&2
      problems=$((problems + 1))
    else
      ok "  All ${kind}s ready."
    fi
  done

  # 2. Pods that are neither Running-and-Ready nor Completed. Jobs legitimately
  #    end as Completed, so they are excluded rather than reported as failures.
  local bad_pods
  bad_pods="$(oc -n "${NS}" get pods --no-headers 2>/dev/null | awk '
    $3 != "Completed" {
      split($2, a, "/")
      if (a[1]+0 != a[2]+0 || $3 != "Running") print "    " $1 "  " $2 "  " $3
    }' || true)"
  if [[ -n "${bad_pods}" ]]; then
    warn "  Pods not Running/Ready:"
    echo "${bad_pods}" | head -20 >&2
    problems=$((problems + 1))
  else
    ok "  All pods Running and Ready."
  fi

  return $(( problems > 0 ? 1 : 0 ))
}

_verify_deployment() {
  step "Step -1/5 — Deployment verification (${NS})"
  local problems=0

  # 1. Workloads (Deployments, StatefulSets, pods) — delegated.
  _verify_workloads || problems=$((problems + 1))

  # 2. CPD operand CRs. Reconciliation state lives in fields named like
  #    zenStatus / wkcStatus / ccsStatus / lakehouseStatus — the suffix is stable
  #    across operands even though the kind names are not, so match on that.
  local groups res crs
  # `wxd` and `ds` are in the list because scripts/cpd_maintenance.sh verifies
  # wxd/lakehouse, wxdAddon/wxdaddon and DataStage/datastage on this cluster —
  # a pattern limited to *.cpd/zen/wkc.ibm.com would silently skip watsonx.data,
  # the one operand this script most depends on.
  groups="$(oc api-resources --namespaced -o name 2>/dev/null \
    | grep -E '\.(cpd|zen|wkc|wxd|ds|datastage|ccs|watsonxdata|watsonx)\.ibm\.com$|lakehouse' || true)"
  if [[ -z "${groups}" ]]; then
    warn "  No CPD/watsonx operand CRDs visible — is '${NS}' the CPD instance namespace?"
    problems=$((problems + 1))
  else
    debug "  Operand CRDs discovered: $(echo "${groups}" | tr '\n' ' ')"
    for res in ${groups}; do
      crs="$(oc -n "${NS}" get "${res}" -o json 2>/dev/null | "${PY}" -c '
import sys, json
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(0)
for item in d.get("items", []):
    name = item.get("metadata", {}).get("name", "?")
    st = item.get("status", {}) or {}
    # Any *Status / *status field the operand chose to publish, plus conditions.
    vals = {k: v for k, v in st.items()
            if k.lower().endswith("status") and isinstance(v, str)}
    if not vals and "conditions" in st:
        for c in st["conditions"]:
            if c.get("type") in ("Ready", "Successful", "Succeeded"):
                vals[c["type"]] = c.get("status", "?")
    if vals:
        print(name + "\t" + ", ".join(f"{k}={v}" for k, v in sorted(vals.items())))
' 2>/dev/null || true)"
      [[ -z "${crs}" ]] && continue
      while IFS=$'\t' read -r cr_name cr_status; do
        [[ -z "${cr_name}" ]] && continue
        # "Completed"/"True" are the healthy terminal states CPD operators use.
        if [[ "${cr_status}" == *Completed* ]] || [[ "${cr_status}" == *True* ]]; then
          ok "  ${res}/${cr_name}: ${cr_status}"
        else
          warn "  ${res}/${cr_name}: ${cr_status}  ← not Completed"
          problems=$((problems + 1))
        fi
      done <<< "${crs}"
    done
  fi

  # 3. Quiesce check. This is the one failure mode a pod-readiness scan cannot
  #    see: `scripts/cpd_maintenance.sh shutdown` sets .spec.shutdown on the wxd
  #    CRs, the operators scale their Deployments to 0/0 — and 0/0 satisfies
  #    "ready == desired", so a fully shut-down cluster reads perfectly healthy.
  #    The CR flags are the authoritative signal, so they are what's checked;
  #    guessing from replica counts would misfire on legitimately-scaled-down
  #    optional services.
  local kind_name flags_seen=0
  for kind_name in "wxd:lakehouse" "wxdAddon:wxdaddon"; do
    local kind="${kind_name%%:*}" cr="${kind_name##*:}" sd
    sd="$(oc -n "${NS}" get "${kind}" "${cr}" -o jsonpath='{.spec.shutdown}' 2>/dev/null || true)"
    [[ -z "${sd}" ]] && { debug "  ${kind}/${cr} .spec.shutdown not set/readable"; continue; }
    flags_seen=$((flags_seen + 1))
    # The field is a STRING in this CRD ("true"/"false"), not a boolean.
    if [[ "${sd}" == "true" ]]; then
      warn "  ${kind}/${cr} .spec.shutdown=true — watsonx.data is QUIESCED."
      warn "    Its Deployments are scaled to 0/0, which every readiness check"
      warn "    reads as healthy. Bring it back before demoing:"
      warn "      bash scripts/cpd_maintenance.sh startup"
      problems=$((problems + 1))
    else
      ok "  ${kind}/${cr} .spec.shutdown=${sd}"
    fi
  done
  (( flags_seen == 0 )) && debug "  No wxd shutdown flags visible — skipped the quiesce check."

  # 4. Platform layer. The operands above sit on top of these two; when either is
  #    mid-reconcile the CPD API answers unpredictably (401s, empty project lists).
  local plat
  for plat in "ZenService:lite-cr" "Ibmcpd:ibmcpd-cr"; do
    local pkind="${plat%%:*}" pcr="${plat##*:}" pstatus
    pstatus="$(oc -n "${NS}" get "${pkind}" "${pcr}" \
      -o jsonpath='{.status.zenStatus}{.status.controlPlaneStatus}' 2>/dev/null || true)"
    [[ -z "${pstatus}" ]] && { debug "  ${pkind}/${pcr} status not readable"; continue; }
    if [[ "${pstatus}" == *Completed* ]]; then
      ok "  ${pkind}/${pcr}: ${pstatus}"
    else
      warn "  ${pkind}/${pcr}: ${pstatus}  ← not Completed"
      problems=$((problems + 1))
    fi
  done

  # 5. IKC reporting prerequisites. Checked here because they explain a failure
  #    this script cannot fix and would otherwise be blamed on it: the reporting
  #    data mart is an Enterprise-edition feature. On Standard/Base, IKCBI2019E
  #    persists no matter how healthy the pods are or which roles cpadmin holds.
  local lic
  lic="$(oc -n "${NS}" get wkc wkc-cr -o jsonpath='{.spec.license}' 2>/dev/null || true)"
  if [[ -z "${lic}" ]]; then
    debug "  wkc-cr .spec.license not readable — skipping the IKC edition check."
  elif [[ "${lic}" == *Enterprise* ]]; then
    ok "  IKC license: Enterprise (reporting data mart is available)."
  else
    warn "  IKC license: ${lic}"
    warn "    The reporting data mart needs Enterprise. On Standard/Base the"
    warn "    connection below still registers, but IKC reporting (IKCBI2019E)"
    warn "    will not work and no role grant can change that."
    problems=$((problems + 1))
  fi

  # The flags the reporting service reads. ccs-features-configmap is the source
  # of truth; the two Deployments get their env from it.
  local flag val
  for flag in enforceAuthorizeReporting defaultAuthorizeReporting; do
    val="$(oc -n "${NS}" get configmap ccs-features-configmap \
             -o "jsonpath={.data.${flag}}" 2>/dev/null || true)"
    [[ -z "${val}" ]] && { debug "  ccs-features-configmap.${flag}: not set"; continue; }
    if [[ "${val}" == "true" ]]; then
      ok "  ccs-features-configmap.${flag}=true"
    else
      warn "  ccs-features-configmap.${flag}=${val} (reporting authorization off)"
      problems=$((problems + 1))
    fi
  done

  if (( problems > 0 )); then
    warn "  ${problems} deployment check(s) reported problems."
    warn "  A degraded add-on may still be harmless for this script — but if the"
    warn "  CPD API calls below fail, start here. Detail:"
    warn "    oc -n ${NS} get pods | grep -v Running"
    return 1
  fi
  ok "Deployment looks healthy."
  return 0
}

if $VERIFY_ONLY; then
  _verify_deployment && exit 0 || exit 1
fi
$DO_VERIFY && { _verify_deployment || true; }

# ── Crunchy operator: CRD *and* a running controller ─────────────────────────
# A CRD can linger after an uninstall, or the operator can be scoped to other
# namespaces. In both cases the PostgresCluster CR is accepted but never
# reconciled, so no pods appear and the wait below would burn its whole timeout
# for no reason. Catch that here with an actionable message instead.
oc api-resources --api-group=postgres-operator.crunchydata.com 2>/dev/null \
  | grep -q postgresclusters \
  || die "CRD postgresclusters not found — Crunchy Postgres for Kubernetes operator not installed.\n\
  Check for an EDB/CloudNativePG operator instead:\n\
    oc api-resources | grep -i postgres\n\
  CPD's own Postgres instances use clusters.postgresql.k8s.enterprisedb.io,\n\
  which this script does NOT manage."
ok "Crunchy operator CRD confirmed."

PGO_PODS="$(oc get pods --all-namespaces \
  -l postgres-operator.crunchydata.com/control-plane=postgres-operator \
  --no-headers 2>/dev/null | grep -c Running || true)"
if [[ "${PGO_PODS:-0}" -eq 0 ]]; then
  # Second attempt: older/OLM installs label the controller differently.
  PGO_PODS="$(oc get pods --all-namespaces --no-headers 2>/dev/null \
    | grep -E 'pgo|postgres-operator' | grep -c Running || true)"
fi
if [[ "${PGO_PODS:-0}" -eq 0 ]]; then
  warn "No running Crunchy operator pod found in any namespace."
  warn "  The PostgresCluster CR will be accepted but never reconciled, so no"
  warn "  Postgres pods will ever start. Verify the operator is installed and"
  warn "  that '${NS}' is inside its watch scope:"
  warn "    oc get pods -A | grep -Ei 'pgo|postgres-operator'"
  warn "    oc get csv -A | grep -i postgres"
else
  ok "Crunchy operator controller running (${PGO_PODS} pod(s))."
fi

# ── Helper: run SQL inside the primary pod ────────────────────────────────────
# Peer-auth via the postgres Unix socket: no password, no -h flag.
# Crunchy configures pg_hba.conf to allow 'postgres' OS user via local socket.
# [ST-5] pg_sql now returns the exit code of psql so callers can act on failure.
# The instance pod runs several containers (database, replication-cert-copy,
# pgbackrest, pgbackrest-config). Without -c, oc picks the first one, which is
# not guaranteed to be 'database' — always target it explicitly.
# PGO sets PGHOST=/tmp/postgres in the database container, so bare psql uses the
# Unix socket and matches the 'local all "postgres" peer' HBA rule.
#
# psql chatter (DO / NOTICE / CREATE SCHEMA) is captured rather than let loose on
# the terminal: it is shown under --verbose, or unconditionally when psql fails,
# so a failing statement still surfaces its error text to the caller's die().
pg_sql() {
  local db="$1" sql="$2" out rc
  debug "SQL on db=${db}: $(echo "${sql}" | head -1 | xargs)"
  out="$(oc -n "${NS}" exec -i "${PG_POD:-__no_pod__}" -c "${PG_CONTAINER}" -- \
    psql -U "${SU_USER:-postgres}" -d "${db}" -v ON_ERROR_STOP=1 <<< "${sql}" 2>&1)"
  rc=$?
  if (( rc != 0 )); then
    warn "psql output:"
    echo "${out}" | sed 's/^/    /' >&2
  else
    [[ -n "${out}" ]] && debug "  psql: $(echo "${out}" | tr '\n' ' ' | xargs)"
  fi
  return "${rc}"
}

# ── [ST-4] cpd_curl() — centralised curl wrapper for all CPD API calls ────────
# Usage: cpd_curl METHOD URL [payload_string]
# Sets global _CURL_HTTP to the HTTP status code.
# Sets global _CURL_BODY to the response body.
# Emits debug lines for method, URL, and HTTP code.
_CURL_HTTP=""
_CURL_BODY=""
cpd_curl() {
  local method="$1"
  local url="$2"
  local payload="${3:-}"
  local raw=""

  debug "→ ${method} ${url}"

  local curl_args=(-sk --max-time 30 -w "\nHTTP_CODE:%{http_code}"
    -X "${method}"
    -H "Authorization: Bearer ${TOKEN}"
    -H "Content-Type: application/json")

  if [[ -n "${payload}" ]]; then
    curl_args+=(-d "${payload}")
  fi

  raw="$(curl "${curl_args[@]}" "${url}" 2>&1 || true)"

  # Extract HTTP code from trailing sentinel line
  _CURL_HTTP="$(printf '%s' "${raw}" | grep '^HTTP_CODE:' | tail -1 | cut -d: -f2)"
  _CURL_BODY="$(printf '%s' "${raw}" | grep -v '^HTTP_CODE:' || true)"

  debug "← HTTP ${_CURL_HTTP}  body_len=${#_CURL_BODY}"
  if $VERBOSE; then
    debug "   body(first 300): ${_CURL_BODY:0:300}"
  fi
}

# ── [ST-7] _parse_json() — safe python3 JSON parser with visible failure ──────
# Usage: result="$(_parse_json 'python_expr' <<< "$json_string")"
# The python_expr receives the loaded object as 'd'.
# On failure, prints empty string and emits a warn with raw snippet.
_parse_json() {
  local expr="$1"
  local raw_input
  raw_input="$(cat)"
  local result
  result="$(echo "${raw_input}" | "${PY}" -c "
import sys, json
try:
    d = json.load(sys.stdin)
    result = ${expr}
    print(result if result is not None else '')
except Exception as e:
    import sys
    print('', file=sys.stderr)
    sys.exit(1)
" 2>/dev/null)" || {
    warn "JSON parse failed — raw response snippet: ${raw_input:0:200}"
    echo ""
    return 0
  }
  echo "${result}"
}

# ── Leader-pod discovery ─────────────────────────────────────────────────────
# Why this is not a one-liner: Patroni — not PGO — owns the role label on pods,
# and it writes role=master for the leader. PGO's "primary" role value is only
# ever applied to the Service object, so the v4.1 selector
# (role=primary on a pod) matched nothing and the script always timed out.
# Rather than swap one hardcoded guess for another, try every known leader value
# and, if none match, fall back to asking Postgres itself which node is the
# leader. That is label-independent and therefore future-proof.

# True when the pod's Ready condition is True. Checking the pod condition (not
# containerStatuses[0]) matters because instance pods run several containers and
# index 0 is not guaranteed to be 'database'.
_pod_ready() {
  local pod="$1"
  [[ "$(oc -n "${NS}" get pod "${pod}" \
        -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' \
        2>/dev/null || true)" == "True" ]]
}

# Sets the globals PG_POD and _LEADER_VIA; returns 0 on success, 1 otherwise.
# Deliberately assigns globals rather than echoing the pod name: a
# "$(_find_leader_pod)" call substitutes in a subshell, so any _LEADER_VIA set
# inside would be discarded and the caller would report an empty provenance.
_LEADER_VIA=""
_find_leader_pod() {
  local pod role candidates
  _LEADER_VIA=""
  PG_POD=""

  # 1. Label-based: try each known Patroni/PGO leader role value.
  for role in "${LEADER_ROLE_VALUES[@]}"; do
    pod="$(oc -n "${NS}" get pods \
      -l "${LABEL_CLUSTER}=${CLUSTER_NAME},${LABEL_ROLE}=${role}" \
      -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
    if [[ -n "${pod}" ]] && _pod_ready "${pod}"; then
      _LEADER_VIA="label ${LABEL_ROLE}=${role}"
      PG_POD="${pod}"
      return 0
    fi
    [[ -n "${pod}" ]] && debug "  ${LABEL_ROLE}=${role} matched ${pod} but it is not Ready yet."
  done

  # 2. Label-independent fallback: any Ready data=postgres pod that reports
  #    pg_is_in_recovery() = false is the read-write primary, by definition.
  candidates="$(oc -n "${NS}" get pods \
    -l "${LABEL_CLUSTER}=${CLUSTER_NAME},${LABEL_DATA}=postgres" \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null || true)"
  while IFS= read -r pod; do
    [[ -z "${pod}" ]] && continue
    _pod_ready "${pod}" || continue
    if [[ "$(oc -n "${NS}" exec "${pod}" -c "${PG_CONTAINER}" -- \
              psql -U postgres -d postgres -tAc 'SELECT pg_is_in_recovery()' \
              2>/dev/null | tr -d '[:space:]')" == "f" ]]; then
      _LEADER_VIA="pg_is_in_recovery() probe (role label absent or unknown)"
      PG_POD="${pod}"
      return 0
    fi
    debug "  ${pod} is Ready but in recovery — a replica, not the primary."
  done <<< "${candidates}"

  return 1
}

# Print everything needed to understand a stuck cluster, so the user does not
# have to run four oc commands by hand after a timeout.
_dump_pg_diagnostics() {
  echo
  warn "──── Diagnostics for PostgresCluster '${CLUSTER_NAME}' in '${NS}' ────"

  echo; warn "Pods (all pods for this cluster, with role labels):"
  oc -n "${NS}" get pods -l "${LABEL_CLUSTER}=${CLUSTER_NAME}" \
    -o custom-columns='NAME:.metadata.name,PHASE:.status.phase,READY:.status.conditions[?(@.type=="Ready")].status,ROLE:.metadata.labels.postgres-operator\.crunchydata\.com/role,NODE:.spec.nodeName' \
    2>&1 | sed 's/^/    /' || true

  echo; warn "Container states (why a pod is not Ready):"
  oc -n "${NS}" get pods -l "${LABEL_CLUSTER}=${CLUSTER_NAME}" \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{range .status.containerStatuses[*]}    - {.name}: ready={.ready} {.state}{"\n"}{end}{end}' \
    2>&1 | sed 's/^/    /' || true

  echo; warn "PostgresCluster status conditions:"
  oc -n "${NS}" get postgrescluster "${CLUSTER_NAME}" \
    -o jsonpath='{range .status.conditions[*]}    - {.type}={.status} reason={.reason} msg={.message}{"\n"}{end}' \
    2>&1 | sed 's/^/    /' || true

  # Pending PVCs are the single most common cause: no default StorageClass, or a
  # class that cannot satisfy ReadWriteOnce.
  echo; warn "PersistentVolumeClaims (Pending here means storage is the blocker):"
  oc -n "${NS}" get pvc -l "${LABEL_CLUSTER}=${CLUSTER_NAME}" \
    -o custom-columns='NAME:.metadata.name,STATUS:.status.phase,CLASS:.spec.storageClassName,SIZE:.spec.resources.requests.storage' \
    2>&1 | sed 's/^/    /' || true

  echo; warn "Default StorageClass:"
  oc get storageclass -o custom-columns='NAME:.metadata.name,DEFAULT:.metadata.annotations.storageclass\.kubernetes\.io/is-default-class' \
    2>&1 | sed 's/^/    /' || true

  echo; warn "Recent events for this cluster (last 15):"
  oc -n "${NS}" get events --sort-by=.lastTimestamp 2>/dev/null \
    | grep -i "${CLUSTER_NAME}" | tail -15 | sed 's/^/    /' || true
  echo
}

# Wait until the leader pod is Ready, then set the global PG_POD.
_wait_for_leader_pod() {
  local minutes="$1"
  local attempts=$(( minutes * 12 ))   # one attempt every 5 s
  local i

  info "Waiting for the primary pod to become Ready (timeout ${minutes} min) …"
  info "  Trying role label values: ${LEADER_ROLE_VALUES[*]}  (Patroni uses 'master')"

  for (( i = 1; i <= attempts; i++ )); do
    # Called directly, NOT in a $( ) subshell, so PG_POD and _LEADER_VIA stick.
    if _find_leader_pod; then
      ok "Primary pod: ${PG_POD}"
      info "  Found via: ${_LEADER_VIA}"
      return 0
    fi
    debug "Attempt ${i}/${attempts}: no Ready primary yet — sleeping 5s …"
    # Surface progress every minute so a long wait does not look like a hang.
    if (( i % 12 == 0 )); then
      info "  … still waiting ($(( i / 12 ))/${minutes} min)"
    fi
    sleep 5
  done

  _dump_pg_diagnostics
  die "No Ready primary pod for cluster '${CLUSTER_NAME}' after ${minutes} min.\n\
  The diagnostics above show where it is stuck. Most common causes:\n\
    • PVC Pending           → no default StorageClass, or RWO cannot be satisfied.\n\
                              Re-run with --storage-class <sc>.\n\
    • No pods at all        → the Crunchy operator is not running, or '${NS}' is\n\
                              outside its watch scope.\n\
    • Pod not Ready         → check the container states and logs:\n\
                              oc -n ${NS} logs -l ${LABEL_CLUSTER}=${CLUSTER_NAME} -c ${PG_CONTAINER} --tail=50\n\
    • Insufficient quota    → see the events section above.\n\
  Skip this step entirely with --skip-cluster once the cluster is healthy."
}

# ─────────────────────────────────────────────────────────────────────────────
# STEP 0 — Create PostgresCluster CR (idempotent)
# ─────────────────────────────────────────────────────────────────────────────
PG_POD=""

if ! $SKIP_CLUSTER; then
  step "Step 0/5 — PostgresCluster '${CLUSTER_NAME}' in ${NS}"

  if $DRY_RUN; then
    dryrun "Would apply PostgresCluster CR: ${CLUSTER_NAME} (postgres ${PG_VERSION}, storage ${PG_STORAGE})"
  else
    # Check if already exists
    if oc -n "${NS}" get postgrescluster "${CLUSTER_NAME}" &>/dev/null; then
      ok "PostgresCluster '${CLUSTER_NAME}' already exists — skipping create."
    else
      info "Creating PostgresCluster '${CLUSTER_NAME}' …"
      # storageClassName is emitted only when --storage-class was given, so the
      # cluster's default StorageClass is used otherwise.
      _SC_LINE=""
      if [[ -n "${STORAGE_CLASS}" ]]; then
        _SC_LINE="storageClassName: ${STORAGE_CLASS}"
        info "  StorageClass: ${STORAGE_CLASS}"
      else
        _DEFAULT_SC="$(oc get storageclass \
          -o jsonpath='{.items[?(@.metadata.annotations.storageclass\.kubernetes\.io/is-default-class=="true")].metadata.name}' \
          2>/dev/null || true)"
        if [[ -z "${_DEFAULT_SC}" ]]; then
          warn "  No default StorageClass on this cluster — the PVCs will stay Pending."
          warn "  Re-run with --storage-class <name>. Available classes:"
          oc get storageclass --no-headers -o custom-columns='NAME:.metadata.name' \
            2>&1 | sed 's/^/      /' || true
        else
          info "  StorageClass: ${_DEFAULT_SC} (cluster default)"
        fi
      fi

      oc -n "${NS}" apply -f - <<EOF
apiVersion: postgres-operator.crunchydata.com/v1beta1
kind: PostgresCluster
metadata:
  name: ${CLUSTER_NAME}
  namespace: ${NS}
  labels:
    app.kubernetes.io/managed-by: provision_pg_reporting.sh
spec:
  postgresVersion: ${PG_VERSION}
  instances:
    - name: pgha
      replicas: 1
      dataVolumeClaimSpec:
        accessModes: ["ReadWriteOnce"]
        ${_SC_LINE}
        resources:
          requests:
            storage: ${PG_STORAGE}
  backups:
    pgbackrest:
      repos:
        - name: repo1
          volume:
            volumeClaimSpec:
              accessModes: ["ReadWriteOnce"]
              ${_SC_LINE}
              resources:
                requests:
                  storage: 1Gi
EOF
      ok "PostgresCluster CR created."
    fi

    _wait_for_leader_pod "${WAIT_MINUTES}"
  fi
fi

# ── Locate the leader pod ────────────────────────────────────────────────────
# Only Step 1 needs it (psql runs through `oc exec` into the leader), so this is
# skipped with --skip-postgres. It used to run unconditionally, which meant a
# CPD-only re-run — the normal way to iterate on the project/connection steps —
# died in the pod wait even though it was never going to touch Postgres.
if [[ -z "${PG_POD}" ]] && ! $DRY_RUN && ! $SKIP_PG; then
  step "Locating primary pod for cluster '${CLUSTER_NAME}' in ${NS}"
  # Same code path as the wait above, but with a short timeout: when the cluster
  # is expected to be up already there is no reason to wait minutes for it.
  _wait_for_leader_pod 1
fi

# ── Resolve the read-write Service for real ──────────────────────────────────
# PG_HOST ends up in the K8s Secret, the CPD connection and .env, so a wrong
# name here fails later and far away from its cause (v4.1 shipped '-rw', which
# is CloudNativePG naming and does not exist on a Crunchy cluster). Verify the
# Service actually exists, and read the port from it rather than assuming 5432.
if ! $DRY_RUN; then
  step "Resolving read-write Service for cluster '${CLUSTER_NAME}'"

  if [[ -n "${PG_SVC_OVERRIDE}" ]]; then
    PG_SVC="${PG_SVC_OVERRIDE}"
    info "Using Service from --svc: ${PG_SVC}"
    oc -n "${NS}" get svc "${PG_SVC}" &>/dev/null \
      || die "Service '${PG_SVC}' not found in namespace '${NS}'.\n\
  List candidates: oc -n ${NS} get svc | grep ${CLUSTER_NAME}"
  else
    PG_SVC=""
    # '-primary' is the correct Crunchy name; the others are tried only so a
    # differently-named or EDB-managed cluster still resolves rather than
    # silently writing a hostname that does not exist.
    for _cand in "${CLUSTER_NAME}-primary" "${CLUSTER_NAME}-rw" "${CLUSTER_NAME}-ha" "${CLUSTER_NAME}"; do
      if oc -n "${NS}" get svc "${_cand}" &>/dev/null; then
        PG_SVC="${_cand}"
        debug "Service resolved: ${PG_SVC}"
        break
      fi
      debug "  Service '${_cand}' does not exist — trying next."
    done

    if [[ -z "${PG_SVC}" ]]; then
      warn "Could not find a read-write Service for cluster '${CLUSTER_NAME}'. Services present:"
      oc -n "${NS}" get svc -l "${LABEL_CLUSTER}=${CLUSTER_NAME}" \
        --no-headers -o custom-columns='NAME:.metadata.name,PORTS:.spec.ports[*].port' \
        2>&1 | sed 's/^/    /' || true
      die "No read-write Service found for cluster '${CLUSTER_NAME}' in '${NS}'.\n\
  Crunchy PGO normally creates '${CLUSTER_NAME}-primary'.\n\
  Pass the correct name with --svc <name>."
    fi
    ok "Read-write Service: ${PG_SVC}"
  fi

  # Read the real port off the Service rather than hardcoding 5432.
  _SVC_PORT="$(oc -n "${NS}" get svc "${PG_SVC}" \
    -o jsonpath='{.spec.ports[?(@.name=="postgres")].port}' 2>/dev/null || true)"
  [[ -z "${_SVC_PORT}" ]] && _SVC_PORT="$(oc -n "${NS}" get svc "${PG_SVC}" \
    -o jsonpath='{.spec.ports[0].port}' 2>/dev/null || true)"
  if [[ -n "${_SVC_PORT}" ]] && [[ "${_SVC_PORT}" != "${PG_PORT}" ]]; then
    info "Service port is ${_SVC_PORT} (not the default ${PG_PORT}) — using it."
    PG_PORT="${_SVC_PORT}"
  fi

  PG_HOST="${PG_SVC}.${NS}.svc.cluster.local"
  info "In-cluster host: ${PG_HOST}:${PG_PORT}  (sslmode=${PG_SSL_MODE})"
fi

# ── Use postgres OS-user peer auth for DDL (CREATE USER / CREATE DATABASE) ───
# Crunchy's declared app-user (ibmas-reporting) is not a superuser and cannot
# create roles or databases. The 'postgres' OS user on the pod has Unix-socket
# peer auth with superuser privileges — no password required.
SU_USER="postgres"
info "Superuser: ${SU_USER}  (peer auth via Unix socket in pod ${PG_POD:-<dry-run: not resolved>})"

# ─────────────────────────────────────────────────────────────────────────────
# STEP 1 — PostgreSQL
# ─────────────────────────────────────────────────────────────────────────────
if ! $SKIP_PG; then
  step "Step 1/5 — PostgreSQL: user + database + schema"

  # [ST-3] Generate password here, inside the SKIP_PG guard.
  # When --skip-postgres is used, password is read from the existing Secret
  # in Step 2 so DB credentials stay in sync.
  info "Generating new credentials for user '${REPORT_USER}' …"
  REPORT_PASS="$(LC_ALL=C tr -dc 'A-Za-z0-9' </dev/urandom 2>/dev/null | head -c 32 || true)"
  [[ -z "${REPORT_PASS}" ]] && REPORT_PASS="$(openssl rand -hex 16)"
  debug "Password for ${REPORT_USER}: generated (${#REPORT_PASS} chars)"

  if $DRY_RUN; then
    dryrun "CREATE USER ${REPORT_USER} WITH PASSWORD '<generated>';"
    dryrun "CREATE DATABASE ${REPORT_DB} OWNER ${REPORT_USER};"
    dryrun "GRANT CONNECT ON DATABASE ${REPORT_DB} TO ${REPORT_USER};"
    dryrun "CREATE SCHEMA IF NOT EXISTS ${REPORT_SCHEMA} AUTHORIZATION ${REPORT_USER};"
  else
    # [ST-5] Check return code of every pg_sql call
    debug "Running user create/update SQL on database: postgres"
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
END \$\$;" || die "psql failed: could not create/update user '${REPORT_USER}' on database 'postgres'.\n\
  Check pod logs: oc -n ${NS} logs ${PG_POD}"
    ok "User '${REPORT_USER}' ready."

    debug "Running CREATE DATABASE SQL"
    pg_sql postgres "
SELECT 'CREATE DATABASE ${REPORT_DB} OWNER ${REPORT_USER}'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${REPORT_DB}') \gexec" \
      || die "psql failed: could not create database '${REPORT_DB}'.\n\
  Verify user '${REPORT_USER}' exists and superuser peer auth is working.\n\
  Check: oc -n ${NS} logs ${PG_POD}"

    debug "Granting CONNECT on database"
    pg_sql postgres "GRANT CONNECT ON DATABASE ${REPORT_DB} TO ${REPORT_USER};" \
      || die "psql failed: GRANT CONNECT on '${REPORT_DB}' to '${REPORT_USER}' failed."
    ok "Database '${REPORT_DB}' ready."

    debug "Creating schema '${REPORT_SCHEMA}' in database '${REPORT_DB}'"
    pg_sql "${REPORT_DB}" "
CREATE SCHEMA IF NOT EXISTS ${REPORT_SCHEMA} AUTHORIZATION ${REPORT_USER};
GRANT ALL ON SCHEMA ${REPORT_SCHEMA} TO ${REPORT_USER};
ALTER USER ${REPORT_USER} SET search_path TO ${REPORT_SCHEMA}, public;" \
      || die "psql failed: schema '${REPORT_SCHEMA}' setup in database '${REPORT_DB}' failed.\n\
  Check: oc -n ${NS} logs ${PG_POD}"
    ok "Schema '${REPORT_SCHEMA}' ready."

    # [ST-5] Smoke-test TCP — show what was returned even on failure.
    # PGSSLMODE=require is required, not cosmetic: PGO's pg_hba.conf grants
    # regular users 'hostssl ... md5' only, so an unencrypted TCP attempt is
    # rejected with "no pg_hba.conf entry ... SSL off". This mirrors exactly how
    # CPD and pg_reporting.py will connect.
    info "Smoke-testing TCP auth for user '${REPORT_USER}' (sslmode=${PG_SSL_MODE}) …"
    SMOKE="$(oc -n "${NS}" exec -i "${PG_POD}" -c "${PG_CONTAINER}" -- \
      bash -c "PGPASSWORD='${REPORT_PASS}' PGSSLMODE='${PG_SSL_MODE}' psql \
        -U '${REPORT_USER}' -d '${REPORT_DB}' -h 127.0.0.1 -p ${PG_PORT} \
        -tA -c 'SELECT current_user,current_database(),current_schema()' 2>&1" || true)"
    if echo "${SMOKE}" | grep -q "${REPORT_USER}"; then
      ok "TCP smoke-test passed — ${SMOKE//|/ / }"
    else
      warn "TCP smoke-test FAILED for user '${REPORT_USER}' on db '${REPORT_DB}'."
      warn "  psql output: ${SMOKE}"
      if echo "${SMOKE}" | grep -qi 'no pg_hba.conf entry'; then
        warn "  This is an HBA rejection. PGO permits 'hostssl' only — confirm the"
        warn "  client is negotiating TLS (sslmode=require, not disable)."
      elif echo "${SMOKE}" | grep -qi 'password authentication failed'; then
        warn "  The password in the DB and the one used here disagree. Re-run"
        warn "  without --skip-postgres to reset it."
      fi
    fi
  fi
fi

# ─────────────────────────────────────────────────────────────────────────────
# STEP 2 — Kubernetes Secret
# ─────────────────────────────────────────────────────────────────────────────
step "Step 2/5 — Kubernetes Secret '${SECRET_NAME}'"

# [ST-3] When --skip-postgres was used, REPORT_PASS was never generated above.
# Read the existing Secret to stay in sync with what's actually in the DB.
if $SKIP_PG; then
  info "  --skip-postgres active: checking for existing Secret '${SECRET_NAME}' …"
  if oc -n "${NS}" get secret "${SECRET_NAME}" &>/dev/null; then
    EXISTING_PASS="$(oc -n "${NS}" get secret "${SECRET_NAME}" \
      -o jsonpath='{.data.password}' 2>/dev/null | base64 -d 2>/dev/null || true)"
    if [[ -n "${EXISTING_PASS}" ]]; then
      REPORT_PASS="${EXISTING_PASS}"
      info "  Password: read from existing Secret '${NS}/${SECRET_NAME}' (DB credentials unchanged)."
      debug "  Password length: ${#REPORT_PASS} chars"
    else
      warn "  Secret '${SECRET_NAME}' exists but password key is empty — generating new password."
      warn "  NOTE: DB password will NOT be updated (--skip-postgres). Manual DB ALTER USER may be needed."
      REPORT_PASS="$(LC_ALL=C tr -dc 'A-Za-z0-9' </dev/urandom 2>/dev/null | head -c 32 || true)"
      [[ -z "${REPORT_PASS}" ]] && REPORT_PASS="$(openssl rand -hex 16)"
    fi
  else
    info "  Secret '${SECRET_NAME}' does not exist — generating new password."
    warn "  NOTE: DB password will NOT be set (--skip-postgres). Create the DB user manually first."
    REPORT_PASS="$(LC_ALL=C tr -dc 'A-Za-z0-9' </dev/urandom 2>/dev/null | head -c 32 || true)"
    [[ -z "${REPORT_PASS}" ]] && REPORT_PASS="$(openssl rand -hex 16)"
  fi
fi

# ssl=true + sslmode=require: encrypt, but do not verify PGO's internal CA.
JDBC_URI="jdbc:postgresql://${PG_HOST}:${PG_PORT}/${REPORT_DB}?user=${REPORT_USER}&password=${REPORT_PASS}&ssl=true&sslmode=require"

if $DRY_RUN; then
  dryrun "oc create secret generic ${SECRET_NAME} --from-literal=... (8 keys) | oc apply"
else
  debug "Writing Secret '${NS}/${SECRET_NAME}' with 8 keys"
  oc -n "${NS}" create secret generic "${SECRET_NAME}" \
    --from-literal=database="${REPORT_DB}" \
    --from-literal=schema="${REPORT_SCHEMA}" \
    --from-literal=username="${REPORT_USER}" \
    --from-literal=password="${REPORT_PASS}" \
    --from-literal=host="${PG_HOST}" \
    --from-literal=port="${PG_PORT}" \
    --from-literal=sslmode="${PG_SSL_MODE}" \
    --from-literal=jdbc-uri="${JDBC_URI}" \
    --dry-run=client -o yaml | oc -n "${NS}" apply -f - >/dev/null
  ok "Secret '${NS}/${SECRET_NAME}' ready."

  # Read back from the Secret (the source of truth) — but only overwrite the
  # in-memory value if the read actually returned something. An unconditional
  # assignment silently blanked REPORT_PASS whenever the read hiccuped, and the
  # blank then went straight into the CPD connection's password property.
  SECRET_PASS="$(oc -n "${NS}" get secret "${SECRET_NAME}" \
    -o jsonpath='{.data.password}' 2>/dev/null | base64 -d 2>/dev/null || true)"
  if [[ -n "${SECRET_PASS}" ]]; then
    REPORT_PASS="${SECRET_PASS}"
    debug "Password confirmed from Secret (${#REPORT_PASS} chars)."
  else
    warn "Could not read the password back from Secret '${NS}/${SECRET_NAME}'."
    warn "  Keeping the in-memory value; verify with:"
    warn "    oc -n ${NS} get secret ${SECRET_NAME} -o jsonpath='{.data.password}' | base64 -d"
  fi
  [[ -z "${REPORT_PASS}" ]] && die "No password available for '${REPORT_USER}' — refusing to\n\
  register a CPD connection with an empty password. Re-run without --skip-postgres\n\
  so Step 1 sets it, or fix Secret '${NS}/${SECRET_NAME}' by hand."
fi

# ─────────────────────────────────────────────────────────────────────────────
# STEP 3 — CPD bearer token
# ─────────────────────────────────────────────────────────────────────────────
step "Step 3/5 — CPD authentication"

_cpd_token() {
  local auth_url="https://${CPD_HOST}/icp4d-api/v1/authorize"
  TOKEN="${TOKEN:-}"
  local api_key="${WXD_API_KEY:-}"
  local cpd_pass="${CPD_PASS:-}"
  local cpd_token="${CPD_TOKEN:-}"
  local _resp=""

  debug "CPD auth URL: ${auth_url}"

  if [[ -n "${api_key}" ]]; then
    info "  Trying API key auth for user '${CPD_USER}' …"
    _resp="$(curl -sk --max-time 20 -X POST "${auth_url}" \
      -H "Content-Type: application/json" \
      -d "{\"username\":\"${CPD_USER}\",\"api_key\":\"${api_key}\"}" || true)"
    debug "  API key auth response (first 200): ${_resp:0:200}"
    TOKEN="$(echo "${_resp}" | "${PY}" -c "import sys,json; print(json.load(sys.stdin)['token'])" 2>/dev/null || true)"
    [[ -n "${TOKEN}" ]] && debug "  Token obtained via API key (${#TOKEN} chars)."
  fi

  if [[ -z "${TOKEN}" ]] && [[ -n "${cpd_pass}" ]]; then
    info "  Trying password auth for user '${CPD_USER}' …"
    _resp="$(curl -sk --max-time 20 -X POST "${auth_url}" \
      -H "Content-Type: application/json" \
      -d "{\"username\":\"${CPD_USER}\",\"password\":\"${cpd_pass}\"}" || true)"
    debug "  Password auth response (first 200): ${_resp:0:200}"
    TOKEN="$(echo "${_resp}" | "${PY}" -c "import sys,json; print(json.load(sys.stdin)['token'])" 2>/dev/null || true)"
    [[ -n "${TOKEN}" ]] && debug "  Token obtained via password (${#TOKEN} chars)."
  fi

  # Third path: a bearer token supplied directly (--cpd-token) or left in .env by
  # scripts/get_token.py. Validate it before use — an expired token would
  # otherwise surface as a confusing 401 several steps later.
  if [[ -z "${TOKEN}" ]] && [[ -n "${cpd_token}" ]]; then
    info "  Trying pre-existing bearer token (WXD_SPARK_BEARER_TOKEN / --cpd-token) …"
    local _probe
    _probe="$(curl -sk --max-time 20 -o /dev/null -w '%{http_code}' \
      -H "Authorization: Bearer ${cpd_token}" \
      "https://${CPD_HOST}/v2/projects" || true)"
    if [[ "${_probe}" == "200" ]]; then
      TOKEN="${cpd_token}"
      debug "  Supplied bearer token validated (${#TOKEN} chars)."
    else
      warn "  Supplied bearer token rejected (HTTP ${_probe}) — probably expired."
      warn "  Refresh it with: .venv/bin/python scripts/get_token.py --export"
      _resp="bearer token probe returned HTTP ${_probe}"
    fi
  fi

  # [ST-6] die() with multi-line message now renders correctly via printf
  if [[ -z "${TOKEN}" ]]; then
    die "Could not obtain CPD bearer token.\n\
  CPD host : ${CPD_HOST}\n\
  CPD user : ${CPD_USER}\n\
  Methods tried: api_key=$([[ -n "${api_key}" ]] && echo yes || echo 'no (WXD_API_KEY unset)')\
 password=$([[ -n "${cpd_pass}" ]] && echo yes || echo 'no (WXD_CPD_PASSWORD unset)')\
 bearer=$([[ -n "${cpd_token}" ]] && echo yes || echo 'no (WXD_SPARK_BEARER_TOKEN unset)')\n\
  Last HTTP response snippet: ${_resp:0:300}\n\
  Causes:\n\
    • WXD_API_KEY / WXD_CPD_PASSWORD not set or incorrect in .env\n\
    • Inline comments in .env on the key/password line (must be stripped)\n\
    • CPD host unreachable or TLS certificate rejected\n\
  Fix: set one of them in .env, pass --cpd-password / --cpd-token, or run:\n\
    .venv/bin/python scripts/get_token.py --export"
  fi
}

# ── Project resolution ───────────────────────────────────────────────────────
# Look up the target project by name; create it when it does not exist.
# There is deliberately NO hardcoded GUID fallback any more: v4.1 pointed at a
# fixed GUID when lookup failed, which silently registered the connection into
# a project that may belong to someone else or not exist at all.

# Sets _PROJECT_GUID to the GUID of a project whose name matches; returns 1 if
# none matched. Deliberately does NOT echo the GUID: callers used to do
#   PROJECT_ID="$(_lookup_project …)"
# which runs the function in a subshell and throws away _CURL_HTTP / _CURL_BODY,
# so a failure reported "Last HTTP status: " with both fields empty — exactly the
# useless error the customer hit. Returning through a global keeps the diagnostic
# state alive in the caller.
_PROJECT_GUID=""
_PROJECT_MATCHED_NAME=""
_lookup_project() {
  local want="$1" hit
  _PROJECT_GUID=""; _PROJECT_MATCHED_NAME=""
  cpd_curl GET "https://${CPD_HOST}/v2/projects?limit=100"
  [[ "${_CURL_HTTP}" != "200" ]] && return 1

  # Emits "how<TAB>name<TAB>guid" so the caller can say WHICH project it bound to.
  # A silent fuzzy hit is dangerous: on a cluster with several *-ingest-demo
  # projects it would wire the reporting connection into the wrong one, and
  # nothing downstream would notice.
  hit="$(printf '%s' "${_CURL_BODY}" | "${PY}" -c "
import sys, json
want = sys.argv[1].lower()
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(1)
exact, fuzzy = None, None
for p in d.get('resources', []):
    name = (p.get('entity', {}) or {}).get('name', '') or ''
    guid = (p.get('metadata', {}) or {}).get('guid', '') or ''
    if not guid:
        continue
    if name.lower() == want:
        exact = (name, guid)
        break
    # Tolerate the historical naming spread: ibmas-ingest-demo, ingest-demo, …
    if fuzzy is None and ('ingest-demo' in name.lower() or 'ibmas-ingest' in name.lower()):
        fuzzy = (name, guid)
if exact:
    print('exact\t%s\t%s' % exact)
elif fuzzy:
    print('fuzzy\t%s\t%s' % fuzzy)
" "${want}" 2>/dev/null || true)"

  local how
  IFS=$'\t' read -r how _PROJECT_MATCHED_NAME _PROJECT_GUID <<< "${hit}"

  if [[ "${how}" == "fuzzy" ]]; then
    if ! $ALLOW_FUZZY_PROJECT; then
      warn "  No project named exactly '${want}'. A similar one exists:"
      warn "    ${_PROJECT_MATCHED_NAME} (${_PROJECT_GUID})"
      warn "  NOT adopting it — that could attach the reporting connection to the"
      warn "  wrong project. Pass --project-name '${_PROJECT_MATCHED_NAME}' to use it,"
      warn "  or --allow-fuzzy-project to accept near matches."
      _PROJECT_GUID=""; _PROJECT_MATCHED_NAME=""
      return 1
    fi
    warn "  Fuzzy match accepted: '${want}' → '${_PROJECT_MATCHED_NAME}' (${_PROJECT_GUID})"
  fi

  # >100 projects: this endpoint's paging behaviour is unverified, so say so
  # rather than silently creating a duplicate of a project that is just off-page.
  if [[ -z "${_PROJECT_GUID}" ]] \
     && [[ "$(printf '%s' "${_CURL_BODY}" | "${PY}" -c \
              'import sys,json;print(len(json.load(sys.stdin).get("resources",[])))' \
              2>/dev/null || echo 0)" == "100" ]]; then
    warn "  The project list came back full (100 rows) — '${want}' may exist beyond"
    warn "  the first page. Verify in the UI before letting this script create it."
  fi

  [[ -n "${_PROJECT_GUID}" ]]
}

# Create a CPD project. Sets _PROJECT_GUID on success; returns 1 on failure.
#
# Endpoint verified against the IBM cpdctl CLI (`cpdctl project create --help`,
# operation transactional_post_project):
#
#   POST /transactional/v2/projects
#     IBM's own help text: "when creating projects programmatically, always use
#     this endpoint, not /v2/projects" — so there is deliberately NO fallback to
#     POST /v2/projects. That route exists but is not the creation API, and using
#     it yields a project without the storage/asset container wired up.
#   type            "cpd"        — the on-prem project type (cpdctl --type cpd)
#   storage.type    "assetfiles" — the Software Hub backing store. Storage is
#                                  "required on public cloud, optional on Cloud
#                                  Pak for Data"; sent explicitly so the asset
#                                  container is created deterministically.
#   enforce_members false        — do not scope members to the creator's SAML.
#   generator                    — free text, but validated: must be >= 8 chars
#                                  and reject anything with '.' or '_' ("Entity
#                                  body.generator contains invalid characters"
#                                  / "falls below the minimum acceptable
#                                  length: N < 8" — both hit live testing this
#                                  script against a real 5.3 cluster). Hyphens
#                                  are accepted; this is why the value below is
#                                  "provision-pg-reporting", not the filename.
#
# Body fields are snake_case (the CLI's kebab-case flags are cosmetic). The
# endpoint accepts exactly ten body keys — generator, name, storage, compute,
# description, enforce_members, public, tags, tools, type — of which only
# generator, name and storage are required. The eight sent below are all inside
# that set.
#
# verify_unique_name is a QUERY PARAMETER, not a body property. Sending it in the
# body is what produced the customer's
#     HTTP 400 "Extraneous properties for project: verify_unique_name"
# The IBM Go SDK writes it via RequestBuilder.AddQuery (bool → "true"/"false"),
# which is why the CLI's --verify-unique-name flag looked like a body field but
# never was. With it on the URL the server refuses to make a second project with
# a name that appeared since our lookup; that 409 is caught below and the
# existing project adopted.
#
# WHY THE RETRY LOOP: a CPD build could still validate the body differently from
# the SDK's spec. Rather than hand-maintaining a per-version allow-list, we let
# the server tell us — it enumerates the offending keys in `reason`, so we strip
# exactly those and retry. Same for the query parameter, which is dropped if the
# server complains about it. That makes one run converge instead of dead-ending.
_create_project() {
  local name="$1" payload attempt=1 max_attempts=4 drop
  local url="https://${CPD_HOST}/transactional/v2/projects?verify_unique_name=true"

  payload="$("${PY}" -c '
import json, sys
name, desc = sys.argv[1], sys.argv[2]
print(json.dumps({
    "name": name,
    "description": desc,
    "generator": "provision-pg-reporting",
    "type": "cpd",
    "public": False,
    "enforce_members": False,
    "storage": {"type": "assetfiles"},
    "tags": ["provision_pg_reporting"],
}))' "${name}" \
      "Auto-created by provision_pg_reporting.sh to host the ibmas-reporting PostgreSQL connection.")"

  while (( attempt <= max_attempts )); do
    debug "  Create attempt ${attempt}/${max_attempts} → ${url}"
    debug "    body: ${payload}"
    cpd_curl POST "${url}" "${payload}"

    # Any 2xx is success — the generated SDK compares against no specific code,
    # and the .zip-import flavour of this endpoint can answer 202.
    if [[ "${_CURL_HTTP}" =~ ^2[0-9][0-9]$ ]]; then
      break
    fi

    # The name appeared between our lookup and this call — adopt it.
    if [[ "${_CURL_HTTP}" == "409" ]] || [[ "${_CURL_BODY}" == *"not unique"* ]] \
       || [[ "${_CURL_BODY}" == *"already exists"* ]]; then
      info "    Name already taken (HTTP ${_CURL_HTTP}) — adopting the existing project."
      _lookup_project "${name}" && return 0
    fi

    # Query parameter refused: only possible if this build validates the query
    # string strictly. It is a uniqueness nicety, not a requirement — the lookup
    # above and the 409 branch already cover the race, so drop it and retry
    # rather than failing the run over it.
    if [[ "${_CURL_HTTP}" == "400" ]] && [[ "${_CURL_BODY}" == *"verify_unique_name"* ]] \
       && [[ "${payload}" != *"verify_unique_name"* ]] && [[ "${url}" == *"?"* ]]; then
      warn "    Server rejected the verify_unique_name query parameter — retrying without it."
      url="${url%%\?*}"
      (( attempt++ ))
      continue
    fi

    # Strict-schema rejection: peel off the properties the server named.
    if [[ "${_CURL_HTTP}" == "400" ]] && [[ "${_CURL_BODY}" == *"xtraneous propert"* ]]; then
      drop="$(printf '%s' "${_CURL_BODY}" | "${PY}" -c '
import json, re, sys
raw = sys.stdin.read()
# Prefer the structured field, fall back to scanning the whole body.
try:
    text = json.loads(raw).get("reason") or raw
except Exception:
    text = raw
m = re.search(r"[Ee]xtraneous propert(?:y|ies) for [^:]*:\s*([^.]*)", text)
print(" ".join(sorted({p.strip() for p in m.group(1).split(",") if p.strip()})) if m else "")
' 2>/dev/null || true)"

      if [[ -n "${drop}" ]]; then
        warn "    Server rejected unsupported propert$( [[ "${drop}" == *' '* ]] && echo ies || echo y ): ${drop}"
        info "    Retrying without $( [[ "${drop}" == *' '* ]] && echo them || echo it ) …"
        payload="$(printf '%s' "${payload}" | "${PY}" -c '
import json, sys
body = json.load(sys.stdin)
for key in sys.argv[1:]:
    body.pop(key, None)
print(json.dumps(body))' ${drop})"
        (( attempt++ ))
        continue
      fi
    fi

    warn "  Project create returned HTTP ${_CURL_HTTP}: ${_CURL_BODY:0:400}"
    return 1
  done

  if [[ ! "${_CURL_HTTP}" =~ ^2[0-9][0-9]$ ]]; then
    warn "  Project create still failing after ${max_attempts} attempts (HTTP ${_CURL_HTTP})."
    return 1
  fi

  # The success response carries exactly one property, {"location": "…"} — the
  # transactional endpoint does NOT return metadata.guid (that belongs to
  # GET /v2/projects/{id}). metadata.guid is still accepted below in case a build
  # answers with the richer model.
  _PROJECT_GUID="$(printf '%s' "${_CURL_BODY}" | "${PY}" -c "
import sys, json, re
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(1)
loc = d.get('location') or d.get('href') or ''
m = re.search(r'([0-9a-fA-F-]{36})', loc)
print(m.group(1) if m
      else (d.get('metadata', {}) or {}).get('guid', '') or d.get('guid', '') or '')
" 2>/dev/null || true)"

  if [[ -z "${_PROJECT_GUID}" ]]; then
    warn "  Project created but no GUID in response: ${_CURL_BODY:0:300}"
    # Last resort: the project exists server-side, so look it up by name.
    _lookup_project "${name}" || return 1
  fi
  return 0
}

_resolve_or_create_project() {
  info "Resolving CPD project '${PROJECT_NAME}' …"

  # Note the plain `if`, not PROJECT_ID="$(_lookup_project …)": a command
  # substitution runs the helper in a subshell, so _CURL_HTTP/_CURL_BODY set
  # inside it are discarded and the die below printed empty diagnostics.
  if _lookup_project "${PROJECT_NAME}"; then
    PROJECT_ID="${_PROJECT_GUID}"
    # Report the name that was actually bound, not the one that was asked for:
    # under --allow-fuzzy-project those differ, and that is precisely the case
    # where echoing the request back would hide which project got the connection.
    # PROJECT_NAME is updated too, so the summary and the URL agree with it.
    PROJECT_NAME="${_PROJECT_MATCHED_NAME:-${PROJECT_NAME}}"
    ok "Project found: ${PROJECT_NAME} (${PROJECT_ID})"
    return 0
  fi

  info "  Project '${PROJECT_NAME}' not found in CPD."
  if $NO_CREATE_PROJECT; then
    die "Project '${PROJECT_NAME}' does not exist and --no-create-project was given.\n\
  Either drop that flag, pass --project-id <GUID>, or create the project in CPD:\n\
    https://${CPD_HOST}/projects?context=icp4data"
  fi

  info "  Creating it …"
  if _create_project "${PROJECT_NAME}"; then
    PROJECT_ID="${_PROJECT_GUID}"
  fi

  if [[ -z "${PROJECT_ID}" ]]; then
    die "Could not create CPD project '${PROJECT_NAME}'.\n\
  Last HTTP status: ${_CURL_HTTP}\n\
  Response: ${_CURL_BODY:0:400}\n\
  Common causes:\n\
    • '${CPD_USER}' lacks the 'Create projects' permission in CPD.\n\
    • No storage is configured for projects on this deployment.\n\
  Work around it by creating the project in the UI and re-running with\n\
  --project-id <GUID>:\n\
    https://${CPD_HOST}/projects?context=icp4data"
  fi

  # Confirm the new project is really readable before writing assets into it.
  cpd_curl GET "https://${CPD_HOST}/v2/projects/${PROJECT_ID}"
  if [[ "${_CURL_HTTP}" == "200" ]]; then
    # "ready" rather than "created": on a name collision _create_project adopts
    # the project a competing creator made instead of failing.
    ok "Project ready and verified: ${PROJECT_NAME} (${PROJECT_ID})"
  else
    warn "Project created (${PROJECT_ID}) but GET returned HTTP ${_CURL_HTTP}."
    warn "  It may still be initialising — the connection step below may need a re-run."
  fi
}

if ! $DRY_RUN; then
  _cpd_token
  info "Bearer token obtained (${#TOKEN} chars)."

  # Resolve project ID if not set, creating the project when absent
  if [[ -z "${PROJECT_ID}" ]]; then
    _resolve_or_create_project
  else
    info "Project ID supplied via --project-id: ${PROJECT_ID}"
  fi
else
  TOKEN="dry-run-token"
  if [[ -z "${PROJECT_ID}" ]]; then
    PROJECT_ID="<resolved-or-created-at-runtime>"
    dryrun "Would look up CPD project '${PROJECT_NAME}' and create it if absent."
  fi
fi

# ─────────────────────────────────────────────────────────────────────────────
# STEP 3b — Grant wkc_reporting_administrator role to cpadmin
# ─────────────────────────────────────────────────────────────────────────────
if ! $SKIP_ROLE; then
  step "Step 3b/5 — Reporting role for ${CPD_USER} ($($GRANT_ROLE && echo 'grant enabled' || echo 'read-only'))"

  if $DRY_RUN; then
    dryrun "GET  https://${CPD_HOST}/icp4d-api/v1/users/${CPD_USER}"
    $GRANT_ROLE && dryrun "PUT  https://${CPD_HOST}/icp4d-api/v1/users/${CPD_USER}  (--grant-role given)"
    $GRANT_ROLE || dryrun "No PUT — the write needs --grant-role."
  else
    # ── Cheapest check first: the token already carries the answer ─────────────
    # CPD embeds the caller's permission list inside the JWT. If manage_reporting
    # is already there, nothing about reporting access is missing and there is no
    # reason to touch the sole admin's role list at all.
    #
    # This also explains why a grant can never help the *current* run: the token
    # was minted in Step 3, before any change, and permissions are baked in at
    # mint time. A real grant needs a fresh token plus a wkc-bi-data-service
    # restart — both printed below rather than done silently.
    TOKEN_PERMS="$("${PY}" -c '
import base64, json, sys
tok = sys.argv[1]
parts = tok.split(".")
if len(parts) < 2:
    sys.exit(1)
raw = parts[1] + "=" * (-len(parts[1]) % 4)      # JWT strips base64 padding
try:
    claims = json.loads(base64.urlsafe_b64decode(raw))
except Exception:
    sys.exit(1)
perms = claims.get("permissions") or []
print(" ".join(p for p in perms if "reporting" in p))' "${TOKEN}" 2>/dev/null || true)"

    if [[ -n "${TOKEN_PERMS}" ]]; then
      debug "Reporting permissions in the current token: ${TOKEN_PERMS}"
    fi
    if [[ "${TOKEN_PERMS}" == *manage_reporting* ]]; then
      ok "Token for ${CPD_USER} already carries 'manage_reporting' — no role change needed."
    fi

    # PUT /icp4d-api/v1/users/<username> REPLACES user_roles wholesale — it is
    # not a merge. Worse, that contract has NO primary source: cpdctl 1.8.233 has
    # no user/role command group at all, and IBM's own cloud-pak-deployer
    # deliberately never PUTs an individual user's roles (it grants platform
    # roles additively through user groups). So "replace" is an assumption we
    # cannot verify, and on a single-admin deployment guessing wrong is an
    # unrecoverable lockout: no second account could restore admin via the UI.
    #
    # Therefore: read-only by default, write only behind --grant-role, and even
    # then only with a role list we actually read back from the server.
    info "Fetching current roles for user '${CPD_USER}' …"
    cpd_curl GET "https://${CPD_HOST}/icp4d-api/v1/users/${CPD_USER}"

    CURRENT_ROLES=""
    ROLE_READ_OK=false
    if [[ "${_CURL_HTTP}" == "200" ]]; then
      # Exit 2 (→ empty output) when user_roles is absent or not a list, so a
      # response shaped differently than expected can't be mistaken for "no roles".
      CURRENT_ROLES="$(echo "${_CURL_BODY}" | "${PY}" -c "
import sys, json
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(2)
ui = d.get('UserInfo', d)
roles = ui.get('user_roles')
if not isinstance(roles, list) or not roles:
    sys.exit(2)          # unknown shape or empty — do NOT risk a replacing PUT
if 'wkc_reporting_administrator' not in roles:
    roles.append('wkc_reporting_administrator')
print(json.dumps(roles))
" 2>/dev/null)" && ROLE_READ_OK=true
    fi

    if ! $ROLE_READ_OK || [[ -z "${CURRENT_ROLES}" ]]; then
      warn "Skipping the role grant — could not read '${CPD_USER}''s existing roles."
      warn "  GET returned HTTP ${_CURL_HTTP}: ${_CURL_BODY:0:200}"
      warn "  Not sending a PUT: user_roles is a full replacement, so guessing the"
      warn "  list would strip '${CPD_USER}''s other roles (including admin)."
      warn "  Grant it by hand instead:"
      warn "    CPD → Manage → Access control → Users → ${CPD_USER} → add"
      warn "    'Reporting administrator' (wkc_reporting_administrator)"
    else
      debug "Roles read from CPD, with the new one appended: ${CURRENT_ROLES}"

      # Nothing to do if it is already granted — avoids a pointless write to the
      # only admin account on the cluster.
      if [[ "${_CURL_BODY}" == *"wkc_reporting_administrator"* ]]; then
        ok "Role wkc_reporting_administrator already held by ${CPD_USER} — no change."
      elif ! $GRANT_ROLE; then
        warn "${CPD_USER} does not hold wkc_reporting_administrator."
        warn "  Not writing it: PUT /icp4d-api/v1/users/${CPD_USER} replaces the whole"
        warn "  role list and ${CPD_USER} is the only admin on this cluster."
        warn "  Grant it in the UI (CPD → Manage → Access control → Users), or re-run"
        warn "  this script with --grant-role to let it send:"
        warn "    {\"user_roles\": ${CURRENT_ROLES}}"
        warn "  Safer still, grant additively through a user group:"
        warn "    POST /usermgmt/v2/groups {\"name\":…,\"role_identifiers\":[\"wkc_reporting_administrator\"]}"
      else
        info "Sending role update → ${CURRENT_ROLES}"
        cpd_curl PUT "https://${CPD_HOST}/icp4d-api/v1/users/${CPD_USER}" \
          "{\"user_roles\": ${CURRENT_ROLES}}"

        if [[ "${_CURL_HTTP}" == "200" ]]; then
          # A 200 here does NOT mean the list applied, so nothing is announced as
          # granted until the read-back confirms it — claiming success on the
          # status code alone is how a silently-dropped role becomes a green run.
          debug "PUT accepted (HTTP 200); verifying against the user record …"
          cpd_curl GET "https://${CPD_HOST}/icp4d-api/v1/users/${CPD_USER}"
          if [[ "${_CURL_BODY}" == *"wkc_reporting_administrator"* ]]; then
            ok "Role wkc_reporting_administrator granted to ${CPD_USER} (verified)."
            # A granted role is invisible to any token minted before the grant —
            # permissions live inside the JWT. These two steps are what actually
            # make it take effect; they are not done here because the restart
            # interrupts other users of the reporting service. Printed only on a
            # verified grant: after a failed one there is nothing to activate.
            info "  To make it effective:"
            info "    .venv/bin/python scripts/get_token.py --export   # fresh token"
            info "    oc -n ${NS} rollout restart deployment/wkc-bi-data-service"
          else
            warn "PUT returned 200 but ${CPD_USER} still does not show"
            warn "  wkc_reporting_administrator. Treat the grant as NOT applied and"
            warn "  set it in CPD → Manage → Access control → Users."
          fi
        else
          warn "Role grant returned HTTP ${_CURL_HTTP}: ${_CURL_BODY:0:300}"
          warn "You may need to grant this role manually in CPD → Manage → Access control → Users."
        fi
      fi
    fi
  fi
fi

# ─────────────────────────────────────────────────────────────────────────────
# STEP 4 — CPD Connection
# ─────────────────────────────────────────────────────────────────────────────

# Ask the cluster which datasource type PostgreSQL is, instead of shipping a
# GUID. The registry is global — deliberately NO project_id/catalog_id/space_id
# scope — and paged by offset, unlike /v2/connections which pages by token.
# Falls back to the bare name, which the API documents as an alternative to the
# id ("The id or the name of the data source type … For example … or db2").
#
# Two things verified against a real CPD 5.3 instance, both required:
#   • limit is capped at 100 server-side. limit=200 gets a flat HTTP 400
#     ("bad_paging_range"), so the whole lookup always fell through to the
#     name fallback — never a hard failure, but the "could not resolve" warning
#     fired on every single run.
#   • entity.name=… is NOT a real server-side filter on this endpoint (it is
#     silently ignored — same first page comes back regardless), and this
#     registry has 155 entries with the generic "postgresql" entry
#     (e1c23729-99d8-4407-b3df-336e33ffdc82) sitting on page 2. So even fixing
#     the limit alone still would not have found it — real pagination is
#     required, not just a smaller page size.
_resolve_datasource_type() {
  # Initialised, not just declared: `local found` alone leaves the variable
  # UNSET, so the `[[ -n "${found}" ]]` below aborts the whole script under
  # `set -u` on exactly the path this fallback exists to handle (the registry
  # call not returning 200).
  local found="" offset=0 page_size=100 last_http="" page
  for (( page = 0; page < 10; page++ )); do   # 10 * 100 = 1000-entry safety cap
    cpd_curl GET "https://${CPD_HOST}/v2/datasource_types?limit=${page_size}&offset=${offset}"
    last_http="${_CURL_HTTP}"
    [[ "${last_http}" != "200" ]] && break

    found="$(printf '%s' "${_CURL_BODY}" | "${PY}" -c '
import sys, json
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(1)
want_name = sys.argv[1].lower()
by_name = by_label = None
for r in d.get("resources", []):
    e = r.get("entity", {}) or {}
    aid = (r.get("metadata", {}) or {}).get("asset_id", "")
    if not aid:
        continue
    name  = (e.get("name")  or "").lower()
    label = (e.get("label") or "").lower()
    if name == want_name:
        by_name = (aid, e.get("name"), e.get("label"))
        break
    # "PostgreSQL" is the display label; only used if the name never matches,
    # and never for IBM Cloud "Databases for PostgreSQL" — a different
    # connector, wrong for a self-managed Crunchy cluster.
    if by_label is None and label == "postgresql":
        by_label = (aid, e.get("name"), e.get("label"))
hit = by_name or by_label
if hit:
    print("%s\t%s\t%s" % hit)' "${PG_DATASOURCE_NAME}" 2>/dev/null || true)"
    [[ -n "${found}" ]] && break

    # Stop once this page came back short (last page) rather than looping to
    # the safety cap for nothing.
    local _count
    _count="$(printf '%s' "${_CURL_BODY}" | "${PY}" -c \
      'import sys,json; print(len(json.load(sys.stdin).get("resources",[])))' 2>/dev/null || echo 0)"
    (( _count < page_size )) && break
    offset=$(( offset + page_size ))
  done

  if [[ -n "${found}" ]]; then
    local ds_label ds_name
    IFS=$'\t' read -r PG_DATASOURCE_TYPE ds_name ds_label <<< "${found}"
    ok "  Datasource type resolved: ${ds_label:-?} (name=${ds_name:-?}) → ${PG_DATASOURCE_TYPE}"
    return 0
  fi

  PG_DATASOURCE_TYPE="${PG_DATASOURCE_NAME}"
  warn "  Could not resolve a PostgreSQL datasource type (last HTTP ${last_http})."
  warn "  Falling back to the name '${PG_DATASOURCE_NAME}', which the API accepts"
  warn "  in place of an id. If the create below fails on datasource_type, list them:"
  warn "    curl -sk -H \"Authorization: Bearer \$TOKEN\" \\"
  warn "      'https://${CPD_HOST}/v2/datasource_types?limit=100&offset=0' | \\"
  warn "      python3 -c 'import json,sys;[print(r[\"metadata\"][\"asset_id\"], r[\"entity\"].get(\"name\"), r[\"entity\"].get(\"label\")) for r in json.load(sys.stdin)[\"resources\"]]'"
  return 1
}

# Shared by both connection-creation steps (project-scoped Step 4 and the
# catalog-scoped/"platform" Step 4b) — the connection body itself is identical
# either way; catalog_id vs project_id is a query param on the create URL, not
# part of the payload.
_build_conn_payload() {
  # Built with json.dumps, not string interpolation: the password comes from a
  # K8s Secret when --skip-postgres is used, and a human-set value containing a
  # quote or backslash would otherwise emit malformed JSON that the API rejects
  # with an opaque parse error.
  #
  # ssl=true is mandatory, not a preference: PGO's pg_hba.conf only emits
  # 'hostssl … md5' lines, so a non-TLS connection is refused outright. Values
  # are strings because that is the form the platform itself writes into its own
  # connection assets (port too).
  #
  # There is no "proxy" property: it appears in no real CPD export and in no
  # connector definition, and CPD 5.3 rejects unknown properties outright — it
  # was a prime candidate for an extraneous-property 400. Dropped.
  CONN_PAYLOAD="$("${PY}" -c '
import json, sys
(name, desc, dstype, host, port, db, user, pwd) = sys.argv[1:9]
print(json.dumps({
    "name": name,
    "description": desc,
    "datasource_type": dstype,
    "origin_country": "us",
    "properties": {
        "host": host,
        "port": port,
        "database": db,
        "username": user,
        "password": pwd,
        "ssl": "true",
        "ssl_certificate_validation": "false",
        "query_timeout": "300",
    },
}))' \
    "ibmas-reporting" \
    "ibmas_reporting schema — Crunchy PostgresCluster '${CLUSTER_NAME}' in ${NS}, provisioned by provision_pg_reporting.sh" \
    "${PG_DATASOURCE_TYPE}" "${PG_HOST}" "${PG_PORT}" "${REPORT_DB}" \
    "${REPORT_USER}" "${REPORT_PASS}")"
}

# Resolves the Platform assets catalog's GUID by name. Confirmed live
# (2026-08-20): a CPD "Data Source Definition" is a generic catalog asset of
# type 'ibm_data_source', and that asset type's scope_restrictions.uids is
# ["ibm-global-catalog"] — the Platform assets catalog's fixed internal uid on
# every CPD install (distinct from its per-cluster metadata.guid, which we
# still need for the actual create/list calls). DSDs, and the optional
# catalog-scoped "platform" connection, can ONLY live in this one catalog.
_resolve_platform_catalog() {
  cpd_curl GET "https://${CPD_HOST}/v2/catalogs?limit=100"
  [[ "${_CURL_HTTP}" != "200" ]] && return 1

  PLATFORM_CATALOG_ID="$(printf '%s' "${_CURL_BODY}" | "${PY}" -c '
import sys, json
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(1)
# /v2/catalogs is its own snowflake: the list key is "catalogs", not the
# "resources" every other CPD list endpoint (connections, projects,
# datasource_types, ...) uses. Confirmed live 2026-08-20 — check both, "resources"
# first only as a defensive fallback in case a future CPD version normalizes it.
for r in d.get("catalogs") or d.get("resources") or []:
    ent  = r.get("entity", {}) or {}
    cat  = ent.get("catalog", {}) or {}
    name = (ent.get("name") or cat.get("name") or "").lower()
    uid  = ent.get("uid") or cat.get("uid") or ""
    if "platform assets" in name or uid == "ibm-global-catalog":
        print((r.get("metadata", {}) or {}).get("guid", ""))
        break
' 2>/dev/null || true)"

  [[ -n "${PLATFORM_CATALOG_ID}" ]] && return 0
  return 1
}

# Resolves PLATFORM_CATALOG_ID once (memoised) — call this at the top of any
# step that needs the Platform assets catalog (Step 4b, Step 5/DSD).
_ensure_platform_catalog() {
  [[ -n "${PLATFORM_CATALOG_ID}" ]] && return 0
  info "Resolving the Platform assets catalog …"
  if _resolve_platform_catalog; then
    ok "  Platform assets catalog: ${PLATFORM_CATALOG_ID}"
    return 0
  fi
  warn "  Could not resolve the Platform assets catalog by name."
  warn "  Pass --platform-catalog-id <GUID> explicitly. List catalogs with:"
  warn "    curl -sk -H \"Authorization: Bearer \$TOKEN\" 'https://${CPD_HOST}/v2/catalogs?limit=100'"
  return 1
}

CONN_ID=""
if ! $SKIP_CONN; then
  step "Step 4/5 — CPD connection 'ibmas-reporting' in project ${PROJECT_ID}"

  if $DRY_RUN; then
    PG_DATASOURCE_TYPE="${PG_DATASOURCE_NAME}"
    dryrun "GET  https://${CPD_HOST}/v2/datasource_types?limit=200  → PostgreSQL asset_id"
  else
    info "Resolving the PostgreSQL datasource type …"
    _resolve_datasource_type || true
  fi

  _build_conn_payload

  if $DRY_RUN; then
    dryrun "POST https://${CPD_HOST}/v2/connections?project_id=${PROJECT_ID}"
    dryrun "${CONN_PAYLOAD}"
  else
    # Pre-check, not error-handling. Connection names are NOT unique server-side
    # (cpdctl even ships a "found more than one connection with query" error), so
    # a second run of the old code — which only looked for an existing asset
    # *after* matching "already exist|duplicate|conflict" in an error message —
    # quietly created a SECOND 'ibmas-reporting'. Look first instead.
    # entity.name is a documented server-side filter; without it the default
    # limit of 100 plus token-based paging would eventually hide the match.
    info "Checking for an existing 'ibmas-reporting' connection …"
    cpd_curl GET "https://${CPD_HOST}/v2/connections?project_id=${PROJECT_ID}&entity.name=ibmas-reporting"
    if [[ "${_CURL_HTTP}" == "200" ]]; then
      CONN_ID="$(echo "${_CURL_BODY}" | "${PY}" -c "
import sys, json
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(1)
hits = [r for r in d.get('resources', [])
        if (r.get('entity', {}) or {}).get('name') == 'ibmas-reporting']
if len(hits) > 1:
    sys.stderr.write('MULTIPLE\n')
if hits:
    print((hits[0].get('metadata', {}) or {}).get('asset_id', ''))
" 2>/dev/null || true)"
    fi

    if [[ -n "${CONN_ID}" ]]; then
      ok "Connection 'ibmas-reporting' already exists: ${CONN_ID}"
      info "  Not recreating it. To rotate the password in place:"
      info "    PATCH /v2/connections/${CONN_ID}?project_id=${PROJECT_ID}"
      info "    [{\"op\":\"replace\",\"path\":\"/properties/password\",\"value\":\"…\"}]"
    else

    # [ST-4] Use cpd_curl for --max-time and HTTP status capture
    info "Registering connection 'ibmas-reporting' in CPD project …"
    cpd_curl POST "https://${CPD_HOST}/v2/connections?project_id=${PROJECT_ID}" "${CONN_PAYLOAD}"

    CONN_ID="$(echo "${_CURL_BODY}" | "${PY}" -c \
      "import sys,json; d=json.load(sys.stdin); print(d.get('metadata',{}).get('asset_id',''))" 2>/dev/null || true)"

    if [[ -n "${CONN_ID}" ]] && [[ "${CONN_ID}" != "ERROR" ]]; then
      ok "Connection registered: ${CONN_ID}"
    else
      MSG="$(echo "${_CURL_BODY}" | "${PY}" -c \
        "import sys,json; d=json.load(sys.stdin); e=d.get('errors',[{}]); print(e[0].get('message','') if e else '')" 2>/dev/null || true)"
      debug "Connection create HTTP ${_CURL_HTTP}, message: ${MSG}"

      # Duplicate: match on the HTTP status as well as the message. Relying on
      # the message alone missed a bare 409 with an empty/unparsed body, which
      # then fell through to "registration failed" even though the connection
      # was there all along.
      if [[ "${_CURL_HTTP}" == "409" ]] \
         || echo "${MSG}" | grep -qi "already exist\|duplicate\|conflict"; then
        warn "Connection 'ibmas-reporting' already exists — looking up existing asset ID."
        cpd_curl GET "https://${CPD_HOST}/v2/connections?project_id=${PROJECT_ID}"
        CONN_ID="$(echo "${_CURL_BODY}" | "${PY}" -c "
import sys,json
d=json.load(sys.stdin)
for r in d.get('resources',[]):
    if r.get('entity',{}).get('name','') == 'ibmas-reporting':
        print(r['metadata']['asset_id'])
        break
" 2>/dev/null || true)"
        if [[ -n "${CONN_ID}" ]]; then
          ok "Existing connection found: ${CONN_ID}"
        else
          warn "Could not resolve existing connection ID."
          warn "  HTTP ${_CURL_HTTP}  list response: ${_CURL_BODY:0:300}"
        fi
      else
        warn "Connection registration failed (HTTP ${_CURL_HTTP}): ${MSG:-${_CURL_BODY:0:300}}"
        # Print the body verbatim, not just errors[0].message. CPD 5.3 enumerates
        # the offending keys ("Extraneous properties for project: …") — that one
        # line turns a property guess into a one-run diagnosis, and it is exactly
        # what was missing when the project step failed.
        warn "  Raw response body:"
        printf '%s\n' "${_CURL_BODY:0:1200}" >&2
        if [[ "${_CURL_BODY}" == *"xtraneous propert"* ]]; then
          warn "  → the server named the properties it rejects above; drop them from"
          warn "    the properties block. Authoritative list for this connector:"
          warn "    GET /v2/datasource_types/${PG_DATASOURCE_TYPE}?connection_properties=true"
        fi
      fi
    fi

    fi   # existing-connection pre-check
  fi
fi

# ─────────────────────────────────────────────────────────────────────────────
# STEP 4b — Optional CPD Platform Connection (catalog-scoped, reusable across
#           every project/catalog, as opposed to Step 4's project-scoped one)
# ─────────────────────────────────────────────────────────────────────────────
PLATFORM_CONN_ID=""
if $DO_PLATFORM_CONN; then
  step "Step 4b/5 — CPD platform connection 'ibmas-reporting' (Platform assets catalog)"

  if $DRY_RUN; then
    dryrun "GET  https://${CPD_HOST}/v2/catalogs?limit=100  → Platform assets catalog GUID"
    [[ -z "${PG_DATASOURCE_TYPE}" ]] && PG_DATASOURCE_TYPE="${PG_DATASOURCE_NAME}"
    _build_conn_payload
    dryrun "POST https://${CPD_HOST}/v2/connections?catalog_id=<platform catalog>"
    dryrun "${CONN_PAYLOAD}"
  elif _ensure_platform_catalog; then
    # --skip-connection can leave PG_DATASOURCE_TYPE/CONN_PAYLOAD unset if Step 4
    # never ran — resolve/build them here too so this step works standalone.
    [[ -z "${PG_DATASOURCE_TYPE}" ]] && { info "Resolving the PostgreSQL datasource type …"; _resolve_datasource_type || true; }
    [[ -z "${CONN_PAYLOAD:-}" ]] && _build_conn_payload

    info "Checking for an existing 'ibmas-reporting' connection in the platform catalog …"
    cpd_curl GET "https://${CPD_HOST}/v2/connections?catalog_id=${PLATFORM_CATALOG_ID}&entity.name=ibmas-reporting"
    if [[ "${_CURL_HTTP}" == "200" ]]; then
      PLATFORM_CONN_ID="$(echo "${_CURL_BODY}" | "${PY}" -c "
import sys, json
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(1)
hits = [r for r in d.get('resources', [])
        if (r.get('entity', {}) or {}).get('name') == 'ibmas-reporting']
if hits:
    print((hits[0].get('metadata', {}) or {}).get('asset_id', ''))
" 2>/dev/null || true)"
    fi

    if [[ -n "${PLATFORM_CONN_ID}" ]]; then
      ok "Platform connection 'ibmas-reporting' already exists: ${PLATFORM_CONN_ID}"
      info "  Not recreating it. To rotate the password in place:"
      info "    PATCH /v2/connections/${PLATFORM_CONN_ID}?catalog_id=${PLATFORM_CATALOG_ID}"
      info "    [{\"op\":\"replace\",\"path\":\"/properties/password\",\"value\":\"…\"}]"
    else
      info "Registering platform connection 'ibmas-reporting' in catalog ${PLATFORM_CATALOG_ID} …"
      cpd_curl POST "https://${CPD_HOST}/v2/connections?catalog_id=${PLATFORM_CATALOG_ID}" "${CONN_PAYLOAD}"

      PLATFORM_CONN_ID="$(echo "${_CURL_BODY}" | "${PY}" -c \
        "import sys,json; d=json.load(sys.stdin); print(d.get('metadata',{}).get('asset_id',''))" 2>/dev/null || true)"

      if [[ -n "${PLATFORM_CONN_ID}" ]] && [[ "${PLATFORM_CONN_ID}" != "ERROR" ]]; then
        ok "Platform connection registered: ${PLATFORM_CONN_ID}"
      else
        MSG="$(echo "${_CURL_BODY}" | "${PY}" -c \
          "import sys,json; d=json.load(sys.stdin); e=d.get('errors',[{}]); print(e[0].get('message','') if e else '')" 2>/dev/null || true)"
        debug "Platform connection create HTTP ${_CURL_HTTP}, message: ${MSG}"

        if [[ "${_CURL_HTTP}" == "409" ]] \
           || echo "${MSG}" | grep -qi "already exist\|duplicate\|conflict"; then
          warn "Platform connection 'ibmas-reporting' already exists — looking up existing asset ID."
          cpd_curl GET "https://${CPD_HOST}/v2/connections?catalog_id=${PLATFORM_CATALOG_ID}"
          PLATFORM_CONN_ID="$(echo "${_CURL_BODY}" | "${PY}" -c "
import sys,json
d=json.load(sys.stdin)
for r in d.get('resources',[]):
    if r.get('entity',{}).get('name','') == 'ibmas-reporting':
        print(r['metadata']['asset_id'])
        break
" 2>/dev/null || true)"
          if [[ -n "${PLATFORM_CONN_ID}" ]]; then
            ok "Existing platform connection found: ${PLATFORM_CONN_ID}"
          else
            warn "Could not resolve existing platform connection ID."
            warn "  HTTP ${_CURL_HTTP}  list response: ${_CURL_BODY:0:300}"
          fi
        else
          warn "Platform connection registration failed (HTTP ${_CURL_HTTP}): ${MSG:-${_CURL_BODY:0:300}}"
          warn "  Raw response body:"
          printf '%s\n' "${_CURL_BODY:0:1200}" >&2
        fi
      fi
    fi
  fi
fi

# ─────────────────────────────────────────────────────────────────────────────
# STEP 5 — Optional DSD
# ─────────────────────────────────────────────────────────────────────────────
# A CPD "Data Source Definition" is NOT the dedicated REST resource its name
# suggests — confirmed live (2026-08-20) that POST/GET /v2/datasource_definitions
# is a routing-level 404 on this cluster (plain-text "Error 404 - Not Found",
# not even a CAMS JSON error — nothing is wired to that path at all). A DSD is
# a generic catalog asset of type 'ibm_data_source' (GET /v2/asset_types/
# data_source_definition itself 404s with "AssetType 'data_source_definition'
# does not exist"; 'ibm_data_source' is the real one, found by listing all 111
# registered asset types). Its scope_restrictions lock it to the Platform
# assets catalog only, so it always needs _ensure_platform_catalog — never a
# project_id or an arbitrary catalog_id.
DSD_ID=""
if $DO_DSD; then
  step "Step 5/5 — CPD Data Source Definition 'IBMAS-Reporting-Postgres-DSD'"

  if $DRY_RUN; then
    dryrun "GET  https://${CPD_HOST}/v2/catalogs?limit=100  → Platform assets catalog GUID"
    [[ -z "${PG_DATASOURCE_TYPE}" ]] && PG_DATASOURCE_TYPE="${PG_DATASOURCE_NAME}"
    dryrun "POST https://${CPD_HOST}/v2/assets?catalog_id=<platform catalog>"
    dryrun '{"metadata":{"name":"IBMAS-Reporting-Postgres-DSD","asset_type":"ibm_data_source",...},"entity":{"ibm_data_source":{"data_source_type_id":"'"${PG_DATASOURCE_TYPE}"'","data_source_endpoints":{"values":[{"host":"'"${PG_HOST}"'","port":'"${PG_PORT}"'}]}}}}'
    [[ -n "${EXTERNAL_URL}" ]] && \
      dryrun "Would also create external DSD with host=${EXTERNAL_URL%%:*} port=${EXTERNAL_URL##*:}"
  elif _ensure_platform_catalog; then
    # --skip-connection can leave PG_DATASOURCE_TYPE unset if Step 4 never ran
    # (Step 4b may not have run either, if --platform-connection wasn't given)
    # — resolve it here too. The DSD's data_source_type_id is the SAME
    # datasource-type UUID the connection steps use, just under a different
    # field name; reused, not re-resolved from scratch.
    [[ -z "${PG_DATASOURCE_TYPE}" ]] && { info "Resolving the PostgreSQL datasource type …"; _resolve_datasource_type || true; }

    # asset_type=ibm_data_source's schema, confirmed live: port is a NUMBER
    # (not a string, unlike the connection asset's properties.port), and there
    # is no "database" field at all — a DSD is a host/port endpoint only; the
    # database name lives on the connection asset created in Step 4/4b.
    _build_dsd_payload() {
      "${PY}" -c '
import json, sys
(name, desc, dstype_id, host, port) = sys.argv[1:6]
print(json.dumps({
    "metadata": {
        "name": name,
        "description": desc,
        "asset_type": "ibm_data_source",
        "origin_country": "us",
        "rov": {"mode": 0},
    },
    "entity": {
        "ibm_data_source": {
            "data_source_state": "ACTIVE",
            "data_source_type_id": dstype_id,
            "data_source_endpoints": {"values": [{"host": host, "port": int(port)}]},
        }
    },
}))' "$@"
    }

    # Pre-check by name via the asset-type search endpoint (the only confirmed-
    # working way to query ibm_data_source assets — a plain
    # GET /v2/assets?catalog_id=…&entity.name=… is not documented for generic
    # asset types the way it is for connections).
    _find_dsd_by_name() {
      local dsd_name="$1"
      cpd_curl POST "https://${CPD_HOST}/v2/asset_types/ibm_data_source/search?catalog_id=${PLATFORM_CATALOG_ID}" \
        "{\"query\": \"asset.name:${dsd_name}\"}"
      [[ "${_CURL_HTTP}" != "200" ]] && return 1
      echo "${_CURL_BODY}" | "${PY}" -c "
import sys, json
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(1)
for r in d.get('resources', []) or []:
    if (r.get('metadata', {}) or {}).get('name') == '${dsd_name}':
        print((r.get('metadata', {}) or {}).get('asset_id', ''))
        break
" 2>/dev/null || true
    }

    info "Checking for an existing 'IBMAS-Reporting-Postgres-DSD' asset …"
    DSD_ID="$(_find_dsd_by_name "IBMAS-Reporting-Postgres-DSD")"

    if [[ -n "${DSD_ID}" ]]; then
      ok "DSD 'IBMAS-Reporting-Postgres-DSD' already exists: ${DSD_ID}"
    else
      DSD_PAYLOAD="$(_build_dsd_payload \
        "IBMAS-Reporting-Postgres-DSD" \
        "Standalone PostgreSQL reporting instance — ibmas_reporting database" \
        "${PG_DATASOURCE_TYPE}" "${PG_HOST}" "${PG_PORT}")"

      info "Creating internal DSD 'IBMAS-Reporting-Postgres-DSD' …"
      cpd_curl POST "https://${CPD_HOST}/v2/assets?catalog_id=${PLATFORM_CATALOG_ID}" "${DSD_PAYLOAD}"

      DSD_ID="$(echo "${_CURL_BODY}" | "${PY}" -c \
        "import sys,json; d=json.load(sys.stdin); print(d.get('metadata',{}).get('asset_id',''))" 2>/dev/null || true)"

      if [[ -n "${DSD_ID}" ]] && [[ "${DSD_ID}" != "ERROR" ]]; then
        ok "DSD registered: IBMAS-Reporting-Postgres-DSD (${DSD_ID})"
      else
        DSD_MSG="$(echo "${_CURL_BODY}" | "${PY}" -c \
          "import sys,json; d=json.load(sys.stdin); e=d.get('errors',[{}]); print(e[0].get('message','') if e else '')" 2>/dev/null || true)"
        warn "DSD registration failed (HTTP ${_CURL_HTTP}): ${DSD_MSG:-${_CURL_BODY:0:300}}"
        printf '%s\n' "${_CURL_BODY:0:1200}" >&2
      fi
    fi

    # External DSD if requested
    if [[ -n "${EXTERNAL_URL}" ]]; then
      EXT_HOST="${EXTERNAL_URL%%:*}"
      EXT_PORT="${EXTERNAL_URL##*:}"

      EXT_DSD_ID="$(_find_dsd_by_name "IBMAS-Reporting-Postgres-External-DSD")"
      if [[ -n "${EXT_DSD_ID}" ]]; then
        ok "External DSD 'IBMAS-Reporting-Postgres-External-DSD' already exists: ${EXT_DSD_ID}"
      else
        info "Creating external DSD for ${EXT_HOST}:${EXT_PORT} …"
        EXT_DSD_PAYLOAD="$(_build_dsd_payload \
          "IBMAS-Reporting-Postgres-External-DSD" \
          "External / workstation access to ibmas_reporting (port-forward: oc -n ${NS} port-forward pod/\${PG_POD} ${EXT_PORT}:5432)" \
          "${PG_DATASOURCE_TYPE}" "${EXT_HOST}" "${EXT_PORT}")"

        cpd_curl POST "https://${CPD_HOST}/v2/assets?catalog_id=${PLATFORM_CATALOG_ID}" "${EXT_DSD_PAYLOAD}"

        EXT_DSD_ID="$(echo "${_CURL_BODY}" | "${PY}" -c \
          "import sys,json; d=json.load(sys.stdin); print(d.get('metadata',{}).get('asset_id',''))" 2>/dev/null || true)"

        if [[ -n "${EXT_DSD_ID}" ]] && [[ "${EXT_DSD_ID}" != "ERROR" ]]; then
          ok "External DSD registered: ${EXT_HOST}:${EXT_PORT} (${EXT_DSD_ID})"
        else
          warn "External DSD registration failed (HTTP ${_CURL_HTTP}): ${_CURL_BODY:0:300}"
        fi
      fi
    fi
  fi
fi

# ─────────────────────────────────────────────────────────────────────────────
# STEP 6 — Update .env
# ─────────────────────────────────────────────────────────────────────────────
step "Updating .env"
ENV_FILE="${REPO}/.env"

# [ST-7] _set_env: portable sed -i for both GNU (Linux) and BSD (macOS)
_set_env() {
  local k="$1" v="$2"
  if grep -q "^${k}=" "${ENV_FILE}" 2>/dev/null; then
    debug "  Updating .env: ${k}=…"
    # BSD sed (macOS) requires an explicit extension argument; use '' for in-place with no backup.
    # GNU sed (Linux) accepts -i without argument. Detect by OS.
    if [[ "$(uname -s)" == "Darwin" ]]; then
      sed -i '' "s|^${k}=.*|${k}=${v}|" "${ENV_FILE}"
    else
      sed -i "s|^${k}=.*|${k}=${v}|" "${ENV_FILE}"
    fi
  else
    debug "  Appending to .env: ${k}=…"
    echo "${k}=${v}" >> "${ENV_FILE}"
  fi
}

if [[ -f "${ENV_FILE}" ]] && ! $DRY_RUN; then
  _set_env "PG_HOST"             "${PG_HOST}"
  _set_env "PG_PORT"             "${PG_PORT}"
  _set_env "PG_DATABASE"         "${REPORT_DB}"
  _set_env "PG_USER"             "${REPORT_USER}"
  _set_env "PG_PASSWORD"         "${REPORT_PASS}"
  # 'require', not 'disable': PGO's pg_hba.conf has no plain-'host' permit rule
  # for regular users, so an unencrypted client is refused outright.
  _set_env "PG_SSL_MODE"         "${PG_SSL_MODE}"
  _set_env "PG_GOLD_SCHEMA"      "dbt_demo_gold"
  _set_env "PG_REPORTING_SCHEMA" "${REPORT_SCHEMA}"
  ok "PG_* vars written to .env"
else
  $DRY_RUN && dryrun ".env would be updated with PG_* vars" || warn ".env not found — skipping .env update."
fi

# ─────────────────────────────────────────────────────────────────────────────
# SUMMARY
# ─────────────────────────────────────────────────────────────────────────────
W=74
line() { printf "${BOLD}║${RESET}  %-26s : %-${W}s${BOLD}║${RESET}\n" "$1" "$2"; }
echo
INNER=$((W + 33))
# Centre the title arithmetically — the old version hardcoded 17/21 spaces and a
# fixed database name, so any --db override knocked the box out of alignment.
TITLE="${REPORT_DB} — Connection Details"
LPAD=$(( (INNER - ${#TITLE}) / 2 ))
RPAD=$(( INNER - LPAD - ${#TITLE} ))
echo -e "${BOLD}╔$(printf '═%.0s' $(seq 1 "${INNER}"))╗${RESET}"
printf '%s║%*s%s%*s║%s\n' "${BOLD}" "${LPAD}" "" "${TITLE}" "${RPAD}" "" "${RESET}"
echo -e "${BOLD}╠$(printf '═%.0s' $(seq 1 "${INNER}"))╣${RESET}"
line "Host (in-cluster)"    "${PG_HOST}"
line "Port"                 "${PG_PORT}"
line "Database"             "${REPORT_DB}"
line "Schema"               "${REPORT_SCHEMA}"
line "User"                 "${REPORT_USER}"
line "Password"             "${REPORT_PASS}"
line "SSL"                  "${PG_SSL_MODE} (PGO pg_hba permits hostssl only)"
line "RW Service"           "${PG_SVC}"
line "Primary pod"          "${PG_POD:-<not resolved>}"
line "K8s Secret"           "${NS}/${SECRET_NAME}"
[[ -n "${CONN_ID}" ]]          && line "CPD Connection ID"          "${CONN_ID}"
[[ -n "${PLATFORM_CONN_ID}" ]] && line "CPD Platform Connection ID" "${PLATFORM_CONN_ID}"
[[ -n "${DSD_ID}"  ]]          && line "CPD DSD ID"                 "${DSD_ID}"
echo -e "${BOLD}╠$(printf '═%.0s' $(seq 1 "${INNER}"))╣${RESET}"
line "JDBC URL" "jdbc:postgresql://${PG_HOST}:${PG_PORT}/${REPORT_DB}?ssl=true&sslmode=${PG_SSL_MODE}"
echo -e "${BOLD}╚$(printf '═%.0s' $(seq 1 "${INNER}"))╝${RESET}"

cat <<EOF

${BOLD}Retrieve password at any time:${RESET}
  oc -n ${NS} get secret ${SECRET_NAME} \\
    -o jsonpath='{.data.password}' | base64 -d && echo

${BOLD}Workstation access (port-forward):${RESET}
  # Target the POD, not svc/${PG_SVC} — Crunchy's *-primary Service has no pod
  # selector (it's bound via a hand-managed Endpoints object that follows the
  # Patroni leader), so 'oc port-forward svc/${PG_SVC}' fails outright with
  # "Service is defined without a selector". Confirmed live.
  oc -n ${NS} port-forward pod/${PG_POD:-<primary-pod>} 15432:${PG_PORT} &
  # PG_SSL_MODE stays 'require' even over the tunnel: the TLS session terminates
  # at Postgres, not at the port-forward, and PGO rejects unencrypted clients.
  PG_HOST=localhost PG_PORT=15432 PG_SSL_MODE=${PG_SSL_MODE} \\
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
