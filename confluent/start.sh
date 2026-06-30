#!/usr/bin/env bash
# =============================================================================
#  start.sh — single, argument-driven entrypoint for the Confluent stack
#
#  Location  : confluent/start.sh
#  Repository: https://github.com/aseelert/ibmas-watsonxdata-dbt
#  Project   : watsonx.data · dbt · Spark · Confluent medallion demo
#  Author    : Alexander Seelert — IBM Customer Success Engineer
#  Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.
#
#  Changelog :
#    v1.0 (2026-06-26) — Initial version. Consolidated the previous one-shot
#      script + the loose "do this next" instructions into ONE subcommand-
#      driven orchestrator: --all/--stack/--silver/--gold/--status/--reset/
#      --stop, with -y / --dry-run, an ERR trap and confirm() prompts. Sources
#      scripts/lib/log.sh. Fixed the old topic-count / step-numbering / stop-
#      guidance inaccuracies (the stack creates 8 topics, not 4).
# =============================================================================
#
#  WHAT THIS SCRIPT IS (for an 18-year-old learner):
#  -------------------------------------------------
#  The Confluent path of this demo has several moving parts that you used to
#  start "by hand" in a fixed order. This one script wraps ALL of them behind
#  named actions so you never have to remember the order or the exact docker
#  commands again. Pick an action, optionally add -y (no prompts) or
#  --dry-run (preview only), and go.
#
#  THE PIPELINE (raw CSV  →  Kafka  →  Flink silver  →  Spark/DataStage gold):
#    raw_*.csv seeds  ──produce──▶  Kafka topics  ──Flink SQL──▶  Iceberg
#    silver (confluent_demo_silver)  ──Spark|DataStage──▶  confluent_demo_gold
#
#  See confluent/NAMING.md for the canonical schema/table names and the
#  "all three engines reach the SAME gold" parity contract.
# =============================================================================
set -euo pipefail

# ---------------------------------------------------------------------------
# Locate ourselves and the repo root, then pull in the shared helper library
# (info/success/warn/error/step, confirm(), run(), install_err_trap, load_env).
# Everything in this repo logs in the same style by borrowing that toolbox.
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"   # …/confluent
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"                  # repo root
# shellcheck source=../scripts/lib/log.sh
source "${REPO_ROOT}/scripts/lib/log.sh"

install_err_trap          # "Command failed (exit N) at file:line → cmd"
cd "$REPO_ROOT"           # docker compose / relative paths resolve from here
load_env                  # export everything from <repo>/.env (CONFLUENT_*, WXD_*)

# ---------------------------------------------------------------------------
# Constants — the long-running services that make up the local stack. The
# one-shot helpers (confluent-kafka-init, confluent-schema-prep, …) are NOT
# listed here: they run, do their job, and exit on their own.
# ---------------------------------------------------------------------------
COMPOSE_SERVICES=(
  confluent-kafka
  confluent-schema-registry
  confluent-kafbat-ui
  confluent-iceberg-rest
  confluent-flink-jobmanager
  confluent-flink-taskmanager
  confluent-flink-sql-gateway
)

VENV_PYTHON="${REPO_ROOT}/.venv/bin/python"
VENV_PIP="${REPO_ROOT}/.venv/bin/pip"

# Kafka bootstrap reachable from the host (external listener).
KAFKA_BOOTSTRAP="${CONFLUENT_KAFKA_BOOTSTRAP:-localhost:29092}"

# Default gold engine comes from .env (CONFLUENT_GOLD_ENGINE), falling back to
# "spark". --engine on the command line overrides it for a single run.
GOLD_ENGINE="${CONFLUENT_GOLD_ENGINE:-spark}"

# Where the two gold-building entrypoints live (owned by sibling scripts).
GOLD_SPARK_SCRIPT="confluent/scripts/submit_confluent_gold.py"
GOLD_DATASTAGE_SCRIPT="confluent/scripts/create_datastage_flow.py"

# =============================================================================
#  HELP
# =============================================================================
usage() {
  cat <<'EOF'

  confluent/start.sh — one entrypoint for the Confluent streaming stack

  USAGE
    bash confluent/start.sh [ACTION] [OPTIONS]

  ACTIONS (pick one; default is --all)
    --all            Full local bring-up: virtualenv → build Flink image →
                     start services → create topics → produce the 4 seed CSVs
                     into Kafka → print a status summary. (default)
    --stack          ONLY start the long-running services (virtualenv + Flink
                     image + the 7 containers). No topic creation, no seeding.
    --silver         Run the Flink SILVER pipeline: create the Iceberg schemas,
                     submit confluent/flink/sql/silver_jobs.sql, and register
                     the resulting confluent_demo_silver tables in watsonx.data.
                     REQUIRES a reachable MinIO Route — run
                       bash confluent/scripts/expose_minio_route.sh
                     first and paste WXD_OBJECT_STORE_ENDPOINT into .env.
    --gold           Build the confluent_demo_gold marts from the silver tables.
                     Uses --engine (default: $CONFLUENT_GOLD_ENGINE, else spark).
    --status         Show service health, per-topic message counts and UI URLs.
    --reset          Tear down the Confluent surface (calls
                     scripts/reset_demo.sh --confluent). DESTRUCTIVE.
    --stop           Stop the 7 long-running containers (keeps data/volumes).

  OPTIONS
    --engine ENGINE  With --gold: 'spark' (default) or 'datastage'. Both write
                     the SAME confluent_demo_gold marts — see confluent/NAMING.md.
    -y, --yes        Answer "yes" to every confirmation prompt (unattended).
    --dry-run        Print what WOULD run; change nothing. Implies non-interactive.
    -h, --help       Show this help and exit.

  EXAMPLES
    bash confluent/start.sh                       # full local bring-up + seed
    bash confluent/start.sh --stack               # just start the containers
    bash confluent/start.sh --silver              # submit the Flink silver jobs
    bash confluent/start.sh --gold --engine datastage
    bash confluent/start.sh --status              # health + topic counts + UIs
    bash confluent/start.sh --reset -y            # wipe Confluent surface, no prompt
    bash confluent/start.sh --stop --dry-run      # preview the teardown

  PORTS / UIs
    29092  Kafka (host listener)        http://localhost:28080  Kafbat UI
    28081  Schema Registry              http://localhost:28085  Flink Web UI
    28181  Iceberg REST catalog         http://localhost:28083  Flink SQL Gateway

  See confluent/NAMING.md for schemas, table names and the parity contract.
EOF
}

# =============================================================================
#  SMALL WRAPPERS
# =============================================================================

# compose <args...>
#   docker compose, but (a) honours --dry-run (prints instead of running) and
#   (b) strips the noisy "time=…"/"obsolete version" lines compose prints to
#   stderr so our log stays clean.
compose() {
  if [[ "${DRY_RUN:-false}" == "true" ]]; then
    echo "${__C_YELLOW}  + docker compose $*${__C_RESET} ${__C_BOLD}(dry-run — not executed)${__C_RESET}" >&2
    return 0
  fi
  docker compose "$@" 2>&1 | grep -vE '^time=|obsolete' || true
}

# wait_kafka_healthy
#   Block until the Kafka container reports healthy. Skipped under --dry-run
#   (there is nothing running to wait for, and we must not loop forever).
wait_kafka_healthy() {
  if [[ "${DRY_RUN:-false}" == "true" ]]; then
    info "(dry-run) would wait for confluent-kafka to become healthy"
    return 0
  fi
  info "Waiting for confluent-kafka to be healthy ..."
  until docker inspect --format='{{.State.Health.Status}}' confluent-kafka 2>/dev/null | grep -q "healthy"; do
    printf "."
    sleep 2
  done
  echo ""
  success "Kafka healthy"
}

# =============================================================================
#  BUILDING BLOCKS (each is one reusable phase)
# =============================================================================

# ensure_venv — create .venv if missing and (idempotently) install requirements.
ensure_venv() {
  step "Python virtual environment (.venv)"
  if [[ ! -x "$VENV_PYTHON" ]]; then
    info ".venv not found — creating it with python3 ..."
    run python3 -m venv "${REPO_ROOT}/.venv"
    success ".venv created"
  fi
  info "Installing requirements into .venv (idempotent) ..."
  run "$VENV_PIP" install --quiet -r requirements.txt
  if [[ "${DRY_RUN:-false}" != "true" ]]; then
    success ".venv ready ($(${VENV_PYTHON} --version 2>&1))"
  fi
}

# build_image — build the custom Flink image, skipping if it already exists.
build_image() {
  step "Custom Flink image (wxd-flink:1.20)"
  if docker image inspect wxd-flink:1.20 > /dev/null 2>&1; then
    success "wxd-flink:1.20 already built — skipping (docker rmi wxd-flink:1.20 to force a rebuild)"
  else
    info "Building confluent/flink/Dockerfile ..."
    run docker build -t wxd-flink:1.20 -f confluent/flink/Dockerfile confluent/flink/
    success "wxd-flink:1.20 built"
  fi
}

# start_services — bring up the 7 long-running containers.
start_services() {
  step "Start services (Kafka, Schema Registry, Kafbat UI, Iceberg REST, Flink JM/TM/Gateway)"
  compose up -d "${COMPOSE_SERVICES[@]}"
  success "Service containers requested"
}

# create_topics — run the one-shot init container that creates the topics.
#   NOTE: create-topics.sh makes 8 topics — the 4 raw_* entity topics AND the
#   4 silver_* topics — not 4. (This is the inaccuracy the old script had.)
create_topics() {
  step "Create Kafka topics (4 raw_* + 4 silver_* = 8 topics, idempotent)"
  compose run --rm confluent-kafka-init
  success "Topics ensured"
}

# seed_kafka — produce the 4 raw seed CSVs (50+20+500+1134 = 1704 rows) to Kafka.
seed_kafka() {
  step "Produce seed CSV rows to Kafka (1704 rows across the 4 raw_* topics)"
  info "Running ingest_csv_to_kafka.py via .venv ..."
  run "$VENV_PYTHON" confluent/scripts/ingest_csv_to_kafka.py \
    --bootstrap-servers "$KAFKA_BOOTSTRAP" \
    --csv-dir seeds/
  success "Seed rows produced"
}

# submit_silver — the watsonx.data profile one-shots: create Iceberg schemas,
#   submit the Flink silver SQL, then register the silver tables in watsonx.data.
submit_silver() {
  step "Flink SILVER pipeline → confluent_demo_silver in watsonx.data"
  warn "This needs a reachable MinIO Route. If you have not done so yet, run:"
  warn "    bash confluent/scripts/expose_minio_route.sh"
  warn "and paste the printed WXD_OBJECT_STORE_ENDPOINT into your .env."
  if [[ -z "${WXD_OBJECT_STORE_ENDPOINT:-}" ]]; then
    warn "WXD_OBJECT_STORE_ENDPOINT is not set in your environment/.env."
  fi
  compose --profile watsonxdata up -d \
    confluent-schema-prep \
    confluent-flink-runner \
    confluent-prep
  success "Silver one-shot containers launched"
  info "Follow their logs with:"
  info "    docker logs -f confluent-schema-prep"
  info "    docker logs -f confluent-flink-runner"
  info "    docker logs -f confluent-prep"
}

# build_gold — build confluent_demo_gold using the chosen engine.
#   spark     → confluent/scripts/submit_confluent_gold.py
#   datastage → confluent/scripts/create_datastage_flow.py
#   Both produce the SAME confluent_gold_* marts (see NAMING.md).
build_gold() {
  step "Build confluent_demo_gold marts (engine: ${GOLD_ENGINE})"

  local script
  case "$GOLD_ENGINE" in
    spark)     script="$GOLD_SPARK_SCRIPT" ;;
    datastage) script="$GOLD_DATASTAGE_SCRIPT" ;;
    *)
      error "Unknown --engine '${GOLD_ENGINE}'. Use 'spark' or 'datastage'."
      exit 2
      ;;
  esac

  if [[ ! -f "$script" ]]; then
    error "Gold builder not found: ${script}"
    error "(The ${GOLD_ENGINE} engine entrypoint is provided by a sibling script.)"
    exit 1
  fi

  info "Engine '${GOLD_ENGINE}' → ${script}"
  run "$VENV_PYTHON" "$script"
  success "Gold build (${GOLD_ENGINE}) finished — target schema: ${CONFLUENT_GOLD_SCHEMA:-confluent_demo_gold}"
}

# show_status — read-only: service health, topic message counts, UI URLs.
#   Safe to run anytime (never gated by --dry-run; it changes nothing).
show_status() {
  step "Confluent stack status"

  echo ""
  echo "  Services:"
  docker compose ps --format "  {{.Name}}: {{.Status}}" 2>/dev/null \
    | grep confluent | sort || info "(no confluent containers running)"

  echo ""
  echo "  Topic message counts:"
  if [[ -x "$VENV_PYTHON" ]]; then
    curl -sf "http://localhost:28080/api/clusters/confluent-local/topics?showInternal=false" \
      | "$VENV_PYTHON" -c "
import sys, json
data = json.load(sys.stdin)
for t in sorted(data.get('topics', []), key=lambda x: x['name']):
    msgs = sum(p.get('offsetMax', 0) - p.get('offsetMin', 0) for p in t.get('partitions', []))
    print(f'    {t[\"name\"]:<25} {msgs:>5} messages')
" 2>/dev/null || info "(Kafbat UI not ready yet — try again in a few seconds)"
  else
    info "(.venv missing — run 'bash confluent/start.sh --stack' first to read topic counts)"
  fi

  echo ""
  echo "  UIs:"
  echo "    Kafbat UI (Kafka)    →  http://localhost:28080"
  echo "    Flink Web UI         →  http://localhost:28085"
  echo "    Flink SQL Gateway    →  http://localhost:28083"
  echo "    Schema Registry      →  http://localhost:28081"
  echo "    Iceberg REST catalog →  http://localhost:28181"
  echo ""
}

# do_reset — DESTRUCTIVE. Delegates to the repo-wide reset, scoped to Confluent.
do_reset() {
  step "Reset the Confluent surface"
  if ! confirm "This will tear down the Confluent demo surface. Continue?"; then
    warn "Reset cancelled."
    return 0
  fi
  local args=(--confluent)
  [[ "${DRY_RUN:-false}"   == "true" ]] && args+=(--dry-run)
  [[ "${ASSUME_YES:-false}" == "true" ]] && args+=(-y)
  # reset_demo.sh has its OWN dry-run handling, so call it directly (not via run).
  bash scripts/reset_demo.sh "${args[@]}"
  success "Reset delegated to scripts/reset_demo.sh"
}

# do_stop — stop the long-running containers (data/volumes are preserved).
do_stop() {
  step "Stop the Confluent services"
  if ! confirm "Stop the 7 long-running Confluent containers? (data is kept)"; then
    warn "Stop cancelled."
    return 0
  fi
  compose stop "${COMPOSE_SERVICES[@]}"
  success "Services stopped. Restart with: bash confluent/start.sh --stack"
  info "To remove containers + data too, use: bash confluent/start.sh --reset"
}

# =============================================================================
#  COMPOSITE ACTIONS
# =============================================================================

# action_all — the full happy path (the old default behaviour, corrected).
action_all() {
  ensure_venv
  build_image
  start_services
  wait_kafka_healthy
  create_topics
  seed_kafka
  echo ""
  echo "${__C_BOLD}═══════════════════════════════════════════════════════════${__C_RESET}"
  echo "${__C_BOLD}  Confluent Streaming Stack — READY${__C_RESET}"
  echo "${__C_BOLD}═══════════════════════════════════════════════════════════${__C_RESET}"
  # Give Kafbat a moment to scrape offsets before we read them.
  [[ "${DRY_RUN:-false}" == "true" ]] || sleep 3
  show_status
  info "Next: 'bash confluent/start.sh --silver' then '--gold' to finish the pipeline."
}

# action_stack — only start the containers (plus prerequisites).
action_stack() {
  ensure_venv
  build_image
  start_services
  success "Services started. Topics/seed NOT created — run '--all' for the full bring-up."
}

# =============================================================================
#  ARGUMENT PARSING
# =============================================================================
ACTION="all"          # default action when none is given
ACTION_SET=false      # detect conflicting/duplicate action flags
ASSUME_YES=false
DRY_RUN=false

set_action() {
  if [[ "$ACTION_SET" == "true" ]]; then
    error "Only one action may be given at a time (got '--$1' after '--$ACTION')."
    error "Run 'bash confluent/start.sh --help' for usage."
    exit 2
  fi
  ACTION="$1"
  ACTION_SET=true
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --all)     set_action all ;;
    --stack)   set_action stack ;;
    --silver)  set_action silver ;;
    --gold)    set_action gold ;;
    --status)  set_action status ;;
    --reset)   set_action reset ;;
    --stop)    set_action stop ;;
    --engine)
      shift
      [[ $# -gt 0 ]] || { error "--engine requires a value (spark|datastage)."; exit 2; }
      GOLD_ENGINE="$1"
      ;;
    --engine=*) GOLD_ENGINE="${1#*=}" ;;
    -y|--yes)   ASSUME_YES=true ;;
    --dry-run)  DRY_RUN=true ;;
    -h|--help)  usage; exit 0 ;;
    *)
      error "Unknown argument: $1"
      error "Run 'bash confluent/start.sh --help' for usage."
      exit 2
      ;;
  esac
  shift
done

# Make the two switches visible to confirm()/run() in log.sh (they read globals).
export ASSUME_YES DRY_RUN

[[ "${DRY_RUN:-false}" == "true" ]] && warn "DRY-RUN mode: no changes will be made."

# =============================================================================
#  DISPATCH
# =============================================================================
case "$ACTION" in
  all)     action_all ;;
  stack)   action_stack ;;
  silver)  ensure_venv; submit_silver ;;
  gold)    ensure_venv; build_gold ;;
  status)  show_status ;;
  reset)   do_reset ;;
  stop)    do_stop ;;
  *)       error "Internal error: unknown action '$ACTION'"; exit 99 ;;
esac
