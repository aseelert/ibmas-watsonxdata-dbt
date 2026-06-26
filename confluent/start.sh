#!/usr/bin/env bash
# =============================================================================
#  confluent/start.sh — one-shot setup for the Confluent streaming stack
#
#  Run once from the repo root:
#    bash confluent/start.sh
#
#  What it does (in order):
#    1. Ensure .venv exists and confluent-kafka is installed
#    2. Build the custom Flink image (wxd-flink:1.20) — skipped if already built
#    3. Start all 7 long-running services (Kafka, Schema Registry, Kafbat UI,
#       Iceberg REST, Flink JobManager, TaskManager, SQL Gateway)
#    4. Wait for Kafka to be healthy, then create the 4 raw topics via kafka-init
#    5. Produce all 1704 seed CSV rows to Kafka (50+20+500+1134)
#    6. Print a status summary + UI URLs
#
#  To also run the watsonx.data Flink silver jobs (requires OpenShift Route):
#    bash confluent/start.sh --watsonxdata
#
#  Ports:
#    29092  Kafka (external)        http://localhost:28080  Kafbat UI
#    28081  Schema Registry         http://localhost:28085  Flink Web UI
#    28181  Iceberg REST catalog    http://localhost:28083  Flink SQL Gateway
# =============================================================================
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

WATSONXDATA_MODE=false
if [[ "${1:-}" == "--watsonxdata" ]]; then
  WATSONXDATA_MODE=true
fi

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
info()    { echo "  [INFO]  $*"; }
success() { echo "  [OK]    $*"; }
warn()    { echo "  [WARN]  $*"; }
step()    { echo ""; echo "▶  $*"; }

# ---------------------------------------------------------------------------
# Step 1 — Ensure .venv exists with all dependencies
# ---------------------------------------------------------------------------
step "Step 1/5 — Check Python virtual environment (.venv)"

VENV_PYTHON="${REPO_ROOT}/.venv/bin/python"
VENV_PIP="${REPO_ROOT}/.venv/bin/pip"

if [[ ! -x "$VENV_PYTHON" ]]; then
  info ".venv not found — creating it with python3 ..."
  python3 -m venv "${REPO_ROOT}/.venv"
  success ".venv created"
fi

# Install / sync all requirements (idempotent — pip skips already-installed packages)
info "Installing requirements into .venv ..."
"$VENV_PIP" install --quiet -r requirements.txt
success ".venv ready ($(${VENV_PYTHON} --version))"

# ---------------------------------------------------------------------------
# Step 2 — Build Flink image (fast if already cached)
# ---------------------------------------------------------------------------
step "Step 2/5 — Build custom Flink image (wxd-flink:1.20)"

if docker image inspect wxd-flink:1.20 > /dev/null 2>&1; then
  success "wxd-flink:1.20 already built — skipping (use 'docker rmi wxd-flink:1.20' to force rebuild)"
else
  info "Building confluent/flink/Dockerfile ..."
  docker build -t wxd-flink:1.20 -f confluent/flink/Dockerfile confluent/flink/
  success "wxd-flink:1.20 built"
fi

# ---------------------------------------------------------------------------
# Step 3 — Start long-running services
# ---------------------------------------------------------------------------
step "Step 3/5 — Start Kafka, Schema Registry, Kafbat UI, Iceberg REST, Flink"

COMPOSE_SERVICES=(
  confluent-kafka
  confluent-schema-registry
  confluent-kafbat-ui
  confluent-iceberg-rest
  confluent-flink-jobmanager
  confluent-flink-taskmanager
  confluent-flink-sql-gateway
)

docker compose up -d "${COMPOSE_SERVICES[@]}" 2>&1 | grep -v "^time=" | grep -v "obsolete"

# ---------------------------------------------------------------------------
# Step 4 — Wait for Kafka healthy, then run kafka-init (creates 4 topics)
# ---------------------------------------------------------------------------
step "Step 4/5 — Wait for Kafka healthy and create topics"

info "Waiting for confluent-kafka to be healthy ..."
until docker inspect --format='{{.State.Health.Status}}' confluent-kafka 2>/dev/null | grep -q "healthy"; do
  printf "."
  sleep 2
done
echo ""
success "Kafka healthy"

# Run kafka-init (idempotent — topics already exist → no-op)
docker compose run --rm confluent-kafka-init 2>&1 | grep -v "^time=" | grep -v "obsolete"

# ---------------------------------------------------------------------------
# Step 5 — Ingest seed CSVs into Kafka (uses .venv/bin/python on host)
# ---------------------------------------------------------------------------
step "Step 5/5 — Produce seed CSV rows to Kafka topics"

info "Running ingest_csv_to_kafka.py via .venv ..."
"$VENV_PYTHON" confluent/scripts/ingest_csv_to_kafka.py \
  --bootstrap-servers localhost:29092 \
  --csv-dir seeds/

# ---------------------------------------------------------------------------
# Optional Step 6 — watsonx.data profile (Flink silver jobs + register_table)
# ---------------------------------------------------------------------------
if [[ "$WATSONXDATA_MODE" == "true" ]]; then
  step "Step 6/6 — Submit Flink silver jobs + register tables in watsonx.data"
  info "This requires WXD_OBJECT_STORE_ENDPOINT to be a valid OpenShift Route URL."
  info "Run 'bash confluent/scripts/expose_minio_route.sh' first if not done yet."
  docker compose --profile watsonxdata up -d \
    confluent-schema-prep \
    confluent-flink-runner \
    confluent-prep 2>&1 | grep -v "^time=" | grep -v "obsolete"
  info "One-shot containers started — follow logs with:"
  info "  docker logs -f confluent-schema-prep"
  info "  docker logs -f confluent-flink-runner"
  info "  docker logs -f confluent-prep"
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "═══════════════════════════════════════════════════════════"
echo "  Confluent Streaming Stack — READY"
echo "═══════════════════════════════════════════════════════════"
echo ""
echo "  Services:"
docker compose ps --format "  {{.Name}}: {{.Status}}" 2>/dev/null \
  | grep confluent | sort
echo ""
echo "  Topic message counts:"
sleep 3
curl -sf "http://localhost:28080/api/clusters/confluent-local/topics?showInternal=false" \
  | "$VENV_PYTHON" -c "
import sys, json
for t in sorted(json.load(sys.stdin).get('topics',[]), key=lambda x: x['name']):
    msgs = sum(p.get('offsetMax',0)-p.get('offsetMin',0) for p in t.get('partitions',[]))
    print(f'    {t[\"name\"]:<25} {msgs:>5} messages')
" 2>/dev/null || info "(Kafbat UI not yet ready — try again in a few seconds)"
echo ""
echo "  UIs:"
echo "    Kafbat UI (Kafka)    →  http://localhost:28080"
echo "    Flink Web UI         →  http://localhost:28085"
echo "    Flink SQL Gateway    →  http://localhost:28083"
echo "    Schema Registry      →  http://localhost:28081"
echo "    Iceberg REST catalog →  http://localhost:28181"
echo ""
echo "  To stop the confluent stack:"
echo "    docker compose stop confluent-kafka confluent-schema-registry confluent-kafbat-ui \\"
echo "      confluent-iceberg-rest confluent-flink-jobmanager confluent-flink-taskmanager confluent-flink-sql-gateway"
echo ""
