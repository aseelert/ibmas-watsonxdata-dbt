#!/usr/bin/env bash
# -----------------------------------------------------------------------------
#  reset_demo.sh — get the watsonx.data medallion demo back to a 100% clean state.
#
#  Location  : scripts/reset_demo.sh
#  Repository: https://github.com/aseelert/ibmas-watsonxdata-dbt
#  Project   : watsonx.data · dbt · Spark · Confluent medallion demo
#  Author    : Alexander Seelert — IBM Customer Success Engineer
#  Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.
#
#  Changelog :
#    v1.1 (2026-06-26) — Add a --confluent surface (drop the confluent_demo_silver
#      + confluent_demo_gold schemas, delete their MinIO prefixes, and stop/remove
#      the Confluent Docker services + Kafka topics/volume/network); include it in
#      --all. Source scripts/lib/log.sh for shared helpers + an ERR trap.
#    v1.0 (earlier) — Initial version. --docker / --schemas / --minio surfaces.
#
#  WHAT / WHY
#    Tears down the demo so it can be replayed from scratch. The demo spreads
#    state across three INDEPENDENT surfaces — local Docker stacks, watsonx.data
#    schemas (Presto), and object storage (MinIO/S3) — and this script lets you
#    reset any subset of them cleanly, scoped tightly to this demo's own names so
#    it never disturbs unrelated containers, schemas, or bucket contents.
#
#  WHEN TO RUN IT
#    Between demo runs, or whenever a surface is in a bad state. `--docker` is
#    safe any time; `--schemas`/`--minio` undo the dbt + Spark + cpdctl work and
#    should be paired with a fresh re-ingest afterwards. Always available:
#    preview first with `--dry-run`.
#
#  ENV VARS
#    Indirectly via the helper Python scripts it calls (cleanup_watsonxdata.py,
#    cleanup_minio.py), which read the demo's `.env` (WXD_* connection settings,
#    schema prefixes, MinIO/S3 endpoint + credentials).
#
#  PREREQUISITES
#    `docker` for --docker; a working repo `.venv` (boto3, prestodb, dotenv) or a
#    system python for --schemas/--minio; `oc login` to the cluster for --minio
#    (MinIO is reached via an oc port-forward). Missing tools are skipped with a
#    clear message rather than aborting.
#
#  SIDE EFFECTS / EXIT
#    PERMANENTLY removes the selected resources (containers, volumes, images,
#    schemas, object-store files). Prompts for confirmation unless `-y/--yes` or
#    `--dry-run`. Exits 0 on success; 1 on abort/no-selection; 2 on bad option.
#
# -----------------------------------------------------------------------------
#  The demo has four independent "surfaces" you may want to reset. Pick one, several,
# or all of them:
#
#   --docker      Stop & remove the local Docker stacks (Metabase, Airflow,
#                 OpenMetadata): containers + named volumes + the demo's OWN
#                 (locally built) images. Shared public base images are kept by
#                 default — see --purge-base-images.
#   --schemas     DROP the watsonx.data schemas and their tables/views via Presto
#                 (dbt_demo_*, spark_demo_*, the cpdctl raw schema, AND the
#                 confluent_demo_silver/gold schemas).
#   --minio       Delete the demo's files from MinIO/S3 (Iceberg table data, the
#                 uploaded Spark app + raw CSVs, the confluent_demo_* folders, the
#                 dbt artifacts). Needs `oc`.
#   --confluent   Reset ONLY the Confluent streaming path: drop the
#                 confluent_demo_silver + confluent_demo_gold schemas, delete their
#                 MinIO prefixes, and stop/remove the Confluent Docker services
#                 (Kafka, Flink, Schema Registry, Iceberg REST, …) plus the Kafka
#                 data volume (wipes topics) and the confluent-network. Leaves the
#                 dbt + Spark surfaces untouched. (MinIO step needs `oc`.)
#   --warehouse   Shorthand for: --schemas then --minio (the whole remote side).
#   --all         Everything: --docker + --schemas + --minio + --confluent.
#
# Options:
#   --dry-run       Show what WOULD happen; change nothing.
#   --keep-images   With --docker, remove NO images (only containers + volumes).
#   --purge-base-images  With --docker, ALSO remove shared public base images
#                   (postgres:16, metabase/metabase, elasticsearch, python).
#                   Off by default so other projects on your machine are untouched.
#   -y, --yes       Skip the confirmation prompt.
#   -h, --help      Show this help.
#
# Examples:
#   scripts/reset_demo.sh --all --dry-run     # preview a full wipe
#   scripts/reset_demo.sh --docker            # just tear down the containers
#   scripts/reset_demo.sh --confluent -y      # reset only the Confluent path
#   scripts/reset_demo.sh --warehouse -y      # drop schemas + MinIO files, no prompt
#
# Safety notes:
#   * Docker teardown is scoped to THIS demo's compose files / container names only
#     — it never touches unrelated containers/volumes on your machine.
#   * --confluent targets ONLY the confluent-* containers + the confluent-kafka-data
#     volume + the confluent-network; the other demo stacks are left running.
#   * Schema + MinIO deletion are scoped to the exact demo schema names / prefixes
#     derived from your .env; they never empty the whole bucket.
#
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Shared helper library (info/warn/success/step log helpers + install_err_trap).
# The script keeps its own confirm()/run() below (defined after this source, so
# they win) for backward-compatible behaviour; we mainly want the ERR trap and a
# consistent toolbox. Sourcing is a no-op if it was already loaded elsewhere.
# shellcheck source=scripts/lib/log.sh
if [ -f "$REPO/scripts/lib/log.sh" ]; then
  source "$REPO/scripts/lib/log.sh"
  install_err_trap
fi

# Prefer the repo virtualenv's python so deps (boto3, prestodb, dotenv) resolve.
if [ -x "$REPO/.venv/bin/python" ]; then
  PY="$REPO/.venv/bin/python"
else
  PY="$(command -v python3 || command -v python)"
fi

DO_DOCKER=false
DO_SCHEMAS=false
DO_MINIO=false
DO_CONFLUENT=false
DRY_RUN=false
KEEP_IMAGES=false
PURGE_IMAGES=false
ASSUME_YES=false

# Prints the user-facing help block (the lines between the canonical header and
# `set -euo pipefail`): description, options, examples, and safety notes.
usage() { sed -n '48,91p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; }

if [ $# -eq 0 ]; then usage; exit 1; fi

while [ $# -gt 0 ]; do
  case "$1" in
    --docker)      DO_DOCKER=true ;;
    --schemas)     DO_SCHEMAS=true ;;
    --minio)       DO_MINIO=true ;;
    --confluent)   DO_CONFLUENT=true ;;
    --warehouse)   DO_SCHEMAS=true; DO_MINIO=true ;;
    --all)         DO_DOCKER=true; DO_SCHEMAS=true; DO_MINIO=true; DO_CONFLUENT=true ;;
    --dry-run)     DRY_RUN=true ;;
    --keep-images) KEEP_IMAGES=true ;;
    --purge-base-images) PURGE_IMAGES=true ;;
    -y|--yes)      ASSUME_YES=true ;;
    -h|--help)     usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; echo "Try --help." >&2; exit 2 ;;
  esac
  shift
done

# ---------------------------------------------------------------------------

confirm() {
  $DRY_RUN && return 0
  $ASSUME_YES && return 0
  echo
  read -r -p "This will PERMANENTLY remove the selected resources. Continue? [y/N] " ans
  case "$ans" in y|Y|yes|YES) return 0 ;; *) echo "Aborted."; exit 1 ;; esac
}

run() {  # echo + execute (or just echo on dry-run)
  if $DRY_RUN; then echo "    DRY: $*"; else "$@"; fi
}

reset_docker() {
  echo "==> Docker teardown (Metabase, Airflow, OpenMetadata)"
  if ! command -v docker >/dev/null 2>&1; then
    echo "    docker not found — skipping."; return
  fi
  # Image policy (default is conservative — only this demo's OWN images):
  #   default              --rmi local : removes only locally-BUILT images (the
  #                                      ibmas-watsonxdata-dbt-airflow-* images).
  #                                      Shared PUBLIC base images (postgres:16,
  #                                      metabase/metabase, elasticsearch,
  #                                      python:3.12-slim) are KEPT.
  #   --keep-images        remove no images at all.
  #   --purge-base-images  --rmi all : also remove the shared public base images
  #                                      (Docker auto-skips any still in use).
  local rmi=(--rmi local)
  $KEEP_IMAGES && rmi=()
  if $PURGE_IMAGES; then
    rmi=(--rmi all)
    echo "    NOTE: --purge-base-images will also remove SHARED public images"
    echo "          (postgres:16, metabase/metabase, elasticsearch, python:3.12-slim)."
    echo "          Docker skips any image still used by another container."
  fi

  if [ -f "$REPO/docker-compose.yml" ]; then
    # Unified project (docker-compose.yml owns Airflow and includes the optional
    # companion stacks) — a single `down` removes everything together.
    # --remove-orphans is safe here because it IS one project.
    echo "    -- unified project (docker-compose.yml): Metabase + Airflow + OpenMetadata"
    if $DRY_RUN; then
      echo "    DRY: (cd $REPO && docker compose down --volumes --remove-orphans ${rmi[*]})"
      echo "    DRY: (cd $REPO && docker compose -f openmetadata/docker-compose.yml down --volumes --remove-orphans)  # legacy OM project safety"
    else
      ( cd "$REPO" && docker compose down --volumes --remove-orphans "${rmi[@]}" ) \
        || echo "    (warning: unified teardown reported issues; continuing)"
      # Safety: also tear down OpenMetadata under its legacy standalone project
      # name, in case it was started the old way (docker compose -f openmetadata/...).
      ( cd "$REPO" && docker compose -f openmetadata/docker-compose.yml down --volumes --remove-orphans 2>/dev/null ) || true
    fi
  else
    # Legacy fallback: tear down each stack file separately. OpenMetadata is its
    # own project.
    local stacks=(
      "Metabase|docker-compose-metabase.yml|"
      "OpenMetadata|openmetadata/docker-compose.yml|--remove-orphans"
    )
    local s name file extra
    for s in "${stacks[@]}"; do
      IFS='|' read -r name file extra <<< "$s"
      if [ ! -f "$REPO/$file" ]; then echo "    skip $name (no $file)"; continue; fi
      echo "    -- $name ($file)"
      local extra_args=()
      [ -n "$extra" ] && extra_args=("$extra")
      if $DRY_RUN; then
        echo "    DRY: (cd $REPO && docker compose -f $file down --volumes ${extra_args[*]} ${rmi[*]})"
      else
        ( cd "$REPO" && docker compose -f "$file" down --volumes "${extra_args[@]}" "${rmi[@]}" ) \
          || echo "    (warning: $name teardown reported issues; continuing)"
      fi
    done
  fi

  # Fallback volume sweep (only matters if a 'down' above didn't remove a volume).
  # Scoped by Docker's OWN compose-project label, so it can ONLY ever match this
  # demo's two projects — never an unrelated stack (e.g. an 'insurance_*' project).
  echo "    -- sweeping leftover demo volumes (by compose-project label)"
  local proj_main found_any=false proj vols v
  proj_main="$(basename "$REPO" | tr '[:upper:]' '[:lower:]')"
  for proj in "$proj_main" openmetadata; do
    vols="$(docker volume ls -q --filter "label=com.docker.compose.project=$proj" 2>/dev/null || true)"
    [ -z "$vols" ] && continue
    while read -r v; do
      [ -z "$v" ] && continue
      found_any=true
      run docker volume rm "$v"
    done <<< "$vols"
  done
  $found_any || echo "       (none)"

  # OpenMetadata's mysql + elasticsearch persist to a BIND MOUNT
  # (./openmetadata/docker-volume), NOT a docker volume — so `down -v` and the
  # sweep above never touch it. Stale data left here can crash mysql on the next
  # start (InnoDB on an incompatible/half-written datadir), so clear it for a
  # truly clean rerun.
  if [ -d "$REPO/openmetadata/docker-volume" ]; then
    echo "    -- clearing OpenMetadata bind-mount data (openmetadata/docker-volume)"
    run rm -rf "$REPO/openmetadata/docker-volume"
  fi
}

reset_schemas() {
  echo "==> Drop watsonx.data schemas + tables/views (Presto)"
  if $DRY_RUN; then
    echo "    DRY: $PY scripts/cleanup_watsonxdata.py"
    return
  fi
  ( cd "$REPO" && "$PY" scripts/cleanup_watsonxdata.py )
}

reset_minio() {
  echo "==> Delete demo files from MinIO/S3"
  if ! command -v oc >/dev/null 2>&1; then
    echo "    oc not found — MinIO is only reachable via an oc port-forward on this"
    echo "    cluster. Skipping. (Install oc + 'oc login', then re-run with --minio.)"
    return
  fi
  if ! oc whoami >/dev/null 2>&1; then
    echo "    Not logged in with oc — run 'oc login ...' first. Skipping MinIO cleanup."
    return
  fi
  local args=()
  $DRY_RUN && args=(--dry-run)
  ( cd "$REPO" && "$PY" scripts/cleanup_minio.py "${args[@]}" )
}

reset_confluent() {
  echo "==> Confluent streaming path reset (Kafka, Flink, Schema Registry, Iceberg REST)"

  # The Confluent silver+gold schema names come from .env (defaults shown). We
  # only echo the defaults here — the Python helpers below load .env themselves.
  local silver="${CONFLUENT_SILVER_SCHEMA:-confluent_demo_silver}"
  local gold="${CONFLUENT_GOLD_SCHEMA:-confluent_demo_gold}"

  # --- 1) Catalog: drop ONLY the two confluent_* schemas (Presto) ------------
  echo "    -- watsonx.data schemas: ${silver} + ${gold}"
  if $DRY_RUN; then
    echo "    DRY: $PY scripts/cleanup_watsonxdata.py --confluent-only"
  else
    ( cd "$REPO" && "$PY" scripts/cleanup_watsonxdata.py --confluent-only ) \
      || echo "    (warning: confluent schema drop reported issues; continuing)"
  fi

  # --- 2) Object store: delete ONLY the confluent_* MinIO prefixes -----------
  # Same oc-port-forward requirement as reset_minio (no external MinIO Route).
  if ! command -v oc >/dev/null 2>&1; then
    echo "    -- skipping MinIO cleanup: 'oc' not found (object store is reached via an"
    echo "       oc port-forward). Install oc + 'oc login', then re-run with --confluent."
  elif ! oc whoami >/dev/null 2>&1; then
    echo "    -- skipping MinIO cleanup: not logged in with oc ('oc login ...' first)."
  else
    local margs=(--confluent-only)
    $DRY_RUN && margs+=(--dry-run)
    ( cd "$REPO" && "$PY" scripts/cleanup_minio.py "${margs[@]}" ) \
      || echo "    (warning: confluent MinIO cleanup reported issues; continuing)"
  fi

  # --- 3) Docker: stop + remove the confluent-* services -----------------------
  if ! command -v docker >/dev/null 2>&1; then
    echo "    docker not found — skipping container teardown."; return
  fi

  # Every Confluent container has a fixed, confluent-prefixed name — listing them
  # explicitly keeps this teardown tightly scoped (the Metabase/Airflow/
  # OpenMetadata services in the same compose project are NEVER touched). Order
  # is "leaf-first" so dependents go before the things they depend on.
  local confluent_services=(
    confluent-prep
    confluent-flink-runner
    confluent-schema-prep
    confluent-kafka-init
    confluent-flink-sql-gateway
    confluent-flink-taskmanager
    confluent-flink-jobmanager
    confluent-iceberg-rest
    confluent-kafbat-ui
    confluent-schema-registry
    confluent-kafka
  )

  echo "    -- removing confluent containers (incl. one-shot init/runner jobs)"
  if [ -f "$REPO/docker-compose.yml" ]; then
    # Unified compose project: 'rm -fsv' targets ONLY the named confluent services
    # (stop + force-remove + drop their anonymous volumes). Profiled one-shots that
    # aren't currently present are simply skipped.
    if $DRY_RUN; then
      echo "    DRY: (cd $REPO && docker compose rm -fsv ${confluent_services[*]})"
    else
      ( cd "$REPO" && docker compose rm -fsv "${confluent_services[@]}" 2>/dev/null ) \
        || echo "    (warning: 'docker compose rm' reported issues; continuing)"
    fi
  fi

  # Belt-and-suspenders: force-remove any leftover confluent container by name
  # (covers one-shots started outside the unified project, e.g. via start.sh).
  local c
  for c in "${confluent_services[@]}"; do
    if docker container inspect "$c" >/dev/null 2>&1; then
      run docker rm -f "$c"
    fi
  done

  # Kafka's topics + messages live in the named 'confluent-kafka-data' volume.
  # Removing it wipes all topic data so the next start.sh replays from scratch.
  echo "    -- removing Kafka data volume (wipes topics + messages)"
  if docker volume inspect confluent-kafka-data >/dev/null 2>&1; then
    run docker volume rm confluent-kafka-data \
      || echo "       (volume still in use — make sure all confluent containers are gone)"
  else
    echo "       (confluent-kafka-data volume not present)"
  fi

  # Finally drop the dedicated confluent-network (only succeeds once no container
  # is attached — harmless if it is shared or already gone).
  echo "    -- removing confluent-network"
  if docker network inspect confluent-network >/dev/null 2>&1; then
    run docker network rm confluent-network \
      || echo "       (network still in use or already removed — left as-is)"
  else
    echo "       (confluent-network not present)"
  fi
}

# ---------------------------------------------------------------------------

docker_label="containers + volumes + demo-built images"
$KEEP_IMAGES && docker_label="containers + volumes (all images kept)"
$PURGE_IMAGES && docker_label="containers + volumes + ALL images (incl. shared base)"

echo "watsonx.data demo reset"
$DRY_RUN && echo "(dry-run — nothing will be changed)"
echo "Selected:"
$DO_DOCKER    && echo "  • Docker stacks ($docker_label)"
$DO_SCHEMAS   && echo "  • watsonx.data schemas (Presto DROP)"
$DO_MINIO     && echo "  • MinIO demo files"
$DO_CONFLUENT && echo "  • Confluent path (schemas + MinIO + Kafka/Flink containers, volume, network)"

confirm

$DO_DOCKER    && reset_docker
$DO_SCHEMAS   && reset_schemas
$DO_MINIO     && reset_minio
$DO_CONFLUENT && reset_confluent

echo
if $DRY_RUN; then
  echo "Dry-run complete. Re-run without --dry-run to apply."
else
  echo "Reset complete. You can now rerun the demo from a clean state."
fi
