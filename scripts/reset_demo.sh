#!/usr/bin/env bash
#
# reset_demo.sh — get the watsonx.data medallion demo back to a 100% clean state.
#
# The demo has three independent "surfaces" you may want to reset. Pick one, several,
# or all of them:
#
#   --docker      Stop & remove the local Docker stacks (Metabase, Airflow,
#                 OpenMetadata): containers + named volumes + the demo's OWN
#                 (locally built) images. Shared public base images are kept by
#                 default — see --purge-base-images.
#   --schemas     DROP the watsonx.data schemas and their tables/views via Presto
#                 (dbt_demo_*, spark_demo_*, the cpdctl raw schema).
#   --minio       Delete the demo's files from MinIO/S3 (Iceberg table data, the
#                 uploaded Spark app + raw CSVs, the dbt artifacts). Needs `oc`.
#   --warehouse   Shorthand for: --schemas then --minio (the whole remote side).
#   --all         Everything: --docker + --schemas + --minio.
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
#   scripts/reset_demo.sh --warehouse -y      # drop schemas + MinIO files, no prompt
#
# Safety notes:
#   * Docker teardown is scoped to THIS demo's compose files only — it never
#     touches unrelated containers/volumes on your machine.
#   * Schema + MinIO deletion are scoped to the exact demo schema names / prefixes
#     derived from your .env; they never empty the whole bucket.
#
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Prefer the repo virtualenv's python so deps (boto3, prestodb, dotenv) resolve.
if [ -x "$REPO/.venv/bin/python" ]; then
  PY="$REPO/.venv/bin/python"
else
  PY="$(command -v python3 || command -v python)"
fi

DO_DOCKER=false
DO_SCHEMAS=false
DO_MINIO=false
DRY_RUN=false
KEEP_IMAGES=false
PURGE_IMAGES=false
ASSUME_YES=false

usage() { sed -n '2,40p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; }

if [ $# -eq 0 ]; then usage; exit 1; fi

while [ $# -gt 0 ]; do
  case "$1" in
    --docker)      DO_DOCKER=true ;;
    --schemas)     DO_SCHEMAS=true ;;
    --minio)       DO_MINIO=true ;;
    --warehouse)   DO_SCHEMAS=true; DO_MINIO=true ;;
    --all)         DO_DOCKER=true; DO_SCHEMAS=true; DO_MINIO=true ;;
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

  # name | compose-file | extra-down-flags
  # Metabase + Airflow share one compose project (repo dir), so we do NOT pass
  # --remove-orphans there (it would catch the other stack). OpenMetadata is its
  # own project, so --remove-orphans is safe and useful.
  local stacks=(
    "Metabase|docker-compose-metabase.yml|"
    "Airflow|docker-compose-airflow.yml|"
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

# ---------------------------------------------------------------------------

docker_label="containers + volumes + demo-built images"
$KEEP_IMAGES && docker_label="containers + volumes (all images kept)"
$PURGE_IMAGES && docker_label="containers + volumes + ALL images (incl. shared base)"

echo "watsonx.data demo reset"
$DRY_RUN && echo "(dry-run — nothing will be changed)"
echo "Selected:"
$DO_DOCKER  && echo "  • Docker stacks ($docker_label)"
$DO_SCHEMAS && echo "  • watsonx.data schemas (Presto DROP)"
$DO_MINIO   && echo "  • MinIO demo files"

confirm

$DO_DOCKER  && reset_docker
$DO_SCHEMAS && reset_schemas
$DO_MINIO   && reset_minio

echo
if $DRY_RUN; then
  echo "Dry-run complete. Re-run without --dry-run to apply."
else
  echo "Reset complete. You can now rerun the demo from a clean state."
fi
