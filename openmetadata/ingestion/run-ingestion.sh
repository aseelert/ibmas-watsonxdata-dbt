#!/usr/bin/env bash
# -----------------------------------------------------------------------------
#  run-ingestion.sh — push the medallion tables + dbt lineage into OpenMetadata.
#
#  Location  : openmetadata/ingestion/run-ingestion.sh
#  Repository: https://github.ibm.com/alexander/ibmas-watsonxdata-dbt
#  Project   : watsonx.data · dbt · Spark medallion demo
#  Author    : Alexander Seelert
#  Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.
#
#  WHAT / WHY
#    Drives the OPTIONAL OpenMetadata catalogue+lineage demo. OpenMetadata is a
#    standalone add-on: it assumes the medallion has ALREADY been built (by dbt
#    or Spark) — it never builds data itself. This script makes the bronze/
#    silver/gold tables, their columns, descriptions, tags, and lineage show up
#    in the OpenMetadata UI, in three passes:
#
#      Pass 1 — TABLES. OpenMetadata's dbt ingestion only ATTACHES lineage to
#      tables that already exist as entities, so the tables must be created
#      first. This script tries the LIVE path: a real Presto metadata ingestion
#      (metadata-ingestion.yaml) that discovers the actual watsonx.data tables
#      (and enables profiling/sample data). If that fails (Presto down, cert/
#      API-key issues), it FALLS BACK to the OFFLINE seed
#      (scripts/seed_openmetadata_tables.py), which builds the same table
#      entities from the staged dbt catalog.json with no live connection.
#
#      Pass 2 — LINEAGE. The dbt ingestion (dbt-ingestion.yaml) reads the dbt
#      manifest/catalog and attaches model descriptions, dbt tags, and bronze→
#      silver→gold (table- and column-level) lineage onto those tables.
#
#      Pass 3 — GOVERNANCE. The governance script creates/updates the demo
#      glossary, classifications, descriptions, and online/offline mode tags.
#
#  WHEN TO RUN IT
#    AFTER (a) the local OpenMetadata Docker stack is up and healthy on
#    localhost:8585 (see openmetadata/docker-compose.yml) and (b) the dbt
#    artifacts exist + are staged — run scripts/generate_lineage_docs.sh (or
#    scripts/prepare_openmetadata_dbt_artifacts.py) first. Re-running is safe:
#    every write is an idempotent create-or-update.
#
#  ENV VARS (read from <repo>/.env)
#    WXD_HOST / WXD_PORT / WXD_USER / WXD_API_KEY / WXD_CATALOG — live Presto
#    connection for pass 1. WXD_SSL_VERIFY — path to the watsonx CA cert.
#    If any are missing the live pass is skipped and the offline seed is used.
#
#  PREREQUISITES
#    The repo virtualenv at .venv (activated below); a reachable OpenMetadata at
#    http://localhost:8585; get_om_token.py, metadata-ingestion.yaml, and
#    dbt-ingestion.yaml present. `openmetadata-ingestion[dbt,presto]` is
#    pip-installed on the fly (pinned to 1.13.0.0, falling back to latest).
#
#  USAGE
#    openmetadata/ingestion/run-ingestion.sh
#    WXD_OM_SKIP_LIVE=1 openmetadata/ingestion/run-ingestion.sh   # force offline seed
#
#  SIDE EFFECTS / EXIT
#    pip-installs into the active venv; writes rendered configs (with the JWT and
#    secrets substituted) under /tmp; creates/updates the watsonxdata-presto
#    service, schemas, and tables in OpenMetadata; attaches dbt lineage. Exits
#    non-zero only if the final dbt ingestion fails.
# -----------------------------------------------------------------------------
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ing_dir="${repo_root}/openmetadata/ingestion"

echo "[ingest] activating repo virtualenv at ${repo_root}/.venv"
cd "${repo_root}" && source .venv/bin/activate

if [[ -f "${repo_root}/.env" ]]; then
  set -a; source "${repo_root}/.env"; set +a
fi

echo "[ingest] installing openmetadata-ingestion[dbt,presto]..."
if ! pip install "openmetadata-ingestion[dbt,presto]==1.13.0.0" -q 2>&1 | tail -3; then
    echo "[ingest] pinned version failed, trying latest..." >&2
    pip install "openmetadata-ingestion[dbt,presto]" -q
fi

# --- Pass 1: create the table entities (live Presto, else offline seed) -------
live_ok=0
if [[ "${WXD_OM_SKIP_LIVE:-0}" != "1" && -n "${WXD_HOST:-}" && -n "${WXD_USER:-}" && -n "${WXD_API_KEY:-}" ]]; then
  echo "[ingest] Pass 1 (live): ingesting real Presto tables from watsonx.data..."
  ca_abs=""
  if [[ -n "${WXD_SSL_VERIFY:-}" ]]; then
    ca_abs="$(cd "$(dirname "${WXD_SSL_VERIFY}")" && pwd)/$(basename "${WXD_SSL_VERIFY}")"
  fi
  TOKEN=$(python "${ing_dir}/get_om_token.py")
  sed -e "s|__JWT_TOKEN__|${TOKEN}|g" \
      -e "s|__WXD_HOST__|${WXD_HOST}|g" \
      -e "s|__WXD_PORT__|${WXD_PORT:-443}|g" \
      -e "s|__WXD_USER__|${WXD_USER}|g" \
      -e "s|__WXD_API_KEY__|${WXD_API_KEY}|g" \
      -e "s|__WXD_CATALOG__|${WXD_CATALOG:-iceberg_data}|g" \
      -e "s|__WXD_CA_PEM__|${ca_abs}|g" \
      "${ing_dir}/metadata-ingestion.yaml" > /tmp/metadata-ingestion-final.yaml
  if metadata ingest -c /tmp/metadata-ingestion-final.yaml; then
    live_ok=1
    echo "[ingest] live Presto ingestion OK."
  else
    echo "[ingest] live Presto ingestion FAILED — falling back to offline seed." >&2
  fi
else
  echo "[ingest] Pass 1: live path skipped (WXD_OM_SKIP_LIVE set or creds missing)."
fi

if [[ "${live_ok}" -ne 1 ]]; then
  echo "[ingest] Pass 1 (offline): seeding table entities from dbt catalog.json..."
  python "${repo_root}/scripts/seed_openmetadata_tables.py"
  export WXD_OM_INGESTION_MODE=offline
else
  export WXD_OM_INGESTION_MODE=online
fi

# --- Pass 2: attach dbt model descriptions, tags, and lineage -----------------
echo "[ingest] Pass 2: attaching dbt lineage..."
TOKEN=$(python "${ing_dir}/get_om_token.py")
sed "s|__JWT_TOKEN__|${TOKEN}|g" \
    "${ing_dir}/dbt-ingestion.yaml" > /tmp/dbt-ingestion-final.yaml
metadata ingest -c /tmp/dbt-ingestion-final.yaml

# --- Pass 3: attach glossary, classifications, and description fallbacks ------
echo "[ingest] Pass 3: applying glossary terms and auto-classifications..."
if ! python "${repo_root}/scripts/apply_openmetadata_governance.py"; then
  echo "[ingest] governance enrichment failed; lineage ingestion is still complete." >&2
fi

echo "[ingest] Done. View the catalogue + lineage at http://localhost:8585"
