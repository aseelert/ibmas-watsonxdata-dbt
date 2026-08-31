#!/usr/bin/env bash
# -----------------------------------------------------------------------------
#  run-ingestion.sh — push the medallion tables + dbt lineage into OpenMetadata.
#
#  Location  : openmetadata/ingestion/run-ingestion.sh
#  Repository: https://github.com/aseelert/ibmas-watsonxdata-dbt
#  Project   : watsonx.data · dbt · Spark medallion demo
#  Author    : Alexander Seelert
#  Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.
#
#  WHAT / WHY
#    Drives the OPTIONAL OpenMetadata catalogue+lineage demo. OpenMetadata is a
#    standalone add-on: it assumes the medallion has ALREADY been built (by dbt
#    or Spark) — it never builds data itself. This script makes the bronze/
#    silver/gold tables, their columns, descriptions, tags, sample data,
#    profile stats, BI lineage, and dbt lineage show up in the OpenMetadata UI,
#    in six passes:
#
#      Pass 1 — TABLES. OpenMetadata's dbt/Metabase/profiler/classification
#      passes below only ATTACH to tables that already exist as entities, so
#      the tables must be created first. This script tries the LIVE path: a
#      real Presto metadata ingestion (metadata-ingestion.yaml) that
#      discovers the actual watsonx.data tables. If that fails (Presto down,
#      cert/API-key issues), it FALLS BACK to the OFFLINE seed
#      (scripts/seed_openmetadata_tables.py), which builds the same table
#      entities from the staged dbt catalog.json with no live connection.
#
#      Pass 2 — BI LINEAGE (Metabase). metabase-ingestion.yaml ingests the
#      local Metabase instance as a Dashboard service and derives lineage from
#      each chart's query back to the Pass-1 Presto tables. Skipped (non-fatal)
#      if Metabase isn't reachable, or if Pass 1 fell back to the offline seed
#      (no live Table entities to match lineage against).
#
#      Pass 3 — PROFILE STATS. profiler-ingestion.yaml runs OpenMetadata's
#      Profiler workflow against the Pass-1 "watsonxdata-presto" service,
#      populating each table's Profiler & Data Quality tab (min/max/nulls/
#      distinct/rowCount/etc.). Does NOT produce sample data — see Pass 4.
#      Skipped (non-fatal) if Pass 1 fell back to the offline seed.
#
#      Pass 4 — SAMPLE DATA + PII TAGGING. autoclassification-ingestion.yaml
#      runs OpenMetadata's separate AutoClassification workflow (verified: the
#      plain Profiler workflow above has no Sampler step at all in 1.13.0.0,
#      despite some docs implying otherwise) — this is what actually
#      populates the Sample Data tab, and additionally auto-tags likely-PII
#      columns. Skipped (non-fatal) if Pass 1 fell back to the offline seed.
#
#      Pass 5 — dbt LINEAGE. The dbt ingestion (dbt-ingestion.yaml) reads the
#      dbt manifest/catalog and attaches model descriptions, dbt tags, and
#      bronze→silver→gold (table- and column-level) lineage onto those tables.
#
#      Pass 6 — GOVERNANCE. The governance script creates/updates the demo
#      glossary, classifications, descriptions, and online/offline mode tags
#      (table- AND column-level glossary terms).
#
#  WHEN TO RUN IT
#    AFTER (a) the local OpenMetadata Docker stack is up and healthy on
#    localhost:8585 (see openmetadata/docker-compose.yml) and (b) the dbt
#    artifacts exist + are staged — run scripts/07b_generate_lineage_docs.sh (or
#    scripts/07a_prepare_openmetadata_dbt_artifacts.py) first. Re-running is safe:
#    every write is an idempotent create-or-update.
#
#  ENV VARS (read from <repo>/.env)
#    WXD_HOST / WXD_PORT / WXD_USER / WXD_API_KEY / WXD_CATALOG — live Presto
#    connection for pass 1. WXD_SSL_VERIFY — path to the watsonx CA cert.
#    If any are missing the live pass is skipped and the offline seed is used.
#    MB_SETUP_EMAIL / MB_SETUP_PASSWORD — Metabase admin login for pass 2
#    (defaults admin@admin.com / admin12345, matching metabase/provision.py).
#
#  PREREQUISITES
#    The repo virtualenv at .venv (activated below); a reachable OpenMetadata at
#    http://localhost:8585; get_om_token.py + the *-ingestion.yaml recipes
#    present. `openmetadata-ingestion[dbt,presto,pii-processor]` is
#    pip-installed on the fly (pinned to 1.13.0.0, falling back to latest) —
#    [dbt,presto] covers Passes 1/2/3/5, [pii-processor] (pulls in Microsoft's
#    presidio-analyzer) is required for Pass 4's PII auto-tagging step, or it
#    fails with "No module named 'presidio_analyzer'".
#
#  USAGE
#    openmetadata/ingestion/run-ingestion.sh
#    WXD_OM_SKIP_LIVE=1 openmetadata/ingestion/run-ingestion.sh   # force offline seed
#
#  SIDE EFFECTS / EXIT
#    pip-installs into the active venv; writes rendered configs (with the JWT and
#    secrets substituted) under /tmp; creates/updates the watsonxdata-presto
#    DatabaseService + a metabase-demo DashboardService; profiles + samples
#    every dbt_demo_* table; attaches Metabase BI lineage and dbt lineage.
#    Exits non-zero only if Pass 1 or Pass 5 (dbt lineage) fails — Pass 2
#    (Metabase), Pass 3 (profiler), and Pass 4 (sample data/PII) failures are
#    logged but non-fatal, since they're enrichment on top of an
#    already-complete catalogue.
# -----------------------------------------------------------------------------
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
ing_dir="${repo_root}/07-openmetadata/openmetadata/ingestion"

echo "[ingest] activating repo virtualenv at ${repo_root}/.venv"
cd "${repo_root}" && source .venv/bin/activate

if [[ -f "${repo_root}/.env" ]]; then
  set -a; source "${repo_root}/.env"; set +a
fi

ca_abs=""
if [[ -n "${WXD_SSL_VERIFY:-}" ]]; then
  ca_abs="$(cd "$(dirname "${WXD_SSL_VERIFY}")" && pwd)/$(basename "${WXD_SSL_VERIFY}")"
fi

echo "[ingest] installing openmetadata-ingestion[dbt,presto,pii-processor]..."
if ! pip install "openmetadata-ingestion[dbt,presto,pii-processor]==1.13.0.0" -q 2>&1 | tail -3; then
    echo "[ingest] pinned version failed, trying latest..." >&2
    pip install "openmetadata-ingestion[dbt,presto,pii-processor]" -q
fi

# --- Pass 1: create the table entities (live Presto, else offline seed) -------
live_ok=0
if [[ "${WXD_OM_SKIP_LIVE:-0}" != "1" && -n "${WXD_HOST:-}" && -n "${WXD_USER:-}" && -n "${WXD_API_KEY:-}" ]]; then
  echo "[ingest] Pass 1 (live): ingesting real Presto tables from watsonx.data..."
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

# --- Pass 2: attach Metabase BI lineage (non-fatal) ----------------------------
if [[ "${live_ok}" -eq 1 ]]; then
  MB_HOST="http://localhost:3000"
  if curl -sf --max-time 5 "${MB_HOST}/api/health" >/dev/null 2>&1; then
    echo "[ingest] Pass 2: ingesting Metabase dashboards + BI lineage..."
    TOKEN=$(python "${ing_dir}/get_om_token.py")
    sed -e "s|__JWT_TOKEN__|${TOKEN}|g" \
        -e "s|__MB_HOST__|${MB_HOST}|g" \
        -e "s|__MB_EMAIL__|${MB_SETUP_EMAIL:-admin@admin.com}|g" \
        -e "s|__MB_PASSWORD__|${MB_SETUP_PASSWORD:-admin12345}|g" \
        "${ing_dir}/metabase-ingestion.yaml" > /tmp/metabase-ingestion-final.yaml
    if ! metadata ingest -c /tmp/metabase-ingestion-final.yaml; then
      echo "[ingest] Metabase ingestion FAILED — continuing without BI lineage." >&2
    fi
  else
    echo "[ingest] Pass 2: Metabase not reachable at ${MB_HOST} — skipping BI lineage." >&2
  fi
else
  echo "[ingest] Pass 2: skipped (no live Table entities to attach BI lineage to)." >&2
fi

# --- Pass 3: profile + sample every dbt_demo_* table (non-fatal) --------------
if [[ "${live_ok}" -eq 1 ]]; then
  echo "[ingest] Pass 3: profiling + sampling watsonxdata-presto tables..."
  TOKEN=$(python "${ing_dir}/get_om_token.py")
  sed "s|__JWT_TOKEN__|${TOKEN}|g" \
      "${ing_dir}/profiler-ingestion.yaml" > /tmp/profiler-ingestion-final.yaml
  if ! metadata profile -c /tmp/profiler-ingestion-final.yaml; then
    echo "[ingest] Profiler run FAILED — continuing without sample data/stats." >&2
  fi
else
  echo "[ingest] Pass 3: skipped (Profiler workflow needs a live registered service)." >&2
fi

# --- Pass 4: real sample data + PII auto-tagging (non-fatal) -----------------
if [[ "${live_ok}" -eq 1 ]]; then
  echo "[ingest] Pass 4: fetching sample data + auto-classifying PII columns..."
  TOKEN=$(python "${ing_dir}/get_om_token.py")
  sed "s|__JWT_TOKEN__|${TOKEN}|g" \
      "${ing_dir}/autoclassification-ingestion.yaml" > /tmp/autoclassification-ingestion-final.yaml
  if ! metadata classify -c /tmp/autoclassification-ingestion-final.yaml; then
    echo "[ingest] AutoClassification run FAILED — continuing without sample data." >&2
  fi
else
  echo "[ingest] Pass 4: skipped (AutoClassification workflow needs a live registered service)." >&2
fi

# --- Pass 5: attach dbt model descriptions, tags, and lineage -----------------
echo "[ingest] Pass 5: attaching dbt lineage..."
TOKEN=$(python "${ing_dir}/get_om_token.py")
sed -e "s|__JWT_TOKEN__|${TOKEN}|g" \
    -e "s|__WXD_HOST__|${WXD_HOST:-}|g" \
    -e "s|__WXD_PORT__|${WXD_PORT:-443}|g" \
    -e "s|__WXD_USER__|${WXD_USER:-}|g" \
    -e "s|__WXD_API_KEY__|${WXD_API_KEY:-}|g" \
    -e "s|__WXD_CATALOG__|${WXD_CATALOG:-iceberg_data}|g" \
    -e "s|__WXD_CA_PEM__|${ca_abs}|g" \
    -e "s|__DBT_ARTIFACT_DIR__|${repo_root}/07-openmetadata/openmetadata/dbt-artifacts|g" \
    "${ing_dir}/dbt-ingestion.yaml" > /tmp/dbt-ingestion-final.yaml
metadata ingest -c /tmp/dbt-ingestion-final.yaml

# --- Pass 6: attach glossary, classifications, and description fallbacks ------
echo "[ingest] Pass 6: applying glossary terms and auto-classifications..."
if ! python "${repo_root}/scripts/07d_apply_openmetadata_governance.py"; then
  echo "[ingest] governance enrichment failed; lineage ingestion is still complete." >&2
fi

echo "[ingest] Done. View the catalogue + lineage at http://localhost:8585"
