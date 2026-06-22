#!/usr/bin/env bash
# -----------------------------------------------------------------------------
#  generate_lineage_docs.sh — refresh ONLY the dbt lineage artifacts for OpenMetadata.
#
#  Location  : scripts/generate_lineage_docs.sh
#  Repository: https://github.ibm.com/alexander/ibmas-watsonxdata-dbt
#  Project   : watsonx.data · dbt · Spark medallion demo
#  Author    : Alexander Seelert
#  Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.
#
#  WHAT / WHY
#    A lineage-only convenience entry point. It is a thin wrapper that calls
#    `scripts/prepare_openmetadata_dbt_artifacts.py --docs-only`, which runs ONLY
#    `dbt docs generate` (emitting manifest.json + catalog.json + run_results.json)
#    and then stages those three files into the OpenMetadata artifact directory.
#    Unlike the full prepare run, NO seed/run/test happens. Delegating to the
#    Python script means this wrapper inherits its per-command timeout, retry, and
#    staging/validation logic instead of re-implementing them. Use it when the
#    medallion data is already built and you just want fresh lineage/column
#    metadata for the catalogue.
#
#  WHEN TO RUN IT
#    AFTER dbt has built the models at least once (so the warehouse tables exist
#    for `docs generate` to introspect for catalog.json), and BEFORE
#    openmetadata/ingestion/run-ingestion.sh. If the tables do NOT yet exist,
#    use scripts/prepare_openmetadata_dbt_artifacts.py instead (full build).
#
#  ENV VARS
#    Reads none directly. `dbt docs generate` runs through scripts/dbt_env.sh,
#    which sources <repo>/.env (WXD_HOST / WXD_USER / WXD_PASSWORD / ...). The
#    staging step honours WXD_DBT_ARTIFACT_DIR (default openmetadata/dbt-artifacts).
#
#  PREREQUISITES
#    A working dbt profile pointing at watsonx.data, a reachable Presto engine
#    (catalog.json is built from a live warehouse query), and the repo .venv.
#
#  USAGE
#    scripts/generate_lineage_docs.sh
#    scripts/generate_lineage_docs.sh --artifact-dir /tmp/om-dbt
#    scripts/generate_lineage_docs.sh --retries 3
#    # all flags are forwarded verbatim to prepare_openmetadata_dbt_artifacts.py
#    # (e.g. --artifact-dir, --retries). Do not pass --skip-dbt here.
#
#  SIDE EFFECTS / EXIT
#    Writes target/{manifest,catalog,run_results}.json then copies them into the
#    staging directory. Exits non-zero if `docs generate` fails or manifest.json
#    is missing after the run. No seed/run/test is performed.
# -----------------------------------------------------------------------------
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "[lineage] refreshing OpenMetadata lineage artifacts (docs generate only — no seed/run/test)" >&2
exec python3 "${repo_root}/scripts/prepare_openmetadata_dbt_artifacts.py" --docs-only "$@"
