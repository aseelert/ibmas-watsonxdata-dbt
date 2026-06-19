#!/usr/bin/env bash
# -----------------------------------------------------------------------------
#  run-ingestion.sh — push dbt models + lineage into OpenMetadata over Presto.
#
#  Location  : openmetadata/ingestion/run-ingestion.sh
#  Repository: https://github.ibm.com/alexander/ibmas-watsonxdata-dbt
#  Project   : watsonx.data · dbt · Spark medallion demo
#  Author    : Alexander Seelert
#  Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.
#
#  WHAT / WHY
#    Drives the OpenMetadata "dbt ingestion" workflow for the demo. It registers
#    a Presto database service (pointing at watsonx.data) in the local
#    OpenMetadata instance, then runs the OpenMetadata ingestion CLI against the
#    dbt manifest/catalog so that the bronze/silver/gold models, their columns,
#    descriptions, and lineage show up in the OpenMetadata UI. This is what gives
#    the demo its "see the data catalogue + lineage graph" payoff.
#
#  WHEN TO RUN IT
#    AFTER (a) the local OpenMetadata Docker stack is up and healthy on
#    localhost:8585 (see openmetadata/docker-compose.yml) and (b) dbt has run at
#    least once so the dbt artifacts (manifest.json / catalog.json) exist for the
#    ingestion config to read. Re-running is safe: the service create is
#    idempotent (a 409 "already exists" is treated as success).
#
#  ENV VARS
#    Reads none directly. The Presto host/catalog are inlined in the service
#    POST body below, and the JWT is generated at runtime by get_om_token.py.
#    The ingestion config (dbt-ingestion.yaml) carries the dbt artifact paths.
#
#  PREREQUISITES
#    The repo virtualenv at .venv (activated below); a reachable OpenMetadata at
#    http://localhost:8585; openmetadata/ingestion/get_om_token.py and
#    dbt-ingestion.yaml present. `openmetadata-ingestion[dbt]` is pip-installed
#    on the fly (pinned to 1.13.0.0, falling back to latest if the pin fails).
#
#  USAGE
#    openmetadata/ingestion/run-ingestion.sh
#
#  SIDE EFFECTS / EXIT
#    pip-installs into the active venv; writes /tmp/dbt-ingestion-final.yaml (the
#    config with the JWT token substituted in); creates the watsonxdata-presto
#    service in OpenMetadata; runs `metadata ingest`. Exits non-zero if the
#    ingestion run itself fails (the service-create step is non-fatal).
# -----------------------------------------------------------------------------
set -euo pipefail

echo "[ingest] activating repo virtualenv at /Users/aseelert/GitHub/ibmas-watsonxdata-dbt/.venv"
cd /Users/aseelert/GitHub/ibmas-watsonxdata-dbt && source .venv/bin/activate

echo "Installing openmetadata-ingestion[dbt]..."
if ! pip install "openmetadata-ingestion[dbt]==1.13.0.0" -q 2>&1 | tail -3; then
    echo "Pinned version failed, trying latest..." >&2
    pip install "openmetadata-ingestion[dbt]" -q
fi

TOKEN=$(python /Users/aseelert/GitHub/ibmas-watsonxdata-dbt/openmetadata/ingestion/get_om_token.py)
echo "Got JWT token (length: ${#TOKEN})"

echo "Rendering ingestion config with JWT -> /tmp/dbt-ingestion-final.yaml"
sed "s|__JWT_TOKEN__|${TOKEN}|g" \
    /Users/aseelert/GitHub/ibmas-watsonxdata-dbt/openmetadata/ingestion/dbt-ingestion.yaml \
    > /tmp/dbt-ingestion-final.yaml

echo "Creating Presto database service in OpenMetadata..."
curl -s -o /dev/null -w "%{http_code}" \
    -X POST "http://localhost:8585/api/v1/services/databaseServices" \
    -H "Authorization: Bearer ${TOKEN}" \
    -H "Content-Type: application/json" \
    -d '{"name":"watsonxdata-presto","serviceType":"Presto","connection":{"config":{"type":"Presto","hostPort":"ibm-lh-lakehouse-presto651-presto-svc.apps.watson.ibmas-zocp-techcluster.org:443","catalog":"iceberg_data"}}}' \
| grep -qE "^(200|201|409)$" && echo "Service ready (created or already exists)" \
|| echo "Warning: unexpected status code when creating service" >&2

echo "Running dbt ingestion..."
metadata ingest -c /tmp/dbt-ingestion-final.yaml

echo "Done. View dbt lineage at http://localhost:8585"
