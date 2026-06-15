#!/usr/bin/env bash
set -euo pipefail

cd /Users/aseelert/GitHub/ibmas-watsonxdata-dbt && source .venv/bin/activate

echo "Installing openmetadata-ingestion[dbt]..."
if ! pip install "openmetadata-ingestion[dbt]==1.13.0.0" -q 2>&1 | tail -3; then
    echo "Pinned version failed, trying latest..." >&2
    pip install "openmetadata-ingestion[dbt]" -q
fi

TOKEN=$(python /Users/aseelert/GitHub/ibmas-watsonxdata-dbt/openmetadata/ingestion/get_om_token.py)
echo "Got JWT token (length: ${#TOKEN})"

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
