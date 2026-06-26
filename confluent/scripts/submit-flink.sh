#!/bin/bash
# =============================================================================
#  submit-flink.sh — wait for Flink SQL Gateway, then submit silver_jobs.sql
#  Run as one-shot container using the wxd-flink:1.20 image.
#
#  Uses "gateway" mode so the runner container does not attempt to bind an
#  embedded SQL Gateway (which fails with UnresolvedAddressException when the
#  container hostname is not DNS-resolvable).
# =============================================================================
set -euo pipefail

JM_HOST="${JOBMANAGER_HOST:-confluent-flink-jobmanager}"
JM_PORT="${JOBMANAGER_PORT:-8081}"
GW_HOST="${SQL_GATEWAY_HOST:-confluent-flink-sql-gateway}"
GW_PORT="${SQL_GATEWAY_PORT:-8083}"
SQL_FILE="${SQL_FILE:-/opt/sql/silver_jobs.sql}"

echo "Waiting for Flink JobManager at http://${JM_HOST}:${JM_PORT}/v1/overview ..."
until curl -sf "http://${JM_HOST}:${JM_PORT}/v1/overview" >/dev/null 2>&1; do
  echo "  JobManager not ready — sleeping 3s"
  sleep 3
done
echo "JobManager ready."

echo "Waiting for Flink SQL Gateway at http://${GW_HOST}:${GW_PORT}/v1/info ..."
until curl -sf "http://${GW_HOST}:${GW_PORT}/v1/info" >/dev/null 2>&1; do
  echo "  SQL Gateway not ready — sleeping 3s"
  sleep 3
done
echo "SQL Gateway ready."

echo "Submitting SQL file: ${SQL_FILE}"
exec /opt/flink/bin/sql-client.sh gateway \
  -e "http://${GW_HOST}:${GW_PORT}" \
  -f "${SQL_FILE}"
