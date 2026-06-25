#!/bin/bash
# =============================================================================
#  submit-flink.sh — wait for Flink JobManager, then submit silver_jobs.sql
#  Run as one-shot container using the wxd-flink:1.20 image
# =============================================================================
set -euo pipefail

JM_HOST="${JOBMANAGER_HOST:-confluent-flink-jobmanager}"
JM_PORT="${JOBMANAGER_PORT:-8081}"
SQL_FILE="${SQL_FILE:-/opt/sql/silver_jobs.sql}"

echo "Waiting for Flink JobManager at http://${JM_HOST}:${JM_PORT}/v1/overview ..."
until curl -sf "http://${JM_HOST}:${JM_PORT}/v1/overview" >/dev/null 2>&1; do
  echo "  JobManager not ready — sleeping 3s"
  sleep 3
done
echo "JobManager ready."

echo "Submitting SQL file: ${SQL_FILE}"
exec /opt/flink/bin/sql-client.sh -f "${SQL_FILE}"
