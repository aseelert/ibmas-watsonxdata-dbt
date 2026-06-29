#!/usr/bin/env bash
# =============================================================================
#  create-topics.sh — create the 8 demo Kafka topics (4 raw_* + 4 silver_*)
# -----------------------------------------------------------------------------
#  Location  : confluent/scripts/create-topics.sh
#  Repository: https://github.ibm.com/alexander/ibmas-watsonxdata-dbt
#  Project   : watsonx.data · dbt · Spark · Confluent medallion demo
#  Author    : Alexander Seelert — IBM Customer Success Engineer
#  Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.
#
#  Changelog :
#    v1.0 (2026-06-26) — Initial version. Idempotently creates the EIGHT topics
#      the pipeline needs — the 4 raw_* entity topics the Avro producer writes,
#      and the 4 silver_* topics the Flink Stage-1 jobs write and Stage-2 reads.
#      (Earlier drafts mislabelled this as "4 topics"; corrected here.)
# -----------------------------------------------------------------------------
#
#  WHY 8 topics (for an 18-year-old learner)
#    The broker has AUTO topic creation turned OFF on purpose, so every topic
#    must be created up front. Our medallion has two Kafka hops:
#      raw_*    — the producer lands the seed CSV rows here as Avro (4 topics)
#      silver_* — Flink writes the cleaned/typed rows here as Avro (4 topics)
#    The Iceberg "sales_enriched" table is built by a join and is NOT a topic.
#    Runs as a one-shot container: confluentinc/cp-kafka:7.7.1
# =============================================================================
set -euo pipefail
trap 'rc=$?; echo "  [ERROR] create-topics.sh failed (exit ${rc}) on line ${LINENO}" >&2' ERR

BROKER="${KAFKA_BROKER:-confluent-kafka:9092}"

# The exact topic set: 4 raw entity topics + 4 silver entity topics = 8.
RAW_TOPICS=(raw_customers raw_products raw_orders raw_order_items)
SILVER_TOPICS=(silver_customers silver_products silver_orders silver_order_items)
ALL_TOPICS=("${RAW_TOPICS[@]}" "${SILVER_TOPICS[@]}")

echo "Waiting for Kafka broker at ${BROKER}..."
until kafka-topics --bootstrap-server "${BROKER}" --list >/dev/null 2>&1; do
  echo "  broker not ready yet — sleeping 2s"
  sleep 2
done
echo "Broker ready."

for TOPIC in "${ALL_TOPICS[@]}"; do
  kafka-topics \
    --bootstrap-server "${BROKER}" \
    --create \
    --if-not-exists \
    --topic "${TOPIC}" \
    --partitions 1 \
    --replication-factor 1
  echo "Topic ready: ${TOPIC}"
done

echo "All ${#ALL_TOPICS[@]} topics ready (${#RAW_TOPICS[@]} raw_* + ${#SILVER_TOPICS[@]} silver_*)."
