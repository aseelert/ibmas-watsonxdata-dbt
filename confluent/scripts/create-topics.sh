#!/bin/bash
# =============================================================================
#  create-topics.sh — bootstrap the 4 raw entity topics in Kafka
#  Run as one-shot container: confluentinc/cp-kafka:7.7.1
# =============================================================================
set -euo pipefail

BROKER="${KAFKA_BROKER:-confluent-kafka:9092}"

echo "Waiting for Kafka broker at ${BROKER}..."
until kafka-topics --bootstrap-server "${BROKER}" --list >/dev/null 2>&1; do
  echo "  broker not ready yet — sleeping 2s"
  sleep 2
done
echo "Broker ready."

for TOPIC in \
  raw_customers raw_products raw_orders raw_order_items \
  silver_customers silver_products silver_orders silver_order_items; do
  kafka-topics \
    --bootstrap-server "${BROKER}" \
    --create \
    --if-not-exists \
    --topic "${TOPIC}" \
    --partitions 1 \
    --replication-factor 1
  echo "Topic ready: ${TOPIC}"
done

echo "All 4 topics ready."
