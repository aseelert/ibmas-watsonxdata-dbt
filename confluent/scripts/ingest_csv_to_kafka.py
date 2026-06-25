#!/usr/bin/env python3
# =============================================================================
#  ingest_csv_to_kafka.py — read seed CSVs and produce each row as JSON to Kafka
# -----------------------------------------------------------------------------
#  Location  : confluent/scripts/ingest_csv_to_kafka.py
#  Repository: ibmas-watsonxdata-dbt
#
#  WHAT / WHY
#    Reads the 4 seed CSV files from seeds/ and produces each row as a JSON
#    message to the matching Kafka topic. Nothing more — raw CSV rows as JSON,
#    column names preserved exactly as CSV headers.
#
#  USAGE
#    python confluent/scripts/ingest_csv_to_kafka.py
#    python confluent/scripts/ingest_csv_to_kafka.py \
#      --bootstrap-servers localhost:29092 \
#      --csv-dir seeds/
#
#  ENV VARS
#    KAFKA_BOOTSTRAP_SERVERS  — default: localhost:29092
#    CSV_BASE_DIR             — default: seeds/  (relative to repo root)
#
#  PREREQUISITES
#    pip install confluent-kafka python-dotenv
# =============================================================================
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

try:
    from confluent_kafka import Producer
except ImportError as exc:
    raise SystemExit(
        "Missing dependency 'confluent-kafka'. "
        "Install with: pip install confluent-kafka"
    ) from exc

ROOT = Path(__file__).resolve().parents[2]

# Map CSV filename stem → Kafka topic name
TOPIC_MAP = {
    "raw_customers":   "raw_customers",
    "raw_products":    "raw_products",
    "raw_orders":      "raw_orders",
    "raw_order_items": "raw_order_items",
}


def _delivery_report(err, msg):
    if err:
        print(f"  ERROR delivery failed for {msg.topic()}: {err}", file=sys.stderr)


def produce_csv(producer: Producer, csv_path: Path, topic: str) -> int:
    """Produce every row of a CSV file to a Kafka topic. Returns rows produced."""
    count = 0
    with csv_path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            value = json.dumps(row, ensure_ascii=False).encode("utf-8")
            producer.produce(topic, value=value, callback=_delivery_report)
            count += 1
            # Poll periodically to drain the delivery queue
            if count % 100 == 0:
                producer.poll(0)
    producer.flush()
    return count


def main() -> int:
    if load_dotenv is not None:
        load_dotenv(ROOT / ".env")

    parser = argparse.ArgumentParser(
        description="Produce seed CSV rows as JSON to Kafka topics."
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
        help="Kafka bootstrap servers (default: localhost:29092)",
    )
    parser.add_argument(
        "--csv-dir",
        default=os.getenv("CSV_BASE_DIR", str(ROOT / "seeds")),
        help="Directory containing the seed CSV files (default: seeds/)",
    )
    args = parser.parse_args()

    csv_dir = Path(args.csv_dir)
    if not csv_dir.is_dir():
        raise SystemExit(f"CSV directory not found: {csv_dir}")

    producer = Producer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "linger.ms": 5,
            "batch.size": 65536,
        }
    )

    total = 0
    for stem, topic in TOPIC_MAP.items():
        csv_path = csv_dir / f"{stem}.csv"
        if not csv_path.exists():
            print(f"  WARN: {csv_path} not found — skipping topic {topic}")
            continue
        count = produce_csv(producer, csv_path, topic)
        print(f"  {topic:<25} → {count:>4} messages produced  (from {csv_path.name})")
        total += count

    print(f"\nDone. {total} total messages produced to {len(TOPIC_MAP)} topics.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
