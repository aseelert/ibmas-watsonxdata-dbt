#!/usr/bin/env python3
# =============================================================================
#  ingest_csv_to_kafka.py — produce the 4 seed CSVs to Kafka as governed Avro
# -----------------------------------------------------------------------------
#  Location  : confluent/scripts/ingest_csv_to_kafka.py
#  Repository: https://github.ibm.com/alexander/ibmas-watsonxdata-dbt
#  Project   : watsonx.data · dbt · Spark · Confluent medallion demo
#  Author    : Alexander Seelert — IBM Customer Success Engineer
#  Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.
#
#  Changelog :
#    v1.0 (2026-06-26) — Initial version. Read the 4 seed CSVs and produce each
#      row as an industry-standard Avro message governed by Confluent Schema
#      Registry: one registered subject "<topic>-value" per topic, using the
#      Avro contracts in confluent/schemas/*.avsc. Replaces the old schemaless
#      JSON producer. CSV stays the single source of truth.
# -----------------------------------------------------------------------------
#
#  WHAT / WHY (for an 18-year-old learner)
#    A real streaming platform never ships "naked" JSON — every message carries
#    a CONTRACT (an Avro schema) so producers and consumers can never disagree
#    about the shape of the data. We do the same here:
#
#      seeds/raw_*.csv  ──read──>  coerce to the .avsc types  ──Avro+SchemaRegistry──>  Kafka topic
#
#    For each topic we load the matching Avro schema from confluent/schemas/,
#    hand it to confluent-kafka's AvroSerializer, and the first message we send
#    auto-registers the subject "<topic>-value" in the Schema Registry. Flink
#    then reads those same registered schemas back (format = avro-confluent), so
#    the whole pipeline is type-safe end to end.
#
#  USAGE (from repo root, using .venv)
#    .venv/bin/python confluent/scripts/ingest_csv_to_kafka.py
#    .venv/bin/python confluent/scripts/ingest_csv_to_kafka.py \
#      --bootstrap-servers localhost:29092 \
#      --schema-registry-url http://localhost:28081 \
#      --csv-dir seeds/ --schemas-dir confluent/schemas/
#
#  ENV VARS
#    KAFKA_BOOTSTRAP_SERVERS  — default: localhost:29092
#    SCHEMA_REGISTRY_URL      — default: http://localhost:28081 (HOST view)
#    CSV_BASE_DIR             — default: <repo>/seeds
#
#  PREREQUISITES  (all in requirements.txt — installed into .venv)
#    pip install -r requirements.txt   # or: bash confluent/start.sh (auto-installs)
# =============================================================================
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
from pathlib import Path

# --- Optional .env loader -----------------------------------------------------
try:
    from dotenv import load_dotenv
except ImportError:  # dotenv is optional — real env vars work too
    load_dotenv = None

# --- Hard dependencies (clear message if the venv is missing them) ------------
try:
    from confluent_kafka import Producer
    from confluent_kafka.serialization import MessageField, SerializationContext
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroSerializer
except ImportError as exc:  # pragma: no cover - environment guard
    raise SystemExit(
        "Missing Avro dependencies. Install with:\n"
        "    pip install -r requirements.txt\n"
        f"(original error: {exc})"
    ) from exc

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s  %(message)s",
    stream=sys.stderr,
)
log = logging.getLogger("ingest_csv_to_kafka")

_SCRIPT = Path(__file__).resolve()
ROOT = _SCRIPT.parents[2] if len(_SCRIPT.parents) > 2 else _SCRIPT.parent

# Map CSV filename stem → (Kafka topic, Avro schema file stem).
# Topic == CSV stem == schema stem on purpose: one obvious name per entity.
ENTITIES = {
    "raw_customers":   "raw_customers",
    "raw_products":    "raw_products",
    "raw_orders":      "raw_orders",
    "raw_order_items": "raw_order_items",
}


def _delivery_report(err, msg) -> None:
    """Called once per message after the broker (n)acks it."""
    if err is not None:
        log.error("delivery failed for %s: %s", msg.topic(), err)


# ---------------------------------------------------------------------------
# Type coercion driven by the Avro schema
# ---------------------------------------------------------------------------
#  CSV gives us everything as text. Avro is typed, so before we serialize a row
#  we must turn "1001" into the int 1001 and "49.9" into the float 49.9, etc.
#  We read the REQUIRED type straight out of the .avsc so the CSV and the schema
#  can never drift apart — change the schema and the coercion follows for free.
# ---------------------------------------------------------------------------

def _primitive(avro_type) -> tuple[str, bool]:
    """Return (base_type, nullable) for an Avro field type (handles ["null", T])."""
    nullable = False
    if isinstance(avro_type, list):  # a union, e.g. ["null", "int"]
        nullable = "null" in avro_type
        non_null = [t for t in avro_type if t != "null"]
        avro_type = non_null[0] if non_null else "string"
    if isinstance(avro_type, dict):  # logical/complex type — treat payload as-is
        avro_type = avro_type.get("type", "string")
    return str(avro_type), nullable


def _build_coercers(schema_dict: dict) -> dict:
    """Map each field name → a function that converts a CSV string to its Avro type."""
    coercers: dict = {}
    for field in schema_dict.get("fields", []):
        name = field["name"]
        base, nullable = _primitive(field["type"])

        def make(base=base, nullable=nullable, name=name):
            def coerce(raw: str):
                text = (raw or "").strip()
                if text == "":
                    if nullable:
                        return None
                    # Required field but empty cell — signal the caller to skip the row.
                    raise ValueError(f"required field '{name}' is empty")
                if base in ("int", "long"):
                    return int(float(text)) if ("." in text or "e" in text.lower()) else int(text)
                if base in ("float", "double"):
                    return float(text)
                if base == "boolean":
                    return text.lower() in ("1", "true", "yes")
                return text  # string / bytes / everything else stays as text
            return coerce

        coercers[name] = make()
    return coercers


def _load_schema(schemas_dir: Path, stem: str) -> tuple[str, dict]:
    """Read a .avsc file and return (raw_json_string, parsed_dict)."""
    path = schemas_dir / f"{stem}.avsc"
    if not path.exists():
        raise SystemExit(f"Avro schema not found: {path}")
    schema_str = path.read_text(encoding="utf-8")
    return schema_str, json.loads(schema_str)


def produce_csv(
    producer: Producer,
    serializer: AvroSerializer,
    coercers: dict,
    csv_path: Path,
    topic: str,
) -> tuple[int, int]:
    """Produce every row of a CSV file to a Kafka topic as Avro.

    Returns (produced, skipped)."""
    produced = 0
    skipped = 0
    ctx = SerializationContext(topic, MessageField.VALUE)

    with csv_path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for line_no, row in enumerate(reader, start=2):  # header is line 1
            try:
                value = {name: coercers[name](row.get(name, "")) for name in coercers}
            except (ValueError, KeyError) as exc:
                # Bad/empty mandatory cell — drop the row but keep going (data quality).
                log.warning("%s line %d skipped: %s", csv_path.name, line_no, exc)
                skipped += 1
                continue

            producer.produce(
                topic=topic,
                value=serializer(value, ctx),
                callback=_delivery_report,
            )
            produced += 1
            if produced % 100 == 0:  # drain the delivery queue periodically
                producer.poll(0)

    producer.flush()
    return produced, skipped


def main() -> int:
    if load_dotenv is not None:
        load_dotenv(ROOT / ".env")

    parser = argparse.ArgumentParser(
        description="Produce the 4 seed CSV files to Kafka as Avro governed by "
                    "Confluent Schema Registry (one subject '<topic>-value' per topic).",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
        help="Kafka bootstrap servers (default: localhost:29092)",
    )
    parser.add_argument(
        "--schema-registry-url",
        default=os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:28081"),
        help="Confluent Schema Registry URL (default: http://localhost:28081)",
    )
    parser.add_argument(
        "--csv-dir",
        default=os.getenv("CSV_BASE_DIR", str(ROOT / "seeds")),
        help="Directory containing the seed CSV files (default: <repo>/seeds)",
    )
    parser.add_argument(
        "--schemas-dir",
        default=str(_SCRIPT.parent.parent / "schemas"),
        help="Directory containing the .avsc Avro schemas (default: confluent/schemas)",
    )
    args = parser.parse_args()

    csv_dir = Path(args.csv_dir)
    schemas_dir = Path(args.schemas_dir)
    if not csv_dir.is_dir():
        raise SystemExit(f"CSV directory not found: {csv_dir}")
    if not schemas_dir.is_dir():
        raise SystemExit(f"Schemas directory not found: {schemas_dir}")

    log.info("Kafka bootstrap : %s", args.bootstrap_servers)
    log.info("Schema Registry : %s", args.schema_registry_url)
    log.info("CSV dir         : %s", csv_dir)
    log.info("Schemas dir     : %s", schemas_dir)

    sr_client = SchemaRegistryClient({"url": args.schema_registry_url})
    producer = Producer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "linger.ms": 5,
            "batch.size": 65536,
        }
    )

    total_produced = 0
    total_skipped = 0
    for stem, topic in ENTITIES.items():
        csv_path = csv_dir / f"{stem}.csv"
        if not csv_path.exists():
            log.warning("%s not found — skipping topic %s", csv_path, topic)
            continue

        schema_str, schema_dict = _load_schema(schemas_dir, stem)
        coercers = _build_coercers(schema_dict)
        # to_dict is identity: our value is already a plain dict matching the schema.
        serializer = AvroSerializer(
            sr_client, schema_str, to_dict=lambda obj, ctx: obj
        )

        produced, skipped = produce_csv(
            producer, serializer, coercers, csv_path, topic
        )
        log.info(
            "%-25s -> %4d Avro messages (subject '%s-value', %d skipped)",
            topic, produced, topic, skipped,
        )
        total_produced += produced
        total_skipped += skipped

    log.info(
        "Done. %d Avro messages produced to %d topics (%d rows skipped).",
        total_produced, len(ENTITIES), total_skipped,
    )
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:  # pragma: no cover
        log.error("interrupted by user")
        sys.exit(130)
    except SystemExit:
        raise
    except Exception as exc:  # pragma: no cover - top-level safety net
        log.exception("ingest failed: %s", exc)
        sys.exit(1)
