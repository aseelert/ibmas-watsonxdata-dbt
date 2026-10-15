# Image and component versions

Every Docker image in this demo is **pinned to an exact version** — no `latest`
tags — so you always know what is running. This page was last verified on
**2026-10-15**. The date column is when that exact tag was published (or last
rebuilt) on its registry, so you can see at a glance that nothing here is old
or abandoned.

| Component | Version | Published | Where it is pinned |
|---|---|---|---|
| Kafka (Confluent `cp-kafka`) | 8.3.2 | 2026-09-16 | `04-confluent-streaming/confluent/docker-compose.yml` |
| Schema Registry (`cp-schema-registry`) | 8.3.2 | 2026-09-16 | same |
| Flink (base image `flink`) | 2.2.1 | 2026-10-02 | `.../flink/Dockerfile` (built as `wxd-flink:2.3`) |
| Iceberg Flink runtime + AWS bundle | 1.12.0 | 2026-09-25 | `.../flink/Dockerfile` (`ICEBERG_VER`) |
| Flink Kafka SQL connector | 5.0.0-2.2 | 2026-05-12 | `.../flink/Dockerfile` |
| Flink Avro / Schema Registry format | 2.3.0 | 2026-06 | `.../flink/Dockerfile` |
| Iceberg REST catalog (`apache/iceberg-rest-fixture`) | digest `sha256:db8de90b…`, built from Iceberg `main` commit `b0df3ca` | 2026-04-29 | `.../docker-compose.yml` |
| Kafbat UI | v1.5.0 | 2026-04-20 | `.../docker-compose.yml` |
| OpenMetadata (server, db, ingestion) | 2.0.5 | 2026-10-08 / 10-09 | `07-openmetadata/openmetadata/docker-compose.yml` |
| `openmetadata-ingestion` (pip) | 2.0.5.0 | 2026-10-08 | `07-openmetadata/openmetadata/ingestion/run-ingestion.sh` |
| Elasticsearch (for OpenMetadata) | 9.3.0 | — | same compose file |
| Airflow | 3.3.2 | 2026-09-17 | `05-airflow/airflow/Dockerfile` |
| Metabase | v0.64.2.5 | 2026-10-15 | `02-metabase/compose.yaml` |
| PostgreSQL | 18.6 | 2026 | root, Metabase and Marquez compose files |
| Marquez and Marquez web | 0.51.1 | 2025-03-27 | `06-openlineage-marquez/openlineage-marquez/docker-compose.yml` |

## Notes

- **Flink 2.3.0 runs the Iceberg 1.12.0 and Kafka connector builds made for
  Flink 2.2**, because no 2.3 builds exist yet. The full Kafka → Flink → Iceberg
  silver pipeline and the Spark gold step were verified end to end (2026-10-15).
- **The Iceberg REST fixture has no versioned tag after 1.10.1.** Its `latest`
  tag is a build of Iceberg's `main` branch, so it is pinned by image digest to
  stay reproducible.
- **Marquez 0.51.1 is the newest Marquez release, but it is about 18 months
  old.** The project is slow-moving; check
  [github.com/MarquezProject/marquez](https://github.com/MarquezProject/marquez)
  before relying on it for a new customer demo.
- **PostgreSQL 18 mounts its volume at `/var/lib/postgresql`** (not
  `/var/lib/postgresql/data` as in 16 and earlier). Old volumes from earlier
  versions cannot be reused — start from empty volumes.
- **Elasticsearch stays on 9.3.0**, the version the OpenMetadata 2.0.5
  upstream compose file ships with.
- **Airflow 3.4 is still a beta**, so the demo uses the latest stable 3.3.2.
- **The MinIO Route stays HTTPS-only.** The Iceberg REST and Flink containers
  trust the cluster's ingress CA through a Java truststore that
  `04-confluent-streaming/confluent/scripts/build_truststore.sh` builds
  automatically on every `bin/demo docker start` / `bin/demo streaming`.
