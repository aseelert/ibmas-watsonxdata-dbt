# Docker services

The demo uses several optional local services. All are delivered as Docker
Compose stacks and managed through `bin/demo` or directly with `docker compose`.
Two services require a custom image that must be **built before the first
`docker compose up`**; the rest pull pre-built images from Docker Hub.

!!! note "Prerequisites"
    Docker Engine 24+ (or Docker Desktop) and Compose v2 (`docker compose`,
    not the legacy `docker-compose`). Confirm with:
    ```bash
    docker compose version
    ```

---

## Images to build

### Airflow — `05-airflow/airflow/Dockerfile`

The Airflow stack uses a custom image that extends the official
`apache/airflow:3.2.2` base with the demo's Python dependencies.  
The `docker-compose.yml` at the repo root references this Dockerfile via a
`build:` block, so running `docker compose up` for the airflow services will
build the image automatically on first use:

```bash
# Build only (optional — useful to verify the image builds before the demo):
docker compose build airflow-webserver

# Or let the first 'bin/demo airflow' trigger the build automatically:
bin/demo airflow
```

The image is built from the **repo root** (not from inside `05-airflow/`),
so the `COPY` path in the Dockerfile (`05-airflow/airflow/requirements.txt`)
resolves correctly.

### Flink — `04-confluent-streaming/confluent/flink/Dockerfile`

The Confluent streaming stack requires a custom Flink image tagged
`wxd-flink:1.20`. It extends `flink:1.20-scala_2.12` with the Kafka SQL
connector, Iceberg runtime, Hadoop S3A JARs, and the Confluent Avro format.

Because the `confluent/docker-compose.yml` references the image **by tag** (not
by a `build:` block), Docker will not build it automatically — it must be built
once manually before `bin/demo streaming` is called:

```bash
docker build \
  -t wxd-flink:1.20 \
  04-confluent-streaming/confluent/flink
```

The build downloads several JARs from Maven Central (~200 MB). Run it on a
fast connection; subsequent starts use the local image cache.

---

## Service stacks

All four stacks are wired into the root `docker-compose.yml` via `include:`
blocks. You can start individual stacks or all at once:

```bash
# Start ALL four stacks (heavy — only do this on a machine with ≥ 16 GB RAM):
docker compose up -d

# Or start individual stacks with bin/demo:
bin/demo metabase      # Metabase only
bin/demo airflow       # Airflow only
bin/demo lineage       # Marquez only
bin/demo catalog       # OpenMetadata + runs ingestion
bin/demo streaming     # Confluent/Flink/Iceberg (Flink image must be pre-built)
```

### Metabase — port 3000

Defined in [`02-metabase/compose.yaml`](https://github.com/aseelert/ibmas-watsonxdata-dbt/blob/main/02-metabase/compose.yaml).
Pulls `metabase/metabase:latest` and `postgres:16` — no custom build needed.

| Service | Image | Role |
| --- | --- | --- |
| `metabase-postgres` | `postgres:16` | Metabase app database |
| `metabase` | `metabase/metabase:latest` | BI front-end |
| `metabase-provision` | `python:3.12-slim` | One-shot setup (admin user + Presto connection) |

```bash
bin/demo metabase
# or directly:
docker compose -f 02-metabase/compose.yaml up -d
```

### Airflow — port 8082

Defined in [`docker-compose.yml`](https://github.com/aseelert/ibmas-watsonxdata-dbt/blob/main/docker-compose.yml) (root).
**Requires the custom image build** (see [above](#airflow-05-airflowairflowdockerfile)).

| Service | Image | Role |
| --- | --- | --- |
| `airflow-postgres` | `postgres:16` | Airflow metadata database |
| `airflow-init` | Built from `05-airflow/airflow/Dockerfile` | One-shot DB migration |
| `airflow-webserver` | Built from `05-airflow/airflow/Dockerfile` | API server and UI |
| `airflow-scheduler` | Built from `05-airflow/airflow/Dockerfile` | DAG scheduler |
| `airflow-dag-processor` | Built from `05-airflow/airflow/Dockerfile` | DAG file parsing |

```bash
bin/demo airflow
# or directly (init must complete before the others):
docker compose up -d airflow-init
docker compose up -d airflow-webserver airflow-scheduler airflow-dag-processor
```

### Confluent streaming — ports 29092, 28080–28085, 28181

Defined in
[`04-confluent-streaming/confluent/docker-compose.yml`](https://github.com/aseelert/ibmas-watsonxdata-dbt/blob/main/04-confluent-streaming/confluent/docker-compose.yml).
**Requires `wxd-flink:1.20` to be pre-built** (see [above](#flink-04-confluent-streamingconfluentflinkdockerfile)).

| Service | Image | Role |
| --- | --- | --- |
| `confluent-kafka` | `confluentinc/cp-kafka:7.7.1` | Kafka broker (KRaft, no ZooKeeper) |
| `confluent-schema-registry` | `confluentinc/cp-schema-registry:7.7.1` | Avro Schema Registry |
| `confluent-kafbat` | `ghcr.io/kafbat/kafka-ui:latest` | Kafka web UI (port 28080) |
| `confluent-iceberg-rest` | `tabulario/iceberg-rest:*` | Local Iceberg REST catalog for Flink |
| `flink-jobmanager` | `wxd-flink:1.20` (custom) | Flink cluster coordinator (port 28085) |
| `flink-taskmanager` | `wxd-flink:1.20` (custom) | Flink task execution |
| `flink-sql-gateway` | `wxd-flink:1.20` (custom) | Flink SQL Gateway (port 28083) |

```bash
# Pre-build the Flink image (once):
docker build -t wxd-flink:1.20 04-confluent-streaming/confluent/flink

# Then start the stack:
bin/demo streaming
# or the full orchestrator directly:
bash 04-confluent-streaming/confluent/start.sh --all
```

### Marquez — ports 3001, 5010, 5012

Defined in
[`06-openlineage-marquez/openlineage-marquez/docker-compose.yml`](https://github.com/aseelert/ibmas-watsonxdata-dbt/blob/main/06-openlineage-marquez/openlineage-marquez/docker-compose.yml).
No custom build needed.

```bash
bin/demo lineage
# or directly:
docker compose -f 06-openlineage-marquez/openlineage-marquez/docker-compose.yml up -d
```

### OpenMetadata — port 8585

Defined in
[`07-openmetadata/openmetadata/docker-compose.yml`](https://github.com/aseelert/ibmas-watsonxdata-dbt/blob/main/07-openmetadata/openmetadata/docker-compose.yml).
No custom build needed. `bin/demo catalog` also runs the ingestion pipeline
after the containers are up.

```bash
bin/demo catalog
# or just the containers:
docker compose -f 07-openmetadata/openmetadata/docker-compose.yml up -d
```

---

## Minimal startup sequence (first time)

```bash
# 1. Prepare .env and Python virtual environment (see Environment setup):
cp .env.example .env          # fill in WXD_* values
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
bin/demo setup

# 2. Build custom images (one-time):
docker compose build airflow-webserver          # Airflow image (~1 min)
docker build -t wxd-flink:1.20 \
  04-confluent-streaming/confluent/flink        # Flink image (~3–5 min)

# 3. Start whichever services you need:
bin/demo metabase
bin/demo airflow
bin/demo streaming
bin/demo lineage
bin/demo catalog
```

---

## Teardown

```bash
# Stop all stacks, keep volumes:
docker compose down

# Stop all stacks and remove all demo volumes:
docker compose down -v --remove-orphans

# Coordinated full demo reset (preferred):
bin/demo reset --dry-run          # preview first
bin/demo reset --docker           # then execute
```

See [Scripts and automation](scripts.md) for the `reset` script details and
[Access and interfaces](access.md) for the complete port reference.
