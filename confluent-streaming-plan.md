# Confluent Streaming Stack Plan
## Kafka + Flink + Iceberg → watsonx.data (silver layer, parallel to dbt/Spark)

---

## Top-Level Overview

Add a fully self-contained **Confluent streaming path** to the existing medallion demo.
This is the **fourth approach** (alongside dbt, Spark, and cpdctl) that produces silver
Iceberg tables in watsonx.data — but does it via Kafka streaming + Flink SQL instead of
batch CSV seeds.

**All new files live under `confluent/`**. The root `docker-compose.yml` gets one new
`include:` line pointing at `confluent/docker-compose.yml`. Nothing in the existing
Airflow, dbt, or Spark stacks is modified.

---

## Architecture: Remote `iceberg-bucket` via OpenShift Route

### The problem and its solution

`ibm-lh-lakehouse-minio-svc` is a **ClusterIP service with no Route** — not reachable
from Docker on a laptop. `oc port-forward` only works on the host's loopback
(`127.0.0.1`), not from inside a Docker container. The DAS/CAS endpoint uses
proprietary IBM auth — not standard S3.

**Solution (confirmed in [`docs/configuration.md:184`](docs/configuration.md:184)):**
> *"non-localhost URL works only if an administrator first exposes MinIO via an
> OpenShift Route"*

We create that Route using `oc expose` — the same `oc` tooling already used by
[`scripts/upload_spark_assets.py`](scripts/upload_spark_assets.py) and
[`scripts/reset_demo.sh`](scripts/reset_demo.sh), reading credentials from the
same `.env` file. The Route is **permanent** (survives restarts, no tunnel needed)
and gives every Docker container a stable HTTPS URL for MinIO.

### New script: `confluent/scripts/expose_minio_route.sh`

A one-time setup script (run once by the demo operator before `docker compose up`):

```bash
#!/usr/bin/env bash
# Load WXD_OPENSHIFT_NAMESPACE, WXD_OPENSHIFT_CONTEXT, WXD_OPENSHIFT_API from .env
source .env

# 1. Log in if not already authenticated
oc whoami --show-server 2>/dev/null || \
  oc login "${WXD_OPENSHIFT_API}" --token="${WXD_OC_TOKEN}"

# 2. Create the Route (idempotent — errors if already exists, which is fine)
oc -n "${WXD_OPENSHIFT_NAMESPACE:-cpd-instance}" \
   expose svc/ibm-lh-lakehouse-minio-svc \
   --name=ibm-lh-minio-route \
   --port=9000 \
   --overrides='{"spec":{"tls":{"termination":"edge","insecureEdgeTerminationPolicy":"Redirect"}}}' \
   --dry-run=none 2>/dev/null || echo "Route already exists — OK"

# 3. Read and print the assigned hostname
MINIO_ROUTE=$(oc -n "${WXD_OPENSHIFT_NAMESPACE:-cpd-instance}" \
  get route ibm-lh-minio-route \
  -o jsonpath='{.spec.host}')

echo "MinIO Route: https://${MINIO_ROUTE}"
echo "Add to .env:  WXD_OBJECT_STORE_ENDPOINT=https://${MINIO_ROUTE}"
```

The Route hostname follows the cluster's wildcard DNS pattern:
`ibm-lh-minio-route-cpd-instance.apps.watson.ibmas-zocp-techcluster.org`

This URL is reachable from any Docker container via `extra_hosts: host.docker.internal`
or directly by DNS — **no port-forward, no tunnel, no local MinIO**.

### Confirmed connectivity after Route creation

| Interface | From Docker? | Used by | Notes |
|---|---|---|---|
| `https://ibm-lh-minio-route-cpd-instance.apps...` | ✅ Yes — HTTPS Route | Flink S3A, iceberg-rest, confluent-prep | Standard AWS SigV4, same creds as existing scripts |
| `WXD_HOST:443` (Presto) | ✅ Yes | confluent-prep | Schema creation + table visibility |
| HMS thrift `:9083` | ❌ No | — | Not needed — Flink uses Iceberg REST catalog |
| DAS/CAS | ❌ | — | Non-standard auth, not used |
| `oc port-forward` | ❌ In Docker | — | Host only — replaced by Route |

### Final architecture — Flink writes directly to `iceberg-bucket`

```
One-time setup (run on host before docker compose up):
  confluent/scripts/expose_minio_route.sh
    → creates OpenShift Route: ibm-lh-minio-route
    → sets WXD_OBJECT_STORE_ENDPOINT=https://ibm-lh-minio-route-cpd-instance.apps...

docker compose up -d

┌─ confluent-network ────────────────────────────────────────────────────────────┐
│                                                                                 │
│  seeds/*.csv → ingest_csv_to_kafka.py → confluent-kafka:9092                  │
│                                               │                                 │
│                                    Flink SQL (silver_jobs.sql)                 │
│                             catalog: iceberg-rest    data: S3A → Route         │
│                                    ↓                       ↓                   │
│                      confluent-iceberg-rest:8181    https://minio-route...     │
│                      (REST catalog, SQLite)         → iceberg-bucket           │
│                                                       (real watsonx.data MinIO) │
└─────────────────────────────────────────────────────────────────────────────────┘

After Flink checkpoints (confluent-prep):
  1. prestodb → CREATE SCHEMA IF NOT EXISTS iceberg_data.confluent_silver
  2. requests → GET confluent-iceberg-rest:8181/v1/namespaces/confluent_silver/tables/silver_customers
                → metadata_location: s3://iceberg-bucket/confluent_silver/.../v1.metadata.json
  3. prestodb → CALL iceberg_data.system.register_table(
                  schema_name => 'confluent_silver',
                  table_name  => 'silver_customers',
                  metadata_location => 's3://iceberg-bucket/confluent_silver/.../v1.metadata.json'
                )
  → iceberg_data.confluent_silver.silver_customers now queryable in watsonx.data ✅
```

**Data files land on the real `iceberg-bucket`** (watsonx.data's MinIO). After
`register_table`, Presto reads both metadata and data directly from `iceberg-bucket`
— same as dbt and Spark tables. No copy, no local MinIO, no sync step.

### `confluent-iceberg-rest` configuration with Route

```yaml
confluent-iceberg-rest:
  environment:
    CATALOG_WAREHOUSE:             "s3://iceberg-bucket/confluent_silver/"
    CATALOG_S3_ENDPOINT:           "${WXD_OBJECT_STORE_ENDPOINT}"   # Route URL
    CATALOG_S3_PATH__STYLE__ACCESS: "true"
    CATALOG_IO__IMPL:              "org.apache.iceberg.aws.s3.S3FileIO"
    AWS_ACCESS_KEY_ID:             "${WXD_OBJECT_STORE_ACCESS_KEY}"
    AWS_SECRET_ACCESS_KEY:         "${WXD_OBJECT_STORE_SECRET_KEY}"
    AWS_REGION:                    "${WXD_OBJECT_STORE_REGION:-us-east-1}"
    # SQLite JDBC catalog with WAL + IMMEDIATE (firefighter pattern)
    CATALOG_CATALOG__IMPL:         "org.apache.iceberg.jdbc.JdbcCatalog"
    CATALOG_URI:                   "jdbc:sqlite:/tmp/iceberg.db?busy_timeout=30000&journal_mode=WAL&transaction_mode=IMMEDIATE"
    CATALOG_JDBC_SCHEMA__VERSION:  "V1"
```

### Flink Dockerfile — S3A JARs needed

Because Flink writes data files to the real MinIO via the HTTPS Route (standard S3A):
- `flink-sql-connector-kafka-3.3.0-1.20.jar` — Kafka source
- `iceberg-flink-runtime-1.20-1.9.1.jar` — Iceberg sink + REST catalog client
- `flink-s3-fs-hadoop-1.20.0.jar` — move from `/opt/flink/opt/` (bundled, just activate)
- `hadoop-aws-3.3.4.jar` — S3A connector
- `aws-java-sdk-bundle-1.12.367.jar` — AWS SDK

### `register_table` — why it works now

After the Route is in place:
- Flink writes `s3://iceberg-bucket/confluent_silver/...` data files to the **real
  `iceberg-bucket`** via the Route
- `confluent-iceberg-rest` records `metadata_location =
  s3://iceberg-bucket/confluent_silver/silver_customers/metadata/v1.metadata.json`
- `confluent-prep` calls `register_table` with that path
- Presto reads it from `iceberg-bucket` (cluster-internal access, always available)
- ✅ Tables visible in watsonx.data — **same bucket, same catalog, different schema prefix**

### `WXD_OC_TOKEN` — new required env var

The Route script needs an active `oc` session. Add `WXD_OC_TOKEN` to `.env.example`
alongside the existing `WXD_OPENSHIFT_API`. This is the only new credential.
All other MinIO credentials (`WXD_OBJECT_STORE_ACCESS_KEY`, `WXD_OBJECT_STORE_SECRET_KEY`)
are already in `.env` and are reused directly.

### Sub-task conclusions

- **Sub-Task 0 (new):** `confluent/scripts/expose_minio_route.sh` — one-time `oc expose`
  that creates the MinIO Route and prints the URL to add to `.env` as
  `WXD_OBJECT_STORE_ENDPOINT`
- **Sub-Task 1:** No local MinIO container. `confluent-iceberg-rest` backed by real
  `iceberg-bucket` via `WXD_OBJECT_STORE_ENDPOINT` (Route URL)
- **Sub-Task 2:** Flink Dockerfile needs all 5 JARs including S3A (writes to real MinIO)
- **Sub-Task 5:** `prep_iceberg_schemas.py` — Phase A: `CREATE SCHEMA` via Presto;
  Phase B: read `metadata_location` from `confluent-iceberg-rest` REST API → call
  `register_table` via Presto. Uses `prestodb` + `requests` only (already in `requirements.txt`)
- **Sub-Task 6:** `silver_jobs.sql` — `catalog-type=rest`, `uri=confluent-iceberg-rest:8181`,
  `warehouse=s3://iceberg-bucket/confluent_silver/` (real bucket)
- **Sub-Task 8:** Add `WXD_OC_TOKEN` + Route URL docs to `.env.example`. No local MinIO creds.

---

### What the streaming path does

```
CSVs → Python ingest script → Kafka topics (4 topics, 1 per entity)
                                        │
                          Schema Registry (JSON Schema)
                                        │
                              Flink SQL (silver transforms)
                              — mirrors dbt silver logic exactly —
                                        │
                         Iceberg tables in watsonx.data
                         confluent_silver_customers
                         confluent_silver_products
                         confluent_silver_orders
                         confluent_silver_order_items
                         confluent_silver_sales_enriched   ← joined fact
                                        │
                         (Tableflow path, separate)
                         confluent_tableflow_customers
                         confluent_tableflow_products
                         confluent_tableflow_orders
                         confluent_tableflow_order_items
```

### Schema naming convention

| Path     | Catalog        | Schema prefix         | Example table                       |
|----------|---------------|-----------------------|-------------------------------------|
| dbt      | iceberg_data  | dbt_demo_silver       | dbt_demo_silver.silver_customers    |
| Spark    | iceberg_data  | spark_demo_silver     | spark_demo_silver.silver_customers  |
| Flink    | iceberg_data  | confluent_silver      | confluent_silver.silver_customers   |
| Tableflow| iceberg_data  | confluent_tableflow   | confluent_tableflow.orders          |

### Port allocation (no conflicts with existing stack)

| Service             | Container port | Host port | Existing ports used |
|---------------------|---------------|-----------|---------------------|
| Kafka (KRaft)       | 9092 / 9093   | **29092** | (airflow 8082, meta 3000, openmetadata 8585) |
| Schema Registry     | 8081          | **28081** | |
| Kafbat UI           | 8080          | **28080** | |
| Flink JobManager    | 8081          | **28085** | |
| Flink SQL Gateway   | 8083          | **28083** | |

---

## Sub-Tasks

---

### Sub-Task 1 — `confluent/docker-compose.yml`

**Intent:** Define all Confluent streaming services in a standalone compose file that is
included by the root `docker-compose.yml`. Modelled directly on the firefighter
`docker-compose.local.yml` reference with adjusted ports to avoid collisions.

**Expected Outcomes:**
- `docker compose up -d` (from repo root) starts the full stack including the confluent services
- Alternatively `docker compose -f confluent/docker-compose.yml up -d` starts only the
  confluent stack independently
- Services: `confluent-kafka`, `confluent-schema-registry`, `confluent-kafbat-ui`,
  `confluent-flink-jobmanager`, `confluent-flink-taskmanager`, `confluent-flink-sql-gateway`,
  `confluent-kafka-init` (one-shot), `confluent-flink-runner` (one-shot)
- All services on dedicated network `confluent-network`; `extra_hosts: host.docker.internal`
  so Flink can reach watsonx.data Presto/Hive Metastore through the host

**Todo List:**
1. Create `confluent/docker-compose.yml` with the following services:
   - `confluent-kafka`: `confluentinc/cp-kafka:7.7.1`, KRaft mode, `CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qk`,
     `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"`, host port **29092**, volume `confluent-kafka-data`
   - `confluent-schema-registry`: `confluentinc/cp-schema-registry:7.7.1`, host port **28081**,
     depends on `confluent-kafka` healthy
   - `confluent-kafbat-ui`: `ghcr.io/kafbat/kafka-ui:latest`, host port **28080**,
     `KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=confluent-kafka:9092`,
     `KAFKA_CLUSTERS_0_SCHEMAREGISTRY=http://confluent-schema-registry:8081`,
     `DYNAMIC_CONFIG_ENABLED=true`
   - `confluent-iceberg-rest`: `apache/iceberg-rest-fixture:1.9.1`, host port **28181**,
     configured with `CATALOG_WAREHOUSE=s3://iceberg-bucket/confluent/`,
     `CATALOG_IO__IMPL=org.apache.iceberg.aws.s3.S3FileIO`,
     `CATALOG_S3_ENDPOINT=http://host.docker.internal:19000` (real watsonx.data MinIO via port-forward),
     `AWS_ACCESS_KEY_ID=${WXD_OBJECT_STORE_ACCESS_KEY}`, `AWS_SECRET_ACCESS_KEY=${WXD_OBJECT_STORE_SECRET_KEY}`,
     SQLite JDBC catalog with WAL + IMMEDIATE transaction mode (same as firefighter pattern),
     `extra_hosts: host.docker.internal:host-gateway`
     **No local MinIO container needed** — data writes go to the real `iceberg-bucket`
   - `confluent-flink-jobmanager`: custom image built from `confluent/flink/Dockerfile`,
     host port **28085** (Web UI), checkpointing 30s, `parallelism.default: 2`
   - `confluent-flink-taskmanager`: same image, `taskmanager.numberOfTaskSlots: 8`,
     `taskmanager.memory.process.size: 2048m`
   - `confluent-flink-sql-gateway`: same image, host port **28083**
   - `confluent-kafka-init`: one-shot using `confluentinc/cp-kafka:7.7.1`,
     mounts `confluent/scripts/create-topics.sh`, runs after kafka healthy
   - `confluent-flink-runner`: one-shot using flink image,
     mounts `confluent/flink/sql/silver_jobs.sql` and `confluent/scripts/submit-flink.sh`,
     depends on kafka-init `service_completed_successfully` + flink-jobmanager started +
     iceberg-rest healthy
   - `confluent-schema-prep`: one-shot Python container (`python:3.12-slim`),
     runs Phase A of `prep_iceberg_schemas.py` (schema creation only),
     depends on kafka-init `service_completed_successfully`, before flink-runner starts,
     uses `.env` via `env_file`
   - `confluent-prep`: one-shot Python container (`python:3.12-slim`), runs
     Phase B of `prep_iceberg_schemas.py` (table registration into watsonx.data via Presto),
     depends on flink-runner `service_completed_successfully`,
     uses `.env` (watsonx.data credentials) via `env_file`;
     this is the **mount step** — registers Flink-written Iceberg tables into `iceberg_data`
2. Define named volumes: `confluent-kafka-data` only (no local MinIO needed)
3. Define network: `confluent-network` with `extra_hosts: host.docker.internal:host-gateway`
   so all containers can reach the host's `oc port-forward` at `host.docker.internal:19000`
   and reach Presto at `host.docker.internal` (resolved via the gateway)
4. Add `include: - confluent/docker-compose.yml` at the bottom of root `docker-compose.yml`
   (alongside existing metabase and openmetadata includes)

**Relevant Context:**
- Root [`docker-compose.yml`](docker-compose.yml:136) — existing `include:` pattern at line 136-138
- Firefighter reference: KRaft config with `CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qk`,
  health-check `kafka-topics --bootstrap-server localhost:9092 --list`
- `.env.example` — `WXD_CATALOG`, `WXD_HOST`, `WXD_API_KEY`, `WXD_PORT` already defined

**Status:** `[ ] pending`

---

### Sub-Task 2 — `confluent/flink/Dockerfile`

**Intent:** Build a custom Flink 1.20 image that bundles the Kafka SQL connector JAR,
the Iceberg Flink runtime JAR (for watsonx.data Iceberg sink), and the Hadoop AWS JAR
needed for S3/HMS connectivity.

**Expected Outcomes:**
- Image tagged `wxd-flink:1.20` (same pattern as firefighter `fire-flink:1.20`)
- Image contains all 5 JARs:
  1. `flink-sql-connector-kafka-3.3.0-1.20.jar` — Kafka source connector
  2. `iceberg-flink-runtime-1.20-1.9.1.jar` — Iceberg sink + REST catalog client
  3. `flink-s3-fs-hadoop-1.20.0.jar` — Flink's bundled S3A plugin (already in `/opt/flink/opt/`, just move it)
  4. `hadoop-aws-3.3.4.jar` — AWS S3A connector for Hadoop (needed for real MinIO writes)
  5. `aws-java-sdk-bundle-1.12.367.jar` — AWS SDK (required by Hadoop S3A)
- Flink SQL can write data files to `s3://iceberg-bucket/` via `host.docker.internal:19000`
- `CREATE CATALOG local_iceberg WITH ('catalog-type'='rest', 'uri'='http://confluent-iceberg-rest:8181')` works

**Todo List:**
1. Create `confluent/flink/Dockerfile`:
   - `FROM flink:1.20-scala_2.12`
   - Download `flink-sql-connector-kafka-3.3.0-1.20.jar` from Maven Central into
     `/opt/flink/lib/`
   - Download `iceberg-flink-runtime-1.20-1.9.1.jar` from Apache releases into
     `/opt/flink/lib/`
   - Move `/opt/flink/opt/flink-s3-fs-hadoop-1.20.0.jar` → `/opt/flink/lib/`
     (already bundled in the base Flink image, just needs activating)
   - Download `hadoop-aws-3.3.4.jar` from Maven Central into `/opt/flink/lib/`
   - Download `aws-java-sdk-bundle-1.12.367.jar` from Maven Central into `/opt/flink/lib/`
   - Set `ENV HADOOP_CLASSPATH=/opt/flink/lib`
   - Set `ENV FLINK_PROPERTIES` defaults for `jobmanager.rpc.address`, `rest.address`,
     `s3.endpoint: http://host.docker.internal:19000`,
     `s3.path.style.access: true`

**Relevant Context:**
- Firefighter reference: same `FROM flink:1.20-scala_2.12` base image
- **S3A JARs are needed** because data files land on real MinIO (`iceberg-bucket`) via
  the host port-forward — not local storage. Flink's Iceberg sink uses S3FileIO
  which requires the S3A JARs for the actual file writes
- Flink S3 plugin JAR location: `/opt/flink/opt/flink-s3-fs-hadoop-1.20.0.jar` — ships
  with the base image but must be moved to `/opt/flink/lib/` to be active
- All connector JARs must match Flink 1.20 — wrong versions cause `ClassNotFoundException`

**Status:** `[ ] pending`

---

### Sub-Task 3 — `confluent/scripts/create-topics.sh`

**Intent:** One-shot bootstrap script that creates the 4 Kafka topics corresponding
to the 4 CSV entity files. Topic names match the CSV seed names for clarity.

**Expected Outcomes:**
- Topics created: `raw_customers`, `raw_products`, `raw_orders`, `raw_order_items`
- 1 partition, replication factor 1 (single-broker local demo)
- Idempotent: script exits 0 even if topics already exist

**Todo List:**
1. Create `confluent/scripts/create-topics.sh`:
   ```bash
   #!/bin/bash
   # Wait for broker to be ready, then create 4 topics
   BROKER="${KAFKA_BROKER:-confluent-kafka:9092}"
   for topic in raw_customers raw_products raw_orders raw_order_items; do
     kafka-topics --bootstrap-server "$BROKER" \
       --create --if-not-exists \
       --topic "$topic" \
       --partitions 1 \
       --replication-factor 1
   done
   ```
2. Make executable (`chmod +x`)

**Relevant Context:**
- Firefighter `create-topics.sh` pattern: loop over topic list with `--if-not-exists`
- Seeds: [`seeds/raw_customers.csv`](seeds/raw_customers.csv), [`seeds/raw_orders.csv`](seeds/raw_orders.csv),
  [`seeds/raw_products.csv`](seeds/raw_products.csv), [`seeds/raw_order_items.csv`](seeds/raw_order_items.csv)

**Status:** `[ ] pending`

---

### Sub-Task 4 — `confluent/scripts/ingest_csv_to_kafka.py`

**Intent:** Standalone Python script that reads the 4 existing seed CSV files from
`seeds/` and produces each row as a JSON message to the corresponding Kafka topic.
Nothing more — no schema transformation, just raw CSV rows as JSON.

**Expected Outcomes:**
- Each CSV row becomes one JSON message with the same column names as CSV headers
- Produces to: `raw_customers`, `raw_products`, `raw_orders`, `raw_order_items`
- Prints progress per topic (rows produced, any errors)
- Requires only `confluent-kafka` Python package (already in `requirements.txt` or
  pip-installable)
- Configurable via env vars: `KAFKA_BOOTSTRAP_SERVERS` (default `localhost:29092`),
  `CSV_BASE_DIR` (default `seeds/`)

**Todo List:**
1. Create `confluent/scripts/ingest_csv_to_kafka.py`
2. Use `confluent_kafka.Producer` (not `kafka-python`) — matches the existing project's
   Python ecosystem
3. Map topic names exactly: `raw_customers.csv → raw_customers`, etc.
4. Read CSV with `csv.DictReader` — preserves column names as JSON keys
5. Serialize each row as `json.dumps(row)` → UTF-8 bytes
6. Call `producer.flush()` after each topic, print count
7. Add `if __name__ == "__main__"` guard and `argparse` for `--bootstrap-servers`
   and `--csv-dir` flags (env vars as defaults)

**Relevant Context:**
- Seeds dir: [`seeds/`](seeds/) — 4 CSV files, ~50 rows each
- CSV columns:
  - `raw_customers`: customer_id, first_name, last_name, email, signup_date, country
  - `raw_products`: product_id, product_name, category, unit_price
  - `raw_orders`: order_id, customer_id, order_ts, status, payment_method
  - `raw_order_items`: order_item_id, order_id, product_id, quantity, discount_pct

**Status:** `[ ] pending`

---

### Sub-Task 5 — `confluent/scripts/prep_iceberg_schemas.py`

**Intent:** Two-phase prep script run as the `confluent-prep` container. It runs
**after `confluent-flink-runner` completes** (not before Flink):

- **Phase A** (runs first, before Flink via a separate `confluent-schema-prep` container):
  Creates the two schemas in watsonx.data via Presto so the `register_table` calls
  in Phase B have somewhere to land.
- **Phase B** (runs after Flink has written tables): Queries the local Iceberg REST
  catalog (`http://confluent-iceberg-rest:8181`) to discover the current
  `metadata.json` location for each Flink-written table, then calls
  `CALL iceberg_data.system.register_table(...)` via Presto to make each table
  visible in watsonx.data.

This is the bridge between the local Flink/Iceberg stack and the remote watsonx.data
catalog — using only the Presto HTTPS connection that already works for dbt.

**Expected Outcomes:**
- Schema `confluent_silver` created in `iceberg_data` (if not exists)
- Schema `confluent_tableflow` created in `iceberg_data` (if not exists)
- All 5 Flink-written silver tables registered in `iceberg_data.confluent_silver`:
  `silver_customers`, `silver_products`, `silver_orders`, `silver_order_items`,
  `silver_sales_enriched`
- Tables queryable from Presto immediately after registration:
  `SELECT * FROM iceberg_data.confluent_silver.silver_customers LIMIT 5`
- Idempotent: `CREATE SCHEMA IF NOT EXISTS` + `register_table` only if table not
  already registered (check via `SHOW TABLES IN iceberg_data.confluent_silver`)
- Uses only `prestodb` (already in `requirements.txt`) and `requests` (for the
  Iceberg REST catalog API to discover metadata locations)

**Todo List:**
1. Create `confluent/scripts/prep_iceberg_schemas.py`
2. Connect to Presto using the identical pattern from
   [`scripts/bootstrap_watsonxdata.py`](scripts/bootstrap_watsonxdata.py):
   `prestodb.dbapi.connect(host, port, user, auth=BasicAuthentication(user, password),
   http_scheme='https', http_headers={'LhInstanceId': instance_id})` — reads
   `WXD_USER`, `WXD_API_KEY`, `WXD_HOST`, `WXD_PORT`, `WXD_INSTANCE_ID`,
   `WXD_SSL_VERIFY`, `WXD_CATALOG` from env
3. **Phase A**: execute `CREATE SCHEMA IF NOT EXISTS iceberg_data.confluent_silver`
   and `iceberg_data.confluent_tableflow`
4. **Phase B** (called after Flink runner completes): for each of the 5 silver tables:
   a. Call `GET http://confluent-iceberg-rest:8181/v1/namespaces/confluent_silver/tables/<name>`
      to retrieve the table's current `metadata-location`
   b. Check if table already registered: `SHOW TABLES IN iceberg_data.confluent_silver`
   c. If not registered, execute:
      ```sql
      CALL iceberg_data.system.register_table(
        schema_name       => 'confluent_silver',
        table_name        => '<name>',
        metadata_location => '<metadata-location from step a>'
      )
      ```
5. Print status for each table (schema created / already exists / registered / skipped)
6. Handle SSL via `WXD_SSL_VERIFY` (path to CA cert or `false`)
7. Docker compose splits this into two containers:
   - `confluent-schema-prep`: runs Phase A only (before kafka-init / flink-runner)
   - `confluent-prep`: runs Phase B (depends on flink-runner `service_completed_successfully`)

**Relevant Context:**
- [`scripts/bootstrap_watsonxdata.py`](scripts/bootstrap_watsonxdata.py) — exact Presto
  connection pattern, `_ssl_verify()` helper, `_http_headers()` helper, `_execute()` wrapper
- `requirements.txt` already has `presto-python-client==0.8.4` and `requests>=2.31`
- Iceberg REST catalog API: `GET /v1/namespaces/{ns}/tables/{table}` returns
  `{"metadata-location": "s3://...", ...}`
- `CALL iceberg_data.system.register_table` is a Presto/Iceberg procedure available
  in watsonx.data (same Presto engine that runs dbt)

**Status:** `[ ] pending`

---

### Sub-Task 6 — `confluent/flink/sql/silver_jobs.sql`

**Intent:** Flink SQL script that exactly mirrors the dbt silver layer logic. Creates
Kafka source tables (JSON), creates Iceberg sink tables in the **local Iceberg REST
catalog** (`confluent-iceberg-rest:8181`), and submits streaming `INSERT INTO` jobs.
The `confluent-prep` container (Sub-Task 5) then registers the resulting tables into
watsonx.data via Presto after the first snapshot is written.

**Expected Outcomes:**
- 5 Flink streaming jobs running continuously:
  1. `confluent_silver.silver_customers` — cast + trim + filter on email
  2. `confluent_silver.silver_products` — cast + trim + filter on product_id
  3. `confluent_silver.silver_orders` — cast + derive order_date + lowercase status/payment
  4. `confluent_silver.silver_order_items` — cast + filter quantity > 0
  5. `confluent_silver.silver_sales_enriched` — streaming temporal join of all 4 above
- Iceberg tables written to `confluent-minio:9000` (local S3), namespace `confluent_silver`
- Transformations **identical** to dbt silver SQL (same column names, casts, filters)
- Tables visible in Flink Web UI (http://localhost:28085) as 5 running jobs
- After `confluent-prep` runs: tables also visible in watsonx.data as
  `iceberg_data.confluent_silver.*`

**Todo List:**
1. Create `confluent/flink/sql/silver_jobs.sql`
2. **Catalog definition** — local Iceberg REST catalog backed by real `iceberg-bucket`:
   ```sql
   CREATE CATALOG local_iceberg WITH (
     'type'         = 'iceberg',
     'catalog-type' = 'rest',
     'uri'          = 'http://confluent-iceberg-rest:8181',
     'warehouse'    = 's3://iceberg-bucket/confluent/'
   );
   USE CATALOG local_iceberg;
   CREATE DATABASE IF NOT EXISTS confluent_silver;
   ```
   **The warehouse path `s3://iceberg-bucket/confluent/` is the real watsonx.data bucket.**
   Data files written by Flink land at `s3://iceberg-bucket/confluent/confluent_silver/<table>/`
   which is directly accessible to Presto after `register_table`.
3. **Kafka source tables** (4 TEMPORARY tables, one per topic):
   ```sql
   CREATE TEMPORARY TABLE src_customers (
     customer_id  STRING,
     first_name   STRING,
     last_name    STRING,
     email        STRING,
     signup_date  STRING,
     country      STRING
   ) WITH (
     'connector'                    = 'kafka',
     'topic'                        = 'raw_customers',
     'properties.bootstrap.servers' = 'confluent-kafka:9092',
     'properties.group.id'          = 'flink-silver-customers',
     'scan.startup.mode'            = 'earliest-offset',
     'format'                       = 'json'
   );
   ```
   Same pattern for `src_products`, `src_orders`, `src_order_items` — all columns
   as STRING (raw Kafka JSON, same types as the CSV seed headers).
4. **Iceberg sink table DDL** — `CREATE TABLE IF NOT EXISTS local_iceberg.confluent_silver.<name>`:
   - `silver_customers`: `customer_id INT, first_name STRING, last_name STRING, email STRING, signup_date DATE, country STRING, transformed_at TIMESTAMP`
   - `silver_products`: `product_id INT, product_name STRING, category STRING, unit_price DECIMAL(12,2), transformed_at TIMESTAMP`
   - `silver_orders`: `order_id INT, customer_id INT, order_ts TIMESTAMP, order_date DATE, status STRING, payment_method STRING, transformed_at TIMESTAMP` — partitioned by `months(order_date)`
   - `silver_order_items`: `order_item_id INT, order_id INT, product_id INT, quantity INT, discount_pct DECIMAL(5,2), transformed_at TIMESTAMP`
   - `silver_sales_enriched`: all joined columns, `gross_amount DECIMAL(14,2)`, `net_amount DECIMAL(14,2)`, `transformed_at TIMESTAMP` — partitioned by `months(order_date)`
5. **INSERT INTO** mirroring dbt SQL exactly:
   - `silver_customers`: `CAST(customer_id AS INT)`, `TRIM(first_name)`, `LOWER(TRIM(email))`,
     `UPPER(TRIM(country))`, `CAST(signup_date AS DATE)`, `WHERE email IS NOT NULL`
   - `silver_products`: `CAST(product_id AS INT)`, `TRIM(product_name)`, `TRIM(category)`,
     `CAST(unit_price AS DECIMAL(12,2))`, `WHERE product_id IS NOT NULL`
   - `silver_orders`: `CAST(order_id AS INT)`, `CAST(order_ts AS TIMESTAMP)`,
     `CAST(CAST(order_ts AS TIMESTAMP) AS DATE) AS order_date`, `LOWER(TRIM(status))`,
     `WHERE order_id IS NOT NULL`
   - `silver_order_items`: `CAST(quantity AS INT)`, `CAST(discount_pct AS DECIMAL(5,2))`,
     `WHERE CAST(quantity AS INT) > 0`
   - `silver_sales_enriched`: `FOR SYSTEM_TIME AS OF` temporal join across all 4 silver tables,
     `CAST(quantity * unit_price AS DECIMAL(14,2)) AS gross_amount`,
     `CAST(quantity * unit_price * (1 - discount_pct) AS DECIMAL(14,2)) AS net_amount`
6. `SET 'execution.runtime-mode' = 'streaming';` at top
7. `SET 'pipeline.name' = 'confluent-silver-medallion';`
8. `SET 'execution.checkpointing.interval' = '30s';` — ensures Iceberg commits happen
   every 30 seconds so `confluent-prep` can find a committed snapshot

**Relevant Context:**
- dbt silver models are the exact source of truth for all transforms:
  [`models/silver/silver_customers.sql`](models/silver/silver_customers.sql),
  [`models/silver/silver_orders.sql`](models/silver/silver_orders.sql),
  [`models/silver/silver_products.sql`](models/silver/silver_products.sql),
  [`models/silver/silver_order_items.sql`](models/silver/silver_order_items.sql),
  [`models/silver/silver_sales_enriched.sql`](models/silver/silver_sales_enriched.sql)
- Firefighter `iceberg_jobs.sql` uses identical `catalog-type = 'rest'` pattern —
  this is the proven working approach for local Iceberg REST catalog with Flink 1.20
- `silver_sales_enriched` uses a temporal join (not a regular join) in streaming mode —
  Flink requires one side to be a versioned table or use `FOR SYSTEM_TIME AS OF`

**Status:** `[ ] pending`

---

### Sub-Task 7 — `confluent/scripts/submit-flink.sh`

**Intent:** One-shot entrypoint script for the `confluent-flink-runner` container.
Waits for the Flink JobManager REST to be ready, then submits the SQL file via
Flink SQL Client in non-interactive mode.

**Expected Outcomes:**
- Polls `http://flink-jobmanager:8081/v1/overview` until 200 OK
- Executes: `/opt/flink/bin/sql-client.sh -f /opt/sql/silver_jobs.sql`
  (embedded mode, submits all statements in the file)
- Exits 0 on success, logs errors clearly

**Todo List:**
1. Create `confluent/scripts/submit-flink.sh`:
   ```bash
   #!/bin/bash
   JM="${JOBMANAGER_HOST:-flink-jobmanager}:${JOBMANAGER_PORT:-8081}"
   until curl -sf "http://$JM/v1/overview" >/dev/null; do
     echo "waiting for Flink JobManager..."; sleep 3
   done
   SQL_FILE="${SQL_FILE:-/opt/sql/silver_jobs.sql}"
   exec /opt/flink/bin/sql-client.sh -f "$SQL_FILE"
   ```
2. Make executable

**Relevant Context:**
- Firefighter `submit.sh` pattern: poll JobManager REST, then run SQL client
- SQL client `-f` flag: non-interactive file execution (Flink 1.16+)

**Status:** `[ ] pending`

---

### Sub-Task 8 — `.env.example` additions

**Intent:** Document the new env vars needed by the confluent stack. The architecture
finding removes `WXD_HIVE_METASTORE_URI` entirely (not needed — Flink uses local REST
catalog, not HMS). The only new variables are for the local Confluent stack itself.

**Expected Outcomes:**
- `.env.example` has a new `# Confluent / Flink streaming stack (confluent/)` section:
  - `CONFLUENT_SILVER_SCHEMA` — default `confluent_silver` (schema name in `iceberg_data`)
  - `CONFLUENT_TABLEFLOW_SCHEMA` — default `confluent_tableflow`
  - `CONFLUENT_MINIO_USER` — default `confluent` (local MinIO root user)
  - `CONFLUENT_MINIO_PASSWORD` — default `confluent` (local MinIO root password)
  - A comment block explaining the two-phase approach: Flink → local MinIO/Iceberg REST,
    then `confluent-prep` → Presto `register_table` → visible in `iceberg_data`
  - A note that `WXD_USER`, `WXD_API_KEY`, `WXD_HOST`, `WXD_PORT`, `WXD_INSTANCE_ID`,
    `WXD_SSL_VERIFY`, `WXD_CATALOG` are **reused** by `confluent-prep` — no new
    watsonx.data credentials needed

**Todo List:**
1. Append a new clearly-bounded section to [`.env.example`](.env.example)
2. No changes to any existing variable — purely additive

**Status:** `[ ] pending`

---

### Sub-Task 9 — Root `docker-compose.yml` include

**Intent:** Wire the new confluent compose file into the root compose project so
`docker compose up -d` from the repo root brings up the confluent stack alongside
Airflow, Metabase, and OpenMetadata — without modifying any existing service.

**Expected Outcomes:**
- Root [`docker-compose.yml`](docker-compose.yml) `include:` block has a third entry:
  `- confluent/docker-compose.yml`
- `docker compose ps` shows all confluent services alongside airflow/metabase/openmetadata
- `docker compose up -d airflow-webserver` still works unchanged

**Todo List:**
1. Edit [`docker-compose.yml`](docker-compose.yml:136) — append `- confluent/docker-compose.yml`
   to the existing `include:` block (lines 136–138)

**Relevant Context:**
- Current include block at [`docker-compose.yml:136`](docker-compose.yml:136)

**Status:** `[ ] pending`

---

## Dependency Order

```
Sub-Task 2 (Flink Dockerfile)
    ↓
Sub-Task 1 (docker-compose.yml)  ←── Sub-Task 3 (create-topics.sh)
    ↓                                      ↓
Sub-Task 9 (root compose include)    Sub-Task 4 (ingest_csv_to_kafka.py)
                                           ↓
Sub-Task 5 (prep_iceberg_schemas.py)
    ↓
Sub-Task 6 (silver_jobs.sql)
    ↓
Sub-Task 7 (submit-flink.sh)
    ↓
Sub-Task 8 (.env.example additions)
```

## File Tree (all new files)

```
confluent/
├── docker-compose.yml              ← Sub-Task 1
├── flink/
│   ├── Dockerfile                  ← Sub-Task 2
│   └── sql/
│       └── silver_jobs.sql         ← Sub-Task 6
└── scripts/
    ├── create-topics.sh            ← Sub-Task 3
    ├── ingest_csv_to_kafka.py      ← Sub-Task 4
    ├── prep_iceberg_schemas.py     ← Sub-Task 5
    └── submit-flink.sh             ← Sub-Task 7

docker-compose.yml                  ← Sub-Task 9 (1-line edit)
.env.example                        ← Sub-Task 8 (append section)
```

## Notes

- **Tableflow** (Sub-Task 1) runs as a separate Flink job from `confluent-flink-runner`
  using a second SQL file `tableflow_jobs.sql` that only does raw Kafka → Iceberg without
  silver transforms. This produces `confluent_tableflow.*` tables. This can be added as
  a follow-on sub-task once the silver path is validated.
- **Gold layer** via Flink is out of scope for this plan — the existing dbt gold views
  (`gold_daily_sales`, `gold_category_performance`, `gold_customer_360`) can be pointed
  at `confluent_silver.silver_sales_enriched` with a `dbt run --vars` override once
  silver tables are live.
- **Kafbat UI** (port 28080) provides full topic inspection, Schema Registry browser,
  and consumer group lag monitoring — no additional config needed beyond the env vars
  in Sub-Task 1.
