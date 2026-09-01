# watsonx.data Lakehouse Demo

A dbt-first customer demo for ingesting retail data into an open Apache Iceberg
lakehouse on IBM watsonx.data. The same data can then be consumed through
Metabase, rebuilt with Spark, processed through Kafka/Flink, orchestrated by
Airflow, traced with OpenLineage/Marquez, and cataloged in OpenMetadata.

## Start here

```bash
# 1. Python 3.11 venv
python3.11 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 2. Build custom Docker images (once — Airflow + Flink)
bin/demo docker build

# 3. Configure .env and connect to watsonx.data
cp .env.example .env          # fill in WXD_OC_PASSWORD, place connection JSON
bin/demo setup                # derives all endpoints, writes cert + tokens

# 4. Run dbt (no Docker needed — talks directly to Presto)
bin/demo dbt build

# 5. Start UI services
bin/demo docker start metabase          # or: docker start (all services)
bin/demo docker status                  # verify all containers are up
```

Use `bin/demo --help` for all supported commands. The complete walkthrough is at
[docs/demo/setup.md](docs/demo/setup.md) and the published MkDocs site.

## Demo access

| UI | URL | Start command |
| --- | --- | --- |
| Metabase | http://localhost:3000 | `bin/demo docker start metabase` |
| Airflow | http://localhost:8082 | `bin/demo docker start airflow` |
| Kafka UI | http://localhost:28080 | `bin/demo docker start streaming` |
| Flink UI | http://localhost:28085 | `bin/demo docker start streaming` |
| Marquez | http://localhost:3001 | `bin/demo docker start lineage` |
| OpenMetadata | http://localhost:8585 | `bin/demo docker start catalog` |

All services share one Docker project (`ibmas-watsonxdata-dbt`) — visible as a
single group in OrbStack. Check status with `bin/demo docker status`.

The full UI/API port index and presentation order are in
[docs/demo/access.md](docs/demo/access.md).

## Layout

- `02-metabase/` - standalone Metabase Compose stack
- `03-spark/` - Spark batch implementation
- `04-confluent-streaming/` - Kafka, Flink, schemas, and streaming Gold build
- `05-airflow/` - orchestration
- `06-openlineage-marquez/` - runtime lineage
- `07-openmetadata/` - catalog and metadata ingestion
- `08-governance/` - IKC governance import assets
- `09-agent-tools/` - shared MCP server and agent guidance

`docker-compose.yml` is the optional umbrella Compose file; every component
also has its own Compose file where it is independently runnable.

## Platform position

The open-source stack is deliberately shown end-to-end, including its
operational cost: multiple deployments, upgrades, identities, and metadata
contracts. IBM watsonx.data retains open formats and engines, while watsonx.data
Integration (DataStage) and watsonx.data Intelligence / IKC can reduce the
operational fragmentation for enterprise integration, data quality, governance,
and business lineage. Confirm exact licence entitlements before making product
claims.
