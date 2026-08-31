# watsonx.data Lakehouse Demo

A dbt-first customer demo for ingesting retail data into an open Apache Iceberg
lakehouse on IBM watsonx.data. The same data can then be consumed through
Metabase, rebuilt with Spark, processed through Kafka/Flink, orchestrated by
Airflow, traced with OpenLineage/Marquez, and cataloged in OpenMetadata.

## Start here

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
bin/demo setup
bin/demo dbt build
bin/demo metabase
```

Use `bin/demo --help` for all supported demo commands. The complete, concise
walkthrough is the MkDocs site in [`docs/demo/`](docs/demo/).

## Demo access

| UI | URL | Start |
| --- | --- | --- |
| Metabase | http://localhost:3000 | `bin/demo metabase` |
| Airflow | http://localhost:8082 | `bin/demo airflow` |
| Kafka UI | http://localhost:28080 | `bin/demo streaming` |
| Flink UI | http://localhost:28085 | `bin/demo streaming` |
| Marquez | http://localhost:3001 | `bin/demo lineage` |
| OpenMetadata | http://localhost:8585 | `bin/demo catalog` |

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
