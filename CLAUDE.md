# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A customer-facing **IBM watsonx.data** demo/workshop teaching the Bronze → Silver → Gold
medallion pattern. The same four CSV seed files (customers, products, orders, order_items)
flow through **three interchangeable paths**, all writing to the `iceberg_data` Iceberg
catalog on MinIO, all queryable via the Presto SQL engine:

- **Path A — dbt** (`models/`): full SQL pipeline via the `dbt-watsonx-presto` adapter → schemas `dbt_demo_raw/bronze/silver/gold`
- **Path B — Spark** (`spark/load_medallion_demo.py`): PySpark on the watsonx.data Spark engine → `spark_demo_bronze/silver/gold`
- **Path C — cpdctl** (`scripts/ingest_with_cpdctl.py`): native ingestion loader, raw only → `spark_demo_cpdctl_raw`
- **Streaming (newer)** — `confluent/`: Kafka → Flink → Iceberg, a self-contained Docker stack

The audience is non-technical learners. **Docs are part of the product**: when changing
behavior, update the MkDocs pages in `docs/` and keep explanations student-friendly
(2–6 sentences per concept). When adding a feature to one ETL path, consider whether the
dbt **and** Spark paths should stay in parity.

## Common commands

dbt is always invoked through `scripts/dbt_env.sh`, a wrapper that sources `.env` and uses
the `.venv` dbt binary. Don't call bare `dbt` — env vars from `.env` won't be loaded.

```bash
# One-time setup
python3.11 -m venv .venv && source .venv/bin/activate   # Python 3.11 REQUIRED (3.14 breaks dbt)
pip install -r requirements.txt
python scripts/prepare_watsonx_env.py     # parses watsonx_data/instance_details.json → .env + certs/watsonxdata-ca.pem

# dbt path (all go through the wrapper)
bash scripts/dbt_env.sh debug
bash scripts/dbt_env.sh seed              # load 4 CSVs into *_raw
bash scripts/dbt_env.sh run               # build bronze → silver → gold
bash scripts/dbt_env.sh test              # run schema tests
bash scripts/dbt_env.sh run  --select bronze            # single layer (tags: bronze/silver/gold)
bash scripts/dbt_env.sh run  --select silver_orders     # single model
bash scripts/dbt_env.sh test --select silver+           # one model + downstream
bash scripts/dbt_env.sh run  --threads 2                # if cluster is under load (avoids connection aborts)

# Docs site
mkdocs serve                              # http://127.0.0.1:8000, live-reloads docs/
mkdocs build --strict                     # fail on broken links — use in CI

# Streaming stack (Kafka → Flink → Iceberg)
bash confluent/start.sh                   # build Flink image, start 7 services, seed topics
bash confluent/start.sh --watsonxdata     # also submit Flink silver jobs to watsonx.data

# Optional local services (Airflow, Metabase, OpenMetadata)
docker compose up -d
```

There is no application test suite — "tests" means `dbt test` (schema/data tests defined in
`models/**/schema.yml` and `models/bronze/bronze_sources.yml`).

## Architecture notes that aren't obvious from one file

**Schema naming is env-driven.** `dbt_project.yml` derives each layer's schema from
`WXD_SCHEMA` (default `dbt_demo`) plus a suffix — e.g. `WXD_BRONZE_SCHEMA` or
`{WXD_SCHEMA}_bronze`. `macros/generate_schema_name.sql` is overridden so a model's
`+schema` is used **verbatim** (not prefixed with the target schema, which is dbt's default).
Changing the demo's schema prefix is a single `WXD_SCHEMA` change.

**Gold materialization is configurable.** Gold defaults to `view` (`WXD_GOLD_MATERIALIZED`).
`gold_daily_sales` is a table; `gold_category_performance` and `gold_customer_360` are views.

**Iceberg format is PARQUET, never ORC** (explicit project requirement). Tables set
`properties={"format": "'PARQUET'", "partitioning": "ARRAY['order_date']"}` — note the
**inner single quotes** are required by the adapter.

**`CREATE MATERIALIZED VIEW` is not supported** on Presto Iceberg (errors `NOT_SUPPORTED`).
Use regular `view`. `macros/materialized_view.sql` exists only for future forward-compat —
do not wire it into gold models.

**Semantic models** (`models/semantic_models.yml`) are validated by `dbt parse` only;
MetricFlow is not installed, so don't expect `dbt sl` / metric queries to run.

**Auth.** Presto uses ZenApiKey via BasicAuth — user `ibmlhapikey_cpadmin`, password is the
API key (`WXD_API_KEY`), plus an `LhInstanceId` HTTP header. TLS requires
`certs/watsonxdata-ca.pem`. Re-run `python scripts/prepare_watsonx_env.py --overwrite` when
the instance is re-provisioned or the token/cert changes.

**Secrets.** `.env` is git-ignored and holds `WXD_API_KEY`. Never commit it. If a key leaks
into a commit or chat, rotate it before any customer demo.

## Layout

- `models/{bronze,silver,gold}/` — dbt SQL models; `bronze_sources.yml` defines seed sources, `schema.yml` per layer holds tests
- `seeds/raw_*.csv` — the 4 demo datasets (50 customers, 20 products, 500 orders, 1134 order_items)
- `macros/` — schema-name override, medallion schema creation, materialized-view stub
- `profiles/profiles.yml` — dbt connection profile (`watsonx_presto` type), all values via `env_var()`
- `scripts/` — Python/bash helpers (env prep, bootstrap schemas, cpdctl ingest, Spark submit, OpenMetadata ingestion, cleanup/reset). Most Python scripts use argparse — check `--help`.
- `spark/load_medallion_demo.py` — the full Spark medallion job (parallel to the dbt path)
- `confluent/` — Kafka/Flink/Iceberg streaming stack (`start.sh` orchestrates; `flink/sql/` holds the SQL jobs)
- `airflow/dags/` — `dag_dbt_medallion.py` and `dag_spark_medallion.py` orchestrate the two batch paths
- `openmetadata/` — local Docker data-catalog stack; ingests dbt artifacts (manifest/catalog/run_results.json) to draw lineage — see README "OpenMetadata" section
- `cpd-mcpserver/` — standalone MCP server for watsonx project validation
- `docs/` + `mkdocs.yml` — the published workshop docs (`site/` is the git-ignored build output)
