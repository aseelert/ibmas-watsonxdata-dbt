# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A customer-facing **IBM watsonx.data** demo/workshop teaching the Bronze → Silver → Gold
medallion pattern. The same four CSV seed files (customers, products, orders, order_items)
flow through **three interchangeable paths**, all writing to the `iceberg_data` Iceberg
catalog on MinIO, all queryable via the Presto SQL engine:

- **Path A — dbt** (`01-dbt/models/`): full SQL pipeline via the `dbt-watsonx-presto` adapter → schemas `dbt_demo_raw/bronze/silver/gold`
- **Path B — Spark** (`03-spark/spark/load_medallion_demo.py`): PySpark on the watsonx.data Spark engine → `spark_demo_bronze/silver/gold`
- **Path C — cpdctl** (`scripts/04_ingest_with_cpdctl.py`): native ingestion loader, raw only → `spark_demo_cpdctl_raw`
- **Streaming (newer)** — `04-confluent-streaming/confluent/`: Kafka → Flink → Iceberg, a self-contained Docker stack

The audience is non-technical learners. **Docs are part of the product**: when changing
behavior, update the MkDocs pages in `docs/` and keep explanations student-friendly
(2–6 sentences per concept). When adding a feature to one ETL path, consider whether the
dbt **and** Spark paths should stay in parity.

## Common commands

`bin/demo` is the supported entry point — prefer it over calling the scripts below directly;
it forwards extra args to the underlying script (e.g. `bin/demo streaming --gold --engine spark`).
See `docs/demo/scripts.md` for the full script-by-script reference.

dbt is always invoked through `scripts/02_dbt_env.sh` (what `bin/demo dbt` calls), a wrapper that
sources `.env`, uses the `.venv` dbt binary, and points `--project-dir` at `01-dbt/`. Don't call
bare `dbt` — env vars from `.env` won't be loaded and it won't find the project.

**Credentials are short-lived — refresh them first, every session** (login token + Spark bearer
token expire; the API key doesn't but a stale `.env` can still fail auth):

```bash
bin/demo setup                            # == python3 scripts/00a_prepare_watsonx_env.py
```
Safe and cheap to re-run — by default it only *fills in* missing/placeholder `.env` values, but it
ALWAYS refreshes `WXD_API_KEY` (fetch-or-create, never destructively rotates an existing key) and
`WXD_SPARK_BEARER_TOKEN` (always re-derived — it's short-lived by design), then re-derives every
dependent endpoint. Run this before the first command of any session, and again whenever an
`oc login` or CPD login has since expired.

```bash
# One-time setup
python3.11 -m venv .venv && source .venv/bin/activate   # Python 3.11 REQUIRED (3.14 breaks dbt)
pip install -r requirements.txt

# Bootstrap .env from the Presto connection JSON exported from the watsonx.data UI
# (save the export as watsonx_data/instance_details.json first):
bin/demo setup
#   → parses JSON → writes WXD_HOST, WXD_PORT, WXD_INSTANCE_ID, WXD_CPD_HOST, etc.
#   → derives all URLs (WXD_CPD_AUTH_URL, WXD_OPENSHIFT_CONSOLE, Spark endpoints, MinIO, PG)
#   → writes CA cert to certs/watsonxdata-ca.pem
#   → if logged in with oc: also reads WXD_SPARK_ENGINE_ID, MinIO creds, PG_PASSWORD from secrets
#   → logs in with WXD_CPD_PASSWORD and refreshes WXD_API_KEY + WXD_SPARK_BEARER_TOKEN (default on)
#
# Useful flags (forwarded: `bin/demo setup --dry-run`, etc.):
#   --dry-run               show diff only, write nothing
#   --overwrite             also replace existing non-placeholder values
#   --no-oc                 skip OpenShift secret discovery (offline / no oc session)
#   --no-fetch-tokens       skip the API-key/bearer-token refresh (keep current values)
#   --presto-json PATH      use a different Presto connection JSON
#   --spark-json  PATH      also parse a Spark engine connection JSON
#   -v / --verbose          DEBUG logging
#
# Mid-session, when just the bearer token has expired (~12h), a lighter refresh:
#   python3 scripts/00b_get_token.py --export       # validate + refresh WXD_SPARK_BEARER_TOKEN
#   python3 scripts/00b_get_token.py --refresh-key  # force a new API key
#   python3 scripts/refresh_token.py --check        # just print the current token's expiry

# dbt path
bin/demo dbt debug
bin/demo dbt seed              # load 4 CSVs into *_raw
bin/demo dbt build             # bronze → silver → gold + schema tests, in DAG order
bin/demo dbt run  --select bronze            # single layer (tags: bronze/silver/gold)
bin/demo dbt run  --select silver_orders     # single model
bin/demo dbt test --select silver+           # one model + downstream
bin/demo dbt run  --threads 2                # if cluster is under load (avoids connection aborts)

# Spark path — uploads assets, submits, waits for FINISHED, builds gold views
bin/demo spark                             # real submit IF WXD_SPARK_DRY_RUN=false in .env
bin/demo spark --dry-run                   # preview only, regardless of .env

# cpdctl native-ingestion path (raw only)
bin/demo ingest --wait

# Streaming stack (Kafka → Flink → Iceberg) — 04-confluent-streaming/confluent/
bin/demo streaming                        # --all: build Flink image, start 7 services, seed topics
bash 04-confluent-streaming/confluent/scripts/expose_minio_route.sh   # required once, before --silver
bin/demo streaming --silver               # Flink silver pipeline → confluent_demo_silver
bin/demo streaming --gold --engine spark  # confluent_demo_gold (or --engine datastage)
bin/demo streaming --status               # health + topic counts, read-only

# Compare Gold across dbt/Spark/Confluent (skips any path not built yet, never fails on that alone)
bin/demo query
bin/demo validate

# Docs site
mkdocs serve                              # http://127.0.0.1:8000, live-reloads docs/
mkdocs build --strict                     # fail on broken links — use in CI

# Docker lifecycle (one project: ibmas-watsonxdata-dbt)
bin/demo docker build          # build Airflow + Flink images (once after clone)
bin/demo docker start          # start all services (metabase/airflow/lineage/catalog/streaming)
bin/demo docker start metabase # start one service
bin/demo docker stop           # stop all
bin/demo docker status         # show all container states
```

There is no application test suite — "tests" means `dbt test` (schema/data tests defined in
`01-dbt/models/**/schema.yml` and `01-dbt/models/bronze/bronze_sources.yml`).

## Architecture notes that aren't obvious from one file

**Schema naming is env-driven.** `dbt_project.yml` derives each layer's schema from
`WXD_SCHEMA` (default `dbt_demo`) plus a suffix — e.g. `WXD_BRONZE_SCHEMA` or
`{WXD_SCHEMA}_bronze`. `01-dbt/macros/generate_schema_name.sql` is overridden so a model's
`+schema` is used **verbatim** (not prefixed with the target schema, which is dbt's default).
Changing the demo's schema prefix is a single `WXD_SCHEMA` change.

**Gold materialization is configurable.** Gold defaults to `view` (`WXD_GOLD_MATERIALIZED`).
`gold_daily_sales` is a table; `gold_category_performance` and `gold_customer_360` are views.

**Iceberg format is PARQUET, never ORC** (explicit project requirement). Tables set
`properties={"format": "'PARQUET'", "partitioning": "ARRAY['month(order_date)']"}` — note the
**inner single quotes** are required by the adapter.

**`CREATE MATERIALIZED VIEW` is not supported** on Presto Iceberg (errors `NOT_SUPPORTED`).
Use regular `view`. `01-dbt/macros/materialized_view.sql` exists only for future forward-compat —
do not wire it into gold models.

**Semantic models** (`01-dbt/models/semantic_models.yml`) are validated by `dbt parse` only;
MetricFlow is not installed, so don't expect `dbt sl` / metric queries to run.

**Auth.** Presto uses ZenApiKey via BasicAuth — user `ibmlhapikey_cpadmin`, password is the
API key (`WXD_API_KEY`), plus an `LhInstanceId` HTTP header. TLS requires
`certs/watsonxdata-ca.pem`. Re-run `python scripts/00a_prepare_watsonx_env.py --overwrite` when
the instance is re-provisioned or the token/cert changes.

**Secrets.** `.env` is git-ignored and holds `WXD_API_KEY`. Never commit it. If a key leaks
into a commit or chat, rotate it before any customer demo.

**dbt reads `~/.dbt/profiles.yml`, not `01-dbt/profiles/profiles.yml` directly** —
`scripts/02_dbt_env.sh` pins `DBT_PROFILES_DIR` to `01-dbt/profiles/` so this repo's tracked file
is always authoritative. If you ever invoke `dbt` directly (bypassing the wrapper), set
`DBT_PROFILES_DIR` yourself or edits to `01-dbt/profiles/profiles.yml` won't take effect.

## Layout

As of the 2026-08-31 reorg (commit `fd00670`), each workshop path lives under
its own numbered top-level directory; `scripts/` and `bin/` stay at the repo
root since they're shared across paths.

- `bin/demo` — the supported entry point (`bin/demo setup|dbt|spark|ingest|streaming|query|validate|reset|...`); prefer this over calling scripts under the numbered dirs directly
- `scripts/` — Python/bash helpers shared across paths (env prep, bootstrap schemas, cpdctl ingest, Spark submit/status, reconcile_gold, cleanup/reset). Most Python scripts use argparse — check `--help`. See `docs/demo/scripts.md` for the full script-by-script table.
- `01-dbt/` — `models/{bronze,silver,gold}/` (SQL models; `bronze_sources.yml` defines seed sources, `schema.yml` per layer holds tests), `seeds/raw_*.csv` (50 customers, 20 products, 500 orders, 1134 order_items), `macros/` (schema-name override, medallion schema creation, materialized-view stub), `profiles/profiles.yml` (dbt connection profile, `watsonx_presto` type, all values via `env_var()`)
- `02-metabase/` — Metabase BI stack + `provision.py` (admin user, Presto data source, one demo chart per medallion path)
- `03-spark/spark/load_medallion_demo.py` — the full Spark medallion job (parallel to the dbt path)
- `04-confluent-streaming/confluent/` — Kafka/Flink/Iceberg streaming stack (`start.sh` orchestrates; `flink/sql/` holds the SQL jobs)
- `05-airflow/airflow/dags/` — `dag_dbt_medallion.py` and `dag_spark_medallion.py` orchestrate the two batch paths
- `06-openlineage-marquez/openlineage-marquez/` — OpenLineage/Marquez stack
- `07-openmetadata/openmetadata/` — local Docker data-catalog stack; ingests dbt artifacts (manifest/catalog/run_results.json) to draw lineage
- `08-governance/governance/` — IKC governance provisioning assets
- `09-agent-tools/mcp-server/` — standalone MCP server for watsonx project validation (replaces the old `cpd-mcpserver/`)
- `docs/` + `mkdocs.yml` — the published workshop docs (`site/` is the git-ignored build output)
