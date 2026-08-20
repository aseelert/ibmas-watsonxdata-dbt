---
name: ibmas-dbt-medallion-modeling
description: "Use when writing or modifying dbt models for the bronze/silver/gold medallion layers against watsonx.data Presto/Iceberg in this project. Covers schema-naming overrides, PARQUET/materialized-view quirks, and semantic model limits. Triggers on phrases like: 'add a dbt model', 'gold view', 'dbt schema', 'iceberg table properties', 'materialized view presto', 'dbt seed', 'dbt test'."
---

# dbt Medallion Modeling (Presto + Iceberg on watsonx.data)

## Always go through the wrapper

```bash
bash scripts/dbt_env.sh run --select bronze        # never call bare `dbt` — .env won't load
bash scripts/dbt_env.sh test --select silver+
bash scripts/dbt_env.sh run --threads 2             # use 2 threads if cluster is under load
```

## Schema naming is env-driven and taken verbatim

`dbt_project.yml` sets `+schema:` per layer from `WXD_{RAW,BRONZE,SILVER,GOLD}_SCHEMA`
(falling back to `{WXD_SCHEMA}_<layer>`, default prefix `dbt_demo`). Critically,
`macros/generate_schema_name.sql` is overridden so a model's `+schema` is used
**verbatim** — dbt's stock behavior of prefixing with the target schema is disabled:
```sql
{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- if custom_schema_name is none -%}{{ target.schema }}
  {%- else -%}{{ custom_schema_name | trim }}{%- endif -%}
{%- endmacro %}
```
Changing the whole demo's schema prefix is a single `WXD_SCHEMA` edit in `.env` — never
hand-edit `+schema:` values in `dbt_project.yml` to rename the demo.

## Iceberg tables: PARQUET only, with a quoting gotcha

This project explicitly requires PARQUET, never ORC:
```python
config(
    materialized='table',
    properties={"format": "'PARQUET'", "partitioning": "ARRAY['month(order_date)']"},
)
```
Note the **inner single quotes** inside the Python string — the `dbt-watsonx-presto`
adapter needs them literally; dropping them breaks table creation.

## `CREATE MATERIALIZED VIEW` is NOT supported on Presto Iceberg

Fails with `NOT_SUPPORTED`. Always use a regular `view`:
```python
config(materialized="{{ env_var('WXD_GOLD_MATERIALIZED', 'view') }}")
```
`macros/materialized_view.sql` exists only for future forward-compatibility — do not
wire it into any gold model.

## Gold layer materialization

Default `view` (`WXD_GOLD_MATERIALIZED`); `gold_daily_sales` is the one gold model kept
as a `table`. `gold_category_performance`/`gold_customer_360` are views.

## Semantic models are inert here

`models/semantic_models.yml` is validated by `dbt parse` only — **MetricFlow is not
installed**, so `dbt sl` / metric queries will not run. Don't propose semantic-layer
queries as if they work; treat the file as schema documentation only.

## Testing

"Tests" = `dbt test` against `models/**/schema.yml` + `models/bronze/bronze_sources.yml`
— there is no separate application test suite in this project.

## Cross-path parity discipline

When adding a feature to the dbt path, check whether the Spark path
(`spark/load_medallion_demo.py`, see `ibmas-spark-analyticsengine-ops`) needs the same
change to stay in parity — gold numbers across all paths are expected to match exactly.
