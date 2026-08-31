# Reference path — dbt and Presto

dbt is the workshop reference implementation for business SQL. Models are
declared as `SELECT` statements, but dbt materializations determine whether the
adapter creates a table, view, or incremental object. Here the Presto adapter
executes those statements against watsonx.data Iceberg schemas.

```text
CSV → dbt seed → Raw → Bronze models → Silver models → Gold marts → tests
```

```bash
bin/demo dbt build
```

## What executes when the command runs

The command does more than execute a directory of SQL files. dbt resolves
`ref()` dependencies, compiles each model for the adapter, creates the
configured relation type, and runs the associated tests. The Presto adapter
submits the compiled SQL to watsonx.data; Presto executes it against Iceberg.
The runtime engine therefore remains Presto, while dbt provides the project
structure and transformation control plane.

```text
01-dbt/seeds/*.csv → dbt seed → dbt_demo_raw
01-dbt/models/bronze → dbt_demo_bronze
01-dbt/models/silver → dbt_demo_silver
01-dbt/models/gold   → dbt_demo_gold → Metabase
```

The model files are deliberately ordinary SQL `SELECT` statements. Their
materialization configuration determines whether the result is a table, view,
or another supported relation type. This is why dbt is a strong reference path
for transparent business transformations but not a general replacement for
connectors, file ingestion, Python processing, or streaming infrastructure.

## What dbt contributes

| Concern | dbt contribution |
| --- | --- |
| Transformation | Versioned SQL models and materializations |
| Trust | Tests, source declarations, and model documentation |
| Dependency view | Declared model DAG and compiled artifacts |
| Execution | Submits SQL through the configured Presto adapter |

The result is the canonical business contract. The Spark and streaming paths
are compared to it; they do not become competing definitions of Gold.

## What to validate before moving on

1. The raw, Bronze, Silver, and Gold schemas are present in the configured
   Iceberg catalog.
2. `dbt build` completes its model and test phases without errors.
3. Gold measures and grains match the documented acceptance query.
4. Metabase is pointed at the intended Gold schema—not at a transient staging
   or raw table.

This sequence keeps the live demonstration understandable: first establish a
business-SQL baseline; then use the Spark or event pages to explain why a
different execution engine may be chosen for a different workload.

References: [dbt SQL models](https://docs.getdbt.com/docs/build/sql-models),
[materializations](https://docs.getdbt.com/docs/build/materializations),
[seeds](https://docs.getdbt.com/docs/build/seeds), and
[`dbt build`](https://docs.getdbt.com/reference/commands/build).
