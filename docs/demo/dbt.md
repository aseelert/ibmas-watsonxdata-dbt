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

## What dbt contributes

| Concern | dbt contribution |
| --- | --- |
| Transformation | Versioned SQL models and materializations |
| Trust | Tests, source declarations, and model documentation |
| Dependency view | Declared model DAG and compiled artifacts |
| Execution | Submits SQL through the configured Presto adapter |

The result is the canonical business contract. The Spark and streaming paths
are compared to it; they do not become competing definitions of Gold.

References: [dbt SQL models](https://docs.getdbt.com/docs/build/sql-models),
[materializations](https://docs.getdbt.com/docs/build/materializations),
[seeds](https://docs.getdbt.com/docs/build/seeds), and
[`dbt build`](https://docs.getdbt.com/reference/commands/build).
