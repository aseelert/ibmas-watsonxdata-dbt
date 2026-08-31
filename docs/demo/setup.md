# Baseline workshop

The baseline is the shortest complete story: source fixtures become tested
Iceberg Gold marts and a business dashboard. Run it before discussing optional
batch, event, or enterprise alternatives.

## Prerequisites

Use [Environment setup](environment.md) to create the virtual environment,
prepare the watsonx.data certificate, and configure `.env`. Ensure the
watsonx.data connection is available before proceeding.

The first `bin/demo dbt ...` invocation creates the ignored local dbt profile
from `01-dbt/profiles/profiles.example.yml`. The template contains only
environment-variable references; credentials stay in `.env` and are never
committed. Set `DBT_PROFILES_DIR` only when deliberately using a separate
profile location.

## Run and validate

```bash
bin/demo setup
bin/demo dbt debug
bin/demo dbt build
bin/demo metabase
```

Validation point: dbt completes its models and tests, and Metabase opens the
Gold dashboard at [http://localhost:3000](http://localhost:3000).

```bash
bin/demo validate
```

The Spark and Kafka/Flink paths can now be demonstrated as independent
implementations of the same Gold contract.
