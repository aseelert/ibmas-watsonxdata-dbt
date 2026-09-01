# Baseline workshop

The baseline is the shortest complete story: source fixtures become tested
Iceberg Gold marts and a business dashboard. Run it before discussing optional
batch, event, or enterprise alternatives.

## Prerequisites

Before this page, complete [Environment setup](environment.md) in full:

1. **Python 3.11** — create and activate `.venv`, install `requirements.txt`
2. **Docker runtime** — OrbStack or Docker Desktop running; `docker compose version` works
3. **Custom images built** — `bin/demo docker` (one-time; needed before Airflow/Flink)
4. **`.env` configured** — `cp .env.example .env`, then `bin/demo setup`

The first `bin/demo dbt ...` invocation creates the ignored local dbt profile
from `01-dbt/profiles/profiles.example.yml`. The template contains only
environment-variable references; credentials stay in `.env` and are never
committed. Set `DBT_PROFILES_DIR` only when deliberately using a separate
profile location.

## Run and validate

```bash
# Re-run at the start of every session (token refresh):
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
