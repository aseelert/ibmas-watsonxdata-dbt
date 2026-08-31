# 5. Airflow: schedule, do not transform

Airflow schedules dbt and Spark commands; it does not replace their lineage or
business logic.

```bash
bin/demo airflow
```

Use the DAG graph to explain retries, dependencies, and schedules. Keep dbt
models as the SQL dependency source of truth.
