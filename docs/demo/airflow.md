# Orchestrate with Airflow

Airflow schedules and observes work; it should not become the home of business
transformation logic. This workshop includes dbt and Spark DAGs that call the
same public execution paths described elsewhere.

```bash
bin/demo airflow
```

![Airflow DAGs](../assets/images/screenshots/airflow-dags.png)

Use the DAG graph to discuss dependencies, retries, schedules, and operational
ownership. Keep dbt model dependencies in dbt and Spark transformation logic in
the submitted application. Airflow operational events can complement runtime
lineage, but they do not replace data-level lineage.
