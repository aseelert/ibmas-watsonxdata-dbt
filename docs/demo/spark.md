# Batch alternative — submitted Spark application

Spark uses a different execution model from dbt. A Python application and its
input assets are staged in object storage, then the watsonx.data Spark
application API starts a managed runtime. The application uses DataFrame and
Spark SQL operations to read inputs and commit Bronze, Silver, and Gold
Iceberg tables.

```text
Python application + CSV assets
  → object storage
  → watsonx.data Spark application API
  → managed driver and executors
  → Iceberg Bronze / Silver / Gold
```

```bash
python3 scripts/03a_upload_spark_assets.py
WXD_SPARK_DRY_RUN=false python3 scripts/03b_submit_spark_application.py
python3 scripts/03c_spark_application_status.py <application-id>
python3 scripts/reconcile_gold.py --paths dbt,spark
```

## Why the distinction matters

dbt expresses the business transformation as adapter-executed SQL. Spark runs a
submitted distributed application, which is useful when the workload needs
code libraries, distributed processing control, or non-SQL transformations.
Both can implement ETL; neither is intrinsically more governed. The business
definition is validated by Gold reconciliation.

References: [Spark SQL and DataFrames](https://spark.apache.org/docs/latest/sql-programming-guide.html),
[Spark data sources](https://spark.apache.org/docs/latest/sql-data-sources.html),
and [Iceberg Spark writes](https://iceberg.apache.org/docs/latest/spark-writes/).
