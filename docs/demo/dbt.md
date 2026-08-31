# 2. dbt: CSV to trusted Gold

dbt loads the seed CSVs, creates Bronze, Silver, and Gold Iceberg objects via
Presto, and tests the resulting business model.

```bash
bin/demo dbt seed
bin/demo dbt run
bin/demo dbt test
```

This is the reference implementation. Spark and streaming outputs are compared
with it, rather than becoming competing sources of business truth.
