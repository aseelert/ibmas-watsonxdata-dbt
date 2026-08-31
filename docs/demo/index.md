# watsonx.data Lakehouse demo

This demo tells one story: land four retail CSV datasets once, create governed
Iceberg tables, and show how SQL, Spark, streaming, orchestration, lineage,
and cataloging consume the same business data.

Start with dbt. It is the shortest complete path from CSV to tested Gold
tables. Spark and Confluent demonstrate alternative processing styles, not
extra prerequisites.

```bash
bin/demo setup
bin/demo dbt build
bin/demo metabase
```

All public commands run from the repository root through `bin/demo`.

For all end-user UIs, service APIs, and ports, see [Access: URLs and ports](access.md).
