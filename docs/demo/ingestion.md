# Ingest to Bronze

## Baseline source path

The baseline starts with four CSV fixtures under `01-dbt/seeds/`. `dbt seed`
loads them into the raw Iceberg schema through Presto. Bronze models then add
typed columns and ingestion metadata. This is suitable for a controlled demo
fixture; it is not a claim that dbt is a general-purpose source-ingestion
platform.

```text
CSV fixtures → dbt seed → iceberg_data.dbt_demo_raw
             → dbt Bronze SQL → iceberg_data.dbt_demo_bronze
```

## Production choice

For operational ingestion, select a mechanism based on source and latency:

| Need | Open-source composition | IBM enterprise option |
| --- | --- | --- |
| Scheduled files and databases | Custom jobs, connectors, orchestration | DataStage / watsonx.data Integration connectors and jobs |
| Continuous events | Kafka, Schema Registry, Flink | Managed integration pattern where entitled |
| Audit and recovery | Object-store conventions plus service-specific controls | Platform governance and operational controls where licensed |

Regardless of tooling, record source identity, arrival time, batch or event
identifier, and schema version before business transformation. Those fields
make Bronze an auditable boundary rather than a second Raw area.
