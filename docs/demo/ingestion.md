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

## Loading is not transformation

The distinction matters when presenting the demo. A load moves source-shaped
records to a durable, queryable boundary. A transformation applies business or
technical rules to that boundary. `dbt seed` is used here because the fixture
is versioned alongside the models and gives every participant a deterministic
starting point. The Bronze models then make the first controlled changes under
version control.

| Question | Ingestion answer | Transformation answer |
| --- | --- | --- |
| What arrived? | Source file, arrival time, batch/event identity, schema version | Not applicable yet |
| Is it usable? | It is safely landed and recoverable | Types, keys, records, and joins are validated |
| Who owns the logic? | Source/integration owner | Data-product or analytics owner |
| Workshop example | `dbt seed` for the CSV fixture | dbt Bronze, Silver, and Gold models |

For an operational source, retain the original input or an immutable copy and
capture the load metadata before applying any business cleaning. It provides
the evidence needed to replay a batch, isolate a schema change, and explain a
downstream result.

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

In an IBM platform discussion, DataStage and watsonx.data integration are
appropriate options when the delivery model needs governed connectors,
graphical development, managed schedules, or enterprise operating controls.
Their availability depends on the installed services and entitlement; this
workshop does not represent them as included in every watsonx.data deployment.
