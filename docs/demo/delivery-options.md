# Delivery-path decision

Start with one business contract, then select the execution model that fits the
workload. The paths below are alternatives, not a mandatory chain.

| Path | Best fit | How it runs in this workshop | Gold contract |
| --- | --- | --- | --- |
| dbt + Presto | SQL-first analytics engineering | dbt seeds fixtures and submits compiled SQL to Presto | Reference implementation |
| Managed Spark | Distributed batch, code libraries, or ML-adjacent processing | Python application is staged in object storage, then submitted to the watsonx.data Spark API | Reconciled to dbt |
| Kafka + Flink | Continuous events and stream state | Producers write Avro events; Schema Registry and self-managed Flink SQL build Silver; Spark or DataStage builds Gold | Reconciled to dbt |
| DataStage | Managed, visual enterprise integration | Optional enterprise implementation of the same source/target contract | Reconciled to dbt |
| cpdctl native ingestion | Proving out the platform's own connector-based load mechanism, independent of any transformation choice | `cpdctl` submits native watsonx.data ingestion jobs for the same 4 CSVs (`bin/demo ingest`) | None — raw only by design; see [Ingest to Bronze](ingestion.md#a-path-that-stops-at-raw-on-purpose-cpdctl-native-ingestion) |

## Decision rule

Choose dbt when the transformation is governed primarily as warehouse-style
SQL. Choose Spark when a submitted distributed application is the better
engineering model. Choose Kafka and Flink when records arrive continuously and
stateful stream processing is required. Choose DataStage when managed
connectors, operational integration, and a visual delivery experience are more
important than composing individual open-source services.

The output format remains Iceberg. Interoperability still requires compatible
catalog configuration, warehouse paths, credentials, and engine support; it is
not automatic simply because all tools mention Iceberg.
