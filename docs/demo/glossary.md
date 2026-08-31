# Glossary

Short definitions used throughout the workshop. The examples describe this
repository, not a universal rule for every lakehouse implementation.

| Term | Plain-language meaning | In this demo |
| --- | --- | --- |
| **Apache Iceberg** | An open table format that makes files in object storage behave like reliable, versioned tables | Every lakehouse path targets Iceberg relations |
| **Apache Flink** | A processing engine designed for continuous, stateful event processing | Consumes Kafka events and produces Silver tables in the local streaming path |
| **Apache Spark** | A distributed processing engine for batch data engineering, SQL, and code-driven workloads | A managed Spark application writes the batch alternative’s layers |
| **Avro** | A compact, typed event serialization format | Kafka messages are produced under Avro schemas |
| **Bronze** | The first controlled, auditable data layer | dbt/Spark create tables with ingestion context; streaming uses replayable raw topics as the analogous boundary |
| **Catalog** | The namespace and metadata service used to find tables | `iceberg_data` is the catalog used in workshop examples |
| **Confluent** | A commercial data-streaming platform built around Kafka and related products | The repository uses a local Confluent Platform-style stack; Tableflow is explained as a separate Confluent capability |
| **dbt** | Data build tool: versioned SQL transformations with dependencies, tests, and documentation artifacts | The SQL-first reference implementation that submits SQL through Presto |
| **Flink checkpoint** | A coordinated saved processing state used for recovery in a streaming job | Relevant to delivery guarantees, but not a substitute for source/sink design validation |
| **Gold** | A business-ready table or view at a clear reporting grain | Daily sales, category performance, and customer 360 |
| **Kafka** | A durable, ordered event log used by producers and independent consumers | Raw and Silver topics form the streaming transport layer |
| **Lakehouse** | Open object storage plus table metadata and multiple compute engines | watsonx.data with Iceberg, Presto, Spark, and object storage |
| **Medallion** | A convention for Raw/Bronze/Silver/Gold quality and serving layers | The workshop’s way to explain progressive data trust |
| **Metabase** | A business-intelligence application | Local dashboard consumer for Gold tables |
| **OpenLineage** | An open event specification for lineage runtime events | dbt/Spark runs can emit events to Marquez |
| **Parquet** | A columnar data-file format | The data files beneath the workshop’s Iceberg tables |
| **Presto** | A distributed SQL query engine | Executes dbt-compiled SQL and serves analytic queries |
| **Schema Registry** | A service that stores and checks event-schema compatibility | Holds the Avro contracts for the streaming path |
| **Silver** | Clean, typed, joined reusable entities | The normalized source tables and enriched sales fact |
| **Tableflow** | A Confluent capability that materializes Kafka topics as Iceberg or Delta tables | Not the local repo implementation; used as an architecture comparison |
| **watsonx.data integration** | IBM integration service family, including capabilities such as DataStage depending on deployment and entitlement | Enterprise implementation option, not assumed to be bundled |
| **watsonx.data intelligence / IKC** | IBM governance, catalog, semantic, lineage, and data-product capabilities depending on installed scope | Enterprise governance option, not equivalent to a local OpenMetadata stack |
