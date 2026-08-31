# Open source and IBM platform

The workshop deliberately makes the open-source composition visible. It is a
useful architecture for learning and selective adoption, but each service also
adds deployment, upgrade, identity, monitoring, metadata, and support
ownership. The IBM services are compared as an operating-model choice, not as
an assertion that one product replaces every component.

## Capability comparison

| Concern | Open-source workshop composition | IBM platform option on Software Hub 5.4.x |
| --- | --- | --- |
| Open lakehouse | Iceberg tables, object storage, Presto, Spark | watsonx.data 2.4.0 lakehouse service |
| Batch integration | dbt SQL and custom Spark applications | watsonx.data integration 2.4.0; DataStage for graphical batch/real-time flows |
| Streaming integration | Kafka, Schema Registry, self-managed Flink | watsonx.data integration capabilities; assess the required component and entitlement |
| Orchestration | Airflow | Platform or external orchestration according to the delivery design |
| Runtime lineage | OpenLineage events and Marquez | Can coexist; not a claim of feature equivalence |
| Business/impact lineage | Separately composed scanner/catalog process | Manta Data Lineage with related governance services |
| Catalog and governance | OpenMetadata plus local processes | watsonx.data intelligence 2.4.0 and IBM Knowledge Catalog capabilities |
| Data quality | dbt tests and separately composed validation | watsonx.data intelligence quality capabilities; DataStage Enterprise Plus quality functions where entitled |

## Product boundaries

watsonx.data is the open lakehouse/data layer. watsonx.data integration is a
separate service family for transformation, integration, and observability;
its documented components include DataStage, StreamSets, Data Replication,
Unstructured Data Integration, and Data Observability. watsonx.data
intelligence is a separate governance, lineage, and data-product capability
layer. Manta Data Lineage and IBM Knowledge Catalog are complementary services,
not synonyms for Marquez or OpenMetadata.

Confirm the exact Software Hub release, installed service version, licensing,
and entitlement in the target environment before committing to an architecture.
The statements above use the IBM Software Hub 5.4.x documentation baseline,
which lists watsonx.data, watsonx.data integration, and watsonx.data
intelligence at version 2.4.0.

References: [watsonx.data](https://www.ibm.com/docs/en/software-hub/5.4.x?topic=services-watsonxdata),
[watsonx.data integration](https://www.ibm.com/docs/en/software-hub/5.4.x?topic=services-watsonxdata-integration),
[watsonx.data intelligence](https://www.ibm.com/docs/en/software-hub/5.4.x?topic=services-watsonxdata-intelligence),
[DataStage](https://www.ibm.com/docs/en/software-hub/5.4.x?topic=services-datastage),
[Manta Data Lineage](https://www.ibm.com/docs/en/software-hub/5.4.x?topic=services-manta-data-lineage),
and [IBM Knowledge Catalog](https://www.ibm.com/docs/en/software-hub/5.4.x?topic=services-knowledge-catalog).
