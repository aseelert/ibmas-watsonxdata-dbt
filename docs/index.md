# watsonx.data lakehouse workshop

This workshop uses one retail data contract and three implementation paths to
show an open Iceberg lakehouse in practice. The baseline completes first; the
Spark and event paths are independently runnable alternatives whose Gold
outputs are reconciled to that baseline.

## Workshop outcome

At the end, the audience can explain where data is stored, how it becomes a
trusted Gold mart, who consumes it, and which form of lineage answers which
question.

| Stage | Primary outcome | Demonstrated with |
| --- | --- | --- |
| Foundation | Open tables in object storage | Apache Iceberg and watsonx.data |
| Baseline | Tested retail marts | dbt + Presto |
| Consumption | Business dashboard | Metabase |
| Alternatives | Batch and event implementations | Spark; Kafka + Flink |
| Trust | Execution, technical, and business context | OpenLineage/Marquez, OpenMetadata, IBM options |

## Recommended 20-minute route

```bash
bin/demo setup
bin/demo dbt build
bin/demo metabase
bin/demo validate
```

Then use the Spark and event pages to choose the implementation appropriate to
the workload. They are not mandatory sequential steps.

All public commands run from the repository root through `bin/demo`. See
[Access and interfaces](demo/access.md) for the business and operator URLs.
