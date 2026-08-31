<section class="workshop-hero" markdown>
<span class="eyebrow">IBM watsonx.data · Iceberg lakehouse workshop</span>
<h1>One retail contract. Multiple execution paths. One trusted Gold outcome.</h1>
<p>This is a technical workshop for showing how a retail source becomes governed, consumable Iceberg data on watsonx.data. Start with the dbt and Presto reference implementation; then compare Spark batch and Kafka/Flink event processing without confusing them as mandatory stages of a single pipeline.</p>
</section>

## What this workshop demonstrates

The workshop deliberately keeps the business outcome constant. Four retail
sources—customers, products, orders, and order items—become three Gold marts:
daily sales, category performance, and customer 360. The delivery mechanism
can change; the metric contract, Iceberg storage layer, validation evidence,
and business result should not.

<div class="workshop-path" markdown>
<strong>Recommended narrative:</strong> understand the lakehouse, ingest the
fixture, build the dbt reference path, consume Gold in Metabase, validate it,
then introduce Spark and streaming as workload-specific alternatives. Finish
with the lineage and governance operating model.
</div>

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

## The flow at a glance

```text
Retail CSVs → Raw Iceberg → Bronze → Silver → Gold marts → Metabase
                         └──────── validation, lineage, governance ────────┘
```

| Workshop chapter | What to understand | What is actually run or inspected |
| --- | --- | --- |
| **Foundation** | Object storage, Iceberg, catalog, compute, and the medallion convention | Lakehouse architecture and table layers |
| **Ingest and transform** | The difference between loading data and transforming it | dbt/Presto reference path; optional Spark or Kafka/Flink paths |
| **Consume and operate** | How trusted Gold is served, scheduled, and verified | Metabase, Airflow, reconciliation checks |
| **Lineage and governance** | Runtime evidence versus catalog and enterprise impact analysis | dbt DAG, OpenLineage/Marquez, OpenMetadata, Manta/IBM options |

The first path is intentionally SQL-first. `dbt seed` provides a repeatable
fixture load for the demo; dbt then compiles models and submits SQL to Presto.
That is distinct from Spark, which stages assets in object storage and runs a
managed application, and from the event path, where Kafka and Flink process
records continuously before Iceberg tables are served.

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

## How to read the alternative paths

The alternatives are not a product bake-off and should not all be run for a
short demo. They show the same lakehouse contract under different processing
conditions:

| Path | Select it when | What changes | What remains invariant |
| --- | --- | --- | --- |
| **dbt + Presto** | Transformations are business SQL and the priority is an auditable analytics workflow | SQL models, tests, and materializations run through Presto | Iceberg tables, naming, Gold measures, validation |
| **Spark** | Scale, Python libraries, complex parsing, or distributed ETL justify an application | A managed Spark job reads/writes Iceberg after assets are staged | Source contract and Gold acceptance criteria |
| **Kafka + Flink** | Events arrive continuously and Silver needs low-latency processing | Producers, Schema Registry, and Flink SQL maintain streaming tables | Business semantics and downstream Gold contract |

For a production conversation, use the final governance section to make the
operating-model choice visible: independently operated open-source components
provide flexibility, while watsonx.data integration and watsonx.data
intelligence can consolidate supported integration, quality, governance, and
lineage capabilities where installed and entitled.
