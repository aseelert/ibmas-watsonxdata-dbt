# Lakehouse and medallion

## The architecture contract

A lakehouse separates durable data from the engines that process and consume
it. In this workshop, object storage holds Apache Iceberg tables; Presto and
Spark are compute engines; dbt, Airflow, and Flink define different execution
paths. Iceberg provides table metadata, snapshots, and schema evolution. It
does not prescribe Bronze, Silver, or Gold.

![Lakehouse medallion architecture](../assets/images/infographics/wxd-infographic-01-medallion.png)

## The layers have different responsibilities

| Layer | In this workshop | Why the separation matters |
| --- | --- | --- |
| **Object storage** | Durable Parquet data and Iceberg metadata | Compute can be replaced without moving the business data estate |
| **Iceberg catalog** | The `iceberg_data` namespace and table definitions | Readers and writers use tables, snapshots, and schema contracts—not loose files |
| **Compute** | Presto for SQL; managed Spark for distributed applications; Flink for the event path | Each engine can serve the workload it is suited to while sharing the table format |
| **Control plane** | dbt, Airflow, job submission scripts, and validations | Makes execution repeatable, observable, and reviewable in Git |
| **Consumption and governance** | Metabase, OpenMetadata, lineage tools, and IBM services where licensed | Adds meaning, ownership, operational evidence, and access control context |

This separation is the architectural point of the demo. A Gold mart is not a
feature of dbt, Spark, or Flink; it is an agreed business interface stored as
an Iceberg table or view. The processing tool is selected for the workload,
then validated against that interface.

## Medallion is a quality and serving convention

| Layer | Purpose | Typical control |
| --- | --- | --- |
| Raw | Land source-shaped records | Source identity and load timestamp |
| Bronze | Preserve typed, auditable copies | Technical validation and ingestion metadata |
| Silver | Standardize and join reusable entities | Keys, type rules, and quality tests |
| Gold | Serve a business grain | Metric definition, access, and semantic ownership |

The workshop has four retail sources—customers, products, orders, and order
items—and publishes three Gold marts: daily sales, category performance, and
customer 360. Every processing path must produce the same business result.

### A practical definition of the layers

Raw preserves source-shaped inputs and their arrival context. Bronze makes an
auditable managed copy. Silver applies reusable type, key, and join rules.
Gold serves a declared business grain such as daily category sales. These are
conventions: a team may name or implement them differently, but it should be
able to point to the same controls and ownership boundaries.

## What is deliberately separate

Storage, processing, orchestration, cataloging, and lineage are separate
concerns. That modularity is useful, but it creates operational integration
work. The [delivery-path decision](delivery-options.md) and [platform
comparison](platform-choice.md) make that trade-off explicit.

For the demo, this modularity is valuable because each tool is visible and
testable. For a production estate, the same modularity introduces more
identity, upgrade, monitoring, metadata, and support responsibilities. The
IBM capability comparison later in the workshop frames that as an
operating-model decision rather than a claim that an open stack is incomplete.
