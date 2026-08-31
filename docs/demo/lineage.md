# Lineage methods — dbt, OpenLineage, Marquez, and Manta

Lineage is not one product or one graph. Each view answers a distinct question;
presenting them as substitutes creates confusion.

| Method | Evidence collected | Primary question | Workshop role |
| --- | --- | --- | --- |
| dbt DAG | Declared model dependencies and compiled SQL | What should this SQL transformation depend on? | Design-time model view |
| OpenLineage | Standardized runtime events | What execution event happened to which dataset? | Open event contract |
| Marquez | OpenLineage events, jobs, and datasets | What ran, read, and wrote at runtime? | Runtime technical lineage UI |
| Manta Data Lineage | Enterprise metadata and code parsing | What is the end-to-end impact and traceability across systems? | Enterprise technical/business lineage option |

## Runtime lineage: OpenLineage and Marquez

OpenLineage is an event specification, not a catalog or a UI. Marquez receives
and visualizes those events. In this demo, dbt can emit events after successful
artifact-producing commands, and submitted Spark applications send events to
the in-cluster collector.

```bash
bin/demo lineage
```

![Marquez dbt runtime lineage](../assets/images/screenshots/marquez-runtime-lineage.png)

![Marquez Spark runtime lineage](../assets/images/screenshots/marquez-spark-lineage.png)

## Enterprise lineage: Manta Data Lineage

Manta Data Lineage is not a replacement name for Marquez. It addresses an
enterprise parsing, traceability, and impact-analysis operating model and is
complementary to governance services such as IBM Knowledge Catalog. The
following customer-provided workshop capture is illustrative; it is not a
live dependency of this repository.

![Illustrative Manta lineage and impact-analysis view](../assets/images/screenshots/manta-business-lineage.png)

Use Marquez to show the actual dbt/Spark execution events produced by the demo.
Use Manta when the discussion is cross-system lineage, impact analysis, and
regulated evidence. Use a catalog to add ownership, definitions, policies, and
quality context.

References: [OpenLineage](https://openlineage.io/docs/),
[Marquez](https://marquezproject.ai/docs/), and
[IBM Manta Data Lineage on Software Hub 5.4](https://www.ibm.com/docs/en/software-hub/5.4.x?topic=services-manta-data-lineage).
