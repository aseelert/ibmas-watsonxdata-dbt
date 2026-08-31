# Catalog, quality, and governance

Cataloging adds context that runtime lineage alone cannot provide: ownership,
definitions, classifications, stewardship workflows, and evidence of quality.

```bash
bin/demo catalog
```

## Open-source catalog demonstration

OpenMetadata ingests the dbt artifacts and table metadata from this workshop.
It exposes technical assets, dbt lineage, and governance enrichment in one UI.
The catalog workflow is deliberately separate from the data transformation
workflow.

![OpenMetadata lineage](../assets/images/screenshots/openmetadata-lineage.png)

## Open-source and IBM capability view

| Capability | Workshop open-source stack | IBM enterprise option |
| --- | --- | --- |
| Technical catalog | OpenMetadata | watsonx.data intelligence capabilities where entitled |
| Business vocabulary and approvals | Configured catalog/governance process | IBM Knowledge Catalog governance artifacts and workflow |
| Data quality | dbt tests and separately composed profiling/validation | watsonx.data intelligence quality profiling, cleansing, and validation; DataStage Enterprise Plus quality functions where entitled |
| Enterprise lineage and impact | OpenLineage/Marquez runtime events plus catalog metadata | Manta Data Lineage and related governance services |

On Software Hub 5.4.x, the service pages document watsonx.data intelligence
2.4.0 as an integrated governance, lineage, and data-product capability layer.
That is a deployment and entitlement decision; do not imply it is included in
every base watsonx.data installation.

References: [watsonx.data intelligence 2.4.0](https://www.ibm.com/docs/en/software-hub/5.4.x?topic=services-watsonxdata-intelligence),
[IBM Knowledge Catalog 5.4](https://www.ibm.com/docs/en/software-hub/5.4.x?topic=services-knowledge-catalog),
and [DataStage 5.4](https://www.ibm.com/docs/en/software-hub/5.4.x?topic=services-datastage).
