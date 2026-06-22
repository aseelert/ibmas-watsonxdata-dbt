# OpenMetadata Glossary & Classification

This page documents the governance layer that is applied after OpenMetadata has the dbt tables and lineage.

It is intentionally separate from the core [dbt](dbt-demo.md) and [Spark](spark-demo.md) walkthroughs: dbt and Spark build the data, while OpenMetadata catalogs the meaning, tags, glossary terms, descriptions, and lineage.

## What Gets Applied

The ingestion flow runs in three passes:

1. **Live table discovery:** OpenMetadata connects to Presto and discovers the real `iceberg_data.dbt_demo_*` tables.
2. **Offline fallback:** if the live Presto pass fails, `scripts/seed_openmetadata_tables.py` creates the same table entities from staged dbt `catalog.json`.
3. **Governance enrichment:** `scripts/apply_openmetadata_governance.py` creates glossary/classification objects and attaches them to the dbt tables and columns.

The governance pass is the same for online and offline runs. The only difference is the ingestion-mode tag:

| Mode | Applied tag | Meaning |
| --- | --- | --- |
| Online | `MetadataIngestionMode.OnlinePresto` | Table entities came from a live Presto metadata scan. |
| Offline | `MetadataIngestionMode.OfflineDbtArtifacts` | Table entities came from staged dbt artifacts because live Presto was skipped or failed. |

## Glossary

The script creates an OpenMetadata glossary named `MedallionGlossary`.

| Term | Used for |
| --- | --- |
| `RawLanding` | dbt seed tables in `dbt_demo_raw`. |
| `BronzeLayer` | managed raw-copy models in `dbt_demo_bronze`. |
| `SilverLayer` | clean and conformed models in `dbt_demo_silver`. |
| `GoldLayer` | reporting marts in `dbt_demo_gold`. |
| `Customer` | customer dimension tables and customer identifiers. |
| `Product` | product dimension/category tables and fields. |
| `Order` | order header and order status data. |
| `OrderItem` | order line item data. |
| `RevenueMetric` | price, discount, amount, revenue, and lifetime value fields. |
| `Customer360` | the `gold_customer_360` customer mart. |

## Classifications

Three custom classifications are created:

| Classification | Tags |
| --- | --- |
| `MedallionLayer` | `Raw`, `Bronze`, `Silver`, `Gold` |
| `DemoDataDomain` | `Customer`, `Product`, `Order`, `FinancialMetric`, `OperationalMetadata`, `PII` |
| `MetadataIngestionMode` | `OnlinePresto`, `OfflineDbtArtifacts` |

The table layer tag is inferred from the schema suffix. Column domain tags are inferred from stable names such as `customer_id`, `email`, `net_revenue`, `_source_file`, and `transformed_at`.

## Source of Truth

Descriptions exist in two places:

| Source | Purpose |
| --- | --- |
| `models/**/schema.yml` | dbt documentation-as-code. These descriptions appear in dbt docs and are included in dbt artifacts. |
| `scripts/apply_openmetadata_governance.py` | OpenMetadata fallback descriptions and classification rules. This keeps live Presto and offline dbt-artifact runs consistent. |

When the two differ, update `schema.yml` first, then update the governance script only if the OpenMetadata fallback should change too.

## Run Manually

Normally this runs automatically from `openmetadata/ingestion/run-ingestion.sh`. To re-apply only governance labels after an existing ingestion:

```bash
source .venv/bin/activate
python scripts/apply_openmetadata_governance.py --mode online
```

Force the offline classification tag:

```bash
python scripts/apply_openmetadata_governance.py --mode offline
```

Use `--strict` when you want the command to fail if the local OpenMetadata API rejects a glossary, tag, or table update.
