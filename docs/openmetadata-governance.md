# OpenMetadata Glossary & Classification

This page documents the governance layer that is applied after OpenMetadata has the dbt tables and lineage. It explains what OpenMetadata is, what this demo sends into it, how glossary terms and classifications are attached, and where OpenLineage fits when you want lineage events from multiple execution engines.

It is intentionally separate from the core [dbt](dbt-demo.md) and [Spark](spark-demo.md) walkthroughs: dbt and Spark build the data, while OpenMetadata catalogs the meaning, tags, glossary terms, descriptions, and lineage.

## OpenMetadata in Plain English

OpenMetadata is a **data catalog**. It does not replace dbt, Spark, Presto, Iceberg, Airflow, or a BI tool. It sits beside them and records what they know about the data:

| Question | OpenMetadata answer in this demo |
| --- | --- |
| What tables exist? | `raw`, `bronze`, `silver`, and `gold` Iceberg tables under `watsonxdata-presto.iceberg_data`. |
| What does each table/column mean? | Descriptions from dbt `schema.yml`, with fallback descriptions from `scripts/apply_openmetadata_governance.py`. |
| Where did the data come from? | dbt lineage from `manifest.json`, showing raw seeds through bronze, silver, and gold. |
| Can I trust the model? | dbt test results from `run_results.json` in the Data Quality tab. |
| How is the asset governed? | Glossary terms, classification tags, PII/financial tags, medallion layer tags, and online/offline ingestion-mode tags. |

OpenMetadata stores **metadata**, not business data rows. The actual rows stay in watsonx.data as Iceberg/Parquet data. OpenMetadata stores the catalog record: names, descriptions, owners/tags, lineage edges, test status, and glossary relationships.

## Governance Model

Governance in this demo has four layers:

| Layer | Implemented by | Purpose |
| --- | --- | --- |
| dbt documentation | `models/**/schema.yml` | Version-controlled table and column descriptions close to the SQL. |
| dbt tests | `schema.yml` tests + `run_results.json` | Data quality evidence, such as primary-key, not-null, accepted-value, and relationship tests. |
| OpenMetadata glossary | `MedallionGlossary` | Business meaning: Customer, Product, Order, RevenueMetric, Customer360, and medallion-layer concepts. |
| OpenMetadata classifications | `MedallionLayer`, `DemoDataDomain`, `MetadataIngestionMode` | Searchable labels for layer, domain, PII/financial fields, operational metadata, and live/offline ingestion source. |

This split matters. dbt owns the transformation and documentation-as-code. OpenMetadata owns the cross-tool catalog view. The governance script only enriches the catalog; it does not change the warehouse tables or dbt SQL.

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

## Auto-Classification Rules

The auto-classification in this repo is deterministic and local. It is not an ML classifier; it is a set of transparent naming rules that make the demo catalog complete and repeatable.

| Rule | Result |
| --- | --- |
| Schema ends in `_raw`, `_bronze`, `_silver`, or `_gold` | Apply the matching `MedallionLayer` tag. |
| Column/table name contains `customer`, or fields like `first_name`, `last_name`, `email` | Apply `DemoDataDomain.Customer`. |
| Column name is `email` | Apply `DemoDataDomain.PII`. |
| Column/table name contains `product`, `category`, or `unit_price` | Apply `DemoDataDomain.Product`. |
| Column/table name contains `order`, `status`, `quantity`, or `discount_pct` | Apply `DemoDataDomain.Order`. |
| Column name contains `price`, `amount`, `revenue`, `value`, or `discount` | Apply `DemoDataDomain.FinancialMetric`. |
| Column starts with `_` or is `transformed_at` | Apply `DemoDataDomain.OperationalMetadata`. |
| Table entities were created from live Presto | Apply `MetadataIngestionMode.OnlinePresto`. |
| Table entities were created from dbt artifacts | Apply `MetadataIngestionMode.OfflineDbtArtifacts`. |

Because these rules are explicit, they are easy to review in a workshop and easy to adapt for a real client taxonomy.

## Source of Truth

Descriptions exist in two places:

| Source | Purpose |
| --- | --- |
| `models/**/schema.yml` | dbt documentation-as-code. These descriptions appear in dbt docs and are included in dbt artifacts. |
| `scripts/apply_openmetadata_governance.py` | OpenMetadata fallback descriptions and classification rules. This keeps live Presto and offline dbt-artifact runs consistent. |

When the two differ, update `schema.yml` first, then update the governance script only if the OpenMetadata fallback should change too.

## OpenMetadata vs OpenLineage

OpenMetadata and OpenLineage solve related but different problems:

| Capability | OpenMetadata | OpenLineage |
| --- | --- | --- |
| Primary role | Catalog and governance UI/API. | Open event standard for runtime lineage. |
| Stores catalog entities | Yes: services, databases, schemas, tables, columns, glossary terms, tags. | No. It defines events; a collector stores or forwards them. |
| User-facing UI | Yes. | Not by itself. You view events in a collector such as Marquez, OpenMetadata, or another lineage platform. |
| Best input for dbt-only lineage | dbt artifacts are simple and complete for dbt models. | Useful if you want dbt runs to emit runtime events. |
| Best input for Spark/Airflow lineage | Can consume lineage, but needs events/connectors. | Strong fit: Spark listeners and Airflow providers emit run events. |
| Demo status | Fully wired through dbt artifacts and governance enrichment. | Explained as the standard; not wired in this repo yet. |

Use **OpenMetadata** when you want a browsable catalog with glossary, tags, descriptions, quality tests, and lineage. Use **OpenLineage** when you want a common event format that multiple engines can emit while jobs run.

## Enterprise Catalogs: IKC, watsonx.data Intelligence, and Manta

In an IBM client environment, OpenLineage is useful because it gives pipeline tools a standard event shape before those events are sent to a lineage/catalog system. A production design can use OpenLineage events from dbt, Spark, and Airflow and then forward, transform, or import that metadata into enterprise governance products such as IBM Knowledge Catalog / watsonx.data intelligence or a Manta lineage deployment, depending on the connectors and import paths supported in that installed version.

This repo does **not** claim that OpenLineage is already connected to IBM Knowledge Catalog, watsonx.data intelligence, or Manta. The point is architectural:

1. dbt, Spark, and Airflow emit lineage in a common OpenLineage format.
2. A collector or integration service receives the events.
3. The enterprise catalog or lineage tool consumes the resulting metadata through its supported API, connector, file import, or bridge.
4. OpenMetadata can remain the local workshop UI, while the same lineage concept scales to a governed enterprise catalog.

That distinction is important for demos: OpenMetadata proves the local lineage and glossary story; OpenLineage explains how the same pipeline can participate in a broader governance estate.

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
