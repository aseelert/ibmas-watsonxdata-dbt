<!--
=============================================================================
 NAMING.md — single source of truth for the 3-way medallion naming + parity

 Location  : confluent/NAMING.md
 Repository: https://github.ibm.com/alexander/ibmas-watsonxdata-dbt
 Project   : watsonx.data · dbt · Spark · Confluent medallion demo
 Author    : Alexander Seelert — IBM Customer Success Engineer
 Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.

 Changelog :
   v1.0 (2026-06-26) — Initial version. Captures the schema/naming map and the
     3-way (dbt / Spark / Confluent) gold-parity contract.
=============================================================================
-->

# Schema & Naming Map — the parity contract

This demo proves that **three independent engines** — dbt, Spark, and
Confluent/Flink — produce the **identical gold result** in the
`iceberg_data` Iceberg catalog, starting from the **same 4 seed CSVs**:

| Seed file               | Rows  | Meaning              |
|-------------------------|-------|----------------------|
| `seeds/raw_customers.csv`   |   50  | customers            |
| `seeds/raw_products.csv`    |   20  | products             |
| `seeds/raw_orders.csv`      |  500  | orders               |
| `seeds/raw_order_items.csv` | 1134  | order line items     |

If any engine's gold numbers differ, the demo has a bug. **dbt is the source
of truth for the business logic** — Spark and Confluent must match it exactly,
including the intentional non-distinct order counting and decimal math.

---

## Canonical gold marts (identical logic across all paths)

| Mart                      | Grain                          | Key metrics                                                        |
|---------------------------|--------------------------------|--------------------------------------------------------------------|
| `daily_sales`             | one row per (`order_date`, `category`) | `order_count` (count of completed orders), `total_revenue` (sum of `net_amount`), … |
| `category_performance`    | one row per product category   | per-category revenue / order / item rollups                        |
| `customer_360`            | one row per customer           | LEFT join to orders, 0-filled (every customer appears, even with no orders) |

> The dbt model `gold_daily_sales` defines the canonical SQL. Match it
> **exactly** — same joins, same filters, same rounding.

---

## Schema / table naming per engine (catalog `iceberg_data`)

| Engine        | Medallion schemas                              | Gold mart table names                                                                  |
|---------------|------------------------------------------------|----------------------------------------------------------------------------------------|
| **dbt**       | `dbt_demo_{raw,bronze,silver,gold}`            | `gold_daily_sales`, `gold_category_performance`, `gold_customer_360`                    |
| **Spark**     | `spark_demo_{bronze,silver,gold}`              | `spark_gold_daily_sales`, `spark_gold_category_performance`, `spark_gold_customer_360`  |
| **Confluent** | silver: `confluent_demo_silver` (Flink-written, tables `confluent_silver_*`)<br>gold: `confluent_demo_gold` | `confluent_gold_daily_sales`, `confluent_gold_category_performance`, `confluent_gold_customer_360` |

All names come from `.env` via env-var lookups — **nothing is hardcoded**. The
relevant variables are:

| Variable                  | Default                  | Used for                                  |
|---------------------------|--------------------------|-------------------------------------------|
| `WXD_SCHEMA`              | `dbt_demo`               | dbt medallion schema prefix               |
| `WXD_SPARK_SCHEMA`       | `spark_demo`             | Spark medallion schema prefix             |
| `CONFLUENT_SILVER_SCHEMA`| `confluent_demo_silver`  | Flink-written silver tables               |
| `CONFLUENT_GOLD_SCHEMA`  | `confluent_demo_gold`    | Confluent gold marts                      |

---

## Confluent gold: Spark **or** DataStage (same result, two engines)

The Confluent path's **silver** layer is always written by **Flink**. The
**gold** marts on top are built by a *second* engine chosen with
`CONFLUENT_GOLD_ENGINE`:

| `CONFLUENT_GOLD_ENGINE` | How gold is built                                              | Demo story                          |
|-------------------------|----------------------------------------------------------------|-------------------------------------|
| `spark` (default)       | watsonx.data **Spark** job runs the gold SQL                   | code-first, reuses Spark engine     |
| `datastage`             | IBM **DataStage** flow runs the same logic as visual no-code ETL | enterprise GUI ETL on the same data |

Both engines read `confluent_demo_silver` and write the **same**
`confluent_demo_gold` marts, so the parity contract holds regardless of which
one runs.

DataStage details (only when `CONFLUENT_GOLD_ENGINE=datastage`):

| Variable                    | Value                                    |
|-----------------------------|------------------------------------------|
| `WXD_DATASTAGE_PROJECT_NAME`| `ibmas-ingest-demo`                      |
| `WXD_DATASTAGE_PROJECT_ID`  | `2d2415ea-71b5-4215-a7b6-b32a4889611e`   |

Auth reuses `WXD_API_KEY` + `WXD_CPD_USERNAME` (`cpadmin`) against `WXD_CPD_HOST`.

---

## The contract, in one sentence

> **Same 4 CSVs → same 3 gold marts (`daily_sales`, `category_performance`,
> `customer_360`) with identical numbers, whether the path is dbt, Spark, or
> Confluent (Flink silver + Spark/DataStage gold).**
