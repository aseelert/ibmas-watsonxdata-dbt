#!/usr/bin/env python3
# -----------------------------------------------------------------------------
#  load_medallion_demo.py — PySpark job that builds the bronze/silver/gold demo
#
#  Location  : spark/load_medallion_demo.py
#  Repository: https://github.com/aseelert/ibmas-watsonxdata-dbt
#  Project   : watsonx.data · dbt · Spark · Confluent medallion demo
#  Author    : Alexander Seelert — IBM Customer Success Engineer
#  Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.
#
#  Changelog :
#    v1.0 (2026-06-26) — Initial version. Imperative PySpark counterpart to the
#                        dbt path: builds the full bronze/silver/gold medallion as
#                        Iceberg tables.
#    v1.1 (2026-06-26) — Parity pass with the dbt gold marts. Confirmed the gold
#                        daily-sales / category-performance / customer-360 SQL
#                        logic is column-and-logic identical to dbt (non-distinct
#                        order_count sum, nullif divide-by-zero guard, LEFT-join
#                        0-filled customer 360, INNER-join enriched fact) and that
#                        no non-binding ORDER BY is emitted (reconciliation is
#                        order-insensitive).
#    v1.2 (2026-06-26) — FULL materialisation parity with dbt: category_performance
#                        and customer_360 are no longer physical tables. This job
#                        now writes ONLY spark_gold_daily_sales (table) and drops
#                        any stale objects at the other two names; those two marts
#                        are created as Presto VIEWS by scripts/create_gold_views.py
#                        (run by scripts/submit_spark_application.py after FINISH),
#                        because a Spark CREATE VIEW makes a Hive view watsonx
#                        Presto cannot read.
# -----------------------------------------------------------------------------
"""PySpark job that writes the demo data as Iceberg tables in a medallion layout.

WHAT & WHY
----------
This is the optional, Spark-native counterpart to the dbt path of the demo. It
reads the same raw demo CSVs and materialises a full bronze/silver/gold
medallion architecture as Apache Iceberg tables in the watsonx.data Iceberg
catalog (backed by MinIO/S3 object storage). It exists so the demo can show the
SAME business model produced two ways — declaratively via dbt and
imperatively via PySpark — and so the resulting Iceberg tables can be queried
interchangeably from Presto, dbt, OpenMetadata, or Metabase.

Medallion layers produced:
- bronze: the four raw CSVs ingested as-is, each annotated with ingestion
  metadata columns (_ingested_at, _ingested_by, _source_file, _ingest_batch_id).
- silver: cleaned / typed dimensions and facts (customers, products, orders,
  order_items), plus one enriched fact (spark_silver_sales_enriched) that joins
  all four entities at order-line grain. Orders and the enriched fact are
  partitioned by month of order_date.
- gold: business aggregates — spark_gold_daily_sales (a physical, partitioned
  table) and, layered on top of it, spark_gold_category_performance, plus a
  spark_gold_customer_360 view of per-customer metrics joined back to the
  customer dimension so customers without orders still appear.

WHEN TO RUN
-----------
Run this AFTER the raw demo CSVs have been generated and uploaded to object
storage (so that <WXD_SPARK_INPUT_BASE>/raw_*.csv exist) and against an
environment where Spark is already configured for the watsonx.data Iceberg
catalog and MinIO. It is an alternative to / supplement of the dbt run; it does
not depend on the dbt models existing.

ENV VARS (read at startup; a sibling .env is auto-loaded if python-dotenv is
installed)
- WXD_SPARK_CATALOG        : Iceberg catalog name (default "iceberg_data").
- WXD_SPARK_INPUT_BASE     : base path holding raw_*.csv; s3a://, s3:// or a
                             local path (default "s3a://iceberg-bucket/spark_demo/raw").
- WXD_SPARK_SCHEMA         : base schema/namespace (default "spark_demo"); the
                             bronze/silver/gold schemas default to this with a
                             _bronze/_silver/_gold suffix.
- WXD_SPARK_BRONZE_SCHEMA  : override the bronze namespace.
- WXD_SPARK_SILVER_SCHEMA  : override the silver namespace.
- WXD_SPARK_GOLD_SCHEMA    : override the gold namespace.
- WXD_SPARK_INGEST_BATCH_ID: batch id stamped onto bronze rows
                             (default "spark_demo_batch").

PREREQUISITES
-------------
A running Spark session/engine wired to the watsonx.data Iceberg catalog and the
MinIO/S3 object store (e.g. a watsonx.data Spark application or a local Spark
with the Iceberg + S3A jars and catalog config). No oc/cpdctl login is performed
by this script — those must already be in place if your Spark engine needs them.
NOTE: the watsonx.data Spark engine's Iceberg catalog uses a fixed warehouse, so
managed tables always land at <bucket-root>/<schema>.db/ and a CREATE NAMESPACE
... LOCATION clause is ignored — this job deliberately does not set a location.

USAGE
-----
    spark-submit spark/load_medallion_demo.py
    # or inside a configured pyspark environment:
    python3 spark/load_medallion_demo.py

SIDE EFFECTS & EXIT
-------------------
Creates the three namespaces if absent and createOrReplace()s every bronze,
silver and gold table listed above (existing tables are overwritten). Progress
is printed per namespace/table. The Spark session is stopped before return; the
process exits 0 on success and propagates any Spark error otherwise.
"""

from __future__ import annotations

import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None


ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    if load_dotenv is not None:
        load_dotenv(ROOT / ".env")

    catalog = os.getenv("WXD_SPARK_CATALOG", "iceberg_data")
    input_base = os.getenv("WXD_SPARK_INPUT_BASE", "s3a://iceberg-bucket/spark_demo/raw").rstrip("/")
    base_schema = os.getenv("WXD_SPARK_SCHEMA", "spark_demo")
    bronze_schema = os.getenv("WXD_SPARK_BRONZE_SCHEMA", f"{base_schema}_bronze")
    silver_schema = os.getenv("WXD_SPARK_SILVER_SCHEMA", f"{base_schema}_silver")
    gold_schema = os.getenv("WXD_SPARK_GOLD_SCHEMA", f"{base_schema}_gold")

    print("[load_medallion_demo] starting watsonx.data medallion build (bronze/silver/gold)")
    spark = SparkSession.builder.appName("watsonxdata-medallion-demo").getOrCreate()
    print(f"Spark catalog: {catalog}")
    print(f"Input CSV base: {input_base}")
    print(f"Bronze schema: {bronze_schema}")
    print(f"Silver schema: {silver_schema}")
    print(f"Gold schema: {gold_schema}")

    # The watsonx.data Spark engine's Iceberg catalog uses a fixed warehouse, so
    # managed tables always land at <bucket-root>/<schema>.db/ — a CREATE NAMESPACE
    # ... LOCATION clause is ignored here. We therefore do not set a location.
    for schema in [bronze_schema, silver_schema, gold_schema]:
        print(f"Ensuring namespace {catalog}.{schema}")
        spark.sql(f"create namespace if not exists {catalog}.{schema}")

    for table in ["customers", "products", "orders", "order_items"]:
        source_name = f"raw_{table}.csv"
        if input_base.startswith(("s3a://", "s3://")):
            source = f"{input_base}/{source_name}"
        else:
            source = str(Path(input_base) / source_name)
        df = (
            spark.read.option("header", "true").csv(source)
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_ingested_by", F.lit("spark job"))
            .withColumn("_source_file", F.lit(source_name))
            .withColumn("_ingest_batch_id", F.lit(os.getenv("WXD_SPARK_INGEST_BATCH_ID", "spark_demo_batch")))
        )
        print(f"Writing bronze table {catalog}.{bronze_schema}.bronze_{table}")
        df.writeTo(f"{catalog}.{bronze_schema}.bronze_{table}").using("iceberg").createOrReplace()

    customers = spark.table(f"{catalog}.{bronze_schema}.bronze_customers")
    products = spark.table(f"{catalog}.{bronze_schema}.bronze_products")
    orders = spark.table(f"{catalog}.{bronze_schema}.bronze_orders")
    order_items = spark.table(f"{catalog}.{bronze_schema}.bronze_order_items")

    (
        customers.select(
            F.col("customer_id").cast("int").alias("customer_id"),
            F.trim("first_name").alias("first_name"),
            F.trim("last_name").alias("last_name"),
            F.lower(F.trim("email")).alias("email"),
            F.to_date("signup_date").alias("signup_date"),
            F.upper(F.trim("country")).alias("country"),
        )
        .writeTo(f"{catalog}.{silver_schema}.spark_silver_customers")
        .using("iceberg")
        .createOrReplace()
    )
    (
        products.select(
            F.col("product_id").cast("int").alias("product_id"),
            F.trim("product_name").alias("product_name"),
            F.trim("category").alias("category"),
            F.col("unit_price").cast("decimal(12,2)").alias("unit_price"),
        )
        .writeTo(f"{catalog}.{silver_schema}.spark_silver_products")
        .using("iceberg")
        .createOrReplace()
    )
    spark_silver_orders = orders.select(
        F.col("order_id").cast("int").alias("order_id"),
        F.col("customer_id").cast("int").alias("customer_id"),
        F.to_timestamp("order_ts").alias("order_ts"),
        F.to_date("order_ts").alias("order_date"),
        F.lower(F.trim("status")).alias("status"),
        F.lower(F.trim("payment_method")).alias("payment_method"),
    )
    print(f"Writing partitioned silver orders table {catalog}.{silver_schema}.spark_silver_orders")
    (
        spark_silver_orders.writeTo(f"{catalog}.{silver_schema}.spark_silver_orders")
        .using("iceberg")
        .partitionedBy(F.months("order_date"))
        .createOrReplace()
    )
    (
        order_items.select(
            F.col("order_item_id").cast("int").alias("order_item_id"),
            F.col("order_id").cast("int").alias("order_id"),
            F.col("product_id").cast("int").alias("product_id"),
            F.col("quantity").cast("int").alias("quantity"),
            F.col("discount_pct").cast("decimal(5,2)").alias("discount_pct"),
        )
        .writeTo(f"{catalog}.{silver_schema}.spark_silver_order_items")
        .using("iceberg")
        .createOrReplace()
    )

    silver_orders = spark.table(f"{catalog}.{silver_schema}.spark_silver_orders")
    silver_items = spark.table(f"{catalog}.{silver_schema}.spark_silver_order_items")
    silver_products = spark.table(f"{catalog}.{silver_schema}.spark_silver_products")
    silver_customers = spark.table(f"{catalog}.{silver_schema}.spark_silver_customers")

    # Silver enrichment: conform + join all four entities into one fact at
    # order-line grain (the augmented/joined silver layer).
    # ORPHAN POLICY (mirrors dbt's silver_sales_enriched): these are INNER joins
    # ON PURPOSE — an order-item survives only with a matching order, product and
    # customer. Orphans are dropped, identically to the dbt path, so both engines
    # share the same row universe. Do NOT switch to outer joins or the marts diverge.
    sales_enriched = (
        silver_items.alias("oi")
        .join(silver_orders.alias("o"), F.col("oi.order_id") == F.col("o.order_id"))
        .join(silver_products.alias("p"), F.col("oi.product_id") == F.col("p.product_id"))
        .join(silver_customers.alias("c"), F.col("o.customer_id") == F.col("c.customer_id"))
        .select(
            F.col("oi.order_item_id"),
            F.col("oi.order_id"),
            F.col("o.order_date"),
            F.col("o.order_ts"),
            F.col("o.status"),
            F.col("o.payment_method"),
            F.col("c.customer_id"),
            F.col("c.country").alias("customer_country"),
            F.col("p.product_id"),
            F.col("p.product_name"),
            F.col("p.category"),
            F.col("oi.quantity"),
            F.col("p.unit_price"),
            F.col("oi.discount_pct"),
            (F.col("oi.quantity") * F.col("p.unit_price")).cast("decimal(14,2)").alias("gross_amount"),
            (F.col("oi.quantity") * F.col("p.unit_price") * (F.lit(1) - F.col("oi.discount_pct"))).cast("decimal(14,2)").alias("net_amount"),
            F.current_timestamp().alias("transformed_at"),
        )
    )
    print(f"Writing enriched silver fact {catalog}.{silver_schema}.spark_silver_sales_enriched")
    (
        sales_enriched.writeTo(f"{catalog}.{silver_schema}.spark_silver_sales_enriched")
        .using("iceberg")
        .tableProperty("write.format.default", "parquet")
        .partitionedBy(F.months("order_date"))
        .createOrReplace()
    )

    enriched = spark.table(f"{catalog}.{silver_schema}.spark_silver_sales_enriched")

    # Gold business aggregate as a PHYSICAL TABLE, built from enriched silver.
    daily_sales = (
        enriched.where(F.col("status") == "completed")
        .groupBy("order_date", "category")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.sum("quantity").alias("units_sold"),
            F.sum("net_amount").cast("decimal(14,2)").alias("net_revenue"),
        )
    )
    print(f"Writing partitioned gold daily sales table {catalog}.{gold_schema}.spark_gold_daily_sales")
    (
        daily_sales.writeTo(f"{catalog}.{gold_schema}.spark_gold_daily_sales")
        .using("iceberg")
        .tableProperty("write.format.default", "parquet")
        .partitionedBy(F.months("order_date"))
        .createOrReplace()
    )

    # Gold category_performance + customer_360 are VIEWS (parity with dbt), NOT
    # physical tables. They are not created here: Spark's CREATE VIEW makes a Hive
    # view that watsonx Presto refuses to read ("Hive views are not supported"),
    # so the views are created THROUGH PRESTO (dbt's exact dialect) by
    # scripts/create_gold_views.py --path spark, which the orchestrator
    # (scripts/submit_spark_application.py) runs after this job FINISHES.
    #
    # Clean any object a prior run left at those two names (an old physical table,
    # or a Hive view from a transitional run) so the Presto CREATE VIEW lands on a
    # free name. Spark can drop BOTH kinds; each drop is guarded because DROP TABLE
    # on a view (or DROP VIEW on a table) raises.
    for _name in (
        f"{catalog}.{gold_schema}.spark_gold_category_performance",
        f"{catalog}.{gold_schema}.spark_gold_customer_360",
    ):
        for _ddl in (f"DROP VIEW IF EXISTS {_name}", f"DROP TABLE IF EXISTS {_name}"):
            try:
                spark.sql(_ddl)
            except Exception as _exc:  # name holds the *other* object type — ignore
                print(f"  (drop ignored) {_ddl}: {str(_exc)[:80]}")
    print(
        "Gold daily_sales TABLE written; category_performance + customer_360 are "
        "created as Presto VIEWS by the orchestrator (scripts/create_gold_views.py)."
    )

    print(f"[OK] medallion build complete — bronze/silver/gold written to catalog {catalog}")
    spark.stop()


if __name__ == "__main__":
    main()
