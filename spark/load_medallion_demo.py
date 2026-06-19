#!/usr/bin/env python3
"""Optional PySpark job that writes the same demo data as Iceberg tables.

Run this inside an environment that already has Spark configured for the
watsonx.data Iceberg catalog and MinIO object storage.

Medallion layers:
- bronze: raw CSVs ingested as-is plus ingestion metadata.
- silver: cleaned/typed dimensions and facts, plus one enriched fact
  (spark_silver_sales_enriched) that joins all four entities.
- gold: business aggregates. spark_gold_daily_sales is a physical table;
  spark_gold_category_performance is layered on top of that gold table.
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

    # Gold category performance, layered on top of the gold daily-sales table.
    gold_daily = spark.table(f"{catalog}.{gold_schema}.spark_gold_daily_sales")
    category_performance = (
        gold_daily.groupBy("category")
        .agg(
            F.sum("order_count").alias("total_orders"),
            F.sum("units_sold").alias("total_units"),
            F.sum("net_revenue").cast("decimal(14,2)").alias("total_revenue"),
        )
        .withColumn(
            # Divide-by-zero guard to match the dbt model's nullif(sum(units_sold),0):
            # a category with 0 total units yields NULL, not an error/inf.
            "avg_revenue_per_unit",
            F.when(F.col("total_units") == 0, None)
            .otherwise(F.col("total_revenue") / F.col("total_units"))
            .cast("decimal(14,2)"),
        )
    )
    print(f"Writing gold category performance table {catalog}.{gold_schema}.spark_gold_category_performance")
    (
        category_performance.writeTo(f"{catalog}.{gold_schema}.spark_gold_category_performance")
        .using("iceberg")
        .tableProperty("write.format.default", "parquet")
        .createOrReplace()
    )

    # Gold customer 360 from the enriched fact, joined back to the customer
    # dimension so customers without orders are still represented.
    metrics = enriched.groupBy("customer_id").agg(
        F.countDistinct(F.when(F.col("status") == "completed", F.col("order_id"))).alias("completed_orders"),
        F.countDistinct(F.when(F.col("status") == "returned", F.col("order_id"))).alias("returned_orders"),
        F.countDistinct(F.when(F.col("status") == "pending", F.col("order_id"))).alias("pending_orders"),
        F.countDistinct(F.when(F.col("status") == "cancelled", F.col("order_id"))).alias("cancelled_orders"),
        F.coalesce(
            F.sum(F.when(F.col("status") == "completed", F.col("net_amount")).otherwise(F.lit(0))),
            F.lit(0),
        ).cast("decimal(14,2)").alias("lifetime_value"),
        F.max(F.when(F.col("status") == "completed", F.col("order_ts"))).alias("last_completed_order_ts"),
        F.max(F.col("order_ts")).alias("last_activity_ts"),
    )
    customer_360 = (
        silver_customers.alias("c")
        .join(metrics.alias("m"), F.col("c.customer_id") == F.col("m.customer_id"), "left")
        .select(
            F.col("c.customer_id"),
            F.col("c.first_name"),
            F.col("c.last_name"),
            F.col("c.email"),
            F.col("c.country"),
            F.col("c.signup_date"),
            F.coalesce(F.col("m.completed_orders"), F.lit(0)).alias("completed_orders"),
            F.coalesce(F.col("m.returned_orders"), F.lit(0)).alias("returned_orders"),
            F.coalesce(F.col("m.pending_orders"), F.lit(0)).alias("pending_orders"),
            F.coalesce(F.col("m.cancelled_orders"), F.lit(0)).alias("cancelled_orders"),
            F.coalesce(F.col("m.lifetime_value"), F.lit(0)).cast("decimal(14,2)").alias("lifetime_value"),
            F.col("m.last_completed_order_ts"),
            F.col("m.last_activity_ts"),
        )
    )
    print(f"Writing gold customer 360 table {catalog}.{gold_schema}.spark_gold_customer_360")
    (
        customer_360.writeTo(f"{catalog}.{gold_schema}.spark_gold_customer_360")
        .using("iceberg")
        .createOrReplace()
    )

    spark.stop()


if __name__ == "__main__":
    main()
