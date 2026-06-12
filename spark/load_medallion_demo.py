#!/usr/bin/env python3
"""Optional PySpark job that writes the same demo data as Iceberg tables.

Run this inside an environment that already has Spark configured for the
watsonx.data Iceberg catalog and MinIO object storage.
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
    base_schema = os.getenv("WXD_SPARK_SCHEMA", "spark_demo")
    bronze_schema = os.getenv("WXD_SPARK_BRONZE_SCHEMA", f"{base_schema}_bronze")
    silver_schema = os.getenv("WXD_SPARK_SILVER_SCHEMA", f"{base_schema}_silver")
    gold_schema = os.getenv("WXD_SPARK_GOLD_SCHEMA", f"{base_schema}_gold")

    spark = SparkSession.builder.appName("watsonxdata-medallion-demo").getOrCreate()

    for schema in [bronze_schema, silver_schema, gold_schema]:
        spark.sql(f"create namespace if not exists {catalog}.{schema}")

    for table in ["customers", "products", "orders", "order_items"]:
        source = ROOT / "seeds" / f"raw_{table}.csv"
        df = (
            spark.read.option("header", "true").csv(str(source))
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_ingested_by", F.lit("spark job"))
            .withColumn("_source_file", F.lit(source.name))
            .withColumn("_ingest_batch_id", F.lit(os.getenv("WXD_SPARK_INGEST_BATCH_ID", "spark_demo_batch")))
        )
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
    (
        orders.select(
            F.col("order_id").cast("int").alias("order_id"),
            F.col("customer_id").cast("int").alias("customer_id"),
            F.to_timestamp("order_ts").alias("order_ts"),
            F.to_date("order_ts").alias("order_date"),
            F.lower(F.trim("status")).alias("status"),
            F.lower(F.trim("payment_method")).alias("payment_method"),
        )
        .writeTo(f"{catalog}.{silver_schema}.spark_silver_orders")
        .using("iceberg")
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
    daily_sales = (
        silver_orders.alias("o")
        .join(silver_items.alias("oi"), F.col("o.order_id") == F.col("oi.order_id"))
        .join(silver_products.alias("p"), F.col("oi.product_id") == F.col("p.product_id"))
        .where(F.col("o.status") == "completed")
        .groupBy(F.col("o.order_date"), F.col("p.category"))
        .agg(
            F.countDistinct("o.order_id").alias("order_count"),
            F.sum("oi.quantity").alias("units_sold"),
            F.sum(F.col("oi.quantity") * F.col("p.unit_price") * (F.lit(1) - F.col("oi.discount_pct"))).cast("decimal(14,2)").alias("net_revenue"),
        )
    )
    daily_sales.writeTo(f"{catalog}.{gold_schema}.spark_gold_daily_sales").using("iceberg").createOrReplace()

    spark.stop()


if __name__ == "__main__":
    main()
