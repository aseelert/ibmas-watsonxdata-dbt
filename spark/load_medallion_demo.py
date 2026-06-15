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
        .partitionedBy("order_date")
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
    print(f"Writing partitioned gold daily sales table {catalog}.{gold_schema}.spark_gold_daily_sales")
    (
        daily_sales.writeTo(f"{catalog}.{gold_schema}.spark_gold_daily_sales")
        .using("iceberg")
        .partitionedBy("order_date")
        .createOrReplace()
    )

    customer_360 = (
        silver_customers.alias("c")
        .join(silver_orders.alias("o"), F.col("c.customer_id") == F.col("o.customer_id"), "left")
        .join(silver_items.alias("oi"), F.col("o.order_id") == F.col("oi.order_id"), "left")
        .join(silver_products.alias("p"), F.col("oi.product_id") == F.col("p.product_id"), "left")
        .groupBy(
            F.col("c.customer_id"),
            F.col("c.first_name"),
            F.col("c.last_name"),
            F.col("c.email"),
            F.col("c.country"),
            F.col("c.signup_date"),
        )
        .agg(
            F.countDistinct(F.when(F.col("o.status") == "completed", F.col("o.order_id"))).alias("completed_orders"),
            F.countDistinct(F.when(F.col("o.status") == "returned", F.col("o.order_id"))).alias("returned_orders"),
            F.countDistinct(F.when(F.col("o.status") == "pending", F.col("o.order_id"))).alias("pending_orders"),
            F.countDistinct(F.when(F.col("o.status") == "cancelled", F.col("o.order_id"))).alias("cancelled_orders"),
            F.coalesce(
                F.sum(
                    F.when(
                        F.col("o.status") == "completed",
                        F.col("oi.quantity") * F.col("p.unit_price") * (F.lit(1) - F.col("oi.discount_pct")),
                    ).otherwise(F.lit(0))
                ),
                F.lit(0),
            ).cast("decimal(14,2)").alias("lifetime_value"),
            F.max(F.when(F.col("o.status") == "completed", F.col("o.order_ts"))).alias("last_completed_order_ts"),
            F.max(F.col("o.order_ts")).alias("last_activity_ts"),
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
