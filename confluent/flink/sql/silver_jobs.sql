-- =============================================================================
--  silver_jobs.sql — two-stage Flink SQL streaming pipeline
--
--  Architecture:
--
--  STAGE 1 — Transform (raw Kafka → silver Kafka)
--    raw_customers     →  [Flink: cast/trim/filter]  →  silver_customers  (Kafka)
--    raw_products      →  [Flink: cast/trim/filter]  →  silver_products   (Kafka)
--    raw_orders        →  [Flink: cast/derive/filter] → silver_orders     (Kafka)
--    raw_order_items   →  [Flink: cast/filter]        → silver_order_items(Kafka)
--
--  STAGE 2 — Sink (silver Kafka → Iceberg in iceberg-bucket)
--    silver_customers    →  iceberg_data.confluent_demo_silver.confluent_silver_customers
--    silver_products     →  iceberg_data.confluent_demo_silver.confluent_silver_products
--    silver_orders       →  iceberg_data.confluent_demo_silver.confluent_silver_orders
--    silver_order_items  →  iceberg_data.confluent_demo_silver.confluent_silver_order_items
--    silver_* (join)     →  iceberg_data.confluent_demo_silver.confluent_silver_sales_enriched
--
--  Schema naming matches the project convention:
--    dbt_demo_silver.*            (dbt approach)
--    spark_demo_silver.spark_silver_*  (Spark approach)
--    confluent_demo_silver.confluent_silver_*  (this file — Kafka/Flink approach)
--
--  Column types are 1:1 identical to dbt_demo_silver (source of truth).
--  The only difference is transformed_at uses TIMESTAMP(6) WITH LOCAL TIME ZONE
--  which Presto/Iceberg surfaces as "timestamp with time zone" — same as dbt.
--
--  Sources of truth for transforms: models/silver/*.sql
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Runtime settings
-- ---------------------------------------------------------------------------
SET 'execution.runtime-mode'              = 'streaming';
SET 'pipeline.name'                       = 'confluent-silver-medallion';
SET 'execution.checkpointing.interval'    = '30000';
SET 'execution.checkpointing.mode'        = 'EXACTLY_ONCE';
SET 'table.exec.source.idle-timeout'      = '10000';

-- ---------------------------------------------------------------------------
-- Iceberg REST catalog — backed by real iceberg-bucket via MinIO HTTP Route
-- ---------------------------------------------------------------------------
CREATE CATALOG local_iceberg WITH (
  'type'                    = 'iceberg',
  'catalog-type'            = 'rest',
  'uri'                     = 'http://confluent-iceberg-rest:8181',
  'warehouse'               = 's3://iceberg-bucket/confluent_demo_silver/',
  's3.endpoint'             = 'http://ibm-lh-minio-route-cpd-instance.apps.watson.ibmas-zocp-techcluster.org',
  's3.path-style-access'    = 'true',
  'client.region'           = 'us-east-1',
  'io-impl'                 = 'org.apache.iceberg.aws.s3.S3FileIO'
);

USE CATALOG local_iceberg;
CREATE DATABASE IF NOT EXISTS confluent_demo_silver;

-- ===========================================================================
-- STAGE 1 SOURCES — raw Kafka topics (all STRING columns, earliest-offset)
-- ===========================================================================

CREATE TEMPORARY TABLE src_customers (
  customer_id  STRING,
  first_name   STRING,
  last_name    STRING,
  email        STRING,
  signup_date  STRING,
  country      STRING
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'raw_customers',
  'properties.bootstrap.servers' = 'confluent-kafka:9092',
  'properties.group.id'          = 'flink-raw-customers',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'json',
  'json.ignore-parse-errors'     = 'true'
);

CREATE TEMPORARY TABLE src_products (
  product_id   STRING,
  product_name STRING,
  category     STRING,
  unit_price   STRING
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'raw_products',
  'properties.bootstrap.servers' = 'confluent-kafka:9092',
  'properties.group.id'          = 'flink-raw-products',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'json',
  'json.ignore-parse-errors'     = 'true'
);

CREATE TEMPORARY TABLE src_orders (
  order_id        STRING,
  customer_id     STRING,
  order_ts        STRING,
  status          STRING,
  payment_method  STRING
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'raw_orders',
  'properties.bootstrap.servers' = 'confluent-kafka:9092',
  'properties.group.id'          = 'flink-raw-orders',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'json',
  'json.ignore-parse-errors'     = 'true'
);

CREATE TEMPORARY TABLE src_order_items (
  order_item_id STRING,
  order_id      STRING,
  product_id    STRING,
  quantity      STRING,
  discount_pct  STRING
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'raw_order_items',
  'properties.bootstrap.servers' = 'confluent-kafka:9092',
  'properties.group.id'          = 'flink-raw-order-items',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'json',
  'json.ignore-parse-errors'     = 'true'
);

-- ===========================================================================
-- STAGE 1 SINKS — silver Kafka topics (typed JSON)
-- Intermediate "Tableflow" topics: clean, typed, consumable by any downstream.
-- DATE/TIMESTAMP transported as ISO-8601 strings over JSON.
-- ===========================================================================

CREATE TEMPORARY TABLE kafka_silver_customers (
  customer_id    INT,
  first_name     STRING,
  last_name      STRING,
  email          STRING,
  signup_date    STRING,
  country        STRING,
  transformed_at STRING
) WITH (
  'connector'                     = 'kafka',
  'topic'                         = 'silver_customers',
  'properties.bootstrap.servers'  = 'confluent-kafka:9092',
  'format'                        = 'json'
);

CREATE TEMPORARY TABLE kafka_silver_products (
  product_id     INT,
  product_name   STRING,
  category       STRING,
  unit_price     DOUBLE,
  transformed_at STRING
) WITH (
  'connector'                     = 'kafka',
  'topic'                         = 'silver_products',
  'properties.bootstrap.servers'  = 'confluent-kafka:9092',
  'format'                        = 'json'
);

CREATE TEMPORARY TABLE kafka_silver_orders (
  order_id        INT,
  customer_id     INT,
  order_ts        STRING,
  order_date      STRING,
  status          STRING,
  payment_method  STRING,
  transformed_at  STRING
) WITH (
  'connector'                     = 'kafka',
  'topic'                         = 'silver_orders',
  'properties.bootstrap.servers'  = 'confluent-kafka:9092',
  'format'                        = 'json'
);

CREATE TEMPORARY TABLE kafka_silver_order_items (
  order_item_id  INT,
  order_id       INT,
  product_id     INT,
  quantity       INT,
  discount_pct   DOUBLE,
  transformed_at STRING
) WITH (
  'connector'                     = 'kafka',
  'topic'                         = 'silver_order_items',
  'properties.bootstrap.servers'  = 'confluent-kafka:9092',
  'format'                        = 'json'
);

-- ===========================================================================
-- STAGE 2 SINKS — Iceberg tables (persistent watsonx.data layer)
--
--  Schema: confluent_demo_silver  (matches dbt_demo_silver / spark_demo_silver)
--  Table names: confluent_silver_*  (matches spark_silver_* prefix convention)
--
--  Column types exactly mirror dbt_demo_silver (source of truth):
--    integer                  ← INT in Flink
--    varchar                  ← STRING in Flink
--    date                     ← DATE in Flink
--    decimal(p,s)             ← DECIMAL(p,s) in Flink
--    timestamp                ← TIMESTAMP(6) in Flink  [silver_orders.order_ts]
--    timestamp with time zone ← TIMESTAMP(6) WITH LOCAL TIME ZONE in Flink
-- ===========================================================================

CREATE TABLE IF NOT EXISTS local_iceberg.confluent_demo_silver.confluent_silver_customers (
  customer_id    INT,
  first_name     STRING,
  last_name      STRING,
  email          STRING,
  signup_date    DATE,
  country        STRING,
  transformed_at TIMESTAMP(6) WITH LOCAL TIME ZONE
) WITH (
  'format-version'        = '2',
  'write.format.default'  = 'parquet'
);

CREATE TABLE IF NOT EXISTS local_iceberg.confluent_demo_silver.confluent_silver_products (
  product_id     INT,
  product_name   STRING,
  category       STRING,
  unit_price     DECIMAL(12, 2),
  transformed_at TIMESTAMP(6) WITH LOCAL TIME ZONE
) WITH (
  'format-version'        = '2',
  'write.format.default'  = 'parquet'
);

-- silver_orders.order_ts: dbt casts to plain timestamp (no tz) → TIMESTAMP(6) here
-- silver_orders.transformed_at: timestamp with time zone → TIMESTAMP(6) WITH LOCAL TIME ZONE
CREATE TABLE IF NOT EXISTS local_iceberg.confluent_demo_silver.confluent_silver_orders (
  order_id        INT,
  customer_id     INT,
  order_ts        TIMESTAMP(6),
  order_date      DATE,
  status          STRING,
  payment_method  STRING,
  transformed_at  TIMESTAMP(6) WITH LOCAL TIME ZONE
) WITH (
  'format-version'        = '2',
  'write.format.default'  = 'parquet'
);

CREATE TABLE IF NOT EXISTS local_iceberg.confluent_demo_silver.confluent_silver_order_items (
  order_item_id  INT,
  order_id       INT,
  product_id     INT,
  quantity       INT,
  discount_pct   DECIMAL(5, 2),
  transformed_at TIMESTAMP(6) WITH LOCAL TIME ZONE
) WITH (
  'format-version'        = '2',
  'write.format.default'  = 'parquet'
);

-- silver_sales_enriched.order_ts: plain timestamp (from silver_orders.order_ts, no tz)
CREATE TABLE IF NOT EXISTS local_iceberg.confluent_demo_silver.confluent_silver_sales_enriched (
  order_item_id    INT,
  order_id         INT,
  order_date       DATE,
  order_ts         TIMESTAMP(6),
  status           STRING,
  payment_method   STRING,
  customer_id      INT,
  customer_country STRING,
  product_id       INT,
  product_name     STRING,
  category         STRING,
  quantity         INT,
  unit_price       DECIMAL(12, 2),
  discount_pct     DECIMAL(5, 2),
  gross_amount     DECIMAL(14, 2),
  net_amount       DECIMAL(14, 2),
  transformed_at   TIMESTAMP(6) WITH LOCAL TIME ZONE
) WITH (
  'format-version'        = '2',
  'write.format.default'  = 'parquet'
);

-- ===========================================================================
-- STAGE 1 JOBS — raw Kafka → silver Kafka (transform + filter)
-- Mirrors dbt silver SQL exactly: same casts, filters, normalisation.
-- ===========================================================================

-- Job 1: raw_customers → silver_customers (Kafka)
-- Mirrors: models/silver/silver_customers.sql
INSERT INTO kafka_silver_customers
SELECT
  CAST(customer_id AS INT),
  TRIM(first_name),
  TRIM(last_name),
  LOWER(TRIM(email)),
  CAST(CAST(signup_date AS DATE) AS STRING),
  UPPER(TRIM(country)),
  CAST(CURRENT_TIMESTAMP AS STRING)
FROM src_customers
WHERE email IS NOT NULL AND TRIM(email) <> '';

-- Job 2: raw_products → silver_products (Kafka)
-- Mirrors: models/silver/silver_products.sql
INSERT INTO kafka_silver_products
SELECT
  CAST(product_id AS INT),
  TRIM(product_name),
  TRIM(category),
  CAST(unit_price AS DOUBLE),
  CAST(CURRENT_TIMESTAMP AS STRING)
FROM src_products
WHERE product_id IS NOT NULL AND TRIM(product_id) <> '';

-- Job 3: raw_orders → silver_orders (Kafka)
-- Mirrors: models/silver/silver_orders.sql
INSERT INTO kafka_silver_orders
SELECT
  CAST(order_id AS INT),
  CAST(customer_id AS INT),
  CAST(CAST(order_ts AS TIMESTAMP(6)) AS STRING),
  CAST(CAST(CAST(order_ts AS TIMESTAMP(6)) AS DATE) AS STRING),
  LOWER(TRIM(status)),
  LOWER(TRIM(payment_method)),
  CAST(CURRENT_TIMESTAMP AS STRING)
FROM src_orders
WHERE order_id IS NOT NULL AND TRIM(order_id) <> '';

-- Job 4: raw_order_items → silver_order_items (Kafka)
-- Mirrors: models/silver/silver_order_items.sql
INSERT INTO kafka_silver_order_items
SELECT
  CAST(order_item_id AS INT),
  CAST(order_id AS INT),
  CAST(product_id AS INT),
  CAST(quantity AS INT),
  CAST(discount_pct AS DOUBLE),
  CAST(CURRENT_TIMESTAMP AS STRING)
FROM src_order_items
WHERE quantity IS NOT NULL AND CAST(quantity AS INT) > 0;

-- ===========================================================================
-- STAGE 2 SOURCES — silver Kafka topics (read back as typed streams)
-- ===========================================================================

CREATE TEMPORARY TABLE silver_src_customers (
  customer_id    INT,
  first_name     STRING,
  last_name      STRING,
  email          STRING,
  signup_date    STRING,
  country        STRING,
  transformed_at STRING
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'silver_customers',
  'properties.bootstrap.servers' = 'confluent-kafka:9092',
  'properties.group.id'          = 'flink-iceberg-customers',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'json',
  'json.ignore-parse-errors'     = 'true'
);

CREATE TEMPORARY TABLE silver_src_products (
  product_id     INT,
  product_name   STRING,
  category       STRING,
  unit_price     DOUBLE,
  transformed_at STRING
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'silver_products',
  'properties.bootstrap.servers' = 'confluent-kafka:9092',
  'properties.group.id'          = 'flink-iceberg-products',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'json',
  'json.ignore-parse-errors'     = 'true'
);

CREATE TEMPORARY TABLE silver_src_orders (
  order_id        INT,
  customer_id     INT,
  order_ts        STRING,
  order_date      STRING,
  status          STRING,
  payment_method  STRING,
  transformed_at  STRING
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'silver_orders',
  'properties.bootstrap.servers' = 'confluent-kafka:9092',
  'properties.group.id'          = 'flink-iceberg-orders',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'json',
  'json.ignore-parse-errors'     = 'true'
);

CREATE TEMPORARY TABLE silver_src_order_items (
  order_item_id  INT,
  order_id       INT,
  product_id     INT,
  quantity       INT,
  discount_pct   DOUBLE,
  transformed_at STRING
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'silver_order_items',
  'properties.bootstrap.servers' = 'confluent-kafka:9092',
  'properties.group.id'          = 'flink-iceberg-order-items',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'json',
  'json.ignore-parse-errors'     = 'true'
);

-- ===========================================================================
-- STAGE 2 JOBS — silver Kafka → Iceberg
-- ===========================================================================

-- Job 5: confluent_silver_customers
-- transformed_at: STRING → TIMESTAMP(6) WITH LOCAL TIME ZONE (= dbt "timestamp with time zone")
INSERT INTO local_iceberg.confluent_demo_silver.confluent_silver_customers
SELECT
  customer_id,
  first_name,
  last_name,
  email,
  CAST(signup_date    AS DATE),
  country,
  CAST(transformed_at AS TIMESTAMP(6) WITH LOCAL TIME ZONE)
FROM silver_src_customers;

-- Job 6: confluent_silver_products
INSERT INTO local_iceberg.confluent_demo_silver.confluent_silver_products
SELECT
  product_id,
  product_name,
  category,
  CAST(unit_price     AS DECIMAL(12, 2)),
  CAST(transformed_at AS TIMESTAMP(6) WITH LOCAL TIME ZONE)
FROM silver_src_products;

-- Job 7: confluent_silver_orders
-- order_ts: STRING → TIMESTAMP(6)  (plain timestamp, no tz — matches dbt)
-- transformed_at: STRING → TIMESTAMP(6) WITH LOCAL TIME ZONE
INSERT INTO local_iceberg.confluent_demo_silver.confluent_silver_orders
SELECT
  order_id,
  customer_id,
  CAST(order_ts       AS TIMESTAMP(6)),
  CAST(order_date     AS DATE),
  status,
  payment_method,
  CAST(transformed_at AS TIMESTAMP(6) WITH LOCAL TIME ZONE)
FROM silver_src_orders;

-- Job 8: confluent_silver_order_items
INSERT INTO local_iceberg.confluent_demo_silver.confluent_silver_order_items
SELECT
  order_item_id,
  order_id,
  product_id,
  quantity,
  CAST(discount_pct   AS DECIMAL(5, 2)),
  CAST(transformed_at AS TIMESTAMP(6) WITH LOCAL TIME ZONE)
FROM silver_src_order_items;

-- Job 9: confluent_silver_sales_enriched
-- Stream-stream inner join across all 4 silver Kafka topics.
-- order_ts: plain TIMESTAMP(6) (from orders stream, no tz — matches dbt)
-- transformed_at: TIMESTAMP(6) WITH LOCAL TIME ZONE
INSERT INTO local_iceberg.confluent_demo_silver.confluent_silver_sales_enriched
SELECT
  oi.order_item_id,
  oi.order_id,
  CAST(o.order_date    AS DATE)                                           AS order_date,
  CAST(o.order_ts      AS TIMESTAMP(6))                                   AS order_ts,
  o.status,
  o.payment_method,
  c.customer_id,
  c.country                                                               AS customer_country,
  p.product_id,
  p.product_name,
  p.category,
  oi.quantity,
  CAST(p.unit_price    AS DECIMAL(12, 2))                                 AS unit_price,
  CAST(oi.discount_pct AS DECIMAL(5, 2))                                  AS discount_pct,
  CAST(oi.quantity * p.unit_price AS DECIMAL(14, 2))                      AS gross_amount,
  CAST(oi.quantity * p.unit_price * (1 - oi.discount_pct)
       AS DECIMAL(14, 2))                                                 AS net_amount,
  CURRENT_TIMESTAMP                                                        AS transformed_at
FROM      silver_src_order_items  AS oi
JOIN      silver_src_orders       AS o  ON oi.order_id   = o.order_id
JOIN      silver_src_products     AS p  ON oi.product_id = p.product_id
JOIN      silver_src_customers    AS c  ON o.customer_id = c.customer_id;
