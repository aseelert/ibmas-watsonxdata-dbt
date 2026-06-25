-- =============================================================================
--  silver_jobs.sql — Flink SQL streaming jobs for the confluent_silver layer
--
--  Mirrors the dbt silver layer exactly:
--    models/silver/silver_customers.sql
--    models/silver/silver_products.sql
--    models/silver/silver_orders.sql
--    models/silver/silver_order_items.sql
--    models/silver/silver_sales_enriched.sql
--
--  Catalog backend : confluent-iceberg-rest:8181 (Iceberg REST, SQLite)
--  Data backend    : s3://iceberg-bucket/confluent_silver/ (real watsonx.data MinIO
--                    via the OpenShift Route set in WXD_OBJECT_STORE_ENDPOINT)
--  Checkpointing   : every 30 s — ensures Iceberg commits land quickly so
--                    confluent-prep can discover metadata_location for register_table
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Runtime settings
-- ---------------------------------------------------------------------------
SET 'execution.runtime-mode' = 'streaming';
SET 'pipeline.name' = 'confluent-silver-medallion';
SET 'execution.checkpointing.interval' = '30000';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';

-- ---------------------------------------------------------------------------
-- Catalog — Iceberg REST backed by real iceberg-bucket
-- ---------------------------------------------------------------------------
CREATE CATALOG local_iceberg WITH (
  'type'         = 'iceberg',
  'catalog-type' = 'rest',
  'uri'          = 'http://confluent-iceberg-rest:8181',
  'warehouse'    = 's3://iceberg-bucket/confluent_silver/'
);

USE CATALOG local_iceberg;

CREATE DATABASE IF NOT EXISTS confluent_silver;

USE confluent_silver;

-- ---------------------------------------------------------------------------
-- Source: Kafka temporary tables (all columns STRING — raw JSON from CSV ingest)
-- ---------------------------------------------------------------------------

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
  'properties.group.id'          = 'flink-silver-customers',
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
  'properties.group.id'          = 'flink-silver-products',
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
  'properties.group.id'          = 'flink-silver-orders',
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
  'properties.group.id'          = 'flink-silver-order-items',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'json',
  'json.ignore-parse-errors'     = 'true'
);

-- ---------------------------------------------------------------------------
-- Sink: Iceberg tables in local_iceberg.confluent_silver
-- Partitioning and column layout exactly mirrors dbt silver models
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS local_iceberg.confluent_silver.silver_customers (
  customer_id    INT,
  first_name     STRING,
  last_name      STRING,
  email          STRING,
  signup_date    DATE,
  country        STRING,
  transformed_at TIMESTAMP(6)
) WITH (
  'format-version' = '2',
  'write.format.default' = 'parquet'
);

CREATE TABLE IF NOT EXISTS local_iceberg.confluent_silver.silver_products (
  product_id     INT,
  product_name   STRING,
  category       STRING,
  unit_price     DECIMAL(12, 2),
  transformed_at TIMESTAMP(6)
) WITH (
  'format-version' = '2',
  'write.format.default' = 'parquet'
);

CREATE TABLE IF NOT EXISTS local_iceberg.confluent_silver.silver_orders (
  order_id        INT,
  customer_id     INT,
  order_ts        TIMESTAMP(6),
  order_date      DATE,
  status          STRING,
  payment_method  STRING,
  transformed_at  TIMESTAMP(6)
) PARTITIONED BY (months(order_date))
WITH (
  'format-version' = '2',
  'write.format.default' = 'parquet'
);

CREATE TABLE IF NOT EXISTS local_iceberg.confluent_silver.silver_order_items (
  order_item_id  INT,
  order_id       INT,
  product_id     INT,
  quantity       INT,
  discount_pct   DECIMAL(5, 2),
  transformed_at TIMESTAMP(6)
) WITH (
  'format-version' = '2',
  'write.format.default' = 'parquet'
);

CREATE TABLE IF NOT EXISTS local_iceberg.confluent_silver.silver_sales_enriched (
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
  transformed_at   TIMESTAMP(6)
) PARTITIONED BY (months(order_date))
WITH (
  'format-version' = '2',
  'write.format.default' = 'parquet'
);

-- ---------------------------------------------------------------------------
-- silver_customers
-- Mirrors: models/silver/silver_customers.sql
--   CAST(customer_id AS INT), TRIM, LOWER(TRIM(email)), UPPER(TRIM(country)),
--   CAST(signup_date AS DATE), WHERE email IS NOT NULL
-- ---------------------------------------------------------------------------
INSERT INTO local_iceberg.confluent_silver.silver_customers
SELECT
  CAST(customer_id AS INT)                         AS customer_id,
  TRIM(first_name)                                 AS first_name,
  TRIM(last_name)                                  AS last_name,
  LOWER(TRIM(email))                               AS email,
  CAST(signup_date AS DATE)                        AS signup_date,
  UPPER(TRIM(country))                             AS country,
  CURRENT_TIMESTAMP                                AS transformed_at
FROM src_customers
WHERE email IS NOT NULL AND TRIM(email) <> '';

-- ---------------------------------------------------------------------------
-- silver_products
-- Mirrors: models/silver/silver_products.sql
--   CAST(product_id AS INT), TRIM(product_name), TRIM(category),
--   CAST(unit_price AS DECIMAL(12,2)), WHERE product_id IS NOT NULL
-- ---------------------------------------------------------------------------
INSERT INTO local_iceberg.confluent_silver.silver_products
SELECT
  CAST(product_id AS INT)                          AS product_id,
  TRIM(product_name)                               AS product_name,
  TRIM(category)                                   AS category,
  CAST(unit_price AS DECIMAL(12, 2))               AS unit_price,
  CURRENT_TIMESTAMP                                AS transformed_at
FROM src_products
WHERE product_id IS NOT NULL AND TRIM(product_id) <> '';

-- ---------------------------------------------------------------------------
-- silver_orders
-- Mirrors: models/silver/silver_orders.sql
--   CAST(order_id AS INT), CAST(order_ts AS TIMESTAMP),
--   CAST(CAST(order_ts AS TIMESTAMP) AS DATE) AS order_date,
--   LOWER(TRIM(status)), LOWER(TRIM(payment_method)), WHERE order_id IS NOT NULL
-- ---------------------------------------------------------------------------
INSERT INTO local_iceberg.confluent_silver.silver_orders
SELECT
  CAST(order_id AS INT)                            AS order_id,
  CAST(customer_id AS INT)                         AS customer_id,
  CAST(order_ts AS TIMESTAMP(6))                   AS order_ts,
  CAST(CAST(order_ts AS TIMESTAMP(6)) AS DATE)     AS order_date,
  LOWER(TRIM(status))                              AS status,
  LOWER(TRIM(payment_method))                      AS payment_method,
  CURRENT_TIMESTAMP                                AS transformed_at
FROM src_orders
WHERE order_id IS NOT NULL AND TRIM(order_id) <> '';

-- ---------------------------------------------------------------------------
-- silver_order_items
-- Mirrors: models/silver/silver_order_items.sql
--   CAST(order_item_id AS INT), CAST(order_id AS INT), CAST(product_id AS INT),
--   CAST(quantity AS INT), CAST(discount_pct AS DECIMAL(5,2)), WHERE quantity > 0
-- ---------------------------------------------------------------------------
INSERT INTO local_iceberg.confluent_silver.silver_order_items
SELECT
  CAST(order_item_id AS INT)                       AS order_item_id,
  CAST(order_id AS INT)                            AS order_id,
  CAST(product_id AS INT)                          AS product_id,
  CAST(quantity AS INT)                            AS quantity,
  CAST(discount_pct AS DECIMAL(5, 2))              AS discount_pct,
  CURRENT_TIMESTAMP                                AS transformed_at
FROM src_order_items
WHERE CAST(quantity AS INT) > 0;

-- ---------------------------------------------------------------------------
-- silver_sales_enriched
-- Mirrors: models/silver/silver_sales_enriched.sql
--
-- Streaming temporal join pattern:
--   silver_order_items is the driving "fact" stream.
--   silver_orders, silver_products, silver_customers are treated as versioned
--   lookup tables via FOR SYSTEM_TIME AS OF proctime — the standard Flink
--   pattern for enriching a stream with slowly-changing dimension data.
--
-- gross_amount = quantity * unit_price
-- net_amount   = quantity * unit_price * (1 - discount_pct)
-- ---------------------------------------------------------------------------
INSERT INTO local_iceberg.confluent_silver.silver_sales_enriched
SELECT
  oi.order_item_id,
  oi.order_id,
  o.order_date,
  o.order_ts,
  o.status,
  o.payment_method,
  c.customer_id,
  c.country                                                            AS customer_country,
  p.product_id,
  p.product_name,
  p.category,
  oi.quantity,
  p.unit_price,
  oi.discount_pct,
  CAST(oi.quantity * p.unit_price AS DECIMAL(14, 2))                  AS gross_amount,
  CAST(oi.quantity * p.unit_price * (1 - oi.discount_pct)
       AS DECIMAL(14, 2))                                             AS net_amount,
  CURRENT_TIMESTAMP                                                    AS transformed_at
FROM local_iceberg.confluent_silver.silver_order_items AS oi
JOIN local_iceberg.confluent_silver.silver_orders      AS o
  ON oi.order_id = o.order_id
JOIN local_iceberg.confluent_silver.silver_products    AS p
  ON oi.product_id = p.product_id
JOIN local_iceberg.confluent_silver.silver_customers   AS c
  ON o.customer_id = c.customer_id;
