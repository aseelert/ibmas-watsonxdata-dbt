-- =============================================================================
--  silver_jobs.sql — two-stage Flink SQL streaming pipeline (Avro + Iceberg)
-- -----------------------------------------------------------------------------
--  Location  : confluent/flink/sql/silver_jobs.sql
--  Repository: https://github.com/aseelert/ibmas-watsonxdata-dbt
--  Project   : watsonx.data · dbt · Spark · Confluent medallion demo
--  Author    : Alexander Seelert — IBM Customer Success Engineer
--  Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.
--
--  Changelog :
--    v1.0 (2026-06-26) — Initial version. Industry-standard, idempotent silver
--      pipeline. (a) ALL Kafka tables use Confluent Avro (format=avro-confluent)
--      governed by Schema Registry. (b) No cluster-specific literals: the S3
--      endpoint, Schema Registry URL and silver schema name are dollar-brace
--      placeholders substituted by submit-flink.sh at submit. (c) The Iceberg sinks are
--      idempotent — PRIMARY KEY + write.upsert.enabled means a re-run upserts by
--      key instead of appending duplicates. (d) price/discount are cast to
--      DECIMAL already in the silver stage (not only at the sink) so revenue ties
--      to the cent and matches dbt exactly.
--    v1.1 (2026-06-27) — Unique pipeline.name per INSERT job (9 names, all
--      prefixed "confluent-silver-"). The old single global SET was removed;
--      each job now sets its own name immediately before the INSERT so the
--      submit-flink.sh cancel-before-submit guard can target them individually.
--    v1.2 (2026-06-26) — Durable s3a:// fix. The catalog warehouse now uses the
--      's3a://' scheme (was 's3://') so the data-file/manifest paths recorded in
--      the Iceberg metadata carry 's3a://'. The watsonx.data Spark engine
--      configures only the 's3a' Hadoop filesystem, so the gold Spark job now
--      reads the Flink-written silver tables NATIVELY — no per-job s3->s3a bridge
--      needed. Iceberg's S3FileIO normalises s3/s3a/s3n, so it still performs the
--      actual object I/O unchanged; only the recorded scheme prefix differs.
--      docker-compose.yml CATALOG_WAREHOUSE was changed to match in lockstep.
-- -----------------------------------------------------------------------------
--
--  Architecture (for an 18-year-old learner):
--
--  STAGE 1 — Transform (raw Kafka → silver Kafka)
--    raw_customers     →  [Flink: cast/trim/filter]  →  silver_customers   (Kafka, Avro)
--    raw_products      →  [Flink: cast/trim/filter]  →  silver_products    (Kafka, Avro)
--    raw_orders        →  [Flink: cast/derive/filter] → silver_orders      (Kafka, Avro)
--    raw_order_items   →  [Flink: cast/filter]        → silver_order_items (Kafka, Avro)
--
--  STAGE 2 — Sink (silver Kafka → Iceberg in iceberg-bucket, UPSERT by key)
--    silver_customers    →  ${CONFLUENT_SILVER_SCHEMA}.confluent_silver_customers
--    silver_products     →  ${CONFLUENT_SILVER_SCHEMA}.confluent_silver_products
--    silver_orders       →  ${CONFLUENT_SILVER_SCHEMA}.confluent_silver_orders
--    silver_order_items  →  ${CONFLUENT_SILVER_SCHEMA}.confluent_silver_order_items
--    silver_* (join)     →  ${CONFLUENT_SILVER_SCHEMA}.confluent_silver_sales_enriched
--
--  Naming matches the project convention (see confluent/NAMING.md):
--    dbt_demo_silver.*                         (dbt approach)
--    spark_demo_silver.spark_silver_*          (Spark approach)
--    confluent_demo_silver.confluent_silver_*  (this file — Kafka/Flink approach)
--
--  Column types are 1:1 identical to dbt_demo_silver (the source of truth).
--  Sources of truth for the transforms: models/silver/*.sql
--
--  Placeholders substituted at submit time by confluent/scripts/submit-flink.sh:
--    ${WXD_OBJECT_STORE_ENDPOINT}  — MinIO/S3 endpoint (OpenShift Route URL)
--    ${SCHEMA_REGISTRY_URL}        — in-container Schema Registry URL
--    ${CONFLUENT_SILVER_SCHEMA}    — Iceberg schema/database for the silver tables
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Runtime settings
-- ---------------------------------------------------------------------------
SET 'execution.runtime-mode'              = 'streaming';
SET 'execution.checkpointing.interval'    = '30000';
SET 'execution.checkpointing.mode'        = 'EXACTLY_ONCE';
SET 'table.exec.source.idle-timeout'      = '10000';
-- Submit jobs to the existing standalone session cluster by its container name.
-- Without these the SQL Gateway resolves the JobManager REST endpoint to the
-- default 0.0.0.0:8081 and fails with "Connection refused: /0.0.0.0:8081".
SET 'execution.target'                    = 'remote';
SET 'rest.address'                        = 'confluent-flink-jobmanager';
SET 'rest.port'                           = '8081';

-- ---------------------------------------------------------------------------
-- Iceberg REST catalog — backed by the real iceberg-bucket via the MinIO Route.
-- NOTHING here is hardcoded: the S3 endpoint and schema/warehouse name are
-- placeholders substituted from .env at submit time (see header).
-- ---------------------------------------------------------------------------
CREATE CATALOG local_iceberg WITH (
  'type'                    = 'iceberg',
  'catalog-type'            = 'rest',
  'uri'                     = 'http://confluent-iceberg-rest:8181',
  'warehouse'               = 's3a://iceberg-bucket/${CONFLUENT_SILVER_SCHEMA}/',
  's3.endpoint'             = '${WXD_OBJECT_STORE_ENDPOINT}',
  's3.path-style-access'    = 'true',
  'client.region'           = 'us-east-1',
  'io-impl'                 = 'org.apache.iceberg.aws.s3.S3FileIO'
);

USE CATALOG local_iceberg;
CREATE DATABASE IF NOT EXISTS ${CONFLUENT_SILVER_SCHEMA};

-- ===========================================================================
-- STAGE 1 SOURCES — raw Kafka topics, decoded as Confluent Avro.
-- Column types match confluent/schemas/raw_*.avsc (ids INT, prices DOUBLE,
-- temporal/text values STRING). The Schema Registry holds the contract; Flink
-- looks up each message's schema by id and decodes it into these columns.
-- ===========================================================================

CREATE TEMPORARY TABLE src_customers (
  customer_id  INT NOT NULL,
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
  'format'                       = 'avro-confluent',
  'avro-confluent.url'           = '${SCHEMA_REGISTRY_URL}'
);

CREATE TEMPORARY TABLE src_products (
  product_id   INT NOT NULL,
  product_name STRING,
  category     STRING,
  unit_price   DOUBLE
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'raw_products',
  'properties.bootstrap.servers' = 'confluent-kafka:9092',
  'properties.group.id'          = 'flink-raw-products',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'avro-confluent',
  'avro-confluent.url'           = '${SCHEMA_REGISTRY_URL}'
);

CREATE TEMPORARY TABLE src_orders (
  order_id        INT NOT NULL,
  customer_id     INT NOT NULL,
  order_ts        STRING,
  status          STRING,
  payment_method  STRING
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'raw_orders',
  'properties.bootstrap.servers' = 'confluent-kafka:9092',
  'properties.group.id'          = 'flink-raw-orders',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'avro-confluent',
  'avro-confluent.url'           = '${SCHEMA_REGISTRY_URL}'
);

CREATE TEMPORARY TABLE src_order_items (
  order_item_id INT NOT NULL,
  order_id      INT NOT NULL,
  product_id    INT NOT NULL,
  quantity      INT NOT NULL,
  discount_pct  DOUBLE
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'raw_order_items',
  'properties.bootstrap.servers' = 'confluent-kafka:9092',
  'properties.group.id'          = 'flink-raw-order-items',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'avro-confluent',
  'avro-confluent.url'           = '${SCHEMA_REGISTRY_URL}'
);

-- ===========================================================================
-- STAGE 1 SINKS — silver Kafka topics, written as Confluent Avro.
-- These intermediate "Tableflow" topics are clean, typed, and consumable by
-- ANY downstream. Flink auto-registers the subject "<topic>-value".
-- DATE/TIMESTAMP travel as ISO-8601 strings; MONEY (unit_price, discount_pct)
-- is already DECIMAL here so the cent-exact math happens in the silver stage,
-- not only at the final sink.
-- ===========================================================================

CREATE TEMPORARY TABLE kafka_silver_customers (
  customer_id    INT NOT NULL,
  first_name     STRING,
  last_name      STRING,
  email          STRING,
  signup_date    STRING,
  country        STRING,
  transformed_at STRING
) WITH (
  'connector'           = 'kafka',
  'topic'               = 'silver_customers',
  'properties.bootstrap.servers' = 'confluent-kafka:9092',
  'format'              = 'avro-confluent',
  'avro-confluent.url'  = '${SCHEMA_REGISTRY_URL}'
);

CREATE TEMPORARY TABLE kafka_silver_products (
  product_id     INT NOT NULL,
  product_name   STRING,
  category       STRING,
  unit_price     DECIMAL(12, 2),
  transformed_at STRING
) WITH (
  'connector'           = 'kafka',
  'topic'               = 'silver_products',
  'properties.bootstrap.servers' = 'confluent-kafka:9092',
  'format'              = 'avro-confluent',
  'avro-confluent.url'  = '${SCHEMA_REGISTRY_URL}'
);

CREATE TEMPORARY TABLE kafka_silver_orders (
  order_id        INT NOT NULL,
  customer_id     INT NOT NULL,
  order_ts        STRING,
  order_date      STRING,
  status          STRING,
  payment_method  STRING,
  transformed_at  STRING
) WITH (
  'connector'           = 'kafka',
  'topic'               = 'silver_orders',
  'properties.bootstrap.servers' = 'confluent-kafka:9092',
  'format'              = 'avro-confluent',
  'avro-confluent.url'  = '${SCHEMA_REGISTRY_URL}'
);

CREATE TEMPORARY TABLE kafka_silver_order_items (
  order_item_id  INT NOT NULL,
  order_id       INT NOT NULL,
  product_id     INT NOT NULL,
  quantity       INT NOT NULL,
  discount_pct   DECIMAL(5, 2),
  transformed_at STRING
) WITH (
  'connector'           = 'kafka',
  'topic'               = 'silver_order_items',
  'properties.bootstrap.servers' = 'confluent-kafka:9092',
  'format'              = 'avro-confluent',
  'avro-confluent.url'  = '${SCHEMA_REGISTRY_URL}'
);

-- ===========================================================================
-- STAGE 2 SINKS — Iceberg tables (persistent watsonx.data layer)
--
--  Schema: ${CONFLUENT_SILVER_SCHEMA}  (matches dbt_demo_silver / spark_demo_silver)
--  Table names: confluent_silver_*     (matches the spark_silver_* convention)
--
--  IDEMPOTENT BY DESIGN: each table has a PRIMARY KEY and write.upsert.enabled.
--  In upsert mode Flink writes an equality-delete + insert keyed by that PK, so
--  re-submitting this job UPSERTS the same rows instead of appending duplicates.
--  (Iceberg v2 tables only — that is why format-version = '2'.)
--
--  Column types exactly mirror dbt_demo_silver (source of truth):
--    integer                  ← INT in Flink
--    varchar                  ← STRING in Flink
--    date                     ← DATE in Flink
--    decimal(p,s)             ← DECIMAL(p,s) in Flink
--    timestamp                ← TIMESTAMP(6) in Flink  [order_ts, no tz]
--    timestamp with time zone ← TIMESTAMP(6) WITH LOCAL TIME ZONE in Flink
-- ===========================================================================

CREATE TABLE IF NOT EXISTS local_iceberg.${CONFLUENT_SILVER_SCHEMA}.confluent_silver_customers (
  customer_id    INT NOT NULL,
  first_name     STRING,
  last_name      STRING,
  email          STRING,
  signup_date    DATE,
  country        STRING,
  transformed_at TIMESTAMP(6) WITH LOCAL TIME ZONE,
  PRIMARY KEY (customer_id) NOT ENFORCED
) WITH (
  'format-version'        = '2',
  'write.format.default'  = 'parquet',
  'write.upsert.enabled'  = 'true'
);

CREATE TABLE IF NOT EXISTS local_iceberg.${CONFLUENT_SILVER_SCHEMA}.confluent_silver_products (
  product_id     INT NOT NULL,
  product_name   STRING,
  category       STRING,
  unit_price     DECIMAL(12, 2),
  transformed_at TIMESTAMP(6) WITH LOCAL TIME ZONE,
  PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
  'format-version'        = '2',
  'write.format.default'  = 'parquet',
  'write.upsert.enabled'  = 'true'
);

-- order_ts: dbt casts to a plain timestamp (no tz) → TIMESTAMP(6) here
-- transformed_at: timestamp with time zone → TIMESTAMP(6) WITH LOCAL TIME ZONE
CREATE TABLE IF NOT EXISTS local_iceberg.${CONFLUENT_SILVER_SCHEMA}.confluent_silver_orders (
  order_id        INT NOT NULL,
  customer_id     INT NOT NULL,
  order_ts        TIMESTAMP(6),
  order_date      DATE,
  status          STRING,
  payment_method  STRING,
  transformed_at  TIMESTAMP(6) WITH LOCAL TIME ZONE,
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'format-version'        = '2',
  'write.format.default'  = 'parquet',
  'write.upsert.enabled'  = 'true'
);

CREATE TABLE IF NOT EXISTS local_iceberg.${CONFLUENT_SILVER_SCHEMA}.confluent_silver_order_items (
  order_item_id  INT NOT NULL,
  order_id       INT NOT NULL,
  product_id     INT NOT NULL,
  quantity       INT,
  discount_pct   DECIMAL(5, 2),
  transformed_at TIMESTAMP(6) WITH LOCAL TIME ZONE,
  PRIMARY KEY (order_item_id) NOT ENFORCED
) WITH (
  'format-version'        = '2',
  'write.format.default'  = 'parquet',
  'write.upsert.enabled'  = 'true'
);

-- sales_enriched.order_ts: plain timestamp (from orders, no tz)
-- PRIMARY KEY = order_item_id (the line-grain natural key) → idempotent upserts.
CREATE TABLE IF NOT EXISTS local_iceberg.${CONFLUENT_SILVER_SCHEMA}.confluent_silver_sales_enriched (
  order_item_id    INT NOT NULL,
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
  transformed_at   TIMESTAMP(6) WITH LOCAL TIME ZONE,
  PRIMARY KEY (order_item_id) NOT ENFORCED
) WITH (
  'format-version'        = '2',
  'write.format.default'  = 'parquet',
  'write.upsert.enabled'  = 'true'
);

-- ===========================================================================
-- STAGE 1 JOBS — raw Kafka → silver Kafka (transform + filter)
-- Mirrors the dbt silver SQL exactly: same casts, filters, normalisation.
-- ===========================================================================

-- Job 1: raw_customers → silver_customers (Kafka, Avro)
-- Mirrors: models/silver/silver_customers.sql (where email is not null)
SET 'pipeline.name' = 'kafka-raw-to-silver :: customers';
INSERT INTO kafka_silver_customers
SELECT
  customer_id,
  TRIM(first_name),
  TRIM(last_name),
  LOWER(TRIM(email)),
  CAST(CAST(signup_date AS DATE) AS STRING),
  UPPER(TRIM(country)),
  CAST(CURRENT_TIMESTAMP AS STRING)
FROM src_customers
WHERE email IS NOT NULL AND TRIM(email) <> '';

-- Job 2: raw_products → silver_products (Kafka, Avro)
-- Mirrors: models/silver/silver_products.sql (where product_id is not null)
-- unit_price tightened DOUBLE → DECIMAL(12,2) HERE, in the silver stage.
SET 'pipeline.name' = 'kafka-raw-to-silver :: products';
INSERT INTO kafka_silver_products
SELECT
  product_id,
  TRIM(product_name),
  TRIM(category),
  CAST(unit_price AS DECIMAL(12, 2)),
  CAST(CURRENT_TIMESTAMP AS STRING)
FROM src_products
WHERE product_id IS NOT NULL;

-- Job 3: raw_orders → silver_orders (Kafka, Avro)
-- Mirrors: models/silver/silver_orders.sql (where order_id is not null)
SET 'pipeline.name' = 'kafka-raw-to-silver :: orders';
INSERT INTO kafka_silver_orders
SELECT
  order_id,
  customer_id,
  CAST(CAST(order_ts AS TIMESTAMP(6)) AS STRING),
  CAST(CAST(CAST(order_ts AS TIMESTAMP(6)) AS DATE) AS STRING),
  LOWER(TRIM(status)),
  LOWER(TRIM(payment_method)),
  CAST(CURRENT_TIMESTAMP AS STRING)
FROM src_orders
WHERE order_id IS NOT NULL;

-- Job 4: raw_order_items → silver_order_items (Kafka, Avro)
-- Mirrors: models/silver/silver_order_items.sql (where quantity > 0)
-- discount_pct tightened DOUBLE → DECIMAL(5,2) HERE, in the silver stage.
SET 'pipeline.name' = 'kafka-raw-to-silver :: order_items';
INSERT INTO kafka_silver_order_items
SELECT
  order_item_id,
  order_id,
  product_id,
  quantity,
  CAST(discount_pct AS DECIMAL(5, 2)),
  CAST(CURRENT_TIMESTAMP AS STRING)
FROM src_order_items
WHERE quantity > 0;

-- ===========================================================================
-- STAGE 2 SOURCES — silver Kafka topics, read back as typed Avro streams.
-- MONEY is already DECIMAL here (it was written as DECIMAL in Stage 1).
-- ===========================================================================

CREATE TEMPORARY TABLE silver_src_customers (
  customer_id    INT NOT NULL,
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
  'format'                       = 'avro-confluent',
  'avro-confluent.url'           = '${SCHEMA_REGISTRY_URL}'
);

CREATE TEMPORARY TABLE silver_src_products (
  product_id     INT NOT NULL,
  product_name   STRING,
  category       STRING,
  unit_price     DECIMAL(12, 2),
  transformed_at STRING
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'silver_products',
  'properties.bootstrap.servers' = 'confluent-kafka:9092',
  'properties.group.id'          = 'flink-iceberg-products',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'avro-confluent',
  'avro-confluent.url'           = '${SCHEMA_REGISTRY_URL}'
);

CREATE TEMPORARY TABLE silver_src_orders (
  order_id        INT NOT NULL,
  customer_id     INT NOT NULL,
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
  'format'                       = 'avro-confluent',
  'avro-confluent.url'           = '${SCHEMA_REGISTRY_URL}'
);

CREATE TEMPORARY TABLE silver_src_order_items (
  order_item_id  INT NOT NULL,
  order_id       INT NOT NULL,
  product_id     INT NOT NULL,
  quantity       INT,
  discount_pct   DECIMAL(5, 2),
  transformed_at STRING
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'silver_order_items',
  'properties.bootstrap.servers' = 'confluent-kafka:9092',
  'properties.group.id'          = 'flink-iceberg-order-items',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'avro-confluent',
  'avro-confluent.url'           = '${SCHEMA_REGISTRY_URL}'
);

-- ===========================================================================
-- STAGE 2 JOBS — silver Kafka → Iceberg (UPSERT by primary key)
-- ===========================================================================

-- Job 5: confluent_silver_customers
-- transformed_at: STRING → TIMESTAMP(6) WITH LOCAL TIME ZONE (= dbt "timestamp with time zone")
SET 'pipeline.name' = 'kafka-silver-to-iceberg :: customers';
INSERT INTO local_iceberg.${CONFLUENT_SILVER_SCHEMA}.confluent_silver_customers
SELECT
  customer_id,
  first_name,
  last_name,
  email,
  CAST(signup_date    AS DATE),
  country,
  CAST(transformed_at AS TIMESTAMP(6) WITH LOCAL TIME ZONE)
FROM silver_src_customers;

-- Job 6: confluent_silver_products  (unit_price already DECIMAL(12,2))
SET 'pipeline.name' = 'kafka-silver-to-iceberg :: products';
INSERT INTO local_iceberg.${CONFLUENT_SILVER_SCHEMA}.confluent_silver_products
SELECT
  product_id,
  product_name,
  category,
  unit_price,
  CAST(transformed_at AS TIMESTAMP(6) WITH LOCAL TIME ZONE)
FROM silver_src_products;

-- Job 7: confluent_silver_orders
-- order_ts: STRING → TIMESTAMP(6) (plain timestamp, no tz — matches dbt)
SET 'pipeline.name' = 'kafka-silver-to-iceberg :: orders';
INSERT INTO local_iceberg.${CONFLUENT_SILVER_SCHEMA}.confluent_silver_orders
SELECT
  order_id,
  customer_id,
  CAST(order_ts       AS TIMESTAMP(6)),
  CAST(order_date     AS DATE),
  status,
  payment_method,
  CAST(transformed_at AS TIMESTAMP(6) WITH LOCAL TIME ZONE)
FROM silver_src_orders;

-- Job 8: confluent_silver_order_items  (discount_pct already DECIMAL(5,2))
SET 'pipeline.name' = 'kafka-silver-to-iceberg :: order_items';
INSERT INTO local_iceberg.${CONFLUENT_SILVER_SCHEMA}.confluent_silver_order_items
SELECT
  order_item_id,
  order_id,
  product_id,
  quantity,
  discount_pct,
  CAST(transformed_at AS TIMESTAMP(6) WITH LOCAL TIME ZONE)
FROM silver_src_order_items;

-- Job 9: confluent_silver_sales_enriched
-- Stream-stream INNER join across all 4 silver Kafka topics (matches the dbt
-- INNER-join orphan policy in models/silver/silver_sales_enriched.sql).
-- ALL money math is now in DECIMAL (unit_price DECIMAL(12,2), discount DECIMAL(5,2)),
-- so net_amount ties to the cent and equals dbt's value exactly.
SET 'pipeline.name' = 'kafka-silver-to-iceberg :: sales_enriched [join]';
INSERT INTO local_iceberg.${CONFLUENT_SILVER_SCHEMA}.confluent_silver_sales_enriched
SELECT
  oi.order_item_id,
  oi.order_id,
  CAST(o.order_date AS DATE)                                              AS order_date,
  CAST(o.order_ts   AS TIMESTAMP(6))                                      AS order_ts,
  o.status,
  o.payment_method,
  c.customer_id,
  c.country                                                              AS customer_country,
  p.product_id,
  p.product_name,
  p.category,
  oi.quantity,
  p.unit_price                                                           AS unit_price,
  oi.discount_pct                                                        AS discount_pct,
  CAST(oi.quantity * p.unit_price AS DECIMAL(14, 2))                     AS gross_amount,
  CAST(oi.quantity * p.unit_price * (1 - oi.discount_pct)
       AS DECIMAL(14, 2))                                                AS net_amount,
  CURRENT_TIMESTAMP                                                       AS transformed_at
FROM      silver_src_order_items  AS oi
JOIN      silver_src_orders       AS o  ON oi.order_id   = o.order_id
JOIN      silver_src_products     AS p  ON oi.product_id = p.product_id
JOIN      silver_src_customers    AS c  ON o.customer_id = c.customer_id;
