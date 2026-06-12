-- watsonx.data dbt and Spark medallion demo SQL
-- Run these in the watsonx.data SQL editor against the Presto engine.

-- 1. Inspect the dbt medallion schemas.
show schemas from iceberg_data like 'lakehouse_demo%';

show tables from iceberg_data.lakehouse_demo_raw;
show tables from iceberg_data.lakehouse_demo_bronze;
show tables from iceberg_data.lakehouse_demo_silver;
show tables from iceberg_data.lakehouse_demo_gold;

-- 2. Raw landing data: this is the direct CSV load.
select *
from iceberg_data.lakehouse_demo_raw.raw_orders
order by order_id;

-- 3. Bronze data: same business payload plus ingestion metadata.
select
  order_id,
  customer_id,
  order_ts,
  status,
  payment_method,
  _ingested_at,
  _ingested_by,
  _source_file,
  _ingest_batch_id
from iceberg_data.lakehouse_demo_bronze.bronze_orders
order by order_id;

-- 4. Silver data: cleaned, typed, conformed.
select
  order_id,
  customer_id,
  order_ts,
  order_date,
  status,
  payment_method,
  transformed_at
from iceberg_data.lakehouse_demo_silver.silver_orders
order by order_id;

-- 5. Gold marts: business-facing views.
select *
from iceberg_data.lakehouse_demo_gold.gold_daily_sales
order by order_date, category;

select *
from iceberg_data.lakehouse_demo_gold.gold_customer_360
order by lifetime_value desc, customer_id;

-- 6. Iceberg metadata: snapshots, history, and partitions.
select
  committed_at,
  snapshot_id,
  operation,
  summary
from iceberg_data.lakehouse_demo_silver."silver_orders$snapshots"
order by committed_at desc;

select *
from iceberg_data.lakehouse_demo_silver."silver_orders$history"
order by made_current_at desc;

select *
from iceberg_data.lakehouse_demo_silver."silver_orders$partitions"
order by order_date;

show create table iceberg_data.lakehouse_demo_silver.silver_orders;

-- 7. Iceberg time travel by snapshot id.
-- Replace <snapshot_id> with a value from "silver_orders$snapshots".
select count(*)
from iceberg_data.lakehouse_demo_silver.silver_orders
for version as of <snapshot_id>;

select *
from iceberg_data.lakehouse_demo_silver.silver_orders
for system_version as of <snapshot_id>
order by order_id;

-- 8. Iceberg time travel by timestamp.
-- Use a timestamp after a snapshot commit time from "silver_orders$snapshots".
select count(*)
from iceberg_data.lakehouse_demo_silver.silver_orders
for timestamp as of timestamp '2026-06-12 12:46:47 UTC';

select *
from iceberg_data.lakehouse_demo_silver.silver_orders
for system_time as of timestamp '2026-06-12 12:46:47 UTC'
order by order_id;

-- 9. Spark demo output: separate schemas, same CSV data.
show schemas from iceberg_data like 'spark_demo%';

show tables from iceberg_data.spark_demo_bronze;
show tables from iceberg_data.spark_demo_silver;
show tables from iceberg_data.spark_demo_gold;

select *
from iceberg_data.spark_demo_gold.spark_gold_daily_sales
order by order_date, category;
