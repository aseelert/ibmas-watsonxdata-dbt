-- watsonx.data dbt and Spark medallion demo SQL
-- Run these in the watsonx.data SQL editor against the Presto engine.
--
-- Medallion layers:
-- - Raw landing is the CSV payload loaded as-is for traceability.
-- - Bronze is the first managed Iceberg copy with ingestion metadata.
-- - Silver is typed, cleaned, conformed data, including one ENRICHED fact
--   (silver_sales_enriched) that JOINS all four entities together.
-- - Gold is business-facing. gold_daily_sales is a physical TABLE;
--   gold_category_performance is a VIEW layered on that gold table.
--
-- The dbt path writes to lakehouse_demo_* schemas.
-- The Spark path writes to spark_demo_* schemas.
-- Both use the same CSV files so customers can compare the approaches.

-- 1. Inspect the dbt medallion schemas.
show schemas from iceberg_data like 'lakehouse_demo%';

show tables from iceberg_data.lakehouse_demo_raw;
show tables from iceberg_data.lakehouse_demo_bronze;
show tables from iceberg_data.lakehouse_demo_silver;
show tables from iceberg_data.lakehouse_demo_gold;

-- 2. Raw landing data: the direct CSV load, kept close to the source file.
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

-- 4. Silver data: cleaned, typed, conformed dimension/fact tables.
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

-- 4b. Silver ENRICHED fact: orders + items + products + customers joined into
-- one analytics-ready table at order-line grain. This is the single source the
-- gold marts read from.
select
  order_item_id,
  order_id,
  order_date,
  customer_id,
  customer_country,
  product_id,
  category,
  quantity,
  unit_price,
  discount_pct,
  gross_amount,
  net_amount
from iceberg_data.lakehouse_demo_silver.silver_sales_enriched
order by order_id, order_item_id;

-- 5. Gold TABLE: business aggregate, physically materialized for fast reads.
select *
from iceberg_data.lakehouse_demo_gold.gold_daily_sales
order by order_date, category;

-- 5b. Gold VIEW layered on the gold table above. It stores no data; it
-- recomputes from gold_daily_sales every time you query it.
select *
from iceberg_data.lakehouse_demo_gold.gold_category_performance
order by total_revenue desc;

-- 5c. Prove the difference: one is a TABLE, one is a VIEW.
show create table iceberg_data.lakehouse_demo_gold.gold_daily_sales;
show create view iceberg_data.lakehouse_demo_gold.gold_category_performance;

-- 5d. Customer 360 mart (a view built from the enriched silver fact).
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
from iceberg_data.spark_demo_silver.spark_silver_sales_enriched
order by order_id, order_item_id;

select *
from iceberg_data.spark_demo_gold.spark_gold_daily_sales
order by order_date, category;

select *
from iceberg_data.spark_demo_gold.spark_gold_category_performance
order by total_revenue desc;

select *
from iceberg_data.spark_demo_gold.spark_gold_customer_360
order by lifetime_value desc, customer_id;

-- 10. Compare dbt gold and Spark gold daily sales for the same source data.
select
  'dbt' as path,
  order_date,
  category,
  order_count,
  units_sold,
  net_revenue
from iceberg_data.lakehouse_demo_gold.gold_daily_sales
union all
select
  'spark' as path,
  order_date,
  category,
  order_count,
  units_sold,
  net_revenue
from iceberg_data.spark_demo_gold.spark_gold_daily_sales
order by order_date, category, path;

-- 11. Compare dbt and Spark category performance (the gold view).
select
  'dbt' as path,
  category,
  total_orders,
  total_units,
  total_revenue
from iceberg_data.lakehouse_demo_gold.gold_category_performance
union all
select
  'spark' as path,
  category,
  total_orders,
  total_units,
  total_revenue
from iceberg_data.spark_demo_gold.spark_gold_category_performance
order by category, path;

-- 12. Compare dbt and Spark customer 360.
select
  'dbt' as path,
  customer_id,
  completed_orders,
  returned_orders,
  pending_orders,
  cancelled_orders,
  lifetime_value
from iceberg_data.lakehouse_demo_gold.gold_customer_360
union all
select
  'spark' as path,
  customer_id,
  completed_orders,
  returned_orders,
  pending_orders,
  cancelled_orders,
  lifetime_value
from iceberg_data.spark_demo_gold.spark_gold_customer_360
order by customer_id, path;
