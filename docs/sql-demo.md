# SQL Demo

Run these statements in the watsonx.data SQL editor against the Presto engine.

!!! note "Raw in SQL vs raw files"
    The original raw data is the CSV files. The `lakehouse_demo_raw` tables exist because dbt seed loads those CSV files into queryable Presto tables. Spark reads the CSV files directly from object storage, so there is no `spark_demo_raw` schema in this demo.

## Inspect dbt Schemas

```sql
show schemas from iceberg_data like 'lakehouse_demo%';

show tables from iceberg_data.lakehouse_demo_raw;
show tables from iceberg_data.lakehouse_demo_bronze;
show tables from iceberg_data.lakehouse_demo_silver;
show tables from iceberg_data.lakehouse_demo_gold;
```

## Raw Landing

```sql
select *
from iceberg_data.lakehouse_demo_raw.raw_orders
order by order_id;
```

## Bronze Metadata

```sql
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
```

## Silver Orders

```sql
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
```

## Gold Marts

```sql
select *
from iceberg_data.lakehouse_demo_gold.gold_daily_sales
order by order_date, category;

select *
from iceberg_data.lakehouse_demo_gold.gold_customer_360
order by lifetime_value desc, customer_id;
```

## Iceberg Metadata

```sql
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
```

## Time Travel

Replace `<snapshot_id>` with a value from `"silver_orders$snapshots"`.

```sql
select *
from iceberg_data.lakehouse_demo_silver.silver_orders
for version as of <snapshot_id>
order by order_id;
```

Use a timestamp after one of the `committed_at` values.

```sql
select *
from iceberg_data.lakehouse_demo_silver.silver_orders
for timestamp as of timestamp '2026-06-12 12:46:47 UTC'
order by order_id;
```

## Spark Output

```sql
show schemas from iceberg_data like 'spark_demo%';

show tables from iceberg_data.spark_demo_bronze;
show tables from iceberg_data.spark_demo_silver;
show tables from iceberg_data.spark_demo_gold;

select *
from iceberg_data.spark_demo_gold.spark_gold_daily_sales
order by order_date, category;

select *
from iceberg_data.spark_demo_gold.spark_gold_customer_360
order by lifetime_value desc, customer_id;
```

## Compare dbt and Spark Gold

```sql
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
```

## Compare dbt and Spark Customer 360

```sql
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
```

The full SQL script is also available in `docs/watsonxdata_sql_demo.sql`.
