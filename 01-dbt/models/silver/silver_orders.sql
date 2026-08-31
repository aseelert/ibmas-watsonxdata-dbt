{{
  config(
    properties={
      "format": "'PARQUET'",
      "partitioning": "ARRAY['month(order_date)']"
    }
  )
}}

select
  cast(order_id as integer) as order_id,
  cast(customer_id as integer) as customer_id,
  cast(order_ts as timestamp) as order_ts,
  cast(cast(order_ts as timestamp) as date) as order_date,
  lower(trim(status)) as status,
  lower(trim(payment_method)) as payment_method,
  current_timestamp as transformed_at
from {{ ref('bronze_orders') }}
where order_id is not null
