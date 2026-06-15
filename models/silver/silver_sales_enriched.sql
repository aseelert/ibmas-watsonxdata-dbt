{{ config(
    materialized="table",
    properties={
      "format": "'PARQUET'",
      "partitioning": "ARRAY['month(order_date)']"
    }
) }}

-- Silver enrichment (the augmented/joined layer):
-- conform + join the four clean silver entities into one analytics-ready
-- fact at order-line grain. Gold marts read from this single table.
select
  oi.order_item_id,
  oi.order_id,
  o.order_date,
  o.order_ts,
  o.status,
  o.payment_method,
  c.customer_id,
  c.country as customer_country,
  p.product_id,
  p.product_name,
  p.category,
  oi.quantity,
  p.unit_price,
  oi.discount_pct,
  cast(oi.quantity * p.unit_price as decimal(14, 2)) as gross_amount,
  cast(oi.quantity * p.unit_price * (1 - oi.discount_pct) as decimal(14, 2)) as net_amount,
  current_timestamp as transformed_at
from {{ ref('silver_order_items') }} oi
join {{ ref('silver_orders') }} o
  on oi.order_id = o.order_id
join {{ ref('silver_products') }} p
  on oi.product_id = p.product_id
join {{ ref('silver_customers') }} c
  on o.customer_id = c.customer_id
