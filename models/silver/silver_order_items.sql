select
  cast(order_item_id as integer) as order_item_id,
  cast(order_id as integer) as order_id,
  cast(product_id as integer) as product_id,
  cast(quantity as integer) as quantity,
  cast(discount_pct as decimal(5, 2)) as discount_pct,
  current_timestamp as transformed_at
from {{ ref('bronze_order_items') }}
where quantity > 0
