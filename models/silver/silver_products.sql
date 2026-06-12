select
  cast(product_id as integer) as product_id,
  trim(product_name) as product_name,
  trim(category) as category,
  cast(unit_price as decimal(12, 2)) as unit_price,
  current_timestamp as transformed_at
from {{ ref('bronze_products') }}
where product_id is not null
