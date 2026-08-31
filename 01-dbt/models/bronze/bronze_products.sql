select
  product_id,
  product_name,
  category,
  unit_price,
  current_timestamp as _ingested_at,
  'dbt seed' as _ingested_by,
  'raw_products.csv' as _source_file,
  '{{ env_var("WXD_INGEST_BATCH_ID", "demo_seed_batch") }}' as _ingest_batch_id
from {{ ref('raw_products') }}
