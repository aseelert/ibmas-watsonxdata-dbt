select
  order_id,
  customer_id,
  order_ts,
  status,
  payment_method,
  current_timestamp as _ingested_at,
  'dbt seed' as _ingested_by,
  'raw_orders.csv' as _source_file,
  '{{ env_var("WXD_INGEST_BATCH_ID", "demo_seed_batch") }}' as _ingest_batch_id
from {{ ref('raw_orders') }}
