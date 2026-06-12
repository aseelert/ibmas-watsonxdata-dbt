select
  customer_id,
  first_name,
  last_name,
  email,
  signup_date,
  country,
  current_timestamp as _ingested_at,
  'dbt seed' as _ingested_by,
  'raw_customers.csv' as _source_file,
  '{{ env_var("WXD_INGEST_BATCH_ID", "demo_seed_batch") }}' as _ingest_batch_id
from {{ ref('raw_customers') }}
