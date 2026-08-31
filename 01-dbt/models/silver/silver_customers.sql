select
  cast(customer_id as integer) as customer_id,
  trim(first_name) as first_name,
  trim(last_name) as last_name,
  lower(trim(email)) as email,
  cast(signup_date as date) as signup_date,
  upper(trim(country)) as country,
  current_timestamp as transformed_at
from {{ ref('bronze_customers') }}
where email is not null
