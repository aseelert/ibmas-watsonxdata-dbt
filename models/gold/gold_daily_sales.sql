{{ config(
    materialized="table",
    properties={
        "format": "'PARQUET'",
        "partitioning": "ARRAY['month(order_date)']"
    }
) }}

-- Gold business aggregate, materialized as a PHYSICAL TABLE.
-- Built from the enriched silver fact (single source, no re-joining here).
select
  order_date,
  category,
  count(distinct order_id) as order_count,
  sum(quantity) as units_sold,
  cast(sum(net_amount) as decimal(14, 2)) as net_revenue
from {{ ref('silver_sales_enriched') }}
where status = 'completed'
group by 1, 2
