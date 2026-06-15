{{ config(materialized='view') }}

-- Gold VIEW built on top of the gold_daily_sales TABLE.
-- It stores no data and recomputes from the gold table on every read.
select
  category,
  sum(order_count) as total_orders,
  sum(units_sold) as total_units,
  cast(sum(net_revenue) as decimal(14, 2)) as total_revenue,
  cast(sum(net_revenue) / nullif(sum(units_sold), 0) as decimal(14, 2)) as avg_revenue_per_unit
from {{ ref('gold_daily_sales') }}
group by 1
order by total_revenue desc
