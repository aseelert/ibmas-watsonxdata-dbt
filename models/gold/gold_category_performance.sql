{{ config(materialized='view') }}

-- Gold VIEW built on top of the gold_daily_sales TABLE.
-- Category-level roll-up computed on demand; no data stored in this object.
-- NOTE: Presto's Iceberg connector does not yet support CREATE MATERIALIZED VIEW.
-- When that feature becomes available, change materialized='view' to 'materialized_view'
-- and the custom macro in macros/materialized_view.sql will handle the DDL automatically.
select
  category,
  sum(order_count) as total_orders,
  sum(units_sold) as total_units,
  cast(sum(net_revenue) as decimal(14, 2)) as total_revenue,
  cast(sum(net_revenue) / nullif(sum(units_sold), 0) as decimal(14, 2)) as avg_revenue_per_unit
from {{ ref('gold_daily_sales') }}
group by 1
order by total_revenue desc
