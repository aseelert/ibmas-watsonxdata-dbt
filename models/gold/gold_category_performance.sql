-- -----------------------------------------------------------------------------
--  gold_category_performance.sql — category-level roll-up of the gold daily sales
--
--  Location  : models/gold/gold_category_performance.sql
--  Repository: https://github.com/aseelert/ibmas-watsonxdata-dbt
--  Project   : watsonx.data · dbt · Spark · Confluent medallion demo
--  Author    : Alexander Seelert — IBM Customer Success Engineer
--  Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.
--
--  Changelog :
--    v1.0 (2026-06-26) — Initial version. Category roll-up over gold_daily_sales.
--    v1.1 (2026-06-26) — Parity/correctness fix: pinned an EXPLICIT view
--                        materialization (no longer relying on the project's
--                        WXD_GOLD_MATERIALIZED default) and removed the
--                        non-binding `order by total_revenue desc` so cross-engine
--                        reconciliation is order-insensitive (a SQL view/table has
--                        no guaranteed row order — ordering is a presentation
--                        concern for the BI tool, not the mart). Logic unchanged.
-- -----------------------------------------------------------------------------

-- WHAT: one row per product category, aggregating the gold daily-sales grain.
-- WHY view: this is a thin, cheap roll-up over the gold_daily_sales TABLE, so we
-- store no extra data and always reflect the latest daily-sales table. We pin
-- 'view' EXPLICITLY here (rather than inheriting WXD_GOLD_MATERIALIZED) so this
-- decision is visible and stable no matter how the env default is set.
-- NOTE: Presto's Iceberg connector does not yet support CREATE MATERIALIZED VIEW.
-- When that feature becomes available, change materialized='view' to 'materialized_view'
-- and the custom macro in macros/materialized_view.sql will handle the DDL automatically.
{{ config(materialized='view') }}

select
  category,
  sum(order_count) as total_orders,
  sum(units_sold) as total_units,
  cast(sum(net_revenue) as decimal(14, 2)) as total_revenue,
  cast(sum(net_revenue) / nullif(sum(units_sold), 0) as decimal(14, 2)) as avg_revenue_per_unit
from {{ ref('gold_daily_sales') }}
group by 1
-- No ORDER BY on purpose: row order is non-deterministic in a relational mart and
-- not part of the parity contract. Reconciliation across dbt/Spark/Confluent
-- compares the SET of rows; sorting is left to the consuming BI tool.
