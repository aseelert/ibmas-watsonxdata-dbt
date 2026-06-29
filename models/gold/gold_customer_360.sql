-- -----------------------------------------------------------------------------
--  gold_customer_360.sql — one row per customer: lifetime metrics + attributes
--
--  Location  : models/gold/gold_customer_360.sql
--  Repository: https://github.ibm.com/alexander/ibmas-watsonxdata-dbt
--  Project   : watsonx.data · dbt · Spark · Confluent medallion demo
--  Author    : Alexander Seelert — IBM Customer Success Engineer
--  Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.
--
--  Changelog :
--    v1.0 (2026-06-26) — Initial version. Per-customer 360 view (LEFT join so
--                        customers with no orders still appear, 0-filled).
--    v1.1 (2026-06-26) — Parity/correctness fix: pinned an EXPLICIT view
--                        materialization instead of relying on the implicit
--                        project default (WXD_GOLD_MATERIALIZED), so this mart's
--                        physical shape is stable and matches its sibling marts.
--                        Logic unchanged.
-- -----------------------------------------------------------------------------

-- WHAT: customer 360 mart — grain is one row PER CUSTOMER. Metrics come from the
-- enriched silver fact; customer attributes are joined from the silver dimension
-- via a LEFT join so that customers with NO orders are still represented (their
-- metric columns are coalesced to 0 / NULL timestamps).
-- WHY view: like the other thin gold roll-ups this is computed on demand over
-- already-materialized silver; we pin 'view' EXPLICITLY (not via the env default)
-- so the decision is self-documenting and consistent with gold_category_performance.
{{ config(materialized='view') }}

with metrics as (
  select
    customer_id,
    count(distinct case when status = 'completed' then order_id end) as completed_orders,
    count(distinct case when status = 'returned' then order_id end) as returned_orders,
    count(distinct case when status = 'pending' then order_id end) as pending_orders,
    count(distinct case when status = 'cancelled' then order_id end) as cancelled_orders,
    cast(coalesce(sum(case when status = 'completed' then net_amount else 0 end), 0) as decimal(14, 2)) as lifetime_value,
    max(case when status = 'completed' then order_ts end) as last_completed_order_ts,
    max(order_ts) as last_activity_ts
  from {{ ref('silver_sales_enriched') }}
  group by customer_id
)

select
  c.customer_id,
  c.first_name,
  c.last_name,
  c.email,
  c.country,
  c.signup_date,
  coalesce(m.completed_orders, 0) as completed_orders,
  coalesce(m.returned_orders, 0) as returned_orders,
  coalesce(m.pending_orders, 0) as pending_orders,
  coalesce(m.cancelled_orders, 0) as cancelled_orders,
  coalesce(m.lifetime_value, 0) as lifetime_value,
  m.last_completed_order_ts,
  m.last_activity_ts
from {{ ref('silver_customers') }} c
left join metrics m
  on c.customer_id = m.customer_id
