{{ config(
    materialized="table",
    properties={
      "format": "'PARQUET'",
      "partitioning": "ARRAY['month(order_date)']"
    }
) }}

-- -----------------------------------------------------------------------------
--  silver_sales_enriched.sql — joined order-line fact feeding every gold mart
--
--  Location  : models/silver/silver_sales_enriched.sql
--  Repository: https://github.com/aseelert/ibmas-watsonxdata-dbt
--  Project   : watsonx.data · dbt · Spark · Confluent medallion demo
--  Author    : Alexander Seelert — IBM Customer Success Engineer
--  Copyright : (c) 2026 Alexander Seelert — demo asset, provided as-is.
--
--  Changelog :
--    v1.0 (2026-06-26) — Initial version. Conform + join the four silver entities
--                        into one order-line-grain analytics fact.
--    v1.1 (2026-06-26) — Documentation only: spelled out the INNER-join orphan
--                        policy below (NO row-count change) so the parity contract
--                        with Spark/Confluent stays explicit and intentional.
-- -----------------------------------------------------------------------------

-- Silver enrichment (the augmented/joined layer):
-- conform + join the four clean silver entities into one analytics-ready
-- fact at order-line grain. Gold marts read from this single table.
--
-- ORPHAN POLICY (READ BEFORE "FIXING" THE JOINS):
-- All three joins below are INNER joins ON PURPOSE. A row survives only if its
-- order_item has a matching order AND a matching product AND that order has a
-- matching customer. Any order-item referencing a missing order/product, or any
-- order referencing a missing customer, is intentionally DROPPED here. In this
-- demo the seeds are referentially complete, so no rows are actually lost — but
-- the policy is stated so nobody silently switches these to LEFT joins (which
-- would inject NULL dimensions and quietly diverge from the Spark/Confluent
-- builds, which use the same INNER-join shape).
--
-- WHY THIS KEEPS THE GOLD MARTS CONSISTENT:
-- BOTH gold_daily_sales and gold_customer_360's metrics CTE read from THIS single
-- enriched fact, so they share an identical universe of order rows — they cannot
-- disagree on which orders "count". gold_customer_360 then LEFT joins the customer
-- DIMENSION on top purely to re-introduce customers who have zero orders
-- (0-filled); that LEFT join adds customers, never orders, so daily_sales and
-- customer_360 remain reconcilable. No fix is warranted here — only this note.
select
  oi.order_item_id,
  oi.order_id,
  o.order_date,
  o.order_ts,
  o.status,
  o.payment_method,
  c.customer_id,
  c.country as customer_country,
  p.product_id,
  p.product_name,
  p.category,
  oi.quantity,
  p.unit_price,
  oi.discount_pct,
  cast(oi.quantity * p.unit_price as decimal(14, 2)) as gross_amount,
  cast(oi.quantity * p.unit_price * (1 - oi.discount_pct) as decimal(14, 2)) as net_amount,
  current_timestamp as transformed_at
from {{ ref('silver_order_items') }} oi
join {{ ref('silver_orders') }} o
  on oi.order_id = o.order_id
join {{ ref('silver_products') }} p
  on oi.product_id = p.product_id
join {{ ref('silver_customers') }} c
  on o.customer_id = c.customer_id
