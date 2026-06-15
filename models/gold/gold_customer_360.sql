-- Gold customer mart (view by default). Metrics come from the enriched
-- silver fact; customer attributes are joined from the silver dimension so
-- that customers with no orders are still represented.
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
