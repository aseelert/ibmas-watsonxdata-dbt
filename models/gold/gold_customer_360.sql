select
  c.customer_id,
  c.first_name,
  c.last_name,
  c.email,
  c.country,
  c.signup_date,
  count(distinct case when o.status = 'completed' then o.order_id end) as completed_orders,
  count(distinct case when o.status = 'returned' then o.order_id end) as returned_orders,
  count(distinct case when o.status = 'pending' then o.order_id end) as pending_orders,
  count(distinct case when o.status = 'cancelled' then o.order_id end) as cancelled_orders,
  cast(coalesce(sum(
    case
      when o.status = 'completed'
      then oi.quantity * p.unit_price * (1 - oi.discount_pct)
      else 0
    end
  ), 0) as decimal(14, 2)) as lifetime_value,
  max(case when o.status = 'completed' then o.order_ts end) as last_completed_order_ts,
  max(o.order_ts) as last_activity_ts
from {{ ref('silver_customers') }} c
left join {{ ref('silver_orders') }} o
  on c.customer_id = o.customer_id
left join {{ ref('silver_order_items') }} oi
  on o.order_id = oi.order_id
left join {{ ref('silver_products') }} p
  on oi.product_id = p.product_id
group by 1, 2, 3, 4, 5, 6
