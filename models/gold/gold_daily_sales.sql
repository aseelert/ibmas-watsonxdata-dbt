select
  o.order_date,
  p.category,
  count(distinct o.order_id) as order_count,
  sum(oi.quantity) as units_sold,
  cast(sum(oi.quantity * p.unit_price * (1 - oi.discount_pct)) as decimal(14, 2)) as net_revenue
from {{ ref('silver_orders') }} o
join {{ ref('silver_order_items') }} oi
  on o.order_id = oi.order_id
join {{ ref('silver_products') }} p
  on oi.product_id = p.product_id
where o.status = 'completed'
group by 1, 2
