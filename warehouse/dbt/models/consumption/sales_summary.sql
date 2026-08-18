select
    count(distinct order_id) as total_orders,
    count(order_item_id) as total_order_items,
    sum(quantity) as total_quantity,
    sum(item_total) as total_revenue,
    sum(item_total) / nullif(count(distinct order_id), 0) as average_order_value
from {{ ref('mart_sales') }}
