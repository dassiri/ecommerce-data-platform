select
    p.product_id,
    p.product_name,
    p.category,
    p.brand,
    p.rating as current_rating,
    count(distinct f.order_id) as total_orders,
    count(f.order_item_id) as total_order_items,
    sum(f.quantity) as total_quantity,
    sum(f.item_total) as total_revenue,
    avg(f.item_price) as average_item_price,
    min(o.order_date) as first_order_date,
    max(o.order_date) as last_order_date
from {{ ref('dim_products') }} as p
inner join {{ ref('fct_order_items') }} as f
    on p.product_id = f.product_id
inner join {{ ref('dim_orders') }} as o
    on f.order_id = o.order_id
group by
    p.product_id,
    p.product_name,
    p.category,
    p.brand,
    p.rating
