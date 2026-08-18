select
    f.order_item_id,
    f.order_id,
    o.order_date,
    o.order_status,
    f.user_id,
    u.name as customer_name,
    u.email as customer_email,
    f.product_id,
    p.product_name,
    p.category,
    p.brand,
    f.quantity,
    f.item_price,
    f.item_total
from {{ ref('fct_order_items') }} as f
inner join {{ ref('dim_orders') }} as o
    on f.order_id = o.order_id
inner join {{ ref('dim_products') }} as p
    on f.product_id = p.product_id
inner join {{ ref('dim_users') }} as u
    on f.user_id = u.user_id
