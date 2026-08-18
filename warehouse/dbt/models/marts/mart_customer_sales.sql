select
    u.user_id,
    u.name as customer_name,
    u.email as customer_email,
    u.city,
    u.signup_date,
    count(distinct o.order_id) as total_orders,
    count(f.order_item_id) as total_order_items,
    sum(f.quantity) as total_quantity,
    sum(f.item_total) as total_spend,
    sum(f.item_total) / nullif(count(distinct o.order_id), 0) as average_order_value,
    min(o.order_date) as first_order_date,
    max(o.order_date) as last_order_date
from {{ ref('dim_users') }} as u
inner join {{ ref('dim_orders') }} as o
    on u.user_id = o.user_id
inner join {{ ref('fct_order_items') }} as f
    on o.order_id = f.order_id
group by
    u.user_id,
    u.name,
    u.email,
    u.city,
    u.signup_date
