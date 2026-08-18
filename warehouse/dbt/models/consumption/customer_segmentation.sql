select
    user_id,
    customer_name,
    customer_email,
    city,
    signup_date,
    total_orders,
    total_spend,
    last_order_date,
    case
        when total_spend >= 5000 then 'VIP'
        when total_orders >= 5
            and total_spend < 5000 then 'Loyal'
        when total_orders >= 2
            and total_orders < 5 then 'Regular'
        when total_orders = 1 then 'One-Time'
    end as customer_segment
from {{ ref('mart_customer_sales') }}
