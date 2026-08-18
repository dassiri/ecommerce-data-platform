select
    order_id,
    user_id,
    order_date,
    order_status,
    total_amount
from {{ source('raw', 'orders') }}
