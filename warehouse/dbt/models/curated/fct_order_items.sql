select
    order_item_id,
    order_id,
    product_id,
    user_id,
    quantity,
    item_price,
    item_total
from {{ ref('stg_order_items') }}
