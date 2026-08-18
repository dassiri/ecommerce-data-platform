select
    product_id,
    product_name,
    category,
    brand,
    price,
    rating
from {{ ref('stg_products') }}
