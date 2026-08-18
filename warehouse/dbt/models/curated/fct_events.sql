select
    event_id,
    user_id,
    product_id,
    event_type,
    event_timestamp
from {{ ref('stg_events') }}
