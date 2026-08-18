select
    user_id,
    name,
    email,
    gender,
    city,
    signup_date
from {{ ref('stg_users') }}
