select
    user_id,
    name,
    email,
    gender,
    city,
    signup_date
from {{ source('raw', 'users') }}
