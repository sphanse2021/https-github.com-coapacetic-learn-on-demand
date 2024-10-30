select
    id as customer_id,
    upper(first_name),
    last_name

from {{ source('jaffle_shop','customers') }}