select
    CAST(id AS VARCHAR) as customer_id,
    name,
    first_name,
    last_name
from {{ source('jaffle_shop', 'customers') }}