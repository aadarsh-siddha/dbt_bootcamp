select 
    unique_customer
from
(
    select 
        md5(customer_id || first_name || last_name) as unique_customer
    from {{ ref('stg_jaffle_shop__customers') }}
)
group by 1
having count(*) > 1