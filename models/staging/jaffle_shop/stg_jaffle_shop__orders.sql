select
    CAST(id AS VARCHAR) as order_id,
    CAST(customer AS VARCHAR) as customer_id,
    ordered_at as order_date,
    CAST(store_id AS VARCHAR) as location_id,
    subtotal/100 as subtotal,
    tax_paid/100 as tax_paid,
    order_total/100 as order_total
from {{ source('jaffle_shop', 'orders') }}