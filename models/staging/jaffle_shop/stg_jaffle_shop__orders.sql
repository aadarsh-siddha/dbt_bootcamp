select
    id as order_id,
    customer as customer_id,
    ordered_at as order_date,
    store_id,
    subtotal/100 as subtotal,
    tax_paid/100 as tax_paid,
    order_total/100 as order_total
from {{ source('jaffle_shop', 'orders') }}