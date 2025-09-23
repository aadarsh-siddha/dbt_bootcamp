{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='merge',
        on_schema_change='fail'

    )
}}
with payments AS
(
    SELECT * FROM {{ ref("stg_stripe__payments") }}
),
orders AS
(
    SELECT * FROM {{ ref("stg_jaffle_shop__orders") }}
),
final AS
(
    SELECT 
        p.order_id,
        o.customer_id,
        p.amount as order_amount,
        o.order_date
    FROM payments p
    JOIN orders o
    ON p.order_id = o.order_id
)
SELECT * FROM final
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where order_date > (select max(order_date) from {{ this }}) 
{% endif %}
order by order_date desc