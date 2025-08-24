with payments AS
(
    SELECT * FROM {{ ref("stg_stripe__payments") }}
),
orders AS
(
    SELECT * FROM {{ ref("stg_jaffle_shop__orders") }}
)
SELECT 
    p.order_id,
    o.customer_id,
    p.amount
FROM payments p
JOIN orders o
ON p.order_id = o.order_id